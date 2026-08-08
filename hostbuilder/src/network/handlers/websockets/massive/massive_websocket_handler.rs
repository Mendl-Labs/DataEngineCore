use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use futures_util::{
    stream::{SplitSink, SplitStream},
    SinkExt, StreamExt,
};
use performance::{CpuAffinityManager, cpu_affinity::{set_high_priority, prefault_stack}};
use protocol::broker::messages::{
    market_message, publish_request, Bar, MarketMessage, PublishRequest, Trade, Trades,
};
use publisher::{Publisher, PublisherConfig};
use serde::Deserialize;
use serde_json::Value;
use std::{env, sync::Arc};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::net::TcpStream;
use tokio::sync::{broadcast::Receiver, RwLock};
use tokio_tungstenite::{
    connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream,
};

use crate::network::endpoint::EndpointHandler;
use crate::network::handlers::websockets::massive::BarAggregator;
use crate::infrastructure::logging_facade::MASSIVE_LOGGER;
use crate::{log_info, log_warn, log_error, log_debug};

// =============================================================================
// Message model
// =============================================================================

/// Decoded Massive crypto trade event (`ev == "XT"`).
#[derive(Debug, Deserialize)]
struct MassiveTrade {
    /// Trading pair, e.g. "BTC-USD"
    pub pair: String,
    /// Price
    pub p: f64,
    /// Size / quantity
    pub s: f64,
    /// Conditions: 1 = sellside, 2 = buyside
    #[serde(default)]
    pub c: Vec<i32>,
    /// Timestamp in milliseconds
    pub t: i64,
    /// Trade ID (optional)
    pub i: Option<String>,
}

/// Decoded Massive stock trade event (`ev == "T"`).
#[derive(Debug, Deserialize)]
struct MassiveStockTrade {
    /// Ticker symbol, e.g. "AAPL"
    #[serde(rename = "sym")]
    pub sym: String,
    /// Price
    pub p: f64,
    /// Size / quantity
    pub s: f64,
    /// Trade conditions (NYSE/TAPE codes)
    #[serde(default)]
    pub c: Vec<i32>,
    /// Timestamp in milliseconds
    pub t: i64,
    /// Trade ID (optional)
    pub i: Option<i64>,
}

/// Decoded Massive forex per-second aggregate event (`ev == "CAS"`).
///
/// Polygon's forex feed has no trade events — only quotes (`C`) and
/// aggregates (`CA`/`CAS`). We subscribe to per-second aggregates and
/// synthesize one pseudo-trade per aggregate downstream.
#[derive(Debug, Deserialize)]
struct MassiveForexAgg {
    /// Currency pair, e.g. "EUR/USD"
    pub pair: String,
    /// Open price
    #[allow(dead_code)]
    pub o: f64,
    /// High price
    #[allow(dead_code)]
    pub h: f64,
    /// Low price
    #[allow(dead_code)]
    pub l: f64,
    /// Close price
    pub c: f64,
    /// Volume (tick count for forex)
    pub v: f64,
    /// Start timestamp in milliseconds
    #[serde(rename = "s")]
    #[allow(dead_code)]
    pub start_ms: i64,
    /// End timestamp in milliseconds
    #[serde(rename = "e")]
    pub end_ms: i64,
}

// =============================================================================
// Handler struct
// =============================================================================

pub struct MassiveWebSocketHandler {
    config: Config,
    asset_class: String,
    /// Execution venue this feed serves (e.g. "kraken"). Market data is
    /// published on venue-scoped topics (`market.data.{venue}.{kind}`)
    /// because SignalEngine subscribes per deployment venue — a global
    /// provider-named topic (`market.data.massive.trades`) is invisible to
    /// every strategy. Empty = legacy behavior (config topic_routing).
    venue: String,
    writer: RwLock<SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, Message>>,
    reader: RwLock<SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>>,
    publisher: Option<Arc<tokio::sync::Mutex<Publisher>>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    bar_aggregator: BarAggregator,
}

impl MassiveWebSocketHandler {
    /// Connect to the Massive WebSocket feed, authenticate, and subscribe to
    /// trade events for the given symbols.
    ///
    /// For `asset_class = "stocks"` subscribes to `T.*` (stock trades).
    /// For `asset_class = "forex"` subscribes to `CAS.*` (per-second aggregates —
    /// Polygon's forex feed has no trade events).
    /// For `asset_class = "crypto"` (default) subscribes to `XT.*` (crypto trades).
    ///
    /// Reads `MASSIVE_API_KEY` from the environment.
    /// Reads `CONFIG_PATH` for the DataEngine config (publisher address).
    pub async fn new(
        feed_url: &str,
        symbols: Vec<String>,
        asset_class: String,
        venue: String,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
    ) -> Result<Self> {
        let api_key = env::var("MASSIVE_API_KEY")
            .unwrap_or_default();
        if api_key.is_empty() {
            return Err(anyhow::anyhow!("MASSIVE_API_KEY environment variable is not set or empty"));
        }

        // Connect
        log_info!(MASSIVE_LOGGER, "Connecting to Massive WebSocket: {}", feed_url);
        let (ws_stream, _) = connect_async(feed_url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Massive WebSocket at {}: {}", feed_url, e))?;
        log_info!(MASSIVE_LOGGER, "Connected to Massive WebSocket");

        let (mut writer, mut reader) = ws_stream.split();

        // Expect initial "connected" status message
        if let Some(Ok(Message::Text(msg))) = reader.next().await {
            log_info!(MASSIVE_LOGGER, "Initial message from Massive: {}", msg);
        }

        // Authenticate
        let auth_msg = format!(r#"{{"action":"auth","params":"{}"}}"#, api_key);
        writer.send(Message::Text(auth_msg)).await
            .map_err(|e| anyhow::anyhow!("Failed to send auth message: {}", e))?;

        // Expect auth_success
        loop {
            match reader.next().await {
                Some(Ok(Message::Text(msg))) => {
                    log_info!(MASSIVE_LOGGER, "Auth response: {}", msg);
                    if msg.contains("auth_success") {
                        break;
                    } else if msg.contains("auth_failed") || msg.contains("error") {
                        return Err(anyhow::anyhow!("Massive authentication failed: {}", msg));
                    }
                    // Other status messages — keep waiting
                }
                Some(Ok(_)) => {}
                Some(Err(e)) => return Err(anyhow::anyhow!("WebSocket error during auth: {}", e)),
                None => return Err(anyhow::anyhow!("WebSocket closed before auth_success")),
            }
        }
        log_info!(MASSIVE_LOGGER, "Massive authentication successful");

        // Subscribe to trade events for all symbols.
        // Stocks use `T.` (trades), forex uses `CAS.` (per-second aggregates,
        // pairs use "/" separator), crypto uses `XT.` (crypto trades).
        let event_prefix = match asset_class.as_str() {
            "stocks" => "T",
            "forex" => "CAS",
            _ => "XT",
        };
        let params: Vec<String> = symbols
            .iter()
            .map(|s| {
                if asset_class == "forex" {
                    format!("{}.{}", event_prefix, s.replace('-', "/").to_uppercase())
                } else {
                    format!("{}.{}", event_prefix, s)
                }
            })
            .collect();
        let sub_msg = format!(r#"{{"action":"subscribe","params":"{}"}}"#, params.join(","));
        writer.send(Message::Text(sub_msg)).await
            .map_err(|e| anyhow::anyhow!("Failed to send subscribe message: {}", e))?;
        log_info!(MASSIVE_LOGGER, "Subscribed to Massive {} trade events for {} symbols", event_prefix, symbols.len());

        let config_path = env::var("CONFIG_PATH")
            .map_err(|_| anyhow::anyhow!("CONFIG_PATH environment variable must be set"))?;
        let config = Config::new(&config_path)?;

        // Create publisher (or None in database-only / no-broker mode)
        let database_only_mode = env::var("DATABASE_ONLY_MODE")
            .unwrap_or_default()
            .to_lowercase() == "true"
            || env::var("DISABLE_MESSAGE_BROKER")
                .unwrap_or_default()
                .to_lowercase() == "true";

        let publisher = if database_only_mode {
            log_info!(MASSIVE_LOGGER, "DATABASE-ONLY MODE: Message broker publishing disabled");
            None
        } else {
            let addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
            let publisher_config = PublisherConfig::new(&addr);
            Some(Arc::new(tokio::sync::Mutex::new(
                Publisher::new(publisher_config)
                    .map_err(|e| anyhow::anyhow!("Failed to create publisher: {:?}", e))?,
            )))
        };

        // Mark as connected
        is_connected.store(true, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        last_message_time.store(now_ms, Ordering::Relaxed);

        Ok(Self {
            config,
            asset_class,
            venue,
            writer: RwLock::new(writer),
            reader: RwLock::new(reader),
            publisher,
            is_connected,
            last_message_time,
            bar_aggregator: BarAggregator::new(1000),
        })
    }

    /// Resolve the publish topic for a market-data kind ("trades", "bars").
    ///
    /// Venue-scoped names win: SignalEngine subscribes to
    /// `market.data.{deployment_exchange}.{kind}`, so data MUST be published
    /// under the venue that requested it or no strategy ever sees a tick
    /// (the silent-starvation failure behind deployment 865f45ff). The
    /// config `topic_routing` map remains as the legacy fallback for
    /// handlers constructed without a venue.
    fn data_topic(&self, kind: &str) -> String {
        if !self.venue.is_empty() {
            return format!("market.data.{}.{}", self.venue.to_lowercase(), kind);
        }
        self.config
            .topic_routing
            .get(kind)
            .cloned()
            .unwrap_or_else(|| format!("market.data.massive.{}", kind))
    }

    /// Decode a Massive `XT` message and publish it as a protobuf `Trade`
    /// to `market.data.massive.trades`.
    async fn process_trade(&self, trade: MassiveTrade) {
        let side = if trade.c.contains(&2) {
            "buy"
        } else if trade.c.contains(&1) {
            "sell"
        } else {
            "unknown"
        };

        let trade_msg = Trade {
            symbol: trade.pair.clone(),
            exchange: "massive".to_string(),
            side: side.to_string(),
            price: trade.p as f32,
            quantity: trade.s as f32,
            qty: trade.s as f32,
            ord_type: "market".to_string(),
            timestamp: trade.t / 1000,
            trade_id: trade.i.unwrap_or_default(),
        };

        let trades_payload = Trades {
            trades: vec![trade_msg],
        };

        let market_message = MarketMessage {
            market_id: "massive_trade".to_string(),
            payload: Some(market_message::Payload::TradesPayload(trades_payload)),
        };

        // Venue-scoped topic (falls back to config routing when venue is empty)
        let topic_name = self.data_topic("trades");

        // Use topics[0] as the publish-request wrapper topic
        let topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.massive".to_string());

        let request = PublishRequest {
            topic,
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        if let Some(publisher_ref) = &self.publisher {
            match publisher_ref.lock().await.publish(prost::Message::encode_to_vec(&request), &topic_name) {
                Ok(_) => {
                    // Update health timestamp on successful publish
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    self.last_message_time.store(now_ms, Ordering::Relaxed);
                }
                Err(e) => {
                    log_error!(MASSIVE_LOGGER, "Failed to publish trade for {}: {:?}", trade.pair, e);
                }
            }

            // Feed tick into the bar aggregator; publish a completed bar if one closed.
            if let Some(bar) = self.bar_aggregator.ingest(
                &trade.pair,
                "massive",
                trade.p,
                trade.s,
                trade.t,
            ) {
                self.publish_bar(publisher_ref, bar).await;
            }
        } else {
            log_debug!(MASSIVE_LOGGER, "STREAM-ONLY MODE: skipping publish for {}", trade.pair);
        }
    }

    /// Publish a completed [`Bar`] to `market.data.massive.bars`.
    async fn publish_bar(&self, publisher_ref: &Arc<tokio::sync::Mutex<Publisher>>, bar: Bar) {
        let bars_topic = self.data_topic("bars");

        let market_message = MarketMessage {
            market_id: "massive_bar".to_string(),
            payload: Some(market_message::Payload::Bar(bar)),
        };

        let envelope_topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.massive".to_string());

        let request = PublishRequest {
            topic: envelope_topic,
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        if let Err(e) = publisher_ref.lock().await.publish(prost::Message::encode_to_vec(&request), &bars_topic) {
            log_error!(MASSIVE_LOGGER, "Failed to publish bar: {:?}", e);
        }
    }

    /// Decode a Massive `T` message (stock trade) and publish it as a protobuf `Trade`.
    async fn process_stock_trade(&self, trade: MassiveStockTrade) {
        // Polygon stock trade conditions don't map to a simple buy/sell side.
        // Use "unknown" until we add a condition-code lookup table.
        let trade_msg = Trade {
            symbol: trade.sym.clone(),
            exchange: "massive".to_string(),
            side: "unknown".to_string(),
            price: trade.p as f32,
            quantity: trade.s as f32,
            qty: trade.s as f32,
            ord_type: "market".to_string(),
            timestamp: trade.t / 1000,
            trade_id: trade.i.map(|i| i.to_string()).unwrap_or_default(),
        };

        let trades_payload = Trades {
            trades: vec![trade_msg],
        };

        let market_message = MarketMessage {
            market_id: "massive_stock_trade".to_string(),
            payload: Some(market_message::Payload::TradesPayload(trades_payload)),
        };

        // SignalEngine subscribes to market.data.{venue}.trades regardless of
        // asset class; the stocks-specific name is legacy (venue empty).
        let topic_name = if !self.venue.is_empty() {
            self.data_topic("trades")
        } else {
            self.config
                .topic_routing
                .get("stocks_trades")
                .or_else(|| self.config.topic_routing.get("trades"))
                .cloned()
                .unwrap_or_else(|| "market.data.massive.stocks.trades".to_string())
        };

        let topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.massive".to_string());

        let request = PublishRequest {
            topic,
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        if let Some(publisher_ref) = &self.publisher {
            match publisher_ref.lock().await.publish(prost::Message::encode_to_vec(&request), &topic_name) {
                Ok(_) => {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    self.last_message_time.store(now_ms, Ordering::Relaxed);
                }
                Err(e) => {
                    log_error!(MASSIVE_LOGGER, "Failed to publish stock trade for {}: {:?}", trade.sym, e);
                }
            }

            if let Some(bar) = self.bar_aggregator.ingest(
                &trade.sym,
                "massive",
                trade.p,
                trade.s,
                trade.t,
            ) {
                self.publish_bar(publisher_ref, bar).await;
            }
        } else {
            log_debug!(MASSIVE_LOGGER, "STREAM-ONLY MODE: skipping publish for {}", trade.sym);
        }
    }

    /// Decode a Massive `CAS` message (forex per-second aggregate) and publish it
    /// as a synthesized pseudo-trade.
    ///
    /// Polygon's forex feed has no trade events, so each per-second aggregate
    /// becomes one Trade: price = close, quantity = volume (tick count),
    /// timestamp = aggregate end. Limitation: downstream consumers see at most
    /// one print per second per pair (the closing rate of that second).
    async fn process_forex_agg(&self, agg: MassiveForexAgg) {
        // Normalize "EUR/USD" → platform format "EUR-USD"
        let symbol = agg.pair.replace('/', "-");

        let trade_msg = Trade {
            symbol: symbol.clone(),
            exchange: "massive".to_string(),
            side: "unknown".to_string(),
            price: agg.c as f32,
            quantity: agg.v as f32,
            qty: agg.v as f32,
            ord_type: "market".to_string(),
            timestamp: agg.end_ms / 1000,
            trade_id: String::new(),
        };

        let trades_payload = Trades {
            trades: vec![trade_msg],
        };

        let market_message = MarketMessage {
            market_id: "massive_forex_agg".to_string(),
            payload: Some(market_message::Payload::TradesPayload(trades_payload)),
        };

        // SignalEngine subscribes to market.data.{venue}.trades regardless of
        // asset class; the forex-specific name is legacy (venue empty).
        let topic_name = if !self.venue.is_empty() {
            self.data_topic("trades")
        } else {
            self.config
                .topic_routing
                .get("forex_trades")
                .or_else(|| self.config.topic_routing.get("trades"))
                .cloned()
                .unwrap_or_else(|| "market.data.massive.forex.trades".to_string())
        };

        let topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.massive".to_string());

        let request = PublishRequest {
            topic,
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };

        if let Some(publisher_ref) = &self.publisher {
            match publisher_ref.lock().await.publish(prost::Message::encode_to_vec(&request), &topic_name) {
                Ok(_) => {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    self.last_message_time.store(now_ms, Ordering::Relaxed);
                }
                Err(e) => {
                    log_error!(MASSIVE_LOGGER, "Failed to publish forex agg for {}: {:?}", symbol, e);
                }
            }

            if let Some(bar) = self.bar_aggregator.ingest(
                &symbol,
                "massive",
                agg.c,
                agg.v,
                agg.end_ms,
            ) {
                self.publish_bar(publisher_ref, bar).await;
            }
        } else {
            log_debug!(MASSIVE_LOGGER, "STREAM-ONLY MODE: skipping publish for {}", symbol);
        }
    }

    /// Dispatch a single parsed JSON object from the Massive array payload.
    async fn dispatch_event(&self, event: &Value) {
        let ev = match event.get("ev").and_then(Value::as_str) {
            Some(e) => e,
            None => return,
        };

        match ev {
            "XT" => {
                match serde_json::from_value::<MassiveTrade>(event.clone()) {
                    Ok(trade) => {
                        log_debug!(MASSIVE_LOGGER, "Trade: {} @ {}", trade.pair, trade.p);
                        self.process_trade(trade).await;
                    }
                    Err(e) => {
                        log_error!(MASSIVE_LOGGER, "Failed to deserialize XT event: {}", e);
                    }
                }
            }
            "T" => {
                match serde_json::from_value::<MassiveStockTrade>(event.clone()) {
                    Ok(trade) => {
                        log_debug!(MASSIVE_LOGGER, "Stock trade: {} @ {}", trade.sym, trade.p);
                        self.process_stock_trade(trade).await;
                    }
                    Err(e) => {
                        log_error!(MASSIVE_LOGGER, "Failed to deserialize T event: {}", e);
                    }
                }
            }
            "CAS" => {
                match serde_json::from_value::<MassiveForexAgg>(event.clone()) {
                    Ok(agg) => {
                        log_debug!(MASSIVE_LOGGER, "Forex agg: {} close {}", agg.pair, agg.c);
                        self.process_forex_agg(agg).await;
                    }
                    Err(e) => {
                        log_error!(MASSIVE_LOGGER, "Failed to deserialize CAS event: {}", e);
                    }
                }
            }
            "status" => {
                let status = event.get("status").and_then(Value::as_str).unwrap_or("?");
                let message = event.get("message").and_then(Value::as_str).unwrap_or("");
                log_info!(MASSIVE_LOGGER, "Status [{}]: {}", status, message);
            }
            other => {
                log_debug!(MASSIVE_LOGGER, "Unhandled Massive event type: {}", other);
            }
        }
    }
}

#[async_trait]
impl EndpointHandler for MassiveWebSocketHandler {
    async fn listen(self: Arc<Self>, mut shutdown_rx: Receiver<()>) -> Result<()> {
        let handler = Arc::clone(&self);

        let handle = tokio::spawn(async move {
            let cpu_manager = CpuAffinityManager::new();

            if let Err(e) = cpu_manager.pin_websocket_handler() {
                log_warn!(MASSIVE_LOGGER, "Failed to set CPU affinity: {}", e);
            }
            if let Err(e) = set_high_priority() {
                log_warn!(MASSIVE_LOGGER, "Failed to set high priority: {}", e);
            }
            if let Err(e) = prefault_stack(8) {
                log_warn!(MASSIVE_LOGGER, "Failed to prefault stack: {}", e);
            }

            tokio::select! {
                result = async {
                    loop {
                        let message = handler.reader.write().await.next().await;

                        match message {
                            Some(Ok(Message::Text(text))) => {
                                // Update health timestamp on any incoming message
                                let now_ms = SystemTime::now()
                                    .duration_since(UNIX_EPOCH)
                                    .unwrap_or_default()
                                    .as_millis() as u64;
                                handler.last_message_time.store(now_ms, Ordering::Relaxed);

                                // Massive delivers messages as JSON arrays
                                match serde_json::from_str::<Vec<Value>>(&text) {
                                    Ok(events) => {
                                        for event in &events {
                                            handler.dispatch_event(event).await;
                                        }
                                    }
                                    Err(e) => {
                                        log_error!(MASSIVE_LOGGER, "Failed to parse message as JSON array: {} — raw: {}", e, &text[..text.len().min(200)]);
                                    }
                                }
                            }
                            Some(Ok(Message::Ping(data))) => {
                                if let Err(e) = handler.writer.write().await.send(Message::Pong(data)).await {
                                    log_error!(MASSIVE_LOGGER, "Failed to send pong: {}", e);
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                log_warn!(MASSIVE_LOGGER, "Massive WebSocket connection closed by server");
                                handler.is_connected.store(false, Ordering::Relaxed);
                                return Err(anyhow::anyhow!("Massive WebSocket closed by server"));
                            }
                            Some(Ok(_)) => {
                                // Binary / other frame types — ignore
                            }
                            Some(Err(e)) => {
                                log_error!(MASSIVE_LOGGER, "WebSocket error: {}", e);
                                handler.is_connected.store(false, Ordering::Relaxed);
                                return Err(anyhow::anyhow!("Massive WebSocket error: {}", e));
                            }
                            None => {
                                log_warn!(MASSIVE_LOGGER, "Massive WebSocket stream ended unexpectedly");
                                handler.is_connected.store(false, Ordering::Relaxed);
                                return Err(anyhow::anyhow!("Massive WebSocket connection closed unexpectedly"));
                            }
                        }
                    }
                } => {
                    log_info!(MASSIVE_LOGGER, "Massive WebSocket listener loop completed");
                    result?;
                }

                _ = shutdown_rx.recv() => {
                    log_info!(MASSIVE_LOGGER, "Shutting down MassiveWebSocketHandler");
                    // Send unsubscribe for the event prefix this handler subscribed to
                    let prefix = match handler.asset_class.as_str() {
                        "stocks" => "T",
                        "forex" => "CAS",
                        _ => "XT",
                    };
                    let unsub = format!(r#"{{"action":"unsubscribe","params":"{}.*"}}"#, prefix);
                    if let Err(e) = handler.writer.write().await.send(Message::Text(unsub)).await {
                        log_warn!(MASSIVE_LOGGER, "Failed to send unsubscribe: {}", e);
                    }
                }
            }

            Ok::<(), anyhow::Error>(())
        });

        handle
            .await
            .map_err(|e| anyhow::anyhow!("MassiveWebSocketHandler task panicked: {}", e))??;

        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Returns the Massive WebSocket feed URL for the given asset class.
///
/// Reads `MASSIVE_FEED_URL` from the environment to allow override.
/// Defaults: crypto → `wss://socket.polygon.io/crypto`, stocks → `wss://socket.polygon.io/stocks`.
/// Returns the Massive WebSocket feed URL for the given asset class.
///
/// Reads `MASSIVE_FEED_URL` from the environment to allow override.
/// Defaults: crypto → `wss://socket.polygon.io/crypto`,
/// stocks → `wss://socket.polygon.io/stocks`,
/// forex → `wss://socket.polygon.io/forex`.
pub fn get_massive_feed_url(asset_class: &str) -> String {
    if let Ok(url) = env::var("MASSIVE_FEED_URL") {
        return url;
    }
    match asset_class {
        "stocks" => "wss://socket.polygon.io/stocks".to_string(),
        "forex" => "wss://socket.polygon.io/forex".to_string(),
        _ => "wss://socket.polygon.io/crypto".to_string(),
    }
}
