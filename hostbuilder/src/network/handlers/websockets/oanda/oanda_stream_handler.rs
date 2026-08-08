use anyhow::Result;
use async_trait::async_trait;
use config::Config;
use futures_util::StreamExt;
use protocol::broker::messages::{
    market_message, publish_request, Bar, MarketMessage, PublishRequest, Trade, Trades,
};
use publisher::{Publisher, PublisherConfig};
use serde::Deserialize;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{broadcast::Receiver, RwLock};

use crate::network::endpoint::EndpointHandler;
use crate::network::handlers::websockets::massive::BarAggregator;
use crate::infrastructure::logging_facade::OANDA_LOGGER;
use crate::{log_info, log_warn, log_error, log_debug};

/// Nominal per-level "depth" carried on Oanda's synthesized pseudo-trades
/// (see `OandaStreamHandler::process_tick`'s doc). Oanda has no real L2
/// feed, so this is a placeholder representing an assumed-deep-liquidity FX
/// market -- large enough that downstream availability-scaling (currently
/// 70% per level in `SimulationEngine`'s paper-fill matching) never becomes
/// the binding constraint for any realistic paper position size.
const NOMINAL_SYNTHETIC_DEPTH: f32 = 1_000_000.0;

// =============================================================================
// Message model
// =============================================================================

/// One side (bid or ask) of an Oanda `PRICE` tick.
#[derive(Debug, Deserialize)]
struct OandaPriceLevel {
    price: String,
}

/// A line from Oanda's v20 pricing stream
/// (`GET /v3/accounts/{id}/pricing/stream`). Two shapes share this endpoint:
/// `{"type":"PRICE", instrument, time, bids, asks, ...}` ticks, and
/// `{"type":"HEARTBEAT", time}` keepalives with no `instrument`/price data.
/// Both are modeled here with optional fields so one deserializer handles
/// either; `instrument.is_none()` is what actually distinguishes a heartbeat.
#[derive(Debug, Deserialize)]
struct OandaPriceTick {
    #[serde(rename = "type")]
    #[allow(dead_code)]
    kind: Option<String>,
    instrument: Option<String>,
    time: Option<String>,
    #[serde(default)]
    bids: Vec<OandaPriceLevel>,
    #[serde(default)]
    asks: Vec<OandaPriceLevel>,
}

// =============================================================================
// Handler struct
// =============================================================================

pub struct OandaStreamHandler {
    /// Execution venue this feed serves (always "oanda" today, but kept
    /// venue-scoped like the Massive handler so SignalEngine's per-deployment
    /// topic subscription (`market.data.{venue}.*`) keeps working unchanged).
    venue: String,
    /// Oanda-format instruments (underscore-separated, e.g. "USD_ZAR") this
    /// connection subscribes to.
    instruments: Vec<String>,
    token: String,
    account_id: String,
    stream_base_url: String,
    config: Config,
    publisher: Option<Arc<tokio::sync::Mutex<Publisher>>>,
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    bar_aggregator: BarAggregator,
    /// Guards against overlapping stream_once() calls holding the same
    /// http_client across reconnects -- reqwest::Client is cheap to clone
    /// (Arc internally), so this is just for symmetry with the WS handler's
    /// RwLock-guarded reader; a plain field would do too.
    #[allow(dead_code)]
    http_client: RwLock<reqwest::Client>,
}

impl OandaStreamHandler {
    /// Build a handler for Oanda's pricing stream.
    ///
    /// `symbols` are in this platform's dash convention (e.g. "USD-ZAR") and
    /// are converted to Oanda's underscore convention (e.g. "USD_ZAR") here.
    ///
    /// Reads `OANDA_API_KEY` (personal access token, Bearer auth) and
    /// `OANDA_PRACTICE_ACCOUNT_ID` from the environment -- the same
    /// credentials `broker_tradability_service.rs` already uses successfully
    /// in BacktestingEngine, sourced from the same Azure Key Vault secrets.
    /// Reads `CONFIG_PATH` for the DataEngine config (publisher address).
    pub async fn new(
        symbols: Vec<String>,
        venue: String,
        is_connected: Arc<AtomicBool>,
        last_message_time: Arc<AtomicU64>,
    ) -> Result<Self> {
        let token = std::env::var("OANDA_API_KEY").unwrap_or_default();
        let account_id = std::env::var("OANDA_PRACTICE_ACCOUNT_ID").unwrap_or_default();
        if token.is_empty() || account_id.is_empty() {
            return Err(anyhow::anyhow!(
                "OANDA_API_KEY / OANDA_PRACTICE_ACCOUNT_ID environment variables must be set"
            ));
        }

        let instruments: Vec<String> = symbols.iter().map(|s| to_oanda_instrument(s)).collect();

        let config_path = std::env::var("CONFIG_PATH")
            .map_err(|_| anyhow::anyhow!("CONFIG_PATH environment variable must be set"))?;
        let config = Config::new(&config_path)?;

        let database_only_mode = std::env::var("DATABASE_ONLY_MODE")
            .unwrap_or_default()
            .to_lowercase() == "true"
            || std::env::var("DISABLE_MESSAGE_BROKER")
                .unwrap_or_default()
                .to_lowercase() == "true";

        let publisher = if database_only_mode {
            log_info!(OANDA_LOGGER, "DATABASE-ONLY MODE: Message broker publishing disabled");
            None
        } else {
            let addr = format!("{}:{}", config.message_broker.address, config.message_broker.port);
            let publisher_config = PublisherConfig::new(&addr);
            Some(Arc::new(tokio::sync::Mutex::new(
                Publisher::new(publisher_config)
                    .map_err(|e| anyhow::anyhow!("Failed to create publisher: {:?}", e))?,
            )))
        };

        Ok(Self {
            venue,
            instruments,
            token,
            account_id,
            stream_base_url: get_oanda_stream_url(),
            config,
            publisher,
            is_connected,
            last_message_time,
            bar_aggregator: BarAggregator::new(1000),
            http_client: RwLock::new(reqwest::Client::new()),
        })
    }

    /// Resolve the publish topic for a market-data kind ("trades", "bars"),
    /// mirroring `MassiveWebSocketHandler::data_topic` -- SignalEngine
    /// subscribes per deployment venue, not per data provider.
    fn data_topic(&self, kind: &str) -> String {
        if !self.venue.is_empty() {
            return format!("market.data.{}.{}", self.venue.to_lowercase(), kind);
        }
        self.config
            .topic_routing
            .get(kind)
            .cloned()
            .unwrap_or_else(|| format!("market.data.oanda.{}", kind))
    }

    /// Decode one pricing-stream line and, if it's a real price tick (not a
    /// heartbeat), publish a synthesized pseudo-trade at the bid/ask
    /// midpoint. Oanda streams quotes, not discrete trades -- there is no
    /// real "quantity" here, so quantity/qty carry a nominal placeholder
    /// (`NOMINAL_SYNTHETIC_DEPTH`) representing an assumed-deep-liquidity FX
    /// market, not a real depth reading.
    ///
    /// This nominal value previously used `1.0`, which corrupted a live
    /// paper deployment in production: SignalEngine's synthetic
    /// ±1bp-top-of-book bootstrap (`datahandler::process_trade`) carries
    /// this quantity straight through as each book level's depth, and
    /// `SimulationEngine`'s mock exchange applies a 70%-availability factor
    /// per level when matching (`1.0 * 0.7 = 0.7`) -- so every fill clamped
    /// to a flat 0.7 units regardless of the requested order size or the
    /// pair's price, once the requested size (any realistic position, e.g.
    /// ~150 units at typical sizing) exceeded that nominal depth. A large
    /// nominal value here keeps the availability-scaled depth comfortably
    /// above any realistic paper position size on this platform.
    async fn process_tick(&self, tick: OandaPriceTick) {
        let Some(instrument) = tick.instrument else {
            // Heartbeat: no price data, but it proves the connection is alive.
            return;
        };

        let Some(mid) = midpoint_from_levels(&tick.bids, &tick.asks) else {
            return;
        };

        let symbol = to_platform_symbol(&instrument);
        let timestamp = tick
            .time
            .as_deref()
            .and_then(parse_oanda_timestamp)
            .unwrap_or_else(|| chrono::Utc::now().timestamp());

        let trade_msg = Trade {
            symbol: symbol.clone(),
            exchange: "oanda".to_string(),
            side: "unknown".to_string(),
            price: mid as f32,
            quantity: NOMINAL_SYNTHETIC_DEPTH,
            qty: NOMINAL_SYNTHETIC_DEPTH,
            ord_type: "market".to_string(),
            timestamp,
            trade_id: String::new(),
        };

        let trades_payload = Trades { trades: vec![trade_msg] };
        let market_message = MarketMessage {
            market_id: "oanda_price".to_string(),
            payload: Some(market_message::Payload::TradesPayload(trades_payload)),
        };

        let topic_name = self.data_topic("trades");
        let envelope_topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.oanda".to_string());

        let request = PublishRequest {
            topic: envelope_topic,
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
                    log_error!(OANDA_LOGGER, "Failed to publish price for {}: {:?}", symbol, e);
                }
            }

            if let Some(bar) = self.bar_aggregator.ingest(&symbol, "oanda", mid, 1.0, timestamp * 1000) {
                self.publish_bar(publisher_ref, bar).await;
            }
        } else {
            log_debug!(OANDA_LOGGER, "STREAM-ONLY MODE: skipping publish for {}", symbol);
        }
    }

    /// Publish a completed [`Bar`] to `market.data.oanda.bars`.
    async fn publish_bar(&self, publisher_ref: &Arc<tokio::sync::Mutex<Publisher>>, bar: Bar) {
        let bars_topic = self.data_topic("bars");
        let market_message = MarketMessage {
            market_id: "oanda_bar".to_string(),
            payload: Some(market_message::Payload::Bar(bar)),
        };
        let envelope_topic = self
            .config
            .topics
            .first()
            .cloned()
            .unwrap_or_else(|| "market.data.oanda".to_string());
        let request = PublishRequest {
            topic: envelope_topic,
            payload: Some(publish_request::Payload::MarketPayload(market_message)),
        };
        if let Err(e) = publisher_ref.lock().await.publish(prost::Message::encode_to_vec(&request), &bars_topic) {
            log_error!(OANDA_LOGGER, "Failed to publish bar: {:?}", e);
        }
    }

    /// Connect once and read the stream until it drops or errors. Returns
    /// `Ok(true)` if the connection was successfully established (regardless
    /// of how long it lasted before dropping) so the caller's reconnect
    /// backoff can reset on a clean connect but keep growing on repeated
    /// connect failures (bad credentials, DNS, etc.).
    async fn stream_once(&self) -> Result<bool> {
        let url = format!(
            "{}/v3/accounts/{}/pricing/stream",
            self.stream_base_url, self.account_id
        );

        let client = self.http_client.read().await;
        let response = client
            .get(&url)
            .bearer_auth(&self.token)
            .query(&[("instruments", self.instruments.join(","))])
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to connect to Oanda pricing stream: {}", e))?;
        drop(client);

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Oanda pricing stream returned {}: {}",
                status,
                &body[..body.len().min(500)]
            ));
        }

        log_info!(
            OANDA_LOGGER,
            "Connected to Oanda pricing stream for {} instrument(s): {}",
            self.instruments.len(),
            self.instruments.join(", ")
        );
        self.is_connected.store(true, Ordering::Relaxed);
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        self.last_message_time.store(now_ms, Ordering::Relaxed);

        let mut byte_stream = response.bytes_stream();
        let mut line_buf: Vec<u8> = Vec::new();

        while let Some(chunk) = byte_stream.next().await {
            let chunk = chunk.map_err(|e| anyhow::anyhow!("Oanda pricing stream read error: {}", e))?;
            line_buf.extend_from_slice(&chunk);

            while let Some(newline_pos) = line_buf.iter().position(|&b| b == b'\n') {
                let line: Vec<u8> = line_buf.drain(..=newline_pos).collect();
                let line = &line[..line.len().saturating_sub(1)]; // strip trailing \n
                if line.is_empty() {
                    continue;
                }

                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                self.last_message_time.store(now_ms, Ordering::Relaxed);

                match serde_json::from_slice::<OandaPriceTick>(line) {
                    Ok(tick) => self.process_tick(tick).await,
                    Err(e) => {
                        let raw = String::from_utf8_lossy(line);
                        log_warn!(
                            OANDA_LOGGER,
                            "Failed to parse pricing stream line: {} — raw: {}",
                            e,
                            &raw[..raw.len().min(200)]
                        );
                    }
                }
            }
        }

        log_warn!(OANDA_LOGGER, "Oanda pricing stream ended (connection closed)");
        self.is_connected.store(false, Ordering::Relaxed);
        Ok(true)
    }
}

#[async_trait]
impl EndpointHandler for OandaStreamHandler {
    async fn listen(self: Arc<Self>, mut shutdown_rx: Receiver<()>) -> Result<()> {
        let handler = Arc::clone(&self);

        let handle = tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            const MAX_BACKOFF: Duration = Duration::from_secs(30);

            loop {
                tokio::select! {
                    result = handler.stream_once() => {
                        match result {
                            Ok(_) => {
                                backoff = Duration::from_secs(1);
                            }
                            Err(e) => {
                                log_error!(OANDA_LOGGER, "Oanda pricing stream error: {}", e);
                                handler.is_connected.store(false, Ordering::Relaxed);
                            }
                        }
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(MAX_BACKOFF);
                    }
                    _ = shutdown_rx.recv() => {
                        log_info!(OANDA_LOGGER, "Shutting down OandaStreamHandler");
                        return Ok::<(), anyhow::Error>(());
                    }
                }
            }
        });

        handle
            .await
            .map_err(|e| anyhow::anyhow!("OandaStreamHandler task panicked: {}", e))??;

        Ok(())
    }
}

// =============================================================================
// Helpers
// =============================================================================

/// Returns Oanda's v20 streaming API base URL.
///
/// Reads `OANDA_STREAM_URL` from the environment to allow override (e.g. for
/// a future live-account cutover to `stream-fxtrade.oanda.com`); defaults to
/// the practice environment, matching the practice-only credentials this
/// platform already provisions for Oanda (see `broker_tradability_service.rs`).
pub fn get_oanda_stream_url() -> String {
    std::env::var("OANDA_STREAM_URL")
        .unwrap_or_else(|_| "https://stream-fxpractice.oanda.com".to_string())
}

/// Converts this platform's dash symbol convention (e.g. "USD-ZAR", or
/// "USD/ZAR") to Oanda's underscore instrument convention (e.g. "USD_ZAR").
fn to_oanda_instrument(symbol: &str) -> String {
    symbol.trim().to_uppercase().replace('-', "_").replace('/', "_")
}

/// Converts an Oanda instrument name (e.g. "USD_ZAR") back to this
/// platform's dash convention (e.g. "USD-ZAR").
fn to_platform_symbol(instrument: &str) -> String {
    instrument.replace('_', "-")
}

/// Computes the mid-price from the best bid/ask levels of a pricing tick.
/// Falls back to whichever single side is present (Oanda can send
/// one-sided updates during low liquidity); `None` if both sides are empty.
fn midpoint_from_levels(bids: &[OandaPriceLevel], asks: &[OandaPriceLevel]) -> Option<f64> {
    let bid: Option<f64> = bids.first().and_then(|b| b.price.parse().ok());
    let ask: Option<f64> = asks.first().and_then(|a| a.price.parse().ok());
    match (bid, ask) {
        (Some(b), Some(a)) => Some((b + a) / 2.0),
        (Some(b), None) => Some(b),
        (None, Some(a)) => Some(a),
        (None, None) => None,
    }
}

/// Parses Oanda's RFC3339 pricing-tick timestamp (e.g.
/// "2020-06-25T20:36:36.643927416Z") into Unix seconds.
fn parse_oanda_timestamp(time: &str) -> Option<i64> {
    chrono::DateTime::parse_from_rfc3339(time).ok().map(|dt| dt.timestamp())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn to_oanda_instrument_converts_dash_to_underscore() {
        assert_eq!(to_oanda_instrument("usd-zar"), "USD_ZAR");
        assert_eq!(to_oanda_instrument("USD/ZAR"), "USD_ZAR");
        assert_eq!(to_oanda_instrument(" usd-zar "), "USD_ZAR");
    }

    #[test]
    fn to_platform_symbol_converts_underscore_to_dash() {
        assert_eq!(to_platform_symbol("USD_ZAR"), "USD-ZAR");
    }

    #[test]
    fn midpoint_from_levels_averages_bid_and_ask() {
        let bids = vec![OandaPriceLevel { price: "1.1000".to_string() }];
        let asks = vec![OandaPriceLevel { price: "1.1002".to_string() }];
        let mid = midpoint_from_levels(&bids, &asks).unwrap();
        assert!((mid - 1.1001).abs() < 1e-9, "expected ~1.1001, got {mid}");
    }

    #[test]
    fn midpoint_from_levels_falls_back_to_bid_only() {
        let bids = vec![OandaPriceLevel { price: "1.1000".to_string() }];
        assert_eq!(midpoint_from_levels(&bids, &[]), Some(1.1000));
    }

    #[test]
    fn midpoint_from_levels_falls_back_to_ask_only() {
        let asks = vec![OandaPriceLevel { price: "1.1002".to_string() }];
        assert_eq!(midpoint_from_levels(&[], &asks), Some(1.1002));
    }

    #[test]
    fn midpoint_from_levels_none_when_both_sides_empty() {
        // The shape of a HEARTBEAT line if it were ever mis-decoded as a
        // price tick: no real quote data on either side.
        assert_eq!(midpoint_from_levels(&[], &[]), None);
    }

    #[test]
    fn parse_oanda_timestamp_parses_nanosecond_rfc3339() {
        let ts = parse_oanda_timestamp("2020-06-25T20:36:36.643927416Z");
        assert!(ts.is_some());
    }

    #[test]
    fn parse_oanda_timestamp_rejects_garbage() {
        assert_eq!(parse_oanda_timestamp("not-a-timestamp"), None);
    }

    #[test]
    fn heartbeat_tick_has_no_instrument() {
        // Confirms the exact shape process_tick relies on to skip
        // heartbeats: no "instrument" field at all.
        let json = r#"{"type":"HEARTBEAT","time":"2020-06-25T20:36:36.643927416Z"}"#;
        let tick: OandaPriceTick = serde_json::from_str(json).unwrap();
        assert!(tick.instrument.is_none());
    }

    #[test]
    fn price_tick_decodes_instrument_and_levels() {
        let json = r#"{
            "type":"PRICE",
            "instrument":"USD_ZAR",
            "time":"2020-06-25T20:36:36.643927416Z",
            "bids":[{"price":"18.5000","liquidity":1000000}],
            "asks":[{"price":"18.5100","liquidity":1000000}]
        }"#;
        let tick: OandaPriceTick = serde_json::from_str(json).unwrap();
        assert_eq!(tick.instrument.as_deref(), Some("USD_ZAR"));
        let mid = midpoint_from_levels(&tick.bids, &tick.asks).unwrap();
        assert!((mid - 18.505).abs() < 1e-9, "expected ~18.505, got {mid}");
    }

    #[test]
    fn get_oanda_stream_url_defaults_to_practice() {
        // SAFETY: test-only env manipulation, single-threaded within this test.
        unsafe { std::env::remove_var("OANDA_STREAM_URL"); }
        assert_eq!(get_oanda_stream_url(), "https://stream-fxpractice.oanda.com");
    }
}
