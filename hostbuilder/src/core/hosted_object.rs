//! HostedObject implementation
//! 
//! This module contains the main application object that orchestrates
//! all components of the DataEngine.
//!
//! # Demand-Driven Architecture
//! 
//! DataEngine only connects to exchange WebSockets when SignalEngine has active
//! strategies that need market data. This ensures resources are only consumed
//! when strategies are actively running.
//!
//! ## Flow
//! 1. SignalEngine deploys a strategy
//! 2. SignalEngine publishes `MarketDataSubscribe` to MessageBroker
//! 3. DataEngine receives subscription request
//! 4. DataEngine connects to exchange WebSocket for requested symbols
//! 5. Data streams to MessageBroker for SignalEngine consumption
//! 6. When strategy deactivates, SignalEngine publishes `MarketDataUnsubscribe`
//! 7. If no more subscribers, DataEngine disconnects from exchange

use anyhow::Result;
use async_trait::async_trait;
use config::Config;
#[cfg(feature = "database")]
use databaseschema::create_connection_pool;
use dotenv::dotenv;
use mockall::automock;
use std::{
    collections::HashMap,
    env,
    sync::Arc,
    sync::atomic::{AtomicBool, AtomicU64},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
#[cfg(unix)]
use tokio::signal::unix::{signal, SignalKind};
use tokio::{signal, sync::broadcast::channel, sync::mpsc};

#[cfg(feature = "database")]
use crate::data::get_info::get_exchange;
use crate::{
    core::subscription_manager::{ConnectionEvent, SubscriptionManager},
    core::tenant_subscription_limits::SubscriptionTier,
    infrastructure::{
        monitoring::{PerformanceHealthChecker, SystemHealthMonitor, WebSocketHealthChecker},
        resilience::ErrorHandler,
    },
    network::{
        endpoint::EndpointHandler,
        handlers::websockets::{MassiveWebSocketHandler, get_massive_feed_url, OandaStreamHandler},
    },
    security::{SecurityConfig, SecurityManager},
    // Import logging macros (exported at crate root) and logger instance
    log_info, log_warn, log_error,
    infrastructure::logging_facade::MAIN_LOGGER,
};
#[cfg(feature = "database")]
use crate::infrastructure::monitoring::DatabaseHealthChecker;

#[automock]
#[async_trait]
pub trait HostedObjectTrait {
    async fn run(&mut self) -> Result<()>;
}

/// Cached exchange metadata loaded from the database at startup.
/// This allows the runtime event loop to operate without any database dependency.
#[cfg(feature = "database")]
pub struct ExchangeMetadata {
    pub exchange: crate::data::get_info::Exchange,
    pub securities: Vec<crate::data::get_info::Security>,
}

pub struct HostedObject {
    config: Config,
    // Database pool removed from runtime — metadata is cached at startup
    #[cfg(feature = "database")]
    exchange_metadata: HashMap<String, ExchangeMetadata>,
    _health_monitor: Arc<SystemHealthMonitor>,
    _security_manager: Arc<SecurityManager>,
    _error_handler: Arc<ErrorHandler>,
    // WebSocket health tracking
    ws_is_connected: Arc<AtomicBool>,
    ws_last_message: Arc<AtomicU64>,
    // MessageBroker address for subscription manager
    broker_address: String,
    // Tier-limits policy for the subscription manager -- see `TierLimits`.
    tier_limits: std::sync::Arc<dyn super::tenant_subscription_limits::TierLimits>,
}

impl HostedObject {
    pub async fn new(tier_limits: std::sync::Arc<dyn super::tenant_subscription_limits::TierLimits>) -> Result<Self> {
        dotenv().ok();
        let config_path = env::var("CONFIG_PATH")
            .map_err(|_| anyhow::anyhow!("CONFIG_PATH environment variable must be set"))?;
        let config = Config::new(&config_path)?;

        // Initialize performance monitoring
        let global_metrics = performance::metrics::global_metrics();

        // Initialize security manager
        let security_config = SecurityConfig::default();
        let security_manager = Arc::new(SecurityManager::new(security_config));

        // Initialize error handler
        let error_handler = Arc::new(ErrorHandler::new());

        // Initialize health monitoring
        let mut health_monitor = SystemHealthMonitor::new(Duration::from_secs(30));

        // Add health checkers
        let ws_is_connected = Arc::new(AtomicBool::new(false));
        let ws_last_message = Arc::new(AtomicU64::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        ));
        let ws_health_checker = Arc::new(WebSocketHealthChecker::new(
            Arc::clone(&ws_is_connected),
            Arc::clone(&ws_last_message),
        ));
        health_monitor.add_health_checker(ws_health_checker);

        // --- Database: cache metadata at startup, then drop the pool ---
        #[cfg(feature = "database")]
        let (exchange_metadata, db_health_checker) = {
            let postgres_pool = Arc::new(create_connection_pool());

            // Pre-cache exchange metadata so the runtime loop never touches the DB
            let mut exchange_metadata = HashMap::new();
            for ex_config in &config.exchanges {
                let exchange_name = ex_config.name.clone();
                match get_exchange(Arc::clone(&postgres_pool), exchange_name.clone()).await {
                    Ok(exchange) => {
                        // Collect all symbols from all websocket channels for this exchange
                        // Securities will be populated lazily when Connect events arrive
                        log_info!(
                            MAIN_LOGGER,
                            "Cached metadata for exchange: {}",
                            exchange_name
                        );
                        exchange_metadata.insert(
                            exchange_name.to_lowercase(),
                            ExchangeMetadata {
                                exchange,
                                securities: Vec::new(),
                            },
                        );
                    }
                    Err(e) => {
                        log_warn!(
                            MAIN_LOGGER,
                            "Failed to cache metadata for exchange {}: {} (will retry on connect)",
                            exchange_name,
                            e
                        );
                    }
                }
            }

            let db_health_checker = Arc::new(DatabaseHealthChecker::new(
                Arc::clone(&postgres_pool),
            ));
            // Pool is kept alive through the health checker Arc; runtime code never uses it directly
            (exchange_metadata, db_health_checker)
        };

        #[cfg(feature = "database")]
        health_monitor.add_health_checker(db_health_checker);

        let perf_health_checker =
            Arc::new(PerformanceHealthChecker::new(Arc::clone(global_metrics)));
        health_monitor.add_health_checker(perf_health_checker);

        let health_monitor = Arc::new(health_monitor);

        // Start background services
        health_monitor.start().await?;

        // Get MessageBroker address from environment
        let broker_address = env::var("MESSAGE_BROKER_ADDRESS")
            .unwrap_or_else(|_| "127.0.0.1:9999".to_string());

        Ok(Self {
            config,
            #[cfg(feature = "database")]
            exchange_metadata,
            _health_monitor: health_monitor,
            _security_manager: security_manager,
            _error_handler: error_handler,
            ws_is_connected,
            ws_last_message,
            broker_address,
            tier_limits,
        })
    }
}

#[async_trait]
impl HostedObjectTrait for HostedObject {
    async fn run(&mut self) -> Result<()> {
        log_info!(
            MAIN_LOGGER,
            "Starting DataEngine in DEMAND-DRIVEN mode - awaiting subscription requests on {}",
            self.broker_address
        );

        let (shutdown_tx, mut shutdown_rx) = channel::<()>(1);
        let (event_tx, mut event_rx) = mpsc::channel::<ConnectionEvent>(100);

        // Start subscription manager
        let mut subscription_manager = SubscriptionManager::new(self.broker_address.clone(), self.tier_limits.clone());
        subscription_manager.start(event_tx).await?;

        // Seed tenant subscription tiers from TENANT_TIERS env var.
        // Format: "uuid1:Tier,uuid2:Tier,..." e.g. "200d95b5-...:Enterprise,abc-...:Professional"
        // Tiers: Free | Starter | Professional | Enterprise (case-insensitive).
        // Without registration, tenants default to Free (only "trades" allowed, no orderbook).
        if let Ok(tenant_tiers) = env::var("TENANT_TIERS") {
            for entry in tenant_tiers.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                let mut parts = entry.splitn(2, ':');
                let uuid_str = parts.next().unwrap_or("").trim();
                let tier_str = parts.next().unwrap_or("").trim();
                let uuid = match uuid::Uuid::parse_str(uuid_str) {
                    Ok(u) => u,
                    Err(e) => {
                        log_warn!(
                            MAIN_LOGGER,
                            "TENANT_TIERS: invalid UUID '{}': {}",
                            uuid_str, e
                        );
                        continue;
                    }
                };
                let tier = match tier_str.to_lowercase().as_str() {
                    "free" => SubscriptionTier::Free,
                    "starter" => SubscriptionTier::Starter,
                    "professional" | "pro" => SubscriptionTier::Professional,
                    "enterprise" => SubscriptionTier::Enterprise,
                    other => {
                        log_warn!(
                            MAIN_LOGGER,
                            "TENANT_TIERS: unknown tier '{}' for tenant {}",
                            other, uuid
                        );
                        continue;
                    }
                };
                subscription_manager.register_tenant(uuid, tier);
                log_info!(
                    MAIN_LOGGER,
                    "Registered tenant {} at tier {:?}",
                    uuid, tier
                );
            }
        }

        // Track active connections
        let mut active_connections: HashMap<String, tokio::task::JoinHandle<Result<()>>> = HashMap::new();
        let mut connection_stop_channels: HashMap<String, mpsc::Sender<()>> = HashMap::new();

        // Clone values for the event processing loop
        let config = self.config.clone();
        let ws_is_connected = Arc::clone(&self.ws_is_connected);
        let ws_last_message = Arc::clone(&self.ws_last_message);

        // Spawn shutdown signal handler
        let shutdown_tx_clone = shutdown_tx.clone();
        tokio::spawn(async move {
            log_info!(MAIN_LOGGER, "Shutdown handler spawned and listening for signals");
            
            #[cfg(unix)]
            {
                match signal(SignalKind::terminate()) {
                    Ok(mut term_signal) => match term_signal.recv().await {
                        Some(_) => {
                            log_info!(MAIN_LOGGER, "Received SIGTERM, initiating graceful shutdown");
                            let _ = shutdown_tx_clone.send(());
                        }
                        None => {
                            log_error!(MAIN_LOGGER, "SIGTERM signal stream ended unexpectedly");
                        }
                    },
                    Err(e) => {
                        log_error!(MAIN_LOGGER, "Failed to register SIGTERM handler: {}", e);
                    }
                }
            }

            #[cfg(not(unix))]
            {
                match signal::ctrl_c().await {
                    Ok(_) => {
                        log_info!(MAIN_LOGGER, "Received Ctrl+C, initiating graceful shutdown");
                        let _ = shutdown_tx_clone.send(());
                    }
                    Err(e) => {
                        log_error!(MAIN_LOGGER, "Failed to register Ctrl+C handler: {}", e);
                    }
                }
            }
        });

        log_info!(MAIN_LOGGER, "DataEngine ready - waiting for market data subscription requests");

        // Event processing loop
        loop {
            tokio::select! {
                // Handle connection events from subscription manager
                Some(event) = event_rx.recv() => {
                    match event {
                        ConnectionEvent::Connect { exchange, symbols, data_types, orderbook_depth: _ } => {
                            log_info!(
                                MAIN_LOGGER,
                                "Received Connect event: exchange={}, symbols={:?}",
                                exchange,
                                symbols
                            );

                            // Find exchange config — exact match first, else fall
                            // back to the default data-provider entry. Most venue
                            // names (e.g. kraken) only route fees/fills in
                            // SignalEngine and have no real data connector, so their
                            // market data is served by the consolidated Massive
                            // (Polygon) feed regardless of execution venue —
                            // requiring an exact name here made every such venue
                            // absent from the config silently produce NO data for
                            // its strategies. Oanda is the one exception with a
                            // real, venue-specific connector (see the
                            // ex_config.name == "oanda" branch below); its config
                            // entry always exact-matches, so it never falls through
                            // to this Massive default.
                            let exchange_config = config.exchanges.iter()
                                .find(|e| e.name.to_lowercase() == exchange.to_lowercase())
                                .or_else(|| {
                                    let fallback = config.exchanges.iter()
                                        .find(|e| e.name.eq_ignore_ascii_case("massive"))
                                        .or_else(|| config.exchanges.first());
                                    if let Some(f) = fallback {
                                        log_info!(
                                            MAIN_LOGGER,
                                            "Exchange {} not in config — serving market data via default provider '{}' (consolidated feed)",
                                            exchange,
                                            f.name
                                        );
                                    }
                                    fallback
                                });

                            if let Some(ex_config) = exchange_config {
                                // Create stop channel for this connection
                                let (stop_tx, _stop_rx) = mpsc::channel::<()>(1);
                                connection_stop_channels.insert(exchange.clone(), stop_tx);

                                if ex_config.name.eq_ignore_ascii_case("oanda") {
                                    // Oanda has a real market-data connector (its own
                                    // v20 pricing stream) instead of the Massive
                                    // fallback every other venue uses -- see
                                    // spawn_oanda_handler's doc comment.
                                    let handle = spawn_oanda_handler(
                                        &symbols,
                                        exchange.clone(),
                                        shutdown_tx.subscribe(),
                                        Arc::clone(&ws_is_connected),
                                        Arc::clone(&ws_last_message),
                                    );
                                    active_connections.insert(
                                        format!("{}_oanda_pricing", exchange),
                                        handle,
                                    );
                                } else {
                                    // Spawn WebSocket handlers for requested data types
                                    for websocket in ex_config.websockets.iter() {
                                        if data_types.is_empty()
                                            || data_types.iter().any(|dt| data_type_matches_channel(dt, &websocket.channel))
                                        {
                                    let handle = spawn_websocket_handler(
                                                &symbols,
                                                ex_config.asset_class.as_deref().unwrap_or("crypto").to_string(),
                                                // Publish under the REQUESTING venue's topics
                                                // (market.data.{venue}.*): SignalEngine subscribes
                                                // per deployment exchange, not per data provider.
                                                exchange.clone(),
                                                shutdown_tx.subscribe(),
                                                Arc::clone(&ws_is_connected),
                                                Arc::clone(&ws_last_message),
                                            );
                                            active_connections.insert(
                                                format!("{}_{}", exchange, websocket.channel),
                                                handle,
                                            );
                                        }
                                    }
                                }

                                log_info!(
                                    MAIN_LOGGER,
                                    "Successfully connected to {} WebSocket for symbols: {:?}",
                                    exchange,
                                    symbols
                                );
                            } else {
                                log_warn!(
                                    MAIN_LOGGER,
                                    "Exchange {} not found in configuration",
                                    exchange
                                );
                            }
                        }
                        ConnectionEvent::AddSymbols { exchange, symbols } => {
                            log_info!(
                                MAIN_LOGGER,
                                "Add symbols to {}: {:?} (requires reconnection)",
                                exchange,
                                symbols
                            );
                            // WebSocket reconnection with updated symbol list not yet implemented
                        }
                        ConnectionEvent::RemoveSymbols { exchange, symbols } => {
                            log_info!(
                                MAIN_LOGGER,
                                "Remove symbols from {}: {:?}",
                                exchange,
                                symbols
                            );
                        }
                        ConnectionEvent::Disconnect { exchange } => {
                            log_info!(
                                MAIN_LOGGER,
                                "Disconnecting from exchange: {}",
                                exchange
                            );

                            // Stop connections for this exchange
                            if let Some(stop_tx) = connection_stop_channels.remove(&exchange) {
                                let _ = stop_tx.send(()).await;
                            }

                            // Remove active connection handles
                            let keys_to_remove: Vec<String> = active_connections.keys()
                                .filter(|k| k.starts_with(&format!("{}_", exchange)))
                                .cloned()
                                .collect();

                            for key in keys_to_remove {
                                if let Some(handle) = active_connections.remove(&key) {
                                    handle.abort();
                                }
                            }

                            log_info!(
                                MAIN_LOGGER,
                                "Disconnected from exchange: {}",
                                exchange
                            );
                        }
                    }
                }
                // Handle shutdown
                _ = shutdown_rx.recv() => {
                    log_info!(MAIN_LOGGER, "Shutdown signal received, stopping subscription manager");
                    subscription_manager.stop();
                    
                    // Abort all active connections
                    for (key, handle) in active_connections.drain() {
                        log_info!(MAIN_LOGGER, "Stopping connection: {}", key);
                        handle.abort();
                    }
                    
                    break;
                }
            }
        }

        log_info!(MAIN_LOGGER, "DataEngine shutdown complete");
        Ok(())
    }
}

// =============================================================================
// Helper Functions for Refactored Architecture
// =============================================================================

/// Returns true if a requested data-type from a `MarketDataSubscribe` message
/// (canonical SignalEngine vocabulary, e.g. `trades`, `orderbook`) refers to
/// the same logical stream as a per-exchange websocket `channel` configured in
/// `config.yaml` (e.g. `trade` in the Massive config).
///
/// Without this alias map, `data_types.contains(&channel)` would silently drop
/// all subscriptions whenever the two vocabularies disagree (which is currently
/// the case for every exchange).
fn data_type_matches_channel(requested: &str, channel: &str) -> bool {
    if requested.eq_ignore_ascii_case(channel) {
        return true;
    }
    let r = requested.to_ascii_lowercase();
    let c = channel.to_ascii_lowercase();
    matches!(
        (r.as_str(), c.as_str()),
        ("trades", "trade")
            | ("trade", "trades")
            | ("orderbook", "level3")
            | ("level3", "orderbook")
            | ("orderbook", "level2")
            | ("level2", "orderbook")
            | ("orderbook", "book")
            | ("book", "orderbook")
            | ("ticker", "tickers")
            | ("tickers", "ticker")
    )
}

/// Spawn a `MassiveWebSocketHandler` task.
///
/// The handler connects to the Massive feed URL (from `MASSIVE_FEED_URL` env var or
/// the default), authenticates with `MASSIVE_API_KEY`, subscribes to trade events
/// for every symbol, and forwards decoded trades to the MessageBroker.
fn spawn_websocket_handler(
    symbols: &[String],
    asset_class: String,
    venue: String,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ws_is_connected: Arc<AtomicBool>,
    ws_last_message: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<Result<()>> {
    let symbols = symbols.to_vec();

    tokio::spawn(async move {
        let feed_url = get_massive_feed_url(&asset_class);
        log_info!(
            MAIN_LOGGER,
            "Connecting Massive WebSocket: {} (publishing as venue '{}')",
            feed_url,
            venue
        );

        let h = MassiveWebSocketHandler::new(
            &feed_url,
            symbols,
            asset_class,
            venue,
            Arc::clone(&ws_is_connected),
            Arc::clone(&ws_last_message),
        )
        .await?;

        let handler: Arc<dyn EndpointHandler + Send + Sync> = Arc::new(h);

        if let Err(e) = handler.listen(shutdown_rx).await {
            log_error!(MAIN_LOGGER, "Massive WebSocket handler error: {}", e);
            return Err(e);
        }

        log_info!(MAIN_LOGGER, "Massive WebSocket handler finished");
        Ok(())
    })
}

/// Spawns a task that connects to Oanda's own v20 pricing stream and
/// forwards decoded prices to the MessageBroker -- unlike every other venue,
/// which is served by the consolidated Massive (Polygon) feed regardless of
/// its config entry, Oanda gets a real, venue-specific market-data
/// connection here because Polygon does not carry Oanda's FX instrument
/// universe (confirmed: exotic crosses like USD_ZAR/AUD_NZD/CHF_ZAR are
/// listed on Oanda's real instrument catalog but have no equivalent on
/// Polygon's forex feed).
///
/// The handler connects to Oanda's practice pricing stream (from
/// `OANDA_STREAM_URL` env var or the default), authenticates with
/// `OANDA_API_KEY` + `OANDA_PRACTICE_ACCOUNT_ID`, and forwards decoded price
/// ticks to the MessageBroker.
fn spawn_oanda_handler(
    symbols: &[String],
    venue: String,
    shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ws_is_connected: Arc<AtomicBool>,
    ws_last_message: Arc<AtomicU64>,
) -> tokio::task::JoinHandle<Result<()>> {
    let symbols = symbols.to_vec();

    tokio::spawn(async move {
        log_info!(
            MAIN_LOGGER,
            "Connecting Oanda pricing stream (publishing as venue '{}')",
            venue
        );

        let h = OandaStreamHandler::new(
            symbols,
            venue,
            Arc::clone(&ws_is_connected),
            Arc::clone(&ws_last_message),
        )
        .await?;

        let handler: Arc<dyn EndpointHandler + Send + Sync> = Arc::new(h);

        if let Err(e) = handler.listen(shutdown_rx).await {
            log_error!(MAIN_LOGGER, "Oanda pricing stream handler error: {}", e);
            return Err(e);
        }

        log_info!(MAIN_LOGGER, "Oanda pricing stream handler finished");
        Ok(())
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_type_matches_channel_exact() {
        assert!(data_type_matches_channel("trade", "trade"));
        assert!(data_type_matches_channel("TRADE", "trade"));
    }

    #[test]
    fn test_data_type_matches_channel_alias() {
        assert!(data_type_matches_channel("trades", "trade"));
        assert!(data_type_matches_channel("trade", "trades"));
        assert!(data_type_matches_channel("orderbook", "level3"));
        assert!(data_type_matches_channel("orderbook", "book"));
    }

    #[test]
    fn test_data_type_matches_channel_no_match() {
        assert!(!data_type_matches_channel("trades", "level3"));
        assert!(!data_type_matches_channel("ticker", "trade"));
    }
}