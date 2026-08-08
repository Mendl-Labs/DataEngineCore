//! Market Data Subscription Manager
//!
//! Manages demand-driven WebSocket connections based on SignalEngine subscriptions.
//! DataEngine only connects to exchange WebSockets when there are active strategies
//! that need specific symbols/exchanges.
//!
//! # Architecture
//!
//! ```text
//! SignalEngine deploys strategy
//!          ↓
//! Publishes MarketDataSubscribe to "market.subscription.subscribe"
//!          ↓
//! SubscriptionManager receives request
//!          ↓
//! Checks if exchange connection exists
//!          ↓
//!    ┌─────┴─────┐
//!    │ NO        │ YES
//!    ↓           ↓
//! Connect to   Add symbols to
//! WebSocket    existing subscription
//!    ↓           ↓
//! Publish MarketDataSubscriptionAck
//!          ↓
//! Stream data to MessageBroker
//! ```
//!
//! # Reference Counting
//!
//! Multiple strategies may subscribe to the same symbol. The manager tracks
//! reference counts and only disconnects when the last subscriber unsubscribes.

use dashmap::DashMap;
use prost::Message;
use protocol::broker::messages::{
    publish_request, ExchangeConnectionInfo, MarketDataStatusRequest, MarketDataStatusResponse,
    MarketDataSubscribe, MarketDataSubscriptionAck, MarketDataUnsubscribe, PublishRequest,
};
use publisher::{PublisherConfig, UltraFastPublisher};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use subscriber::UltraFastSubscriber;
use thiserror::Error;
use tokio::sync::{mpsc, RwLock};
use uuid::Uuid;

use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::log_info;
use super::tenant_subscription_limits::{TenantSubscriptionLimiter, SubscriptionTier};

/// Topics for market data subscriptions
pub mod topics {
    pub const SUBSCRIBE: &str = "market.subscription.subscribe";
    pub const UNSUBSCRIBE: &str = "market.subscription.unsubscribe";
    pub const ACK: &str = "market.subscription.ack";
    pub const STATUS_REQUEST: &str = "market.subscription.status.request";
    pub const STATUS_RESPONSE: &str = "market.subscription.status.response";
}

/// Errors from subscription management
#[derive(Debug, Error)]
pub enum SubscriptionError {
    #[error("Failed to connect to MessageBroker: {0}")]
    BrokerConnectionError(String),
    #[error("Failed to connect to exchange: {0}")]
    ExchangeConnectionError(String),
    #[error("Unsupported exchange: {0}")]
    UnsupportedExchange(String),
    #[error("Subscription not found: {0}")]
    SubscriptionNotFound(String),
}

/// Tracks a single subscription from a strategy instance
#[derive(Debug, Clone)]
pub struct Subscription {
    pub subscription_id: String,
    pub tenant_id: String,
    pub strategy_instance_id: String,
    pub exchange: String,
    pub symbols: HashSet<String>,
    pub data_types: HashSet<String>,
    pub orderbook_depth: i32,
    pub created_at: i64,
}

/// Tracks an active exchange connection
pub struct ExchangeConnection {
    pub exchange: String,
    pub connected: AtomicBool,
    pub connected_since: AtomicI64,
    pub messages_processed: AtomicU64,
    /// Symbols subscribed on this connection
    pub symbols: RwLock<HashSet<String>>,
    /// Data types being streamed
    pub data_types: RwLock<HashSet<String>>,
    /// Strategy instances using this connection (subscription_id -> Subscription)
    pub subscribers: DashMap<String, Subscription>,
    /// Handle to stop the WebSocket task
    pub stop_tx: Option<mpsc::Sender<()>>,
}

impl ExchangeConnection {
    pub fn new(exchange: String) -> Self {
        Self {
            exchange,
            connected: AtomicBool::new(false),
            connected_since: AtomicI64::new(0),
            messages_processed: AtomicU64::new(0),
            symbols: RwLock::new(HashSet::new()),
            data_types: RwLock::new(HashSet::new()),
            subscribers: DashMap::new(),
            stop_tx: None,
        }
    }

    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }

    pub async fn add_symbols(&self, symbols: &[String]) {
        let mut current = self.symbols.write().await;
        for symbol in symbols {
            current.insert(symbol.clone());
        }
    }

    pub async fn remove_symbols(&self, symbols: &[String]) {
        let mut current = self.symbols.write().await;
        for symbol in symbols {
            current.remove(symbol);
        }
    }

    pub async fn get_symbols(&self) -> Vec<String> {
        self.symbols.read().await.iter().cloned().collect()
    }

    pub async fn to_info(&self) -> ExchangeConnectionInfo {
        ExchangeConnectionInfo {
            exchange: self.exchange.clone(),
            connected: self.connected.load(Ordering::Relaxed),
            symbols: self.get_symbols().await,
            data_types: self.data_types.read().await.iter().cloned().collect(),
            subscriber_count: self.subscriber_count() as i32,
            connected_since: self.connected_since.load(Ordering::Relaxed),
            messages_processed: self.messages_processed.load(Ordering::Relaxed) as i64,
        }
    }
}

/// Event sent to the main application to manage WebSocket connections
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Connect to an exchange WebSocket
    Connect {
        exchange: String,
        symbols: Vec<String>,
        data_types: Vec<String>,
        orderbook_depth: i32,
    },
    /// Add symbols to existing connection
    AddSymbols {
        exchange: String,
        symbols: Vec<String>,
    },
    /// Remove symbols from connection
    RemoveSymbols {
        exchange: String,
        symbols: Vec<String>,
    },
    /// Disconnect from exchange (no more subscribers)
    Disconnect { exchange: String },
}

/// Manages market data subscriptions from SignalEngine
pub struct SubscriptionManager {
    /// Node ID for this DataEngine instance
    node_id: String,
    /// MessageBroker address
    broker_address: String,
    /// Active exchange connections
    connections: DashMap<String, Arc<ExchangeConnection>>,
    /// Publisher for sending acknowledgments
    publisher: Option<Arc<UltraFastPublisher>>,
    /// Channel to send connection events to main app
    event_tx: Option<mpsc::Sender<ConnectionEvent>>,
    /// Running flag
    is_running: Arc<AtomicBool>,
    /// Multi-tenant subscription limiter
    tenant_limiter: Arc<TenantSubscriptionLimiter>,
}

impl SubscriptionManager {
    /// Create a new subscription manager using the given tier-limits policy.
    pub fn new(broker_address: String, tier_limits: Arc<dyn super::tenant_subscription_limits::TierLimits>) -> Self {
        let node_id = format!("dataengine-{}", Uuid::new_v4().to_string()[..8].to_string());

        Self {
            node_id,
            broker_address,
            connections: DashMap::new(),
            publisher: None,
            event_tx: None,
            is_running: Arc::new(AtomicBool::new(false)),
            tenant_limiter: Arc::new(TenantSubscriptionLimiter::new(tier_limits)),
        }
    }

    /// Create with a custom tenant limiter
    pub fn with_limiter(broker_address: String, limiter: TenantSubscriptionLimiter) -> Self {
        let node_id = format!("dataengine-{}", Uuid::new_v4().to_string()[..8].to_string());

        Self {
            node_id,
            broker_address,
            connections: DashMap::new(),
            publisher: None,
            event_tx: None,
            is_running: Arc::new(AtomicBool::new(false)),
            tenant_limiter: Arc::new(limiter),
        }
    }

    /// Register a tenant with a specific tier
    pub fn register_tenant(&self, tenant_id: Uuid, tier: SubscriptionTier) {
        self.tenant_limiter.register_tenant(tenant_id, tier);
    }

    /// Get the tenant limiter for external access
    pub fn tenant_limiter(&self) -> &Arc<TenantSubscriptionLimiter> {
        &self.tenant_limiter
    }

    /// Start listening for subscription requests
    pub async fn start(
        &mut self,
        event_tx: mpsc::Sender<ConnectionEvent>,
    ) -> Result<(), SubscriptionError> {
        self.event_tx = Some(event_tx);

        // Connect to MessageBroker
        let config = PublisherConfig::new(&self.broker_address);
        let mut publisher = UltraFastPublisher::new(config);
        publisher
            .connect()
            .await
            .map_err(|e| SubscriptionError::BrokerConnectionError(format!("{:?}", e)))?;
        self.publisher = Some(Arc::new(publisher));

        // Create subscriber for subscription requests
        let subscriber = UltraFastSubscriber::new(fastrand::u64(..));

        // Subscribe to topics
        for topic in [topics::SUBSCRIBE, topics::UNSUBSCRIBE, topics::STATUS_REQUEST] {
            log_info!(
                MAIN_LOGGER,
                "Subscribing to broker topic '{}' via {}",
                topic,
                self.broker_address
            );

            subscriber
                .subscribe_to_topic(topic)
                .await
                .map_err(|e| SubscriptionError::BrokerConnectionError(format!("{:?}", e)))?;

            log_info!(
                MAIN_LOGGER,
                "Successfully subscribed to broker topic '{}'",
                topic
            );
        }

        // Start subscriber reader loop so incoming publish frames are consumed.
        subscriber.start();
        log_info!(MAIN_LOGGER, "SubscriptionManager subscriber reader loop started");

        self.is_running.store(true, Ordering::Relaxed);

        log_info!(
            MAIN_LOGGER,
            "SubscriptionManager started, listening for subscription requests on {}",
            self.broker_address
        );

        // Start processing loop
        let is_running = Arc::clone(&self.is_running);
        let connections = self.connections.clone();
        let publisher = self.publisher.clone();
        let event_tx = self.event_tx.clone();
        let node_id = self.node_id.clone();
        let tenant_limiter = Arc::clone(&self.tenant_limiter);

        tokio::spawn(async move {
            Self::process_messages(subscriber, connections, publisher, event_tx, node_id, is_running, tenant_limiter)
                .await;
        });

        Ok(())
    }

    /// Stop the subscription manager
    pub fn stop(&self) {
        self.is_running.store(false, Ordering::Relaxed);
        log_info!(MAIN_LOGGER, "SubscriptionManager stopping");
    }

    /// Get current connection status
    pub async fn get_status(&self) -> Vec<ExchangeConnectionInfo> {
        let mut infos = Vec::new();
        for entry in self.connections.iter() {
            infos.push(entry.value().to_info().await);
        }
        infos
    }

    /// Process incoming messages
    async fn process_messages(
        subscriber: UltraFastSubscriber,
        connections: DashMap<String, Arc<ExchangeConnection>>,
        publisher: Option<Arc<UltraFastPublisher>>,
        event_tx: Option<mpsc::Sender<ConnectionEvent>>,
        node_id: String,
        is_running: Arc<AtomicBool>,
        tenant_limiter: Arc<TenantSubscriptionLimiter>,
    ) {
        let topics = [topics::SUBSCRIBE, topics::UNSUBSCRIBE, topics::STATUS_REQUEST];

        while is_running.load(Ordering::Relaxed) {
            let mut had_message = false;

            for topic in &topics {
                if let Some(msg) = subscriber.get_message_from_topic(topic) {
                    had_message = true;

                    // Try to decode as PublishRequest
                    if let Ok(request) = PublishRequest::decode(msg.data.as_slice()) {
                        match (topic, &request.payload) {
                            (&topics::SUBSCRIBE, Some(publish_request::Payload::RawData(data))) => {
                                if let Ok(sub_req) =
                                    MarketDataSubscribe::decode(data.as_slice())
                                {
                                    Self::handle_subscribe(
                                        sub_req,
                                        &connections,
                                        publisher.as_ref(),
                                        event_tx.as_ref(),
                                        &node_id,
                                        &tenant_limiter,
                                    )
                                    .await;
                                }
                            }
                            (&topics::UNSUBSCRIBE, Some(publish_request::Payload::RawData(data))) => {
                                if let Ok(unsub_req) =
                                    MarketDataUnsubscribe::decode(data.as_slice())
                                {
                                    Self::handle_unsubscribe(
                                        unsub_req,
                                        &connections,
                                        publisher.as_ref(),
                                        event_tx.as_ref(),
                                        &node_id,
                                        &tenant_limiter,
                                    )
                                    .await;
                                }
                            }
                            (
                                &topics::STATUS_REQUEST,
                                Some(publish_request::Payload::RawData(data)),
                            ) => {
                                if let Ok(status_req) =
                                    MarketDataStatusRequest::decode(data.as_slice())
                                {
                                    Self::handle_status_request(
                                        status_req,
                                        &connections,
                                        publisher.as_ref(),
                                        &node_id,
                                    )
                                    .await;
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            if !had_message {
                tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
            }
        }
    }

    /// Handle a subscription request
    async fn handle_subscribe(
        request: MarketDataSubscribe,
        connections: &DashMap<String, Arc<ExchangeConnection>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        event_tx: Option<&mpsc::Sender<ConnectionEvent>>,
        node_id: &str,
        tenant_limiter: &TenantSubscriptionLimiter,
    ) {
        log_info!(
            MAIN_LOGGER,
            "Received subscription request: exchange={}, symbols={:?}, tenant={}, strategy={}",
            request.exchange,
            request.symbols,
            request.tenant_id,
            request.strategy_instance_id
        );

        let exchange = request.exchange.to_lowercase();

        // Parse tenant_id
        let tenant_id = match Uuid::parse_str(&request.tenant_id) {
            Ok(id) => id,
            Err(_) => {
                log_info!(
                    MAIN_LOGGER,
                    "Invalid tenant_id format: {}, using default",
                    request.tenant_id
                );
                Uuid::nil() // Use nil UUID for invalid tenant IDs
            }
        };

        // Validate subscription against tenant limits
        let validation = tenant_limiter
            .validate_subscription(
                tenant_id,
                &exchange,
                &request.symbols,
                &request.data_types,
                request.orderbook_depth,
            )
            .await;

        if !validation.allowed {
            log_info!(
                MAIN_LOGGER,
                "Subscription rejected for tenant {}: {:?}",
                tenant_id,
                validation.reason
            );

            // Send rejection acknowledgment
            if let Some(pub_ref) = publisher {
                let ack = MarketDataSubscriptionAck {
                    subscription_id: request.subscription_id,
                    success: false,
                    error_message: validation.reason.unwrap_or_else(|| "Subscription limit exceeded".to_string()),
                    data_engine_node: node_id.to_string(),
                    data_topics: vec![],
                    timestamp: now_timestamp(),
                };

                let encoded = ack.encode_to_vec();
                if let Err(e) = pub_ref.publish_raw(encoded, topics::ACK).await {
                    log_info!(MAIN_LOGGER, "Failed to publish rejection ack: {:?}", e);
                }
            }
            return;
        }

        // Use validated/adjusted values
        let symbols = validation.allowed_symbols.clone();
        let data_types = validation.allowed_data_types.clone();
        let orderbook_depth = validation.adjusted_orderbook_depth;
        let use_priority = validation.use_priority_routing;

        // Log if any were rejected
        if !validation.rejected_symbols.is_empty() {
            log_info!(
                MAIN_LOGGER,
                "Tenant {} symbols rejected (over limit): {:?}",
                tenant_id,
                validation.rejected_symbols
            );
        }
        if !validation.rejected_data_types.is_empty() {
            log_info!(
                MAIN_LOGGER,
                "Tenant {} data types rejected (not allowed): {:?}",
                tenant_id,
                validation.rejected_data_types
            );
        }

        // Check if we already have a connection to this exchange
        let is_new_connection = !connections.contains_key(&exchange);

        // Get or create connection
        let connection = connections
            .entry(exchange.clone())
            .or_insert_with(|| Arc::new(ExchangeConnection::new(exchange.clone())));

        // Create subscription record
        let subscription = Subscription {
            subscription_id: request.subscription_id.clone(),
            tenant_id: request.tenant_id.clone(),
            strategy_instance_id: request.strategy_instance_id.clone(),
            exchange: exchange.clone(),
            symbols: symbols.iter().cloned().collect(),
            data_types: data_types.iter().cloned().collect(),
            orderbook_depth,
            created_at: now_timestamp(),
        };

        // Add subscription
        connection
            .subscribers
            .insert(request.subscription_id.clone(), subscription);
        connection.add_symbols(&symbols).await;

        // Record subscription in tenant limiter
        tenant_limiter.record_subscription(tenant_id, &exchange, &symbols).await;

        // Send connection event to main app
        if let Some(tx) = event_tx {
            let event = if is_new_connection {
                ConnectionEvent::Connect {
                    exchange: exchange.clone(),
                    symbols: symbols.clone(),
                    data_types: data_types.clone(),
                    orderbook_depth,
                }
            } else {
                ConnectionEvent::AddSymbols {
                    exchange: exchange.clone(),
                    symbols: symbols.clone(),
                }
            };

            if let Err(e) = tx.send(event).await {
                log_info!(MAIN_LOGGER, "Failed to send connection event: {}", e);
            }
        }

        // Send acknowledgment with appropriate topics (priority or standard)
        if let Some(pub_ref) = publisher {
            let topic_prefix = if use_priority {
                "market.data.priority"
            } else {
                "market.data"
            };

            let ack = MarketDataSubscriptionAck {
                subscription_id: request.subscription_id,
                success: true,
                error_message: if validation.rejected_symbols.is_empty() && validation.rejected_data_types.is_empty() {
                    String::new()
                } else {
                    format!(
                        "Partial: {} symbols rejected, {} data types rejected",
                        validation.rejected_symbols.len(),
                        validation.rejected_data_types.len()
                    )
                },
                data_engine_node: node_id.to_string(),
                data_topics: data_types
                    .iter()
                    .map(|dt| format!("{}.{}.{}", topic_prefix, exchange, dt))
                    .collect(),
                timestamp: now_timestamp(),
            };

            let encoded = ack.encode_to_vec();
            if let Err(e) = pub_ref.publish_raw(encoded, topics::ACK).await {
                log_info!(MAIN_LOGGER, "Failed to publish subscription ack: {:?}", e);
            }
        }
    }

    /// Handle an unsubscription request
    async fn handle_unsubscribe(
        request: MarketDataUnsubscribe,
        connections: &DashMap<String, Arc<ExchangeConnection>>,
        _publisher: Option<&Arc<UltraFastPublisher>>,
        event_tx: Option<&mpsc::Sender<ConnectionEvent>>,
        _node_id: &str,
        tenant_limiter: &TenantSubscriptionLimiter,
    ) {
        log_info!(
            MAIN_LOGGER,
            "Received unsubscription request: subscription_id={}, reason={}",
            request.subscription_id,
            request.reason
        );

        let exchange = request.exchange.to_lowercase();

        if let Some(connection) = connections.get(&exchange) {
            // Get tenant_id from the subscription before removing it
            let tenant_id = connection
                .subscribers
                .get(&request.subscription_id)
                .and_then(|sub| Uuid::parse_str(&sub.tenant_id).ok());

            // Remove the subscription
            connection.subscribers.remove(&request.subscription_id);

            // Record unsubscription in tenant limiter
            if let Some(tid) = tenant_id {
                tenant_limiter.record_unsubscription(tid, &exchange, &request.symbols).await;
            }

            // Check if this was the last subscriber
            if connection.subscriber_count() == 0 {
                log_info!(
                    MAIN_LOGGER,
                    "No more subscribers for exchange {}, disconnecting",
                    exchange
                );

                // Remove the connection entry
                connections.remove(&exchange);

                // Send disconnect event
                if let Some(tx) = event_tx {
                    let event = ConnectionEvent::Disconnect {
                        exchange: exchange.clone(),
                    };
                    if let Err(e) = tx.send(event).await {
                        log_info!(MAIN_LOGGER, "Failed to send disconnect event: {}", e);
                    }
                }
            } else {
                // Just remove the symbols if no other subscriber needs them
                // (In a more sophisticated implementation, we'd track per-symbol reference counts)
                connection.remove_symbols(&request.symbols).await;

                if let Some(tx) = event_tx {
                    let event = ConnectionEvent::RemoveSymbols {
                        exchange: exchange.clone(),
                        symbols: request.symbols.clone(),
                    };
                    if let Err(e) = tx.send(event).await {
                        log_info!(MAIN_LOGGER, "Failed to send remove symbols event: {}", e);
                    }
                }
            }
        }
    }

    /// Handle a status request
    async fn handle_status_request(
        request: MarketDataStatusRequest,
        connections: &DashMap<String, Arc<ExchangeConnection>>,
        publisher: Option<&Arc<UltraFastPublisher>>,
        node_id: &str,
    ) {
        let mut active_connections = Vec::new();
        let mut total_subscriptions = 0;

        for entry in connections.iter() {
            let conn = entry.value();

            // Apply exchange filter if specified
            if !request.exchange_filter.is_empty()
                && conn.exchange != request.exchange_filter.to_lowercase()
            {
                continue;
            }

            total_subscriptions += conn.subscriber_count();
            active_connections.push(conn.to_info().await);
        }

        let response = MarketDataStatusResponse {
            request_id: request.request_id,
            data_engine_node: node_id.to_string(),
            active_connections,
            total_subscriptions: total_subscriptions as i32,
        };

        if let Some(pub_ref) = publisher {
            let encoded = response.encode_to_vec();
            if let Err(e) = pub_ref.publish_raw(encoded, topics::STATUS_RESPONSE).await {
                log_info!(MAIN_LOGGER, "Failed to publish status response: {:?}", e);
            }
        }
    }
}

fn now_timestamp() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exchange_connection_new() {
        let conn = ExchangeConnection::new("kraken".to_string());
        assert_eq!(conn.exchange, "kraken");
        assert!(!conn.connected.load(Ordering::Relaxed));
        assert_eq!(conn.subscriber_count(), 0);
    }

    #[tokio::test]
    async fn test_add_remove_symbols() {
        let conn = ExchangeConnection::new("kraken".to_string());
        conn.add_symbols(&["XBT/USD".to_string(), "ETH/USD".to_string()])
            .await;

        let symbols = conn.get_symbols().await;
        assert_eq!(symbols.len(), 2);
        assert!(symbols.contains(&"XBT/USD".to_string()));

        conn.remove_symbols(&["XBT/USD".to_string()]).await;
        let symbols = conn.get_symbols().await;
        assert_eq!(symbols.len(), 1);
        assert!(!symbols.contains(&"XBT/USD".to_string()));
    }
}
