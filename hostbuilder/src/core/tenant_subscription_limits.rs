//! Multi-Tenant Subscription Limits for DataEngine
//!
//! This module provides the generic bookkeeping for per-tenant subscription
//! entitlements -- symbol/exchange caps, allowed data types, orderbook depth,
//! priority routing, retention -- without dictating what those numbers
//! actually are. The concrete tier table (what "Free" vs "Enterprise" means)
//! is a pricing/business decision owned by the deployer; implement
//! [`TierLimits`] and pass it to [`TenantSubscriptionLimiter::new`].
//!
//! # Features
//!
//! - **Subscription Limits per Tier**: Enforced via the caller-supplied [`TierLimits`] impl
//! - **Priority Data Routing**: Deployer-defined tiers can get dedicated high-priority topics
//! - **Tenant Metrics**: Track data usage per tenant for billing
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────┐
//! │                    MULTI-TENANT DATA ENGINE                             │
//! │                                                                         │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                TENANT SUBSCRIPTION LIMITER                       │   │
//! │  │                                                                  │   │
//! │  │   Resolves SubscriptionTier -> concrete limits via a             │   │
//! │  │   caller-supplied `TierLimits` implementation (no built-in       │   │
//! │  │   policy ships in this crate)                                    │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                           │                                             │
//! │                           ▼                                             │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                   TENANT METRICS TRACKER                         │   │
//! │  │                                                                  │   │
//! │  │   • Messages received per tenant                                 │   │
//! │  │   • Bandwidth usage per tenant                                   │   │
//! │  │   • Active symbols per tenant                                    │   │
//! │  │   • Peak concurrent subscriptions                                │   │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! │                           │                                             │
//! │                           ▼                                             │
//! │  ┌─────────────────────────────────────────────────────────────────┐   │
//! │  │                 PRIORITY TOPIC ROUTER                            │   │
//! │  │                                                                  │   │
//! │  │   Deployer-defined priority tiers: market.data.priority.{exchange}.{symbol} │
//! │  │   Others:                          market.data.{exchange}.{symbol}          │
//! │  └─────────────────────────────────────────────────────────────────┘   │
//! └─────────────────────────────────────────────────────────────────────────┘
//! ```

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::log_info;

// ============================================================================
// Subscription Tier
// ============================================================================

/// Subscription tier for rate limiting and resource allocation. Tier
/// *naming* only -- what each tier actually allows is resolved via
/// [`TierLimits`], not hardcoded here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum SubscriptionTier {
    /// Free tier: Limited symbols, single exchange, trades only
    #[default]
    Free,
    /// Starter tier: More symbols, multiple exchanges
    Starter,
    /// Professional tier: High limits, all features
    Professional,
    /// Enterprise tier: Unlimited + priority routing
    Enterprise,
}

impl std::fmt::Display for SubscriptionTier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriptionTier::Free => write!(f, "Free"),
            SubscriptionTier::Starter => write!(f, "Starter"),
            SubscriptionTier::Professional => write!(f, "Professional"),
            SubscriptionTier::Enterprise => write!(f, "Enterprise"),
        }
    }
}

// ============================================================================
// Tier Limits Policy
// ============================================================================

/// Resolves a [`SubscriptionTier`] into concrete entitlement numbers. This
/// crate ships no built-in policy -- what each tier allows (symbol/exchange
/// counts, data types, orderbook depth, priority routing, retention) is a
/// pricing/business decision for the deployer to own, not something a
/// generic ingestion engine should dictate. Implement this trait with your
/// own tier table and pass it to [`TenantSubscriptionLimiter::new`].
pub trait TierLimits: Send + Sync {
    /// Maximum number of symbols this tier can subscribe to.
    fn max_symbols(&self, tier: SubscriptionTier) -> usize;
    /// Maximum number of exchanges this tier can connect to.
    fn max_exchanges(&self, tier: SubscriptionTier) -> usize;
    /// Allowed data types for this tier.
    fn allowed_data_types(&self, tier: SubscriptionTier) -> Vec<&'static str>;
    /// Maximum orderbook depth for this tier.
    fn max_orderbook_depth(&self, tier: SubscriptionTier) -> i32;
    /// Whether this tier gets priority data routing.
    fn has_priority_routing(&self, tier: SubscriptionTier) -> bool;
    /// Data retention for historical queries (days).
    fn data_retention_days(&self, tier: SubscriptionTier) -> u32;
}

// ============================================================================
// Tenant Metrics
// ============================================================================

/// Metrics tracked per tenant for billing and monitoring
#[derive(Debug)]
pub struct TenantMetrics {
    /// Total messages received
    pub messages_received: AtomicU64,
    /// Total bytes received
    pub bytes_received: AtomicU64,
    /// Messages received today (reset daily)
    pub messages_today: AtomicU64,
    /// Bytes received today (reset daily)
    pub bytes_today: AtomicU64,
    /// Peak concurrent symbols
    pub peak_symbols: AtomicU64,
    /// Peak concurrent exchanges
    pub peak_exchanges: AtomicU64,
    /// Subscription requests made
    pub subscription_requests: AtomicU64,
    /// Subscription requests rejected (over limit)
    pub subscription_rejections: AtomicU64,
    /// Last activity timestamp
    pub last_activity: AtomicU64,
    /// Day of last reset (for daily metrics)
    pub last_reset_day: AtomicU64,
}

impl TenantMetrics {
    pub fn new() -> Self {
        Self {
            messages_received: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            messages_today: AtomicU64::new(0),
            bytes_today: AtomicU64::new(0),
            peak_symbols: AtomicU64::new(0),
            peak_exchanges: AtomicU64::new(0),
            subscription_requests: AtomicU64::new(0),
            subscription_rejections: AtomicU64::new(0),
            last_activity: AtomicU64::new(now_timestamp()),
            last_reset_day: AtomicU64::new(current_day()),
        }
    }

    /// Record a message received
    pub fn record_message(&self, bytes: u64) {
        self.messages_received.fetch_add(1, Ordering::Relaxed);
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
        self.messages_today.fetch_add(1, Ordering::Relaxed);
        self.bytes_today.fetch_add(bytes, Ordering::Relaxed);
        self.last_activity.store(now_timestamp(), Ordering::Relaxed);

        // Check if we need to reset daily counters
        let today = current_day();
        let last_reset = self.last_reset_day.load(Ordering::Relaxed);
        if today > last_reset {
            self.messages_today.store(1, Ordering::Relaxed);
            self.bytes_today.store(bytes, Ordering::Relaxed);
            self.last_reset_day.store(today, Ordering::Relaxed);
        }
    }

    /// Update peak metrics
    pub fn update_peaks(&self, symbols: usize, exchanges: usize) {
        let current_peak_symbols = self.peak_symbols.load(Ordering::Relaxed);
        if symbols as u64 > current_peak_symbols {
            self.peak_symbols.store(symbols as u64, Ordering::Relaxed);
        }

        let current_peak_exchanges = self.peak_exchanges.load(Ordering::Relaxed);
        if exchanges as u64 > current_peak_exchanges {
            self.peak_exchanges
                .store(exchanges as u64, Ordering::Relaxed);
        }
    }

    /// Record a subscription request
    pub fn record_subscription_request(&self, rejected: bool) {
        self.subscription_requests.fetch_add(1, Ordering::Relaxed);
        if rejected {
            self.subscription_rejections.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl Default for TenantMetrics {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Tenant Subscription State
// ============================================================================

/// Active subscriptions for a tenant
#[derive(Debug)]
pub struct TenantSubscriptionState {
    /// Tenant ID
    pub tenant_id: Uuid,
    /// Subscription tier
    pub tier: SubscriptionTier,
    /// Active symbols per exchange
    pub symbols_by_exchange: RwLock<HashMap<String, HashSet<String>>>,
    /// Active data types
    pub active_data_types: RwLock<HashSet<String>>,
    /// Metrics for this tenant
    pub metrics: TenantMetrics,
    /// Created timestamp
    pub created_at: u64,
}

impl TenantSubscriptionState {
    pub fn new(tenant_id: Uuid, tier: SubscriptionTier) -> Self {
        Self {
            tenant_id,
            tier,
            symbols_by_exchange: RwLock::new(HashMap::new()),
            active_data_types: RwLock::new(HashSet::new()),
            metrics: TenantMetrics::new(),
            created_at: now_timestamp(),
        }
    }

    /// Get total symbol count across all exchanges
    pub async fn total_symbols(&self) -> usize {
        let map = self.symbols_by_exchange.read().await;
        map.values().map(|s| s.len()).sum()
    }

    /// Get exchange count
    pub async fn exchange_count(&self) -> usize {
        self.symbols_by_exchange.read().await.len()
    }

    /// Add symbols for an exchange
    pub async fn add_symbols(&self, exchange: &str, symbols: &[String]) {
        let mut map = self.symbols_by_exchange.write().await;
        let entry = map.entry(exchange.to_string()).or_insert_with(HashSet::new);
        for symbol in symbols {
            entry.insert(symbol.clone());
        }

        // Update peak metrics
        let total_symbols: usize = map.values().map(|s| s.len()).sum();
        let total_exchanges = map.len();
        self.metrics.update_peaks(total_symbols, total_exchanges);
    }

    /// Remove symbols for an exchange
    pub async fn remove_symbols(&self, exchange: &str, symbols: &[String]) {
        let mut map = self.symbols_by_exchange.write().await;
        if let Some(entry) = map.get_mut(exchange) {
            for symbol in symbols {
                entry.remove(symbol);
            }
            // Remove exchange if no symbols left
            if entry.is_empty() {
                map.remove(exchange);
            }
        }
    }

    /// Check if tenant has this symbol
    pub async fn has_symbol(&self, exchange: &str, symbol: &str) -> bool {
        let map = self.symbols_by_exchange.read().await;
        map.get(exchange).is_some_and(|s| s.contains(symbol))
    }
}

// ============================================================================
// Subscription Validation Result
// ============================================================================

/// Result of subscription validation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionValidation {
    pub allowed: bool,
    pub reason: Option<String>,
    pub allowed_symbols: Vec<String>,
    pub rejected_symbols: Vec<String>,
    pub allowed_data_types: Vec<String>,
    pub rejected_data_types: Vec<String>,
    pub adjusted_orderbook_depth: i32,
    pub use_priority_routing: bool,
}

impl SubscriptionValidation {
    pub fn allowed(
        symbols: Vec<String>,
        data_types: Vec<String>,
        orderbook_depth: i32,
        priority_routing: bool,
    ) -> Self {
        Self {
            allowed: true,
            reason: None,
            allowed_symbols: symbols,
            rejected_symbols: vec![],
            allowed_data_types: data_types,
            rejected_data_types: vec![],
            adjusted_orderbook_depth: orderbook_depth,
            use_priority_routing: priority_routing,
        }
    }

    pub fn rejected(reason: String) -> Self {
        Self {
            allowed: false,
            reason: Some(reason),
            allowed_symbols: vec![],
            rejected_symbols: vec![],
            allowed_data_types: vec![],
            rejected_data_types: vec![],
            adjusted_orderbook_depth: 0,
            use_priority_routing: false,
        }
    }

    pub fn partial(
        allowed_symbols: Vec<String>,
        rejected_symbols: Vec<String>,
        allowed_data_types: Vec<String>,
        rejected_data_types: Vec<String>,
        orderbook_depth: i32,
        priority_routing: bool,
    ) -> Self {
        Self {
            allowed: !allowed_symbols.is_empty(),
            reason: if rejected_symbols.is_empty() && rejected_data_types.is_empty() {
                None
            } else {
                Some(format!(
                    "Partial subscription: {} symbols rejected, {} data types rejected",
                    rejected_symbols.len(),
                    rejected_data_types.len()
                ))
            },
            allowed_symbols,
            rejected_symbols,
            allowed_data_types,
            rejected_data_types,
            adjusted_orderbook_depth: orderbook_depth,
            use_priority_routing: priority_routing,
        }
    }
}

// ============================================================================
// Tenant Subscription Limiter
// ============================================================================

/// Main component for managing tenant subscription limits
pub struct TenantSubscriptionLimiter {
    /// Tenant states
    tenants: DashMap<Uuid, Arc<TenantSubscriptionState>>,
    /// Default tier for new tenants
    default_tier: SubscriptionTier,
    /// Resolves tiers into concrete entitlement numbers -- see [`TierLimits`].
    tier_limits: Arc<dyn TierLimits>,
}

impl TenantSubscriptionLimiter {
    /// Create a new limiter using the given tier-limits policy.
    pub fn new(tier_limits: Arc<dyn TierLimits>) -> Self {
        Self {
            tenants: DashMap::new(),
            default_tier: SubscriptionTier::Free,
            tier_limits,
        }
    }

    /// Create with a specific default tier and tier-limits policy.
    pub fn with_default_tier(tier: SubscriptionTier, tier_limits: Arc<dyn TierLimits>) -> Self {
        Self {
            tenants: DashMap::new(),
            default_tier: tier,
            tier_limits,
        }
    }

    /// Register or update a tenant's tier
    pub fn register_tenant(&self, tenant_id: Uuid, tier: SubscriptionTier) {
        if let Some(existing) = self.tenants.get(&tenant_id) {
            // Update tier if different (would need interior mutability for tier)
            // For now, we'll just log
            log_info!(
                MAIN_LOGGER,
                "Tenant {} already registered with tier {:?}",
                tenant_id,
                existing.tier
            );
        } else {
            let state = Arc::new(TenantSubscriptionState::new(tenant_id, tier));
            self.tenants.insert(tenant_id, state);
            log_info!(
                MAIN_LOGGER,
                "Registered tenant {} with tier {}",
                tenant_id,
                tier
            );
        }
    }

    /// Get or create tenant state (uses default tier if new)
    pub fn get_or_create_tenant(&self, tenant_id: Uuid) -> Arc<TenantSubscriptionState> {
        self.tenants
            .entry(tenant_id)
            .or_insert_with(|| {
                log_info!(
                    MAIN_LOGGER,
                    "Creating new tenant {} with default tier {}",
                    tenant_id,
                    self.default_tier
                );
                Arc::new(TenantSubscriptionState::new(tenant_id, self.default_tier))
            })
            .clone()
    }

    /// Validate a subscription request
    pub async fn validate_subscription(
        &self,
        tenant_id: Uuid,
        exchange: &str,
        symbols: &[String],
        data_types: &[String],
        orderbook_depth: i32,
    ) -> SubscriptionValidation {
        let tenant = self.get_or_create_tenant(tenant_id);
        tenant.metrics.record_subscription_request(false);

        let tier = tenant.tier;

        // Check exchange limit
        let current_exchanges = tenant.exchange_count().await;
        let has_exchange = tenant
            .symbols_by_exchange
            .read()
            .await
            .contains_key(exchange);

        if !has_exchange && current_exchanges >= self.tier_limits.max_exchanges(tier) {
            tenant.metrics.record_subscription_request(true);
            return SubscriptionValidation::rejected(format!(
                "Exchange limit exceeded: {} tier allows {} exchanges, you have {}",
                tier,
                self.tier_limits.max_exchanges(tier),
                current_exchanges
            ));
        }

        // Check symbol limit
        let current_symbols = tenant.total_symbols().await;
        let max_symbols = self.tier_limits.max_symbols(tier);
        let available_slots = max_symbols.saturating_sub(current_symbols);

        let (allowed_symbols, rejected_symbols): (Vec<_>, Vec<_>) = if available_slots == 0 {
            (vec![], symbols.to_vec())
        } else if symbols.len() <= available_slots {
            (symbols.to_vec(), vec![])
        } else {
            // Partial: allow up to available slots
            let allowed: Vec<String> = symbols.iter().take(available_slots).cloned().collect();
            let rejected: Vec<String> = symbols.iter().skip(available_slots).cloned().collect();
            (allowed, rejected)
        };

        if allowed_symbols.is_empty() {
            tenant.metrics.record_subscription_request(true);
            return SubscriptionValidation::rejected(format!(
                "Symbol limit exceeded: {} tier allows {} symbols, you have {}",
                tier, max_symbols, current_symbols
            ));
        }

        // Check data types
        let allowed_types = self.tier_limits.allowed_data_types(tier);
        let (allowed_data_types, rejected_data_types): (Vec<_>, Vec<_>) = data_types
            .iter()
            .partition(|dt| allowed_types.contains(&dt.as_str()));

        let allowed_data_types: Vec<String> = allowed_data_types.into_iter().cloned().collect();
        let rejected_data_types: Vec<String> = rejected_data_types.into_iter().cloned().collect();

        // Adjust orderbook depth
        let adjusted_depth = orderbook_depth.min(self.tier_limits.max_orderbook_depth(tier));

        // Determine if priority routing applies
        let priority_routing = self.tier_limits.has_priority_routing(tier);

        if rejected_symbols.is_empty() && rejected_data_types.is_empty() {
            SubscriptionValidation::allowed(
                allowed_symbols,
                allowed_data_types,
                adjusted_depth,
                priority_routing,
            )
        } else {
            SubscriptionValidation::partial(
                allowed_symbols,
                rejected_symbols,
                allowed_data_types,
                rejected_data_types,
                adjusted_depth,
                priority_routing,
            )
        }
    }

    /// Record that symbols were successfully subscribed
    pub async fn record_subscription(&self, tenant_id: Uuid, exchange: &str, symbols: &[String]) {
        if let Some(tenant) = self.tenants.get(&tenant_id) {
            tenant.add_symbols(exchange, symbols).await;
        }
    }

    /// Record that symbols were unsubscribed
    pub async fn record_unsubscription(&self, tenant_id: Uuid, exchange: &str, symbols: &[String]) {
        if let Some(tenant) = self.tenants.get(&tenant_id) {
            tenant.remove_symbols(exchange, symbols).await;
        }
    }

    /// Record a message received for a tenant
    pub fn record_message(&self, tenant_id: Uuid, bytes: u64) {
        if let Some(tenant) = self.tenants.get(&tenant_id) {
            tenant.metrics.record_message(bytes);
        }
    }

    /// Get the topic for publishing data to a tenant
    pub fn get_data_topic(&self, tenant_id: Uuid, exchange: &str, data_type: &str) -> String {
        let use_priority = self
            .tenants
            .get(&tenant_id)
            .map(|t| self.tier_limits.has_priority_routing(t.tier))
            .unwrap_or(false);

        if use_priority {
            format!("market.data.priority.{}.{}", exchange, data_type)
        } else {
            format!("market.data.{}.{}", exchange, data_type)
        }
    }

    /// Get tenant statistics
    pub async fn get_tenant_stats(&self, tenant_id: Uuid) -> Option<TenantSubscriptionStats> {
        self.tenants.get(&tenant_id).map(|tenant| {
            let metrics = &tenant.metrics;
            TenantSubscriptionStats {
                tenant_id,
                tier: tenant.tier,
                messages_received: metrics.messages_received.load(Ordering::Relaxed),
                bytes_received: metrics.bytes_received.load(Ordering::Relaxed),
                messages_today: metrics.messages_today.load(Ordering::Relaxed),
                bytes_today: metrics.bytes_today.load(Ordering::Relaxed),
                peak_symbols: metrics.peak_symbols.load(Ordering::Relaxed),
                peak_exchanges: metrics.peak_exchanges.load(Ordering::Relaxed),
                subscription_requests: metrics.subscription_requests.load(Ordering::Relaxed),
                subscription_rejections: metrics.subscription_rejections.load(Ordering::Relaxed),
                last_activity: metrics.last_activity.load(Ordering::Relaxed),
            }
        })
    }

    /// Get global statistics
    pub fn get_global_stats(&self) -> GlobalSubscriptionStats {
        let mut total_tenants = 0;
        let mut total_messages = 0u64;
        let mut total_bytes = 0u64;
        let mut total_rejections = 0u64;
        let mut tenants_by_tier: HashMap<SubscriptionTier, u32> = HashMap::new();

        for entry in self.tenants.iter() {
            total_tenants += 1;
            let tenant = entry.value();
            total_messages += tenant.metrics.messages_received.load(Ordering::Relaxed);
            total_bytes += tenant.metrics.bytes_received.load(Ordering::Relaxed);
            total_rejections += tenant
                .metrics
                .subscription_rejections
                .load(Ordering::Relaxed);
            *tenants_by_tier.entry(tenant.tier).or_insert(0) += 1;
        }

        GlobalSubscriptionStats {
            total_tenants,
            total_messages,
            total_bytes,
            total_rejections,
            tenants_by_tier,
        }
    }

    /// Remove a tenant (cleanup)
    pub fn remove_tenant(&self, tenant_id: Uuid) {
        self.tenants.remove(&tenant_id);
        log_info!(MAIN_LOGGER, "Removed tenant {}", tenant_id);
    }
}

// ============================================================================
// Statistics Structs
// ============================================================================

/// Statistics for a single tenant
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantSubscriptionStats {
    pub tenant_id: Uuid,
    pub tier: SubscriptionTier,
    pub messages_received: u64,
    pub bytes_received: u64,
    pub messages_today: u64,
    pub bytes_today: u64,
    pub peak_symbols: u64,
    pub peak_exchanges: u64,
    pub subscription_requests: u64,
    pub subscription_rejections: u64,
    pub last_activity: u64,
}

/// Global statistics across all tenants
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GlobalSubscriptionStats {
    pub total_tenants: u32,
    pub total_messages: u64,
    pub total_bytes: u64,
    pub total_rejections: u64,
    pub tenants_by_tier: HashMap<SubscriptionTier, u32>,
}

// ============================================================================
// Helper Functions
// ============================================================================

fn now_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn current_day() -> u64 {
    now_timestamp() / 86400 // Seconds per day
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    /// Minimal test-only tier-limits policy -- just enough to exercise the
    /// limiter's logic, not meant to demonstrate a real pricing table.
    struct TestTierLimits;
    impl TierLimits for TestTierLimits {
        fn max_symbols(&self, tier: SubscriptionTier) -> usize {
            match tier {
                SubscriptionTier::Free => 5,
                SubscriptionTier::Starter => 20,
                SubscriptionTier::Professional => 100,
                SubscriptionTier::Enterprise => usize::MAX,
            }
        }
        fn max_exchanges(&self, tier: SubscriptionTier) -> usize {
            match tier {
                SubscriptionTier::Free => 1,
                SubscriptionTier::Starter => 3,
                SubscriptionTier::Professional => 10,
                SubscriptionTier::Enterprise => usize::MAX,
            }
        }
        fn allowed_data_types(&self, tier: SubscriptionTier) -> Vec<&'static str> {
            match tier {
                SubscriptionTier::Free => vec!["trades"],
                SubscriptionTier::Starter => vec!["trades", "orderbook"],
                SubscriptionTier::Professional => vec!["trades", "orderbook", "ticker"],
                SubscriptionTier::Enterprise => vec!["trades", "orderbook", "ticker", "ohlcv"],
            }
        }
        fn max_orderbook_depth(&self, tier: SubscriptionTier) -> i32 {
            match tier {
                SubscriptionTier::Free => 0,
                SubscriptionTier::Starter => 10,
                SubscriptionTier::Professional => 25,
                SubscriptionTier::Enterprise => 100,
            }
        }
        fn has_priority_routing(&self, tier: SubscriptionTier) -> bool {
            matches!(tier, SubscriptionTier::Enterprise)
        }
        fn data_retention_days(&self, tier: SubscriptionTier) -> u32 {
            match tier {
                SubscriptionTier::Free => 1,
                SubscriptionTier::Starter => 7,
                SubscriptionTier::Professional => 30,
                SubscriptionTier::Enterprise => 365,
            }
        }
    }

    fn test_limiter() -> TenantSubscriptionLimiter {
        TenantSubscriptionLimiter::new(Arc::new(TestTierLimits))
    }

    #[test]
    fn test_tier_limits() {
        let limits = TestTierLimits;
        assert_eq!(limits.max_symbols(SubscriptionTier::Free), 5);
        assert_eq!(limits.max_exchanges(SubscriptionTier::Free), 1);
        assert_eq!(
            limits.allowed_data_types(SubscriptionTier::Free),
            vec!["trades"]
        );

        assert_eq!(limits.max_symbols(SubscriptionTier::Enterprise), usize::MAX);
        assert!(limits.has_priority_routing(SubscriptionTier::Enterprise));
    }

    #[tokio::test]
    async fn test_validate_subscription_free_tier() {
        let limiter = test_limiter();
        let tenant_id = Uuid::new_v4();

        // Register as free tier
        limiter.register_tenant(tenant_id, SubscriptionTier::Free);

        // Should allow up to 5 symbols
        let validation = limiter
            .validate_subscription(
                tenant_id,
                "kraken",
                &["BTC/USD".to_string(), "ETH/USD".to_string()],
                &["trades".to_string()],
                10,
            )
            .await;

        assert!(validation.allowed);
        assert_eq!(validation.allowed_symbols.len(), 2);
        assert_eq!(validation.adjusted_orderbook_depth, 0); // Free tier has no orderbook
        assert!(!validation.use_priority_routing);
    }

    #[tokio::test]
    async fn test_symbol_limit_exceeded() {
        let limiter = test_limiter();
        let tenant_id = Uuid::new_v4();

        limiter.register_tenant(tenant_id, SubscriptionTier::Free);

        // Request 10 symbols (free tier allows 5)
        let symbols: Vec<String> = (0..10).map(|i| format!("SYM{}/USD", i)).collect();

        let validation = limiter
            .validate_subscription(tenant_id, "kraken", &symbols, &["trades".to_string()], 0)
            .await;

        // Should allow 5 and reject 5
        assert!(validation.allowed); // Partial is still allowed
        assert_eq!(validation.allowed_symbols.len(), 5);
        assert_eq!(validation.rejected_symbols.len(), 5);
    }

    #[tokio::test]
    async fn test_enterprise_priority_routing() {
        let limiter = test_limiter();
        let tenant_id = Uuid::new_v4();

        limiter.register_tenant(tenant_id, SubscriptionTier::Enterprise);

        let validation = limiter
            .validate_subscription(
                tenant_id,
                "kraken",
                &["BTC/USD".to_string()],
                &["trades".to_string(), "orderbook".to_string()],
                100,
            )
            .await;

        assert!(validation.allowed);
        assert!(validation.use_priority_routing);
        assert_eq!(validation.adjusted_orderbook_depth, 100);
    }

    #[tokio::test]
    async fn test_get_data_topic() {
        let limiter = test_limiter();

        let free_tenant = Uuid::new_v4();
        let enterprise_tenant = Uuid::new_v4();

        limiter.register_tenant(free_tenant, SubscriptionTier::Free);
        limiter.register_tenant(enterprise_tenant, SubscriptionTier::Enterprise);

        // Free tier gets standard topic
        let topic = limiter.get_data_topic(free_tenant, "kraken", "trades");
        assert_eq!(topic, "market.data.kraken.trades");

        // Enterprise gets priority topic
        let topic = limiter.get_data_topic(enterprise_tenant, "kraken", "trades");
        assert_eq!(topic, "market.data.priority.kraken.trades");
    }
}
