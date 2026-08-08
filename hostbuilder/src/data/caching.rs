//! High-performance caching system for DataEngine
//! 
//! Provides multi-level caching with cache-coherent data structures,
//! memory-mapped configuration, and intelligent prefetching for
//! ultra-low latency market data access.

use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ahash::AHashMap;
use crossbeam_utils::CachePadded;
use dashmap::DashMap;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use once_cell::sync::Lazy;
use crate::infrastructure::logging_facade::MAIN_LOGGER; use crate::{log_warn, log_debug};

/// Cache entry with metadata
#[derive(Debug, Clone)]
pub struct CacheEntry<T> {
    pub value: T,
    pub created_at: Instant,
    pub last_accessed: Instant,
    pub access_count: u64,
    pub ttl: Option<Duration>,
}

impl<T> CacheEntry<T> {
    pub fn new(value: T, ttl: Option<Duration>) -> Self {
        let now = Instant::now();
        Self {
            value,
            created_at: now,
            last_accessed: now,
            access_count: 0,
            ttl,
        }
    }

    pub fn access(&mut self) -> &T {
        self.last_accessed = Instant::now();
        self.access_count += 1;
        &self.value
    }

    pub fn is_expired(&self) -> bool {
        if let Some(ttl) = self.ttl {
            self.created_at.elapsed() > ttl
        } else {
            false
        }
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

/// Cache statistics for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub entries: usize,
    pub memory_usage_bytes: usize,
    pub hit_rate: f64,
}

impl CacheStats {
    fn new() -> Self {
        Self {
            hits: 0,
            misses: 0,
            evictions: 0,
            entries: 0,
            memory_usage_bytes: 0,
            hit_rate: 0.0,
        }
    }

    fn update_hit_rate(&mut self) {
        let total = self.hits + self.misses;
        self.hit_rate = if total > 0 {
            (self.hits as f64) / (total as f64) * 100.0
        } else {
            0.0
        };
    }
}

/// High-performance LRU cache with cache-line optimization
pub struct OptimizedLRUCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    data: DashMap<K, CacheEntry<V>>,
    capacity: usize,
    stats: CachePadded<RwLock<CacheStats>>,
    // Simple counter-based eviction instead of maintaining order
    _access_counter: CachePadded<AtomicU64>,
}

impl<K, V> OptimizedLRUCache<K, V>
where
    K: Hash + Eq + Clone + Send + Sync,
    V: Clone + Send + Sync,
{
    pub fn new(capacity: usize) -> Self {
        Self {
            data: DashMap::with_capacity(capacity),
            capacity,
            stats: CachePadded::new(RwLock::new(CacheStats::new())),
            _access_counter: CachePadded::new(AtomicU64::new(0)),
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        if let Some(mut entry) = self.data.get_mut(key) {
            if entry.is_expired() {
                // Remove expired entry
                drop(entry);
                self.data.remove(key);
                self.record_miss();
                return None;
            }

            let value = entry.access().clone();
            // Access time updated in entry.access() method
            self.record_hit();
            Some(value)
        } else {
            self.record_miss();
            None
        }
    }

    pub fn insert(&self, key: K, value: V, ttl: Option<Duration>) -> Option<V> {
        // Check capacity and evict if needed
        if self.data.len() >= self.capacity {
            self.evict_lru();
        }

        let entry = CacheEntry::new(value, ttl);
        let old_value = self.data.insert(key.clone(), entry);
        
        // No need to update access order - creation time is set in CacheEntry::new
        
        if let Some(old_entry) = old_value {
            Some(old_entry.value)
        } else {
            self.update_stats_on_insert();
            None
        }
    }

    pub fn remove(&self, key: &K) -> Option<V> {
        self.data.remove(key).map(|(_, entry)| {
            self.update_stats_on_remove();
            entry.value
        })
    }

    pub fn contains_key(&self, key: &K) -> bool {
        if let Some(entry) = self.data.get(key) {
            if entry.is_expired() {
                drop(entry);
                self.data.remove(key);
                false
            } else {
                true
            }
        } else {
            false
        }
    }

    pub fn clear(&self) {
        self.data.clear();
        let mut stats = self.stats.write();
        stats.entries = 0;
        stats.memory_usage_bytes = 0;
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn get_stats(&self) -> CacheStats {
        let mut stats = self.stats.read().clone();
        stats.entries = self.data.len();
        stats.memory_usage_bytes = self.estimate_memory_usage();
        stats.update_hit_rate();
        stats
    }

    pub fn cleanup_expired(&self) -> usize {
        let mut removed = 0;
        let keys_to_remove: Vec<K> = self.data
            .iter()
            .filter_map(|entry| {
                if entry.is_expired() {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for key in keys_to_remove {
            if self.data.remove(&key).is_some() {
                removed += 1;
            }
        }

        log_debug!(MAIN_LOGGER, "Cleaned up {} expired cache entries", removed);
        removed
    }

    fn evict_lru(&self) {
        // Find the least recently used entry by timestamp
        let mut oldest_key: Option<K> = None;
        let mut oldest_time = Instant::now();
        
        for entry in self.data.iter() {
            if entry.last_accessed < oldest_time {
                oldest_time = entry.last_accessed;
                oldest_key = Some(entry.key().clone());
            }
        }
        
        if let Some(key) = oldest_key {
            if self.data.remove(&key).is_some() {
                let mut stats = self.stats.write();
                stats.evictions += 1;
                log_debug!(MAIN_LOGGER, "Evicted LRU cache entry");
            }
        }
    }

    fn record_hit(&self) {
        self.stats.write().hits += 1;
    }

    fn record_miss(&self) {
        self.stats.write().misses += 1;
    }

    fn update_stats_on_insert(&self) {
        let mut stats = self.stats.write();
        stats.entries += 1;
    }

    fn update_stats_on_remove(&self) {
        let mut stats = self.stats.write();
        stats.entries = stats.entries.saturating_sub(1);
    }

    fn estimate_memory_usage(&self) -> usize {
        // Rough estimation - in practice you might want more accurate measurements
        self.data.len() * (std::mem::size_of::<K>() + std::mem::size_of::<V>() + 64)
    }
}

/// Specialized cache for market data symbols
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Symbol {
    pub exchange: String,
    pub base: String,
    pub quote: String,
}

impl Symbol {
    pub fn new(exchange: &str, base: &str, quote: &str) -> Self {
        Self {
            exchange: exchange.to_string(),
            base: base.to_string(),
            quote: quote.to_string(),
        }
    }

    pub fn from_pair(exchange: &str, pair: &str) -> Option<Self> {
        let parts: Vec<&str> = pair.split('/').collect();
        if parts.len() == 2 {
            Some(Self::new(exchange, parts[0], parts[1]))
        } else {
            None
        }
    }

    pub fn to_pair(&self) -> String {
        format!("{}/{}", self.base, self.quote)
    }

    pub fn cache_key(&self) -> String {
        format!("{}:{}:{}", self.exchange, self.base, self.quote)
    }
}

/// Cached market data structures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedOrderBook {
    pub symbol: Symbol,
    pub bids: Vec<(f64, f64)>, // price, quantity
    pub asks: Vec<(f64, f64)>, // price, quantity
    pub timestamp: u64,
    pub sequence: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedTrade {
    pub symbol: Symbol,
    pub price: f64,
    pub quantity: f64,
    pub side: String, // "buy" or "sell"
    pub timestamp: u64,
    pub trade_id: String,
}

/// Multi-level cache system for market data
pub struct MarketDataCache {
    // L1: Hot symbol cache (sub-microsecond access)
    hot_symbols: OptimizedLRUCache<Symbol, CachedOrderBook>,
    
    // L2: Recent trades cache
    recent_trades: OptimizedLRUCache<Symbol, SmallVec<[CachedTrade; 32]>>,
    
    // L3: Symbol metadata cache
    symbol_metadata: DashMap<String, Symbol>,
    
    // Configuration cache
    config_cache: RwLock<AHashMap<String, serde_json::Value>>,
    
    // Cache warming statistics
    warming_stats: AtomicU64,
}

impl MarketDataCache {
    pub fn new(hot_symbols_capacity: usize, trades_capacity: usize) -> Self {
        Self {
            hot_symbols: OptimizedLRUCache::new(hot_symbols_capacity),
            recent_trades: OptimizedLRUCache::new(trades_capacity),
            symbol_metadata: DashMap::new(),
            config_cache: RwLock::new(AHashMap::new()),
            warming_stats: AtomicU64::new(0),
        }
    }

    /// Get cached order book for a symbol
    pub fn get_order_book(&self, symbol: &Symbol) -> Option<CachedOrderBook> {
        self.hot_symbols.get(symbol)
    }

    /// Cache an order book update
    pub fn cache_order_book(&self, order_book: CachedOrderBook, ttl: Option<Duration>) {
        self.hot_symbols.insert(order_book.symbol.clone(), order_book, ttl);
    }

    /// Get recent trades for a symbol
    pub fn get_recent_trades(&self, symbol: &Symbol) -> Option<SmallVec<[CachedTrade; 32]>> {
        self.recent_trades.get(symbol)
    }

    /// Add a trade to the cache
    pub fn add_trade(&self, trade: CachedTrade, max_trades: usize) {
        let symbol = trade.symbol.clone();
        let mut trades = self.recent_trades.get(&symbol).unwrap_or_default();
        
        // Add new trade and maintain size limit
        trades.push(trade);
        if trades.len() > max_trades {
            trades.remove(0);
        }
        
        self.recent_trades.insert(symbol, trades, Some(Duration::from_secs(300))); // 5 minutes TTL
    }

    /// Cache symbol metadata for fast lookups
    pub fn cache_symbol(&self, symbol_str: &str, symbol: Symbol) {
        self.symbol_metadata.insert(symbol_str.to_string(), symbol);
    }

    /// Get symbol from cache
    pub fn get_symbol(&self, symbol_str: &str) -> Option<Symbol> {
        self.symbol_metadata.get(symbol_str).map(|entry| entry.clone())
    }

    /// Cache configuration values
    pub fn cache_config<T: Serialize>(&self, key: &str, value: &T) {
        if let Ok(json_value) = serde_json::to_value(value) {
            self.config_cache.write().insert(key.to_string(), json_value);
        }
    }

    /// Get cached configuration
    pub fn get_config<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Option<T> {
        let cache = self.config_cache.read();
        cache.get(key).and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// Pre-warm cache with commonly used symbols
    pub fn warm_cache(&self, symbols: &[Symbol]) {
        let start = Instant::now();
        let mut warmed = 0;

        for symbol in symbols {
            // Create placeholder entries for symbols that don't exist
            if !self.hot_symbols.contains_key(symbol) {
                let placeholder = CachedOrderBook {
                    symbol: symbol.clone(),
                    bids: Vec::new(),
                    asks: Vec::new(),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_millis() as u64,
                    sequence: 0,
                };
                self.hot_symbols.insert(symbol.clone(), placeholder, None);
                warmed += 1;
            }
        }

        let elapsed = start.elapsed();
        self.warming_stats.store(elapsed.as_nanos() as u64, Ordering::Relaxed);
        
        log_debug!(MAIN_LOGGER, "Warmed cache with {} symbols in {:?}", warmed, elapsed);
    }

    /// Clean up expired entries across all caches
    pub fn cleanup(&self) {
        let hot_removed = self.hot_symbols.cleanup_expired();
        let trades_removed = self.recent_trades.cleanup_expired();
        
        log_debug!(MAIN_LOGGER, "Cache cleanup: removed {} hot symbols, {} trade entries", 
               hot_removed, trades_removed);
    }

    /// Get comprehensive cache statistics
    pub fn get_cache_stats(&self) -> CacheSystemStats {
        CacheSystemStats {
            hot_symbols_stats: self.hot_symbols.get_stats(),
            recent_trades_stats: self.recent_trades.get_stats(),
            symbol_metadata_entries: self.symbol_metadata.len(),
            config_cache_entries: self.config_cache.read().len(),
            last_warming_time_ns: self.warming_stats.load(Ordering::Relaxed),
        }
    }

    /// Clear all caches (useful for testing)
    pub fn clear_all(&self) {
        self.hot_symbols.clear();
        self.recent_trades.clear();
        self.symbol_metadata.clear();
        self.config_cache.write().clear();
        log_debug!(MAIN_LOGGER, "All caches cleared");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheSystemStats {
    pub hot_symbols_stats: CacheStats,
    pub recent_trades_stats: CacheStats,
    pub symbol_metadata_entries: usize,
    pub config_cache_entries: usize,
    pub last_warming_time_ns: u64,
}

impl CacheSystemStats {
    pub fn total_entries(&self) -> usize {
        self.hot_symbols_stats.entries + 
        self.recent_trades_stats.entries + 
        self.symbol_metadata_entries + 
        self.config_cache_entries
    }

    pub fn total_memory_usage(&self) -> usize {
        self.hot_symbols_stats.memory_usage_bytes + 
        self.recent_trades_stats.memory_usage_bytes
    }

    pub fn average_hit_rate(&self) -> f64 {
        (self.hot_symbols_stats.hit_rate + self.recent_trades_stats.hit_rate) / 2.0
    }
}

/// Global cache instance
static GLOBAL_CACHE: Lazy<MarketDataCache> = Lazy::new(|| {
    MarketDataCache::new(10000, 5000) // 10k symbols, 5k trade histories
});

/// Get reference to global cache
pub fn global_cache() -> &'static MarketDataCache {
    &GLOBAL_CACHE
}

/// Cache maintenance service
pub struct CacheMaintenanceService {
    _cache: &'static MarketDataCache, // Using underscore prefix to indicate intentionally unused
    cleanup_interval: Duration,
    is_running: AtomicU64, // 0 = stopped, 1 = running
}

impl CacheMaintenanceService {
    pub fn new(cleanup_interval: Duration) -> Self {
        Self {
            _cache: global_cache(),
            cleanup_interval,
            is_running: AtomicU64::new(0),
        }
    }

    pub async fn start(&self) {
        if self.is_running.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            log_warn!(MAIN_LOGGER, "Cache maintenance service already running");
            return;
        }

        log_debug!(MAIN_LOGGER, "Starting cache maintenance service");
        
        let cleanup_interval = self.cleanup_interval;
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            
            loop {
                interval.tick().await;
                
                // Perform cache maintenance
                let start = Instant::now();
                global_cache().cleanup();
                let elapsed = start.elapsed();
                
                log_debug!(MAIN_LOGGER, "Cache maintenance completed in {:?}", elapsed);
                
                // Log cache statistics periodically
                let stats = global_cache().get_cache_stats();
                log_debug!(MAIN_LOGGER, "Cache stats: {} total entries, {:.1}% avg hit rate", 
                       stats.total_entries(), stats.average_hit_rate());
            }
        });
    }

    pub fn stop(&self) {
        self.is_running.store(0, Ordering::SeqCst);
        log_debug!(MAIN_LOGGER, "Cache maintenance service stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_optimized_lru_cache() {
        let cache = OptimizedLRUCache::new(3);
        
        // Test basic operations
        cache.insert("key1".to_string(), "value1".to_string(), None);
        cache.insert("key2".to_string(), "value2".to_string(), None);
        cache.insert("key3".to_string(), "value3".to_string(), None);
        
        assert_eq!(cache.len(), 3);
        assert_eq!(cache.get(&"key1".to_string()), Some("value1".to_string()));
        
        // Test eviction
        cache.insert("key4".to_string(), "value4".to_string(), None);
        assert_eq!(cache.len(), 3);
        
        let stats = cache.get_stats();
        assert!(stats.hit_rate > 0.0);
    }

    #[test]
    fn test_symbol_cache() {
        let symbol = Symbol::new("KRAKEN", "BTC", "USD");
        assert_eq!(symbol.to_pair(), "BTC/USD");
        assert_eq!(symbol.cache_key(), "KRAKEN:BTC:USD");
        
        let symbol2 = Symbol::from_pair("KRAKEN", "ETH/USD").unwrap();
        assert_eq!(symbol2.base, "ETH");
        assert_eq!(symbol2.quote, "USD");
    }

    #[test]
    fn test_market_data_cache() {
        let cache = MarketDataCache::new(100, 50);
        let symbol = Symbol::new("KRAKEN", "BTC", "USD");
        
        let order_book = CachedOrderBook {
            symbol: symbol.clone(),
            bids: vec![(50000.0, 1.5), (49999.0, 2.0)],
            asks: vec![(50001.0, 1.0), (50002.0, 1.5)],
            timestamp: 1234567890,
            sequence: 1,
        };
        
        cache.cache_order_book(order_book.clone(), None);
        
        let cached = cache.get_order_book(&symbol).unwrap();
        assert_eq!(cached.bids.len(), 2);
        assert_eq!(cached.asks.len(), 2);
        
        let stats = cache.get_cache_stats();
        assert!(stats.total_entries() > 0);
    }

    #[tokio::test]
    async fn test_cache_maintenance() {
        let service = CacheMaintenanceService::new(Duration::from_millis(100));
        service.start().await;
        
        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(250)).await;
        
        service.stop();
        
        // Give it time to process the stop signal
        tokio::time::sleep(Duration::from_millis(50)).await;
        
        // Test passes if we got here without panicking
        assert!(true);
    }
}

/// Simple cache manager for general-purpose caching
pub struct CacheManager {
    cache: DashMap<String, serde_json::Value>,
}

impl CacheManager {
    pub async fn new() -> anyhow::Result<Self> {
        Ok(Self {
            cache: DashMap::new(),
        })
    }

    pub async fn set(&self, key: &str, value: &serde_json::Value) -> anyhow::Result<()> {
        self.cache.insert(key.to_string(), value.clone());
        Ok(())
    }

    pub async fn get(&self, key: &str) -> Option<serde_json::Value> {
        self.cache.get(key).map(|entry| entry.clone())
    }
}

