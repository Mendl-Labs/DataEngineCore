//! Ultra-high precision metrics for sub-microsecond trading
//!
//! Provides nanosecond-precision timing and hardware-level performance
//! monitoring specifically designed for high-frequency trading systems.

use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::{log_info, log_warn};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
#[cfg(target_arch = "x86_64")]
use std::time::Duration;
use std::time::Instant;

/// Ultra-precision metrics collector
pub struct UltraPrecisionMetrics {
    // Hardware timestamp counters for nanosecond precision
    processing_latency_sum_ns: AtomicU64,
    processing_latency_count: AtomicU64,
    processing_latency_min_ns: AtomicU64,
    processing_latency_max_ns: AtomicU64,

    // Trading-specific counters
    orders_processed: AtomicU64,
    trades_executed: AtomicU64,
    l1_updates: AtomicU64,
    l3_updates: AtomicU64,

    // Performance optimization counters
    simd_operations: AtomicU64,
    kernel_bypass_operations: AtomicU64,
    zero_copy_operations: AtomicU64,

    // Cache performance
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,

    // Hardware performance
    _cpu_cycles_per_operation: AtomicU64,

    // Symbol-specific metrics
    symbol_metrics: Arc<RwLock<HashMap<String, SymbolMetrics>>>,

    // Timing calibration
    _rdtsc_frequency: u64,
    _start_time: Instant,
}

#[derive(Debug, Default)]
struct SymbolMetrics {
    orders_added: AtomicU64,
    trades_count: AtomicU64,
    last_trade_price: AtomicU64, // Fixed point representation
    _best_bid_updates: AtomicU64,
    _best_ask_updates: AtomicU64,
}

impl UltraPrecisionMetrics {
    // Deliberately NOT implementing `Default`: `new()` calls
    // `calibrate_rdtsc_frequency()`, which blocks the calling thread for
    // ~100ms (see below). A `Default` impl would hide that latency behind an
    // innocuous-looking `T::default()` call. Flagged during the fmt/clippy
    // cleanup pass rather than auto-adding the derive clippy suggests.
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let rdtsc_freq = Self::calibrate_rdtsc_frequency();

        Self {
            processing_latency_sum_ns: AtomicU64::new(0),
            processing_latency_count: AtomicU64::new(0),
            processing_latency_min_ns: AtomicU64::new(u64::MAX),
            processing_latency_max_ns: AtomicU64::new(0),
            orders_processed: AtomicU64::new(0),
            trades_executed: AtomicU64::new(0),
            l1_updates: AtomicU64::new(0),
            l3_updates: AtomicU64::new(0),
            simd_operations: AtomicU64::new(0),
            kernel_bypass_operations: AtomicU64::new(0),
            zero_copy_operations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            _cpu_cycles_per_operation: AtomicU64::new(0),
            symbol_metrics: Arc::new(RwLock::new(HashMap::new())),
            _rdtsc_frequency: rdtsc_freq,
            _start_time: Instant::now(),
        }
    }

    /// Hardware timestamp using RDTSC
    #[inline(always)]
    pub fn hardware_timestamp() -> u64 {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            std::arch::x86_64::_rdtsc()
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            std::time::Instant::now().elapsed().as_nanos() as u64
        }
    }

    /// Calibrate RDTSC frequency
    fn calibrate_rdtsc_frequency() -> u64 {
        #[cfg(target_arch = "x86_64")]
        {
            let start_time = Instant::now();
            let start_rdtsc = Self::hardware_timestamp();

            std::thread::sleep(Duration::from_millis(100));

            let end_time = Instant::now();
            let end_rdtsc = Self::hardware_timestamp();

            let elapsed_ns = (end_time - start_time).as_nanos() as u64;
            let elapsed_cycles = end_rdtsc - start_rdtsc;

            if elapsed_ns > 0 {
                elapsed_cycles * 1000000 / elapsed_ns
            } else {
                3000000 // 3GHz fallback
            }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            1
        }
    }

    /// Record processing latency with nanosecond precision
    pub fn record_processing_latency_ns(&self, latency_ns: u64) {
        self.processing_latency_sum_ns
            .fetch_add(latency_ns, Ordering::Relaxed);
        self.processing_latency_count
            .fetch_add(1, Ordering::Relaxed);

        // Update min
        let mut current_min = self.processing_latency_min_ns.load(Ordering::Relaxed);
        while latency_ns < current_min {
            match self.processing_latency_min_ns.compare_exchange_weak(
                current_min,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_min = actual,
            }
        }

        // Update max
        let mut current_max = self.processing_latency_max_ns.load(Ordering::Relaxed);
        while latency_ns > current_max {
            match self.processing_latency_max_ns.compare_exchange_weak(
                current_max,
                latency_ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current_max = actual,
            }
        }
    }

    /// Record order processing
    pub fn record_order(&self, symbol: &str) {
        self.orders_processed.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut metrics) = self.symbol_metrics.write() {
            let symbol_metric = metrics
                .entry(symbol.to_string())
                .or_insert_with(SymbolMetrics::default);
            symbol_metric.orders_added.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record trade execution
    pub fn record_trade(&self, symbol: &str, price: f64) {
        self.trades_executed.fetch_add(1, Ordering::Relaxed);

        if let Ok(mut metrics) = self.symbol_metrics.write() {
            let symbol_metric = metrics
                .entry(symbol.to_string())
                .or_insert_with(SymbolMetrics::default);
            symbol_metric.trades_count.fetch_add(1, Ordering::Relaxed);
            symbol_metric
                .last_trade_price
                .store((price * 10000.0) as u64, Ordering::Relaxed);
        }
    }

    /// Record L1 update
    pub fn record_l1_update(&self) {
        self.l1_updates.fetch_add(1, Ordering::Relaxed);
    }

    /// Record SIMD operation
    pub fn record_simd_operation(&self) {
        self.simd_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Record kernel bypass operation
    pub fn record_kernel_bypass(&self) {
        self.kernel_bypass_operations
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Record zero-copy operation
    pub fn record_zero_copy(&self) {
        self.zero_copy_operations.fetch_add(1, Ordering::Relaxed);
    }

    /// Get performance statistics
    pub fn get_stats(&self) -> UltraPrecisionStats {
        let count = self.processing_latency_count.load(Ordering::Relaxed);
        let sum_ns = self.processing_latency_sum_ns.load(Ordering::Relaxed);
        let min_ns = self.processing_latency_min_ns.load(Ordering::Relaxed);
        let max_ns = self.processing_latency_max_ns.load(Ordering::Relaxed);

        UltraPrecisionStats {
            avg_latency_ns: if count > 0 { sum_ns / count } else { 0 },
            min_latency_ns: if min_ns == u64::MAX { 0 } else { min_ns },
            max_latency_ns: max_ns,
            orders_processed: self.orders_processed.load(Ordering::Relaxed),
            trades_executed: self.trades_executed.load(Ordering::Relaxed),
            l1_updates: self.l1_updates.load(Ordering::Relaxed),
            l3_updates: self.l3_updates.load(Ordering::Relaxed),
            simd_operations: self.simd_operations.load(Ordering::Relaxed),
            kernel_bypass_operations: self.kernel_bypass_operations.load(Ordering::Relaxed),
            zero_copy_operations: self.zero_copy_operations.load(Ordering::Relaxed),
            cache_hit_rate: self.calculate_cache_hit_rate(),
        }
    }

    /// Calculate cache hit rate
    fn calculate_cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total > 0 {
            (hits as f64 / total as f64) * 100.0
        } else {
            0.0
        }
    }

    /// Print performance report
    pub fn print_performance_report(&self) {
        let stats = self.get_stats();

        log_info!(MAIN_LOGGER, "=== Ultra-Precision Performance Report ===");
        log_info!(
            MAIN_LOGGER,
            "Average Latency: {}ns ({:.3}µs)",
            stats.avg_latency_ns,
            stats.avg_latency_ns as f64 / 1000.0
        );
        log_info!(MAIN_LOGGER, "Min Latency: {}ns", stats.min_latency_ns);
        log_info!(
            MAIN_LOGGER,
            "Max Latency: {}ns ({:.3}µs)",
            stats.max_latency_ns,
            stats.max_latency_ns as f64 / 1000.0
        );
        log_info!(MAIN_LOGGER, "Orders Processed: {}", stats.orders_processed);
        log_info!(MAIN_LOGGER, "Trades Executed: {}", stats.trades_executed);
        log_info!(MAIN_LOGGER, "L1 Updates: {}", stats.l1_updates);
        log_info!(MAIN_LOGGER, "SIMD Operations: {}", stats.simd_operations);
        log_info!(
            MAIN_LOGGER,
            "Kernel Bypass Ops: {}",
            stats.kernel_bypass_operations
        );
        log_info!(MAIN_LOGGER, "Zero-Copy Ops: {}", stats.zero_copy_operations);
        log_info!(MAIN_LOGGER, "Cache Hit Rate: {:.2}%", stats.cache_hit_rate);

        // Performance warnings
        if stats.avg_latency_ns > 1000 {
            log_warn!(
                MAIN_LOGGER,
                "Average latency {}ns exceeds sub-microsecond target",
                stats.avg_latency_ns
            );
        }

        if stats.max_latency_ns > 5000 {
            log_warn!(
                MAIN_LOGGER,
                "Maximum latency {}ns indicates potential performance spikes",
                stats.max_latency_ns
            );
        }

        if stats.cache_hit_rate < 95.0 {
            log_warn!(
                MAIN_LOGGER,
                "Cache hit rate {:.2}% is below optimal threshold",
                stats.cache_hit_rate
            );
        }

        log_info!(MAIN_LOGGER, "==========================================");
    }
}

#[derive(Debug, Clone)]
pub struct UltraPrecisionStats {
    pub avg_latency_ns: u64,
    pub min_latency_ns: u64,
    pub max_latency_ns: u64,
    pub orders_processed: u64,
    pub trades_executed: u64,
    pub l1_updates: u64,
    pub l3_updates: u64,
    pub simd_operations: u64,
    pub kernel_bypass_operations: u64,
    pub zero_copy_operations: u64,
    pub cache_hit_rate: f64,
}

/// Global ultra-precision metrics instance
use std::sync::Once;
static mut GLOBAL_ULTRA_METRICS: Option<UltraPrecisionMetrics> = None;
static INIT_ULTRA_METRICS: Once = Once::new();

/// Initialize global ultra-precision metrics
pub fn init_global_ultra_metrics() {
    INIT_ULTRA_METRICS.call_once(|| unsafe {
        GLOBAL_ULTRA_METRICS = Some(UltraPrecisionMetrics::new());
    });
}

/// Get global ultra-precision metrics
pub fn get_global_ultra_metrics() -> &'static UltraPrecisionMetrics {
    INIT_ULTRA_METRICS.call_once(|| unsafe {
        GLOBAL_ULTRA_METRICS = Some(UltraPrecisionMetrics::new());
    });

    unsafe {
        let ptr = std::ptr::addr_of!(GLOBAL_ULTRA_METRICS);
        (*ptr)
            .as_ref()
            .expect("Ultra-precision metrics not initialized")
    }
}

/// Macro for timing operations with nanosecond precision
#[macro_export]
macro_rules! time_operation {
    ($metrics:expr, $operation:expr) => {{
        let start = $metrics.hardware_timestamp();
        let result = $operation;
        let end = $metrics.hardware_timestamp();
        let latency_cycles = end - start;
        let latency_ns = if $metrics.rdtsc_frequency > 0 {
            latency_cycles * 1000000 / $metrics.rdtsc_frequency
        } else {
            latency_cycles
        };
        $metrics.record_processing_latency_ns(latency_ns);
        result
    }};
}
