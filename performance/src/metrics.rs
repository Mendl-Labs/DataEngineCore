//! Ultra-high performance metrics collection for DataEngine
//! 
//! Provides sub-microsecond precision timing and zero-allocation metrics
//! collection for trading system performance monitoring.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};
use std::collections::VecDeque;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

/// High-performance metrics collector for trading operations
pub struct PerformanceMetrics {
    // Message processing metrics
    messages_processed: AtomicU64,
    messages_per_second: AtomicU64,
    
    // Latency tracking (nanoseconds)
    min_latency_ns: AtomicU64,
    max_latency_ns: AtomicU64,
    avg_latency_ns: AtomicU64,
    total_latency_ns: AtomicU64,
    
    // Memory and resource usage
    memory_usage: AtomicUsize,
    cpu_usage_percent: AtomicU64,
    
    // Connection and error metrics
    active_connections: AtomicU64,
    failed_connections: AtomicU64,
    total_errors: AtomicU64,
    
    // Throughput tracking
    bytes_received: AtomicU64,
    bytes_sent: AtomicU64,
    
    // Latency histogram for percentile calculations
    latency_histogram: RwLock<LatencyHistogram>,
    
    // Start time for uptime calculation
    start_time: Instant,
}

impl PerformanceMetrics {
    pub fn new() -> Self {
        Self {
            messages_processed: AtomicU64::new(0),
            messages_per_second: AtomicU64::new(0),
            min_latency_ns: AtomicU64::new(u64::MAX),
            max_latency_ns: AtomicU64::new(0),
            avg_latency_ns: AtomicU64::new(0),
            total_latency_ns: AtomicU64::new(0),
            memory_usage: AtomicUsize::new(0),
            cpu_usage_percent: AtomicU64::new(0),
            active_connections: AtomicU64::new(0),
            failed_connections: AtomicU64::new(0),
            total_errors: AtomicU64::new(0),
            bytes_received: AtomicU64::new(0),
            bytes_sent: AtomicU64::new(0),
            latency_histogram: RwLock::new(LatencyHistogram::new()),
            start_time: Instant::now(),
        }
    }

    /// Record message processing latency (high-performance path)
    #[inline(always)]
    pub fn record_message_latency(&self, latency_ns: u64) {
        // Update counters atomically
        let messages = self.messages_processed.fetch_add(1, Ordering::Relaxed);
        let total_latency = self.total_latency_ns.fetch_add(latency_ns, Ordering::Relaxed);
        
        // Update average (approximate for performance)
        let new_avg = (total_latency + latency_ns) / (messages + 1);
        self.avg_latency_ns.store(new_avg, Ordering::Relaxed);
        
        // Update min/max
        self.update_min_latency(latency_ns);
        self.update_max_latency(latency_ns);
        
        // Update histogram for percentile calculation
        if let Some(mut histogram) = self.latency_histogram.try_write() {
            histogram.record(latency_ns);
        }
    }

    #[inline(always)]
    fn update_min_latency(&self, latency_ns: u64) {
        let mut current = self.min_latency_ns.load(Ordering::Relaxed);
        while latency_ns < current {
            match self.min_latency_ns.compare_exchange_weak(
                current, latency_ns, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    #[inline(always)]
    fn update_max_latency(&self, latency_ns: u64) {
        let mut current = self.max_latency_ns.load(Ordering::Relaxed);
        while latency_ns > current {
            match self.max_latency_ns.compare_exchange_weak(
                current, latency_ns, Ordering::Relaxed, Ordering::Relaxed
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    /// Record network bytes (zero-allocation)
    #[inline(always)]
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received.fetch_add(bytes, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record connection events
    #[inline(always)]
    pub fn record_connection_opened(&self) {
        self.active_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_connection_closed(&self) {
        self.active_connections.fetch_sub(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_connection_failed(&self) {
        self.failed_connections.fetch_add(1, Ordering::Relaxed);
    }

    #[inline(always)]
    pub fn record_error(&self) {
        self.total_errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Update system resource metrics
    pub fn update_system_metrics(&self, memory_bytes: usize, cpu_percent: u64) {
        self.memory_usage.store(memory_bytes, Ordering::Relaxed);
        self.cpu_usage_percent.store(cpu_percent, Ordering::Relaxed);
    }

    /// Calculate messages per second
    pub fn update_throughput(&self) {
        let messages = self.messages_processed.load(Ordering::Relaxed);
        let elapsed_secs = self.start_time.elapsed().as_secs();
        
        if elapsed_secs > 0 {
            let mps = messages / elapsed_secs;
            self.messages_per_second.store(mps, Ordering::Relaxed);
        }
    }

    /// Get comprehensive metrics snapshot
    pub fn get_metrics_snapshot(&self) -> MetricsSnapshot {
        let histogram = self.latency_histogram.read();
        
        MetricsSnapshot {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
                
            // Message metrics
            messages_processed: self.messages_processed.load(Ordering::Relaxed),
            messages_per_second: self.messages_per_second.load(Ordering::Relaxed),
            
            // Latency metrics (convert to microseconds for readability)
            min_latency_us: self.min_latency_ns.load(Ordering::Relaxed) / 1000,
            max_latency_us: self.max_latency_ns.load(Ordering::Relaxed) / 1000,
            avg_latency_us: self.avg_latency_ns.load(Ordering::Relaxed) / 1000,
            p50_latency_us: histogram.percentile(50.0) / 1000,
            p95_latency_us: histogram.percentile(95.0) / 1000,
            p99_latency_us: histogram.percentile(99.0) / 1000,
            p999_latency_us: histogram.percentile(99.9) / 1000,
            
            // Resource metrics
            memory_usage_mb: (self.memory_usage.load(Ordering::Relaxed) / (1024 * 1024)) as u64,
            cpu_usage_percent: self.cpu_usage_percent.load(Ordering::Relaxed),
            
            // Connection metrics
            active_connections: self.active_connections.load(Ordering::Relaxed),
            failed_connections: self.failed_connections.load(Ordering::Relaxed),
            total_errors: self.total_errors.load(Ordering::Relaxed),
            
            // Network metrics
            bytes_received_mb: self.bytes_received.load(Ordering::Relaxed) / (1024 * 1024),
            bytes_sent_mb: self.bytes_sent.load(Ordering::Relaxed) / (1024 * 1024),
            
            // Uptime
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }

    /// Reset all metrics (useful for benchmarking)
    pub fn reset(&self) {
        self.messages_processed.store(0, Ordering::Relaxed);
        self.messages_per_second.store(0, Ordering::Relaxed);
        self.min_latency_ns.store(u64::MAX, Ordering::Relaxed);
        self.max_latency_ns.store(0, Ordering::Relaxed);
        self.avg_latency_ns.store(0, Ordering::Relaxed);
        self.total_latency_ns.store(0, Ordering::Relaxed);
        self.total_errors.store(0, Ordering::Relaxed);
        self.bytes_received.store(0, Ordering::Relaxed);
        self.bytes_sent.store(0, Ordering::Relaxed);
        
        if let Some(mut histogram) = self.latency_histogram.try_write() {
            histogram.clear();
        }
    }
}

/// Latency histogram for percentile calculations
struct LatencyHistogram {
    buckets: VecDeque<u64>,
    max_samples: usize,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: VecDeque::with_capacity(10000), // Keep last 10k samples
            max_samples: 10000,
        }
    }

    fn record(&mut self, latency_ns: u64) {
        if self.buckets.len() >= self.max_samples {
            self.buckets.pop_front();
        }
        self.buckets.push_back(latency_ns);
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.buckets.is_empty() {
            return 0;
        }

        let mut sorted: Vec<u64> = self.buckets.iter().copied().collect();
        sorted.sort_unstable();

        let index = ((p / 100.0) * (sorted.len() - 1) as f64) as usize;
        sorted[index.min(sorted.len() - 1)]
    }

    fn clear(&mut self) {
        self.buckets.clear();
    }
}

/// Comprehensive metrics snapshot for monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    pub timestamp: u64,
    
    // Message processing
    pub messages_processed: u64,
    pub messages_per_second: u64,
    
    // Latency (microseconds)
    pub min_latency_us: u64,
    pub max_latency_us: u64,
    pub avg_latency_us: u64,
    pub p50_latency_us: u64,
    pub p95_latency_us: u64,
    pub p99_latency_us: u64,
    pub p999_latency_us: u64,
    
    // Resources
    pub memory_usage_mb: u64,
    pub cpu_usage_percent: u64,
    
    // Connections
    pub active_connections: u64,
    pub failed_connections: u64,
    pub total_errors: u64,
    
    // Network
    pub bytes_received_mb: u64,
    pub bytes_sent_mb: u64,
    
    // System
    pub uptime_seconds: u64,
}

impl MetricsSnapshot {
    /// Check if any metrics indicate performance issues
    pub fn has_performance_issues(&self) -> bool {
        self.p99_latency_us > 1000 || // P99 > 1ms
        self.cpu_usage_percent > 80 || 
        self.memory_usage_mb > 8192 || // > 8GB
        self.total_errors > 0
    }

    /// Get performance score (0-100, higher is better)
    pub fn performance_score(&self) -> u32 {
        let mut score = 100u32;
        
        // Penalize high latency
        if self.p99_latency_us > 500 { score -= 20; }
        if self.p99_latency_us > 1000 { score -= 30; }
        
        // Penalize high CPU usage
        if self.cpu_usage_percent > 70 { score -= 15; }
        if self.cpu_usage_percent > 90 { score -= 25; }
        
        // Penalize errors
        if self.total_errors > 0 { 
            score = score.saturating_sub((self.total_errors as u32).min(50));
        }
        
        score
    }
}

/// Global metrics instance for the entire DataEngine
use once_cell::sync::Lazy;
use std::sync::Arc;

static GLOBAL_METRICS: Lazy<Arc<PerformanceMetrics>> = Lazy::new(|| {
    Arc::new(PerformanceMetrics::new())
});

/// Get reference to global metrics instance
pub fn global_metrics() -> &'static Arc<PerformanceMetrics> {
    &GLOBAL_METRICS
}

/// High-performance timing utilities
pub struct Timer {
    start: Instant,
}

impl Timer {
    #[inline(always)]
    pub fn start() -> Self {
        Self {
            start: Instant::now(),
        }
    }

    #[inline(always)]
    pub fn elapsed_ns(&self) -> u64 {
        self.start.elapsed().as_nanos() as u64
    }

    #[inline(always)]
    pub fn record_and_reset(&mut self, metrics: &PerformanceMetrics) -> u64 {
        let elapsed = self.elapsed_ns();
        metrics.record_message_latency(elapsed);
        self.start = Instant::now();
        elapsed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_metrics_recording() {
        let metrics = PerformanceMetrics::new();
        
        // Record some latencies
        metrics.record_message_latency(1000); // 1μs
        metrics.record_message_latency(2000); // 2μs  
        metrics.record_message_latency(500);  // 0.5μs
        
        let snapshot = metrics.get_metrics_snapshot();
        assert_eq!(snapshot.messages_processed, 3);
        assert_eq!(snapshot.min_latency_us, 0); // 500ns = 0μs when truncated
        assert_eq!(snapshot.max_latency_us, 2);
    }

    #[test]
    fn test_timer() {
        let timer = Timer::start();
        thread::sleep(Duration::from_millis(1));
        let elapsed = timer.elapsed_ns();
        assert!(elapsed > 500_000); // Should be > 0.5ms
    }

    #[test]
    fn test_performance_score() {
        let snapshot = MetricsSnapshot {
            timestamp: 0,
            messages_processed: 1000,
            messages_per_second: 1000,
            min_latency_us: 100,
            max_latency_us: 500,
            avg_latency_us: 200,
            p50_latency_us: 200,
            p95_latency_us: 400,
            p99_latency_us: 450,
            p999_latency_us: 500,
            memory_usage_mb: 1024,
            cpu_usage_percent: 50,
            active_connections: 10,
            failed_connections: 0,
            total_errors: 0,
            bytes_received_mb: 100,
            bytes_sent_mb: 50,
            uptime_seconds: 3600,
        };
        
        let score = snapshot.performance_score();
        assert_eq!(score, 100); // Perfect performance
    }
}
