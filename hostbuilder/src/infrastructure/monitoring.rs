//! Comprehensive monitoring and health check system for DataEngine
//! 
//! Provides real-time health monitoring, performance tracking, and
//! alerting for production trading system operations.

use performance::PerformanceMetrics;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, atomic::{AtomicBool, AtomicU64, Ordering}};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::time::{interval, timeout};
use parking_lot::RwLock;
use crate::infrastructure::logging_facade::MAIN_LOGGER; use crate::{log_info, log_warn, log_error, log_debug};
use anyhow::{Result, anyhow};

/// Overall system health status
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    Healthy,
    Degraded,
    Unhealthy,
    Critical,
}

/// Health check result for individual components
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResult {
    pub component: String,
    pub status: HealthStatus,
    pub message: String,
    pub response_time_ms: u64,
    pub timestamp: u64,
    pub details: HashMap<String, serde_json::Value>,
}

impl HealthCheckResult {
    pub fn healthy(component: &str, response_time_ms: u64) -> Self {
        Self {
            component: component.to_string(),
            status: HealthStatus::Healthy,
            message: "All systems operational".to_string(),
            response_time_ms,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
            details: HashMap::new(),
        }
    }

    pub fn unhealthy(component: &str, message: &str, response_time_ms: u64) -> Self {
        Self {
            component: component.to_string(),
            status: HealthStatus::Unhealthy,
            message: message.to_string(),
            response_time_ms,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
            details: HashMap::new(),
        }
    }

    pub fn with_detail<V: serde::Serialize>(mut self, key: &str, value: V) -> Self {
        self.details.insert(
            key.to_string(), 
            serde_json::to_value(value).unwrap_or(serde_json::Value::Null)
        );
        self
    }
}

/// Trait for components that can be health checked
#[async_trait::async_trait]
pub trait HealthCheck: Send + Sync {
    async fn check_health(&self) -> HealthCheckResult;
    fn component_name(&self) -> &str;
}

/// WebSocket connection health checker
pub struct WebSocketHealthChecker {
    is_connected: Arc<AtomicBool>,
    last_message_time: Arc<AtomicU64>,
    connection_timeout: Duration,
}

impl WebSocketHealthChecker {
    pub fn new(is_connected: Arc<AtomicBool>, last_message_time: Arc<AtomicU64>) -> Self {
        Self {
            is_connected,
            last_message_time,
            connection_timeout: Duration::from_secs(30), // 30 second timeout
        }
    }

    pub fn record_message_received(&self) {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64;
        self.last_message_time.store(now, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl HealthCheck for WebSocketHealthChecker {
    async fn check_health(&self) -> HealthCheckResult {
        let start = Instant::now();
        let is_connected = self.is_connected.load(Ordering::Relaxed);
        let last_message = self.last_message_time.load(Ordering::Relaxed);
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64;
        let time_since_last_message = now - last_message;

        let response_time = start.elapsed().as_millis() as u64;

        if !is_connected {
            return HealthCheckResult::unhealthy(
                "websocket", 
                "WebSocket connection is down", 
                response_time
            ).with_detail("connected", false);
        }

        if time_since_last_message > self.connection_timeout.as_millis() as u64 {
            return HealthCheckResult::unhealthy(
                "websocket", 
                &format!("No messages received for {}ms", time_since_last_message),
                response_time
            ).with_detail("last_message_ms_ago", time_since_last_message);
        }

        HealthCheckResult::healthy("websocket", response_time)
            .with_detail("connected", true)
            .with_detail("last_message_ms_ago", time_since_last_message)
    }

    fn component_name(&self) -> &str {
        "websocket"
    }
}

/// Database connection health checker
#[cfg(feature = "database")]
pub struct DatabaseHealthChecker {
    postgres_pool: Arc<diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>>,
}

#[cfg(feature = "database")]
impl DatabaseHealthChecker {
    pub fn new(
        postgres_pool: Arc<diesel_async::pooled_connection::deadpool::Pool<diesel_async::AsyncPgConnection>>,
    ) -> Self {
        Self {
            postgres_pool,
        }
    }
}

#[cfg(feature = "database")]
#[async_trait::async_trait]
impl HealthCheck for DatabaseHealthChecker {
    async fn check_health(&self) -> HealthCheckResult {
        let start = Instant::now();
        let mut details = HashMap::new();
        let mut issues = Vec::new();

        // Check PostgreSQL
        match timeout(Duration::from_secs(5), self.postgres_pool.get()).await {
            Ok(Ok(mut conn)) => {
                // Simple query to test connection
                match diesel_async::RunQueryDsl::execute(
                    diesel::sql_query("SELECT 1"),
                    &mut *conn
                ).await {
                    Ok(_) => {
                        details.insert("postgres_status".to_string(), serde_json::json!("healthy"));
                    }
                    Err(_) => {
                        issues.push("PostgreSQL query failed");
                        details.insert("postgres_status".to_string(), serde_json::json!("query_failed"));
                    }
                }
            }
            _ => {
                issues.push("PostgreSQL connection failed");
                details.insert("postgres_status".to_string(), serde_json::json!("connection_failed"));
            }
        }

        let response_time = start.elapsed().as_millis() as u64;

        if issues.is_empty() {
            HealthCheckResult {
                component: "database".to_string(),
                status: HealthStatus::Healthy,
                message: "All database connections healthy".to_string(),
                response_time_ms: response_time,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
                details,
            }
        } else {
            HealthCheckResult {
                component: "database".to_string(),
                status: HealthStatus::Unhealthy,
                message: issues.join(", "),
                response_time_ms: response_time,
                timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
                details,
            }
        }
    }

    fn component_name(&self) -> &str {
        "database"
    }
}

/// Performance health checker
pub struct PerformanceHealthChecker {
    metrics: Arc<PerformanceMetrics>,
    latency_threshold_us: u64,
    error_rate_threshold: f64,
}

impl PerformanceHealthChecker {
    pub fn new(metrics: Arc<PerformanceMetrics>) -> Self {
        Self {
            metrics,
            latency_threshold_us: 1000, // 1ms
            error_rate_threshold: 0.01, // 1%
        }
    }
}

#[async_trait::async_trait]
impl HealthCheck for PerformanceHealthChecker {
    async fn check_health(&self) -> HealthCheckResult {
        let start = Instant::now();
        let snapshot = self.metrics.get_metrics_snapshot();
        let response_time = start.elapsed().as_millis() as u64;

        let mut status = HealthStatus::Healthy;
        let mut issues = Vec::new();

        // Check latency
        if snapshot.p99_latency_us > self.latency_threshold_us {
            status = HealthStatus::Degraded;
            issues.push(format!("High P99 latency: {}μs", snapshot.p99_latency_us));
        }

        // Check error rate
        if snapshot.messages_processed > 0 {
            let error_rate = (snapshot.total_errors as f64) / (snapshot.messages_processed as f64);
            if error_rate > self.error_rate_threshold {
                status = if status == HealthStatus::Healthy { 
                    HealthStatus::Degraded 
                } else { 
                    HealthStatus::Unhealthy 
                };
                issues.push(format!("High error rate: {:.2}%", error_rate * 100.0));
            }
        }

        // Check CPU usage
        if snapshot.cpu_usage_percent > 90 {
            status = HealthStatus::Critical;
            issues.push(format!("Critical CPU usage: {}%", snapshot.cpu_usage_percent));
        } else if snapshot.cpu_usage_percent > 80 {
            status = if matches!(status, HealthStatus::Healthy) {
                HealthStatus::Degraded
            } else {
                status
            };
            issues.push(format!("High CPU usage: {}%", snapshot.cpu_usage_percent));
        }

        let message = if issues.is_empty() {
            "Performance metrics within normal parameters".to_string()
        } else {
            issues.join(", ")
        };

        HealthCheckResult {
            component: "performance".to_string(),
            status,
            message,
            response_time_ms: response_time,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
            details: serde_json::to_value(&snapshot)
                .ok()
                .and_then(|v| v.as_object().cloned())
                .map(|obj| obj.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
                .unwrap_or_default(),
        }
    }

    fn component_name(&self) -> &str {
        "performance"
    }
}

/// Comprehensive system health monitor
pub struct SystemHealthMonitor {
    health_checkers: Vec<Arc<dyn HealthCheck>>,
    check_interval: Duration,
    last_check_results: Arc<RwLock<HashMap<String, HealthCheckResult>>>,
    overall_status: Arc<RwLock<HealthStatus>>,
    is_running: AtomicBool,
    start_time: Instant,
}

impl SystemHealthMonitor {
    pub fn new(check_interval: Duration) -> Self {
        Self {
            health_checkers: Vec::new(),
            check_interval,
            last_check_results: Arc::new(RwLock::new(HashMap::new())),
            overall_status: Arc::new(RwLock::new(HealthStatus::Healthy)),
            is_running: AtomicBool::new(false),
            start_time: Instant::now(),
        }
    }

    pub fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }

    pub fn add_health_checker(&mut self, checker: Arc<dyn HealthCheck>) {
        self.health_checkers.push(checker);
    }

    pub async fn start(&self) -> Result<()> {
        if self.is_running.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_err() {
            return Err(anyhow!("Health monitor is already running"));
        }

        log_info!(MAIN_LOGGER, "Starting system health monitor with {} checkers", self.health_checkers.len());
        
        let mut interval = interval(self.check_interval);
        let results: Arc<RwLock<HashMap<String, HealthCheckResult>>> = Arc::clone(&self.last_check_results);
        let status: Arc<RwLock<HealthStatus>> = Arc::clone(&self.overall_status);
        let checkers = self.health_checkers.clone();

        tokio::spawn(async move {
            loop {
                interval.tick().await;
                
                let mut check_results = HashMap::new();
                let mut worst_status = HealthStatus::Healthy;

                // Run all health checks concurrently
                let check_futures: Vec<_> = checkers.iter().map(|checker| {
                    let checker = Arc::clone(checker);
                    tokio::spawn(async move {
                        checker.check_health().await
                    })
                }).collect();

                for future in check_futures {
                    match future.await {
                        Ok(result) => {
                            log_debug!(MAIN_LOGGER, "Health check result for {}: {:?}", result.component, result.status);
                            
                            // Update worst status
                            match result.status {
                                HealthStatus::Critical => worst_status = HealthStatus::Critical,
                                HealthStatus::Unhealthy if !matches!(worst_status, HealthStatus::Critical) => {
                                    worst_status = HealthStatus::Unhealthy;
                                }
                                HealthStatus::Degraded if matches!(worst_status, HealthStatus::Healthy) => {
                                    worst_status = HealthStatus::Degraded;
                                }
                                _ => {}
                            }

                            check_results.insert(result.component.clone(), result);
                        }
                        Err(e) => {
                            log_error!(MAIN_LOGGER, "Health check task failed: {}", e);
                            worst_status = HealthStatus::Critical;
                        }
                    }
                }

                // Update global state
                {
                    let mut results_guard = results.write();
                    *results_guard = check_results;
                }

                {
                    let mut status_guard = status.write();
                    let old_status = status_guard.clone();
                    *status_guard = worst_status.clone();
                    
                    if old_status != worst_status {
                        match worst_status {
                            HealthStatus::Critical => log_error!(MAIN_LOGGER, "System status changed to CRITICAL"),
                            HealthStatus::Unhealthy => log_warn!(MAIN_LOGGER, "System status changed to UNHEALTHY"),
                            HealthStatus::Degraded => log_warn!(MAIN_LOGGER, "System status changed to DEGRADED"),
                            HealthStatus::Healthy => log_info!(MAIN_LOGGER, "System status changed to HEALTHY"),
                        }
                    }
                }
            }
        });

        Ok(())
    }

    pub fn get_overall_status(&self) -> HealthStatus {
        self.overall_status.read().clone()
    }

    pub fn get_component_status(&self, component: &str) -> Option<HealthCheckResult> {
        self.last_check_results.read().get(component).cloned()
    }

    pub fn get_all_component_statuses(&self) -> HashMap<String, HealthCheckResult> {
        self.last_check_results.read().clone()
    }

    pub fn is_healthy(&self) -> bool {
        matches!(self.get_overall_status(), HealthStatus::Healthy)
    }

    /// Get comprehensive health report
    pub fn get_health_report(&self) -> SystemHealthReport {
        let overall_status = self.get_overall_status();
        let components = self.get_all_component_statuses();
        let uptime = self.start_time.elapsed();
        
        // Convert HealthCheckResult to ComponentHealthStatus for compatibility
        let component_health: HashMap<String, ComponentHealthStatus> = components
            .iter()
            .map(|(name, result)| {
                (
                    name.clone(),
                    ComponentHealthStatus {
                        status: result.status.clone(),
                        message: Some(result.message.clone()),
                        last_check: result.timestamp,
                    },
                )
            })
            .collect();
        
        SystemHealthReport {
            overall_status,
            components: components.clone(),
            component_health,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
            summary: self.generate_summary(&components),
            uptime,
        }
    }

    fn generate_summary(&self, components: &HashMap<String, HealthCheckResult>) -> String {
        let healthy = components.values().filter(|r| r.status == HealthStatus::Healthy).count();
        let degraded = components.values().filter(|r| r.status == HealthStatus::Degraded).count();
        let unhealthy = components.values().filter(|r| r.status == HealthStatus::Unhealthy).count();
        let critical = components.values().filter(|r| r.status == HealthStatus::Critical).count();

        format!(
            "{} components: {} healthy, {} degraded, {} unhealthy, {} critical",
            components.len(), healthy, degraded, unhealthy, critical
        )
    }

    /// Check health of all components and return comprehensive report
    pub async fn check_health(&self) -> Result<SystemHealthReport> {
        let mut check_results = HashMap::new();
        let mut worst_status = HealthStatus::Healthy;

        // Run all health checks concurrently
        let check_futures: Vec<_> = self.health_checkers.iter().map(|checker| {
            let checker = Arc::clone(checker);
            tokio::spawn(async move {
                checker.check_health().await
            })
        }).collect();

        for future in check_futures {
            match future.await {
                Ok(result) => {
                    // Update worst status
                    match result.status {
                        HealthStatus::Critical => worst_status = HealthStatus::Critical,
                        HealthStatus::Unhealthy if !matches!(worst_status, HealthStatus::Critical) => {
                            worst_status = HealthStatus::Unhealthy;
                        }
                        HealthStatus::Degraded if matches!(worst_status, HealthStatus::Healthy) => {
                            worst_status = HealthStatus::Degraded;
                        }
                        _ => {}
                    }

                    check_results.insert(result.component.clone(), result);
                }
                Err(e) => {
                    log_error!(MAIN_LOGGER, "Health check task failed: {}", e);
                    worst_status = HealthStatus::Critical;
                    
                    // Create error result for failed check
                    let error_result = HealthCheckResult {
                        component: "unknown".to_string(),
                        status: HealthStatus::Critical,
                        message: format!("Health check failed: {}", e),
                        response_time_ms: 0,
                        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
                        details: HashMap::new(),
                    };
                    check_results.insert("unknown".to_string(), error_result);
                }
            }
        }

        // Convert HealthCheckResult to ComponentHealthStatus for compatibility
        let component_health: HashMap<String, ComponentHealthStatus> = check_results
            .iter()
            .map(|(name, result)| {
                (
                    name.clone(),
                    ComponentHealthStatus {
                        status: result.status.clone(),
                        message: Some(result.message.clone()),
                        last_check: result.timestamp,
                    },
                )
            })
            .collect();

        Ok(SystemHealthReport {
            overall_status: worst_status,
            components: check_results.clone(),
            component_health,
            timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64,
            summary: self.generate_summary(&check_results),
            uptime: self.start_time.elapsed(),
        })
    }

    pub fn stop(&self) {
        self.is_running.store(false, Ordering::SeqCst);
        log_info!(MAIN_LOGGER, "System health monitor stopped");
    }
}

/// Comprehensive health report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemHealthReport {
    pub overall_status: HealthStatus,
    pub components: HashMap<String, HealthCheckResult>,
    pub component_health: HashMap<String, ComponentHealthStatus>, // For compatibility
    pub timestamp: u64,
    pub summary: String,
    pub uptime: Duration,
}

/// Component health status for compatibility
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComponentHealthStatus {
    pub status: HealthStatus,
    pub message: Option<String>,
    pub last_check: u64,
}

impl SystemHealthReport {
    /// Convert to JSON for API responses
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| anyhow!("Failed to serialize health report: {}", e))
    }

    /// Check if system needs immediate attention
    pub fn needs_immediate_attention(&self) -> bool {
        matches!(self.overall_status, HealthStatus::Critical | HealthStatus::Unhealthy)
    }

    /// Get components that are not healthy
    pub fn get_problematic_components(&self) -> Vec<&HealthCheckResult> {
        self.components.values()
            .filter(|result| !matches!(result.status, HealthStatus::Healthy))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    struct MockHealthChecker {
        component: String,
        should_fail: AtomicBool,
    }

    impl MockHealthChecker {
        fn new(component: &str) -> Self {
            Self {
                component: component.to_string(),
                should_fail: AtomicBool::new(false),
            }
        }

        fn set_should_fail(&self, fail: bool) {
            self.should_fail.store(fail, Ordering::Relaxed);
        }
    }

    #[async_trait::async_trait]
    impl HealthCheck for MockHealthChecker {
        async fn check_health(&self) -> HealthCheckResult {
            if self.should_fail.load(Ordering::Relaxed) {
                HealthCheckResult::unhealthy(&self.component, "Mock failure", 10)
            } else {
                HealthCheckResult::healthy(&self.component, 5)
            }
        }

        fn component_name(&self) -> &str {
            &self.component
        }
    }

    #[tokio::test]
    async fn test_health_monitor() {
        let mut monitor = SystemHealthMonitor::new(Duration::from_millis(100));
        let checker = Arc::new(MockHealthChecker::new("test"));
        
        monitor.add_health_checker(checker.clone());
        
        // Start monitoring
        monitor.start().await.unwrap();
        
        // Wait for first check
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Should be healthy
        assert_eq!(monitor.get_overall_status(), HealthStatus::Healthy);
        
        // Make it fail
        checker.set_should_fail(true);
        
        // Wait for check
        tokio::time::sleep(Duration::from_millis(200)).await;
        
        // Should be unhealthy now
        assert_eq!(monitor.get_overall_status(), HealthStatus::Unhealthy);
        
        monitor.stop();
    }

    #[tokio::test]
    async fn test_websocket_health_checker() {
        let is_connected = Arc::new(AtomicBool::new(true));
        let last_message_time = Arc::new(AtomicU64::new(
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_millis() as u64
        ));
        
        let checker = WebSocketHealthChecker::new(
            Arc::clone(&is_connected), 
            Arc::clone(&last_message_time)
        );
        
        // Should be healthy
        let result = checker.check_health().await;
        assert_eq!(result.status, HealthStatus::Healthy);
        
        // Disconnect
        is_connected.store(false, Ordering::Relaxed);
        let result = checker.check_health().await;
        assert_eq!(result.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_health_check_result_constructors() {
        let healthy = HealthCheckResult::healthy("db", 5);
        assert_eq!(healthy.component, "db");
        assert_eq!(healthy.status, HealthStatus::Healthy);
        assert_eq!(healthy.response_time_ms, 5);

        let unhealthy = HealthCheckResult::unhealthy("ws", "connection lost", 100);
        assert_eq!(unhealthy.component, "ws");
        assert_eq!(unhealthy.status, HealthStatus::Unhealthy);
        assert_eq!(unhealthy.message, "connection lost");
    }

    #[test]
    fn test_health_check_result_with_detail() {
        let result = HealthCheckResult::healthy("api", 2)
            .with_detail("version", "1.0")
            .with_detail("connections", 42);
        assert_eq!(result.details.len(), 2);
        assert_eq!(result.details["version"], serde_json::json!("1.0"));
        assert_eq!(result.details["connections"], serde_json::json!(42));
    }

    #[test]
    fn test_health_report_needs_attention() {
        let make_report = |status: HealthStatus| SystemHealthReport {
            overall_status: status,
            components: HashMap::new(),
            component_health: HashMap::new(),
            timestamp: 0,
            summary: String::new(),
            uptime: Duration::ZERO,
        };
        assert!(make_report(HealthStatus::Critical).needs_immediate_attention());
        assert!(make_report(HealthStatus::Unhealthy).needs_immediate_attention());
        assert!(!make_report(HealthStatus::Healthy).needs_immediate_attention());
        assert!(!make_report(HealthStatus::Degraded).needs_immediate_attention());
    }

    #[test]
    fn test_health_report_problematic_components() {
        let mut components = HashMap::new();
        components.insert("ok".to_string(), HealthCheckResult::healthy("ok", 1));
        components.insert("bad".to_string(), HealthCheckResult::unhealthy("bad", "down", 50));
        let report = SystemHealthReport {
            overall_status: HealthStatus::Unhealthy,
            components,
            component_health: HashMap::new(),
            timestamp: 0,
            summary: String::new(),
            uptime: Duration::ZERO,
        };
        let problems = report.get_problematic_components();
        assert_eq!(problems.len(), 1);
        assert_eq!(problems[0].component, "bad");
    }

    #[test]
    fn test_metrics_collector() {
        let collector = MetricsCollector::new();
        collector.record_processing_latency(Duration::from_millis(10));
        collector.record_trade("BTC/USD", 50000.0, 1.0);
        assert_eq!(collector.processing_count.load(Ordering::Relaxed), 2);
    }
}

/// Simple metrics collector for data pipeline compatibility
pub struct MetricsCollector {
    processing_count: AtomicU64,
}

impl MetricsCollector {
    pub fn new() -> Self {
        Self {
            processing_count: AtomicU64::new(0),
        }
    }

    pub fn record_processing_latency(&self, _latency: Duration) {
        self.processing_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_trade(&self, _symbol: &str, _price: f64, _quantity: f64) {
        self.processing_count.fetch_add(1, Ordering::Relaxed);
    }
}

