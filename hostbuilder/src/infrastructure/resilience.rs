//! Centralized error handling and resilience patterns for DataEngine
//!
//! This module provides:
//! - Circuit breaker pattern for external dependencies
//! - Retry mechanisms with exponential backoff
//! - Error categorization and recovery strategies
//! - Performance-aware error handling

use crate::{infrastructure::logging_facade::MAIN_LOGGER, log_error, log_info, log_warn};
use anyhow::{anyhow, Result};
use fastrand;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tokio::time::sleep;

/// Circuit breaker states
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CircuitState {
    Closed,   // Normal operation
    Open,     // Failing, reject requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker for external service dependencies
pub struct CircuitBreaker {
    state: Arc<AtomicU64>, // 0=Closed, 1=Open, 2=HalfOpen
    failure_count: Arc<AtomicU64>,
    success_count: Arc<AtomicU64>,
    last_failure_time: Arc<AtomicU64>,
    failure_threshold: u64,
    recovery_timeout: Duration,
    _reset_timeout: Duration,
}

impl CircuitBreaker {
    pub fn new(
        failure_threshold: u64,
        recovery_timeout: Duration,
        reset_timeout: Duration,
    ) -> Self {
        Self {
            state: Arc::new(AtomicU64::new(0)), // Start closed
            failure_count: Arc::new(AtomicU64::new(0)),
            success_count: Arc::new(AtomicU64::new(0)),
            last_failure_time: Arc::new(AtomicU64::new(0)),
            failure_threshold,
            recovery_timeout,
            _reset_timeout: reset_timeout,
        }
    }

    pub fn get_state(&self) -> CircuitState {
        match self.state.load(Ordering::Acquire) {
            0 => CircuitState::Closed,
            1 => CircuitState::Open,
            2 => CircuitState::HalfOpen,
            _ => CircuitState::Closed, // Default fallback
        }
    }

    pub async fn call<F, T, E>(&self, operation: F) -> Result<T>
    where
        F: std::future::Future<Output = std::result::Result<T, E>>,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        // Check if circuit is open
        if self.get_state() == CircuitState::Open {
            let now = Instant::now().elapsed().as_millis() as u64;
            let last_failure = self.last_failure_time.load(Ordering::Acquire);

            if now - last_failure < self.recovery_timeout.as_millis() as u64 {
                return Err(anyhow!("Circuit breaker is OPEN - service unavailable"));
            } else {
                // Try half-open
                self.state.store(2, Ordering::Release);
            }
        }

        // Execute operation
        match operation.await {
            Ok(result) => {
                self.on_success();
                Ok(result)
            }
            Err(e) => {
                self.on_failure();
                Err(anyhow!("Operation failed: {}", e))
            }
        }
    }

    fn on_success(&self) {
        let current_state = self.get_state();
        self.success_count.fetch_add(1, Ordering::Relaxed);

        match current_state {
            CircuitState::HalfOpen => {
                // Reset to closed state
                self.state.store(0, Ordering::Release);
                self.failure_count.store(0, Ordering::Relaxed);
                log_info!(MAIN_LOGGER, "Circuit breaker reset to CLOSED state");
            }
            CircuitState::Closed => {
                // Reset failure count on success
                self.failure_count.store(0, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    fn on_failure(&self) {
        let failures = self.failure_count.fetch_add(1, Ordering::Relaxed) + 1;
        let now = Instant::now().elapsed().as_millis() as u64;
        self.last_failure_time.store(now, Ordering::Release);

        if failures >= self.failure_threshold {
            self.state.store(1, Ordering::Release); // Open
            log_warn!(
                MAIN_LOGGER,
                "Circuit breaker opened after {} failures",
                failures
            );
        }
    }

    pub fn get_metrics(&self) -> CircuitBreakerMetrics {
        CircuitBreakerMetrics {
            state: self.get_state(),
            failure_count: self.failure_count.load(Ordering::Relaxed),
            success_count: self.success_count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CircuitBreakerMetrics {
    pub state: CircuitState,
    pub failure_count: u64,
    pub success_count: u64,
}

/// Retry configuration for different operation types
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
    pub backoff_multiplier: f64,
    pub jitter: bool,
}

impl RetryConfig {
    pub fn new_fast() -> Self {
        Self {
            max_attempts: 3,
            base_delay: Duration::from_millis(10),
            max_delay: Duration::from_millis(100),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    pub fn new_standard() -> Self {
        Self {
            max_attempts: 5,
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }

    pub fn new_persistent() -> Self {
        Self {
            max_attempts: 10,
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 1.5,
            jitter: true,
        }
    }
}

/// Resilient operation executor with retry and circuit breaker
pub struct ResilientExecutor {
    circuit_breaker: Arc<CircuitBreaker>,
    retry_config: RetryConfig,
}

impl ResilientExecutor {
    pub fn new(circuit_breaker: Arc<CircuitBreaker>, retry_config: RetryConfig) -> Self {
        Self {
            circuit_breaker,
            retry_config,
        }
    }

    pub async fn execute<F, T, E>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> std::pin::Pin<
            Box<dyn std::future::Future<Output = std::result::Result<T, E>> + Send>,
        >,
        E: std::fmt::Display + Send + Sync + 'static,
    {
        let mut attempt = 1;
        let mut delay = self.retry_config.base_delay;

        loop {
            // Try operation through circuit breaker
            match self.circuit_breaker.call(operation()).await {
                Ok(result) => return Ok(result),
                Err(e) if attempt >= self.retry_config.max_attempts => {
                    log_error!(
                        MAIN_LOGGER,
                        "Operation failed after {} attempts: {}",
                        attempt,
                        e
                    );
                    return Err(e);
                }
                Err(e) => {
                    log_warn!(
                        MAIN_LOGGER,
                        "Attempt {} failed: {}, retrying in {:?}",
                        attempt,
                        e,
                        delay
                    );

                    // Sleep with exponential backoff
                    sleep(delay).await;

                    // Calculate next delay
                    delay = Duration::from_millis(std::cmp::min(
                        (delay.as_millis() as f64 * self.retry_config.backoff_multiplier) as u64,
                        self.retry_config.max_delay.as_millis() as u64,
                    ));

                    // Add jitter to prevent thundering herd
                    if self.retry_config.jitter {
                        let jitter_ms = fastrand::u64(0..=delay.as_millis() as u64 / 4);
                        delay = Duration::from_millis(delay.as_millis() as u64 + jitter_ms);
                    }

                    attempt += 1;
                }
            }
        }
    }
}

/// Error categorization for different handling strategies
#[derive(Debug, Clone, PartialEq)]
pub enum ErrorCategory {
    /// Transient errors that should be retried
    Transient,
    /// Permanent errors that should not be retried
    Permanent,
    /// Rate limiting errors - backoff required
    RateLimit,
    /// Authentication/authorization errors
    Auth,
    /// Network connectivity issues
    Network,
    /// Data validation errors
    Validation,
}

pub trait CategorizeError {
    fn categorize(&self) -> ErrorCategory;
}

/// DataEngine-specific error types
#[derive(Debug, thiserror::Error)]
pub enum DataEngineError {
    #[error("WebSocket connection failed: {0}")]
    WebSocketConnection(String),

    #[error("Message parsing failed: {0}")]
    MessageParsing(String),

    #[error("Database operation failed: {0}")]
    Database(String),

    #[error("Redis operation failed: {0}")]
    Redis(String),

    #[error("Publisher error: {0}")]
    Publisher(String),

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("Rate limit exceeded: {0}")]
    RateLimit(String),

    #[error("Authentication failed: {0}")]
    Authentication(String),
}

impl CategorizeError for DataEngineError {
    fn categorize(&self) -> ErrorCategory {
        match self {
            DataEngineError::WebSocketConnection(_) => ErrorCategory::Network,
            DataEngineError::MessageParsing(_) => ErrorCategory::Permanent,
            DataEngineError::Database(_) => ErrorCategory::Transient,
            DataEngineError::Redis(_) => ErrorCategory::Transient,
            DataEngineError::Publisher(_) => ErrorCategory::Transient,
            DataEngineError::Configuration(_) => ErrorCategory::Permanent,
            DataEngineError::RateLimit(_) => ErrorCategory::RateLimit,
            DataEngineError::Authentication(_) => ErrorCategory::Auth,
        }
    }
}

/// Global error handler for the DataEngine system
pub struct ErrorHandler {
    metrics: ErrorMetrics,
}

impl Default for ErrorHandler {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorHandler {
    pub fn new() -> Self {
        Self {
            metrics: ErrorMetrics::new(),
        }
    }

    pub async fn handle_error<E>(&self, error: E, context: &str) -> Result<()>
    where
        E: CategorizeError + std::fmt::Display,
    {
        let category = error.categorize();
        self.metrics.record_error(&category);

        match category {
            ErrorCategory::Transient => {
                log_warn!(MAIN_LOGGER, "Transient error in {}: {}", context, error);
                // Could implement automatic retry here
            }
            ErrorCategory::Permanent => {
                log_error!(MAIN_LOGGER, "Permanent error in {}: {}", context, error);
                // Log for investigation, don't retry
            }
            ErrorCategory::RateLimit => {
                log_warn!(MAIN_LOGGER, "Rate limit in {}: {}", context, error);
                // Implement backoff strategy
            }
            ErrorCategory::Network => {
                log_warn!(MAIN_LOGGER, "Network error in {}: {}", context, error);
                // Could trigger circuit breaker
            }
            _ => {
                log_error!(
                    MAIN_LOGGER,
                    "Error in {}: {} (category: {:?})",
                    context,
                    error,
                    category
                );
            }
        }

        Ok(())
    }

    pub fn get_metrics(&self) -> &ErrorMetrics {
        &self.metrics
    }
}

/// Error metrics for monitoring and alerting
pub struct ErrorMetrics {
    transient_errors: AtomicU64,
    permanent_errors: AtomicU64,
    rate_limit_errors: AtomicU64,
    network_errors: AtomicU64,
    auth_errors: AtomicU64,
    validation_errors: AtomicU64,
}

impl Default for ErrorMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl ErrorMetrics {
    pub fn new() -> Self {
        Self {
            transient_errors: AtomicU64::new(0),
            permanent_errors: AtomicU64::new(0),
            rate_limit_errors: AtomicU64::new(0),
            network_errors: AtomicU64::new(0),
            auth_errors: AtomicU64::new(0),
            validation_errors: AtomicU64::new(0),
        }
    }

    pub fn record_error(&self, category: &ErrorCategory) {
        match category {
            ErrorCategory::Transient => self.transient_errors.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::Permanent => self.permanent_errors.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::RateLimit => self.rate_limit_errors.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::Network => self.network_errors.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::Auth => self.auth_errors.fetch_add(1, Ordering::Relaxed),
            ErrorCategory::Validation => self.validation_errors.fetch_add(1, Ordering::Relaxed),
        };
    }

    pub fn get_error_counts(&self) -> ErrorCounts {
        ErrorCounts {
            transient: self.transient_errors.load(Ordering::Relaxed),
            permanent: self.permanent_errors.load(Ordering::Relaxed),
            rate_limit: self.rate_limit_errors.load(Ordering::Relaxed),
            network: self.network_errors.load(Ordering::Relaxed),
            auth: self.auth_errors.load(Ordering::Relaxed),
            validation: self.validation_errors.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ErrorCounts {
    pub transient: u64,
    pub permanent: u64,
    pub rate_limit: u64,
    pub network: u64,
    pub auth: u64,
    pub validation: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU32;

    #[tokio::test]
    async fn test_circuit_breaker_basic_flow() {
        let cb = CircuitBreaker::new(3, Duration::from_millis(100), Duration::from_secs(1));

        // Should start closed
        assert_eq!(cb.get_state(), CircuitState::Closed);

        // Simulate failures
        for i in 0..3 {
            let result = cb.call(async { Err::<(), &str>("test error") }).await;
            assert!(result.is_err());

            if i < 2 {
                assert_eq!(cb.get_state(), CircuitState::Closed);
            }
        }

        // Should be open now
        assert_eq!(cb.get_state(), CircuitState::Open);
    }

    #[tokio::test]
    async fn test_resilient_executor() {
        let cb = Arc::new(CircuitBreaker::new(
            3,
            Duration::from_millis(10),
            Duration::from_secs(1),
        ));
        let retry_config = RetryConfig::new_fast();
        let executor = ResilientExecutor::new(cb, retry_config);

        let call_count = Arc::new(AtomicU32::new(0));
        let count_clone: Arc<AtomicU32> = Arc::clone(&call_count);

        let result = executor
            .execute(move || {
                let count = count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                Box::pin(async move {
                    if count < 3 {
                        Err("failing")
                    } else {
                        Ok("success")
                    }
                })
            })
            .await;

        assert!(result.is_ok());
        assert_eq!(call_count.load(Ordering::SeqCst), 3);
    }

    #[test]
    fn test_retry_config_presets() {
        let fast = RetryConfig::new_fast();
        assert_eq!(fast.max_attempts, 3);
        assert_eq!(fast.base_delay, Duration::from_millis(10));

        let standard = RetryConfig::new_standard();
        assert_eq!(standard.max_attempts, 5);
        assert_eq!(standard.base_delay, Duration::from_millis(100));

        let persistent = RetryConfig::new_persistent();
        assert_eq!(persistent.max_attempts, 10);
        assert!(persistent.jitter);
    }

    #[test]
    fn test_circuit_breaker_initial_metrics() {
        let cb = CircuitBreaker::new(5, Duration::from_secs(10), Duration::from_secs(30));
        let metrics = cb.get_metrics();
        assert_eq!(metrics.state, CircuitState::Closed);
        assert_eq!(metrics.failure_count, 0);
        assert_eq!(metrics.success_count, 0);
    }

    #[test]
    fn test_error_metrics_record_and_count() {
        let metrics = ErrorMetrics::new();
        metrics.record_error(&ErrorCategory::Transient);
        metrics.record_error(&ErrorCategory::Transient);
        metrics.record_error(&ErrorCategory::Network);
        metrics.record_error(&ErrorCategory::Auth);

        let counts = metrics.get_error_counts();
        assert_eq!(counts.transient, 2);
        assert_eq!(counts.network, 1);
        assert_eq!(counts.auth, 1);
        assert_eq!(counts.permanent, 0);
        assert_eq!(counts.rate_limit, 0);
        assert_eq!(counts.validation, 0);
    }

    #[test]
    fn test_data_engine_error_categorization() {
        assert_eq!(
            DataEngineError::WebSocketConnection("err".into()).categorize(),
            ErrorCategory::Network
        );
        assert_eq!(
            DataEngineError::MessageParsing("err".into()).categorize(),
            ErrorCategory::Permanent
        );
        assert_eq!(
            DataEngineError::Database("err".into()).categorize(),
            ErrorCategory::Transient
        );
        assert_eq!(
            DataEngineError::RateLimit("err".into()).categorize(),
            ErrorCategory::RateLimit
        );
        assert_eq!(
            DataEngineError::Authentication("err".into()).categorize(),
            ErrorCategory::Auth
        );
    }
}
