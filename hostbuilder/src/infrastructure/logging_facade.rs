use ultra_logger::{UltraLogger, LogLevel, LoggerConfig, TransportConfig, ConnectionConfig};
use std::sync::Arc;
use std::collections::HashMap;
use once_cell::sync::Lazy;

/// Global logging facade for the DataEngine
/// Provides unified access to UltraLogger across all components
pub struct DataEngineLogger {
    logger: UltraLogger,
    _service_name: String,
}

impl DataEngineLogger {
    pub fn new(service_name: &str) -> Self {
        // Create the appropriate logger configuration
        let config = Self::create_elasticsearch_config();

        Self {
            logger: UltraLogger::with_config(format!("DataEngine-{}", service_name), config),
            _service_name: service_name.to_string(),
        }
    }

    /// Create Elasticsearch configuration for DataEngine
    fn create_elasticsearch_config() -> LoggerConfig {
        // Check if we should use Elasticsearch (environment variable or default)
        let use_elasticsearch = std::env::var("USE_ELASTICSEARCH_LOGGING")
            .map(|v| v.to_lowercase() == "true")
            .unwrap_or(true); // Default to true for production

        if use_elasticsearch {
            // Get credentials from environment variables (required in production)
            let endpoint = std::env::var("ELASTIC_CLOUD_ENDPOINT").unwrap_or_default();
            let username = std::env::var("ELASTIC_CLOUD_USERNAME")
                .unwrap_or_else(|_| "elastic".to_string());
            let password = std::env::var("ELASTIC_CLOUD_PASSWORD").unwrap_or_default();
            
            // Only configure elasticsearch if endpoint is set
            if endpoint.is_empty() {
                return LoggerConfig::default();
            }

            let mut options = HashMap::new();
            options.insert("index".to_string(), "dataengine-logs".to_string());

            LoggerConfig {
                level: "info".to_string(),
                transport: TransportConfig {
                    transport_type: "elasticsearch".to_string(),
                    connection: ConnectionConfig {
                        host: endpoint,
                        port: 443,
                        username: Some(username),
                        password: Some(password),
                        options,
                    },
                },
            }
        } else {
            // Fallback to stdout for development
            LoggerConfig::default()
        }
    }

    /// Log debug information
    pub async fn debug(&self, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.logger.debug(message.to_string()).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Log informational messages
    pub async fn info(&self, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.logger.info(message.to_string()).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Log warning messages
    pub async fn warn(&self, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.logger.warn(message.to_string()).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Log error messages
    pub async fn error(&self, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.logger.error(message.to_string()).await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Log with custom level
    pub async fn log(&self, level: LogLevel, message: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        match level {
            LogLevel::Debug => self.logger.debug(message.to_string()).await,
            LogLevel::Info => self.logger.info(message.to_string()).await,
            LogLevel::Warn => self.logger.warn(message.to_string()).await,
            LogLevel::Error => self.logger.error(message.to_string()).await,
        }
        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
    }

    /// Log performance metrics
    pub async fn log_performance(
        &self,
        operation: &str,
        duration_nanos: u64,
        success: bool
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let message = format!("Performance: {} took {}ns, success: {}", operation, duration_nanos, success);
        self.info(&message).await
    }

    /// Log Redis operations
    pub async fn log_redis_operation(
        &self,
        operation: &str,
        key: &str,
        success: bool,
        error_message: Option<String>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let message = if success {
            format!("Redis {}: {}", operation, key)
        } else {
            format!("Redis {} failed: {} - error: {:?}", operation, key, error_message)
        };
        
        if success { 
            self.debug(&message).await 
        } else { 
            self.error(&message).await 
        }
    }

    /// Log database operations
    pub async fn log_database_operation(
        &self,
        operation: &str,
        table: &str,
        affected_rows: Option<usize>,
        success: bool,
        error_message: Option<String>
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let message = if success {
            format!("Database {}: {} (rows: {:?})", operation, table, affected_rows)
        } else {
            format!("Database {} failed: {} - error: {:?}", operation, table, error_message)
        };
        
        if success { 
            self.debug(&message).await 
        } else { 
            self.error(&message).await 
        }
    }

    /// Flush logs synchronously (no-op for ultra-logger)
    pub async fn flush(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Ultra-logger doesn't have a flush method, it's always real-time
        Ok(())
    }

    /// Shutdown the logger (not available for shared instances)
    pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Cannot shutdown shared logger instances
        // In a real implementation, this would coordinate with a manager
        Ok(())
    }
}

/// Global logger instances for different DataEngine components
pub static MAIN_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("Main"))
});

pub static KRAKEN_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("Kraken"))
});

pub static ORDERBOOK_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("OrderBook"))
});

pub static PERFORMANCE_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("Performance"))
});

pub static DATA_PIPELINE_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("DataPipeline"))
});

/// Massive (Polygon.io) WebSocket logger
pub static MASSIVE_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("Massive"))
});

/// Oanda pricing stream logger
pub static OANDA_LOGGER: Lazy<Arc<DataEngineLogger>> = Lazy::new(|| {
    Arc::new(DataEngineLogger::new("Oanda"))
});

/// Convenience macros for logging throughout the DataEngine
#[macro_export]
macro_rules! log_debug {
    ($logger:expr, $($arg:tt)*) => {
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let logger = $logger.clone();
                let message = format!($($arg)*);
                handle.spawn(async move {
                    let _ = logger.debug(&message).await;
                });
            }
        }
    };
}

#[macro_export]
macro_rules! log_info {
    ($logger:expr, $($arg:tt)*) => {
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let logger = $logger.clone();
                let message = format!($($arg)*);
                handle.spawn(async move {
                    let _ = logger.info(&message).await;
                });
            }
        }
    };
}

#[macro_export]
macro_rules! log_warn {
    ($logger:expr, $($arg:tt)*) => {
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let logger = $logger.clone();
                let message = format!($($arg)*);
                handle.spawn(async move {
                    let _ = logger.warn(&message).await;
                });
            }
        }
    };
}

#[macro_export]
macro_rules! log_error {
    ($logger:expr, $($arg:tt)*) => {
        {
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                let logger = $logger.clone();
                let message = format!($($arg)*);
                handle.spawn(async move {
                    let _ = logger.error(&message).await;
                });
            }
        }
    };
}

/// Create a standalone UltraLogger instance with Elasticsearch configuration
/// Used by KrakenLogger and other specialized loggers
pub fn create_ultra_logger(service_name: &str) -> UltraLogger {
    let config = DataEngineLogger::create_elasticsearch_config();
    UltraLogger::with_config(format!("DataEngine-{}", service_name), config)
}
