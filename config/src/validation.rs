//! Configuration validation and management system
//! Provides type-safe configuration with comprehensive validation

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::collections::HashMap;
use validator::{Validate, ValidationError};

/// Main configuration structure with comprehensive validation
#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct DataEngineConfig {
    #[validate(nested)]
    pub database: DatabaseConfig,
    
    #[validate(nested)]
    pub redis: RedisConfig,
    
    #[validate(nested)]
    pub cache: CacheConfig,
    
    #[validate(nested)]
    pub performance: PerformanceConfig,
    
    #[validate(nested)]
    pub network: NetworkConfig,
    
    #[validate(nested)]
    pub security: SecurityConfig,
    
    #[validate(nested)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct DatabaseConfig {
    #[validate(length(min = 1, message = "Database host cannot be empty"))]
    pub host: String,
    
    #[validate(range(min = 1, max = 65535, message = "Invalid port number"))]
    pub port: u16,
    
    #[validate(length(min = 1, message = "Database name cannot be empty"))]
    pub database: String,
    
    #[validate(length(min = 1, message = "Username cannot be empty"))]
    pub username: String,
    
    pub password: String,
    
    #[validate(range(min = 1, max = 1000, message = "Max connections must be between 1 and 1000"))]
    pub max_connections: u32,
    
    #[validate(range(min = 1, max = 300, message = "Connection timeout must be between 1 and 300 seconds"))]
    pub connection_timeout_seconds: u64,
    
    pub ssl_mode: SslMode,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SslMode {
    Disable,
    Prefer,
    Require,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct RedisConfig {
    #[validate(length(min = 1, message = "Redis host cannot be empty"))]
    pub host: String,
    
    #[validate(range(min = 1, max = 65535, message = "Invalid port number"))]
    pub port: u16,
    
    pub password: Option<String>,
    
    #[validate(range(min = 1, max = 1000, message = "Max connections must be between 1 and 1000"))]
    pub max_connections: u32,
    
    #[validate(range(min = 0, max = 15, message = "Redis database index must be between 0 and 15"))]
    pub database: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct CacheConfig {
    #[validate(range(min = 100, max = 1000000, message = "Cache capacity must be between 100 and 1,000,000"))]
    pub capacity: usize,
    
    #[validate(range(min = 10, max = 3600, message = "TTL must be between 10 and 3600 seconds"))]
    pub ttl_seconds: u64,
    
    #[validate(range(min = 10, max = 300, message = "Cleanup interval must be between 10 and 300 seconds"))]
    pub cleanup_interval_seconds: u64,
    
    pub enable_compression: bool,
    
    #[validate(range(min = 1, max = 10, message = "Cache levels must be between 1 and 10"))]
    pub cache_levels: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct PerformanceConfig {
    #[validate(range(min = 0, max = 256, message = "Worker threads must be between 0 and 256 (0 = auto)"))]
    pub worker_threads: usize,
    
    #[validate(range(min = 1, max = 1024, message = "Max blocking threads must be between 1 and 1024"))]
    pub max_blocking_threads: usize,
    
    pub enable_cpu_affinity: bool,
    pub enable_memory_pools: bool,
    pub enable_simd: bool,
    
    #[validate(custom = "validate_memory_limit")]
    pub memory_limit_mb: Option<usize>,
}

fn validate_memory_limit(memory_limit: &Option<usize>) -> Result<(), ValidationError> {
    if let Some(limit) = memory_limit {
        if *limit < 64 || *limit > 32768 {
            return Err(ValidationError::new("Memory limit must be between 64MB and 32GB"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct NetworkConfig {
    #[validate(range(min = 1, max = 65535, message = "Invalid port number"))]
    pub listen_port: u16,
    
    #[validate(length(min = 1, message = "Listen address cannot be empty"))]
    pub listen_address: String,
    
    #[validate(range(min = 1, max = 300, message = "Connection timeout must be between 1 and 300 seconds"))]
    pub connection_timeout_seconds: u64,
    
    #[validate(range(min = 1, max = 10000, message = "Max concurrent connections must be between 1 and 10,000"))]
    pub max_concurrent_connections: usize,
    
    pub enable_tls: bool,
    
    #[validate(custom = "validate_tls_config")]
    pub tls_cert_path: Option<String>,
    
    #[validate(custom = "validate_tls_config")]
    pub tls_key_path: Option<String>,
}

fn validate_tls_config(config: &NetworkConfig) -> Result<(), ValidationError> {
    if config.enable_tls {
        if config.tls_cert_path.is_none() || config.tls_key_path.is_none() {
            return Err(ValidationError::new("TLS certificate and key paths required when TLS is enabled"));
        }
    }
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct SecurityConfig {
    pub enable_authentication: bool,
    
    #[validate(length(min = 32, message = "JWT secret must be at least 32 characters"))]
    pub jwt_secret: Option<String>,
    
    #[validate(range(min = 300, max = 86400, message = "Token expiry must be between 5 minutes and 24 hours"))]
    pub token_expiry_seconds: u64,
    
    pub enable_rate_limiting: bool,
    
    #[validate(range(min = 1, max = 10000, message = "Rate limit must be between 1 and 10,000 requests per minute"))]
    pub rate_limit_per_minute: u32,
    
    pub allowed_origins: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Validate)]
pub struct LoggingConfig {
    #[validate(custom = "validate_log_level")]
    pub level: String,
    
    pub enable_json_format: bool,
    pub enable_console_output: bool,
    pub enable_file_output: bool,
    
    pub log_file_path: Option<String>,
    
    #[validate(range(min = 1, max = 1000, message = "Max log file size must be between 1MB and 1GB"))]
    pub max_file_size_mb: Option<usize>,
    
    #[validate(range(min = 1, max = 365, message = "Log retention must be between 1 and 365 days"))]
    pub retention_days: Option<u32>,
}

fn validate_log_level(level: &str) -> Result<(), ValidationError> {
    match level.to_lowercase().as_str() {
        "error" | "warn" | "info" | "debug" | "trace" => Ok(()),
        _ => Err(ValidationError::new("Log level must be one of: error, warn, info, debug, trace")),
    }
}

impl DataEngineConfig {
    /// Load configuration from file with validation
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: DataEngineConfig = toml::from_str(&content)?;
        
        // Validate configuration
        config.validate()
            .map_err(|e| anyhow!("Configuration validation failed: {}", e))?;
        
        // Additional custom validations
        Self::validate_business_rules(&config)?;
        
        Ok(config)
    }
    
    /// Load configuration from environment variables
    /// Falls back to defaults if environment variables are not set
    pub fn load_from_env() -> Result<Self> {
        let mut config = Self::default();
        config.apply_env_overrides()?;
        config.validate()
            .map_err(|e| anyhow!("Environment configuration validation failed: {}", e))?;
        Ok(config)
    }
    
    /// Merge configurations (file + env + defaults)
    pub fn load_merged() -> Result<Self> {
        let mut config = Self::default();
        
        // 1. Load from file if CONFIG_PATH is set
        if let Ok(config_path) = std::env::var("CONFIG_PATH") {
            config = Self::load_from_file(&config_path)?;
        }
        
        // 2. Override with environment variables
        config.apply_env_overrides()?;
        
        // 3. Final validation
        config.validate()
            .map_err(|e| anyhow!("Final configuration validation failed: {}", e))?;
        
        Ok(config)
    }
    
    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) -> Result<()> {
        // Database overrides
        if let Ok(host) = std::env::var("DATABASE_HOST") {
            self.database.host = host;
        }
        if let Ok(port) = std::env::var("DATABASE_PORT") {
            self.database.port = port.parse()?;
        }
        
        // Redis overrides
        if let Ok(redis_url) = std::env::var("REDIS_URL") {
            // Parse Redis URL and update config
            // Implementation depends on URL format
        }
        
        // Add more environment overrides as needed
        Ok(())
    }
    
    /// Business rule validations
    fn validate_business_rules(config: &DataEngineConfig) -> Result<()> {
        // Ensure cache capacity is reasonable for available memory
        if let Some(memory_limit) = config.performance.memory_limit_mb {
            let cache_memory_estimate = config.cache.capacity * 1024; // Rough estimate
            if cache_memory_estimate > memory_limit * 1024 * 1024 / 2 {
                return Err(anyhow!("Cache capacity too large for memory limit"));
            }
        }
        
        // Ensure database and Redis aren't on the same port if on same host
        if config.database.host == config.redis.host && 
           config.database.port == config.redis.port {
            return Err(anyhow!("Database and Redis cannot use the same host:port"));
        }
        
        // Validate TLS configuration
        if config.network.enable_tls {
            if let (Some(cert), Some(key)) = (&config.network.tls_cert_path, &config.network.tls_key_path) {
                if !Path::new(cert).exists() {
                    return Err(anyhow!("TLS certificate file not found: {}", cert));
                }
                if !Path::new(key).exists() {
                    return Err(anyhow!("TLS key file not found: {}", key));
                }
            }
        }
        
        Ok(())
    }
    
    /// Get configuration summary for logging
    pub fn summary(&self) -> HashMap<String, String> {
        let mut summary = HashMap::new();
        
        summary.insert("database_host".to_string(), self.database.host.clone());
        summary.insert("database_port".to_string(), self.database.port.to_string());
        summary.insert("redis_host".to_string(), self.redis.host.clone());
        summary.insert("redis_port".to_string(), self.redis.port.to_string());
        summary.insert("cache_capacity".to_string(), self.cache.capacity.to_string());
        summary.insert("worker_threads".to_string(), self.performance.worker_threads.to_string());
        summary.insert("listen_port".to_string(), self.network.listen_port.to_string());
        summary.insert("log_level".to_string(), self.logging.level.clone());
        
        summary
    }
}

impl Default for DataEngineConfig {
    fn default() -> Self {
        Self {
            database: DatabaseConfig {
                host: "localhost".to_string(),
                port: 5432,
                database: "trading".to_string(),
                username: "user".to_string(),
                password: "password".to_string(),
                max_connections: 10,
                connection_timeout_seconds: 30,
                ssl_mode: SslMode::Prefer,
            },
            redis: RedisConfig {
                host: "localhost".to_string(),
                port: 6379,
                password: None,
                max_connections: 10,
                database: 0,
            },
            cache: CacheConfig {
                capacity: 10000,
                ttl_seconds: 300,
                cleanup_interval_seconds: 60,
                enable_compression: true,
                cache_levels: 3,
            },
            performance: PerformanceConfig {
                worker_threads: 0, // Auto-detect
                max_blocking_threads: 512,
                enable_cpu_affinity: true,
                enable_memory_pools: true,
                enable_simd: true,
                memory_limit_mb: None,
            },
            network: NetworkConfig {
                listen_port: 8080,
                listen_address: "0.0.0.0".to_string(),
                connection_timeout_seconds: 30,
                max_concurrent_connections: 1000,
                enable_tls: false,
                tls_cert_path: None,
                tls_key_path: None,
            },
            security: SecurityConfig {
                enable_authentication: true,
                jwt_secret: None,
                token_expiry_seconds: 3600,
                enable_rate_limiting: true,
                rate_limit_per_minute: 1000,
                allowed_origins: vec!["https://localhost:3000".to_string()],
            },
            logging: LoggingConfig {
                level: "info".to_string(),
                enable_json_format: true,
                enable_console_output: true,
                enable_file_output: false,
                log_file_path: None,
                max_file_size_mb: Some(100),
                retention_days: Some(30),
            },
        }
    }
}