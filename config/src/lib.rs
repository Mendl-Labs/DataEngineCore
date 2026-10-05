//! Centralized configuration management for DataEngine
//!
//! Simple, focused configuration handling for the DataEngine with
//! environment variable support and basic validation.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Environment types for configuration
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub enum Environment {
    #[default]
    Development,
    Testing,
    Production,
}

/// Core DataEngine configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub environment: Environment,
    pub message_broker: MessageBrokerConfig,
    pub database: DatabaseConfig,
    pub exchanges: Vec<ExchangeConfig>,
    pub performance: PerformanceConfig,
    pub security: SecurityConfig,
    pub topics: Vec<String>,
    pub topic_routing: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketConfig {
    pub endpoint: String,
    pub channel: String,
    /// Bybit only: "spot" or "linear" (perpetual). Determines which WebSocket
    /// endpoint variant is used when the handler supports multiple market types.
    #[serde(default)]
    pub market_type: Option<String>,
    /// Deribit only: connect to testnet (`wss://test.deribit.com`) when true.
    #[serde(default)]
    pub testnet: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExchangeConfig {
    pub name: String,
    pub enabled: bool,
    pub symbols: Vec<String>,
    /// "crypto" (default) or "stocks". Controls WS subscription prefix and feed URL.
    #[serde(default)]
    pub asset_class: Option<String>,
    pub websocket_url: String,
    pub api_url: String,
    pub max_depth: u32,
    #[serde(default)]
    pub websocket_token: String,
    pub websockets: Vec<WebSocketConfig>,
    pub depth: u32,
    #[serde(default)]
    pub require_auth: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageBrokerConfig {
    pub address: String,
    pub port: u16,
    pub buffer_size: usize,
    pub batch_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    pub postgres_url: String,
    pub max_connections: u32,
    pub connection_timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    pub worker_threads: u32,
    pub memory_limit_mb: u64,
    pub metrics_enabled: bool,
    pub cpu_affinity_enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub api_key_required: bool,
    pub request_signing_required: bool,
    pub rate_limiting_enabled: bool,
    pub max_requests_per_minute: u32,
}

impl Default for Config {
    fn default() -> Self {
        let mut topic_routing = HashMap::new();
        topic_routing.insert(
            "trades".to_string(),
            "market.data.massive.trades".to_string(),
        );

        Self {
            environment: Environment::Development,
            message_broker: MessageBrokerConfig {
                address: "localhost".to_string(),
                port: 8080,
                buffer_size: 8192,
                batch_size: 1000,
            },
            database: DatabaseConfig {
                postgres_url: "postgresql://localhost:5432/dataengine".to_string(),
                max_connections: 20,
                connection_timeout_secs: 30,
            },
            exchanges: vec![
                // Massive (Polygon.io) — single real-time feed for all crypto pairs
                ExchangeConfig {
                    name: "massive".to_string(),
                    enabled: true,
                    symbols: vec!["BTC-USD".to_string(), "ETH-USD".to_string()],
                    asset_class: None,
                    websocket_url: "wss://socket.massive.com/crypto".to_string(),
                    api_url: "https://api.polygon.io".to_string(),
                    max_depth: 0,
                    websocket_token: "".to_string(),
                    websockets: vec![WebSocketConfig {
                        endpoint: "wss://socket.massive.com/crypto".to_string(),
                        channel: "trade".to_string(),
                        market_type: None,
                        testnet: None,
                    }],
                    depth: 0,
                    require_auth: false,
                },
            ],
            performance: PerformanceConfig {
                worker_threads: num_cpus::get() as u32,
                memory_limit_mb: 1024,
                metrics_enabled: true,
                cpu_affinity_enabled: false,
            },
            security: SecurityConfig {
                api_key_required: false,
                request_signing_required: false,
                rate_limiting_enabled: true,
                max_requests_per_minute: 100,
            },
            topics: vec!["market.data.massive".to_string()],
            topic_routing,
        }
    }
}

impl Config {
    /// Create a new config from file path (alias for from_file)
    pub fn new(path: &str) -> Result<Self> {
        Self::from_file(path)
    }

    /// Load configuration from YAML file with environment overrides
    pub fn from_file(path: &str) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let mut config: Self = serde_yaml::from_str(&content)?;
        config.apply_env_overrides();
        config.validate()?;
        Ok(config)
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) {
        if let Ok(url) = std::env::var("DATABASE_URL") {
            self.database.postgres_url = url;
        }
        // REDIS_URL environment variable ignored - Redis removed from system
        if let Ok(addr) = std::env::var("MESSAGE_BROKER_HOST")
            .or_else(|_| std::env::var("MESSAGE_BROKER_ADDRESS"))
        {
            self.message_broker.address = addr;
        }
        if let Ok(port) = std::env::var("MESSAGE_BROKER_PORT") {
            if let Ok(port_num) = port.parse() {
                self.message_broker.port = port_num;
            }
        }
        if let Ok(threads) = std::env::var("WORKER_THREADS") {
            if let Ok(thread_count) = threads.parse() {
                self.performance.worker_threads = thread_count;
            }
        }
    }

    /// Basic validation
    fn validate(&self) -> Result<()> {
        if self.exchanges.is_empty() {
            return Err(anyhow::anyhow!("At least one exchange must be configured"));
        }
        for exchange in &self.exchanges {
            if exchange.name.is_empty() {
                return Err(anyhow::anyhow!("Exchange name cannot be empty"));
            }
            if exchange.symbols.is_empty() {
                return Err(anyhow::anyhow!(
                    "Exchange '{}' must have symbols",
                    exchange.name
                ));
            }
        }
        if self.message_broker.address.is_empty() {
            return Err(anyhow::anyhow!("Message broker address cannot be empty"));
        }
        if self.message_broker.port == 0 {
            return Err(anyhow::anyhow!(
                "Message broker port must be greater than 0"
            ));
        }
        Ok(())
    }

    /// Get enabled exchange by name
    pub fn get_exchange(&self, name: &str) -> Option<&ExchangeConfig> {
        self.exchanges.iter().find(|e| e.name == name && e.enabled)
    }

    /// Save configuration to YAML file
    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let yaml = serde_yaml::to_string(self)?;
        std::fs::write(path, yaml)?;
        Ok(())
    }

    /// Get configuration as JSON for API responses (with sensitive data redacted)
    pub fn to_json(&self) -> Result<String> {
        let mut sanitized = self.clone();
        sanitized.database.postgres_url = "***REDACTED***".to_string();
        // redis_url removed - no longer part of DatabaseConfig

        serde_json::to_string_pretty(&sanitized)
            .map_err(|e| anyhow::anyhow!("Failed to serialize config to JSON: {}", e))
    }
}

/// Simple configuration manager for API compatibility
pub struct ConfigManager {
    config: std::sync::Arc<std::sync::RwLock<Config>>,
}

impl ConfigManager {
    pub fn new(config: Config) -> Self {
        Self {
            config: std::sync::Arc::new(std::sync::RwLock::new(config)),
        }
    }

    pub fn get_config(&self) -> std::sync::Arc<std::sync::RwLock<Config>> {
        std::sync::Arc::clone(&self.config)
    }

    /// Update configuration with a closure
    pub fn update_config<F>(&self, updater: F) -> Result<()>
    where
        F: FnOnce(&mut Config) -> Result<()>,
    {
        let mut config = self
            .config
            .write()
            .map_err(|e| anyhow::anyhow!("Failed to acquire config lock: {}", e))?;

        updater(&mut config)?;
        config.validate()?;
        Ok(())
    }

    /// Reload configuration from file if changed (placeholder implementation)
    pub fn reload_if_changed(&self) -> Result<bool> {
        // In a real implementation, this would check file modification time
        // For now, just return false indicating no reload occurred
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert!(config.validate().is_ok());
        assert!(!config.exchanges.is_empty());
        assert_eq!(config.environment, Environment::Development);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let yaml = serde_yaml::to_string(&config).unwrap();
        let deserialized: Config = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(config.exchanges.len(), deserialized.exchanges.len());
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();

        // Valid config should pass
        assert!(config.validate().is_ok());

        // Invalid config should fail
        config.exchanges.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_exchange_lookup() {
        let config = Config::default();
        assert!(config.get_exchange("massive").is_some());
        assert!(config.get_exchange("nonexistent").is_none());
    }

    #[test]
    fn test_empty_exchange_name_fails() {
        let mut config = Config::default();
        config.exchanges[0].name = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_symbols_fails() {
        let mut config = Config::default();
        config.exchanges[0].symbols.clear();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_empty_broker_address_fails() {
        let mut config = Config::default();
        config.message_broker.address = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_zero_broker_port_fails() {
        let mut config = Config::default();
        config.message_broker.port = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_multiple_exchange_lookup() {
        let config = Config::default();
        // Default config uses the single Massive feed; legacy multi-exchange entries removed
        assert!(config.get_exchange("massive").is_some());
        assert!(config.get_exchange("coinbase").is_none());
        assert!(config.get_exchange("binance_us").is_none());
    }

    #[test]
    fn test_default_topic_routing() {
        let config = Config::default();
        assert!(config.topic_routing.contains_key("trades"));
        // level3 / balances topics removed after Massive migration
        assert!(!config.topic_routing.contains_key("level3"));
    }

    #[test]
    fn test_environment_default() {
        let config = Config::default();
        assert_eq!(config.environment, Environment::Development);
    }

    #[test]
    fn test_config_manager_reload() {
        let config = Config::default();
        let manager = ConfigManager::new(config);
        assert!(!manager.reload_if_changed().unwrap());
    }
}
