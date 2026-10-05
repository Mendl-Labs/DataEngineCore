//! Security and authentication system for DataEngine
//!
//! Provides secure API key management, rate limiting, request signing,
//! and security monitoring for production trading systems.

use crate::{infrastructure::logging_facade::MAIN_LOGGER, log_debug, log_error, log_warn};
use ahash::AHashMap;
use anyhow::{anyhow, Result};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine as _};
use dashmap::DashMap;
use fastrand;
use hmac::{Hmac, Mac};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Sha512};
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Security configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub api_key_rotation_interval: Duration,
    pub max_requests_per_minute: u32,
    pub max_requests_per_hour: u32,
    pub request_timeout: Duration,
    pub enable_request_signing: bool,
    pub hmac_algorithm: HmacAlgorithm,
    pub rate_limit_window: Duration,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            api_key_rotation_interval: Duration::from_secs(24 * 60 * 60), // 24 hours
            max_requests_per_minute: 100,
            max_requests_per_hour: 6000,
            request_timeout: Duration::from_secs(30),
            enable_request_signing: true,
            hmac_algorithm: HmacAlgorithm::Sha256,
            rate_limit_window: Duration::from_secs(60),
        }
    }
}

/// HMAC algorithm selection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HmacAlgorithm {
    Sha256,
    Sha512,
}

/// API credential with metadata
#[derive(Debug)]
pub struct ApiCredential {
    pub key: String,
    pub secret: String,
    pub created_at: Instant,
    pub last_used: Option<Instant>,
    pub usage_count: AtomicU64,
    pub is_active: bool,
    pub permissions: Vec<String>,
}

impl ApiCredential {
    pub fn new(key: String, secret: String, permissions: Vec<String>) -> Self {
        Self {
            key,
            secret,
            created_at: Instant::now(),
            last_used: None,
            usage_count: AtomicU64::new(0),
            is_active: true,
            permissions,
        }
    }

    pub fn record_usage(&mut self) {
        self.last_used = Some(Instant::now());
        self.usage_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn deactivate(&mut self) {
        self.is_active = false;
    }

    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.contains(&permission.to_string())
    }

    pub fn age(&self) -> Duration {
        self.created_at.elapsed()
    }
}

impl Clone for ApiCredential {
    fn clone(&self) -> Self {
        Self {
            key: self.key.clone(),
            secret: self.secret.clone(),
            created_at: self.created_at,
            last_used: self.last_used,
            usage_count: AtomicU64::new(self.usage_count.load(std::sync::atomic::Ordering::SeqCst)),
            is_active: self.is_active,
            permissions: self.permissions.clone(),
        }
    }
}

/// Rate limiting tracker
#[derive(Debug)]
pub struct RateLimit {
    pub requests: AtomicU64,
    pub window_start: Instant,
    pub max_requests: u32,
    pub window_duration: Duration,
}

impl RateLimit {
    pub fn new(max_requests: u32, window_duration: Duration) -> Self {
        Self {
            requests: AtomicU64::new(0),
            window_start: Instant::now(),
            max_requests,
            window_duration,
        }
    }

    pub fn check_and_increment(&mut self) -> bool {
        let now = Instant::now();

        // Reset window if expired
        if now.duration_since(self.window_start) > self.window_duration {
            self.window_start = now;
            self.requests.store(0, Ordering::Relaxed);
        }

        let current = self.requests.fetch_add(1, Ordering::Relaxed);
        (current + 1) <= self.max_requests as u64
    }

    pub fn remaining_requests(&self) -> u32 {
        let current = self.requests.load(Ordering::Relaxed);
        self.max_requests.saturating_sub(current as u32)
    }

    pub fn time_until_reset(&self) -> Duration {
        let elapsed = self.window_start.elapsed();
        if elapsed < self.window_duration {
            self.window_duration - elapsed
        } else {
            Duration::ZERO
        }
    }
}

impl Clone for RateLimit {
    fn clone(&self) -> Self {
        Self {
            requests: AtomicU64::new(self.requests.load(std::sync::atomic::Ordering::SeqCst)),
            window_start: self.window_start,
            max_requests: self.max_requests,
            window_duration: self.window_duration,
        }
    }
}

/// Request signature for API authentication
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestSignature {
    pub timestamp: u64,
    pub nonce: String,
    pub signature: String,
    pub algorithm: HmacAlgorithm,
}

impl RequestSignature {
    /// Create a new request signature
    pub fn create(
        method: &str,
        path: &str,
        body: &str,
        secret: &str,
        algorithm: HmacAlgorithm,
    ) -> Result<Self> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let nonce = fastrand::u64(..).to_string();

        // Create message to sign: timestamp + nonce + method + path + body
        let message = format!(
            "{}{}{}{}{}",
            timestamp,
            nonce,
            method.to_uppercase(),
            path,
            body
        );

        let signature = match algorithm {
            HmacAlgorithm::Sha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
                mac.update(message.as_bytes());
                BASE64.encode(mac.finalize().into_bytes())
            }
            HmacAlgorithm::Sha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes())
                    .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
                mac.update(message.as_bytes());
                BASE64.encode(mac.finalize().into_bytes())
            }
        };

        Ok(Self {
            timestamp,
            nonce,
            signature,
            algorithm,
        })
    }

    /// Verify a request signature
    pub fn verify(&self, method: &str, path: &str, body: &str, secret: &str) -> Result<bool> {
        // Check timestamp (prevent replay attacks)
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let age_ms = now.saturating_sub(self.timestamp);
        if age_ms > 300_000 {
            // 5 minutes
            return Ok(false);
        }

        // Recreate the message
        let message = format!(
            "{}{}{}{}{}",
            self.timestamp,
            self.nonce,
            method.to_uppercase(),
            path,
            body
        );

        let expected_signature = match self.algorithm {
            HmacAlgorithm::Sha256 => {
                let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
                mac.update(message.as_bytes());
                BASE64.encode(mac.finalize().into_bytes())
            }
            HmacAlgorithm::Sha512 => {
                let mut mac = Hmac::<Sha512>::new_from_slice(secret.as_bytes())
                    .map_err(|e| anyhow!("Invalid secret key: {}", e))?;
                mac.update(message.as_bytes());
                BASE64.encode(mac.finalize().into_bytes())
            }
        };

        Ok(self.signature == expected_signature)
    }
}

/// Security event for monitoring and alerting
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityEvent {
    pub event_type: SecurityEventType,
    pub source_ip: Option<String>,
    pub api_key: Option<String>,
    pub timestamp: u64,
    pub details: HashMap<String, serde_json::Value>,
    pub severity: SecuritySeverity,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SecurityEventType {
    InvalidApiKey,
    RateLimitExceeded,
    InvalidSignature,
    SuspiciousActivity,
    BruteForceAttempt,
    UnauthorizedAccess,
    ApiKeyRotated,
    PermissionDenied,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecuritySeverity {
    Low,
    Medium,
    High,
    Critical,
}

/// Comprehensive security manager
pub struct SecurityManager {
    config: SecurityConfig,
    credentials: RwLock<AHashMap<String, Arc<RwLock<ApiCredential>>>>,
    rate_limits: DashMap<String, Arc<RwLock<RateLimit>>>, // keyed by API key or IP
    security_events: RwLock<Vec<SecurityEvent>>,
    suspicious_ips: DashMap<String, AtomicU64>, // IP -> failed attempts
    nonce_cache: DashMap<String, Instant>,      // Prevent nonce reuse
}

impl SecurityManager {
    pub fn new(config: SecurityConfig) -> Self {
        Self {
            config,
            credentials: RwLock::new(AHashMap::new()),
            rate_limits: DashMap::new(),
            security_events: RwLock::new(Vec::new()),
            suspicious_ips: DashMap::new(),
            nonce_cache: DashMap::new(),
        }
    }

    /// Add API credential
    pub fn add_credential(&self, credential: ApiCredential) {
        let key = credential.key.clone();
        self.credentials
            .write()
            .insert(key.clone(), Arc::new(RwLock::new(credential)));

        // Initialize rate limit for this API key
        let rate_limit = RateLimit::new(
            self.config.max_requests_per_minute,
            self.config.rate_limit_window,
        );
        self.rate_limits
            .insert(key, Arc::new(RwLock::new(rate_limit)));
    }

    /// Authenticate API request
    pub async fn authenticate_request(
        &self,
        api_key: &str,
        signature: Option<&RequestSignature>,
        method: &str,
        path: &str,
        body: &str,
        source_ip: Option<&str>,
    ) -> Result<bool> {
        // Check if API key exists and is active
        let credential = {
            let credentials = self.credentials.read();
            credentials.get(api_key).cloned()
        };

        let credential = match credential {
            Some(c) => c,
            None => {
                self.record_security_event(SecurityEvent {
                    event_type: SecurityEventType::InvalidApiKey,
                    source_ip: source_ip.map(|s| s.to_string()),
                    api_key: Some(api_key.to_string()),
                    timestamp: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::ZERO)
                        .as_millis() as u64,
                    details: HashMap::new(),
                    severity: SecuritySeverity::Medium,
                });
                return Ok(false);
            }
        };

        {
            let cred = credential.read();
            if !cred.is_active {
                return Ok(false);
            }
        }

        // Check rate limit
        if !self.check_rate_limit(api_key) {
            self.record_security_event(SecurityEvent {
                event_type: SecurityEventType::RateLimitExceeded,
                source_ip: source_ip.map(|s| s.to_string()),
                api_key: Some(api_key.to_string()),
                timestamp: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or(Duration::ZERO)
                    .as_millis() as u64,
                details: HashMap::new(),
                severity: SecuritySeverity::High,
            });
            return Ok(false);
        }

        // Verify signature if required
        if self.config.enable_request_signing {
            if let Some(sig) = signature {
                // Check nonce reuse
                if self.nonce_cache.get(&sig.nonce).is_some() {
                    return Ok(false); // Nonce already used
                }

                let secret = {
                    let cred = credential.read();
                    cred.secret.clone()
                };

                if !sig.verify(method, path, body, &secret)? {
                    self.record_security_event(SecurityEvent {
                        event_type: SecurityEventType::InvalidSignature,
                        source_ip: source_ip.map(|s| s.to_string()),
                        api_key: Some(api_key.to_string()),
                        timestamp: SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .unwrap_or(Duration::ZERO)
                            .as_millis() as u64,
                        details: HashMap::new(),
                        severity: SecuritySeverity::High,
                    });
                    return Ok(false);
                }

                // Store nonce to prevent reuse
                self.nonce_cache.insert(sig.nonce.clone(), Instant::now());
            } else {
                // Signature required but not provided
                return Ok(false);
            }
        }

        // Update credential usage
        {
            let mut cred = credential.write();
            cred.record_usage();
        }

        Ok(true)
    }

    /// Check if request is within rate limit
    fn check_rate_limit(&self, api_key: &str) -> bool {
        if let Some(rate_limit) = self.rate_limits.get(api_key) {
            let mut limit = rate_limit.write();
            limit.check_and_increment()
        } else {
            // No rate limit set, allow request
            true
        }
    }

    /// Check permission for specific operation
    pub fn check_permission(&self, api_key: &str, permission: &str) -> bool {
        let credentials = self.credentials.read();
        if let Some(credential) = credentials.get(api_key) {
            let cred = credential.read();
            cred.has_permission(permission)
        } else {
            false
        }
    }

    /// Rotate API key
    pub fn rotate_api_key(&self, old_key: &str) -> Result<ApiCredential> {
        let mut credentials = self.credentials.write();

        // Get the old credential and extract needed info
        let (new_credential, old_cred_arc) = if let Some(old_cred_arc) = credentials.get(old_key) {
            let old_cred = old_cred_arc.read();

            // Generate new credentials
            let new_key = format!("api_{}", fastrand::u64(..));
            let new_secret = format!("sec_{}", fastrand::u128(..));

            let new_credential =
                ApiCredential::new(new_key.clone(), new_secret, old_cred.permissions.clone());

            (new_credential, old_cred_arc.clone())
        } else {
            return Err(anyhow!("API key not found: {}", old_key));
        };

        // Add new credential
        let new_key = new_credential.key.clone();
        credentials.insert(
            new_key.clone(),
            Arc::new(RwLock::new(new_credential.clone())),
        );

        // Initialize rate limit for new key
        let rate_limit = RateLimit::new(
            self.config.max_requests_per_minute,
            self.config.rate_limit_window,
        );
        self.rate_limits
            .insert(new_key, Arc::new(RwLock::new(rate_limit)));

        // Now it's safe to deactivate old credential
        {
            let mut old_cred_mut = old_cred_arc.write();
            old_cred_mut.deactivate();
        }

        drop(credentials); // Release the write lock

        self.record_security_event(SecurityEvent {
            event_type: SecurityEventType::ApiKeyRotated,
            source_ip: None,
            api_key: Some(old_key.to_string()),
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::ZERO)
                .as_millis() as u64,
            details: {
                let mut details = HashMap::new();
                details.insert("new_key".to_string(), serde_json::json!(new_credential.key));
                details
            },
            severity: SecuritySeverity::Low,
        });

        Ok(new_credential)
    }

    /// Record security event
    fn record_security_event(&self, event: SecurityEvent) {
        // Log the event
        match event.severity {
            SecuritySeverity::Critical => log_error!(MAIN_LOGGER, "Security event: {:?}", event),
            SecuritySeverity::High => log_warn!(MAIN_LOGGER, "Security event: {:?}", event),
            SecuritySeverity::Medium => log_warn!(MAIN_LOGGER, "Security event: {:?}", event),
            SecuritySeverity::Low => log_debug!(MAIN_LOGGER, "Security event: {:?}", event),
        }

        // Store in event log (with size limit)
        let mut events = self.security_events.write();
        events.push(event);

        // Keep only last 10,000 events
        if events.len() > 10_000 {
            events.drain(..1_000); // Remove oldest 1,000
        }
    }

    /// Get recent security events
    pub fn get_security_events(&self, limit: Option<usize>) -> Vec<SecurityEvent> {
        let events = self.security_events.read();
        let start_index = if let Some(limit) = limit {
            events.len().saturating_sub(limit)
        } else {
            0
        };
        events[start_index..].to_vec()
    }

    /// Get rate limit status for API key
    pub fn get_rate_limit_status(&self, api_key: &str) -> Option<(u32, Duration)> {
        self.rate_limits.get(api_key).map(|rate_limit| {
            let limit = rate_limit.read();
            (limit.remaining_requests(), limit.time_until_reset())
        })
    }

    /// Clean up old nonces and events
    pub fn cleanup(&self) {
        let cutoff = Instant::now() - Duration::from_secs(300); // 5 minutes

        // Clean old nonces
        let old_nonces: Vec<String> = self
            .nonce_cache
            .iter()
            .filter_map(|entry| {
                if *entry.value() < cutoff {
                    Some(entry.key().clone())
                } else {
                    None
                }
            })
            .collect();

        for nonce in old_nonces {
            self.nonce_cache.remove(&nonce);
        }

        log_debug!(MAIN_LOGGER, "Security cleanup completed");
    }

    /// Get security statistics
    pub fn get_security_stats(&self) -> SecurityStats {
        let events = self.security_events.read();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or(Duration::ZERO)
            .as_millis() as u64;
        let hour_ago = now - (60 * 60 * 1000); // 1 hour ago

        let recent_events: Vec<&SecurityEvent> =
            events.iter().filter(|e| e.timestamp > hour_ago).collect();

        let mut stats = SecurityStats {
            total_events: events.len(),
            recent_events: recent_events.len(),
            active_credentials: 0,
            suspicious_ips: self.suspicious_ips.len(),
            events_by_type: HashMap::new(),
            events_by_severity: HashMap::new(),
        };

        // Count active credentials
        let credentials = self.credentials.read();
        stats.active_credentials = credentials
            .values()
            .filter(|cred| cred.read().is_active)
            .count();

        // Count events by type and severity
        for event in &recent_events {
            *stats
                .events_by_type
                .entry(format!("{:?}", event.event_type))
                .or_insert(0) += 1;
            *stats
                .events_by_severity
                .entry(format!("{:?}", event.severity))
                .or_insert(0) += 1;
        }

        stats
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityStats {
    pub total_events: usize,
    pub recent_events: usize,
    pub active_credentials: usize,
    pub suspicious_ips: usize,
    pub events_by_type: HashMap<String, u32>,
    pub events_by_severity: HashMap<String, u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_credential() {
        let mut cred = ApiCredential::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            vec!["read".to_string(), "write".to_string()],
        );

        assert!(cred.has_permission("read"));
        assert!(cred.has_permission("write"));
        assert!(!cred.has_permission("admin"));

        cred.record_usage();
        assert!(cred.last_used.is_some());
        assert_eq!(cred.usage_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_rate_limit() {
        let mut rate_limit = RateLimit::new(5, Duration::from_secs(60));

        // Should allow 5 requests
        for _ in 0..5 {
            assert!(rate_limit.check_and_increment());
        }

        // 6th request should be denied
        assert!(!rate_limit.check_and_increment());

        assert_eq!(rate_limit.remaining_requests(), 0);
    }

    #[tokio::test]
    async fn test_request_signature() {
        let signature = RequestSignature::create(
            "POST",
            "/api/v1/orders",
            r#"{"symbol":"BTC/USD","side":"buy","amount":1.0}"#,
            "test_secret",
            HmacAlgorithm::Sha256,
        )
        .unwrap();

        let is_valid = signature
            .verify(
                "POST",
                "/api/v1/orders",
                r#"{"symbol":"BTC/USD","side":"buy","amount":1.0}"#,
                "test_secret",
            )
            .unwrap();

        assert!(is_valid);

        // Test with wrong secret
        let is_invalid = signature
            .verify(
                "POST",
                "/api/v1/orders",
                r#"{"symbol":"BTC/USD","side":"buy","amount":1.0}"#,
                "wrong_secret",
            )
            .unwrap();

        assert!(!is_invalid);
    }

    #[tokio::test]
    async fn test_security_manager() {
        let config = SecurityConfig::default();
        let manager = SecurityManager::new(config);

        let credential = ApiCredential::new(
            "test_key".to_string(),
            "test_secret".to_string(),
            vec!["read".to_string()],
        );

        manager.add_credential(credential);

        // Test authentication without signature (signature disabled)
        let manager_no_sig = SecurityManager::new(SecurityConfig {
            enable_request_signing: false,
            ..Default::default()
        });

        let credential2 = ApiCredential::new(
            "test_key2".to_string(),
            "test_secret2".to_string(),
            vec!["read".to_string()],
        );
        manager_no_sig.add_credential(credential2);

        let is_authenticated = manager_no_sig
            .authenticate_request(
                "test_key2",
                None,
                "GET",
                "/api/v1/status",
                "",
                Some("127.0.0.1"),
            )
            .await
            .unwrap();

        assert!(is_authenticated);

        // Test permission check
        assert!(manager_no_sig.check_permission("test_key2", "read"));
        assert!(!manager_no_sig.check_permission("test_key2", "write"));
    }
}
