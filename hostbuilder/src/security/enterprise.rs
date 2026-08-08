//! Enterprise security manager with authentication, authorization, and monitoring
//! Comprehensive security implementation for production trading systems

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use chrono::{DateTime, Utc, Duration};
use jsonwebtoken::{Header, Algorithm, Validation, EncodingKey, DecodingKey};
use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::{log_warn, log_error};

/// Enhanced security context for all operations
#[derive(Debug, Clone)]
pub struct EnterpriseSecurityContext {
    pub user_id: Option<String>,
    pub session_id: String,
    pub permissions: Vec<SecurityPermission>,
    pub source_ip: String,
    pub user_agent: Option<String>,
    pub authenticated_at: DateTime<Utc>,
    pub security_level: SecurityLevel,
    pub mfa_verified: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SecurityPermission {
    ReadMarketData,
    WriteMarketData,
    ReadTradeData,
    WriteTradeData,
    SystemAdmin,
    SecurityAudit,
    ConfigManagement,
    UserManagement,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SecurityLevel {
    Public,      // No authentication required
    Authenticated, // Basic authentication required
    Elevated,    // MFA required
    Admin,       // Admin privileges required
}

/// Enhanced JWT token claims with security metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct EnhancedTokenClaims {
    pub sub: String,      // Subject (user ID)
    pub iat: i64,         // Issued at
    pub exp: i64,         // Expiration
    pub permissions: Vec<SecurityPermission>,
    pub session_id: String,
    pub security_level: String,
    pub mfa_verified: bool,
    pub issuer: String,
}

/// Enterprise security manager with comprehensive protection
pub struct EnterpriseSecurityManager {
    jwt_secret: String,
    rate_limits: Arc<RwLock<HashMap<String, RateLimitEntry>>>,
    blocked_ips: Arc<RwLock<HashMap<String, BlockedEntry>>>,
    active_sessions: Arc<RwLock<HashMap<String, SessionEntry>>>,
    security_config: SecurityConfig,
}

#[derive(Debug, Clone)]
struct RateLimitEntry {
    count: u32,
    window_start: DateTime<Utc>,
    last_request: DateTime<Utc>,
    violations: u32,
}

#[derive(Debug, Clone)]
struct BlockedEntry {
    blocked_at: DateTime<Utc>,
    reason: String,
    expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone)]
struct SessionEntry {
    user_id: String,
    created_at: DateTime<Utc>,
    last_activity: DateTime<Utc>,
    ip_address: String,
    user_agent: Option<String>,
    mfa_verified: bool,
}

#[derive(Debug, Clone)]
pub struct SecurityConfig {
    pub rate_limit_per_minute: u32,
    pub token_expiry_hours: i64,
    pub session_timeout_minutes: i64,
    pub max_failed_attempts: u32,
    pub block_duration_minutes: i64,
    pub require_mfa_for_admin: bool,
    pub password_min_length: usize,
    pub enable_audit_logging: bool,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_minute: 100,
            token_expiry_hours: 8,
            session_timeout_minutes: 60,
            max_failed_attempts: 5,
            block_duration_minutes: 30,
            require_mfa_for_admin: true,
            password_min_length: 12,
            enable_audit_logging: true,
        }
    }
}

impl EnterpriseSecurityManager {
    pub fn new(jwt_secret: String, config: SecurityConfig) -> Self {
        Self {
            jwt_secret,
            rate_limits: Arc::new(RwLock::new(HashMap::new())),
            blocked_ips: Arc::new(RwLock::new(HashMap::new())),
            active_sessions: Arc::new(RwLock::new(HashMap::new())),
            security_config: config,
        }
    }
    
    /// Enterprise-grade authentication with comprehensive security checks
    pub async fn authenticate_enterprise(&self, 
        username: &str, 
        password: &str, 
        source_ip: &str,
        user_agent: Option<&str>,
        mfa_token: Option<&str>
    ) -> Result<String> {
        // 1. Input validation
        self.validate_auth_input(username, password)?;
        
        // 2. Rate limiting and IP blocking checks
        self.check_comprehensive_limits(source_ip).await?;
        
        // 3. Verify credentials with enhanced security
        let (user_id, security_level) = self.verify_enhanced_credentials(username, password).await?;
        
        // 4. MFA verification if required
        let mfa_verified = if self.requires_mfa(&security_level) {
            self.verify_mfa(&user_id, mfa_token).await?
        } else {
            false
        };
        
        // 5. Get comprehensive user permissions
        let permissions = self.get_enhanced_permissions(&user_id).await?;
        
        // 6. Create secure session
        let session_id = self.create_secure_session(&user_id, source_ip, user_agent).await?;
        
        // 7. Generate enhanced JWT token
        let token = self.create_enhanced_jwt_token(
            user_id.clone(),
            permissions,
            session_id.clone(),
            security_level,
            mfa_verified
        ).await?;
        
        // 8. Audit logging
        self.log_security_event("authentication_success", &user_id, source_ip, Some(&format!(
            "MFA: {}, Security Level: {:?}", mfa_verified, security_level
        ))).await;
        
        Ok(token)
    }
    
    /// Enhanced token validation with session management
    pub async fn validate_enhanced_token(&self, token: &str, source_ip: &str) -> Result<EnterpriseSecurityContext> {
        // Decode and validate token structure
        let claims = self.decode_enhanced_jwt_token(token)?;
        
        // Check token expiration
        let now = Utc::now().timestamp();
        if claims.exp < now {
            return Err(anyhow!("Token expired"));
        }
        
        // Validate active session
        self.validate_active_session(&claims.session_id, source_ip).await?;
        
        // Update session activity
        self.update_session_activity(&claims.session_id).await?;
        
        // Parse security level
        let security_level = match claims.security_level.as_str() {
            "Public" => SecurityLevel::Public,
            "Authenticated" => SecurityLevel::Authenticated,
            "Elevated" => SecurityLevel::Elevated,
            "Admin" => SecurityLevel::Admin,
            _ => return Err(anyhow!("Invalid security level")),
        };
        
        Ok(EnterpriseSecurityContext {
            user_id: Some(claims.sub),
            session_id: claims.session_id,
            permissions: claims.permissions,
            source_ip: source_ip.to_string(),
            user_agent: None,
            authenticated_at: DateTime::from_timestamp(claims.iat, 0).unwrap_or_else(Utc::now),
            security_level,
            mfa_verified: claims.mfa_verified,
        })
    }
    
    /// Enhanced authorization with context-aware permissions
    pub fn authorize_enhanced(&self, 
        context: &EnterpriseSecurityContext, 
        required_permission: SecurityPermission,
        required_security_level: SecurityLevel
    ) -> Result<()> {
        // Check security level
        if !self.security_level_sufficient(&context.security_level, &required_security_level) {
            return Err(anyhow!("Insufficient security level"));
        }
        
        // Check specific permissions
        if !context.permissions.contains(&required_permission) &&
           !context.permissions.contains(&SecurityPermission::SystemAdmin) {
            return Err(anyhow!("Insufficient permissions"));
        }
        
        // Additional checks for admin operations
        if required_security_level == SecurityLevel::Admin {
            if self.security_config.require_mfa_for_admin && !context.mfa_verified {
                return Err(anyhow!("MFA required for admin operations"));
            }
        }
        
        Ok(())
    }
    
    /// Comprehensive input validation with security scanning
    pub fn validate_input_comprehensive(&self, input: &str, input_type: &str) -> Result<()> {
        // Length validation
        let max_length = match input_type {
            "username" => 100,
            "password" => 200,
            "symbol" => 20,
            "message" => 1000,
            _ => 500,
        };
        
        if input.len() > max_length {
            return Err(anyhow!("Input exceeds maximum length for {}", input_type));
        }
        
        // SQL injection detection
        let sql_patterns = [
            "union select", "drop table", "delete from", "insert into",
            "update set", "create table", "alter table", "--", "/*", "*/",
            "xp_", "sp_", "exec(", "execute("
        ];
        
        let input_lower = input.to_lowercase();
        for pattern in &sql_patterns {
            if input_lower.contains(pattern) {
                self.log_security_incident("sql_injection_attempt", input, "").await;
                return Err(anyhow!("Potentially malicious SQL pattern detected"));
            }
        }
        
        // XSS detection
        let xss_patterns = [
            "<script", "javascript:", "onload=", "onerror=", "onclick=",
            "onmouseover=", "onfocus=", "onblur=", "eval(", "expression("
        ];
        
        for pattern in &xss_patterns {
            if input_lower.contains(pattern) {
                self.log_security_incident("xss_attempt", input, "").await;
                return Err(anyhow!("Potentially malicious XSS pattern detected"));
            }
        }
        
        // Command injection detection
        let cmd_patterns = ["|", "&", ";", "$", "`", "$(", "${"];
        if input_type != "password" {  // Passwords might legitimately contain these
            for pattern in &cmd_patterns {
                if input.contains(pattern) {
                    self.log_security_incident("command_injection_attempt", input, "").await;
                    return Err(anyhow!("Potentially malicious command pattern detected"));
                }
            }
        }
        
        Ok(())
    }
    
    /// Advanced rate limiting with progressive penalties
    async fn check_comprehensive_limits(&self, ip: &str) -> Result<()> {
        // Check if IP is blocked
        {
            let blocked_ips = self.blocked_ips.read().await;
            if let Some(blocked_entry) = blocked_ips.get(ip) {
                if blocked_entry.expires_at.map_or(true, |exp| Utc::now() < exp) {
                    return Err(anyhow!("IP address blocked: {}", blocked_entry.reason));
                }
            }
        }
        
        // Rate limiting with escalating penalties
        let mut rate_limits = self.rate_limits.write().await;
        let now = Utc::now();
        
        let entry = rate_limits.entry(ip.to_string()).or_insert(RateLimitEntry {
            count: 0,
            window_start: now,
            last_request: now,
            violations: 0,
        });
        
        // Reset window if more than a minute has passed
        if now.signed_duration_since(entry.window_start).num_minutes() >= 1 {
            entry.count = 0;
            entry.window_start = now;
        }
        
        entry.count += 1;
        entry.last_request = now;
        
        // Progressive rate limiting based on violation history
        let effective_limit = if entry.violations == 0 {
            self.security_config.rate_limit_per_minute
        } else {
            // Reduce limit for repeat offenders
            std::cmp::max(10, self.security_config.rate_limit_per_minute / (entry.violations + 1))
        };
        
        if entry.count > effective_limit {
            entry.violations += 1;
            
            // Block IP with increasing duration
            let block_duration = Duration::minutes(
                self.security_config.block_duration_minutes * entry.violations as i64
            );
            
            self.block_ip_with_reason(ip, "Rate limit exceeded", Some(block_duration)).await;
            
            return Err(anyhow!("Rate limit exceeded (violation #{} will result in {} minute block)", 
                entry.violations, block_duration.num_minutes()));
        }
        
        Ok(())
    }
    
    /// Enhanced credential verification with security features
    async fn verify_enhanced_credentials(&self, username: &str, password: &str) -> Result<(String, SecurityLevel)> {
        // In production, this would:
        // 1. Use proper password hashing (bcrypt, scrypt, or Argon2)
        // 2. Implement constant-time comparison
        // 3. Check against compromised password databases
        // 4. Implement account lockout policies
        
        // Mock implementation with different security levels
        match username {
            "admin" if password == "admin_secure_pass_2024!" => {
                Ok(("admin_user_id".to_string(), SecurityLevel::Admin))
            },
            "trader" if password == "trader_secure_pass_2024!" => {
                Ok(("trader_user_id".to_string(), SecurityLevel::Elevated))
            },
            "viewer" if password == "viewer_pass_2024!" => {
                Ok(("viewer_user_id".to_string(), SecurityLevel::Authenticated))
            },
            _ => {
                self.log_security_event("authentication_failed", username, "").await;
                Err(anyhow!("Invalid credentials"))
            }
        }
    }
    
    /// Create secure session with comprehensive tracking
    async fn create_secure_session(&self, 
        user_id: &str, 
        ip: &str, 
        user_agent: Option<&str>
    ) -> Result<String> {
        let session_id = self.generate_secure_session_id();
        let now = Utc::now();
        
        let session_entry = SessionEntry {
            user_id: user_id.to_string(),
            created_at: now,
            last_activity: now,
            ip_address: ip.to_string(),
            user_agent: user_agent.map(|s| s.to_string()),
            mfa_verified: false,
        };
        
        let mut sessions = self.active_sessions.write().await;
        sessions.insert(session_id.clone(), session_entry);
        
        // Clean up expired sessions (in production, this would be a background task)
        let session_timeout = Duration::minutes(self.security_config.session_timeout_minutes);
        sessions.retain(|_, entry| {
            now.signed_duration_since(entry.last_activity) < session_timeout
        });
        
        Ok(session_id)
    }
    
    /// Generate cryptographically secure session ID
    fn generate_secure_session_id(&self) -> String {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let mut session_bytes = [0u8; 32];
        rng.fill(&mut session_bytes);
        
        // Add timestamp to ensure uniqueness
        let timestamp = Utc::now().timestamp_millis();
        format!("{}_{}", hex::encode(session_bytes), timestamp)
    }
    
    /// Enhanced security event logging
    async fn log_security_event(&self, event_type: &str, user_id: &str, ip: &str, details: Option<&str>) {
        if !self.security_config.enable_audit_logging {
            return;
        }
        
        let log_entry = format!(
            "{{\"event_type\":\"{}\",\"user_id\":\"{}\",\"ip\":\"{}\",\"timestamp\":\"{}\",\"details\":\"{}\"}}",
            event_type,
            user_id,
            ip,
            Utc::now().to_rfc3339(),
            details.unwrap_or("")
        );
        
        // In production, send to SIEM system
        log_warn!(MAIN_LOGGER, "SECURITY_AUDIT: {}", log_entry);
    }
    
    /// Log security incidents for investigation
    async fn log_security_incident(&self, incident_type: &str, input: &str, ip: &str) {
        let incident_log = format!(
            "SECURITY_INCIDENT: {} | Input: {} | IP: {} | Time: {}",
            incident_type,
            input.chars().take(100).collect::<String>(), // Truncate for safety
            ip,
            Utc::now().to_rfc3339()
        );
        
        // In production, this would trigger alerts and investigations
        log_error!(MAIN_LOGGER, "{}", incident_log);
    }
    
    // Additional helper methods...
    
    fn security_level_sufficient(&self, current: &SecurityLevel, required: &SecurityLevel) -> bool {
        use SecurityLevel::*;
        match (current, required) {
            (Admin, _) => true,
            (Elevated, Elevated) | (Elevated, Authenticated) | (Elevated, Public) => true,
            (Authenticated, Authenticated) | (Authenticated, Public) => true,
            (Public, Public) => true,
            _ => false,
        }
    }
    
    async fn block_ip_with_reason(&self, ip: &str, reason: &str, duration: Option<Duration>) {
        let mut blocked_ips = self.blocked_ips.write().await;
        let expires_at = duration.map(|d| Utc::now() + d);
        
        blocked_ips.insert(ip.to_string(), BlockedEntry {
            blocked_at: Utc::now(),
            reason: reason.to_string(),
            expires_at,
        });
        
        self.log_security_event("ip_blocked", "", ip, Some(reason)).await;
    }
    
    // Mock implementations for demonstration
    async fn requires_mfa(&self, _security_level: &SecurityLevel) -> bool {
        self.security_config.require_mfa_for_admin
    }
    
    async fn verify_mfa(&self, _user_id: &str, _mfa_token: Option<&str>) -> Result<bool> {
        // Mock MFA verification - in production, use TOTP/HOTP
        Ok(_mfa_token.is_some())
    }
    
    async fn get_enhanced_permissions(&self, user_id: &str) -> Result<Vec<SecurityPermission>> {
        match user_id {
            "admin_user_id" => Ok(vec![SecurityPermission::SystemAdmin]),
            "trader_user_id" => Ok(vec![
                SecurityPermission::ReadMarketData,
                SecurityPermission::WriteTradeData,
                SecurityPermission::ReadTradeData,
            ]),
            "viewer_user_id" => Ok(vec![SecurityPermission::ReadMarketData]),
            _ => Ok(vec![]),
        }
    }
    
    fn validate_auth_input(&self, username: &str, password: &str) -> Result<()> {
        self.validate_input_comprehensive(username, "username")?;
        
        if password.len() < self.security_config.password_min_length {
            return Err(anyhow!("Password too short"));
        }
        
        Ok(())
    }
    
    async fn create_enhanced_jwt_token(&self,
        user_id: String,
        permissions: Vec<SecurityPermission>,
        session_id: String,
        security_level: SecurityLevel,
        mfa_verified: bool
    ) -> Result<String> {
        let now = Utc::now();
        let exp = now + Duration::hours(self.security_config.token_expiry_hours);
        
        let claims = EnhancedTokenClaims {
            sub: user_id,
            iat: now.timestamp(),
            exp: exp.timestamp(),
            permissions,
            session_id,
            security_level: format!("{:?}", security_level),
            mfa_verified,
            issuer: "DataEngine-Security".to_string(),
        };
        
        let header = Header::new(Algorithm::HS256);
        let key = EncodingKey::from_secret(self.jwt_secret.as_ref());
        
        jsonwebtoken::encode(&header, &claims, &key)
            .map_err(|e| anyhow!("Failed to create token: {}", e))
    }
    
    fn decode_enhanced_jwt_token(&self, token: &str) -> Result<EnhancedTokenClaims> {
        let key = DecodingKey::from_secret(self.jwt_secret.as_ref());
        let mut validation = Validation::new(Algorithm::HS256);
        validation.set_issuer(&["DataEngine-Security"]);
        
        let token_data = jsonwebtoken::decode::<EnhancedTokenClaims>(token, &key, &validation)
            .map_err(|e| anyhow!("Invalid token: {}", e))?;
        
        Ok(token_data.claims)
    }
    
    async fn validate_active_session(&self, session_id: &str, ip: &str) -> Result<()> {
        let sessions = self.active_sessions.read().await;
        let session = sessions.get(session_id)
            .ok_or_else(|| anyhow!("Invalid session"))?;
        
        // Check session timeout
        let now = Utc::now();
        let session_timeout = Duration::minutes(self.security_config.session_timeout_minutes);
        if now.signed_duration_since(session.last_activity) > session_timeout {
            return Err(anyhow!("Session expired"));
        }
        
        // Optionally check IP consistency (can be disabled for mobile users)
        // if session.ip_address != ip {
        //     return Err(anyhow!("IP address mismatch"));
        // }
        
        Ok(())
    }
    
    async fn update_session_activity(&self, session_id: &str) -> Result<()> {
        let mut sessions = self.active_sessions.write().await;
        if let Some(session) = sessions.get_mut(session_id) {
            session.last_activity = Utc::now();
        }
        Ok(())
    }
}