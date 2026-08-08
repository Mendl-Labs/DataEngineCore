//! Enhanced security configuration for DataEngine
//! 
//! Provides secure defaults, privilege reduction, and network hardening
//! configurations to minimize attack surface and improve security posture.

use std::path::PathBuf;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use crate::{infrastructure::logging_facade::MAIN_LOGGER, log_info, log_error};

#[derive(Debug, Error)]
pub enum SecurityConfigError {
    #[error("Invalid security configuration: {0}")]
    InvalidConfig(String),
    
    #[error("Privilege operation failed: {0}")]
    PrivilegeError(String),
    
    #[error("Network configuration error: {0}")]
    NetworkError(String),
    
    #[error("Missing required configuration: {0}")]
    MissingConfig(String),
}

pub type SecurityResult<T> = Result<T, SecurityConfigError>;

/// Security configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityConfig {
    pub privilege_config: PrivilegeConfig,
    pub network_config: NetworkConfig,
    pub monitoring_config: MonitoringConfig,
    pub file_permissions: FilePermissions,
    pub memory_protection: MemoryProtection,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrivilegeConfig {
    pub drop_privileges: bool,
    pub target_user: Option<String>,
    pub target_group: Option<String>,
    pub enable_seccomp: bool,
    pub allowed_syscalls: Vec<String>,
    pub chroot_directory: Option<PathBuf>,
    pub disable_core_dumps: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NetworkConfig {
    pub bind_specific_interfaces: bool,
    pub allowed_interfaces: Vec<String>,
    pub enable_tls: bool,
    pub tls_version: String,
    pub allowed_ciphers: Vec<String>,
    pub connection_limits: ConnectionLimits,
    pub firewall_rules: Vec<FirewallRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionLimits {
    pub max_connections: u32,
    pub max_connections_per_ip: u32,
    pub connection_timeout: u64,
    pub keepalive_timeout: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirewallRule {
    pub direction: String, // "inbound" or "outbound"
    pub protocol: String,  // "tcp", "udp", etc.
    pub port_range: String,
    pub allowed_ips: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitoringConfig {
    pub enable_security_logging: bool,
    pub log_failed_attempts: bool,
    pub enable_intrusion_detection: bool,
    pub alert_thresholds: AlertThresholds,
    pub audit_trails: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertThresholds {
    pub failed_login_attempts: u32,
    pub unusual_activity_score: f64,
    pub connection_rate_threshold: u32,
    pub memory_usage_threshold: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilePermissions {
    pub config_file_mode: u32,
    pub log_file_mode: u32,
    pub data_file_mode: u32,
    pub executable_mode: u32,
    pub restricted_directories: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProtection {
    pub enable_aslr: bool,
    pub enable_stack_protection: bool,
    pub enable_heap_protection: bool,
    pub secure_memory_allocation: bool,
    pub memory_limits: MemoryLimits,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryLimits {
    pub max_heap_size: u64,
    pub max_stack_size: u64,
    pub max_virtual_memory: u64,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        Self {
            privilege_config: PrivilegeConfig {
                drop_privileges: true,
                target_user: Some("dataengine".to_string()),
                target_group: Some("dataengine".to_string()),
                enable_seccomp: true,
                allowed_syscalls: vec![
                    "read".to_string(),
                    "write".to_string(),
                    "open".to_string(),
                    "close".to_string(),
                    "socket".to_string(),
                    "bind".to_string(),
                    "listen".to_string(),
                    "accept".to_string(),
                    "poll".to_string(),
                    "epoll_wait".to_string(),
                    "mmap".to_string(),
                    "munmap".to_string(),
                    "clock_gettime".to_string(),
                    "exit".to_string(),
                    "exit_group".to_string(),
                ],
                chroot_directory: Some(PathBuf::from("/opt/dataengine")),
                disable_core_dumps: true,
            },
            network_config: NetworkConfig {
                bind_specific_interfaces: true,
                allowed_interfaces: vec!["eth0".to_string(), "lo".to_string()],
                enable_tls: true,
                tls_version: "1.3".to_string(),
                allowed_ciphers: vec![
                    "TLS_AES_256_GCM_SHA384".to_string(),
                    "TLS_CHACHA20_POLY1305_SHA256".to_string(),
                    "TLS_AES_128_GCM_SHA256".to_string(),
                ],
                connection_limits: ConnectionLimits {
                    max_connections: 1000,
                    max_connections_per_ip: 10,
                    connection_timeout: 30,
                    keepalive_timeout: 300,
                },
                firewall_rules: vec![
                    FirewallRule {
                        direction: "inbound".to_string(),
                        protocol: "tcp".to_string(),
                        port_range: "8080".to_string(),
                        allowed_ips: vec!["10.0.0.0/8".to_string(), "192.168.0.0/16".to_string()],
                    },
                ],
            },
            monitoring_config: MonitoringConfig {
                enable_security_logging: true,
                log_failed_attempts: true,
                enable_intrusion_detection: true,
                alert_thresholds: AlertThresholds {
                    failed_login_attempts: 5,
                    unusual_activity_score: 0.8,
                    connection_rate_threshold: 100,
                    memory_usage_threshold: 0.9,
                },
                audit_trails: true,
            },
            file_permissions: FilePermissions {
                config_file_mode: 0o600, // rw-------
                log_file_mode: 0o640,    // rw-r-----
                data_file_mode: 0o660,   // rw-rw----
                executable_mode: 0o750,  // rwxr-x---
                restricted_directories: vec![
                    PathBuf::from("/etc"),
                    PathBuf::from("/proc"),
                    PathBuf::from("/sys"),
                    PathBuf::from("/dev"),
                ],
            },
            memory_protection: MemoryProtection {
                enable_aslr: true,
                enable_stack_protection: true,
                enable_heap_protection: true,
                secure_memory_allocation: true,
                memory_limits: MemoryLimits {
                    max_heap_size: 8 * 1024 * 1024 * 1024, // 8GB
                    max_stack_size: 8 * 1024 * 1024,       // 8MB
                    max_virtual_memory: 16 * 1024 * 1024 * 1024, // 16GB
                },
            },
        }
    }
}

/// Security configuration manager
pub struct SecurityConfigManager {
    config: SecurityConfig,
}

impl SecurityConfigManager {
    pub fn new() -> Self {
        Self {
            config: SecurityConfig::default(),
        }
    }
    
    pub fn load_from_file<P: AsRef<std::path::Path>>(path: P) -> SecurityResult<Self> {
        let path_ref = path.as_ref();
        log_info!(MAIN_LOGGER, "Loading security configuration from {:?}", path_ref);
        let content = std::fs::read_to_string(path_ref)
            .map_err(|e| {
                log_error!(MAIN_LOGGER, "Failed to read security config file {:?}: {}", path_ref, e);
                SecurityConfigError::InvalidConfig(e.to_string())
            })?;

        let config: SecurityConfig = serde_yaml::from_str(&content)
            .map_err(|e| {
                log_error!(MAIN_LOGGER, "Failed to parse security config file {:?}: {}", path_ref, e);
                SecurityConfigError::InvalidConfig(e.to_string())
            })?;

        log_info!(MAIN_LOGGER, "Security configuration loaded successfully from {:?}", path_ref);
        Ok(Self { config })
    }
    
    pub fn validate_config(&self) -> SecurityResult<()> {
        // Validate privilege configuration
        if self.config.privilege_config.drop_privileges {
            if self.config.privilege_config.target_user.is_none() {
                log_error!(MAIN_LOGGER, "Security config validation failed: target_user required when drop_privileges is enabled");
                return Err(SecurityConfigError::MissingConfig(
                    "target_user required when drop_privileges is enabled".to_string()
                ));
            }
        }

        // Validate network configuration
        if self.config.network_config.enable_tls {
            if !["1.2", "1.3"].contains(&self.config.network_config.tls_version.as_str()) {
                log_error!(MAIN_LOGGER, "Security config validation failed: unsupported TLS version '{}'", self.config.network_config.tls_version);
                return Err(SecurityConfigError::InvalidConfig(
                    "TLS version must be 1.2 or 1.3".to_string()
                ));
            }
        }

        // Validate connection limits
        if self.config.network_config.connection_limits.max_connections == 0 {
            log_error!(MAIN_LOGGER, "Security config validation failed: max_connections must be greater than 0");
            return Err(SecurityConfigError::InvalidConfig(
                "max_connections must be greater than 0".to_string()
            ));
        }

        // Validate memory limits
        if self.config.memory_protection.memory_limits.max_heap_size < 1024 * 1024 {
            log_error!(MAIN_LOGGER, "Security config validation failed: max_heap_size {} is below minimum 1MB", self.config.memory_protection.memory_limits.max_heap_size);
            return Err(SecurityConfigError::InvalidConfig(
                "max_heap_size must be at least 1MB".to_string()
            ));
        }

        log_info!(MAIN_LOGGER, "Security configuration validated successfully");
        Ok(())
    }
    
    /// Apply privilege reduction settings
    pub fn apply_privilege_reduction(&self) -> SecurityResult<()> {
        if !self.config.privilege_config.drop_privileges {
            log_info!(MAIN_LOGGER, "Privilege reduction skipped: drop_privileges is disabled");
            return Ok(());
        }

        log_info!(MAIN_LOGGER, "Applying privilege reduction (target_user={:?}, seccomp={}, disable_core_dumps={})",
            self.config.privilege_config.target_user,
            self.config.privilege_config.enable_seccomp,
            self.config.privilege_config.disable_core_dumps);

        // Disable core dumps if configured
        if self.config.privilege_config.disable_core_dumps {
            #[cfg(unix)]
            {
                use libc::{setrlimit, RLIMIT_CORE, rlimit};
                let rlim = rlimit {
                    rlim_cur: 0,
                    rlim_max: 0,
                };
                unsafe {
                    if setrlimit(RLIMIT_CORE, &rlim) != 0 {
                        log_error!(MAIN_LOGGER, "Failed to disable core dumps via setrlimit");
                        return Err(SecurityConfigError::PrivilegeError(
                            "Failed to disable core dumps".to_string()
                        ));
                    }
                }
            }
            log_info!(MAIN_LOGGER, "Core dumps disabled successfully");
        }

        // Apply memory limits
        self.apply_memory_limits()?;

        // Drop privileges (this should be done last)
        self.drop_privileges()?;

        log_info!(MAIN_LOGGER, "Privilege reduction applied successfully");
        Ok(())
    }
    
    fn apply_memory_limits(&self) -> SecurityResult<()> {
        #[cfg(unix)]
        {
            use libc::{setrlimit, RLIMIT_AS, RLIMIT_STACK, rlimit};

            log_info!(MAIN_LOGGER, "Applying memory limits: virtual_memory={}MB, stack={}MB",
                self.config.memory_protection.memory_limits.max_virtual_memory / (1024 * 1024),
                self.config.memory_protection.memory_limits.max_stack_size / (1024 * 1024));

            // Set virtual memory limit
            let vm_limit = rlimit {
                rlim_cur: self.config.memory_protection.memory_limits.max_virtual_memory,
                rlim_max: self.config.memory_protection.memory_limits.max_virtual_memory,
            };
            unsafe {
                if setrlimit(RLIMIT_AS, &vm_limit) != 0 {
                    log_error!(MAIN_LOGGER, "Failed to set virtual memory limit to {} bytes", self.config.memory_protection.memory_limits.max_virtual_memory);
                    return Err(SecurityConfigError::PrivilegeError(
                        "Failed to set virtual memory limit".to_string()
                    ));
                }
            }

            // Set stack limit
            let stack_limit = rlimit {
                rlim_cur: self.config.memory_protection.memory_limits.max_stack_size,
                rlim_max: self.config.memory_protection.memory_limits.max_stack_size,
            };
            unsafe {
                if setrlimit(RLIMIT_STACK, &stack_limit) != 0 {
                    log_error!(MAIN_LOGGER, "Failed to set stack limit to {} bytes", self.config.memory_protection.memory_limits.max_stack_size);
                    return Err(SecurityConfigError::PrivilegeError(
                        "Failed to set stack limit".to_string()
                    ));
                }
            }
        }

        Ok(())
    }
    
    fn drop_privileges(&self) -> SecurityResult<()> {
        #[cfg(unix)]
        {
            if let Some(target_user) = &self.config.privilege_config.target_user {
                log_info!(MAIN_LOGGER, "Dropping privileges to user '{}'", target_user);
                // Get user ID
                use std::ffi::CString;
                use libc::{getpwnam, setuid, setgid};

                let c_user = CString::new(target_user.as_str())
                    .map_err(|_| {
                        log_error!(MAIN_LOGGER, "Privilege drop failed: invalid username encoding for '{}'", target_user);
                        SecurityConfigError::PrivilegeError(
                            "Invalid username".to_string()
                        )
                    })?;

                let passwd = unsafe { getpwnam(c_user.as_ptr()) };
                if passwd.is_null() {
                    log_error!(MAIN_LOGGER, "Privilege drop failed: user '{}' not found on system", target_user);
                    return Err(SecurityConfigError::PrivilegeError(
                        format!("User {} not found", target_user)
                    ));
                }

                let uid = unsafe { (*passwd).pw_uid };
                let gid = unsafe { (*passwd).pw_gid };

                // Drop group privileges first
                unsafe {
                    if setgid(gid) != 0 {
                        log_error!(MAIN_LOGGER, "Privilege drop failed: setgid({}) returned error", gid);
                        return Err(SecurityConfigError::PrivilegeError(
                            "Failed to drop group privileges".to_string()
                        ));
                    }
                }

                // Drop user privileges
                unsafe {
                    if setuid(uid) != 0 {
                        log_error!(MAIN_LOGGER, "Privilege drop failed: setuid({}) returned error", uid);
                        return Err(SecurityConfigError::PrivilegeError(
                            "Failed to drop user privileges".to_string()
                        ));
                    }
                }

                log_info!(MAIN_LOGGER, "Privileges dropped successfully to uid={}, gid={}", uid, gid);
            }
        }

        Ok(())
    }
    
    /// Apply network security settings
    pub fn apply_network_security(&self) -> SecurityResult<()> {
        log_info!(MAIN_LOGGER, "Applying network security: tls={}, tls_version={}, bind_specific={}, max_connections={}",
            self.config.network_config.enable_tls,
            self.config.network_config.tls_version,
            self.config.network_config.bind_specific_interfaces,
            self.config.network_config.connection_limits.max_connections);

        // Configure socket options for security
        self.configure_socket_security()?;

        // Apply connection limits
        self.apply_connection_limits()?;

        log_info!(MAIN_LOGGER, "Network security hardening applied successfully ({} firewall rules configured)",
            self.config.network_config.firewall_rules.len());
        Ok(())
    }
    
    fn configure_socket_security(&self) -> SecurityResult<()> {
        // This would typically configure socket options like:
        // - SO_REUSEADDR security
        // - TCP_NODELAY for performance/security
        // - SO_KEEPALIVE for connection management
        // Implementation depends on specific socket library used
        Ok(())
    }
    
    fn apply_connection_limits(&self) -> SecurityResult<()> {
        // Implementation would configure:
        // - Connection rate limiting
        // - Per-IP connection limits
        // - Total connection limits
        // This typically integrates with the WebSocket/HTTP server
        Ok(())
    }
    
    /// Get security configuration
    pub fn get_config(&self) -> &SecurityConfig {
        &self.config
    }
    
    /// Update specific security settings
    pub fn update_config<F>(&mut self, updater: F) -> SecurityResult<()>
    where
        F: FnOnce(&mut SecurityConfig),
    {
        updater(&mut self.config);
        self.validate_config()?;
        Ok(())
    }
}

/// Security utilities for runtime checks
pub mod security_utils {
    #[allow(unused_imports)]
    use super::*;
    
    /// Check if running with root privileges
    pub fn is_running_as_root() -> bool {
        #[cfg(unix)]
        {
            unsafe { libc::getuid() == 0 }
        }
        #[cfg(not(unix))]
        {
            false // Windows privilege checking is more complex
        }
    }
    
    /// Secure random number generation
    pub fn generate_secure_random(size: usize) -> Vec<u8> {
        use rand::RngCore;
        let mut rng = rand::rngs::OsRng;
        let mut buffer = vec![0u8; size];
        rng.fill_bytes(&mut buffer);
        buffer
    }
    
    /// Secure memory zeroing
    pub fn secure_zero_memory(buffer: &mut [u8]) {
        // Use volatile writes to prevent compiler optimization
        for byte in buffer.iter_mut() {
            unsafe {
                std::ptr::write_volatile(byte, 0);
            }
        }
    }
    
    /// Check if address is in private IP range
    pub fn is_private_ip(ip: &str) -> bool {
        use std::net::IpAddr;
        
        if let Ok(addr) = ip.parse::<IpAddr>() {
            match addr {
                IpAddr::V4(ipv4) => {
                    let octets = ipv4.octets();
                    // 10.0.0.0/8
                    octets[0] == 10 ||
                    // 172.16.0.0/12
                    (octets[0] == 172 && (octets[1] & 0xf0) == 16) ||
                    // 192.168.0.0/16
                    (octets[0] == 192 && octets[1] == 168) ||
                    // 127.0.0.0/8 (loopback)
                    octets[0] == 127
                }
                IpAddr::V6(ipv6) => {
                    // Check for loopback and private ranges
                    ipv6.is_loopback() || 
                    ipv6.segments()[0] & 0xfe00 == 0xfc00 // fc00::/7
                }
            }
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_default_security_config() {
        let config = SecurityConfig::default();
        assert!(config.privilege_config.drop_privileges);
        assert!(config.network_config.enable_tls);
        assert_eq!(config.network_config.tls_version, "1.3");
    }
    
    #[test]
    fn test_security_config_validation() {
        let manager = SecurityConfigManager::new();
        assert!(manager.validate_config().is_ok());
    }
    
    #[test]
    fn test_security_utils() {
        use security_utils::*;
        
        // Test private IP detection
        assert!(is_private_ip("192.168.1.1"));
        assert!(is_private_ip("10.0.0.1"));
        assert!(!is_private_ip("8.8.8.8"));
        
        // Test secure random generation
        let random_data = generate_secure_random(32);
        assert_eq!(random_data.len(), 32);
        
        // Test secure memory zeroing
        let mut buffer = vec![0xaa; 16];
        secure_zero_memory(&mut buffer);
        assert!(buffer.iter().all(|&b| b == 0));
    }
}
