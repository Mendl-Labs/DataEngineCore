//! Infrastructure module
//! 
//! This module contains system infrastructure components including:
//! - Health monitoring and system checks
//! - Error handling and resilience patterns
//! - Logging facade and log management

pub mod logging_facade;
pub mod monitoring;
pub mod resilience;

// Re-export commonly used items
pub use logging_facade::*;
pub use monitoring::{SystemHealthMonitor, WebSocketHealthChecker, PerformanceHealthChecker};
#[cfg(feature = "database")]
pub use monitoring::DatabaseHealthChecker;
pub use resilience::*;

// Re-export logging macros at crate root via this module
pub use crate::{log_debug, log_info, log_warn, log_error};