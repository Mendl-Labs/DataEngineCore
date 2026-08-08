//! DataEngine Hostbuilder Library
//!
//! This library provides the main application structure for the DataEngine,
//! organized into logical modules for better maintainability and clarity.
//!
//! # Module Organization
//!
//! - `core`: Main application structure and hosted object
//! - `infrastructure`: System infrastructure (monitoring, resilience, logging)
//! - `security`: Security management and validation
//! - `data`: Data handling, caching, and pipeline processing
//! - `network`: Network communication and API endpoints
//! - `optimization`: Performance optimizations and metrics

// Core application modules
pub mod core;
pub mod infrastructure;
pub mod security;

// Data handling modules
pub mod data;

// Network communication modules
pub mod network;

// Performance optimization modules
pub mod optimization;

// Legacy modules (to be moved/removed)
pub mod tests;

// Re-export the main application components for backward compatibility
pub use core::{HostedObject, HostedObjectTrait};
pub use core::{
    TenantSubscriptionLimiter, SubscriptionTier, TenantSubscriptionStats,
    GlobalSubscriptionStats, SubscriptionValidation, TierLimits,
};
pub use network::api_endpoints::start_api_server;

// Re-export commonly used items from each module
#[cfg(feature = "database")]
pub use data::{get_exchange, get_securities, get_orderbooks};
pub use infrastructure::logging_facade::MAIN_LOGGER;
pub use security::{SecurityManager, SecurityConfig};
