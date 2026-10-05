//! Core module  
//!
//! This module contains the core application structure including:
//! - HostedObject trait and implementation
//! - Application initialization logic
//! - Main application runtime
//! - Subscription-based WebSocket connection management
//! - Multi-tenant subscription limits and metrics

pub mod hosted_object;
pub mod subscription_manager;
pub mod tenant_subscription_limits;

// Re-export the main application components
pub use hosted_object::{HostedObject, HostedObjectTrait};
pub use subscription_manager::{ConnectionEvent, SubscriptionManager};
pub use tenant_subscription_limits::{
    GlobalSubscriptionStats, SubscriptionTier, SubscriptionValidation, TenantSubscriptionLimiter,
    TenantSubscriptionStats, TierLimits,
};
