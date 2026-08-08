//! Security module
//! 
//! This module contains all security-related functionality including:
//! - Security manager and configuration
//! - Input validation and error handling  
//! - Security monitoring and logging

pub mod config;
pub mod error_handling;
pub mod input_validation;
pub mod manager;
pub mod monitoring;

// Re-export commonly used items for convenience
pub use config::*;
pub use error_handling::*;
pub use input_validation::*;
pub use manager::{SecurityManager, SecurityConfig};
pub use monitoring::*;