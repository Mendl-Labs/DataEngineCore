//! Network module
//!
//! This module contains all network communication functionality including:
//! - API endpoints and HTTP servers
//! - WebSocket and REST handlers  
//! - Network endpoint configuration

pub mod api_endpoints;
pub mod endpoint;
pub mod handlers;

// Re-export commonly used items
pub use api_endpoints::*;
pub use endpoint::*;
pub use handlers::*;
