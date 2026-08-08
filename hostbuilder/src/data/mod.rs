//! Data module
//! 
//! This module contains all data handling functionality including:
//! - Caching strategies and implementations
//! - Data pipeline processing
//! - Order book management
//! - Information retrieval and persistence

pub mod caching;
#[cfg(feature = "database")]
pub mod get_info;
pub mod order_book;
pub mod pipeline;

// Re-export commonly used items
pub use caching::*;
#[cfg(feature = "database")]
pub use get_info::{get_exchange, get_securities, get_orderbooks};
pub use order_book::*;
pub use pipeline::*;