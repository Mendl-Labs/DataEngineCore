//! Optimization module
//! 
//! This module contains performance optimization functionality including:
//! - SIMD optimizations for numerical processing
//! - Kernel bypass techniques for low-latency operations
//! - Ultra-fast metrics and performance monitoring

pub mod kernel_bypass;
pub mod simd_optimizations;
pub mod ultra_metrics;

// Re-export commonly used items
pub use kernel_bypass::*;
pub use simd_optimizations::*;
pub use ultra_metrics::*;