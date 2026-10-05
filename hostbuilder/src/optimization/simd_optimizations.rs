// SIMD optimizations for data processing

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::*;

/// SIMD-optimized operations for market data processing
pub struct SimdProcessor {
    pub enabled: bool,
}

impl SimdProcessor {
    /// Create a new SIMD processor
    pub fn new() -> Self {
        Self {
            #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
            enabled: is_x86_feature_detected!("avx2"),
            #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
            enabled: false,
        }
    }

    /// Process price data using SIMD instructions
    pub fn process_prices(&self, prices: &mut [f32]) {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if self.enabled && prices.len() >= 8 {
            unsafe {
                self.process_prices_avx2(prices);
            }
        } else {
            self.process_prices_scalar(prices);
        }

        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        self.process_prices_scalar(prices);
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn process_prices_avx2(&self, _prices: &mut [f32]) {
        // Stub implementation for SIMD price processing
        // Would contain actual AVX2 operations for price calculations
    }

    fn process_prices_scalar(&self, _prices: &mut [f32]) {
        // Fallback scalar implementation
        // Regular floating point operations
    }

    /// Calculate moving averages using SIMD
    pub fn calculate_moving_average(&self, data: &[f32], window_size: usize) -> Vec<f32> {
        if data.len() < window_size {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(data.len() - window_size + 1);

        for i in 0..=(data.len() - window_size) {
            let sum: f32 = data[i..i + window_size].iter().sum();
            result.push(sum / window_size as f32);
        }

        result
    }

    /// Vectorized operations for large datasets
    pub fn vectorized_sum(&self, data: &[f32]) -> f32 {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        if self.enabled && data.len() >= 8 {
            unsafe {
                return self.vectorized_sum_avx2(data);
            }
        }

        data.iter().sum()
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn vectorized_sum_avx2(&self, data: &[f32]) -> f32 {
        // Stub implementation for AVX2 sum
        // Would use _mm256_add_ps and related intrinsics
        data.iter().sum() // Fallback for now
    }
}

impl Default for SimdProcessor {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_processor_creation() {
        // `enabled` depends on runtime AVX2 feature detection, so it can't be
        // asserted to a fixed value portably across CI machines. This test
        // only verifies construction doesn't panic.
        let _processor = SimdProcessor::new();
    }

    #[test]
    fn test_moving_average() {
        let processor = SimdProcessor::new();
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = processor.calculate_moving_average(&data, 3);
        assert_eq!(result, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn test_vectorized_sum() {
        let processor = SimdProcessor::new();
        let data = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let result = processor.vectorized_sum(&data);
        assert_eq!(result, 15.0);
    }
}
