#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
use std::arch::x86_64::*;
use anyhow::Result;
use std::sync::Once;

#[derive(Debug)]
pub struct SimdCapabilities {
    pub has_avx512: bool,
    pub has_avx2: bool,
    pub has_sse42: bool,
}

#[derive(Clone)]
pub struct ParsedMessage {
    pub message_type: u32,
    pub timestamp: u64,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Clone, Debug)]
pub enum MessageType {
    Level3,
    Trade,
    Balance,
    Unknown,
}

#[derive(Clone)]
pub struct ParsedMessageBatch {
    pub message_type: MessageType,
    pub timestamp: u64,
    pub data: Vec<u8>,
}

pub struct SimdMessageParser {
    _capabilities: SimdCapabilities,
}

pub struct SimdCalculator {
    _capabilities: SimdCapabilities,
}

static INIT: Once = Once::new();
static mut PARSER: Option<SimdMessageParser> = None;
static mut CALCULATOR: Option<SimdCalculator> = None;

impl SimdCapabilities {
    pub fn detect() -> Self {
        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            Self {
                has_avx512: is_x86_feature_detected!("avx512f"),
                has_avx2: is_x86_feature_detected!("avx2"),
                has_sse42: is_x86_feature_detected!("sse4.2"),
            }
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            Self {
                has_avx512: false,
                has_avx2: false,
                has_sse42: false,
            }
        }
    }
}

impl SimdMessageParser {
    pub fn new() -> Self {
        Self {
            _capabilities: SimdCapabilities::detect(),
        }
    }

    pub fn parse_message(&self, buffer: &[u8]) -> Result<ParsedMessage> {
        if buffer.len() < 32 {
            return Err(anyhow::anyhow!("Buffer too small"));
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if self._capabilities.has_avx2 {
                unsafe { self.parse_with_avx2(buffer) }
            } else if self._capabilities.has_sse42 {
                unsafe { self.parse_with_sse42(buffer) }
            } else {
                self.parse_scalar(buffer)
            }
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            self.parse_scalar(buffer)
        }
    }

    /// Parse multiple messages from buffer
    pub fn parse_messages(&self, buffer: &[u8]) -> Vec<ParsedMessageBatch> {
        // Simple implementation - parse as single message for now
        let mut messages = Vec::new();
        
        if buffer.len() >= 4 {
            let message_type = match buffer[0] {
                1 => MessageType::Level3,
                2 => MessageType::Trade,
                3 => MessageType::Balance,
                _ => MessageType::Unknown,
            };
            
            messages.push(ParsedMessageBatch {
                message_type,
                timestamp: get_hardware_timestamp(),
                data: buffer.to_vec(),
            });
        }
        
        messages
    }

    /// Hardware timestamp function
    pub fn hardware_timestamp() -> u64 {
        get_hardware_timestamp()
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn parse_with_avx2(&self, buffer: &[u8]) -> Result<ParsedMessage> {
        let data = _mm256_loadu_si256(buffer.as_ptr() as *const __m256i);
        let timestamp = rdtsc();
        
        // Extract message components using SIMD
        let mut temp_buf = [0u8; 32];
        _mm256_storeu_si256(temp_buf.as_mut_ptr() as *mut __m256i, data);
        
        Ok(ParsedMessage {
            message_type: u32::from_le_bytes([temp_buf[0], temp_buf[1], temp_buf[2], temp_buf[3]]),
            timestamp,
            price: f64::from_le_bytes([
                temp_buf[8], temp_buf[9], temp_buf[10], temp_buf[11],
                temp_buf[12], temp_buf[13], temp_buf[14], temp_buf[15]
            ]),
            quantity: f64::from_le_bytes([
                temp_buf[16], temp_buf[17], temp_buf[18], temp_buf[19],
                temp_buf[20], temp_buf[21], temp_buf[22], temp_buf[23]
            ]),
        })
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "sse4.2")]
    unsafe fn parse_with_sse42(&self, buffer: &[u8]) -> Result<ParsedMessage> {
        let data = _mm_loadu_si128(buffer.as_ptr() as *const __m128i);
        let timestamp = rdtsc();
        
        let mut temp_buf = [0u8; 16];
        _mm_storeu_si128(temp_buf.as_mut_ptr() as *mut __m128i, data);
        
        Ok(ParsedMessage {
            message_type: u32::from_le_bytes([temp_buf[0], temp_buf[1], temp_buf[2], temp_buf[3]]),
            timestamp,
            price: f64::from_le_bytes([
                temp_buf[4], temp_buf[5], temp_buf[6], temp_buf[7],
                buffer[12], buffer[13], buffer[14], buffer[15]
            ]),
            quantity: f64::from_le_bytes([
                buffer[16], buffer[17], buffer[18], buffer[19],
                buffer[20], buffer[21], buffer[22], buffer[23]
            ]),
        })
    }

    fn parse_scalar(&self, buffer: &[u8]) -> Result<ParsedMessage> {
        Ok(ParsedMessage {
            message_type: u32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]),
            timestamp: get_hardware_timestamp(),
            price: f64::from_le_bytes([
                buffer[8], buffer[9], buffer[10], buffer[11],
                buffer[12], buffer[13], buffer[14], buffer[15]
            ]),
            quantity: f64::from_le_bytes([
                buffer[16], buffer[17], buffer[18], buffer[19],
                buffer[20], buffer[21], buffer[22], buffer[23]
            ]),
        })
    }
}

impl SimdCalculator {
    pub fn new() -> Self {
        Self {
            _capabilities: SimdCapabilities::detect(),
        }
    }

    pub fn calculate_vwap(&self, prices: &[f64], volumes: &[f64]) -> f64 {
        if prices.len() != volumes.len() || prices.is_empty() {
            return 0.0;
        }

        #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
        {
            if self._capabilities.has_avx2 {
                unsafe { self.calculate_vwap_avx2(prices, volumes) }
            } else {
                self.calculate_vwap_scalar(prices, volumes)
            }
        }
        #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
        {
            self.calculate_vwap_scalar(prices, volumes)
        }
    }

    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    #[target_feature(enable = "avx2")]
    unsafe fn calculate_vwap_avx2(&self, prices: &[f64], volumes: &[f64]) -> f64 {
        let mut total_value = 0.0;
        let mut total_volume = 0.0;
        
        let chunks = prices.len() / 4;
        let remainder = prices.len() % 4;
        
        for i in 0..chunks {
            let base = i * 4;
            let price_vec = _mm256_loadu_pd(prices.as_ptr().add(base));
            let volume_vec = _mm256_loadu_pd(volumes.as_ptr().add(base));
            let value_vec = _mm256_mul_pd(price_vec, volume_vec);
            
            let mut values = [0.0; 4];
            let mut vols = [0.0; 4];
            _mm256_storeu_pd(values.as_mut_ptr(), value_vec);
            _mm256_storeu_pd(vols.as_mut_ptr(), volume_vec);
            
            total_value += values.iter().sum::<f64>();
            total_volume += vols.iter().sum::<f64>();
        }
        
        // Handle remainder
        for i in (chunks * 4)..prices.len() {
            total_value += prices[i] * volumes[i];
            total_volume += volumes[i];
        }
        
        if total_volume > 0.0 { total_value / total_volume } else { 0.0 }
    }

    fn calculate_vwap_scalar(&self, prices: &[f64], volumes: &[f64]) -> f64 {
        let total_value: f64 = prices.iter().zip(volumes.iter()).map(|(p, v)| p * v).sum();
        let total_volume: f64 = volumes.iter().sum();
        if total_volume > 0.0 { total_value / total_volume } else { 0.0 }
    }
}

pub fn get_global_parser() -> &'static SimdMessageParser {
    unsafe {
        INIT.call_once(|| {
            PARSER = Some(SimdMessageParser::new());
            CALCULATOR = Some(SimdCalculator::new());
        });
        match PARSER {
            Some(ref parser) => parser,
            None => panic!("Parser not initialized"),
        }
    }
}

pub fn get_global_calculator() -> &'static SimdCalculator {
    unsafe {
        INIT.call_once(|| {
            PARSER = Some(SimdMessageParser::new());
            CALCULATOR = Some(SimdCalculator::new());
        });
        match CALCULATOR {
            Some(ref calculator) => calculator,
            None => panic!("Calculator not initialized"),
        }
    }
}

// Initialize global SIMD parsers - for compatibility
pub fn init_global_simd_parser() {
    let _ = get_global_parser();
    let _ = get_global_calculator();
}

// For compatibility with existing code
pub fn get_global_simd_parser() -> &'static SimdMessageParser {
    get_global_parser()
}

#[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
#[inline]
fn rdtsc() -> u64 {
    unsafe { _rdtsc() }
}

fn get_hardware_timestamp() -> u64 {
    #[cfg(any(target_arch = "x86", target_arch = "x86_64"))]
    {
        unsafe { _rdtsc() }
    }
    #[cfg(not(any(target_arch = "x86", target_arch = "x86_64")))]
    {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simd_capabilities() {
        let caps = SimdCapabilities::detect();
        println!("SIMD Capabilities: {:?}", caps);
    }

    #[test]
    fn test_message_parsing() {
        let parser = SimdMessageParser::new();
        let buffer = [0u8; 32];
        let result = parser.parse_message(&buffer);
        assert!(result.is_ok());
    }

    #[test]
    fn test_vwap_calculation() {
        let calc = SimdCalculator::new();
        let prices = vec![100.0, 101.0, 102.0, 103.0];
        let volumes = vec![10.0, 20.0, 30.0, 40.0];
        let vwap = calc.calculate_vwap(&prices, &volumes);
        assert!(vwap > 0.0);
    }
}
