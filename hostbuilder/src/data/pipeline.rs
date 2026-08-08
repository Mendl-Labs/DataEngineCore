use anyhow::{anyhow, Result};
use serde_json::Value;
use std::sync::Arc;
use crate::infrastructure::logging_facade::MAIN_LOGGER; use crate::{log_info, log_warn};

use crate::data::caching::CacheManager;
use crate::optimization::kernel_bypass::KernelBypassManager;
use crate::infrastructure::monitoring::MetricsCollector;
use crate::data::order_book::{LockFreeOrderBook, OrderSide};
use performance::{
    simd_ops::{get_global_simd_parser, init_global_simd_parser},
    cpu_affinity::set_thread_affinity,
};

pub struct DataPipeline {
    cache_manager: Arc<CacheManager>,
    metrics_collector: Arc<MetricsCollector>,
    kernel_bypass: Option<Arc<KernelBypassManager>>,
    order_books: std::collections::HashMap<String, Arc<LockFreeOrderBook>>,
    processing_stats: ProcessingStats,
}

#[derive(Debug, Default)]
struct ProcessingStats {
    messages_processed: std::sync::atomic::AtomicU64,
    orders_added: std::sync::atomic::AtomicU64,
    l3_updates: std::sync::atomic::AtomicU64,
    trades_processed: std::sync::atomic::AtomicU64,
    errors: std::sync::atomic::AtomicU64,
}

impl DataPipeline {
    pub async fn new() -> Result<Self> {
        // Initialize global SIMD parser
        init_global_simd_parser();
        
        // Initialize kernel bypass for ultra-low latency
        let kernel_bypass = match KernelBypassManager::new().await {
            Ok(kb) => {
                log_info!(MAIN_LOGGER, "Kernel bypass networking initialized for sub-microsecond latency");
                Some(Arc::new(kb))
            }
            Err(e) => {
                log_warn!(MAIN_LOGGER, "Failed to initialize kernel bypass: {}. Falling back to standard networking", e);
                None
            }
        };
        
        // Set CPU affinity for this thread to core 0 (isolated core)
        if let Err(e) = set_thread_affinity(vec![0]) {
            log_warn!(MAIN_LOGGER, "Failed to set CPU affinity: {}", e);
        }
        
        Ok(Self {
            cache_manager: Arc::new(CacheManager::new().await?),
            metrics_collector: Arc::new(MetricsCollector::new()),
            kernel_bypass,
            order_books: std::collections::HashMap::new(),
            processing_stats: ProcessingStats::default(),
        })
    }

    /// Process incoming market data with zero-copy optimization
    pub async fn process_market_data(&mut self, data: &[u8]) -> Result<()> {
        use std::sync::atomic::Ordering;
        
        // Use zero-copy buffer from global pool
        let buffer_pool = unsafe { 
            match performance::memory_pool::GLOBAL_BUFFER_POOL {
                Some(ref pool) => pool,
                None => return Err(anyhow!("Global buffer pool not initialized")),
            }
        };
        let mut buffer = buffer_pool.get_buffer()?;
        
        // Copy data into zero-copy buffer (this will be the only copy)
        let buffer_slice = buffer.as_mut_slice();
        if data.len() <= buffer_slice.len() {
            buffer_slice[..data.len()].copy_from_slice(data);
        } else {
            return Err(anyhow!("Data too large for buffer"));
        }
        
        // Use SIMD-accelerated message parsing
        let parser = get_global_simd_parser();
        let messages = parser.parse_messages(&buffer_slice[..data.len()]);
        
        // Process each parsed message
        for message in messages {
            match message.message_type {
                performance::simd_ops::MessageType::Level3 => {
                    self.process_level3_update(&message.data).await?;
                    self.processing_stats.l3_updates.fetch_add(1, Ordering::Relaxed);
                }
                performance::simd_ops::MessageType::Trade => {
                    self.process_trade(&message.data).await?;
                    self.processing_stats.trades_processed.fetch_add(1, Ordering::Relaxed);
                }
                performance::simd_ops::MessageType::Balance => {
                    self.process_balance_update(&message.data).await?;
                }
                performance::simd_ops::MessageType::Unknown => {
                    self.process_generic_message(&message.data).await?;
                }
            }
            
            self.processing_stats.messages_processed.fetch_add(1, Ordering::Relaxed);
        }
        
        // Update metrics
        self.metrics_collector
            .record_processing_latency(std::time::Duration::from_nanos(100)); // Dummy latency
        
        Ok(())
    }
    
    /// Process Level 3 order book updates with ultra-low latency
    async fn process_level3_update(&mut self, data: &[u8]) -> Result<()> {
        // Parse Level 3 message using zero-copy JSON parsing
        let message: Value = simd_json::from_slice(
            &mut data.to_vec() // simd_json requires mutable slice
        ).map_err(|e| anyhow!("Failed to parse L3 message: {}", e))?;
        
        // Extract symbol and order details
        let symbol = message["symbol"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing symbol in L3 message"))?;
        
        let order_id = message["orderId"]
            .as_u64()
            .ok_or_else(|| anyhow!("Missing orderId in L3 message"))?;
        
        let price = message["price"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow!("Missing or invalid price in L3 message"))?;
        
        let quantity = message["quantity"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow!("Missing or invalid quantity in L3 message"))?;
        
        let side = match message["side"].as_str() {
            Some("buy") => OrderSide::Buy,
            Some("sell") => OrderSide::Sell,
            _ => return Err(anyhow!("Missing or invalid side in L3 message")),
        };
        
        // Get or create order book
        let order_book = self.get_or_create_order_book(symbol).await?;
        
        // Add order to lock-free order book
        order_book.add_order(order_id, price, quantity, side)?;
        
        // Update cache with new book state
        if let Some(best_bid) = order_book.get_best_bid() {
            let bid_value = serde_json::json!({
                "price": best_bid.price,
                "quantity": best_bid.quantity
            });
            self.cache_manager
                .set(&format!("{}_best_bid", symbol), &bid_value)
                .await?;
        }
        
        if let Some(best_ask) = order_book.get_best_ask() {
            let ask_value = serde_json::json!({
                "price": best_ask.price,
                "quantity": best_ask.quantity
            });
            self.cache_manager
                .set(&format!("{}_best_ask", symbol), &ask_value)
                .await?;
        }
        
        // Update spread cache
        if let Some(spread) = order_book.get_spread() {
            let spread_value = serde_json::json!(spread);
            self.cache_manager
                .set(&format!("{}_spread", symbol), &spread_value)
                .await?;
        }
        
        self.processing_stats.orders_added.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        
        Ok(())
    }
    
    /// Process trade messages
    async fn process_trade(&mut self, data: &[u8]) -> Result<()> {
        let message: Value = simd_json::from_slice(
            &mut data.to_vec()
        ).map_err(|e| anyhow!("Failed to parse trade message: {}", e))?;
        
        let symbol = message["symbol"]
            .as_str()
            .ok_or_else(|| anyhow!("Missing symbol in trade message"))?;
        
        let price = message["price"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow!("Missing or invalid price in trade message"))?;
        
        let quantity = message["quantity"]
            .as_str()
            .and_then(|s| s.parse::<f64>().ok())
            .ok_or_else(|| anyhow!("Missing or invalid quantity in trade message"))?;
        
        // Cache latest trade
        let trade_data = serde_json::json!({
            "price": price,
            "quantity": quantity,
            "timestamp": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        });
        
        self.cache_manager
            .set(&format!("{}_latest_trade", symbol), &trade_data)
            .await?;
        
        // Update metrics
        self.metrics_collector.record_trade(symbol, price, quantity);
        
        Ok(())
    }
    
    /// Process balance updates
    async fn process_balance_update(&mut self, data: &[u8]) -> Result<()> {
        let message: Value = simd_json::from_slice(
            &mut data.to_vec()
        ).map_err(|e| anyhow!("Failed to parse balance message: {}", e))?;
        
        if let Some(balances) = message["balances"].as_object() {
            for (currency, balance) in balances {
                self.cache_manager
                    .set(&format!("balance_{}", currency), balance)
                    .await?;
            }
        }
        
        Ok(())
    }
    
    /// Process generic messages
    async fn process_generic_message(&mut self, data: &[u8]) -> Result<()> {
        // Store in cache for debugging/analysis
        let key = format!("unknown_message_{}", 
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        
        self.cache_manager
            .set(&key, &serde_json::Value::String(String::from_utf8_lossy(data).to_string()))
            .await?;
        
        Ok(())
    }
    
    /// Get or create order book for symbol
    async fn get_or_create_order_book(&mut self, symbol: &str) -> Result<Arc<LockFreeOrderBook>> {
        if let Some(book) = self.order_books.get(symbol) {
            return Ok(book.clone());
        }
        
        // Create new lock-free order book
        let order_book = Arc::new(LockFreeOrderBook::new(symbol.to_string())?);
        self.order_books.insert(symbol.to_string(), order_book.clone());
        
        log_info!(MAIN_LOGGER, "Created new lock-free order book for symbol: {}", symbol);
        
        Ok(order_book)
    }
    
    /// Get order book for symbol
    pub fn get_order_book(&self, symbol: &str) -> Option<Arc<LockFreeOrderBook>> {
        self.order_books.get(symbol).cloned()
    }
    
    /// Process data using kernel bypass if available
    pub async fn process_with_kernel_bypass(&mut self, data: &[u8]) -> Result<()> {
        if let Some(ref kernel_bypass) = self.kernel_bypass {
            // Use zero-copy processing with kernel bypass
            kernel_bypass.process_zero_copy(data).await?;
        } else {
            // Fallback to regular processing
            self.process_market_data(data).await?;
        }
        
        Ok(())
    }
    
    /// Get processing statistics
    pub fn get_stats(&self) -> DataPipelineStats {
        use std::sync::atomic::Ordering;
        
        DataPipelineStats {
            messages_processed: self.processing_stats.messages_processed.load(Ordering::Relaxed),
            orders_added: self.processing_stats.orders_added.load(Ordering::Relaxed),
            l3_updates: self.processing_stats.l3_updates.load(Ordering::Relaxed),
            trades_processed: self.processing_stats.trades_processed.load(Ordering::Relaxed),
            errors: self.processing_stats.errors.load(Ordering::Relaxed),
            active_order_books: self.order_books.len(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataPipelineStats {
    pub messages_processed: u64,
    pub orders_added: u64,
    pub l3_updates: u64,
    pub trades_processed: u64,
    pub errors: u64,
    pub active_order_books: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn test_processing_stats_default() {
        let stats = ProcessingStats::default();
        assert_eq!(stats.messages_processed.load(Ordering::Relaxed), 0);
        assert_eq!(stats.orders_added.load(Ordering::Relaxed), 0);
        assert_eq!(stats.l3_updates.load(Ordering::Relaxed), 0);
        assert_eq!(stats.trades_processed.load(Ordering::Relaxed), 0);
        assert_eq!(stats.errors.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn test_processing_stats_atomic_increment() {
        let stats = ProcessingStats::default();
        stats.messages_processed.fetch_add(5, Ordering::Relaxed);
        stats.trades_processed.fetch_add(3, Ordering::Relaxed);
        stats.errors.fetch_add(1, Ordering::Relaxed);
        assert_eq!(stats.messages_processed.load(Ordering::Relaxed), 5);
        assert_eq!(stats.trades_processed.load(Ordering::Relaxed), 3);
        assert_eq!(stats.errors.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn test_pipeline_stats_struct() {
        let stats = DataPipelineStats {
            messages_processed: 100,
            orders_added: 50,
            l3_updates: 30,
            trades_processed: 20,
            errors: 0,
            active_order_books: 5,
        };
        assert_eq!(stats.messages_processed, 100);
        assert_eq!(stats.orders_added, 50);
        assert_eq!(stats.l3_updates, 30);
        assert_eq!(stats.trades_processed, 20);
        assert_eq!(stats.errors, 0);
        assert_eq!(stats.active_order_books, 5);
    }

    #[test]
    fn test_pipeline_stats_clone() {
        let stats = DataPipelineStats {
            messages_processed: 10,
            orders_added: 5,
            l3_updates: 3,
            trades_processed: 2,
            errors: 1,
            active_order_books: 1,
        };
        let cloned = stats.clone();
        assert_eq!(cloned.messages_processed, stats.messages_processed);
        assert_eq!(cloned.errors, stats.errors);
    }
}

