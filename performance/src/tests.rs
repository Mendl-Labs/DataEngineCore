#[cfg(test)]
mod tests {
    use crate::performance::{
        cpu_affinity::CpuAffinityManager,
        metrics::PerformanceMetrics,
        zero_copy::{ZeroCopyBufferPool, HighPerformanceMessageQueue},
    };

    #[test]
    fn test_cpu_affinity_manager_creation() {
        let manager = CpuAffinityManager::new();
        assert!(manager.get_total_cores() > 0);
    }

    #[test]
    fn test_performance_metrics_creation() {
        let metrics = PerformanceMetrics::new();
        let stats = metrics.get_stats();
        assert_eq!(stats.messages_processed, 0);
        assert_eq!(stats.parse_errors, 0);
    }

    #[test] 
    fn test_performance_metrics_recording() {
        let metrics = PerformanceMetrics::new();
        let start = std::time::Instant::now();
        
        // Simulate some processing time
        std::thread::sleep(std::time::Duration::from_micros(100));
        
        metrics.record_message_processed(start);
        
        let stats = metrics.get_stats();
        assert_eq!(stats.messages_processed, 1);
        assert!(stats.avg_latency_ns > 0);
    }

    #[test]
    fn test_buffer_pool_creation() {
        let pool = ZeroCopyBufferPool::new(1024, 10).unwrap();
        assert_eq!(pool.available_buffers(), 10);
        assert_eq!(pool.total_buffers(), 10);
    }

    #[test]
    fn test_buffer_get_and_return() {
        let pool = ZeroCopyBufferPool::new(1024, 5).unwrap();
        
        let buffer = pool.get_buffer().unwrap();
        assert_eq!(pool.available_buffers(), 4);
        
        drop(buffer); // Should return to pool
        assert_eq!(pool.available_buffers(), 5);
    }

    #[test]
    fn test_high_performance_message_queue() {
        let queue = HighPerformanceMessageQueue::new(100);
        
        // Test push and pop
        assert!(queue.push("test_message".to_string()).is_ok());
        assert_eq!(queue.len(), 1);
        
        let message = queue.pop();
        assert_eq!(message, Some("test_message".to_string()));
        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_message_queue_overflow() {
        let queue = HighPerformanceMessageQueue::new(2);
        
        // Fill queue to capacity
        assert!(queue.push("msg1".to_string()).is_ok());
        assert!(queue.push("msg2".to_string()).is_ok());
        
        // Should reject overflow
        assert!(queue.push("msg3".to_string()).is_err());
        assert_eq!(queue.dropped_count(), 1);
    }
}
