use std::{
    sync::Arc,
    collections::VecDeque,
    alloc::{alloc, dealloc, Layout},
    ptr::NonNull,
};
use crossbeam::queue::SegQueue;
use parking_lot::Mutex;

/// Ultra-high performance zero-copy message buffer pool
/// Preallocates aligned memory buffers to eliminate allocations 
/// in critical trading message processing paths.
pub struct ZeroCopyBufferPool {
    free_buffers: SegQueue<NonNull<u8>>,
    buffer_size: usize,
    buffer_count: usize,
    layout: Layout,
}

unsafe impl Send for ZeroCopyBufferPool {}
unsafe impl Sync for ZeroCopyBufferPool {}

impl ZeroCopyBufferPool {
    /// Create a new buffer pool with preallocated memory
    /// buffer_size: Size of each buffer in bytes (should be power of 2)
    /// buffer_count: Number of buffers to preallocate
    pub fn new(buffer_size: usize, buffer_count: usize) -> Result<Self, Box<dyn std::error::Error>> {
        // Ensure buffer size is aligned to cache line boundary (64 bytes)
        let aligned_size = (buffer_size + 63) & !63;
        
        let layout = Layout::from_size_align(aligned_size, 64)
            .map_err(|e| format!("Invalid buffer layout: {}", e))?;

        let free_buffers = SegQueue::new();

        // Preallocate all buffers
        unsafe {
            for _ in 0..buffer_count {
                let ptr = alloc(layout);
                if ptr.is_null() {
                    return Err("Failed to allocate buffer memory".into());
                }
                
                // Zero-initialize for security
                std::ptr::write_bytes(ptr, 0, aligned_size);
                
                free_buffers.push(NonNull::new_unchecked(ptr));
            }
        }

        crate::perf_log_info!(
            "Created zero-copy buffer pool: {} buffers of {} bytes each ({}KB total)",
            buffer_count,
            aligned_size,
            (buffer_count * aligned_size) / 1024
        );

        Ok(Self {
            free_buffers,
            buffer_size: aligned_size,
            buffer_count,
            layout,
        })
    }

    /// Get a buffer from the pool (zero-copy, lock-free)
    pub fn get_buffer(&self) -> Option<ZeroCopyBuffer> {
        if let Some(ptr) = self.free_buffers.pop() {
            Some(ZeroCopyBuffer {
                ptr,
                size: self.buffer_size,
                pool: self,
            })
        } else {
            // Pool exhausted - should be rare in properly sized system
            crate::perf_log_warn!("Zero-copy buffer pool exhausted");
            None
        }
    }

    /// Return buffer to pool (called automatically on drop)
    fn return_buffer(&self, ptr: NonNull<u8>) {
        // Zero the buffer for security before returning to pool
        unsafe {
            std::ptr::write_bytes(ptr.as_ptr(), 0, self.buffer_size);
        }
        
        self.free_buffers.push(ptr);
    }

    pub fn available_buffers(&self) -> usize {
        self.free_buffers.len()
    }

    pub fn total_buffers(&self) -> usize {
        self.buffer_count
    }
}

impl Drop for ZeroCopyBufferPool {
    fn drop(&mut self) {
        // Deallocate all buffers
        while let Some(ptr) = self.free_buffers.pop() {
            unsafe {
                dealloc(ptr.as_ptr(), self.layout);
            }
        }
    }
}

/// RAII wrapper for zero-copy buffer that automatically returns to pool
pub struct ZeroCopyBuffer<'a> {
    ptr: NonNull<u8>,
    size: usize,
    pool: &'a ZeroCopyBufferPool,
}

impl<'a> ZeroCopyBuffer<'a> {
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.size)
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.ptr.as_ptr(), self.size)
        }
    }

    pub fn capacity(&self) -> usize {
        self.size
    }

    /// Write data to buffer with bounds checking
    pub fn write(&mut self, data: &[u8]) -> Result<usize, &'static str> {
        if data.len() > self.size {
            return Err("Data exceeds buffer capacity");
        }

        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), self.ptr.as_ptr(), data.len());
        }

        Ok(data.len())
    }

    /// Zero-copy view of buffer data
    pub fn as_str(&self, len: usize) -> Result<&str, std::str::Utf8Error> {
        if len > self.size {
            return Ok("");
        }

        let slice = unsafe {
            std::slice::from_raw_parts(self.ptr.as_ptr(), len)
        };

        std::str::from_utf8(slice)
    }
}

impl<'a> Drop for ZeroCopyBuffer<'a> {
    fn drop(&mut self) {
        self.pool.return_buffer(self.ptr);
    }
}

/// Lock-free message queue optimized for trading systems
pub struct HighPerformanceMessageQueue<T> {
    queue: SegQueue<T>,
    capacity: usize,
    dropped_messages: std::sync::atomic::AtomicUsize,
}

impl<T> HighPerformanceMessageQueue<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            queue: SegQueue::new(),
            capacity,
            dropped_messages: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    /// Push message with overflow handling
    pub fn push(&self, item: T) -> Result<(), T> {
        if self.queue.len() >= self.capacity {
            self.dropped_messages.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            return Err(item);
        }

        self.queue.push(item);
        Ok(())
    }

    /// Pop message (lock-free)
    pub fn pop(&self) -> Option<T> {
        self.queue.pop()
    }

    pub fn len(&self) -> usize {
        self.queue.len()
    }

    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub fn dropped_count(&self) -> usize {
        self.dropped_messages.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_creation() {
        let pool = ZeroCopyBufferPool::new(1024, 10).unwrap();
        assert_eq!(pool.available_buffers(), 10);
        assert_eq!(pool.total_buffers(), 10);
    }

    #[test]
    fn test_buffer_get_return() {
        let pool = ZeroCopyBufferPool::new(1024, 5).unwrap();
        
        let buffer = pool.get_buffer().unwrap();
        assert_eq!(pool.available_buffers(), 4);
        
        drop(buffer);
        assert_eq!(pool.available_buffers(), 5);
    }

    #[test]
    fn test_buffer_write_read() {
        let pool = ZeroCopyBufferPool::new(1024, 1).unwrap();
        let mut buffer = pool.get_buffer().unwrap();
        
        let test_data = b"Hello, high-frequency trading!";
        assert!(buffer.write(test_data).is_ok());
        
        let read_str = buffer.as_str(test_data.len()).unwrap();
        assert_eq!(read_str, "Hello, high-frequency trading!");
    }
}
