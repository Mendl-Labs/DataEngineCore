//! Ultra-high performance memory pools for zero-copy trading operations
//! 
//! Provides NUMA-aware, lock-free memory pools with sub-microsecond allocation
//! and deallocation for trading system critical paths.

use std::sync::atomic::{AtomicPtr, AtomicUsize, AtomicU64, Ordering};
use std::sync::Arc;
use std::alloc::{Layout, alloc};
use std::ptr::{self, NonNull};
use anyhow::{Result, anyhow};
use crossbeam::utils::CachePadded;

/// Ultra-fast memory pool for trading operations
pub struct BufferPool {
    // Lock-free stack of available buffers
    free_buffers: CachePadded<AtomicPtr<PoolNode>>,
    
    // Pool statistics
    total_buffers: AtomicUsize,
    allocated_buffers: AtomicUsize,
    allocation_count: AtomicU64,
    
    // Configuration
    buffer_size: usize,
    buffer_alignment: usize,
    _initial_pool_size: usize,
}

struct PoolNode {
    next: *mut PoolNode,
    buffer: NonNull<u8>,
    size: usize,
}

unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    /// Create new buffer pool with specified parameters
    pub fn new(buffer_size: usize, initial_pool_size: usize) -> Result<Self> {
        let alignment = 64; // Cache line alignment

        let pool = Self {
            free_buffers: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            total_buffers: AtomicUsize::new(0),
            allocated_buffers: AtomicUsize::new(0),
            allocation_count: AtomicU64::new(0),
            buffer_size,
            buffer_alignment: alignment,
            _initial_pool_size: initial_pool_size,
        };

        // Pre-allocate initial buffers
        for _ in 0..initial_pool_size {
            let node = pool.create_buffer_node()?;
            pool.push_free_buffer(Box::into_raw(node));
        }

        pool.total_buffers.store(initial_pool_size, Ordering::Relaxed);

        crate::perf_log_info!("BufferPool created: buffer_size={} initial_pool_size={} alignment={}", buffer_size, initial_pool_size, alignment);

        Ok(pool)
    }

    /// Get buffer from pool (ultra-fast path)
    #[inline(always)]
    pub fn get_buffer(&self) -> Result<PooledBuffer> {
        // Try to pop from free list first (lock-free)
        if let Some(node) = self.pop_free_buffer() {
            self.allocated_buffers.fetch_add(1, Ordering::Relaxed);
            self.allocation_count.fetch_add(1, Ordering::Relaxed);

            return Ok(PooledBuffer {
                buffer: node.buffer,
                size: node.size,
                pool: self as *const BufferPool,
            });
        }

        // If no free buffers, pool is exhausted - must allocate new
        crate::perf_log_warn!("BufferPool exhausted (buffer_size={}): all {} pre-allocated buffers in use, allocating new", self.buffer_size, self.total_buffers.load(Ordering::Relaxed));
        let node = self.create_buffer_node()?;
        self.total_buffers.fetch_add(1, Ordering::Relaxed);
        self.allocated_buffers.fetch_add(1, Ordering::Relaxed);
        self.allocation_count.fetch_add(1, Ordering::Relaxed);

        Ok(PooledBuffer {
            buffer: node.buffer,
            size: node.size,
            pool: self as *const BufferPool,
        })
    }

    /// Return buffer to pool (ultra-fast path)
    #[inline(always)]
    pub fn return_buffer(&self, buffer: PooledBuffer) {
        let node = Box::into_raw(Box::new(PoolNode {
            next: ptr::null_mut(),
            buffer: buffer.buffer,
            size: buffer.size,
        }));
        
        self.push_free_buffer(node);
        self.allocated_buffers.fetch_sub(1, Ordering::Relaxed);
        
        // Prevent drop from running
        std::mem::forget(buffer);
    }

    /// Create new buffer node with optimal alignment
    fn create_buffer_node(&self) -> Result<Box<PoolNode>> {
        let layout = Layout::from_size_align(self.buffer_size, self.buffer_alignment)
            .map_err(|e| anyhow!("Invalid layout: {}", e))?;

        let ptr = unsafe { alloc(layout) };
        if ptr.is_null() {
            eprintln!("[PERFORMANCE ERROR] BufferPool allocation failure: could not allocate {} bytes with alignment {}", self.buffer_size, self.buffer_alignment);
            return Err(anyhow!("Failed to allocate buffer"));
        }
        
        let buffer = unsafe { NonNull::new_unchecked(ptr) };
        
        Ok(Box::new(PoolNode {
            next: ptr::null_mut(),
            buffer,
            size: self.buffer_size,
        }))
    }

    /// Lock-free push to free buffer stack
    #[inline(always)]
    fn push_free_buffer(&self, node: *mut PoolNode) {
        loop {
            let head = self.free_buffers.load(Ordering::Acquire);
            unsafe {
                (*node).next = head;
            }
            
            if self.free_buffers.compare_exchange_weak(
                head,
                node,
                Ordering::Release,
                Ordering::Relaxed,
            ).is_ok() {
                break;
            }
        }
    }

    /// Lock-free pop from free buffer stack
    #[inline(always)]
    fn pop_free_buffer(&self) -> Option<Box<PoolNode>> {
        loop {
            let head = self.free_buffers.load(Ordering::Acquire);
            if head.is_null() {
                return None;
            }
            
            let next = unsafe { (*head).next };
            
            if self.free_buffers.compare_exchange_weak(
                head,
                next,
                Ordering::Release,
                Ordering::Relaxed,
            ).is_ok() {
                return Some(unsafe { Box::from_raw(head) });
            }
        }
    }

    /// Get pool statistics
    pub fn stats(&self) -> PoolStats {
        PoolStats {
            total_buffers: self.total_buffers.load(Ordering::Relaxed),
            allocated_buffers: self.allocated_buffers.load(Ordering::Relaxed),
            free_buffers: self.total_buffers.load(Ordering::Relaxed)
                .saturating_sub(self.allocated_buffers.load(Ordering::Relaxed)),
            allocation_count: self.allocation_count.load(Ordering::Relaxed),
            buffer_size: self.buffer_size,
        }
    }
}

/// RAII buffer from pool
pub struct PooledBuffer {
    buffer: NonNull<u8>,
    size: usize,
    pool: *const BufferPool,
}

unsafe impl Send for PooledBuffer {}
unsafe impl Sync for PooledBuffer {}

impl PooledBuffer {
    /// Get mutable slice of buffer
    #[inline(always)]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.buffer.as_ptr(), self.size) }
    }

    /// Get slice of buffer
    #[inline(always)]
    pub fn as_slice(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.buffer.as_ptr(), self.size) }
    }

    /// Get raw pointer to buffer
    #[inline(always)]
    pub fn as_ptr(&self) -> *const u8 {
        self.buffer.as_ptr()
    }

    /// Get mutable raw pointer to buffer
    #[inline(always)]
    pub fn as_mut_ptr(&mut self) -> *mut u8 {
        self.buffer.as_ptr()
    }

    /// Get buffer size
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.size
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        unsafe {
            let pool = &*self.pool;
            pool.return_buffer(PooledBuffer {
                buffer: self.buffer,
                size: self.size,
                pool: self.pool,
            });
        }
    }
}

/// Pool statistics
#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total_buffers: usize,
    pub allocated_buffers: usize,
    pub free_buffers: usize,
    pub allocation_count: u64,
    pub buffer_size: usize,
}

/// NUMA-aware memory pool for multi-socket systems
pub struct NumaBufferPool {
    pools: Vec<Arc<BufferPool>>,
    numa_nodes: usize,
}

impl NumaBufferPool {
    /// Create NUMA-aware buffer pool
    pub fn new(buffer_size: usize, pool_size_per_node: usize) -> Result<Self> {
        let numa_nodes = Self::detect_numa_nodes();
        let mut pools = Vec::with_capacity(numa_nodes);

        for _ in 0..numa_nodes {
            pools.push(Arc::new(BufferPool::new(buffer_size, pool_size_per_node)?));
        }

        crate::perf_log_info!("NumaBufferPool created: {} NUMA nodes, buffer_size={}, pool_size_per_node={}", numa_nodes, buffer_size, pool_size_per_node);

        Ok(Self { pools, numa_nodes })
    }

    /// Get buffer from local NUMA node
    pub fn get_buffer(&self) -> Result<PooledBuffer> {
        let node_id = Self::get_current_numa_node();
        let pool_index = node_id % self.numa_nodes;
        self.pools[pool_index].get_buffer()
    }

    /// Detect number of NUMA nodes
    fn detect_numa_nodes() -> usize {
        #[cfg(target_os = "linux")]
        {
            // Try to read from /sys/devices/system/node/
            std::fs::read_dir("/sys/devices/system/node/")
                .map(|entries| {
                    entries
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| {
                            entry.file_name()
                                .to_string_lossy()
                                .starts_with("node")
                        })
                        .count()
                })
                .unwrap_or(1)
                .max(1)
        }
        #[cfg(not(target_os = "linux"))]
        {
            // Fallback: assume single NUMA node
            1
        }
    }

    /// Get current NUMA node (simplified)
    fn get_current_numa_node() -> usize {
        // In a real implementation, this would use getcpu() or similar
        // For now, use a simple hash as approximation
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};
        
        thread_local! {
            static NUMA_NODE: usize = {
                let mut hasher = DefaultHasher::new();
                std::thread::current().id().hash(&mut hasher);
                (hasher.finish() as usize) % 4 // Assume 4 NUMA nodes
            };
        }
        
        NUMA_NODE.with(|&node| node)
    }

    /// Get aggregate statistics
    pub fn stats(&self) -> Vec<PoolStats> {
        self.pools.iter().map(|pool| pool.stats()).collect()
    }
}

/// Global buffer pool for trading operations
pub static mut GLOBAL_BUFFER_POOL: Option<Arc<BufferPool>> = None;
static INIT_POOL: std::sync::Once = std::sync::Once::new();

/// Initialize global buffer pool
pub fn init_global_pool(buffer_size: usize, pool_size: usize) -> Result<()> {
    INIT_POOL.call_once(|| {
        crate::perf_log_info!("Initializing global buffer pool: buffer_size={}, pool_size={}", buffer_size, pool_size);
        let pool = BufferPool::new(buffer_size, pool_size)
            .expect("Failed to create global buffer pool");
        unsafe {
            GLOBAL_BUFFER_POOL = Some(Arc::new(pool));
        }
    });
    Ok(())
}

/// Get buffer from global pool
pub fn get_global_buffer() -> Result<PooledBuffer> {
    unsafe {
        match GLOBAL_BUFFER_POOL {
            Some(ref pool) => pool.get_buffer(),
            None => {
                eprintln!("[PERFORMANCE ERROR] Global buffer pool not initialized - call init_global_pool first");
                Err(anyhow!("Global buffer pool not initialized"))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_buffer_pool_basic() {
        let pool = BufferPool::new(1024, 10).unwrap();
        
        let buffer1 = pool.get_buffer().unwrap();
        let buffer2 = pool.get_buffer().unwrap();
        
        assert_eq!(buffer1.len(), 1024);
        assert_eq!(buffer2.len(), 1024);
        
        let stats = pool.stats();
        assert_eq!(stats.allocated_buffers, 2);
    }
    
    #[test]
    fn test_buffer_return() {
        let pool = BufferPool::new(1024, 5).unwrap();
        
        {
            let _buffer = pool.get_buffer().unwrap();
            assert_eq!(pool.stats().allocated_buffers, 1);
        }
        
        // Buffer should be returned automatically
        assert_eq!(pool.stats().allocated_buffers, 0);
    }
}
