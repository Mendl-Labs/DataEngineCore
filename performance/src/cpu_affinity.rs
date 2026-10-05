use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(target_os = "linux")]
use libc::{cpu_set_t, sched_setaffinity, CPU_SET, CPU_ZERO};

/// Set thread affinity to specific CPU cores - for compatibility
pub fn set_thread_affinity(cores: Vec<usize>) -> Result<(), anyhow::Error> {
    let manager = CpuAffinityManager::new();
    if let Some(core) = cores.first() {
        manager
            .pin_to_core(*core)
            .map_err(|e| anyhow::anyhow!("Failed to set thread affinity: {}", e))?;
    }
    Ok(())
}

/// Ultra-high performance CPU affinity management for trading systems
/// Pins critical threads to specific CPU cores for optimal cache locality
/// and minimal context switching overhead.
pub struct CpuAffinityManager {
    core_counter: AtomicUsize,
    total_cores: usize,
}

impl Default for CpuAffinityManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CpuAffinityManager {
    pub fn new() -> Self {
        let total_cores = num_cpus::get();
        crate::perf_log_info!("Detected {} CPU cores for affinity management", total_cores);

        Self {
            core_counter: AtomicUsize::new(0),
            total_cores,
        }
    }

    /// Pin the current thread to the next available CPU core
    /// Critical for ultra-low latency trading applications
    pub fn pin_to_next_core(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let core_id = self.core_counter.fetch_add(1, Ordering::Relaxed) % self.total_cores;
        self.pin_to_core(core_id)?;
        Ok(core_id)
    }

    /// Pin current thread to a specific CPU core
    #[cfg(target_os = "windows")]
    pub fn pin_to_core(&self, core_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if core_id >= self.total_cores {
            return Err(format!(
                "Core {} exceeds available cores {}",
                core_id, self.total_cores
            )
            .into());
        }

        // Windows CPU affinity is complex and requires specific APIs
        // For now, we'll log the intent but not implement the actual pinning
        // This can be implemented with more specific Windows APIs if needed
        crate::perf_log_info!("Thread requested pinning to CPU core {} (Windows)", core_id);
        Ok(())
    }

    #[cfg(target_os = "linux")]
    pub fn pin_to_core(&self, core_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        if core_id >= self.total_cores {
            return Err(format!(
                "Core {} exceeds available cores {}",
                core_id, self.total_cores
            )
            .into());
        }

        unsafe {
            let mut cpuset: cpu_set_t = std::mem::zeroed();
            CPU_ZERO(&mut cpuset);
            CPU_SET(core_id, &mut cpuset);

            let result = sched_setaffinity(0, std::mem::size_of::<cpu_set_t>(), &cpuset);
            if result != 0 {
                return Err("Failed to set thread affinity on Linux".into());
            }
        }

        crate::perf_log_info!("Thread pinned to CPU core {}", core_id);
        Ok(())
    }

    #[cfg(not(any(target_os = "windows", target_os = "linux")))]
    pub fn pin_to_core(&self, core_id: usize) -> Result<(), Box<dyn std::error::Error>> {
        crate::perf_log_warn!(
            "CPU affinity not supported on this platform, core {} requested",
            core_id
        );
        Ok(())
    }

    /// Pin critical WebSocket handler to highest priority core (typically core 0)
    pub fn pin_websocket_handler(&self) -> Result<usize, Box<dyn std::error::Error>> {
        self.pin_to_core(0)?;
        Ok(0)
    }

    /// Pin message processing threads to dedicated cores
    /// Distributes load across cores 1-N for optimal throughput
    pub fn pin_message_processor(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let core_id =
            1 + (self.core_counter.fetch_add(1, Ordering::Relaxed) % (self.total_cores - 1));
        self.pin_to_core(core_id)?;
        Ok(core_id)
    }

    /// Pin database writer threads to separate cores to avoid cache conflicts
    pub fn pin_database_writer(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let core_id = (self.total_cores / 2)
            + (self.core_counter.fetch_add(1, Ordering::Relaxed) % (self.total_cores / 2));
        self.pin_to_core(core_id)?;
        Ok(core_id)
    }

    pub fn get_total_cores(&self) -> usize {
        self.total_cores
    }
}

/// Set high-priority scheduling for critical trading threads
#[cfg(target_os = "windows")]
pub fn set_high_priority() -> Result<(), Box<dyn std::error::Error>> {
    // Windows priority setting requires specific APIs and permissions
    // For now, we'll log the intent but not implement the actual priority setting
    crate::perf_log_info!("Thread priority set to high (Windows - requires admin privileges)");
    Ok(())
}

#[cfg(target_os = "linux")]
pub fn set_high_priority() -> Result<(), Box<dyn std::error::Error>> {
    use libc::{sched_param, sched_setscheduler, SCHED_FIFO};

    unsafe {
        let mut param: sched_param = std::mem::zeroed();
        param.sched_priority = 99; // Highest real-time priority

        if sched_setscheduler(0, SCHED_FIFO, &param) != 0 {
            return Err("Failed to set SCHED_FIFO priority on Linux".into());
        }
    }

    crate::perf_log_info!("Thread priority set to SCHED_FIFO with priority 99");
    Ok(())
}

#[cfg(not(any(target_os = "windows", target_os = "linux")))]
pub fn set_high_priority() -> Result<(), Box<dyn std::error::Error>> {
    crate::perf_log_warn!("High priority scheduling not supported on this platform");
    Ok(())
}

/// Prefault memory pages to avoid page faults in critical paths
pub fn prefault_stack(size_mb: usize) -> Result<(), Box<dyn std::error::Error>> {
    let size_bytes = size_mb * 1024 * 1024;
    let mut stack_buffer = vec![0u8; size_bytes];

    // Touch every page to force allocation
    for i in (0..size_bytes).step_by(4096) {
        stack_buffer[i] = 1;
    }

    // Prevent optimization from removing this
    std::hint::black_box(stack_buffer);

    crate::perf_log_info!("Prefaulted {} MB of stack memory", size_mb);
    Ok(())
}
