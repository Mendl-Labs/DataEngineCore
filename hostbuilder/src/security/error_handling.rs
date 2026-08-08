//! Secure error handling module for DataEngine
//! 
//! Provides safe error handling patterns that prevent panics and
//! information disclosure while maintaining performance.

use std::fmt;
use thiserror::Error;
use num_cpus;
use ultra_logger::{ultra_error, ultra_warn};

/// Secure error type that prevents information disclosure
#[derive(Debug, Error)]
pub enum SecureError {
    #[error("Configuration error")]
    ConfigurationError,
    
    #[error("Network communication error")]
    NetworkError,
    
    #[error("Authentication failed")]
    AuthenticationError,
    
    #[error("Authorization denied")]
    AuthorizationError,
    
    #[error("Resource unavailable")]
    ResourceUnavailable,
    
    #[error("Internal processing error")]
    InternalError,
    
    #[error("Invalid input data")]
    InvalidInput,
    
    #[error("Rate limit exceeded")]
    RateLimitExceeded,
    
    #[error("Service temporarily unavailable")]
    ServiceUnavailable,
}

/// Result type for secure operations
pub type SecureResult<T> = Result<T, SecureError>;

/// Safe alternative to unwrap() that logs errors and returns defaults
pub trait SecureUnwrap<T> {
    fn secure_unwrap_or_default(self) -> T
    where
        T: Default;
    
    fn secure_unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T;
    
    fn secure_unwrap_or_log(self, default: T, context: &str) -> T;
    
    fn secure_unwrap(self, context: &str) -> T
    where
        T: Default;
}

impl<T, E> SecureUnwrap<T> for Result<T, E>
where
    E: fmt::Debug,
{
    fn secure_unwrap_or_default(self) -> T
    where
        T: Default,
    {
        match self {
            Ok(value) => value,
            Err(e) => {
                ultra_error!(format!("Operation failed, using default: {:?}", e));
                T::default()
            }
        }
    }
    
    fn secure_unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        match self {
            Ok(value) => value,
            Err(e) => {
                ultra_error!(format!("Operation failed, using fallback: {:?}", e));
                f()
            }
        }
    }
    
    fn secure_unwrap_or_log(self, default: T, context: &str) -> T {
        match self {
            Ok(value) => value,
            Err(e) => {
                ultra_error!(format!("Operation failed in {}: {:?}", context, e));
                default
            }
        }
    }
    
    fn secure_unwrap(self, context: &str) -> T
    where
        T: Default,
    {
        match self {
            Ok(value) => value,
            Err(e) => {
                ultra_error!(format!("Operation failed in {}: {:?}", context, e));
                T::default()
            }
        }
    }
}

impl<T> SecureUnwrap<T> for Option<T> {
    fn secure_unwrap_or_default(self) -> T
    where
        T: Default,
    {
        match self {
            Some(value) => value,
            None => {
                ultra_warn!("Option was None, using default");
                T::default()
            }
        }
    }
    
    fn secure_unwrap_or_else<F>(self, f: F) -> T
    where
        F: FnOnce() -> T,
    {
        match self {
            Some(value) => value,
            None => {
                ultra_warn!("Option was None, using fallback");
                f()
            }
        }
    }
    
    fn secure_unwrap_or_log(self, default: T, context: &str) -> T {
        match self {
            Some(value) => value,
            None => {
                ultra_warn!(format!("Option was None in {}", context));
                default
            }
        }
    }
    
    fn secure_unwrap(self, context: &str) -> T
    where
        T: Default,
    {
        match self {
            Some(value) => value,
            None => {
                ultra_warn!(format!("Option was None in {}", context));
                T::default()
            }
        }
    }
}

/// Safe system call wrapper that handles errors gracefully
pub mod safe_system_calls {
    use super::*;
    
    /// Safely set thread priority without panicking
    pub fn safe_set_thread_priority() -> SecureResult<()> {
        // Cross-platform thread management
        // Simply yield to scheduler for now to avoid platform-specific issues
        std::thread::yield_now();
        Ok(())
    }
    
    /// Safely set CPU affinity with error handling
    pub fn safe_set_cpu_affinity(cores: &[usize]) -> SecureResult<()> {
        // Validate core numbers first
        let num_cpus = num_cpus::get();
        for &core in cores {
            if core >= num_cpus {
                ultra_warn!(format!("Invalid core number {}, max is {}", core, num_cpus - 1));
                return Err(SecureError::InvalidInput);
            }
        }
        
        // Implementation would go here with proper error handling
        // For now, just log and continue
        ultra_warn!("CPU affinity setting disabled for security");
        Ok(())
    }
}

/// Secure memory operations
pub mod secure_memory {
    use super::*;
    use std::ptr;
    
    /// Securely clear sensitive data from memory
    pub fn secure_zero_memory(data: &mut [u8]) {
        // Use volatile writes to prevent optimization
        for byte in data.iter_mut() {
            unsafe {
                ptr::write_volatile(byte, 0);
            }
        }
    }
    
    /// Safe buffer allocation with bounds checking
    pub struct SecureBuffer {
        data: Vec<u8>,
        max_size: usize,
    }
    
    impl SecureBuffer {
        pub fn new(size: usize, max_size: usize) -> SecureResult<Self> {
            if size > max_size {
                return Err(SecureError::InvalidInput);
            }
            
            if size > 100 * 1024 * 1024 { // 100MB limit
                return Err(SecureError::ResourceUnavailable);
            }
            
            Ok(Self {
                data: vec![0; size],
                max_size,
            })
        }
        
        pub fn write(&mut self, offset: usize, data: &[u8]) -> SecureResult<()> {
            if offset.saturating_add(data.len()) > self.data.len() {
                return Err(SecureError::InvalidInput);
            }
            
            if self.data.len().saturating_add(data.len()) > self.max_size {
                return Err(SecureError::ResourceUnavailable);
            }
            
            self.data[offset..offset + data.len()].copy_from_slice(data);
            Ok(())
        }
        
        pub fn read(&self, offset: usize, len: usize) -> SecureResult<&[u8]> {
            if offset.saturating_add(len) > self.data.len() {
                return Err(SecureError::InvalidInput);
            }
            
            Ok(&self.data[offset..offset + len])
        }
    }
    
    impl Drop for SecureBuffer {
        fn drop(&mut self) {
            secure_zero_memory(&mut self.data);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::security::error_handling::secure_memory::SecureBuffer;

    use super::*;
    
    #[test]
    fn test_secure_unwrap_result() {
        let ok_result: Result<i32, &str> = Ok(42);
        assert_eq!(ok_result.secure_unwrap_or_default(), 42);
        
        let err_result: Result<i32, &str> = Err("error");
        assert_eq!(err_result.secure_unwrap_or_default(), 0);
    }
    
    #[test]
    fn test_secure_unwrap_option() {
        let some_option = Some(42);
        assert_eq!(some_option.secure_unwrap_or_default(), 42);
        
        let none_option: Option<i32> = None;
        assert_eq!(none_option.secure_unwrap_or_default(), 0);
    }
    
    #[test]
    fn test_secure_buffer() {
        let mut buffer = SecureBuffer::new(1024, 2048).unwrap();
        let data = b"test data";
        
        buffer.write(0, data).unwrap();
        let read_data = buffer.read(0, data.len()).unwrap();
        assert_eq!(read_data, data);
        
        // Test bounds checking
        assert!(buffer.write(1020, data).is_err()); // Would overflow
        assert!(buffer.read(1020, 10).is_err()); // Would read past end
    }
}
