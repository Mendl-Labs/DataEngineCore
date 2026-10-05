//! Ultra-fast lock-free order book for sub-microsecond trading
//!
//! Implements a high-performance, lock-free order book using advanced concurrent
//! data structures and RCU-style updates for minimal latency operations.

use crate::infrastructure::logging_facade::ORDERBOOK_LOGGER;
use crate::{log_debug, log_error, log_info};
use anyhow::{anyhow, Result};
use crossbeam_utils::CachePadded;
use std::alloc::{alloc, Layout};
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

/// Cache-aligned price level for lock-free operations
#[repr(align(64))] // Cache line alignment
#[derive(Debug)]
struct PriceLevel {
    price: i64,                        // Price in fixed-point format (scaled by 10000)
    total_quantity: AtomicU64,         // Total quantity at this price level
    order_count: AtomicUsize,          // Number of orders at this level
    first_order: AtomicPtr<Order>,     // Head of order linked list
    last_order: AtomicPtr<Order>,      // Tail of order linked list
    next_level: AtomicPtr<PriceLevel>, // Next price level (for linked list)
    level_id: u64,                     // Unique level identifier
    timestamp: AtomicU64,              // Last update timestamp
}

/// Individual order in the book
#[repr(align(64))]
#[derive(Debug)]
struct Order {
    order_id: u64,
    price: i64,
    quantity: AtomicU64,
    side: OrderSide,
    timestamp: u64,
    next_order: AtomicPtr<Order>,
    prev_order: AtomicPtr<Order>,
    is_deleted: AtomicPtr<bool>, // Tombstone for RCU-style deletion
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderSide {
    Buy,
    Sell,
}

/// Lock-free order book implementation
pub struct LockFreeOrderBook {
    // Best bid/ask pointers for ultra-fast L1 access
    best_bid: CachePadded<AtomicPtr<PriceLevel>>,
    best_ask: CachePadded<AtomicPtr<PriceLevel>>,

    // Price level trees (separate for bids/asks)
    bid_levels: CachePadded<AtomicPtr<PriceLevel>>,
    ask_levels: CachePadded<AtomicPtr<PriceLevel>>,

    // Book statistics
    total_bid_quantity: CachePadded<AtomicU64>,
    total_ask_quantity: CachePadded<AtomicU64>,
    total_orders: CachePadded<AtomicUsize>,
    _last_trade_price: CachePadded<AtomicU64>,

    // Update sequence counter for lock-free reads
    sequence: CachePadded<AtomicU64>,

    // Symbol identifier
    _symbol: String,

    // Memory management
    level_allocator: LevelAllocator,
    order_allocator: OrderAllocator,

    // Performance counters
    operations_count: AtomicU64,
    l1_updates: AtomicU64,
    level_changes: AtomicU64,
}

impl LockFreeOrderBook {
    /// Create a new lock-free order book
    pub fn new(symbol: String) -> Result<Self> {
        log_info!(ORDERBOOK_LOGGER, "Creating lock-free order book for symbol '{}' (1024 levels, 10000 orders pre-allocated)", symbol);
        let level_allocator = LevelAllocator::new(1024)?; // Pre-allocate 1024 levels
        let order_allocator = OrderAllocator::new(10000)?; // Pre-allocate 10000 orders

        Ok(Self {
            best_bid: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            best_ask: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            bid_levels: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            ask_levels: CachePadded::new(AtomicPtr::new(ptr::null_mut())),
            total_bid_quantity: CachePadded::new(AtomicU64::new(0)),
            total_ask_quantity: CachePadded::new(AtomicU64::new(0)),
            total_orders: CachePadded::new(AtomicUsize::new(0)),
            _last_trade_price: CachePadded::new(AtomicU64::new(0)),
            sequence: CachePadded::new(AtomicU64::new(0)),
            _symbol: symbol,
            level_allocator,
            order_allocator,
            operations_count: AtomicU64::new(0),
            l1_updates: AtomicU64::new(0),
            level_changes: AtomicU64::new(0),
        })
    }

    /// Add order to the book (lock-free)
    pub fn add_order(
        &self,
        order_id: u64,
        price: f64,
        quantity: f64,
        side: OrderSide,
    ) -> Result<()> {
        let start_seq = self.sequence.fetch_add(1, Ordering::AcqRel);
        let price_fixed = (price * 10000.0) as i64;
        let quantity_fixed = (quantity * 10000.0) as u64;

        // Allocate new order
        let order = self.order_allocator.allocate()?;
        unsafe {
            (*order).order_id = order_id;
            (*order).price = price_fixed;
            (*order).quantity.store(quantity_fixed, Ordering::Relaxed);
            (*order).side = side;
            (*order).timestamp = hardware_timestamp();
            (*order)
                .next_order
                .store(ptr::null_mut(), Ordering::Relaxed);
            (*order)
                .prev_order
                .store(ptr::null_mut(), Ordering::Relaxed);
        }

        match side {
            OrderSide::Buy => self.add_bid_order(order, price_fixed, quantity_fixed)?,
            OrderSide::Sell => self.add_ask_order(order, price_fixed, quantity_fixed)?,
        }

        self.operations_count.fetch_add(1, Ordering::Relaxed);

        // Complete sequence update
        self.sequence
            .compare_exchange_weak(
                start_seq + 1,
                start_seq + 2,
                Ordering::AcqRel,
                Ordering::Relaxed,
            )
            .ok();

        Ok(())
    }

    /// Add bid order (lock-free insertion)
    fn add_bid_order(&self, order: *mut Order, price: i64, quantity: u64) -> Result<()> {
        loop {
            let current_best = self.best_bid.load(Ordering::Acquire);

            if current_best.is_null() {
                log_debug!(
                    ORDERBOOK_LOGGER,
                    "First bid level being created at price {}",
                    price as f64 / 10000.0
                );
            }

            // Find or create price level
            let level = self.find_or_create_bid_level(price)?;

            // Add order to price level
            unsafe {
                self.add_order_to_level(level, order)?;
            }

            // Update best bid if necessary
            if current_best.is_null() || unsafe { (*current_best).price < price } {
                match self.best_bid.compare_exchange_weak(
                    current_best,
                    level,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.l1_updates.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => continue, // Retry if CAS failed
                }
            } else {
                break;
            }
        }

        self.total_bid_quantity
            .fetch_add(quantity, Ordering::AcqRel);
        self.total_orders.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Add ask order (lock-free insertion)
    fn add_ask_order(&self, order: *mut Order, price: i64, quantity: u64) -> Result<()> {
        loop {
            let current_best = self.best_ask.load(Ordering::Acquire);

            if current_best.is_null() {
                log_debug!(
                    ORDERBOOK_LOGGER,
                    "First ask level being created at price {}",
                    price as f64 / 10000.0
                );
            }

            // Find or create price level
            let level = self.find_or_create_ask_level(price)?;

            // Add order to price level
            unsafe {
                self.add_order_to_level(level, order)?;
            }

            // Update best ask if necessary
            if current_best.is_null() || unsafe { (*current_best).price > price } {
                match self.best_ask.compare_exchange_weak(
                    current_best,
                    level,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => {
                        self.l1_updates.fetch_add(1, Ordering::Relaxed);
                        break;
                    }
                    Err(_) => continue,
                }
            } else {
                break;
            }
        }

        self.total_ask_quantity
            .fetch_add(quantity, Ordering::AcqRel);
        self.total_orders.fetch_add(1, Ordering::AcqRel);
        Ok(())
    }

    /// Find or create bid level (lock-free)
    fn find_or_create_bid_level(&self, price: i64) -> Result<*mut PriceLevel> {
        loop {
            let head = self.bid_levels.load(Ordering::Acquire);

            // Search for existing level
            if let Some(existing) = unsafe { self.find_price_level(head, price) } {
                return Ok(existing);
            }

            // Create new level
            let new_level = self.level_allocator.allocate()?;
            unsafe {
                (*new_level).price = price;
                (*new_level).total_quantity.store(0, Ordering::Relaxed);
                (*new_level).order_count.store(0, Ordering::Relaxed);
                (*new_level)
                    .first_order
                    .store(ptr::null_mut(), Ordering::Relaxed);
                (*new_level)
                    .last_order
                    .store(ptr::null_mut(), Ordering::Relaxed);
                (*new_level).level_id = hardware_timestamp();
                (*new_level)
                    .timestamp
                    .store(hardware_timestamp(), Ordering::Relaxed);
            }

            // Insert into sorted list (highest price first for bids)
            if self.insert_bid_level_sorted(new_level, price).is_ok() {
                return Ok(new_level);
            }

            // If insertion failed, deallocate and retry
            self.level_allocator.deallocate(new_level);
        }
    }

    /// Find or create ask level (lock-free)
    fn find_or_create_ask_level(&self, price: i64) -> Result<*mut PriceLevel> {
        loop {
            let head = self.ask_levels.load(Ordering::Acquire);

            // Search for existing level
            if let Some(existing) = unsafe { self.find_price_level(head, price) } {
                return Ok(existing);
            }

            // Create new level
            let new_level = self.level_allocator.allocate()?;
            unsafe {
                (*new_level).price = price;
                (*new_level).total_quantity.store(0, Ordering::Relaxed);
                (*new_level).order_count.store(0, Ordering::Relaxed);
                (*new_level)
                    .first_order
                    .store(ptr::null_mut(), Ordering::Relaxed);
                (*new_level)
                    .last_order
                    .store(ptr::null_mut(), Ordering::Relaxed);
                (*new_level).level_id = hardware_timestamp();
                (*new_level)
                    .timestamp
                    .store(hardware_timestamp(), Ordering::Relaxed);
            }

            // Insert into sorted list (lowest price first for asks)
            if self.insert_ask_level_sorted(new_level, price).is_ok() {
                return Ok(new_level);
            }

            self.level_allocator.deallocate(new_level);
        }
    }

    /// Insert bid level in sorted order (highest first)
    fn insert_bid_level_sorted(&self, new_level: *mut PriceLevel, price: i64) -> Result<()> {
        loop {
            let head = self.bid_levels.load(Ordering::Acquire);

            if head.is_null() || unsafe { (*head).price < price } {
                // Insert at head
                unsafe {
                    (*new_level).next_level.store(head, Ordering::Relaxed);
                }

                match self.bid_levels.compare_exchange_weak(
                    head,
                    new_level,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => continue,
                }
            } else {
                // Find insertion point
                let mut current = head;
                loop {
                    let next = unsafe { (*current).next_level.load(Ordering::Acquire) };
                    if next.is_null() || unsafe { (*next).price < price } {
                        // Insert between current and next
                        unsafe {
                            (*new_level).next_level.store(next, Ordering::Relaxed);
                        }

                        match unsafe {
                            (*current).next_level.compare_exchange_weak(
                                next,
                                new_level,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            )
                        } {
                            Ok(_) => return Ok(()),
                            Err(_) => break, // Retry from beginning
                        }
                    }
                    current = next;
                }
            }
        }
    }

    /// Insert ask level in sorted order (lowest first)
    fn insert_ask_level_sorted(&self, new_level: *mut PriceLevel, price: i64) -> Result<()> {
        loop {
            let head = self.ask_levels.load(Ordering::Acquire);

            if head.is_null() || unsafe { (*head).price > price } {
                // Insert at head
                unsafe {
                    (*new_level).next_level.store(head, Ordering::Relaxed);
                }

                match self.ask_levels.compare_exchange_weak(
                    head,
                    new_level,
                    Ordering::AcqRel,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => return Ok(()),
                    Err(_) => continue,
                }
            } else {
                // Find insertion point
                let mut current = head;
                loop {
                    let next = unsafe { (*current).next_level.load(Ordering::Acquire) };
                    if next.is_null() || unsafe { (*next).price > price } {
                        // Insert between current and next
                        unsafe {
                            (*new_level).next_level.store(next, Ordering::Relaxed);
                        }

                        match unsafe {
                            (*current).next_level.compare_exchange_weak(
                                next,
                                new_level,
                                Ordering::AcqRel,
                                Ordering::Relaxed,
                            )
                        } {
                            Ok(_) => return Ok(()),
                            Err(_) => break,
                        }
                    }
                    current = next;
                }
            }
        }
    }

    /// Find existing price level
    unsafe fn find_price_level(
        &self,
        head: *mut PriceLevel,
        price: i64,
    ) -> Option<*mut PriceLevel> {
        let mut current = head;
        while !current.is_null() {
            if (*current).price == price {
                return Some(current);
            }
            current = (*current).next_level.load(Ordering::Acquire);
        }
        None
    }

    /// Add order to price level (lock-free)
    unsafe fn add_order_to_level(&self, level: *mut PriceLevel, order: *mut Order) -> Result<()> {
        let quantity = (*order).quantity.load(Ordering::Relaxed);

        // Update level totals
        (*level)
            .total_quantity
            .fetch_add(quantity, Ordering::AcqRel);
        (*level).order_count.fetch_add(1, Ordering::AcqRel);
        (*level)
            .timestamp
            .store(hardware_timestamp(), Ordering::Relaxed);

        // Insert order at end of level's order list
        loop {
            let last_order = (*level).last_order.load(Ordering::Acquire);

            if last_order.is_null() {
                // First order in level
                (*order)
                    .prev_order
                    .store(ptr::null_mut(), Ordering::Relaxed);
                (*order)
                    .next_order
                    .store(ptr::null_mut(), Ordering::Relaxed);

                // Try to set as both first and last
                if (*level)
                    .first_order
                    .compare_exchange_weak(
                        ptr::null_mut(),
                        order,
                        Ordering::AcqRel,
                        Ordering::Relaxed,
                    )
                    .is_ok()
                {
                    (*level).last_order.store(order, Ordering::Release);
                    break;
                }
            } else {
                // Add to end
                (*order).prev_order.store(last_order, Ordering::Relaxed);
                (*order)
                    .next_order
                    .store(ptr::null_mut(), Ordering::Relaxed);

                if (*level)
                    .last_order
                    .compare_exchange_weak(last_order, order, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    (*last_order).next_order.store(order, Ordering::Release);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Get best bid/ask (ultra-fast L1 access)
    pub fn get_best_bid(&self) -> Option<BookLevel> {
        let level_ptr = self.best_bid.load(Ordering::Acquire);
        if level_ptr.is_null() {
            return None;
        }

        unsafe {
            Some(BookLevel {
                price: (*level_ptr).price as f64 / 10000.0,
                quantity: (*level_ptr).total_quantity.load(Ordering::Acquire) as f64 / 10000.0,
                order_count: (*level_ptr).order_count.load(Ordering::Acquire),
                timestamp: (*level_ptr).timestamp.load(Ordering::Acquire),
            })
        }
    }

    pub fn get_best_ask(&self) -> Option<BookLevel> {
        let level_ptr = self.best_ask.load(Ordering::Acquire);
        if level_ptr.is_null() {
            return None;
        }

        unsafe {
            Some(BookLevel {
                price: (*level_ptr).price as f64 / 10000.0,
                quantity: (*level_ptr).total_quantity.load(Ordering::Acquire) as f64 / 10000.0,
                order_count: (*level_ptr).order_count.load(Ordering::Acquire),
                timestamp: (*level_ptr).timestamp.load(Ordering::Acquire),
            })
        }
    }

    /// Get spread (bid-ask difference)
    pub fn get_spread(&self) -> Option<f64> {
        let bid = self.get_best_bid()?;
        let ask = self.get_best_ask()?;
        Some(ask.price - bid.price)
    }

    /// Get book statistics
    pub fn get_stats(&self) -> OrderBookStats {
        OrderBookStats {
            total_bid_quantity: self.total_bid_quantity.load(Ordering::Acquire),
            total_ask_quantity: self.total_ask_quantity.load(Ordering::Acquire),
            total_orders: self.total_orders.load(Ordering::Acquire),
            operations_count: self.operations_count.load(Ordering::Acquire),
            l1_updates: self.l1_updates.load(Ordering::Acquire),
            level_changes: self.level_changes.load(Ordering::Acquire),
            sequence_number: self.sequence.load(Ordering::Acquire),
        }
    }
}

/// Book level information
#[derive(Debug, Clone)]
pub struct BookLevel {
    pub price: f64,
    pub quantity: f64,
    pub order_count: usize,
    pub timestamp: u64,
}

/// Order book statistics
#[derive(Debug, Clone)]
pub struct OrderBookStats {
    pub total_bid_quantity: u64,
    pub total_ask_quantity: u64,
    pub total_orders: usize,
    pub operations_count: u64,
    pub l1_updates: u64,
    pub level_changes: u64,
    pub sequence_number: u64,
}

/// Lock-free price level allocator
struct LevelAllocator {
    free_list: AtomicPtr<PriceLevel>,
    _capacity: usize,
    allocated: AtomicUsize,
}

impl LevelAllocator {
    fn new(capacity: usize) -> Result<Self> {
        let layout = Layout::new::<PriceLevel>();
        let mut free_list = ptr::null_mut();

        // Pre-allocate levels and link them
        for _ in 0..capacity {
            unsafe {
                let level = alloc(layout) as *mut PriceLevel;
                if level.is_null() {
                    log_error!(
                        ORDERBOOK_LOGGER,
                        "Failed to allocate memory for price level (layout size={})",
                        layout.size()
                    );
                    return Err(anyhow!("Failed to allocate memory for price level"));
                }
                (*level).next_level.store(free_list, Ordering::Relaxed);
                free_list = level;
            }
        }

        Ok(Self {
            free_list: AtomicPtr::new(free_list),
            _capacity: capacity,
            allocated: AtomicUsize::new(0),
        })
    }

    fn allocate(&self) -> Result<*mut PriceLevel> {
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            if head.is_null() {
                log_error!(
                    ORDERBOOK_LOGGER,
                    "Price level allocator exhausted - all pre-allocated levels in use"
                );
                return Err(anyhow!("Price level allocator exhausted"));
            }

            unsafe {
                let next = (*head).next_level.load(Ordering::Relaxed);
                if self
                    .free_list
                    .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.allocated.fetch_add(1, Ordering::Relaxed);

                    // Initialize level
                    ptr::write_bytes(head, 0, 1);
                    (*head).total_quantity = AtomicU64::new(0);
                    (*head).order_count = AtomicUsize::new(0);
                    (*head).first_order = AtomicPtr::new(ptr::null_mut());
                    (*head).last_order = AtomicPtr::new(ptr::null_mut());
                    (*head).next_level = AtomicPtr::new(ptr::null_mut());
                    (*head).timestamp = AtomicU64::new(0);

                    return Ok(head);
                }
            }
        }
    }

    fn deallocate(&self, level: *mut PriceLevel) {
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            unsafe {
                (*level).next_level.store(head, Ordering::Relaxed);
            }

            if self
                .free_list
                .compare_exchange_weak(head, level, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.allocated.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        }
    }
}

/// Lock-free order allocator
struct OrderAllocator {
    free_list: AtomicPtr<Order>,
    _capacity: usize,
    allocated: AtomicUsize,
}

impl OrderAllocator {
    fn new(capacity: usize) -> Result<Self> {
        let layout = Layout::new::<Order>();
        let mut free_list = ptr::null_mut();

        // Pre-allocate orders and link them
        for _ in 0..capacity {
            unsafe {
                let order = alloc(layout) as *mut Order;
                if order.is_null() {
                    log_error!(
                        ORDERBOOK_LOGGER,
                        "Failed to allocate memory for order (layout size={})",
                        layout.size()
                    );
                    return Err(anyhow!("Failed to allocate memory for order"));
                }
                (*order).next_order.store(free_list, Ordering::Relaxed);
                free_list = order;
            }
        }

        Ok(Self {
            free_list: AtomicPtr::new(free_list),
            _capacity: capacity,
            allocated: AtomicUsize::new(0),
        })
    }

    fn allocate(&self) -> Result<*mut Order> {
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            if head.is_null() {
                log_error!(
                    ORDERBOOK_LOGGER,
                    "Order allocator exhausted - all pre-allocated orders in use"
                );
                return Err(anyhow!("Order allocator exhausted"));
            }

            unsafe {
                let next = (*head).next_order.load(Ordering::Relaxed);
                if self
                    .free_list
                    .compare_exchange_weak(head, next, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    self.allocated.fetch_add(1, Ordering::Relaxed);

                    // Initialize order
                    ptr::write_bytes(head, 0, 1);
                    (*head).quantity = AtomicU64::new(0);
                    (*head).next_order = AtomicPtr::new(ptr::null_mut());
                    (*head).prev_order = AtomicPtr::new(ptr::null_mut());
                    (*head).is_deleted = AtomicPtr::new(ptr::null_mut());

                    return Ok(head);
                }
            }
        }
    }

    #[allow(dead_code)]
    fn deallocate(&self, order: *mut Order) {
        loop {
            let head = self.free_list.load(Ordering::Acquire);
            unsafe {
                (*order).next_order.store(head, Ordering::Relaxed);
            }

            if self
                .free_list
                .compare_exchange_weak(head, order, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.allocated.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        }
    }
}

/// Hardware timestamp using RDTSC
#[inline(always)]
fn hardware_timestamp() -> u64 {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        std::arch::x86_64::_rdtsc()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        std::time::Instant::now().elapsed().as_nanos() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_order_book_creation() {
        let book = LockFreeOrderBook::new("BTCUSD".to_string()).unwrap();
        assert!(book.get_best_bid().is_none());
        assert!(book.get_best_ask().is_none());
    }

    #[test]
    fn test_add_orders() {
        let book = LockFreeOrderBook::new("BTCUSD".to_string()).unwrap();

        // Add bid
        book.add_order(1, 50000.0, 1.0, OrderSide::Buy).unwrap();
        let best_bid = book.get_best_bid().unwrap();
        assert_eq!(best_bid.price, 50000.0);
        assert_eq!(best_bid.quantity, 1.0);

        // Add ask
        book.add_order(2, 50100.0, 0.5, OrderSide::Sell).unwrap();
        let best_ask = book.get_best_ask().unwrap();
        assert_eq!(best_ask.price, 50100.0);
        assert_eq!(best_ask.quantity, 0.5);

        // Check spread
        let spread = book.get_spread().unwrap();
        assert_eq!(spread, 100.0);
    }
}
