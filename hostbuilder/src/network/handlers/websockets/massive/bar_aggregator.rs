use crate::infrastructure::logging_facade::MASSIVE_LOGGER;
use crate::{log_debug, log_info};
use dashmap::DashMap;
use protocol::broker::messages::Bar;

// =============================================================================
// Internal bar accumulation window
// =============================================================================

struct BarWindow {
    open: f64,
    high: f64,
    low: f64,
    close: f64,
    volume: f64,
    trade_count: i32,
    window_start_ms: i64,
}

impl BarWindow {
    fn new(price: f64, size: f64, window_start_ms: i64) -> Self {
        Self {
            open: price,
            high: price,
            low: price,
            close: price,
            volume: size,
            trade_count: 1,
            window_start_ms,
        }
    }

    fn update(&mut self, price: f64, size: f64) {
        if price > self.high {
            self.high = price;
        }
        if price < self.low {
            self.low = price;
        }
        self.close = price;
        self.volume += size;
        self.trade_count += 1;
    }
}

// =============================================================================
// BarAggregator
// =============================================================================

/// Lock-free, multi-symbol tick-to-OHLCV aggregator.
///
/// Each call to [`ingest`] updates the current bar window for a
/// `(symbol, exchange)` pair.  When a trade arrives in a new window, the
/// completed window is finalised into a [`Bar`] and returned; the new window
/// is started automatically.
///
/// Thread-safe via `DashMap` — safe to share behind an `Arc`.
pub struct BarAggregator {
    windows: DashMap<(String, String), BarWindow>,
    /// Window duration in milliseconds (e.g. 1000 for 1-second bars).
    interval_ms: i64,
}

impl BarAggregator {
    /// Create a new aggregator.
    ///
    /// `interval_ms` — bar window length in milliseconds.  Pass `1000` for
    /// standard 1-second bars.
    pub fn new(interval_ms: i64) -> Self {
        Self {
            windows: DashMap::new(),
            interval_ms,
        }
    }

    /// Returns the bar resolution in whole seconds (rounded down).
    fn interval_secs(&self) -> i32 {
        (self.interval_ms / 1000) as i32
    }

    /// Feed a single trade tick into the aggregator.
    ///
    /// Returns `Some(Bar)` if this tick caused a previous window to close, or
    /// `None` if the tick was accumulated into the current open window.
    pub fn ingest(
        &self,
        symbol: &str,
        exchange: &str,
        price: f64,
        size: f64,
        timestamp_ms: i64,
    ) -> Option<Bar> {
        let window_start = (timestamp_ms / self.interval_ms) * self.interval_ms;
        let key = (symbol.to_string(), exchange.to_string());

        if let Some(mut window) = self.windows.get_mut(&key) {
            if window.window_start_ms == window_start {
                // Same window — just update.
                window.update(price, size);
                return None;
            }

            // New window: extract the completed bar, then replace.
            // We need to take ownership of the old window to convert it.
            let old_open = window.open;
            let old_high = window.high;
            let old_low = window.low;
            let old_close = window.close;
            let old_volume = window.volume;
            let old_trade_count = window.trade_count;
            let old_start = window.window_start_ms;

            // Overwrite in-place with the new window.
            window.open = price;
            window.high = price;
            window.low = price;
            window.close = price;
            window.volume = size;
            window.trade_count = 1;
            window.window_start_ms = window_start;

            let bar_end_ms = old_start + self.interval_ms - 1;
            let completed_bar = Bar {
                symbol: symbol.to_string(),
                exchange: exchange.to_string(),
                open: old_open,
                high: old_high,
                low: old_low,
                close: old_close,
                volume: old_volume,
                bar_start_ms: old_start,
                bar_end_ms,
                trade_count: old_trade_count,
                interval_secs: self.interval_secs(),
            };
            log_debug!(
                MASSIVE_LOGGER,
                "BarAggregator: bar completed for {}:{} O={} H={} L={} C={} V={} trades={}",
                symbol,
                exchange,
                old_open,
                old_high,
                old_low,
                old_close,
                old_volume,
                old_trade_count
            );
            return Some(completed_bar);
        }

        // No existing window — create one (first tick for this symbol).
        log_info!(
            MASSIVE_LOGGER,
            "BarAggregator: new symbol registered: {}:{} (interval={}ms)",
            symbol,
            exchange,
            self.interval_ms
        );
        self.windows
            .insert(key, BarWindow::new(price, size, window_start));
        None
    }
}
