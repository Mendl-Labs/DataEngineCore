#[cfg(test)]
mod tests {
    use crate::network::handlers::websockets::massive::BarAggregator;

    #[test]
    fn test_bar_aggregator_creation() {
        let agg = BarAggregator::new(1000);
        // No ticks yet — ingest returns None
        let result = agg.ingest("BTC/USD", "massive", 50000.0, 0.5, 1_000_000);
        assert!(result.is_none());
    }

    #[test]
    fn test_bar_aggregator_emits_on_window_close() {
        let agg = BarAggregator::new(1000);
        // First tick in window 0
        let r1 = agg.ingest("BTC/USD", "massive", 50000.0, 1.0, 0);
        assert!(r1.is_none());
        // Tick in window 1 — should emit the completed bar for window 0
        let bar = agg.ingest("BTC/USD", "massive", 51000.0, 0.5, 1000);
        assert!(bar.is_some());
        let b = bar.unwrap();
        assert_eq!(b.symbol, "BTC/USD");
        assert_eq!(b.open, 50000.0);
        assert_eq!(b.close, 50000.0);
        assert_eq!(b.volume, 1.0);
        assert_eq!(b.trade_count, 1);
    }

    #[test]
    fn test_bar_aggregator_multi_symbol() {
        let agg = BarAggregator::new(1000);
        let _ = agg.ingest("BTC/USD", "massive", 50000.0, 1.0, 0);
        let _ = agg.ingest("ETH/USD", "massive", 3000.0, 2.0, 0);
        // Both in same window — no bars yet
        let r = agg.ingest("BTC/USD", "massive", 50100.0, 0.5, 1000);
        assert!(r.is_some());
        let b = r.unwrap();
        assert_eq!(b.symbol, "BTC/USD");
        assert_eq!(b.high, 50000.0);
    }
}
