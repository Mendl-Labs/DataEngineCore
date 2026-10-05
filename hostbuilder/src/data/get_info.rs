//! Exchange / security / orderbook metadata retrieval.
//!
//! The underlying `exchanges`, `securities`, and `order_books` tables were removed
//! from the `databaseschema` crate. These lightweight structs preserve the API so
//! callers that are gated behind `#[cfg(feature = "database")]` continue to compile.
//! Re-implement against real tables when the schema is restored.

use std::collections::HashMap;
use std::sync::Arc;

use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::log_info;
use anyhow::Error;
use diesel_async::AsyncPgConnection;

// ── Lightweight stand-in types for removed DB models ────────────────────

/// Represents an exchange record (previously backed by the `exchanges` table).
#[derive(Debug, Clone)]
pub struct Exchange {
    pub exchange_id: i32,
    pub exchange: String,
}

/// Represents a security record (previously backed by the `securities` table).
#[derive(Debug, Clone)]
pub struct Security {
    pub security_id: i32,
    pub symbol: String,
}

/// Represents an order-book record (previously backed by the `order_books` table).
#[derive(Debug, Clone)]
pub struct OrderBook {
    pub order_book_id: i32,
    pub exchange_id: i32,
    pub security_id: i32,
}

// ── Public API (same signatures as before) ──────────────────────────────

pub async fn get_exchange(
    _postgres_pool: Arc<diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>>,
    name: String,
) -> Result<Exchange, Error> {
    log_info!(
        MAIN_LOGGER,
        "get_exchange called for '{}' — using in-memory stub (DB tables removed)",
        name
    );
    Err(anyhow::anyhow!(
        "exchanges table was removed from databaseschema; exchange metadata is no longer persisted"
    ))
}

pub async fn get_securities(
    _postgres_pool: Arc<diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>>,
    symbols: &[String],
) -> Result<Vec<Security>, Error> {
    log_info!(
        MAIN_LOGGER,
        "get_securities called for {} symbols — using in-memory stub (DB tables removed)",
        symbols.len()
    );
    Err(anyhow::anyhow!(
        "securities table was removed from databaseschema; security metadata is no longer persisted"
    ))
}

pub async fn get_orderbooks(
    _postgres_pool: Arc<diesel_async::pooled_connection::deadpool::Pool<AsyncPgConnection>>,
    _securities: Vec<Security>,
    _exchange: &Exchange,
) -> Result<HashMap<String, OrderBook>, Error> {
    log_info!(
        MAIN_LOGGER,
        "get_orderbooks called — using in-memory stub (DB tables removed)"
    );
    Err(anyhow::anyhow!(
        "order_books table was removed from databaseschema; orderbook metadata is no longer persisted"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exchange_struct() {
        let exchange = Exchange {
            exchange_id: 1,
            exchange: "kraken".to_string(),
        };
        assert_eq!(exchange.exchange_id, 1);
        assert_eq!(exchange.exchange, "kraken");

        let cloned = exchange.clone();
        assert_eq!(cloned.exchange_id, exchange.exchange_id);
    }

    #[test]
    fn test_security_struct() {
        let security = Security {
            security_id: 42,
            symbol: "BTC-USD".to_string(),
        };
        assert_eq!(security.security_id, 42);
        assert_eq!(security.symbol, "BTC-USD");

        let cloned = security.clone();
        assert_eq!(cloned.symbol, security.symbol);
    }

    #[test]
    fn test_orderbook_struct() {
        let ob = OrderBook {
            order_book_id: 10,
            exchange_id: 1,
            security_id: 42,
        };
        assert_eq!(ob.order_book_id, 10);
        assert_eq!(ob.exchange_id, 1);
        assert_eq!(ob.security_id, 42);
    }
}
