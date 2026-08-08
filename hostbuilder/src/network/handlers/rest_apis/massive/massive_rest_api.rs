use anyhow::{anyhow, Result};
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION},
    Client,
};
use serde::Deserialize;
use std::env;

use crate::infrastructure::logging_facade::MAIN_LOGGER;
use crate::{log_info, log_error, log_debug};

// Base URL is read from MASSIVE_API_BASE_URL env var (defaults to api.polygon.io)

// =============================================================================
// Response types
// =============================================================================

#[derive(Debug, Deserialize)]
pub struct AggregateResponse {
    pub ticker: String,
    #[serde(default)]
    pub results: Vec<AggResult>,
    #[serde(rename = "resultsCount", default)]
    pub results_count: usize,
    pub status: String,
    /// Polygon pagination cursor — present when more pages are available.
    pub next_url: Option<String>,
}

/// Response from /v3/reference/tickers
#[derive(Debug, Deserialize)]
pub struct TickerListResponse {
    pub results: Option<Vec<TickerInfo>>,
    pub next_url: Option<String>,
    pub status: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct TickerInfo {
    pub ticker: String,
    pub name: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct AggResult {
    /// Volume
    pub v: f64,
    /// Open
    pub o: f64,
    /// Close
    pub c: f64,
    /// High
    pub h: f64,
    /// Low
    pub l: f64,
    /// Unix timestamp (ms, start of window)
    pub t: i64,
    /// Number of transactions
    pub n: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotResponse {
    pub status: String,
    pub tickers: Vec<SnapshotTicker>,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotTicker {
    pub ticker: String,
    pub day: Option<SnapshotDay>,
    #[serde(rename = "lastTrade")]
    pub last_trade: Option<LastTrade>,
    #[serde(rename = "prevDay")]
    pub prev_day: Option<SnapshotDay>,
}

#[derive(Debug, Deserialize)]
pub struct SnapshotDay {
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub v: f64,
    pub vw: Option<f64>,
}

#[derive(Debug, Deserialize)]
pub struct LastTrade {
    pub p: f64,
    pub s: f64,
    pub t: i64,
}

// =============================================================================
// Handler
// =============================================================================

pub struct MassiveRestHandler {
    api_key: String,
    base_url: String,
    client: Client,
}

impl MassiveRestHandler {
    pub fn new() -> Result<Self> {
        let api_key = env::var("MASSIVE_API_KEY")
            .map_err(|_| anyhow!("MASSIVE_API_KEY environment variable is not set"))?;
        if api_key.is_empty() {
            return Err(anyhow!("MASSIVE_API_KEY is set but empty"));
        }
        let base_url = env::var("MASSIVE_API_BASE_URL")
            .unwrap_or_else(|_| "https://api.polygon.io".into());
        let client = Client::new();
        log_info!(MAIN_LOGGER, "MassiveRestHandler initialized (base_url={})", base_url);
        Ok(Self { api_key, base_url, client })
    }

    fn bearer_headers(&self) -> Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        let value = HeaderValue::from_str(&format!("Bearer {}", self.api_key))
            .map_err(|e| anyhow!("Invalid API key format: {}", e))?;
        headers.insert(AUTHORIZATION, value);
        Ok(headers)
    }

    /// Convert a trading symbol to the Massive REST ticker format.
    /// "BTC-USD" -> "X:BTCUSD"
    fn symbol_to_ticker(symbol: &str) -> String {
        format!("X:{}", symbol.replace('-', ""))
    }

    /// Fetch aggregate (OHLCV) bars for a crypto ticker with pagination.
    ///
    /// Follows Polygon `next_url` cursor until all pages are consumed.
    ///
    /// * `ticker`      - Polygon ticker, e.g. "X:BTCUSD"
    /// * `multiplier`  - Bar size multiplier, e.g. 1
    /// * `timespan`    - "minute" | "hour" | "day" | "second"
    /// * `from`        - ISO date or Unix ms, e.g. "2024-01-01"
    /// * `to`          - ISO date or Unix ms, e.g. "2024-01-31"
    pub async fn get_crypto_aggregates(
        &self,
        ticker: &str,
        multiplier: u32,
        timespan: &str,
        from: &str,
        to: &str,
    ) -> Result<AggregateResponse> {
        let first_url = format!(
            "{}/v2/aggs/ticker/{}/range/{}/{}/{}/{}?adjusted=true&sort=asc&limit=50000",
            self.base_url, ticker, multiplier, timespan, from, to
        );

        let mut all_results: Vec<AggResult> = Vec::new();
        let mut url = first_url;
        let mut final_status = String::new();
        let mut final_ticker = ticker.to_string();

        loop {
            log_debug!(MAIN_LOGGER, "GET {}", url);
            let headers = self.bearer_headers()?;
            let response = self.client
                .get(&url)
                .headers(headers)
                .send()
                .await
                .map_err(|e| anyhow!("Massive API request failed: {}", e))?;

            let status = response.status();
            if status.as_u16() == 429 {
                return Err(anyhow!("Massive API rate limited (429) for {}", ticker));
            }
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                log_error!(MAIN_LOGGER, "Massive API error {}: {}", status, body);
                return Err(anyhow!("Massive API returned {}: {}", status, body));
            }

            let page: AggregateResponse = response
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse aggregate response: {}", e))?;

            final_status = page.status.clone();
            final_ticker = page.ticker.clone();
            all_results.extend(page.results);

            match page.next_url {
                Some(next) if !next.is_empty() => url = next,
                _ => break,
            }
        }

        let count = all_results.len();
        log_info!(MAIN_LOGGER, "Fetched {} bars for {}", count, ticker);
        Ok(AggregateResponse {
            ticker: final_ticker,
            results: all_results,
            results_count: count,
            status: final_status,
            next_url: None,
        })
    }

    /// Fetch aggregate bars for a slice of trading symbols (e.g. ["BTC-USD", "ETH-USD"]).
    /// Converts symbols to Massive tickers and calls get_crypto_aggregates for each.
    pub async fn get_aggregates_for_symbols(
        &self,
        symbols: &[String],
        multiplier: u32,
        timespan: &str,
        from: &str,
        to: &str,
    ) -> Vec<Result<AggregateResponse>> {
        let mut results = Vec::with_capacity(symbols.len());
        for symbol in symbols {
            let ticker = Self::symbol_to_ticker(symbol);
            results.push(
                self.get_crypto_aggregates(&ticker, multiplier, timespan, from, to).await,
            );
        }
        results
    }

    /// Fetch the current day snapshot for one or more crypto symbols.
    ///
    /// * `symbols` - Trading symbols, e.g. ["BTC-USD", "ETH-USD"]
    pub async fn get_crypto_snapshot(&self, symbols: &[String]) -> Result<SnapshotResponse> {
        let tickers: Vec<String> = symbols.iter().map(|s| Self::symbol_to_ticker(s)).collect();
        let tickers_csv = tickers.join(",");
        let url = format!(
            "{}/v2/snapshot/locale/global/markets/crypto/tickers?tickers={}",
            self.base_url, tickers_csv
        );
        log_debug!(MAIN_LOGGER, "GET {}", url);
        let headers = self.bearer_headers()?;
        let response = self.client
            .get(&url)
            .headers(headers)
            .send()
            .await
            .map_err(|e| anyhow!("Massive snapshot request failed: {}", e))?;

        let status = response.status();
        if !status.is_success() {
            let body = response.text().await.unwrap_or_default();
            log_error!(MAIN_LOGGER, "Massive snapshot error {}: {}", status, body);
            return Err(anyhow!("Massive snapshot returned {}: {}", status, body));
        }

        let result: SnapshotResponse = response
            .json()
            .await
            .map_err(|e| anyhow!("Failed to parse snapshot response: {}", e))?;

        log_info!(MAIN_LOGGER, "Snapshot fetched for {} tickers", result.tickers.len());
        Ok(result)
    }

    /// Fetch the full list of active crypto tickers from Polygon,
    /// following pagination until all pages are consumed.
    pub async fn get_ticker_list(&self) -> Result<Vec<TickerInfo>> {
        let mut tickers: Vec<TickerInfo> = Vec::new();
        let mut url = format!(
            "{}/v3/reference/tickers?market=crypto&active=true&limit=1000&sort=ticker&order=asc",
            self.base_url
        );

        loop {
            log_debug!(MAIN_LOGGER, "GET {}", url);
            let headers = self.bearer_headers()?;
            let response = self.client
                .get(&url)
                .headers(headers)
                .send()
                .await
                .map_err(|e| anyhow!("Massive ticker list request failed: {}", e))?;

            let status = response.status();
            if !status.is_success() {
                let body = response.text().await.unwrap_or_default();
                log_error!(MAIN_LOGGER, "Massive ticker list error {}: {}", status, body);
                return Err(anyhow!("Massive ticker list returned {}: {}", status, body));
            }

            let page: TickerListResponse = response
                .json()
                .await
                .map_err(|e| anyhow!("Failed to parse ticker list response: {}", e))?;

            if let Some(results) = page.results {
                tickers.extend(results);
            }

            match page.next_url {
                Some(next) if !next.is_empty() => url = next,
                _ => break,
            }
        }

        log_info!(MAIN_LOGGER, "Fetched {} tickers from Massive", tickers.len());
        Ok(tickers)
    }
}
