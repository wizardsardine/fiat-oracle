use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Bitstamp;

const PAGE_SIZE: u64 = 1_000;

fn bitstamp_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Eur => Some("btceur"),
        Currency::Usd => Some("btcusd"),
        Currency::Gbp => Some("btcgbp"),
        _ => None,
    }
}

fn parse_ohlc_response(json: &Json) -> Option<Vec<(u64, f64)>> {
    let ohlc_arr = json.get("data")?.get("ohlc")?.as_array()?;

    let candles: Vec<(u64, f64)> = ohlc_arr
        .iter()
        .filter_map(|candle| {
            let ts: u64 = candle.get("timestamp")?.as_str()?.parse().ok()?;
            let close: f64 = candle.get("close")?.as_str()?.parse().ok()?;
            Some((ts, close))
        })
        .collect();

    Some(candles)
}

pub(crate) struct BitstampPager {
    pub(crate) pair: &'static str,
    pub(crate) done: bool,
    /// Upper bound for this shard. `None` = use `now()`.
    pub(crate) upper_ts: Option<u64>,
    /// Number of parallel shards; reported via [`PagedFetch::concurrency`].
    pub(crate) concurrency: u32,
    /// Throughput target (candles/sec). Threaded in by `make_pager`.
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL: `...?step=60&limit=1000&start={cursor}`. Returns `PAGE_SIZE` candles.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("start=").unwrap() + 6;
    let cursor: u64 = url[start_idx..].parse().unwrap();
    let mut ohlc = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 0..PAGE_SIZE {
        let ts = cursor + i * 60;
        ohlc.push(serde_json::json!({
            "timestamp": ts.to_string(),
            "open": "100", "high": "200", "low": "50", "close": "150", "volume": "1"
        }));
    }
    serde_json::json!({ "data": { "pair": "BTC/USD", "ohlc": ohlc } })
}

#[async_trait]
impl PagedFetch for BitstampPager {
    fn name(&self) -> &'static str {
        "bitstamp"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts
    }

    fn check_stop(&self, cursor: u64) -> bool {
        self.done || cursor >= self.upper_ts.unwrap_or_else(now)
    }

    fn url(&self, cursor: u64) -> String {
        let pair = self.pair;
        format!("https://www.bitstamp.net/api/v2/ohlc/{pair}/?step=60&limit=1000&start={cursor}")
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_ohlc_response(json)?;
        let next_cursor = candles.last().map(|(ts, _)| ts + 60).unwrap_or(cursor);
        Some(PageResult {
            candles,
            next_cursor,
        })
    }

    fn on_page(&mut self, page: &PageResult, _from_ts: u64, _cursor: u64) {
        self.done = page.candles.len() < PAGE_SIZE as usize;
    }

    fn page_size(&self) -> u64 {
        PAGE_SIZE
    }

    fn concurrency(&self) -> u32 {
        self.concurrency
    }

    fn target_candles_per_sec(&self) -> u64 {
        self.target_candles_per_sec
    }
}

#[async_trait]
impl HistoricalSource for Bitstamp {
    fn name(&self) -> &'static str {
        "bitstamp"
    }

    fn supports(&self, currency: Currency) -> bool {
        bitstamp_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 1000 candles/req × ~17 req/s sustained (10k/10min secondary cap).
        17_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let pair = bitstamp_pair(currency)?;
        Some(Box::new(BitstampPager {
            pair,
            done: false,
            upper_ts: Some(hi),
            concurrency,
            target_candles_per_sec,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetcher::historical::now;
    use serde_json::json;

    #[test]
    fn parse_ohlc_fixture() {
        let fixture = json!({
            "data": {
                "pair": "BTC/USD",
                "ohlc": [
                    {"timestamp": "1714000000", "open": "60000.0", "high": "60100.0", "low": "59900.0", "close": "60050.50", "volume": "10.5"},
                    {"timestamp": "1714000060", "open": "60050.5", "high": "60200.0", "low": "59950.0", "close": "60150.25", "volume": "8.2"},
                    {"timestamp": "1714000120", "open": "60150.25", "high": "60300.0", "low": "60000.0", "close": "60200.00", "volume": "12.1"}
                ]
            }
        });

        let candles = parse_ohlc_response(&fixture).expect("should parse fixture");
        assert_eq!(candles.len(), 3);

        assert_eq!(candles[0].0, 1714000000);
        assert!(
            (candles[0].1 - 60050.5).abs() < 0.001,
            "close: {}",
            candles[0].1
        );

        assert_eq!(candles[1].0, 1714000060);
        assert!(
            (candles[1].1 - 60150.25).abs() < 0.001,
            "close: {}",
            candles[1].1
        );

        assert_eq!(candles[2].0, 1714000120);
        assert!(
            (candles[2].1 - 60200.0).abs() < 0.001,
            "close: {}",
            candles[2].1
        );

        // 1-minute intervals
        assert_eq!(candles[1].0 - candles[0].0, 60);
        assert_eq!(candles[2].0 - candles[1].0, 60);
    }

    #[test]
    fn empty_ohlc_array_returns_empty_vec() {
        let fixture = json!({"data": {"pair": "BTC/USD", "ohlc": []}});
        let candles = parse_ohlc_response(&fixture).expect("should parse empty array");
        assert!(candles.is_empty());
    }

    #[test]
    fn missing_data_key_returns_none() {
        let fixture = json!({"ohlc": []});
        assert!(parse_ohlc_response(&fixture).is_none());
    }

    #[test]
    fn missing_ohlc_key_returns_none() {
        let fixture = json!({"data": {"pair": "BTC/USD"}});
        assert!(parse_ohlc_response(&fixture).is_none());
    }

    #[test]
    fn supports_eur_usd_gbp() {
        let src = Bitstamp;
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Usd));
        assert!(src.supports(Currency::Gbp));
    }

    #[test]
    fn does_not_support_jpy_cad() {
        let src = Bitstamp;
        assert!(!src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Cad));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_usd_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["bitstamp"]);
        let src = Bitstamp;
        let from = now() - 2 * 24 * 3600;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        src.fetch(Currency::Usd, from, tx).await.unwrap();

        let mut out = Vec::new();
        while let Some(batch) = rx.recv().await {
            out.extend(batch);
        }
        assert!(
            out.len() >= 100,
            "expected >=100 1-minute candles in 48h, got {}",
            out.len()
        );
        for (_ts, p) in &out {
            assert!(*p > 1000.0 && *p < 1_000_000.0, "implausible price {p}");
        }
    }
}
