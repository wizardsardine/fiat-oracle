use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Bitfinex;

const PAGE_SIZE: u64 = 10_000;

fn bitfinex_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Eur => Some("tBTCEUR"),
        Currency::Usd => Some("tBTCUSD"),
        Currency::Gbp => Some("tBTCGBP"),
        // tBTCJPY no longer in Bitfinex active pair config; /candles/last
        // returns a stale candle from Nov 2025 but /hist returns empty for
        // any recent window. Excluded from historical.
        _ => None,
    }
}

/// Parses Bitfinex v2 candles response (array of arrays).
/// Each inner array: [mts_ms, open, close, high, low, volume]
/// Returns (ts_secs, close) pairs or None on parse failure.
fn parse_candles(json: &Json) -> Option<Vec<(u64, f64)>> {
    let arr = json.as_array()?;

    let candles: Vec<(u64, f64)> = arr
        .iter()
        .filter_map(|candle| {
            let inner = candle.as_array()?;
            let ts_ms: u64 = inner.first()?.as_u64()?;
            let close: f64 = inner.get(2)?.as_f64()?;
            Some((ts_ms / 1000, close))
        })
        .collect();

    Some(candles)
}

pub(crate) struct BitfinexPager {
    pub(crate) symbol: &'static str,
    pub(crate) done: bool,
    pub(crate) upper_ts: Option<u64>,
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL: `...?start={ms}&end={ms}&limit=10000&sort=1`. Returns `PAGE_SIZE` candles ascending.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("start=").unwrap() + 6;
    let amp = url[start_idx..].find('&').unwrap();
    let cursor_ms: u64 = url[start_idx..start_idx + amp].parse().unwrap();
    let mut candles = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 0..PAGE_SIZE {
        let ts_ms = cursor_ms + i * 60_000;
        candles.push(serde_json::json!([ts_ms, 100.0, 150.0, 200.0, 50.0, 1.0]));
    }
    serde_json::json!(candles)
}

#[async_trait]
impl PagedFetch for BitfinexPager {
    fn name(&self) -> &'static str {
        "bitfinex"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts * 1000
    }

    fn check_stop(&self, cursor_ms: u64) -> bool {
        self.done || cursor_ms > self.upper_ts.unwrap_or_else(now) * 1000
    }

    fn url(&self, cursor_ms: u64) -> String {
        let to_ms = self.upper_ts.unwrap_or_else(now) * 1000;
        let symbol = self.symbol;
        format!(
            "https://api-pub.bitfinex.com/v2/candles/trade:1m:{symbol}/hist\
             ?start={cursor_ms}&end={to_ms}&limit=10000&sort=1"
        )
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor_ms: u64) -> Option<PageResult> {
        let candles = parse_candles(json)?;
        let next_cursor = candles
            .last()
            .map(|(ts, _)| ts * 1000 + 60_000)
            .unwrap_or(cursor_ms);
        Some(PageResult {
            candles,
            next_cursor,
        })
    }

    fn on_page(&mut self, page: &PageResult, _from_ts: u64, _cursor: u64) {
        self.done = page.candles.is_empty();
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
impl HistoricalSource for Bitfinex {
    fn name(&self) -> &'static str {
        "bitfinex"
    }

    fn supports(&self, currency: Currency) -> bool {
        bitfinex_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 10 000 candles/req × ~1.5 req/s public-endpoint cap.
        15_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let symbol = bitfinex_pair(currency)?;
        Some(Box::new(BitfinexPager {
            symbol,
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
    fn parse_candles_fixture() {
        // Bitfinex v2 candle layout: [mts_ms, open, close, high, low, volume]
        let fixture = json!([
            [1714000000000u64, 60000.0, 60050.5, 60100.0, 59900.0, 10.5],
            [1714000060000u64, 60050.5, 60150.25, 60200.0, 59950.0, 8.2],
            [1714000120000u64, 60150.25, 60200.0, 60300.0, 60000.0, 12.1]
        ]);

        let candles = parse_candles(&fixture).expect("should parse fixture");
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
    fn parse_candles_empty_array() {
        let fixture = json!([]);
        let candles = parse_candles(&fixture).expect("should parse empty array");
        assert!(candles.is_empty());
    }

    #[test]
    fn parse_candles_not_array_returns_none() {
        let fixture = json!({"error": "invalid"});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_eur_usd_gbp() {
        let src = Bitfinex;
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Usd));
        assert!(src.supports(Currency::Gbp));
    }

    #[test]
    fn does_not_support_jpy_or_cad() {
        let src = Bitfinex;
        assert!(!src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Cad));
    }

    macro_rules! live_fetch_test {
        ($name:ident, $currency:expr, $min:expr, $max:expr) => {
            #[tokio::test]
            #[ignore]
            async fn $name() {
                let _eg = crate::fetcher::historical::exchange_locks(&["bitfinex"]);
                let src = Bitfinex;
                let from = now() - 2 * 24 * 3600;

                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                src.fetch($currency, from, tx).await.unwrap();

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
                    assert!(*p > $min && *p < $max, "implausible price {p}");
                }
            }
        };
    }

    live_fetch_test!(live_fetch_usd_48h, Currency::Usd, 1_000.0, 1_000_000.0);
    live_fetch_test!(live_fetch_eur_48h, Currency::Eur, 1_000.0, 1_000_000.0);
    live_fetch_test!(live_fetch_gbp_48h, Currency::Gbp, 1_000.0, 1_000_000.0);
}
