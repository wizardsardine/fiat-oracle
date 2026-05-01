use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Binance;

const PAGE_SIZE: u64 = 1_000;

fn binance_symbol(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Usd => Some("BTCUSDT"),
        Currency::Eur => Some("BTCEUR"),
        // BTCGBP is in BREAK status on Binance — last klines candle is from
        // Dec 2023, /ticker/price still returns a stale cached value. Excluded
        // from historical because it can't provide recent OHLC.
        Currency::Jpy => Some("BTCJPY"),
        _ => None,
    }
}

fn parse_klines(json: &Json) -> Option<Vec<(u64, f64)>> {
    let arr = json.as_array()?;
    let candles = arr
        .iter()
        .filter_map(|item| {
            let row = item.as_array()?;
            let open_time_ms = row.first()?.as_u64()?;
            let ts = open_time_ms / 1000;
            let close: f64 = row.get(4)?.as_str()?.parse().ok()?;
            Some((ts, close))
        })
        .collect();
    Some(candles)
}

pub(crate) struct BinancePager {
    pub(crate) symbol: &'static str,
    pub(crate) done: bool,
    pub(crate) upper_ts: Option<u64>,
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL has `startTime={ms}`; returns PAGE_SIZE candles ascending from startTime.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("startTime=").unwrap() + 10;
    let cursor_ms: u64 = url[start_idx..].parse().unwrap();
    let mut rows = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 0..PAGE_SIZE {
        let open_ms = cursor_ms + i * 60_000;
        let close_ms = open_ms + 59_999;
        rows.push(serde_json::json!([
            open_ms, "100", "200", "50", "150", "1.0", close_ms, "1.0", 1, "0.5", "0.5", "0"
        ]));
    }
    serde_json::json!(rows)
}

#[async_trait]
impl PagedFetch for BinancePager {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts
    }

    fn check_stop(&self, cursor: u64) -> bool {
        self.done || cursor >= self.upper_ts.unwrap_or_else(now)
    }

    fn url(&self, cursor: u64) -> String {
        let symbol = self.symbol;
        let cursor_ms = cursor * 1000;
        format!(
            "https://api.binance.com/api/v3/klines\
             ?symbol={symbol}&interval=1m&limit={PAGE_SIZE}&startTime={cursor_ms}"
        )
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_klines(json)?;
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
impl HistoricalSource for Binance {
    fn name(&self) -> &'static str {
        "binance"
    }

    fn supports(&self, currency: Currency) -> bool {
        binance_symbol(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 1000 candles/req × ~50 req/s headroom (klines weight 2 of 6000/min).
        50_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let symbol = binance_symbol(currency)?;
        Some(Box::new(BinancePager {
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
    fn parse_klines_fixture() {
        let fixture = json!([
            [
                1714000000000u64,
                "60000.00",
                "60100.00",
                "59900.00",
                "60050.50",
                "1.0",
                1714000059999u64,
                "1.0",
                1,
                "0.5",
                "0.5",
                "0"
            ],
            [
                1714000060000u64,
                "60050.50",
                "60200.00",
                "59950.00",
                "60150.25",
                "0.8",
                1714000119999u64,
                "0.8",
                1,
                "0.4",
                "0.4",
                "0"
            ],
            [
                1714000120000u64,
                "60150.25",
                "60300.00",
                "60000.00",
                "60200.00",
                "1.2",
                1714000179999u64,
                "1.2",
                1,
                "0.6",
                "0.6",
                "0"
            ]
        ]);

        let candles = parse_klines(&fixture).expect("should parse fixture");
        assert_eq!(candles.len(), 3);

        assert_eq!(candles[0].0, 1714000000);
        assert!(
            (candles[0].1 - 60050.50).abs() < 0.001,
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

        assert_eq!(candles[1].0 - candles[0].0, 60);
        assert_eq!(candles[2].0 - candles[1].0, 60);
    }

    #[test]
    fn parse_klines_empty() {
        let fixture = json!([]);
        let candles = parse_klines(&fixture).expect("empty array should parse");
        assert!(candles.is_empty());
    }

    #[test]
    fn parse_klines_non_array() {
        let fixture = json!({"error": "bad request"});
        assert!(parse_klines(&fixture).is_none());
    }

    #[test]
    fn supports_usd_eur_jpy() {
        let src = Binance;
        assert!(src.supports(Currency::Usd));
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Jpy));
    }

    #[test]
    fn does_not_support_gbp_or_cad() {
        let src = Binance;
        assert!(!src.supports(Currency::Gbp));
        assert!(!src.supports(Currency::Cad));
    }

    macro_rules! live_fetch_test {
        ($name:ident, $currency:expr, $min:expr, $max:expr) => {
            #[tokio::test]
            #[ignore]
            async fn $name() {
                let _eg = crate::fetcher::historical::exchange_locks(&["binance"]);
                let src = Binance;
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
    live_fetch_test!(live_fetch_jpy_48h, Currency::Jpy, 1_000_000.0, 30_000_000.0);
}
