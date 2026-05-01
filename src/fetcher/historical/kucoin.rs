use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Kucoin;

const PAGE_SIZE: u64 = 1_500;
const WINDOW: u64 = PAGE_SIZE * 60;

/// KuCoin pair string for `currency`.
/// USD maps to BTC-USDT (KuCoin does not list a direct BTC/USD pair;
/// USDT is treated as a USD-equivalent for historical price purposes).
fn kucoin_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Eur => Some("BTC-EUR"),
        Currency::Usd => Some("BTC-USDT"),
        _ => None,
    }
}

/// Parse KuCoin candles response. Returns `(timestamp, close)` pairs or `None` on error.
///
/// KuCoin returns `{"code":"200000","data":[["ts","open","close","high","low","amount","vol"],...]}`.
/// All fields are strings. Results are returned most-recent-first by the API.
fn parse_candles(json: &Json) -> Option<Vec<(u64, f64)>> {
    let data = json.get("data")?.as_array()?;

    let candles: Vec<(u64, f64)> = data
        .iter()
        .filter_map(|candle| {
            let arr = candle.as_array()?;
            let ts: u64 = arr.first()?.as_str()?.parse().ok()?;
            let close: f64 = arr.get(2)?.as_str()?.parse().ok()?;
            Some((ts, close))
        })
        .collect();

    Some(candles)
}

pub(crate) struct KucoinPager {
    pub(crate) pair: &'static str,
    pub(crate) done: bool,
    pub(crate) upper_ts: Option<u64>,
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL: `...?type=1min&symbol=...&startAt={cursor}&endAt={end}`. Returns `PAGE_SIZE` candles
/// (newest-first per KuCoin convention).
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("startAt=").unwrap() + 8;
    let amp = url[start_idx..].find('&').unwrap();
    let cursor: u64 = url[start_idx..start_idx + amp].parse().unwrap();
    let mut candles = Vec::with_capacity(PAGE_SIZE as usize);
    for i in (0..PAGE_SIZE).rev() {
        let ts = cursor + i * 60;
        candles.push(serde_json::json!([
            ts.to_string(),
            "100.0",
            "150.0",
            "200.0",
            "50.0",
            "1.0",
            "150.0"
        ]));
    }
    serde_json::json!({ "code": "200000", "data": candles })
}

#[async_trait]
impl PagedFetch for KucoinPager {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts
    }

    fn check_stop(&self, cursor: u64) -> bool {
        self.done || cursor >= self.upper_ts.unwrap_or_else(now)
    }

    fn url(&self, cursor: u64) -> String {
        let upper = self.upper_ts.unwrap_or_else(now);
        let end = upper.min(cursor + WINDOW);
        let pair = self.pair;
        format!(
            "https://api.kucoin.com/api/v1/market/candles\
             ?type=1min&symbol={pair}&startAt={cursor}&endAt={end}"
        )
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_candles(json)?;
        let upper = self.upper_ts.unwrap_or_else(now);
        let next_cursor = upper.min(cursor + WINDOW) + 1;
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
impl HistoricalSource for Kucoin {
    fn name(&self) -> &'static str {
        "kucoin"
    }

    fn supports(&self, currency: Currency) -> bool {
        kucoin_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 1500 candles/req × ~22 req/s (klines weight 3 of 2000-pt/30 s pool).
        33_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let pair = kucoin_pair(currency)?;
        Some(Box::new(KucoinPager {
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
    fn parse_candles_fixture() {
        // KuCoin layout: [ts_str, open_str, close_str, high_str, low_str, amount_str, vol_str]
        // API returns most-recent-first; timestamps 60s apart for 1-minute candles.
        let fixture = json!({
            "code": "200000",
            "data": [
                ["1714000120", "60150.25", "60200.00", "60300.00", "60000.00", "12.1", "730000"],
                ["1714000060", "60050.50", "60150.25", "60200.00", "59950.00", "8.2", "493000"],
                ["1714000000", "60000.00", "60050.50", "60100.00", "59900.00", "10.5", "630000"]
            ]
        });

        let candles = parse_candles(&fixture).expect("should parse fixture");
        assert_eq!(candles.len(), 3);

        assert_eq!(candles[0].0, 1714000120);
        assert!(
            (candles[0].1 - 60200.0).abs() < 0.001,
            "close: {}",
            candles[0].1
        );

        assert_eq!(candles[1].0, 1714000060);
        assert!(
            (candles[1].1 - 60150.25).abs() < 0.001,
            "close: {}",
            candles[1].1
        );

        assert_eq!(candles[2].0, 1714000000);
        assert!(
            (candles[2].1 - 60050.50).abs() < 0.001,
            "close: {}",
            candles[2].1
        );

        // 1-minute intervals between consecutive candles (newest-first order)
        assert_eq!(candles[0].0 - candles[1].0, 60);
        assert_eq!(candles[1].0 - candles[2].0, 60);
    }

    #[test]
    fn parse_candles_empty_data() {
        let fixture = json!({"code": "200000", "data": []});
        let candles = parse_candles(&fixture).expect("should parse empty data");
        assert!(candles.is_empty());
    }

    #[test]
    fn parse_candles_missing_data_returns_none() {
        let fixture = json!({"code": "200000"});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_eur_and_usd() {
        let src = Kucoin;
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Usd));
    }

    #[test]
    fn does_not_support_other_currencies() {
        let src = Kucoin;
        assert!(!src.supports(Currency::Gbp));
        assert!(!src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Cad));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_eur_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["kucoin"]);
        let src = Kucoin;
        let from = now() - 2 * 24 * 3600;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        src.fetch(Currency::Eur, from, tx).await.unwrap();

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
