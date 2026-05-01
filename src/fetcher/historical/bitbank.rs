use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Bitbank;

/// One request returns up to 1440 1-minute candles (one full day).
const PAGE_SIZE: u64 = 1_440;
const SECS_PER_DAY: u64 = 86_400;

fn bitbank_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Jpy => Some("btc_jpy"),
        _ => None,
    }
}

/// Convert unix seconds to (year, month, day) using the Howard Hinnant
/// civil_from_days algorithm — same approach as `coinbase::days_to_ymd`.
fn ts_to_yyyymmdd(ts: u64) -> String {
    let days = (ts / SECS_PER_DAY) as i64 + 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}{m:02}{d:02}")
}

/// Bitbank candlestick response shape:
/// `{"data": {"candlestick": [{"type": "1min", "ohlcv": [[o,h,l,c,v,ts_ms], ...]}]}}`
fn parse_candles(json: &Json) -> Option<Vec<(u64, f64)>> {
    let ohlcv = json
        .get("data")?
        .get("candlestick")?
        .as_array()?
        .first()?
        .get("ohlcv")?
        .as_array()?;

    let candles: Vec<(u64, f64)> = ohlcv
        .iter()
        .filter_map(|row| {
            let arr = row.as_array()?;
            // close is at index 3, ts_ms at index 5
            let close: f64 = arr.get(3)?.as_str()?.parse().ok()?;
            let ts_ms: u64 = arr.get(5)?.as_u64()?;
            Some((ts_ms / 1000, close))
        })
        .collect();

    Some(candles)
}

/// Canned response for tests. URL ends with `/{YYYYMMDD}`; emits PAGE_SIZE
/// minute-aligned candles starting at midnight UTC of that day.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let date = &url[url.len() - 8..];
    let year: i64 = date[0..4].parse().unwrap();
    let month: u64 = date[4..6].parse().unwrap();
    let day: u64 = date[6..8].parse().unwrap();
    // Howard-Hinnant days_from_civil
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146097 + doe as i64 - 719468) as u64;
    let ts0_ms = days * SECS_PER_DAY * 1000;
    let mut ohlcv = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 0..PAGE_SIZE {
        let ts_ms = ts0_ms + i * 60_000;
        ohlcv.push(serde_json::json!(["100", "200", "50", "150", "1.0", ts_ms]));
    }
    serde_json::json!({
        "success": 1,
        "data": {
            "candlestick": [{"type": "1min", "ohlcv": ohlcv}],
            "timestamp": ts0_ms + SECS_PER_DAY * 1000
        }
    })
}

pub(crate) struct BitbankPager {
    pub(crate) pair: &'static str,
    pub(crate) done: bool,
    /// Upper bound for this shard. `None` = use `now()`.
    pub(crate) upper_ts: Option<u64>,
    /// Number of parallel shards; reported via [`PagedFetch::concurrency`].
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

#[async_trait]
impl PagedFetch for BitbankPager {
    fn name(&self) -> &'static str {
        "bitbank"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        // Align to UTC midnight so each request fetches a complete day.
        from_ts / SECS_PER_DAY * SECS_PER_DAY
    }

    fn check_stop(&self, cursor: u64) -> bool {
        self.done || cursor >= self.upper_ts.unwrap_or_else(now)
    }

    fn url(&self, cursor: u64) -> String {
        let pair = self.pair;
        let date = ts_to_yyyymmdd(cursor);
        format!("https://public.bitbank.cc/{pair}/candlestick/1min/{date}")
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_candles(json)?;
        // Always advance by one day even if today's response is short
        // (the partial-page check in `on_page` handles termination).
        let next_cursor = cursor + SECS_PER_DAY;
        Some(PageResult {
            candles,
            next_cursor,
        })
    }

    fn on_page(&mut self, page: &PageResult, _from_ts: u64, _cursor: u64) {
        // A short page means we've reached the in-progress current day;
        // future days won't have any data either.
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
impl HistoricalSource for Bitbank {
    fn name(&self) -> &'static str {
        "bitbank"
    }

    fn supports(&self, currency: Currency) -> bool {
        bitbank_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 1440 candles/req (one full day) × ~1 req/s courteous default
        // — Bitbank doesn't publish a public-endpoint rate limit.
        1_440
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let pair = bitbank_pair(currency)?;
        Some(Box::new(BitbankPager {
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
    fn ts_to_yyyymmdd_known_dates() {
        // 2024-05-01T00:00:00Z = 1714521600
        assert_eq!(ts_to_yyyymmdd(1714521600), "20240501");
        // 1970-01-01T00:00:00Z = 0
        assert_eq!(ts_to_yyyymmdd(0), "19700101");
        // 2017-02-28T00:00:00Z = 1488240000 (Bitbank launch day)
        assert_eq!(ts_to_yyyymmdd(1488240000), "20170228");
        // mid-day timestamp still rounds to the same day
        assert_eq!(ts_to_yyyymmdd(1714521600 + 12 * 3600), "20240501");
    }

    #[test]
    fn parse_candles_fixture() {
        // OHLCV: [open, high, low, close, volume, timestamp_ms] — strings
        // for prices, integer for ts.
        let fixture = json!({
            "success": 1,
            "data": {
                "candlestick": [{
                    "type": "1min",
                    "ohlcv": [
                        ["6000000", "6010000", "5990000", "6005000", "1.0", 1714521600000u64],
                        ["6005000", "6020000", "5995000", "6015000", "0.5", 1714521660000u64]
                    ]
                }]
            }
        });

        let candles = parse_candles(&fixture).expect("should parse fixture");
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].0, 1714521600);
        assert!((candles[0].1 - 6005000.0).abs() < 0.001);
        assert_eq!(candles[1].0 - candles[0].0, 60);
    }

    #[test]
    fn parse_candles_empty_ohlcv() {
        let fixture = json!({
            "success": 1,
            "data": {"candlestick": [{"type": "1min", "ohlcv": []}]}
        });
        let candles = parse_candles(&fixture).expect("empty ohlcv should parse");
        assert!(candles.is_empty());
    }

    #[test]
    fn parse_candles_missing_data() {
        let fixture = json!({"success": 0});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_jpy_only() {
        let src = Bitbank;
        assert!(src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Usd));
        assert!(!src.supports(Currency::Eur));
        assert!(!src.supports(Currency::Krw));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_jpy_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["bitbank"]);
        let src = Bitbank;
        let from = now() - 2 * 24 * 3600;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        src.fetch(Currency::Jpy, from, tx).await.unwrap();

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
            assert!(
                *p > 1_000_000.0 && *p < 30_000_000.0,
                "implausible JPY price {p}"
            );
        }
    }
}
