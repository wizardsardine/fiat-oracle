use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Upbit;

/// Upbit caps `count` at 200.
const PAGE_SIZE: u64 = 200;

fn upbit_market(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Krw => Some("KRW-BTC"),
        _ => None,
    }
}

/// Convert unix seconds to `YYYY-MM-DDTHH:MM:SS` (Upbit accepts UTC without
/// the trailing `Z`). Algorithm matches the Howard-Hinnant routine reused
/// across the other date-aware fetchers.
fn ts_to_upbit_iso(ts: u64) -> String {
    let secs_of_day = ts % 86400;
    let sec = (secs_of_day % 60) as u32;
    let min = ((secs_of_day % 3600) / 60) as u32;
    let hour = (secs_of_day / 3600) as u32;
    let days = (ts / 86400) as i64 + 719468;
    let era = if days >= 0 { days } else { days - 146096 } / 146097;
    let doe = (days - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    format!("{year:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}")
}

/// Returns `(candles, oldest_ts)` parsed from an Upbit minute-candle response
/// (sorted newest-first). `oldest_ts` is `u64::MAX` for an empty page.
fn parse_candles(json: &Json) -> Option<(Vec<(u64, f64)>, u64)> {
    let arr = json.as_array()?;
    if arr.is_empty() {
        return Some((vec![], u64::MAX));
    }
    let mut out = Vec::with_capacity(arr.len());
    for row in arr {
        let ts_ms: u64 = row.get("timestamp")?.as_u64()?;
        let close: f64 = row.get("trade_price")?.as_f64()?;
        out.push((ts_ms / 1000, close));
    }
    let oldest = arr.last()?.get("timestamp")?.as_u64()? / 1000;
    Some((out, oldest))
}

pub(crate) struct UpbitPager {
    pub(crate) market: &'static str,
    pub(crate) done: bool,
    /// Upper bound (inclusive, in unix seconds). `None` = real `now()`.
    pub(crate) upper_ts: Option<u64>,
    /// Lower bound; backward pagination stops when oldest returned ≤ this.
    pub(crate) lower_ts: Option<u64>,
    /// Number of parallel shards; multiplies `min_interval`.
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// Backward-paginated: returns `PAGE_SIZE` candles strictly older than the
/// `to` parameter, newest-first, minute-aligned.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let to_idx = url.find("to=").expect("upbit URL must carry to=") + 3;
    let to_str = &url[to_idx..];
    let ts = upbit_iso_to_ts(to_str);
    let aligned = ts / 60 * 60;
    let mut rows = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 1..=PAGE_SIZE {
        let cts = aligned.saturating_sub(i * 60);
        rows.push(serde_json::json!({
            "market": "KRW-BTC",
            "candle_date_time_utc": ts_to_upbit_iso(cts),
            "candle_date_time_kst": ts_to_upbit_iso(cts + 9 * 3600),
            "opening_price": 119_000_000.0,
            "high_price": 119_100_000.0,
            "low_price": 118_900_000.0,
            "trade_price": 119_050_000.0,
            "timestamp": cts * 1000,
            "candle_acc_trade_price": 0.0,
            "candle_acc_trade_volume": 0.0,
            "unit": 1
        }));
    }
    serde_json::json!(rows)
}

#[cfg(test)]
fn upbit_iso_to_ts(s: &str) -> u64 {
    // Accept both `YYYY-MM-DDTHH:MM:SS` and a URL-encoded form (`%3A` for `:`).
    let s = s.replace("%3A", ":");
    let year: i64 = s[0..4].parse().unwrap();
    let month: u64 = s[5..7].parse().unwrap();
    let day: u64 = s[8..10].parse().unwrap();
    let hour: u64 = s[11..13].parse().unwrap();
    let min: u64 = s[14..16].parse().unwrap();
    let sec: u64 = s[17..19].parse().unwrap();
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146097 + doe as i64 - 719468) as u64;
    days * 86400 + hour * 3600 + min * 60 + sec
}

#[async_trait]
impl PagedFetch for UpbitPager {
    fn name(&self) -> &'static str {
        "upbit"
    }

    fn init_cursor(&self, _from_ts: u64) -> u64 {
        // Cursor is a unix-second timestamp; start at the upper bound.
        self.upper_ts.unwrap_or_else(now)
    }

    fn check_stop(&self, _cursor: u64) -> bool {
        self.done
    }

    fn url(&self, cursor: u64) -> String {
        let market = self.market;
        let to = ts_to_upbit_iso(cursor);
        format!(
            "https://api.upbit.com/v1/candles/minutes/1?market={market}&count={PAGE_SIZE}&to={to}"
        )
    }

    async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
        // Upbit's documented rate limit is 10 req/s; sharded fetches at full
        // throttle exceed it (`base = page_size * 1000 / TARGET ≈ 69 ms` →
        // ~14.5 req/s aggregate). When we trip it the response is
        // `{"name":"too_many_requests"}` (or an `error` envelope on some
        // routes); back off and let `run_paged_fetch` retry.
        tokio::time::timeout(Duration::from_secs(5), async {
            let json: serde_json::Value = super::HTTP_CLIENT
                .get(url)
                .header(reqwest::header::ACCEPT, "application/json")
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            let throttled = json
                .get("name")
                .and_then(|v| v.as_str())
                .map(|n| n == "too_many_requests")
                .unwrap_or(false)
                || json
                    .get("error")
                    .and_then(|e| e.get("name"))
                    .and_then(|v| v.as_str())
                    .map(|n| n == "too_many_requests")
                    .unwrap_or(false);
            if throttled {
                tokio::time::sleep(Duration::from_secs(2)).await;
                return None;
            }
            Some(json)
        })
        .await
        .ok()?
    }

    fn parse(&self, json: &Json, _from_ts: u64, _cursor: u64) -> Option<PageResult> {
        let (candles, oldest) = parse_candles(json)?;
        Some(PageResult {
            candles,
            next_cursor: oldest,
        })
    }

    fn on_page(&mut self, page: &PageResult, from_ts: u64, _cursor: u64) {
        let lower = self.lower_ts.unwrap_or(from_ts);
        self.done = page.candles.is_empty() || page.next_cursor <= lower;
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
impl HistoricalSource for Upbit {
    fn name(&self) -> &'static str {
        "upbit"
    }

    fn supports(&self, currency: Currency) -> bool {
        upbit_market(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 200 candles/req × 8 req/s sustained (10 req/s docs, 25 % safety
        // margin). This is the slowest source overall — Upbit caps the
        // KRW backfill TARGET.
        1_600
    }

    fn make_pager(
        &self,
        currency: Currency,
        lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let market = upbit_market(currency)?;
        Some(Box::new(UpbitPager {
            market,
            done: false,
            upper_ts: Some(hi),
            lower_ts: Some(lo),
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
    fn ts_to_upbit_iso_known() {
        assert_eq!(ts_to_upbit_iso(0), "1970-01-01T00:00:00");
        assert_eq!(ts_to_upbit_iso(1714521600), "2024-05-01T00:00:00");
        // Upbit launch: 2017-10-29
        assert_eq!(ts_to_upbit_iso(1509235200), "2017-10-29T00:00:00");
    }

    #[test]
    fn iso_round_trip() {
        for ts in [0u64, 60, 86_400, 1_714_521_600, 1_777_890_476] {
            let s = ts_to_upbit_iso(ts);
            assert_eq!(upbit_iso_to_ts(&s), ts);
        }
    }

    #[test]
    fn parse_candles_fixture() {
        // Upbit minute-candle response is an array of objects, sorted newest-first.
        let fixture = json!([
            {
                "market": "KRW-BTC",
                "candle_date_time_utc": "2024-05-01T00:02:00",
                "trade_price": 90_000_000.0,
                "timestamp": 1714521720000u64
            },
            {
                "market": "KRW-BTC",
                "candle_date_time_utc": "2024-05-01T00:01:00",
                "trade_price": 89_950_000.0,
                "timestamp": 1714521660000u64
            },
            {
                "market": "KRW-BTC",
                "candle_date_time_utc": "2024-05-01T00:00:00",
                "trade_price": 89_900_000.0,
                "timestamp": 1714521600000u64
            }
        ]);

        let (candles, oldest) = parse_candles(&fixture).expect("should parse");
        assert_eq!(candles.len(), 3);
        // Newest first
        assert_eq!(candles[0].0, 1714521720);
        assert!((candles[0].1 - 90_000_000.0).abs() < 0.001);
        assert_eq!(candles[2].0, 1714521600);
        assert_eq!(oldest, 1714521600);
    }

    #[test]
    fn parse_candles_empty() {
        let fixture = json!([]);
        let (candles, oldest) = parse_candles(&fixture).expect("empty array should parse");
        assert!(candles.is_empty());
        assert_eq!(oldest, u64::MAX);
    }

    #[test]
    fn parse_candles_non_array() {
        let fixture = json!({"error": {"name": "invalid_request"}});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_krw_only() {
        let src = Upbit;
        assert!(src.supports(Currency::Krw));
        assert!(!src.supports(Currency::Usd));
        assert!(!src.supports(Currency::Eur));
        assert!(!src.supports(Currency::Jpy));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_krw_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["upbit"]);
        let src = Upbit;
        let from = now() - 2 * 24 * 3600;

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        src.fetch(Currency::Krw, from, tx).await.unwrap();

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
                *p > 10_000_000.0 && *p < 1_000_000_000.0,
                "implausible KRW price {p}"
            );
        }
    }
}
