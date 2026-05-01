use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Coinbase;

const PAGE_SIZE: u64 = 300;
const WINDOW: u64 = PAGE_SIZE * 60;

fn coinbase_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Usd => Some("BTC-USD"),
        Currency::Eur => Some("BTC-EUR"),
        Currency::Gbp => Some("BTC-GBP"),
        _ => None,
    }
}

/// Convert unix seconds to civil (year, month, day).
/// Uses the Howard Hinnant civil_from_days algorithm.
fn days_to_ymd(days: u32) -> (u32, u32, u32) {
    let z = days as i64 + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = (z - era * 146097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = if m <= 2 { y + 1 } else { y };
    (year as u32, m as u32, d as u32)
}

fn ts_to_iso8601(ts: u64) -> String {
    let secs_of_day = ts % 86400;
    let sec = (secs_of_day % 60) as u32;
    let min = ((secs_of_day % 3600) / 60) as u32;
    let hour = (secs_of_day / 3600) as u32;
    let days = (ts / 86400) as u32;
    let (year, month, day) = days_to_ymd(days);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:{sec:02}Z")
}

/// Parses Coinbase exchange candles: `[[ts, low, high, open, close, volume], ...]`.
/// Returns `None` if `json` is not an array (API error response).
fn parse_candles(json: &Json) -> Option<Vec<(u64, f64)>> {
    let arr = json.as_array()?;

    let candles: Vec<(u64, f64)> = arr
        .iter()
        .filter_map(|candle| {
            let inner = candle.as_array()?;
            let ts: u64 = inner.first()?.as_u64()?;
            let close: f64 = inner.get(4)?.as_f64()?;
            Some((ts, close))
        })
        .collect();

    Some(candles)
}

/// End of the URL window for a given cursor and current time.
fn url_end_pure(cursor: u64, now: u64) -> u64 {
    now.min(cursor + WINDOW)
}

/// Cursor for the next iteration after fetching a page that started at `cursor`.
fn next_cursor_pure(cursor: u64, now: u64) -> u64 {
    now.min(cursor + WINDOW) + 1
}

/// Build a canned JSON response for a Coinbase URL, used in cross-source
/// pacing tests. Extracts the start/end window from the URL and emits
/// minute-aligned candles in [start, end].
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("start=").expect("start in url") + 6;
    let amp = url[start_idx..].find('&').unwrap();
    let start_iso = &url[start_idx..start_idx + amp];
    let end_idx = url.find("end=").expect("end in url") + 4;
    let end_iso = &url[end_idx..];
    let start = iso8601_to_ts(start_iso);
    let end = iso8601_to_ts(end_iso);
    let first = start.div_ceil(60) * 60;
    let last = (end / 60) * 60;
    let mut candles: Vec<serde_json::Value> = Vec::new();
    let mut t = first;
    while t <= last && candles.len() < PAGE_SIZE as usize {
        candles.push(serde_json::json!([t, 100.0, 200.0, 150.0, 175.0, 1.0]));
        t += 60;
    }
    serde_json::json!(candles)
}

/// Inverse of `ts_to_iso8601` (test-only). Format is fixed `YYYY-MM-DDTHH:MM:SSZ`.
#[cfg(test)]
fn iso8601_to_ts(s: &str) -> u64 {
    let year: i64 = s[0..4].parse().unwrap();
    let month: u32 = s[5..7].parse().unwrap();
    let day: u32 = s[8..10].parse().unwrap();
    let hour: u64 = s[11..13].parse().unwrap();
    let min: u64 = s[14..16].parse().unwrap();
    let sec: u64 = s[17..19].parse().unwrap();
    // days-since-epoch via Howard-Hinnant days_from_civil
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let m = month as u64;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + day as u64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146097 + doe as i64 - 719468) as u64;
    days * 86400 + hour * 3600 + min * 60 + sec
}

pub(crate) struct CoinbasePager {
    pub(crate) pair: &'static str,
    /// Upper bound for this shard's range. `None` = use real `now()`.
    pub(crate) upper_ts: Option<u64>,
    /// Number of parallel shards this fetch uses; multiplies `min_interval`
    /// so the total per-source request rate matches the unsharded rate.
    pub(crate) concurrency: u32,
    /// Throughput target (candles/sec). Threaded in by `make_pager`.
    pub(crate) target_candles_per_sec: u64,
}

impl CoinbasePager {
    fn upper(&self) -> u64 {
        self.upper_ts.unwrap_or_else(now)
    }
}

#[async_trait]
impl PagedFetch for CoinbasePager {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts
    }

    fn check_stop(&self, cursor: u64) -> bool {
        cursor >= self.upper()
    }

    fn url(&self, cursor: u64) -> String {
        let end = url_end_pure(cursor, self.upper());
        let pair = self.pair;
        format!(
            "https://api.exchange.coinbase.com/products/{pair}/candles\
             ?granularity=60&start={}&end={}",
            ts_to_iso8601(cursor),
            ts_to_iso8601(end),
        )
    }

    async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            super::HTTP_CLIENT
                .get(url)
                .header(
                    reqwest::header::USER_AGENT,
                    "Mozilla/5.0 (X11; Linux x86_64; rv:128.0) Gecko/20100101 Firefox/128.0",
                )
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()
        })
        .await
        .ok()?
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_candles(json)?;
        let next_cursor = next_cursor_pure(cursor, self.upper());
        Some(PageResult {
            candles,
            next_cursor,
        })
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
impl HistoricalSource for Coinbase {
    fn name(&self) -> &'static str {
        "coinbase"
    }

    fn supports(&self, currency: Currency) -> bool {
        coinbase_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 300 candles/req × 10 req/s unauthenticated IP cap = 3 000 c/s.
        // This is the slowest non-rate-capped source — typically the
        // bottleneck for USD/EUR/GBP backfills.
        3_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let pair = coinbase_pair(currency)?;
        Some(Box::new(CoinbasePager {
            pair,
            upper_ts: Some(hi),
            concurrency,
            target_candles_per_sec,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fetcher::historical::{now, run_paged_fetch};
    use serde_json::json;

    #[test]
    fn days_to_ymd_known_dates() {
        // 2024-05-01 = day 19844 since epoch
        assert_eq!(days_to_ymd(19844), (2024, 5, 1));
        // 1970-01-01 = day 0
        assert_eq!(days_to_ymd(0), (1970, 1, 1));
        // 2000-01-01 = day 10957
        assert_eq!(days_to_ymd(10957), (2000, 1, 1));
    }

    #[test]
    fn ts_to_iso8601_known_timestamp() {
        // 2024-05-01T16:00:00Z = 1714579200
        assert_eq!(ts_to_iso8601(1714579200), "2024-05-01T16:00:00Z");
        // 1970-01-01T00:00:00Z = 0
        assert_eq!(ts_to_iso8601(0), "1970-01-01T00:00:00Z");
        // with minutes: 2024-05-01T16:27:00Z = 1714579200 + 27*60 = 1714580820
        assert_eq!(ts_to_iso8601(1714580820), "2024-05-01T16:27:00Z");
    }

    #[test]
    fn parse_candles_fixture() {
        // Coinbase candle layout: [ts, low, high, open, close, volume]
        let fixture = json!([
            [1714000000u64, 59900.0, 60100.0, 60000.0, 60050.5, 10.5],
            [1714000060u64, 59950.0, 60200.0, 60050.5, 60150.25, 8.2],
            [1714000120u64, 60000.0, 60300.0, 60150.25, 60200.0, 12.1]
        ]);

        let mut candles = parse_candles(&fixture).expect("should parse fixture");
        candles.sort_unstable_by_key(|(ts, _)| *ts);
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
    fn parse_candles_error_object_returns_none() {
        let fixture = json!({"message": "NotFound"});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_usd_eur_gbp_only() {
        let src = Coinbase;
        assert!(src.supports(Currency::Usd));
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Gbp));
        assert!(!src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Cad));
    }

    // -----------------------------------------------------------------
    // Cursor-progression tests (pure, no network, no real time)
    // -----------------------------------------------------------------

    #[test]
    fn cursor_advances_by_window_plus_one_when_not_caught_up() {
        // now is far in the future of cursor + WINDOW
        let cursor = 1_000_000_000u64;
        let now = cursor + WINDOW * 100;
        assert_eq!(url_end_pure(cursor, now), cursor + WINDOW);
        assert_eq!(next_cursor_pure(cursor, now), cursor + WINDOW + 1);
        assert_eq!(next_cursor_pure(cursor, now) - cursor, 18001);
    }

    #[test]
    fn cursor_clips_to_now_when_caught_up() {
        let cursor = 1_000_000_000u64;
        let now = cursor + 500; // less than a full window away
        assert_eq!(url_end_pure(cursor, now), now);
        assert_eq!(next_cursor_pure(cursor, now), now + 1);
    }

    #[test]
    fn cursor_progression_30_days_is_consistent() {
        // Walk through a 30-day catch-up at WINDOW=18000s using a fixed mock now.
        // After ceil(30d / WINDOW) = 144 iterations, cursor should be > now -> stop.
        let now = 2_000_000_000u64;
        let from = now - 30 * 86_400;
        let mut cursor = from;
        let mut iters = 0u64;
        while cursor < now {
            // At each step, compute next via the pure helper.
            cursor = next_cursor_pure(cursor, now);
            iters += 1;
            assert!(iters < 200, "too many iterations: {iters}");
        }
        // Each iteration except possibly the last advances by exactly WINDOW+1.
        // Total cursor advance >= 30d, total iters ~= ceil(30d / (WINDOW+1)).
        let expected_iters_min = 30 * 86_400 / (WINDOW + 1);
        let expected_iters_max = expected_iters_min + 2;
        assert!(
            (expected_iters_min..=expected_iters_max).contains(&iters),
            "iters={iters} expected in {expected_iters_min}..={expected_iters_max}"
        );
        assert!(
            cursor > now,
            "must overshoot now to terminate, got {cursor}"
        );
    }

    // -----------------------------------------------------------------
    // Full pager-loop test with mocked fetch_json (no network).
    // Verifies that run_paged_fetch produces contiguous candle coverage
    // and a finite iteration count.
    // -----------------------------------------------------------------

    fn fake_candles_for_window(start: u64, end: u64, max: usize) -> serde_json::Value {
        // Coinbase format: [ts, low, high, open, close, volume]; minute-aligned.
        let first = start.div_ceil(60) * 60; // first minute >= start
        let last = (end / 60) * 60; // last minute <= end
        let mut out: Vec<serde_json::Value> = Vec::new();
        let mut t = first;
        while t <= last && out.len() < max {
            out.push(json!([t, 100.0, 200.0, 150.0, 175.0, 1.0]));
            t += 60;
        }
        json!(out)
    }

    struct MockCoinbasePager {
        pair: &'static str,
        // Logical "now" the pager will see. We can't override the real now() helper
        // without trait surgery, but we can simulate via an injected end ceiling.
        fake_now: u64,
        urls_fetched: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl PagedFetch for MockCoinbasePager {
        fn name(&self) -> &'static str {
            "coinbase-mock"
        }
        fn init_cursor(&self, from_ts: u64) -> u64 {
            from_ts
        }
        fn check_stop(&self, cursor: u64) -> bool {
            cursor >= self.fake_now
        }
        fn url(&self, cursor: u64) -> String {
            let end = url_end_pure(cursor, self.fake_now);
            let pair = self.pair;
            format!(
                "https://api.exchange.coinbase.com/products/{pair}/candles\
                 ?granularity=60&start={}&end={}",
                ts_to_iso8601(cursor),
                ts_to_iso8601(end),
            )
        }
        async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
            self.urls_fetched.lock().unwrap().push(url.to_string());
            // Parse cursor & end out of the url (very rough, sufficient for test).
            let start_idx = url.find("start=").unwrap() + 6;
            let amp = url[start_idx..].find('&').unwrap();
            let start_iso = &url[start_idx..start_idx + amp];
            let end_idx = url.find("end=").unwrap() + 4;
            let end_iso = &url[end_idx..];
            let start = iso8601_to_ts(start_iso);
            let end = iso8601_to_ts(end_iso);
            Some(fake_candles_for_window(start, end, PAGE_SIZE as usize))
        }
        fn parse(
            &self,
            json: &serde_json::Value,
            _from_ts: u64,
            cursor: u64,
        ) -> Option<PageResult> {
            let candles = parse_candles(json)?;
            let next_cursor = next_cursor_pure(cursor, self.fake_now);
            Some(PageResult {
                candles,
                next_cursor,
            })
        }
        fn page_size(&self) -> u64 {
            PAGE_SIZE
        }
        fn target_candles_per_sec(&self) -> u64 {
            // Bypassed: this pager hard-overrides `min_interval` to 0.
            1
        }
        fn min_interval(&self) -> std::time::Duration {
            // Make the test fast — no waiting between mock pages.
            std::time::Duration::from_millis(0)
        }
    }

    #[test]
    fn iso_round_trip() {
        let ts = 1_777_800_499u64;
        let s = ts_to_iso8601(ts);
        assert_eq!(iso8601_to_ts(&s), ts);
        for &t in &[0u64, 60, 86_400, 1_714_579_200, 1_777_890_476] {
            assert_eq!(
                iso8601_to_ts(&ts_to_iso8601(t)),
                t,
                "round-trip failed for {t}"
            );
        }
    }

    #[tokio::test]
    async fn mock_loop_produces_contiguous_coverage() {
        let fake_now = 2_000_000_000u64; // arbitrary, minute-misaligned ok
        let from = fake_now - 25 * 3600; // 25 hours, mirrors the user's log

        let urls = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let mut pager = MockCoinbasePager {
            pair: "BTC-USD",
            fake_now,
            urls_fetched: urls.clone(),
        };

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        run_paged_fetch(&mut pager, from, tx).await.unwrap();

        let mut out: Vec<(u64, f64)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            out.extend(batch);
        }

        // 1) iteration count: ceil((fake_now - from) / WINDOW) ~= 5
        let urls_n = urls.lock().unwrap().len();
        let expected_iters = (fake_now - from).div_ceil(WINDOW);
        assert!(
            (expected_iters..=expected_iters + 1).contains(&(urls_n as u64)),
            "iters={urls_n} expected ~{expected_iters}"
        );

        // 2) candles must be unique-per-minute (no duplicates from boundary handling)
        let mut seen = std::collections::HashSet::new();
        for (ts, _) in &out {
            assert!(seen.insert(*ts), "duplicate candle at ts={ts}");
        }

        // 3) candles must be contiguous: every minute in (from, fake_now] is covered.
        // Allow the very-first and very-last minute to be missing (boundary fuzz).
        let first = from.div_ceil(60) * 60;
        let last = (fake_now / 60) * 60;
        let expected = (last - first) / 60 + 1;
        let coverage = out.len() as u64;
        assert!(
            coverage + 2 >= expected,
            "coverage={coverage} expected~={expected}"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_usd_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["coinbase"]);
        let src = Coinbase;
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
