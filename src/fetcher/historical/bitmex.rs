use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as Json;

use crate::{currency::Currency, fetcher::sources};

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct BitMex;

const PAGE_SIZE: u64 = 12_000;

/// Convert a unix timestamp to ISO 8601 with minute precision for BitMEX.
fn unix_to_iso8601(ts: u64) -> String {
    let secs_of_day = ts % 86400;
    let hour = (secs_of_day / 3600) as u32;
    let min = ((secs_of_day % 3600) / 60) as u32;
    let days = (ts / 86400) as i64;
    // Julian Day Number conversion
    let jdn = days + 2_440_588;
    let a = jdn + 32044;
    let b = (4 * a + 3) / 146097;
    let c = a - (146097 * b) / 4;
    let d = (4 * c + 3) / 1461;
    let e = c - (1461 * d) / 4;
    let m = (5 * e + 2) / 153;
    let day = (e - (153 * m + 2) / 5 + 1) as u32;
    let month = (m + 3 - 12 * (m / 10)) as u32;
    let year = (100 * b + d - 4800 + m / 10) as u32;
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{min:02}:00.000Z")
}

/// Parse a BitMEX ISO 8601 timestamp to unix seconds (minute precision).
fn iso8601_to_unix(s: &str) -> Option<u64> {
    if s.len() < 16 {
        return None;
    }
    let year: u32 = s[0..4].parse().ok()?;
    let month: u32 = s[5..7].parse().ok()?;
    let day: u32 = s[8..10].parse().ok()?;
    let hour: u32 = s[11..13].parse().ok()?;
    let min: u32 = s[14..16].parse().ok()?;
    let days = days_since_epoch(year, month, day)?;
    Some(days * 86400 + hour as u64 * 3600 + min as u64 * 60)
}

fn days_since_epoch(year: u32, month: u32, day: u32) -> Option<u64> {
    let y = year as i64;
    let m = month as i64;
    let d = day as i64;
    let a = (14 - m) / 12;
    let y_adj = y + 4800 - a;
    let m_adj = m + 12 * a - 3;
    let jdn =
        d + (153 * m_adj + 2) / 5 + 365 * y_adj + y_adj / 4 - y_adj / 100 + y_adj / 400 - 32045;
    let epoch_jdn = 2_440_588i64;
    if jdn < epoch_jdn {
        return None;
    }
    Some((jdn - epoch_jdn) as u64)
}

fn parse_candles(json: &Json) -> Option<Vec<(u64, f64)>> {
    let arr = json.as_array()?;
    let candles = arr
        .iter()
        .filter_map(|item| {
            let ts_str = item.get("timestamp")?.as_str()?;
            let ts = iso8601_to_unix(ts_str)?;
            let close = item.get("close")?.as_f64()?;
            Some((ts, close))
        })
        .collect();
    Some(candles)
}

pub(crate) struct BitMexPager {
    pub(crate) pair: &'static str,
    pub(crate) upper_ts: Option<u64>,
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL has `startTime={ISO}&endTime={ISO}`; returns `PAGE_SIZE` candles ascending
/// from start.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let start_idx = url.find("startTime=").unwrap() + 10;
    let amp = url[start_idx..].find('&').unwrap();
    let start_iso = &url[start_idx..start_idx + amp];
    let cursor = iso8601_to_unix(start_iso).unwrap();
    let mut candles = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 0..PAGE_SIZE {
        let ts = cursor + i * 60;
        candles.push(serde_json::json!({
            "timestamp": unix_to_iso8601(ts),
            "symbol": "XBTUSD",
            "open": 100.0,
            "high": 200.0,
            "low": 50.0,
            "close": 150.0
        }));
    }
    serde_json::json!(candles)
}

#[async_trait]
impl PagedFetch for BitMexPager {
    fn name(&self) -> &'static str {
        "bitmex"
    }

    fn init_cursor(&self, from_ts: u64) -> u64 {
        from_ts
    }

    fn check_stop(&self, cursor: u64) -> bool {
        cursor > self.upper_ts.unwrap_or_else(now)
    }

    fn url(&self, cursor: u64) -> String {
        let upper = self.upper_ts.unwrap_or_else(now);
        let window_end = (cursor + PAGE_SIZE * 60 - 60).min(upper);
        let (si, ei) = (unix_to_iso8601(cursor), unix_to_iso8601(window_end));
        let pair = self.pair;
        format!(
            "https://www.bitmex.com/api/v1/trade/bucketed\
             ?binSize=1m&partial=false&symbol={pair}\
             &startTime={si}&endTime={ei}&count={PAGE_SIZE}&reverse=false"
        )
    }

    async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let resp = super::HTTP_CLIENT.get(url).send().await.ok()?;
            if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
                tokio::time::sleep(Duration::from_secs(2)).await;
                return None;
            }
            resp.json().await.ok()
        })
        .await
        .ok()?
    }

    fn parse(&self, json: &Json, _from_ts: u64, cursor: u64) -> Option<PageResult> {
        let candles = parse_candles(json)?;
        let next_cursor = cursor + PAGE_SIZE * 60;
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
impl HistoricalSource for BitMex {
    fn name(&self) -> &'static str {
        "bitmex"
    }

    fn supports(&self, currency: Currency) -> bool {
        sources::bitmex_pair(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 12 000 candles/req × 3 req/s (180 req/min, header-confirmed).
        36_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        _lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let pair = sources::bitmex_pair(currency)?;
        Some(Box::new(BitMexPager {
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
    use crate::fetcher::historical::now;

    #[test]
    fn iso_roundtrip() {
        // 2024-04-30T08:00:00Z
        let ts = 1_714_464_000u64;
        assert_eq!(unix_to_iso8601(ts), "2024-04-30T08:00:00.000Z");
        assert_eq!(iso8601_to_unix("2024-04-30T08:00:00.000Z"), Some(ts));
    }

    #[test]
    fn iso_roundtrip_with_minutes() {
        // 2024-04-30T08:27:00Z = 1714464000 + 27*60 = 1714465620
        let ts = 1_714_465_620u64;
        assert_eq!(unix_to_iso8601(ts), "2024-04-30T08:27:00.000Z");
        assert_eq!(iso8601_to_unix("2024-04-30T08:27:00.000Z"), Some(ts));
    }

    #[test]
    fn iso_roundtrip_epoch() {
        let ts = 0u64;
        assert_eq!(unix_to_iso8601(ts), "1970-01-01T00:00:00.000Z");
        assert_eq!(iso8601_to_unix("1970-01-01T00:00:00.000Z"), Some(ts));
    }

    #[test]
    fn iso_roundtrip_midnight() {
        // 2023-01-01T00:00:00Z = 1672531200
        let ts = 1_672_531_200u64;
        let s = unix_to_iso8601(ts);
        assert_eq!(s, "2023-01-01T00:00:00.000Z");
        assert_eq!(iso8601_to_unix(&s), Some(ts));
    }

    #[test]
    fn parse_candles_fixture() {
        let json = serde_json::json!([
            {"timestamp": "2024-04-30T08:00:00.000Z", "symbol": "XBTUSD", "open": 60000.0, "close": 60050.5, "high": 60100.0, "low": 59900.0},
            {"timestamp": "2024-04-30T08:01:00.000Z", "symbol": "XBTUSD", "open": 60050.5, "close": 60150.25, "high": 60200.0, "low": 59950.0}
        ]);
        let candles = parse_candles(&json).expect("should parse");
        assert_eq!(candles.len(), 2);

        let ts0 = iso8601_to_unix("2024-04-30T08:00:00.000Z").unwrap();
        assert_eq!(candles[0].0, ts0);
        assert!((candles[0].1 - 60050.5).abs() < 0.001);

        let ts1 = ts0 + 60;
        assert_eq!(candles[1].0, ts1);
        assert!((candles[1].1 - 60150.25).abs() < 0.001);

        // 1-minute interval
        assert_eq!(candles[1].0 - candles[0].0, 60);
    }

    #[test]
    fn parse_candles_empty() {
        let json = serde_json::json!([]);
        let candles = parse_candles(&json).expect("empty array should parse");
        assert!(candles.is_empty());
    }

    #[test]
    fn parse_candles_non_array() {
        let json = serde_json::json!({"error": "some error"});
        assert!(parse_candles(&json).is_none());
    }

    #[test]
    fn iso8601_ignores_fractional_seconds() {
        let ts = iso8601_to_unix("2024-04-30T08:00:00.000Z").unwrap();
        let ts2 = iso8601_to_unix("2024-04-30T08:00:00Z").unwrap();
        assert_eq!(ts, ts2);
    }

    #[test]
    fn supports_usd_eur_only() {
        let src = BitMex;
        assert!(src.supports(Currency::Usd));
        assert!(src.supports(Currency::Eur));
        assert!(!src.supports(Currency::Jpy));
        assert!(!src.supports(Currency::Gbp));
        assert!(!src.supports(Currency::Cad));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_usd_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["bitmex"]);
        let src = BitMex;
        let from = (now() - 2 * 24 * 3600) / 60 * 60; // align to minute

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
            assert!(*p > 1_000.0 && *p < 1_000_000.0, "implausible price {p}");
        }
    }
}
