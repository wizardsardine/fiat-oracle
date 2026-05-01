use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as Json;

use crate::currency::Currency;

use super::{now, HistoricalSource, PageResult, PagedFetch};

pub struct Okx;

const PAGE_SIZE: u64 = 300;

/// Returns the OKX instId for `currency`, or `None` if unsupported.
/// USD is proxied via USDT; OKX does not list a BTC-USD pair.
fn okx_inst_id(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Eur => Some("BTC-EUR"),
        Currency::Usd => Some("BTC-USDT"),
        _ => None,
    }
}

/// Returns `(candles sorted newest-first, oldest_ts_ms)` or `None` on error.
///
/// OKX candle layout: `[ts_ms, o, h, l, c, vol, volCcy, volCcyQuote, confirm]` -- all strings.
fn parse_candles(json: &Json) -> Option<(Vec<(u64, f64)>, u64)> {
    if json.get("code")?.as_str()? != "0" {
        return None;
    }
    let data = json.get("data")?.as_array()?;
    if data.is_empty() {
        return Some((vec![], u64::MAX));
    }
    let mut out = Vec::with_capacity(data.len());
    for row in data {
        let arr = row.as_array()?;
        let ts_ms: u64 = arr.first()?.as_str()?.parse().ok()?;
        let close: f64 = arr.get(4)?.as_str()?.parse().ok()?;
        out.push((ts_ms / 1000, close));
    }
    let oldest_ms: u64 = data.last()?.as_array()?.first()?.as_str()?.parse().ok()?;
    Some((out, oldest_ms))
}

pub(crate) struct OkxPager {
    pub(crate) inst_id: &'static str,
    pub(crate) done: bool,
    /// Upper bound (inclusive, in unix seconds). `None` = use real `now()`.
    pub(crate) upper_ts: Option<u64>,
    /// Lower bound (in unix seconds). `None` = use the `from_ts` passed into
    /// the run loop. Backward pagination stops when next_cursor/1000 ≤ this.
    pub(crate) lower_ts: Option<u64>,
    /// Number of parallel shards this fetch uses; multiplies `min_interval`.
    pub(crate) concurrency: u32,
    pub(crate) target_candles_per_sec: u64,
}

/// Canned response for cross-source pacing tests.
/// URL: `...&after={cursor_ms}&limit=300`. OKX is backward-paginated:
/// returns `PAGE_SIZE` minute-aligned candles strictly older than cursor_ms,
/// newest-first. Real OKX returns candles at minute boundaries; aligning
/// the mock matches that behaviour and prevents fractional-second drift
/// from making adjacent shards overlap at their boundary.
#[cfg(test)]
pub(crate) fn mock_response(url: &str) -> serde_json::Value {
    let after_idx = url.find("after=").unwrap() + 6;
    let amp = url[after_idx..].find('&').unwrap_or(url.len() - after_idx);
    let cursor_ms: u64 = url[after_idx..after_idx + amp].parse().unwrap();
    // Align to the nearest minute boundary at-or-below the cursor — real OKX
    // candle timestamps are always at minute boundaries.
    let aligned_ms = cursor_ms / 60_000 * 60_000;
    let mut candles = Vec::with_capacity(PAGE_SIZE as usize);
    for i in 1..=PAGE_SIZE {
        let ts_ms = aligned_ms.saturating_sub(i * 60_000);
        candles.push(serde_json::json!([
            ts_ms.to_string(),
            "100.0",
            "200.0",
            "50.0",
            "150.0",
            "1.0",
            "1.0",
            "150.0",
            "1"
        ]));
    }
    serde_json::json!({ "code": "0", "data": candles })
}

#[async_trait]
impl PagedFetch for OkxPager {
    fn name(&self) -> &'static str {
        "okx"
    }

    fn init_cursor(&self, _from_ts: u64) -> u64 {
        (self.upper_ts.unwrap_or_else(now) + 1) * 1000
    }

    fn check_stop(&self, _cursor_ms: u64) -> bool {
        self.done
    }

    fn url(&self, cursor_ms: u64) -> String {
        let inst_id = self.inst_id;
        format!(
            "https://www.okx.com/api/v5/market/history-candles\
             ?instId={inst_id}&bar=1m&after={cursor_ms}&limit=300"
        )
    }

    async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let json: serde_json::Value = super::HTTP_CLIENT
                .get(url)
                .send()
                .await
                .ok()?
                .json()
                .await
                .ok()?;
            if json.get("code").and_then(|c| c.as_str()) == Some("50011") {
                tokio::time::sleep(Duration::from_secs(2)).await;
                return None;
            }
            Some(json)
        })
        .await
        .ok()?
    }

    fn parse(&self, json: &Json, _from_ts: u64, _cursor_ms: u64) -> Option<PageResult> {
        let (candles, oldest_ms) = parse_candles(json)?;
        Some(PageResult {
            candles,
            next_cursor: oldest_ms,
        })
    }

    fn on_page(&mut self, page: &PageResult, from_ts: u64, _cursor: u64) {
        let lower = self.lower_ts.unwrap_or(from_ts);
        self.done = page.candles.is_empty() || page.next_cursor / 1000 <= lower;
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
impl HistoricalSource for Okx {
    fn name(&self) -> &'static str {
        "okx"
    }

    fn supports(&self, currency: Currency) -> bool {
        okx_inst_id(currency).is_some()
    }

    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        // 300 candles/req × 20 req/s (40 req per 2 s public IP cap).
        6_000
    }

    fn make_pager(
        &self,
        currency: Currency,
        lo: u64,
        hi: u64,
        concurrency: u32,
        target_candles_per_sec: u64,
    ) -> Option<super::BoxedPager> {
        let inst_id = okx_inst_id(currency)?;
        Some(Box::new(OkxPager {
            inst_id,
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
    fn parse_candles_success() {
        let fixture = json!({
            "code": "0",
            "data": [
                ["1714000300000", "60200.00", "60300.00", "60100.00", "60250.00", "5.0", "5.0", "301250.0", "1"],
                ["1714000240000", "60100.00", "60200.00", "60000.00", "60150.00", "4.0", "4.0", "240600.0", "1"]
            ]
        });

        let (candles, oldest_ms) = parse_candles(&fixture).expect("should parse");
        assert_eq!(candles.len(), 2);
        assert_eq!(candles[0].0, 1714000300);
        assert!((candles[0].1 - 60250.0).abs() < 0.001);
        assert_eq!(candles[1].0, 1714000240);
        assert!((candles[1].1 - 60150.0).abs() < 0.001);
        // 1-minute interval between candles
        assert_eq!(candles[0].0 - candles[1].0, 60);
        assert_eq!(oldest_ms, 1714000240000);
    }

    #[test]
    fn parse_candles_empty_data() {
        let fixture = json!({"code": "0", "data": []});
        let (candles, oldest_ms) = parse_candles(&fixture).expect("should parse empty");
        assert!(candles.is_empty());
        assert_eq!(oldest_ms, u64::MAX);
    }

    #[test]
    fn parse_candles_error_code() {
        let fixture = json!({"code": "51001", "data": [], "msg": "Instrument ID does not exist"});
        assert!(parse_candles(&fixture).is_none());
    }

    #[test]
    fn supports_eur_and_usd_only() {
        let src = Okx;
        assert!(src.supports(Currency::Eur));
        assert!(src.supports(Currency::Usd));
        assert!(!src.supports(Currency::Gbp));
        assert!(!src.supports(Currency::Jpy));
    }

    #[tokio::test]
    #[ignore]
    async fn live_fetch_eur_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["okx"]);
        let src = Okx;
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

    #[tokio::test]
    #[ignore]
    async fn live_fetch_usd_48h() {
        let _eg = crate::fetcher::historical::exchange_locks(&["okx"]);
        let src = Okx;
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
