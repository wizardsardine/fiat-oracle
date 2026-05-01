// Production live-poll path uses `historical::run_backfill` directly (see
// `Fetcher::spawn_live_poll`). The functions in this module are kept as a
// single-shot "current price" convenience for tests and ad-hoc external
// callers; allowed dead until wired to a real consumer.
#![allow(dead_code)]

use std::collections::BTreeMap;

use tokio::task::JoinSet;

use super::sources::ALL_SOURCES;
use crate::currency::Currency;
use crate::error::{Error, Result};

/// Minimum number of price sources required to compute a valid median.
const MIN_PRICE_SOURCES: usize = 3;

/// Get the BTC spot price in `currency` as the median across all supporting
/// exchanges. Returns the **(candle timestamp, median close)** for the most
/// recent finalised 1-minute candle that reaches `MIN_PRICE_SOURCES` quorum
/// across the spot sources.
///
/// This API is **not** on the production live-poll path — `Fetcher::spawn_live_poll`
/// uses `historical::run_backfill` directly so the live writes are byte-for-byte
/// reproducible by a later `--rescan-from`. `median_spot_price` is kept as a
/// single-shot convenience for tests / external callers.
///
/// Sources that don't support the requested currency, fail their request, or
/// don't share a candle timestamp with at least `MIN_PRICE_SOURCES - 1` other
/// contributors are silently skipped.
pub async fn median_spot_price(currency: Currency) -> Result<(u64, f64)> {
    let mut set = JoinSet::new();
    for &source in ALL_SOURCES {
        if source.supports(currency) {
            set.spawn(async move { (source, source.fetch(currency).await) });
        }
    }

    // Group prices by candle timestamp; pick the most recent ts that
    // reaches MIN_PRICE_SOURCES quorum. Different sources may publish their
    // most-recent finalised candle at slightly different ts (depending on
    // per-source publish delay), so a strict "all agree on the latest ts"
    // would fail more often than necessary.
    let mut by_ts: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
    while let Some(res) = set.join_next().await {
        match res {
            Ok((_, Some((ts, p)))) => by_ts.entry(ts).or_default().push(p),
            Ok((source, None)) => {
                tracing::warn!(?source, ?currency, "spot source fetch failed")
            }
            Err(e) => tracing::warn!(?currency, ?e, "spot source task panicked"),
        }
    }

    for (ts, prices) in by_ts.into_iter().rev() {
        if prices.len() >= MIN_PRICE_SOURCES {
            return Ok((ts, median(prices)));
        }
    }

    tracing::warn!(?currency, "insufficient spot sources at any recent ts");
    Err(Error::InsufficientSources(MIN_PRICE_SOURCES))
}

fn median(mut prices: Vec<f64>) -> f64 {
    prices.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    prices[prices.len() / 2]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_median_3() {
        assert_eq!(median(vec![30.0, 10.0, 20.0]), 20.0);
    }

    #[test]
    fn test_median_4() {
        assert_eq!(median(vec![40.0, 10.0, 30.0, 20.0]), 30.0);
    }

    #[test]
    fn test_median_5() {
        assert_eq!(median(vec![50.0, 10.0, 30.0, 20.0, 40.0]), 30.0);
    }

    // Integration tests — run with: cargo test -- --ignored

    #[tokio::test]
    #[ignore]
    async fn test_median_spot_price_eur() {
        let _eg = crate::fetcher::historical::exchange_locks(&[
            "bitfinex", "bitmex", "bitstamp", "coinbase", "kucoin", "okx",
        ]);
        let (ts, price) = median_spot_price(Currency::Eur)
            .await
            .expect("Failed to fetch EUR median price");
        assert!(
            price > 10_000.0 && price < 1_000_000.0,
            "Price {price} out of range"
        );
        let now = crate::fetcher::historical::now();
        assert!(
            ts <= now && ts + 5 * 60 >= now,
            "median ts {ts} should be within ~5 minutes of now ({now})"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn test_median_spot_price_usd() {
        let _eg = crate::fetcher::historical::exchange_locks(&[
            "bitfinex", "bitmex", "bitstamp", "coinbase", "kucoin", "okx",
        ]);
        let (ts, price) = median_spot_price(Currency::Usd)
            .await
            .expect("Failed to fetch USD median price");
        assert!(
            price > 10_000.0 && price < 1_000_000.0,
            "Price {price} out of range"
        );
        let now = crate::fetcher::historical::now();
        assert!(
            ts <= now && ts + 5 * 60 >= now,
            "median ts {ts} should be within ~5 minutes of now ({now})"
        );
    }
}
