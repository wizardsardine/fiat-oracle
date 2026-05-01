//! `SpotSource` — single-shot "give me the current price" wrapper around
//! `HistoricalSource`.
//!
//! The production live-poll path (`Fetcher::spawn_live_poll`) does **not**
//! use this module — it calls `historical::run_backfill` directly with a
//! recent window so live writes are byte-for-byte reproducible by a later
//! `--rescan-from`.
//!
//! `SpotSource` exists as a convenience for tests and ad-hoc external use:
//! "give me the close of the most-recently-finalised 1m candle for X".
//! Internally it delegates to the same `HistoricalSource` implementation
//! used by backfill, so its output is also reproducible.

use tokio::sync::mpsc;

use crate::currency::Currency;
use crate::fetcher::historical::{self, HistoricalSource};

/// Returns the BitMEX instrument symbol for `currency`, or `None` if unsupported.
///
/// XBTJPY: instrument lookup returns `[]` and `/trade/bucketed` returns empty
/// for any recent date. The contract is delisted; excluded entirely.
pub fn bitmex_pair(currency: Currency) -> Option<&'static str> {
    match currency {
        Currency::Eur => Some("XBTEUR"),
        Currency::Usd => Some("XBTUSD"),
        _ => None,
    }
}

/// One variant per supported exchange. Each variant maps 1:1 to a
/// `HistoricalSource`; the spot wrapper here just calls that source's
/// fetch path for the most recent finalised 1m candle.
///
/// A currency is only marked as supported (v) if the source can provide
/// historical OHLC for that currency (and therefore — under the wrapper
/// design — also a "spot" reading reproducible by backfill).
///
/// | Source   | EUR | USD | GBP | JPY | KRW |
/// |----------|-----|-----|-----|-----|-----|
/// | Bitstamp |  v  |  v  |  v  |     |     |
/// | Bitfinex |  v  |  v  |  v  |     |     |
/// | Coinbase |  v  |  v  |  v  |     |     |
/// | Kucoin   |  v  |  v  |     |     |     |
/// | Okx      |  v  |  v  |     |     |     |
/// | Bitmex   |  v  |  v  |     |     |     |
/// | Binance  |  v  |  v  |     |  v  |     |
/// | Bitbank  |     |     |     |  v  |     |
/// | Upbit    |     |     |     |     |  v  |
///
/// `Currency::Cad` is intentionally absent: no source has a real BTC/CAD
/// market with a deep enough archive.
///
/// Pairs dropped because their historical OHLC has been frozen / removed:
/// - Binance BTCGBP — symbol status `BREAK`, last klines candle 2023-12-29.
/// - Bitfinex tBTCJPY — no longer in the active pair config.
/// - BitMEX XBTJPY — instrument lookup returns `[]`. Delisted.
/// - Kraken (every currency) — `/public/OHLC` retains only ~1000 candles
///   per interval; can't reach the 1-year backfill target.
/// - Bithumb — historical impl uses a 1h endpoint synthesised to 1m, so
///   live (1m) and backfill (1h-synth) can't agree. Dropped to keep the
///   "live and backfill produce the same data" contract.
// Production live-poll path uses `historical::run_backfill` directly and
// doesn't go through `SpotSource`. Allowed dead until the wrapper is wired
// to a real caller (e.g. an HTTP endpoint serving "current price").
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpotSource {
    Bitstamp,
    Bitfinex,
    Coinbase,
    Kucoin,
    Okx,
    Bitmex,
    Binance,
    Bitbank,
    Upbit,
}

#[allow(dead_code)] // see enum-level allow above
impl SpotSource {
    /// Returns whether this source supports `currency`. Delegates to the
    /// wrapped `HistoricalSource`.
    pub fn supports(self, currency: Currency) -> bool {
        self.as_historical().supports(currency)
    }

    /// Fetch the close of the most-recently-finalised 1m candle for
    /// `currency`. Returns `(candle_ts, close)` or `None` if the request /
    /// parse failed or no recent candle was available.
    ///
    /// Reads the last 3 minutes from the same OHLC endpoint as backfill —
    /// one of those minutes will be the most recent finalised one (T-1 or
    /// T-2 depending on the source's publishing delay).
    pub async fn fetch(self, currency: Currency) -> Option<(u64, f64)> {
        let source = self.as_historical();
        if !source.supports(currency) {
            return None;
        }
        let now = historical::now();
        let from_ts = now.saturating_sub(3 * 60);
        let (tx, mut rx) = mpsc::unbounded_channel();
        if source.fetch(currency, from_ts, tx).await.is_err() {
            return None;
        }
        let mut latest: Option<(u64, f64)> = None;
        while let Some(batch) = rx.recv().await {
            for (ts, p) in batch {
                if latest.map(|(t, _)| ts > t).unwrap_or(true) {
                    latest = Some((ts, p));
                }
            }
        }
        latest
    }

    /// Map a `SpotSource` variant to its corresponding `HistoricalSource`.
    fn as_historical(self) -> Box<dyn HistoricalSource> {
        match self {
            SpotSource::Bitstamp => Box::new(historical::bitstamp::Bitstamp),
            SpotSource::Bitfinex => Box::new(historical::bitfinex::Bitfinex),
            SpotSource::Coinbase => Box::new(historical::coinbase::Coinbase),
            SpotSource::Kucoin => Box::new(historical::kucoin::Kucoin),
            SpotSource::Okx => Box::new(historical::okx::Okx),
            SpotSource::Bitmex => Box::new(historical::bitmex::BitMex),
            SpotSource::Binance => Box::new(historical::binance::Binance),
            SpotSource::Bitbank => Box::new(historical::bitbank::Bitbank),
            SpotSource::Upbit => Box::new(historical::upbit::Upbit),
        }
    }
}

#[allow(dead_code)] // see enum-level allow above
pub const ALL_SOURCES: &[SpotSource] = &[
    SpotSource::Bitstamp,
    SpotSource::Bitfinex,
    SpotSource::Coinbase,
    SpotSource::Kucoin,
    SpotSource::Okx,
    SpotSource::Bitmex,
    SpotSource::Binance,
    SpotSource::Bitbank,
    SpotSource::Upbit,
];
