//! Depth-bound regression tests — one per (source, currency) pair in scope.
//!
//! Each test makes a small live request against the source's documented
//! earliest date (or — for sliding-window sources — `now - 365 days`) and
//! asserts at least one 1-minute candle comes back.
//!
//! Failure semantics: a failing test means **upstream reality changed**
//! (pair delisted, retention shrunk, endpoint format moved). Investigate
//! before "fixing" — the right response is usually to update the asserted
//! date, drop the pair, or pick a different source.
//!
//! Tests are `#[ignore]`d by default — they hit the live exchanges.
//!   Run them on demand:  `cargo test --test depth_bounds -- --ignored`

use std::os::unix::io::AsRawFd;
use std::time::Duration;

use tokio::sync::mpsc;

use fiat_oracle::currency::Currency;
use fiat_oracle::fetcher::historical::{
    binance::Binance, bitbank::Bitbank, bitfinex::Bitfinex, bitmex::BitMex, bitstamp::Bitstamp,
    coinbase::Coinbase, kucoin::Kucoin, okx::Okx, upbit::Upbit, HistoricalSource,
};

const SECS_PER_DAY: u64 = 86_400;
/// How long to wait for the first batch from a source before declaring failure.
const FIRST_BATCH_TIMEOUT: Duration = Duration::from_secs(20);

fn iso_to_ts(s: &str) -> u64 {
    // Accept "YYYY-MM-DD" — tests use plain dates.
    let year: i64 = s[0..4].parse().unwrap();
    let month: u64 = s[5..7].parse().unwrap();
    let day: u64 = s[8..10].parse().unwrap();
    let y = if month <= 2 { year - 1 } else { year };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as u64;
    let doy = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = (era * 146097 + doe as i64 - 719468) as u64;
    days * SECS_PER_DAY
}

fn lock(name: &str) -> std::fs::File {
    let path = format!("/tmp/fiat_oracle_test_{name}.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(&path)
        .unwrap();
    let ret = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) };
    assert_eq!(
        ret,
        0,
        "flock({path}) failed: {}",
        std::io::Error::last_os_error()
    );
    // Brief pause to let the previous lock-holder's in-flight requests clear
    // any rate-limit windows.
    std::thread::sleep(Duration::from_secs(3));
    file
}

/// Spawn `src.fetch()` from `from_ts`, wait up to [`FIRST_BATCH_TIMEOUT`]
/// for any non-empty batch, then abort. Returns the count of candles in
/// the first non-empty batch, or 0 if none arrived.
///
/// We can't pass an upper bound to `fetch()` (the trait API runs to
/// `now()`), so we abort after the first hit. That's enough to verify the
/// earliest date still returns data; the depth-of-archive contract is
/// violated only when a full window comes back empty.
async fn first_batch_count(
    src: Box<dyn HistoricalSource + 'static>,
    currency: Currency,
    from_ts: u64,
) -> usize {
    let (tx, mut rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let _ = src.fetch(currency, from_ts, tx).await;
    });
    let deadline = tokio::time::Instant::now() + FIRST_BATCH_TIMEOUT;
    let mut count = 0usize;
    while count == 0 && tokio::time::Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        match tokio::time::timeout(remaining, rx.recv()).await {
            Ok(Some(batch)) => count = batch.len(),
            _ => break,
        }
    }
    task.abort();
    count
}

/// Standard fixed-archive depth test: assert that the source returns at
/// least one candle when started at `earliest_iso`.
async fn assert_archive_depth(
    src: Box<dyn HistoricalSource + 'static>,
    name: &'static str,
    currency: Currency,
    earliest_iso: &'static str,
) {
    let from = iso_to_ts(earliest_iso);
    let count = first_batch_count(src, currency, from).await;
    assert!(
        count > 0,
        "{name} {currency:?}: expected at least 1 candle starting {earliest_iso} ({from}), got 0. \
         Either the pair was delisted, retention shrank past that date, or the \
         endpoint shape changed — investigate before bumping the asserted date.",
    );
}

// -------------------------------------------------------------------------
// Bitstamp
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn bitstamp_usd_depth() {
    let _g = lock("bitstamp");
    assert_archive_depth(Box::new(Bitstamp), "bitstamp", Currency::Usd, "2012-01-01").await;
}

#[tokio::test]
#[ignore]
async fn bitstamp_eur_depth() {
    let _g = lock("bitstamp");
    assert_archive_depth(Box::new(Bitstamp), "bitstamp", Currency::Eur, "2018-01-01").await;
}

#[tokio::test]
#[ignore]
async fn bitstamp_gbp_depth() {
    let _g = lock("bitstamp");
    assert_archive_depth(Box::new(Bitstamp), "bitstamp", Currency::Gbp, "2021-06-01").await;
}

// -------------------------------------------------------------------------
// Bitfinex
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn bitfinex_usd_depth() {
    let _g = lock("bitfinex");
    assert_archive_depth(Box::new(Bitfinex), "bitfinex", Currency::Usd, "2013-04-01").await;
}

#[tokio::test]
#[ignore]
async fn bitfinex_eur_depth() {
    let _g = lock("bitfinex");
    assert_archive_depth(Box::new(Bitfinex), "bitfinex", Currency::Eur, "2018-01-01").await;
}

#[tokio::test]
#[ignore]
async fn bitfinex_gbp_depth() {
    let _g = lock("bitfinex");
    assert_archive_depth(Box::new(Bitfinex), "bitfinex", Currency::Gbp, "2019-01-01").await;
}

// -------------------------------------------------------------------------
// Coinbase
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn coinbase_usd_depth() {
    let _g = lock("coinbase");
    assert_archive_depth(Box::new(Coinbase), "coinbase", Currency::Usd, "2015-07-31").await;
}

#[tokio::test]
#[ignore]
async fn coinbase_eur_depth() {
    let _g = lock("coinbase");
    assert_archive_depth(Box::new(Coinbase), "coinbase", Currency::Eur, "2015-08-01").await;
}

#[tokio::test]
#[ignore]
async fn coinbase_gbp_depth() {
    let _g = lock("coinbase");
    assert_archive_depth(Box::new(Coinbase), "coinbase", Currency::Gbp, "2016-01-01").await;
}

// -------------------------------------------------------------------------
// KuCoin
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn kucoin_usd_depth() {
    let _g = lock("kucoin");
    // KuCoin launched Sept 2017, but BTC-USDT didn't list until ~Oct 15 2017.
    assert_archive_depth(Box::new(Kucoin), "kucoin", Currency::Usd, "2017-10-15").await;
}

#[tokio::test]
#[ignore]
async fn kucoin_eur_depth() {
    let _g = lock("kucoin");
    assert_archive_depth(Box::new(Kucoin), "kucoin", Currency::Eur, "2024-01-01").await;
}

// -------------------------------------------------------------------------
// OKX
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn okx_usd_depth() {
    let _g = lock("okx");
    assert_archive_depth(Box::new(Okx), "okx", Currency::Usd, "2019-01-01").await;
}

#[tokio::test]
#[ignore]
async fn okx_eur_depth() {
    let _g = lock("okx");
    assert_archive_depth(Box::new(Okx), "okx", Currency::Eur, "2024-01-01").await;
}

// -------------------------------------------------------------------------
// BitMEX
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn bitmex_usd_depth() {
    let _g = lock("bitmex");
    assert_archive_depth(Box::new(BitMex), "bitmex", Currency::Usd, "2015-09-26").await;
}

#[tokio::test]
#[ignore]
async fn bitmex_eur_depth() {
    let _g = lock("bitmex");
    assert_archive_depth(Box::new(BitMex), "bitmex", Currency::Eur, "2023-01-01").await;
}

// -------------------------------------------------------------------------
// Binance
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn binance_usd_depth() {
    let _g = lock("binance");
    assert_archive_depth(Box::new(Binance), "binance", Currency::Usd, "2017-08-17").await;
}

#[tokio::test]
#[ignore]
async fn binance_eur_depth() {
    let _g = lock("binance");
    assert_archive_depth(Box::new(Binance), "binance", Currency::Eur, "2020-01-03").await;
}

#[tokio::test]
#[ignore]
async fn binance_jpy_depth() {
    let _g = lock("binance");
    assert_archive_depth(Box::new(Binance), "binance", Currency::Jpy, "2024-03-12").await;
}

// -------------------------------------------------------------------------
// Bitbank (JPY)
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn bitbank_jpy_depth() {
    let _g = lock("bitbank");
    assert_archive_depth(Box::new(Bitbank), "bitbank", Currency::Jpy, "2017-02-28").await;
}

// -------------------------------------------------------------------------
// Upbit (KRW)
// -------------------------------------------------------------------------

#[tokio::test]
#[ignore]
async fn upbit_krw_depth() {
    let _g = lock("upbit");
    assert_archive_depth(Box::new(Upbit), "upbit", Currency::Krw, "2017-10-29").await;
}

// Bithumb dropped end-to-end — see `historical/sources.md` for rationale.
