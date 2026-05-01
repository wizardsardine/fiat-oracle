pub mod binance;
pub mod bitbank;
pub mod bitfinex;
pub mod bitmex;
pub mod bitstamp;
pub mod coinbase;
pub mod kucoin;
pub mod okx;
pub mod synth;
pub mod upbit;

use std::{
    collections::{BTreeMap, BTreeSet},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use tokio::time::Instant;

use async_trait::async_trait;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};

pub(crate) type Batch = Vec<(u64, f64)>;

use crate::{
    cache::PriceWriter,
    currency::Currency,
    error::{Error, Result},
};

pub(crate) static HTTP_CLIENT: std::sync::LazyLock<reqwest::Client> =
    std::sync::LazyLock::new(reqwest::Client::new);

/// Number of price sources required per 1-minute bucket to write a valid median.
pub const MIN_PRICE_SOURCES: usize = 3;

pub const MAX_CONSECUTIVE_ERRORS: usize = 3;

/// Per-request RTT headroom every source must keep regardless of native page
/// size. Sources with `min_interval` below this shard their fetch so
/// `concurrency * min_interval >= PARALLEL_RTT_BUDGET_MS`.
pub(crate) const PARALLEL_RTT_BUDGET_MS: u64 = 300;

/// Number of parallel shards needed for a source with the given `min_interval`
/// to keep its per-request budget at least [`PARALLEL_RTT_BUDGET_MS`].
pub(crate) fn shard_count(min_interval: Duration) -> u32 {
    let mi_ms = min_interval.as_millis() as u64;
    if mi_ms >= PARALLEL_RTT_BUDGET_MS {
        1
    } else {
        PARALLEL_RTT_BUDGET_MS.div_ceil(mi_ms.max(1)) as u32
    }
}

/// Optional CLI override for the operating throughput. When `Some(v)`,
/// every backfill uses `v` as TARGET regardless of which sources are active.
/// When `None` (the default), TARGET is derived per-currency in
/// [`run_backfill`] as `min(source.max_candles_per_sec(currency))` across
/// the sources participating in that backfill.
///
/// Stored as `AtomicU64` where `0` is the "unset" sentinel (0 c/s would be
/// meaningless anyway).
static TARGET_CANDLES_PER_SEC_OVERRIDE: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Set (or clear) the CLI override for operating TARGET. `None` clears it,
/// returning behaviour to per-currency derivation. Intended for one-time
/// startup configuration.
pub fn set_target_candles_per_sec(v: Option<u64>) {
    let stored = match v {
        Some(n) => {
            assert!(n > 0, "target candles/sec must be positive");
            n
        }
        None => 0,
    };
    TARGET_CANDLES_PER_SEC_OVERRIDE.store(stored, std::sync::atomic::Ordering::Relaxed);
}

/// Returns the CLI override if set, otherwise `None` (derive per-currency).
pub fn target_candles_per_sec_override() -> Option<u64> {
    let v = TARGET_CANDLES_PER_SEC_OVERRIDE.load(std::sync::atomic::Ordering::Relaxed);
    if v == 0 {
        None
    } else {
        Some(v)
    }
}

/// Call the progress callback once every this many finalised buckets.
const PROGRESS_BATCH_SIZE: u64 = 1_000;

pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before epoch")
        .as_secs()
}

pub(crate) struct PageResult {
    pub candles: Vec<(u64, f64)>,
    pub next_cursor: u64,
}

#[async_trait]
pub(crate) trait PagedFetch: Send + Sync {
    fn name(&self) -> &'static str;
    fn init_cursor(&self, from_ts: u64) -> u64;
    /// Return true to bail before fetching. Called at the top of every iteration.
    fn check_stop(&self, cursor: u64) -> bool;
    /// Build URL for this cursor. May call now() internally (fixed-window sources).
    fn url(&self, cursor: u64) -> String;
    /// Default HTTP fetch; override for custom error handling (e.g. 429 on BitMex).
    async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
        tokio::time::timeout(Duration::from_secs(5), async {
            HTTP_CLIENT.get(url).send().await.ok()?.json().await.ok()
        })
        .await
        .ok()?
    }
    /// Parse one page. `cursor` is the value used to build the URL.
    fn parse(&self, json: &serde_json::Value, from_ts: u64, cursor: u64) -> Option<PageResult>;
    /// Called after every successful page. Pagers that need response-based
    /// termination (empty response, partial page, backward-pagination sentinel,
    /// stall guard) set internal state here; `check_stop` reads it next iteration.
    fn on_page(&mut self, _page: &PageResult, _from_ts: u64, _cursor: u64) {}
    /// Maximum candles returned per request. Drives the rate-limit sleep so
    /// the pager advances through history at its `target_candles_per_sec()`.
    fn page_size(&self) -> u64;
    /// Number of parallel shards this pager is part of. Sources that auto-shard
    /// (see [`run_sharded_fetch`]) override this on each shard's pager so the
    /// per-shard `min_interval` scales up: total per-source request rate stays
    /// constant, but each individual request gets `concurrency × base` budget.
    fn concurrency(&self) -> u32 {
        1
    }
    /// Operating throughput target for this pager (candles/sec). Threaded in
    /// at construction time by `HistoricalSource::make_pager` and ultimately
    /// derived (per-currency) by [`run_backfill`].
    fn target_candles_per_sec(&self) -> u64;
    /// Minimum wall-clock time between consecutive requests on this pager.
    /// `(page_size * 1000 / target) * concurrency` — sources don't override.
    fn min_interval(&self) -> Duration {
        let base = self.page_size() * 1000 / self.target_candles_per_sec();
        Duration::from_millis(base * self.concurrency() as u64)
    }
}

pub(crate) async fn run_paged_fetch<S: PagedFetch + ?Sized>(
    pager: &mut S,
    from_ts: u64,
    tx: UnboundedSender<Batch>,
) -> Result<()> {
    let source = pager.name();
    let mut cursor = pager.init_cursor(from_ts);
    let mut errs: usize = 0;
    loop {
        if pager.check_stop(cursor) {
            eprintln!("[{source}] check_stop fired at cursor={cursor}, done");
            return Ok(());
        }
        let t0 = Instant::now();
        let url = pager.url(cursor);
        eprintln!("[{source}] fetching {url}");
        let json = match pager.fetch_json(&url).await {
            Some(j) => j,
            None => {
                errs += 1;
                eprintln!("[{source}] fetch failed cursor={cursor} errs={errs}");
                if errs >= MAX_CONSECUTIVE_ERRORS {
                    return Err(Error::Other("too many consecutive fetch errors".into()));
                }
                sleep_remaining(t0, pager.min_interval()).await;
                continue;
            }
        };
        match pager.parse(&json, from_ts, cursor) {
            Some(page) => {
                errs = 0;
                let n = page.candles.len();
                let next = page.next_cursor;
                eprintln!("[{source}] page ok cursor={cursor} candles={n} next_cursor={next}");
                pager.on_page(&page, from_ts, cursor);
                cursor = page.next_cursor;
                let _ = tx.send(page.candles);
            }
            None => {
                eprintln!("[{source}] parse failed cursor={cursor} json={json}");
                return Err(Error::Other(format!(
                    "{source}: failed to parse response at cursor={cursor}"
                )));
            }
        }
        sleep_remaining(t0, pager.min_interval()).await;
    }
}

async fn sleep_remaining(t0: Instant, min_interval: Duration) {
    let elapsed = t0.elapsed();
    if elapsed < min_interval {
        tokio::time::sleep(min_interval - elapsed).await;
    }
}

/// Boxed dyn-pager type used by [`run_sharded_fetch`] and `HistoricalSource::make_pager`.
pub(crate) type BoxedPager = Box<dyn PagedFetch + Send + Sync + 'static>;

/// Generic sharded fetch driver. Probes the factory once at concurrency=1 to
/// learn `min_interval`, asks [`shard_count`] how many parallel shards the
/// per-request budget needs, then either runs a single pager or spawns N
/// `run_paged_fetch` tasks each owning a contiguous time sub-range.
///
/// Sharding is opt-out: any source whose `min_interval` exceeds
/// [`PARALLEL_RTT_BUDGET_MS`] gets `n=1` and runs sequentially with no
/// overhead. Sources just provide a factory `|lo, hi, concurrency| -> BoxedPager`
/// describing how to construct one shard's pager — the per-shard pager is
/// expected to bound its fetch range to `[lo, hi]` and report `concurrency()`
/// so the inflated `min_interval` keeps the total request rate constant.
pub(crate) async fn run_sharded_fetch<F>(
    from_ts: u64,
    tx: UnboundedSender<Batch>,
    mut factory: F,
) -> Result<()>
where
    F: FnMut(u64, u64, u32) -> BoxedPager,
{
    let upper = now();
    if upper <= from_ts {
        return Ok(());
    }
    let probe = factory(from_ts, upper, 1);
    let n = shard_count(probe.min_interval());
    drop(probe);
    if n <= 1 {
        let mut p = factory(from_ts, upper, 1);
        return run_paged_fetch(&mut *p, from_ts, tx).await;
    }
    let span = upper - from_ts;
    let chunk = span / n as u64;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..n {
        let lo = from_ts + (i as u64) * chunk;
        let hi = if i == n - 1 {
            upper
        } else {
            from_ts + ((i + 1) as u64) * chunk
        };
        let mut p = factory(lo, hi, n);
        let tx_i = tx.clone();
        set.spawn(async move { run_paged_fetch(&mut *p, lo, tx_i).await });
    }
    while let Some(res) = set.join_next().await {
        res.map_err(|e| Error::Other(format!("shard panicked: {e}")))??;
    }
    Ok(())
}

#[async_trait]
pub trait HistoricalSource: Send + Sync {
    fn name(&self) -> &'static str;
    fn supports(&self, currency: Currency) -> bool;

    /// Maximum sustained candles/sec this source can deliver for `currency`,
    /// derived from `page_size × max_request_rate`. [`run_backfill`] uses
    /// this to compute the per-currency TARGET as `min` across active
    /// sources. Default `u64::MAX` = "this source doesn't constrain the
    /// per-currency cap" (e.g. one-shot sources like Bithumb that issue a
    /// single request for the whole range).
    fn max_candles_per_sec(&self, _currency: Currency) -> u64 {
        u64::MAX
    }

    /// Construct one shard's pager for `[lo, hi]` running with `concurrency`
    /// parallel siblings, paced at `target_candles_per_sec`. Returns `None`
    /// if the currency is unsupported. The default [`HistoricalSource::fetch`]
    /// dispatches to this. Sources that override `fetch` directly may leave
    /// this as the default.
    fn make_pager(
        &self,
        _currency: Currency,
        _lo: u64,
        _hi: u64,
        _concurrency: u32,
        _target_candles_per_sec: u64,
    ) -> Option<BoxedPager> {
        None
    }

    /// Fetch using this source's own max throughput as the operating target.
    /// Suitable for single-source live tests; the multi-source aggregator
    /// in [`run_backfill`] uses [`fetch_at_target`] instead so every source
    /// in the same backfill paces against the same per-currency TARGET.
    #[allow(dead_code)] // used by per-source `live_fetch_*` integration tests
    async fn fetch(
        &self,
        currency: Currency,
        from_ts: u64,
        tx: UnboundedSender<Batch>,
    ) -> Result<()> {
        let target = self.max_candles_per_sec(currency);
        // u64::MAX would underflow `min_interval`; sources that report it
        // (e.g. one-shot Bithumb) are expected to override `fetch_at_target`
        // rather than go through the paged path. Guard with a sane default
        // for safety.
        let target = if target == u64::MAX {
            DEFAULT_NO_CAP_TARGET
        } else {
            target
        };
        self.fetch_at_target(currency, from_ts, target, tx).await
    }

    /// Multi-source variant: caller supplies the operating TARGET so all
    /// sources participating in the same backfill pace identically. Used by
    /// [`run_backfill`].
    async fn fetch_at_target(
        &self,
        currency: Currency,
        from_ts: u64,
        target_candles_per_sec: u64,
        tx: UnboundedSender<Batch>,
    ) -> Result<()> {
        if !self.supports(currency) {
            return Ok(());
        }
        run_sharded_fetch(from_ts, tx, |lo, hi, n| {
            self.make_pager(currency, lo, hi, n, target_candles_per_sec)
                .expect("supports(currency) returned true but make_pager returned None")
        })
        .await
    }
}

/// Fallback target used by `HistoricalSource::fetch` when a source reports
/// `max_candles_per_sec = u64::MAX` (i.e. it doesn't paginate at all). The
/// value matches the historical default and keeps the paged path safe even
/// for sources that aren't expected to use it.
#[allow(dead_code)] // referenced from `fetch`, gated by `#[allow(dead_code)]`
const DEFAULT_NO_CAP_TARGET: u64 = 1_500;

pub fn all_sources() -> Vec<Box<dyn HistoricalSource>> {
    vec![
        Box::new(bitstamp::Bitstamp),
        Box::new(bitfinex::Bitfinex),
        Box::new(coinbase::Coinbase),
        Box::new(kucoin::Kucoin),
        Box::new(okx::Okx),
        Box::new(bitmex::BitMex),
        Box::new(binance::Binance),
        Box::new(bitbank::Bitbank),
        Box::new(upbit::Upbit),
    ]
}

/// Backfill 1-minute price data for `currency` from `from_ts` up to now.
///
/// Spawns one async task per supporting source; each streams batches of
/// (ts, price) tuples to a shared channel. The calling task aggregates: once a 1-minute bucket
/// has >= MIN_PRICE_SOURCES contributions the median is written immediately.
///
/// Returns `Err` if any source task hits MAX_CONSECUTIVE_ERRORS or panics.
pub async fn run_backfill(
    sources: Vec<Box<dyn HistoricalSource>>,
    currency: Currency,
    from_ts: u64,
    writer: &PriceWriter,
    on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
) -> Result<()> {
    let from_ts = writer.tip().map_or(from_ts, |t| from_ts.max(t + 60));

    // Per-currency TARGET: the operating throughput is bounded by the
    // slowest active source's documented max. Sources that don't paginate
    // (e.g. Bithumb's one-shot 1h endpoint) report `u64::MAX` and don't
    // constrain the cap. CLI override wins when set.
    let target = target_candles_per_sec_override().unwrap_or_else(|| {
        sources
            .iter()
            .filter(|s| s.supports(currency))
            .map(|s| s.max_candles_per_sec(currency))
            .min()
            .expect("run_backfill called with no source supporting this currency")
    });
    tracing::info!(
        ?currency,
        target_candles_per_sec = target,
        "backfill target"
    );

    let (sender, receiver) = mpsc::unbounded_channel::<Batch>();

    let mut handles = Vec::new();
    for source in sources {
        if source.supports(currency) {
            let name = source.name();
            let sender2 = sender.clone();
            let handle = tokio::task::spawn(async move {
                println!("starting historical fetch task from {name} for {currency:?}",);
                tracing::debug!("starting historical fetch task from {name} for {currency:?}",);
                source
                    .fetch_at_target(currency, from_ts, target, sender2)
                    .await
            });
            handles.push(handle);
        }
    }
    drop(sender);

    aggregator(receiver, writer, from_ts, on_progress).await;

    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(e) => return Err(Error::Other(format!("fetch task panicked: {e}"))),
        }
    }

    Ok(())
}

async fn aggregator(
    mut rx: UnboundedReceiver<Batch>,
    writer: &PriceWriter,
    from_ts: u64,
    on_progress: Option<&(dyn Fn(u64) + Send + Sync)>,
) {
    let mut buckets: BTreeMap<u64, Vec<f64>> = BTreeMap::new();
    // Track ts that have already been finalized as a price. Late-arriving
    // contributions for these ts are dropped instead of forming a fresh
    // sub-quorum bucket that the end-of-stream drain would clobber with
    // write_error.
    let mut finalized: BTreeSet<u64> = BTreeSet::new();
    let mut done_count: u64 = 0;

    while let Some(batch) = rx.recv().await {
        for (ts, price) in batch {
            if ts < from_ts || finalized.contains(&ts) {
                continue;
            }
            let bucket = buckets.entry(ts).or_default();
            bucket.push(price);
            if bucket.len() >= MIN_PRICE_SOURCES {
                let prices = buckets.remove(&ts).expect("just inserted");
                finalize(writer, ts, prices);
                finalized.insert(ts);
                done_count += 1;
                if done_count % PROGRESS_BATCH_SIZE == 0 {
                    if let Some(cb) = on_progress {
                        cb(done_count);
                    }
                }
            }
        }
    }

    for (ts, prices) in buckets {
        finalize(writer, ts, prices);
        done_count += 1;
    }

    if let Some(cb) = on_progress {
        cb(done_count);
    }
}

fn finalize(writer: &PriceWriter, ts: u64, mut prices: Vec<f64>) {
    let result = if prices.len() >= MIN_PRICE_SOURCES {
        prices.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let median = prices[prices.len() / 2];
        writer.write_price(ts, median)
    } else {
        writer.write_error(ts)
    };
    if let Err(e) = result {
        tracing::error!(ts, ?e, "backfill: failed to persist bucket");
    }
}

/// Lock names for all historical exchange sources, sorted alphabetically.
/// Pass to [`exchange_locks`] when a test needs to hold multiple source locks.
#[cfg(test)]
pub(crate) const HISTORICAL_LOCK_NAMES: &[&str] = &[
    "binance", "bitbank", "bitfinex", "bitmex", "bitstamp", "coinbase", "kucoin", "okx", "upbit",
];

/// Acquire an exclusive `flock(2)` on a per-source lock file for each name.
/// Returns the file handles; locks are released when the `Vec` is dropped.
/// Always pass names in sorted order to prevent deadlock when multiple callers
/// hold subsets of the same locks.
#[cfg(test)]
pub(crate) fn exchange_locks(names: &[&str]) -> Vec<std::fs::File> {
    use std::os::unix::io::AsRawFd;
    let guards: Vec<std::fs::File> = names
        .iter()
        .map(|name| {
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
            file
        })
        .collect();
    // Allow the previous lock-holder's in-flight exchange requests to clear
    // any rate-limit windows before we start making requests.
    std::thread::sleep(std::time::Duration::from_secs(3));
    guards
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::Arc;

    use tempfile::tempdir;

    use crate::cache::PriceCache;
    use crate::storage::Storage;

    // -------------------------------------------------------------------
    // Pacing / wait verification under tokio::time::pause()
    //
    // Hypothesis under test (user-stated): the wait between requests should
    // make network latency irrelevant. If true, total iter time should always
    // be >= min_interval, regardless of request duration. This test pins down
    // the actual contract.
    // -------------------------------------------------------------------

    struct PaceMockPager {
        page_size: u64,
        min_interval: Duration,
        req_time: Duration,
        cursor_step: u64,
        max_iters: u64,
        iters_done: std::sync::atomic::AtomicU64,
    }

    #[async_trait]
    impl PagedFetch for PaceMockPager {
        fn name(&self) -> &'static str {
            "pace-mock"
        }
        fn init_cursor(&self, from_ts: u64) -> u64 {
            from_ts
        }
        fn check_stop(&self, _cursor: u64) -> bool {
            self.iters_done.load(std::sync::atomic::Ordering::SeqCst) >= self.max_iters
        }
        fn url(&self, cursor: u64) -> String {
            format!("mock://{cursor}")
        }
        async fn fetch_json(&self, _url: &str) -> Option<serde_json::Value> {
            tokio::time::sleep(self.req_time).await;
            Some(serde_json::json!([]))
        }
        fn parse(
            &self,
            _json: &serde_json::Value,
            _from_ts: u64,
            cursor: u64,
        ) -> Option<PageResult> {
            self.iters_done
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Some(PageResult {
                candles: Vec::new(),
                next_cursor: cursor + self.cursor_step,
            })
        }
        fn page_size(&self) -> u64 {
            self.page_size
        }
        fn target_candles_per_sec(&self) -> u64 {
            // Bypassed: this pager hard-overrides `min_interval`.
            1
        }
        fn min_interval(&self) -> Duration {
            self.min_interval
        }
    }

    /// When request_time < min_interval, sleep_remaining pads the rest.
    /// Total iter time should be ~= min_interval.
    #[tokio::test(start_paused = true)]
    async fn pacing_request_faster_than_min_interval() {
        let mut pager = PaceMockPager {
            page_size: 300,
            min_interval: Duration::from_millis(104), // coinbase
            req_time: Duration::from_millis(50),      // faster than 104ms
            cursor_step: 18_001,
            max_iters: 10,
            iters_done: std::sync::atomic::AtomicU64::new(0),
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let t0 = tokio::time::Instant::now();
        run_paged_fetch(&mut pager, 0, tx).await.unwrap();
        let elapsed = t0.elapsed();
        let per_iter = elapsed / 10;
        assert!(
            per_iter >= Duration::from_millis(100) && per_iter <= Duration::from_millis(110),
            "expected ~104 ms/iter (min_interval), got {per_iter:?}"
        );
    }

    /// When request_time > min_interval, sleep_remaining adds NOTHING.
    /// Total iter time = request_time. This is the user's hypothesis #2:
    /// if this passes, the wait does NOT compensate for slow requests, and
    /// Coinbase's `min_interval = 104 ms` budget is exceeded by typical
    /// internet RTTs every iteration -> trailing.
    #[tokio::test(start_paused = true)]
    async fn pacing_request_slower_than_min_interval() {
        let mut pager = PaceMockPager {
            page_size: 300,
            min_interval: Duration::from_millis(104), // coinbase
            req_time: Duration::from_millis(500),     // slower than 104ms
            cursor_step: 18_001,
            max_iters: 10,
            iters_done: std::sync::atomic::AtomicU64::new(0),
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let t0 = tokio::time::Instant::now();
        run_paged_fetch(&mut pager, 0, tx).await.unwrap();
        let elapsed = t0.elapsed();
        let per_iter = elapsed / 10;
        assert!(
            per_iter >= Duration::from_millis(495) && per_iter <= Duration::from_millis(510),
            "request dominates: expected ~500 ms/iter, got {per_iter:?}"
        );
    }

    // -----------------------------------------------------------------
    // Cross-source pacing test using each source's REAL pager.
    //
    // Wraps each source's PagedFetch impl in a generic latency wrapper that
    // sleeps `latency` per request and returns a source-specific canned JSON
    // (defined in each source module as `mock_response`). Runs every source
    // through the actual run_paged_fetch over the same range and compares
    // wall times.
    //
    // This is THE contract test for the throughput design.
    // -----------------------------------------------------------------

    use crate::currency::Currency;

    struct WithMockHttp<P: PagedFetch> {
        inner: P,
        latency: Duration,
        responder: fn(&str) -> serde_json::Value,
    }

    #[async_trait]
    impl<P: PagedFetch> PagedFetch for WithMockHttp<P> {
        fn name(&self) -> &'static str {
            self.inner.name()
        }
        fn init_cursor(&self, from_ts: u64) -> u64 {
            self.inner.init_cursor(from_ts)
        }
        fn check_stop(&self, cursor: u64) -> bool {
            self.inner.check_stop(cursor)
        }
        fn url(&self, cursor: u64) -> String {
            self.inner.url(cursor)
        }
        async fn fetch_json(&self, url: &str) -> Option<serde_json::Value> {
            tokio::time::sleep(self.latency).await;
            Some((self.responder)(url))
        }
        fn parse(&self, json: &serde_json::Value, from_ts: u64, cursor: u64) -> Option<PageResult> {
            self.inner.parse(json, from_ts, cursor)
        }
        fn on_page(&mut self, page: &PageResult, from_ts: u64, cursor: u64) {
            self.inner.on_page(page, from_ts, cursor);
        }
        fn page_size(&self) -> u64 {
            self.inner.page_size()
        }
        fn target_candles_per_sec(&self) -> u64 {
            self.inner.target_candles_per_sec()
        }
        fn min_interval(&self) -> Duration {
            self.inner.min_interval()
        }
        fn concurrency(&self) -> u32 {
            self.inner.concurrency()
        }
    }

    /// Runs the production `run_sharded_fetch` against the source's REAL
    /// pager, with each request wrapped to inject `latency` + canned JSON.
    /// Sharding is decided by `run_sharded_fetch` from `min_interval` —
    /// no per-source plumbing in the test.
    #[allow(clippy::too_many_arguments)]
    async fn time_source<P, F>(
        name: &str,
        from_ts: u64,
        latency: Duration,
        responder: fn(&str) -> serde_json::Value,
        target_candles_per_sec: u64,
        expected_min_wall_time: Duration,
        mut build: F,
    ) -> (String, Duration, u64, Duration)
    where
        P: PagedFetch + Send + Sync + 'static,
        F: FnMut(u64, u64, u32, u64) -> P,
    {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let t0 = Instant::now();
        run_sharded_fetch(from_ts, tx, |lo, hi, n| {
            Box::new(WithMockHttp {
                inner: build(lo, hi, n, target_candles_per_sec),
                latency,
                responder,
            }) as BoxedPager
        })
        .await
        .unwrap();
        let elapsed = t0.elapsed();
        let mut total: u64 = 0;
        while let Some(b) = rx.recv().await {
            total += b.len() as u64;
        }
        (name.to_string(), elapsed, total, expected_min_wall_time)
    }

    /// Runs every source through the production `run_sharded_fetch` over the
    /// same 30-day range, using each source's REAL pager + parser, with
    /// `latency` injected per HTTP request and source-specific canned JSON.
    /// Each pager paces at its **own** `max_candles_per_sec` — there is no
    /// global TARGET to converge on. Returns one
    /// `(name, wall_time, candles_received, expected_min_wall_time)` tuple
    /// per source so the convergence assertion can compare each source's
    /// actual time against its own theoretical floor.
    /// Sharding decisions live in `run_sharded_fetch` — no per-source code here.
    async fn all_sources_30d_with_latency(
        latency: Duration,
    ) -> Vec<(String, Duration, u64, Duration)> {
        use super::{binance, bitbank, bitfinex, bitmex, bitstamp, coinbase, kucoin, okx, upbit};

        let from_ts = now() - 30 * 24 * 3600;
        let pair = "BTC-USD";
        // 30 days of 1m candles.
        let candles_total = 30 * 24 * 60u64;

        // Compute the theoretical minimum wall time for a source given its
        // page_size and target throughput: `pages_needed × min_interval` where
        // `min_interval = page_size × 1000 / target × concurrency` and
        // `concurrency` collapses out (per-shard time × shard count = per-shard
        // budget × shard count = wall clock for one source). Net: independent
        // of concurrency, equals `(candles_total × 1000) / target` ms.
        fn expected_wall_time(target: u64) -> Duration {
            Duration::from_millis(30 * 24 * 60 * 1000 / target)
        }
        let _ = candles_total; // documentary

        vec![
            time_source(
                "coinbase",
                from_ts,
                latency,
                coinbase::mock_response,
                coinbase::Coinbase.max_candles_per_sec(Currency::Usd),
                expected_wall_time(coinbase::Coinbase.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| coinbase::CoinbasePager {
                    pair,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "bitstamp",
                from_ts,
                latency,
                bitstamp::mock_response,
                bitstamp::Bitstamp.max_candles_per_sec(Currency::Usd),
                expected_wall_time(bitstamp::Bitstamp.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| bitstamp::BitstampPager {
                    pair,
                    done: false,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "kucoin",
                from_ts,
                latency,
                kucoin::mock_response,
                kucoin::Kucoin.max_candles_per_sec(Currency::Usd),
                expected_wall_time(kucoin::Kucoin.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| kucoin::KucoinPager {
                    pair,
                    done: false,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "bitfinex",
                from_ts,
                latency,
                bitfinex::mock_response,
                bitfinex::Bitfinex.max_candles_per_sec(Currency::Usd),
                expected_wall_time(bitfinex::Bitfinex.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| bitfinex::BitfinexPager {
                    symbol: pair,
                    done: false,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "bitmex",
                from_ts,
                latency,
                bitmex::mock_response,
                bitmex::BitMex.max_candles_per_sec(Currency::Usd),
                expected_wall_time(bitmex::BitMex.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| bitmex::BitMexPager {
                    pair,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "okx",
                from_ts,
                latency,
                okx::mock_response,
                okx::Okx.max_candles_per_sec(Currency::Usd),
                expected_wall_time(okx::Okx.max_candles_per_sec(Currency::Usd)),
                |lo, hi, n, target| okx::OkxPager {
                    inst_id: pair,
                    done: false,
                    upper_ts: Some(hi),
                    lower_ts: Some(lo),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "binance",
                from_ts,
                latency,
                binance::mock_response,
                binance::Binance.max_candles_per_sec(Currency::Usd),
                expected_wall_time(binance::Binance.max_candles_per_sec(Currency::Usd)),
                |_lo, hi, n, target| binance::BinancePager {
                    symbol: pair,
                    done: false,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "bitbank",
                from_ts,
                latency,
                bitbank::mock_response,
                bitbank::Bitbank.max_candles_per_sec(Currency::Jpy),
                expected_wall_time(bitbank::Bitbank.max_candles_per_sec(Currency::Jpy)),
                |_lo, hi, n, target| bitbank::BitbankPager {
                    pair: "btc_jpy",
                    done: false,
                    upper_ts: Some(hi),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
            time_source(
                "upbit",
                from_ts,
                latency,
                upbit::mock_response,
                upbit::Upbit.max_candles_per_sec(Currency::Krw),
                expected_wall_time(upbit::Upbit.max_candles_per_sec(Currency::Krw)),
                |lo, hi, n, target| upbit::UpbitPager {
                    market: "KRW-BTC",
                    done: false,
                    upper_ts: Some(hi),
                    lower_ts: Some(lo),
                    concurrency: n,
                    target_candles_per_sec: target,
                },
            )
            .await,
        ]
    }

    /// Per-source throughput contract: each source's actual wall time must
    /// be within `ratio_max ×` of its theoretical minimum, given the source's
    /// own `max_candles_per_sec`. Sources do **not** share a TARGET; the
    /// per-currency derivation in `run_backfill` lets each go as fast as it
    /// can. The convergence test asserts the pacing logic produces
    /// near-optimal wall time for *every* source independently.
    fn assert_convergence(results: &[(String, Duration, u64, Duration)], ratio_max: f64) {
        let report: String = results
            .iter()
            .map(|(n, d, c, exp)| {
                format!(
                    "  {n:>9}: actual {:>6} ms, expected {:>6} ms, {c:>6} candles",
                    d.as_millis(),
                    exp.as_millis()
                )
            })
            .collect::<Vec<_>>()
            .join("\n");
        for (name, actual, _candles, expected) in results {
            let ratio = actual.as_secs_f64() / expected.as_secs_f64();
            assert!(
                ratio <= ratio_max,
                "{name}: actual {actual:?} > {ratio_max:.2} × theoretical-min {expected:?} (ratio {ratio:.2}x).\n{report}"
            );
        }
    }

    /// Shard correctness for forward-paginated Coinbase: 3 concurrent shards
    /// must collectively cover `[from_ts, now)` exactly once per minute
    /// bucket — no gaps, no cross-shard duplicates.
    #[tokio::test(start_paused = true)]
    async fn coinbase_3_shards_cover_range_once() {
        use super::coinbase;

        let upper = now();
        let from_ts = upper - 30 * 24 * 3600;
        let n = 3u32;
        let chunk = (upper - from_ts) / n as u64;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..n {
            let lo = from_ts + (i as u64) * chunk;
            let hi = if i == n - 1 {
                upper
            } else {
                from_ts + ((i + 1) as u64) * chunk
            };
            let pager = coinbase::CoinbasePager {
                pair: "BTC-USD",
                upper_ts: Some(hi),
                concurrency: n,
                target_candles_per_sec: 3_000,
            };
            let mut wrapped = WithMockHttp {
                inner: pager,
                latency: Duration::ZERO,
                responder: coinbase::mock_response,
            };
            let tx_i = tx.clone();
            set.spawn(async move {
                run_paged_fetch(&mut wrapped, lo, tx_i).await.unwrap();
            });
        }
        drop(tx);
        while let Some(r) = set.join_next().await {
            r.unwrap();
        }
        let mut all: Vec<(u64, f64)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            all.extend(batch);
        }

        // No duplicates per minute bucket.
        let mut seen = std::collections::HashSet::new();
        for (ts, _) in &all {
            assert!(seen.insert(*ts), "duplicate candle at ts={ts}");
        }
        // Coverage: at least (span / 60) - small fuzz.
        let span_min = (upper - from_ts) / 60;
        let coverage = all.len() as u64;
        assert!(
            coverage >= span_min - 5 && coverage <= span_min + 5,
            "coverage={coverage} span_min={span_min}"
        );
    }

    /// Same correctness test for OKX (backward-paginated).
    #[tokio::test(start_paused = true)]
    async fn okx_3_shards_cover_range_once() {
        use super::okx;

        let upper = now();
        let from_ts = upper - 30 * 24 * 3600;
        let n = 3u32;
        let chunk = (upper - from_ts) / n as u64;
        let (tx, mut rx) = mpsc::unbounded_channel();
        let mut set = tokio::task::JoinSet::new();
        for i in 0..n {
            let lo = from_ts + (i as u64) * chunk;
            let hi = if i == n - 1 {
                upper
            } else {
                from_ts + ((i + 1) as u64) * chunk
            };
            let pager = okx::OkxPager {
                inst_id: "BTC-USDT",
                done: false,
                upper_ts: Some(hi),
                lower_ts: Some(lo),
                concurrency: n,
                target_candles_per_sec: 6_000,
            };
            let mut wrapped = WithMockHttp {
                inner: pager,
                latency: Duration::ZERO,
                responder: okx::mock_response,
            };
            let tx_i = tx.clone();
            set.spawn(async move {
                run_paged_fetch(&mut wrapped, lo, tx_i).await.unwrap();
            });
        }
        drop(tx);
        while let Some(r) = set.join_next().await {
            r.unwrap();
        }
        let mut all: Vec<(u64, f64)> = Vec::new();
        while let Some(batch) = rx.recv().await {
            all.extend(batch);
        }

        let mut seen = std::collections::HashSet::new();
        for (ts, _) in &all {
            assert!(seen.insert(*ts), "duplicate candle at ts={ts}");
        }
        let span_min = (upper - from_ts) / 60;
        let coverage = all.len() as u64;
        // OKX's mock returns 300 candles per request regardless of shard
        // bounds, so it slightly over-fetches at boundaries. Bound loosely.
        assert!(
            coverage >= span_min - 5 && coverage <= span_min + 3 * 300,
            "coverage={coverage} span_min={span_min}"
        );
    }

    /// Pure-logic contract: with zero per-request latency, every source's
    /// per-iter time collapses to its `min_interval`, and total wall time
    /// should be within ~30 % of the theoretical floor for that source's
    /// own target throughput (range / page_size × min_interval rounded up
    /// to the nearest full page introduces some quantisation slack).
    #[tokio::test(start_paused = true)]
    async fn all_sources_take_similar_time_zero_latency() {
        let results = all_sources_30d_with_latency(Duration::ZERO).await;
        assert_convergence(&results, 1.3);
    }

    /// Worst-case contract: with 300 ms-per-request latency — the
    /// `PARALLEL_RTT_BUDGET_MS` threshold itself, the ceiling sharding
    /// was designed around. For every source the effective per-iter
    /// budget (sharded `concurrency × min_interval`, or plain
    /// `min_interval` for the larger-page sources) must remain ≥ 300 ms
    /// so `sleep_remaining` still absorbs each request. Each source must
    /// still complete within ~30 % of its theoretical floor.
    ///
    /// If this regresses, either RTT exceeds the design threshold (lower
    /// TARGET, or get a faster network) or the rate-limit / sharding
    /// logic regressed.
    #[tokio::test(start_paused = true)]
    async fn all_sources_take_similar_time_at_300ms_latency() {
        let results = all_sources_30d_with_latency(Duration::from_millis(300)).await;
        assert_convergence(&results, 1.3);
    }

    /// Direct smoking gun for Coinbase specifically: 25h catch-up, 5 pages,
    /// each request 500ms. With the wait properly absorbing latency, this
    /// would be ~520ms (1 effective iter at TARGET throughput). Currently it
    /// is ~2500ms — wait does nothing once req_time > min_interval.
    #[tokio::test(start_paused = true)]
    async fn coinbase_25h_catchup_takes_5x_req_time_not_5x_min_interval() {
        let mut pager = PaceMockPager {
            page_size: 300,
            min_interval: Duration::from_millis(104),
            req_time: Duration::from_millis(500),
            cursor_step: 18_001, // matches actual coinbase WINDOW + 1
            max_iters: 5,        // ceil(25h / 5h)
            iters_done: std::sync::atomic::AtomicU64::new(0),
        };
        let (tx, _rx) = mpsc::unbounded_channel();
        let t0 = tokio::time::Instant::now();
        run_paged_fetch(&mut pager, 0, tx).await.unwrap();
        let elapsed = t0.elapsed();
        // If wait worked as user expects: would be 5 * 500ms = 2500ms (req_time wins)
        //   OR 5 * 104ms = 520ms if wait *replaced* the request.
        // What actually happens: 5 * 500ms = 2500ms — wait adds zero.
        assert!(
            elapsed >= Duration::from_millis(2400) && elapsed <= Duration::from_millis(2600),
            "expected ~2500ms (5 iters x 500ms req), got {elapsed:?} \
             (a value of ~520ms would mean wait was working as user expects)"
        );
    }

    #[test]
    fn registry_has_9_sources() {
        let sources = all_sources();
        let names: Vec<_> = sources.iter().map(|s| s.name()).collect();
        assert_eq!(names.len(), 9);
        assert!(names.contains(&"bitstamp"));
        assert!(names.contains(&"bitfinex"));
        assert!(names.contains(&"coinbase"));
        assert!(names.contains(&"kucoin"));
        assert!(names.contains(&"okx"));
        assert!(names.contains(&"bitmex"));
        assert!(names.contains(&"binance"));
        assert!(names.contains(&"bitbank"));
        assert!(names.contains(&"upbit"));
    }

    struct MockSource {
        label: &'static str,
        data: HashMap<Currency, Vec<(u64, f64)>>,
        supported: HashSet<Currency>,
        fail: bool,
    }

    impl MockSource {
        fn new(
            label: &'static str,
            data: HashMap<Currency, Vec<(u64, f64)>>,
            supported: HashSet<Currency>,
        ) -> Self {
            Self {
                label,
                data,
                supported,
                fail: false,
            }
        }

        fn failing(mut self) -> Self {
            self.fail = true;
            self
        }
    }

    #[async_trait::async_trait]
    impl HistoricalSource for MockSource {
        fn name(&self) -> &'static str {
            self.label
        }

        fn supports(&self, c: Currency) -> bool {
            self.supported.contains(&c)
        }

        async fn fetch_at_target(
            &self,
            c: Currency,
            from_ts: u64,
            _target: u64,
            tx: UnboundedSender<Batch>,
        ) -> Result<()> {
            if self.fail {
                return Ok(());
            }
            if let Some(data) = self.data.get(&c) {
                let batch: Batch = data
                    .iter()
                    .copied()
                    .filter(|(ts, _)| *ts >= from_ts)
                    .collect();
                let _ = tx.send(batch);
            }
            Ok(())
        }
    }

    fn make_cache(dir: &Path, currencies: &[Currency]) -> Arc<PriceCache> {
        let storage = Arc::new(Storage::open(dir, currencies).unwrap());
        PriceCache::new(currencies, storage)
    }

    fn single_source(label: &'static str, currency: Currency, data: Vec<(u64, f64)>) -> MockSource {
        let mut map = HashMap::new();
        map.insert(currency, data);
        let mut supported = HashSet::new();
        supported.insert(currency);
        MockSource::new(label, map, supported)
    }

    #[tokio::test]
    async fn median_of_3_sources() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let t1 = 60u64;
        let s1 = single_source("s1", Currency::Eur, vec![(t1, 100.0)]);
        let s2 = single_source("s2", Currency::Eur, vec![(t1, 102.0)]);
        let s3 = single_source("s3", Currency::Eur, vec![(t1, 105.0)]);

        run_backfill(
            vec![
                Box::new(s1) as Box<dyn HistoricalSource>,
                Box::new(s2),
                Box::new(s3),
            ],
            Currency::Eur,
            t1,
            &writer,
            None,
        )
        .await
        .unwrap();

        // Sorted: [100, 102, 105], median at index 1 -> 102.0
        let result = cache.lookup_batch(Currency::Eur, &[t1]).expect("lookup");
        let price = result[0].as_price().expect("price should be written");
        assert!((price - 102.0).abs() < 1.0, "expected ~102.0, got {price}");
    }

    /// Regression: with >MIN_PRICE_SOURCES sources contributing the same ts,
    /// the aggregator used to finalize the bucket on the 3rd contribution and
    /// then create a fresh bucket from the late arrivals. If the late bucket
    /// ended below MIN_PRICE_SOURCES, the end-of-stream drain called
    /// write_error and clobbered the median that was already written.
    #[tokio::test]
    async fn late_arrivals_do_not_clobber_finalized_price() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let t1 = 60u64;
        // 4 sources for the same ts. Quorum (3) hits, then a 4th candle
        // arrives that previously formed a sub-quorum bucket and triggered
        // write_error at end-of-stream.
        let s1 = single_source("s1", Currency::Eur, vec![(t1, 100.0)]);
        let s2 = single_source("s2", Currency::Eur, vec![(t1, 102.0)]);
        let s3 = single_source("s3", Currency::Eur, vec![(t1, 104.0)]);
        let s4 = single_source("s4", Currency::Eur, vec![(t1, 106.0)]);

        run_backfill(
            vec![
                Box::new(s1) as Box<dyn HistoricalSource>,
                Box::new(s2),
                Box::new(s3),
                Box::new(s4),
            ],
            Currency::Eur,
            t1,
            &writer,
            None,
        )
        .await
        .unwrap();

        let result = cache.lookup_batch(Currency::Eur, &[t1]).expect("lookup");
        assert!(
            result[0].as_price().is_some(),
            "price must survive late-arriving sub-quorum bucket"
        );
    }

    #[tokio::test]
    async fn insufficient_sources_writes_error() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let t1 = 60u64;
        let s1 = single_source("s1", Currency::Eur, vec![(t1, 100.0)]);
        let s2 = single_source("s2", Currency::Eur, vec![(t1, 102.0)]);

        run_backfill(
            vec![Box::new(s1) as Box<dyn HistoricalSource>, Box::new(s2)],
            Currency::Eur,
            t1,
            &writer,
            None,
        )
        .await
        .unwrap();

        let result = cache.lookup_batch(Currency::Eur, &[t1]).expect("lookup");
        assert!(
            result[0].as_price().is_none(),
            "expected no price for < MIN_PRICE_SOURCES sources"
        );
    }

    #[tokio::test]
    async fn bailing_source_does_not_abort_others() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let t1 = 60u64;
        let s1 = single_source("s1", Currency::Eur, vec![(t1, 100.0)]);
        let s2 = single_source("s2", Currency::Eur, vec![(t1, 102.0)]).failing();
        let s3 = single_source("s3", Currency::Eur, vec![(t1, 102.0)]);
        let s4 = single_source("s4", Currency::Eur, vec![(t1, 104.0)]);

        run_backfill(
            vec![
                Box::new(s1) as Box<dyn HistoricalSource>,
                Box::new(s2),
                Box::new(s3),
                Box::new(s4),
            ],
            Currency::Eur,
            t1,
            &writer,
            None,
        )
        .await
        .unwrap();

        // s2 bailed; s1/s3/s4 contribute -> 3 prices -> median written
        let result = cache.lookup_batch(Currency::Eur, &[t1]).expect("lookup");
        assert!(
            result[0].as_price().is_some(),
            "price should be written despite one source bailing"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn live_run_backfill_usd_30_days() {
        let _eg = super::exchange_locks(super::HISTORICAL_LOCK_NAMES);
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);

        let now = now() / 60 * 60; // align to minute
        let from = now - 30 * 24 * 3600;

        run_backfill(all_sources(), Currency::Usd, from, &writer, None)
            .await
            .unwrap();

        let expected: Vec<u64> = (0..=(now - from) / 60).map(|i| from + i * 60).collect();
        let total = expected.len();

        let mut filled = 0usize;
        let mut prices: Vec<f64> = Vec::new();
        for chunk in expected.chunks(1000) {
            let results = cache.lookup_batch(Currency::Usd, chunk).expect("lookup");
            for entry in &results {
                if let Some(p) = entry.as_price() {
                    filled += 1;
                    prices.push(p);
                }
            }
        }

        let coverage = filled as f64 / total as f64;
        assert!(
            coverage >= 0.80,
            "expected >=80% minute coverage over 30 days, got {:.1}% ({filled}/{total})",
            coverage * 100.0,
        );

        for p in &prices {
            assert!(*p > 1_000.0 && *p < 1_000_000.0, "implausible price {p}");
        }
    }

    #[tokio::test]
    async fn unsupported_currency_source_skipped() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let t1 = 60u64;
        let s1 = single_source("s1", Currency::Eur, vec![(t1, 100.0)]);
        let s2 = single_source("s2", Currency::Eur, vec![(t1, 102.0)]);
        let s3 = single_source("s3", Currency::Usd, vec![(t1, 999.0)]);
        let s4 = single_source("s4", Currency::Eur, vec![(t1, 104.0)]);

        run_backfill(
            vec![
                Box::new(s1) as Box<dyn HistoricalSource>,
                Box::new(s2),
                Box::new(s3),
                Box::new(s4),
            ],
            Currency::Eur,
            t1,
            &writer,
            None,
        )
        .await
        .unwrap();

        let result = cache.lookup_batch(Currency::Eur, &[t1]).expect("lookup");
        assert!(
            result[0].as_price().is_some(),
            "3 EUR sources should produce a median"
        );
    }

    #[tokio::test]
    async fn progress_callback_called_after_backfill() {
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::sync::Arc;

        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Eur]);
        let writer = cache.writer(Currency::Eur);

        let n = PROGRESS_BATCH_SIZE + 1;
        let base_ts = 60u64;
        let data: Vec<(u64, f64)> = (0..n).map(|i| (base_ts + i * 60, 100.0)).collect();

        let s1 = single_source("s1", Currency::Eur, data.clone());
        let s2 = single_source("s2", Currency::Eur, data.clone());
        let s3 = single_source("s3", Currency::Eur, data.clone());

        let last_reported = Arc::new(AtomicU64::new(0));
        let last_clone = Arc::clone(&last_reported);
        let cb = move |done: u64| {
            last_clone.store(done, Ordering::Relaxed);
        };

        run_backfill(
            vec![
                Box::new(s1) as Box<dyn HistoricalSource>,
                Box::new(s2),
                Box::new(s3),
            ],
            Currency::Eur,
            base_ts,
            &writer,
            Some(&cb),
        )
        .await
        .unwrap();

        assert_eq!(
            last_reported.load(Ordering::Relaxed),
            n,
            "progress callback must report all finalised buckets"
        );
    }
}
