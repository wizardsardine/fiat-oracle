use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
};

use serde::Serialize;

use crate::{
    currency::{from_stored, to_stored, Currency, FETCH_ERROR, NO_DATA},
    error::{Error, Result},
    paging::{page_id, sample_offset, MAX_WALK_SAMPLES, SAMPLES_PER_PAGE},
    storage::Storage,
};

// Pages are Box'd so the BTreeMap node only carries an 8-byte pointer instead
// of an inline 4 KB array; rebalances move pointers, not whole pages.
type PageMap = BTreeMap<Currency, BTreeMap<u32, Box<[u32; SAMPLES_PER_PAGE]>>>;

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PriceResult {
    /// Found a price in the walk window.
    Price(f64),
    /// No sample within the walk window has data.
    NoData,
    /// The exact slot was marked as a fetch failure and the walk found no
    /// valid neighbour.
    FetchError,
    /// The cache is not tracking this currency.
    Unsupported,
}

#[cfg(test)]
impl PriceResult {
    /// Convenience accessor for tests that only care about the Price case.
    pub fn as_price(self) -> Option<f64> {
        match self {
            PriceResult::Price(v) => Some(v),
            _ => None,
        }
    }
}

pub struct PriceCache {
    storage: Arc<Storage>,
    cache: RwLock<PageMap>,
}

#[derive(Clone)]
pub struct PriceWriter {
    cache: Arc<PriceCache>,
    currency: Currency,
}

impl PriceCache {
    pub fn new(currencies: &[Currency], storage: Arc<Storage>) -> Arc<Self> {
        let pages = currencies.iter().map(|c| (*c, BTreeMap::new())).collect();
        Arc::new(Self {
            storage,
            cache: RwLock::new(pages),
        })
    }

    pub fn writer(self: &Arc<Self>, currency: Currency) -> PriceWriter {
        PriceWriter {
            cache: Arc::clone(self),
            currency,
        }
    }

    pub fn lookup_batch(&self, currency: Currency, timestamps: &[u64]) -> Result<Vec<PriceResult>> {
        let needed: BTreeSet<_> = timestamps.iter().map(|t| page_id(*t)).collect();

        // Currency not configured in this cache: tell the caller in-band, no
        // I/O performed, no error.
        let currency_supported = self.cache.read().expect("poisoned").contains_key(&currency);
        if !currency_supported {
            return Ok(timestamps
                .iter()
                .map(|_| PriceResult::Unsupported)
                .collect());
        }

        let missing: Vec<u32> = {
            let lock = self.cache.read().expect("poisoned");
            let table = lock
                .get(&currency)
                .expect("currency presence checked above");
            needed
                .iter()
                .copied()
                .filter(|pid| !table.contains_key(pid))
                .collect()
        };

        if !missing.is_empty() {
            let mut loaded: Vec<(u32, Box<[u32; SAMPLES_PER_PAGE]>)> =
                Vec::with_capacity(missing.len());
            for &pid in &missing {
                let page = self
                    .storage
                    .get_page(currency, pid)?
                    .unwrap_or_else(|| Box::new([NO_DATA; SAMPLES_PER_PAGE]));
                loaded.push((pid, page));
            }

            let mut pages = self.cache.write().expect("poisoned");
            let cp = pages
                .get_mut(&currency)
                .expect("currency presence checked above");
            for (pid, page) in loaded {
                cp.entry(pid).or_insert(page);
            }
        }

        let lock = self.cache.read().expect("poisoned");
        let table = lock
            .get(&currency)
            .expect("currency presence checked above");

        let mut output = Vec::with_capacity(timestamps.len());
        'outer: for ts in timestamps {
            let pid = page_id(*ts);
            let offset = sample_offset(*ts);
            let page = table.get(&pid).expect("page faulted in by phase 3");

            let exact = page[offset];
            if let Some(v) = from_stored(currency, exact) {
                output.push(PriceResult::Price(v));
                continue;
            }
            // sentinel at the requested slot: walk outward within the page.
            for delta in 1..=MAX_WALK_SAMPLES {
                if delta <= offset {
                    if let Some(v) = from_stored(currency, page[offset - delta]) {
                        output.push(PriceResult::Price(v));
                        continue 'outer;
                    }
                }
                if offset + delta < SAMPLES_PER_PAGE {
                    if let Some(v) = from_stored(currency, page[offset + delta]) {
                        output.push(PriceResult::Price(v));
                        continue 'outer;
                    }
                }
            }
            // Walk found nothing. Distinguish "tried and failed" from "never
            // sampled" based on the exact slot's sentinel.
            output.push(if exact == FETCH_ERROR {
                PriceResult::FetchError
            } else {
                PriceResult::NoData
            });
        }
        Ok(output)
    }

    fn write_to_storage(&self, currency: Currency, timestamp: u64, value: u32) -> Result<()> {
        let pid = page_id(timestamp);
        let off = sample_offset(timestamp);

        // We write a complete page so we need to have the page cached prior to write
        let page_cached = {
            let lock = self.cache.read().expect("poisoned");
            lock.get(&currency)
                .is_some_and(|table| table.contains_key(&pid))
        };

        if !page_cached {
            let page = self
                .storage
                .get_page(currency, pid)?
                // page missing in DB, we create it
                .unwrap_or_else(|| Box::new([NO_DATA; SAMPLES_PER_PAGE]));
            let mut lock = self.cache.write().expect("poisoned");
            let table = lock
                .get_mut(&currency)
                .ok_or_else(|| Error::MissingCurrency(currency.to_string()))?;
            table.entry(pid).or_insert(page);
        }

        let page_copy: [u32; SAMPLES_PER_PAGE];
        {
            let mut lock = self.cache.write().expect("poisoned");
            let cache_table = lock
                .get_mut(&currency)
                .ok_or_else(|| Error::MissingCurrency(currency.to_string()))?;
            let cache_page = cache_table
                .get_mut(&pid)
                .expect("page just faulted in by the block above");
            cache_page[off] = value;
            page_copy = **cache_page;
        }

        self.storage.put_page(currency, pid, &page_copy)?;
        Ok(())
    }
}

impl PriceWriter {
    pub fn write_price(&self, ts: u64, price: f64) -> Result<()> {
        let value = to_stored(self.currency, price);
        self.cache.write_to_storage(self.currency, ts, value)
    }

    pub fn write_error(&self, ts: u64) -> Result<()> {
        self.cache.write_to_storage(self.currency, ts, FETCH_ERROR)
    }

    /// Latest stored timestamp for this currency, or None if no data yet.
    pub fn tip(&self) -> Option<u64> {
        self.cache
            .storage
            .last_timestamp(self.currency)
            .ok()
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;

    fn make_cache(dir: &Path, currencies: &[Currency]) -> Arc<PriceCache> {
        let storage = Arc::new(Storage::open(dir, currencies).unwrap());
        PriceCache::new(currencies, storage)
    }

    #[test]
    fn hit() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts = 64 * 10;
        writer.write_price(ts, 50000.0).expect("write");
        let results = cache.lookup_batch(Currency::Usd, &[ts]).expect("lookup");
        let val = results[0]
            .as_price()
            .expect("expected Some for written price");
        assert!(
            (val - 50000.0).abs() <= 0.01,
            "expected ~50000.0, got {val}"
        );
    }

    #[test]
    fn miss() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let ts = 64 * 100;
        let results = cache.lookup_batch(Currency::Usd, &[ts]).expect("lookup");
        assert!(
            results[0].as_price().is_none(),
            "expected None for never-written timestamp"
        );
    }

    #[test]
    fn walk_back_within_page() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts = 64 * 10;
        writer.write_price(ts, 50000.0).expect("write");
        // Look up 5 samples ahead; backward walk should find it.
        let ts_future = ts + 64 * 5;
        let results = cache
            .lookup_batch(Currency::Usd, &[ts_future])
            .expect("lookup");
        let val = results[0]
            .as_price()
            .expect("expected Some via backward walk");
        assert!(
            (val - 50000.0).abs() <= 0.01,
            "expected ~50000.0, got {val}"
        );
    }

    #[test]
    fn walk_forward_within_page() {
        // Backward walk from offset 5 hits NO_DATA; forward walk reaches the
        // price at offset 50, confirming the back-then-forward order.
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts_data = 64 * 50;
        writer.write_price(ts_data, 50000.0).expect("write");
        let ts_query = 64 * 5;
        let results = cache
            .lookup_batch(Currency::Usd, &[ts_query])
            .expect("lookup");
        let val = results[0]
            .as_price()
            .expect("expected Some via forward walk");
        assert!(
            (val - 50000.0).abs() <= 0.01,
            "expected ~50000.0, got {val}"
        );
    }

    #[test]
    fn walk_does_not_cross_page_boundary() {
        // Data at the last sample of page 0 must not be visible from the
        // first sample of page 1: the walk is bounded to the current page.
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts_end_p0 = (SAMPLES_PER_PAGE as u64 - 1) * 64;
        writer.write_price(ts_end_p0, 50000.0).expect("write");
        let ts_start_p1 = SAMPLES_PER_PAGE as u64 * 64;
        let results = cache
            .lookup_batch(Currency::Usd, &[ts_start_p1])
            .expect("lookup");
        assert!(
            results[0].as_price().is_none(),
            "walk must not cross into pid - 1, got {:?}",
            results[0]
        );
    }

    #[test]
    fn walk_prefers_backward_over_forward() {
        // When valid samples exist at equal distance both ways, backward wins.
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let center = 64 * 100;
        writer.write_price(center - 64 * 3, 49000.0).expect("write");
        writer.write_price(center + 64 * 3, 51000.0).expect("write");
        let results = cache
            .lookup_batch(Currency::Usd, &[center])
            .expect("lookup");
        let val = results[0].as_price().expect("expected Some");
        assert!(
            (val - 49000.0).abs() <= 0.01,
            "expected backward value 49000.0, got {val}"
        );
    }

    #[test]
    fn walk_respects_max_walk_samples() {
        // Sample exactly MAX_WALK_SAMPLES away is found; one further is not.
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let center_offset: usize = 100;
        let center = center_offset as u64 * 64;

        let too_far = (center_offset - MAX_WALK_SAMPLES - 1) as u64 * 64;
        writer.write_price(too_far, 50000.0).expect("write");
        let results = cache
            .lookup_batch(Currency::Usd, &[center])
            .expect("lookup");
        assert!(
            results[0].as_price().is_none(),
            "value beyond MAX_WALK_SAMPLES must not be found, got {:?}",
            results[0]
        );

        let at_bound = (center_offset - MAX_WALK_SAMPLES) as u64 * 64;
        writer.write_price(at_bound, 50000.0).expect("write");
        let results = cache
            .lookup_batch(Currency::Usd, &[center])
            .expect("lookup");
        let val = results[0]
            .as_price()
            .expect("at MAX_WALK_SAMPLES should be findable");
        assert!((val - 50000.0).abs() <= 0.01, "got {val}");
    }

    #[test]
    fn walk_skips_sentinels_forward() {
        // Forward walk steps past two FETCH_ERROR slots to reach a valid one.
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let center = 64 * 5;
        writer.write_error(center + 64).expect("write");
        writer.write_error(center + 64 * 2).expect("write");
        writer.write_price(center + 64 * 3, 50000.0).expect("write");
        let results = cache
            .lookup_batch(Currency::Usd, &[center])
            .expect("lookup");
        let val = results[0]
            .as_price()
            .expect("walk should jump past sentinels");
        assert!((val - 50000.0).abs() <= 0.01, "got {val}");
    }

    #[test]
    fn sentinel_error_blocks_lookup() {
        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts = 64 * 50;
        writer.write_error(ts).expect("write");

        // FETCH_ERROR alone: walk finds nothing else in the page.
        let results = cache.lookup_batch(Currency::Usd, &[ts]).expect("lookup");
        assert!(
            results[0].as_price().is_none(),
            "FETCH_ERROR-only page should be None"
        );

        // Write a valid price one sample before the error; walk should find it.
        let ts_before = ts - 64;
        writer.write_price(ts_before, 50000.0).expect("write");
        let results = cache.lookup_batch(Currency::Usd, &[ts]).expect("lookup");
        let val = results[0]
            .as_price()
            .expect("should find price after walking past FETCH_ERROR");
        assert!(
            (val - 50000.0).abs() <= 0.01,
            "expected ~50000.0 after walking past error, got {val}"
        );
    }

    #[test]
    fn persistence() {
        let dir = tempdir().unwrap();
        let currencies = &[Currency::Usd];
        let ts = 64 * 42;

        // Write and drop the cache
        {
            let cache = make_cache(dir.path(), currencies);
            cache
                .writer(Currency::Usd)
                .write_price(ts, 50000.0)
                .expect("write");
        }

        // Re-open storage + new cache and verify the value survived
        let cache = make_cache(dir.path(), currencies);
        let results = cache.lookup_batch(Currency::Usd, &[ts]).expect("lookup");
        let val = results[0]
            .as_price()
            .expect("persisted value should survive cache drop and re-open");
        assert!(
            (val - 50000.0).abs() <= 0.01,
            "expected ~50000.0 after re-open, got {val}"
        );
    }

    #[test]
    fn concurrent_reads() {
        use std::thread;

        let dir = tempdir().unwrap();
        let cache = make_cache(dir.path(), &[Currency::Usd]);
        let writer = cache.writer(Currency::Usd);
        let ts = 64 * 5;
        writer.write_price(ts, 50000.0).expect("write");

        let c1 = Arc::clone(&cache);
        let c2 = Arc::clone(&cache);

        let t1 = thread::spawn(move || c1.lookup_batch(Currency::Usd, &[ts]));
        let t2 = thread::spawn(move || c2.lookup_batch(Currency::Usd, &[ts]));

        let r1 = t1.join().expect("thread 1 panicked").expect("lookup");
        let r2 = t2.join().expect("thread 2 panicked").expect("lookup");
        assert!(r1[0].as_price().is_some(), "thread 1 should find the price");
        assert!(r2[0].as_price().is_some(), "thread 2 should find the price");
    }
}
