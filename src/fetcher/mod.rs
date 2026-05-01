pub mod historical;
pub mod sources;
pub mod spot;

use std::{sync::Arc, time::Duration};

use tokio::task::JoinHandle;

use crate::{cache::PriceCache, currency::Currency, paging::SAMPLE_INTERVAL_SECS};

/// How far back each live-poll tick re-fetches. Wider than
/// `SAMPLE_INTERVAL_SECS` so any minute that finalised between two ticks
/// is caught by the next one. `run_backfill`'s `tip()` clamp turns this
/// into "fetch from where we last left off" after the first tick.
const LIVE_POLL_WINDOW_SECS: u64 = 10 * 60;

pub struct Fetcher {
    cache: Arc<PriceCache>,
    currencies: Vec<Currency>,
}

impl Fetcher {
    pub fn new(cache: &Arc<PriceCache>, currencies: &[Currency]) -> Self {
        Self {
            cache: Arc::clone(cache),
            currencies: currencies.to_vec(),
        }
    }

    /// Spawn the live poll loop. Each tick (every `SAMPLE_INTERVAL_SECS`)
    /// runs `historical::run_backfill` per currency over the last
    /// `LIVE_POLL_WINDOW_SECS`. This is the same code path as
    /// `--rescan-from`, so live writes are reproducible by a later backfill
    /// (see plan: "Make live poll reproducible with backfill").
    pub fn spawn_live_poll(self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(SAMPLE_INTERVAL_SECS));
            loop {
                // Tokio's first tick resolves immediately, so the first poll
                // fires at t=0 and subsequent ones every SAMPLE_INTERVAL_SECS.
                interval.tick().await;
                let from_ts = historical::now().saturating_sub(LIVE_POLL_WINDOW_SECS);
                for &currency in &self.currencies {
                    let writer = self.cache.writer(currency);
                    tokio::spawn(async move {
                        let sources = historical::all_sources();
                        if let Err(e) =
                            historical::run_backfill(sources, currency, from_ts, &writer, None)
                                .await
                        {
                            tracing::warn!(?currency, ?e, "live poll backfill failed");
                        }
                    });
                }
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use super::*;
    use crate::storage::Storage;

    fn make_cache(dir: &Path, currencies: &[Currency]) -> Arc<PriceCache> {
        let storage = Arc::new(Storage::open(dir, currencies).unwrap());
        PriceCache::new(currencies, storage)
    }

    #[test]
    fn fetcher_new_records_currencies() {
        let dir = tempdir().unwrap();
        let currencies = [Currency::Usd, Currency::Eur];
        let cache = make_cache(dir.path(), &currencies);
        let fetcher = Fetcher::new(&cache, &currencies);
        assert_eq!(fetcher.currencies, vec![Currency::Usd, Currency::Eur]);
    }
}
