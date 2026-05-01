mod api;
mod cache;
mod config;
mod currency;
mod error;
mod estimator;
mod fetcher;
mod paging;
mod state;
mod storage;

use std::sync::{Arc, RwLock};

use clap::Parser;

use state::{ServiceState, Status};

#[tokio::main]
async fn main() -> error::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let args = config::Args::parse();
    fetcher::historical::set_target_candles_per_sec(args.target_candles_per_sec);
    if let Some(t) = args.target_candles_per_sec {
        tracing::info!(
            target_candles_per_sec = t,
            "operating TARGET overridden via CLI"
        );
    } else {
        tracing::info!("operating TARGET will be derived per-currency from per-source caps");
    }
    let cfg = config::load_config(&args.config)?;
    validate_currencies(&cfg.currencies)?;
    std::fs::create_dir_all(&cfg.data_dir)?;

    let storage = Arc::new(storage::Storage::open(&cfg.data_dir, &cfg.currencies)?);
    let cache = cache::PriceCache::new(&cfg.currencies, Arc::clone(&storage));

    // Find the earliest timestamp already in the DB (may be None for a fresh install).
    let history_start = earliest_history_start(&storage, &cfg.currencies)?;

    let service_state = Arc::new(RwLock::new(ServiceState {
        currencies: cfg.currencies.clone(),
        history_start,
        status: Status::Serving,
    }));

    if let Some(from_ts) = args.rescan_from {
        let now = fetcher::historical::now();
        tracing::info!(from_ts, to_ts = now, "starting historical backfill");

        // from_ts may be earlier than any existing data — update history_start.
        let new_history_start = Some(match history_start {
            Some(existing) => existing.min(from_ts),
            None => from_ts,
        });

        let minutes_per_currency = (now.saturating_sub(from_ts)) / 60;
        let total_minutes = minutes_per_currency * cfg.currencies.len() as u64;

        {
            let mut s = service_state.write().expect("poisoned");
            s.history_start = new_history_start;
            s.status = Status::Sync {
                total_minutes,
                done_minutes: 0,
                eta_secs: None,
                estimator: estimator::BackfillEstimator::with_defaults(),
            };
        }

        let state_for_backfill = Arc::clone(&service_state);
        let currencies_for_backfill = cfg.currencies.clone();
        let cache_for_backfill = Arc::clone(&cache);
        let currencies_count = cfg.currencies.len() as u64;

        tokio::spawn(async move {
            for (i, &c) in currencies_for_backfill.iter().enumerate() {
                let writer = cache_for_backfill.writer(c);
                let base = i as u64 * minutes_per_currency;
                let state2 = Arc::clone(&state_for_backfill);

                let sources = fetcher::historical::all_sources();
                let cb = move |local_done: u64| {
                    let total =
                        fetcher::historical::now().saturating_sub(from_ts) / 60 * currencies_count;
                    let mut s = state2.write().expect("poisoned");
                    s.status.tick(base + local_done, total);
                };
                match fetcher::historical::run_backfill(sources, c, from_ts, &writer, Some(&cb))
                    .await
                {
                    Ok(()) => tracing::info!(?c, "backfill complete"),
                    Err(e) => tracing::error!(?e, ?c, "backfill failed"),
                }
            }

            // Transition to serving; re-compute history_start from DB.
            let final_start = {
                // We know from_ts is the earliest since we set it above.
                new_history_start
            };
            let mut s = state_for_backfill.write().expect("poisoned");
            s.history_start = final_start;
            s.status = Status::Serving;
            tracing::info!("all backfill complete, now serving");

            // Start live polling now that historical data is ready.
            let _live = fetcher::Fetcher::new(&cache_for_backfill, &currencies_for_backfill)
                .spawn_live_poll();
        });
    } else {
        // No backfill — start live polling immediately.
        let _live_handle = fetcher::Fetcher::new(&cache, &cfg.currencies).spawn_live_poll();
    }

    let state = api::AppState {
        cache: Arc::clone(&cache),
        service_state: Arc::clone(&service_state),
    };
    let router = api::router(state);
    let listener = tokio::net::TcpListener::bind(&cfg.bind_addr)
        .await
        .map_err(error::Error::Io)?;
    tracing::info!(addr = %cfg.bind_addr, "listening");
    axum::serve(listener, router)
        .await
        .map_err(|e| error::Error::Other(format!("axum: {e}")))?;

    Ok(())
}

/// Reject any currency that has fewer than [`fetcher::historical::MIN_PRICE_SOURCES`]
/// historical exchange sources. Those currencies can never produce a valid median
/// and would silently result in an empty DB.
fn validate_currencies(currencies: &[currency::Currency]) -> error::Result<()> {
    let sources = fetcher::historical::all_sources();
    for &c in currencies {
        let n = sources.iter().filter(|s| s.supports(c)).count();
        if n < fetcher::historical::MIN_PRICE_SOURCES {
            return Err(error::Error::Other(format!(
                "currency {c} has only {n} historical source(s); \
                 at least {} are required to produce a median price",
                fetcher::historical::MIN_PRICE_SOURCES,
            )));
        }
    }
    Ok(())
}

fn earliest_history_start(
    storage: &storage::Storage,
    currencies: &[currency::Currency],
) -> error::Result<Option<u64>> {
    let mut earliest: Option<u64> = None;
    for &c in currencies {
        if let Some(ts) = storage.first_timestamp(c)? {
            earliest = Some(match earliest {
                Some(e) => e.min(ts),
                None => ts,
            });
        }
    }
    Ok(earliest)
}
