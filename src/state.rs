use crate::{currency::Currency, estimator::BackfillEstimator};

pub struct ServiceState {
    pub currencies: Vec<Currency>,
    pub history_start: Option<u64>,
    pub status: Status,
}

pub enum Status {
    Sync {
        total_minutes: u64,
        done_minutes: u64,
        eta_secs: Option<u64>,
        estimator: BackfillEstimator,
    },
    Serving,
}

impl Status {
    /// Called from the aggregator thread each time a batch of buckets is finalised.
    /// `total` should be recomputed fresh by the caller on each invocation so the
    /// estimate tracks the actual remaining work as `now()` advances.
    /// No-op if the status is already Serving.
    pub fn tick(&mut self, done: u64, total: u64) {
        if let Status::Sync {
            done_minutes,
            total_minutes,
            eta_secs,
            estimator,
        } = self
        {
            *done_minutes = done;
            *total_minutes = total;
            estimator.update(done, total);
            *eta_secs = estimator.estimate_secs();
        }
    }
}
