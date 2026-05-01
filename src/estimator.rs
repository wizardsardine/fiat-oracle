use std::time::Instant;

const DEFAULT_ALPHA: f64 = 0.01;
const DEFAULT_WARMUP: u32 = 10;

/// EMA-based estimator for remaining backfill time.
///
/// Tracks wall-clock seconds per history-minute processed.
#[derive(Debug, Clone)]
pub struct BackfillEstimator {
    alpha: f64,
    warmup: u32,
    last_done: u64,
    remaining: u64,
    last_time: Option<Instant>,
    ema_secs_per_minute: f64,
    samples: u32,
}

impl BackfillEstimator {
    pub fn new(alpha: f64, warmup: u32) -> Self {
        Self {
            alpha,
            warmup,
            last_done: 0,
            remaining: 0,
            last_time: None,
            ema_secs_per_minute: 0.0,
            samples: 0,
        }
    }

    pub fn with_defaults() -> Self {
        Self::new(DEFAULT_ALPHA, DEFAULT_WARMUP)
    }

    /// Update from a progress sample: `done` minutes processed so far, `total` overall.
    pub fn update(&mut self, done: u64, total: u64) {
        let now = Instant::now();

        if let Some(last_time) = self.last_time {
            let delta = done.saturating_sub(self.last_done);
            if delta == 0 {
                return;
            }

            let elapsed = now.duration_since(last_time).as_secs_f64();

            // Skip samples where too much wall-clock time passed (pause/restart).
            if elapsed > 60.0 {
                self.last_done = done;
                self.last_time = Some(now);
                return;
            }

            let rate = elapsed / delta as f64;

            if self.samples > 0 {
                self.ema_secs_per_minute =
                    self.alpha * rate + (1.0 - self.alpha) * self.ema_secs_per_minute;
            } else {
                self.ema_secs_per_minute = rate;
            }
            self.samples += 1;
        }

        self.last_done = done;
        self.remaining = total.saturating_sub(done);
        self.last_time = Some(now);
    }

    /// Estimated remaining seconds; returns `None` before enough samples are collected.
    pub fn estimate_secs(&self) -> Option<u64> {
        if self.samples < self.warmup {
            return None;
        }
        Some((self.remaining as f64 * self.ema_secs_per_minute) as u64)
    }
}

impl Default for BackfillEstimator {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{thread, time::Duration};

    #[test]
    fn no_estimate_during_warmup() {
        let mut est = BackfillEstimator::new(0.1, 3);
        est.update(1_000, 10_000);
        assert_eq!(est.estimate_secs(), None);

        thread::sleep(Duration::from_millis(50));
        est.update(2_000, 10_000);
        assert_eq!(est.estimate_secs(), None, "only 1 sample so far");

        thread::sleep(Duration::from_millis(50));
        est.update(3_000, 10_000);
        assert_eq!(est.estimate_secs(), None, "only 2 samples so far");
    }

    #[test]
    fn estimate_after_warmup() {
        let mut est = BackfillEstimator::new(0.1, 3);
        est.update(0, 10_000);
        for i in 1..=3 {
            thread::sleep(Duration::from_millis(50));
            est.update(i * 1_000, 10_000);
        }
        assert_eq!(est.samples, 3);
        let eta = est
            .estimate_secs()
            .expect("should have estimate after warmup");
        assert!(eta < 10, "ETA should be small for fast progress, got {eta}");
    }

    #[test]
    fn zero_delta_skipped() {
        let mut est = BackfillEstimator::new(0.1, 3);
        est.update(1_000, 10_000);
        thread::sleep(Duration::from_millis(50));
        est.update(1_000, 10_000); // same done count
        assert_eq!(est.samples, 0, "no sample when done didn't advance");
    }
}
