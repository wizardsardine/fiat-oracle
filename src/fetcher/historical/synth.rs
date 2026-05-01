//! Synthesize 1-minute candles from a coarser-interval feed.
//!
//! Used when an exchange's 1m endpoint is too shallow to satisfy the
//! 1-year backfill rule but its 1h (or other coarser) endpoint reaches
//! deeper. We re-emit the parent close across each minute it covers.
//!
//! The downstream aggregator only consumes the close, so OHLC fidelity
//! within the parent interval is intentionally lost — every minute in a
//! parent bucket gets the same close. That is coarse but bounded: a 1h
//! parent can drift the synthesized minute price by at most one hour of
//! intra-bucket movement.
//!
//! Daily synthesis is **not allowed** — too coarse to be meaningfully
//! representative of intraday OHLC. Callers gate on that themselves.

/// Expand each `(parent_ts, close)` candle into `parent_interval_secs / 60`
/// consecutive 1-minute candles, all carrying the parent close.
///
/// `parent_interval_secs` must be a positive multiple of 60. Inputs that
/// fail those preconditions return an empty vec.
///
/// Currently no source uses this — kept for future use (e.g. an exchange
/// whose only useful interval is coarser than 1m). Allowed dead until then.
#[allow(dead_code)]
pub fn expand_to_minutes(parents: &[(u64, f64)], parent_interval_secs: u64) -> Vec<(u64, f64)> {
    if parent_interval_secs == 0 || parent_interval_secs % 60 != 0 {
        return Vec::new();
    }
    let minutes_per_parent = (parent_interval_secs / 60) as usize;
    let mut out = Vec::with_capacity(parents.len() * minutes_per_parent);
    for &(parent_ts, close) in parents {
        for i in 0..minutes_per_parent {
            out.push((parent_ts + (i as u64) * 60, close));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn expand_1h_to_60_minutes() {
        // One 1h parent at midnight → 60 minute candles, all with the same close.
        let parents = vec![(0u64, 100.0)];
        let out = expand_to_minutes(&parents, 3600);
        assert_eq!(out.len(), 60);
        assert_eq!(out[0], (0, 100.0));
        assert_eq!(out[1], (60, 100.0));
        assert_eq!(out[59], (3540, 100.0));
    }

    #[test]
    fn expand_5m_to_5_minutes() {
        let parents = vec![(60u64, 50.5), (360u64, 51.0)];
        let out = expand_to_minutes(&parents, 300);
        assert_eq!(out.len(), 10);
        assert_eq!(out[0], (60, 50.5));
        assert_eq!(out[4], (60 + 240, 50.5));
        assert_eq!(out[5], (360, 51.0));
        assert_eq!(out[9], (360 + 240, 51.0));
    }

    #[test]
    fn empty_input_returns_empty() {
        assert!(expand_to_minutes(&[], 3600).is_empty());
    }

    #[test]
    fn invalid_interval_returns_empty() {
        let parents = vec![(0u64, 100.0)];
        // Sub-minute interval makes no sense.
        assert!(expand_to_minutes(&parents, 30).is_empty());
        // Zero interval rejected.
        assert!(expand_to_minutes(&parents, 0).is_empty());
        // Non-multiple-of-60 rejected (would mis-align the 1m grid).
        assert!(expand_to_minutes(&parents, 90).is_empty());
    }

    #[test]
    fn timestamps_strictly_increasing_within_parent() {
        let parents = vec![(1714521600u64, 60_000_000.0)];
        let out = expand_to_minutes(&parents, 3600);
        for w in out.windows(2) {
            assert_eq!(w[1].0 - w[0].0, 60);
        }
    }
}
