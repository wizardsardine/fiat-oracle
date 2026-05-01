// The only two design knobs: every other paging constant and bit-op below is
// derived from these. Sampling cadence is one sample every 2^SAMPLE_INTERVAL_BITS
// seconds; one page spans 2^PAGE_SPAN_BITS seconds.
pub const SAMPLE_INTERVAL_BITS: u32 = 6; // -> 64 s between samples
pub const PAGE_SPAN_BITS: u32 = 16; // -> 65536 s ~= 18h12m per page

pub const SAMPLE_INTERVAL_SECS: u64 = 1 << SAMPLE_INTERVAL_BITS; // 64 s
pub const SAMPLES_PER_PAGE: usize = 1 << (PAGE_SPAN_BITS - SAMPLE_INTERVAL_BITS); // 1024

// Max neighbour samples a lookup walk inspects around a queried timestamp,
// bounded to the current page. 57 * 64s ~= 1 hour.
pub const MAX_WALK_SAMPLES: usize = 57;

// SAMPLES_PER_PAGE = 1 << (PAGE_SPAN_BITS - SAMPLE_INTERVAL_BITS), so a page
// must span strictly more than one sample interval. Equal would give 1 sample
// per page (paging is pointless); smaller would underflow the const subtraction.
const _: () = assert!(PAGE_SPAN_BITS > SAMPLE_INTERVAL_BITS);

pub fn page_id(timestamp: u64) -> u32 {
    (timestamp >> PAGE_SPAN_BITS) as u32
}

pub fn sample_offset(timestamp: u64) -> usize {
    ((timestamp >> SAMPLE_INTERVAL_BITS) & (SAMPLES_PER_PAGE as u64 - 1)) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn page_id_boundaries() {
        assert_eq!(page_id(0), 0);
        assert_eq!(page_id(65535), 0);
        assert_eq!(page_id(65536), 1);
    }

    #[test]
    fn sample_offset_boundaries() {
        assert_eq!(sample_offset(0), 0);
        assert_eq!(sample_offset(63), 0);
        assert_eq!(sample_offset(64), 1);
        assert_eq!(sample_offset(65535), 1023);
    }

    #[test]
    fn round_trip() {
        let test_cases: &[(u32, usize)] = &[
            (0, 0),
            (0, 1),
            (0, 1023),
            (1, 0),
            (1, 512),
            (42, 7),
            (100, 999),
        ];
        for &(p, s) in test_cases {
            let ts = p as u64 * (SAMPLES_PER_PAGE as u64 * SAMPLE_INTERVAL_SECS)
                + s as u64 * SAMPLE_INTERVAL_SECS;
            assert_eq!(page_id(ts), p, "page_id failed for p={p}, s={s}");
            assert_eq!(
                sample_offset(ts),
                s,
                "sample_offset failed for p={p}, s={s}"
            );
        }
    }
}
