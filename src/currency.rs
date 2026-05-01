use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const NO_DATA: u32 = 0;
pub const FETCH_ERROR: u32 = u32::MAX;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Currency {
    Usd,
    Eur,
    Gbp,
    Cad,
    Jpy,
    Krw,
}

pub fn scale(c: Currency) -> i8 {
    match c {
        Currency::Usd | Currency::Eur | Currency::Gbp | Currency::Cad => 2,
        // KRW and JPY are large-magnitude (~10⁸ for BTC/KRW, ~10⁷ for BTC/JPY);
        // a negative scale widens the bin so a u32 still spans far above
        // current prices.
        Currency::Jpy | Currency::Krw => -5,
    }
}

pub fn to_stored(c: Currency, price: f64) -> u32 {
    // Sanitize bad inputs (NaN, +/-inf, negative) before encoding. Without
    // this, NaN would survive the pipeline: `NaN.clamp(...)` returns NaN, and
    // `NaN as u32` saturates to 0 (the NO_DATA sentinel), silently turning a
    // caller bug into "no data ever recorded". Folding bad inputs to 0.0 here
    // lets the later clamp lift them to 1 instead: a valid stored value,
    // distinct from both sentinels.
    let price = if price.is_finite() && price >= 0.0 {
        price
    } else {
        0.0
    };

    // Fixed-point u32 encoding. Multiplying f64 by 2^k only flips the float's
    // exponent field, so the scale step is bit-exact, equivalent to a binary
    // shift; only `round()` quantizes, and the only thing lost is unneeded
    // precision (< $0.25 USD-equivalent, well below any meaningful BTC
    // spread). Power-of-two scale also lets `scale(c)` go negative for
    // high-magnitude currencies (widening the bin) so a u32 still covers
    // ~10,000x current BTC prices.
    let scaled = price * 2f64.powi(scale(c) as i32);
    let rounded = scaled.round();

    // clamp to [1, u32::MAX - 1] to avoid sentinel collision
    rounded.clamp(1.0, (u32::MAX - 1) as f64) as u32
}

pub fn from_stored(c: Currency, stored: u32) -> Option<f64> {
    if stored == NO_DATA || stored == FETCH_ERROR {
        return None;
    }
    Some(stored as f64 * 2f64.powi(-(scale(c) as i32)))
}

impl FromStr for Currency {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        serde_json::from_str(&format!("\"{s}\""))
            .map_err(|_| crate::error::Error::UnknownCurrency(s.to_string()))
    }
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Reuse serde's UPPERCASE rename so the wire/file/display name stays
        // in one place. Serialization of a fieldless variant is infallible.
        let s = serde_json::to_string(self).map_err(|_| std::fmt::Error)?;
        f.write_str(s.trim_matches('"'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_round_trip() {
        let cases: &[(Currency, f64, f64)] = &[
            (Currency::Usd, 67234.5, 0.01),
            (Currency::Eur, 62000.0, 0.01),
            (Currency::Gbp, 53000.0, 0.01),
            (Currency::Cad, 91000.0, 0.01),
            (Currency::Jpy, 10_000_000.0, 32.0), // scale -5 → 1 LSB = 2^5 = 32
            (Currency::Krw, 150_000_000.0, 32.0), // same scale as JPY
        ];
        for &(currency, price, tolerance) in cases {
            let stored = to_stored(currency, price);
            let back = from_stored(currency, stored).expect("should not be sentinel");
            let diff = (back - price).abs();
            assert!(
                diff <= tolerance,
                "{currency:?}: {price} → stored={stored} → {back}, diff={diff} > tol={tolerance}"
            );
        }
    }

    #[test]
    fn sentinel_none() {
        assert_eq!(from_stored(Currency::Usd, NO_DATA), None);
        assert_eq!(from_stored(Currency::Usd, FETCH_ERROR), None);
    }

    #[test]
    fn from_str_uppercase() {
        assert_eq!(Currency::from_str("EUR").unwrap(), Currency::Eur);
        assert_eq!(Currency::from_str("USD").unwrap(), Currency::Usd);
    }

    #[test]
    fn from_str_lowercase_rejected() {
        assert!(Currency::from_str("eur").is_err());
        assert!(Currency::from_str("usd").is_err());
    }

    #[test]
    fn serde_round_trip() {
        let variants = [
            Currency::Usd,
            Currency::Eur,
            Currency::Gbp,
            Currency::Cad,
            Currency::Jpy,
        ];
        for variant in variants {
            let json = serde_json::to_string(&variant).expect("serialize");
            let back: Currency = serde_json::from_str(&json).expect("deserialize");
            assert_eq!(
                back, variant,
                "round-trip failed for {variant:?}: json={json}"
            );
        }
    }

    #[test]
    fn serde_uses_uppercase_keys() {
        let json = serde_json::to_string(&Currency::Usd).unwrap();
        assert_eq!(json, "\"USD\"");
        let json = serde_json::to_string(&Currency::Eur).unwrap();
        assert_eq!(json, "\"EUR\"");
    }

    #[test]
    fn to_stored_clamps_away_from_sentinels() {
        // very small positive price should become 1, not 0
        let stored = to_stored(Currency::Usd, 1e-30);
        assert!(stored >= 1);
        assert_ne!(stored, NO_DATA);
        assert_ne!(stored, FETCH_ERROR);
    }
}
