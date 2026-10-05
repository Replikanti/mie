//! Fixed-point prices and quantities (ADR-027, proposed).
//!
//! Exchange values arrive as decimal strings. Holding them as integers at one
//! fixed scale keeps sums (delta, CVD, volume at price) exact and makes price
//! levels comparable with `==`, so feature values cannot drift between live
//! processing and replay. Ratios and statistics are derived from these exact
//! inputs at the feature level.

use std::fmt;

/// Decimal places carried by [`Price`] and [`Qty`].
pub const DECIMALS: u32 = 8;

/// Units per whole number: `10^DECIMALS`.
pub const SCALE: i64 = 10_i64.pow(DECIMALS);

/// A price in the quote currency (USDT), as a count of `1 / SCALE` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

impl Price {
    /// Creates a price from a count of `1 / SCALE` units.
    pub const fn from_units(units: i64) -> Self {
        Self(units)
    }

    /// The count of `1 / SCALE` units.
    pub const fn units(self) -> i64 {
        self.0
    }
}

/// A quantity in the base currency (BTC), as a count of `1 / SCALE` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(i64);

impl Qty {
    /// Creates a quantity from a count of `1 / SCALE` units.
    pub const fn from_units(units: i64) -> Self {
        Self(units)
    }

    /// The count of `1 / SCALE` units.
    pub const fn units(self) -> i64 {
        self.0
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(self.0, f)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(self.0, f)
    }
}

fn fmt_fixed(units: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let sign = if units < 0 { "-" } else { "" };
    let abs = units.unsigned_abs();
    let scale = SCALE.unsigned_abs();
    write!(
        f,
        "{sign}{}.{:0width$}",
        abs / scale,
        abs % scale,
        width = DECIMALS as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn displays_all_decimal_places() {
        assert_eq!(
            Price::from_units(6_354_210_000_000).to_string(),
            "63542.10000000"
        );
        assert_eq!(Qty::from_units(1_500_000).to_string(), "0.01500000");
        assert_eq!(Qty::from_units(0).to_string(), "0.00000000");
    }

    #[test]
    fn displays_negative_values_and_extremes() {
        assert_eq!(Qty::from_units(-1_500_000).to_string(), "-0.01500000");
        assert_eq!(
            Qty::from_units(i64::MIN).to_string(),
            "-92233720368.54775808"
        );
    }
}
