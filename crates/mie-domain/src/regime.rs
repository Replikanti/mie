//! Volatility regime (brief §9, ADR-017).
//!
//! The canonical initial regime maps the ATR percentile onto four labels. The
//! raw percentile is always retained next to its label, and the regime is
//! context — never an entry signal on its own.

use std::fmt;

/// ATR percentile on the canonical 0–100 scale (ADR-017).
///
/// How the percentile is computed — ATR(14), lookback 200, bar timeframe,
/// rank method — belongs to the volatility-regime feature; this type carries
/// the result and maps it to its canonical label.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AtrPercentile(f64);

impl AtrPercentile {
    /// Validates a percentile.
    ///
    /// # Errors
    ///
    /// [`PercentileOutOfRange`] unless `0.0 <= value <= 100.0` (NaN fails).
    pub fn new(value: f64) -> Result<Self, PercentileOutOfRange> {
        if (0.0..=100.0).contains(&value) {
            Ok(Self(value))
        } else {
            Err(PercentileOutOfRange(value))
        }
    }

    /// The raw percentile.
    pub fn value(self) -> f64 {
        self.0
    }

    /// The canonical label for this percentile.
    pub fn label(self) -> RegimeLabel {
        RegimeLabel::from_percentile(self)
    }
}

/// Canonical ATR-percentile regime labels (ADR-017).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum RegimeLabel {
    /// Percentile 0–25.
    Low,
    /// Percentile 26–50.
    Medium,
    /// Percentile 51–75.
    High,
    /// Percentile 76–100.
    Extreme,
}

impl RegimeLabel {
    /// Maps a percentile onto the canonical scale.
    ///
    /// ADR-017 states the bands on whole percentiles (0–25, 26–50, 51–75,
    /// 76–100). Fractional percentiles fall into upper-closed bands — `[0, 25]`,
    /// `(25, 50]`, `(50, 75]`, `(75, 100]` — which is identical to ADR-017 on
    /// every whole number and to round-half-up on the half-steps a 200-bar
    /// lookback produces.
    pub fn from_percentile(percentile: AtrPercentile) -> Self {
        match percentile.value() {
            v if v <= 25.0 => Self::Low,
            v if v <= 50.0 => Self::Medium,
            v if v <= 75.0 => Self::High,
            _ => Self::Extreme,
        }
    }
}

/// A regime classification: the canonical label plus the raw percentile it
/// came from (ADR-017 requires retaining both).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Regime {
    /// The raw ATR percentile.
    pub atr_percentile: AtrPercentile,
    /// Its canonical label.
    pub label: RegimeLabel,
}

impl From<AtrPercentile> for Regime {
    fn from(atr_percentile: AtrPercentile) -> Self {
        Self {
            atr_percentile,
            label: atr_percentile.label(),
        }
    }
}

/// A percentile outside `0..=100`, or NaN.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PercentileOutOfRange(pub f64);

impl fmt::Display for PercentileOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ATR percentile {} is outside 0..=100", self.0)
    }
}

impl std::error::Error for PercentileOutOfRange {}

#[cfg(test)]
mod tests {
    use super::*;

    fn label(value: f64) -> RegimeLabel {
        AtrPercentile::new(value).unwrap().label()
    }

    #[test]
    fn whole_number_bands_match_adr_017() {
        for (value, expected) in [
            (0.0, RegimeLabel::Low),
            (25.0, RegimeLabel::Low),
            (26.0, RegimeLabel::Medium),
            (50.0, RegimeLabel::Medium),
            (51.0, RegimeLabel::High),
            (75.0, RegimeLabel::High),
            (76.0, RegimeLabel::Extreme),
            (100.0, RegimeLabel::Extreme),
        ] {
            assert_eq!(label(value), expected, "percentile {value}");
        }
    }

    #[test]
    fn fifty_seven_is_high_not_medium() {
        // The worked example from the Market State & Regime brief.
        assert_eq!(label(57.0), RegimeLabel::High);
    }

    #[test]
    fn fractional_percentiles_use_upper_closed_bands() {
        assert_eq!(label(25.4), RegimeLabel::Medium);
        assert_eq!(label(25.5), RegimeLabel::Medium);
        assert_eq!(label(50.5), RegimeLabel::High);
        assert_eq!(label(75.5), RegimeLabel::Extreme);
    }

    #[test]
    fn rejects_values_off_the_scale() {
        for value in [-0.1, 100.1, f64::NAN, f64::INFINITY] {
            assert!(AtrPercentile::new(value).is_err(), "accepted {value}");
        }
    }

    #[test]
    fn regime_retains_the_raw_percentile() {
        let regime = Regime::from(AtrPercentile::new(57.0).unwrap());
        assert_eq!(regime.atr_percentile.value(), 57.0);
        assert_eq!(regime.label, RegimeLabel::High);
    }
}
