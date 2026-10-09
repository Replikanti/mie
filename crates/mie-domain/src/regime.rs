//! Volatility regime (brief §9, ADR-017, ADR-033).
//!
//! The canonical initial regime maps the ATR percentile onto four labels. The
//! raw percentile is always retained next to its label, and the regime is
//! context — never an entry signal on its own. How the percentile is
//! computed — ATR(14) on 1h bars, previous-200 at-or-below rank — is the
//! `volatility.regime.1h@1` feature in [`volatility`](crate::volatility).

use crate::feature::FeatureKey;
use crate::fingerprint::Fingerprinter;
use crate::state_hash::StateEncode;
use std::fmt;

/// ATR percentile on the canonical 0–100 scale (ADR-017).
///
/// How the percentile is computed — ATR(14), lookback 200, bar timeframe,
/// rank method — belongs to the volatility-regime feature; this type carries
/// the result and maps it to its canonical label.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd)]
pub struct AtrPercentile(f64);

impl StateEncode for AtrPercentile {
    /// The bit pattern of the `f64` (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        let Self(value) = self;
        value.encode(f);
    }
}

/// Sound because every constructor rejects NaN, so `==` is reflexive.
impl Eq for AtrPercentile {}

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

    /// The percentile of a rank: `100 * at_or_below / lookback` (ADR-033,
    /// decision 3). Exact for a lookback of 200, whose values are the
    /// half-steps `k / 2`.
    ///
    /// # Errors
    ///
    /// [`PercentileOutOfRange`] if `at_or_below > lookback` or `lookback`
    /// is 0.
    pub fn from_rank(at_or_below: u16, lookback: u16) -> Result<Self, PercentileOutOfRange> {
        Self::new(f64::from(at_or_below) * 100.0 / f64::from(lookback))
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

impl StateEncode for RegimeLabel {
    /// `write_u8` in declaration order: Low 0, Medium 1, High 2, Extreme 3
    /// (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::Low => 0,
            Self::Medium => 1,
            Self::High => 2,
            Self::Extreme => 3,
        });
    }
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

    /// The canonical name: `LOW`, `MEDIUM`, `HIGH` or `EXTREME`.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Low => "LOW",
            Self::Medium => "MEDIUM",
            Self::High => "HIGH",
            Self::Extreme => "EXTREME",
        }
    }
}

impl fmt::Display for RegimeLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A regime classification: the canonical label plus the raw percentile it
/// came from (ADR-017 requires retaining both), and the feature version that
/// produced it (ADR-029).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `HIGH 57.5 volatility.regime.1h@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Regime {
    /// The raw ATR percentile.
    pub atr_percentile: AtrPercentile,
    /// Its canonical label.
    pub label: RegimeLabel,
    /// The feature that computed it, such as `volatility.regime.1h@1`.
    pub feature: FeatureKey,
}

impl StateEncode for Regime {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            atr_percentile,
            label,
            feature,
        } = self;
        atr_percentile.encode(f);
        label.encode(f);
        feature.encode(f);
    }
}

impl Regime {
    /// The regime of `atr_percentile`, as computed by `feature`.
    pub fn new(atr_percentile: AtrPercentile, feature: FeatureKey) -> Self {
        Self {
            atr_percentile,
            label: atr_percentile.label(),
            feature,
        }
    }
}

impl fmt::Display for Regime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {}",
            self.label,
            self.atr_percentile.value(),
            self.feature
        )
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
    fn regime_retains_the_raw_percentile_and_its_feature() {
        let feature = FeatureKey::new("volatility.regime.1h", 1);
        let regime = Regime::new(AtrPercentile::new(57.0).unwrap(), feature);
        assert_eq!(regime.atr_percentile.value(), 57.0);
        assert_eq!(regime.label, RegimeLabel::High);
        assert_eq!(regime.feature, feature);
        assert_eq!(regime.to_string(), "HIGH 57 volatility.regime.1h@1");
        let half = Regime::new(AtrPercentile::from_rank(115, 200).unwrap(), feature);
        assert_eq!(half.to_string(), "HIGH 57.5 volatility.regime.1h@1");
    }

    #[test]
    fn ranks_on_a_200_bar_lookback_are_half_steps_in_adr_017_bands() {
        // ADR-033 decision 4: upper-closed bands on k / 2 equal round-half-up
        // followed by the ADR-017 whole-number bands.
        for k in 0..=200_u16 {
            let percentile = AtrPercentile::from_rank(k, 200).unwrap();
            assert_eq!(percentile.value() * 2.0, f64::from(k), "k = {k}");
            // Round half up of k / 2.
            let rounded = k.div_ceil(2);
            let expected = match rounded {
                0..=25 => RegimeLabel::Low,
                26..=50 => RegimeLabel::Medium,
                51..=75 => RegimeLabel::High,
                _ => RegimeLabel::Extreme,
            };
            assert_eq!(percentile.label(), expected, "k = {k}");
        }
        assert_eq!(AtrPercentile::from_rank(0, 200).unwrap().value(), 0.0);
        assert_eq!(AtrPercentile::from_rank(200, 200).unwrap().value(), 100.0);
    }

    #[test]
    fn from_rank_rejects_impossible_ranks() {
        assert!(AtrPercentile::from_rank(201, 200).is_err());
        assert!(AtrPercentile::from_rank(0, 0).is_err());
        assert!(AtrPercentile::from_rank(1, 0).is_err());
    }

    #[test]
    fn labels_have_canonical_names() {
        let names: Vec<_> = [
            RegimeLabel::Low,
            RegimeLabel::Medium,
            RegimeLabel::High,
            RegimeLabel::Extreme,
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(names, ["LOW", "MEDIUM", "HIGH", "EXTREME"]);
    }
}
