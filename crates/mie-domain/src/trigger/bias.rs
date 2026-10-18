//! The bias context `bias.context@1` (ADR-053 D3): what the broader
//! evidence shows, recorded next to every trigger and never read by a
//! trigger rule.
//!
//! Two components, each with its input's validity:
//!
//! - `regime`: a copy of `volatility.regime.1h@1`, the label and the raw
//!   percentile (brief §11's "HIGH volatility");
//! - per structure timeframe, a [`SwingSequence`]: for each side the step
//!   of the last swing pair of `candidate.divergence.cvd.<tf>@1`, by the
//!   sign of its price change, with the later swing's bar time.
//!
//! There is no aggregate score or direction (ADR-013: its weights would be
//! unmeasured), no order flow (ADR-023: aggression is not direction) and no
//! trend/range label (brief §9: deferred until validated). The location
//! half of brief §11 is each trigger's own `LocationContext`.

use crate::bars::Timeframe;
use crate::candidate::Candidates;
use crate::candidate::divergence::SwingPair;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::Fingerprinter;
use crate::regime::Regime;
use crate::state::MarketState;
use crate::state_hash::StateEncode;
use crate::structure::{STRUCTURE_TIMEFRAMES, Side};
use crate::time::EventTime;
use std::cmp::Ordering;
use std::fmt;

/// The step of a swing pair: where the later swing of one side lies
/// against the earlier one, by the sign of the price change.
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SwingStep {
    /// The later swing is strictly above the earlier one.
    Higher,
    /// Both swings are at the same price.
    Equal,
    /// The later swing is strictly below the earlier one.
    Lower,
}

impl SwingStep {
    /// Every step, in declaration order.
    pub const ALL: [Self; 3] = [Self::Higher, Self::Equal, Self::Lower];

    /// The step of `pair`: the sign of its price change.
    fn of(pair: &SwingPair) -> Self {
        match pair.price_change.units().cmp(&0) {
            Ordering::Greater => Self::Higher,
            Ordering::Equal => Self::Equal,
            Ordering::Less => Self::Lower,
        }
    }
}

impl StateEncode for SwingStep {
    /// `write_u8` in declaration order: Higher 0, Equal 1, Lower 2
    /// (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::Higher => 0,
            Self::Equal => 1,
            Self::Lower => 2,
        });
    }
}

impl fmt::Display for SwingStep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Higher => "higher",
            Self::Equal => "equal",
            Self::Lower => "lower",
        })
    }
}

/// The last swing pair of one side, as a step.
///
/// `Display` prints `<step>@<swing_time>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StepAt {
    /// The step.
    pub step: SwingStep,
    /// Open time of the later swing's bar.
    pub swing_time: EventTime,
}

impl StateEncode for StepAt {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { step, swing_time } = self;
        step.encode(f);
        swing_time.encode(f);
    }
}

impl fmt::Display for StepAt {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.step, self.swing_time)
    }
}

/// The swing sequence of one structure timeframe: the step of the last
/// pair of swing highs and of swing lows, each `None` until that side's
/// second anchored swing.
///
/// `Display` prints `high=<step> low=<step>`, `-` for none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwingSequence {
    /// The last pair of swing highs.
    pub high: Option<StepAt>,
    /// The last pair of swing lows.
    pub low: Option<StepAt>,
}

impl StateEncode for SwingSequence {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { high, low } = self;
        high.encode(f);
        low.encode(f);
    }
}

impl fmt::Display for SwingSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, (side, step)) in [(Side::High, &self.high), (Side::Low, &self.low)]
            .into_iter()
            .enumerate()
        {
            if index > 0 {
                f.write_str(" ")?;
            }
            match step {
                Some(step) => write!(f, "{side}={step}")?,
                None => write!(f, "{side}=-")?,
            }
        }
        Ok(())
    }
}

/// The bias context: `bias.context@1` (ADR-053 D3). Explicitly
/// non-authorizing: it has no aggregate, no direction and no method that
/// returns an action, and no trigger rule reads it.
///
/// `Display` prints `bias regime=<regime>` and `<tf>=[<sequence>]` per
/// structure timeframe (`warming` or `unavailable` for a component that is
/// not ready), then the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bias {
    /// The feature that produced the value: `bias.context@1`.
    pub feature: FeatureKey,
    /// A copy of `volatility.regime.1h@1`.
    pub regime: FeatureValue<Regime>,
    /// The swing sequence of every structure timeframe, shortest first,
    /// with the validity of its `candidate.divergence.cvd.<tf>@1`.
    swings: [FeatureValue<SwingSequence>; 4],
}

impl StateEncode for Bias {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            regime,
            swings,
        } = self;
        feature.encode(f);
        regime.encode(f);
        swings.encode(f);
    }
}

impl Bias {
    /// The bias of `state`: pure, so a consumer that samples states
    /// without a trigger (#25's unconditional baseline) reads the same
    /// definition the trigger events carry.
    pub fn read(state: &MarketState) -> Self {
        Self::from_parts(&state.regime, &state.candidates)
    }

    /// The bias of the regime `regime` and the candidates `candidates`.
    pub(crate) fn from_parts(regime: &FeatureValue<Regime>, candidates: &Candidates) -> Self {
        Self {
            feature: catalog::BIAS_CONTEXT_V1.key,
            regime: *regime,
            swings: STRUCTURE_TIMEFRAMES.map(|timeframe| {
                match candidates.divergences(timeframe) {
                    Some(divergences) => divergences.cvd.map(|divergence| SwingSequence {
                        high: divergence.high.map(|comparison| step_at(&comparison.pair)),
                        low: divergence.low.map(|comparison| step_at(&comparison.pair)),
                    }),
                    // Every structure timeframe has divergences.
                    None => FeatureValue::WarmingUp {
                        observed: 0,
                        required: 1,
                    },
                }
            }),
        }
    }

    /// The swing sequence of `timeframe`, if it is a structure timeframe.
    pub fn swings(&self, timeframe: Timeframe) -> Option<&FeatureValue<SwingSequence>> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.swings[index])
    }

    /// The swing sequence of every structure timeframe, shortest first.
    pub fn iter_swings(&self) -> impl Iterator<Item = (Timeframe, &FeatureValue<SwingSequence>)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.swings)
    }

    /// The swing sequence of `timeframe`, for tests that build a bias by
    /// hand.
    #[cfg(test)]
    pub(crate) fn swings_mut(
        &mut self,
        timeframe: Timeframe,
    ) -> Option<&mut FeatureValue<SwingSequence>> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &mut self.swings[index])
    }
}

/// The step of `pair` at its later swing.
fn step_at(pair: &SwingPair) -> StepAt {
    StepAt {
        step: SwingStep::of(pair),
        swing_time: pair.current.swing_time,
    }
}

impl fmt::Display for Bias {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bias regime=")?;
        match &self.regime {
            FeatureValue::Ready(regime) => write!(f, "[{regime}]")?,
            FeatureValue::WarmingUp { .. } => f.write_str("warming")?,
            FeatureValue::Unavailable { .. } => f.write_str("unavailable")?,
        }
        for (timeframe, swings) in self.iter_swings() {
            match swings {
                FeatureValue::Ready(swings) => write!(f, " {timeframe}=[{swings}]")?,
                FeatureValue::WarmingUp { .. } => write!(f, " {timeframe}=warming")?,
                FeatureValue::Unavailable { .. } => write!(f, " {timeframe}=unavailable")?,
            }
        }
        write!(f, " {}", self.feature)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Coverage;
    use crate::candidate::divergence::{CvdComparison, Divergence, SwingAnchor};
    use crate::feature::Unavailability;
    use crate::num::{Price, Qty};
    use crate::regime::AtrPercentile;

    fn anchor(price: i64, swing_time: i64) -> SwingAnchor {
        SwingAnchor {
            swing_time: EventTime::from_millis(swing_time),
            known_at: EventTime::from_millis(swing_time + 1),
            price: Price::from_units(price),
            time: EventTime::from_millis(swing_time + 2),
            cvd: Qty::from_units(0),
            gaps: 0,
            coverage: Coverage::default(),
        }
    }

    fn comparison(side: Side, from: i64, to: i64) -> CvdComparison {
        CvdComparison {
            pair: SwingPair {
                side,
                previous: anchor(from, 1_000),
                current: anchor(to, 2_000),
                price_change: Price::from_units(to - from),
                cvd_change: Qty::from_units(0),
                gaps_between: 0,
                span_ms: 1_000,
                coverage: Coverage::default(),
            },
            divergent: false,
        }
    }

    #[test]
    fn steps_follow_the_sign_of_the_price_change_one_unit_apart() {
        let mut candidates = Candidates::new();
        let divergences = candidates
            .divergences_mut(Timeframe::H1)
            .expect("1h is a structure timeframe");
        divergences.cvd = FeatureValue::Ready(Divergence {
            feature: catalog::CANDIDATE_DIVERGENCE_CVD_1H_V1.key,
            high: Some(comparison(Side::High, 100, 101)),
            low: Some(comparison(Side::Low, 100, 99)),
        });
        let bias = Bias::from_parts(
            &FeatureValue::WarmingUp {
                observed: 3,
                required: 214,
            },
            &candidates,
        );
        let sequence = bias.swings(Timeframe::H1).unwrap().ready().unwrap();
        assert_eq!(
            sequence.high,
            Some(StepAt {
                step: SwingStep::Higher,
                swing_time: EventTime::from_millis(2_000)
            })
        );
        assert_eq!(sequence.low.map(|step| step.step), Some(SwingStep::Lower));
        for (from, to, step) in [
            (100, 101, SwingStep::Higher),
            (100, 100, SwingStep::Equal),
            (100, 99, SwingStep::Lower),
        ] {
            assert_eq!(SwingStep::of(&comparison(Side::High, from, to).pair), step);
        }
        // The regime is copied with its validity, the other timeframes
        // warm up with their divergences.
        assert_eq!(
            bias.regime,
            FeatureValue::WarmingUp {
                observed: 3,
                required: 214
            }
        );
        assert_eq!(
            bias.swings(Timeframe::M15),
            Some(&FeatureValue::WarmingUp {
                observed: 0,
                required: 1
            })
        );
        assert_eq!(bias.swings(Timeframe::M5), None);
        assert_eq!(
            bias.to_string(),
            "bias regime=warming 15m=warming 1h=[high=higher@2000ms low=lower@2000ms] \
             4h=warming 1d=warming bias.context@1"
        );
    }

    #[test]
    fn a_ready_regime_and_an_unavailable_divergence_are_copied() {
        let mut candidates = Candidates::new();
        candidates.divergences_mut(Timeframe::D1).unwrap().cvd = FeatureValue::Unavailable {
            reason: Unavailability::OutOfRange,
        };
        let percentile = AtrPercentile::new(57.5).unwrap();
        let regime = FeatureValue::Ready(Regime {
            atr_percentile: percentile,
            label: percentile.label(),
            feature: catalog::VOLATILITY_REGIME_1H_V1.key,
        });
        let bias = Bias::from_parts(&regime, &candidates);
        assert_eq!(bias.regime, regime);
        assert_eq!(
            bias.swings(Timeframe::D1),
            Some(&FeatureValue::Unavailable {
                reason: Unavailability::OutOfRange
            })
        );
        assert!(
            bias.to_string()
                .starts_with("bias regime=[HIGH 57.5 volatility.regime.1h@1] 15m=warming"),
            "{bias}"
        );
        assert!(bias.to_string().contains(" 1d=unavailable "), "{bias}");
    }

    #[test]
    fn swing_step_codes_follow_the_declaration_order() {
        for (step, code) in SwingStep::ALL.into_iter().zip(0_u8..) {
            let mut expected = Fingerprinter::new();
            expected.write_u8(code);
            let mut f = Fingerprinter::new();
            step.encode(&mut f);
            assert_eq!(f.finish(), expected.finish(), "{step}");
        }
    }
}
