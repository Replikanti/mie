//! Price motion, ATR(14) and the ATR-percentile regime (brief §8–§9,
//! ADR-017, ADR-033, proposed).
//!
//! Everything here is computed from closed bars ([`bars`](crate::bars)),
//! one bar at a time, so live processing and replay agree (ADR-019).
//!
//! - **Motion** (`bars.motion.<tf>@1`, [`BarMotion`]): per timeframe, the
//!   change and range of each closed bar, exact, plus the simple return and
//!   the velocity (return per minute) derived from them on demand.
//! - **ATR(14)** (`volatility.atr.1h@1`): Pine Script v5 `ta.atr(14)` on 1h
//!   bars — the mean of the first 14 true ranges, then Wilder smoothing —
//!   held as a [`Price`], each step rounded half to even to 1e-8. The
//!   deviation from the exact value stays below 7 units of 1e-8 (ADR-033,
//!   decision 2).
//! - **Regime** (`volatility.regime.1h@1`, [`Regime`]): Pine Script v5
//!   `ta.percentrank(atr, 200)` — the share of the previous 200 ATR values at
//!   or below the current one, in half-steps from 0 to 100 — mapped onto the
//!   ADR-017 labels. Context, never an entry signal (ADR-012).
//! - **Series continuity** (ADR-033, decision 6), per closed bar. The anchor
//!   is the previous close; every bar with trades sets it afterwards.
//!
//!   | Closed bar | Anchor | ATR / regime | Motion |
//!   |---|---|---|---|
//!   | trades, complete | yes | sample, TR with the anchor | value |
//!   | trades, complete | no | sample, TR = high − low | anchor only |
//!   | trades, incomplete | yes | sample on the observed prices | value, coverage carried |
//!   | trades, incomplete | no | anchor only | anchor only |
//!   | empty, complete | yes | sample, TR = 0 | value: change 0, range 0 |
//!   | empty, complete | no | skipped | skipped |
//!   | empty, incomplete | any | break: back to warming up from 0 | break |

use crate::bars::{Bar, Coverage, Timeframe};
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::num::Price;
use crate::regime::{AtrPercentile, Regime};
use crate::time::EventTime;
use std::fmt;

/// ATR length: `volatility.atr.1h@1` parameter `length` (ADR-033).
pub const ATR_LENGTH: u16 = 14;

/// Percentile lookback: `volatility.regime.1h@1` parameter `lookback`
/// (ADR-033).
pub const REGIME_LOOKBACK: u16 = 200;

/// Samples before the first regime: the ATR warm-up plus a full lookback of
/// previous ATR values (ADR-033, decision 5).
pub const REGIME_WARM_UP: u16 = ATR_LENGTH + REGIME_LOOKBACK;

/// [`REGIME_LOOKBACK`] as an array length.
const LOOKBACK: usize = REGIME_LOOKBACK as usize;

/// What one closed bar does to a series (ADR-033, decision 6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Continuity {
    /// A sample against the previous close.
    Continued(Price),
    /// A complete bar with trades and no previous close: an ATR sample with
    /// `TR = high − low`; motion only anchors.
    Fresh,
    /// An incomplete bar with trades and no previous close: anchors only.
    AnchorOnly,
    /// An empty complete bar without a previous close: nothing changes.
    Skip,
    /// An empty incomplete bar: the series starts over.
    Break,
}

impl Continuity {
    /// Classifies `bar` given the series' `anchor`.
    fn of(bar: &Bar, anchor: Option<Price>) -> Self {
        match (bar.ohlc.is_some(), bar.is_complete(), anchor) {
            (false, false, _) => Self::Break,
            (_, _, Some(previous_close)) => Self::Continued(previous_close),
            (true, true, None) => Self::Fresh,
            (true, false, None) => Self::AnchorOnly,
            (false, true, None) => Self::Skip,
        }
    }

    /// The anchor after `bar`: cleared by a break, else the bar's close if
    /// it has trades, else unchanged.
    fn anchor_after(self, bar: &Bar, anchor: Option<Price>) -> Option<Price> {
        match (self, bar.ohlc) {
            (Self::Break, _) => None,
            (_, Some(ohlc)) => Some(ohlc.close),
            (_, None) => anchor,
        }
    }
}

/// A value in `1 / SCALE` units, or [`VolatilityError::Overflow`] outside
/// the `Price` range.
fn to_price(units: i128) -> Result<Price, VolatilityError> {
    i64::try_from(units)
        .map(Price::from_units)
        .map_err(|_| VolatilityError::Overflow)
}

/// The true range of `bar`: `high − low` without a previous close, else
/// `max(high − low, |high − previous close|, |low − previous close|)`; 0 for
/// an empty bar (ADR-033, decision 2).
fn true_range(bar: &Bar, previous_close: Option<Price>) -> Result<Price, VolatilityError> {
    let Some(ohlc) = bar.ohlc else {
        return Ok(Price::from_units(0));
    };
    let high = i128::from(ohlc.high.units());
    let low = i128::from(ohlc.low.units());
    let mut range = high - low;
    if let Some(close) = previous_close {
        let close = i128::from(close.units());
        range = range.max((high - close).abs()).max((low - close).abs());
    }
    to_price(range)
}

/// `numerator / denominator` rounded half to even; `denominator > 0`.
fn div_half_even(numerator: i128, denominator: i128) -> i128 {
    let quotient = numerator.div_euclid(denominator);
    let twice_remainder = 2 * numerator.rem_euclid(denominator);
    let round_up = twice_remainder > denominator
        || (twice_remainder == denominator && quotient.rem_euclid(2) == 1);
    quotient + i128::from(round_up)
}

/// How many of `previous` are at or below `current` (ADR-033, decision 3).
fn at_or_below(previous: &[Price], current: Price) -> usize {
    previous.iter().filter(|&&value| value <= current).count()
}

/// The motion of one closed bar against the previous close
/// (`bars.motion.<tf>@1`, ADR-033, decision 7).
///
/// The exact fields are stored; the floats are derived on demand in a fixed
/// operation order, so they are the same everywhere. `Display` prints the
/// canonical line the golden tests pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarMotion {
    /// The timeframe.
    pub timeframe: Timeframe,
    /// Open time of the bar.
    pub open_time: EventTime,
    /// The close the bar moved from.
    pub previous_close: Price,
    /// The bar's close; the previous close for an empty bar.
    pub close: Price,
    /// `close − previous_close`.
    pub change: Price,
    /// `high − low`; 0 for an empty bar.
    pub range: Price,
    /// The bar's coverage: an incomplete bar's motion is a sample on what
    /// was observed.
    pub coverage: Coverage,
}

impl BarMotion {
    /// The motion of `bar` from `previous_close`.
    fn of(bar: &Bar, previous_close: Price) -> Result<Self, VolatilityError> {
        let previous = i128::from(previous_close.units());
        let (close, range) = match bar.ohlc {
            Some(ohlc) => (
                ohlc.close,
                to_price(i128::from(ohlc.high.units()) - i128::from(ohlc.low.units()))?,
            ),
            None => (previous_close, Price::from_units(0)),
        };
        Ok(Self {
            timeframe: bar.timeframe,
            open_time: bar.open_time,
            previous_close,
            close,
            change: to_price(i128::from(close.units()) - previous)?,
            range,
            coverage: bar.coverage,
        })
    }

    /// The simple return `change / previous_close`, or `None` unless the
    /// previous close is positive.
    pub fn simple_return(&self) -> Option<f64> {
        (self.previous_close.units() > 0)
            .then(|| self.change.units() as f64 / self.previous_close.units() as f64)
    }

    /// The velocity: the simple return per minute of the bar's timeframe,
    /// comparable across timeframes. `None` when the return is.
    pub fn velocity_per_minute(&self) -> Option<f64> {
        // Whole minutes: exact in f64.
        let minutes = (self.timeframe.millis() / 60_000) as f64;
        self.simple_return().map(|simple| simple / minutes)
    }
}

impl fmt::Display for BarMotion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} prev={} close={} change={} range={} return=",
            self.timeframe,
            self.open_time,
            self.previous_close,
            self.close,
            self.change,
            self.range
        )?;
        match self.simple_return() {
            Some(simple) => write!(f, "{simple}")?,
            None => f.write_str("-")?,
        }
        f.write_str(" velocity=")?;
        match self.velocity_per_minute() {
            Some(velocity) => write!(f, "{velocity}")?,
            None => f.write_str("-")?,
        }
        write!(f, " {}", self.coverage)
    }
}

/// The validity of a motion that has not appeared yet.
const MOTION_NOT_YET: FeatureValue<BarMotion> = FeatureValue::WarmingUp {
    observed: 0,
    required: 1,
};

/// The motion of every timeframe, in [`Timeframe::ALL`] order: the
/// `bars.motion.*@1` features of the Market State. Each value is the motion
/// of the last closed bar of its timeframe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MotionSet {
    values: [FeatureValue<BarMotion>; 6],
}

impl Default for MotionSet {
    fn default() -> Self {
        Self::new()
    }
}

impl MotionSet {
    /// Every timeframe warming up, for [`catalog::BARS_MOTION`].
    pub fn new() -> Self {
        Self {
            values: [MOTION_NOT_YET; 6],
        }
    }

    /// The motion of `timeframe`, if the set computes it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&FeatureValue<BarMotion>> {
        motion_index(timeframe).map(|index| &self.values[index])
    }

    /// Every timeframe, shortest first, with its feature
    /// (`bars.motion.<label>@1`) and value.
    pub fn iter(&self) -> impl Iterator<Item = (Timeframe, FeatureKey, &FeatureValue<BarMotion>)> {
        catalog::BARS_MOTION
            .iter()
            .zip(&self.values)
            .map(|((timeframe, definition), value)| (*timeframe, definition.key, value))
    }
}

/// The position of `timeframe` in [`catalog::BARS_MOTION`].
fn motion_index(timeframe: Timeframe) -> Option<usize> {
    catalog::BARS_MOTION
        .iter()
        .position(|(candidate, _)| *candidate == timeframe)
}

/// The previous close of every motion series: engine state, not part of the
/// Market State.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) struct MotionAnchors {
    anchors: [Option<Price>; 6],
}

impl MotionAnchors {
    /// Steps the motion series of `bar`'s timeframe. On error neither `self`
    /// nor `motion` changed.
    pub(crate) fn apply(
        &mut self,
        bar: &Bar,
        motion: &mut MotionSet,
    ) -> Result<(), VolatilityError> {
        let Some(index) = motion_index(bar.timeframe) else {
            return Ok(());
        };
        let anchor = self.anchors[index];
        let continuity = Continuity::of(bar, anchor);
        let value = match continuity {
            Continuity::Continued(previous_close) => {
                Some(FeatureValue::Ready(BarMotion::of(bar, previous_close)?))
            }
            Continuity::Break => Some(MOTION_NOT_YET),
            Continuity::Fresh | Continuity::AnchorOnly | Continuity::Skip => None,
        };
        if let Some(value) = value {
            motion.values[index] = value;
        }
        self.anchors[index] = continuity.anchor_after(bar, anchor);
        Ok(())
    }
}

/// ATR(14) and the ATR-percentile regime of one bar series:
/// `volatility.atr.1h@1` and `volatility.regime.1h@1` (ADR-033).
///
/// A pure stepper over closed [`REGIME_TIMEFRAME`](catalog::REGIME_TIMEFRAME)
/// bars; bars of other timeframes are ignored. The previous ATR values live
/// in a fixed ring, so a step never allocates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AtrRegimeSeries {
    /// The previous close.
    anchor: Option<Price>,
    /// Samples since the series (re)started.
    samples: u64,
    /// Sum of the true ranges before the first ATR.
    seed_sum: i128,
    /// The current ATR, after the seed.
    current: Option<Price>,
    /// The latest ATR values, oldest overwritten first.
    window: [Price; LOOKBACK],
    /// How many of `window` hold a value.
    filled: usize,
    /// Where the next ATR value goes.
    next: usize,
    atr: FeatureValue<Price>,
    regime: FeatureValue<Regime>,
}

impl Default for AtrRegimeSeries {
    fn default() -> Self {
        Self::new()
    }
}

impl AtrRegimeSeries {
    /// An empty series, warming up.
    pub fn new() -> Self {
        Self {
            anchor: None,
            samples: 0,
            seed_sum: 0,
            current: None,
            window: [Price::from_units(0); LOOKBACK],
            filled: 0,
            next: 0,
            atr: FeatureValue::WarmingUp {
                observed: 0,
                required: u64::from(ATR_LENGTH),
            },
            regime: FeatureValue::WarmingUp {
                observed: 0,
                required: u64::from(REGIME_WARM_UP),
            },
        }
    }

    /// ATR(14) after the last sample: `volatility.atr.1h@1`.
    pub fn atr(&self) -> FeatureValue<Price> {
        self.atr
    }

    /// The regime after the last sample: `volatility.regime.1h@1`.
    pub fn regime(&self) -> FeatureValue<Regime> {
        self.regime
    }

    /// Consumes the next closed bar of the series (ADR-033, decisions 2, 3
    /// and 6).
    ///
    /// # Errors
    ///
    /// [`VolatilityError::Overflow`] if the bar's true range leaves the
    /// `Price` range; the series is then unchanged.
    pub fn push(&mut self, bar: &Bar) -> Result<(), VolatilityError> {
        if bar.timeframe != catalog::REGIME_TIMEFRAME {
            return Ok(());
        }
        let continuity = Continuity::of(bar, self.anchor);
        let previous_close = match continuity {
            Continuity::Break => {
                *self = Self::new();
                return Ok(());
            }
            Continuity::AnchorOnly | Continuity::Skip => {
                self.anchor = continuity.anchor_after(bar, self.anchor);
                return Ok(());
            }
            Continuity::Fresh => None,
            Continuity::Continued(previous_close) => Some(previous_close),
        };
        let range = i128::from(true_range(bar, previous_close)?.units());
        let length = i128::from(ATR_LENGTH);
        let samples = self.samples + 1;
        let (seed_sum, current) = match self.current {
            Some(previous) => {
                let smoothed = (length - 1) * i128::from(previous.units()) + range;
                (
                    self.seed_sum,
                    Some(to_price(div_half_even(smoothed, length))?),
                )
            }
            None => {
                let sum = self.seed_sum + range;
                let seeded = samples == u64::from(ATR_LENGTH);
                let current = if seeded {
                    Some(to_price(div_half_even(sum, length))?)
                } else {
                    None
                };
                (sum, current)
            }
        };

        // Nothing below can fail.
        self.anchor = continuity.anchor_after(bar, self.anchor);
        self.samples = samples;
        self.seed_sum = seed_sum;
        self.current = current;
        let Some(atr) = current else {
            self.atr = FeatureValue::WarmingUp {
                observed: samples,
                required: u64::from(ATR_LENGTH),
            };
            self.regime = FeatureValue::WarmingUp {
                observed: samples,
                required: u64::from(REGIME_WARM_UP),
            };
            return Ok(());
        };
        self.atr = FeatureValue::Ready(atr);
        self.regime = if self.filled == LOOKBACK {
            let rank = u16::try_from(at_or_below(&self.window, atr)).ok();
            match rank.and_then(|rank| AtrPercentile::from_rank(rank, REGIME_LOOKBACK).ok()) {
                Some(percentile) => FeatureValue::Ready(Regime::new(
                    percentile,
                    catalog::VOLATILITY_REGIME_1H_V1.key,
                )),
                // Unreachable: at most the whole window is at or below.
                None => FeatureValue::Unavailable {
                    reason: Unavailability::OutOfRange,
                },
            }
        } else {
            FeatureValue::WarmingUp {
                observed: samples,
                required: u64::from(REGIME_WARM_UP),
            }
        };
        self.window[self.next] = atr;
        self.next = (self.next + 1) % LOOKBACK;
        self.filled = (self.filled + 1).min(LOOKBACK);
        Ok(())
    }
}

/// Why a volatility feature could not take a bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VolatilityError {
    /// A true range, change or range left the `Price` range (ADR-027).
    Overflow,
}

impl fmt::Display for VolatilityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("a volatility value leaves the price range"),
        }
    }
}

impl std::error::Error for VolatilityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Ohlc;
    use crate::bars::tests::golden_tape;
    use crate::event::samples::{gap, t};
    use crate::event::{Aggressor, GapReason, MarketEvent, Stream, Trade};
    use crate::feature::{FeatureDefinition, Param, ParamValue, WarmUp};
    use crate::num::{Qty, SCALE};
    use crate::state::MarketStateEngine;

    const HOUR: i64 = 3_600_000;
    const COMPLETE: Coverage = Coverage {
        partial_start: false,
        feed_gap: false,
    };
    const PARTIAL: Coverage = Coverage {
        partial_start: true,
        feed_gap: false,
    };
    const GAP: Coverage = Coverage {
        partial_start: false,
        feed_gap: true,
    };

    fn p(units: i64) -> Price {
        Price::from_units(units)
    }

    /// A closed bar; `ohlc` in price units, `None` for an empty bar.
    fn bar(timeframe: Timeframe, open: i64, ohlc: Option<[i64; 4]>, coverage: Coverage) -> Bar {
        Bar {
            ohlc: ohlc.map(|[open, high, low, close]| Ohlc {
                open: p(open),
                high: p(high),
                low: p(low),
                close: p(close),
            }),
            trade_count: u64::from(ohlc.is_some()),
            coverage,
            ..Bar::empty(timeframe, t(open))
        }
    }

    /// A complete 1h bar number `index`.
    fn hour(index: i64, ohlc: [i64; 4]) -> Bar {
        bar(Timeframe::H1, index * HOUR, Some(ohlc), COMPLETE)
    }

    /// A complete 1h bar whose true range is `range` after another such bar
    /// (its close is its low, 1 000).
    fn ranged(index: i64, range: i64) -> Bar {
        hour(index, [1_000, 1_000 + range, 1_000, 1_000])
    }

    fn series_of(ranges: &[i64]) -> AtrRegimeSeries {
        let mut series = AtrRegimeSeries::new();
        for (index, range) in (0..).zip(ranges) {
            series.push(&ranged(index, *range)).unwrap();
        }
        series
    }

    fn warming<T>(observed: u64, required: u64) -> FeatureValue<T> {
        FeatureValue::WarmingUp { observed, required }
    }

    fn show<T: fmt::Display>(value: &FeatureValue<T>) -> String {
        match value {
            FeatureValue::Ready(value) => value.to_string(),
            FeatureValue::WarmingUp { observed, required } => {
                format!("warming {observed}/{required}")
            }
            FeatureValue::Unavailable { reason } => format!("unavailable {reason:?}"),
        }
    }

    #[test]
    fn true_range_follows_pine() {
        assert_eq!(true_range(&hour(0, [100, 110, 95, 105]), None), Ok(p(15)));
        // Gap up and gap down: the distance to the previous close wins.
        let up = hour(1, [112, 120, 110, 115]);
        assert_eq!(true_range(&up, Some(p(100))), Ok(p(20)));
        let down = hour(1, [88, 90, 80, 85]);
        assert_eq!(true_range(&down, Some(p(100))), Ok(p(20)));
        let inside = hour(1, [100, 104, 96, 100]);
        assert_eq!(true_range(&inside, Some(p(100))), Ok(p(8)));
        let empty = bar(Timeframe::H1, 0, None, COMPLETE);
        assert_eq!(true_range(&empty, Some(p(100))), Ok(p(0)));
        let extreme = hour(1, [i64::MAX; 4]);
        assert_eq!(
            true_range(&extreme, Some(p(i64::MIN))),
            Err(VolatilityError::Overflow)
        );
    }

    #[test]
    fn division_rounds_half_to_even() {
        for (numerator, denominator, expected) in [
            (3, 2, 2),
            (5, 2, 2),
            (7, 2, 4),
            (-3, 2, -2),
            (-5, 2, -2),
            (1, 3, 0),
            (2, 3, 1),
            (21, 14, 2),
            (35, 14, 2),
            (28, 14, 2),
        ] {
            assert_eq!(
                div_half_even(numerator, denominator),
                expected,
                "{numerator}/{denominator}"
            );
        }
    }

    #[test]
    fn the_seed_is_the_mean_of_the_first_14_true_ranges() {
        let mut ranges = [1; 14];
        let series = series_of(&ranges[..13]);
        assert_eq!(series.atr(), warming(13, 14));
        // 21 / 14 = 1.5 and 35 / 14 = 2.5: both ties round to 2.
        ranges[13] = 8;
        assert_eq!(series_of(&ranges).atr(), FeatureValue::Ready(p(2)));
        ranges[13] = 22;
        assert_eq!(series_of(&ranges).atr(), FeatureValue::Ready(p(2)));
        ranges[13] = 9;
        assert_eq!(series_of(&ranges).atr(), FeatureValue::Ready(p(2)));
        ranges[13] = 1;
        assert_eq!(series_of(&ranges).atr(), FeatureValue::Ready(p(1)));
    }

    #[test]
    fn wilder_smoothing_steps_from_the_seed() {
        let mut series = series_of(&[14; 14]);
        assert_eq!(series.atr(), FeatureValue::Ready(p(14)));
        // (13 · 14 + 28) / 14 = 15.
        series.push(&ranged(14, 28)).unwrap();
        assert_eq!(series.atr(), FeatureValue::Ready(p(15)));
        // (13 · 15 + 8) / 14 = 14.5, a tie: down to the even 14.
        series.push(&ranged(15, 8)).unwrap();
        assert_eq!(series.atr(), FeatureValue::Ready(p(14)));
        // (13 · 14 + 22) / 14 = 14.57…: 15.
        series.push(&ranged(16, 22)).unwrap();
        assert_eq!(series.atr(), FeatureValue::Ready(p(15)));
    }

    #[test]
    fn rank_counts_previous_values_at_or_below() {
        let rising: Vec<Price> = (1..=200).map(p).collect();
        let percentile = |window: &[Price], current| {
            let rank = u16::try_from(at_or_below(window, current)).unwrap();
            AtrPercentile::from_rank(rank, REGIME_LOOKBACK).unwrap()
        };
        assert_eq!(percentile(&rising, p(0)).value(), 0.0);
        assert_eq!(percentile(&rising, p(200)).value(), 100.0);
        assert_eq!(percentile(&rising, p(115)).value(), 57.5);
        // Ties count as at or below: a constant window ranks 100.
        assert_eq!(percentile(&[p(5); 200], p(5)).value(), 100.0);
        let mixed: Vec<Price> = [p(5); 100].into_iter().chain([p(10); 100]).collect();
        assert_eq!(percentile(&mixed, p(5)).value(), 50.0);
        assert_eq!(percentile(&mixed, p(7)).value(), 50.0);
        assert_eq!(percentile(&mixed, p(4)).value(), 0.0);
        assert_eq!(percentile(&mixed, p(10)).value(), 100.0);
    }

    #[test]
    fn the_regime_is_ready_at_sample_214() {
        let mut series = series_of(&[50; 213]);
        assert_eq!(series.atr(), FeatureValue::Ready(p(50)));
        assert_eq!(series.regime(), warming(213, 214));
        series.push(&ranged(213, 50)).unwrap();
        // A constant ATR window ranks 100 (ADR-033, decision 3).
        assert_eq!(show(&series.regime()), "EXTREME 100 volatility.regime.1h@1");
        // A calmer bar drops the ATR below every previous value.
        series.push(&ranged(214, 0)).unwrap();
        assert_eq!(series.atr(), FeatureValue::Ready(p(46)));
        assert_eq!(show(&series.regime()), "LOW 0 volatility.regime.1h@1");
        let FeatureValue::Ready(regime) = series.regime() else {
            unreachable!("ready above")
        };
        assert_eq!(regime.feature, catalog::VOLATILITY_REGIME_1H_V1.key);
    }

    #[test]
    fn the_atr_series_follows_the_continuity_table() {
        let mut series = AtrRegimeSeries::new();
        // Trades, incomplete, no anchor: anchor only.
        series
            .push(&bar(Timeframe::H1, 0, Some([100, 110, 90, 105]), PARTIAL))
            .unwrap();
        assert_eq!((series.samples, series.anchor), (0, Some(p(105))));
        assert_eq!(series.atr(), warming(0, 14));
        // Trades, complete, anchor: TR against the previous close.
        series.push(&hour(1, [120, 130, 120, 125])).unwrap();
        assert_eq!((series.samples, series.seed_sum), (1, 25));
        assert_eq!(series.atr(), warming(1, 14));
        // Trades, incomplete, anchor: a sample on the observed prices.
        series
            .push(&bar(
                Timeframe::H1,
                2 * HOUR,
                Some([125, 127, 124, 126]),
                GAP,
            ))
            .unwrap();
        assert_eq!((series.samples, series.seed_sum), (2, 28));
        // Empty, complete, anchor: TR 0; the anchor stays.
        series
            .push(&bar(Timeframe::H1, 3 * HOUR, None, COMPLETE))
            .unwrap();
        assert_eq!((series.samples, series.seed_sum), (3, 28));
        assert_eq!(series.anchor, Some(p(126)));
        // Empty, incomplete: break.
        series
            .push(&bar(Timeframe::H1, 4 * HOUR, None, GAP))
            .unwrap();
        assert_eq!(series, AtrRegimeSeries::new());
        // Empty, complete, no anchor: skipped.
        series
            .push(&bar(Timeframe::H1, 5 * HOUR, None, COMPLETE))
            .unwrap();
        assert_eq!(series, AtrRegimeSeries::new());
        // Trades, complete, no anchor: a sample with TR = high − low.
        series.push(&hour(6, [100, 110, 95, 105])).unwrap();
        assert_eq!((series.samples, series.seed_sum), (1, 15));
        // After a break, a bar with trades and no anchor only anchors, and
        // the next bar's TR uses its close.
        series
            .push(&bar(Timeframe::H1, 7 * HOUR, None, PARTIAL))
            .unwrap();
        series
            .push(&bar(
                Timeframe::H1,
                8 * HOUR,
                Some([199, 201, 198, 200]),
                GAP,
            ))
            .unwrap();
        assert_eq!((series.samples, series.anchor), (0, Some(p(200))));
        series.push(&hour(9, [210, 220, 210, 215])).unwrap();
        assert_eq!((series.samples, series.seed_sum), (1, 20));
    }

    #[test]
    fn a_break_restarts_the_warm_up() {
        let mut series = series_of(&[50; 220]);
        assert!(series.regime().is_ready());
        series
            .push(&bar(Timeframe::H1, 220 * HOUR, None, GAP))
            .unwrap();
        assert_eq!(series.atr(), warming(0, 14));
        assert_eq!(series.regime(), warming(0, 214));
        // A complete bar with trades restarts it at once.
        series.push(&ranged(221, 50)).unwrap();
        assert_eq!(series.regime(), warming(1, 214));
    }

    #[test]
    fn the_atr_series_ignores_other_timeframes_and_survives_overflow() {
        let mut series = series_of(&[50; 3]);
        let before = series.clone();
        series
            .push(&bar(Timeframe::M5, 0, Some([1, 2, 0, 1]), COMPLETE))
            .unwrap();
        assert_eq!(series, before);

        let mut series = AtrRegimeSeries::new();
        series
            .push(&bar(Timeframe::H1, 0, Some([i64::MIN; 4]), COMPLETE))
            .unwrap();
        let before = series.clone();
        assert_eq!(
            series.push(&hour(1, [i64::MAX; 4])),
            Err(VolatilityError::Overflow)
        );
        assert_eq!(series, before);
    }

    #[test]
    fn motion_measures_change_range_return_and_velocity() {
        let usdt = |whole: i64| whole * SCALE;
        let five = bar(
            Timeframe::M5,
            300_000,
            Some([usdt(101), usdt(110), usdt(95), usdt(105)]),
            COMPLETE,
        );
        let motion = BarMotion::of(&five, p(usdt(100))).unwrap();
        assert_eq!(motion.change, p(usdt(5)));
        assert_eq!(motion.range, p(usdt(15)));
        assert_eq!(motion.close, p(usdt(105)));
        assert_eq!(motion.simple_return(), Some(0.05));
        assert_eq!(motion.velocity_per_minute(), Some(0.05 / 5.0));
        assert_eq!(
            motion.to_string(),
            "5m 300000ms prev=100.00000000 close=105.00000000 change=5.00000000 \
             range=15.00000000 return=0.05 velocity=0.01 complete"
        );
        // An empty bar moves nothing.
        let empty = bar(Timeframe::M5, 600_000, None, COMPLETE);
        let motion = BarMotion::of(&empty, p(usdt(100))).unwrap();
        assert_eq!(
            (motion.close, motion.change, motion.range),
            (p(usdt(100)), p(0), p(0))
        );
        assert_eq!(motion.simple_return(), Some(0.0));
        // The coverage is carried.
        let gapped = bar(Timeframe::M5, 900_000, Some([1, 3, 1, 2]), GAP);
        assert_eq!(BarMotion::of(&gapped, p(1)).unwrap().coverage, GAP);
        // No return from a non-positive previous close.
        for previous in [0, -1] {
            let motion = BarMotion::of(&gapped, p(previous)).unwrap();
            assert_eq!(motion.simple_return(), None);
            assert_eq!(motion.velocity_per_minute(), None);
            assert!(motion.to_string().contains("return=- velocity=-"));
        }
        let extreme = bar(Timeframe::M5, 0, Some([i64::MAX; 4]), COMPLETE);
        assert_eq!(
            BarMotion::of(&extreme, p(i64::MIN)),
            Err(VolatilityError::Overflow)
        );
        let wide = bar(Timeframe::M5, 0, Some([0, i64::MAX, i64::MIN, 0]), COMPLETE);
        assert_eq!(BarMotion::of(&wide, p(0)), Err(VolatilityError::Overflow));
    }

    #[test]
    fn motion_follows_the_continuity_table() {
        let mut anchors = MotionAnchors::default();
        let mut motion = MotionSet::new();
        let mut step = |bar: Bar| {
            anchors.apply(&bar, &mut motion).unwrap();
            (*motion.get(Timeframe::M5).unwrap(), anchors.anchors[1])
        };
        let m5 = |index: i64, ohlc, coverage| bar(Timeframe::M5, index * 300_000, ohlc, coverage);
        // Trades, incomplete, no anchor: anchor only.
        assert_eq!(
            step(m5(0, Some([100, 110, 90, 105]), PARTIAL)),
            (warming(0, 1), Some(p(105)))
        );
        // Trades, complete, anchor: a value.
        let (value, _) = step(m5(1, Some([106, 112, 104, 110]), COMPLETE));
        let ready = *value.ready().unwrap();
        assert_eq!((ready.change, ready.range), (p(5), p(8)));
        // Empty, complete, anchor: change and range 0.
        let (value, anchor) = step(m5(2, None, COMPLETE));
        let ready = *value.ready().unwrap();
        assert_eq!(
            (ready.change, ready.range, ready.close),
            (p(0), p(0), p(110))
        );
        assert_eq!(anchor, Some(p(110)));
        // Trades, incomplete, anchor: a value with its coverage.
        let (value, _) = step(m5(3, Some([111, 111, 108, 109]), GAP));
        let ready = *value.ready().unwrap();
        assert_eq!(
            (ready.change, ready.range, ready.coverage),
            (p(-1), p(3), GAP)
        );
        // Empty, incomplete: break.
        assert_eq!(step(m5(4, None, GAP)), (warming(0, 1), None));
        // Empty, complete, no anchor: skipped.
        assert_eq!(step(m5(5, None, COMPLETE)), (warming(0, 1), None));
        // Trades, complete, no anchor: anchor only.
        assert_eq!(
            step(m5(6, Some([120, 121, 119, 120]), COMPLETE)),
            (warming(0, 1), Some(p(120)))
        );
        let (value, _) = step(m5(7, Some([120, 125, 120, 124]), COMPLETE));
        assert_eq!(value.ready().unwrap().change, p(4));
        // Other timeframes are untouched.
        assert_eq!(motion.get(Timeframe::M1), Some(&warming(0, 1)));
        let timeframes: Vec<_> = motion.iter().map(|(timeframe, _, _)| timeframe).collect();
        assert_eq!(timeframes, Timeframe::ALL);
        for (timeframe, feature, _) in motion.iter() {
            assert_eq!(feature.to_string(), format!("bars.motion.{timeframe}@1"));
        }
    }

    #[test]
    fn constants_match_the_catalog() {
        fn param(definition: &FeatureDefinition, name: &str) -> Option<ParamValue> {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        }
        let atr = &catalog::VOLATILITY_ATR_1H_V1;
        let regime = &catalog::VOLATILITY_REGIME_1H_V1;
        assert_eq!(
            param(atr, "length"),
            Some(ParamValue::Int(i64::from(ATR_LENGTH)))
        );
        assert_eq!(atr.warm_up, WarmUp::Samples(u32::from(ATR_LENGTH)));
        assert_eq!(
            param(regime, "lookback"),
            Some(ParamValue::Int(i64::from(REGIME_LOOKBACK)))
        );
        assert_eq!(regime.warm_up, WarmUp::Samples(u32::from(REGIME_WARM_UP)));
        assert_eq!(REGIME_WARM_UP, 214);
        for (_, definition) in catalog::BARS_MOTION {
            assert_eq!(definition.warm_up, WarmUp::Samples(1), "{}", definition.key);
        }
        assert_eq!(
            regime.params.first(),
            Some(&Param {
                name: "bands",
                value: ParamValue::Text("adr017_upper_closed"),
            })
        );
    }

    /// The motion after each closed `timeframe` bar of the bars golden tape,
    /// driven through the Market State engine.
    fn golden_motion_lines(timeframe: Timeframe) -> Vec<String> {
        let mut engine = MarketStateEngine::new();
        let mut anchors = MotionAnchors::default();
        let mut motion = MotionSet::new();
        let mut lines = Vec::new();
        for event in golden_tape(timeframe) {
            engine.apply(&event).unwrap();
            for bar in engine.closed_bars() {
                if bar.timeframe != timeframe {
                    continue;
                }
                anchors.apply(bar, &mut motion).unwrap();
                let value = motion.get(timeframe).unwrap();
                lines.push(match value {
                    FeatureValue::Ready(motion) => motion.to_string(),
                    other => format!("{timeframe} {} {}", bar.open_time, show(other)),
                });
            }
            assert_eq!(
                engine.state().motion.get(timeframe),
                motion.get(timeframe),
                "{event:?}"
            );
        }
        lines
    }

    #[test]
    fn golden_bars_motion_1m_v1() {
        assert_eq!(golden_motion_lines(Timeframe::M1), GOLDEN_MOTION_1M);
    }

    #[test]
    fn golden_bars_motion_5m_v1() {
        assert_eq!(golden_motion_lines(Timeframe::M5), GOLDEN_MOTION_5M);
    }

    #[test]
    fn golden_bars_motion_15m_v1() {
        assert_eq!(golden_motion_lines(Timeframe::M15), GOLDEN_MOTION_15M);
    }

    #[test]
    fn golden_bars_motion_1h_v1() {
        assert_eq!(golden_motion_lines(Timeframe::H1), GOLDEN_MOTION_1H);
    }

    #[test]
    fn golden_bars_motion_4h_v1() {
        assert_eq!(golden_motion_lines(Timeframe::H4), GOLDEN_MOTION_4H);
    }

    #[test]
    fn golden_bars_motion_1d_v1() {
        assert_eq!(golden_motion_lines(Timeframe::D1), GOLDEN_MOTION_1D);
    }

    /// Deterministic 64-bit LCG (Knuth's MMIX constants) for test tapes.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, bound: u64) -> i64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            i64::try_from((self.0 >> 11) % bound).unwrap()
        }
    }

    /// Hours on the regime tape; the last one stays open.
    const TAPE_HOURS: i64 = 262;
    /// The hour without trades inside a trades gap: an empty, incomplete
    /// bar that breaks the series.
    const BREAK_HOUR: i64 = 240;

    /// A synthetic BTCUSDT-like tape: four trades per hour on a random walk
    /// whose step size changes every 12 hours. Hour 0 is the partial start;
    /// a trades gap from 50 minutes into hour 239 to 2 minutes into hour 241
    /// leaves hour 240 empty.
    fn regime_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0016);
        let mut events = Vec::new();
        let mut price = 60_000 * SCALE;
        let mut step = SCALE;
        let mut trade_id = 0;
        for hour in 0..TAPE_HOURS {
            if hour % 12 == 0 {
                step = [20, 50, 120, 300][usize::try_from(lcg.below(4)).unwrap()] * SCALE;
            }
            if hour == BREAK_HOUR {
                continue;
            }
            for slot in 0..4 {
                let minute = 5 + 10 * slot + lcg.below(5);
                price += lcg.below(2 * step.unsigned_abs() + 1) - step;
                trade_id += 1;
                events.push(MarketEvent::Trade(Trade {
                    time: t(hour * HOUR + minute * 60_000),
                    trade_id,
                    price: p(price),
                    qty: Qty::from_units(SCALE / 100),
                    aggressor: if slot % 2 == 0 {
                        Aggressor::Buy
                    } else {
                        Aggressor::Sell
                    },
                }));
            }
            if hour == BREAK_HOUR - 1 {
                events.push(gap(
                    Stream::Trades,
                    hour * HOUR + 50 * 60_000,
                    (BREAK_HOUR + 1) * HOUR + 2 * 60_000,
                    GapReason::Disconnected,
                ));
            }
        }
        events
    }

    /// Per closed 1h bar of the regime tape: its hour, coverage, ATR and
    /// regime, checked against the engine's state after every event.
    fn regime_rows() -> Vec<(i64, Bar, String, String)> {
        let mut engine = MarketStateEngine::new();
        let mut series = AtrRegimeSeries::new();
        let mut rows = Vec::new();
        for event in regime_tape() {
            engine.apply(&event).unwrap();
            for bar in engine.closed_bars() {
                if bar.timeframe != Timeframe::H1 {
                    continue;
                }
                series.push(bar).unwrap();
                rows.push((
                    bar.open_time.as_millis() / HOUR,
                    *bar,
                    show(&series.atr()),
                    show(&series.regime()),
                ));
            }
            assert_eq!(engine.state().atr, series.atr(), "{event:?}");
            assert_eq!(engine.state().regime, series.regime(), "{event:?}");
        }
        let hours: Vec<_> = rows.iter().map(|(hour, ..)| *hour).collect();
        assert_eq!(hours, (0..TAPE_HOURS - 1).collect::<Vec<_>>());
        // The tape exercises what it claims to.
        let coverage = |hour: usize| (rows[hour].1.coverage, rows[hour].1.is_empty());
        assert_eq!(coverage(0), (PARTIAL, false));
        assert_eq!(coverage(239), (GAP, false));
        assert_eq!(coverage(240), (GAP, true));
        assert_eq!(coverage(241), (GAP, false));
        rows
    }

    #[test]
    fn golden_volatility_atr_1h_v1() {
        let rows = regime_rows();
        let lines: Vec<String> = [
            0, 1, 13, 14, 15, 100, 213, 214, 239, 240, 241, 242, 254, 255, 260,
        ]
        .iter()
        .map(|&hour| format!("{hour} {}", rows[hour].2))
        .collect();
        assert_eq!(lines, GOLDEN_ATR_1H);
    }

    #[test]
    fn golden_volatility_regime_1h_v1() {
        let rows = regime_rows();
        let lines: Vec<String> = [212, 213, 214]
            .into_iter()
            .chain(224..=239)
            .chain([240, 241, 242, 260])
            .map(|hour| format!("{hour} {}", rows[hour].3))
            .collect();
        assert_eq!(lines, GOLDEN_REGIME_1H);
    }

    const GOLDEN_MOTION_1M: [&str; 6] = [
        "1m 0ms warming 0/1",
        "1m 60000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.00004724483850139372 complete",
        "1m 120000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "1m 180000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.000015747535510692576 feed_gap",
        "1m 240000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.000015747783499472448 feed_gap",
        "1m 300000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.000003937007874015748 complete",
    ];
    const GOLDEN_MOTION_5M: [&str; 6] = [
        "5m 0ms warming 0/1",
        "5m 300000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.000009448967700278745 complete",
        "5m 600000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "5m 900000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.000003149507102138515 feed_gap",
        "5m 1200000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.0000031495566998944897 feed_gap",
        "5m 1500000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.0000007874015748031496 complete",
    ];
    const GOLDEN_MOTION_15M: [&str; 6] = [
        "15m 0ms warming 0/1",
        "15m 900000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.0000031496559000929146 complete",
        "15m 1800000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "15m 2700000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.0000010498357007128384 feed_gap",
        "15m 3600000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.0000010498522332981632 feed_gap",
        "15m 4500000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.0000002624671916010499 complete",
    ];
    const GOLDEN_MOTION_1H: [&str; 6] = [
        "1h 0ms warming 0/1",
        "1h 3600000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.0000007874139750232287 complete",
        "1h 7200000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "1h 10800000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.0000002624589251782096 feed_gap",
        "1h 14400000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.0000002624630583245408 feed_gap",
        "1h 18000000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.00000006561679790026247 complete",
    ];
    const GOLDEN_MOTION_4H: [&str; 6] = [
        "4h 0ms warming 0/1",
        "4h 14400000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.00000019685349375580717 complete",
        "4h 28800000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "4h 43200000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.0000000656147312945524 feed_gap",
        "4h 57600000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.0000000656157645811352 feed_gap",
        "4h 72000000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.000000016404199475065618 complete",
    ];
    const GOLDEN_MOTION_1D: [&str; 6] = [
        "1d 0ms warming 0/1",
        "1d 86400000ms prev=63499.00000000 close=63502.00000000 change=3.00000000 range=1.50000000 return=0.00004724483850139372 velocity=0.000000032808915625967865 complete",
        "1d 172800000ms prev=63502.00000000 close=63502.00000000 change=0.00000000 range=0.00000000 return=0 velocity=0 complete",
        "1d 259200000ms prev=63502.00000000 close=63501.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747535510692576 velocity=-0.000000010935788549092067 feed_gap",
        "1d 345600000ms prev=63501.00000000 close=63500.00000000 change=-1.00000000 range=0.00000000 return=-0.000015747783499472448 velocity=-0.000000010935960763522533 feed_gap",
        "1d 432000000ms prev=63500.00000000 close=63499.75000000 change=-0.25000000 range=0.50000000 return=-0.000003937007874015748 velocity=-0.0000000027340332458442695 complete",
    ];
    const GOLDEN_ATR_1H: [&str; 15] = [
        "0 warming 0/14",
        "1 warming 1/14",
        "13 warming 13/14",
        "14 136.82595836",
        "15 166.97318809",
        "100 191.64054986",
        "213 233.67570802",
        "214 234.72743818",
        "239 115.40527848",
        "240 warming 0/14",
        "241 warming 0/14",
        "242 warming 1/14",
        "254 warming 13/14",
        "255 58.14107340",
        "260 49.43245981",
    ];
    const GOLDEN_REGIME_1H: [&str; 23] = [
        "212 warming 212/214",
        "213 warming 213/214",
        "214 EXTREME 80.5 volatility.regime.1h@1",
        "224 HIGH 60 volatility.regime.1h@1",
        "225 HIGH 54.5 volatility.regime.1h@1",
        "226 HIGH 53 volatility.regime.1h@1",
        "227 HIGH 52 volatility.regime.1h@1",
        "228 MEDIUM 49.5 volatility.regime.1h@1",
        "229 MEDIUM 48.5 volatility.regime.1h@1",
        "230 MEDIUM 47 volatility.regime.1h@1",
        "231 MEDIUM 46 volatility.regime.1h@1",
        "232 MEDIUM 45.5 volatility.regime.1h@1",
        "233 MEDIUM 44 volatility.regime.1h@1",
        "234 MEDIUM 42 volatility.regime.1h@1",
        "235 MEDIUM 41 volatility.regime.1h@1",
        "236 MEDIUM 40.5 volatility.regime.1h@1",
        "237 MEDIUM 38.5 volatility.regime.1h@1",
        "238 MEDIUM 34 volatility.regime.1h@1",
        "239 MEDIUM 30.5 volatility.regime.1h@1",
        "240 warming 0/214",
        "241 warming 0/214",
        "242 warming 1/214",
        "260 warming 19/214",
    ];
}
