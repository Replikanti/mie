//! Effort vs result and exhaustion (ADR-051 D5, D6):
//! `candidate.effort_result.<5m|15m|1h>@1` and `candidate.exhaustion@1`.
//!
//! - **Evaluation.** Once per event that closes at least one 1m bar, every
//!   ready `flow.window.<tf>@1` with a displacement and a nonzero delta is
//!   evaluated: `effort = |delta|`, `progress = displacement × sign(delta)`.
//! - **Baseline.** The previous [`BASELINE_EVALUATIONS`] evaluations of the
//!   same window. Quantiles are nearest rank and exact: the q-quantile is the
//!   ⌈q·B⌉-th smallest value, so q = 0.5 is the lower median.
//! - **Condition.** `effort ≥ effort_high` (the [`EFFORT_QUANTILE`] quantile
//!   of effort) and `progress ≤ move_median` (the median of
//!   `|displacement|`). A candidate event is emitted on the rising edge.
//! - **Exhaustion.** At the 5m evaluation ending at `t`: the 1h evaluation
//!   ending at `t − 5 min` expanded (`effort ≥ effort_high`, `progress ≥
//!   move_high`), the 5m aggression in its direction is at most the 5m
//!   median, and the last close lies within [`EXTREME_TOLERANCE`] of the
//!   extreme of the closed minutes `(t − 65 min, t]`.
//!
//! A minute without an evaluation (`delta == 0`, no displacement, a window
//! or a baseline not ready, a missing expansion) leaves the value, and so
//! its condition, as it was: it neither emits nor re-arms.

use crate::bars::{Coverage, Timeframe};
use crate::event::Aggressor;
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::fingerprint::Fingerprinter;
use crate::flow::AggressionWindow;
use crate::num::{Price, Qty, Rate, SCALE};
use crate::state_hash::StateEncode;
use crate::time::EventTime;
use std::collections::VecDeque;
use std::fmt;

/// `B`: the evaluations a baseline holds (ADR-051 D5), fixed by R1b on the
/// hold-out year (D9).
pub const BASELINE_EVALUATIONS: usize = 1_440;

/// `q_hi`: the effort quantile of the condition (ADR-051 D5), fixed by R1
/// at `B` = 1 440 on the hold-out year (D9).
pub const EFFORT_QUANTILE: Rate = catalog::EFFORT_QUANTILE;

/// The median: q = 0.5, the parameter-free "typical window".
const MEDIAN: Rate = Rate::from_units(SCALE / 2);

/// The decay window of exhaustion: the 5m window, the finest (ADR-051 D6).
pub const DECAY_WINDOW: Timeframe = Timeframe::M5;

/// The expansion window of exhaustion: the 1h window, the longest.
pub const EXPANSION_WINDOW: Timeframe = Timeframe::H1;

/// The expansion is read at `t − 5 min`: the decay window's length, so the
/// expansion and the decay cover disjoint minutes.
const EXPANSION_OFFSET_MS: i64 = DECAY_WINDOW.millis();

/// The closed minutes `(t − 65 min, t]` the extreme is read over: the
/// expansion's hour and the decay's five minutes.
const EXTREME_SPAN_MS: i64 = EXPANSION_WINDOW.millis() + DECAY_WINDOW.millis();

/// Exhaustion's extreme tolerance: within 6 bps of the extreme (ADR-051 D6).
pub const EXTREME_TOLERANCE: Rate = catalog::EXHAUSTION_EXTREME_TOLERANCE;

/// Exhaustion's warm-up: six judged 1h evaluations, the expansion at
/// `t − 5 min` and the five after it.
pub const EXHAUSTION_SAMPLES: u64 = 6;

/// Judged 1h evaluations kept for the expansion lookup: those ending in
/// `[t − 5 min, t]`, at most one per minute.
const EXPANSIONS: usize = 6;

/// The taker side of a nonzero net aggression `delta`.
fn aggressor_of(delta: i64) -> Aggressor {
    if delta > 0 {
        Aggressor::Buy
    } else {
        Aggressor::Sell
    }
}

/// The label of `aggressor`, as the golden lines print it.
pub(crate) fn aggressor_label(aggressor: Aggressor) -> &'static str {
    match aggressor {
        Aggressor::Buy => "buy",
        Aggressor::Sell => "sell",
    }
}

/// The nearest rank of the `q` quantile of `n` values: ⌈q·n⌉, at least 1,
/// exact in `i128`.
fn rank(q: Rate, n: usize) -> usize {
    let n = i128::try_from(n).unwrap_or(i128::MAX);
    let product = i128::from(q.units()) * n;
    let scale = i128::from(SCALE);
    let rank = (product + scale - 1).div_euclid(scale).clamp(1, n.max(1));
    usize::try_from(rank).unwrap_or(1)
}

/// The `high` and `median` nearest-rank quantiles of `values` (not empty),
/// selected in `scratch`: one copy, one selection for the higher rank, and
/// one within the values below it for the lower.
fn quantiles(values: &VecDeque<i64>, high: Rate, scratch: &mut Vec<i64>) -> (i64, i64) {
    let (front, back) = values.as_slices();
    scratch.clear();
    scratch.extend_from_slice(front);
    scratch.extend_from_slice(back);
    let n = scratch.len();
    let (high_rank, median_rank) = (rank(high, n), rank(MEDIAN, n));
    let (outer, inner) = if high_rank >= median_rank {
        (high_rank, median_rank)
    } else {
        (median_rank, high_rank)
    };
    let (below, outer_value, _) = scratch.select_nth_unstable(outer - 1);
    let outer_value = *outer_value;
    // Everything before the outer rank is at most its value, so the inner
    // rank's value is the inner-th smallest of them (or the outer value at
    // an equal rank).
    let inner_value = if inner < outer {
        *below.select_nth_unstable(inner - 1).1
    } else {
        outer_value
    };
    if high_rank >= median_rank {
        (outer_value, inner_value)
    } else {
        (inner_value, outer_value)
    }
}

/// One judged effort-vs-result evaluation: `candidate.effort_result.<tf>@1`.
///
/// `Display` prints the window, its end, the aggressor, effort and
/// progress, the four baseline quantiles, the window's coverage, whether
/// the condition holds, and the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffortResult {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The window length.
    pub timeframe: Timeframe,
    /// Exclusive end of the window's last minute.
    pub end: EventTime,
    /// The taker side that carried the window's net aggression (ADR-023).
    pub aggressor: Aggressor,
    /// `|delta|`.
    pub effort: Qty,
    /// `displacement × sign(delta)`: price movement in the direction of the
    /// net aggression, at most 0 when price did not move or moved against
    /// it.
    pub progress: Price,
    /// The `q_hi` quantile of the baseline's effort.
    pub effort_high: Qty,
    /// The median of the baseline's effort.
    pub effort_median: Qty,
    /// The `q_hi` quantile of the baseline's `|displacement|`.
    pub move_high: Price,
    /// The median of the baseline's `|displacement|`.
    pub move_median: Price,
    /// The window's coverage.
    pub coverage: Coverage,
    /// `effort ≥ effort_high` and `progress ≤ move_median`.
    pub active: bool,
}

impl StateEncode for EffortResult {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            timeframe,
            end,
            aggressor,
            effort,
            progress,
            effort_high,
            effort_median,
            move_high,
            move_median,
            coverage,
            active,
        } = self;
        feature.encode(f);
        timeframe.encode(f);
        end.encode(f);
        aggressor.encode(f);
        effort.encode(f);
        progress.encode(f);
        effort_high.encode(f);
        effort_median.encode(f);
        move_high.encode(f);
        move_median.encode(f);
        coverage.encode(f);
        active.encode(f);
    }
}

impl fmt::Display for EffortResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} end={} aggressor={} effort={} progress={} effort_high={} effort_median={} \
             move_high={} move_median={} {} active={} {}",
            self.timeframe,
            self.end,
            aggressor_label(self.aggressor),
            self.effort,
            self.progress,
            self.effort_high,
            self.effort_median,
            self.move_high,
            self.move_median,
            self.coverage,
            self.active,
            self.feature
        )
    }
}

/// The effort-vs-result state of one window. Engine state, updated in
/// place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffortJudge {
    timeframe: Timeframe,
    feature: FeatureKey,
    /// `B`.
    capacity: usize,
    /// `q_hi`.
    quantile: Rate,
    /// The previous evaluations' effort, oldest first.
    effort: VecDeque<i64>,
    /// The previous evaluations' `|displacement|`, oldest first.
    movement: VecDeque<i64>,
    /// Whether the last judged evaluation met the condition.
    active: bool,
}

impl EffortJudge {
    /// The judge of the window at `position` in
    /// [`catalog::CANDIDATE_EFFORT_RESULT`] order.
    pub(crate) fn new(position: usize) -> Self {
        let (timeframe, definition) = catalog::CANDIDATE_EFFORT_RESULT[position];
        Self::with(
            timeframe,
            definition.key,
            BASELINE_EVALUATIONS,
            EFFORT_QUANTILE,
        )
    }

    /// A judge with its own baseline length and quantile.
    pub(crate) fn with(
        timeframe: Timeframe,
        feature: FeatureKey,
        capacity: usize,
        quantile: Rate,
    ) -> Self {
        Self {
            timeframe,
            feature,
            capacity,
            quantile,
            effort: VecDeque::with_capacity(capacity + 1),
            movement: VecDeque::with_capacity(capacity + 1),
            active: false,
        }
    }

    /// The value before the first evaluation.
    pub(crate) fn initial(&self) -> FeatureValue<EffortResult> {
        FeatureValue::WarmingUp {
            observed: 0,
            required: self.capacity as u64,
        }
    }

    /// Evaluates `window` (ADR-051 D5): judges it against the previous
    /// evaluations, updates `value`, then adds it to them. Returns the judged
    /// result and whether its condition just turned true; `None` without an
    /// evaluation, or while the baseline fills.
    pub(crate) fn evaluate(
        &mut self,
        window: &FeatureValue<AggressionWindow>,
        value: &mut FeatureValue<EffortResult>,
        scratch: &mut Vec<i64>,
    ) -> Option<(EffortResult, bool)> {
        let window = window.ready()?;
        let displacement = i128::from(window.displacement?.units());
        let delta = window.delta.units();
        if delta == 0 {
            return None;
        }
        let sign = i128::from(delta.signum());
        let fitted = (
            i64::try_from(i128::from(delta).abs()),
            i64::try_from(displacement * sign),
            i64::try_from(displacement.abs()),
        );
        let (Ok(effort), Ok(progress), Ok(movement)) = fitted else {
            // Unreachable on real data: no sample, and the condition stays.
            *value = FeatureValue::Unavailable {
                reason: Unavailability::OutOfRange,
            };
            return None;
        };
        let judged = if self.effort.len() == self.capacity {
            let (effort_high, effort_median) = quantiles(&self.effort, self.quantile, scratch);
            let (move_high, move_median) = quantiles(&self.movement, self.quantile, scratch);
            let active = effort >= effort_high && progress <= move_median;
            let edge = active && !self.active;
            self.active = active;
            let result = EffortResult {
                feature: self.feature,
                timeframe: self.timeframe,
                end: window.end,
                aggressor: aggressor_of(delta),
                effort: Qty::from_units(effort),
                progress: Price::from_units(progress),
                effort_high: Qty::from_units(effort_high),
                effort_median: Qty::from_units(effort_median),
                move_high: Price::from_units(move_high),
                move_median: Price::from_units(move_median),
                coverage: window.coverage,
                active,
            };
            *value = FeatureValue::Ready(result);
            Some((result, edge))
        } else {
            *value = FeatureValue::WarmingUp {
                observed: self.effort.len() as u64,
                required: self.capacity as u64,
            };
            None
        };
        self.effort.push_back(effort);
        self.movement.push_back(movement);
        if self.effort.len() > self.capacity {
            self.effort.pop_front();
            self.movement.pop_front();
        }
        judged
    }
}

/// One exhaustion evaluation: `candidate.exhaustion@1`.
///
/// `Display` prints the evaluation's end, the expansion and decay
/// evaluations it read, the decay's aggression in the expansion's
/// direction, the extreme and the close, the three parts of the condition,
/// whether all hold, and the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exhaustion {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// `t`: the end of the decay window.
    pub end: EventTime,
    /// The judged 1h evaluation ending at `t − 5 min`.
    pub expansion: EffortResult,
    /// The judged 5m evaluation ending at `t`.
    pub decay: EffortResult,
    /// `delta_5m × sign(delta_1h)`: the decay window's net aggression in the
    /// expansion's direction, negative when it reversed.
    pub decay_aggression: Qty,
    /// The highest high (after net buying) or lowest low (after net selling)
    /// of the closed 1m bars with trades of `(t − 65 min, t]`.
    pub extreme: Price,
    /// The close of the last closed 1m bar with trades.
    pub close: Price,
    /// The expansion's `effort ≥ effort_high` and `progress ≥ move_high`.
    pub expanded: bool,
    /// `decay_aggression ≤` the decay's `effort_median`.
    pub decayed: bool,
    /// `|extreme − close| · 10 000 ≤ |extreme| · 6`.
    pub at_extreme: bool,
    /// All three.
    pub active: bool,
}

impl StateEncode for Exhaustion {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            end,
            expansion,
            decay,
            decay_aggression,
            extreme,
            close,
            expanded,
            decayed,
            at_extreme,
            active,
        } = self;
        feature.encode(f);
        end.encode(f);
        expansion.encode(f);
        decay.encode(f);
        decay_aggression.encode(f);
        extreme.encode(f);
        close.encode(f);
        expanded.encode(f);
        decayed.encode(f);
        at_extreme.encode(f);
        active.encode(f);
    }
}

impl fmt::Display for Exhaustion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "end={} expansion=[{}] decay=[{}] decay_aggression={} extreme={} close={} \
             expanded={} decayed={} at_extreme={} active={} {}",
            self.end,
            self.expansion,
            self.decay,
            self.decay_aggression,
            self.extreme,
            self.close,
            self.expanded,
            self.decayed,
            self.at_extreme,
            self.active,
            self.feature
        )
    }
}

/// The exhaustion state. Engine state, updated in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExhaustionTracker {
    feature: FeatureKey,
    /// The closed 1m bars with trades of the last [`EXTREME_SPAN_MS`]:
    /// end, high, low; oldest first.
    minutes: VecDeque<(EventTime, Price, Price)>,
    /// The latest judged 1h evaluations, oldest first.
    expansions: VecDeque<EffortResult>,
    /// Judged 1h evaluations seen, up to [`EXHAUSTION_SAMPLES`].
    samples: u64,
    /// Whether the last evaluation met the condition.
    active: bool,
}

impl Default for ExhaustionTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl ExhaustionTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            feature: catalog::CANDIDATE_EXHAUSTION_V1.key,
            minutes: VecDeque::new(),
            expansions: VecDeque::with_capacity(EXPANSIONS + 1),
            samples: 0,
            active: false,
        }
    }

    /// The value before the first sample.
    pub(crate) fn initial() -> FeatureValue<Exhaustion> {
        FeatureValue::WarmingUp {
            observed: 0,
            required: EXHAUSTION_SAMPLES,
        }
    }

    /// Records a closed 1m bar with trades.
    pub(crate) fn minute(&mut self, end: EventTime, high: Price, low: Price) {
        self.minutes.push_back((end, high, low));
    }

    /// Exhaustion at the minute closed last (ADR-051 D6): `expansion` is the
    /// 1h evaluation judged at this minute, `decay` the 5m one, `close` the
    /// last close. Updates `value` and returns the evaluation and whether
    /// its condition just turned true; `None` without an evaluation, and
    /// before [`EXHAUSTION_SAMPLES`] judged 1h evaluations.
    pub(crate) fn step(
        &mut self,
        expansion: Option<&EffortResult>,
        decay: Option<&EffortResult>,
        close: Option<Price>,
        value: &mut FeatureValue<Exhaustion>,
    ) -> Option<(Exhaustion, bool)> {
        if let Some(expansion) = expansion {
            self.expansions.push_back(*expansion);
            if self.expansions.len() > EXPANSIONS {
                self.expansions.pop_front();
            }
            self.samples = (self.samples + 1).min(EXHAUSTION_SAMPLES);
            if !value.is_ready() {
                *value = FeatureValue::WarmingUp {
                    observed: self.samples,
                    required: EXHAUSTION_SAMPLES,
                };
            }
        }
        // Warm-up is all six samples, even when one event closes several
        // minutes and the expansion at `t − 5 min` is in the ring earlier.
        let evaluated = if self.samples == EXHAUSTION_SAMPLES {
            self.evaluate(decay, close)
        } else {
            None
        };
        if let Some(latest) = self.minutes.back().map(|(end, _, _)| end.as_millis()) {
            while self.minutes.front().is_some_and(|(end, _, _)| {
                i128::from(end.as_millis()) <= i128::from(latest) - i128::from(EXTREME_SPAN_MS)
            }) {
                self.minutes.pop_front();
            }
        }
        let (exhaustion, edge) = evaluated?;
        *value = FeatureValue::Ready(exhaustion);
        Some((exhaustion, edge))
    }

    /// The evaluation at the decay window's end, if every input is there.
    fn evaluate(
        &mut self,
        decay: Option<&EffortResult>,
        close: Option<Price>,
    ) -> Option<(Exhaustion, bool)> {
        let decay = decay?;
        let t = i128::from(decay.end.as_millis());
        let expansion = *self.expansions.iter().find(|expansion| {
            i128::from(expansion.end.as_millis()) == t - i128::from(EXPANSION_OFFSET_MS)
        })?;
        let up = expansion.aggressor == Aggressor::Buy;
        let span = self.minutes.iter().filter(|(end, _, _)| {
            let end = i128::from(end.as_millis());
            end > t - i128::from(EXTREME_SPAN_MS) && end <= t
        });
        let extreme = if up {
            span.map(|(_, high, _)| *high).max()
        } else {
            span.map(|(_, _, low)| *low).min()
        }?;
        let close = close?;
        let distance = (i128::from(extreme.units()) - i128::from(close.units())).abs();
        let at_extreme = distance * i128::from(SCALE)
            <= i128::from(extreme.units()).abs() * i128::from(EXTREME_TOLERANCE.units());
        // The decay's effort fits an `i64`, so its negation does too.
        let decay_aggression = if decay.aggressor == expansion.aggressor {
            decay.effort
        } else {
            Qty::from_units(-decay.effort.units())
        };
        let expanded =
            expansion.effort >= expansion.effort_high && expansion.progress >= expansion.move_high;
        let decayed = decay_aggression <= decay.effort_median;
        let active = expanded && decayed && at_extreme;
        let edge = active && !self.active;
        self.active = active;
        Some((
            Exhaustion {
                feature: self.feature,
                end: decay.end,
                expansion,
                decay: *decay,
                decay_aggression,
                extreme,
                close,
                expanded,
                decayed,
                at_extreme,
                active,
            },
            edge,
        ))
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event::samples::t;

    const M5: i64 = 300_000;

    /// A ready 5m window ending at `end` with `delta` and `displacement`
    /// (units).
    pub(crate) fn window(
        end: i64,
        delta: i64,
        displacement: Option<i64>,
    ) -> FeatureValue<AggressionWindow> {
        // `i64::MIN` has no negation: the sell volume saturates.
        let (buy, sell) = if delta >= 0 {
            (delta, 0)
        } else {
            (0, delta.checked_neg().unwrap_or(i64::MAX))
        };
        FeatureValue::Ready(AggressionWindow {
            feature: catalog::FLOW_WINDOW_5M_V1.key,
            timeframe: Timeframe::M5,
            end: t(end),
            buy_volume: Qty::from_units(buy),
            sell_volume: Qty::from_units(sell),
            delta: Qty::from_units(delta),
            volume: Qty::from_units(buy.saturating_add(sell)),
            trade_count: 1,
            large_count: 0,
            large_buy_volume: Qty::from_units(0),
            large_sell_volume: Qty::from_units(0),
            reference_price: displacement.map(|_| Price::from_units(60_000 * SCALE)),
            displacement: displacement.map(Price::from_units),
            coverage: Coverage::default(),
        })
    }

    /// A judge of B = 5 at q = 0.8 (rank 4 of 5; the median is rank 3).
    fn small() -> EffortJudge {
        EffortJudge::with(
            Timeframe::M5,
            catalog::CANDIDATE_EFFORT_RESULT_5M_V1.key,
            5,
            Rate::from_units(80_000_000),
        )
    }

    /// `judge` after the baseline efforts 10, 20, 30, 40, 50 with
    /// displacements 1–5 (so `effort_high` = 40, `effort_median` = 30,
    /// `move_high` = 4, `move_median` = 3).
    fn filled(judge: &mut EffortJudge, value: &mut FeatureValue<EffortResult>) -> i64 {
        let mut scratch = Vec::new();
        for (minute, (effort, movement)) in [(10, 1), (20, 2), (30, 3), (40, 4), (50, 5)]
            .into_iter()
            .enumerate()
        {
            let minute = i64::try_from(minute).unwrap();
            assert!(
                judge
                    .evaluate(
                        &window(minute * 60_000, effort, Some(movement)),
                        value,
                        &mut scratch
                    )
                    .is_none()
            );
        }
        5 * 60_000
    }

    fn judged(
        judge: &mut EffortJudge,
        value: &mut FeatureValue<EffortResult>,
        end: i64,
        delta: i64,
        displacement: i64,
    ) -> Option<(EffortResult, bool)> {
        judge.evaluate(
            &window(end, delta, Some(displacement)),
            value,
            &mut Vec::new(),
        )
    }

    #[test]
    fn constants_match_the_catalog() {
        let param = |definition: &crate::feature::FeatureDefinition, name: &str| {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        use crate::feature::{ParamValue, WarmUp};
        for (timeframe, definition) in catalog::CANDIDATE_EFFORT_RESULT {
            assert_eq!(
                param(definition, "baseline_evaluations"),
                Some(ParamValue::Int(
                    i64::try_from(BASELINE_EVALUATIONS).unwrap()
                ))
            );
            assert_eq!(
                param(definition, "effort_quantile"),
                Some(ParamValue::Rate(EFFORT_QUANTILE))
            );
            assert_eq!(
                param(definition, "window_ms"),
                Some(ParamValue::Int(timeframe.millis()))
            );
        }
        let exhaustion = &catalog::CANDIDATE_EXHAUSTION_V1;
        assert_eq!(
            param(exhaustion, "decay_window_ms"),
            Some(ParamValue::Int(DECAY_WINDOW.millis()))
        );
        assert_eq!(
            param(exhaustion, "expansion_window_ms"),
            Some(ParamValue::Int(EXPANSION_WINDOW.millis()))
        );
        assert_eq!(
            param(exhaustion, "extreme_tolerance"),
            Some(ParamValue::Rate(EXTREME_TOLERANCE))
        );
        assert_eq!(
            exhaustion.warm_up,
            WarmUp::Samples(u32::try_from(EXHAUSTION_SAMPLES).unwrap())
        );
        // 5 minutes and 60 + 5 = 65 minutes (ADR-051 D6).
        assert_eq!(EXPANSION_OFFSET_MS, 300_000);
        assert_eq!(EXTREME_SPAN_MS, 65 * 60_000);
        assert_eq!(EXPANSIONS as u64, EXHAUSTION_SAMPLES);
    }

    #[test]
    fn quantiles_are_nearest_rank() {
        let values: VecDeque<i64> = [50, 10, 40, 20, 30].into_iter().collect();
        let mut scratch = Vec::new();
        assert_eq!(
            quantiles(&values, Rate::from_units(80_000_000), &mut scratch),
            (40, 30)
        );
        // ⌈0.99 · 5⌉ = 5, the largest; a quantile below the median still
        // reads the right ranks.
        assert_eq!(
            quantiles(&values, Rate::from_units(99_000_000), &mut scratch),
            (50, 30)
        );
        assert_eq!(
            quantiles(&values, Rate::from_units(20_000_000), &mut scratch),
            (10, 30)
        );
        // B = 1440: ranks 1152 and 720.
        assert_eq!(rank(EFFORT_QUANTILE, BASELINE_EVALUATIONS), 1_152);
        assert_eq!(rank(MEDIAN, BASELINE_EVALUATIONS), 720);
        let ascending: VecDeque<i64> = (1..=1_440).collect();
        assert_eq!(
            quantiles(&ascending, EFFORT_QUANTILE, &mut scratch),
            (1_152, 720)
        );
    }

    #[test]
    fn effort_at_its_high_quantile_fires_and_one_unit_below_does_not() {
        for (effort, fires) in [(40, true), (39, false)] {
            let mut judge = small();
            let mut value = judge.initial();
            let end = filled(&mut judge, &mut value);
            let (result, edge) = judged(&mut judge, &mut value, end, effort, 0).unwrap();
            assert_eq!((result.active, edge), (fires, fires), "effort {effort}");
            assert_eq!(result.effort_high, Qty::from_units(40));
            assert_eq!(value, FeatureValue::Ready(result));
        }
    }

    #[test]
    fn progress_at_the_move_median_fires_and_one_tick_above_does_not() {
        for (displacement, fires) in [(3, true), (4, false)] {
            let mut judge = small();
            let mut value = judge.initial();
            let end = filled(&mut judge, &mut value);
            let (result, _) = judged(&mut judge, &mut value, end, 45, displacement).unwrap();
            assert_eq!(result.active, fires, "displacement {displacement}");
            assert_eq!(result.move_median, Price::from_units(3));
            assert_eq!(result.progress, Price::from_units(displacement));
        }
        // Net selling: progress is the displacement against the sign, so a
        // rise of 10 is a progress of −10, which fires.
        let mut judge = small();
        let mut value = judge.initial();
        let end = filled(&mut judge, &mut value);
        let (result, _) = judged(&mut judge, &mut value, end, -45, 10).unwrap();
        assert_eq!(result.aggressor, Aggressor::Sell);
        assert_eq!(result.effort, Qty::from_units(45));
        assert_eq!(result.progress, Price::from_units(-10));
        assert!(result.active);
    }

    #[test]
    fn no_delta_or_no_displacement_is_never_evaluated() {
        let mut judge = small();
        let mut value = judge.initial();
        let end = filled(&mut judge, &mut value);
        let before = judge.clone();
        let mut scratch = Vec::new();
        for window in [window(end, 0, Some(1)), window(end, 45, None)] {
            assert_eq!(judge.evaluate(&window, &mut value, &mut scratch), None);
            assert_eq!(judge, before);
        }
        let warming: FeatureValue<AggressionWindow> = FeatureValue::WarmingUp {
            observed: 1,
            required: 5,
        };
        assert_eq!(judge.evaluate(&warming, &mut value, &mut scratch), None);
        assert_eq!(judge, before);
        assert_eq!(
            value,
            FeatureValue::WarmingUp {
                observed: 4,
                required: 5
            }
        );
    }

    #[test]
    fn a_persisting_condition_emits_once_and_rearms_after_one_false_minute() {
        let mut judge = small();
        let mut value = judge.initial();
        let mut end = filled(&mut judge, &mut value);
        let mut edges = Vec::new();
        // Efforts 100, 100, 100 meet the condition; then a small effort
        // fails it; then 1 000 meets it again. A window with no delta in
        // between changes nothing.
        let mut scratch = Vec::new();
        for (delta, displacement) in [(100, 0), (100, 0), (0, 0), (100, 0), (1, 0), (1_000, 0)] {
            end += 60_000;
            let evaluated = judge.evaluate(
                &window(end, delta, Some(displacement)),
                &mut value,
                &mut scratch,
            );
            edges.push(evaluated.map(|(result, edge)| (result.active, edge)));
        }
        assert_eq!(
            edges,
            [
                Some((true, true)),
                Some((true, false)),
                None,
                Some((true, false)),
                Some((false, false)),
                Some((true, true)),
            ]
        );
    }

    #[test]
    fn the_baseline_needs_b_earlier_evaluations() {
        let mut judge = small();
        let mut value = judge.initial();
        let mut scratch = Vec::new();
        for minute in 0..5 {
            // B − 1 earlier evaluations at the fifth: still warming up.
            assert_eq!(
                judge.evaluate(
                    &window(minute * 60_000, 10 + minute, Some(1)),
                    &mut value,
                    &mut scratch
                ),
                None
            );
            assert_eq!(
                value,
                FeatureValue::WarmingUp {
                    observed: u64::try_from(minute).unwrap(),
                    required: 5
                }
            );
        }
        // B earlier evaluations: ready.
        assert!(
            judge
                .evaluate(&window(5 * 60_000, 1, Some(1)), &mut value, &mut scratch)
                .is_some()
        );
        assert!(value.is_ready());
        // The engine's judge needs 1 440.
        let engine = EffortJudge::new(0);
        assert_eq!(
            engine.initial(),
            FeatureValue::<EffortResult>::WarmingUp {
                observed: 0,
                required: 1_440
            }
        );
    }

    #[test]
    fn an_effort_out_of_range_makes_the_value_unavailable() {
        let mut judge = small();
        let mut value = judge.initial();
        let end = filled(&mut judge, &mut value);
        let before = judge.clone();
        assert_eq!(judged(&mut judge, &mut value, end, i64::MIN, 0), None);
        assert_eq!(
            value,
            FeatureValue::Unavailable {
                reason: Unavailability::OutOfRange
            }
        );
        assert_eq!(judge, before);
        // The next evaluation that fits is judged again.
        assert!(judged(&mut judge, &mut value, end + 60_000, 45, 0).is_some());
        assert!(value.is_ready());
    }

    /// A judged evaluation of `timeframe` ending at `end`.
    pub(crate) fn result(
        timeframe: Timeframe,
        end: i64,
        delta: i64,
        progress: i64,
        quantiles: [i64; 4],
    ) -> EffortResult {
        let [effort_high, effort_median, move_high, move_median] = quantiles;
        let feature = catalog::CANDIDATE_EFFORT_RESULT
            .iter()
            .find(|(candidate, _)| *candidate == timeframe)
            .unwrap()
            .1
            .key;
        let effort = delta.abs();
        EffortResult {
            feature,
            timeframe,
            end: t(end),
            aggressor: aggressor_of(delta),
            effort: Qty::from_units(effort),
            progress: Price::from_units(progress),
            effort_high: Qty::from_units(effort_high),
            effort_median: Qty::from_units(effort_median),
            move_high: Price::from_units(move_high),
            move_median: Price::from_units(move_median),
            coverage: Coverage::default(),
            active: effort >= effort_high && progress <= move_median,
        }
    }

    /// The minute of the decay: `t` = 100 minutes.
    const T: i64 = 100 * 60_000;
    /// The extreme of the span, 60 000 USDT, and a close at it.
    const EXTREME: i64 = 60_000 * SCALE;

    /// An exhaustion tracker with closed minutes up to `t` whose highest high
    /// is [`EXTREME`] and lowest low 59 000 USDT, and the expansion at
    /// `t − 5 min` already judged.
    fn exhaustion_at(expansion: EffortResult) -> ExhaustionTracker {
        let mut tracker = ExhaustionTracker::new();
        let mut value = ExhaustionTracker::initial();
        for minute in 30..=100 {
            let end = t(minute * 60_000);
            let high = if minute == 70 {
                EXTREME
            } else {
                EXTREME - 50 * SCALE
            };
            let low = if minute == 71 {
                59_000 * SCALE
            } else {
                59_900 * SCALE
            };
            tracker.minute(end, Price::from_units(high), Price::from_units(low));
            if minute * 60_000 == T - M5 {
                assert!(
                    tracker
                        .step(Some(&expansion), None, None, &mut value)
                        .is_none()
                );
            }
        }
        // Warmed up: the near-miss tests read one expansion, not six.
        tracker.samples = EXHAUSTION_SAMPLES;
        tracker
    }

    /// The 1h expansion ending at `t − 5 min`: buying, effort 100 at its
    /// high, progress 50 at its high.
    fn expansion() -> EffortResult {
        result(Timeframe::H1, T - M5, 100, 50, [100, 60, 50, 20])
    }

    /// The 5m decay ending at `t`: `delta` against an `effort_median` of 30.
    fn decay(delta: i64) -> EffortResult {
        result(Timeframe::M5, T, delta, 0, [80, 30, 10, 5])
    }

    fn evaluate(
        tracker: &mut ExhaustionTracker,
        decay: EffortResult,
        close: i64,
    ) -> Option<(Exhaustion, bool)> {
        let mut value = FeatureValue::WarmingUp {
            observed: 6,
            required: 6,
        };
        tracker.step(
            None,
            Some(&decay),
            Some(Price::from_units(close)),
            &mut value,
        )
    }

    #[test]
    fn exhaustion_fires_on_expansion_decay_and_extreme() {
        let mut tracker = exhaustion_at(expansion());
        let (exhaustion, edge) = evaluate(&mut tracker, decay(30), EXTREME).unwrap();
        assert!(edge);
        assert!(exhaustion.expanded && exhaustion.decayed && exhaustion.at_extreme);
        assert_eq!(exhaustion.extreme, Price::from_units(EXTREME));
        assert_eq!(exhaustion.decay_aggression, Qty::from_units(30));
        assert_eq!(exhaustion.end, t(T));
        // Aggression that reversed is decayed too: −40 in the expansion's
        // direction.
        let mut tracker = exhaustion_at(expansion());
        let (exhaustion, _) = evaluate(&mut tracker, decay(-40), EXTREME).unwrap();
        assert_eq!(exhaustion.decay_aggression, Qty::from_units(-40));
        assert!(exhaustion.active);
        // After net selling the extreme is the lowest low.
        let selling = result(Timeframe::H1, T - M5, -100, 50, [100, 60, 50, 20]);
        let mut tracker = exhaustion_at(selling);
        let (exhaustion, _) = evaluate(&mut tracker, decay(-30), 59_000 * SCALE).unwrap();
        assert_eq!(exhaustion.extreme, Price::from_units(59_000 * SCALE));
        assert!(exhaustion.active);
    }

    #[test]
    fn exhaustion_near_misses_do_not_fire() {
        // The decay's aggression one unit above the median.
        let mut tracker = exhaustion_at(expansion());
        let (exhaustion, edge) = evaluate(&mut tracker, decay(31), EXTREME).unwrap();
        assert!(!exhaustion.decayed && !edge);
        // The close one unit beyond 6 bps of the extreme, and exactly at it.
        let six_bps = EXTREME / 10_000 * 6;
        for (close, at) in [(EXTREME - six_bps - 1, false), (EXTREME - six_bps, true)] {
            let mut tracker = exhaustion_at(expansion());
            let (exhaustion, edge) = evaluate(&mut tracker, decay(30), close).unwrap();
            assert_eq!((exhaustion.at_extreme, edge), (at, at), "close {close}");
        }
        // The expansion's progress one unit below `move_high`, and its effort
        // one unit below `effort_high`.
        for expansion in [
            result(Timeframe::H1, T - M5, 100, 49, [100, 60, 50, 20]),
            result(Timeframe::H1, T - M5, 99, 50, [100, 60, 50, 20]),
        ] {
            let mut tracker = exhaustion_at(expansion);
            let (exhaustion, edge) = evaluate(&mut tracker, decay(30), EXTREME).unwrap();
            assert!(!exhaustion.expanded && !edge);
        }
        // No 1h evaluation at t − 5 min: no evaluation.
        let early = result(Timeframe::H1, T - M5 - 60_000, 100, 50, [100, 60, 50, 20]);
        let mut tracker = exhaustion_at(early);
        assert_eq!(evaluate(&mut tracker, decay(30), EXTREME), None);
    }

    #[test]
    fn exhaustion_waits_for_six_samples_when_one_event_closes_five_minutes() {
        let mut tracker = ExhaustionTracker::new();
        let mut value = ExhaustionTracker::initial();
        for minute in 1..=6 {
            tracker.minute(
                t(minute * 60_000),
                Price::from_units(EXTREME),
                Price::from_units(EXTREME),
            );
        }
        // The first judged 1h evaluation, at minute 1.
        let first = result(Timeframe::H1, 60_000, 100, 50, [100, 60, 50, 20]);
        assert_eq!(tracker.step(Some(&first), None, None, &mut value), None);
        // One event closes minutes 2..6 (a gap): one evaluation, at minute 6.
        // The expansion at `t − 5 min` is in the ring, but 2 of 6 samples
        // are no warm-up.
        let second = result(Timeframe::H1, 6 * 60_000, 100, 50, [100, 60, 50, 20]);
        let decay = result(Timeframe::M5, 6 * 60_000, 30, 0, [80, 30, 10, 5]);
        let evaluated = tracker.step(
            Some(&second),
            Some(&decay),
            Some(Price::from_units(EXTREME)),
            &mut value,
        );
        assert_eq!(evaluated, None);
        assert_eq!(
            value,
            FeatureValue::WarmingUp {
                observed: 2,
                required: 6
            }
        );
    }

    #[test]
    fn exhaustion_warms_up_over_six_judged_expansions() {
        let mut tracker = ExhaustionTracker::new();
        let mut value = ExhaustionTracker::initial();
        for minute in 1..=6 {
            let end = minute * 60_000;
            tracker.minute(
                t(end),
                Price::from_units(EXTREME),
                Price::from_units(EXTREME),
            );
            let expansion = result(Timeframe::H1, end, 100, 50, [100, 60, 50, 20]);
            let decay = result(Timeframe::M5, end, 30, 0, [80, 30, 10, 5]);
            let evaluated = tracker.step(
                Some(&expansion),
                Some(&decay),
                Some(Price::from_units(EXTREME)),
                &mut value,
            );
            if minute < 6 {
                assert_eq!(evaluated, None);
                assert_eq!(
                    value,
                    FeatureValue::WarmingUp {
                        observed: u64::try_from(minute).unwrap(),
                        required: 6
                    }
                );
            } else {
                // The sixth reads the first as its expansion.
                let (exhaustion, edge) = evaluated.unwrap();
                assert_eq!(exhaustion.expansion.end, t(60_000));
                assert!(edge);
                assert_eq!(value, FeatureValue::Ready(exhaustion));
            }
        }
    }
}
