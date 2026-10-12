//! CVD and open-interest divergences on confirmed swings (ADR-051 D4):
//! `candidate.divergence.cvd.<tf>@1` and `candidate.divergence.oi.<tf>@1`
//! for every structure timeframe.
//!
//! - **Anchor.** For each structure timeframe the tracker records the first
//!   trade that sets the developing bar's high, and the first that sets its
//!   low: a strictly higher (lower) price replaces the record, so a tie
//!   keeps the first. At that trade it records `flow.cvd.continuous@1`
//!   after the trade and the ready `derivatives.oi.5m@1` point, if any.
//! - **Lookup.** Closed bars' records sit in a ring of [`RING`] per
//!   timeframe: the swing bar is exactly [`SWING_BARS`] closed bars before
//!   the confirming bar, because every elapsed interval yields a bar
//!   (ADR-031, ADR-037 D3).
//! - **CVD rule** (signed, so it mirrors): a higher high with a lower CVD,
//!   or a lower low with a higher CVD, all comparisons strict.
//! - **OI rule** (unsigned, so it does not mirror): a further extreme on
//!   either side with a lower open interest. Without a ready grid point at
//!   either swing the OI comparison has no change and emits nothing.
//!
//! No magnitude threshold: every comparison carries its changes, so
//! research can threshold on measured contribution (ADR-013).

use super::Inputs;
use super::context::{Exclude, LocationContext};
use crate::bars::{Bar, Coverage, Timeframe};
use crate::derivatives::OiGridPoint;
use crate::event::Trade;
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::fingerprint::Fingerprinter;
use crate::flow::Cvd;
use crate::location::LevelKind;
use crate::num::{Price, Qty};
use crate::state_hash::StateEncode;
use crate::structure::{SWING_BARS, Side, Swing};
use crate::time::EventTime;
use std::collections::VecDeque;
use std::fmt;

/// Closed-bar records kept per timeframe: the swing bar and the
/// [`SWING_BARS`] bars up to the confirming one.
pub const RING: usize = SWING_BARS + 1;

/// One confirmed swing with the readings at its anchor: the first trade
/// that set the swing bar's extreme.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwingAnchor {
    /// Open time of the swing bar.
    pub swing_time: EventTime,
    /// When the swing became visible: [`Swing::known_at`].
    pub known_at: EventTime,
    /// The swing price: the bar's extreme, the anchor trade's price.
    pub price: Price,
    /// Time of the anchor trade.
    pub time: EventTime,
    /// `flow.cvd.continuous@1` after the anchor trade.
    pub cvd: Qty,
    /// The CVD's trades-gap count after the anchor trade.
    pub gaps: u64,
    /// The swing's coverage.
    pub coverage: Coverage,
}

impl StateEncode for SwingAnchor {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            swing_time,
            known_at,
            price,
            time,
            cvd,
            gaps,
            coverage,
        } = self;
        swing_time.encode(f);
        known_at.encode(f);
        price.encode(f);
        time.encode(f);
        cvd.encode(f);
        gaps.encode(f);
        coverage.encode(f);
    }
}

/// Two consecutive anchored swings of one side and timeframe, with the
/// exact changes between their anchors.
///
/// `Display` prints the side, both anchors as `price@time`, the changes,
/// the trades gaps between them, the span and the coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SwingPair {
    /// Which extreme.
    pub side: Side,
    /// The earlier swing.
    pub previous: SwingAnchor,
    /// The swing just confirmed.
    pub current: SwingAnchor,
    /// `current.price − previous.price`.
    pub price_change: Price,
    /// `current.cvd − previous.cvd`: the net taker aggression between the
    /// two extremes when `gaps_between` is 0 (ADR-035 D2).
    pub cvd_change: Qty,
    /// Trades feed gaps between the two anchors.
    pub gaps_between: u64,
    /// Milliseconds from the earlier anchor trade to the later one.
    pub span_ms: u64,
    /// The OR of both swings' coverage, plus `feed_gap` when
    /// `gaps_between > 0`.
    pub coverage: Coverage,
}

impl StateEncode for SwingPair {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            side,
            previous,
            current,
            price_change,
            cvd_change,
            gaps_between,
            span_ms,
            coverage,
        } = self;
        side.encode(f);
        previous.encode(f);
        current.encode(f);
        price_change.encode(f);
        cvd_change.encode(f);
        gaps_between.encode(f);
        span_ms.encode(f);
        coverage.encode(f);
    }
}

impl fmt::Display for SwingPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} prev={}@{} cur={}@{} dprice={} cvd={}->{} dcvd={} gaps={} span={}ms {}",
            self.side,
            self.previous.price,
            self.previous.time,
            self.current.price,
            self.current.time,
            self.price_change,
            self.previous.cvd,
            self.current.cvd,
            self.cvd_change,
            self.gaps_between,
            self.span_ms,
            self.coverage
        )
    }
}

impl SwingPair {
    /// Whether the current swing is a further extreme: a strictly higher
    /// high or a strictly lower low.
    pub fn is_further_extreme(&self) -> bool {
        match self.side {
            Side::High => self.price_change.units() > 0,
            Side::Low => self.price_change.units() < 0,
        }
    }
}

/// The CVD comparison of one swing pair: `candidate.divergence.cvd.<tf>@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CvdComparison {
    /// The swings and their changes.
    pub pair: SwingPair,
    /// A further extreme while the net aggression since the previous one
    /// ran the other way: a higher high with a lower CVD, or a lower low
    /// with a higher CVD (strict).
    pub divergent: bool,
}

impl StateEncode for CvdComparison {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { pair, divergent } = self;
        pair.encode(f);
        divergent.encode(f);
    }
}

impl fmt::Display for CvdComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} divergent={}", self.pair, self.divergent)
    }
}

/// The open-interest comparison of one swing pair:
/// `candidate.divergence.oi.<tf>@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OiComparison {
    /// The swings and their changes.
    pub pair: SwingPair,
    /// The ready `derivatives.oi.5m@1` point at the earlier anchor.
    pub previous_oi: Option<OiGridPoint>,
    /// The ready `derivatives.oi.5m@1` point at the later anchor.
    pub current_oi: Option<OiGridPoint>,
    /// The change of open interest between the two points; `None` unless
    /// both are present.
    pub oi_change: Option<Qty>,
    /// A further extreme, on either side, with strictly lower open
    /// interest. `false` without an `oi_change`.
    pub divergent: bool,
}

impl StateEncode for OiComparison {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            pair,
            previous_oi,
            current_oi,
            oi_change,
            divergent,
        } = self;
        pair.encode(f);
        previous_oi.encode(f);
        current_oi.encode(f);
        oi_change.encode(f);
        divergent.encode(f);
    }
}

impl fmt::Display for OiComparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let oi = |point: Option<OiGridPoint>| {
            point.map_or_else(|| "-".to_owned(), |point| point.open_interest.to_string())
        };
        write!(
            f,
            "{} oi={}->{} doi={} divergent={}",
            self.pair,
            oi(self.previous_oi),
            oi(self.current_oi),
            self.oi_change
                .map_or_else(|| "-".to_owned(), |change| change.to_string()),
            self.divergent
        )
    }
}

/// The value of a divergence feature: the last comparison of each side,
/// `None` until that side's second anchored swing.
///
/// `Display` prints `high=[…] low=[…]` (`-` for none) and the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Divergence<C> {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The last comparison of swing highs.
    pub high: Option<C>,
    /// The last comparison of swing lows.
    pub low: Option<C>,
}

impl<C: StateEncode> StateEncode for Divergence<C> {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { feature, high, low } = self;
        feature.encode(f);
        high.encode(f);
        low.encode(f);
    }
}

impl<C: fmt::Display> fmt::Display for Divergence<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (label, comparison) in [("high", &self.high), ("low", &self.low)] {
            match comparison {
                Some(comparison) => write!(f, "{label}=[{comparison}] ")?,
                None => write!(f, "{label}=- ")?,
            }
        }
        write!(f, "{}", self.feature)
    }
}

/// The divergence features of one structure timeframe.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeframeDivergences {
    /// `candidate.divergence.cvd.<tf>@1`.
    pub cvd: FeatureValue<Divergence<CvdComparison>>,
    /// `candidate.divergence.oi.<tf>@1`.
    pub oi: FeatureValue<Divergence<OiComparison>>,
}

impl StateEncode for TimeframeDivergences {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { cvd, oi } = self;
        cvd.encode(f);
        oi.encode(f);
    }
}

impl TimeframeDivergences {
    /// Both values warming up: no anchored swing yet.
    pub(crate) fn new() -> Self {
        Self {
            cvd: warming(),
            oi: warming(),
        }
    }
}

/// A divergence value before its first anchored swing.
fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

/// A divergent comparison, emitted by the event that confirmed its later
/// swing (ADR-051 D4).
///
/// `Display` prints the timeframe, the comparison, the feature key and the
/// location context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DivergenceEvent<C> {
    /// The feature that produced it: `candidate.divergence.<cvd|oi>.<tf>@1`.
    pub feature: FeatureKey,
    /// The structure timeframe of the swings.
    pub timeframe: Timeframe,
    /// Time of the event that confirmed the later swing: the swing's
    /// `known_at`.
    pub known_at: EventTime,
    /// The swing input: `structure.swing.<tf>@1`.
    pub swings: FeatureKey,
    /// The CVD input: `flow.cvd.continuous@1`.
    pub cvd: FeatureKey,
    /// The comparison.
    pub comparison: C,
    /// Where the later swing's price sits (ADR-051 D3).
    pub context: LocationContext,
}

impl<C: fmt::Display> fmt::Display for DivergenceEvent<C> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} {} {}",
            self.timeframe, self.comparison, self.feature, self.context
        )
    }
}

/// The comparisons a confirmed swing produced, ready to be emitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Emitted {
    /// A divergent CVD comparison.
    Cvd(DivergenceEvent<CvdComparison>),
    /// A divergent OI comparison.
    Oi(DivergenceEvent<OiComparison>),
}

/// The record of the first trade at one extreme of a developing bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Mark {
    price: Price,
    time: EventTime,
    cvd: Qty,
    gaps: u64,
    oi: Option<OiGridPoint>,
}

/// The extreme records of one bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Extremes {
    open_time: EventTime,
    high: Option<Mark>,
    low: Option<Mark>,
}

/// One anchored swing as the tracker keeps it: the public anchor and the
/// OI point at it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Anchored {
    anchor: SwingAnchor,
    oi: Option<OiGridPoint>,
}

/// The last comparison of one side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Slot<C> {
    /// No comparison yet.
    Empty,
    /// The last comparison.
    Compared(C),
    /// The last comparison's changes did not fit their type.
    OutOfRange,
}

/// The divergence state of one structure timeframe. Engine state, updated
/// in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DivergenceTracker {
    timeframe: Timeframe,
    swings: FeatureKey,
    cvd_feature: FeatureKey,
    oi_feature: FeatureKey,
    /// The id of the timeframe's `structure.levels` registry, whose level
    /// at the swing is left out of the context.
    levels_id: &'static str,
    /// The developing bar's records.
    developing: Option<Extremes>,
    /// The last [`RING`] closed bars' records, oldest first.
    ring: VecDeque<Extremes>,
    /// The last anchored swing of each side: high, low.
    last: [Option<Anchored>; 2],
    /// The last CVD comparison of each side.
    cvd: [Slot<CvdComparison>; 2],
    /// The last OI comparison of each side.
    oi: [Slot<OiComparison>; 2],
    /// Whether a swing has been anchored: the warm-up sample.
    warm: bool,
}

/// The index of `side` in the per-side arrays.
fn index(side: Side) -> usize {
    match side {
        Side::High => 0,
        Side::Low => 1,
    }
}

impl DivergenceTracker {
    /// The tracker of the structure timeframe at `position` in
    /// [`catalog::STRUCTURE_SWING`] order.
    pub(crate) fn new(position: usize) -> Self {
        let (timeframe, swings) = catalog::STRUCTURE_SWING[position];
        let (_, cvd) = catalog::CANDIDATE_DIVERGENCE_CVD[position];
        let (_, oi) = catalog::CANDIDATE_DIVERGENCE_OI[position];
        let (_, levels) = catalog::STRUCTURE_LEVELS[position];
        Self {
            timeframe,
            swings: swings.key,
            cvd_feature: cvd.key,
            oi_feature: oi.key,
            levels_id: levels.key.id.as_str(),
            developing: None,
            ring: VecDeque::with_capacity(RING + 1),
            last: [None; 2],
            cvd: [Slot::Empty; 2],
            oi: [Slot::Empty; 2],
            warm: false,
        }
    }

    /// The timeframe.
    pub(crate) fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Moves the developing records into the ring as those of `bar`, a
    /// closed bar of this timeframe. A bar without records (no trade, or
    /// records of another bar) enters with none.
    pub(crate) fn close(&mut self, bar: &Bar) {
        let entry = self
            .developing
            .take()
            .filter(|extremes| extremes.open_time == bar.open_time)
            .unwrap_or(Extremes {
                open_time: bar.open_time,
                high: None,
                low: None,
            });
        self.ring.push_back(entry);
        if self.ring.len() > RING {
            self.ring.pop_front();
        }
    }

    /// Records `trade` if it sets the developing bar's high or low, with the
    /// CVD after it and the ready OI point.
    pub(crate) fn trade(&mut self, trade: &Trade, cvd: &Cvd, oi: Option<&OiGridPoint>) {
        let Some(open_time) = self.timeframe.open_of(trade.time) else {
            return;
        };
        let extremes = match &mut self.developing {
            Some(extremes) if extremes.open_time == open_time => extremes,
            developing => developing.insert(Extremes {
                open_time,
                high: None,
                low: None,
            }),
        };
        let mark = || Mark {
            price: trade.price,
            time: trade.time,
            cvd: cvd.cvd,
            gaps: cvd.gaps,
            oi: oi.copied(),
        };
        if extremes.high.is_none_or(|high| trade.price > high.price) {
            extremes.high = Some(mark());
        }
        if extremes.low.is_none_or(|low| trade.price < low.price) {
            extremes.low = Some(mark());
        }
    }

    /// Handles `swing`, confirmed by the bar just closed: anchors it,
    /// compares it with the previous anchored swing of its side, updates
    /// `value` and appends the divergent comparisons to `emitted`.
    pub(crate) fn confirm(
        &mut self,
        swing: &Swing,
        inputs: &Inputs<'_>,
        value: &mut TimeframeDivergences,
        emitted: &mut Vec<Emitted>,
    ) {
        let side = index(swing.side);
        let mark = (self.ring.len() == RING)
            .then(|| self.ring.front())
            .flatten()
            .filter(|extremes| extremes.open_time == swing.swing_time)
            .and_then(|extremes| match swing.side {
                Side::High => extremes.high,
                Side::Low => extremes.low,
            })
            .filter(|mark| mark.price == swing.price);
        let Some(mark) = mark else {
            // No record of the swing's extreme: the chain of pairs breaks
            // rather than compare across it.
            self.last[side] = None;
            return;
        };
        let current = Anchored {
            anchor: SwingAnchor {
                swing_time: swing.swing_time,
                known_at: swing.known_at,
                price: mark.price,
                time: mark.time,
                cvd: mark.cvd,
                gaps: mark.gaps,
                coverage: swing.coverage,
            },
            oi: mark.oi,
        };
        self.warm = true;
        if let Some(previous) = self.last[side] {
            self.compare(swing, &previous, &current, inputs, emitted);
        }
        self.last[side] = Some(current);
        *value = TimeframeDivergences {
            cvd: self.value(self.cvd_feature, &self.cvd),
            oi: self.value(self.oi_feature, &self.oi),
        };
    }

    /// Compares two anchored swings of one side and stores the result.
    fn compare(
        &mut self,
        swing: &Swing,
        previous: &Anchored,
        current: &Anchored,
        inputs: &Inputs<'_>,
        emitted: &mut Vec<Emitted>,
    ) {
        let side = index(swing.side);
        let Some(pair) = pair(swing.side, &previous.anchor, &current.anchor) else {
            self.cvd[side] = Slot::OutOfRange;
            self.oi[side] = Slot::OutOfRange;
            return;
        };
        let further = pair.is_further_extreme();
        let cvd = CvdComparison {
            pair,
            divergent: further
                && match swing.side {
                    Side::High => pair.cvd_change.units() < 0,
                    Side::Low => pair.cvd_change.units() > 0,
                },
        };
        self.cvd[side] = Slot::Compared(cvd);
        let oi = match (previous.oi, current.oi) {
            (Some(before), Some(after)) => {
                difference(before.open_interest.units(), after.open_interest.units())
                    .map(|change| Some(Qty::from_units(change)))
            }
            _ => Some(None),
        }
        .map(|oi_change| OiComparison {
            pair,
            previous_oi: previous.oi,
            current_oi: current.oi,
            oi_change,
            divergent: further && oi_change.is_some_and(|change| change.units() < 0),
        });
        self.oi[side] = oi.map_or(Slot::OutOfRange, Slot::Compared);
        if !cvd.divergent && !oi.is_some_and(|oi| oi.divergent) {
            return;
        }
        let context = LocationContext::read(
            swing.price,
            Some(Exclude {
                kind: match swing.side {
                    Side::High => LevelKind::StructuralHigh,
                    Side::Low => LevelKind::StructuralLow,
                },
                source_id: self.levels_id,
                anchor: swing.swing_time,
            }),
            inputs.location,
        );
        if cvd.divergent {
            let event = self.event(self.cvd_feature, swing, cvd, context.clone());
            emitted.push(Emitted::Cvd(event));
        }
        if let Some(oi) = oi.filter(|oi| oi.divergent) {
            emitted.push(Emitted::Oi(self.event(self.oi_feature, swing, oi, context)));
        }
    }

    /// The event of a divergent `comparison` of `feature`, confirmed with
    /// `swing`.
    fn event<C>(
        &self,
        feature: FeatureKey,
        swing: &Swing,
        comparison: C,
        context: LocationContext,
    ) -> DivergenceEvent<C> {
        DivergenceEvent {
            feature,
            timeframe: self.timeframe,
            known_at: swing.known_at,
            swings: self.swings,
            cvd: catalog::FLOW_CVD_CONTINUOUS_V1.key,
            comparison,
            context,
        }
    }

    /// The value of one family: warming up until the first anchored swing,
    /// `Unavailable(OutOfRange)` while a side's last comparison did not
    /// fit, ready otherwise.
    fn value<C: Copy>(
        &self,
        feature: FeatureKey,
        slots: &[Slot<C>; 2],
    ) -> FeatureValue<Divergence<C>> {
        if !self.warm {
            return warming();
        }
        let mut sides = [None; 2];
        for (side, slot) in sides.iter_mut().zip(slots) {
            match slot {
                Slot::Empty => {}
                Slot::Compared(comparison) => *side = Some(*comparison),
                Slot::OutOfRange => {
                    return FeatureValue::Unavailable {
                        reason: Unavailability::OutOfRange,
                    };
                }
            }
        }
        let [high, low] = sides;
        FeatureValue::Ready(Divergence { feature, high, low })
    }
}

/// `after − before` in `i128`, if it fits an `i64`.
fn difference(before: i64, after: i64) -> Option<i64> {
    i64::try_from(i128::from(after) - i128::from(before)).ok()
}

/// The pair of two anchored swings of `side`; `None` if a change does not
/// fit its type.
fn pair(side: Side, previous: &SwingAnchor, current: &SwingAnchor) -> Option<SwingPair> {
    let gaps_between = current.gaps.checked_sub(previous.gaps)?;
    let mut coverage = Coverage {
        partial_start: previous.coverage.partial_start || current.coverage.partial_start,
        feed_gap: previous.coverage.feed_gap || current.coverage.feed_gap,
    };
    coverage.feed_gap |= gaps_between > 0;
    Some(SwingPair {
        side,
        previous: *previous,
        current: *current,
        price_change: Price::from_units(difference(previous.price.units(), current.price.units())?),
        cvd_change: Qty::from_units(difference(previous.cvd.units(), current.cvd.units())?),
        gaps_between,
        span_ms: u64::try_from(difference(
            previous.time.as_millis(),
            current.time.as_millis(),
        )?)
        .ok()?,
        coverage,
    })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::candidate::CandidateEvent;
    use crate::event::samples::t;
    use crate::event::{Aggressor, FeedGap, GapReason, MarketEvent, OpenInterest, Stream};
    use crate::num::SCALE;
    use crate::state::MarketStateEngine;
    use crate::structure::StructureEvent;

    const M15: i64 = 900_000;
    /// The base price of the scenarios: 60 000 USDT.
    const BASE: i64 = 60_000 * SCALE;
    /// The anchor of swing A lies 10 USDT beyond the base.
    const A: i64 = 10 * SCALE;
    /// The swing bars.
    const BAR_A: i64 = 3;
    const BAR_B: i64 = 10;
    /// The anchor trade's offset in its bar.
    const ANCHOR_OFFSET: i64 = 3_000;
    /// The aggression of each anchor trade, in quantity units (a buy).
    const ANCHOR_QTY: i64 = 100;

    /// A two-swing tape on 15m bars (ADR-051 D4 tests). The swing bars are 3
    /// (A) and 10 (B), with extremes `A` and `A + b` beyond the base on
    /// `side`; every other bar's extreme lies 0–3 USDT beyond the base, so
    /// both are swings. Every trade comes in zero-net buy/sell pairs, but
    /// the anchor trades (a buy of [`ANCHOR_QTY`] each) and one adjusting
    /// trade in bar 7, so that `cvd(B) − cvd(A)` is exactly `cvd_change`.
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct Scenario {
        pub(crate) side: Side,
        /// Units from A's extreme to B's, away from the base.
        pub(crate) b: i64,
        pub(crate) cvd_change: i64,
        /// Open interest at A and from bar 7 on, in whole BTC; `None` for no
        /// samples.
        pub(crate) oi: Option<(i64, i64)>,
        /// The first bar with open-interest samples.
        pub(crate) oi_from_bar: i64,
        /// A trades gap in bar 8.
        pub(crate) gap: bool,
        /// A second trade at B's extreme, after the anchor.
        pub(crate) double_touch: bool,
        /// Bars 11–15 trade nothing: the confirming bar closes inside a
        /// multi-bar close.
        pub(crate) silent_right: bool,
    }

    impl Scenario {
        /// A one-unit further extreme with a one-unit opposite CVD change: a
        /// CVD divergence.
        pub(crate) fn divergent(side: Side) -> Self {
            Self {
                side,
                b: 1,
                cvd_change: match side {
                    Side::High => -1,
                    Side::Low => 1,
                },
                oi: None,
                oi_from_bar: 0,
                gap: false,
                double_touch: false,
                silent_right: false,
            }
        }

        /// The price `offset` units beyond the base on the scenario's side.
        fn beyond(&self, offset: i64) -> i64 {
            match self.side {
                Side::High => BASE + offset,
                Side::Low => BASE - offset,
            }
        }

        /// The extreme of `bar`.
        fn extreme(&self, bar: i64) -> i64 {
            let offset = match bar {
                BAR_A => A,
                BAR_B => A + self.b,
                _ => [0, 1, 2, 0, 2, 1, 0, 1, 2, 3, 0, 3, 2, 1, 0, 0, 0][bar as usize] * SCALE,
            };
            self.beyond(offset)
        }

        /// The price one USDT back from the extreme of `bar`.
        fn mid(&self, bar: i64) -> i64 {
            match self.side {
                Side::High => self.extreme(bar) - SCALE,
                Side::Low => self.extreme(bar) + SCALE,
            }
        }

        /// Open time of the anchor trade of swing bar `bar`.
        pub(crate) fn anchor_time(bar: i64) -> i64 {
            bar * M15 + ANCHOR_OFFSET
        }

        /// The tape: bars 0–13 (bar 14 only confirms), or 0–10 and a trade
        /// in bar 16 when the right side is silent.
        pub(crate) fn tape(&self) -> Vec<MarketEvent> {
            let mut events = Vec::new();
            let mut id = 0;
            let mut push = |events: &mut Vec<MarketEvent>, millis: i64, price: i64, qty: i64| {
                id += 1;
                events.push(MarketEvent::Trade(Trade {
                    time: t(millis),
                    trade_id: id,
                    price: Price::from_units(price),
                    qty: Qty::from_units(qty.abs()),
                    aggressor: if qty > 0 {
                        Aggressor::Buy
                    } else {
                        Aggressor::Sell
                    },
                }));
            };
            let last_bar = if self.silent_right { BAR_B } else { 13 };
            for bar in 0..=last_bar {
                let open = bar * M15;
                if let Some((before, after)) = self.oi
                    && bar >= self.oi_from_bar
                {
                    for boundary in 0..3 {
                        events.push(MarketEvent::OpenInterest(OpenInterest {
                            time: t(open + boundary * 300_000),
                            open_interest: Qty::from_units(
                                if bar < 7 { before } else { after } * SCALE,
                            ),
                            resolution_ms: 300_000,
                        }));
                    }
                }
                let mut trades = vec![
                    (open + 1_000, self.mid(bar), 10),
                    (open + 2_000, self.mid(bar), -10),
                ];
                if bar == BAR_A || bar == BAR_B {
                    trades.push((open + ANCHOR_OFFSET, self.extreme(bar), ANCHOR_QTY));
                    if bar == BAR_B && self.double_touch {
                        trades.push((open + 4_000, self.extreme(bar), -50));
                        trades.push((open + 4_500, self.mid(bar), 50));
                    }
                } else {
                    trades.push((open + ANCHOR_OFFSET, self.extreme(bar), 7));
                    trades.push((open + 3_500, self.extreme(bar), -7));
                }
                let adjust = self.cvd_change - ANCHOR_QTY;
                if bar == 7 && adjust != 0 {
                    trades.push((open + 5_000, self.mid(bar), adjust));
                }
                // Keep the open interest boundaries apart from the trades.
                trades.sort_by_key(|(millis, _, _)| *millis);
                for (millis, price, qty) in trades {
                    push(&mut events, millis, price, qty);
                }
                if bar == 8 && self.gap {
                    events.push(MarketEvent::FeedGap(FeedGap {
                        stream: Stream::Trades,
                        start: t(open + 10_000),
                        end: t(open + 20_000),
                        reason: GapReason::Disconnected,
                    }));
                }
            }
            let confirm = if self.silent_right { 16 } else { 14 };
            push(&mut events, confirm * M15 + 1_000, self.mid(13), 10);
            events.sort();
            events
        }
    }

    /// Runs `events` through `engine` and returns every candidate fact with
    /// the time of the event that emitted it.
    pub(crate) fn run(
        engine: &mut MarketStateEngine,
        events: &[MarketEvent],
    ) -> Vec<(EventTime, CandidateEvent)> {
        let mut facts = Vec::new();
        for event in events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
            facts.extend(
                engine
                    .candidate_events()
                    .iter()
                    .map(|fact| (event.time(), fact.clone())),
            );
        }
        facts
    }

    fn facts_of(scenario: Scenario) -> (Vec<(EventTime, CandidateEvent)>, MarketStateEngine) {
        let mut engine = MarketStateEngine::new();
        let facts = run(&mut engine, &scenario.tape());
        (facts, engine)
    }

    /// The 15m CVD comparison of `side` in the engine's state.
    fn cvd_of(engine: &MarketStateEngine, side: Side) -> Option<CvdComparison> {
        let value = engine
            .state()
            .candidates
            .divergences(Timeframe::M15)
            .unwrap()
            .cvd;
        let divergence = value.ready().copied().unwrap();
        match side {
            Side::High => divergence.high,
            Side::Low => divergence.low,
        }
    }

    /// The 15m OI comparison of `side` in the engine's state.
    fn oi_of(engine: &MarketStateEngine, side: Side) -> Option<OiComparison> {
        let value = engine
            .state()
            .candidates
            .divergences(Timeframe::M15)
            .unwrap()
            .oi;
        let divergence = value.ready().copied().unwrap();
        match side {
            Side::High => divergence.high,
            Side::Low => divergence.low,
        }
    }

    /// The 15m CVD facts of `side`.
    fn cvd_facts(
        facts: &[(EventTime, CandidateEvent)],
        side: Side,
    ) -> Vec<DivergenceEvent<CvdComparison>> {
        facts
            .iter()
            .filter_map(|(_, fact)| match fact {
                CandidateEvent::CvdDivergence(event)
                    if event.timeframe == Timeframe::M15 && event.comparison.pair.side == side =>
                {
                    Some(event.clone())
                }
                _ => None,
            })
            .collect()
    }

    /// The 15m OI facts of `side`.
    fn oi_facts(
        facts: &[(EventTime, CandidateEvent)],
        side: Side,
    ) -> Vec<DivergenceEvent<OiComparison>> {
        facts
            .iter()
            .filter_map(|(_, fact)| match fact {
                CandidateEvent::OiDivergence(event)
                    if event.timeframe == Timeframe::M15 && event.comparison.pair.side == side =>
                {
                    Some(event.clone())
                }
                _ => None,
            })
            .collect()
    }

    #[test]
    fn a_one_unit_further_extreme_with_a_one_unit_opposite_cvd_fires_on_both_sides() {
        for side in [Side::High, Side::Low] {
            let scenario = Scenario::divergent(side);
            let (facts, engine) = facts_of(scenario);
            let fired = cvd_facts(&facts, side);
            assert_eq!(fired.len(), 1, "{side}: {facts:?}");
            let event = &fired[0];
            let pair = event.comparison.pair;
            let one = match side {
                Side::High => 1,
                Side::Low => -1,
            };
            assert_eq!(pair.price_change, Price::from_units(one));
            assert_eq!(pair.cvd_change, Qty::from_units(-one));
            assert_eq!(pair.previous.price, Price::from_units(scenario.beyond(A)));
            assert_eq!(
                pair.current.price,
                Price::from_units(scenario.beyond(A + 1))
            );
            assert_eq!(pair.previous.time, t(Scenario::anchor_time(BAR_A)));
            assert_eq!(pair.current.time, t(Scenario::anchor_time(BAR_B)));
            assert_eq!(pair.previous.swing_time, t(BAR_A * M15));
            assert_eq!(pair.current.swing_time, t(BAR_B * M15));
            assert_eq!(pair.span_ms, u64::try_from((BAR_B - BAR_A) * M15).unwrap());
            assert_eq!((pair.gaps_between, pair.coverage.feed_gap), (0, false));
            assert!(event.comparison.divergent);
            assert_eq!(event.feature, catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1.key);
            assert_eq!(event.swings, catalog::STRUCTURE_SWING_15M_V1.key);
            assert_eq!(event.cvd, catalog::FLOW_CVD_CONTINUOUS_V1.key);
            // The value holds the same comparison.
            assert_eq!(cvd_of(&engine, side), Some(event.comparison));
            // Without samples, the OI comparison exists but has no change.
            let oi = oi_of(&engine, side).unwrap();
            assert_eq!(
                (oi.previous_oi, oi.current_oi, oi.oi_change),
                (None, None, None)
            );
            assert!(!oi.divergent);
            assert!(oi_facts(&facts, side).is_empty());
        }
    }

    #[test]
    fn near_misses_one_unit_across_each_boundary_do_not_fire() {
        for side in [Side::High, Side::Low] {
            let toward = match side {
                Side::High => 1,
                Side::Low => -1,
            };
            let base = Scenario::divergent(side);
            for (name, scenario) in [
                ("equal price", Scenario { b: 0, ..base }),
                (
                    "equal cvd",
                    Scenario {
                        cvd_change: 0,
                        ..base
                    },
                ),
                (
                    "cvd the same way",
                    Scenario {
                        cvd_change: toward,
                        ..base
                    },
                ),
                ("a nearer extreme", Scenario { b: -1, ..base }),
            ] {
                let (facts, engine) = facts_of(scenario);
                assert!(
                    cvd_facts(&facts, side).is_empty(),
                    "{side} {name}: {facts:?}"
                );
                // The pair is still compared and recorded.
                let comparison = cvd_of(&engine, side).unwrap();
                assert!(!comparison.divergent, "{side} {name}");
                assert_eq!(
                    comparison.pair.cvd_change,
                    Qty::from_units(scenario.cvd_change),
                    "{side} {name}"
                );
            }
        }
    }

    #[test]
    fn open_interest_diverges_on_lower_open_interest_on_either_side() {
        for side in [Side::High, Side::Low] {
            let base = Scenario {
                oi: Some((80_000, 79_999)),
                ..Scenario::divergent(side)
            };
            // Unsigned: a further extreme on lower open interest, high or
            // low.
            let (facts, engine) = facts_of(base);
            let fired = oi_facts(&facts, side);
            assert_eq!(fired.len(), 1, "{side}: {facts:?}");
            let comparison = fired[0].comparison;
            assert_eq!(comparison.oi_change, Some(Qty::from_units(-SCALE)));
            assert_eq!(
                comparison.previous_oi.map(|point| point.open_interest),
                Some(Qty::from_units(80_000 * SCALE))
            );
            assert_eq!(
                fired[0].feature,
                catalog::CANDIDATE_DIVERGENCE_OI_15M_V1.key
            );
            assert_eq!(oi_of(&engine, side), Some(comparison));
            // The CVD event comes first, from the same swing.
            let kinds: Vec<bool> = facts
                .iter()
                .filter(|(_, fact)| fact.feature().id.as_str().ends_with(".15m"))
                .map(|(_, fact)| matches!(fact, CandidateEvent::CvdDivergence(_)))
                .collect();
            assert_eq!(kinds, [true, false], "{side}");
            for (name, scenario) in [
                (
                    "equal open interest",
                    Scenario {
                        oi: Some((80_000, 80_000)),
                        ..base
                    },
                ),
                (
                    "higher open interest",
                    Scenario {
                        oi: Some((80_000, 80_001)),
                        ..base
                    },
                ),
                ("equal price", Scenario { b: 0, ..base }),
                // No sample before bar 7: no grid point at A.
                (
                    "no grid point at one swing",
                    Scenario {
                        oi_from_bar: 7,
                        ..base
                    },
                ),
            ] {
                let (facts, engine) = facts_of(scenario);
                assert!(
                    oi_facts(&facts, side).is_empty(),
                    "{side} {name}: {facts:?}"
                );
                let comparison = oi_of(&engine, side).unwrap();
                assert!(!comparison.divergent, "{side} {name}");
                if name == "no grid point at one swing" {
                    // The value records the missing point.
                    assert_eq!(comparison.previous_oi, None);
                    assert!(comparison.current_oi.is_some());
                    assert_eq!(comparison.oi_change, None);
                }
            }
        }
    }

    #[test]
    fn the_anchor_is_the_first_trade_at_the_extreme() {
        let scenario = Scenario {
            double_touch: true,
            ..Scenario::divergent(Side::High)
        };
        let (facts, _) = facts_of(scenario);
        let fired = cvd_facts(&facts, Side::High);
        assert_eq!(fired.len(), 1);
        let current = fired[0].comparison.pair.current;
        // The second touch at the same price, a sell, does not replace it.
        assert_eq!(current.time, t(Scenario::anchor_time(BAR_B)));
        assert_eq!(fired[0].comparison.pair.cvd_change, Qty::from_units(-1));
    }

    #[test]
    fn nothing_is_emitted_or_changed_before_the_swing_is_known() {
        let scenario = Scenario {
            oi: Some((80_000, 79_999)),
            ..Scenario::divergent(Side::High)
        };
        let mut engine = MarketStateEngine::new();
        let mut before = engine.state().candidates.clone();
        let mut changes = Vec::new();
        for event in scenario.tape() {
            engine.apply(&event).unwrap();
            let swings: Vec<Swing> = engine
                .structure_events()
                .iter()
                .filter_map(|fact| match fact {
                    StructureEvent::Swing(swing) if swing.timeframe == Timeframe::M15 => {
                        Some(*swing)
                    }
                    _ => None,
                })
                .collect();
            let after = engine.state().candidates.clone();
            if after != before || !engine.candidate_events().is_empty() {
                // Only the event that confirms a 15m swing changes anything,
                // and every fact is visible at that swing's known_at.
                assert!(!swings.is_empty(), "{}", event.time());
                for fact in engine.candidate_events() {
                    assert_eq!(fact.time(), event.time());
                    assert!(swings.iter().any(|swing| swing.known_at == fact.time()));
                }
                changes.push(event.time());
            }
            before = after;
        }
        // A's confirmation readies the values; B's emits.
        assert_eq!(changes, [t((BAR_A + 4) * M15 + 1_000), t(14 * M15 + 1_000)]);
    }

    #[test]
    fn a_trades_gap_between_the_anchors_is_counted_and_flagged() {
        let scenario = Scenario {
            gap: true,
            ..Scenario::divergent(Side::High)
        };
        let (facts, _) = facts_of(scenario);
        let fired = cvd_facts(&facts, Side::High);
        assert_eq!(fired.len(), 1);
        let pair = fired[0].comparison.pair;
        assert_eq!(pair.gaps_between, 1);
        assert!(pair.coverage.feed_gap);
        // Every bar between the anchors lies in one swing's window, so that
        // swing is flagged too; the count alone flags a pair as well.
        assert!(pair.current.coverage.feed_gap);
        let clean = SwingAnchor {
            coverage: Coverage::default(),
            ..pair.previous
        };
        let counted = SwingAnchor {
            gaps: clean.gaps + 1,
            time: t(clean.time.as_millis() + 1),
            ..clean
        };
        let flagged = super::pair(Side::High, &clean, &counted).unwrap();
        assert_eq!((flagged.gaps_between, flagged.coverage.feed_gap), (1, true));
        let unflagged = super::pair(Side::High, &clean, &clean).unwrap();
        assert_eq!(
            (unflagged.gaps_between, unflagged.coverage.feed_gap),
            (0, false)
        );
    }

    #[test]
    fn a_swing_confirmed_inside_a_multi_bar_close_finds_its_anchor() {
        let scenario = Scenario {
            silent_right: true,
            ..Scenario::divergent(Side::High)
        };
        let mut engine = MarketStateEngine::new();
        let events = scenario.tape();
        let facts = run(&mut engine, &events);
        // The last trade closes bars 10 to 15 in one event.
        let closed: Vec<i64> = engine
            .closed_bars()
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M15)
            .map(|bar| bar.open_time.as_millis() / M15)
            .collect();
        assert_eq!(closed, [10, 11, 12, 13, 14, 15]);
        let fired = cvd_facts(&facts, Side::High);
        assert_eq!(fired.len(), 1, "{facts:?}");
        assert_eq!(
            fired[0].comparison.pair.current.time,
            t(Scenario::anchor_time(BAR_B))
        );
        assert_eq!(fired[0].known_at, events.last().unwrap().time());
    }

    #[test]
    fn the_context_lists_the_levels_at_the_swing_but_its_own() {
        let scenario = Scenario::divergent(Side::High);
        let levels_v1 = || {
            let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
            MarketStateEngine::with_features(features).unwrap()
        };
        for (mut engine, version) in [
            (MarketStateEngine::new(), catalog::LOCATION_LEVELS_V2.key),
            (levels_v1(), catalog::LOCATION_LEVELS_V1.key),
        ] {
            let facts = run(&mut engine, &scenario.tape());
            let fired = cvd_facts(&facts, Side::High);
            assert_eq!(fired.len(), 1);
            let context = &fired[0].context;
            let swing = fired[0].comparison.pair.current;
            assert_eq!(context.anchor, swing.price);
            let levels = context.levels.ready().unwrap();
            assert_eq!(levels.feature, version);
            // The registry holds B's own structural level at the swing price…
            let registry = engine.state().location.levels.ready().unwrap();
            let own = |kind: LevelKind, source: FeatureKey, anchor: Option<EventTime>| {
                kind == LevelKind::StructuralHigh
                    && source == catalog::STRUCTURE_LEVELS_15M_V1.key
                    && anchor == Some(swing.swing_time)
            };
            assert!(registry.levels().iter().any(|level| own(
                level.kind,
                level.source,
                level.anchor
            )));
            // …which the context leaves out, while it lists A's level, swept
            // one unit below, as a prior sweep.
            assert!(
                levels
                    .levels
                    .iter()
                    .all(|level| !own(level.kind, level.source, level.anchor))
            );
            assert!(levels.levels.iter().any(|level| {
                level.kind == LevelKind::PriorSweep
                    && level.price == Price::from_units(scenario.beyond(A))
            }));
            // Every listed level's padded zone contains the anchor.
            for level in &levels.levels {
                let pad = i128::from(level.price.units()).abs() * 6 / 10_000;
                let price = i128::from(swing.price.units());
                assert!(
                    i128::from(level.low.units()) - pad <= price
                        && price <= i128::from(level.high.units()) + pad,
                    "{level}"
                );
            }
            assert_eq!(context.auction, engine.state().location.auction);
        }
    }

    #[test]
    fn a_change_out_of_range_makes_the_value_unavailable() {
        let anchor = |price: i64, cvd: i64| SwingAnchor {
            swing_time: t(0),
            known_at: t(0),
            price: Price::from_units(price),
            time: t(0),
            cvd: Qty::from_units(cvd),
            gaps: 0,
            coverage: Coverage::default(),
        };
        assert!(pair(Side::High, &anchor(0, 0), &anchor(1, -1)).is_some());
        assert!(pair(Side::High, &anchor(-1, 0), &anchor(i64::MAX, 0)).is_none());
        assert!(pair(Side::High, &anchor(0, i64::MIN), &anchor(0, 1)).is_none());
        // A later anchor before the earlier one has no span.
        let mut later = anchor(1, 0);
        later.time = t(-1);
        assert!(pair(Side::High, &anchor(0, 0), &later).is_none());
        let mut tracker = DivergenceTracker::new(0);
        tracker.warm = true;
        tracker.cvd = [Slot::Empty, Slot::OutOfRange];
        assert_eq!(
            tracker.value(tracker.cvd_feature, &tracker.cvd),
            FeatureValue::Unavailable {
                reason: Unavailability::OutOfRange
            }
        );
        tracker.warm = false;
        assert_eq!(tracker.value(tracker.cvd_feature, &tracker.cvd), warming());
    }
}
