//! Order-flow candidates (brief §8; Market State & Regime brief; ADR-051,
//! proposed).
//!
//! A candidate is a Market State fact composed from features that already
//! exist: a hypothesis for Conditional Behavior and research, never a
//! signal (ADR-012, ADR-013). Candidates carry no bias, direction, location
//! gate or execution meaning; the trigger rule of each
//! [`TriggerFamily`](crate::trigger::TriggerFamily) that builds on one, with
//! its location gate, is ADR-053's (D4, D6). The only sided fields are
//! facts: which extreme ([`Side`](crate::structure::Side)), which taker side
//! carried the net aggression ([`Aggressor`](crate::event::Aggressor),
//! ADR-023) and which passive side.
//!
//! - `candidate.divergence.cvd.<tf>@1` and `candidate.divergence.oi.<tf>@1`
//!   ([`divergence`], decision 4): CVD and open-interest divergences on
//!   confirmed swings of every structure timeframe.
//! - `candidate.effort_result.<5m|15m|1h>@1` and `candidate.exhaustion@1`
//!   ([`effort`], decisions 5 and 6): aggression against displacement,
//!   judged against each window's own previous evaluations, and aggression
//!   decaying at an extreme after an expansion.
//! - `candidate.absorption.<5m|15m|1h>@1` ([`absorption`], decision 7, live
//!   only): effort vs result with the passive side replenished.
//!
//! Candidates are the last stage of an event (decision 2): they run after
//! the location stage, on the committed state, and cannot fail. A change
//! that does not fit its type makes that value `Unavailable(OutOfRange)`
//! and never rejects the event. Each emitted [`CandidateEvent`] carries its
//! inputs with their feature keys and a [`LocationContext`] (decision 3);
//! events are not hashed, the same as structure and location facts.

pub mod absorption;
pub mod divergence;
pub mod effort;

mod context;

pub(crate) use context::Exclude;
pub use context::{ContextLevel, ContextLevels, LocationContext};

use crate::bars::{Bar, Timeframe};
use crate::derivatives::Derivatives;
use crate::event::MarketEvent;
use crate::feature::{FeatureKey, FeatureValue, Input, catalog};
use crate::fingerprint::Fingerprinter;
use crate::flow::OrderFlow;
use crate::liquidity::BookState;
use crate::location::LocationState;
use crate::num::Price;
use crate::state_hash::StateEncode;
use crate::structure::{STRUCTURE_TIMEFRAMES, StructureEvent};
use crate::time::EventTime;
use absorption::{Absorption, AbsorptionJudge};
use divergence::{
    CvdComparison, DivergenceEvent, DivergenceTracker, Emitted, OiComparison, TimeframeDivergences,
};
use effort::{EffortJudge, EffortResult, Exhaustion, ExhaustionTracker};
use std::fmt;

/// The effort windows, shortest first, in
/// [`catalog::CANDIDATE_EFFORT_RESULT`] order.
const EFFORT_WINDOWS: [Timeframe; 3] = [Timeframe::M5, Timeframe::M15, Timeframe::H1];

/// The position of [`effort::DECAY_WINDOW`] in [`EFFORT_WINDOWS`].
const DECAY: usize = 0;

/// The position of [`effort::EXPANSION_WINDOW`] in [`EFFORT_WINDOWS`].
const EXPANSION: usize = 2;

/// The candidate features of the Market State (ADR-051 D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Candidates {
    /// The divergence features of every structure timeframe, shortest
    /// first.
    divergences: [TimeframeDivergences; 4],
    /// `candidate.effort_result.<tf>@1`, in [`EFFORT_WINDOWS`] order.
    effort: [FeatureValue<EffortResult>; 3],
    /// `candidate.exhaustion@1`.
    exhaustion: FeatureValue<Exhaustion>,
    /// `candidate.absorption.<tf>@1`, in [`EFFORT_WINDOWS`] order.
    absorption: [FeatureValue<Absorption>; 3],
}

impl StateEncode for Candidates {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            divergences,
            effort,
            exhaustion,
            absorption,
        } = self;
        divergences.encode(f);
        effort.encode(f);
        exhaustion.encode(f);
        absorption.encode(f);
    }
}

impl Default for Candidates {
    fn default() -> Self {
        Self::new()
    }
}

impl Candidates {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            divergences: STRUCTURE_TIMEFRAMES.map(|_| TimeframeDivergences::new()),
            effort: [0, 1, 2].map(|position| EffortJudge::new(position).initial()),
            exhaustion: ExhaustionTracker::initial(),
            absorption: [0, 1, 2].map(|_| AbsorptionJudge::initial()),
        }
    }

    /// The divergence features of `timeframe`, if it is a structure
    /// timeframe.
    pub fn divergences(&self, timeframe: Timeframe) -> Option<&TimeframeDivergences> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.divergences[index])
    }

    /// The divergence features of `timeframe`, for tests that build a
    /// state by hand.
    #[cfg(test)]
    pub(crate) fn divergences_mut(
        &mut self,
        timeframe: Timeframe,
    ) -> Option<&mut TimeframeDivergences> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &mut self.divergences[index])
    }

    /// The divergence features of every structure timeframe, shortest
    /// first.
    pub fn iter_divergences(&self) -> impl Iterator<Item = (Timeframe, &TimeframeDivergences)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.divergences)
    }

    /// `candidate.effort_result.<tf>@1` of the window of length
    /// `timeframe`, if it is an effort window.
    pub fn effort_result(&self, timeframe: Timeframe) -> Option<&FeatureValue<EffortResult>> {
        effort_position(timeframe).map(|index| &self.effort[index])
    }

    /// `candidate.exhaustion@1`.
    pub fn exhaustion(&self) -> &FeatureValue<Exhaustion> {
        &self.exhaustion
    }

    /// `candidate.absorption.<tf>@1` of the window of length `timeframe`,
    /// if it is an effort window.
    pub fn absorption(&self, timeframe: Timeframe) -> Option<&FeatureValue<Absorption>> {
        effort_position(timeframe).map(|index| &self.absorption[index])
    }

    /// The values of the effort windows, shortest first: effort vs result
    /// and absorption, with the window length.
    pub fn iter_windows(
        &self,
    ) -> impl Iterator<
        Item = (
            Timeframe,
            &FeatureValue<EffortResult>,
            &FeatureValue<Absorption>,
        ),
    > {
        EFFORT_WINDOWS
            .into_iter()
            .zip(&self.effort)
            .zip(&self.absorption)
            .map(|((timeframe, effort), absorption)| (timeframe, effort, absorption))
    }

    /// The effort-window values of `timeframe`, for tests that build a
    /// state by hand.
    #[cfg(test)]
    pub(crate) fn windows_mut(
        &mut self,
        timeframe: Timeframe,
    ) -> Option<(
        &mut FeatureValue<EffortResult>,
        &mut FeatureValue<Absorption>,
    )> {
        effort_position(timeframe)
            .map(|index| (&mut self.effort[index], &mut self.absorption[index]))
    }

    /// `candidate.exhaustion@1`, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn exhaustion_mut(&mut self) -> &mut FeatureValue<Exhaustion> {
        &mut self.exhaustion
    }
}

/// The position of the effort window of length `timeframe`.
fn effort_position(timeframe: Timeframe) -> Option<usize> {
    EFFORT_WINDOWS
        .iter()
        .position(|candidate| *candidate == timeframe)
}

/// The rising edge of an evaluated candidate (ADR-051 D5–D7): the value of
/// the minute whose evaluation turned the condition true.
///
/// `Display` prints the value, which ends with its feature key, then the
/// location context.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EdgeEvent<V> {
    /// The feature that produced it.
    pub feature: FeatureKey,
    /// Time of the event that closed the minute.
    pub known_at: EventTime,
    /// The feature's inputs, as its definition declares them.
    pub inputs: &'static [Input],
    /// The evaluation.
    pub value: V,
    /// Where the last close sits (ADR-051 D3).
    pub context: LocationContext,
}

impl<V: fmt::Display> fmt::Display for EdgeEvent<V> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {}", self.value, self.context)
    }
}

/// A candidate fact, in emission order (ADR-051 D2): the divergences per
/// closed bar in close order, per swing it confirmed, the CVD comparison
/// before the OI one; then, at an event that closes a 1m bar, effort vs
/// result per window shortest first, exhaustion, and absorption per window
/// shortest first.
///
/// Not hashed. `Display` prints the canonical line the golden tests pin:
/// the kind, then the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CandidateEvent {
    /// A CVD divergence: `candidate.divergence.cvd.<tf>@1`.
    CvdDivergence(DivergenceEvent<CvdComparison>),
    /// An open-interest divergence: `candidate.divergence.oi.<tf>@1`.
    OiDivergence(DivergenceEvent<OiComparison>),
    /// Aggression in the tail with little progress:
    /// `candidate.effort_result.<tf>@1`.
    EffortVsResult(EdgeEvent<EffortResult>),
    /// Aggression decaying at an extreme after an expansion:
    /// `candidate.exhaustion@1`.
    Exhaustion(EdgeEvent<Exhaustion>),
    /// Effort vs result into replenished passive liquidity:
    /// `candidate.absorption.<tf>@1`.
    Absorption(EdgeEvent<Absorption>),
}

impl CandidateEvent {
    /// When the fact became visible: the time of the event that emitted it.
    pub fn time(&self) -> EventTime {
        match self {
            Self::CvdDivergence(event) => event.known_at,
            Self::OiDivergence(event) => event.known_at,
            Self::EffortVsResult(event) => event.known_at,
            Self::Exhaustion(event) => event.known_at,
            Self::Absorption(event) => event.known_at,
        }
    }

    /// The feature that produced the fact.
    pub fn feature(&self) -> crate::feature::FeatureKey {
        match self {
            Self::CvdDivergence(event) => event.feature,
            Self::OiDivergence(event) => event.feature,
            Self::EffortVsResult(event) => event.feature,
            Self::Exhaustion(event) => event.feature,
            Self::Absorption(event) => event.feature,
        }
    }

    /// Where the fact happened.
    pub fn context(&self) -> &LocationContext {
        match self {
            Self::CvdDivergence(event) => &event.context,
            Self::OiDivergence(event) => &event.context,
            Self::EffortVsResult(event) => &event.context,
            Self::Exhaustion(event) => &event.context,
            Self::Absorption(event) => &event.context,
        }
    }
}

impl fmt::Display for CandidateEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CvdDivergence(event) => write!(f, "cvd_divergence {event}"),
            Self::OiDivergence(event) => write!(f, "oi_divergence {event}"),
            Self::EffortVsResult(event) => write!(f, "effort_result {event}"),
            Self::Exhaustion(event) => write!(f, "exhaustion {event}"),
            Self::Absorption(event) => write!(f, "absorption {event}"),
        }
    }
}

/// The committed state the candidate stage reads.
pub(crate) struct Inputs<'a> {
    /// The order flow: `flow.cvd.continuous@1`.
    pub(crate) flow: &'a OrderFlow,
    /// The derivatives context: `derivatives.oi.5m@1`.
    pub(crate) derivatives: &'a Derivatives,
    /// Location, for the context of each event (decision 3).
    pub(crate) location: &'a LocationState,
    /// The order book: `book.liquidity.window.<tf>@1`.
    pub(crate) book: &'a BookState,
}

/// The candidate engine state (ADR-051 D2). Engine state, updated in place;
/// the Market State exposes only [`Candidates`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CandidateTracker {
    /// One per structure timeframe, shortest first.
    divergences: [DivergenceTracker; 4],
    /// Scratch buffer for one swing's comparisons.
    emitted: Vec<Emitted>,
    /// Effort vs result per window, in [`EFFORT_WINDOWS`] order.
    effort: [EffortJudge; 3],
    /// Exhaustion.
    exhaustion: ExhaustionTracker,
    /// Absorption per window, in [`EFFORT_WINDOWS`] order.
    absorption: [AbsorptionJudge; 3],
    /// The close of the last closed 1m bar with trades: the anchor of every
    /// effort-vs-result, exhaustion and absorption context.
    last_close: Option<Price>,
    /// Scratch buffer for the baseline quantiles.
    scratch: Vec<i64>,
}

impl Default for CandidateTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl CandidateTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            divergences: [0, 1, 2, 3].map(DivergenceTracker::new),
            emitted: Vec::new(),
            effort: [0, 1, 2].map(EffortJudge::new),
            exhaustion: ExhaustionTracker::new(),
            absorption: [0, 1, 2].map(AbsorptionJudge::new),
            last_close: None,
            scratch: Vec::with_capacity(effort::BASELINE_EVALUATIONS),
        }
    }

    /// The infallible stage (decision 2): after every family and location
    /// have committed `event`, walks the bars it closed in close order,
    /// handling after each one the swings it confirmed, then evaluates
    /// effort vs result, exhaustion and absorption if it closed a 1m bar,
    /// then records the event's trade at the developing bars' extremes.
    /// Updates `candidates` and appends the facts to `events`.
    pub(crate) fn detect(
        &mut self,
        event: &MarketEvent,
        closed: &[Bar],
        structure_events: &[StructureEvent],
        inputs: Inputs<'_>,
        candidates: &mut Candidates,
        events: &mut Vec<CandidateEvent>,
    ) {
        for bar in closed {
            let Some(position) = self
                .divergences
                .iter()
                .position(|tracker| tracker.timeframe() == bar.timeframe)
            else {
                continue;
            };
            let tracker = &mut self.divergences[position];
            tracker.close(bar);
            for fact in structure_events {
                let StructureEvent::Swing(swing) = fact else {
                    continue;
                };
                if swing.timeframe != bar.timeframe || swing.confirmed_bar_end != bar.end() {
                    continue;
                }
                tracker.confirm(
                    swing,
                    &inputs,
                    &mut candidates.divergences[position],
                    &mut self.emitted,
                );
                events.extend(self.emitted.drain(..).map(|emitted| match emitted {
                    Emitted::Cvd(event) => CandidateEvent::CvdDivergence(event),
                    Emitted::Oi(event) => CandidateEvent::OiDivergence(event),
                }));
            }
        }
        self.minute(event.time(), closed, &inputs, candidates, events);
        if let MarketEvent::Trade(trade) = event
            && let Some(cvd) = inputs.flow.cvd.ready()
        {
            let oi = inputs.derivatives.oi_5m.ready();
            for tracker in &mut self.divergences {
                tracker.trade(trade, cvd, oi);
            }
        }
    }

    /// Effort vs result, exhaustion and absorption (decisions 5–7), once per
    /// event that closes at least one 1m bar, on the windows after it.
    fn minute(
        &mut self,
        known_at: EventTime,
        closed: &[Bar],
        inputs: &Inputs<'_>,
        candidates: &mut Candidates,
        events: &mut Vec<CandidateEvent>,
    ) {
        let mut minute = false;
        for bar in closed.iter().filter(|bar| bar.timeframe == Timeframe::M1) {
            minute = true;
            if let Some(ohlc) = bar.ohlc {
                self.last_close = Some(ohlc.close);
                self.exhaustion.minute(bar.end(), ohlc.high, ohlc.low);
            }
        }
        if !minute {
            return;
        }
        // A judged evaluation has a displacement, so a trade before its
        // window and a closed 1m bar with trades: the anchor is there.
        let context = |anchor: Option<Price>| {
            anchor.map(|anchor| LocationContext::read(anchor, None, inputs.location))
        };
        let mut judged: [Option<EffortResult>; 3] = [None; 3];
        for (position, judge) in self.effort.iter_mut().enumerate() {
            let Some(window) = inputs.flow.windows.get(EFFORT_WINDOWS[position]) else {
                continue;
            };
            let Some((result, edge)) =
                judge.evaluate(window, &mut candidates.effort[position], &mut self.scratch)
            else {
                continue;
            };
            judged[position] = Some(result);
            if edge && let Some(context) = context(self.last_close) {
                let (_, definition) = catalog::CANDIDATE_EFFORT_RESULT[position];
                events.push(CandidateEvent::EffortVsResult(EdgeEvent {
                    feature: definition.key,
                    known_at,
                    inputs: definition.inputs,
                    value: result,
                    context,
                }));
            }
        }
        if let Some((exhaustion, edge)) = self.exhaustion.step(
            judged[EXPANSION].as_ref(),
            judged[DECAY].as_ref(),
            self.last_close,
            &mut candidates.exhaustion,
        ) && edge
            && let Some(context) = context(self.last_close)
        {
            let definition = &catalog::CANDIDATE_EXHAUSTION_V1;
            events.push(CandidateEvent::Exhaustion(EdgeEvent {
                feature: definition.key,
                known_at,
                inputs: definition.inputs,
                value: exhaustion,
                context,
            }));
        }
        for (position, judge) in self.absorption.iter_mut().enumerate() {
            let Some(book) = inputs.book.windows.get(EFFORT_WINDOWS[position]) else {
                continue;
            };
            if let Some((absorption, edge)) = judge.step(
                judged[position].as_ref(),
                book,
                &mut candidates.absorption[position],
            ) && edge
                && let Some(context) = context(self.last_close)
            {
                let (_, definition) = catalog::CANDIDATE_ABSORPTION[position];
                events.push(CandidateEvent::Absorption(EdgeEvent {
                    feature: definition.key,
                    known_at,
                    inputs: definition.inputs,
                    value: absorption,
                    context,
                }));
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::bars::tests::Lcg;
    use crate::event::samples::t;
    use crate::event::{
        Aggressor, BookSnapshot, BookUpdate, FeedGap, GapReason, Level, OpenInterest, Stream, Trade,
    };
    use crate::feature::{FeatureDefinition, FeatureKey, FeatureValue, catalog};
    use crate::fingerprint::Fingerprinter;
    use crate::num::{Price, Qty, SCALE};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;

    /// The candidate golden tape: 13:00 UTC on day 0 to 00:00 UTC on day
    /// 45. A random walk in cents with drifting trends and trades 20 s to
    /// 160 s apart, as the structure golden tape; open interest on every
    /// 5-minute boundary, a random walk of its own; a trades gap on day 9
    /// and no open interest for two hours of day 20. Also the trigger
    /// golden tape (ADR-053).
    pub(crate) fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0022);
        let mut events = Vec::new();
        let mut time = 13 * 3_600_000;
        let mut cents: i64 = 62_000 * 100;
        let mut trend = 0;
        let mut trade_id = 0;
        let mut gapped = false;
        let mut boundary = (time / 300_000 + 1) * 300_000;
        let mut oi: i64 = 80_000 * SCALE;
        let gap_start = 9 * DAY + 10 * 3_600_000;
        let gap_end = gap_start + 45 * 60_000;
        let oi_hole = (20 * DAY + 3_600_000)..(20 * DAY + 3 * 3_600_000);
        while time < 45 * DAY {
            time += 20_000 + lcg.below(140_000);
            while boundary < time {
                oi += (lcg.below(2_001) - 1_000) * 10_000_000;
                if !oi_hole.contains(&boundary) {
                    events.push(MarketEvent::OpenInterest(OpenInterest {
                        time: t(boundary),
                        open_interest: Qty::from_units(oi),
                        resolution_ms: 300_000,
                    }));
                }
                boundary += 300_000;
            }
            if !gapped && time >= gap_start {
                events.push(MarketEvent::FeedGap(FeedGap {
                    stream: Stream::Trades,
                    start: t(gap_start),
                    end: t(gap_end),
                    reason: GapReason::Disconnected,
                }));
                time = gap_end + 1 + lcg.below(30_000);
                boundary = (time / 300_000 + 1) * 300_000;
                gapped = true;
            }
            if lcg.below(400) == 0 {
                trend = lcg.below(7) - 3;
            }
            cents += trend + lcg.below(1_001) - 500;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(time),
                trade_id,
                price: Price::from_units(cents * (SCALE / 100)),
                qty: Qty::from_units(1 + lcg.below(200_000_000)),
                aggressor: if lcg.below(2) == 0 {
                    Aggressor::Buy
                } else {
                    Aggressor::Sell
                },
            }));
        }
        events
    }

    /// The absorption golden tape: 27 hours from 00:00 UTC on day 1 of a
    /// book 0.1 USDT wide around 60 000 USDT, ten levels a side 4 USDT
    /// apart (so the trusted window reaches beyond the 5 bps band), and one
    /// to four trades a minute that lift the best ask or hit the best bid.
    /// One book update per minute, at second 55, shows each best level's
    /// fills and a refill of 0, ½, 1 or 1½ times them; a level that falls
    /// below 30 BTC is topped up to 50.
    pub(crate) fn book_tape() -> Vec<MarketEvent> {
        const T0: i64 = DAY;
        const MINUTES: i64 = 27 * 60;
        let mut lcg = Lcg(0x6d69_6500_0022_0003);
        let bid = 60_000 * SCALE;
        let ask = bid + SCALE / 10;
        let side_levels = |best: i64, step: i64| -> Vec<Level> {
            (0..10)
                .map(|k| Level {
                    price: Price::from_units(best + k * step),
                    qty: Qty::from_units(50 * SCALE),
                })
                .collect()
        };
        let mut last_id = 1_000;
        let mut events = vec![MarketEvent::BookSnapshot(BookSnapshot {
            time: t(T0),
            last_update_id: last_id,
            bids: side_levels(bid, -4 * SCALE),
            asks: side_levels(ask, 4 * SCALE),
        })];
        // The resting quantity at the best bid and the best ask.
        let mut resting = [50 * SCALE; 2];
        let mut trade_id = 0;
        for minute in 0..MINUTES {
            let open = T0 + minute * 60_000;
            let mut filled = [0; 2];
            for slot in 0..1 + lcg.below(4) {
                let buy = lcg.below(2) == 0;
                let side = usize::from(buy);
                let mut size = (1 + lcg.below(100)) * SCALE / 50;
                if lcg.below(10) == 0 {
                    size *= 10;
                }
                let size = size.min(resting[side] - filled[side] - SCALE);
                if size <= 0 {
                    continue;
                }
                filled[side] += size;
                trade_id += 1;
                events.push(MarketEvent::Trade(Trade {
                    time: t(open + 1_000 + slot * 10_000 + lcg.below(9_000)),
                    trade_id,
                    price: Price::from_units(if buy { ask } else { bid }),
                    qty: Qty::from_units(size),
                    aggressor: if buy { Aggressor::Buy } else { Aggressor::Sell },
                }));
            }
            let mut changed = [Vec::new(), Vec::new()];
            for (side, price) in [bid, ask].into_iter().enumerate() {
                let refill = filled[side] * lcg.below(4) / 2;
                let mut next = resting[side] - filled[side] + refill;
                if next < 30 * SCALE {
                    next = 50 * SCALE;
                }
                if next != resting[side] {
                    changed[side].push(Level {
                        price: Price::from_units(price),
                        qty: Qty::from_units(next),
                    });
                }
                resting[side] = next;
            }
            // The first update straddles the snapshot's id; the rest chain.
            let (first, previous) = if minute == 0 {
                (last_id - 5, 0)
            } else {
                (last_id + 1, last_id)
            };
            last_id += 5;
            let [bids, asks] = changed;
            events.push(MarketEvent::BookUpdate(BookUpdate {
                time: t(open + 55_000),
                first_update_id: first,
                last_update_id: last_id,
                prev_update_id: previous,
                bids,
                asks,
            }));
        }
        events.push(MarketEvent::Trade(Trade {
            time: t(T0 + MINUTES * 60_000 + 1_000),
            trade_id: trade_id + 1,
            price: Price::from_units(bid),
            qty: Qty::from_units(SCALE),
            aggressor: Aggressor::Sell,
        }));
        events.sort();
        events
    }

    /// Whether a change from `old` to `new` gets a golden value line: every
    /// change but a ready value followed by a ready one whose condition
    /// (`active`) is unchanged. The evaluated candidates change at every
    /// minute; their facts pin the values that turn a condition true.
    fn transition<T>(old: &FeatureValue<T>, new: &FeatureValue<T>, active: fn(&T) -> bool) -> bool
    where
        T: PartialEq,
    {
        match (old, new) {
            (FeatureValue::Ready(old), FeatureValue::Ready(new)) => active(old) != active(new),
            _ => old != new,
        }
    }

    /// The golden lines of every candidate feature on the golden tape, in
    /// tape order: `<event time> value <value>` whenever a divergence value
    /// changes or an evaluated candidate's value makes a [`transition`],
    /// and `<event time> <fact>` for each fact it emits. Replayed once and
    /// shared by the golden tests of every id (a test-only cache: the tape
    /// takes seconds to replay in a debug build).
    fn golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| replay_golden(golden_tape()))
    }

    /// The golden lines of the book tape ([`book_tape`]), as
    /// [`golden_lines`]: the absorption goldens.
    fn book_golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| replay_golden(book_tape()))
    }

    /// Replays `tape` into its golden lines ([`golden_lines`]).
    fn replay_golden(tape: Vec<MarketEvent>) -> Vec<(FeatureKey, String)> {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        let mut before = engine.state().candidates.clone();
        for event in tape {
            engine.apply(&event).unwrap();
            let time = event.time();
            let after = &engine.state().candidates;
            for ((timeframe, old), (_, new)) in
                before.iter_divergences().zip(after.iter_divergences())
            {
                let (_, cvd) = catalog::CANDIDATE_DIVERGENCE_CVD
                    .into_iter()
                    .find(|(candidate, _)| *candidate == timeframe)
                    .unwrap();
                let (_, oi) = catalog::CANDIDATE_DIVERGENCE_OI
                    .into_iter()
                    .find(|(candidate, _)| *candidate == timeframe)
                    .unwrap();
                if old.cvd != new.cvd {
                    lines.push((cvd.key, format!("{time} value {}", shown(&new.cvd))));
                }
                if old.oi != new.oi {
                    lines.push((oi.key, format!("{time} value {}", shown(&new.oi))));
                }
            }
            for (position, ((_, old_effort, old_absorption), (_, new_effort, new_absorption))) in
                before.iter_windows().zip(after.iter_windows()).enumerate()
            {
                let effort = catalog::CANDIDATE_EFFORT_RESULT[position].1.key;
                let absorption = catalog::CANDIDATE_ABSORPTION[position].1.key;
                if transition(old_effort, new_effort, |value| value.active) {
                    lines.push((effort, format!("{time} value {}", shown(new_effort))));
                }
                if transition(old_absorption, new_absorption, |value| value.active) {
                    lines.push((
                        absorption,
                        format!("{time} value {}", shown(new_absorption)),
                    ));
                }
            }
            if transition(before.exhaustion(), after.exhaustion(), |value| {
                value.active
            }) {
                lines.push((
                    catalog::CANDIDATE_EXHAUSTION_V1.key,
                    format!("{time} value {}", shown(after.exhaustion())),
                ));
            }
            for fact in engine.candidate_events() {
                lines.push((fact.feature(), format!("{time} {fact}")));
            }
            before = after.clone();
        }
        lines
    }

    /// A value as a golden line shows it.
    fn shown<T: fmt::Display>(value: &FeatureValue<T>) -> String {
        match value {
            FeatureValue::Ready(value) => value.to_string(),
            FeatureValue::WarmingUp { observed, required } => {
                format!("warming {observed}/{required}")
            }
            FeatureValue::Unavailable { reason } => format!("unavailable {reason:?}"),
        }
    }

    /// The digest of `lines`.
    fn digest(lines: &[&str]) -> String {
        let mut hasher = Fingerprinter::new();
        hasher.write_len(lines.len());
        for line in lines {
            hasher.write_str(line);
        }
        hasher.finish().to_string()
    }

    /// Checks the golden of `feature`: the count of its lines, its first
    /// fact line verbatim (or its first value line, without facts), and
    /// the digest of all of them.
    fn assert_golden(feature: &FeatureDefinition, count: usize, first: &str, all: &str) {
        assert_golden_of(golden_lines(), feature, count, first, all);
    }

    /// [`assert_golden`] over `lines`.
    fn assert_golden_of(
        lines: &[(FeatureKey, String)],
        feature: &FeatureDefinition,
        count: usize,
        first: &str,
        all: &str,
    ) {
        let own: Vec<&str> = lines
            .iter()
            .filter(|(key, _)| *key == feature.key)
            .map(|(_, line)| line.as_str())
            .collect();
        let shown = own
            .iter()
            .find(|line| !line.contains(" value "))
            .or(own.first())
            .copied()
            .unwrap_or_default();
        assert_eq!(shown, first, "{}", feature.key);
        assert_eq!(
            (own.len(), digest(&own).as_str()),
            (count, all),
            "{}",
            feature.key
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_15m_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1,
            961,
            GOLDEN_CVD_15M,
            "4f4d7236e5ef3d6e",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_1h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_1H_V1,
            233,
            GOLDEN_CVD_1H,
            "8f8c74676e526e86",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_4h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_4H_V1,
            53,
            GOLDEN_CVD_4H,
            "4fc7cb36282c4d69",
        );
    }

    #[test]
    fn golden_candidate_divergence_cvd_1d_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_CVD_1D_V1,
            4,
            GOLDEN_CVD_1D,
            "9284f60919fa13b1",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_15m_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_15M_V1,
            984,
            GOLDEN_OI_15M,
            "cbf50ccc3590c65c",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_1h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_1H_V1,
            234,
            GOLDEN_OI_1H,
            "206e41e0e7be6838",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_4h_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_4H_V1,
            53,
            GOLDEN_OI_4H,
            "8c76cfff3a887542",
        );
    }

    #[test]
    fn golden_candidate_divergence_oi_1d_v1() {
        assert_golden(
            &catalog::CANDIDATE_DIVERGENCE_OI_1D_V1,
            4,
            GOLDEN_OI_1D,
            "520811c85c537e3e",
        );
    }

    #[test]
    fn golden_candidate_effort_result_5m_v1() {
        assert_golden(
            &catalog::CANDIDATE_EFFORT_RESULT_5M_V1,
            11927,
            GOLDEN_EFFORT_5M,
            "959f667a184dc3b0",
        );
    }

    #[test]
    fn golden_candidate_effort_result_15m_v1() {
        assert_golden(
            &catalog::CANDIDATE_EFFORT_RESULT_15M_V1,
            7898,
            GOLDEN_EFFORT_15M,
            "d205fea3f43f8767",
        );
    }

    #[test]
    fn golden_candidate_effort_result_1h_v1() {
        assert_golden(
            &catalog::CANDIDATE_EFFORT_RESULT_1H_V1,
            4914,
            GOLDEN_EFFORT_1H,
            "83cf6588cc393c1b",
        );
    }

    #[test]
    fn golden_candidate_exhaustion_v1() {
        assert_golden(
            &catalog::CANDIDATE_EXHAUSTION_V1,
            471,
            GOLDEN_EXHAUSTION,
            "aad12e26550e3e85",
        );
    }

    #[test]
    fn golden_candidate_absorption_5m_v1() {
        assert_golden_of(
            book_golden_lines(),
            &catalog::CANDIDATE_ABSORPTION_5M_V1,
            16,
            GOLDEN_ABSORPTION_5M,
            "7e558cafad7c4aec",
        );
    }

    #[test]
    fn golden_candidate_absorption_15m_v1() {
        assert_golden_of(
            book_golden_lines(),
            &catalog::CANDIDATE_ABSORPTION_15M_V1,
            22,
            GOLDEN_ABSORPTION_15M,
            "6f71090d41b3e227",
        );
    }

    #[test]
    fn golden_candidate_absorption_1h_v1() {
        assert_golden_of(
            book_golden_lines(),
            &catalog::CANDIDATE_ABSORPTION_1H_V1,
            13,
            GOLDEN_ABSORPTION_1H,
            "006df9f69dbbc08a",
        );
    }

    /// The facts of every kind among `lines`, as `(kind, fact)`.
    fn facts_of_kind<'a>(lines: &'a [(FeatureKey, String)], kind: &str) -> Vec<&'a str> {
        let prefix = format!(" {kind} ");
        lines
            .iter()
            .map(|(_, line)| line.as_str())
            .filter(|line| line.contains(&prefix))
            .collect()
    }

    #[test]
    fn every_evaluated_kind_fires_on_its_golden_tape() {
        // The goldens are not vacuous: each evaluated kind emits, and each
        // fact is a rising edge of a true condition.
        for (lines, kind) in [
            (golden_lines(), "effort_result"),
            (golden_lines(), "exhaustion"),
            (book_golden_lines(), "absorption"),
        ] {
            let facts = facts_of_kind(lines, kind);
            assert!(!facts.is_empty(), "{kind}");
            for fact in facts {
                assert!(fact.contains(" active=true "), "{fact}");
            }
        }
        // The golden tape has no book: absorption stays warming up there.
        assert!(facts_of_kind(golden_lines(), "absorption").is_empty());
    }

    #[test]
    fn absorption_reads_the_passive_side_opposite_the_aggressor() {
        let mut engine = MarketStateEngine::new();
        let mut absorptions = Vec::new();
        for event in book_tape() {
            engine.apply(&event).unwrap();
            for fact in engine.candidate_events() {
                if let CandidateEvent::Absorption(event) = fact {
                    absorptions.push(event.clone());
                }
            }
        }
        assert!(!absorptions.is_empty());
        for event in &absorptions {
            let absorption = event.value;
            assert!(absorption.active && absorption.replenished && absorption.effort.active);
            assert!(absorption.added >= absorption.filled);
            assert_eq!(
                absorption.passive,
                match absorption.effort.aggressor {
                    Aggressor::Buy => crate::book::Side::Ask,
                    Aggressor::Sell => crate::book::Side::Bid,
                }
            );
            assert_eq!(absorption.band, absorption::BAND);
            // Emitted by the trade that closed the window's last minute.
            assert!(event.known_at >= absorption.effort.end);
            let (_, definition) = catalog::CANDIDATE_ABSORPTION
                .into_iter()
                .find(|(_, definition)| definition.key == event.feature)
                .unwrap();
            assert_eq!(event.inputs, definition.inputs);
            // The context is read at the last close: the bid or the ask.
            let anchor = event.context.anchor.units();
            assert!(
                [60_000 * SCALE, 60_000 * SCALE + SCALE / 10].contains(&anchor),
                "{anchor}"
            );
        }
        // Every window fires at least once, so each id has a fact.
        for (_, definition) in catalog::CANDIDATE_ABSORPTION {
            assert!(
                absorptions
                    .iter()
                    .any(|event| event.feature == definition.key),
                "{}",
                definition.key
            );
        }
    }

    #[test]
    fn a_depth_outage_is_no_absorption() {
        // The book tape loses its depth stream 25 hours in, after every
        // window has warmed up, and the book stays unavailable; trades go
        // on. The liquidity windows stay ready over minutes without book
        // updates (flagged `feed_gap` once the gap is known), and no fill
        // is booked there: windows over the outage read `added = filled =
        // 0`, which `added ≥ filled` alone counts as replenished. ADR-051
        // D7: no evaluation without fills.
        let outage = DAY + 25 * 3_600_000;
        let mut events: Vec<MarketEvent> = book_tape()
            .into_iter()
            .filter(|event| {
                !matches!(event, MarketEvent::BookUpdate(update) if update.time.as_millis() >= outage)
            })
            .collect();
        events.push(crate::event::samples::gap(
            Stream::OrderBook,
            outage,
            outage + 20 * 60_000,
            GapReason::Disconnected,
        ));
        events.sort();
        let mut engine = MarketStateEngine::new();
        let (mut unfilled, mut evaluated) = (0, 0);
        for event in events {
            let before = engine.state().candidates.clone();
            engine.apply(&event).unwrap();
            let state = engine.state();
            for ((timeframe, old_effort, old_absorption), (_, new_effort, new_absorption)) in
                before.iter_windows().zip(state.candidates.iter_windows())
            {
                if let FeatureValue::Ready(absorption) = new_absorption
                    && new_absorption != old_absorption
                {
                    assert!(absorption.filled.units() > 0, "{absorption}");
                    evaluated += 1;
                }
                // A judged minute whose book window of the same minute has
                // no fills on the passive side: the value stays.
                let FeatureValue::Ready(effort) = new_effort else {
                    continue;
                };
                let Some(FeatureValue::Ready(window)) = state.book.windows.get(timeframe) else {
                    continue;
                };
                let passive = match effort.aggressor {
                    Aggressor::Buy => crate::book::Side::Ask,
                    Aggressor::Sell => crate::book::Side::Bid,
                };
                let band = window.band(absorption::BAND).unwrap();
                if new_effort != old_effort
                    && window.end == effort.end
                    && !band.beyond_window
                    && band.side(passive).filled.units() == 0
                {
                    assert_eq!(new_absorption, old_absorption);
                    unfilled += 1;
                }
            }
            for fact in engine.candidate_events() {
                if let CandidateEvent::Absorption(fact) = fact {
                    assert!(fact.value.filled.units() > 0, "{fact}");
                }
            }
        }
        // The outage produced judged minutes without fills, and absorption
        // was still evaluated where fills were observed.
        assert!(unfilled > 0 && evaluated > 0, "{unfilled} {evaluated}");
    }

    #[test]
    fn a_tape_without_a_book_keeps_absorption_warming_up() {
        let mut engine = MarketStateEngine::new();
        for event in crate::bars::tests::random_tape(0x6d69_6500_0022_0004, 20_000) {
            let _ = engine.apply(&event);
        }
        for (_, _, absorption) in engine.state().candidates.iter_windows() {
            assert_eq!(
                *absorption,
                FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1
                }
            );
        }
    }

    #[test]
    fn a_multi_minute_close_is_one_evaluation() {
        // One buy a minute for minutes 0–19, then none until minute 26: the
        // trade at minute 26 closes minutes 19–25 in one event. The 15m
        // window is first evaluated at the close of minute 15 (its
        // reference is the trade of minute 0): four evaluations by the
        // close of minute 18, then one for the seven minutes closed at once.
        let mut engine = MarketStateEngine::new();
        let trade = |minute: i64, id: u64| {
            MarketEvent::Trade(Trade {
                time: t(DAY + minute * 60_000 + 30_000),
                trade_id: id,
                price: Price::from_units((60_000 + minute) * SCALE),
                qty: Qty::from_units(SCALE),
                aggressor: Aggressor::Buy,
            })
        };
        for minute in 0..20 {
            engine.apply(&trade(minute, minute as u64 + 1)).unwrap();
        }
        let observed = |engine: &MarketStateEngine| match engine
            .state()
            .candidates
            .effort_result(crate::bars::Timeframe::M15)
        {
            Some(FeatureValue::WarmingUp { observed, .. }) => *observed,
            other => panic!("{other:?}"),
        };
        // Four evaluations: three earlier ones behind the latest.
        assert_eq!(observed(&engine), 3);
        engine.apply(&trade(26, 100)).unwrap();
        let closed = engine
            .closed_bars()
            .iter()
            .filter(|bar| bar.timeframe == crate::bars::Timeframe::M1)
            .count();
        assert_eq!(closed, 7);
        assert_eq!(observed(&engine), 4);
    }

    #[test]
    fn the_same_tape_gives_the_same_facts_and_hashes() {
        let run = |events: &[MarketEvent]| {
            let mut engine = MarketStateEngine::new();
            let mut facts = Vec::new();
            let mut hashes = Vec::new();
            for event in events {
                engine.apply(event).unwrap();
                facts.extend(engine.candidate_events().iter().map(ToString::to_string));
                hashes.push(engine.state().state_hash());
            }
            (facts, hashes)
        };
        let tape = crate::bars::tests::random_tape(0x6d69_6500_0000_0022, 20_000);
        let (facts, hashes) = run(&tape);
        assert!(!facts.is_empty());
        assert_eq!(run(&tape), (facts, hashes));
    }

    #[test]
    fn a_rejected_event_leaves_the_candidates_and_their_facts() {
        use crate::candidate::divergence::tests::Scenario;
        use crate::state::StateError;
        let events = Scenario::divergent(crate::structure::Side::High).tape();
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        // The confirming trade emitted the divergence.
        let facts = engine.candidate_events().to_vec();
        assert!(!facts.is_empty());
        let candidates = engine.state().candidates.clone();
        // A trade whose volume overflows the developing bars.
        let last = events.last().unwrap().time().as_millis();
        let overflow = MarketEvent::Trade(Trade {
            time: t(last + 1),
            trade_id: 1_000,
            price: Price::from_units(60_000 * SCALE),
            qty: Qty::from_units(i64::MAX),
            aggressor: Aggressor::Buy,
        });
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.candidate_events(), facts.as_slice());
        assert_eq!(engine.state().candidates, candidates);
    }

    #[test]
    fn no_line_names_a_direction_or_an_execution_meaning() {
        // Candidates are facts (ADR-012, ADR-023, ADR-051): no fact or
        // value line may read as a direction or a trade instruction.
        let forbidden = [
            "bull",
            "bear",
            "long",
            "short",
            "signal",
            "reversal",
            "continuation",
        ];
        let mut lines: Vec<&str> = golden_lines()
            .iter()
            .chain(book_golden_lines())
            .map(|(_, line)| line.as_str())
            .collect();
        let samples: Vec<String> = [crate::structure::Side::High, crate::structure::Side::Low]
            .into_iter()
            .flat_map(|side| {
                let events = crate::candidate::divergence::tests::Scenario::divergent(side).tape();
                let mut engine = MarketStateEngine::new();
                crate::candidate::divergence::tests::run(&mut engine, &events)
                    .into_iter()
                    .map(|(_, fact)| fact.to_string())
            })
            .collect();
        assert!(!samples.is_empty());
        lines.extend(samples.iter().map(String::as_str));
        for kind in [
            "cvd_divergence",
            "oi_divergence",
            "effort_result",
            "exhaustion",
            "absorption",
        ] {
            assert!(lines.iter().any(|line| line.contains(kind)), "{kind}");
        }
        for line in lines {
            let lower = line.to_lowercase();
            for word in forbidden {
                assert!(!lower.contains(word), "{word}: {line}");
            }
        }
    }

    #[test]
    fn the_location_levels_v1_engine_still_builds() {
        // ADR-051 D3: no candidate declares a location input, so the
        // `location.levels@1` selection keeps its closure (ADR-044 D10).
        for definition in catalog::CANDIDATE_DIVERGENCE_CVD
            .into_iter()
            .chain(catalog::CANDIDATE_DIVERGENCE_OI)
            .chain(catalog::CANDIDATE_EFFORT_RESULT)
            .chain(catalog::CANDIDATE_ABSORPTION)
            .map(|(_, definition)| definition)
            .chain([&catalog::CANDIDATE_EXHAUSTION_V1])
        {
            assert!(
                definition
                    .inputs
                    .iter()
                    .all(|input| !matches!(input, crate::feature::Input::Feature(key) if key.id.as_str().starts_with("location."))),
                "{}",
                definition.key
            );
        }
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        assert!(
            features
                .definitions()
                .any(|definition| definition.key == catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1.key)
        );
        let engine = MarketStateEngine::with_features(features.clone()).unwrap();
        assert_eq!(engine.state().feature_set, features.version());
    }

    #[test]
    #[ignore = "prints the golden lines: cargo test -p mie-domain print_candidate_golden -- --ignored --nocapture"]
    fn print_candidate_golden() {
        let tape = catalog::CANDIDATE_DIVERGENCE_CVD
            .into_iter()
            .chain(catalog::CANDIDATE_DIVERGENCE_OI)
            .chain(catalog::CANDIDATE_EFFORT_RESULT)
            .map(|(_, definition)| (definition, golden_lines()))
            .chain([(&catalog::CANDIDATE_EXHAUSTION_V1, golden_lines())]);
        let book = catalog::CANDIDATE_ABSORPTION
            .into_iter()
            .map(|(_, definition)| (definition, book_golden_lines()));
        for (definition, lines) in tape.chain(book) {
            let own: Vec<&str> = lines
                .iter()
                .filter(|(key, _)| *key == definition.key)
                .map(|(_, line)| line.as_str())
                .collect();
            let first = own
                .iter()
                .find(|line| !line.contains(" value "))
                .or(own.first())
                .copied()
                .unwrap_or_default();
            println!("{} {} {}\n{first}", definition.key, own.len(), digest(&own));
        }
    }

    const GOLDEN_CVD_15M: &str = "64809276ms cvd_divergence 15m low prev=61969.20000000@56332199ms cur=61966.54000000@61514129ms dprice=-2.66000000 cvd=-11.99608482->-9.75701857 dcvd=2.23906625 gaps=0 span=5181930ms complete divergent=true candidate.divergence.cvd.15m@1 ctx=61966.54000000 levels=sfp_zone:low@61969.20000000[61966.54000000,61969.20000000],prior_sweep:low@61969.20000000,vwap@61983.67574530 location.levels@2 auction=warming";

    const GOLDEN_CVD_1H: &str = "108035804ms cvd_divergence 1h low prev=61900.09000000@79003423ms cur=61882.07000000@95647782ms dprice=-18.02000000 cvd=30.34139291->45.29007073 dcvd=14.94867782 gaps=0 span=16644359ms complete divergent=true candidate.divergence.cvd.1h@1 ctx=61882.07000000 levels=structural_low:low@61882.07000000,sfp_zone:low@61900.09000000[61882.07000000,61900.09000000],prior_sweep:low@61900.09000000,prior_sweep:low@61900.09000000 location.levels@2 auction=acceptance";

    const GOLDEN_CVD_4H: &str = "374447759ms cvd_divergence 4h high prev=62057.32000000@185552262ms cur=62140.03000000@321483259ms dprice=82.71000000 cvd=60.27519949->38.05121265 dcvd=-22.22398684 gaps=0 span=135930997ms complete divergent=true candidate.divergence.cvd.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000,structural_high:high@62140.03000000,structural_high:high@62140.03000000 location.levels@2 auction=unavailable";

    const GOLDEN_CVD_1D: &str = "1209708595ms value high=- low=- candidate.divergence.cvd.1d@1";

    const GOLDEN_OI_15M: &str = "64809276ms oi_divergence 15m low prev=61969.20000000@56332199ms cur=61966.54000000@61514129ms dprice=-2.66000000 cvd=-11.99608482->-9.75701857 dcvd=2.23906625 gaps=0 span=5181930ms complete oi=80319.70000000->80067.90000000 doi=-251.80000000 divergent=true candidate.divergence.oi.15m@1 ctx=61966.54000000 levels=sfp_zone:low@61969.20000000[61966.54000000,61969.20000000],prior_sweep:low@61969.20000000,vwap@61983.67574530 location.levels@2 auction=warming";

    const GOLDEN_OI_1H: &str = "198042133ms oi_divergence 1h high prev=61947.27000000@107168767ms cur=62057.32000000@185552262ms dprice=110.05000000 cvd=63.52029271->60.27519949 dcvd=-3.24509322 gaps=0 span=78383495ms complete oi=80141.20000000->79555.50000000 doi=-585.70000000 divergent=true candidate.divergence.oi.1h@1 ctx=62057.32000000 levels=prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.03772028,prior_sweep:high@62033.71000000,structural_high:high@62049.17000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value";

    const GOLDEN_OI_4H: &str = "374447759ms oi_divergence 4h high prev=62057.32000000@185552262ms cur=62140.03000000@321483259ms dprice=82.71000000 cvd=60.27519949->38.05121265 dcvd=-22.22398684 gaps=0 span=135930997ms complete oi=79555.50000000->79417.30000000 doi=-138.20000000 divergent=true candidate.divergence.oi.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000,structural_high:high@62140.03000000,structural_high:high@62140.03000000 location.levels@2 auction=unavailable";

    const GOLDEN_EFFORT_5M: &str = "188300455ms effort_result 5m end=188280000ms aggressor=sell effort=3.13009011 progress=-1.39000000 effort_high=2.60044813 effort_median=1.39537000 move_high=6.51000000 move_median=3.50000000 complete active=true candidate.effort_result.5m@1 ctx=62037.54000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62029.67532057,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000] location.levels@2 auction=outside_value";

    const GOLDEN_EFFORT_15M: &str = "189553603ms effort_result 15m end=189540000ms aggressor=sell effort=9.48881729 progress=4.33000000 effort_high=4.70789481 effort_median=2.62103277 move_high=10.92000000 move_median=6.00000000 complete active=true candidate.effort_result.15m@1 ctx=62034.73000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.25160864,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value";

    const GOLDEN_EFFORT_1H: &str = "194941793ms effort_result 1h end=194940000ms aggressor=sell effort=9.95302085 progress=-16.50000000 effort_high=9.69932969 effort_median=5.35401407 move_high=24.72000000 move_median=10.39000000 complete active=true candidate.effort_result.1h@1 ctx=62041.38000000 levels=sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],structural_low:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62031.11563947,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value";

    const GOLDEN_EXHAUSTION: &str = "196838221ms exhaustion end=196800000ms expansion=[1h end=196500000ms aggressor=sell effort=14.00580309 progress=30.02000000 effort_high=9.98765481 effort_median=5.44429252 move_high=24.72000000 move_median=10.46000000 complete active=false candidate.effort_result.1h@1] decay=[5m end=196800000ms aggressor=sell effort=0.51751975 progress=3.93000000 effort_high=2.60784460 effort_median=1.40469943 move_high=6.54000000 move_median=3.53000000 complete active=false candidate.effort_result.5m@1] decay_aggression=0.51751975 extreme=62009.55000000 close=62011.08000000 expanded=true decayed=true at_extreme=true active=true candidate.exhaustion@1 ctx=62011.08000000 levels=lvn@61975.00000000[61970.00000000,61980.00000000],prior_sweep:low@61975.11000000,prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],prior_sweep:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.78929859,prior_sweep:high@62033.71000000 location.levels@2 auction=outside_value";

    const GOLDEN_ABSORPTION_5M: &str = "173705421ms absorption effort=[5m end=173700000ms aggressor=sell effort=22.20000000 progress=0.00000000 effort_high=16.04000000 effort_median=5.80000000 move_high=0.10000000 move_median=0.00000000 complete active=true candidate.effort_result.5m@1] passive=bid band=0.00050000 added=31.83000000 filled=24.02000000 book=complete replenished=true active=true candidate.absorption.5m@1 ctx=60000.10000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.03655487,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable";

    const GOLDEN_ABSORPTION_15M: &str = "179341368ms absorption effort=[15m end=179340000ms aggressor=buy effort=25.58000000 progress=0.00000000 effort_high=24.64000000 effort_median=12.76000000 move_high=0.10000000 move_median=0.00000000 complete active=true candidate.effort_result.15m@1] passive=ask band=0.00050000 added=53.00000000 filled=49.70000000 book=complete replenished=true active=true candidate.absorption.15m@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05888156,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable";

    const GOLDEN_ABSORPTION_1H: &str = "178862262ms absorption effort=[1h end=178860000ms aggressor=buy effort=64.62000000 progress=0.00000000 effort_high=51.32000000 effort_median=31.04000000 move_high=0.10000000 move_median=0.10000000 complete active=true candidate.effort_result.1h@1] passive=ask band=0.00050000 added=146.27000000 filled=144.28000000 book=complete replenished=true active=true candidate.absorption.1h@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05717431,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable";

    const GOLDEN_OI_1D: &str = "1209708595ms value high=- low=- candidate.divergence.oi.1d@1";
}
