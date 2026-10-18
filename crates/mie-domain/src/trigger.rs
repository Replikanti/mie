//! Bias and trigger (brief §11, ADR-012, ADR-023, ADR-024; ADR-053,
//! proposed).
//!
//! A bias describes what the broader evidence suggests and never authorizes
//! execution. Only a trigger — an observable event at a defined location —
//! can, and only through the validation pipeline. Order-flow features feed
//! triggers; they are not strategy authority on their own, and aggression is
//! not direction.
//!
//! - `bias.context@1` ([`Bias`], [`bias`], ADR-053 D3): the volatility
//!   regime and, per structure timeframe, the step of the last swing pair
//!   of each side. Recorded with every trigger, read by no rule.
//! - One rule per [`TriggerFamily`], each a versioned feature
//!   `trigger.<family>[.<qualifier>]@1`:
//!   - failed auction, failed reclaim and breakout + acceptance on the
//!     auction facts of `location.auction.prior_day@1` (ADR-053 D5,
//!     `auction`);
//!   - SFP on the structure facts of `structure.levels.<tf>@1` (ADR-053 D5,
//!     `structure`);
//!   - CVD divergence, effort vs result, exhaustion and absorption on the
//!     order-flow candidates, gated on location (ADR-053 D4, D6): #24 M2;
//!   - OI expansion followed by rejection (ADR-053 D7): #24 M3.
//!
//! Triggers are the last stage of an event (ADR-053 D2): they run after the
//! candidate stage, on the committed state, read only the facts the same
//! event produced plus that state, and cannot fail. Each emitted
//! [`TriggerEvent`] carries the rule's feature key (the parameter version),
//! the family, the evidence fact, a [`LocationContext`] at its anchor and a
//! [`Bias`] snapshot. Events are not hashed; the value of each trigger id,
//! its last trigger, is part of [`MarketState`](crate::state::MarketState)
//! as [`Triggers`]. No trigger carries a direction or an execution meaning
//! (ADR-010): no target, stop or size.

pub mod bias;

mod auction;
mod structure;

pub use bias::{Bias, StepAt, SwingSequence, SwingStep};

use crate::bars::Timeframe;
use crate::candidate::{Candidates, LocationContext};
use crate::feature::{FeatureKey, FeatureValue, Input, catalog};
use crate::fingerprint::Fingerprinter;
use crate::location::{AuctionStatus, LocationEvent, LocationState};
use crate::num::Price;
use crate::regime::Regime;
use crate::state_hash::StateEncode;
use crate::structure::{STRUCTURE_TIMEFRAMES, StructureEvent, StructureSet, Sweep};
use crate::time::EventTime;
use crate::volatility::AtrRegimeSeries;
use auction::AUCTION_TRIGGERS;
use std::fmt;

/// Families of observable trigger events that hypotheses can test.
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041, ADR-053 D1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerFamily {
    /// Failed auction: a probe beyond value rejected back inside.
    ///
    /// The location fact is [`AuctionState::FailedBreakout`](crate::location::AuctionState::FailedBreakout)
    /// of `location.auction.prior_day@1` (ADR-044); the trigger rule is
    /// `trigger.failed_auction@1` (ADR-053 D5).
    FailedAuction,
    /// Swing failure pattern: a sweep of a swing level followed by rejection.
    ///
    /// The structure fact is [`StructureEvent::Sfp`](crate::structure::StructureEvent::Sfp)
    /// (ADR-037); the trigger rule is `trigger.sfp.<tf>@1` (ADR-053 D5).
    Sfp,
    /// Aggression absorbed by passive liquidity.
    ///
    /// The candidate fact is [`CandidateEvent::Absorption`](crate::candidate::CandidateEvent::Absorption)
    /// of `candidate.absorption.<tf>@1` (ADR-051); the trigger rule is
    /// ADR-053 D6.
    Absorption,
    /// Aggression fading out at an extreme.
    ///
    /// The candidate fact is [`CandidateEvent::Exhaustion`](crate::candidate::CandidateEvent::Exhaustion)
    /// of `candidate.exhaustion@1` (ADR-051); the trigger rule is ADR-053
    /// D6.
    Exhaustion,
    /// Aggression without price progress (effort vs result).
    ///
    /// The candidate fact is [`CandidateEvent::EffortVsResult`](crate::candidate::CandidateEvent::EffortVsResult)
    /// of `candidate.effort_result.<tf>@1` (ADR-051); the trigger rule is
    /// ADR-053 D6.
    EffortVsResult,
    /// Cumulative volume delta diverging from price.
    ///
    /// The candidate fact is [`CandidateEvent::CvdDivergence`](crate::candidate::CandidateEvent::CvdDivergence)
    /// of `candidate.divergence.cvd.<tf>@1` (ADR-051); the trigger rule is
    /// ADR-053 D6.
    CvdDivergence,
    /// Open-interest expansion followed by rejection.
    ///
    /// The rejection is an SFP or a failed auction, the expansion a rise of
    /// `derivatives.oi.5m@1` over the excursion; the trigger rule is
    /// ADR-053 D7.
    OiExpansionRejection,
    /// Failed reclaim of a lost level.
    ///
    /// The location fact is [`AuctionState::FailedReclaim`](crate::location::AuctionState::FailedReclaim)
    /// (ADR-044); the trigger rule is `trigger.failed_reclaim@1` (ADR-053
    /// D5).
    FailedReclaim,
    /// Breakout followed by acceptance.
    ///
    /// The location fact is a [`Breakout`](crate::location::AuctionState::Breakout)
    /// that turns into [`Acceptance`](crate::location::AuctionState::Acceptance)
    /// (ADR-044); the trigger rule is `trigger.breakout_acceptance@1`
    /// (ADR-053 D5).
    BreakoutAcceptance,
}

impl TriggerFamily {
    /// Every family, in declaration order.
    pub const ALL: [Self; 9] = [
        Self::FailedAuction,
        Self::Sfp,
        Self::Absorption,
        Self::Exhaustion,
        Self::EffortVsResult,
        Self::CvdDivergence,
        Self::OiExpansionRejection,
        Self::FailedReclaim,
        Self::BreakoutAcceptance,
    ];

    /// The canonical snake_case name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FailedAuction => "failed_auction",
            Self::Sfp => "sfp",
            Self::Absorption => "absorption",
            Self::Exhaustion => "exhaustion",
            Self::EffortVsResult => "effort_vs_result",
            Self::CvdDivergence => "cvd_divergence",
            Self::OiExpansionRejection => "oi_expansion_rejection",
            Self::FailedReclaim => "failed_reclaim",
            Self::BreakoutAcceptance => "breakout_acceptance",
        }
    }
}

impl StateEncode for TriggerFamily {
    /// `write_u8` in declaration order: FailedAuction 0, Sfp 1, Absorption
    /// 2, Exhaustion 3, EffortVsResult 4, CvdDivergence 5,
    /// OiExpansionRejection 6, FailedReclaim 7, BreakoutAcceptance 8
    /// (ADR-041, ADR-053 D1).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::FailedAuction => 0,
            Self::Sfp => 1,
            Self::Absorption => 2,
            Self::Exhaustion => 3,
            Self::EffortVsResult => 4,
            Self::CvdDivergence => 5,
            Self::OiExpansionRejection => 6,
            Self::FailedReclaim => 7,
            Self::BreakoutAcceptance => 8,
        });
    }
}

impl fmt::Display for TriggerFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One trigger as a value holds it (ADR-053 D1): when it became visible,
/// its anchor price and its evidence in the hashed form of its input
/// feature. The location context and the bias are carried by the event
/// only: they are reads of other hashed features.
///
/// `Display` prints `<known_at> anchor=<price> [<evidence>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trigger<E> {
    /// Time of the event that emitted it: its fact's `known_at`.
    pub known_at: EventTime,
    /// The price its location is read at.
    pub anchor: Price,
    /// The fact it was built on.
    pub evidence: E,
}

impl<E: StateEncode> StateEncode for Trigger<E> {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            known_at,
            anchor,
            evidence,
        } = self;
        known_at.encode(f);
        anchor.encode(f);
        evidence.encode(f);
    }
}

impl<E: fmt::Display> fmt::Display for Trigger<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} anchor={} [{}]",
            self.known_at, self.anchor, self.evidence
        )
    }
}

/// The value of one trigger id: its last trigger, `None` before the first.
///
/// `Display` prints `last=[<trigger>]` (`last=-` for none) and the feature
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TriggerValue<E> {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The last trigger.
    pub last: Option<Trigger<E>>,
}

impl<E: StateEncode> StateEncode for TriggerValue<E> {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { feature, last } = self;
        feature.encode(f);
        last.encode(f);
    }
}

impl<E: fmt::Display> fmt::Display for TriggerValue<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.last {
            Some(last) => write!(f, "last=[{last}] {}", self.feature),
            None => write!(f, "last=- {}", self.feature),
        }
    }
}

/// The bias and trigger features of the Market State (ADR-053 D1). Later
/// milestones append their values at the end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Triggers {
    /// `bias.context@1`, recomputed after every event.
    bias: Bias,
    /// `trigger.failed_auction@1`, `trigger.failed_reclaim@1` and
    /// `trigger.breakout_acceptance@1`, in `AUCTION_TRIGGERS` order.
    auction: [FeatureValue<TriggerValue<AuctionStatus>>; 3],
    /// `trigger.sfp.<tf>@1`, in [`STRUCTURE_TIMEFRAMES`] order.
    sfp: [FeatureValue<TriggerValue<Sweep>>; 4],
}

impl StateEncode for Triggers {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { bias, auction, sfp } = self;
        bias.encode(f);
        auction.encode(f);
        sfp.encode(f);
    }
}

impl Default for Triggers {
    fn default() -> Self {
        Self::new()
    }
}

impl Triggers {
    /// The values before the first event: each follows its input as that
    /// input stands in a new engine, with no trigger yet.
    pub fn new() -> Self {
        let regime = AtrRegimeSeries::new().regime();
        let structure = StructureSet::new();
        let location = LocationState::new();
        let candidates = Candidates::new();
        TriggerTracker::new().values(&Inputs {
            regime: &regime,
            structure: &structure,
            location: &location,
            candidates: &candidates,
        })
    }

    /// `bias.context@1`.
    pub fn bias(&self) -> &Bias {
        &self.bias
    }

    /// `trigger.failed_auction@1`.
    pub fn failed_auction(&self) -> &FeatureValue<TriggerValue<AuctionStatus>> {
        &self.auction[0]
    }

    /// `trigger.failed_reclaim@1`.
    pub fn failed_reclaim(&self) -> &FeatureValue<TriggerValue<AuctionStatus>> {
        &self.auction[1]
    }

    /// `trigger.breakout_acceptance@1`.
    pub fn breakout_acceptance(&self) -> &FeatureValue<TriggerValue<AuctionStatus>> {
        &self.auction[2]
    }

    /// The auction trigger values with their families: failed auction,
    /// failed reclaim, breakout + acceptance.
    pub fn iter_auction(
        &self,
    ) -> impl Iterator<Item = (TriggerFamily, &FeatureValue<TriggerValue<AuctionStatus>>)> {
        AUCTION_TRIGGERS
            .into_iter()
            .map(|(family, _)| family)
            .zip(&self.auction)
    }

    /// `trigger.sfp.<tf>@1` of `timeframe`, if it is a structure timeframe.
    pub fn sfp(&self, timeframe: Timeframe) -> Option<&FeatureValue<TriggerValue<Sweep>>> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.sfp[index])
    }

    /// `trigger.sfp.<tf>@1` of every structure timeframe, shortest first.
    pub fn iter_sfp(
        &self,
    ) -> impl Iterator<Item = (Timeframe, &FeatureValue<TriggerValue<Sweep>>)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.sfp)
    }

    /// `bias.context@1`, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn bias_mut(&mut self) -> &mut Bias {
        &mut self.bias
    }

    /// The auction trigger values, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn auction_mut(&mut self) -> &mut [FeatureValue<TriggerValue<AuctionStatus>>; 3] {
        &mut self.auction
    }

    /// The SFP trigger values, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn sfp_mut(&mut self) -> &mut [FeatureValue<TriggerValue<Sweep>>; 4] {
        &mut self.sfp
    }
}

/// The fact a trigger was built on (ADR-053 D1): its input feature's own
/// hashed form.
///
/// `Display` prints the fact.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TriggerEvidence {
    /// A `location.auction.prior_day@1` status: the status of the close
    /// whose label changed, against the reference it was classified under.
    Auction(AuctionStatus),
    /// A `structure.levels.<tf>@1` sweep resolved as an SFP.
    Sfp(Sweep),
}

impl fmt::Display for TriggerEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auction(status) => write!(f, "{status}"),
            Self::Sfp(sweep) => write!(f, "{} {sweep}", sweep.timeframe),
        }
    }
}

/// A trigger fact, in emission order (ADR-053 D2): per structure fact of
/// the event, in its order, the SFP triggers; then per location fact, in
/// its order, the auction triggers.
///
/// Not hashed. `Display` prints the canonical line the golden tests pin:
/// the family, the evidence, the rule's feature key, the location context
/// and the bias.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TriggerEvent {
    /// The rule that produced it, at its parameter version:
    /// `trigger.<family>[.<qualifier>]@1`.
    pub feature: FeatureKey,
    /// The family.
    pub family: TriggerFamily,
    /// When it became visible: its fact's `known_at`, the time of the event
    /// that emitted it.
    pub known_at: EventTime,
    /// The rule's inputs, as its definition declares them.
    pub inputs: &'static [Input],
    /// The fact it was built on.
    pub evidence: TriggerEvidence,
    /// Where it happened: the registry levels and the auction state at its
    /// anchor (ADR-053 D4).
    pub context: LocationContext,
    /// The bias as it stood after the event (ADR-053 D3); read by no rule.
    pub bias: Bias,
}

impl TriggerEvent {
    /// The price its location is read at.
    pub fn anchor(&self) -> Price {
        self.context.anchor
    }
}

impl fmt::Display for TriggerEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} evidence=[{}] {} {} {}",
            self.family, self.evidence, self.feature, self.context, self.bias
        )
    }
}

/// The committed state the trigger stage reads.
pub(crate) struct Inputs<'a> {
    /// `volatility.regime.1h@1`, for the bias.
    pub(crate) regime: &'a FeatureValue<Regime>,
    /// The structure, for the validity of `structure.levels.<tf>@1`.
    pub(crate) structure: &'a StructureSet,
    /// Location: the registry for each context, and the validity of
    /// `location.auction.prior_day@1`.
    pub(crate) location: &'a LocationState,
    /// The candidates: `candidate.divergence.cvd.<tf>@1`, for the bias.
    pub(crate) candidates: &'a Candidates,
}

/// The trigger engine state (ADR-053 D2): the last trigger of each id.
/// Engine state, updated in place; the Market State exposes [`Triggers`],
/// which follows each id's input validity around it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TriggerTracker {
    /// The auction triggers, in `AUCTION_TRIGGERS` order.
    auction: [Option<Trigger<AuctionStatus>>; 3],
    /// The SFP triggers, in [`STRUCTURE_TIMEFRAMES`] order.
    sfp: [Option<Trigger<Sweep>>; 4],
}

impl TriggerTracker {
    /// No trigger yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The infallible stage (ADR-053 D2): after the candidate stage has
    /// committed the event, reads the bias, turns the event's structure
    /// facts and then its location facts, each in their order, into
    /// trigger events, and refreshes `triggers`. Reads only those facts and
    /// the committed state; appends the events to `events`.
    pub(crate) fn detect(
        &mut self,
        structure_events: &[StructureEvent],
        location_events: &[LocationEvent],
        inputs: Inputs<'_>,
        triggers: &mut Triggers,
        events: &mut Vec<TriggerEvent>,
    ) {
        let bias = Bias::from_parts(inputs.regime, inputs.candidates);
        for fact in structure_events {
            let Some((position, definition, sweep)) = structure::triggered(fact) else {
                continue;
            };
            let anchor = sweep.level.price;
            let context =
                LocationContext::read(anchor, Some(structure::swept_swing(sweep)), inputs.location);
            self.sfp[position] = Some(Trigger {
                known_at: sweep.known_at,
                anchor,
                evidence: *sweep,
            });
            events.push(TriggerEvent {
                feature: definition.key,
                family: TriggerFamily::Sfp,
                known_at: sweep.known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::Sfp(*sweep),
                context,
                bias,
            });
        }
        for fact in location_events {
            let LocationEvent::Auction {
                from,
                to,
                known_at,
                status,
                ..
            } = fact
            else {
                continue;
            };
            let Some((position, anchor)) = auction::triggered(*from, *to, *known_at, status) else {
                continue;
            };
            let (family, definition) = AUCTION_TRIGGERS[position];
            self.auction[position] = Some(Trigger {
                known_at: *known_at,
                anchor,
                evidence: *status,
            });
            events.push(TriggerEvent {
                feature: definition.key,
                family,
                known_at: *known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::Auction(*status),
                context: LocationContext::read(anchor, None, inputs.location),
                bias,
            });
        }
        let Triggers {
            bias: held,
            auction,
            sfp,
        } = triggers;
        *held = bias;
        *auction = self.auction_values(inputs.location);
        *sfp = self.sfp_values(inputs.structure);
    }

    /// Every value as `inputs` stand, with the last triggers held.
    fn values(&self, inputs: &Inputs<'_>) -> Triggers {
        Triggers {
            bias: Bias::from_parts(inputs.regime, inputs.candidates),
            auction: self.auction_values(inputs.location),
            sfp: self.sfp_values(inputs.structure),
        }
    }

    /// The auction trigger values: each follows
    /// `location.auction.prior_day@1` (ADR-053 D1).
    fn auction_values(
        &self,
        location: &LocationState,
    ) -> [FeatureValue<TriggerValue<AuctionStatus>>; 3] {
        [0, 1, 2].map(|position| {
            following(
                &location.auction,
                TriggerValue {
                    feature: AUCTION_TRIGGERS[position].1.key,
                    last: self.auction[position],
                },
            )
        })
    }

    /// The SFP trigger values: each follows its timeframe's
    /// `structure.levels.<tf>@1` (ADR-053 D1).
    fn sfp_values(&self, structure: &StructureSet) -> [FeatureValue<TriggerValue<Sweep>>; 4] {
        [0, 1, 2, 3].map(|position| {
            let value = TriggerValue {
                feature: catalog::TRIGGER_SFP[position].1.key,
                last: self.sfp[position],
            };
            match structure.get(STRUCTURE_TIMEFRAMES[position]) {
                Some(timeframe) => following(&timeframe.levels, value),
                // Every structure timeframe has structure.
                None => FeatureValue::Ready(value),
            }
        })
    }
}

/// `value`, warming up or unavailable exactly while `input` is, with the
/// same counts or reason (ADR-053 D1: a trigger has no warm-up of its own).
fn following<T, U>(input: &FeatureValue<T>, value: U) -> FeatureValue<U> {
    match input {
        FeatureValue::Ready(_) => FeatureValue::Ready(value),
        FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
            observed: *observed,
            required: *required,
        },
        FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason: *reason },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::divergence::{CvdComparison, Divergence, SwingAnchor, SwingPair};
    use crate::event::samples::t;
    use crate::event::{Aggressor, MarketEvent, Trade};
    use crate::feature::FeatureDefinition;
    use crate::location::{AuctionState, LevelKind, Region, zone_contains};
    use crate::num::{Qty, SCALE};
    use crate::regime::AtrPercentile;
    use crate::state::{MarketStateEngine, StateError};
    use crate::structure::{Side, SweepOutcome};

    const MINUTE: i64 = 60_000;
    const DAY: i64 = 86_400_000;

    /// Whole USDT as a price.
    fn usdt(whole: i64) -> Price {
        Price::from_units(whole * SCALE)
    }

    /// A tape of trades, ids counting up.
    #[derive(Default)]
    struct Tape {
        events: Vec<MarketEvent>,
        trade_id: u64,
    }

    impl Tape {
        fn trade(&mut self, millis: i64, price: Price) -> &mut Self {
            self.trade_id += 1;
            self.events.push(MarketEvent::Trade(Trade {
                time: t(millis),
                trade_id: self.trade_id,
                price,
                qty: Qty::from_units(10_000_000),
                aggressor: Aggressor::Buy,
            }));
            self
        }

        /// Day 0 of the auction tapes: one trade a minute sweeping
        /// 60 000 … 60 990 USDT, the prior day's value for day 1.
        fn prior_day() -> Self {
            let mut tape = Self::default();
            for minute in 0..1_440 {
                tape.trade(minute * MINUTE + 1_000, usdt(60_000 + (minute % 100) * 10));
            }
            tape
        }

        /// One trade a minute on day 1, at `path`, 1 s into each minute: each
        /// trade closes the minute before it.
        fn day_one(&mut self, path: &[Price]) -> &mut Self {
            for (minute, price) in (0..).zip(path) {
                self.trade(DAY + minute * MINUTE + 1_000, *price);
            }
            self
        }

        /// A swing high at 60 015 USDT on bar 7 of `timeframe`, after rising
        /// bars, confirmed at the end of bar 10 by the trade 1 s into bar 11
        /// at 60 008 (ADR-037 D2, D3). Each bar is three trades: its high,
        /// its low, its close.
        fn swing_high(timeframe: Timeframe) -> Self {
            let span = timeframe.millis();
            let mut tape = Self::default();
            for (index, high) in (0..).zip([
                59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011,
                60_010,
            ]) {
                let open = index * span;
                tape.trade(open + 1_000, usdt(high))
                    .trade(open + 2_000, usdt(high - 5))
                    .trade(open + 3_000, usdt(high - 2));
            }
            tape.trade(11 * span + 1_000, usdt(60_008));
            tape
        }
    }

    /// The prior day's value area `(VAL, VAH)` on day 1 of the auction
    /// tapes.
    fn value_area() -> (Price, Price) {
        let mut tape = Tape::prior_day();
        tape.trade(DAY + 1_000, usdt(60_500));
        let mut engine = MarketStateEngine::new();
        for event in &tape.events {
            engine.apply(event).unwrap();
        }
        let prior = engine.state().profile.prior_day.ready().unwrap();
        (prior.val, prior.vah)
    }

    /// Inside value, above it and below it, beyond every edge band.
    const INSIDE: i64 = 60_500;
    const ABOVE: i64 = 62_000;
    const BELOW: i64 = 58_000;

    /// `count` minutes at `whole` USDT.
    fn minutes(whole: i64, count: usize) -> Vec<Price> {
        vec![usdt(whole); count]
    }

    /// Day 0, then day 1 along `path`.
    fn auction_tape(path: &[Price]) -> Vec<MarketEvent> {
        let mut tape = Tape::prior_day();
        tape.day_one(path);
        tape.events
    }

    /// Runs `events` and returns the engine and every trigger event with
    /// the index of the event that emitted it. Checks after every event
    /// that each value follows its input's validity and that the values of
    /// the ids that fired hold the events' triggers.
    fn run(engine: &mut MarketStateEngine, events: &[MarketEvent]) -> Vec<(usize, TriggerEvent)> {
        let mut fired = Vec::new();
        for (index, event) in events.iter().enumerate() {
            engine.apply(event).unwrap();
            check_values(engine);
            fired.extend(
                engine
                    .trigger_events()
                    .iter()
                    .map(|fact| (index, fact.clone())),
            );
        }
        fired
    }

    /// The tag of a value: 0 ready, 1 warming up, 2 unavailable, with the
    /// counts or reason.
    fn validity<T>(value: &FeatureValue<T>) -> FeatureValue<()> {
        following(value, ())
    }

    /// ADR-053 D1, D2 on the engine's last event: each trigger value is
    /// ready exactly while its fact input is, and an id that fired holds
    /// that trigger as its last.
    fn check_values(engine: &MarketStateEngine) {
        let state = engine.state();
        let triggers = &state.triggers;
        assert_eq!(triggers.bias(), &Bias::read(state));
        for (_, value) in triggers.iter_auction() {
            assert_eq!(validity(value), validity(&state.location.auction));
        }
        for (timeframe, value) in triggers.iter_sfp() {
            let levels = &state.structure.get(timeframe).unwrap().levels;
            assert_eq!(validity(value), validity(levels));
        }
        let facts = engine.trigger_events();
        for (index, fact) in facts.iter().enumerate() {
            assert_eq!(fact.known_at, state.as_of.unwrap());
            assert_eq!(&fact.bias, triggers.bias());
            // One sweeping trade can sweep several levels of a timeframe, so
            // one event can emit an id more than once: the value holds the
            // last in emission order.
            if facts[index + 1..]
                .iter()
                .any(|later| later.feature == fact.feature)
            {
                continue;
            }
            match fact.evidence {
                TriggerEvidence::Auction(status) => {
                    let (_, value) = triggers
                        .iter_auction()
                        .find(|(family, _)| *family == fact.family)
                        .unwrap();
                    if let FeatureValue::Ready(value) = value {
                        assert_eq!(value.feature, fact.feature);
                        assert_eq!(
                            value.last,
                            Some(Trigger {
                                known_at: fact.known_at,
                                anchor: fact.anchor(),
                                evidence: status,
                            })
                        );
                    }
                }
                TriggerEvidence::Sfp(sweep) => {
                    let FeatureValue::Ready(value) = triggers.sfp(sweep.timeframe).unwrap() else {
                        panic!("an SFP resolves on a ready registry: {fact}");
                    };
                    assert_eq!(value.feature, fact.feature);
                    assert_eq!(
                        value.last,
                        Some(Trigger {
                            known_at: fact.known_at,
                            anchor: fact.anchor(),
                            evidence: sweep,
                        })
                    );
                }
            }
        }
    }

    /// The trigger events of `events` from a new engine.
    fn triggers_of(events: &[MarketEvent]) -> Vec<(usize, TriggerEvent)> {
        run(&mut MarketStateEngine::new(), events)
    }

    /// The families of `fired`, in order.
    fn families(fired: &[(usize, TriggerEvent)]) -> Vec<TriggerFamily> {
        fired.iter().map(|(_, fact)| fact.family).collect()
    }

    /// The auction triggers of `fired`, in order: the auction tapes also
    /// sweep day 0's swing highs, so they emit SFP triggers too.
    fn auction_only(fired: Vec<(usize, TriggerEvent)>) -> Vec<(usize, TriggerEvent)> {
        fired
            .into_iter()
            .filter(|(_, fact)| fact.family != TriggerFamily::Sfp)
            .collect()
    }

    /// The auction status a trigger was built on.
    fn status_of(fact: &TriggerEvent) -> &AuctionStatus {
        match &fact.evidence {
            TriggerEvidence::Auction(status) => status,
            TriggerEvidence::Sfp(sweep) => panic!("{sweep}"),
        }
    }

    #[test]
    fn a_failed_auction_fires_once_at_the_crossed_edge() {
        let (val, vah) = value_area();
        for (beyond, toward, edge) in [(ABOVE, Region::Above, vah), (BELOW, Region::Below, val)] {
            // Inside value, five closes beyond it, two back inside: the
            // first close back fails the probe; the trade that closes it, 1 s
            // into minute 16, is the trigger's event.
            let mut path = minutes(INSIDE, 10);
            path.extend(minutes(beyond, 5));
            path.extend(minutes(INSIDE, 2));
            let fired = auction_only(triggers_of(&auction_tape(&path)));
            assert_eq!(families(&fired), [TriggerFamily::FailedAuction]);
            let (_, fact) = &fired[0];
            assert_eq!(fact.feature, catalog::TRIGGER_FAILED_AUCTION_V1.key);
            assert_eq!(fact.inputs, catalog::TRIGGER_FAILED_AUCTION_V1.inputs);
            assert_eq!(fact.known_at, t(DAY + 16 * MINUTE + 1_000));
            assert_eq!(fact.anchor(), edge);
            let status = status_of(fact);
            assert_eq!(status.state, AuctionState::FailedBreakout);
            let failure = status.failure.unwrap();
            assert_eq!(
                (failure.toward, failure.closes, failure.at),
                (toward, 5, fact.known_at)
            );
            assert_eq!(fact.context.anchor, edge);
            assert_eq!(fact.context.auction, FeatureValue::Ready(*status));
        }
    }

    #[test]
    fn near_misses_of_a_failed_auction_do_not_fire() {
        let (_, vah) = value_area();
        // A close one unit below VAH lies in the edge band: neutral, it
        // neither counts nor fails the probe.
        let mut path = minutes(INSIDE, 10);
        path.extend(minutes(ABOVE, 5));
        path.extend([Price::from_units(vah.units() - 1); 3]);
        assert_eq!(
            families(&auction_only(triggers_of(&auction_tape(&path)))),
            []
        );
        // A probe that reaches 60 counted closes is accepted, not failed.
        let mut path = minutes(INSIDE, 10);
        path.extend(minutes(ABOVE, 61));
        assert_eq!(
            families(&auction_only(triggers_of(&auction_tape(&path)))),
            [TriggerFamily::BreakoutAcceptance]
        );
    }

    #[test]
    fn breakout_acceptance_fires_at_the_nth_counted_close() {
        let (val, vah) = value_area();
        assert_eq!(crate::location::ACCEPTANCE_CLOSES, 60);
        for (beyond, edge) in [(ABOVE, vah), (BELOW, val)] {
            // 60 closes beyond: the 60th, closed 1 s into minute 70, accepts.
            let mut path = minutes(INSIDE, 10);
            path.extend(minutes(beyond, 61));
            let fired = auction_only(triggers_of(&auction_tape(&path)));
            assert_eq!(families(&fired), [TriggerFamily::BreakoutAcceptance]);
            let (_, fact) = &fired[0];
            assert_eq!(fact.feature, catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key);
            assert_eq!(fact.known_at, t(DAY + 70 * MINUTE + 1_000));
            assert_eq!(fact.anchor(), edge);
            assert_eq!(status_of(fact).state, AuctionState::Acceptance);
            // 59 closes beyond, then back: a failed auction instead.
            let mut path = minutes(INSIDE, 10);
            path.extend(minutes(beyond, 59));
            path.extend(minutes(INSIDE, 2));
            let fired = auction_only(triggers_of(&auction_tape(&path)));
            assert_eq!(families(&fired), [TriggerFamily::FailedAuction]);
            assert_eq!(status_of(&fired[0].1).failure.unwrap().closes, 59);
        }
    }

    #[test]
    fn a_failed_reclaim_anchors_at_the_accepted_edge() {
        let (val, vah) = value_area();
        for (outside, edge) in [(ABOVE, vah), (BELOW, val)] {
            // The first close accepts the region outside value; three closes
            // back inside probe toward value, and the close back outside
            // fails that probe.
            let mut path = minutes(outside, 10);
            path.extend(minutes(INSIDE, 3));
            path.extend(minutes(outside, 2));
            let fired = auction_only(triggers_of(&auction_tape(&path)));
            assert_eq!(families(&fired), [TriggerFamily::FailedReclaim]);
            let (_, fact) = &fired[0];
            assert_eq!(fact.feature, catalog::TRIGGER_FAILED_RECLAIM_V1.key);
            assert_eq!(fact.known_at, t(DAY + 14 * MINUTE + 1_000));
            assert_eq!(fact.anchor(), edge);
            let status = status_of(fact);
            assert_eq!(status.state, AuctionState::FailedReclaim);
            assert_eq!(status.failure.unwrap().toward, Region::In);
        }
        // Accepted above by a probe, then the same reclaim attempt.
        let mut path = minutes(INSIDE, 10);
        path.extend(minutes(ABOVE, 61));
        path.extend(minutes(INSIDE, 3));
        path.extend(minutes(ABOVE, 2));
        assert_eq!(
            families(&auction_only(triggers_of(&auction_tape(&path)))),
            [
                TriggerFamily::BreakoutAcceptance,
                TriggerFamily::FailedReclaim
            ]
        );
    }

    #[test]
    fn the_rollover_close_is_read_from_its_event() {
        // ADR-053 D5, ADR-044 D2: the trade that closes 23:59 and the day
        // fails a probe against day 0's value, while the state has already
        // switched to day 1's (unavailable: one bin of 60 500 is narrower
        // than both edge bands).
        let (_, vah) = value_area();
        let mut path = minutes(INSIDE, 1_430);
        path.extend(minutes(ABOVE, 9));
        path.extend(minutes(INSIDE, 2));
        let mut engine = MarketStateEngine::new();
        let fired = auction_only(run(&mut engine, &auction_tape(&path)));
        assert_eq!(families(&fired), [TriggerFamily::FailedAuction]);
        let (_, fact) = &fired[0];
        assert_eq!(fact.known_at, t(2 * DAY + 1_000));
        let status = status_of(fact);
        assert_eq!(status.reference_start, t(0));
        assert_eq!(fact.anchor(), vah);
        let unavailable = FeatureValue::Unavailable {
            reason: crate::feature::Unavailability::InputInvalid,
        };
        assert_eq!(engine.state().location.auction, unavailable);
        assert_eq!(fact.context.auction, unavailable);
        // The value follows its input; the trigger is held.
        assert_eq!(
            validity(engine.state().triggers.failed_auction()),
            validity(&unavailable)
        );
    }

    /// The SFP scenario of `timeframe`: [`Tape::swing_high`], then bar 11
    /// sweeps to 60 020 and closes back at 60 014, closed by the trade 1 s
    /// into bar 12. With `break_out`, bar 11 closes at 60 020 and bar 12
    /// beyond the level too, closed 1 s into bar 13.
    fn sfp_tape(timeframe: Timeframe, break_out: bool) -> Vec<MarketEvent> {
        let span = timeframe.millis();
        let mut tape = Tape::swing_high(timeframe);
        tape.trade(11 * span + 2_000, usdt(60_020));
        if break_out {
            tape.trade(12 * span + 1_000, usdt(60_030))
                .trade(12 * span + 2_000, usdt(60_017))
                .trade(13 * span + 1_000, usdt(60_017));
        } else {
            tape.trade(11 * span + 3_000, usdt(60_014))
                .trade(12 * span + 1_000, usdt(60_014));
        }
        tape.events
    }

    #[test]
    fn an_sfp_fires_on_every_structure_timeframe_and_a_break_does_not() {
        for (position, (timeframe, definition)) in catalog::TRIGGER_SFP.into_iter().enumerate() {
            let span = timeframe.millis();
            let mut engine = MarketStateEngine::new();
            let own: Vec<TriggerEvent> = run(&mut engine, &sfp_tape(timeframe, false))
                .into_iter()
                .map(|(_, fact)| fact)
                .filter(|fact| fact.feature == definition.key)
                .collect();
            let [fact] = own.as_slice() else {
                panic!("{timeframe}: {own:?}")
            };
            assert_eq!(fact.family, TriggerFamily::Sfp);
            assert_eq!(fact.inputs, definition.inputs);
            assert_eq!(fact.known_at, t(12 * span + 1_000), "{timeframe}");
            assert_eq!(fact.anchor(), usdt(60_015));
            let TriggerEvidence::Sfp(sweep) = fact.evidence else {
                panic!("{fact}")
            };
            assert_eq!(
                (sweep.timeframe, sweep.outcome, sweep.level.swing_time),
                (timeframe, SweepOutcome::Sfp, t(7 * span))
            );
            assert_eq!(sweep.feature, catalog::STRUCTURE_LEVELS[position].1.key);
            // The context leaves out every level of the swept swing, which
            // the registry holds at the anchor.
            let own_swing = |level_side: Option<crate::location::LevelSide>,
                             source: FeatureKey,
                             anchor: Option<EventTime>| {
                level_side == Some(crate::location::LevelSide::High)
                    && source == sweep.feature
                    && anchor == Some(t(7 * span))
            };
            let registry = engine.state().location.levels.ready().unwrap();
            assert!(
                registry.levels().iter().any(|level| {
                    own_swing(level.side, level.source, level.anchor)
                        && zone_contains(level, fact.anchor())
                }),
                "{timeframe}"
            );
            let levels = fact.context.levels.ready().unwrap();
            assert!(
                levels.levels.iter().all(|level| !own_swing(
                    level.side,
                    level.source,
                    level.anchor
                )),
                "{fact}"
            );
            // A clean break is no trigger.
            let broken = triggers_of(&sfp_tape(timeframe, true));
            assert!(
                broken
                    .iter()
                    .all(|(_, fact)| fact.feature != definition.key),
                "{timeframe}"
            );
        }
    }

    /// Whether a level is the swing `sweep` swept, as any structure
    /// timeframe registered it (ADR-053 D4): a `structure.levels.<tf>`
    /// level of the swing's side and price whose swing bar nests with the
    /// swept swing's bar. Written from the ADR text, not from the code.
    fn of_swept_swing(
        sweep: &Sweep,
        (side, source, anchor, price): (
            Option<crate::location::LevelSide>,
            FeatureKey,
            Option<EventTime>,
            Price,
        ),
    ) -> bool {
        let Some((timeframe, _)) = catalog::STRUCTURE_LEVELS
            .iter()
            .find(|(_, definition)| definition.key.id == source.id)
        else {
            return false;
        };
        let Some(open) = anchor.map(EventTime::as_millis) else {
            return false;
        };
        let end = open + timeframe.millis();
        let swing_open = sweep.level.swing_time.as_millis();
        let swing_end = swing_open + sweep.timeframe.millis();
        let swing_side = match sweep.level.side {
            Side::High => crate::location::LevelSide::High,
            Side::Low => crate::location::LevelSide::Low,
        };
        side == Some(swing_side)
            && price == sweep.level.price
            && ((open <= swing_open && swing_end <= end)
                || (swing_open <= open && end <= swing_end))
    }

    #[test]
    fn an_sfp_context_leaves_out_its_swing_on_every_structure_timeframe() {
        // ADR-053 D4: a swing bar of another structure timeframe at the
        // swept price and side, nested in the swept swing's bar or holding
        // it, marks the same extreme, so the context leaves it out too. On
        // the candidate golden tape the registry holds such a level at the
        // anchor for many SFPs, so the test is not vacuous: 202 of 515, 104
        // of them with a kind the candidate gate admits.
        let mut engine = MarketStateEngine::new();
        let (mut sfps, mut other_timeframe, mut gate_kind) = (0, 0, 0);
        for event in crate::candidate::tests::golden_tape() {
            engine.apply(&event).unwrap();
            for fact in engine.trigger_events() {
                let TriggerEvidence::Sfp(sweep) = fact.evidence else {
                    continue;
                };
                sfps += 1;
                let registry = engine.state().location.levels.ready().unwrap();
                let other: Vec<LevelKind> = registry
                    .levels()
                    .iter()
                    .filter(|level| {
                        level.source != sweep.feature
                            && zone_contains(level, fact.anchor())
                            && of_swept_swing(
                                &sweep,
                                (level.side, level.source, level.anchor, level.price),
                            )
                    })
                    .map(|level| level.kind)
                    .collect();
                if !other.is_empty() {
                    other_timeframe += 1;
                }
                if other.iter().any(|kind| {
                    matches!(
                        kind,
                        LevelKind::StructuralHigh
                            | LevelKind::StructuralLow
                            | LevelKind::SfpRejectionZone
                    )
                }) {
                    gate_kind += 1;
                }
                let levels = fact.context.levels.ready().unwrap();
                let left: Vec<&crate::candidate::ContextLevel> = levels
                    .levels
                    .iter()
                    .filter(|level| {
                        of_swept_swing(
                            &sweep,
                            (level.side, level.source, level.anchor, level.price),
                        )
                    })
                    .collect();
                assert!(left.is_empty(), "{fact}: {left:?}");
            }
        }
        // ADR-053 D4 quotes these counts.
        assert_eq!((sfps, other_timeframe, gate_kind), (515, 202, 104));
    }

    /// The brief §11 tape: day 0 as [`Tape::prior_day`]; on day 1 one trade
    /// a minute in the middle of value, but a 15m swing high exactly at the
    /// prior day's VAH on bar 16 (minute 247). Bar 20 probes above value
    /// from minute 301 to 313, which also sweeps the swing, and its last
    /// minute closes back inside. The trade 1 s into minute 315 closes that
    /// minute and bar 20: one event fails the probe and resolves the sweep
    /// as an SFP. Returns the tape and VAH.
    fn at_value_edge() -> (Vec<MarketEvent>, Price) {
        let (val, vah) = value_area();
        let middle = Price::from_units((val.units() + vah.units()) / 2);
        let beyond = Price::from_units(vah.units() + 100 * SCALE);
        let mut path = vec![middle; 316];
        path[247] = vah;
        for price in &mut path[301..314] {
            *price = beyond;
        }
        (auction_tape(&path), vah)
    }

    #[test]
    fn one_bias_and_location_meet_two_families() {
        // Brief §11, ADR-053 D2–D4: the same bias and location, tested
        // against two families in one event, in emission order.
        let (events, vah) = at_value_edge();
        let fired = triggers_of(&events);
        assert_eq!(
            families(&fired),
            [TriggerFamily::Sfp, TriggerFamily::FailedAuction]
        );
        let [(sfp_at, sfp), (auction_at, auction)] = fired.as_slice() else {
            unreachable!()
        };
        assert_eq!(sfp_at, auction_at);
        assert_eq!(*sfp_at, events.len() - 1);
        assert_eq!(sfp.feature, catalog::TRIGGER_SFP_15M_V1.key);
        assert_eq!(sfp.bias, auction.bias);
        assert_eq!((sfp.anchor(), auction.anchor()), (vah, vah));
        let prior_vah = |fact: &TriggerEvent| {
            fact.context
                .levels
                .ready()
                .unwrap()
                .levels
                .iter()
                .find(|level| {
                    level.kind == LevelKind::Vah
                        && level.source == catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key
                })
                .copied()
        };
        let level = prior_vah(sfp).expect("the SFP context lists the prior day's VAH");
        assert_eq!(prior_vah(auction), Some(level));
        assert_eq!(level.price, vah);
    }

    #[test]
    fn the_bias_gates_nothing() {
        // ADR-053 D3: two states that differ only in the bias inputs give
        // the same triggers, apart from the snapshot.
        let (events, _) = at_value_edge();
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        let state = engine.state();
        let detect = |regime: &FeatureValue<Regime>, candidates: &Candidates| {
            let mut triggers = Triggers::new();
            let mut fired = Vec::new();
            TriggerTracker::new().detect(
                engine.structure_events(),
                engine.location_events(),
                Inputs {
                    regime,
                    structure: &state.structure,
                    location: &state.location,
                    candidates,
                },
                &mut triggers,
                &mut fired,
            );
            (triggers, fired)
        };
        let (triggers, fired) = detect(&state.regime, &state.candidates);
        assert_eq!(fired.len(), 2);
        let percentile = AtrPercentile::new(90.0).unwrap();
        let regime = FeatureValue::Ready(Regime {
            atr_percentile: percentile,
            label: percentile.label(),
            feature: catalog::VOLATILITY_REGIME_1H_V1.key,
        });
        let mut candidates = state.candidates.clone();
        let anchor = |price: i64| SwingAnchor {
            swing_time: t(0),
            known_at: t(1),
            price: usdt(price),
            time: t(2),
            cvd: Qty::from_units(0),
            gaps: 0,
            coverage: crate::bars::Coverage::default(),
        };
        candidates.divergences_mut(Timeframe::H4).unwrap().cvd = FeatureValue::Ready(Divergence {
            feature: catalog::CANDIDATE_DIVERGENCE_CVD_4H_V1.key,
            high: Some(CvdComparison {
                pair: SwingPair {
                    side: Side::High,
                    previous: anchor(60_000),
                    current: anchor(61_000),
                    price_change: usdt(1_000),
                    cvd_change: Qty::from_units(0),
                    gaps_between: 0,
                    span_ms: 2,
                    coverage: crate::bars::Coverage::default(),
                },
                divergent: false,
            }),
            low: None,
        });
        let (other_triggers, other_fired) = detect(&regime, &candidates);
        assert_ne!(other_triggers.bias(), triggers.bias());
        assert_eq!(other_triggers.auction, triggers.auction);
        assert_eq!(other_triggers.sfp, triggers.sfp);
        assert_eq!(other_fired.len(), fired.len());
        for (fact, other) in fired.iter().zip(&other_fired) {
            assert_ne!(fact.bias, other.bias);
            let mut other = other.clone();
            other.bias = fact.bias;
            assert_eq!(*fact, other);
        }
    }

    #[test]
    fn a_rejected_event_leaves_the_triggers_and_their_facts() {
        let (events, _) = at_value_edge();
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        let facts = engine.trigger_events().to_vec();
        assert_eq!(facts.len(), 2);
        let triggers = engine.state().triggers.clone();
        // A trade whose volume overflows the developing bars.
        let last = events.last().unwrap().time().as_millis();
        let overflow = MarketEvent::Trade(Trade {
            time: t(last + 1),
            trade_id: 100_000,
            price: usdt(60_500),
            qty: Qty::from_units(i64::MAX),
            aggressor: Aggressor::Buy,
        });
        assert_eq!(
            engine.apply(&overflow),
            Err(StateError::Overflow {
                event: overflow.canonical_key()
            })
        );
        assert_eq!(engine.trigger_events(), facts.as_slice());
        assert_eq!(engine.state().triggers, triggers);
    }

    #[test]
    fn the_same_tape_gives_the_same_triggers_and_hashes() {
        let replay = |events: &[MarketEvent]| {
            let mut engine = MarketStateEngine::new();
            let mut facts = Vec::new();
            let mut hashes = Vec::new();
            for event in events {
                engine.apply(event).unwrap();
                facts.extend(engine.trigger_events().iter().map(ToString::to_string));
                hashes.push(engine.state().state_hash());
            }
            (facts, hashes)
        };
        let tape = crate::location::tests::walk_tape(0x6d69_6500_0000_0024, 4);
        let (facts, hashes) = replay(&tape);
        assert!(!facts.is_empty());
        assert_eq!(replay(&tape), (facts, hashes));
    }

    #[test]
    fn the_location_levels_v1_engine_still_computes_triggers() {
        // ADR-053 D4: no trigger declares a `location.levels` input, so the
        // `location.levels@1` selection keeps its closure (ADR-044 D10), and
        // its contexts name the registry they read.
        for definition in trigger_definitions() {
            assert!(
                definition.inputs.iter().all(|input| !matches!(
                    input,
                    Input::Feature(key) if key.id.as_str() == "location.levels"
                )),
                "{}",
                definition.key
            );
        }
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        assert!(
            features
                .definitions()
                .any(|definition| definition.key == catalog::TRIGGER_SFP_15M_V1.key)
        );
        let mut engine = MarketStateEngine::with_features(features.clone()).unwrap();
        assert_eq!(engine.state().feature_set, features.version());
        let (events, _) = at_value_edge();
        let fired = run(&mut engine, &events);
        assert_eq!(fired.len(), 2);
        for (_, fact) in &fired {
            assert_eq!(
                fact.context.levels.ready().unwrap().feature,
                catalog::LOCATION_LEVELS_V1.key
            );
        }
    }

    #[test]
    fn trigger_family_codes_names_and_order_are_frozen() {
        let names: Vec<String> = TriggerFamily::ALL.iter().map(ToString::to_string).collect();
        assert_eq!(
            names,
            [
                "failed_auction",
                "sfp",
                "absorption",
                "exhaustion",
                "effort_vs_result",
                "cvd_divergence",
                "oi_expansion_rejection",
                "failed_reclaim",
                "breakout_acceptance"
            ]
        );
        for (family, code) in TriggerFamily::ALL.into_iter().zip(0_u8..) {
            let mut expected = Fingerprinter::new();
            expected.write_u8(code);
            let mut f = Fingerprinter::new();
            family.encode(&mut f);
            assert_eq!(f.finish(), expected.finish(), "{family}");
        }
    }

    /// Every bias and trigger definition of the catalog.
    fn trigger_definitions() -> Vec<&'static FeatureDefinition> {
        let mut definitions: Vec<&'static FeatureDefinition> = vec![&catalog::BIAS_CONTEXT_V1];
        definitions.extend(AUCTION_TRIGGERS.map(|(_, definition)| definition));
        definitions.extend(catalog::TRIGGER_SFP.map(|(_, definition)| definition));
        definitions
    }

    /// Whether a value changed: the bias, each auction id, each SFP id, in
    /// [`trigger_definitions`] order.
    fn changes(before: &Triggers, after: &Triggers) -> Vec<(FeatureKey, Option<String>)> {
        let mut changes = vec![(
            catalog::BIAS_CONTEXT_V1.key,
            (before.bias() != after.bias()).then(|| after.bias().to_string()),
        )];
        for ((old, new), (_, definition)) in before
            .auction
            .iter()
            .zip(&after.auction)
            .zip(AUCTION_TRIGGERS)
        {
            changes.push((definition.key, (old != new).then(|| shown(new))));
        }
        for ((old, new), (_, definition)) in
            before.sfp.iter().zip(&after.sfp).zip(catalog::TRIGGER_SFP)
        {
            changes.push((definition.key, (old != new).then(|| shown(new))));
        }
        changes
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

    /// The golden lines of every bias and trigger id on the candidate
    /// golden tape, in tape order: `<event time> value <value>` whenever a
    /// value changes, and `<event time> <fact>` for each trigger event.
    /// Replayed once and shared by the golden tests (a test-only cache:
    /// the tape takes seconds to replay in a debug build).
    fn golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| replay_golden(crate::candidate::tests::golden_tape()))
    }

    /// The golden lines of the auction golden tape, as [`golden_lines`]:
    /// the location golden tape, on which every auction state occurs
    /// (ADR-044), so every auction family fires.
    fn auction_golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| {
            replay_golden(crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7))
        })
    }

    /// Replays `tape` into its golden lines ([`golden_lines`]).
    fn replay_golden(tape: Vec<MarketEvent>) -> Vec<(FeatureKey, String)> {
        {
            let mut engine = MarketStateEngine::new();
            let mut lines = Vec::new();
            let mut before = engine.state().triggers.clone();
            for event in tape {
                engine.apply(&event).unwrap();
                check_values(&engine);
                let time = event.time();
                let after = &engine.state().triggers;
                for (key, change) in changes(&before, after) {
                    if let Some(value) = change {
                        lines.push((key, format!("{time} value {value}")));
                    }
                }
                for fact in engine.trigger_events() {
                    lines.push((fact.feature, format!("{time} {fact}")));
                }
                before = after.clone();
            }
            lines
        }
    }

    /// The golden lines `feature` is pinned on: the auction golden tape for
    /// the auction ids, the candidate golden tape for the rest.
    fn golden_of(feature: &FeatureDefinition) -> &'static [(FeatureKey, String)] {
        if AUCTION_TRIGGERS
            .iter()
            .any(|(_, definition)| definition.key == feature.key)
        {
            auction_golden_lines()
        } else {
            golden_lines()
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

    /// The golden lines of `key`, and the line a golden test shows: its
    /// first fact line, or its first value line without facts.
    fn own_lines(
        lines: &'static [(FeatureKey, String)],
        key: FeatureKey,
    ) -> (Vec<&'static str>, &'static str) {
        let own: Vec<&str> = lines
            .iter()
            .filter(|(feature, _)| *feature == key)
            .map(|(_, line)| line.as_str())
            .collect();
        let first = own
            .iter()
            .find(|line| !line.contains(" value "))
            .or(own.first())
            .copied()
            .unwrap_or_default();
        (own, first)
    }

    /// Checks the golden of `feature`: the count of its lines, the line
    /// [`own_lines`] shows verbatim, and the digest of all of them.
    fn assert_golden(feature: &FeatureDefinition, count: usize, first: &str, all: &str) {
        let (own, shown) = own_lines(golden_of(feature), feature.key);
        assert_eq!(shown, first, "{}", feature.key);
        assert_eq!(
            (own.len(), digest(&own).as_str()),
            (count, all),
            "{}",
            feature.key
        );
    }

    #[test]
    fn every_trigger_id_but_sfp_1d_fires_on_its_golden_tape() {
        // The goldens are not vacuous. The 45-day tape holds no SFP of a
        // daily swing; the synthetic SFP test fires that id.
        for definition in trigger_definitions().into_iter().skip(1) {
            let (own, _) = own_lines(golden_of(definition), definition.key);
            let facts = own.iter().filter(|line| !line.contains(" value ")).count();
            if definition.key == catalog::TRIGGER_SFP_1D_V1.key {
                assert_eq!(facts, 0);
            } else {
                assert!(facts > 0, "{}", definition.key);
            }
        }
    }

    #[test]
    #[ignore = "prints the golden lines: cargo test -p mie-domain print_trigger_golden -- --ignored --nocapture"]
    fn print_trigger_golden() {
        for definition in trigger_definitions() {
            let (own, first) = own_lines(golden_of(definition), definition.key);
            let facts = own.iter().filter(|line| !line.contains(" value ")).count();
            println!(
                "{} {} {} facts={facts}\n{first}",
                definition.key,
                own.len(),
                digest(&own)
            );
        }
    }

    #[test]
    fn no_line_names_a_direction_or_an_execution_meaning() {
        // ADR-053 D1: triggers and the bias are facts, never an instruction
        // (ADR-010, ADR-012, ADR-023).
        let forbidden = [
            "bull",
            "bear",
            "long",
            "short",
            "signal",
            "reversal",
            "continuation",
            "entry",
        ];
        let (edge, _) = at_value_edge();
        let mut samples: Vec<String> = triggers_of(&edge)
            .into_iter()
            .map(|(_, fact)| fact.to_string())
            .collect();
        let mut path = minutes(INSIDE, 10);
        path.extend(minutes(ABOVE, 61));
        path.extend(minutes(INSIDE, 3));
        path.extend(minutes(ABOVE, 2));
        samples.extend(
            triggers_of(&auction_tape(&path))
                .into_iter()
                .map(|(_, fact)| fact.to_string()),
        );
        let mut lines: Vec<&str> = golden_lines()
            .iter()
            .chain(auction_golden_lines())
            .map(|(_, line)| line.as_str())
            .collect();
        lines.extend(samples.iter().map(String::as_str));
        for family in [
            TriggerFamily::FailedAuction,
            TriggerFamily::FailedReclaim,
            TriggerFamily::BreakoutAcceptance,
            TriggerFamily::Sfp,
        ] {
            let prefix = format!("{family} evidence=");
            assert!(lines.iter().any(|line| line.contains(&prefix)), "{family}");
        }
        for line in lines {
            let lower = line.to_lowercase();
            for word in forbidden {
                assert!(!lower.contains(word), "{word}: {line}");
            }
        }
    }

    #[test]
    fn golden_bias_context_v1() {
        assert_golden(
            &catalog::BIAS_CONTEXT_V1,
            1595,
            GOLDEN_BIAS,
            "bd3a92d625a45518",
        );
    }

    #[test]
    fn golden_trigger_failed_auction_v1() {
        assert_golden(
            &catalog::TRIGGER_FAILED_AUCTION_V1,
            24,
            GOLDEN_FAILED_AUCTION,
            "dc637bec0ad21ec5",
        );
    }

    #[test]
    fn golden_trigger_failed_reclaim_v1() {
        assert_golden(
            &catalog::TRIGGER_FAILED_RECLAIM_V1,
            14,
            GOLDEN_FAILED_RECLAIM,
            "c82c3a60e61183ca",
        );
    }

    #[test]
    fn golden_trigger_breakout_acceptance_v1() {
        assert_golden(
            &catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1,
            48,
            GOLDEN_BREAKOUT_ACCEPTANCE,
            "ef56b07697b5deb9",
        );
    }

    #[test]
    fn golden_trigger_sfp_15m_v1() {
        assert_golden(
            &catalog::TRIGGER_SFP_15M_V1,
            756,
            GOLDEN_SFP_15M,
            "25f83224a86821cb",
        );
    }

    #[test]
    fn golden_trigger_sfp_1h_v1() {
        assert_golden(
            &catalog::TRIGGER_SFP_1H_V1,
            191,
            GOLDEN_SFP_1H,
            "ed614487d89208f9",
        );
    }

    #[test]
    fn golden_trigger_sfp_4h_v1() {
        assert_golden(
            &catalog::TRIGGER_SFP_4H_V1,
            47,
            GOLDEN_SFP_4H,
            "e2a97b7e4f2cff19",
        );
    }

    #[test]
    fn golden_trigger_sfp_1d_v1() {
        assert_golden(
            &catalog::TRIGGER_SFP_1D_V1,
            7,
            GOLDEN_SFP_1D,
            "62c0bc4b8a59f21c",
        );
    }

    const GOLDEN_BIAS: &str = "54007615ms value bias regime=warming 15m=warming 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_FAILED_AUCTION: &str = "178028180ms failed_auction evidence=[failed_breakout ref=86400000ms val=61450.00000000 vah=62040.00000000 close=61997.13000000 at=in side=above a=in/start failed=above:44/1 complete location.auction.prior_day@1] trigger.failed_auction@1 ctx=62040.00000000 levels=hvn@62005.00000000[62000.00000000,62010.00000000],prior_sweep:high@62029.35000000,prior_sweep:high@62029.35000000,vah@62040.00000000[62030.00000000,62040.00000000],vwap@62053.90800666,lvn@62065.00000000[62060.00000000,62070.00000000] location.levels@2 auction=failed_breakout bias regime=warming 15m=[high=higher@157500000ms low=higher@169200000ms] 1h=[high=higher@154800000ms low=higher@133200000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_FAILED_RECLAIM: &str = "412174301ms failed_reclaim evidence=[failed_reclaim ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62061.91000000 at=above side=above a=above/accepted failed=in:40/1 complete location.auction.prior_day@1] trigger.failed_reclaim@1 ctx=62020.00000000 levels=vah@62020.00000000[62010.00000000,62020.00000000],prior_sweep:low@62018.22000000,prior_sweep:low@62018.22000000,prior_sweep:high@62029.35000000 location.levels@2 auction=failed_reclaim bias regime=warming 15m=[high=lower@400500000ms low=lower@396000000ms] 1h=[high=higher@381600000ms low=higher@360000000ms] 4h=[high=higher@216000000ms low=higher@259200000ms] 1d=warming bias.context@1";

    const GOLDEN_BREAKOUT_ACCEPTANCE: &str = "105154318ms breakout_acceptance evidence=[acceptance ref=0ms val=61610.00000000 vah=62040.00000000 close=61273.97000000 at=below side=below a=below/accepted partial_start location.auction.prior_day@1] trigger.breakout_acceptance@1 ctx=61610.00000000 levels=lvn@61585.00000000[61580.00000000,61590.00000000],val@61610.00000000[61610.00000000,61620.00000000] location.levels@2 auction=acceptance bias regime=warming 15m=[high=higher@94500000ms low=higher@85500000ms] 1h=[high=- low=-] 4h=warming 1d=warming bias.context@1";

    const GOLDEN_SFP_15M: &str = "62161380ms sfp evidence=[15m low level=61969.20000000 bar=55800000ms confirmed=59400000ms touches=2 at=61514129ms price=61966.54000000 extreme=61966.54000000 bars=1 sfp resolved=62100000ms complete structure.levels.15m@1] trigger.sfp.15m@1 ctx=61969.20000000 levels=vwap@61984.17149090 location.levels@2 auction=warming bias regime=warming 15m=[high=- low=-] 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_SFP_1H: &str = "97260667ms sfp evidence=[1h low level=61900.09000000 bar=75600000ms confirmed=90000000ms touches=0 at=93129730ms price=61897.65000000 extreme=61882.07000000 bars=2 sfp resolved=97200000ms complete structure.levels.1h@1] trigger.sfp.1h@1 ctx=61900.09000000 levels=vwap@61914.46220263,hvn@61925.00000000[61920.00000000,61930.00000000],prior_sweep:low@61920.27000000,val@61930.00000000[61930.00000000,61940.00000000] location.levels@2 auction=breakout bias regime=warming 15m=[high=higher@87300000ms low=higher@84600000ms] 1h=[high=- low=-] 4h=warming 1d=warming bias.context@1";

    const GOLDEN_SFP_4H: &str = "288021457ms sfp evidence=[4h high level=62057.32000000 bar=172800000ms confirmed=230400000ms touches=2 at=259693621ms price=62057.71000000 extreme=62088.48000000 bars=2 sfp resolved=288000000ms complete structure.levels.4h@1] trigger.sfp.4h@1 ctx=62057.32000000 levels=poc@62025.00000000[62020.00000000,62030.00000000],hvn@62025.00000000[62020.00000000,62030.00000000],prior_sweep:high@62024.72000000,prior_sweep:high@62024.72000000,sfp_zone:high@62024.72000000[62024.72000000,62027.66000000],sfp_zone:high@62024.72000000[62024.72000000,62027.66000000],structural_low:low@62025.15000000,prior_sweep:high@62027.66000000,vah@62040.00000000[62030.00000000,62040.00000000],prior_sweep:low@62036.28000000,prior_sweep:high@62042.67000000,prior_sweep:high@62042.70000000,sfp_zone:high@62042.70000000[62042.70000000,62044.63000000],prior_sweep:high@62049.17000000,sfp_zone:high@62049.17000000[62049.17000000,62051.12000000],prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],prior_sweep:high@62051.12000000,structural_high:high@62062.20000000,vwap@62063.50291131,prior_sweep:low@62065.69000000,prior_sweep:low@62066.40000000,prior_sweep:high@62081.53000000,sfp_zone:high@62081.53000000[62081.53000000,62084.18000000],prior_sweep:high@62081.62000000,sfp_zone:high@62081.62000000[62081.62000000,62084.18000000],prior_sweep:high@62084.18000000,sfp_zone:high@62084.18000000[62084.18000000,62088.48000000],structural_high:high@62088.48000000,structural_high:high@62088.48000000 location.levels@2 auction=unavailable bias regime=warming 15m=[high=lower@282600000ms low=lower@283500000ms] 1h=[high=higher@270000000ms low=lower@230400000ms] 4h=[high=- low=higher@230400000ms] 1d=warming bias.context@1";

    const GOLDEN_SFP_1D: &str = "86450066ms value warming 1/7";
}
