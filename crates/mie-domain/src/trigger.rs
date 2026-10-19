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
//!     order-flow candidates `candidate.*@1`, each the candidate fact at a
//!     defined location: the context the rule reads lists a level of an
//!     admitted kind (ADR-053 D4, D6, `candidate`);
//!   - OI expansion followed by rejection on the SFP of each structure
//!     timeframe and on the failed auction: a strictly higher
//!     `derivatives.oi.5m@1` point at the rejection than at the start of
//!     the excursion, each read at the boundary [`OI_LAG_MS`] before its
//!     event (ADR-053 D7, `oi`).
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
mod candidate;
mod oi;
mod structure;

pub use bias::{Bias, StepAt, SwingSequence, SwingStep};
pub use oi::{OI_LAG_MS, OiExpansion, OiRejection};

use crate::bars::Timeframe;
use crate::candidate::absorption::Absorption;
use crate::candidate::divergence::CvdComparison;
use crate::candidate::effort::{EffortResult, Exhaustion};
use crate::candidate::{CandidateEvent, Candidates, LocationContext};
use crate::derivatives::{Derivatives, OiGridPoint};
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
use candidate::Slot;
use oi::{OiPoints, ProbeStart, SweepStart};
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
    /// `trigger.cvd_divergence.<tf>@1`, in [`STRUCTURE_TIMEFRAMES`] order
    /// (ADR-053 D6).
    cvd_divergence: [FeatureValue<TriggerValue<CvdComparison>>; 4],
    /// `trigger.effort_result.<w>@1`, in
    /// [`catalog::TRIGGER_EFFORT_RESULT`] order.
    effort_result: [FeatureValue<TriggerValue<EffortResult>>; 3],
    /// `trigger.exhaustion@1`.
    exhaustion: FeatureValue<TriggerValue<Exhaustion>>,
    /// `trigger.absorption.<w>@1`, in [`catalog::TRIGGER_ABSORPTION`]
    /// order.
    absorption: [FeatureValue<TriggerValue<Absorption>>; 3],
    /// `trigger.oi_expansion_rejection.<tf>@1`, in
    /// [`STRUCTURE_TIMEFRAMES`] order (ADR-053 D7).
    oi_expansion_rejection: [FeatureValue<TriggerValue<OiExpansion>>; 4],
    /// `trigger.oi_expansion_rejection.prior_day@1` (ADR-053 D7).
    oi_expansion_rejection_prior_day: FeatureValue<TriggerValue<OiExpansion>>,
}

impl StateEncode for Triggers {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            bias,
            auction,
            sfp,
            cvd_divergence,
            effort_result,
            exhaustion,
            absorption,
            oi_expansion_rejection,
            oi_expansion_rejection_prior_day,
        } = self;
        bias.encode(f);
        auction.encode(f);
        sfp.encode(f);
        cvd_divergence.encode(f);
        effort_result.encode(f);
        exhaustion.encode(f);
        absorption.encode(f);
        oi_expansion_rejection.encode(f);
        oi_expansion_rejection_prior_day.encode(f);
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
        let derivatives = Derivatives::new();
        TriggerTracker::new().values(&Inputs {
            regime: &regime,
            structure: &structure,
            location: &location,
            candidates: &candidates,
            oi_5m: &derivatives.oi_5m,
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

    /// `trigger.cvd_divergence.<tf>@1` of `timeframe`, if it is a
    /// structure timeframe.
    pub fn cvd_divergence(
        &self,
        timeframe: Timeframe,
    ) -> Option<&FeatureValue<TriggerValue<CvdComparison>>> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.cvd_divergence[index])
    }

    /// `trigger.cvd_divergence.<tf>@1` of every structure timeframe,
    /// shortest first.
    pub fn iter_cvd_divergence(
        &self,
    ) -> impl Iterator<Item = (Timeframe, &FeatureValue<TriggerValue<CvdComparison>>)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.cvd_divergence)
    }

    /// `trigger.effort_result.<w>@1` of the window of length `timeframe`,
    /// if it is an effort window.
    pub fn effort_result(
        &self,
        timeframe: Timeframe,
    ) -> Option<&FeatureValue<TriggerValue<EffortResult>>> {
        window_position(timeframe).map(|index| &self.effort_result[index])
    }

    /// `trigger.exhaustion@1`.
    pub fn exhaustion(&self) -> &FeatureValue<TriggerValue<Exhaustion>> {
        &self.exhaustion
    }

    /// `trigger.absorption.<w>@1` of the window of length `timeframe`, if
    /// it is an effort window.
    pub fn absorption(
        &self,
        timeframe: Timeframe,
    ) -> Option<&FeatureValue<TriggerValue<Absorption>>> {
        window_position(timeframe).map(|index| &self.absorption[index])
    }

    /// The trigger values of the effort windows, shortest first: effort vs
    /// result and absorption, with the window length.
    pub fn iter_windows(
        &self,
    ) -> impl Iterator<
        Item = (
            Timeframe,
            &FeatureValue<TriggerValue<EffortResult>>,
            &FeatureValue<TriggerValue<Absorption>>,
        ),
    > {
        catalog::TRIGGER_EFFORT_RESULT
            .into_iter()
            .zip(&self.effort_result)
            .zip(&self.absorption)
            .map(|(((timeframe, _), effort), absorption)| (timeframe, effort, absorption))
    }

    /// `trigger.oi_expansion_rejection.<tf>@1` of `timeframe`, if it is a
    /// structure timeframe.
    pub fn oi_expansion_rejection(
        &self,
        timeframe: Timeframe,
    ) -> Option<&FeatureValue<TriggerValue<OiExpansion>>> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.oi_expansion_rejection[index])
    }

    /// `trigger.oi_expansion_rejection.<tf>@1` of every structure
    /// timeframe, shortest first.
    pub fn iter_oi_expansion_rejection(
        &self,
    ) -> impl Iterator<Item = (Timeframe, &FeatureValue<TriggerValue<OiExpansion>>)> {
        STRUCTURE_TIMEFRAMES
            .into_iter()
            .zip(&self.oi_expansion_rejection)
    }

    /// `trigger.oi_expansion_rejection.prior_day@1`.
    pub fn oi_expansion_rejection_prior_day(&self) -> &FeatureValue<TriggerValue<OiExpansion>> {
        &self.oi_expansion_rejection_prior_day
    }

    /// Whether `self` and `other` agree in every value that does not read
    /// the level registry: the bias and the auction, SFP and OI-expansion
    /// trigger values (#96 rule I). The candidate trigger values depend on
    /// the selected registry by design, since their location gate reads it
    /// (ADR-053 D4), so they are not compared. The OI-expansion values read
    /// no registry (their facts and points do not, only their events'
    /// contexts, which are not values), and comparing them keeps a future
    /// registry read in ADR-053 D7 from going unseen. Exhaustive, so a new
    /// value does not compile until it is placed on one side.
    pub fn equal_but_registry(&self, other: &Self) -> bool {
        let Self {
            bias,
            auction,
            sfp,
            cvd_divergence: _,
            effort_result: _,
            exhaustion: _,
            absorption: _,
            oi_expansion_rejection,
            oi_expansion_rejection_prior_day,
        } = self;
        *bias == other.bias
            && *auction == other.auction
            && *sfp == other.sfp
            && *oi_expansion_rejection == other.oi_expansion_rejection
            && *oi_expansion_rejection_prior_day == other.oi_expansion_rejection_prior_day
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

    /// The CVD divergence trigger values, for tests that build a state by
    /// hand.
    #[cfg(test)]
    pub(crate) fn cvd_divergence_mut(
        &mut self,
    ) -> &mut [FeatureValue<TriggerValue<CvdComparison>>; 4] {
        &mut self.cvd_divergence
    }

    /// The effort-vs-result trigger values, for tests that build a state by
    /// hand.
    #[cfg(test)]
    pub(crate) fn effort_result_mut(
        &mut self,
    ) -> &mut [FeatureValue<TriggerValue<EffortResult>>; 3] {
        &mut self.effort_result
    }

    /// The exhaustion trigger value, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn exhaustion_mut(&mut self) -> &mut FeatureValue<TriggerValue<Exhaustion>> {
        &mut self.exhaustion
    }

    /// The absorption trigger values, for tests that build a state by hand.
    #[cfg(test)]
    pub(crate) fn absorption_mut(&mut self) -> &mut [FeatureValue<TriggerValue<Absorption>>; 3] {
        &mut self.absorption
    }

    /// The OI-expansion trigger values of the SFP timeframes, for tests that
    /// build a state by hand.
    #[cfg(test)]
    pub(crate) fn oi_expansion_rejection_mut(
        &mut self,
    ) -> &mut [FeatureValue<TriggerValue<OiExpansion>>; 4] {
        &mut self.oi_expansion_rejection
    }

    /// The OI-expansion trigger value of the failed auction, for tests that
    /// build a state by hand.
    #[cfg(test)]
    pub(crate) fn oi_expansion_rejection_prior_day_mut(
        &mut self,
    ) -> &mut FeatureValue<TriggerValue<OiExpansion>> {
        &mut self.oi_expansion_rejection_prior_day
    }
}

/// The position of the effort window of length `timeframe` in
/// [`catalog::TRIGGER_EFFORT_RESULT`] (and [`catalog::TRIGGER_ABSORPTION`]).
fn window_position(timeframe: Timeframe) -> Option<usize> {
    catalog::TRIGGER_EFFORT_RESULT
        .iter()
        .position(|(candidate, _)| *candidate == timeframe)
}

/// The fact a trigger was built on (ADR-053 D1): its input feature's own
/// hashed form.
///
/// `Display` prints the fact.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TriggerEvidence {
    /// A `location.auction.prior_day@1` status: the status of the close
    /// whose label changed, against the reference it was classified under.
    Auction(AuctionStatus),
    /// A `structure.levels.<tf>@1` sweep resolved as an SFP.
    Sfp(Sweep),
    /// A `candidate.*@1` fact (ADR-053 D6): the full candidate event, with
    /// its own ADR-051 D3 context. Boxed: it is several times the size of
    /// the other facts.
    Candidate(Box<CandidateEvent>),
    /// An open-interest expansion judged at an SFP or a failed auction
    /// (ADR-053 D7): the rejection, both `derivatives.oi.5m@1` points and
    /// the change.
    OiExpansion(OiExpansion),
}

impl fmt::Display for TriggerEvidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Auction(status) => write!(f, "{status}"),
            Self::Sfp(sweep) => write!(f, "{} {sweep}", sweep.timeframe),
            Self::Candidate(fact) => write!(f, "{fact}"),
            Self::OiExpansion(expansion) => write!(f, "{expansion}"),
        }
    }
}

/// A trigger fact, in emission order (ADR-053 D2): per structure fact of
/// the event, in its order, the SFP trigger, followed by its OI-expansion
/// trigger if any; then per location fact, in its order, the auction
/// trigger, a failed auction followed by its OI-expansion trigger if any;
/// then per candidate fact, in its order, the candidate trigger.
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
    /// `derivatives.oi.5m@1`, for the OI-expansion points.
    pub(crate) oi_5m: &'a FeatureValue<OiGridPoint>,
}

/// The trigger engine state (ADR-053 D2): the last trigger of each id, and
/// the OI-expansion records (ADR-053 D7). Engine state, updated in place
/// and rebuilt by replay; the Market State exposes [`Triggers`], which
/// follows each id's input validity around it.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TriggerTracker {
    /// The auction triggers, in `AUCTION_TRIGGERS` order.
    auction: [Option<Trigger<AuctionStatus>>; 3],
    /// The SFP triggers, in [`STRUCTURE_TIMEFRAMES`] order.
    sfp: [Option<Trigger<Sweep>>; 4],
    /// The CVD divergence triggers, in [`STRUCTURE_TIMEFRAMES`] order.
    cvd_divergence: [Option<Trigger<CvdComparison>>; 4],
    /// The effort-vs-result triggers, in
    /// [`catalog::TRIGGER_EFFORT_RESULT`] order.
    effort_result: [Option<Trigger<EffortResult>>; 3],
    /// The exhaustion trigger.
    exhaustion: Option<Trigger<Exhaustion>>,
    /// The absorption triggers, in [`catalog::TRIGGER_ABSORPTION`] order.
    absorption: [Option<Trigger<Absorption>>; 3],
    /// The OI-expansion triggers of the SFPs, in [`STRUCTURE_TIMEFRAMES`]
    /// order.
    oi_sfp: [Option<Trigger<OiExpansion>>; 4],
    /// The OI-expansion trigger of the failed auction.
    oi_prior_day: Option<Trigger<OiExpansion>>,
    /// The last two ready `derivatives.oi.5m@1` points.
    oi_points: OiPoints,
    /// The start point of each pending sweep that had one, ordered by key:
    /// at most one per pending sweep of the structure registries.
    sweep_starts: Vec<SweepStart>,
    /// The record of the committed probe toward above or below.
    probe_start: Option<ProbeStart>,
}

impl TriggerTracker {
    /// No trigger yet.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// The infallible stage (ADR-053 D2): after the candidate stage has
    /// committed the event, reads the bias and the open-interest point,
    /// turns the event's structure facts, then its location facts, then its
    /// candidate facts, each in their order, into trigger events, updates
    /// the OI-expansion records, and refreshes `triggers`. Reads only those
    /// facts and the committed state; appends the events to `events`.
    ///
    /// ADR-053 D7 on the structure facts: a sweep records the point at its
    /// sweeping trade, a break drops it, and an SFP takes it and emits its
    /// OI-expansion trigger right after its SFP trigger. On the location
    /// facts: a failed auction consults the probe record before the record
    /// follows the committed status after the location facts.
    pub(crate) fn detect(
        &mut self,
        structure_events: &[StructureEvent],
        location_events: &[LocationEvent],
        candidate_events: &[CandidateEvent],
        inputs: Inputs<'_>,
        triggers: &mut Triggers,
        events: &mut Vec<TriggerEvent>,
    ) {
        let bias = Bias::from_parts(inputs.regime, inputs.candidates);
        self.oi_points.observe(inputs.oi_5m);
        for fact in structure_events {
            let start = match fact {
                StructureEvent::Sweep(sweep) => {
                    oi::record_sweep(&mut self.sweep_starts, sweep, &self.oi_points);
                    continue;
                }
                StructureEvent::Break(sweep) => {
                    oi::take_sweep(&mut self.sweep_starts, sweep);
                    continue;
                }
                StructureEvent::Sfp(sweep) => oi::take_sweep(&mut self.sweep_starts, sweep),
                StructureEvent::Swing(_) => continue,
            };
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
            let expansion = start
                .zip(self.oi_points.at(sweep.known_at))
                .and_then(|(start, end)| oi::expansion(OiRejection::Sfp(*sweep), start, end));
            events.push(TriggerEvent {
                feature: definition.key,
                family: TriggerFamily::Sfp,
                known_at: sweep.known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::Sfp(*sweep),
                context: context.clone(),
                bias,
            });
            if let Some(expansion) = expansion {
                let (_, definition) = catalog::TRIGGER_OI_EXPANSION_REJECTION[position];
                self.oi_sfp[position] = Some(Trigger {
                    known_at: sweep.known_at,
                    anchor,
                    evidence: expansion,
                });
                events.push(TriggerEvent {
                    feature: definition.key,
                    family: TriggerFamily::OiExpansionRejection,
                    known_at: sweep.known_at,
                    inputs: definition.inputs,
                    evidence: TriggerEvidence::OiExpansion(expansion),
                    context,
                    bias,
                });
            }
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
            let context = LocationContext::read(anchor, None, inputs.location);
            // Only a failed auction is an OI-expansion rejection.
            let expansion = (family == TriggerFamily::FailedAuction)
                .then(|| ProbeStart::failed(self.probe_start, status))
                .flatten()
                .zip(self.oi_points.at(*known_at))
                .and_then(|(start, end)| {
                    oi::expansion(OiRejection::FailedAuction(*status), start, end)
                });
            events.push(TriggerEvent {
                feature: definition.key,
                family,
                known_at: *known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::Auction(*status),
                context: context.clone(),
                bias,
            });
            if let Some(expansion) = expansion {
                let definition = &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1;
                self.oi_prior_day = Some(Trigger {
                    known_at: *known_at,
                    anchor,
                    evidence: expansion,
                });
                events.push(TriggerEvent {
                    feature: definition.key,
                    family: TriggerFamily::OiExpansionRejection,
                    known_at: *known_at,
                    inputs: definition.inputs,
                    evidence: TriggerEvidence::OiExpansion(expansion),
                    context,
                    bias,
                });
            }
        }
        // The probe record follows the committed status (ADR-053 D7).
        self.probe_start =
            ProbeStart::next(self.probe_start, &inputs.location.auction, &self.oi_points);
        for fact in candidate_events {
            let Some(triggered) = candidate::triggered(fact, inputs.location) else {
                continue;
            };
            let (known_at, anchor) = (fact.time(), triggered.anchor());
            match triggered.slot {
                Slot::CvdDivergence(position, evidence) => {
                    self.cvd_divergence[position] = Some(Trigger {
                        known_at,
                        anchor,
                        evidence,
                    });
                }
                Slot::EffortResult(position, evidence) => {
                    self.effort_result[position] = Some(Trigger {
                        known_at,
                        anchor,
                        evidence,
                    });
                }
                Slot::Exhaustion(evidence) => {
                    self.exhaustion = Some(Trigger {
                        known_at,
                        anchor,
                        evidence,
                    });
                }
                Slot::Absorption(position, evidence) => {
                    self.absorption[position] = Some(Trigger {
                        known_at,
                        anchor,
                        evidence,
                    });
                }
            }
            events.push(TriggerEvent {
                feature: triggered.definition.key,
                family: triggered.family,
                known_at,
                inputs: triggered.definition.inputs,
                evidence: TriggerEvidence::Candidate(Box::new(fact.clone())),
                context: triggered.context,
                bias,
            });
        }
        let Triggers {
            bias: held,
            auction,
            sfp,
            cvd_divergence,
            effort_result,
            exhaustion,
            absorption,
            oi_expansion_rejection,
            oi_expansion_rejection_prior_day,
        } = triggers;
        *held = bias;
        *auction = self.auction_values(inputs.location);
        *sfp = self.sfp_values(inputs.structure);
        *cvd_divergence = self.cvd_divergence_values(inputs.candidates);
        *effort_result = self.effort_result_values(inputs.candidates);
        *exhaustion = self.exhaustion_value(inputs.candidates);
        *absorption = self.absorption_values(inputs.candidates);
        *oi_expansion_rejection = self.oi_sfp_values(inputs.structure);
        *oi_expansion_rejection_prior_day = self.oi_prior_day_value(inputs.location);
    }

    /// Every value as `inputs` stand, with the last triggers held.
    fn values(&self, inputs: &Inputs<'_>) -> Triggers {
        Triggers {
            bias: Bias::from_parts(inputs.regime, inputs.candidates),
            auction: self.auction_values(inputs.location),
            sfp: self.sfp_values(inputs.structure),
            cvd_divergence: self.cvd_divergence_values(inputs.candidates),
            effort_result: self.effort_result_values(inputs.candidates),
            exhaustion: self.exhaustion_value(inputs.candidates),
            absorption: self.absorption_values(inputs.candidates),
            oi_expansion_rejection: self.oi_sfp_values(inputs.structure),
            oi_expansion_rejection_prior_day: self.oi_prior_day_value(inputs.location),
        }
    }

    /// The OI-expansion trigger values of the SFPs: each follows its
    /// rejection's fact input, its timeframe's `structure.levels.<tf>@1`,
    /// not the open-interest grid (ADR-053 D1, D7).
    fn oi_sfp_values(
        &self,
        structure: &StructureSet,
    ) -> [FeatureValue<TriggerValue<OiExpansion>>; 4] {
        [0, 1, 2, 3].map(|position| {
            let value = TriggerValue {
                feature: catalog::TRIGGER_OI_EXPANSION_REJECTION[position].1.key,
                last: self.oi_sfp[position],
            };
            match structure.get(STRUCTURE_TIMEFRAMES[position]) {
                Some(timeframe) => following(&timeframe.levels, value),
                // Every structure timeframe has structure.
                None => FeatureValue::Ready(value),
            }
        })
    }

    /// The OI-expansion trigger value of the failed auction: it follows
    /// `location.auction.prior_day@1` (ADR-053 D1, D7).
    fn oi_prior_day_value(
        &self,
        location: &LocationState,
    ) -> FeatureValue<TriggerValue<OiExpansion>> {
        following(
            &location.auction,
            TriggerValue {
                feature: catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key,
                last: self.oi_prior_day,
            },
        )
    }

    /// The OI-expansion records, for the bounded-records test: the sweep
    /// start records and whether a probe record is held.
    #[cfg(test)]
    pub(crate) fn oi_records(&self) -> (usize, bool) {
        (self.sweep_starts.len(), self.probe_start.is_some())
    }

    /// The CVD divergence trigger values: each follows its timeframe's
    /// `candidate.divergence.cvd.<tf>@1` (ADR-053 D1, D6).
    fn cvd_divergence_values(
        &self,
        candidates: &Candidates,
    ) -> [FeatureValue<TriggerValue<CvdComparison>>; 4] {
        [0, 1, 2, 3].map(|position| {
            let (timeframe, definition) = catalog::TRIGGER_CVD_DIVERGENCE[position];
            let value = TriggerValue {
                feature: definition.key,
                last: self.cvd_divergence[position],
            };
            match candidates.divergences(timeframe) {
                Some(divergences) => following(&divergences.cvd, value),
                // Every structure timeframe has divergences.
                None => FeatureValue::Ready(value),
            }
        })
    }

    /// The effort-vs-result trigger values: each follows its window's
    /// `candidate.effort_result.<w>@1` (ADR-053 D1, D6).
    fn effort_result_values(
        &self,
        candidates: &Candidates,
    ) -> [FeatureValue<TriggerValue<EffortResult>>; 3] {
        [0, 1, 2].map(|position| {
            let (timeframe, definition) = catalog::TRIGGER_EFFORT_RESULT[position];
            let value = TriggerValue {
                feature: definition.key,
                last: self.effort_result[position],
            };
            match candidates.effort_result(timeframe) {
                Some(input) => following(input, value),
                // Every effort window has an evaluation.
                None => FeatureValue::Ready(value),
            }
        })
    }

    /// The exhaustion trigger value: it follows `candidate.exhaustion@1`
    /// (ADR-053 D1, D6).
    fn exhaustion_value(&self, candidates: &Candidates) -> FeatureValue<TriggerValue<Exhaustion>> {
        following(
            candidates.exhaustion(),
            TriggerValue {
                feature: catalog::TRIGGER_EXHAUSTION_V1.key,
                last: self.exhaustion,
            },
        )
    }

    /// The absorption trigger values: each follows its window's
    /// `candidate.absorption.<w>@1` (ADR-053 D1, D6).
    fn absorption_values(
        &self,
        candidates: &Candidates,
    ) -> [FeatureValue<TriggerValue<Absorption>>; 3] {
        [0, 1, 2].map(|position| {
            let (timeframe, definition) = catalog::TRIGGER_ABSORPTION[position];
            let value = TriggerValue {
                feature: definition.key,
                last: self.absorption[position],
            };
            match candidates.absorption(timeframe) {
                Some(input) => following(input, value),
                // Every effort window has an absorption value.
                None => FeatureValue::Ready(value),
            }
        })
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
    use crate::candidate::divergence::{Divergence, SwingAnchor, SwingPair};
    use crate::event::samples::t;
    use crate::event::{Aggressor, MarketEvent, OpenInterest, Trade};
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
            check_records(engine);
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
        for (timeframe, value) in triggers.iter_cvd_divergence() {
            let cvd = &state.candidates.divergences(timeframe).unwrap().cvd;
            assert_eq!(validity(value), validity(cvd));
        }
        for ((timeframe, effort, absorption), (_, effort_input, absorption_input)) in
            triggers.iter_windows().zip(state.candidates.iter_windows())
        {
            assert_eq!(validity(effort), validity(effort_input), "{timeframe}");
            assert_eq!(
                validity(absorption),
                validity(absorption_input),
                "{timeframe}"
            );
        }
        assert_eq!(
            validity(triggers.exhaustion()),
            validity(state.candidates.exhaustion())
        );
        // The OI-expansion values follow their rejection's fact input, not
        // the open-interest grid (ADR-053 D7).
        for (timeframe, value) in triggers.iter_oi_expansion_rejection() {
            let levels = &state.structure.get(timeframe).unwrap().levels;
            assert_eq!(validity(value), validity(levels), "{timeframe}");
        }
        assert_eq!(
            validity(triggers.oi_expansion_rejection_prior_day()),
            validity(&state.location.auction)
        );
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
            match &fact.evidence {
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
                                evidence: *status,
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
                            evidence: *sweep,
                        })
                    );
                }
                TriggerEvidence::Candidate(candidate) => check_candidate(triggers, fact, candidate),
                TriggerEvidence::OiExpansion(expansion) => check_oi(triggers, fact, expansion),
            }
        }
        // ADR-053 D2, D7: each OI-expansion trigger directly follows the
        // trigger of its rejection, at the same anchor, context and bias.
        for (index, fact) in facts.iter().enumerate() {
            let TriggerEvidence::OiExpansion(expansion) = &fact.evidence else {
                continue;
            };
            let rejection = &facts[index - 1];
            match (&expansion.rejection, &rejection.evidence) {
                (OiRejection::Sfp(sweep), TriggerEvidence::Sfp(rejected)) => {
                    assert_eq!(sweep, rejected, "{fact}");
                }
                (OiRejection::FailedAuction(status), TriggerEvidence::Auction(rejected)) => {
                    assert_eq!(status, rejected, "{fact}");
                    assert_eq!(rejection.family, TriggerFamily::FailedAuction);
                }
                _ => panic!("{fact} follows {rejection}"),
            }
            assert_eq!(rejection.context, fact.context, "{fact}");
            assert_eq!(rejection.bias, fact.bias, "{fact}");
        }
    }

    /// ADR-053 D7 on one OI-expansion trigger, written from the ADR text:
    /// both points on the boundary the lag before their events, the same
    /// resolution, a later boundary, strictly more open interest and the
    /// exact change; its id's value, ready with its rejection's fact input,
    /// holds the trigger.
    fn check_oi(triggers: &Triggers, fact: &TriggerEvent, expansion: &OiExpansion) {
        assert_eq!(fact.family, TriggerFamily::OiExpansionRejection);
        let lagged = |time: EventTime| {
            let millis = time.as_millis() - 25_000;
            millis - millis.rem_euclid(300_000)
        };
        let value = match &expansion.rejection {
            OiRejection::Sfp(sweep) => {
                assert_eq!(sweep.known_at, fact.known_at);
                assert_eq!(expansion.start.boundary.as_millis(), lagged(sweep.time));
                triggers.oi_expansion_rejection(sweep.timeframe)
            }
            // The start is the failed probe's first close, which the
            // failing status no longer holds; the probe tests check it.
            OiRejection::FailedAuction(status) => {
                assert_eq!(status.failure.unwrap().at, fact.known_at);
                Some(triggers.oi_expansion_rejection_prior_day())
            }
        };
        assert_eq!(expansion.end.boundary.as_millis(), lagged(fact.known_at));
        assert_eq!(expansion.start.resolution_ms, expansion.end.resolution_ms);
        assert!(expansion.start.boundary < expansion.end.boundary);
        assert_eq!(
            i128::from(expansion.change.units()),
            i128::from(expansion.end.open_interest.units())
                - i128::from(expansion.start.open_interest.units())
        );
        assert!(expansion.change.units() > 0);
        match (value.unwrap(), &expansion.rejection) {
            (FeatureValue::Ready(value), _) => {
                assert_eq!(value.feature, fact.feature);
                assert_eq!(
                    value.last,
                    Some(Trigger {
                        known_at: fact.known_at,
                        anchor: fact.anchor(),
                        evidence: *expansion,
                    })
                );
            }
            // At a UTC-day rollover the auction state has already switched
            // (ADR-044 D2), and the value follows it.
            (_, OiRejection::FailedAuction(_)) => {}
            (_, OiRejection::Sfp(_)) => panic!("an SFP resolves on a ready registry: {fact}"),
        }
    }

    /// ADR-053 D7: the OI-expansion records stay bounded — at most one
    /// sweep start per pending sweep of the structure registries.
    fn check_records(engine: &MarketStateEngine) {
        let state = engine.state();
        let pending: usize = STRUCTURE_TIMEFRAMES
            .iter()
            .map(|timeframe| {
                state
                    .structure
                    .get(*timeframe)
                    .unwrap()
                    .levels
                    .ready()
                    .map_or(0, |registry| registry.pending().len())
            })
            .sum();
        let (sweeps, _) = engine.trigger_tracker().oi_records();
        assert!(sweeps <= pending, "{sweeps} records, {pending} pending");
    }

    /// The candidate a trigger was built on, if any.
    fn candidate_of(fact: &TriggerEvent) -> Option<&CandidateEvent> {
        match &fact.evidence {
            TriggerEvidence::Candidate(candidate) => Some(candidate),
            TriggerEvidence::Auction(_)
            | TriggerEvidence::Sfp(_)
            | TriggerEvidence::OiExpansion(_) => None,
        }
    }

    /// ADR-053 D4, D6 on one candidate trigger: the candidate passed the
    /// gate on the context the event carries, and its id's value, ready
    /// with the candidate's input, holds the trigger with the candidate's
    /// hashed value as evidence.
    fn check_candidate(triggers: &Triggers, fact: &TriggerEvent, candidate: &CandidateEvent) {
        assert_eq!(fact.known_at, candidate.time());
        assert!(candidate::at_location(&fact.context), "{fact}");
        fn expect<E: PartialEq + fmt::Debug>(
            fact: &TriggerEvent,
            value: Option<&FeatureValue<TriggerValue<E>>>,
            evidence: E,
        ) {
            let FeatureValue::Ready(value) = value.unwrap() else {
                panic!("a candidate fires on a ready input: {fact}");
            };
            assert_eq!(value.feature, fact.feature);
            assert_eq!(
                value.last,
                Some(Trigger {
                    known_at: fact.known_at,
                    anchor: fact.anchor(),
                    evidence,
                })
            );
        }
        match candidate {
            CandidateEvent::CvdDivergence(event) => {
                assert_eq!(fact.family, TriggerFamily::CvdDivergence);
                assert_eq!(fact.anchor(), event.comparison.pair.current.price);
                expect(
                    fact,
                    triggers.cvd_divergence(event.timeframe),
                    event.comparison,
                );
            }
            CandidateEvent::EffortVsResult(event) => {
                assert_eq!(fact.family, TriggerFamily::EffortVsResult);
                assert_eq!(fact.context, event.context);
                expect(
                    fact,
                    triggers.effort_result(event.value.timeframe),
                    event.value,
                );
            }
            CandidateEvent::Exhaustion(event) => {
                assert_eq!(fact.family, TriggerFamily::Exhaustion);
                assert_eq!(fact.context, event.context);
                expect(fact, Some(triggers.exhaustion()), event.value);
            }
            CandidateEvent::Absorption(event) => {
                assert_eq!(fact.family, TriggerFamily::Absorption);
                assert_eq!(fact.context, event.context);
                expect(
                    fact,
                    triggers.absorption(event.value.effort.timeframe),
                    event.value,
                );
            }
            CandidateEvent::OiDivergence(_) => panic!("an OI divergence feeds no family: {fact}"),
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
    /// sweep day 0's swing highs and judge effort vs result, so they emit
    /// SFP and candidate triggers too.
    fn auction_only(fired: Vec<(usize, TriggerEvent)>) -> Vec<(usize, TriggerEvent)> {
        fired
            .into_iter()
            .filter(|(_, fact)| {
                AUCTION_TRIGGERS
                    .iter()
                    .any(|(family, _)| *family == fact.family)
            })
            .collect()
    }

    /// The auction status a trigger was built on.
    fn status_of(fact: &TriggerEvent) -> &AuctionStatus {
        match &fact.evidence {
            TriggerEvidence::Auction(status) => status,
            other => panic!("{other}"),
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
            let TriggerEvidence::Sfp(sweep) = &fact.evidence else {
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
        level: (
            Option<crate::location::LevelSide>,
            FeatureKey,
            Option<EventTime>,
            Price,
        ),
    ) -> bool {
        of_swing(
            (
                sweep.timeframe,
                sweep.level.side,
                sweep.level.price,
                sweep.level.swing_time,
            ),
            level,
        )
    }

    /// Whether a level marks the extreme of a swing — its timeframe, side,
    /// price and swing-bar open — as any structure timeframe registered it
    /// (ADR-053 D4): a `structure.levels.<tf>` level of that side and price
    /// whose swing bar nests with the swing's bar. Written from the ADR
    /// text, not from the code.
    fn of_swing(
        (swing_timeframe, swing_side, swing_price, swing_time): (Timeframe, Side, Price, EventTime),
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
        let swing_open = swing_time.as_millis();
        let swing_end = swing_open + swing_timeframe.millis();
        let swing_side = match swing_side {
            Side::High => crate::location::LevelSide::High,
            Side::Low => crate::location::LevelSide::Low,
        };
        side == Some(swing_side)
            && price == swing_price
            && ((open <= swing_open && swing_end <= end)
                || (swing_open <= open && end <= swing_end))
    }

    /// Whether `kind` is one the candidate gate admits, written from the
    /// ADR-053 D4 text.
    fn admitted(kind: LevelKind) -> bool {
        matches!(
            kind,
            LevelKind::Poc
                | LevelKind::Vah
                | LevelKind::Val
                | LevelKind::Hvn
                | LevelKind::Lvn
                | LevelKind::StructuralHigh
                | LevelKind::StructuralLow
                | LevelKind::SfpRejectionZone
                | LevelKind::Vwap
        )
    }

    #[test]
    fn the_candidate_gate_on_the_golden_tape() {
        // ADR-053 D4, D6, D8: on the candidate golden tape the gate, written
        // here from the ADR text over the registry the stage read, passes
        // exactly the candidates that fire; no CVD trigger context lists a
        // level of either swing, and every other candidate trigger carries
        // its candidate's own context. The counts the ADR quotes are pinned
        // here; a change to one moves the ADR text in the same PR.
        let mut engine = MarketStateEngine::new();
        let mut effort_warmed_up = false;
        let mut divergences = [0; 4];
        let mut previous_listed = 0;
        let mut current_skipped = 0;
        let mut both_skipped = [0; 4];
        let (mut unpadded, mut padded) = (0, 0);
        let mut outside_pair_only = 0;
        let (mut edges, mut edge_triggers) = (0, 0);
        for event in crate::candidate::tests::golden_tape() {
            engine.apply(&event).unwrap();
            check_values(&engine);
            // The effort ids follow their input through its 1 440
            // evaluations of warm-up.
            if let Some(FeatureValue::WarmingUp { required, .. }) =
                engine.state().triggers.effort_result(Timeframe::M5)
            {
                assert_eq!(*required, 1_440);
                effort_warmed_up = true;
            }
            let fired: Vec<&TriggerEvent> = engine
                .trigger_events()
                .iter()
                .filter(|fact| matches!(fact.evidence, TriggerEvidence::Candidate(_)))
                .collect();
            for fact in &fired {
                if matches!(
                    fact.family,
                    TriggerFamily::EffortVsResult | TriggerFamily::Exhaustion
                ) {
                    edge_triggers += 1;
                }
            }
            let fired_on = |candidate: &CandidateEvent| {
                fired
                    .iter()
                    .filter(|fact| candidate_of(fact) == Some(candidate))
                    .collect::<Vec<_>>()
            };
            for candidate in engine.candidate_events() {
                let event = match candidate {
                    CandidateEvent::CvdDivergence(event) => event,
                    CandidateEvent::EffortVsResult(_) | CandidateEvent::Exhaustion(_) => {
                        edges += 1;
                        let gate = candidate.context().levels.ready().is_some_and(|levels| {
                            levels.levels.iter().any(|level| admitted(level.kind))
                        });
                        assert_eq!(fired_on(candidate).len(), usize::from(gate), "{candidate}");
                        continue;
                    }
                    _ => continue,
                };
                let position = STRUCTURE_TIMEFRAMES
                    .iter()
                    .position(|timeframe| *timeframe == event.timeframe)
                    .unwrap();
                divergences[position] += 1;
                let pair = event.comparison.pair;
                let price = pair.current.price;
                let swing = |anchor: &SwingAnchor| {
                    (event.timeframe, pair.side, anchor.price, anchor.swing_time)
                };
                let identity = |level: &crate::location::LocationLevel| {
                    (level.side, level.source, level.anchor, level.price)
                };
                let of_current = |level| of_swing(swing(&pair.current), identity(level));
                let of_previous = |level| of_swing(swing(&pair.previous), identity(level));
                let at: Vec<&crate::location::LocationLevel> = engine
                    .state()
                    .location
                    .levels
                    .ready()
                    .map(|registry| {
                        registry
                            .levels()
                            .iter()
                            .filter(|level| zone_contains(level, price))
                            .collect()
                    })
                    .unwrap_or_default();
                let previous: Vec<_> = at
                    .iter()
                    .filter(|level| admitted(level.kind) && of_previous(level))
                    .collect();
                if !previous.is_empty() {
                    previous_listed += 1;
                }
                assert!(
                    previous
                        .iter()
                        .all(|level| level.kind == LevelKind::SfpRejectionZone),
                    "{candidate}"
                );
                let without_current: Vec<_> = at
                    .iter()
                    .filter(|level| admitted(level.kind) && !of_current(level))
                    .collect();
                let without_pair: Vec<_> = without_current
                    .iter()
                    .filter(|level| !of_previous(level))
                    .collect();
                let gate_current = !without_current.is_empty();
                let gate_pair = !without_pair.is_empty();
                if gate_current {
                    current_skipped += 1;
                }
                if gate_pair {
                    both_skipped[position] += 1;
                    // Passes only on SFP zones that resolved at or after the
                    // current swing's anchor trade: swings outside the pair
                    // that the candidate's own move made a zone of.
                    if without_pair.iter().all(|level| {
                        level.kind == LevelKind::SfpRejectionZone
                            && level.known_at >= pair.current.time
                    }) {
                        outside_pair_only += 1;
                    }
                } else if gate_current {
                    // The previous swing alone decides: does its zone reach
                    // the current price, or only its padding?
                    if without_current
                        .iter()
                        .any(|level| level.low <= price && price <= level.high)
                    {
                        unpadded += 1;
                    } else {
                        padded += 1;
                    }
                }
                let own = fired_on(candidate);
                assert_eq!(own.len(), usize::from(gate_pair), "{candidate}");
                for fact in own {
                    assert_eq!(
                        fact.feature,
                        catalog::TRIGGER_CVD_DIVERGENCE[position].1.key
                    );
                    assert_eq!(fact.anchor(), price);
                    let levels = &fact.context.levels.ready().unwrap().levels;
                    assert!(
                        levels.iter().all(|level| {
                            let identity = (level.side, level.source, level.anchor, level.price);
                            !of_swing(swing(&pair.current), identity)
                                && !of_swing(swing(&pair.previous), identity)
                        }),
                        "{fact}"
                    );
                }
            }
        }
        assert!(effort_warmed_up);
        // Trades only: the absorption ids stay warming up (ADR-053 D6).
        for (timeframe, _, absorption) in engine.state().triggers.iter_windows() {
            assert!(
                matches!(absorption, FeatureValue::WarmingUp { .. }),
                "{timeframe}"
            );
        }
        // ADR-053 D4 and D8 quote these counts.
        assert_eq!(
            (
                divergences,
                previous_listed,
                current_skipped,
                both_skipped,
                (unpadded, padded),
                outside_pair_only,
                (edges, edge_triggers),
            ),
            (
                [194, 52, 11, 0],
                120,
                248,
                [182, 50, 11, 0],
                (1, 4),
                3,
                (6_962, 6_892),
            )
        );
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
                let TriggerEvidence::Sfp(sweep) = &fact.evidence else {
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
                                sweep,
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
                        of_swept_swing(sweep, (level.side, level.source, level.anchor, level.price))
                    })
                    .collect();
                assert!(left.is_empty(), "{fact}: {left:?}");
            }
        }
        // ADR-053 D4 quotes these counts.
        assert_eq!((sfps, other_timeframe, gate_kind), (515, 202, 104));
    }

    /// `events` with every trade's time multiplied by `time` and its
    /// distance from 60 000 USDT by `price`: the same structure on longer
    /// bars or a wider range.
    fn scaled(events: Vec<MarketEvent>, time: i64, price: i64) -> Vec<MarketEvent> {
        let base = 60_000 * SCALE;
        events
            .into_iter()
            .map(|event| match event {
                MarketEvent::Trade(trade) => MarketEvent::Trade(Trade {
                    time: t(trade.time.as_millis() * time),
                    price: Price::from_units(base + (trade.price.units() - base) * price),
                    ..trade
                }),
                other => other,
            })
            .collect()
    }

    /// Replays `events` through a new engine, checking the values after
    /// each one, and returns the CVD divergence candidates and the CVD
    /// divergence triggers with the engine.
    fn divergences_of(
        events: &[MarketEvent],
    ) -> (
        Vec<
            crate::candidate::divergence::DivergenceEvent<
                crate::candidate::divergence::CvdComparison,
            >,
        >,
        Vec<TriggerEvent>,
        MarketStateEngine,
    ) {
        let mut engine = MarketStateEngine::new();
        let (mut candidates, mut triggers) = (Vec::new(), Vec::new());
        for event in events {
            engine.apply(event).unwrap();
            check_values(&engine);
            candidates.extend(
                engine
                    .candidate_events()
                    .iter()
                    .filter_map(|fact| match fact {
                        CandidateEvent::CvdDivergence(event) => Some(event.clone()),
                        _ => None,
                    }),
            );
            triggers.extend(
                engine
                    .trigger_events()
                    .iter()
                    .filter(|fact| fact.family == TriggerFamily::CvdDivergence)
                    .cloned(),
            );
        }
        (candidates, triggers, engine)
    }

    /// Whether a context level marks either swing of `pair` on `timeframe`
    /// (ADR-053 D4).
    fn of_pair(
        timeframe: Timeframe,
        pair: &SwingPair,
        level: &crate::candidate::ContextLevel,
    ) -> bool {
        let identity = (level.side, level.source, level.anchor, level.price);
        [pair.previous, pair.current].iter().any(|swing| {
            of_swing(
                (timeframe, pair.side, swing.price, swing.swing_time),
                identity,
            )
        })
    }

    #[test]
    fn a_divergence_does_not_pass_the_gate_on_its_own_pair() {
        // ADR-053 D4: B's move sweeps swing A and closes back inside within
        // the SFP window, so A's SFP zone reaches B's price. On the
        // divergence scenario spread ten times wider, nothing else lies
        // within the tolerance of B: the candidate fires and lists A's
        // zone, the trigger does not.
        for side in [Side::High, Side::Low] {
            let scenario = crate::candidate::divergence::tests::Scenario::divergent(side);
            let (candidates, triggers, engine) = divergences_of(&scaled(scenario.tape(), 1, 10));
            let [candidate] = candidates.as_slice() else {
                panic!("{side}: {candidates:?}")
            };
            let pair = candidate.comparison.pair;
            let levels = &candidate.context.levels.ready().unwrap().levels;
            let admitted_levels: Vec<_> =
                levels.iter().filter(|level| admitted(level.kind)).collect();
            assert!(
                admitted_levels.iter().any(|level| {
                    level.kind == LevelKind::SfpRejectionZone && level.price == pair.previous.price
                }),
                "{candidate}"
            );
            assert!(
                admitted_levels
                    .iter()
                    .all(|level| of_pair(Timeframe::M15, &pair, level)),
                "{candidate}"
            );
            assert_eq!(triggers, [], "{side}");
            assert_eq!(
                engine.state().triggers.cvd_divergence(Timeframe::M15),
                Some(&FeatureValue::Ready(TriggerValue {
                    feature: catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.key,
                    last: None,
                }))
            );
        }
    }

    #[test]
    fn a_divergence_at_a_place_outside_its_pair_fires_once() {
        // ADR-053 D4, D6: on the divergence scenario as it stands, the swing
        // of the other side and the VWAP lie within the tolerance of B. One
        // trigger fires; its context lists neither swing's levels, which
        // the candidate's own context still lists, and its evidence is the
        // candidate event.
        for side in [Side::High, Side::Low] {
            let scenario = crate::candidate::divergence::tests::Scenario::divergent(side);
            let (candidates, triggers, engine) = divergences_of(&scenario.tape());
            let ([candidate], [fact]) = (candidates.as_slice(), triggers.as_slice()) else {
                panic!("{side}: {candidates:?} {triggers:?}")
            };
            let pair = candidate.comparison.pair;
            assert_eq!(fact.feature, catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.key);
            assert_eq!(fact.inputs, catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.inputs);
            assert_eq!(fact.known_at, candidate.known_at);
            assert_eq!(fact.anchor(), pair.current.price);
            assert_eq!(
                fact.evidence,
                TriggerEvidence::Candidate(Box::new(CandidateEvent::CvdDivergence(
                    candidate.clone()
                )))
            );
            let own = &candidate.context.levels.ready().unwrap().levels;
            assert!(
                own.iter().any(|level| level.price == pair.previous.price
                    && of_pair(Timeframe::M15, &pair, level)),
                "{candidate}"
            );
            let levels = &fact.context.levels.ready().unwrap().levels;
            assert!(levels.iter().any(|level| admitted(level.kind)), "{fact}");
            assert!(
                levels
                    .iter()
                    .all(|level| !of_pair(Timeframe::M15, &pair, level)),
                "{fact}"
            );
            // The levels it lists are the candidate's, without the pair's.
            let kept: Vec<_> = own
                .iter()
                .filter(|level| !of_pair(Timeframe::M15, &pair, level))
                .collect();
            assert_eq!(levels.iter().collect::<Vec<_>>(), kept);
            let FeatureValue::Ready(value) = engine
                .state()
                .triggers
                .cvd_divergence(Timeframe::M15)
                .unwrap()
            else {
                panic!("{side}")
            };
            assert_eq!(
                value.last,
                Some(Trigger {
                    known_at: fact.known_at,
                    anchor: pair.current.price,
                    evidence: candidate.comparison,
                })
            );
        }
    }

    #[test]
    fn a_daily_divergence_fires_through_the_engine() {
        // The 45-day golden tape holds no daily divergence; the divergence
        // scenario on daily bars does. Its swings are also swings of every
        // shorter structure timeframe inside their days, which the context
        // leaves out (ADR-053 D4).
        for side in [Side::High, Side::Low] {
            let scenario = crate::candidate::divergence::tests::Scenario::divergent(side);
            let (candidates, triggers, engine) =
                divergences_of(&scaled(scenario.tape(), DAY / 900_000, 1));
            let daily: Vec<_> = triggers
                .iter()
                .filter(|fact| fact.feature == catalog::TRIGGER_CVD_DIVERGENCE_1D_V1.key)
                .collect();
            let [fact] = daily.as_slice() else {
                panic!("{side}: {triggers:?}")
            };
            let Some(CandidateEvent::CvdDivergence(candidate)) = candidate_of(fact) else {
                panic!("{fact}")
            };
            assert!(candidates.contains(candidate));
            assert_eq!(candidate.timeframe, Timeframe::D1);
            let pair = candidate.comparison.pair;
            let levels = &fact.context.levels.ready().unwrap().levels;
            assert!(
                levels
                    .iter()
                    .all(|level| !of_pair(Timeframe::D1, &pair, level)),
                "{fact}"
            );
            let FeatureValue::Ready(value) = engine
                .state()
                .triggers
                .cvd_divergence(Timeframe::D1)
                .unwrap()
            else {
                panic!("{side}")
            };
            assert_eq!(
                value.last.map(|last| last.evidence),
                Some(candidate.comparison)
            );
        }
    }

    #[test]
    fn absorption_fires_on_a_book_with_its_candidates_location() {
        // ADR-053 D6: live only. On the book tape every absorption id fires
        // on its candidate's rising edge, at the candidate's own context.
        let mut engine = MarketStateEngine::new();
        let mut fired = [0; 3];
        for event in crate::candidate::tests::book_tape() {
            engine.apply(&event).unwrap();
            check_values(&engine);
            for fact in engine.trigger_events() {
                let Some(CandidateEvent::Absorption(candidate)) = candidate_of(fact) else {
                    continue;
                };
                assert_eq!(fact.context, candidate.context);
                let position = catalog::TRIGGER_ABSORPTION
                    .iter()
                    .position(|(_, definition)| definition.key == fact.feature)
                    .unwrap();
                fired[position] += 1;
            }
        }
        assert!(fired.iter().all(|count| *count > 0), "{fired:?}");
    }

    /// The brief §11 tape: day 0 as [`Tape::prior_day`]; on day 1 one trade
    /// a minute in the middle of value, but a 15m swing high exactly at the
    /// prior day's VAH on bar 16 (minute 247). Bar 20 probes above value
    /// from minute 301 to 313, which also sweeps the swing, and its last
    /// minute closes back inside, 40 USDT below VAH: inside value beyond the
    /// edge band, and within the padded zone of the VAH level. The trade
    /// 1 s into minute 315 closes that minute and bar 20: one event fails
    /// the probe, resolves the sweep as an SFP and judges the minute's
    /// effort vs result at that close. Returns the tape and VAH.
    fn at_value_edge() -> (Vec<MarketEvent>, Price) {
        let (val, vah) = value_area();
        let middle = Price::from_units((val.units() + vah.units()) / 2);
        let beyond = Price::from_units(vah.units() + 100 * SCALE);
        let mut path = vec![middle; 316];
        path[247] = vah;
        for price in &mut path[301..314] {
            *price = beyond;
        }
        path[314] = Price::from_units(vah.units() - 40 * SCALE);
        (auction_tape(&path), vah)
    }

    /// The trigger events of the last event of `fired`.
    fn last_event(fired: &[(usize, TriggerEvent)]) -> Vec<&TriggerEvent> {
        let last = fired.last().map(|(index, _)| *index);
        fired
            .iter()
            .filter(|(index, _)| Some(*index) == last)
            .map(|(_, fact)| fact)
            .collect()
    }

    #[test]
    fn one_bias_and_location_meet_three_families() {
        // Brief §11, ADR-053 D2–D4: the same bias and location, tested
        // against three families in one event, in emission order: the
        // structure fact, the location fact, the candidate fact.
        let (events, vah) = at_value_edge();
        let mut engine = MarketStateEngine::new();
        let fired = run(&mut engine, &events);
        assert_eq!(fired.last().unwrap().0, events.len() - 1);
        let last = last_event(&fired);
        assert_eq!(
            last.iter().map(|fact| fact.family).collect::<Vec<_>>(),
            [
                TriggerFamily::Sfp,
                TriggerFamily::FailedAuction,
                TriggerFamily::EffortVsResult
            ]
        );
        let [sfp, auction, effort] = last.as_slice() else {
            unreachable!()
        };
        assert_eq!(sfp.feature, catalog::TRIGGER_SFP_15M_V1.key);
        assert_eq!(sfp.bias, auction.bias);
        assert_eq!(effort.bias, sfp.bias);
        assert_eq!(effort.bias, Bias::read(engine.state()));
        assert_eq!((sfp.anchor(), auction.anchor()), (vah, vah));
        // The candidate's anchor is the last close, inside value.
        assert_eq!(effort.anchor(), Price::from_units(vah.units() - 40 * SCALE));
        let Some(CandidateEvent::EffortVsResult(candidate)) = candidate_of(effort) else {
            panic!("{effort}")
        };
        assert_eq!(effort.context, candidate.context);
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
        assert_eq!(prior_vah(effort), Some(level));
        assert_eq!(level.price, vah);
    }

    #[test]
    fn the_bias_gates_nothing() {
        // ADR-053 D3: two states that differ only in the bias inputs give
        // the same triggers, apart from the snapshot.
        let (events, _) = at_value_edge_with_oi();
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
                engine.candidate_events(),
                Inputs {
                    regime,
                    structure: &state.structure,
                    location: &state.location,
                    candidates,
                    oi_5m: &state.derivatives.oi_5m,
                },
                &mut triggers,
                &mut fired,
            );
            (triggers, fired)
        };
        let (triggers, fired) = detect(&state.regime, &state.candidates);
        // A fresh tracker holds no OI-expansion record, so the rejections
        // of the last event fire without their expansions.
        assert_eq!(
            families(
                &fired
                    .iter()
                    .cloned()
                    .map(|fact| (0, fact))
                    .collect::<Vec<_>>()
            ),
            [
                TriggerFamily::Sfp,
                TriggerFamily::FailedAuction,
                TriggerFamily::EffortVsResult
            ]
        );
        // The engine's tracker holds them; the bias does not change them.
        let detect_held = |regime: &FeatureValue<Regime>, candidates: &Candidates| {
            let mut engine = MarketStateEngine::new();
            for event in &events[..events.len() - 1] {
                engine.apply(event).unwrap();
            }
            let mut tracker = engine.trigger_tracker().clone();
            engine.apply(events.last().unwrap()).unwrap();
            let mut triggers = Triggers::new();
            let mut fired = Vec::new();
            tracker.detect(
                engine.structure_events(),
                engine.location_events(),
                engine.candidate_events(),
                Inputs {
                    regime,
                    structure: &state.structure,
                    location: &state.location,
                    candidates,
                    oi_5m: &state.derivatives.oi_5m,
                },
                &mut triggers,
                &mut fired,
            );
            fired
        };
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
        let held = detect_held(&state.regime, &state.candidates);
        assert_eq!(
            held.iter().map(|fact| fact.family).collect::<Vec<_>>(),
            EDGE_FAMILIES
        );
        assert_eq!(held, engine.trigger_events());
        let other_held = detect_held(&regime, &candidates);
        assert_eq!(other_held.len(), held.len());
        for (fact, other) in held.iter().zip(&other_held) {
            let mut other = other.clone();
            other.bias = fact.bias;
            assert_eq!(*fact, other);
        }
        let (mut other_triggers, other_fired) = detect(&regime, &candidates);
        assert_ne!(other_triggers.bias(), triggers.bias());
        // The 4h CVD divergence trigger follows the validity of the input
        // the edit made ready (ADR-053 D1), and holds no trigger either
        // way; every other value, the candidate ones included, is the same.
        assert_eq!(
            validity(&triggers.cvd_divergence[2]),
            validity(&state.candidates.divergences(Timeframe::H4).unwrap().cvd)
        );
        assert_eq!(
            other_triggers.cvd_divergence[2],
            FeatureValue::Ready(TriggerValue {
                feature: catalog::TRIGGER_CVD_DIVERGENCE_4H_V1.key,
                last: None,
            })
        );
        other_triggers.bias = triggers.bias;
        other_triggers.cvd_divergence[2] = triggers.cvd_divergence[2];
        assert_eq!(other_triggers, triggers);
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
        let (events, _) = at_value_edge_with_oi();
        let mut engine = MarketStateEngine::new();
        for event in &events {
            engine.apply(event).unwrap();
        }
        let facts = engine.trigger_events().to_vec();
        assert_eq!(facts.len(), EDGE_FAMILIES.len());
        let triggers = engine.state().triggers.clone();
        let tracker = engine.trigger_tracker().clone();
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
        // The OI-expansion records too.
        assert_eq!(engine.trigger_tracker(), &tracker);
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
        let tape = with_open_interest(
            crate::location::tests::walk_tape(0x6d69_6500_0000_0024, 4),
            Feed::Archive,
            oi_walk(0x6d69_6500_0024_0001),
        );
        let (facts, hashes) = replay(&tape);
        assert!(!facts.is_empty());
        // The candidate and OI-expansion ids are among them.
        for family in ["effort_vs_result ", "oi_expansion_rejection "] {
            assert!(
                facts.iter().any(|fact| fact.starts_with(family)),
                "{family}: {facts:?}"
            );
        }
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
        let (events, _) = at_value_edge_with_oi();
        let fired = run(&mut engine, &events);
        assert_eq!(
            last_event(&fired)
                .iter()
                .map(|fact| fact.family)
                .collect::<Vec<_>>(),
            EDGE_FAMILIES
        );
        for (_, fact) in &fired {
            assert_eq!(
                fact.context.levels.ready().unwrap().feature,
                catalog::LOCATION_LEVELS_V1.key
            );
        }
    }

    /// The open-interest feed of a test tape.
    #[derive(Debug, Clone, Copy)]
    enum Feed {
        /// One sample at each boundary at 300 000 ms resolution, as the
        /// archive's `metrics` rows: the boundary closes at itself.
        Archive,
        /// Two 10 s polls around each boundary, 4 s before it with the
        /// boundary's open interest and 6 s after it with 0.07 BTC more, as
        /// a live poll lands: the boundary closes at the poll after it.
        Live,
    }

    /// `events` with open-interest samples merged in canonical order: for
    /// every grid boundary after the first event up to the last,
    /// `oi(boundary)` units on `feed`.
    fn with_open_interest(
        mut events: Vec<MarketEvent>,
        feed: Feed,
        mut oi: impl FnMut(i64) -> i64,
    ) -> Vec<MarketEvent> {
        let time = |event: Option<&MarketEvent>| event.map_or(0, |event| event.time().as_millis());
        let (first, last) = (time(events.first()), time(events.last()));
        let sample = |millis: i64, units: i64, resolution_ms: u32| {
            MarketEvent::OpenInterest(OpenInterest {
                time: t(millis),
                open_interest: Qty::from_units(units),
                resolution_ms,
            })
        };
        let mut boundary = first - first.rem_euclid(GRID_MS) + GRID_MS;
        let mut samples = Vec::new();
        while boundary <= last {
            let units = oi(boundary);
            match feed {
                Feed::Archive => samples.push(sample(boundary, units, 300_000)),
                Feed::Live => {
                    samples.push(sample(boundary - 4_000, units, 10_000));
                    samples.push(sample(boundary + 6_000, units + 7_000_000, 10_000));
                }
            }
            boundary += GRID_MS;
        }
        events.extend(samples);
        events.sort();
        events
    }

    /// The open-interest grid in milliseconds (ADR-042 D2).
    const GRID_MS: i64 = 300_000;

    /// Open interest rising by 0.1 BTC per boundary from 80 000 BTC.
    fn rising(boundary: i64) -> i64 {
        80_000 * SCALE + boundary / GRID_MS * (SCALE / 10)
    }

    /// A seeded open-interest walk from 80 000 BTC, ±10 BTC per boundary
    /// in steps of 0.1 BTC.
    fn oi_walk(seed: u64) -> impl FnMut(i64) -> i64 {
        let (mut state, mut oi) = (seed, 80_000 * SCALE);
        move |_| {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let step = i64::try_from((state >> 11) % 201).unwrap() - 100;
            oi += step * (SCALE / 10);
            oi
        }
    }

    /// The boundary an event at `millis` reads its point at, written from
    /// ADR-053 D7: `⌊(t − 25 000) / 300 000⌋ · 300 000`.
    fn lagged(millis: i64) -> EventTime {
        let lagged = millis - 25_000;
        t(lagged - lagged.rem_euclid(GRID_MS))
    }

    /// The OI expansion a trigger carries.
    fn expansion_of(fact: &TriggerEvent) -> &OiExpansion {
        match &fact.evidence {
            TriggerEvidence::OiExpansion(expansion) => expansion,
            other => panic!("{other}"),
        }
    }

    /// The at-value-edge tape with rising archive open interest: the last
    /// event emits the SFP, its OI expansion, the failed auction, its OI
    /// expansion and the effort-vs-result trigger.
    fn at_value_edge_with_oi() -> (Vec<MarketEvent>, Price) {
        let (events, vah) = at_value_edge();
        (with_open_interest(events, Feed::Archive, rising), vah)
    }

    /// The families of the last event of [`at_value_edge_with_oi`], in
    /// emission order (ADR-053 D2).
    const EDGE_FAMILIES: [TriggerFamily; 5] = [
        TriggerFamily::Sfp,
        TriggerFamily::OiExpansionRejection,
        TriggerFamily::FailedAuction,
        TriggerFamily::OiExpansionRejection,
        TriggerFamily::EffortVsResult,
    ];

    #[test]
    fn an_oi_expansion_follows_its_sfp_on_every_structure_timeframe() {
        // ADR-053 D7 with rising open interest: the sweep at 2 s into bar
        // 11 and the SFP at 1 s into bar 12 read different boundaries, the
        // later one higher. On the daily tape this is the 1d id, which the
        // 45-day golden tape cannot reach.
        for (position, (timeframe, definition)) in catalog::TRIGGER_OI_EXPANSION_REJECTION
            .into_iter()
            .enumerate()
        {
            let span = timeframe.millis();
            let mut engine = MarketStateEngine::new();
            let tape = with_open_interest(sfp_tape(timeframe, false), Feed::Archive, rising);
            let fired = run(&mut engine, &tape);
            let own: Vec<usize> = (0..fired.len())
                .filter(|index| fired[*index].1.feature == definition.key)
                .collect();
            let [index] = own.as_slice() else {
                panic!("{timeframe}: {own:?}")
            };
            let ((event, fact), (sfp_event, sfp)) = (&fired[*index], &fired[index - 1]);
            assert_eq!(sfp.feature, catalog::TRIGGER_SFP[position].1.key);
            assert_eq!(event, sfp_event);
            assert_eq!(fact.family, TriggerFamily::OiExpansionRejection);
            assert_eq!(fact.inputs, definition.inputs);
            assert_eq!(fact.known_at, t(12 * span + 1_000), "{timeframe}");
            assert_eq!((fact.anchor(), &fact.context), (usdt(60_015), &sfp.context));
            assert_eq!(fact.bias, sfp.bias);
            let TriggerEvidence::Sfp(sweep) = &sfp.evidence else {
                panic!("{sfp}")
            };
            let expansion = expansion_of(fact);
            assert_eq!(expansion.rejection, OiRejection::Sfp(*sweep));
            assert_eq!(expansion.start.boundary, lagged(11 * span + 2_000));
            assert_eq!(expansion.end.boundary, lagged(12 * span + 1_000));
            assert_eq!(
                expansion.start.open_interest,
                Qty::from_units(rising(expansion.start.boundary.as_millis()))
            );
            assert_eq!(
                expansion.change.units(),
                expansion.end.open_interest.units() - expansion.start.open_interest.units()
            );
            assert_eq!(
                engine.state().triggers.oi_expansion_rejection(timeframe),
                Some(&FeatureValue::Ready(TriggerValue {
                    feature: definition.key,
                    last: Some(Trigger {
                        known_at: fact.known_at,
                        anchor: usdt(60_015),
                        evidence: *expansion,
                    }),
                }))
            );
        }
    }

    #[test]
    fn near_misses_of_an_oi_expansion_do_not_fire() {
        let span = Timeframe::M15.millis();
        let sfp = catalog::TRIGGER_SFP_15M_V1.key;
        let oi = catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1.key;
        let fires = |events: &[MarketEvent]| {
            let fired = triggers_of(events);
            let any = |key: FeatureKey| fired.iter().any(|(_, fact)| fact.feature == key);
            (any(sfp), any(oi))
        };
        let tape = |oi: fn(i64) -> i64| {
            with_open_interest(sfp_tape(Timeframe::M15, false), Feed::Archive, oi)
        };
        assert_eq!(fires(&tape(rising)), (true, true));
        // Equal open interest, and falling open interest.
        assert_eq!(fires(&tape(|_| 80_000 * SCALE)), (true, false));
        assert_eq!(
            fires(&tape(|boundary| 80_000 * SCALE - boundary / GRID_MS * SCALE)),
            (true, false)
        );
        // A sweep 100 s before the bar end: its point and the SFP's lie on
        // one boundary, so there is no comparison.
        let mut late = Tape::swing_high(Timeframe::M15);
        late.trade(12 * span - 100_000, usdt(60_020))
            .trade(12 * span - 99_000, usdt(60_014))
            .trade(12 * span + 1_000, usdt(60_014));
        assert_eq!(lagged(12 * span - 100_000), lagged(12 * span + 1_000));
        assert_eq!(
            fires(&with_open_interest(late.events, Feed::Archive, rising)),
            (true, false)
        );
        // An open-interest hole over the SFP's boundary: no end point.
        let holed: Vec<MarketEvent> = tape(rising)
            .into_iter()
            .filter(|event| {
                !matches!(event, MarketEvent::OpenInterest(sample)
                    if (10 * span..12 * span).contains(&sample.time.as_millis()))
            })
            .collect();
        assert_eq!(fires(&holed), (true, false));
        // A clean break is no SFP, so no expansion either, and its record
        // goes with the sweep.
        let mut engine = MarketStateEngine::new();
        let broken = with_open_interest(sfp_tape(Timeframe::M15, true), Feed::Archive, rising);
        let fired = run(&mut engine, &broken);
        assert!(
            fired
                .iter()
                .all(|(_, fact)| fact.feature != sfp && fact.feature != oi)
        );
        let pending = engine
            .state()
            .structure
            .get(Timeframe::M15)
            .unwrap()
            .levels
            .ready()
            .unwrap()
            .pending()
            .len();
        assert_eq!(pending, 0);
    }

    /// The failed-auction triggers and their OI expansions of `fired`.
    fn failed_auctions(fired: &[(usize, TriggerEvent)]) -> Vec<&TriggerEvent> {
        fired
            .iter()
            .map(|(_, fact)| fact)
            .filter(|fact| {
                fact.family == TriggerFamily::FailedAuction
                    || fact.feature == catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key
            })
            .collect()
    }

    #[test]
    fn a_failed_auction_with_rising_oi_fires_its_expansion() {
        // ADR-053 D7: the probe's first close, 1 s into minute 11, and the
        // failing close, 1 s into minute 16, read different boundaries.
        let (val, vah) = value_area();
        for (beyond, edge) in [(ABOVE, vah), (BELOW, val)] {
            let mut path = minutes(INSIDE, 10);
            path.extend(minutes(beyond, 5));
            path.extend(minutes(INSIDE, 2));
            let mut engine = MarketStateEngine::new();
            let fired = run(
                &mut engine,
                &with_open_interest(auction_tape(&path), Feed::Archive, rising),
            );
            let [auction, fact] = failed_auctions(&fired)[..] else {
                panic!("{fired:?}")
            };
            assert_eq!(auction.family, TriggerFamily::FailedAuction);
            let definition = &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1;
            assert_eq!(
                (fact.feature, fact.inputs),
                (definition.key, definition.inputs)
            );
            assert_eq!(fact.known_at, t(DAY + 16 * MINUTE + 1_000));
            assert_eq!(fact.known_at, auction.known_at);
            assert_eq!((fact.anchor(), &fact.context), (edge, &auction.context));
            let expansion = expansion_of(fact);
            assert_eq!(
                expansion.rejection,
                OiRejection::FailedAuction(*status_of(auction))
            );
            assert_eq!(expansion.start.boundary, lagged(DAY + 11 * MINUTE + 1_000));
            assert_eq!(expansion.end.boundary, lagged(DAY + 16 * MINUTE + 1_000));
            let FeatureValue::Ready(value) =
                engine.state().triggers.oi_expansion_rejection_prior_day()
            else {
                panic!("{beyond}")
            };
            assert_eq!(value.last.map(|last| last.evidence), Some(*expansion));
            // With flat open interest only the failed auction fires.
            let flat = triggers_of(&with_open_interest(
                auction_tape(&path),
                Feed::Archive,
                |_| 80_000 * SCALE,
            ));
            assert_eq!(
                failed_auctions(&flat)
                    .iter()
                    .map(|fact| fact.family)
                    .collect::<Vec<_>>(),
                [TriggerFamily::FailedAuction]
            );
        }
    }

    #[test]
    fn a_retarget_restarts_the_excursion() {
        // A probe above from 1 s into minute 11, retargeted below 1 s into
        // minute 19 (no auction fact: the label stays a breakout), failed
        // 1 s into minute 22. The excursion starts at the retarget.
        let (val, _) = value_area();
        let mut path = minutes(INSIDE, 10);
        path.extend(minutes(ABOVE, 8));
        path.extend(minutes(BELOW, 3));
        path.extend(minutes(INSIDE, 2));
        let (above, below, failed) = (
            lagged(DAY + 11 * MINUTE + 1_000),
            lagged(DAY + 19 * MINUTE + 1_000),
            lagged(DAY + 22 * MINUTE + 1_000),
        );
        assert!(above < below && below < failed);
        let fired = triggers_of(&with_open_interest(
            auction_tape(&path),
            Feed::Archive,
            rising,
        ));
        let [auction, fact] = failed_auctions(&fired)[..] else {
            panic!("{fired:?}")
        };
        assert_eq!(status_of(auction).failure.unwrap().toward, Region::Below);
        assert_eq!(fact.anchor(), val);
        let expansion = expansion_of(fact);
        assert_eq!(
            (expansion.start.boundary, expansion.end.boundary),
            (below, failed)
        );
        // Open interest that peaks at the retarget's boundary rose over the
        // first probe but not over the excursion: no expansion.
        let peaked = |boundary: i64| rising(boundary.min(DAY + 900_000));
        assert_eq!(below, t(DAY + 900_000));
        let fired = triggers_of(&with_open_interest(
            auction_tape(&path),
            Feed::Archive,
            peaked,
        ));
        assert_eq!(
            failed_auctions(&fired)
                .iter()
                .map(|fact| fact.family)
                .collect::<Vec<_>>(),
            [TriggerFamily::FailedAuction]
        );
    }

    #[test]
    fn a_failure_at_the_rollover_finds_its_probe() {
        // ADR-053 D7, ADR-044 D2: the trade that closes 23:59 and the day
        // fails a probe under day 0's value; the record is consulted before
        // the switch drops it.
        let mut path = minutes(INSIDE, 1_430);
        path.extend(minutes(ABOVE, 9));
        path.extend(minutes(INSIDE, 2));
        let mut engine = MarketStateEngine::new();
        let fired = run(
            &mut engine,
            &with_open_interest(auction_tape(&path), Feed::Archive, rising),
        );
        let [auction, fact] = failed_auctions(&fired)[..] else {
            panic!("{fired:?}")
        };
        assert_eq!(fact.known_at, t(2 * DAY + 1_000));
        let expansion = expansion_of(fact);
        assert_eq!(
            expansion.rejection,
            OiRejection::FailedAuction(*status_of(auction))
        );
        assert_eq!(status_of(auction).reference_start, t(0));
        assert_eq!(
            expansion.start.boundary,
            lagged(DAY + 1_431 * MINUTE + 1_000)
        );
        assert_eq!(expansion.end.boundary, lagged(2 * DAY + 1_000));
        // The probe record is gone with the old reference; the value
        // follows the auction state, unavailable on day 2, and holds the
        // trigger.
        assert!(!engine.trigger_tracker().oi_records().1);
        let unavailable = validity(&engine.state().location.auction);
        assert_eq!(
            validity(engine.state().triggers.oi_expansion_rejection_prior_day()),
            unavailable
        );
        assert_eq!(
            engine
                .trigger_tracker()
                .oi_prior_day
                .map(|last| last.evidence),
            Some(*expansion)
        );
    }

    #[test]
    fn archive_and_live_open_interest_select_the_same_boundaries() {
        // ADR-053 D7, ADR-019: the same trades with the same open interest
        // at each boundary, once as archive rows and once as live polls,
        // fire the same ids on the same boundaries and values; only the
        // resolution and sample times of the points differ.
        let trades = crate::location::tests::walk_tape(0x6d69_6500_0024_0004, 4);
        let picks = |feed: Feed| {
            let events = with_open_interest(trades.clone(), feed, oi_walk(0x6d69_6500_0024_0005));
            let fired = triggers_of(&events);
            let sfps = fired
                .iter()
                .filter(|(_, fact)| fact.family == TriggerFamily::Sfp)
                .count();
            let mut resolutions = Vec::new();
            let picked: Vec<_> = fired
                .iter()
                .filter_map(|(_, fact)| match &fact.evidence {
                    TriggerEvidence::OiExpansion(expansion) => {
                        resolutions.push(expansion.start.resolution_ms);
                        Some((
                            fact.feature,
                            fact.known_at,
                            expansion.start.boundary,
                            expansion.end.boundary,
                            expansion.start.open_interest,
                            expansion.change,
                        ))
                    }
                    _ => None,
                })
                .collect();
            (sfps, picked, resolutions)
        };
        let (archive_sfps, archive, archive_resolutions) = picks(Feed::Archive);
        let (live_sfps, live, live_resolutions) = picks(Feed::Live);
        assert!(archive.len() > 10, "{archive:?}");
        assert!(
            archive
                .iter()
                .any(|pick| pick.0 == catalog::TRIGGER_OI_EXPANSION_REJECTION_1H_V1.key)
        );
        assert_eq!(archive_sfps, live_sfps);
        assert_eq!(archive, live);
        assert!(archive_resolutions.iter().all(|res| *res == 300_000));
        assert!(live_resolutions.iter().all(|res| *res == 10_000));
    }

    #[test]
    fn one_event_emits_each_rejection_before_its_expansion() {
        // ADR-053 D2: the structure fact's SFP and its OI expansion, then the
        // location fact's failed auction and its OI expansion, then the
        // candidate fact; every trigger with the event's bias.
        let (events, vah) = at_value_edge_with_oi();
        let mut engine = MarketStateEngine::new();
        let fired = run(&mut engine, &events);
        let last = last_event(&fired);
        assert_eq!(
            last.iter().map(|fact| fact.family).collect::<Vec<_>>(),
            EDGE_FAMILIES
        );
        assert_eq!(
            last[1].feature,
            catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1.key
        );
        assert_eq!(
            last[3].feature,
            catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key
        );
        assert_eq!((last[1].anchor(), last[3].anchor()), (vah, vah));
        for fact in &last {
            assert_eq!(fact.bias, Bias::read(engine.state()));
        }
    }

    #[test]
    fn the_point_rule_on_the_golden_tape() {
        // ADR-053 D7 and D8 quote these counts: on the candidate golden
        // tape (archive open interest), per SFP timeframe, the SFPs; those
        // the rejected rule — the newest ready point within 360 000 ms of
        // the event — would fire, and how many of them start and end on one
        // boundary; those the lagged rule fires, and how many start and end
        // on one boundary with both points present; and the SFPs resolved
        // within 25 s of their bar end, where the two rules read different
        // end boundaries. Both rules are written here from the ADR text.
        let mut engine = MarketStateEngine::new();
        let (mut newest, mut held): (Option<OiGridPoint>, [Option<OiGridPoint>; 2]) =
            (None, [None, None]);
        let mut starts: Vec<(Sweep, Option<OiGridPoint>, Option<OiGridPoint>)> = Vec::new();
        let mut counts = [[0_u32; 6]; 3];
        let fires = |start: Option<OiGridPoint>, end: Option<OiGridPoint>| match (start, end) {
            (Some(start), Some(end)) => {
                start.resolution_ms == end.resolution_ms
                    && start.boundary < end.boundary
                    && start.open_interest < end.open_interest
            }
            _ => false,
        };
        let shared = |start: Option<OiGridPoint>, end: Option<OiGridPoint>| {
            start
                .zip(end)
                .is_some_and(|(start, end)| start.boundary == end.boundary)
        };
        for event in crate::candidate::tests::golden_tape() {
            engine.apply(&event).unwrap();
            let time = event.time().as_millis();
            if let FeatureValue::Ready(point) = &engine.state().derivatives.oi_5m {
                newest = Some(*point);
                if held[0].is_none_or(|last| point.boundary > last.boundary) {
                    held = [Some(*point), held[0]];
                }
            }
            let old = newest.filter(|point| point.boundary.as_millis() >= time - 360_000);
            let new = held
                .into_iter()
                .flatten()
                .find(|point| point.boundary == lagged(time));
            for fact in engine.structure_events() {
                match fact {
                    StructureEvent::Sweep(sweep) => starts.push((*sweep, old, new)),
                    StructureEvent::Sfp(sweep) | StructureEvent::Break(sweep) => {
                        let key = |other: &Sweep| {
                            (other.timeframe, other.level.side, other.level.swing_time)
                        };
                        let index = starts
                            .iter()
                            .position(|(start, _, _)| key(start) == key(sweep))
                            .unwrap();
                        let (_, old_start, new_start) = starts.remove(index);
                        let Some(row) = [Timeframe::M15, Timeframe::H1, Timeframe::H4]
                            .iter()
                            .position(|timeframe| *timeframe == sweep.timeframe)
                        else {
                            continue;
                        };
                        if matches!(fact, StructureEvent::Break(_)) {
                            continue;
                        }
                        let fired = engine.trigger_events().iter().any(|trigger| {
                            matches!(&trigger.evidence, TriggerEvidence::OiExpansion(expansion)
                                if expansion.rejection == OiRejection::Sfp(*sweep))
                        });
                        assert_eq!(fired, fires(new_start, new), "{sweep}");
                        let late = time - sweep.resolved_bar_end.unwrap().as_millis() < 25_000;
                        for (cell, count) in counts[row].iter_mut().zip([
                            true,
                            fires(old_start, old),
                            shared(old_start, old),
                            fired,
                            shared(new_start, new),
                            late,
                        ]) {
                            *cell += u32::from(count);
                        }
                    }
                    StructureEvent::Swing(_) => {}
                }
            }
        }
        assert_eq!(
            counts,
            [
                [398, 197, 0, 195, 12, 122],
                [96, 48, 0, 46, 1, 32],
                [21, 9, 0, 9, 0, 6],
            ]
        );
    }

    #[test]
    fn the_oi_records_stay_bounded_on_the_golden_tapes() {
        // ADR-053 D7: the replay of every golden tape checks after each
        // event that the sweep records number at most the pending sweeps
        // ([`check_records`]); the OI-expansion ids fire there, so the
        // records were in use.
        for (lines, family) in [
            (golden_lines(), "oi_expansion_rejection evidence=[sfp "),
            (
                auction_golden_lines(),
                "oi_expansion_rejection evidence=[failed_auction ",
            ),
        ] {
            assert!(
                lines.iter().any(|(_, line)| line.contains(family)),
                "{family}"
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
        definitions.extend(candidate_definitions());
        definitions.extend(oi_definitions());
        definitions
    }

    /// Every OI-expansion trigger definition (ADR-053 D7), in [`Triggers`]
    /// order.
    fn oi_definitions() -> Vec<&'static FeatureDefinition> {
        let mut definitions: Vec<&'static FeatureDefinition> =
            catalog::TRIGGER_OI_EXPANSION_REJECTION
                .map(|(_, definition)| definition)
                .to_vec();
        definitions.push(&catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1);
        definitions
    }

    /// Every candidate trigger definition (ADR-053 D6), in
    /// [`Triggers`] order.
    fn candidate_definitions() -> Vec<&'static FeatureDefinition> {
        let mut definitions: Vec<&'static FeatureDefinition> = Vec::new();
        definitions.extend(catalog::TRIGGER_CVD_DIVERGENCE.map(|(_, definition)| definition));
        definitions.extend(catalog::TRIGGER_EFFORT_RESULT.map(|(_, definition)| definition));
        definitions.push(&catalog::TRIGGER_EXHAUSTION_V1);
        definitions.extend(catalog::TRIGGER_ABSORPTION.map(|(_, definition)| definition));
        definitions
    }

    /// Whether a value changed: the bias, each auction id, each SFP id,
    /// each candidate id, then each OI-expansion id, in
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
        let candidate_changes = before
            .cvd_divergence
            .iter()
            .zip(&after.cvd_divergence)
            .map(|(old, new)| (old != new).then(|| shown(new)))
            .chain(
                before
                    .effort_result
                    .iter()
                    .zip(&after.effort_result)
                    .map(|(old, new)| (old != new).then(|| shown(new))),
            )
            .chain(std::iter::once(
                (before.exhaustion != after.exhaustion).then(|| shown(&after.exhaustion)),
            ))
            .chain(
                before
                    .absorption
                    .iter()
                    .zip(&after.absorption)
                    .map(|(old, new)| (old != new).then(|| shown(new))),
            );
        for (change, definition) in candidate_changes.zip(candidate_definitions()) {
            changes.push((definition.key, change));
        }
        let oi_changes = before
            .oi_expansion_rejection
            .iter()
            .zip(&after.oi_expansion_rejection)
            .map(|(old, new)| (old != new).then(|| shown(new)))
            .chain(std::iter::once(
                (before.oi_expansion_rejection_prior_day != after.oi_expansion_rejection_prior_day)
                    .then(|| shown(&after.oi_expansion_rejection_prior_day)),
            ));
        for (change, definition) in oi_changes.zip(oi_definitions()) {
            changes.push((definition.key, change));
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
    /// (ADR-044), so every auction family fires, with a seeded archive
    /// open-interest walk ([`with_open_interest`]), so the failed auction's
    /// OI-expansion id fires too. Open interest feeds no auction rule, so
    /// the walk leaves the auction ids' lines as they were.
    fn auction_golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| {
            replay_golden(with_open_interest(
                crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7),
                Feed::Archive,
                oi_walk(0x6d69_6500_0024_0003),
            ))
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
                check_records(&engine);
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

    /// The golden lines of the book tape (the candidate absorption golden
    /// tape), as [`golden_lines`]: absorption is live only, so its trigger
    /// ids fire only on a tape with a book.
    fn book_golden_lines() -> &'static [(FeatureKey, String)] {
        static LINES: std::sync::OnceLock<Vec<(FeatureKey, String)>> = std::sync::OnceLock::new();
        LINES.get_or_init(|| replay_golden(crate::candidate::tests::book_tape()))
    }

    /// The golden lines `feature` is pinned on: the auction golden tape for
    /// the auction ids and the failed auction's OI-expansion id, the book
    /// tape for the absorption ids, the candidate golden tape for the rest.
    fn golden_of(feature: &FeatureDefinition) -> &'static [(FeatureKey, String)] {
        if AUCTION_TRIGGERS
            .iter()
            .any(|(_, definition)| definition.key == feature.key)
            || feature.key == catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key
        {
            auction_golden_lines()
        } else if catalog::TRIGGER_ABSORPTION
            .iter()
            .any(|(_, definition)| definition.key == feature.key)
        {
            book_golden_lines()
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
    fn every_trigger_id_with_a_fact_on_its_golden_tape_fires_there() {
        // The goldens are not vacuous. The 45-day tape holds no SFP of a
        // daily swing and no daily divergence; the synthetic SFP, OI and 1d
        // divergence tests fire those ids.
        let without = [
            catalog::TRIGGER_SFP_1D_V1.key,
            catalog::TRIGGER_CVD_DIVERGENCE_1D_V1.key,
            catalog::TRIGGER_OI_EXPANSION_REJECTION_1D_V1.key,
        ];
        for definition in trigger_definitions().into_iter().skip(1) {
            let (own, _) = own_lines(golden_of(definition), definition.key);
            let facts = own.iter().filter(|line| !line.contains(" value ")).count();
            if without.contains(&definition.key) {
                assert_eq!(facts, 0, "{}", definition.key);
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
        let (edge, _) = at_value_edge_with_oi();
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
            .chain(book_golden_lines())
            .map(|(_, line)| line.as_str())
            .collect();
        lines.extend(samples.iter().map(String::as_str));
        for family in TriggerFamily::ALL {
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

    #[test]
    fn golden_trigger_cvd_divergence_15m_v1() {
        assert_golden(
            &catalog::TRIGGER_CVD_DIVERGENCE_15M_V1,
            365,
            GOLDEN_CVD_DIVERGENCE_15M,
            "a6259dbebcb71ba3",
        );
    }

    #[test]
    fn golden_trigger_cvd_divergence_1h_v1() {
        assert_golden(
            &catalog::TRIGGER_CVD_DIVERGENCE_1H_V1,
            101,
            GOLDEN_CVD_DIVERGENCE_1H,
            "dda22abd91e5fc1b",
        );
    }

    #[test]
    fn golden_trigger_cvd_divergence_4h_v1() {
        assert_golden(
            &catalog::TRIGGER_CVD_DIVERGENCE_4H_V1,
            23,
            GOLDEN_CVD_DIVERGENCE_4H,
            "6e0e2a1d22cc85fe",
        );
    }

    #[test]
    fn golden_trigger_cvd_divergence_1d_v1() {
        assert_golden(
            &catalog::TRIGGER_CVD_DIVERGENCE_1D_V1,
            1,
            GOLDEN_CVD_DIVERGENCE_1D,
            "e9b34ffdcfdadaa4",
        );
    }

    #[test]
    fn golden_trigger_effort_result_5m_v1() {
        assert_golden(
            &catalog::TRIGGER_EFFORT_RESULT_5M_V1,
            8369,
            GOLDEN_EFFORT_RESULT_5M,
            "c3c79821d8dff78b",
        );
    }

    #[test]
    fn golden_trigger_effort_result_15m_v1() {
        assert_golden(
            &catalog::TRIGGER_EFFORT_RESULT_15M_V1,
            5704,
            GOLDEN_EFFORT_RESULT_15M,
            "e64a843912fc5708",
        );
    }

    #[test]
    fn golden_trigger_effort_result_1h_v1() {
        assert_golden(
            &catalog::TRIGGER_EFFORT_RESULT_1H_V1,
            3728,
            GOLDEN_EFFORT_RESULT_1H,
            "5a9b9c8a4fa51f76",
        );
    }

    #[test]
    fn golden_trigger_exhaustion_v1() {
        assert_golden(
            &catalog::TRIGGER_EXHAUSTION_V1,
            308,
            GOLDEN_EXHAUSTION,
            "e1ff7823ea5f220f",
        );
    }

    #[test]
    fn golden_trigger_absorption_5m_v1() {
        assert_golden(
            &catalog::TRIGGER_ABSORPTION_5M_V1,
            11,
            GOLDEN_ABSORPTION_5M,
            "b77fb84f936394c8",
        );
    }

    #[test]
    fn golden_trigger_absorption_15m_v1() {
        assert_golden(
            &catalog::TRIGGER_ABSORPTION_15M_V1,
            15,
            GOLDEN_ABSORPTION_15M,
            "a497d617fe26b9f6",
        );
    }

    #[test]
    fn golden_trigger_absorption_1h_v1() {
        assert_golden(
            &catalog::TRIGGER_ABSORPTION_1H_V1,
            9,
            GOLDEN_ABSORPTION_1H,
            "c7ef0141900854ee",
        );
    }

    #[test]
    fn golden_trigger_oi_expansion_rejection_15m_v1() {
        assert_golden(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1,
            374,
            GOLDEN_OI_15M,
            "2a3bc1adf0206358",
        );
    }

    #[test]
    fn golden_trigger_oi_expansion_rejection_1h_v1() {
        assert_golden(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_1H_V1,
            97,
            GOLDEN_OI_1H,
            "fe2424248daee909",
        );
    }

    #[test]
    fn golden_trigger_oi_expansion_rejection_4h_v1() {
        assert_golden(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_4H_V1,
            25,
            GOLDEN_OI_4H,
            "1a4237713c25f86b",
        );
    }

    #[test]
    fn golden_trigger_oi_expansion_rejection_1d_v1() {
        assert_golden(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_1D_V1,
            7,
            GOLDEN_OI_1D,
            "752615bfc4555f80",
        );
    }

    #[test]
    fn golden_trigger_oi_expansion_rejection_prior_day_v1() {
        assert_golden(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1,
            16,
            GOLDEN_OI_PRIOR_DAY,
            "ebb7d51bf76fb780",
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

    const GOLDEN_CVD_DIVERGENCE_15M: &str = "64809276ms cvd_divergence evidence=[cvd_divergence 15m low prev=61969.20000000@56332199ms cur=61966.54000000@61514129ms dprice=-2.66000000 cvd=-11.99608482->-9.75701857 dcvd=2.23906625 gaps=0 span=5181930ms complete divergent=true candidate.divergence.cvd.15m@1 ctx=61966.54000000 levels=sfp_zone:low@61969.20000000[61966.54000000,61969.20000000],prior_sweep:low@61969.20000000,vwap@61983.67574530 location.levels@2 auction=warming] trigger.cvd_divergence.15m@1 ctx=61966.54000000 levels=vwap@61983.67574530 location.levels@2 auction=warming bias regime=warming 15m=[high=- low=lower@61200000ms] 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_CVD_DIVERGENCE_1H: &str = "198042133ms cvd_divergence evidence=[cvd_divergence 1h high prev=61947.27000000@107168767ms cur=62057.32000000@185552262ms dprice=110.05000000 cvd=63.52029271->60.27519949 dcvd=-3.24509322 gaps=0 span=78383495ms complete divergent=true candidate.divergence.cvd.1h@1 ctx=62057.32000000 levels=prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.03772028,prior_sweep:high@62033.71000000,structural_high:high@62049.17000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value] trigger.cvd_divergence.1h@1 ctx=62057.32000000 levels=prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.03772028,prior_sweep:high@62033.71000000,structural_high:high@62049.17000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000] location.levels@2 auction=outside_value bias regime=warming 15m=[high=lower@192600000ms low=higher@189900000ms] 1h=[high=higher@183600000ms low=higher@158400000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_CVD_DIVERGENCE_4H: &str = "374447759ms cvd_divergence evidence=[cvd_divergence 4h high prev=62057.32000000@185552262ms cur=62140.03000000@321483259ms dprice=82.71000000 cvd=60.27519949->38.05121265 dcvd=-22.22398684 gaps=0 span=135930997ms complete divergent=true candidate.divergence.cvd.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000,structural_high:high@62140.03000000,structural_high:high@62140.03000000 location.levels@2 auction=unavailable] trigger.cvd_divergence.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,vwap@62112.68118136,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],structural_high:high@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],structural_high:high@62136.28000000,structural_high:high@62136.28000000 location.levels@2 auction=unavailable bias regime=warming 15m=[high=lower@367200000ms low=lower@370800000ms] 1h=[high=lower@356400000ms low=higher@338400000ms] 4h=[high=higher@316800000ms low=higher@230400000ms] 1d=warming bias.context@1";

    const GOLDEN_CVD_DIVERGENCE_1D: &str = "1209708595ms value last=- trigger.cvd_divergence.1d@1";

    const GOLDEN_EFFORT_RESULT_5M: &str = "188300455ms effort_vs_result evidence=[effort_result 5m end=188280000ms aggressor=sell effort=3.13009011 progress=-1.39000000 effort_high=2.60044813 effort_median=1.39537000 move_high=6.51000000 move_median=3.50000000 complete active=true candidate.effort_result.5m@1 ctx=62037.54000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62029.67532057,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000] location.levels@2 auction=outside_value] trigger.effort_result.5m@1 ctx=62037.54000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62029.67532057,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000] location.levels@2 auction=outside_value bias regime=warming 15m=[high=higher@181800000ms low=higher@179100000ms] 1h=[high=lower@104400000ms low=higher@158400000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_EFFORT_RESULT_15M: &str = "189553603ms effort_vs_result evidence=[effort_result 15m end=189540000ms aggressor=sell effort=9.48881729 progress=4.33000000 effort_high=4.70789481 effort_median=2.62103277 move_high=10.92000000 move_median=6.00000000 complete active=true candidate.effort_result.15m@1 ctx=62034.73000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.25160864,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value] trigger.effort_result.15m@1 ctx=62034.73000000 levels=prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.25160864,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value bias regime=warming 15m=[high=higher@185400000ms low=higher@179100000ms] 1h=[high=lower@104400000ms low=higher@158400000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_EFFORT_RESULT_1H: &str = "194941793ms effort_vs_result evidence=[effort_result 1h end=194940000ms aggressor=sell effort=9.95302085 progress=-16.50000000 effort_high=9.69932969 effort_median=5.35401407 move_high=24.72000000 move_median=10.39000000 complete active=true candidate.effort_result.1h@1 ctx=62041.38000000 levels=sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],structural_low:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62031.11563947,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value] trigger.effort_result.1h@1 ctx=62041.38000000 levels=sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],structural_low:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],structural_low:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62031.11563947,prior_sweep:high@62033.71000000,prior_sweep:high@62049.24000000,sfp_zone:high@62049.24000000[62049.24000000,62057.32000000],structural_high:high@62057.32000000 location.levels@2 auction=outside_value bias regime=warming 15m=[high=higher@185400000ms low=higher@189900000ms] 1h=[high=lower@104400000ms low=higher@158400000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_EXHAUSTION: &str = "196838221ms exhaustion evidence=[exhaustion end=196800000ms expansion=[1h end=196500000ms aggressor=sell effort=14.00580309 progress=30.02000000 effort_high=9.98765481 effort_median=5.44429252 move_high=24.72000000 move_median=10.46000000 complete active=false candidate.effort_result.1h@1] decay=[5m end=196800000ms aggressor=sell effort=0.51751975 progress=3.93000000 effort_high=2.60784460 effort_median=1.40469943 move_high=6.54000000 move_median=3.53000000 complete active=false candidate.effort_result.5m@1] decay_aggression=0.51751975 extreme=62009.55000000 close=62011.08000000 expanded=true decayed=true at_extreme=true active=true candidate.exhaustion@1 ctx=62011.08000000 levels=lvn@61975.00000000[61970.00000000,61980.00000000],prior_sweep:low@61975.11000000,prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],prior_sweep:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.78929859,prior_sweep:high@62033.71000000 location.levels@2 auction=outside_value] trigger.exhaustion@1 ctx=62011.08000000 levels=lvn@61975.00000000[61970.00000000,61980.00000000],prior_sweep:low@61975.11000000,prior_sweep:high@62000.97000000,prior_sweep:high@62000.97000000,sfp_zone:high@62000.97000000[62000.97000000,62006.01000000],hvn@62015.00000000[62010.00000000,62020.00000000],prior_sweep:low@62012.36000000,prior_sweep:high@62013.41000000,sfp_zone:high@62013.41000000[62013.41000000,62019.20000000],prior_sweep:low@62014.71000000,prior_sweep:high@62019.20000000,prior_sweep:high@62029.62000000,sfp_zone:high@62029.62000000[62029.62000000,62031.68000000],vwap@62030.78929859,prior_sweep:high@62033.71000000 location.levels@2 auction=outside_value bias regime=warming 15m=[high=lower@192600000ms low=higher@189900000ms] 1h=[high=lower@104400000ms low=higher@158400000ms] 4h=[high=- low=-] 1d=warming bias.context@1";

    const GOLDEN_ABSORPTION_5M: &str = "173705421ms absorption evidence=[absorption effort=[5m end=173700000ms aggressor=sell effort=22.20000000 progress=0.00000000 effort_high=16.04000000 effort_median=5.80000000 move_high=0.10000000 move_median=0.00000000 complete active=true candidate.effort_result.5m@1] passive=bid band=0.00050000 added=31.83000000 filled=24.02000000 book=complete replenished=true active=true candidate.absorption.5m@1 ctx=60000.10000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.03655487,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable] trigger.absorption.5m@1 ctx=60000.10000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.03655487,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable bias regime=warming 15m=warming 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_ABSORPTION_15M: &str = "179341368ms absorption evidence=[absorption effort=[15m end=179340000ms aggressor=buy effort=25.58000000 progress=0.00000000 effort_high=24.64000000 effort_median=12.76000000 move_high=0.10000000 move_median=0.00000000 complete active=true candidate.effort_result.15m@1] passive=ask band=0.00050000 added=53.00000000 filled=49.70000000 book=complete replenished=true active=true candidate.absorption.15m@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05888156,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable] trigger.absorption.15m@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05888156,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable bias regime=warming 15m=warming 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_ABSORPTION_1H: &str = "178862262ms absorption evidence=[absorption effort=[1h end=178860000ms aggressor=buy effort=64.62000000 progress=0.00000000 effort_high=51.32000000 effort_median=31.04000000 move_high=0.10000000 move_median=0.10000000 complete active=true candidate.effort_result.1h@1] passive=ask band=0.00050000 added=146.27000000 filled=144.28000000 book=complete replenished=true active=true candidate.absorption.1h@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05717431,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable] trigger.absorption.1h@1 ctx=60000.00000000 levels=cluster:bid@59984.00000000,cluster:bid@59988.00000000,cluster:bid@59992.00000000,cluster:bid@59996.00000000,cluster:bid@60000.00000000,poc@60005.00000000[60000.00000000,60010.00000000],vah@60010.00000000[60000.00000000,60010.00000000],val@60000.00000000[60000.00000000,60010.00000000],hvn@60005.00000000[60000.00000000,60010.00000000],vwap@60000.05717431,cluster:ask@60004.10000000,cluster:ask@60008.10000000,cluster:ask@60012.10000000,cluster:ask@60016.10000000,cluster:ask@60020.10000000 location.levels@2 auction=unavailable bias regime=warming 15m=warming 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_OI_15M: &str = "62161380ms oi_expansion_rejection evidence=[sfp 15m low level=61969.20000000 bar=55800000ms confirmed=59400000ms touches=2 at=61514129ms price=61966.54000000 extreme=61966.54000000 bars=1 sfp resolved=62100000ms complete structure.levels.15m@1 start=[boundary=61200000ms oi=80005.40000000 sample=61200000ms res=300000ms delta=-85.30000000 vel_per_min=-17.06 derivatives.oi.5m@1] end=[boundary=62100000ms oi=80066.10000000 sample=62100000ms res=300000ms delta=-42.20000000 vel_per_min=-8.44 derivatives.oi.5m@1] change=60.70000000] trigger.oi_expansion_rejection.15m@1 ctx=61969.20000000 levels=vwap@61984.17149090 location.levels@2 auction=warming bias regime=warming 15m=[high=- low=-] 1h=warming 4h=warming 1d=warming bias.context@1";

    const GOLDEN_OI_1H: &str = "97260667ms oi_expansion_rejection evidence=[sfp 1h low level=61900.09000000 bar=75600000ms confirmed=90000000ms touches=0 at=93129730ms price=61897.65000000 extreme=61882.07000000 bars=2 sfp resolved=97200000ms complete structure.levels.1h@1 start=[boundary=93000000ms oi=80130.10000000 sample=93000000ms res=300000ms delta=42.50000000 vel_per_min=8.5 derivatives.oi.5m@1] end=[boundary=97200000ms oi=80238.40000000 sample=97200000ms res=300000ms delta=-93.10000000 vel_per_min=-18.62 derivatives.oi.5m@1] change=108.30000000] trigger.oi_expansion_rejection.1h@1 ctx=61900.09000000 levels=vwap@61914.46220263,hvn@61925.00000000[61920.00000000,61930.00000000],prior_sweep:low@61920.27000000,val@61930.00000000[61930.00000000,61940.00000000] location.levels@2 auction=breakout bias regime=warming 15m=[high=higher@87300000ms low=higher@84600000ms] 1h=[high=- low=-] 4h=warming 1d=warming bias.context@1";

    const GOLDEN_OI_4H: &str = "403209273ms oi_expansion_rejection evidence=[sfp 4h high level=62140.03000000 bar=316800000ms confirmed=374400000ms touches=0 at=385610639ms price=62142.90000000 extreme=62171.37000000 bars=2 sfp resolved=403200000ms complete structure.levels.4h@1 start=[boundary=385500000ms oi=78670.00000000 sample=385500000ms res=300000ms delta=-24.30000000 vel_per_min=-4.86 derivatives.oi.5m@1] end=[boundary=402900000ms oi=78833.10000000 sample=402900000ms res=300000ms delta=51.10000000 vel_per_min=10.22 derivatives.oi.5m@1] change=163.10000000] trigger.oi_expansion_rejection.4h@1 ctx=62140.03000000 levels=sfp_zone:low@62108.54000000[62095.20000000,62108.54000000],vah@62110.00000000[62100.00000000,62110.00000000],sfp_zone:low@62110.52000000[62100.74000000,62110.52000000],prior_sweep:high@62103.29000000,structural_low:low@62107.31000000,prior_sweep:low@62108.54000000,prior_sweep:low@62110.52000000,prior_sweep:high@62113.10000000,sfp_zone:high@62113.10000000[62113.10000000,62117.71000000],prior_sweep:high@62117.71000000,sfp_zone:high@62117.71000000[62117.71000000,62117.72000000],vwap@62124.77724970,prior_sweep:high@62128.26000000,structural_low:low@62128.71000000,sfp_zone:low@62130.28000000[62128.71000000,62130.28000000],prior_sweep:high@62130.23000000,prior_sweep:high@62130.28000000,prior_sweep:low@62130.28000000,prior_sweep:high@62134.29000000,sfp_zone:high@62134.29000000[62134.29000000,62136.28000000],prior_sweep:high@62136.28000000,prior_sweep:high@62136.28000000,prior_sweep:high@62160.98000000,sfp_zone:high@62160.98000000[62160.98000000,62171.37000000],structural_high:high@62171.37000000 location.levels@2 auction=unavailable bias regime=warming 15m=[high=higher@396900000ms low=lower@399600000ms] 1h=[high=lower@356400000ms low=higher@370800000ms] 4h=[high=higher@316800000ms low=higher@230400000ms] 1d=warming bias.context@1";

    const GOLDEN_OI_1D: &str = "86450066ms value warming 1/7";

    const GOLDEN_OI_PRIOR_DAY: &str = "243847289ms oi_expansion_rejection evidence=[failed_auction failed_breakout ref=86400000ms val=61450.00000000 vah=62040.00000000 close=61994.59000000 at=in side=above a=in/accepted failed=above:56/1 feed_gap location.auction.prior_day@1 start=[boundary=240000000ms oi=79815.00000000 sample=240000000ms res=300000ms delta=1.30000000 vel_per_min=0.26 derivatives.oi.5m@1] end=[boundary=243600000ms oi=79840.50000000 sample=243600000ms res=300000ms delta=7.10000000 vel_per_min=1.42 derivatives.oi.5m@1] change=25.50000000] trigger.oi_expansion_rejection.prior_day@1 ctx=62040.00000000 levels=hvn@62005.00000000[62000.00000000,62010.00000000],vwap@62016.40530259,prior_sweep:high@62029.35000000,prior_sweep:high@62029.35000000,vah@62040.00000000[62030.00000000,62040.00000000],lvn@62065.00000000[62060.00000000,62070.00000000] location.levels@2 auction=failed_breakout bias regime=warming 15m=[high=higher@224100000ms low=higher@233100000ms] 1h=[high=higher@223200000ms low=lower@216000000ms] 4h=[high=- low=-] 1d=warming bias.context@1";
}
