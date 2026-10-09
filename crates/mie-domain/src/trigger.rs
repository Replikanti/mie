//! Bias and trigger (brief §11, ADR-012, ADR-023, ADR-024).
//!
//! A bias describes what the broader evidence suggests and never authorizes
//! execution. Only a trigger — an observable event at a defined location —
//! can, and only through the validation pipeline. Order-flow features feed
//! triggers; they are not strategy authority on their own, and aggression is
//! not direction.
//!
//! The bias representation and the deterministic detection rule of each
//! trigger family are owned by the bias/trigger issue.

/// Families of observable trigger events that hypotheses can test.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TriggerFamily {
    /// Failed auction: a probe beyond value rejected back inside.
    ///
    /// The location fact is [`AuctionState::FailedBreakout`](crate::location::AuctionState::FailedBreakout)
    /// of `location.auction.prior_day@1` (ADR-044); the trigger rule is
    /// owned by the bias/trigger issue.
    FailedAuction,
    /// Swing failure pattern: a sweep of a swing level followed by rejection.
    ///
    /// The structure fact is [`StructureEvent::Sfp`](crate::structure::StructureEvent::Sfp)
    /// (ADR-037); the trigger rule — location, bias, parameter version — is
    /// owned by the bias/trigger issue.
    Sfp,
    /// Aggression absorbed by passive liquidity.
    Absorption,
    /// Aggression fading out at an extreme.
    Exhaustion,
    /// Aggression without price progress (effort vs result).
    EffortVsResult,
    /// Cumulative volume delta diverging from price.
    CvdDivergence,
    /// Open-interest expansion followed by rejection.
    OiExpansionRejection,
    /// Failed reclaim of a lost level.
    ///
    /// The location fact is [`AuctionState::FailedReclaim`](crate::location::AuctionState::FailedReclaim)
    /// (ADR-044); the trigger rule is owned by the bias/trigger issue.
    FailedReclaim,
    /// Breakout followed by acceptance.
    ///
    /// The location fact is a [`Breakout`](crate::location::AuctionState::Breakout)
    /// that turns into [`Acceptance`](crate::location::AuctionState::Acceptance)
    /// (ADR-044); the trigger rule is owned by the bias/trigger issue.
    BreakoutAcceptance,
}
