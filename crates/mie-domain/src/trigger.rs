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
    FailedAuction,
    /// Swing failure pattern: a sweep of a swing level followed by rejection.
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
    FailedReclaim,
    /// Breakout followed by acceptance.
    BreakoutAcceptance,
}
