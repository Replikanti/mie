//! Strategy lifecycle and edge health (ADR-008, ADR-016; Strategy Registry &
//! Suitability brief).
//!
//! The registry owns these semantics; persistence sits behind the
//! `StrategyRepository` port. Allowed transitions and the evidence each
//! promotion requires are owned by the registry issue.

/// Lifecycle state of a strategy candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LifecycleState {
    /// Hypothesis under research.
    Research,
    /// Backtested on historical replay.
    Backtested,
    /// Passed walk-forward validation.
    WalkForward,
    /// Running on live data without execution.
    Paper,
    /// Validated, including the unseen forward test.
    Validated,
    /// Eligible for suitability evaluation at runtime.
    Active,
    /// Live performance is deteriorating.
    Degrading,
    /// Withdrawn from suitability pending review.
    Suspended,
    /// Retired for good.
    Deprecated,
}

/// Health of a validated strategy's edge, monitored live (edge decay is a
/// first-class state, ADR-016).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EdgeHealth {
    /// Performing within its validation distributions.
    Healthy,
    /// Drifting; under closer observation.
    Watch,
    /// Measurably degraded.
    Degrading,
    /// Suspended from use.
    Suspended,
    /// Edge considered gone.
    Deprecated,
}
