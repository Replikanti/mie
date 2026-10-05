//! Location and auction state (brief §10).
//!
//! Location answers "where is price relative to meaningful levels?". It is
//! monitored continuously, whether or not any trade is authorized.
//!
//! These enums are the design vocabulary. The measurable definition of each
//! level and auction state (windows, thresholds, what counts as acceptance) is
//! owned by the location issue and must be deterministic.

/// Kinds of levels the location subsystem maintains and scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LevelKind {
    /// Volume-profile point of control.
    Poc,
    /// Value-area high.
    Vah,
    /// Value-area low.
    Val,
    /// High-volume node.
    Hvn,
    /// Low-volume node.
    Lvn,
    /// Structural swing high.
    StructuralHigh,
    /// Structural swing low.
    StructuralLow,
    /// Cluster of resting liquidity.
    LiquidityCluster,
    /// Level of a prior liquidity sweep.
    PriorSweep,
    /// Swing-failure-pattern / rejection zone.
    SfpRejectionZone,
    /// Volume-weighted average price.
    Vwap,
    /// Any other reference level, admitted only after validation.
    ValidatedReference,
}

/// Auction state of price relative to value, as range analysis must
/// distinguish it (brief §10).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AuctionState {
    /// Inside the value area.
    InsideValue,
    /// At a value-area edge (VAH or VAL).
    AtValueEdge,
    /// Outside the value area.
    OutsideValue,
    /// Breaking out of value.
    Breakout,
    /// Breakout that failed back into value (failed auction).
    FailedBreakout,
    /// Failed attempt to reclaim a lost level.
    FailedReclaim,
    /// Price accepted at a new level.
    Acceptance,
}
