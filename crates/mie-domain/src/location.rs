//! Location and auction state (brief §10).
//!
//! Location answers "where is price relative to meaningful levels?". It is
//! monitored continuously, whether or not any trade is authorized.
//!
//! These enums are the design vocabulary. The volume-profile levels (POC,
//! VAH, VAL, HVN, LVN) are defined by [`crate::profile`] (ADR-036), which
//! hands them over as [`ProfileLevel`](crate::profile::ProfileLevel)s. The
//! structural highs and lows, prior sweeps and SFP rejection zones are
//! defined by [`crate::structure`] (ADR-037), which hands them over as
//! [`StructureLevel`](crate::structure::StructureLevel)s. The measurable
//! definition of every other level and of each auction state (windows,
//! thresholds, what counts as acceptance) is owned by the location issue
//! and must be deterministic.

/// Kinds of levels the location subsystem maintains and scores.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LevelKind {
    /// Volume-profile point of control: the bin with the most volume
    /// ([`crate::profile`], ADR-036, decision 4).
    Poc,
    /// Value-area high: the upper (exclusive) edge of the value area
    /// ([`crate::profile`], ADR-036, decision 5).
    Vah,
    /// Value-area low: the lower edge of the value area
    /// ([`crate::profile`], ADR-036, decision 5).
    Val,
    /// High-volume node: a prominent peak of the smoothed profile
    /// ([`crate::profile`], ADR-036, decision 6).
    Hvn,
    /// Low-volume node: a prominent interior valley of the smoothed profile
    /// ([`crate::profile`], ADR-036, decision 6).
    Lvn,
    /// Structural swing high: a confirmed swing high no trade has gone
    /// beyond yet ([`crate::structure`], ADR-037, decisions 2–5).
    StructuralHigh,
    /// Structural swing low: a confirmed swing low no trade has gone below
    /// yet ([`crate::structure`], ADR-037, decisions 2–5).
    StructuralLow,
    /// Cluster of resting liquidity.
    LiquidityCluster,
    /// Level of a prior liquidity sweep: a structural level a trade went
    /// beyond ([`crate::structure`], ADR-037, decision 6).
    PriorSweep,
    /// Swing-failure-pattern / rejection zone: from a swept level to the
    /// sweep's extreme, after a close back inside ([`crate::structure`],
    /// ADR-037, decision 7).
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
