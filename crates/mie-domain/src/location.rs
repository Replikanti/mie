//! Location and auction state (brief §10; ADR-044, proposed).
//!
//! Location answers "where is price relative to meaningful levels?". It is
//! monitored continuously, whether or not any trade is authorized, on the
//! clock of closed 1m bars (decision 1): an empty bar neither counts nor
//! fails anything.
//!
//! - `location.vwap.utc_day@1` ([`Vwap`], decision 4): the volume-weighted
//!   average price of the current UTC day over its closed minutes, exact in
//!   `i128` and floored to [`Price`](crate::num::Price).
//!
//! The volume-profile levels (POC, VAH, VAL, HVN, LVN) are defined by
//! [`crate::profile`] (ADR-036), which hands them over as
//! [`ProfileLevel`](crate::profile::ProfileLevel)s. The structural highs and
//! lows, prior sweeps and SFP rejection zones are defined by
//! [`crate::structure`] (ADR-037), which hands them over as
//! [`StructureLevel`](crate::structure::StructureLevel)s.

mod vwap;

pub use vwap::Vwap;
pub(crate) use vwap::{VwapStep, VwapTracker};

use crate::bars::Bar;
use crate::event::MarketEvent;
use crate::feature::FeatureValue;
use crate::fingerprint::Fingerprinter;
use crate::state_hash::StateEncode;
use std::fmt;

/// Kinds of levels the location subsystem maintains and scores.
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
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
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
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

/// The location features of the Market State (ADR-044).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationState {
    /// `location.vwap.utc_day@1`, warming up until a minute of the current
    /// UTC day with volume closes.
    pub vwap: FeatureValue<Vwap>,
}

impl StateEncode for LocationState {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { vwap } = self;
        vwap.encode(f);
    }
}

impl Default for LocationState {
    fn default() -> Self {
        Self::new()
    }
}

impl LocationState {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            vwap: vwap::warming(),
        }
    }
}

/// The location engine state (ADR-044): the VWAP sums. Engine state; the
/// Market State exposes only [`LocationState`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct LocationTracker {
    pub(crate) vwap: VwapTracker,
}

/// The fallible part of one event's location step, committed with the other
/// families (decision 2).
pub(crate) struct LocationStep {
    vwap: VwapStep,
}

impl LocationTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Steps the VWAP with `event`, given the bars it closed, without
    /// changing the tracker (decision 4).
    ///
    /// # Errors
    ///
    /// [`LocationError::Overflow`] if a VWAP sum leaves its range; nothing is
    /// committed then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
    ) -> Result<LocationStep, LocationError> {
        Ok(LocationStep {
            vwap: self.vwap.step(event, closed)?,
        })
    }

    /// Commits a step computed by [`Self::step`] into `state`. Copies only:
    /// it cannot fail.
    pub(crate) fn commit(&mut self, step: LocationStep, state: &mut LocationState) {
        self.vwap.commit(step.vwap, &mut state.vwap);
    }
}

/// Why location could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationError {
    /// A VWAP sum left its integer range (ADR-027, ADR-044 decision 4).
    Overflow,
}

impl fmt::Display for LocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("a location value leaves its integer range"),
        }
    }
}

impl std::error::Error for LocationError {}
