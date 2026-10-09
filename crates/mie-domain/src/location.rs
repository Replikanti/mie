//! Location and auction state (brief §10; ADR-044, proposed).
//!
//! Location answers "where is price relative to meaningful levels?". It is
//! monitored continuously, whether or not any trade is authorized, on the
//! clock of closed 1m bars (decision 1): an empty bar neither counts nor
//! fails anything.
//!
//! - `location.vwap.utc_day@1` ([`Vwap`], decision 4): the volume-weighted
//!   average price of the current UTC day over its closed minutes, exact in
//!   `i128` and floored to [`Price`].
//! - `location.levels@1` ([`LevelSet`], decisions 5–7): every level location
//!   monitors, rebuilt at each closed 1m bar with trades from the prior-day
//!   and 5-day composite profiles, the structure registries, the book's
//!   cluster candidates and the VWAP. Each level carries its score
//!   components — distance, touches, age, source, confluence, strength —
//!   and no scalar score (ADR-013). A level is **in zone** while the closed
//!   bar's range overlaps its zone padded by the tolerance
//!   ([`TOLERANCE_BPS`], decision 3); entering and leaving are
//!   [`LocationEvent`]s, exposed by
//!   [`MarketStateEngine::location_events`](crate::state::MarketStateEngine::location_events).
//!   Liquidity clusters are scored but not monitored.
//! - `location.auction.prior_day@1` ([`AuctionStatus`], decisions 8 and 9):
//!   the auction state of each closed 1m bar with trades against the prior
//!   day's value area — [`AuctionState`] from a measurable definition: a
//!   region or an edge band per close, probes that count closes beyond an
//!   edge, failure on a close back, acceptance at [`ACCEPTANCE_CLOSES`]
//!   counted closes. Its transitions are [`LocationEvent::Auction`] facts.
//!
//! Location is the last stage of an event (decision 2): the VWAP steps with
//! the other trackers and can fail; the registry runs after every family
//! has committed, on the committed state of the same event, and cannot
//! fail. Every visibility time is the time of the event that closed the
//! bar, never a bar end.
//!
//! The volume-profile levels (POC, VAH, VAL, HVN, LVN) are defined by
//! [`crate::profile`] (ADR-036), which hands them over as
//! [`ProfileLevel`](crate::profile::ProfileLevel)s. The structural highs and
//! lows, prior sweeps and SFP rejection zones are defined by
//! [`crate::structure`] (ADR-037), which hands them over as
//! [`StructureLevel`](crate::structure::StructureLevel)s.

mod auction;
mod registry;
mod vwap;

pub use auction::{
    ACCEPTANCE_CLOSES, AuctionClassifier, AuctionStatus, Classified, Failure, Origin, Position,
    Probe, Region,
};
pub use registry::{ClusterStrength, EnterCause, LeaveCause, LevelSet, LevelSide, LocationLevel};
pub use vwap::Vwap;
pub(crate) use vwap::{VwapStep, VwapTracker};

use crate::bars::{Bar, Timeframe};
use crate::event::MarketEvent;
use crate::feature::FeatureValue;
use crate::fingerprint::Fingerprinter;
use crate::liquidity::BookState;
use crate::num::Price;
use crate::profile::VolumeProfiles;
use crate::state_hash::StateEncode;
use crate::structure::StructureSet;
use crate::time::EventTime;
use std::fmt;

/// The location tolerance `w` in basis points of the level's price: the
/// edge bands of the auction classifier and the padding of a monitored
/// zone (ADR-044, decision 3). Parameter `tolerance` = 0.0006 of
/// `location.levels@1` and `location.auction.prior_day@1`.
pub const TOLERANCE_BPS: i64 = 6;

/// Whether `gap` (in `1e-8` USDT; negative means overlap) is at most
/// `tolerance_bps` basis points of `reference`: `gap · 10 000 ≤ |reference|
/// · tolerance_bps`, exact in `i128` (decision 3).
pub(crate) fn within(gap: i128, reference: Price, tolerance_bps: i64) -> bool {
    gap * 10_000 <= i128::from(reference.units()).abs() * i128::from(tolerance_bps)
}

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

impl LevelKind {
    /// The state-hash code, in declaration order.
    pub(crate) fn code(self) -> u8 {
        match self {
            Self::Poc => 0,
            Self::Vah => 1,
            Self::Val => 2,
            Self::Hvn => 3,
            Self::Lvn => 4,
            Self::StructuralHigh => 5,
            Self::StructuralLow => 6,
            Self::LiquidityCluster => 7,
            Self::PriorSweep => 8,
            Self::SfpRejectionZone => 9,
            Self::Vwap => 10,
            Self::ValidatedReference => 11,
        }
    }
}

impl StateEncode for LevelKind {
    /// `write_u8` in declaration order: Poc 0 … ValidatedReference 11
    /// (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(self.code());
    }
}

impl fmt::Display for LevelKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Poc => "poc",
            Self::Vah => "vah",
            Self::Val => "val",
            Self::Hvn => "hvn",
            Self::Lvn => "lvn",
            Self::StructuralHigh => "structural_high",
            Self::StructuralLow => "structural_low",
            Self::LiquidityCluster => "cluster",
            Self::PriorSweep => "prior_sweep",
            Self::SfpRejectionZone => "sfp_zone",
            Self::Vwap => "vwap",
            Self::ValidatedReference => "validated_reference",
        })
    }
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

impl AuctionState {
    /// Every state, in declaration order.
    pub const ALL: [Self; 7] = [
        Self::InsideValue,
        Self::AtValueEdge,
        Self::OutsideValue,
        Self::Breakout,
        Self::FailedBreakout,
        Self::FailedReclaim,
        Self::Acceptance,
    ];
}

impl StateEncode for AuctionState {
    /// `write_u8` in declaration order: InsideValue 0 … Acceptance 6
    /// (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::InsideValue => 0,
            Self::AtValueEdge => 1,
            Self::OutsideValue => 2,
            Self::Breakout => 3,
            Self::FailedBreakout => 4,
            Self::FailedReclaim => 5,
            Self::Acceptance => 6,
        });
    }
}

impl fmt::Display for AuctionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::InsideValue => "inside_value",
            Self::AtValueEdge => "at_value_edge",
            Self::OutsideValue => "outside_value",
            Self::Breakout => "breakout",
            Self::FailedBreakout => "failed_breakout",
            Self::FailedReclaim => "failed_reclaim",
            Self::Acceptance => "acceptance",
        })
    }
}

/// The location features of the Market State (ADR-044).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationState {
    /// `location.vwap.utc_day@1`, warming up until a minute of the current
    /// UTC day with volume closes.
    pub vwap: FeatureValue<Vwap>,
    /// `location.levels@1`, warming up until the first closed 1m bar with
    /// trades.
    pub levels: FeatureValue<LevelSet>,
    /// `location.auction.prior_day@1`, warming up until the first closed 1m
    /// bar with trades under a ready prior day, and again from each new
    /// prior day until its first close; `Unavailable(InputInvalid)` while
    /// the prior day is unavailable or its value area is not wider than
    /// both edge bands.
    pub auction: FeatureValue<AuctionStatus>,
}

impl StateEncode for LocationState {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            vwap,
            levels,
            auction,
        } = self;
        vwap.encode(f);
        levels.encode(f);
        auction.encode(f);
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
            levels: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            auction: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
        }
    }
}

/// A monitored-location fact, in emission order per closed bar: the
/// auction transition, then every [`Left`](Self::Left), then every
/// [`Entered`](Self::Entered), each in registry order (decision 7).
///
/// Not hashed: the in-zone flags and the auction state they follow from are
/// part of the state.
/// `Display` prints the canonical line the golden tests pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LocationEvent {
    /// A level's zone was entered.
    Entered {
        /// The level after the bar.
        level: LocationLevel,
        /// Why.
        cause: EnterCause,
        /// Time of the event that closed the bar.
        known_at: EventTime,
        /// End of the bar.
        bar_end: EventTime,
    },
    /// A level's zone was left.
    Left {
        /// The level after the bar, or as it was last held when it retired.
        level: LocationLevel,
        /// Why.
        cause: LeaveCause,
        /// Time of the event that closed the bar.
        known_at: EventTime,
        /// End of the bar.
        bar_end: EventTime,
    },
    /// The auction state changed with a close (decision 8).
    Auction {
        /// The previous close's state under the same prior day; `None` for
        /// the first close under it.
        from: Option<AuctionState>,
        /// The state after the close.
        to: AuctionState,
        /// Time of the event that closed the bar.
        known_at: EventTime,
        /// End of the bar.
        bar_end: EventTime,
    },
}

impl LocationEvent {
    /// When the fact became visible: the time of the event that closed the
    /// bar, never the bar end (decision 2).
    pub fn time(&self) -> EventTime {
        match self {
            Self::Entered { known_at, .. }
            | Self::Left { known_at, .. }
            | Self::Auction { known_at, .. } => *known_at,
        }
    }
}

impl fmt::Display for LocationEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Entered {
                level,
                cause,
                bar_end,
                ..
            } => write!(f, "entered {cause:?} bar={bar_end} {level}"),
            Self::Left {
                level,
                cause,
                bar_end,
                ..
            } => write!(f, "left {cause:?} bar={bar_end} {level}"),
            Self::Auction {
                from, to, bar_end, ..
            } => match from {
                Some(from) => write!(f, "auction {from}->{to} bar={bar_end}"),
                None => write!(f, "auction ->{to} bar={bar_end}"),
            },
        }
    }
}

/// The location engine state (ADR-044): the VWAP sums and the auction
/// classifier. Engine state; the Market State exposes only
/// [`LocationState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LocationTracker {
    pub(crate) vwap: VwapTracker,
    auction: AuctionClassifier,
}

impl Default for LocationTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// The fallible part of one event's location step, committed with the other
/// families (decision 2).
pub(crate) struct LocationStep {
    vwap: VwapStep,
}

impl LocationTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            vwap: VwapTracker::default(),
            auction: AuctionClassifier::new(TOLERANCE_BPS, ACCEPTANCE_CLOSES),
        }
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

    /// The infallible stage (decision 2): after every family has committed
    /// `event`, steps the auction classifier through `closed` — the old
    /// day's closes against the old reference, then the new prior day —
    /// and rebuilds the level registry at the closed 1m bar with trades,
    /// appending the auction transition and the zone events to `events`.
    /// Empty bars neither count nor fail anything.
    pub(crate) fn locate(
        &mut self,
        event: &MarketEvent,
        closed: &[Bar],
        inputs: Inputs<'_>,
        state: &mut LocationState,
        events: &mut Vec<LocationEvent>,
    ) {
        let known_at = event.time();
        if closed.is_empty() {
            return;
        }
        self.auction
            .step(closed, &inputs.profile.prior_day, known_at, |classified| {
                let to = classified.status.state;
                if classified.from != Some(to) {
                    events.push(LocationEvent::Auction {
                        from: classified.from,
                        to,
                        known_at,
                        bar_end: classified.status.bar_end,
                    });
                }
            });
        state.auction = *self.auction.status();
        // At most one: a bar with trades is closed by the next trades-stream
        // event, before any later minute could fill.
        let minute = closed.iter().rev().find_map(|bar| match bar.ohlc {
            Some(ohlc) if bar.timeframe == Timeframe::M1 => Some((bar, ohlc)),
            _ => None,
        });
        let Some((bar, ohlc)) = minute else {
            return;
        };
        let sourced = registry::sourced(inputs.profile, inputs.structure, inputs.book, &state.vwap);
        let (set, zone) = registry::rebuild(state.levels.ready(), sourced, bar, &ohlc, known_at);
        let bar_end = bar.end();
        events.extend(
            zone.left
                .into_iter()
                .map(|(level, cause)| LocationEvent::Left {
                    level,
                    cause,
                    known_at,
                    bar_end,
                }),
        );
        events.extend(
            zone.entered
                .into_iter()
                .map(|(level, cause)| LocationEvent::Entered {
                    level,
                    cause,
                    known_at,
                    bar_end,
                }),
        );
        state.levels = FeatureValue::Ready(set);
    }
}

/// The committed state location reads its levels from (decision 5).
pub(crate) struct Inputs<'a> {
    /// The volume profiles.
    pub(crate) profile: &'a VolumeProfiles,
    /// The structure registries.
    pub(crate) structure: &'a StructureSet,
    /// The order book's cluster candidates.
    pub(crate) book: &'a BookState,
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

#[cfg(test)]
pub(crate) mod tests {
    use crate::bars::tests::Lcg;
    use crate::event::samples::t;
    use crate::event::{Aggressor, GapReason, MarketEvent, Stream, Trade};
    use crate::num::{Price, Qty, SCALE};

    /// The location golden tape: `days` UTC days from 13:00 UTC on day 0 (a
    /// partial day) of trades every 10–60 s on a random walk with an
    /// hourly trend that swings over the day, so price leaves and re-enters
    /// the prior day's value; a trades gap on day 2.
    pub(crate) fn walk_tape(seed: u64, days: i64) -> Vec<MarketEvent> {
        const HOUR: i64 = 3_600_000;
        let mut lcg = Lcg(seed);
        let mut events = Vec::new();
        let mut time = 13 * HOUR;
        let mut trade_id = 0;
        let mut gapped = false;
        // In cents.
        let mut price: i64 = 6_200_000;
        while time < days * 86_400_000 {
            time += 10_000 + lcg.below(50_000);
            if !gapped && time >= 2 * 86_400_000 + 9 * HOUR {
                let end = time + 25 * 60_000;
                events.push(MarketEvent::FeedGap(crate::event::FeedGap {
                    stream: Stream::Trades,
                    start: t(time),
                    end: t(end),
                    reason: GapReason::Disconnected,
                }));
                time = end + 1;
                gapped = true;
            }
            // A trend of up to ±4 USDT per trade, its sign and size set per
            // hour, on noise of ±12 USDT.
            let hour = time / HOUR;
            let trend = ((hour * 7 + seed as i64) % 9) - 4;
            price += trend * 100 + lcg.below(2_401) - 1_200;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(time),
                trade_id,
                price: Price::from_units(price * (SCALE / 100)),
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
}
