//! Backtest simulation on spec text v1 (ADR-060).
//!
//! The [`Simulator`] turns the decisions of one experiment spec into trade
//! records: entry at the first trade after the decision and its latency,
//! fixed slippage and the taker fee on both legs, recorded funding, stop,
//! target and time exits, and censoring by ADR-055 D6's rules applied to the
//! position (ADR-060 D4–D10). [`metrics`] computes ADR-060 D11's metrics
//! and the regime split from the records, and the `text` submodule renders
//! and parses the lines of the `backtest` outcome kind (D12) that
//! `research::result` embeds (#168). [`rules`] holds the rule registry v1
//! (D2): [`resolve`] turns a spec's rule references into a [`Plan`], and
//! [`Plan::intents`] decides D4's checks 1, 2, 4 and 5 for each emission of
//! the plan's trigger id (D3–D5). The pipeline that drives them is a
//! separate step (#170).
//!
//! - *Simulation only.* The MVP has no execution path (ADR-010): nothing
//!   here is an order or a fill report, only the prices a resting order
//!   would have met on the recorded trades.
//! - *Direction lives here.* Trigger, candidate, record and label types
//!   carry no side (ADR-053 D1, ADR-055 D7). The side of a hypothesis is in
//!   the spec and in this module only (ADR-060 D3).
//! - *Isolation.* The simulator reads the trades after each decision, the
//!   path ADR-055 D11 closes for labels. No core file outside ADR-060 D15's
//!   allow-list may name this module (`tools/check-architecture.sh`), and
//!   this module names no label type.
//! - *Purity and exactness.* Pure and deterministic like the rest of the
//!   domain (ADR-025); every value is integer fixed point with one
//!   half-to-even rounding (ADR-027, ADR-060 D10). Floats are denied for the
//!   whole module tree below.

#![deny(clippy::float_arithmetic)]

pub mod metrics;
pub mod rules;
pub mod simulator;
pub(crate) mod text;

pub use metrics::{BacktestMetrics, Metrics, REGIME_BUCKETS};
pub use rules::{Plan, ResolutionError, resolve};
pub use simulator::Simulator;
pub use text::{event_kind_from_name, event_kind_name};

use crate::num::{Price, Rate, SCALE};
use crate::order::CanonicalKey;
use crate::regime::RegimeLabel;
use crate::time::EventTime;

/// The side of a position (ADR-060 D2, D3): σ is +1 for a long and −1 for
/// a short.
///
/// Not named `Side`: the D3 side mapping reads `structure::Side` (a high or
/// a low) next to it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PositionSide {
    /// Profits when the price rises.
    Long,
    /// Profits when the price falls.
    Short,
}

impl PositionSide {
    /// σ: +1 for a long, −1 for a short (ADR-060 D2).
    pub const fn sign(self) -> i64 {
        match self {
            Self::Long => 1,
            Self::Short => -1,
        }
    }

    /// The `side=` value of the trade line (ADR-060 D12).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

/// The funding rule of a spec (ADR-060 D2, D7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FundingRule {
    /// `funding.recorded@1`: every recorded settlement inside a position's
    /// booking span is booked, and a funding gap that meets the span
    /// censors the trade.
    Recorded,
    /// `funding.none@1`: no funding is booked, and funding gaps change
    /// nothing.
    None,
}

/// Why [`Costs::new`] refused its values: each lies outside ADR-060 D2's
/// domain of definition or ADR-040's fee range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostsError {
    /// The slippage rate is not in `[0, 1)`: at 1 or above a fill price is
    /// at or below 0 (D2).
    Slippage,
    /// The taker fee is not in `[0, 1)` (ADR-040 D1).
    Taker,
    /// `max_hold_ms` is below 1 ms (D2).
    MaxHold,
    /// The latency is not below `max_hold_ms`, so no entry can fill (D2,
    /// D6).
    Latency,
}

/// The resolved cost and timing assumptions of a spec (ADR-060 D2, D6, D7).
///
/// Built only through [`new`](Self::new), which keeps the simulator's
/// arithmetic total: every value is inside its domain of definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Costs {
    latency_ms: u32,
    max_hold_ms: i64,
    slippage: Rate,
    taker: Rate,
    funding: FundingRule,
}

impl Costs {
    /// The costs, or the first value outside its range in argument order:
    /// slippage and taker in `[0, 1)`, `max_hold_ms` ≥ 1 and
    /// `latency_ms` < `max_hold_ms` (ADR-060 D2).
    ///
    /// # Errors
    ///
    /// The [`CostsError`] of the first refused value.
    pub fn new(
        latency_ms: u32,
        max_hold_ms: i64,
        slippage: Rate,
        taker: Rate,
        funding: FundingRule,
    ) -> Result<Self, CostsError> {
        let fraction = |rate: Rate| (0..SCALE).contains(&rate.units());
        if !fraction(slippage) {
            return Err(CostsError::Slippage);
        }
        if !fraction(taker) {
            return Err(CostsError::Taker);
        }
        if max_hold_ms < 1 {
            return Err(CostsError::MaxHold);
        }
        if i64::from(latency_ms) >= max_hold_ms {
            return Err(CostsError::Latency);
        }
        Ok(Self {
            latency_ms,
            max_hold_ms,
            slippage,
            taker,
            funding,
        })
    }

    /// The entry latency in ms (ADR-060 D6).
    pub const fn latency_ms(&self) -> u32 {
        self.latency_ms
    }

    /// The hold from the decision to the deadline, in ms (ADR-060 D6).
    pub const fn max_hold_ms(&self) -> i64 {
        self.max_hold_ms
    }

    /// The adverse slippage rate s per leg (ADR-060 D6).
    pub const fn slippage(&self) -> Rate {
        self.slippage
    }

    /// The taker fee per leg (ADR-060 D6).
    pub const fn taker(&self) -> Rate {
        self.taker
    }

    /// The funding rule (ADR-060 D7).
    pub const fn funding(&self) -> FundingRule {
        self.funding
    }
}

/// Why [`Levels::new`] refused its prices (ADR-060 D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LevelsError {
    /// σ · (reference − stop) ≤ 0: the stop is not on the losing side.
    StopSide,
    /// σ · (target − reference) ≤ 0: the target is not on the winning side.
    TargetSide,
    /// The risk leaves the `i64` range of price units (ADR-060 D10).
    Risk,
}

/// The levels of one decision, fixed at the decision from the reference
/// (ADR-060 D5), with the risk R they define.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Levels {
    side: PositionSide,
    reference: Price,
    stop: Price,
    target: Price,
    risk: Price,
}

impl Levels {
    /// The levels, with R = σ · (reference − stop) (ADR-060 D5).
    ///
    /// # Errors
    ///
    /// [`LevelsError::StopSide`] and [`LevelsError::TargetSide`] when a
    /// level is not strictly on its side of the reference, checked in that
    /// order, and [`LevelsError::Risk`] when R leaves the `i64` unit range.
    pub fn new(
        side: PositionSide,
        reference: Price,
        stop: Price,
        target: Price,
    ) -> Result<Self, LevelsError> {
        let sigma = i128::from(side.sign());
        let units = |price: Price| i128::from(price.units());
        let risk = sigma * (units(reference) - units(stop));
        if risk <= 0 {
            return Err(LevelsError::StopSide);
        }
        if sigma * (units(target) - units(reference)) <= 0 {
            return Err(LevelsError::TargetSide);
        }
        let risk = i64::try_from(risk).map_err(|_| LevelsError::Risk)?;
        Ok(Self {
            side,
            reference,
            stop,
            target,
            risk: Price::from_units(risk),
        })
    }

    /// The side (ADR-060 D3).
    pub const fn side(&self) -> PositionSide {
        self.side
    }

    /// `trade.last_price@1` at the decision (ADR-060 D4).
    pub const fn reference(&self) -> Price {
        self.reference
    }

    /// The stop level (ADR-060 D5).
    pub const fn stop(&self) -> Price {
        self.stop
    }

    /// The target level (ADR-060 D5).
    pub const fn target(&self) -> Price {
        self.target
    }

    /// R, the planned risk in price units, always > 0 (ADR-060 D5).
    pub const fn risk(&self) -> Price {
        self.risk
    }
}

/// The outcome of ADR-060 D4's checks 1, 2, 4 and 5 for one decision, which
/// the rule evaluator ([`Plan::intents`]) decides from the spec and the
/// state at the decision. The simulator adds check 3 (`stale_reference`) and check 6
/// (`overlap`), so it enforces D4's order in one place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Check 1: a location or regime filter removes the decision.
    Filtered,
    /// Check 2: the evidence gives no side (D3).
    NoSide,
    /// Check 4: no valid risk (D5), for example an unready ATR.
    NoRisk,
    /// Check 5: the stop is not beyond the reference (D5).
    StopSide,
    /// Checks 1, 2, 4 and 5 pass, with these levels.
    Levels(Levels),
}

/// One emission of the spec's trigger id, handed to
/// [`Simulator::decide`] (ADR-060 D4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeIntent {
    /// The canonical key of the decision event (ADR-028, ADR-060 D12).
    pub decision: CanonicalKey,
    /// `trade.last_price@1` at the decision, `None` when unready (D4).
    pub reference: Option<Price>,
    /// The label of each regime feature of the spec's set at the decision,
    /// in feature-id order, `None` when unready (D11, D12). Carried into
    /// the record unchanged.
    pub regime: Vec<Option<RegimeLabel>>,
    /// D4's checks 1, 2, 4 and 5.
    pub admission: Admission,
}

/// Why a trade is censored: exactly the six tokens of ADR-060 D12.
///
/// Defined here, not imported: ADR-060 D15 keeps this module off the label
/// types, and the first three share ADR-057's `race_censor_reason` strings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CensorReason {
    /// A trades `FeedGap` (D9).
    Gap,
    /// A rejected trades-stream event (D9, ADR-055 D6's min rule).
    Rejected,
    /// The end of the data (D9).
    EndOfData,
    /// A chain restart (D9, ADR-052 D4).
    ChainRestart,
    /// A funding gap, or a rejected funding-stream event, that meets the
    /// booking span under `funding.recorded@1` (D7, D9).
    FundingGap,
    /// A value left the `i64` unit range (D10).
    Overflow,
}

impl CensorReason {
    /// The `censor=` token of the trade line (ADR-060 D12).
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gap => "gap",
            Self::Rejected => "rejected",
            Self::EndOfData => "end_of_data",
            Self::ChainRestart => "chain_restart",
            Self::FundingGap => "funding_gap",
            Self::Overflow => "overflow",
        }
    }
}

/// How a trade ended (ADR-060 D6, D9, D12). The censor reason is part of
/// the censored case, so `censor` is `-` exactly when the exit is not
/// censored.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Exit {
    /// A trade touched the target first.
    Target,
    /// A trade touched the stop first.
    Stop,
    /// No touch before the deadline.
    Time,
    /// No entry fill before the deadline.
    Unfilled,
    /// The outcome is unknown from the censor time on (D9).
    Censored(CensorReason),
}

impl Exit {
    /// The `exit=` value of the trade line (ADR-060 D12). A censored exit
    /// carries its reason on the `censor=` field.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Target => "target",
            Self::Stop => "stop",
            Self::Time => "time",
            Self::Unfilled => "unfilled",
            Self::Censored(_) => "censored",
        }
    }
}

/// A fill **trade**: the time and price of the recorded trade a leg fills
/// at, not the slipped fill (ADR-060 D12).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fill {
    /// The trade time.
    pub at: EventTime,
    /// The trade price (e or x).
    pub price: Price,
}

/// One taken decision: every field of ADR-060 D12's trade line. `None` is
/// the line's `-`; R values are 8-place [`Rate`]s (D10).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TradeRecord {
    /// The decision event's canonical key.
    pub decision: CanonicalKey,
    /// The side.
    pub side: PositionSide,
    /// The regime labels at the decision (D12).
    pub regime: Vec<Option<RegimeLabel>>,
    /// The reference price.
    pub reference: Price,
    /// The stop level.
    pub stop: Price,
    /// The target level.
    pub target: Price,
    /// R in price units.
    pub risk: Price,
    /// The entry fill trade; `None` without one, or on a censored line
    /// whose entry fill is not strictly before the censor time.
    pub entry: Option<Fill>,
    /// How the trade ended.
    pub exit: Exit,
    /// The touch, the deadline or the censor time.
    pub exit_at: EventTime,
    /// The exit fill trade; `None` on unfilled and censored lines.
    pub fill: Option<Fill>,
    /// Funding per unit of base, positive meaning paid (D7); 0 under
    /// `funding.none@1`, `None` on unfilled and censored lines.
    pub funding: Option<Price>,
    /// σ(x − e) / R.
    pub gross: Option<Rate>,
    /// (σ(X′ − E′) − fees − funding) / R.
    pub net: Option<Rate>,
    /// σ(e − reference) / R; positive means adverse.
    pub displacement: Option<Rate>,
    /// Maximum favourable excursion in R, ≥ 0; a lower bound on a censored
    /// line (trades strictly before the censor time only).
    pub mfe: Option<Rate>,
    /// Maximum adverse excursion in R, ≥ 0; a lower bound on a censored
    /// line.
    pub mae: Option<Rate>,
}

/// The decisions that passed the filters but could not trade, by the
/// first D4 check that removed them (ADR-060 D4, D12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SkipCounts {
    /// Check 6: the position slot was busy (D8).
    pub overlap: u64,
    /// Check 3: no trade before the decision, or a trades gap since the
    /// last one (D4).
    pub stale_reference: u64,
    /// Check 2: no side (D3).
    pub no_side: u64,
    /// Check 4: no valid risk (D5).
    pub no_risk: u64,
    /// Check 5: the stop is not beyond the reference (D5).
    pub stop_side: u64,
}

impl SkipCounts {
    /// Σ`skipped` of D4's conservation rule.
    pub const fn total(&self) -> u64 {
        self.overlap + self.stale_reference + self.no_side + self.no_risk + self.stop_side
    }
}

/// The counts of a run (ADR-060 D4, D9, D12). Conservation (D4):
/// `triggers` = `filtered` + Σ`skipped` + the number of records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SimulationStats {
    /// Decisions handed to [`Simulator::decide`].
    pub triggers: u64,
    /// Decisions removed by a filter (check 1).
    pub filtered: u64,
    /// Decisions inside the hypothesis that could not trade.
    pub skipped: SkipCounts,
    /// Trades-stream causes whose censor time reaches back into a final
    /// trade, which stays as it was (D9).
    pub reach_backs: u64,
}
