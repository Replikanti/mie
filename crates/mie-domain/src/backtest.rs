//! Backtest simulation on spec text v1 (ADR-060).
//!
//! The [`Simulator`] turns the decisions of one experiment spec into trade
//! records: entry at the first trade after the decision and its latency,
//! fixed slippage and the taker fee on both legs, recorded funding, stop,
//! target and time exits, and censoring by ADR-055 D6's rules applied to the
//! position (ADR-060 D4–D10). Under a book slippage rule its fills are
//! priced on `book.l2@1` instead (`crate::slippage`), and a maker target
//! fills as a resting limit order (ADR-061 D4, D6). [`metrics`] computes ADR-060 D11's metrics
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
pub use rules::{Plan, ResolutionError, is_book_slippage, resolve};
pub use simulator::Simulator;
pub use text::{event_kind_from_name, event_kind_name};

use crate::num::{Price, Qty, Rate, SCALE};
use crate::order::CanonicalKey;
use crate::regime::RegimeLabel;
use crate::research::FeeSchedule;
use crate::slippage::BookModel;
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

/// The slippage rule of a spec (ADR-060 D2, D6; ADR-061 D2–D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlippageRule {
    /// `slippage.fixed@1`: fill = ⟨trade price × (1 ± σs)⟩ (ADR-060 D6).
    Fixed(Rate),
    /// `slippage.spread@1`: the first reachable level of the book at the
    /// fill trade (ADR-061 D2).
    Spread,
    /// `slippage.depth_walk@1`: the volume-weighted price of walking the
    /// book at the fill trade for size q > 0 (ADR-061 D2).
    DepthWalk(Qty),
}

impl SlippageRule {
    /// The book model of a book rule; `None` for `slippage.fixed@1`.
    pub const fn book_model(self) -> Option<BookModel> {
        match self {
            Self::Fixed(_) => None,
            Self::Spread => Some(BookModel::Spread),
            Self::DepthWalk(qty) => Some(BookModel::DepthWalk(qty)),
        }
    }

    /// Whether fills are priced on the book (ADR-061 D4), so that every
    /// trade line carries the slipped fills (D5).
    pub const fn is_book(self) -> bool {
        self.book_model().is_some()
    }
}

/// How the target exit fills (ADR-060 D6, ADR-061 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetFill {
    /// `target.r_multiple@1`: a touch triggers a taker exit at the next
    /// trade (ADR-060 D6).
    Taker,
    /// `target.r_multiple_maker@1 fill=text:through`: a resting limit order
    /// filled by the first trade σ-strictly beyond the target (ADR-061 D6).
    MakerThrough,
    /// `target.r_multiple_maker@1 fill=text:touch`: filled by the first
    /// trade at or beyond the target, the optimistic bound (ADR-061 D6).
    MakerTouch,
}

impl TargetFill {
    /// The `fill=` text of `target.r_multiple_maker@1`; `None` for any
    /// other text.
    pub fn from_text(text: &str) -> Option<Self> {
        match text {
            "through" => Some(Self::MakerThrough),
            "touch" => Some(Self::MakerTouch),
            _ => None,
        }
    }

    /// Whether the target rests as a maker order (ADR-061 D6).
    pub const fn is_maker(self) -> bool {
        !matches!(self, Self::Taker)
    }
}

/// Why [`Costs::new`] refused its values: each lies outside ADR-060 D2's
/// or ADR-061 D3's domain of definition or ADR-040's fee range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CostsError {
    /// The fixed slippage rate is not in `[0, 1)`: at 1 or above a fill
    /// price is at or below 0 (D2).
    Slippage,
    /// The depth-walk size is not above 0: no walk (ADR-061 D3).
    DepthQty,
    /// The taker fee is not in `[0, 1)` (ADR-040 D1).
    Taker,
    /// The maker fee is not in `[0, 1)` (ADR-040 D1).
    Maker,
    /// `max_hold_ms` is below 1 ms (D2).
    MaxHold,
    /// The latency is not below `max_hold_ms`, so no entry can fill (D2,
    /// D6).
    Latency,
}

/// The resolved cost and timing assumptions of a spec (ADR-060 D2, D6, D7;
/// ADR-061 D3, D6).
///
/// Built only through [`new`](Self::new), which keeps the simulator's
/// arithmetic total: every value is inside its domain of definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Costs {
    latency_ms: u32,
    max_hold_ms: i64,
    slippage: SlippageRule,
    fees: FeeSchedule,
    target: TargetFill,
    funding: FundingRule,
}

impl Costs {
    /// The costs, or the first value outside its range in argument order.
    /// `fees.maker` is charged only on a maker target fill (ADR-061 D6),
    /// `fees.taker` on every other leg (ADR-060 D6). The ranges:
    /// a fixed slippage rate in `[0, 1)` or a depth-walk size above 0, the
    /// taker and maker fees in `[0, 1)`, `max_hold_ms` ≥ 1 and
    /// `latency_ms` < `max_hold_ms` (ADR-060 D2, ADR-061 D3).
    ///
    /// # Errors
    ///
    /// The [`CostsError`] of the first refused value.
    pub fn new(
        latency_ms: u32,
        max_hold_ms: i64,
        slippage: SlippageRule,
        fees: FeeSchedule,
        target: TargetFill,
        funding: FundingRule,
    ) -> Result<Self, CostsError> {
        let fraction = |rate: Rate| (0..SCALE).contains(&rate.units());
        match slippage {
            SlippageRule::Fixed(rate) if !fraction(rate) => return Err(CostsError::Slippage),
            SlippageRule::DepthWalk(qty) if qty.units() <= 0 => {
                return Err(CostsError::DepthQty);
            }
            _ => {}
        }
        if !fraction(fees.taker) {
            return Err(CostsError::Taker);
        }
        if !fraction(fees.maker) {
            return Err(CostsError::Maker);
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
            fees,
            target,
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

    /// The slippage rule (ADR-060 D6, ADR-061 D4).
    pub const fn slippage(&self) -> SlippageRule {
        self.slippage
    }

    /// The taker fee per leg (ADR-060 D6).
    pub const fn taker(&self) -> Rate {
        self.fees.taker
    }

    /// The maker fee of a maker target fill (ADR-061 D6).
    pub const fn maker(&self) -> Rate {
        self.fees.maker
    }

    /// How the target exit fills (ADR-060 D6, ADR-061 D6).
    pub const fn target_fill(&self) -> TargetFill {
        self.target
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

/// Why a trade is censored: exactly the six tokens of ADR-060 D12 and
/// ADR-061 D5's `no_book`.
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
    /// A fill a book slippage rule cannot price: the book is not ready,
    /// one-sided or crossed, or the trusted window holds no reachable level
    /// or less than the size (ADR-061 D2, D4).
    NoBook,
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
            Self::NoBook => "no_book",
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

/// The slipped fills E′ and X′ of a trade line under a book slippage rule
/// (ADR-061 D5): they depend on the book, so they cannot be re-derived
/// from e, x and the spec. `None` is the line's `-`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlippedFills {
    /// E′, `-` exactly when the line's `entry` is.
    pub entry: Option<Price>,
    /// X′, `-` exactly when the line's `fill` is.
    pub exit: Option<Price>,
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
    /// `entry_slipped=` and `exit_slipped=`, set exactly under a book
    /// slippage rule (ADR-061 D5); `None` under `slippage.fixed@1`, whose
    /// lines stay as ADR-060 D12 defines them.
    pub slipped: Option<SlippedFills>,
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
