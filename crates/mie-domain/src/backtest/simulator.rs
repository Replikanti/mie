//! The event-driven fill simulator and trade lifecycle (ADR-060 D4–D10),
//! with the book-priced fills and the maker target of ADR-061 D4 and D6.

use super::{
    Admission, CensorReason, Costs, Exit, Fill, FundingRule, Levels, PositionSide, SimulationStats,
    SlippageRule, SlippedFills, TargetFill, TradeIntent, TradeRecord,
};
use crate::book::OrderBook;
use crate::event::{FundingSettlement, MarketEvent, Stream};
use crate::feature::FeatureValue;
use crate::num::{Price, Rate, SCALE, div_half_even};
use crate::order::CanonicalKey;
use crate::regime::RegimeLabel;
use crate::slippage::{BookModel, FillSide, price_fill};
use crate::time::EventTime;

#[cfg(test)]
pub(super) mod tests;

/// Simulates one spec's positions over the event stream (ADR-060 D4–D10).
///
/// Drive it in canonical order (ADR-028):
///
/// 1. [`observe`](Self::observe) every **accepted** event, from the
///    engine's start, the prefix included (ADR-052 D10), so that the
///    staleness of the reference follows the engine's last price, with the
///    engine's `book.l2@1` after the event (ADR-061 D1);
/// 2. after each event, [`decide`](Self::decide) each emission of the
///    spec's trigger id at that event, in emission order (D4, D8);
/// 3. [`reject`](Self::reject) every rejected event instead of observing
///    it;
/// 4. [`restart`](Self::restart) at a chain restart, before the restart
///    event is observed or rejected (ADR-052 D4);
/// 5. at the sample end, observe the trailing trades gaps and, under
///    `funding.recorded@1`, the trailing funding gaps, in canonical order
///    (D9), then [`finish`](Self::finish).
///
/// One position at a time (D8). A closed trade stays censorable until it is
/// final (D9), and by funding gaps until `finish`, so each trade keeps the
/// breakpoints of its running excursions, never its price path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Simulator {
    /// The cost and timing assumptions.
    costs: Costs,
    /// One position per taken decision, in decision order.
    positions: Vec<Position>,
    /// The position holding the slot (D8).
    current: Option<usize>,
    /// The positions that are not final yet (D9), in decision order.
    live: Vec<usize>,
    /// No trade observed since the start or the last restart, or a trades
    /// gap observed after the last trade (D4's stale condition).
    stale: bool,
    /// The latest time of an observed trades-stream event (a trade, or a
    /// trades gap at its end). Rejected events never raise it.
    trades_until: Option<EventTime>,
    /// The latest exclusive bound of the final trades: a cause at an
    /// earlier time reaches back into one of them (D9).
    final_until: Option<EventTime>,
    /// The funding interval `[start, end]` with the latest end seen so far
    /// under `funding.recorded@1`, with the earliest start among those
    /// ending there. A funding gap is ordered at its end, before the trades
    /// of that millisecond (ADR-028), so it can precede an entry fill
    /// trade whose span it meets (D9).
    funding_seen: Option<(EventTime, EventTime)>,
    /// Counts.
    stats: SimulationStats,
}

/// A taken decision and its lifecycle.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Position {
    /// The decision event.
    decision: CanonicalKey,
    /// The regime labels at the decision.
    regime: Vec<Option<RegimeLabel>>,
    /// The levels, fixed at the decision (D5).
    levels: Levels,
    /// The earliest entry time: decision time + latency (D6).
    entry_after: EventTime,
    /// Decision time + `max_hold_ms` (D6).
    deadline: EventTime,
    /// Where the position is.
    phase: Phase,
    /// The entry fill and its values, once filled without overflow.
    entry: Option<Entry>,
    /// The excursions over the path, from the entry fill.
    excursion: Excursion,
    /// The last trade price on the path: the funding price p (D7).
    last_price: Price,
    /// Σ of the booked funding terms, in price units (D7).
    funding: i128,
    /// The start of a funding interval seen before the entry fill trade
    /// that starts at or after the entry fill time: it hits unless the
    /// exit fill trade is at or before that start (D9), which only the
    /// exit fill can tell.
    funding_pending: Option<EventTime>,
}

/// The lifecycle of a position (ADR-060 D6–D9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Waiting for the entry fill trade.
    Pending,
    /// Filled; the trades before the deadline are monitored.
    Monitoring,
    /// A trade touched a level at `at`; the next trade fills the exit.
    Touched { exit: Exit, at: EventTime },
    /// The deadline passed without a touch; the first trade at or after it
    /// fills the exit.
    Expired,
    /// A target, stop or time exit filled.
    Closed(Closed),
    /// No entry fill before the deadline.
    Unfilled,
    /// Censored at `at`.
    Censored { at: EventTime, reason: CensorReason },
}

/// A target, stop or time exit with its values (D10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Closed {
    exit: Exit,
    /// The touch, the deadline, or a maker target's fill trade.
    exit_at: EventTime,
    fill: Fill,
    /// X′.
    slipped: Price,
    funding: Price,
    gross: Rate,
    net: Rate,
}

/// The entry fill and the values it fixes (D6, D10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    fill: Fill,
    /// E′.
    slipped: Price,
    /// The entry leg's fee in price units.
    fee: i128,
    displacement: Rate,
}

/// How an exit leg fills (ADR-060 D6, ADR-061 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Leg {
    /// A taker order at the fill trade: X′ by the slippage rule, the taker
    /// fee, gross R on the trade price.
    Taker,
    /// The resting maker target: X′ = the target, the maker fee, gross R
    /// on the target.
    Maker,
}

/// The running σ-max and σ-min of the path against the entry fill price,
/// as breakpoints: `(time, value in R)` each time the excursion grows. The
/// excursion over the trades strictly before c is the last breakpoint
/// before c (D9, D12).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Excursion {
    /// The largest σ(p − e) so far, in price units.
    best: i128,
    /// The largest σ(e − p) so far, in price units.
    worst: i128,
    /// MFE breakpoints.
    favorable: Vec<(EventTime, Rate)>,
    /// MAE breakpoints.
    adverse: Vec<(EventTime, Rate)>,
}

impl Excursion {
    /// The excursion at the entry fill: 0 both ways.
    fn at_entry(at: EventTime) -> Self {
        Self {
            best: 0,
            worst: 0,
            favorable: vec![(at, Rate::from_units(0))],
            adverse: vec![(at, Rate::from_units(0))],
        }
    }

    /// The last breakpoint strictly before `before`, or the last of all.
    fn read(points: &[(EventTime, Rate)], before: Option<EventTime>) -> Option<Rate> {
        let count = match before {
            Some(at) => points.partition_point(|&(time, _)| time < at),
            None => points.len(),
        };
        count.checked_sub(1).map(|last| points[last].1)
    }
}

/// `value` as `i64` units, or `None` outside the range of `Price` and
/// `Rate` (ADR-060 D10).
fn units(value: i128) -> Option<i64> {
    i64::try_from(value).ok()
}

/// The order side of a leg: a long buys to enter and sells to exit, a short
/// the reverse (ADR-061 D2).
const fn fill_side(side: PositionSide, entering: bool) -> FillSide {
    match (side, entering) {
        (PositionSide::Long, true) | (PositionSide::Short, false) => FillSide::Buy,
        (PositionSide::Long, false) | (PositionSide::Short, true) => FillSide::Sell,
    }
}

/// The slipped fill of a taker leg at the trade price `price` (ADR-060 D6,
/// ADR-061 D2, D4): ⟨price × (1 ± σs)⟩ under `slippage.fixed@1`, with +
/// entering and − exiting, else the book's price at the fill trade.
///
/// # Errors
///
/// [`CensorReason::Overflow`] when the fixed fill leaves the `i64` unit
/// range (D10), [`CensorReason::NoBook`] when the book rule cannot price
/// the fill (ADR-061 D4).
fn slip(
    costs: &Costs,
    side: PositionSide,
    entering: bool,
    price: Price,
    book: &FeatureValue<OrderBook>,
) -> Result<Price, CensorReason> {
    match costs.slippage() {
        SlippageRule::Fixed(rate) => {
            let sigma = i128::from(side.sign()) * if entering { 1 } else { -1 };
            let slip = i128::from(SCALE) + sigma * i128::from(rate.units());
            times_rate(i128::from(price.units()), slip)
                .and_then(units)
                .map(Price::from_units)
                .ok_or(CensorReason::Overflow)
        }
        SlippageRule::Spread => {
            price_fill(book, fill_side(side, entering), price, BookModel::Spread)
                .map_err(|_| CensorReason::NoBook)
        }
        SlippageRule::DepthWalk(qty) => price_fill(
            book,
            fill_side(side, entering),
            price,
            BookModel::DepthWalk(qty),
        )
        .map_err(|_| CensorReason::NoBook),
    }
}

/// ⟨value × rate⟩ in price units: one half-to-even rounding (ADR-060 D6,
/// D7).
fn times_rate(value: i128, rate: i128) -> Option<i128> {
    Some(div_half_even(value.checked_mul(rate)?, i128::from(SCALE)))
}

/// `numerator / risk` in R, to 8 places with one half-to-even rounding,
/// or `None` outside the `i64` unit range (ADR-060 D10).
fn r_value(numerator: i128, risk: Price) -> Option<Rate> {
    let scaled = numerator.checked_mul(i128::from(SCALE))?;
    units(div_half_even(scaled, i128::from(risk.units()))).map(Rate::from_units)
}

impl Position {
    fn sigma(&self) -> i128 {
        i128::from(self.levels.side().sign())
    }

    /// The entry at a fill trade with its slipped fill E′ ([`slip`]; D6,
    /// D10): the fee ⟨taker · E′⟩ and the displacement σ(e − reference) /
    /// R. `None` on overflow.
    fn entry(&self, costs: &Costs, fill: Fill, slipped: Price) -> Option<Entry> {
        let sigma = self.sigma();
        let e = i128::from(fill.price.units());
        let fee = times_rate(
            i128::from(slipped.units()),
            i128::from(costs.taker().units()),
        )?;
        units(fee)?;
        let moved = sigma * (e - i128::from(self.levels.reference().units()));
        let displacement = r_value(moved, self.levels.risk())?;
        Some(Entry {
            fill,
            slipped,
            fee,
            displacement,
        })
    }

    /// Adds a trade of the path to the excursions (D10). `false` on
    /// overflow.
    fn extend(&mut self, at: EventTime, price: Price) -> bool {
        self.last_price = price;
        let Some(entry) = self.entry else {
            return true;
        };
        let moved =
            self.sigma() * (i128::from(price.units()) - i128::from(entry.fill.price.units()));
        let risk = self.levels.risk();
        let excursion = &mut self.excursion;
        if moved > excursion.best {
            let Some(value) = r_value(moved, risk) else {
                return false;
            };
            excursion.best = moved;
            excursion.favorable.push((at, value));
        }
        if -moved > excursion.worst {
            let Some(value) = r_value(-moved, risk) else {
                return false;
            };
            excursion.worst = -moved;
            excursion.adverse.push((at, value));
        }
        true
    }

    /// Whether a maker target fills at a trade at `price` (ADR-061 D6):
    /// σ-strictly beyond the target under `through`, at or beyond it under
    /// `touch`.
    fn maker_fills(&self, costs: &Costs, price: Price) -> bool {
        let to =
            self.sigma() * (i128::from(price.units()) - i128::from(self.levels.target().units()));
        match costs.target_fill() {
            TargetFill::MakerThrough => to > 0,
            TargetFill::MakerTouch => to >= 0,
            TargetFill::Taker => false,
        }
    }

    /// Whether a trade at `price` touches a level, and which (ADR-055 D5,
    /// ADR-060 D6). One trade cannot touch both.
    fn touch(&self, price: Price) -> Option<Exit> {
        let sigma = self.sigma();
        let price = i128::from(price.units());
        let to = |level: Price| sigma * (price - i128::from(level.units()));
        if to(self.levels.stop()) <= 0 {
            Some(Exit::Stop)
        } else if to(self.levels.target()) >= 0 {
            Some(Exit::Target)
        } else {
            None
        }
    }

    /// The exit at a fill trade (D6, D10) with its slipped fill X′: its
    /// fee, gross and net R. A taker leg pays ⟨taker · X′⟩ and its gross R
    /// is σ(x − e) / R; the maker target fills at X′ = the target with
    /// ⟨maker · X′⟩ and gross R σ(target − e) / R (ADR-061 D6). `None` on
    /// overflow.
    fn close(
        &self,
        costs: &Costs,
        exit: Exit,
        exit_at: EventTime,
        fill: Fill,
        (leg, slipped): (Leg, Price),
    ) -> Option<Closed> {
        let entry = self.entry?;
        let sigma = self.sigma();
        let risk = self.levels.risk();
        let (fee_rate, gross_at) = match leg {
            Leg::Taker => (costs.taker(), fill.price),
            Leg::Maker => (costs.maker(), slipped),
        };
        let exit_units = i128::from(slipped.units());
        let fee = times_rate(exit_units, i128::from(fee_rate.units()))?;
        units(fee)?;
        let funding = Price::from_units(units(self.funding)?);
        let e = i128::from(entry.fill.price.units());
        let gross = r_value(sigma * (i128::from(gross_at.units()) - e), risk)?;
        let net = (sigma * (exit_units - i128::from(entry.slipped.units())))
            .checked_sub(entry.fee + fee)?
            .checked_sub(self.funding)?;
        let net = r_value(net, risk)?;
        Some(Closed {
            exit,
            exit_at,
            fill,
            slipped,
            funding,
            gross,
            net,
        })
    }

    /// The booking span holds a funding interval `[start, end]` (D9): it
    /// starts before the exit fill trade, or the position has none, and
    /// ends at or after the entry fill trade. `None` without an entry; the
    /// censor time max(start, entry fill time) otherwise.
    fn funding_hit(&self, start: EventTime, end: EventTime) -> Option<EventTime> {
        let entry = self.entry?.fill.at;
        let before_exit = match self.phase {
            Phase::Closed(closed) => start < closed.fill.at,
            _ => true,
        };
        (before_exit && end >= entry).then_some(start.max(entry))
    }

    /// The record (D12), with the slipped fills when `book` (a book
    /// slippage rule, ADR-061 D5). Every position is closed, unfilled or
    /// censored after [`Simulator::finish`].
    fn record(&self, book: bool) -> TradeRecord {
        let (exit, exit_at, closed, cut) = match self.phase {
            Phase::Closed(closed) => (closed.exit, closed.exit_at, Some(closed), None),
            Phase::Unfilled => (Exit::Unfilled, self.deadline, None, None),
            Phase::Censored { at, reason } => (Exit::Censored(reason), at, None, Some(at)),
            Phase::Pending | Phase::Monitoring | Phase::Touched { .. } | Phase::Expired => {
                unreachable!("finish closes or censors every position")
            }
        };
        let entry = self
            .entry
            .filter(|entry| cut.is_none_or(|at| entry.fill.at < at));
        let excursion =
            |points: &[(EventTime, Rate)]| entry.and_then(|_| Excursion::read(points, cut));
        TradeRecord {
            decision: self.decision,
            side: self.levels.side(),
            regime: self.regime.clone(),
            reference: self.levels.reference(),
            stop: self.levels.stop(),
            target: self.levels.target(),
            risk: self.levels.risk(),
            entry: entry.map(|entry| entry.fill),
            exit,
            exit_at,
            fill: closed.map(|closed| closed.fill),
            funding: closed.map(|closed| closed.funding),
            gross: closed.map(|closed| closed.gross),
            net: closed.map(|closed| closed.net),
            displacement: entry.map(|entry| entry.displacement),
            mfe: excursion(&self.excursion.favorable),
            mae: excursion(&self.excursion.adverse),
            slipped: book.then(|| SlippedFills {
                entry: entry.map(|entry| entry.slipped),
                exit: closed.map(|closed| closed.slipped),
            }),
        }
    }
}

impl Simulator {
    /// A simulator with nothing observed, under `costs`.
    pub fn new(costs: Costs) -> Self {
        Self {
            costs,
            positions: Vec::new(),
            current: None,
            live: Vec::new(),
            stale: true,
            trades_until: None,
            final_until: None,
            funding_seen: None,
            stats: SimulationStats::default(),
        }
    }

    /// The cost and timing assumptions.
    pub fn costs(&self) -> &Costs {
        &self.costs
    }

    /// Counts so far.
    pub fn stats(&self) -> SimulationStats {
        self.stats
    }

    /// Applies an accepted event to the open position and to the trades
    /// coverage (ADR-060 D4, D6, D7, D9), before the event's decisions.
    ///
    /// A trades gap censors at its start and makes the reference stale. A
    /// `Stream::Funding` gap censors under `funding.recorded@1` only, and
    /// moves no trades-stream time and closes nothing, so a trailing one
    /// can be passed before [`finish`](Self::finish). Any other event at or
    /// after a deadline ends a pending entry as `unfilled` or starts the
    /// time exit; then a trade fills, is monitored or fills the exit, and a
    /// settlement is booked.
    ///
    /// `book` is the engine's `book.l2@1` after `event`: a trade never
    /// changes the book, so for a trade it is the book the print met
    /// (ADR-061 D1). Only a book slippage rule reads it, and only at a fill
    /// trade, so any value will do for an event that is no trade.
    pub fn observe(&mut self, event: &MarketEvent, book: &FeatureValue<OrderBook>) {
        let key = event.canonical_key();
        match event {
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Funding && gap.start <= gap.end => {
                self.funding_gap(gap.start, gap.end);
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                // Censor before the deadlines: a gap ordered at or after a
                // deadline may still have started before it.
                self.reach_back(gap.start);
                self.censor_live(Some(gap.start), CensorReason::Gap);
                self.stale = true;
                self.expire(key.time);
            }
            MarketEvent::Trade(trade) => {
                self.expire(key.time);
                self.trade(key, trade.price, book);
                self.stale = false;
            }
            MarketEvent::FundingSettlement(settlement) => {
                self.expire(key.time);
                self.settle(*settlement);
            }
            _ => self.expire(key.time),
        }
        if event.stream() == Stream::Trades {
            self.trades_until = self.trades_until.max(Some(key.time));
        }
        self.finalize();
    }

    /// Takes one decision through ADR-060 D4's checks in order and opens
    /// the position when it passes all six. Call it after
    /// [`observe`](Self::observe) of the decision event.
    pub fn decide(&mut self, intent: TradeIntent) {
        self.stats.triggers += 1;
        let skipped = &mut self.stats.skipped;
        let levels = match intent.admission {
            Admission::Filtered => {
                self.stats.filtered += 1;
                return;
            }
            Admission::NoSide => {
                skipped.no_side += 1;
                return;
            }
            _ if intent.reference.is_none() || self.stale => {
                skipped.stale_reference += 1;
                return;
            }
            Admission::NoRisk => {
                skipped.no_risk += 1;
                return;
            }
            Admission::StopSide => {
                skipped.stop_side += 1;
                return;
            }
            Admission::Levels(levels) => levels,
        };
        if self.current.is_some() {
            skipped.overlap += 1;
            return;
        }
        let time = intent.decision.time.as_millis();
        let index = self.positions.len();
        self.positions.push(Position {
            decision: intent.decision,
            regime: intent.regime,
            levels,
            entry_after: EventTime::from_millis(
                time.saturating_add(i64::from(self.costs.latency_ms())),
            ),
            deadline: EventTime::from_millis(time.saturating_add(self.costs.max_hold_ms())),
            phase: Phase::Pending,
            entry: None,
            excursion: Excursion::default(),
            last_price: levels.reference(),
            funding: 0,
            funding_pending: None,
        });
        self.current = Some(index);
        self.live.push(index);
    }

    /// Reports an event the engine rejected (ADR-060 D9).
    ///
    /// A trades-stream event censors every trade that is not final at
    /// c = max(decision time, min(its time, the last accepted trades-stream
    /// event)), ADR-055 D6's min rule, and closes nothing. Under
    /// `funding.recorded@1`, a funding-stream event censors as
    /// `funding_gap`: a refused settlement every trade whose booking span
    /// holds its time, at max(its time, the entry fill time), and a refused
    /// funding gap by its declared interval, like an accepted one. A gap
    /// with `start > end` (`StateError::InvalidGap`) declares no interval,
    /// so it censors as the point at its time, like a settlement. Either
    /// would otherwise book zero funding by omission (D7). Events of other
    /// streams are ignored.
    pub fn reject(&mut self, event: &MarketEvent) {
        match event {
            _ if event.stream() == Stream::Trades => {
                let start = self.trades_until.map(|until| until.min(event.time()));
                if let Some(start) = start {
                    self.reach_back(start);
                }
                self.censor_live(start, CensorReason::Rejected);
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Funding && gap.start <= gap.end => {
                self.funding_gap(gap.start, gap.end);
            }
            _ if event.stream() == Stream::Funding => {
                self.funding_gap(event.time(), event.time());
            }
            _ => {}
        }
    }

    /// A chain restart (ADR-052 D4, ADR-060 D9): censors every trade that
    /// is not final at max(decision time, the last trades-stream event
    /// accepted before the restart), and makes the reference stale, since
    /// the fresh engine has no last price.
    pub fn restart(&mut self) {
        self.censor_live(self.trades_until, CensorReason::ChainRestart);
        self.stale = true;
    }

    /// Ends the data (ADR-060 D9): censors every trade that is not final
    /// at max(decision time, the last accepted trades-stream event), and
    /// returns the records in decision order with the counts.
    pub fn finish(mut self) -> (Vec<TradeRecord>, SimulationStats) {
        self.censor_live(self.trades_until, CensorReason::EndOfData);
        let book = self.costs.slippage().is_book();
        let records = self
            .positions
            .iter()
            .map(|position| position.record(book))
            .collect();
        (records, self.stats)
    }

    /// Ends a pending entry as `unfilled`, or starts the time exit, at an
    /// event at or after the deadline: it follows every trade before the
    /// deadline in canonical order (D6).
    fn expire(&mut self, now: EventTime) {
        let Some(index) = self.current else {
            return;
        };
        let position = &mut self.positions[index];
        if position.deadline > now {
            return;
        }
        match position.phase {
            Phase::Pending => {
                position.phase = Phase::Unfilled;
                self.current = None;
            }
            Phase::Monitoring => position.phase = Phase::Expired,
            _ => {}
        }
    }

    /// A trade, after [`expire`](Self::expire): the entry fill, a monitored
    /// trade or the exit fill of the open position (D6, D10), priced on
    /// `book` under a book slippage rule (ADR-061 D4). Under a maker target,
    /// a monitored trade after the entry fill trade may fill the resting
    /// target itself (ADR-061 D6).
    fn trade(&mut self, key: CanonicalKey, price: Price, book: &FeatureValue<OrderBook>) {
        let Some(index) = self.current else {
            return;
        };
        let costs = self.costs;
        let seen = self.funding_seen;
        let position = &mut self.positions[index];
        let side = position.levels.side();
        let fill = Fill {
            at: key.time,
            price,
        };
        let entering = position.phase == Phase::Pending;
        if entering {
            if key <= position.decision || key.time < position.entry_after {
                return;
            }
            let entry = slip(&costs, side, true, price, book).and_then(|slipped| {
                position
                    .entry(&costs, fill, slipped)
                    .ok_or(CensorReason::Overflow)
            });
            let entry = match entry {
                Ok(entry) => entry,
                Err(reason) => {
                    self.censor(index, key.time, reason);
                    return;
                }
            };
            position.entry = Some(entry);
            position.excursion = Excursion::at_entry(key.time);
            position.phase = Phase::Monitoring;
            // A funding interval seen before this trade that ends at or
            // after it meets the span (D9). One starting before the fill
            // starts before any exit fill, so it hits now, at the fill.
            if let Some((start, _)) = seen.filter(|&(_, end)| end >= key.time) {
                if start < key.time {
                    self.censor(index, key.time, CensorReason::FundingGap);
                    return;
                }
                position.funding_pending = Some(start);
            }
        }
        // The entry fill, a monitored trade and the exit fill are all on
        // the path (D10).
        if !position.extend(key.time, price) {
            self.censor(index, key.time, CensorReason::Overflow);
            return;
        }
        let (exit, exit_at, leg) = match position.phase {
            // A maker target rests from the entry; the entry fill trade
            // itself only finds it marketable, a taker touch (ADR-061 D6).
            Phase::Monitoring if costs.target_fill().is_maker() && !entering => {
                match position.touch(price) {
                    Some(Exit::Stop) => {
                        position.phase = Phase::Touched {
                            exit: Exit::Stop,
                            at: key.time,
                        };
                        return;
                    }
                    _ if position.maker_fills(&costs, price) => {
                        (Exit::Target, key.time, Leg::Maker)
                    }
                    _ => return,
                }
            }
            Phase::Monitoring => {
                if let Some(exit) = position.touch(price) {
                    position.phase = Phase::Touched { exit, at: key.time };
                }
                return;
            }
            Phase::Touched { exit, at } => (exit, at, Leg::Taker),
            Phase::Expired => (Exit::Time, position.deadline, Leg::Taker),
            _ => return,
        };
        if let Some(start) = position.funding_pending.take()
            && start < key.time
        {
            self.censor(index, start, CensorReason::FundingGap);
            return;
        }
        let slipped = match leg {
            Leg::Maker => Ok(position.levels.target()),
            Leg::Taker => slip(&costs, side, false, price, book),
        };
        let slipped = match slipped {
            Ok(slipped) => slipped,
            Err(reason) => {
                self.censor(index, key.time, reason);
                return;
            }
        };
        match position.close(&costs, exit, exit_at, fill, (leg, slipped)) {
            Some(closed) => {
                position.phase = Phase::Closed(closed);
                self.current = None;
            }
            None => self.censor(index, key.time, CensorReason::Overflow),
        }
    }

    /// Books a settlement on the open position under
    /// `funding.recorded@1`: after the entry fill trade, before the exit
    /// fill trade, at ⟨σ · rate · p⟩ with p the last trade price before it
    /// (D7).
    fn settle(&mut self, settlement: FundingSettlement) {
        if self.costs.funding() != FundingRule::Recorded {
            return;
        }
        let Some(index) = self.current else {
            return;
        };
        let position = &mut self.positions[index];
        if position.entry.is_none() {
            return;
        }
        let sigma = position.sigma();
        let term = times_rate(
            sigma * i128::from(settlement.rate.units()),
            i128::from(position.last_price.units()),
        );
        match term.and_then(|term| position.funding.checked_add(term)) {
            Some(funding) if units(funding).is_some() => position.funding = funding,
            _ => self.censor(index, settlement.time, CensorReason::Overflow),
        }
    }

    /// A funding interval `[start, end]` under `funding.recorded@1`: a
    /// funding gap, accepted or rejected, or the time of a rejected
    /// settlement. It censors every trade whose booking span it meets,
    /// final or not, and is never a reach-back (D9). An entry fill trade
    /// later in the interval's end millisecond checks it at the fill.
    fn funding_gap(&mut self, start: EventTime, end: EventTime) {
        if self.costs.funding() != FundingRule::Recorded {
            return;
        }
        self.funding_seen = match self.funding_seen {
            Some((earlier, until)) if until == end => Some((earlier.min(start), end)),
            Some((_, until)) if until > end => self.funding_seen,
            _ => Some((start, end)),
        };
        for index in 0..self.positions.len() {
            if let Some(at) = self.positions[index].funding_hit(start, end) {
                self.censor(index, at, CensorReason::FundingGap);
            }
        }
    }

    /// Censors every trade that is not final at max(`start`, decision
    /// time), or at the decision time without a `start` (D9).
    fn censor_live(&mut self, start: Option<EventTime>, reason: CensorReason) {
        for slot in 0..self.live.len() {
            let index = self.live[slot];
            let decision = self.positions[index].decision.time;
            let at = start.map_or(decision, |start| start.max(decision));
            self.censor(index, at, reason);
        }
    }

    /// Applies a censor at `at` to one position (D9). The earliest censor
    /// wins, and at an equal time the first one stays. A trades-stream
    /// cause leaves an outcome that stands: a touch (or a maker target's
    /// fill trade) before `at`, or a deadline at or before it for a time
    /// exit or an unfilled entry. A funding gap, an overflow or an unpriced
    /// book fill (ADR-061 D4) censors outright. Anything else is
    /// censored, including an exit that would stand without an observed
    /// fill, which the provider contract rules out.
    fn censor(&mut self, index: usize, at: EventTime, reason: CensorReason) {
        // A pending funding interval hits once the trade cannot reach an
        // exit fill, and as the earlier cause it is applied first.
        if let Some(start) = self.positions[index].funding_pending.take() {
            self.censor(index, start, CensorReason::FundingGap);
        }
        let outright = matches!(
            reason,
            CensorReason::FundingGap | CensorReason::Overflow | CensorReason::NoBook
        );
        let position = &mut self.positions[index];
        match position.phase {
            Phase::Censored { at: earlier, .. } if earlier <= at => return,
            Phase::Censored { .. } => {}
            _ if outright => {}
            Phase::Closed(Closed {
                exit: Exit::Target | Exit::Stop,
                exit_at,
                ..
            }) if exit_at < at => return,
            Phase::Closed(Closed {
                exit: Exit::Time, ..
            })
            | Phase::Unfilled
                if position.deadline <= at =>
            {
                return;
            }
            Phase::Pending if position.deadline <= at => {
                position.phase = Phase::Unfilled;
                self.release(index);
                return;
            }
            _ => {}
        }
        position.phase = Phase::Censored { at, reason };
        self.release(index);
    }

    /// Frees the slot if `index` holds it.
    fn release(&mut self, index: usize) {
        if self.current == Some(index) {
            self.current = None;
        }
    }

    /// Counts a trades-stream cause at `at` that reaches into a final
    /// trade (D9).
    fn reach_back(&mut self, at: EventTime) {
        if self.final_until.is_some_and(|until| at < until) {
            self.stats.reach_backs += 1;
        }
    }

    /// Makes final every trade that a trades-stream event has passed (D9,
    /// ADR-055 D6's emission rule): a target or stop exit once an event in
    /// a later millisecond than the touch, a time exit or an unfilled entry
    /// once an event at or after the deadline, a censored trade once an
    /// event at or after its censor time.
    fn finalize(&mut self) {
        let Some(until) = self.trades_until else {
            return;
        };
        let positions = &self.positions;
        let final_until = &mut self.final_until;
        self.live.retain(|&index| {
            let position = &positions[index];
            let bound = match position.phase {
                Phase::Closed(Closed {
                    exit: Exit::Target | Exit::Stop,
                    exit_at,
                    ..
                }) => (until > exit_at)
                    .then(|| EventTime::from_millis(exit_at.as_millis().saturating_add(1))),
                Phase::Closed(_) | Phase::Unfilled => {
                    (until >= position.deadline).then_some(position.deadline)
                }
                Phase::Censored { at, .. } => (until >= at).then_some(at),
                _ => None,
            };
            match bound {
                Some(bound) => {
                    *final_until = Some(final_until.map_or(bound, |at| at.max(bound)));
                    false
                }
                None => true,
            }
        });
    }
}
