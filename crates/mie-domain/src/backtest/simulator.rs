//! The event-driven fill simulator and trade lifecycle (ADR-060 D4–D10).

use super::{
    Admission, CensorReason, Costs, Exit, Fill, FundingRule, Levels, SimulationStats, TradeIntent,
    TradeRecord,
};
use crate::event::{FundingSettlement, MarketEvent, Stream};
use crate::num::{Price, Rate, SCALE, div_half_even};
use crate::order::CanonicalKey;
use crate::regime::RegimeLabel;
use crate::time::EventTime;

#[cfg(test)]
mod tests;

/// Simulates one spec's positions over the event stream (ADR-060 D4–D10).
///
/// Drive it in canonical order (ADR-028):
///
/// 1. [`observe`](Self::observe) every **accepted** event, from the
///    engine's start, the prefix included (ADR-052 D10), so that the
///    staleness of the reference follows the engine's last price;
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
    /// The touch or the deadline.
    exit_at: EventTime,
    fill: Fill,
    funding: Price,
    gross: Rate,
    net: Rate,
}

/// The entry fill and the values it fixes (D6, D10).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Entry {
    fill: Fill,
    /// E′ in price units.
    slipped: i128,
    /// The entry leg's fee in price units.
    fee: i128,
    displacement: Rate,
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

    /// The entry at a fill trade (D6, D10): E′ = ⟨e(1 + σs)⟩, the fee
    /// ⟨taker · E′⟩ and the displacement σ(e − reference) / R. `None` on
    /// overflow.
    fn entry(&self, costs: &Costs, fill: Fill) -> Option<Entry> {
        let sigma = self.sigma();
        let e = i128::from(fill.price.units());
        let slip = i128::from(SCALE) + sigma * i128::from(costs.slippage().units());
        let slipped = times_rate(e, slip)?;
        units(slipped)?;
        let fee = times_rate(slipped, i128::from(costs.taker().units()))?;
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

    /// The exit at a fill trade (D6, D10): X′ = ⟨x(1 − σs)⟩, its fee, gross
    /// and net R. `None` on overflow.
    fn close(&self, costs: &Costs, exit: Exit, exit_at: EventTime, fill: Fill) -> Option<Closed> {
        let entry = self.entry?;
        let sigma = self.sigma();
        let risk = self.levels.risk();
        let x = i128::from(fill.price.units());
        let slip = i128::from(SCALE) - sigma * i128::from(costs.slippage().units());
        let slipped = times_rate(x, slip)?;
        units(slipped)?;
        let fee = times_rate(slipped, i128::from(costs.taker().units()))?;
        units(fee)?;
        let funding = Price::from_units(units(self.funding)?);
        let gross = r_value(sigma * (x - i128::from(entry.fill.price.units())), risk)?;
        let net = (sigma * (slipped - entry.slipped))
            .checked_sub(entry.fee + fee)?
            .checked_sub(self.funding)?;
        let net = r_value(net, risk)?;
        Some(Closed {
            exit,
            exit_at,
            fill,
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

    /// The record (D12). Every position is closed, unfilled or censored
    /// after [`Simulator::finish`].
    fn record(&self) -> TradeRecord {
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
    pub fn observe(&mut self, event: &MarketEvent) {
        let key = event.canonical_key();
        match event {
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Funding => {
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
                self.trade(key, trade.price);
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
        });
        self.current = Some(index);
        self.live.push(index);
    }

    /// Reports an event the engine rejected (ADR-060 D9).
    ///
    /// A trades-stream event censors every trade that is not final at
    /// c = max(decision time, min(its time, the last accepted trades-stream
    /// event)), ADR-055 D6's min rule, and closes nothing. Under
    /// `funding.recorded@1`, a funding-stream event censors the trade whose
    /// booking span holds its time as `funding_gap`, at max(its time, the
    /// entry fill time): a refused settlement would otherwise book zero
    /// funding by omission (D7). Events of other streams are ignored.
    pub fn reject(&mut self, event: &MarketEvent) {
        match event.stream() {
            Stream::Trades => {
                let start = self.trades_until.map(|until| until.min(event.time()));
                if let Some(start) = start {
                    self.reach_back(start);
                }
                self.censor_live(start, CensorReason::Rejected);
            }
            Stream::Funding => self.funding_gap(event.time(), event.time()),
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
        let records = self.positions.iter().map(Position::record).collect();
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
    /// trade or the exit fill of the open position (D6, D10).
    fn trade(&mut self, key: CanonicalKey, price: Price) {
        let Some(index) = self.current else {
            return;
        };
        let costs = self.costs;
        let position = &mut self.positions[index];
        let fill = Fill {
            at: key.time,
            price,
        };
        if position.phase == Phase::Pending {
            if key <= position.decision || key.time < position.entry_after {
                return;
            }
            let Some(entry) = position.entry(&costs, fill) else {
                self.censor(index, key.time, CensorReason::Overflow);
                return;
            };
            position.entry = Some(entry);
            position.excursion = Excursion::at_entry(key.time);
            position.phase = Phase::Monitoring;
        }
        // The entry fill, a monitored trade and the exit fill are all on
        // the path (D10).
        if !position.extend(key.time, price) {
            self.censor(index, key.time, CensorReason::Overflow);
            return;
        }
        let (exit, exit_at) = match position.phase {
            Phase::Monitoring => {
                if let Some(exit) = position.touch(price) {
                    position.phase = Phase::Touched { exit, at: key.time };
                }
                return;
            }
            Phase::Touched { exit, at } => (exit, at),
            Phase::Expired => (Exit::Time, position.deadline),
            _ => return,
        };
        match position.close(&costs, exit, exit_at, fill) {
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
    /// funding gap, or the time of a rejected funding-stream event. It
    /// censors every trade whose booking span it meets, final or not, and
    /// is never a reach-back (D9).
    fn funding_gap(&mut self, start: EventTime, end: EventTime) {
        if self.costs.funding() != FundingRule::Recorded {
            return;
        }
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
    /// cause leaves an outcome that stands: a touch before `at`, or a
    /// deadline at or before it for a time exit or an unfilled entry. A
    /// funding gap or an overflow censors outright. Anything else is
    /// censored, including an exit that would stand without an observed
    /// fill, which the provider contract rules out.
    fn censor(&mut self, index: usize, at: EventTime, reason: CensorReason) {
        let outright = matches!(reason, CensorReason::FundingGap | CensorReason::Overflow);
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
