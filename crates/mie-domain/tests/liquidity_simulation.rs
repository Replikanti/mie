//! A deterministic exchange against the order-book liquidity flow
//! (ADR-043, decision 5).
//!
//! A seeded LCG drives an exchange book on a 0.1 USDT grid around
//! 10 000 USDT: adds, cancels and fills at the best level, each fill with its
//! taker trade at the fill's time. Actions are cut into book updates of one
//! to four actions whose `T` is the last action's time (or one millisecond
//! after the previous update's), so a batch boundary can fall inside a
//! millisecond. Faulty runs add checkpoint snapshots and an
//! order-book outage closed by a gap and a resync snapshot.
//!
//! The ground truth attributes every action of an update the engine applies
//! to a ready book with the engine's conventions: the update's minute, the
//! mid and trusted window before it, the accounted prices and the bands.
//! Asserts:
//!
//! - **conservation**: `added − filled − cancelled` equals the net change at
//!   the accounted prices, exactly, per side and band, in every 5m window;
//! - without offsetting actions (an add and a decrease at one price within
//!   one batch) and without resets, `added`, `cancelled` and `filled` equal
//!   the truth over the run, per side and band;
//! - with offsetting actions, `added` and `cancelled` never exceed the truth,
//!   and `filled` equals the traded volume at accounted prices in the outer
//!   band. An inferred fill is booked with the mid of the update that infers
//!   it, one update after its own, so an inner band may gain or lose at most
//!   the run's inferred volume against the truth;
//! - resets only lose flow: `added` never exceeds the truth, nor does
//!   `filled` in the outer band;
//! - the same seed gives the identical state.

use mie_domain::bars::Timeframe;
use mie_domain::book::{BookStep, OrderBook, Side};
use mie_domain::event::{Aggressor, BookSnapshot, BookUpdate, FeedGap, GapReason, Level};
use mie_domain::event::{MarketEvent, Stream, Trade};
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::liquidity::{BANDS, LiquidityWindow};
use mie_domain::num::{Price, Qty, SCALE};
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::time::EventTime;
use std::collections::BTreeMap;

/// 00:00 UTC on day 1.
const T0: i64 = 86_400_000;
const MINUTE: i64 = 60_000;
/// The span with exchange activity.
const ACTIVE_MS: i64 = 15 * MINUTE;
/// 0.1 USDT.
const TICK: i64 = SCALE / 10;
const MID: i64 = 10_000 * SCALE;
/// Levels a snapshot carries per side.
const SNAPSHOT_DEPTH: usize = 70;

/// Deterministic 64-bit LCG (Knuth's MMIX constants).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> i64 {
        i64::try_from(self.next() % n).unwrap()
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.next() % 1_000 < per_mille
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Add,
    Cancel,
    Fill,
}

/// One exchange action: `qty` added, cancelled or filled at `price`.
#[derive(Debug, Clone, Copy)]
struct Action {
    side: Side,
    price: i64,
    qty: i64,
    kind: Kind,
}

/// What a run exercises.
#[derive(Debug, Clone, Copy)]
struct Config {
    seed: u64,
    /// Whether an add and a decrease may hit one price within a batch.
    offsetting: bool,
    /// Checkpoint snapshots and an order-book outage.
    resets: bool,
}

/// One simulated session.
struct Session {
    events: Vec<MarketEvent>,
    /// The actions of each delivered update, by its last update id.
    actions: BTreeMap<u64, Vec<Action>>,
}

/// The exchange book: price units to quantity units per side.
struct Exchange {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
}

impl Exchange {
    fn side(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    fn best(&self, side: Side) -> i64 {
        match side {
            Side::Bid => *self.bids.last_key_value().unwrap().0,
            Side::Ask => *self.asks.first_key_value().unwrap().0,
        }
    }

    fn snapshot(&self, time: i64, id: u64) -> MarketEvent {
        let level = |(&price, &qty): (&i64, &i64)| Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        };
        MarketEvent::BookSnapshot(BookSnapshot {
            time: EventTime::from_millis(time),
            last_update_id: id,
            bids: self
                .bids
                .iter()
                .rev()
                .take(SNAPSHOT_DEPTH)
                .map(level)
                .collect(),
            asks: self.asks.iter().take(SNAPSHOT_DEPTH).map(level).collect(),
        })
    }

    /// The next action, or `None` when it would offset an earlier action of
    /// the batch at the same price and that is not allowed.
    fn act(
        &mut self,
        lcg: &mut Lcg,
        offsetting: bool,
        directions: &mut BTreeMap<(Side, i64), Kind>,
    ) -> Option<Action> {
        let side = if lcg.chance(500) {
            Side::Bid
        } else {
            Side::Ask
        };
        let (best_bid, best_ask) = (self.best(Side::Bid), self.best(Side::Ask));
        let levels = self.side(side).len();
        let roll = lcg.below(10);
        let kind = if levels < 30 || roll < 4 {
            Kind::Add
        } else if roll < 7 {
            Kind::Cancel
        } else {
            Kind::Fill
        };
        let book = self.side(side);
        let (price, qty) = match kind {
            Kind::Add => {
                // Within 60 ticks of the opposite best: the spread stays
                // tight, and some adds land beyond 5 bps.
                let k = 1 + lcg.below(60);
                let price = match side {
                    Side::Bid => best_ask - k * TICK,
                    Side::Ask => best_bid + k * TICK,
                };
                (price, (1 + lcg.below(50)) * SCALE / 100)
            }
            Kind::Cancel => {
                let reach = u64::try_from(book.len().min(50)).unwrap();
                let k = usize::try_from(lcg.below(reach)).unwrap();
                let (&price, &resting) = match side {
                    Side::Bid => book.iter().rev().nth(k).unwrap(),
                    Side::Ask => book.iter().nth(k).unwrap(),
                };
                let qty = if lcg.chance(500) {
                    resting
                } else {
                    1 + lcg.below(u64::try_from(resting).unwrap())
                };
                (price, qty)
            }
            Kind::Fill => {
                let price = match side {
                    Side::Bid => best_bid,
                    Side::Ask => best_ask,
                };
                let resting = book[&price];
                (price, resting.min((1 + lcg.below(40)) * SCALE / 100))
            }
        };
        let direction = if kind == Kind::Add {
            Kind::Add
        } else {
            Kind::Cancel
        };
        let previous = directions.entry((side, price)).or_insert(direction);
        if !offsetting && *previous != direction {
            return None;
        }
        let resting = book.get(&price).copied().unwrap_or(0);
        let after = if kind == Kind::Add {
            resting + qty
        } else {
            resting - qty
        };
        if after == 0 {
            book.remove(&price);
        } else {
            book.insert(price, after);
        }
        Some(Action {
            side,
            price,
            qty,
            kind,
        })
    }
}

fn simulate(config: Config) -> Session {
    let mut lcg = Lcg(config.seed);
    let mut exchange = Exchange {
        bids: BTreeMap::new(),
        asks: BTreeMap::new(),
    };
    for k in 0..90 {
        exchange
            .bids
            .insert(MID - (k + 1) * TICK, (1 + lcg.below(200)) * SCALE / 100);
        exchange
            .asks
            .insert(MID + (k + 1) * TICK, (1 + lcg.below(200)) * SCALE / 100);
    }
    let mut session = Session {
        events: Vec::new(),
        actions: BTreeMap::new(),
    };
    let mut id: u64 = 1_000;
    let mut trade_id: u64 = 0;
    session.events.push(exchange.snapshot(T0, id + 1));
    let mut time = T0 + 1;
    let mut last_delivered = T0;
    let mut previous_update = T0;
    // The outage: updates from 5:00 to 5:01 are lost.
    let outage = (T0 + 5 * MINUTE, T0 + 5 * MINUTE + 1_000);
    let mut resynced = false;
    let mut next_checkpoint = T0 + 45_000;
    while time < T0 + ACTIVE_MS {
        // A snapshot sorts before an update of its millisecond (ADR-028), so
        // it takes the next millisecond, and the next batch the one after.
        if config.resets && !resynced && time >= outage.1 {
            resynced = true;
            time += 1;
            session.events.push(MarketEvent::FeedGap(FeedGap {
                stream: Stream::OrderBook,
                start: EventTime::from_millis(last_delivered),
                end: EventTime::from_millis(time),
                reason: GapReason::Disconnected,
            }));
            session.events.push(exchange.snapshot(time, id + 1));
            time += 1;
        }
        if config.resets && time >= next_checkpoint {
            next_checkpoint += 45_000;
            time += 1;
            session.events.push(exchange.snapshot(time, id + 1));
            time += 1;
        }
        let lost = config.resets && time >= outage.0 && time < outage.1;
        let mut directions = BTreeMap::new();
        let mut actions = Vec::new();
        let (mut bids, mut asks) = (Vec::new(), Vec::new());
        let mut last_time = time;
        for _ in 0..1 + lcg.below(4) {
            // Increments of zero put actions, and batch boundaries, inside
            // one millisecond.
            time += lcg.below(40);
            let Some(action) = exchange.act(&mut lcg, config.offsetting, &mut directions) else {
                continue;
            };
            last_time = time;
            if action.kind == Kind::Fill {
                trade_id += 1;
                session.events.push(MarketEvent::Trade(Trade {
                    time: EventTime::from_millis(time),
                    trade_id,
                    price: Price::from_units(action.price),
                    qty: Qty::from_units(action.qty),
                    aggressor: match action.side {
                        Side::Bid => Aggressor::Sell,
                        Side::Ask => Aggressor::Buy,
                    },
                }));
            }
            let book = exchange.side(action.side);
            let level = Level {
                price: Price::from_units(action.price),
                qty: Qty::from_units(book.get(&action.price).copied().unwrap_or(0)),
            };
            match action.side {
                Side::Bid => bids.push(level),
                Side::Ask => asks.push(level),
            }
            actions.push(action);
        }
        if actions.is_empty() {
            continue;
        }
        // Every update has its own `T`, as the exchange's 100 ms batches do:
        // only one batch boundary can fall inside a millisecond, which the
        // one-update carry covers (ADR-043, decision 5).
        let update_time = last_time.max(previous_update + 1);
        previous_update = update_time;
        time = time.max(update_time);
        let first = id + 1;
        id += 1 + lcg.below(4).unsigned_abs();
        if lost {
            continue;
        }
        session.events.push(MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(update_time),
            first_update_id: first,
            last_update_id: id,
            prev_update_id: first - 1,
            bids,
            asks,
        }));
        session.actions.insert(id, actions);
        last_delivered = update_time;
    }
    // Ten quiet minutes with one trade each, far above the book, close the
    // last active minutes.
    for minute in 0..10 {
        trade_id += 1;
        session.events.push(MarketEvent::Trade(Trade {
            time: EventTime::from_millis(T0 + ACTIVE_MS + minute * MINUTE + 30_000),
            trade_id,
            price: Price::from_units(2 * MID),
            qty: Qty::from_units(1),
            aggressor: Aggressor::Buy,
        }));
    }
    session.events.sort();
    session
}

/// Twice the mid and the trusted window of a two-sided, uncrossed book,
/// computed here independently of the engine.
#[derive(Clone, Copy)]
struct Frame {
    sum: i128,
    lowest_bid: Option<Price>,
    highest_ask: Option<Price>,
}

impl Frame {
    fn of(book: &OrderBook) -> Option<Self> {
        let (bid, ask) = (book.best_bid()?.price, book.best_ask()?.price);
        (bid < ask).then(|| Self {
            sum: i128::from(bid.units()) + i128::from(ask.units()),
            lowest_bid: book.window().lowest_bid,
            highest_ask: book.window().highest_ask,
        })
    }

    fn within(self, side: Side, price: i64, band: usize) -> bool {
        let twice = 2 * i128::from(price);
        let distance = match side {
            Side::Bid => self.sum - twice,
            Side::Ask => twice - self.sum,
        };
        distance * i128::from(SCALE) <= i128::from(catalog::BOOK_BANDS[band].units()) * self.sum
    }

    fn accounted(self, side: Side, price: i64) -> bool {
        let inside = match side {
            Side::Bid => self.lowest_bid.is_some_and(|w| price >= w.units()),
            Side::Ask => self.highest_ask.is_some_and(|w| price <= w.units()),
        };
        inside && self.within(side, price, BANDS - 1)
    }
}

/// Flow per side: added, cancelled, filled, the net change and (for the
/// engine only) the inferred fills.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Truth {
    added: i64,
    cancelled: i64,
    filled: i64,
    net: i64,
    inferred: i64,
}

impl Truth {
    fn add(&mut self, other: &Self) {
        self.added += other.added;
        self.cancelled += other.cancelled;
        self.filled += other.filled;
        self.net += other.net;
        self.inferred += other.inferred;
    }
}

/// Truth per band and side (bid 0, ask 1) of one minute.
type MinuteTruth = [[Truth; 2]; BANDS];

fn side_index(side: Side) -> usize {
    match side {
        Side::Bid => 0,
        Side::Ask => 1,
    }
}

/// What a run produced: the truth per minute, the 5m windows the engine
/// showed, the first valid book's minute and the final state.
struct Outcome {
    truth: BTreeMap<i64, MinuteTruth>,
    windows: Vec<LiquidityWindow>,
    start: i64,
    state: MarketState,
}

fn run(config: Config) -> Outcome {
    let session = simulate(config);
    let mut engine = MarketStateEngine::new();
    let mut truth: BTreeMap<i64, MinuteTruth> = BTreeMap::new();
    let mut windows: Vec<LiquidityWindow> = Vec::new();
    for (i, event) in session.events.iter().enumerate() {
        if let (MarketEvent::BookUpdate(update), FeatureValue::Ready(book)) =
            (event, &engine.state().book.l2)
            && book.peek(event) == BookStep::Applied
            && let Some(frame) = Frame::of(book)
        {
            let minute = update.time.as_millis().div_euclid(MINUTE) * MINUTE;
            let cell = truth.entry(minute).or_default();
            for action in &session.actions[&update.last_update_id] {
                if !frame.accounted(action.side, action.price) {
                    continue;
                }
                for (band, sides) in cell.iter_mut().enumerate() {
                    if frame.within(action.side, action.price, band) {
                        let t = &mut sides[side_index(action.side)];
                        match action.kind {
                            Kind::Add => t.added += action.qty,
                            Kind::Cancel => t.cancelled += action.qty,
                            Kind::Fill => t.filled += action.qty,
                        }
                    }
                }
            }
            for (side, levels) in [(Side::Bid, &update.bids), (Side::Ask, &update.asks)] {
                for (j, level) in levels.iter().enumerate() {
                    let price = level.price.units();
                    if levels[j + 1..]
                        .iter()
                        .any(|later| later.price == level.price)
                        || !frame.accounted(side, price)
                    {
                        continue;
                    }
                    let old = book.qty_at(side, level.price).map_or(0, Qty::units);
                    for (band, sides) in cell.iter_mut().enumerate() {
                        if frame.within(side, price, band) {
                            sides[side_index(side)].net += level.qty.units() - old;
                        }
                    }
                }
            }
        }
        engine
            .apply(event)
            .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
        if let FeatureValue::Ready(window) = engine.state().book.windows.get(Timeframe::M5).unwrap()
            && windows.last().is_none_or(|last| last.end != window.end)
        {
            windows.push(*window);
        }
    }
    Outcome {
        truth,
        windows,
        start: T0,
        state: engine.state().clone(),
    }
}

/// The truth summed over the minutes in `[from, to)`.
fn truth_over(truth: &BTreeMap<i64, MinuteTruth>, from: i64, to: i64) -> MinuteTruth {
    let mut sum = MinuteTruth::default();
    for (_, minute) in truth.range(from..to) {
        for (band, sides) in sum.iter_mut().enumerate() {
            for (side, t) in sides.iter_mut().enumerate() {
                t.add(&minute[band][side]);
            }
        }
    }
    sum
}

/// The engine's flow of `window` per band and side, as truth.
fn engine_flow(window: &LiquidityWindow) -> MinuteTruth {
    let mut out = MinuteTruth::default();
    for (band, sides) in out.iter_mut().enumerate() {
        for (side, t) in sides.iter_mut().enumerate() {
            let flow = window.bands[band].side(if side == 0 { Side::Bid } else { Side::Ask });
            *t = Truth {
                added: flow.added.units(),
                cancelled: flow.cancelled.units(),
                filled: flow.filled.units(),
                net: flow.added.units() - flow.filled.units() - flow.cancelled.units(),
                inferred: flow.inferred.units(),
            };
        }
    }
    out
}

/// Asserts conservation in every window and returns the engine's and the
/// truth's totals over disjoint windows covering the run.
fn check(config: Config) -> (MinuteTruth, MinuteTruth) {
    let outcome = run(config);
    assert_eq!(outcome.windows.len(), 20, "{config:?}");
    let (mut engine_total, mut truth_total) = (MinuteTruth::default(), MinuteTruth::default());
    for window in &outcome.windows {
        let end = window.end.as_millis();
        let from = end - 5 * MINUTE;
        let truth = truth_over(&outcome.truth, from, end);
        let engine = engine_flow(window);
        for band in 0..BANDS {
            for side in 0..2 {
                assert_eq!(
                    engine[band][side].net, truth[band][side].net,
                    "{config:?} conservation, window {window}, band {band}, side {side}"
                );
            }
        }
        if (end - outcome.start) % (5 * MINUTE) == 0 {
            for band in 0..BANDS {
                for side in 0..2 {
                    engine_total[band][side].add(&engine[band][side]);
                    truth_total[band][side].add(&truth[band][side]);
                }
            }
        }
    }
    // Every active minute is covered by the disjoint windows.
    let last = outcome.windows.last().unwrap().end.as_millis();
    assert!(last >= T0 + ACTIVE_MS + MINUTE, "{config:?}");
    // The same seed reproduces the state exactly.
    let again = run(config);
    assert_eq!(again.state, outcome.state, "{config:?}");
    assert_eq!(
        again.state.state_hash(),
        outcome.state.state_hash(),
        "{config:?}"
    );
    (engine_total, truth_total)
}

#[test]
fn without_offsetting_actions_the_flow_is_exact() {
    for seed in 1..=3 {
        let config = Config {
            seed,
            offsetting: false,
            resets: false,
        };
        let (engine, truth) = check(config);
        for band in 0..BANDS {
            for side in 0..2 {
                let (e, t) = (engine[band][side], truth[band][side]);
                assert_eq!(e.added, t.added, "{config:?} band {band} side {side}");
                assert_eq!(
                    e.cancelled, t.cancelled,
                    "{config:?} band {band} side {side}"
                );
                assert_eq!(e.filled, t.filled, "{config:?} band {band} side {side}");
                assert!(t.filled > 0 && t.cancelled > 0 && t.added > 0, "{config:?}");
            }
        }
    }
}

#[test]
fn offsetting_actions_make_added_and_cancelled_lower_bounds() {
    let outer = BANDS - 1;
    let mut inferred = 0;
    for seed in 4..=6 {
        let config = Config {
            seed,
            offsetting: true,
            resets: false,
        };
        let (engine, truth) = check(config);
        for side in 0..2 {
            let shift = engine[outer][side].inferred;
            inferred += shift;
            for band in 0..BANDS {
                let (e, t) = (engine[band][side], truth[band][side]);
                assert!(e.added <= t.added, "{config:?} band {band} side {side}");
                assert!(
                    e.cancelled <= t.cancelled,
                    "{config:?} band {band} side {side}"
                );
                assert!(
                    (e.filled - t.filled).abs() <= shift,
                    "{config:?} band {band} side {side}"
                );
            }
            assert_eq!(
                engine[outer][side].filled, truth[outer][side].filled,
                "{config:?} side {side}"
            );
        }
    }
    // Replenished levels were exercised.
    assert!(inferred > 0);
}

#[test]
fn resets_only_lose_flow() {
    let outer = BANDS - 1;
    for seed in 7..=9 {
        let config = Config {
            seed,
            offsetting: true,
            resets: true,
        };
        let (engine, truth) = check(config);
        for side in 0..2 {
            let shift = engine[outer][side].inferred;
            for band in 0..BANDS {
                let (e, t) = (engine[band][side], truth[band][side]);
                assert!(e.added <= t.added, "{config:?} band {band} side {side}");
                assert!(
                    e.filled <= t.filled + shift,
                    "{config:?} band {band} side {side}"
                );
            }
            assert!(
                engine[outer][side].filled <= truth[outer][side].filled,
                "{config:?} side {side}"
            );
        }
        // The outage was exercised.
        let outcome = run(config);
        assert!(outcome.windows.iter().any(|window| window.feed_gap));
    }
}
