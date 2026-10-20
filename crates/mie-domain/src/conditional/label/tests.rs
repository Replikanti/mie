//! Tests of the forward labeler (ADR-055): hand-built paths for every rule,
//! a naive per-label model on random and golden tapes, the D8 table, the
//! thinning flags, the leakage property, and the goldens.

use super::*;
use crate::bars::tests::{Lcg, random_tape};
use crate::candidate::divergence::{DivergenceEvent, OiComparison};
use crate::conditional::ContextSampler;
use crate::conditional::tests::forbidden_in;
use crate::event::samples::t;
use crate::event::{Aggressor, FeedGap, GapReason, OpenInterest, Trade};
use crate::feature::FeatureValue;
use crate::num::{Qty, SCALE};
use crate::state::MarketStateEngine;
use crate::trigger::TriggerFamily;

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;

/// The decision time of the hand-built paths.
const T0: i64 = 10 * DAY;

/// Whole USDT as a price.
fn usdt(whole: i64) -> Price {
    Price::from_units(whole * SCALE)
}

/// Hundredths of a USDT as a price.
fn cents(hundredths: i64) -> Price {
    Price::from_units(hundredths * (SCALE / 100))
}

/// A trade at `millis` with id `trade_id`.
fn trade(millis: i64, trade_id: u64, price: Price) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: t(millis),
        trade_id,
        price,
        qty: Qty::from_units(10_000_000),
        aggressor: Aggressor::Buy,
    })
}

/// A gap on `stream` over `[start, end]`.
fn gap(stream: Stream, start: i64, end: i64) -> MarketEvent {
    MarketEvent::FeedGap(FeedGap {
        stream,
        start: t(start),
        end: t(end),
        reason: GapReason::Disconnected,
    })
}

/// An open-interest observation at `millis`: closes every window ending at
/// or before it, and touches nothing.
fn tick_of_time(millis: i64) -> MarketEvent {
    MarketEvent::OpenInterest(OpenInterest {
        time: t(millis),
        open_interest: Qty::from_units(80_000 * SCALE),
        resolution_ms: 300_000,
    })
}

/// A baseline record from a two-minute tape, whose fields the tests
/// replace.
fn template() -> ContextRecord {
    let events = [
        trade(1_000, 1, usdt(60_000)),
        trade(MINUTE + 1_000, 2, usdt(60_000)),
    ];
    let mut engine = MarketStateEngine::new();
    let mut sampler = ContextSampler::new(None);
    let mut records = Vec::new();
    for event in &events {
        engine.apply(event).unwrap();
        sampler.observe(event, &engine, |record| records.push(record));
    }
    assert!(!records[0].state.atr.is_ready());
    records.remove(0)
}

/// A baseline record at `event` with the reference `reference` and the
/// ATR `atr` (warming up with `None`).
fn record_at(event: CanonicalKey, reference: Price, atr: Option<Price>) -> ContextRecord {
    let mut record = template();
    record.event = event;
    record.state.last_price = FeatureValue::Ready(reference);
    if let Some(atr) = atr {
        record.state.atr = FeatureValue::Ready(atr);
    }
    record
}

/// The standard decision: a trade at `T0` with id 10 at 60 000 USDT, and a
/// baseline record on it with an ATR of 1 000 USDT. The barrier offsets
/// are then 90.45, 156.66, 313.33, 626.66 and 1 534.99 USDT.
fn decision() -> Vec<Step> {
    let event = trade(T0, 10, usdt(60_000));
    let record = record_at(event.canonical_key(), usdt(60_000), Some(usdt(1_000)));
    vec![Step::See(event), Step::Open(Box::new(record))]
}

/// One input to a labeler.
enum Step {
    /// An accepted event.
    See(MarketEvent),
    /// A record of the last seen event (boxed: a record is large).
    Open(Box<ContextRecord>),
    /// A rejected event.
    Reject(MarketEvent),
}

/// Runs `steps` through a fresh labeler and finishes it.
fn run(steps: Vec<Step>) -> (Vec<ForwardLabel>, LabelStats) {
    let mut labeler = ForwardLabeler::new();
    let mut labels = Vec::new();
    for step in steps {
        match step {
            Step::See(event) => labeler.observe(&event, |label| labels.push(label)),
            Step::Open(record) => labeler.open(&record),
            Step::Reject(event) => labeler.reject(&event),
        }
    }
    labeler.finish(|label| labels.push(label));
    (labels, labeler.stats())
}

/// The single label of `steps`.
fn label_of(steps: Vec<Step>) -> ForwardLabel {
    let (mut labels, stats) = run(steps);
    assert_eq!(labels.len(), 1);
    assert_eq!((stats.opened, stats.emitted), (1, 1));
    labels.remove(0)
}

/// `steps` after the standard decision.
fn after_decision(steps: impl IntoIterator<Item = Step>) -> Vec<Step> {
    let mut all = decision();
    all.extend(steps);
    all
}

fn see(event: MarketEvent) -> Step {
    Step::See(event)
}

fn excursion(size: Price, at: i64) -> Excursion {
    Excursion {
        size,
        at: Some(t(at)),
    }
}

fn censor(at: i64, reason: CensorReason) -> Censor {
    Censor { at: t(at), reason }
}

// ---------------------------------------------------------------- races

#[test]
fn barriers_are_symmetric_atr_multiples() {
    let label = label_of(after_decision([see(tick_of_time(T0 + DAY))]));
    let offsets = [
        cents(9_045),
        cents(15_666),
        cents(31_333),
        cents(62_666),
        cents(153_499),
    ];
    for (horizon, offset) in label.horizons.iter().zip(offsets) {
        let barriers = horizon.barriers.unwrap();
        assert_eq!(
            barriers.upper.units() - usdt(60_000).units(),
            offset.units()
        );
        assert_eq!(
            usdt(60_000).units() - barriers.lower.units(),
            offset.units()
        );
    }
    assert_eq!(label.reference, Some(usdt(60_000)));
    assert_eq!(label.atr, Some(usdt(1_000)));
}

#[test]
fn barrier_offsets_round_half_to_even() {
    for (position, multiple) in BARRIER_MULTIPLE_E5.into_iter().enumerate() {
        // The smallest ATRs whose product ends in exactly half a unit, and
        // whose quotient is even and odd.
        let halves: Vec<i64> = (1..400_000)
            .filter(|atr| (atr * multiple) % MULTIPLE_SCALE == MULTIPLE_SCALE / 2)
            .take(2)
            .collect();
        assert_eq!(halves.len(), 2, "{position}");
        for atr in halves {
            let quotient = atr * multiple / MULTIPLE_SCALE;
            let expected = quotient + (quotient & 1);
            assert_eq!(
                barrier_offset(Price::from_units(atr), position).map(Price::units),
                Some(expected).filter(|units| *units > 0),
                "{position} {atr}"
            );
        }
        // Below and above half.
        let below = (1..400_000)
            .find(|atr| (atr * multiple) % MULTIPLE_SCALE == MULTIPLE_SCALE / 2 - 1)
            .unwrap_or(0);
        if below > 0 {
            assert_eq!(
                barrier_offset(Price::from_units(below), position).map(Price::units),
                Some(below * multiple / MULTIPLE_SCALE).filter(|units| *units > 0)
            );
        }
    }
    assert_eq!(barrier_offset(Price::from_units(0), 4), None);
    assert_eq!(barrier_offset(Price::from_units(-5), 4), None);
}

#[test]
fn the_upper_and_the_lower_barrier_race() {
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        // At or above 60 090.45: the 5m upper barrier.
        see(trade(T0 + 2_000, 12, usdt(60_091))),
        // Beyond the 5m lower barrier, but in a later millisecond.
        see(trade(T0 + 3_000, 13, usdt(59_900))),
        // At or below 59 843.34: the 15m lower barrier.
        see(trade(T0 + 4_000, 14, usdt(59_800))),
        see(tick_of_time(T0 + DAY)),
    ]));
    let [m5, m15, h1, h4, d1] = label.horizons;
    assert_eq!(
        m5.race,
        Race::Upper {
            at: t(T0 + 2_000),
            same_ms: false
        }
    );
    assert_eq!(
        m15.race,
        Race::Lower {
            at: t(T0 + 4_000),
            same_ms: false
        }
    );
    for horizon in [h1, h4, d1] {
        assert_eq!(horizon.race, Race::Neither, "{horizon}");
        assert_eq!(horizon.up, excursion(usdt(91), T0 + 2_000));
        assert_eq!(horizon.down, excursion(usdt(200), T0 + 4_000));
        assert_eq!(horizon.last, Some(usdt(59_800)));
        assert_eq!(horizon.censored, None);
    }
}

#[test]
fn neither_barrier_at_the_vertical_barrier() {
    let label = label_of(after_decision([
        see(trade(T0 + MINUTE, 11, usdt(60_050))),
        see(trade(T0 + 2 * MINUTE, 12, usdt(59_950))),
        see(trade(T0 + 2 * MINUTE + 1, 13, usdt(60_050))),
        see(tick_of_time(T0 + DAY)),
    ]));
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Neither, "{horizon}");
        assert_eq!(horizon.censored, None);
        // The earliest trade at the maximum.
        assert_eq!(horizon.up, excursion(usdt(50), T0 + MINUTE));
        assert_eq!(horizon.down, excursion(usdt(50), T0 + 2 * MINUTE));
    }
}

#[test]
fn the_race_is_unavailable_without_a_ready_atr_or_a_zero_offset() {
    let event = trade(T0, 10, usdt(60_000));
    let warming = record_at(event.canonical_key(), usdt(60_000), None);
    let label = label_of(vec![
        see(event),
        Step::Open(Box::new(warming)),
        see(trade(T0 + 1_000, 11, usdt(70_000))),
        see(tick_of_time(T0 + DAY)),
    ]);
    assert_eq!(label.atr, None);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Unavailable);
        assert_eq!(horizon.barriers, None);
        // The excursions are still read.
        assert_eq!(horizon.up, excursion(usdt(10_000), T0 + 1_000));
    }
    // One unit of ATR: b_H · 1 rounds to 0 up to 1h, to 1 and 2 units at
    // 4h and 1d.
    let event = trade(T0, 10, usdt(60_000));
    let tiny = record_at(
        event.canonical_key(),
        usdt(60_000),
        Some(Price::from_units(1)),
    );
    let label = label_of(vec![
        see(event),
        Step::Open(Box::new(tiny)),
        see(tick_of_time(T0 + DAY)),
    ]);
    let races: Vec<Race> = label.horizons.iter().map(|horizon| horizon.race).collect();
    assert_eq!(
        races,
        [
            Race::Unavailable,
            Race::Unavailable,
            Race::Unavailable,
            Race::Neither,
            Race::Neither
        ]
    );
    assert_eq!(
        label.horizons[4].barriers.unwrap().upper.units() - usdt(60_000).units(),
        2
    );
}

#[test]
fn the_window_end_is_exclusive() {
    // A trade at exactly t + 5m is outside the 5m window and closes it.
    let label = label_of(after_decision([
        see(trade(T0 + 5 * MINUTE, 11, usdt(60_100))),
        see(tick_of_time(T0 + DAY)),
    ]));
    let [m5, m15, ..] = label.horizons;
    assert_eq!(m5.race, Race::Neither);
    assert_eq!((m5.up, m5.last), (Excursion::NONE, None));
    assert_eq!(m15.race, Race::Neither);
    assert_eq!(m15.up, excursion(usdt(100), T0 + 5 * MINUTE));
    // One millisecond earlier it is inside.
    let label = label_of(after_decision([
        see(trade(T0 + 5 * MINUTE - 1, 11, usdt(60_100))),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(
        label.horizons[0].race,
        Race::Upper {
            at: t(T0 + 5 * MINUTE - 1),
            same_ms: false
        }
    );
}

// ----------------------------------------------------------------- ties

#[test]
fn a_same_millisecond_tie_goes_to_the_lower_trade_id() {
    let at = T0 + 1_000;
    for upper_first in [true, false] {
        let (first, second) = if upper_first {
            (usdt(60_100), usdt(59_900))
        } else {
            (usdt(59_900), usdt(60_100))
        };
        let label = label_of(after_decision([
            see(trade(at, 11, first)),
            see(trade(at, 12, second)),
            see(tick_of_time(T0 + DAY)),
        ]));
        let expected = if upper_first {
            Race::Upper {
                at: t(at),
                same_ms: true,
            }
        } else {
            Race::Lower {
                at: t(at),
                same_ms: true,
            }
        };
        assert_eq!(label.horizons[0].race, expected);
        // 15m: neither barrier reached.
        assert_eq!(label.horizons[1].race, Race::Neither);
    }
    // The other barrier one millisecond later is not a tie.
    let label = label_of(after_decision([
        see(trade(at, 11, usdt(60_100))),
        see(trade(at + 1, 12, usdt(59_900))),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(
        label.horizons[0].race,
        Race::Upper {
            at: t(at),
            same_ms: false
        }
    );
}

#[test]
fn a_single_trade_touches_one_barrier() {
    // Exactly at the upper barrier: a touch of it, and only of it.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, cents(6_009_045))),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(
        label.horizons[0].race,
        Race::Upper {
            at: t(T0 + 1_000),
            same_ms: false
        }
    );
    // One unit inside it: no touch.
    let label = label_of(after_decision([
        see(trade(
            T0 + 1_000,
            11,
            Price::from_units(cents(6_009_045).units() - 1),
        )),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(label.horizons[0].race, Race::Neither);
    // Exactly at the lower barrier.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, cents(5_990_955))),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(
        label.horizons[0].race,
        Race::Lower {
            at: t(T0 + 1_000),
            same_ms: false
        }
    );
}

// ------------------------------------------------------------- boundary

#[test]
fn the_decision_trade_is_outside_the_window_and_a_later_same_ms_trade_inside() {
    // A trade before the decision in its millisecond, and the decision
    // trade itself: neither is in the window.
    let decision_trade = trade(T0, 10, usdt(60_000));
    let record = record_at(
        decision_trade.canonical_key(),
        usdt(60_000),
        Some(usdt(1_000)),
    );
    let label = label_of(vec![
        see(trade(T0, 9, usdt(60_500))),
        see(decision_trade.clone()),
        Step::Open(Box::new(record.clone())),
        see(tick_of_time(T0 + DAY)),
    ]);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Neither);
        assert_eq!(
            (horizon.up, horizon.down),
            (Excursion::NONE, Excursion::NONE)
        );
        assert_eq!(horizon.last, None);
    }
    // A trade in the decision's millisecond with a higher id is inside.
    let label = label_of(vec![
        see(decision_trade),
        Step::Open(Box::new(record)),
        see(trade(T0, 11, usdt(60_500))),
        see(tick_of_time(T0 + DAY)),
    ]);
    assert_eq!(
        label.horizons[0].race,
        Race::Upper {
            at: t(T0),
            same_ms: false
        }
    );
    assert_eq!(label.horizons[4].last, Some(usdt(60_500)));
}

// ------------------------------------------------------------ censoring

#[test]
fn a_gap_before_a_touch_censors_from_its_start() {
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(trade(T0 + 1_500, 12, usdt(60_050))),
        see(gap(Stream::Trades, T0 + 2_000, T0 + 3_000)),
        see(trade(T0 + 4_000, 13, usdt(61_000))),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 2_000, CensorReason::Gap);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Censored(cut), "{horizon}");
        assert_eq!(horizon.censored, Some(cut));
        // What was seen before the gap stays, as a lower bound; the
        // earliest trade at the maximum.
        assert_eq!(horizon.up, excursion(usdt(50), T0 + 1_000));
        assert_eq!(horizon.last, Some(usdt(60_050)));
    }
}

#[test]
fn a_gap_after_a_touch_keeps_the_race() {
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_100))),
        see(gap(Stream::Trades, T0 + 2_000, T0 + 3_000)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 2_000, CensorReason::Gap);
    let [m5, m15, ..] = label.horizons;
    assert_eq!(
        m5.race,
        Race::Upper {
            at: t(T0 + 1_000),
            same_ms: false
        }
    );
    assert_eq!(m5.censored, Some(cut));
    assert_eq!(m15.race, Race::Censored(cut));
    // A gap that starts after a window's end leaves that window alone.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(gap(Stream::Trades, T0 + 5 * MINUTE, T0 + 20 * MINUTE)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let [m5, m15, ..] = label.horizons;
    assert_eq!((m5.race, m5.censored), (Race::Neither, None));
    let cut = censor(T0 + 5 * MINUTE, CensorReason::Gap);
    assert_eq!((m15.race, m15.censored), (Race::Censored(cut), Some(cut)));
}

#[test]
fn a_late_gap_that_starts_before_the_decision_censors_from_the_decision() {
    // Delivered after a touch in the window, starting before the decision:
    // nothing after the decision is known.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_100))),
        see(gap(Stream::Trades, T0 - 10 * MINUTE, T0 + 2_000)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0, CensorReason::Gap);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Censored(cut), "{horizon}");
        assert_eq!((horizon.up, horizon.last), (Excursion::NONE, None));
    }
}

#[test]
fn gaps_of_other_streams_do_not_censor() {
    let path = [
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(trade(T0 + 10 * MINUTE, 12, usdt(59_000))),
        see(tick_of_time(T0 + DAY)),
    ];
    let plain = label_of(after_decision(path.iter().map(|step| match step {
        Step::See(event) => see(event.clone()),
        _ => unreachable!(),
    })));
    let gapped = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(gap(Stream::OrderBook, T0 + 2_000, T0 + 3_000)),
        see(gap(Stream::OpenInterest, T0 + 2_000, T0 + 4_000)),
        see(gap(Stream::Klines, T0 + 2_000, T0 + 5_000)),
        see(trade(T0 + 10 * MINUTE, 12, usdt(59_000))),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert_eq!(gapped, plain);
    assert!(
        plain
            .horizons
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
}

#[test]
fn a_gap_since_the_last_trade_makes_the_reference_stale() {
    // The decision is the gap event itself (it closes a minute).
    let decision_gap = gap(Stream::Trades, T0 - 4_000, T0);
    let record = record_at(
        decision_gap.canonical_key(),
        usdt(60_000),
        Some(usdt(1_000)),
    );
    let label = label_of(vec![
        see(trade(T0 - 5_000, 9, usdt(60_000))),
        see(decision_gap),
        Step::Open(Box::new(record)),
        see(trade(T0 + 1_000, 11, usdt(60_100))),
        see(tick_of_time(T0 + DAY)),
    ]);
    let cut = censor(T0, CensorReason::StaleReference);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Censored(cut));
        assert_eq!(horizon.censored, Some(cut));
        assert_eq!((horizon.up, horizon.last), (Excursion::NONE, None));
    }
    // A trade after the gap refreshes the reference.
    let fresh = trade(T0 + 1, 10, usdt(60_000));
    let record = record_at(fresh.canonical_key(), usdt(60_000), Some(usdt(1_000)));
    let label = label_of(vec![
        see(trade(T0 - 5_000, 9, usdt(60_000))),
        see(gap(Stream::Trades, T0 - 4_000, T0)),
        see(fresh),
        Step::Open(Box::new(record)),
        see(trade(T0 + 1_000, 11, usdt(60_100))),
        see(tick_of_time(T0 + DAY)),
    ]);
    assert_eq!(
        label.horizons[0].race,
        Race::Upper {
            at: t(T0 + 1_000),
            same_ms: false
        }
    );
    // No trade before the decision at all: no reference.
    let event = tick_of_time(T0);
    let mut record = record_at(event.canonical_key(), usdt(60_000), Some(usdt(1_000)));
    record.state.last_price = template().state.atr;
    let label = label_of(vec![
        see(event),
        Step::Open(Box::new(record)),
        see(tick_of_time(T0 + DAY)),
    ]);
    assert_eq!(label.reference, None);
    assert!(
        label
            .horizons
            .iter()
            .all(|horizon| horizon.race == Race::Unavailable
                && horizon.censored == Some(censor(T0, CensorReason::StaleReference)))
    );
}

#[test]
fn a_rejected_trades_event_censors_from_its_time() {
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        // Other streams are ignored.
        Step::Reject(tick_of_time(T0 + 1_500)),
        Step::Reject(trade(T0 + 2_000, 12, usdt(61_000))),
        see(trade(T0 + 3_000, 13, usdt(61_000))),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 2_000, CensorReason::Rejected);
    for horizon in label.horizons {
        assert_eq!(horizon.race, Race::Censored(cut));
        assert_eq!(horizon.up, excursion(usdt(50), T0 + 1_000));
    }
    // A rejection only: nothing was censored.
    let label = label_of(after_decision([
        Step::Reject(tick_of_time(T0 + 1_500)),
        see(tick_of_time(T0 + DAY)),
    ]));
    assert!(
        label
            .horizons
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
}

#[test]
fn the_end_of_data_censors_and_drops_nothing() {
    let mut steps = Vec::new();
    for (index, at) in [0, 2 * HOUR, 20 * HOUR].into_iter().enumerate() {
        let event = trade(T0 + at, 10 + index as u64, usdt(60_000));
        steps.push(see(event.clone()));
        steps.push(Step::Open(Box::new(record_at(
            event.canonical_key(),
            usdt(60_000),
            Some(usdt(1_000)),
        ))));
    }
    steps.push(see(trade(T0 + 20 * HOUR + 1_000, 20, usdt(60_010))));
    let (labels, stats) = run(steps);
    assert_eq!(labels.len(), 3);
    assert_eq!((stats.opened, stats.emitted), (3, 3));
    let cut = censor(T0 + 20 * HOUR + 1_000, CensorReason::EndOfData);
    // The first record's 5m … 4h windows closed; its 1d window did not.
    let first = &labels[0];
    assert!(
        first.horizons[..4]
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
    assert_eq!(first.horizons[4].race, Race::Censored(cut));
    // The last record: everything censored, the trade before the cut read.
    let last = &labels[2];
    for horizon in last.horizons {
        assert_eq!(horizon.censored, Some(cut));
        assert_eq!(horizon.race, Race::Censored(cut));
        assert_eq!(horizon.last, None);
    }
    let events: Vec<CanonicalKey> = [0, 2 * HOUR, 20 * HOUR]
        .into_iter()
        .zip(10..)
        .map(|(at, id)| trade(T0 + at, id, usdt(60_000)).canonical_key())
        .collect();
    assert_eq!(
        labels.iter().map(|label| label.event).collect::<Vec<_>>(),
        events
    );
}

#[test]
fn a_gap_censors_a_closed_window_of_an_unemitted_label() {
    // Review regression (PR #148): a trades outage over [T0+2s, T0+10m] is
    // announced only when trades resume (ADR-028), and an open-interest
    // event at T0+5m closes the 5m window meanwhile. The label is not
    // emitted yet, so the gap still censors that window, exactly as on the
    // same tape without the other stream's event.
    let outage = |other: bool| {
        let mut steps = vec![see(trade(T0 + 1_000, 11, usdt(60_050)))];
        if other {
            steps.push(see(tick_of_time(T0 + 5 * MINUTE)));
        }
        steps.extend([
            see(gap(Stream::Trades, T0 + 2_000, T0 + 10 * MINUTE)),
            see(trade(T0 + 10 * MINUTE, 12, usdt(60_050))),
            see(tick_of_time(T0 + DAY)),
        ]);
        run(after_decision(steps))
    };
    let (labels, stats) = outage(true);
    let (plain, _) = outage(false);
    assert_eq!(labels, plain);
    assert_eq!(stats.reach_backs, 0);
    let cut = censor(T0 + 2_000, CensorReason::Gap);
    for horizon in labels[0].horizons {
        assert_eq!(horizon.race, Race::Censored(cut), "{horizon}");
        assert_eq!(horizon.censored, Some(cut));
        assert_eq!(horizon.up, excursion(usdt(50), T0 + 1_000));
        assert_eq!(horizon.last, Some(usdt(60_050)));
    }
    // A race won strictly before the gap keeps its winner on the closed
    // window; a rejected trade censors a closed window the same way.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_100))),
        see(tick_of_time(T0 + 5 * MINUTE)),
        see(gap(Stream::Trades, T0 + 2_000, T0 + 10 * MINUTE)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let upper = Race::Upper {
        at: t(T0 + 1_000),
        same_ms: false,
    };
    assert_eq!(
        (label.horizons[0].race, label.horizons[0].censored),
        (upper, Some(cut))
    );
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(tick_of_time(T0 + 5 * MINUTE)),
        Step::Reject(trade(T0 + 3_000, 5, usdt(1))),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 3_000, CensorReason::Rejected);
    assert_eq!(label.horizons[0].race, Race::Censored(cut));
}

#[test]
fn the_earliest_censor_of_an_unemitted_label_wins() {
    // A rejected trade censors every window at T0+1h; a later gap that
    // started earlier cuts them again before the label is emitted, while a
    // later censor changes nothing.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        Step::Reject(trade(T0 + HOUR, 5, usdt(1))),
        see(gap(Stream::Trades, T0 + 2_000, T0 + HOUR + 1_000)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 2_000, CensorReason::Gap);
    for horizon in label.horizons {
        assert_eq!(
            (horizon.race, horizon.censored),
            (Race::Censored(cut), Some(cut))
        );
    }
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        Step::Reject(trade(T0 + 2_000, 5, usdt(1))),
        see(gap(Stream::Trades, T0 + 3_000, T0 + HOUR)),
        see(tick_of_time(T0 + DAY)),
    ]));
    let cut = censor(T0 + 2_000, CensorReason::Rejected);
    for horizon in label.horizons {
        assert_eq!(horizon.censored, Some(cut), "{horizon}");
    }
}

#[test]
fn a_late_gap_after_emission_reaches_back_and_changes_nothing() {
    let mut labeler = ForwardLabeler::new();
    let mut labels = Vec::new();
    for step in decision() {
        match step {
            Step::See(event) => labeler.observe(&event, |label| labels.push(label)),
            Step::Open(record) => labeler.open(&record),
            Step::Reject(_) => unreachable!(),
        }
    }
    labeler.observe(&trade(T0 + 1_000, 11, usdt(60_100)), |label| {
        labels.push(label);
    });
    labeler.observe(&tick_of_time(T0 + DAY), |label| labels.push(label));
    assert_eq!(labels.len(), 1);
    let emitted = labels[0].clone();
    // Starts inside the emitted window: reaches back.
    labeler.observe(
        &gap(Stream::Trades, T0 + 2_000, T0 + DAY + 5_000),
        |label| {
            labels.push(label);
        },
    );
    assert_eq!(labeler.stats().reach_backs, 1);
    // Starts at the window's end: does not.
    labeler.observe(&gap(Stream::Trades, T0 + DAY, T0 + DAY + 6_000), |label| {
        labels.push(label);
    });
    // A rejected trade inside it does.
    labeler.reject(&trade(T0 + 3_000, 5, usdt(1)));
    labeler.finish(|label| labels.push(label));
    assert_eq!(labeler.stats().reach_backs, 2);
    assert_eq!(labels, [emitted]);
}

// ------------------------------------------------------------- passages

/// A trigger record at `event` like `template_record`, with `trigger` as
/// its trigger.
fn trigger_record(template_record: &ContextRecord, trigger: TriggerEvent) -> ContextRecord {
    let Sample::Trigger { episode, .. } = &template_record.sample else {
        panic!("a trigger record")
    };
    ContextRecord {
        sample: Sample::Trigger {
            trigger: Box::new(trigger),
            episode: *episode,
        },
        ..template_record.clone()
    }
}

/// An SFP trigger of the golden tape with its level and extreme set.
fn sfp_with(level: Price, extreme: Price) -> TriggerEvent {
    let mut trigger = golden()
        .triggers
        .iter()
        .find_map(|(_, record)| match &record.sample {
            Sample::Trigger { trigger, .. }
                if matches!(trigger.evidence, TriggerEvidence::Sfp(_)) =>
            {
                Some(trigger.as_ref().clone())
            }
            _ => None,
        })
        .unwrap();
    let TriggerEvidence::Sfp(sweep) = &mut trigger.evidence else {
        unreachable!()
    };
    sweep.level.price = level;
    sweep.extreme = extreme;
    trigger
}

/// The standard decision with an SFP trigger record of `level` and
/// `extreme` instead of the baseline.
fn sfp_decision(level: Price, extreme: Price) -> Vec<Step> {
    let event = trade(T0, 10, usdt(60_000));
    let base = record_at(event.canonical_key(), usdt(60_000), Some(usdt(1_000)));
    let (_, golden_trigger) = &golden().triggers[0];
    let mut record = trigger_record(golden_trigger, sfp_with(level, extreme));
    record.event = base.event;
    record.state = base.state;
    vec![see(event), Step::Open(Box::new(record))]
}

#[test]
fn passages_reach_a_price_on_the_side_fixed_at_decision() {
    let mut steps = sfp_decision(usdt(60_200), usdt(59_950));
    steps.extend([
        see(trade(T0 + MINUTE, 11, usdt(60_100))),
        see(trade(T0 + 2 * MINUTE, 12, usdt(59_950))),
        see(trade(T0 + 3 * MINUTE, 13, usdt(60_300))),
        see(tick_of_time(T0 + DAY)),
    ]);
    let label = label_of(steps);
    assert_eq!(
        label.level,
        Some(PassageLabel {
            price: usdt(60_200),
            passage: Passage::Touched {
                at: t(T0 + 3 * MINUTE)
            },
        })
    );
    assert_eq!(
        label.extreme,
        Some(PassageLabel {
            price: usdt(59_950),
            passage: Passage::Touched {
                at: t(T0 + 2 * MINUTE)
            },
        })
    );
    // A price at the reference is reached at the decision; one not reached
    // within 1d is not within; a trade at t + 1d is outside.
    let mut steps = sfp_decision(usdt(60_000), usdt(59_000));
    steps.extend([
        see(trade(T0 + DAY, 11, usdt(58_000))),
        see(tick_of_time(T0 + 2 * DAY)),
    ]);
    let label = label_of(steps);
    assert_eq!(label.level.unwrap().passage, Passage::AtDecision);
    assert_eq!(label.extreme.unwrap().passage, Passage::NotWithin);
    // A late gap from before the decision censors both, the price at the
    // reference included.
    let mut steps = sfp_decision(usdt(60_000), usdt(59_990));
    steps.extend([
        see(trade(T0 + 1_000, 11, usdt(59_000))),
        see(gap(Stream::Trades, T0 - MINUTE, T0 + 2_000)),
        see(tick_of_time(T0 + 2 * DAY)),
    ]);
    let label = label_of(steps);
    let cut = Passage::Censored(censor(T0, CensorReason::Gap));
    assert_eq!(label.level.unwrap().passage, cut);
    assert_eq!(label.extreme.unwrap().passage, cut);
    // A gap after the passage keeps it.
    let mut steps = sfp_decision(usdt(60_000), usdt(59_990));
    steps.extend([
        see(trade(T0 + 1_000, 11, usdt(59_000))),
        see(gap(Stream::Trades, T0 + 2_000, T0 + 3_000)),
        see(tick_of_time(T0 + 2 * DAY)),
    ]);
    let label = label_of(steps);
    assert_eq!(label.level.unwrap().passage, Passage::AtDecision);
    assert_eq!(
        label.extreme.unwrap().passage,
        Passage::Touched { at: t(T0 + 1_000) }
    );
}

/// The trigger events of a replay of `tape`.
fn fired(tape: &[MarketEvent]) -> Vec<TriggerEvent> {
    let mut engine = MarketStateEngine::new();
    let mut triggers = Vec::new();
    for event in tape {
        engine.apply(event).unwrap();
        triggers.extend(engine.trigger_events().iter().cloned());
    }
    triggers
}

#[test]
fn evidence_prices_follow_the_d8_table() {
    let golden_triggers: Vec<TriggerEvent> = golden()
        .triggers
        .iter()
        .filter_map(|(_, record)| match &record.sample {
            Sample::Trigger { trigger, .. } => Some(trigger.as_ref().clone()),
            Sample::Baseline { .. } => None,
        })
        .collect();
    let auction = fired(&crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7));
    let book = fired(&crate::candidate::tests::book_tape());
    let find = |triggers: &[TriggerEvent], pick: &dyn Fn(&TriggerEvent) -> bool| {
        triggers
            .iter()
            .find(|trigger| pick(trigger))
            .cloned()
            .expect("the tape has the evidence")
    };
    let candidate = |trigger: &TriggerEvent| match &trigger.evidence {
        TriggerEvidence::Candidate(fact) => Some(fact.as_ref().clone()),
        _ => None,
    };
    let mut rows: Vec<(&str, TriggerEvent, EvidencePrices)> = Vec::new();
    // Sfp(sweep): sweep.level.price, sweep.extreme.
    let sfp = find(&golden_triggers, &|trigger| {
        matches!(trigger.evidence, TriggerEvidence::Sfp(_))
    });
    let TriggerEvidence::Sfp(sweep) = &sfp.evidence else {
        unreachable!()
    };
    assert_ne!(sweep.level.price, sweep.extreme);
    let sfp_prices = EvidencePrices {
        level: Some(sweep.level.price),
        extreme: Some(sweep.extreme),
    };
    rows.push(("sfp", sfp.clone(), sfp_prices));
    // Auction(status): the value edge on status.side, no extreme.
    for (side, edge) in [
        (
            Region::Above,
            (|status: &AuctionStatus| status.vah) as fn(&AuctionStatus) -> Price,
        ),
        (Region::Below, |status: &AuctionStatus| status.val),
    ] {
        let trigger = find(
            &auction,
            &|trigger| matches!(&trigger.evidence, TriggerEvidence::Auction(status) if status.side == Some(side)),
        );
        let TriggerEvidence::Auction(status) = &trigger.evidence else {
            unreachable!()
        };
        assert_ne!(status.vah, status.val);
        let prices = EvidencePrices {
            level: Some(edge(status)),
            extreme: None,
        };
        rows.push(("auction", trigger, prices));
    }
    for side in [Some(Region::In), None] {
        let mut trigger = find(&auction, &|trigger| {
            matches!(trigger.evidence, TriggerEvidence::Auction(_))
        });
        let TriggerEvidence::Auction(status) = &mut trigger.evidence else {
            unreachable!()
        };
        status.side = side;
        rows.push(("auction inside value", trigger, EvidencePrices::default()));
    }
    // Candidate(CvdDivergence | OiDivergence): comparison.pair.current.price.
    let cvd = find(&golden_triggers, &|trigger| {
        matches!(candidate(trigger), Some(CandidateEvent::CvdDivergence(_)))
    });
    let Some(CandidateEvent::CvdDivergence(event)) = candidate(&cvd) else {
        unreachable!()
    };
    let current = event.comparison.pair.current.price;
    assert_ne!(current, event.comparison.pair.previous.price);
    rows.push((
        "cvd_divergence",
        cvd.clone(),
        EvidencePrices {
            level: Some(current),
            extreme: Some(current),
        },
    ));
    let oi_divergence = DivergenceEvent {
        feature: event.feature,
        timeframe: event.timeframe,
        known_at: event.known_at,
        swings: event.swings,
        cvd: event.cvd,
        comparison: OiComparison {
            pair: event.comparison.pair,
            previous_oi: None,
            current_oi: None,
            oi_change: None,
            divergent: false,
        },
        context: event.context.clone(),
    };
    let mut oi_trigger = cvd.clone();
    oi_trigger.evidence =
        TriggerEvidence::Candidate(Box::new(CandidateEvent::OiDivergence(oi_divergence)));
    rows.push((
        "oi_divergence",
        oi_trigger,
        EvidencePrices {
            level: Some(current),
            extreme: Some(current),
        },
    ));
    // Candidate(Exhaustion): value.extreme only.
    let exhaustion = find(&golden_triggers, &|trigger| {
        matches!(candidate(trigger), Some(CandidateEvent::Exhaustion(_)))
    });
    let Some(CandidateEvent::Exhaustion(event)) = candidate(&exhaustion) else {
        unreachable!()
    };
    rows.push((
        "exhaustion",
        exhaustion,
        EvidencePrices {
            level: None,
            extreme: Some(event.value.extreme),
        },
    ));
    // Candidate(EffortVsResult | Absorption): none.
    let effort = find(&golden_triggers, &|trigger| {
        matches!(candidate(trigger), Some(CandidateEvent::EffortVsResult(_)))
    });
    rows.push(("effort_vs_result", effort, EvidencePrices::default()));
    let absorption = find(&book, &|trigger| {
        matches!(candidate(trigger), Some(CandidateEvent::Absorption(_)))
    });
    rows.push(("absorption", absorption, EvidencePrices::default()));
    // OiExpansion(e): as for e.rejection, both kinds.
    let oi = find(&golden_triggers, &|trigger| {
        matches!(&trigger.evidence, TriggerEvidence::OiExpansion(expansion)
            if matches!(expansion.rejection, OiRejection::Sfp(_)))
    });
    let TriggerEvidence::OiExpansion(expansion) = &oi.evidence else {
        unreachable!()
    };
    let OiRejection::Sfp(sweep) = &expansion.rejection else {
        unreachable!()
    };
    rows.push((
        "oi_expansion via sfp",
        oi.clone(),
        EvidencePrices {
            level: Some(sweep.level.price),
            extreme: Some(sweep.extreme),
        },
    ));
    let failed = find(&auction, &|trigger| {
        trigger.family == TriggerFamily::FailedAuction
            && matches!(&trigger.evidence, TriggerEvidence::Auction(status)
                if matches!(status.side, Some(Region::Above | Region::Below)))
    });
    let TriggerEvidence::Auction(status) = failed.evidence.clone() else {
        unreachable!()
    };
    let edge = if status.side == Some(Region::Above) {
        status.vah
    } else {
        status.val
    };
    let mut via_auction = oi.clone();
    let TriggerEvidence::OiExpansion(expansion) = &mut via_auction.evidence else {
        unreachable!()
    };
    expansion.rejection = OiRejection::FailedAuction(status);
    rows.push((
        "oi_expansion via failed auction",
        via_auction,
        EvidencePrices {
            level: Some(edge),
            extreme: None,
        },
    ));
    // Each row, directly and through a label.
    let (_, golden_record) = &golden().triggers[0];
    for (name, trigger, expected) in rows {
        assert_eq!(evidence_prices(&trigger), expected, "{name}");
        let record = trigger_record(golden_record, trigger);
        let mut labeler = ForwardLabeler::new();
        labeler.open(&record);
        let mut labels = Vec::new();
        labeler.finish(|label| labels.push(label));
        assert_eq!(
            labels[0].level.map(|passage| passage.price),
            expected.level,
            "{name}"
        );
        assert_eq!(
            labels[0].extreme.map(|passage| passage.price),
            expected.extreme,
            "{name}"
        );
    }
}

// ------------------------------------------------------- the golden run

/// What the naive model needs to know of one record.
#[derive(Debug, Clone)]
struct Info {
    /// The index of its event in the tape.
    index: usize,
    /// Its event.
    event: CanonicalKey,
    /// `state.last_price` and `state.atr`.
    reference: Option<Price>,
    atr: Option<Price>,
    /// The evidence prices; none for a baseline.
    prices: EvidencePrices,
    /// The thinning stream: `None` for the baseline, the trigger id slot
    /// otherwise, with the opener flag.
    trigger: Option<(usize, bool)>,
    /// A baseline's `minute_end`.
    minute_end: Option<EventTime>,
}

impl Info {
    fn of(index: usize, record: &ContextRecord) -> Self {
        let (prices, trigger, minute_end) = match &record.sample {
            Sample::Trigger { trigger, episode } => (
                evidence_prices(trigger),
                Some((trigger_position(trigger.feature).unwrap(), episode.opener)),
                None,
            ),
            Sample::Baseline { minute_end } => (EvidencePrices::default(), None, Some(*minute_end)),
        };
        Self {
            index,
            event: record.event,
            reference: record.state.last_price.ready().copied(),
            atr: record.state.atr.ready().copied(),
            prices,
            trigger,
            minute_end,
        }
    }
}

/// One drive of the engine, the sampler and the labeler (the ADR-055 D11
/// order: the labeler observes an event before the sampler's records of it
/// are opened).
struct Drive {
    /// Every record's model input, in record order.
    infos: Vec<Info>,
    /// Every record's event and line fingerprint.
    lines: Vec<(CanonicalKey, Fingerprint)>,
    /// The trigger records, with their event index.
    triggers: Vec<(usize, ContextRecord)>,
    /// Every label, with the index of the event whose observation emitted
    /// it (the tape's length for `finish`).
    labels: Vec<(usize, ForwardLabel)>,
    /// The labeler's counts.
    stats: LabelStats,
}

fn drive(tape: &[MarketEvent]) -> Drive {
    let mut engine = MarketStateEngine::new();
    let mut sampler = ContextSampler::new(None);
    let mut labeler = ForwardLabeler::new();
    let mut run = Drive {
        infos: Vec::new(),
        lines: Vec::new(),
        triggers: Vec::new(),
        labels: Vec::new(),
        stats: LabelStats::default(),
    };
    for (index, event) in tape.iter().enumerate() {
        engine.apply(event).unwrap();
        labeler.observe(event, |label| run.labels.push((index, label)));
        sampler.observe(event, &engine, |record| {
            labeler.open(&record);
            run.infos.push(Info::of(index, &record));
            let mut hasher = Fingerprinter::new();
            hasher.write_str(&record.to_string());
            run.lines.push((record.event, hasher.finish()));
            if matches!(record.sample, Sample::Trigger { .. }) {
                run.triggers.push((index, record));
            }
        });
    }
    labeler.finish(|label| run.labels.push((tape.len(), label)));
    run.stats = labeler.stats();
    run
}

/// One drive over the candidate golden tape (ADR-051), and its label
/// golden: the count, the digest of every line, and the first label of a
/// trigger record with a ready ATR.
struct Golden {
    tape: Vec<MarketEvent>,
    run: Drive,
    triggers: Vec<(usize, ContextRecord)>,
    golden: (usize, String, String),
}

fn golden() -> &'static Golden {
    static RUN: std::sync::OnceLock<Golden> = std::sync::OnceLock::new();
    RUN.get_or_init(|| {
        let tape = crate::candidate::tests::golden_tape();
        let mut run = drive(&tape);
        let triggers = std::mem::take(&mut run.triggers);
        let mut hasher = Fingerprinter::new();
        let mut first_trigger = String::new();
        for (_, label) in &run.labels {
            let line = label.to_string();
            hasher.write_str(&line);
            if first_trigger.is_empty() && label.level.is_some() && label.atr.is_some() {
                first_trigger = line;
            }
        }
        let golden = (run.labels.len(), hasher.finish().to_string(), first_trigger);
        Golden {
            tape,
            run,
            triggers,
            golden,
        }
    })
}

// ----------------------------------------------------- the naive model

/// The D4 offset, written from the ADR text: `b_H · ATR` per 10⁵, half to
/// even.
fn model_offset(atr: Price, position: usize) -> Option<i64> {
    let product = i128::from(atr.units()) * i128::from(BARRIER_MULTIPLE_E5[position]);
    let (quotient, remainder) = (product / 100_000, product % 100_000);
    let up = remainder * 2 > 100_000 || (remainder * 2 == 100_000 && quotient % 2 == 1);
    let offset = i64::try_from(quotient + i128::from(up)).unwrap();
    (offset > 0).then_some(offset)
}

/// The D9 flags of every record, by the greedy rule per stream.
fn model_clear(infos: &[Info]) -> Vec<[bool; 5]> {
    let mut last: Vec<(Option<usize>, [Option<i64>; 5])> = Vec::new();
    infos
        .iter()
        .map(|info| {
            let stream = match info.trigger {
                None => None,
                Some((_, false)) => return [false; 5],
                Some((slot, true)) => Some(slot),
            };
            let position = match last.iter().position(|(key, _)| *key == stream) {
                Some(position) => position,
                None => {
                    last.push((stream, [None; 5]));
                    last.len() - 1
                }
            };
            let time = info.event.time.as_millis();
            std::array::from_fn(|horizon| {
                let slot = &mut last[position].1[horizon];
                let clear = slot.is_none_or(|at| time >= at + HORIZONS[horizon].millis());
                if clear {
                    *slot = Some(time);
                }
                clear
            })
        })
        .collect()
}

/// The censor of each horizon of the label of `info` and the index of the
/// event after which all its windows are closed (`tape.len()`: only by
/// `finish`), written from ADR-055 D6. A window closes at the first event at
/// or after its end, or when a censor reaches it. Every trades gap delivered
/// before the label is emitted, at the index `emitted` (`None`: at its own
/// completion), censors every horizon whose window ends after its c, closed
/// windows included, keeping the earliest censor.
fn model_censors(
    tape: &[MarketEvent],
    info: &Info,
    emitted: Option<usize>,
) -> ([Option<Censor>; 5], usize) {
    let decision = info.event.time.as_millis();
    let ends: [i64; 5] = std::array::from_fn(|position| decision + HORIZONS[position].millis());
    let mut stale = info.reference.is_none();
    for event in tape[..=info.index].iter().rev() {
        match event {
            MarketEvent::Trade(_) => break,
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                stale = true;
                break;
            }
            _ => {}
        }
    }
    if stale {
        let cut = censor(decision, CensorReason::StaleReference);
        return ([Some(cut); 5], info.index);
    }
    let mut censors: [Option<Censor>; 5] = [None; 5];
    let mut closed = [false; 5];
    let mut complete = None;
    for (index, event) in tape.iter().enumerate().skip(info.index + 1) {
        if complete.is_some() && emitted.is_none_or(|emitted| index > emitted) {
            break;
        }
        if let MarketEvent::FeedGap(gap) = event
            && gap.stream == Stream::Trades
        {
            let at = gap.start.as_millis().max(decision);
            for position in 0..5 {
                let earlier = censors[position].is_some_and(|cut| cut.at.as_millis() <= at);
                if at < ends[position] && !earlier {
                    censors[position] = Some(censor(at, CensorReason::Gap));
                    closed[position] = true;
                }
            }
        }
        for position in 0..5 {
            closed[position] |= event.time().as_millis() >= ends[position];
        }
        if complete.is_none() && closed.iter().all(|closed| *closed) {
            complete = Some(index);
        }
    }
    let Some(complete) = complete else {
        // `finish` censors every window still open at the last event.
        let last_time = tape.last().unwrap().time().as_millis();
        for position in 0..5 {
            if !closed[position] {
                censors[position] = Some(censor(last_time.max(decision), CensorReason::EndOfData));
            }
        }
        return (censors, tape.len());
    };
    (censors, complete)
}

/// The label of `info`, emitted after the event at `emitted`, by scanning
/// the tape after its event: written from the ADR-055 text, independent of
/// the labeler's data structures.
fn model_label(
    tape: &[MarketEvent],
    info: &Info,
    ordinal: u32,
    clear: [bool; 5],
    emitted: usize,
) -> ForwardLabel {
    let decision = info.event.time.as_millis();
    let (censors, _) = model_censors(tape, info, Some(emitted));
    // The considered trades and the censor of the horizon at `position`.
    let window = |position: usize| -> (Vec<Trade>, Option<Censor>) {
        let end = decision + HORIZONS[position].millis();
        let cut = censors[position];
        let until = cut.map_or(end, |cut| cut.at.as_millis().min(end));
        let trades = tape[info.index + 1..]
            .iter()
            .filter_map(|event| match event {
                MarketEvent::Trade(trade) if trade.time.as_millis() < until => Some(*trade),
                _ => None,
            })
            .collect();
        (trades, cut)
    };
    let mut passage_window = (Vec::new(), None);
    let horizons = std::array::from_fn(|position| {
        let (trades, cut) = window(position);
        let barriers = info.reference.and_then(|reference| {
            let offset = model_offset(info.atr?, position)?;
            Some(Barriers {
                lower: Price::from_units(reference.units() - offset),
                upper: Price::from_units(reference.units() + offset),
            })
        });
        let race = match barriers {
            None => Race::Unavailable,
            Some(barriers) => {
                let touches_upper = |trade: &Trade| trade.price >= barriers.upper;
                let touches_lower = |trade: &Trade| trade.price <= barriers.lower;
                match trades
                    .iter()
                    .position(|trade| touches_upper(trade) || touches_lower(trade))
                {
                    None => cut.map_or(Race::Neither, Race::Censored),
                    Some(first) => {
                        let winner = trades[first];
                        let upper = touches_upper(&winner);
                        let same_ms = trades[first + 1..].iter().any(|other| {
                            other.time == winner.time
                                && if upper {
                                    touches_lower(other)
                                } else {
                                    touches_upper(other)
                                }
                        });
                        if upper {
                            Race::Upper {
                                at: winner.time,
                                same_ms,
                            }
                        } else {
                            Race::Lower {
                                at: winner.time,
                                same_ms,
                            }
                        }
                    }
                }
            }
        };
        let (mut up, mut down) = (Excursion::NONE, Excursion::NONE);
        if let Some(reference) = info.reference {
            let mut high: Option<&Trade> = None;
            let mut low: Option<&Trade> = None;
            for trade in &trades {
                if high.is_none_or(|high| trade.price > high.price) {
                    high = Some(trade);
                }
                if low.is_none_or(|low| trade.price < low.price) {
                    low = Some(trade);
                }
            }
            if let Some(high) = high.filter(|high| high.price > reference) {
                up = Excursion {
                    size: Price::from_units(high.price.units() - reference.units()),
                    at: Some(high.time),
                };
            }
            if let Some(low) = low.filter(|low| low.price < reference) {
                down = Excursion {
                    size: Price::from_units(reference.units() - low.price.units()),
                    at: Some(low.time),
                };
            }
        }
        let horizon = HorizonLabel {
            horizon: HORIZONS[position],
            clear: clear[position],
            barriers,
            race,
            up,
            down,
            last: trades.last().map(|trade| trade.price),
            censored: cut,
        };
        if position == 4 {
            passage_window = (trades, cut);
        }
        horizon
    });
    let (trades, cut) = passage_window;
    let passage = |price: Option<Price>| {
        price.map(|price| {
            let passage = if Some(price) == info.reference {
                match cut {
                    Some(cut) if cut.at.as_millis() <= decision => Passage::Censored(cut),
                    _ => Passage::AtDecision,
                }
            } else {
                let above = info.reference.is_some_and(|reference| price > reference);
                trades
                    .iter()
                    .find(|trade| {
                        if above {
                            trade.price >= price
                        } else {
                            trade.price <= price
                        }
                    })
                    .map_or(cut.map_or(Passage::NotWithin, Passage::Censored), |trade| {
                        Passage::Touched { at: trade.time }
                    })
            };
            PassageLabel { price, passage }
        })
    };
    ForwardLabel {
        event: info.event,
        ordinal,
        reference: info.reference,
        atr: info.atr,
        horizons,
        level: passage(info.prices.level),
        extreme: passage(info.prices.extreme),
    }
}

/// Checks the labels of `run` over `tape` against the model, for the
/// records `pick` selects; returns how many were checked.
fn check_model(tape: &[MarketEvent], run: &Drive, pick: impl Fn(&Info) -> bool) -> usize {
    assert_eq!(run.labels.len(), run.infos.len(), "one label per record");
    assert_eq!(run.stats.opened, run.infos.len() as u64);
    assert_eq!(run.stats.emitted, run.infos.len() as u64);
    let clear = model_clear(&run.infos);
    // A label is emitted in record order, once every earlier one is.
    let mut emitted = 0;
    let emitted: Vec<usize> = run
        .infos
        .iter()
        .map(|info| {
            emitted = emitted.max(model_censors(tape, info, None).1);
            emitted
        })
        .collect();
    let mut checked = 0;
    let mut ordinal = 0;
    for (position, (info, (_, label))) in run.infos.iter().zip(&run.labels).enumerate() {
        ordinal = if position > 0 && run.infos[position - 1].event == info.event {
            ordinal + 1
        } else {
            0
        };
        assert_eq!(
            (label.event, label.ordinal),
            (info.event, ordinal),
            "record order"
        );
        if pick(info) {
            let expected = model_label(tape, info, ordinal, clear[position], emitted[position]);
            assert_eq!(*label, expected, "\n{label}\n{expected}");
            checked += 1;
        }
    }
    checked
}

/// Counts of label outcomes over `labels`, so the model checks are not
/// vacuous: upper, lower, neither, censored, unavailable, same_ms, gap
/// censors, touched passages.
fn outcomes<'a>(labels: impl Iterator<Item = &'a ForwardLabel>) -> [usize; 8] {
    let mut counts = [0; 8];
    for label in labels {
        for horizon in &label.horizons {
            match horizon.race {
                Race::Upper { same_ms, .. } | Race::Lower { same_ms, .. } => {
                    counts[usize::from(matches!(horizon.race, Race::Lower { .. }))] += 1;
                    counts[5] += usize::from(same_ms);
                }
                Race::Neither => counts[2] += 1,
                Race::Censored(_) => counts[3] += 1,
                Race::Unavailable => counts[4] += 1,
            }
            counts[6] += usize::from(
                horizon
                    .censored
                    .is_some_and(|censor| censor.reason == CensorReason::Gap),
            );
        }
        for passage in [label.level, label.extreme].into_iter().flatten() {
            counts[7] += usize::from(matches!(passage.passage, Passage::Touched { .. }));
        }
    }
    counts
}

/// A random walk of `days` days: bursts of up to four trades in one
/// millisecond, trade ids with holes, trades gaps (some starting before the
/// last trade, as a late detection does) with open-interest events inside
/// them, delivered before the gap is announced (ADR-028), and gaps of other
/// streams.
fn walk(seed: u64, days: i64) -> Vec<MarketEvent> {
    let mut lcg = Lcg(seed);
    let mut events = Vec::new();
    let mut time = 13 * HOUR;
    let mut hundredths: i64 = 6_000_000;
    let mut trade_id = 0;
    while time < days * DAY {
        time += 1 + lcg.below(40_000);
        match lcg.below(1_000) {
            0..=2 => {
                let start = time - lcg.below(600_000);
                let end = time + lcg.below(300_000);
                // Other streams go on during the outage and close windows
                // that end inside it.
                let mut inside = time;
                for _ in 0..lcg.below(4) {
                    inside += 1 + lcg.below(120_000);
                    if inside >= end {
                        break;
                    }
                    events.push(tick_of_time(inside));
                }
                events.push(gap(Stream::Trades, start, end));
                time = end;
            }
            3..=5 => events.push(gap(Stream::OrderBook, time - 1_000, time)),
            _ => {
                let burst = if lcg.below(8) == 0 {
                    2 + lcg.below(3)
                } else {
                    1
                };
                for _ in 0..burst {
                    hundredths += lcg.below(2_001) - 1_000;
                    trade_id += 1 + lcg.below(3) as u64;
                    events.push(trade(time, trade_id, cents(hundredths)));
                }
            }
        }
    }
    events
}

#[test]
fn labels_match_a_naive_model_on_random_tapes() {
    let mut totals = [0; 8];
    let mut checked = 0;
    for seed in [1, 2, 3] {
        let tape = random_tape(0x6d69_6500_0026_0000 + seed, 9_000);
        let run = drive(&tape);
        checked += check_model(&tape, &run, |_| true);
        let counts = outcomes(run.labels.iter().map(|(_, label)| label));
        for (total, count) in totals.iter_mut().zip(counts) {
            *total += count;
        }
    }
    // A same-millisecond tie on both barriers is rare on the walk (about one
    // tape in five); seed 10 holds two.
    for seed in [4, 5, 10] {
        let tape = walk(0x6d69_6500_0026_0000 + seed, 4);
        let run = drive(&tape);
        checked += check_model(&tape, &run, |_| true);
        let counts = outcomes(run.labels.iter().map(|(_, label)| label));
        for (total, count) in totals.iter_mut().zip(counts) {
            *total += count;
        }
    }
    assert!(checked > 10_000, "{checked}");
    // Every outcome occurs; same_ms ties included.
    assert!(totals.iter().all(|count| *count > 0), "{totals:?}");
}

#[test]
fn labels_match_a_naive_model_on_the_golden_tape() {
    // Every trigger record, and the baselines on a whole UTC hour (the M3
    // subset, chosen by decision time only).
    let golden = golden();
    let checked = check_model(&golden.tape, &golden.run, |info| {
        info.trigger.is_some()
            || info
                .minute_end
                .is_some_and(|end| end.as_millis() % HOUR == 0)
    });
    assert!(checked > 2_000, "{checked}");
    let counts = outcomes(golden.run.labels.iter().map(|(_, label)| label));
    // The golden tape has one trades gap and no same-millisecond trades.
    assert!(counts[..5].iter().all(|count| *count > 0), "{counts:?}");
    assert!(counts[6] > 0 && counts[7] > 0, "{counts:?}");
}

// ------------------------------------------------------------- thinning

#[test]
fn clear_records_have_disjoint_windows() {
    let golden = golden();
    let labels: Vec<&ForwardLabel> = golden.run.labels.iter().map(|(_, label)| label).collect();
    let mut thinned_openers = 0;
    for (position, horizon) in HORIZONS.iter().enumerate() {
        // Per stream, the decision times of the clear records.
        let mut streams: Vec<(Option<usize>, Vec<i64>)> = Vec::new();
        for (info, label) in golden.run.infos.iter().zip(&labels) {
            let clear = label.horizons[position].clear;
            match info.trigger {
                Some((_, false)) => {
                    assert!(!clear, "a non-opener is never clear");
                    continue;
                }
                Some((_, true)) if !clear => thinned_openers += 1,
                _ => {}
            }
            let stream = info.trigger.map(|(slot, _)| slot);
            let entry = match streams.iter().position(|(key, _)| *key == stream) {
                Some(entry) => entry,
                None => {
                    streams.push((stream, Vec::new()));
                    streams.len() - 1
                }
            };
            let times = &mut streams[entry].1;
            let time = info.event.time.as_millis();
            // Greedy: clear exactly when the last clear window has ended.
            let expected = times
                .last()
                .is_none_or(|last| time >= last + horizon.millis());
            assert_eq!(clear, expected, "{horizon} {label}");
            if clear {
                times.push(time);
            }
        }
        let baseline = &streams.iter().find(|(key, _)| key.is_none()).unwrap().1;
        assert!(baseline.len() > 30, "{horizon}");
        for (_, times) in &streams {
            assert!(
                times
                    .windows(2)
                    .all(|pair| pair[1] - pair[0] >= horizon.millis())
            );
        }
    }
    // Some openers fall inside the window of their id's last clear one.
    assert!(thinned_openers > 0);
}

#[test]
fn thinning_is_greedy_from_the_window_end_on() {
    // Baselines at 0, 4, 5, 9 and 10 minutes: clear at 5m are those at 0,
    // 5 and 10 (a window [t, t + 5m) ends where the next may start); at
    // 15m only the first.
    let mut steps = Vec::new();
    for (index, minutes) in [0, 4, 5, 9, 10].into_iter().enumerate() {
        let event = trade(T0 + minutes * MINUTE, 10 + index as u64, usdt(60_000));
        steps.push(see(event.clone()));
        steps.push(Step::Open(Box::new(record_at(
            event.canonical_key(),
            usdt(60_000),
            Some(usdt(1_000)),
        ))));
    }
    // Trigger records of one id: an opener, a later non-opener, and an
    // opener 5 minutes after the first.
    let (_, golden_trigger) = &golden().triggers[0];
    let trigger_at = |minutes: i64, id: u64, opener: bool| {
        let event = trade(T0 + DAY + minutes * MINUTE, id, usdt(60_000));
        let mut record = golden_trigger.clone();
        record.event = event.canonical_key();
        let Sample::Trigger { episode, .. } = &mut record.sample else {
            unreachable!()
        };
        episode.opener = opener;
        [see(event), Step::Open(Box::new(record))]
    };
    steps.extend(trigger_at(0, 20, true));
    steps.extend(trigger_at(1, 21, false));
    steps.extend(trigger_at(5, 22, true));
    steps.push(see(tick_of_time(T0 + 3 * DAY)));
    let (labels, _) = run(steps);
    let flags: Vec<[bool; 2]> = labels
        .iter()
        .map(|label| [label.horizons[0].clear, label.horizons[1].clear])
        .collect();
    assert_eq!(
        flags,
        [
            [true, true],
            [false, false],
            [true, false],
            [false, false],
            [true, false],
            [true, true],
            [false, false],
            [true, false],
        ]
    );
}

// -------------------------------------------------------------- leakage

/// `tape` with every trade id doubled, so a trade can be inserted after
/// any trade in its millisecond.
fn spaced(tape: &[MarketEvent]) -> Vec<MarketEvent> {
    tape.iter()
        .map(|event| match event {
            MarketEvent::Trade(trade) => MarketEvent::Trade(Trade {
                trade_id: trade.trade_id * 2,
                ..*trade
            }),
            other => other.clone(),
        })
        .collect()
}

/// `tape` with `event` inserted at its canonical position.
fn inserted(tape: &[MarketEvent], event: MarketEvent) -> Vec<MarketEvent> {
    let mut tape = tape.to_vec();
    let at = tape.partition_point(|other| *other < event);
    tape.insert(at, event);
    tape
}

/// The leakage property (#26 acceptance): mutating only events after a
/// decision event changes labels, never the records up to it or their
/// clear flags, and leaves every label emitted before the first mutated
/// event as it was.
fn assert_no_leakage(tape: &[MarketEvent], cuts: &[i64]) -> usize {
    let base = drive(tape);
    let mut mutations = 0;
    for &cut in cuts {
        // The decision: the first trade at or after `cut` with a record
        // whose 5m window is not censored, so every mutation can show.
        let (index, decision) = base
            .infos
            .iter()
            .zip(&base.labels)
            .find(|(info, (_, label))| {
                info.event.time.as_millis() >= cut
                    && matches!(tape[info.index], MarketEvent::Trade(_))
                    && label.horizons[0].censored.is_none()
            })
            .map(|(info, _)| (info.index, info.event))
            .unwrap();
        let MarketEvent::Trade(at) = tape[index] else {
            unreachable!()
        };
        let reference = at.price.units();
        let decided = |key: CanonicalKey| key <= decision;
        let shifted: Vec<MarketEvent> = tape
            .iter()
            .enumerate()
            .map(|(position, event)| match event {
                MarketEvent::Trade(trade) if position > index => MarketEvent::Trade(Trade {
                    price: Price::from_units(trade.price.units() + 1_000 * SCALE),
                    ..*trade
                }),
                other => other.clone(),
            })
            .collect();
        let time = at.time.as_millis();
        let variants: Vec<(&str, Vec<MarketEvent>)> = vec![
            ("truncated", tape[..=index].to_vec()),
            ("shifted", shifted),
            (
                "gap after",
                inserted(tape, gap(Stream::Trades, time + 1, time + 10 * MINUTE)),
            ),
            (
                "late gap",
                inserted(tape, gap(Stream::Trades, time - 30 * MINUTE, time + 1)),
            ),
            (
                "same ms",
                inserted(
                    tape,
                    trade(
                        time,
                        at.trade_id + 1,
                        Price::from_units(reference + 1_000 * SCALE),
                    ),
                ),
            ),
        ];
        for (name, variant) in variants {
            let mutated = drive(&variant);
            // The records up to the decision are identical.
            let before = |run: &Drive| -> Vec<(CanonicalKey, Fingerprint)> {
                run.lines
                    .iter()
                    .copied()
                    .filter(|(key, _)| decided(*key))
                    .collect()
            };
            let records = before(&base);
            assert!(records.len() > 1_000, "{name} {}", records.len());
            assert_eq!(before(&mutated), records, "{name}");
            // Their labels: the clear flags are identical, and at least one
            // label changed.
            let labels = |run: &Drive| -> Vec<ForwardLabel> {
                run.labels
                    .iter()
                    .filter(|(_, label)| decided(label.event))
                    .map(|(_, label)| label.clone())
                    .collect()
            };
            let (old, new) = (labels(&base), labels(&mutated));
            assert_eq!(old.len(), new.len(), "{name}");
            let flags = |labels: &[ForwardLabel]| -> Vec<[bool; 5]> {
                labels
                    .iter()
                    .map(|label| label.horizons.map(|horizon| horizon.clear))
                    .collect()
            };
            assert_eq!(flags(&old), flags(&new), "{name}");
            assert!(
                old != new,
                "{name}: no label changed; last {}",
                old.last().unwrap()
            );
            // Labels emitted before the first mutated event are identical.
            let early: Vec<&ForwardLabel> = base
                .labels
                .iter()
                .filter(|(emitted, _)| *emitted <= index)
                .map(|(_, label)| label)
                .collect();
            assert!(!early.is_empty(), "{name}");
            for (position, label) in early.into_iter().enumerate() {
                assert_eq!(&mutated.labels[position].1, label, "{name}");
            }
            mutations += 1;
        }
    }
    mutations
}

#[test]
fn labels_never_leak_into_records_on_the_golden_tape() {
    // Day 3, and 30 minutes before the tape's trades gap on day 9.
    let tape: Vec<MarketEvent> = spaced(&crate::candidate::tests::golden_tape())
        .into_iter()
        .filter(|event| event.time().as_millis() < 11 * DAY)
        .collect();
    let mutations = assert_no_leakage(
        &tape,
        &[3 * DAY + 7 * HOUR, 9 * DAY + 9 * HOUR + 30 * MINUTE],
    );
    assert_eq!(mutations, 10);
}

#[test]
fn labels_never_leak_into_records_on_random_tapes() {
    for seed in [6, 7] {
        let tape = spaced(&walk(0x6d69_6500_0026_0000 + seed, 4));
        assert_eq!(assert_no_leakage(&tape, &[2 * DAY + 3 * HOUR]), 5);
    }
}

// --------------------------------------------------- goldens and naming

#[test]
fn barrier_multiples_are_the_derived_values() {
    for (horizon, multiple) in HORIZONS.iter().zip(BARRIER_MULTIPLE_E5) {
        let hours = horizon.millis() as f64 / HOUR as f64;
        let derived = (std::f64::consts::PI * hours / 32.0).sqrt();
        assert_eq!(multiple, (derived * 1e5).round() as i64, "{horizon}");
    }
    // The null share of races open at H with mean exit time H / 4:
    // (4 / π) · e^(−π² / 2), about 0.92 %.
    let open = 4.0 / std::f64::consts::PI * (-std::f64::consts::PI.powi(2) / 2.0).exp();
    assert!((open - 0.0092).abs() < 0.00005, "{open}");
}

#[test]
fn labeling_fingerprint_golden() {
    let text = labeling_text();
    assert!(text.starts_with(
        "mie-labeling 1\nlabeling conditional.labels@1\nreference trade.last_price@1 path=trades window=after_decision_key_before_decision_time_plus_h\nhorizons 5m,15m,1h,4h,1d\nbarriers atr=volatility.atr.1h@1 multiple_e5=9045,15666,31333,62666,153499 scale=100000 "
    ));
    assert_eq!(text.lines().count(), 16);
    assert!(text.ends_with('\n'));
    assert_eq!(
        labeling_fingerprint().to_string(),
        GOLDEN_LABELING_FINGERPRINT
    );
}

#[test]
fn provenance_names_the_records_and_the_labeling() {
    let engine = MarketStateEngine::new();
    let data = crate::research::DataVersion::from_hex(&"0f".repeat(32)).unwrap();
    let context = ContextProvenance::new(data, &engine);
    let provenance = LabelProvenance::new(context.clone());
    assert_eq!(provenance.context, context);
    assert_eq!(provenance.labeling, LABELING_V1);
    assert_eq!(provenance.labeling_fingerprint, labeling_fingerprint());
    assert_eq!(
        provenance.to_string(),
        format!(
            "{context} labeling=conditional.labels@1 labeling_fingerprint={GOLDEN_LABELING_FINGERPRINT}"
        )
    );
}

#[test]
fn no_label_text_names_a_direction_or_an_execution() {
    let golden = golden();
    let mut lines: Vec<String> = golden
        .run
        .labels
        .iter()
        .map(|(_, label)| label.to_string())
        .collect();
    lines.push(labeling_text());
    lines.push(
        [
            CensorReason::Gap,
            CensorReason::StaleReference,
            CensorReason::Rejected,
            CensorReason::EndOfData,
        ]
        .map(|reason| reason.to_string())
        .join(" "),
    );
    for line in &lines {
        assert_eq!(forbidden_in(line), None, "{line}");
    }
}

#[test]
fn golden_forward_labels() {
    let (count, digest, first) = &golden().golden;
    assert_eq!(first, GOLDEN_FIRST_TRIGGER_LABEL);
    assert_eq!((*count, digest.as_str()), GOLDEN_LABELS);
}

#[test]
#[ignore = "prints the goldens: cargo test -p mie-domain print_label_golden -- --ignored --nocapture"]
fn print_label_golden() {
    let golden = golden();
    println!("labeling {}", labeling_fingerprint());
    println!("labels {} {}", golden.golden.0, golden.golden.1);
    println!("first {}", golden.golden.2);
    println!("stats {:?}", golden.run.stats);
}

const GOLDEN_LABELING_FINGERPRINT: &str = "6e042252f905ac1c";
const GOLDEN_LABELS: (usize, &str) = (46_403, "8d7c4cebfc62d7da");
const GOLDEN_FIRST_TRIGGER_LABEL: &str = "117032157ms Trade seq 785 #1 ref=61922.67000000 atr=26.01616928 [5m clear barriers=61920.31683749..61925.02316251 race=lower@117098590ms up=0.64000000@117309050ms down=4.22000000@117248836ms last=61923.31000000] [15m clear barriers=61918.59430692..61926.74569308 race=lower@117248836ms up=0.64000000@117309050ms down=11.49000000@117700476ms last=61915.79000000] [1h clear barriers=61914.51835368..61930.82164632 race=lower@117700476ms up=7.40000000@120543138ms down=11.49000000@117700476ms last=61928.16000000] [4h clear barriers=61906.36670736..61938.97329264 race=lower@122609041ms up=23.70000000@129865828ms down=33.07000000@126265310ms last=61935.31000000] [1d barriers=61882.73544032..61962.60455968 race=upper@146208863ms up=134.65000000@185552262ms down=33.07000000@126265310ms last=62018.89000000] level=61910.65000000:touched@121677419ms extreme=61908.96000000:touched@121677419ms";
