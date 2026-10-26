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
use crate::state::{MarketStateEngine, StateError};
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

/// A trade at the 1d end of a decision at `T0`, at its reference: outside
/// every window, and the trades-stream event that shows them complete, so
/// the label is emitted before `finish` (ADR-055 D6).
fn day_end() -> MarketEvent {
    trade(T0 + DAY, 99, usdt(60_000))
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

/// Runs `steps` through a fresh v1 labeler and finishes it.
fn run(steps: Vec<Step>) -> (Vec<ForwardLabel>, LabelStats) {
    run_with(Labeling::V1, steps)
}

/// Runs `steps` through a fresh labeler of `labeling` and finishes it.
fn run_with(labeling: Labeling, steps: Vec<Step>) -> (Vec<ForwardLabel>, LabelStats) {
    let mut labeler = ForwardLabeler::new(labeling);
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
    let label = label_of(after_decision([see(day_end())]));
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
                barrier_offset(Price::from_units(atr), multiple).map(Price::units),
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
                barrier_offset(Price::from_units(below), multiple).map(Price::units),
                Some(below * multiple / MULTIPLE_SCALE).filter(|units| *units > 0)
            );
        }
    }
    assert_eq!(
        barrier_offset(Price::from_units(0), BARRIER_MULTIPLE_E5[4]),
        None
    );
    assert_eq!(
        barrier_offset(Price::from_units(-5), BARRIER_MULTIPLE_E5[4]),
        None
    );
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
    let label = label_of(vec![see(event), Step::Open(Box::new(tiny)), see(day_end())]);
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
        see(day_end()),
    ]));
    let [m5, m15, ..] = label.horizons;
    assert_eq!(m5.race, Race::Neither);
    assert_eq!((m5.up, m5.last), (Excursion::NONE, None));
    assert_eq!(m15.race, Race::Neither);
    assert_eq!(m15.up, excursion(usdt(100), T0 + 5 * MINUTE));
    // One millisecond earlier it is inside.
    let label = label_of(after_decision([
        see(trade(T0 + 5 * MINUTE - 1, 11, usdt(60_100))),
        see(day_end()),
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
            see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
    ]));
    assert_eq!(label.horizons[0].race, Race::Neither);
    // Exactly at the lower barrier.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, cents(5_990_955))),
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
        see(day_end()),
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
fn a_rejected_trades_event_censors_from_the_last_accepted_one() {
    // ADR-055 D6: a rejection never extends the coverage the accepted
    // trades show, but may shorten it. Dated after the last accepted trade
    // it censors there, wherever its own time lies; dated before it, at its
    // own time, since the refused trade may have been in the path. A trade
    // at the censor's millisecond is not read: it may be incomplete.
    for (rejected_at, at) in [
        (T0 + 500, T0 + 500),
        (T0 + 2_000, T0 + 1_200),
        (T0 + 40 * DAY, T0 + 1_200),
    ] {
        let label = label_of(after_decision([
            see(trade(T0 + 1_000, 11, usdt(60_050))),
            see(trade(T0 + 1_200, 12, usdt(60_000))),
            // Other streams are ignored.
            Step::Reject(tick_of_time(T0 + 1_500)),
            Step::Reject(trade(rejected_at, 13, usdt(61_000))),
            see(trade(T0 + 3_000, 14, usdt(61_000))),
            see(day_end()),
        ]));
        let cut = censor(at, CensorReason::Rejected);
        let read = at > T0 + 1_000;
        for horizon in label.horizons {
            assert_eq!(
                (horizon.race, horizon.censored),
                (Race::Censored(cut), Some(cut))
            );
            if read {
                assert_eq!(horizon.up, excursion(usdt(50), T0 + 1_000));
                assert_eq!(horizon.last, Some(usdt(60_050)));
            } else {
                assert_eq!((horizon.up, horizon.last), (Excursion::NONE, None));
            }
        }
    }
    // Right after the decision trade: censored at the decision.
    let label = label_of(after_decision([
        Step::Reject(trade(T0 + 2_000, 11, usdt(61_000))),
        see(day_end()),
    ]));
    let cut = censor(T0, CensorReason::Rejected);
    assert!(
        label
            .horizons
            .iter()
            .all(|horizon| horizon.race == Race::Censored(cut))
    );
    // Nor does its time count for emission: a rejected trade dated two
    // days ahead, then a record whose windows an open-interest event closes
    // inside a trades outage. The record waits for the trades stream, so
    // the gap announced when trades resume still censors it.
    let later = trade(T0 + 2_000, 12, usdt(60_000));
    let (labels, stats) = run(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        Step::Reject(trade(T0 + 2 * DAY, 13, usdt(60_050))),
        see(later.clone()),
        Step::Open(Box::new(record_at(
            later.canonical_key(),
            usdt(60_000),
            Some(usdt(1_000)),
        ))),
        see(tick_of_time(T0 + DAY + 3_000)),
        see(gap(Stream::Trades, T0 + 3_000, T0 + DAY + 5_000)),
    ]));
    assert_eq!(stats.reach_backs, 0);
    let first = censor(T0 + 1_000, CensorReason::Rejected);
    assert!(labels[0].horizons.iter().all(|h| h.censored == Some(first)));
    let cut = censor(T0 + 3_000, CensorReason::Gap);
    assert!(
        labels[1]
            .horizons
            .iter()
            .all(|h| h.race == Race::Censored(cut))
    );
    // A rejection only: nothing was censored.
    let label = label_of(after_decision([
        Step::Reject(tick_of_time(T0 + 1_500)),
        see(day_end()),
    ]));
    assert!(
        label
            .horizons
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
}

/// `label` with every censor of reason `from` renamed `to`.
fn renamed(label: &ForwardLabel, from: CensorReason, to: CensorReason) -> ForwardLabel {
    let swap = |cut: Censor| {
        if cut.reason == from {
            Censor { reason: to, ..cut }
        } else {
            cut
        }
    };
    let mut label = label.clone();
    for horizon in &mut label.horizons {
        horizon.censored = horizon.censored.map(swap);
        if let Race::Censored(cut) = horizon.race {
            horizon.race = Race::Censored(swap(cut));
        }
    }
    for passage in [&mut label.level, &mut label.extreme].into_iter().flatten() {
        if let Passage::Censored(cut) = passage.passage {
            passage.passage = Passage::Censored(swap(cut));
        }
    }
    label
}

#[test]
fn a_rejected_trade_as_the_final_event_loses_no_label() {
    // Re-review regressions (PR #148). The data ends on a trade the engine
    // rejects: at the 5m end, just after it, or 32 days ahead (a
    // `TimeJump`, after which the replay stops). Its time is not trusted,
    // so the label comes out once, censored at the last accepted trade,
    // with the reason `Rejected`, never covered up to the rejected time.
    let base = || {
        after_decision([
            see(trade(T0 + 1_000, 11, usdt(60_050))),
            see(trade(T0 + 5 * MINUTE - 10, 12, usdt(60_000))),
        ])
    };
    let cut = censor(T0 + 5 * MINUTE - 10, CensorReason::Rejected);
    for rejected_at in [T0 + 5 * MINUTE, T0 + 5 * MINUTE + 5, T0 + 32 * DAY] {
        let mut steps = base();
        steps.push(Step::Reject(trade(rejected_at, 7, usdt(60_050))));
        let label = label_of(steps);
        for horizon in &label.horizons {
            assert_eq!(
                (horizon.race, horizon.censored),
                (Race::Censored(cut), Some(cut))
            );
            assert_eq!(horizon.up, excursion(usdt(50), T0 + 1_000));
            assert_eq!(horizon.last, Some(usdt(60_050)));
        }
        // The data ending at the last accepted trade gives the same label,
        // named `EndOfData`.
        assert_eq!(
            label,
            renamed(
                &label_of(base()),
                CensorReason::EndOfData,
                CensorReason::Rejected
            )
        );
    }
    // A time jump with three labels open: they come out in record order,
    // as the data ending at the last accepted trade gives them. The first
    // record's 5m … 4h windows closed before; its 1d window and every
    // window of the last record are censored at that trade.
    let steps = |jump: bool| {
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
        if jump {
            steps.push(Step::Reject(trade(T0 + 32 * DAY, 21, usdt(60_010))));
        }
        steps
    };
    let (labels, stats) = run(steps(true));
    let (plain, _) = run(steps(false));
    assert_eq!((stats.opened, stats.emitted), (3, 3));
    let expected: Vec<ForwardLabel> = plain
        .iter()
        .map(|label| renamed(label, CensorReason::EndOfData, CensorReason::Rejected))
        .collect();
    assert_eq!(labels, expected);
    let cut = censor(T0 + 20 * HOUR + 1_000, CensorReason::Rejected);
    assert!(
        labels[0].horizons[..4]
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
    assert_eq!(labels[0].horizons[4].race, Race::Censored(cut));
    assert!(
        labels[2]
            .horizons
            .iter()
            .all(|horizon| horizon.race == Race::Censored(cut))
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
    let cut = censor(T0 + 1_000, CensorReason::Rejected);
    assert_eq!(label.horizons[0].race, Race::Censored(cut));
}

#[test]
fn the_earliest_censor_of_an_unemitted_label_wins() {
    // A rejected trade censors the 1h … 1d windows at the last accepted
    // trade, T0+1h; a later gap that started earlier (a live hold-back gap
    // can) cuts every window again before the label is emitted, the closed
    // 5m and 15m ones included, while a later censor changes nothing.
    let label = label_of(after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(trade(T0 + HOUR, 12, usdt(60_000))),
        Step::Reject(trade(T0 + HOUR + 500, 5, usdt(1))),
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
    let cut = censor(T0 + 1_000, CensorReason::Rejected);
    for horizon in label.horizons {
        assert_eq!(horizon.censored, Some(cut), "{horizon}");
    }
}

#[test]
fn a_short_outage_over_the_1d_end_censors_the_label() {
    // Re-review regression (PR #148): an open-interest event at T0+1d closes
    // the last (1d) window inside a ten-minute trades outage, announced
    // when trades resume (ADR-028). The label waits for a trades-stream
    // event at or after its 1d end, so the gap still censors the 1d window
    // and the passages, exactly as on the tape without the other stream's
    // event.
    let outage = |other: bool, mut steps: Vec<Step>| {
        steps.push(see(trade(T0 + 1_000, 11, usdt(60_050))));
        if other {
            steps.push(see(tick_of_time(T0 + DAY)));
        }
        steps.extend([
            see(gap(
                Stream::Trades,
                T0 + DAY - 5 * MINUTE,
                T0 + DAY + 5 * MINUTE,
            )),
            see(trade(T0 + DAY + 5 * MINUTE, 12, usdt(60_050))),
            see(tick_of_time(T0 + 2 * DAY)),
        ]);
        run(steps)
    };
    let (with, with_stats) = outage(true, decision());
    let (plain, plain_stats) = outage(false, decision());
    assert_eq!(with, plain);
    assert_eq!((with_stats.reach_backs, plain_stats.reach_backs), (0, 0));
    let cut = censor(T0 + DAY - 5 * MINUTE, CensorReason::Gap);
    let day = with[0].horizons[4];
    assert_eq!((day.race, day.censored), (Race::Censored(cut), Some(cut)));
    assert_eq!(day.up, excursion(usdt(50), T0 + 1_000));
    assert!(
        with[0].horizons[..4]
            .iter()
            .all(|horizon| horizon.censored.is_none())
    );
    // The passages are cut with the 1d window.
    let (with, _) = outage(true, sfp_decision(usdt(60_200), usdt(59_000)));
    let (plain, _) = outage(false, sfp_decision(usdt(60_200), usdt(59_000)));
    assert_eq!(with, plain);
    assert_eq!(with[0].level.unwrap().passage, Passage::Censored(cut));
    assert_eq!(with[0].extreme.unwrap().passage, Passage::Censored(cut));
    // Two records: the older one waits for the trades stream as well, and
    // both are censored from the gap's start.
    let mut steps = decision();
    let later = trade(T0 + DAY - 5 * MINUTE, 11, usdt(60_000));
    steps.push(see(later.clone()));
    steps.push(Step::Open(Box::new(record_at(
        later.canonical_key(),
        usdt(60_000),
        Some(usdt(1_000)),
    ))));
    steps.extend([
        see(tick_of_time(T0 + DAY)),
        see(gap(
            Stream::Trades,
            T0 + DAY - 5 * MINUTE + 2_000,
            T0 + DAY + 5 * MINUTE,
        )),
        see(trade(T0 + DAY + 5 * MINUTE, 12, usdt(60_050))),
        see(tick_of_time(T0 + 3 * DAY)),
    ]);
    let (labels, stats) = run(steps);
    assert_eq!(stats.reach_backs, 0);
    let cut = censor(T0 + DAY - 5 * MINUTE + 2_000, CensorReason::Gap);
    assert_eq!(labels[0].horizons[4].censored, Some(cut));
    assert!(labels[1].horizons.iter().all(|h| h.censored == Some(cut)));
}

#[test]
fn end_of_data_while_trades_are_silent_censors_at_the_last_trade() {
    // Re-review regression (PR #148): one trade after the decision, then
    // the trades fall silent and the data ends. An open-interest event
    // after the 1d end closes the 1d window, but no trades-stream event
    // shows the trades complete past the last trade, so `finish` censors
    // there, exactly as on the tape without the other stream's event.
    let tail = |other: bool, mut steps: Vec<Step>| {
        steps.push(see(trade(T0 + 1_000, 11, usdt(60_050))));
        if other {
            steps.push(see(tick_of_time(T0 + DAY + HOUR)));
        }
        run(steps)
    };
    let (with, _) = tail(true, decision());
    let (plain, _) = tail(false, decision());
    assert_eq!(with, plain);
    let cut = censor(T0 + 1_000, CensorReason::EndOfData);
    for horizon in with[0].horizons {
        assert_eq!(
            (horizon.race, horizon.censored),
            (Race::Censored(cut), Some(cut))
        );
    }
    let (with, _) = tail(true, sfp_decision(usdt(60_200), usdt(59_000)));
    let (plain, _) = tail(false, sfp_decision(usdt(60_200), usdt(59_000)));
    assert_eq!(with, plain);
    assert_eq!(with[0].level.unwrap().passage, Passage::Censored(cut));
}

#[test]
fn trailing_gaps_of_the_driver_contract() {
    // ADR-055 D6: after the last event the driver passes the trades-stream
    // trailing gaps to `observe`, no other stream's, then calls `finish`.
    let tail = |trailing: Option<MarketEvent>| {
        let mut steps = after_decision([
            see(trade(T0 + 1_000, 11, usdt(60_050))),
            see(tick_of_time(T0 + DAY - HOUR)),
        ]);
        steps.extend(trailing.map(see));
        label_of(steps)
    };
    let silent = tail(None);
    let end = censor(T0 + 1_000, CensorReason::EndOfData);
    assert!(silent.horizons.iter().all(|h| h.censored == Some(end)));
    // The trades trailing gap starts at the last trade: the same cut, now
    // named for the outage the provider announced.
    let announced = tail(Some(gap(Stream::Trades, T0 + 1_000, T0 + DAY + 2 * HOUR)));
    let cut = censor(T0 + 1_000, CensorReason::Gap);
    assert!(announced.horizons.iter().all(|h| h.censored == Some(cut)));
    for (announced, silent) in announced.horizons.iter().zip(silent.horizons) {
        assert_eq!((announced.up, announced.down), (silent.up, silent.down));
    }
    // Another stream's trailing gap ends past the data end; it closes the
    // 1d window and changes no label.
    let other = tail(Some(gap(
        Stream::OpenInterest,
        T0 + DAY - HOUR,
        T0 + DAY + HOUR,
    )));
    assert_eq!(other, silent);
}

#[test]
fn a_late_gap_after_emission_reaches_back_and_changes_nothing() {
    let mut labeler = ForwardLabeler::new(Labeling::V1);
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
    // Another stream's event closes the 1d window, but only a trades-stream
    // event at or after its end emits the label.
    labeler.observe(&tick_of_time(T0 + DAY), |label| labels.push(label));
    assert!(labels.is_empty());
    labeler.observe(&trade(T0 + DAY, 12, usdt(60_100)), |label| {
        labels.push(label);
    });
    assert_eq!(labels.len(), 1);
    let emitted = labels[0].clone();
    // Starts inside the emitted window, after a later trade was delivered
    // (as a live `LateEvent` gap can): reaches back.
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
    // A rejected trade dated inside it (out of order) does: it censors at
    // its own time, earlier than the last accepted trades-stream event.
    labeler.reject(&trade(T0 + 3_000, 5, usdt(1)));
    labeler.finish(|label| labels.push(label));
    assert_eq!(labeler.stats().reach_backs, 2);
    assert_eq!(labels, [emitted]);
    // A label censored in every window is emitted by the gap itself: the
    // gap is a trades-stream event at or after its 1d censor time, so a
    // rejected trade dated before that censor only reaches back.
    let mut labeler = ForwardLabeler::new(Labeling::V1);
    let mut labels = Vec::new();
    for step in decision() {
        match step {
            Step::See(event) => labeler.observe(&event, |label| labels.push(label)),
            Step::Open(record) => labeler.open(&record),
            Step::Reject(_) => unreachable!(),
        }
    }
    labeler.observe(&trade(T0 + 1_000, 11, usdt(60_050)), |label| {
        labels.push(label);
    });
    labeler.observe(
        &gap(Stream::Trades, T0 + 2_000, T0 + 10 * MINUTE),
        |label| labels.push(label),
    );
    assert_eq!(labels.len(), 1);
    labeler.reject(&trade(T0 + 1_500, 5, usdt(1)));
    labeler.finish(|label| labels.push(label));
    assert_eq!(labeler.stats().reach_backs, 1);
    let cut = censor(T0 + 2_000, CensorReason::Gap);
    assert!(labels[0].horizons.iter().all(|h| h.censored == Some(cut)));
    // A rejected trades-stream event dated at the 1d end does not count for
    // emission: it censors the label, whose windows another stream closed,
    // at the last accepted trade, and the next observation emits it there.
    let mut labeler = ForwardLabeler::new(Labeling::V1);
    let mut labels = Vec::new();
    for step in after_decision([
        see(trade(T0 + 1_000, 11, usdt(60_050))),
        see(tick_of_time(T0 + DAY)),
        Step::Reject(trade(T0 + DAY, 5, usdt(1))),
    ]) {
        match step {
            Step::See(event) => labeler.observe(&event, |label| labels.push(label)),
            Step::Open(record) => labeler.open(&record),
            Step::Reject(event) => labeler.reject(&event),
        }
    }
    assert!(labels.is_empty());
    labeler.observe(&tick_of_time(T0 + DAY + 1), |label| labels.push(label));
    assert_eq!(labels.len(), 1);
    labeler.observe(&gap(Stream::Trades, T0 + 2_000, T0 + DAY + 2), |label| {
        labels.push(label);
    });
    labeler.finish(|label| labels.push(label));
    assert_eq!(labeler.stats().reach_backs, 0);
    let cut = censor(T0 + 1_000, CensorReason::Rejected);
    assert!(labels[0].horizons.iter().all(|h| h.censored == Some(cut)));
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
        see(day_end()),
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
        let mut labeler = ForwardLabeler::new(Labeling::V1);
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
    /// Per tape event, the engine's rejection; a rejected event is reported
    /// to the labeler and passed to nothing else.
    rejected: Vec<Option<StateError>>,
}

fn drive(tape: &[MarketEvent]) -> Drive {
    drive_with(tape, Labeling::V1)
}

/// [`drive`] with a labeler of `labeling`.
fn drive_with(tape: &[MarketEvent], labeling: Labeling) -> Drive {
    let mut engine = MarketStateEngine::new();
    let mut sampler = ContextSampler::new(None);
    let mut labeler = ForwardLabeler::new(labeling);
    let mut run = Drive {
        infos: Vec::new(),
        lines: Vec::new(),
        triggers: Vec::new(),
        labels: Vec::new(),
        stats: LabelStats::default(),
        rejected: Vec::new(),
    };
    for (index, event) in tape.iter().enumerate() {
        let rejection = engine.apply(event).err();
        run.rejected.push(rejection);
        if rejection.is_some() {
            labeler.reject(event);
            continue;
        }
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
    RUN.get_or_init(|| golden_of(Labeling::V1))
}

/// [`golden`] under [`Labeling::V2`].
fn golden_v2() -> &'static Golden {
    static RUN: std::sync::OnceLock<Golden> = std::sync::OnceLock::new();
    RUN.get_or_init(|| golden_of(Labeling::V2))
}

/// One drive of `labeling` over the candidate golden tape, and its golden.
fn golden_of(labeling: Labeling) -> Golden {
    let tape = crate::candidate::tests::golden_tape();
    let mut run = drive_with(&tape, labeling);
    assert!(run.rejected.iter().all(Option::is_none));
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
/// event at whose observation it is ready (`tape.len()`: only by `finish`),
/// written from ADR-055 D6. A window closes at the first event at or after
/// its end, or when a censor reaches it. The label is ready once all its
/// windows are closed and some accepted trades-stream event so far (a
/// trade or a trades gap, before the decision too) has a time at or after
/// its 1d censor time, or its 1d window end when that is uncensored. Every
/// trades gap delivered before the label is emitted, at the index
/// `emitted` (`None`: when it is ready), censors every horizon whose window
/// ends after its c, closed windows included, keeping the earliest censor.
/// A rejected trades-stream event never shows more trades than the
/// accepted ones did, but may show fewer: it censors the same way at the
/// earlier of its own time and the latest accepted trades-stream event
/// before it (`Rejected`; the decision when there is none). A label never ready is censored the same way at the
/// tape's last accepted trades-stream event (`EndOfData`).
fn model_censors(
    tape: &[MarketEvent],
    rejected: &[Option<StateError>],
    info: &Info,
    emitted: Option<usize>,
) -> ([Option<Censor>; 5], usize) {
    let decision = info.event.time.as_millis();
    let ends: [i64; 5] = std::array::from_fn(|position| decision + HORIZONS[position].millis());
    let of_trades = |event: &MarketEvent| match event {
        MarketEvent::Trade(_) => true,
        MarketEvent::FeedGap(gap) => gap.stream == Stream::Trades,
        _ => false,
    };
    // The latest accepted trades-stream time up to the decision event.
    let accepted_trades_until = |end: usize| {
        tape[..end]
            .iter()
            .zip(rejected)
            .filter(|(event, rejection)| rejection.is_none() && of_trades(event))
            .map(|(event, _)| event.time().as_millis())
            .max()
    };
    let mut trades_until = accepted_trades_until(info.index + 1);
    let mut stale = info.reference.is_none();
    for (event, _) in tape[..=info.index]
        .iter()
        .zip(rejected)
        .rev()
        .filter(|(_, rejection)| rejection.is_none())
    {
        match event {
            MarketEvent::Trade(_) => break,
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                stale = true;
                break;
            }
            _ => {}
        }
    }
    let mut censors: [Option<Censor>; 5] = [None; 5];
    let mut closed = [false; 5];
    if stale {
        censors = [Some(censor(decision, CensorReason::StaleReference)); 5];
        closed = [true; 5];
    }
    let mut ready = None;
    for (index, event) in tape.iter().enumerate().skip(info.index + 1) {
        if ready.is_some() && emitted.is_none_or(|emitted| index > emitted) {
            break;
        }
        // A rejected trades-stream event: dated after the latest accepted
        // trades-stream event it proves nothing beyond it, dated before it
        // the refused trade may have been in the path from its time on.
        // It closes nothing, moves no trades-stream time, emits nothing.
        if rejected[index].is_some() {
            if of_trades(event) {
                let at = match trades_until {
                    None => decision,
                    Some(until) => until.min(event.time().as_millis()).max(decision),
                };
                for position in 0..5 {
                    let earlier = censors[position].is_some_and(|cut| cut.at.as_millis() <= at);
                    if at < ends[position] && !earlier {
                        censors[position] = Some(censor(at, CensorReason::Rejected));
                        closed[position] = true;
                    }
                }
            }
            continue;
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
        if of_trades(event) {
            trades_until = trades_until.max(Some(event.time().as_millis()));
        }
        let until = censors[4].map_or(ends[4], |cut| cut.at.as_millis());
        if ready.is_none()
            && closed.iter().all(|closed| *closed)
            && trades_until.is_some_and(|at| at >= until)
        {
            ready = Some(index);
        }
    }
    if ready.is_none() {
        // Emitted by `finish`: the trades are known only up to the tape's
        // last accepted trades-stream event, so every horizon whose window
        // ends after it is censored there, closed windows included, unless
        // an earlier censor already cut it; every other window ends at or
        // before an accepted event, which closed it.
        let at = accepted_trades_until(tape.len()).map_or(decision, |last| last.max(decision));
        for position in 0..5 {
            let earlier = censors[position].is_some_and(|cut| cut.at.as_millis() <= at);
            if at < ends[position] && !earlier {
                censors[position] = Some(censor(at, CensorReason::EndOfData));
            }
        }
    }
    (censors, ready.unwrap_or(tape.len()))
}

/// The label of `info`, emitted after the event at `emitted`, by scanning
/// the tape after its event: written from the ADR-055 text, independent of
/// the labeler's data structures.
fn model_label(
    tape: &[MarketEvent],
    rejected: &[Option<StateError>],
    info: &Info,
    ordinal: u32,
    clear: [bool; 5],
    emitted: usize,
) -> ForwardLabel {
    let decision = info.event.time.as_millis();
    let (censors, _) = model_censors(tape, rejected, info, Some(emitted));
    // The considered trades and the censor of the horizon at `position`.
    let window = |position: usize| -> (Vec<Trade>, Option<Censor>) {
        let end = decision + HORIZONS[position].millis();
        let cut = censors[position];
        let until = cut.map_or(end, |cut| cut.at.as_millis().min(end));
        let trades = tape[info.index + 1..]
            .iter()
            .zip(&rejected[info.index + 1..])
            .filter_map(|(event, rejection)| match event {
                MarketEvent::Trade(trade)
                    if rejection.is_none() && trade.time.as_millis() < until =>
                {
                    Some(*trade)
                }
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
            emitted = emitted.max(model_censors(tape, &run.rejected, info, None).1);
            emitted
        })
        .collect();
    let mut checked = 0;
    let mut ordinal = 0;
    for (position, (info, (at, label))) in run.infos.iter().zip(&run.labels).enumerate() {
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
        assert_eq!(*at, emitted[position], "emission point of {label}");
        if pick(info) {
            let expected = model_label(
                tape,
                &run.rejected,
                info,
                ordinal,
                clear[position],
                emitted[position],
            );
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
/// streams. With `sparse` above 1, only one trades gap in about `sparse`
/// is kept, so most 1d windows see no earlier gap and an open-interest
/// event inside an outage is what closes them (ADR-055 D6).
fn walk(seed: u64, days: i64, sparse: u64) -> Vec<MarketEvent> {
    let mut lcg = Lcg(seed);
    let mut events = Vec::new();
    let mut time = 13 * HOUR;
    let mut hundredths: i64 = 6_000_000;
    let mut trade_id = 0;
    while time < days * DAY {
        time += 1 + lcg.below(40_000);
        match lcg.below(1_000) {
            // No draw when every gap is kept: the dense tapes stay as they
            // were.
            0..=2 if sparse == 1 || lcg.below(sparse) == 0 => {
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

/// How many labels of `run` have a 1d window that an event of another
/// stream closed before the trades gap that censors it was delivered.
fn closed_before_the_gap(tape: &[MarketEvent], run: &Drive) -> usize {
    run.infos
        .iter()
        .zip(&run.labels)
        .filter(|(info, (_, label))| {
            let Some(cut) = label.horizons[4].censored else {
                return false;
            };
            let end = info.event.time.as_millis() + DAY;
            let closing = tape
                .iter()
                .position(|event| event.time().as_millis() >= end);
            let censoring = tape
                .iter()
                .enumerate()
                .skip(info.index + 1)
                .position(|(_, event)| {
                    matches!(event, MarketEvent::FeedGap(gap)
                    if gap.stream == Stream::Trades && gap.start.max(info.event.time) == cut.at)
                });
            cut.reason == CensorReason::Gap
                && closing
                    .zip(censoring)
                    .is_some_and(|(closing, censoring)| info.index + 1 + censoring > closing)
        })
        .count()
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
    // Sparse outages leave most 1d windows uncensored, so some of them are
    // closed by an open-interest event inside an outage and censored by the
    // gap announced after it (ADR-055 D6); seeds 11 and 12 hold eleven.
    let mut late_closed = 0;
    for (seed, days, sparse) in [(4, 4, 1), (5, 4, 1), (10, 4, 1), (11, 6, 10), (12, 6, 10)] {
        let tape = walk(0x6d69_6500_0026_0000 + seed, days, sparse);
        let run = drive(&tape);
        checked += check_model(&tape, &run, |_| true);
        let counts = outcomes(run.labels.iter().map(|(_, label)| label));
        for (total, count) in totals.iter_mut().zip(counts) {
            *total += count;
        }
        late_closed += closed_before_the_gap(&tape, &run);
    }
    // The data ends while the trades are silent: open-interest events run
    // on past every 1d end, and no trades-stream event follows the last
    // trade (ADR-055 D6). Every label whose 1d window ends after the last
    // trade is censored there, the windows they closed included.
    let mut tape = walk(0x6d69_6500_0026_0000 + 13, 3, 1);
    let last = tape.last().unwrap().time().as_millis();
    for step in 1..=(DAY + HOUR) / (5 * MINUTE) {
        tape.push(tick_of_time(last + step * 5 * MINUTE));
    }
    let run = drive(&tape);
    checked += check_model(&tape, &run, |_| true);
    let end = tape.last().unwrap().time().as_millis();
    let silent = run
        .infos
        .iter()
        .zip(&run.labels)
        .filter(|(info, (_, label))| {
            info.event.time.as_millis() + DAY <= end
                && label.horizons[4]
                    .censored
                    .is_some_and(|cut| cut.reason == CensorReason::EndOfData)
        })
        .count();
    assert!(silent > 0, "{silent}");
    assert!(late_closed > 0, "{late_closed}");
    assert!(checked > 10_000, "{checked}");
    // Every outcome occurs; same_ms ties included.
    assert!(totals.iter().all(|count| *count > 0), "{totals:?}");
}

#[test]
fn labels_match_a_naive_model_when_the_data_ends_on_a_rejection() {
    // Re-review regressions (PR #148): the engine rejects the tape's final
    // event, a trades-stream event dated before, or later than, every
    // accepted one. Its time is not trusted (ADR-055 D6): every label still
    // comes out once, in record order, as the model says. Dated at or after
    // the last accepted trades-stream event, the rejection gives exactly
    // what the tape without it gives, with the end-of-data censor named
    // `Rejected`; dated before it (out of order), it censors earlier.
    let base = walk(0x6d69_6500_0026_0000 + 14, 3, 1);
    let last = base.last().unwrap().time().as_millis();
    let &MarketEvent::Trade(final_trade) = base
        .iter()
        .rev()
        .find(|event| matches!(event, MarketEvent::Trade(_)))
        .unwrap()
    else {
        unreachable!()
    };
    let later = |minutes: i64, trade_id: u64, qty: Qty| {
        MarketEvent::Trade(Trade {
            time: t(last + minutes * MINUTE),
            trade_id,
            qty,
            ..final_trade
        })
    };
    let next = final_trade.trade_id + 1;
    let plain = drive(&base);
    let known = base
        .iter()
        .filter(|event| event.stream() == Stream::Trades)
        .map(|event| event.time().as_millis())
        .max()
        .unwrap();
    // Each rejected event, with a check of the engine's reason.
    type Kind = fn(&StateError) -> bool;
    let rejections: [(MarketEvent, Kind); 6] = [
        (
            MarketEvent::Trade(Trade {
                time: t(last - 1),
                trade_id: next,
                ..final_trade
            }),
            |error| matches!(error, StateError::OutOfOrder { .. }),
        ),
        (later(3, final_trade.trade_id, final_trade.qty), |error| {
            matches!(error, StateError::Duplicate { .. })
        }),
        (later(3, next, Qty::from_units(i64::MAX)), |error| {
            matches!(error, StateError::Overflow { .. })
        }),
        (
            later(3, final_trade.trade_id - 1, final_trade.qty),
            |error| matches!(error, StateError::IdRegression { .. }),
        ),
        (
            gap(Stream::Trades, last + 4 * MINUTE, last + 3 * MINUTE),
            |error| matches!(error, StateError::InvalidGap { .. }),
        ),
        // More than 31 days of one-minute bars ahead.
        (later(32 * 24 * 60, next, final_trade.qty), |error| {
            matches!(error, StateError::TimeJump { .. })
        }),
    ];
    for (rejection, kind) in rejections {
        let mut tape = base.clone();
        tape.push(rejection);
        let run = drive(&tape);
        let (rejected, accepted) = run.rejected.split_last().unwrap();
        assert!(accepted.iter().all(Option::is_none));
        assert!(rejected.as_ref().is_some_and(kind), "{rejected:?}");
        check_model(&tape, &run, |_| true);
        assert_eq!(run.labels.len(), plain.labels.len());
        let rejected_at = tape.last().unwrap().time().as_millis();
        let mut cut = 0;
        for ((_, label), (_, plain)) in run.labels.iter().zip(&plain.labels) {
            if rejected_at >= known {
                assert_eq!(
                    *label,
                    renamed(plain, CensorReason::EndOfData, CensorReason::Rejected)
                );
            }
            cut += label
                .horizons
                .iter()
                .filter(|horizon| {
                    horizon
                        .censored
                        .is_some_and(|cut| cut.reason == CensorReason::Rejected)
                })
                .count();
        }
        assert!(cut > 0, "{rejected:?}");
    }
}

/// One drive like [`drive`], with the hand-built `records` instead of the
/// sampler's: each is opened after the tape event at its index, in order.
fn drive_records(tape: &[MarketEvent], records: &[(usize, ContextRecord)]) -> Drive {
    let mut engine = MarketStateEngine::new();
    let mut labeler = ForwardLabeler::new(Labeling::V1);
    let mut run = Drive {
        infos: Vec::new(),
        lines: Vec::new(),
        triggers: Vec::new(),
        labels: Vec::new(),
        stats: LabelStats::default(),
        rejected: Vec::new(),
    };
    for (index, event) in tape.iter().enumerate() {
        let rejection = engine.apply(event).err();
        run.rejected.push(rejection);
        if rejection.is_some() {
            labeler.reject(event);
            continue;
        }
        labeler.observe(event, |label| run.labels.push((index, label)));
        for (_, record) in records.iter().filter(|(at, _)| *at == index) {
            labeler.open(record);
            run.infos.push(Info::of(index, record));
        }
    }
    labeler.finish(|label| run.labels.push((tape.len(), label)));
    run.stats = labeler.stats();
    run
}

/// The ways the data can end, as the final event of
/// [`every_way_the_data_can_end_matches_the_model_and_the_rule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Ending {
    Trade,
    OtherEvent,
    TradesGap,
    OtherGap,
    Duplicate,
    IdRegression,
    Overflow,
    InvalidGap,
    OtherRejected,
    OutOfOrder,
    TimeJump,
}

#[test]
fn every_way_the_data_can_end_matches_the_model_and_the_rule() {
    // Re-review convergence (PR #148): every final event, accepted or
    // rejected (each engine rejection kind, checked from its error), placed
    // a millisecond before, at and after each window end of three open
    // labels, and the rejections that can carry an early time also before
    // the last accepted trade, with and without a trailing trades gap after
    // it (the M2 driver contract). Each run is model-checked (one label per
    // record, in record order), and every horizon's censor is checked
    // against the rule stated here from ADR-055 D6: the trades are known up
    // to the last accepted trades-stream event, a rejection censors at the
    // earlier of its time and that event (it never extends the coverage),
    // and a trades gap cuts from its start.
    //
    // Record E at T0-10m, censored by a trades gap from T0-9m and emitted
    // with it; a trade at T0-7m. Trades at T0 (record A), +30 s, +90 s
    // (record B), +2 m (touches both 5m uppers), and +3 m, the last trade
    // (`last`); record C at an open-interest event at +4 m, after it.
    let last = T0 + 3 * MINUTE;
    let touch = T0 + 2 * MINUTE;
    let emitted_until = T0 - 9 * MINUTE;
    let mut tape = vec![
        trade(T0 - 10 * MINUTE, 5, usdt(60_000)),
        gap(Stream::Trades, emitted_until, T0 - 8 * MINUTE),
        trade(T0 - 7 * MINUTE, 6, usdt(60_000)),
        trade(T0, 10, usdt(60_000)),
        trade(T0 + 30_000, 11, usdt(60_050)),
        trade(T0 + 90_000, 12, usdt(60_000)),
        trade(touch, 13, usdt(60_100)),
        trade(last, 14, usdt(59_990)),
        tick_of_time(T0 + 4 * MINUTE),
    ];
    let records: Vec<(usize, ContextRecord)> = [0, 3, 5, 8]
        .into_iter()
        .map(|index| {
            let key = tape[index].canonical_key();
            (index, record_at(key, usdt(60_000), Some(usdt(1_000))))
        })
        .collect();
    let decisions: Vec<i64> = records
        .iter()
        .map(|(index, _)| tape[*index].time().as_millis())
        .collect();
    let base = tape.len();
    let at = |millis: i64, trade_id: u64, qty: Qty| {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: usdt(60_000),
            qty,
            aggressor: Aggressor::Buy,
        })
    };
    let normal = Qty::from_units(10_000_000);
    let final_event = |ending: Ending, p: i64| match ending {
        Ending::Trade => at(p, 15, normal),
        Ending::OtherEvent => tick_of_time(p),
        Ending::TradesGap => gap(Stream::Trades, last, p),
        Ending::OtherGap => gap(Stream::OrderBook, last, p),
        Ending::Duplicate => at(p, 14, normal),
        Ending::IdRegression => at(p, 13, normal),
        Ending::Overflow => at(p, 15, Qty::from_units(i64::MAX)),
        Ending::InvalidGap => gap(Stream::Trades, p + 1, p),
        Ending::OtherRejected => gap(Stream::OrderBook, p + 1, p),
        Ending::OutOfOrder | Ending::TimeJump => at(p, 15, normal),
    };
    // The engine's verdict on each ending.
    let verdict = |ending: Ending, error: Option<&StateError>| match ending {
        Ending::Trade | Ending::OtherEvent | Ending::TradesGap | Ending::OtherGap => {
            error.is_none()
        }
        Ending::Duplicate => matches!(error, Some(StateError::Duplicate { .. })),
        Ending::IdRegression => matches!(error, Some(StateError::IdRegression { .. })),
        Ending::Overflow => matches!(error, Some(StateError::Overflow { .. })),
        Ending::InvalidGap | Ending::OtherRejected => {
            matches!(error, Some(StateError::InvalidGap { .. }))
        }
        Ending::OutOfOrder => matches!(error, Some(StateError::OutOfOrder { .. })),
        Ending::TimeJump => matches!(error, Some(StateError::TimeJump { .. })),
    };
    let mut points: Vec<i64> = decisions[1..]
        .iter()
        .flat_map(|decision| {
            HORIZONS.iter().flat_map(move |horizon| {
                [-1, 0, 1].map(|offset| decision + horizon.millis() + offset)
            })
        })
        .collect();
    points.sort_unstable();
    let positioned = [
        Ending::Trade,
        Ending::OtherEvent,
        Ending::TradesGap,
        Ending::OtherGap,
        Ending::Duplicate,
        Ending::IdRegression,
        Ending::Overflow,
        Ending::InvalidGap,
        Ending::OtherRejected,
    ];
    let mut cases: Vec<(Ending, i64)> = positioned
        .iter()
        .flat_map(|&ending| points.iter().map(move |&p| (ending, p)))
        .collect();
    // Dated before the last trade: before E's window, inside it, after its
    // censor, at A's decision, between A's and B's, at the touch, just
    // after it, and just before the last trade.
    let early = [
        T0 - 10 * MINUTE - 1,
        T0 - 9 * MINUTE - 30_000,
        T0 - MINUTE,
        T0,
        T0 + MINUTE,
        touch,
        touch + 1,
        last - 1,
    ];
    for p in early {
        cases.push((Ending::OutOfOrder, p));
        cases.push((Ending::InvalidGap, p));
    }
    // A month ahead.
    cases.push((Ending::TimeJump, T0 + 32 * DAY));
    let mut runs = 0;
    let (mut covered, mut reach_backs, mut early_cuts) = (0, 0, 0);
    for (ending, p) in cases {
        for trailing in [false, true] {
            tape.truncate(base);
            tape.push(final_event(ending, p));
            let rejected = !matches!(
                ending,
                Ending::Trade | Ending::OtherEvent | Ending::TradesGap | Ending::OtherGap
            );
            // The last accepted trades-stream event, by hand.
            let known = match ending {
                Ending::Trade | Ending::TradesGap => p,
                _ => last,
            };
            if trailing {
                // Starts at the trades stream's last delivered event and
                // ends after every event the engine accepted, or would.
                let end = if ending == Ending::TimeJump {
                    last + 2 * MINUTE
                } else {
                    p.max(T0 + 4 * MINUTE) + 1
                };
                tape.push(gap(Stream::Trades, known, end));
            }
            let run = drive_records(&tape, &records);
            let case = format!("{ending:?} at {} trailing={trailing}", p - T0);
            assert!(verdict(ending, run.rejected[base].as_ref()), "{case}");
            assert!(
                run.rejected[base + 1..].iter().all(Option::is_none),
                "{case}"
            );
            check_model(&tape, &run, |_| true);
            // The rule: the censor time c and its reason, by hand.
            let (c, reason) = match ending {
                Ending::Trade if trailing => (p, CensorReason::Gap),
                Ending::Trade => (p, CensorReason::EndOfData),
                Ending::TradesGap => (last, CensorReason::Gap),
                Ending::OtherEvent | Ending::OtherGap | Ending::OtherRejected if trailing => {
                    (last, CensorReason::Gap)
                }
                Ending::OtherEvent | Ending::OtherGap | Ending::OtherRejected => {
                    (last, CensorReason::EndOfData)
                }
                // Every trades-stream rejection: never later than `last`,
                // so a trailing gap, which starts at `last`, replaces it
                // nowhere.
                _ => {
                    assert!(rejected, "{case}");
                    (p.min(last), CensorReason::Rejected)
                }
            };
            // E stays as it was emitted; a cause before its until reaches
            // back.
            let (_, emitted) = &run.labels[0];
            let cut = censor(emitted_until, CensorReason::Gap);
            assert!(
                emitted
                    .horizons
                    .iter()
                    .all(|horizon| horizon.race == Race::Censored(cut)),
                "{case}"
            );
            let reaches = reason == CensorReason::Rejected && c < emitted_until;
            assert_eq!(run.stats.reach_backs, u64::from(reaches), "{case}");
            reach_backs += usize::from(reaches);
            early_cuts += usize::from(reason == CensorReason::Rejected && c < last);
            for ((_, label), decision) in run.labels[1..].iter().zip(&decisions[1..]) {
                for (position, horizon) in label.horizons.iter().enumerate() {
                    let end = decision + HORIZONS[position].millis();
                    // A window ending after c is censored at max(c,
                    // decision); one ending at or before c is covered.
                    let expected = (c < end).then(|| censor(c.max(*decision), reason));
                    assert_eq!(horizon.censored, expected, "{case}: {label}");
                    covered += usize::from(expected.is_none());
                    // A race won strictly before the censor keeps its winner.
                    let won = position == 0
                        && *decision < touch
                        && expected.is_none_or(|cut| cut.at > t(touch));
                    let race = if won {
                        Race::Upper {
                            at: t(touch),
                            same_ms: false,
                        }
                    } else {
                        expected.map_or(Race::Neither, Race::Censored)
                    };
                    assert_eq!(horizon.race, race, "{case}: {label}");
                }
            }
            runs += 1;
        }
    }
    assert_eq!(runs, 2 * (positioned.len() * 45 + 2 * early.len() + 1));
    assert!(covered > 0);
    assert_eq!(reach_backs, 2 * 2 * 2);
    assert_eq!(early_cuts, 2 * 2 * early.len());
}

#[test]
fn an_out_of_order_rejection_mid_data_censors_from_its_time() {
    // ADR-055 D6: a trade refused as out of order, dated before the last
    // accepted trade, may have been in the path from its time on, so it
    // cuts the race B's 5m window won after that time, and it reaches back
    // into A, emitted already, which stays as it was. A rejection dated
    // after the last accepted trade (a duplicate id) censors there and
    // keeps the race. The data then goes on; the model agrees on both.
    let tape = |rejection: MarketEvent| {
        vec![
            trade(T0, 10, usdt(60_000)),
            trade(T0 + 1_000, 11, usdt(60_050)),
            gap(Stream::Trades, T0 + 2_000, T0 + 10 * MINUTE),
            trade(T0 + 10 * MINUTE, 12, usdt(60_000)),
            trade(T0 + 10 * MINUTE + 30_000, 13, usdt(60_100)),
            trade(T0 + 11 * MINUTE, 14, usdt(60_000)),
            rejection,
            trade(T0 + 12 * MINUTE, 15, usdt(60_000)),
            trade(T0 + 10 * MINUTE + DAY, 16, usdt(60_000)),
        ]
    };
    let records = |tape: &[MarketEvent]| -> Vec<(usize, ContextRecord)> {
        [0, 3]
            .into_iter()
            .map(|index| {
                let key = tape[index].canonical_key();
                (index, record_at(key, usdt(60_000), Some(usdt(1_000))))
            })
            .collect()
    };
    let a_cut = censor(T0 + 2_000, CensorReason::Gap);
    let won = Race::Upper {
        at: t(T0 + 10 * MINUTE + 30_000),
        same_ms: false,
    };
    for (rejection, cut_at, reach_backs) in [
        (trade(T0 + 1_500, 17, usdt(60_000)), T0 + 10 * MINUTE, 1),
        (
            trade(T0 + 11 * MINUTE + 500, 14, usdt(60_000)),
            T0 + 11 * MINUTE,
            0,
        ),
    ] {
        let tape = tape(rejection);
        let run = drive_records(&tape, &records(&tape));
        let error = run.rejected[6].as_ref();
        if reach_backs == 1 {
            assert!(matches!(error, Some(StateError::OutOfOrder { .. })));
        } else {
            assert!(matches!(error, Some(StateError::Duplicate { .. })));
        }
        check_model(&tape, &run, |_| true);
        assert_eq!(run.stats.reach_backs, reach_backs);
        let (a, b) = (&run.labels[0].1, &run.labels[1].1);
        assert!(a.horizons.iter().all(|h| h.race == Race::Censored(a_cut)));
        let cut = censor(cut_at, CensorReason::Rejected);
        assert!(b.horizons.iter().all(|h| h.censored == Some(cut)));
        let race = if cut_at > T0 + 10 * MINUTE + 30_000 {
            won
        } else {
            Race::Censored(cut)
        };
        assert_eq!(b.horizons[0].race, race);
        assert_eq!(b.horizons[1].race, Race::Censored(cut));
    }
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
/// decision event changes labels, never the records up to it, their clear
/// flags or their barriers (ADR-056: the v2 scale reads the bars closed up
/// to the decision only), and leaves every label emitted before the first
/// mutated event as it was.
fn assert_no_leakage(tape: &[MarketEvent], cuts: &[i64], labeling: Labeling) -> usize {
    let base = drive_with(tape, labeling);
    assert!(base.rejected.iter().all(Option::is_none));
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
            let mutated = drive_with(&variant, labeling);
            assert!(mutated.rejected.iter().all(Option::is_none), "{name}");
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
            let barriers = |labels: &[ForwardLabel]| -> Vec<[Option<Barriers>; 5]> {
                labels
                    .iter()
                    .map(|label| label.horizons.map(|horizon| horizon.barriers))
                    .collect()
            };
            assert_eq!(barriers(&old), barriers(&new), "{name}");
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
    for labeling in [Labeling::V1, Labeling::V2] {
        let mutations = assert_no_leakage(
            &tape,
            &[3 * DAY + 7 * HOUR, 9 * DAY + 9 * HOUR + 30 * MINUTE],
            labeling,
        );
        assert_eq!(mutations, 10);
    }
}

#[test]
fn labels_never_leak_into_records_on_random_tapes() {
    for seed in [6, 7] {
        let tape = spaced(&walk(0x6d69_6500_0026_0000 + seed, 4, 1));
        for labeling in [Labeling::V1, Labeling::V2] {
            assert_eq!(assert_no_leakage(&tape, &[2 * DAY + 3 * HOUR], labeling), 5);
        }
    }
}

// ------------------------------------------- labeling v2 (ADR-056 D4′)

/// The true range of every closed bar of [`minute_tape`] that holds two
/// consecutive minutes: 100 USDT.
const MINUTE_RANGE: i64 = 100;

/// One trade a minute, a second past each minute `0..minutes` except those
/// in `skip`, at 60 000 USDT on even minutes and 60 100 USDT on odd ones.
/// Every closed bar of a horizon timeframe that holds two consecutive
/// minutes has a range of [`MINUTE_RANGE`] and a true range of exactly that
/// (every close is one of the two prices), so every ready ATR_H is 100 USDT.
fn minute_tape(minutes: i64, skip: std::ops::Range<i64>) -> Vec<MarketEvent> {
    (0..minutes)
        .filter(|minute| !skip.contains(minute))
        .map(|minute| {
            let price = usdt(60_000 + MINUTE_RANGE * (minute % 2));
            trade(minute * MINUTE + 1_000, minute as u64 + 1, price)
        })
        .collect()
}

/// `tape` through a labeler of `labeling`, a baseline record with a
/// warming-up ATR(1h) opened at the trade of each minute in `decisions`.
fn label_minutes(
    labeling: Labeling,
    tape: &[MarketEvent],
    decisions: &[i64],
) -> (Vec<ForwardLabel>, LabelStats) {
    let mut steps = Vec::new();
    for event in tape {
        steps.push(see(event.clone()));
        let MarketEvent::Trade(at) = event else {
            continue;
        };
        if decisions.contains(&(at.time.as_millis() / MINUTE)) {
            let record = record_at(event.canonical_key(), at.price, None);
            steps.push(Step::Open(Box::new(record)));
        }
    }
    run_with(labeling, steps)
}

/// The v2 offset of a ready ATR_H of [`MINUTE_RANGE`] USDT: 31.333 USDT.
fn minute_offset() -> i64 {
    usdt(MINUTE_RANGE).units() * SCALE_MULTIPLE_E5 / MULTIPLE_SCALE
}

#[test]
fn v2_scales_warm_up_on_fifteen_bars_of_each_horizon() {
    // A scale is ready once 15 bars of its timeframe have closed: the
    // partial first bar anchors, then 14 samples (ADR-033 decision 6), so
    // at minute 15 · tf, read after the decision event closes the 15th.
    let boundaries = HORIZONS.map(|horizon| 15 * horizon.millis() / MINUTE);
    assert_eq!(boundaries, [75, 225, 900, 3_600, 21_600]);
    let decisions: Vec<i64> = boundaries
        .iter()
        .flat_map(|minute| [minute - 1, *minute])
        .collect();
    let tape = minute_tape(boundaries[4] + 2, 0..0);
    let (labels, stats) = label_minutes(Labeling::V2, &tape, &decisions);
    assert_eq!(labels.len(), decisions.len());
    assert_eq!(stats.scale_breaks, 0);
    let offset = minute_offset();
    assert_eq!(offset, 3_133_300_000);
    for (label, minute) in labels.iter().zip(&decisions) {
        let reference = label.reference.unwrap().units();
        for (position, horizon) in label.horizons.iter().enumerate() {
            if *minute >= boundaries[position] {
                assert_eq!(
                    horizon.barriers,
                    Some(Barriers {
                        lower: Price::from_units(reference - offset),
                        upper: Price::from_units(reference + offset),
                    }),
                    "{minute} {}",
                    horizon.horizon
                );
                assert_ne!(horizon.race, Race::Unavailable);
            } else {
                assert_eq!(horizon.barriers, None, "{minute} {}", horizon.horizon);
                assert_eq!(horizon.race, Race::Unavailable);
            }
        }
        // The record's ATR(1h) stays the record's; v2 does not read it.
        assert_eq!(label.atr, None);
    }
    // Under v1 the same records have no race: their ATR(1h) is warming up.
    let (labels, _) = label_minutes(Labeling::V1, &tape, &decisions);
    assert!(labels.iter().all(|label| {
        label
            .horizons
            .iter()
            .all(|horizon| horizon.race == Race::Unavailable)
    }));
}

#[test]
fn an_empty_incomplete_bar_restarts_the_v2_scale_warm_up() {
    // No trades in minutes 110..121, and a trades gap over them: the 5m
    // bars [110, 115) and [115, 120) are empty and incomplete, a break
    // (ADR-033 decision 6); [120, 125) only anchors, so the 5m scale is
    // ready again after the 14 samples [125, 195).
    let mut tape = minute_tape(200, 110..121);
    let resumed = tape
        .iter()
        .position(|event| event.time().as_millis() > 110 * MINUTE)
        .unwrap();
    tape.insert(
        resumed,
        gap(Stream::Trades, 109 * MINUTE + 1_000, 121 * MINUTE),
    );
    let decisions = [100, 121, 194, 195];
    let (labels, stats) = label_minutes(Labeling::V2, &tape, &decisions);
    let ready: Vec<bool> = labels
        .iter()
        .map(|label| label.horizons[0].barriers.is_some())
        .collect();
    assert_eq!(ready, [true, false, false, true]);
    let reference = labels[3].reference.unwrap().units();
    assert_eq!(
        labels[3].horizons[0].barriers.unwrap().upper.units(),
        reference + minute_offset()
    );
    // A break of the bar series is the ATR's own rule, not a scale break.
    assert_eq!(stats.scale_breaks, 0);
}

#[test]
fn a_bar_error_restarts_every_v2_scale_and_is_counted() {
    // A labeler alone takes any event; the engine would refuse a trade
    // 40 days ahead as a time jump (more than MAX_BARS_PER_EVENT 1m bars),
    // and so do the labeler's bars. The scales start over, the labels still
    // come out.
    let mut tape = minute_tape(100, 0..0);
    tape.push(trade(40 * DAY, 1_000, usdt(60_000)));
    tape.push(trade(40 * DAY + MINUTE, 1_001, usdt(60_050)));
    let decisions = [80, 40 * DAY / MINUTE, 40 * DAY / MINUTE + 1];
    let (labels, stats) = label_minutes(Labeling::V2, &tape, &decisions);
    assert_eq!(stats.scale_breaks, 1);
    assert_eq!((stats.opened, stats.emitted), (3, 3));
    assert!(labels[0].horizons[0].barriers.is_some());
    for label in &labels[1..] {
        assert!(
            label
                .horizons
                .iter()
                .all(|horizon| horizon.race == Race::Unavailable),
            "{label}"
        );
    }
}

/// `label` without the barriers and the races of every horizon but 1h.
fn at_1h_only(label: &ForwardLabel) -> ForwardLabel {
    let mut label = label.clone();
    for (position, horizon) in label.horizons.iter_mut().enumerate() {
        if position != 2 {
            horizon.barriers = None;
            horizon.race = Race::Unavailable;
        }
    }
    label
}

#[test]
fn v2_is_v1_at_1h_and_reads_its_own_scale_elsewhere() {
    // The v2 1h scale is ATR(14) of the 1h bars, the record's ATR(1h), at
    // the same multiple: the 1h band and race are v1's, and so is every
    // field that does not depend on a barrier (ADR-056 D4′).
    let mut tapes = vec![golden().tape.clone()];
    tapes.extend([4, 5].map(|seed| walk(0x6d69_6500_0026_0000 + seed, 4, 1)));
    let (mut ready_1h, mut differ) = (0, 0);
    for tape in &tapes {
        let (v1, v2) = (
            drive_with(tape, Labeling::V1),
            drive_with(tape, Labeling::V2),
        );
        assert_eq!(v1.labels.len(), v2.labels.len());
        assert_eq!(v2.stats.scale_breaks, 0);
        for ((at1, one), (at2, two)) in v1.labels.iter().zip(&v2.labels) {
            assert_eq!(at1, at2, "the emission point");
            assert_eq!(at_1h_only(one), at_1h_only(two), "\n{one}\n{two}");
            ready_1h += usize::from(two.horizons[2].barriers.is_some());
            differ += usize::from((0..5).any(|position| {
                one.horizons[position].barriers != two.horizons[position].barriers
            }));
        }
    }
    assert!(ready_1h > 1_000, "{ready_1h}");
    assert!(differ > 1_000, "{differ}");
}

#[test]
fn a_rejected_trade_never_reaches_the_v2_bars() {
    // A trade the engine refuses as a duplicate, 1 000 USDT above the one it
    // repeats: accepted, it would raise its bars' highs and every scale.
    let tape = walk(0x6d69_6500_0026_0000 + 4, 3, 1);
    let (index, at) = tape
        .iter()
        .enumerate()
        .find_map(|(index, event)| match event {
            MarketEvent::Trade(trade) if trade.time.as_millis() >= 2 * DAY => Some((index, *trade)),
            _ => None,
        })
        .unwrap();
    let mut refused = tape.clone();
    refused.insert(
        index + 1,
        MarketEvent::Trade(Trade {
            price: Price::from_units(at.price.units() + 1_000 * SCALE),
            ..at
        }),
    );
    let (base, run) = (
        drive_with(&tape, Labeling::V2),
        drive_with(&refused, Labeling::V2),
    );
    assert!(matches!(
        run.rejected[index + 1],
        Some(StateError::Duplicate { .. })
    ));
    assert_eq!(base.labels.len(), run.labels.len());
    let barriers = |run: &Drive| -> Vec<[Option<Barriers>; 5]> {
        run.labels
            .iter()
            .map(|(_, label)| label.horizons.map(|horizon| horizon.barriers))
            .collect()
    };
    assert_eq!(barriers(&base), barriers(&run));
    // The labels after it have ready scales, and the rejection censored
    // some open labels: the comparison is not vacuous.
    let after = run
        .labels
        .iter()
        .filter(|(_, label)| label.event.time > at.time)
        .filter(|(_, label)| label.horizons[1].barriers.is_some())
        .count();
    assert!(after > 100, "{after}");
    assert!(run.labels.iter().any(|(_, label)| {
        label.horizons.iter().any(|horizon| {
            horizon
                .censored
                .is_some_and(|censor| censor.reason == CensorReason::Rejected)
        })
    }));
}

#[test]
fn the_labeling_versions_and_their_keys() {
    assert_eq!(Labeling::CURRENT, Labeling::V2);
    assert_eq!(Labeling::V1.key(), LABELING_V1);
    assert_eq!(Labeling::V2.key(), LABELING_V2);
    assert_eq!(LABELING_V2.to_string(), "conditional.labels@2");
    assert_eq!(ForwardLabeler::new(Labeling::V2).labeling(), Labeling::V2);
    // b = √(π / 32), v1's 1h multiple.
    let derived = (std::f64::consts::PI / 32.0).sqrt();
    assert_eq!(SCALE_MULTIPLE_E5, (derived * 1e5).round() as i64);
    assert_eq!(SCALE_MULTIPLE_E5, BARRIER_MULTIPLE_E5[2]);
}

#[test]
fn labeling_v2_fingerprint_golden() {
    let (v1, v2) = (labeling_text(Labeling::V1), labeling_text(Labeling::V2));
    assert_eq!(v2.lines().count(), v1.lines().count());
    assert!(v2.ends_with('\n'));
    // Only the labeling line and the barriers line differ.
    let differing: Vec<usize> = v1
        .lines()
        .zip(v2.lines())
        .enumerate()
        .filter(|(_, (one, two))| one != two)
        .map(|(line, _)| line)
        .collect();
    assert_eq!(differing, [1, 4]);
    assert!(v2.contains("\nlabeling conditional.labels@2\n"));
    assert!(v2.contains(
        "\nbarriers atr=atr14_adr033_over_closed_adr031_bars_of_the_horizon_timeframe at=after_decision_event bar_error=restart_warm_up multiple_e5=31333 scale=100000 offset=round_half_even unavailable=scale_not_ready,zero_offset,out_of_range\n"
    ));
    assert_eq!(
        labeling_fingerprint(Labeling::V2).to_string(),
        GOLDEN_LABELING_V2_FINGERPRINT
    );
    // v1 is untouched.
    assert_eq!(
        labeling_fingerprint(Labeling::V1).to_string(),
        GOLDEN_LABELING_FINGERPRINT
    );
}

#[test]
fn provenance_names_the_v2_labeling() {
    let engine = MarketStateEngine::new();
    let data = crate::research::DataVersion::from_hex(&"0f".repeat(32)).unwrap();
    let context = ContextProvenance::new(data, &engine);
    let provenance = LabelProvenance::new(context.clone(), Labeling::V2);
    assert_eq!(provenance.labeling, LABELING_V2);
    assert_eq!(
        provenance.to_string(),
        format!(
            "{context} labeling=conditional.labels@2 labeling_fingerprint={GOLDEN_LABELING_V2_FINGERPRINT}"
        )
    );
}

#[test]
fn golden_forward_labels_v2() {
    let (count, digest, first) = &golden_v2().golden;
    assert_eq!(first, GOLDEN_FIRST_TRIGGER_LABEL_V2);
    assert_eq!((*count, digest.as_str()), GOLDEN_LABELS_V2);
    // The same records as v1, one label each.
    assert_eq!(*count, GOLDEN_LABELS.0);
    let lines: Vec<String> = golden_v2()
        .run
        .labels
        .iter()
        .map(|(_, label)| label.to_string())
        .chain([labeling_text(Labeling::V2)])
        .collect();
    for line in &lines {
        assert_eq!(forbidden_in(line), None, "{line}");
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
    let text = labeling_text(Labeling::V1);
    assert!(text.starts_with(
        "mie-labeling 1\nlabeling conditional.labels@1\nreference trade.last_price@1 path=trades window=after_decision_key_before_decision_time_plus_h\nhorizons 5m,15m,1h,4h,1d\nbarriers atr=volatility.atr.1h@1 multiple_e5=9045,15666,31333,62666,153499 scale=100000 "
    ));
    assert_eq!(text.lines().count(), 16);
    assert!(text.ends_with('\n'));
    assert_eq!(
        labeling_fingerprint(Labeling::V1).to_string(),
        GOLDEN_LABELING_FINGERPRINT
    );
}

#[test]
fn provenance_names_the_records_and_the_labeling() {
    let engine = MarketStateEngine::new();
    let data = crate::research::DataVersion::from_hex(&"0f".repeat(32)).unwrap();
    let context = ContextProvenance::new(data, &engine);
    let provenance = LabelProvenance::new(context.clone(), Labeling::V1);
    assert_eq!(provenance.context, context);
    assert_eq!(provenance.labeling, LABELING_V1);
    assert_eq!(
        provenance.labeling_fingerprint,
        labeling_fingerprint(Labeling::V1)
    );
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
    lines.push(labeling_text(Labeling::V1));
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
    println!("labeling {}", labeling_fingerprint(Labeling::V1));
    println!("labels {} {}", golden.golden.0, golden.golden.1);
    println!("first {}", golden.golden.2);
    println!("stats {:?}", golden.run.stats);
    let golden = golden_v2();
    println!("labeling v2 {}", labeling_fingerprint(Labeling::V2));
    println!("labels v2 {} {}", golden.golden.0, golden.golden.1);
    println!("first v2 {}", golden.golden.2);
    println!("stats v2 {:?}", golden.run.stats);
}

const GOLDEN_LABELING_FINGERPRINT: &str = "604b624cc2522df9";
const GOLDEN_LABELS: (usize, &str) = (46_403, "8d7c4cebfc62d7da");
const GOLDEN_LABELING_V2_FINGERPRINT: &str = "6f90bff079e746f2";
const GOLDEN_LABELS_V2: (usize, &str) = (46_403, "d20a74869c28c233");
const GOLDEN_FIRST_TRIGGER_LABEL_V2: &str = "117032157ms Trade seq 785 #1 ref=61922.67000000 atr=26.01616928 [5m clear barriers=61920.67209748..61924.66790252 race=lower@117098590ms up=0.64000000@117309050ms down=4.22000000@117248836ms last=61923.31000000] [15m clear barriers=61918.82876666..61926.51123334 race=lower@117098590ms up=0.64000000@117309050ms down=11.49000000@117700476ms last=61915.79000000] [1h clear barriers=61914.51835368..61930.82164632 race=lower@117700476ms up=7.40000000@120543138ms down=11.49000000@117700476ms last=61928.16000000] [4h clear barriers=- race=unavailable up=23.70000000@129865828ms down=33.07000000@126265310ms last=61935.31000000] [1d barriers=- race=unavailable up=134.65000000@185552262ms down=33.07000000@126265310ms last=62018.89000000] level=61910.65000000:touched@121677419ms extreme=61908.96000000:touched@121677419ms";
const GOLDEN_FIRST_TRIGGER_LABEL: &str = "117032157ms Trade seq 785 #1 ref=61922.67000000 atr=26.01616928 [5m clear barriers=61920.31683749..61925.02316251 race=lower@117098590ms up=0.64000000@117309050ms down=4.22000000@117248836ms last=61923.31000000] [15m clear barriers=61918.59430692..61926.74569308 race=lower@117248836ms up=0.64000000@117309050ms down=11.49000000@117700476ms last=61915.79000000] [1h clear barriers=61914.51835368..61930.82164632 race=lower@117700476ms up=7.40000000@120543138ms down=11.49000000@117700476ms last=61928.16000000] [4h clear barriers=61906.36670736..61938.97329264 race=lower@122609041ms up=23.70000000@129865828ms down=33.07000000@126265310ms last=61935.31000000] [1d barriers=61882.73544032..61962.60455968 race=upper@146208863ms up=134.65000000@185552262ms down=33.07000000@126265310ms last=62018.89000000] level=61910.65000000:touched@121677419ms extreme=61908.96000000:touched@121677419ms";
