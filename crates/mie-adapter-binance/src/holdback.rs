//! Cross-stream merge into the canonical order with a bounded hold-back
//! (ADR-028 D5/D6).
//!
//! Events of all streams arrive interleaved in network order. They wait in
//! a buffer ordered by the full canonical order (`Ord for MarketEvent`) and
//! are released once they are more than `hold_back_ms` older than the
//! watermark: the highest ordering time among the events pushed so far,
//! gaps excluded. The watermark is exchange time, never the wall clock, so
//! the output is a pure function of the input sequence.
//!
//! An event that is not above the last released event cannot be delivered
//! any more. An exact repeat is dropped; anything lower is replaced by a
//! `LateEvent` gap on its stream that ends one millisecond after the last
//! released event, so the gap sorts after it (ADR-028 D6). A late gap keeps
//! its own reason and start. The output is strictly increasing by
//! construction.
//!
//! **Open-interest re-timing (ADR-032 D12, superseding ADR-028's live
//! open-interest ordering time).** Open interest is a sampled
//! state observation, and its REST `time` trails the poll by several
//! seconds. A late `OpenInterest` whose slot passed by no more than
//! `oi_retime_ms` (`last released + 1 − its time`) is not replaced by a gap.
//! It is delivered at `last released + 1` instead: the first millisecond
//! still open, so the order stays strictly increasing. Its value then
//! becomes valid later than the exchange stamped it, never earlier (ADR-028:
//! never a validity time before publication). Anything later than the
//! allowance is a `LateEvent` gap as usual. So is a sample whose exchange
//! `time` is not strictly newer than every open-interest sample already
//! admitted: re-timing it would deliver a stale snapshot as the current
//! value.

use mie_domain::event::{FeedGap, GapReason, MarketEvent, OpenInterest};
use mie_domain::time::EventTime;
use std::collections::BTreeSet;

/// What happened to a pushed event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Admission {
    /// Buffered for release in canonical order.
    Buffered,
    /// Dropped: it equals an event already released or buffered.
    Duplicate,
    /// Replaced by a `LateEvent` gap (or, for a gap, re-timed).
    Late,
    /// A late open-interest sample, delivered at the first open
    /// millisecond instead (within the re-time allowance).
    Retimed,
}

/// The canonical merge buffer.
#[derive(Debug, Clone)]
pub struct HoldBack {
    hold_back_ms: i64,
    oi_retime_ms: i64,
    buffer: BTreeSet<MarketEvent>,
    watermark: Option<EventTime>,
    last_released: Option<MarketEvent>,
    /// The newest exchange `time` of an open-interest sample admitted so
    /// far (buffered or re-timed). A late sample is re-timed only when it
    /// is strictly newer.
    newest_oi: Option<EventTime>,
}

impl HoldBack {
    /// A buffer that holds events back by `hold_back_ms` of exchange time
    /// and re-times open interest that is late by at most `oi_retime_ms`.
    /// Negative values count as zero.
    pub fn new(hold_back_ms: i64, oi_retime_ms: i64) -> Self {
        Self {
            hold_back_ms: hold_back_ms.max(0),
            oi_retime_ms: oi_retime_ms.max(0),
            buffer: BTreeSet::new(),
            watermark: None,
            last_released: None,
            newest_oi: None,
        }
    }

    /// The highest ordering time pushed so far, gaps excluded.
    pub fn watermark(&self) -> Option<EventTime> {
        self.watermark
    }

    /// Number of buffered events.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Whether nothing is buffered.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Admits `event` and returns the events now due, in canonical order.
    pub fn push(&mut self, event: MarketEvent) -> (Admission, Vec<MarketEvent>) {
        if !matches!(event, MarketEvent::FeedGap(_)) {
            let time = event.time();
            self.watermark = Some(self.watermark.map_or(time, |w| w.max(time)));
        }
        let oi_time = match &event {
            MarketEvent::OpenInterest(oi) => Some(oi.time),
            _ => None,
        };
        let (admission, admitted) = match &self.last_released {
            Some(last) if event == *last => (Admission::Duplicate, None),
            Some(last) if event < *last => {
                match retimed_open_interest(&event, last, self.oi_retime_ms, self.newest_oi) {
                    Some(retimed) => (Admission::Retimed, Some(retimed)),
                    None => (Admission::Late, Some(late_gap(&event, last))),
                }
            }
            _ => (Admission::Buffered, Some(event)),
        };
        let inserted = admitted.is_some_and(|admitted| self.buffer.insert(admitted));
        let admission = if admission == Admission::Buffered && !inserted {
            Admission::Duplicate
        } else {
            admission
        };
        if let (Some(time), Admission::Buffered | Admission::Retimed) = (oi_time, admission) {
            self.newest_oi = Some(self.newest_oi.map_or(time, |newest| newest.max(time)));
        }
        (admission, self.release_due())
    }

    /// Releases everything still buffered, in canonical order.
    pub fn finish(&mut self) -> Vec<MarketEvent> {
        let out: Vec<_> = std::mem::take(&mut self.buffer).into_iter().collect();
        if let Some(last) = out.last() {
            self.last_released = Some(last.clone());
        }
        out
    }

    fn release_due(&mut self) -> Vec<MarketEvent> {
        let Some(watermark) = self.watermark else {
            return Vec::new();
        };
        let horizon = watermark.as_millis().saturating_sub(self.hold_back_ms);
        let mut out = Vec::new();
        while self
            .buffer
            .first()
            .is_some_and(|first| first.time().as_millis() < horizon)
        {
            out.extend(self.buffer.pop_first());
        }
        if let Some(last) = out.last() {
            self.last_released = Some(last.clone());
        }
        out
    }
}

/// `event` delivered at `last + 1 ms`, if it is open interest whose slot
/// passed by at most `allowance_ms` and whose exchange time is strictly
/// newer than `newest_oi`, the newest open interest admitted so far.
fn retimed_open_interest(
    event: &MarketEvent,
    last: &MarketEvent,
    allowance_ms: i64,
    newest_oi: Option<EventTime>,
) -> Option<MarketEvent> {
    let MarketEvent::OpenInterest(oi) = event else {
        return None;
    };
    if newest_oi.is_some_and(|newest| oi.time <= newest) {
        return None;
    }
    let open = last.time().as_millis().saturating_add(1);
    (open.saturating_sub(oi.time.as_millis()) <= allowance_ms).then(|| {
        MarketEvent::OpenInterest(OpenInterest {
            time: EventTime::from_millis(open),
            ..*oi
        })
    })
}

/// The gap that replaces `event`, which sorts at or below `last`.
fn late_gap(event: &MarketEvent, last: &MarketEvent) -> MarketEvent {
    let end = EventTime::from_millis(last.time().as_millis().saturating_add(1));
    let gap = match event {
        MarketEvent::FeedGap(gap) => FeedGap { end, ..*gap },
        other => FeedGap {
            stream: other.stream(),
            start: other.time(),
            end,
            reason: GapReason::LateEvent,
        },
    };
    MarketEvent::FeedGap(gap)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::event::{
        Aggressor, Kline, Liquidation, MarkPrice, OpenInterest, Stream, Trade,
    };
    use mie_domain::num::{Price, Qty, Rate};
    use mie_domain::state::MarketStateEngine;

    fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    fn trade(millis: i64, id: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id: id,
            price: Price::from_units(8_500_000_000_000 + millis),
            qty: Qty::from_units(1_000_000),
            aggressor: Aggressor::Sell,
        })
    }

    fn liquidation(millis: i64, filled: i64) -> MarketEvent {
        MarketEvent::Liquidation(Liquidation {
            time: t(millis),
            aggressor: Aggressor::Buy,
            price: Price::from_units(8_400_000_000_000),
            avg_price: Price::from_units(8_401_000_000_000),
            filled_qty: Qty::from_units(filled),
        })
    }

    fn mark(millis: i64) -> MarketEvent {
        MarketEvent::MarkPrice(MarkPrice {
            time: t(millis),
            mark_price: Price::from_units(8_500_000_000_000),
            index_price: Price::from_units(8_500_100_000_000),
            funding_rate: Rate::from_units(523),
            next_funding_time: t(28_800_000),
        })
    }

    fn open_interest(millis: i64) -> MarketEvent {
        MarketEvent::OpenInterest(OpenInterest {
            time: t(millis),
            open_interest: Qty::from_units(9_500_000_000_000),
            resolution_ms: 10_000,
        })
    }

    fn kline(close: i64) -> MarketEvent {
        MarketEvent::Kline(Kline {
            open_time: t(close - 59_999),
            close_time: t(close),
            open: Price::from_units(8_500_000_000_000),
            high: Price::from_units(8_510_000_000_000),
            low: Price::from_units(8_490_000_000_000),
            close: Price::from_units(8_505_000_000_000),
            volume: Qty::from_units(1_000_000_000),
            taker_buy_volume: Qty::from_units(500_000_000),
            trade_count: 42,
        })
    }

    fn gap(stream: Stream, start: i64, end: i64, reason: GapReason) -> MarketEvent {
        MarketEvent::FeedGap(FeedGap {
            stream,
            start: t(start),
            end: t(end),
            reason,
        })
    }

    /// A mixed event set: trades every 7 ms, mark prices every 100 ms, a few
    /// liquidations (two in one millisecond), open interest, klines and a
    /// gap.
    fn fixture_set() -> Vec<MarketEvent> {
        let mut events = Vec::new();
        for i in 0..400_u64 {
            events.push(trade(10_000 + 7 * i as i64, 1_000 + i));
        }
        for i in 0..30 {
            events.push(mark(10_000 + 100 * i));
        }
        events.push(liquidation(10_350, 1_000));
        events.push(liquidation(10_350, 2_000));
        events.push(liquidation(11_111, 500));
        events.push(open_interest(11_000));
        events.push(open_interest(12_000));
        events.push(kline(11_999));
        events.push(gap(
            Stream::MarkPrice,
            10_900,
            11_000,
            GapReason::Disconnected,
        ));
        events
    }

    /// Deterministic linear congruential generator for test permutations.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }
    }

    /// Displaces every event by up to `max_shift` positions: a bounded
    /// arrival disorder.
    fn jitter(events: &[MarketEvent], seed: u64, max_shift: u64) -> Vec<MarketEvent> {
        let mut rng = Lcg(seed);
        let mut keyed: Vec<_> = events
            .iter()
            .enumerate()
            .map(|(i, e)| (i as u64 + rng.next() % (max_shift + 1), e.clone()))
            .collect();
        keyed.sort_by_key(|(k, _)| *k);
        keyed.into_iter().map(|(_, e)| e).collect()
    }

    fn run(hold: &mut HoldBack, events: Vec<MarketEvent>) -> Vec<MarketEvent> {
        let mut out = Vec::new();
        for event in events {
            out.extend(hold.push(event).1);
        }
        out.extend(hold.finish());
        out
    }

    fn assert_engine_accepts(events: &[MarketEvent]) {
        let mut engine = MarketStateEngine::new();
        for event in events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
        }
    }

    #[test]
    fn bounded_disorder_within_the_hold_back_yields_the_sorted_set() {
        let mut sorted = fixture_set();
        sorted.sort();
        for seed in 1..=20 {
            // Arrival order sorted by time, then jittered by up to 20
            // positions (about 140 ms of trades), well inside 2000 ms.
            let arrivals = jitter(&sorted, seed, 20);
            assert_ne!(arrivals, sorted, "seed {seed} permutes nothing");
            let out = run(&mut HoldBack::new(2_000, 0), arrivals);
            assert_eq!(out, sorted, "seed {seed}");
            assert_engine_accepts(&out);
        }
    }

    #[test]
    fn release_follows_the_exchange_time_watermark() {
        let mut hold = HoldBack::new(1_000, 0);
        assert_eq!(hold.push(trade(1_000, 1)), (Admission::Buffered, vec![]));
        assert_eq!(hold.push(trade(1_999, 2)).1, vec![]);
        // 1000 < 2001 - 1000: due.
        assert_eq!(hold.push(trade(2_001, 3)).1, vec![trade(1_000, 1)]);
        assert_eq!(hold.watermark(), Some(t(2_001)));
        assert_eq!(hold.len(), 2);
        // Gaps never move the watermark.
        assert_eq!(
            hold.push(gap(Stream::Trades, 0, 9_999, GapReason::Disconnected))
                .1,
            vec![]
        );
        assert_eq!(hold.watermark(), Some(t(2_001)));
    }

    #[test]
    fn events_later_than_the_hold_back_become_late_gaps() {
        let mut hold = HoldBack::new(1_000, 0);
        let mut out = Vec::new();
        out.extend(hold.push(trade(1_000, 1)).1);
        out.extend(hold.push(trade(3_000, 2)).1);
        assert_eq!(out, vec![trade(1_000, 1)]);
        // An exact repeat of the last released event is a duplicate.
        assert_eq!(hold.push(trade(1_000, 1)), (Admission::Duplicate, vec![]));
        // A liquidation at 900 arrives after 1000 was released. Its gap ends
        // at 1001, below the horizon, so it is due at once.
        let (admission, released) = hold.push(liquidation(900, 1));
        assert_eq!(admission, Admission::Late);
        assert_eq!(
            released,
            vec![gap(Stream::Liquidations, 900, 1_001, GapReason::LateEvent)]
        );
        out.extend(released);
        out.extend(hold.finish());
        assert_eq!(out.last(), Some(&trade(3_000, 2)));
        assert_engine_accepts(&out);
    }

    #[test]
    fn a_late_gap_keeps_its_reason() {
        let mut hold = HoldBack::new(0, 0);
        hold.push(trade(1_000, 1));
        assert_eq!(hold.push(trade(2_000, 2)).1, vec![trade(1_000, 1)]);
        let late = gap(Stream::Trades, 400, 900, GapReason::SequenceBreak);
        let (admission, released) = hold.push(late);
        assert_eq!(admission, Admission::Late);
        assert_eq!(
            released,
            vec![gap(Stream::Trades, 400, 1_001, GapReason::SequenceBreak)]
        );
        assert_eq!(hold.finish(), vec![trade(2_000, 2)]);
    }

    #[test]
    fn disorder_beyond_the_hold_back_still_yields_an_accepted_sequence() {
        let mut sorted = fixture_set();
        sorted.sort();
        for seed in 1..=10 {
            let arrivals = jitter(&sorted, seed, 60);
            let out = run(&mut HoldBack::new(100, 0), arrivals);
            assert!(out.windows(2).all(|w| w[0] < w[1]), "seed {seed}");
            assert_engine_accepts(&out);
            let late = out
                .iter()
                .filter(
                    |e| matches!(e, MarketEvent::FeedGap(g) if g.reason == GapReason::LateEvent),
                )
                .count();
            assert!(late > 0, "seed {seed}: no event was late");
        }
    }

    #[test]
    fn late_open_interest_is_retimed_within_the_allowance() {
        let mut hold = HoldBack::new(2_000, 10_000);
        let mut out = Vec::new();
        for millis in (0..=10_000).step_by(1_000) {
            out.extend(hold.push(mark(millis)).1);
        }
        // Released up to 7000. A sample stamped 3000 (7 s behind the
        // watermark) is delivered at 7001 rather than lost.
        assert_eq!(out.last(), Some(&mark(7_000)));
        let (admission, released) = hold.push(open_interest(3_000));
        assert_eq!(admission, Admission::Retimed);
        assert_eq!(released, vec![open_interest(7_001)]);
        out.extend(released);
        // Beyond the allowance: 7001 - (-5000) > 10 000 ms.
        let (admission, released) = hold.push(open_interest(-5_000));
        assert_eq!(admission, Admission::Late);
        assert_eq!(
            released,
            vec![gap(
                Stream::OpenInterest,
                -5_000,
                7_002,
                GapReason::LateEvent
            )]
        );
        out.extend(released);
        out.extend(hold.finish());
        assert!(out.windows(2).all(|w| w[0] < w[1]));
        assert_engine_accepts(&out);
        // Only open interest is re-timed; other kinds still become gaps.
        let (admission, _) = hold.push(mark(1));
        assert_eq!(admission, Admission::Late);
        // With no allowance the sample is a gap.
        let mut strict = HoldBack::new(2_000, 0);
        for millis in (0..=10_000).step_by(1_000) {
            strict.push(mark(millis));
        }
        assert_eq!(strict.push(open_interest(3_000)).0, Admission::Late);
    }

    fn open_interest_of(millis: i64, value_units: i64) -> MarketEvent {
        MarketEvent::OpenInterest(OpenInterest {
            time: t(millis),
            open_interest: Qty::from_units(value_units),
            resolution_ms: 10_000,
        })
    }

    #[test]
    fn a_stale_open_interest_snapshot_is_a_gap_not_the_current_value() {
        let mut hold = HoldBack::new(2_000, 10_000);
        let mut out = Vec::new();
        out.extend(hold.push(mark(11_000)).1);
        // OI 100 stamped 12001 is admitted on time.
        out.extend(hold.push(open_interest_of(12_001, 100)).1);
        for millis in [13_000, 14_000, 15_000] {
            out.extend(hold.push(mark(millis)).1);
        }
        assert_eq!(out.last(), Some(&open_interest_of(12_001, 100)));
        // An older snapshot (OI 200 stamped 9000) arrives late. Within the
        // allowance, but not newer than 12001: a gap, never re-timed.
        let (admission, released) = hold.push(open_interest_of(9_000, 200));
        assert_eq!(admission, Admission::Late);
        assert_eq!(
            released,
            vec![gap(
                Stream::OpenInterest,
                9_000,
                12_002,
                GapReason::LateEvent
            )]
        );
        out.extend(released);
        out.extend(hold.finish());
        let current = out.iter().rev().find_map(|e| match e {
            MarketEvent::OpenInterest(oi) => Some(*oi),
            _ => None,
        });
        assert_eq!(current.map(|oi| oi.open_interest.units()), Some(100));
        assert_engine_accepts(&out);

        // An equal exchange time is not newer either.
        let mut hold = HoldBack::new(0, 10_000);
        hold.push(open_interest_of(5_000, 1));
        hold.push(mark(6_000));
        hold.push(mark(7_000));
        assert_eq!(hold.push(open_interest_of(5_000, 2)).0, Admission::Late);
        // A strictly newer one is re-timed.
        assert_eq!(hold.push(open_interest_of(5_001, 3)).0, Admission::Retimed);
    }

    #[test]
    fn buffered_duplicates_are_dropped() {
        let mut hold = HoldBack::new(1_000, 0);
        assert_eq!(hold.push(mark(1_000)).0, Admission::Buffered);
        assert_eq!(hold.push(mark(1_000)).0, Admission::Duplicate);
        assert_eq!(hold.finish(), vec![mark(1_000)]);
        assert!(hold.is_empty());
    }
}
