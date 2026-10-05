//! The canonical order of market events (ADR-028).
//!
//! Events from different streams carry no common sequence, and archive data
//! has no receive times. The order is therefore built from exchange fields
//! only, as the total order
//!
//! `(ordering time, kind rank, per-stream sequence id)`, then the payload,
//!
//! where the payload comparison is the derived structural order of the event
//! struct (field declaration order). The delivered sequence is a pure function
//! of the event *set*: live capture, archive import and any correct merge
//! algorithm (stable sort, unstable sort, k-way heap merge) produce the same
//! sequence. [`MarketStateEngine`](crate::state::MarketStateEngine) requires
//! it to be strictly increasing.
//!
//! | Kind (rank)            | Ordering time             | Sequence id           |
//! |------------------------|---------------------------|-----------------------|
//! | `FeedGap` (0)          | gap end                   | ordinal of the stream |
//! | `BookSnapshot` (1)     | snapshot transaction time | last update id        |
//! | `Trade` (2)            | trade time                | trade id              |
//! | `Liquidation` (3)      | liquidation trade time    | 0                     |
//! | `BookUpdate` (4)       | diff transaction time     | last update id        |
//! | `MarkPrice` (5)        | exchange event time       | 0                     |
//! | `FundingSettlement` (6)| funding time              | 0                     |
//! | `OpenInterest` (7)     | publication time          | 0                     |
//! | `Kline` (8)            | close time                | 0                     |
//!
//! Changing any of this needs a superseding ADR and invalidates recorded
//! equivalence hashes.

use crate::event::MarketEvent;
use crate::time::EventTime;
use std::cmp::Ordering;
use std::fmt;

/// The kind of a market event; its discriminant is the same-millisecond rank
/// (ADR-028).
///
/// Continuity loss first, then resets, then executions, then the book changes
/// they cause, then periodic observations, then aggregates. A snapshot ranks
/// before an update so that an update straddling the snapshot's last update id
/// is applied after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(u8)]
pub enum EventKind {
    /// [`MarketEvent::FeedGap`].
    FeedGap = 0,
    /// [`MarketEvent::BookSnapshot`].
    BookSnapshot = 1,
    /// [`MarketEvent::Trade`].
    Trade = 2,
    /// [`MarketEvent::Liquidation`].
    Liquidation = 3,
    /// [`MarketEvent::BookUpdate`].
    BookUpdate = 4,
    /// [`MarketEvent::MarkPrice`].
    MarkPrice = 5,
    /// [`MarketEvent::FundingSettlement`].
    FundingSettlement = 6,
    /// [`MarketEvent::OpenInterest`].
    OpenInterest = 7,
    /// [`MarketEvent::Kline`].
    Kline = 8,
}

/// The exchange-derived prefix of the canonical order (ADR-028).
///
/// The field order is the priority: time, then kind rank, then sequence id.
/// Distinct events may share a key (for example two mark prices in one
/// millisecond); [`Ord for MarketEvent`](MarketEvent) breaks such ties on the
/// payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct CanonicalKey {
    /// Ordering time of the event.
    pub time: EventTime,
    /// Kind rank.
    pub kind: EventKind,
    /// Per-stream sequence id; 0 for kinds without one.
    pub seq: u64,
}

impl fmt::Display for CanonicalKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {:?} seq {}", self.time, self.kind, self.seq)
    }
}

impl MarketEvent {
    /// The event's canonical key (ADR-028).
    pub fn canonical_key(&self) -> CanonicalKey {
        let seq = match self {
            Self::FeedGap(gap) => u64::from(gap.stream as u8),
            Self::BookSnapshot(snapshot) => snapshot.last_update_id,
            Self::Trade(trade) => trade.trade_id,
            Self::BookUpdate(update) => update.last_update_id,
            Self::Liquidation(_)
            | Self::MarkPrice(_)
            | Self::FundingSettlement(_)
            | Self::OpenInterest(_)
            | Self::Kline(_) => 0,
        };
        CanonicalKey {
            time: self.time(),
            kind: self.kind(),
            seq,
        }
    }

    /// Structural order of two payloads. Only reached with equal keys, which
    /// implies equal kinds; the cross-kind arms keep the function total and
    /// consistent with the key all the same.
    fn cmp_payload(&self, other: &Self) -> Ordering {
        let cross_kind = || self.kind().cmp(&other.kind());
        match self {
            Self::FeedGap(a) => match other {
                Self::FeedGap(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::BookSnapshot(a) => match other {
                Self::BookSnapshot(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::Trade(a) => match other {
                Self::Trade(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::Liquidation(a) => match other {
                Self::Liquidation(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::BookUpdate(a) => match other {
                Self::BookUpdate(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::MarkPrice(a) => match other {
                Self::MarkPrice(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::FundingSettlement(a) => match other {
                Self::FundingSettlement(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::OpenInterest(a) => match other {
                Self::OpenInterest(b) => a.cmp(b),
                _ => cross_kind(),
            },
            Self::Kline(a) => match other {
                Self::Kline(b) => a.cmp(b),
                _ => cross_kind(),
            },
        }
    }
}

/// The canonical order (ADR-028): [`MarketEvent::canonical_key`], then the
/// payload. Total and consistent with `==`.
impl Ord for MarketEvent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.canonical_key()
            .cmp(&other.canonical_key())
            .then_with(|| self.cmp_payload(other))
    }
}

impl PartialOrd for MarketEvent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::samples::{
        RANKED_KINDS, gap, kline, liquidation, mark, of_kind, one_of_each, open_interest,
        settlement, snapshot, t, trade, update,
    };
    use crate::event::{GapReason, Stream};
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    /// Deterministic 64-bit LCG (Knuth's MMIX constants) for shuffles.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            usize::try_from((self.0 >> 33) % u64::try_from(n).unwrap()).unwrap()
        }

        fn shuffle<T>(&mut self, items: &mut [T]) {
            for i in (1..items.len()).rev() {
                items.swap(i, self.below(i + 1));
            }
        }
    }

    fn sorted(mut events: Vec<MarketEvent>) -> Vec<MarketEvent> {
        events.sort();
        events
    }

    fn key(millis: i64, kind: EventKind, seq: u64) -> CanonicalKey {
        CanonicalKey {
            time: t(millis),
            kind,
            seq,
        }
    }

    /// At least 20 events, including distinct events that share a key.
    fn totality_fixture() -> Vec<MarketEvent> {
        let mut events = one_of_each(1_000);
        events.extend([
            // Same key, distinct payload: two id-less mark prices.
            mark(1_000, 6_354_100_000_000),
            mark(1_000, 6_354_200_000_000),
            // Same stream and end, different start and reason.
            gap(Stream::Trades, 400, 1_000, GapReason::SequenceBreak),
            gap(Stream::Trades, 900, 1_000, GapReason::Disconnected),
            // Gaps on other streams at the same end.
            gap(Stream::OrderBook, 900, 1_000, GapReason::Disconnected),
            gap(Stream::Klines, 1_000, 1_000, GapReason::MissingData),
            // 1m and 5m klines closing together.
            kline(1_000 - 299_999, 1_000),
            // Two liquidations in one millisecond.
            liquidation(1_000, 20_000_000),
            liquidation(1_000, 30_000_000),
            settlement(1_000, -5_000),
            open_interest(1_000, 5_000),
            trade(1_000, 3),
            trade(1_000, 9),
            update(1_000, 106, 110, 105),
            update(1_000, 90, 99, 89),
            snapshot(1_000, 98),
            trade(999, 99),
            trade(1_001, 1),
            kline(-59_000, 1_000_000),
            gap(Stream::Funding, -10, -5, GapReason::LateEvent),
        ]);
        events
    }

    #[test]
    fn kind_rank_is_pinned() {
        let mut ranked = RANKED_KINDS;
        ranked.sort();
        assert_eq!(ranked, RANKED_KINDS);
        for (rank, kind) in RANKED_KINDS.iter().enumerate() {
            assert_eq!(usize::from(*kind as u8), rank, "{kind:?}");
        }
        assert_eq!(RANKED_KINDS.len(), 9);
    }

    #[test]
    fn stream_ordinals_are_pinned() {
        let streams = [
            Stream::Trades,
            Stream::OrderBook,
            Stream::Liquidations,
            Stream::MarkPrice,
            Stream::Funding,
            Stream::OpenInterest,
            Stream::Klines,
        ];
        let mut ordered = streams;
        ordered.sort();
        assert_eq!(ordered, streams);
        for (ordinal, stream) in streams.iter().enumerate() {
            assert_eq!(usize::from(*stream as u8), ordinal, "{stream:?}");
        }
    }

    #[test]
    fn key_fields_have_priority_in_declaration_order() {
        // Time beats kind and seq.
        assert!(key(999, EventKind::Kline, u64::MAX) < key(1_000, EventKind::FeedGap, 0));
        // Kind beats seq.
        assert!(key(1_000, EventKind::BookSnapshot, u64::MAX) < key(1_000, EventKind::Trade, 0));
        // Seq decides last.
        assert!(key(1_000, EventKind::Trade, 1) < key(1_000, EventKind::Trade, 2));

        // The same on events: a kline one millisecond earlier sorts before a gap.
        assert!(kline(0, 999) < gap(Stream::Trades, 0, 1_000, GapReason::Disconnected));
        // A snapshot with a huge update id still sorts before a trade with id 1.
        assert!(snapshot(1_000, u64::MAX) < trade(1_000, 1));
        // With time and kind tied, the trade id decides.
        assert!(trade(1_000, 1) < trade(1_000, 2));
    }

    #[test]
    fn same_millisecond_kinds_sort_in_rank_order() {
        let golden = one_of_each(5_000);
        let kinds: Vec<EventKind> = golden.iter().map(MarketEvent::kind).collect();
        assert_eq!(
            kinds,
            [
                EventKind::FeedGap,
                EventKind::BookSnapshot,
                EventKind::Trade,
                EventKind::Liquidation,
                EventKind::BookUpdate,
                EventKind::MarkPrice,
                EventKind::FundingSettlement,
                EventKind::OpenInterest,
                EventKind::Kline,
            ]
        );
        assert_eq!(sorted(golden.iter().rev().cloned().collect()), golden);
        let mut lcg = Lcg(7);
        for _ in 0..1_000 {
            let mut shuffled = golden.clone();
            lcg.shuffle(&mut shuffled);
            assert_eq!(sorted(shuffled), golden);
        }
    }

    #[test]
    fn trades_sort_by_id_and_book_updates_by_last_update_id() {
        let trades = sorted(vec![trade(1_000, 5), trade(1_000, 3), trade(1_000, 4)]);
        assert_eq!(trades, [trade(1_000, 3), trade(1_000, 4), trade(1_000, 5)]);

        let updates = sorted(vec![
            update(1_000, 111, 120, 110),
            update(1_000, 101, 110, 100),
            update(1_000, 121, 121, 120),
        ]);
        assert_eq!(
            updates,
            [
                update(1_000, 101, 110, 100),
                update(1_000, 111, 120, 110),
                update(1_000, 121, 121, 120),
            ]
        );
    }

    #[test]
    fn snapshot_precedes_an_update_straddling_its_last_update_id() {
        // The update covers ids 95..=105 and the snapshot ends at 100; the
        // update's lower last id must not pull it ahead of the reset.
        assert!(snapshot(1_000, 100) < update(1_000, 95, 105, 94));
        assert!(snapshot(1_000, 100) < update(1_000, 50, 60, 49));
    }

    #[test]
    fn gap_precedes_the_resumed_stream_and_kline_follows_its_close() {
        let resumed_gap = gap(Stream::Trades, 1_000, 2_000, GapReason::Disconnected);
        assert!(resumed_gap < trade(2_000, 0));
        assert!(resumed_gap < snapshot(2_000, 0));
        assert!(trade(1_999, u64::MAX) < resumed_gap);

        assert!(trade(60_000, u64::MAX) < kline(1, 60_000));
        assert!(kline(1, 60_000) < trade(60_001, 0));
    }

    #[test]
    fn order_is_total_and_consistent_with_eq() {
        let events = totality_fixture();
        assert!(events.len() >= 20);
        let distinct: std::collections::BTreeSet<&MarketEvent> = events.iter().collect();
        assert_eq!(distinct.len(), events.len(), "fixture events are distinct");

        let mut same_key_pairs = 0;
        for a in &events {
            for b in &events {
                let ab = a.cmp(b);
                assert_eq!(ab, b.cmp(a).reverse(), "antisymmetry {a:?} {b:?}");
                assert_eq!(ab == Ordering::Equal, a == b, "Eq consistency {a:?} {b:?}");
                assert_eq!(a.partial_cmp(b), Some(ab));
                // The key is a prefix of the order.
                let key_order = a.canonical_key().cmp(&b.canonical_key());
                if key_order != Ordering::Equal {
                    assert_eq!(ab, key_order, "key prefix {a:?} {b:?}");
                } else if a != b {
                    same_key_pairs += 1;
                }
            }
        }
        // Ordered pairs of distinct same-key events: marks (3 events: 6),
        // trade-stream gaps (3: 6), liquidations (3: 6), klines (2: 2),
        // settlements (2: 2), open interest (2: 2).
        assert_eq!(same_key_pairs, 24);

        for a in &events {
            for b in &events {
                for c in &events {
                    if a <= b && b <= c {
                        assert!(a <= c, "transitivity {a:?} {b:?} {c:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn every_permutation_sorts_to_one_sequence() {
        let base = vec![
            update(1_000, 101, 105, 100),
            trade(1_000, 7),
            gap(Stream::OrderBook, 900, 1_000, GapReason::SequenceBreak),
            mark(1_000, 6_354_100_000_000),
            mark(1_000, 6_354_000_000_000),
            kline(941, 1_000),
            trade(999, 6),
        ];
        let expected = sorted(base.clone());

        // Heap's algorithm, iterative.
        let mut items = base;
        let mut counters = [0_usize; 7];
        let mut permutations = 1;
        let check = |items: &[MarketEvent]| {
            assert_eq!(sorted(items.to_vec()), expected);
            let mut unstable = items.to_vec();
            unstable.sort_unstable();
            assert_eq!(unstable, expected);
        };
        check(&items);
        let mut i = 0;
        while i < items.len() {
            if counters[i] < i {
                let j = if i % 2 == 0 { 0 } else { counters[i] };
                items.swap(j, i);
                check(&items);
                permutations += 1;
                counters[i] += 1;
                i = 0;
            } else {
                counters[i] = 0;
                i += 1;
            }
        }
        assert_eq!(permutations, 5_040);
    }

    #[test]
    fn shuffles_of_a_mixed_tape_sort_to_one_sequence() {
        let mut tape = totality_fixture();
        tape.extend(one_of_each(3_000));
        tape.extend([trade(2_000, 50), trade(2_000, 51), mark(2_500, 1)]);
        assert!(tape.len() >= 40);
        let expected = sorted(tape.clone());
        assert!(
            expected.windows(2).all(|w| w[0] < w[1]),
            "strictly increasing"
        );

        let mut lcg = Lcg(0x6d69_6500_0000_0028);
        for _ in 0..1_000 {
            lcg.shuffle(&mut tape);
            assert_eq!(sorted(tape.clone()), expected);
        }
    }

    #[test]
    fn k_way_heap_merge_of_streams_equals_the_global_sort() {
        let mut tape = totality_fixture();
        tape.extend(one_of_each(3_000));
        let expected = sorted(tape.clone());

        // Per-stream recordings, each sorted on its own.
        let streams = [
            Stream::Trades,
            Stream::OrderBook,
            Stream::Liquidations,
            Stream::MarkPrice,
            Stream::Funding,
            Stream::OpenInterest,
            Stream::Klines,
        ];
        let recordings: Vec<Vec<MarketEvent>> = streams
            .iter()
            .map(|&stream| {
                sorted(
                    tape.iter()
                        .filter(|e| e.stream() == stream)
                        .cloned()
                        .collect(),
                )
            })
            .collect();
        assert_eq!(recordings.iter().map(Vec::len).sum::<usize>(), tape.len());

        let mut cursors = vec![0_usize; recordings.len()];
        let mut heap = BinaryHeap::new();
        for (index, recording) in recordings.iter().enumerate() {
            if let Some(head) = recording.first() {
                heap.push(Reverse((head.clone(), index)));
                cursors[index] = 1;
            }
        }
        let mut merged = Vec::new();
        while let Some(Reverse((event, index))) = heap.pop() {
            merged.push(event);
            if let Some(next) = recordings[index].get(cursors[index]) {
                heap.push(Reverse((next.clone(), index)));
                cursors[index] += 1;
            }
        }
        assert_eq!(merged, expected);
    }

    #[test]
    fn accessors_cover_every_kind() {
        use EventKind as K;
        let rows: [(EventKind, i64, Stream, u64); 9] = [
            (K::FeedGap, 1_000, Stream::Trades, 0),
            (K::BookSnapshot, 1_000, Stream::OrderBook, 100),
            (K::Trade, 1_000, Stream::Trades, 7),
            (K::Liquidation, 1_000, Stream::Liquidations, 0),
            (K::BookUpdate, 1_000, Stream::OrderBook, 105),
            (K::MarkPrice, 1_000, Stream::MarkPrice, 0),
            (K::FundingSettlement, 1_000, Stream::Funding, 0),
            (K::OpenInterest, 1_000, Stream::OpenInterest, 0),
            (K::Kline, 1_000, Stream::Klines, 0),
        ];
        for (kind, millis, stream, seq) in rows {
            let event = of_kind(kind, 1_000);
            assert_eq!(event.kind(), kind);
            assert_eq!(event.time(), t(millis), "{kind:?}");
            assert_eq!(event.stream(), stream, "{kind:?}");
            assert_eq!(event.canonical_key(), key(millis, kind, seq), "{kind:?}");
        }

        // Kinds whose ordering time or seq is not the obvious field.
        let late_gap = gap(Stream::Klines, 10, 20, GapReason::LateEvent);
        assert_eq!(late_gap.time(), t(20));
        assert_eq!(late_gap.stream(), Stream::Klines);
        assert_eq!(late_gap.canonical_key(), key(20, K::FeedGap, 6));
        assert_eq!(kline(0, 59_999).time(), t(59_999));
        assert_eq!(snapshot(5, 42).canonical_key(), key(5, K::BookSnapshot, 42));
        assert_eq!(
            update(5, 40, 44, 39).canonical_key(),
            key(5, K::BookUpdate, 44)
        );
    }

    #[test]
    fn canonical_key_displays_its_fields() {
        assert_eq!(
            key(1_000, EventKind::Trade, 7).to_string(),
            "1000ms Trade seq 7"
        );
    }
}
