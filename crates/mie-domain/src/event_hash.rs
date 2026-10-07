//! The event-stream hash: one stable identity for a delivered sequence of
//! market events (ADR-039 D7).
//!
//! Replaying the same window twice must yield the same sequence (#11), and
//! the equivalence harness (#13) compares live and replay deliveries. Both
//! compare an [`EventStreamHash`]: the number of events plus a FNV-1a 64
//! [`Fingerprint`] over an explicit byte encoding of every event in delivery
//! order, written with the [`Fingerprinter`] writers (ADR-029). The order is
//! part of the hash: the same events in another order hash differently.
//!
//! **Encoding v1.** A header, then one record per event:
//!
//! | Part | Bytes |
//! |---|---|
//! | header | `write_str("mie-event-stream")`, then `write_u32(1)` (the encoding version) |
//! | kind | `write_u8` of the ADR-028 kind rank ([`EventKind`] discriminant) |
//! | payload | every field in declaration order, as below |
//!
//! | Field type | Writer |
//! |---|---|
//! | `EventTime` | `write_i64` of the epoch milliseconds |
//! | `Price`, `Qty`, `Rate` | `write_i64` of the `10^-8` units (ADR-027) |
//! | exchange ids, counts (`u64`) | `write_u64` |
//! | `u32` (open-interest resolution) | `write_u32` |
//! | `Vec<Level>` | `write_len` of the level count, then price and quantity per level |
//! | [`Stream`] | `write_u8` of its ADR-028 ordinal |
//! | [`Aggressor`] | `write_u8`: `Buy` 0, `Sell` 1 |
//! | [`GapReason`] | `write_u8`: `Disconnected` 0, `SequenceBreak` 1, `LateEvent` 2, `MissingData` 3 |
//!
//! `Aggressor` and `GapReason` go through an explicit match, never through
//! their discriminants, which no ADR freezes. Changing any part of this
//! encoding is a new encoding version and a new ADR: recorded hashes become
//! incomparable.

use crate::event::{Aggressor, GapReason, Level, MarketEvent, Stream};
use crate::fingerprint::{Fingerprint, Fingerprinter};
use std::fmt;

/// The encoding version written into the header.
pub const ENCODING_VERSION: u32 = 1;

/// The identity of a delivered event sequence: its length and its
/// fingerprint. Displays as `<events>:<16 lowercase hex digits>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EventStreamHash {
    /// Number of events hashed.
    pub events: u64,
    /// Fingerprint of the encoding (module docs).
    pub fingerprint: Fingerprint,
}

impl fmt::Display for EventStreamHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.events, self.fingerprint)
    }
}

/// Hashes a sequence of events in delivery order (module docs).
#[derive(Debug, Clone)]
pub struct EventStreamHasher {
    fingerprinter: Fingerprinter,
    events: u64,
}

impl Default for EventStreamHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStreamHasher {
    /// Starts a hash: writes the header.
    pub fn new() -> Self {
        let mut fingerprinter = Fingerprinter::new();
        fingerprinter.write_str("mie-event-stream");
        fingerprinter.write_u32(ENCODING_VERSION);
        Self {
            fingerprinter,
            events: 0,
        }
    }

    /// Adds the next delivered event.
    pub fn push(&mut self, event: &MarketEvent) {
        let f = &mut self.fingerprinter;
        f.write_u8(event.kind() as u8);
        match event {
            MarketEvent::FeedGap(gap) => {
                f.write_u8(stream_ordinal(gap.stream));
                f.write_i64(gap.start.as_millis());
                f.write_i64(gap.end.as_millis());
                f.write_u8(reason_code(gap.reason));
            }
            MarketEvent::BookSnapshot(snapshot) => {
                f.write_i64(snapshot.time.as_millis());
                f.write_u64(snapshot.last_update_id);
                write_levels(f, &snapshot.bids);
                write_levels(f, &snapshot.asks);
            }
            MarketEvent::Trade(trade) => {
                f.write_i64(trade.time.as_millis());
                f.write_u64(trade.trade_id);
                f.write_i64(trade.price.units());
                f.write_i64(trade.qty.units());
                f.write_u8(aggressor_code(trade.aggressor));
            }
            MarketEvent::Liquidation(liquidation) => {
                f.write_i64(liquidation.time.as_millis());
                f.write_u8(aggressor_code(liquidation.aggressor));
                f.write_i64(liquidation.price.units());
                f.write_i64(liquidation.avg_price.units());
                f.write_i64(liquidation.filled_qty.units());
            }
            MarketEvent::BookUpdate(update) => {
                f.write_i64(update.time.as_millis());
                f.write_u64(update.first_update_id);
                f.write_u64(update.last_update_id);
                f.write_u64(update.prev_update_id);
                write_levels(f, &update.bids);
                write_levels(f, &update.asks);
            }
            MarketEvent::MarkPrice(mark) => {
                f.write_i64(mark.time.as_millis());
                f.write_i64(mark.mark_price.units());
                f.write_i64(mark.index_price.units());
                f.write_i64(mark.funding_rate.units());
                f.write_i64(mark.next_funding_time.as_millis());
            }
            MarketEvent::FundingSettlement(settlement) => {
                f.write_i64(settlement.time.as_millis());
                f.write_i64(settlement.rate.units());
            }
            MarketEvent::OpenInterest(oi) => {
                f.write_i64(oi.time.as_millis());
                f.write_i64(oi.open_interest.units());
                f.write_u32(oi.resolution_ms);
            }
            MarketEvent::Kline(kline) => {
                f.write_i64(kline.open_time.as_millis());
                f.write_i64(kline.close_time.as_millis());
                f.write_i64(kline.open.units());
                f.write_i64(kline.high.units());
                f.write_i64(kline.low.units());
                f.write_i64(kline.close.units());
                f.write_i64(kline.volume.units());
                f.write_i64(kline.taker_buy_volume.units());
                f.write_u64(kline.trade_count);
            }
        }
        self.events += 1;
    }

    /// The hash of everything pushed so far.
    pub fn finish(&self) -> EventStreamHash {
        EventStreamHash {
            events: self.events,
            fingerprint: self.fingerprinter.finish(),
        }
    }
}

/// The ADR-028 ordinal of a stream: its frozen discriminant.
fn stream_ordinal(stream: Stream) -> u8 {
    stream as u8
}

fn aggressor_code(aggressor: Aggressor) -> u8 {
    match aggressor {
        Aggressor::Buy => 0,
        Aggressor::Sell => 1,
    }
}

fn reason_code(reason: GapReason) -> u8 {
    match reason {
        GapReason::Disconnected => 0,
        GapReason::SequenceBreak => 1,
        GapReason::LateEvent => 2,
        GapReason::MissingData => 3,
    }
}

fn write_levels(f: &mut Fingerprinter, levels: &[Level]) {
    f.write_len(levels.len());
    for level in levels {
        f.write_i64(level.price.units());
        f.write_i64(level.qty.units());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::samples::{gap, one_of_each, trade};
    use crate::event::{FeedGap, Trade};
    use crate::num::{Price, Qty};
    use crate::time::EventTime;

    fn hash(events: &[MarketEvent]) -> EventStreamHash {
        let mut hasher = EventStreamHasher::new();
        for event in events {
            hasher.push(event);
        }
        hasher.finish()
    }

    #[test]
    fn the_golden_values_of_encoding_v1_are_pinned() {
        // Changing either value is a new encoding version (module docs).
        assert_eq!(hash(&[]).to_string(), "0:5abe9be5e2d301cf");
        assert_eq!(hash(&one_of_each(1_000)).to_string(), "9:324b1237324a4510");
    }

    #[test]
    fn the_header_is_written_through_the_fingerprinter() {
        let mut expected = Fingerprinter::new();
        expected.write_str("mie-event-stream");
        expected.write_u32(1);
        assert_eq!(
            EventStreamHasher::default().finish().fingerprint,
            expected.finish()
        );
    }

    fn trade_with(edit: impl FnOnce(&mut Trade)) -> MarketEvent {
        let MarketEvent::Trade(mut t) = trade(1_000, 7) else {
            unreachable!()
        };
        edit(&mut t);
        MarketEvent::Trade(t)
    }

    fn gap_with(edit: impl FnOnce(&mut FeedGap)) -> MarketEvent {
        let MarketEvent::FeedGap(mut g) = gap(Stream::Trades, 500, 1_000, GapReason::Disconnected)
        else {
            unreachable!()
        };
        edit(&mut g);
        MarketEvent::FeedGap(g)
    }

    #[test]
    fn every_field_and_the_order_count() {
        // `one_of_each` ranks the gap first and the trade third (ADR-028).
        let base = one_of_each(1_000);
        let reference = hash(&base);
        let mut swapped = base.clone();
        swapped.swap(2, 3);
        assert_ne!(hash(&swapped).fingerprint, reference.fingerprint);
        assert_eq!(hash(&swapped).events, reference.events);

        let edits: Vec<(usize, MarketEvent)> = vec![
            (2, trade_with(|t| t.time = EventTime::from_millis(1_001))),
            (2, trade_with(|t| t.trade_id = 8)),
            (2, trade_with(|t| t.price = Price::from_units(1))),
            (2, trade_with(|t| t.qty = Qty::from_units(1))),
            (2, trade_with(|t| t.aggressor = Aggressor::Sell)),
            (0, gap_with(|g| g.stream = Stream::Klines)),
            (0, gap_with(|g| g.start = EventTime::from_millis(501))),
            (0, gap_with(|g| g.end = EventTime::from_millis(999))),
            (0, gap_with(|g| g.reason = GapReason::SequenceBreak)),
            (0, gap_with(|g| g.reason = GapReason::LateEvent)),
            (0, gap_with(|g| g.reason = GapReason::MissingData)),
        ];
        let mut seen = vec![reference];
        for (index, edited) in edits {
            let mut events = base.clone();
            assert_ne!(events[index], edited);
            events[index] = edited;
            let h = hash(&events);
            assert!(!seen.contains(&h), "{events:?}");
            seen.push(h);
        }
    }

    #[test]
    fn displays_the_count_and_the_fingerprint() {
        let h = EventStreamHash {
            events: 42,
            fingerprint: Fingerprint::from_raw(0xab),
        };
        assert_eq!(h.to_string(), "42:00000000000000ab");
    }
}
