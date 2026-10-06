//! Per-stream continuity: dedupe by exchange id and explicit feed gaps
//! (ADR-026 consequences, ADR-028 D4/D5).
//!
//! One [`StreamSequencer`] per captured stream sees that stream's events in
//! arrival order, together with the capture session (connection) each came
//! from:
//!
//! - **Dedupe**: a trade whose id is at or below the last delivered id is
//!   dropped (equal: duplicate, lower: regression). An id-less event is
//!   dropped only when it exactly repeats the stream's last delivered event.
//! - **Session change**: the first event of a new session is preceded by a
//!   `Disconnected` gap from the last delivered event to it — always, even
//!   when trade ids happen to be contiguous, so every reconnect is visible.
//! - **Sequence break**: within one session, a trade id other than
//!   `last + 1` is preceded by a `SequenceBreak` gap between the two trades.
//! - **Seed**: the first event of a run is preceded by a `Disconnected` gap
//!   from the previous run's last persisted event time, which turns any
//!   restart (and the crash loss of ADR-030 D5) into a gap.
//! - **Unparsable sample**: a record of an id-less stream that fails
//!   normalization is replaced by a `MissingData` gap
//!   ([`StreamSequencer::missing`]), so the lost sample is visible in band.
//!   On trades the next id break already reports a lost trade.
//!
//! A gap is ordered at its `end`, the time of the event it precedes, and
//! ranks first in that millisecond (ADR-028 D4). Gap starts are clamped to
//! the end, so every gap is well formed.

use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::time::EventTime;

/// Where the previous run of a stream stopped: the largest event time it
/// persisted (from the sealed manifests).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Seed {
    /// Largest persisted record event time of the stream.
    pub last_event_time: EventTime,
}

/// Continuity tracking of one stream.
#[derive(Debug, Clone)]
pub struct StreamSequencer {
    stream: Stream,
    seed: Option<Seed>,
    /// Session of the last delivered event.
    session: Option<String>,
    /// The last delivered event (never a gap).
    last: Option<MarketEvent>,
    last_trade_id: Option<u64>,
    duplicates: u64,
    regressions: u64,
}

impl StreamSequencer {
    /// A sequencer for the domain series `stream`, optionally seeded with the
    /// previous run's end.
    pub fn new(stream: Stream, seed: Option<Seed>) -> Self {
        Self {
            stream,
            seed,
            session: None,
            last: None,
            last_trade_id: None,
            duplicates: 0,
            regressions: 0,
        }
    }

    /// Takes the next event of the stream, received in `session_id`, and
    /// returns what to deliver: nothing (dropped), the event, or a gap
    /// followed by the event.
    pub fn push(&mut self, session_id: &str, event: MarketEvent) -> Vec<MarketEvent> {
        if self.is_repeat(&event) {
            return Vec::new();
        }
        let end = event.time();
        let gap = match (&self.last, &self.session) {
            (None, _) => self
                .seed
                .map(|seed| (seed.last_event_time, GapReason::Disconnected)),
            (Some(last), Some(session)) if session != session_id => {
                Some((last.time(), GapReason::Disconnected))
            }
            (Some(MarketEvent::Trade(last)), _) => match &event {
                MarketEvent::Trade(trade)
                    if Some(trade.trade_id) != last.trade_id.checked_add(1) =>
                {
                    Some((last.time, GapReason::SequenceBreak))
                }
                _ => None,
            },
            (Some(_), _) => None,
        };
        let mut out = Vec::with_capacity(2);
        if let Some((start, reason)) = gap {
            out.push(MarketEvent::FeedGap(FeedGap {
                stream: self.stream,
                start: start.min(end),
                end,
                reason,
            }));
        }
        if let MarketEvent::Trade(trade) = &event {
            self.last_trade_id = Some(trade.trade_id);
        }
        self.session = Some(session_id.to_owned());
        self.last = Some(event.clone());
        out.push(event);
        out
    }

    /// Whether `event` repeats what was already delivered; counts it.
    fn is_repeat(&mut self, event: &MarketEvent) -> bool {
        if let (MarketEvent::Trade(trade), Some(last_id)) = (event, self.last_trade_id) {
            if trade.trade_id == last_id {
                self.duplicates += 1;
                return true;
            }
            if trade.trade_id < last_id {
                self.regressions += 1;
                return true;
            }
            return false;
        }
        if self.last.as_ref() == Some(event) {
            self.duplicates += 1;
            return true;
        }
        false
    }

    /// Events dropped as repeats of a delivered id or event.
    pub fn duplicates(&self) -> u64 {
        self.duplicates
    }

    /// Trades dropped because their id fell below the last delivered id.
    pub fn regressions(&self) -> u64 {
        self.regressions
    }

    /// The gap that stands in for a record of this stream that failed
    /// normalization: `MissingData` from the last delivered event (else the
    /// seed, else `end`) to `end`, the record's raw event time. The
    /// sequencer's state does not change.
    pub fn missing(&self, end: EventTime) -> MarketEvent {
        let start = self
            .last_time()
            .or(self.seed.map(|seed| seed.last_event_time))
            .unwrap_or(end);
        MarketEvent::FeedGap(FeedGap {
            stream: self.stream,
            start: start.min(end),
            end,
            reason: GapReason::MissingData,
        })
    }

    /// Ordering time of the last delivered event.
    pub fn last_time(&self) -> Option<EventTime> {
        self.last.as_ref().map(MarketEvent::time)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::event::{Aggressor, Liquidation, Trade};
    use mie_domain::num::{Price, Qty};

    fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    fn trade(millis: i64, id: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id: id,
            price: Price::from_units(8_500_000_000_000),
            qty: Qty::from_units(1_000_000),
            aggressor: Aggressor::Buy,
        })
    }

    fn liquidation(millis: i64, filled: i64) -> MarketEvent {
        MarketEvent::Liquidation(Liquidation {
            time: t(millis),
            aggressor: Aggressor::Sell,
            price: Price::from_units(8_400_000_000_000),
            avg_price: Price::from_units(8_400_000_000_000),
            filled_qty: Qty::from_units(filled),
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

    #[test]
    fn id_skip_is_a_sequence_break_between_the_trades() {
        let mut seq = StreamSequencer::new(Stream::Trades, None);
        assert_eq!(seq.push("s1", trade(1_000, 100)), [trade(1_000, 100)]);
        assert_eq!(seq.push("s1", trade(1_010, 101)), [trade(1_010, 101)]);
        assert_eq!(
            seq.push("s1", trade(1_030, 103)),
            [
                gap(Stream::Trades, 1_010, 1_030, GapReason::SequenceBreak),
                trade(1_030, 103)
            ]
        );
    }

    #[test]
    fn repeated_and_regressing_ids_are_dropped_and_counted() {
        let mut seq = StreamSequencer::new(Stream::Trades, None);
        seq.push("s1", trade(1_000, 100));
        seq.push("s1", trade(1_010, 101));
        // Same id, different payload: still a duplicate.
        assert!(seq.push("s1", trade(1_011, 101)).is_empty());
        assert!(seq.push("s1", trade(990, 99)).is_empty());
        assert_eq!((seq.duplicates(), seq.regressions()), (1, 1));
        // The next contiguous id continues without a gap.
        assert_eq!(seq.push("s1", trade(1_020, 102)), [trade(1_020, 102)]);
    }

    #[test]
    fn session_change_is_a_disconnect_even_with_contiguous_ids() {
        let mut seq = StreamSequencer::new(Stream::Trades, None);
        seq.push("s1", trade(1_000, 100));
        assert_eq!(
            seq.push("s2", trade(4_000, 101)),
            [
                gap(Stream::Trades, 1_000, 4_000, GapReason::Disconnected),
                trade(4_000, 101)
            ]
        );
        // A skip at the reconnect is reported once, as the disconnect.
        assert_eq!(
            seq.push("s3", trade(9_000, 250)),
            [
                gap(Stream::Trades, 4_000, 9_000, GapReason::Disconnected),
                trade(9_000, 250)
            ]
        );
    }

    #[test]
    fn a_duplicate_does_not_consume_the_session_change() {
        let mut seq = StreamSequencer::new(Stream::Trades, None);
        seq.push("s1", trade(1_000, 100));
        assert!(seq.push("s2", trade(1_000, 100)).is_empty());
        assert_eq!(
            seq.push("s2", trade(2_000, 101)),
            [
                gap(Stream::Trades, 1_000, 2_000, GapReason::Disconnected),
                trade(2_000, 101)
            ]
        );
    }

    #[test]
    fn a_seed_opens_the_run_with_a_gap() {
        let seed = Some(Seed {
            last_event_time: t(500),
        });
        let mut seq = StreamSequencer::new(Stream::Trades, seed);
        assert_eq!(
            seq.push("s1", trade(1_000, 7)),
            [
                gap(Stream::Trades, 500, 1_000, GapReason::Disconnected),
                trade(1_000, 7)
            ]
        );
        // A seed after the first event (an open kline's close time, say) is
        // clamped to a well-formed gap.
        let late_seed = Some(Seed {
            last_event_time: t(5_000),
        });
        let mut seq = StreamSequencer::new(Stream::Klines, late_seed);
        let out = seq.push("s1", liquidation(1_000, 1));
        assert_eq!(
            out[0],
            gap(Stream::Klines, 1_000, 1_000, GapReason::Disconnected)
        );
        assert_eq!(seq.last_time(), Some(t(1_000)));
    }

    #[test]
    fn a_lost_sample_is_a_missing_data_gap_from_the_last_event() {
        let mut seq = StreamSequencer::new(Stream::Liquidations, None);
        assert_eq!(
            seq.missing(t(700)),
            gap(Stream::Liquidations, 700, 700, GapReason::MissingData)
        );
        seq.push("s1", liquidation(1_000, 1));
        assert_eq!(
            seq.missing(t(4_000)),
            gap(Stream::Liquidations, 1_000, 4_000, GapReason::MissingData)
        );
        // Clamped when the record's time is below the last event.
        assert_eq!(
            seq.missing(t(900)),
            gap(Stream::Liquidations, 900, 900, GapReason::MissingData)
        );
        // The state is untouched: the next event continues normally.
        assert_eq!(
            seq.push("s1", liquidation(5_000, 2)),
            [liquidation(5_000, 2)]
        );
        let seeded = StreamSequencer::new(
            Stream::Klines,
            Some(Seed {
                last_event_time: t(100),
            }),
        );
        assert_eq!(
            seeded.missing(t(500)),
            gap(Stream::Klines, 100, 500, GapReason::MissingData)
        );
    }

    #[test]
    fn idless_events_drop_only_exact_repeats() {
        let mut seq = StreamSequencer::new(Stream::Liquidations, None);
        assert_eq!(
            seq.push("s1", liquidation(1_000, 1)),
            [liquidation(1_000, 1)]
        );
        // Two distinct liquidations in the same millisecond are both kept.
        assert_eq!(
            seq.push("s1", liquidation(1_000, 2)),
            [liquidation(1_000, 2)]
        );
        assert!(seq.push("s1", liquidation(1_000, 2)).is_empty());
        assert_eq!(seq.duplicates(), 1);
        // No sequence ids, so no sequence break between id-less events.
        assert_eq!(
            seq.push("s1", liquidation(9_000, 3)),
            [liquidation(9_000, 3)]
        );
    }
}
