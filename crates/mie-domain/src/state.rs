//! Market State: the canonical deterministic representation of current
//! conditions (brief §8, Market State & Regime brief).
//!
//! Walking-skeleton stage: the engine enforces the canonical event order
//! (ADR-028) and tracks only the latest trade; every other kind passes
//! through. Feature families (volatility, order flow, order book,
//! OI/funding, volume profile, structure) are added by the Market State
//! issues, each as a deterministic, versioned definition.

use crate::event::MarketEvent;
use crate::num::Price;
use crate::order::CanonicalKey;
use crate::time::EventTime;
use std::cmp::Ordering;
use std::fmt;

/// The market as known after the last consumed event.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct MarketState {
    /// Ordering time (ADR-028) of the last consumed event; `None` before the
    /// first one.
    pub as_of: Option<EventTime>,
    /// Price of the last trade.
    pub last_trade_price: Option<Price>,
    /// Number of trades consumed.
    pub trade_count: u64,
}

/// Builds [`MarketState`] incrementally from an event stream in canonical
/// order (ADR-028).
///
/// Live ingestion and historical replay drive the same engine (ADR-019); the
/// engine never knows which of them is feeding it.
#[derive(Debug, Default)]
pub struct MarketStateEngine {
    state: MarketState,
    /// The last consumed event: the lower bound for the next one.
    last: Option<MarketEvent>,
}

impl MarketStateEngine {
    /// Creates an engine with an empty state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the next event.
    ///
    /// # Errors
    ///
    /// The state is left unchanged on every error:
    /// - [`StateError::InvalidGap`] for a feed gap with `start > end`;
    /// - [`StateError::OutOfOrder`] if the event sorts before the last
    ///   consumed one;
    /// - [`StateError::Duplicate`] if it equals the last consumed one.
    ///
    /// Ordering is the market-data provider's job — the domain never
    /// reorders.
    pub fn apply(&mut self, event: &MarketEvent) -> Result<(), StateError> {
        if let MarketEvent::FeedGap(gap) = event
            && gap.start > gap.end
        {
            return Err(StateError::InvalidGap {
                start: gap.start,
                end: gap.end,
            });
        }
        if let Some(last) = &self.last {
            match event.cmp(last) {
                Ordering::Less => {
                    return Err(StateError::OutOfOrder {
                        last: last.canonical_key(),
                        event: event.canonical_key(),
                    });
                }
                Ordering::Equal => {
                    return Err(StateError::Duplicate {
                        key: event.canonical_key(),
                    });
                }
                Ordering::Greater => {}
            }
        }

        match event {
            MarketEvent::Trade(trade) => {
                self.state.last_trade_price = Some(trade.price);
                self.state.trade_count += 1;
            }
            // No feature consumes these yet.
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        self.state.as_of = Some(event.time());
        self.last = Some(event.clone());
        Ok(())
    }

    /// The current state.
    pub fn state(&self) -> &MarketState {
        &self.state
    }
}

/// Why the engine rejected an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateError {
    /// The event sorts before the last consumed event in the canonical order
    /// (ADR-028).
    OutOfOrder {
        /// Key of the last consumed event.
        last: CanonicalKey,
        /// Key of the rejected event.
        event: CanonicalKey,
    },
    /// The event equals the last consumed event.
    Duplicate {
        /// Key of the repeated event.
        key: CanonicalKey,
    },
    /// A feed gap whose start is after its end.
    InvalidGap {
        /// Start of the rejected gap.
        start: EventTime,
        /// End of the rejected gap.
        end: EventTime,
    },
}

impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfOrder { last, event } => write!(
                f,
                "event ({event}) sorts before the last consumed event ({last})"
            ),
            Self::Duplicate { key } => {
                write!(f, "event ({key}) repeats the last consumed event")
            }
            Self::InvalidGap { start, end } => {
                write!(f, "feed gap starts at {start}, after its end at {end}")
            }
        }
    }
}

impl std::error::Error for StateError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::samples::{gap, mark, one_of_each, snapshot, t, trade};
    use crate::event::{GapReason, Stream};
    use crate::order::EventKind;

    fn engine_after(events: &[MarketEvent]) -> MarketStateEngine {
        let mut engine = MarketStateEngine::new();
        for event in events {
            engine.apply(event).unwrap();
        }
        engine
    }

    /// Applies `rejected` after `accepted`, expects `err`, and checks that
    /// neither the state nor the ordering bound moved.
    fn assert_rejected(accepted: Option<&MarketEvent>, rejected: &MarketEvent, err: StateError) {
        let mut engine = engine_after(accepted.map(std::slice::from_ref).unwrap_or_default());
        let before = engine.state().clone();
        assert_eq!(engine.apply(rejected), Err(err));
        assert_eq!(engine.state(), &before);
        if let Some(last) = accepted {
            // The bound is still the last accepted event, not the rejected one.
            assert_eq!(
                engine.apply(last),
                Err(StateError::Duplicate {
                    key: last.canonical_key()
                })
            );
            assert_eq!(engine.state(), &before);
        }
    }

    #[test]
    fn tracks_latest_trade() {
        let engine = engine_after(&[trade(1_000, 1), trade(1_000, 2), trade(1_500, 3)]);
        assert_eq!(
            engine.state(),
            &MarketState {
                as_of: Some(t(1_500)),
                last_trade_price: Some(Price::from_units(6_354_210_000_000)),
                trade_count: 3,
            }
        );
    }

    #[test]
    fn accepts_every_kind_and_only_trades_change_trade_fields() {
        let mut events = one_of_each(1_000);
        events.extend(one_of_each(2_000));
        let mut engine = MarketStateEngine::new();
        for event in &events {
            let before = engine.state().clone();
            engine.apply(event).unwrap();
            let after = engine.state();
            assert_eq!(after.as_of, Some(event.time()), "{event:?}");
            if event.kind() == EventKind::Trade {
                assert_eq!(after.trade_count, before.trade_count + 1);
                assert!(after.last_trade_price.is_some());
            } else {
                assert_eq!(after.trade_count, before.trade_count, "{event:?}");
                assert_eq!(after.last_trade_price, before.last_trade_price, "{event:?}");
            }
        }
        assert_eq!(engine.state().trade_count, 2);
        // The kline closing at 2_000 is the last event.
        assert_eq!(engine.state().as_of, Some(t(2_000)));
    }

    #[test]
    fn rejects_an_earlier_time() {
        let last = trade(2_000, 1);
        let late = trade(1_999, 2);
        assert_rejected(
            Some(&last),
            &late,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: late.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_a_lower_rank_in_the_same_millisecond() {
        let last = mark(1_000, 1);
        let lower = trade(1_000, u64::MAX);
        assert_rejected(
            Some(&last),
            &lower,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: lower.canonical_key(),
            },
        );
        // A gap at the same millisecond after the resumed event is late too.
        let resumed = snapshot(1_000, 10);
        let gap_after = gap(Stream::OrderBook, 900, 1_000, GapReason::Disconnected);
        assert_rejected(
            Some(&resumed),
            &gap_after,
            StateError::OutOfOrder {
                last: resumed.canonical_key(),
                event: gap_after.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_a_lower_seq_of_the_same_kind() {
        let last = trade(1_000, 5);
        let lower = trade(1_000, 4);
        assert_rejected(
            Some(&last),
            &lower,
            StateError::OutOfOrder {
                last: last.canonical_key(),
                event: lower.canonical_key(),
            },
        );
    }

    #[test]
    fn rejects_same_key_events_in_the_wrong_structural_order() {
        let higher = mark(1_000, 200);
        let lower = mark(1_000, 100);
        assert_eq!(higher.canonical_key(), lower.canonical_key());
        assert_rejected(
            Some(&higher),
            &lower,
            StateError::OutOfOrder {
                last: higher.canonical_key(),
                event: lower.canonical_key(),
            },
        );
        // The right order is accepted.
        engine_after(&[lower, higher]);
    }

    #[test]
    fn rejects_a_duplicate() {
        for event in [
            trade(1_000, 1),
            mark(1_000, 1),
            gap(Stream::Trades, 900, 1_000, GapReason::SequenceBreak),
        ] {
            assert_rejected(
                Some(&event),
                &event,
                StateError::Duplicate {
                    key: event.canonical_key(),
                },
            );
        }
    }

    #[test]
    fn rejects_a_gap_that_ends_before_it_starts() {
        let inverted = gap(Stream::Klines, 2_001, 2_000, GapReason::MissingData);
        let err = StateError::InvalidGap {
            start: t(2_001),
            end: t(2_000),
        };
        assert_rejected(None, &inverted, err);
        assert_rejected(Some(&trade(1_000, 1)), &inverted, err);
        // Validity is checked before order: an inverted gap that is also late
        // is reported as invalid.
        assert_rejected(Some(&trade(3_000, 1)), &inverted, err);

        // The rejected gap did not move the bound to its end.
        let mut engine = engine_after(&[trade(1_000, 1)]);
        engine
            .apply(&gap(
                Stream::Trades,
                1_000_000,
                9_000,
                GapReason::Disconnected,
            ))
            .unwrap_err();
        engine.apply(&trade(1_500, 2)).unwrap();
    }

    #[test]
    fn accepts_a_single_instant_gap() {
        let engine = engine_after(&[
            trade(1_000, 1),
            gap(Stream::Trades, 2_000, 2_000, GapReason::LateEvent),
            trade(2_000, 2),
        ]);
        assert_eq!(engine.state().as_of, Some(t(2_000)));
        assert_eq!(engine.state().trade_count, 2);
    }

    #[test]
    fn errors_describe_themselves() {
        let last = trade(2_000, 1).canonical_key();
        let event = trade(1_999, 2).canonical_key();
        assert_eq!(
            StateError::OutOfOrder { last, event }.to_string(),
            "event (1999ms Trade seq 2) sorts before the last consumed event (2000ms Trade seq 1)"
        );
        assert_eq!(
            StateError::Duplicate { key: last }.to_string(),
            "event (2000ms Trade seq 1) repeats the last consumed event"
        );
        assert_eq!(
            StateError::InvalidGap {
                start: t(5),
                end: t(4)
            }
            .to_string(),
            "feed gap starts at 5ms, after its end at 4ms"
        );
    }
}
