//! Market State: the canonical deterministic representation of current
//! conditions (brief §8, Market State & Regime brief).
//!
//! Walking-skeleton stage: the engine enforces the canonical event order
//! (ADR-028) and tracks only the latest trade; every other kind passes
//! through. Feature families (volatility, order flow, order book,
//! OI/funding, volume profile, structure) are added by the Market State
//! issues, each as a deterministic, versioned definition.

use crate::event::{MarketEvent, Stream};
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
    /// Last accepted exchange id of each id-carrying kind.
    ids: LastIds,
}

/// Last accepted exchange ids (ADR-028): the canonical order alone lets the
/// same trade or book id through twice when the payloads differ, or an id
/// fall at a later time.
#[derive(Debug, Default, Clone, Copy)]
struct LastIds {
    trade: Option<u64>,
    snapshot: Option<u64>,
    update: Option<u64>,
}

impl LastIds {
    /// Checks the exchange id of `event` and returns the bounds after it.
    fn check(self, event: &MarketEvent) -> Result<Self, StateError> {
        let mut next = self;
        match event {
            MarketEvent::Trade(trade) => {
                check_id(event, trade.trade_id, self.trade, None, Stream::Trades)?;
                next.trade = Some(trade.trade_id);
            }
            MarketEvent::BookSnapshot(snapshot) => {
                let id = snapshot.last_update_id;
                check_id(event, id, self.snapshot, self.update, Stream::OrderBook)?;
                next.snapshot = Some(id);
            }
            MarketEvent::BookUpdate(update) => {
                let id = update.last_update_id;
                check_id(event, id, self.update, self.snapshot, Stream::OrderBook)?;
                next.update = Some(id);
            }
            // Kinds without an exchange id.
            MarketEvent::FeedGap(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        Ok(next)
    }
}

/// `id` must exceed the last id of the same kind (`same_kind`) and must not
/// fall below the last id of the other kind on the same stream
/// (`other_kind`): an order-book update may carry the snapshot's last update
/// id, and a snapshot may restate the book at the last applied update.
fn check_id(
    event: &MarketEvent,
    id: u64,
    same_kind: Option<u64>,
    other_kind: Option<u64>,
    stream: Stream,
) -> Result<(), StateError> {
    if same_kind == Some(id) {
        return Err(StateError::Duplicate {
            key: event.canonical_key(),
        });
    }
    for last_id in [same_kind, other_kind].into_iter().flatten() {
        if id < last_id {
            return Err(StateError::IdRegression {
                stream,
                last_id,
                event: event.canonical_key(),
            });
        }
    }
    Ok(())
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
    /// - [`StateError::Duplicate`] if it equals the last consumed one, or
    ///   repeats the exchange id of the last trade, snapshot or update;
    /// - [`StateError::IdRegression`] if its exchange id falls below the last
    ///   accepted one of its stream.
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
        let ids = self.ids.check(event)?;

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
        self.ids = ids;
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
    /// The event equals the last consumed event, or repeats the exchange id
    /// of the last accepted event of its kind (ADR-028).
    Duplicate {
        /// Key of the repeated event.
        key: CanonicalKey,
    },
    /// The event's exchange id falls below the last accepted id of its
    /// stream, although it sorts after the last consumed event (ADR-028).
    IdRegression {
        /// The stream whose ids regressed.
        stream: Stream,
        /// The last accepted id the event falls below.
        last_id: u64,
        /// Key of the rejected event.
        event: CanonicalKey,
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
            Self::IdRegression {
                stream,
                last_id,
                event,
            } => write!(
                f,
                "event ({event}) has an exchange id below {last_id}, the last accepted on {stream:?}"
            ),
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
    use crate::event::samples::{gap, mark, one_of_each, snapshot, t, trade, update};
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
        // The second round carries fresh exchange ids.
        events.extend(one_of_each(2_000).into_iter().map(|event| match event {
            MarketEvent::Trade(trade) => MarketEvent::Trade(crate::event::Trade {
                trade_id: trade.trade_id + 1,
                ..trade
            }),
            MarketEvent::BookSnapshot(snapshot) => {
                MarketEvent::BookSnapshot(crate::event::BookSnapshot {
                    last_update_id: 200,
                    ..snapshot
                })
            }
            MarketEvent::BookUpdate(update) => MarketEvent::BookUpdate(crate::event::BookUpdate {
                first_update_id: 201,
                last_update_id: 205,
                prev_update_id: 200,
                ..update
            }),
            other @ (MarketEvent::FeedGap(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_)) => other,
        }));
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

    /// A trade with `trade_id` at `millis` and a price of `price_units`.
    fn priced_trade(millis: i64, trade_id: u64, price_units: i64) -> MarketEvent {
        let MarketEvent::Trade(base) = trade(millis, trade_id) else {
            unreachable!("samples::trade builds a trade")
        };
        MarketEvent::Trade(crate::event::Trade {
            price: Price::from_units(price_units),
            ..base
        })
    }

    #[test]
    fn late_event_gap_must_end_after_the_last_released_millisecond() {
        // A mark price at 2_000 was released; a trade for 1_500 arrives late.
        let released = mark(2_000, 1);
        let same_ms = gap(Stream::Trades, 1_500, 2_000, GapReason::LateEvent);
        assert_rejected(
            Some(&released),
            &same_ms,
            StateError::OutOfOrder {
                last: released.canonical_key(),
                event: same_ms.canonical_key(),
            },
        );
        // The same holds after a released trade at that millisecond.
        let released_trade = trade(2_000, 9);
        assert_rejected(
            Some(&released_trade),
            &same_ms,
            StateError::OutOfOrder {
                last: released_trade.canonical_key(),
                event: same_ms.canonical_key(),
            },
        );
        // One millisecond later sorts strictly after it and is accepted.
        let engine = engine_after(&[
            released,
            gap(Stream::Trades, 1_500, 2_001, GapReason::LateEvent),
            trade(2_001, 10),
        ]);
        assert_eq!(engine.state().as_of, Some(t(2_001)));
    }

    #[test]
    fn rejects_a_repeated_trade_id_with_a_different_payload() {
        let first = priced_trade(1_000, 7, 100);
        let same_ms = priced_trade(1_000, 7, 101);
        assert!(
            first < same_ms,
            "sorts after, so only the id check catches it"
        );
        assert_rejected(
            Some(&first),
            &same_ms,
            StateError::Duplicate {
                key: same_ms.canonical_key(),
            },
        );
        let later = priced_trade(1_500, 7, 100);
        assert_rejected(
            Some(&first),
            &later,
            StateError::Duplicate {
                key: later.canonical_key(),
            },
        );
        let mut engine = engine_after(&[first]);
        engine.apply(&same_ms).unwrap_err();
        assert_eq!(engine.state().trade_count, 1);
    }

    #[test]
    fn rejects_a_trade_id_regression_at_a_later_time() {
        let first = trade(1_000, 7);
        let regressed = trade(2_000, 5);
        let err = StateError::IdRegression {
            stream: Stream::Trades,
            last_id: 7,
            event: regressed.canonical_key(),
        };
        assert_rejected(Some(&first), &regressed, err);

        // The rejection did not lower the bound; the next id is accepted.
        let mut engine = engine_after(&[first]);
        assert_eq!(engine.apply(&regressed), Err(err));
        assert_eq!(
            engine.apply(&trade(2_000, 6)),
            Err(StateError::IdRegression {
                stream: Stream::Trades,
                last_id: 7,
                event: trade(2_000, 6).canonical_key(),
            })
        );
        engine.apply(&trade(2_000, 8)).unwrap();
        // Gaps carry no exchange id and do not reset the bound.
        engine
            .apply(&gap(Stream::Trades, 2_001, 3_000, GapReason::SequenceBreak))
            .unwrap();
        assert!(matches!(
            engine.apply(&trade(3_000, 8)),
            Err(StateError::Duplicate { .. })
        ));
        engine.apply(&trade(3_000, 9)).unwrap();
    }

    #[test]
    fn rejects_repeated_or_regressing_book_ids() {
        let snap = snapshot(1_000, 100);
        let repeated_snap = snapshot(2_000, 100);
        assert_rejected(
            Some(&snap),
            &repeated_snap,
            StateError::Duplicate {
                key: repeated_snap.canonical_key(),
            },
        );
        let older_snap = snapshot(2_000, 99);
        assert_rejected(
            Some(&snap),
            &older_snap,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 100,
                event: older_snap.canonical_key(),
            },
        );

        let diff = update(1_000, 101, 105, 100);
        let repeated_diff = update(2_000, 101, 105, 100);
        assert_rejected(
            Some(&diff),
            &repeated_diff,
            StateError::Duplicate {
                key: repeated_diff.canonical_key(),
            },
        );
        let older_diff = update(2_000, 99, 104, 98);
        assert_rejected(
            Some(&diff),
            &older_diff,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 105,
                event: older_diff.canonical_key(),
            },
        );

        // Across kinds: an update below the snapshot (same millisecond, so
        // it sorts after it by rank) regresses; equal ids are allowed both
        // ways.
        let stale = update(1_000, 90, 99, 89);
        assert_rejected(
            Some(&snap),
            &stale,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 100,
                event: stale.canonical_key(),
            },
        );
        let stale_snap = snapshot(2_000, 104);
        assert_rejected(
            Some(&diff),
            &stale_snap,
            StateError::IdRegression {
                stream: Stream::OrderBook,
                last_id: 105,
                event: stale_snap.canonical_key(),
            },
        );
        engine_after(&[
            snapshot(1_000, 100),
            update(1_000, 95, 100, 94),
            update(1_001, 101, 105, 100),
            snapshot(2_000, 105),
            update(2_000, 106, 106, 105),
        ]);
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
            StateError::IdRegression {
                stream: Stream::Trades,
                last_id: 7,
                event,
            }
            .to_string(),
            "event (1999ms Trade seq 2) has an exchange id below 7, the last accepted on Trades"
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
