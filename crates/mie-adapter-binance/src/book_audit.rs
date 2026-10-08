//! The live checkpoint audit: does the book rebuilt from diffs match a fresh
//! REST snapshot (ADR-038, the acceptance check of #10)?
//!
//! [`BookAudit`] follows the **released** events in canonical order, as the
//! core sees them, and mirrors the core's book with a domain
//! [`OrderBook`]. A `BookSnapshot` that arrives while that book is valid is
//! a checkpoint re-anchor (rule 6 of [`crate::book_sync`]): the audit keeps
//! the book it rebuilt since the previous anchor and builds a shadow book
//! from the snapshot. The next `BookUpdate` is the diff that straddles the
//! snapshot; it is applied to both books, they are compared within both
//! trusted windows, and the shadow is adopted, as the core resets at the
//! snapshot.
//!
//! The audit has no clock and does no I/O; it only observes. A mismatch
//! never changes what the core receives.

use mie_domain::book::{BookStep, Invalidation, LevelMismatch, OrderBook};
use mie_domain::event::{GapReason, MarketEvent, Stream};
use mie_domain::num::Price;

/// Examples kept per mismatched checkpoint.
pub const MISMATCH_EXAMPLES: usize = 5;

/// The verdict on one checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointResult {
    /// Every level inside both trusted windows matched.
    Matched {
        /// Distinct prices compared, both sides.
        levels: usize,
        /// Depth of the compared bid window below the best bid, in basis
        /// points of the best bid; `None` without a bid window.
        window_bid_bps: Option<u32>,
        /// Depth of the compared ask window above the best ask, in basis
        /// points of the best ask; `None` without an ask window.
        window_ask_bps: Option<u32>,
    },
    /// Some levels differ.
    Mismatched {
        /// Distinct prices compared, both sides.
        levels: usize,
        /// How many of them differ.
        mismatches: usize,
        /// The first [`MISMATCH_EXAMPLES`] of them.
        examples: Vec<LevelMismatch>,
    },
    /// An `OrderBook` gap came between the checkpoint and its straddling
    /// diff, so there is nothing to compare.
    Unverifiable(GapReason),
    /// The mirrored book was invalidated by something other than an
    /// `OrderBook` gap: the provider delivered an inconsistent sequence.
    Invalidated(Invalidation),
}

/// Mirrors the core's book over released events and judges checkpoints.
#[derive(Debug, Clone, Default)]
pub struct BookAudit {
    book: OrderBook,
    /// The book of a checkpoint waiting for its straddling diff.
    shadow: Option<OrderBook>,
}

impl BookAudit {
    /// An audit before any book event.
    pub fn new() -> Self {
        Self::default()
    }

    /// Consumes the next released event; `Some` when it settles a
    /// checkpoint or reveals an inconsistent sequence.
    pub fn apply(&mut self, event: &MarketEvent) -> Option<CheckpointResult> {
        match event {
            MarketEvent::FeedGap(gap) if gap.stream == Stream::OrderBook => {
                self.book.apply(event);
                self.shadow
                    .take()
                    .map(|_| CheckpointResult::Unverifiable(gap.reason))
            }
            MarketEvent::BookSnapshot(_) => {
                if self.book.is_valid() {
                    let mut shadow = OrderBook::new();
                    shadow.apply(event);
                    self.shadow = Some(shadow);
                } else {
                    self.book.apply(event);
                }
                None
            }
            MarketEvent::BookUpdate(_) => {
                if let BookStep::Invalidated(why) = self.book.apply(event) {
                    self.shadow = None;
                    return Some(CheckpointResult::Invalidated(why));
                }
                let mut shadow = self.shadow.take()?;
                if let BookStep::Invalidated(why) = shadow.apply(event) {
                    self.book = shadow;
                    return Some(CheckpointResult::Invalidated(why));
                }
                let result = judge(&self.book, &shadow);
                self.book = shadow;
                Some(result)
            }
            _ => None,
        }
    }

    /// Whether the mirrored book is valid.
    pub fn is_valid(&self) -> bool {
        self.book.is_valid()
    }
}

/// Compares the rebuilt book with the snapshot's.
fn judge(rebuilt: &OrderBook, fresh: &OrderBook) -> CheckpointResult {
    let Some(comparison) = rebuilt.compare(fresh) else {
        // Both books are valid here; keep the function total.
        return CheckpointResult::Unverifiable(GapReason::SequenceBreak);
    };
    if comparison.is_match() {
        CheckpointResult::Matched {
            levels: comparison.levels_compared,
            window_bid_bps: comparison
                .window
                .lowest_bid
                .zip(fresh.best_bid())
                .map(|(lowest, best)| bps(best.price, lowest)),
            window_ask_bps: comparison
                .window
                .highest_ask
                .zip(fresh.best_ask())
                .map(|(highest, best)| bps(best.price, highest)),
        }
    } else {
        CheckpointResult::Mismatched {
            levels: comparison.levels_compared,
            mismatches: comparison.mismatches.len(),
            examples: comparison
                .mismatches
                .iter()
                .take(MISMATCH_EXAMPLES)
                .copied()
                .collect(),
        }
    }
}

/// `|edge − best| / best` in whole basis points, rounded down.
fn bps(best: Price, edge: Price) -> u32 {
    let best = i128::from(best.units());
    if best <= 0 {
        return 0;
    }
    let distance = (i128::from(edge.units()) - best).abs();
    u32::try_from(distance * 10_000 / best).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use mie_domain::book::Side;
    use mie_domain::event::{BookSnapshot, BookUpdate, FeedGap, Level};
    use mie_domain::num::Qty;
    use mie_domain::time::EventTime;

    fn lv(price: i64, qty: i64) -> Level {
        Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        }
    }

    fn snapshot(id: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> MarketEvent {
        MarketEvent::BookSnapshot(BookSnapshot {
            time: EventTime::from_millis(1_000),
            last_update_id: id,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
        })
    }

    fn update(first: u64, last: u64, prev: u64, bids: &[(i64, i64)]) -> MarketEvent {
        MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(1_000),
            first_update_id: first,
            last_update_id: last,
            prev_update_id: prev,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: Vec::new(),
        })
    }

    fn gap() -> MarketEvent {
        MarketEvent::FeedGap(FeedGap {
            stream: Stream::OrderBook,
            start: EventTime::from_millis(900),
            end: EventTime::from_millis(1_000),
            reason: GapReason::SequenceBreak,
        })
    }

    /// Book at 10 000.00 / 10 001.00 with bids down to 9 990.00 and asks up
    /// to 10 020.00, then one chained diff.
    fn anchored() -> BookAudit {
        let mut audit = BookAudit::new();
        let initial = snapshot(
            100,
            &[(1_000_000_000_000, 5), (999_000_000_000, 7)],
            &[(1_000_100_000_000, 3), (1_002_000_000_000, 9)],
        );
        assert_eq!(audit.apply(&initial), None);
        assert_eq!(
            audit.apply(&update(100, 105, 99, &[(999_500_000_000, 2)])),
            None
        );
        assert!(audit.is_valid());
        audit
    }

    /// The checkpoint the exchange would send after `anchored`, at id 107,
    /// with `extra` bid levels.
    fn checkpoint(extra: &[(i64, i64)]) -> MarketEvent {
        let mut bids = vec![
            (1_000_000_000_000, 5),
            (999_500_000_000, 2),
            (999_000_000_000, 7),
        ];
        bids.extend_from_slice(extra);
        snapshot(
            107,
            &bids,
            &[(1_000_100_000_000, 3), (1_002_000_000_000, 9)],
        )
    }

    #[test]
    fn a_consistent_checkpoint_matches() {
        let mut audit = anchored();
        assert_eq!(audit.apply(&checkpoint(&[])), None);
        // The straddling diff settles it.
        let result = audit.apply(&update(106, 110, 105, &[(1_000_000_000_000, 6)]));
        assert_eq!(
            result,
            Some(CheckpointResult::Matched {
                levels: 5,
                // 10 000 → 9 990 is 10 bps; 10 001 → 10 020 is 18.99 bps.
                window_bid_bps: Some(10),
                window_ask_bps: Some(18),
            })
        );
        // The shadow was adopted; the next diff chains on it.
        assert_eq!(audit.apply(&update(111, 112, 110, &[])), None);
        assert!(audit.is_valid());
    }

    #[test]
    fn a_perturbed_book_mismatches_with_at_most_five_examples() {
        let mut audit = anchored();
        let extra: Vec<(i64, i64)> = (1..=7)
            .map(|i| (999_000_000_000 + i * 10_000_000, 1))
            .collect();
        audit.apply(&checkpoint(&extra));
        let result = audit.apply(&update(106, 110, 105, &[])).expect("settled");
        let CheckpointResult::Mismatched {
            levels,
            mismatches,
            examples,
        } = result
        else {
            panic!("{result:?}");
        };
        assert_eq!((levels, mismatches, examples.len()), (12, 7, 5));
        assert_eq!(examples[0].side, Side::Bid);
        assert_eq!(examples[0].ours, None);
        assert_eq!(examples[0].theirs, Some(Qty::from_units(1)));
        // The fresh book is adopted all the same.
        assert!(audit.is_valid());
    }

    #[test]
    fn a_gap_before_the_straddling_diff_is_unverifiable() {
        let mut audit = anchored();
        audit.apply(&checkpoint(&[]));
        assert_eq!(
            audit.apply(&gap()),
            Some(CheckpointResult::Unverifiable(GapReason::SequenceBreak))
        );
        assert!(!audit.is_valid());
        // After a gap a snapshot is a resync, not a checkpoint.
        assert_eq!(audit.apply(&checkpoint(&[])), None);
        assert_eq!(audit.apply(&update(106, 110, 105, &[])), None);
        // A gap without a waiting checkpoint reports nothing.
        assert_eq!(audit.apply(&gap()), None);
    }

    #[test]
    fn an_invalidation_without_a_gap_is_reported() {
        let mut audit = anchored();
        assert_eq!(
            audit.apply(&update(120, 121, 119, &[])),
            Some(CheckpointResult::Invalidated(Invalidation::ChainBreak {
                expected: 105,
                found: 119
            }))
        );
        // A checkpoint whose diff does not straddle it.
        let mut audit = anchored();
        audit.apply(&checkpoint(&[]));
        assert_eq!(
            audit.apply(&update(106, 106, 105, &[])),
            Some(CheckpointResult::Invalidated(
                Invalidation::MissedStraddle {
                    snapshot_id: 107,
                    first_update_id: 106,
                    last_update_id: 106,
                }
            ))
        );
        assert!(!audit.is_valid());
        // Other streams pass through.
        let trade_gap = MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(1),
            reason: GapReason::Disconnected,
        });
        assert_eq!(anchored().apply(&trade_gap), None);
    }

    #[test]
    fn window_bps_round_down() {
        let p = Price::from_units;
        assert_eq!(bps(p(1_000_000), p(999_000)), 10);
        assert_eq!(bps(p(1_000_000), p(1_000_999)), 9);
        assert_eq!(bps(p(1_000_000), p(1_000_000)), 0);
        assert_eq!(bps(p(0), p(5)), 0);
    }
}
