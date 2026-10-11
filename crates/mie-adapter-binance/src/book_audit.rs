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
//! A comparison without a mismatch is `Matched` only when each side
//! compared at least [`MIN_SIDE_LEVELS`] levels; otherwise it is
//! `Inconclusive`, because a side that compared nothing proves nothing
//! (ADR-049). Each side reports its compared levels and the width of its
//! compared window in hundredths of a basis point ([`SideCoverage`]).
//!
//! The audit has no clock and does no I/O; it only observes. A mismatch
//! never changes what the core receives.

use mie_domain::book::{BookStep, Invalidation, LevelMismatch, OrderBook, Side};
use mie_domain::event::{GapReason, MarketEvent, Stream};
use mie_domain::num::Price;

/// Examples kept per mismatched checkpoint.
pub const MISMATCH_EXAMPLES: usize = 5;

/// Fewest levels a side must compare for a checkpoint without a mismatch to
/// count as `Matched` (ADR-049 D2): one level is the least evidence that a
/// side was compared at all. A code constant, not a run parameter.
pub const MIN_SIDE_LEVELS: usize = 1;

/// What one side of a checkpoint comparison covered (ADR-049 D1, D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SideCoverage {
    /// Distinct prices compared on this side.
    pub levels: usize,
    /// Width of the compared window from the fresh book's best price to the
    /// window edge, in hundredths of a basis point of that best, rounded up;
    /// 0 when the edge lies beyond the best price, so no level is inside.
    /// `None` when the side has no window or the fresh book no best on it.
    pub width_cbps: Option<u32>,
}

/// The verdict on one checkpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointResult {
    /// No level inside both trusted windows differs, and each side compared
    /// at least [`MIN_SIDE_LEVELS`] levels.
    Matched {
        /// The bid side.
        bid: SideCoverage,
        /// The ask side.
        ask: SideCoverage,
    },
    /// No level differs, but a side compared fewer than
    /// [`MIN_SIDE_LEVELS`] levels, so the comparison proves nothing about
    /// it (ADR-049). Typically the price moved past the previous window's
    /// edge between two checkpoints.
    Inconclusive {
        /// The bid side.
        bid: SideCoverage,
        /// The ask side.
        ask: SideCoverage,
    },
    /// Some levels differ; this takes precedence over a thin side.
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
    if !comparison.is_match() {
        return CheckpointResult::Mismatched {
            levels: comparison.levels_compared(),
            mismatches: comparison.mismatches.len(),
            examples: comparison
                .mismatches
                .iter()
                .take(MISMATCH_EXAMPLES)
                .copied()
                .collect(),
        };
    }
    let bid = SideCoverage {
        levels: comparison.bid_levels,
        width_cbps: comparison
            .window
            .lowest_bid
            .zip(fresh.best_bid())
            .map(|(lowest, best)| width_cbps(Side::Bid, best.price, lowest)),
    };
    let ask = SideCoverage {
        levels: comparison.ask_levels,
        width_cbps: comparison
            .window
            .highest_ask
            .zip(fresh.best_ask())
            .map(|(highest, best)| width_cbps(Side::Ask, best.price, highest)),
    };
    if bid.levels < MIN_SIDE_LEVELS || ask.levels < MIN_SIDE_LEVELS {
        CheckpointResult::Inconclusive { bid, ask }
    } else {
        CheckpointResult::Matched { bid, ask }
    }
}

/// Width of a side's window from `best` to `edge`, in hundredths of a basis
/// point of `best`, rounded up (ADR-049 D4). The edge lies at or below the
/// best bid, or at or above the best ask; beyond the best it is 0, as no
/// level lies inside. A non-positive `best` gives 0; a width that does not
/// fit saturates.
fn width_cbps(side: Side, best: Price, edge: Price) -> u32 {
    let best = i128::from(best.units());
    if best <= 0 {
        return 0;
    }
    let edge = i128::from(edge.units());
    let width = match side {
        Side::Bid => best - edge,
        Side::Ask => edge - best,
    };
    if width <= 0 {
        return 0;
    }
    let cbps = (width * 1_000_000 + best - 1) / best;
    u32::try_from(cbps).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
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
                // 10 000 → 9 990 is exactly 10 bps.
                bid: SideCoverage {
                    levels: 3,
                    width_cbps: Some(1_000),
                },
                // 10 001 → 10 020 is 18.998 bps, rounded up.
                ask: SideCoverage {
                    levels: 2,
                    width_cbps: Some(1_900),
                },
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

    /// Settles one checkpoint after the price fell through the bid side of
    /// the anchor's window: the initial snapshot holds `MIN_SIDE_LEVELS + 2`
    /// bids at 1 000, 999, …; a diff removes all but the `kept` lowest of
    /// them and adds one bid below the window, which the checkpoint repeats.
    /// The bid side then compares exactly `kept` levels.
    fn thinned(kept: usize) -> Option<CheckpointResult> {
        let total = MIN_SIDE_LEVELS + 2;
        assert!(kept <= total);
        let bids: Vec<(i64, i64)> = (0..total).map(|i| (1_000 - i as i64, 1)).collect();
        let below = 1_000 - total as i64 - 5;
        let mut diff: Vec<(i64, i64)> = bids[..total - kept].iter().map(|&(p, _)| (p, 0)).collect();
        diff.push((below, 1));
        let mut fresh: Vec<(i64, i64)> = bids[total - kept..].to_vec();
        fresh.push((below, 1));

        let mut audit = BookAudit::new();
        audit.apply(&snapshot(100, &bids, &[(1_001, 1), (1_002, 1)]));
        assert_eq!(audit.apply(&update(100, 105, 99, &diff)), None);
        assert_eq!(
            audit.apply(&snapshot(107, &fresh, &[(1_001, 1), (1_002, 1)])),
            None
        );
        audit.apply(&update(106, 110, 105, &[]))
    }

    #[test]
    fn an_edge_beyond_the_best_price_is_zero_width_and_inconclusive() {
        // Every bid of the anchor's window is gone; the fresh best lies
        // below the compared window's edge.
        let result = thinned(0);
        assert_eq!(
            result,
            Some(CheckpointResult::Inconclusive {
                bid: SideCoverage {
                    levels: 0,
                    width_cbps: Some(0),
                },
                // 1 001 → 1 002 is 999.001 hundredths of a bp, rounded up.
                ask: SideCoverage {
                    levels: 2,
                    width_cbps: Some(1_000),
                },
            })
        );
    }

    #[test]
    fn an_edge_at_the_best_price_is_zero_width_with_one_level() {
        let Some(CheckpointResult::Matched { bid, .. }) = thinned(1) else {
            panic!("expected Matched at a floor of {MIN_SIDE_LEVELS}");
        };
        assert_eq!(
            bid,
            SideCoverage {
                levels: 1,
                width_cbps: Some(0),
            }
        );
    }

    #[test]
    fn the_floor_separates_inconclusive_from_matched() {
        let below = thinned(MIN_SIDE_LEVELS - 1).expect("settled");
        let CheckpointResult::Inconclusive { bid, .. } = below else {
            panic!("{below:?}");
        };
        assert_eq!(bid.levels, MIN_SIDE_LEVELS - 1);
        let at = thinned(MIN_SIDE_LEVELS).expect("settled");
        let CheckpointResult::Matched { bid, .. } = at else {
            panic!("{at:?}");
        };
        assert_eq!(bid.levels, MIN_SIDE_LEVELS);
    }

    #[test]
    fn a_side_with_zero_levels_is_inconclusive() {
        // The rebuilt book lost every ask of its window; the checkpoint has
        // asks only above it.
        let mut audit = BookAudit::new();
        audit.apply(&snapshot(100, &[(1_000, 1)], &[(1_001, 1), (1_002, 1)]));
        let ask_diff = MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(1_000),
            first_update_id: 100,
            last_update_id: 105,
            prev_update_id: 99,
            bids: Vec::new(),
            asks: vec![lv(1_001, 0), lv(1_002, 0), lv(1_010, 1)],
        });
        assert_eq!(audit.apply(&ask_diff), None);
        audit.apply(&snapshot(107, &[(1_000, 1)], &[(1_010, 1)]));
        let result = audit.apply(&update(106, 110, 105, &[]));
        assert_eq!(
            result,
            Some(CheckpointResult::Inconclusive {
                bid: SideCoverage {
                    levels: 1,
                    width_cbps: Some(0),
                },
                ask: SideCoverage {
                    levels: 0,
                    width_cbps: Some(0),
                },
            })
        );
    }

    #[test]
    fn a_mismatch_takes_precedence_over_an_empty_side() {
        // The bid side is empty as in `thinned(0)`, but the checkpoint
        // disagrees on an ask.
        let total = MIN_SIDE_LEVELS + 2;
        let bids: Vec<(i64, i64)> = (0..total).map(|i| (1_000 - i as i64, 1)).collect();
        let mut diff: Vec<(i64, i64)> = bids.iter().map(|&(p, _)| (p, 0)).collect();
        diff.push((900, 1));
        let mut audit = BookAudit::new();
        audit.apply(&snapshot(100, &bids, &[(1_001, 1), (1_002, 1)]));
        audit.apply(&update(100, 105, 99, &diff));
        audit.apply(&snapshot(107, &[(900, 1)], &[(1_001, 2), (1_002, 1)]));
        let result = audit.apply(&update(106, 110, 105, &[])).expect("settled");
        let CheckpointResult::Mismatched {
            levels, mismatches, ..
        } = result
        else {
            panic!("{result:?}");
        };
        assert_eq!((levels, mismatches), (2, 1));
    }

    #[test]
    fn a_sub_bp_width_is_not_zero() {
        let p = Price::from_units;
        // 50 units of 1 000 000 is 0.5 bps, i.e. 50 hundredths.
        assert_eq!(width_cbps(Side::Bid, p(1_000_000), p(999_950)), 50);
        assert_eq!(width_cbps(Side::Ask, p(1_000_000), p(1_000_050)), 50);
        // One unit is 0.01 bps here.
        assert_eq!(width_cbps(Side::Bid, p(1_000_000), p(999_999)), 1);
    }

    #[test]
    fn window_widths_round_up_per_side() {
        let p = Price::from_units;
        assert_eq!(width_cbps(Side::Bid, p(1_000_000), p(999_000)), 1_000);
        assert_eq!(width_cbps(Side::Ask, p(1_000_000), p(1_001_000)), 1_000);
        // 999 units of 1 000 000 is exactly 999 hundredths; half a
        // hundredth or 1.5 hundredths round up.
        assert_eq!(width_cbps(Side::Ask, p(1_000_000), p(1_000_999)), 999);
        assert_eq!(width_cbps(Side::Ask, p(2_000_000), p(2_000_001)), 1);
        assert_eq!(width_cbps(Side::Ask, p(2_000_000), p(2_000_003)), 2);
        // At the best, and beyond it on either side, the width is 0.
        assert_eq!(width_cbps(Side::Bid, p(1_000_000), p(1_000_000)), 0);
        assert_eq!(width_cbps(Side::Ask, p(1_000_000), p(1_000_000)), 0);
        assert_eq!(width_cbps(Side::Bid, p(1_000_000), p(1_000_500)), 0);
        assert_eq!(width_cbps(Side::Ask, p(1_000_000), p(999_500)), 0);
        // A non-positive best gives 0; an oversized width saturates.
        assert_eq!(width_cbps(Side::Ask, p(0), p(5)), 0);
        assert_eq!(width_cbps(Side::Bid, p(-5), p(-10)), 0);
        assert_eq!(width_cbps(Side::Ask, p(1), p(i64::MAX)), u32::MAX);
    }
}
