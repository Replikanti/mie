//! The L2 order book (Data Plane order-flow contract: depth events;
//! ADR-021, ADR-022, ADR-028, ADR-038, proposed).
//!
//! [`OrderBook`] rebuilds one instrument's price levels from the canonical
//! event sequence: a [`BookSnapshot`] resets it, every [`BookUpdate`] after
//! that must continue the update-id chain, and a `FeedGap` on
//! [`Stream::OrderBook`] invalidates it until the next snapshot.
//!
//! - **Chain** (ADR-038): the first update after a snapshot with last update
//!   id `L` must straddle it, `first_update_id <= L <= last_update_id`;
//!   every later update must have `prev_update_id` equal to the last applied
//!   `last_update_id`. A violation invalidates the book and clears its
//!   levels; nothing is applied to an invalid book until the next snapshot.
//! - **Levels** carry the absolute resting quantity after the change and
//!   apply in source order. A quantity of zero removes the level; removing an
//!   absent level is a no-op. A negative quantity is not a book state and
//!   invalidates the book.
//! - **Trusted window** ([`TrustedWindow`]): a snapshot covers a bounded
//!   number of levels per side. The book is fully known only between the
//!   snapshot's deepest bid and its deepest ask; beyond that, only the levels
//!   changed since the snapshot are known, never their absence. The window
//!   stays at the snapshot's bounds until the next reset, also when the
//!   snapshot returned fewer levels than requested.
//!
//! A snapshot is a reset, never liquidity flow: a feature that measures
//! added or removed liquidity (#18) starts over at a snapshot instead of
//! reading the difference to the previous book as market activity.

use crate::event::{BookSnapshot, BookUpdate, GapReason, Level, MarketEvent, Stream};
use crate::num::{Price, Qty};
use std::collections::{BTreeMap, BTreeSet};

/// A side of the book.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    /// Resting buy orders.
    Bid,
    /// Resting sell orders.
    Ask,
}

/// Why the book became invalid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalidation {
    /// A `FeedGap` on [`Stream::OrderBook`]: the provider announced lost
    /// continuity.
    Gap(GapReason),
    /// The first update after a snapshot does not straddle the snapshot's
    /// last update id.
    MissedStraddle {
        /// The snapshot's last update id.
        snapshot_id: u64,
        /// The update's first update id.
        first_update_id: u64,
        /// The update's last update id.
        last_update_id: u64,
    },
    /// An update's `prev_update_id` is not the last applied update id.
    ChainBreak {
        /// The last applied update id.
        expected: u64,
        /// The update's `prev_update_id`.
        found: u64,
    },
    /// A level with a negative quantity.
    NegativeQty {
        /// Its side.
        side: Side,
        /// Its price.
        price: Price,
    },
}

/// What [`OrderBook::apply`] did with an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookStep {
    /// A snapshot replaced the book.
    Reset,
    /// An update was applied.
    Applied,
    /// An update arrived while the book is invalid and was not applied.
    Ignored,
    /// The event does not concern the book.
    Unrelated,
    /// The event invalidated the book; its levels are cleared.
    Invalidated(Invalidation),
}

/// The price range in which the book is fully known (ADR-038).
///
/// Bids at or above `lowest_bid` and asks at or below `highest_ask` are
/// fully known; `None` means the snapshot had no level on that side, so no
/// price of it is fully known.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TrustedWindow {
    /// The snapshot's deepest bid.
    pub lowest_bid: Option<Price>,
    /// The snapshot's deepest ask.
    pub highest_ask: Option<Price>,
}

impl TrustedWindow {
    /// The window both books fully know: the higher lowest bid and the
    /// lower highest ask; `None` on a side either window lacks.
    pub fn intersect(self, other: Self) -> Self {
        let both =
            |a: Option<Price>, b: Option<Price>, pick: fn(Price, Price) -> Price| match (a, b) {
                (Some(a), Some(b)) => Some(pick(a, b)),
                _ => None,
            };
        Self {
            lowest_bid: both(self.lowest_bid, other.lowest_bid, Price::max),
            highest_ask: both(self.highest_ask, other.highest_ask, Price::min),
        }
    }
}

/// One price whose quantity differs between two books.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LevelMismatch {
    /// The side.
    pub side: Side,
    /// The price.
    pub price: Price,
    /// The quantity in the book compared from; `None` when absent.
    pub ours: Option<Qty>,
    /// The quantity in the book compared with; `None` when absent.
    pub theirs: Option<Qty>,
}

/// The result of [`OrderBook::compare`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookComparison {
    /// The window compared: the intersection of both books' windows.
    pub window: TrustedWindow,
    /// Distinct prices compared, both sides.
    pub levels_compared: usize,
    /// The prices whose quantities differ, bids from the best down, then
    /// asks from the best up.
    pub mismatches: Vec<LevelMismatch>,
}

impl BookComparison {
    /// Whether every compared level matched.
    pub fn is_match(&self) -> bool {
        self.mismatches.is_empty()
    }
}

/// Where the book stands in its update-id chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Chain {
    /// No snapshot since the start or the last invalidation.
    Invalid,
    /// A snapshot with this last update id, no update applied yet.
    Straddle(u64),
    /// Updates applied up to this last update id.
    Chained(u64),
}

/// An L2 order book rebuilt from snapshots and updates (ADR-038).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<Price, Qty>,
    asks: BTreeMap<Price, Qty>,
    chain: Chain,
    window: TrustedWindow,
}

impl Default for OrderBook {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderBook {
    /// An empty, invalid book: it becomes valid at its first snapshot.
    pub fn new() -> Self {
        Self {
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            chain: Chain::Invalid,
            window: TrustedWindow::default(),
        }
    }

    /// Consumes the next event of the canonical sequence.
    pub fn apply(&mut self, event: &MarketEvent) -> BookStep {
        match event {
            MarketEvent::FeedGap(gap) if gap.stream == Stream::OrderBook => {
                self.invalidate(Invalidation::Gap(gap.reason))
            }
            MarketEvent::BookSnapshot(snapshot) => self.reset(snapshot),
            MarketEvent::BookUpdate(update) => self.update(update),
            _ => BookStep::Unrelated,
        }
    }

    fn invalidate(&mut self, why: Invalidation) -> BookStep {
        self.bids.clear();
        self.asks.clear();
        self.chain = Chain::Invalid;
        self.window = TrustedWindow::default();
        BookStep::Invalidated(why)
    }

    fn reset(&mut self, snapshot: &BookSnapshot) -> BookStep {
        if let Some(why) = negative(&snapshot.bids, &snapshot.asks) {
            return self.invalidate(why);
        }
        self.bids.clear();
        self.asks.clear();
        set_levels(&mut self.bids, &snapshot.bids);
        set_levels(&mut self.asks, &snapshot.asks);
        self.window = TrustedWindow {
            lowest_bid: snapshot.bids.iter().map(|l| l.price).min(),
            highest_ask: snapshot.asks.iter().map(|l| l.price).max(),
        };
        self.chain = Chain::Straddle(snapshot.last_update_id);
        BookStep::Reset
    }

    fn update(&mut self, update: &BookUpdate) -> BookStep {
        match self.chain {
            Chain::Invalid => return BookStep::Ignored,
            Chain::Straddle(snapshot_id) => {
                if !(update.first_update_id <= snapshot_id && snapshot_id <= update.last_update_id)
                {
                    return self.invalidate(Invalidation::MissedStraddle {
                        snapshot_id,
                        first_update_id: update.first_update_id,
                        last_update_id: update.last_update_id,
                    });
                }
            }
            Chain::Chained(last) => {
                if update.prev_update_id != last {
                    return self.invalidate(Invalidation::ChainBreak {
                        expected: last,
                        found: update.prev_update_id,
                    });
                }
            }
        }
        if let Some(why) = negative(&update.bids, &update.asks) {
            return self.invalidate(why);
        }
        set_levels(&mut self.bids, &update.bids);
        set_levels(&mut self.asks, &update.asks);
        self.chain = Chain::Chained(update.last_update_id);
        BookStep::Applied
    }

    /// Whether the book is anchored at a snapshot and its chain is intact.
    pub fn is_valid(&self) -> bool {
        self.chain != Chain::Invalid
    }

    /// The last update id the book reflects: its snapshot's, or the last
    /// applied update's; `None` while invalid.
    pub fn last_update_id(&self) -> Option<u64> {
        match self.chain {
            Chain::Invalid => None,
            Chain::Straddle(id) | Chain::Chained(id) => Some(id),
        }
    }

    /// The fully known price range; empty while invalid.
    pub fn window(&self) -> TrustedWindow {
        self.window
    }

    /// The highest bid.
    pub fn best_bid(&self) -> Option<Level> {
        self.bids
            .last_key_value()
            .map(|(&price, &qty)| Level { price, qty })
    }

    /// The lowest ask.
    pub fn best_ask(&self) -> Option<Level> {
        self.asks
            .first_key_value()
            .map(|(&price, &qty)| Level { price, qty })
    }

    /// The bid levels, best (highest) first.
    pub fn bids(&self) -> impl Iterator<Item = Level> + '_ {
        self.bids
            .iter()
            .rev()
            .map(|(&price, &qty)| Level { price, qty })
    }

    /// The ask levels, best (lowest) first.
    pub fn asks(&self) -> impl Iterator<Item = Level> + '_ {
        self.asks.iter().map(|(&price, &qty)| Level { price, qty })
    }

    /// Number of levels, both sides.
    pub fn len(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    /// Whether the book holds no level.
    pub fn is_empty(&self) -> bool {
        self.bids.is_empty() && self.asks.is_empty()
    }

    /// Compares every level inside both books' trusted windows, per side;
    /// `None` when either book is invalid.
    ///
    /// A price counts once when either book has it within the intersected
    /// window; it mismatches when the quantities differ or one book lacks
    /// it. Levels outside the window are never compared: there, an absent
    /// level is unknown, not empty.
    pub fn compare(&self, other: &Self) -> Option<BookComparison> {
        if !self.is_valid() || !other.is_valid() {
            return None;
        }
        let window = self.window.intersect(other.window);
        let mut levels_compared = 0;
        let mut mismatches = Vec::new();
        if let Some(lowest) = window.lowest_bid {
            let prices: BTreeSet<Price> = self
                .bids
                .range(lowest..)
                .chain(other.bids.range(lowest..))
                .map(|(&price, _)| price)
                .collect();
            levels_compared += prices.len();
            for &price in prices.iter().rev() {
                push_mismatch(&mut mismatches, Side::Bid, price, &self.bids, &other.bids);
            }
        }
        if let Some(highest) = window.highest_ask {
            let prices: BTreeSet<Price> = self
                .asks
                .range(..=highest)
                .chain(other.asks.range(..=highest))
                .map(|(&price, _)| price)
                .collect();
            levels_compared += prices.len();
            for &price in &prices {
                push_mismatch(&mut mismatches, Side::Ask, price, &self.asks, &other.asks);
            }
        }
        Some(BookComparison {
            window,
            levels_compared,
            mismatches,
        })
    }
}

/// The first negative quantity among `bids` and `asks`.
fn negative(bids: &[Level], asks: &[Level]) -> Option<Invalidation> {
    let side = |levels: &[Level], side: Side| {
        levels
            .iter()
            .find(|l| l.qty.units() < 0)
            .map(|l| Invalidation::NegativeQty {
                side,
                price: l.price,
            })
    };
    side(bids, Side::Bid).or_else(|| side(asks, Side::Ask))
}

/// Applies `levels` in source order: zero removes, anything else sets.
fn set_levels(book: &mut BTreeMap<Price, Qty>, levels: &[Level]) {
    for level in levels {
        if level.qty.units() == 0 {
            book.remove(&level.price);
        } else {
            book.insert(level.price, level.qty);
        }
    }
}

fn push_mismatch(
    out: &mut Vec<LevelMismatch>,
    side: Side,
    price: Price,
    ours: &BTreeMap<Price, Qty>,
    theirs: &BTreeMap<Price, Qty>,
) {
    let (ours, theirs) = (ours.get(&price).copied(), theirs.get(&price).copied());
    if ours != theirs {
        out.push(LevelMismatch {
            side,
            price,
            ours,
            theirs,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::FeedGap;
    use crate::time::EventTime;

    fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    fn lv(price: i64, qty: i64) -> Level {
        Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        }
    }

    fn snapshot(id: u64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> MarketEvent {
        MarketEvent::BookSnapshot(BookSnapshot {
            time: t(1_000),
            last_update_id: id,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
        })
    }

    fn update(
        first: u64,
        last: u64,
        prev: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> MarketEvent {
        MarketEvent::BookUpdate(BookUpdate {
            time: t(1_000),
            first_update_id: first,
            last_update_id: last,
            prev_update_id: prev,
            bids: bids.iter().map(|&(p, q)| lv(p, q)).collect(),
            asks: asks.iter().map(|&(p, q)| lv(p, q)).collect(),
        })
    }

    fn gap(stream: Stream) -> MarketEvent {
        MarketEvent::FeedGap(FeedGap {
            stream,
            start: t(900),
            end: t(1_000),
            reason: GapReason::SequenceBreak,
        })
    }

    /// Bids 98, 99, 100 and asks 101, 102, 103, last update id 100.
    fn base() -> OrderBook {
        let mut book = OrderBook::new();
        let step = book.apply(&snapshot(
            100,
            &[(99, 20), (100, 10), (98, 30)],
            &[(101, 11), (102, 21), (103, 31)],
        ));
        assert_eq!(step, BookStep::Reset);
        book
    }

    /// `(price, qty)` units per level, best first.
    type Units = Vec<(i64, i64)>;

    fn levels(book: &OrderBook) -> (Units, Units) {
        let pairs = |it: &mut dyn Iterator<Item = Level>| -> Vec<(i64, i64)> {
            it.map(|l| (l.price.units(), l.qty.units())).collect()
        };
        (pairs(&mut book.bids()), pairs(&mut book.asks()))
    }

    #[test]
    fn a_snapshot_resets_the_book_and_sets_the_window() {
        let mut book = base();
        assert!(book.is_valid());
        assert_eq!(book.last_update_id(), Some(100));
        assert_eq!(
            levels(&book),
            (
                vec![(100, 10), (99, 20), (98, 30)],
                vec![(101, 11), (102, 21), (103, 31)]
            )
        );
        assert_eq!(book.best_bid(), Some(lv(100, 10)));
        assert_eq!(book.best_ask(), Some(lv(101, 11)));
        assert_eq!((book.len(), book.is_empty()), (6, false));
        assert_eq!(
            book.window(),
            TrustedWindow {
                lowest_bid: Some(Price::from_units(98)),
                highest_ask: Some(Price::from_units(103)),
            }
        );
        // A second snapshot replaces everything, also the window.
        assert_eq!(book.apply(&snapshot(150, &[(97, 1)], &[])), BookStep::Reset);
        assert_eq!(levels(&book), (vec![(97, 1)], vec![]));
        assert_eq!(
            book.window(),
            TrustedWindow {
                lowest_bid: Some(Price::from_units(97)),
                highest_ask: None,
            }
        );
        assert_eq!(book.last_update_id(), Some(150));
    }

    #[test]
    fn the_first_update_must_straddle_the_snapshot() {
        for (first, last) in [(95, 100), (100, 100), (100, 105), (90, 110)] {
            let mut book = base();
            assert_eq!(
                book.apply(&update(first, last, 0, &[], &[])),
                BookStep::Applied,
                "{first}..={last}"
            );
            assert_eq!(book.last_update_id(), Some(last));
        }
        for (first, last) in [(101, 105), (90, 99)] {
            let mut book = base();
            assert_eq!(
                book.apply(&update(first, last, 0, &[], &[])),
                BookStep::Invalidated(Invalidation::MissedStraddle {
                    snapshot_id: 100,
                    first_update_id: first,
                    last_update_id: last,
                }),
                "{first}..={last}"
            );
            assert!(!book.is_valid());
            assert!(book.is_empty());
            assert_eq!(book.last_update_id(), None);
        }
    }

    #[test]
    fn later_updates_chain_on_prev_update_id() {
        let mut book = base();
        assert_eq!(
            book.apply(&update(96, 104, 95, &[(100, 12)], &[])),
            BookStep::Applied
        );
        assert_eq!(
            book.apply(&update(107, 110, 104, &[], &[(101, 0)])),
            BookStep::Applied
        );
        assert_eq!(book.last_update_id(), Some(110));
        assert_eq!(book.best_bid(), Some(lv(100, 12)));
        assert_eq!(book.best_ask(), Some(lv(102, 21)));
        assert_eq!(
            book.apply(&update(115, 120, 111, &[], &[])),
            BookStep::Invalidated(Invalidation::ChainBreak {
                expected: 110,
                found: 111
            })
        );
        assert!(!book.is_valid());
        // Nothing applies to an invalid book, not even a chaining update.
        assert_eq!(
            book.apply(&update(121, 125, 120, &[(50, 1)], &[])),
            BookStep::Ignored
        );
        assert!(book.is_empty());
        assert_eq!(book.window(), TrustedWindow::default());
        // The next snapshot makes it valid again.
        assert_eq!(book.apply(&snapshot(130, &[(1, 1)], &[])), BookStep::Reset);
        assert!(book.is_valid());
    }

    #[test]
    fn zero_removes_a_level_and_removing_an_absent_one_is_a_no_op() {
        let mut book = base();
        book.apply(&update(
            100,
            101,
            0,
            &[(99, 0), (97, 0), (96, 5), (96, 7)],
            &[(104, 0), (102, 0)],
        ));
        assert_eq!(
            levels(&book),
            (
                vec![(100, 10), (98, 30), (96, 7)],
                vec![(101, 11), (103, 31)]
            )
        );
        // Source order: set, then remove, leaves nothing.
        book.apply(&update(102, 102, 101, &[(95, 3), (95, 0)], &[]));
        assert_eq!(book.bids().count(), 3);
    }

    #[test]
    fn negative_quantities_invalidate() {
        let mut book = base();
        assert_eq!(
            book.apply(&update(100, 101, 0, &[(99, 1)], &[(102, -1)])),
            BookStep::Invalidated(Invalidation::NegativeQty {
                side: Side::Ask,
                price: Price::from_units(102),
            })
        );
        assert!(!book.is_valid());
        let mut book = OrderBook::new();
        assert_eq!(
            book.apply(&snapshot(1, &[(5, -2)], &[])),
            BookStep::Invalidated(Invalidation::NegativeQty {
                side: Side::Bid,
                price: Price::from_units(5),
            })
        );
    }

    #[test]
    fn only_an_order_book_gap_invalidates() {
        let mut book = base();
        assert_eq!(book.apply(&gap(Stream::Trades)), BookStep::Unrelated);
        assert!(book.is_valid());
        assert_eq!(
            book.apply(&crate::event::samples::trade(1_000, 1)),
            BookStep::Unrelated
        );
        assert_eq!(
            book.apply(&gap(Stream::OrderBook)),
            BookStep::Invalidated(Invalidation::Gap(GapReason::SequenceBreak))
        );
        assert!(!book.is_valid());
        assert!(book.is_empty());
        // A fresh book ignores updates until its first snapshot.
        let mut fresh = OrderBook::default();
        assert_eq!(fresh.apply(&update(1, 2, 0, &[], &[])), BookStep::Ignored);
    }

    #[test]
    fn equal_books_compare_equal_inside_the_window() {
        let book = base();
        let comparison = book.compare(&book.clone()).unwrap();
        assert!(comparison.is_match());
        assert_eq!(comparison.levels_compared, 6);
        assert_eq!(comparison.window, book.window());
        assert_eq!(book.compare(&OrderBook::new()), None);
        assert_eq!(OrderBook::new().compare(&book), None);
    }

    #[test]
    fn differences_inside_the_window_are_mismatches() {
        // Bids 980, 990, 1000 and asks 1010, 1020, 1030.
        let wide = || {
            let mut book = OrderBook::new();
            book.apply(&snapshot(
                100,
                &[(980, 30), (990, 20), (1_000, 10)],
                &[(1_010, 11), (1_020, 21), (1_030, 31)],
            ));
            book
        };
        let ours = wide();
        let mut theirs = wide();
        theirs.apply(&update(
            100,
            101,
            0,
            // 990 differs, 1000 is missing, 995 is extra.
            &[(990, 21), (1_000, 0), (995, 5)],
            // 1020 is missing, 1010 differs, 1040 lies beyond the window.
            &[(1_020, 0), (1_010, 12), (1_040, 4)],
        ));
        let comparison = ours.compare(&theirs).unwrap();
        let mismatch = |side, price, ours: Option<i64>, theirs: Option<i64>| LevelMismatch {
            side,
            price: Price::from_units(price),
            ours: ours.map(Qty::from_units),
            theirs: theirs.map(Qty::from_units),
        };
        assert_eq!(
            comparison.mismatches,
            [
                mismatch(Side::Bid, 1_000, Some(10), None),
                mismatch(Side::Bid, 995, None, Some(5)),
                mismatch(Side::Bid, 990, Some(20), Some(21)),
                mismatch(Side::Ask, 1_010, Some(11), Some(12)),
                mismatch(Side::Ask, 1_020, Some(21), None),
            ]
        );
        // 980, 990, 995, 1000 and 1010, 1020, 1030; not 1040.
        assert_eq!(comparison.levels_compared, 7);
        assert!(!comparison.is_match());
        // The comparison is symmetric up to the roles.
        let back = theirs.compare(&ours).unwrap();
        assert_eq!(back.levels_compared, 7);
        assert_eq!(back.mismatches.len(), 5);
        assert_eq!(back.mismatches[1].ours, Some(Qty::from_units(5)));
    }

    #[test]
    fn the_compared_window_is_the_intersection() {
        // Ours knows bids down to 98 and asks up to 103; theirs only the
        // top level of each side.
        let ours = base();
        let mut theirs = OrderBook::new();
        theirs.apply(&snapshot(100, &[(100, 10)], &[(101, 11)]));
        let comparison = ours.compare(&theirs).unwrap();
        assert_eq!(
            comparison.window,
            TrustedWindow {
                lowest_bid: Some(Price::from_units(100)),
                highest_ask: Some(Price::from_units(101)),
            }
        );
        assert_eq!(comparison.levels_compared, 2);
        assert!(comparison.is_match());
        assert_eq!(theirs.compare(&ours).unwrap(), comparison);

        // A side without a window is not compared at all.
        let mut bids_only = OrderBook::new();
        bids_only.apply(&snapshot(100, &[(100, 99)], &[]));
        let comparison = ours.compare(&bids_only).unwrap();
        assert_eq!(comparison.window.highest_ask, None);
        assert_eq!(comparison.levels_compared, 1);
        assert_eq!(comparison.mismatches.len(), 1);

        // A level changed outside the window is known but not compared.
        let mut moved = base();
        moved.apply(&update(100, 101, 0, &[(50, 1)], &[(200, 1)]));
        assert!(base().compare(&moved).unwrap().is_match());
        assert_eq!(moved.window(), base().window());
    }
}
