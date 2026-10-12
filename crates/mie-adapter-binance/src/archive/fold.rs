//! The aggTrades fold (ADR-050): trades the `aggTrades` archive omits,
//! restored from the `trades` archive of the same source day.
//!
//! The `aggTrades` archive of 2025-10 … 2026-06-09 leaves individual trades
//! out of every aggregate's `first_trade_id ..= last_trade_id` range
//! (ADR-047). Aggregate ids stay consecutive, so no whole aggregate is
//! missing: the lost trades are the tail of the aggregate right below them
//! (ADR-050 D3, measured on all 2109 known ones).
//!
//! [`fold_day`] is a pure pass over one source day, both inputs in id order.
//! For a trade `u`, `P` is the aggregate with the largest
//! `last_trade_id < u` and `N` the one with the smallest
//! `first_trade_id > u`:
//!
//! - **Covered.** A trade inside some aggregate's range is already counted.
//! - **Uncovered.** A trade strictly between two ranges of the day (the
//!   population ADR-047 measured) is folded into `P` when its price and
//!   aggressor equal `P`'s and every earlier uncovered trade between `P`
//!   and `N` was folded too: `P` keeps its id, price, time and side, its
//!   quantity grows, and its restored range stays contiguous. From the
//!   first trade that does not match on, the rest of that gap is
//!   [`Unattributed`] and the replay marks the span with a gap (ADR-050 D4).
//! - **Edge ids.** A trade after the day's last range has that range as
//!   `P` (its `N` opens a later day) and follows the same rule. A trade
//!   before the day's first range belongs to the previous day's last range
//!   when it is inside it (covered, the ADR-047 706/707 shape), or has it as
//!   `P` when it is above it; the replay hands that range over as a
//!   [`Carry`], with whether the run above it already broke. Without a
//!   carry `P` is unknown, and the trade is **unresolved**: counted and
//!   gapped, never folded (ADR-050 D3).
//! - **Repeats.** An aggregate or trade id at or below the previous one is
//!   skipped and counted, as the live [`StreamSequencer`] drops it, so
//!   overlapping sealed files fold as they deliver.
//! - **Contract.** A range with `first > last`, or one that starts at or
//!   below the previous range's end, fails the fold naming its aggregate.
//!
//! The pass keeps O(1) state beyond its output, so the replay can feed it
//! one sealed part at a time.
//!
//! [`StreamSequencer`]: crate::sequence::StreamSequencer

use mie_domain::event::{Aggressor, Trade};
use mie_domain::num::{Price, Qty};
use mie_domain::time::EventTime;
use std::collections::BTreeMap;
use std::fmt;

/// Whether an archive replay folds the `trades` archive into `aggTrades`
/// (ADR-050). Every caller states it; there is no hidden default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TradeCorrection {
    /// Fold uncovered trades into their aggregate (ADR-050 D3), reading the
    /// `trades` files of every source day that has them.
    Fold,
    /// Deliver every aggregate exactly as normalized: the data and dataset
    /// versions recorded before ADR-050.
    None,
}

impl TradeCorrection {
    /// The command-line spelling: `fold` or `none`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Fold => "fold",
            Self::None => "none",
        }
    }

    /// The correction spelled `text` (`fold` or `none`), if any.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "fold" => Some(Self::Fold),
            "none" => Some(Self::None),
            _ => Option::None,
        }
    }
}

impl fmt::Display for TradeCorrection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One `aggTrades` row as the fold needs it: the aggregate and the
/// constituent trade-id range the normalized `Trade` drops.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggRange {
    /// The aggregate id (the delivered event's trade id).
    pub agg_id: u64,
    /// First constituent trade id.
    pub first_trade_id: u64,
    /// Last constituent trade id.
    pub last_trade_id: u64,
    /// The aggregate's price.
    pub price: Price,
    /// The side that crossed the spread.
    pub aggressor: Aggressor,
    /// The aggregate's `transact_time`.
    pub time: EventTime,
}

/// What an aggregate gains from the trades folded into it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Fold {
    /// Summed quantity of the folded trades.
    pub qty: Qty,
    /// Folded trades.
    pub trades: u64,
    /// The largest folded trade id: the aggregate's restored
    /// `last_trade_id`.
    pub last_trade_id: u64,
}

/// An uncovered trade that folds nowhere: its price or aggressor differs
/// from `P`'s, or an earlier trade of the same gap did not fold (ADR-050
/// D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Unattributed {
    /// The trade id.
    pub trade_id: u64,
    /// The trade's time.
    pub time: EventTime,
    /// The aggregate right below it (`P`), possibly the previous day's last.
    pub after: AggRange,
    /// The aggregate right above it (`N`), before which the replay
    /// delivers the gap; `None` after the day's last range, where `N` opens
    /// a later day and the gap goes before the next aggregate after `P`.
    pub before: Option<AggRange>,
}

/// What the fold of one day hands to the next (module docs): the day's
/// last range and whether the run of uncovered trades above it broke.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Carry {
    /// The day's last aggregate range: `P` of the next day's head trades.
    pub range: AggRange,
    /// An uncovered trade above `range` was unattributed, so no later one
    /// of that gap folds.
    pub broken: bool,
}

/// The fold of one source day.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DayFold {
    /// Per aggregate id, what is folded into it (an edge fold may target
    /// the previous day's last aggregate).
    pub folds: BTreeMap<u64, Fold>,
    /// Uncovered trades that fold nowhere, in id order.
    pub unattributed: Vec<Unattributed>,
    /// Uncovered trades with a known `P`: folded plus unattributed, edge
    /// ids included.
    pub uncovered: u64,
    /// Of `uncovered`, the edge ids: after the day's last range, or before
    /// its first and above the carried range.
    pub edge: u64,
    /// Of the folded trades, the edge ids.
    pub edge_folded: u64,
    /// Their summed quantity.
    pub edge_qty: Qty,
    /// Trades before the day's first range whose `P` the fold does not
    /// have (no carry, or below the carried range), and every trade of a
    /// day without ranges: neither covered nor folded.
    pub unresolved: u64,
    /// The day's first range when an unresolved trade lies below it: the
    /// replay delivers their gap before it.
    pub unresolved_before: Option<AggRange>,
    /// Aggregate and trade rows skipped as repeated or regressing ids.
    pub repeats: u64,
    /// What the next day's fold starts from; `None` for a day without
    /// ranges.
    pub carry: Option<Carry>,
}

impl Default for DayFold {
    fn default() -> Self {
        Self {
            folds: BTreeMap::new(),
            unattributed: Vec::new(),
            uncovered: 0,
            edge: 0,
            edge_folded: 0,
            edge_qty: Qty::from_units(0),
            unresolved: 0,
            unresolved_before: None,
            repeats: 0,
            carry: None,
        }
    }
}

impl DayFold {
    /// Folded trades, summed over the aggregates.
    pub fn folded_trades(&self) -> u64 {
        self.folds.values().map(|f| f.trades).sum()
    }
}

/// Why a fold failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FoldError {
    /// An input row could not be read or normalized.
    Input(String),
    /// The aggregate ranges break their contract (module docs).
    Contract(String),
    /// A folded quantity exceeds the fixed-point range.
    Overflow {
        /// The aggregate whose quantity overflowed.
        agg_id: u64,
    },
}

impl fmt::Display for FoldError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Input(detail) | Self::Contract(detail) => f.write_str(detail),
            Self::Overflow { agg_id } => {
                write!(f, "the folded quantity of aggregate {agg_id} overflows")
            }
        }
    }
}

/// The aggregate ranges in id order, repeats skipped and the range
/// contract checked.
struct Ranges<I> {
    inner: I,
    last: Option<AggRange>,
    repeats: u64,
}

impl<I: Iterator<Item = Result<AggRange, FoldError>>> Ranges<I> {
    fn next(&mut self) -> Result<Option<AggRange>, FoldError> {
        for range in self.inner.by_ref() {
            let range = range?;
            if range.first_trade_id > range.last_trade_id {
                return Err(FoldError::Contract(format!(
                    "aggregate {}: first trade id {} is above its last {}",
                    range.agg_id, range.first_trade_id, range.last_trade_id
                )));
            }
            if let Some(last) = self.last {
                if range.agg_id <= last.agg_id {
                    self.repeats += 1;
                    continue;
                }
                if range.first_trade_id <= last.last_trade_id {
                    return Err(FoldError::Contract(format!(
                        "aggregate {}: trade ids {} ..= {} overlap aggregate {} ending at {}",
                        range.agg_id,
                        range.first_trade_id,
                        range.last_trade_id,
                        last.agg_id,
                        last.last_trade_id
                    )));
                }
            }
            self.last = Some(range);
            return Ok(Some(range));
        }
        Ok(None)
    }
}

/// Folds one source day: `ranges` are the day's `aggTrades` rows, `trades`
/// its `trades` rows, each in id order, and `carry` what the previous
/// source day's fold handed over, if the replay has that day (module docs).
///
/// # Errors
///
/// The first input error, a broken range contract, or a quantity overflow.
pub fn fold_day(
    carry: Option<Carry>,
    ranges: impl Iterator<Item = Result<AggRange, FoldError>>,
    trades: impl Iterator<Item = Result<Trade, FoldError>>,
) -> Result<DayFold, FoldError> {
    let mut ranges = Ranges {
        inner: ranges,
        last: None,
        repeats: 0,
    };
    let mut day = DayFold::default();
    // `previous` ends below the current trade; `current` ends at or above it.
    let mut previous: Option<AggRange> = None;
    let mut current = ranges.next()?;
    let first = current;
    // The `P` of the gap being folded, and whether that gap's run broke.
    let mut run: Option<(u64, bool)> = carry.map(|c| (c.range.agg_id, c.broken));
    let mut last_trade: Option<u64> = None;
    for trade in trades {
        let trade = trade?;
        if last_trade.is_some_and(|last| trade.trade_id <= last) {
            day.repeats += 1;
            continue;
        }
        last_trade = Some(trade.trade_id);
        while let Some(range) = current.filter(|r| r.last_trade_id < trade.trade_id) {
            previous = Some(range);
            current = ranges.next()?;
        }
        if current.is_some_and(|above| above.first_trade_id <= trade.trade_id) {
            continue;
        }
        let (below, edge) = match (previous, carry) {
            // A day without ranges has no aggregate to fold into.
            _ if first.is_none() => {
                day.unresolved += 1;
                continue;
            }
            (Some(below), _) => (below, current.is_none()),
            // Below the day's first range: the carried range's, if any.
            (None, Some(c)) if trade.trade_id > c.range.last_trade_id => (c.range, true),
            (None, Some(c)) if trade.trade_id >= c.range.first_trade_id => continue,
            _ => {
                day.unresolved += 1;
                day.unresolved_before = first;
                continue;
            }
        };
        day.uncovered += 1;
        day.edge += u64::from(edge);
        let broken = run.is_some_and(|(p, broken)| p == below.agg_id && broken);
        if !broken && trade.price == below.price && trade.aggressor == below.aggressor {
            let overflow = || FoldError::Overflow {
                agg_id: below.agg_id,
            };
            let fold = day.folds.entry(below.agg_id).or_insert(Fold {
                qty: Qty::from_units(0),
                trades: 0,
                last_trade_id: below.last_trade_id,
            });
            fold.qty = fold.qty.checked_add(trade.qty).ok_or_else(overflow)?;
            fold.trades += 1;
            fold.last_trade_id = trade.trade_id;
            if edge {
                day.edge_folded += 1;
                day.edge_qty = day.edge_qty.checked_add(trade.qty).ok_or_else(overflow)?;
            }
            run = Some((below.agg_id, false));
        } else {
            day.unattributed.push(Unattributed {
                trade_id: trade.trade_id,
                time: trade.time,
                after: below,
                before: current,
            });
            run = Some((below.agg_id, true));
        }
    }
    // The rest of the ranges: their contract and repeats still count.
    while ranges.next()?.is_some() {}
    day.repeats += ranges.repeats;
    day.carry = ranges.last.map(|range| Carry {
        range,
        broken: run.is_some_and(|(p, broken)| p == range.agg_id && broken),
    });
    Ok(day)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    fn qty(text: &str) -> Qty {
        text.parse().unwrap()
    }

    fn range(agg_id: u64, first: u64, last: u64, p: &str, aggressor: Aggressor) -> AggRange {
        AggRange {
            agg_id,
            first_trade_id: first,
            last_trade_id: last,
            price: price(p),
            aggressor,
            time: EventTime::from_millis(1_000 * agg_id as i64),
        }
    }

    fn trade(id: u64, p: &str, q: &str, aggressor: Aggressor) -> Trade {
        Trade {
            time: EventTime::from_millis(10 * id as i64),
            trade_id: id,
            price: price(p),
            qty: qty(q),
            aggressor,
        }
    }

    fn run(ranges: &[AggRange], trades: &[Trade]) -> Result<DayFold, FoldError> {
        run_after(None, ranges, trades)
    }

    fn run_after(
        carry: Option<Carry>,
        ranges: &[AggRange],
        trades: &[Trade],
    ) -> Result<DayFold, FoldError> {
        fold_day(
            carry,
            ranges.iter().copied().map(Ok),
            trades.iter().copied().map(Ok),
        )
    }

    /// The fold of a day where nothing is uncovered: only the carry.
    fn nothing(ranges: &[AggRange]) -> DayFold {
        DayFold {
            carry: ranges.last().map(|&range| Carry {
                range,
                broken: false,
            }),
            ..DayFold::default()
        }
    }

    use Aggressor::{Buy, Sell};

    /// Three aggregates over ids 1 ..= 2, 5 ..= 5 and 8 ..= 9.
    fn three() -> Vec<AggRange> {
        vec![
            range(10, 1, 2, "100.0", Buy),
            range(11, 5, 5, "100.5", Sell),
            range(12, 8, 9, "101.0", Buy),
        ]
    }

    #[test]
    fn covered_trades_fold_nothing() {
        let trades = [
            trade(1, "100.0", "0.1", Buy),
            trade(2, "100.0", "0.1", Buy),
            trade(5, "100.5", "0.2", Sell),
            trade(8, "101.0", "0.3", Buy),
            trade(9, "101.0", "0.3", Buy),
        ];
        assert_eq!(run(&three(), &trades), Ok(nothing(&three())));
    }

    #[test]
    fn one_tail_trade_folds_into_the_aggregate_below() {
        let trades = [
            trade(2, "100.0", "0.1", Buy),
            trade(3, "100.0", "0.25", Buy),
            trade(5, "100.5", "0.2", Sell),
        ];
        let day = run(&three(), &trades).unwrap();
        assert_eq!(
            day.folds,
            BTreeMap::from([(
                10,
                Fold {
                    qty: qty("0.25"),
                    trades: 1,
                    last_trade_id: 3
                }
            )])
        );
        assert_eq!((day.uncovered, day.folded_trades()), (1, 1));
        assert!(day.unattributed.is_empty());
    }

    #[test]
    fn several_tail_trades_of_one_gap_fold_together() {
        let trades = [
            trade(6, "100.5", "0.01", Sell),
            trade(7, "100.5", "0.02", Sell),
        ];
        let day = run(&three(), &trades).unwrap();
        assert_eq!(
            day.folds[&11],
            Fold {
                qty: qty("0.03"),
                trades: 2,
                last_trade_id: 7
            }
        );
        assert_eq!(day.uncovered, 2);
    }

    #[test]
    fn a_gap_without_trade_rows_folds_nothing() {
        // Ids 3, 4, 6 and 7 lie between ranges, but the sparse `trades`
        // file has no row for them (ADR-045).
        let trades = [trade(1, "100.0", "0.1", Buy), trade(9, "101.0", "0.1", Buy)];
        assert_eq!(run(&three(), &trades), Ok(nothing(&three())));
    }

    #[test]
    fn a_price_or_side_mismatch_is_unattributed() {
        let trades = [trade(3, "100.1", "0.1", Buy), trade(6, "100.5", "0.1", Buy)];
        let day = run(&three(), &trades).unwrap();
        assert!(day.folds.is_empty());
        assert_eq!(day.uncovered, 2);
        assert_eq!(
            day.unattributed,
            [
                Unattributed {
                    trade_id: 3,
                    time: EventTime::from_millis(30),
                    after: three()[0],
                    before: Some(three()[1])
                },
                Unattributed {
                    trade_id: 6,
                    time: EventTime::from_millis(60),
                    after: three()[1],
                    before: Some(three()[2])
                }
            ]
        );
    }

    /// #109 review R3: once a trade of a gap is unattributed, a later one
    /// of the same gap at `P`'s price and side does not fold either, or
    /// `P`'s restored range would skip the unattributed id (ADR-050 D3).
    #[test]
    fn a_gap_folds_only_its_matching_run_right_above_the_aggregate() {
        let trades = [
            trade(3, "100.1", "0.1", Buy),
            trade(4, "100.0", "0.2", Buy),
            // The next gap starts a new run.
            trade(6, "100.5", "0.3", Sell),
        ];
        let day = run(&three(), &trades).unwrap();
        assert_eq!(
            day.unattributed
                .iter()
                .map(|u| u.trade_id)
                .collect::<Vec<_>>(),
            [3, 4]
        );
        assert!(day.unattributed.iter().all(|u| u.after == three()[0]));
        assert_eq!(
            day.folds,
            BTreeMap::from([(
                11,
                Fold {
                    qty: qty("0.3"),
                    trades: 1,
                    last_trade_id: 6
                }
            )])
        );
        assert_eq!(day.uncovered, 3);

        // A matching run folds up to the first mismatch only.
        let trades = [trade(3, "100.0", "0.1", Buy), trade(4, "100.1", "0.1", Buy)];
        let day = run(&three(), &trades).unwrap();
        assert_eq!(day.folds[&10].last_trade_id, 3);
        assert_eq!(day.unattributed[0].trade_id, 4);
    }

    /// #109 review R1: a trade after the day's last range has that range as
    /// `P` and is folded or unattributed within the day, never ignored.
    #[test]
    fn a_trade_after_the_last_range_is_its_tail() {
        let trades = [
            trade(10, "101.0", "0.1", Buy),
            trade(11, "101.1", "0.1", Buy),
            trade(12, "101.0", "0.1", Buy),
        ];
        let day = run(&three(), &trades).unwrap();
        assert_eq!(
            day.folds,
            BTreeMap::from([(
                12,
                Fold {
                    qty: qty("0.1"),
                    trades: 1,
                    last_trade_id: 10
                }
            )])
        );
        assert_eq!(
            day.unattributed
                .iter()
                .map(|u| (u.trade_id, u.after.agg_id, u.before))
                .collect::<Vec<_>>(),
            [(11, 12, None), (12, 12, None)]
        );
        assert_eq!((day.uncovered, day.edge), (3, 3));
        assert_eq!((day.edge_folded, day.edge_qty), (1, qty("0.1")));
        assert_eq!(
            day.carry,
            Some(Carry {
                range: three()[2],
                broken: true
            })
        );
    }

    /// #109 review R2: a trade before the day's first range is covered by,
    /// or the tail of, the previous day's last range; without that range
    /// it is unresolved.
    #[test]
    fn a_trade_before_the_first_range_resolves_with_the_carry() {
        let previous = range(9, 0, 0, "99.0", Sell);
        let ranges = [range(10, 5, 5, "100.0", Buy), range(11, 6, 6, "100.0", Buy)];
        let trades = [
            trade(0, "99.0", "0.1", Sell),
            trade(1, "99.0", "0.2", Sell),
            trade(2, "99.0", "0.3", Sell),
            trade(5, "100.0", "0.1", Buy),
        ];
        let carry = Carry {
            range: previous,
            broken: false,
        };
        let day = run_after(Some(carry), &ranges, &trades).unwrap();
        // Trade 0 is the carried range's own; 1 and 2 are its tail.
        assert_eq!(
            day.folds,
            BTreeMap::from([(
                9,
                Fold {
                    qty: qty("0.5"),
                    trades: 2,
                    last_trade_id: 2
                }
            )])
        );
        assert_eq!((day.uncovered, day.edge, day.edge_folded), (2, 2, 2));
        assert_eq!((day.unresolved, day.unresolved_before), (0, None));

        // The previous day's run already broke: nothing of the gap folds.
        let broken = Carry {
            broken: true,
            ..carry
        };
        let day = run_after(Some(broken), &ranges, &trades).unwrap();
        assert!(day.folds.is_empty());
        assert_eq!(
            day.unattributed
                .iter()
                .map(|u| (u.trade_id, u.after.agg_id, u.before.map(|n| n.agg_id)))
                .collect::<Vec<_>>(),
            [(1, 9, Some(10)), (2, 9, Some(10))]
        );

        // No carry: `P` is unknown, so all three are unresolved, gapped
        // before the day's first range.
        let day = run_after(None, &ranges, &trades).unwrap();
        assert!(day.folds.is_empty() && day.unattributed.is_empty());
        assert_eq!(day.uncovered, 0);
        assert_eq!(
            (day.unresolved, day.unresolved_before),
            (3, Some(ranges[0]))
        );

        // Below the carried range: unresolved as well.
        let later = Carry {
            range: range(9, 1, 1, "99.0", Sell),
            broken: false,
        };
        let day = run_after(Some(later), &ranges, &trades).unwrap();
        assert_eq!(day.unresolved, 1);
        assert_eq!(day.folds[&9].last_trade_id, 2);

        // A day without ranges folds nothing: every trade is unresolved.
        let day = run_after(Some(carry), &[], &trades).unwrap();
        assert_eq!((day.unresolved, day.unresolved_before), (4, None));
        assert_eq!((day.uncovered, day.carry), (0, None));
    }

    #[test]
    fn repeated_or_regressing_ids_are_skipped_and_counted() {
        let mut ranges = three();
        // A second, overlapping file repeats aggregate 11 and regresses.
        ranges.insert(2, range(11, 5, 5, "100.5", Sell));
        ranges.insert(3, range(10, 1, 2, "100.0", Buy));
        let trades = [
            trade(3, "100.0", "0.1", Buy),
            trade(3, "100.0", "0.1", Buy),
            trade(2, "100.0", "0.1", Buy),
            trade(4, "100.0", "0.2", Buy),
        ];
        let day = run(&ranges, &trades).unwrap();
        assert_eq!(day.repeats, 4);
        assert_eq!(day.folds[&10].qty, qty("0.3"));
        assert_eq!(day.folds[&10].trades, 2);
    }

    #[test]
    fn an_overlapping_or_inverted_range_is_a_contract_error() {
        let overlapping = [range(10, 1, 5, "1.0", Buy), range(11, 5, 6, "1.0", Buy)];
        let inverted = [range(10, 1, 2, "1.0", Buy), range(11, 7, 6, "1.0", Buy)];
        for ranges in [&overlapping[..], &inverted[..]] {
            let error = run(ranges, &[]).unwrap_err();
            assert!(
                matches!(&error, FoldError::Contract(detail) if detail.starts_with("aggregate 11:")),
                "{error}"
            );
        }
    }

    #[test]
    fn a_quantity_overflow_is_an_error() {
        let big = Trade {
            qty: Qty::from_units(i64::MAX),
            ..trade(3, "100.0", "1", Buy)
        };
        let trades = [big, Trade { trade_id: 4, ..big }];
        assert_eq!(
            run(&three(), &trades),
            Err(FoldError::Overflow { agg_id: 10 })
        );
    }

    #[test]
    fn an_input_error_fails_the_fold() {
        let failing = [Err(FoldError::Input("part-00000: bad row".to_owned()))];
        assert_eq!(
            fold_day(None, failing.into_iter(), std::iter::empty()),
            Err(FoldError::Input("part-00000: bad row".to_owned()))
        );
    }

    /// The live truncation of 2026-10-08 (#90 M0.3): a delivered aggregate
    /// starts at the same trade as the complete one but ends early. Folding
    /// the trades up to the next aggregate restores the complete row's `q`
    /// and `l`.
    #[test]
    fn the_live_truncation_shape_restores_the_complete_aggregate() {
        // Complete: aggregate 7 over ids 100 ..= 104, q = 0.5.
        let truncated = [
            range(7, 100, 101, "62000.1", Sell),
            range(8, 105, 105, "62000.0", Sell),
        ];
        let trades = [
            trade(100, "62000.1", "0.1", Sell),
            trade(101, "62000.1", "0.1", Sell),
            trade(102, "62000.1", "0.1", Sell),
            trade(103, "62000.1", "0.15", Sell),
            trade(104, "62000.1", "0.05", Sell),
            trade(105, "62000.0", "0.3", Sell),
        ];
        let day = run(&truncated, &trades).unwrap();
        let fold = day.folds[&7];
        assert_eq!(fold.last_trade_id, 104);
        assert_eq!(
            qty("0.2").checked_add(fold.qty),
            Some(qty("0.5")),
            "q of the complete aggregate"
        );
    }

    #[test]
    fn corrections_spell_fold_and_none() {
        for correction in [TradeCorrection::Fold, TradeCorrection::None] {
            assert_eq!(
                TradeCorrection::parse(correction.as_str()),
                Some(correction)
            );
            assert_eq!(correction.to_string(), correction.as_str());
        }
        assert_eq!(TradeCorrection::parse("Fold"), Option::None);
    }
}
