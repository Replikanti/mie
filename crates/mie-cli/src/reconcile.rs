//! Detector C3 (ADR-048), the core of `capture-report` check 7: reconciles
//! closed `kline_1m` frames with the captured aggregate trades in trade-id
//! space, so a trade lost inside an aggregate — which leaves the aggregate
//! id consecutive and journals no gap — still shows.
//!
//! Pure: no I/O. The inputs are one run's records in `receive_seq` order.
//!
//! 1. Each closed minute has the trade-id interval `[first, last]` (`k.f`,
//!    `k.L`) and the volume `k.v`. A minute with no trades has no interval:
//!    it joins no aggregate.
//! 2. An aggregate belongs to every minute whose interval intersects its
//!    `[first, last]` (`f`, `l`); one that intersects none is an orphan at
//!    the minute of its `T`.
//! 3. Minutes that share an aggregate merge into one block, transitively;
//!    each aggregate's quantity counts once, in its block.
//! 4. A block is inconclusive when its minutes are not consecutive, when it
//!    reaches outside the run's aggregate time span, when it overlaps a gap
//!    passed in (the run's `Trades` and `Klines` gaps), or when one of its
//!    minutes is an orphan's minute — first reason in that order.
//! 5. A conclusive block passes when the kline volume equals the aggregate
//!    quantity exactly (ADR-027 fixed point, tolerance 0); otherwise it is
//!    flagged with `difference = kline volume − aggregate quantity`.

use mie_domain::num::Qty;
use std::collections::{BTreeMap, BTreeSet};

/// Milliseconds per minute: `kline_1m` is the finest reference captured
/// (ADR-032 D1).
const MINUTE_MS: i64 = 60_000;

/// One closed `kline_1m` frame (`k.x == true`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClosedMinute {
    /// `k.t / 60000`.
    pub minute: i64,
    /// First trade id, `k.f`.
    pub first: i64,
    /// Last trade id, `k.L` (`k.l` is the low price).
    pub last: i64,
    /// `k.v`.
    pub volume: Qty,
    /// `k.n`; 0 means the minute has no trade-id interval.
    pub trades: u64,
}

/// One `aggTrade` record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AggregateRange {
    /// The aggregate id `a`.
    pub id: u64,
    /// First trade id `f`.
    pub first: i64,
    /// Last trade id `l`.
    pub last: i64,
    /// `q`.
    pub qty: Qty,
    /// Trade time `T`, in ms.
    pub time: i64,
}

/// Why a block is not checked.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Inconclusive {
    /// The block's minutes are not consecutive: a minute in between has no
    /// closed kline.
    NotConsecutive,
    /// The block reaches outside the run's aggregate time span.
    RunEdge,
    /// The block overlaps a journaled `Trades` or `Klines` gap.
    Gap,
    /// One of the block's minutes holds an aggregate that intersects no
    /// closed minute, or an unreadable `aggTrade` record.
    Orphan,
}

/// The verdict on one block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Kline volume equals aggregate quantity.
    Pass,
    /// Conclusive and the sums differ.
    Flagged,
    /// Not checked, for the first reason that applies.
    Inconclusive(Inconclusive),
}

/// A set of minutes linked by shared aggregates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    /// The minutes (`open time / 60000`), ascending.
    pub minutes: Vec<i64>,
    /// Σ `k.v` over the minutes.
    pub kline_volume: Qty,
    /// Σ `q` over the aggregates of the block.
    pub aggregate_qty: Qty,
    /// `kline_volume − aggregate_qty`; positive when trades are missing from
    /// the aggregates.
    pub difference: Qty,
    /// The verdict.
    pub verdict: Verdict,
}

/// Runs C3 over one run; blocks ordered by their first minute.
///
/// `minutes` and `aggregates` are in `receive_seq` order: the last closed
/// record of a minute and the last record of an aggregate id count.
/// `unreadable` holds the record times of `aggTrade` records whose fields
/// could not be read; each is an orphan at its minute. `span` is the run's
/// `[min T, max T]` over its aggregates (`(i64::MAX, i64::MIN)` when it has
/// none, which makes every block a run edge); `gaps` are `[start, end]` in
/// ms.
///
/// # Errors
///
/// When a closed minute with trades has a first trade id at or below the
/// previous such minute's last trade id (the sweep needs intervals that
/// increase with the minute), or on `Qty` overflow.
pub fn reconcile(
    minutes: &[ClosedMinute],
    aggregates: &[AggregateRange],
    unreadable: &[i64],
    span: (i64, i64),
    gaps: &[(i64, i64)],
) -> Result<Vec<Block>, String> {
    let overflow = || "kline reconciliation: quantity overflow".to_owned();
    // Later records overwrite earlier ones: the last in receive_seq wins.
    let by_minute: BTreeMap<i64, &ClosedMinute> = minutes.iter().map(|m| (m.minute, m)).collect();
    let mins: Vec<&ClosedMinute> = by_minute.into_values().collect();
    let by_id: BTreeMap<u64, &AggregateRange> = aggregates.iter().map(|a| (a.id, a)).collect();
    let mut aggs: Vec<&AggregateRange> = by_id.into_values().collect();
    aggs.sort_by_key(|a| (a.first, a.last));

    // Indices of the minutes with a trade-id interval, by minute.
    let traded: Vec<usize> = (0..mins.len()).filter(|&i| mins[i].trades > 0).collect();
    for pair in traded.windows(2) {
        let (prev, next) = (mins[pair[0]], mins[pair[1]]);
        if next.first <= prev.last {
            return Err(format!(
                "closed kline {}: first trade id {} is not above the last trade id {} of \
                 closed kline {}",
                next.minute.saturating_mul(MINUTE_MS),
                next.first,
                prev.last,
                prev.minute.saturating_mul(MINUTE_MS)
            ));
        }
    }

    let mut parent: Vec<usize> = (0..mins.len()).collect();
    let mut qty = vec![Qty::from_units(0); mins.len()];
    let mut orphans: BTreeSet<i64> = unreadable.iter().map(|t| t.div_euclid(MINUTE_MS)).collect();
    // Two pointers: aggregates by first id, minutes by interval.
    let mut j = 0;
    for agg in &aggs {
        while j < traded.len() && mins[traded[j]].last < agg.first {
            j += 1;
        }
        let hits: Vec<usize> = traded[j..]
            .iter()
            .copied()
            .take_while(|&i| mins[i].first <= agg.last)
            .collect();
        let Some((&head, rest)) = hits.split_first() else {
            orphans.insert(agg.time.div_euclid(MINUTE_MS));
            continue;
        };
        qty[head] = qty[head].checked_add(agg.qty).ok_or_else(overflow)?;
        for &other in rest {
            let (a, b) = (find(&mut parent, head), find(&mut parent, other));
            parent[a.max(b)] = a.min(b);
        }
    }

    // Groups in order of their first minute.
    let mut slot: BTreeMap<usize, usize> = BTreeMap::new();
    let mut groups: Vec<Vec<usize>> = Vec::new();
    for i in 0..mins.len() {
        let root = find(&mut parent, i);
        let k = *slot.entry(root).or_insert_with(|| {
            groups.push(Vec::new());
            groups.len() - 1
        });
        groups[k].push(i);
    }

    let mut blocks = Vec::with_capacity(groups.len());
    for group in groups {
        let mut kline_volume = Qty::from_units(0);
        let mut aggregate_qty = Qty::from_units(0);
        for &i in &group {
            kline_volume = kline_volume
                .checked_add(mins[i].volume)
                .ok_or_else(overflow)?;
            aggregate_qty = aggregate_qty.checked_add(qty[i]).ok_or_else(overflow)?;
        }
        let difference = kline_volume
            .checked_sub(aggregate_qty)
            .ok_or_else(overflow)?;
        let minutes: Vec<i64> = group.iter().map(|&i| mins[i].minute).collect();
        let (first, last) = (minutes[0], minutes[minutes.len() - 1]);
        let lo = first.saturating_mul(MINUTE_MS);
        let hi = last.saturating_mul(MINUTE_MS).saturating_add(MINUTE_MS - 1);
        let verdict = if minutes.windows(2).any(|w| w[1] != w[0] + 1) {
            Verdict::Inconclusive(Inconclusive::NotConsecutive)
        } else if lo < span.0 || hi > span.1 {
            Verdict::Inconclusive(Inconclusive::RunEdge)
        } else if gaps.iter().any(|&(start, end)| start <= hi && lo <= end) {
            Verdict::Inconclusive(Inconclusive::Gap)
        } else if minutes.iter().any(|m| orphans.contains(m)) {
            Verdict::Inconclusive(Inconclusive::Orphan)
        } else if difference == Qty::from_units(0) {
            Verdict::Pass
        } else {
            Verdict::Flagged
        };
        blocks.push(Block {
            minutes,
            kline_volume,
            aggregate_qty,
            difference,
            verdict,
        });
    }
    Ok(blocks)
}

/// Union-find root of `i`, with path halving.
fn find(parent: &mut [usize], mut i: usize) -> usize {
    while parent[i] != i {
        parent[i] = parent[parent[i]];
        i = parent[i];
    }
    i
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-08T12:50:00Z as a minute number.
    const M: i64 = 1_791_463_800_000 / MINUTE_MS;

    fn q(units: i64) -> Qty {
        Qty::from_units(units)
    }

    fn minute(minute: i64, first: i64, last: i64, volume: i64) -> ClosedMinute {
        ClosedMinute {
            minute,
            first,
            last,
            volume: q(volume),
            trades: u64::try_from(last - first + 1).unwrap(),
        }
    }

    fn agg(id: u64, first: i64, last: i64, qty: i64, minute: i64) -> AggregateRange {
        AggregateRange {
            id,
            first,
            last,
            qty: q(qty),
            time: minute * MINUTE_MS + 30_000,
        }
    }

    /// A span covering minutes `M - 1 ..= M + 10`.
    fn span() -> (i64, i64) {
        ((M - 1) * MINUTE_MS, (M + 11) * MINUTE_MS - 1)
    }

    fn run(minutes: &[ClosedMinute], aggs: &[AggregateRange]) -> Vec<Block> {
        reconcile(minutes, aggs, &[], span(), &[]).unwrap()
    }

    #[test]
    fn one_aggregate_per_minute_passes() {
        let blocks = run(
            &[minute(M, 10, 12, 30), minute(M + 1, 13, 14, 7)],
            &[agg(1, 10, 12, 30, M), agg(2, 13, 14, 7, M + 1)],
        );
        assert_eq!(blocks.len(), 2);
        assert!(blocks.iter().all(|b| b.verdict == Verdict::Pass));
        assert_eq!(blocks[0].minutes, vec![M]);
        assert_eq!(blocks[1].minutes, vec![M + 1]);
    }

    #[test]
    fn an_aggregate_straddling_two_minutes_merges_them_and_passes() {
        // Trades 10..12 in M, 13..14 in M + 1; aggregate 2 holds 12..13.
        let blocks = run(
            &[minute(M, 10, 12, 30), minute(M + 1, 13, 14, 20)],
            &[
                agg(1, 10, 11, 20, M),
                agg(2, 12, 13, 20, M),
                agg(3, 14, 14, 10, M + 1),
            ],
        );
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].minutes, vec![M, M + 1]);
        assert_eq!(blocks[0].kline_volume, q(50));
        assert_eq!(blocks[0].aggregate_qty, q(50));
        assert_eq!(blocks[0].verdict, Verdict::Pass);
    }

    #[test]
    fn a_truncated_aggregate_is_flagged_with_the_removed_quantity() {
        // The 2026-10-08 shape: aggregate 1 should hold 10..15 (60 units),
        // the stream delivers it ending at 12 with 25 units; the next
        // aggregate starts at the old l + 1. Its `a` stays consecutive.
        let blocks = run(
            &[minute(M, 10, 18, 90)],
            &[agg(1, 10, 12, 25, M), agg(2, 16, 18, 30, M)],
        );
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].verdict, Verdict::Flagged);
        assert_eq!(blocks[0].difference, q(35));
    }

    #[test]
    fn an_aggregate_removed_entirely_is_flagged() {
        let blocks = run(
            &[minute(M, 10, 14, 50)],
            &[agg(1, 10, 11, 20, M), agg(3, 14, 14, 10, M)],
        );
        assert_eq!(blocks[0].verdict, Verdict::Flagged);
        assert_eq!(blocks[0].difference, q(20));
    }

    #[test]
    fn aggregates_holding_more_than_the_kline_are_flagged_negative() {
        let blocks = run(&[minute(M, 10, 11, 20)], &[agg(1, 10, 11, 25, M)]);
        assert_eq!(blocks[0].verdict, Verdict::Flagged);
        assert_eq!(blocks[0].difference, q(-5));
    }

    #[test]
    fn a_block_with_a_missing_minute_inside_is_not_consecutive() {
        // No closed kline for M + 1; aggregate 2 spans M and M + 2.
        let blocks = run(
            &[minute(M, 10, 12, 30), minute(M + 2, 20, 22, 30)],
            &[
                agg(1, 10, 11, 20, M),
                agg(2, 12, 20, 20, M),
                agg(3, 21, 22, 20, M + 2),
            ],
        );
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].minutes, vec![M, M + 2]);
        assert_eq!(
            blocks[0].verdict,
            Verdict::Inconclusive(Inconclusive::NotConsecutive)
        );
    }

    #[test]
    fn a_block_before_the_first_aggregate_time_is_a_run_edge() {
        let minutes = [minute(M, 10, 11, 20), minute(M + 1, 12, 13, 20)];
        let aggs = [agg(1, 10, 11, 20, M), agg(2, 12, 13, 20, M + 1)];
        // The run's first T lies inside M, after its open.
        let span = (M * MINUTE_MS + 30_000, (M + 2) * MINUTE_MS);
        let blocks = reconcile(&minutes, &aggs, &[], span, &[]).unwrap();
        assert_eq!(
            blocks[0].verdict,
            Verdict::Inconclusive(Inconclusive::RunEdge)
        );
        assert_eq!(blocks[1].verdict, Verdict::Pass);
    }

    #[test]
    fn a_block_overlapping_a_gap_is_inconclusive() {
        let minutes = [minute(M, 10, 11, 20), minute(M + 1, 12, 13, 99)];
        let aggs = [agg(1, 10, 11, 20, M), agg(2, 12, 13, 20, M + 1)];
        let gap = ((M + 1) * MINUTE_MS + 59_999, (M + 3) * MINUTE_MS);
        let blocks = reconcile(&minutes, &aggs, &[], span(), &[gap]).unwrap();
        assert_eq!(blocks[0].verdict, Verdict::Pass);
        assert_eq!(blocks[1].verdict, Verdict::Inconclusive(Inconclusive::Gap));
    }

    #[test]
    fn an_orphan_aggregate_makes_the_block_of_its_minute_inconclusive() {
        // Aggregate 2 covers ids that lie in no closed minute.
        let minutes = [minute(M, 10, 11, 20), minute(M + 1, 30, 31, 20)];
        let aggs = [agg(1, 10, 11, 20, M), agg(2, 20, 21, 5, M + 1)];
        let blocks = reconcile(&minutes, &aggs, &[], span(), &[]).unwrap();
        assert_eq!(blocks[0].verdict, Verdict::Pass);
        assert_eq!(
            blocks[1].verdict,
            Verdict::Inconclusive(Inconclusive::Orphan)
        );
        // An unreadable record counts the same.
        let blocks =
            reconcile(&minutes[..1], &aggs[..1], &[M * MINUTE_MS + 5], span(), &[]).unwrap();
        assert_eq!(
            blocks[0].verdict,
            Verdict::Inconclusive(Inconclusive::Orphan)
        );
    }

    #[test]
    fn inconclusive_reasons_follow_their_precedence() {
        // Not consecutive and overlapping a gap: NotConsecutive wins.
        let minutes = [minute(M, 10, 12, 30), minute(M + 2, 20, 22, 30)];
        let aggs = [agg(1, 10, 20, 60, M), agg(2, 21, 22, 0, M + 2)];
        let gap = (M * MINUTE_MS, M * MINUTE_MS);
        let blocks = reconcile(&minutes, &aggs, &[], span(), &[gap]).unwrap();
        assert_eq!(
            blocks[0].verdict,
            Verdict::Inconclusive(Inconclusive::NotConsecutive)
        );
        // A gap and an orphan: Gap wins.
        let blocks = reconcile(&minutes[..1], &aggs[..1], &[M * MINUTE_MS], span(), &[gap]);
        assert_eq!(
            blocks.unwrap()[0].verdict,
            Verdict::Inconclusive(Inconclusive::Gap)
        );
    }

    #[test]
    fn a_duplicate_aggregate_id_counts_once() {
        let blocks = run(
            &[minute(M, 10, 11, 20)],
            &[agg(1, 10, 11, 20, M), agg(1, 10, 11, 20, M)],
        );
        assert_eq!(blocks[0].aggregate_qty, q(20));
        assert_eq!(blocks[0].verdict, Verdict::Pass);
    }

    #[test]
    fn the_last_closed_record_of_a_minute_counts() {
        let blocks = run(
            &[minute(M, 10, 11, 99), minute(M, 10, 11, 20)],
            &[agg(1, 10, 11, 20, M)],
        );
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].kline_volume, q(20));
        assert_eq!(blocks[0].verdict, Verdict::Pass);
    }

    #[test]
    fn a_minute_without_trades_passes_on_zero_volume() {
        let empty = ClosedMinute {
            minute: M + 1,
            first: -1,
            last: -1,
            volume: q(0),
            trades: 0,
        };
        let blocks = run(
            &[
                minute(M, 10, 11, 20),
                empty.clone(),
                minute(M + 2, 12, 13, 20),
            ],
            &[agg(1, 10, 11, 20, M), agg(2, 12, 13, 20, M + 2)],
        );
        assert_eq!(blocks.len(), 3);
        assert!(blocks.iter().all(|b| b.verdict == Verdict::Pass));
        assert_eq!(blocks[1].minutes, vec![M + 1]);
        let nonzero = ClosedMinute {
            volume: q(1),
            ..empty
        };
        let blocks = run(&[nonzero], &[]);
        assert_eq!(blocks[0].verdict, Verdict::Flagged);
    }

    #[test]
    fn decreasing_kline_intervals_are_an_error() {
        let result = reconcile(
            &[minute(M, 10, 12, 30), minute(M + 1, 12, 14, 20)],
            &[],
            &[],
            span(),
            &[],
        );
        assert!(result.unwrap_err().contains("is not above"));
    }

    #[test]
    fn quantity_overflow_is_an_error() {
        let result = reconcile(
            &[minute(M, 10, 11, i64::MAX)],
            &[agg(1, 10, 10, i64::MAX, M), agg(2, 11, 11, 1, M)],
            &[],
            span(),
            &[],
        );
        assert!(result.unwrap_err().contains("overflow"));
    }
}
