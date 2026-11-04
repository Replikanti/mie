use super::*;
use crate::backtest::{CostsError, LevelsError, PositionSide, SkipCounts};
use crate::event::samples::{mark, settlement, t};
use crate::event::{Aggressor, FeedGap, GapReason, Trade};
use crate::num::Qty;

/// A decimal price in USDT.
fn usd(text: &str) -> Price {
    text.parse().unwrap()
}

/// A decimal rate.
fn rate(text: &str) -> Rate {
    text.parse().unwrap()
}

fn trade(millis: i64, trade_id: u64, price: &str) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: t(millis),
        trade_id,
        price: usd(price),
        qty: Qty::from_units(100_000_000),
        aggressor: Aggressor::Buy,
    })
}

fn gap(stream: Stream, start: i64, end: i64) -> MarketEvent {
    MarketEvent::FeedGap(FeedGap {
        stream,
        start: t(start),
        end: t(end),
        reason: GapReason::Disconnected,
    })
}

fn costs(
    latency_ms: u32,
    max_hold_ms: i64,
    slippage: &str,
    taker: &str,
    funding: FundingRule,
) -> Costs {
    Costs::new(
        latency_ms,
        max_hold_ms,
        rate(slippage),
        rate(taker),
        funding,
    )
    .unwrap()
}

/// No latency, a 1 000 ms hold, no slippage, no fee, recorded funding.
fn plain() -> Costs {
    costs(0, 1_000, "0", "0", FundingRule::Recorded)
}

fn levels(side: PositionSide, reference: &str, stop: &str, target: &str) -> Admission {
    Admission::Levels(Levels::new(side, usd(reference), usd(stop), usd(target)).unwrap())
}

fn long(reference: &str, stop: &str, target: &str) -> Admission {
    levels(PositionSide::Long, reference, stop, target)
}

fn short(reference: &str, stop: &str, target: &str) -> Admission {
    levels(PositionSide::Short, reference, stop, target)
}

/// Drives a simulator as the pipeline will (#170): observes accepted
/// events and decides at the last one, with the engine's last trade price
/// as the reference (`trade.last_price@1` keeps its value across a gap and
/// a fresh engine has none).
struct Run {
    simulator: Simulator,
    decision: Option<CanonicalKey>,
    last_price: Option<Price>,
}

impl Run {
    fn new(costs: Costs) -> Self {
        Self {
            simulator: Simulator::new(costs),
            decision: None,
            last_price: None,
        }
    }

    fn on(&mut self, event: MarketEvent) -> &mut Self {
        if let MarketEvent::Trade(trade) = &event {
            self.last_price = Some(trade.price);
        }
        self.decision = Some(event.canonical_key());
        self.simulator.observe(&event);
        self
    }

    fn decide(&mut self, admission: Admission) -> &mut Self {
        self.decide_in(Vec::new(), admission)
    }

    fn decide_in(&mut self, regime: Vec<Option<RegimeLabel>>, admission: Admission) -> &mut Self {
        self.simulator.decide(TradeIntent {
            decision: self.decision.expect("an observed decision event"),
            reference: self.last_price,
            regime,
            admission,
        });
        self
    }

    fn reject(&mut self, event: MarketEvent) -> &mut Self {
        self.simulator.reject(&event);
        self
    }

    fn restart(&mut self) -> &mut Self {
        self.last_price = None;
        self.simulator.restart();
        self
    }

    fn finish(self) -> (Vec<TradeRecord>, SimulationStats) {
        let (records, stats) = self.simulator.finish();
        assert_eq!(
            stats.triggers,
            stats.filtered + stats.skipped.total() + records.len() as u64,
            "conservation (ADR-060 D4)"
        );
        (records, stats)
    }

    /// The single record of the run.
    fn only(self) -> TradeRecord {
        let (mut records, _) = self.finish();
        assert_eq!(records.len(), 1, "{records:?}");
        records.remove(0)
    }
}

fn key(millis: i64, trade_id: u64) -> CanonicalKey {
    trade(millis, trade_id, "1").canonical_key()
}

fn fill(millis: i64, price: &str) -> Option<Fill> {
    Some(Fill {
        at: t(millis),
        price: usd(price),
    })
}

fn r(text: &str) -> Option<Rate> {
    Some(rate(text))
}

/// A record with the decision's levels, no entry and nothing filled yet;
/// each test sets what its trade reached.
fn record(
    decision: CanonicalKey,
    side: PositionSide,
    [reference, stop, target, risk]: [&str; 4],
) -> TradeRecord {
    TradeRecord {
        decision,
        side,
        regime: Vec::new(),
        reference: usd(reference),
        stop: usd(stop),
        target: usd(target),
        risk: usd(risk),
        entry: None,
        exit: Exit::Unfilled,
        exit_at: t(0),
        fill: None,
        funding: None,
        gross: None,
        net: None,
        displacement: None,
        mfe: None,
        mae: None,
    }
}

/// The hand-computed fixture (#167 acceptance a), with latency 10 ms, a
/// 1 000 ms hold, slippage s = 0.001, taker 0.0005 and recorded funding.
///
/// Every R value below is (price units × 10⁸) / R units, rounded half to
/// even once (ADR-060 D10). All four trades have R = 2 USDT.
///
/// **A, long, target exit.** Decision at trade #1 (1 000 ms, 100), levels
/// 98 / 104, so R = 100 − 98 = 2. The entry needs a time ≥ 1 010: #2 at
/// 1 005 is too early, #3 at 1 010 fills at e = 101.
/// - E′ = ⟨101 × 1.001⟩ = 101.101, entry fee = ⟨0.0005 × 101.101⟩ =
///   0.0505505; displacement = (101 − 100) / 2 = 0.5.
/// - #4 103 (+2), #6 99.5 (−1.5), #7 104.5 touches the target (≥ 104) at
///   1 300; #8 at 1 301 fills the exit at x = 104.2.
/// - X′ = ⟨104.2 × 0.999⟩ = 104.0958, exit fee = ⟨0.0005 × 104.0958⟩ =
///   0.0520479; no settlement, funding 0.
/// - gross = (104.2 − 101) / 2 = 1.6; net = (104.0958 − 101.101 −
///   0.0505505 − 0.0520479) / 2 = 2.8922016 / 2 = 1.4461008.
/// - MFE = 3.5 / 2 = 1.75 (#7); MAE = 1.5 / 2 = 0.75 (#6).
/// - At #5 (1 150) five more decisions: `filtered`, `no_side`, `no_risk`
///   and `stop_side` take their own counts although A is open, and one with
///   levels is an `overlap` (D4's order puts `overlap` last).
///
/// **C, short, stop exit.** Decision at A's exit fill #8 (1 301, 104.2):
/// the slot is free again (D4). Levels 106.2 / 100.2, R = −(104.2 −
/// 106.2) = 2. #9 at 1 311 fills at e = 104.
/// - E′ = ⟨104 × 0.999⟩ = 103.896, fee 0.051948; displacement = −(104 −
///   104.2) / 2 = 0.1 (adverse for a short).
/// - #10 103 (+1 for a short), #11 106.5 touches the stop (≥ 106.2) at
///   1 500, with −2.5; #12 at 1 600 fills at x = 106.
/// - X′ = ⟨106 × 1.001⟩ = 106.106, fee 0.053053.
/// - gross = −(106 − 104) / 2 = −1; net = (−(106.106 − 103.896) −
///   0.051948 − 0.053053) / 2 = −2.315001 / 2 = −1.1575005.
/// - MFE = 1 / 2 = 0.5; MAE = 2.5 / 2 = 1.25.
///
/// **E, stale.** A trades gap [1 600, 1 700] follows the last trade, so a
/// decision at the mark price at 1 750 is `stale_reference` although the
/// engine still holds 106 as its last price.
///
/// **D, long, time exit with funding.** Decision at #13 (2 000, 100),
/// levels 98 / 104, deadline 3 000. #14 at 2 010 fills at e = 100.5.
/// - E′ = ⟨100.5 × 1.001⟩ = 100.6005, fee = 0.05030025; displacement =
///   0.5 / 2 = 0.25.
/// - #15 101.5 (+1), #16 99 (−1.5), #17 100.5; the settlement at 2 800 at
///   rate 0.00002233 is priced at #17: ⟨0.00002233 × 100.5⟩ =
///   ⟨0.002244165⟩, **an exact tie**, rounded to the even 0.00224416.
/// - No touch; #18 at 3 000, the first trade at the deadline, fills at
///   x = 100. X′ = ⟨100 × 0.999⟩ = 99.9, fee 0.04995.
/// - gross = (100 − 100.5) / 2 = −0.25; net = (99.9 − 100.6005 −
///   0.05030025 − 0.04995 − 0.00224416) / 2 = −0.80299441 / 2 =
///   −0.401497205, **an exact tie**, rounded to the even −0.40149720.
/// - MFE = 1 / 2 = 0.5; MAE = 1.5 / 2 = 0.75; `exit_at` is the deadline.
///
/// **F, long, censored by a trades gap.** Decision at #19 (4 000, 100),
/// levels 98 / 104; #20 at 4 010 fills at e = 100 (displacement 0). #21 101
/// (+1), #22 99.5 at 4 200 (−0.5), then a trades gap [4 200, 4 500]: c =
/// max(4 200, 4 000) = 4 200. The line keeps what came strictly before c:
/// the entry, MFE = 1 / 2 = 0.5 and MAE = 0, a lower bound that leaves out
/// #22 at c itself.
///
/// Counts: 10 triggers = 1 filtered + 5 skipped + 4 trades.
fn fixture(costs: Costs) -> (Vec<TradeRecord>, SimulationStats) {
    let mut run = Run::new(costs);
    run.on(trade(1_000, 1, "100"))
        .decide_in(vec![Some(RegimeLabel::High)], long("100", "98", "104"))
        .on(trade(1_005, 2, "100.5"))
        .on(trade(1_010, 3, "101"))
        .on(trade(1_100, 4, "103"))
        .on(trade(1_150, 5, "102"))
        .decide(Admission::Filtered)
        .decide(Admission::NoSide)
        .decide(Admission::NoRisk)
        .decide(Admission::StopSide)
        .decide(long("102", "100", "106"))
        .on(trade(1_200, 6, "99.5"))
        .on(trade(1_300, 7, "104.5"))
        .on(trade(1_301, 8, "104.2"))
        .decide_in(vec![None], short("104.2", "106.2", "100.2"))
        .on(trade(1_311, 9, "104"))
        .on(trade(1_400, 10, "103"))
        .on(trade(1_500, 11, "106.5"))
        .on(trade(1_600, 12, "106"))
        .on(gap(Stream::Trades, 1_600, 1_700))
        .on(mark(1_750, 10_600_000_000))
        .decide(long("106", "104", "110"))
        .on(trade(2_000, 13, "100"))
        .decide(long("100", "98", "104"))
        .on(trade(2_010, 14, "100.5"))
        .on(trade(2_500, 15, "101.5"))
        .on(trade(2_600, 16, "99"))
        .on(trade(2_700, 17, "100.5"))
        .on(settlement(2_800, 2_233))
        .on(trade(3_000, 18, "100"))
        .on(trade(4_000, 19, "100"))
        .decide(long("100", "98", "104"))
        .on(trade(4_010, 20, "100"))
        .on(trade(4_100, 21, "101"))
        .on(trade(4_200, 22, "99.5"))
        .on(gap(Stream::Trades, 4_200, 4_500))
        .on(trade(4_600, 23, "100"));
    run.finish()
}

fn fixture_costs() -> Costs {
    costs(10, 1_000, "0.001", "0.0005", FundingRule::Recorded)
}

#[test]
fn the_hand_computed_fixture_matches_field_by_field() {
    let (records, stats) = fixture(fixture_costs());
    let a = TradeRecord {
        regime: vec![Some(RegimeLabel::High)],
        entry: fill(1_010, "101"),
        exit: Exit::Target,
        exit_at: t(1_300),
        fill: fill(1_301, "104.2"),
        funding: Some(usd("0")),
        gross: r("1.6"),
        net: r("1.4461008"),
        displacement: r("0.5"),
        mfe: r("1.75"),
        mae: r("0.75"),
        ..record(key(1_000, 1), PositionSide::Long, ["100", "98", "104", "2"])
    };
    let c = TradeRecord {
        regime: vec![None],
        entry: fill(1_311, "104"),
        exit: Exit::Stop,
        exit_at: t(1_500),
        fill: fill(1_600, "106"),
        funding: Some(usd("0")),
        gross: r("-1"),
        net: r("-1.1575005"),
        displacement: r("0.1"),
        mfe: r("0.5"),
        mae: r("1.25"),
        ..record(
            key(1_301, 8),
            PositionSide::Short,
            ["104.2", "106.2", "100.2", "2"],
        )
    };
    let d = TradeRecord {
        entry: fill(2_010, "100.5"),
        exit: Exit::Time,
        exit_at: t(3_000),
        fill: fill(3_000, "100"),
        funding: Some(usd("0.00224416")),
        gross: r("-0.25"),
        net: r("-0.4014972"),
        displacement: r("0.25"),
        mfe: r("0.5"),
        mae: r("0.75"),
        ..record(
            key(2_000, 13),
            PositionSide::Long,
            ["100", "98", "104", "2"],
        )
    };
    let f = TradeRecord {
        entry: fill(4_010, "100"),
        exit: Exit::Censored(CensorReason::Gap),
        exit_at: t(4_200),
        displacement: r("0"),
        mfe: r("0.5"),
        mae: r("0"),
        ..record(
            key(4_000, 19),
            PositionSide::Long,
            ["100", "98", "104", "2"],
        )
    };
    assert_eq!(records, vec![a, c, d, f]);
    assert_eq!(
        stats,
        SimulationStats {
            triggers: 10,
            filtered: 1,
            skipped: SkipCounts {
                overlap: 1,
                stale_reference: 1,
                no_side: 1,
                no_risk: 1,
                stop_side: 1,
            },
            reach_backs: 0,
        }
    );
}

#[test]
fn runs_are_deterministic() {
    assert_eq!(fixture(fixture_costs()), fixture(fixture_costs()));
}

/// The records with net R cleared, and the net R values of the trades
/// that stand.
fn split_net(records: &[TradeRecord]) -> (Vec<TradeRecord>, Vec<i64>) {
    let nets = records
        .iter()
        .filter_map(|record| record.net.map(Rate::units))
        .collect();
    let cleared = records
        .iter()
        .cloned()
        .map(|record| TradeRecord {
            net: None,
            ..record
        })
        .collect();
    (cleared, nets)
}

#[test]
fn a_higher_fee_or_slippage_lowers_net_r_and_nothing_else() {
    let (base, base_stats) = fixture(fixture_costs());
    let (base_rest, base_net) = split_net(&base);
    assert_eq!(base_net.len(), 3);
    for higher in [
        costs(10, 1_000, "0.001", "0.001", FundingRule::Recorded),
        costs(10, 1_000, "0.002", "0.0005", FundingRule::Recorded),
    ] {
        let (moved, stats) = fixture(higher);
        let (moved_rest, moved_net) = split_net(&moved);
        // Costs move fills, never levels, exits or counts (ADR-060 D5).
        assert_eq!(moved_rest, base_rest, "{higher:?}");
        assert_eq!(stats, base_stats, "{higher:?}");
        for (moved, base) in moved_net.iter().zip(&base_net) {
            assert!(moved < base, "{higher:?}: {moved} < {base}");
        }
    }
}

#[test]
fn funding_flips_sign_with_the_side_and_a_long_pays_a_positive_rate() {
    let run = |admission| {
        let mut run = Run::new(plain());
        run.on(trade(0, 1, "100"))
            .decide(admission)
            .on(trade(1, 2, "100"))
            .on(settlement(500, 10_000))
            .on(trade(1_000, 3, "100"));
        run.only()
    };
    let long = run(long("100", "90", "110"));
    let short = run(short("100", "110", "90"));
    assert_eq!((long.exit, short.exit), (Exit::Time, Exit::Time));
    // ⟨±1 × 0.0001 × 100⟩ = ±0.01.
    assert_eq!(long.funding, Some(usd("0.01")));
    assert_eq!(short.funding, Some(usd("-0.01")));
    // Paid funding lowers the long's net: 0.01 / 10 = 0.001 R.
    assert_eq!((long.net, short.net), (r("-0.001"), r("0.001")));
}

#[test]
fn a_longer_latency_displaces_the_entry_further_on_a_rising_tape() {
    let run = |latency_ms| {
        let mut run = Run::new(costs(latency_ms, 1_000, "0", "0", FundingRule::Recorded));
        run.on(trade(0, 1, "100"))
            .decide(long("100", "90", "120"))
            .on(trade(10, 2, "101"))
            .on(trade(20, 3, "102"))
            .on(trade(30, 4, "103"))
            .on(trade(1_000, 5, "103"));
        run.only()
    };
    let (early, late) = (run(5), run(15));
    assert_eq!(early.entry, fill(10, "101"));
    assert_eq!(late.entry, fill(20, "102"));
    // (101 − 100) / 10 and (102 − 100) / 10.
    assert_eq!(
        (early.displacement, late.displacement),
        (r("0.1"), r("0.2"))
    );
    assert!(late.displacement > early.displacement);
}

#[test]
fn the_entry_fill_trade_can_touch_a_level() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "98.5"))
        .on(trade(2, 3, "98"));
    let record = run.only();
    assert_eq!(record.entry, fill(1, "98.5"));
    assert_eq!(record.exit, Exit::Stop);
    assert_eq!(record.exit_at, t(1));
    assert_eq!(record.fill, fill(2, "98"));
    // (98 − 98.5) / 1.
    assert_eq!(record.gross, r("-0.5"));
    // Excursions count from the entry fill price e: (98.5 − 98) / 1.
    assert_eq!((record.mfe, record.mae), (r("0"), r("0.5")));
}

#[test]
fn an_unfilled_entry_holds_the_slot_until_its_deadline() {
    let mut run = Run::new(costs(10, 100, "0", "0", FundingRule::Recorded));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        // Before the latency: no fill.
        .on(trade(5, 2, "100"))
        .on(mark(50, 1))
        .decide(long("100", "99", "101"))
        // The deadline ends the entry before this event's decisions.
        .on(mark(100, 1))
        .decide(long("100", "99", "101"))
        .on(trade(150, 3, "100"))
        .on(trade(200, 4, "100"));
    let (records, stats) = run.finish();
    assert_eq!(stats.skipped.overlap, 1);
    let unfilled = TradeRecord {
        exit: Exit::Unfilled,
        exit_at: t(100),
        ..record(key(0, 1), PositionSide::Long, ["100", "99", "101", "1"])
    };
    assert_eq!(records[0], unfilled);
    assert_eq!(records[1].decision, mark(100, 1).canonical_key());
    assert_eq!(records[1].entry, fill(150, "100"));
    assert_eq!(records[1].exit, Exit::Time);
    assert_eq!(records[1].exit_at, t(200));
}

#[test]
fn a_decision_on_the_exit_fill_trade_opens_a_position() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(2, 3, "101"))
        .on(trade(2, 4, "100.5"))
        .decide(long("100.5", "99.5", "101.5"))
        .on(trade(3, 5, "100.5"))
        .on(trade(1_002, 6, "100.5"));
    let (records, stats) = run.finish();
    assert_eq!(stats.skipped, SkipCounts::default());
    assert_eq!(records.len(), 2);
    assert_eq!(records[0].fill, fill(2, "100.5"));
    // Its entry is strictly after its own decision event.
    assert_eq!(records[1].entry, fill(3, "100.5"));
    assert_eq!(records[1].exit, Exit::Time);
}

#[test]
fn several_emissions_on_one_event_go_through_the_checks_in_order() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(Admission::NoRisk)
        .decide(long("100", "99", "101"))
        .decide(long("100", "98", "102"))
        .on(trade(1_000, 2, "100"));
    let (records, stats) = run.finish();
    assert_eq!(stats.skipped.no_risk, 1);
    assert_eq!(stats.skipped.overlap, 1);
    // The first emission that passes every check takes the slot.
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].stop, usd("99"));
}

#[test]
fn a_final_trade_is_not_reopened_and_each_cause_reaching_back_counts_once() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(2, 3, "101"))
        .on(trade(3, 4, "101"))
        // A later millisecond than the touch: A is final.
        .on(trade(4, 5, "101"))
        .reject(trade(1, 2, "100"))
        .reject(trade(2, 3, "101"))
        // B fills its exit in the touch's millisecond, so it is not final.
        .on(trade(10, 6, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(11, 7, "100"))
        .on(trade(12, 8, "101"))
        .on(trade(12, 9, "101"))
        .on(gap(Stream::Trades, 12, 20));
    let (records, stats) = run.finish();
    assert_eq!(stats.reach_backs, 2);
    assert_eq!(records[0].exit, Exit::Target);
    assert_eq!(records[0].fill, fill(3, "101"));
    // c = max(12, 10) = 12 is not after B's touch at 12 (ADR-055 D6).
    assert_eq!(records[1].exit, Exit::Censored(CensorReason::Gap));
    assert_eq!(records[1].exit_at, t(12));
    assert_eq!((records[1].fill, records[1].net), (None, None));
}

#[test]
fn a_trades_gap_censors_a_pending_entry() {
    let mut run = Run::new(costs(10, 1_000, "0", "0", FundingRule::Recorded));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(gap(Stream::Trades, 5, 8))
        .on(trade(9, 2, "100"));
    let record = run.only();
    assert_eq!(record.exit, Exit::Censored(CensorReason::Gap));
    assert_eq!(record.exit_at, t(5));
    assert_eq!((record.entry, record.mfe), (None, None));
}

#[test]
fn a_rejected_trades_event_censors_at_the_min_rule() {
    // c = max(decision, min(event time, the last accepted trades event)).
    for (rejected_at, censor_at) in [(80, 50), (20, 20)] {
        let mut run = Run::new(plain());
        run.on(trade(0, 1, "100"))
            .decide(long("100", "99", "101"))
            .on(trade(1, 2, "100"))
            .on(trade(50, 3, "100"))
            .reject(trade(rejected_at, 4, "100"))
            .on(trade(60, 5, "100"));
        let record = run.only();
        assert_eq!(
            (record.exit, record.exit_at),
            (Exit::Censored(CensorReason::Rejected), t(censor_at)),
            "{rejected_at}"
        );
    }
}

#[test]
fn the_end_of_data_censors_at_the_last_trades_event() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(3, 3, "100.5"))
        .on(trade(7, 4, "99.5"))
        .on(mark(9, 1));
    let record = run.only();
    assert_eq!(record.exit, Exit::Censored(CensorReason::EndOfData));
    assert_eq!(record.exit_at, t(7));
    assert_eq!(record.entry, fill(1, "100"));
    // The trade at c itself is not strictly before it.
    assert_eq!((record.mfe, record.mae), (r("0.5"), r("0")));
}

#[test]
fn a_chain_restart_censors_and_makes_the_reference_stale() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(5, 3, "100"))
        .restart()
        .on(mark(100_000, 1));
    // The fresh engine has no last price; even a reference handed in is
    // stale until a trade is observed.
    run.last_price = Some(usd("100"));
    run.decide(long("100", "99", "101"))
        .on(trade(100_001, 4, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(100_002, 5, "100"))
        .on(trade(101_001, 6, "100"));
    let (records, stats) = run.finish();
    assert_eq!(stats.skipped.stale_reference, 1);
    assert_eq!(records[0].exit, Exit::Censored(CensorReason::ChainRestart));
    assert_eq!(records[0].exit_at, t(5));
    assert_eq!(records[1].exit, Exit::Time);
}

#[test]
fn a_funding_gap_inside_the_span_censors_from_its_start() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(3, 3, "100.5"))
        .on(gap(Stream::Funding, 5, 6))
        // The slot is free again.
        .on(trade(10, 4, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(11, 5, "100"))
        .on(trade(1_010, 6, "100"));
    let (records, _) = run.finish();
    assert_eq!(records[0].exit, Exit::Censored(CensorReason::FundingGap));
    // c = max(5, the entry fill at 1).
    assert_eq!(records[0].exit_at, t(5));
    assert_eq!(records[0].mfe, r("0.5"));
    assert_eq!(records[1].exit, Exit::Time);
}

#[test]
fn a_funding_gap_censors_a_final_trade_and_is_no_reach_back() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(2, 3, "101"))
        .on(trade(3, 4, "101"))
        .on(trade(4, 5, "101"))
        .on(gap(Stream::Funding, 2, 2))
        // A gap that ends before the entry, or starts at the exit fill,
        // meets no booking span.
        .on(gap(Stream::Funding, 0, 0));
    let (records, stats) = run.finish();
    assert_eq!(stats.reach_backs, 0);
    assert_eq!(records[0].exit, Exit::Censored(CensorReason::FundingGap));
    assert_eq!(records[0].exit_at, t(2));
    assert_eq!(records[0].entry, fill(1, "100"));
    assert_eq!((records[0].fill, records[0].funding), (None, None));

    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(2, 3, "101"))
        .on(trade(3, 4, "101"))
        .on(gap(Stream::Funding, 3, 9));
    assert_eq!(run.only().exit, Exit::Target);
}

#[test]
fn a_trailing_funding_gap_only_censors() {
    // A pending entry books nothing, and the gap's end past the deadline
    // closes nothing: the entry is censored at the end of the data, not
    // `unfilled`.
    let mut run = Run::new(costs(10, 100, "0", "0", FundingRule::Recorded));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(5, 2, "100"))
        .on(gap(Stream::Funding, 6, 5_000));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::EndOfData), t(5))
    );

    // An open position is censored at max(start, entry fill), which the
    // later end-of-data censor does not replace.
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(50, 3, "100"))
        .on(gap(Stream::Funding, 20, 5_000));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::FundingGap), t(20))
    );
}

#[test]
fn funding_none_ignores_funding_gaps_settlements_and_rejections() {
    let mut run = Run::new(costs(0, 1_000, "0", "0", FundingRule::None));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(gap(Stream::Funding, 5, 6))
        .on(settlement(8, 10_000))
        .reject(settlement(9, 10_000))
        .on(trade(1_000, 3, "100"));
    let record = run.only();
    assert_eq!(record.exit, Exit::Time);
    assert_eq!(record.funding, Some(usd("0")));
}

#[test]
fn a_rejected_funding_event_in_the_span_censors_as_a_funding_gap() {
    // STOP 1 decision 1 of #167: a refused settlement would book zero
    // funding by omission (ADR-060 D7).
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        // Before the entry fill: the span does not hold it.
        .reject(settlement(0, 10_000))
        .on(trade(1, 2, "100"))
        .on(trade(10, 3, "100"))
        .reject(settlement(8, 10_000))
        .on(trade(1_000, 4, "100"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::FundingGap), t(8))
    );
}

#[test]
fn a_settlement_books_in_the_entry_fill_millisecond_but_not_in_the_exit_fill_one() {
    // A settlement ranks after the trades of its millisecond (ADR-028).
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "90", "110"))
        .on(trade(1, 2, "100"))
        .on(settlement(1, 10_000))
        .on(trade(1_000, 3, "102"))
        .on(settlement(1_000, 10_000));
    let record = run.only();
    assert_eq!(record.exit, Exit::Time);
    // One term, ⟨0.0001 × 100⟩, priced at the entry fill trade.
    assert_eq!(record.funding, Some(usd("0.01")));
}

#[test]
fn an_overflow_censors_at_the_entry_fill() {
    // R is one unit, so the displacement (1 000 − 2 000) × 10⁸ / 10⁻⁸
    // leaves the i64 range.
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "2000"))
        .decide(long("2000", "1999.99999999", "2000.00000001"))
        .on(trade(1, 2, "1000"))
        .on(trade(2, 3, "1000"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Overflow), t(1))
    );
    assert_eq!((record.entry, record.displacement), (None, None));
}

#[test]
fn an_overflow_censors_at_a_monitored_trade() {
    // MFE = 950 USDT / 10⁻⁸ USDT = 9.5 · 10¹⁸ R > i64::MAX units.
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "2000"))
        .decide(long("2000", "1999.99999999", "3000"))
        .on(trade(1, 2, "2000"))
        .on(trade(5, 3, "2950"))
        .on(trade(6, 4, "2000"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Overflow), t(5))
    );
    assert_eq!(record.entry, fill(1, "2000"));
    assert_eq!((record.mfe, record.mae), (r("0"), r("0")));
}

#[test]
fn an_overflow_censors_at_a_settlement() {
    // ⟨10 000 × 10⁷⟩ = 10¹¹ USDT is outside the i64 range of units.
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "10000000"))
        .decide(long("10000000", "9000000", "11000000"))
        .on(trade(1, 2, "10000000"))
        .on(settlement(5, 1_000_000_000_000))
        .on(trade(6, 3, "10000000"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Overflow), t(5))
    );
}

#[test]
fn an_overflow_censors_at_the_exit_fill() {
    // Taker 0.5 on both legs: net = −2 000 USDT over R = 10⁻⁸ USDT.
    let mut run = Run::new(costs(0, 1_000, "0", "0.5", FundingRule::Recorded));
    run.on(trade(0, 1, "2000"))
        .decide(long("2000", "1999.99999999", "2000.00000001"))
        .on(trade(1, 2, "2000"))
        .on(trade(2, 3, "2000.00000001"))
        .on(trade(3, 4, "2000"))
        .on(trade(4, 5, "2000"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Overflow), t(3))
    );
    assert_eq!(record.entry, fill(1, "2000"));
    assert_eq!((record.fill, record.net), (None, None));
}

#[test]
fn at_an_equal_censor_time_the_first_censor_stays() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(10, 3, "100"))
        // c = min(30, 10) = 10, and the end of the data is at 10 as well.
        .reject(trade(30, 4, "100"));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Rejected), t(10))
    );
}

#[test]
fn a_later_cause_with_an_earlier_censor_time_replaces_the_censor() {
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(10, 3, "100.5"))
        // c = 50, after every trades event: not final yet.
        .on(gap(Stream::Funding, 50, 60))
        .on(gap(Stream::Trades, 20, 70));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Gap), t(20))
    );
    assert_eq!(record.mfe, r("0.5"));
}

#[test]
fn contract_violating_gaps_censor_without_panicking() {
    // The touch at 2 has no observed fill, yet the gap claims the trades
    // complete until 5: the exit would stand without a fill.
    let mut run = Run::new(plain());
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(trade(2, 3, "101"))
        .on(gap(Stream::Trades, 5, 9));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Gap), t(5))
    );

    // The same for a time exit whose deadline (100) passed before the gap.
    let mut run = Run::new(costs(0, 100, "0", "0", FundingRule::Recorded));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(trade(1, 2, "100"))
        .on(gap(Stream::Trades, 150, 200));
    let record = run.only();
    assert_eq!(
        (record.exit, record.exit_at),
        (Exit::Censored(CensorReason::Gap), t(150))
    );

    // A pending entry whose deadline lies before the gap's start stands
    // as `unfilled`: the trades are complete past the deadline.
    let mut run = Run::new(costs(10, 100, "0", "0", FundingRule::Recorded));
    run.on(trade(0, 1, "100"))
        .decide(long("100", "99", "101"))
        .on(gap(Stream::Trades, 150, 200));
    let record = run.only();
    assert_eq!((record.exit, record.exit_at), (Exit::Unfilled, t(100)));
}

#[test]
fn costs_refuse_values_outside_their_domain() {
    let new = |latency, hold, slippage, taker| {
        Costs::new(
            latency,
            hold,
            rate(slippage),
            rate(taker),
            FundingRule::None,
        )
    };
    assert_eq!(new(0, 1_000, "-0.00000001", "0"), Err(CostsError::Slippage));
    assert_eq!(new(0, 1_000, "1", "0"), Err(CostsError::Slippage));
    assert_eq!(new(0, 1_000, "0", "-0.00000001"), Err(CostsError::Taker));
    assert_eq!(new(0, 1_000, "0", "1"), Err(CostsError::Taker));
    assert_eq!(new(0, 0, "0", "0"), Err(CostsError::MaxHold));
    assert_eq!(new(1_000, 1_000, "0", "0"), Err(CostsError::Latency));
    let edge = new(0, 1, "0.99999999", "0.99999999").unwrap();
    assert_eq!(
        (edge.latency_ms(), edge.max_hold_ms(), edge.funding()),
        (0, 1, FundingRule::None)
    );
    assert_eq!(
        (edge.slippage(), edge.taker()),
        (rate("0.99999999"), rate("0.99999999"))
    );
    assert!(new(999, 1_000, "0", "0").is_ok());
}

#[test]
fn levels_refuse_a_stop_or_target_on_the_wrong_side_and_an_unrepresentable_risk() {
    use PositionSide::{Long, Short};
    let new = |side, reference, stop, target| {
        Levels::new(side, usd(reference), usd(stop), usd(target)).map(|levels| levels.risk())
    };
    assert_eq!(new(Long, "100", "100", "101"), Err(LevelsError::StopSide));
    assert_eq!(new(Long, "100", "101", "102"), Err(LevelsError::StopSide));
    assert_eq!(new(Short, "100", "99", "98"), Err(LevelsError::StopSide));
    assert_eq!(new(Long, "100", "99", "100"), Err(LevelsError::TargetSide));
    assert_eq!(
        new(Short, "100", "101", "100.5"),
        Err(LevelsError::TargetSide)
    );
    assert_eq!(new(Short, "100", "101.5", "99"), Ok(usd("1.5")));
    let units = |side, reference, stop, target| {
        Levels::new(
            side,
            Price::from_units(reference),
            Price::from_units(stop),
            Price::from_units(target),
        )
        .map(|levels| levels.risk().units())
    };
    assert_eq!(
        units(Long, i64::MAX - 1, i64::MIN, i64::MAX),
        Err(LevelsError::Risk)
    );
    assert_eq!(units(Long, 0, i64::MIN + 1, 1), Ok(i64::MAX));
}

#[test]
fn the_names_are_the_trade_line_tokens() {
    assert_eq!(
        [PositionSide::Long, PositionSide::Short].map(|side| (side.as_str(), side.sign())),
        [("long", 1), ("short", -1)]
    );
    assert_eq!(
        [
            CensorReason::Gap,
            CensorReason::Rejected,
            CensorReason::EndOfData,
            CensorReason::ChainRestart,
            CensorReason::FundingGap,
            CensorReason::Overflow,
        ]
        .map(CensorReason::as_str),
        [
            "gap",
            "rejected",
            "end_of_data",
            "chain_restart",
            "funding_gap",
            "overflow"
        ]
    );
}
