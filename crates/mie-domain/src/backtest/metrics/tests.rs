use super::*;
use crate::backtest::simulator::tests::{costs, fixture};
use crate::backtest::{CensorReason, Fill, FundingRule, PositionSide};
use crate::num::Price;
use crate::order::{CanonicalKey, EventKind};
use crate::time::EventTime;

fn rate(text: &str) -> Rate {
    text.parse().unwrap()
}

fn r(text: &str) -> Option<Rate> {
    Some(rate(text))
}

/// A record decided at `decision` ms with `exit` at `exit_at` ms and the
/// R values `[net, gross, mfe, mae, displacement]`, `None` for `-`.
/// Prices and fills play no part in the metrics (ADR-060 D11).
fn record(
    regime: Option<RegimeLabel>,
    decision: i64,
    exit: Exit,
    exit_at: i64,
    [net, gross, mfe, mae, displacement]: [Option<Rate>; 5],
) -> TradeRecord {
    let fill = Fill {
        at: EventTime::from_millis(decision + 1),
        price: Price::from_units(100),
    };
    let closed = matches!(exit, Exit::Target | Exit::Stop | Exit::Time);
    TradeRecord {
        decision: CanonicalKey {
            time: EventTime::from_millis(decision),
            kind: EventKind::Trade,
            seq: 0,
        },
        side: PositionSide::Long,
        regime: vec![regime],
        reference: Price::from_units(100),
        stop: Price::from_units(90),
        target: Price::from_units(120),
        risk: Price::from_units(10),
        entry: displacement.map(|_| fill),
        exit,
        exit_at: EventTime::from_millis(exit_at),
        fill: closed.then_some(fill),
        funding: closed.then_some(Price::from_units(0)),
        gross,
        net,
        displacement,
        mfe,
        mae,
        slipped: None,
    }
}

/// A closed trade with the R values given as decimals.
fn trade(
    regime: RegimeLabel,
    decision: i64,
    exit: Exit,
    exit_at: i64,
    values: [&str; 5],
) -> TradeRecord {
    record(Some(regime), decision, exit, exit_at, values.map(r))
}

/// A metrics value with every field given; `-` as `None`.
fn metrics(
    [trades, censored, unfilled]: [u64; 3],
    [
        expectancy,
        mean_gross,
        profit_factor,
        win_rate,
        max_drawdown,
    ]: [&str; 5],
    time_to_target_ms: Option<i64>,
    [mean_mfe, mean_mae, mean_displacement]: [&str; 3],
) -> Metrics {
    let value = |text: &str| (text != "-").then(|| rate(text));
    Metrics {
        trades,
        censored,
        unfilled,
        expectancy: value(expectancy),
        mean_gross: value(mean_gross),
        profit_factor: value(profit_factor),
        win_rate: value(win_rate),
        max_drawdown: value(max_drawdown),
        time_to_target_ms,
        mean_mfe: value(mean_mfe),
        mean_mae: value(mean_mae),
        mean_displacement: value(mean_displacement),
    }
}

/// The hand-computed metrics (#168 acceptance 1), one regime feature.
///
/// Values per record as `net, gross, mfe, mae, displacement`, in R; times
/// in ms from the decision.
///
/// | # | bucket  | exit           | values                       | to exit |
/// |---|---------|----------------|------------------------------|---------|
/// | 1 | HIGH    | stop           | −0.5, −0.4, 0.1, 1, 0.05     |         |
/// | 2 | HIGH    | target         | 1, 1.2, 1.5, 0.2, 0.1        | 600     |
/// | 3 | HIGH    | stop           | −0.5, −0.4, 0.3, 1, 0        |         |
/// | 4 | HIGH    | target         | 2, 2.2, 2.5, 0.4, 0.15       | 901     |
/// | 5 | HIGH    | time           | −1.5, −1.3, 0.2, 1.6, 0.2    |         |
/// | 6 | HIGH    | time           | −0.5, −0.3, 0.4, 0.9, −0.1   |         |
/// | 7 | HIGH    | censored (gap) | -, -, 5, 5, 5                |         |
/// | 8 | LOW     | target         | 0.75, 0.9, 1, 0.1, 0.05      | 400     |
/// | 9 | LOW     | time           | 0.25, 0.35, 0.5, 0.3, 0.05   |         |
/// | 10| EXTREME | time           | −0.25, −0.2, 0.1, 0.5, 0.02  |         |
/// | 11| EXTREME | time           | 1, 1.1, 1.2, 0.1, 0.04       |         |
/// | 12| -       | unfilled       | all `-`                      |         |
///
/// The censored line 7 carries MFE, MAE and displacement of 5 R, so a mean
/// that included it would move; the unfilled line 12 has nothing to add.
///
/// **HIGH**, six trades, one censored:
/// - expectancy = (−0.5 + 1 − 0.5 + 2 − 1.5 − 0.5) / 6 = 0;
/// - mean gross = 1 / 6 = 0.1666666666… → 0.16666667;
/// - profit factor = (1 + 2) / |−0.5 − 0.5 − 1.5 − 0.5| = 3 / 3 = 1;
/// - win rate = 2 / 6 = 0.3333333333… → 0.33333333;
/// - drawdown: cumulative net −0.5 (peak 0, fall 0.5), 0.5 (peak 0.5), 0,
///   2 (peak 2), 0.5, 0 (fall 2). So 2.0 after a new peak: measured from
///   the first peak 0.5 it would be 0.5, and from zero (−0.5) it would be
///   0.5 too;
/// - time to target = (600 + 901) / 2 = 750.5, a tie, → the even 750;
/// - mean MFE = 5 / 6 → 0.83333333; MAE = 5.1 / 6 = 0.85; displacement
///   = 0.4 / 6 = 0.0666666666… → 0.06666667.
///
/// **LOW**, winners only: expectancy 1 / 2 = 0.5, gross 1.25 / 2 = 0.625,
/// profit factor `-` (no losing trade), win rate 1, drawdown 0, time to
/// target 400, MFE 0.75, MAE 0.2, displacement 0.05.
///
/// **MEDIUM**, empty: counts 0, every mean, profit factor, win rate and
/// time `-`, drawdown 0 (the running peak starts at 0).
///
/// **EXTREME**, time exits only, the first a loss: expectancy 0.75 / 2 =
/// 0.375, gross 0.9 / 2 = 0.45, profit factor 1 / 0.25 = 4, win rate 0.5,
/// drawdown 0.25 (the peak starts at 0, so the first loss counts), time to
/// target `-` (no target exit), MFE 0.65, MAE 0.3, displacement 0.03.
///
/// **`-`**: the unfilled line only: counts 0, 0, 1, the rest as MEDIUM.
///
/// **all**, ten trades:
/// - expectancy = (0 + 1 + 0.75) / 10 = 0.175; gross = 3.15 / 10 = 0.315;
/// - profit factor = (3 + 1 + 1) / (3 + 0.25) = 1.5384615384… →
///   1.53846154; win rate = 5 / 10 = 0.5;
/// - drawdown: as HIGH up to line 6 (2.0), then 0.75, 1, 0.75, 1.75: 2.0;
/// - time to target = (600 + 901 + 400) / 3 = 633.66… → 634;
/// - MFE = 7.8 / 10 = 0.78, MAE = 6.1 / 10 = 0.61, displacement =
///   0.56 / 10 = 0.056.
#[test]
fn the_hand_computed_metrics_match() {
    use Exit::{Stop, Target, Time};
    use RegimeLabel::{Extreme, High, Low};
    let records = vec![
        trade(
            High,
            1_000,
            Stop,
            1_500,
            ["-0.5", "-0.4", "0.1", "1", "0.05"],
        ),
        trade(
            High,
            2_000,
            Target,
            2_600,
            ["1", "1.2", "1.5", "0.2", "0.1"],
        ),
        trade(High, 3_000, Stop, 3_100, ["-0.5", "-0.4", "0.3", "1", "0"]),
        trade(
            High,
            4_000,
            Target,
            4_901,
            ["2", "2.2", "2.5", "0.4", "0.15"],
        ),
        trade(
            High,
            5_000,
            Time,
            6_000,
            ["-1.5", "-1.3", "0.2", "1.6", "0.2"],
        ),
        trade(
            High,
            7_000,
            Time,
            8_000,
            ["-0.5", "-0.3", "0.4", "0.9", "-0.1"],
        ),
        record(
            Some(High),
            9_000,
            Exit::Censored(CensorReason::Gap),
            9_100,
            [None, None, r("5"), r("5"), r("5")],
        ),
        trade(
            Low,
            10_000,
            Target,
            10_400,
            ["0.75", "0.9", "1", "0.1", "0.05"],
        ),
        trade(
            Low,
            11_000,
            Time,
            12_000,
            ["0.25", "0.35", "0.5", "0.3", "0.05"],
        ),
        trade(
            Extreme,
            13_000,
            Time,
            14_000,
            ["-0.25", "-0.2", "0.1", "0.5", "0.02"],
        ),
        trade(
            Extreme,
            15_000,
            Time,
            16_000,
            ["1", "1.1", "1.2", "0.1", "0.04"],
        ),
        record(None, 17_000, Exit::Unfilled, 18_000, [None; 5]),
    ];
    let empty = |unfilled| {
        metrics(
            [0, 0, unfilled],
            ["-", "-", "-", "-", "0"],
            None,
            ["-", "-", "-"],
        )
    };
    let expected = BacktestMetrics {
        all: metrics(
            [10, 1, 1],
            ["0.175", "0.315", "1.53846154", "0.5", "2"],
            Some(634),
            ["0.78", "0.61", "0.056"],
        ),
        regimes: vec![[
            metrics(
                [2, 0, 0],
                ["0.5", "0.625", "-", "1", "0"],
                Some(400),
                ["0.75", "0.2", "0.05"],
            ),
            empty(0),
            metrics(
                [6, 1, 0],
                ["0", "0.16666667", "1", "0.33333333", "2"],
                Some(750),
                ["0.83333333", "0.85", "0.06666667"],
            ),
            metrics(
                [2, 0, 0],
                ["0.375", "0.45", "4", "0.5", "0.25"],
                None,
                ["0.65", "0.3", "0.03"],
            ),
            empty(1),
        ]],
    };
    assert_eq!(BacktestMetrics::compute(&records, 1), expected);
}

#[test]
fn regime_splits_are_independent() {
    use RegimeLabel::{Extreme, High, Low, Medium};
    let labels = [
        [Some(Low), None],
        [Some(High), Some(Medium)],
        [None, Some(Medium)],
        [Some(High), Some(Extreme)],
        [Some(Medium), None],
    ];
    let records: Vec<TradeRecord> = labels
        .iter()
        .zip(0..)
        .map(|(regime, i)| TradeRecord {
            regime: regime.to_vec(),
            ..trade(
                Low,
                1_000 * i,
                Exit::Time,
                1_000 * i + 500,
                ["0.5", "0.5", "0.5", "0.5", "0.5"],
            )
        })
        .collect();
    let metrics = BacktestMetrics::compute(&records, 2);
    assert_eq!(metrics.all.trades, 5);
    assert_eq!(metrics.regimes.len(), 2);
    for buckets in &metrics.regimes {
        let trades: u64 = buckets.iter().map(|bucket| bucket.trades).sum();
        assert_eq!(trades, metrics.all.trades);
    }
    let counts = |feature: usize| metrics.regimes[feature].map(|bucket| bucket.trades);
    assert_eq!(counts(0), [1, 1, 2, 0, 1]);
    assert_eq!(counts(1), [0, 2, 0, 1, 2]);
    // A spec without a regime feature has only `all`.
    assert!(BacktestMetrics::compute(&records, 0).regimes.is_empty());
}

/// Every mean and the win rate are one half-to-even rounding of an exact
/// sum over a count (ADR-060 D11): 1 unit / 2 = 0.5 → 0, 3 units / 2 =
/// 1.5 → 2, −1 unit / 2 = −0.5 → 0; 2 wins of 3 → 0.66666667.
#[test]
fn means_round_half_to_even_once() {
    let unit = |units: i64| Some(Rate::from_units(units));
    let two = [
        record(
            Some(RegimeLabel::Low),
            0,
            Exit::Time,
            1,
            [unit(1), unit(1), unit(1), unit(0), unit(-1)],
        ),
        record(
            Some(RegimeLabel::Low),
            1,
            Exit::Time,
            2,
            [unit(0), unit(2), unit(0), unit(0), unit(0)],
        ),
    ];
    let metrics = Metrics::over(&two);
    assert_eq!(metrics.expectancy, unit(0));
    assert_eq!(metrics.mean_gross, unit(2));
    assert_eq!(metrics.mean_mfe, unit(0));
    assert_eq!(metrics.mean_displacement, unit(0));
    // A net of exactly 0 is no win.
    assert_eq!(metrics.win_rate, r("0.5"));

    let three = [
        trade(
            RegimeLabel::Low,
            0,
            Exit::Target,
            1,
            ["1", "1", "1", "0", "0"],
        ),
        trade(
            RegimeLabel::Low,
            1,
            Exit::Target,
            3,
            ["1", "1", "1", "0", "0"],
        ),
        trade(
            RegimeLabel::Low,
            2,
            Exit::Stop,
            4,
            ["-1", "-1", "0", "1", "0"],
        ),
    ];
    let metrics = Metrics::over(&three);
    assert_eq!(metrics.win_rate, r("0.66666667"));
    // (1 + 2) / 2 = 1.5 ms over the two target exits: the even 2.
    assert_eq!(metrics.time_to_target_ms, Some(2));
}

/// Profit factor, an unbounded ratio, and drawdown, a sum over trades, are
/// the two metrics that can leave the `i64` unit range (ADR-060 D12).
#[test]
fn metrics_outside_the_unit_range_are_undefined() {
    let unit = |units: i64| Some(Rate::from_units(units));
    let closed = |decision: i64, net: i64| {
        record(
            Some(RegimeLabel::Low),
            decision,
            Exit::Time,
            decision + 1,
            [unit(net), unit(0), unit(0), unit(0), unit(0)],
        )
    };
    // P · 10⁸ / 1 unit leaves the range.
    let metrics = Metrics::over(&[closed(0, i64::MAX), closed(1, -1)]);
    assert_eq!(metrics.profit_factor, None);
    assert_eq!(metrics.max_drawdown, unit(1));
    // From the peak i64::MAX down to −1: a fall of 2⁶³ units. Profit
    // factor (2⁶³ − 1) / 2⁶³ and expectancy −1 / 2 round to 1 and 0.
    let metrics = Metrics::over(&[closed(0, i64::MAX), closed(1, i64::MIN)]);
    assert_eq!(metrics.max_drawdown, None);
    assert_eq!(metrics.profit_factor, r("1"));
    assert_eq!(metrics.expectancy, unit(0));
    // Extremes in every field stay total.
    let extreme = record(
        Some(RegimeLabel::Low),
        i64::MIN,
        Exit::Target,
        i64::MAX,
        [unit(i64::MIN); 5],
    );
    let metrics = Metrics::over(&[extreme.clone(), extreme]);
    assert_eq!(metrics.time_to_target_ms, None);
    assert_eq!(metrics.expectancy, unit(i64::MIN));
    assert_eq!(metrics.max_drawdown, None);
}

/// A record whose exit is target, stop or time but which lacks a value is
/// no trade: the shape the parser refuses, which keeps `over` total.
#[test]
fn a_record_without_the_shape_of_a_trade_joins_no_mean() {
    let mut partial = trade(
        RegimeLabel::Low,
        0,
        Exit::Target,
        5,
        ["1", "1", "1", "0", "0"],
    );
    partial.mae = None;
    let metrics = Metrics::over(&[partial]);
    assert_eq!(metrics.trades, 0);
    assert_eq!(metrics.expectancy, None);
    assert_eq!(metrics.time_to_target_ms, None);
}

/// A higher fee never raises expectancy, profit factor or win rate (#168
/// acceptance 2), on S2's hand-computed fixture with slippage 0.001,
/// latency 10 and a 1 000 ms hold.
///
/// The taker steps are 1-2-5: 0.0005 is the fixture's base (the
/// `docs/direction.md` VIP 0 taker), 0.001 S2's own higher-fee case. Trade
/// A's net is (2.9948 − 205.1968 · taker) / 2 R, which crosses 0 at taker
/// ≈ 0.0146, so 0.02 is the first step that moves the win rate (1/3 → 0)
/// and the profit factor (P → 0) strictly.
///
/// The general claim is weak: fees enter net R with a negative sign and
/// change no exit (ADR-060 D5, D6), and half-to-even rounding is monotone
/// but can tie. So the strict move is a property of this fixture, as in
/// S2's latency test.
#[test]
fn a_higher_fee_lowers_expectancy_profit_factor_and_win_rate() {
    let at = |taker: &str| {
        let (records, _) = fixture(costs(10, 1_000, "0.001", taker, FundingRule::Recorded));
        Metrics::over(&records)
    };
    let steps: Vec<Metrics> = ["0.0005", "0.001", "0.005", "0.02"]
        .into_iter()
        .map(at)
        .collect();
    for pair in steps.windows(2) {
        let [lower, higher] = pair else {
            unreachable!("windows of two");
        };
        assert!(higher.expectancy <= lower.expectancy, "{pair:?}");
        assert!(higher.profit_factor <= lower.profit_factor, "{pair:?}");
        assert!(higher.win_rate <= lower.win_rate, "{pair:?}");
        assert_eq!(
            [higher.trades, higher.censored, higher.unfilled],
            [lower.trades, lower.censored, lower.unfilled]
        );
    }
    let (base, top) = (steps[0], steps[3]);
    assert_eq!([base.trades, base.censored, base.unfilled], [3, 1, 0]);
    assert!(top.expectancy < base.expectancy);
    assert!(top.profit_factor < base.profit_factor);
    assert!(top.win_rate < base.win_rate);
    assert_eq!(base.win_rate, r("0.33333333"));
    assert_eq!(top.win_rate, r("0"));
    assert_eq!(top.profit_factor, r("0"));
}
