use super::*;
use crate::backtest::FundingRule;
use crate::backtest::simulator::tests::{costs, fixture};
use crate::event_hash::EventStreamHash;
use crate::feature::{FeatureSet, catalog};
use crate::fingerprint::Fingerprint;
use crate::research::spec::tests::golden;
use crate::research::{
    EngineKey, ExperimentResult, ExperimentSpec, HistoryReplaySummary, Outcome, PipelineKey,
    ResultParseError,
};
use crate::state_hash::StateHash;

const PIPELINE: PipelineKey = PipelineKey::new("research.backtest", 1);

fn usd(text: &str) -> Price {
    text.parse().unwrap()
}

fn r(text: &str) -> Option<Rate> {
    Some(text.parse().unwrap())
}

fn t(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

fn at(millis: i64, price: &str) -> Option<Fill> {
    Some(Fill {
        at: t(millis),
        price: usd(price),
    })
}

fn key(millis: i64, kind: EventKind, seq: u64) -> CanonicalKey {
    CanonicalKey {
        time: t(millis),
        kind,
        seq,
    }
}

/// A record with its decision and levels, unfilled at `exit_at`; each
/// golden line sets what its trade reached.
fn base(
    decision: CanonicalKey,
    side: PositionSide,
    regime: Vec<Option<RegimeLabel>>,
    [reference, stop, target, risk]: [&str; 4],
    exit_at: i64,
) -> TradeRecord {
    TradeRecord {
        decision,
        side,
        regime,
        reference: usd(reference),
        stop: usd(stop),
        target: usd(target),
        risk: usd(risk),
        entry: None,
        exit: Exit::Unfilled,
        exit_at: t(exit_at),
        fill: None,
        funding: None,
        gross: None,
        net: None,
        displacement: None,
        mfe: None,
        mae: None,
    }
}

const LONG: [&str; 4] = ["100", "98", "104", "2"];

/// The chain provenance of both goldens: the sample starts at the origin,
/// so the prefix is empty.
fn summary() -> HistoryReplaySummary {
    HistoryReplaySummary {
        origin: t(1_759_276_800_000),
        effective_origin: None,
        cutover: t(1_791_244_800_000),
        prefix_data: None,
        prefix_stream: EventStreamHash {
            events: 0,
            fingerprint: Fingerprint::from_raw(0xcbf2_9ce4_8422_2325),
        },
        prefix_rejections: 0,
        engine: EngineKey {
            features: crate::feature::FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(
                0x0fed_cba9_8765_4321,
            )),
            state_encoding: 1,
        },
        state_start: StateHash::from_fingerprint(Fingerprint::from_raw(0x5555_6666_7777_8888)),
        stream: EventStreamHash {
            events: 4_800,
            fingerprint: Fingerprint::from_raw(0x0123_4567_89ab_cdef),
        },
        rejections: 0,
        state_end: StateHash::from_fingerprint(Fingerprint::from_raw(0x9999_aaaa_bbbb_cccc)),
    }
}

/// The trade records of golden G1, on the golden spec (one regime feature,
/// `volatility.regime.1h@1`).
///
/// The four closed trades follow ADR-060 D6/D10 from e and x with the
/// spec's slippage 0.0001 and taker 0.0005; MFE and MAE stand for a path
/// the text does not hold. With σ, E′ = ⟨e(1 + σs)⟩, X′ = ⟨x(1 − σs)⟩ and
/// each fee ⟨taker · fill⟩:
/// - T1, long, R 2, e 100.5, x 104.2: E′ 100.51005, fee 0.05025502, X′
///   104.18958, fee 0.05209479; gross 3.7 / 2 = 1.85, net (3.67953 −
///   0.10234981) / 2 = 1.78859010; displacement 0.5 / 2 = 0.25.
/// - T2, short, R 2, e 104.9, x 107.1, funding 0.00105 paid: E′
///   104.88951, fee 0.05244476, X′ 107.11071, fee 0.05355536; gross −2.2 /
///   2 = −1.1, net (−2.2212 − 0.10600012 − 0.00105) / 2 = −1.16412506;
///   displacement 0.1 / 2 = 0.05.
/// - T3, long, R 1, e 100.1, x 102.05: E′ 100.11001, fee 0.050055, X′
///   102.039795, fee 0.0510199; gross 1.95, net (1.929785 − 0.1010749) =
///   1.8287101; displacement 0.1.
/// - T4, long, R 3, e 100.2, x 99.9, funding −0.002 received: E′
///   100.21002, fee 0.05010501, X′ 99.89001, fee 0.049945; gross −0.3 / 3
///   = −0.1, net (−0.32001 − 0.10005001 + 0.002) / 3 = −0.41806001 / 3 =
///   −0.13935333666… → −0.13935334; displacement 0.2 / 3 → 0.06666667.
fn g1_records() -> Vec<TradeRecord> {
    use EventKind::{Kline, Liquidation, MarkPrice, Trade};
    use PositionSide::{Long, Short};
    use RegimeLabel::{Extreme, High, Low};
    let label = |label| vec![Some(label)];
    vec![
        TradeRecord {
            entry: at(1_759_280_400_250, "100.5"),
            exit: Exit::Target,
            fill: at(1_759_280_460_001, "104.2"),
            funding: Some(usd("0")),
            gross: r("1.85"),
            net: r("1.7885901"),
            displacement: r("0.25"),
            mfe: r("1.85"),
            mae: r("0.5"),
            ..base(
                key(1_759_280_400_000, Trade, 1_001),
                Long,
                label(Low),
                LONG,
                1_759_280_460_000,
            )
        },
        TradeRecord {
            entry: at(1_759_284_000_250, "104.9"),
            exit: Exit::Stop,
            fill: at(1_759_284_100_001, "107.1"),
            funding: Some(usd("0.00105")),
            gross: r("-1.1"),
            net: r("-1.16412506"),
            displacement: r("0.05"),
            mfe: r("0.4"),
            mae: r("1.1"),
            ..base(
                key(1_759_284_000_000, Kline, 0),
                Short,
                label(High),
                ["105", "107", "101", "2"],
                1_759_284_100_000,
            )
        },
        TradeRecord {
            entry: at(1_759_287_600_250, "100.1"),
            exit: Exit::Target,
            fill: at(1_759_287_690_002, "102.05"),
            funding: Some(usd("0")),
            gross: r("1.95"),
            net: r("1.8287101"),
            displacement: r("0.1"),
            mfe: r("1.95"),
            mae: r("0.3"),
            ..base(
                key(1_759_287_600_000, MarkPrice, 0),
                Long,
                label(High),
                ["100", "99", "102", "1"],
                1_759_287_690_001,
            )
        },
        TradeRecord {
            entry: at(1_759_291_200_250, "100.2"),
            exit: Exit::Time,
            fill: at(1_759_294_800_000, "99.9"),
            funding: Some(usd("-0.002")),
            gross: r("-0.1"),
            net: r("-0.13935334"),
            displacement: r("0.06666667"),
            mfe: r("0.43333333"),
            mae: r("0.33333333"),
            ..base(
                key(1_759_291_200_000, Trade, 5_001),
                Long,
                label(Extreme),
                ["100", "97", "106", "3"],
                1_759_294_800_000,
            )
        },
        base(
            key(1_759_298_400_000, Trade, 6_001),
            Long,
            vec![None],
            LONG,
            1_759_302_000_000,
        ),
        TradeRecord {
            entry: at(1_759_305_600_250, "100"),
            exit: Exit::Censored(CensorReason::Gap),
            displacement: r("0"),
            mfe: r("0.5"),
            mae: r("0.25"),
            ..base(
                key(1_759_305_600_000, Trade, 7_001),
                Long,
                label(High),
                LONG,
                1_759_305_700_000,
            )
        },
        TradeRecord {
            exit: Exit::Censored(CensorReason::Rejected),
            ..base(
                key(1_759_309_200_000, Kline, 0),
                Long,
                label(Low),
                LONG,
                1_759_309_200_100,
            )
        },
        TradeRecord {
            entry: at(1_759_312_800_250, "99.9"),
            exit: Exit::Censored(CensorReason::EndOfData),
            displacement: r("0.05"),
            mfe: r("0.3"),
            mae: r("0.1"),
            ..base(
                key(1_759_312_800_000, Trade, 9_001),
                Short,
                vec![None],
                ["100", "102", "96", "2"],
                1_759_313_400_000,
            )
        },
        TradeRecord {
            exit: Exit::Censored(CensorReason::ChainRestart),
            ..base(
                key(1_759_316_400_000, Liquidation, 0),
                Long,
                label(Extreme),
                LONG,
                1_759_316_400_000,
            )
        },
        TradeRecord {
            entry: at(1_759_320_000_250, "100.1"),
            exit: Exit::Censored(CensorReason::FundingGap),
            displacement: r("0.05"),
            mfe: r("0.2"),
            mae: r("0.15"),
            ..base(
                key(1_759_320_000_000, Trade, 10_001),
                Long,
                label(Low),
                LONG,
                1_759_320_100_000,
            )
        },
        TradeRecord {
            exit: Exit::Censored(CensorReason::Overflow),
            ..base(
                key(1_759_323_600_000, Trade, 11_001),
                Long,
                vec![None],
                LONG,
                1_759_323_600_250,
            )
        },
    ]
}

/// 23 triggers = 3 filtered + 9 skipped + 11 trade lines.
fn g1_stats() -> SimulationStats {
    SimulationStats {
        triggers: 23,
        filtered: 3,
        skipped: SkipCounts {
            overlap: 4,
            stale_reference: 1,
            no_side: 2,
            no_risk: 1,
            stop_side: 1,
        },
        reach_backs: 1,
    }
}

fn result_on(spec: &str, stats: SimulationStats, records: Vec<TradeRecord>) -> ExperimentResult {
    ExperimentResult {
        spec: ExperimentSpec::parse(spec, &catalog::registry()).unwrap(),
        pipeline: PIPELINE,
        outcome: Outcome::Backtest {
            summary: summary(),
            stats,
            records,
        },
    }
}

fn g1() -> ExperimentResult {
    result_on(&golden(), g1_stats(), g1_records())
}

fn parse(text: &str) -> Result<ExperimentResult, ResultParseError> {
    ExperimentResult::parse(text, &catalog::registry())
}

/// The provenance lines of both goldens.
const PROVENANCE: &str = "origin 1759276800000\n\
                          effective-origin -\n\
                          cutover 1791244800000\n\
                          prefix-data -\n\
                          prefix-stream 0:cbf29ce484222325\n\
                          prefix-rejections 0\n\
                          engine-features 0fedcba987654321\n\
                          state-encoding 1\n\
                          state-start 5555666677778888\n\
                          stream 4800:0123456789abcdef\n\
                          rejections 0\n\
                          state-end 9999aaaabbbbcccc\n";

/// Golden G1 (#168 acceptance 3). Its metric lines, derived by hand from
/// the trade lines (ADR-060 D11):
///
/// - **all**, trades T1–T4, 6 censored, 1 unfilled: expectancy (1.78859010
///   − 1.16412506 + 1.82871010 − 0.13935334) / 4 = 2.3138218 / 4 =
///   0.57845545; gross 2.6 / 4 = 0.65; profit factor 3.6173002 /
///   1.3034784 = 2.77511327 (rounded once); win rate 2 / 4; drawdown: the
///   cumulative net 1.78859010 (peak), 0.62446504, 2.45317514 (peak),
///   2.31382180, so the largest fall is T2's 1.16412506; time to target
///   (60 000 + 90 001) / 2 = 75 000.5, a tie, → the even 75 000; MFE
///   4.63333333 / 4 = 1.1583333325 → 1.15833333; MAE 2.23333333 / 4 →
///   0.55833333; displacement 0.46666667 / 4 = 0.1166666675 → 0.11666667.
/// - **LOW**, T1 and two censored lines: T1's own values, profit factor
///   `-` (no losing trade), win rate 1, drawdown 0.
/// - **MEDIUM**: empty, every value `-` but the counts and drawdown 0.
/// - **HIGH**, T2, T3 and a censored line: expectancy 0.66458504 / 2 =
///   0.33229252; gross 0.85 / 2 = 0.425; profit factor 1.8287101 /
///   1.16412506 = 1.57088801; win rate 0.5; drawdown 1.16412506 (T2
///   first, from the peak 0); time 90 001; MFE 2.35 / 2, MAE 1.4 / 2,
///   displacement 0.15 / 2.
/// - **EXTREME**, T4 and a censored line: T4's values, profit factor 0 /
///   0.13935334 = 0, win rate 0, drawdown 0.13935334, time `-` (no target
///   exit).
/// - **-**, the unfilled line and two censored lines: no trade.
fn g1_text() -> String {
    format!(
        "mie-result 1\n\
         experiment 3f902847001e1998\n\
         pipeline research.backtest@1\n\
         outcome backtest\n\
         {PROVENANCE}\
         triggers 23\n\
         filtered 3\n\
         skipped overlap=4 stale_reference=1 no_side=2 no_risk=1 stop_side=1\n\
         reach-backs 1\n\
         trade 1759280400000 trade 1001 side=long regime=LOW reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 \
         entry=1759280400250:100.50000000 exit=target exit_at=1759280460000 \
         fill=1759280460001:104.20000000 funding=0.00000000 gross=1.85000000 net=1.78859010 \
         displacement=0.25000000 mfe=1.85000000 mae=0.50000000 censor=-\n\
         trade 1759284000000 kline 0 side=short regime=HIGH reference=105.00000000 \
         stop=107.00000000 target=101.00000000 risk=2.00000000 \
         entry=1759284000250:104.90000000 exit=stop exit_at=1759284100000 \
         fill=1759284100001:107.10000000 funding=0.00105000 gross=-1.10000000 \
         net=-1.16412506 displacement=0.05000000 mfe=0.40000000 mae=1.10000000 censor=-\n\
         trade 1759287600000 mark_price 0 side=long regime=HIGH reference=100.00000000 \
         stop=99.00000000 target=102.00000000 risk=1.00000000 \
         entry=1759287600250:100.10000000 exit=target exit_at=1759287690001 \
         fill=1759287690002:102.05000000 funding=0.00000000 gross=1.95000000 net=1.82871010 \
         displacement=0.10000000 mfe=1.95000000 mae=0.30000000 censor=-\n\
         trade 1759291200000 trade 5001 side=long regime=EXTREME reference=100.00000000 \
         stop=97.00000000 target=106.00000000 risk=3.00000000 \
         entry=1759291200250:100.20000000 exit=time exit_at=1759294800000 \
         fill=1759294800000:99.90000000 funding=-0.00200000 gross=-0.10000000 \
         net=-0.13935334 displacement=0.06666667 mfe=0.43333333 mae=0.33333333 censor=-\n\
         trade 1759298400000 trade 6001 side=long regime=- reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 entry=- exit=unfilled \
         exit_at=1759302000000 fill=- funding=- gross=- net=- displacement=- mfe=- mae=- \
         censor=-\n\
         trade 1759305600000 trade 7001 side=long regime=HIGH reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 \
         entry=1759305600250:100.00000000 exit=censored exit_at=1759305700000 fill=- \
         funding=- gross=- net=- displacement=0.00000000 mfe=0.50000000 mae=0.25000000 \
         censor=gap\n\
         trade 1759309200000 kline 0 side=long regime=LOW reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 entry=- exit=censored \
         exit_at=1759309200100 fill=- funding=- gross=- net=- displacement=- mfe=- mae=- \
         censor=rejected\n\
         trade 1759312800000 trade 9001 side=short regime=- reference=100.00000000 \
         stop=102.00000000 target=96.00000000 risk=2.00000000 \
         entry=1759312800250:99.90000000 exit=censored exit_at=1759313400000 fill=- \
         funding=- gross=- net=- displacement=0.05000000 mfe=0.30000000 mae=0.10000000 \
         censor=end_of_data\n\
         trade 1759316400000 liquidation 0 side=long regime=EXTREME reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 entry=- exit=censored \
         exit_at=1759316400000 fill=- funding=- gross=- net=- displacement=- mfe=- mae=- \
         censor=chain_restart\n\
         trade 1759320000000 trade 10001 side=long regime=LOW reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 \
         entry=1759320000250:100.10000000 exit=censored exit_at=1759320100000 fill=- \
         funding=- gross=- net=- displacement=0.05000000 mfe=0.20000000 mae=0.15000000 \
         censor=funding_gap\n\
         trade 1759323600000 trade 11001 side=long regime=- reference=100.00000000 \
         stop=98.00000000 target=104.00000000 risk=2.00000000 entry=- exit=censored \
         exit_at=1759323600250 fill=- funding=- gross=- net=- displacement=- mfe=- mae=- \
         censor=overflow\n\
         metrics all trades=4 censored=6 unfilled=1 expectancy=0.57845545 \
         mean_gross=0.65000000 profit_factor=2.77511327 win_rate=0.50000000 \
         max_drawdown=1.16412506 time_to_target=75000 mean_mfe=1.15833333 \
         mean_mae=0.55833333 mean_displacement=0.11666667\n\
         metrics volatility.regime.1h@1 LOW trades=1 censored=2 unfilled=0 \
         expectancy=1.78859010 mean_gross=1.85000000 profit_factor=- win_rate=1.00000000 \
         max_drawdown=0.00000000 time_to_target=60000 mean_mfe=1.85000000 \
         mean_mae=0.50000000 mean_displacement=0.25000000\n\
         metrics volatility.regime.1h@1 MEDIUM trades=0 censored=0 unfilled=0 expectancy=- \
         mean_gross=- profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- \
         mean_mfe=- mean_mae=- mean_displacement=-\n\
         metrics volatility.regime.1h@1 HIGH trades=2 censored=1 unfilled=0 \
         expectancy=0.33229252 mean_gross=0.42500000 profit_factor=1.57088801 \
         win_rate=0.50000000 max_drawdown=1.16412506 time_to_target=90001 \
         mean_mfe=1.17500000 mean_mae=0.70000000 mean_displacement=0.07500000\n\
         metrics volatility.regime.1h@1 EXTREME trades=1 censored=1 unfilled=0 \
         expectancy=-0.13935334 mean_gross=-0.10000000 profit_factor=0.00000000 \
         win_rate=0.00000000 max_drawdown=0.13935334 time_to_target=- mean_mfe=0.43333333 \
         mean_mae=0.33333333 mean_displacement=0.06666667\n\
         metrics volatility.regime.1h@1 - trades=0 censored=2 unfilled=1 expectancy=- \
         mean_gross=- profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- \
         mean_mfe=- mean_mae=- mean_displacement=-\n\
         spec\n\
         {}",
        golden()
    )
}

#[test]
fn golden_g1_is_pinned_and_round_trips() {
    let result = g1();
    assert_eq!(result.canonical_text(), g1_text());
    assert_eq!(
        result.key().to_string(),
        "3f902847001e1998/research.backtest@1-0fedcba987654321-1"
    );
    assert_eq!(parse(&g1_text()), Ok(result));
}

/// The golden spec without a regime feature: its set without
/// `volatility.regime.1h@1`, and `regime-filter none`.
fn spec_without_regime() -> String {
    let features = "bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,volatility.atr.1h@1";
    golden()
        .replace(
            "features 4a14739bcf46afb3 bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,\
             volatility.atr.1h@1,volatility.regime.1h@1",
            &format!("features {G2_FEATURES_HEX} {features}"),
        )
        .replace(
            "regime-filter volatility.regime.1h@1 HIGH,EXTREME",
            "regime-filter none",
        )
}

/// The version of G2's feature set, pinned by
/// `golden_g2_is_pinned_and_round_trips`.
const G2_FEATURES_HEX: &str = "5cc3f2d21c618361";

/// G2's records: a spec without a regime feature (`regime=none`), and a
/// profit factor outside the `i64` unit range. Their net values are chosen
/// for that edge, not derived from the fills (parsing never re-derives a
/// trade line, #171): P = 1 000 R over N = 10⁻⁸ R gives 10¹¹ · 10⁸ units,
/// above 2⁶³.
fn g2_records() -> Vec<TradeRecord> {
    vec![
        TradeRecord {
            entry: at(1_759_280_400_250, "100"),
            exit: Exit::Target,
            fill: at(1_759_280_401_001, "100.1"),
            funding: Some(usd("0")),
            gross: r("1000"),
            net: r("1000"),
            displacement: r("0"),
            mfe: r("1000"),
            mae: r("0"),
            ..base(
                key(1_759_280_400_000, EventKind::Trade, 1),
                PositionSide::Long,
                Vec::new(),
                ["100", "99.9999", "100.1", "0.0001"],
                1_759_280_401_000,
            )
        },
        TradeRecord {
            entry: at(1_759_284_000_250, "100"),
            exit: Exit::Stop,
            fill: at(1_759_284_001_001, "100"),
            funding: Some(usd("0")),
            gross: r("0"),
            net: r("-0.00000001"),
            displacement: r("0"),
            mfe: r("0"),
            mae: r("1"),
            ..base(
                key(1_759_284_000_000, EventKind::Kline, 0),
                PositionSide::Short,
                Vec::new(),
                ["100", "100.0001", "99.9", "0.0001"],
                1_759_284_001_000,
            )
        },
    ]
}

/// Golden G2. Its one metrics line: expectancy (1 000 − 0.00000001) / 2 =
/// 499.999999995, a tie in the last place, → the even 500.00000000; gross
/// 1 000 / 2 = 500; profit factor `-` (outside the range); win rate 0.5;
/// drawdown 0.00000001; time to target 1 000 ms; MFE 500, MAE 0.5,
/// displacement 0.
#[test]
fn golden_g2_is_pinned_and_round_trips() {
    let registry = catalog::registry();
    let keys: Vec<_> = "bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,volatility.atr.1h@1"
        .split(',')
        .map(|text| registry.resolve(text).unwrap().key)
        .collect();
    let version = FeatureSet::new(&registry, &keys).unwrap().version();
    assert_eq!(version.to_string(), G2_FEATURES_HEX);

    let stats = SimulationStats {
        triggers: 2,
        ..SimulationStats::default()
    };
    let result = result_on(&spec_without_regime(), stats, g2_records());
    assert!(result.spec.regime_features().is_empty());
    let expected = format!(
        "mie-result 1\n\
         experiment 92c7d837754d846d\n\
         pipeline research.backtest@1\n\
         outcome backtest\n\
         {PROVENANCE}\
         triggers 2\n\
         filtered 0\n\
         skipped overlap=0 stale_reference=0 no_side=0 no_risk=0 stop_side=0\n\
         reach-backs 0\n\
         trade 1759280400000 trade 1 side=long regime=none reference=100.00000000 \
         stop=99.99990000 target=100.10000000 risk=0.00010000 \
         entry=1759280400250:100.00000000 exit=target exit_at=1759280401000 \
         fill=1759280401001:100.10000000 funding=0.00000000 gross=1000.00000000 \
         net=1000.00000000 displacement=0.00000000 mfe=1000.00000000 mae=0.00000000 \
         censor=-\n\
         trade 1759284000000 kline 0 side=short regime=none reference=100.00000000 \
         stop=100.00010000 target=99.90000000 risk=0.00010000 \
         entry=1759284000250:100.00000000 exit=stop exit_at=1759284001000 \
         fill=1759284001001:100.00000000 funding=0.00000000 gross=0.00000000 \
         net=-0.00000001 displacement=0.00000000 mfe=0.00000000 mae=1.00000000 censor=-\n\
         metrics all trades=2 censored=0 unfilled=0 expectancy=500.00000000 \
         mean_gross=500.00000000 profit_factor=- win_rate=0.50000000 \
         max_drawdown=0.00000001 time_to_target=1000 mean_mfe=500.00000000 \
         mean_mae=0.50000000 mean_displacement=0.00000000\n\
         spec\n\
         {}",
        spec_without_regime()
    );
    assert_eq!(result.canonical_text(), expected);
    assert_eq!(
        result.key().to_string(),
        "92c7d837754d846d/research.backtest@1-0fedcba987654321-1"
    );
    assert_eq!(parse(&expected), Ok(result));
}

/// The simulator's output parses: S2's hand-computed fixture, with each
/// regime list normalised to one entry for G1's spec, round-trips. So the
/// parser's shapes accept what the simulator produces.
#[test]
fn simulator_output_round_trips() {
    let (mut records, stats) = fixture(costs(10, 1_000, "0.001", "0.0005", FundingRule::Recorded));
    for record in &mut records {
        record.regime = vec![record.regime.first().copied().flatten()];
    }
    let result = result_on(&golden(), stats, records);
    assert_eq!(parse(&result.canonical_text()), Ok(result));
}

/// Two decisions may share a canonical key (klines of one millisecond),
/// so decision order is non-decreasing, not strict.
#[test]
fn equal_decision_keys_parse() {
    let mut records = g1_records();
    records.insert(1, records[0].clone());
    let stats = SimulationStats {
        triggers: 24,
        ..g1_stats()
    };
    let result = result_on(&golden(), stats, records);
    assert_eq!(parse(&result.canonical_text()), Ok(result));
}

#[test]
fn only_canonical_backtest_text_parses() {
    let text = g1_text();
    let low = "metrics volatility.regime.1h@1 LOW ";
    let medium = "metrics volatility.regime.1h@1 MEDIUM ";
    let swapped = text
        .replace(low, "<low>")
        .replace(medium, low)
        .replace("<low>", medium);
    let non_canonical = [
        text.replace("triggers 23", "triggers 023"),
        text.replace("triggers 23", "triggers +23"),
        text.replacen("reference=100.00000000", "reference=100.0", 1),
        text.replacen("reference=100.00000000", "reference=100.5", 1),
        text.replacen("funding=0.00000000", "funding=-0.00000000", 1),
        text.replace("displacement=0.00000000", "displacement=-0.00000000"),
        text.replace(
            "trade 1759284000000 kline 0",
            "trade 1759284000000 kline 00",
        ),
        text.replace("expectancy=0.57845545", "expectancy=0.57845546"),
        text.replace("time_to_target=75000", "time_to_target=75001"),
        swapped,
    ];
    for text in non_canonical {
        assert_eq!(parse(&text), Err(ResultParseError::NotCanonical), "{text}");
    }
}

/// The line of a malformed text and the start of its reason.
fn malformed(text: &str) -> (usize, String) {
    match parse(text) {
        Err(ResultParseError::Malformed { line, reason }) => (line, reason),
        other => panic!("{text}: {other:?}"),
    }
}

#[test]
fn malformed_backtest_text_names_the_line() {
    let text = g1_text();
    let t1 = 21;
    let unfilled = 25;
    let gap = 26;
    let cases = [
        // Fields out of order, and a missing field.
        (
            text.replace(
                "stop=98.00000000 target=104.00000000 risk=2.00000000 entry=1759280400250",
                "target=104.00000000 stop=98.00000000 risk=2.00000000 entry=1759280400250",
            ),
            t1,
        ),
        (
            text.replacen(" mae=0.50000000 censor=-", " censor=-", 1),
            t1,
        ),
        // The exit and censor pairs.
        (
            text.replacen("mae=0.50000000 censor=-", "mae=0.50000000 censor=gap", 1),
            t1,
        ),
        (text.replace("censor=gap", "censor=-"), gap),
        (text.replace("censor=gap", "censor=stale_reference"), gap),
        (text.replacen("exit=target", "exit=touch", 1), t1),
        // Values and positional fields.
        (
            text.replace("1759280400000 trade 1001", "1759280400000 agg_trade 1001"),
            t1,
        ),
        (
            text.replace("1759280400000 trade 1001", "1759280400000 trade x"),
            t1,
        ),
        (text.replacen("side=long", "side=buy", 1), t1),
        (text.replacen("regime=LOW", "regime=CALM", 1), t1),
        (text.replacen("regime=LOW", "regime=", 1), t1),
        (text.replacen("risk=2.00000000", "risk=2.000000001", 1), t1),
        (
            text.replacen("entry=1759280400250:100.50000000", "entry=1759280400250", 1),
            t1,
        ),
        // Shapes: a target line with a `-`, an unfilled line with an
        // entry, a censored line with a fill, a partial entry, an entry
        // at the censor time.
        (text.replacen("mae=0.50000000", "mae=-", 1), t1),
        (
            text.replace("entry=- exit=unfilled", "entry=1:1 exit=unfilled"),
            unfilled,
        ),
        (
            text.replace(
                "exit_at=1759305700000 fill=-",
                "exit_at=1759305700000 fill=1759305700001:99.00000000",
            ),
            gap,
        ),
        (
            text.replace("displacement=0.00000000", "displacement=-"),
            gap,
        ),
        (
            text.replace(
                "entry=1759305600250:100.00000000",
                "entry=1759305700000:100.00000000",
            ),
            gap,
        ),
        // Conservation off by one, on the `triggers` line.
        (text.replace("triggers 23", "triggers 24"), 17),
        (text.replace("reach-backs 1", "reach-backs one"), 20),
        (text.replace("skipped overlap=4", "skipped overlaps=4"), 19),
        // Regime arity against the spec's one regime feature.
        (text.replacen("regime=LOW", "regime=LOW,HIGH", 1), t1),
        (text.replacen("regime=LOW", "regime=none", 1), t1),
    ];
    for (text, line) in cases {
        assert_eq!(malformed(&text).0, line, "{text}");
    }

    // Trade lines out of order: the second line is below the first.
    let lines: Vec<&str> = text.split('\n').collect();
    let mut swapped = lines.clone();
    swapped.swap(t1 - 1, t1);
    let (line, reason) = malformed(&swapped.join("\n"));
    assert_eq!(line, t1 + 1);
    assert!(reason.contains("decision order"), "{reason}");

    // A missing `metrics all`, and a trade line after the metric lines.
    let mut no_all = lines.clone();
    no_all.remove(31);
    let (line, reason) = malformed(&no_all.join("\n"));
    assert_eq!(line, 32);
    assert!(reason.contains("metrics all"), "{reason}");
    let mut late = lines.clone();
    late.insert(32, lines[t1 - 1]);
    assert_eq!(malformed(&late.join("\n")).0, 33);
    let mut trailing = lines.clone();
    trailing.insert(37, lines[t1 - 1]);
    assert_eq!(malformed(&trailing.join("\n")).0, 38);

    // The pipeline fixes the kind, both ways, at line 4.
    for pipeline in ["research.replay_summary@1", "research.replay_summary@2"] {
        let text = text.replace(
            "pipeline research.backtest@1",
            &format!("pipeline {pipeline}"),
        );
        let (line, reason) = malformed(&text);
        assert_eq!(line, 4);
        assert!(
            reason.ends_with(&format!("the outcome of {pipeline}")),
            "{reason}"
        );
    }
}

/// Extreme values in every field render and parse back, and counts whose
/// sum overflows `u64` are malformed, never a panic.
#[test]
fn extreme_values_never_panic() {
    let min_price = Price::from_units(i64::MIN);
    let max_rate = Some(Rate::from_units(i64::MAX));
    let extreme = |decision: CanonicalKey, exit_at: i64| TradeRecord {
        decision,
        side: PositionSide::Short,
        regime: vec![Some(RegimeLabel::Extreme)],
        reference: min_price,
        stop: Price::from_units(i64::MAX),
        target: min_price,
        risk: min_price,
        entry: Some(Fill {
            at: t(i64::MIN),
            price: min_price,
        }),
        exit: Exit::Target,
        exit_at: t(exit_at),
        fill: Some(Fill {
            at: t(i64::MAX),
            price: Price::from_units(i64::MAX),
        }),
        funding: Some(min_price),
        gross: Some(Rate::from_units(i64::MIN)),
        net: max_rate,
        displacement: max_rate,
        mfe: max_rate,
        mae: Some(Rate::from_units(i64::MIN)),
    };
    let records = vec![
        extreme(key(i64::MIN, EventKind::FeedGap, 0), i64::MAX),
        extreme(key(i64::MAX, EventKind::Kline, u64::MAX), i64::MIN),
    ];
    let stats = SimulationStats {
        triggers: u64::MAX,
        filtered: u64::MAX - 2,
        skipped: SkipCounts::default(),
        reach_backs: u64::MAX,
    };
    let result = result_on(&golden(), stats, records);
    let text = result.canonical_text();
    assert_eq!(parse(&text), Ok(result));

    let overflow = text.replace(
        "skipped overlap=0 stale_reference=0",
        &format!("skipped overlap={} stale_reference=3", u64::MAX),
    );
    assert_eq!(malformed(&overflow).0, 17);
    for (from, to) in [
        ("trades=2", "trades=18446744073709551616"),
        ("trade -9223372036854775808", "trade -9223372036854775809"),
        (
            "reference=-92233720368.54775808",
            "reference=-92233720368.54775809",
        ),
        ("kline 18446744073709551615", "kline 18446744073709551616"),
    ] {
        assert!(text.contains(from), "{from}");
        assert!(
            matches!(
                parse(&text.replacen(from, to, 1)),
                Err(ResultParseError::Malformed { .. })
            ),
            "{to}"
        );
    }
}

#[test]
fn name_tables_are_pinned() {
    // ADR-057 D2's strings: the parquet adapter pins its own table against
    // this one.
    let kinds = [
        (EventKind::FeedGap, "feed_gap"),
        (EventKind::BookSnapshot, "book_snapshot"),
        (EventKind::Trade, "trade"),
        (EventKind::Liquidation, "liquidation"),
        (EventKind::BookUpdate, "book_update"),
        (EventKind::MarkPrice, "mark_price"),
        (EventKind::FundingSettlement, "funding_settlement"),
        (EventKind::OpenInterest, "open_interest"),
        (EventKind::Kline, "kline"),
    ];
    for (kind, name) in kinds {
        assert_eq!(event_kind_name(kind), name);
        assert_eq!(event_kind_from_name(name), Some(kind));
    }
    assert_eq!(
        EVENT_KINDS.map(event_kind_name),
        kinds.map(|(_, name)| name)
    );
    for unknown in ["", "Trade", "agg_trade", "trades"] {
        assert_eq!(event_kind_from_name(unknown), None, "{unknown}");
    }
    // ADR-060 D12's censor tokens; the first three are ADR-057's
    // `race_censor_reason` strings.
    assert_eq!(
        CENSOR_REASONS.map(CensorReason::as_str),
        [
            "gap",
            "rejected",
            "end_of_data",
            "chain_restart",
            "funding_gap",
            "overflow"
        ]
    );
    assert_eq!(
        [
            Exit::Target,
            Exit::Stop,
            Exit::Time,
            Exit::Unfilled,
            Exit::Censored(CensorReason::Gap)
        ]
        .map(Exit::as_str),
        ["target", "stop", "time", "unfilled", "censored"]
    );
}

/// Each line kind on its own: counts, a trade line, a metrics line.
#[test]
fn lines_round_trip_on_their_own() {
    let mut out = String::new();
    render_counts(&g1_stats(), &mut out);
    let lines: Vec<&str> = out.lines().collect();
    assert_eq!(parse_count(lines[0], "triggers"), Ok(23));
    assert_eq!(parse_count(lines[1], "filtered"), Ok(3));
    assert_eq!(parse_skipped(lines[2]), Ok(g1_stats().skipped));
    assert_eq!(parse_count(lines[3], "reach-backs"), Ok(1));
    assert!(conserves(&g1_stats(), 11));
    assert!(!conserves(&g1_stats(), 10));

    for record in g1_records() {
        let mut out = String::new();
        render_trade(&record, &mut out);
        assert_eq!(parse_trade(out.trim_end()), Ok(record));
    }

    let records = g1_records();
    let metrics = BacktestMetrics::compute(&records, 1);
    let mut out = String::new();
    render_metrics(
        &metrics,
        &[FeatureKey::new("volatility.regime.1h", 1)],
        &mut out,
    );
    let parsed: Vec<_> = out
        .lines()
        .map(|line| parse_metrics(line).unwrap())
        .collect();
    assert_eq!(parsed.len(), 6);
    assert_eq!(parsed[0], (MetricsScope::All, metrics.all));
    for (line, bucket) in parsed[1..].iter().zip(metrics.regimes[0]) {
        assert_eq!(*line, (MetricsScope::Bucket, bucket));
    }
    for bad in [
        "metrics",
        "metrics all",
        "metrics volatility.regime.1h LOW trades=0",
        "metrics volatility.regime.1h@1 CALM trades=0",
        "trade 1 trade 1",
    ] {
        assert!(parse_metrics(bad).is_err(), "{bad}");
    }
}
