//! The backtest pipeline `research.backtest@1` (ADR-060 D13, D14, #170): a
//! spec's hypothesis simulated on the history chain, recorded once and
//! reproduced exactly. The in-memory providers and store stand in for the
//! history chain and the file adapter (`mie-app` may not depend on an
//! adapter, ADR-025), so the CLI's offline test pins the real chain.
//!
//! The tape is the SFP pattern of the domain's trigger tests
//! (`crates/mie-domain/src/trigger.rs`, `Tape::swing_high` and `sfp_tape`)
//! on the 15m timeframe, after a prefix that makes ATR(1h) ready with a
//! value derivable by hand.

mod common;

use common::{Feed, MemoryStore, Recorded, Unopenable};
use mie_app::backtest::ChainBacktest;
use mie_app::{
    BACKTEST_V1, BacktestService, ChainBounds, ExperimentService, FundingCoverage,
    REPLAY_SUMMARY_V2,
};
use mie_domain::backtest::resolve;
use mie_domain::bars::MAX_BARS_PER_EVENT;
use mie_domain::event::{
    Aggressor, FeedGap, FundingSettlement, GapReason, MarketEvent, Stream, Trade,
};
use mie_domain::feature::{
    FeatureDefinition, FeatureKey, FeatureRegistry, FeatureSet, FeatureSetError, Input, WarmUp,
    catalog,
};
use mie_domain::num::{Price, Qty, Rate, SCALE};
use mie_domain::research::{ExperimentSpec, HistoryReplaySummary, Outcome};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
use mie_ports::outbound::{HistoricalDataProvider, ProviderError};

const MINUTE: i64 = 60_000;
const HOUR: i64 = 3_600_000;
/// The structure timeframe of the SFP: 15m.
const SPAN: i64 = 15 * MINUTE;
/// The chain's origin.
const ORIGIN: i64 = 0;
/// The sample's start and the pattern's first bar: 16 prefix hours.
const T0: i64 = 16 * HOUR;
/// The trade 1 s into bar 12 that closes bar 11 and fires the SFP.
const DECISION: i64 = T0 + 12 * SPAN + 1_000;
/// The default sample's end.
const END: i64 = T0 + 4 * HOUR;
/// The chain's cutover, after every sample here unless a test moves it.
const CUTOVER: i64 = 100 * 24 * HOUR;

/// Whole USDT as a price.
fn usdt(whole: i64) -> Price {
    Price::from_units(whole * SCALE)
}

/// A tape of trades, ids counting up, with funding settlements.
#[derive(Default, Clone)]
struct Tape {
    events: Vec<MarketEvent>,
    trade_id: u64,
}

impl Tape {
    fn trade(&mut self, millis: i64, whole: i64) -> &mut Self {
        self.trade_id += 1;
        self.events.push(MarketEvent::Trade(Trade {
            time: EventTime::from_millis(millis),
            trade_id: self.trade_id,
            price: usdt(whole),
            qty: Qty::from_units(10_000_000),
            aggressor: Aggressor::Buy,
        }));
        self
    }

    fn settle(&mut self, millis: i64, rate_units: i64) -> &mut Self {
        self.events
            .push(MarketEvent::FundingSettlement(FundingSettlement {
                time: EventTime::from_millis(millis),
                rate: Rate::from_units(rate_units),
            }));
        self
    }
}

/// The prefix and the SFP pattern up to the decision.
///
/// - *Prefix* `[0, T0)`: 16 hours, each with three trades 1, 2 and 3 s
///   into the hour at 59 176, 59 226 and 59 176 USDT, so every 1h bar has
///   high − low = 50 and the previous close inside it: a true range of 50.
///   The first hour is `partial_start` and only anchors; hours 1 … 14 seed
///   ATR(14) with their mean, 50, and hour 15 (closed by the pattern's
///   first trade) keeps it: ⟨(13 · 50 + 50) / 14⟩ = 50. The prefix lies
///   800 USDT below the swing, so it emits no `trigger.sfp.15m@1`.
/// - *Pattern* from T0, the domain's `sfp_tape(M15, false)`: bars 0 … 10
///   of 15m with highs 59 990, 59 992, 59 994, 59 996, 60 010, 60 011,
///   60 012, 60 015, 60 012, 60 011, 60 010, each as three trades (high,
///   high − 5, high − 2), a swing high at 60 015 on bar 7; bar 11 opens at
///   60 008, sweeps to 60 020 and closes back at 60 014; the trade at
///   60 014 1 s into bar 12 closes bar 11: the decision (the SFP fires).
fn base() -> Tape {
    let mut tape = Tape::default();
    for hour in 0..16 {
        let open = hour * HOUR;
        tape.trade(open + 1_000, 59_176)
            .trade(open + 2_000, 59_226)
            .trade(open + 3_000, 59_176);
    }
    for (index, high) in (0..).zip([
        59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011, 60_010,
    ]) {
        let open = T0 + index * SPAN;
        tape.trade(open + 1_000, high)
            .trade(open + 2_000, high - 5)
            .trade(open + 3_000, high - 2);
    }
    tape.trade(T0 + 11 * SPAN + 1_000, 60_008)
        .trade(T0 + 11 * SPAN + 2_000, 60_020)
        .trade(T0 + 11 * SPAN + 3_000, 60_014)
        .trade(DECISION, 60_014);
    tape
}

/// The hand-computed tape: [`base`], then the entry fill trade at 60 016
/// 1 s after the decision, 60 020 0.5 s later (the entry fill under a
/// latency of 1.5 s: the path rises monotonically from the decision to it,
/// against the long's entry), a dip to 59 990, a settlement at rate
/// 0.0001, the touch at 60 107 and the exit fill trade at 60 110.
fn tape() -> Tape {
    let mut tape = base();
    tape.trade(DECISION + 1_000, 60_016)
        .trade(DECISION + 1_500, 60_020)
        .trade(DECISION + 2_000, 59_990)
        .settle(DECISION + 2_500, 10_000)
        .trade(DECISION + 3_000, 60_107)
        .trade(DECISION + 4_000, 60_110);
    tape
}

/// The features of every spec here, closed over their upstream features.
fn feature_set(registry: &FeatureRegistry, mut keys: Vec<FeatureKey>) -> FeatureSet {
    loop {
        match FeatureSet::new(registry, &keys) {
            Ok(set) => return set,
            Err(FeatureSetError::MissingDependency { requires, .. }) => keys.push(requires),
            Err(error) => panic!("{error}"),
        }
    }
}

/// The assumptions a spec varies.
#[derive(Clone, Copy)]
struct Variant {
    side: &'static str,
    taker: &'static str,
    slippage: &'static str,
    latency_ms: u32,
    funding: &'static str,
    sample: (i64, i64),
}

const WITH: Variant = Variant {
    side: "with",
    taker: "0.0005",
    slippage: "0.0001",
    latency_ms: 0,
    funding: "funding.recorded@1",
    sample: (T0, END),
};

const AGAINST: Variant = Variant {
    side: "against",
    ..WITH
};

/// The spec text of `variant`: `trigger.sfp.15m@1` decisions, a stop at
/// 1 × ATR(1h), a target at 1 R, a one-hour hold, on the spec's set with
/// `last_price` and `volatility.regime.1h@1`, which is unready on every
/// tape here.
fn spec_text(variant: Variant, registry: &FeatureRegistry, last_price: FeatureKey) -> String {
    let keys = vec![
        catalog::TRIGGER_SFP_15M_V1.key,
        catalog::VOLATILITY_ATR_1H_V1.key,
        catalog::VOLATILITY_REGIME_1H_V1.key,
        last_price,
    ];
    let set = feature_set(registry, keys);
    format!(
        "mie-experiment 1\n\
         hypothesis sfp.15m\n\
         sample {} {}\n\
         data {}\n\
         features {} {set}\n\
         state-filter none\n\
         regime-filter none\n\
         location location.any@1\n\
         trigger trigger.fires@1 feature=feature:trigger.sfp.15m@1\n\
         entry entry.next_trade@1 side=text:{}\n\
         invalidation invalidation.atr@1 atr=feature:volatility.atr.1h@1 \
         multiple=rate:1.00000000\n\
         target target.r_multiple@1 max_hold_ms=int:3600000 r=rate:1.00000000\n\
         fees maker=0.0002 taker={}\n\
         slippage slippage.fixed@1 rate=rate:{}\n\
         funding {}\n\
         latency {}\n",
        variant.sample.0,
        variant.sample.1,
        "0f".repeat(32),
        set.version(),
        variant.side,
        variant.taker,
        variant.slippage,
        variant.funding,
        variant.latency_ms,
    )
}

fn spec(variant: Variant) -> ExperimentSpec {
    let registry = catalog::registry();
    ExperimentSpec::parse(
        &spec_text(variant, &registry, catalog::TRADE_LAST_PRICE_V1.key),
        &registry,
    )
    .unwrap()
}

fn bounds() -> ChainBounds {
    ChainBounds {
        origin: EventTime::from_millis(ORIGIN),
        cutover: EventTime::from_millis(CUTOVER),
    }
}

/// The chain's funding coverage when its archive replays `fundingRate`.
fn covered() -> FundingCoverage {
    FundingCoverage {
        until: Some(EventTime::from_millis(CUTOVER)),
    }
}

type Engine<H> = ChainBacktest<H, Box<dyn Fn(&Feed) -> Vec<FeedGap>>>;

/// The engine over `history` with the trailing gaps `trailing`.
fn engine<H>(history: H, trailing: Vec<FeedGap>) -> Engine<H>
where
    H: HistoricalDataProvider<Stream = Feed>,
{
    ChainBacktest::new(
        history,
        bounds(),
        Box::new(move |_: &Feed| trailing.clone()),
    )
}

/// A backtest service over `events` with a fresh store.
fn service(events: Vec<MarketEvent>) -> BacktestService<Engine<Recorded>, MemoryStore> {
    BacktestService::new(
        engine(Recorded(events), Vec::new()),
        MemoryStore::default(),
        covered(),
    )
}

/// The result text of one recorded run of `spec` over `events`, with the
/// trailing gaps `trailing`.
fn run_text(events: Vec<MarketEvent>, trailing: Vec<FeedGap>, spec: &ExperimentSpec) -> String {
    let mut service = BacktestService::new(
        engine(Recorded(events), trailing),
        MemoryStore::default(),
        covered(),
    );
    let run = service.run(spec).unwrap();
    let ExperimentRun::Recorded(result) = &run else {
        panic!("{run:?}");
    };
    let text = result.canonical_text();
    assert_eq!(service.store().results[&result.key()], text);
    text
}

/// The `@2` summary of `spec` over `events` on the same chain.
fn v2_summary(events: Vec<MarketEvent>, spec: &ExperimentSpec) -> (HistoryReplaySummary, String) {
    let mut service =
        ExperimentService::on_chain(Recorded(events), MemoryStore::default(), bounds());
    let run = service.run(spec).unwrap();
    assert_eq!(run.result().pipeline, REPLAY_SUMMARY_V2);
    let Outcome::HistoryReplaySummary(summary) = &run.result().outcome else {
        panic!("{run:?}");
    };
    (summary.clone(), run.result().canonical_text())
}

/// The value of `field=` on the result's trade line.
fn field<'a>(text: &'a str, name: &str) -> &'a str {
    let trade = text
        .lines()
        .find(|line| line.starts_with("trade "))
        .unwrap_or_else(|| panic!("no trade line in\n{text}"));
    trade
        .split(' ')
        .find_map(|token| token.strip_prefix(name)?.strip_prefix('='))
        .unwrap_or_else(|| panic!("no {name}= in {trade}"))
}

/// The value of the line `<key> <value>`.
fn line<'a>(text: &'a str, key: &str) -> &'a str {
    text.lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("no {key} line in\n{text}"))
}

/// A decimal R value of the result text in units of 10⁻⁸.
fn units(text: &str) -> i64 {
    let negative = text.starts_with('-');
    let (whole, fraction) = text.trim_start_matches('-').split_once('.').unwrap();
    let value = whole.parse::<i64>().unwrap() * SCALE + fraction.parse::<i64>().unwrap();
    if negative { -value } else { value }
}

/// The trade line and the metrics lines of the `with` spec on [`tape`].
///
/// Every value by hand, prices in USDT, R values to 8 places with one
/// half-to-even rounding (ADR-060 D10):
///
/// - *ATR(1h)* at the decision, which closes the third pattern hour: the
///   prefix leaves 50 (see [`base`]). Pattern hour 0 (bars 0 … 3): high
///   59 996, low 59 985, close 59 994, previous close 59 176, so TR =
///   59 996 − 59 176 = 820 and ATR = (13 · 50 + 820) / 14 = 105. Hour 1
///   (bars 4 … 7): high 60 015, low 60 005, close 60 013, TR = 60 015 −
///   59 994 = 21, ATR = (13 · 105 + 21) / 14 = 99. Hour 2 (bars 8 … 11):
///   high 60 020, low 60 005, close 60 014, TR = max(15, 7, 8) = 15, ATR =
///   (13 · 99 + 15) / 14 = 93. Every division is exact.
/// - *Side:* the SFP sweeps a swing high, evidence up (D3), `with` → long.
/// - *Levels* (D5): reference = the decision trade, 60 014; offset =
///   1 × 93; stop = 60 014 − 93 = 59 921; R = 93; target = 60 014 + 1 ×
///   93 = 60 107.
/// - *Entry* (D6, latency 0): the next trade, e = 60 016 at decision +
///   1 s; E′ = 60 016 × 1.0001 = 60 022.0016; entry fee = 0.0005 × E′ =
///   30.0110008; displacement = (60 016 − 60 014) / 93 = 0.0215053763… →
///   0.02150538.
/// - *Path:* 60 020, 59 990 (σ(p − e) = −26), the settlement, 60 107 (≥
///   target: the touch, at decision + 3 s), then the exit fill trade x =
///   60 110.
/// - *Funding* (D7): one settlement after the entry fill and before the
///   exit fill; p = the last trade before it, 59 990; σ · 0.0001 · 59 990
///   = 5.999 paid.
/// - *Exit* (D6): X′ = 60 110 × 0.9999 = 60 103.989; exit fee = 0.0005 ×
///   X′ = 30.0519945.
/// - *gross* = (60 110 − 60 016) / 93 = 94 / 93 = 1.0107526881… →
///   1.01075269.
/// - *net* = (60 103.989 − 60 022.0016 − 30.0110008 − 30.0519945 − 5.999)
///   / 93 = 15.9254047 / 93 = 0.1712409107… → 0.17124091.
/// - *MFE* = 94 / 93 → 1.01075269 (the exit fill trade is on the path);
///   *MAE* = 26 / 93 = 0.2795698924… → 0.27956989.
/// - *Metrics* (D11) of one winning target exit: the means are its values;
///   no losing trade, so `profit_factor=-`; `win_rate` 1; the cumulative
///   net never falls, so `max_drawdown` 0; `time_to_target` = touch −
///   decision = 3 000 ms. Its regime label is unready, so it joins the `-`
///   bucket, and the four labelled buckets are empty.
const WITH_LINES: &str = "\
triggers 1
filtered 0
skipped overlap=0 stale_reference=0 no_side=0 no_risk=0 stop_side=0
reach-backs 0
trade 68401000 trade 85 side=long regime=- reference=60014.00000000 stop=59921.00000000 \
target=60107.00000000 risk=93.00000000 entry=68402000:60016.00000000 exit=target \
exit_at=68404000 fill=68405000:60110.00000000 funding=5.99900000 gross=1.01075269 \
net=0.17124091 displacement=0.02150538 mfe=1.01075269 mae=0.27956989 censor=-
metrics all trades=1 censored=0 unfilled=0 expectancy=0.17124091 mean_gross=1.01075269 \
profit_factor=- win_rate=1.00000000 max_drawdown=0.00000000 time_to_target=3000 \
mean_mfe=1.01075269 mean_mae=0.27956989 mean_displacement=0.02150538
EMPTY_BUCKETS\
metrics volatility.regime.1h@1 - trades=1 censored=0 unfilled=0 expectancy=0.17124091 \
mean_gross=1.01075269 profit_factor=- win_rate=1.00000000 max_drawdown=0.00000000 \
time_to_target=3000 mean_mfe=1.01075269 mean_mae=0.27956989 mean_displacement=0.02150538
";

/// The `against` spec on [`tape`]: the same decision, now short (D3), so
/// with k = 1 the long's target 60 014 + 93 = 60 107 is the short's stop
/// and 59 921 its target, and R is 93 again.
///
/// - *Entry:* e = 60 016; E′ = 60 016 × 0.9999 = 60 009.9984; fee =
///   30.0049992; displacement = −(60 016 − 60 014) / 93 → −0.02150538
///   (favourable).
/// - *Exit:* the touch at 60 107 is the stop; x = 60 110; X′ = 60 110 ×
///   1.0001 = 60 116.011; fee = 30.0580055.
/// - *Funding:* σ · 0.0001 · 59 990 = −5.999: received, the long's
///   amount with the opposite sign.
/// - *gross* = −94 / 93 → −1.01075269; *net* = (−(60 116.011 −
///   60 009.9984) − 30.0049992 − 30.0580055 + 5.999) / 93 = −160.0766047
///   / 93 = −1.7212538139… → −1.72125381.
/// - *MFE* = (60 016 − 59 990) / 93 → 0.27956989; *MAE* = (60 110 −
///   60 016) / 93 → 1.01075269.
/// - *Metrics:* one losing stop exit: Σ positive net 0, so `profit_factor`
///   = 0 / 1.72125381 = 0; `win_rate` 0; the cumulative net falls from its
///   peak 0 to −1.72125381, so `max_drawdown` 1.72125381; no target exit,
///   so `time_to_target=-`.
const AGAINST_LINES: &str = "\
triggers 1
filtered 0
skipped overlap=0 stale_reference=0 no_side=0 no_risk=0 stop_side=0
reach-backs 0
trade 68401000 trade 85 side=short regime=- reference=60014.00000000 stop=60107.00000000 \
target=59921.00000000 risk=93.00000000 entry=68402000:60016.00000000 exit=stop \
exit_at=68404000 fill=68405000:60110.00000000 funding=-5.99900000 gross=-1.01075269 \
net=-1.72125381 displacement=-0.02150538 mfe=0.27956989 mae=1.01075269 censor=-
metrics all trades=1 censored=0 unfilled=0 expectancy=-1.72125381 mean_gross=-1.01075269 \
profit_factor=0.00000000 win_rate=0.00000000 max_drawdown=1.72125381 time_to_target=- \
mean_mfe=0.27956989 mean_mae=1.01075269 mean_displacement=-0.02150538
EMPTY_BUCKETS\
metrics volatility.regime.1h@1 - trades=1 censored=0 unfilled=0 expectancy=-1.72125381 \
mean_gross=-1.01075269 profit_factor=0.00000000 win_rate=0.00000000 \
max_drawdown=1.72125381 time_to_target=- mean_mfe=0.27956989 mean_mae=1.01075269 \
mean_displacement=-0.02150538
";

/// The four labelled buckets of `volatility.regime.1h@1`, empty on every
/// tape here: no trade, so every mean is `-` and the drawdown 0 (D11).
const EMPTY_BUCKETS: &str = "\
metrics volatility.regime.1h@1 LOW trades=0 censored=0 unfilled=0 expectancy=- mean_gross=- \
profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- mean_mfe=- mean_mae=- \
mean_displacement=-
metrics volatility.regime.1h@1 MEDIUM trades=0 censored=0 unfilled=0 expectancy=- \
mean_gross=- profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- mean_mfe=- \
mean_mae=- mean_displacement=-
metrics volatility.regime.1h@1 HIGH trades=0 censored=0 unfilled=0 expectancy=- mean_gross=- \
profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- mean_mfe=- mean_mae=- \
mean_displacement=-
metrics volatility.regime.1h@1 EXTREME trades=0 censored=0 unfilled=0 expectancy=- \
mean_gross=- profit_factor=- win_rate=- max_drawdown=0.00000000 time_to_target=- mean_mfe=- \
mean_mae=- mean_displacement=-
";

/// The expected result text: `@2`'s text of the same spec and tape with
/// the backtest's pipeline and outcome lines, and `lines` before the spec.
/// The twelve provenance lines are `@2`'s (ADR-060 D12), and test
/// `the_provenance_is_the_replay_summary_v2s` pins that identity.
fn expected(v2_text: &str, lines: &str) -> String {
    let lines = lines.replace("EMPTY_BUCKETS", EMPTY_BUCKETS);
    let text = v2_text.replacen(
        "pipeline research.replay_summary@2\noutcome history-replay-summary\n",
        "pipeline research.backtest@1\noutcome backtest\n",
        1,
    );
    let (provenance, spec) = text.split_once("\nspec\n").unwrap();
    format!("{provenance}\n{lines}spec\n{spec}")
}

#[test]
fn the_hand_computed_trades_and_metrics_are_recorded() {
    // The pattern fires `trigger.sfp.15m@1` exactly once, at the decision,
    // and the prefix none.
    let mut engine = MarketStateEngine::new();
    let mut fired = Vec::new();
    for event in &tape().events {
        engine.apply(event).unwrap();
        for fact in engine.trigger_events() {
            if fact.feature == catalog::TRIGGER_SFP_15M_V1.key {
                fired.push((event.canonical_key(), fact.known_at));
            }
        }
    }
    let decision = tape()
        .events
        .iter()
        .find(|event| event.time().as_millis() == DECISION)
        .unwrap()
        .canonical_key();
    assert_eq!(
        fired,
        [(decision, EventTime::from_millis(DECISION))],
        "one SFP, at the decision"
    );
    assert_eq!(
        engine.state().atr.ready(),
        Some(&usdt(93)),
        "ATR(1h) as derived"
    );
    assert!(
        engine.state().regime.ready().is_none(),
        "the regime is unready"
    );

    for (variant, lines) in [(WITH, WITH_LINES), (AGAINST, AGAINST_LINES)] {
        let spec = spec(variant);
        let text = run_text(tape().events, Vec::new(), &spec);
        let (_, v2) = v2_summary(tape().events, &spec);
        assert_eq!(text, expected(&v2, lines), "{}", variant.side);
        // The decision key is the SFP fact's event key.
        assert!(text.contains(&format!(
            "\ntrade {} trade {} ",
            decision.time.as_millis(),
            decision.seq
        )));
    }
    // One ends at the target, the other at the stop of the same touch, and
    // their funding signs are opposite.
    let long = run_text(tape().events, Vec::new(), &spec(WITH));
    let short = run_text(tape().events, Vec::new(), &spec(AGAINST));
    assert_eq!(
        (field(&long, "exit"), field(&short, "exit")),
        ("target", "stop")
    );
    assert_eq!(field(&long, "exit_at"), field(&short, "exit_at"));
    assert_eq!(
        units(field(&long, "funding")),
        -units(field(&short, "funding"))
    );
}

#[test]
fn cost_variants_order_their_results_as_s2_predicts() {
    let base_spec = spec(WITH);
    let taker = spec(Variant {
        taker: "0.0010",
        ..WITH
    });
    let slippage = spec(Variant {
        slippage: "0.0002",
        ..WITH
    });
    // The path rises monotonically from the decision to the trade 1.5 s
    // after it, so the later entry fill is the dearer one.
    let latency = spec(Variant {
        latency_ms: 1_500,
        ..WITH
    });
    let specs = [&base_spec, &taker, &slippage, &latency];
    let mut service = service(tape().events);
    let texts: Vec<String> = specs
        .iter()
        .map(|spec| service.run(spec).unwrap().result().canonical_text())
        .collect();
    let [base, taker, slippage, latency] = texts.as_slice() else {
        unreachable!();
    };

    // Higher fees and slippage cost net R, and move nothing else.
    for higher in [taker, slippage] {
        assert!(
            units(field(higher, "net")) < units(field(base, "net")),
            "{higher}"
        );
        for name in [
            "reference",
            "stop",
            "target",
            "risk",
            "entry",
            "gross",
            "exit",
            "exit_at",
            "fill",
        ] {
            assert_eq!(field(higher, name), field(base, name), "{name}");
        }
    }
    // Latency fills the entry later and higher: the displacement grows.
    // This pins the fixture, not a theorem (S2).
    assert_eq!(field(base, "entry"), "68402000:60016.00000000");
    assert_eq!(field(latency, "entry"), "68402500:60020.00000000");
    assert!(units(field(latency, "displacement")) > units(field(base, "displacement")));
    assert_eq!(field(latency, "stop"), field(base, "stop"));

    // Four distinct experiments, all recorded in one store.
    let mut ids: Vec<_> = specs.iter().map(|spec| spec.id()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 4);
    assert_eq!(service.store().appends, 4);
    assert_eq!(service.store().results.len(), 4);
}

#[test]
fn a_rerun_reproduces_and_another_result_under_the_key_diverges() {
    let spec = spec(WITH);
    let mut service = service(tape().events);
    let first = service.run(&spec).unwrap();
    let ExperimentRun::Recorded(recorded) = &first else {
        panic!("{first:?}");
    };
    assert_eq!(recorded.pipeline, BACKTEST_V1);
    let key = recorded.key();
    let stored = service.store().results[&key].clone();
    assert_eq!(stored, recorded.canonical_text());

    let second = service.run(&spec).unwrap();
    assert_eq!(second, ExperimentRun::Reproduced(recorded.clone()));
    assert_eq!(second.result().canonical_text(), stored);
    assert_eq!(service.store().appends, 1);

    // The exit fill trade altered under the same data version: another
    // result under the key.
    let (_, store) = service.into_parts();
    let mut altered = tape();
    let MarketEvent::Trade(exit) = altered.events.last_mut().unwrap() else {
        unreachable!();
    };
    exit.price = usdt(60_111);
    let mut service = BacktestService::new(
        engine(Recorded(altered.events), Vec::new()),
        store,
        covered(),
    );
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::Diverged { key: key.clone() })
    );
    assert_eq!(service.store().results[&key], stored);
    assert_eq!(service.store().appends, 1);
}

/// `trade.last_price@2`: registered, but not computed by the engine.
static TRADE_LAST_PRICE_V2: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("trade.last_price", 2),
    params: &[],
    inputs: &[Input::Stream(Stream::Trades)],
    warm_up: WarmUp::Samples(2),
};

/// A service over [`Unopenable`]: reaching the engine surfaces its
/// provider error, so any other error proves a check ran before anything
/// was opened.
fn unopenable(funding: FundingCoverage) -> BacktestService<Engine<Unopenable>, MemoryStore> {
    BacktestService::new(
        engine(Unopenable, Vec::new()),
        MemoryStore::default(),
        funding,
    )
}

fn not_opened() -> ResearchError {
    ResearchError::Provider(ProviderError::Source("the replay must not open".to_owned()))
}

#[test]
fn the_checks_before_any_drive_run_in_order_and_open_nothing() {
    // An unresolvable spec: every resolution error, in `resolve`'s order.
    let registry = catalog::registry();
    let unresolvable_text = spec_text(WITH, &registry, catalog::TRADE_LAST_PRICE_V1.key)
        .replace(
            "location location.any@1\n",
            "location location.at_level@1 level=text:vah\n",
        )
        .replace(" r=rate:1.00000000", "");
    let unresolvable = ExperimentSpec::parse(&unresolvable_text, &registry).unwrap();
    let errors = resolve(&unresolvable).unwrap_err();
    assert_eq!(errors.len(), 2, "{errors:?}");
    let mut service = unopenable(covered());
    assert_eq!(
        service.run(&unresolvable),
        Err(ResearchError::Unresolved(errors))
    );

    // Funding not delivered: no `fundingRate` in the archive, or a sample
    // that ends after the cutover.
    let cutover = EventTime::from_millis(END - 1);
    let refused = [
        (FundingCoverage { until: None }, None),
        (
            FundingCoverage {
                until: Some(cutover),
            },
            Some(cutover),
        ),
    ];
    for (funding, until) in refused {
        assert_eq!(
            unopenable(funding).run(&spec(WITH)),
            Err(ResearchError::FundingNotDelivered {
                sample_end: EventTime::from_millis(END),
                until,
            })
        );
    }
    // A sample that ends at the cutover is covered (≤), and `funding.none@1`
    // needs no coverage: both reach the engine.
    let at_cutover = FundingCoverage {
        until: Some(EventTime::from_millis(END)),
    };
    assert_eq!(unopenable(at_cutover).run(&spec(WITH)), Err(not_opened()));
    let none = spec(Variant {
        funding: "funding.none@1",
        ..WITH
    });
    assert_eq!(
        unopenable(FundingCoverage { until: None }).run(&none),
        Err(not_opened())
    );

    // The feature check comes first, even before resolution.
    let mut definitions = catalog::DEFINITIONS.to_vec();
    definitions.push(&TRADE_LAST_PRICE_V2);
    let older = FeatureRegistry::new(&definitions).unwrap();
    let text = spec_text(WITH, &older, TRADE_LAST_PRICE_V2.key).replace(
        "location location.any@1\n",
        "location location.at_level@1 level=text:vah\n",
    );
    let both = ExperimentSpec::parse(&text, &older).unwrap();
    assert!(resolve(&both).is_err());
    assert_eq!(
        unopenable(FundingCoverage { until: None }).run(&both),
        Err(ResearchError::FeatureUnavailable {
            feature: TRADE_LAST_PRICE_V2.key
        })
    );
    assert!(service.store().results.is_empty());
}

/// A tape with an uncrossable hole inside the open position (ADR-031 D11):
/// the entry fill trade opens the 1m bar `[68 400 000, 68 460 000)`, and a
/// trade at the bound after its end restarts the engine.
fn restart_tape() -> (Tape, i64) {
    let mut tape = base();
    tape.trade(DECISION + 1_000, 60_016);
    let at = 68_460_000 + i64::try_from(MAX_BARS_PER_EVENT).unwrap() * MINUTE;
    tape.trade(at, 60_030).trade(at + 1_000, 60_031);
    (tape, at)
}

#[test]
fn the_provenance_is_the_replay_summary_v2s() {
    let (restart, at) = restart_tape();
    let cases = [
        (tape().events, spec(WITH)),
        (
            restart.events,
            spec(Variant {
                sample: (T0, at + HOUR),
                ..WITH
            }),
        ),
    ];
    for (events, spec) in cases {
        let mut service = service(events.clone());
        let run = service.run(&spec).unwrap();
        let Outcome::Backtest { summary, .. } = &run.result().outcome else {
            panic!("{run:?}");
        };
        let (v2, _) = v2_summary(events, &spec);
        assert_eq!(summary, &v2);
        assert_eq!(run.result().key().engine, Some(v2.engine));
    }
}

#[test]
fn restarts_rejections_and_the_prefix_reach_the_simulator() {
    // (a) An uncrossable hole inside the position: a chain restart censors
    // it at the last trade before the hole, and moves the effective origin.
    let (restart, at) = restart_tape();
    let spec_a = spec(Variant {
        sample: (T0, at + HOUR),
        ..WITH
    });
    let text = run_text(restart.events, Vec::new(), &spec_a);
    assert_eq!(field(&text, "censor"), "chain_restart");
    assert_eq!(field(&text, "exit_at"), (DECISION + 1_000).to_string());
    assert_eq!(line(&text, "effective-origin"), at.to_string());
    assert_eq!(line(&text, "rejections"), "0");

    // (b) An out-of-order trade inside the position: the engine rejects it,
    // and it censors at its own time (ADR-055 D6's min rule).
    let mut rejected = base();
    rejected
        .trade(DECISION + 1_000, 60_016)
        .trade(DECISION + 500, 60_015)
        .trade(DECISION + 2_000, 60_017);
    let text = run_text(rejected.events, Vec::new(), &spec(WITH));
    assert_eq!(field(&text, "censor"), "rejected");
    assert_eq!(field(&text, "exit_at"), (DECISION + 500).to_string());
    assert_eq!(line(&text, "rejections"), "1");

    // (c) A sample that starts after the SFP: its decision is in the
    // prefix, which is never evaluated.
    let late = spec(Variant {
        sample: (DECISION + 1, END + HOUR),
        ..WITH
    });
    let text = run_text(tape().events, Vec::new(), &late);
    assert_eq!(line(&text, "triggers"), "0");
    assert!(!text.contains("\ntrade "), "{text}");
}

fn gap(stream: Stream, start: i64, end: i64) -> FeedGap {
    FeedGap {
        stream,
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
        reason: GapReason::MissingData,
    }
}

#[test]
fn the_trailing_gaps_end_the_open_position() {
    // The position is still open when the data ends: entry fill, a dip,
    // no touch.
    let mut open = base();
    open.trade(DECISION + 1_000, 60_016)
        .trade(DECISION + 2_000, 59_990);
    let last = DECISION + 2_000;
    let none = spec(Variant {
        funding: "funding.none@1",
        ..WITH
    });

    let plain = run_text(open.events.clone(), Vec::new(), &spec(WITH));
    assert_eq!(field(&plain, "censor"), "end_of_data");
    assert_eq!(field(&plain, "exit_at"), last.to_string());

    // A trailing trades gap from the last trade: `gap`, at its start.
    let trades = gap(Stream::Trades, last, END);
    let text = run_text(open.events.clone(), vec![trades], &spec(WITH));
    assert_eq!(field(&text, "censor"), "gap");
    assert_eq!(field(&text, "exit_at"), last.to_string());

    // A trailing funding gap that meets the booking span: `funding_gap`
    // under recorded funding, nothing under `funding.none@1`.
    let funding = gap(Stream::Funding, DECISION + 1_500, END);
    let text = run_text(open.events.clone(), vec![funding], &spec(WITH));
    assert_eq!(field(&text, "censor"), "funding_gap");
    assert_eq!(field(&text, "exit_at"), (DECISION + 1_500).to_string());
    let text = run_text(open.events.clone(), vec![funding], &none);
    assert_eq!(field(&text, "censor"), "end_of_data");

    // A trailing trades gap that starts after the known trades breaks the
    // provider contract: nothing is stored.
    let late = gap(Stream::Trades, last + 1, END);
    let mut service = BacktestService::new(
        engine(Recorded(open.events), vec![late]),
        MemoryStore::default(),
        covered(),
    );
    let error = service.run(&spec(WITH)).unwrap_err();
    assert!(
        matches!(&error, ResearchError::Provider(ProviderError::Contract(detail))
            if detail.contains("after the trades known")),
        "{error}"
    );
    assert!(service.store().results.is_empty());
    assert_eq!(service.store().appends, 0);
}
