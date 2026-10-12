//! The research pipeline (ADR-040): a spec runs through the domain, its
//! result is recorded once, a re-run on the same data reproduces it
//! exactly, and a stored result is never replaced. The in-memory providers
//! and store stand in for the raw-store replay and the file adapter.
//!
//! `research.replay_summary@2` (ADR-052 D10) runs on an in-memory stand-in
//! for the history chain: `mie-app` may not depend on an adapter
//! (ADR-025), so the CLI's offline test pins the real chain.

use mie_app::{
    ChainBounds, ExperimentService, REPLAY_SUMMARY_V1, REPLAY_SUMMARY_V2, ReplayService,
    drive_chain,
};
use mie_domain::bars::MAX_BARS_PER_EVENT;
use mie_domain::event::{Aggressor, MarketEvent, Stream, Trade};
use mie_domain::feature::{
    FeatureDefinition, FeatureKey, FeatureRegistry, FeatureSet, FeatureSetVersion, Input, WarmUp,
    catalog,
};
use mie_domain::fingerprint::Fingerprint;
use mie_domain::num::{Price, Qty};
use mie_domain::research::{
    DataVersion, EngineKey, ExperimentResult, ExperimentSpec, HistoryReplaySummary, HypothesisId,
    Outcome, ResultKey,
};
use mie_domain::state::MarketStateEngine;
use mie_domain::state_hash::STATE_HASH_ENCODING;
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ReplayMarket, ResearchError, RunResearchExperiment};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
    ResearchResultReader, ResearchResultStore, ResultStoreError,
};
use mie_ports::raw::DatasetVersion;
use std::collections::{BTreeMap, VecDeque};

/// Yields queued events in order.
struct Feed(VecDeque<MarketEvent>);

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

/// Recorded events under the fixed dataset version `0f…0f`.
struct Recorded(Vec<MarketEvent>);

impl HistoricalDataProvider for Recorded {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        let in_window = self.0.iter().filter(|e| window.contains(e.time()));
        Ok(Replay {
            stream: Feed(in_window.cloned().collect()),
            dataset: DatasetVersion::from_hex(&"0f".repeat(32)).unwrap(),
        })
    }
}

/// An append-only store in memory, holding result text like the file
/// adapter. `appends` counts successful appends.
#[derive(Default)]
struct MemoryStore {
    results: BTreeMap<ResultKey, String>,
    appends: usize,
}

impl ResearchResultReader for MemoryStore {
    fn get(&self, key: &ResultKey) -> Result<Option<ExperimentResult>, ResultStoreError> {
        self.results
            .get(key)
            .map(|text| {
                ExperimentResult::parse(text, &catalog::registry())
                    .map_err(|e| ResultStoreError::Corrupt(e.to_string()))
            })
            .transpose()
    }

    fn by_hypothesis(&self, hypothesis: &HypothesisId) -> Result<Vec<ResultKey>, ResultStoreError> {
        let mut keys = Vec::new();
        for key in self.results.keys() {
            if self
                .get(key)?
                .is_some_and(|r| r.spec.hypothesis() == hypothesis)
            {
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }
}

impl ResearchResultStore for MemoryStore {
    fn append(&mut self, result: &ExperimentResult) -> Result<ResultKey, ResultStoreError> {
        let key = result.key();
        let text = result.canonical_text();
        if let Some(stored) = self.results.get(&key) {
            return Err(ResultStoreError::AlreadyStored {
                identical: *stored == text,
                key,
            });
        }
        self.results.insert(key.clone(), text);
        self.appends += 1;
        Ok(key)
    }
}

fn trade(millis: i64, trade_id: u64, price_units: i64) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: EventTime::from_millis(millis),
        trade_id,
        price: Price::from_units(price_units),
        qty: Qty::from_units(1_000_000),
        aggressor: Aggressor::Buy,
    })
}

fn tape() -> Vec<MarketEvent> {
    vec![
        trade(1_000, 1, 6_354_200_000_000),
        trade(1_250, 2, 6_354_210_000_000),
        // Out of order: the domain rejects it, the run counts it.
        trade(1_100, 3, 6_354_190_000_000),
        trade(2_000, 4, 6_354_180_000_000),
    ]
}

/// A spec over `features` (comma-separated, or `none`) with the given
/// sample and data version.
fn spec_with(
    registry: &FeatureRegistry,
    features: &str,
    sample: (i64, i64),
    data: &str,
) -> ExperimentSpec {
    let keys: Vec<FeatureKey> = if features == "none" {
        Vec::new()
    } else {
        features
            .split(',')
            .map(|text| registry.resolve(text).unwrap().key)
            .collect()
    };
    let version = FeatureSet::new(registry, &keys).unwrap().version();
    let text = format!(
        "mie-experiment 1\n\
         hypothesis vah.failed_auction.short\n\
         sample {} {}\n\
         data {data}\n\
         features {version} {features}\n\
         state-filter none\n\
         regime-filter none\n\
         location location.at_level@1 level=text:vah\n\
         trigger trigger.failed_auction@1\n\
         entry entry.next_trade@1\n\
         invalidation invalidation.beyond_extreme@1\n\
         target target.level@1 level=text:poc\n\
         fees maker=0.0002 taker=0.0005\n\
         slippage slippage.fixed@1 rate=rate:0.0001\n\
         funding funding.recorded@1\n\
         latency 250\n",
        sample.0, sample.1
    );
    ExperimentSpec::parse(&text, registry).unwrap()
}

fn spec(sample: (i64, i64)) -> ExperimentSpec {
    spec_with(
        &catalog::registry(),
        "trade.last_price@1,volatility.atr.1h@1,bars.time.1h@1",
        sample,
        &"0f".repeat(32),
    )
}

#[test]
fn a_rerun_reproduces_the_stored_result_exactly() {
    let spec = spec((0, 10_000));
    let mut service = ExperimentService::new(Recorded(tape()), MemoryStore::default());

    let first = service.run(&spec).unwrap();
    let ExperimentRun::Recorded(recorded) = &first else {
        panic!("{first:?}");
    };
    assert_eq!(recorded.pipeline, REPLAY_SUMMARY_V1);
    let Outcome::ReplaySummary {
        stream,
        domain_rejections,
    } = recorded.outcome
    else {
        panic!("{:?}", recorded.outcome);
    };
    assert_eq!(stream.events, 4);
    assert_eq!(domain_rejections, 1);
    let key = recorded.key();
    let stored_text = service.store().results[&key].clone();
    assert_eq!(stored_text, recorded.canonical_text());

    let second = service.run(&spec).unwrap();
    assert_eq!(second, ExperimentRun::Reproduced(recorded.clone()));
    assert_eq!(service.store().results.len(), 1);
    assert_eq!(service.store().appends, 1);
    assert_eq!(service.store().results[&key], stored_text);
    assert_eq!(
        service.store().by_hypothesis(spec.hypothesis()).unwrap(),
        [key]
    );
}

#[test]
fn another_data_version_is_refused_before_anything_is_stored() {
    let spec = spec_with(
        &catalog::registry(),
        "trade.last_price@1",
        (0, 10_000),
        &"0e".repeat(32),
    );
    let mut service = ExperimentService::new(Recorded(tape()), MemoryStore::default());
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::DataVersionMismatch {
            declared: DataVersion::from_hex(&"0e".repeat(32)).unwrap(),
            opened: DataVersion::from_hex(&"0f".repeat(32)).unwrap(),
        })
    );
    assert!(service.store().results.is_empty());
}

/// Refuses to open: proves a check runs before the replay.
struct Unopenable;

impl HistoricalDataProvider for Unopenable {
    type Stream = Feed;

    fn replay(&self, _: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        Err(ProviderError::Source("the replay must not open".to_owned()))
    }
}

/// `trade.last_price@2`: registered, but not computed by the engine.
static TRADE_LAST_PRICE_V2: FeatureDefinition = FeatureDefinition {
    key: FeatureKey::new("trade.last_price", 2),
    params: &[],
    inputs: &[Input::Stream(Stream::Trades)],
    warm_up: WarmUp::Samples(2),
};

#[test]
fn a_feature_the_engine_does_not_compute_is_refused_before_the_replay() {
    let mut definitions = catalog::DEFINITIONS.to_vec();
    definitions.push(&TRADE_LAST_PRICE_V2);
    let registry = FeatureRegistry::new(&definitions).unwrap();
    let spec = spec_with(
        &registry,
        "trade.last_price@2",
        (0, 10_000),
        &"0f".repeat(32),
    );
    let mut service = ExperimentService::new(Unopenable, MemoryStore::default());
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::FeatureUnavailable {
            feature: TRADE_LAST_PRICE_V2.key
        })
    );
    assert!(service.store().results.is_empty());
}

#[test]
fn a_spec_on_a_selectable_older_version_runs_and_reproduces() {
    // A spec recorded on the default set before #96: `location.levels@1`,
    // without `profile.volume.composite_7d@1` (ADR-044 D10, ADR-040 D4).
    let keys: Vec<FeatureKey> = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key])
        .unwrap()
        .definitions()
        .map(|definition| definition.key)
        .filter(|key| *key != catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key)
        .collect();
    let features = FeatureSet::new(&catalog::registry(), &keys)
        .unwrap()
        .to_string();
    let spec = spec_with(
        &catalog::registry(),
        &features,
        (0, 10_000),
        &"0f".repeat(32),
    );
    let mut service = ExperimentService::new(Recorded(tape()), MemoryStore::default());
    let ExperimentRun::Recorded(recorded) = service.run(&spec).unwrap() else {
        panic!("the first run records");
    };
    assert_eq!(service.run(&spec), Ok(ExperimentRun::Reproduced(recorded)));
    assert_eq!(service.store().appends, 1);
}

#[test]
fn other_events_under_the_same_data_version_diverge_and_the_stored_result_stays() {
    let spec = spec((0, 10_000));
    let mut service = ExperimentService::new(Recorded(tape()), MemoryStore::default());
    let ExperimentRun::Recorded(recorded) = service.run(&spec).unwrap() else {
        panic!("the first run records");
    };
    let (_, store) = service.into_parts();
    let stored = store.results.clone();

    let mut altered = tape();
    altered.pop();
    let mut service = ExperimentService::new(Recorded(altered), store);
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::Diverged {
            key: recorded.key()
        })
    );
    assert_eq!(service.store().results, stored);
    assert_eq!(service.store().appends, 1);
}

#[test]
fn an_empty_sample_is_refused() {
    let spec = spec((5_000, 10_000));
    let mut service = ExperimentService::new(Recorded(tape()), MemoryStore::default());
    assert_eq!(service.run(&spec), Err(ResearchError::EmptySample));
    assert!(service.store().results.is_empty());
}

#[test]
fn a_provider_failure_is_reported() {
    let spec = spec((0, 10_000));
    let mut service = ExperimentService::new(Unopenable, MemoryStore::default());
    let error = service.run(&spec).unwrap_err();
    assert_eq!(
        error.to_string(),
        "market data source failed: the replay must not open"
    );
}

// ---------------------------------------------------------------------------
// `research.replay_summary@2` on the history chain (ADR-052 D10).

/// The origin of the stand-in chain.
const ORIGIN: i64 = 0;

fn bounds() -> ChainBounds {
    ChainBounds {
        origin: EventTime::from_millis(ORIGIN),
        cutover: EventTime::from_millis(1_500),
    }
}

fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

/// Two prefix trades before 2 000 ms, three sample trades after.
fn chain_tape() -> Vec<MarketEvent> {
    vec![
        trade(1_000, 1, 6_354_200_000_000),
        trade(1_250, 2, 6_354_210_000_000),
        trade(2_000, 3, 6_354_190_000_000),
        trade(3_000, 4, 6_354_180_000_000),
        trade(4_000, 5, 6_354_230_000_000),
    ]
}

/// The history summary of a run's result.
fn summary(run: &ExperimentRun) -> &HistoryReplaySummary {
    match &run.result().outcome {
        Outcome::HistoryReplaySummary(summary) => summary,
        other => panic!("{other:?}"),
    }
}

/// The engine key of this build for a spec without `catalog::SELECTABLE`
/// members.
fn this_engine() -> EngineKey {
    EngineKey {
        features: MarketStateEngine::new().feature_set().version(),
        state_encoding: STATE_HASH_ENCODING,
    }
}

/// The `@1` outcome of `spec` over `events`.
fn v1_outcome(events: Vec<MarketEvent>, spec: &ExperimentSpec) -> Outcome {
    let mut service = ExperimentService::new(Recorded(events), MemoryStore::default());
    service.run(spec).unwrap().result().outcome.clone()
}

#[test]
fn a_history_run_records_the_prefix_and_reproduces_byte_for_byte() {
    let spec = spec((2_000, 10_000));
    let mut service =
        ExperimentService::on_chain(Recorded(chain_tape()), MemoryStore::default(), bounds());

    let first = service.run(&spec).unwrap();
    let ExperimentRun::Recorded(recorded) = &first else {
        panic!("{first:?}");
    };
    assert_eq!(recorded.pipeline, REPLAY_SUMMARY_V2);
    let key = recorded.key();
    assert_eq!(key.engine, Some(this_engine()));
    assert_eq!(
        key.to_string(),
        format!(
            "{}/research.replay_summary@2-{}-{STATE_HASH_ENCODING}",
            spec.id(),
            this_engine().features
        )
    );
    let stored_text = service.store().results[&key].clone();
    assert_eq!(stored_text, recorded.canonical_text());

    let summary = summary(&first);
    assert_eq!(summary.origin, bounds().origin);
    assert_eq!(summary.cutover, bounds().cutover);
    assert_eq!(summary.effective_origin, None);
    assert_eq!(summary.prefix_data, Some(spec.data_version().clone()));
    // The prefix is the chain's replay of [origin, sample start).
    let prefix = ReplayService::new(Recorded(chain_tape()))
        .replay(window(ORIGIN, 2_000))
        .unwrap();
    assert_eq!(summary.prefix_stream, prefix.stream_hash);
    assert_eq!(summary.prefix_stream.events, 2);
    assert_eq!(summary.prefix_rejections, 0);
    // The sample's lines are `@1`'s over the same provider and sample.
    assert_eq!(
        v1_outcome(chain_tape(), &spec),
        Outcome::ReplaySummary {
            stream: summary.stream,
            domain_rejections: summary.rejections,
        }
    );
    assert_eq!(summary.stream.events, 3);
    // The state at the sample's end is the union's: one engine over
    // [origin, sample end).
    let mut union = MarketStateEngine::new();
    let mut stream = Recorded(chain_tape())
        .replay(window(ORIGIN, 10_000))
        .unwrap()
        .stream;
    drive_chain(&mut stream, &mut union, |e| panic!("{e}")).unwrap();
    assert_eq!(summary.state_end, union.state().state_hash());
    assert_ne!(
        summary.state_start,
        MarketStateEngine::new().state().state_hash()
    );

    let second = service.run(&spec).unwrap();
    assert_eq!(second, ExperimentRun::Reproduced(recorded.clone()));
    assert_eq!(service.store().appends, 1);
    assert_eq!(service.store().results[&key], stored_text);
}

#[test]
fn a_sample_at_the_origin_has_an_empty_prefix_and_the_v1_stream() {
    // `tape` holds an out-of-order trade: both pipelines count it.
    let spec = spec((ORIGIN, 10_000));
    let mut service =
        ExperimentService::on_chain(Recorded(tape()), MemoryStore::default(), bounds());
    let run = service.run(&spec).unwrap();
    let summary = summary(&run);
    assert_eq!(summary.prefix_data, None);
    assert_eq!(summary.prefix_stream.events, 0);
    assert_eq!(summary.prefix_rejections, 0);
    assert_eq!(
        summary.state_start,
        MarketStateEngine::new().state().state_hash()
    );
    assert_eq!(
        v1_outcome(tape(), &spec),
        Outcome::ReplaySummary {
            stream: summary.stream,
            domain_rejections: summary.rejections,
        }
    );
    assert_eq!(summary.rejections, 1);
    assert!(
        run.result()
            .canonical_text()
            .contains("\neffective-origin -\n"),
    );
}

#[test]
fn changed_prefix_events_under_the_same_data_version_diverge() {
    let spec = spec((2_000, 10_000));
    let mut service =
        ExperimentService::on_chain(Recorded(chain_tape()), MemoryStore::default(), bounds());
    let ExperimentRun::Recorded(recorded) = service.run(&spec).unwrap() else {
        panic!("the first run records");
    };
    let (_, store) = service.into_parts();
    let stored = store.results.clone();

    let mut altered = chain_tape();
    altered.remove(1);
    let mut service = ExperimentService::on_chain(Recorded(altered), store, bounds());
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::Diverged {
            key: recorded.key()
        })
    );
    assert_eq!(service.store().results, stored);
    assert_eq!(service.store().appends, 1);
}

/// Opens `window` only, under the fixed dataset version `0f…0f`: proves
/// which windows a run opens, and in which order.
struct OnlyWindow(ReplayWindow);

impl HistoricalDataProvider for OnlyWindow {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        if window != self.0 {
            return Err(ProviderError::Source(format!(
                "the replay must not open [{}, {})",
                window.start, window.end
            )));
        }
        Recorded(chain_tape()).replay(window)
    }
}

#[test]
fn a_data_mismatch_is_reported_before_the_prefix_opens() {
    let spec = spec_with(
        &catalog::registry(),
        "trade.last_price@1",
        (2_000, 10_000),
        &"0e".repeat(32),
    );
    let mut service = ExperimentService::on_chain(
        OnlyWindow(window(2_000, 10_000)),
        MemoryStore::default(),
        bounds(),
    );
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::DataVersionMismatch {
            declared: DataVersion::from_hex(&"0e".repeat(32)).unwrap(),
            opened: DataVersion::from_hex(&"0f".repeat(32)).unwrap(),
        })
    );
    assert!(service.store().results.is_empty());
}

#[test]
fn a_sample_before_the_origin_is_refused_before_any_replay() {
    let spec = spec((2_000, 10_000));
    let origin = EventTime::from_millis(2_001);
    let mut service = ExperimentService::on_chain(
        Unopenable,
        MemoryStore::default(),
        ChainBounds {
            origin,
            cutover: EventTime::from_millis(5_000),
        },
    );
    let error = service.run(&spec).unwrap_err();
    assert_eq!(
        error,
        ResearchError::SampleBeforeOrigin {
            start: EventTime::from_millis(2_000),
            origin,
        }
    );
    assert_eq!(
        error.to_string(),
        "the sample starts at 2000 ms, before the history origin 2001 ms (ADR-052 D10)"
    );
    assert!(service.store().results.is_empty());
}

#[test]
fn an_uncrossable_hole_in_the_prefix_moves_the_effective_origin() {
    // The first trade opens the 1m bar [0, 60 s); a trade at the bound
    // after that bar's end is an uncrossable hole (ADR-031 D11).
    let at = 60_000 + i64::try_from(MAX_BARS_PER_EVENT).unwrap() * 60_000;
    let events = vec![
        trade(1_000, 1, 6_354_200_000_000),
        trade(at, 2, 6_354_210_000_000),
        trade(at + 2_000, 3, 6_354_190_000_000),
        trade(at + 3_000, 4, 6_354_180_000_000),
    ];
    let spec = spec((at + 1_000, at + 10_000));
    let mut service =
        ExperimentService::on_chain(Recorded(events), MemoryStore::default(), bounds());
    let run = service.run(&spec).unwrap();
    let summary = summary(&run);
    assert_eq!(summary.effective_origin, Some(EventTime::from_millis(at)));
    assert_eq!(summary.prefix_rejections, 0);
    assert_eq!(summary.prefix_stream.events, 2);
    assert_eq!(summary.rejections, 0);
}

#[test]
fn a_stored_result_of_another_engine_leaves_the_run_recorded_under_this_one() {
    let spec = spec((2_000, 10_000));
    let ExperimentRun::Recorded(this) =
        ExperimentService::on_chain(Recorded(chain_tape()), MemoryStore::default(), bounds())
            .run(&spec)
            .unwrap()
    else {
        panic!("the first run records");
    };
    let others = [
        EngineKey {
            features: FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(0xabc)),
            ..this_engine()
        },
        EngineKey {
            state_encoding: STATE_HASH_ENCODING + 1,
            ..this_engine()
        },
    ];
    for other in others {
        let mut foreign = this.clone();
        let Outcome::HistoryReplaySummary(summary) = &mut foreign.outcome else {
            unreachable!();
        };
        summary.engine = other;
        summary.state_end = summary.state_start;
        let mut store = MemoryStore::default();
        store.append(&foreign).unwrap();
        let before = store.results.clone();

        let mut service = ExperimentService::on_chain(Recorded(chain_tape()), store, bounds());
        assert_eq!(
            service.run(&spec),
            Ok(ExperimentRun::Recorded(this.clone()))
        );
        let store = service.store();
        assert_eq!(store.results.len(), 2);
        assert_eq!(store.results[&foreign.key()], before[&foreign.key()]);
        assert_eq!(store.results[&this.key()], this.canonical_text());
        assert_eq!(store.by_hypothesis(spec.hypothesis()).unwrap(), {
            let mut keys = vec![foreign.key(), this.key()];
            keys.sort();
            keys
        });
    }
}
