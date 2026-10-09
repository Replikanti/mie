//! The research pipeline (ADR-040): a spec runs through the domain, its
//! result is recorded once, a re-run on the same data reproduces it
//! exactly, and a stored result is never replaced. The in-memory providers
//! and store stand in for the raw-store replay and the file adapter.

use mie_app::{ExperimentService, REPLAY_SUMMARY_V1};
use mie_domain::event::{Aggressor, MarketEvent, Stream, Trade};
use mie_domain::feature::{
    FeatureDefinition, FeatureKey, FeatureRegistry, FeatureSet, Input, WarmUp, catalog,
};
use mie_domain::num::{Price, Qty};
use mie_domain::research::{
    DataVersion, ExperimentResult, ExperimentSpec, HypothesisId, Outcome, ResultKey,
};
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};
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
    } = recorded.outcome;
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
