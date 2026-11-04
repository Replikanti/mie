//! Shared fakes of the `mie-app` integration tests: an in-memory provider
//! and result store standing in for the raw-store replay and the file
//! adapter, which `mie-app` may not depend on (ADR-025).

#![allow(dead_code)]

use mie_domain::event::MarketEvent;
use mie_domain::feature::catalog;
use mie_domain::research::{ExperimentResult, HypothesisId, ResultKey};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
    ResearchResultReader, ResearchResultStore, ResultStoreError,
};
use mie_ports::raw::DatasetVersion;
use std::collections::{BTreeMap, VecDeque};

/// Yields queued events in order.
pub struct Feed(pub VecDeque<MarketEvent>);

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

/// Recorded events under the fixed dataset version `0f…0f`.
pub struct Recorded(pub Vec<MarketEvent>);

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
pub struct MemoryStore {
    pub results: BTreeMap<ResultKey, String>,
    pub appends: usize,
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
        // The port contract: an invalid result is refused before writing.
        result.validate().map_err(ResultStoreError::Invalid)?;
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

/// Refuses to open: proves a check runs before the replay.
pub struct Unopenable;

impl HistoricalDataProvider for Unopenable {
    type Stream = Feed;

    fn replay(&self, _: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        Err(ProviderError::Source("the replay must not open".to_owned()))
    }
}
