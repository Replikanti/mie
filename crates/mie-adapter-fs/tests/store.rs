//! The append-only result store (ADR-040) on a real filesystem: layout,
//! read-only files, no overwrite, concurrent writers, verified reads.

use mie_adapter_fs::FsResultStore;
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::FeatureSetVersion;
use mie_domain::feature::{FeatureSet, catalog};
use mie_domain::fingerprint::Fingerprint;
use mie_domain::research::{
    DataVersion, EngineKey, ExperimentResult, ExperimentSpec, HistoryReplaySummary, HypothesisId,
    Outcome, PipelineKey, ResultKey,
};
use mie_domain::state_hash::StateHash;
use mie_domain::time::EventTime;
use mie_ports::outbound::{ResearchResultReader, ResearchResultStore, ResultStoreError};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;

/// A fresh directory under the system temp dir, removed on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mie-adapter-fs-{tag}-{}-{n}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

const REPLAY_SUMMARY: PipelineKey = PipelineKey::new("research.replay_summary", 1);

fn spec(hypothesis: &str, latency: u32) -> ExperimentSpec {
    let registry = catalog::registry();
    let key = registry.resolve("trade.last_price@1").unwrap().key;
    let version = FeatureSet::new(&registry, &[key]).unwrap().version();
    let text = format!(
        "mie-experiment 1\n\
         hypothesis {hypothesis}\n\
         sample 0 10000\n\
         data {}\n\
         features {version} trade.last_price@1\n\
         state-filter none\n\
         regime-filter none\n\
         location location.at_level@1 level=text:vah\n\
         trigger trigger.failed_auction@1\n\
         entry entry.next_trade@1\n\
         invalidation invalidation.beyond_extreme@1\n\
         target target.level@1 level=text:poc\n\
         fees maker=0.0002 taker=0.0005\n\
         slippage slippage.fixed@1\n\
         funding funding.recorded@1\n\
         latency {latency}\n",
        "0f".repeat(32)
    );
    ExperimentSpec::parse(&text, &registry).unwrap()
}

fn result_of(spec: ExperimentSpec, pipeline: PipelineKey, events: u64) -> ExperimentResult {
    ExperimentResult {
        spec,
        pipeline,
        outcome: Outcome::ReplaySummary {
            stream: EventStreamHash {
                events,
                fingerprint: Fingerprint::from_raw(0xfeed),
            },
            domain_rejections: 0,
        },
    }
}

fn result(events: u64) -> ExperimentResult {
    result_of(
        spec("vah.failed_auction.short", 250),
        REPLAY_SUMMARY,
        events,
    )
}

const HISTORY_SUMMARY: PipelineKey = PipelineKey::new("research.replay_summary", 2);

fn engine(features: u64, state_encoding: u32) -> EngineKey {
    EngineKey {
        features: FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(features)),
        state_encoding,
    }
}

/// A `research.replay_summary@2` result computed by `engine`.
fn history_result(engine: EngineKey) -> ExperimentResult {
    let hash = |raw| StateHash::from_fingerprint(Fingerprint::from_raw(raw));
    ExperimentResult {
        spec: spec("vah.failed_auction.short", 250),
        pipeline: HISTORY_SUMMARY,
        outcome: Outcome::HistoryReplaySummary(HistoryReplaySummary {
            origin: EventTime::from_millis(0),
            effective_origin: None,
            cutover: EventTime::from_millis(5_000),
            prefix_data: DataVersion::from_hex(&"0f".repeat(32)),
            prefix_stream: EventStreamHash {
                events: 3,
                fingerprint: Fingerprint::from_raw(0xbeef),
            },
            prefix_rejections: 0,
            engine,
            state_start: hash(0x1234),
            stream: EventStreamHash {
                events: 4,
                fingerprint: Fingerprint::from_raw(0xfeed),
            },
            rejections: 0,
            state_end: hash(0x5678),
        }),
    }
}

#[test]
fn a_history_result_is_stored_under_its_engine_key() {
    let dir = TempDir::new("engine-key");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let result = history_result(engine(0x0123_4567_89ab_cdef, 1));
    let key = store.append(&result).unwrap();
    assert_eq!(key, result.key());
    let path = store.path_of(&key);
    assert_eq!(
        path,
        dir.path()
            .join(key.experiment.to_string())
            .join("research.replay_summary@2-0123456789abcdef-1.result")
    );
    assert_eq!(store.get(&key), Ok(Some(result)));
    let without_engine = ResultKey {
        engine: None,
        ..key
    };
    assert_eq!(store.get(&without_engine), Ok(None));
}

#[test]
fn the_results_of_two_engines_store_alongside() {
    let dir = TempDir::new("two-engines");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let v1 = store.append(&result(4)).unwrap();
    let new_build = history_result(engine(0xfff, 1));
    let old_build = history_result(engine(0x00a, 1));
    let new_key = store.append(&new_build).unwrap();
    let old_key = store.append(&old_build).unwrap();
    assert_eq!(store.get(&new_key), Ok(Some(new_build)));
    assert_eq!(store.get(&old_key), Ok(Some(old_build)));
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
            .unwrap(),
        [v1, old_key, new_key]
    );
}

#[test]
fn a_history_result_under_another_engines_name_is_corrupt() {
    let dir = TempDir::new("engine-misplaced");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let key = store
        .append(&history_result(engine(0x0123_4567_89ab_cdef, 1)))
        .unwrap();
    let other = ResultKey {
        engine: Some(engine(0x0123_4567_89ab_cdef, 2)),
        ..key.clone()
    };
    fs::copy(store.path_of(&key), store.path_of(&other)).unwrap();
    let Err(ResultStoreError::Corrupt(detail)) = store.get(&other) else {
        panic!("a misplaced result is corrupt");
    };
    assert!(
        detail.ends_with(&format!("holds the result {key}, not {other}")),
        "{detail}"
    );

    // A non-canonical name is not a key: the listing ignores it.
    let dir2 = TempDir::new("engine-non-canonical");
    let mut store = FsResultStore::open(dir2.path()).unwrap();
    let key = store
        .append(&history_result(engine(0x0123_4567_89ab_cdef, 1)))
        .unwrap();
    let experiment_dir = store.path_of(&key).parent().unwrap().to_owned();
    fs::copy(
        store.path_of(&key),
        experiment_dir.join("research.replay_summary@2-0123456789ABCDEF-1.result"),
    )
    .unwrap();
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
            .unwrap(),
        [key]
    );
}

#[test]
fn appends_and_reads_back_a_read_only_file_at_the_key_path() {
    let dir = TempDir::new("round-trip");
    let mut store = FsResultStore::open(dir.path().join("results")).unwrap();
    let result = result(4);

    let key = store.append(&result).unwrap();
    assert_eq!(key, result.key());
    let path = store.path_of(&key);
    assert_eq!(
        path,
        dir.path()
            .join("results")
            .join(key.experiment.to_string())
            .join("research.replay_summary@1.result")
    );
    let bytes = fs::read(&path).unwrap();
    let text = String::from_utf8(bytes).unwrap();
    let (body, trailer) = text.rsplit_once("sha256 ").unwrap();
    assert_eq!(body, result.canonical_text());
    assert_eq!(trailer.len(), 65, "64 hex digits and a line break");
    assert!(fs::metadata(&path).unwrap().permissions().readonly());
    assert_eq!(store.get(&key), Ok(Some(result)));

    let other = ResultKey {
        experiment: key.experiment,
        pipeline: PipelineKey::new("research.backtest", 1),
        engine: None,
    };
    assert_eq!(store.get(&other), Ok(None));
}

#[test]
fn a_stored_result_is_never_overwritten() {
    let dir = TempDir::new("no-overwrite");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let first = result(4);
    let key = store.append(&first).unwrap();
    let path = store.path_of(&key);
    let bytes = fs::read(&path).unwrap();

    assert_eq!(
        store.append(&first),
        Err(ResultStoreError::AlreadyStored {
            key: key.clone(),
            identical: true
        })
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);

    let different = result(5);
    assert_eq!(
        store.append(&different),
        Err(ResultStoreError::AlreadyStored {
            key: key.clone(),
            identical: false
        })
    );
    assert_eq!(fs::read(&path).unwrap(), bytes);
    assert_eq!(store.get(&key), Ok(Some(first)));
    // No temporary file is left behind.
    let names: Vec<_> = fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(names, ["research.replay_summary@1.result"]);
}

#[test]
fn of_concurrent_writers_of_one_key_exactly_one_wins() {
    const WRITERS: u64 = 16;
    for round in 0..20 {
        let dir = TempDir::new(&format!("race-{round}"));
        let barrier = Arc::new(Barrier::new(WRITERS as usize));
        let writers: Vec<_> = (0..WRITERS)
            .map(|writer| {
                let root = dir.path().to_owned();
                let barrier = Arc::clone(&barrier);
                // Each writer offers a different outcome under the same key.
                thread::spawn(move || {
                    let mut store = FsResultStore::open(root).unwrap();
                    let result = result(writer + 1);
                    barrier.wait();
                    (writer, store.append(&result))
                })
            })
            .collect();
        let outcomes: Vec<_> = writers.into_iter().map(|w| w.join().unwrap()).collect();

        let winners: Vec<u64> = outcomes
            .iter()
            .filter_map(|(writer, outcome)| outcome.is_ok().then_some(*writer))
            .collect();
        assert_eq!(winners.len(), 1, "round {round}: {outcomes:?}");
        let key = result(1).key();
        for (writer, outcome) in &outcomes {
            if *writer != winners[0] {
                assert_eq!(
                    outcome,
                    &Err(ResultStoreError::AlreadyStored {
                        key: key.clone(),
                        identical: false
                    }),
                    "round {round}, writer {writer}"
                );
            }
        }
        let store = FsResultStore::open(dir.path()).unwrap();
        assert_eq!(store.get(&key), Ok(Some(result(winners[0] + 1))));
        // No temporary file is left behind.
        let names: Vec<_> = fs::read_dir(store.path_of(&key).parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(names, ["research.replay_summary@1.result"], "round {round}");
    }
}

#[test]
fn open_creates_a_nested_root() {
    let dir = TempDir::new("nested");
    let root = dir.path().join("a").join("b").join("results");
    let mut store = FsResultStore::open(&root).unwrap();
    assert!(root.is_dir());
    let key = store.append(&result(4)).unwrap();
    assert_eq!(
        FsResultStore::open(&root).unwrap().get(&key),
        Ok(Some(result(4)))
    );
}

#[test]
fn another_pipeline_stores_alongside() {
    let dir = TempDir::new("pipelines");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let first = store.append(&result(4)).unwrap();
    let next = result_of(
        spec("vah.failed_auction.short", 250),
        PipelineKey::new("research.backtest", 1),
        4,
    );
    let second = store.append(&next).unwrap();
    assert_eq!(first.experiment, second.experiment);
    assert_ne!(first, second);
    assert_eq!(store.get(&second), Ok(Some(next)));
    // Sorted: `research.backtest@1` before `research.replay_summary@1`.
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
            .unwrap(),
        [second, first]
    );
}

#[test]
fn a_flipped_byte_fails_the_integrity_check() {
    let dir = TempDir::new("integrity");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let key = store.append(&result(4)).unwrap();
    let path = store.path_of(&key);
    let mut bytes = fs::read(&path).unwrap();
    // `latency 250` becomes `latency 251`.
    let at = bytes.windows(11).position(|w| w == b"latency 250").unwrap() + 10;
    bytes[at] = b'1';
    replace(&path, &bytes);
    assert!(matches!(
        store.get(&key),
        Err(ResultStoreError::Integrity(_))
    ));

    let mut no_trailer = fs::read(&path).unwrap();
    no_trailer.truncate(no_trailer.len() - 10);
    replace(&path, &no_trailer);
    assert!(matches!(
        store.get(&key),
        Err(ResultStoreError::Integrity(_))
    ));
}

#[test]
fn a_file_at_another_keys_path_is_corrupt() {
    let dir = TempDir::new("misplaced");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let key = store.append(&result(4)).unwrap();
    let other = ResultKey {
        experiment: key.experiment,
        pipeline: PipelineKey::new("research.backtest", 1),
        engine: None,
    };
    fs::copy(store.path_of(&key), store.path_of(&other)).unwrap();
    let Err(ResultStoreError::Corrupt(detail)) = store.get(&other) else {
        panic!("a misplaced result is corrupt");
    };
    assert!(
        detail.ends_with(&format!("holds the result {key}, not {other}")),
        "{detail}"
    );
}

#[test]
fn leftover_temporary_files_are_ignored_and_never_block_an_append() {
    let dir = TempDir::new("leftover");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let result = result(4);
    let experiment_dir = store.path_of(&result.key()).parent().unwrap().to_owned();
    fs::create_dir_all(&experiment_dir).unwrap();
    let leftover = experiment_dir.join(format!(
        ".research.replay_summary@1.result.{}-0.tmp",
        std::process::id()
    ));
    fs::write(&leftover, b"half a result").unwrap();
    fs::write(experiment_dir.join("notes.txt"), b"not a result").unwrap();

    let key = store.append(&result).unwrap();
    assert_eq!(store.get(&key), Ok(Some(result)));
    assert_eq!(fs::read(&leftover).unwrap(), b"half a result");
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
            .unwrap(),
        [key]
    );
}

#[test]
fn listing_by_hypothesis_is_sorted_and_filtered() {
    let dir = TempDir::new("by-hypothesis");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let mut short = Vec::new();
    for latency in [100, 200, 300] {
        let result = result_of(spec("vah.failed_auction.short", latency), REPLAY_SUMMARY, 4);
        short.push(store.append(&result).unwrap());
    }
    let long = result_of(spec("val.failed_auction.long", 100), REPLAY_SUMMARY, 4);
    let long_key = store.append(&long).unwrap();
    fs::write(dir.path().join("README"), b"not an experiment").unwrap();
    short.sort();

    let listed = store
        .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
        .unwrap();
    assert_eq!(listed, short);
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("val.failed_auction.long").unwrap())
            .unwrap(),
        [long_key]
    );
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("unknown").unwrap())
            .unwrap(),
        []
    );
}

/// Replaces a read-only file's content, as corruption or an edit would.
fn replace(path: &Path, bytes: &[u8]) {
    fs::remove_file(path).unwrap();
    fs::write(path, bytes).unwrap();
}
