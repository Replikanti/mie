//! The append-only result store (ADR-040) on a real filesystem: layout,
//! read-only files, no overwrite, verified reads.

use mie_adapter_fs::FsResultStore;
use mie_domain::event_hash::EventStreamHash;
use mie_domain::feature::{FeatureSet, catalog};
use mie_domain::fingerprint::Fingerprint;
use mie_domain::research::{
    ExperimentResult, ExperimentSpec, HypothesisId, Outcome, PipelineKey, ResultKey,
};
use mie_ports::outbound::{ResearchResultReader, ResearchResultStore, ResultStoreError};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

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
fn another_pipeline_stores_alongside() {
    let dir = TempDir::new("pipelines");
    let mut store = FsResultStore::open(dir.path()).unwrap();
    let first = store.append(&result(4)).unwrap();
    let next = result_of(
        spec("vah.failed_auction.short", 250),
        PipelineKey::new("research.replay_summary", 2),
        4,
    );
    let second = store.append(&next).unwrap();
    assert_eq!(first.experiment, second.experiment);
    assert_ne!(first, second);
    assert_eq!(store.get(&second), Ok(Some(next)));
    assert_eq!(
        store
            .by_hypothesis(&HypothesisId::new("vah.failed_auction.short").unwrap())
            .unwrap(),
        [first, second]
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

/// Replaces a read-only file's content, as tampering would.
fn replace(path: &Path, bytes: &[u8]) {
    fs::remove_file(path).unwrap();
    fs::write(path, bytes).unwrap();
}
