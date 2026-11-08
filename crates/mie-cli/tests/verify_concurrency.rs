//! AW11 (ADR-062 D11, #192): the replication pass of `mie ingest` and a
//! `mie store verify` appending to the copy ledger at the same time, in
//! threads and across processes, leave the hash chain valid on hot and on
//! every copy, every copy a byte prefix of hot's ledger, and every
//! verification passing. The deterministic interleaving (a pass between
//! the verification's snapshot and its first append) is a unit test of
//! `store::verify`, which has the hook.
//!
//! Why 50 and 20 rounds: each round takes milliseconds on this fixture, so
//! the binary stays at a few seconds in CI. The deterministic test pins the
//! interleaving; the rounds are the regression net. Fewer rounds lower the
//! chance that a nondeterministic regression shows; more only cost time.

mod common;

use common::tiers::ingest_toml;
use common::{MapProbe, TempDir};
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Truncation};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_cli::config::IngestConfig;
use mie_cli::store::{self, PassObserver};
use mie_domain::time::EventTime;
use mie_ports::raw::{RawRecord, RawRecordSink, RawStreamKey};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

const CONFIG_VAR: &str = "MIE_VERIFY_CONCURRENCY_CONFIG";
const THREAD_ROUNDS: usize = 50;
const PROCESS_ROUNDS: usize = 20;
/// A hot window that starts long before every part: nothing is a removal
/// candidate, so the planned pass only copies.
const FIRST_HOT: &str = "2000-01-01";

/// Serializes the tests of this binary: a child spawned by one test
/// inherits, between its fork and its exec, every lock descriptor open in
/// the process (#198).
static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn serial() -> std::sync::MutexGuard<'static, ()> {
    SERIAL
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The wall clock: a part sealed in this round is within G of it, as in
/// production.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

struct Ignore;

impl PassObserver for Ignore {
    fn ledger_truncated(&mut self, _: &Path, _: Truncation) {}
}

/// A tiered store with two marked copy targets and one sealed part,
/// replicated once; its configuration is written to `dir/ingest.toml`.
fn store(dir: &Path) -> IngestConfig {
    let cold = common::cold_roots(dir);
    let config = ingest_toml(
        &dir.join("ingest.toml"),
        &dir.join("raw"),
        &dir.join("journal.jsonl"),
        &cold,
    );
    common::init_tiers(&config.paths.raw_root, &cold);
    std::fs::write(&config.paths.journal, "{\"type\":\"run_start\"}\n").unwrap();
    seal(&config, 0);
    pass(&config);
    config
}

fn probe(config: &IngestConfig) -> MapProbe {
    let mut devices = vec![(config.paths.raw_root.clone(), 1)];
    for (i, root) in config.paths.cold_roots.iter().enumerate() {
        devices.push((root.path.clone(), i as u64 + 2));
    }
    MapProbe(devices)
}

/// Seals one part of `aggTrade` on hot, as the capture does between two
/// passes, and journals it.
fn seal(config: &IngestConfig, round: usize) {
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let mut writer = ParquetRawStore::tiered(&config.paths.raw_root, Vec::new())
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    let time = now_ms() + round as i64;
    writer
        .append(
            &key,
            RawRecord {
                event_time: EventTime::from_millis(time),
                capture: None,
                payload: format!("round-{round}").into_bytes(),
            },
        )
        .unwrap();
    writer.close().unwrap();
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(&config.paths.journal)
        .unwrap()
        .write_all(format!("{{\"type\":\"sealed\",\"round\":{round}}}\n").as_bytes())
        .unwrap();
}

/// The pass of the `mie ingest` thread.
fn pass(config: &IngestConfig) {
    let (report, _) = store::replicate::run_planned(
        config,
        None,
        &probe(config),
        &now_ms,
        &CopyLedger::at(&config.paths.raw_root),
        &mut Ignore,
        FIRST_HOT,
    );
    assert_eq!(report.problems, Vec::<String>::new(), "{report:?}");
}

/// One verification; its problems.
fn verify(config: &IngestConfig) -> Vec<String> {
    store::verify::run(
        config,
        None,
        "24",
        &probe(config),
        &now_ms,
        &CopyLedger::at(&config.paths.raw_root),
    )
    .problems
}

/// The chain verifies on hot and on every copy, and every copy (read
/// first) is a byte prefix of hot's ledger.
fn assert_chains(config: &IngestConfig) {
    let copies: Vec<(PathBuf, Vec<u8>)> = config
        .paths
        .cold_roots
        .iter()
        .map(|root| {
            let path = copy_ledger::ledger_path(&root.path);
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect();
    let hot = std::fs::read(copy_ledger::ledger_path(&config.paths.raw_root)).unwrap();
    let check = copy_ledger::check_chain(&hot);
    // A running append may leave a partial last line; no record fails.
    assert!(
        check.failure.is_none() || check.failure.is_some_and(|f| f.record == check.records),
        "{check:?}"
    );
    copy_ledger::read(&config.paths.raw_root).unwrap().unwrap();
    for (path, copy) in copies {
        assert_eq!(
            copy_ledger::check_chain(&copy).failure,
            None,
            "{}",
            path.display()
        );
        assert!(hot.starts_with(&copy), "{}", path.display());
    }
}

/// Threads: the `mie ingest` loop (seal a part, then the planned pass)
/// beside a verification loop.
#[test]
fn a_replicator_thread_and_a_verification_thread_keep_every_chain_valid() {
    let _serial = serial();
    let dir = TempDir::new("verify-threads");
    let config = store(dir.path());
    let replicator = {
        let config = config.clone();
        std::thread::spawn(move || {
            for round in 1..=THREAD_ROUNDS {
                seal(&config, round);
                pass(&config);
            }
        })
    };
    for _ in 0..THREAD_ROUNDS {
        assert_eq!(verify(&config), Vec::<String>::new());
        assert_chains(&config);
    }
    replicator.join().unwrap();
    pass(&config);
    assert_chains(&config);
    assert_eq!(verify(&config), Vec::<String>::new());
}

/// The child of `a_verification_in_another_process_keeps_every_chain_valid`.
#[test]
#[ignore = "run by the parent test in a child process"]
fn child_verifies_once() {
    let Some(path) = std::env::var_os(CONFIG_VAR) else {
        return;
    };
    let config = IngestConfig::load(Path::new(&path)).unwrap();
    assert_eq!(verify(&config), Vec::<String>::new());
}

/// Processes: each round, a verification in a child process (it re-execs
/// this test binary: the `mie` binary's `st_dev` probe would refuse roots
/// on one filesystem) while this process seals and replicates.
#[test]
fn a_verification_in_another_process_keeps_every_chain_valid() {
    let _serial = serial();
    let dir = TempDir::new("verify-processes");
    let config = store(dir.path());
    for round in 1..=PROCESS_ROUNDS {
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["child_verifies_once", "--exact", "--ignored", "--nocapture"])
            .env(CONFIG_VAR, dir.path().join("ingest.toml"))
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        seal(&config, round);
        pass(&config);
        let status = child.wait().unwrap();
        assert!(
            status.success(),
            "round {round}: the child's verification failed"
        );
        assert_chains(&config);
    }
    assert_eq!(verify(&config), Vec::<String>::new());
}
