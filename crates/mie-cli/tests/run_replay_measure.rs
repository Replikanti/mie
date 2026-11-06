//! The ADR-063 "Accept when" measurement over a real capture (not run in
//! CI): each cleanly ended run of a `mie ingest` capture is drained through
//! `LiveReplay::run_replay`, and the most records it held at once is checked
//! against the run's largest seal, counted from the journal's `sealed`
//! entries (ADR-063 D4, D5). It prints, per run, the records, the events,
//! the peak, that largest seal and the wall time.
//!
//! Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test run_replay_measure -- --ignored
//! --nocapture`.

use mie_adapter_binance::LiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_ports::outbound::MarketDataProvider;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Instant;

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("set {name}")))
}

/// Per run, the largest row total of one `sealed` journal entry: the run's
/// records of one seal (`RawWriter::seal_all` returns every part sealed
/// since the last call, so one entry holds every record appended between
/// two seals).
fn largest_seal(journal: &std::path::Path) -> BTreeMap<String, u64> {
    let text = std::fs::read_to_string(journal).unwrap();
    let mut largest = BTreeMap::new();
    for line in text.lines() {
        let Ok(value) = serde_json::from_str::<Value>(line) else {
            continue;
        };
        if value["type"] != "sealed" {
            continue;
        }
        let run = value["run_id"]
            .as_str()
            .expect("a sealed entry names its run");
        let rows: u64 = value["files"]
            .as_array()
            .expect("a sealed entry lists its files")
            .iter()
            .map(|file| {
                file["rows"]
                    .as_u64()
                    .expect("a sealed file counts its rows")
            })
            .sum();
        let entry = largest.entry(run.to_owned()).or_insert(0);
        *entry = rows.max(*entry);
    }
    largest
}

#[test]
#[ignore = "measures a live capture for ADR-063; see the module docs"]
fn measure_run_replay_bound() {
    let raw_root = env_path("MIE_MEASURE_RAW_ROOT");
    let journal = env_path("MIE_MEASURE_JOURNAL");
    let runs: Vec<JournaledRun> = read_runs(&journal, "binance-um").unwrap();
    let largest = largest_seal(&journal);
    let store = ParquetRawStore::new(&raw_root);
    let live = LiveReplay::new(
        &store,
        "binance-um",
        "BTCUSDT",
        runs.iter().map(JournaledRun::live_run).collect(),
    );
    let mut measured = 0;
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let started = Instant::now();
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        let mut events = 0_u64;
        while replay.next_event().unwrap().is_some() {
            events += 1;
        }
        let records = replay.stats().records;
        let peak = replay.peak_buffered_records();
        let seal = largest.get(id).copied().unwrap_or(0);
        println!(
            "run {id}: records {records}, events {events}, peak buffered records {peak}, \
             largest seal {seal} records, {:.1} s",
            started.elapsed().as_secs_f64()
        );
        assert!(
            peak <= seal,
            "run {id}: the replay held {peak} records, its largest seal holds {seal}"
        );
        measured += 1;
    }
    assert!(measured > 0, "no cleanly ended run in the journal");
}
