//! The ADR-063 equivalence check over a real capture (not run in CI): the
//! event count and the `EventStreamHasher` digest of the whole stream
//! `LiveReplay::run_replay` delivers for each cleanly ended run. It uses
//! only API that the replay had before ADR-063, so the same file runs on
//! an older checkout unchanged, and the two outputs compare byte for byte.
//!
//! Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test run_replay_hash_measure -- --ignored
//! --nocapture`.

use mie_adapter_binance::LiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_domain::event_hash::EventStreamHasher;
use mie_ports::outbound::MarketDataProvider;
use std::path::PathBuf;

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("set {name}")))
}

#[test]
#[ignore = "hashes a live capture's run replays for ADR-063; see the module docs"]
fn measure_run_replay_event_hash() {
    let raw_root = env_path("MIE_MEASURE_RAW_ROOT");
    let journal = env_path("MIE_MEASURE_JOURNAL");
    let runs: Vec<JournaledRun> = read_runs(&journal, "binance-um").unwrap();
    let store = ParquetRawStore::new(&raw_root);
    let live = LiveReplay::new(
        &store,
        "binance-um",
        "BTCUSDT",
        runs.iter().map(JournaledRun::live_run).collect(),
    );
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        let mut hasher = EventStreamHasher::new();
        while let Some(event) = replay.next_event().unwrap() {
            hasher.push(&event);
        }
        let stats = replay.stats();
        println!(
            "run {id}: records {}, dataset {}, event stream {}, pipeline {:?}",
            stats.records,
            replay.dataset(),
            hasher.finish(),
            stats.pipeline
        );
    }
}
