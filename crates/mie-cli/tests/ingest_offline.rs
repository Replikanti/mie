//! The full `mie ingest` composition offline: scripted WebSocket frames
//! into a real Parquet raw store, journal and core.

mod common;

use common::{
    D0, DepthConnection, HOUR, TempDir, agg, config, config_with, depth, depth_snapshot, ingest,
    ingest_depth, journal,
};
use mie_adapter_binance::{BinanceStream, Pipeline};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::config::IngestConfig;
use mie_cli::journal::RunParameters;
use mie_domain::event::MarketEvent;
use mie_domain::event_hash;
use mie_domain::feature::catalog;
use mie_domain::state_hash::STATE_HASH_ENCODING;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use serde_json::Value;
use std::collections::BTreeSet;

fn of_type<'a>(lines: &'a [Value], kind: &str) -> Vec<&'a Value> {
    lines.iter().filter(|l| l["type"] == kind).collect()
}

#[test]
fn ingest_seals_raw_files_journals_and_seeds_the_next_run() {
    let dir = TempDir::new("ingest");
    let config = config(dir.path());
    let t1 = D0 + HOUR;

    // Run 1: ids 100..102, 104, 105, and one unparsable frame.
    let frames = vec![
        agg(100, t1 + 10),
        agg(101, t1 + 20),
        agg(102, t1 + 30),
        "{\"e\":\"aggTrade\"".to_owned(),
        agg(104, t1 + 50),
        agg(105, t1 + 60),
    ];
    let first = ingest(&config, t1, vec![frames]);
    assert_eq!(first.exit_code(), 0, "{:?}", first.error);
    assert_eq!(first.run_id, "20261006T010000Z");
    // Five trades and the sequence-break gap.
    assert_eq!(first.events, 6);
    assert_eq!(first.domain_rejections, 0);
    let stats = &first.summary.as_ref().unwrap().stats;
    assert_eq!(stats.records, 6);

    // Every record, the unparsable one included, is sealed in the store.
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let selection = RawSelection::new(
        BTreeSet::from([key]),
        ReplayWindow {
            start: EventTime::from_millis(D0),
            end: EventTime::from_millis(D0 + 24 * HOUR),
        },
    )
    .unwrap();
    let dataset = store.select(&selection).unwrap();
    let rows: u64 = dataset.files.iter().map(|f| f.rows).sum();
    assert_eq!(rows, 6);
    let records = store.read(&dataset.files[0]).unwrap();
    assert_eq!(records[3].payload, b"{\"e\":\"aggTrade\"");

    let lines = journal(&config);
    let recovery = of_type(&lines, "recovery");
    assert_eq!(recovery.len(), 1);
    assert_eq!(recovery[0]["clean"], true);
    assert_eq!(
        of_type(&lines, "run_start")[0]["seeds"],
        serde_json::json!({})
    );
    let gaps = of_type(&lines, "gap");
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["reason"], "SequenceBreak");
    assert_eq!(
        (gaps[0]["start"].as_i64(), gaps[0]["end"].as_i64()),
        (Some(t1 + 30), Some(t1 + 50))
    );
    assert_eq!(of_type(&lines, "normalize_error")[0]["receive_seq"], 3);
    assert!(!of_type(&lines, "sealed").is_empty());
    let end = of_type(&lines, "run_end")[0];
    assert_eq!(end["exit_code"], 0);
    assert_eq!(end["normalize_errors"], 1);

    // The comparability keys of the equivalence harness (ADR-041), and the
    // core's checkpoints: the run spans 50 ms of event time, so only the
    // last one, after its final event.
    let start = of_type(&lines, "run_start")[0];
    assert_eq!(start["state_checkpoint_interval_ms"], 60_000);
    assert_eq!(start["state_hash_encoding"], STATE_HASH_ENCODING);
    assert_eq!(start["event_hash_encoding"], event_hash::ENCODING_VERSION);
    assert_eq!(
        start["feature_set"],
        catalog::current_set().version().to_string()
    );
    let checkpoints = of_type(&lines, "state_checkpoint");
    assert_eq!(checkpoints.len(), 1);
    assert_eq!(checkpoints[0]["ordinal"], 6);
    assert_eq!(checkpoints[0]["as_of"], t1 + 60);
    assert_eq!(checkpoints[0]["last"], true);
    assert_eq!(checkpoints[0]["event_hash"].as_str().unwrap().len(), 16);
    assert_eq!(checkpoints[0]["state_hash"].as_str().unwrap().len(), 16);

    // Run 2 an hour later: seeded with run 1's last event, so it opens
    // with a restart gap.
    let t2 = t1 + HOUR;
    let second = ingest(&config, t2, vec![vec![agg(300, t2 + 5), agg(301, t2 + 6)]]);
    assert_eq!(second.exit_code(), 0, "{:?}", second.error);
    let lines = journal(&config);
    let run2: Vec<&Value> = lines
        .iter()
        .filter(|l| l["run_id"] == "20261006T020000Z")
        .collect();
    let start = run2.iter().find(|l| l["type"] == "run_start").unwrap();
    assert_eq!(start["seeds"]["aggTrade"], t1 + 60);
    let gaps: Vec<_> = run2.iter().filter(|l| l["type"] == "gap").collect();
    assert_eq!(gaps.len(), 1);
    assert_eq!(gaps[0]["reason"], "Disconnected");
    assert_eq!(
        (gaps[0]["start"].as_i64(), gaps[0]["end"].as_i64()),
        (Some(t1 + 60), Some(t2 + 5))
    );
    assert_eq!(second.events, 3);
}

/// A gap as journaled: stream, reason, start and end in ms.
type JournaledGap = (String, String, i64, i64);

/// Recomputes every run of the journal from the raw store (all configured
/// streams, merged by `receive_seq`) and its `run_start` parameters, and
/// checks the gaps against the journaled ones; returns them per run.
fn recompute_gaps(config: &IngestConfig) -> Vec<(String, Vec<JournaledGap>)> {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let keys: BTreeSet<RawStreamKey> = config
        .streams()
        .unwrap()
        .iter()
        .map(|s| RawStreamKey::new("binance-um", "BTCUSDT", s.raw_name()).unwrap())
        .collect();
    let selection = RawSelection::new(
        keys,
        ReplayWindow {
            start: EventTime::from_millis(D0 - 24 * HOUR),
            end: EventTime::from_millis(D0 + 24 * HOUR),
        },
    )
    .unwrap();
    let mut records = Vec::new();
    for file in store.select(&selection).unwrap().files {
        let stream = BinanceStream::from_raw_name(file.stream.stream()).unwrap();
        records.extend(store.read(&file).unwrap().into_iter().map(|r| (stream, r)));
    }
    let lines = journal(config);
    let mut out = Vec::new();
    for start in lines.iter().filter(|l| l["type"] == "run_start") {
        let params = RunParameters::from_run_start(start).unwrap();
        // Configured, not the default: the recompute must use run_start.
        assert_eq!(params.hold_back_ms, 750);
        assert_eq!(params.oi_retime_ms, 10_000);
        let mut run: Vec<_> = records
            .iter()
            .filter(|(_, r)| {
                let session = &r.capture.as_ref().unwrap().session_id;
                session.starts_with(&format!("{}/", params.run_id))
            })
            .collect();
        run.sort_by_key(|(_, r)| r.capture.as_ref().unwrap().receive_seq);
        let mut pipeline = Pipeline::new(
            &params.symbol,
            params.hold_back_ms,
            params.oi_retime_ms,
            &params.seeds,
        );
        let mut events = Vec::new();
        for (stream, record) in run {
            events.extend(pipeline.push(*stream, record).events);
        }
        events.extend(pipeline.finish());
        let recomputed: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => Some((
                    format!("{:?}", g.stream),
                    format!("{:?}", g.reason),
                    g.start.as_millis(),
                    g.end.as_millis(),
                )),
                _ => None,
            })
            .collect();
        let journaled: Vec<_> = lines
            .iter()
            .filter(|l| l["type"] == "gap" && l["run_id"] == params.run_id.as_str())
            .map(|l| {
                (
                    l["stream"].as_str().unwrap().to_owned(),
                    l["reason"].as_str().unwrap().to_owned(),
                    l["start"].as_i64().unwrap(),
                    l["end"].as_i64().unwrap(),
                )
            })
            .collect();
        assert_eq!(recomputed, journaled, "run {}", params.run_id);
        out.push((params.run_id, journaled));
    }
    out
}

#[test]
fn the_raw_store_plus_run_start_reproduces_the_journaled_gaps() {
    let dir = TempDir::new("recompute");
    let config = config(dir.path());
    let t1 = D0 + HOUR;
    ingest(
        &config,
        t1,
        vec![vec![agg(1, t1 + 1), agg(2, t1 + 2), agg(5, t1 + 3)]],
    );
    let t2 = t1 + HOUR;
    ingest(&config, t2, vec![vec![agg(9, t2 + 1), agg(11, t2 + 2)]]);
    let runs = recompute_gaps(&config);
    assert_eq!(runs.len(), 2);
    assert!(runs.iter().all(|(_, gaps)| !gaps.is_empty()));
    // Run 2 opens with the seed gap, which only run_start can reproduce.
    let lines = journal(&config);
    let run2 = RunParameters::from_run_start(
        lines
            .iter()
            .find(|l| l["type"] == "run_start" && l["run_id"] == "20261006T020000Z")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        run2.seeds[&BinanceStream::AggTrade],
        EventTime::from_millis(t1 + 3)
    );

    // The book gaps too: a reconnect resync, then a restart whose first
    // sync opens with the seed gap.
    let dir = TempDir::new("recompute-depth");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    ingest_depth(
        &config,
        t1,
        vec![
            DepthConnection {
                frames: vec![depth(11, 20, 10, t1), depth(21, 30, 20, t1 + 100)],
                await_served: 1,
            },
            DepthConnection {
                frames: vec![depth(41, 50, 40, t1 + 5_000)],
                await_served: 2,
            },
        ],
        vec![depth_snapshot(15, t1 - 50), depth_snapshot(45, t1 + 4_950)],
        vec![],
    );
    ingest_depth(
        &config,
        t2,
        vec![DepthConnection {
            frames: vec![depth(911, 920, 910, t2 + 10)],
            await_served: 1,
        }],
        vec![depth_snapshot(915, t2 + 5)],
        vec![],
    );
    let runs = recompute_gaps(&config);
    assert_eq!(
        runs,
        [
            (
                "20261006T010000Z".to_owned(),
                vec![(
                    "OrderBook".to_owned(),
                    "Disconnected".to_owned(),
                    t1 + 100,
                    t1 + 4_950
                )]
            ),
            (
                "20261006T020000Z".to_owned(),
                vec![(
                    "OrderBook".to_owned(),
                    "Disconnected".to_owned(),
                    t1 + 5_000,
                    t2 + 5
                )]
            ),
        ]
    );
}

#[test]
fn ingest_routes_depth_and_open_interest_by_url_into_the_raw_store() {
    let dir = TempDir::new("routes");
    let config = config_with(dir.path(), &["openInterest", "depth", "depthSnapshot"]);
    let t1 = D0 + HOUR;
    let outcome = ingest_depth(
        &config,
        t1,
        vec![DepthConnection {
            frames: vec![depth(11, 20, 10, t1 + 10), depth(21, 30, 20, t1 + 110)],
            await_served: 1,
        }],
        vec![depth_snapshot(15, t1 + 5)],
        vec![format!(
            r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{}}}"#,
            t1 - 4_000
        )],
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_eq!(outcome.domain_rejections, 0);

    let store = ParquetRawStore::new(&config.paths.raw_root);
    for (stream, rows) in [("openInterest", 1), ("depth", 2), ("depthSnapshot", 1)] {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", stream).unwrap();
        let selection = RawSelection::new(
            BTreeSet::from([key]),
            ReplayWindow {
                start: EventTime::from_millis(D0),
                end: EventTime::from_millis(D0 + 24 * HOUR),
            },
        )
        .unwrap();
        let sealed: u64 = store
            .select(&selection)
            .unwrap()
            .files
            .iter()
            .map(|f| f.rows)
            .sum();
        assert_eq!(sealed, rows, "{stream}");
    }
    let lines = journal(&config);
    let start = of_type(&lines, "run_start")[0];
    assert_eq!(start["depth_snapshot_limit"], 1000);
    assert_eq!(start["depth_checkpoint_interval_ms"], 60_000);
    let fetch = of_type(&lines, "depth_snapshot_fetch");
    assert_eq!(fetch.len(), 1);
    assert_eq!(
        (&fetch[0]["trigger"], &fetch[0]["persisted"]),
        (&serde_json::json!("sync"), &serde_json::json!(true))
    );
    assert_eq!(of_type(&lines, "oi_poll")[0]["persisted"], true);
    let book: Vec<&str> = of_type(&lines, "book")
        .iter()
        .map(|l| l["event"].as_str().unwrap())
        .collect();
    assert_eq!(book, ["desync", "sync"]);
    let end = of_type(&lines, "run_end")[0];
    assert_eq!(end["streams"]["depth"]["book"]["syncs"], 1);
    assert_eq!(end["streams"]["depth"]["events"], 2);
}
