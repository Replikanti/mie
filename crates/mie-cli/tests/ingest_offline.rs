//! The full `mie ingest` composition offline: scripted WebSocket frames
//! into a real Parquet raw store, journal and core.
//!
//! The warm start (ADR-052 D3, D7, D8, D13) runs offline too: an archive
//! imported before the cutover, chain runs whose prefix is compared with
//! `mie replay --source history`, the capture going on while the warm-up is
//! held, the cold cases, the memory guard and an uncrossable hole.

mod common;

use common::warm::{
    AGG, ClockOf, Oi, Script, ScriptClock, Step, WAIT_DEADLINE, Ws, import_archive, lines_of, oi,
    one, run_steps, warm_config, warm_ingest,
};
use common::{
    CUTOVER, D0, DepthConnection, HOUR, ORIGIN, TempDir, agg, config, config_with, depth,
    depth_snapshot, history_config, ingest, ingest_depth, journal, journaled_run, write_records,
};
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::{BinanceStream, Pipeline, RUN_MARGIN_MS};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::config::IngestConfig;
use mie_cli::ingest::{IngestOutcome, Transports, WarmUpHold};
use mie_cli::journal::RunParameters;
use mie_cli::replay::{self, ReplayOutcome, ReplayRequest, ReplaySource};
use mie_domain::event::MarketEvent;
use mie_domain::event_hash;
use mie_domain::feature::catalog;
use mie_domain::state_hash::STATE_HASH_ENCODING;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use serde_json::Value;
use std::collections::{BTreeSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

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

// ---------------------------------------------------------------------------
// The warm start (ADR-052), with the scripted capture of `common::warm`.

/// `mie replay --source history` of `config` over `[start, end)`.
fn history_replay(config: &IngestConfig, start: i64, end: i64) -> (ReplayOutcome, String) {
    let request = ReplayRequest {
        source: ReplaySource::History(config.clone()),
        window: ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        },
    };
    let mut out = Vec::new();
    let outcome = replay::run(&request, &mut out).expect("the report is written");
    (outcome, String::from_utf8(out).unwrap())
}

/// The gaps by reason the report delivered on the domain stream `stream`.
fn report_gaps(report: &str, stream: &str, reason: &str) -> u64 {
    let Some(line) = report
        .lines()
        .find(|l| l.starts_with(&format!("delivered {stream}:")))
    else {
        return 0;
    };
    line.split("gaps ")
        .nth(1)
        .unwrap()
        .split(", ")
        .find_map(|g| g.strip_prefix(&format!("{reason} ")))
        .map_or(0, |n| n.parse().unwrap())
}

/// Checks a warm start's prefix against the history replay over
/// `[ORIGIN, end)`.
fn assert_prefix(warm: &Value, config: &IngestConfig, end: i64) {
    assert_eq!(warm["warm"], true, "{warm}");
    let (prefix, report) = history_replay(config, ORIGIN, end);
    assert!(prefix.error.is_none(), "{report}");
    assert_eq!(warm["prefix_dataset"], prefix.dataset.unwrap().as_str());
    let stream = prefix.stream_hash.unwrap();
    assert_eq!(warm["prefix_events"], stream.events);
    assert_eq!(warm["prefix_event_hash"], stream.fingerprint.to_string());
    assert_eq!(warm["prefix_rejections"], prefix.domain_rejections);
    assert_eq!(
        warm["effective_origin_ms"].as_i64(),
        prefix.restarts.last().copied()
    );
    assert_eq!(
        warm["handoff_state_hash"],
        prefix.state_hash.unwrap().to_string()
    );
    assert_eq!(warm["origin_ms"], ORIGIN);
    assert_eq!(warm["cutover_ms"], CUTOVER);
}

/// Checks that run `run_id`'s final state is the history replay's through
/// the run, without rejections, and returns that replay's report.
fn assert_final_state(lines: &[Value], config: &IngestConfig, run_id: &str) -> String {
    let end = one(lines, run_id, "run_end");
    assert_eq!(end["exit_code"], 0, "{end}");
    assert_eq!(end["domain_rejections"], 0);
    let end_ms = end["at_ms"].as_i64().unwrap();
    let (through, report) = history_replay(config, ORIGIN, end_ms + RUN_MARGIN_MS);
    assert!(through.pass, "{report}");
    // The last checkpoint is after the run's final event (marked `last`
    // unless that event recorded one on its cadence).
    let checkpoints = lines_of(lines, run_id, "state_checkpoint");
    let last = checkpoints.last().expect("checkpoints");
    assert_eq!(last["ordinal"], end["events"]);
    assert_eq!(
        last["state_hash"],
        through.state_hash.unwrap().to_string(),
        "{report}"
    );
    report
}

#[test]
fn a_warm_start_continues_the_history_chain_at_the_seam() {
    let dir = TempDir::new("warm-seam");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    // (A) The first chain run starts an hour after the cutover, so
    // [origin, cutover) has no live segment.
    let t1 = CUTOVER + HOUR;
    let outcome = warm_ingest(
        &config,
        t1,
        run_steps(t1, 10_000),
        vec![oi(t1 - 4_000)],
        None,
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_eq!(outcome.domain_rejections, 0);
    let run = outcome.run_id.as_str();
    assert_eq!(run, "20261006T010000Z");
    let lines = journal(&config);
    let start = one(&lines, run, "run_start");
    assert_eq!(start["history_origin_ms"], ORIGIN);
    assert_eq!(start["history_cutover_ms"], CUTOVER);

    // The prefix and the handoff are those of `mie replay --source
    // history` over [origin, cutover).
    let warm = one(&lines, run, "warm_start");
    assert_prefix(warm, &config, CUTOVER);
    assert_eq!(warm["prefix_last_event_ms"], CUTOVER - 200);
    // The seeds come from the archive: trades and open interest, nothing
    // for mark price, liquidations or depth (ADR-052 D3).
    let seeds: Vec<&str> = warm["seeds"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(seeds, ["aggTrade", "openInterest"]);
    assert_eq!(warm["seeds"]["aggTrade"], CUTOVER - 200);
    // Three days of history leave the regime (8.9 days) warming up.
    let warming: Vec<&str> = warm["warming_up"]
        .as_array()
        .unwrap()
        .iter()
        .map(|id| id.as_str().unwrap())
        .collect();
    assert!(warming.contains(&"volatility.regime.1h@1"), "{warming:?}");
    assert!(!warming.contains(&"structure.swing.15m@1"), "{warming:?}");
    assert!(
        one(&lines, run, "warm_ready")["ready_after_ms"]
            .as_i64()
            .unwrap()
            >= 0
    );
    assert!(lines_of(&lines, run, "chain_restart").is_empty());
    // The capture's own pipeline keeps the lookback seeds of `run_start`.
    assert_eq!(start["seeds"], serde_json::json!({}));

    // The run's final state is the chain's, through the run.
    let report = assert_final_state(&lines, &config, run);
    // The seam: one `Disconnected` gap per seeded stream, none for the
    // unseeded mark price.
    assert_eq!(
        report_gaps(&report, "Trades", "Disconnected"),
        1,
        "{report}"
    );
    assert_eq!(
        report_gaps(&report, "OpenInterest", "Disconnected"),
        1,
        "{report}"
    );
    assert_eq!(
        report_gaps(&report, "MarkPrice", "Disconnected"),
        0,
        "{report}"
    );
    assert!(report.contains("delivered MarkPrice: events 3"), "{report}");

    // (C) Checkpoints count this run's events only, from its first event.
    let end = one(&lines, run, "run_end");
    let checkpoints = lines_of(&lines, run, "state_checkpoint");
    assert!(checkpoints.len() >= 2, "{checkpoints:?}");
    assert_eq!(checkpoints.last().unwrap()["ordinal"], end["events"]);
    assert!(checkpoints.iter().all(|c| {
        c["ordinal"].as_u64() <= end["events"].as_u64() && c["as_of"].as_i64() >= Some(t1 - 4_000)
    }));

    // `mie capture-report` reads past the new journal records.
    let mut out = Vec::new();
    mie_cli::report::run(&config, D0, D0 + DAY_MS, 1.0, &mut out).expect("the report runs");
    let text = String::from_utf8(out).unwrap();
    assert!(!text.contains("unparsable"), "{text}");
    assert!(text.contains("1 run(s)"), "{text}");
}

#[test]
fn a_second_run_warm_starts_from_the_chain_through_its_predecessor() {
    let dir = TempDir::new("warm-second");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let t1 = CUTOVER + HOUR;
    let first = warm_ingest(
        &config,
        t1,
        run_steps(t1, 10_000),
        vec![oi(t1 - 4_000)],
        None,
    );
    assert_eq!(first.exit_code(), 0, "{:?}", first.error);
    // (B) An hour later: more than 2 × RUN_MARGIN_MS after the first ended.
    let t2 = t1 + HOUR;
    let second = warm_ingest(
        &config,
        t2,
        run_steps(t2, 20_000),
        vec![oi(t2 - 3_000)],
        None,
    );
    assert_eq!(second.exit_code(), 0, "{:?}", second.error);
    let lines = journal(&config);
    let r1_end = one(&lines, &first.run_id, "run_end")["at_ms"]
        .as_i64()
        .unwrap();
    let warm = one(&lines, &second.run_id, "warm_start");
    assert_prefix(warm, &config, r1_end + RUN_MARGIN_MS);
    // Every series the first run captured is seeded from it now.
    assert_eq!(warm["seeds"]["aggTrade"], t1 + 65_000);
    assert_eq!(warm["seeds"]["markPrice"], t1 + 3_020);
    assert!(warm["seeds"]["openInterest"].as_i64() >= Some(t1 - 4_000));
    let report = assert_final_state(&lines, &config, &second.run_id);
    // The seam and the hole between the runs.
    assert_eq!(
        report_gaps(&report, "Trades", "Disconnected"),
        2,
        "{report}"
    );
    assert_eq!(
        report_gaps(&report, "MarkPrice", "Disconnected"),
        1,
        "{report}"
    );
}

#[test]
fn a_fast_restart_overlap_is_late_or_retimed_never_rejected() {
    let dir = TempDir::new("warm-fast-restart");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let t1 = CUTOVER + HOUR;
    let first = warm_ingest(
        &config,
        t1,
        run_steps(t1, 10_000),
        vec![oi(t1 - 4_000)],
        None,
    );
    assert_eq!(first.exit_code(), 0, "{:?}", first.error);
    // The second run starts 2 s after the first ended (at t1 + 1.5 s); its
    // first poll is at the next 10 s slot. Its open interest and first
    // trade are older than the first run's last event (t1 + 65 s).
    let t2 = t1 + 3_500;
    let steps = vec![
        Step::Advance(6_500),
        Step::Polled(1),
        Step::Frame(AGG, agg(10_100, t1 + 64_000)),
        Step::Frame(AGG, agg(10_101, t1 + 70_000)),
        Step::Gate,
        Step::Frame(AGG, agg(10_102, t1 + 71_000)),
        Step::Frame(AGG, agg(10_103, t1 + 72_000)),
    ];
    let second = warm_ingest(&config, t2, steps, vec![oi(t1 + 60_000)], None);
    assert_eq!(second.exit_code(), 0, "{:?}", second.error);
    assert_eq!(second.domain_rejections, 0);
    let lines = journal(&config);
    assert_eq!(one(&lines, &second.run_id, "warm_start")["warm"], true);
    let report = assert_final_state(&lines, &config, &second.run_id);
    assert!(report_gaps(&report, "Trades", "LateEvent") >= 1, "{report}");
    assert_eq!(
        report_gaps(&report, "OpenInterest", "LateEvent"),
        0,
        "{report}"
    );
    // The archive's sample, the first run's and the second run's,
    // re-timed after the first run's last event.
    assert!(
        report.contains("delivered OpenInterest: events 3"),
        "the second sample is re-timed: {report}"
    );
}

/// Records of run `run_id` in the sealed `aggTrade` files of the cutover
/// day.
fn sealed_records(store: &ParquetRawStore, run_id: &str) -> u64 {
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let selection = RawSelection::new(
        BTreeSet::from([key]),
        ReplayWindow {
            start: EventTime::from_millis(CUTOVER),
            end: EventTime::from_millis(CUTOVER + DAY_MS),
        },
    )
    .unwrap();
    let Ok(dataset) = store.select(&selection) else {
        return 0;
    };
    let prefix = format!("{run_id}/");
    dataset
        .files
        .iter()
        .flat_map(|file| store.read(file).unwrap_or_default())
        .filter(|r| r.capture.as_ref().unwrap().session_id.starts_with(&prefix))
        .count() as u64
}

#[test]
fn the_capture_goes_on_while_the_warm_up_is_held() {
    let dir = TempDir::new("warm-held");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let t1 = CUTOVER + HOUR;
    // (D) The warm-up waits until the store holds a sealed file of the run.
    let root = config.paths.raw_root.clone();
    let held = Arc::new(AtomicU64::new(0));
    let seen = Arc::clone(&held);
    let before: Box<dyn FnOnce() + Send> = Box::new(move || {
        let store = ParquetRawStore::new(&root);
        let deadline = Instant::now() + WAIT_DEADLINE;
        loop {
            let sealed = sealed_records(&store, "20261006T010000Z");
            if sealed > 0 {
                seen.store(sealed, Ordering::SeqCst);
                break;
            }
            assert!(Instant::now() < deadline, "no sealed file of the run");
            std::thread::sleep(Duration::from_millis(1));
        }
        // Let the core take what the capture sent before the seal.
        std::thread::sleep(Duration::from_millis(50));
    });
    let outcome = warm_ingest(
        &config,
        t1,
        run_steps(t1, 10_000),
        vec![oi(t1 - 4_000)],
        Some(before),
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    // At least one trade was sealed before the warm-up started.
    let held = held.load(Ordering::SeqCst);
    assert!(held > 0);
    let lines = journal(&config);
    let run = outcome.run_id.as_str();
    let position = |kind: &str| {
        lines
            .iter()
            .position(|l| l["type"] == kind && l["run_id"] == run)
            .unwrap()
    };
    assert!(position("sealed") < position("warm_start"));
    let warm = one(&lines, run, "warm_start");
    assert_eq!(warm["warm"], true);
    assert!(
        warm["queue_high_water"].as_u64().unwrap() >= held,
        "{warm} after {held} sealed trades"
    );
    assert!(warm["queue_high_water_bytes"].as_u64().unwrap() > 0);
    assert!(position("warm_start") < position("warm_ready"));
    assert_final_state(&lines, &config, run);
}

/// Checks a cold run: exit 0, the reason, sealed records and checkpoints.
fn assert_cold(config: &IngestConfig, outcome: &IngestOutcome, reason: &str) -> Value {
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let lines = journal(config);
    let run = outcome.run_id.as_str();
    let cold = one(&lines, run, "warm_start").clone();
    assert_eq!(cold["warm"], false, "{cold}");
    assert_eq!(cold["cold"], reason, "{cold}");
    assert!(!lines_of(&lines, run, "sealed").is_empty());
    assert!(!lines_of(&lines, run, "state_checkpoint").is_empty());
    assert!(lines_of(&lines, run, "warm_ready").is_empty());
    assert!(outcome.events > 0);
    cold
}

#[test]
fn runs_without_history_or_before_the_cutover_or_after_a_failed_warm_up_start_cold() {
    let t1 = D0 + HOUR;
    let frames = || vec![vec![agg(1, t1 + 10), agg(2, t1 + 20), agg(3, t1 + 30)]];

    // (E) Without `[history]`.
    let dir = TempDir::new("cold-no-history");
    let plain = config(dir.path());
    let outcome = ingest(&plain, t1, frames());
    let cold = assert_cold(&plain, &outcome, "no-history");
    assert_eq!(cold["error"], Value::Null);
    assert!(cold.get("origin_ms").is_none());
    let start = &journal(&plain)[1];
    assert_eq!(start["type"], "run_start");
    assert_eq!(start["history_origin_ms"], Value::Null);

    // A start before the cutover.
    let dir = TempDir::new("cold-before-cutover");
    let before = history_config(dir.path(), "2026-10-03", "2026-10-07");
    let outcome = ingest(&before, t1, frames());
    let cold = assert_cold(&before, &outcome, "before-cutover");
    assert_eq!(cold["cutover_ms"], D0 + DAY_MS);

    // A run before the cutover crashed, and the cutover falls inside it:
    // the chain refuses it, and the warm-up fails with that error.
    let dir = TempDir::new("cold-warm-up-failed");
    import_archive(dir.path());
    let mut inside = history_config(dir.path(), "2026-10-03", "2026-10-06");
    inside.history.as_mut().unwrap().max_run_records = Some(1_000);
    let (mut lines, records) = journaled_run(
        "20261005T233000Z",
        CUTOVER - 30 * 60_000,
        0,
        500,
        &[CUTOVER - 60_000],
    );
    lines.truncate(1);
    write_records(&inside, &records, &lines);
    let outcome = ingest(&inside, t1, frames());
    let cold = assert_cold(&inside, &outcome, "warm-up-failed");
    let error = cold["error"].as_str().unwrap();
    assert!(
        error.contains("run 20261005T233000Z") && error.contains("must not fall inside a run"),
        "{error}"
    );
    assert_eq!(cold["origin_ms"], ORIGIN);
}

#[test]
fn the_memory_guard_starts_the_run_cold_and_the_capture_goes_on() {
    let t = CUTOVER + 2 * HOUR;
    // Two records: never more than the queue may hold before the guard has
    // run (max_run_records 2), so the guard's own error is the one journaled.
    let frames = || vec![vec![agg(1, t + 10), agg(2, t + 20)]];
    let predecessor = |dir: &Path, max: Option<u64>, clean: bool| {
        import_archive(dir);
        let mut config = history_config(dir, "2026-10-03", "2026-10-06");
        config.history.as_mut().unwrap().max_run_records = max;
        let times = [
            CUTOVER + HOUR,
            CUTOVER + HOUR + 1_000,
            CUTOVER + HOUR + 2_000,
        ];
        let (mut lines, records) = journaled_run(
            "20261006T010000Z",
            CUTOVER + HOUR,
            CUTOVER + HOUR + 60_000,
            500,
            &times,
        );
        if !clean {
            lines.truncate(1);
        }
        write_records(&config, &records, &lines);
        config
    };

    // (F) Above the guard: the run_end count of a clean predecessor, then
    // the sealed rows of a crashed one.
    for clean in [true, false] {
        let dir = TempDir::new("guard-above");
        let config = predecessor(dir.path(), Some(2), clean);
        let outcome = ingest(&config, t, frames());
        let cold = assert_cold(&config, &outcome, "warm-up-failed");
        let error = cold["error"].as_str().unwrap();
        // A crashed run counts the rows of every file over its extent, an
        // upper bound: the files of the starting run may already be among
        // them.
        let held: u64 = error
            .split("holds ")
            .nth(1)
            .and_then(|rest| rest.split(' ').next())
            .and_then(|n| n.parse().ok())
            .unwrap_or_else(|| panic!("{error}"));
        assert!(
            error.contains("run 20261006T010000Z holds")
                && error.contains("max_run_records 2")
                && if clean { held == 3 } else { held >= 3 },
            "{error}"
        );
    }

    // Without the key.
    let dir = TempDir::new("guard-missing");
    let config = predecessor(dir.path(), None, true);
    let outcome = ingest(&config, t, frames());
    let cold = assert_cold(&config, &outcome, "warm-up-failed");
    assert!(
        cold["error"].as_str().unwrap().contains("not configured"),
        "{cold}"
    );

    // Within the guard, with room for the queue beside the run: warm.
    let dir = TempDir::new("guard-within");
    let config = predecessor(dir.path(), Some(103), true);
    let outcome = warm_ingest(
        &config,
        t,
        vec![
            Step::Frame(AGG, agg(1, t + 10)),
            Step::Gate,
            Step::Frame(AGG, agg(2, t + 20)),
        ],
        Vec::new(),
        None,
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let lines = journal(&config);
    assert_eq!(one(&lines, &outcome.run_id, "warm_start")["warm"], true);
}

#[test]
fn an_uncrossable_hole_restarts_the_warm_engine_in_the_live_part() {
    let dir = TempDir::new("warm-hole");
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let times = [
        CUTOVER + HOUR,
        CUTOVER + HOUR + 1_000,
        CUTOVER + HOUR + 2_000,
    ];
    let (lines, records) = journaled_run(
        "20261006T010000Z",
        CUTOVER + HOUR,
        CUTOVER + HOUR + 60_000,
        500,
        &times,
    );
    write_records(&config, &records, &lines);
    // (G) 40 days later: more than the domain's 31-day bound.
    let t = CUTOVER + 40 * DAY_MS;
    let steps = vec![
        Step::Frame(AGG, agg(1_000, t + 10)),
        Step::Gate,
        Step::Frame(AGG, agg(1_001, t + 20)),
        Step::Frame(AGG, agg(1_002, t + 65_000)),
    ];
    let outcome = warm_ingest(&config, t, steps, Vec::new(), None);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_eq!(outcome.domain_rejections, 0, "no rejection storm");
    let lines = journal(&config);
    let run = outcome.run_id.as_str();
    let warm = one(&lines, run, "warm_start");
    assert_eq!(warm["warm"], true);
    assert_eq!(warm["effective_origin_ms"], Value::Null);
    let restarts: Vec<i64> = lines_of(&lines, run, "chain_restart")
        .iter()
        .map(|l| l["time"].as_i64().unwrap())
        .collect();
    assert_eq!(restarts, [t + 10]);
    // Journaled when it happened, before the checkpoints that followed.
    let position = |kind: &str| {
        lines
            .iter()
            .position(|l| l["type"] == kind && l["run_id"] == run)
            .unwrap()
    };
    assert!(position("chain_restart") < position("state_checkpoint"));
    assert_final_state(&lines, &config, run);
    let end_ms = one(&lines, run, "run_end")["at_ms"].as_i64().unwrap();
    let (through, _) = history_replay(&config, ORIGIN, end_ms + RUN_MARGIN_MS);
    assert_eq!(through.restarts, [t + 10]);
}

/// Whether this process can create entries in a read-only directory (it
/// runs as root): the capture failure below cannot be provoked then.
fn ignores_permissions(dir: &Path) -> bool {
    use std::os::unix::fs::PermissionsExt;
    let probe = dir.join("probe");
    std::fs::create_dir_all(&probe).unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o555)).unwrap();
    let ignored = std::fs::create_dir(probe.join("inside")).is_ok();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o755)).unwrap();
    std::fs::remove_dir_all(&probe).unwrap();
    ignored
}

#[test]
fn a_restart_is_journaled_when_it_happens_even_if_the_run_then_fails() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("warm-hole-failed");
    if ignores_permissions(dir.path()) {
        eprintln!("skipped: a read-only directory cannot fail the capture for this user");
        return;
    }
    import_archive(dir.path());
    let config = warm_config(dir.path(), Some(1_000));
    let times = [
        CUTOVER + HOUR,
        CUTOVER + HOUR + 1_000,
        CUTOVER + HOUR + 2_000,
    ];
    let (lines, records) = journaled_run(
        "20261006T010000Z",
        CUTOVER + HOUR,
        CUTOVER + HOUR + 60_000,
        500,
        &times,
    );
    write_records(&config, &records, &lines);
    let t = CUTOVER + 40 * DAY_MS;
    // The trades' stream directory turns read-only once the restart is due:
    // the next day's partition cannot be created, the append fails, and so
    // does the capture and with it the core's drive.
    let stream_dir = config
        .paths
        .raw_root
        .join("source=binance-um/instrument=BTCUSDT/stream=aggTrade");
    let locked = stream_dir.clone();
    let steps = vec![
        Step::Frame(AGG, agg(1_000, t + 10)),
        Step::Gate,
        Step::Frame(AGG, agg(1_001, t + 20)),
        // Releases the first two trades from the hold-back: the hole's gap
        // restarts the engine.
        Step::Frame(AGG, agg(1_002, t + 65_000)),
        Step::Run(Box::new(move || {
            std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();
        })),
        Step::Frame(AGG, agg(1_003, t + DAY_MS + 1_000)),
    ];
    let outcome = warm_ingest(&config, t, steps, Vec::new(), None);
    std::fs::set_permissions(&stream_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert_eq!(outcome.exit_code(), 1, "the capture failed");
    assert!(
        outcome
            .error
            .as_deref()
            .is_some_and(|e| e.contains("append")),
        "{:?}",
        outcome.error
    );
    let lines = journal(&config);
    let run = outcome.run_id.as_str();
    assert_eq!(one(&lines, run, "warm_start")["warm"], true);
    let restarts: Vec<i64> = lines_of(&lines, run, "chain_restart")
        .iter()
        .map(|l| l["time"].as_i64().unwrap())
        .collect();
    assert_eq!(restarts, [t + 10]);
    assert_eq!(one(&lines, run, "run_end")["exit_code"], 1);
}

/// Where the warm-up of [`overflow_run`] is held.
#[derive(Clone, Copy)]
enum HoldAt {
    /// Before its memory guard: the queue may hold all of
    /// `max_run_records`.
    BeforeGuard,
    /// After it has its result, so after the guard lowered the queue's
    /// limit to what `max_run_records` leaves beside the largest run.
    AfterGuard,
}

/// A chain run after a 3-record predecessor with `max_run_records` `max`
/// whose warm-up is held at `at` until the run has journaled its
/// `warm_start`: no record arrives before the hold is reached, and the hold
/// lasts until the queue has overflowed, so the limit in force is exactly
/// the one of `at`. Returns the cold run's error and the run's journal.
fn overflow_run(tag: &str, max: u64, at: HoldAt) -> (String, Vec<Value>) {
    let dir = TempDir::new(tag);
    import_archive(dir.path());
    let mut config = history_config(dir.path(), "2026-10-03", "2026-10-06");
    config.history.as_mut().unwrap().max_run_records = Some(max);
    let times = [
        CUTOVER + HOUR,
        CUTOVER + HOUR + 1_000,
        CUTOVER + HOUR + 2_000,
    ];
    let (lines, records) = journaled_run(
        "20261006T010000Z",
        CUTOVER + HOUR,
        CUTOVER + HOUR + 60_000,
        500,
        &times,
    );
    write_records(&config, &records, &lines);
    let t = CUTOVER + 2 * HOUR;

    let reached = Arc::new(AtomicBool::new(false));
    let release = Arc::new(AtomicBool::new(false));
    let hold: Box<dyn FnOnce() + Send> = {
        let (reached, release) = (Arc::clone(&reached), Arc::clone(&release));
        Box::new(move || {
            reached.store(true, Ordering::SeqCst);
            let deadline = Instant::now() + WAIT_DEADLINE;
            while !release.load(Ordering::SeqCst) {
                assert!(Instant::now() < deadline, "the warm-up was never released");
                std::thread::sleep(Duration::from_millis(1));
            }
        })
    };
    let journal_path = config.paths.journal.clone();
    let mut steps = vec![Step::Run(Box::new(move || {
        // No record before the warm-up has reached its hold.
        let deadline = Instant::now() + WAIT_DEADLINE;
        while !reached.load(Ordering::SeqCst) {
            assert!(
                Instant::now() < deadline,
                "the warm-up never reached its hold"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }))];
    steps.extend((0..8).map(|i| Step::Frame(AGG, agg(1 + i, t + 10 + 1_000 * i as i64))));
    steps.push(Step::Run(Box::new(move || {
        // The overflow journals (and syncs) the cold `warm_start`; only
        // then is the abandoned warm-up let go.
        let deadline = Instant::now() + WAIT_DEADLINE;
        while !std::fs::read_to_string(&journal_path)
            .unwrap_or_default()
            .contains("\"warm_start\"")
        {
            assert!(Instant::now() < deadline, "the queue never overflowed");
            std::thread::sleep(Duration::from_millis(1));
        }
        release.store(true, Ordering::SeqCst);
    })));
    steps.extend((8..10).map(|i| Step::Frame(AGG, agg(1 + i, t + 10 + 1_000 * i as i64))));
    let shutdown = Arc::new(AtomicBool::new(false));
    let script = Arc::new(Script {
        steps: Mutex::new((steps.into(), None)),
        clock: ScriptClock(Mutex::new(t * 1_000_000)),
        bodies: Mutex::new(VecDeque::new()),
        polled: AtomicU64::new(0),
        gate: AtomicBool::new(false),
        shutdown: Arc::clone(&shutdown),
    });
    let transports = Transports {
        connector: Arc::new(Ws(Arc::clone(&script))),
        http: Arc::new(Oi(Arc::clone(&script))),
        clock: Arc::new(ClockOf(Arc::clone(&script))),
        device_probe: Arc::new(mie_cli::store::StDevProbe),
    };
    let hold = match at {
        HoldAt::BeforeGuard => WarmUpHold {
            before: Some(hold),
            after: None,
        },
        HoldAt::AfterGuard => WarmUpHold {
            before: None,
            after: Some(hold),
        },
    };
    let outcome =
        mie_cli::ingest::run_with_hold(&config, transports, shutdown, hold).expect("ingest starts");
    let cold = assert_cold(&config, &outcome, "warm-up-failed");
    let lines = journal(&config);
    // Every trade reached the cold engine: the queued ones, then the rest.
    let end = one(&lines, &outcome.run_id, "run_end");
    assert_eq!(end["records"], 10);
    assert_eq!(outcome.domain_rejections, 0);
    assert!(outcome.events >= 10, "{}", outcome.events);
    (cold["error"].as_str().unwrap().to_owned(), lines)
}

#[test]
fn a_queue_past_its_limit_abandons_the_warm_up_and_starts_cold() {
    let expected = |queued: u64, limit: u64, max: u64| {
        format!(
            "warm-up queue (ADR-052 D13): {queued} records queued, more than the {limit} that \
             [history] max_run_records {max} leaves beside the run the warm-up replays"
        )
    };
    // Before the guard: nothing is replayed yet, so the queue may hold all
    // of max_run_records.
    let (error, _) = overflow_run("queue-before-guard", 5, HoldAt::BeforeGuard);
    assert_eq!(error, expected(6, 5, 5));
    // After the guard: the 3-record predecessor leaves 2.
    let (error, _) = overflow_run("queue-after-guard", 5, HoldAt::AfterGuard);
    assert_eq!(error, expected(3, 2, 5));
    // The boundary: the guard admits a run of exactly max_run_records,
    // which leaves the queue no room; the first queued record overflows it.
    let (error, _) = overflow_run("queue-no-room", 3, HoldAt::AfterGuard);
    assert_eq!(error, expected(1, 0, 3));
}

mod tiered_start {
    //! The start refusal on a tiered store's hot without its marker
    //! (ADR-062 D7, *Hot lost*), no `.mie/` without cold roots, and the
    //! seed scan of a tiered store: the union `select` with a readable copy
    //! ledger, the degraded start without one (D7 *Seeds*, *Hot intact,
    //! ledger lost*).

    use super::common::{
        D0, FakeClock, HOUR, NoHttp, ScriptedConnector, TempDir, agg, config, ingest,
    };
    use mie_adapter_parquet::root_marker::{self, HOT_ID};
    use mie_cli::config::{ColdRoot, IngestConfig};
    use mie_cli::ingest::Transports;
    use std::path::Path;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn tiered(dir: &Path) -> IngestConfig {
        let mut config = config(dir);
        config.paths.cold_roots = ["cold-a", "cold-b"]
            .map(|name| ColdRoot {
                path: dir.join(name),
                copy_target: true,
            })
            .to_vec();
        config
    }

    fn start(config: &IngestConfig) -> Result<mie_cli::ingest::IngestOutcome, String> {
        let shutdown = Arc::new(AtomicBool::new(false));
        let transports = Transports {
            connector: ScriptedConnector::new(
                "btcusdt@aggTrade",
                vec![vec![agg(1, D0 + HOUR + 10)]],
                &shutdown,
            ),
            http: Arc::new(NoHttp),
            clock: FakeClock::new(D0 + HOUR, "mie-aggTrade"),
            device_probe: super::common::probe_for(config),
        };
        mie_cli::ingest::run(config, transports, shutdown)
    }

    #[test]
    fn a_hot_without_its_marker_refuses_to_start_and_writes_nothing() {
        let dir = TempDir::new("tiered-no-marker");
        let config = tiered(dir.path());
        let error = start(&config).expect_err("refused");
        assert!(
            error.contains("mie store init") && error.contains("hot restore"),
            "{error}"
        );
        assert!(!config.paths.journal.exists());
        assert!(!config.paths.raw_root.exists());

        // With an existing, empty raw root: still nothing is created in it.
        std::fs::create_dir_all(&config.paths.raw_root).unwrap();
        start(&config).expect_err("refused");
        assert!(!config.paths.journal.exists());
        assert_eq!(
            std::fs::read_dir(&config.paths.raw_root).unwrap().count(),
            0
        );
    }

    #[test]
    fn a_raw_root_carrying_a_cold_marker_is_refused() {
        let dir = TempDir::new("tiered-cold-marker");
        let config = tiered(dir.path());
        std::fs::create_dir_all(&config.paths.raw_root).unwrap();
        root_marker::create_meta(&config.paths.raw_root, "cold-a", false).unwrap();
        let error = start(&config).expect_err("refused");
        assert!(error.contains("cold root \"cold-a\""), "{error}");
        assert!(!config.paths.journal.exists());
    }

    #[test]
    fn a_hot_with_its_marker_starts_as_today() {
        let dir = TempDir::new("tiered-marker");
        let config = tiered(dir.path());
        std::fs::create_dir_all(&config.paths.raw_root).unwrap();
        root_marker::create_meta(&config.paths.raw_root, HOT_ID, true).unwrap();
        let outcome = start(&config).expect("starts");
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        assert_eq!(outcome.summary.unwrap().stats.records, 1);
        assert!(config.paths.journal.exists());
    }

    #[test]
    fn without_cold_roots_no_meta_directory_is_created() {
        let dir = TempDir::new("single-root");
        let config = config(dir.path());
        assert_eq!(config.paths.cold_roots, []);
        let outcome = ingest(&config, D0 + HOUR, vec![vec![agg(1, D0 + HOUR + 10)]]);
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        assert!(config.paths.raw_root.exists());
        assert!(!config.paths.raw_root.join(".mie").exists());
    }

    // ------------------------------------------------- the seed listing

    use super::common::warm::import_archive;
    use super::common::{
        CUTOVER, cold_roots, cold_roots_toml, history_config, init_tiers, journal, journaled_run,
        write_records,
    };
    use mie_adapter_binance::archive::catalog::DAY_MS;
    use mie_adapter_binance::transport::HttpDownload;
    use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Kind, Record};
    use mie_cli::archive::{self, ArchiveTransports, ImportRequest};
    use mie_cli::config::ArchiveConfig;
    use mie_cli::replay::{self, ReplayRequest, ReplaySource};
    use mie_domain::time::EventTime;
    use mie_ports::outbound::ReplayWindow;
    use serde_json::Value;
    use std::path::PathBuf;

    /// The start of the runs below: an hour after the prior run.
    const T: i64 = CUTOVER + 2 * HOUR;

    /// Every path below `dir`, sorted.
    fn tree(dir: &Path) -> Vec<PathBuf> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(path) = stack.pop() {
            if path.is_dir() {
                for entry in std::fs::read_dir(&path).unwrap() {
                    stack.push(entry.unwrap().path());
                }
            }
            out.push(path);
        }
        out.sort();
        out
    }

    /// Copies the directory tree `from` to `to`.
    fn copy_tree(from: &Path, to: &Path) {
        std::fs::create_dir_all(to).unwrap();
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy_tree(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), &target).unwrap();
            }
        }
    }

    /// A tiered chain configuration under `dir`: the archive of
    /// [`import_archive`] with the same `cold_roots` in `archive.toml`, the
    /// memory guard, a clean prior run after the cutover, hot initialized.
    /// Before the initialization the store and the journal are copied to
    /// `single`, with a single-root configuration over the copy.
    fn tiered_chain(dir: &Path, single: &Path) -> (IngestConfig, IngestConfig) {
        import_archive(dir);
        let cold = cold_roots(dir);
        let archive_toml = dir.join("archive.toml");
        let text = std::fs::read_to_string(&archive_toml).unwrap().replacen(
            "[paths]\n",
            &format!("[paths]\ncold_roots = {}\n", cold_roots_toml(&cold)),
            1,
        );
        std::fs::write(&archive_toml, text).unwrap();
        let mut config = history_config(dir, "2026-10-03", "2026-10-06");
        config.history.as_mut().unwrap().max_run_records = Some(1_000);
        config.paths.cold_roots = cold;
        let (lines, records) = journaled_run(
            "20261006T010000Z",
            CUTOVER + HOUR,
            CUTOVER + HOUR + 60_000,
            500,
            &[
                CUTOVER + HOUR,
                CUTOVER + HOUR + 1_000,
                CUTOVER + HOUR + 2_000,
            ],
        );
        write_records(&config, &records, &lines);

        let single_config = super::common::config(single);
        copy_tree(&config.paths.raw_root, &single_config.paths.raw_root);
        std::fs::copy(&config.paths.journal, &single_config.paths.journal).unwrap();
        init_tiers(&config.paths.raw_root, &config.paths.cold_roots);
        (config, single_config)
    }

    fn frames() -> Vec<Vec<String>> {
        vec![vec![agg(1, T + 10), agg(2, T + 20), agg(3, T + 30)]]
    }

    /// The `run_start` line of the last run in the journal of `config`.
    fn last_run_start(config: &IngestConfig) -> Value {
        journal(config)
            .into_iter()
            .rfind(|line| line["type"] == "run_start")
            .unwrap()
    }

    #[test]
    fn a_readable_ledger_gives_the_single_root_seeds() {
        let (dir, single) = (TempDir::new("tiered-seeds"), TempDir::new("tiered-seeds-1"));
        let (tiered, single_config) = tiered_chain(dir.path(), single.path());
        // Without `[history]`: the seeds alone are compared here.
        let mut plain = tiered.clone();
        plain.history = None;
        let outcome = ingest(&plain, T, frames());
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        let reference = ingest(&single_config, T, frames());
        assert_eq!(reference.exit_code(), 0, "{:?}", reference.error);

        let (start, reference) = (last_run_start(&plain), last_run_start(&single_config));
        assert_ne!(reference["seeds"], serde_json::json!({}));
        assert_eq!(start["seeds"], reference["seeds"]);
        assert_eq!(start["seed_listing"], "union");
        assert!(start.get("seed_listing_error").is_none(), "{start}");
        assert!(reference.get("seed_listing").is_none(), "{reference}");
    }

    /// An HTTP download that must not be called.
    struct NoDownload;

    impl HttpDownload for NoDownload {
        fn download(&self, url: &str, _: &mut dyn std::io::Write) -> Result<u16, String> {
            panic!("unexpected download of {url}")
        }
    }

    /// The degraded start after `damage` made the copy ledger unusable:
    /// the capture runs with the single-root seeds from hot's listing, the
    /// warm-up starts cold, the importer refuses and a live replay fails,
    /// each naming the ledger. `union_error` is how the union `select`'s
    /// error displays (its kind).
    fn assert_degraded_start(tag: &str, union_error: &str, damage: impl FnOnce(&Path)) {
        degraded_start(tag, union_error, |_| (), damage);
    }

    /// [`assert_degraded_start`], with `before` run on the intact store
    /// before the damage; returns the directories (kept alive), the
    /// tiered configuration and what `before` returned.
    fn degraded_start<R>(
        tag: &str,
        union_error: &str,
        before: impl FnOnce(&IngestConfig) -> R,
        damage: impl FnOnce(&Path),
    ) -> (TempDir, TempDir, IngestConfig, R) {
        let (dir, single) = (TempDir::new(tag), TempDir::new(&format!("{tag}-1")));
        let (config, single_config) = tiered_chain(dir.path(), single.path());
        let seen = before(&config);
        damage(&copy_ledger::ledger_path(&config.paths.raw_root));
        let ledger = format!(
            "copy ledger {}",
            copy_ledger::ledger_path(&config.paths.raw_root).display()
        );

        let outcome = ingest(&config, T, frames());
        let cold = super::assert_cold(&config, &outcome, "warm-up-failed");
        let error = cold["error"].as_str().unwrap();
        assert!(error.contains(&ledger), "{error}");

        let reference = ingest(&single_config, T, frames());
        assert_eq!(reference.exit_code(), 0, "{:?}", reference.error);
        let (start, reference) = (last_run_start(&config), last_run_start(&single_config));
        assert_ne!(reference["seeds"], serde_json::json!({}));
        assert_eq!(start["seeds"], reference["seeds"]);
        assert_eq!(start["seed_listing"], "hot");
        let listing_error = start["seed_listing_error"].as_str().unwrap();
        assert!(
            listing_error.starts_with(union_error) && listing_error.contains(&ledger),
            "{listing_error}"
        );

        // The importer refuses before it opens anything, dry runs too.
        let archive_config = ArchiveConfig::parse(
            &std::fs::read_to_string(dir.path().join("archive.toml")).unwrap(),
        )
        .unwrap();
        let transports = ArchiveTransports {
            http: Arc::new(NoHttp),
            download: Arc::new(NoDownload),
            clock: FakeClock::new(T, "nobody"),
        };
        let before = tree(dir.path());
        for dry_run in [true, false] {
            let request = ImportRequest {
                from_day: CUTOVER / DAY_MS - 1,
                to_day: CUTOVER / DAY_MS - 1,
                streams: None,
                dry_run,
            };
            let error = archive::import(
                &archive_config,
                &request,
                &transports,
                &AtomicBool::new(false),
                &mut Vec::new(),
            )
            .expect_err("refused");
            assert!(
                error.contains(&ledger) && error.contains("copy-ledger rebuild"),
                "{error}"
            );
            assert_eq!(tree(dir.path()), before);
        }

        // A live replay fails on the same store.
        let mut out = Vec::new();
        let replayed = replay::run(
            &ReplayRequest {
                source: ReplaySource::Live(config.clone()),
                window: ReplayWindow {
                    start: EventTime::from_millis(CUTOVER),
                    end: EventTime::from_millis(CUTOVER + DAY_MS),
                },
            },
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(!replayed.pass, "{text}");
        assert!(
            text.contains(union_error) && text.contains(&ledger),
            "{text}"
        );
        (dir, single, config, seen)
    }

    const INTEGRITY: &str = "raw store integrity check failed";

    /// The live replay of `config` over `[CUTOVER, end)`.
    fn live_replay(config: &IngestConfig, end: i64) -> (bool, String) {
        let mut out = Vec::new();
        let replayed = replay::run(
            &ReplayRequest {
                source: ReplaySource::Live(config.clone()),
                window: ReplayWindow {
                    start: EventTime::from_millis(CUTOVER),
                    end: EventTime::from_millis(end),
                },
            },
            &mut out,
        )
        .unwrap();
        (replayed.pass, String::from_utf8(out).unwrap())
    }

    /// The live replay of the prior run's hour, before the degraded
    /// capture's.
    fn replay_before_t(config: &IngestConfig) -> (bool, String) {
        live_replay(config, CUTOVER + HOUR + 30 * 60_000)
    }

    /// AW8 offline (#193): after the degraded start (the capture runs, the
    /// warm-up fails naming the ledger, a replay fails with `Integrity`),
    /// `mie store rebuild` restores the ledger, and the live replay equals
    /// the one taken before the damage.
    #[test]
    fn aw8_degraded_capture_then_rebuild() {
        let (dir, single, config, before) =
            degraded_start("tiered-aw8", INTEGRITY, replay_before_t, |ledger| {
                std::fs::remove_file(ledger).unwrap();
            });
        assert!(before.0, "{}", before.1);
        let rebuild = |record_unlisted: bool| {
            let mut out = Vec::new();
            let pass = mie_cli::store::rebuild(
                &config,
                Some(&dir.path().join("archive.toml")),
                mie_cli::store::rebuild::Flags {
                    record_unlisted,
                    ..mie_cli::store::rebuild::Flags::default()
                },
                &mie_cli::ingest::first_hot_date(T),
                super::common::probe_for(&config).as_ref(),
                &|| T + HOUR,
                &mut out,
            )
            .unwrap();
            (pass, String::from_utf8(out).unwrap())
        };
        // The fixture seals its archive rows without the importer, so no
        // import ledger exists: with archive parts on hot that is a stop
        // (review of #209, finding 2). An importer that ran creates it.
        let (pass, text) = rebuild(false);
        assert!(!pass && text.contains("nothing written"), "{text}");
        assert!(text.contains("does not exist"), "{text}");
        std::fs::create_dir_all(dir.path().join("ledger")).unwrap();
        // With the directory, its archive parts are listed by no entry: the
        // first run names them and writes nothing, the second records the
        // holds (--record-unlisted).
        let (pass, text) = rebuild(false);
        assert!(!pass && text.contains("nothing written"), "{text}");
        assert!(text.contains("--record-unlisted"), "{text}");
        assert!(!copy_ledger::ledger_path(&config.paths.raw_root).exists());
        let (pass, text) = rebuild(true);
        assert!(
            text.contains("started empty") && text.contains("wrote "),
            "{text}"
        );
        // The fixture seals its archive rows without import entries, so the
        // rebuild holds them (ADR-062 D11, unlisted); the live source has
        // no finding.
        let fails: Vec<&str> = text.lines().filter(|l| l.starts_with("FAIL: ")).collect();
        assert!(!pass && !fails.is_empty(), "{text}");
        assert!(
            fails
                .iter()
                .all(|l| l.contains("source=binance-archive/") && l.contains("held (unlisted)")),
            "{text}"
        );
        assert_eq!(replay_before_t(&config), before);
        // The whole day, the degraded capture included, replays as the
        // single-root reference that ran the same capture (its trade ids
        // restart below the prior run's, so both report the same domain
        // rejections).
        let reference = super::common::config(single.path());
        assert_eq!(
            live_replay(&config, CUTOVER + DAY_MS),
            live_replay(&reference, CUTOVER + DAY_MS)
        );
    }

    #[test]
    fn a_missing_ledger_is_the_degraded_start() {
        assert_degraded_start("tiered-no-ledger", INTEGRITY, |ledger| {
            std::fs::remove_file(ledger).unwrap();
        });
    }

    #[test]
    fn a_ledger_damaged_before_its_last_record_is_the_degraded_start() {
        assert_degraded_start("tiered-damaged-ledger", INTEGRITY, |ledger| {
            let raw_root = ledger.parent().unwrap().parent().unwrap();
            for at_ms in [1, 2] {
                CopyLedger::at(raw_root)
                    .append(&Record {
                        at_ms,
                        kind: Kind::Removed {
                            partition: "source=binance-um/instrument=BTCUSDT/stream=aggTrade/\
                                        date=2026-09-01"
                                .to_owned(),
                        },
                    })
                    .unwrap();
            }
            let mut bytes = std::fs::read(ledger).unwrap();
            bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
            std::fs::write(ledger, bytes).unwrap();
        });
    }

    /// QA of #198: a `Corrupt` union `select` falls back to hot's listing
    /// too (#188 STOP 1 decision 1). The ledger's header names an unknown
    /// format version, which the ledger read reports as `Corrupt`. A
    /// garbage manifest on hot cannot show this: hot's listing would fail
    /// on it as well, and the start then fails as before.
    #[test]
    fn a_corrupt_union_select_is_the_degraded_start() {
        assert_degraded_start(
            "tiered-corrupt-ledger",
            "raw store data is corrupt",
            |ledger| {
                std::fs::write(ledger, "mie-copy-ledger 9\n").unwrap();
            },
        );
    }

    // ------------------------------------- AW2 after an adoption (#190)

    use super::common::MapProbe;
    use mie_cli::store::{self, AdoptRequest};

    /// A closed single-root store under `dir/src` with one clean run per
    /// entry of `runs` (start, records' times), adopted with
    /// `first_hot_date` into the tiered store of [`tiered`] under `dir`.
    /// Before the adoption the source is copied to `single`, with a
    /// single-root configuration over the copy: the reference start.
    fn adopted(
        dir: &Path,
        single: &Path,
        first_hot_date: &str,
        runs: &[(i64, &[i64])],
    ) -> (IngestConfig, IngestConfig) {
        let source = config(&dir.join("src"));
        let (mut lines, mut records) = (Vec::new(), Vec::new());
        for (i, (start, times)) in runs.iter().enumerate() {
            let run_id = format!("2026100{i}T000000Z");
            let (l, r) = journaled_run(
                &run_id,
                *start,
                times.last().unwrap() + 1_000,
                500 + 100 * i as u64,
                times,
            );
            lines.extend(l);
            records.extend(r);
        }
        write_records(&source, &records, &lines);
        let single_config = config(single);
        copy_tree(&source.paths.raw_root, &single_config.paths.raw_root);
        std::fs::copy(&source.paths.journal, &single_config.paths.journal).unwrap();

        let config = tiered(dir);
        init_tiers(&config.paths.raw_root, &config.paths.cold_roots);
        let probe = MapProbe(vec![
            (config.paths.raw_root.clone(), 1),
            (dir.join("cold-a"), 2),
            (dir.join("cold-b"), 3),
        ]);
        let request = AdoptRequest {
            from: source.paths.raw_root.clone(),
            journal: Some(source.paths.journal.clone()),
            import_ledger: None,
            archive_config: None,
        };
        let mut out = Vec::new();
        let pass =
            store::adopt(&config, &request, first_hot_date, &probe, &|| T, &mut out).unwrap();
        assert!(pass, "{}", String::from_utf8_lossy(&out));
        (config, single_config)
    }

    /// The seeds and listing of a start at T on `config`, and the seeds of
    /// the reference start on `single`.
    fn starts(config: &IngestConfig, single: &IngestConfig) -> (Value, Value) {
        let outcome = ingest(config, T, frames());
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        assert_eq!(
            outcome.summary.unwrap().stats.records,
            3,
            "the capture runs"
        );
        let reference = ingest(single, T, frames());
        assert_eq!(reference.exit_code(), 0, "{:?}", reference.error);
        (last_run_start(config), last_run_start(single))
    }

    /// AW2 (a) and (b) offline: after an adoption, `mie ingest` starts with
    /// both copy targets gone, with one unmounted (an empty directory
    /// without its marker), and with the copy ledger deleted on the intact
    /// hot, captures, and journals the seeds of a single-root start over
    /// the source; the listing is `union`, `union` and `hot`. The
    /// replication thread runs throughout (#191): with cold-a unmounted its
    /// `replication` lines name cold-a as skipped, and cold-b receives the
    /// newly sealed files.
    #[test]
    fn after_an_adoption_the_capture_starts_with_the_single_root_seeds() {
        let runs: [(i64, &[i64]); 2] = [
            (
                T - 5 * DAY_MS,
                &[T - 5 * DAY_MS + 1_000, T - 5 * DAY_MS + 2_000],
            ),
            (T - DAY_MS, &[T - DAY_MS + 1_000, T - DAY_MS + 3_000]),
        ];
        let first_hot = mie_cli::ingest::first_hot_date(T);
        for case in ["targets gone", "one target unmounted", "ledger deleted"] {
            let (dir, single) = (TempDir::new("adopted-aw2"), TempDir::new("adopted-aw2-1"));
            let (config, single_config) = adopted(dir.path(), single.path(), &first_hot, &runs);
            if case == "targets gone" {
                // Both gone is stronger than one unreadable: the union
                // `select` never lists a cold root.
                for name in ["cold-a", "cold-b"] {
                    let root = dir.path().join(name);
                    std::fs::rename(&root, root.with_extension("aside")).unwrap();
                }
            } else if case == "one target unmounted" {
                let root = dir.path().join("cold-a");
                std::fs::rename(&root, root.with_extension("aside")).unwrap();
                std::fs::create_dir(&root).unwrap();
            } else {
                std::fs::remove_file(copy_ledger::ledger_path(&config.paths.raw_root)).unwrap();
            }
            let (start, reference) = starts(&config, &single_config);
            assert_ne!(reference["seeds"], serde_json::json!({}), "{case}");
            assert_eq!(start["seeds"], reference["seeds"], "{case}");
            if case == "one target unmounted" {
                let lines = journal(&config);
                let replication: Vec<&Value> = lines
                    .iter()
                    .filter(|l| l["type"] == "replication")
                    .collect();
                assert!(!replication.is_empty(), "{case}");
                let cold_a = dir.path().join("cold-a").display().to_string();
                for line in &replication {
                    let first = line["first_problem"].as_str().unwrap();
                    assert!(
                        first.contains(&cold_a) && first.contains("skipped for this pass"),
                        "{line}"
                    );
                }
                let sealed: Vec<&str> = lines
                    .iter()
                    .filter(|l| l["type"] == "sealed")
                    .flat_map(|l| l["files"].as_array().unwrap())
                    .map(|f| f["path"].as_str().unwrap())
                    .collect();
                assert!(!sealed.is_empty(), "{case}");
                for path in sealed {
                    assert!(dir.path().join("cold-b").join(path).exists(), "{path}");
                }
            }
            if case != "ledger deleted" {
                assert_eq!(start["seed_listing"], "union", "{case}: {start}");
            } else {
                assert_eq!(start["seed_listing"], "hot", "{case}: {start}");
                let error = start["seed_listing_error"].as_str().unwrap();
                assert!(error.contains("copy ledger"), "{error}");
            }
        }
    }

    /// AW2's short-window variant: a hot window shorter than the seed
    /// lookback (first hot date T − 2 d) removes the partition of a stream
    /// quiet since T − 5 d. With the ledger the union keeps its seed; the
    /// degraded start lists hot only and misses it, which is why D8 ties
    /// the hot window to the lookback.
    #[test]
    fn a_hot_window_shorter_than_the_lookback_loses_a_seed_in_the_degraded_start() {
        let quiet = T - 5 * DAY_MS;
        let runs: [(i64, &[i64]); 1] = [(quiet, &[quiet + 1_000, quiet + 2_000])];
        // The date of T − 2 d; the production window starts at T − 7 d.
        let first_hot = "2026-10-04";
        assert_eq!(mie_cli::ingest::first_hot_date(T), "2026-09-29");
        for ledger in ["intact", "deleted"] {
            let (dir, single) = (
                TempDir::new("adopted-short"),
                TempDir::new("adopted-short-1"),
            );
            let (config, single_config) = adopted(dir.path(), single.path(), first_hot, &runs);
            let raw = &config.paths.raw_root;
            assert!(
                !raw.join("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-01")
                    .exists()
            );
            if ledger == "deleted" {
                std::fs::remove_file(copy_ledger::ledger_path(raw)).unwrap();
            }
            let (start, reference) = starts(&config, &single_config);
            assert_eq!(
                reference["seeds"]["aggTrade"],
                serde_json::json!(quiet + 2_000),
                "{reference}"
            );
            if ledger == "intact" {
                assert_eq!(start["seeds"], reference["seeds"]);
                assert_eq!(start["seed_listing"], "union");
            } else {
                assert_eq!(start["seeds"], serde_json::json!({}), "{start}");
                assert_eq!(start["seed_listing"], "hot");
            }
        }
    }

    // ------------------------------- replication and removal (#191)

    use mie_adapter_binance::transport::{Clock, ReadOutcome, WsConnection, WsConnector};
    use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
    use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
    use std::time::{Duration, Instant};

    /// Real time from a fixed UTC epoch: seal passes come from the wall
    /// clock, as in production.
    struct ShiftedClock {
        base_ns: i64,
        start: Instant,
    }

    impl Clock for ShiftedClock {
        fn now_utc_ns(&self) -> i64 {
            self.base_ns + self.start.elapsed().as_nanos() as i64
        }

        fn monotonic_ns(&self) -> u64 {
            self.start.elapsed().as_nanos() as u64
        }

        fn sleep(&self, d: Duration) {
            std::thread::sleep(d);
        }
    }

    /// Plays `frames`, then waits (real time) until `done` holds or 30 s
    /// pass, then asks for shutdown.
    struct UntilConnector {
        frames: std::sync::Mutex<Option<Vec<String>>>,
        done: Arc<dyn Fn() -> bool + Send + Sync>,
        shutdown: Arc<AtomicBool>,
    }

    struct UntilConnection {
        frames: std::collections::VecDeque<String>,
        done: Arc<dyn Fn() -> bool + Send + Sync>,
        shutdown: Arc<AtomicBool>,
        deadline: Instant,
    }

    impl WsConnector for UntilConnector {
        fn connect(&self, _: &str) -> Result<Box<dyn WsConnection>, String> {
            let frames = self
                .frames
                .lock()
                .unwrap()
                .take()
                .ok_or("script exhausted")?;
            Ok(Box::new(UntilConnection {
                frames: frames.into(),
                done: Arc::clone(&self.done),
                shutdown: Arc::clone(&self.shutdown),
                deadline: Instant::now() + Duration::from_secs(30),
            }))
        }
    }

    impl WsConnection for UntilConnection {
        fn read(&mut self) -> ReadOutcome {
            if let Some(frame) = self.frames.pop_front() {
                return ReadOutcome::Frame(frame.into_bytes());
            }
            if (self.done)() || Instant::now() >= self.deadline {
                self.shutdown
                    .store(true, std::sync::atomic::Ordering::Relaxed);
            }
            std::thread::sleep(Duration::from_millis(20));
            ReadOutcome::Timeout
        }

        fn ping(&mut self) -> Result<(), String> {
            Ok(())
        }

        fn close(&mut self) {}
    }

    /// A real-time capture of `config` from `base_ms`, until `done`.
    fn capture_until(
        config: &IngestConfig,
        base_ms: i64,
        done: Arc<dyn Fn() -> bool + Send + Sync>,
    ) -> mie_cli::ingest::IngestOutcome {
        let shutdown = Arc::new(AtomicBool::new(false));
        let transports = Transports {
            connector: Arc::new(UntilConnector {
                frames: std::sync::Mutex::new(Some(vec![
                    agg(1, base_ms + 10),
                    agg(2, base_ms + 20),
                ])),
                done,
                shutdown: Arc::clone(&shutdown),
            }),
            http: Arc::new(NoHttp),
            clock: Arc::new(ShiftedClock {
                base_ns: base_ms * 1_000_000,
                start: Instant::now(),
            }),
            device_probe: super::common::probe_for(config),
        };
        mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
    }

    /// A tiered store under `dir` (1 s seal interval) whose hot holds a
    /// live `aggTrade` partition ten days before `base_ms`, copied to both
    /// targets by one pass; returns the configuration and the partition.
    fn tiered_with_old_partition(dir: &Path, base_ms: i64) -> (IngestConfig, String) {
        let mut config = tiered(dir);
        config.capture.seal_interval_secs = 1;
        init_tiers(&config.paths.raw_root, &config.paths.cold_roots);
        let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let old = base_ms - 10 * DAY_MS;
        let mut writer = ParquetRawStore::new(&config.paths.raw_root)
            .writer("binance-um", RotationPolicy::default())
            .unwrap();
        for i in 0..3 {
            writer
                .append(
                    &key,
                    RawRecord {
                        event_time: EventTime::from_millis(old + i * 1_000),
                        capture: None,
                        payload: format!("old-{i}").into_bytes(),
                    },
                )
                .unwrap();
        }
        let sealed = writer.close().unwrap();
        let partition = sealed[0]
            .relative_path
            .rsplit_once('/')
            .unwrap()
            .0
            .to_owned();
        let mut out = Vec::new();
        let probe = super::common::probe_for(&config);
        let pass = store::replicate(
            &config,
            None,
            probe.as_ref(),
            &|| base_ms - DAY_MS,
            &mut out,
        )
        .unwrap();
        assert!(pass, "{}", String::from_utf8_lossy(&out));
        (config, partition)
    }

    /// The selection of the old date.
    fn old_day(base_ms: i64) -> RawSelection {
        let start = (base_ms - 10 * DAY_MS).div_euclid(DAY_MS) * DAY_MS;
        RawSelection::new(
            [RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap()].into(),
            ReplayWindow {
                start: EventTime::from_millis(start),
                end: EventTime::from_millis(start + DAY_MS),
            },
        )
        .unwrap()
    }

    /// #191: a tiered capture replicates after its seal passes and removes
    /// a live partition before the hot window once both targets and both
    /// ledger copies hold it: a `replication` line passed, a `removal` line
    /// names the partition, the ledger records it removed, its directory
    /// is gone, and a `select` over the old date is unchanged.
    #[test]
    fn a_tiered_capture_replicates_and_removes_old_partitions() {
        let dir = TempDir::new("tiered-remove");
        let base = D0 + HOUR;
        let (config, partition) = tiered_with_old_partition(dir.path(), base);
        let store =
            mie_cli::store::raw_store(&config.paths.raw_root, &config.paths.cold_roots).unwrap();
        let before = store.select(&old_day(base)).unwrap();
        assert_eq!(before.files.len(), 1);

        let gone = config.paths.raw_root.join(&partition);
        let outcome = capture_until(&config, base, Arc::new(move || !gone.exists()));
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        assert_eq!(outcome.summary.unwrap().stats.records, 2);

        let lines = journal(&config);
        assert!(
            lines
                .iter()
                .any(|l| l["type"] == "replication" && l["passed"] == true),
            "{lines:?}"
        );
        let removal: Vec<&Value> = lines.iter().filter(|l| l["type"] == "removal").collect();
        assert!(
            removal
                .iter()
                .any(|l| l["source"] == "binance-um"
                    && l["removed"] == serde_json::json!([partition])),
            "{removal:?}"
        );
        assert!(!config.paths.raw_root.join(&partition).exists());
        let records = copy_ledger::read(&config.paths.raw_root)
            .unwrap()
            .unwrap()
            .records;
        assert!(records.iter().any(|r| matches!(
            &r.kind,
            Kind::Removed { partition: p } if *p == partition
        )));
        assert_eq!(store.select(&old_day(base)).unwrap(), before);
        // The run_end comes before the final pass's line.
        let end = lines.iter().position(|l| l["type"] == "run_end").unwrap();
        assert!(
            lines[end + 1..]
                .iter()
                .all(|l| l["type"] == "replication" || l["type"] == "copy_ledger_truncated")
        );
    }

    /// #191: a copy target whose marker is a FIFO blocks the replication
    /// thread like a hung mount; the capture still records and seals, ends
    /// with exit code 0, and `ingest` returns within the bound (one seal
    /// interval after `run_end`). The blocked thread ends with the test
    /// binary.
    #[test]
    fn a_hung_copy_target_never_stalls_the_capture() {
        let dir = TempDir::new("tiered-hung");
        let base = D0 + HOUR;
        let (config, _) = tiered_with_old_partition(dir.path(), base);
        let marker = root_marker::marker_path(&dir.path().join("cold-a"));
        std::fs::remove_file(&marker).unwrap();
        let made = std::process::Command::new("mkfifo")
            .arg(&marker)
            .status()
            .unwrap();
        assert!(made.success());
        let started = Instant::now();
        let deadline = Instant::now() + Duration::from_secs(3);
        let outcome = capture_until(&config, base, Arc::new(move || Instant::now() >= deadline));
        assert!(
            started.elapsed() < Duration::from_secs(15),
            "{:?}",
            started.elapsed()
        );
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        assert_eq!(outcome.summary.unwrap().stats.records, 2);
        let lines = journal(&config);
        let end = lines.iter().find(|l| l["type"] == "run_end").unwrap();
        assert_eq!(end["exit_code"], 0);
        assert!(lines.iter().any(|l| l["type"] == "sealed"), "{lines:?}");
    }

    /// AW2 (b), #191: without the copy ledger the replication passes name
    /// it and refuse, and nothing is removed.
    #[test]
    fn a_capture_without_the_ledger_journals_the_refusal_and_removes_nothing() {
        let dir = TempDir::new("tiered-no-ledger-pass");
        let base = D0 + HOUR;
        let (config, partition) = tiered_with_old_partition(dir.path(), base);
        let ledger = copy_ledger::ledger_path(&config.paths.raw_root);
        std::fs::remove_file(&ledger).unwrap();
        let outcome = ingest(&config, base, vec![vec![agg(1, base + 10)]]);
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        let lines = journal(&config);
        let replication: Vec<&Value> = lines
            .iter()
            .filter(|l| l["type"] == "replication")
            .collect();
        assert!(!replication.is_empty());
        for line in replication {
            assert_eq!(line["passed"], false);
            let first = line["first_problem"].as_str().unwrap();
            assert!(
                first.contains(&format!("copy ledger {} does not exist", ledger.display())),
                "{first}"
            );
            assert_eq!(line["candidates"], Value::Null);
        }
        assert!(lines.iter().all(|l| l["type"] != "removal"));
        assert!(config.paths.raw_root.join(&partition).exists());
    }

    /// #191: `read_runs` and `mie capture-report` give the same result over
    /// a tiered journal, whose `replication` lines land after `run_end`, as
    /// over the same journal without them.
    #[test]
    fn the_new_line_kinds_change_no_run_and_no_report() {
        let dir = TempDir::new("tiered-lines");
        let base = D0 + HOUR;
        let (config, _) = tiered_with_old_partition(dir.path(), base);
        let outcome = ingest(
            &config,
            base,
            vec![vec![agg(1, base + 10), agg(2, base + 20)]],
        );
        assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
        let text = std::fs::read_to_string(&config.paths.journal).unwrap();
        assert!(text.contains("\"type\":\"replication\""), "{text}");
        let plain: String = text
            .lines()
            .filter(|l| {
                !["replication", "removal", "recall", "copy_ledger_truncated"]
                    .iter()
                    .any(|kind| l.contains(&format!("\"type\":\"{kind}\"")))
            })
            .map(|l| format!("{l}\n"))
            .collect();
        let mut stripped = config.clone();
        stripped.paths.journal = dir.path().join("plain.jsonl");
        std::fs::write(&stripped.paths.journal, plain).unwrap();
        assert_eq!(
            mie_cli::journal::read_runs(&config.paths.journal, "binance-um").unwrap(),
            mie_cli::journal::read_runs(&stripped.paths.journal, "binance-um").unwrap()
        );
        let report = |config: &IngestConfig| {
            let mut out = Vec::new();
            let pass =
                mie_cli::report::run(config, base - HOUR, base + HOUR, 0.0, &mut out).unwrap();
            (pass, String::from_utf8(out).unwrap())
        };
        assert_eq!(report(&config), report(&stripped));
    }
}
