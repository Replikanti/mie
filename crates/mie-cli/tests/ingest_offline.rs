//! The full `mie ingest` composition offline: scripted WebSocket frames
//! into a real Parquet raw store, journal and core.

mod common;

use common::{D0, HOUR, TempDir, agg, config, ingest, journal};
use mie_adapter_binance::{BinanceStream, Pipeline};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::RunParameters;
use mie_domain::event::MarketEvent;
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
    let mut records = Vec::new();
    for file in store.select(&selection).unwrap().files {
        records.extend(store.read(&file).unwrap());
    }
    let lines = journal(&config);
    for start in lines.iter().filter(|l| l["type"] == "run_start") {
        let params = RunParameters::from_run_start(start).unwrap();
        // Configured, not the default: the recompute must use run_start.
        assert_eq!(params.hold_back_ms, 750);
        let mut run: Vec<_> = records
            .iter()
            .filter(|r| {
                let session = &r.capture.as_ref().unwrap().session_id;
                session.starts_with(&format!("{}/", params.run_id))
            })
            .collect();
        run.sort_by_key(|r| r.capture.as_ref().unwrap().receive_seq);
        let mut pipeline = Pipeline::new(&params.symbol, params.hold_back_ms, &params.seeds);
        let mut events = Vec::new();
        for record in run {
            events.extend(pipeline.push(BinanceStream::AggTrade, record).events);
        }
        events.extend(pipeline.finish());
        let recomputed: Vec<_> = events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => Some((
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
                    l["reason"].as_str().unwrap().to_owned(),
                    l["start"].as_i64().unwrap(),
                    l["end"].as_i64().unwrap(),
                )
            })
            .collect();
        assert!(!journaled.is_empty());
        assert_eq!(recomputed, journaled, "run {}", params.run_id);
    }
    // Run 2 opens with the seed gap, which only run_start can reproduce.
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
}
