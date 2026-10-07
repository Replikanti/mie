//! The recorded live sync window (`tests/fixtures/depth*.jsonl`) through the
//! pipeline: the book syncs on the first snapshot, chains, re-anchors on the
//! second, and the audit matches the rebuilt book against it. This pins the
//! ADR-038 assumption that a REST snapshot's `T` is the time of its
//! `lastUpdateId`, on real data.

use mie_adapter_binance::book_sync::{BookTransition, SnapshotRejection};
use mie_adapter_binance::normalize::record_time;
use mie_adapter_binance::{BinanceStream, BookAudit, CheckpointResult, Pipeline};
use mie_domain::book::OrderBook;
use mie_domain::event::MarketEvent;
use mie_domain::state::MarketStateEngine;
use mie_ports::raw::{Capture, RawRecord};
use std::collections::BTreeMap;

const RUN: &str = "20261007T191737Z";

fn lines(stream: BinanceStream) -> Vec<String> {
    let path = format!(
        "{}/tests/fixtures/{}.jsonl",
        env!("CARGO_MANIFEST_DIR"),
        stream.raw_name()
    );
    std::fs::read_to_string(path)
        .expect("read fixture")
        .lines()
        .map(str::to_owned)
        .collect()
}

/// The capture's records in arrival order: snapshot 1 after diff 5,
/// snapshot 2 after diff 27 (see the fixture README).
fn records() -> Vec<(BinanceStream, RawRecord)> {
    let diffs = lines(BinanceStream::Depth);
    let snapshots = lines(BinanceStream::DepthSnapshot);
    let mut arrival: Vec<(BinanceStream, &str, String)> = Vec::new();
    for (i, diff) in diffs.iter().enumerate() {
        arrival.push((BinanceStream::Depth, "depth", diff.clone()));
        let snapshot = match i + 1 {
            5 => Some(&snapshots[0]),
            27 => Some(&snapshots[1]),
            _ => None,
        };
        if let Some(snapshot) = snapshot {
            arrival.push((
                BinanceStream::DepthSnapshot,
                "depthSnapshot",
                snapshot.clone(),
            ));
        }
    }
    arrival
        .into_iter()
        .enumerate()
        .map(|(seq, (stream, name, payload))| {
            let record = RawRecord {
                event_time: record_time(stream, payload.as_bytes()).expect("record time"),
                capture: Some(Capture {
                    receive_time_ns: 0,
                    receive_seq: seq as u64,
                    session_id: format!("{RUN}/{name}/1"),
                }),
                payload: payload.into_bytes(),
            };
            (stream, record)
        })
        .collect()
}

#[test]
fn the_recorded_window_syncs_chains_and_matches_its_checkpoint() {
    let records = records();
    assert_eq!(records.len(), 31);
    // The defaults of a live capture.
    let mut pipeline = Pipeline::new("BTCUSDT", 2_000, 10_000, &BTreeMap::new());
    let mut events = Vec::new();
    let mut transitions = Vec::new();
    for (stream, record) in &records {
        let pushed = pipeline.push(*stream, record);
        assert_eq!(pushed.error, None);
        events.extend(pushed.events);
        transitions.extend(
            pushed
                .book
                .into_iter()
                .map(|t| (record.capture.as_ref().unwrap().receive_seq, t)),
        );
    }
    events.extend(pipeline.finish());

    let first = 11_759_094_709_820;
    let second = 11_759_095_019_149;
    let seqs: Vec<(u64, BookTransition)> = transitions;
    assert_eq!(seqs.len(), 3, "{seqs:?}");
    assert!(matches!(seqs[0], (0, BookTransition::Desynced(_))));
    // Snapshot 1 waits for diff 6, which straddles it.
    assert!(matches!(
        seqs[1],
        (6, BookTransition::Synced { last_update_id, .. }) if last_update_id == first
    ));
    // Snapshot 2 arrives before its straddling diff (28) and re-anchors on it.
    assert!(matches!(
        seqs[2],
        (29, BookTransition::CheckpointEmitted { last_update_id, .. }) if last_update_id == second
    ));
    assert!(
        !seqs.iter().any(|(_, t)| matches!(
            t,
            BookTransition::SnapshotRejected(SnapshotRejection::TimeOrder)
        )),
        "a snapshot's T orders before its straddling diff"
    );

    // No gap on the first sync of an unseeded run; two snapshots and the 24
    // diffs from the straddling one on.
    assert!(!events.iter().any(|e| matches!(e, MarketEvent::FeedGap(_))));
    let snapshots = events
        .iter()
        .filter(|e| matches!(e, MarketEvent::BookSnapshot(_)))
        .count();
    let updates = events
        .iter()
        .filter(|e| matches!(e, MarketEvent::BookUpdate(_)))
        .count();
    assert_eq!((snapshots, updates), (2, 24));

    let mut engine = MarketStateEngine::new();
    let mut book = OrderBook::new();
    let mut audit = BookAudit::new();
    let mut checkpoints = Vec::new();
    for (i, event) in events.iter().enumerate() {
        engine
            .apply(event)
            .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
        book.apply(event);
        assert!(book.is_valid(), "event {i}");
        checkpoints.extend(audit.apply(event));
    }
    assert_eq!(checkpoints.len(), 1);
    let CheckpointResult::Matched { levels, .. } = checkpoints[0] else {
        panic!("{:?}", checkpoints[0]);
    };
    assert!(levels > 100, "{levels}");

    let stats = pipeline.stats();
    assert_eq!(stats.book.syncs, 1);
    assert_eq!(stats.book.stale_diffs, 5);
    assert_eq!(stats.book.checkpoints_emitted, 1);
    assert_eq!(stats.streams[&BinanceStream::Depth].records, 29);
}
