//! The recorded live sync window (`tests/fixtures/depth*.jsonl`) through the
//! pipeline: the book syncs on the first snapshot, chains, re-anchors on the
//! second, and the audit matches the rebuilt book against it. This pins the
//! ADR-038 assumption that a REST snapshot's `T` is the time of its
//! `lastUpdateId`, on real data, and the engine's book features on it
//! (ADR-043): which depth bands a `limit=100` window can know.

use mie_adapter_binance::book_sync::{BookTransition, SnapshotRejection};
use mie_adapter_binance::normalize::record_time;
use mie_adapter_binance::{BinanceStream, BookAudit, CheckpointResult, Pipeline};
use mie_domain::book::OrderBook;
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureValue, Unavailability};
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
    // The engine's book (ADR-043 D1) is ready at the last diff's `u`.
    let book = &engine.state().book;
    let l2 = book.l2.ready().expect("book.l2@1 is ready");
    let last_u = events
        .iter()
        .rev()
        .find_map(|e| match e {
            MarketEvent::BookUpdate(u) => Some(u.last_update_id),
            _ => None,
        })
        .unwrap();
    assert_eq!(l2.last_update_id(), Some(last_u));
    assert_eq!(l2, &book_after(&events));
    // The fixture's snapshots are `limit=100`: the trusted window reaches
    // 14.2 USDT below the mid on the bid side and 12.1 USDT above on the ask
    // side, about 1.7 and 1.45 bps. So the 1 bps band is known, 2 and 5 bps
    // reach beyond the window, and so do the 5 bps clusters of both sides.
    let window = l2.window();
    assert_eq!(
        (window.lowest_bid, window.highest_ask),
        (
            Some("83398.2".parse().unwrap()),
            Some("83424.6".parse().unwrap())
        )
    );
    let depth = book.depth.ready().expect("book.depth@1 is ready");
    let inner = depth.bands[0].ready().expect("1 bps is in range");
    assert_eq!((inner.bid_levels, inner.ask_levels), (54, 66));
    let out_of_range = Unavailability::OutOfRange;
    for band in &depth.bands[1..] {
        assert_eq!(
            band,
            &FeatureValue::Unavailable {
                reason: out_of_range
            }
        );
    }
    let clusters = book.clusters.ready().expect("book.clusters@1 is ready");
    for side in [clusters.bid, clusters.ask] {
        assert_eq!(
            side,
            FeatureValue::Unavailable {
                reason: out_of_range
            }
        );
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

/// The domain book after `events`.
fn book_after(events: &[MarketEvent]) -> OrderBook {
    let mut book = OrderBook::new();
    for event in events {
        book.apply(event);
    }
    book
}
