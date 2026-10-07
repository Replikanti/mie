//! Order-book scenarios: recorded arrival sequences through the pipeline
//! (ADR-038, acceptance criterion 1 of #10).
//!
//! Each `tests/fixtures/scenario-book-*.tsv` row is one raw record in
//! processing order: `receive_seq \t stream \t session_id \t payload`. The
//! output must be deterministic, accepted by the engine as a whole, and keep
//! a domain book valid except across `OrderBook` gaps.

use mie_adapter_binance::book_sync::{BookTransition, CheckpointSkip, SnapshotRejection};
use mie_adapter_binance::normalize::record_time;
use mie_adapter_binance::{BinanceStream, Pipeline, PipelineStats};
use mie_domain::book::{BookStep, OrderBook};
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord};
use std::collections::BTreeMap;

/// 2026-10-06T07:33:20Z, the scenarios' base time.
const B: i64 = 1_791_272_000_000;

fn load(name: &str) -> Vec<(BinanceStream, RawRecord)> {
    let path = format!("{}/tests/fixtures/{name}", env!("CARGO_MANIFEST_DIR"));
    let text = std::fs::read_to_string(&path).expect("read scenario");
    text.lines()
        .filter(|line| !line.is_empty() && !line.starts_with('#'))
        .map(|line| {
            let fields: Vec<_> = line.splitn(4, '\t').collect();
            let [seq, stream, session, payload] = fields[..] else {
                panic!("malformed row {line:?}");
            };
            let stream = BinanceStream::from_raw_name(stream).expect("known stream");
            let record = RawRecord {
                event_time: record_time(stream, payload.as_bytes()).expect("record time"),
                capture: Some(Capture {
                    receive_time_ns: 0,
                    receive_seq: seq.parse().expect("receive_seq"),
                    session_id: session.to_owned(),
                }),
                payload: payload.as_bytes().to_vec(),
            };
            (stream, record)
        })
        .collect()
}

struct Outcome {
    events: Vec<MarketEvent>,
    /// (receive_seq, transition).
    transitions: Vec<(u64, BookTransition)>,
    stats: PipelineStats,
}

fn run(records: &[(BinanceStream, RawRecord)]) -> Outcome {
    let mut pipeline = Pipeline::new("BTCUSDT", 2_000, 10_000, &BTreeMap::new());
    let mut events = Vec::new();
    let mut transitions = Vec::new();
    for (stream, record) in records {
        let pushed = pipeline.push(*stream, record);
        assert_eq!(pushed.error, None);
        let seq = record.capture.as_ref().unwrap().receive_seq;
        transitions.extend(pushed.book.into_iter().map(|t| (seq, t)));
        events.extend(pushed.events);
    }
    events.extend(pipeline.finish());
    Outcome {
        events,
        transitions,
        stats: pipeline.stats(),
    }
}

/// The engine accepts everything; the domain book is invalidated only by
/// `OrderBook` gaps. Returns the book.
fn verify(events: &[MarketEvent]) -> OrderBook {
    let mut engine = MarketStateEngine::new();
    let mut book = OrderBook::new();
    for (i, event) in events.iter().enumerate() {
        engine
            .apply(event)
            .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
        if let BookStep::Invalidated(why) = book.apply(event) {
            assert!(
                matches!(event, MarketEvent::FeedGap(_)),
                "event {i} invalidated the book: {why:?}"
            );
        }
    }
    book
}

fn gaps(events: &[MarketEvent]) -> Vec<FeedGap> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::FeedGap(gap) => Some(*gap),
            _ => None,
        })
        .collect()
}

fn update_ids(events: &[MarketEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::BookUpdate(u) => Some(u.last_update_id),
            _ => None,
        })
        .collect()
}

fn snapshot_ids(events: &[MarketEvent]) -> Vec<u64> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::BookSnapshot(s) => Some(s.last_update_id),
            _ => None,
        })
        .collect()
}

#[test]
fn a_valid_sequence_syncs_and_chains() {
    let records = load("scenario-book-valid.tsv");
    let out = run(&records);
    assert_eq!(run(&records).events, out.events, "two runs differ");
    let book = verify(&out.events);
    assert!(book.is_valid());
    assert_eq!(book.last_update_id(), Some(140));
    assert!(gaps(&out.events).is_empty());
    assert_eq!(snapshot_ids(&out.events), [115]);
    // 110 lies below the snapshot and is dropped.
    assert_eq!(update_ids(&out.events), [120, 130, 140]);
    assert_eq!(
        out.transitions,
        [
            (1, BookTransition::Desynced(GapReason::Disconnected)),
            (
                4,
                BookTransition::Synced {
                    last_update_id: 115,
                    time: EventTime::from_millis(B + 50)
                }
            ),
        ]
    );
    assert_eq!(out.stats.book.stale_diffs, 1);
    assert_eq!(out.stats.streams[&BinanceStream::MarkPrice].events, 4);
}

#[test]
fn a_pu_break_resyncs_behind_one_sequence_break_gap() {
    let records = load("scenario-book-pu-break.tsv");
    let out = run(&records);
    assert_eq!(run(&records).events, out.events, "two runs differ");
    verify(&out.events);
    assert_eq!(
        gaps(&out.events),
        [FeedGap {
            stream: Stream::OrderBook,
            start: EventTime::from_millis(B + 100),
            end: EventTime::from_millis(B + 350),
            reason: GapReason::SequenceBreak,
        }]
    );
    // Nothing between the break and the resync: 140 is below the snapshot,
    // 150 straddles it.
    assert_eq!(update_ids(&out.events), [110, 120, 150, 160]);
    assert_eq!(snapshot_ids(&out.events), [105, 145]);
    // The gap sits right before the resync snapshot.
    let at = out
        .events
        .iter()
        .position(|e| matches!(e, MarketEvent::FeedGap(_)))
        .unwrap();
    assert!(matches!(&out.events[at + 1], MarketEvent::BookSnapshot(s) if s.last_update_id == 145));
    let kinds: Vec<_> = out.transitions.iter().map(|(seq, t)| (*seq, *t)).collect();
    assert_eq!(
        kinds[2],
        (5, BookTransition::Desynced(GapReason::SequenceBreak))
    );
    assert!(matches!(
        kinds[3],
        (
            7,
            BookTransition::Synced {
                last_update_id: 145,
                ..
            }
        )
    ));
    assert_eq!(kinds.len(), 4);
    let depth = &out.stats.streams[&BinanceStream::Depth];
    assert_eq!(depth.gaps[&GapReason::SequenceBreak], 1);
    assert_eq!(out.stats.book.desyncs[&GapReason::SequenceBreak], 1);
}

#[test]
fn stale_events_are_dropped_and_counted() {
    let records = load("scenario-book-stale.tsv");
    let out = run(&records);
    assert_eq!(run(&records).events, out.events, "two runs differ");
    let book = verify(&out.events);
    assert_eq!(book.last_update_id(), Some(170));
    assert_eq!(update_ids(&out.events), [130, 140, 160, 170]);
    assert_eq!(snapshot_ids(&out.events), [125, 155]);
    let transitions: Vec<BookTransition> = out.transitions.iter().map(|(_, t)| *t).collect();
    assert!(transitions.contains(&BookTransition::CheckpointSkipped(CheckpointSkip::NotNewer)));
    assert!(transitions.contains(&BookTransition::SnapshotRejected(SnapshotRejection::Stale)));
    let stats = &out.stats.book;
    assert_eq!(stats.stale_diffs, 2);
    assert_eq!(stats.checkpoints_skipped[&CheckpointSkip::NotNewer], 1);
    assert_eq!(stats.snapshots_rejected[&SnapshotRejection::Stale], 1);
    assert_eq!(
        gaps(&out.events)
            .iter()
            .map(|g| (g.start.as_millis() - B, g.end.as_millis() - B, g.reason))
            .collect::<Vec<_>>(),
        [(300, 450, GapReason::SequenceBreak)]
    );
    // Every snapshot record counts; only the delivered ones are events.
    let snapshots = &out.stats.streams[&BinanceStream::DepthSnapshot];
    assert_eq!((snapshots.records, snapshots.events), (4, 2));
}
