//! Scenario replay: recorded arrival sequences through the pipeline.
//!
//! Each `tests/fixtures/scenario-*.tsv` row is one raw record in processing
//! order: `receive_seq \t stream \t session_id \t payload`. The pipeline
//! output must be deterministic, carry the expected gaps at the expected
//! positions, and be accepted by the domain engine as a whole.

use mie_adapter_binance::normalize::{NormalizeError, record_time};
use mie_adapter_binance::{BinanceStream, Pipeline};
use mie_domain::bars::Timeframe;
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream};
use mie_domain::feature::FeatureValue;
use mie_domain::num::Qty;
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord};
use std::collections::BTreeMap;

const HOLD_BACK_MS: i64 = 2_000;

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
                event_time: record_time(stream, payload.as_bytes())
                    .unwrap_or(EventTime::from_millis(0)),
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

fn run(records: &[(BinanceStream, RawRecord)]) -> (Vec<MarketEvent>, Pipeline) {
    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut out = Vec::new();
    for (stream, record) in records {
        out.extend(pipeline.push(*stream, record).events);
    }
    out.extend(pipeline.finish());
    (out, pipeline)
}

fn gaps(events: &[MarketEvent]) -> Vec<(usize, FeedGap)> {
    events
        .iter()
        .enumerate()
        .filter_map(|(i, e)| match e {
            MarketEvent::FeedGap(gap) => Some((i, *gap)),
            _ => None,
        })
        .collect()
}

fn assert_engine_accepts(events: &[MarketEvent]) {
    let mut engine = MarketStateEngine::new();
    for (i, event) in events.iter().enumerate() {
        engine
            .apply(event)
            .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
    }
}

fn gap(stream: Stream, start: i64, end: i64, reason: GapReason) -> FeedGap {
    FeedGap {
        stream,
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
        reason,
    }
}

#[test]
fn interleaved_streams_merge_into_the_canonical_order() {
    let records = load("scenario-interleaved.tsv");
    let (out, pipeline) = run(&records);
    assert_eq!(run(&records).0, out, "two runs differ");
    assert!(gaps(&out).is_empty(), "unexpected gaps: {:?}", gaps(&out));
    let mut sorted = out.clone();
    sorted.sort();
    assert_eq!(out, sorted);
    assert_engine_accepts(&out);
    // 9 trades, 6 mark prices, 2 liquidations in one millisecond, 1 open
    // interest, 1 closed kline (the open one has no event).
    assert_eq!(out.len(), 19);
    let stats = pipeline.stats();
    assert_eq!(stats.streams[&BinanceStream::ForceOrder].events, 2);
    assert_eq!(stats.streams[&BinanceStream::Kline1m].records, 2);
    assert_eq!(stats.streams[&BinanceStream::Kline1m].events, 1);
    assert!(stats.streams[&BinanceStream::AggTrade].max_lateness_ms > 0);
}

#[test]
fn faults_become_gaps_at_their_positions() {
    let records = load("scenario-faults.tsv");
    let (out, pipeline) = run(&records);
    assert_eq!(run(&records).0, out, "two runs differ");
    assert_engine_accepts(&out);

    const B: i64 = 1_791_271_000_000;
    let found = gaps(&out);
    let expected = [
        gap(
            Stream::Trades,
            B + 10_100,
            B + 10_200,
            GapReason::SequenceBreak,
        ),
        gap(
            Stream::Trades,
            B + 11_000,
            B + 12_000,
            GapReason::Disconnected,
        ),
        gap(
            Stream::Liquidations,
            B + 11_900,
            B + 12_001,
            GapReason::LateEvent,
        ),
    ];
    assert_eq!(found.iter().map(|(_, g)| *g).collect::<Vec<_>>(), expected);

    // Each gap directly precedes the event it was found at.
    let after = |i: usize| out[i + 1].clone();
    let MarketEvent::Trade(skip) = after(found[0].0) else {
        panic!("sequence break not before a trade");
    };
    assert_eq!(skip.trade_id, 103);
    let MarketEvent::Trade(resumed) = after(found[1].0) else {
        panic!("disconnect not before a trade");
    };
    assert_eq!(resumed.trade_id, 105);
    // The late gap sorts right after the last event released before it.
    assert!(
        matches!(&out[found[2].0 - 1], MarketEvent::MarkPrice(m) if m.time.as_millis() == B + 12_000)
    );

    let stats = pipeline.stats();
    let agg = &stats.streams[&BinanceStream::AggTrade];
    assert_eq!((agg.duplicates, agg.normalize_errors), (1, 1));
    assert_eq!(agg.gaps[&GapReason::SequenceBreak], 1);
    assert_eq!(agg.gaps[&GapReason::Disconnected], 1);
    assert_eq!(
        stats.streams[&BinanceStream::ForceOrder].gaps[&GapReason::LateEvent],
        1
    );
    assert_eq!(stats.streams[&BinanceStream::ForceOrder].events, 0);
    assert!(stats.streams[&BinanceStream::ForceOrder].max_lateness_ms >= 2_600);
}

/// A live `aggTrade` record (the `tests/fixtures/aggTrade.jsonl` shape) in
/// arrival order `seq`.
fn agg_trade(seq: u64, id: u64, time: i64, qty: &str) -> (BinanceStream, RawRecord) {
    let payload = format!(
        r#"{{"e":"aggTrade","E":{time},"a":{id},"s":"BTCUSDT","p":"85299.90","q":"{qty}","nq":"{qty}","f":{id},"l":{id},"T":{time},"m":true,"st":1}}"#
    );
    let record = RawRecord {
        event_time: record_time(BinanceStream::AggTrade, payload.as_bytes()).expect("record time"),
        capture: Some(Capture {
            receive_time_ns: 0,
            receive_seq: seq,
            session_id: "s1".to_owned(),
        }),
        payload: payload.into_bytes(),
    };
    (BinanceStream::AggTrade, record)
}

/// 2026-10-06T07:17:00Z: a minute boundary well inside one UTC day.
const MINUTE: i64 = 1_791_271_020_000;

#[test]
fn a_non_positive_trade_quantity_is_a_counted_error_and_a_trade_id_gap() {
    let records = [
        agg_trade(1, 100, MINUTE + 1_000, "0.004"),
        agg_trade(2, 101, MINUTE + 2_000, "0"),
        agg_trade(3, 102, MINUTE + 3_000, "0.010"),
    ];
    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut out = Vec::new();
    for (i, (stream, record)) in records.iter().enumerate() {
        let pushed = pipeline.push(*stream, record);
        if i == 1 {
            assert!(
                matches!(pushed.error, Some(NormalizeError::Unexpected(_))),
                "{:?}",
                pushed.error
            );
            assert!(pushed.events.is_empty(), "{:?}", pushed.events);
        } else {
            assert_eq!(pushed.error, None);
        }
        out.extend(pushed.events);
    }
    out.extend(pipeline.finish());
    assert_engine_accepts(&out);

    // The rejected trade never becomes an event; the id break reports it.
    let ids: Vec<u64> = out
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Trade(trade) => Some(trade.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, [100, 102]);
    let found = gaps(&out);
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].1.reason, GapReason::SequenceBreak);
    assert!(matches!(&out[found[0].0 + 1], MarketEvent::Trade(t) if t.trade_id == 102));

    let agg = &pipeline.stats().streams[&BinanceStream::AggTrade];
    assert_eq!((agg.records, agg.normalize_errors, agg.events), (3, 1, 2));
    assert_eq!(agg.gaps[&GapReason::SequenceBreak], 1);
}

#[test]
fn rejected_quantities_keep_bars_and_the_profile_consistent() {
    // (record, valid quantity in units if the record is a valid trade in a
    // minute that closes). The valid ids 1000..=1004 are contiguous; the two
    // rejected records carry their own ids, so no gap muddies the sums.
    let records = [
        (agg_trade(1, 1000, MINUTE + 1_000, "0.004"), Some(400_000)),
        (agg_trade(2, 5001, MINUTE + 2_000, "0"), None),
        (agg_trade(3, 1001, MINUTE + 3_000, "0.010"), Some(1_000_000)),
        (
            agg_trade(4, 1002, MINUTE + 61_000, "0.250"),
            Some(25_000_000),
        ),
        (agg_trade(5, 5002, MINUTE + 62_000, "-0.004"), None),
        (
            agg_trade(6, 1003, MINUTE + 63_000, "1.5"),
            Some(150_000_000),
        ),
        // The developing minute: it closes the first two and stays out of
        // the profile.
        (agg_trade(7, 1004, MINUTE + 121_000, "0.002"), None),
    ];
    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut out = Vec::new();
    let mut errors = 0;
    for ((stream, record), _) in &records {
        let pushed = pipeline.push(*stream, record);
        if let Some(error) = pushed.error {
            assert!(matches!(error, NormalizeError::Unexpected(_)), "{error}");
            errors += 1;
        }
        out.extend(pushed.events);
    }
    out.extend(pipeline.finish());
    assert_eq!(errors, 2);
    assert_eq!(
        pipeline.stats().streams[&BinanceStream::AggTrade].normalize_errors,
        2
    );
    assert!(gaps(&out).is_empty(), "unexpected gaps: {:?}", gaps(&out));

    let mut engine = MarketStateEngine::new();
    let mut closed_volume = 0;
    let mut closed_minutes = 0;
    for (i, event) in out.iter().enumerate() {
        engine
            .apply(event)
            .unwrap_or_else(|e| panic!("event {i} rejected: {e}"));
        for bar in engine.closed_bars() {
            if bar.timeframe == Timeframe::M1 {
                closed_volume += bar.volume.units();
                closed_minutes += 1;
            }
        }
    }
    assert_eq!(closed_minutes, 2);

    let expected: i64 = records.iter().filter_map(|(_, units)| *units).sum();
    assert_eq!(expected, 176_400_000);
    assert_eq!(closed_volume, expected);
    let FeatureValue::Ready(profile) = &engine.state().profile.utc_day else {
        panic!(
            "profile still warming up: {:?}",
            engine.state().profile.utc_day
        );
    };
    assert_eq!(profile.total_volume, Qty::from_units(expected));
}
