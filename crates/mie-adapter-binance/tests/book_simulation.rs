//! A deterministic exchange simulator against the order-book pipeline
//! (ADR-038).
//!
//! A seeded LCG drives an exchange book: transactions at non-contiguous
//! update ids, sometimes several in one millisecond; 100 ms diff batches
//! whose `T` is the time of the batch's last transaction; top-N REST
//! snapshots taken at random moments and answered after random delays;
//! dropped diffs (`pu` breaks), reconnects and stalls. Every record goes
//! through [`Pipeline`] in arrival order, a snapshot is requested whenever
//! the pipeline wants one (as the live capture does), and checkpoints are
//! fetched on a cadence.
//!
//! Asserts: the engine accepts everything, the domain book is invalidated
//! only by `OrderBook` gaps, [`BookAudit`] reports only `Matched`, and the
//! recorded records recompute to the identical output. Perturbing one level
//! of one diff makes the audit report a mismatch.

use mie_adapter_binance::book_sync::BookTransition;
use mie_adapter_binance::normalize::record_time;
use mie_adapter_binance::{BinanceStream, BookAudit, CheckpointResult, Pipeline};
use mie_domain::book::{BookStep, OrderBook};
use mie_domain::event::{GapReason, MarketEvent};
use mie_domain::state::MarketStateEngine;
use mie_ports::raw::{Capture, RawRecord};
use std::collections::BTreeMap;

const T0: i64 = 1_791_272_000_000;
const HORIZON_MS: i64 = 90_000;
const HOLD_BACK_MS: i64 = 2_000;
const SNAPSHOT_DEPTH: usize = 25;
const CHECKPOINT_EVERY_MS: i64 = 3_000;

/// Deterministic 64-bit LCG (Knuth's MMIX constants).
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0 >> 33
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    fn chance(&mut self, per_mille: u64) -> bool {
        self.below(1_000) < per_mille
    }
}

/// One exchange transaction: a level set to an absolute quantity.
#[derive(Clone, Copy)]
struct Tx {
    time: i64,
    id: u64,
    bid: bool,
    /// Price in cents.
    cents: i64,
    /// Quantity in thousandths; 0 removes.
    milli: i64,
}

fn price(cents: i64) -> String {
    format!("{}.{:02}", cents / 100, cents % 100)
}

fn qty(milli: i64) -> String {
    format!("{}.{:03}", milli / 1_000, milli % 1_000)
}

/// The exchange: its transactions, generated up front.
struct Exchange {
    txs: Vec<Tx>,
}

impl Exchange {
    fn new(lcg: &mut Lcg) -> Self {
        let mut txs = Vec::new();
        let mut id: u64 = 9_000_000_000;
        let mut mid: i64 = 8_500_000;
        // Seed the book: 70 levels a side at T0 - 1.
        for k in 1..=70 {
            for bid in [true, false] {
                id += 1 + lcg.below(4);
                let cents = if bid { mid - 10 * k } else { mid + 10 * k };
                let milli = 1 + i64::try_from(lcg.below(5_000)).unwrap();
                txs.push(Tx {
                    time: T0 - 1,
                    id,
                    bid,
                    cents,
                    milli,
                });
            }
        }
        let mut time = T0;
        while time < T0 + HORIZON_MS {
            // Several transactions may share a millisecond.
            time += i64::try_from(lcg.below(25)).unwrap();
            if lcg.chance(20) {
                mid += if lcg.chance(500) { 10 } else { -10 };
            }
            id += 1 + lcg.below(6);
            let bid = lcg.chance(500);
            // Mostly near the top; sometimes deep, beyond any snapshot.
            let k = if lcg.chance(900) {
                1 + i64::try_from(lcg.below(20)).unwrap()
            } else {
                1 + i64::try_from(lcg.below(80)).unwrap()
            };
            let cents = if bid { mid - 10 * k } else { mid + 10 * k };
            let milli = if lcg.chance(250) {
                0
            } else {
                1 + i64::try_from(lcg.below(9_000)).unwrap()
            };
            txs.push(Tx {
                time,
                id,
                bid,
                cents,
                milli,
            });
        }
        Self { txs }
    }

    /// Book transactions in `(from, to]` exchange time, grouped into 100 ms
    /// diff batches: `(U, u, pu, T, levels)`.
    fn batches(&self) -> Vec<String> {
        let live: Vec<&Tx> = self.txs.iter().filter(|tx| tx.time >= T0).collect();
        let mut out = Vec::new();
        let mut prev = self
            .txs
            .iter()
            .filter(|tx| tx.time < T0)
            .map(|tx| tx.id)
            .max()
            .unwrap();
        let mut start = 0;
        while start < live.len() {
            let window = live[start].time.div_euclid(100);
            let end = live[start..]
                .iter()
                .position(|tx| tx.time.div_euclid(100) != window)
                .map_or(live.len(), |n| start + n);
            let batch = &live[start..end];
            // The final quantity per price, in first-touch order.
            let mut bids: Vec<(i64, i64)> = Vec::new();
            let mut asks: Vec<(i64, i64)> = Vec::new();
            for tx in batch {
                let side = if tx.bid { &mut bids } else { &mut asks };
                match side.iter_mut().find(|(c, _)| *c == tx.cents) {
                    Some(level) => level.1 = tx.milli,
                    None => side.push((tx.cents, tx.milli)),
                }
            }
            let levels = |side: &[(i64, i64)]| {
                side.iter()
                    .map(|&(c, m)| format!(r#"["{}","{}"]"#, price(c), qty(m)))
                    .collect::<Vec<_>>()
                    .join(",")
            };
            let (first, last) = (batch[0].id, batch[batch.len() - 1].id);
            let time = batch[batch.len() - 1].time;
            out.push(format!(
                r#"{{"e":"depthUpdate","E":{},"T":{time},"s":"BTCUSDT","ps":"BTCUSDT","U":{first},"u":{last},"pu":{prev},"b":[{}],"a":[{}]}}"#,
                time + 3,
                levels(&bids),
                levels(&asks)
            ));
            prev = last;
            start = end;
        }
        out
    }

    /// The top-N snapshot after the last transaction at or before `at`.
    fn snapshot(&self, at: i64) -> String {
        let mut bids: BTreeMap<i64, i64> = BTreeMap::new();
        let mut asks: BTreeMap<i64, i64> = BTreeMap::new();
        let mut last = self.txs[0];
        for tx in self.txs.iter().take_while(|tx| tx.time <= at) {
            let side = if tx.bid { &mut bids } else { &mut asks };
            if tx.milli == 0 {
                side.remove(&tx.cents);
            } else {
                side.insert(tx.cents, tx.milli);
            }
            last = *tx;
        }
        let fmt = |levels: Vec<(&i64, &i64)>| {
            levels
                .iter()
                .map(|(c, m)| format!(r#"["{}","{}"]"#, price(**c), qty(**m)))
                .collect::<Vec<_>>()
                .join(",")
        };
        format!(
            r#"{{"lastUpdateId":{},"E":{},"T":{},"bids":[{}],"asks":[{}]}}"#,
            last.id,
            last.time + 4,
            last.time,
            fmt(bids.iter().rev().take(SNAPSHOT_DEPTH).collect()),
            fmt(asks.iter().take(SNAPSHOT_DEPTH).collect())
        )
    }
}

fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

/// A record waiting for delivery.
#[derive(Clone)]
enum Pending {
    Frame(BinanceStream, String, String),
    /// Fetch a snapshot of the exchange at this time.
    Snapshot(i64, u64),
}

/// What one simulated capture produced.
struct Capture_ {
    /// The persisted records in processing order.
    records: Vec<(BinanceStream, RawRecord)>,
    events: Vec<MarketEvent>,
    transitions: Vec<BookTransition>,
}

fn record(stream: BinanceStream, session: String, seq: u64, payload: String) -> RawRecord {
    RawRecord {
        event_time: record_time(stream, payload.as_bytes()).expect("record time"),
        capture: Some(Capture {
            receive_time_ns: 0,
            receive_seq: seq,
            session_id: session,
        }),
        payload: payload.into_bytes(),
    }
}

/// Runs one capture of a seeded exchange.
fn simulate(seed: u64) -> Capture_ {
    let mut lcg = Lcg(seed);
    let exchange = Exchange::new(&mut lcg);
    // Arrival queue: (arrival ms, tie-breaker) → record.
    let mut queue: BTreeMap<(i64, u64), Pending> = BTreeMap::new();
    let mut order: u64 = 0;
    let mut enqueue = |queue: &mut BTreeMap<(i64, u64), Pending>, at: i64, pending: Pending| {
        order += 1;
        queue.insert((at, order), pending);
    };

    // Diffs: in order per connection, with latency, drops, reconnects and
    // stalls.
    let mut session = 1;
    let mut arrival = T0;
    for diff in exchange.batches() {
        let time: i64 = diff
            .split(r#""T":"#)
            .nth(1)
            .and_then(|rest| rest.split(',').next())
            .and_then(|t| t.parse().ok())
            .unwrap();
        if lcg.chance(8) {
            // A lost diff: the next one breaks the chain.
            continue;
        }
        if lcg.chance(4) {
            session += 1;
            if lcg.chance(500) {
                continue;
            }
        }
        let mut latency = 5 + i64::try_from(lcg.below(40)).unwrap();
        if lcg.chance(3) {
            // A stall: this and everything behind it arrive late.
            latency += 3_500;
        }
        arrival = arrival.max(time + latency);
        enqueue(
            &mut queue,
            arrival,
            Pending::Frame(BinanceStream::Depth, format!("sim/depth/{session}"), diff),
        );
    }
    let mut at = T0;
    while at < T0 + HORIZON_MS {
        enqueue(
            &mut queue,
            at + 10,
            Pending::Frame(
                BinanceStream::MarkPrice,
                "sim/markPrice/1".to_owned(),
                mark(at),
            ),
        );
        at += 1_000;
    }
    let mut at = T0 + CHECKPOINT_EVERY_MS;
    while at < T0 + HORIZON_MS {
        enqueue(&mut queue, at, Pending::Snapshot(at, 0));
        at += CHECKPOINT_EVERY_MS;
    }

    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut requested = None;
    let mut seq = 0;
    let mut out = Capture_ {
        records: Vec::new(),
        events: Vec::new(),
        transitions: Vec::new(),
    };
    while let Some(((now, _), pending)) = queue.pop_first() {
        let (stream, session, payload) = match pending {
            Pending::Frame(stream, session, payload) => (stream, session, payload),
            Pending::Snapshot(requested_at, ordinal) => {
                // Taken while the request is in flight, answered later.
                let delay = if lcg.chance(30) {
                    2_600
                } else {
                    50 + i64::try_from(lcg.below(900)).unwrap()
                };
                let body = exchange.snapshot(requested_at + delay / 2);
                enqueue(
                    &mut queue,
                    requested_at + delay,
                    Pending::Frame(
                        BinanceStream::DepthSnapshot,
                        format!("sim/depthSnapshot/{ordinal}"),
                        body,
                    ),
                );
                continue;
            }
        };
        let raw = record(stream, session, seq, payload);
        seq += 1;
        let pushed = pipeline.push(stream, &raw);
        assert_eq!(pushed.error, None);
        out.records.push((stream, raw));
        out.events.extend(pushed.events);
        out.transitions.extend(pushed.book);
        if let Some(want) = pipeline.book_snapshot_wanted()
            && requested != Some(want)
        {
            requested = Some(want);
            enqueue(&mut queue, now, Pending::Snapshot(now, want));
        }
    }
    out.events.extend(pipeline.finish());
    out
}

/// The pipeline over `records`, in order: what a recompute delivers.
fn recompute(records: &[(BinanceStream, RawRecord)]) -> Vec<MarketEvent> {
    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut events = Vec::new();
    for (stream, record) in records {
        events.extend(pipeline.push(*stream, record).events);
    }
    events.extend(pipeline.finish());
    events
}

/// Engine, domain book and audit over `events`; returns the checkpoints.
fn verify(events: &[MarketEvent]) -> Vec<CheckpointResult> {
    let mut engine = MarketStateEngine::new();
    let mut book = OrderBook::new();
    let mut audit = BookAudit::new();
    let mut checkpoints = Vec::new();
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
        checkpoints.extend(audit.apply(event));
    }
    checkpoints
}

#[test]
fn simulated_captures_sync_resync_and_match_every_checkpoint() {
    let mut totals: BTreeMap<&str, u64> = BTreeMap::new();
    for seed in 1..=6 {
        let capture = simulate(seed);
        let checkpoints = verify(&capture.events);
        assert!(
            checkpoints
                .iter()
                .all(|c| matches!(c, CheckpointResult::Matched { .. })),
            "seed {seed}: {checkpoints:?}"
        );
        // Determinism: the same seed, and the persisted records alone.
        assert_eq!(simulate(seed).events, capture.events, "seed {seed}");
        assert_eq!(recompute(&capture.records), capture.events, "seed {seed}");

        *totals.entry("checkpoints matched").or_default() += checkpoints.len() as u64;
        for transition in &capture.transitions {
            let key = match transition {
                BookTransition::Desynced(GapReason::Disconnected) => "desync disconnected",
                BookTransition::Desynced(GapReason::SequenceBreak) => "desync sequence break",
                BookTransition::Desynced(_) => "desync other",
                BookTransition::Synced { .. } => "synced",
                BookTransition::SnapshotRejected(_) => "snapshot rejected",
                BookTransition::CheckpointEmitted { .. } => "checkpoint emitted",
                BookTransition::CheckpointSkipped(_) => "checkpoint skipped",
            };
            *totals.entry(key).or_default() += 1;
        }
        let late = capture
            .events
            .iter()
            .filter(|e| matches!(e, MarketEvent::FeedGap(g) if g.reason == GapReason::LateEvent))
            .count();
        *totals.entry("late gaps").or_default() += late as u64;
    }
    // The faults were exercised, not only the happy path.
    for key in [
        "checkpoints matched",
        "desync disconnected",
        "desync sequence break",
        "synced",
        "snapshot rejected",
        "checkpoint emitted",
        "late gaps",
    ] {
        assert!(
            totals.get(key).copied().unwrap_or(0) > 0,
            "{key}: {totals:?}"
        );
    }
    assert!(totals["synced"] > 6, "{totals:?}");
}

#[test]
fn a_perturbed_diff_is_caught_at_the_next_checkpoint() {
    let capture = simulate(3);
    // The first checkpoint and the anchor before it, by record position.
    let mut pipeline = Pipeline::new("BTCUSDT", HOLD_BACK_MS, 10_000, &BTreeMap::new());
    let mut anchor = None;
    let mut checkpoint = None;
    for (i, (stream, record)) in capture.records.iter().enumerate() {
        for transition in pipeline.push(*stream, record).book {
            match transition {
                BookTransition::Synced { .. } => anchor = Some(i),
                BookTransition::CheckpointEmitted { .. } if anchor.is_some() => {
                    checkpoint = Some(i);
                }
                BookTransition::Desynced(_) => anchor = None,
                _ => {}
            }
        }
        if checkpoint.is_some() {
            break;
        }
    }
    let (anchor, checkpoint) = (anchor.unwrap(), checkpoint.unwrap());
    // A diff between them with bids; its highest bid is near the top.
    let marker = r#""b":["#;
    let bid_cents = |payload: &str| -> Vec<i64> {
        let at = payload.find(marker).unwrap() + marker.len();
        let end = at + payload[at..].find("]]").map_or(0, |n| n + 1);
        payload[at..end]
            .split(r#"[""#)
            .filter_map(|level| level.split('"').next())
            .filter_map(|p| p.replace('.', "").parse().ok())
            .collect()
    };
    let (target, best) = (anchor + 1..checkpoint)
        .filter(|&i| capture.records[i].0 == BinanceStream::Depth)
        .find_map(|i| {
            let payload = String::from_utf8(capture.records[i].1.payload.clone()).unwrap();
            bid_cents(&payload).into_iter().max().map(|best| (i, best))
        })
        .expect("a diff with bids between anchor and checkpoint");

    // Add a bid off the exchange's 0.10 price grid next to it: inside every
    // trusted window, never touched again.
    let mut records = capture.records.clone();
    let payload = String::from_utf8(records[target].1.payload.clone()).unwrap();
    let at = payload.find(marker).unwrap() + marker.len();
    let extra = format!(r#"["{}","7.777"],"#, price(best - 5));
    let mutated = format!("{}{extra}{}", &payload[..at], &payload[at..]);
    records[target].1.payload = mutated.into_bytes();

    let checkpoints = verify(&recompute(&records));
    assert!(
        checkpoints
            .iter()
            .any(|c| matches!(c, CheckpointResult::Mismatched { .. })),
        "{checkpoints:?}"
    );
}
