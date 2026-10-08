//! `mie replay` offline (#11, ADR-039): live captures and archive rows in a
//! real Parquet raw store, replayed through the core.
//!
//! - Round trip (ADR-027): what a live capture delivered equals the replay
//!   of the store it wrote, event for event, decimals exact.
//! - Determinism: the same window prints the same report and hash.
//! - Look-ahead: the sequence is strictly increasing, and open interest is
//!   never delivered before its exchange time.
//! - Order book (ADR-038): a depth capture replays to the book events live
//!   delivered, resyncs and restart seed gaps included.

mod common;

use common::{
    D0, DepthConnection, HOUR, TempDir, agg, config, config_with, depth, depth_snapshot,
    depth_transports, ingest, ingest_depth, journal,
};
use mie_adapter_binance::archive::catalog::{DAY_MS, parse_day};
use mie_adapter_binance::archive::normalize::record_time as archive_record_time;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_adapter_binance::{
    BinanceStream, CaptureEvent, CaptureObserver, LiveConfig, LiveReplay, LiveRun, start,
};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_cli::config::ArchiveConfig;
use mie_cli::journal::read_runs;
use mie_cli::replay::{self, ReplayOutcome, ReplayRequest, ReplaySource};
use mie_domain::book::{BookStep, Invalidation, OrderBook};
use mie_domain::event::{GapReason, MarketEvent, Stream};
use mie_domain::num::{Price, Qty, Rate};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawSelection, RawStreamKey};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

fn run_replay(request: &ReplayRequest) -> (ReplayOutcome, String) {
    let mut out = Vec::new();
    let outcome = replay::run(request, &mut out).expect("the report is written");
    (outcome, String::from_utf8(out).unwrap())
}

fn drain<P: MarketDataProvider>(provider: &mut P) -> Vec<MarketEvent> {
    let mut out = Vec::new();
    while let Some(event) = provider.next_event().unwrap() {
        out.push(event);
    }
    out
}

fn assert_no_look_ahead(events: &[MarketEvent]) {
    assert!(
        events.windows(2).all(|w| w[0] < w[1]),
        "the sequence is strictly increasing"
    );
    let mut engine = MarketStateEngine::new();
    for event in events {
        engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
    }
}

// ---------------------------------------------------------------------------
// Round trip through a live capture.

/// A clock that the threads named in `drivers` advance by sleeping; other
/// threads' sleeps are short real sleeps. The open-interest poller drives
/// the poll cadence and the trade stream its own reconnect backoff, so
/// neither waits for the other.
struct DrivenClock {
    utc_ns: Mutex<i64>,
    drivers: [&'static str; 2],
}

impl Clock for DrivenClock {
    fn now_utc_ns(&self) -> i64 {
        *self.utc_ns.lock().unwrap()
    }

    fn monotonic_ns(&self) -> u64 {
        *self.utc_ns.lock().unwrap() as u64
    }

    fn sleep(&self, d: Duration) {
        let name = std::thread::current().name().map(str::to_owned);
        if name.is_some_and(|n| self.drivers.contains(&n.as_str())) {
            *self.utc_ns.lock().unwrap() += d.as_nanos() as i64;
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Scripted WebSocket connections per stream path. A connection whose
/// frames are spent closes while another one is scripted (a reconnect),
/// otherwise it idles; `pending` counts the paths not yet spent. The path
/// `last_path` sends only once every other path is spent, so the arrival
/// order across streams — and with it what the hold-back makes late — is
/// fixed.
struct Connections {
    scripts: Mutex<BTreeMap<String, VecDeque<Vec<String>>>>,
    pending: Arc<AtomicUsize>,
    last_path: String,
}

impl WsConnector for Connections {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        let path = url.rsplit('/').next().unwrap_or_default().to_owned();
        let mut scripts = self.scripts.lock().unwrap();
        let queue = scripts.get_mut(&path).ok_or("unscripted path")?;
        let frames = queue.pop_front().ok_or("script exhausted")?;
        Ok(Box::new(Connection {
            frames: frames.into(),
            last: queue.is_empty(),
            spent: false,
            waits: path == self.last_path,
            pending: Arc::clone(&self.pending),
        }))
    }
}

struct Connection {
    frames: VecDeque<String>,
    last: bool,
    spent: bool,
    waits: bool,
    pending: Arc<AtomicUsize>,
}

impl WsConnection for Connection {
    fn read(&mut self) -> ReadOutcome {
        if self.waits && self.pending.load(Ordering::SeqCst) > 1 {
            std::thread::sleep(Duration::from_millis(1));
            return ReadOutcome::Control;
        }
        if let Some(frame) = self.frames.pop_front() {
            return ReadOutcome::Frame(frame.into_bytes());
        }
        if !self.last {
            return ReadOutcome::Closed("scripted reconnect".to_owned());
        }
        // The previous frame was handed to the capture before this read.
        if !self.spent {
            self.spent = true;
            self.pending.fetch_sub(1, Ordering::SeqCst);
        }
        std::thread::sleep(Duration::from_millis(1));
        ReadOutcome::Control
    }

    fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}
}

/// Answers each open-interest poll once every WebSocket frame was handed to
/// the capture, with `time = poll slot − lag`; shuts the capture down when
/// the lags are spent.
struct LaggingOi {
    lags_ms: Mutex<VecDeque<i64>>,
    clock: Arc<DrivenClock>,
    pending: Arc<AtomicUsize>,
    shutdown: Arc<AtomicBool>,
}

impl HttpGet for LaggingOi {
    fn get(&self, _url: &str) -> Result<(u16, Vec<u8>), String> {
        let slot = self.clock.now_utc_ns() / 1_000_000;
        let Some(lag) = self.lags_ms.lock().unwrap().pop_front() else {
            self.shutdown.store(true, Ordering::Relaxed);
            return Err("script exhausted".to_owned());
        };
        while self.pending.load(Ordering::SeqCst) > 0 {
            std::thread::sleep(Duration::from_millis(1));
        }
        let body = format!(
            r#"{{"symbol":"BTCUSDT","openInterest":"95253.4750","time":{}}}"#,
            slot - lag
        );
        Ok((200, body.into_bytes()))
    }
}

struct Silent;

impl CaptureObserver for Silent {
    fn on_event(&mut self, _: i64, _: &CaptureEvent) {}
}

/// An aggregate trade with the given decimal literals.
fn trade(id: u64, time: i64, price: &str, qty: &str) -> String {
    format!(
        r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"{price}","q":"{qty}","f":1,"l":1,"T":{time},"m":false}}"#,
        time + 40
    )
}

fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

#[test]
fn a_replay_of_the_store_equals_what_the_live_capture_delivered() {
    let dir = TempDir::new("replay-round-trip");
    let t = D0 + HOUR;
    let start_ms = t + 1_000;
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = Arc::new(DrivenClock {
        utc_ns: Mutex::new(start_ms * 1_000_000),
        drivers: ["mie-openInterest", "mie-aggTrade"],
    });
    let pending = Arc::new(AtomicUsize::new(2));
    let literals = [
        (1_001, t + 10, "123456.12345678", "0.0100000000"),
        (1_002, t + 20, "85000.10", "1.000"),
        // An id skip.
        (1_005, t + 30, "0.0100000000", "123456.12345678"),
        // After the reconnect.
        (1_006, t + 3_000, "85000.20000000", "0.001"),
        (1_007, t + 3_500, "85000.3", "2"),
    ];
    let frames = |range: std::ops::Range<usize>| -> Vec<String> {
        literals[range]
            .iter()
            .map(|&(id, time, p, q)| trade(id, time, p, q))
            .collect()
    };
    // After every trade; the 6.5 s sample arrives after 8 s was released.
    let marks: Vec<String> = [4_000, 5_000, 8_000, 9_000, 6_500, 12_000, 15_000]
        .iter()
        .map(|offset| mark(t + offset))
        .collect();
    let connector = Arc::new(Connections {
        scripts: Mutex::new(BTreeMap::from([
            (
                "btcusdt@aggTrade".to_owned(),
                VecDeque::from([frames(0..3), frames(3..5)]),
            ),
            ("btcusdt@markPrice@1s".to_owned(), VecDeque::from([marks])),
        ])),
        pending: Arc::clone(&pending),
        last_path: "btcusdt@markPrice@1s".to_owned(),
    });
    let http = Arc::new(LaggingOi {
        lags_ms: Mutex::new(VecDeque::from([4_200, 6_000])),
        clock: Arc::clone(&clock),
        pending,
        shutdown: Arc::clone(&shutdown),
    });
    let mut live = LiveConfig::new("20261006T010001Z");
    live.ws_base_url = "wss://fake.invalid/ws".to_owned();
    live.rest_base_url = "https://fake.invalid".to_owned();
    live.streams = vec![
        BinanceStream::AggTrade,
        BinanceStream::MarkPrice,
        BinanceStream::OpenInterest,
    ];
    live.hold_back_ms = 750;
    let store = ParquetRawStore::new(dir.path().join("raw"));
    let writer = store
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    let (mut provider, handle) = start(
        live.clone(),
        writer,
        connector,
        http,
        Arc::clone(&clock) as Arc<dyn Clock>,
        Box::new(Silent),
        Arc::clone(&shutdown),
    )
    .unwrap();
    let delivered = drain(&mut provider);
    drop(provider);
    let (writer, summary) = handle.join().unwrap();
    writer.close().unwrap();
    let end_ms = clock.now_utc_ns() / 1_000_000;

    let run = LiveRun {
        run_id: live.run_id.clone(),
        symbol: live.symbol.clone(),
        hold_back_ms: live.hold_back_ms,
        oi_retime_ms: live.oi_retime_ms,
        seeds: BTreeMap::new(),
        streams: live.streams.clone(),
        started_at_ms: start_ms,
        ended_at_ms: Some(end_ms),
        clean_records: Some(summary.stats.records),
    };
    assert_eq!(summary.stats.records, 5 + 7 + 2);
    let replay = LiveReplay::new(&store, "binance-um", "BTCUSDT", vec![run]);
    let replayed = drain(&mut replay.replay(window(D0, D0 + DAY_MS)).unwrap().stream);
    assert_eq!(replayed, delivered);
    assert_no_look_ahead(&replayed);

    // Exact decimals: the stored literals parse to the replayed units.
    let trades: BTreeMap<u64, (i64, i64)> = replayed
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Trade(t) => Some((t.trade_id, (t.price.units(), t.qty.units()))),
            _ => None,
        })
        .collect();
    assert_eq!(trades.len(), literals.len());
    for (id, _, price, qty) in literals {
        let expected = (
            price.parse::<Price>().unwrap().units(),
            qty.parse::<Qty>().unwrap().units(),
        );
        assert_eq!(trades[&id], expected, "trade {id}");
    }
    assert_eq!(trades[&1_001].0, 12_345_612_345_678);
    assert_eq!(trades[&1_001].1, 1_000_000);

    let reasons: BTreeSet<(Stream, GapReason)> = replayed
        .iter()
        .filter_map(|e| match e {
            MarketEvent::FeedGap(g) => Some((g.stream, g.reason)),
            _ => None,
        })
        .collect();
    for expected in [
        (Stream::Trades, GapReason::SequenceBreak),
        (Stream::Trades, GapReason::Disconnected),
        (Stream::MarkPrice, GapReason::LateEvent),
    ] {
        assert!(reasons.contains(&expected), "{expected:?} in {reasons:?}");
    }

    // Look-ahead: open interest is delivered at or after its exchange time,
    // the lagging sample re-timed to when live knew it (ADR-032 D12).
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "openInterest").unwrap();
    let selection = RawSelection::new(BTreeSet::from([key]), window(D0, D0 + DAY_MS)).unwrap();
    let mut raw_oi: Vec<RawRecord> = Vec::new();
    for file in store.select(&selection).unwrap().files {
        raw_oi.extend(store.read(&file).unwrap());
    }
    raw_oi.sort_by_key(|r| r.capture.as_ref().unwrap().receive_seq);
    let delivered_oi: Vec<i64> = replayed
        .iter()
        .filter_map(|e| match e {
            MarketEvent::OpenInterest(oi) => Some(oi.time.as_millis()),
            _ => None,
        })
        .collect();
    assert_eq!(delivered_oi.len(), 2);
    for (delivered, raw) in delivered_oi.iter().zip(&raw_oi) {
        assert!(*delivered >= raw.event_time.as_millis());
    }
    assert!(
        delivered_oi[0] > raw_oi[0].event_time.as_millis(),
        "re-timed"
    );
}

// ---------------------------------------------------------------------------
// Determinism over `mie ingest` runs.

#[test]
fn the_same_window_replays_to_the_same_bytes_and_matches_the_journal() {
    let dir = TempDir::new("replay-determinism");
    let config = config(dir.path());
    let t1 = D0 + HOUR;
    let first = ingest(
        &config,
        t1,
        vec![vec![
            agg(100, t1 + 10),
            agg(101, t1 + 20),
            agg(102, t1 + 30),
            agg(104, t1 + 50),
            agg(105, t1 + 60),
        ]],
    );
    let t2 = t1 + HOUR;
    let second = ingest(&config, t2, vec![vec![agg(300, t2 + 5), agg(301, t2 + 6)]]);
    assert_eq!((first.exit_code(), second.exit_code()), (0, 0));

    let request = |w: ReplayWindow| ReplayRequest {
        source: ReplaySource::Live(config.clone()),
        window: w,
    };
    let all = window(D0, D0 + DAY_MS);
    let (outcome, text) = run_replay(&request(all));
    let (again, text_again) = run_replay(&request(all));
    assert_eq!(text, text_again);
    assert_eq!(outcome, again);
    assert!(outcome.pass, "{text}");
    assert_eq!(outcome.exit_code(), 0);
    assert!(text.ends_with("PASS\n"), "{text}");
    assert_eq!(outcome.events, first.events + second.events);
    assert_eq!(outcome.domain_rejections, 0);
    let hash = outcome.stream_hash.unwrap();
    assert!(text.contains(&format!("event-stream {hash}\n")), "{text}");
    assert!(
        text.contains(&format!("dataset {}\n", outcome.dataset.as_ref().unwrap())),
        "{text}"
    );
    assert!(
        text.contains("run 20261006T010000Z: clean, records 5, replayed 5, ignored 0"),
        "{text}"
    );
    assert!(
        text.contains("delivered Trades: events 7, gaps Disconnected 1, SequenceBreak 1"),
        "{text}"
    );
    assert!(!text.contains("at_ms"), "no wall clock in the report");

    // The replayed gaps are the journaled ones.
    let runs: Vec<LiveRun> = read_runs(&config.paths.journal, "binance-um")
        .unwrap()
        .iter()
        .map(|r| r.live_run())
        .collect();
    assert_eq!(runs.len(), 2);
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let live = LiveReplay::new(&store, "binance-um", "BTCUSDT", runs);
    let replayed = drain(&mut live.replay(all).unwrap().stream);
    assert_no_look_ahead(&replayed);
    let replayed_gaps: Vec<(String, i64, i64)> = replayed
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
    let journaled: Vec<(String, i64, i64)> = journal(&config)
        .iter()
        .filter(|l| l["type"] == "gap")
        .map(|l| {
            (
                l["reason"].as_str().unwrap().to_owned(),
                l["start"].as_i64().unwrap(),
                l["end"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(replayed_gaps, journaled);

    // A window ending before run 2 resumed: its seed gap is trailing.
    let (cut, text) = run_replay(&request(window(D0, t2 + 5)));
    assert!(cut.pass, "{text}");
    assert_eq!(cut.events, first.events);
    assert!(
        text.contains(&format!(
            "trailing gap Trades [{}, {}] ms Disconnected",
            t1 + 60,
            t2 + 5
        )),
        "{text}"
    );
    assert_ne!(cut.dataset, outcome.dataset);

    // Nothing in the window: no evidence.
    let (empty, text) = run_replay(&request(window(D0 + 20 * HOUR, D0 + 21 * HOUR)));
    assert!(!empty.pass);
    assert_eq!(empty.exit_code(), 1);
    assert!(text.ends_with("FAIL: no event in the window\n"), "{text}");

    // Run 2's journal lines are lost (say its buffered run_start never
    // reached the disk): its sealed records fail the replay, loudly.
    let kept: Vec<String> = std::fs::read_to_string(&config.paths.journal)
        .unwrap()
        .lines()
        .filter(|l| !l.contains("20261006T020000Z"))
        .map(str::to_owned)
        .collect();
    std::fs::write(&config.paths.journal, kept.join("\n") + "\n").unwrap();
    for w in [all, window(t2 - HOUR / 2, t2 + HOUR)] {
        let (lost, text) = run_replay(&request(w));
        assert_eq!(lost.exit_code(), 1, "{text}");
        assert!(
            text.contains("without a run_start in the journal")
                && text.contains("20261006T020000Z"),
            "{text}"
        );
    }
}

// ---------------------------------------------------------------------------
// The order book through the replay (ADR-038, ADR-039).

/// A depth capture at `t`: a sync, a reconnect, and a resync from the
/// second snapshot.
fn depth_script(t: i64) -> (Vec<DepthConnection>, Vec<String>) {
    (
        vec![
            DepthConnection {
                frames: vec![depth(11, 20, 10, t), depth(21, 30, 20, t + 100)],
                await_served: 1,
            },
            DepthConnection {
                frames: vec![depth(41, 50, 40, t + 5_000)],
                await_served: 2,
            },
        ],
        vec![depth_snapshot(15, t - 50), depth_snapshot(45, t + 4_950)],
    )
}

/// The book `events` rebuild and its steps, unrelated events left out.
fn book_steps(events: &[MarketEvent]) -> (OrderBook, Vec<BookStep>) {
    let mut book = OrderBook::new();
    let steps = events
        .iter()
        .map(|e| book.apply(e))
        .filter(|step| *step != BookStep::Unrelated)
        .collect();
    (book, steps)
}

#[test]
fn a_replay_of_a_depth_capture_equals_the_book_live_delivered() {
    let dir = TempDir::new("replay-depth-round-trip");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    let t = D0 + HOUR;
    let (connections, snapshots) = depth_script(t);
    let (transports, shutdown) = depth_transports(t, connections, snapshots, vec![]);
    let clock = Arc::clone(&transports.clock);
    let live = config
        .live_config("20261006T010000Z", BTreeMap::new())
        .unwrap();
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let writer = store
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    let (mut provider, handle) = start(
        live.clone(),
        writer,
        transports.connector,
        transports.http,
        transports.clock,
        Box::new(Silent),
        shutdown,
    )
    .unwrap();
    let delivered = drain(&mut provider);
    drop(provider);
    let (writer, summary) = handle.join().unwrap();
    writer.close().unwrap();
    assert_eq!(summary.stats.records, 3 + 2);

    let run = LiveRun {
        run_id: live.run_id.clone(),
        symbol: live.symbol.clone(),
        hold_back_ms: live.hold_back_ms,
        oi_retime_ms: live.oi_retime_ms,
        seeds: BTreeMap::new(),
        streams: live.streams.clone(),
        started_at_ms: t,
        ended_at_ms: Some(clock.now_utc_ns() / 1_000_000),
        clean_records: Some(summary.stats.records),
    };
    let replay = LiveReplay::new(&store, "binance-um", "BTCUSDT", vec![run]);
    let replayed = drain(&mut replay.replay(window(D0, D0 + DAY_MS)).unwrap().stream);
    assert_eq!(replayed, delivered);
    assert_no_look_ahead(&replayed);

    // Sync, the reconnect's gap, resync: the replayed book ends valid.
    let (book, steps) = book_steps(&replayed);
    assert_eq!(
        steps,
        [
            BookStep::Reset,
            BookStep::Applied,
            BookStep::Applied,
            BookStep::Invalidated(Invalidation::Gap(GapReason::Disconnected)),
            BookStep::Reset,
            BookStep::Applied,
        ]
    );
    assert!(book.is_valid());
    assert_eq!(book.last_update_id(), Some(50));
}

#[test]
fn mie_replay_carries_the_book_across_restarts_as_journaled() {
    let dir = TempDir::new("replay-depth-runs");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    let t1 = D0 + HOUR;
    let (connections, snapshots) = depth_script(t1);
    let first = ingest_depth(&config, t1, connections, snapshots, vec![]);
    let t2 = t1 + HOUR;
    let second = ingest_depth(
        &config,
        t2,
        vec![DepthConnection {
            frames: vec![depth(911, 920, 910, t2 + 10)],
            await_served: 1,
        }],
        vec![depth_snapshot(915, t2 + 5)],
        vec![],
    );
    assert_eq!((first.exit_code(), second.exit_code()), (0, 0));

    let all = window(D0, D0 + DAY_MS);
    let (outcome, text) = run_replay(&ReplayRequest {
        source: ReplaySource::Live(config.clone()),
        window: all,
    });
    assert!(outcome.pass, "{text}");
    assert_eq!(outcome.events, first.events + second.events);
    assert_eq!(outcome.domain_rejections, 0);
    assert!(
        text.contains("delivered OrderBook: events 7, gaps Disconnected 2"),
        "{text}"
    );

    // The replayed book gaps are the journaled ones, the restart's seed gap
    // included, and the book resyncs after each.
    let runs: Vec<LiveRun> = read_runs(&config.paths.journal, "binance-um")
        .unwrap()
        .iter()
        .map(|r| r.live_run())
        .collect();
    assert!(
        runs.iter()
            .all(|r| r.streams.contains(&BinanceStream::Depth))
    );
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let replayed = drain(
        &mut LiveReplay::new(&store, "binance-um", "BTCUSDT", runs)
            .replay(all)
            .unwrap()
            .stream,
    );
    let replayed_gaps: Vec<(String, i64, i64)> = replayed
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
    let journaled: Vec<(String, i64, i64)> = journal(&config)
        .iter()
        .filter(|l| l["type"] == "gap")
        .map(|l| {
            (
                l["reason"].as_str().unwrap().to_owned(),
                l["start"].as_i64().unwrap(),
                l["end"].as_i64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        replayed_gaps,
        [
            ("Disconnected".to_owned(), t1 + 100, t1 + 4_950),
            ("Disconnected".to_owned(), t1 + 5_000, t2 + 5),
        ]
    );
    assert_eq!(replayed_gaps, journaled);
    let (book, _) = book_steps(&replayed);
    assert!(book.is_valid());
    assert_eq!(book.last_update_id(), Some(920));
}

// ---------------------------------------------------------------------------
// Archive replay.

fn archive_config(dir: &Path) -> ArchiveConfig {
    ArchiveConfig::parse(&format!(
        r#"
[instrument]
symbol = "BTCUSDT"
[paths]
raw_root = "{}"
import_ledger = "{}"
staging = "{}"
[archive]
base_url = "https://fake.invalid"
streams = ["aggTrades", "fundingRate", "metrics", "trades"]
"#,
        dir.join("raw").display(),
        dir.join("ledger").display(),
        dir.join("staging").display()
    ))
    .expect("valid archive config")
}

/// Appends archive rows the way the importer files them (ADR-034 D3).
fn import_rows(config: &ArchiveConfig, rows: &[(ArchiveStream, String)]) {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let mut writer = store
        .writer(ARCHIVE_SOURCE, mie_cli::archive::rotation_policy())
        .unwrap();
    for (stream, row) in rows {
        let key = RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", stream.raw_name()).unwrap();
        let record = RawRecord {
            event_time: archive_record_time(*stream, row.as_bytes()).unwrap(),
            capture: None,
            payload: row.as_bytes().to_vec(),
        };
        writer.append(&key, record).unwrap();
    }
    writer.close().unwrap();
}

#[test]
fn an_archive_replay_is_exact_deterministic_and_marks_a_missing_day() {
    let dir = TempDir::new("replay-archive");
    let config = archive_config(dir.path());
    let (d0, d2) = (
        parse_day("2026-10-06").unwrap(),
        parse_day("2026-10-08").unwrap(),
    );
    let mut rows = Vec::new();
    for d in d0..=d2 {
        for h in [0, 8, 16] {
            let rate = if d == d0 && h == 8 {
                "-1.8E-7"
            } else {
                "0.00010000"
            };
            rows.push((
                ArchiveStream::FundingRate,
                format!("{},8,{rate}", d * DAY_MS + h * HOUR),
            ));
        }
    }
    // Trades and metrics on the outer days only: 2026-10-07 is missing.
    let mut id = 1;
    for (d, label) in [(d0, "2026-10-06"), (d2, "2026-10-08")] {
        for h in 0..24 {
            rows.push((
                ArchiveStream::AggTrades,
                format!(
                    "{id},85000.10,0.010,{id},{id},{},true",
                    d * DAY_MS + h * HOUR + 3
                ),
            ));
            id += 1;
        }
        // Published unsorted.
        for time in ["10:00:00", "02:30:00"] {
            rows.push((
                ArchiveStream::Metrics,
                format!(
                    "{label} {time},BTCUSDT,92849.1660000000000000,7734131259.6348000000000000,1.45104650,1.93356400,1.38112049,0.41920300"
                ),
            ));
        }
    }
    import_rows(&config, &rows);

    let request = ReplayRequest {
        source: ReplaySource::Archive {
            config: config.clone(),
            streams: None,
        },
        window: window(d0 * DAY_MS, (d2 + 1) * DAY_MS),
    };
    let (outcome, text) = run_replay(&request);
    let (again, text_again) = run_replay(&request);
    assert_eq!((outcome.clone(), text.clone()), (again, text_again));
    assert!(outcome.pass, "{text}");
    // `trades` is configured but opt-in for replay.
    assert!(
        text.contains("streams aggTrades,fundingRate,metrics\n"),
        "{text}"
    );
    assert!(text.contains("  missing day 2026-10-07"), "{text}");
    assert!(
        text.contains("delivered Trades: events 48, gaps MissingData 1"),
        "{text}"
    );
    assert!(
        text.contains("delivered Funding: events 9, gaps 0"),
        "{text}"
    );

    let store = ParquetRawStore::new(&config.paths.raw_root);
    let archive = mie_adapter_binance::archive::replay::ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[
            ArchiveStream::AggTrades,
            ArchiveStream::FundingRate,
            ArchiveStream::Metrics,
        ],
    );
    let opened = archive.replay(request.window).unwrap();
    assert_eq!(Some(&opened.dataset), outcome.dataset.as_ref());
    let mut stream = opened.stream;
    let events = drain(&mut stream);
    assert_no_look_ahead(&events);
    // The exponent row is exact: -1.8E-7 is -0.00000018.
    let rates: Vec<i64> = events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::FundingSettlement(s) if s.time.as_millis() == d0 * DAY_MS + 8 * HOUR => {
                Some(s.rate.units())
            }
            _ => None,
        })
        .collect();
    assert_eq!(rates, ["-0.00000018".parse::<Rate>().unwrap().units()]);
    // Metrics are delivered at the end of their interval.
    let oi: Vec<i64> = events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::OpenInterest(oi) => Some(oi.time.as_millis()),
            _ => None,
        })
        .collect();
    let at = |d: i64, h: i64, m: i64| d * DAY_MS + h * HOUR + m * 60_000 + 300_000;
    assert_eq!(
        oi,
        [at(d0, 2, 30), at(d0, 10, 0), at(d2, 2, 30), at(d2, 10, 0)]
    );
    let missing: Vec<_> = events
        .iter()
        .filter(|e| matches!(e, MarketEvent::FeedGap(g) if g.reason == GapReason::MissingData))
        .collect();
    // Trades and metrics each resume after the missing day.
    assert_eq!(missing.len(), 2, "{missing:?}");

    // bookDepth has no domain event: the replay fails, exit 1.
    let (refused, text) = run_replay(&ReplayRequest {
        source: ReplaySource::Archive {
            config,
            streams: Some(vec![ArchiveStream::BookDepth]),
        },
        window: request.window,
    });
    assert_eq!(refused.exit_code(), 1);
    assert!(text.contains("FAIL: "), "{text}");
}

#[test]
fn bounds_take_epoch_ms_or_inclusive_days() {
    let day = parse_day("2026-10-06").unwrap();
    assert_eq!(
        replay::parse_bound("--from", "2026-10-06", false),
        Ok(day * DAY_MS)
    );
    assert_eq!(
        replay::parse_bound("--to", "2026-10-06", true),
        Ok((day + 1) * DAY_MS)
    );
    assert_eq!(replay::parse_bound("--to", "1234", true), Ok(1_234));
    assert!(replay::parse_bound("--to", "06.10.2026", true).is_err());
}
