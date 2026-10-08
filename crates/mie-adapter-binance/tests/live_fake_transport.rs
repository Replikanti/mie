//! Live capture over scripted transports: no network anywhere.
//!
//! A scripted [`WsConnector`] and [`HttpGet`], a fake [`Clock`] and an
//! in-memory [`RawRecordSink`] drive the real connection loops, the capture
//! thread and the provider. The fake clock advances only when the scripted
//! stream's own thread sleeps or reads, so cadences are exact.

use mie_adapter_binance::book_sync::{BookTransition, SnapshotRejection};
use mie_adapter_binance::live::CaptureError;
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_adapter_binance::{
    BinanceStream, CaptureEvent, CaptureObserver, CaptureSummary, CheckpointResult, LiveConfig,
    SnapshotTrigger, start,
};
use mie_domain::event::{GapReason, MarketEvent, Stream};
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError};
use mie_ports::raw::{RawRecord, RawRecordSink, RawStoreError, RawStreamKey, SealedFile};
use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// 2026-10-06T00:00:00Z in ms.
const D0: i64 = 1_791_244_800_000;
const MS: i64 = 1_000_000;

/// A clock that only the thread named `driver` advances; other threads'
/// sleeps are short real sleeps.
struct FakeClock {
    state: Mutex<(i64, u64)>,
    driver: String,
}

impl FakeClock {
    fn new(start_utc_ms: i64, driver: &str) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new((start_utc_ms * MS, 0)),
            driver: driver.to_owned(),
        })
    }

    fn advance(&self, d: Duration) {
        let mut state = self.state.lock().unwrap();
        state.0 += d.as_nanos() as i64;
        state.1 += d.as_nanos() as u64;
    }

    fn mono_secs(&self) -> f64 {
        self.monotonic_ns() as f64 / 1e9
    }
}

impl Clock for FakeClock {
    fn now_utc_ns(&self) -> i64 {
        self.state.lock().unwrap().0
    }

    fn monotonic_ns(&self) -> u64 {
        self.state.lock().unwrap().1
    }

    fn sleep(&self, d: Duration) {
        if std::thread::current().name() == Some(self.driver.as_str()) {
            self.advance(d);
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// One scripted read.
#[derive(Clone)]
enum Step {
    /// A text frame.
    Frame(String),
    /// Advance the clock, then deliver a pong.
    Tick(Duration),
    /// Advance the clock, then time out.
    Silence(Duration),
    /// The peer closes.
    Close(&'static str),
    /// Request shutdown, then time out.
    Shutdown,
    /// Wait (in real time) until the sink holds this many records, then
    /// time out: lets the capture thread catch up with the clock.
    AwaitRecords(usize),
}

/// One scripted connect attempt.
enum Connect {
    Fail,
    Open(Vec<Step>),
}

#[derive(Default)]
struct WsLog {
    /// Every URL connected to, in order.
    urls: Vec<String>,
    /// (stream path, clock seconds) of every ping.
    pings: Vec<(String, f64)>,
    /// (stream path, clock seconds) of every close.
    closes: Vec<(String, f64)>,
}

struct FakeConnector {
    scripts: Mutex<BTreeMap<String, VecDeque<Connect>>>,
    clock: Arc<FakeClock>,
    shutdown: Arc<AtomicBool>,
    log: Arc<Mutex<WsLog>>,
    /// The capture's sink, for [`Step::AwaitRecords`].
    sink: Mutex<Option<Arc<Mutex<SinkLog>>>>,
}

impl FakeConnector {
    fn new(
        scripts: Vec<(&str, Vec<Connect>)>,
        clock: &Arc<FakeClock>,
        shutdown: &Arc<AtomicBool>,
    ) -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(
                scripts
                    .into_iter()
                    .map(|(path, s)| (path.to_owned(), s.into()))
                    .collect(),
            ),
            clock: Arc::clone(clock),
            shutdown: Arc::clone(shutdown),
            log: Arc::default(),
            sink: Mutex::default(),
        })
    }
}

impl WsConnector for FakeConnector {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        self.log.lock().unwrap().urls.push(url.to_owned());
        let path = url.rsplit('/').next().unwrap().to_owned();
        let next = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(&path)
            .and_then(VecDeque::pop_front);
        match next {
            Some(Connect::Open(steps)) => Ok(Box::new(FakeConnection {
                path,
                steps: steps.into(),
                clock: Arc::clone(&self.clock),
                shutdown: Arc::clone(&self.shutdown),
                log: Arc::clone(&self.log),
                sink: self.sink.lock().unwrap().clone(),
            })),
            Some(Connect::Fail) => Err("scripted connect failure".to_owned()),
            None => Err("script exhausted".to_owned()),
        }
    }
}

struct FakeConnection {
    path: String,
    steps: VecDeque<Step>,
    clock: Arc<FakeClock>,
    shutdown: Arc<AtomicBool>,
    log: Arc<Mutex<WsLog>>,
    sink: Option<Arc<Mutex<SinkLog>>>,
}

impl WsConnection for FakeConnection {
    fn read(&mut self) -> ReadOutcome {
        match self.steps.pop_front() {
            Some(Step::Frame(text)) => ReadOutcome::Frame(text.into_bytes()),
            Some(Step::Tick(d)) => {
                self.clock.advance(d);
                ReadOutcome::Control
            }
            Some(Step::Silence(d)) => {
                self.clock.advance(d);
                ReadOutcome::Timeout
            }
            Some(Step::Close(reason)) => ReadOutcome::Closed(reason.to_owned()),
            Some(Step::Shutdown) => {
                self.shutdown.store(true, Ordering::Relaxed);
                ReadOutcome::Timeout
            }
            Some(Step::AwaitRecords(n)) => {
                let sink = self.sink.as_ref().expect("sink registered");
                while sink.lock().unwrap().records.len() < n {
                    std::thread::sleep(Duration::from_millis(1));
                }
                ReadOutcome::Timeout
            }
            // An idle but healthy connection.
            None => {
                std::thread::sleep(Duration::from_millis(1));
                ReadOutcome::Control
            }
        }
    }

    fn ping(&mut self) -> Result<(), String> {
        let at = self.clock.mono_secs();
        self.log.lock().unwrap().pings.push((self.path.clone(), at));
        Ok(())
    }

    fn close(&mut self) {
        let at = self.clock.mono_secs();
        self.log
            .lock()
            .unwrap()
            .closes
            .push((self.path.clone(), at));
    }
}

/// One scripted HTTP response.
enum Http {
    Status(u16, String),
    Fail,
}

struct FakeHttp {
    script: Mutex<VecDeque<Http>>,
    clock: Arc<FakeClock>,
    shutdown: Arc<AtomicBool>,
    /// Clock ms of every request.
    requests: Mutex<Vec<i64>>,
}

impl HttpGet for FakeHttp {
    fn get(&self, _url: &str) -> Result<(u16, Vec<u8>), String> {
        self.requests
            .lock()
            .unwrap()
            .push(self.clock.now_utc_ns() / MS);
        // Every response takes 37 ms.
        self.clock.advance(Duration::from_millis(37));
        match self.script.lock().unwrap().pop_front() {
            Some(Http::Status(status, body)) => Ok((status, body.into_bytes())),
            Some(Http::Fail) => Err("scripted transport failure".to_owned()),
            None => {
                self.shutdown.store(true, Ordering::Relaxed);
                Err("script exhausted".to_owned())
            }
        }
    }
}

fn no_http(clock: &Arc<FakeClock>, shutdown: &Arc<AtomicBool>) -> Arc<FakeHttp> {
    Arc::new(FakeHttp {
        script: Mutex::default(),
        clock: Arc::clone(clock),
        shutdown: Arc::clone(shutdown),
        requests: Mutex::default(),
    })
}

#[derive(Debug, Default)]
struct SinkLog {
    records: Vec<(RawStreamKey, RawRecord)>,
    seal_calls: u64,
    unsealed: u64,
}

/// An in-memory sink; `fail_at` makes that append (0-based) fail.
#[derive(Debug, Clone)]
struct MemorySink {
    log: Arc<Mutex<SinkLog>>,
    fail_at: Option<usize>,
}

impl RawRecordSink for MemorySink {
    fn append(&mut self, stream: &RawStreamKey, record: RawRecord) -> Result<(), RawStoreError> {
        let mut log = self.log.lock().unwrap();
        if Some(log.records.len()) == self.fail_at {
            return Err(RawStoreError::Io("disk full".to_owned()));
        }
        log.records.push((stream.clone(), record));
        log.unsealed += 1;
        Ok(())
    }

    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        let mut log = self.log.lock().unwrap();
        log.seal_calls += 1;
        if log.unsealed == 0 {
            return Ok(Vec::new());
        }
        let rows = std::mem::take(&mut log.unsealed);
        let (stream, _) = log.records.last().unwrap().clone();
        Ok(vec![SealedFile {
            relative_path: format!("part-{}", log.seal_calls),
            stream,
            date: "2026-10-06".to_owned(),
            part: 0,
            rows,
            bytes: 1,
            min_event_time: EventTime::from_millis(0),
            max_event_time: EventTime::from_millis(0),
            sha256: "0".repeat(64),
        }])
    }
}

#[derive(Clone, Default)]
struct Recorder(Arc<Mutex<Vec<CaptureEvent>>>);

impl CaptureObserver for Recorder {
    fn on_event(&mut self, _at_utc_ns: i64, event: &CaptureEvent) {
        self.0.lock().unwrap().push(event.clone());
    }
}

fn agg(id: u64, time: i64) -> String {
    format!(
        r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"0.010","f":1,"l":1,"T":{time},"m":false}}"#,
        time + 100
    )
}

fn oi(time: i64) -> String {
    format!(r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{time}}}"#)
}

fn config(streams: &[BinanceStream]) -> LiveConfig {
    let mut config = LiveConfig::new("20261006T000000Z");
    config.ws_base_url = "wss://fake.invalid/ws".to_owned();
    config.ws_public_base_url = "wss://fake.invalid/public/ws".to_owned();
    config.rest_base_url = "https://fake.invalid".to_owned();
    config.streams = streams.to_vec();
    config.hold_back_ms = 0;
    config.seal_interval = Duration::from_secs(3_600);
    config.stats_interval = Duration::from_secs(3_600);
    config
}

struct Outcome {
    events: Vec<MarketEvent>,
    provider_error: Option<ProviderError>,
    joined: Result<(MemorySink, CaptureSummary), CaptureError>,
    observed: Vec<CaptureEvent>,
    sink: Arc<Mutex<SinkLog>>,
}

impl Outcome {
    fn records(&self) -> Vec<RawRecord> {
        let log = self.sink.lock().unwrap();
        log.records.iter().map(|(_, r)| r.clone()).collect()
    }

    fn backoffs(&self) -> Vec<u64> {
        self.observed
            .iter()
            .filter_map(|e| match e {
                CaptureEvent::Backoff { delay_ms, .. } => Some(*delay_ms),
                _ => None,
            })
            .collect()
    }

    fn gaps(&self) -> Vec<(Stream, GapReason)> {
        self.events
            .iter()
            .filter_map(|e| match e {
                MarketEvent::FeedGap(g) => Some((g.stream, g.reason)),
                _ => None,
            })
            .collect()
    }
}

/// Runs a capture to completion, checking on every delivered trade that its
/// record was appended first.
fn run(
    config: LiveConfig,
    connector: Arc<FakeConnector>,
    http: Arc<dyn HttpGet>,
    clock: Arc<FakeClock>,
    shutdown: Arc<AtomicBool>,
    fail_at: Option<usize>,
) -> Outcome {
    let sink_log = Arc::new(Mutex::new(SinkLog::default()));
    *connector.sink.lock().unwrap() = Some(Arc::clone(&sink_log));
    let sink = MemorySink {
        log: Arc::clone(&sink_log),
        fail_at,
    };
    let recorder = Recorder::default();
    let (mut provider, handle) = start(
        config,
        sink,
        connector,
        http,
        clock,
        Box::new(recorder.clone()),
        shutdown,
    )
    .expect("start");
    let mut events = Vec::new();
    let provider_error = loop {
        match provider.next_event() {
            Ok(Some(event)) => {
                if let MarketEvent::BookSnapshot(snapshot) = &event {
                    let needle = format!(r#""lastUpdateId":{},"#, snapshot.last_update_id);
                    let log = sink_log.lock().unwrap();
                    assert!(
                        log.records
                            .iter()
                            .any(|(_, r)| String::from_utf8_lossy(&r.payload).contains(&needle)),
                        "snapshot {} delivered before its record was appended",
                        snapshot.last_update_id
                    );
                }
                if let MarketEvent::Trade(trade) = &event {
                    let needle = format!(r#""a":{},"#, trade.trade_id);
                    let log = sink_log.lock().unwrap();
                    assert!(
                        log.records
                            .iter()
                            .any(|(_, r)| String::from_utf8_lossy(&r.payload).contains(&needle)),
                        "trade {} delivered before its record was appended",
                        trade.trade_id
                    );
                }
                events.push(event);
            }
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    };
    drop(provider);
    let joined = handle.join();
    let observed = recorder.0.lock().unwrap().clone();
    Outcome {
        events,
        provider_error,
        joined,
        observed,
        sink: sink_log,
    }
}

#[test]
fn backoff_doubles_and_resets_after_a_healthy_connection() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, "mie-aggTrade");
    let connector = FakeConnector::new(
        vec![(
            "btcusdt@aggTrade",
            vec![
                Connect::Fail,
                Connect::Fail,
                Connect::Fail,
                // Healthy for 61 s, then lost: the backoff resets.
                Connect::Open(vec![
                    Step::Frame(agg(1, D0 + 1_000)),
                    Step::Tick(Duration::from_secs(61)),
                    Step::Close("server restart"),
                ]),
                // Lost at once: the backoff keeps doubling.
                Connect::Open(vec![Step::Close("flap")]),
                Connect::Open(vec![Step::Frame(agg(2, D0 + 70_000)), Step::Shutdown]),
            ],
        )],
        &clock,
        &shutdown,
    );
    let http = no_http(&clock, &shutdown);
    let out = run(
        config(&[BinanceStream::AggTrade]),
        connector,
        http,
        clock,
        shutdown,
        None,
    );
    assert_eq!(out.backoffs(), [250, 500, 1_000, 250, 500]);
    let failed = out
        .observed
        .iter()
        .filter(|e| matches!(e, CaptureEvent::ConnectFailed { .. }))
        .count();
    assert_eq!(failed, 3);
    // Sessions count successful connections.
    let sessions: Vec<_> = out
        .observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::Connected { session_id, .. } => Some(session_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sessions,
        [
            "20261006T000000Z/aggTrade/1",
            "20261006T000000Z/aggTrade/2",
            "20261006T000000Z/aggTrade/3"
        ]
    );
    // Trades 1 and 2 are contiguous, but the reconnect is still a gap.
    assert_eq!(out.gaps(), [(Stream::Trades, GapReason::Disconnected)]);
    assert!(out.provider_error.is_none());
    assert!(out.joined.is_ok());
}

#[test]
fn silence_reconnects_and_pings_keep_their_cadence() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, "mie-aggTrade");
    let mut alive = vec![Step::Frame(agg(1, D0 + 1))];
    alive.extend(std::iter::repeat_n(Step::Tick(Duration::from_secs(10)), 10));
    // 100 s pass without any frame: more than the 90 s liveness timeout.
    alive.push(Step::Silence(Duration::from_secs(100)));
    let connector = FakeConnector::new(
        vec![(
            "btcusdt@aggTrade",
            vec![
                Connect::Open(alive),
                Connect::Open(vec![Step::Frame(agg(2, D0 + 300_000)), Step::Shutdown]),
            ],
        )],
        &clock,
        &shutdown,
    );
    let log = Arc::clone(&connector.log);
    let out = run(
        config(&[BinanceStream::AggTrade]),
        connector,
        no_http(&clock, &shutdown),
        clock,
        shutdown,
        None,
    );
    let pings: Vec<f64> = log.lock().unwrap().pings.iter().map(|p| p.1).collect();
    assert_eq!(pings, [30.0, 60.0, 90.0]);
    let reasons: Vec<_> = out
        .observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::Disconnected { reason, .. } => Some(reason.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(reasons, ["no frame for 90000 ms"]);
    assert_eq!(out.gaps(), [(Stream::Trades, GapReason::Disconnected)]);
}

/// Runs one capture of `streams` in which only `driver`'s script advances
/// the clock, and returns the clock times at which `driver` closed
/// connections plus the observed events.
fn rotation_run(
    streams: &[BinanceStream],
    driver: &str,
    driver_path: &str,
    first: usize,
    second: usize,
) -> (Vec<f64>, Vec<CaptureEvent>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, driver);
    let tick = Step::Tick(Duration::from_secs(10));
    let mut last: Vec<_> = std::iter::repeat_n(tick.clone(), second).collect();
    last.push(Step::Shutdown);
    // Every other stream has no script: its connects keep failing without
    // advancing the clock.
    let connector = FakeConnector::new(
        vec![(
            driver_path,
            vec![
                Connect::Open(std::iter::repeat_n(tick, first).collect()),
                Connect::Open(last),
            ],
        )],
        &clock,
        &shutdown,
    );
    let log = Arc::clone(&connector.log);
    let mut cfg = config(streams);
    cfg.max_connection_age = Duration::from_secs(100);
    cfg.rotation_stagger = Duration::from_secs(50);
    let out = run(
        cfg,
        connector,
        no_http(&clock, &shutdown),
        clock,
        shutdown,
        None,
    );
    let closes = log
        .lock()
        .unwrap()
        .closes
        .iter()
        .filter(|c| c.0 == driver_path)
        .map(|c| c.1)
        .collect();
    (closes, out.observed)
}

#[test]
fn planned_rotation_happens_at_max_age_plus_stagger() {
    // Index 0: rotated at the max age, then closed at shutdown 50 s later.
    let (closes, observed) = rotation_run(
        &[BinanceStream::AggTrade],
        "mie-aggTrade",
        "btcusdt@aggTrade",
        30,
        5,
    );
    assert_eq!(closes, [100.0, 150.0]);
    assert!(observed.iter().any(|e| matches!(
        e,
        CaptureEvent::PlannedRotation { session_id, .. }
            if session_id == "20261006T000000Z/aggTrade/1"
    )));

    // Index 1: 100 s + 1 × 50 s.
    let (closes, observed) = rotation_run(
        &[BinanceStream::AggTrade, BinanceStream::MarkPrice],
        "mie-markPrice",
        "btcusdt@markPrice@1s",
        30,
        10,
    );
    assert_eq!(closes, [150.0, 250.0]);
    let mark_events: Vec<_> = observed
        .iter()
        .filter(|e| match e {
            CaptureEvent::Connected { stream, .. }
            | CaptureEvent::PlannedRotation { stream, .. }
            | CaptureEvent::Disconnected { stream, .. }
            | CaptureEvent::Backoff { stream, .. } => *stream == BinanceStream::MarkPrice,
            _ => false,
        })
        .cloned()
        .collect();
    // A rotation reconnects at once: no disconnect, no backoff.
    assert_eq!(
        mark_events,
        [
            CaptureEvent::Connected {
                stream: BinanceStream::MarkPrice,
                session_id: "20261006T000000Z/markPrice/1".to_owned()
            },
            CaptureEvent::PlannedRotation {
                stream: BinanceStream::MarkPrice,
                session_id: "20261006T000000Z/markPrice/1".to_owned()
            },
            CaptureEvent::Connected {
                stream: BinanceStream::MarkPrice,
                session_id: "20261006T000000Z/markPrice/2".to_owned()
            },
        ]
    );
}

#[test]
fn unparsable_frames_are_persisted_and_the_stream_continues() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, "mie-aggTrade");
    let connector = FakeConnector::new(
        vec![(
            "btcusdt@aggTrade",
            vec![Connect::Open(vec![
                Step::Frame(agg(10, D0 + 1_000)),
                Step::Frame("not json".to_owned()),
                Step::Frame(agg(11, D0 + 1_001)),
                Step::Frame(agg(12, D0 + 1_002)),
                Step::Shutdown,
            ])],
        )],
        &clock,
        &shutdown,
    );
    let out = run(
        config(&[BinanceStream::AggTrade]),
        connector,
        no_http(&clock, &shutdown),
        clock,
        shutdown,
        None,
    );
    let records = out.records();
    assert_eq!(records.len(), 4);
    assert_eq!(records[1].payload, b"not json");
    // The unparsable frame is ordered at its receive time.
    assert_eq!(records[1].event_time, EventTime::from_millis(D0));
    let seqs: Vec<u64> = records
        .iter()
        .map(|r| r.capture.as_ref().unwrap().receive_seq)
        .collect();
    assert_eq!(seqs, [0, 1, 2, 3]);
    assert!(
        records
            .iter()
            .all(|r| { r.capture.as_ref().unwrap().session_id == "20261006T000000Z/aggTrade/1" })
    );
    let ids: Vec<u64> = out
        .events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::Trade(t) => Some(t.trade_id),
            _ => None,
        })
        .collect();
    assert_eq!(ids, [10, 11, 12]);
    assert!(out.observed.iter().any(|e| matches!(
        e,
        CaptureEvent::NormalizeError {
            receive_seq: 1,
            stream: BinanceStream::AggTrade,
            ..
        }
    )));
    let (_, summary) = out.joined.expect("clean capture");
    assert_eq!(summary.stats.records, 4);
    assert_eq!(summary.stats.time_fallbacks, 1);
    assert_eq!(
        summary.stats.pipeline.streams[&BinanceStream::AggTrade].normalize_errors,
        1
    );
    // Four frames and one connect event.
    assert_eq!(summary.stats.inbound.sent, 5);
    assert!(summary.stats.core.high_water >= 1);
}

#[test]
fn seals_run_on_their_cadence_and_at_shutdown() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, "mie-aggTrade");
    let mut steps = Vec::new();
    for i in 0..4 {
        steps.push(Step::Frame(agg(i, D0 + i as i64 * 1_000)));
        // The capture thread processes each frame before the clock moves
        // on by 6 s, past the 5 s seal cadence.
        steps.push(Step::AwaitRecords(i as usize + 1));
        steps.push(Step::Tick(Duration::from_secs(6)));
    }
    steps.push(Step::Frame(agg(4, D0 + 30_000)));
    steps.push(Step::Shutdown);
    let connector = FakeConnector::new(
        vec![("btcusdt@aggTrade", vec![Connect::Open(steps)])],
        &clock,
        &shutdown,
    );
    let mut cfg = config(&[BinanceStream::AggTrade]);
    cfg.seal_interval = Duration::from_secs(5);
    cfg.stats_interval = Duration::from_secs(10);
    let out = run(
        cfg,
        connector,
        no_http(&clock, &shutdown),
        clock,
        shutdown,
        None,
    );
    // Cadence seals after the 2nd, 3rd and 4th frame (the clock passed 6,
    // 12 and 18 s), then the shutdown seal; the 5th frame may or may not
    // see the 24 s mark first.
    let seal_calls = out.sink.lock().unwrap().seal_calls;
    assert!((4..=5).contains(&seal_calls), "{seal_calls}");
    let sealed_rows: u64 = out
        .observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::Sealed(files) => Some(files.iter().map(|f| f.rows).sum::<u64>()),
            _ => None,
        })
        .sum();
    // Every record ends up sealed, the last ones by the shutdown seal.
    assert_eq!(sealed_rows, 5);
    assert!(
        out.observed
            .iter()
            .any(|e| matches!(e, CaptureEvent::Stats(_)))
    );
    let (_, summary) = out.joined.expect("clean capture");
    assert_eq!(summary.stats.sealed_files, seal_files(&out.observed));
    assert_eq!(out.events.len(), 5);
    assert!(out.provider_error.is_none());
}

fn seal_files(observed: &[CaptureEvent]) -> u64 {
    observed
        .iter()
        .map(|e| match e {
            CaptureEvent::Sealed(files) => files.len() as u64,
            _ => 0,
        })
        .sum()
}

#[test]
fn a_failed_append_stops_the_capture_and_fails_the_provider() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0, "mie-aggTrade");
    let connector = FakeConnector::new(
        vec![(
            "btcusdt@aggTrade",
            vec![Connect::Open(vec![
                Step::Frame(agg(1, D0 + 1)),
                Step::Frame(agg(2, D0 + 2)),
                Step::Frame(agg(3, D0 + 3)),
            ])],
        )],
        &clock,
        &shutdown,
    );
    let out = run(
        config(&[BinanceStream::AggTrade]),
        connector,
        no_http(&clock, &shutdown),
        clock,
        Arc::clone(&shutdown),
        Some(1),
    );
    assert!(
        matches!(&out.provider_error, Some(ProviderError::Source(m)) if m.contains("disk full")),
        "{:?}",
        out.provider_error
    );
    assert_eq!(out.records().len(), 1);
    let error = out.joined.expect_err("capture failed");
    assert!(error.message.contains("disk full"));
    // Nothing after the failed record reached the core.
    assert!(out.events.iter().all(|e| match e {
        MarketEvent::Trade(t) => t.trade_id < 2,
        _ => true,
    }));
    assert!(shutdown.load(Ordering::Relaxed));
}

#[test]
fn open_interest_polls_align_back_off_and_never_persist_failures() {
    let shutdown = Arc::new(AtomicBool::new(false));
    // Start 3.456 s into a 10 s slot.
    let clock = FakeClock::new(D0 + 3_456, "mie-openInterest");
    let http = Arc::new(FakeHttp {
        script: Mutex::new(
            vec![
                Http::Status(200, oi(D0 + 10_030)),
                Http::Status(429, "{\"code\":-1003}".to_owned()),
                Http::Status(429, "{\"code\":-1003}".to_owned()),
                Http::Fail,
                Http::Status(200, oi(D0 + 90_030)),
                Http::Status(200, oi(D0 + 100_030)),
            ]
            .into(),
        ),
        clock: Arc::clone(&clock),
        shutdown: Arc::clone(&shutdown),
        requests: Mutex::default(),
    });
    let connector = FakeConnector::new(vec![], &clock, &shutdown);
    let mut cfg = config(&[BinanceStream::OpenInterest]);
    cfg.backoff_initial = Duration::from_secs(15);
    cfg.backoff_max = Duration::from_secs(30);
    let requests = Arc::clone(&http);
    let out = run(cfg, connector, http, clock, shutdown, None);
    let requests: Vec<i64> = requests
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|t| t - D0)
        .collect();
    // 10 s slots; a 429 at 20 s defers past 35 s, the next one past 70 s.
    assert_eq!(
        requests[..6],
        [10_000, 20_000, 40_000, 80_000, 90_000, 100_000]
    );
    let polls: Vec<_> = out
        .observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::OiPoll {
                request_time_ns,
                response_time_ns,
                status,
                persisted,
                ..
            } => Some((
                (request_time_ns / MS - D0),
                response_time_ns - request_time_ns,
                *status,
                *persisted,
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        polls[..6],
        [
            (10_000, 37 * MS, Some(200), true),
            (20_000, 37 * MS, Some(429), false),
            (40_000, 37 * MS, Some(429), false),
            (80_000, 37 * MS, None, false),
            (90_000, 37 * MS, Some(200), true),
            (100_000, 37 * MS, Some(200), true),
        ]
    );
    let records = out.records();
    assert_eq!(records.len(), 3);
    let sessions: Vec<_> = records
        .iter()
        .map(|r| r.capture.as_ref().unwrap().session_id.clone())
        .collect();
    assert_eq!(
        sessions,
        [
            "20261006T000000Z/openInterest/1",
            "20261006T000000Z/openInterest/4",
            "20261006T000000Z/openInterest/4"
        ]
    );
    // The receive time is the response time.
    assert_eq!(
        records[0].capture.as_ref().unwrap().receive_time_ns,
        (D0 + 10_037) * MS
    );
    // The missed samples are a gap on the open-interest series.
    assert_eq!(
        out.gaps(),
        [(Stream::OpenInterest, GapReason::Disconnected)]
    );
}

fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

/// Recomputes the pipeline from the sink's records in `receive_seq` order
/// with the given run parameters, as replay (#11) will from the raw store
/// plus the journal's `run_start`.
fn recompute(
    records: &[(RawStreamKey, RawRecord)],
    symbol: &str,
    hold_back_ms: i64,
    oi_retime_ms: i64,
    seeds: &BTreeMap<BinanceStream, EventTime>,
) -> Vec<MarketEvent> {
    let mut sorted: Vec<_> = records.to_vec();
    sorted.sort_by_key(|(_, r)| r.capture.as_ref().unwrap().receive_seq);
    let mut pipeline =
        mie_adapter_binance::Pipeline::new(symbol, hold_back_ms, oi_retime_ms, seeds);
    let mut out = Vec::new();
    for (key, record) in &sorted {
        let stream = BinanceStream::from_raw_name(key.stream()).unwrap();
        out.extend(pipeline.push(stream, record).events);
    }
    out.extend(pipeline.finish());
    out
}

#[test]
fn recomputing_from_the_sink_with_the_run_parameters_reproduces_live_output() {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0 + 40_000, "mie-aggTrade");
    let trades_1 = vec![
        Step::Frame(agg(1, D0 + 1_000)),
        Step::Frame(agg(2, D0 + 1_010)),
        // Id skip.
        Step::Frame(agg(4, D0 + 1_020)),
        Step::Frame("garbage".to_owned()),
        Step::Close("dropped"),
    ];
    let trades_2 = vec![
        // Reconnect with an overlapping duplicate.
        Step::Frame(agg(4, D0 + 1_020)),
        Step::Frame(agg(5, D0 + 1_500)),
        // Both streams' frames are persisted before shutdown.
        Step::AwaitRecords(13),
        Step::Shutdown,
    ];
    let marks = vec![
        Step::Frame(mark(D0 + 1_000)),
        // Disorder within the hold-back.
        Step::Frame(mark(D0 + 1_300)),
        Step::Frame(mark(D0 + 1_200)),
        Step::Frame("{\"e\":\"markPriceUpdate\"".to_owned()),
        Step::Frame(mark(D0 + 20_000)),
        // Late: 1000 was released once the watermark reached 20000.
        Step::Frame(mark(D0 + 500)),
        Step::Frame(mark(D0 + 21_000)),
    ];
    let connector = FakeConnector::new(
        vec![
            (
                "btcusdt@aggTrade",
                vec![Connect::Open(trades_1), Connect::Open(trades_2)],
            ),
            ("btcusdt@markPrice@1s", vec![Connect::Open(marks)]),
        ],
        &clock,
        &shutdown,
    );
    let mut cfg = config(&[BinanceStream::AggTrade, BinanceStream::MarkPrice]);
    cfg.hold_back_ms = 2_000;
    cfg.seeds = BTreeMap::from([(BinanceStream::AggTrade, EventTime::from_millis(D0 + 200))]);
    let params = cfg.clone();
    let out = run(
        cfg,
        connector,
        no_http(&clock, &shutdown),
        clock,
        shutdown,
        None,
    );
    assert!(out.provider_error.is_none());
    let records = out.sink.lock().unwrap().records.clone();
    assert_eq!(records.len(), 13);

    // Every fault shows up in what live delivered.
    let reasons: std::collections::BTreeSet<_> = out.gaps().into_iter().collect();
    for expected in [
        (Stream::Trades, GapReason::Disconnected),
        (Stream::Trades, GapReason::SequenceBreak),
        (Stream::MarkPrice, GapReason::MissingData),
        (Stream::MarkPrice, GapReason::LateEvent),
    ] {
        assert!(
            reasons.contains(&expected),
            "{expected:?} missing in {reasons:?}"
        );
    }
    let (_, summary) = out.joined.as_ref().expect("clean capture");
    assert_eq!(
        summary.stats.pipeline.streams[&BinanceStream::AggTrade].duplicates,
        1
    );

    // The raw records plus the run parameters reproduce it exactly.
    let replayed = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        params.oi_retime_ms,
        &params.seeds,
    );
    assert_eq!(replayed, out.events);

    // The parameters are inputs: other values give other output.
    let longer = recompute(
        &records,
        &params.symbol,
        60_000,
        params.oi_retime_ms,
        &params.seeds,
    );
    assert_ne!(longer, out.events);
    assert!(
        !longer
            .iter()
            .any(|e| matches!(e, MarketEvent::FeedGap(g) if g.reason == GapReason::LateEvent)),
        "a 60 s hold-back absorbs the late mark price"
    );
    let unseeded = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        params.oi_retime_ms,
        &BTreeMap::new(),
    );
    assert_ne!(unseeded, out.events);
}

/// Mark prices every second, each sent once the clock reaches its `E`.
/// `sent_upto` is the `E` of the last frame the capture has been handed.
struct PacedMarks {
    next: i64,
    end: i64,
    clock: Arc<FakeClock>,
    sent_upto: Arc<std::sync::atomic::AtomicI64>,
    pending: Option<i64>,
}

impl WsConnection for PacedMarks {
    fn read(&mut self) -> ReadOutcome {
        // The previous frame has been sent by now.
        if let Some(sent) = self.pending.take() {
            self.sent_upto.store(sent, Ordering::SeqCst);
        }
        if self.next <= self.end && self.next <= self.clock.now_utc_ns() / MS {
            let e = self.next;
            self.next += 1_000;
            self.pending = Some(e);
            return ReadOutcome::Frame(mark(e).into_bytes());
        }
        std::thread::sleep(Duration::from_millis(1));
        ReadOutcome::Control
    }

    fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}
}

struct PacedConnector(Mutex<Option<PacedMarks>>);

impl WsConnector for PacedConnector {
    fn connect(&self, _url: &str) -> Result<Box<dyn WsConnection>, String> {
        let marks = self.0.lock().unwrap().take().ok_or("script exhausted")?;
        Ok(Box::new(marks))
    }
}

/// Answers each poll at slot `S` with an open-interest `time` of `S - lag`,
/// once every mark price up to `S` was handed to the capture.
struct LaggedOi {
    lags_ms: Mutex<VecDeque<i64>>,
    clock: Arc<FakeClock>,
    sent_upto: Arc<std::sync::atomic::AtomicI64>,
    shutdown: Arc<AtomicBool>,
}

impl HttpGet for LaggedOi {
    fn get(&self, _url: &str) -> Result<(u16, Vec<u8>), String> {
        let slot = self.clock.now_utc_ns() / MS;
        let Some(lag) = self.lags_ms.lock().unwrap().pop_front() else {
            self.shutdown.store(true, Ordering::Relaxed);
            return Err("script exhausted".to_owned());
        };
        while self.sent_upto.load(Ordering::SeqCst) < slot {
            std::thread::sleep(Duration::from_millis(1));
        }
        self.clock.advance(Duration::from_millis(37));
        Ok((200, oi(slot - lag).into_bytes()))
    }
}

/// A capture of mark prices plus open interest polled with `lags_ms`, at
/// the shipped defaults or with another re-time allowance.
fn lagged_oi_run(lags_ms: &[i64], oi_retime_ms: Option<i64>) -> (Outcome, LiveConfig) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0 + 3_000, "mie-openInterest");
    let sent_upto = Arc::new(std::sync::atomic::AtomicI64::new(0));
    let marks = PacedMarks {
        next: D0 + 3_000,
        end: D0 + 200_000,
        clock: Arc::clone(&clock),
        sent_upto: Arc::clone(&sent_upto),
        pending: None,
    };
    let http = Arc::new(LaggedOi {
        lags_ms: Mutex::new(lags_ms.iter().copied().collect()),
        clock: Arc::clone(&clock),
        sent_upto,
        shutdown: Arc::clone(&shutdown),
    });
    let connector = Arc::new(PacedConnector(Mutex::new(Some(marks))));
    // The shipped defaults: 2000 ms hold-back, 10 000 ms re-time allowance.
    let mut cfg = config(&[BinanceStream::MarkPrice, BinanceStream::OpenInterest]);
    cfg.hold_back_ms = LiveConfig::new("r").hold_back_ms;
    cfg.oi_retime_ms = oi_retime_ms.unwrap_or(LiveConfig::new("r").oi_retime_ms);
    let params = cfg.clone();
    let sink_log = Arc::new(Mutex::new(SinkLog::default()));
    let recorder = Recorder::default();
    let (mut provider, handle) = start(
        cfg,
        MemorySink {
            log: Arc::clone(&sink_log),
            fail_at: None,
        },
        connector,
        http,
        clock,
        Box::new(recorder.clone()),
        shutdown,
    )
    .expect("start");
    let mut events = Vec::new();
    let provider_error = loop {
        match provider.next_event() {
            Ok(Some(event)) => events.push(event),
            Ok(None) => break None,
            Err(error) => break Some(error),
        }
    };
    drop(provider);
    let joined = handle.join();
    let observed = recorder.0.lock().unwrap().clone();
    let outcome = Outcome {
        events,
        provider_error,
        joined,
        observed,
        sink: sink_log,
    };
    (outcome, params)
}

fn open_interest_times(events: &[MarketEvent]) -> Vec<i64> {
    events
        .iter()
        .filter_map(|e| match e {
            MarketEvent::OpenInterest(oi) => Some(oi.time.as_millis() - D0),
            _ => None,
        })
        .collect()
}

#[test]
fn open_interest_lagging_the_poll_is_delivered_at_the_defaults() {
    // The live lag range: 4 to 8 s behind the poll.
    let (out, params) = lagged_oi_run(&[4_200, 6_000, 7_900, 5_100, 4_000, 7_000], None);
    assert!(out.provider_error.is_none());
    assert_eq!(params.oi_retime_ms, 10_000);
    // Every sample reaches the core, re-timed to the first open millisecond
    // after the last released event (a mark price at slot - 3000).
    assert_eq!(
        open_interest_times(&out.events),
        [7_001, 17_001, 27_001, 37_001, 47_001, 57_001]
    );
    assert!(out.gaps().is_empty(), "{:?}", out.gaps());
    let (_, summary) = out.joined.as_ref().expect("clean capture");
    let oi_stats = &summary.stats.pipeline.streams[&BinanceStream::OpenInterest];
    assert_eq!((oi_stats.events, oi_stats.retimed), (6, 6));
    assert!(oi_stats.max_lateness_ms >= 7_900);

    // The raw records plus the run parameters reproduce it; without the
    // allowance the same records give LateEvent gaps instead.
    let records = out.sink.lock().unwrap().records.clone();
    let replayed = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        params.oi_retime_ms,
        &params.seeds,
    );
    assert_eq!(replayed, out.events);
    let strict = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        0,
        &params.seeds,
    );
    assert_ne!(strict, out.events);
    assert!(open_interest_times(&strict).is_empty());
}

#[test]
fn open_interest_later_than_the_allowance_is_still_a_late_gap() {
    // The third sample trails its poll by 15 s: 12 s past the last
    // released event, beyond the 10 s allowance.
    let (out, _) = lagged_oi_run(&[5_000, 6_000, 15_000, 5_000], None);
    assert_eq!(open_interest_times(&out.events), [7_001, 17_001, 37_001]);
    assert_eq!(out.gaps(), [(Stream::OpenInterest, GapReason::LateEvent)]);
}

#[test]
fn a_non_default_retime_allowance_is_an_input_of_the_recompute() {
    // With 2000 ms, only samples at most 2 s past their slot are re-timed:
    // lag - 2999 is 1201, 3001, 4901 and 1101 ms here.
    let (out, params) = lagged_oi_run(&[4_200, 6_000, 7_900, 4_100], Some(2_000));
    assert_eq!(params.oi_retime_ms, 2_000);
    assert_eq!(open_interest_times(&out.events), [7_001, 37_001]);
    assert_eq!(
        out.gaps(),
        [
            (Stream::OpenInterest, GapReason::LateEvent),
            (Stream::OpenInterest, GapReason::LateEvent)
        ]
    );
    let records = out.sink.lock().unwrap().records.clone();
    let replayed = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        2_000,
        &params.seeds,
    );
    assert_eq!(replayed, out.events);
    let default = recompute(
        &records,
        &params.symbol,
        params.hold_back_ms,
        LiveConfig::new("r").oi_retime_ms,
        &params.seeds,
    );
    assert_ne!(default, out.events);
    assert_eq!(open_interest_times(&default).len(), 4);
}

/// A depth diff setting `bids` and `asks` (price, quantity) levels.
fn depth(first: u64, last: u64, prev: u64, time: i64, bids: &str, asks: &str) -> String {
    format!(
        r#"{{"e":"depthUpdate","E":{},"T":{time},"s":"BTCUSDT","ps":"BTCUSDT","U":{first},"u":{last},"pu":{prev},"b":[{bids}],"a":[{asks}]}}"#,
        time + 4
    )
}

/// A REST depth snapshot body.
fn depth_snapshot(last: u64, time: i64, bids: &str, asks: &str) -> String {
    format!(
        r#"{{"lastUpdateId":{last},"E":{},"T":{time},"bids":[{bids}],"asks":[{asks}]}}"#,
        time + 3
    )
}

/// Answers depth snapshot requests from a script and records each URL and
/// clock ms; asks for shutdown once the script is exhausted.
struct DepthHttp {
    script: Mutex<VecDeque<Http>>,
    clock: Arc<FakeClock>,
    shutdown: Arc<AtomicBool>,
    requests: Mutex<Vec<(String, i64)>>,
}

impl DepthHttp {
    fn new(script: Vec<Http>, clock: &Arc<FakeClock>, shutdown: &Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            script: Mutex::new(script.into()),
            clock: Arc::clone(clock),
            shutdown: Arc::clone(shutdown),
            requests: Mutex::default(),
        })
    }

    fn request_ms(&self) -> Vec<i64> {
        let requests = self.requests.lock().unwrap();
        requests.iter().map(|(_, ms)| ms - D0).collect()
    }
}

impl HttpGet for DepthHttp {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        self.requests
            .lock()
            .unwrap()
            .push((url.to_owned(), self.clock.now_utc_ns() / MS));
        self.clock.advance(Duration::from_millis(37));
        match self.script.lock().unwrap().pop_front() {
            Some(Http::Status(status, body)) => Ok((status, body.into_bytes())),
            Some(Http::Fail) => Err("scripted transport failure".to_owned()),
            None => {
                self.shutdown.store(true, Ordering::Relaxed);
                Err("script exhausted".to_owned())
            }
        }
    }
}

fn fetches(observed: &[CaptureEvent]) -> Vec<(SnapshotTrigger, i64, Option<u16>, bool)> {
    observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::DepthSnapshotFetch {
                trigger,
                request_ns,
                status,
                persisted,
                ..
            } => Some((*trigger, request_ns / MS - D0, *status, *persisted)),
            _ => None,
        })
        .collect()
}

fn book_transitions(observed: &[CaptureEvent]) -> Vec<BookTransition> {
    observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::Book { transition, .. } => Some(*transition),
            _ => None,
        })
        .collect()
}

#[test]
fn depth_syncs_on_requested_snapshots_and_audits_a_checkpoint() {
    let shutdown = Arc::new(AtomicBool::new(false));
    // The depth connection drives the clock.
    let clock = FakeClock::new(D0, "mie-depth");
    let bid = |p: &str, q: &str| format!(r#"["{p}","{q}"]"#);
    let both = |a: String, b: String| format!("{a},{b}");
    let script = vec![
        Step::Frame(depth(11, 20, 10, D0, &bid("100.00", "1.000"), "")),
        // A snapshot before the buffered diffs: rejected, asked again.
        Step::AwaitRecords(2),
        Step::AwaitRecords(3),
        Step::Frame(depth(21, 30, 20, D0 + 100, &bid("99.00", "2.000"), "")),
        // 31..=40 never arrive: the book desyncs and asks again.
        Step::Frame(depth(41, 50, 40, D0 + 300, "", &bid("101.00", "3.000"))),
        Step::AwaitRecords(6),
        Step::Frame(depth(51, 60, 50, D0 + 400, &bid("100.00", "4.000"), "")),
        // The checkpoint cadence.
        Step::Tick(Duration::from_secs(6)),
        Step::AwaitRecords(8),
        Step::Shutdown,
    ];
    let connector = FakeConnector::new(
        vec![("btcusdt@depth@100ms", vec![Connect::Open(script)])],
        &clock,
        &shutdown,
    );
    let http = DepthHttp::new(
        vec![
            Http::Status(
                200,
                depth_snapshot(
                    5,
                    D0 - 100,
                    &bid("100.00", "9.000"),
                    &bid("101.00", "5.000"),
                ),
            ),
            Http::Status(
                200,
                depth_snapshot(
                    15,
                    D0 - 50,
                    &bid("100.00", "1.000"),
                    &bid("101.00", "5.000"),
                ),
            ),
            Http::Status(
                200,
                depth_snapshot(
                    45,
                    D0 + 250,
                    &both(bid("100.00", "1.000"), bid("99.00", "2.000")),
                    &bid("101.00", "5.000"),
                ),
            ),
            Http::Status(
                200,
                depth_snapshot(
                    55,
                    D0 + 350,
                    &both(bid("100.00", "1.000"), bid("99.00", "2.000")),
                    &bid("101.00", "3.000"),
                ),
            ),
        ],
        &clock,
        &shutdown,
    );
    let mut cfg = config(&[BinanceStream::Depth, BinanceStream::DepthSnapshot]);
    cfg.depth_checkpoint_interval = Duration::from_secs(5);
    cfg.depth_snapshot_min_spacing = Duration::from_millis(1);
    let log = Arc::clone(&connector.log);
    let requests = Arc::clone(&http);
    let out = run(cfg, connector, http, clock, shutdown, None);
    assert!(out.provider_error.is_none());

    // Depth is served on the public route.
    assert_eq!(
        log.lock().unwrap().urls,
        ["wss://fake.invalid/public/ws/btcusdt@depth@100ms"]
    );
    let urls: Vec<String> = requests
        .requests
        .lock()
        .unwrap()
        .iter()
        .map(|(url, _)| url.clone())
        .collect();
    assert_eq!(
        urls.len(),
        4,
        "one request per want id, then the checkpoint"
    );
    assert!(
        urls.iter()
            .all(|u| u == "https://fake.invalid/fapi/v1/depth?symbol=BTCUSDT&limit=1000")
    );
    let triggers: Vec<SnapshotTrigger> = fetches(&out.observed).iter().map(|f| f.0).collect();
    assert_eq!(
        triggers,
        [
            SnapshotTrigger::Sync,
            SnapshotTrigger::Sync,
            SnapshotTrigger::Sync,
            SnapshotTrigger::Checkpoint
        ]
    );
    let transitions = book_transitions(&out.observed);
    assert_eq!(
        transitions[..5],
        [
            BookTransition::Desynced(GapReason::Disconnected),
            BookTransition::SnapshotRejected(SnapshotRejection::TooOld),
            BookTransition::Synced {
                last_update_id: 15,
                time: EventTime::from_millis(D0 - 50)
            },
            BookTransition::Desynced(GapReason::SequenceBreak),
            BookTransition::Synced {
                last_update_id: 45,
                time: EventTime::from_millis(D0 + 250)
            },
        ]
    );
    assert!(matches!(
        transitions[5],
        BookTransition::CheckpointEmitted {
            last_update_id: 55,
            ..
        }
    ));
    let checkpoints: Vec<&CheckpointResult> = out
        .observed
        .iter()
        .filter_map(|e| match e {
            CaptureEvent::BookCheckpoint(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(
        checkpoints,
        [&CheckpointResult::Matched {
            levels: 3,
            window_bid_bps: Some(100),
            window_ask_bps: Some(0),
        }]
    );
    assert_eq!(out.gaps(), [(Stream::OrderBook, GapReason::SequenceBreak)]);
    // Four snapshots persisted, sessions by fetch ordinal.
    let records = out.records();
    let snapshot_sessions: Vec<String> = records
        .iter()
        .map(|r| r.capture.as_ref().unwrap().session_id.clone())
        .filter(|s| s.contains("depthSnapshot"))
        .collect();
    assert_eq!(snapshot_sessions.len(), 4);
    assert!(
        snapshot_sessions
            .iter()
            .all(|s| s == "20261006T000000Z/depthSnapshot/1")
    );
}

#[test]
fn depth_checkpoints_keep_their_cadence_spacing_and_backoff() {
    let shutdown = Arc::new(AtomicBool::new(false));
    // The snapshot fetcher drives the clock; no diff ever arrives.
    let clock = FakeClock::new(D0, "mie-depthSnapshot");
    let body = |id: u64| depth_snapshot(id, D0, r#"["100.00","1.000"]"#, "");
    let http = DepthHttp::new(
        vec![
            Http::Status(200, body(1)),
            Http::Status(429, "{\"code\":-1003}".to_owned()),
            Http::Status(503, "unavailable".to_owned()),
            Http::Status(200, body(2)),
            Http::Fail,
            Http::Status(200, body(3)),
        ],
        &clock,
        &shutdown,
    );
    let connector = FakeConnector::new(
        vec![("btcusdt@depth@100ms", vec![Connect::Open(vec![])])],
        &clock,
        &shutdown,
    );
    let mut cfg = config(&[BinanceStream::Depth, BinanceStream::DepthSnapshot]);
    cfg.depth_checkpoint_interval = Duration::from_secs(10);
    cfg.depth_snapshot_min_spacing = Duration::from_secs(2);
    cfg.backoff_initial = Duration::from_secs(5);
    cfg.backoff_max = Duration::from_secs(20);
    let requests = Arc::clone(&http);
    let out = run(cfg, connector, http, clock, shutdown, None);
    // 10 s after the start; 10 s after the success (10 037); a 429 defers
    // past 20 074 + 5 000; a 503 only waits the 2 s spacing between request
    // starts; a transport failure too; 10 s after the last success the
    // script ends.
    assert_eq!(
        requests.request_ms(),
        [10_000, 20_037, 25_074, 27_074, 37_111, 39_111, 49_148]
    );
    assert_eq!(
        fetches(&out.observed),
        [
            (SnapshotTrigger::Checkpoint, 10_000, Some(200), true),
            (SnapshotTrigger::Checkpoint, 20_037, Some(429), false),
            (SnapshotTrigger::Checkpoint, 25_074, Some(503), false),
            (SnapshotTrigger::Checkpoint, 27_074, Some(200), true),
            (SnapshotTrigger::Checkpoint, 37_111, None, false),
            (SnapshotTrigger::Checkpoint, 39_111, Some(200), true),
            (SnapshotTrigger::Checkpoint, 49_148, None, false),
        ]
    );
    // Failures are never persisted; the ordinal grows after each.
    let sessions: Vec<String> = out
        .records()
        .iter()
        .map(|r| r.capture.as_ref().unwrap().session_id.clone())
        .collect();
    assert_eq!(
        sessions,
        [
            "20261006T000000Z/depthSnapshot/1",
            "20261006T000000Z/depthSnapshot/3",
            "20261006T000000Z/depthSnapshot/4",
        ]
    );
    // Without diffs nothing reaches the core.
    assert!(out.events.is_empty(), "{:?}", out.events);
}
