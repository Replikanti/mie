//! The full `mie ingest` composition offline: scripted WebSocket frames
//! into a real Parquet raw store, journal and core.
//!
//! The warm start (ADR-052 D3, D7, D8, D13) runs offline too: an archive
//! imported before the cutover, chain runs whose prefix is compared with
//! `mie replay --source history`, the capture going on while the warm-up is
//! held, the cold cases, the memory guard and an uncrossable hole.

mod common;

use common::{
    CUTOVER, D0, DepthConnection, HOUR, ORIGIN, TempDir, agg, archive_trades, config, config_with,
    depth, depth_snapshot, history_archive_config, history_config, import_rows, ingest,
    ingest_depth, journal, journaled_run, write_records,
};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
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
// The warm start (ADR-052): a scripted capture of trades, mark prices and
// open interest whose steps run in one global order.

const AGG: &str = "btcusdt@aggTrade";
const MARK: &str = "btcusdt@markPrice@1s";

/// How long a scripted wait may take in real time before the test fails.
const WAIT_DEADLINE: Duration = Duration::from_secs(30);

/// A wall clock only the script moves; every other sleep is a short real
/// one.
struct ScriptClock(Mutex<i64>);

impl Clock for ScriptClock {
    fn now_utc_ns(&self) -> i64 {
        *self.0.lock().unwrap()
    }

    fn monotonic_ns(&self) -> u64 {
        *self.0.lock().unwrap() as u64
    }

    fn sleep(&self, _: Duration) {
        std::thread::sleep(Duration::from_millis(1));
    }
}

/// One step of a capture script, taken in order across all connections.
enum Step {
    /// A frame on the WebSocket path.
    Frame(&'static str, String),
    /// Waits until this many open-interest polls were answered, then lets
    /// the poller hand the last one to the capture.
    Polled(u64),
    /// Advances the clock by this many ms: the capture's 1 s seal cadence
    /// passes.
    Advance(i64),
    /// Waits until the warm-up has its result ([`WarmUpHold::after`]).
    Gate,
    /// Runs a test action in the script's order.
    Run(Box<dyn FnOnce() + Send>),
}

/// The shared script: steps, the path whose frame was handed out but not yet
/// passed to the capture, the open-interest answers and the gate.
struct Script {
    steps: Mutex<(VecDeque<Step>, Option<String>)>,
    clock: ScriptClock,
    bodies: Mutex<VecDeque<String>>,
    polled: AtomicU64,
    gate: AtomicBool,
    shutdown: Arc<AtomicBool>,
}

struct Ws(Arc<Script>);

impl WsConnector for Ws {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        Ok(Box::new(Scripted {
            path: url.rsplit('/').next().unwrap_or_default().to_owned(),
            script: Arc::clone(&self.0),
        }))
    }
}

struct Scripted {
    path: String,
    script: Arc<Script>,
}

impl WsConnection for Scripted {
    fn read(&mut self) -> ReadOutcome {
        let deadline = Instant::now() + WAIT_DEADLINE;
        let script = &self.script;
        loop {
            assert!(Instant::now() < deadline, "the capture script stalled");
            let mut guard = script.steps.lock().unwrap();
            let (steps, in_flight) = &mut *guard;
            // A connection reads again only after it passed its last frame
            // to the capture: the next step may go to another connection.
            if in_flight.as_deref() == Some(self.path.as_str()) {
                *in_flight = None;
            }
            let idle = |guard| {
                drop(guard);
                std::thread::sleep(Duration::from_millis(1));
                ReadOutcome::Control
            };
            if in_flight.is_some() {
                return idle(guard);
            }
            match steps.front() {
                None => {
                    script.shutdown.store(true, Ordering::Relaxed);
                    return idle(guard);
                }
                Some(Step::Frame(path, _)) if *path == self.path => {
                    let Some(Step::Frame(_, frame)) = steps.pop_front() else {
                        unreachable!()
                    };
                    *in_flight = Some(self.path.clone());
                    return ReadOutcome::Frame(frame.into_bytes());
                }
                Some(Step::Advance(ms)) => {
                    *script.clock.0.lock().unwrap() += ms * 1_000_000;
                    steps.pop_front();
                }
                Some(Step::Polled(n)) if script.polled.load(Ordering::SeqCst) >= *n => {
                    // Holding the steps: nothing else is delivered before
                    // the poller has handed the sample to the capture.
                    std::thread::sleep(Duration::from_millis(50));
                    steps.pop_front();
                }
                Some(Step::Gate) if script.gate.load(Ordering::SeqCst) => {
                    steps.pop_front();
                }
                Some(Step::Run(_)) => {
                    let Some(Step::Run(action)) = steps.pop_front() else {
                        unreachable!()
                    };
                    action();
                }
                Some(_) => return idle(guard),
            }
        }
    }

    fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}
}

struct Oi(Arc<Script>);

impl HttpGet for Oi {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        assert!(
            url.contains("/fapi/v1/openInterest?symbol=BTCUSDT"),
            "{url}"
        );
        let body = self
            .0
            .bodies
            .lock()
            .unwrap()
            .pop_front()
            .ok_or("script exhausted")?;
        self.0.polled.fetch_add(1, Ordering::SeqCst);
        Ok((200, body.into_bytes()))
    }
}

/// Runs one offline ingest at `start_ms` over `steps`, answering the
/// open-interest polls with `open_interest`; `before` holds the warm-up.
/// The script's `Gate` opens once the warm-up has its result.
fn warm_ingest(
    config: &IngestConfig,
    start_ms: i64,
    steps: Vec<Step>,
    open_interest: Vec<String>,
    before: Option<Box<dyn FnOnce() + Send>>,
) -> IngestOutcome {
    let shutdown = Arc::new(AtomicBool::new(false));
    let script = Arc::new(Script {
        steps: Mutex::new((steps.into(), None)),
        clock: ScriptClock(Mutex::new(start_ms * 1_000_000)),
        bodies: Mutex::new(open_interest.into()),
        polled: AtomicU64::new(0),
        gate: AtomicBool::new(false),
        shutdown: Arc::clone(&shutdown),
    });
    let gate = Arc::clone(&script);
    let hold = WarmUpHold {
        before,
        after: Some(Box::new(move || gate.gate.store(true, Ordering::SeqCst))),
    };
    let clock: Arc<dyn Clock> = Arc::new(ClockOf(Arc::clone(&script)));
    let transports = Transports {
        connector: Arc::new(Ws(Arc::clone(&script))),
        http: Arc::new(Oi(Arc::clone(&script))),
        clock,
    };
    mie_cli::ingest::run_with_hold(config, transports, shutdown, hold).expect("ingest starts")
}

/// The script's clock as the run's clock.
struct ClockOf(Arc<Script>);

impl Clock for ClockOf {
    fn now_utc_ns(&self) -> i64 {
        self.0.clock.now_utc_ns()
    }

    fn monotonic_ns(&self) -> u64 {
        self.0.clock.monotonic_ns()
    }

    fn sleep(&self, d: Duration) {
        self.0.clock.sleep(d);
    }
}

fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

fn oi(time: i64) -> String {
    format!(r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{time}}}"#)
}

/// The archive of the seam: three days of hourly aggregate trades from the
/// origin, the last one 200 ms before the cutover, and one metrics row on
/// the day before the cutover; written to `dir/archive.toml`.
fn import_archive(dir: &Path) {
    let archive = history_archive_config(dir, r#""aggTrades", "metrics""#);
    let mut rows = Vec::new();
    let mut id = 1;
    for d in 0..3 {
        let last = (d == 2).then_some(CUTOVER - 200);
        archive_trades(&mut rows, ORIGIN / DAY_MS + d, &mut id, last);
    }
    rows.push((
        ArchiveStream::Metrics,
        "2026-10-05 12:00:00,BTCUSDT,93000.0000000000000000,7734131259.6348000000000000,1.45,1.93,1.38,0.41"
            .to_owned(),
    ));
    import_rows(&archive, &rows);
}

/// An ingest configuration under `dir` capturing trades, mark prices and
/// open interest, sealing every second, with the history of
/// [`import_archive`] and the memory guard `max_run_records`.
fn warm_config(dir: &Path, max_run_records: Option<u64>) -> IngestConfig {
    let guard = max_run_records.map_or(String::new(), |max| format!("max_run_records = {max}\n"));
    IngestConfig::parse(&format!(
        r#"
[instrument]
symbol = "BTCUSDT"
source = "binance-um"

[paths]
raw_root = "{}"
journal = "{}"

[binance]
ws_base_url = "wss://fake.invalid/ws"
rest_base_url = "https://fake.invalid"
streams = ["aggTrade", "markPrice", "openInterest"]

[capture]
hold_back_ms = 750
seal_interval_secs = 1

[history]
origin = "2026-10-03"
cutover = "2026-10-06"
archive_config = "{}"
{guard}"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display(),
        dir.join("archive.toml").display(),
    ))
    .expect("valid test config")
}

/// A chain run's capture from `t` (on a 10 s poll slot): its open-interest
/// sample first, trades and mark prices, a seal, then the gate, and the
/// rest, ending a minute later. Trade ids from `id`.
fn run_steps(t: i64, id: u64) -> Vec<Step> {
    vec![
        Step::Polled(1),
        Step::Frame(AGG, agg(id, t + 10)),
        Step::Frame(MARK, mark(t + 20)),
        Step::Frame(AGG, agg(id + 1, t + 30)),
        Step::Frame(MARK, mark(t + 1_020)),
        Step::Frame(AGG, agg(id + 2, t + 1_200)),
        Step::Advance(1_500),
        Step::Frame(AGG, agg(id + 3, t + 2_500)),
        Step::Gate,
        Step::Frame(AGG, agg(id + 4, t + 3_000)),
        Step::Frame(MARK, mark(t + 3_020)),
        Step::Frame(AGG, agg(id + 5, t + 65_000)),
    ]
}

/// The journal lines of `kind` of run `run_id`.
fn lines_of<'a>(lines: &'a [Value], run_id: &str, kind: &str) -> Vec<&'a Value> {
    lines
        .iter()
        .filter(|l| l["type"] == kind && l["run_id"] == run_id)
        .collect()
}

/// The only journal line of `kind` of run `run_id`.
fn one<'a>(lines: &'a [Value], run_id: &str, kind: &str) -> &'a Value {
    let found = lines_of(lines, run_id, kind);
    assert_eq!(found.len(), 1, "{kind} of {run_id}: {found:?}");
    found[0]
}

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
