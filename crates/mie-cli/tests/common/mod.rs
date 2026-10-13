//! Shared fakes of the `mie-cli` integration tests: no network anywhere.

#![allow(dead_code)]

use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::archive::normalize::record_time as archive_record_time;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_adapter_parquet::{ParquetRawStore, RotationPolicy};
use mie_cli::config::{ArchiveConfig, History, IngestConfig};
use mie_cli::ingest::Transports;
use mie_domain::time::EventTime;
use mie_ports::raw::{Capture, RawRecord, RawRecordSink, RawStreamKey};
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

pub mod warm;

/// 2026-10-06T00:00:00Z in ms.
pub const D0: i64 = 1_791_244_800_000;
pub const HOUR: i64 = 3_600_000;
const MS: i64 = 1_000_000;

/// A fresh directory under the system temp dir, removed on drop.
pub struct TempDir(PathBuf);

impl TempDir {
    pub fn new(tag: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("mie-cli-it-{tag}-{}-{n}", std::process::id()));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create temp dir");
        Self(path)
    }

    pub fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// A configuration capturing only `aggTrade` into `dir`.
pub fn config(dir: &Path) -> IngestConfig {
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
streams = ["aggTrade"]

[capture]
hold_back_ms = 750
"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display()
    ))
    .expect("valid test config")
}

/// A clock that only the thread named `driver` advances.
pub struct FakeClock {
    utc_ns: Mutex<i64>,
    driver: String,
}

impl FakeClock {
    pub fn new(start_utc_ms: i64, driver: &str) -> Arc<Self> {
        Arc::new(Self {
            utc_ns: Mutex::new(start_utc_ms * MS),
            driver: driver.to_owned(),
        })
    }
}

impl Clock for FakeClock {
    fn now_utc_ns(&self) -> i64 {
        *self.utc_ns.lock().unwrap()
    }

    fn monotonic_ns(&self) -> u64 {
        (*self.utc_ns.lock().unwrap()) as u64
    }

    fn sleep(&self, d: Duration) {
        if std::thread::current().name() == Some(self.driver.as_str()) {
            *self.utc_ns.lock().unwrap() += d.as_nanos() as i64;
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// Each connection to a stream path plays the next frame list, then asks
/// for shutdown.
pub struct ScriptedConnector {
    scripts: Mutex<BTreeMap<String, VecDeque<Vec<String>>>>,
    shutdown: Arc<AtomicBool>,
}

impl ScriptedConnector {
    pub fn new(path: &str, connections: Vec<Vec<String>>, shutdown: &Arc<AtomicBool>) -> Arc<Self> {
        Arc::new(Self {
            scripts: Mutex::new(BTreeMap::from([(path.to_owned(), connections.into())])),
            shutdown: Arc::clone(shutdown),
        })
    }
}

impl WsConnector for ScriptedConnector {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        let path = url.rsplit('/').next().unwrap_or_default();
        let frames = self
            .scripts
            .lock()
            .unwrap()
            .get_mut(path)
            .and_then(VecDeque::pop_front)
            .ok_or("script exhausted")?;
        Ok(Box::new(Scripted {
            frames: frames.into(),
            shutdown: Arc::clone(&self.shutdown),
        }))
    }
}

struct Scripted {
    frames: VecDeque<String>,
    shutdown: Arc<AtomicBool>,
}

impl WsConnection for Scripted {
    fn read(&mut self) -> ReadOutcome {
        match self.frames.pop_front() {
            Some(frame) => ReadOutcome::Frame(frame.into_bytes()),
            None => {
                self.shutdown.store(true, Ordering::Relaxed);
                ReadOutcome::Timeout
            }
        }
    }

    fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}
}

/// An HTTP client that must not be called.
pub struct NoHttp;

impl HttpGet for NoHttp {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        panic!("unexpected HTTP request to {url}")
    }
}

/// One aggregate-trade frame.
pub fn agg(id: u64, time: i64) -> String {
    format!(
        r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"0.010","nq":"0.010","f":1,"l":1,"T":{time},"m":true,"st":1}}"#,
        time + 120
    )
}

/// Runs one offline ingest at `start_ms` playing `connections`.
pub fn ingest(
    config: &IngestConfig,
    start_ms: i64,
    connections: Vec<Vec<String>>,
) -> mie_cli::ingest::IngestOutcome {
    let shutdown = Arc::new(AtomicBool::new(false));
    let transports = Transports {
        connector: ScriptedConnector::new("btcusdt@aggTrade", connections, &shutdown),
        http: Arc::new(NoHttp),
        clock: FakeClock::new(start_ms, "mie-aggTrade"),
    };
    mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
}

/// The journal's lines as JSON.
pub fn journal(config: &IngestConfig) -> Vec<serde_json::Value> {
    std::fs::read_to_string(&config.paths.journal)
        .expect("read journal")
        .lines()
        .map(|l| serde_json::from_str(l).expect("journal line is JSON"))
        .collect()
}

/// A configuration capturing `streams` into `dir`, hold-back 750 ms. The
/// fake clocks barely move, so snapshot requests are spaced by 1 ms only.
pub fn config_with(dir: &Path, streams: &[&str]) -> IngestConfig {
    let streams: Vec<String> = streams.iter().map(|s| format!("{s:?}")).collect();
    IngestConfig::parse(&format!(
        r#"
[instrument]
symbol = "BTCUSDT"
source = "binance-um"

[paths]
raw_root = "{}"
journal = "{}"

[binance]
ws_base_url = "wss://fake.invalid/market/ws"
ws_public_base_url = "wss://fake.invalid/public/ws"
rest_base_url = "https://fake.invalid"
streams = [{}]

[capture]
hold_back_ms = 750
depth_snapshot_min_spacing_ms = 1
"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display(),
        streams.join(", ")
    ))
    .expect("valid test config")
}

/// One depth diff frame setting one bid level.
pub fn depth(first: u64, last: u64, prev: u64, time: i64) -> String {
    format!(
        r#"{{"e":"depthUpdate","E":{},"T":{time},"s":"BTCUSDT","ps":"BTCUSDT","U":{first},"u":{last},"pu":{prev},"b":[["85000.00","{last}.000"]],"a":[]}}"#,
        time + 4
    )
}

/// One REST depth snapshot body.
pub fn depth_snapshot(last: u64, time: i64) -> String {
    format!(
        r#"{{"lastUpdateId":{last},"E":{},"T":{time},"bids":[["85000.00","1.000"],["84999.90","2.000"]],"asks":[["85000.10","3.000"]]}}"#,
        time + 3
    )
}

/// Answers by URL: depth snapshots and open-interest bodies from their
/// scripts, in order; counts the depth snapshots served.
pub struct RoutedHttp {
    depth: Mutex<VecDeque<String>>,
    open_interest: Mutex<VecDeque<String>>,
    pub served: AtomicU64,
}

impl RoutedHttp {
    pub fn new(depth: Vec<String>, open_interest: Vec<String>) -> Arc<Self> {
        Arc::new(Self {
            depth: Mutex::new(depth.into()),
            open_interest: Mutex::new(open_interest.into()),
            served: AtomicU64::new(0),
        })
    }
}

impl HttpGet for RoutedHttp {
    fn get(&self, url: &str) -> Result<(u16, Vec<u8>), String> {
        let (script, depth) = if url.contains("/fapi/v1/depth?symbol=BTCUSDT&limit=") {
            (&self.depth, true)
        } else if url.contains("/fapi/v1/openInterest?symbol=BTCUSDT") {
            (&self.open_interest, false)
        } else {
            panic!("unexpected HTTP request to {url}")
        };
        let body = script
            .lock()
            .unwrap()
            .pop_front()
            .ok_or("script exhausted")?;
        if depth {
            self.served.fetch_add(1, Ordering::SeqCst);
        }
        Ok((200, body.into_bytes()))
    }
}

/// One scripted depth connection: its frames, then it waits until the
/// HTTP fake served `await_served` snapshots and closes; the last one asks
/// for shutdown instead.
pub struct DepthConnection {
    pub frames: Vec<String>,
    pub await_served: u64,
}

struct DepthConnector {
    connections: Mutex<VecDeque<DepthConnection>>,
    http: Arc<RoutedHttp>,
    shutdown: Arc<AtomicBool>,
}

impl WsConnector for DepthConnector {
    fn connect(&self, url: &str) -> Result<Box<dyn WsConnection>, String> {
        assert_eq!(url, "wss://fake.invalid/public/ws/btcusdt@depth@100ms");
        let mut connections = self.connections.lock().unwrap();
        let next = connections.pop_front().ok_or("script exhausted")?;
        Ok(Box::new(DepthScripted {
            frames: next.frames.into(),
            await_served: next.await_served,
            last: connections.is_empty(),
            http: Arc::clone(&self.http),
            shutdown: Arc::clone(&self.shutdown),
        }))
    }
}

struct DepthScripted {
    frames: VecDeque<String>,
    await_served: u64,
    last: bool,
    http: Arc<RoutedHttp>,
    shutdown: Arc<AtomicBool>,
}

impl WsConnection for DepthScripted {
    fn read(&mut self) -> ReadOutcome {
        if let Some(frame) = self.frames.pop_front() {
            return ReadOutcome::Frame(frame.into_bytes());
        }
        while self.http.served.load(Ordering::SeqCst) < self.await_served {
            std::thread::sleep(Duration::from_millis(1));
        }
        // Let the fetcher hand the snapshot to the capture first.
        std::thread::sleep(Duration::from_millis(50));
        if self.last {
            self.shutdown.store(true, Ordering::Relaxed);
            ReadOutcome::Timeout
        } else {
            ReadOutcome::Closed("scripted close".to_owned())
        }
    }

    fn ping(&mut self) -> Result<(), String> {
        Ok(())
    }

    fn close(&mut self) {}
}

/// The transports of an offline depth capture at `start_ms` and the
/// shutdown flag the last depth connection sets: the depth connections play
/// their frames, snapshots and open-interest bodies are served by URL.
pub fn depth_transports(
    start_ms: i64,
    connections: Vec<DepthConnection>,
    snapshots: Vec<String>,
    open_interest: Vec<String>,
) -> (Transports, Arc<AtomicBool>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let http = RoutedHttp::new(snapshots, open_interest);
    let transports = Transports {
        connector: Arc::new(DepthConnector {
            connections: Mutex::new(connections.into()),
            http: Arc::clone(&http),
            shutdown: Arc::clone(&shutdown),
        }),
        http,
        clock: FakeClock::new(start_ms, "mie-depth"),
    };
    (transports, shutdown)
}

/// Runs one offline ingest of depth (and, if configured, open interest) at
/// `start_ms` over [`depth_transports`].
pub fn ingest_depth(
    config: &IngestConfig,
    start_ms: i64,
    connections: Vec<DepthConnection>,
    snapshots: Vec<String>,
    open_interest: Vec<String>,
) -> mie_cli::ingest::IngestOutcome {
    let (transports, shutdown) = depth_transports(start_ms, connections, snapshots, open_interest);
    mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
}

/// A clock that the threads named in `drivers` advance by sleeping; other
/// threads' sleeps are short real sleeps. The open-interest poller drives
/// the poll cadence and the trade stream its own reconnect backoff, so
/// neither waits for the other.
pub struct DrivenClock {
    pub utc_ns: Mutex<i64>,
    pub drivers: [&'static str; 2],
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
pub struct Connections {
    pub scripts: Mutex<BTreeMap<String, VecDeque<Vec<String>>>>,
    pub pending: Arc<AtomicUsize>,
    pub last_path: String,
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

/// One raw record of [`write_records`].
pub struct RawLine {
    /// The raw stream name, e.g. `aggTrade`.
    pub stream: &'static str,
    pub receive_seq: u64,
    pub session_id: String,
    pub event_time_ms: i64,
    pub payload: String,
}

/// Writes `records` into the raw store of `config` (sealed, source
/// `binance-um`) and `journal` as its journal: a capture written directly,
/// without the ingest pipeline, so the fixture is exactly these records.
pub fn write_records(config: &IngestConfig, records: &[RawLine], journal: &[serde_json::Value]) {
    let mut writer = ParquetRawStore::new(&config.paths.raw_root)
        .writer("binance-um", RotationPolicy::default())
        .unwrap();
    for r in records {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", r.stream).unwrap();
        let record = RawRecord {
            event_time: EventTime::from_millis(r.event_time_ms),
            capture: Some(Capture {
                receive_time_ns: r.event_time_ms * MS + 120 * MS,
                receive_seq: r.receive_seq,
                session_id: r.session_id.clone(),
            }),
            payload: r.payload.clone().into_bytes(),
        };
        writer.append(&key, record).unwrap();
    }
    writer.close().unwrap();
    let text: String = journal.iter().map(|l| format!("{l}\n")).collect();
    std::fs::create_dir_all(config.paths.journal.parent().unwrap()).unwrap();
    std::fs::write(&config.paths.journal, text).unwrap();
}

// ---------------------------------------------------------------------------
// Archive rows and the history chain (ADR-052): the archive up to the
// cutover, then the live runs from it.

/// Appends archive rows the way the importer files them (ADR-034 D3).
pub fn import_rows(config: &ArchiveConfig, rows: &[(ArchiveStream, String)]) {
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

/// An archive configuration under `dir` replaying `streams`, written to
/// `dir/archive.toml` as `[history]` names it.
pub fn history_archive_config(dir: &Path, streams: &str) -> ArchiveConfig {
    let text = format!(
        r#"
[instrument]
symbol = "BTCUSDT"
[paths]
raw_root = "{}"
import_ledger = "{}"
staging = "{}"
[archive]
base_url = "https://fake.invalid"
streams = [{streams}]
"#,
        dir.join("raw").display(),
        dir.join("ledger").display(),
        dir.join("staging").display()
    );
    std::fs::write(dir.join("archive.toml"), &text).unwrap();
    ArchiveConfig::parse(&text).expect("valid archive config")
}

/// The ingest configuration of `dir` with `[history]` from `origin` to
/// `cutover` over `dir/archive.toml`.
pub fn history_config(dir: &Path, origin: &str, cutover: &str) -> IngestConfig {
    let mut config = config(dir);
    config.history = Some(History {
        origin: origin.to_owned(),
        cutover: cutover.to_owned(),
        archive_config: dir.join("archive.toml"),
        max_run_records: None,
    });
    config
}

/// Hourly archive aggregate trades over the day `d`, ids from `*id`, plus
/// one at `last` when given.
pub fn archive_trades(
    rows: &mut Vec<(ArchiveStream, String)>,
    d: i64,
    id: &mut u64,
    last: Option<i64>,
) {
    let times = (0..24).map(|h| d * DAY_MS + h * HOUR + 7).chain(last);
    for time in times {
        rows.push((
            ArchiveStream::AggTrades,
            format!("{id},85000.10,0.010,{id},{id},{time},false"),
        ));
        *id += 1;
    }
}

/// A clean run's journal lines and its aggTrade records at `times`, ids
/// from `first_id`.
pub fn journaled_run(
    run_id: &str,
    started: i64,
    ended: i64,
    first_id: u64,
    times: &[i64],
) -> (Vec<serde_json::Value>, Vec<RawLine>) {
    let records: Vec<RawLine> = times
        .iter()
        .enumerate()
        .map(|(i, &time)| RawLine {
            stream: "aggTrade",
            receive_seq: i as u64,
            session_id: format!("{run_id}/aggTrade/1"),
            event_time_ms: time,
            payload: agg(first_id + i as u64, time),
        })
        .collect();
    let lines = vec![
        serde_json::json!({"type": "run_start", "run_id": run_id, "at_ms": started,
            "symbol": "BTCUSDT", "source": "binance-um", "streams": ["aggTrade"],
            "hold_back_ms": 750, "oi_retime_ms": 10_000, "seeds": {}}),
        serde_json::json!({"type": "run_end", "run_id": run_id, "at_ms": ended, "exit_code": 0,
            "records": records.len()}),
    ];
    (lines, records)
}

/// 2026-10-06, the cutover of the seam fixture, three archive days after
/// its origin.
pub const CUTOVER: i64 = D0;
pub const ORIGIN: i64 = D0 - 3 * DAY_MS;

/// The seam fixture in a Parquet store: three archive days whose last
/// trade lies 200 ms before the cutover, plus metrics on the last day; R0
/// before the cutover; R1 right after it, whose first trade is older than
/// the archive's last one; R2 half an hour later.
pub fn seam_fixture(dir: &Path) -> IngestConfig {
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

    let config = history_config(dir, "2026-10-03", "2026-10-06");
    let c = CUTOVER;
    let mut journal = Vec::new();
    let mut records = Vec::new();
    let r0: Vec<i64> = (0..5).map(|i| c - 29 * 60_000 + i * 60_000).collect();
    let mut r1 = vec![c - 1_000];
    r1.extend((0..10).map(|i| c + 10_000 + i * 30_000));
    let r2: Vec<i64> = (0..10)
        .map(|i| c + 30 * 60_000 + 5_000 + i * 10_000)
        .collect();
    for (run_id, started, ended, first_id, times) in [
        ("20261005T233000Z", c - 30 * 60_000, c - 60_000, 500, r0),
        ("20261006T000010Z", c + 10_000, c + 20 * 60_000, 10_000, r1),
        (
            "20261006T003000Z",
            c + 30 * 60_000,
            c + 40 * 60_000,
            20_000,
            r2,
        ),
    ] {
        let (lines, run) = journaled_run(run_id, started, ended, first_id, &times);
        journal.extend(lines);
        records.extend(run);
    }
    write_records(&config, &records, &journal);
    config
}
