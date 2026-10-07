//! Shared fakes of the `mie-cli` integration tests: no network anywhere.

#![allow(dead_code)]

use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_cli::config::IngestConfig;
use mie_cli::ingest::Transports;
use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// Runs one offline ingest of depth (and, if configured, open interest) at
/// `start_ms`: the depth connections play their frames, snapshots and
/// open-interest bodies are served by URL.
pub fn ingest_depth(
    config: &IngestConfig,
    start_ms: i64,
    connections: Vec<DepthConnection>,
    snapshots: Vec<String>,
    open_interest: Vec<String>,
) -> mie_cli::ingest::IngestOutcome {
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
    mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
}
