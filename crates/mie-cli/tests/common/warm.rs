//! The scripted capture of the warm-start tests (ADR-052): trades, mark
//! prices and open interest whose steps run in one global order, the
//! archive of the seam and the configuration with `[history]`. Shared by
//! `ingest_offline.rs` and `equivalence_offline.rs`.

use super::{CUTOVER, ORIGIN, agg, archive_trades, history_archive_config, import_rows};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_cli::config::IngestConfig;
use mie_cli::ingest::{IngestOutcome, Transports, WarmUpHold};
use serde_json::Value;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub const AGG: &str = "btcusdt@aggTrade";
pub const MARK: &str = "btcusdt@markPrice@1s";

/// How long a scripted wait may take in real time before the test fails.
pub const WAIT_DEADLINE: Duration = Duration::from_secs(30);

/// A wall clock only the script moves; every other sleep is a short real
/// one.
pub struct ScriptClock(pub Mutex<i64>);

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
pub enum Step {
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
pub struct Script {
    pub steps: Mutex<(VecDeque<Step>, Option<String>)>,
    pub clock: ScriptClock,
    pub bodies: Mutex<VecDeque<String>>,
    pub polled: AtomicU64,
    pub gate: AtomicBool,
    pub shutdown: Arc<AtomicBool>,
}

pub struct Ws(pub Arc<Script>);

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

pub struct Oi(pub Arc<Script>);

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
pub fn warm_ingest(
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
pub struct ClockOf(pub Arc<Script>);

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

pub fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

pub fn oi(time: i64) -> String {
    format!(r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{time}}}"#)
}

/// The archive of the seam: three days of hourly aggregate trades from the
/// origin, the last one 200 ms before the cutover, and one metrics row on
/// the day before the cutover; written to `dir/archive.toml`.
pub fn import_archive(dir: &Path) {
    import_archive_with(dir, |_| {});
}

/// [`import_archive`] with its rows changed by `mutate` before the import.
pub fn import_archive_with(dir: &Path, mutate: impl FnOnce(&mut Vec<(ArchiveStream, String)>)) {
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
    mutate(&mut rows);
    import_rows(&archive, &rows);
}

/// An ingest configuration under `dir` capturing trades, mark prices and
/// open interest, sealing every second, with the history of
/// [`import_archive`] and the memory guard `max_run_records`.
pub fn warm_config(dir: &Path, max_run_records: Option<u64>) -> IngestConfig {
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
pub fn run_steps(t: i64, id: u64) -> Vec<Step> {
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
pub fn lines_of<'a>(lines: &'a [Value], run_id: &str, kind: &str) -> Vec<&'a Value> {
    lines
        .iter()
        .filter(|l| l["type"] == kind && l["run_id"] == run_id)
        .collect()
}

/// The only journal line of `kind` of run `run_id`.
pub fn one<'a>(lines: &'a [Value], run_id: &str, kind: &str) -> &'a Value {
    let found = lines_of(lines, run_id, kind);
    assert_eq!(found.len(), 1, "{kind} of {run_id}: {found:?}");
    found[0]
}
