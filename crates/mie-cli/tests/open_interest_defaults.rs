//! Open interest at the shipped defaults: its REST `time` trails the poll
//! by seconds, yet every sample reaches the core and the capture passes
//! (ADR-032 D12).

mod common;

use common::{D0, FakeClock, HOUR, TempDir};
use mie_adapter_binance::transport::{Clock, HttpGet, ReadOutcome, WsConnection, WsConnector};
use mie_adapter_binance::{BinanceStream, Pipeline};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::config::IngestConfig;
use mie_cli::ingest::{IngestOutcome, Transports};
use mie_cli::journal::RunParameters;
use mie_cli::report;
use mie_domain::event::{GapReason, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecordSource, RawSelection, RawStreamKey};
use std::collections::{BTreeSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const MS: i64 = 1_000_000;

/// The shipped defaults: no `[capture]` section.
fn defaults_config(dir: &std::path::Path) -> IngestConfig {
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
rest_base_url = "https://fake.invalid"
streams = ["markPrice", "openInterest"]
"#,
        dir.join("raw").display(),
        dir.join("journal.jsonl").display()
    ))
    .expect("valid config")
}

fn mark(time: i64) -> String {
    format!(
        r#"{{"e":"markPriceUpdate","E":{time},"s":"BTCUSDT","p":"85001.00000000","i":"85002.50000000","r":"0.00010000","T":1791273600000}}"#
    )
}

/// Mark prices every second, each sent once the clock reaches its `E`.
struct PacedMarks {
    next: i64,
    clock: Arc<FakeClock>,
    sent_upto: Arc<AtomicI64>,
    pending: Option<i64>,
}

impl WsConnection for PacedMarks {
    fn read(&mut self) -> ReadOutcome {
        if let Some(sent) = self.pending.take() {
            self.sent_upto.store(sent, Ordering::SeqCst);
        }
        if self.next <= self.clock.now_utc_ns() / MS {
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

/// Answers the poll at slot `S` with `time = S - lag` once every mark
/// price up to `S` was handed to the capture; 37 ms per response.
struct LaggedOi {
    lags_ms: Mutex<VecDeque<i64>>,
    clock: Arc<FakeClock>,
    sent_upto: Arc<AtomicI64>,
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
        // The poller's thread drives the clock.
        self.clock.sleep(Duration::from_millis(37));
        let body = format!(
            r#"{{"symbol":"BTCUSDT","openInterest":"95253.475","time":{}}}"#,
            slot - lag
        );
        Ok((200, body.into_bytes()))
    }
}

fn ingest(config: &IngestConfig, lags_ms: &[i64]) -> IngestOutcome {
    let shutdown = Arc::new(AtomicBool::new(false));
    let clock = FakeClock::new(D0 + 3_000, "mie-openInterest");
    let sent_upto = Arc::new(AtomicI64::new(0));
    let marks = PacedMarks {
        next: D0 + 3_000,
        clock: Arc::clone(&clock),
        sent_upto: Arc::clone(&sent_upto),
        pending: None,
    };
    let transports = Transports {
        connector: Arc::new(PacedConnector(Mutex::new(Some(marks)))),
        http: Arc::new(LaggedOi {
            lags_ms: Mutex::new(lags_ms.iter().copied().collect()),
            clock: Arc::clone(&clock),
            sent_upto,
            shutdown: Arc::clone(&shutdown),
        }),
        clock,
    };
    mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
}

fn report(config: &IngestConfig) -> (bool, String) {
    let mut out = Vec::new();
    // The default limit of `mie capture-report`.
    let pass = report::run(config, D0, D0 + 24 * HOUR, 0.0, &mut out).expect("report runs");
    (pass, String::from_utf8(out).unwrap())
}

fn journal(config: &IngestConfig) -> Vec<serde_json::Value> {
    std::fs::read_to_string(&config.paths.journal)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

#[test]
fn lagging_open_interest_reaches_the_core_and_the_capture_passes() {
    let dir = TempDir::new("oi-defaults");
    let config = defaults_config(dir.path());
    // The lag range seen live: 4 to 8 s behind the poll.
    let outcome = ingest(&config, &[4_200, 6_000, 7_900, 5_100, 4_000, 7_000]);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let stats = &outcome.summary.as_ref().unwrap().stats.pipeline;
    let oi = &stats.streams[&BinanceStream::OpenInterest];
    assert_eq!((oi.records, oi.events, oi.retimed), (6, 6, 6));
    assert!(oi.gaps.is_empty(), "{:?}", oi.gaps);

    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(text.contains("openInterest"), "{text}");

    // The raw store plus run_start reproduce the delivery.
    let lines = journal(&config);
    let start = lines.iter().find(|l| l["type"] == "run_start").unwrap();
    let params = RunParameters::from_run_start(start).unwrap();
    assert_eq!((params.hold_back_ms, params.oi_retime_ms), (2_000, 10_000));
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let mut records = Vec::new();
    for stream in [BinanceStream::MarkPrice, BinanceStream::OpenInterest] {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", stream.raw_name()).unwrap();
        let selection = RawSelection::new(
            BTreeSet::from([key]),
            ReplayWindow {
                start: EventTime::from_millis(D0),
                end: EventTime::from_millis(D0 + 24 * HOUR),
            },
        )
        .unwrap();
        for file in store.select(&selection).unwrap().files {
            records.extend(store.read(&file).unwrap().into_iter().map(|r| (stream, r)));
        }
    }
    records.sort_by_key(|(_, r)| r.capture.as_ref().unwrap().receive_seq);
    let recompute = |oi_retime_ms: i64| {
        let mut pipeline = Pipeline::new(
            &params.symbol,
            params.hold_back_ms,
            oi_retime_ms,
            &params.seeds,
        );
        let mut events = Vec::new();
        for (stream, record) in &records {
            events.extend(pipeline.push(*stream, record).events);
        }
        events.extend(pipeline.finish());
        events
    };
    let count = |events: &[MarketEvent]| {
        let oi = events
            .iter()
            .filter(|e| matches!(e, MarketEvent::OpenInterest(_)))
            .count();
        let late = events
            .iter()
            .filter(|e| matches!(e, MarketEvent::FeedGap(g) if g.reason == GapReason::LateEvent))
            .count();
        (oi, late)
    };
    let end = lines.iter().find(|l| l["type"] == "run_end").unwrap();
    let replayed = recompute(params.oi_retime_ms);
    assert_eq!(count(&replayed), (6, 0));
    assert_eq!(
        replayed.len() as u64,
        end["streams"]["markPrice"]["events"].as_u64().unwrap()
            + end["streams"]["openInterest"]["events"].as_u64().unwrap()
    );
    // Without the allowance the same records lose every sample.
    assert_eq!(count(&recompute(0)), (0, 6));
}

#[test]
fn open_interest_later_than_the_allowance_fails_the_capture() {
    let dir = TempDir::new("oi-too-late");
    let config = defaults_config(dir.path());
    let outcome = ingest(&config, &[5_000, 6_000, 15_000, 5_000]);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("openInterest: 1 of 4 samples late"), "{text}");
}
