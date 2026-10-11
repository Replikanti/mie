//! `mie capture-report` over offline captures.

mod common;

use common::{
    D0, DepthConnection, HOUR, RawLine, TempDir, agg, config, config_with, depth, depth_snapshot,
    ingest, ingest_depth, write_records,
};
use mie_cli::config::IngestConfig;
use mie_cli::report;

fn report(config: &IngestConfig) -> (bool, String) {
    let mut out = Vec::new();
    let pass = report::run(config, D0, D0 + 24 * HOUR, 0.0, &mut out).expect("report runs");
    (pass, String::from_utf8(out).unwrap())
}

#[test]
fn a_clean_capture_passes_and_an_unjournaled_id_skip_fails() {
    let dir = TempDir::new("report");
    let config = config(dir.path());
    let t1 = D0 + HOUR;
    // A skip (102 -> 104) and a reconnect, both journaled as gaps.
    let connections = vec![
        vec![
            agg(100, t1 + 10),
            agg(101, t1 + 20),
            agg(102, t1 + 30),
            agg(104, t1 + 50),
        ],
        vec![agg(105, t1 + 5_000), agg(106, t1 + 5_010)],
    ];
    let outcome = ingest_with_reconnect(&config, t1, connections);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    ingest(&config, t1 + HOUR, vec![vec![agg(200, t1 + HOUR + 1)]]);

    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(text.ends_with("PASS\n"), "{text}");
    assert!(text.contains("2 run(s)"), "{text}");

    // Drop the journaled sequence break: the skip is now undetected.
    let journal = std::fs::read_to_string(&config.paths.journal).unwrap();
    let tampered = drop_gaps(&journal, "20261006T010000Z", "SequenceBreak");
    assert_ne!(tampered, journal);
    std::fs::write(&config.paths.journal, &tampered).unwrap();
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("ids 102 -> 104"), "{text}");
    assert!(text.trim_end().ends_with("FAIL (1 problem(s))"), "{text}");

    // Drop the reconnect gap too: the session change is uncovered.
    let tampered = drop_gaps(&tampered, "20261006T010000Z", "Disconnected");
    std::fs::write(&config.paths.journal, tampered).unwrap();
    let (pass, text) = report(&config);
    assert!(!pass);
    assert!(text.contains("has no Disconnected gap"), "{text}");
}

/// The journal without the `gap` lines of `run` with `reason`.
fn drop_gaps(journal: &str, run: &str, reason: &str) -> String {
    journal
        .lines()
        .filter(|line| {
            let value: serde_json::Value = serde_json::from_str(line).unwrap();
            !(value["type"] == "gap" && value["run_id"] == run && value["reason"] == reason)
        })
        .map(|line| format!("{line}\n"))
        .collect()
}

/// Like [`ingest`], but the first connection closes instead of shutting
/// down, so the second one resumes in a new session.
fn ingest_with_reconnect(
    config: &IngestConfig,
    start_ms: i64,
    connections: Vec<Vec<String>>,
) -> mie_cli::ingest::IngestOutcome {
    use mie_adapter_binance::transport::{ReadOutcome, WsConnection, WsConnector};
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    struct Connector {
        connections: Mutex<VecDeque<Vec<String>>>,
        shutdown: Arc<AtomicBool>,
    }
    struct Connection {
        frames: VecDeque<String>,
        last: bool,
        shutdown: Arc<AtomicBool>,
    }
    impl WsConnector for Connector {
        fn connect(&self, _url: &str) -> Result<Box<dyn WsConnection>, String> {
            let mut connections = self.connections.lock().unwrap();
            let frames = connections.pop_front().ok_or("script exhausted")?;
            Ok(Box::new(Connection {
                frames: frames.into(),
                last: connections.is_empty(),
                shutdown: Arc::clone(&self.shutdown),
            }))
        }
    }
    impl WsConnection for Connection {
        fn read(&mut self) -> ReadOutcome {
            match self.frames.pop_front() {
                Some(frame) => ReadOutcome::Frame(frame.into_bytes()),
                None if self.last => {
                    self.shutdown.store(true, Ordering::Relaxed);
                    ReadOutcome::Timeout
                }
                None => ReadOutcome::Closed("scripted close".to_owned()),
            }
        }
        fn ping(&mut self) -> Result<(), String> {
            Ok(())
        }
        fn close(&mut self) {}
    }

    let shutdown = Arc::new(AtomicBool::new(false));
    let transports = mie_cli::ingest::Transports {
        connector: Arc::new(Connector {
            connections: Mutex::new(connections.into()),
            shutdown: Arc::clone(&shutdown),
        }),
        http: Arc::new(common::NoHttp),
        clock: common::FakeClock::new(start_ms, "mie-aggTrade"),
    };
    mie_cli::ingest::run(config, transports, shutdown).expect("ingest starts")
}

/// A journal of one clean run whose final counters carry `streams`.
fn journal_with(config: &IngestConfig, streams: serde_json::Value) {
    let run = "20261006T010000Z";
    let at = D0 + HOUR;
    let lines = [
        serde_json::json!({"type": "run_start", "run_id": run, "at_ms": at, "symbol": "BTCUSDT",
            "hold_back_ms": 2000, "seeds": {}}),
        serde_json::json!({"type": "run_end", "run_id": run, "at_ms": at + 60_000, "exit_code": 0,
            "error": null, "normalize_errors": 0, "domain_rejections": 0, "time_fallbacks": 0,
            "streams": streams}),
    ];
    let text: String = lines.iter().map(|l| format!("{l}\n")).collect();
    std::fs::create_dir_all(config.paths.journal.parent().unwrap()).unwrap();
    std::fs::write(&config.paths.journal, text).unwrap();
}

fn report_with_limit(config: &IngestConfig, limit: f64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = report::run(config, D0, D0 + 24 * HOUR, limit, &mut out).expect("report runs");
    (pass, String::from_utf8(out).unwrap())
}

#[test]
fn open_interest_that_is_always_late_fails() {
    let dir = TempDir::new("all-late");
    let config = config(dir.path());
    journal_with(
        &config,
        serde_json::json!({
            "markPrice": {"events": 115, "gaps": {}, "max_lateness_ms": 0},
            "openInterest": {"events": 0, "gaps": {"LateEvent": 12}, "max_lateness_ms": 7943},
        }),
    );
    let (pass, text) = report_with_limit(&config, 0.0);
    assert!(!pass, "{text}");
    assert!(
        text.contains(
            "openInterest: 12 of 12 samples late (100.00 %, limit 0.00 %), max lateness 7943 ms"
        ),
        "{text}"
    );
    assert!(text.contains("LateEvent=12"), "{text}");
    assert!(text.trim_end().ends_with("FAIL (1 problem(s))"), "{text}");
    // Even a generous limit below 100 % fails it.
    assert!(!report_with_limit(&config, 0.99).0);
    assert!(report_with_limit(&config, 1.0).0);
}

#[test]
fn the_late_fraction_is_judged_against_the_limit() {
    let dir = TempDir::new("some-late");
    let config = config(dir.path());
    journal_with(
        &config,
        serde_json::json!({
            "forceOrder": {"events": 9, "gaps": {"LateEvent": 1, "Disconnected": 2}, "max_lateness_ms": 2500},
        }),
    );
    let (pass, text) = report_with_limit(&config, 0.0);
    assert!(!pass, "{text}");
    assert!(
        text.contains("forceOrder: 1 of 10 samples late (10.00 %"),
        "{text}"
    );
    assert!(text.contains("Disconnected=2 LateEvent=1"), "{text}");
    let (pass, text) = report_with_limit(&config, 0.1);
    assert!(pass, "{text}");
}

/// A depth capture that reconnects once: synced on the first snapshot,
/// resynced behind a `Disconnected` gap on the second.
fn depth_capture_with_reconnect(config: &IngestConfig) -> mie_cli::ingest::IngestOutcome {
    let t1 = D0 + HOUR;
    ingest_depth(
        config,
        t1,
        vec![
            DepthConnection {
                frames: vec![depth(11, 20, 10, t1), depth(21, 30, 20, t1 + 100)],
                await_served: 1,
            },
            DepthConnection {
                frames: vec![depth(41, 50, 40, t1 + 5_000), depth(51, 60, 50, t1 + 5_100)],
                await_served: 2,
            },
        ],
        vec![depth_snapshot(15, t1 - 50), depth_snapshot(45, t1 + 4_950)],
        vec![],
    )
}

/// The journal plus `extra` lines.
fn append_lines(config: &IngestConfig, extra: &[serde_json::Value]) {
    let mut journal = std::fs::read_to_string(&config.paths.journal).unwrap();
    for line in extra {
        journal.push_str(&format!("{line}\n"));
    }
    std::fs::write(&config.paths.journal, journal).unwrap();
}

#[test]
fn a_clean_depth_capture_passes_and_its_reconnect_needs_the_book_gap() {
    let dir = TempDir::new("depth-report");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    let outcome = depth_capture_with_reconnect(&config);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    assert_eq!(outcome.domain_rejections, 0);

    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(
        text.contains("order book: syncs 2 | desyncs Disconnected=2"),
        "{text}"
    );

    // A mismatched checkpoint fails the run.
    let journal = std::fs::read_to_string(&config.paths.journal).unwrap();
    append_lines(
        &config,
        &[
            serde_json::json!({"type": "book_checkpoint", "run_id": "20261006T010000Z",
            "at_ms": D0 + HOUR + 6_000, "result": "mismatched", "levels": 40, "mismatches": 1,
            "examples": []}),
        ],
    );
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("book checkpoint mismatched"), "{text}");
    assert!(text.trim_end().ends_with("FAIL (1 problem(s))"), "{text}");

    // Without its OrderBook gap the reconnect is uncovered.
    let tampered = drop_gaps(&journal, "20261006T010000Z", "Disconnected");
    assert_ne!(tampered, journal);
    std::fs::write(&config.paths.journal, tampered).unwrap();
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("has no covering OrderBook gap"), "{text}");
}

#[test]
fn a_long_depth_run_needs_a_matched_checkpoint() {
    let dir = TempDir::new("depth-long");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    let t1 = D0 + HOUR;
    // Diffs over 130 s of exchange time, more than twice the 60 s default
    // checkpoint interval; the fake clock never reaches a checkpoint.
    let frames: Vec<String> = (0..14)
        .map(|i| {
            depth(
                10 * i + 11,
                10 * i + 20,
                10 * i + 10,
                t1 + 10_000 * i as i64,
            )
        })
        .collect();
    let outcome = ingest_depth(
        &config,
        t1,
        vec![DepthConnection {
            frames,
            await_served: 1,
        }],
        vec![depth_snapshot(15, t1 - 50)],
        vec![],
    );
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("without a matched checkpoint"), "{text}");

    append_lines(
        &config,
        &[
            serde_json::json!({"type": "book_checkpoint", "run_id": "20261006T010000Z",
            "at_ms": t1 + 70_000, "result": "matched", "levels": 40,
            "window_bid_bps": 12, "window_ask_bps": 9}),
        ],
    );
    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(
        text.contains("matched 1, unverifiable 0 | window min 9 bps, median 9 bps"),
        "{text}"
    );
}

#[test]
fn a_depth_reconnect_covered_by_a_gap_of_another_cause_passes() {
    // Rule 9 of ADR-038: when a pu break opened the unsynced period, the
    // reconnect inside it is announced by that period's SequenceBreak gap.
    let dir = TempDir::new("depth-cause");
    let config = config_with(dir.path(), &["depth", "depthSnapshot"]);
    let outcome = depth_capture_with_reconnect(&config);
    assert_eq!(outcome.exit_code(), 0, "{:?}", outcome.error);
    let journal = std::fs::read_to_string(&config.paths.journal).unwrap();
    for cause in ["SequenceBreak", "MissingData"] {
        let renamed: String = journal
            .lines()
            .map(|line| {
                let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
                if value["type"] == "gap" && value["stream"] == "OrderBook" {
                    value["reason"] = serde_json::json!(cause);
                }
                format!("{value}\n")
            })
            .collect();
        assert!(renamed.contains(&format!(r#""reason":"{cause}""#)));
        std::fs::write(&config.paths.journal, renamed).unwrap();
        let (pass, text) = report(&config);
        assert!(pass, "{cause}: {text}");
    }
}

/// The run of the check 7 fixtures.
const RUN: &str = "20261006T010000Z";
/// The first closed minute of the check 7 fixtures (open time, ms).
const M0: i64 = D0 + HOUR + 60_000;

/// An `aggTrade` frame with trade ids `first..=last`.
fn agg_range(id: u64, first: u64, last: u64, qty: &str, time: i64) -> String {
    format!(
        r#"{{"e":"aggTrade","E":{},"a":{id},"s":"BTCUSDT","p":"85000.10","q":"{qty}","nq":"{qty}","f":{first},"l":{last},"T":{time},"m":true,"st":1}}"#,
        time + 120
    )
}

/// A closed `kline_1m` frame of the minute opening at `open`.
fn closed_kline(open: i64, first: u64, last: u64, volume: &str) -> String {
    format!(
        r#"{{"e":"kline","E":{},"s":"BTCUSDT","k":{{"t":{open},"T":{},"s":"BTCUSDT","i":"1m","f":{first},"L":{last},"o":"85000.00","c":"85000.10","h":"85000.10","l":"85000.00","v":"{volume}","n":{},"x":true,"q":"0","V":"0","Q":"0","B":"0"}}}}"#,
        open + 60_050,
        open + 59_999,
        last - first + 1
    )
}

/// One run, one session per stream, consecutive aggregate ids 100 … 106.
/// Closed minutes M0 (trade ids 1000 … 1009, 0.750), M0 + 1 (1010 … 1019,
/// 1.125) and M0 + 2 (1020 … 1029, 0.300); aggregates 100 and 106 lie
/// before and after them, so the three blocks are inside the run's span.
/// `truncated` delivers aggregate 103 the way the stream delivered the
/// 2026-10-08 losses: same `a` and `f`, a smaller `l` and `q`, and the next
/// aggregate still starting at the old `l + 1`.
fn kline_capture(truncated: bool) -> Vec<RawLine> {
    let m = |k: i64| M0 + 60_000 * k;
    let a103 = if truncated {
        agg_range(103, 1010, 1012, "0.400", m(1) + 1_000)
    } else {
        agg_range(103, 1010, 1014, "1.000", m(1) + 1_000)
    };
    let aggs = [
        agg_range(100, 990, 999, "2.000", m(-1) + 30_000),
        agg_range(101, 1000, 1004, "0.500", m(0) + 1_000),
        agg_range(102, 1005, 1009, "0.250", m(0) + 40_000),
        a103,
        agg_range(104, 1015, 1019, "0.125", m(1) + 50_000),
        agg_range(105, 1020, 1029, "0.300", m(2) + 5_000),
        agg_range(106, 1030, 1031, "0.010", m(3) + 10_000),
    ];
    let klines = [
        closed_kline(m(0), 1000, 1009, "0.750"),
        closed_kline(m(1), 1010, 1019, "1.125"),
        closed_kline(m(2), 1020, 1029, "0.300"),
    ];
    let line = |stream, seq, payload: String| {
        let value: serde_json::Value = serde_json::from_str(&payload).unwrap();
        let time = if stream == "aggTrade" {
            value["T"].as_i64().unwrap()
        } else {
            value["k"]["T"].as_i64().unwrap()
        };
        RawLine {
            stream,
            receive_seq: seq,
            session_id: format!("{RUN}/{stream}/1"),
            event_time_ms: time,
            payload,
        }
    };
    let mut records: Vec<RawLine> = Vec::new();
    for (i, payload) in aggs.into_iter().enumerate() {
        records.push(line("aggTrade", 2 * i as u64, payload));
    }
    for (i, payload) in klines.into_iter().enumerate() {
        records.push(line("kline_1m", 2 * i as u64 + 1, payload));
    }
    records
}

/// The journal of [`RUN`]: a clean start and end, plus `gaps`.
fn kline_journal(gaps: &[serde_json::Value]) -> Vec<serde_json::Value> {
    let at = D0 + HOUR;
    let mut lines = vec![
        serde_json::json!({"type": "run_start", "run_id": RUN, "at_ms": at,
        "symbol": "BTCUSDT", "hold_back_ms": 2000, "seeds": {}}),
    ];
    lines.extend(gaps.iter().cloned());
    lines.push(
        serde_json::json!({"type": "run_end", "run_id": RUN, "at_ms": at + 600_000,
        "exit_code": 0, "error": null, "normalize_errors": 0, "domain_rejections": 0,
        "time_fallbacks": 0, "streams": {}}),
    );
    lines
}

/// A journaled gap of [`RUN`] on `stream` over the minute M0 + 1.
fn gap_over_m1(stream: &str, reason: &str) -> serde_json::Value {
    serde_json::json!({"type": "gap", "run_id": RUN, "at_ms": M0 + 90_000, "stream": stream,
        "start": M0 + 60_000 + 10_000, "end": M0 + 60_000 + 20_000, "reason": reason})
}

#[test]
fn a_clean_aggtrade_and_kline_capture_passes_check_7() {
    let dir = TempDir::new("c3-clean");
    let config = config_with(dir.path(), &["aggTrade", "kline_1m"]);
    write_records(&config, &kline_capture(false), &kline_journal(&[]));
    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(
        text.contains(
            "kline reconciliation: blocks 3 | conclusive 3 | inconclusive 0 (not consecutive 0, \
             run edge 0, gap 0, orphan 0) | flagged 0 (0 minutes) | difference 0.00000000\n"
        ),
        "{text}"
    );
    // Without kline_1m configured, check 7 is skipped.
    let (pass, text) = report(&config_with(dir.path(), &["aggTrade"]));
    assert!(pass, "{text}");
    assert!(
        text.contains("kline reconciliation: skipped (needs aggTrade and kline_1m)\n"),
        "{text}"
    );
}

#[test]
fn an_aggregate_truncated_inside_its_id_range_fails_only_check_7() {
    let dir = TempDir::new("c3-truncated");
    let config = config_with(dir.path(), &["aggTrade", "kline_1m"]);
    write_records(&config, &kline_capture(true), &kline_journal(&[]));
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    // Check 1 is silent: the aggregate ids stay consecutive.
    assert!(!text.contains("without a journaled gap"), "{text}");
    assert!(
        text.contains(&format!(
            "FAIL: kline_1m/aggTrade run {RUN}: minutes {}: kline volume 1.12500000, \
             aggregate quantity 0.52500000, difference 0.60000000\n",
            M0 + 60_000
        )),
        "{text}"
    );
    assert!(
        text.contains(
            "kline reconciliation: blocks 3 | conclusive 3 | inconclusive 0 (not consecutive 0, \
             run edge 0, gap 0, orphan 0) | flagged 1 (1 minutes) | difference 0.60000000\n"
        ),
        "{text}"
    );
    assert!(text.trim_end().ends_with("FAIL (1 problem(s))"), "{text}");
}

#[test]
fn liquidation_gaps_do_not_hide_a_loss() {
    // The 2026-10-08 journal: only Liquidations LateEvent gaps in the loss
    // window. They do not touch check 7.
    let dir = TempDir::new("c3-gaps");
    let config = config_with(dir.path(), &["aggTrade", "kline_1m"]);
    let journal = kline_journal(&[gap_over_m1("Liquidations", "LateEvent")]);
    write_records(&config, &kline_capture(true), &journal);
    let (pass, text) = report(&config);
    assert!(!pass, "{text}");
    assert!(text.contains("difference 0.60000000\n"), "{text}");
    assert!(text.trim_end().ends_with("FAIL (1 problem(s))"), "{text}");

    // A Trades gap over the minute makes its block inconclusive instead.
    let dir = TempDir::new("c3-trades-gap");
    let config = config_with(dir.path(), &["aggTrade", "kline_1m"]);
    let journal = kline_journal(&[gap_over_m1("Trades", "LateEvent")]);
    write_records(&config, &kline_capture(true), &journal);
    let (pass, text) = report(&config);
    assert!(pass, "{text}");
    assert!(
        text.contains(
            "kline reconciliation: blocks 3 | conclusive 2 | inconclusive 1 (not consecutive 0, \
             run edge 0, gap 1, orphan 0) | flagged 0 (0 minutes) | difference 0.00000000\n"
        ),
        "{text}"
    );
}
