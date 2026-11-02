//! `mie alerts replay` and `mie alerts report` against an archive fixture
//! store (#38, ADR-059): the dry-run's alert log is reproducible to the
//! byte, its summary counts its message lines, an existing log is never
//! overwritten, and the report is the market as of `--to`.

mod common;

use common::tapes::Lcg;
use common::{TempDir, history_archive_config, import_rows};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::alert::{ReportService, render};
use mie_cli::alerts::{self, ReplayRequest, ReportRequest};
use mie_cli::config::ArchiveConfig;
use mie_cli::replay::{ReplaySource, archive_replay};
use mie_domain::alert::{ALERT_POLICY_V1, Alert, policy_fingerprint};
use mie_domain::time::EventTime;
use mie_ports::inbound::GenerateMarketReport;
use mie_ports::outbound::ReplayWindow;
use serde_json::Value;
use std::fs;
use std::path::Path;

/// 2026-10-03, the fixture's first day.
const D0: i64 = 1_790_985_600_000;

fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

/// Three archive days of aggregate trades, one every 5 s, on a seeded
/// random walk around 85 000 with an hourly swing, so price crosses the
/// prior day's levels.
fn archive_fixture(dir: &Path) -> ArchiveConfig {
    let config = history_archive_config(dir, r#""aggTrades""#);
    let mut lcg = Lcg(38);
    let mut price: i64 = 8_500_000; // cents
    let mut rows = Vec::new();
    for (id, time) in (D0..D0 + 3 * DAY_MS).step_by(5_000).enumerate() {
        let trend = ((time - D0) / 3_600_000 * 7 % 9) - 4;
        price += trend * 4 + lcg.below(41) - 20;
        let id = id + 1;
        rows.push((
            ArchiveStream::AggTrades,
            format!(
                "{id},{}.{:02},0.010,{id},{id},{time},{}",
                price / 100,
                price % 100,
                lcg.below(2) == 0
            ),
        ));
    }
    import_rows(&config, &rows);
    config
}

fn archive(config: &ArchiveConfig) -> ReplaySource {
    ReplaySource::Archive {
        config: config.clone(),
        streams: Some(vec![ArchiveStream::AggTrades]),
        correction: TradeCorrection::None,
    }
}

fn run_replay(request: &ReplayRequest) -> (bool, String) {
    let mut out = Vec::new();
    let pass = alerts::replay(request, &mut out).expect("the report is written");
    (pass, String::from_utf8(out).unwrap())
}

fn lines(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

#[test]
fn a_dry_run_writes_the_same_log_twice() {
    let dir = TempDir::new("alerts-replay");
    let config = archive_fixture(dir.path());
    // Days 2 and 3 emit; day 1 warms the engine and the monitor up.
    let request = |out: &str| ReplayRequest {
        source: archive(&config),
        replay: window(D0, D0 + 3 * DAY_MS),
        emit_from: EventTime::from_millis(D0 + DAY_MS),
        out: dir.path().join(out),
    };
    let (pass, a) = run_replay(&request("a.jsonl"));
    assert!(pass, "{a}");
    let (pass, b) = run_replay(&request("b.jsonl"));
    assert!(pass, "{b}");
    assert_eq!(
        a.replace("a.jsonl", "x"),
        b.replace("b.jsonl", "x"),
        "the report differs"
    );
    let bytes = fs::read(dir.path().join("a.jsonl")).unwrap();
    assert_eq!(bytes, fs::read(dir.path().join("b.jsonl")).unwrap());
    assert!(a.ends_with("PASS\n"), "{a}");

    let log = lines(&dir.path().join("a.jsonl"));
    let header = &log[0];
    assert_eq!(header["type"], "header");
    assert_eq!(header["format"], alerts::LOG_FORMAT);
    assert_eq!(
        header["source"],
        "archive binance-archive/BTCUSDT streams aggTrades correction none"
    );
    assert_eq!(header["policy"], ALERT_POLICY_V1.to_string());
    assert_eq!(
        header["policy_fingerprint"],
        policy_fingerprint().to_string()
    );
    assert_eq!(header["emit_from_ms"], D0 + DAY_MS);
    assert_eq!(header["replay"]["start_ms"], D0);
    assert_eq!(header["replay"]["end_ms"], D0 + 3 * DAY_MS);

    let summary = log.last().unwrap();
    assert_eq!(summary["type"], "summary");
    let messages: Vec<&Value> = log[1..log.len() - 1].iter().collect();
    assert!(messages.iter().all(|line| line["type"] == "message"));
    // The summary counts the message lines and their alerts.
    assert_eq!(summary["messages"], messages.len() as u64);
    let alerts: u64 = messages
        .iter()
        .map(|line| line["alerts"].as_array().unwrap().len() as u64)
        .sum();
    let per_kind: u64 = summary["alerts"]
        .as_object()
        .unwrap()
        .values()
        .map(|count| count.as_u64().unwrap())
        .sum();
    assert_eq!(per_kind, alerts);
    assert_eq!(summary["rejections"], 0);
    assert_eq!(summary["messages_per_day"]["days"], 2);
    // Both rollovers in the emit window bring a report.
    assert!(
        summary["alerts"]["report"].as_u64().unwrap() >= 1,
        "{summary}"
    );
    assert!(
        summary["alerts"]["zone_entered"].as_u64().unwrap() >= 1,
        "{summary}"
    );
    for line in &messages {
        assert!(line["known_at_ms"].as_i64().unwrap() >= D0 + DAY_MS);
        let text = line["text"].as_str().unwrap();
        assert!(text.starts_with("MIE alert 2026-10-0"), "{text}");
    }
    assert!(a.contains(&format!("messages {}", messages.len())), "{a}");
}

#[test]
fn an_existing_log_and_an_empty_window_are_refused() {
    let dir = TempDir::new("alerts-refuse");
    let config = archive_fixture(dir.path());
    let out = dir.path().join("exists.jsonl");
    fs::write(&out, "keep me").unwrap();
    let (pass, text) = run_replay(&ReplayRequest {
        source: archive(&config),
        replay: window(D0, D0 + DAY_MS),
        emit_from: EventTime::from_millis(D0),
        out: out.clone(),
    });
    assert!(!pass);
    assert!(
        text.starts_with("FAIL: ") && text.contains("never overwritten"),
        "{text}"
    );
    assert_eq!(fs::read_to_string(&out).unwrap(), "keep me");

    // A window without data writes nothing.
    let empty = dir.path().join("empty.jsonl");
    let (pass, text) = run_replay(&ReplayRequest {
        source: archive(&config),
        replay: window(D0 + 10 * DAY_MS, D0 + 11 * DAY_MS),
        emit_from: EventTime::from_millis(D0 + 10 * DAY_MS),
        out: empty.clone(),
    });
    assert!(!pass, "{text}");
    assert_eq!(text, "FAIL: the window holds no event\n");
    assert!(!empty.exists());
}

#[test]
fn the_report_is_the_market_as_of_the_window_end() {
    let dir = TempDir::new("alerts-report");
    let config = archive_fixture(dir.path());
    let request = ReportRequest {
        source: archive(&config),
        window: window(D0, D0 + DAY_MS + 6 * 3_600_000),
    };
    let mut out = Vec::new();
    assert!(alerts::report(&request, &mut out).unwrap());
    let text = String::from_utf8(out).unwrap();
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let replay = archive_replay(
        &store,
        &config,
        Some(&[ArchiveStream::AggTrades]),
        TradeCorrection::None,
    )
    .unwrap();
    let report = ReportService::new(replay).report(request.window).unwrap();
    assert!(report.as_of.unwrap().as_millis() < request.window.end.as_millis());
    assert!(report.levels.is_ready(), "the prior day is published");
    assert_eq!(text, format!("{}PASS\n", render(&[Alert::Report(report)])));
    assert!(text.contains("Monitored prior-day levels:\n"), "{text}");

    let mut out = Vec::new();
    let empty = ReportRequest {
        window: window(D0 + 10 * DAY_MS, D0 + 11 * DAY_MS),
        ..request
    };
    assert!(!alerts::report(&empty, &mut out).unwrap());
    assert_eq!(
        String::from_utf8(out).unwrap(),
        "FAIL: the window holds no event\n"
    );
}
