//! `mie conditional build` and `verify` against archive and live fixture
//! stores (#27, ADR-057): the content-hash reproducibility criterion at
//! fixture scale, the row counts against a direct labeling run, and the
//! refusals.

mod common;

use common::tapes::Lcg;
use common::{TempDir, history_archive_config, import_rows, journaled_run, write_records};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::DAY_MS;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_parquet::ParquetRawStore;
use mie_adapter_parquet::outcome::{OutcomeDataset, TABLES};
use mie_app::conditional::label_window;
use mie_cli::conditional::{self, BuildRequest, CODE_VERSION};
use mie_cli::config::ArchiveConfig;
use mie_cli::replay::{ReplaySource, archive_replay};
use mie_domain::conditional::label::Labeling;
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
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
/// random walk around 85 000.
fn archive_fixture(dir: &Path) -> ArchiveConfig {
    let config = history_archive_config(dir, r#""aggTrades""#);
    let mut lcg = Lcg(27);
    let mut price: i64 = 8_500_000; // cents
    let mut rows = Vec::new();
    for (id, time) in (D0..D0 + 3 * DAY_MS).step_by(5_000).enumerate() {
        price += lcg.below(41) - 20;
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

fn run_build(request: &BuildRequest) -> (bool, String) {
    let mut out = Vec::new();
    let pass = conditional::build(request, &mut out).expect("the report is written");
    (pass, String::from_utf8(out).unwrap())
}

fn run_verify(dir: &Path) -> (bool, String) {
    let mut out = Vec::new();
    let pass = conditional::verify(dir, &mut out).expect("the report is written");
    (pass, String::from_utf8(out).unwrap())
}

/// Every file of a dataset directory with its bytes, sorted by name.
fn files(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut names: Vec<String> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    names
        .into_iter()
        .map(|name| {
            let bytes = fs::read(dir.join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

#[test]
fn rebuilding_from_the_same_inputs_gives_the_same_dataset() {
    let dir = TempDir::new("conditional-build");
    let config = archive_fixture(dir.path());
    // Day 2's records, with day 1 as warm-up and day 3 as lookahead.
    let request = |out: &str| BuildRequest {
        source: archive(&config),
        records: window(D0 + DAY_MS, D0 + 2 * DAY_MS),
        replay: window(D0, D0 + 3 * DAY_MS),
        out: dir.path().join(out),
    };
    let (pass, a) = run_build(&request("a"));
    assert!(pass, "{a}");
    let (pass, b) = run_build(&request("b"));
    assert!(pass, "{b}");

    // ADR-057 D3, criterion 1 of #27 at fixture scale: the same manifest,
    // the same content hash and byte-identical files.
    assert_eq!(a, b);
    assert_eq!(files(&dir.path().join("a")), files(&dir.path().join("b")));
    assert!(
        a.starts_with(&format!(
            "mie-outcome-dataset 2\n\
             source archive binance-archive/BTCUSDT streams aggTrades correction none\n\
             replay {} {}\n\
             records {} {}\n",
            D0,
            D0 + 3 * DAY_MS,
            D0 + DAY_MS,
            D0 + 2 * DAY_MS
        )),
        "{a}"
    );
    assert!(a.contains(&format!("\ncode {CODE_VERSION}\n")), "{a}");
    assert!(a.ends_with("\nPASS\n"), "{a}");

    // The rows are those of a direct labeling run over the same windows.
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let replay = archive_replay(
        &store,
        &config,
        Some(&[ArchiveStream::AggTrades]),
        TradeCorrection::None,
    )
    .unwrap();
    let (mut records, mut labels, mut passages) = (0_u64, 0_u64, 0_u64);
    let (provenance, report) = label_window(
        &replay,
        window(D0, D0 + 3 * DAY_MS),
        window(D0 + DAY_MS, D0 + 2 * DAY_MS),
        Labeling::CURRENT,
        |stream| stream.trailing_gaps().to_vec(),
        |_| records += 1,
        |label| {
            labels += 1;
            passages += u64::from(label.level.is_some()) + u64::from(label.extreme.is_some());
        },
    )
    .unwrap();
    // One baseline per closed minute of the day at least.
    assert!(records >= 1_440, "{records}");
    assert_eq!(labels, records);
    let dataset = OutcomeDataset::open(&dir.path().join("a")).unwrap();
    let manifest = dataset.manifest();
    assert_eq!(manifest.rows(TABLES[0]), Some(records));
    assert_eq!(manifest.rows(TABLES[1]), Some(5 * records));
    assert_eq!(manifest.rows(TABLES[2]), Some(passages));
    assert_eq!(manifest.rows(TABLES[3]), Some(3 * records));
    assert_eq!(
        (manifest.events, manifest.rejections),
        (report.events, report.rejections)
    );
    assert_eq!(manifest.data, provenance.context.data.to_string());
    assert_eq!(manifest.engine, provenance.context.engine.to_string());
    assert_eq!(manifest.labeling, provenance.labeling.to_string());
    assert!(
        a.contains(&format!("records {records} labels {records}\n")),
        "{a}"
    );

    // `verify` passes and prints the same manifest.
    let (pass, verified) = run_verify(&dir.path().join("a"));
    assert!(pass, "{verified}");
    assert_eq!(verified, format!("{}PASS\n", manifest.render()));

    // Another records window is another dataset.
    let (pass, c) = run_build(&BuildRequest {
        records: window(D0 + DAY_MS, D0 + DAY_MS + 12 * 3_600_000),
        ..request("c")
    });
    assert!(pass, "{c}");
    let content = |text: &str| {
        text.lines()
            .find(|line| line.starts_with("content "))
            .unwrap()
            .to_owned()
    };
    assert_ne!(content(&c), content(&a));
}

#[test]
fn a_tampered_dataset_fails_verify() {
    let dir = TempDir::new("conditional-tamper");
    let config = archive_fixture(dir.path());
    let out = dir.path().join("dataset");
    let (pass, text) = run_build(&BuildRequest {
        source: archive(&config),
        records: window(D0 + DAY_MS, D0 + DAY_MS + 3_600_000),
        replay: window(D0, D0 + 2 * DAY_MS),
        out: out.clone(),
    });
    assert!(pass, "{text}");
    let path = out.join(TABLES[0]);
    let mut permissions = fs::metadata(&path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&path, permissions).unwrap();
    let mut bytes = fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    fs::write(&path, bytes).unwrap();
    let (pass, text) = run_verify(&out);
    assert!(!pass);
    assert!(text.starts_with("FAIL: "), "{text}");
    assert!(text.contains("contexts.parquet"), "{text}");
}

#[test]
fn refusals_leave_no_dataset() {
    let dir = TempDir::new("conditional-refuse");
    let config = archive_fixture(dir.path());

    // An existing directory is never overwritten.
    let existing = dir.path().join("existing");
    fs::create_dir(&existing).unwrap();
    let request = BuildRequest {
        source: archive(&config),
        records: window(D0 + DAY_MS, D0 + DAY_MS + 3_600_000),
        replay: window(D0, D0 + 2 * DAY_MS),
        out: existing.clone(),
    };
    let (pass, text) = run_build(&request);
    assert!(!pass);
    assert!(
        text.starts_with("FAIL: ") && text.contains("never overwritten"),
        "{text}"
    );
    assert_eq!(fs::read_dir(&existing).unwrap().count(), 0);

    // A missing parent is refused before the replay, and nothing is
    // created; so is a parent that is a file.
    for out in [
        dir.path().join("no").join("such").join("dataset"),
        dir.path().join("archive.toml").join("dataset"),
    ] {
        let (pass, text) = run_build(&BuildRequest {
            out: out.clone(),
            ..request.clone()
        });
        assert!(!pass);
        assert!(
            text.starts_with("FAIL: ") && text.contains("not an existing directory"),
            "{text}"
        );
        assert!(!out.exists());
    }
    assert!(!dir.path().join("no").exists());

    // The history chain is refused (ADR-057 D6).
    let out = dir.path().join("history");
    let (pass, text) = run_build(&BuildRequest {
        source: ReplaySource::History(common::history_config(
            dir.path(),
            "2026-10-03",
            "2026-10-05",
        )),
        out: out.clone(),
        ..request.clone()
    });
    assert!(!pass);
    assert!(text.contains("--source history is refused"), "{text}");
    assert!(!out.exists());

    // A records window outside the replay window.
    let (pass, text) = run_build(&BuildRequest {
        replay: window(D0 + DAY_MS + 60_000, D0 + 2 * DAY_MS),
        out: dir.path().join("window"),
        ..request.clone()
    });
    assert!(!pass, "{text}");
    assert!(!dir.path().join("window").exists());

    // No record in the window: the build fails without a manifest.
    let out = dir.path().join("empty");
    let (pass, text) = run_build(&BuildRequest {
        records: window(D0 + 5 * DAY_MS, D0 + 6 * DAY_MS),
        replay: window(D0 + 5 * DAY_MS, D0 + 6 * DAY_MS),
        out: out.clone(),
        ..request
    });
    assert!(!pass);
    assert!(text.contains("no context record"), "{text}");
    let (pass, text) = run_verify(&out);
    assert!(!pass);
    assert!(text.contains("incomplete"), "{text}");
}

#[test]
fn a_live_store_builds_with_its_journaled_streams() {
    let dir = TempDir::new("conditional-live");
    let config = common::config(dir.path());
    let start = D0;
    let times: Vec<i64> = (0..360).map(|i| start + 5_000 + i * 10_000).collect();
    let (journal, records) = journaled_run(
        "20261003T000000Z",
        start,
        start + 3_600_000 + 60_000,
        100,
        &times,
    );
    write_records(&config, &records, &journal);
    let out = dir.path().join("dataset");
    let (pass, text) = run_build(&BuildRequest {
        source: ReplaySource::Live(config),
        records: window(start, start + 3_600_000),
        replay: window(start, start + 3_600_000),
        out: out.clone(),
    });
    assert!(pass, "{text}");
    assert!(
        text.contains("\nsource live binance-um/BTCUSDT streams journaled correction none\n"),
        "{text}"
    );
    assert!(run_verify(&out).0);
}

#[test]
fn the_code_version_names_a_commit_or_unknown() {
    // `^(git:[0-9a-f]{40}(-dirty)?|unknown)$`, without a regex crate.
    let valid = CODE_VERSION == "unknown"
        || CODE_VERSION.strip_prefix("git:").is_some_and(|rest| {
            let commit = rest.strip_suffix("-dirty").unwrap_or(rest);
            commit.len() == 40
                && commit
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        });
    assert!(valid, "{CODE_VERSION}");
}
