//! `mie experiment` offline (#29, ADR-040): specs validated, run over
//! archive rows in a real Parquet raw store, results recorded once in the
//! append-only result store and reproduced exactly.

mod common;

use common::{HOUR, TempDir};
use mie_adapter_binance::archive::catalog::{DAY_MS, parse_day};
use mie_adapter_binance::archive::normalize::record_time;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::config::ArchiveConfig;
use mie_cli::experiment::{self, ExperimentRequest};
use mie_cli::replay::ReplaySource;
use mie_domain::feature::{FeatureSet, catalog};
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, ReplayWindow};
use mie_ports::raw::{RawRecord, RawRecordSink, RawStreamKey};
use std::fs;
use std::path::{Path, PathBuf};

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
streams = ["aggTrades", "fundingRate"]
"#,
        dir.join("raw").display(),
        dir.join("ledger").display(),
        dir.join("staging").display()
    ))
    .expect("valid archive config")
}

/// Files one day of hourly trades and three funding settlements the way
/// the importer does (ADR-034 D3); returns the day's window.
fn import_day(config: &ArchiveConfig) -> ReplayWindow {
    let day = parse_day("2026-10-06").unwrap();
    let mut rows = Vec::new();
    for h in 0..24 {
        rows.push((
            ArchiveStream::AggTrades,
            format!(
                "{id},85000.10,0.010,{id},{id},{},true",
                day * DAY_MS + h * HOUR + 3,
                id = h + 1
            ),
        ));
    }
    for h in [0, 8, 16] {
        rows.push((
            ArchiveStream::FundingRate,
            format!("{},8,0.00010000", day * DAY_MS + h * HOUR),
        ));
    }
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let mut writer = store
        .writer(ARCHIVE_SOURCE, mie_cli::archive::rotation_policy())
        .unwrap();
    for (stream, row) in &rows {
        let key = RawStreamKey::new(ARCHIVE_SOURCE, "BTCUSDT", stream.raw_name()).unwrap();
        let record = RawRecord {
            event_time: record_time(*stream, row.as_bytes()).unwrap(),
            capture: None,
            payload: row.as_bytes().to_vec(),
        };
        writer.append(&key, record).unwrap();
    }
    writer.close().unwrap();
    ReplayWindow {
        start: EventTime::from_millis(day * DAY_MS),
        end: EventTime::from_millis((day + 1) * DAY_MS),
    }
}

/// The dataset version a replay of `window` opens.
fn dataset_of(config: &ArchiveConfig, window: ReplayWindow) -> String {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let streams = config.streams().unwrap();
    let replay = ArchiveReplay::with_defaults(&store, "BTCUSDT", &streams);
    replay.replay(window).unwrap().dataset.to_string()
}

fn spec_text(window: ReplayWindow, data: &str) -> String {
    let registry = catalog::registry();
    let key = registry.resolve("trade.last_price@1").unwrap().key;
    let version = FeatureSet::new(&registry, &[key]).unwrap().version();
    format!(
        "mie-experiment 1\n\
         latency 250\n\
         hypothesis vah.failed_auction.short\n\
         sample {} {}\n\
         data {data}\n\
         features {version} trade.last_price@1\n\
         state-filter none\n\
         regime-filter none\n\
         location location.at_level@1 level=text:vah\n\
         trigger trigger.failed_auction@1\n\
         entry entry.next_trade@1\n\
         invalidation invalidation.beyond_extreme@1\n\
         target target.level@1 level=text:poc\n\
         fees maker=0.0002 taker=0.0005\n\
         slippage slippage.fixed@1\n\
         funding funding.recorded@1\n",
        window.start.as_millis(),
        window.end.as_millis()
    )
}

fn write_spec(dir: &Path, name: &str, text: &str) -> PathBuf {
    let path = dir.join(name);
    fs::write(&path, text).unwrap();
    path
}

fn validate(path: &Path) -> (bool, String) {
    let mut out = Vec::new();
    let valid = experiment::validate(path, &mut out).unwrap();
    (valid, String::from_utf8(out).unwrap())
}

fn run(request: &ExperimentRequest) -> (bool, String) {
    let mut out = Vec::new();
    let pass = experiment::run(request, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

/// Every file under `dir`, recursively, with its bytes; sorted by path.
fn files(dir: &Path) -> Vec<(PathBuf, Vec<u8>)> {
    let mut out = Vec::new();
    let mut pending = vec![dir.to_owned()];
    while let Some(next) = pending.pop() {
        for entry in fs::read_dir(&next).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                pending.push(path);
            } else {
                let bytes = fs::read(&path).unwrap();
                out.push((path, bytes));
            }
        }
    }
    out.sort();
    out
}

#[test]
fn validate_prints_the_id_and_the_canonical_spec() {
    let dir = TempDir::new("experiment-validate");
    let window = ReplayWindow {
        start: EventTime::from_millis(1_000),
        end: EventTime::from_millis(2_000),
    };
    let path = write_spec(dir.path(), "a.spec", &spec_text(window, &"0f".repeat(32)));
    let (valid, text) = validate(&path);
    assert!(valid, "{text}");
    assert_eq!(
        text,
        format!(
            "experiment 97039c13a465144b\n\
             mie-experiment 1\n\
             hypothesis vah.failed_auction.short\n\
             sample 1000 2000\n\
             data {}\n\
             features 9b8a3d042c350a62 trade.last_price@1\n\
             state-filter none\n\
             regime-filter none\n\
             location location.at_level@1 level=text:vah\n\
             trigger trigger.failed_auction@1\n\
             entry entry.next_trade@1\n\
             invalidation invalidation.beyond_extreme@1\n\
             target target.level@1 level=text:poc\n\
             fees maker=0.00020000 taker=0.00050000\n\
             slippage slippage.fixed@1\n\
             funding funding.recorded@1\n\
             latency 250\n",
            "0f".repeat(32)
        )
    );
}

#[test]
fn a_spec_without_a_latency_assumption_is_invalid() {
    let dir = TempDir::new("experiment-invalid");
    let window = ReplayWindow {
        start: EventTime::from_millis(1_000),
        end: EventTime::from_millis(2_000),
    };
    let text = spec_text(window, &"0f".repeat(32)).replace("latency 250\n", "");
    let path = write_spec(dir.path(), "a.spec", &text);
    assert_eq!(
        validate(&path),
        (
            false,
            "missing latency assumption \"latency\": add a line `latency <ms>`\n".to_owned()
        )
    );
    let text = text
        .replace("fees maker=0.0002 taker=0.0005\n", "")
        .replace("data 0f", "data 0F");
    let path = write_spec(dir.path(), "b.spec", &text);
    let (valid, output) = validate(&path);
    assert!(!valid);
    let lines: Vec<&str> = output.lines().collect();
    assert_eq!(lines.len(), 3, "{output}");
    assert!(lines[0].starts_with("line 4: data: "), "{output}");
    assert!(
        lines[1].starts_with("missing cost assumption \"fees\""),
        "{output}"
    );
    assert!(
        lines[2].starts_with("missing latency assumption"),
        "{output}"
    );
}

#[test]
fn the_example_spec_is_valid() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("experiment.example.spec");
    let (valid, text) = validate(&path);
    assert!(valid, "{text}");
}

#[test]
fn a_rerun_reproduces_the_recorded_result_and_never_rewrites_it() {
    let dir = TempDir::new("experiment-run");
    let config = archive_config(dir.path());
    let window = import_day(&config);
    let data = dataset_of(&config, window);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &data)),
        source: ReplaySource::Archive {
            config: config.clone(),
            streams: None,
        },
        results: dir.path().join("results"),
    };

    let (pass, first) = run(&request);
    assert!(pass, "{first}");
    let key = first.strip_prefix("recorded ").expect(&first).trim_end();
    assert!(key.ends_with("/research.replay_summary@1"), "{first}");
    let stored = files(&request.results);
    assert_eq!(stored.len(), 1, "{stored:?}");
    let stored_text = String::from_utf8(stored[0].1.clone()).unwrap();
    assert!(stored_text.contains("\nstream 27:"), "{stored_text}");
    assert!(stored_text.contains("\nrejections 0\n"), "{stored_text}");

    let (pass, second) = run(&request);
    assert!(pass, "{second}");
    assert_eq!(second, format!("reproduced {key}\n"));
    assert_eq!(files(&request.results), stored);
}

#[test]
fn another_data_version_fails_the_run() {
    let dir = TempDir::new("experiment-mismatch");
    let config = archive_config(dir.path());
    let window = import_day(&config);
    let data = dataset_of(&config, window);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &"0e".repeat(32))),
        source: ReplaySource::Archive {
            config,
            streams: None,
        },
        results: dir.path().join("results"),
    };
    let (pass, text) = run(&request);
    assert!(!pass);
    assert_eq!(
        text,
        format!(
            "FAIL: data version mismatch: the spec names {}, the sample opened {data}\n",
            "0e".repeat(32)
        )
    );
    assert!(files(&request.results).is_empty());
}

#[test]
fn an_empty_sample_fails_the_run() {
    let dir = TempDir::new("experiment-empty");
    let config = archive_config(dir.path());
    let day = import_day(&config);
    // An hour before the first trade: nothing in the window.
    let window = ReplayWindow {
        start: EventTime::from_millis(day.start.as_millis() + 30 * 60_000),
        end: EventTime::from_millis(day.start.as_millis() + 60 * 60_000),
    };
    let data = dataset_of(&config, window);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &data)),
        source: ReplaySource::Archive {
            config,
            streams: None,
        },
        results: dir.path().join("results"),
    };
    assert_eq!(
        run(&request),
        (false, "FAIL: the sample period holds no event\n".to_owned())
    );
}
