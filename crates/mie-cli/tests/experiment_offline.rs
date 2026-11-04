//! `mie experiment` offline (#29, ADR-040): specs validated, run over
//! archive rows in a real Parquet raw store, results recorded once in the
//! append-only result store and reproduced exactly. `--source history`
//! runs `research.replay_summary@2` on the history chain of the seam
//! fixture (ADR-052 D10), and `--pipeline backtest` runs
//! `research.backtest@1` on the same chain (ADR-060 D13).

mod common;

use common::{CUTOVER, HOUR, ORIGIN, TempDir, import_rows, seam_fixture, seam_fixture_with};
use mie_adapter_binance::archive::catalog::{DAY_MS, parse_day};
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::normalize::record_time;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::config::ArchiveConfig;
use mie_cli::config::IngestConfig;
use mie_cli::experiment::{self, ExperimentPipeline, ExperimentRequest};
use mie_cli::replay::{self, ReplayOutcome, ReplayRequest, ReplaySource};
use mie_domain::feature::{FeatureKey, FeatureSet, FeatureSetError, catalog};
use mie_domain::research::ExperimentSpec;
use mie_domain::state_hash::STATE_HASH_ENCODING;
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
    import_day_with(config, false)
}

/// [`import_day`], with `trades` rows when `with_trades`: one per
/// aggregate, plus one uncovered tail trade after aggregate 3 that the
/// fold restores (ADR-050).
fn import_day_with(config: &ArchiveConfig, with_trades: bool) -> ReplayWindow {
    let day = parse_day("2026-10-06").unwrap();
    let mut rows = Vec::new();
    if with_trades {
        for h in 0..24 {
            let time = day * DAY_MS + h * HOUR + 3;
            rows.push((
                ArchiveStream::Trades,
                format!("{},85000.10,0.010,0,{time},true", 10 * (h + 1)),
            ));
            if h == 2 {
                rows.push((
                    ArchiveStream::Trades,
                    format!("{},85000.10,0.004,0,{},true", 10 * (h + 1) + 1, time + 5),
                ));
            }
        }
    }
    for h in 0..24 {
        rows.push((
            ArchiveStream::AggTrades,
            format!(
                "{id},85000.10,0.010,{first},{first},{},true",
                day * DAY_MS + h * HOUR + 3,
                id = h + 1,
                first = 10 * (h + 1)
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

/// The dataset version a replay of `window` opens under `correction`.
fn dataset_of(config: &ArchiveConfig, window: ReplayWindow, correction: TradeCorrection) -> String {
    let store = ParquetRawStore::new(&config.paths.raw_root);
    let streams = config.streams().unwrap();
    let replay = ArchiveReplay::with_defaults(&store, "BTCUSDT", &streams, correction);
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
             latency 250\n\
             backtest: location: location.at_level@1: unknown rule (location rules: \
             location.any@1)\n\
             backtest: trigger: trigger.failed_auction@1: unknown rule (trigger rules: \
             trigger.fires@1)\n\
             backtest: entry: entry.next_trade@1: missing parameter side: add \
             `side=text:<with|against>`\n\
             backtest: invalidation: invalidation.beyond_extreme@1: missing parameter atr: add \
             `atr=feature:<volatility.atr.* id@N of the features line>`\n\
             backtest: invalidation: invalidation.beyond_extreme@1: missing parameter buffer: \
             add `buffer=rate:<decimal at or above 0>`\n\
             backtest: target: target.level@1: unknown rule (target rules: \
             target.r_multiple@1)\n\
             backtest: slippage: slippage.fixed@1: missing parameter rate: add \
             `rate=rate:<decimal from 0, below 1>`\n",
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
    let data = dataset_of(&config, window, TradeCorrection::Fold);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &data)),
        source: ReplaySource::Archive {
            config: config.clone(),
            streams: None,
            correction: TradeCorrection::Fold,
        },
        results: dir.path().join("results"),
        pipeline: ExperimentPipeline::Summary,
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
    let data = dataset_of(&config, window, TradeCorrection::Fold);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &"0e".repeat(32))),
        source: ReplaySource::Archive {
            config,
            streams: None,
            correction: TradeCorrection::Fold,
        },
        results: dir.path().join("results"),
        pipeline: ExperimentPipeline::Summary,
    };
    let (pass, text) = run(&request);
    assert!(!pass);
    assert_eq!(
        text,
        format!(
            "FAIL: data version mismatch: the spec names {}, the sample opened {data}\n{HINT}\n",
            "0e".repeat(32)
        )
    );
    assert!(files(&request.results).is_empty());
}

/// The hint a folding archive run adds to a data-version mismatch.
const HINT: &str = "hint: --trade-correction none reproduces data versions recorded before \
                    ADR-050 (the aggTrades fold)";

#[test]
fn a_spec_pinned_to_the_unfolded_data_runs_under_none_and_mismatches_under_fold() {
    let dir = TempDir::new("experiment-fold");
    let config = archive_config(dir.path());
    let window = import_day_with(&config, true);
    let none = dataset_of(&config, window, TradeCorrection::None);
    let fold = dataset_of(&config, window, TradeCorrection::Fold);
    assert_ne!(none, fold, "the trades files join the folding version");
    let request = |correction| ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &none)),
        source: ReplaySource::Archive {
            config: config.clone(),
            streams: None,
            correction,
        },
        results: dir.path().join("results"),
        pipeline: ExperimentPipeline::Summary,
    };

    let (pass, text) = run(&request(TradeCorrection::None));
    assert!(pass, "{text}");
    assert!(text.starts_with("recorded "), "{text}");
    let stored = files(&dir.path().join("results"));

    let (pass, text) = run(&request(TradeCorrection::Fold));
    assert!(!pass);
    assert_eq!(
        text,
        format!(
            "FAIL: data version mismatch: the spec names {none}, the sample opened {fold}\n{HINT}\n"
        )
    );
    assert_eq!(files(&dir.path().join("results")), stored);

    // `none` still reproduces the result recorded before the fold.
    let (pass, text) = run(&request(TradeCorrection::None));
    assert_eq!(
        (pass, text.starts_with("reproduced ")),
        (true, true),
        "{text}"
    );
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
    let data = dataset_of(&config, window, TradeCorrection::Fold);
    let request = ExperimentRequest {
        spec: write_spec(dir.path(), "a.spec", &spec_text(window, &data)),
        source: ReplaySource::Archive {
            config,
            streams: None,
            correction: TradeCorrection::Fold,
        },
        results: dir.path().join("results"),
        pipeline: ExperimentPipeline::Summary,
    };
    assert_eq!(
        run(&request),
        (false, "FAIL: the sample period holds no event\n".to_owned())
    );
}

// ---------------------------------------------------------------------------
// `--source history`: `research.replay_summary@2` (ADR-052 D10).

fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

/// `mie replay --source history` of `config` over `w`.
fn history_replay(config: &IngestConfig, w: ReplayWindow) -> ReplayOutcome {
    let mut out = Vec::new();
    let outcome = replay::run(
        &ReplayRequest {
            source: ReplaySource::History(config.clone()),
            window: w,
        },
        &mut out,
    )
    .unwrap();
    assert!(outcome.pass, "{}", String::from_utf8_lossy(&out));
    outcome
}

/// The value of the line `<key> <value>` of a stored result.
fn stored_line<'a>(text: &'a str, key: &str) -> &'a str {
    text.lines()
        .find_map(|line| line.strip_prefix(key)?.strip_prefix(' '))
        .unwrap_or_else(|| panic!("no {key} line in\n{text}"))
}

fn history_request(dir: &Path, config: &IngestConfig, spec: &str) -> ExperimentRequest {
    ExperimentRequest {
        spec: write_spec(dir, "history.spec", spec),
        source: ReplaySource::History(config.clone()),
        results: dir.join("results"),
        pipeline: ExperimentPipeline::Summary,
    }
}

#[test]
fn a_history_run_records_then_reproduces_the_prefix_and_the_sample() {
    let dir = TempDir::new("experiment-history");
    let config = seam_fixture(dir.path());
    let sample = window(CUTOVER, CUTOVER + DAY_MS);
    let replayed = history_replay(&config, sample);
    let text = spec_text(sample, replayed.dataset.as_ref().unwrap().as_str());
    let request = history_request(dir.path(), &config, &text);
    let experiment = ExperimentSpec::parse(&text, &catalog::registry())
        .unwrap()
        .id();
    // The spec lists no `catalog::SELECTABLE` member: the engine computes
    // the current set.
    let key = format!(
        "{experiment}/research.replay_summary@2-{}-{STATE_HASH_ENCODING}",
        catalog::current_set().version()
    );

    let (pass, first) = run(&request);
    assert!(pass, "{first}");
    assert_eq!(first, format!("recorded {key}\n"));
    let stored = files(&request.results);
    assert_eq!(stored.len(), 1, "{stored:?}");
    assert!(stored[0].0.ends_with(format!("{key}.result")), "{stored:?}");
    let stored_text = String::from_utf8(stored[0].1.clone()).unwrap();

    // The prefix is the chain's replay of [origin, sample start), the
    // sample its replay of the sample.
    let prefix = history_replay(&config, window(ORIGIN, CUTOVER));
    assert_eq!(
        stored_line(&stored_text, "prefix-data"),
        prefix.dataset.unwrap().as_str()
    );
    assert_eq!(
        stored_line(&stored_text, "prefix-stream"),
        prefix.stream_hash.unwrap().to_string()
    );
    assert_eq!(stored_line(&stored_text, "prefix-rejections"), "0");
    assert_eq!(
        stored_line(&stored_text, "stream"),
        replayed.stream_hash.unwrap().to_string()
    );
    assert_eq!(stored_line(&stored_text, "rejections"), "0");
    assert_eq!(stored_line(&stored_text, "origin"), ORIGIN.to_string());
    assert_eq!(stored_line(&stored_text, "cutover"), CUTOVER.to_string());
    assert_eq!(stored_line(&stored_text, "effective-origin"), "-");

    let (pass, second) = run(&request);
    assert!(pass, "{second}");
    assert_eq!(second, format!("reproduced {key}\n"));
    assert_eq!(files(&request.results), stored);
}

#[test]
fn an_archive_row_imported_on_a_prefix_day_fails_the_rerun() {
    let dir = TempDir::new("experiment-history-prefix");
    let config = seam_fixture(dir.path());
    let sample = window(CUTOVER, CUTOVER + DAY_MS);
    let recorded = history_replay(&config, sample).dataset.unwrap();
    let request = history_request(dir.path(), &config, &spec_text(sample, recorded.as_str()));
    let (pass, text) = run(&request);
    assert!(pass, "{text}");
    let stored = files(&request.results);

    // A late archive row on the origin's second day: the sample's chain
    // version covers the archive segment, so it moves.
    let (_, archive) = config.history_archive().unwrap().unwrap();
    let time = ORIGIN + DAY_MS + 30 * 60_000 + 11;
    import_rows(
        &archive,
        &[(
            ArchiveStream::AggTrades,
            format!("9000,85000.10,0.010,9000,9000,{time},false"),
        )],
    );
    let moved = history_replay(&config, sample).dataset.unwrap();
    assert_ne!(moved, recorded);
    assert_eq!(
        run(&request),
        (
            false,
            format!(
                "FAIL: data version mismatch: the spec names {recorded}, the sample opened \
                 {moved}\n"
            )
        )
    );
    assert_eq!(files(&request.results), stored);
}

#[test]
fn a_history_run_without_a_history_section_or_before_the_origin_fails() {
    let dir = TempDir::new("experiment-history-refused");
    let config = seam_fixture(dir.path());
    let sample = window(CUTOVER, CUTOVER + DAY_MS);
    let data = history_replay(&config, sample).dataset.unwrap();

    let mut cold = config.clone();
    cold.history = None;
    let request = history_request(dir.path(), &cold, &spec_text(sample, data.as_str()));
    let (pass, text) = run(&request);
    assert!(!pass);
    assert!(
        text.starts_with("FAIL: ") && text.contains("no [history] section"),
        "{text}"
    );
    assert!(files(&request.results).is_empty());

    let early = window(ORIGIN - DAY_MS, ORIGIN + DAY_MS);
    let request = history_request(dir.path(), &config, &spec_text(early, data.as_str()));
    assert_eq!(
        run(&request),
        (
            false,
            format!(
                "FAIL: the sample starts at {} ms, before the history origin {ORIGIN} ms \
                 (ADR-052 D10)\n",
                ORIGIN - DAY_MS
            )
        )
    );
    assert!(files(&request.results).is_empty());
}

// ---------------------------------------------------------------------------
// `--pipeline backtest`: `research.backtest@1` (ADR-060 D13).

/// A spec that resolves under rule registry v1: `trigger.sfp.15m@1`
/// decisions with an ATR(1h) stop and a 1 R target, under `funding`. The
/// seam fixture's flat tape fires no SFP, so a run records no trade.
fn backtest_spec_text(window: ReplayWindow, data: &str, funding: &str) -> String {
    let registry = catalog::registry();
    let mut keys: Vec<FeatureKey> = vec![
        catalog::TRIGGER_SFP_15M_V1.key,
        catalog::VOLATILITY_ATR_1H_V1.key,
    ];
    let set = loop {
        match FeatureSet::new(&registry, &keys) {
            Ok(set) => break set,
            Err(FeatureSetError::MissingDependency { requires, .. }) => keys.push(requires),
            Err(error) => panic!("{error}"),
        }
    };
    format!(
        "mie-experiment 1\n\
         hypothesis sfp.15m\n\
         sample {} {}\n\
         data {data}\n\
         features {} {set}\n\
         state-filter none\n\
         regime-filter none\n\
         location location.any@1\n\
         trigger trigger.fires@1 feature=feature:trigger.sfp.15m@1\n\
         entry entry.next_trade@1 side=text:with\n\
         invalidation invalidation.atr@1 atr=feature:volatility.atr.1h@1 \
         multiple=rate:1.00000000\n\
         target target.r_multiple@1 max_hold_ms=int:3600000 r=rate:1.00000000\n\
         fees maker=0.0002 taker=0.0005\n\
         slippage slippage.fixed@1 rate=rate:0.0001\n\
         funding {funding}\n\
         latency 0\n",
        window.start.as_millis(),
        window.end.as_millis(),
        set.version(),
    )
}

fn backtest_request(dir: &Path, config: &IngestConfig, spec: &str) -> ExperimentRequest {
    ExperimentRequest {
        pipeline: ExperimentPipeline::Backtest,
        ..history_request(dir, config, spec)
    }
}

/// The twelve provenance lines of a stored chain result: from `origin` up
/// to `state-end`.
fn provenance(text: &str) -> Vec<&str> {
    text.lines()
        .skip_while(|line| !line.starts_with("origin "))
        .take(12)
        .collect()
}

#[test]
fn a_backtest_records_then_reproduces_with_the_provenance_of_v2() {
    let dir = TempDir::new("experiment-backtest");
    let config = seam_fixture(dir.path());
    // A sample in the archive segment, after a prefix day.
    let sample = window(ORIGIN + DAY_MS, CUTOVER);
    let data = history_replay(&config, sample).dataset.unwrap();
    let text = backtest_spec_text(sample, data.as_str(), "funding.none@1");
    let request = backtest_request(dir.path(), &config, &text);
    let experiment = ExperimentSpec::parse(&text, &catalog::registry())
        .unwrap()
        .id();
    let key = format!(
        "{experiment}/research.backtest@1-{}-{STATE_HASH_ENCODING}",
        catalog::current_set().version()
    );

    let (pass, first) = run(&request);
    assert!(pass, "{first}");
    assert_eq!(first, format!("recorded {key}\n"));
    let stored = files(&request.results);
    assert_eq!(stored.len(), 1, "{stored:?}");
    assert!(stored[0].0.ends_with(format!("{key}.result")), "{stored:?}");
    let stored_text = String::from_utf8(stored[0].1.clone()).unwrap();
    assert_eq!(stored_line(&stored_text, "outcome"), "backtest");
    assert_eq!(stored_line(&stored_text, "triggers"), "0");

    let (pass, second) = run(&request);
    assert!(pass, "{second}");
    assert_eq!(second, format!("reproduced {key}\n"));
    assert_eq!(files(&request.results), stored);

    // `@2` of the same window on the same chain: the same provenance.
    let summary = history_request(dir.path(), &config, &text);
    let summary = ExperimentRequest {
        results: dir.path().join("summary-results"),
        ..summary
    };
    let (pass, out) = run(&summary);
    assert!(pass, "{out}");
    let v2 = files(&summary.results);
    let v2_text = String::from_utf8(v2[0].1.clone()).unwrap();
    assert_eq!(provenance(&stored_text), provenance(&v2_text));
    assert_eq!(provenance(&stored_text).len(), 12);
}

#[test]
fn recorded_funding_needs_a_chain_that_delivers_the_settlements() {
    // The seam fixture's archive replays `aggTrades` and `metrics` only.
    let dir = TempDir::new("experiment-backtest-funding");
    let config = seam_fixture(dir.path());
    let sample = window(ORIGIN + DAY_MS, CUTOVER);
    let data = history_replay(&config, sample).dataset.unwrap();
    let text = backtest_spec_text(sample, data.as_str(), "funding.recorded@1");
    let request = backtest_request(dir.path(), &config, &text);
    let (pass, out) = run(&request);
    assert!(!pass);
    assert!(
        out.starts_with("FAIL: ") && out.contains("does not replay fundingRate"),
        "{out}"
    );
    assert!(files(&request.results).is_empty());

    // An archive that replays `fundingRate`: a sample that ends at the
    // cutover is covered, one that ends 1 ms past it is not.
    let dir = TempDir::new("experiment-backtest-funding-rate");
    let config = seam_fixture_with(dir.path(), r#""aggTrades", "metrics", "fundingRate""#);
    let data = history_replay(&config, sample).dataset.unwrap();
    let text = backtest_spec_text(sample, data.as_str(), "funding.recorded@1");
    let request = backtest_request(dir.path(), &config, &text);
    let (pass, out) = run(&request);
    assert!(pass, "{out}");
    assert!(out.starts_with("recorded "), "{out}");

    let past = window(ORIGIN + DAY_MS, CUTOVER + 1);
    let data = history_replay(&config, past).dataset.unwrap();
    let text = backtest_spec_text(past, data.as_str(), "funding.recorded@1");
    let request = ExperimentRequest {
        results: dir.path().join("past-results"),
        ..backtest_request(dir.path(), &config, &text)
    };
    assert_eq!(
        run(&request),
        (
            false,
            format!(
                "FAIL: the sample ends at {} ms, after the history cutover {CUTOVER} ms: \
                 settlements are delivered only up to the cutover, so funding.recorded@1 \
                 would book zero funding by omission (ADR-060 D7); end the sample at or \
                 before the cutover, or state funding.none@1\n",
                CUTOVER + 1
            )
        )
    );
    assert!(files(&request.results).is_empty());
}

#[test]
fn a_backtest_on_another_source_fails_before_the_store_opens() {
    let dir = TempDir::new("experiment-backtest-archive");
    let config = archive_config(dir.path());
    let window = import_day(&config);
    let data = dataset_of(&config, window, TradeCorrection::Fold);
    let request = ExperimentRequest {
        spec: write_spec(
            dir.path(),
            "a.spec",
            &backtest_spec_text(window, &data, "funding.none@1"),
        ),
        source: ReplaySource::Archive {
            config,
            streams: None,
            correction: TradeCorrection::Fold,
        },
        results: dir.path().join("results"),
        pipeline: ExperimentPipeline::Backtest,
    };
    assert_eq!(
        run(&request),
        (
            false,
            "FAIL: --pipeline backtest runs on --source history only (ADR-060 D13)\n".to_owned()
        )
    );
    assert!(!request.results.exists());
}

#[test]
fn an_unresolvable_backtest_spec_fails_with_every_resolution_error() {
    let dir = TempDir::new("experiment-backtest-unresolved");
    let config = seam_fixture(dir.path());
    let sample = window(ORIGIN + DAY_MS, CUTOVER);
    let data = history_replay(&config, sample).dataset.unwrap();
    // The ADR-040 golden spec's rules: they parse, and do not resolve.
    let text = spec_text(sample, data.as_str());
    let spec = ExperimentSpec::parse(&text, &catalog::registry()).unwrap();
    let errors = mie_domain::backtest::resolve(&spec).unwrap_err();
    assert!(errors.len() > 1, "{errors:?}");
    let request = backtest_request(dir.path(), &config, &text);
    let (pass, out) = run(&request);
    assert!(!pass);
    let mut expected =
        "FAIL: the spec does not resolve under rule registry v1 (ADR-060 D2):\n".to_owned();
    for error in &errors {
        expected.push_str(&format!("{error}\n"));
    }
    assert_eq!(out, expected);
    assert!(files(&request.results).is_empty());
}

#[test]
fn validate_resolves_a_backtest_spec_and_keeps_the_parse_verdict() {
    let dir = TempDir::new("experiment-validate-backtest");
    let window = ReplayWindow {
        start: EventTime::from_millis(0),
        end: EventTime::from_millis(DAY_MS),
    };
    let path = write_spec(
        dir.path(),
        "b.spec",
        &backtest_spec_text(window, &"0f".repeat(32), "funding.none@1"),
    );
    let (valid, text) = validate(&path);
    assert!(valid, "{text}");
    assert!(
        text.ends_with("latency 0\nbacktest: resolves under rule registry v1\n"),
        "{text}"
    );
}
