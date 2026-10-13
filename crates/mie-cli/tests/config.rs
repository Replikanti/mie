//! The ingest configuration file.

mod common;

use mie_adapter_binance::BinanceStream;
use mie_cli::config::IngestConfig;
use std::collections::BTreeMap;
use std::time::Duration;

const REQUIRED: &str = r#"
[instrument]
symbol = "BTCUSDT"
source = "binance-um"
[paths]
raw_root = "data/raw"
journal = "data/journal.jsonl"
[binance]
ws_base_url = "wss://fstream.binance.com/market/ws"
rest_base_url = "https://fapi.binance.com"
"#;

#[test]
fn the_example_file_parses_with_the_documented_defaults() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/ingest.example.toml");
    let example = IngestConfig::load(std::path::Path::new(path)).expect("example parses");
    let minimal = IngestConfig::parse(REQUIRED).expect("required keys suffice");
    // The example spells out every default.
    assert_eq!(example.capture, minimal.capture);
    assert_eq!(example.binance, minimal.binance);
    assert_eq!(example.streams().unwrap(), BinanceStream::ALL);
    assert_eq!(example.capture.hold_back_ms, 2_000);
    assert_eq!(example.capture.seal_interval_secs, 300);
    let live = example
        .live_config("20261006T000000Z", BTreeMap::new())
        .unwrap();
    assert_eq!(live.ws_base_url, "wss://fstream.binance.com/market/ws");
    assert_eq!(
        live.ws_public_base_url,
        "wss://fstream.binance.com/public/ws"
    );
    assert_eq!(live.max_connection_age, Duration::from_secs(82_800));
    assert_eq!(live.rotation_stagger, Duration::from_secs(300));
    assert_eq!(live.depth_snapshot_limit, 1_000);
    assert_eq!(live.depth_checkpoint_interval, Duration::from_secs(60));
    assert_eq!(live.depth_snapshot_min_spacing, Duration::from_secs(2));
    // Channel capacity, derived in ADR-026 ("Why 65 536").
    assert_eq!(live.inbound_capacity, 65_536);
    assert_eq!(live.core_capacity, 65_536);
    assert_eq!(example.capture.state_checkpoint_interval_secs, 60);
    assert_eq!(example.state_checkpoint_interval_ms(), Ok(60_000));
    // Every stream by default, depth included.
    assert_eq!(live.streams.len(), 7);
    assert!(live.streams.contains(&BinanceStream::Depth));
    // The `[history]` block is commented out: the example stays cold
    // (ADR-052 D8).
    assert_eq!(example.history, None);
    assert_eq!(example.history_archive(), Ok(None));
}

#[test]
fn depth_settings_are_mapped_and_validated() {
    let text = format!(
        "{REQUIRED}ws_public_base_url = \"wss://example.invalid/public/ws\"\n\
         [capture]\ndepth_snapshot_limit = 100\ndepth_checkpoint_interval_secs = 30\n\
         depth_snapshot_min_spacing_ms = 500\n"
    );
    let config = IngestConfig::parse(&text).expect("valid");
    let live = config.live_config("r", BTreeMap::new()).unwrap();
    assert_eq!(live.ws_public_base_url, "wss://example.invalid/public/ws");
    assert_eq!(live.depth_snapshot_limit, 100);
    assert_eq!(live.depth_checkpoint_interval, Duration::from_secs(30));
    assert_eq!(live.depth_snapshot_min_spacing, Duration::from_millis(500));

    for capture in [
        "depth_snapshot_limit = 200",
        "depth_snapshot_limit = 0",
        "depth_checkpoint_interval_secs = 0",
        "depth_snapshot_min_spacing_ms = 0",
    ] {
        let text = format!("{REQUIRED}[capture]\n{capture}\n");
        assert!(IngestConfig::parse(&text).is_err(), "{capture}");
    }
    // depth and depthSnapshot go together.
    for streams in [r#"["depth"]"#, r#"["aggTrade", "depthSnapshot"]"#] {
        let text = format!("{REQUIRED}streams = {streams}\n");
        let error = IngestConfig::parse(&text).expect_err(streams).0;
        assert!(error.contains("go together"), "{error}");
    }
    let text = format!("{REQUIRED}streams = [\"depth\", \"depthSnapshot\"]\n");
    assert!(IngestConfig::parse(&text).is_ok());
    let text = format!("{REQUIRED}ws_public_base_url = \"public\"\n");
    assert!(IngestConfig::parse(&text).is_err());
}

#[test]
fn unknown_keys_are_rejected() {
    for extra in [
        "\n[capture]\nhold_back = 5\n",
        "\n[secrets]\napi_key = \"x\"\n",
    ] {
        let text = format!("{REQUIRED}{extra}");
        let error = IngestConfig::parse(&text).expect_err("rejected").0;
        assert!(error.contains("unknown"), "{error}");
    }
    let text = REQUIRED.replace(
        "symbol = \"BTCUSDT\"",
        "symbol = \"BTCUSDT\"\nvenue = \"x\"",
    );
    assert!(IngestConfig::parse(&text).is_err());
}

#[test]
fn live_capture_may_not_use_the_archive_source() {
    let text = REQUIRED.replace("source = \"binance-um\"", "source = \"binance-archive\"");
    assert_ne!(text, REQUIRED);
    let error = IngestConfig::parse(&text).expect_err("reserved source").0;
    assert!(
        error.contains("reserved for the archive backfill"),
        "{error}"
    );
}

#[test]
fn missing_keys_are_rejected() {
    for key in ["raw_root = ", "journal = ", "ws_base_url = ", "symbol = "] {
        let text: String = REQUIRED
            .lines()
            .filter(|line| !line.starts_with(key))
            .map(|line| format!("{line}\n"))
            .collect();
        let error = IngestConfig::parse(&text).expect_err(key).0;
        assert!(error.contains("missing"), "{key}: {error}");
    }
}

#[test]
fn invalid_values_are_rejected() {
    let cases = [
        ("symbol = \"BTCUSDT\"", "symbol = \"BTC/USDT\""),
        (
            "rest_base_url = \"https://fapi.binance.com\"",
            "rest_base_url = \"https://fapi.binance.com\"\nstreams = [\"depth\"]",
        ),
        (
            "rest_base_url = \"https://fapi.binance.com\"",
            "rest_base_url = \"https://fapi.binance.com\"\nstreams = [\"aggTrade\", \"aggTrade\"]",
        ),
        (
            "rest_base_url = \"https://fapi.binance.com\"",
            "rest_base_url = \"https://fapi.binance.com\"\nstreams = []",
        ),
        (
            "ws_base_url = \"wss://fstream.binance.com/market/ws\"",
            "ws_base_url = \"fstream\"",
        ),
    ];
    for (from, to) in cases {
        let text = REQUIRED.replace(from, to);
        assert!(IngestConfig::parse(&text).is_err(), "{to}");
    }
    for capture in [
        "seal_interval_secs = 0",
        "state_checkpoint_interval_secs = 0",
        "state_checkpoint_interval_secs = 18446744073709551",
        "backoff_initial_ms = 5000\nbackoff_max_ms = 100",
        // The seventh stream would rotate at 85 500 + 6 × 300 s, past 24 h.
        "max_connection_age_secs = 85500",
    ] {
        let text = format!("{REQUIRED}[capture]\n{capture}\n");
        assert!(IngestConfig::parse(&text).is_err(), "{capture}");
    }
}

mod history {
    use super::REQUIRED;
    use mie_cli::config::{ConfigError, IngestConfig};
    use std::path::Path;

    const HISTORY: &str = "\n[history]\norigin = \"2025-10-01\"\ncutover = \"2026-10-20\"\n\
                           archive_config = \"archive.toml\"\n";

    #[test]
    fn the_history_section_parses() {
        let config = IngestConfig::parse(&format!("{REQUIRED}{HISTORY}")).expect("valid");
        let history = config.history.expect("present");
        assert_eq!(history.origin, "2025-10-01");
        assert_eq!(history.archive_config, Path::new("archive.toml"));
        // 2025-10-01 and 2026-10-20, 00:00 UTC.
        assert_eq!(history.origin_ms(), Ok(1_759_276_800_000));
        assert_eq!(history.cutover_ms(), Ok(1_792_454_400_000));
        assert_eq!(IngestConfig::parse(REQUIRED).unwrap().history, None);
    }

    #[test]
    fn invalid_history_sections_are_rejected() {
        for (from, to, expected) in [
            (
                "origin = \"2025-10-01\"",
                "origin = \"2025-10-01T00:00:00Z\"",
                "not a UTC date",
            ),
            (
                "cutover = \"2026-10-20\"",
                "cutover = \"2026-10-20 00:00\"",
                "not a UTC date",
            ),
            (
                "cutover = \"2026-10-20\"",
                "cutover = \"2025-10-01\"",
                "not before",
            ),
            (
                "cutover = \"2026-10-20\"",
                "cutover = \"2025-09-30\"",
                "not before",
            ),
            (
                "archive_config = \"archive.toml\"",
                "archive_config = \"archive.toml\"\nstreams = [\"aggTrades\"]",
                "unknown field",
            ),
            ("archive_config = \"archive.toml\"", "", "missing field"),
            ("origin = \"2025-10-01\"", "", "missing field"),
        ] {
            let text = format!("{REQUIRED}{}", HISTORY.replace(from, to));
            let error = IngestConfig::parse(&text).expect_err(to).0;
            assert!(error.contains(expected), "{to}: {error}");
        }
    }

    #[test]
    fn the_memory_guard_key_is_optional_positive_and_without_default() {
        let config = IngestConfig::parse(&format!("{REQUIRED}{HISTORY}")).expect("valid");
        assert_eq!(config.history.unwrap().max_run_records, None);
        let text = format!("{REQUIRED}{HISTORY}max_run_records = 25000000\n");
        let config = IngestConfig::parse(&text).expect("valid");
        assert_eq!(config.history.unwrap().max_run_records, Some(25_000_000));
        for (line, expected) in [
            ("max_run_records = 0", "must be positive"),
            ("max_run_records = -5", "max_run_records"),
            ("max_run_records = \"many\"", "max_run_records"),
            ("max_run_bytes = 5", "unknown field"),
        ] {
            let text = format!("{REQUIRED}{HISTORY}{line}\n");
            let error = IngestConfig::parse(&text).expect_err(line).0;
            assert!(error.contains(expected), "{line}: {error}");
        }
    }

    /// An ingest configuration under `dir` with a `[history]` naming the
    /// archive configuration `archive.toml` there, whose raw root is
    /// `archive_root` and symbol `symbol`.
    fn configs(dir: &Path, archive_root: &Path, symbol: &str) -> IngestConfig {
        std::fs::create_dir_all(dir.join("raw")).unwrap();
        std::fs::create_dir_all(archive_root).unwrap();
        std::fs::write(
            dir.join("archive.toml"),
            format!(
                "[instrument]\nsymbol = \"{symbol}\"\n[paths]\nraw_root = \"{}\"\n\
                 import_ledger = \"{}\"\nstaging = \"{}\"\n",
                archive_root.display(),
                dir.join("ledger").display(),
                dir.join("staging").display()
            ),
        )
        .unwrap();
        IngestConfig::parse(&format!(
            "[instrument]\nsymbol = \"BTCUSDT\"\nsource = \"binance-um\"\n[paths]\n\
             raw_root = \"{}\"\njournal = \"{}\"\n[binance]\n\
             ws_base_url = \"wss://fake.invalid/ws\"\nrest_base_url = \"https://fake.invalid\"\n\
             [history]\norigin = \"2025-10-01\"\ncutover = \"2026-10-20\"\n\
             archive_config = \"{}\"\n",
            dir.join("raw").display(),
            dir.join("journal.jsonl").display(),
            dir.join("archive.toml").display()
        ))
        .unwrap()
    }

    #[test]
    fn the_archive_configuration_shares_the_raw_store_and_the_symbol() {
        let dir = crate::common::TempDir::new("config-history");
        let dir = dir.path();
        // The same root, spelled differently.
        let config = configs(dir, &dir.join("sub").join("..").join("raw"), "BTCUSDT");
        let (history, archive) = config.history_archive().unwrap().expect("present");
        assert_eq!(Some(history), config.history);
        assert_eq!(archive.instrument.symbol, "BTCUSDT");

        // Another raw root: the error names both.
        let other = dir.join("other-raw");
        let error = configs(dir, &other, "BTCUSDT")
            .history_archive()
            .unwrap_err()
            .0;
        assert!(
            error.contains(&other.display().to_string())
                && error.contains(&dir.join("raw").display().to_string()),
            "{error}"
        );
        // Another symbol.
        let error = configs(dir, &dir.join("raw"), "ETHUSDT")
            .history_archive()
            .unwrap_err()
            .0;
        assert!(error.contains("ETHUSDT"), "{error}");
        // An unreadable archive configuration.
        let config = configs(dir, &dir.join("raw"), "BTCUSDT");
        std::fs::remove_file(dir.join("archive.toml")).unwrap();
        let error: ConfigError = config.history_archive().unwrap_err();
        assert!(error.0.contains("archive.toml"), "{}", error.0);
    }
}

mod archive {
    use mie_adapter_binance::archive::ArchiveStream;
    use mie_cli::config::ArchiveConfig;
    use mie_domain::bars::Timeframe;

    const REQUIRED: &str = r#"
[instrument]
symbol = "BTCUSDT"
[paths]
raw_root = "data/raw"
import_ledger = "data/archive-ledger"
staging = "data/archive-staging"
"#;

    #[test]
    fn the_example_file_parses_with_the_documented_defaults() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/archive.example.toml");
        let example = ArchiveConfig::load(std::path::Path::new(path)).expect("example parses");
        let minimal = ArchiveConfig::parse(REQUIRED).expect("required keys suffice");
        assert_eq!(example.archive, minimal.archive);
        assert_eq!(example.archive.base_url, "https://data.binance.vision");
        let streams = example.streams().unwrap();
        assert_eq!(streams.len(), 10);
        assert!(streams.contains(&ArchiveStream::Klines(Timeframe::H4)));
        assert!(!streams.contains(&ArchiveStream::Trades));
    }

    #[test]
    fn unknown_keys_and_a_source_key_are_rejected() {
        for text in [
            REQUIRED.replace(
                "symbol = \"BTCUSDT\"",
                "symbol = \"BTCUSDT\"\nsource = \"binance-um\"",
            ),
            format!("{REQUIRED}[archive]\nconcurrency = 4\n"),
            format!("{REQUIRED}[secrets]\napi_key = \"x\"\n"),
        ] {
            let error = ArchiveConfig::parse(&text).expect_err("rejected").0;
            assert!(error.contains("unknown"), "{error}");
        }
    }

    #[test]
    fn unknown_or_repeated_streams_and_intervals_are_rejected() {
        for archive in [
            "streams = [\"bookTicker\"]",
            "streams = [\"aggTrades\", \"aggTrades\"]",
            "streams = [\"klines\", \"klines_1m\"]",
            "streams = []",
            "kline_intervals = [\"3m\"]",
            "kline_intervals = [\"1m\", \"1m\"]",
            "max_attempts = 0",
            "backoff_initial_ms = 5000\nbackoff_max_ms = 100",
            "base_url = \"data.binance.vision\"",
        ] {
            let text = format!("{REQUIRED}[archive]\n{archive}\n");
            assert!(ArchiveConfig::parse(&text).is_err(), "{archive}");
        }
        let text = format!("{REQUIRED}[archive]\nstreams = [\"trades\", \"klines_1d\"]\n");
        assert_eq!(
            ArchiveConfig::parse(&text).unwrap().streams().unwrap(),
            [ArchiveStream::Klines(Timeframe::D1), ArchiveStream::Trades]
        );
    }
}
