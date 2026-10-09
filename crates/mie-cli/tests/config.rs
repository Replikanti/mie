//! The ingest configuration file.

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
    assert_eq!(example.capture.state_checkpoint_interval_secs, 60);
    assert_eq!(example.state_checkpoint_interval_ms(), Ok(60_000));
    // Every stream by default, depth included.
    assert_eq!(live.streams.len(), 7);
    assert!(live.streams.contains(&BinanceStream::Depth));
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
