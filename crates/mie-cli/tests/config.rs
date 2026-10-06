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
    assert_eq!(live.max_connection_age, Duration::from_secs(82_800));
    assert_eq!(live.rotation_stagger, Duration::from_secs(300));
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
        "backoff_initial_ms = 5000\nbackoff_max_ms = 100",
        // The fifth stream would rotate at 85 500 + 4 × 300 s, past 24 h.
        "max_connection_age_secs = 85500",
    ] {
        let text = format!("{REQUIRED}[capture]\n{capture}\n");
        assert!(IngestConfig::parse(&text).is_err(), "{capture}");
    }
}
