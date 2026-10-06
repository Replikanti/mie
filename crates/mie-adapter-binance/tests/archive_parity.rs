//! Live and archive normalization agree: the same trade or one-minute bar,
//! as a live payload and as an archive row, becomes the identical domain
//! event (ADR-019: one domain path for live and replay).
//!
//! The live payloads are recorded frames (`tests/fixtures/*.jsonl`, captured
//! 2026-10-06). The archive day 2026-10-06 was not yet published when this
//! test was written, so each archive row is built from the documented column
//! mapping of the same exchange fields, with the archive's number style
//! (trailing zeros of prices dropped, as in the published files).

use mie_adapter_binance::BinanceStream;
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::normalize as archive;
use mie_adapter_binance::normalize as live;
use mie_domain::bars::Timeframe;

const SYMBOL: &str = "BTCUSDT";

fn live_lines(stream: BinanceStream) -> Vec<String> {
    let path = format!(
        "{}/tests/fixtures/{}.jsonl",
        env!("CARGO_MANIFEST_DIR"),
        stream.raw_name()
    );
    std::fs::read_to_string(path)
        .expect("read fixture")
        .lines()
        .map(str::to_owned)
        .collect()
}

#[test]
fn an_aggregate_trade_normalizes_identically_from_both_sources() {
    let frames = live_lines(BinanceStream::AggTrade);
    // `a`, `p`, `q`, `f`, `l`, `T`, `m` ↔ the seven archive columns.
    let pairs = [
        (
            &frames[0],
            "3476323248,85294.8,0.001,8149111032,8149111032,1791271449268,true",
        ),
        (
            frames
                .iter()
                .find(|f| f.contains(r#""a":3476323250,"#))
                .unwrap(),
            "3476323250,85294.9,0.075,8149111035,8149111035,1791271453237,false",
        ),
    ];
    for (frame, row) in pairs {
        let from_live = live::parse(BinanceStream::AggTrade, SYMBOL, frame.as_bytes())
            .unwrap()
            .unwrap();
        let from_archive = archive::parse(ArchiveStream::AggTrades, SYMBOL, row.as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(from_live, from_archive, "{row}");
        // Both are filed at the same raw event time.
        assert_eq!(
            live::record_time(BinanceStream::AggTrade, frame.as_bytes()),
            archive::record_time(ArchiveStream::AggTrades, row.as_bytes()).ok()
        );
    }
}

#[test]
fn a_closed_one_minute_kline_normalizes_identically_from_both_sources() {
    let frames = live_lines(BinanceStream::Kline1m);
    let closed = frames
        .iter()
        .find(|f| f.contains(r#""t":1791271440000"#) && f.contains(r#""x":true"#))
        .expect("the closed 07:24 bar");
    // `t`, `o`, `h`, `l`, `c`, `v`, `T`, `q`, `n`, `V`, `Q`, `B` ↔ the twelve
    // archive columns.
    let row = "1791271440000,85294.90,85294.90,85277.90,85280.80,12.501,1791271499999,\
               1066119.24170,461,7.953,678229.08630,0";
    let from_live = live::parse(BinanceStream::Kline1m, SYMBOL, closed.as_bytes())
        .unwrap()
        .unwrap();
    let from_archive = archive::parse(ArchiveStream::Klines(Timeframe::M1), SYMBOL, row.as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(from_live, from_archive);
    assert_eq!(
        live::record_time(BinanceStream::Kline1m, closed.as_bytes()),
        archive::record_time(ArchiveStream::Klines(Timeframe::M1), row.as_bytes()).ok()
    );
}
