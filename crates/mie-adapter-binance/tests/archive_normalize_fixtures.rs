//! Normalization of real archive rows (`tests/fixtures/archive/*.csv`, see
//! the README there for provenance): the first 20 and last 5 data rows of
//! each 2026-09-30 file and of the 2026-08 funding file.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::normalize::{METRICS_RESOLUTION_MS, parse, record_time};
use mie_adapter_binance::normalize::NormalizeError;
use mie_domain::bars::Timeframe;
use mie_domain::event::{
    Aggressor, FundingSettlement, Kline, MarketEvent, OpenInterest, Stream, Trade,
};
use mie_domain::num::{ParseDecimalError, Price, Qty, Rate};
use mie_domain::time::EventTime;

const SYMBOL: &str = "BTCUSDT";

/// 2026-09-30T00:00:00Z in ms.
const DAY: i64 = 1_790_726_400_000;

const FIXTURES: [(&str, ArchiveStream); 6] = [
    ("BTCUSDT-aggTrades-2026-09-30", ArchiveStream::AggTrades),
    (
        "BTCUSDT-1m-2026-09-30",
        ArchiveStream::Klines(Timeframe::M1),
    ),
    (
        "BTCUSDT-1d-2026-09-30",
        ArchiveStream::Klines(Timeframe::D1),
    ),
    ("BTCUSDT-metrics-2026-09-30", ArchiveStream::Metrics),
    ("BTCUSDT-bookDepth-2026-09-30", ArchiveStream::BookDepth),
    ("BTCUSDT-fundingRate-2026-08", ArchiveStream::FundingRate),
];

/// The header line and the data rows of a fixture.
fn fixture(name: &str) -> (String, Vec<String>) {
    let path = format!(
        "{}/tests/fixtures/archive/{name}.csv",
        env!("CARGO_MANIFEST_DIR")
    );
    let text = std::fs::read_to_string(&path).expect("read fixture");
    let mut lines = text.lines().map(str::to_owned);
    let header = lines.next().expect("header");
    (header, lines.collect())
}

fn rows(stream: ArchiveStream) -> Vec<String> {
    let (name, _) = FIXTURES.iter().find(|(_, s)| *s == stream).unwrap();
    fixture(name).1
}

fn event(stream: ArchiveStream, row: &str) -> MarketEvent {
    parse(stream, SYMBOL, row.as_bytes())
        .unwrap_or_else(|e| panic!("{stream} {row}: {e}"))
        .expect("has an event")
}

fn t(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

#[test]
fn every_fixture_row_normalizes_and_is_filed_at_its_ordering_time() {
    for (name, stream) in FIXTURES {
        let (header, rows) = fixture(name);
        assert_eq!(header, stream.expected_header(), "{name}");
        assert_eq!(rows.len(), if name.contains("-1d-") { 1 } else { 25 });
        for row in rows {
            let time = record_time(stream, row.as_bytes()).expect("record time");
            assert!(
                (DAY - 61 * 86_400_000..=DAY + 86_400_000).contains(&time.as_millis()),
                "{name}: {row}"
            );
            match parse(stream, SYMBOL, row.as_bytes()).expect(&row) {
                Some(event) => {
                    assert_eq!(Some(event.stream()), stream.domain_stream());
                    assert_eq!(event.time(), time, "{row}");
                }
                None => assert_eq!(stream, ArchiveStream::BookDepth),
            }
        }
    }
}

#[test]
fn agg_trades_map_exact_units_and_the_maker_flag_like_live() {
    let rows = rows(ArchiveStream::AggTrades);
    assert_eq!(
        event(ArchiveStream::AggTrades, &rows[0]),
        MarketEvent::Trade(Trade {
            time: t(1_790_726_400_261),
            trade_id: 3_469_657_145,
            price: Price::from_units(8_362_450_000_000),
            qty: Qty::from_units(3_000_000),
            aggressor: Aggressor::Buy,
        })
    );
    // `is_buyer_maker = true`: the seller crossed the spread.
    let last = rows.last().unwrap();
    assert!(last.ends_with(",false"));
    let maker = last.replace(",false", ",true");
    let MarketEvent::Trade(trade) = event(ArchiveStream::AggTrades, &maker) else {
        panic!("not a trade");
    };
    assert_eq!(trade.aggressor, Aggressor::Sell);
    // The head rows are contiguous aggregate ids in time order.
    let trades: Vec<Trade> = rows[..20]
        .iter()
        .map(|row| match event(ArchiveStream::AggTrades, row) {
            MarketEvent::Trade(trade) => trade,
            other => panic!("{other:?}"),
        })
        .collect();
    for pair in trades.windows(2) {
        assert_eq!(pair[1].trade_id, pair[0].trade_id + 1);
        assert!(pair[1].time >= pair[0].time);
    }
}

#[test]
fn klines_map_exactly_and_match_their_timeframe() {
    let one_minute = ArchiveStream::Klines(Timeframe::M1);
    let first = event(one_minute, &rows(one_minute)[0]);
    let MarketEvent::Kline(kline) = first else {
        panic!("not a kline");
    };
    assert_eq!(kline.open_time, t(DAY));
    assert_eq!(kline.close_time, t(DAY + 59_999));
    assert_eq!(Timeframe::of_kline(&kline), Some(Timeframe::M1));

    let daily = ArchiveStream::Klines(Timeframe::D1);
    assert_eq!(
        event(daily, &rows(daily)[0]),
        MarketEvent::Kline(Kline {
            open_time: t(DAY),
            close_time: t(DAY + 86_399_999),
            open: Price::from_units(8_362_450_000_000),
            high: Price::from_units(8_563_270_000_000),
            low: Price::from_units(8_290_130_000_000),
            close: Price::from_units(8_357_690_000_000),
            volume: Qty::from_units(17_684_875_500_000),
            taker_buy_volume: Qty::from_units(8_853_744_700_000),
            trade_count: 3_850_262,
        })
    );
}

#[test]
fn a_kline_on_the_wrong_interval_is_rejected() {
    let row = &rows(ArchiveStream::Klines(Timeframe::M1))[0];
    let error = parse(ArchiveStream::Klines(Timeframe::M5), SYMBOL, row.as_bytes())
        .expect_err("a 1m row is not a 5m kline");
    assert!(matches!(error, NormalizeError::Unexpected(_)), "{error}");
}

#[test]
fn metrics_are_ordered_one_resolution_step_after_create_time() {
    let rows = rows(ArchiveStream::Metrics);
    assert!(rows[0].starts_with("2026-09-30 02:30:00,BTCUSDT,92849.1660000000000000,"));
    assert_eq!(
        event(ArchiveStream::Metrics, &rows[0]),
        MarketEvent::OpenInterest(OpenInterest {
            // 02:30:00 + 5 min.
            time: t(DAY + 9_000_000 + 300_000),
            open_interest: Qty::from_units(9_284_916_600_000),
            resolution_ms: 300_000,
        })
    );
    assert_eq!(METRICS_RESOLUTION_MS, 300_000);
    // The day's last sample is ordered at the next day's first millisecond.
    let last = format!("2026-09-30 23:55:00{}", &rows[0][19..]);
    assert_eq!(
        record_time(ArchiveStream::Metrics, last.as_bytes()),
        Ok(t(DAY + 86_400_000))
    );
    // The rows are not in time order.
    let times: Vec<_> = rows
        .iter()
        .map(|r| record_time(ArchiveStream::Metrics, r.as_bytes()).unwrap())
        .collect();
    assert!(times.windows(2).any(|w| w[1] < w[0]));
}

#[test]
fn trailing_zeros_pass_but_a_ninth_significant_decimal_fails() {
    let row = &rows(ArchiveStream::Metrics)[0];
    let ninth = row.replace("92849.1660000000000000", "92849.1660000010000000");
    assert_eq!(
        parse(ArchiveStream::Metrics, SYMBOL, ninth.as_bytes()),
        Err(NormalizeError::Decimal {
            field: "sum_open_interest",
            error: ParseDecimalError::TooManyDecimals,
        })
    );
}

#[test]
fn a_metrics_row_of_another_symbol_is_rejected() {
    let row = rows(ArchiveStream::Metrics)[0].replace(",BTCUSDT,", ",ETHUSDT,");
    assert!(matches!(
        parse(ArchiveStream::Metrics, SYMBOL, row.as_bytes()),
        Err(NormalizeError::Unexpected(_))
    ));
}

#[test]
fn funding_settlements_keep_the_published_calc_time() {
    let rows = rows(ArchiveStream::FundingRate);
    assert_eq!(
        event(ArchiveStream::FundingRate, &rows[0]),
        MarketEvent::FundingSettlement(FundingSettlement {
            // 2026-08-01T00:00:00.001Z: the archive's millisecond jitter.
            time: t(1_785_542_400_001),
            rate: Rate::from_units(4_123),
        })
    );
    assert_eq!(
        event(ArchiveStream::FundingRate, &rows[0]).stream(),
        Stream::Funding
    );
}

#[test]
fn book_depth_is_raw_only_with_a_second_resolution_time() {
    let rows = rows(ArchiveStream::BookDepth);
    assert_eq!(
        parse(ArchiveStream::BookDepth, SYMBOL, rows[0].as_bytes()),
        Ok(None)
    );
    assert_eq!(
        record_time(ArchiveStream::BookDepth, rows[0].as_bytes()),
        Ok(t(DAY + 1_000))
    );
}

#[test]
fn a_header_line_is_not_a_row() {
    for (name, stream) in FIXTURES {
        let (header, _) = fixture(name);
        assert!(
            record_time(stream, header.as_bytes()).is_err(),
            "{name}: the header must never pass as data"
        );
    }
}

#[test]
fn individual_trades_carry_the_raw_trade_id() {
    // The first row of the 2026-09-30 `trades` file (raw only, cross-check).
    let row = "8131311698,83624.5,0.006,501.747,1790726400003,false";
    assert_eq!(
        parse(ArchiveStream::Trades, SYMBOL, row.as_bytes()),
        Ok(Some(MarketEvent::Trade(Trade {
            time: t(1_790_726_400_003),
            trade_id: 8_131_311_698,
            price: Price::from_units(8_362_450_000_000),
            qty: Qty::from_units(600_000),
            aggressor: Aggressor::Buy,
        })))
    );
    assert_eq!(
        ArchiveStream::Trades.expected_header(),
        "id,price,qty,quote_qty,time,is_buyer_maker"
    );
}
