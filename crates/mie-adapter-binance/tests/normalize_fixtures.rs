//! Normalization of recorded payloads (`tests/fixtures/*.jsonl`, see the
//! README there for provenance).

use mie_adapter_binance::BinanceStream;
use mie_adapter_binance::normalize::{parse, record_time};
use mie_domain::event::{
    Aggressor, Kline, Liquidation, MarkPrice, MarketEvent, OpenInterest, Trade,
};
use mie_domain::num::{Price, Qty, Rate};
use mie_domain::time::EventTime;

fn lines(stream: BinanceStream) -> Vec<String> {
    let path = format!(
        "{}/tests/fixtures/{}.jsonl",
        env!("CARGO_MANIFEST_DIR"),
        stream.raw_name()
    );
    std::fs::read_to_string(&path)
        .expect("read fixture")
        .lines()
        .map(str::to_owned)
        .collect()
}

fn events(stream: BinanceStream) -> Vec<Option<MarketEvent>> {
    lines(stream)
        .iter()
        .enumerate()
        .map(|(i, line)| {
            parse(stream, "BTCUSDT", line.as_bytes())
                .unwrap_or_else(|e| panic!("{} line {}: {e}", stream.raw_name(), i + 1))
        })
        .collect()
}

fn t(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

#[test]
fn every_fixture_line_normalizes_and_has_a_record_time() {
    for stream in BinanceStream::ALL {
        let lines = lines(stream);
        assert!(!lines.is_empty(), "{}", stream.raw_name());
        for (line, event) in lines.iter().zip(events(stream)) {
            let time = record_time(stream, line.as_bytes()).expect("record time");
            if let Some(event) = event {
                assert_eq!(event.stream(), stream.domain_stream());
                // A record is filed at its event's ordering time.
                assert_eq!(event.time(), time, "{line}");
            }
        }
    }
}

#[test]
fn recorded_trades_map_exactly_and_ids_are_contiguous() {
    let trades: Vec<Trade> = events(BinanceStream::AggTrade)
        .into_iter()
        .map(|e| match e {
            Some(MarketEvent::Trade(trade)) => trade,
            other => panic!("not a trade: {other:?}"),
        })
        .collect();
    assert_eq!(
        trades[0],
        Trade {
            time: t(1_791_271_449_268),
            trade_id: 3_476_323_248,
            price: Price::from_units(8_529_480_000_000),
            qty: Qty::from_units(100_000),
            aggressor: Aggressor::Sell,
        }
    );
    assert!(
        trades
            .windows(2)
            .all(|w| w[1].trade_id == w[0].trade_id + 1)
    );
    // `m` = buyer is maker ⇒ aggressive sell, on every line.
    let lines = lines(BinanceStream::AggTrade);
    for (line, trade) in lines.iter().zip(&trades) {
        let maker_buyer = line.contains(r#""m":true"#);
        assert_eq!(trade.aggressor == Aggressor::Sell, maker_buyer, "{line}");
    }
    let sells = trades
        .iter()
        .filter(|t| t.aggressor == Aggressor::Sell)
        .count();
    assert_eq!(sells, 246);
}

#[test]
fn recorded_mark_prices_map_exactly() {
    assert_eq!(
        events(BinanceStream::MarkPrice)[0],
        Some(MarketEvent::MarkPrice(MarkPrice {
            time: t(1_791_271_448_000),
            mark_price: Price::from_units(8_529_480_000_000),
            index_price: Price::from_units(8_534_059_652_174),
            funding_rate: Rate::from_units(124),
            next_funding_time: t(1_791_273_600_000),
        }))
    );
}

#[test]
fn only_closed_klines_become_events() {
    let events = events(BinanceStream::Kline1m);
    let closed: Vec<&MarketEvent> = events.iter().flatten().collect();
    assert_eq!(closed.len(), 2);
    assert_eq!(
        *closed[0],
        MarketEvent::Kline(Kline {
            open_time: t(1_791_271_440_000),
            close_time: t(1_791_271_499_999),
            open: Price::from_units(8_529_490_000_000),
            high: Price::from_units(8_529_490_000_000),
            low: Price::from_units(8_527_790_000_000),
            close: Price::from_units(8_528_080_000_000),
            volume: Qty::from_units(1_250_100_000),
            taker_buy_volume: Qty::from_units(795_300_000),
            trade_count: 461,
        })
    );
}

#[test]
fn recorded_open_interest_carries_the_poll_resolution() {
    let events = events(BinanceStream::OpenInterest);
    assert_eq!(
        events[0],
        Some(MarketEvent::OpenInterest(OpenInterest {
            time: t(1_791_271_442_709),
            open_interest: Qty::from_units(9_530_569_400_000),
            resolution_ms: 10_000,
        }))
    );
    assert!(events.iter().all(|e| matches!(
        e,
        Some(MarketEvent::OpenInterest(oi)) if oi.resolution_ms == 10_000
    )));
}

#[test]
fn liquidation_sides_map_from_the_order_side() {
    let events = events(BinanceStream::ForceOrder);
    assert_eq!(
        events[0],
        Some(MarketEvent::Liquidation(Liquidation {
            time: t(1_568_014_460_893),
            aggressor: Aggressor::Sell,
            price: Price::from_units(991_000_000_000),
            avg_price: Price::from_units(991_000_000_000),
            filled_qty: Qty::from_units(1_400_000),
        }))
    );
    assert!(matches!(
        events[1],
        Some(MarketEvent::Liquidation(Liquidation {
            aggressor: Aggressor::Buy,
            ..
        }))
    ));
}
