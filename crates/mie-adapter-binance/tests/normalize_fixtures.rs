//! Normalization of recorded payloads (`tests/fixtures/*.jsonl`, see the
//! README there for provenance).

use mie_adapter_binance::BinanceStream;
use mie_adapter_binance::normalize::{parse, record_time};
use mie_domain::event::{
    Aggressor, BookSnapshot, BookUpdate, Kline, Level, Liquidation, MarkPrice, MarketEvent,
    OpenInterest, Trade,
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

fn level(price: i64, qty: i64) -> Level {
    Level {
        price: Price::from_units(price),
        qty: Qty::from_units(qty),
    }
}

#[test]
fn recorded_depth_diffs_map_exactly_and_chain() {
    let updates: Vec<BookUpdate> = events(BinanceStream::Depth)
        .into_iter()
        .map(|e| match e {
            Some(MarketEvent::BookUpdate(update)) => update,
            other => panic!("not a book update: {other:?}"),
        })
        .collect();
    assert_eq!(updates.len(), 29);
    let first = &updates[0];
    assert_eq!(first.time, t(1_791_400_658_578));
    assert_eq!(
        (
            first.first_update_id,
            first.last_update_id,
            first.prev_update_id
        ),
        (11_759_094_651_167, 11_759_094_663_893, 11_759_094_651_009)
    );
    assert_eq!((first.bids.len(), first.asks.len()), (174, 179));
    // Diffs reach far beyond the top of the book.
    assert_eq!(first.bids[0], level(3_289_740_000_000, 82_800_000));
    assert_eq!(first.asks[0], level(8_339_750_000_000, 130_600_000));
    // One connection: every diff chains on the previous one.
    assert!(
        updates
            .windows(2)
            .all(|w| w[1].prev_update_id == w[0].last_update_id)
    );
    // Removals are zero quantities, kept in source order.
    let removals = updates
        .iter()
        .flat_map(|u| u.bids.iter().chain(&u.asks))
        .filter(|l| l.qty.units() == 0)
        .count();
    assert_eq!(removals, 924);
}

#[test]
fn recorded_depth_snapshots_map_exactly() {
    let snapshots: Vec<BookSnapshot> = events(BinanceStream::DepthSnapshot)
        .into_iter()
        .map(|e| match e {
            Some(MarketEvent::BookSnapshot(snapshot)) => snapshot,
            other => panic!("not a book snapshot: {other:?}"),
        })
        .collect();
    assert_eq!(snapshots.len(), 2);
    let first = &snapshots[0];
    assert_eq!(first.time, t(1_791_400_659_048));
    assert_eq!(first.last_update_id, 11_759_094_709_820);
    // limit=100: one hundred levels per side, best first.
    assert_eq!((first.bids.len(), first.asks.len()), (100, 100));
    assert_eq!(first.bids[0], level(8_339_740_000_000, 913_200_000));
    assert_eq!(first.asks[0], level(8_339_750_000_000, 125_000_000));
    assert_eq!(first.bids[99].price, Price::from_units(8_338_460_000_000));
    assert_eq!(first.asks[99].price, Price::from_units(8_341_140_000_000));
    assert_eq!(snapshots[1].last_update_id, 11_759_095_019_149);
}
