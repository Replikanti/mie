//! The kline cross-check harness (ADR-031) on a synthetic multi-stream tape.
//! The in-memory feed stands in for a live stream or an archive replay.

use mie_app::kline_check::{KlineCheckReport, KlineMismatch, cross_check_klines};
use mie_app::{drive, drive_observed};
use mie_domain::bars::{Bar, KlineField, Timeframe};
use mie_domain::event::{
    Aggressor, BookSnapshot, FeedGap, GapReason, Kline, Level, MarkPrice, MarketEvent, Stream,
    Trade,
};
use mie_domain::num::{Price, Qty, Rate};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::inbound::UseCaseError;
use mie_ports::outbound::{MarketDataProvider, ProviderError};
use std::collections::VecDeque;

/// Stand-in for any market-data adapter: releases its events in canonical
/// order.
struct Feed(VecDeque<MarketEvent>);

impl Feed {
    fn of(mut events: Vec<MarketEvent>) -> Self {
        events.sort();
        Self(events.into())
    }
}

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

fn at(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

fn trade(millis: i64, trade_id: u64, price: i64, qty: i64, aggressor: Aggressor) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: at(millis),
        trade_id,
        price: Price::from_units(price),
        qty: Qty::from_units(qty),
        aggressor,
    })
}

/// A kline over `[open, close]` with prices `[open, high, low, close]` and
/// volumes `[volume, taker_buy_volume]`.
fn kline(open: i64, close: i64, prices: [i64; 4], volumes: [i64; 2], count: u64) -> MarketEvent {
    MarketEvent::Kline(Kline {
        open_time: at(open),
        close_time: at(close),
        open: Price::from_units(prices[0]),
        high: Price::from_units(prices[1]),
        low: Price::from_units(prices[2]),
        close: Price::from_units(prices[3]),
        volume: Qty::from_units(volumes[0]),
        taker_buy_volume: Qty::from_units(volumes[1]),
        trade_count: count,
    })
}

fn mark(millis: i64) -> MarketEvent {
    MarketEvent::MarkPrice(MarkPrice {
        time: at(millis),
        mark_price: Price::from_units(100),
        index_price: Price::from_units(100),
        funding_rate: Rate::from_units(10_000),
        next_funding_time: at(28_800_000),
    })
}

fn snapshot(millis: i64) -> MarketEvent {
    let level = |price| Level {
        price: Price::from_units(price),
        qty: Qty::from_units(1),
    };
    MarketEvent::BookSnapshot(BookSnapshot {
        time: at(millis),
        last_update_id: 1,
        bids: vec![level(100)],
        asks: vec![level(101)],
    })
}

fn trades_gap(start: i64, end: i64) -> MarketEvent {
    MarketEvent::FeedGap(FeedGap {
        stream: Stream::Trades,
        start: at(start),
        end: at(end),
        reason: GapReason::Disconnected,
    })
}

use Aggressor::{Buy, Sell};

/// Eight minutes of trades, mark prices, a book snapshot, a trades gap and
/// exchange klines (1m, plus one 5m and one 3m).
fn tape() -> Vec<MarketEvent> {
    vec![
        // Minute 0: consumption starts mid-minute, so its bar is partial.
        mark(500),
        trade(30_000, 1, 100, 10, Buy),
        kline(0, 59_999, [100, 100, 100, 100], [10, 10], 1),
        // Minute 1: matches.
        trade(60_000, 2, 101, 5, Buy),
        snapshot(70_000),
        trade(90_000, 3, 103, 2, Sell),
        trade(119_999, 4, 102, 3, Buy),
        kline(60_000, 119_999, [101, 103, 101, 102], [10, 8], 3),
        // Minute 2: matches.
        trade(120_000, 5, 102, 4, Sell),
        trade(150_000, 6, 99, 6, Buy),
        kline(120_000, 179_999, [102, 102, 99, 99], [10, 6], 2),
        // Minute 3: the kline's high is one unit off.
        trade(181_000, 7, 100, 1, Buy),
        trade(200_000, 8, 104, 1, Buy),
        kline(180_000, 239_999, [100, 105, 100, 104], [2, 2], 2),
        // Minute 4: a trades gap makes it incomplete, and the 5m bar too.
        trade(250_000, 9, 104, 1, Sell),
        trades_gap(260_000, 270_000),
        trade(280_000, 10, 103, 1, Sell),
        kline(240_000, 299_999, [104, 104, 103, 103], [2, 0], 2),
        kline(0, 299_999, [100, 105, 99, 103], [27, 21], 10),
        // Minute 5: no trades; the exchange kline is flat at the last close
        // with zero volume. A 3m kline is not a configured interval.
        mark(330_000),
        kline(300_000, 359_999, [103, 103, 103, 103], [0, 0], 0),
        kline(180_000, 359_999, [100, 104, 100, 103], [4, 2], 4),
        // Minute 6: matches, but the kline counts raw trades.
        trade(360_500, 11, 105, 2, Buy),
        trade(400_000, 12, 106, 1, Sell),
        kline(360_000, 419_999, [105, 106, 105, 106], [3, 2], 5),
        // Minute 7 opens; its kline arrives, but no trade closes the bar.
        trade(420_000, 13, 107, 1, Buy),
        kline(420_000, 479_999, [107, 107, 107, 107], [1, 1], 1),
    ]
}

#[test]
fn compares_every_complete_bar_with_its_kline() {
    let mut engine = MarketStateEngine::new();
    let report = cross_check_klines(&mut Feed::of(tape()), &mut engine).unwrap();
    assert_eq!(
        report,
        KlineCheckReport {
            events: 27,
            compared: 5,
            matched: 4,
            mismatches: vec![KlineMismatch {
                timeframe: Timeframe::M1,
                open_time: at(180_000),
                fields: vec![KlineField::High],
            }],
            // Minutes 0 and 4, and the first 5m bar.
            incomplete_skipped: 3,
            bars_without_kline: 0,
            klines_without_bar: 1,
            unconfigured_klines: 1,
            trade_count_differences: 1,
        }
    );
    assert!(!report.all_matched());
    assert_eq!(
        report.to_string(),
        "kline cross-check over 27 events\n\
         complete bars compared: 5, matched: 4, mismatched: 1\n\
         incomplete bars skipped: 3\n\
         complete bars without a kline: 0\n\
         klines without a closed bar: 1\n\
         klines of an unconfigured interval: 1\n\
         trade-count differences (not a mismatch): 1\n\
         mismatch: 1m 180000ms high"
    );
    // The harness drove the normal domain path.
    assert_eq!(engine.state().trade_count, 13);
    assert_eq!(engine.state().as_of, Some(at(479_999)));
}

#[test]
fn a_complete_bar_without_a_kline_is_counted() {
    let mut engine = MarketStateEngine::new();
    let report = cross_check_klines(
        &mut Feed::of(vec![
            trade(30_000, 1, 100, 1, Buy),
            trade(60_000, 2, 100, 1, Buy),
            trade(120_000, 3, 100, 1, Buy),
        ]),
        &mut engine,
    )
    .unwrap();
    assert_eq!(report.incomplete_skipped, 1);
    assert_eq!(report.bars_without_kline, 1);
    assert_eq!(report.compared, 0);
    assert!(report.all_matched());
}

#[test]
fn a_domain_rejection_stops_the_check() {
    let late = trade(1_000, 2, 100, 1, Buy);
    let mut feed = Feed(VecDeque::from([trade(2_000, 1, 100, 1, Buy), late.clone()]));
    let mut engine = MarketStateEngine::new();
    let err = cross_check_klines(&mut feed, &mut engine).unwrap_err();
    assert!(matches!(
        err,
        UseCaseError::Domain(StateError::OutOfOrder { event, .. }) if event == late.canonical_key()
    ));
}

#[test]
fn drive_and_drive_observed_agree() {
    let mut plain = MarketStateEngine::new();
    let plain_events = drive(&mut Feed::of(tape()), &mut plain).unwrap();

    let mut observed = MarketStateEngine::new();
    let mut seen = Vec::new();
    let mut closed: Vec<Bar> = Vec::new();
    let observed_events = drive_observed(&mut Feed::of(tape()), &mut observed, |event, engine| {
        // The observer sees the state as of this event.
        assert_eq!(engine.state().as_of, Some(event.time()));
        seen.push(event.clone());
        closed.extend_from_slice(engine.closed_bars());
    })
    .unwrap();

    assert_eq!(observed_events, plain_events);
    assert_eq!(observed.state(), plain.state());
    assert_eq!(Feed::of(tape()).0, VecDeque::from(seen));
    // Minutes 0–6 and the first 5m bar closed; never one ending after the
    // event that closed it.
    assert_eq!(closed.len(), 8);
    assert_eq!(
        closed
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M1)
            .count(),
        7
    );
}
