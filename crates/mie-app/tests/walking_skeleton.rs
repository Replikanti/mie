//! Walking skeleton: one domain path serves live analysis and historical
//! replay (ADR-019). The in-memory providers stand in for the Binance and
//! raw-Parquet adapters.

use mie_app::{ReplayService, drive};
use mie_domain::event::{Aggressor, MarketEvent, Trade};
use mie_domain::num::{Price, Qty};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::inbound::{ReplayMarket, UseCaseError};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, ReplayWindow,
};
use std::collections::VecDeque;

/// Stand-in for any market-data adapter: yields queued results in order.
struct Feed(VecDeque<Result<MarketEvent, ProviderError>>);

impl Feed {
    fn of(events: Vec<MarketEvent>) -> Self {
        Self(events.into_iter().map(Ok).collect())
    }
}

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        self.0.pop_front().transpose()
    }
}

/// Stand-in for the raw-data replay adapter.
struct Recorded(Vec<MarketEvent>);

impl HistoricalDataProvider for Recorded {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Feed, ProviderError> {
        let in_window = self.0.iter().filter(|e| window.contains(e.time()));
        Ok(Feed::of(in_window.cloned().collect()))
    }
}

fn trade(millis: i64, trade_id: u64, price_units: i64, aggressor: Aggressor) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: EventTime::from_millis(millis),
        trade_id,
        price: Price::from_units(price_units),
        qty: Qty::from_units(1_000_000),
        aggressor,
    })
}

fn tape() -> Vec<MarketEvent> {
    vec![
        trade(1_000, 1, 6_354_200_000_000, Aggressor::Buy),
        trade(1_000, 2, 6_354_210_000_000, Aggressor::Buy),
        trade(1_250, 3, 6_354_190_000_000, Aggressor::Sell),
        trade(2_000, 4, 6_354_180_000_000, Aggressor::Sell),
    ]
}

fn window(start: i64, end: i64) -> ReplayWindow {
    ReplayWindow {
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
    }
}

#[test]
fn live_and_replay_paths_produce_identical_state() {
    let mut engine = MarketStateEngine::new();
    let live_events = drive(&mut Feed::of(tape()), &mut engine).unwrap();

    let report = ReplayService::new(Recorded(tape()))
        .replay(window(0, 10_000))
        .unwrap();

    assert_eq!(live_events, 4);
    assert_eq!(report.events, live_events);
    assert_eq!(&report.state, engine.state());
}

#[test]
fn replay_respects_the_window() {
    let report = ReplayService::new(Recorded(tape()))
        .replay(window(1_000, 2_000))
        .unwrap();

    assert_eq!(report.events, 3);
    assert_eq!(report.state.as_of, Some(EventTime::from_millis(1_250)));
    assert_eq!(
        report.state.last_trade_price,
        Some(Price::from_units(6_354_190_000_000))
    );
}

#[test]
fn domain_rejects_an_out_of_order_feed() {
    let mut feed = Feed::of(vec![
        trade(2_000, 1, 1, Aggressor::Buy),
        trade(1_000, 2, 2, Aggressor::Sell),
    ]);
    let mut engine = MarketStateEngine::new();

    let err = drive(&mut feed, &mut engine).unwrap_err();

    assert_eq!(
        err,
        UseCaseError::Domain(StateError::TimeWentBackwards {
            as_of: EventTime::from_millis(2_000),
            event: EventTime::from_millis(1_000),
        })
    );
    assert_eq!(engine.state().trade_count, 1);
}

#[test]
fn provider_failure_stops_the_drive() {
    let failure = ProviderError::Source("connection reset".into());
    let mut feed = Feed(VecDeque::from([
        Ok(trade(1_000, 1, 1, Aggressor::Buy)),
        Err(failure.clone()),
        Ok(trade(2_000, 2, 2, Aggressor::Buy)),
    ]));
    let mut engine = MarketStateEngine::new();

    let err = drive(&mut feed, &mut engine).unwrap_err();

    assert_eq!(err, UseCaseError::Provider(failure));
    assert_eq!(engine.state().trade_count, 1);
}
