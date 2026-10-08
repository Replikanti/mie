//! Walking skeleton: one domain path serves live analysis and historical
//! replay (ADR-019), in the canonical event order (ADR-028). The in-memory
//! providers stand in for the Binance and raw-Parquet adapters.

use mie_app::{HashingProvider, ReplayService, drive, drive_tolerant};
use mie_domain::event::{
    Aggressor, BookSnapshot, BookUpdate, FeedGap, FundingSettlement, GapReason, Kline, Level,
    Liquidation, MarkPrice, MarketEvent, OpenInterest, Stream, Trade,
};
use mie_domain::event_hash::EventStreamHasher;
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::num::{Price, Qty, Rate};
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_ports::inbound::{ReplayMarket, UseCaseError};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, VecDeque};

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

/// A fixed dataset version for the in-memory providers.
fn fixture_dataset() -> DatasetVersion {
    DatasetVersion::from_hex(&"0f".repeat(32)).unwrap()
}

/// Stand-in for the raw-data replay adapter.
struct Recorded(Vec<MarketEvent>);

impl HistoricalDataProvider for Recorded {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        let in_window = self.0.iter().filter(|e| window.contains(e.time()));
        Ok(Replay {
            stream: Feed::of(in_window.cloned().collect()),
            dataset: fixture_dataset(),
        })
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
    // Both paths record the feature-set version they computed with (ADR-029).
    assert_eq!(report.state.feature_set, catalog::current_set().version());
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
        FeatureValue::Ready(Price::from_units(6_354_190_000_000))
    );
}

#[test]
fn domain_rejects_an_out_of_order_feed() {
    let first = trade(2_000, 1, 1, Aggressor::Buy);
    let late = trade(1_000, 2, 2, Aggressor::Sell);
    let mut feed = Feed::of(vec![first.clone(), late.clone()]);
    let mut engine = MarketStateEngine::new();

    let err = drive(&mut feed, &mut engine).unwrap_err();

    assert_eq!(
        err,
        UseCaseError::Domain(StateError::OutOfOrder {
            last: first.canonical_key(),
            event: late.canonical_key(),
        })
    );
    assert_eq!(engine.state().trade_count, 1);
}

#[test]
fn a_same_millisecond_rank_inversion_stops_the_drive() {
    let mark = mark_price(1_000, 6_354_150_000_000);
    let trade_after_mark = trade(1_000, 2, 6_354_210_000_000, Aggressor::Buy);
    let mut feed = Feed::of(vec![
        trade(1_000, 1, 6_354_200_000_000, Aggressor::Buy),
        mark.clone(),
        trade_after_mark.clone(),
        trade(1_001, 3, 6_354_220_000_000, Aggressor::Buy),
    ]);
    let mut engine = MarketStateEngine::new();

    let err = drive(&mut feed, &mut engine).unwrap_err();

    assert_eq!(
        err,
        UseCaseError::Domain(StateError::OutOfOrder {
            last: mark.canonical_key(),
            event: trade_after_mark.canonical_key(),
        })
    );
    assert_eq!(engine.state().trade_count, 1);
    assert_eq!(engine.state().as_of, Some(EventTime::from_millis(1_000)));
}

/// Wraps a provider and records every event it delivers.
struct Recording<P> {
    inner: P,
    delivered: Vec<MarketEvent>,
}

impl<P> Recording<P> {
    fn new(inner: P) -> Self {
        Self {
            inner,
            delivered: Vec::new(),
        }
    }
}

impl<P: MarketDataProvider> MarketDataProvider for Recording<P> {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let event = self.inner.next_event()?;
        if let Some(event) = &event {
            self.delivered.push(event.clone());
        }
        Ok(event)
    }
}

/// Stand-in for a live adapter: events arrive in a jittered order across
/// streams and the adapter releases them in canonical order. The whole tape
/// fits in its hold-back here, so nothing turns into a late-event gap.
fn live_feed(mut arrivals: Vec<MarketEvent>) -> Feed {
    arrivals.sort();
    Feed::of(arrivals)
}

/// Stand-in for the raw-data replay adapter: one recording per stream,
/// k-way merged through a heap.
struct PerStream(Vec<Vec<MarketEvent>>);

/// A lazy k-way merge of per-stream recordings.
struct Merge {
    recordings: Vec<VecDeque<MarketEvent>>,
    heap: BinaryHeap<Reverse<(MarketEvent, usize)>>,
}

impl Merge {
    fn new(recordings: Vec<Vec<MarketEvent>>) -> Self {
        let mut merge = Self {
            recordings: recordings.into_iter().map(VecDeque::from).collect(),
            heap: BinaryHeap::new(),
        };
        for index in 0..merge.recordings.len() {
            merge.refill(index);
        }
        merge
    }

    fn refill(&mut self, index: usize) {
        if let Some(event) = self.recordings[index].pop_front() {
            self.heap.push(Reverse((event, index)));
        }
    }
}

impl MarketDataProvider for Merge {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        let Some(Reverse((event, index))) = self.heap.pop() else {
            return Ok(None);
        };
        self.refill(index);
        Ok(Some(event))
    }
}

impl HistoricalDataProvider for PerStream {
    type Stream = Merge;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Merge>, ProviderError> {
        let in_window = self
            .0
            .iter()
            .map(|recording| {
                let mut events: Vec<MarketEvent> = recording
                    .iter()
                    .filter(|e| window.contains(e.time()))
                    .cloned()
                    .collect();
                // Recordings keep arrival order; each stream is sorted before
                // the merge (the two liquidations below arrive out of order).
                events.sort();
                events
            })
            .collect();
        Ok(Replay {
            stream: Merge::new(in_window),
            dataset: fixture_dataset(),
        })
    }
}

fn millis(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

fn level(price_units: i64, qty_units: i64) -> Level {
    Level {
        price: Price::from_units(price_units),
        qty: Qty::from_units(qty_units),
    }
}

fn mark_price(at: i64, mark_units: i64) -> MarketEvent {
    MarketEvent::MarkPrice(MarkPrice {
        time: millis(at),
        mark_price: Price::from_units(mark_units),
        index_price: Price::from_units(6_354_000_000_000),
        funding_rate: Rate::from_units(10_000),
        next_funding_time: millis(28_800_000),
    })
}

/// Per-stream recordings with same-millisecond collisions across every kind.
fn recordings() -> Vec<Vec<MarketEvent>> {
    let trades = vec![
        trade(1_000, 1, 6_354_200_000_000, Aggressor::Buy),
        trade(1_000, 2, 6_354_210_000_000, Aggressor::Buy),
        trade(1_250, 3, 6_354_190_000_000, Aggressor::Sell),
        MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: millis(1_251),
            end: millis(1_500),
            reason: GapReason::Disconnected,
        }),
        trade(1_500, 7, 6_354_180_000_000, Aggressor::Sell),
        trade(2_000, 8, 6_354_170_000_000, Aggressor::Sell),
    ];
    let book = vec![
        MarketEvent::BookSnapshot(BookSnapshot {
            time: millis(1_000),
            last_update_id: 100,
            bids: vec![level(6_354_200_000_000, 300_000_000)],
            asks: vec![level(6_354_210_000_000, 120_000_000)],
        }),
        MarketEvent::BookUpdate(BookUpdate {
            time: millis(1_000),
            first_update_id: 95,
            last_update_id: 104,
            prev_update_id: 94,
            bids: vec![],
            asks: vec![level(6_354_210_000_000, 20_000_000)],
        }),
        MarketEvent::BookUpdate(BookUpdate {
            time: millis(1_250),
            first_update_id: 105,
            last_update_id: 109,
            prev_update_id: 104,
            bids: vec![level(6_354_190_000_000, 0)],
            asks: vec![],
        }),
        MarketEvent::BookUpdate(BookUpdate {
            time: millis(2_000),
            first_update_id: 110,
            last_update_id: 112,
            prev_update_id: 109,
            bids: vec![level(6_354_170_000_000, 10_000_000)],
            asks: vec![],
        }),
    ];
    let liquidations = vec![
        MarketEvent::Liquidation(Liquidation {
            time: millis(1_250),
            aggressor: Aggressor::Sell,
            price: Price::from_units(6_354_000_000_000),
            avg_price: Price::from_units(6_354_190_000_000),
            filled_qty: Qty::from_units(2_000_000),
        }),
        MarketEvent::Liquidation(Liquidation {
            time: millis(1_250),
            aggressor: Aggressor::Sell,
            price: Price::from_units(6_354_000_000_000),
            avg_price: Price::from_units(6_354_190_000_000),
            filled_qty: Qty::from_units(1_000_000),
        }),
    ];
    let marks = vec![
        mark_price(1_000, 6_354_150_000_000),
        mark_price(2_000, 6_354_120_000_000),
    ];
    let funding = vec![MarketEvent::FundingSettlement(FundingSettlement {
        time: millis(2_000),
        rate: Rate::from_units(-2_233),
    })];
    let open_interest = vec![MarketEvent::OpenInterest(OpenInterest {
        time: millis(1_500),
        open_interest: Qty::from_units(8_000_000_000_000),
        resolution_ms: 300_000,
    })];
    let klines = vec![MarketEvent::Kline(Kline {
        open_time: millis(1_000),
        close_time: millis(2_000),
        open: Price::from_units(6_354_200_000_000),
        high: Price::from_units(6_354_210_000_000),
        low: Price::from_units(6_354_170_000_000),
        close: Price::from_units(6_354_170_000_000),
        volume: Qty::from_units(5_000_000),
        taker_buy_volume: Qty::from_units(2_000_000),
        trade_count: 5,
    })];
    vec![
        trades,
        book,
        liquidations,
        marks,
        funding,
        open_interest,
        klines,
    ]
}

/// Deterministic shuffle (64-bit LCG, Knuth's MMIX constants) standing in
/// for network jitter.
fn jitter(events: &mut [MarketEvent], seed: u64) {
    let mut state = seed;
    for i in (1..events.len()).rev() {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        let j = usize::try_from((state >> 33) % u64::try_from(i + 1).unwrap()).unwrap();
        events.swap(i, j);
    }
}

#[test]
fn multi_stream_live_and_replay_deliver_one_sequence_and_state() {
    let recordings = recordings();
    let total: usize = recordings.iter().map(Vec::len).sum();
    let all = window(0, 10_000);

    let mut replay = Recording::new(PerStream(recordings.clone()).replay(all).unwrap().stream);
    let mut replay_engine = MarketStateEngine::new();
    let replayed = drive(&mut replay, &mut replay_engine).unwrap();
    assert_eq!(replayed, u64::try_from(total).unwrap());
    assert!(
        replay.delivered.windows(2).all(|w| w[0] < w[1]),
        "replay is strictly increasing"
    );
    // The gap precedes the resumed trade, and the kline closes the tape.
    let gap_at = replay
        .delivered
        .iter()
        .position(|e| matches!(e, MarketEvent::FeedGap(_)))
        .unwrap();
    assert_eq!(replay.delivered[gap_at + 1].time(), millis(1_500));
    assert!(matches!(
        replay.delivered.last(),
        Some(MarketEvent::Kline(_))
    ));

    for seed in 0..64 {
        let mut arrivals: Vec<MarketEvent> = recordings.iter().flatten().cloned().collect();
        jitter(&mut arrivals, seed);
        let mut live = Recording::new(live_feed(arrivals));
        let mut live_engine = MarketStateEngine::new();
        drive(&mut live, &mut live_engine).unwrap();

        assert_eq!(live.delivered, replay.delivered, "seed {seed}");
        assert_eq!(live_engine.state(), replay_engine.state(), "seed {seed}");
    }

    let report = ReplayService::new(PerStream(recordings))
        .replay(all)
        .unwrap();
    assert_eq!(&report.state, replay_engine.state());
    assert_eq!(report.state.trade_count, 5);
    assert_eq!(report.state.as_of, Some(millis(2_000)));
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

#[test]
fn replay_reports_the_dataset_and_the_hash_of_what_it_delivered() {
    let report = ReplayService::new(Recorded(tape()))
        .replay(window(0, 10_000))
        .unwrap();
    let mut hasher = EventStreamHasher::new();
    for event in &tape() {
        hasher.push(event);
    }
    assert_eq!(report.dataset, fixture_dataset());
    assert_eq!(report.stream_hash, hasher.finish());
    assert_eq!(report.stream_hash.events, report.events);
    assert_eq!(report.domain_rejections, 0);

    // The same window twice: the same hash.
    let again = ReplayService::new(Recorded(tape()))
        .replay(window(0, 10_000))
        .unwrap();
    assert_eq!(again, report);
}

#[test]
fn replay_counts_a_domain_rejection_and_continues_like_ingest() {
    // A trade that repeats an id is rejected; the next one still applies.
    let mut events = tape();
    events.insert(2, trade(1_100, 2, 6_354_000_000_000, Aggressor::Buy));
    let report = ReplayService::new(Recorded(events.clone()))
        .replay(window(0, 10_000))
        .unwrap();
    assert_eq!(report.events, 5);
    assert_eq!(report.domain_rejections, 1);
    assert_eq!(report.state.trade_count, 4);

    let mut rejected = Vec::new();
    let mut engine = MarketStateEngine::new();
    let mut hashing = HashingProvider::new(Feed::of(events));
    let delivered = drive_tolerant(&mut hashing, &mut engine, |e| rejected.push(*e)).unwrap();
    assert_eq!(delivered, 5);
    assert_eq!(hashing.hash(), report.stream_hash);
    assert!(matches!(
        rejected[..],
        [StateError::IdRegression { .. } | StateError::Duplicate { .. }]
    ));
    assert_eq!(engine.state(), &report.state);
}

#[test]
fn a_provider_failure_stops_the_tolerant_drive() {
    let failure = ProviderError::Source("connection reset".into());
    let mut feed = Feed(VecDeque::from([
        Ok(trade(1_000, 1, 1, Aggressor::Buy)),
        Err(failure.clone()),
        Ok(trade(2_000, 2, 2, Aggressor::Buy)),
    ]));
    let mut engine = MarketStateEngine::new();
    let err = drive_tolerant(&mut feed, &mut engine, |_| {}).unwrap_err();
    assert_eq!(err, failure);
    assert_eq!(engine.state().trade_count, 1);
}
