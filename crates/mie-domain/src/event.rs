//! Domain-level market observations.
//!
//! This is what the `MarketDataProvider` port hands to the core — never
//! exchange SDK or wire types (Data Plane brief, design rule 5). One variant
//! per raw stream the Data Plane preserves (brief §7: trades, depth/order-book
//! events, klines, open interest, funding, liquidations), plus explicit
//! feed-gap markers: continuity loss is data, not silence (ADR-026).
//!
//! Every event has one position in the canonical order (ADR-028,
//! [`crate::order`]), computed from exchange fields alone, so live capture and
//! archive import deliver the same sequence. All values are exact fixed point
//! (ADR-027).

use crate::num::{Price, Qty, Rate};
use crate::order::EventKind;
use crate::time::EventTime;

/// One market observation, in the order the core consumes it.
///
/// Ordered by [`Ord`] (ADR-028): canonical key first, then the payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MarketEvent {
    /// Continuity loss on one stream.
    FeedGap(FeedGap),
    /// A full order-book reset.
    BookSnapshot(BookSnapshot),
    /// An executed trade.
    Trade(Trade),
    /// A forced liquidation order.
    Liquidation(Liquidation),
    /// An incremental order-book change.
    BookUpdate(BookUpdate),
    /// Mark and index price with the indicative funding rate.
    MarkPrice(MarkPrice),
    /// A settled funding rate.
    FundingSettlement(FundingSettlement),
    /// An open-interest observation.
    OpenInterest(OpenInterest),
    /// A closed candlestick.
    Kline(Kline),
}

impl MarketEvent {
    /// The event time the core treats as "now" for this observation: the
    /// ordering time of ADR-028 (a kline's close time, a gap's end).
    pub fn time(&self) -> EventTime {
        match self {
            Self::FeedGap(gap) => gap.end,
            Self::BookSnapshot(snapshot) => snapshot.time,
            Self::Trade(trade) => trade.time,
            Self::Liquidation(liquidation) => liquidation.time,
            Self::BookUpdate(update) => update.time,
            Self::MarkPrice(mark) => mark.time,
            Self::FundingSettlement(settlement) => settlement.time,
            Self::OpenInterest(oi) => oi.time,
            Self::Kline(kline) => kline.close_time,
        }
    }

    /// The kind of observation, which ranks same-millisecond events
    /// (ADR-028).
    pub fn kind(&self) -> EventKind {
        match self {
            Self::FeedGap(_) => EventKind::FeedGap,
            Self::BookSnapshot(_) => EventKind::BookSnapshot,
            Self::Trade(_) => EventKind::Trade,
            Self::Liquidation(_) => EventKind::Liquidation,
            Self::BookUpdate(_) => EventKind::BookUpdate,
            Self::MarkPrice(_) => EventKind::MarkPrice,
            Self::FundingSettlement(_) => EventKind::FundingSettlement,
            Self::OpenInterest(_) => EventKind::OpenInterest,
            Self::Kline(_) => EventKind::Kline,
        }
    }

    /// The domain series this observation belongs to; for a gap, the series
    /// that lost continuity.
    pub fn stream(&self) -> Stream {
        match self {
            Self::FeedGap(gap) => gap.stream,
            Self::BookSnapshot(_) | Self::BookUpdate(_) => Stream::OrderBook,
            Self::Trade(_) => Stream::Trades,
            Self::Liquidation(_) => Stream::Liquidations,
            Self::MarkPrice(_) => Stream::MarkPrice,
            Self::FundingSettlement(_) => Stream::Funding,
            Self::OpenInterest(_) => Stream::OpenInterest,
            Self::Kline(_) => Stream::Klines,
        }
    }
}

/// An executed trade with its aggressor side (Data Plane order-flow
/// contract; ADR-021, ADR-022).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Trade {
    /// Exchange trade time.
    pub time: EventTime,
    /// Exchange-assigned trade id (unique per instrument).
    pub trade_id: u64,
    /// Execution price.
    pub price: Price,
    /// Executed quantity.
    pub qty: Qty,
    /// The side that crossed the spread.
    pub aggressor: Aggressor,
}

/// Which side initiated a trade by crossing the spread.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Aggressor {
    /// An aggressive buy lifted the ask.
    Buy,
    /// An aggressive sell hit the bid.
    Sell,
}

/// One price level of an order-book change or snapshot (Data Plane order-flow
/// contract: bid/ask liquidity changes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Level {
    /// Level price.
    pub price: Price,
    /// Resting quantity at the level after the change; zero removes the
    /// level.
    pub qty: Qty,
}

/// An incremental order-book change (Data Plane order-flow contract: depth
/// events; ADR-021, ADR-022).
///
/// The update ids chain the diffs of one book: `prev_update_id` is the
/// `last_update_id` of the preceding diff. Validating that chain belongs to
/// the book model, not to this type.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BookUpdate {
    /// Exchange transaction time of the diff.
    pub time: EventTime,
    /// First update id covered by this diff.
    pub first_update_id: u64,
    /// Last update id covered by this diff.
    pub last_update_id: u64,
    /// Last update id of the preceding diff.
    pub prev_update_id: u64,
    /// Changed bid levels, in source order.
    pub bids: Vec<Level>,
    /// Changed ask levels, in source order.
    pub asks: Vec<Level>,
}

/// A full order-book reset (Data Plane order-flow contract: depth events;
/// ADR-021, ADR-022).
///
/// Replaces the book. Levels below the snapshot depth are unknown, not empty:
/// the book is trusted only to that depth.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct BookSnapshot {
    /// Exchange transaction time of the snapshot.
    pub time: EventTime,
    /// Last update id included in the snapshot.
    pub last_update_id: u64,
    /// Bid levels, in source order.
    pub bids: Vec<Level>,
    /// Ask levels, in source order.
    pub asks: Vec<Level>,
}

/// A forced liquidation order (brief §7, raw liquidations).
///
/// A lower bound on liquidation activity: the exchange source is throttled
/// and does not publish every liquidation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Liquidation {
    /// Exchange trade time of the liquidation order.
    pub time: EventTime,
    /// Side of the liquidation order, which crosses the spread: `Sell`
    /// closes a long, `Buy` closes a short.
    pub aggressor: Aggressor,
    /// Order price.
    pub price: Price,
    /// Average fill price.
    pub avg_price: Price,
    /// Filled quantity.
    pub filled_qty: Qty,
}

/// Mark and index price with the indicative funding rate (brief §7 funding,
/// §8 Market State).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MarkPrice {
    /// Exchange event time.
    pub time: EventTime,
    /// Mark price.
    pub mark_price: Price,
    /// Index price.
    pub index_price: Price,
    /// Indicative funding rate for the next settlement.
    pub funding_rate: Rate,
    /// When the next funding settles.
    pub next_funding_time: EventTime,
}

/// A settled funding rate (brief §7 funding).
///
/// Separate from [`MarkPrice`] because the archive funding history contains
/// settlements only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FundingSettlement {
    /// Funding time.
    pub time: EventTime,
    /// The settled rate.
    pub rate: Rate,
}

/// An open-interest observation (brief §8: OI, ΔOI, OI velocity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct OpenInterest {
    /// Exchange publication time — never a validity time earlier than
    /// publication, so features see the value only once it was known.
    pub time: EventTime,
    /// Open interest in contracts of the base currency.
    pub open_interest: Qty,
    /// Sampling resolution of the source in milliseconds, exposed per value
    /// because live and archive sources differ.
    pub resolution_ms: u32,
}

/// A closed candlestick (brief §7 klines).
///
/// Closed bars only, for cross-checks. No feature may derive OHLCV from
/// klines: bars come from trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Kline {
    /// Open time of the bar.
    pub open_time: EventTime,
    /// Close time of the bar; the bar is ordered at this time.
    pub close_time: EventTime,
    /// Open price.
    pub open: Price,
    /// High price.
    pub high: Price,
    /// Low price.
    pub low: Price,
    /// Close price.
    pub close: Price,
    /// Traded base volume.
    pub volume: Qty,
    /// Base volume bought by aggressors.
    pub taker_buy_volume: Qty,
    /// Number of trades in the bar.
    pub trade_count: u64,
}

/// Continuity loss on one stream (ADR-026 consequences, ADR-028).
///
/// The stream may be incomplete over the closed interval `[start, end]`. The
/// gap is ordered at `end` ahead of every other kind, so it precedes the
/// stream's first resumed event. Well-formed gaps have `start <= end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeedGap {
    /// The series that lost continuity.
    pub stream: Stream,
    /// First instant that may be missing data.
    pub start: EventTime,
    /// Last instant that may be missing data.
    pub end: EventTime,
    /// Why continuity was lost.
    pub reason: GapReason,
}

/// A domain series, independent of exchange stream names. Feature inputs are
/// identified by it.
///
/// The explicit discriminants are the ordinals ADR-028 uses to order gaps of
/// different streams at the same millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Stream {
    /// Executed trades.
    Trades = 0,
    /// Order-book snapshots and updates.
    OrderBook = 1,
    /// Liquidation orders.
    Liquidations = 2,
    /// Mark price and indicative funding.
    MarkPrice = 3,
    /// Funding settlements.
    Funding = 4,
    /// Open interest.
    OpenInterest = 5,
    /// Closed klines.
    Klines = 6,
}

/// Why a stream lost continuity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum GapReason {
    /// The connection dropped.
    Disconnected,
    /// The exchange sequence (trade ids, update-id chain) broke.
    SequenceBreak,
    /// An event arrived after its slot in the canonical order had been
    /// released; it is replaced by this gap, never delivered into the past.
    LateEvent,
    /// The source has no data for the interval (for example an archive
    /// hole).
    MissingData,
}

/// One sample event of every kind, for tests across the crate.
#[cfg(test)]
pub(crate) mod samples {
    use super::*;

    /// Milliseconds as an [`EventTime`].
    pub(crate) fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    pub(crate) fn trade(millis: i64, trade_id: u64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: Price::from_units(6_354_210_000_000),
            qty: Qty::from_units(1_500_000),
            aggressor: Aggressor::Buy,
        })
    }

    pub(crate) fn gap(stream: Stream, start: i64, end: i64, reason: GapReason) -> MarketEvent {
        MarketEvent::FeedGap(FeedGap {
            stream,
            start: t(start),
            end: t(end),
            reason,
        })
    }

    pub(crate) fn snapshot(millis: i64, last_update_id: u64) -> MarketEvent {
        MarketEvent::BookSnapshot(BookSnapshot {
            time: t(millis),
            last_update_id,
            bids: vec![level(6_354_200_000_000, 300_000_000)],
            asks: vec![level(6_354_210_000_000, 120_000_000)],
        })
    }

    pub(crate) fn update(millis: i64, first: u64, last: u64, prev: u64) -> MarketEvent {
        MarketEvent::BookUpdate(BookUpdate {
            time: t(millis),
            first_update_id: first,
            last_update_id: last,
            prev_update_id: prev,
            bids: vec![level(6_354_200_000_000, 0)],
            asks: vec![level(6_354_220_000_000, 50_000_000)],
        })
    }

    pub(crate) fn liquidation(millis: i64, filled_units: i64) -> MarketEvent {
        MarketEvent::Liquidation(Liquidation {
            time: t(millis),
            aggressor: Aggressor::Sell,
            price: Price::from_units(6_350_000_000_000),
            avg_price: Price::from_units(6_350_100_000_000),
            filled_qty: Qty::from_units(filled_units),
        })
    }

    pub(crate) fn mark(millis: i64, mark_units: i64) -> MarketEvent {
        MarketEvent::MarkPrice(MarkPrice {
            time: t(millis),
            mark_price: Price::from_units(mark_units),
            index_price: Price::from_units(6_354_000_000_000),
            funding_rate: Rate::from_units(10_000),
            next_funding_time: t(28_800_000),
        })
    }

    pub(crate) fn settlement(millis: i64, rate_units: i64) -> MarketEvent {
        MarketEvent::FundingSettlement(FundingSettlement {
            time: t(millis),
            rate: Rate::from_units(rate_units),
        })
    }

    pub(crate) fn open_interest(millis: i64, resolution_ms: u32) -> MarketEvent {
        MarketEvent::OpenInterest(OpenInterest {
            time: t(millis),
            open_interest: Qty::from_units(8_000_000_000_000),
            resolution_ms,
        })
    }

    pub(crate) fn kline(open: i64, close: i64) -> MarketEvent {
        MarketEvent::Kline(Kline {
            open_time: t(open),
            close_time: t(close),
            open: Price::from_units(6_354_000_000_000),
            high: Price::from_units(6_356_000_000_000),
            low: Price::from_units(6_353_000_000_000),
            close: Price::from_units(6_355_000_000_000),
            volume: Qty::from_units(4_200_000_000),
            taker_buy_volume: Qty::from_units(2_100_000_000),
            trade_count: 1_234,
        })
    }

    fn level(price_units: i64, qty_units: i64) -> Level {
        Level {
            price: Price::from_units(price_units),
            qty: Qty::from_units(qty_units),
        }
    }

    /// A sample of `kind` at `millis`. The exhaustive match makes a new
    /// [`EventKind`] fail to compile until it has a sample.
    pub(crate) fn of_kind(kind: EventKind, millis: i64) -> MarketEvent {
        match kind {
            EventKind::FeedGap => gap(
                Stream::Trades,
                millis - 500,
                millis,
                GapReason::Disconnected,
            ),
            EventKind::BookSnapshot => snapshot(millis, 100),
            EventKind::Trade => trade(millis, 7),
            EventKind::Liquidation => liquidation(millis, 10_000_000),
            EventKind::BookUpdate => update(millis, 101, 105, 100),
            EventKind::MarkPrice => mark(millis, 6_354_150_000_000),
            EventKind::FundingSettlement => settlement(millis, 10_000),
            EventKind::OpenInterest => open_interest(millis, 300_000),
            EventKind::Kline => kline(millis - 59_999, millis),
        }
    }

    /// Every kind in the rank order ADR-028 fixes (D2).
    pub(crate) const RANKED_KINDS: [EventKind; 9] = [
        EventKind::FeedGap,
        EventKind::BookSnapshot,
        EventKind::Trade,
        EventKind::Liquidation,
        EventKind::BookUpdate,
        EventKind::MarkPrice,
        EventKind::FundingSettlement,
        EventKind::OpenInterest,
        EventKind::Kline,
    ];

    /// One event of every kind at `millis`, in canonical order.
    pub(crate) fn one_of_each(millis: i64) -> Vec<MarketEvent> {
        RANKED_KINDS
            .iter()
            .map(|&kind| of_kind(kind, millis))
            .collect()
    }
}
