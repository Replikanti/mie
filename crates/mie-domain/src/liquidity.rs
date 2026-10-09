//! Order-book liquidity state: depth, imbalance, liquidity flow and
//! concentration (brief §8, Market State & Regime brief; ADR-043, proposed).
//!
//! The engine owns the L2 book of ADR-038 as `book.l2@1` ([`BookState::l2`],
//! decision 1) and derives three feature families from it:
//!
//! - **Depth and imbalance** `book.depth@1` ([`BookDepth`], decision 4): best
//!   bid and ask, and per band the resting quantity and level count of each
//!   side. Imbalance `(bid − ask) / (bid + ask)` is derived on demand.
//! - **Liquidity flow** `book.liquidity.window.<5m|15m|1h>@1`
//!   ([`LiquidityWindow`], decisions 5 and 6): liquidity added, cancelled and
//!   filled per side and band over the last 5, 15 or 60 closed 1m bars, on
//!   the clock of the order-flow windows (ADR-035).
//! - **Concentration** `book.clusters@1` ([`BookClusters`], decision 8): per
//!   side within 5 bps, the largest levels with the side's median level
//!   quantity. No threshold: #23 owns what counts as a cluster.
//!
//! **Bands** (decision 2) are cumulative, 1, 2 and 5 bps of the mid
//! `(best bid + best ask) / 2`, inclusive, compared exactly in doubled `i128`
//! units: price `p` is in band `e` iff `|a + b − 2p| · SCALE ≤ e · (a + b)`,
//! on either side. For the resting levels of an uncrossed book that is the
//! distance away from the spread; a level an update places beyond the
//! pre-update mid counts by its true distance too.
//!
//! **Validity** (decision 3): everything is unavailable while the book is not
//! ready, one-sided or crossed. A band whose far edge lies beyond the
//! trusted window (ADR-038 D6) on a side is `OutOfRange`; the edge exactly at
//! the window bound is still in range.
//!
//! **Attribution** (decision 5). A depth diff carries absolute quantities, so
//! a decrease alone cannot tell a fill from a cancel. For every update
//! applied to a ready book, with the mid before the update:
//!
//! 1. the last value per (side, price) in the update wins;
//! 2. accounted prices lie inside the trusted window and within 5 bps; each
//!    counts in every band that contains it;
//! 3. `Δ = final − old` per accounted price;
//! 4. taker trades are pending fills at their price: a `Buy` aggressor at the
//!    ask, a `Sell` aggressor at the bid, each fresh until the next update
//!    and carried for one more;
//! 5. a decrease is `filled` up to the pending fills there, carried ones
//!    first, and `cancelled` beyond; an increase is `added`;
//! 6. after the update, a carried fill still unmatched at an accounted price
//!    is booked as `added` and `filled` and counted in `inferred` — the
//!    level was replenished within one batch; elsewhere it is dropped;
//! 7. a snapshot, an order-book gap or an invalidation books no flow and
//!    drops every pending fill (ADR-038 D6).
//!
//! `added` and `cancelled` are lower bounds: an add and a cancel that offset
//! each other at one price within a batch are invisible. Liquidations are
//! not fills: their executions are in the trade stream already.
//!
//! Sums are exact (ADR-027) and checked; an overflow in a minute's flow or
//! a pending fill rejects the event, an overflow in a window, depth or
//! cluster sum makes that value `OutOfRange`. The book changes only on
//! commit, after nothing can fail. Floats are derived on demand and never
//! stored, so the state stays `Eq`
//! (decision 9).
//!
//! Nothing here names or encodes a direction or a signal (ADR-012, ADR-023,
//! ADR-024).

use crate::bars::{Bar, Timeframe};
use crate::book::{BookStep, OrderBook, Side};
use crate::event::{Aggressor, BookUpdate, FeedGap, Level, MarketEvent, Stream, Trade};
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::fingerprint::Fingerprinter;
use crate::num::{Price, Qty, Rate, SCALE};
use crate::state_hash::StateEncode;
use crate::time::EventTime;
use std::fmt;

/// The number of depth bands, [`catalog::BOOK_BANDS`].
pub const BANDS: usize = 3;

/// The levels per side `book.clusters@1` names: parameter `top_k` (ADR-043,
/// decision 8).
pub const TOP_K: usize = 5;

/// The book updates a taker trade stays pending after the first one:
/// `book.liquidity.window.*@1` parameter `carry_updates` (ADR-043,
/// decision 5).
pub const CARRY_UPDATES: i64 = 1;

/// The outer band: accounted prices and clusters lie within it.
const OUTER: Rate = catalog::BOOK_BANDS[BANDS - 1];

/// Flow minutes kept, keyed by minute: 60 for the longest window plus one,
/// so a minute is overwritten only by one at least 61 minutes newer, which
/// no window that can still be computed reaches back to.
const RING: usize = 61;

/// The length of a minute in milliseconds.
const MINUTE_MS: i64 = 60_000;

/// Rate units per basis point.
const BPS: i64 = SCALE / 10_000;

const ZERO: Qty = Qty::from_units(0);

/// `a − b`, for `b ≤ a` by construction; zero otherwise.
fn less(a: Qty, b: Qty) -> Qty {
    a.checked_sub(b).filter(|d| d.units() >= 0).unwrap_or(ZERO)
}

/// Writes a band as `<n>bps`, or as its rate when not a whole number of
/// basis points.
struct BandLabel(Rate);

impl fmt::Display for BandLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let units = self.0.units();
        if units % BPS == 0 {
            write!(f, "{}bps", units / BPS)
        } else {
            write!(f, "{}", self.0)
        }
    }
}

/// Writes `price@qty`.
fn write_level(f: &mut fmt::Formatter<'_>, level: Level) -> fmt::Result {
    write!(f, "{}@{}", level.price, level.qty)
}

/// Writes a feature value: the value when ready, its state otherwise.
fn write_value<T: fmt::Display>(
    f: &mut fmt::Formatter<'_>,
    value: &FeatureValue<T>,
) -> fmt::Result {
    match value {
        FeatureValue::Ready(value) => write!(f, "{value}"),
        FeatureValue::WarmingUp { observed, required } => {
            write!(f, "warming {observed}/{required}")
        }
        FeatureValue::Unavailable { reason } => write!(f, "unavailable {reason:?}"),
    }
}

/// Encodes a level as its price and quantity.
fn encode_level(level: &Level, f: &mut Fingerprinter) {
    let Level { price, qty } = level;
    price.encode(f);
    qty.encode(f);
}

/// `numerator / denominator` of two quantities in `f64`; `None` for a zero
/// denominator.
fn ratio(numerator: i128, denominator: i128) -> Option<f64> {
    (denominator != 0).then(|| numerator as f64 / denominator as f64)
}

/// The mid and the trusted window of a two-sided, uncrossed book: what bands
/// and accounted prices are measured against (ADR-043, decisions 2 and 3).
#[derive(Debug, Clone, Copy)]
struct Frame {
    /// Best bid plus best ask in units: twice the mid.
    sum: i128,
    lowest_bid: Option<Price>,
    highest_ask: Option<Price>,
}

impl Frame {
    /// The frame of `book`; `None` when it is one-sided, crossed or not
    /// positive.
    fn of(book: &OrderBook) -> Option<Self> {
        let bid = book.best_bid()?.price;
        let ask = book.best_ask()?.price;
        let sum = i128::from(bid.units()) + i128::from(ask.units());
        let window = book.window();
        (bid < ask && sum > 0).then_some(Self {
            sum,
            lowest_bid: window.lowest_bid,
            highest_ask: window.highest_ask,
        })
    }

    /// Twice the distance of `price` from mid, in units, positive away from
    /// the spread on `side`.
    fn distance(self, side: Side, price: Price) -> i128 {
        let twice = 2 * i128::from(price.units());
        match side {
            Side::Bid => self.sum - twice,
            Side::Ask => twice - self.sum,
        }
    }

    /// Whether `price` on `side` lies within `band` of mid, inclusive, by
    /// its absolute distance: a level an update puts on the far side of the
    /// pre-update mid is as far from mid as it is, not inside every band.
    fn within(self, side: Side, price: Price, band: Rate) -> bool {
        self.distance(side, price).abs() * i128::from(SCALE) <= i128::from(band.units()) * self.sum
    }

    /// Whether the far edge of `band` on `side` lies inside the trusted
    /// window; the edge exactly at the window bound does.
    fn edge_known(self, side: Side, band: Rate) -> bool {
        let bound = match side {
            Side::Bid => self.lowest_bid,
            Side::Ask => self.highest_ask,
        };
        bound.is_some_and(|bound| {
            self.distance(side, bound) * i128::from(SCALE) >= i128::from(band.units()) * self.sum
        })
    }

    /// Whether `band` is fully known on both sides.
    fn band_known(self, band: Rate) -> bool {
        self.edge_known(Side::Bid, band) && self.edge_known(Side::Ask, band)
    }

    /// Whether `price` on `side` lies inside the trusted window.
    fn in_window(self, side: Side, price: Price) -> bool {
        match side {
            Side::Bid => self.lowest_bid.is_some_and(|lowest| price >= lowest),
            Side::Ask => self.highest_ask.is_some_and(|highest| price <= highest),
        }
    }

    /// Whether `price` on `side` is accounted: inside the trusted window and
    /// within the outer band (decision 5).
    fn accounted(self, side: Side, price: Price) -> bool {
        self.in_window(side, price) && self.within(side, price, OUTER)
    }
}

/// Resting liquidity of both sides within one band (ADR-043, decision 4).
///
/// `Display` prints `<band> bid=<qty>/<levels> ask=<qty>/<levels>
/// imb=<imbalance>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandDepth {
    /// The band, a share of mid (`1 bps` = 10 000 units).
    pub band: Rate,
    /// Resting bid quantity within the band.
    pub bid_qty: Qty,
    /// Bid levels within the band.
    pub bid_levels: u64,
    /// Resting ask quantity within the band.
    pub ask_qty: Qty,
    /// Ask levels within the band.
    pub ask_levels: u64,
}

impl StateEncode for BandDepth {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            band,
            bid_qty,
            bid_levels,
            ask_qty,
            ask_levels,
        } = self;
        band.encode(f);
        bid_qty.encode(f);
        bid_levels.encode(f);
        ask_qty.encode(f);
        ask_levels.encode(f);
    }
}

impl BandDepth {
    /// The bid/ask imbalance `(bid − ask) / (bid + ask)` in `[-1, 1]`, from
    /// the exact `i128` difference and sum of the units; `None` when both
    /// sides are empty.
    pub fn imbalance(&self) -> Option<f64> {
        let (bid, ask) = (
            i128::from(self.bid_qty.units()),
            i128::from(self.ask_qty.units()),
        );
        ratio(bid - ask, bid + ask)
    }
}

impl fmt::Display for BandDepth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} bid={}/{} ask={}/{} imb=",
            BandLabel(self.band),
            self.bid_qty,
            self.bid_levels,
            self.ask_qty,
            self.ask_levels
        )?;
        match self.imbalance() {
            Some(imbalance) => write!(f, "{imbalance}"),
            None => f.write_str("-"),
        }
    }
}

/// Best levels and banded depth: `book.depth@1` (ADR-043, decision 4).
///
/// `Display` prints the canonical line the golden tests pin: the best bid
/// and ask, each band (or why it is unavailable), then the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookDepth {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The highest bid.
    pub best_bid: Level,
    /// The lowest ask.
    pub best_ask: Level,
    /// Each band of [`catalog::BOOK_BANDS`], innermost first;
    /// `Unavailable(OutOfRange)` when its edge lies beyond the trusted window
    /// on either side or a sum leaves the `Qty` range.
    pub bands: [FeatureValue<BandDepth>; BANDS],
}

impl StateEncode for BookDepth {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            best_bid,
            best_ask,
            bands,
        } = self;
        feature.encode(f);
        encode_level(best_bid, f);
        encode_level(best_ask, f);
        bands.encode(f);
    }
}

impl BookDepth {
    /// The band `band`, if the feature has it.
    pub fn band(&self, band: Rate) -> Option<&FeatureValue<BandDepth>> {
        catalog::BOOK_BANDS
            .iter()
            .position(|candidate| *candidate == band)
            .map(|index| &self.bands[index])
    }
}

impl fmt::Display for BookDepth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("best=")?;
        write_level(f, self.best_bid)?;
        f.write_str("/")?;
        write_level(f, self.best_ask)?;
        for (band, value) in catalog::BOOK_BANDS.iter().zip(&self.bands) {
            f.write_str(" | ")?;
            if value.is_ready() {
                write_value(f, value)?;
            } else {
                write!(f, "{} ", BandLabel(*band))?;
                write_value(f, value)?;
            }
        }
        write!(f, " | {}", self.feature)
    }
}

/// The concentration of one side within 5 bps (ADR-043, decision 8).
///
/// `Display` prints `median=<qty> total=<qty>/<levels> top=` and the top
/// levels as `price@qty`, comma-separated, with the multiple of the median
/// in brackets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SideClusters {
    /// The [`TOP_K`] largest levels by quantity, largest first; on equal
    /// quantity the level nearer mid first. `None` past the side's levels.
    pub top: [Option<Level>; TOP_K],
    /// The lower median level quantity: the `⌊(n − 1) / 2⌋`-th smallest of
    /// the side's `n` levels within the band; zero without levels.
    pub median_qty: Qty,
    /// The side's quantity within the band.
    pub total_qty: Qty,
    /// The side's levels within the band.
    pub levels: u64,
}

impl StateEncode for SideClusters {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            top,
            median_qty,
            total_qty,
            levels,
        } = self;
        f.write_len(top.len());
        for level in top {
            match level {
                None => f.write_u8(0),
                Some(level) => {
                    f.write_u8(1);
                    encode_level(level, f);
                }
            }
        }
        median_qty.encode(f);
        total_qty.encode(f);
        levels.encode(f);
    }
}

impl SideClusters {
    /// The quantity of the `rank`-th top level (0 is the largest) as a
    /// multiple of the median level quantity; `None` without that level or
    /// with a zero median.
    pub fn multiple(&self, rank: usize) -> Option<f64> {
        let level = (*self.top.get(rank)?)?;
        ratio(
            i128::from(level.qty.units()),
            i128::from(self.median_qty.units()),
        )
    }
}

impl fmt::Display for SideClusters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "median={} total={}/{} top=",
            self.median_qty, self.total_qty, self.levels
        )?;
        let mut first = true;
        for (rank, level) in self.top.iter().enumerate() {
            let Some(level) = level else { break };
            if !first {
                f.write_str(",")?;
            }
            first = false;
            write_level(f, *level)?;
            match self.multiple(rank) {
                Some(multiple) => write!(f, "[{multiple}]")?,
                None => f.write_str("[-]")?,
            }
        }
        if first {
            f.write_str("-")?;
        }
        Ok(())
    }
}

/// Liquidity concentration per side: `book.clusters@1` (ADR-043, decision
/// 8). Candidates for the liquidity clusters of #23, without a threshold.
///
/// `Display` prints the canonical line the golden tests pin: the bid side,
/// the ask side, then the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BookClusters {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The bid side; `Unavailable(OutOfRange)` when its 5 bps edge lies
    /// beyond the trusted window or its total leaves the `Qty` range.
    pub bid: FeatureValue<SideClusters>,
    /// The ask side, likewise.
    pub ask: FeatureValue<SideClusters>,
}

impl StateEncode for BookClusters {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { feature, bid, ask } = self;
        feature.encode(f);
        bid.encode(f);
        ask.encode(f);
    }
}

impl fmt::Display for BookClusters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("bid ")?;
        write_value(f, &self.bid)?;
        f.write_str(" | ask ")?;
        write_value(f, &self.ask)?;
        write!(f, " | {}", self.feature)
    }
}

/// Liquidity flow of one side (ADR-043, decision 5).
///
/// `added − filled − cancelled` is the net change of the resting quantity
/// at the accounted prices. `inferred` is the part of `filled` (and of
/// `added`) that no decrease showed: a fill on a level replenished within
/// one batch. `Display` prints `added/cancelled/filled/inferred`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SideFlow {
    /// Resting quantity added; a lower bound.
    pub added: Qty,
    /// Resting quantity removed without a matching taker trade; a lower
    /// bound.
    pub cancelled: Qty,
    /// Resting quantity taken by taker trades.
    pub filled: Qty,
    /// The part of `filled` inferred from trades alone, also counted in
    /// `added`.
    pub inferred: Qty,
}

impl StateEncode for SideFlow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            added,
            cancelled,
            filled,
            inferred,
        } = self;
        added.encode(f);
        cancelled.encode(f);
        filled.encode(f);
        inferred.encode(f);
    }
}

impl SideFlow {
    /// No flow.
    pub const ZERO: Self = Self {
        added: ZERO,
        cancelled: ZERO,
        filled: ZERO,
        inferred: ZERO,
    };

    /// Adds `other` field by field, or `None` on overflow.
    fn add(&mut self, other: &Self) -> Option<()> {
        self.added = self.added.checked_add(other.added)?;
        self.cancelled = self.cancelled.checked_add(other.cancelled)?;
        self.filled = self.filled.checked_add(other.filled)?;
        self.inferred = self.inferred.checked_add(other.inferred)?;
        Some(())
    }
}

impl fmt::Display for SideFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.added, self.cancelled, self.filled, self.inferred
        )
    }
}

/// Liquidity flow of both sides within one band (ADR-043, decisions 5 and
/// 6).
///
/// `Display` prints `<band> bid=<flow> ask=<flow>`, then ` beyond` when
/// flagged.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BandFlow {
    /// The band, a share of mid (`1 bps` = 10 000 units).
    pub band: Rate,
    /// Bid-side flow.
    pub bid: SideFlow,
    /// Ask-side flow.
    pub ask: SideFlow,
    /// An accounted update saw the band's far edge beyond the trusted window
    /// on a side, so part of the band's flow was not observed.
    pub beyond_window: bool,
}

impl StateEncode for BandFlow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            band,
            bid,
            ask,
            beyond_window,
        } = self;
        band.encode(f);
        bid.encode(f);
        ask.encode(f);
        beyond_window.encode(f);
    }
}

impl BandFlow {
    /// No flow in `band`.
    fn empty(band: Rate) -> Self {
        Self {
            band,
            bid: SideFlow::ZERO,
            ask: SideFlow::ZERO,
            beyond_window: false,
        }
    }

    /// The flow of `side`.
    pub fn side(&self, side: Side) -> &SideFlow {
        match side {
            Side::Bid => &self.bid,
            Side::Ask => &self.ask,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut SideFlow {
        match side {
            Side::Bid => &mut self.bid,
            Side::Ask => &mut self.ask,
        }
    }

    /// Adds `other`, or `None` on overflow; the flags are OR-ed.
    fn add(&mut self, other: &Self) -> Option<()> {
        self.bid.add(&other.bid)?;
        self.ask.add(&other.ask)?;
        self.beyond_window |= other.beyond_window;
        Some(())
    }
}

impl fmt::Display for BandFlow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} bid={} ask={}",
            BandLabel(self.band),
            self.bid,
            self.ask
        )?;
        if self.beyond_window {
            f.write_str(" beyond")?;
        }
        Ok(())
    }
}

/// Order-book liquidity flow over the last closed minutes:
/// `book.liquidity.window.<tf>@1` (ADR-043, decisions 5 and 6).
///
/// `Display` prints the canonical line the golden tests pin, ending with the
/// coverage and the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidityWindow {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The window length.
    pub timeframe: Timeframe,
    /// Exclusive end of the window's last minute.
    pub end: EventTime,
    /// The flow per band of [`catalog::BOOK_BANDS`], innermost first.
    pub bands: [BandFlow; BANDS],
    /// The window contains the minute of the first valid book, so flow
    /// before consumption started is missing.
    pub partial_start: bool,
    /// The window overlaps an order-book or trades gap, or a minute with a
    /// book event while the book was unavailable.
    pub feed_gap: bool,
}

impl StateEncode for LiquidityWindow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            timeframe,
            end,
            bands,
            partial_start,
            feed_gap,
        } = self;
        feature.encode(f);
        timeframe.encode(f);
        end.encode(f);
        bands.encode(f);
        partial_start.encode(f);
        feed_gap.encode(f);
    }
}

impl LiquidityWindow {
    /// The flow of band `band`, if the feature has it.
    pub fn band(&self, band: Rate) -> Option<&BandFlow> {
        self.bands.iter().find(|flow| flow.band == band)
    }
}

impl fmt::Display for LiquidityWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let coverage = match (self.partial_start, self.feed_gap) {
            (false, false) => "complete",
            (true, false) => "partial_start",
            (false, true) => "feed_gap",
            (true, true) => "partial_start+feed_gap",
        };
        write!(f, "{} end={}", self.timeframe, self.end)?;
        for band in &self.bands {
            write!(f, " | {band}")?;
        }
        write!(f, " | {coverage} {}", self.feature)
    }
}

/// The liquidity window of every length, in
/// [`catalog::BOOK_LIQUIDITY_WINDOWS`] order: the
/// `book.liquidity.window.*@1` features of the Market State.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidityWindows {
    values: [FeatureValue<LiquidityWindow>; 3],
}

impl StateEncode for LiquidityWindows {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { values } = self;
        values.encode(f);
    }
}

impl Default for LiquidityWindows {
    fn default() -> Self {
        Self::new()
    }
}

impl LiquidityWindows {
    /// Every window warming up, for [`catalog::BOOK_LIQUIDITY_WINDOWS`].
    pub fn new() -> Self {
        Self {
            values: catalog::BOOK_LIQUIDITY_WINDOWS.map(|(timeframe, _)| FeatureValue::WarmingUp {
                observed: 0,
                required: window_minutes(timeframe),
            }),
        }
    }

    /// The window of length `timeframe`, if the set computes it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&FeatureValue<LiquidityWindow>> {
        catalog::BOOK_LIQUIDITY_WINDOWS
            .iter()
            .position(|(candidate, _)| *candidate == timeframe)
            .map(|index| &self.values[index])
    }

    /// Every window, shortest first, with its feature
    /// (`book.liquidity.window.<label>@1`) and value.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (Timeframe, FeatureKey, &FeatureValue<LiquidityWindow>)> {
        catalog::BOOK_LIQUIDITY_WINDOWS
            .iter()
            .zip(&self.values)
            .map(|((timeframe, definition), value)| (*timeframe, definition.key, value))
    }
}

/// The number of 1m bars in a window of length `timeframe`.
fn window_minutes(timeframe: Timeframe) -> u64 {
    // Every window length is a positive whole number of minutes.
    (timeframe.millis() / Timeframe::M1.millis()).unsigned_abs()
}

/// The order-book features of the Market State (ADR-043).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BookState {
    /// `book.l2@1`: the L2 book (ADR-038 D6). Warming up until the first
    /// snapshot; `Unavailable(InputInvalid)` after an order-book gap or an
    /// invalidating update, until the next snapshot.
    pub l2: FeatureValue<OrderBook>,
    /// `book.depth@1`, recomputed after every order-book event.
    pub depth: FeatureValue<BookDepth>,
    /// `book.clusters@1`, recomputed after every order-book event.
    pub clusters: FeatureValue<BookClusters>,
    /// `book.liquidity.window.<5m|15m|1h>@1`, each warming up until its
    /// window of closed minutes since the first valid book is full;
    /// `Unavailable(OutOfRange)` while a flow sum leaves the `Qty` range.
    pub windows: LiquidityWindows,
}

impl StateEncode for BookState {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            l2,
            depth,
            clusters,
            windows,
        } = self;
        l2.encode(f);
        depth.encode(f);
        clusters.encode(f);
        windows.encode(f);
    }
}

impl Default for BookState {
    fn default() -> Self {
        Self::new()
    }
}

impl BookState {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            l2: warming(),
            depth: warming(),
            clusters: warming(),
            windows: LiquidityWindows::new(),
        }
    }
}

/// A value waiting for the first snapshot: `Samples(1)`.
fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

/// The flow of one minute, and whether a gap or an unavailable book
/// overlaps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlowMinute {
    /// Open time of the minute: the slot's key.
    open: EventTime,
    bands: [BandFlow; BANDS],
    feed_gap: bool,
}

impl FlowMinute {
    fn empty(open: EventTime) -> Self {
        Self {
            open,
            bands: catalog::BOOK_BANDS.map(BandFlow::empty),
            feed_gap: false,
        }
    }

    /// Adds `flow` of `side` at `price` to every band that contains the
    /// price, or `None` on overflow.
    fn add(&mut self, frame: Frame, side: Side, price: Price, flow: &SideFlow) -> Option<()> {
        for band in &mut self.bands {
            if frame.within(side, price, band.band) {
                band.side_mut(side).add(flow)?;
            }
        }
        Some(())
    }
}

/// The flow minutes, one slot per minute modulo [`RING`]. A slot holds the
/// minute its key names; a stale or empty slot reads as a minute without
/// flow or gaps.
#[derive(Debug, Clone, PartialEq, Eq)]
struct FlowRing {
    minutes: [Option<FlowMinute>; RING],
}

impl FlowRing {
    fn new() -> Self {
        Self {
            minutes: [None; RING],
        }
    }

    /// The slot of the minute opening at `open`.
    fn slot(open: EventTime) -> usize {
        let minute = open.as_millis().div_euclid(MINUTE_MS);
        // `rem_euclid` of a positive `RING` is in `0..RING`.
        usize::try_from(minute.rem_euclid(RING as i64)).unwrap_or(0)
    }

    /// The minute opening at `open`, if the ring holds it.
    fn get(&self, open: EventTime) -> Option<&FlowMinute> {
        self.minutes[Self::slot(open)]
            .as_ref()
            .filter(|minute| minute.open == open)
    }

    /// The minute opening at `open`, or an empty one.
    fn copy(&self, open: EventTime) -> FlowMinute {
        self.get(open)
            .copied()
            .unwrap_or_else(|| FlowMinute::empty(open))
    }

    /// Stores `minute` in its slot, replacing whatever older minute it held.
    fn put(&mut self, minute: FlowMinute) {
        self.minutes[Self::slot(minute.open)] = Some(minute);
    }

    /// Flags every minute opening in `[first, last]` with `feed_gap`.
    fn flag(&mut self, first: EventTime, last: EventTime) {
        let mut open = first;
        while open <= last {
            let mut minute = self.copy(open);
            minute.feed_gap = true;
            self.put(minute);
            match open.as_millis().checked_add(MINUTE_MS) {
                Some(next) => open = EventTime::from_millis(next),
                None => break,
            }
        }
    }
}

/// Taker volume at one price that no book decrease has matched yet
/// (ADR-043, decision 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingFill {
    /// The book side the trades took: the ask for a `Buy` aggressor.
    side: Side,
    price: Price,
    /// Volume traded since the last book update (age 0).
    fresh: Qty,
    /// Volume carried over one update (age 1).
    carried: Qty,
}

/// What a decrease of `removed` at a pending fill's price takes from it:
/// `(from carried, from fresh)`, carried first.
fn consume(fill: &PendingFill, removed: Qty) -> (Qty, Qty) {
    let carried = removed.min(fill.carried);
    let fresh = less(removed, carried).min(fill.fresh);
    (carried, fresh)
}

/// The final value per price of `side` in `update`: the last one wins.
fn final_qty(update: &BookUpdate, side: Side, price: Price) -> Option<Qty> {
    let levels = match side {
        Side::Bid => &update.bids,
        Side::Ask => &update.asks,
    };
    levels
        .iter()
        .rev()
        .find(|level| level.price == price)
        .map(|level| level.qty)
}

/// What `update` takes from `fill` on `book` (before the update):
/// `(from carried, from fresh)`; nothing unless the fill's price is
/// accounted and the update decreases it.
fn consumption(
    fill: &PendingFill,
    frame: Frame,
    book: &OrderBook,
    update: &BookUpdate,
) -> (Qty, Qty) {
    if !frame.accounted(fill.side, fill.price) {
        return (ZERO, ZERO);
    }
    let Some(new) = final_qty(update, fill.side, fill.price) else {
        return (ZERO, ZERO);
    };
    let old = book.qty_at(fill.side, fill.price).unwrap_or(ZERO);
    if new >= old {
        return (ZERO, ZERO);
    }
    consume(fill, less(old, new))
}

/// How the pending fills change with an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingStep {
    /// Unchanged.
    Keep,
    /// A trade: `fill` goes to `index`, inserted or replacing.
    Put {
        index: usize,
        insert: bool,
        fill: PendingFill,
    },
    /// An update applied to a ready book: matched volume leaves, fresh
    /// volume is carried, carried volume is booked or dropped.
    Age,
    /// A reset or an invalidation drops everything.
    Clear,
}

/// One side of the book within the outer band, in one pass.
#[derive(Debug, Clone, Copy)]
struct SideScan {
    band_qty: [i128; BANDS],
    band_levels: [u64; BANDS],
    total: i128,
    levels: u64,
    top: [Option<Level>; TOP_K],
}

impl SideScan {
    /// Scans `levels`, best first, while they lie within the outer band;
    /// collects their quantities into `quantities`.
    fn of(
        levels: impl Iterator<Item = Level>,
        side: Side,
        frame: Frame,
        quantities: &mut Vec<Qty>,
    ) -> Self {
        let mut scan = Self {
            band_qty: [0; BANDS],
            band_levels: [0; BANDS],
            total: 0,
            levels: 0,
            top: [None; TOP_K],
        };
        quantities.clear();
        for level in levels.take_while(|level| frame.within(side, level.price, OUTER)) {
            let qty = i128::from(level.qty.units());
            for (index, band) in catalog::BOOK_BANDS.iter().enumerate() {
                if frame.within(side, level.price, *band) {
                    scan.band_qty[index] += qty;
                    scan.band_levels[index] += 1;
                }
            }
            scan.total += qty;
            scan.levels += 1;
            quantities.push(level.qty);
            // Largest first; a later (farther) level displaces only a
            // strictly smaller one.
            if let Some(rank) = scan
                .top
                .iter()
                .position(|slot| slot.is_none_or(|top| level.qty > top.qty))
            {
                scan.top[rank..].rotate_right(1);
                scan.top[rank] = Some(level);
            }
        }
        scan
    }
}

/// The lower median of `quantities`, reordering them; zero when empty.
fn lower_median(quantities: &mut [Qty]) -> Qty {
    if quantities.is_empty() {
        return ZERO;
    }
    let index = (quantities.len() - 1) / 2;
    *quantities.select_nth_unstable(index).1
}

/// A `Qty` from an `i128` sum, or `None` outside its range.
fn qty_of(units: i128) -> Option<Qty> {
    i64::try_from(units).ok().map(Qty::from_units)
}

/// The order-book engine state (ADR-043): the start of the windows, the
/// flow minutes and the pending fills. Engine state; the Market State
/// exposes only [`BookState`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LiquidityTracker {
    /// Open time of the minute of the first valid book; `None` before it.
    start: Option<EventTime>,
    /// Closed 1m bars from `start` on.
    closed_minutes: u64,
    ring: FlowRing,
    /// Sorted by (side, price).
    pending: Vec<PendingFill>,
    /// Scratch buffer for the level quantities of one side.
    quantities: Vec<Qty>,
}

/// The tracker after one event, committed with the bars it was computed
/// from.
pub(crate) struct LiquidityStep {
    start: Option<EventTime>,
    closed_minutes: u64,
    /// `None` when the windows did not change.
    windows: Option<LiquidityWindows>,
    /// The one minute the event changed, if any.
    slot: Option<FlowMinute>,
    /// Minutes `[first, last]` an order-book or trades gap flags.
    gap: Option<(EventTime, EventTime)>,
    pending: PendingStep,
}

impl Default for LiquidityTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl LiquidityTracker {
    /// An empty tracker.
    pub(crate) fn new() -> Self {
        Self {
            start: None,
            closed_minutes: 0,
            ring: FlowRing::new(),
            pending: Vec::new(),
            quantities: Vec::new(),
        }
    }

    /// Steps the liquidity flow with `event`, given the bars it closed
    /// (`closed`, in close order) and the book features before it (ADR-043).
    /// Read-only: the book itself changes in [`Self::commit`].
    ///
    /// Order: closed 1m bars from the first valid book on are counted and
    /// the windows recomputed, then the event applies by kind: a trade on a
    /// ready book is a pending fill; an update applied to a ready book is
    /// accounted in its minute (decision 5); a gap flags the minutes and
    /// windows it overlaps; a snapshot starts the windows at the first valid
    /// book. A book event while the book is unavailable flags its minute.
    ///
    /// # Errors
    ///
    /// [`LiquidityError::Overflow`] if a minute's flow sum, a pending fill,
    /// a minute count or a time leaves its integer range; nothing is
    /// committed then. A window sum out of range makes that window
    /// `Unavailable(OutOfRange)` instead.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
        book: &BookState,
    ) -> Result<LiquidityStep, LiquidityError> {
        let mut step = LiquidityStep {
            start: self.start,
            closed_minutes: self.closed_minutes,
            windows: None,
            slot: None,
            gap: None,
            pending: PendingStep::Keep,
        };
        if let Some(start) = self.start {
            let mut end = None;
            for bar in closed
                .iter()
                .filter(|bar| bar.timeframe == Timeframe::M1 && bar.open_time >= start)
            {
                step.closed_minutes = step
                    .closed_minutes
                    .checked_add(1)
                    .ok_or(LiquidityError::Overflow)?;
                end = Some(bar.end());
            }
            if let Some(end) = end {
                step.windows = Some(windows(&self.ring, end, start, step.closed_minutes)?);
            }
        }

        match event {
            MarketEvent::Trade(trade) => {
                if book.l2.is_ready() {
                    step.pending = self.trade(trade)?;
                }
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                self.gap(gap, book, &mut step)?;
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::OrderBook => {
                self.gap(gap, book, &mut step)?;
                step.pending = PendingStep::Clear;
            }
            MarketEvent::BookSnapshot(snapshot) => {
                let resets = match &book.l2 {
                    FeatureValue::Ready(l2) => l2.peek(event),
                    FeatureValue::WarmingUp { .. } | FeatureValue::Unavailable { .. } => {
                        OrderBook::new().peek(event)
                    }
                } == BookStep::Reset;
                // A snapshot that finds the book unavailable, or leaves it
                // so, ends or extends a period without flow.
                if !book.l2.is_ready() || !resets {
                    self.flag_minute(snapshot.time, book, &mut step)?;
                }
                if resets && step.start.is_none() {
                    step.start = Some(minute_of(snapshot.time)?);
                }
                step.pending = PendingStep::Clear;
            }
            MarketEvent::BookUpdate(update) => match &book.l2 {
                FeatureValue::Ready(l2) => {
                    if l2.peek(event) == BookStep::Applied {
                        step.slot = Some(self.account(l2, update)?);
                        step.pending = PendingStep::Age;
                    } else {
                        // An invalidation: the provider broke the chain.
                        self.flag_minute(update.time, book, &mut step)?;
                        step.pending = PendingStep::Clear;
                    }
                }
                FeatureValue::Unavailable { .. } => {
                    self.flag_minute(update.time, book, &mut step)?;
                }
                // Before the first snapshot nothing is known or flagged.
                FeatureValue::WarmingUp { .. } => {}
            },
            // Other streams carry no order-book liquidity.
            MarketEvent::FeedGap(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        Ok(step)
    }

    /// Commits a step computed by [`Self::step`] for `event`: the windows,
    /// the flow minutes and the pending fills, then the book itself, then
    /// depth and clusters after a book event.
    pub(crate) fn commit(
        &mut self,
        step: LiquidityStep,
        event: &MarketEvent,
        book: &mut BookState,
    ) {
        let LiquidityStep {
            start,
            closed_minutes,
            windows,
            slot,
            gap,
            pending,
        } = step;
        self.start = start;
        self.closed_minutes = closed_minutes;
        if let Some(windows) = windows {
            book.windows = windows;
        }
        if let Some(slot) = slot {
            self.ring.put(slot);
        }
        if let Some((first, last)) = gap {
            self.ring.flag(first, last);
        }
        match pending {
            PendingStep::Keep => {}
            PendingStep::Put {
                index,
                insert,
                fill,
            } => {
                if insert {
                    self.pending.insert(index, fill);
                } else {
                    self.pending[index] = fill;
                }
            }
            PendingStep::Age => self.age(event, &book.l2),
            PendingStep::Clear => self.pending.clear(),
        }
        if apply_book(event, &mut book.l2) {
            self.measure(book);
        }
    }

    /// The pending fills after `trade` (decision 5, rule 4).
    fn trade(&self, trade: &Trade) -> Result<PendingStep, LiquidityError> {
        let side = match trade.aggressor {
            // An aggressive buy lifts the ask; an aggressive sell hits the
            // bid.
            Aggressor::Buy => Side::Ask,
            Aggressor::Sell => Side::Bid,
        };
        Ok(
            match self
                .pending
                .binary_search_by(|fill| (fill.side, fill.price).cmp(&(side, trade.price)))
            {
                Ok(index) => {
                    let mut fill = self.pending[index];
                    fill.fresh = fill
                        .fresh
                        .checked_add(trade.qty)
                        .ok_or(LiquidityError::Overflow)?;
                    PendingStep::Put {
                        index,
                        insert: false,
                        fill,
                    }
                }
                Err(index) => PendingStep::Put {
                    index,
                    insert: true,
                    fill: PendingFill {
                        side,
                        price: trade.price,
                        fresh: trade.qty,
                        carried: ZERO,
                    },
                },
            },
        )
    }

    /// The minute of `update` with its flow accounted (decision 5): `book`
    /// is ready and the update applies to it.
    fn account(&self, book: &OrderBook, update: &BookUpdate) -> Result<FlowMinute, LiquidityError> {
        let overflow = || LiquidityError::Overflow;
        let mut minute = self.ring.copy(minute_of(update.time)?);
        let Some(frame) = Frame::of(book) else {
            // No mid: no band can be located, none is observed.
            for band in &mut minute.bands {
                band.beyond_window = true;
            }
            return Ok(minute);
        };
        for band in &mut minute.bands {
            if !frame.band_known(band.band) {
                band.beyond_window = true;
            }
        }
        for (side, levels) in [(Side::Bid, &update.bids), (Side::Ask, &update.asks)] {
            for (index, level) in levels.iter().enumerate() {
                let replaced = levels[index + 1..]
                    .iter()
                    .any(|later| later.price == level.price);
                if replaced || !frame.accounted(side, level.price) {
                    continue;
                }
                let old = book.qty_at(side, level.price).unwrap_or(ZERO);
                let mut flow = SideFlow::ZERO;
                if level.qty > old {
                    flow.added = level.qty.checked_sub(old).ok_or_else(overflow)?;
                } else if level.qty < old {
                    let removed = old.checked_sub(level.qty).ok_or_else(overflow)?;
                    let (carried, fresh) = self
                        .pending
                        .binary_search_by(|fill| (fill.side, fill.price).cmp(&(side, level.price)))
                        .map_or((ZERO, ZERO), |at| consume(&self.pending[at], removed));
                    flow.filled = carried.checked_add(fresh).ok_or_else(overflow)?;
                    flow.cancelled = less(removed, flow.filled);
                }
                minute
                    .add(frame, side, level.price, &flow)
                    .ok_or_else(overflow)?;
            }
        }
        // Carried fills that no decrease matched: the level was replenished
        // within one batch (rule 6).
        for fill in &self.pending {
            if fill.carried == ZERO || !frame.accounted(fill.side, fill.price) {
                continue;
            }
            let (matched, _) = consumption(fill, frame, book, update);
            let left = less(fill.carried, matched);
            if left > ZERO {
                let flow = SideFlow {
                    added: left,
                    cancelled: ZERO,
                    filled: left,
                    inferred: left,
                };
                minute
                    .add(frame, fill.side, fill.price, &flow)
                    .ok_or_else(overflow)?;
            }
        }
        Ok(minute)
    }

    /// Ages the pending fills over `event`, an update about to apply to
    /// `l2` (rules 5 and 6): matched volume leaves, fresh volume becomes
    /// carried, carried volume goes.
    fn age(&mut self, event: &MarketEvent, l2: &FeatureValue<OrderBook>) {
        let (MarketEvent::BookUpdate(update), FeatureValue::Ready(book)) = (event, l2) else {
            self.pending.clear();
            return;
        };
        let frame = Frame::of(book);
        self.pending.retain_mut(|fill| {
            let (_, fresh) =
                frame.map_or((ZERO, ZERO), |frame| consumption(fill, frame, book, update));
            fill.carried = less(fill.fresh, fresh);
            fill.fresh = ZERO;
            fill.carried > ZERO
        });
    }

    /// Flags the minute of `time` with `feed_gap`: a book event while the
    /// book is not ready, once the windows started.
    fn flag_minute(
        &self,
        time: EventTime,
        book: &BookState,
        step: &mut LiquidityStep,
    ) -> Result<(), LiquidityError> {
        if step.start.is_none() || matches!(book.l2, FeatureValue::WarmingUp { .. }) {
            return Ok(());
        }
        let mut minute = self.ring.copy(minute_of(time)?);
        minute.feed_gap = true;
        step.slot = Some(minute);
        Ok(())
    }

    /// Flags what an order-book or trades gap overlaps (decision 6): every
    /// ready window, and every minute from the windows' start on that a
    /// window can still reach.
    ///
    /// The minutes of the ready windows end before the gap's end, so the
    /// windows gain the flag directly. Windows computed later end at or
    /// after the minute of the gap's end and reach back at most 60 minutes
    /// from there, which bounds the minutes flagged in the ring.
    fn gap(
        &self,
        gap: &FeedGap,
        book: &BookState,
        step: &mut LiquidityStep,
    ) -> Result<(), LiquidityError> {
        let Some(start) = step.start else {
            return Ok(());
        };
        let overlaps = |from: EventTime, to: EventTime| gap.start < to && gap.end >= from;
        let mut windows = step.windows.unwrap_or(book.windows);
        for (timeframe, value) in catalog::BOOK_LIQUIDITY_WINDOWS
            .iter()
            .map(|(timeframe, _)| *timeframe)
            .zip(&mut windows.values)
        {
            if let FeatureValue::Ready(window) = value {
                let from = shift(window.end, -timeframe.millis())?;
                if overlaps(from, window.end) {
                    window.feed_gap = true;
                }
            }
        }
        step.windows = Some(windows);
        let last = minute_of(gap.end)?;
        let reach = shift(last, -Timeframe::H1.millis())?;
        let first = minute_of(gap.start)?.max(reach).max(start);
        if first <= last {
            step.gap = Some((first, last));
        }
        Ok(())
    }

    /// Recomputes depth and clusters from the book (decisions 3, 4 and 8).
    fn measure(&mut self, book: &mut BookState) {
        let (depth, clusters) = match &book.l2 {
            FeatureValue::WarmingUp { observed, required } => (
                FeatureValue::WarmingUp {
                    observed: *observed,
                    required: *required,
                },
                FeatureValue::WarmingUp {
                    observed: *observed,
                    required: *required,
                },
            ),
            FeatureValue::Unavailable { .. } => invalid(),
            FeatureValue::Ready(l2) => match (Frame::of(l2), l2.best_bid(), l2.best_ask()) {
                (Some(frame), Some(best_bid), Some(best_ask)) => {
                    let bid = SideScan::of(l2.bids(), Side::Bid, frame, &mut self.quantities);
                    let bid_median = lower_median(&mut self.quantities);
                    let ask = SideScan::of(l2.asks(), Side::Ask, frame, &mut self.quantities);
                    let ask_median = lower_median(&mut self.quantities);
                    (
                        FeatureValue::Ready(depth(frame, best_bid, best_ask, &bid, &ask)),
                        FeatureValue::Ready(BookClusters {
                            feature: catalog::BOOK_CLUSTERS_V1.key,
                            bid: side_clusters(frame, Side::Bid, &bid, bid_median),
                            ask: side_clusters(frame, Side::Ask, &ask, ask_median),
                        }),
                    )
                }
                _ => invalid(),
            },
        };
        book.depth = depth;
        book.clusters = clusters;
    }
}

/// Depth and clusters of a book that cannot produce them.
fn invalid() -> (FeatureValue<BookDepth>, FeatureValue<BookClusters>) {
    let invalid = Unavailability::InputInvalid;
    (
        FeatureValue::Unavailable { reason: invalid },
        FeatureValue::Unavailable { reason: invalid },
    )
}

/// `book.depth@1` from the scans of both sides.
fn depth(
    frame: Frame,
    best_bid: Level,
    best_ask: Level,
    bid: &SideScan,
    ask: &SideScan,
) -> BookDepth {
    let mut bands = [FeatureValue::Unavailable {
        reason: Unavailability::OutOfRange,
    }; BANDS];
    for (index, (band, value)) in catalog::BOOK_BANDS.iter().zip(&mut bands).enumerate() {
        if !frame.band_known(*band) {
            continue;
        }
        if let (Some(bid_qty), Some(ask_qty)) =
            (qty_of(bid.band_qty[index]), qty_of(ask.band_qty[index]))
        {
            *value = FeatureValue::Ready(BandDepth {
                band: *band,
                bid_qty,
                bid_levels: bid.band_levels[index],
                ask_qty,
                ask_levels: ask.band_levels[index],
            });
        }
    }
    BookDepth {
        feature: catalog::BOOK_DEPTH_V1.key,
        best_bid,
        best_ask,
        bands,
    }
}

/// One side of `book.clusters@1` from its scan.
fn side_clusters(
    frame: Frame,
    side: Side,
    scan: &SideScan,
    median: Qty,
) -> FeatureValue<SideClusters> {
    let out_of_range = FeatureValue::Unavailable {
        reason: Unavailability::OutOfRange,
    };
    if !frame.edge_known(side, OUTER) {
        return out_of_range;
    }
    let Some(total_qty) = qty_of(scan.total) else {
        return out_of_range;
    };
    FeatureValue::Ready(SideClusters {
        top: scan.top,
        median_qty: median,
        total_qty,
        levels: scan.levels,
    })
}

/// Applies a book event to `l2` (decision 1); `false` for any other event.
fn apply_book(event: &MarketEvent, l2: &mut FeatureValue<OrderBook>) -> bool {
    match event {
        MarketEvent::BookSnapshot(_) | MarketEvent::BookUpdate(_) => {}
        MarketEvent::FeedGap(gap) if gap.stream == Stream::OrderBook => {}
        _ => return false,
    }
    let invalid = FeatureValue::Unavailable {
        reason: Unavailability::InputInvalid,
    };
    match l2 {
        FeatureValue::Ready(book) => {
            if let BookStep::Invalidated(_) = book.apply(event) {
                *l2 = invalid;
            }
        }
        FeatureValue::WarmingUp { .. } | FeatureValue::Unavailable { .. } => {
            if matches!(event, MarketEvent::BookSnapshot(_)) {
                // A snapshot builds a fresh book.
                let mut book = OrderBook::new();
                *l2 = match book.apply(event) {
                    BookStep::Reset => FeatureValue::Ready(book),
                    _ => invalid,
                };
            } else if matches!(event, MarketEvent::FeedGap(_))
                && !matches!(l2, FeatureValue::WarmingUp { .. })
            {
                *l2 = invalid;
            }
            // An update to a book that is not ready is ignored, and a gap
            // before the first snapshot leaves it warming up.
        }
    }
    true
}

/// The open time of the minute containing `time`.
fn minute_of(time: EventTime) -> Result<EventTime, LiquidityError> {
    Timeframe::M1.open_of(time).ok_or(LiquidityError::Overflow)
}

/// `time + millis`, checked.
fn shift(time: EventTime, millis: i64) -> Result<EventTime, LiquidityError> {
    time.as_millis()
        .checked_add(millis)
        .map(EventTime::from_millis)
        .ok_or(LiquidityError::Overflow)
}

/// Every liquidity window after the latest closed minute, which ends at
/// `end`; `start` is the minute of the first valid book.
fn windows(
    ring: &FlowRing,
    end: EventTime,
    start: EventTime,
    closed_minutes: u64,
) -> Result<LiquidityWindows, LiquidityError> {
    let mut values = LiquidityWindows::new().values;
    for ((timeframe, definition), value) in catalog::BOOK_LIQUIDITY_WINDOWS.iter().zip(&mut values)
    {
        let required = window_minutes(*timeframe);
        *value = if closed_minutes < required {
            FeatureValue::WarmingUp {
                observed: closed_minutes,
                required,
            }
        } else {
            match window(ring, *timeframe, definition.key, end, start)? {
                Some(window) => FeatureValue::Ready(window),
                // A sum beyond the `Qty` range degrades this window only, as
                // a depth sum does its band: the event is not lost.
                None => FeatureValue::Unavailable {
                    reason: Unavailability::OutOfRange,
                },
            }
        };
    }
    Ok(LiquidityWindows { values })
}

/// The window of length `timeframe` ending at `end`; at least a window's
/// worth of minutes closed since `start`. `Ok(None)` when a flow sum leaves
/// the `Qty` range.
fn window(
    ring: &FlowRing,
    timeframe: Timeframe,
    feature: FeatureKey,
    end: EventTime,
    start: EventTime,
) -> Result<Option<LiquidityWindow>, LiquidityError> {
    let from = shift(end, -timeframe.millis())?;
    let mut sum = LiquidityWindow {
        feature,
        timeframe,
        end,
        bands: catalog::BOOK_BANDS.map(BandFlow::empty),
        partial_start: from <= start,
        feed_gap: false,
    };
    let mut open = from;
    while open < end {
        if let Some(minute) = ring.get(open) {
            for (band, flow) in sum.bands.iter_mut().zip(&minute.bands) {
                if band.add(flow).is_none() {
                    return Ok(None);
                }
            }
            sum.feed_gap |= minute.feed_gap;
        }
        open = shift(open, MINUTE_MS)?;
    }
    Ok(Some(sum))
}

/// Why the order-book liquidity could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiquidityError {
    /// A minute's flow sum, a pending fill, a minute count or a time left
    /// its integer range (ADR-027).
    Overflow,
}

impl fmt::Display for LiquidityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("an order-book liquidity value leaves its integer range"),
        }
    }
}

impl std::error::Error for LiquidityError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::samples::t;
    use crate::event::{BookSnapshot, GapReason};
    use crate::feature::{ParamValue, WarmUp};
    use crate::state::MarketStateEngine;

    /// Mid of the test books: 100 000 USDT, so 1 bps is 10 USDT.
    const MID: i64 = 100_000 * SCALE;
    /// 1 bps of [`MID`] in price units.
    const BP: i64 = 10 * SCALE;
    /// Best bid and ask, 0.05 USDT either side of [`MID`].
    const BID: i64 = MID - 5_000_000;
    const ASK: i64 = MID + 5_000_000;
    /// One BTC.
    const BTC: i64 = SCALE;
    /// 00:10 UTC on day 0: a minute open.
    const T0: i64 = 600_000;

    /// `(price, qty)` units per level.
    type Units = Vec<(i64, i64)>;

    fn lv(price: i64, qty: i64) -> Level {
        Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        }
    }

    fn q(units: i64) -> Qty {
        Qty::from_units(units)
    }

    fn levels(levels: &[(i64, i64)]) -> Vec<Level> {
        levels.iter().map(|&(price, qty)| lv(price, qty)).collect()
    }

    fn snapshot_event(
        millis: i64,
        id: u64,
        bids: &[(i64, i64)],
        asks: &[(i64, i64)],
    ) -> MarketEvent {
        MarketEvent::BookSnapshot(BookSnapshot {
            time: t(millis),
            last_update_id: id,
            bids: levels(bids),
            asks: levels(asks),
        })
    }

    /// Bids at the best, 1, 3 and 6 bps; asks likewise. The window reaches
    /// 6 bps on both sides, beyond every band.
    fn standard() -> (Units, Units) {
        (
            vec![
                (BID, 5 * BTC),
                (MID - BP, 2 * BTC),
                (MID - 3 * BP, 3 * BTC),
                (MID - 6 * BP, 4 * BTC),
            ],
            vec![
                (ASK, 5 * BTC),
                (MID + BP, 2 * BTC),
                (MID + 3 * BP, 3 * BTC),
                (MID + 6 * BP, 4 * BTC),
            ],
        )
    }

    fn sell(millis: i64, id: u64, price: i64, qty: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id: id,
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
            aggressor: Aggressor::Sell,
        })
    }

    fn buy(millis: i64, id: u64, price: i64, qty: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id: id,
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
            aggressor: Aggressor::Buy,
        })
    }

    fn book_gap(start: i64, end: i64) -> MarketEvent {
        crate::event::samples::gap(Stream::OrderBook, start, end, GapReason::Disconnected)
    }

    fn trades_gap(start: i64, end: i64) -> MarketEvent {
        crate::event::samples::gap(Stream::Trades, start, end, GapReason::Disconnected)
    }

    /// The tracker and the book features alone, without bars: events in the
    /// order given.
    struct Harness {
        tracker: LiquidityTracker,
        book: BookState,
        last_id: u64,
        straddle: bool,
    }

    impl Harness {
        fn new() -> Self {
            Self {
                tracker: LiquidityTracker::new(),
                book: BookState::new(),
                last_id: 100,
                straddle: false,
            }
        }

        /// A harness after the standard snapshot at `T0`.
        fn standard() -> Self {
            let mut harness = Self::new();
            let (bids, asks) = standard();
            harness.snapshot(T0, &bids, &asks);
            harness
        }

        fn apply(&mut self, event: &MarketEvent) {
            let step = self.tracker.step(event, &[], &self.book).unwrap();
            self.tracker.commit(step, event, &mut self.book);
        }

        fn snapshot(&mut self, millis: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
            self.last_id += 10;
            self.straddle = true;
            self.apply(&snapshot_event(millis, self.last_id, bids, asks));
        }

        /// The next update in the chain.
        fn next_update(
            &mut self,
            millis: i64,
            bids: &[(i64, i64)],
            asks: &[(i64, i64)],
        ) -> MarketEvent {
            let (first, prev) = if self.straddle {
                (self.last_id - 5, 0)
            } else {
                (self.last_id + 1, self.last_id)
            };
            self.straddle = false;
            self.last_id += 5;
            MarketEvent::BookUpdate(BookUpdate {
                time: t(millis),
                first_update_id: first,
                last_update_id: self.last_id,
                prev_update_id: prev,
                bids: levels(bids),
                asks: levels(asks),
            })
        }

        fn update(&mut self, millis: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) {
            let event = self.next_update(millis, bids, asks);
            self.apply(&event);
        }

        fn minute(&self, millis: i64) -> FlowMinute {
            self.tracker.ring.copy(minute_of(t(millis)).unwrap())
        }

        /// The flow of `side` in band `index` of the minute of `millis`.
        fn flow(&self, millis: i64, index: usize, side: Side) -> SideFlow {
            *self.minute(millis).bands[index].side(side)
        }

        fn l2(&self) -> &OrderBook {
            self.book.l2.ready().expect("a ready book")
        }
    }

    fn flow(added: i64, cancelled: i64, filled: i64, inferred: i64) -> SideFlow {
        SideFlow {
            added: q(added),
            cancelled: q(cancelled),
            filled: q(filled),
            inferred: q(inferred),
        }
    }

    fn warming<T>(observed: u64, required: u64) -> FeatureValue<T> {
        FeatureValue::WarmingUp { observed, required }
    }

    fn unavailable<T>(reason: Unavailability) -> FeatureValue<T> {
        FeatureValue::Unavailable { reason }
    }

    #[test]
    fn constants_match_the_catalog() {
        let param = |name: &str| {
            catalog::BOOK_LIQUIDITY_WINDOW_5M_V1
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        assert_eq!(param("carry_updates"), Some(ParamValue::Int(CARRY_UPDATES)));
        for (index, band) in catalog::BOOK_BANDS.iter().enumerate() {
            assert_eq!(
                param(&format!("band_{}", index + 1)),
                Some(ParamValue::Rate(*band))
            );
        }
        assert_eq!(BANDS, catalog::BOOK_BANDS.len());
        assert_eq!(OUTER, catalog::BOOK_BANDS[2]);
        assert_eq!(
            catalog::BOOK_CLUSTERS_V1.params,
            &[
                crate::feature::Param {
                    name: "band",
                    value: ParamValue::Rate(OUTER),
                },
                crate::feature::Param {
                    name: "top_k",
                    value: ParamValue::Int(i64::try_from(TOP_K).unwrap()),
                },
            ]
        );
        for (timeframe, definition) in catalog::BOOK_LIQUIDITY_WINDOWS {
            assert_eq!(
                definition.warm_up,
                WarmUp::Samples(u32::try_from(window_minutes(timeframe)).unwrap())
            );
        }
        // The ring outlives the longest window by one minute.
        assert_eq!(
            RING as u64,
            catalog::BOOK_LIQUIDITY_WINDOWS
                .map(|(timeframe, _)| window_minutes(timeframe))
                .into_iter()
                .max()
                .unwrap()
                + 1
        );
        assert_eq!(BPS, 10_000);
    }

    // -----------------------------------------------------------------
    // Mixed cancels and fills.

    #[test]
    fn a_fill_and_a_cancel_at_one_price_split_exactly() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 100, 1, BID, 2 * BTC));
        // 5 → 1 BTC: 2 filled by the trade, 2 cancelled.
        h.update(T0 + 100, &[(BID, BTC)], &[]);
        for index in 0..BANDS {
            assert_eq!(h.flow(T0, index, Side::Bid), flow(0, 2 * BTC, 2 * BTC, 0));
            assert_eq!(h.flow(T0, index, Side::Ask), SideFlow::ZERO);
        }
        assert!(h.tracker.pending.is_empty());
    }

    #[test]
    fn fills_at_several_prices_and_both_sides() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.apply(&sell(T0 + 60, 2, MID - BP, BTC / 2));
        h.apply(&buy(T0 + 70, 3, ASK, 7 * BTC / 10));
        h.apply(&buy(T0 + 80, 4, MID + 3 * BP, BTC));
        h.update(
            T0 + 100,
            // The best bid filled, 1 bps filled, 3 bps grew.
            &[
                (BID, 4 * BTC),
                (MID - BP, 3 * BTC / 2),
                (MID - 3 * BP, 4 * BTC),
            ],
            // The best ask filled and cancelled; 3 bps filled.
            &[(ASK, 4 * BTC), (MID + 3 * BP, 2 * BTC)],
        );
        // 1 bps band: the best and the 1 bps levels.
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, 0, 3 * BTC / 2, 0));
        assert_eq!(
            h.flow(T0, 0, Side::Ask),
            flow(0, 3 * BTC / 10, 7 * BTC / 10, 0)
        );
        // 2 bps band: the same levels.
        assert_eq!(h.flow(T0, 1, Side::Bid), h.flow(T0, 0, Side::Bid));
        // 5 bps band: also 3 bps.
        assert_eq!(h.flow(T0, 2, Side::Bid), flow(BTC, 0, 3 * BTC / 2, 0));
        assert_eq!(
            h.flow(T0, 2, Side::Ask),
            flow(0, 3 * BTC / 10, 17 * BTC / 10, 0)
        );
    }

    #[test]
    fn a_fill_at_the_previous_updates_millisecond_matches_through_the_carry() {
        let mut h = Harness::standard();
        // The trade sorts before the update of its millisecond, but its
        // decrease lands in the next batch.
        h.apply(&sell(T0 + 100, 1, BID, BTC));
        h.update(T0 + 100, &[(MID - 3 * BP, 4 * BTC)], &[]);
        assert_eq!(h.flow(T0, 2, Side::Bid), flow(BTC, 0, 0, 0));
        assert_eq!(h.tracker.pending.len(), 1);
        assert_eq!(h.tracker.pending[0].carried, q(BTC));
        h.update(T0 + 200, &[(BID, 4 * BTC)], &[]);
        // Filled, not cancelled, and nothing inferred.
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, 0, BTC, 0));
        assert_eq!(h.flow(T0, 2, Side::Bid), flow(BTC, 0, BTC, 0));
        assert!(h.tracker.pending.is_empty());
    }

    #[test]
    fn a_replenished_level_is_inferred_after_one_carry() {
        let mut h = Harness::standard();
        // 1 BTC filled and 2 BTC added in one batch: the book shows +1.
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.update(T0 + 100, &[(BID, 6 * BTC)], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(BTC, 0, 0, 0));
        // The next update does not touch the price: the carried fill is
        // booked as added and filled.
        h.update(T0 + 200, &[], &[(MID + 3 * BP, 3 * BTC)]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(2 * BTC, 0, BTC, BTC));
        assert!(h.tracker.pending.is_empty());
        // A decrease in that update takes the carried fill first.
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, 2 * BTC));
        h.update(T0 + 100, &[(BID, 6 * BTC)], &[]);
        h.update(T0 + 200, &[(BID, 5 * BTC)], &[]);
        // +1 added, then 1 removed (filled from the carry), then 1 inferred.
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(2 * BTC, 0, 2 * BTC, BTC));
    }

    #[test]
    fn fresh_fills_are_matched_after_carried_ones() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.update(T0 + 100, &[(MID - 6 * BP, 5 * BTC)], &[]);
        h.apply(&sell(T0 + 150, 2, BID, BTC));
        // 1.5 BTC removed: the carried 1 first, then 0.5 of the fresh one.
        h.update(T0 + 200, &[(BID, 7 * BTC / 2)], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, 0, 3 * BTC / 2, 0));
        assert_eq!(
            h.tracker.pending,
            [PendingFill {
                side: Side::Bid,
                price: Price::from_units(BID),
                fresh: ZERO,
                carried: q(BTC / 2),
            }]
        );
        // The rest is inferred one update later.
        h.update(T0 + 300, &[], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(BTC / 2, 0, 2 * BTC, BTC / 2));
    }

    #[test]
    fn fills_outside_the_band_or_the_window_are_dropped() {
        let mut h = Harness::standard();
        // 6 bps: inside the window, beyond the outer band. 7 bps: beyond the
        // window.
        h.apply(&sell(T0 + 50, 1, MID - 6 * BP, BTC));
        h.apply(&sell(T0 + 60, 2, MID - 7 * BP, BTC));
        h.update(T0 + 100, &[(MID - 6 * BP, 3 * BTC)], &[]);
        h.update(T0 + 200, &[], &[]);
        h.update(T0 + 300, &[], &[]);
        assert_eq!(h.minute(T0), FlowMinute::empty(t(T0)));
        assert!(h.tracker.pending.is_empty());
    }

    #[test]
    fn the_last_value_per_price_in_an_update_wins() {
        let mut h = Harness::standard();
        h.update(T0 + 100, &[(BID, 9 * BTC), (BID, 4 * BTC)], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, BTC, 0, 0));
        assert_eq!(
            h.l2().qty_at(Side::Bid, Price::from_units(BID)),
            Some(q(4 * BTC))
        );
    }

    #[test]
    fn liquidations_are_not_fills() {
        let mut h = Harness::standard();
        h.apply(&crate::event::samples::liquidation(T0 + 50, BTC));
        h.update(T0 + 100, &[(BID, 4 * BTC)], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, BTC, 0, 0));
    }

    #[test]
    fn a_band_edge_beyond_the_window_flags_the_minute() {
        let mut h = Harness::new();
        // The bid window ends at 3 bps: the 5 bps band is beyond it.
        h.snapshot(
            T0,
            &[(BID, BTC), (MID - 3 * BP, BTC)],
            &[(ASK, BTC), (MID + 6 * BP, BTC)],
        );
        h.update(T0 + 100, &[(BID, 2 * BTC)], &[]);
        let minute = h.minute(T0);
        assert_eq!(
            minute.bands.map(|band| band.beyond_window),
            [false, false, true]
        );
        assert_eq!(minute.bands[2].bid, flow(BTC, 0, 0, 0));
    }

    // -----------------------------------------------------------------
    // Resets.

    #[test]
    fn a_checkpoint_snapshot_books_no_flow_and_drops_pending_fills() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        let (bids, asks) = standard();
        let mut moved = bids.clone();
        moved[0].1 = BTC;
        h.snapshot(T0 + 100, &moved, &asks);
        assert_eq!(h.minute(T0), FlowMinute::empty(t(T0)));
        assert!(h.tracker.pending.is_empty());
        assert_eq!(
            h.l2().qty_at(Side::Bid, Price::from_units(BID)),
            Some(q(BTC))
        );
        // The decrease after it is a cancel: the fill was inside the
        // snapshot.
        h.update(T0 + 200, &[(BID, 0)], &[]);
        assert_eq!(h.flow(T0, 0, Side::Bid), flow(0, BTC, 0, 0));
    }

    #[test]
    fn an_order_book_gap_invalidates_until_the_next_snapshot() {
        let mut h = Harness::standard();
        assert!(h.book.depth.is_ready());
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.apply(&book_gap(T0 + 60, T0 + 70_000));
        assert_eq!(h.book.l2, unavailable(Unavailability::InputInvalid));
        assert_eq!(h.book.depth, unavailable(Unavailability::InputInvalid));
        assert_eq!(h.book.clusters, unavailable(Unavailability::InputInvalid));
        assert!(h.tracker.pending.is_empty());
        // Both overlapped minutes are flagged.
        assert!(h.minute(T0).feed_gap);
        assert!(h.minute(T0 + 60_000).feed_gap);
        assert!(!h.minute(T0 + 120_000).feed_gap);
        // Trades and updates while unavailable count nothing; an update
        // flags its minute.
        h.apply(&sell(T0 + 125_000, 2, BID, BTC));
        assert!(h.tracker.pending.is_empty());
        let update = h.next_update(T0 + 125_000, &[(BID, 0)], &[]);
        h.apply(&update);
        assert_eq!(h.minute(T0 + 120_000).bands[0].bid, SideFlow::ZERO);
        assert!(h.minute(T0 + 120_000).feed_gap);
        // The next snapshot builds a fresh book.
        let (bids, asks) = standard();
        h.snapshot(T0 + 130_000, &bids, &asks);
        assert!(h.book.l2.is_ready());
        assert!(h.book.depth.is_ready());
        assert_eq!(h.l2().len(), 8);
    }

    #[test]
    fn an_invalidation_without_a_gap_behaves_the_same() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        // An update that does not straddle the snapshot.
        h.apply(&MarketEvent::BookUpdate(BookUpdate {
            time: t(T0 + 70_000),
            first_update_id: 200,
            last_update_id: 205,
            prev_update_id: 0,
            bids: levels(&[(BID, 0)]),
            asks: Vec::new(),
        }));
        assert_eq!(h.book.l2, unavailable(Unavailability::InputInvalid));
        assert_eq!(h.book.depth, unavailable(Unavailability::InputInvalid));
        assert_eq!(h.book.clusters, unavailable(Unavailability::InputInvalid));
        assert!(h.tracker.pending.is_empty());
        assert!(h.minute(T0 + 60_000).feed_gap);
        assert_eq!(h.minute(T0 + 60_000).bands[0].bid, SideFlow::ZERO);
        let (bids, asks) = standard();
        h.snapshot(T0 + 80_000, &bids, &asks);
        assert!(h.book.l2.is_ready());
    }

    #[test]
    fn a_snapshot_with_a_negative_quantity_invalidates_and_flags() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.snapshot(T0 + 100, &[(BID, -1)], &[(ASK, BTC)]);
        assert_eq!(h.book.l2, unavailable(Unavailability::InputInvalid));
        assert_eq!(h.book.depth, unavailable(Unavailability::InputInvalid));
        assert!(h.tracker.pending.is_empty());
        assert!(h.minute(T0).feed_gap);
    }

    #[test]
    fn a_trades_gap_flags_the_minutes_but_keeps_the_book() {
        let mut h = Harness::standard();
        h.apply(&sell(T0 + 50, 1, BID, BTC));
        h.apply(&trades_gap(T0 + 60, T0 + 61_000));
        assert!(h.book.l2.is_ready());
        assert!(h.book.depth.is_ready());
        assert_eq!(h.tracker.pending.len(), 1);
        assert!(h.minute(T0).feed_gap);
        assert!(h.minute(T0 + 60_000).feed_gap);
    }

    #[test]
    fn nothing_is_flagged_or_counted_before_the_first_snapshot() {
        let mut h = Harness::new();
        h.apply(&book_gap(T0, T0 + 1_000));
        h.apply(&sell(T0 + 1_100, 1, BID, BTC));
        let update = h.next_update(T0 + 1_200, &[(BID, BTC)], &[]);
        h.apply(&update);
        assert_eq!(h.book, BookState::new());
        assert_eq!(h.tracker, LiquidityTracker::new());
    }

    // -----------------------------------------------------------------
    // Band edges and validity.

    /// The depth of `bids` and `asks` around [`MID`], window as given.
    fn depth_of(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> FeatureValue<BookDepth> {
        let mut h = Harness::new();
        h.snapshot(T0, bids, asks);
        h.book.depth
    }

    fn band(depth: &FeatureValue<BookDepth>, index: usize) -> FeatureValue<BandDepth> {
        depth.ready().unwrap().bands[index]
    }

    #[test]
    fn levels_exactly_at_a_band_edge_are_inside() {
        // Levels exactly at 1, 2 and 5 bps, and one unit beyond each; the
        // window reaches 6 bps.
        let side = |sign: i64| {
            let mut out = vec![(MID - sign * 5_000_000, BTC)];
            for bps in [1, 2, 5] {
                out.push((MID - sign * bps * BP, 10 * BTC));
                out.push((MID - sign * (bps * BP + 1), 100 * BTC));
            }
            out.push((MID - sign * 6 * BP, 1_000 * BTC));
            out
        };
        let depth = depth_of(&side(1), &side(-1));
        let expected = |levels: u64, qty: i64| {
            FeatureValue::Ready(BandDepth {
                band: Rate::from_units(0),
                bid_qty: q(qty),
                bid_levels: levels,
                ask_qty: q(qty),
                ask_levels: levels,
            })
        };
        let strip = |value: FeatureValue<BandDepth>| {
            value.map(|band| BandDepth {
                band: Rate::from_units(0),
                ..band
            })
        };
        assert_eq!(strip(band(&depth, 0)), expected(2, 11 * BTC));
        assert_eq!(strip(band(&depth, 1)), expected(4, 121 * BTC));
        assert_eq!(strip(band(&depth, 2)), expected(6, 231 * BTC));
        let clusters = depth_of(&side(1), &side(-1));
        assert!(clusters.is_ready());
    }

    #[test]
    fn a_band_edge_at_the_window_bound_is_in_range() {
        // The deepest level exactly at 5 bps: the 5 bps band is known.
        let at = depth_of(
            &[(BID, BTC), (MID - 5 * BP, BTC)],
            &[(ASK, BTC), (MID + 5 * BP, BTC)],
        );
        assert!(band(&at, 2).is_ready());
        // One unit short on the bid side: the band is out of range, the
        // inner ones are not.
        let short = depth_of(
            &[(BID, BTC), (MID - 5 * BP + 1, BTC)],
            &[(ASK, BTC), (MID + 5 * BP, BTC)],
        );
        assert_eq!(band(&short, 2), unavailable(Unavailability::OutOfRange));
        assert!(band(&short, 1).is_ready());
        assert!(band(&short, 0).is_ready());
        // Likewise on the ask side.
        let short = depth_of(
            &[(BID, BTC), (MID - 5 * BP, BTC)],
            &[(ASK, BTC), (MID + 5 * BP - 1, BTC)],
        );
        assert_eq!(band(&short, 2), unavailable(Unavailability::OutOfRange));
    }

    #[test]
    fn per_side_out_of_range_for_clusters() {
        let mut h = Harness::new();
        h.snapshot(
            T0,
            &[(BID, BTC), (MID - 6 * BP, BTC)],
            &[(ASK, BTC), (MID + 4 * BP, BTC)],
        );
        let clusters = h.book.clusters.ready().unwrap();
        assert!(clusters.bid.is_ready());
        assert_eq!(clusters.ask, unavailable(Unavailability::OutOfRange));
    }

    #[test]
    fn one_sided_and_crossed_books_are_invalid_input() {
        let invalid = unavailable::<BookDepth>(Unavailability::InputInvalid);
        assert_eq!(depth_of(&[(BID, BTC)], &[]), invalid);
        assert_eq!(depth_of(&[], &[(ASK, BTC)]), invalid);
        assert_eq!(depth_of(&[(ASK, BTC)], &[(BID, BTC)]), invalid);
        assert_eq!(depth_of(&[(BID, BTC)], &[(BID, BTC)]), invalid);
        let mut h = Harness::new();
        h.snapshot(T0, &[(BID, BTC)], &[]);
        assert!(h.book.l2.is_ready());
        assert_eq!(h.book.clusters, unavailable(Unavailability::InputInvalid));
        // An update to a one-sided book has no mid: every band is beyond.
        h.update(T0 + 100, &[(BID, 2 * BTC)], &[]);
        assert_eq!(
            h.minute(T0)
                .bands
                .map(|band| (band.beyond_window, band.bid)),
            [(true, SideFlow::ZERO); 3]
        );
    }

    #[test]
    fn a_half_unit_mid_stays_exact() {
        // Bid 99 999.99999999, ask 100 000.00000002: twice the mid is odd.
        let (bid, ask) = (MID - 1, MID + 2);
        let sum = i128::from(bid) + i128::from(ask);
        let frame = Frame {
            sum,
            lowest_bid: Some(Price::from_units(MID - 6 * BP)),
            highest_ask: Some(Price::from_units(MID + 6 * BP)),
        };
        // The exact 1 bps edge of a bid: 2p ≥ sum · (1 − 1e-4), at the
        // ceiling of a half unit.
        let edge = |band: i128| {
            let twice = sum * (i128::from(SCALE) - band);
            let denominator = 2 * i128::from(SCALE);
            i64::try_from((twice + denominator - 1).div_euclid(denominator)).unwrap()
        };
        let bid_edge = edge(10_000);
        assert!(frame.within(
            Side::Bid,
            Price::from_units(bid_edge),
            catalog::BOOK_BANDS[0]
        ));
        assert!(!frame.within(
            Side::Bid,
            Price::from_units(bid_edge - 1),
            catalog::BOOK_BANDS[0]
        ));
        let ask_edge = {
            let twice = sum * (i128::from(SCALE) + 10_000);
            i64::try_from(twice.div_euclid(2 * i128::from(SCALE))).unwrap()
        };
        assert!(frame.within(
            Side::Ask,
            Price::from_units(ask_edge),
            catalog::BOOK_BANDS[0]
        ));
        assert!(!frame.within(
            Side::Ask,
            Price::from_units(ask_edge + 1),
            catalog::BOOK_BANDS[0]
        ));
        // Through the book: the edge levels count, one unit beyond does not.
        let depth = depth_of(
            &[
                (bid, BTC),
                (bid_edge, BTC),
                (bid_edge - 1, BTC),
                (MID - 6 * BP, BTC),
            ],
            &[
                (ask, BTC),
                (ask_edge, BTC),
                (ask_edge + 1, BTC),
                (MID + 6 * BP, BTC),
            ],
        );
        let inner = band(&depth, 0);
        let inner = inner.ready().unwrap();
        assert_eq!((inner.bid_levels, inner.ask_levels), (2, 2));
    }

    #[test]
    fn sums_beyond_qty_are_out_of_range() {
        let half = i64::MAX / 2 + 1;
        let depth = depth_of(
            &[(BID, half), (BID - 1, half), (MID - 6 * BP, 1)],
            &[(ASK, 1), (MID + 6 * BP, 1)],
        );
        for index in 0..BANDS {
            assert_eq!(band(&depth, index), unavailable(Unavailability::OutOfRange));
        }
        let mut h = Harness::new();
        h.snapshot(
            T0,
            &[(BID, half), (BID - 1, half), (MID - 6 * BP, 1)],
            &[(ASK, 1), (MID + 6 * BP, 1)],
        );
        let clusters = h.book.clusters.ready().unwrap();
        assert_eq!(clusters.bid, unavailable(Unavailability::OutOfRange));
        assert!(clusters.ask.is_ready());
    }

    #[test]
    fn imbalance_is_none_when_both_sides_are_empty() {
        let band = |bid: i64, ask: i64| BandDepth {
            band: catalog::BOOK_BANDS[0],
            bid_qty: q(bid),
            bid_levels: 1,
            ask_qty: q(ask),
            ask_levels: 1,
        };
        assert_eq!(band(0, 0).imbalance(), None);
        assert_eq!(band(3, 1).imbalance(), Some(0.5));
        assert_eq!(band(0, 5).imbalance(), Some(-1.0));
        assert_eq!(
            band(i64::MAX, i64::MAX).imbalance().map(f64::to_bits),
            Some(0.0_f64.to_bits())
        );
    }

    #[test]
    fn clusters_order_ties_and_the_lower_median() {
        let mut h = Harness::new();
        // Bids within 5 bps: 1, 7, 3, 7, 2, 9, 1 BTC from the best down; an
        // 8 BTC level beyond 5 bps does not count.
        h.snapshot(
            T0,
            &[
                (BID, BTC),
                (MID - BP, 7 * BTC),
                (MID - 2 * BP, 3 * BTC),
                (MID - 3 * BP, 7 * BTC),
                (MID - 4 * BP, 2 * BTC),
                (MID - 9 * BP / 2, 9 * BTC),
                (MID - 5 * BP, BTC),
                (MID - 6 * BP, 8 * BTC),
            ],
            &[(ASK, 2 * BTC), (MID + 6 * BP, BTC)],
        );
        let clusters = h.book.clusters.ready().unwrap();
        let bid = clusters.bid.ready().unwrap();
        // Largest first; the tie at 7 goes to the level nearer mid.
        assert_eq!(
            bid.top,
            [
                Some(lv(MID - 9 * BP / 2, 9 * BTC)),
                Some(lv(MID - BP, 7 * BTC)),
                Some(lv(MID - 3 * BP, 7 * BTC)),
                Some(lv(MID - 2 * BP, 3 * BTC)),
                Some(lv(MID - 4 * BP, 2 * BTC)),
            ]
        );
        // Sorted 1, 1, 2, 3, 7, 7, 9: the lower median of 7 is the 4th.
        assert_eq!(bid.median_qty, q(3 * BTC));
        assert_eq!((bid.total_qty, bid.levels), (q(30 * BTC), 7));
        assert_eq!(bid.multiple(0), Some(3.0));
        assert_eq!(bid.multiple(5), None);
        // One ask level: the rest of the top is empty; an even count takes
        // the lower median.
        let ask = clusters.ask.ready().unwrap();
        assert_eq!(ask.top, [Some(lv(ASK, 2 * BTC)), None, None, None, None]);
        assert_eq!(ask.median_qty, q(2 * BTC));
        assert_eq!(ask.multiple(1), None);
        let mut quantities = [q(4), q(1), q(3), q(2)];
        assert_eq!(lower_median(&mut quantities), q(2));
        assert_eq!(lower_median(&mut []), ZERO);
        assert_eq!(
            clusters.to_string(),
            "bid median=3.00000000 total=30.00000000/7 \
             top=99955.00000000@9.00000000[3],99990.00000000@7.00000000[2.3333333333333335],\
             99970.00000000@7.00000000[2.3333333333333335],99980.00000000@3.00000000[1],\
             99960.00000000@2.00000000[0.6666666666666666] \
             | ask median=2.00000000 total=2.00000000/1 top=100000.05000000@2.00000000[1] \
             | book.clusters@1"
        );
    }

    // -----------------------------------------------------------------
    // Windows through the engine.

    /// One trade per minute from `from` to `to` (exclusive, minutes), so
    /// every 1m bar closes; 100 bps above mid, beyond every window, so it is
    /// never accounted. Trade ids step by 10 from `first_id`, leaving room.
    fn clock(from: i64, to: i64, first_id: u64) -> Vec<MarketEvent> {
        (from..to)
            .zip((first_id..).step_by(10))
            .map(|(minute, id)| buy(minute * 60_000 + 30_000, id, MID + 100 * BP, 1))
            .collect()
    }

    fn engine_after(events: &[MarketEvent]) -> MarketStateEngine {
        let mut engine = MarketStateEngine::new();
        for event in events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
        }
        engine
    }

    fn window_at(
        engine: &MarketStateEngine,
        timeframe: Timeframe,
    ) -> FeatureValue<LiquidityWindow> {
        *engine.state().book.windows.get(timeframe).unwrap()
    }

    #[test]
    fn windows_warm_up_from_the_minute_of_the_first_valid_book() {
        // Trades from minute 0; the first book at minute 3.
        let (bids, asks) = standard();
        let mut events = clock(0, 3, 1);
        events.push(snapshot_event(3 * 60_000 + 40_000, 100, &bids, &asks));
        events.extend(clock(3, 10, 100));
        events.sort();
        let mut engine = MarketStateEngine::new();
        let mut seen = Vec::new();
        for event in &events {
            engine.apply(event).unwrap();
            seen.push(window_at(&engine, Timeframe::M5));
        }
        // Minutes 0–2 closed before the book: no sample. Minute 3 is the
        // first; the window is ready after minute 7 closed.
        assert!(seen[..4].iter().all(|value| *value == warming(0, 5)));
        let ready = engine.state().book.windows.get(Timeframe::M5).unwrap();
        let window = ready.ready().unwrap();
        assert_eq!(window.end, t(9 * 60_000));
        assert!(!window.partial_start);
        let first = seen
            .iter()
            .find_map(|value| value.ready().copied())
            .unwrap();
        assert_eq!(first.end, t(8 * 60_000));
        assert!(first.partial_start);
        assert_eq!(window_at(&engine, Timeframe::M15), warming(6, 15));
    }

    #[test]
    fn archive_like_input_without_a_book_warms_up_forever() {
        let engine = engine_after(&clock(0, 180, 1));
        let book = &engine.state().book;
        assert_eq!(book, &BookState::new());
        for (timeframe, _, value) in book.windows.iter() {
            assert_eq!(*value, warming(0, window_minutes(timeframe)));
        }
    }

    #[test]
    fn an_order_book_gap_flags_ready_windows_and_its_minutes() {
        let (bids, asks) = standard();
        let mut events = vec![snapshot_event(30_000, 100, &bids, &asks)];
        events.extend(clock(0, 7, 1));
        events.sort();
        let mut engine = engine_after(&events);
        let before = window_at(&engine, Timeframe::M5);
        assert!(!before.ready().unwrap().feed_gap);
        // A gap from minute 5 to minute 6: the ready window (minutes 1–5)
        // overlaps it.
        engine
            .apply(&book_gap(5 * 60_000 + 10_000, 6 * 60_000 + 40_000))
            .unwrap();
        let flagged = window_at(&engine, Timeframe::M5);
        assert!(flagged.ready().unwrap().feed_gap);
        assert_eq!(
            engine.state().book.l2,
            unavailable(Unavailability::InputInvalid)
        );
        // Later windows keep the flag while they reach minutes 5 or 6, and
        // lose it after.
        engine
            .apply(&snapshot_event(6 * 60_000 + 40_000, 200, &bids, &asks))
            .unwrap();
        for (id, minute) in (1_000..).zip(7..14) {
            engine
                .apply(&buy(minute * 60_000 + 30_000, id, MID + 100 * BP, 1))
                .unwrap();
            let window = window_at(&engine, Timeframe::M5);
            let window = window.ready().unwrap();
            let reaches = window.end.as_millis() - 5 * 60_000 <= 6 * 60_000;
            assert_eq!(window.feed_gap, reaches, "{window}");
        }
    }

    #[test]
    fn windows_sum_their_minutes() {
        let (bids, asks) = standard();
        let mut events = vec![snapshot_event(10_000, 100, &bids, &asks)];
        events.extend(clock(0, 8, 1));
        // A fill and an add in minute 3, a cancel in minute 4.
        events.push(sell(190_000, 25, BID, BTC));
        events.push(MarketEvent::BookUpdate(BookUpdate {
            time: t(190_000),
            first_update_id: 95,
            last_update_id: 105,
            prev_update_id: 0,
            bids: levels(&[(BID, 4 * BTC), (MID - 3 * BP, 5 * BTC)]),
            asks: Vec::new(),
        }));
        events.push(MarketEvent::BookUpdate(BookUpdate {
            time: t(250_000),
            first_update_id: 106,
            last_update_id: 110,
            prev_update_id: 105,
            bids: Vec::new(),
            asks: levels(&[(ASK, 4 * BTC)]),
        }));
        events.sort();
        let engine = engine_after(&events);
        let window = window_at(&engine, Timeframe::M5);
        let window = window.ready().unwrap();
        assert_eq!(window.end, t(7 * 60_000));
        assert_eq!(window.bands[0].bid, flow(0, 0, BTC, 0));
        assert_eq!(window.bands[2].bid, flow(2 * BTC, 0, BTC, 0));
        assert_eq!(window.bands[0].ask, flow(0, BTC, 0, 0));
        assert_eq!(
            window.to_string(),
            "5m end=420000ms \
             | 1bps bid=0.00000000/0.00000000/1.00000000/0.00000000 \
             ask=0.00000000/1.00000000/0.00000000/0.00000000 \
             | 2bps bid=0.00000000/0.00000000/1.00000000/0.00000000 \
             ask=0.00000000/1.00000000/0.00000000/0.00000000 \
             | 5bps bid=2.00000000/0.00000000/1.00000000/0.00000000 \
             ask=0.00000000/1.00000000/0.00000000/0.00000000 \
             | complete book.liquidity.window.5m@1"
        );
        assert_eq!(window.band(catalog::BOOK_BANDS[2]), Some(&window.bands[2]));
    }

    #[test]
    fn a_level_beyond_the_old_mid_counts_by_its_true_distance() {
        // A batch gaps price up 50 bps: it takes the asks up to 50 bps away
        // and places a bid there. Against the pre-update mid that bid is
        // 50 bps away on the far side, outside every band, not inside all.
        let bids = vec![(BID, 5 * BTC), (MID - 10 * BP, BTC)];
        let asks = vec![(ASK, 5 * BTC), (MID + 50 * BP, BTC), (MID + 100 * BP, BTC)];
        let mut events = vec![snapshot_event(10_000, 100, &bids, &asks)];
        events.extend(clock(0, 8, 1));
        events.push(MarketEvent::BookUpdate(BookUpdate {
            time: t(190_000),
            first_update_id: 95,
            last_update_id: 105,
            prev_update_id: 0,
            bids: levels(&[(MID + 50 * BP, 7 * BTC)]),
            asks: levels(&[(ASK, 0), (MID + 50 * BP, 0)]),
        }));
        events.sort();
        let engine = engine_after(&events);
        let window = window_at(&engine, Timeframe::M5);
        let window = window.ready().unwrap();
        for band in &window.bands {
            assert_eq!(band.bid, flow(0, 0, 0, 0), "{window}");
        }
        // The best ask, 0.05 USDT from mid, is still accounted.
        assert_eq!(window.bands[0].ask, flow(0, 5 * BTC, 0, 0), "{window}");
        assert!(engine.state().book.l2.is_ready());
    }

    #[test]
    fn a_window_sum_beyond_the_qty_range_degrades_only_that_window() {
        // Half the `Qty` range added at one 1 bps bid in minutes 3 and 5:
        // each minute holds it, a window over both cannot.
        let half = i64::MAX / 2 + 1;
        let price = MID - BP / 2;
        let (bids, asks) = standard();
        let update = |minute: i64, id: u64, qty: i64| {
            MarketEvent::BookUpdate(BookUpdate {
                time: t(minute * 60_000 + 40_000),
                first_update_id: if id == 101 { 95 } else { id },
                last_update_id: id,
                prev_update_id: if id == 101 { 0 } else { id - 1 },
                bids: levels(&[(price, qty)]),
                asks: Vec::new(),
            })
        };
        let mut events = vec![
            snapshot_event(10_000, 100, &bids, &asks),
            update(3, 101, half),
            update(4, 102, 0),
            update(5, 103, half),
        ];
        events.extend(clock(0, 30, 1));
        events.sort();
        // Every event applies: the trades that close minutes 6 to 8, whose
        // 5m windows reach minutes 3 and 5, are not rejected.
        let at = |minutes: i64| {
            let cut = minutes * 60_000;
            let prefix: Vec<_> = events
                .iter()
                .filter(|event| event.time().as_millis() < cut)
                .cloned()
                .collect();
            engine_after(&prefix)
        };
        // Minute 8's trade closes minute 7: the 5m window covers 3..7.
        let engine = at(9);
        assert_eq!(
            window_at(&engine, Timeframe::M5),
            unavailable(Unavailability::OutOfRange)
        );
        assert!(engine.state().book.depth.is_ready());
        // Minute 9 closes 8: the window covers 4..8 and holds one half.
        let engine = at(10);
        let window = window_at(&engine, Timeframe::M5);
        let window = window.ready().unwrap();
        assert_eq!(window.bands[0].bid.added, q(half));
        // At the end the 15m window, ready since minute 15, is past both.
        let engine = engine_after(&events);
        assert!(window_at(&engine, Timeframe::M5).is_ready());
        assert!(window_at(&engine, Timeframe::M15).is_ready());
    }

    #[test]
    fn the_book_encoding_counts_every_feature() {
        let encoded = |book: &BookState| {
            let mut f = Fingerprinter::new();
            book.encode(&mut f);
            f.finish()
        };
        let (bids, asks) = standard();
        let mut events = vec![snapshot_event(10_000, 100, &bids, &asks)];
        events.extend(clock(0, 8, 1));
        events.sort();
        let base = engine_after(&events).state().book.clone();
        assert!(window_at(&engine_after(&events), Timeframe::M5).is_ready());
        let mut variants = vec![base.clone()];
        let mut windows = base.clone();
        windows.windows = LiquidityWindows::new();
        variants.push(windows);
        let mut depth = base.clone();
        if let FeatureValue::Ready(value) = &mut depth.depth {
            value.bands[0] = unavailable(Unavailability::OutOfRange);
        }
        variants.push(depth);
        let mut clusters = base.clone();
        if let FeatureValue::Ready(value) = &mut clusters.clusters {
            value.bid = unavailable(Unavailability::OutOfRange);
        }
        variants.push(clusters);
        let mut l2 = base;
        l2.l2 = warming(0, 1);
        variants.push(l2);
        let hashes: Vec<_> = variants.iter().map(encoded).collect();
        for (i, a) in hashes.iter().enumerate() {
            for b in &hashes[i + 1..] {
                assert_ne!(a, b);
            }
        }
    }

    // -----------------------------------------------------------------
    // Golden output per feature (ADR-029, "Adding a feature" step 7).

    const DAY: i64 = 86_400_000;
    const MINUTE: i64 = 60_000;
    /// The checkpoint snapshot of the golden tape: 00:20 UTC on day 1.
    const CHECKPOINT: i64 = DAY + 20 * MINUTE;
    /// The order-book gap of the golden tape: 00:40:00 to 00:40:20 UTC on
    /// day 1; no update is delivered inside it.
    const BOOK_GAP: (i64, i64) = (DAY + 40 * MINUTE, DAY + 40 * MINUTE + 20_000);
    /// The trades gap of the golden tape: 00:55:00 to 00:55:30 UTC on day 1;
    /// no trade is delivered inside it.
    const TRADES_GAP: (i64, i64) = (DAY + 55 * MINUTE, DAY + 55 * MINUTE + 30_000);
    /// The end of the golden tape: 01:15 UTC on day 1.
    const GOLDEN_END: i64 = DAY + 75 * MINUTE;

    /// The book of the golden tape's exchange: price units to quantity
    /// units per side.
    struct GoldenBook {
        bids: std::collections::BTreeMap<i64, i64>,
        asks: std::collections::BTreeMap<i64, i64>,
    }

    impl GoldenBook {
        /// The top `depth` levels per side as a snapshot with id `id`.
        fn snapshot(&self, time: i64, id: u64, depth: usize) -> MarketEvent {
            MarketEvent::BookSnapshot(BookSnapshot {
                time: t(time),
                last_update_id: id,
                bids: self
                    .bids
                    .iter()
                    .rev()
                    .take(depth)
                    .map(|(&price, &qty)| lv(price, qty))
                    .collect(),
                asks: self
                    .asks
                    .iter()
                    .take(depth)
                    .map(|(&price, &qty)| lv(price, qty))
                    .collect(),
            })
        }
    }

    /// The liquidity golden tape: 00:00 to 01:15 UTC on day 1.
    ///
    /// - An exchange book on a 0.5 USDT grid around 62 500 USDT, 100 levels
    ///   a side. A third of the 100 ms batches carry one to three actions
    ///   from an LCG: adds within 40 ticks of the opposite best, partial or
    ///   full cancels among the best 40 levels, and fills at the best level
    ///   with their taker trade. A side below 20 levels only grows.
    ///   Each active batch is one update with the final quantity per price,
    ///   at the time of its last action.
    /// - A 100-level snapshot at 00:00:00.500, a 70-level checkpoint at
    ///   00:20, an order-book gap from 00:40:00 to 00:40:20 resynced by a
    ///   100-level snapshot, and a trades gap from 00:55:00 to 00:55:30.
    fn golden_tape() -> Vec<MarketEvent> {
        use crate::bars::tests::Lcg;
        let mut lcg = Lcg(0x6d69_6500_0000_0012);
        let tick = SCALE / 2;
        let base = 62_500 * SCALE;
        let mut book = GoldenBook {
            bids: std::collections::BTreeMap::new(),
            asks: std::collections::BTreeMap::new(),
        };
        for k in 0..100 {
            book.bids
                .insert(base - k * tick, (1 + lcg.below(40)) * SCALE / 10);
            book.asks
                .insert(base + (k + 1) * tick, (1 + lcg.below(40)) * SCALE / 10);
        }
        let mut events = Vec::new();
        let mut id: u64 = 5_000;
        let mut trade_id: u64 = 0;
        events.push(book.snapshot(DAY + 500, id + 1, 100));
        let mut last_book_event = DAY + 500;
        let (mut checkpointed, mut resynced, mut trades_gap) = (false, false, false);
        let mut open = DAY + 1_000;
        while open < GOLDEN_END {
            let batch = open;
            open += 100;
            if lcg.below(3) != 0 {
                continue;
            }
            let in_book_gap = batch >= BOOK_GAP.0 && batch < BOOK_GAP.1;
            if !checkpointed && batch >= CHECKPOINT {
                checkpointed = true;
                events.push(book.snapshot(batch, id + 1, 70));
                last_book_event = batch;
            }
            if !resynced && batch >= BOOK_GAP.1 {
                resynced = true;
                events.push(crate::event::samples::gap(
                    Stream::OrderBook,
                    last_book_event,
                    batch,
                    GapReason::Disconnected,
                ));
                events.push(book.snapshot(batch, id + 1, 100));
                last_book_event = batch;
            }
            if !trades_gap && batch >= TRADES_GAP.1 {
                trades_gap = true;
                events.push(crate::event::samples::gap(
                    Stream::Trades,
                    TRADES_GAP.0,
                    TRADES_GAP.1,
                    GapReason::Disconnected,
                ));
            }
            let mut time = batch;
            let (mut bids, mut asks): (Units, Units) = (Vec::new(), Vec::new());
            for _ in 0..1 + lcg.below(3) {
                time += lcg.below(30);
                let bid = lcg.below(2) == 0;
                let best_bid = *book.bids.last_key_value().unwrap().0;
                let best_ask = *book.asks.first_key_value().unwrap().0;
                let (side, touched, best) = if bid {
                    (&mut book.bids, &mut bids, best_bid)
                } else {
                    (&mut book.asks, &mut asks, best_ask)
                };
                // A thin side only grows.
                let action = if side.len() < 20 { 0 } else { lcg.below(10) };
                let (price, qty) = match action {
                    0..=3 => {
                        // Within 40 ticks of the opposite best: the spread
                        // stays tight.
                        let k = 1 + lcg.below(40);
                        let price = if bid {
                            best_ask - k * tick
                        } else {
                            best_bid + k * tick
                        };
                        let qty = side.get(&price).copied().unwrap_or(0)
                            + (1 + lcg.below(20)) * SCALE / 100;
                        (price, qty)
                    }
                    4..=6 => {
                        let reach = u64::try_from(side.len().min(40)).unwrap();
                        let k = usize::try_from(lcg.below(reach)).unwrap();
                        let (&price, &qty) = if bid {
                            side.iter().rev().nth(k).unwrap()
                        } else {
                            side.iter().nth(k).unwrap()
                        };
                        (price, qty * lcg.below(3) / 3)
                    }
                    _ => {
                        let qty = side[&best];
                        let fill = qty.min((1 + lcg.below(30)) * SCALE / 100);
                        let in_trades_gap = time >= TRADES_GAP.0 && time < TRADES_GAP.1;
                        if !in_trades_gap {
                            trade_id += 1;
                            events.push(if bid {
                                sell(time, trade_id, best, fill)
                            } else {
                                buy(time, trade_id, best, fill)
                            });
                        }
                        (best, qty - fill)
                    }
                };
                if qty == 0 {
                    side.remove(&price);
                } else {
                    side.insert(price, qty);
                }
                touched.push((price, qty));
            }
            let first = id + 1;
            id += 1 + lcg.below(4).unsigned_abs();
            if !in_book_gap {
                events.push(MarketEvent::BookUpdate(BookUpdate {
                    time: t(time),
                    first_update_id: first,
                    last_update_id: id,
                    prev_update_id: first - 1,
                    bids: levels(&bids),
                    asks: levels(&asks),
                }));
                last_book_event = time;
            }
        }
        events.sort();
        events
    }

    fn show<T: fmt::Display>(value: &FeatureValue<T>) -> String {
        match value {
            FeatureValue::Ready(value) => value.to_string(),
            FeatureValue::WarmingUp { observed, required } => {
                format!("warming {observed}/{required}")
            }
            FeatureValue::Unavailable { reason } => format!("unavailable {reason:?}"),
        }
    }

    /// One line per change of `pick` over the golden tape: the event time,
    /// then the value. `per_minute` samples the value at the last event of
    /// each minute only; `ready_only` skips values that are not ready.
    fn golden_lines<T: fmt::Display + PartialEq>(
        pick: impl Fn(&BookState) -> FeatureValue<T>,
        per_minute: bool,
        ready_only: bool,
    ) -> Vec<String> {
        let events = golden_tape();
        let mut engine = MarketStateEngine::new();
        let mut last = None;
        let mut lines = Vec::new();
        for (index, event) in events.iter().enumerate() {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
            let minute = |event: &MarketEvent| event.time().as_millis().div_euclid(MINUTE);
            if per_minute && events.get(index + 1).map(minute) == Some(minute(event)) {
                continue;
            }
            let value = pick(&engine.state().book);
            if last.as_ref() != Some(&value) && (!ready_only || value.is_ready()) {
                lines.push(format!("{} {}", event.time(), show(&value)));
            }
            last = Some(value);
        }
        lines
    }

    fn golden_window_lines(timeframe: Timeframe) -> Vec<String> {
        golden_lines(|book| *book.windows.get(timeframe).unwrap(), false, true)
    }

    #[test]
    fn golden_book_l2_v1() {
        assert_eq!(golden_lines(|book| book.l2.clone(), true, false), GOLDEN_L2);
    }

    #[test]
    fn golden_book_depth_v1() {
        assert_eq!(golden_lines(|book| book.depth, true, false), GOLDEN_DEPTH);
    }

    #[test]
    fn golden_book_clusters_v1() {
        assert_eq!(
            golden_lines(|book| book.clusters, true, false),
            GOLDEN_CLUSTERS
        );
    }

    #[test]
    fn golden_book_liquidity_window_5m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M5), GOLDEN_WINDOW_5M);
    }

    #[test]
    fn golden_book_liquidity_window_15m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M15), GOLDEN_WINDOW_15M);
    }

    #[test]
    fn golden_book_liquidity_window_1h_v1() {
        assert_eq!(golden_window_lines(Timeframe::H1), GOLDEN_WINDOW_1H);
    }

    const GOLDEN_L2: [&str; 75] = [
        "86459825ms id=5511 window=62450.50000000..62550.00000000 bids=93 asks=87 best=62500.00000000@0.07000000/62503.50000000@2.93000000",
        "86519933ms id=6012 window=62450.50000000..62550.00000000 bids=88 asks=85 best=62500.00000000@0.08666666/62505.00000000@0.01000000",
        "86579968ms id=6502 window=62450.50000000..62550.00000000 bids=77 asks=64 best=62507.00000000@0.02000000/62511.50000000@0.00222222",
        "86639815ms id=7029 window=62450.50000000..62550.00000000 bids=62 asks=67 best=62500.00000000@0.06000000/62505.00000000@0.02000000",
        "86699743ms id=7552 window=62450.50000000..62550.00000000 bids=61 asks=42 best=62510.50000000@0.06000000/62518.00000000@0.32000000",
        "86759933ms id=8027 window=62450.50000000..62550.00000000 bids=49 asks=36 best=62506.00000000@0.22000000/62508.00000000@0.06000000",
        "86819926ms id=8536 window=62450.50000000..62550.00000000 bids=50 asks=22 best=62519.00000000@0.14000000/62523.50000000@0.48000000",
        "86879739ms id=9012 window=62450.50000000..62550.00000000 bids=45 asks=20 best=62522.00000000@0.15000000/62526.00000000@0.03000000",
        "86939340ms id=9462 window=62450.50000000..62550.00000000 bids=36 asks=24 best=62512.50000000@0.10000000/62518.50000000@0.07000000",
        "86999405ms id=9957 window=62450.50000000..62550.00000000 bids=26 asks=22 best=62507.00000000@0.03000000/62521.00000000@0.06000000",
        "87059802ms id=10459 window=62450.50000000..62550.00000000 bids=26 asks=29 best=62507.00000000@0.13000000/62515.00000000@0.10000000",
        "87119856ms id=11009 window=62450.50000000..62550.00000000 bids=22 asks=20 best=62504.50000000@0.07666666/62525.00000000@0.14000000",
        "87179920ms id=11525 window=62450.50000000..62550.00000000 bids=27 asks=19 best=62512.50000000@0.14000000/62514.50000000@0.38000000",
        "87239837ms id=12008 window=62450.50000000..62550.00000000 bids=22 asks=24 best=62513.50000000@0.02000000/62515.00000000@0.17000000",
        "87299974ms id=12517 window=62450.50000000..62550.00000000 bids=20 asks=20 best=62505.50000000@0.14000000/62506.00000000@0.08000000",
        "87359443ms id=13042 window=62450.50000000..62550.00000000 bids=20 asks=24 best=62509.00000000@0.17000000/62513.00000000@0.03000000",
        "87419925ms id=13570 window=62450.50000000..62550.00000000 bids=19 asks=21 best=62502.00000000@0.07363054/62513.00000000@0.18000000",
        "87479846ms id=14032 window=62450.50000000..62550.00000000 bids=19 asks=23 best=62505.50000000@0.04000000/62510.00000000@0.07000000",
        "87539751ms id=14469 window=62450.50000000..62550.00000000 bids=20 asks=25 best=62514.00000000@0.05333333/62515.00000000@0.03333333",
        "87599724ms id=14997 window=62450.50000000..62550.00000000 bids=21 asks=21 best=62510.50000000@0.15333333/62518.50000000@0.11000000",
        "87659633ms id=15457 window=62490.50000000..62539.50000000 bids=26 asks=32 best=62506.00000000@0.08000000/62507.00000000@0.11000000",
        "87719851ms id=15927 window=62490.50000000..62539.50000000 bids=22 asks=27 best=62499.50000000@0.08000000/62512.50000000@0.22000000",
        "87779913ms id=16393 window=62490.50000000..62539.50000000 bids=23 asks=20 best=62510.00000000@0.01000000/62511.50000000@0.07000000",
        "87839961ms id=16894 window=62490.50000000..62539.50000000 bids=20 asks=22 best=62499.00000000@0.11000000/62505.50000000@0.02000000",
        "87899950ms id=17384 window=62490.50000000..62539.50000000 bids=19 asks=23 best=62499.00000000@0.17000000/62502.50000000@0.07000000",
        "87959915ms id=17878 window=62490.50000000..62539.50000000 bids=23 asks=25 best=62499.00000000@0.24000000/62504.50000000@0.10000000",
        "88019147ms id=18308 window=62490.50000000..62539.50000000 bids=25 asks=20 best=62500.00000000@0.02000000/62505.50000000@0.03703703",
        "88079918ms id=18838 window=62490.50000000..62539.50000000 bids=21 asks=27 best=62498.50000000@0.01000000/62501.50000000@0.04000000",
        "88139929ms id=19350 window=62490.50000000..62539.50000000 bids=28 asks=19 best=62499.00000000@0.08000000/62505.50000000@0.19000000",
        "88199818ms id=19831 window=62490.50000000..62539.50000000 bids=28 asks=21 best=62496.50000000@0.13333333/62501.50000000@0.08000000",
        "88259827ms id=20312 window=62490.50000000..62539.50000000 bids=21 asks=20 best=62486.50000000@0.19000000/62497.00000000@0.12000000",
        "88318625ms id=20819 window=62490.50000000..62539.50000000 bids=22 asks=24 best=62486.00000000@0.12000000/62489.00000000@0.11000000",
        "88379904ms id=21289 window=62490.50000000..62539.50000000 bids=20 asks=23 best=62482.00000000@0.24555555/62485.00000000@0.04000000",
        "88439327ms id=21780 window=62490.50000000..62539.50000000 bids=27 asks=24 best=62489.50000000@0.10666666/62492.50000000@0.07000000",
        "88499919ms id=22245 window=62490.50000000..62539.50000000 bids=20 asks=21 best=62488.00000000@0.06000000/62493.50000000@0.18000000",
        "88559914ms id=22781 window=62490.50000000..62539.50000000 bids=20 asks=31 best=62482.00000000@0.23555555/62490.00000000@0.16000000",
        "88619802ms id=23260 window=62490.50000000..62539.50000000 bids=23 asks=22 best=62483.00000000@0.30000000/62487.00000000@0.04000000",
        "88679928ms id=23795 window=62490.50000000..62539.50000000 bids=21 asks=23 best=62486.00000000@0.10000000/62488.00000000@0.16000000",
        "88739528ms id=24267 window=62490.50000000..62539.50000000 bids=25 asks=19 best=62495.00000000@0.19000000/62498.50000000@0.03000000",
        "88799932ms id=24817 window=62490.50000000..62539.50000000 bids=23 asks=20 best=62485.50000000@0.10000000/62493.00000000@0.08000000",
        "88859647ms id=25357 window=62467.50000000..62515.50000000 bids=21 asks=19 best=62484.00000000@0.12000000/62492.00000000@0.06000000",
        "88919736ms id=25859 window=62467.50000000..62515.50000000 bids=24 asks=19 best=62487.00000000@0.05000000/62492.00000000@0.03000000",
        "88979615ms id=26431 window=62467.50000000..62515.50000000 bids=20 asks=22 best=62483.00000000@0.22000000/62488.00000000@0.36000000",
        "89039543ms id=26933 window=62467.50000000..62515.50000000 bids=24 asks=20 best=62492.00000000@0.02000000/62496.50000000@0.34814814",
        "89099835ms id=27437 window=62467.50000000..62515.50000000 bids=23 asks=19 best=62489.50000000@0.11000000/62496.00000000@0.20666666",
        "89159865ms id=27907 window=62467.50000000..62515.50000000 bids=26 asks=20 best=62490.50000000@0.06000000/62496.50000000@0.11000000",
        "89219836ms id=28431 window=62467.50000000..62515.50000000 bids=20 asks=32 best=62478.00000000@0.01185184/62482.00000000@0.19000000",
        "89279920ms id=28889 window=62467.50000000..62515.50000000 bids=20 asks=24 best=62481.00000000@0.15000000/62487.50000000@0.01000000",
        "89339904ms id=29366 window=62467.50000000..62515.50000000 bids=21 asks=24 best=62486.00000000@0.07000000/62493.00000000@0.20000000",
        "89399710ms id=29848 window=62467.50000000..62515.50000000 bids=20 asks=26 best=62483.50000000@0.20000000/62490.00000000@0.12666666",
        "89459950ms id=30348 window=62467.50000000..62515.50000000 bids=22 asks=23 best=62485.50000000@0.16000000/62492.50000000@0.08000000",
        "89519022ms id=30914 window=62467.50000000..62515.50000000 bids=23 asks=21 best=62487.50000000@0.04000000/62494.00000000@0.08000000",
        "89579918ms id=31408 window=62467.50000000..62515.50000000 bids=19 asks=24 best=62484.50000000@0.02000000/62486.50000000@0.06000000",
        "89639949ms id=31891 window=62467.50000000..62515.50000000 bids=21 asks=22 best=62478.00000000@0.15000000/62489.00000000@0.06000000",
        "89699933ms id=32424 window=62467.50000000..62515.50000000 bids=25 asks=26 best=62484.00000000@0.20000000/62490.00000000@0.08000000",
        "89759943ms id=32921 window=62467.50000000..62515.50000000 bids=23 asks=22 best=62481.50000000@0.10000000/62482.00000000@0.02000000",
        "89819635ms id=33422 window=62467.50000000..62515.50000000 bids=19 asks=19 best=62476.50000000@0.17333333/62478.50000000@0.14000000",
        "89879820ms id=33948 window=62467.50000000..62515.50000000 bids=21 asks=21 best=62476.50000000@0.02000000/62486.50000000@0.05333333",
        "89939831ms id=34435 window=62467.50000000..62515.50000000 bids=23 asks=27 best=62475.00000000@0.07000000/62476.00000000@0.12000000",
        "89999808ms id=34934 window=62467.50000000..62515.50000000 bids=23 asks=30 best=62471.50000000@0.14000000/62473.00000000@0.11333333",
        "90059832ms id=35414 window=62467.50000000..62515.50000000 bids=19 asks=21 best=62465.00000000@0.02000000/62469.50000000@0.11000000",
        "90119724ms id=35899 window=62467.50000000..62515.50000000 bids=21 asks=20 best=62473.00000000@0.01000000/62475.50000000@0.14000000",
        "90179718ms id=36409 window=62467.50000000..62515.50000000 bids=27 asks=22 best=62482.00000000@0.05000000/62484.00000000@0.92555555",
        "90239214ms id=36874 window=62467.50000000..62515.50000000 bids=20 asks=25 best=62473.50000000@0.09000000/62479.50000000@0.10000000",
        "90299654ms id=37359 window=62467.50000000..62515.50000000 bids=19 asks=22 best=62465.50000000@0.01000000/62476.50000000@0.09000000",
        "90358010ms id=37859 window=62467.50000000..62515.50000000 bids=30 asks=20 best=62474.00000000@0.17000000/62475.50000000@0.05000000",
        "90419741ms id=38362 window=62467.50000000..62515.50000000 bids=22 asks=19 best=62468.50000000@0.02000000/62480.00000000@0.12000000",
        "90479825ms id=38826 window=62467.50000000..62515.50000000 bids=23 asks=22 best=62476.00000000@0.12000000/62478.00000000@0.17000000",
        "90539840ms id=39279 window=62467.50000000..62515.50000000 bids=22 asks=21 best=62472.00000000@0.08000000/62476.00000000@0.11000000",
        "90599914ms id=39822 window=62467.50000000..62515.50000000 bids=24 asks=28 best=62469.50000000@0.05000000/62476.50000000@0.13000000",
        "90659727ms id=40267 window=62467.50000000..62515.50000000 bids=20 asks=22 best=62466.50000000@0.03000000/62475.00000000@0.20000000",
        "90718778ms id=40779 window=62467.50000000..62515.50000000 bids=21 asks=29 best=62464.50000000@0.43000000/62467.00000000@0.20000000",
        "90779835ms id=41305 window=62467.50000000..62515.50000000 bids=20 asks=30 best=62466.00000000@0.07000000/62466.50000000@0.14000000",
        "90839251ms id=41775 window=62467.50000000..62515.50000000 bids=19 asks=22 best=62467.00000000@0.02000000/62473.00000000@0.29000000",
        "90899935ms id=42274 window=62467.50000000..62515.50000000 bids=26 asks=20 best=62470.50000000@0.02000000/62472.50000000@0.09000000",
    ];

    const GOLDEN_DEPTH: [&str; 75] = [
        "86459825ms best=62500.00000000@0.07000000/62503.50000000@2.93000000 | 1bps bid=8.47629629/8 ask=10.85518517/9 imb=-0.12305776383058435 | 2bps bid=18.08925922/18 ask=21.08407403/19 imb=-0.0764503441891813 | 5bps bid=77.64555549/53 ask=67.62148140/53 imb=0.06900446449933781 | book.depth@1",
        "86519933ms best=62500.00000000@0.08666666/62505.00000000@0.01000000 | 1bps bid=2.29666666/4 ask=4.64666665/6 imb=-0.3384541523615838 | 2bps bid=10.05618652/14 ask=11.23592586/19 imb=-0.05540734141099813 | 5bps bid=47.72133048/46 ask=45.87991758/52 imb=0.019672952424946544 | book.depth@1",
        "86579968ms best=62507.00000000@0.02000000/62511.50000000@0.00222222 | 1bps bid=0.10000000/2 ask=2.31703700/7 imb=-0.9172540594124128 | 2bps bid=0.49000000/5 ask=3.40135798/12 imb=-0.7481598955848313 | 5bps bid=12.76362586/30 ask=45.61680372/45 imb=-0.562742996177864 | book.depth@1",
        "86639815ms best=62500.00000000@0.06000000/62505.00000000@0.02000000 | 1bps bid=0.30000000/4 ask=0.55000000/3 imb=-0.29411764705882354 | 2bps bid=3.49255599/11 ask=3.46481479/12 imb=0.003987310850205974 | 5bps bid=6.51284401/26 ask=14.19209867/36 imb=-0.37088992607633725 | book.depth@1",
        "86699743ms best=62510.50000000@0.06000000/62518.00000000@0.32000000 | 1bps bid=0.21000000/4 ask=1.44000000/5 imb=-0.7454545454545455 | 2bps bid=1.28333333/10 ask=2.44333331/13 imb=-0.3112701220842227 | 5bps bid=3.97470504/27 ask=15.40016451/33 imb=-0.5897051043628833 | book.depth@1",
        "86759933ms best=62506.00000000@0.22000000/62508.00000000@0.06000000 | 1bps bid=1.89382715/8 ask=0.40000000/3 imb=0.651237888609 | 2bps bid=2.90096019/17 ask=0.86000000/6 imb=0.5426699797106866 | 5bps bid=3.51318239/23 ask=4.28106531/25 imb=-0.09851918357688325 | book.depth@1",
        "86819926ms best=62519.00000000@0.14000000/62523.50000000@0.48000000 | 1bps bid=0.14000000/1 ask=1.82966619/7 imb=-0.8578439324279613 | 2bps bid=0.83000000/6 ask=2.51954271/14 imb=-0.504409961681008 | 5bps unavailable OutOfRange | book.depth@1",
        "86879739ms best=62522.00000000@0.15000000/62526.00000000@0.03000000 | 1bps bid=0.15000000/1 ask=0.15000000/2 imb=0 | 2bps bid=0.22000000/3 ask=1.21444442/12 imb=-0.6932610327279185 | 5bps unavailable OutOfRange | book.depth@1",
        "86939340ms best=62512.50000000@0.10000000/62518.50000000@0.07000000 | 1bps bid=0.61518518/4 ask=0.07000000/1 imb=0.7956756741294375 | 2bps bid=2.87444442/12 ask=0.56555555/5 imb=0.6711886308533892 | 5bps bid=3.44591215/21 ask=3.14962957/23 imb=0.04492164443468944 | book.depth@1",
        "86999405ms best=62507.00000000@0.03000000/62521.00000000@0.06000000 | 1bps bid=0.00000000/0 ask=0.00000000/0 imb=- | 2bps bid=0.75629629/6 ask=0.10000000/2 imb=0.7664359844417871 | 5bps bid=1.47526454/19 ask=2.66816182/22 imb=-0.28790116593263165 | book.depth@1",
        "87059802ms best=62507.00000000@0.13000000/62515.00000000@0.10000000 | 1bps bid=0.44333333/4 ask=0.10000000/1 imb=0.6319018382325267 | 2bps bid=1.24432096/12 ask=0.48666665/5 imb=0.4377005968286509 | 5bps bid=1.88900435/21 ask=3.47401914/28 imb=-0.29554500235836184 | book.depth@1",
        "87119856ms best=62504.50000000@0.07666666/62525.00000000@0.14000000 | 1bps bid=0.00000000/0 ask=0.00000000/0 imb=- | 2bps bid=0.59888887/4 ask=0.32000000/2 imb=0.3035066362268595 | 5bps bid=2.47583106/18 ask=2.01733876/20 imb=0.10204205902905313 | book.depth@1",
        "87179920ms best=62512.50000000@0.14000000/62514.50000000@0.38000000 | 1bps bid=0.18000000/2 ask=1.50666666/7 imb=-0.786561263978503 | 2bps bid=0.79000000/7 ask=2.48111109/12 imb=-0.5169836925348812 | 5bps bid=3.13307804/24 ask=2.65299492/19 imb=0.0829721856808387 | book.depth@1",
        "87239837ms best=62513.50000000@0.02000000/62515.00000000@0.17000000 | 1bps bid=0.16000000/3 ask=1.17222221/8 imb=-0.7597998309906573 | 2bps bid=1.86444443/9 ask=2.67439411/17 imb=-0.17844866541562415 | 5bps bid=3.10117130/22 ask=3.19550519/24 imb=-0.01498153671223468 | book.depth@1",
        "87299974ms best=62505.50000000@0.14000000/62506.00000000@0.08000000 | 1bps bid=1.69617282/10 ask=0.08000000/1 imb=0.909918675593741 | 2bps bid=2.98999996/18 ask=0.74000000/5 imb=0.6032171539218998 | 5bps bid=3.17999996/20 ask=3.43245859/20 imb=-0.0381792381897048 | book.depth@1",
        "87359443ms best=62509.00000000@0.17000000/62513.00000000@0.03000000 | 1bps bid=1.20666666/6 ask=0.48000000/5 imb=0.43083003727600805 | 2bps bid=2.38267486/12 ask=2.29981491/14 imb=0.01769570336936369 | 5bps bid=3.03144027/20 ask=3.91790816/24 imb=-0.12756129569977542 | book.depth@1",
        "87419925ms best=62502.00000000@0.07363054/62513.00000000@0.18000000 | 1bps bid=0.59066757/2 ask=0.18000000/1 imb=0.5328725198596329 | 2bps bid=1.96017220/12 ask=0.97925925/6 imb=0.3337083945264313 | 5bps bid=2.84239440/19 ask=4.06208148/21 imb=-0.17665165339095948 | book.depth@1",
        "87479846ms best=62505.50000000@0.04000000/62510.00000000@0.07000000 | 1bps bid=1.05570644/7 ask=0.25000000/3 imb=0.617065532739503 | 2bps bid=2.82613164/16 ask=1.10000000/6 imb=0.43965200311011476 | 5bps bid=3.22724275/19 ask=3.80674433/23 imb=-0.08238593182061972 | book.depth@1",
        "87539751ms best=62514.00000000@0.05333333/62515.00000000@0.03333333 | 1bps bid=0.05333333/1 ask=0.27999999/5 imb=-0.6800000072000003 | 2bps bid=2.24851848/11 ask=2.07319001/13 imb=0.040569249500676065 | 5bps bid=3.34185180/20 ask=3.12616208/25 imb=0.0333471331388052 | book.depth@1",
        "87599724ms best=62510.50000000@0.15333333/62518.50000000@0.11000000 | 1bps bid=0.96333332/3 ask=0.11000000/1 imb=0.795031053354423 | 2bps bid=3.02530860/13 ask=1.06713304/8 imb=0.4784858850180207 | 5bps bid=3.68604930/21 ask=2.89034592/21 imb=0.12099385048820105 | book.depth@1",
        "87659633ms best=62506.00000000@0.08000000/62507.00000000@0.11000000 | 1bps bid=0.77807953/9 ask=0.54333332/5 imb=0.17764789407035053 | 2bps bid=1.15823040/18 ask=1.44999997/13 imb=-0.1118649538614183 | 5bps unavailable OutOfRange | book.depth@1",
        "87719851ms best=62499.50000000@0.08000000/62512.50000000@0.22000000 | 1bps bid=0.00000000/0 ask=0.00000000/0 imb=- | 2bps bid=2.08949244/9 ask=1.49814814/9 imb=0.16482818911586733 | 5bps unavailable OutOfRange | book.depth@1",
        "87779913ms best=62510.00000000@0.01000000/62511.50000000@0.07000000 | 1bps bid=0.29000000/3 ask=1.14333332/7 imb=-0.5953488334451055 | 2bps bid=0.45000000/4 ask=1.71814811/14 imb=-0.584899206908886 | 5bps unavailable OutOfRange | book.depth@1",
        "87839961ms best=62499.00000000@0.11000000/62505.50000000@0.02000000 | 1bps bid=0.54000000/5 ask=0.41000000/4 imb=0.1368421052631579 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "87899950ms best=62499.00000000@0.17000000/62502.50000000@0.07000000 | 1bps bid=0.49000000/2 ask=0.58407407/5 imb=-0.0875862034356718 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "87959915ms best=62499.00000000@0.24000000/62504.50000000@0.10000000 | 1bps bid=0.24000000/1 ask=0.57333333/5 imb=-0.4098360631550658 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88019147ms best=62500.00000000@0.02000000/62505.50000000@0.03703703 | 1bps bid=0.02000000/1 ask=0.72037036/4 imb=-0.94597298573649 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88079918ms best=62498.50000000@0.01000000/62501.50000000@0.04000000 | 1bps bid=0.39000000/4 ask=0.52000000/6 imb=-0.14285714285714285 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88139929ms best=62499.00000000@0.08000000/62505.50000000@0.19000000 | 1bps bid=0.08000000/1 ask=1.37370369/5 imb=-0.889936304694941 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88199818ms best=62496.50000000@0.13333333/62501.50000000@0.08000000 | 1bps bid=0.60333332/5 ask=0.08000000/1 imb=0.7658536539678762 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88259827ms best=62486.50000000@0.19000000/62497.00000000@0.12000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88318625ms best=62486.00000000@0.12000000/62489.00000000@0.11000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88379904ms best=62482.00000000@0.24555555/62485.00000000@0.04000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88439327ms best=62489.50000000@0.10666666/62492.50000000@0.07000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88499919ms best=62488.00000000@0.06000000/62493.50000000@0.18000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88559914ms best=62482.00000000@0.23555555/62490.00000000@0.16000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88619802ms best=62483.00000000@0.30000000/62487.00000000@0.04000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88679928ms best=62486.00000000@0.10000000/62488.00000000@0.16000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88739528ms best=62495.00000000@0.19000000/62498.50000000@0.03000000 | 1bps bid=0.25000000/2 ask=0.89386372/6 imb=-0.5628849912295496 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88799932ms best=62485.50000000@0.10000000/62493.00000000@0.08000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "88859647ms best=62484.00000000@0.12000000/62492.00000000@0.06000000 | 1bps bid=0.33000000/3 ask=0.06000000/1 imb=0.6923076923076923 | 2bps bid=0.93199257/9 ask=0.57880657/5 imb=0.23377429245822842 | 5bps unavailable OutOfRange | book.depth@1",
        "88919736ms best=62487.00000000@0.05000000/62492.00000000@0.03000000 | 1bps bid=0.08333333/2 ask=1.10222221/8 imb=-0.8594189353625727 | 2bps bid=0.47333333/6 ask=2.03901232/15 imb=-0.6231941014963447 | 5bps unavailable OutOfRange | book.depth@1",
        "88979615ms best=62483.00000000@0.22000000/62488.00000000@0.36000000 | 1bps bid=0.25666666/2 ask=1.14555554/6 imb=-0.6339144252601335 | 2bps bid=0.38888888/4 ask=2.23777775/14 imb=-0.7038917116025493 | 5bps unavailable OutOfRange | book.depth@1",
        "89039543ms best=62492.00000000@0.02000000/62496.50000000@0.34814814 | 1bps bid=0.04000000/2 ask=1.63703701/7 imb=-0.9522968190189196 | 2bps bid=0.63000000/7 ask=3.21185180/17 imb=-0.6720331585929473 | 5bps unavailable OutOfRange | book.depth@1",
        "89099835ms best=62489.50000000@0.11000000/62496.00000000@0.20666666 | 1bps bid=0.11000000/1 ask=0.84999999/5 imb=-0.7708333309461806 | 2bps bid=0.82333333/8 ask=2.14584358/13 imb=-0.445413085877729 | 5bps unavailable OutOfRange | book.depth@1",
        "89159865ms best=62490.50000000@0.06000000/62496.50000000@0.11000000 | 1bps bid=0.89000000/5 ask=0.30000000/2 imb=0.4957983193277311 | 2bps bid=2.42851851/16 ask=1.52884773/10 imb=0.22734079320391634 | 5bps unavailable OutOfRange | book.depth@1",
        "89219836ms best=62478.00000000@0.01185184/62482.00000000@0.19000000 | 1bps bid=1.19185184/9 ask=0.19000000/1 imb=0.7250066982578972 | 2bps bid=2.28111109/16 ask=0.81000000/8 imb=0.4759166031784383 | 5bps unavailable OutOfRange | book.depth@1",
        "89279920ms best=62481.00000000@0.15000000/62487.50000000@0.01000000 | 1bps bid=0.28000000/2 ask=0.01000000/1 imb=0.9310344827586207 | 2bps bid=1.48666664/11 ask=2.10485596/12 imb=-0.17212458025462515 | 5bps unavailable OutOfRange | book.depth@1",
        "89339904ms best=62486.00000000@0.07000000/62493.00000000@0.20000000 | 1bps bid=0.16000000/2 ask=0.20000000/1 imb=-0.1111111111111111 | 2bps bid=0.85333333/8 ask=1.37999999/8 imb=-0.23582089394519937 | 5bps unavailable OutOfRange | book.depth@1",
        "89399710ms best=62483.50000000@0.20000000/62490.00000000@0.12666666 | 1bps bid=0.20000000/1 ask=0.84666665/5 imb=-0.617834388819019 | 2bps bid=2.46148146/7 ask=2.09703700/15 imb=0.07994800573868906 | 5bps unavailable OutOfRange | book.depth@1",
        "89459950ms best=62485.50000000@0.16000000/62492.50000000@0.08000000 | 1bps bid=0.16000000/1 ask=0.38925925/3 imb=-0.4173971580815435 | 2bps bid=0.70218105/7 ask=1.81353905/13 imb=-0.44176536173479714 | 5bps unavailable OutOfRange | book.depth@1",
        "89519022ms best=62487.50000000@0.04000000/62494.00000000@0.08000000 | 1bps bid=0.79666666/5 ask=0.08000000/1 imb=0.8174904929086729 | 2bps bid=1.65333332/11 ask=1.09333333/7 imb=0.20388349274201148 | 5bps unavailable OutOfRange | book.depth@1",
        "89579918ms best=62484.50000000@0.02000000/62486.50000000@0.06000000 | 1bps bid=1.10888886/8 ask=0.37000000/3 imb=0.4996243328251184 | 2bps bid=2.05502053/16 ask=0.61000000/5 imb=0.5422174102351099 | 5bps unavailable OutOfRange | book.depth@1",
        "89639949ms best=62478.00000000@0.15000000/62489.00000000@0.06000000 | 1bps bid=0.43218106/2 ask=0.15000000/2 imb=0.4846963932492067 | 2bps bid=1.29037036/8 ask=1.12000000/6 imb=0.07068223324817187 | 5bps unavailable OutOfRange | book.depth@1",
        "89699933ms best=62484.00000000@0.20000000/62490.00000000@0.08000000 | 1bps bid=0.31000000/2 ask=0.71000000/5 imb=-0.39215686274509803 | 2bps bid=1.12666666/7 ask=1.91481480/15 imb=-0.2591329752836961 | 5bps unavailable OutOfRange | book.depth@1",
        "89759943ms best=62481.50000000@0.10000000/62482.00000000@0.02000000 | 1bps bid=0.23777777/4 ask=0.20000000/2 imb=0.0862944000103066 | 2bps bid=2.76098762/13 ask=0.84333333/7 imb=0.5320431550359022 | 5bps unavailable OutOfRange | book.depth@1",
        "89819635ms best=62476.50000000@0.17333333/62478.50000000@0.14000000 | 1bps bid=1.31296292/8 ask=0.14000000/1 imb=0.8072903333279834 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "89879820ms best=62476.50000000@0.02000000/62486.50000000@0.05333333 | 1bps bid=0.14666666/2 ask=0.14407406/3 imb=0.008917223566069452 | 2bps bid=1.35259257/9 ask=1.31074070/11 imb=0.015714094241011 | 5bps unavailable OutOfRange | book.depth@1",
        "89939831ms best=62475.00000000@0.07000000/62476.00000000@0.12000000 | 1bps bid=1.43333333/9 ask=0.12000000/1 imb=0.8454935619002008 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "89999808ms best=62471.50000000@0.14000000/62473.00000000@0.11333333 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90059832ms best=62465.00000000@0.02000000/62469.50000000@0.11000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90119724ms best=62473.00000000@0.01000000/62475.50000000@0.14000000 | 1bps bid=0.17000000/2 ask=0.30000000/3 imb=-0.2765957446808511 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90179718ms best=62482.00000000@0.05000000/62484.00000000@0.92555555 | 1bps bid=0.27000000/3 ask=2.73555552/9 imb=-0.8203327150649341 | 2bps bid=0.49037036/6 ask=4.30777772/19 imb=-0.7956001558001102 | 5bps unavailable OutOfRange | book.depth@1",
        "90239214ms best=62473.50000000@0.09000000/62479.50000000@0.10000000 | 1bps bid=0.41751713/5 ask=0.37000000/4 imb=0.06033790020542156 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90299654ms best=62465.50000000@0.01000000/62476.50000000@0.09000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90358010ms best=62474.00000000@0.17000000/62475.50000000@0.05000000 | 1bps bid=0.37000000/3 ask=0.26000000/2 imb=0.1746031746031746 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90419741ms best=62468.50000000@0.02000000/62480.00000000@0.12000000 | 1bps bid=0.02000000/1 ask=0.12000000/1 imb=-0.7142857142857143 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90479825ms best=62476.00000000@0.12000000/62478.00000000@0.17000000 | 1bps bid=1.19666666/6 ask=0.42000000/3 imb=0.4804123689913912 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90539840ms best=62472.00000000@0.08000000/62476.00000000@0.11000000 | 1bps bid=0.71333333/5 ask=0.21000000/3 imb=0.5451263521484706 | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90599914ms best=62469.50000000@0.05000000/62476.50000000@0.13000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90659727ms best=62466.50000000@0.03000000/62475.00000000@0.20000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90718778ms best=62464.50000000@0.43000000/62467.00000000@0.20000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90779835ms best=62466.00000000@0.07000000/62466.50000000@0.14000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90839251ms best=62467.00000000@0.02000000/62473.00000000@0.29000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
        "90899935ms best=62470.50000000@0.02000000/62472.50000000@0.09000000 | 1bps unavailable OutOfRange | 2bps unavailable OutOfRange | 5bps unavailable OutOfRange | book.depth@1",
    ];

    const GOLDEN_CLUSTERS: [&str; 66] = [
        "86459825ms bid median=1.30000000 total=77.64555549/53 top=62483.50000000@3.99000000[3.0692307692307694],62476.00000000@3.90000000[3],62478.50000000@3.50000000[2.6923076923076925],62493.50000000@3.42000000[2.6307692307692307],62498.50000000@3.40000000[2.6153846153846154] | ask median=0.90000000 total=67.62148140/53 top=62528.50000000@4.00000000[4.444444444444445],62507.00000000@3.42000000[3.8],62530.50000000@3.10000000[3.4444444444444446],62517.00000000@3.01000000[3.3444444444444446],62524.00000000@3.00000000[3.3333333333333335] | book.clusters@1",
        "86519933ms bid median=0.65000000 total=47.72133048/46 top=62476.00000000@3.90000000[6],62475.00000000@3.20000000[4.923076923076923],62485.00000000@3.18000000[4.892307692307693],62472.50000000@3.00000000[4.615384615384615],62481.50000000@2.98000000[4.584615384615384] | ask median=0.50000000 total=45.87991758/52 top=62528.50000000@4.00000000[8],62507.00000000@3.66000000[7.32],62530.50000000@3.10000000[6.2],62517.00000000@3.06000000[6.12],62531.00000000@2.80000000[5.6] | book.clusters@1",
        "86579968ms bid median=0.20000000 total=12.76362586/30 top=62496.00000000@1.96666666[9.8333333],62485.50000000@1.60333333[8.01666665],62492.00000000@1.46888888[7.3444444],62492.50000000@1.15444444[5.7722222],62488.50000000@0.84444444[4.2222222] | ask median=0.70000000 total=45.61680372/45 top=62535.50000000@3.50000000[5],62531.00000000@2.80000000[4],62536.50000000@2.80000000[4],62539.00000000@2.60000000[3.7142857142857144],62539.50000000@2.60000000[3.7142857142857144] | book.clusters@1",
        "86639815ms bid median=0.17777777 total=6.51284401/26 top=62496.00000000@1.22666666[6.900000264375011],62482.00000000@0.62000000[3.4875001525781317],62493.00000000@0.47493827[2.671527885629345],62494.00000000@0.46008687[2.587988756974508],62492.00000000@0.37000000[2.0812500910546916] | ask median=0.19407407 total=14.19209867/36 top=62531.00000000@2.80000000[14.427481218897507],62532.00000000@1.66666666[8.5877864054688],62529.50000000@1.60000000[8.244274982227147],62514.00000000@1.36962962[7.057252006927046],62530.00000000@1.00000000[5.152671863891967] | book.clusters@1",
        "86699743ms bid median=0.09000000 total=3.97470504/27 top=62494.50000000@0.95333333[10.592592555555555],62504.00000000@0.40000000[4.444444444444445],62495.50000000@0.28083676[3.1204084444444447],62507.50000000@0.24000000[2.6666666666666665],62488.50000000@0.24000000[2.6666666666666665] | ask median=0.22222222 total=15.40016451/33 top=62544.50000000@3.30000000[14.850000148500001],62529.50000000@1.33666666[6.01500003015],62535.00000000@1.11111110[5],62545.50000000@1.10000000[4.950000049500001],62530.00000000@1.00000000[4.500000045] | book.clusters@1",
        "86759933ms bid median=0.13777777 total=3.51318239/23 top=62504.50000000@0.45000000[3.266129216636327],62502.00000000@0.40000000[2.9032259703434016],62505.00000000@0.34000000[2.4677420747918912],62495.50000000@0.28083676[2.0383314376477424],62499.50000000@0.23000000[1.669354932947456] | ask median=0.12148148 total=4.28106531/25 top=62529.50000000@0.96777777[7.9664634477617495],62523.50000000@0.60000000[4.939024450475908],62535.00000000@0.24691357[2.0325202656404913],62524.50000000@0.24320987[2.002032490878445],62536.00000000@0.22222222[1.829268296698394] | book.clusters@1",
        "86819926ms bid median=0.12000000 total=4.27259257/28 top=62497.50000000@0.77000000[6.416666666666667],62506.00000000@0.39000000[3.25],62493.50000000@0.27296296[2.274691333333333],62500.50000000@0.25000000[2.0833333333333335],62505.00000000@0.24000000[2] | ask unavailable OutOfRange | book.clusters@1",
        "86879739ms bid median=0.17000000 total=5.13266113/25 top=62497.50000000@0.77000000[4.529411764705882],62506.00000000@0.70000000[4.117647058823529],62503.50000000@0.51000000[3],62505.50000000@0.40000000[2.3529411764705883],62509.50000000@0.33000000[1.9411764705882353] | ask unavailable OutOfRange | book.clusters@1",
        "86939340ms bid median=0.10888888 total=3.44591215/21 top=62506.00000000@1.11000000[10.193878383173745],62511.00000000@0.25296296[2.3231294141330134],62510.50000000@0.25222222[2.3163266992919755],62503.00000000@0.25037036[2.2993198203526384],62507.00000000@0.22000000[2.020408328196598] | ask median=0.07000000 total=3.14962957/23 top=62533.50000000@0.41000000[5.857142857142857],62535.50000000@0.38000000[5.428571428571429],62533.00000000@0.35666666[5.095238],62530.00000000@0.28000000[4],62537.00000000@0.27000000[3.857142857142857] | book.clusters@1",
        "86999405ms bid median=0.05666666 total=1.47526454/19 top=62505.00000000@0.21000000[3.705882788927387],62505.50000000@0.18000000[3.1764709619377602],62501.50000000@0.15629629[2.7581701480200174],62498.00000000@0.15333333[2.705882612456778],62504.00000000@0.13000000[2.294117916955049] | ask median=0.06000000 total=2.66816182/22 top=62533.50000000@0.45000000[7.5],62531.00000000@0.25000000[4.166666666666667],62540.00000000@0.24666666[4.111111],62533.00000000@0.24617283[4.1028805],62529.00000000@0.23000000[3.8333333333333335] | book.clusters@1",
        "87059802ms bid median=0.08000000 total=1.88900435/21 top=62498.50000000@0.21111111[2.638888875],62493.00000000@0.19000000[2.375],62491.00000000@0.17000000[2.125],62503.50000000@0.16000000[2],62499.50000000@0.15000000[1.875] | ask median=0.09333333 total=3.47401914/28 top=62533.50000000@0.37000000[3.964285855867352],62525.00000000@0.33000000[3.5357144119898005],62539.50000000@0.32000000[3.4285715510204127],62530.00000000@0.31333333[3.3571429413265337],62531.00000000@0.27000000[2.892857246173473] | book.clusters@1",
        "87119856ms bid median=0.07666666 total=2.47583106/18 top=62500.00000000@0.62000000[8.086957224952803],62501.00000000@0.30000000[3.9130438185255496],62502.50000000@0.26333333[3.434782863894162],62497.00000000@0.18000000[2.3478262911153296],62491.00000000@0.17000000[2.217391497164478] | ask median=0.08000000 total=2.01733876/20 top=62534.00000000@0.33000000[4.125],62530.00000000@0.25333333[3.166666625],62528.00000000@0.19222222[2.40277775],62526.00000000@0.18000000[2.25],62536.00000000@0.15000000[1.875] | book.clusters@1",
        "87179920ms bid median=0.12000000 total=3.13307804/24 top=62494.00000000@0.35000000[2.9166666666666665],62493.50000000@0.32647999[2.720666583333333],62496.50000000@0.26000000[2.1666666666666665],62496.00000000@0.20000000[1.6666666666666667],62498.00000000@0.19696844[1.6414036666666667] | ask median=0.07000000 total=2.65299492/19 top=62521.00000000@0.44000000[6.285714285714286],62524.50000000@0.39555555[5.650793571428571],62514.50000000@0.38000000[5.428571428571429],62516.50000000@0.33000000[4.714285714285714],62518.50000000@0.26000000[3.7142857142857144] | book.clusters@1",
        "87239837ms bid median=0.07000000 total=3.10117130/22 top=62503.50000000@0.65000000[9.285714285714286],62507.50000000@0.57000000[8.142857142857142],62493.50000000@0.32647999[4.663999857142858],62498.00000000@0.20000000[2.857142857142857],62506.00000000@0.19000000[2.7142857142857144] | ask median=0.11777777 total=3.19550519/24 top=62519.00000000@0.42000000[3.56603797134213],62525.00000000@0.38000000[3.2264153074047845],62524.00000000@0.33333333[2.830188837842659],62520.50000000@0.20000000[1.6981133196867286],62521.00000000@0.19724279[1.6747030445558615] | book.clusters@1",
        "87299974ms bid median=0.14074074 total=3.17999996/20 top=62505.00000000@0.38000000[2.7000000142105263],62498.00000000@0.31975308[2.2719297909048937],62501.00000000@0.27037036[1.92105256800554],62502.50000000@0.25000000[1.7763157988227147],62497.50000000@0.24000000[1.7052631668698062] | ask median=0.12411522 total=3.43245859/20 top=62523.00000000@0.57000000[4.592506865797764],62520.50000000@0.44333333[3.571949757652607],62522.50000000@0.40185185[3.237732245892164],62513.00000000@0.20000000[1.611405917823777],62524.50000000@0.19000000[1.530835621932588] | book.clusters@1",
        "87359443ms bid median=0.06000000 total=3.03144027/20 top=62505.00000000@0.74333333[12.388888833333333],62501.50000000@0.42666666[7.111111],62499.50000000@0.31000000[5.166666666666667],62493.50000000@0.22000000[3.6666666666666665],62502.00000000@0.20851851[3.4753085] | ask median=0.11000000 total=3.91790816/24 top=62520.50000000@0.51222222[4.656565636363636],62523.50000000@0.41044217[3.7312924545454544],62525.00000000@0.41000000[3.727272727272727],62518.50000000@0.35000000[3.1818181818181817],62524.00000000@0.27407407[2.4915824545454544] | book.clusters@1",
        "87419925ms bid median=0.10000000 total=2.84239440/19 top=62501.50000000@0.51703703[5.1703703],62500.50000000@0.41987501[4.1987501],62494.00000000@0.30888888[3.0888888],62499.00000000@0.23000000[2.3],62495.00000000@0.20000000[2] | ask median=0.17000000 total=4.06208148/21 top=62524.00000000@0.70407407[4.141612176470589],62523.50000000@0.56681405[3.334200294117647],62529.50000000@0.47012345[2.7654320588235293],62518.50000000@0.29000000[1.7058823529411764],62520.00000000@0.21592592[1.2701524705882352] | book.clusters@1",
        "87479846ms bid median=0.16000000 total=3.22724275/19 top=62496.50000000@0.46000000[2.875],62500.00000000@0.40000000[2.5],62503.50000000@0.33000000[2.0625],62499.00000000@0.28000000[1.75],62502.50000000@0.20000000[1.25] | ask median=0.14000000 total=3.80674433/23 top=62520.00000000@0.53000000[3.7857142857142856],62521.50000000@0.38222222[2.7301587142857144],62524.00000000@0.34666666[2.4761904285714285],62525.00000000@0.31200730[2.2286235714285714],62519.00000000@0.27000000[1.9285714285714286] | book.clusters@1",
        "87539751ms bid median=0.10074073 total=3.34185180/20 top=62503.50000000@0.41666666[4.136029786561999],62504.00000000@0.36000000[3.573529792766044],62502.00000000@0.33000000[3.275735643368874],62500.00000000@0.29000000[2.87867677750598],62500.50000000@0.27000000[2.680147344574533] | ask median=0.11555555 total=3.12616208/25 top=62527.00000000@0.34629629[2.996794961384373],62526.00000000@0.34444444[2.9807693356139104],62526.50000000@0.31000000[2.6826924366679057],62525.00000000@0.20800486[1.8000421442327954],62529.50000000@0.19630544[1.6987971585960173] | book.clusters@1",
        "87599724ms bid median=0.08666666 total=3.68604930/21 top=62509.50000000@0.75666666[8.730769825443833],62504.00000000@0.57000000[6.5769235828402755],62505.00000000@0.41000000[4.730769594674584],62500.00000000@0.34333333[3.9615387278106713],62507.50000000@0.34000000[3.9230772248520944] | ask median=0.12000000 total=2.89034592/21 top=62522.50000000@0.29000000[2.4166666666666665],62526.50000000@0.26777777[2.2314814166666666],62531.50000000@0.25555555[2.129629583333333],62533.50000000@0.22851851[1.9043209166666666],62525.00000000@0.20666666[1.7222221666666666] | book.clusters@1",
        "87659633ms bid unavailable OutOfRange | ask median=0.10666666 total=4.18655072/32 top=62520.00000000@0.49000000[4.593750287109393],62527.50000000@0.39000000[3.6562502285156393],62523.50000000@0.27333333[2.562500128906258],62529.00000000@0.24000000[2.250000140625009],62512.50000000@0.23666666[2.21875007617188] | book.clusters@1",
        "87719851ms bid unavailable OutOfRange | ask median=0.08888888 total=2.93101957/27 top=62516.00000000@0.32000000[3.600000360000036],62519.00000000@0.26888888[3.02500020250002],62515.00000000@0.25000000[2.812500281250028],62512.50000000@0.22000000[2.4750002475000246],62514.00000000@0.19000000[2.1375002137500214] | book.clusters@1",
        "87779913ms bid unavailable OutOfRange | ask unavailable OutOfRange | book.clusters@1",
        "87839961ms bid unavailable OutOfRange | ask median=0.07333333 total=2.47569825/22 top=62513.50000000@0.69000000[9.409091336776878],62518.50000000@0.40000000[5.45454570247935],62512.00000000@0.18000000[2.4545455661157076],62506.50000000@0.14000000[1.9090909958677726],62507.00000000@0.13000000[1.7727273533057888] | book.clusters@1",
        "87899950ms bid unavailable OutOfRange | ask median=0.10000000 total=2.42392467/23 top=62515.50000000@0.38333333[3.8333333],62505.50000000@0.34000000[3.4],62507.50000000@0.17000000[1.7],62508.50000000@0.16000000[1.6],62517.00000000@0.16000000[1.6] | book.clusters@1",
        "87959915ms bid unavailable OutOfRange | ask median=0.07666666 total=2.66969054/25 top=62515.50000000@0.86333333[11.260870500945261],62516.50000000@0.20666666[2.6956523213610715],62508.00000000@0.20000000[2.608695879017033],62521.00000000@0.16000000[2.0869567032136263],62507.00000000@0.14000000[1.8260871153119231] | book.clusters@1",
        "88019147ms bid unavailable OutOfRange | ask median=0.06172839 total=1.90251930/20 top=62506.00000000@0.48000000[7.776000637632052],62516.00000000@0.19172376[3.105925166685864],62511.50000000@0.17333333[2.8080001762560145],62517.50000000@0.15000000[2.430000199260016],62511.00000000@0.13000000[2.1060001726920143] | book.clusters@1",
        "88079918ms bid unavailable OutOfRange | ask median=0.13008230 total=4.03198287/27 top=62512.50000000@0.46333333[3.561847614933008],62515.00000000@0.39000000[2.9981019708292367],62517.50000000@0.30000000[2.306232285253259],62509.00000000@0.23000000[1.768111418694165],62517.00000000@0.20666666[1.5887377452581943] | book.clusters@1",
        "88139929ms bid unavailable OutOfRange | ask median=0.13000000 total=3.22092408/19 top=62507.50000000@0.52000000[4],62520.00000000@0.42265812[3.2512163076923075],62507.00000000@0.38925925[2.994301923076923],62511.00000000@0.31000000[2.3846153846153846],62514.50000000@0.22678453[1.7444963846153847] | book.clusters@1",
        "88199818ms bid unavailable OutOfRange | ask median=0.14000000 total=3.91662358/21 top=62514.50000000@0.65559484[4.6828202857142855],62511.00000000@0.44333333[3.166666642857143],62515.00000000@0.34666666[2.4761904285714285],62513.00000000@0.32814814[2.3439152857142855],62508.00000000@0.30473250[2.176660714285714] | book.clusters@1",
        "88259827ms bid unavailable OutOfRange | ask median=0.12000000 total=3.47897386/19 top=62514.50000000@0.65559484[5.463290333333333],62503.50000000@0.43000000[3.5833333333333335],62511.00000000@0.34111111[2.8425925833333334],62499.50000000@0.34000000[2.8333333333333335],62512.00000000@0.25666666[2.138888833333333] | book.clusters@1",
        "88318625ms bid unavailable OutOfRange | ask median=0.11000000 total=3.68827153/24 top=62502.00000000@0.44222222[4.020202],62496.50000000@0.33666666[3.060606],62504.50000000@0.26666666[2.4242423636363637],62502.50000000@0.24000000[2.1818181818181817],62506.00000000@0.22666666[2.060606] | book.clusters@1",
        "88379904ms bid unavailable OutOfRange | ask median=0.07000000 total=2.68030172/22 top=62505.50000000@0.42444444[6.063492],62504.50000000@0.26666666[3.809523714285714],62498.00000000@0.26000000[3.7142857142857144],62503.00000000@0.23000000[3.2857142857142856],62510.00000000@0.23000000[3.2857142857142856] | book.clusters@1",
        "88439327ms bid unavailable OutOfRange | ask median=0.12000000 total=4.16193411/24 top=62498.00000000@0.65000000[5.416666666666667],62505.00000000@0.46518518[3.8765431666666665],62500.50000000@0.36000000[3],62501.00000000@0.25000000[2.0833333333333335],62504.50000000@0.24777777[2.06481475] | book.clusters@1",
        "88499919ms bid unavailable OutOfRange | ask median=0.28000000 total=5.72999997/21 top=62504.00000000@0.69000000[2.4642857142857144],62504.50000000@0.61777777[2.2063491785714286],62501.50000000@0.57000000[2.0357142857142856],62510.50000000@0.47000000[1.6785714285714286],62503.00000000@0.38000000[1.3571428571428572] | book.clusters@1",
        "88559914ms bid unavailable OutOfRange | ask median=0.10666666 total=3.93168719/31 top=62502.00000000@0.43148148[4.045139127821195],62502.50000000@0.31444444[2.9479168092448007],62497.50000000@0.26000000[2.4375001523437594],62494.50000000@0.21000000[1.9687501230468827],62500.50000000@0.21000000[1.9687501230468827] | book.clusters@1",
        "88619802ms bid unavailable OutOfRange | ask median=0.10000000 total=2.83909459/22 top=62497.50000000@0.47333333[4.7333333],62498.00000000@0.41000000[4.1],62505.50000000@0.22000000[2.2],62494.00000000@0.20000000[2],62501.00000000@0.18000000[1.8] | book.clusters@1",
        "88679928ms bid unavailable OutOfRange | ask median=0.14000000 total=3.27748051/23 top=62500.50000000@0.42333333[3.0238095],62501.50000000@0.29000000[2.0714285714285716],62501.00000000@0.24000000[1.7142857142857142],62504.00000000@0.21148148[1.510582],62498.50000000@0.20000000[1.4285714285714286] | book.clusters@1",
        "88739528ms bid unavailable OutOfRange | ask median=0.06333333 total=1.98566522/19 top=62499.50000000@0.52000000[8.21052674792246],62507.00000000@0.21204846[3.3481337551649344],62503.50000000@0.18000000[2.84210541274239],62500.00000000@0.15987654[2.524366553914029],62501.00000000@0.13555555[2.1403509021237315] | book.clusters@1",
        "88799932ms bid unavailable OutOfRange | ask median=0.08000000 total=1.68132393/20 top=62507.00000000@0.21068282[2.63353525],62503.00000000@0.17841894[2.23023675],62497.00000000@0.15000000[1.875],62504.50000000@0.13333333[1.666666625],62513.00000000@0.13094649[1.636831125] | book.clusters@1",
        "88859647ms bid unavailable OutOfRange | ask unavailable OutOfRange | book.clusters@1",
        "89219836ms bid unavailable OutOfRange | ask median=0.14000000 total=4.62263979/30 top=62507.50000000@0.60000000[4.285714285714286],62500.00000000@0.40666666[2.9047618571428573],62498.00000000@0.37000000[2.642857142857143],62494.50000000@0.34000000[2.4285714285714284],62497.50000000@0.30000000[2.142857142857143] | book.clusters@1",
        "89279920ms bid unavailable OutOfRange | ask median=0.04444444 total=2.80652945/23 top=62494.00000000@0.52000000[11.700001170000117],62492.00000000@0.32000000[7.200000720000072],62494.50000000@0.30000000[6.750000675000067],62499.00000000@0.27666666[6.225000472500048],62496.50000000@0.26000000[5.850000585000059] | book.clusters@1",
        "89339904ms bid unavailable OutOfRange | ask unavailable OutOfRange | book.clusters@1",
        "89639949ms bid unavailable OutOfRange | ask median=0.08333333 total=3.82945654/22 top=62500.00000000@0.89666666[10.760000350400015],62497.00000000@0.62000000[7.440000297600012],62493.50000000@0.39000000[4.680000187200007],62495.50000000@0.38000000[4.560000182400008],62504.00000000@0.34000000[4.0800001632000065] | book.clusters@1",
        "89699933ms bid unavailable OutOfRange | ask unavailable OutOfRange | book.clusters@1",
        "89759943ms bid unavailable OutOfRange | ask median=0.13000000 total=2.97070412/22 top=62494.50000000@0.36000000[2.769230769230769],62502.50000000@0.26000000[2],62495.50000000@0.23333333[1.7948717692307692],62496.50000000@0.21000000[1.6153846153846154],62493.50000000@0.20000000[1.5384615384615385] | book.clusters@1",
        "89819635ms bid unavailable OutOfRange | ask median=0.14000000 total=3.29355003/18 top=62494.00000000@0.52777777[3.7698412142857145],62494.50000000@0.48666666[3.4761904285714285],62500.50000000@0.36333333[2.595238071428571],62497.50000000@0.33555555[2.3968253571428573],62493.50000000@0.21000000[1.5] | book.clusters@1",
        "89879820ms bid unavailable OutOfRange | ask median=0.05333333 total=2.53679005/21 top=62497.00000000@0.43938271[8.238426327401646],62493.50000000@0.27888888[5.229166826822927],62490.00000000@0.26444444[4.958333559895848],62497.50000000@0.26370370[4.9444446840277925],62496.50000000@0.20740740[3.888888993055562] | book.clusters@1",
        "89939831ms bid unavailable OutOfRange | ask median=0.10654320 total=3.19291715/27 top=62496.50000000@0.55740740[5.23175012577058],62498.00000000@0.24000000[2.252607393057464],62487.00000000@0.23580246[2.213209852904737],62492.50000000@0.22000000[2.0648901103026756],62490.00000000@0.20000000[1.8771728275478867] | book.clusters@1",
        "89999808ms bid unavailable OutOfRange | ask median=0.08238683 total=3.43079555/30 top=62490.00000000@0.57333333[6.959041026338797],62484.50000000@0.32000000[3.8841159442595377],62496.50000000@0.23580246[2.8621377955675684],62493.50000000@0.22000000[2.670329711678432],62475.50000000@0.19000000[2.3061938419041006] | book.clusters@1",
        "90059832ms bid unavailable OutOfRange | ask median=0.05666666 total=2.58647455/20 top=62489.50000000@0.52000000[9.176471667820197],62484.00000000@0.47333333[8.35294210034613],62479.50000000@0.24000000[4.235294615917014],62486.50000000@0.22666666[4.000000352941218],62472.00000000@0.19000000[3.3529415709343025] | book.clusters@1",
        "90119724ms bid unavailable OutOfRange | ask median=0.07333333 total=2.53639377/20 top=62484.00000000@0.59333333[8.090909413223155],62489.50000000@0.52000000[7.090909413223155],62490.50000000@0.19814814[2.7020202137281917],62479.50000000@0.15000000[2.0454546384297565],62475.50000000@0.14000000[1.9090909958677726] | book.clusters@1",
        "90179718ms bid unavailable OutOfRange | ask median=0.14000000 total=4.51275561/22 top=62484.00000000@0.92555555[6.611111071428572],62488.50000000@0.42666666[3.047619],62492.50000000@0.35000000[2.5],62486.50000000@0.34666666[2.4761904285714285],62484.50000000@0.32777777[2.3412697857142857] | book.clusters@1",
        "90239214ms bid unavailable OutOfRange | ask median=0.07222222 total=2.10411515/25 top=62495.50000000@0.21333333[2.953846198579883],62500.50000000@0.20000000[2.7692308544378723],62494.00000000@0.19000000[2.6307693117159787],62490.50000000@0.18000000[2.492307768994085],62480.50000000@0.17000000[2.3538462262721915] | book.clusters@1",
        "90299654ms bid unavailable OutOfRange | ask median=0.09851851 total=3.46506167/22 top=62486.00000000@0.46000000[4.669173336056341],62486.50000000@0.46000000[4.669173336056341],62484.50000000@0.42000000[4.263158263355789],62485.00000000@0.42000000[4.263158263355789],62491.50000000@0.22000000[2.2330828998530325] | book.clusters@1",
        "90358010ms bid unavailable OutOfRange | ask median=0.08000000 total=2.23580243/20 top=62493.00000000@0.28518518[3.56481475],62491.50000000@0.28000000[3.5],62480.50000000@0.21000000[2.625],62487.50000000@0.19000000[2.375],62483.50000000@0.18910836[2.3638545] | book.clusters@1",
        "90419741ms bid unavailable OutOfRange | ask median=0.13333333 total=2.73213073/19 top=62487.00000000@0.40144032[3.010802475270062],62480.50000000@0.32000000[2.4000000600000013],62483.50000000@0.28303612[2.1227709530692738],62488.00000000@0.26000000[1.9500000487500013],62489.50000000@0.23000000[1.725000043125001] | book.clusters@1",
        "90479825ms bid unavailable OutOfRange | ask median=0.08000000 total=4.20711928/22 top=62489.50000000@1.02000000[12.75],62487.00000000@0.92144032[11.518004],62488.00000000@0.50000000[6.25],62485.50000000@0.29000000[3.625],62488.50000000@0.20444444[2.5555555] | book.clusters@1",
        "90539840ms bid unavailable OutOfRange | ask median=0.09777777 total=2.32898487/21 top=62492.00000000@0.35000000[3.5795457392820476],62489.50000000@0.26333333[2.6931819983212955],62489.00000000@0.20000000[2.04545470816117],62485.00000000@0.18666666[1.9090909927686017],62484.00000000@0.17000000[1.7386365019369945] | book.clusters@1",
        "90599914ms bid unavailable OutOfRange | ask median=0.13000000 total=3.82279828/28 top=62481.50000000@0.42000000[3.230769230769231],62483.50000000@0.38333333[2.948717923076923],62482.00000000@0.33000000[2.5384615384615383],62486.00000000@0.25000000[1.9230769230769231],62487.00000000@0.23000000[1.7692307692307692] | book.clusters@1",
        "90659727ms bid unavailable OutOfRange | ask median=0.10971193 total=3.08854133/22 top=62486.00000000@0.54000000[4.921980681590416],62486.50000000@0.30444444[2.774943800551134],62491.50000000@0.24000000[2.1875469695957404],62495.00000000@0.22000000[2.005251388796095],62489.50000000@0.20333333[1.8533383744137943] | book.clusters@1",
        "90718778ms bid unavailable OutOfRange | ask median=0.14000000 total=5.25260167/28 top=62485.50000000@0.69561957[4.968711214285714],62491.50000000@0.52000000[3.7142857142857144],62488.00000000@0.47444444[3.3888888571428573],62470.50000000@0.40000000[2.857142857142857],62490.50000000@0.33333333[2.3809523571428572] | book.clusters@1",
        "90779835ms bid unavailable OutOfRange | ask median=0.08000000 total=3.70643338/29 top=62477.00000000@0.40000000[5],62475.50000000@0.39000000[4.875],62477.50000000@0.36000000[4.5],62472.50000000@0.30000000[3.75],62478.50000000@0.24666666[3.08333325] | book.clusters@1",
        "90839251ms bid unavailable OutOfRange | ask median=0.12333333 total=3.36090987/22 top=62475.50000000@0.39000000[3.1621622476260067],62477.00000000@0.36148148[2.9309309981332703],62473.50000000@0.35000000[2.8378379145361596],62486.50000000@0.31000000[2.513513581446313],62473.00000000@0.29000000[2.3513514149013894] | book.clusters@1",
        "90899935ms bid unavailable OutOfRange | ask median=0.13000000 total=3.72950915/20 top=62476.50000000@0.59666666[4.589743538461539],62477.00000000@0.52098765[4.007597307692308],62477.50000000@0.51333333[3.948717923076923],62487.00000000@0.37000000[2.8461538461538463],62482.00000000@0.27000000[2.076923076923077] | book.clusters@1",
    ];

    const GOLDEN_WINDOW_5M: [&str; 71] = [
        "86701227ms 5m end=86700000ms | 1bps bid=19.15000000/18.40840428/25.03407401/0.39000000 ask=17.06000000/18.43864203/31.73827150/0.64000000 | 2bps bid=33.70000000/53.23340660/25.12407401/0.39000000 ask=31.50000000/54.84613176/31.78827150/0.64000000 | 5bps bid=47.16000000/137.59010987/25.12407401/0.39000000 ask=43.03000000/146.97860101/31.78827150/0.64000000 | partial_start book.liquidity.window.5m@1",
        "86760218ms 5m end=86760000ms | 1bps bid=20.04000000/6.71136723/22.76407402/0.68000000 ask=18.78333333/7.39643807/28.04049373/0.70000000 | 2bps bid=34.39000000/21.16785099/23.18407402/0.69000000 ask=33.72333333/26.82488800/28.39049373/0.70000000 | 5bps bid=45.36000000/92.57274355/23.18407402/0.69000000 ask=43.65333333/111.56388669/28.39049373/0.70000000 beyond | complete book.liquidity.window.5m@1",
        "86821422ms 5m end=86820000ms | 1bps bid=21.11000000/3.65054418/22.41074069/0.76000000 ask=18.70333333/7.61199363/23.90880647/0.54000000 | 2bps bid=34.56000000/13.11638323/23.09037031/0.77000000 ask=31.93333333/22.73538182/24.55880647/0.54000000 | 5bps bid=44.32000000/65.13445817/23.09037031/0.77000000 ask=42.63333333/92.09397810/24.55880647/0.54000000 beyond | complete book.liquidity.window.5m@1",
        "86881030ms 5m end=86880000ms | 1bps bid=22.03000000/1.71703708/22.65629625/1.19000000 ask=19.51333333/4.18458623/22.62489243/0.51000000 | 2bps bid=33.53000000/9.23645637/23.37592587/1.20000000 ask=32.97333333/14.33903828/23.27489243/0.51000000 | 5bps bid=43.01000000/32.72545041/23.37592587/1.20000000 ask=43.67333333/70.80534066/23.27489243/0.51000000 beyond | complete book.liquidity.window.5m@1",
        "86940412ms 5m end=86940000ms | 1bps bid=22.13000000/1.28703708/23.54444439/1.05000000 ask=19.16333333/4.52903067/22.62933688/0.93000000 | 2bps bid=34.45000000/7.39290815/24.30407401/1.06000000 ask=32.50333333/14.73607532/23.22933688/0.93000000 | 5bps bid=43.78000000/20.89709658/24.30407401/1.06000000 ask=43.33666665/58.24143120/23.22933688/0.93000000 beyond | complete book.liquidity.window.5m@1",
        "87000521ms 5m end=87000000ms | 1bps bid=19.81000000/1.58592596/23.51111104/1.02000000 ask=17.04333333/3.41261094/20.80760849/0.95000000 | 2bps bid=32.81000000/7.72716055/24.57370362/1.03000000 ask=31.34333333/11.83903832/21.69575663/1.12000000 | 5bps bid=41.46000000/19.31248178/24.57370362/1.03000000 ask=43.25666665/36.82729770/21.69575663/1.12000000 beyond | complete book.liquidity.window.5m@1",
        "87060027ms 5m end=87060000ms | 1bps bid=19.50000000/1.72370375/23.26222214/1.06000000 ask=16.58000000/3.51851859/19.60242330/1.03000000 | 2bps bid=31.95000000/8.10641983/24.34148138/1.06000000 ask=31.45000000/13.07810099/20.54057144/1.20000000 | 5bps bid=41.51000000/18.58388601/24.34148138/1.06000000 ask=45.35333332/27.40952911/20.54057144/1.20000000 beyond | complete book.liquidity.window.5m@1",
        "87120225ms 5m end=87120000ms | 1bps bid=18.01000000/2.23777785/22.19111100/1.21000000 ask=17.61000000/3.38962969/20.90818462/1.11000000 | 2bps bid=30.36000000/8.19814828/23.43740727/1.21000000 ask=33.03333333/12.53043294/21.87855498/1.28000000 | 5bps bid=40.13000000/18.68725141/23.43740727/1.21000000 ask=46.57666665/26.87611805/21.87855498/1.28000000 beyond | complete book.liquidity.window.5m@1",
        "87180402ms 5m end=87180000ms | 1bps bid=18.65000000/3.20185192/22.13999987/0.95000000 ask=18.08000000/2.27259265/20.05740734/1.00000000 | 2bps bid=32.43000000/9.42444457/23.60185168/0.95000000 ask=33.83333333/9.79245551/21.10777770/1.17000000 | 5bps bid=41.14000000/19.37575203/23.80185168/0.95000000 ask=46.31666665/24.21275269/21.10777770/1.17000000 beyond | complete book.liquidity.window.5m@1",
        "87240011ms 5m end=87240000ms | 1bps bid=19.04000000/3.14185193/20.99444433/1.16000000 ask=18.61000000/2.70362147/21.02740732/0.65000000 | 2bps bid=32.33000000/9.09178339/22.72629614/1.16000000 ask=34.99333333/10.47238238/22.25444434/0.82000000 | 5bps bid=41.47000000/18.88078633/22.92629614/1.16000000 ask=48.28333333/25.39389127/22.25444434/0.82000000 beyond | complete book.liquidity.window.5m@1",
        "87300051ms 5m end=87300000ms | 1bps bid=21.29000000/2.94629639/22.37777767/1.34000000 ask=20.35000000/2.75954739/21.23518509/0.71000000 | 2bps bid=34.77000000/9.11746241/23.80666652/1.34000000 ask=36.10333333/11.63689778/22.17407397/0.71000000 | 5bps bid=44.56000000/18.84695198/24.00666652/1.34000000 ask=48.50333333/24.97382861/22.17407397/0.71000000 beyond | complete book.liquidity.window.5m@1",
        "87360209ms 5m end=87360000ms | 1bps bid=22.43666666/3.61111120/22.09111100/1.47000000 ask=22.31000000/3.59510293/22.91370362/0.57000000 | 2bps bid=36.65666666/10.83057629/23.17333319/1.47000000 ask=37.07333333/11.10355608/23.50259250/0.57000000 | 5bps bid=46.09666666/21.57925147/23.37333319/1.47000000 ask=49.51333333/24.99678778/23.50259250/0.57000000 beyond | complete book.liquidity.window.5m@1",
        "87420411ms 5m end=87420000ms | 1bps bid=23.75666666/3.60724286/23.84777767/1.36000000 ask=21.91000000/3.69399183/21.69111103/0.87000000 | 2bps bid=38.39666666/11.78122705/24.59333321/1.36000000 ask=36.78000000/12.12542653/22.03777769/0.87000000 | 5bps bid=48.70666666/23.54677011/24.79333321/1.36000000 ask=50.39000000/26.05288882/22.03777769/0.87000000 beyond | complete book.liquidity.window.5m@1",
        "87480026ms 5m end=87480000ms | 1bps bid=22.71666666/2.81872434/23.31333325/1.19000000 ask=22.18000000/4.32065850/21.80888879/1.02000000 | 2bps bid=36.65666666/12.17230311/23.72333325/1.19000000 ask=37.30000000/14.89139470/22.07555545/1.02000000 | 5bps bid=48.21666666/24.39916870/23.72333325/1.19000000 ask=51.14000000/27.91069514/22.07555545/1.02000000 | complete book.liquidity.window.5m@1",
        "87540252ms 5m end=87540000ms | 1bps bid=23.03666666/4.12205766/23.14777770/1.13000000 ask=21.78000000/5.55814262/21.58666657/1.31000000 | 2bps bid=38.13666666/13.16348282/23.36777770/1.13000000 ask=36.24000000/14.89935079/21.67666657/1.31000000 | 5bps bid=49.88666666/26.27820846/23.36777770/1.13000000 ask=48.72000000/27.11267654/21.67666657/1.31000000 beyond | complete book.liquidity.window.5m@1",
        "87600024ms 5m end=87600000ms | 1bps bid=22.58666666/4.52427988/20.75555548/0.91000000 ask=22.10000000/5.04977278/21.73634667/1.43000000 | 2bps bid=37.65666666/13.63348282/20.97555548/0.91000000 ask=35.87000000/15.15948387/21.82634667/1.43000000 | 5bps bid=49.42666666/27.94506184/20.97555548/0.91000000 ask=49.87000000/28.58576600/21.82634667/1.43000000 beyond | complete book.liquidity.window.5m@1",
        "87660103ms 5m end=87660000ms | 1bps bid=23.28000000/4.82341567/22.31345669/0.55000000 ask=20.78000000/4.31680985/20.06967999/1.51000000 | 2bps bid=36.77000000/14.60574617/22.66345669/0.55000000 ask=35.22000000/13.47796960/20.15967999/1.51000000 | 5bps bid=49.45000000/28.18160505/22.66345669/0.55000000 ask=48.48000000/28.05167745/20.15967999/1.51000000 beyond | complete book.liquidity.window.5m@1",
        "87720617ms 5m end=87720000ms | 1bps bid=22.39000000/4.64000002/20.28956093/0.74000000 ask=22.06000000/4.00125428/21.71079110/1.64000000 | 2bps bid=35.40000000/13.62587725/20.61956093/0.74000000 ask=35.57000000/12.11207994/21.74079110/1.64000000 beyond | 5bps bid=45.92481482/25.65222993/20.61956093/0.74000000 ask=46.42000000/25.81027081/21.74079110/1.64000000 beyond | complete book.liquidity.window.5m@1",
        "87780301ms 5m end=87780000ms | 1bps bid=22.30000000/5.24333335/20.97460893/0.95000000 ask=21.93000000/3.84162464/21.59968000/1.77000000 | 2bps bid=35.81000000/13.81257899/21.98127559/0.95000000 ask=34.32000000/10.40944508/22.22301333/1.77000000 beyond | 5bps bid=44.43481482/23.61337459/21.98127559/0.95000000 ask=43.89000000/23.37685255/22.22301333/1.77000000 beyond | complete book.liquidity.window.5m@1",
        "87840566ms 5m end=87840000ms | 1bps bid=21.73000000/4.54777780/21.44127559/1.08000000 ask=22.67000000/2.05866726/22.05190224/1.77333333 beyond | 2bps bid=34.50000000/13.63270243/22.45794225/1.08000000 ask=35.79000000/8.84082137/23.25523557/1.77333333 beyond | 5bps bid=41.19481482/20.19316883/22.45794225/1.08000000 ask=45.11000000/22.36328672/23.25523557/1.77333333 beyond | complete book.liquidity.window.5m@1",
        "87900214ms 5m end=87900000ms | 1bps bid=20.80000000/4.70728396/21.33349782/1.00000000 ask=22.63000000/2.95138552/22.14876534/1.72333333 beyond | 2bps bid=32.67000000/12.70109746/22.35016448/1.00000000 ask=36.88913580/9.50558307/23.35209867/1.72333333 beyond | 5bps bid=37.76481482/17.67069964/22.35016448/1.00000000 ask=45.36913580/22.30151684/23.35209867/1.72333333 beyond | complete book.liquidity.window.5m@1",
        "87960704ms 5m end=87960000ms | 1bps bid=20.78000000/3.83888890/20.54448551/1.01000000 ask=22.51000000/3.29101513/23.00358012/1.64333333 beyond | 2bps bid=33.69000000/10.89275726/21.69781883/1.01000000 ask=35.86913580/10.59595346/24.47691345/1.64333333 beyond | 5bps bid=36.68481482/14.67930046/21.69781883/1.01000000 ask=43.45913580/20.30380765/24.47691345/1.64333333 beyond | complete book.liquidity.window.5m@1",
        "88020027ms 5m end=88020000ms | 1bps bid=19.81000000/3.89617287/20.82504794/1.07000000 ask=21.00000000/3.37953366/21.66913568/1.52333333 beyond | 2bps bid=30.72000000/9.85716056/22.62134422/1.07000000 ask=34.66913580/11.06274361/23.26913567/1.52333333 beyond | 5bps bid=33.16000000/12.93407414/22.62134422/1.07000000 ask=42.85913580/20.41853417/23.26913567/1.52333333 beyond | complete book.liquidity.window.5m@1",
        "88080982ms 5m end=88080000ms | 1bps bid=21.35000000/3.44061732/21.90222216/1.12000000 ask=22.03000000/2.96328616/22.95740725/1.22333333 beyond | 2bps bid=32.52000000/8.45641979/23.07185178/1.12000000 ask=36.12913580/11.39622628/23.98407391/1.22333333 beyond | 5bps bid=34.08000000/11.21629636/23.07185178/1.12000000 ask=46.95913580/20.82867437/23.98407391/1.22333333 beyond | complete book.liquidity.window.5m@1",
        "88140362ms 5m end=88140000ms | 1bps bid=22.03000000/3.44506176/21.72999994/1.03000000 ask=21.68000000/3.14328618/21.69296278/0.95000000 beyond | 2bps bid=32.14000000/8.11218112/22.77629622/1.03000000 ask=34.92913580/12.02326336/22.13962944/0.95000000 beyond | 5bps bid=33.53000000/10.37181078/22.77629622/1.03000000 ask=45.78913580/22.83493897/22.13962944/0.95000000 beyond | complete book.liquidity.window.5m@1",
        "88200324ms 5m end=88200000ms | 1bps bid=21.45000000/2.68000004/21.77333328/1.09000000 ask=21.47000000/2.09412293/21.82790108/0.91000000 beyond | 2bps bid=30.52000000/7.92773671/23.11707810/1.09000000 ask=35.37000000/10.63245400/22.77456774/0.91000000 beyond | 5bps bid=31.33000000/8.68958859/23.11707810/1.09000000 ask=45.60000000/21.30339179/22.77456774/0.91000000 beyond | complete book.liquidity.window.5m@1",
        "88260420ms 5m end=88260000ms | 1bps bid=19.21000000/2.53333337/20.22999993/1.09000000 ask=21.13000000/2.18338220/21.13049369/1.20000000 beyond | 2bps bid=25.88000000/6.43292188/21.30707809/1.09000000 ask=35.58000000/9.97467620/21.83604923/1.20000000 beyond | 5bps bid=26.05000000/6.68699597/21.30707809/1.09000000 ask=46.32000000/23.44310368/21.83604923/1.20000000 beyond | complete book.liquidity.window.5m@1",
        "88320026ms 5m end=88320000ms | 1bps bid=16.96000000/1.86555559/17.16444439/0.70000000 ask=22.04000000/2.86375256/20.93716037/1.40000000 beyond | 2bps bid=23.14000000/5.62736632/17.52855959/0.70000000 ask=38.39000000/11.40825645/21.48604925/1.40000000 beyond | 5bps bid=23.31000000/5.88144041/17.52855959/0.70000000 ask=49.15000000/25.65132610/21.48604925/1.40000000 beyond | complete book.liquidity.window.5m@1",
        "88381101ms 5m end=88380000ms | 1bps bid=12.32333333/1.45555558/12.37777773/0.46333333 ask=19.75000000/2.59271611/19.21283941/1.32000000 beyond | 2bps bid=15.54333333/4.72736631/12.69189293/0.46333333 ask=35.57000000/10.31971209/19.86172829/1.32000000 beyond | 5bps bid=15.57333333/4.74069965/12.69189293/0.46333333 ask=45.24000000/26.43439731/19.86172829/1.32000000 beyond | complete book.liquidity.window.5m@1",
        "88440617ms 5m end=88440000ms | 1bps bid=8.47333333/0.57666668/9.05666663/0.52333333 ask=20.37000000/2.74382722/19.89172831/1.39000000 beyond | 2bps bid=9.60333333/2.53333336/9.35411517/0.52333333 ask=35.93000000/10.61798370/20.64950607/1.39000000 beyond | 5bps bid=9.63333333/2.54666670/9.35411517/0.52333333 ask=46.78000000/24.97392835/20.64950607/1.39000000 beyond | complete book.liquidity.window.5m@1",
        "88500218ms 5m end=88500000ms | 1bps bid=7.00333333/0.54000002/7.19333329/0.63333333 ask=19.74000000/4.20827167/18.69246905/1.42000000 beyond | 2bps bid=7.15333333/0.94333335/7.19333329/0.63333333 ask=34.23000000/11.48174225/18.99024681/1.42000000 beyond | 5bps bid=7.18333333/0.94333335/7.19333329/0.63333333 ask=47.17000000/26.15082125/18.99024681/1.42000000 beyond | complete book.liquidity.window.5m@1",
        "88560502ms 5m end=88560000ms | 1bps bid=5.57333333/0.23000002/5.46333331/0.58333333 ask=20.02000000/3.86716054/19.39506167/1.23000000 beyond | 2bps bid=5.69333333/0.23000002/5.46333331/0.58333333 ask=34.72000000/13.31548707/19.66395055/1.23000000 beyond | 5bps bid=5.69333333/0.23000002/5.46333331/0.58333333 ask=48.53000000/28.41333612/19.66395055/1.23000000 beyond | complete book.liquidity.window.5m@1",
        "88621115ms 5m end=88620000ms | 1bps bid=5.26333333/0.21666668/5.16666665/0.69333333 ask=19.56000000/3.48666673/18.85629621/0.71000000 beyond | 2bps bid=5.38333333/0.21666668/5.16666665/0.69333333 ask=32.71925926/13.42820313/19.12518509/0.71000000 beyond | 5bps bid=5.38333333/0.21666668/5.16666665/0.69333333 ask=46.22925926/27.95325111/19.12518509/0.71000000 beyond | complete book.liquidity.window.5m@1",
        "88680029ms 5m end=88680000ms | 1bps bid=6.02000000/0.24666668/5.89333332/0.69000000 ask=20.82000000/4.12333342/19.53444434/0.65000000 beyond | 2bps bid=6.20000000/0.24666668/5.95333332/0.69000000 ask=35.66925926/15.77979433/19.80333322/0.65000000 beyond | 5bps bid=6.20000000/0.24666668/5.95333332/0.69000000 ask=48.30925926/27.98874725/19.80333322/0.65000000 beyond | complete book.liquidity.window.5m@1",
        "88740239ms 5m end=88740000ms | 1bps bid=6.41000000/0.36000002/5.79999998/0.41000000 ask=20.86000000/4.19444451/20.57999990/0.77000000 beyond | 2bps bid=6.47000000/0.36000002/5.85999998/0.41000000 ask=35.85925926/15.82562422/21.01999990/0.77000000 beyond | 5bps bid=6.47000000/0.36000002/5.85999998/0.41000000 ask=47.08925926/28.24552825/21.01999990/0.77000000 beyond | complete book.liquidity.window.5m@1",
        "88802510ms 5m end=88800000ms | 1bps bid=6.45000000/0.24333334/6.20666666/0.27000000 ask=20.48000000/2.76386380/21.98320979/0.86000000 beyond | 2bps bid=6.51000000/0.24333334/6.26666666/0.27000000 ask=35.93925926/14.27446026/22.54839497/0.86000000 beyond | 5bps bid=6.51000000/0.24333334/6.26666666/0.27000000 ask=45.87925926/27.37954033/22.54839497/0.86000000 beyond | complete book.liquidity.window.5m@1",
        "88820100ms 5m end=88800000ms | 1bps bid=6.45000000/0.24333334/6.20666666/0.27000000 ask=20.48000000/2.76386380/21.98320979/0.86000000 beyond | 2bps bid=6.51000000/0.24333334/6.26666666/0.27000000 ask=35.93925926/14.27446026/22.54839497/0.86000000 beyond | 5bps bid=6.51000000/0.24333334/6.26666666/0.27000000 ask=45.87925926/27.37954033/22.54839497/0.86000000 beyond | feed_gap book.liquidity.window.5m@1",
        "88860318ms 5m end=88860000ms | 1bps bid=7.38000000/0.25000000/7.33999997/0.36000000 ask=19.73000000/2.66608602/21.62987646/1.03000000 beyond | 2bps bid=9.66000000/2.04559010/7.86999997/0.36000000 ask=33.54925926/11.38898701/22.73506164/1.03000000 beyond | 5bps bid=10.81000000/3.38898744/7.86999997/0.36000000 ask=42.13925926/21.67534600/22.73506164/1.03000000 beyond | feed_gap book.liquidity.window.5m@1",
        "88920328ms 5m end=88920000ms | 1bps bid=10.57333333/1.47111112/10.45510858/0.34000000 ask=19.46000000/2.20991316/21.77530858/1.18000000 beyond | 2bps bid=15.77000000/5.79545074/10.98510858/0.34000000 ask=33.22000000/9.21063308/22.88049376/1.18000000 beyond | 5bps bid=18.46000000/8.76321023/10.98510858/0.34000000 ask=42.02000000/19.52214875/22.88049376/1.18000000 beyond | feed_gap book.liquidity.window.5m@1",
        "88980029ms 5m end=88980000ms | 1bps bid=14.58333333/3.12419755/14.66597732/0.48000000 ask=21.34000000/1.93941932/23.25987650/1.37000000 beyond | 2bps bid=23.83000000/9.99884810/15.18597732/0.48000000 ask=34.40000000/7.68410361/24.25506168/1.37000000 beyond | 5bps bid=27.02000000/13.58623723/15.18597732/0.48000000 ask=44.81333333/20.38275688/24.25506168/1.37000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89040057ms 5m end=89040000ms | 1bps bid=17.01999999/3.47419755/17.25264397/0.68000000 ask=20.71000000/2.37151812/24.00827151/1.56000000 beyond | 2bps bid=29.03666666/11.13625551/18.03264397/0.68000000 ask=34.78000000/7.88247128/24.72345669/1.56000000 beyond | 5bps bid=34.54666666/17.34317824/18.03264397/0.68000000 ask=46.18333333/20.12180586/24.72345669/1.56000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89100976ms 5m end=89100000ms | 1bps bid=19.32999999/4.93419755/18.48931063/0.82000000 ask=20.58000000/2.92691363/23.17765421/1.28000000 | 2bps bid=34.12777777/14.62069996/19.41931063/0.82000000 ask=34.02000000/9.14333348/23.95283939/1.28000000 beyond | 5bps bid=42.07777777/23.14426194/19.41931063/0.82000000 ask=45.52999999/20.76795841/23.95283939/1.28000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89160252ms 5m end=89160000ms | 1bps bid=20.43999999/5.10086423/19.30264398/1.01000000 ask=21.42000000/3.90019212/24.04898475/1.48000000 | 2bps bid=35.60777777/14.62622099/19.87264398/1.01000000 ask=36.20000000/11.12286711/24.29120696/1.48000000 beyond | 5bps bid=44.05777777/24.06749675/19.87264398/1.01000000 ask=49.64999999/23.70752997/24.29120696/1.48000000 beyond | complete book.liquidity.window.5m@1",
        "89220525ms 5m end=89220000ms | 1bps bid=20.73666666/5.92510291/21.67453123/1.33000000 ask=22.27000000/3.97352547/24.53898475/1.31000000 | 2bps bid=36.41777777/13.84566077/22.25453123/1.33000000 ask=37.52666666/10.72593983/24.84120696/1.31000000 beyond | 5bps bid=45.31777777/22.97516696/22.25453123/1.33000000 ask=51.81666665/24.85006269/24.84120696/1.31000000 beyond | complete book.liquidity.window.5m@1",
        "89280655ms 5m end=89280000ms | 1bps bid=19.57666666/5.35349799/20.08218099/1.27000000 ask=20.74000000/3.85463657/24.05565141/1.05000000 | 2bps bid=33.51777777/12.34300418/20.81218099/1.27000000 ask=35.15666666/10.30013734/24.73787362/1.05000000 beyond | 5bps bid=42.73777777/21.51362148/20.81218099/1.27000000 ask=48.68333332/24.44925944/24.73787362/1.05000000 beyond | complete book.liquidity.window.5m@1",
        "89340430ms 5m end=89340000ms | 1bps bid=19.23000000/5.21683133/20.91884767/1.28000000 ask=20.21000000/3.23698221/23.32947862/0.69000000 | 2bps bid=34.25111111/12.97448568/21.79218100/1.28000000 ask=33.20666666/8.84349806/24.33591207/0.69000000 beyond | 5bps bid=43.11111111/22.27606321/21.79218100/1.28000000 ask=46.12333332/22.33854608/24.33591207/0.69000000 beyond | complete book.liquidity.window.5m@1",
        "89401218ms 5m end=89400000ms | 1bps bid=19.91000000/3.93905357/21.51551432/1.23000000 ask=20.32000000/2.48105629/23.69355271/0.92000000 | 2bps bid=34.70000000/10.99333343/22.50884765/1.23000000 ask=33.29666666/6.89563796/24.59480098/0.92000000 beyond | 5bps bid=42.09000000/18.73246924/22.50884765/1.23000000 ask=45.47666666/19.92388217/24.59480098/0.92000000 beyond | complete book.liquidity.window.5m@1",
        "89460119ms 5m end=89460000ms | 1bps bid=21.36000000/5.76218113/21.74551432/1.03000000 ask=20.53000000/2.65148150/23.21925918/0.63000000 | 2bps bid=36.42000000/13.89207142/22.62884765/1.03000000 ask=34.34666666/8.79598088/24.11347042/0.63000000 beyond | 5bps bid=44.07000000/22.02976690/22.62884765/1.03000000 ask=45.20666666/21.67389586/24.11347042/0.63000000 beyond | complete book.liquidity.window.5m@1",
        "89520304ms 5m end=89520000ms | 1bps bid=22.35000000/3.79683134/20.82185178/1.00000000 ask=20.15000000/2.79666667/24.01321535/0.91000000 | 2bps bid=37.49000000/13.72541848/21.77518511/1.00000000 ask=34.55000000/10.75779510/25.04742659/0.91000000 beyond | 5bps bid=45.47000000/23.40824987/21.77518511/1.00000000 ask=44.96000000/22.14370429/25.04742659/0.91000000 beyond | complete book.liquidity.window.5m@1",
        "89580118ms 5m end=89580000ms | 1bps bid=22.05666666/3.50547329/21.05222215/1.08000000 ask=20.11000000/3.05444445/23.30432648/1.46000000 | 2bps bid=37.64666666/13.73702341/21.80555548/1.08000000 ask=35.36000000/10.87656054/23.97853772/1.46000000 beyond | 5bps bid=46.25666666/24.49720172/21.80555548/1.08000000 ask=46.03000000/21.74047462/23.97853772/1.46000000 beyond | complete book.liquidity.window.5m@1",
        "89640122ms 5m end=89640000ms | 1bps bid=22.12666666/4.50991772/20.99380877/0.87000000 ask=20.71000000/2.92444446/22.47877093/1.55000000 | 2bps bid=36.24666666/14.99453137/21.34380877/0.87000000 ask=36.80333333/10.74998988/23.07877093/1.55000000 beyond | 5bps bid=45.23481481/24.12989490/21.34380877/0.87000000 ask=47.23666666/22.98033213/23.07877093/1.55000000 beyond | complete book.liquidity.window.5m@1",
        "89730000ms 5m end=89700000ms | 1bps bid=22.99666666/4.97102882/22.56265651/1.02000000 ask=21.34000000/3.52703705/23.06728944/1.59000000 | 2bps bid=36.73666666/15.20540014/23.07265651/1.07000000 ask=37.78333333/12.83258247/23.54728944/1.59000000 beyond | 5bps bid=46.72481481/24.62438509/23.07265651/1.07000000 ask=49.08666666/25.47267919/23.54728944/1.59000000 beyond | complete book.liquidity.window.5m@1",
        "89760807ms 5m end=89760000ms | 1bps bid=22.49666666/6.52123461/20.87265650/0.88000000 ask=21.28666667/4.82777781/21.90543759/1.50000000 | 2bps bid=35.98666666/14.99084294/21.44932316/0.93000000 ask=38.07000000/12.97690348/22.38543759/1.50000000 beyond | 5bps bid=45.28481481/23.40312005/21.44932316/0.93000000 ask=48.87111111/26.11424232/22.38543759/1.50000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89821458ms 5m end=89820000ms | 1bps bid=21.77666666/7.49654324/20.54598984/0.61000000 ask=21.43666667/4.89259263/21.58481476/1.95000000 | 2bps bid=34.85666666/15.27079264/21.19043427/0.66000000 ask=37.95000000/12.02296306/21.86481476/1.95000000 beyond | 5bps bid=43.33481481/22.86721887/21.19043427/0.66000000 ask=48.55111111/25.49163519/21.86481476/1.95000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89881201ms 5m end=89880000ms | 1bps bid=21.85000000/7.26562420/19.66925456/0.44000000 ask=21.64333333/4.83962968/22.45296287/1.66000000 beyond | 2bps bid=34.30000000/14.24571720/20.86509815/0.49000000 ask=38.90666666/12.36925940/23.34629620/1.66000000 beyond | 5bps bid=41.42814815/20.92319157/20.86509815/0.49000000 ask=49.13777777/26.30971206/23.41962953/1.66000000 beyond | feed_gap book.liquidity.window.5m@1",
        "89941441ms 5m end=89940000ms | 1bps bid=22.72000000/7.08566537/18.16433460/0.91000000 ask=20.64333333/4.89456796/21.09925916/1.54000000 beyond | 2bps bid=34.30000000/13.23195408/19.52684485/0.96000000 ask=37.18333333/12.77827178/21.74259249/1.54000000 beyond | 5bps bid=39.56000000/19.41263828/19.52684485/0.96000000 ask=48.31111111/27.12061358/21.81592582/1.54000000 beyond | feed_gap book.liquidity.window.5m@1",
        "90000008ms 5m end=90000000ms | 1bps bid=20.27000000/7.44418986/16.39474783/0.74000000 ask=20.95333333/4.80674903/19.49407398/1.21000000 beyond | 2bps bid=30.69000000/13.86917247/17.48725808/0.74000000 ask=37.20333333/11.62262018/20.45740731/1.21000000 beyond | 5bps bid=33.98000000/19.05203772/17.48725808/0.74000000 ask=47.95111111/27.37295325/20.53074064/1.21000000 beyond | feed_gap book.liquidity.window.5m@1",
        "90060515ms 5m end=90060000ms | 1bps bid=18.60000000/3.88085651/17.68746388/0.96000000 ask=20.42666666/2.88897125/20.48296288/1.49000000 beyond | 2bps bid=26.42000000/9.36054723/18.71330747/0.96000000 ask=35.63666666/9.49829918/21.58629621/1.49000000 beyond | 5bps bid=28.49000000/12.89044953/18.71330747/0.96000000 ask=46.76666666/25.39126669/21.65962954/1.49000000 beyond | complete book.liquidity.window.5m@1",
        "90120241ms 5m end=90120000ms | 1bps bid=17.94000000/2.90888121/16.43079721/1.11000000 ask=18.91666666/2.86230459/19.65962953/0.76000000 beyond | 2bps bid=22.85000000/6.41795006/17.30886303/1.11000000 ask=32.86999999/9.10299053/20.97629619/0.76000000 beyond | 5bps bid=23.42000000/7.90597475/17.30886303/1.11000000 ask=43.74999999/23.48863784/21.04962952/0.76000000 beyond | complete book.liquidity.window.5m@1",
        "90181512ms 5m end=90180000ms | 1bps bid=18.78000000/2.76078790/17.43531028/1.23000000 ask=19.11000000/3.64444450/18.96008224/0.65000000 beyond | 2bps bid=22.79000000/5.76364278/17.76197694/1.23000000 ask=33.60333333/10.44965716/19.63341557/0.65000000 beyond | 5bps bid=23.81000000/6.43253166/17.76197694/1.23000000 ask=45.37333333/23.67506331/19.63341557/0.65000000 beyond | complete book.liquidity.window.5m@1",
        "90242512ms 5m end=90240000ms | 1bps bid=18.27000000/2.98545182/18.61308807/1.00000000 ask=19.90000000/4.67851855/20.66255136/0.63000000 beyond | 2bps bid=22.79000000/6.16773059/18.94308807/1.00000000 ask=35.07333333/12.65038313/21.33588469/0.63000000 beyond | 5bps bid=24.33000000/6.78106393/18.94308807/1.00000000 ask=46.12333333/25.78736175/21.33588469/0.63000000 beyond | complete book.liquidity.window.5m@1",
        "90300225ms 5m end=90300000ms | 1bps bid=17.93000000/2.49914955/19.28747591/1.10000000 ask=18.78000000/4.14707822/21.83588469/0.93000000 beyond | 2bps bid=22.75000000/4.65190679/19.75747591/1.10000000 ask=34.80333333/12.06875080/22.45921802/0.93000000 beyond | 5bps bid=24.55000000/5.22524013/19.75747591/1.10000000 ask=45.81333333/23.23096030/22.45921802/0.93000000 beyond | complete book.liquidity.window.5m@1",
        "90361215ms 5m end=90360000ms | 1bps bid=18.57000000/2.43581621/19.07809321/0.88000000 ask=19.30000000/4.60629630/22.06514395/0.96000000 beyond | 2bps bid=24.19000000/5.09857345/19.73809321/0.88000000 ask=35.00999999/13.08421032/22.78847728/1.06000000 beyond | 5bps bid=25.99000000/5.67190679/19.73809321/0.88000000 ask=45.82999999/23.40330594/22.78847728/1.06000000 beyond | complete book.liquidity.window.5m@1",
        "90420129ms 5m end=90420000ms | 1bps bid=18.05000000/2.36248287/18.95809322/1.09000000 ask=19.95000000/4.58629630/21.99514396/0.98000000 beyond | 2bps bid=23.66000000/5.14190678/19.89475988/1.09000000 ask=36.27666666/13.46015910/22.77514396/1.08000000 beyond | 5bps bid=25.46000000/5.71524012/19.89475988/1.09000000 ask=48.16666666/25.19578574/22.77514396/1.08000000 beyond | complete book.liquidity.window.5m@1",
        "90480121ms 5m end=90480000ms | 1bps bid=17.24000000/2.06803843/18.34142655/1.54000000 ask=19.35000000/3.94853224/21.76977739/0.90000000 beyond | 2bps bid=23.62000000/4.53338827/19.27809321/1.54000000 ask=34.99666666/13.18205971/22.54977739/1.00000000 beyond | 5bps bid=25.08000000/5.14338828/19.27809321/1.54000000 ask=45.98666666/23.74252560/22.54977739/1.00000000 beyond | complete book.liquidity.window.5m@1",
        "90540231ms 5m end=90540000ms | 1bps bid=17.01000000/0.77888890/19.72957469/1.84000000 ask=19.18333333/2.90285324/21.21656753/1.02000000 beyond | 2bps bid=22.75000000/3.01296300/20.49624135/1.84000000 ask=35.99999999/13.38487282/22.10656753/1.12000000 beyond | 5bps bid=23.38000000/3.22296300/20.49624135/1.84000000 ask=46.49999999/24.16856274/22.10656753/1.12000000 beyond | complete book.liquidity.window.5m@1",
        "90600021ms 5m end=90600000ms | 1bps bid=17.23666666/0.84555557/18.74333329/1.80000000 ask=20.12333333/3.85618659/19.63323420/0.92000000 beyond | 2bps bid=21.93666666/2.77666671/19.20999995/1.80000000 ask=35.69999999/14.93968764/20.25323420/1.02000000 beyond | 5bps bid=22.30666666/3.04666671/19.20999995/1.80000000 ask=48.31999999/27.68680696/20.25323420/1.02000000 beyond | complete book.liquidity.window.5m@1",
        "90660838ms 5m end=90660000ms | 1bps bid=16.51999999/0.90555557/18.46999995/2.10333333 ask=19.67333333/3.04030184/20.23878974/1.05000000 beyond | 2bps bid=20.89999999/2.52333338/19.05666661/2.10333333 ask=35.23333333/13.05805985/21.01916010/1.05000000 beyond | 5bps bid=21.26999999/2.79333338/19.05666661/2.10333333 ask=47.74333333/25.84921211/21.01916010/1.05000000 beyond | complete book.liquidity.window.5m@1",
        "90720710ms 5m end=90720000ms | 1bps bid=16.23666665/0.95555557/18.64666661/1.71333333 ask=21.33333333/3.23030183/20.43212307/1.11000000 beyond | 2bps bid=21.23666665/2.40000004/18.95666661/1.71333333 ask=37.07333333/12.59451161/21.08249343/1.11000000 beyond | 5bps bid=21.60666665/2.67000004/18.95666661/1.71333333 ask=49.22333333/25.56781753/21.11249343/1.11000000 beyond | complete book.liquidity.window.5m@1",
        "90780638ms 5m end=90780000ms | 1bps bid=13.40999998/0.80888890/16.14999995/1.35333333 ask=21.40333333/2.91666671/20.42777771/1.04000000 beyond | 2bps bid=16.34999998/1.94555557/16.45999995/1.35333333 ask=37.24333333/12.49580716/21.21814807/1.18000000 beyond | 5bps bid=16.50999998/2.17888890/16.45999995/1.35333333 ask=50.00333333/29.23331973/21.24814807/1.18000000 beyond | complete book.liquidity.window.5m@1",
        "90840321ms 5m end=90840000ms | 1bps bid=10.41999998/0.72222223/11.38740737/0.88333333 ask=21.47000000/3.17947422/20.18296288/0.86000000 beyond | 2bps bid=11.97999998/0.93592594/11.69740737/0.88333333 ask=36.03000000/12.13768186/20.86333324/1.00000000 beyond | 5bps bid=11.97999998/0.99592594/11.69740737/0.88333333 ask=50.41148149/28.44976237/20.89333324/1.00000000 beyond | complete book.liquidity.window.5m@1",
    ];

    const GOLDEN_WINDOW_15M: [&str; 61] = [
        "87300051ms 15m end=87300000ms | 1bps bid=60.25000000/22.94062663/70.92296272/2.75000000 ask=54.45333333/24.61080036/73.78106508/2.30000000 | 2bps bid=101.28000000/70.07802956/73.50444415/2.76000000 ask=98.94666666/78.32206786/75.65810210/2.47000000 | 5bps bid=133.18000000/175.74954363/73.70444415/2.76000000 ask=134.78999998/208.77972732/75.65810210/2.47000000 beyond | partial_start book.liquidity.window.15m@1",
        "87360209ms 15m end=87360000ms | 1bps bid=61.97666666/12.04618218/68.11740716/3.21000000 ask=57.67333333/14.51005959/70.55662065/2.30000000 | 2bps bid=102.99666666/40.10484711/70.69888859/3.22000000 ask=102.24666666/51.00654507/72.43365767/2.47000000 | 5bps bid=132.96666666/132.73588103/70.89888859/3.22000000 ask=138.51999998/163.97020358/72.43365767/2.47000000 beyond | complete book.liquidity.window.15m@1",
        "87420411ms 15m end=87420000ms | 1bps bid=62.87666666/9.49556489/68.44962936/3.33000000 ask=58.22333333/14.69561515/66.50810212/2.52000000 | 2bps bid=103.31666666/33.09575856/71.12111079/3.34000000 ask=101.74666666/47.39124129/68.47513914/2.69000000 | 5bps bid=133.15666666/107.36847969/71.32111079/3.34000000 ask=139.59999998/145.02298497/68.47513914/2.69000000 beyond | complete book.liquidity.window.15m@1",
        "87480026ms 15m end=87480000ms | 1bps bid=63.39666666/7.73761334/68.10962937/3.33000000 ask=59.77333333/10.77783738/64.49118856/2.53000000 | 2bps bid=102.61666666/30.83320405/70.70111080/3.34000000 ask=104.10666666/39.02288849/66.45822558/2.70000000 | 5bps bid=132.36666666/76.50037114/70.90111080/3.34000000 ask=141.12999998/122.92878849/66.45822558/2.70000000 beyond | complete book.liquidity.window.15m@1",
        "87540252ms 15m end=87540000ms | 1bps bid=64.20666666/8.55094667/67.68666642/3.34000000 ask=59.55333333/12.79079476/65.24341077/2.89000000 | 2bps bid=104.91666666/29.64817436/70.39814785/3.35000000 ask=103.73666666/40.10780849/67.16044779/3.06000000 | 5bps bid=135.13666666/66.05609137/70.59814785/3.35000000 ask=140.33999998/110.74799901/67.16044779/3.06000000 beyond | complete book.liquidity.window.15m@1",
        "87600024ms 15m end=87600000ms | 1bps bid=63.68666666/9.05650223/66.64444419/3.27000000 ask=59.49333333/11.22193111/63.77914025/3.09000000 | 2bps bid=105.23666666/30.47810578/69.35592562/3.28000000 ask=103.31666666/38.63541997/65.69617727/3.26000000 | 5bps bid=135.44666666/66.10449560/69.55592562/3.28000000 ask=141.62999998/90.38689231/65.69617727/3.26000000 beyond | complete book.liquidity.window.15m@1",
        "87660103ms 15m end=87660000ms | 1bps bid=65.21666666/10.15823062/67.66678983/3.08000000 ask=59.67000000/11.43043137/62.58580691/3.11000000 | 2bps bid=105.37666666/33.54274229/70.17827126/3.08000000 ask=103.74333333/37.65962667/64.20284393/3.28000000 | 5bps bid=137.05666666/68.34474253/70.37827126/3.08000000 ask=143.34666665/80.45799434/64.20284393/3.28000000 beyond | complete book.liquidity.window.15m@1",
        "87720617ms 15m end=87720000ms | 1bps bid=64.15666666/10.48502073/66.32844960/3.31000000 ask=61.58000000/11.08487580/64.31008675/3.62000000 | 2bps bid=104.15666666/33.60525258/68.65030141/3.31000000 ask=105.38333333/36.76793941/65.65712377/3.79000000 beyond | 5bps bid=134.76148148/67.88625145/68.85030141/3.31000000 ask=143.38666665/78.73927768/65.65712377/3.79000000 beyond | complete book.liquidity.window.15m@1",
        "87780301ms 15m end=87780000ms | 1bps bid=63.66666666/11.26390961/66.42794205/3.09000000 ask=62.19000000/10.43487579/63.46597613/3.79000000 | 2bps bid=104.89666666/35.40932667/69.30646052/3.09000000 ask=105.45333333/35.09329529/65.40634648/3.96000000 beyond | 5bps bid=133.79148148/67.38829532/69.50646052/3.09000000 ask=141.34666665/75.50030038/65.40634648/3.96000000 beyond | complete book.liquidity.window.15m@1",
        "87840566ms 15m end=87840000ms | 1bps bid=63.80666666/11.81168739/65.58349762/3.37000000 ask=63.06000000/10.32043135/64.66597613/3.73333333 beyond | 2bps bid=104.96666666/35.88796864/68.55201609/3.37000000 ask=107.02333333/34.21255454/67.18634648/3.90333333 beyond | 5bps bid=132.55148148/65.35216362/68.75201609/3.37000000 ask=142.11333333/74.86985453/67.18634648/3.90333333 beyond | complete book.liquidity.window.15m@1",
        "87900214ms 15m end=87900000ms | 1bps bid=64.67666666/12.17786023/64.46683097/3.25000000 ask=65.08000000/10.76070569/65.12029710/3.86333333 beyond | 2bps bid=105.09666666/35.45204269/67.13238648/3.25000000 ask=108.86246913/36.30196472/67.35251931/3.86333333 beyond | 5bps bid=131.75148148/64.46271346/67.33238648/3.25000000 ask=143.74246913/75.86111145/67.35251931/3.86333333 beyond | complete book.liquidity.window.15m@1",
        "87960704ms 15m end=87960000ms | 1bps bid=66.49666666/12.27341577/64.94905320/3.03000000 ask=65.60000000/11.20292791/65.98696373/3.72333333 beyond | 2bps bid=107.11666666/36.32907972/67.53460871/3.03000000 ask=108.16246913/35.17747914/68.13918594/3.72333333 beyond | 5bps bid=132.23148148/64.44015698/67.73460871/3.03000000 ask=141.45246913/73.35227288/68.13918594/3.72333333 beyond | complete book.liquidity.window.15m@1",
        "88020027ms 15m end=88020000ms | 1bps bid=65.95666666/12.14341575/64.96238654/3.17000000 ask=64.97000000/11.07477977/65.07103781/4.03333333 beyond | 2bps bid=104.51666666/35.26426486/67.83423836/3.17000000 ask=107.01913580/35.30025008/67.04770446/4.03333333 beyond | 5bps bid=127.79148148/62.13307418/68.03423836/3.17000000 ask=139.66913580/72.28169380/67.04770446/4.03333333 beyond | complete book.liquidity.window.15m@1",
        "88080982ms 15m end=88080000ms | 1bps bid=66.36666666/11.50267501/66.19016434/3.26000000 ask=66.14000000/11.12556930/66.36597604/4.01333333 beyond | 2bps bid=104.98666666/34.44130189/68.77646062/3.26000000 ask=107.74913580/36.69706606/68.28264269/4.01333333 beyond | 5bps bid=126.73148148/59.22883965/68.77646062/3.26000000 ask=141.98913580/72.11622206/68.28264269/4.01333333 beyond | complete book.liquidity.window.15m@1",
        "88140362ms 15m end=88140000ms | 1bps bid=66.79666666/12.11489722/66.31905323/3.24000000 ask=66.13000000/10.76009606/65.33153159/4.03333333 beyond | 2bps bid=104.77666666/34.90836637/68.60201617/3.24000000 ask=106.95913580/35.76343552/67.07153158/4.03333333 beyond | 5bps bid=124.61148148/56.84318807/68.60201617/3.24000000 ask=139.61913580/72.31090223/67.07153158/4.03333333 beyond | complete book.liquidity.window.15m@1",
        "88200324ms 15m end=88200000ms | 1bps bid=64.83666666/11.91156388/63.86238658/3.00000000 ask=66.20000000/10.09528123/65.71301309/4.06333333 beyond | 2bps bid=100.84666666/34.26231699/66.44279806/3.00000000 ask=108.12913580/35.29752094/67.95301308/4.06333333 beyond | 5bps bid=118.52148148/54.30535007/66.44279806/3.00000000 ask=140.83913580/72.19067463/67.95301308/4.06333333 beyond | complete book.liquidity.window.15m@1",
        "88260420ms 15m end=88260000ms | 1bps bid=63.27000000/11.19563794/63.08794213/2.65000000 ask=64.42000000/9.79120718/64.20375380/4.35333333 beyond | 2bps bid=96.34000000/31.93142531/65.66835361/2.65000000 ask=106.66913580/34.04859926/66.47264267/4.35333333 beyond | 5bps bid=112.18481482/49.54790148/65.66835361/2.65000000 ask=138.25913580/71.79858878/66.47264267/4.35333333 beyond | complete book.liquidity.window.15m@1",
        "88320026ms 15m end=88320000ms | 1bps bid=59.16000000/10.40172848/58.27905326/2.51000000 ask=65.10000000/10.24454050/64.31708715/4.56333333 beyond | 2bps bid=89.26000000/29.11040413/60.76946474/2.51000000 ask=108.62913580/34.58308000/66.49597602/4.56333333 beyond | 5bps bid=102.39481482/44.46774448/60.76946474/2.51000000 ask=138.42913580/71.88013108/66.49597602/4.56333333 beyond | complete book.liquidity.window.15m@1",
        "88381101ms 15m end=88380000ms | 1bps bid=55.97333333/10.13950625/55.25460882/2.53333333 ask=63.71000000/9.39762691/63.76992666/4.31333333 beyond | 2bps bid=83.87333333/26.99636509/57.74502030/2.53333333 ask=106.01913580/32.12538345/66.06881553/4.31333333 beyond | 5bps bid=94.08814815/39.57037060/57.74502030/2.53333333 ask=136.08913580/70.63992423/66.06881553/4.31333333 beyond | complete book.liquidity.window.15m@1",
        "88440617ms 15m end=88440000ms | 1bps bid=52.23333333/8.56950624/52.22794216/2.63333333 ask=64.72000000/7.94578066/63.63659333/4.11333333 beyond | 2bps bid=76.24333333/24.27821691/54.58835364/2.63333333 ask=106.64913580/31.48206843/66.04437108/4.11333333 beyond | 5bps bid=84.35814815/33.11164631/54.58835364/2.63333333 ask=137.67913580/70.17215404/66.04437108/4.11333333 beyond | complete book.liquidity.window.15m@1",
        "88500218ms 15m end=88500000ms | 1bps bid=49.25333333/7.92728402/50.30016439/2.72333333 ask=63.84000000/9.25378012/62.66913547/4.05333333 beyond | 2bps bid=70.34333333/21.57216752/52.66057587/2.72333333 ask=106.48913580/31.61977932/65.11691322/4.05333333 beyond | 5bps bid=76.27814815/27.30362158/52.66057587/2.72333333 ask=138.13913580/69.75572988/65.11691322/4.05333333 beyond | complete book.liquidity.window.15m@1",
        "88560502ms 15m end=88560000ms | 1bps bid=45.56333333/6.60222229/46.23781875/2.68333333 ask=63.66000000/9.34155787/63.52913548/4.07333333 beyond | 2bps bid=65.26333333/17.55567916/48.46823023/2.68333333 ask=106.16913580/33.88611673/65.97691323/4.07333333 beyond | 5bps bid=68.42814815/21.59629645/48.46823023/2.68333333 ask=138.30913580/72.16024745/65.97691323/4.07333333 beyond | complete book.liquidity.window.15m@1",
        "88621115ms 15m end=88620000ms | 1bps bid=42.03333333/5.97839514/43.15615898/2.46333333 ask=62.60000000/9.72995295/61.46259226/3.63333333 beyond | 2bps bid=59.24333333/15.70119356/45.31657046/2.46333333 ask=105.77839506/35.89920319/63.88037001/3.63333333 beyond | 5bps bid=61.85333333/19.03218123/45.31657046/2.46333333 ask=138.23839506/74.02311138/63.88037001/3.63333333 beyond | complete book.liquidity.window.15m@1",
        "88680029ms 15m end=88680000ms | 1bps bid=39.69333333/5.14283958/40.17333321/2.27333333 ask=62.60000000/9.67933569/61.70469100/3.19333333 beyond | 2bps bid=54.26333333/13.43045278/41.71707803/2.27333333 ask=107.36839506/37.49573270/63.64913542/3.19333333 beyond | 5bps bid=55.85333333/16.20366269/41.71707803/2.27333333 ask=140.50839506/75.25181893/63.64913542/3.19333333 beyond | complete book.liquidity.window.15m@1",
        "88740239ms 15m end=88740000ms | 1bps bid=36.91333333/4.38172846/36.58666655/1.96333333 ask=62.91000000/10.08155791/62.16469099/3.11000000 beyond | 2bps bid=48.21333333/11.00551450/37.99041137/1.96333333 ask=106.71839506/38.46687128/63.80913541/3.11000000 beyond | 5bps bid=49.63333333/13.27847750/37.99041137/1.96333333 ask=139.65839506/76.05439557/63.80913541/3.11000000 beyond | complete book.liquidity.window.15m@1",
        "88802510ms 15m end=88800000ms | 1bps bid=34.90333333/3.46333340/35.17333323/1.99333333 ask=61.69000000/9.06625840/62.50357992/3.19000000 beyond | 2bps bid=44.18333333/9.11440340/36.57707805/1.99333333 ask=105.53925926/36.38865651/64.31320952/3.19000000 beyond | 5bps bid=45.02333333/9.87625528/36.57707805/1.99333333 ask=138.64925926/74.83375337/64.31320952/3.19000000 beyond | complete book.liquidity.window.15m@1",
        "88820100ms 15m end=88800000ms | 1bps bid=34.90333333/3.46333340/35.17333323/1.99333333 ask=61.69000000/9.06625840/62.50357992/3.19000000 beyond | 2bps bid=44.18333333/9.11440340/36.57707805/1.99333333 ask=105.53925926/36.38865651/64.31320952/3.19000000 beyond | 5bps bid=45.02333333/9.87625528/36.57707805/1.99333333 ask=138.64925926/74.83375337/64.31320952/3.19000000 beyond | feed_gap book.liquidity.window.15m@1",
        "88860318ms 15m end=88860000ms | 1bps bid=32.16333333/3.01333339/33.03333321/2.03333333 ask=60.88000000/8.71662876/62.15543182/3.46000000 beyond | 2bps bid=41.23333333/8.70851200/34.64041137/2.03333333 ask=103.84925926/34.67915028/64.23506142/3.46000000 beyond | 5bps bid=42.55333333/10.30598343/34.64041137/2.03333333 ask=136.98925926/73.53178580/64.23506142/3.46000000 beyond | feed_gap book.liquidity.window.15m@1",
        "88920328ms 15m end=88920000ms | 1bps bid=32.79666666/3.55333339/32.78621962/1.73333333 ask=61.06000000/8.56033245/61.56876516/3.29000000 beyond | 2bps bid=44.29333333/11.63948374/33.68033482/1.73333333 ask=104.32925926/34.04709266/63.49172810/3.29000000 beyond | 5bps bid=47.15333333/14.86131732/33.68033482/1.73333333 ask=137.39925926/73.12672596/63.49172810/3.29000000 beyond | feed_gap book.liquidity.window.15m@1",
        "88980029ms 15m end=88980000ms | 1bps bid=32.92666666/4.82641981/32.93708837/1.63333333 ask=61.91000000/8.65546885/62.00716025/3.34000000 beyond | 2bps bid=45.57333333/14.97288109/33.83120357/1.63333333 ask=105.63925926/33.78361003/63.92012319/3.34000000 beyond | 5bps bid=48.79333333/18.57360356/33.83120357/1.63333333 ask=138.36259259/74.80590144/63.92012319/3.34000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89040057ms 15m end=89040000ms | 1bps bid=31.90333332/4.41086425/32.10931058/1.61333333 ask=61.94000000/9.30978985/64.47999972/3.72000000 beyond | 2bps bid=45.10999999/14.02958889/33.24675912/1.61333333 ask=106.56925926/34.32607920/66.39296266/3.72000000 beyond | 5bps bid=50.64999999/20.24984496/33.24675912/1.61333333 ask=140.05259259/73.34126246/66.39296266/3.72000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89100976ms 15m end=89100000ms | 1bps bid=32.78333332/5.71753091/31.88931058/1.72333333 ask=60.80000000/9.89904910/63.85333305/3.56000000 beyond | 2bps bid=47.79111110/15.80736665/32.87931058/1.72333333 ask=104.18925926/34.89953599/65.49148117/3.56000000 beyond | 5bps bid=55.77111110/24.33092863/32.87931058/1.72333333 ask=138.57925925/74.29831999/65.49148117/3.56000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89160252ms 15m end=89160000ms | 1bps bid=33.39333332/5.58086425/32.10597726/1.95333333 ask=61.17000000/10.43343868/65.07392288/3.74000000 beyond | 2bps bid=50.96111110/16.90181111/33.20597726/1.95333333 ask=104.46925926/35.82734119/66.69021915/3.74000000 beyond | 5bps bid=60.56111110/27.68648421/33.20597726/1.95333333 ask=140.31925925/73.79621209/66.69021915/3.74000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89220525ms 15m end=89220000ms | 1bps bid=36.57333332/7.61288071/37.29630646/2.36333333 ask=61.29000000/9.67010536/65.17058954/3.20000000 beyond | 2bps bid=57.57111110/19.85777819/38.40630646/2.36333333 ask=103.46592592/33.36477604/66.84688581/3.20000000 beyond | 5bps bid=69.16111110/31.95504387/38.40630646/2.36333333 ask=140.06592591/72.32546255/66.84688581/3.20000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89280655ms 15m end=89280000ms | 1bps bid=40.17999999/8.72436222/40.64149163/2.44000000 ask=62.90000000/9.91738931/66.84997225/3.07000000 beyond | 2bps bid=63.54777777/22.58851896/41.95149163/2.44000000 ask=105.22592592/33.76403528/68.79626852/3.07000000 beyond | 5bps bid=75.95777777/35.34652539/41.95149163/2.44000000 ask=141.80592591/72.82076357/68.79626852/3.07000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89340430ms 15m end=89340000ms | 1bps bid=42.65999999/9.05102890/43.97149162/2.37000000 ask=61.78000000/9.80294484/67.91775003/3.02000000 beyond | 2bps bid=69.75777777/24.47074121/45.68482495/2.37000000 ask=103.84592592/32.55159356/70.07936866/3.02000000 beyond | 5bps bid=84.12777777/39.97924147/45.68482495/2.37000000 ask=139.39592591/70.70588019/70.07936866/3.02000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89401218ms 15m end=89400000ms | 1bps bid=45.68999999/9.11658446/46.21149161/2.32000000 ask=61.38000000/8.17183372/68.85441671/3.06000000 beyond | 2bps bid=75.33777777/25.85736673/48.19482494/2.32000000 ask=103.25592592/30.31343170/71.09603534/3.06000000 beyond | 5bps bid=90.67777777/42.12006452/48.19482494/2.32000000 ask=136.88592591/68.07138091/71.09603534/3.06000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89460119ms 15m end=89460000ms | 1bps bid=49.17999999/11.11304536/48.38815827/2.40000000 ask=61.68000000/9.21775964/68.89812039/3.14000000 beyond | 2bps bid=81.68777777/30.56388251/50.37149160/2.40000000 ask=104.09592592/31.30783500/71.13973902/3.14000000 beyond | 5bps bid=98.93777777/49.48625109/50.37149160/2.40000000 ask=136.99592591/67.05677183/71.13973902/3.14000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89520304ms 15m end=89520000ms | 1bps bid=53.65999999/11.19304537/52.95149159/2.67000000 ask=61.88000000/8.98010530/70.32750868/3.40000000 beyond | 2bps bid=89.67777777/33.36652999/55.01482492/2.67000000 ask=105.29666666/30.69436801/72.76912731/3.40000000 beyond | 5bps bid=109.24777777/55.14662706/55.01482492/2.67000000 ask=138.79666665/66.51591573/72.76912731/3.40000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89580118ms 15m end=89580000ms | 1bps bid=56.21666665/11.98316883/55.80038046/2.83000000 ask=62.19000000/8.84850034/70.61985439/3.88000000 beyond | 2bps bid=94.99444443/36.07887569/57.80371379/2.83000000 ask=104.91666666/28.86080149/72.97147302/3.88000000 beyond | 5bps bid=116.01444443/59.59706043/57.80371379/2.83000000 ask=139.52666665/66.57249094/72.97147302/3.88000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89640122ms 15m end=89640000ms | 1bps bid=58.37666665/13.20094660/59.16530041/2.83000000 ask=61.63000000/8.53294479/69.81652106/3.80000000 beyond | 2bps bid=99.53444443/39.10527256/61.16863374/2.83000000 ask=104.78999999/27.47595922/72.13813969/3.80000000 beyond | 5bps bid=122.89259258/63.74913635/61.16863374/2.83000000 ask=139.54333331/65.44068407/72.13813969/3.80000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89730000ms 15m end=89700000ms | 1bps bid=62.23666665/13.84427994/62.56748146/3.07000000 ask=62.24000000/8.93500697/69.93849636/3.79000000 | 2bps bid=105.56444443/40.81943353/65.00081479/3.12000000 ask=105.09999999/28.87155391/72.09492981/3.79000000 beyond | 5bps bid=130.89259258/66.50111627/65.00081479/3.12000000 ask=140.09333331/66.16451977/72.09492981/3.79000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89760807ms 15m end=89760000ms | 1bps bid=64.29666665/17.38427997/61.92081480/2.92000000 ask=63.23666667/11.37945143/69.17368152/3.61000000 | 2bps bid=108.01444443/43.50913535/63.95081479/2.97000000 ask=108.61666666/32.89575147/70.79011497/3.61000000 beyond | 5bps bid=133.41259258/69.50038370/63.95081479/2.97000000 ask=143.72777776/71.49566815/70.79011497/3.61000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89821458ms 15m end=89820000ms | 1bps bid=64.86333332/17.21847749/63.04237285/2.94000000 ask=63.85666667/11.66278477/70.13701486/4.17000000 | 2bps bid=108.76444443/42.84187189/65.22015061/2.99000000 ask=110.02666666/33.50669799/71.75344831/4.17000000 beyond | 5bps bid=134.12259258/69.25063570/65.22015061/2.99000000 ask=145.32777776/72.48540217/71.75344831/4.17000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89881201ms 15m end=89880000ms | 1bps bid=63.48333332/16.12459548/60.80365770/2.79000000 ask=62.49333333/11.74871070/69.81294076/4.17000000 beyond | 2bps bid=105.46444443/40.32574479/63.48283462/2.84000000 ask=109.42333332/33.54595728/72.06270754/4.17000000 beyond | 5bps bid=130.42259258/66.93401477/63.48283462/2.84000000 ask=143.85111109/72.49944612/72.13604087/4.17000000 beyond | feed_gap book.liquidity.window.15m@1",
        "89941441ms 15m end=89940000ms | 1bps bid=64.07666666/16.81241442/60.07699104/3.06000000 ask=61.56333333/11.05599463/66.90750871/3.78000000 beyond | 2bps bid=104.79777777/41.20097113/62.66283462/3.11000000 ask=107.19333332/32.37175972/69.15727549/3.78000000 beyond | 5bps bid=127.90592592/65.81859639/62.66283462/3.11000000 ask=141.67111109/72.43949179/69.23060882/3.78000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90000008ms 15m end=90000000ms | 1bps bid=63.17666666/16.35427225/60.47291866/2.99000000 ask=62.61333333/10.81484237/66.25491613/3.72000000 beyond | 2bps bid=102.12666666/40.06790604/63.06876224/3.04000000 ask=108.28333332/31.35084061/68.59949773/3.72000000 beyond | 5bps bid=122.79481481/62.40889205/63.06876224/3.04000000 ask=142.51444443/72.76951461/68.67283106/3.72000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90060515ms 15m end=90060000ms | 1bps bid=62.45666666/16.16427225/60.30563470/2.87000000 ask=62.24333333/10.36823056/65.60765965/3.62000000 beyond | 2bps bid=98.82666666/38.24346159/62.79147828/2.92000000 ask=108.05333332/31.27118354/68.08520422/3.62000000 beyond | 5bps bid=117.84481481/58.32333648/62.79147828/2.92000000 ask=140.84444443/73.17940487/68.15853755/3.62000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90120241ms 15m end=90120000ms | 1bps bid=62.06666666/14.20225579/57.79863883/2.72000000 ask=60.50333333/10.55156389/65.25765964/3.62000000 beyond | 2bps bid=95.19666666/35.41416118/60.27448241/2.77000000 ask=105.36999999/31.88374869/67.88853754/3.62000000 beyond | 5bps bid=112.22481481/54.18144349/60.27448241/2.77000000 ask=137.26111110/71.12397732/67.96187087/3.62000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90181512ms 15m end=90180000ms | 1bps bid=62.68666666/13.53188539/58.15678699/2.75000000 ask=60.86333333/11.53851863/64.71737159/3.77000000 beyond | 2bps bid=94.73666666/33.74638339/60.43263057/2.80000000 ask=107.86999999/33.69547710/66.95824949/3.77000000 beyond | 5bps bid=111.49481481/51.85292495/60.43263057/2.80000000 ask=140.54111110/71.72524999/67.03158282/3.77000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90242512ms 15m end=90240000ms | 1bps bid=63.11666666/14.58103491/57.77123144/2.78000000 ask=61.25333333/12.49753097/64.24058145/3.72000000 beyond | 2bps bid=93.33666666/34.39421604/59.81374169/2.83000000 ask=109.05999999/36.17864479/66.15724811/3.72000000 beyond | 5bps bid=109.12481481/50.32359711/59.81374169/2.83000000 ask=141.67111110/75.88830746/66.23058144/3.72000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90300225ms 15m end=90300000ms | 1bps bid=61.19666666/14.91436823/58.24488025/2.86000000 ask=61.07333333/12.48086430/64.39724811/3.73000000 beyond | 2bps bid=90.17666666/33.72647940/60.31739050/2.91000000 ask=109.78999999/36.52395345/66.46391477/3.73000000 beyond | 5bps bid=105.25481481/48.90166294/60.31739050/2.91000000 ask=142.85111110/76.07659274/66.53724810/3.73000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90361215ms 15m end=90360000ms | 1bps bid=59.66666666/12.83790733/57.63821359/2.72000000 ask=61.01333333/12.32304536/64.45354442/3.95000000 beyond | 2bps bid=86.59666666/29.44996362/59.90072384/2.77000000 ask=108.71666665/35.55941298/66.76021108/4.05000000 beyond | 5bps bid=99.76481481/41.96547637/59.90072384/2.77000000 ask=141.46777776/74.90881495/66.83354441/4.05000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90420129ms 15m end=90420000ms | 1bps bid=57.76666666/12.76790732/55.93488027/2.81000000 ask=60.30333333/12.34119352/63.23958825/3.69000000 beyond | 2bps bid=81.36666666/26.83064948/58.39405718/2.86000000 ask=107.09666665/34.58611269/65.61625491/3.79000000 beyond | 5bps bid=92.21481481/36.48843374/58.39405718/2.86000000 ask=140.46777776/74.17605877/65.68958824/3.79000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90480121ms 15m end=90480000ms | 1bps bid=57.87000000/12.09445053/55.44599139/3.21000000 ask=60.10333333/12.43260642/63.18282250/3.21000000 beyond | 2bps bid=80.71000000/24.54274825/57.90516830/3.26000000 ask=107.50666665/36.00097627/65.52948916/3.31000000 beyond | 5bps bid=90.31814815/32.49911151/57.90516830/3.26000000 ask=140.49777776/73.72730097/65.60282249/3.31000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90540231ms 15m end=90540000ms | 1bps bid=58.00000000/10.85000609/56.50699736/3.75000000 ask=59.72666666/12.47593975/62.97837805/3.19000000 beyond | 2bps bid=79.84000000/22.41264767/58.96617427/3.80000000 ask=108.25666665/38.81352773/65.18504471/3.29000000 beyond | 5bps bid=87.27000000/29.41666521/58.96617427/3.80000000 ask=140.93444443/77.07653807/65.25837804/3.29000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90600021ms 15m end=90600000ms | 1bps bid=55.43666666/10.78889498/54.42555703/3.64000000 ask=59.85666666/12.81001384/60.96319287/3.06000000 beyond | 2bps bid=75.37666666/21.29774597/56.45473394/3.64000000 ask=107.70666665/38.63105862/63.16985953/3.16000000 beyond | 5bps bid=80.83666666/27.32394456/56.45473394/3.64000000 ask=142.08444443/78.29072051/63.24319286/3.16000000 beyond | feed_gap book.liquidity.window.15m@1",
        "90660838ms 15m end=90660000ms | 1bps bid=53.68999999/7.22222829/55.23555704/3.94333333 ask=59.39999999/10.53556939/62.78689657/3.50000000 beyond | 2bps bid=71.50999999/16.98245406/57.50806729/3.94333333 ask=105.87999998/35.64056935/65.39393359/3.60000000 beyond | 5bps bid=75.74999999/21.35568970/57.50806729/3.94333333 ask=140.33999998/74.64378474/65.46726692/3.60000000 beyond | complete book.liquidity.window.15m@1",
        "90720710ms 15m end=90720000ms | 1bps bid=52.22666665/6.22691965/54.03555704/3.91333333 ask=60.19999999/10.67890272/62.08689656/2.85000000 beyond | 2bps bid=67.74666665/13.95985688/56.16028952/3.91333333 ask=106.21999998/35.15766124/64.83393358/2.95000000 beyond | 5bps bid=70.48666665/16.29121491/56.16028952/3.91333333 ask=141.13999998/74.25224111/64.93726691/2.95000000 beyond | complete book.liquidity.window.15m@1",
        "90780638ms 15m end=90780000ms | 1bps bid=49.42999998/5.63771523/51.92673678/4.12333333 ask=59.86333333/10.50964345/61.15763734/2.59000000 beyond | 2bps bid=62.75999998/12.24258662/53.50007010/4.12333333 ask=105.84333332/36.12752403/63.40134103/2.83000000 beyond | 5bps bid=65.39999998/13.75480884/53.50007010/4.12333333 ask=141.36333332/76.65090864/63.43134103/2.83000000 beyond | complete book.liquidity.window.15m@1",
        "90840321ms 15m end=90840000ms | 1bps bid=45.69999998/4.48656295/49.73007013/3.72333333 ask=60.55333333/10.76084601/62.06208177/2.51000000 beyond | 2bps bid=57.51999998/10.11661953/51.13673679/3.72333333 ask=107.10333332/38.17293781/64.30578546/2.75000000 beyond | 5bps bid=59.68999998/10.99995287/51.13673679/3.72333333 ask=143.03481481/78.40568686/64.33578546/2.75000000 beyond | complete book.liquidity.window.15m@1",
    ];

    const GOLDEN_WINDOW_1H: [&str; 15] = [
        "90000008ms 1h end=90000000ms | 1bps bid=221.04666664/56.92399367/227.14757854/10.46333333 ask=244.06666666/55.41997306/269.60232735/13.64333333 beyond | 2bps bid=352.04444442/160.21561924/235.89531503/10.52333333 ask=419.54839504/179.86996540/277.70209408/13.81333333 beyond | 5bps bid=430.26740739/316.79471438/236.09531503/10.52333333 ask=556.72283946/428.03823655/277.77542741/13.81333333 beyond | partial_start+feed_gap book.liquidity.window.1h@1",
        "90060515ms 1h end=90060000ms | 1bps bid=221.09666664/44.98695662/223.61696125/10.68333333 ask=245.50666666/45.10293601/265.44195698/14.01333333 beyond | 2bps bid=349.12444442/127.18154512/232.36469774/10.74333333 ask=421.43839504/152.15366906/273.68172371/14.18333333 beyond | 5bps bid=423.55740739/268.29360320/232.56469774/10.74333333 ask=557.94283946/382.74440932/273.75505704/14.18333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90120241ms 1h end=90120000ms | 1bps bid=220.67666664/41.71242987/221.82362791/10.92333333 ask=245.11666666/45.16182490/261.25343845/13.90333333 beyond | 2bps bid=345.34444442/117.47810206/230.57136440/10.98333333 ask=419.21172837/147.22284602/269.70653851/14.07333333 beyond | 5bps bid=416.93740739/237.97271153/230.77136440/10.98333333 ask=555.35617279/360.35255592/269.77987184/14.07333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90181512ms 1h end=90180000ms | 1bps bid=222.23666664/40.13336720/222.16251681/11.05333333 ask=247.24666666/41.63137223/259.82845906/13.68333333 beyond | 2bps bid=344.77444442/114.16447149/230.83025330/11.11333333 ask=423.22172837/138.60778432/268.28155912/13.85333333 beyond | 5bps bid=413.90740739/203.27019208/231.03025330/11.11333333 ask=559.56617279/338.11472628/268.35489245/13.85333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90242512ms 1h end=90240000ms | 1bps bid=222.21666664/40.75251672/221.65733164/11.12333333 ask=247.30666666/43.03705123/261.03833558/13.74333333 beyond | 2bps bid=344.25444442/112.79134852/230.48506813/11.18333333 ask=423.29172837/140.32011527/269.44143564/13.91333333 beyond | 5bps bid=412.74740739/189.47057626/230.68506813/11.18333333 ask=559.08617279/327.51434070/269.51476897/13.91333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90300225ms 1h end=90300000ms | 1bps bid=219.82666664/41.01473894/221.40098044/11.17333333 ask=245.78666666/41.12840925/259.69994054/13.93333333 beyond | 2bps bid=341.09444442/111.63411943/230.52871693/11.23333333 ask=422.85172837/137.09258444/268.37304060/14.10333333 beyond | 5bps bid=407.65740739/184.42984464/230.72871693/11.23333333 ask=559.50617279/304.29059584/268.44637393/14.10333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90361215ms 1h end=90360000ms | 1bps bid=219.62666664/40.71140560/219.93098044/10.88333333 ask=246.02333333/42.31279424/259.46660720/14.27333333 beyond | 2bps bid=338.92444442/111.11226758/228.91871693/10.93333333 ask=422.72506170/138.41299138/268.07970726/14.54333333 beyond | 5bps bid=404.18740739/181.39276644/229.11871693/10.93333333 ask=560.11950612/294.58382857/268.15304059/14.54333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90420129ms 1h end=90420000ms | 1bps bid=217.61666664/40.42436856/218.37098044/11.25333333 ask=246.36333333/42.13612757/259.33977594/14.34333333 beyond | 2bps bid=334.44444442/109.50362561/227.37575397/11.30333333 ask=423.55506170/137.94762330/267.92287600/14.61333333 beyond | 5bps bid=398.07740739/178.55349348/227.57575397/11.30333333 ask=560.88950612/293.45436356/267.99620933/14.61333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90480121ms 1h end=90480000ms | 1bps bid=217.44666664/40.48436855/217.84764711/11.40333333 ask=247.08333333/41.39531824/258.97334402/14.07333333 beyond | 2bps bid=334.86444442/109.46140339/226.73242064/11.45333333 ask=425.24506170/137.45080575/267.55644408/14.34333333 beyond | 5bps bid=395.97740739/175.68812995/226.93242064/11.45333333 ask=561.87950612/291.05191122/267.62977741/14.34333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90540231ms 1h end=90540000ms | 1bps bid=217.09666664/40.24436854/217.84246194/11.91333333 ask=247.32666666/41.41087380/259.62556623/13.83333333 beyond | 2bps bid=332.55444442/108.41140337/226.67723547/11.96333333 ask=426.78839503/138.96891277/268.31866629/14.10333333 beyond | 5bps bid=392.34740739/171.79644268/226.87723547/11.96333333 ask=562.24950613/293.44147224/268.39199962/14.10333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90600021ms 1h end=90600000ms | 1bps bid=217.25333330/40.27436855/216.63320269/11.95333333 ask=248.86666666/41.57198490/258.52556625/13.90333333 beyond | 2bps bid=330.22111108/106.68362559/225.16501326/12.00333333 ask=427.20839503/140.19323376/266.93051817/14.00333333 beyond | 5bps bid=388.50407405/168.16402957/225.36501326/12.00333333 ask=564.56950613/295.15010510/267.00385150/14.00333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90660838ms 1h end=90660000ms | 1bps bid=216.64666663/39.89325742/215.13875825/11.92666666 ask=249.11666666/41.83457749/260.10297364/14.29333333 beyond | 2bps bid=327.87444441/105.52918113/223.63390216/11.97666666 ask=426.50839503/138.39295024/268.55829592/14.39333333 beyond | 5bps bid=383.94740738/165.60221381/223.83390216/11.97666666 ask=562.50950613/293.02351157/268.63162925/14.39333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90720710ms 1h end=90720000ms | 1bps bid=215.84333329/39.14214628/214.82653605/11.75666666 ask=250.08666666/41.97679971/258.86371439/14.34333333 beyond | 2bps bid=325.32111107/103.70547737/222.89501331/11.80666666 ask=427.59506170/138.01170197/267.12681445/14.44333333 beyond | 5bps bid=379.55407404/162.53624211/223.09501331/11.80666666 ask=563.53617280/292.14606304/267.23014778/14.44333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90780638ms 1h end=90780000ms | 1bps bid=212.20666662/38.09140553/211.85764719/11.80666666 ask=250.40666666/42.03939230/259.34371439/14.11333333 beyond | 2bps bid=318.78444440/101.98251439/219.59056891/11.85666666 ask=428.65506170/140.15415740/267.66681445/14.35333333 beyond | 5bps bid=371.34740737/158.49126682/219.59056891/11.85666666 ask=565.56617280/296.07247826/267.77014778/14.35333333 beyond | feed_gap book.liquidity.window.1h@1",
        "90840321ms 1h end=90840000ms | 1bps bid=208.47666662/37.82473884/208.23542498/11.63666666 ask=250.18666666/41.88672655/258.78112179/14.04333333 beyond | 2bps bid=312.20444440/100.25554592/215.64834670/11.68666666 ask=427.82506170/140.63421225/266.92755519/14.28333333 beyond | 5bps bid=362.85740737/153.91158229/215.64834670/11.68666666 ask=564.37765429/296.49734334/267.03088852/14.28333333 beyond | feed_gap book.liquidity.window.1h@1",
    ];
}
