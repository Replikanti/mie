//! Fill prices on the L2 order book (ADR-061 D1, D2, D7, proposed).
//!
//! A marketable order of a hypothetical fill meets the book `book.l2@1`
//! holds when the fill trade is observed: a trade ranks before the book
//! updates of its millisecond (ADR-028) and never changes the book
//! (ADR-043 D1, D5), so that is the book the print happened against (D1).
//! [`price_fill`] prices such an order by one of two models (D2):
//!
//! - [`BookModel::Spread`]: the first reachable level's price;
//! - [`BookModel::DepthWalk`]: the volume-weighted price of walking the
//!   reachable levels for a size q, rounded half to even once to 10⁻⁸ USDT
//!   (ADR-027).
//!
//! *Reachable* means at or beyond the print price e on the side the order
//! takes (asks ≥ e for a buy, bids ≤ e for a sell), inside the trusted
//! window ([`OrderBook::walk`]). Liquidity better than e was taken by the
//! order that printed e, so a later order cannot get it, and every
//! slippage is ≥ 0. [`slippage_rate`] turns a priced fill into the
//! per-fill rate s of D7.
//!
//! [`SlippageCalibration`] is the outcome value of the calibration
//! pipeline `research.slippage_calibration@1` (D7, D8): every trade gives
//! two hypothetical fills, a buy and a sell at its print, each priced by
//! the spread model and at every size of [`CALIBRATION_GRID`], and each
//! model's line counts the priced fills, the unpriced ones by cause and the
//! exact sum of s. Sums and counts pool exactly across results
//! ([`SlippageCalibration::pool`]), so D9's rate s*(q) = ⟨Σ sum / Σ fills⟩
//! over several stores is the mean of their pooled line.
//!
//! [`Calibrator`] feeds the calibration from the delivered events. A book
//! that stops receiving diffs (a depth reconnect, a run restart) stays
//! `Ready` with its old levels until the resync, and the `OrderBook` gap is
//! announced only right before the snapshot that ends it (ADR-038 rule 9,
//! ADR-028 D4). So a fill is held until the next book event proves the
//! book it met: a `BookUpdate` after which the engine's book is still
//! `Ready` keeps it priced (the sequencer emits one only while the diffs
//! chain, and the domain book invalidates itself on a diff that does not
//! chain to its own, such as the successor of a diff the domain rejected);
//! any other update, a snapshot, an `OrderBook` gap or the end of the
//! sample counts it unpriced `book`. Every priced fill then depends only on
//! the book since the last snapshot and the diffs that chain to it (D7).
//!
//! Pure, integer fixed point, no float (ADR-025, ADR-027). Shared by the
//! trade simulation and the slippage calibration (#172), so this file names
//! neither.

#![deny(clippy::float_arithmetic)]

use crate::book::{OrderBook, Side, Walk};
use crate::event::{MarketEvent, Stream};
use crate::feature::FeatureValue;
use crate::num::{Price, Qty, Rate, SCALE, div_half_even};

#[cfg(test)]
mod tests;

/// The side of a hypothetical marketable order (ADR-061 D2): a buy takes
/// asks, a sell takes bids.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum FillSide {
    /// Takes asks: a long entry or a short exit.
    Buy,
    /// Takes bids: a short entry or a long exit.
    Sell,
}

impl FillSide {
    /// σ_b: +1 for a buy, −1 for a sell (ADR-061 D7).
    pub const fn sign(self) -> i64 {
        match self {
            Self::Buy => 1,
            Self::Sell => -1,
        }
    }

    /// The book side the order takes.
    pub const fn takes(self) -> Side {
        match self {
            Self::Buy => Side::Ask,
            Self::Sell => Side::Bid,
        }
    }
}

/// How a fill is priced on the book (ADR-061 D2, D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BookModel {
    /// `slippage.spread@1`: the first reachable level's price.
    Spread,
    /// `slippage.depth_walk@1`: the volume-weighted price of size q > 0.
    DepthWalk(Qty),
}

/// Why [`price_fill`] gave no price (ADR-061 D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Unpriced {
    /// The book is not `Ready`, is one-sided or is crossed (ADR-043 D3).
    Book,
    /// No level is reachable inside the trusted window, or the window holds
    /// less than q.
    Window,
}

/// The fill price E′ of a marketable `side` order at the print `print` on
/// `book`, by `model` (ADR-061 D2).
///
/// # Errors
///
/// [`Unpriced::Book`] for a book that is not `Ready`, one-sided or crossed
/// (best bid ≥ best ask); [`Unpriced::Window`] when the walk finds no
/// reachable level, or less than q, inside the trusted window.
pub fn price_fill(
    book: &FeatureValue<OrderBook>,
    side: FillSide,
    print: Price,
    model: BookModel,
) -> Result<Price, Unpriced> {
    let Some(book) = book.ready() else {
        return Err(Unpriced::Book);
    };
    match (book.best_bid(), book.best_ask()) {
        (Some(bid), Some(ask)) if bid.price < ask.price => {}
        _ => return Err(Unpriced::Book),
    }
    let qty = match model {
        BookModel::Spread => None,
        BookModel::DepthWalk(qty) => Some(qty),
    };
    match (book.walk(side.takes(), print, qty), qty) {
        (Walk::First(price), None) => Ok(price),
        (Walk::Notional(notional), Some(qty)) => {
            // The mean of level prices lies between the lowest and the
            // highest of them, so one rounding keeps it in the price range.
            let mean = div_half_even(notional, i128::from(qty.units()));
            Ok(Price::from_units(
                i64::try_from(mean).expect("a mean of level prices is in the price range"),
            ))
        }
        _ => Err(Unpriced::Window),
    }
}

/// The calibration's size grid (ADR-061 D8): the 1-2-5 series from
/// 0.001 BTC, the exchange's minimum order, to 100 BTC, plus 120 BTC, the
/// largest market order. Per fill the cost does not decrease as q grows, so
/// the line at the smallest grid size at or above a position size bounds
/// the cost at that size from above, overstating the size by at most 2.5×.
pub const CALIBRATION_GRID: [Qty; 17] = [
    Qty::from_units(100_000),
    Qty::from_units(200_000),
    Qty::from_units(500_000),
    Qty::from_units(1_000_000),
    Qty::from_units(2_000_000),
    Qty::from_units(5_000_000),
    Qty::from_units(10_000_000),
    Qty::from_units(20_000_000),
    Qty::from_units(50_000_000),
    Qty::from_units(100_000_000),
    Qty::from_units(200_000_000),
    Qty::from_units(500_000_000),
    Qty::from_units(1_000_000_000),
    Qty::from_units(2_000_000_000),
    Qty::from_units(5_000_000_000),
    Qty::from_units(10_000_000_000),
    Qty::from_units(12_000_000_000),
];

/// One model's line of a [`SlippageCalibration`] (ADR-061 D7): the fills it
/// priced, the fills it could not price by cause, and the exact sum of
/// their per-fill rates s.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalibrationRow {
    /// Fills the model priced.
    pub fills: u64,
    /// Fills unpriced [`Unpriced::Book`].
    pub book: u64,
    /// Fills unpriced [`Unpriced::Window`].
    pub window: u64,
    /// Σ s over the priced fills, exact (no rounding: each s is already in
    /// `Rate` units).
    pub sum: Rate,
}

impl Default for CalibrationRow {
    /// No fill: every count and the sum 0.
    fn default() -> Self {
        Self {
            fills: 0,
            book: 0,
            window: 0,
            sum: Rate::from_units(0),
        }
    }
}

impl CalibrationRow {
    /// The mean ⟨sum / fills⟩, rounded half to even once (ADR-027); `None`
    /// without a priced fill.
    pub fn mean(&self) -> Option<Rate> {
        (self.fills > 0).then(|| {
            let mean = div_half_even(i128::from(self.sum.units()), i128::from(self.fills));
            // |sum / fills| ≤ |sum|, which is an `i64`.
            Rate::from_units(i64::try_from(mean).expect("a mean is at most its sum"))
        })
    }

    /// Whether the line accounts for exactly `fills` hypothetical fills:
    /// fills + book + window (ADR-061 D7's conservation).
    pub fn accounts_for(&self, fills: u128) -> bool {
        u128::from(self.fills) + u128::from(self.book) + u128::from(self.window) == fills
    }

    /// Counts one fill; `None` when a count or the sum leaves its range.
    fn count(self, priced: Result<Rate, Unpriced>) -> Option<Self> {
        let mut row = self;
        match priced {
            Ok(s) => {
                row.fills = row.fills.checked_add(1)?;
                row.sum = Rate::from_units(row.sum.units().checked_add(s.units())?);
            }
            Err(Unpriced::Book) => row.book = row.book.checked_add(1)?,
            Err(Unpriced::Window) => row.window = row.window.checked_add(1)?,
        }
        Some(row)
    }

    /// The exact sum of two lines of the same model; `None` when a count or
    /// the sum leaves its range.
    fn pool(self, other: Self) -> Option<Self> {
        Some(Self {
            fills: self.fills.checked_add(other.fills)?,
            book: self.book.checked_add(other.book)?,
            window: self.window.checked_add(other.window)?,
            sum: Rate::from_units(self.sum.units().checked_add(other.sum.units())?),
        })
    }
}

/// What the slippage calibration measured over a sample (ADR-061 D7, D8):
/// the trades observed and one [`CalibrationRow`] per model, the spread
/// model first, then the depth walk at each size of [`CALIBRATION_GRID`].
///
/// Every trade gives two hypothetical fills, a buy and a sell at its
/// print, so every line accounts for 2 × `trades` fills
/// ([`first_unconserved`](Self::first_unconserved)).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SlippageCalibration {
    /// The trades observed.
    pub trades: u64,
    /// The line of [`BookModel::Spread`].
    pub spread: CalibrationRow,
    /// The lines of [`BookModel::DepthWalk`], in [`CALIBRATION_GRID`]
    /// order.
    pub depth_walk: [CalibrationRow; 17],
}

/// A calibration count or sum would leave its integer range (ADR-027:
/// arithmetic is explicit about overflow), or a fill's slippage has no
/// `Rate` value ([`slippage_rate`]: a print at or below 0, or a fraction
/// beyond the `i64` units). No real tape comes near either; the
/// calibration stops instead of rounding or dropping the fill.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CalibrationOverflow;

impl SlippageCalibration {
    /// An empty calibration: no trade, every line zero.
    pub fn new() -> Self {
        Self::default()
    }

    /// The lines with their models, in text order: the spread, then the
    /// grid.
    pub fn rows(&self) -> impl Iterator<Item = (BookModel, &CalibrationRow)> {
        std::iter::once((BookModel::Spread, &self.spread)).chain(
            CALIBRATION_GRID
                .iter()
                .zip(&self.depth_walk)
                .map(|(&qty, row)| (BookModel::DepthWalk(qty), row)),
        )
    }

    /// Observes one trade printed at `print` against `book`, the order book
    /// of the engine's state at the trade (ADR-061 D1): a buy and a sell,
    /// each priced by every model (D2), counted on its line. All or
    /// nothing: on an error the value is unchanged.
    ///
    /// # Errors
    ///
    /// [`CalibrationOverflow`] when a count or a sum leaves its range or a
    /// priced fill's slippage has no `Rate` value.
    pub fn observe(
        &mut self,
        book: &FeatureValue<OrderBook>,
        print: Price,
    ) -> Result<(), CalibrationOverflow> {
        let mut next = self.clone();
        next.trades = next.trades.checked_add(1).ok_or(CalibrationOverflow)?;
        for side in [FillSide::Buy, FillSide::Sell] {
            let rate = |model| -> Result<Result<Rate, Unpriced>, CalibrationOverflow> {
                match price_fill(book, side, print, model) {
                    Ok(fill) => slippage_rate(side, print, fill)
                        .map(Ok)
                        .ok_or(CalibrationOverflow),
                    Err(cause) => Ok(Err(cause)),
                }
            };
            next.spread = next
                .spread
                .count(rate(BookModel::Spread)?)
                .ok_or(CalibrationOverflow)?;
            for (row, &qty) in next.depth_walk.iter_mut().zip(&CALIBRATION_GRID) {
                *row = row
                    .count(rate(BookModel::DepthWalk(qty))?)
                    .ok_or(CalibrationOverflow)?;
            }
        }
        *self = next;
        Ok(())
    }

    /// The exact pooling of two calibrations (ADR-061 D9): counts and sums
    /// add line by line, so the pooled line's mean is ⟨Σ sum / Σ fills⟩.
    /// `None` when a count or a sum leaves its range.
    pub fn pool(&self, other: &Self) -> Option<Self> {
        let mut depth_walk = self.depth_walk;
        for (row, other) in depth_walk.iter_mut().zip(&other.depth_walk) {
            *row = row.pool(*other)?;
        }
        Some(Self {
            trades: self.trades.checked_add(other.trades)?,
            spread: self.spread.pool(other.spread)?,
            depth_walk,
        })
    }

    /// The same trades with every fill unpriced `book`: the book they met
    /// was not proven current ([`Calibrator`]). `None` when 2 × `trades`
    /// leaves the `u64` range.
    pub fn unpriced(&self) -> Option<Self> {
        let row = CalibrationRow {
            book: self.trades.checked_mul(2)?,
            ..CalibrationRow::default()
        };
        Some(Self {
            trades: self.trades,
            spread: row,
            depth_walk: [row; 17],
        })
    }

    /// The first line, by its position in [`rows`](Self::rows) (0 for the
    /// spread), that does not account for 2 × `trades` fills (ADR-061 D7);
    /// `None` when every line does.
    pub fn first_unconserved(&self) -> Option<usize> {
        let fills = 2 * u128::from(self.trades);
        self.rows().position(|(_, row)| !row.accounts_for(fills))
    }
}

/// Feeds a [`SlippageCalibration`] from the accepted events of a sample, in
/// delivery order (ADR-061 D7; module docs).
///
/// A trade's fills are priced on the book of the engine's state at the
/// trade and held until the next book event settles them:
///
/// - a `BookUpdate` after which `book` is still `Ready`: the sequencer
///   emits one only while the diffs chain, and the domain book stays ready
///   only while they chain to its own (a diff the domain rejected leaves
///   its successor breaking that chain), so the book was current at the
///   held trades, and their fills count as priced;
/// - a `BookUpdate` that leaves `book` not ready: the book missed a diff,
///   and the held fills count `book`;
/// - a `BookSnapshot` or a `FeedGap` on the order book: the book may have
///   been stale since its last event (an unsynced period is announced only
///   at its resync, a restarted run's book resets), so every held fill
///   counts unpriced `book`;
/// - the end of the sample ([`finish`](Self::finish)): nothing proves the
///   book after its last event, so the held fills count `book`.
///
/// Every other event leaves the held trades held. A trade and a book update
/// of the same millisecond: the trade ranks first (ADR-028), so the update
/// settles it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Calibrator {
    settled: SlippageCalibration,
    held: SlippageCalibration,
}

impl Calibrator {
    /// A calibrator that has seen no event.
    pub fn new() -> Self {
        Self::default()
    }

    /// Observes one event the engine accepted; `book` is the order book of
    /// the engine's state after it.
    ///
    /// # Errors
    ///
    /// [`CalibrationOverflow`] as [`SlippageCalibration::observe`] and
    /// [`SlippageCalibration::pool`] give it. The calibrator is unchanged
    /// then.
    pub fn observe(
        &mut self,
        event: &MarketEvent,
        book: &FeatureValue<OrderBook>,
    ) -> Result<(), CalibrationOverflow> {
        match event {
            MarketEvent::Trade(trade) => self.held.observe(book, trade.price),
            MarketEvent::BookUpdate(_) => self.settle(book.is_ready()),
            MarketEvent::BookSnapshot(_) => self.settle(false),
            MarketEvent::FeedGap(gap) if gap.stream == Stream::OrderBook => self.settle(false),
            _ => Ok(()),
        }
    }

    /// The calibration of the sample: the held fills count `book`.
    ///
    /// # Errors
    ///
    /// [`CalibrationOverflow`] when the pooled counts leave their range.
    pub fn finish(mut self) -> Result<SlippageCalibration, CalibrationOverflow> {
        self.settle(false)?;
        Ok(self.settled)
    }

    /// Moves the held fills into the settled calibration, priced as
    /// observed or unpriced `book`.
    fn settle(&mut self, current: bool) -> Result<(), CalibrationOverflow> {
        if self.held.trades == 0 {
            return Ok(());
        }
        let held = if current {
            self.held.clone()
        } else {
            self.held.unpriced().ok_or(CalibrationOverflow)?
        };
        self.settled = self.settled.pool(&held).ok_or(CalibrationOverflow)?;
        self.held = SlippageCalibration::new();
        Ok(())
    }
}

/// The per-fill slippage s = ⟨σ_b(E′ − e) · 10⁸ / e⟩ in `Rate` units, one
/// half-to-even rounding (ADR-061 D7): the fraction of the print price
/// that the book costs beyond it. ≥ 0 for every price [`price_fill`]
/// gives. `None` for a print at or below 0, where no fraction is defined,
/// or a value outside the `i64` unit range.
pub fn slippage_rate(side: FillSide, print: Price, fill: Price) -> Option<Rate> {
    let e = i128::from(print.units());
    if e <= 0 {
        return None;
    }
    let moved = i128::from(side.sign()) * (i128::from(fill.units()) - e);
    let rate = div_half_even(moved * i128::from(SCALE), e);
    i64::try_from(rate).ok().map(Rate::from_units)
}
