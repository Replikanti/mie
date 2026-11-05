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
//! Pure, integer fixed point, no float (ADR-025, ADR-027). Shared by the
//! trade simulation and the slippage calibration (#172), so this file names
//! neither.

#![deny(clippy::float_arithmetic)]

use crate::book::{OrderBook, Side, Walk};
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
