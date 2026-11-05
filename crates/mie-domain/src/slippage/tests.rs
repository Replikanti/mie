//! Hand-computed fixtures of ADR-061 D2's fill prices and D7's per-fill
//! rate.

use super::*;
use crate::event::{BookSnapshot, BookUpdate, Level, MarketEvent};
use crate::feature::Unavailability;
use crate::time::EventTime;

fn usd(text: &str) -> Price {
    text.parse().unwrap()
}

fn btc(text: &str) -> Qty {
    text.parse().unwrap()
}

fn rate(text: &str) -> Rate {
    text.parse().unwrap()
}

fn levels(pairs: &[(&str, &str)]) -> Vec<Level> {
    pairs
        .iter()
        .map(|&(price, qty)| Level {
            price: usd(price),
            qty: btc(qty),
        })
        .collect()
}

fn snapshot(bids: &[(&str, &str)], asks: &[(&str, &str)]) -> MarketEvent {
    MarketEvent::BookSnapshot(BookSnapshot {
        time: EventTime::from_millis(1_000),
        last_update_id: 100,
        bids: levels(bids),
        asks: levels(asks),
    })
}

fn ready(events: &[MarketEvent]) -> FeatureValue<OrderBook> {
    let mut book = OrderBook::new();
    for event in events {
        book.apply(event);
    }
    assert!(book.is_valid());
    FeatureValue::Ready(book)
}

/// The fixture book, in USDT and BTC.
///
/// - Snapshot (last update id 100): bids 99.9 × 1, 99.8 × 2, 99.7 × 3; asks
///   100.0 × 1, 100.1 × 2, 100.2 × 3. The trusted window is bids ≥ 99.7 and
///   asks ≤ 100.2.
/// - Update 100 … 101 (it straddles the snapshot): ask 100.0 down to 0.6,
///   bid 99.9 down to 0.8, and an ask 100.3 × 5 beyond the window, known
///   but never walked.
///
/// Every value below by hand, E′ rounded half to even once to 10⁻⁸ USDT and
/// s = ⟨σ_b(E′ − e) · 10⁸ / e⟩:
///
/// - **A buy printed at the ask, e = 100.0.** Spread: E′ = 100.0, s = 0.
///   q = 0.5 fits the 0.6 at 100.0: E′ = 100.0, s = 0. q = 1.5: 0.6 at
///   100.0 plus 0.9 at 100.1 is 150.09, E′ = 150.09 / 1.5 = 100.06, s =
///   0.06 / 100 = 0.0006. q = 5.6, the whole window: (60 + 200.2 + 300.6) / 5.6 =
///   100.142857142… → 100.14285714, s = 0.14285714 / 100 → 0.00142857.
///   q = 6 exceeds the window's 5.6: `window`.
/// - **A sell printed at the bid, e = 99.9.** Spread: E′ = 99.9, s = 0.
///   q = 0.5: E′ = 99.9. q = 1.5: 0.8 × 99.9 + 0.7 × 99.8 = 149.78, E′ =
///   99.853333333… → 99.85333333, s = 0.04666667 / 99.9 = 0.000467133… →
///   0.00046713. q = 5.8, the whole window: (79.92 + 199.6 + 299.1) / 5.8
///   = 99.762068965… → 99.76206897, s → 0.00138069. q = 6: `window`.
/// - **A same-millisecond sweep, a buy printed at 100.1.** The order that
///   printed 100.1 took the 0.6 at 100.0 before it, which the pre-trade
///   book still shows. From e = 100.1, spread: E′ = 100.1, s = 0, not the
///   negative slippage of 100.0. q = 1.5 fits 100.1's 2: E′ = 100.1.
///   q = 4: 2 × 100.1 + 2 × 100.2 = 400.6, E′ = 100.15, s = 0.05 / 100.1 =
///   0.000499500… → 0.0004995.
fn fixture() -> FeatureValue<OrderBook> {
    ready(&[
        snapshot(
            &[("99.9", "1"), ("99.8", "2"), ("99.7", "3")],
            &[("100.0", "1"), ("100.1", "2"), ("100.2", "3")],
        ),
        MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(1_100),
            first_update_id: 100,
            last_update_id: 101,
            prev_update_id: 99,
            bids: levels(&[("99.9", "0.8")]),
            asks: levels(&[("100.0", "0.6"), ("100.3", "5")]),
        }),
    ])
}

fn depth(qty: &str) -> BookModel {
    BookModel::DepthWalk(btc(qty))
}

/// `(model, E′ or the cause, s)` of one fill on [`fixture`].
type Case = (BookModel, Result<&'static str, Unpriced>, &'static str);

fn check(side: FillSide, print: &str, cases: &[Case]) {
    let book = fixture();
    for &(model, expected, s) in cases {
        let priced = price_fill(&book, side, usd(print), model);
        assert_eq!(priced, expected.map(usd), "{side:?} {print} {model:?}");
        if let Ok(fill) = priced {
            assert_eq!(
                slippage_rate(side, usd(print), fill),
                Some(rate(s)),
                "{side:?} {print} {model:?}"
            );
        }
    }
}

#[test]
fn a_buy_at_the_ask_walks_the_asks() {
    check(
        FillSide::Buy,
        "100.0",
        &[
            (BookModel::Spread, Ok("100.0"), "0"),
            (depth("0.5"), Ok("100.0"), "0"),
            (depth("1.5"), Ok("100.06"), "0.0006"),
            (depth("5.6"), Ok("100.14285714"), "0.00142857"),
            (depth("6"), Err(Unpriced::Window), ""),
        ],
    );
}

#[test]
fn a_sell_at_the_bid_walks_the_bids() {
    check(
        FillSide::Sell,
        "99.9",
        &[
            (BookModel::Spread, Ok("99.9"), "0"),
            (depth("0.5"), Ok("99.9"), "0"),
            (depth("1.5"), Ok("99.85333333"), "0.00046713"),
            (depth("5.8"), Ok("99.76206897"), "0.00138069"),
            (depth("6"), Err(Unpriced::Window), ""),
        ],
    );
}

#[test]
fn a_sweep_print_is_never_priced_below_itself() {
    check(
        FillSide::Buy,
        "100.1",
        &[
            (BookModel::Spread, Ok("100.1"), "0"),
            (depth("1.5"), Ok("100.1"), "0"),
            (depth("4"), Ok("100.15"), "0.0004995"),
        ],
    );
    // A print beyond the window reaches nothing; 100.3 is known but lies
    // beyond it.
    check(
        FillSide::Buy,
        "100.25",
        &[(BookModel::Spread, Err(Unpriced::Window), "")],
    );
    check(
        FillSide::Sell,
        "99.6",
        &[(depth("0.1"), Err(Unpriced::Window), "")],
    );
}

#[test]
fn a_book_that_is_not_ready_one_sided_or_crossed_prices_nothing() {
    let warming = FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    };
    let unavailable = FeatureValue::Unavailable {
        reason: Unavailability::InputInvalid,
    };
    let one_sided = ready(&[snapshot(&[("99.9", "1")], &[])]);
    let crossed = ready(&[snapshot(&[("100.1", "1")], &[("100.0", "1")])]);
    let locked = ready(&[snapshot(&[("100.0", "1")], &[("100.0", "1")])]);
    for book in [warming, unavailable, one_sided, crossed, locked] {
        for side in [FillSide::Buy, FillSide::Sell] {
            for model in [BookModel::Spread, depth("0.1")] {
                assert_eq!(
                    price_fill(&book, side, usd("100.0"), model),
                    Err(Unpriced::Book),
                    "{book:?} {side:?} {model:?}"
                );
            }
        }
    }
}

/// D2's two claims, over prints from below the bids to above the asks and
/// a series of sizes: every priced fill has s ≥ 0, and per fill, the cost
/// does not decrease as q grows (D8's reading rule), with `window` from the
/// first size the window cannot fill on.
#[test]
fn slippage_is_never_negative_and_grows_with_the_size() {
    let book = fixture();
    let sizes = [
        "0.001", "0.1", "0.5", "0.6", "0.8", "1", "1.5", "2", "5", "5.6", "5.8", "6",
    ];
    let prints = [
        "99.6", "99.7", "99.75", "99.8", "99.9", "99.95", "100.0", "100.1", "100.2",
    ];
    for side in [FillSide::Buy, FillSide::Sell] {
        for print in prints {
            let spread = price_fill(&book, side, usd(print), BookModel::Spread);
            let mut previous: Option<Rate> = None;
            let mut exhausted = false;
            for size in sizes {
                let priced = price_fill(&book, side, usd(print), depth(size));
                let Ok(fill) = priced else {
                    assert_eq!(priced, Err(Unpriced::Window));
                    exhausted = true;
                    continue;
                };
                assert!(
                    !exhausted,
                    "{side:?} {print} {size}: priced after a larger size was not"
                );
                assert!(spread.is_ok(), "a walk reaches the spread's first level");
                let s = slippage_rate(side, usd(print), fill).unwrap();
                assert!(s.units() >= 0, "{side:?} {print} {size}: s = {s}");
                assert!(previous.is_none_or(|p| p <= s), "{side:?} {print} {size}");
                previous = Some(s);
            }
            if let Ok(fill) = spread {
                assert!(slippage_rate(side, usd(print), fill).unwrap().units() >= 0);
            }
        }
    }
}

/// A walk at a size the first reachable level holds equals the spread
/// price (the backstop of the cost-direction tests).
#[test]
fn a_size_within_the_first_level_is_the_spread() {
    let book = fixture();
    for (side, print, first) in [
        (FillSide::Buy, "100.0", "0.6"),
        (FillSide::Sell, "99.9", "0.8"),
    ] {
        assert_eq!(
            price_fill(&book, side, usd(print), depth(first)),
            price_fill(&book, side, usd(print), BookModel::Spread)
        );
    }
}

#[test]
fn the_rate_is_undefined_at_a_print_at_or_below_zero() {
    assert_eq!(slippage_rate(FillSide::Buy, usd("0"), usd("1")), None);
    assert_eq!(slippage_rate(FillSide::Sell, usd("-1"), usd("-2")), None);
    // σ_b flips the sign: a sell below its print is a cost.
    assert_eq!(
        slippage_rate(FillSide::Sell, usd("100"), usd("99")),
        Some(rate("0.01"))
    );
    assert_eq!(FillSide::Buy.takes(), Side::Ask);
    assert_eq!(FillSide::Sell.takes(), Side::Bid);
}
