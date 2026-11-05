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

#[test]
fn the_grid_is_the_1_2_5_series_from_the_minimum_to_the_market_maximum() {
    let expected = [
        "0.001", "0.002", "0.005", "0.01", "0.02", "0.05", "0.1", "0.2", "0.5", "1", "2", "5",
        "10", "20", "50", "100", "120",
    ];
    assert_eq!(
        CALIBRATION_GRID.map(|q| q.to_string()),
        expected.map(|q| btc(q).to_string())
    );
    assert!(CALIBRATION_GRID.windows(2).all(|pair| pair[0] < pair[1]));
    // D8's bound: the next size up is at most 2.5 times the one below.
    assert!(
        CALIBRATION_GRID
            .windows(2)
            .all(|pair| 2 * pair[1].units() <= 5 * pair[0].units())
    );
}

fn row(fills: u64, book: u64, window: u64, sum: &str) -> CalibrationRow {
    CalibrationRow {
        fills,
        book,
        window,
        sum: rate(sum),
    }
}

/// The calibration of two trades, both printed at e = 100.0: one on
/// [`fixture`], one while the book is still warming up.
///
/// The fixture trade gives a buy and a sell at 100.0, by hand (E′ rounded
/// once, s = ⟨σ_b(E′ − e) · 10⁸ / e⟩):
///
/// - Buy, asks ≥ 100.0 inside the window: 100.0 × 0.6, 100.1 × 2,
///   100.2 × 3 (5.6 in all). Spread and q ≤ 0.5: E′ = 100.0, s = 0. q = 1:
///   (60 + 40.04) / 1 = 100.04, s = 0.0004. q = 2: (60 + 140.14) / 2 =
///   100.07, s = 0.0007. q = 5: (60 + 200.2 + 240.48) / 5 = 100.136, s =
///   0.00136. q ≥ 10: `window`.
/// - Sell, bids ≤ 100.0 inside the window: 99.9 × 0.8, 99.8 × 2, 99.7 × 3
///   (5.8 in all). Spread and q ≤ 0.5: E′ = 99.9, s = 0.1 / 100 = 0.001.
///   q = 1: (79.92 + 19.96) / 1 = 99.88, s = 0.0012. q = 2: (79.92 +
///   119.76) / 2 = 99.84, s = 0.0016. q = 5: (79.92 + 199.6 + 219.34) / 5 =
///   99.772, s = 0.00228. q ≥ 10: `window`.
///
/// The warming trade's two fills are `book` on every line. So every line
/// accounts for 4 fills; the means are ⟨sum / fills⟩: spread and q ≤ 0.5
/// 0.0005, q = 1 0.0008, q = 2 0.00115, q = 5 0.00182, q ≥ 10 none.
fn two_trades() -> SlippageCalibration {
    let mut calibration = SlippageCalibration::new();
    calibration.observe(&fixture(), usd("100.0")).unwrap();
    let warming = FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    };
    calibration.observe(&warming, usd("100.0")).unwrap();
    calibration
}

#[test]
fn every_trade_gives_a_buy_and_a_sell_on_every_line() {
    let calibration = two_trades();
    assert_eq!(calibration.trades, 2);
    let small = row(2, 2, 0, "0.001");
    let mut depth_walk = [small; 17];
    depth_walk[9] = row(2, 2, 0, "0.0016");
    depth_walk[10] = row(2, 2, 0, "0.0023");
    depth_walk[11] = row(2, 2, 0, "0.00364");
    for line in &mut depth_walk[12..] {
        *line = row(0, 2, 2, "0");
    }
    assert_eq!(
        calibration,
        SlippageCalibration {
            trades: 2,
            spread: small,
            depth_walk,
        }
    );
    let means: Vec<Option<String>> = calibration
        .rows()
        .map(|(_, line)| line.mean().map(|m| m.to_string()))
        .collect();
    let mut expected = vec![Some(rate("0.0005").to_string()); 10];
    expected.extend(["0.0008", "0.00115", "0.00182"].map(|m| Some(rate(m).to_string())));
    expected.extend([None, None, None, None, None]);
    assert_eq!(means, expected);
    assert!(calibration.conserves());
    let models: Vec<BookModel> = calibration.rows().map(|(model, _)| model).collect();
    assert_eq!(models[0], BookModel::Spread);
    assert_eq!(models[1..], CALIBRATION_GRID.map(BookModel::DepthWalk));
}

#[test]
fn the_mean_rounds_half_to_even_once() {
    let units = |fills, sum| CalibrationRow {
        fills,
        book: 0,
        window: 0,
        sum: Rate::from_units(sum),
    };
    assert_eq!(units(2, 3).mean(), Some(Rate::from_units(2)));
    assert_eq!(units(2, 5).mean(), Some(Rate::from_units(2)));
    assert_eq!(units(3, 5).mean(), Some(Rate::from_units(2)));
    assert_eq!(units(0, 0).mean(), None);
}

#[test]
fn a_line_that_breaks_the_conservation_is_found() {
    let mut calibration = two_trades();
    assert_eq!(calibration.first_unconserved(), None);
    calibration.depth_walk[3].window += 1;
    assert_eq!(calibration.first_unconserved(), Some(4));
    assert!(!calibration.conserves());
    calibration.spread.fills -= 1;
    assert_eq!(calibration.first_unconserved(), Some(0));
}

/// D9: sums and counts pool exactly, so two partitions of a tape give the
/// calibration of one pass over it (the book is the engine's at each trade,
/// so no snapshot boundary is split here).
#[test]
fn pooling_two_partitions_equals_one_pass() {
    let book = fixture();
    let prints = ["100.0", "99.9", "100.1", "100.25", "99.6", "99.95"];
    let mut whole = SlippageCalibration::new();
    let mut first = SlippageCalibration::new();
    let mut second = SlippageCalibration::new();
    for (i, print) in prints.iter().enumerate() {
        whole.observe(&book, usd(print)).unwrap();
        let part = if i < 2 { &mut first } else { &mut second };
        part.observe(&book, usd(print)).unwrap();
    }
    assert_eq!(first.pool(&second), Some(whole.clone()));
    assert_eq!(second.pool(&first), Some(whole.clone()));
    assert_eq!(SlippageCalibration::new().pool(&whole), Some(whole.clone()));
    assert!(whole.conserves());
    // Every priced fill has s ≥ 0, so every sum does.
    assert!(whole.rows().all(|(_, line)| line.sum.units() >= 0));
    let mut full = whole.clone();
    full.spread.sum = Rate::from_units(i64::MAX);
    assert_eq!(full.pool(&whole), None);
}

#[test]
fn an_overflow_leaves_the_calibration_unchanged() {
    let book = fixture();
    let mut calibration = two_trades();
    let before = calibration.clone();
    // A print at 0 has no fraction: the fills price, their s does not.
    assert_eq!(
        calibration.observe(&book, usd("0")),
        Err(CalibrationOverflow)
    );
    assert_eq!(calibration, before);
    // The sell's s = 0.001 at 100.0 would push the spread's sum past i64.
    calibration.spread.sum = Rate::from_units(i64::MAX);
    let full = calibration.clone();
    assert_eq!(
        calibration.observe(&book, usd("100.0")),
        Err(CalibrationOverflow)
    );
    assert_eq!(calibration, full);
    calibration.trades = u64::MAX;
    assert_eq!(
        calibration.observe(&book, usd("100.0")),
        Err(CalibrationOverflow)
    );
}

/// D8's reading rule on the grid itself: per fill, s does not decrease
/// from one grid size to the next, and once the window cannot fill a size
/// it fills no larger one.
#[test]
fn per_fill_the_cost_does_not_decrease_over_the_grid() {
    let book = fixture();
    for side in [FillSide::Buy, FillSide::Sell] {
        for print in ["99.7", "99.9", "99.95", "100.0", "100.1"] {
            let mut previous = Some(Rate::from_units(0));
            for qty in CALIBRATION_GRID {
                let s = price_fill(&book, side, usd(print), BookModel::DepthWalk(qty))
                    .ok()
                    .map(|fill| slippage_rate(side, usd(print), fill).unwrap());
                match (previous, s) {
                    (Some(p), Some(s)) => assert!(p <= s, "{side:?} {print} {qty}"),
                    (None, Some(_)) => panic!("{side:?} {print} {qty}: priced after window"),
                    _ => {}
                }
                previous = s;
            }
        }
    }
}
