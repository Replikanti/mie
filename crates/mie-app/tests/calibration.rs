//! The slippage calibration pipeline (ADR-061 D7): a live tape with a book
//! runs through `CalibrationService`, its result is recorded once and a
//! re-run reproduces it exactly. The in-memory provider and store stand in
//! for the live replay and the file adapter; the CLI's offline test pins
//! the real live store.

mod common;

use common::{MemoryStore, Recorded, Unopenable};
use mie_app::{CalibrationService, SLIPPAGE_CALIBRATION_V1};
use mie_domain::event::{Aggressor, BookSnapshot, Level, MarketEvent, Trade};
use mie_domain::feature::{FeatureKey, FeatureSet, catalog};
use mie_domain::num::{Price, Qty, Rate};
use mie_domain::research::{DataVersion, ExperimentSpec, Outcome};
use mie_domain::slippage::{CalibrationRow, SlippageCalibration};
use mie_domain::time::EventTime;
use mie_ports::inbound::{ExperimentRun, ResearchError, RunResearchExperiment};

fn trade(millis: i64, trade_id: u64, price: &str) -> MarketEvent {
    MarketEvent::Trade(Trade {
        time: EventTime::from_millis(millis),
        trade_id,
        price: price.parse().unwrap(),
        qty: Qty::from_units(1_000_000),
        aggressor: Aggressor::Buy,
    })
}

fn levels(pairs: &[(&str, &str)]) -> Vec<Level> {
    pairs
        .iter()
        .map(|&(price, qty)| Level {
            price: price.parse::<Price>().unwrap(),
            qty: qty.parse().unwrap(),
        })
        .collect()
}

/// A live tape: a trade before the first snapshot, the snapshot (bids
/// 99.9 × 1, 99.8 × 2; asks 100.0 × 1, 100.1 × 2), two trades on it and
/// an out-of-order trade the domain rejects, printed far off so that
/// observing it would show.
fn tape() -> Vec<MarketEvent> {
    vec![
        trade(900, 1, "100.0"),
        MarketEvent::BookSnapshot(BookSnapshot {
            time: EventTime::from_millis(1_000),
            last_update_id: 100,
            bids: levels(&[("99.9", "1"), ("99.8", "2")]),
            asks: levels(&[("100.0", "1"), ("100.1", "2")]),
        }),
        trade(1_100, 2, "100.0"),
        trade(1_200, 3, "99.9"),
        trade(1_150, 4, "50.0"),
    ]
}

/// A spec over `features` with the sample `[0, 10 000)` and the data
/// version of [`Recorded`], on ADR-061's spec template: the spread rule on
/// the book when the set holds it. The pipeline reads no rule line.
fn spec(features: &[FeatureKey]) -> ExperimentSpec {
    let registry = catalog::registry();
    let set = FeatureSet::new(&registry, features).unwrap();
    let slippage = if features.contains(&catalog::BOOK_L2_V1.key) {
        "slippage.spread@1 book=feature:book.l2@1"
    } else {
        "slippage.fixed@1 rate=rate:0"
    };
    let text = format!(
        "mie-experiment 1\n\
         hypothesis slippage.calibration\n\
         sample 0 10000\n\
         data {}\n\
         features {} {set}\n\
         state-filter none\n\
         regime-filter none\n\
         location location.at_level@1 level=text:vah\n\
         trigger trigger.failed_auction@1\n\
         entry entry.next_trade@1\n\
         invalidation invalidation.beyond_extreme@1\n\
         target target.level@1 level=text:poc\n\
         fees maker=0.0002 taker=0.0005\n\
         slippage {slippage}\n\
         funding funding.none@1\n\
         latency 250\n",
        "0f".repeat(32),
        set.version()
    );
    ExperimentSpec::parse(&text, &registry).unwrap()
}

fn row(fills: u64, book: u64, window: u64, sum: &str) -> CalibrationRow {
    CalibrationRow {
        fills,
        book,
        window,
        sum: sum.parse::<Rate>().unwrap(),
    }
}

/// [`tape`]'s calibration by hand (s = ⟨σ_b(E′ − e) · 10⁸ / e⟩):
///
/// - The trade at 900 ms meets no book: its buy and sell are `book` on
///   every line. The rejected trade at 1 150 ms is not observed.
/// - At e = 100.0: the buy takes 100.0 × 1, 100.1 × 2; spread and q ≤ 1
///   s = 0, q = 2 E′ = 100.05, s = 0.0005. The sell takes 99.9 × 1,
///   99.8 × 2; spread and q ≤ 1 E′ = 99.9, s = 0.001, q = 2 E′ = 99.85,
///   s = 0.0015.
/// - At e = 99.9: the buy's spread and q ≤ 1 E′ = 100.0, s = 0.1 / 99.9 =
///   0.001001001… → 0.001001; q = 2 E′ = 100.05, s = 0.15 / 99.9 →
///   0.0015015. The sell's spread and q ≤ 1 s = 0; q = 2 E′ = 99.85,
///   s = 0.05 / 99.9 → 0.0005005.
/// - Each side holds 3 BTC inside the window: q ≥ 5 is `window` for the
///   four priced-book fills.
fn expected() -> SlippageCalibration {
    let small = row(4, 2, 0, "0.002001");
    let mut depth_walk = [small; 17];
    depth_walk[10] = row(4, 2, 0, "0.004002");
    for line in &mut depth_walk[11..] {
        *line = row(0, 2, 4, "0");
    }
    SlippageCalibration {
        trades: 3,
        spread: small,
        depth_walk,
    }
}

#[test]
fn a_calibration_is_recorded_once_and_reproduced_exactly() {
    let spec = spec(&[catalog::BOOK_L2_V1.key]);
    let mut service = CalibrationService::new(Recorded(tape()), MemoryStore::default());

    let first = service.run(&spec).unwrap();
    let ExperimentRun::Recorded(recorded) = &first else {
        panic!("{first:?}");
    };
    assert_eq!(recorded.pipeline, SLIPPAGE_CALIBRATION_V1);
    let Outcome::SlippageCalibration {
        stream,
        domain_rejections,
        calibration,
    } = &recorded.outcome
    else {
        panic!("{:?}", recorded.outcome);
    };
    assert_eq!(stream.events, 5);
    assert_eq!(*domain_rejections, 1);
    assert_eq!(**calibration, expected());
    assert_eq!(
        calibration.spread.mean(),
        Some("0.00050025".parse().unwrap())
    );
    let key = recorded.key();
    assert_eq!(key.engine, None);
    let text = recorded.canonical_text();
    assert!(
        text.contains(
            "\ntrades 3\n\
             spread fills=4 book=2 window=0 sum=0.00200100 mean=0.00050025\n\
             depth_walk 0.00100000 fills=4 book=2 window=0 sum=0.00200100 mean=0.00050025\n"
        ),
        "{text}"
    );
    assert!(
        text.contains(
            "\ndepth_walk 2.00000000 fills=4 book=2 window=0 sum=0.00400200 mean=0.00100050\n\
             depth_walk 5.00000000 fills=0 book=2 window=4 sum=0.00000000 mean=-\n"
        ),
        "{text}"
    );
    assert_eq!(service.store().results[&key], text);

    let second = service.run(&spec).unwrap();
    assert_eq!(second, ExperimentRun::Reproduced(recorded.clone()));
    assert_eq!(service.store().appends, 1);
    assert_eq!(service.store().results[&key], text);
}

#[test]
fn a_spec_without_the_book_is_refused_before_the_replay() {
    let spec = spec(&[catalog::TRADE_LAST_PRICE_V1.key]);
    let mut service = CalibrationService::new(Unopenable, MemoryStore::default());
    assert_eq!(service.run(&spec), Err(ResearchError::BookNotInSet));
    assert!(service.store().results.is_empty());
}

#[test]
fn another_data_version_or_an_empty_sample_records_nothing() {
    let spec = spec(&[catalog::BOOK_L2_V1.key]);
    let other = spec
        .canonical_text()
        .replace(&"0f".repeat(32), &"0e".repeat(32));
    let other = ExperimentSpec::parse(&other, &catalog::registry()).unwrap();
    let mut service = CalibrationService::new(Recorded(tape()), MemoryStore::default());
    assert_eq!(
        service.run(&other),
        Err(ResearchError::DataVersionMismatch {
            declared: DataVersion::from_hex(&"0e".repeat(32)).unwrap(),
            opened: DataVersion::from_hex(&"0f".repeat(32)).unwrap(),
        })
    );
    let mut empty = CalibrationService::new(Recorded(Vec::new()), MemoryStore::default());
    assert_eq!(empty.run(&spec), Err(ResearchError::EmptySample));
    assert!(service.store().results.is_empty());
    assert!(empty.store().results.is_empty());
}

#[test]
fn a_print_without_a_slippage_value_fails_the_run() {
    let mut events = tape();
    events.push(trade(1_300, 5, "0"));
    let spec = spec(&[catalog::BOOK_L2_V1.key]);
    let mut service = CalibrationService::new(Recorded(events), MemoryStore::default());
    assert_eq!(
        service.run(&spec),
        Err(ResearchError::CalibrationOverflow {
            at: EventTime::from_millis(1_300)
        })
    );
    assert!(service.store().results.is_empty());
}
