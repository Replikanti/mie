//! The ADR-043 "Accept when" measurement over a real capture (not run in
//! CI): each cleanly ended run of a `mie ingest` capture is recomputed alone
//! with `LiveReplay::run_replay` into a fresh engine, as `mie equivalence`
//! does (ADR-041 D4), and one table is printed:
//!
//! - book events, and the share of them after which each depth band is
//!   ready (accept: 5 bps ≥ 90 %);
//! - added, cancelled, filled and inferred per side and band, summed over
//!   disjoint 5m windows (accept: inferred < 50 % of filled per side);
//! - the multiple of the 5th cluster candidate over the side's median, p10
//!   and p50, against the side's p90 level multiple (accept: the candidate's
//!   p50 above the p90 multiple's p50), sampled at the first book event of
//!   every second;
//! - the event time `book.l2@1` was not ready.
//!
//! Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test book_liquidity_measure -- --ignored
//! --nocapture`.

use mie_adapter_binance::LiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_domain::bars::Timeframe;
use mie_domain::book::{OrderBook, Side};
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureValue, catalog};
use mie_domain::liquidity::{BANDS, LiquidityWindow, SideClusters};
use mie_domain::num::{Qty, SCALE};
use mie_domain::state::MarketStateEngine;
use mie_ports::outbound::MarketDataProvider;
use std::path::PathBuf;

fn env_path(name: &str) -> PathBuf {
    PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("set {name}")))
}

/// Flow totals per band and side: added, cancelled, filled, inferred.
type Totals = [[[i64; 4]; 2]; BANDS];

/// What one pass over the runs accumulates.
#[derive(Default)]
struct Measure {
    book_events: u64,
    band_ready: [u64; BANDS],
    totals: Totals,
    /// Per side: the 5th candidate's multiple and the p90 level multiple.
    fifth: [Vec<f64>; 2],
    p90: [Vec<f64>; 2],
    not_ready_ms: i64,
    rejections: u64,
    windows: u64,
}

impl Measure {
    fn window(&mut self, window: &LiquidityWindow) {
        self.windows += 1;
        for (band, flow) in window.bands.iter().enumerate() {
            for (side, flow) in [flow.bid, flow.ask].iter().enumerate() {
                let cell = &mut self.totals[band][side];
                for (slot, qty) in [flow.added, flow.cancelled, flow.filled, flow.inferred]
                    .iter()
                    .enumerate()
                {
                    cell[slot] += qty.units();
                }
            }
        }
    }

    /// Samples the cluster multiples of both sides of `book`.
    fn clusters(
        &mut self,
        book: &OrderBook,
        bid: Option<&SideClusters>,
        ask: Option<&SideClusters>,
    ) {
        for (index, (side, clusters)) in
            [(Side::Bid, bid), (Side::Ask, ask)].into_iter().enumerate()
        {
            let Some(clusters) = clusters else { continue };
            let Some(fifth) = clusters.multiple(4) else {
                continue;
            };
            let mut quantities = band_quantities(book, side);
            quantities.sort_unstable();
            let p90 = quantities[(quantities.len() - 1) * 9 / 10];
            let median = clusters.median_qty.units();
            if median > 0 {
                self.fifth[index].push(fifth);
                self.p90[index].push(p90 as f64 / median as f64);
            }
        }
    }
}

/// The level quantities of `side` within 5 bps of mid, as units.
fn band_quantities(book: &OrderBook, side: Side) -> Vec<i64> {
    let (bid, ask) = (
        book.best_bid().unwrap().price,
        book.best_ask().unwrap().price,
    );
    let sum = i128::from(bid.units()) + i128::from(ask.units());
    let outer = i128::from(catalog::BOOK_BANDS[BANDS - 1].units());
    let within = |price: i64| {
        let twice = 2 * i128::from(price);
        let distance = match side {
            Side::Bid => sum - twice,
            Side::Ask => twice - sum,
        };
        distance * i128::from(SCALE) <= outer * sum
    };
    let levels: Box<dyn Iterator<Item = _>> = match side {
        Side::Bid => Box::new(book.bids()),
        Side::Ask => Box::new(book.asks()),
    };
    levels
        .take_while(|level| within(level.price.units()))
        .map(|level| level.qty.units())
        .collect()
}

fn quantile(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    values.sort_by(f64::total_cmp);
    let rank = ((values.len() - 1) as f64 * q).round() as usize;
    values[rank]
}

fn qty(units: i64) -> Qty {
    Qty::from_units(units)
}

#[test]
#[ignore = "measures a real capture for ADR-043; see the module docs"]
fn measure_book_liquidity() {
    let raw_root = env_path("MIE_MEASURE_RAW_ROOT");
    let journal = env_path("MIE_MEASURE_JOURNAL");
    let runs: Vec<JournaledRun> = read_runs(&journal, "binance-um").unwrap();
    let store = ParquetRawStore::new(&raw_root);
    let live = LiveReplay::new(
        &store,
        "binance-um",
        "BTCUSDT",
        runs.iter().map(JournaledRun::live_run).collect(),
    );
    let mut measure = Measure::default();
    for run in &runs {
        let Ok(mut replay) = live.run_replay(&run.parameters.run_id) else {
            println!("run {}: not replayable, skipped", run.parameters.run_id);
            continue;
        };
        let mut engine = MarketStateEngine::new();
        let mut first_window_end: Option<i64> = None;
        let mut last_window_end = None;
        let mut last_second = None;
        let mut not_ready_since: Option<i64> = None;
        while let Some(event) = replay.next_event().unwrap() {
            if engine.apply(&event).is_err() {
                measure.rejections += 1;
                continue;
            }
            let state = engine.state();
            let time = event.time().as_millis();
            let book_event = matches!(
                event,
                MarketEvent::BookSnapshot(_) | MarketEvent::BookUpdate(_)
            ) || matches!(&event, MarketEvent::FeedGap(g) if g.stream == mie_domain::event::Stream::OrderBook);
            if book_event {
                measure.book_events += 1;
                if let FeatureValue::Ready(depth) = &state.book.depth {
                    for (band, value) in depth.bands.iter().enumerate() {
                        measure.band_ready[band] += u64::from(value.is_ready());
                    }
                }
                match (&state.book.l2, not_ready_since) {
                    (FeatureValue::Ready(_), Some(since)) => {
                        measure.not_ready_ms += time - since;
                        not_ready_since = None;
                    }
                    (FeatureValue::Unavailable { .. }, None) => not_ready_since = Some(time),
                    _ => {}
                }
                let second = time.div_euclid(1_000);
                if last_second != Some(second)
                    && let (FeatureValue::Ready(book), FeatureValue::Ready(clusters)) =
                        (&state.book.l2, &state.book.clusters)
                {
                    last_second = Some(second);
                    measure.clusters(book, clusters.bid.ready(), clusters.ask.ready());
                }
            }
            if let FeatureValue::Ready(window) = state.book.windows.get(Timeframe::M5).unwrap() {
                let end = window.end.as_millis();
                let first = *first_window_end.get_or_insert(end);
                if last_window_end != Some(end) && (end - first) % 300_000 == 0 {
                    measure.window(window);
                }
                last_window_end = Some(end);
            }
        }
    }

    println!("book events: {}", measure.book_events);
    for (band, ready) in measure.band_ready.iter().enumerate() {
        println!(
            "band {}: ready after {:.2} % of book events",
            catalog::BOOK_BANDS[band],
            100.0 * *ready as f64 / measure.book_events.max(1) as f64
        );
    }
    println!(
        "flow over {} disjoint 5m windows (added / cancelled / filled / inferred, BTC):",
        measure.windows
    );
    for (band, sides) in measure.totals.iter().enumerate() {
        for (side, cell) in sides.iter().enumerate() {
            let inferred_share = 100.0 * cell[3] as f64 / cell[2].max(1) as f64;
            println!(
                "  band {} {}: {} / {} / {} / {} (inferred {:.2} % of filled)",
                catalog::BOOK_BANDS[band],
                ["bid", "ask"][side],
                qty(cell[0]),
                qty(cell[1]),
                qty(cell[2]),
                qty(cell[3]),
                inferred_share
            );
        }
    }
    for side in 0..2 {
        let samples = measure.fifth[side].len();
        let fifth_p10 = quantile(&mut measure.fifth[side], 0.1);
        let fifth_p50 = quantile(&mut measure.fifth[side], 0.5);
        let p90_p50 = quantile(&mut measure.p90[side], 0.5);
        println!(
            "{}: 5th candidate multiple p10 {fifth_p10:.1}, p50 {fifth_p50:.1}; \
             p90 level multiple p50 {p90_p50:.1} ({samples} samples)",
            ["bid", "ask"][side]
        );
    }
    println!(
        "book.l2 not ready: {:.1} s; domain rejections: {}",
        measure.not_ready_ms as f64 / 1_000.0,
        measure.rejections
    );
}
