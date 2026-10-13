//! The ADR-051 measurements (issue #22, M2 and M3): an independent model of
//! the ADR-051 decision text, run next to one `MarketStateEngine`, that
//! checks the engine's candidates and runs the two pre-registered fixing
//! rules of effort vs result, R1b (the baseline length `B`) and R1 (the high
//! quantile `q_hi`), with `B` ∈ [`BASELINES`] and `q` ∈ [`GRID`] as runtime
//! parameters. The hold-out run fixed `B` = 1 440 and `q_hi` = 0.800 (D9);
//! since M3 the engine computes effort vs result, exhaustion and absorption
//! at those values, and R0 compares them at [`ENGINE_B`] and [`ENGINE_Q`].
//!
//! The model is written from decisions 3–6, not from the engine's
//! internals. Its inputs:
//!
//! - the CVD, summed from the trades themselves, with its trades-gap count
//!   (decision 4's anchor reading, ADR-035 D2);
//! - the swings, from the engine's `StructureEvent::Swing` facts (upstream,
//!   already reproduced by ADR-037's own model);
//! - the 5-minute open interest, from the state's `derivatives.oi.5m@1`;
//! - effort vs result and exhaustion: the state's `flow.window.<5m|15m|1h>@1`
//!   after every event that closes a 1m bar, and the closed 1m bars;
//! - absorption: the state's `book.liquidity.window.<5m|15m|1h>@1` at the
//!   same minute, its 5 bps band on the passive side opposite the
//!   aggressor;
//! - the location bin of each effort-vs-result episode: decision 3's
//!   point-in-zone test at the anchor (the last close), written here from the
//!   ADR text over the state's `location.levels` (the domain helper is
//!   crate-private), and the regime label from `volatility.regime.1h@1`.
//!
//! Two readings the ADR leaves open are fixed here, and the engine matches
//! them (R0): a minute without an evaluation (`delta == 0`, no
//! displacement, a window or a baseline not ready, a missing expansion)
//! leaves `active` as it was, so it neither emits nor re-arms; and the
//! exhaustion extreme reads the closed 1m bars with trades.
//!
//! Reported:
//!
//! - **R0** (asserted): no domain rejection, and at every event the
//!   engine's candidate facts equal the model's — kind, feature, timeframe,
//!   `known_at`, both anchors, every change, coverage and the location
//!   context; for effort vs result, exhaustion and absorption the value at
//!   `B` = 1 440 and `q_hi` = 0.800, its inputs and the context at the last
//!   close, and the location bin each effort-vs-result fact's context gives
//!   equals the model's — and after every event that closes a structure
//!   bar, every divergence value equals the model's, and after every event
//!   that closes a 1m bar, every effort-vs-result, exhaustion and absorption
//!   value equals the model's (warm-up counts included).
//! - **D9 records**: per structure timeframe, half and in total, the
//!   consecutive same-side swing pairs, the share at a further extreme, the
//!   CVD-divergent share, the pairs with both OI points and the
//!   OI-divergent share.
//! - **R1b**: per `B`, half, window length and `q`, the episodes per UTC
//!   hour of `B` and of the reference, counted while the reference baseline
//!   is ready, with the statistic of decision 5 as revised for #124:
//!   Pearson's X² of the 2 × 24 table (`B`, reference) × UTC hour, its `p`
//!   under χ²₂₃, the cells whose expected count is below 5 and below 1,
//!   Cochran's evaluability (no expected count below 1, at most 9 of 48
//!   below 5), the critical value `χ²₂₃(1 − 0.05/k)` over the k evaluable
//!   combinations and the per-combination verdict; then the selection
//!   ([`r1b`]).
//! - **R1**, at R1b's `B`, or at every `B` without a verdict if R1b stops:
//!   per window length, half and `q`, the 12 regime × location cells, the
//!   regime-pooled cells and the unlabelled and unlocated counts; the
//!   minimum-cell classification and the verdict ([`r1`]).
//! - Recorded only: exhaustion episodes per `B`, `q` and half; at the
//!   engine's `B` and `q_hi`, the effort-vs-result, exhaustion and absorption
//!   episodes per half and the minutes absorption was evaluated; the
//!   replay's wall time.
//!
//! The halves split at the window's midpoint, `from + (to − from) / 2`. The
//! fixing rules run on the measurement year of ADR-051 D9, the hold-out
//! year 2024-10-01 … 2025-09-30, split at 2025-04-01 12:00 UTC; the #12 year
//! 2025-10-01 … 2026-09-30 (split 2026-04-01 12:00 UTC) is the development
//! year. An episode or a pair belongs to the half of its emission time. Run
//! it with `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! candidate_measure -- --ignored --nocapture measure_candidates`;
//! `MIE_MEASURE_TO` is included, as in `mie replay`. The hold-out run reads
//! the hold-out raw root with `MIE_MEASURE_FROM=2024-10-01` and
//! `MIE_MEASURE_TO=2025-09-30`. An optional `MIE_MEASURE_EVENTS` asserts the
//! replay's event count (R0). The replay reads `aggTrades` with the ADR-050
//! fold (its D8 default) and `metrics` for the open interest.
//!
//! `measure_candidates_live` (also `--ignored`) runs the same R0 over every
//! cleanly ended run of a `mie ingest` capture, each recomputed alone with
//! `LiveReplay::run_replay` into a fresh engine (the `book_liquidity_measure`
//! pattern), so absorption is checked where depth exists: run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test candidate_measure -- --ignored --nocapture
//! measure_candidates_live`. A run shorter than the 1 440 evaluations effort
//! vs result warms up over evaluates no absorption; the per-run report shows
//! the evaluated minutes.
//!
//! `r1b_from_a_logged_run` (also `--ignored`) re-reads the R1b hour lines of
//! a saved `measure_candidates` output, in M2's format or this one's, from
//! the file `MIE_MEASURE_LOG`, and prints the same R1b report and verdict.
//! It exists for ADR-051 D9's diagnosis table on the #22 M2 log and never
//! replaces a run.
//!
//! `model_reproduces_the_engine_on_a_synthetic_tape` runs the same R0
//! comparison and every report section in CI on a seeded tape, and asserts
//! that every compared kind occurred, so R0 cannot pass vacuously;
//! `model_reproduces_absorption_on_a_synthetic_book_tape` does the same for
//! absorption on a seeded tape with a book. The two
//! selection rules are pure functions over count tables, unit-tested on
//! constructed tables; R1b's statistic is also pinned on three
//! development-year tables from the #22 M2 log, as a check that the code
//! computes what decision 5 says (ADR-051 D9 limits what they may be used
//! for).

use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_cli::replay::parse_bound;
use mie_domain::bars::{Coverage, Timeframe};
use mie_domain::book::Side as BookSide;
use mie_domain::candidate::absorption::Absorption;
use mie_domain::candidate::divergence::{
    CvdComparison, Divergence, DivergenceEvent, OiComparison, SwingAnchor, SwingPair,
    TimeframeDivergences,
};
use mie_domain::candidate::effort::{EffortResult, Exhaustion as ExhaustionValue};
use mie_domain::candidate::{
    CandidateEvent, ContextLevel, ContextLevels, EdgeEvent, LocationContext,
};
use mie_domain::derivatives::OiGridPoint;
use mie_domain::event::{
    Aggressor, BookSnapshot, BookUpdate, FeedGap, GapReason, Level, MarketEvent, OpenInterest,
    Stream, Trade,
};
use mie_domain::feature::{FeatureValue, Unavailability, catalog};
use mie_domain::flow::AggressionWindow;
use mie_domain::liquidity::LiquidityWindow;
use mie_domain::location::{LevelKind, LevelSide, LocationLevel, LocationState};
use mie_domain::num::{Price, Qty, SCALE};
use mie_domain::regime::RegimeLabel;
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::structure::{STRUCTURE_TIMEFRAMES, Side, StructureEvent, Swing};
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;
const MINUTE_MS: i64 = 60_000;

/// Decision 4: the oldest open-interest grid point an anchor reads, from
/// the point's boundary to the anchor trade: one grid interval (300 s) plus
/// the grid's 60 s lag.
const OI_ANCHOR_AGE_MS: i64 = 300_000 + 60_000;

/// Closed-bar records the divergence model keeps per timeframe. Any number
/// above `SWING_BARS + 1` works: the model looks a swing bar up by its open
/// time, so it does not depend on the engine's ring of 4.
const KEPT_BARS: usize = 8;

/// The registry's tolerance, in basis points of the level's price: the
/// padding of decision 3's point-in-zone test (ADR-044 decision 3, 0.0006).
const ZONE_BPS: i128 = 6;

/// The baseline lengths of decision 5, in evaluations: the candidates 60,
/// 240 and 1440, then the reference 10 080.
const BASELINES: [usize; 4] = [60, 240, 1_440, 10_080];

/// The index of the reference baseline in [`BASELINES`]; the candidates
/// precede it.
const REFERENCE: usize = 3;

/// Number of baselines.
const NB: usize = BASELINES.len();

/// The `q_hi` grid of decision 5, in thousandths.
const GRID: [u32; 6] = [800, 850, 900, 950, 975, 990];

/// Number of grid values.
const Q: usize = GRID.len();

/// The median, in thousandths: nearest rank ⌈B/2⌉, the lower median.
const MEDIAN: u32 = 500;

/// The engine's `B`, fixed by R1b on the hold-out year (decision 5, D9):
/// 1 440, at this index of [`BASELINES`].
const ENGINE_B: usize = 2;

/// The engine's `q_hi`, fixed by R1 at that `B` (decision 5, D9): 0.800,
/// at this index of [`GRID`].
const ENGINE_Q: usize = 0;

/// Decision 6: exhaustion warms up over six judged 1h evaluations.
const EXHAUSTION_SAMPLES: u64 = 6;

/// Decision 7: absorption reads the 5 bps band, in rate units (1 bps =
/// 10 000).
const ABSORPTION_BAND_UNITS: i64 = 50_000;

/// The effort windows of decision 5, shortest first.
const WINDOWS: [Timeframe; 3] = [Timeframe::M5, Timeframe::M15, Timeframe::H1];

/// Number of window lengths.
const W: usize = WINDOWS.len();

/// The position of the 5m window (the decay probe of decision 6).
const DECAY: usize = 0;

/// The position of the 1h window (the expansion of decision 6).
const EXPANSION: usize = 2;

/// Decision 6: the expansion is read at `t − 5 min`, the decay window's
/// length.
const EXPANSION_OFFSET_MS: i64 = 5 * MINUTE_MS;

/// Decision 6: the closed minutes `(t − 65 min, t]` the extreme is read
/// over.
const EXTREME_SPAN_MS: i64 = 65 * MINUTE_MS;

/// Decision 6: the close lies within 6 bps of the extreme.
const EXTREME_BPS: i128 = 6;

/// R1's episode floor per cell (decision 5): 1.96² · 0.25 / 0.10² = 96.04.
const N_MIN: u64 = 97;

/// R1b's family error level (decision 5).
const FAMILY_LEVEL: f64 = 0.05;

/// R1b's 24 UTC hours.
const HOURS: usize = 24;

/// The regime labels of `volatility.regime.1h@1`.
const LABELS: [RegimeLabel; 4] = [
    RegimeLabel::Low,
    RegimeLabel::Medium,
    RegimeLabel::High,
    RegimeLabel::Extreme,
];

/// Number of labels.
const NL: usize = LABELS.len();

/// The location bins of R1, in this order.
const BIN_NAMES: [&str; 3] = ["value_edge", "structural_extreme", "neither"];

/// Number of bins.
const BINS: usize = BIN_NAMES.len();

const VALUE_EDGE: usize = 0;
const STRUCTURAL_EXTREME: usize = 1;
const NEITHER: usize = 2;

/// Episodes per UTC hour, per window length and `q`, of one baseline and
/// half (R1b).
type HourTable = [[[u64; HOURS]; Q]; W];

/// Episodes per regime label and location bin, per window length and `q`,
/// of one baseline and half (R1).
type CellTable = [[[[u64; BINS]; NL]; Q]; W];

/// Counts per window length and `q`, of one baseline and half.
type GridCounts = [[u64; Q]; W];

fn side_index(side: Side) -> usize {
    match side {
        Side::High => 0,
        Side::Low => 1,
    }
}

/// The position of the window of length `timeframe` in [`WINDOWS`].
fn window_index(timeframe: Timeframe) -> usize {
    WINDOWS
        .iter()
        .position(|window| *window == timeframe)
        .expect("an effort window")
}

fn label_index(label: RegimeLabel) -> usize {
    match label {
        RegimeLabel::Low => 0,
        RegimeLabel::Medium => 1,
        RegimeLabel::High => 2,
        RegimeLabel::Extreme => 3,
    }
}

/// The half of `time`: 0 before `mid`, 1 from it.
fn half(time: EventTime, mid: i64) -> usize {
    usize::from(time.as_millis() >= mid)
}

/// The UTC hour of `time`.
fn hour(time: EventTime) -> usize {
    usize::try_from(time.as_millis().rem_euclid(DAY_MS) / HOUR_MS).expect("an hour of the day")
}

/// `after − before` in `i128`, if it fits an `i64`.
fn change(before: i64, after: i64) -> Option<i64> {
    i64::try_from(i128::from(after) - i128::from(before)).ok()
}

/// Decision 3: whether `price` lies in `level`'s zone padded by the
/// registry tolerance of the level's price, `[price, price]` overlapping
/// `[low − w, high + w]`, exact in `i128`.
fn in_zone(level: &LocationLevel, price: Price) -> bool {
    let price = i128::from(price.units());
    let reach = i128::from(level.price.units()).abs() * ZONE_BPS;
    (i128::from(level.low.units()) - price) * 10_000 <= reach
        && (price - i128::from(level.high.units())) * 10_000 <= reach
}

/// A level derived from a divergence's own swing (decision 3): same
/// `structure.levels.<tf>` source, same side, anchored at the swing bar.
#[derive(Debug, Clone, Copy)]
struct Own {
    side: LevelSide,
    source_id: &'static str,
    swing_time: EventTime,
}

/// Decision 3's location context of `anchor`, without the levels of `own`.
fn context(anchor: Price, own: Option<Own>, location: &LocationState) -> LocationContext {
    let levels = match &location.levels {
        FeatureValue::Ready(set) => FeatureValue::Ready(ContextLevels {
            feature: set.feature,
            levels: set
                .levels()
                .iter()
                .filter(|level| in_zone(level, anchor))
                .filter(|level| {
                    !own.is_some_and(|own| {
                        level.side == Some(own.side)
                            && level.anchor == Some(own.swing_time)
                            && level.source.id.as_str() == own.source_id
                    })
                })
                .map(|level| ContextLevel {
                    kind: level.kind,
                    source: level.source,
                    side: level.side,
                    anchor: level.anchor,
                    price: level.price,
                    low: level.low,
                    high: level.high,
                })
                .collect(),
        }),
        FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
            observed: *observed,
            required: *required,
        },
        FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason: *reason },
    };
    LocationContext {
        anchor,
        levels,
        auction: location.auction,
    }
}

/// R1's location bins of the levels listed at an anchor: `value_edge` for
/// a VAH or VAL, `structural_extreme` for a structural high or low, both
/// when both, `neither` when neither.
fn bins_of(kinds: impl IntoIterator<Item = LevelKind>) -> [bool; BINS] {
    let mut bins = [false; BINS];
    for kind in kinds {
        match kind {
            LevelKind::Vah | LevelKind::Val => bins[VALUE_EDGE] = true,
            LevelKind::StructuralHigh | LevelKind::StructuralLow => {
                bins[STRUCTURAL_EXTREME] = true;
            }
            _ => {}
        }
    }
    bins[NEITHER] = !bins[VALUE_EDGE] && !bins[STRUCTURAL_EXTREME];
    bins
}

// ---------------------------------------------------------------------------
// Divergences (decision 4)
// ---------------------------------------------------------------------------

/// The CVD summed from the trades: signed aggressor quantity since the
/// first trade, and the trades gaps since then.
#[derive(Debug, Default)]
struct CvdModel {
    started: bool,
    cvd: i128,
    gaps: u64,
}

impl CvdModel {
    fn apply(&mut self, event: &MarketEvent) {
        match event {
            MarketEvent::Trade(trade) => {
                self.started = true;
                let qty = i128::from(trade.qty.units());
                match trade.aggressor {
                    Aggressor::Buy => self.cvd += qty,
                    Aggressor::Sell => self.cvd -= qty,
                }
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades && self.started => {
                self.gaps += 1;
            }
            _ => {}
        }
    }

    fn qty(&self) -> Qty {
        Qty::from_units(i64::try_from(self.cvd).expect("the engine accepted the CVD"))
    }
}

/// The reading at the first trade that set one extreme of a bar.
#[derive(Debug, Clone, Copy)]
struct Mark {
    price: Price,
    time: EventTime,
    cvd: Qty,
    gaps: u64,
    oi: Option<OiGridPoint>,
}

/// The extreme readings of one bar, which exist from its first trade on.
#[derive(Debug, Clone, Copy)]
struct Marks {
    open: i64,
    high: Mark,
    low: Mark,
}

/// One anchored swing and the open-interest point at its anchor.
#[derive(Debug, Clone, Copy)]
struct Anchored {
    anchor: SwingAnchor,
    oi: Option<OiGridPoint>,
}

/// The last comparison of one side.
#[derive(Debug, Clone, Copy)]
enum Slot<C> {
    Empty,
    Compared(C),
    OutOfRange,
}

/// The D9 records of one side and half.
#[derive(Debug, Clone, Copy, Default)]
struct PairStats {
    pairs: u64,
    further: u64,
    cvd_divergent: u64,
    oi_points: u64,
    oi_divergent: u64,
}

impl PairStats {
    fn add(&mut self, other: &PairStats) {
        self.pairs += other.pairs;
        self.further += other.further;
        self.cvd_divergent += other.cvd_divergent;
        self.oi_points += other.oi_points;
        self.oi_divergent += other.oi_divergent;
    }
}

/// The divergence model of one structure timeframe.
struct DivergenceModel {
    position: usize,
    timeframe: Timeframe,
    /// The latest bars' extreme readings, oldest first; the last one is the
    /// developing bar.
    marks: VecDeque<Marks>,
    last: [Option<Anchored>; 2],
    cvd: [Slot<CvdComparison>; 2],
    oi: [Slot<OiComparison>; 2],
    warm: bool,
    /// Per half, per side.
    stats: [[PairStats; 2]; 2],
    /// Comparisons with a missing OI point, and those divergent by neither
    /// rule: kinds the value comparison covers.
    missing_oi: u64,
    plain: u64,
}

impl DivergenceModel {
    fn new(position: usize) -> Self {
        Self {
            position,
            timeframe: STRUCTURE_TIMEFRAMES[position],
            marks: VecDeque::with_capacity(KEPT_BARS + 1),
            last: [None; 2],
            cvd: [Slot::Empty; 2],
            oi: [Slot::Empty; 2],
            warm: false,
            stats: [[PairStats::default(); 2]; 2],
            missing_oi: 0,
            plain: 0,
        }
    }

    /// Records `trade` if it is the first to set its bar's high or low,
    /// with the CVD after it and the state's open-interest point if that is
    /// at most [`OI_ANCHOR_AGE_MS`] old.
    fn trade(&mut self, trade: &Trade, cvd: &CvdModel, oi: Option<&OiGridPoint>) {
        let millis = trade.time.as_millis();
        let open = millis - millis.rem_euclid(self.timeframe.millis());
        let mark = Mark {
            price: trade.price,
            time: trade.time,
            cvd: cvd.qty(),
            gaps: cvd.gaps,
            oi: oi
                .filter(|point| millis - point.boundary.as_millis() <= OI_ANCHOR_AGE_MS)
                .copied(),
        };
        match self.marks.back_mut() {
            Some(marks) if marks.open == open => {
                if trade.price > marks.high.price {
                    marks.high = mark;
                }
                if trade.price < marks.low.price {
                    marks.low = mark;
                }
            }
            _ => {
                self.marks.push_back(Marks {
                    open,
                    high: mark,
                    low: mark,
                });
                if self.marks.len() > KEPT_BARS {
                    self.marks.pop_front();
                }
            }
        }
    }

    /// Handles `swing`, confirmed by the event: anchors it, compares it
    /// with the previous anchored swing of its side, and appends the facts
    /// the model expects.
    fn confirm(
        &mut self,
        swing: &Swing,
        state: &MarketState,
        mid: i64,
        expected: &mut Vec<CandidateEvent>,
    ) {
        let side = side_index(swing.side);
        let mark = self
            .marks
            .iter()
            .find(|marks| marks.open == swing.swing_time.as_millis())
            .map(|marks| match swing.side {
                Side::High => marks.high,
                Side::Low => marks.low,
            })
            .filter(|mark| mark.price == swing.price);
        let Some(mark) = mark else {
            self.last[side] = None;
            return;
        };
        let current = Anchored {
            anchor: SwingAnchor {
                swing_time: swing.swing_time,
                known_at: swing.known_at,
                price: mark.price,
                time: mark.time,
                cvd: mark.cvd,
                gaps: mark.gaps,
                coverage: swing.coverage,
            },
            oi: mark.oi,
        };
        self.warm = true;
        if let Some(previous) = self.last[side] {
            self.compare(swing, &previous, &current, state, mid, expected);
        }
        self.last[side] = Some(current);
    }

    fn compare(
        &mut self,
        swing: &Swing,
        previous: &Anchored,
        current: &Anchored,
        state: &MarketState,
        mid: i64,
        expected: &mut Vec<CandidateEvent>,
    ) {
        let side = side_index(swing.side);
        let (before, after) = (&previous.anchor, &current.anchor);
        let pair = (|| {
            let gaps_between = after.gaps.checked_sub(before.gaps)?;
            let span = change(before.time.as_millis(), after.time.as_millis())?;
            Some(SwingPair {
                side: swing.side,
                previous: *before,
                current: *after,
                price_change: Price::from_units(change(before.price.units(), after.price.units())?),
                cvd_change: Qty::from_units(change(before.cvd.units(), after.cvd.units())?),
                gaps_between,
                span_ms: u64::try_from(span).ok()?,
                coverage: Coverage {
                    partial_start: before.coverage.partial_start || after.coverage.partial_start,
                    feed_gap: before.coverage.feed_gap
                        || after.coverage.feed_gap
                        || gaps_between > 0,
                },
            })
        })();
        let Some(pair) = pair else {
            self.cvd[side] = Slot::OutOfRange;
            self.oi[side] = Slot::OutOfRange;
            return;
        };
        let further = match swing.side {
            Side::High => pair.price_change.units() > 0,
            Side::Low => pair.price_change.units() < 0,
        };
        let cvd = CvdComparison {
            pair,
            divergent: further
                && match swing.side {
                    Side::High => pair.cvd_change.units() < 0,
                    Side::Low => pair.cvd_change.units() > 0,
                },
        };
        self.cvd[side] = Slot::Compared(cvd);
        let oi_change = match (previous.oi, current.oi) {
            (Some(before), Some(after)) => {
                change(before.open_interest.units(), after.open_interest.units())
                    .map(|units| Some(Qty::from_units(units)))
            }
            _ => Some(None),
        };
        let oi = oi_change.map(|oi_change| OiComparison {
            pair,
            previous_oi: previous.oi,
            current_oi: current.oi,
            oi_change,
            divergent: further && oi_change.is_some_and(|units| units.units() < 0),
        });
        self.oi[side] = oi.map_or(Slot::OutOfRange, Slot::Compared);
        let stats = &mut self.stats[half(swing.known_at, mid)][side];
        stats.pairs += 1;
        stats.further += u64::from(further);
        stats.cvd_divergent += u64::from(cvd.divergent);
        if let Some(oi) = oi
            && oi.oi_change.is_some()
        {
            stats.oi_points += 1;
            stats.oi_divergent += u64::from(oi.divergent);
        } else {
            self.missing_oi += 1;
        }
        let oi_divergent = oi.is_some_and(|oi| oi.divergent);
        if !cvd.divergent && !oi_divergent {
            self.plain += 1;
            return;
        }
        let (_, levels) = catalog::STRUCTURE_LEVELS[self.position];
        let ctx = context(
            swing.price,
            Some(Own {
                side: match swing.side {
                    Side::High => LevelSide::High,
                    Side::Low => LevelSide::Low,
                },
                source_id: levels.key.id.as_str(),
                swing_time: swing.swing_time,
            }),
            &state.location,
        );
        let (_, swings) = catalog::STRUCTURE_SWING[self.position];
        let (_, cvd_feature) = catalog::CANDIDATE_DIVERGENCE_CVD[self.position];
        let (_, oi_feature) = catalog::CANDIDATE_DIVERGENCE_OI[self.position];
        if cvd.divergent {
            expected.push(CandidateEvent::CvdDivergence(DivergenceEvent {
                feature: cvd_feature.key,
                timeframe: self.timeframe,
                known_at: swing.known_at,
                swings: swings.key,
                cvd: catalog::FLOW_CVD_CONTINUOUS_V1.key,
                comparison: cvd,
                context: ctx.clone(),
            }));
        }
        if let Some(oi) = oi.filter(|oi| oi.divergent) {
            expected.push(CandidateEvent::OiDivergence(DivergenceEvent {
                feature: oi_feature.key,
                timeframe: self.timeframe,
                known_at: swing.known_at,
                swings: swings.key,
                cvd: catalog::FLOW_CVD_CONTINUOUS_V1.key,
                comparison: oi,
                context: ctx,
            }));
        }
    }

    /// The divergence values the model expects: warming up until the first
    /// anchored swing, unavailable while a side's last comparison did not
    /// fit, ready otherwise.
    fn value(&self) -> TimeframeDivergences {
        fn family<C: Copy>(
            warm: bool,
            feature: mie_domain::feature::FeatureKey,
            slots: &[Slot<C>; 2],
        ) -> FeatureValue<Divergence<C>> {
            if !warm {
                return FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1,
                };
            }
            let mut sides = [None; 2];
            for (side, slot) in sides.iter_mut().zip(slots) {
                match slot {
                    Slot::Empty => {}
                    Slot::Compared(comparison) => *side = Some(*comparison),
                    Slot::OutOfRange => {
                        return FeatureValue::Unavailable {
                            reason: Unavailability::OutOfRange,
                        };
                    }
                }
            }
            let [high, low] = sides;
            FeatureValue::Ready(Divergence { feature, high, low })
        }
        let (_, cvd) = catalog::CANDIDATE_DIVERGENCE_CVD[self.position];
        let (_, oi) = catalog::CANDIDATE_DIVERGENCE_OI[self.position];
        TimeframeDivergences {
            cvd: family(self.warm, cvd.key, &self.cvd),
            oi: family(self.warm, oi.key, &self.oi),
        }
    }
}

/// The R0 comparison of the engine's candidate facts and values.
#[derive(Debug, Default)]
struct Check {
    mismatches: u64,
    notes: Vec<String>,
    /// Matched facts: CVD at highs and lows, OI at highs and lows.
    cvd: [u64; 2],
    oi: [u64; 2],
    /// Matched facts whose pair crosses a trades gap.
    gapped: u64,
    /// Matched facts whose context lists a level.
    located: u64,
    /// Matched ready divergence values.
    values: u64,
    /// Matched effort-vs-result facts per window, exhaustion facts and
    /// absorption facts per window.
    effort: [u64; W],
    exhaustion: u64,
    absorption: [u64; W],
    /// Matched effort-vs-result facts per location bin of their context.
    effort_bins: [u64; BINS],
    /// Matched ready effort-vs-result, exhaustion and absorption values.
    flow_values: u64,
}

impl Check {
    fn mismatch(&mut self, note: String) {
        self.mismatches += 1;
        if self.notes.len() < 20 {
            self.notes.push(note);
        }
    }

    /// Compares the facts of one event. `bins` is the model's location bins
    /// at the last close, which every effort-vs-result fact's context must
    /// give (R1's cells, decision 5).
    fn facts(
        &mut self,
        known_at: EventTime,
        engine: &[CandidateEvent],
        model: &[CandidateEvent],
        bins: Option<[bool; BINS]>,
    ) {
        if engine.len() != model.len() {
            self.mismatch(format!(
                "{known_at}: engine emitted {} candidate facts, the model {}",
                engine.len(),
                model.len()
            ));
        }
        for (theirs, ours) in engine.iter().zip(model) {
            if theirs != ours {
                self.mismatch(format!("{known_at}: engine {theirs}, model {ours}"));
                continue;
            }
            let gapped = match theirs {
                CandidateEvent::CvdDivergence(event) => {
                    self.cvd[side_index(event.comparison.pair.side)] += 1;
                    event.comparison.pair.gaps_between > 0
                }
                CandidateEvent::OiDivergence(event) => {
                    self.oi[side_index(event.comparison.pair.side)] += 1;
                    event.comparison.pair.gaps_between > 0
                }
                CandidateEvent::EffortVsResult(event) => {
                    self.effort[window_index(event.value.timeframe)] += 1;
                    let theirs = event
                        .context
                        .levels
                        .ready()
                        .map(|levels| bins_of(levels.levels.iter().map(|level| level.kind)));
                    if theirs != bins {
                        self.mismatch(format!(
                            "{known_at}: effort-vs-result context bins {theirs:?}, model {bins:?}"
                        ));
                    }
                    for (bin, inside) in theirs.unwrap_or_default().iter().enumerate() {
                        self.effort_bins[bin] += u64::from(*inside);
                    }
                    false
                }
                CandidateEvent::Exhaustion(_) => {
                    self.exhaustion += 1;
                    false
                }
                CandidateEvent::Absorption(event) => {
                    self.absorption[window_index(event.value.effort.timeframe)] += 1;
                    false
                }
            };
            self.gapped += u64::from(gapped);
            if theirs
                .context()
                .levels
                .ready()
                .is_some_and(|levels| !levels.levels.is_empty())
            {
                self.located += 1;
            }
        }
    }

    /// Compares the effort-vs-result, exhaustion and absorption values after
    /// an event that closed a 1m bar.
    fn flow_values(&mut self, known_at: EventTime, model: &EngineModel, state: &MarketState) {
        let candidates = &state.candidates;
        for (window, timeframe) in WINDOWS.iter().enumerate() {
            match candidates.effort_result(*timeframe) {
                Some(theirs) if *theirs == model.effort[window] => {
                    self.flow_values += u64::from(theirs.is_ready());
                }
                theirs => self.mismatch(format!(
                    "{known_at} {timeframe}: engine effort {theirs:?}, model {:?}",
                    model.effort[window]
                )),
            }
            match candidates.absorption(*timeframe) {
                Some(theirs) if *theirs == model.absorption[window] => {
                    self.flow_values += u64::from(theirs.is_ready());
                }
                theirs => self.mismatch(format!(
                    "{known_at} {timeframe}: engine absorption {theirs:?}, model {:?}",
                    model.absorption[window]
                )),
            }
        }
        if *candidates.exhaustion() == model.exhaustion {
            self.flow_values += u64::from(model.exhaustion.is_ready());
        } else {
            self.mismatch(format!(
                "{known_at}: engine exhaustion {:?}, model {:?}",
                candidates.exhaustion(),
                model.exhaustion
            ));
        }
    }

    fn value(&mut self, known_at: EventTime, model: &DivergenceModel, state: &MarketState) {
        let ours = model.value();
        match state.candidates.divergences(model.timeframe) {
            Some(theirs) if *theirs == ours => {
                self.values += u64::from(theirs.cvd.is_ready()) + u64::from(theirs.oi.is_ready());
            }
            theirs => self.mismatch(format!(
                "{known_at} {}: engine value {theirs:?}, model {ours:?}",
                model.timeframe
            )),
        }
    }
}

// ---------------------------------------------------------------------------
// Effort vs result (decision 5) and exhaustion (decision 6)
// ---------------------------------------------------------------------------

/// One effort-vs-result evaluation of a window.
#[derive(Debug, Clone, Copy)]
struct Evaluation {
    /// The window's end.
    end: i64,
    delta: i128,
    /// `|delta|`.
    effort: i128,
    /// `displacement × sign(delta)`.
    progress: i128,
    /// `|displacement|`.
    movement: i128,
    /// The window's coverage, which the engine's value carries.
    coverage: Coverage,
}

/// Decision 5: the evaluation of a ready window, none when `delta == 0` or
/// the displacement is missing. `Err` when effort or progress does not fit
/// an `i64` (decision 2): the engine's value is then `Unavailable`, and the
/// evaluation is no sample.
fn evaluation(window: &FeatureValue<AggressionWindow>) -> Result<Option<Evaluation>, ()> {
    let Some(window) = window.ready() else {
        return Ok(None);
    };
    let Some(displacement) = window.displacement else {
        return Ok(None);
    };
    let displacement = i128::from(displacement.units());
    let delta = i128::from(window.delta.units());
    if delta == 0 {
        return Ok(None);
    }
    let evaluation = Evaluation {
        end: window.end.as_millis(),
        delta,
        effort: delta.abs(),
        progress: displacement * delta.signum(),
        movement: displacement.abs(),
        coverage: window.coverage,
    };
    let fits = |value: i128| i64::try_from(value).is_ok();
    if fits(evaluation.effort) && fits(evaluation.progress) {
        Ok(Some(evaluation))
    } else {
        Err(())
    }
}

/// The engine's value of one judged evaluation at grid value `q` (decision
/// 5): the quantiles it was judged against and its condition.
fn effort_result(
    window: usize,
    evaluation: &Evaluation,
    quantiles: &Quantiles,
    q: usize,
) -> EffortResult {
    let units = |value: i128| i64::try_from(value).expect("a fitting evaluation");
    let (_, definition) = catalog::CANDIDATE_EFFORT_RESULT[window];
    EffortResult {
        feature: definition.key,
        timeframe: WINDOWS[window],
        end: EventTime::from_millis(evaluation.end),
        aggressor: if evaluation.delta > 0 {
            Aggressor::Buy
        } else {
            Aggressor::Sell
        },
        effort: Qty::from_units(units(evaluation.effort)),
        progress: Price::from_units(units(evaluation.progress)),
        effort_high: Qty::from_units(units(quantiles.effort_high[q])),
        effort_median: Qty::from_units(units(quantiles.effort_median)),
        move_high: Price::from_units(units(quantiles.move_high[q])),
        move_median: Price::from_units(units(quantiles.move_median)),
        coverage: evaluation.coverage,
        active: evaluation.effort >= quantiles.effort_high[q]
            && evaluation.progress <= quantiles.move_median,
    }
}

/// The previous `capacity` values in arrival order, and sorted, so every
/// nearest-rank quantile is one read after one insertion and one removal.
#[derive(Debug, Clone)]
struct Baseline {
    capacity: usize,
    arrival: VecDeque<i128>,
    sorted: Vec<i128>,
}

impl Baseline {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            arrival: VecDeque::with_capacity(capacity + 1),
            sorted: Vec::with_capacity(capacity + 1),
        }
    }

    fn ready(&self) -> bool {
        self.arrival.len() == self.capacity
    }

    /// The `thousandths`/1000 quantile, nearest rank: the ⌈q·B⌉-th smallest
    /// value. Only on a full baseline.
    fn quantile(&self, thousandths: u32) -> i128 {
        let rank = (usize::try_from(thousandths).expect("a grid value") * self.capacity)
            .div_ceil(1_000)
            .max(1);
        self.sorted[rank - 1]
    }

    fn push(&mut self, value: i128) {
        if self.arrival.len() == self.capacity {
            let old = self.arrival.pop_front().expect("a full baseline");
            let at = self.sorted.partition_point(|v| *v < old);
            self.sorted.remove(at);
        }
        self.arrival.push_back(value);
        let at = self.sorted.partition_point(|v| *v <= value);
        self.sorted.insert(at, value);
    }
}

/// The baseline quantiles an evaluation was judged against.
#[derive(Debug, Clone, Copy)]
struct Quantiles {
    effort_high: [i128; Q],
    effort_median: i128,
    move_high: [i128; Q],
    move_median: i128,
}

/// Effort vs result of one window at one baseline length, for every `q`.
#[derive(Debug, Clone)]
struct Judge {
    effort: Baseline,
    movement: Baseline,
    active: [bool; Q],
}

impl Judge {
    fn new(capacity: usize) -> Self {
        Self {
            effort: Baseline::new(capacity),
            movement: Baseline::new(capacity),
            active: [false; Q],
        }
    }

    fn ready(&self) -> bool {
        self.effort.ready()
    }

    /// Evaluations in the baseline, at most `B`.
    fn len(&self) -> usize {
        self.effort.arrival.len()
    }

    /// Judges `evaluation` against the previous evaluations, then adds it to
    /// them. Returns the quantiles it was judged against (none while the
    /// baseline fills) and the rising edges per `q`.
    fn judge(&mut self, evaluation: &Evaluation) -> (Option<Quantiles>, [bool; Q]) {
        let mut edges = [false; Q];
        let quantiles = self.ready().then(|| Quantiles {
            effort_high: GRID.map(|q| self.effort.quantile(q)),
            effort_median: self.effort.quantile(MEDIAN),
            move_high: GRID.map(|q| self.movement.quantile(q)),
            move_median: self.movement.quantile(MEDIAN),
        });
        if let Some(quantiles) = &quantiles {
            for (q, edge) in edges.iter_mut().enumerate() {
                let active = evaluation.effort >= quantiles.effort_high[q]
                    && evaluation.progress <= quantiles.move_median;
                *edge = active && !self.active[q];
                self.active[q] = active;
            }
        }
        self.effort.push(evaluation.effort);
        self.movement.push(evaluation.movement);
        (quantiles, edges)
    }
}

/// Exhaustion at one baseline length, for every `q`.
#[derive(Debug, Clone, Default)]
struct Exhaustion {
    /// The latest 1h evaluations and their quantiles, oldest first.
    expansions: VecDeque<(Evaluation, Quantiles)>,
    active: [bool; Q],
}

/// The inputs of one exhaustion evaluation, the same at every `q`.
#[derive(Debug, Clone, Copy)]
struct ExhaustionReading {
    expansion: (Evaluation, Quantiles),
    decay: (Evaluation, Quantiles),
    extreme: Price,
    close: Price,
    decayed: bool,
    at_extreme: bool,
}

impl Exhaustion {
    /// Decision 6 at the 5m evaluation `decay` ending at `t`; returns the
    /// rising edges per `q` and the evaluation's inputs, if it was
    /// evaluated.
    fn step(
        &mut self,
        expansion: Option<(Evaluation, Quantiles)>,
        decay: Option<(Evaluation, Quantiles)>,
        extreme: impl Fn(bool) -> Option<Price>,
        close: Option<Price>,
    ) -> ([bool; Q], Option<ExhaustionReading>) {
        let mut edges = [false; Q];
        if let Some(expansion) = expansion {
            self.expansions.push_back(expansion);
            if self.expansions.len() > 16 {
                self.expansions.pop_front();
            }
        }
        let Some((decay, decay_q)) = decay else {
            return (edges, None);
        };
        let Some((expansion, expansion_q)) = self
            .expansions
            .iter()
            .find(|(evaluation, _)| evaluation.end == decay.end - EXPANSION_OFFSET_MS)
            .copied()
        else {
            return (edges, None);
        };
        let up = expansion.delta > 0;
        let (Some(extreme_price), Some(close)) = (extreme(up), close) else {
            return (edges, None);
        };
        let extreme = i128::from(extreme_price.units());
        let at_extreme =
            (extreme - i128::from(close.units())).abs() * 10_000 <= extreme.abs() * EXTREME_BPS;
        let decayed = decay.delta * expansion.delta.signum() <= decay_q.effort_median;
        for (q, edge) in edges.iter_mut().enumerate() {
            let active = expansion.effort >= expansion_q.effort_high[q]
                && expansion.progress >= expansion_q.move_high[q]
                && decayed
                && at_extreme;
            *edge = active && !self.active[q];
            self.active[q] = active;
        }
        let reading = ExhaustionReading {
            expansion: (expansion, expansion_q),
            decay: (decay, decay_q),
            extreme: extreme_price,
            close,
            decayed,
            at_extreme,
        };
        (edges, Some(reading))
    }
}

/// The values and facts the engine must show at its fixed `B` and `q_hi`
/// ([`ENGINE_B`], [`ENGINE_Q`]): effort vs result, exhaustion and absorption
/// (R0 of decisions 5–7).
#[derive(Debug)]
struct EngineModel {
    effort: [FeatureValue<EffortResult>; W],
    exhaustion: FeatureValue<ExhaustionValue>,
    /// Judged 1h evaluations, up to [`EXHAUSTION_SAMPLES`].
    expansions: u64,
    absorption: [FeatureValue<Absorption>; W],
    absorbing: [bool; W],
    /// Evaluated absorption minutes and rising edges per window and half.
    absorption_evaluations: [[u64; W]; 2],
    absorption_edges: [[u64; W]; 2],
    /// Rising edges of effort vs result per window and half, and of
    /// exhaustion per half.
    effort_edges: [[u64; W]; 2],
    exhaustion_edges: [u64; 2],
}

impl EngineModel {
    fn new() -> Self {
        Self {
            effort: [0; W].map(|_| FeatureValue::WarmingUp {
                observed: 0,
                required: BASELINES[ENGINE_B] as u64,
            }),
            exhaustion: FeatureValue::WarmingUp {
                observed: 0,
                required: EXHAUSTION_SAMPLES,
            },
            expansions: 0,
            absorption: [0; W].map(|_| FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            }),
            absorbing: [false; W],
            absorption_evaluations: [[0; W]; 2],
            absorption_edges: [[0; W]; 2],
            effort_edges: [[0; W]; 2],
            exhaustion_edges: [0; 2],
        }
    }
}

/// Decision 6: an exhaustion evaluation as the engine's value, at grid
/// value `q`.
fn exhaustion_value(reading: &ExhaustionReading, active: bool, q: usize) -> ExhaustionValue {
    let (expansion, expansion_q) = reading.expansion;
    let (decay, decay_q) = reading.decay;
    let decay_aggression = decay.delta * expansion.delta.signum();
    ExhaustionValue {
        feature: catalog::CANDIDATE_EXHAUSTION_V1.key,
        end: EventTime::from_millis(decay.end),
        expansion: effort_result(EXPANSION, &expansion, &expansion_q, q),
        decay: effort_result(DECAY, &decay, &decay_q, q),
        decay_aggression: Qty::from_units(
            i64::try_from(decay_aggression).expect("a fitting evaluation"),
        ),
        extreme: reading.extreme,
        close: reading.close,
        expanded: expansion.effort >= expansion_q.effort_high[q]
            && expansion.progress >= expansion_q.move_high[q],
        decayed: reading.decayed,
        at_extreme: reading.at_extreme,
        active,
    }
}

/// Decision 7: absorption of `effort`, judged at this minute, against the
/// book window of the same length; `None` without an evaluation (the book
/// window not ready or of another minute, or the 5 bps band beyond the
/// trusted window).
fn absorption_of(
    window: usize,
    effort: &EffortResult,
    book: Option<&FeatureValue<LiquidityWindow>>,
) -> Option<Absorption> {
    let book = book?.ready()?;
    if book.end != effort.end {
        return None;
    }
    let band = book
        .bands
        .iter()
        .find(|band| band.band.units() == ABSORPTION_BAND_UNITS)?;
    if band.beyond_window {
        return None;
    }
    // Net buying lifts the ask; net selling hits the bid.
    let passive = match effort.aggressor {
        Aggressor::Buy => BookSide::Ask,
        Aggressor::Sell => BookSide::Bid,
    };
    let flow = match passive {
        BookSide::Bid => band.bid,
        BookSide::Ask => band.ask,
    };
    let replenished = flow.added.units() >= flow.filled.units();
    let (_, definition) = catalog::CANDIDATE_ABSORPTION[window];
    Some(Absorption {
        feature: definition.key,
        effort: *effort,
        passive,
        band: band.band,
        added: flow.added,
        filled: flow.filled,
        book_coverage: Coverage {
            partial_start: book.partial_start,
            feed_gap: book.feed_gap,
        },
        replenished,
        active: effort.active && replenished,
    })
}

/// Where an episode happened: its regime label and location bins, `None`
/// when unlabelled or unlocated.
#[derive(Debug, Clone, Copy)]
struct Place {
    label: Option<usize>,
    bins: Option<[bool; BINS]>,
}

/// Counts one episode at `place` into `cells` (label × bin), or into
/// `unlabelled` / `unlocated` and no cell. An episode at a value edge and a
/// structural extreme counts in both bins.
fn count(cells: &mut [[u64; BINS]; NL], unlabelled: &mut u64, unlocated: &mut u64, place: Place) {
    if place.label.is_none() {
        *unlabelled += 1;
    }
    if place.bins.is_none() {
        *unlocated += 1;
    }
    if let (Some(label), Some(bins)) = (place.label, place.bins) {
        for (bin, inside) in bins.iter().enumerate() {
            if *inside {
                cells[label][bin] += 1;
            }
        }
    }
}

/// The effort-vs-result and exhaustion model at every baseline length.
struct FlowModel {
    mid: i64,
    judges: Vec<[Judge; W]>,
    exhaustion: Vec<Exhaustion>,
    /// Closed 1m bars with trades of the last [`EXTREME_SPAN_MS`]: end,
    /// high, low.
    minutes: VecDeque<(i64, Price, Price)>,
    /// Close of the last closed 1m bar with trades.
    last_close: Option<Price>,
    /// Per baseline, per half.
    hours: [[HourTable; 2]; NB],
    cells: [[CellTable; 2]; NB],
    unlabelled: [[GridCounts; 2]; NB],
    unlocated: [[GridCounts; 2]; NB],
    episodes: [[GridCounts; 2]; NB],
    exhausted: [[[u64; Q]; 2]; NB],
    evaluations: [u64; W],
    /// The engine's values and facts at its fixed `B` and `q_hi` (R0).
    engine: EngineModel,
}

impl FlowModel {
    fn new(mid: i64) -> Self {
        Self {
            mid,
            judges: BASELINES
                .iter()
                .map(|capacity| [0; W].map(|_| Judge::new(*capacity)))
                .collect(),
            exhaustion: vec![Exhaustion::default(); NB],
            minutes: VecDeque::new(),
            last_close: None,
            hours: [[[[[0; HOURS]; Q]; W]; 2]; NB],
            cells: [[[[[[0; BINS]; NL]; Q]; W]; 2]; NB],
            unlabelled: [[[[0; Q]; W]; 2]; NB],
            unlocated: [[[[0; Q]; W]; 2]; NB],
            episodes: [[[[0; Q]; W]; 2]; NB],
            exhausted: [[[0; Q]; 2]; NB],
            evaluations: [0; W],
            engine: EngineModel::new(),
        }
    }

    /// One accepted event: evaluates every window once if it closed a 1m
    /// bar, and appends the facts the engine must emit at its fixed `B` and
    /// `q_hi`: effort vs result per window, exhaustion, absorption per
    /// window (decision 2's order). Returns whether it closed a 1m bar.
    fn step(
        &mut self,
        engine: &MarketStateEngine,
        time: EventTime,
        expected: &mut Vec<CandidateEvent>,
    ) -> bool {
        let mut closed = false;
        for bar in engine.closed_bars() {
            if bar.timeframe != Timeframe::M1 {
                continue;
            }
            closed = true;
            if let Some(ohlc) = bar.ohlc {
                self.minutes
                    .push_back((bar.end().as_millis(), ohlc.high, ohlc.low));
                self.last_close = Some(ohlc.close);
            }
        }
        if !closed {
            return false;
        }
        let state = engine.state();
        let readings: [Result<Option<Evaluation>, ()>; W] = WINDOWS.map(|timeframe| {
            state
                .flow
                .windows
                .get(timeframe)
                .map_or(Ok(None), evaluation)
        });
        for (window, reading) in readings.iter().enumerate() {
            if reading.is_err() {
                // Decision 2: out of range, no sample.
                self.engine.effort[window] = FeatureValue::Unavailable {
                    reason: Unavailability::OutOfRange,
                };
            }
        }
        let evaluations: [Option<Evaluation>; W] = readings.map(|reading| reading.ok().flatten());
        let reference_ready: [bool; W] =
            std::array::from_fn(|window| self.judges[REFERENCE][window].ready());
        let half = half(time, self.mid);
        let hour = hour(time);
        let mut place: Option<Place> = None;
        let context =
            |close: Option<Price>| close.map(|close| context(close, None, &state.location));
        let mut facts: Vec<CandidateEvent> = Vec::new();
        let mut engine_judged: [Option<EffortResult>; W] = [None; W];
        for b in 0..NB {
            let mut judged: [Option<(Evaluation, Quantiles)>; W] = [None; W];
            for window in 0..W {
                let Some(evaluation) = &evaluations[window] else {
                    continue;
                };
                if b == 0 {
                    self.evaluations[window] += 1;
                }
                let earlier = self.judges[b][window].len();
                let (quantiles, edges) = self.judges[b][window].judge(evaluation);
                judged[window] = quantiles.map(|quantiles| (*evaluation, quantiles));
                if b == ENGINE_B {
                    match &quantiles {
                        Some(quantiles) => {
                            let result = effort_result(window, evaluation, quantiles, ENGINE_Q);
                            self.engine.effort[window] = FeatureValue::Ready(result);
                            engine_judged[window] = Some(result);
                            if edges[ENGINE_Q] {
                                self.engine.effort_edges[half][window] += 1;
                                if let Some(context) = context(self.last_close) {
                                    let (_, definition) = catalog::CANDIDATE_EFFORT_RESULT[window];
                                    facts.push(CandidateEvent::EffortVsResult(EdgeEvent {
                                        feature: definition.key,
                                        known_at: time,
                                        inputs: definition.inputs,
                                        value: result,
                                        context,
                                    }));
                                }
                            }
                        }
                        None => {
                            self.engine.effort[window] = FeatureValue::WarmingUp {
                                observed: earlier as u64,
                                required: BASELINES[ENGINE_B] as u64,
                            };
                        }
                    }
                }
                for q in (0..Q).filter(|q| edges[*q]) {
                    self.episodes[b][half][window][q] += 1;
                    if reference_ready[window] {
                        self.hours[b][half][window][q][hour] += 1;
                    }
                    let at = *place.get_or_insert_with(|| self.place(state));
                    count(
                        &mut self.cells[b][half][window][q],
                        &mut self.unlabelled[b][half][window][q],
                        &mut self.unlocated[b][half][window][q],
                        at,
                    );
                }
            }
            let t = evaluations[DECAY].map(|evaluation| evaluation.end);
            let minutes = &self.minutes;
            let extreme = |up: bool| {
                let t = t?;
                let span = minutes
                    .iter()
                    .filter(|(end, _, _)| *end > t - EXTREME_SPAN_MS && *end <= t);
                if up {
                    span.map(|(_, high, _)| *high).max()
                } else {
                    span.map(|(_, _, low)| *low).min()
                }
            };
            let (edges, reading) =
                self.exhaustion[b].step(judged[EXPANSION], judged[DECAY], extreme, self.last_close);
            for q in (0..Q).filter(|q| edges[*q]) {
                self.exhausted[b][half][q] += 1;
            }
            if b != ENGINE_B {
                continue;
            }
            // The engine's exhaustion: warming up over the judged 1h
            // evaluations until the first evaluation.
            if judged[EXPANSION].is_some() {
                self.engine.expansions = (self.engine.expansions + 1).min(EXHAUSTION_SAMPLES);
                if !self.engine.exhaustion.is_ready() {
                    self.engine.exhaustion = FeatureValue::WarmingUp {
                        observed: self.engine.expansions,
                        required: EXHAUSTION_SAMPLES,
                    };
                }
            }
            if let Some(reading) = reading {
                let value =
                    exhaustion_value(&reading, self.exhaustion[b].active[ENGINE_Q], ENGINE_Q);
                self.engine.exhaustion = FeatureValue::Ready(value);
                if edges[ENGINE_Q] {
                    self.engine.exhaustion_edges[half] += 1;
                    if let Some(context) = context(self.last_close) {
                        let definition = &catalog::CANDIDATE_EXHAUSTION_V1;
                        facts.push(CandidateEvent::Exhaustion(EdgeEvent {
                            feature: definition.key,
                            known_at: time,
                            inputs: definition.inputs,
                            value,
                            context,
                        }));
                    }
                }
            }
            for (window, effort) in engine_judged.iter().enumerate() {
                let Some(effort) = effort else {
                    continue;
                };
                let Some(absorption) =
                    absorption_of(window, effort, state.book.windows.get(WINDOWS[window]))
                else {
                    continue;
                };
                self.engine.absorption_evaluations[half][window] += 1;
                self.engine.absorption[window] = FeatureValue::Ready(absorption);
                let edge = absorption.active && !self.engine.absorbing[window];
                self.engine.absorbing[window] = absorption.active;
                if edge {
                    self.engine.absorption_edges[half][window] += 1;
                    if let Some(context) = context(self.last_close) {
                        let (_, definition) = catalog::CANDIDATE_ABSORPTION[window];
                        facts.push(CandidateEvent::Absorption(EdgeEvent {
                            feature: definition.key,
                            known_at: time,
                            inputs: definition.inputs,
                            value: absorption,
                            context,
                        }));
                    }
                }
            }
        }
        // Pushed in decision 2's order: effort vs result per window,
        // exhaustion, absorption per window.
        expected.extend(facts);
        if let Some(latest) = self.minutes.back().map(|(end, _, _)| *end) {
            while self
                .minutes
                .front()
                .is_some_and(|(end, _, _)| *end <= latest - EXTREME_SPAN_MS)
            {
                self.minutes.pop_front();
            }
        }
        true
    }

    /// The regime label and the location bins at the last close.
    fn place(&self, state: &MarketState) -> Place {
        let label = state.regime.ready().map(|regime| label_index(regime.label));
        let bins = match (&state.location.levels, self.last_close) {
            (FeatureValue::Ready(set), Some(close)) => Some(bins_of(
                set.levels()
                    .iter()
                    .filter(|level| in_zone(level, close))
                    .map(|level| level.kind),
            )),
            _ => None,
        };
        Place { label, bins }
    }
}

// ---------------------------------------------------------------------------
// The fixing rules (decision 5), pure functions over count tables
// ---------------------------------------------------------------------------

/// `ln Γ(x)` for `x ≥ 0.5`: Lanczos, g = 7, nine coefficients.
fn ln_gamma(x: f64) -> f64 {
    const COEFFICIENTS: [f64; 9] = [
        0.999_999_999_999_809_9,
        676.520_368_121_885_1,
        -1_259.139_216_722_402_8,
        771.323_428_777_653_1,
        -176.615_029_162_140_6,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    let x = x - 1.0;
    let t = x + 7.5;
    let mut sum = COEFFICIENTS[0];
    for (i, coefficient) in COEFFICIENTS.iter().enumerate().skip(1) {
        sum += coefficient / (x + i as f64);
    }
    0.5 * (2.0 * std::f64::consts::PI).ln() + (x + 0.5) * t.ln() - t + sum.ln()
}

/// The regularized lower incomplete gamma function `P(a, x)`: the series
/// below `a + 1`, the continued fraction (modified Lentz) above.
fn lower_gamma(a: f64, x: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    let front = (a * x.ln() - x - ln_gamma(a)).exp();
    if x < a + 1.0 {
        let mut term = 1.0 / a;
        let mut sum = term;
        let mut n = a;
        for _ in 0..1_000 {
            n += 1.0;
            term *= x / n;
            sum += term;
            if term.abs() < sum.abs() * 1e-16 {
                break;
            }
        }
        sum * front
    } else {
        let tiny = 1e-300;
        let mut b = x + 1.0 - a;
        let mut c = 1.0 / tiny;
        let mut d = 1.0 / b;
        let mut h = d;
        for i in 1..1_000 {
            let i = f64::from(i);
            let an = -i * (i - a);
            b += 2.0;
            d = an * d + b;
            if d.abs() < tiny {
                d = tiny;
            }
            c = b + an / c;
            if c.abs() < tiny {
                c = tiny;
            }
            d = 1.0 / d;
            let delta = d * c;
            h *= delta;
            if (delta - 1.0).abs() < 1e-16 {
                break;
            }
        }
        1.0 - front * h
    }
}

/// The `p` quantile of χ² with `degrees` degrees of freedom, by bisection
/// on the CDF `P(degrees / 2, x / 2)`.
fn chi_square_quantile(p: f64, degrees: f64) -> f64 {
    let (mut low, mut high) = (0.0_f64, 1_000.0_f64);
    for _ in 0..200 {
        let mid = 0.5 * (low + high);
        if lower_gamma(degrees / 2.0, mid / 2.0) < p {
            low = mid;
        } else {
            high = mid;
        }
    }
    0.5 * (low + high)
}

/// R1b's degrees of freedom: (2 − 1) baselines × (24 − 1) hours.
const DEGREES: usize = HOURS - 1;

/// Cochran's rule (decision 5): an expected count below this makes a
/// combination not evaluable.
const MIN_EXPECTED: u128 = 1;

/// Cochran's rule: the expected count below which a cell is sparse.
const SPARSE_EXPECTED: u128 = 5;

/// Cochran's rule: at most 20 % of the 48 cells may be sparse, 9 of 48.
const MAX_SPARSE: usize = 9;

/// R1b's critical value for `k` evaluable combinations:
/// `χ²₂₃(1 − 0.05/k)`.
fn critical(k: usize) -> f64 {
    chi_square_quantile(1.0 - FAMILY_LEVEL / k as f64, DEGREES as f64)
}

/// R1b's statistic of one combination (decision 5): Pearson's X² of the
/// 2 × 24 table (`B`, reference) × UTC hour, its upper-tail `p` under χ²₂₃,
/// and Cochran's evaluability. With `m(h) = n_B(h) + n_ref(h)` and `N` the
/// table's total, the expected count of `B` in hour `h` is `m(h)·N_B/N`, of
/// the reference `m(h)·N_ref/N`; the cell counts and the evaluability are
/// exact in integers. Not evaluable when either baseline has no episode,
/// when an expected count is below 1 (so also when an hour has no episode
/// of either), or when more than 9 of the 48 expected counts are below 5.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Combination {
    x2: f64,
    p: f64,
    /// Cells with an expected count below 5.
    below_five: usize,
    /// Cells with an expected count below 1.
    below_one: usize,
    evaluable: bool,
}

fn combination(n_b: &[u64; HOURS], n_ref: &[u64; HOURS]) -> Combination {
    let total_b: u128 = n_b.iter().map(|n| u128::from(*n)).sum();
    let total_ref: u128 = n_ref.iter().map(|n| u128::from(*n)).sum();
    let total = total_b + total_ref;
    let mut below_five = 0;
    let mut below_one = 0;
    for (b, reference) in n_b.iter().zip(n_ref) {
        let m = u128::from(*b) + u128::from(*reference);
        for side in [total_b, total_ref] {
            // `m(h)·N_side / N < c` ⇔ `m(h)·N_side < c·N`.
            below_five += usize::from(m * side < SPARSE_EXPECTED * total);
            below_one += usize::from(m * side < MIN_EXPECTED * total);
        }
    }
    if total_b == 0 || total_ref == 0 {
        return Combination {
            x2: f64::NAN,
            p: f64::NAN,
            below_five,
            below_one,
            evaluable: false,
        };
    }
    // `Σ (n_B(h)·N_ref − n_ref(h)·N_B)² / (m(h)·N_B·N_ref)`, the 48-cell
    // `Σ (O − E)²/E`; an hour without episodes adds nothing.
    let x2: f64 = n_b
        .iter()
        .zip(n_ref)
        .filter(|(b, reference)| **b + **reference > 0)
        .map(|(b, reference)| {
            let m = u128::from(*b) + u128::from(*reference);
            let numerator = (u128::from(*b) * total_ref).abs_diff(u128::from(*reference) * total_b);
            (numerator as f64).powi(2) / (m * total_b * total_ref) as f64
        })
        .sum();
    Combination {
        x2,
        p: 1.0 - lower_gamma(DEGREES as f64 / 2.0, x2 / 2.0),
        below_five,
        below_one,
        evaluable: below_one == 0 && below_five <= MAX_SPARSE,
    }
}

/// R1b on one baseline and half.
#[derive(Debug, Clone, PartialEq)]
struct HalfTest {
    combinations: [[Combination; Q]; W],
    /// Evaluable combinations.
    k: usize,
    /// The first window length without an evaluable combination.
    unevaluable_window: Option<usize>,
    /// Some evaluable combination has `X² > χ²₂₃(1 − 0.05/k)`.
    fails: bool,
    /// No evaluable combination fails and every window length has one.
    pass: bool,
}

impl HalfTest {
    fn passes(&self, combination: &Combination) -> bool {
        combination.evaluable && combination.x2 <= critical(self.k)
    }
}

fn half_test(b: &HourTable, reference: &HourTable) -> HalfTest {
    let combinations: [[Combination; Q]; W] = std::array::from_fn(|window| {
        std::array::from_fn(|q| combination(&b[window][q], &reference[window][q]))
    });
    let k = combinations
        .iter()
        .flatten()
        .filter(|combination| combination.evaluable)
        .count();
    let unevaluable_window = combinations
        .iter()
        .position(|row| row.iter().all(|combination| !combination.evaluable));
    let mut test = HalfTest {
        combinations,
        k,
        unevaluable_window,
        fails: false,
        pass: false,
    };
    test.fails = test
        .combinations
        .iter()
        .flatten()
        .any(|combination| combination.evaluable && !test.passes(combination));
    test.pass = !test.fails && unevaluable_window.is_none();
    test
}

/// R1b's verdict over the candidates of [`BASELINES`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum R1b {
    /// `B` is the candidate at this index.
    Fixed { candidate: usize },
    /// Stop: a window length of this candidate has no evaluable combination
    /// in this half, and no evaluable combination fails. A larger `B` is not
    /// tried past it: that would pick a value while the smaller one may still
    /// pass. A candidate that fails an evaluable combination is out whatever
    /// its untestable window would show, so it never stops the rule.
    NotEvaluable {
        candidate: usize,
        half: usize,
        window: usize,
    },
    /// Stop: no candidate passes on the first half.
    NoCandidatePasses,
    /// Stop: the smallest candidate passing the first half fails the second.
    SecondHalfFails { candidate: usize },
}

/// R1b (decision 5): `B` is the smallest candidate that passes in every
/// evaluable combination on the first half, checked on the second.
/// `tables` holds every baseline's hour tables per half; the last is the
/// reference. Per candidate and half, a failing evaluable combination decides
/// first (the next candidate, or a second-half failure); only a candidate
/// without one stops on a window length that has no evaluable combination.
fn r1b(tables: &[[HourTable; 2]; NB]) -> R1b {
    let reference = &tables[REFERENCE];
    for (candidate, table) in tables.iter().enumerate().take(REFERENCE) {
        let first = half_test(&table[0], &reference[0]);
        if first.fails {
            continue;
        }
        if let Some(window) = first.unevaluable_window {
            return R1b::NotEvaluable {
                candidate,
                half: 0,
                window,
            };
        }
        let second = half_test(&table[1], &reference[1]);
        if second.fails {
            return R1b::SecondHalfFails { candidate };
        }
        if let Some(window) = second.unevaluable_window {
            return R1b::NotEvaluable {
                candidate,
                half: 1,
                window,
            };
        }
        return R1b::Fixed { candidate };
    }
    R1b::NoCandidatePasses
}

/// A cell R1's floor holds: one regime × location cell, or a bin pooled
/// over the four labels.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FloorCell {
    Cell { label: usize, bin: usize },
    Pooled { bin: usize },
}

impl FloorCell {
    fn count(self, table: &CellTable, window: usize, q: usize) -> u64 {
        match self {
            Self::Cell { label, bin } => table[window][q][label][bin],
            Self::Pooled { bin } => table[window][q].iter().map(|labels| labels[bin]).sum(),
        }
    }

    fn servable(self, table: &CellTable, window: usize) -> bool {
        (0..Q).any(|q| self.count(table, window, q) >= N_MIN)
    }

    fn name(self) -> String {
        match self {
            Self::Cell { label, bin } => format!("{}/{}", LABELS[label], BIN_NAMES[bin]),
            Self::Pooled { bin } => format!("pooled/{}", BIN_NAMES[bin]),
        }
    }
}

/// The minimum-cell classification of one window length.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct WindowFloor {
    floor: Vec<FloorCell>,
    /// Bins whose pooled cell replaced a non-servable cell.
    pooled: Vec<usize>,
    /// Bins left out of the floor: their pooled cell is not servable either.
    unpowered: Vec<usize>,
}

/// The minimum-cell rule (MC) on the first half, read at every grid value:
/// a servable cell is a floor cell; a non-servable `value_edge` or
/// `structural_extreme` cell is replaced by its bin's pooled cell, or makes
/// the bin unpowered if that is not servable either; a non-servable
/// `neither` cell stops R1 (`Err((window, label))`).
fn classify(first: &CellTable) -> Result<[WindowFloor; W], (usize, usize)> {
    let mut floors: [WindowFloor; W] = Default::default();
    for (window, floor) in floors.iter_mut().enumerate() {
        for label in 0..NL {
            let cell = FloorCell::Cell {
                label,
                bin: NEITHER,
            };
            if !cell.servable(first, window) {
                return Err((window, label));
            }
            floor.floor.push(cell);
        }
        for bin in [VALUE_EDGE, STRUCTURAL_EXTREME] {
            let mut replaced = false;
            for label in 0..NL {
                let cell = FloorCell::Cell { label, bin };
                if cell.servable(first, window) {
                    floor.floor.push(cell);
                } else {
                    replaced = true;
                }
            }
            if !replaced {
                continue;
            }
            let pooled = FloorCell::Pooled { bin };
            if pooled.servable(first, window) {
                floor.floor.push(pooled);
                floor.pooled.push(bin);
            } else {
                floor.unpowered.push(bin);
            }
        }
    }
    Ok(floors)
}

/// R1's verdict.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum R1 {
    /// `q_hi` is the grid value at this index; `bottom` when it is the
    /// lowest (the floor binds at the bottom of the grid).
    Fixed { q: usize, bottom: bool },
    /// Stop: a `neither` cell is not servable.
    NeitherNotServable { window: usize, label: usize },
    /// Stop: no grid value passes every floor cell on the first half.
    NoGridValuePasses,
    /// Stop: the second half misses a frozen floor cell at `q_hi`.
    SecondHalfMisses {
        q: usize,
        window: usize,
        cell: FloorCell,
    },
}

/// R1 (decision 5): `q_hi` is the highest grid value at which every floor
/// cell of every window length holds at least [`N_MIN`] episodes on the
/// first half; the second half checks the frozen floor at it.
fn r1(first: &CellTable, second: &CellTable) -> R1 {
    let floors = match classify(first) {
        Ok(floors) => floors,
        Err((window, label)) => return R1::NeitherNotServable { window, label },
    };
    let passes = |table: &CellTable, q: usize| {
        floors.iter().enumerate().all(|(window, floor)| {
            floor
                .floor
                .iter()
                .all(|cell| cell.count(table, window, q) >= N_MIN)
        })
    };
    let Some(q) = (0..Q).rev().find(|q| passes(first, *q)) else {
        return R1::NoGridValuePasses;
    };
    for (window, floor) in floors.iter().enumerate() {
        for cell in &floor.floor {
            if cell.count(second, window, q) < N_MIN {
                return R1::SecondHalfMisses {
                    q,
                    window,
                    cell: *cell,
                };
            }
        }
    }
    R1::Fixed { q, bottom: q == 0 }
}

// ---------------------------------------------------------------------------
// The run and its report
// ---------------------------------------------------------------------------

struct Run {
    engine: MarketStateEngine,
    mid: i64,
    events: u64,
    rejections: u64,
    cvd: CvdModel,
    divergences: [DivergenceModel; 4],
    flow: FlowModel,
    check: Check,
    expected: Vec<CandidateEvent>,
}

impl Run {
    fn new(mid: i64) -> Self {
        Self {
            engine: MarketStateEngine::new(),
            mid,
            events: 0,
            rejections: 0,
            cvd: CvdModel::default(),
            divergences: [0, 1, 2, 3].map(DivergenceModel::new),
            flow: FlowModel::new(mid),
            check: Check::default(),
            expected: Vec::new(),
        }
    }

    fn step(&mut self, event: &MarketEvent) {
        self.events += 1;
        if self.engine.apply(event).is_err() {
            self.rejections += 1;
            return;
        }
        let known_at = event.time();
        self.cvd.apply(event);
        let state = self.engine.state();
        // Per closed bar in close order, the swings it confirmed; the trade
        // comes last (decision 4).
        self.expected.clear();
        let mut closed = [false; 4];
        for bar in self.engine.closed_bars() {
            let Some(position) = STRUCTURE_TIMEFRAMES
                .iter()
                .position(|timeframe| *timeframe == bar.timeframe)
            else {
                continue;
            };
            closed[position] = true;
            for fact in self.engine.structure_events() {
                if let StructureEvent::Swing(swing) = fact
                    && swing.timeframe == bar.timeframe
                    && swing.confirmed_bar_end == bar.end()
                {
                    self.divergences[position].confirm(swing, state, self.mid, &mut self.expected);
                }
            }
        }
        if let MarketEvent::Trade(trade) = event {
            let oi = state.derivatives.oi_5m.ready();
            for model in &mut self.divergences {
                model.trade(trade, &self.cvd, oi);
            }
        }
        // Effort vs result, exhaustion and absorption follow the
        // divergences (decision 2).
        let minute = self.flow.step(&self.engine, known_at, &mut self.expected);
        let bins = self
            .expected
            .iter()
            .any(|fact| matches!(fact, CandidateEvent::EffortVsResult(_)))
            .then(|| self.flow.place(state).bins)
            .flatten();
        self.check.facts(
            known_at,
            self.engine.candidate_events(),
            &self.expected,
            bins,
        );
        for (position, done) in closed.iter().enumerate() {
            if *done {
                self.check
                    .value(known_at, &self.divergences[position], state);
            }
        }
        if minute {
            self.check.flow_values(known_at, &self.flow.engine, state);
        }
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn share(part: u64, whole: u64) -> String {
    if whole == 0 {
        "-".to_owned()
    } else {
        format!("{:.4}", part as f64 / whole as f64)
    }
}

fn q_label(q: usize) -> String {
    format!("{:.3}", f64::from(GRID[q]) / 1_000.0)
}

fn half_label(half: Option<usize>) -> &'static str {
    match half {
        Some(0) => "first",
        Some(_) => "second",
        None => "total",
    }
}

fn hours_line(counts: &[u64; HOURS]) -> String {
    counts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn report(run: &Run, from: i64, to: i64, wall: Duration) {
    println!(
        "window {}..{} ms, split {} ms; events {}, rejections {}, wall {:.1} s",
        from,
        to,
        run.mid,
        run.events,
        run.rejections,
        wall.as_secs_f64()
    );
    report_r0(&run.check);
    report_divergences(run);
    report_r1b(&run.flow);
    report_r1(&run.flow);
    report_exhaustion(&run.flow);
    report_engine_rates(&run.flow);
}

/// R0's counts and the first mismatches.
fn report_r0(check: &Check) {
    println!(
        "R0: mismatches {}; matched facts cvd high {} low {}, oi high {} low {}, \
         across a trades gap {}, with a located context {}; ready values {}",
        check.mismatches,
        check.cvd[0],
        check.cvd[1],
        check.oi[0],
        check.oi[1],
        check.gapped,
        check.located,
        check.values
    );
    println!(
        "R0 (M3, B={} q={}): matched facts effort_result 5m/15m/1h {:?}, exhaustion {}, \
         absorption 5m/15m/1h {:?}; effort_result facts per bin value_edge/structural_extreme/\
         neither {:?}; ready values {}",
        BASELINES[ENGINE_B],
        q_label(ENGINE_Q),
        check.effort,
        check.exhaustion,
        check.absorption,
        check.effort_bins,
        check.flow_values
    );
    for note in &check.notes {
        println!("R0 mismatch: {note}");
    }
}

/// The rates decision 9 records at the engine's fixed `B` and `q_hi`:
/// effort-vs-result, exhaustion and absorption episodes per half, and the
/// minutes absorption was evaluated.
fn report_engine_rates(flow: &FlowModel) {
    let engine = &flow.engine;
    for half in 0..2 {
        println!(
            "M3 rates B={} q={} {}: effort_result 5m/15m/1h {:?}, exhaustion {}, \
             absorption 5m/15m/1h {:?} of evaluated minutes {:?}",
            BASELINES[ENGINE_B],
            q_label(ENGINE_Q),
            half_label(Some(half)),
            engine.effort_edges[half],
            engine.exhaustion_edges[half],
            engine.absorption_edges[half],
            engine.absorption_evaluations[half]
        );
    }
}

fn report_divergences(run: &Run) {
    println!(
        "D9 divergences: timeframe half side pairs further cvd_divergent oi_points oi_divergent"
    );
    for model in &run.divergences {
        for half in [Some(0), Some(1), None] {
            for side in [Some(0), Some(1), None] {
                let mut stats = PairStats::default();
                for h in 0..2 {
                    for s in 0..2 {
                        if half.is_none_or(|half| half == h) && side.is_none_or(|side| side == s) {
                            stats.add(&model.stats[h][s]);
                        }
                    }
                }
                println!(
                    "D9 {} {} {}: pairs {} further {} ({}) cvd_divergent {} ({}) \
                     oi_points {} oi_divergent {} ({} of pairs)",
                    model.timeframe,
                    half_label(half),
                    side.map_or("both", |side| ["high", "low"][side]),
                    stats.pairs,
                    stats.further,
                    share(stats.further, stats.pairs),
                    stats.cvd_divergent,
                    share(stats.cvd_divergent, stats.pairs),
                    stats.oi_points,
                    stats.oi_divergent,
                    share(stats.oi_divergent, stats.pairs)
                );
            }
        }
    }
}

/// The first line of R1b's report: which statistic the lines below carry.
const R1B_STATISTIC: &str =
    "R1b statistic: Pearson X² homogeneity, 2 × 24, Cochran evaluability (ADR-051 D5, #124)";

/// The reference's hour line of one combination. [`parse_r1b_line`] reads it
/// back; M2's log has the same prefix.
fn reference_line(half: usize, window: usize, q: usize, n_ref: &[u64; HOURS]) -> String {
    format!(
        "R1b n_ref B={} {} {} q={}: [{}]",
        BASELINES[REFERENCE],
        half_label(Some(half)),
        WINDOWS[window],
        q_label(q),
        hours_line(n_ref)
    )
}

/// A candidate's line of one combination, with its statistic and verdict
/// under the half's `k`. [`parse_r1b_line`] reads its hours back; M2's log
/// has the same prefix up to the hours.
fn combination_line(
    candidate: usize,
    half: usize,
    window: usize,
    q: usize,
    n_b: &[u64; HOURS],
    test: &HalfTest,
) -> String {
    let combination = &test.combinations[window][q];
    let crit = if test.k > 0 {
        critical(test.k)
    } else {
        f64::NAN
    };
    format!(
        "R1b B={} {} {} q={}: n_B [{}] X2 {:.3} p {:.4} below5 {} below1 {} crit {:.3} (k={}) {}",
        BASELINES[candidate],
        half_label(Some(half)),
        WINDOWS[window],
        q_label(q),
        hours_line(n_b),
        combination.x2,
        combination.p,
        combination.below_five,
        combination.below_one,
        crit,
        test.k,
        if !combination.evaluable {
            "not evaluable"
        } else if test.passes(combination) {
            "PASS"
        } else {
            "FAIL"
        }
    )
}

fn report_r1b(flow: &FlowModel) {
    println!("{R1B_STATISTIC}");
    println!(
        "R1b: evaluations per window {:?}; episodes counted while the reference B={} is ready",
        flow.evaluations, BASELINES[REFERENCE]
    );
    report_r1b_tables(&flow.hours);
}

/// R1b's per-combination lines, the per-half summaries and the verdict, from
/// the hour tables alone (a run, or a logged run's tables).
fn report_r1b_tables(hours: &[[HourTable; 2]; NB]) {
    for (half, table) in hours[REFERENCE].iter().enumerate() {
        for (window, rows) in table.iter().enumerate() {
            for (q, n_ref) in rows.iter().enumerate() {
                println!("{}", reference_line(half, window, q, n_ref));
            }
        }
    }
    for (candidate, baseline) in BASELINES.iter().enumerate().take(REFERENCE) {
        for (half, (table, reference)) in hours[candidate].iter().zip(&hours[REFERENCE]).enumerate()
        {
            let test = half_test(table, reference);
            for (window, rows) in table.iter().enumerate() {
                for (q, n_b) in rows.iter().enumerate() {
                    println!(
                        "{}",
                        combination_line(candidate, half, window, q, n_b, &test)
                    );
                }
            }
            println!(
                "R1b B={} {}: k {} -> {}",
                baseline,
                half_label(Some(half)),
                test.k,
                match test.unevaluable_window {
                    _ if test.fails => "FAIL".to_owned(),
                    Some(window) => format!("{} has no evaluable combination", WINDOWS[window]),
                    None => "PASS".to_owned(),
                }
            );
        }
    }
    println!(
        "R1b verdict: {}",
        match r1b(hours) {
            R1b::Fixed { candidate } => format!("PASS, B = {}", BASELINES[candidate]),
            R1b::NotEvaluable {
                candidate,
                half,
                window,
            } => format!(
                "STOP, B = {} has no evaluable combination on {} in the {} half",
                BASELINES[candidate],
                WINDOWS[window],
                half_label(Some(half))
            ),
            R1b::NoCandidatePasses => "STOP, no candidate passes on the first half".to_owned(),
            R1b::SecondHalfFails { candidate } => format!(
                "STOP, B = {} fails on the second half",
                BASELINES[candidate]
            ),
        }
    );
}

/// One combination's hour line of a logged run, in M2's format or this
/// one's: `R1b n_ref B=… <half> <tf> q=…: [..]` for the reference and
/// `R1b B=… <half> <tf> q=…: n_B [..] …` for a candidate. Returns the
/// baseline's index, the half, the window, `q` and the 24 counts; `None` for
/// every other line.
fn parse_r1b_line(line: &str) -> Option<(usize, usize, usize, usize, [u64; HOURS])> {
    let rest = line.trim();
    let rest = rest
        .strip_prefix("R1b n_ref ")
        .or_else(|| rest.strip_prefix("R1b "))?;
    let (head, tail) = rest.split_once(": ")?;
    let mut fields = head.split(' ');
    let baseline: usize = fields.next()?.strip_prefix("B=")?.parse().ok()?;
    let b = BASELINES.iter().position(|value| *value == baseline)?;
    let half = match fields.next()? {
        "first" => 0,
        "second" => 1,
        _ => return None,
    };
    let timeframe = fields.next()?;
    let window = WINDOWS
        .iter()
        .position(|window| window.to_string() == timeframe)?;
    let q_text = fields.next()?.strip_prefix("q=")?;
    let q = (0..Q).position(|q| q_label(q) == q_text)?;
    if fields.next().is_some() {
        return None;
    }
    let tail = tail.strip_prefix("n_B ").unwrap_or(tail);
    let (list, _) = tail.strip_prefix('[')?.split_once(']')?;
    let counts: Vec<u64> = list
        .split(',')
        .map(|count| count.parse().ok())
        .collect::<Option<_>>()?;
    Some((b, half, window, q, counts.try_into().ok()?))
}

/// The hour tables of a logged run, and the number of hour lines read.
fn parse_r1b_log(text: &str) -> ([[HourTable; 2]; NB], usize) {
    let mut tables = [[[[[0; HOURS]; Q]; W]; 2]; NB];
    let mut lines = 0;
    for (b, half, window, q, counts) in text.lines().filter_map(parse_r1b_line) {
        tables[b][half][window][q] = counts;
        lines += 1;
    }
    (tables, lines)
}

fn report_r1(flow: &FlowModel) {
    let chosen = match r1b(&flow.hours) {
        R1b::Fixed { candidate } => Some(candidate),
        _ => None,
    };
    let printed: Vec<usize> = chosen.map_or_else(|| (0..NB).collect(), |candidate| vec![candidate]);
    if chosen.is_none() {
        println!("R1: R1b stopped; the tables at every B follow, with no verdict");
    }
    for b in printed {
        let cells = &flow.cells[b];
        for (half, tables) in cells.iter().enumerate() {
            for window in 0..W {
                for q in 0..Q {
                    let table = &tables[window][q];
                    let mut line = format!(
                        "R1 B={} {} {} q={}: episodes {} unlabelled {} unlocated {} |",
                        BASELINES[b],
                        half_label(Some(half)),
                        WINDOWS[window],
                        q_label(q),
                        flow.episodes[b][half][window][q],
                        flow.unlabelled[b][half][window][q],
                        flow.unlocated[b][half][window][q]
                    );
                    for (label, name) in LABELS.iter().enumerate() {
                        line.push_str(&format!(
                            " {name} {}/{}/{}",
                            table[label][VALUE_EDGE],
                            table[label][STRUCTURAL_EXTREME],
                            table[label][NEITHER]
                        ));
                    }
                    line.push_str(&format!(
                        " | pooled {}/{}/{}",
                        FloorCell::Pooled { bin: VALUE_EDGE }.count(tables, window, q),
                        FloorCell::Pooled {
                            bin: STRUCTURAL_EXTREME
                        }
                        .count(tables, window, q),
                        FloorCell::Pooled { bin: NEITHER }.count(tables, window, q)
                    ));
                    println!("{line}");
                }
            }
        }
        match classify(&cells[0]) {
            Ok(floors) => {
                for (window, floor) in floors.iter().enumerate() {
                    println!(
                        "R1 MC B={} {}: floor [{}] pooled [{}] unpowered [{}]",
                        BASELINES[b],
                        WINDOWS[window],
                        floor
                            .floor
                            .iter()
                            .map(|cell| cell.name())
                            .collect::<Vec<_>>()
                            .join(", "),
                        floor
                            .pooled
                            .iter()
                            .map(|bin| BIN_NAMES[*bin])
                            .collect::<Vec<_>>()
                            .join(", "),
                        floor
                            .unpowered
                            .iter()
                            .map(|bin| BIN_NAMES[*bin])
                            .collect::<Vec<_>>()
                            .join(", ")
                    );
                }
            }
            Err((window, label)) => println!(
                "R1 MC B={} {}: {}/neither is not servable",
                BASELINES[b], WINDOWS[window], LABELS[label]
            ),
        }
        if chosen.is_some() {
            println!(
                "R1 verdict at B={}: {}",
                BASELINES[b],
                match r1(&cells[0], &cells[1]) {
                    R1::Fixed { q, bottom } => format!(
                        "PASS, q_hi = {}{}",
                        q_label(q),
                        if bottom {
                            " (the floor binds at the bottom of the grid)"
                        } else {
                            ""
                        }
                    ),
                    R1::NeitherNotServable { window, label } => format!(
                        "STOP, {} {}/neither is not servable",
                        WINDOWS[window], LABELS[label]
                    ),
                    R1::NoGridValuePasses =>
                        "STOP, no grid value passes every floor cell on the first half".to_owned(),
                    R1::SecondHalfMisses { q, window, cell } => format!(
                        "STOP, the second half misses {} {} at q_hi = {}",
                        WINDOWS[window],
                        cell.name(),
                        q_label(q)
                    ),
                }
            );
        }
    }
}

fn report_exhaustion(flow: &FlowModel) {
    for (b, baseline) in BASELINES.iter().enumerate() {
        for half in 0..2 {
            println!(
                "exhaustion B={} {}: {}",
                baseline,
                half_label(Some(half)),
                (0..Q)
                    .map(|q| format!("q={} {}", q_label(q), flow.exhausted[b][half][q]))
                    .collect::<Vec<_>>()
                    .join(", ")
            );
        }
    }
}

#[test]
#[ignore = "measures the archive for ADR-051; see the module docs"]
fn measure_candidates() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let from = parse_bound("MIE_MEASURE_FROM", &env("MIE_MEASURE_FROM"), false).unwrap();
    let to = parse_bound("MIE_MEASURE_TO", &env("MIE_MEASURE_TO"), true).unwrap();
    let expected_events: Option<u64> = std::env::var("MIE_MEASURE_EVENTS")
        .ok()
        .map(|events| events.parse().expect("MIE_MEASURE_EVENTS is a count"));
    let window = ReplayWindow {
        start: EventTime::from_millis(from),
        end: EventTime::from_millis(to),
    };
    let store = ParquetRawStore::new(&raw_root);
    // aggTrades with the ADR-050 fold (its D8 default), and metrics for the
    // 5-minute open interest (ADR-042 D2).
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::AggTrades, ArchiveStream::Metrics],
        TradeCorrection::Fold,
    );
    let mut stream = replay.replay(window).unwrap().stream;
    let started = Instant::now();
    let mut run = Run::new(from + (to - from) / 2);
    while let Some(event) = stream.next_event().unwrap() {
        run.step(&event);
    }
    report(&run, from, to, started.elapsed());
    if let Some(expected) = expected_events {
        println!("R0: events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    assert_eq!(run.rejections, 0, "the engine rejected events");
    assert_eq!(
        run.check.mismatches, 0,
        "the model disagrees with the engine"
    );
}

#[test]
#[ignore = "checks the candidates against a live capture for ADR-051; see the module docs"]
fn measure_candidates_live() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let journal = std::path::PathBuf::from(env("MIE_MEASURE_JOURNAL"));
    let runs: Vec<JournaledRun> = read_runs(&journal, "binance-um").unwrap();
    let store = ParquetRawStore::new(&raw_root);
    let live = LiveReplay::new(
        &store,
        "binance-um",
        "BTCUSDT",
        runs.iter().map(JournaledRun::live_run).collect(),
    );
    let (mut mismatches, mut rejections, mut absorbed) = (0, 0, 0);
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        // One run, no halves: every count lands in the first.
        let mut run = Run::new(i64::MAX);
        let started = Instant::now();
        while let Some(event) = replay.next_event().unwrap() {
            run.step(&event);
        }
        println!(
            "run {id}: events {}, rejections {}, wall {:.1} s",
            run.events,
            run.rejections,
            started.elapsed().as_secs_f64()
        );
        report_r0(&run.check);
        let engine = &run.flow.engine;
        println!(
            "run {id}: absorption 5m/15m/1h {:?} of evaluated minutes {:?}; effort_result \
             5m/15m/1h {:?}; exhaustion {}",
            engine.absorption_edges[0],
            engine.absorption_evaluations[0],
            engine.effort_edges[0],
            engine.exhaustion_edges[0]
        );
        mismatches += run.check.mismatches;
        rejections += run.rejections;
        absorbed += engine.absorption_evaluations[0].iter().sum::<u64>();
    }
    println!(
        "live R0: mismatches {mismatches}, rejections {rejections}, evaluated absorption \
         minutes {absorbed}"
    );
    assert_eq!(rejections, 0, "the engine rejected events");
    assert_eq!(mismatches, 0, "the model disagrees with the engine");
}

#[test]
#[ignore = "re-reads a logged run's R1b tables for the ADR-051 D9 diagnosis; see the module docs"]
fn r1b_from_a_logged_run() {
    let path = env("MIE_MEASURE_LOG");
    let text = std::fs::read_to_string(&path).unwrap_or_else(|error| panic!("{path}: {error}"));
    let (tables, lines) = parse_r1b_log(&text);
    assert_eq!(lines, NB * 2 * W * Q, "the log holds every R1b hour line");
    println!("{R1B_STATISTIC}");
    println!("R1b: tables read from a logged run, {lines} hour lines");
    report_r1b_tables(&tables);
}

/// A seeded generator, so the tape needs no `rand`.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u64) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        i64::try_from((self.0 >> 11) % bound).unwrap()
    }
}

/// 2026-01-01 00:00 UTC.
const TAPE_START: i64 = 1_767_225_600_000;

/// Days on the synthetic tape.
const TAPE_DAYS: i64 = 30;

/// The open-interest samples of the tape, in time order: two polls around
/// every 5-minute boundary, `r` seconds before it and `10 − r` after it
/// (`r` ∈ 1…9), as a 10 s live poll lands. The poll after a boundary closes
/// it, so in the seconds before it the newest ready point is just over one
/// interval old: anchors inside decision 4's 60 s slack. No samples for
/// six hours of day 12: anchors there have no current point.
fn open_interest_samples(lcg: &mut Lcg, end: i64) -> VecDeque<OpenInterest> {
    let mut samples = VecDeque::new();
    let hole = (TAPE_START + 12 * DAY_MS + HOUR_MS)..(TAPE_START + 12 * DAY_MS + 7 * HOUR_MS);
    let mut oi: i64 = 80_000 * SCALE;
    let mut boundary = TAPE_START + 300_000;
    while boundary < end {
        oi += (lcg.below(2_001) - 1_000) * 10_000_000;
        let before = 1_000 * (1 + lcg.below(9));
        if !hole.contains(&boundary) {
            for (time, units) in [
                (boundary - before, oi),
                (
                    boundary + 10_000 - before,
                    oi + lcg.below(1_001) * 1_000_000,
                ),
            ] {
                samples.push_back(OpenInterest {
                    time: EventTime::from_millis(time),
                    open_interest: Qty::from_units(units),
                    resolution_ms: 10_000,
                });
            }
        }
        boundary += 300_000;
    }
    samples
}

/// Moves every sample before `time` onto `events`.
fn flush(samples: &mut VecDeque<OpenInterest>, time: i64, events: &mut Vec<MarketEvent>) {
    while samples
        .front()
        .is_some_and(|sample| sample.time.as_millis() < time)
    {
        events.push(MarketEvent::OpenInterest(
            samples.pop_front().expect("a sample"),
        ));
    }
}

/// About 30 days of trades around 62 000 USDT, 5 s to 40 s apart: a random
/// walk in cents with drifting trends and heavy-tailed sizes, so swings,
/// divergences and episodes of every kind occur; the open interest of
/// [`open_interest_samples`]; and a trades gap on day 9.
fn synthetic_tape() -> Vec<MarketEvent> {
    let mut lcg = Lcg(0x6d69_6500_0022_0002);
    let end = TAPE_START + TAPE_DAYS * DAY_MS;
    let mut samples = open_interest_samples(&mut lcg, end);
    let mut events = Vec::new();
    let mut time = TAPE_START + 123_456;
    let mut cents: i64 = 62_000 * 100;
    let mut trend = 0;
    let mut trade_id = 0;
    let gap_start = TAPE_START + 9 * DAY_MS + 10 * HOUR_MS;
    let gap_end = gap_start + 45 * MINUTE_MS;
    let mut gapped = false;
    while time < end {
        time += 5_000 + lcg.below(35_000);
        if !gapped && time >= gap_start {
            gapped = true;
            flush(&mut samples, gap_end, &mut events);
            events.push(MarketEvent::FeedGap(FeedGap {
                stream: Stream::Trades,
                start: EventTime::from_millis(gap_start),
                end: EventTime::from_millis(gap_end),
                reason: GapReason::Disconnected,
            }));
            time = gap_end + 1 + lcg.below(30_000);
        }
        flush(&mut samples, time, &mut events);
        // A sample and a trade never share a millisecond.
        if samples
            .front()
            .is_some_and(|sample| sample.time.as_millis() == time)
        {
            time += 1;
            flush(&mut samples, time, &mut events);
        }
        if lcg.below(600) == 0 {
            trend = lcg.below(9) - 4;
        }
        cents += trend + lcg.below(601) - 300;
        let aggressor = if lcg.below(2) == 0 {
            Aggressor::Buy
        } else {
            Aggressor::Sell
        };
        trade_id += 1;
        events.push(MarketEvent::Trade(Trade {
            time: EventTime::from_millis(time),
            trade_id,
            price: Price::from_units(cents * (SCALE / 100)),
            // Heavy-tailed sizes, so some windows' aggression stands out.
            qty: Qty::from_units(
                (1 + lcg.below(100_000_000)) * if lcg.below(50) == 0 { 40 } else { 1 },
            ),
            aggressor,
        }));
    }
    events
}

/// About 32 hours from 2026-01-01 00:00 UTC of a book 0.1 USDT wide around
/// 62 000 USDT, twelve levels a side 3 USDT apart (the trusted window
/// reaches beyond the 5 bps band), and up to five trades a minute that lift
/// the best ask or hit the best bid. One book update a minute, at second
/// 50, shows each best level's fills and a refill of 0 to 2 times them,
/// in halves; a level below 20 BTC is topped up to 40.
fn book_tape() -> Vec<MarketEvent> {
    let mut lcg = Lcg(0x6d69_6500_0022_0005);
    let minutes = 32 * 60;
    let bid = 62_000 * SCALE;
    let ask = bid + SCALE / 10;
    let step = 3 * SCALE;
    let level = |price: i64, qty: i64| Level {
        price: Price::from_units(price),
        qty: Qty::from_units(qty),
    };
    let levels = |best: i64, sign: i64| -> Vec<Level> {
        (0..12)
            .map(|k| level(best + sign * k * step, 40 * SCALE))
            .collect()
    };
    let mut last_id = 5_000;
    let mut events = vec![MarketEvent::BookSnapshot(BookSnapshot {
        time: EventTime::from_millis(TAPE_START),
        last_update_id: last_id,
        bids: levels(bid, -1),
        asks: levels(ask, 1),
    })];
    let mut resting = [40 * SCALE; 2];
    let mut trade_id = 0;
    for minute in 0..minutes {
        let open = TAPE_START + minute * MINUTE_MS;
        let mut filled = [0; 2];
        for slot in 0..1 + lcg.below(5) {
            let buy = lcg.below(2) == 0;
            let side = usize::from(buy);
            let mut size = (1 + lcg.below(200)) * SCALE / 100;
            if lcg.below(8) == 0 {
                size *= 8;
            }
            let size = size.min(resting[side] - filled[side] - SCALE);
            if size <= 0 {
                continue;
            }
            filled[side] += size;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: EventTime::from_millis(open + 1_000 + slot * 9_000 + lcg.below(8_000)),
                trade_id,
                price: Price::from_units(if buy { ask } else { bid }),
                qty: Qty::from_units(size),
                aggressor: if buy { Aggressor::Buy } else { Aggressor::Sell },
            }));
        }
        let mut changes = [Vec::new(), Vec::new()];
        for (side, price) in [bid, ask].into_iter().enumerate() {
            let refill = filled[side] * lcg.below(5) / 2;
            let mut next = resting[side] - filled[side] + refill;
            if next < 20 * SCALE {
                next = 40 * SCALE;
            }
            if next != resting[side] {
                changes[side].push(level(price, next));
            }
            resting[side] = next;
        }
        let (first, previous) = if minute == 0 {
            (last_id - 5, 0)
        } else {
            (last_id + 1, last_id)
        };
        last_id += 5;
        let [bids, asks] = changes;
        events.push(MarketEvent::BookUpdate(BookUpdate {
            time: EventTime::from_millis(open + 50_000),
            first_update_id: first,
            last_update_id: last_id,
            prev_update_id: previous,
            bids,
            asks,
        }));
    }
    events.push(MarketEvent::Trade(Trade {
        time: EventTime::from_millis(TAPE_START + minutes * MINUTE_MS + 1_000),
        trade_id: trade_id + 1,
        price: Price::from_units(bid),
        qty: Qty::from_units(SCALE),
        aggressor: Aggressor::Sell,
    }));
    events.sort();
    events
}

#[test]
fn model_reproduces_absorption_on_a_synthetic_book_tape() {
    let mut run = Run::new(i64::MAX);
    for event in book_tape() {
        run.step(&event);
    }
    report_r0(&run.check);
    report_engine_rates(&run.flow);
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(run.check.mismatches, 0, "{:#?}", run.check.notes);
    // Every window's absorption was compared, so R0 did not pass vacuously.
    let engine = &run.flow.engine;
    for window in 0..W {
        assert!(run.check.absorption[window] > 0, "absorption {window}");
        assert!(
            engine.absorption_evaluations[0][window] > 0,
            "evaluated {window}"
        );
    }
}

#[test]
fn the_engine_parameters_are_the_fixed_ones() {
    // ADR-051 D9 item 11: B = 1 440 and q_hi = 0.800, which R0 compares at.
    use mie_domain::feature::ParamValue;
    use mie_domain::num::Rate;
    assert_eq!(BASELINES[ENGINE_B], 1_440);
    assert_eq!(GRID[ENGINE_Q], 800);
    for (_, definition) in catalog::CANDIDATE_EFFORT_RESULT {
        let param = |name: &str| {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        assert_eq!(param("baseline_evaluations"), Some(ParamValue::Int(1_440)));
        assert_eq!(
            param("effort_quantile"),
            Some(ParamValue::Rate(Rate::from_units(80_000_000)))
        );
    }
    assert_eq!(catalog::ABSORPTION_BAND.units(), ABSORPTION_BAND_UNITS);
}

#[test]
fn model_reproduces_the_engine_on_a_synthetic_tape() {
    let mid = TAPE_START + TAPE_DAYS * DAY_MS / 2;
    let mut run = Run::new(mid);
    for event in synthetic_tape() {
        run.step(&event);
    }
    // The report runs too, so the archive run cannot fail on it.
    report(
        &run,
        TAPE_START,
        TAPE_START + TAPE_DAYS * DAY_MS,
        Duration::default(),
    );
    let check = &run.check;
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(check.mismatches, 0, "{:#?}", check.notes);
    // Every compared kind occurred, so R0 did not pass vacuously.
    let missing_oi: u64 = run.divergences.iter().map(|model| model.missing_oi).sum();
    let plain: u64 = run.divergences.iter().map(|model| model.plain).sum();
    let flow = &run.flow;
    let total = |table: &[[CellTable; 2]; NB], bin: usize| -> u64 {
        table
            .iter()
            .flatten()
            .flatten()
            .flatten()
            .flatten()
            .map(|labels| labels[bin])
            .sum()
    };
    let grid_total =
        |table: &[[GridCounts; 2]; NB]| -> u64 { table.iter().flatten().flatten().flatten().sum() };
    let reference_hours: u64 = flow.hours[REFERENCE]
        .iter()
        .flatten()
        .flatten()
        .flatten()
        .sum();
    let kinds = [
        ("CVD divergences at highs", check.cvd[0]),
        ("CVD divergences at lows", check.cvd[1]),
        ("OI divergences at highs", check.oi[0]),
        ("OI divergences at lows", check.oi[1]),
        ("divergences across a trades gap", check.gapped),
        ("contexts listing a level", check.located),
        ("ready divergence values", check.values),
        ("comparisons without an OI point", missing_oi),
        ("comparisons divergent by neither rule", plain),
        ("value_edge episodes", total(&flow.cells, VALUE_EDGE)),
        (
            "structural_extreme episodes",
            total(&flow.cells, STRUCTURAL_EXTREME),
        ),
        ("neither episodes", total(&flow.cells, NEITHER)),
        ("unlabelled episodes", grid_total(&flow.unlabelled)),
        ("episodes while the reference is ready", reference_hours),
        (
            "exhaustion episodes",
            flow.exhausted.iter().flatten().flatten().sum(),
        ),
        ("engine effort_result 5m facts", check.effort[0]),
        ("engine effort_result 15m facts", check.effort[1]),
        ("engine effort_result 1h facts", check.effort[2]),
        ("engine exhaustion facts", check.exhaustion),
        (
            "ready effort_result and exhaustion values",
            check.flow_values,
        ),
    ];
    for (kind, count) in kinds {
        assert!(count > 0, "the tape compared no {kind}");
    }
}

// ---------------------------------------------------------------------------
// The fixing rules on constructed count tables
// ---------------------------------------------------------------------------

/// A reference profile with a strong hour-of-day cycle.
fn profile() -> [u64; HOURS] {
    std::array::from_fn(|h| 200 + 150 * (h as u64 % 6))
}

/// An hour table holding `counts` in every combination.
fn hour_table(counts: [u64; HOURS]) -> HourTable {
    [[counts; Q]; W]
}

/// Hour tables per baseline and half: the reference holds [`profile`] and
/// each candidate the table `candidates` gives it.
fn hour_tables(candidates: [[HourTable; 2]; REFERENCE]) -> [[HourTable; 2]; NB] {
    let reference = hour_table(profile());
    [
        candidates[0],
        candidates[1],
        candidates[2],
        [reference, reference],
    ]
}

/// Hour tables in which `B` ranks like the reference everywhere except
/// window `window` and grid value `q`, which hold `n_b` and `n_ref`.
fn one_combination(
    window: usize,
    q: usize,
    n_b: [u64; HOURS],
    n_ref: [u64; HOURS],
) -> (HourTable, HourTable) {
    let mut b = hour_table(profile());
    let mut reference = hour_table(profile());
    b[window][q] = n_b;
    reference[window][q] = n_ref;
    (b, reference)
}

/// An hour table whose window `window` holds one episode per hour at every
/// `q`: against [`profile`], every expected count of `B` is below 1.
fn sparse_window(mut table: HourTable, window: usize) -> HourTable {
    table[window].fill([1; HOURS]);
    table
}

#[test]
fn critical_pins_its_two_ends() {
    assert_eq!(format!("{:.3}", critical(1)), "35.172");
    assert_eq!(format!("{:.3}", critical(18)), "46.258");
}

/// A reference with 4× the base count in 13–16 UTC (#124's concentration).
fn concentrated() -> [u64; HOURS] {
    std::array::from_fn(|h| if (13..=16).contains(&h) { 400 } else { 100 })
}

#[test]
fn a_concentration_both_baselines_share_is_no_failure() {
    let reference = concentrated();
    let combination = combination(&reference.map(|n| 2 * n), &reference);
    assert!(combination.evaluable);
    assert_eq!(format!("{:.3}", combination.x2), "0.000");
    assert!(combination.x2 <= critical(18));
    let b = hour_table(reference.map(|n| 2 * n));
    let reference = hour_table(reference);
    let tables = [[b, b], [b, b], [b, b], [reference, reference]];
    assert!(half_test(&b, &reference).pass);
    assert_eq!(r1b(&tables), R1b::Fixed { candidate: 0 });
}

#[test]
fn a_concentration_only_b_has_fails() {
    let reference = concentrated();
    // The reference profile, with 13–16 UTC a further 1.5× higher.
    let b: [u64; HOURS] = std::array::from_fn(|h| if (13..=16).contains(&h) { 600 } else { 100 });
    let combination = combination(&b, &reference);
    assert!(combination.evaluable);
    assert!(combination.x2 > critical(18), "X² {}", combination.x2);
    let test = half_test(&hour_table(b), &hour_table(reference));
    assert_eq!(test.k, W * Q);
    assert!(test.fails);
}

#[test]
fn flat_raw_rates_against_a_concentrated_reference_fail() {
    // B = 60's behaviour: every hour ranked against itself.
    let combination = combination(&[160; HOURS], &concentrated());
    assert!(combination.evaluable);
    assert!(combination.x2 > critical(18), "X² {}", combination.x2);
}

// Three development-year tables (#22 M2, second half), diagnosis pins only
// (ADR-051 D9): `B` first, then the reference.

/// B = 1440, 1h, q = 0.950: M2's failing combination.
const M2_1440_1H_950: ([u64; HOURS], [u64; HOURS]) = (
    [
        5, 19, 8, 4, 13, 6, 12, 9, 9, 7, 5, 21, 6, 23, 29, 30, 39, 13, 13, 16, 8, 12, 8, 12,
    ],
    [
        3, 14, 8, 3, 3, 6, 12, 1, 4, 8, 5, 24, 8, 20, 24, 27, 19, 11, 12, 11, 11, 8, 13, 2,
    ],
);

/// B = 240, 15m, q = 0.975.
const M2_240_15M_975: ([u64; HOURS], [u64; HOURS]) = (
    [
        27, 16, 21, 20, 14, 21, 29, 24, 15, 12, 30, 13, 31, 41, 29, 15, 11, 6, 8, 10, 23, 22, 15, 8,
    ],
    [
        4, 5, 6, 5, 2, 8, 9, 3, 12, 8, 11, 9, 11, 32, 32, 14, 15, 7, 6, 3, 7, 7, 10, 6,
    ],
);

/// B = 1440, 1h, q = 0.975.
const M2_1440_1H_975: ([u64; HOURS], [u64; HOURS]) = (
    [
        3, 9, 2, 1, 6, 2, 0, 5, 4, 4, 2, 11, 2, 13, 16, 24, 11, 0, 1, 5, 1, 7, 2, 0,
    ],
    [
        1, 4, 0, 1, 0, 2, 5, 1, 2, 0, 1, 17, 1, 10, 12, 14, 5, 1, 1, 2, 8, 3, 7, 2,
    ],
);

#[test]
fn m2_failing_combination_is_evaluable_and_passes() {
    let (n_b, n_ref) = M2_1440_1H_950;
    let combination = combination(&n_b, &n_ref);
    assert_eq!(format!("{:.3}", combination.x2), "26.998");
    assert_eq!(format!("{:.3}", combination.p), "0.256");
    assert_eq!((combination.below_five, combination.below_one), (6, 0));
    assert!(combination.evaluable);
    assert_eq!(format!("{:.3}", critical(14)), "45.378");
    assert!(combination.x2 <= critical(14));
}

#[test]
fn m2_sparse_hour_ranking_baseline_fails() {
    let (n_b, n_ref) = M2_240_15M_975;
    let combination = combination(&n_b, &n_ref);
    assert_eq!(format!("{:.3}", combination.x2), "56.107");
    assert_eq!((combination.below_five, combination.below_one), (4, 0));
    assert!(combination.evaluable);
    assert!(combination.x2 > critical(14));
}

#[test]
fn m2_sparsest_combination_is_not_evaluable_and_not_counted() {
    let (n_b, n_ref) = M2_1440_1H_975;
    let combination = combination(&n_b, &n_ref);
    assert_eq!((combination.below_five, combination.below_one), (33, 6));
    assert!(!combination.evaluable);
    let (b, reference) = one_combination(2, 4, n_b, n_ref);
    let test = half_test(&b, &reference);
    assert_eq!(test.k, W * Q - 1);
    assert!(test.pass);
}

/// A table split 1 : 4 between `B` and the reference in every hour, so
/// X² = 0: `light` hours of 10 episodes (expected 2 and 8), one hour of 25
/// (expected exactly 5 and 20), the others of 50.
fn split_one_to_four(light: usize) -> ([u64; HOURS], [u64; HOURS]) {
    let m = |h: usize| -> u64 {
        if h < light {
            10
        } else if h == light {
            25
        } else {
            50
        }
    };
    (
        std::array::from_fn(|h| m(h) / 5),
        std::array::from_fn(|h| 4 * m(h) / 5),
    )
}

#[test]
fn at_most_nine_of_forty_eight_expected_counts_below_five() {
    let (n_b, n_ref) = split_one_to_four(9);
    let nine = combination(&n_b, &n_ref);
    // An expected count of exactly 5 is not below 5.
    assert_eq!((nine.below_five, nine.below_one), (9, 0));
    assert!(nine.evaluable);
    let (n_b, n_ref) = split_one_to_four(10);
    let ten = combination(&n_b, &n_ref);
    assert_eq!((ten.below_five, ten.below_one), (10, 0));
    assert!(!ten.evaluable);
}

#[test]
fn an_expected_count_of_exactly_one_is_evaluable() {
    // Hour 0 splits 1 : 4 at 5 episodes, the others at 50: N_B = 231,
    // N = 1155 and m(0)·N_B = 5 · 231 = N.
    let mut n_b = [10; HOURS];
    let mut n_ref = [40; HOURS];
    n_b[0] = 1;
    n_ref[0] = 4;
    let one = combination(&n_b, &n_ref);
    assert_eq!((one.below_five, one.below_one), (2, 0));
    assert!(one.evaluable);
    // One more reference episode elsewhere: m(0)·N_B = N − 1.
    n_ref[1] += 1;
    let below = combination(&n_b, &n_ref);
    assert_eq!(below.below_one, 1);
    assert!(!below.evaluable);
}

#[test]
fn a_baseline_without_episodes_is_not_evaluable() {
    let empty = combination(&[0; HOURS], &profile());
    assert!(!empty.evaluable);
    assert!(empty.x2.is_nan());
    assert!(!combination(&profile(), &[0; HOURS]).evaluable);
    assert!(!combination(&[0; HOURS], &[0; HOURS]).evaluable);
}

#[test]
fn an_hour_without_episodes_of_either_is_not_evaluable() {
    let mut n_b = profile();
    let mut n_ref = profile();
    n_b[3] = 0;
    n_ref[3] = 0;
    let combination = combination(&n_b, &n_ref);
    assert_eq!(combination.below_one, 2);
    assert!(!combination.evaluable);
}

#[test]
fn an_empty_hour_of_b_beside_a_large_reference_is_evaluable_and_fails() {
    let mut n_b = profile();
    n_b[7] = 0;
    let combination = combination(&n_b, &profile());
    assert!(combination.evaluable);
    assert!(combination.x2 > critical(18));
}

#[test]
fn logged_r1b_lines_parse_in_both_formats() {
    let (n_b, n_ref) = M2_1440_1H_950;
    // M2's format (#22), with lines the parser skips.
    let m2 = format!(
        "R1b: evaluations per window [525590, 525582, 525539]; episodes counted while \
         the reference B=10080 is ready\n\
         R1b n_ref B=10080 second 1h q=0.950: [{}]\n\
         R1b B=1440 second 1h q=0.950: n_B [{}] U_B 1.0472 σ0 0.5374 c_k 1.4046 (k=14) FAIL\n\
         R1b B=1440 second: k 14 -> FAIL\n\
         R1b verdict: STOP, B = 1440 fails on the second half\n",
        hours_line(&n_ref),
        hours_line(&n_b)
    );
    // This format, from the report's own line builders.
    let (b, reference) = one_combination(2, 3, n_b, n_ref);
    let test = half_test(&b, &reference);
    let new = format!(
        "{R1B_STATISTIC}\n{}\n{}\n",
        reference_line(1, 2, 3, &n_ref),
        combination_line(2, 1, 2, 3, &n_b, &test)
    );
    let (from_m2, lines_m2) = parse_r1b_log(&m2);
    let (from_new, lines_new) = parse_r1b_log(&new);
    assert_eq!((lines_m2, lines_new), (2, 2));
    assert_eq!(from_m2, from_new);
    assert_eq!(from_new[2][1][2][3], n_b);
    assert_eq!(from_new[REFERENCE][1][2][3], n_ref);
    assert_eq!(
        parse_r1b_line(&combination_line(0, 0, 1, 5, &n_b, &test)),
        Some((0, 0, 1, 5, n_b))
    );
}

#[test]
fn r1b_picks_the_smallest_passing_baseline() {
    let like = hour_table(profile());
    // Flat raw rates against a cycling reference: a non-flat split.
    let flat = hour_table([500; HOURS]);
    let tables = hour_tables([[flat, flat], [like, like], [like, like]]);
    assert!(!half_test(&flat, &tables[REFERENCE][0]).pass);
    assert_eq!(r1b(&tables), R1b::Fixed { candidate: 1 });
    let tables = hour_tables([[like, like], [like, like], [flat, flat]]);
    assert_eq!(r1b(&tables), R1b::Fixed { candidate: 0 });
}

#[test]
fn r1b_ignores_combinations_that_are_not_evaluable() {
    let like = hour_table(profile());
    let mut partly = like;
    // One combination has 3 episodes, all in one hour: far from the
    // reference's split, but every expected count of `B` is below 1. The
    // window keeps other evaluable combinations.
    partly[1][3] = [0; HOURS];
    partly[1][3][5] = 3;
    let test = half_test(&partly, &like);
    assert!(!test.combinations[1][3].evaluable);
    assert_eq!(test.k, W * Q - 1);
    assert!(test.pass);
    let tables = hour_tables([[partly, like], [like, like], [like, like]]);
    assert_eq!(r1b(&tables), R1b::Fixed { candidate: 0 });
}

#[test]
fn r1b_stops_when_no_candidate_passes() {
    let flat = hour_table([500; HOURS]);
    let tables = hour_tables([[flat, flat], [flat, flat], [flat, flat]]);
    assert_eq!(r1b(&tables), R1b::NoCandidatePasses);
}

#[test]
fn r1b_stops_on_a_window_without_an_evaluable_combination() {
    let like = hour_table(profile());
    let sparse = sparse_window(like, 2);
    // No evaluable combination fails, so the untestable window stops R1b.
    assert!(!half_test(&sparse, &like).fails);
    let tables = hour_tables([[sparse, like], [like, like], [like, like]]);
    assert_eq!(
        r1b(&tables),
        R1b::NotEvaluable {
            candidate: 0,
            half: 0,
            window: 2
        }
    );
}

#[test]
fn r1b_skips_a_failing_baseline_with_a_window_without_an_evaluable_combination() {
    let like = hour_table(profile());
    // B = 60 fails all 12 evaluable combinations (5m and 15m) on the first
    // half, and 1h is too sparse to evaluate at every q.
    let failing = sparse_window(hour_table([500; HOURS]), 2);
    let test = half_test(&failing, &like);
    assert_eq!(test.k, 2 * Q);
    assert!(test.fails);
    assert_eq!(test.unevaluable_window, Some(2));
    // The failure decides, so R1b goes on to B = 240, which passes both halves.
    let tables = hour_tables([[failing, like], [like, like], [like, like]]);
    assert_eq!(r1b(&tables), R1b::Fixed { candidate: 1 });
}

#[test]
fn r1b_stops_on_a_second_half_failure_before_an_unevaluable_window() {
    let like = hour_table(profile());
    let failing = sparse_window(hour_table([500; HOURS]), 2);
    let tables = hour_tables([[like, failing], [like, like], [like, like]]);
    assert_eq!(r1b(&tables), R1b::SecondHalfFails { candidate: 0 });
}

#[test]
fn r1b_stops_when_the_chosen_baseline_fails_the_second_half() {
    let like = hour_table(profile());
    let flat = hour_table([500; HOURS]);
    let tables = hour_tables([[flat, flat], [like, flat], [like, like]]);
    assert_eq!(r1b(&tables), R1b::SecondHalfFails { candidate: 1 });
}

/// A cell table with `count` in every cell at every grid value.
fn cell_table(count: u64) -> CellTable {
    [[[[count; BINS]; NL]; Q]; W]
}

/// Sets one cell of every window length to `counts` per grid value.
fn set_cell(table: &mut CellTable, label: usize, bin: usize, counts: [u64; Q]) {
    for window in table.iter_mut() {
        for (q, labels) in window.iter_mut().enumerate() {
            labels[label][bin] = counts[q];
        }
    }
}

#[test]
fn r1_passes_with_every_cell_servable() {
    let mut first = cell_table(200);
    // Every cell thins out at the top two grid values.
    for label in 0..NL {
        for bin in 0..BINS {
            set_cell(&mut first, label, bin, [300, 250, 200, 150, 50, 20]);
        }
    }
    let floors = classify(&first).unwrap();
    assert!(floors.iter().all(|floor| floor.floor.len() == NL * BINS));
    assert_eq!(
        r1(&first, &cell_table(200)),
        R1::Fixed {
            q: 3,
            bottom: false
        }
    );
}

#[test]
fn r1_pools_a_rare_location_cell_over_the_labels() {
    let mut first = cell_table(200);
    set_cell(&mut first, 2, VALUE_EDGE, [30; Q]);
    let floors = classify(&first).unwrap();
    assert!(floors.iter().all(|floor| floor.pooled == vec![VALUE_EDGE]
        && floor.floor.contains(&FloorCell::Pooled { bin: VALUE_EDGE })
        && !floor.floor.contains(&FloorCell::Cell {
            label: 2,
            bin: VALUE_EDGE
        })));
    assert_eq!(
        r1(&first, &cell_table(200)),
        R1::Fixed {
            q: Q - 1,
            bottom: false
        }
    );
}

#[test]
fn r1_leaves_an_unpowered_bin_out_of_the_floor() {
    let mut first = cell_table(200);
    for label in 0..NL {
        set_cell(&mut first, label, STRUCTURAL_EXTREME, [10; Q]);
    }
    let floors = classify(&first).unwrap();
    assert!(
        floors
            .iter()
            .all(|floor| floor.unpowered == vec![STRUCTURAL_EXTREME])
    );
    let mut second = cell_table(200);
    for label in 0..NL {
        set_cell(&mut second, label, STRUCTURAL_EXTREME, [0; Q]);
    }
    assert_eq!(
        r1(&first, &second),
        R1::Fixed {
            q: Q - 1,
            bottom: false
        }
    );
}

#[test]
fn r1_stops_on_a_neither_cell_that_is_not_servable() {
    let mut first = cell_table(200);
    set_cell(&mut first, 3, NEITHER, [96; Q]);
    assert_eq!(
        r1(&first, &cell_table(200)),
        R1::NeitherNotServable {
            window: 0,
            label: 3
        }
    );
}

#[test]
fn r1_reads_every_grid_value_for_servability() {
    let mut first = cell_table(200);
    // Servable only at a middle grid value, 0.90, and another cell only
    // at 0.975: both are floor cells, and no grid value passes both.
    set_cell(&mut first, 0, VALUE_EDGE, [50, 60, 97, 40, 30, 20]);
    set_cell(&mut first, 1, NEITHER, [10, 10, 10, 10, 97, 10]);
    let floors = classify(&first).unwrap();
    assert!(floors.iter().all(|floor| {
        floor.floor.contains(&FloorCell::Cell {
            label: 0,
            bin: VALUE_EDGE,
        }) && floor.pooled.is_empty()
    }));
    assert_eq!(r1(&first, &cell_table(200)), R1::NoGridValuePasses);
}

#[test]
fn r1_stops_when_the_second_half_misses_a_frozen_floor_cell() {
    let first = cell_table(200);
    let mut second = cell_table(200);
    set_cell(&mut second, 1, STRUCTURAL_EXTREME, [96; Q]);
    assert_eq!(
        r1(&first, &second),
        R1::SecondHalfMisses {
            q: Q - 1,
            window: 0,
            cell: FloorCell::Cell {
                label: 1,
                bin: STRUCTURAL_EXTREME
            }
        }
    );
}

#[test]
fn r1_flags_a_floor_that_binds_at_the_bottom_of_the_grid() {
    let mut first = cell_table(200);
    set_cell(&mut first, 0, NEITHER, [97, 96, 90, 80, 70, 60]);
    assert_eq!(
        r1(&first, &cell_table(200)),
        R1::Fixed { q: 0, bottom: true }
    );
}

#[test]
fn an_episode_counts_in_both_bins_and_excluded_ones_in_none() {
    let mut cells = [[0; BINS]; NL];
    let (mut unlabelled, mut unlocated) = (0, 0);
    let both = bins_of([LevelKind::Val, LevelKind::Poc, LevelKind::StructuralHigh]);
    assert_eq!(both, [true, true, false]);
    assert_eq!(
        bins_of([LevelKind::PriorSweep, LevelKind::Hvn]),
        [false, false, true]
    );
    count(
        &mut cells,
        &mut unlabelled,
        &mut unlocated,
        Place {
            label: Some(1),
            bins: Some(both),
        },
    );
    assert_eq!(cells[1], [1, 1, 0]);
    count(
        &mut cells,
        &mut unlabelled,
        &mut unlocated,
        Place {
            label: None,
            bins: Some(both),
        },
    );
    count(
        &mut cells,
        &mut unlabelled,
        &mut unlocated,
        Place {
            label: Some(0),
            bins: None,
        },
    );
    assert_eq!((unlabelled, unlocated), (1, 1));
    assert_eq!(cells.iter().flatten().sum::<u64>(), 2);
}

#[test]
fn baseline_quantiles_are_nearest_rank_over_the_previous_values() {
    let mut baseline = Baseline::new(4);
    for value in [40, 10, 30, 20] {
        baseline.push(value);
    }
    assert!(baseline.ready());
    // ⌈0.5 · 4⌉ = 2nd smallest; ⌈0.8 · 4⌉ = 4th.
    assert_eq!(baseline.quantile(MEDIAN), 20);
    assert_eq!(baseline.quantile(800), 40);
    // The oldest value (40) leaves.
    baseline.push(5);
    assert_eq!(baseline.sorted, vec![5, 10, 20, 30]);
    assert_eq!(baseline.quantile(MEDIAN), 10);
}
