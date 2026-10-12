//! The ADR-037 measurements (issues #80 and #104): an independent model of
//! the ADR-037 decision text, run next to one `MarketStateEngine`, with the
//! swing size `N`, the SFP window `K`, the touch tolerance, the active-list
//! rule (a count cap `max_active`, a distance threshold `max_distance_bps`,
//! or none: `until_swept`), the resolved-list cap `max_resolved` and an
//! optional cold start as runtime parameters.
//!
//! The model is written from decisions 2–7, not from the engine's internals.
//! It reads only the engine's closed bars of every structure timeframe, plus
//! the trades themselves, which date each sweep inside its bar. A level
//! becomes active at the event that closes its confirming bar and that
//! event's trade is checked after the close (decision 8), so every trade of
//! a bar opening at or after `confirmed_bar_end` is checked against it: a
//! sweep is a bar of that kind whose high is above the level (whose low is
//! below it), and the per-bar check equals the engine's per-trade one.
//!
//! - **R0**, the run is valid: no domain rejection, and at the engine's
//!   parameters ([`SWING_BARS`], [`SFP_WINDOW_BARS`], [`TOUCH_TOLERANCE_BPS`],
//!   [`MAX_LEVELS`]) the model reproduces every engine `StructureEvent` —
//!   swing: timeframe, side, price, `swing_time`, `confirmed_bar_end`; sweep:
//!   timeframe and level identity, with the engine's sweep time inside the
//!   model's sweep bar; SFP and break: outcome, `resolved_bar_end`, `extreme`,
//!   `window_bars` — and the engine registry's highs, lows and resolved
//!   sweeps (identity and `touches`) after every event that closes a bar of
//!   the timeframe. Levels the closing event's trade swept count as still
//!   present: decision 8 applies that trade's sweeps after the bar close.
//! - **R1**, every structure fact occurs on every timeframe: swing high,
//!   swing low, sweep of a high, sweep of a low, SFP, break, a touch.
//! - **R2**, the cap loses less than the noise: with `n` the cap-20 resolved
//!   sweeps, `p` their SFP share and `lost` the sweeps an uncapped registry
//!   records beyond them (of levels cap 20 had evicted), PASS when
//!   `lost / (n + lost) <= sqrt(p (1 - p) / n)`.
//! - **R3**, the SFP window closes on a thinning tail: fewer sweeps have
//!   their first close at or inside the level in window bar 3 than in bar 2.
//! - Recorded only, per timeframe, per half and over the whole window: S1
//!   swing rates at `N` ∈ {1…5} against the random-walk null `u_N²` and the
//!   confirmation lag; S2 level life in bars at `N` ∈ {1…5}; S3 sweeps per
//!   day, the SFP share at `K` ∈ {1, 2, 3, 4, 6}, the first-reclaim bar and
//!   the sweep's position in its bar; S4 touches at 1–20 bps; S5 the uncapped
//!   list size, evictions, `lost` at caps 10, 20, 40, 80 and 160, the evicted
//!   levels' distance from the last close and the resolved list's history
//!   depth.
//! - **S5, the alternatives** (recorded only): for every cap of [`CAPS`],
//!   every distance of [`DISTANCES`] and `until_swept`, each with
//!   `max_resolved` 20, per timeframe the active evictions, `lost` against
//!   the uncapped model, the R2 verdict the rule would get (printed as
//!   "alternative", not a gate) and the active-list p99 and maximum per
//!   side. A distance rule retires an active level `L` at a closed bar with
//!   trades when `|L - close| * 10 000 > close * D`, exact in `i128`; a
//!   retirement counts as an eviction.
//! - **Premise P** (#95): no cap or distance that evicts at least one active
//!   level passes R2 on all four timeframes; printed `HOLDS` or `FAILS`
//!   with the falsifying candidates ([`premise_p`]).
//! - **S5, TTL** (recorded only): the smallest level age `A*` at which the
//!   uncapped model's sweeps of older levels fit R2's bound, with `n` and
//!   `p` of the kept sweeps ([`ttl_star`]). A TTL removes exactly the sweeps
//!   of the levels that outlive it, so no TTL model is run.
//! - **S5, growth** (recorded only): the uncapped active-list maximum per
//!   UTC calendar month of the bar's open, per timeframe and side.
//! - **S6, the live/replay skew** (recorded only): cold models of cap 20
//!   and of `until_swept`, started at 00:00 UTC on the 1st of every month
//!   after the window start. A cold model ignores the closed bars that open
//!   before its start, and the empty bars before its first bar with trades,
//!   as a fresh engine started there would. After every closed bar it
//!   accepted, it is compared with the full-window model of its rule by
//!   level identity (side, price, `swing_time`): `extra` and `missing`
//!   count the active levels only the full or only the cold model holds,
//!   `extra_swept` and `missing_swept` the same for pending and resolved
//!   sweeps. Printed: their p50, maximum and end values, and the days until
//!   all four stay 0. The record is VALID when `missing` is 0 under
//!   `until_swept`; `missing_swept` is not part of validity, because both
//!   resolved lists keep only their 20 newest sweeps and the cold list can
//!   still hold a sweep the full list has already dropped.
//!
//! The rules are evaluated over the whole window; the test asserts R0 and
//! the validity of S6 and prints R1–R3 as PASS or FAIL, so a failing rule
//! still prints everything. Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! structure_measure -- --ignored --nocapture`; `MIE_MEASURE_TO` is
//! included, as in `mie replay`. An optional `MIE_MEASURE_EVENTS` asserts
//! the replay's event count (R0).
//!
//! `model_reproduces_the_engine_on_a_synthetic_tape` runs the same R0
//! comparison in CI on a seeded tape, with a cold start, so every report
//! section runs, and asserts that every compared kind occurred, so R0
//! cannot pass vacuously. Further CI tests pin distance retirement at its
//! boundary and at 10 000 bps, check a cold cap-20 model against a fresh
//! engine and a cold `until_swept` model's subset property, and pin the
//! `A*` and premise-P helpers.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::{Period, PeriodKind, day_label};
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::replay::parse_bound;
use mie_domain::bars::{Bar, Ohlc, Timeframe};
use mie_domain::event::{Aggressor, FeedGap, GapReason, MarketEvent, Stream, Trade};
use mie_domain::feature::FeatureValue;
use mie_domain::num::{Price, Qty, SCALE};
use mie_domain::state::MarketStateEngine;
use mie_domain::structure::{
    Level, MAX_LEVELS, SFP_WINDOW_BARS, STRUCTURE_TIMEFRAMES, SWING_BARS, Side, StructureEvent,
    Sweep, SweepOutcome, TOUCH_TOLERANCE_BPS,
};
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;

/// The touch tolerances S4 compares, in bps.
const TOLERANCES: [i64; 7] = [1, 2, 3, 5, 8, 10, 20];

/// The swing sizes S1 and S2 compare.
const SWING_SIZES: [usize; 5] = [1, 2, 3, 4, 5];

/// The SFP windows S3 compares.
const WINDOWS: [u32; 5] = [1, 2, 3, 4, 6];

/// The caps S5 compares: #80's 10, 20 and 40, doubled on to the first value
/// above the year's largest uncapped list (148, 15m highs).
const CAPS: [usize; 5] = [10, 20, 40, 80, 160];

/// The distance thresholds S5 compares, in bps of the close: from below the
/// 15m p10 of the distance at which cap 20 evicted, to 10 000, at which a
/// low can never retire (ADR-037, "Recorded, not criteria").
const DISTANCES: [i64; 5] = [500, 1_000, 2_000, 5_000, 10_000];

/// The bars after a sweep S3 follows for the first close at or inside the
/// level; later reclaims count as "> 10".
const RECLAIM_BARS: u32 = 10;

/// The slots of a first-reclaim histogram: 1…10, then 11 for "> 10".
const RECLAIM_SLOTS: usize = RECLAIM_BARS as usize + 2;

const SIDES: [Side; 2] = [Side::High, Side::Low];

/// The model's parameters (ADR-037 decisions 2, 5 and 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Params {
    swing_bars: usize,
    sfp_window_bars: u32,
    touch_tolerance_bps: i64,
    /// The active lists' count cap; `None`: no cap.
    max_active: Option<usize>,
    /// The resolved list's cap; `None`: no cap.
    max_resolved: Option<usize>,
    /// The active lists' distance threshold in bps of the close; `None`: no
    /// threshold. Model only, the engine has none.
    max_distance_bps: Option<i64>,
}

impl Params {
    /// The engine's parameters, read from its public constants (R0).
    const ENGINE: Self = Self {
        swing_bars: SWING_BARS,
        sfp_window_bars: SFP_WINDOW_BARS,
        touch_tolerance_bps: TOUCH_TOLERANCE_BPS,
        max_active: Some(MAX_LEVELS),
        max_resolved: Some(MAX_LEVELS),
        max_distance_bps: None,
    };

    /// No list is capped: R2's reference.
    const UNCAPPED: Self = Self {
        max_active: None,
        max_resolved: None,
        ..Self::ENGINE
    };

    /// `until_swept` (#95 D1 and D3): an active level stays until a trade
    /// sweeps it, the resolved list keeps the engine's cap.
    const UNTIL_SWEPT: Self = Self {
        max_active: None,
        ..Self::ENGINE
    };

    /// The active-list rule, as the report names it.
    fn rule(&self) -> String {
        match (self.max_distance_bps, self.max_active) {
            (Some(bps), _) => format!("distance {bps} bps"),
            (None, Some(cap)) => format!("cap {cap}"),
            (None, None) => "until_swept".to_owned(),
        }
    }
}

fn side_index(side: Side) -> usize {
    match side {
        Side::High => 0,
        Side::Low => 1,
    }
}

fn timeframe_index(timeframe: Timeframe) -> Option<usize> {
    STRUCTURE_TIMEFRAMES.iter().position(|tf| *tf == timeframe)
}

/// Decision 6: a price strictly beyond the level.
fn beyond(side: Side, level: Price, price: Price) -> bool {
    match side {
        Side::High => price > level,
        Side::Low => price < level,
    }
}

/// Decision 7: a close at or inside the level.
fn inside(side: Side, level: Price, ohlc: &Ohlc) -> bool {
    match side {
        Side::High => ohlc.close <= level,
        Side::Low => ohlc.close >= level,
    }
}

/// Decision 5: the bar's extreme at or inside the level and within `bps` of
/// it, exact in `i128`.
fn within(side: Side, level: Price, ohlc: &Ohlc, bps: i64) -> bool {
    let level = i128::from(level.units());
    let distance = match side {
        Side::High => level - i128::from(ohlc.high.units()),
        Side::Low => i128::from(ohlc.low.units()) - level,
    };
    distance >= 0 && distance * 10_000 <= level.abs() * i128::from(bps)
}

/// The running extremes of the developing bar of one timeframe: every trade
/// that set a new high, and every one that set a new low. The first trade
/// beyond a level is the first record beyond it.
#[derive(Default)]
struct Path {
    open: Option<EventTime>,
    highs: Vec<(EventTime, Price)>,
    lows: Vec<(EventTime, Price)>,
}

impl Path {
    fn push(&mut self, open: EventTime, time: EventTime, price: Price) {
        if self.open != Some(open) {
            self.open = Some(open);
            self.highs.clear();
            self.lows.clear();
        }
        if self.highs.last().is_none_or(|(_, high)| price > *high) {
            self.highs.push((time, price));
        }
        if self.lows.last().is_none_or(|(_, low)| price < *low) {
            self.lows.push((time, price));
        }
    }

    /// The time of the first trade of the bar opening at `open` beyond
    /// `level`.
    fn first_beyond(&self, open: EventTime, side: Side, level: Price) -> Option<EventTime> {
        if self.open != Some(open) {
            return None;
        }
        let records = match side {
            Side::High => &self.highs,
            Side::Low => &self.lows,
        };
        records
            .iter()
            .find(|(_, price)| beyond(side, level, *price))
            .map(|(time, _)| *time)
    }
}

/// A model level (decision 5).
#[derive(Debug, Clone, Copy)]
struct ModelLevel {
    side: Side,
    price: Price,
    swing_time: EventTime,
    confirmed_bar_end: EventTime,
    known_at: EventTime,
    /// Touches at the model's tolerance.
    touches: u32,
    /// Touches at every tolerance of [`TOLERANCES`].
    touches_at: [u32; TOLERANCES.len()],
}

/// A model sweep (decisions 6 and 7).
#[derive(Debug, Clone, Copy)]
struct ModelSweep {
    level: ModelLevel,
    /// The first trade beyond the level.
    time: EventTime,
    /// Open of the bar that holds that trade.
    bar_open: EventTime,
    extreme: Option<Price>,
    window_bars: u32,
    outcome: SweepOutcome,
    resolved_bar_end: Option<EventTime>,
}

/// A sweep followed for its first reclaim (S3).
struct Follow {
    side: Side,
    price: Price,
    bars: u32,
    /// The sweep trade's position in its bar, as the elapsed fraction.
    position: f64,
    half: usize,
}

/// A model fact, in the engine's processing order (decision 8).
#[derive(Debug, Clone, Copy)]
enum Fact {
    Swing(ModelLevel),
    Sweep(ModelSweep),
    Resolved(ModelSweep),
}

/// What one model of one timeframe recorded.
#[derive(Default)]
struct Stats {
    /// Closed bars, by half of their open.
    bars: [u64; 2],
    /// Swings, `[side][half]` by the swing bar's open.
    swings: [[u64; 2]; 2],
    /// `known_at - confirmed_bar_end` of every swing, in ms.
    lag_ms: Vec<f64>,
    /// Swing levels added.
    levels: u64,
    /// Sweeps, `[side][half]` by the sweep time.
    sweeps: [[u64; 2]; 2],
    /// Resolved as SFP / break, by half of the sweep time.
    sfp: [u64; 2],
    breaks: [u64; 2],
    /// Level life from `known_at` to the sweep, in bars, by half of
    /// `known_at`.
    life: [Vec<f64>; 2],
    /// The life of every resolved sweep's level, in bars, and whether the
    /// sweep resolved as an SFP (the TTL record).
    resolved_life: Vec<(f64, bool)>,
    /// Touches at every tolerance of every level once it was swept,
    /// evicted, or active at the window end; by half of `known_at`.
    touched: [Vec<[u32; TOLERANCES.len()]>; 2],
    /// Touches counted at the model's tolerance.
    touch_events: u64,
    /// First-reclaim histogram by half of the sweep time.
    reclaim: [[u64; RECLAIM_SLOTS]; 2],
    /// Sweep positions in their bar: every sweep, and those first
    /// reclaimed in window bar 2.
    position_all: Vec<f64>,
    position_k2: Vec<f64>,
    /// Active list size per side after every closed bar.
    active: [Vec<f64>; 2],
    /// The largest active list per side, per UTC calendar month of the
    /// bar's open, in time order (the growth record).
    monthly: Vec<(Period, [usize; 2])>,
    /// Active levels evicted by the cap or retired by the distance
    /// threshold, and resolved sweeps evicted by the cap.
    evicted: u64,
    evicted_resolved: u64,
    /// Distance of evicted or retired active levels from the last close, in
    /// bps.
    evicted_bps: Vec<f64>,
    /// Age of the oldest resolved sweep after every closed bar, in days.
    history_days: Vec<f64>,
    /// Active levels at the window end.
    unswept: u64,
}

impl Stats {
    fn sweeps(&self) -> u64 {
        self.sweeps.iter().flatten().sum()
    }

    fn resolved(&self) -> (u64, u64) {
        (self.sfp.iter().sum(), self.breaks.iter().sum())
    }
}

/// The model of one timeframe.
struct TimeframeModel {
    timeframe: Timeframe,
    params: Params,
    window: VecDeque<Bar>,
    highs: Vec<ModelLevel>,
    lows: Vec<ModelLevel>,
    pending: Vec<ModelSweep>,
    resolved: Vec<ModelSweep>,
    follows: Vec<Follow>,
    last_close: Option<Price>,
    /// The cold start; `None`: the replay start.
    start: Option<EventTime>,
    /// Whether the model takes closed bars: from the replay start, or for a
    /// cold model from its first bar with trades opening at or after
    /// `start`, as a fresh engine's first bar holds its first trade
    /// (ADR-031, `partial_start`).
    started: bool,
    stats: Stats,
}

impl TimeframeModel {
    fn new(timeframe: Timeframe, params: Params, start: Option<EventTime>) -> Self {
        Self {
            timeframe,
            params,
            window: VecDeque::new(),
            highs: Vec::new(),
            lows: Vec::new(),
            pending: Vec::new(),
            resolved: Vec::new(),
            follows: Vec::new(),
            last_close: None,
            start,
            started: start.is_none(),
            stats: Stats::default(),
        }
    }

    /// Whether the model takes `bar`: a cold model ignores every closed bar
    /// that opens before its start, and every empty bar before its first
    /// bar with trades.
    fn takes(&mut self, bar: &Bar) -> bool {
        if !self.started {
            let after = self.start.is_some_and(|start| bar.open_time >= start);
            self.started = after && bar.ohlc.is_some();
        }
        self.started
    }

    fn half(time: EventTime, mid: i64) -> usize {
        usize::from(time.as_millis() >= mid)
    }

    fn retire(&mut self, level: &ModelLevel, mid: i64) {
        self.stats.touched[Self::half(level.known_at, mid)].push(level.touches_at);
    }

    /// Takes one closed bar, closed by the event at `known_at`: the bar's
    /// sweeps, then resolutions, touches and swing confirmations
    /// (decisions 2–8), then the distance retirements. A bar the model does
    /// not take ([`takes`](Self::takes)) changes nothing.
    fn close(
        &mut self,
        bar: &Bar,
        known_at: EventTime,
        path: &Path,
        mid: i64,
        facts: &mut Vec<Fact>,
    ) -> Result<(), String> {
        if !self.takes(bar) {
            return Ok(());
        }
        let half = Self::half(bar.open_time, mid);
        self.stats.bars[half] += 1;
        if let Some(ohlc) = &bar.ohlc {
            self.last_close = Some(ohlc.close);
            self.sweep(bar, ohlc, path, mid, facts)?;
        }
        self.follow(bar);
        self.resolve(bar, mid, facts);
        if let Some(ohlc) = &bar.ohlc {
            for level in self.highs.iter_mut().chain(self.lows.iter_mut()) {
                if bar.open_time < level.confirmed_bar_end {
                    continue;
                }
                if within(
                    level.side,
                    level.price,
                    ohlc,
                    self.params.touch_tolerance_bps,
                ) {
                    level.touches += 1;
                    self.stats.touch_events += 1;
                }
                for (slot, bps) in TOLERANCES.iter().enumerate() {
                    if within(level.side, level.price, ohlc, *bps) {
                        level.touches_at[slot] += 1;
                    }
                }
            }
        }
        self.confirm(bar, known_at, mid, facts);
        // An empty bar has no close (ADR-031), so it retires nothing.
        if let Some(ohlc) = &bar.ohlc {
            self.retire_distant(ohlc.close, mid);
        }
        self.stats.active[0].push(self.highs.len() as f64);
        self.stats.active[1].push(self.lows.len() as f64);
        let month = Period::month_of(bar.open_time.as_millis().div_euclid(DAY_MS));
        let sizes = [self.highs.len(), self.lows.len()];
        match self.stats.monthly.last_mut() {
            Some((last, max)) if *last == month => {
                max[0] = max[0].max(sizes[0]);
                max[1] = max[1].max(sizes[1]);
            }
            _ => self.stats.monthly.push((month, sizes)),
        }
        if let Some(oldest) = self.resolved.iter().map(|sweep| sweep.time).min() {
            let age = bar.end().as_millis() - oldest.as_millis();
            self.stats.history_days.push(age as f64 / DAY_MS as f64);
        }
        Ok(())
    }

    /// Decision 6: every active level the bar's trades went beyond, dated by
    /// the first such trade, in the engine's order: by time, then highs by
    /// price ascending and lows by price descending, then by confirmation.
    fn sweep(
        &mut self,
        bar: &Bar,
        ohlc: &Ohlc,
        path: &Path,
        mid: i64,
        facts: &mut Vec<Fact>,
    ) -> Result<(), String> {
        let extreme = |side| match side {
            Side::High => ohlc.high,
            Side::Low => ohlc.low,
        };
        let (mut swept, highs): (Vec<ModelLevel>, Vec<ModelLevel>) =
            std::mem::take(&mut self.highs)
                .into_iter()
                .partition(|level| beyond(Side::High, level.price, extreme(Side::High)));
        let (lows_swept, lows): (Vec<ModelLevel>, Vec<ModelLevel>) = std::mem::take(&mut self.lows)
            .into_iter()
            .partition(|level| beyond(Side::Low, level.price, extreme(Side::Low)));
        self.highs = highs;
        self.lows = lows;
        swept.extend(lows_swept);
        if swept.is_empty() {
            return Ok(());
        }
        let mut sweeps = Vec::with_capacity(swept.len());
        for level in swept {
            let time = path
                .first_beyond(bar.open_time, level.side, level.price)
                .ok_or_else(|| {
                    format!(
                        "{} bar {}: no trade of the bar beyond the {} level {}",
                        self.timeframe, bar.open_time, level.side, level.price
                    )
                })?;
            sweeps.push(ModelSweep {
                level,
                time,
                bar_open: bar.open_time,
                extreme: None,
                window_bars: 0,
                outcome: SweepOutcome::Pending,
                resolved_bar_end: None,
            });
        }
        sweeps.sort_by_key(|sweep| {
            let price = sweep.level.price.units();
            let crossed = match sweep.level.side {
                Side::High => price,
                Side::Low => -price,
            };
            (
                sweep.time,
                sweep.level.side,
                crossed,
                sweep.level.confirmed_bar_end,
            )
        });
        let bar_ms = self.timeframe.millis();
        for sweep in sweeps {
            let level = sweep.level;
            let half = Self::half(sweep.time, mid);
            self.stats.sweeps[side_index(level.side)][half] += 1;
            let life = sweep.time.as_millis() - level.known_at.as_millis();
            self.stats.life[Self::half(level.known_at, mid)].push(life as f64 / bar_ms as f64);
            self.retire(&level, mid);
            let position =
                (sweep.time.as_millis() - bar.open_time.as_millis()) as f64 / bar_ms as f64;
            self.stats.position_all.push(position);
            self.follows.push(Follow {
                side: level.side,
                price: level.price,
                bars: 0,
                position,
                half,
            });
            self.pending.push(sweep);
            facts.push(Fact::Sweep(sweep));
        }
        Ok(())
    }

    /// S3: steps every followed sweep with `bar` until its first close at or
    /// inside the level, or [`RECLAIM_BARS`] bars.
    fn follow(&mut self, bar: &Bar) {
        for mut follow in std::mem::take(&mut self.follows) {
            follow.bars += 1;
            let reclaimed = bar
                .ohlc
                .is_some_and(|ohlc| inside(follow.side, follow.price, &ohlc));
            if reclaimed {
                self.stats.reclaim[follow.half][follow.bars as usize] += 1;
                if follow.bars == 2 {
                    self.stats.position_k2.push(follow.position);
                }
            } else if follow.bars == RECLAIM_BARS {
                self.stats.reclaim[follow.half][RECLAIM_SLOTS - 1] += 1;
            } else {
                self.follows.push(follow);
            }
        }
    }

    /// Decision 7: steps every pending sweep's window with `bar`, in sweep
    /// order.
    fn resolve(&mut self, bar: &Bar, mid: i64, facts: &mut Vec<Fact>) {
        for mut sweep in std::mem::take(&mut self.pending) {
            sweep.window_bars += 1;
            let side = sweep.level.side;
            let reclaimed = match &bar.ohlc {
                Some(ohlc) => {
                    let far = match side {
                        Side::High => sweep.extreme.map_or(ohlc.high, |e| e.max(ohlc.high)),
                        Side::Low => sweep.extreme.map_or(ohlc.low, |e| e.min(ohlc.low)),
                    };
                    sweep.extreme = Some(far);
                    inside(side, sweep.level.price, ohlc)
                }
                None => false,
            };
            if !reclaimed && sweep.window_bars < self.params.sfp_window_bars {
                self.pending.push(sweep);
                continue;
            }
            sweep.resolved_bar_end = Some(bar.end());
            let half = Self::half(sweep.time, mid);
            if reclaimed {
                sweep.outcome = SweepOutcome::Sfp;
                self.stats.sfp[half] += 1;
            } else {
                sweep.outcome = SweepOutcome::Break;
                self.stats.breaks[half] += 1;
            }
            let life = sweep.time.as_millis() - sweep.level.known_at.as_millis();
            self.stats
                .resolved_life
                .push((life as f64 / self.timeframe.millis() as f64, reclaimed));
            if let Some(cap) = self.params.max_resolved
                && self.resolved.len() >= cap
                && let Some(oldest) = oldest(&self.resolved, |s| (s.time, s.level.price))
            {
                self.resolved.remove(oldest);
                self.stats.evicted_resolved += 1;
            }
            self.resolved.push(sweep);
            facts.push(Fact::Resolved(sweep));
        }
    }

    /// Decisions 2 and 3: the swings of the window's middle bar, the high
    /// first, each added as an active level (decision 5).
    fn confirm(&mut self, bar: &Bar, known_at: EventTime, mid: i64, facts: &mut Vec<Fact>) {
        let n = self.params.swing_bars;
        if self.window.len() == 2 * n + 1 {
            self.window.pop_front();
        }
        self.window.push_back(*bar);
        if self.window.len() < 2 * n + 1 {
            return;
        }
        let candidate = self.window[n];
        let Some(ohlc) = candidate.ohlc else {
            return;
        };
        let left = || self.window.range(..n).filter_map(|bar| bar.ohlc);
        let right = || self.window.range(n + 1..).filter_map(|bar| bar.ohlc);
        let high = left().all(|other| ohlc.high > other.high)
            && right().all(|other| ohlc.high >= other.high);
        let low =
            left().all(|other| ohlc.low < other.low) && right().all(|other| ohlc.low <= other.low);
        for (side, price, confirmed) in [(Side::High, ohlc.high, high), (Side::Low, ohlc.low, low)]
        {
            if !confirmed {
                continue;
            }
            let level = ModelLevel {
                side,
                price,
                swing_time: candidate.open_time,
                confirmed_bar_end: bar.end(),
                known_at,
                touches: 0,
                touches_at: [0; TOLERANCES.len()],
            };
            let half = Self::half(candidate.open_time, mid);
            self.stats.swings[side_index(side)][half] += 1;
            self.stats
                .lag_ms
                .push((known_at.as_millis() - level.confirmed_bar_end.as_millis()) as f64);
            self.stats.levels += 1;
            self.add(level, mid);
            facts.push(Fact::Swing(level));
        }
    }

    /// Decision 5: adds a level, evicting the oldest of its list (by
    /// `confirmed_bar_end`, then price) when the list is full.
    fn add(&mut self, level: ModelLevel, mid: i64) {
        let cap = self.params.max_active;
        let list = match level.side {
            Side::High => &mut self.highs,
            Side::Low => &mut self.lows,
        };
        let mut evicted = None;
        if let Some(cap) = cap
            && list.len() >= cap
            && let Some(oldest) = oldest(list, |l| (l.confirmed_bar_end, l.price))
        {
            evicted = Some(list.remove(oldest));
        }
        list.push(level);
        if let Some(evicted) = evicted {
            self.evict(&evicted, mid);
        }
    }

    /// Records an active level the cap evicted or the distance threshold
    /// retired.
    fn evict(&mut self, level: &ModelLevel, mid: i64) {
        self.stats.evicted += 1;
        if let Some(close) = self.last_close {
            let close = close.units() as f64;
            let distance = (level.price.units() as f64 - close).abs() * 10_000.0 / close;
            self.stats.evicted_bps.push(distance);
        }
        self.retire(level, mid);
    }

    /// The distance rule: retires every active level `L` with
    /// `|L - close| * 10 000 > close * D`, exact in `i128`. The denominator
    /// is the close, so at `D` = 10 000 no low (`0 < L < close`) can retire.
    fn retire_distant(&mut self, close: Price, mid: i64) {
        let Some(bps) = self.params.max_distance_bps else {
            return;
        };
        let close = i128::from(close.units());
        let far = |level: &ModelLevel| {
            (i128::from(level.price.units()) - close).abs() * 10_000 > close * i128::from(bps)
        };
        let mut retired = Vec::new();
        for list in [&mut self.highs, &mut self.lows] {
            let (gone, kept): (Vec<ModelLevel>, Vec<ModelLevel>) = std::mem::take(list)
                .into_iter()
                .partition(|level| far(level));
            *list = kept;
            retired.extend(gone);
        }
        for level in &retired {
            self.evict(level, mid);
        }
    }

    /// Records the levels still active at the window end.
    fn finish(&mut self, mid: i64) {
        let active: Vec<ModelLevel> = self.highs.iter().chain(&self.lows).copied().collect();
        self.stats.unswept = active.len() as u64;
        for level in &active {
            self.retire(level, mid);
        }
    }

    /// Active levels of `side`, by `(price, confirmed_bar_end)`, as the
    /// engine lists them.
    fn sorted(&self, side: Side) -> Vec<ModelLevel> {
        let mut list = match side {
            Side::High => self.highs.clone(),
            Side::Low => self.lows.clone(),
        };
        list.sort_by_key(|level| (level.price, level.confirmed_bar_end));
        list
    }
}

/// The index of the entry with the smallest `key`; the first on ties.
fn oldest<T, K: Ord>(list: &[T], key: impl Fn(&T) -> K) -> Option<usize> {
    list.iter()
        .enumerate()
        .min_by_key(|(index, entry)| (key(entry), *index))
        .map(|(index, _)| index)
}

/// One parameter set over every structure timeframe, from the replay start
/// or from a cold start.
struct Model {
    params: Params,
    /// The cold start; `None`: the replay start.
    start: Option<EventTime>,
    timeframes: [TimeframeModel; 4],
}

impl Model {
    fn new(params: Params, start: Option<EventTime>) -> Self {
        Self {
            params,
            start,
            timeframes: STRUCTURE_TIMEFRAMES
                .map(|timeframe| TimeframeModel::new(timeframe, params, start)),
        }
    }
}

/// A level's identity in the skew record, per side: price and
/// `swing_time`.
type Key = (Price, EventTime);

/// The active levels and the pending and resolved sweeps' levels of one
/// side, each sorted.
fn keys(model: &TimeframeModel, side: Side) -> (Vec<Key>, Vec<Key>) {
    let list = match side {
        Side::High => &model.highs,
        Side::Low => &model.lows,
    };
    let mut active: Vec<Key> = list
        .iter()
        .map(|level| (level.price, level.swing_time))
        .collect();
    let mut swept: Vec<Key> = model
        .pending
        .iter()
        .chain(&model.resolved)
        .filter(|sweep| sweep.level.side == side)
        .map(|sweep| (sweep.level.price, sweep.level.swing_time))
        .collect();
    active.sort_unstable();
    swept.sort_unstable();
    (active, swept)
}

/// By a sorted merge of two sorted key lists: how many keys only `full`
/// holds, and how many only `cold` holds.
fn differ(full: &[Key], cold: &[Key]) -> (u64, u64) {
    let (mut i, mut j) = (0, 0);
    let (mut only_full, mut only_cold) = (0, 0);
    while i < full.len() && j < cold.len() {
        match full[i].cmp(&cold[j]) {
            std::cmp::Ordering::Less => {
                only_full += 1;
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                only_cold += 1;
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                i += 1;
                j += 1;
            }
        }
    }
    only_full += (full.len() - i) as u64;
    only_cold += (cold.len() - j) as u64;
    (only_full, only_cold)
}

/// The skew of one cold model against the full-window model of its rule,
/// on one timeframe and side (S6).
#[derive(Default)]
struct SkewRecord {
    /// `extra` and `extra_swept` after every compared bar.
    extra: Vec<f64>,
    extra_swept: Vec<f64>,
    missing_max: u64,
    missing_swept_max: u64,
    /// `extra`, `missing`, `extra_swept`, `missing_swept` after the last
    /// compared bar.
    end: [u64; 4],
    /// The end of the first closed bar from which all four counts stayed 0;
    /// `None` while any is non-zero.
    converged: Option<EventTime>,
}

impl SkewRecord {
    fn push(&mut self, counts: [u64; 4], bar_end: EventTime) {
        let [extra, missing, extra_swept, missing_swept] = counts;
        self.extra.push(extra as f64);
        self.extra_swept.push(extra_swept as f64);
        self.missing_max = self.missing_max.max(missing);
        self.missing_swept_max = self.missing_swept_max.max(missing_swept);
        self.end = counts;
        if counts.iter().any(|count| *count > 0) {
            self.converged = None;
        } else if self.converged.is_none() {
            self.converged = Some(bar_end);
        }
    }
}

/// The skew record of one rule: its full-window model and its cold models.
struct Skew {
    params: Params,
    /// The full-window model's slot in [`Run::models`].
    full: usize,
    /// Per cold start, the cold model's slot.
    cold: Vec<usize>,
    /// `[cold start][timeframe][side]`.
    records: Vec<[[SkewRecord; 2]; 4]>,
}

impl Skew {
    /// Compares every cold model that took `bar` with the full model, on
    /// the bar's timeframe `index`.
    fn compare(&mut self, index: usize, bar: &Bar, models: &[Model]) {
        if !self
            .cold
            .iter()
            .any(|slot| models[*slot].timeframes[index].started)
        {
            return;
        }
        let full = &models[self.full].timeframes[index];
        let theirs = SIDES.map(|side| keys(full, side));
        for (records, slot) in self.records.iter_mut().zip(&self.cold) {
            let cold = &models[*slot].timeframes[index];
            if !cold.started {
                continue;
            }
            for (side, record) in SIDES.into_iter().zip(&mut records[index]) {
                let (active, swept) = keys(cold, side);
                let (full_active, full_swept) = &theirs[side_index(side)];
                let (extra, missing) = differ(full_active, &active);
                let (extra_swept, missing_swept) = differ(full_swept, &swept);
                record.push([extra, missing, extra_swept, missing_swept], bar.end());
            }
        }
    }

    /// The sum over every start, timeframe and side of the largest
    /// `missing`.
    fn missing(&self) -> u64 {
        self.records
            .iter()
            .flatten()
            .flatten()
            .map(|record| record.missing_max)
            .sum()
    }
}

/// A fresh engine started at a cold start, compared with the cold
/// engine-parameter model under R0's comparison (CI only).
struct Fresh {
    start: EventTime,
    /// The cold engine-parameter model's slot in [`Run::models`].
    slot: usize,
    engine: MarketStateEngine,
    check: Check,
    rejections: u64,
}

/// The R0 comparison of the engine-parameter model with the engine.
#[derive(Default)]
struct Check {
    mismatches: u64,
    notes: Vec<String>,
    /// Engine sweeps since the last close of each timeframe.
    queues: [Vec<Sweep>; 4],
    swings: u64,
    sweeps: [u64; 2],
    sfps: u64,
    breaks: u64,
    registries: u64,
    touched: u64,
}

fn same_level(model: &ModelLevel, engine: &Level) -> bool {
    model.side == engine.side
        && model.price == engine.price
        && model.swing_time == engine.swing_time
        && model.confirmed_bar_end == engine.confirmed_bar_end
        && model.touches == engine.touches
}

impl Check {
    fn mismatch(&mut self, note: String) {
        self.mismatches += 1;
        if self.notes.len() < 20 {
            self.notes.push(note);
        }
    }

    /// Compares one accepted event: `facts` are the model's, `closed` marks
    /// the timeframes the event closed a bar of.
    fn compare(
        &mut self,
        engine: &MarketStateEngine,
        known_at: EventTime,
        facts: &[(usize, Fact)],
        closed: [bool; 4],
        model: &Model,
    ) {
        let mut later: Vec<&StructureEvent> = Vec::new();
        let mut swept: [Vec<Sweep>; 4] = Default::default();
        for event in engine.structure_events() {
            match event {
                StructureEvent::Sweep(sweep) => match timeframe_index(sweep.timeframe) {
                    Some(index) => swept[index].push(*sweep),
                    None => self.mismatch(format!("{known_at}: sweep on {}", sweep.timeframe)),
                },
                other => later.push(other),
            }
        }
        // Sweeps: the model dates them at their bar's close, the engine at
        // their trade, since the timeframe's previous close.
        for (index, done) in closed.iter().enumerate() {
            if !done {
                continue;
            }
            let queue = std::mem::take(&mut self.queues[index]);
            let ours: Vec<ModelSweep> = facts
                .iter()
                .filter_map(|(tf, fact)| match fact {
                    Fact::Sweep(sweep) if *tf == index => Some(*sweep),
                    _ => None,
                })
                .collect();
            if queue.len() != ours.len() {
                self.mismatch(format!(
                    "{known_at} {}: engine swept {} levels, the model {}",
                    STRUCTURE_TIMEFRAMES[index],
                    queue.len(),
                    ours.len()
                ));
            }
            let bar_ms = STRUCTURE_TIMEFRAMES[index].millis();
            for (theirs, ours) in queue.iter().zip(&ours) {
                let open = ours.bar_open.as_millis();
                let in_bar = (open..open + bar_ms).contains(&theirs.time.as_millis());
                if same_level(&ours.level, &theirs.level) && in_bar {
                    self.sweeps[side_index(theirs.level.side)] += 1;
                } else {
                    self.mismatch(format!("{known_at}: engine {theirs}, model {ours:?}"));
                }
            }
        }
        // Resolutions and swings, in processing order.
        let ours: Vec<(usize, Fact)> = facts
            .iter()
            .filter(|(_, fact)| !matches!(fact, Fact::Sweep(_)))
            .copied()
            .collect();
        if ours.len() != later.len() {
            self.mismatch(format!(
                "{known_at}: engine emitted {} swings and resolutions, the model {}",
                later.len(),
                ours.len()
            ));
        }
        for (theirs, (index, fact)) in later.iter().zip(&ours) {
            let timeframe = STRUCTURE_TIMEFRAMES[*index];
            let same = match (theirs, fact) {
                (StructureEvent::Swing(swing), Fact::Swing(level)) => {
                    swing.timeframe == timeframe
                        && swing.side == level.side
                        && swing.price == level.price
                        && swing.swing_time == level.swing_time
                        && swing.confirmed_bar_end == level.confirmed_bar_end
                }
                (
                    StructureEvent::Sfp(sweep) | StructureEvent::Break(sweep),
                    Fact::Resolved(ours),
                ) => {
                    let variant = matches!(theirs, StructureEvent::Sfp(_));
                    sweep.timeframe == timeframe
                        && variant == (ours.outcome == SweepOutcome::Sfp)
                        && same_level(&ours.level, &sweep.level)
                        && sweep.outcome == ours.outcome
                        && sweep.resolved_bar_end == ours.resolved_bar_end
                        && Some(sweep.extreme) == ours.extreme
                        && sweep.window_bars == ours.window_bars
                }
                _ => false,
            };
            if same {
                match theirs {
                    StructureEvent::Swing(_) => self.swings += 1,
                    StructureEvent::Sfp(_) => self.sfps += 1,
                    StructureEvent::Break(_) => self.breaks += 1,
                    StructureEvent::Sweep(_) => {}
                }
            } else {
                self.mismatch(format!("{known_at}: engine {theirs}, model {fact:?}"));
            }
        }
        // The registries of the timeframes the event closed.
        for (index, done) in closed.iter().enumerate() {
            if !done {
                continue;
            }
            let timeframe = STRUCTURE_TIMEFRAMES[index];
            let Some(FeatureValue::Ready(registry)) = engine
                .state()
                .structure
                .get(timeframe)
                .map(|structure| &structure.levels)
            else {
                continue;
            };
            let ours = &model.timeframes[index];
            for side in SIDES {
                let mut theirs: Vec<Level> = match side {
                    Side::High => registry.highs().to_vec(),
                    Side::Low => registry.lows().to_vec(),
                };
                theirs.extend(
                    swept[index]
                        .iter()
                        .filter(|sweep| sweep.level.side == side)
                        .map(|sweep| sweep.level),
                );
                theirs.sort_by_key(|level| (level.price, level.confirmed_bar_end));
                let expected = ours.sorted(side);
                let same = theirs.len() == expected.len()
                    && theirs.iter().zip(&expected).all(|(e, m)| same_level(m, e));
                if !same {
                    self.mismatch(format!(
                        "{known_at} {timeframe}: active {side}s differ: engine {registry}, model {expected:?}"
                    ));
                }
                self.touched += expected.iter().filter(|level| level.touches > 0).count() as u64;
            }
            let resolved = registry.resolved();
            let same = resolved.len() == ours.resolved.len()
                && resolved.iter().zip(&ours.resolved).all(|(e, m)| {
                    same_level(&m.level, &e.level) && e.outcome == m.outcome && e.time == m.time
                });
            if !same {
                self.mismatch(format!(
                    "{known_at} {timeframe}: resolved sweeps differ: engine {registry}, model {:?}",
                    ours.resolved
                ));
            }
            self.registries += 1;
        }
        for (index, sweeps) in swept.into_iter().enumerate() {
            self.queues[index].extend(sweeps);
        }
    }
}

/// One `MarketStateEngine` and every model, stepped together.
struct Run {
    mid: i64,
    engine: MarketStateEngine,
    paths: [Path; 4],
    /// `models[0]` has the engine's parameters; the full-window models come
    /// first, then the cold ones.
    models: Vec<Model>,
    /// The cold starts of the skew record, ascending.
    starts: Vec<EventTime>,
    /// The skew record of cap 20 (the engine's rule), then `until_swept`.
    skews: Vec<Skew>,
    /// A fresh engine checked against a cold model (CI only).
    fresh: Option<Fresh>,
    check: Check,
    events: u64,
    rejections: u64,
}

impl Run {
    /// Every model of the report, and for every start of `cold_starts` a
    /// cold model of cap 20 and one of `until_swept`.
    fn new(mid: i64, cold_starts: &[EventTime]) -> Self {
        let mut params = vec![Params::ENGINE];
        for swing_bars in SWING_SIZES {
            params.push(Params {
                swing_bars,
                ..Params::UNCAPPED
            });
        }
        let alternatives = CAPS
            .map(|cap| Params {
                max_active: Some(cap),
                ..Params::ENGINE
            })
            .into_iter()
            .chain(DISTANCES.map(|bps| Params {
                max_distance_bps: Some(bps),
                ..Params::UNTIL_SWEPT
            }))
            .chain([Params::UNTIL_SWEPT]);
        for alternative in alternatives {
            if !params.contains(&alternative) {
                params.push(alternative);
            }
        }
        let mut models: Vec<Model> = params
            .into_iter()
            .map(|params| Model::new(params, None))
            .collect();
        let mut skews = Vec::new();
        for rule in [Params::ENGINE, Params::UNTIL_SWEPT] {
            let full = models
                .iter()
                .position(|model| model.params == rule)
                .expect("a full-window model of the rule");
            let mut cold = Vec::new();
            for start in cold_starts {
                cold.push(models.len());
                models.push(Model::new(rule, Some(*start)));
            }
            skews.push(Skew {
                params: rule,
                full,
                records: cold.iter().map(|_| Default::default()).collect(),
                cold,
            });
        }
        Self {
            mid,
            engine: MarketStateEngine::new(),
            paths: Default::default(),
            models,
            starts: cold_starts.to_vec(),
            skews,
            fresh: None,
            check: Check::default(),
            events: 0,
            rejections: 0,
        }
    }

    /// Adds a fresh engine that takes the events from `start` on, checked
    /// against the cold engine-parameter model at `start`.
    fn with_fresh(mut self, start: EventTime) -> Self {
        let slot = self
            .models
            .iter()
            .position(|model| model.params == Params::ENGINE && model.start == Some(start))
            .expect("a cold engine-parameter model at the start");
        self.fresh = Some(Fresh {
            start,
            slot,
            engine: MarketStateEngine::new(),
            check: Check::default(),
            rejections: 0,
        });
        self
    }

    /// The full-window model of `params`.
    fn model(&self, params: Params) -> &Model {
        self.models
            .iter()
            .find(|model| model.params == params && model.start.is_none())
            .expect("a model of these parameters")
    }

    /// The cold model of `params` started at `start`.
    fn cold(&self, params: Params, start: EventTime) -> &Model {
        self.models
            .iter()
            .find(|model| model.params == params && model.start == Some(start))
            .expect("a cold model of these parameters")
    }

    fn step(&mut self, event: &MarketEvent) {
        self.events += 1;
        if self.engine.apply(event).is_err() {
            self.rejections += 1;
            return;
        }
        let known_at = event.time();
        let mut facts = Vec::new();
        let mut fresh_facts = Vec::new();
        let fresh_slot = self.fresh.as_ref().map(|fresh| fresh.slot);
        let mut closed = [false; 4];
        let mut scratch = Vec::new();
        for bar in self.engine.closed_bars() {
            let Some(index) = timeframe_index(bar.timeframe) else {
                continue;
            };
            closed[index] = true;
            for (slot, model) in self.models.iter_mut().enumerate() {
                scratch.clear();
                let close = model.timeframes[index].close(
                    bar,
                    known_at,
                    &self.paths[index],
                    self.mid,
                    &mut scratch,
                );
                if let Err(note) = close {
                    self.check.mismatch(note);
                }
                if slot == 0 {
                    facts.extend(scratch.iter().map(|fact| (index, *fact)));
                }
                if Some(slot) == fresh_slot {
                    fresh_facts.extend(scratch.iter().map(|fact| (index, *fact)));
                }
            }
            for skew in &mut self.skews {
                skew.compare(index, bar, &self.models);
            }
        }
        if closed.contains(&true) || !self.engine.structure_events().is_empty() {
            self.check
                .compare(&self.engine, known_at, &facts, closed, &self.models[0]);
        }
        if let Some(fresh) = &mut self.fresh
            && known_at >= fresh.start
        {
            if fresh.engine.apply(event).is_err() {
                fresh.rejections += 1;
            } else {
                // The closed flags are the fresh engine's, the facts the
                // cold model's.
                let mut closed = [false; 4];
                for bar in fresh.engine.closed_bars() {
                    if let Some(index) = timeframe_index(bar.timeframe) {
                        closed[index] = true;
                    }
                }
                if closed.contains(&true) || !fresh.engine.structure_events().is_empty() {
                    fresh.check.compare(
                        &fresh.engine,
                        known_at,
                        &fresh_facts,
                        closed,
                        &self.models[fresh.slot],
                    );
                }
            }
        }
        if let MarketEvent::Trade(trade) = event {
            for (path, timeframe) in self.paths.iter_mut().zip(STRUCTURE_TIMEFRAMES) {
                if let Some(open) = timeframe.open_of(trade.time) {
                    path.push(open, trade.time, trade.price);
                }
            }
        }
    }

    fn finish(&mut self) {
        for model in &mut self.models {
            for timeframe in &mut model.timeframes {
                timeframe.finish(self.mid);
            }
        }
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn quantile(values: &mut [f64], q: f64) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    values.sort_by(f64::total_cmp);
    let rank = ((values.len() - 1) as f64 * q).round() as usize;
    values[rank]
}

fn quantiles(values: &[f64], qs: &[f64], digits: usize) -> String {
    let mut values = values.to_vec();
    let cells: Vec<String> = qs
        .iter()
        .map(|q| format!("{:.digits$}", quantile(&mut values, *q)))
        .collect();
    format!("{} (n {})", cells.join("/"), values.len())
}

fn pct(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "-".to_owned();
    }
    format!("{:.2} %", 100.0 * part as f64 / whole as f64)
}

fn verdict(pass: bool) -> &'static str {
    if pass { "PASS" } else { "FAIL" }
}

/// The halves, then the whole window.
const HALVES: [Option<usize>; 3] = [Some(0), Some(1), None];

fn half_label(half: Option<usize>) -> &'static str {
    match half {
        Some(0) => "first half",
        Some(_) => "second half",
        None => "whole window",
    }
}

fn sum(values: &[u64; 2], half: Option<usize>) -> u64 {
    half.map_or(values[0] + values[1], |half| values[half])
}

fn joined<T: Clone>(values: &[Vec<T>; 2], half: Option<usize>) -> Vec<T> {
    half.map_or_else(|| values.concat(), |half| values[half].clone())
}

/// The random-walk probability that a bar is a swing high (Sparre
/// Andersen): `u_N²`, `u_N = C(2N, N) / 4^N`.
fn null_rate(n: usize) -> f64 {
    let mut u = 1.0;
    for k in 1..=n {
        u *= (2 * k - 1) as f64 / (2 * k) as f64;
    }
    u * u
}

fn reclaims(stats: &Stats, half: Option<usize>) -> [u64; RECLAIM_SLOTS] {
    let mut total = [0; RECLAIM_SLOTS];
    for (slot, count) in total.iter_mut().enumerate() {
        *count = match half {
            Some(half) => stats.reclaim[half][slot],
            None => stats.reclaim[0][slot] + stats.reclaim[1][slot],
        };
    }
    total
}

fn report(run: &Run, from: i64, to: i64, wall: Duration) {
    let days = (to - from) as f64 / DAY_MS as f64;
    let half_days = |half: Option<usize>| if half.is_some() { days / 2.0 } else { days };
    let base = run.model(Params::ENGINE);
    let uncapped = run.model(Params::UNCAPPED);
    let check = &run.check;
    println!(
        "window {} .. {} ({days:.0} days), halves split at {}",
        EventTime::from_millis(from),
        EventTime::from_millis(to),
        EventTime::from_millis(run.mid)
    );
    println!(
        "run: {} events, {} domain rejections, wall time {:.1} s",
        run.events,
        run.rejections,
        wall.as_secs_f64()
    );
    println!(
        "R0 at N={SWING_BARS}, K={SFP_WINDOW_BARS}, tolerance={TOUCH_TOLERANCE_BPS} bps, \
         cap={MAX_LEVELS}: compared swings {}, sweeps of highs {}, sweeps of lows {}, SFPs {}, \
         breaks {}, registries {} ({} touched active levels)",
        check.swings,
        check.sweeps[0],
        check.sweeps[1],
        check.sfps,
        check.breaks,
        check.registries,
        check.touched
    );
    for note in &check.notes {
        println!("  mismatch: {note}");
    }
    println!(
        "R0: engine/model mismatches {}, domain rejections {} -> {}",
        check.mismatches,
        run.rejections,
        if check.mismatches == 0 && run.rejections == 0 {
            "VALID"
        } else {
            "VOID"
        }
    );

    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &base.timeframes[index].stats;
        let (sfp, breaks) = stats.resolved();
        let counts = [
            ("swing highs", stats.swings[0].iter().sum::<u64>()),
            ("swing lows", stats.swings[1].iter().sum()),
            ("sweeps of highs", stats.sweeps[0].iter().sum()),
            ("sweeps of lows", stats.sweeps[1].iter().sum()),
            ("SFPs", sfp),
            ("breaks", breaks),
            ("touches", stats.touch_events),
        ];
        let line: Vec<String> = counts
            .iter()
            .map(|(name, n)| format!("{name} {n}"))
            .collect();
        let pass = counts.iter().all(|(_, n)| *n >= 1);
        println!("R1 {timeframe}: {} -> {}", line.join(", "), verdict(pass));
    }
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &base.timeframes[index].stats;
        let (sfp, breaks) = stats.resolved();
        let n = sfp + breaks;
        let lost = uncapped.timeframes[index].stats.sweeps() - stats.sweeps();
        let p = if n == 0 {
            f64::NAN
        } else {
            sfp as f64 / n as f64
        };
        let bound = (p * (1.0 - p) / n as f64).sqrt();
        let share = lost as f64 / (n + lost) as f64;
        println!(
            "R2 {timeframe}: n {n}, p {p:.4}, lost {lost}, lost share {share:.4}, \
             sqrt(p(1-p)/n) {bound:.4} -> {}",
            verdict(share <= bound)
        );
    }
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let total = reclaims(&base.timeframes[index].stats, None);
        println!(
            "R3 {timeframe}: first reclaim in window bar 2: {}, bar 3: {} -> {}",
            total[2],
            total[3],
            verdict(total[3] < total[2])
        );
    }

    println!("S1: swings per day and per closed bar (first half / second half / whole)");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        for n in SWING_SIZES {
            let stats = &run
                .model(Params {
                    swing_bars: n,
                    ..Params::UNCAPPED
                })
                .timeframes[index]
                .stats;
            let mut cells = Vec::new();
            for (side, name) in [(0, "highs"), (1, "lows")] {
                let per_day: Vec<String> = HALVES
                    .iter()
                    .map(|h| format!("{:.3}", sum(&stats.swings[side], *h) as f64 / half_days(*h)))
                    .collect();
                let per_bar: Vec<String> = HALVES
                    .iter()
                    .map(|h| pct(sum(&stats.swings[side], *h), sum(&stats.bars, *h)))
                    .collect();
                cells.push(format!(
                    "{name}/day {}, per bar {}",
                    per_day.join(" / "),
                    per_bar.join(" / ")
                ));
            }
            println!(
                "  {timeframe} N={n}: {}; null u_N² {:.2} %; confirmation N bars = {} min",
                cells.join("; "),
                100.0 * null_rate(n),
                n as i64 * timeframe.millis() / 60_000
            );
        }
        println!(
            "  {timeframe} N={SWING_BARS}: known_at - confirmed_bar_end ms p50/p99/max {}",
            quantiles(&base.timeframes[index].stats.lag_ms, &[0.5, 0.99, 1.0], 0)
        );
    }

    println!("S2: level life from known_at to the sweep, in bars, p25/p50/p75 (uncapped)");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        for n in SWING_SIZES {
            let stats = &run
                .model(Params {
                    swing_bars: n,
                    ..Params::UNCAPPED
                })
                .timeframes[index]
                .stats;
            let cells: Vec<String> = HALVES
                .iter()
                .map(|h| {
                    format!(
                        "{} {}",
                        half_label(*h),
                        quantiles(&joined(&stats.life, *h), &[0.25, 0.5, 0.75], 1)
                    )
                })
                .collect();
            println!(
                "  {timeframe} N={n}: {}; unswept at the window end {}",
                cells.join("; "),
                pct(stats.unswept, stats.levels)
            );
        }
    }

    println!("S3: sweeps and their outcome (engine parameters)");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &base.timeframes[index].stats;
        for half in HALVES {
            let sweeps_per_day: Vec<String> = [0, 1]
                .iter()
                .map(|side| {
                    format!(
                        "{:.3}",
                        sum(&stats.sweeps[*side], half) as f64 / half_days(half)
                    )
                })
                .collect();
            let reclaim = reclaims(stats, half);
            let followed: u64 = reclaim.iter().sum();
            let shares: Vec<String> = WINDOWS
                .iter()
                .map(|k| {
                    let within: u64 = reclaim[1..=*k as usize].iter().sum();
                    format!("K={k} {}", pct(within, followed))
                })
                .collect();
            let histogram: Vec<String> = (1..RECLAIM_SLOTS)
                .map(|slot| {
                    if slot == RECLAIM_SLOTS - 1 {
                        format!(">{RECLAIM_BARS}: {}", reclaim[slot])
                    } else {
                        format!("{slot}: {}", reclaim[slot])
                    }
                })
                .collect();
            let sfp = sum(&stats.sfp, half);
            println!(
                "  {timeframe} {}: sweeps/day highs {}, lows {}; SFP share {}; model K={SFP_WINDOW_BARS} \
                 outcomes SFP {} of {}; first reclaim bar {}",
                half_label(half),
                sweeps_per_day[0],
                sweeps_per_day[1],
                shares.join(", "),
                sfp,
                sfp + sum(&stats.breaks, half),
                histogram.join(", ")
            );
        }
        let qs = [0.1, 0.25, 0.5, 0.75, 0.9];
        println!(
            "  {timeframe} sweep position in its bar p10/p25/p50/p75/p90: break at K=1 but SFP at K=2 {}; all sweeps {}",
            quantiles(&stats.position_k2, &qs, 3),
            quantiles(&stats.position_all, &qs, 3)
        );
    }

    println!("S4: touches per level by tolerance (engine N and cap)");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &base.timeframes[index].stats;
        for (slot, bps) in TOLERANCES.iter().enumerate() {
            let shares: Vec<String> = HALVES
                .iter()
                .map(|h| {
                    let levels = joined(&stats.touched, *h);
                    let touched = levels.iter().filter(|t| t[slot] >= 1).count() as u64;
                    pct(touched, levels.len() as u64)
                })
                .collect();
            let counts: Vec<f64> = joined(&stats.touched, None)
                .iter()
                .map(|t| f64::from(t[slot]))
                .collect();
            println!(
                "  {timeframe} {bps:>2} bps: levels touched {}; touches per level p50/p90/max {}",
                shares.join(" / "),
                quantiles(&counts, &[0.5, 0.9, 1.0], 0)
            );
        }
    }

    println!("S5: the cap");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let open = &uncapped.timeframes[index].stats;
        let stats = &base.timeframes[index].stats;
        let per_year = 365.0 / days;
        let lost: Vec<String> = CAPS
            .iter()
            .map(|cap| {
                let capped = &run
                    .model(Params {
                        max_active: Some(*cap),
                        ..Params::ENGINE
                    })
                    .timeframes[index]
                    .stats;
                format!("cap {cap} {}", open.sweeps() - capped.sweeps())
            })
            .collect();
        println!(
            "  {timeframe}: uncapped active highs p50/p99/max {}, lows {}; evictions per year at cap \
             {MAX_LEVELS}: active {:.1}, resolved {:.1}; lost {}; evicted distance from the last close \
             bps p10/p50 {}; resolved history days p10/p50 {}",
            quantiles(&open.active[0], &[0.5, 0.99, 1.0], 0),
            quantiles(&open.active[1], &[0.5, 0.99, 1.0], 0),
            stats.evicted as f64 * per_year,
            stats.evicted_resolved as f64 * per_year,
            lost.join(", "),
            quantiles(&stats.evicted_bps, &[0.1, 0.5], 1),
            quantiles(&stats.history_days, &[0.1, 0.5], 2)
        );
    }
    report_alternatives(run);
    report_skew(run);
}

/// R2 for `n` resolved sweeps, `sfp` of them SFPs, and `lost` sweeps:
/// `(p, lost share, sqrt(p (1 - p) / n), PASS)`.
fn r2(n: u64, sfp: u64, lost: u64) -> (f64, f64, f64, bool) {
    let p = if n == 0 {
        f64::NAN
    } else {
        sfp as f64 / n as f64
    };
    let bound = (p * (1.0 - p) / n as f64).sqrt();
    let share = lost as f64 / (n + lost) as f64;
    (p, share, bound, share <= bound)
}

/// One fixed-grid candidate of premise P: a cap or a distance.
#[derive(Clone)]
struct Candidate {
    rule: String,
    /// Active evictions summed over the four timeframes.
    evictions: u64,
    /// The R2 verdict per timeframe.
    passes: [bool; 4],
}

/// Premise P (#95): no candidate that evicts at least one active level
/// passes R2 on all four timeframes. Returns the candidates that falsify
/// it; empty when P holds.
fn premise_p(candidates: &[Candidate]) -> Vec<&str> {
    candidates
        .iter()
        .filter(|candidate| candidate.evictions >= 1 && candidate.passes.iter().all(|pass| *pass))
        .map(|candidate| candidate.rule.as_str())
        .collect()
}

/// The TTL record at the smallest passing age.
#[derive(Debug, PartialEq)]
struct Ttl {
    /// `A*`, in bars.
    age: f64,
    /// Sweeps of levels that lived longer than `A*`.
    lost: u64,
    /// Resolved sweeps of levels that lived at most `A*`, and their SFPs.
    n: u64,
    sfp: u64,
}

/// The smallest level age `A*` at which a TTL would pass R2. `lives` are
/// the level lives of every sweep, `resolved` those of the resolved sweeps
/// with their SFP flag. At an age `A`, `lost` counts the sweeps with life
/// `> A` (a TTL of `A` retires exactly those levels before their sweep);
/// `n` and `p` come from the resolved sweeps with life `<= A`. R2 is not
/// monotone in `A`, so every distinct life is scanned in ascending order
/// and the first passing one is returned; `None` if none passes.
fn ttl_star(lives: &[f64], resolved: &[(f64, bool)]) -> Option<Ttl> {
    let mut lives = lives.to_vec();
    lives.sort_by(f64::total_cmp);
    let mut resolved = resolved.to_vec();
    resolved.sort_by(|a, b| a.0.total_cmp(&b.0));
    let (mut kept, mut n, mut sfp) = (0, 0, 0);
    let mut ages = lives.clone();
    ages.dedup();
    for age in ages {
        while kept < lives.len() && lives[kept] <= age {
            kept += 1;
        }
        while n < resolved.len() && resolved[n].0 <= age {
            sfp += u64::from(resolved[n].1);
            n += 1;
        }
        let lost = (lives.len() - kept) as u64;
        if r2(n as u64, sfp, lost).3 {
            return Some(Ttl {
                age,
                lost,
                n: n as u64,
                sfp,
            });
        }
    }
    None
}

/// S5's alternatives, premise P, the TTL record and the growth record.
fn report_alternatives(run: &Run) {
    let uncapped = run.model(Params::UNCAPPED);
    let rules: Vec<Params> = CAPS
        .map(|cap| Params {
            max_active: Some(cap),
            ..Params::ENGINE
        })
        .into_iter()
        .chain(DISTANCES.map(|bps| Params {
            max_distance_bps: Some(bps),
            ..Params::UNTIL_SWEPT
        }))
        .chain([Params::UNTIL_SWEPT])
        .collect();
    println!(
        "S5: alternatives, active-list rule with max_resolved {MAX_LEVELS} (R2 per rule, not a gate)"
    );
    let mut candidates = Vec::new();
    for rule in rules {
        let model = run.model(rule);
        let mut evictions = 0;
        let mut passes = [false; 4];
        for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
            let stats = &model.timeframes[index].stats;
            let (sfp, breaks) = stats.resolved();
            let n = sfp + breaks;
            let lost = uncapped.timeframes[index].stats.sweeps() - stats.sweeps();
            let (p, share, bound, pass) = r2(n, sfp, lost);
            evictions += stats.evicted;
            passes[index] = pass;
            println!(
                "  {} {timeframe}: active evictions {}, lost {lost}; R2 n {n}, p {p:.4}, lost share \
                 {share:.4}, sqrt(p(1-p)/n) {bound:.4} -> alternative {}; active highs p99/max {}, \
                 lows {}",
                rule.rule(),
                stats.evicted,
                verdict(pass),
                quantiles(&stats.active[0], &[0.99, 1.0], 0),
                quantiles(&stats.active[1], &[0.99, 1.0], 0)
            );
        }
        if rule != Params::UNTIL_SWEPT {
            candidates.push(Candidate {
                rule: rule.rule(),
                evictions,
                passes,
            });
        }
    }
    let falsifying = premise_p(&candidates);
    if falsifying.is_empty() {
        println!("premise P: HOLDS");
    } else {
        println!("premise P: FAILS ({})", falsifying.join(", "));
    }

    println!("S5: TTL, the smallest age A* at which R2 passes (uncapped)");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &uncapped.timeframes[index].stats;
        match ttl_star(&joined(&stats.life, None), &stats.resolved_life) {
            Some(ttl) => {
                let days = ttl.age * timeframe.millis() as f64 / DAY_MS as f64;
                let (p, share, bound, _) = r2(ttl.n, ttl.sfp, ttl.lost);
                println!(
                    "  {timeframe}: A* {:.2} bars ({days:.2} days); lost {}, n {}, p {p:.4}, \
                     lost share {share:.4}, sqrt(p(1-p)/n) {bound:.4}",
                    ttl.age, ttl.lost, ttl.n
                );
            }
            None => println!("  {timeframe}: no age passes R2"),
        }
    }

    println!("S5: growth, the uncapped active-list maximum per UTC month of the bar's open");
    for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
        let stats = &uncapped.timeframes[index].stats;
        for (side, name) in [(0, "highs"), (1, "lows")] {
            let cells: Vec<String> = stats
                .monthly
                .iter()
                .map(|(month, max)| format!("{month} {}", max[side]))
                .collect();
            println!("  {timeframe} {name}: {}", cells.join(", "));
        }
    }
}

/// Days from `start` to the convergence of a record; `None`: not
/// converged, or no bar compared.
fn converged_days(record: &SkewRecord, start: EventTime) -> Option<f64> {
    record
        .converged
        .map(|end| (end.as_millis() - start.as_millis()) as f64 / DAY_MS as f64)
}

fn days_label(days: Option<f64>) -> String {
    days.map_or_else(|| "not converged".to_owned(), |days| format!("{days:.2}"))
}

/// The sum over every start, timeframe and side of the largest `missing`
/// under `until_swept`: the skew record is valid when it is 0.
fn until_swept_missing(run: &Run) -> u64 {
    run.skews
        .iter()
        .filter(|skew| skew.params == Params::UNTIL_SWEPT)
        .map(Skew::missing)
        .sum()
}

/// S6: the skew of the cold models against the full-window models.
fn report_skew(run: &Run) {
    if run.starts.is_empty() {
        return;
    }
    println!(
        "S6: live/replay skew, cold models against the full-window model of their rule, \
         after every closed bar from the start"
    );
    for skew in &run.skews {
        let rule = skew.params.rule();
        for (index, timeframe) in STRUCTURE_TIMEFRAMES.iter().enumerate() {
            for (side, name) in [(0, "highs"), (1, "lows")] {
                for (records, start) in skew.records.iter().zip(&run.starts) {
                    let record = &records[index][side];
                    let label = day_label(start.as_millis().div_euclid(DAY_MS));
                    if record.extra.is_empty() {
                        println!("  {rule} {timeframe} {name} from {label}: no bar compared");
                        continue;
                    }
                    let [extra, missing, extra_swept, missing_swept] = record.end;
                    let convergence = match converged_days(record, *start) {
                        Some(days) => format!("converged after {days:.2} days"),
                        None => "not converged".to_owned(),
                    };
                    println!(
                        "  {rule} {timeframe} {name} from {label}: extra p50/max {}, extra_swept \
                         p50/max {}; at the end extra {extra}, missing {missing}, extra_swept \
                         {extra_swept}, missing_swept {missing_swept}; missing max {}, \
                         missing_swept max {}; {convergence}",
                        quantiles(&record.extra, &[0.5, 1.0], 0),
                        quantiles(&record.extra_swept, &[0.5, 1.0], 0),
                        record.missing_max,
                        record.missing_swept_max
                    );
                }
            }
            // Per start the later side; a start that never converges ranks
            // after every one that does.
            let mut per_start: Vec<Option<f64>> = skew
                .records
                .iter()
                .zip(&run.starts)
                .map(|(records, start)| {
                    let [highs, lows] = &records[index];
                    Some(converged_days(highs, *start)?.max(converged_days(lows, *start)?))
                })
                .collect();
            per_start.sort_by(|a, b| match (a, b) {
                (Some(a), Some(b)) => a.total_cmp(b),
                (a, b) => a.is_none().cmp(&b.is_none()),
            });
            let never = per_start.iter().filter(|days| days.is_none()).count();
            let median = per_start[((per_start.len() - 1) as f64 * 0.5).round() as usize];
            println!(
                "  {rule} {timeframe} over {} starts: convergence days median {}, max {}; \
                 never converged {never}",
                per_start.len(),
                days_label(median),
                days_label(*per_start.last().expect("a start"))
            );
        }
    }
    let missing = until_swept_missing(run);
    println!(
        "S6: missing under until_swept {missing} -> {}",
        if missing == 0 { "VALID" } else { "VOID" }
    );
}

#[test]
#[ignore = "measures the archive for ADR-037; see the module docs"]
fn measure_structure() {
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
    let replay = ArchiveReplay::new(&store, "BTCUSDT", &[ArchiveStream::AggTrades]);
    let mut stream = replay.replay(window).unwrap().stream;
    // S6: 00:00 UTC on the 1st of every month strictly inside the window.
    let cold_starts: Vec<EventTime> = PeriodKind::Monthly
        .periods(from.div_euclid(DAY_MS), (to - 1).div_euclid(DAY_MS))
        .into_iter()
        .map(Period::start_ms)
        .filter(|start| *start > from && *start < to)
        .map(EventTime::from_millis)
        .collect();
    let started = Instant::now();
    let mut run = Run::new(from + (to - from) / 2, &cold_starts);
    while let Some(event) = stream.next_event().unwrap() {
        run.step(&event);
    }
    run.finish();
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
    assert_eq!(
        until_swept_missing(&run),
        0,
        "a cold until_swept model holds a level the full one lacks"
    );
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

/// About 40 days of trades — more than the 1d warm-up of `2N + 1` days —
/// around 100 000 USDT: a random walk with an uptrend (days 10–17) and a
/// downtrend (days 22–27) that pile up unswept levels beyond the cap, a
/// silent stretch of empty bars (day 30), a trades feed gap (day 33) and
/// outside spikes (days 35–37). Prices sit on a coarse grid so that ties between bar extremes, the case
/// `tie_rule` decides, occur.
fn synthetic_tape() -> Vec<MarketEvent> {
    let mut lcg = Lcg(0x0080_5eed);
    let mut events = Vec::new();
    let end = TAPE_START + 40 * DAY_MS;
    let mut time = TAPE_START + 123_456;
    // Cents.
    let mut price: i64 = 10_000_000;
    let mut trade_id = 0;
    let mut gapped = false;
    while time < end {
        let day = (time - TAPE_START) / DAY_MS;
        let drift = match day {
            10..=17 => 80,
            22..=27 => -80,
            _ => 0,
        };
        price += lcg.below(8_401) - 4_200 + drift;
        // Outside spikes, 1 500 USDT down and up (or up and down) within
        // a millisecond, sweep both sides in one bar (decision 6's order).
        let spike = (35..=37).contains(&day) && lcg.below(300) == 0;
        let offsets: &[i64] = match (spike, lcg.below(2)) {
            (false, _) => &[0],
            (true, 0) => &[-150_000, 150_000],
            (true, _) => &[150_000, -150_000],
        };
        for offset in offsets {
            trade_id += 1;
            let aggressor = if lcg.below(2) == 0 {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            };
            events.push(MarketEvent::Trade(Trade {
                time: EventTime::from_millis(time),
                trade_id,
                // On a 10 USDT grid, so equal bar extremes (ties) occur.
                price: Price::from_units((price + offset) / 1_000 * 1_000 * (SCALE / 100)),
                qty: Qty::from_units(1 + lcg.below(SCALE as u64)),
                aggressor,
            }));
            time += 1;
        }
        time += 1 + lcg.below(80_000);
        let silent = TAPE_START + 30 * DAY_MS + 2 * 3_600_000;
        if (silent..silent + 3 * 3_600_000).contains(&time) {
            time = silent + 3 * 3_600_000;
        }
        let gap = TAPE_START + 33 * DAY_MS + 10 * 3_600_000;
        if !gapped && time >= gap {
            gapped = true;
            let start = time;
            time += 2 * 3_600_000;
            events.push(MarketEvent::FeedGap(FeedGap {
                stream: Stream::Trades,
                start: EventTime::from_millis(start),
                end: EventTime::from_millis(time),
                reason: GapReason::Disconnected,
            }));
            time += 1;
        }
    }
    events
}

/// The tape's cold start, `TAPE_START` + 20 days: 00:00 UTC, a bar
/// boundary of every structure timeframe; after the day 10–17 uptrend, so
/// the full model holds unswept lows from before it that a cold model
/// lacks; before the day 22–27 downtrend, which piles highs past cap 20,
/// so the cold cap model evicts; and 20 days before the tape's end, more
/// than the 1d warm-up of `2N + 1` bars, so a fresh engine's 1d registry
/// becomes `Ready` and is compared.
const COLD_START: i64 = TAPE_START + 20 * DAY_MS;

#[test]
fn model_reproduces_the_engine_on_a_synthetic_tape() {
    let mut run = Run::new(
        TAPE_START + 20 * DAY_MS,
        &[EventTime::from_millis(COLD_START)],
    );
    for event in synthetic_tape() {
        run.step(&event);
    }
    run.finish();
    // The report runs too, so the archive run cannot fail on it.
    report(
        &run,
        TAPE_START,
        TAPE_START + 40 * DAY_MS,
        Duration::default(),
    );
    let check = &run.check;
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(check.mismatches, 0, "{:#?}", check.notes);
    // Every compared kind occurred, so R0 did not pass vacuously.
    let base = run.model(Params::ENGINE);
    let evicted: u64 = base.timeframes.iter().map(|tf| tf.stats.evicted).sum();
    let evicted_resolved: u64 = base
        .timeframes
        .iter()
        .map(|tf| tf.stats.evicted_resolved)
        .sum();
    let kinds = [
        ("swings", check.swings),
        ("sweeps of highs", check.sweeps[0]),
        ("sweeps of lows", check.sweeps[1]),
        ("SFPs", check.sfps),
        ("breaks", check.breaks),
        ("touched levels", check.touched),
        ("evicted active levels", evicted),
        ("evicted resolved sweeps", evicted_resolved),
    ];
    for (kind, count) in kinds {
        assert!(count > 0, "the tape compared no {kind}");
    }
}

/// A 15m bar opening at `open` with a trade, its prices in USDT.
fn usdt_bar(open: i64, high: i64, low: i64, close: i64) -> Bar {
    let usdt = |price: i64| Price::from_units(price * SCALE);
    Bar {
        ohlc: Some(Ohlc {
            open: usdt(close),
            high: usdt(high),
            low: usdt(low),
            close: usdt(close),
        }),
        trade_count: 1,
        ..Bar::empty(Timeframe::M15, EventTime::from_millis(open))
    }
}

/// An active level confirmed at the tape start.
fn tape_level(side: Side, price: Price) -> ModelLevel {
    let start = EventTime::from_millis(TAPE_START);
    ModelLevel {
        side,
        price,
        swing_time: start,
        confirmed_bar_end: start,
        known_at: start,
        touches: 0,
        touches_at: [0; TOLERANCES.len()],
    }
}

/// A full-window 15m model of `until_swept` with the distance threshold
/// `bps`.
fn distance_model(bps: i64) -> TimeframeModel {
    let params = Params {
        max_distance_bps: Some(bps),
        ..Params::UNTIL_SWEPT
    };
    TimeframeModel::new(Timeframe::M15, params, None)
}

/// At `D` = 10 000 a low retires when `(close - L) * 10 000 > close *
/// 10 000`, which no `L > 0` meets: the smallest positive price stays. A
/// high at three times the close is `2 * close` away and retires, which a
/// denominator of `|L|` (`2/3` of `L`) would not do.
#[test]
fn distance_10000_never_retires_a_low() {
    let mut model = distance_model(10_000);
    model.lows.push(tape_level(Side::Low, Price::from_units(1)));
    model
        .highs
        .push(tape_level(Side::High, Price::from_units(300_000 * SCALE)));
    let bar = usdt_bar(TAPE_START, 100_000, 100_000, 100_000);
    model
        .close(
            &bar,
            bar.end(),
            &Path::default(),
            TAPE_START,
            &mut Vec::new(),
        )
        .unwrap();
    assert_eq!(model.lows.len(), 1, "the low retired");
    assert!(model.highs.is_empty(), "the high at 3x the close stayed");
    assert_eq!(model.stats.evicted, 1);
}

/// A high at 101 000 and a low at 99 000 are both exactly 100 bps of a
/// 100 000 close away (`1 000 * 10 000 = 100 000 * 100`). The bar sweeps
/// neither. At `D` = 100 the test is not strictly beyond, so none retires;
/// at `D` = 99, below both, both retire at this close, each an eviction
/// recorded at its 100 bps.
#[test]
fn distance_retires_strictly_beyond_the_threshold() {
    for (bps, retired) in [(100, 0), (99, 2)] {
        let mut model = distance_model(bps);
        model
            .highs
            .push(tape_level(Side::High, Price::from_units(101_000 * SCALE)));
        model
            .lows
            .push(tape_level(Side::Low, Price::from_units(99_000 * SCALE)));
        let bar = usdt_bar(TAPE_START, 100_500, 99_500, 100_000);
        let mut facts = Vec::new();
        model
            .close(&bar, bar.end(), &Path::default(), TAPE_START, &mut facts)
            .unwrap();
        assert!(facts.is_empty(), "D = {bps}: the bar swept a level");
        assert_eq!(model.stats.evicted, retired, "D = {bps}");
        assert_eq!(
            model.highs.len() + model.lows.len(),
            2 - retired as usize,
            "D = {bps}"
        );
        assert_eq!(model.stats.evicted_bps.len(), retired as usize);
        for distance in &model.stats.evicted_bps {
            assert!((distance - 100.0).abs() < 1e-9, "D = {bps}: {distance}");
        }
        let recorded: usize = model.stats.touched.iter().map(Vec::len).sum();
        assert_eq!(recorded, retired as usize, "D = {bps}: touches recorded");
    }
}

#[test]
fn cold_model_matches_a_fresh_engine() {
    let start = EventTime::from_millis(COLD_START);
    let mut run = Run::new(COLD_START, &[start]).with_fresh(start);
    for event in synthetic_tape() {
        run.step(&event);
    }
    let fresh = run.fresh.as_ref().expect("a fresh engine");
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(fresh.rejections, 0, "the fresh engine rejected the tape");
    let check = &fresh.check;
    assert_eq!(check.mismatches, 0, "{:#?}", check.notes);
    // Every compared kind occurred, so the check did not pass vacuously.
    let evicted: u64 = run
        .cold(Params::ENGINE, start)
        .timeframes
        .iter()
        .map(|tf| tf.stats.evicted)
        .sum();
    let kinds = [
        ("swings", check.swings),
        ("sweeps of highs", check.sweeps[0]),
        ("sweeps of lows", check.sweeps[1]),
        ("SFPs", check.sfps),
        ("breaks", check.breaks),
        ("registries", check.registries),
        ("evicted active levels", evicted),
    ];
    for (kind, count) in kinds {
        assert!(count > 0, "the cold check compared no {kind}");
    }
}

#[test]
fn cold_until_swept_model_is_a_subset() {
    let start = EventTime::from_millis(COLD_START);
    let mut run = Run::new(COLD_START, &[start]);
    let mut lacking = 0;
    for event in synthetic_tape() {
        run.step(&event);
        for bar in run.engine.closed_bars() {
            let Some(index) = timeframe_index(bar.timeframe) else {
                continue;
            };
            let full = &run.model(Params::UNTIL_SWEPT).timeframes[index];
            let cold = &run.cold(Params::UNTIL_SWEPT, start).timeframes[index];
            if !cold.started {
                continue;
            }
            for (theirs, ours) in [(&full.highs, &cold.highs), (&full.lows, &cold.lows)] {
                for level in ours {
                    let same = theirs.iter().find(|other| {
                        other.price == level.price && other.swing_time == level.swing_time
                    });
                    assert!(
                        same.is_some_and(|other| other.touches == level.touches),
                        "{} {}: cold level {level:?} not in the full model",
                        bar.timeframe,
                        bar.open_time
                    );
                }
                if theirs.len() > ours.len() {
                    lacking += 1;
                }
            }
        }
    }
    assert!(
        lacking > 0,
        "the full model never held a level the cold one lacked"
    );
    // The per-bar S6 record agrees.
    assert_eq!(until_swept_missing(&run), 0);
}

/// Sweep lives 1, 2, 3, 4 and 10 bars; the first four resolved as SFP,
/// break, SFP, break, the last still pending. At age `A`, `lost` counts the
/// lives `> A`, `n` and `p` the resolved lives `<= A`:
/// - `A` = 1: lost 4, n 1, p 1, bound 0, share 4/5: FAIL;
/// - `A` = 2: lost 3, n 2, p 0.5, bound 0.354, share 0.6: FAIL;
/// - `A` = 3: lost 2, n 3, p 2/3, bound 0.272, share 0.4: FAIL;
/// - `A` = 4: lost 1, n 4, p 0.5, bound 0.25, share 0.2: PASS, so `A*` = 4
///   (`A` = 10 passes too, with nothing lost, but is not the smallest).
///
/// The inputs are unsorted, so the scan cannot rely on their order.
/// Without a resolved sweep `p` is undefined and no age passes.
#[test]
fn ttl_star_is_the_smallest_passing_age() {
    let lives = [10.0, 3.0, 1.0, 4.0, 2.0];
    let resolved = [(4.0, false), (1.0, true), (3.0, true), (2.0, false)];
    assert_eq!(
        ttl_star(&lives, &resolved),
        Some(Ttl {
            age: 4.0,
            lost: 1,
            n: 4,
            sfp: 2,
        })
    );
    assert_eq!(ttl_star(&[1.0], &[]), None);
}

/// A candidate falsifies P only when it evicts at least one active level
/// and passes R2 on all four timeframes: one that evicts nothing passes R2
/// trivially (it is the uncapped registry) and is ignored, one that fails
/// R2 on a timeframe does not count.
#[test]
fn premise_p_ignores_candidates_that_retire_nothing() {
    let candidate = |rule: &str, evictions, passes| Candidate {
        rule: rule.to_owned(),
        evictions,
        passes,
    };
    let holds = [
        candidate("distance 10000 bps", 0, [true; 4]),
        candidate("cap 20", 120, [false, false, true, true]),
        candidate("cap 160", 1, [true, true, true, false]),
    ];
    assert!(premise_p(&holds).is_empty());
    let mut fails = holds.to_vec();
    fails.push(candidate("cap 80", 1, [true; 4]));
    assert_eq!(premise_p(&fails), ["cap 80"]);
}
