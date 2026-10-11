//! The ADR-037 measurements (issue #80): an independent model of the
//! ADR-037 decision text, run next to one `MarketStateEngine`, with the swing
//! size `N`, the SFP window `K`, the touch tolerance and the cap as runtime
//! parameters.
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
//!   list size, evictions, `lost` at caps 10, 20 and 40, the evicted levels'
//!   distance from the last close and the resolved list's history depth.
//!
//! The rules are evaluated over the whole window; the test asserts R0 only
//! and prints R1–R3 as PASS or FAIL, so a failing rule still prints
//! everything. Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! structure_measure -- --ignored --nocapture`; `MIE_MEASURE_TO` is
//! included, as in `mie replay`. An optional `MIE_MEASURE_EVENTS` asserts
//! the replay's event count (R0).
//!
//! `model_reproduces_the_engine_on_a_synthetic_tape` runs the same R0
//! comparison in CI on a seeded tape, and asserts that every compared kind
//! occurred, so R0 cannot pass vacuously.

use mie_adapter_binance::archive::ArchiveStream;
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

/// The caps S5 compares.
const CAPS: [usize; 3] = [10, 20, 40];

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
    /// `None`: an uncapped registry.
    max_levels: Option<usize>,
}

impl Params {
    /// The engine's parameters, read from its public constants (R0).
    const ENGINE: Self = Self {
        swing_bars: SWING_BARS,
        sfp_window_bars: SFP_WINDOW_BARS,
        touch_tolerance_bps: TOUCH_TOLERANCE_BPS,
        max_levels: Some(MAX_LEVELS),
    };
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
    /// Active levels and resolved sweeps evicted by the cap.
    evicted: u64,
    evicted_resolved: u64,
    /// Distance of evicted active levels from the last close, in bps.
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
    stats: Stats,
}

impl TimeframeModel {
    fn new(timeframe: Timeframe, params: Params) -> Self {
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
            stats: Stats::default(),
        }
    }

    fn half(time: EventTime, mid: i64) -> usize {
        usize::from(time.as_millis() >= mid)
    }

    fn retire(&mut self, level: &ModelLevel, mid: i64) {
        self.stats.touched[Self::half(level.known_at, mid)].push(level.touches_at);
    }

    /// Takes one closed bar, closed by the event at `known_at`: the bar's
    /// sweeps, then resolutions, touches and swing confirmations
    /// (decisions 2–8).
    fn close(
        &mut self,
        bar: &Bar,
        known_at: EventTime,
        path: &Path,
        mid: i64,
        facts: &mut Vec<Fact>,
    ) -> Result<(), String> {
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
        self.stats.active[0].push(self.highs.len() as f64);
        self.stats.active[1].push(self.lows.len() as f64);
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
            if let Some(cap) = self.params.max_levels
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
        let cap = self.params.max_levels;
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
            self.stats.evicted += 1;
            if let Some(close) = self.last_close {
                let close = close.units() as f64;
                let distance = (evicted.price.units() as f64 - close).abs() * 10_000.0 / close;
                self.stats.evicted_bps.push(distance);
            }
            self.retire(&evicted, mid);
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

/// One parameter set over every structure timeframe.
struct Model {
    params: Params,
    timeframes: [TimeframeModel; 4],
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
    /// `models[0]` has the engine's parameters.
    models: Vec<Model>,
    check: Check,
    events: u64,
    rejections: u64,
}

impl Run {
    fn new(mid: i64) -> Self {
        let mut params = vec![Params::ENGINE];
        for swing_bars in SWING_SIZES {
            params.push(Params {
                swing_bars,
                max_levels: None,
                ..Params::ENGINE
            });
        }
        for cap in CAPS {
            let capped = Params {
                max_levels: Some(cap),
                ..Params::ENGINE
            };
            if !params.contains(&capped) {
                params.push(capped);
            }
        }
        Self {
            mid,
            engine: MarketStateEngine::new(),
            paths: Default::default(),
            models: params
                .into_iter()
                .map(|params| Model {
                    params,
                    timeframes: STRUCTURE_TIMEFRAMES
                        .map(|timeframe| TimeframeModel::new(timeframe, params)),
                })
                .collect(),
            check: Check::default(),
            events: 0,
            rejections: 0,
        }
    }

    fn model(&self, params: Params) -> &Model {
        self.models
            .iter()
            .find(|model| model.params == params)
            .expect("a model of these parameters")
    }

    fn step(&mut self, event: &MarketEvent) {
        self.events += 1;
        if self.engine.apply(event).is_err() {
            self.rejections += 1;
            return;
        }
        let known_at = event.time();
        let mut facts = Vec::new();
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
            }
        }
        if closed.contains(&true) || !self.engine.structure_events().is_empty() {
            self.check
                .compare(&self.engine, known_at, &facts, closed, &self.models[0]);
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
    let uncapped = run.model(Params {
        max_levels: None,
        ..Params::ENGINE
    });
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
                    max_levels: None,
                    ..Params::ENGINE
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
                    max_levels: None,
                    ..Params::ENGINE
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
                        max_levels: Some(*cap),
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
    let started = Instant::now();
    let mut run = Run::new(from + (to - from) / 2);
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

#[test]
fn model_reproduces_the_engine_on_a_synthetic_tape() {
    let mut run = Run::new(TAPE_START + 20 * DAY_MS);
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
