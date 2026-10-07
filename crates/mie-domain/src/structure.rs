//! Market structure: swing highs and lows, the structural level registry,
//! sweeps and swing-failure patterns (brief §8 and §10; Market State &
//! Regime brief; ADR-037, proposed).
//!
//! Structure is built independently on the closed bars of every timeframe
//! in [`STRUCTURE_TIMEFRAMES`] (decision 1):
//!
//! - A **swing** is a fractal with [`SWING_BARS`] bars on each side
//!   (decision 2). It is known only when the last of those bars closes, so
//!   the confirmation delay is part of the definition: a swing is emitted by
//!   the event that closes that bar, never earlier (decision 3).
//! - Every confirmed swing enters its timeframe's **level registry** as an
//!   active structural high or low, which counts its **touches**
//!   (decision 5).
//! - A **sweep** is the first trade strictly beyond an active level
//!   (decision 6). Within [`SFP_WINDOW_BARS`] bars it resolves as a
//!   **swing-failure pattern** — a close back at or inside the level — or as
//!   a **clean break** (decision 7).
//!
//! Each timeframe has two features (decision 10): `structure.swing.<tf>@1`
//! ([`LastSwings`]) and `structure.levels.<tf>@1` ([`LevelRegistry`]). The
//! engine also exposes the [`StructureEvent`]s of every event, in
//! processing order (decision 8). Everything is exact integer arithmetic on
//! fixed point (ADR-027, decision 9): no float is computed or stored.
//!
//! A sweep or an SFP is a structure fact, never a signal: its trigger rule
//! belongs to the bias/trigger issue (ADR-012, ADR-024).

use crate::bars::{Bar, Coverage, Ohlc, Timeframe};
use crate::event::MarketEvent;
use crate::feature::{FeatureDefinition, FeatureKey, FeatureValue, catalog};
use crate::location::LevelKind;
use crate::num::Price;
use crate::time::EventTime;
use std::collections::VecDeque;
use std::fmt;

/// The timeframes structure is built on, shortest first (decision 1).
pub const STRUCTURE_TIMEFRAMES: [Timeframe; 4] =
    [Timeframe::M15, Timeframe::H1, Timeframe::H4, Timeframe::D1];

/// Bars on each side of a swing: parameter `swing_bars` (decision 2).
pub const SWING_BARS: usize = 3;

/// The closed bars one swing reads, `2N + 1`: the warm-up of both features
/// (decision 3).
pub const SWING_WINDOW: usize = 2 * SWING_BARS + 1;

/// Bars of the SFP window, the sweep bar included: parameter
/// `sfp_window_bars` (decision 7).
pub const SFP_WINDOW_BARS: u32 = 2;

/// How near a bar's extreme must come to a level to touch it, in basis
/// points of the level: parameter `touch_tolerance_bps` (decision 5).
pub const TOUCH_TOLERANCE_BPS: i64 = 5;

/// The most entries of each registry list — active highs, active lows,
/// resolved sweeps: parameter `max_levels` (decision 5).
pub const MAX_LEVELS: usize = 20;

/// Which extreme a swing or level is.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Side {
    /// A swing high: a level above price.
    High,
    /// A swing low: a level below price.
    Low,
}

impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::High => "high",
            Self::Low => "low",
        })
    }
}

/// A confirmed swing (decisions 2–4).
///
/// `Display` prints the canonical line the golden tests pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Swing {
    /// The feature that produced it: `structure.swing.<tf>@1`.
    pub feature: FeatureKey,
    /// The timeframe of its bars.
    pub timeframe: Timeframe,
    /// High or low.
    pub side: Side,
    /// The swing bar's high (or low).
    pub price: Price,
    /// Open time of the swing bar.
    pub swing_time: EventTime,
    /// End of the [`SWING_BARS`]-th bar after the swing bar, the bar that
    /// confirms the swing (decision 3). A bar time, not a visibility time:
    /// the swing is known only from [`Self::known_at`] on.
    pub confirmed_bar_end: EventTime,
    /// Time of the event that emitted the swing: the trades-stream event
    /// that closed the confirming bar (ADR-031), at or after
    /// `confirmed_bar_end` — later by a whole outage across a silent trades
    /// gap. The swing's visibility time (decision 8).
    pub known_at: EventTime,
    /// The OR of the coverage of the [`SWING_WINDOW`] bars it read.
    pub coverage: Coverage,
}

impl fmt::Display for Swing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} price={} bar={} confirmed={} {} {}",
            self.side,
            self.price,
            self.swing_time,
            self.confirmed_bar_end,
            self.coverage,
            self.feature
        )
    }
}

/// The last confirmed swing high and swing low of one timeframe: the value
/// of `structure.swing.<tf>@1`. Each is `None` until the first of its side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LastSwings {
    /// The last swing high.
    pub high: Option<Swing>,
    /// The last swing low.
    pub low: Option<Swing>,
}

/// How a sweep resolved (decision 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SweepOutcome {
    /// The SFP window is still open.
    Pending,
    /// A window bar closed back at or inside the level: a swing-failure
    /// pattern.
    Sfp,
    /// The whole window closed beyond the level: a clean break.
    Break,
}

impl fmt::Display for SweepOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Pending => "pending",
            Self::Sfp => "sfp",
            Self::Break => "break",
        })
    }
}

/// A structural level: a confirmed swing, with its touches (decision 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Level {
    /// High or low.
    pub side: Side,
    /// The swing's price.
    pub price: Price,
    /// Open time of the swing bar.
    pub swing_time: EventTime,
    /// End of the bar that confirmed the swing (decision 3).
    pub confirmed_bar_end: EventTime,
    /// Closed bars that came within [`TOUCH_TOLERANCE_BPS`] of the level
    /// while it was active.
    pub touches: u32,
    /// The swing's coverage.
    pub coverage: Coverage,
}

/// A level a trade went beyond, and how that resolved (decisions 6 and 7).
///
/// `Display` prints the canonical line the golden tests pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sweep {
    /// The feature that produced it: `structure.levels.<tf>@1`.
    pub feature: FeatureKey,
    /// The level's timeframe.
    pub timeframe: Timeframe,
    /// The level as it was when swept.
    pub level: Level,
    /// Time of the sweeping trade.
    pub time: EventTime,
    /// Price of the sweeping trade.
    pub price: Price,
    /// The farthest price beyond the level so far: the sweeping trade's
    /// price, then the highest high (lowest low) of the window bars read.
    pub extreme: Price,
    /// Window bars closed so far, the sweep bar included.
    pub window_bars: u32,
    /// The outcome.
    pub outcome: SweepOutcome,
    /// End of the resolving bar; `None` while pending. A bar time, not a
    /// visibility time.
    pub resolved_bar_end: Option<EventTime>,
    /// Time of the event that emitted the sweep's latest fact, its
    /// visibility time (decision 8): the sweeping trade while pending; once
    /// resolved, the trades-stream event that closed the resolving bar
    /// (ADR-031), at or after `resolved_bar_end`.
    pub known_at: EventTime,
    /// The level's coverage OR that of the window bars read.
    pub coverage: Coverage,
}

impl fmt::Display for Sweep {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = &self.level;
        write!(
            f,
            "{} level={} bar={} confirmed={} touches={} at={} price={} extreme={} bars={} {}",
            level.side,
            level.price,
            level.swing_time,
            level.confirmed_bar_end,
            level.touches,
            self.time,
            self.price,
            self.extreme,
            self.window_bars,
            self.outcome
        )?;
        if let Some(resolved) = self.resolved_bar_end {
            write!(f, " resolved={resolved}")?;
        }
        write!(f, " {} {}", self.coverage, self.feature)
    }
}

/// A level for location (brief §10): the hand-off to the level registry of
/// #23. Its zone is `[low, high]`; only an SFP rejection zone is wider than
/// its price.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StructureLevel {
    /// [`LevelKind::StructuralHigh`], [`LevelKind::StructuralLow`],
    /// [`LevelKind::PriorSweep`] or [`LevelKind::SfpRejectionZone`].
    pub kind: LevelKind,
    /// The swing's price.
    pub price: Price,
    /// Lower edge of the zone.
    pub low: Price,
    /// Upper edge of the zone.
    pub high: Price,
    /// The registry that produced the level: `structure.levels.<tf>@1`.
    pub source: FeatureKey,
    /// The timeframe of the swing.
    pub timeframe: Timeframe,
    /// High or low.
    pub side: Side,
    /// Open time of the swing bar.
    pub swing_time: EventTime,
    /// When the level took its kind: the confirming bar's end for a
    /// structural level, the sweeping trade for a prior sweep, the resolving
    /// bar's end for an SFP rejection zone.
    pub created_at: EventTime,
    /// Touches while the level was active.
    pub touches: u32,
    /// The sweep's outcome; `None` for an active structural level.
    pub outcome: Option<SweepOutcome>,
    /// Coverage of the inputs.
    pub coverage: Coverage,
}

impl StructureLevel {
    /// Milliseconds from `created_at` to `now`, derived on demand
    /// (decision 5); negative if `now` comes first, saturating at the `i64`
    /// range.
    pub fn age_ms(&self, now: EventTime) -> i64 {
        now.as_millis().saturating_sub(self.created_at.as_millis())
    }
}

/// The structural levels of one timeframe: the value of
/// `structure.levels.<tf>@1` (decisions 5–7).
///
/// `Display` prints the canonical line the golden tests pin: active levels
/// as `price/touches`, pending sweeps as `side:level`, resolved ones as
/// `side:level:outcome:extreme`, then the feature key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LevelRegistry {
    feature: FeatureKey,
    timeframe: Timeframe,
    /// Active highs by `(price, confirmed_bar_end)`.
    highs: Vec<Level>,
    /// Active lows by `(price, confirmed_bar_end)`.
    lows: Vec<Level>,
    /// Sweeps whose window is open, in sweep order.
    pending: Vec<Sweep>,
    /// Resolved sweeps, in resolution order.
    resolved: Vec<Sweep>,
}

impl LevelRegistry {
    fn new(feature: FeatureKey, timeframe: Timeframe) -> Self {
        Self {
            feature,
            timeframe,
            highs: Vec::new(),
            lows: Vec::new(),
            pending: Vec::new(),
            resolved: Vec::new(),
        }
    }

    /// The feature: `structure.levels.<tf>@1`.
    pub fn feature(&self) -> FeatureKey {
        self.feature
    }

    /// The timeframe.
    pub fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// Active (unswept) highs, by price ascending.
    pub fn highs(&self) -> &[Level] {
        &self.highs
    }

    /// Active (unswept) lows, by price ascending.
    pub fn lows(&self) -> &[Level] {
        &self.lows
    }

    /// Sweeps whose SFP window is still open, in sweep order.
    pub fn pending(&self) -> &[Sweep] {
        &self.pending
    }

    /// The latest resolved sweeps, at most [`MAX_LEVELS`], in resolution
    /// order.
    pub fn resolved(&self) -> &[Sweep] {
        &self.resolved
    }

    /// The registry's levels for location: every active high
    /// ([`LevelKind::StructuralHigh`]), every active low
    /// ([`LevelKind::StructuralLow`]), every swept level, pending or
    /// resolved ([`LevelKind::PriorSweep`]), then every SFP's rejection zone
    /// ([`LevelKind::SfpRejectionZone`]); each group by price ascending,
    /// then by creation.
    pub fn levels(&self) -> impl Iterator<Item = StructureLevel> + '_ {
        let mut swept: Vec<&Sweep> = self.pending.iter().chain(&self.resolved).collect();
        swept.sort_by_key(|sweep| (sweep.level.price, sweep.time));
        let mut zones: Vec<&Sweep> = self
            .resolved
            .iter()
            .filter(|sweep| sweep.outcome == SweepOutcome::Sfp)
            .collect();
        zones.sort_by_key(|sweep| (sweep.level.price, sweep.resolved_bar_end));
        let level =
            move |kind, level: &Level, low, high, created_at, outcome, coverage| StructureLevel {
                kind,
                price: level.price,
                low,
                high,
                source: self.feature,
                timeframe: self.timeframe,
                side: level.side,
                swing_time: level.swing_time,
                created_at,
                touches: level.touches,
                outcome,
                coverage,
            };
        let active = self.highs.iter().chain(&self.lows).map(move |active| {
            let kind = match active.side {
                Side::High => LevelKind::StructuralHigh,
                Side::Low => LevelKind::StructuralLow,
            };
            level(
                kind,
                active,
                active.price,
                active.price,
                active.confirmed_bar_end,
                None,
                active.coverage,
            )
        });
        let prior = swept.into_iter().map(move |sweep| {
            level(
                LevelKind::PriorSweep,
                &sweep.level,
                sweep.level.price,
                sweep.level.price,
                sweep.time,
                Some(sweep.outcome),
                sweep.coverage,
            )
        });
        let rejection = zones.into_iter().map(move |sweep| {
            let price = sweep.level.price;
            let (low, high) = match sweep.level.side {
                Side::High => (price, sweep.extreme),
                Side::Low => (sweep.extreme, price),
            };
            level(
                LevelKind::SfpRejectionZone,
                &sweep.level,
                low,
                high,
                sweep.resolved_bar_end.unwrap_or(sweep.time),
                Some(sweep.outcome),
                sweep.coverage,
            )
        });
        active.chain(prior).chain(rejection)
    }

    /// Whether a trade at `price` goes beyond an active level: one
    /// comparison per side, against the lowest high and the highest low.
    fn swept_by(&self, price: Price) -> bool {
        self.highs.first().is_some_and(|high| price > high.price)
            || self.lows.last().is_some_and(|low| price < low.price)
    }

    /// Adds a confirmed level, evicting the oldest of its list when full.
    fn add(&mut self, level: Level) {
        let list = match level.side {
            Side::High => &mut self.highs,
            Side::Low => &mut self.lows,
        };
        if list.len() >= MAX_LEVELS
            && let Some(oldest) = oldest(list, |level| (level.confirmed_bar_end, level.price))
        {
            list.remove(oldest);
        }
        let key = (level.price, level.confirmed_bar_end);
        let at = list.partition_point(|other| (other.price, other.confirmed_bar_end) <= key);
        list.insert(at, level);
    }

    /// Records a resolved sweep, evicting the oldest when full.
    fn push_resolved(&mut self, sweep: Sweep) {
        if self.resolved.len() >= MAX_LEVELS
            && let Some(oldest) = oldest(&self.resolved, |sweep| (sweep.time, sweep.level.price))
        {
            self.resolved.remove(oldest);
        }
        self.resolved.push(sweep);
    }
}

/// The index of the entry with the smallest `key`; the first on ties.
fn oldest<T, K: Ord>(list: &[T], key: impl Fn(&T) -> K) -> Option<usize> {
    list.iter()
        .enumerate()
        .min_by_key(|(index, entry)| (key(entry), *index))
        .map(|(index, _)| index)
}

/// Writes `items` separated by commas, or `-` for none.
fn write_list<T>(
    f: &mut fmt::Formatter<'_>,
    items: &[T],
    item: impl Fn(&mut fmt::Formatter<'_>, &T) -> fmt::Result,
) -> fmt::Result {
    if items.is_empty() {
        return f.write_str("-");
    }
    for (index, entry) in items.iter().enumerate() {
        if index > 0 {
            f.write_str(",")?;
        }
        item(f, entry)?;
    }
    Ok(())
}

impl fmt::Display for LevelRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let active = |f: &mut fmt::Formatter<'_>, level: &Level| {
            write!(f, "{}/{}", level.price, level.touches)
        };
        f.write_str("highs=")?;
        write_list(f, &self.highs, active)?;
        f.write_str(" lows=")?;
        write_list(f, &self.lows, active)?;
        f.write_str(" pending=")?;
        write_list(f, &self.pending, |f, sweep| {
            write!(f, "{}:{}", sweep.level.side, sweep.level.price)
        })?;
        f.write_str(" swept=")?;
        write_list(f, &self.resolved, |f, sweep| {
            write!(
                f,
                "{}:{}:{}:{}",
                sweep.level.side, sweep.level.price, sweep.outcome, sweep.extreme
            )
        })?;
        write!(f, " {}", self.feature)
    }
}

/// A structure fact, in processing order (decision 8).
///
/// `Display` prints the canonical line the golden tests pin: the kind, the
/// timeframe, then the swing or sweep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructureEvent {
    /// A swing was confirmed; its level is now active.
    Swing(Swing),
    /// A trade went beyond an active level.
    Sweep(Sweep),
    /// A sweep resolved as a swing-failure pattern.
    Sfp(Sweep),
    /// A sweep resolved as a clean break.
    Break(Sweep),
}

impl StructureEvent {
    /// When the fact became visible (decision 8): its `known_at`, the time
    /// of the event that emitted it — the sweeping trade for a sweep, the
    /// event that closed the confirming or resolving bar otherwise. Never
    /// the bar end, which can precede emission by a whole trades outage.
    pub fn time(&self) -> EventTime {
        match self {
            Self::Swing(swing) => swing.known_at,
            Self::Sweep(sweep) | Self::Sfp(sweep) | Self::Break(sweep) => sweep.known_at,
        }
    }

    /// The feature that produced the fact.
    pub fn feature(&self) -> FeatureKey {
        match self {
            Self::Swing(swing) => swing.feature,
            Self::Sweep(sweep) | Self::Sfp(sweep) | Self::Break(sweep) => sweep.feature,
        }
    }

    /// The timeframe of the fact's bars.
    pub fn timeframe(&self) -> Timeframe {
        match self {
            Self::Swing(swing) => swing.timeframe,
            Self::Sweep(sweep) | Self::Sfp(sweep) | Self::Break(sweep) => sweep.timeframe,
        }
    }
}

impl fmt::Display for StructureEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Swing(swing) => write!(f, "swing {} {swing}", swing.timeframe),
            Self::Sweep(sweep) => write!(f, "sweep {} {sweep}", sweep.timeframe),
            Self::Sfp(sweep) => write!(f, "sfp {} {sweep}", sweep.timeframe),
            Self::Break(sweep) => write!(f, "break {} {sweep}", sweep.timeframe),
        }
    }
}

/// The structure features of one timeframe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TimeframeStructure {
    /// `structure.swing.<tf>@1`, warming up over [`SWING_WINDOW`] closed
    /// bars.
    pub swings: FeatureValue<LastSwings>,
    /// `structure.levels.<tf>@1`, warming up over [`SWING_WINDOW`] closed
    /// bars.
    pub levels: FeatureValue<LevelRegistry>,
}

/// The value of a structure feature before its timeframe has closed `bars`
/// bars.
fn warming<T>(bars: u64) -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: bars,
        required: SWING_WINDOW as u64,
    }
}

/// The market structure of the Market State (ADR-037): the structure
/// features of every timeframe in [`STRUCTURE_TIMEFRAMES`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StructureSet {
    timeframes: [TimeframeStructure; 4],
}

impl Default for StructureSet {
    fn default() -> Self {
        Self::new()
    }
}

impl StructureSet {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            timeframes: STRUCTURE_TIMEFRAMES.map(|_| TimeframeStructure {
                swings: warming(0),
                levels: warming(0),
            }),
        }
    }

    /// The structure of `timeframe`, if structure is built on it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&TimeframeStructure> {
        STRUCTURE_TIMEFRAMES
            .iter()
            .position(|candidate| *candidate == timeframe)
            .map(|index| &self.timeframes[index])
    }

    /// Every timeframe's structure, shortest first.
    pub fn iter(&self) -> impl Iterator<Item = (Timeframe, &TimeframeStructure)> {
        STRUCTURE_TIMEFRAMES.into_iter().zip(&self.timeframes)
    }
}

/// The OR of two coverages.
fn or(a: Coverage, b: Coverage) -> Coverage {
    Coverage {
        partial_start: a.partial_start || b.partial_start,
        feed_gap: a.feed_gap || b.feed_gap,
    }
}

/// Whether `ohlc` touches `level` (decision 5): its extreme at or inside
/// the level and within [`TOUCH_TOLERANCE_BPS`] of it, exact in `i128`.
fn touches(level: &Level, ohlc: &Ohlc) -> bool {
    let price = i128::from(level.price.units());
    let tolerance = price.abs() * i128::from(TOUCH_TOLERANCE_BPS);
    let distance = match level.side {
        Side::High => price - i128::from(ohlc.high.units()),
        Side::Low => i128::from(ohlc.low.units()) - price,
    };
    distance >= 0 && distance * 10_000 <= tolerance
}

/// The structure state of one timeframe.
#[derive(Debug, Clone, PartialEq, Eq)]
struct TimeframeTracker {
    /// `structure.swing.<tf>@1`.
    swing_feature: FeatureKey,
    /// The last closed bars, at most [`SWING_WINDOW`], oldest first.
    window: VecDeque<Bar>,
    /// Bars closed since the start.
    closed_bars: u64,
    swings: LastSwings,
    registry: LevelRegistry,
}

impl TimeframeTracker {
    fn new(timeframe: Timeframe, swing: &FeatureDefinition, levels: &FeatureDefinition) -> Self {
        Self {
            swing_feature: swing.key,
            window: VecDeque::with_capacity(SWING_WINDOW),
            closed_bars: 0,
            swings: LastSwings {
                high: None,
                low: None,
            },
            registry: LevelRegistry::new(levels.key, timeframe),
        }
    }

    fn timeframe(&self) -> Timeframe {
        self.registry.timeframe
    }

    fn values(&self) -> TimeframeStructure {
        if self.closed_bars < SWING_WINDOW as u64 {
            TimeframeStructure {
                swings: warming(self.closed_bars),
                levels: warming(self.closed_bars),
            }
        } else {
            TimeframeStructure {
                swings: FeatureValue::Ready(self.swings),
                levels: FeatureValue::Ready(self.registry.clone()),
            }
        }
    }

    /// Takes one closed bar of the timeframe, closed by an event at
    /// `known_at`: resolutions, then touches, then swing confirmations
    /// (decision 8).
    fn close(
        &mut self,
        bar: &Bar,
        known_at: EventTime,
        events: &mut Vec<StructureEvent>,
    ) -> Result<(), StructureError> {
        self.resolve(bar, known_at, events)?;
        self.touch(bar)?;
        self.confirm(bar, known_at, events)
    }

    /// Steps every pending sweep's SFP window with `bar` (decision 7), in
    /// sweep order.
    fn resolve(
        &mut self,
        bar: &Bar,
        known_at: EventTime,
        events: &mut Vec<StructureEvent>,
    ) -> Result<(), StructureError> {
        if self.registry.pending.is_empty() {
            return Ok(());
        }
        for mut sweep in std::mem::take(&mut self.registry.pending) {
            sweep.window_bars = sweep
                .window_bars
                .checked_add(1)
                .ok_or(StructureError::Overflow)?;
            sweep.coverage = or(sweep.coverage, bar.coverage);
            let level = sweep.level.price;
            let inside = match (bar.ohlc, sweep.level.side) {
                (Some(ohlc), Side::High) => {
                    sweep.extreme = sweep.extreme.max(ohlc.high);
                    ohlc.close <= level
                }
                (Some(ohlc), Side::Low) => {
                    sweep.extreme = sweep.extreme.min(ohlc.low);
                    ohlc.close >= level
                }
                // An empty bar counts toward the window and never closes
                // inside.
                (None, _) => false,
            };
            if !inside && sweep.window_bars < SFP_WINDOW_BARS {
                self.registry.pending.push(sweep);
                continue;
            }
            sweep.resolved_bar_end = Some(bar.end());
            sweep.known_at = known_at;
            let event = if inside {
                sweep.outcome = SweepOutcome::Sfp;
                StructureEvent::Sfp(sweep)
            } else {
                sweep.outcome = SweepOutcome::Break;
                StructureEvent::Break(sweep)
            };
            events.push(event);
            self.registry.push_resolved(sweep);
        }
        Ok(())
    }

    /// Counts `bar` as a touch of every active level it comes near
    /// (decision 5).
    fn touch(&mut self, bar: &Bar) -> Result<(), StructureError> {
        let Some(ohlc) = &bar.ohlc else {
            return Ok(());
        };
        let registry = &mut self.registry;
        for level in registry.highs.iter_mut().chain(registry.lows.iter_mut()) {
            if bar.open_time >= level.confirmed_bar_end && touches(level, ohlc) {
                level.touches = level
                    .touches
                    .checked_add(1)
                    .ok_or(StructureError::Overflow)?;
            }
        }
        Ok(())
    }

    /// Adds `bar` to the window and confirms the swings of its middle bar
    /// (decisions 2 and 3): the high first, then the low.
    fn confirm(
        &mut self,
        bar: &Bar,
        known_at: EventTime,
        events: &mut Vec<StructureEvent>,
    ) -> Result<(), StructureError> {
        self.closed_bars = self
            .closed_bars
            .checked_add(1)
            .ok_or(StructureError::Overflow)?;
        if self.window.len() == SWING_WINDOW {
            self.window.pop_front();
        }
        self.window.push_back(*bar);
        if self.window.len() < SWING_WINDOW {
            return Ok(());
        }
        let candidate = self.window[SWING_BARS];
        let Some(ohlc) = candidate.ohlc else {
            return Ok(());
        };
        // Bars without trades neither qualify nor disqualify a swing.
        let left = || self.window.range(..SWING_BARS).filter_map(|bar| bar.ohlc);
        let right = || {
            self.window
                .range(SWING_BARS + 1..)
                .filter_map(|bar| bar.ohlc)
        };
        let high = left().all(|other| ohlc.high > other.high)
            && right().all(|other| ohlc.high >= other.high);
        let low =
            left().all(|other| ohlc.low < other.low) && right().all(|other| ohlc.low <= other.low);
        let coverage = self
            .window
            .iter()
            .fold(Coverage::default(), |coverage, bar| {
                or(coverage, bar.coverage)
            });
        for (side, price, confirmed) in [(Side::High, ohlc.high, high), (Side::Low, ohlc.low, low)]
        {
            if !confirmed {
                continue;
            }
            let swing = Swing {
                feature: self.swing_feature,
                timeframe: self.timeframe(),
                side,
                price,
                swing_time: candidate.open_time,
                confirmed_bar_end: bar.end(),
                known_at,
                coverage,
            };
            match side {
                Side::High => self.swings.high = Some(swing),
                Side::Low => self.swings.low = Some(swing),
            }
            self.registry.add(Level {
                side,
                price,
                swing_time: swing.swing_time,
                confirmed_bar_end: swing.confirmed_bar_end,
                touches: 0,
                coverage,
            });
            events.push(StructureEvent::Swing(swing));
        }
        Ok(())
    }

    /// Sweeps every active level a trade at `price` goes beyond (decision
    /// 6): highs by price ascending, lows by price descending, then by
    /// confirmation.
    fn sweep(&mut self, time: EventTime, price: Price, events: &mut Vec<StructureEvent>) {
        let registry = &mut self.registry;
        let swept_highs = registry.highs.partition_point(|level| level.price < price);
        let first_low = registry.lows.partition_point(|level| level.price <= price);
        if swept_highs == 0 && first_low == registry.lows.len() {
            return;
        }
        let mut swept: Vec<Level> = registry.highs.drain(..swept_highs).collect();
        let mut lows: Vec<Level> = registry.lows.drain(first_low..).collect();
        lows.sort_by(|a, b| {
            b.price
                .cmp(&a.price)
                .then(a.confirmed_bar_end.cmp(&b.confirmed_bar_end))
        });
        swept.extend(lows);
        for level in swept {
            let sweep = Sweep {
                feature: registry.feature,
                timeframe: registry.timeframe,
                level,
                time,
                price,
                extreme: price,
                window_bars: 0,
                outcome: SweepOutcome::Pending,
                resolved_bar_end: None,
                known_at: time,
                coverage: level.coverage,
            };
            registry.pending.push(sweep);
            events.push(StructureEvent::Sweep(sweep));
        }
    }
}

/// The structure engine state (ADR-037): the bar window, the last swings
/// and the level registry of every structure timeframe. Engine state; the
/// Market State exposes only [`StructureSet`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StructureTracker {
    /// In [`STRUCTURE_TIMEFRAMES`] order.
    timeframes: [TimeframeTracker; 4],
}

/// The trackers one event changed, committed with the bars they were
/// computed from.
#[derive(Debug)]
pub(crate) struct StructureStep {
    /// The index and new state of every timeframe the event changed.
    timeframes: Vec<(usize, TimeframeTracker)>,
    /// The facts, in processing order (decision 8).
    events: Vec<StructureEvent>,
}

impl Default for StructureTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl StructureTracker {
    /// An empty tracker for [`catalog::STRUCTURE_SWING`] and
    /// [`catalog::STRUCTURE_LEVELS`].
    pub(crate) fn new() -> Self {
        Self {
            timeframes: std::array::from_fn(|index| {
                let (timeframe, swing) = catalog::STRUCTURE_SWING[index];
                TimeframeTracker::new(timeframe, swing, catalog::STRUCTURE_LEVELS[index].1)
            }),
        }
    }

    /// Steps the structure with `event`, given the bars it closed (`closed`,
    /// in `(end, timeframe)` order), without changing the tracker
    /// (ADR-037, decision 8).
    ///
    /// Order: the closed bars of structure timeframes in close order — for
    /// each, its sweeps' resolutions, its touches, then its swing
    /// confirmations — then a trade's sweeps, timeframe ascending.
    /// `Ok(None)` when no bar of a structure timeframe closed and a trade
    /// sweeps nothing: one comparison per timeframe, nothing cloned. Only
    /// the timeframes the event changes are cloned.
    ///
    /// # Errors
    ///
    /// [`StructureError::Overflow`] if a count leaves its integer range;
    /// nothing is committed then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
    ) -> Result<Option<StructureStep>, StructureError> {
        let trade = match event {
            MarketEvent::Trade(trade) => Some(trade),
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => None,
        };
        let mut changed = Vec::new();
        for (index, tracker) in self.timeframes.iter().enumerate() {
            let timeframe = tracker.timeframe();
            if closed.iter().any(|bar| bar.timeframe == timeframe)
                || trade.is_some_and(|trade| tracker.registry.swept_by(trade.price))
            {
                changed.push((index, tracker.clone()));
            }
        }
        if changed.is_empty() {
            return Ok(None);
        }
        let mut events = Vec::new();
        for bar in closed {
            if let Some((_, tracker)) = changed
                .iter_mut()
                .find(|(_, tracker)| tracker.timeframe() == bar.timeframe)
            {
                tracker.close(bar, event.time(), &mut events)?;
            }
        }
        if let Some(trade) = trade {
            for (_, tracker) in &mut changed {
                tracker.sweep(trade.time, trade.price, &mut events);
            }
        }
        Ok(Some(StructureStep {
            timeframes: changed,
            events,
        }))
    }

    /// Commits a step computed by [`Self::step`], writes the new values to
    /// `set` and appends the step's facts to `events`. Moves and clones: it
    /// cannot fail.
    pub(crate) fn commit(
        &mut self,
        step: StructureStep,
        set: &mut StructureSet,
        events: &mut Vec<StructureEvent>,
    ) {
        for (index, tracker) in step.timeframes {
            set.timeframes[index] = tracker.values();
            self.timeframes[index] = tracker;
        }
        events.extend(step.events);
    }
}

/// Why the structure could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StructureError {
    /// A touch, window or bar count left its integer range (ADR-027).
    Overflow,
}

impl fmt::Display for StructureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("a structure count leaves its integer range"),
        }
    }
}

impl std::error::Error for StructureError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::{Lcg, random_tape};
    use crate::event::samples::{gap, mark, t};
    use crate::event::{Aggressor, GapReason, Stream, Trade};
    use crate::fingerprint::Fingerprinter;
    use crate::num::SCALE;
    use crate::state::MarketStateEngine;

    const M15: i64 = 900_000;
    const DAY: i64 = 86_400_000;
    const COMPLETE: Coverage = Coverage {
        partial_start: false,
        feed_gap: false,
    };
    const PARTIAL: Coverage = Coverage {
        partial_start: true,
        feed_gap: false,
    };
    const GAP: Coverage = Coverage {
        partial_start: false,
        feed_gap: true,
    };

    /// Whole USDT as a price.
    fn usdt(whole: i64) -> Price {
        Price::from_units(whole * SCALE)
    }

    fn trade_at(millis: i64, trade_id: u64, price: Price) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price,
            qty: crate::num::Qty::from_units(1_000_000),
            aggressor: Aggressor::Buy,
        })
    }

    /// A tape built bar by bar; trade ids count up.
    #[derive(Default)]
    struct Tape {
        events: Vec<MarketEvent>,
        trade_id: u64,
    }

    impl Tape {
        fn trade(&mut self, millis: i64, price: Price) -> &mut Self {
            self.trade_id += 1;
            self.events.push(trade_at(millis, self.trade_id, price));
            self
        }

        /// Trades making 15m bar `index` (whole USDT): its high, its low,
        /// then its close.
        fn bar(&mut self, index: i64, high: i64, low: i64, close: i64) -> &mut Self {
            let open = index * M15;
            self.trade(open + 1_000, usdt(high))
                .trade(open + 2_000, usdt(low))
                .trade(open + 3_000, usdt(close))
        }

        /// A zero-length trades gap at `millis`: it closes every bar ending
        /// by then without trading or flagging them.
        fn close_at(&mut self, millis: i64) -> &mut Self {
            self.events
                .push(gap(Stream::Trades, millis, millis, GapReason::LateEvent));
            self
        }

        /// Four rising bars, then a 15m swing high at 60 015 on bar 7,
        /// confirmed at the end of bar 10 by the trade that opens bar 11
        /// at `confirm_price`.
        fn swing_high(confirm_price: Price) -> Self {
            let mut tape = Self::default();
            for (index, high) in (0..).zip([
                59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011,
                60_010,
            ]) {
                tape.bar(index, high, high - 5, high - 2);
            }
            tape.trade(11 * M15 + 1_000, confirm_price);
            tape
        }
    }

    /// The level of [`Tape::swing_high`].
    fn swing_level() -> Price {
        usdt(60_015)
    }

    /// One unit beyond `price` upwards.
    fn above(price: Price) -> Price {
        Price::from_units(price.units() + 1)
    }

    /// Mirrors every trade price around 60 000 USDT, turning highs into
    /// lows.
    fn mirror(events: &[MarketEvent]) -> Vec<MarketEvent> {
        events
            .iter()
            .map(|event| match event {
                MarketEvent::Trade(trade) => MarketEvent::Trade(Trade {
                    price: mirrored(trade.price),
                    ..*trade
                }),
                other => other.clone(),
            })
            .collect()
    }

    fn mirrored(price: Price) -> Price {
        Price::from_units(2 * 60_000 * SCALE - price.units())
    }

    /// Runs `events` and returns the engine and every event's facts.
    fn run(events: &[MarketEvent]) -> (MarketStateEngine, Vec<Vec<StructureEvent>>) {
        let mut engine = MarketStateEngine::new();
        let facts = events
            .iter()
            .map(|event| {
                engine.apply(event).unwrap();
                engine.structure_events().to_vec()
            })
            .collect();
        (engine, facts)
    }

    /// The facts of `timeframe`, with the index of the event that emitted
    /// them.
    fn facts_of(
        facts: &[Vec<StructureEvent>],
        timeframe: Timeframe,
    ) -> Vec<(usize, StructureEvent)> {
        facts
            .iter()
            .enumerate()
            .flat_map(|(index, facts)| {
                facts
                    .iter()
                    .filter(|fact| fact.timeframe() == timeframe)
                    .map(move |fact| (index, *fact))
            })
            .collect()
    }

    fn structure(engine: &MarketStateEngine, timeframe: Timeframe) -> &TimeframeStructure {
        engine.state().structure.get(timeframe).unwrap()
    }

    fn registry_of(engine: &MarketStateEngine, timeframe: Timeframe) -> &LevelRegistry {
        structure(engine, timeframe).levels.ready().unwrap()
    }

    /// A 15m swing at bar `bar`, confirmed by the close of bar `bar + 3`
    /// and emitted by the trade 1 s into bar `bar + 4`.
    fn swing_15m(side: Side, price: Price, bar: i64, coverage: Coverage) -> Swing {
        Swing {
            feature: catalog::STRUCTURE_SWING_15M_V1.key,
            timeframe: Timeframe::M15,
            side,
            price,
            swing_time: t(bar * M15),
            confirmed_bar_end: t((bar + 4) * M15),
            known_at: t((bar + 4) * M15 + 1_000),
            coverage,
        }
    }

    #[test]
    fn a_swing_is_confirmed_by_the_close_of_the_nth_bar_after_it() {
        let tape = Tape::swing_high(usdt(60_008));
        let (engine, facts) = run(&tape.events);
        // Bar k closes with the first trade of bar k + 1, at index 3 (k + 1).
        for k in 0..6 {
            let mut engine = MarketStateEngine::new();
            for event in &tape.events[..=3 * (k + 1)] {
                engine.apply(event).unwrap();
            }
            let observed = u64::try_from(k + 1).unwrap();
            assert_eq!(
                structure(&engine, Timeframe::M15).swings,
                FeatureValue::WarmingUp {
                    observed,
                    required: 7
                }
            );
            assert_eq!(
                structure(&engine, Timeframe::M15).levels,
                FeatureValue::WarmingUp {
                    observed,
                    required: 7
                }
            );
        }
        let swing = swing_15m(Side::High, swing_level(), 7, COMPLETE);
        // Only the trade that closes bar 10 emits it.
        assert_eq!(
            facts_of(&facts, Timeframe::M15),
            [(33, StructureEvent::Swing(swing))]
        );
        assert_eq!(swing.confirmed_bar_end, t(11 * M15));
        // After the event that closes bar 9 the structure is ready, without
        // the swing.
        let mut before = MarketStateEngine::new();
        for event in &tape.events[..33] {
            before.apply(event).unwrap();
        }
        assert_eq!(
            structure(&before, Timeframe::M15).swings,
            FeatureValue::Ready(LastSwings {
                high: None,
                low: None
            })
        );
        assert!(registry_of(&before, Timeframe::M15).highs().is_empty());
        assert_eq!(
            structure(&engine, Timeframe::M15).swings,
            FeatureValue::Ready(LastSwings {
                high: Some(swing),
                low: None
            })
        );
        assert_eq!(
            registry_of(&engine, Timeframe::M15).highs(),
            [Level {
                side: Side::High,
                price: swing_level(),
                swing_time: t(7 * M15),
                confirmed_bar_end: t(11 * M15),
                touches: 0,
                coverage: COMPLETE,
            }]
        );
        assert_eq!(
            StructureEvent::Swing(swing).to_string(),
            "swing 15m high price=60015.00000000 bar=6300000ms confirmed=9900000ms complete \
             structure.swing.15m@1"
        );
        // Mirrored, it is a swing low.
        let (_, facts) = run(&mirror(&tape.events));
        assert_eq!(
            facts_of(&facts, Timeframe::M15),
            [(
                33,
                StructureEvent::Swing(swing_15m(Side::Low, usdt(59_985), 7, COMPLETE))
            )]
        );
        // Other timeframes have too few bars.
        for timeframe in [Timeframe::H1, Timeframe::H4, Timeframe::D1] {
            assert!(facts_of(&facts, timeframe).is_empty());
        }
    }

    /// The bar indices of the 15m swing highs and lows of `bars` (`None` is
    /// an empty bar, `(high, low)` in whole USDT), and the swings.
    fn swings_of(bars: &[Option<(i64, i64)>]) -> (Vec<i64>, Vec<i64>, Vec<Swing>) {
        let mut tape = Tape::default();
        for (index, bar) in (0..).zip(bars) {
            if let Some((high, low)) = bar {
                tape.bar(index, *high, *low, *low);
            }
        }
        let end = i64::try_from(bars.len()).unwrap() * M15;
        tape.close_at(end);
        let (_, facts) = run(&tape.events);
        let swings: Vec<Swing> = facts_of(&facts, Timeframe::M15)
            .into_iter()
            .map(|(_, fact)| match fact {
                StructureEvent::Swing(swing) => swing,
                other => panic!("{other}"),
            })
            .collect();
        let bars_of = |side| {
            swings
                .iter()
                .filter(|swing| swing.side == side)
                .map(|swing| swing.swing_time.as_millis() / M15)
                .collect()
        };
        (bars_of(Side::High), bars_of(Side::Low), swings)
    }

    /// Bars with `high` and `high - 1` as the low.
    fn highs(highs: &[i64]) -> Vec<Option<(i64, i64)>> {
        highs.iter().map(|&high| Some((high, high - 1))).collect()
    }

    #[test]
    fn ties_are_strict_on_the_left_and_weak_on_the_right() {
        // A plateau yields its first bar only.
        let (high, low, _) = swings_of(&highs(&[1, 2, 3, 5, 5, 5, 3, 2, 1]));
        assert_eq!((high, low), (vec![3], vec![]));
        // An equal left bar blocks a swing…
        let (high, _, _) = swings_of(&highs(&[1, 5, 2, 5, 2, 1, 0]));
        assert_eq!(high, Vec::<i64>::new());
        // …an equal right bar does not.
        let (high, _, _) = swings_of(&highs(&[1, 2, 3, 5, 2, 5, 1, 0, 0]));
        assert_eq!(high, [3]);
        // A higher right bar cancels it.
        let (high, _, _) = swings_of(&highs(&[1, 2, 3, 5, 2, 6, 1]));
        assert_eq!(high, Vec::<i64>::new());
        // Lows mirror the rule.
        let lows = |lows: &[i64]| -> Vec<Option<(i64, i64)>> {
            lows.iter().map(|&low| Some((low + 1, low))).collect()
        };
        let (_, low, _) = swings_of(&lows(&[9, 8, 7, 5, 5, 5, 7, 8, 9]));
        assert_eq!(low, [3]);
        let (_, low, _) = swings_of(&lows(&[9, 5, 8, 5, 8, 9, 9]));
        assert_eq!(low, Vec::<i64>::new());
        // An outside bar is both, the high first.
        let (high, low, swings) = swings_of(&[
            Some((10, 8)),
            Some((11, 9)),
            Some((12, 10)),
            Some((20, 1)),
            Some((12, 10)),
            Some((11, 9)),
            Some((10, 8)),
        ]);
        assert_eq!((high, low), (vec![3], vec![3]));
        assert_eq!(
            swings.iter().map(|swing| swing.side).collect::<Vec<_>>(),
            [Side::High, Side::Low]
        );
        assert_eq!((swings[0].price, swings[1].price), (usdt(20), usdt(1)));
    }

    #[test]
    fn empty_and_gap_bars_are_neutral_but_flag_the_coverage() {
        // The empty bars 2 and 4 would have blocked the swing if they had
        // traded higher; empty, they neither block nor qualify.
        let (high, _, _) = swings_of(&[
            Some((1, 0)),
            Some((2, 1)),
            None,
            Some((5, 4)),
            None,
            Some((3, 2)),
            Some((2, 1)),
        ]);
        assert_eq!(high, [3]);
        // An empty candidate is never a swing.
        let (high, low, _) = swings_of(&[
            Some((1, 0)),
            Some((2, 1)),
            Some((3, 2)),
            None,
            Some((3, 2)),
            Some((2, 1)),
            Some((1, 0)),
        ]);
        assert_eq!((high, low), (vec![], vec![]));
        // A trades gap inside bar 9 flags the swing; nothing resets.
        let mut tape = Tape::default();
        for (index, high) in (0..).zip([
            59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011, 60_010,
        ]) {
            tape.bar(index, high, high - 5, high - 2);
            if index == 9 {
                tape.events.push(gap(
                    Stream::Trades,
                    9 * M15 + 4_000,
                    9 * M15 + 5_000,
                    GapReason::Disconnected,
                ));
            }
        }
        tape.trade(11 * M15 + 1_000, usdt(60_008));
        let (_, facts) = run(&tape.events);
        let swings: Vec<StructureEvent> = facts_of(&facts, Timeframe::M15)
            .into_iter()
            .map(|(_, fact)| fact)
            .collect();
        assert_eq!(
            swings,
            [StructureEvent::Swing(swing_15m(
                Side::High,
                swing_level(),
                7,
                GAP
            ))]
        );
        // The first bar of the tape is partial: a swing whose window holds
        // it carries the flag.
        let (_, _, swings) = swings_of(&highs(&[1, 2, 3, 5, 3, 2, 1]));
        assert_eq!(swings[0].coverage, PARTIAL);
    }

    /// The engine and the 15m facts after [`Tape::swing_high`] confirmed
    /// with `confirm_price`, then `then`, without the swings; the tape's
    /// prices mirrored if `mirror_prices`.
    fn after_swing(
        confirm_price: Price,
        then: impl FnOnce(&mut Tape),
        mirror_prices: bool,
    ) -> (MarketStateEngine, Vec<(usize, StructureEvent)>) {
        let mut tape = Tape::swing_high(confirm_price);
        then(&mut tape);
        let events = if mirror_prices {
            mirror(&tape.events)
        } else {
            tape.events
        };
        let (engine, facts) = run(&events);
        let facts = facts_of(&facts, Timeframe::M15)
            .into_iter()
            .filter(|(_, fact)| !matches!(fact, StructureEvent::Swing(_)))
            .collect();
        (engine, facts)
    }

    /// The time a fact is defined at: the confirming bar's end for a swing,
    /// the sweeping trade for a sweep, the resolving bar's end for an SFP or
    /// a break. Its emission, [`StructureEvent::time`], can come later.
    fn bar_time(fact: &StructureEvent) -> EventTime {
        match fact {
            StructureEvent::Swing(swing) => swing.confirmed_bar_end,
            StructureEvent::Sweep(sweep) => sweep.time,
            StructureEvent::Sfp(sweep) | StructureEvent::Break(sweep) => {
                sweep.resolved_bar_end.unwrap()
            }
        }
    }

    /// Each fact's kind and [`bar_time`].
    fn kinds(facts: &[(usize, StructureEvent)]) -> Vec<(&'static str, EventTime)> {
        facts
            .iter()
            .map(|(_, fact)| {
                let kind = match fact {
                    StructureEvent::Swing(_) => "swing",
                    StructureEvent::Sweep(_) => "sweep",
                    StructureEvent::Sfp(_) => "sfp",
                    StructureEvent::Break(_) => "break",
                };
                (kind, bar_time(fact))
            })
            .collect()
    }

    fn sweep_of(fact: &StructureEvent) -> &Sweep {
        match fact {
            StructureEvent::Sweep(sweep)
            | StructureEvent::Sfp(sweep)
            | StructureEvent::Break(sweep) => sweep,
            StructureEvent::Swing(swing) => panic!("{swing}"),
        }
    }

    #[test]
    fn a_trade_at_the_level_is_no_sweep_one_unit_beyond_is() {
        for mirrored_side in [false, true] {
            let price = |price: Price| {
                if mirrored_side {
                    mirrored(price)
                } else {
                    price
                }
            };
            let (engine, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, swing_level());
                },
                mirrored_side,
            );
            assert!(facts.is_empty());
            let registry = registry_of(&engine, Timeframe::M15);
            assert_eq!(registry.highs().len() + registry.lows().len(), 1);

            let at = 11 * M15 + 3_000;
            let (engine, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, swing_level())
                        .trade(at, above(swing_level()));
                },
                mirrored_side,
            );
            assert_eq!(kinds(&facts), [("sweep", t(at))]);
            let sweep = sweep_of(&facts[0].1);
            assert_eq!(
                (sweep.level.price, sweep.price, sweep.extreme),
                (
                    price(swing_level()),
                    price(above(swing_level())),
                    price(above(swing_level()))
                )
            );
            assert_eq!(
                (sweep.outcome, sweep.resolved_bar_end, sweep.window_bars),
                (SweepOutcome::Pending, None, 0)
            );
            let registry = registry_of(&engine, Timeframe::M15);
            assert!(registry.highs().is_empty() && registry.lows().is_empty());
            assert_eq!(registry.pending(), [*sweep]);
            let handed: Vec<_> = registry.levels().collect();
            assert_eq!(handed.len(), 1);
            assert_eq!(
                (handed[0].kind, handed[0].created_at, handed[0].outcome),
                (LevelKind::PriorSweep, t(at), Some(SweepOutcome::Pending))
            );
        }
    }

    #[test]
    fn a_close_back_inside_is_an_sfp_a_close_beyond_a_break() {
        for mirrored_side in [false, true] {
            let price = |price: Price| {
                if mirrored_side {
                    mirrored(price)
                } else {
                    price
                }
            };
            // Bar 11 sweeps to 60 020 and closes back at 60 014.
            let (engine, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, usdt(60_020))
                        .trade(11 * M15 + 3_000, usdt(60_014))
                        .close_at(12 * M15);
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(11 * M15 + 2_000)), ("sfp", t(12 * M15))]
            );
            let sfp = sweep_of(&facts[1].1);
            assert_eq!(
                (sfp.extreme, sfp.window_bars, sfp.outcome),
                (price(usdt(60_020)), 1, SweepOutcome::Sfp)
            );
            let registry = registry_of(&engine, Timeframe::M15);
            assert!(registry.pending().is_empty());
            assert_eq!(registry.resolved(), [*sfp]);
            let zone = registry.levels().last().unwrap();
            assert_eq!(zone.kind, LevelKind::SfpRejectionZone);
            let (low, high) = if mirrored_side {
                (usdt(59_980), usdt(59_985))
            } else {
                (usdt(60_015), usdt(60_020))
            };
            assert_eq!(
                (zone.low, zone.high, zone.created_at),
                (low, high, t(12 * M15))
            );
            assert_eq!(zone.age_ms(t(12 * M15 + 5)), 5);

            // Both window bars close beyond: a clean break, no zone.
            let (engine, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, usdt(60_020))
                        .trade(12 * M15 + 1_000, usdt(60_030))
                        .trade(12 * M15 + 2_000, usdt(60_017))
                        .close_at(13 * M15);
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(11 * M15 + 2_000)), ("break", t(13 * M15))]
            );
            let broken = sweep_of(&facts[1].1);
            assert_eq!(
                (broken.extreme, broken.window_bars),
                (price(usdt(60_030)), 2)
            );
            let registry = registry_of(&engine, Timeframe::M15);
            assert!(
                registry
                    .levels()
                    .all(|level| level.kind == LevelKind::PriorSweep)
            );
            assert_eq!(registry.levels().count(), 1);
        }
    }

    #[test]
    fn the_sfp_window_is_k_bars_from_the_sweep_bar() {
        for mirrored_side in [false, true] {
            // The close inside on bar K, exactly at the level, is an SFP.
            let (_, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, usdt(60_020))
                        .trade(12 * M15 + 1_000, usdt(60_015))
                        .close_at(13 * M15);
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(11 * M15 + 2_000)), ("sfp", t(13 * M15))]
            );
            // The first close inside on bar K + 1 comes too late: a break at
            // the end of bar K, and no SFP after it.
            let (engine, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, usdt(60_020))
                        .trade(12 * M15 + 1_000, usdt(60_016))
                        .trade(13 * M15 + 1_000, usdt(60_014))
                        .close_at(15 * M15);
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(11 * M15 + 2_000)), ("break", t(13 * M15))]
            );
            assert_eq!(registry_of(&engine, Timeframe::M15).resolved().len(), 1);
            // An empty bar counts toward K and never closes inside.
            let (_, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(11 * M15 + 2_000, usdt(60_020))
                        .trade(13 * M15 + 1_000, usdt(60_014));
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(11 * M15 + 2_000)), ("break", t(13 * M15))]
            );
            // A sweep in the last millisecond of a bar counts that bar as
            // bar 1: bar 13's close inside is too late.
            let (_, facts) = after_swing(
                usdt(60_008),
                |tape| {
                    tape.trade(12 * M15 - 1, usdt(60_020))
                        .trade(12 * M15 + 1_000, usdt(60_018))
                        .trade(13 * M15 + 1_000, usdt(60_014))
                        .close_at(14 * M15);
                },
                mirrored_side,
            );
            assert_eq!(
                kinds(&facts),
                [("sweep", t(12 * M15 - 1)), ("break", t(13 * M15))]
            );
            // The trade that closes the confirming bar sweeps the new level
            // in the same event: the swing first, then the sweep.
            let mut tape = Tape::swing_high(usdt(60_016));
            tape.trade(11 * M15 + 2_000, usdt(60_010))
                .close_at(12 * M15);
            let events = if mirrored_side {
                mirror(&tape.events)
            } else {
                tape.events
            };
            let (_, facts) = run(&events);
            let facts = facts_of(&facts, Timeframe::M15);
            assert_eq!(
                kinds(&facts),
                [
                    ("swing", t(11 * M15)),
                    ("sweep", t(11 * M15 + 1_000)),
                    ("sfp", t(12 * M15))
                ]
            );
            assert_eq!((facts[0].0, facts[1].0), (33, 33));
        }
    }

    #[test]
    fn facts_are_timed_at_their_emission_across_a_silent_trades_outage() {
        // The swing of `Tape::swing_high`, whose confirming bar 10 ends at
        // 9 900 000 ms, then 45 minutes without trades or a gap event: only
        // mark prices.
        let mut tape = Tape::default();
        for (index, high) in (0..).zip([
            59_990, 59_992, 59_994, 59_996, 60_010, 60_011, 60_012, 60_015, 60_012, 60_011, 60_010,
        ]) {
            tape.bar(index, high, high - 5, high - 2);
        }
        tape.events.push(mark(11 * M15 + 600_000, 1));
        tape.trade(14 * M15, usdt(60_008));
        let mut engine = MarketStateEngine::new();
        for event in &tape.events[..=33] {
            engine.apply(event).unwrap();
        }
        // Ten minutes past the bar end, the engine has not seen the swing.
        assert!(engine.state().as_of.unwrap() > t(11 * M15));
        assert!(engine.structure_events().is_empty());
        assert_eq!(
            structure(&engine, Timeframe::M15).swings,
            FeatureValue::Ready(LastSwings {
                high: None,
                low: None
            })
        );
        // The trade that ends the outage emits it, timed at that trade —
        // with the low of bar 10, which the outage's empty bars confirm.
        engine.apply(&tape.events[34]).unwrap();
        let swing = Swing {
            known_at: t(14 * M15),
            ..swing_15m(Side::High, swing_level(), 7, COMPLETE)
        };
        assert_eq!(swing.confirmed_bar_end, t(11 * M15));
        let fact = StructureEvent::Swing(swing);
        assert_eq!(engine.structure_events()[0], fact);
        assert_eq!(engine.structure_events().len(), 2);
        assert!(
            engine
                .structure_events()
                .iter()
                .all(|fact| fact.time() == t(14 * M15))
        );
        // A mark at `time()` sees it.
        engine.apply(&mark(14 * M15, 2)).unwrap();
        assert_eq!(engine.state().as_of, Some(fact.time()));
        assert_eq!(
            structure(&engine, Timeframe::M15)
                .swings
                .ready()
                .unwrap()
                .high,
            Some(swing)
        );

        // A sweep in bar 11, then silence: bar 11 closes beyond and bar 12
        // is empty, a break at 11 700 000 ms that only the next trade, 45
        // minutes later, emits.
        let silent = |tape: &mut Tape| {
            tape.trade(11 * M15 + 2_000, usdt(60_020));
            tape.events.push(mark(13 * M15 + 600_000, 1));
            tape.trade(16 * M15, usdt(60_010));
        };
        let (engine, facts) = after_swing(usdt(60_008), silent, false);
        assert_eq!(
            kinds(&facts),
            [("sweep", t(11 * M15 + 2_000)), ("break", t(13 * M15))]
        );
        let times: Vec<(usize, EventTime)> = facts
            .iter()
            .map(|(index, fact)| (*index, fact.time()))
            .collect();
        assert_eq!(times, [(34, t(11 * M15 + 2_000)), (36, t(16 * M15))]);
        assert_eq!(sweep_of(&facts[1].1).known_at, t(16 * M15));
        assert_eq!(
            registry_of(&engine, Timeframe::M15).resolved()[0].known_at,
            t(16 * M15)
        );
        // At the mark, past the break's bar end, the sweep is still pending.
        let mut tape = Tape::swing_high(usdt(60_008));
        silent(&mut tape);
        let mut engine = MarketStateEngine::new();
        for event in &tape.events[..=35] {
            engine.apply(event).unwrap();
        }
        let registry = registry_of(&engine, Timeframe::M15);
        assert!(registry.resolved().is_empty());
        assert_eq!(registry.pending().len(), 1);
    }

    #[test]
    fn touches_count_bars_within_the_tolerance_after_confirmation() {
        // 5 bps of 60 015 is 30.0075 USDT, exactly 3 000 750 000 units; of
        // the mirrored low at 59 985, 29.9925 USDT. The tapes are mirrored
        // after they are built, so the boundary is set per side.
        for mirrored_side in [false, true] {
            let tolerance = if mirrored_side {
                2_999_250_000
            } else {
                3_000_750_000
            };
            let near = Price::from_units(swing_level().units() - tolerance);
            let far = Price::from_units(near.units() - 1);
            let (engine, facts) = after_swing(
                usdt(59_900),
                |tape| {
                    // Bars 8–10 came within 5 USDT before the confirmation;
                    // they never count. The confirming trade opened bar 11
                    // 115 USDT away.
                    tape.trade(11 * M15 + 2_000, near)
                        .trade(12 * M15 + 1_000, far)
                        .trade(13 * M15 + 1_000, swing_level())
                        .trade(14 * M15 + 1_000, usdt(59_900))
                        .close_at(15 * M15);
                },
                mirrored_side,
            );
            assert!(facts.is_empty());
            let registry = registry_of(&engine, Timeframe::M15);
            let price = if mirrored_side {
                mirrored(swing_level())
            } else {
                swing_level()
            };
            let level = registry
                .highs()
                .iter()
                .chain(registry.lows())
                .find(|level| level.price == price)
                .unwrap();
            // Bars 11 and 13 touch; bar 12 is one unit too far.
            assert_eq!(level.touches, 2, "mirrored: {mirrored_side}");
            // Bar 15 sweeps: the sweep bar never touches.
            let (engine, facts) = after_swing(
                usdt(59_900),
                |tape| {
                    tape.trade(11 * M15 + 2_000, near)
                        .trade(15 * M15 + 1_000, swing_level())
                        .trade(15 * M15 + 2_000, usdt(60_030))
                        .trade(15 * M15 + 3_000, usdt(60_000))
                        .close_at(16 * M15);
                },
                mirrored_side,
            );
            assert_eq!(kinds(&facts).len(), 2);
            assert_eq!(sweep_of(&facts[1].1).level.touches, 1);
            assert_eq!(
                registry_of(&engine, Timeframe::M15).resolved()[0]
                    .level
                    .touches,
                1
            );
        }
        // The rule itself, at the boundary on both sides.
        let near = Price::from_units(swing_level().units() - 3_000_750_000);
        let far = Price::from_units(near.units() - 1);
        let high = Level {
            side: Side::High,
            price: swing_level(),
            swing_time: t(0),
            confirmed_bar_end: t(0),
            touches: 0,
            coverage: COMPLETE,
        };
        let at = |price: Price| Ohlc {
            open: price,
            high: price,
            low: price,
            close: price,
        };
        assert!(touches(&high, &at(near)));
        assert!(touches(&high, &at(swing_level())));
        assert!(!touches(&high, &at(far)));
        assert!(!touches(&high, &at(above(swing_level()))));
        let low = Level {
            side: Side::Low,
            price: mirrored(swing_level()),
            ..high
        };
        let tolerance_low = 2_999_250_000; // 5 bps of 59 985
        assert!(touches(
            &low,
            &at(Price::from_units(low.price.units() + tolerance_low))
        ));
        assert!(!touches(
            &low,
            &at(Price::from_units(low.price.units() + tolerance_low + 1))
        ));
        assert!(!touches(
            &low,
            &at(Price::from_units(low.price.units() - 1))
        ));
    }

    /// A tracker whose 15m and 1h registries hold `levels`, ready.
    fn tracker_with(levels: &[(Timeframe, Level)]) -> StructureTracker {
        let mut tracker = StructureTracker::new();
        for timeframe in &mut tracker.timeframes {
            timeframe.closed_bars = SWING_WINDOW as u64;
        }
        for (timeframe, level) in levels {
            let tracker = tracker
                .timeframes
                .iter_mut()
                .find(|tracker| tracker.timeframe() == *timeframe)
                .unwrap();
            tracker.registry.add(*level);
        }
        tracker
    }

    fn active(side: Side, whole: i64, confirmed: i64) -> Level {
        Level {
            side,
            price: usdt(whole),
            swing_time: t(0),
            confirmed_bar_end: t(confirmed),
            touches: 0,
            coverage: COMPLETE,
        }
    }

    #[test]
    fn one_trade_sweeps_by_timeframe_then_crossing_order_then_age() {
        let tracker = tracker_with(&[
            (Timeframe::H1, active(Side::High, 60_018, 10)),
            (Timeframe::M15, active(Side::High, 60_020, 20)),
            (Timeframe::M15, active(Side::High, 60_015, 30)),
            (Timeframe::M15, active(Side::High, 60_020, 5)),
            (Timeframe::M15, active(Side::High, 60_040, 1)),
            (Timeframe::M15, active(Side::Low, 59_000, 1)),
        ]);
        let sweeping = trade_at(1_000, 1, usdt(60_030));
        let step = tracker.step(&sweeping, &[]).unwrap().unwrap();
        let order: Vec<(Timeframe, Price, EventTime)> = step
            .events
            .iter()
            .map(|fact| {
                let sweep = sweep_of(fact);
                (
                    sweep.timeframe,
                    sweep.level.price,
                    sweep.level.confirmed_bar_end,
                )
            })
            .collect();
        assert_eq!(
            order,
            [
                (Timeframe::M15, usdt(60_015), t(30)),
                (Timeframe::M15, usdt(60_020), t(5)),
                (Timeframe::M15, usdt(60_020), t(20)),
                (Timeframe::H1, usdt(60_018), t(10)),
            ]
        );
        // Lows: price descending, then by confirmation.
        let tracker = tracker_with(&[
            (Timeframe::M15, active(Side::Low, 59_980, 20)),
            (Timeframe::M15, active(Side::Low, 59_990, 30)),
            (Timeframe::M15, active(Side::Low, 59_980, 5)),
            (Timeframe::D1, active(Side::Low, 59_995, 1)),
        ]);
        let step = tracker
            .step(&trade_at(1_000, 1, usdt(59_970)), &[])
            .unwrap()
            .unwrap();
        let order: Vec<(Timeframe, Price, EventTime)> = step
            .events
            .iter()
            .map(|fact| {
                let sweep = sweep_of(fact);
                (
                    sweep.timeframe,
                    sweep.level.price,
                    sweep.level.confirmed_bar_end,
                )
            })
            .collect();
        assert_eq!(
            order,
            [
                (Timeframe::M15, usdt(59_990), t(30)),
                (Timeframe::M15, usdt(59_980), t(5)),
                (Timeframe::M15, usdt(59_980), t(20)),
                (Timeframe::D1, usdt(59_995), t(1)),
            ]
        );
        // Nothing swept, no bar closed: no step and nothing cloned.
        assert!(
            tracker
                .step(&trade_at(1_000, 1, usdt(59_996)), &[])
                .unwrap()
                .is_none()
        );
        assert!(tracker.step(&mark(1_000, 1), &[]).unwrap().is_none());
    }

    #[test]
    fn full_lists_evict_their_oldest_entry_but_never_a_pending_sweep() {
        let mut registry = LevelRegistry::new(catalog::STRUCTURE_LEVELS_15M_V1.key, Timeframe::M15);
        // 21 highs at distinct prices 60 000..=60 020, confirmed at their
        // index: the oldest (index 0, 60 010) is in the middle of the price
        // order, the newest (index 20, 60 002) too, the lowest is index 4
        // and the highest index 17. Only eviction by age drops index 0.
        let whole = |index: i64| 60_000 + (index * 8 + 10) % 21;
        assert_eq!(
            (whole(0), whole(20), whole(4), whole(17)),
            (60_010, 60_002, 60_000, 60_020)
        );
        for index in 0..=20 {
            registry.add(active(Side::High, whole(index), index));
        }
        let mut kept: Vec<Level> = (1..=20)
            .map(|index| active(Side::High, whole(index), index))
            .collect();
        kept.sort_by_key(|level| level.price);
        assert_eq!(registry.highs(), kept);
        // On a tie in age, the lower price goes first, whatever the
        // insertion order: lows too.
        for (whole, confirmed) in [(59_990, 0), (59_980, 0)]
            .into_iter()
            .chain((1..=18).map(|index| (59_900 + index, index)))
        {
            registry.add(active(Side::Low, whole, confirmed));
        }
        assert_eq!(registry.lows().len(), MAX_LEVELS);
        registry.add(active(Side::Low, 59_800, 19));
        assert_eq!(registry.lows().len(), MAX_LEVELS);
        assert!(
            registry
                .lows()
                .iter()
                .all(|level| level.price != usdt(59_980))
        );
        assert!(
            registry
                .lows()
                .iter()
                .any(|level| level.price == usdt(59_990))
        );
        // Resolved sweeps: the oldest by sweep time, then the lower price.
        let sweep = |whole: i64, time: i64| Sweep {
            feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
            timeframe: Timeframe::M15,
            level: active(Side::High, whole, 0),
            time: t(time),
            price: usdt(whole + 1),
            extreme: usdt(whole + 1),
            window_bars: 1,
            outcome: SweepOutcome::Break,
            resolved_bar_end: Some(t(time + M15)),
            known_at: t(time + M15),
            coverage: COMPLETE,
        };
        registry.push_resolved(sweep(60_002, 1));
        registry.push_resolved(sweep(60_001, 1));
        for time in 2..=19 {
            registry.push_resolved(sweep(60_100 + time, time));
        }
        assert_eq!(registry.resolved().len(), MAX_LEVELS);
        registry.push_resolved(sweep(60_200, 30));
        assert_eq!(registry.resolved().len(), MAX_LEVELS);
        assert_eq!(registry.resolved()[0].level.price, usdt(60_002));
        registry.push_resolved(sweep(60_201, 31));
        assert_eq!(registry.resolved()[0].time, t(2));
        // Pending sweeps are not capped: a trade sweeps every active high.
        let mut tracker = TimeframeTracker::new(
            Timeframe::M15,
            &catalog::STRUCTURE_SWING_15M_V1,
            &catalog::STRUCTURE_LEVELS_15M_V1,
        );
        tracker.registry = registry;
        let pending = MAX_LEVELS;
        tracker.registry.pending = vec![sweep(1, 1); pending];
        let mut facts = Vec::new();
        tracker.sweep(t(40), usdt(70_000), &mut facts);
        assert_eq!(facts.len(), MAX_LEVELS);
        assert_eq!(tracker.registry.pending().len(), pending + MAX_LEVELS);
        assert!(tracker.registry.highs().is_empty());
    }

    #[test]
    fn a_failed_step_leaves_the_tracker_alone() {
        let mut level = active(Side::High, 60_015, 0);
        level.touches = u32::MAX;
        let tracker = tracker_with(&[(Timeframe::M15, level)]);
        let before = tracker.clone();
        let mut bar = Bar::empty(Timeframe::M15, t(M15));
        bar.ohlc = Some(Ohlc {
            open: usdt(60_010),
            high: usdt(60_014),
            low: usdt(60_000),
            close: usdt(60_001),
        });
        let event = trade_at(2 * M15, 1, usdt(60_000));
        assert_eq!(
            tracker.step(&event, &[bar]).unwrap_err(),
            StructureError::Overflow
        );
        assert_eq!(tracker, before);
        // A bar out of reach is no touch and steps fine.
        bar.ohlc = Some(Ohlc {
            high: usdt(59_000),
            ..bar.ohlc.unwrap()
        });
        let step = tracker.step(&event, &[bar]).unwrap().unwrap();
        let mut committed = tracker.clone();
        let mut set = StructureSet::new();
        let mut facts = Vec::new();
        committed.commit(step, &mut set, &mut facts);
        assert!(facts.is_empty());
        assert_eq!(committed.timeframes[0].closed_bars, 8);
        assert_eq!(
            StructureError::Overflow.to_string(),
            "a structure count leaves its integer range"
        );
    }

    #[test]
    fn the_set_starts_warming_up_on_every_structure_timeframe() {
        let set = StructureSet::new();
        assert_eq!(
            set.iter()
                .map(|(timeframe, _)| timeframe)
                .collect::<Vec<_>>(),
            STRUCTURE_TIMEFRAMES
        );
        for (_, structure) in set.iter() {
            assert_eq!(structure.swings, warming(0));
            assert_eq!(structure.levels, warming(0));
        }
        assert!(set.get(Timeframe::M1).is_none());
        assert!(set.get(Timeframe::M5).is_none());
        assert_eq!(StructureSet::default(), set);
    }

    // --- The written definitions, transcribed offline ---------------------

    /// A sweep of the reference.
    #[derive(Clone, Copy)]
    struct RefSweep {
        level: Level,
        time: EventTime,
        price: Price,
    }

    /// Every closed bar of `timeframe` and the index of the event that
    /// closed it.
    fn bars_of(closed: &[Vec<Bar>], timeframe: Timeframe) -> Vec<(usize, Bar)> {
        closed
            .iter()
            .enumerate()
            .flat_map(|(index, bars)| {
                bars.iter()
                    .filter(move |bar| bar.timeframe == timeframe)
                    .map(move |bar| (index, *bar))
            })
            .collect()
    }

    /// Decisions 2–4 over the whole bar list: every swing, keyed by the
    /// bar that confirms it and known at the event of `events` that closed
    /// that bar.
    fn reference_swings(
        events: &[MarketEvent],
        bars: &[(usize, Bar)],
        feature: FeatureKey,
    ) -> Vec<(usize, Swing)> {
        let n = SWING_BARS;
        let mut swings = Vec::new();
        for i in n..bars.len().saturating_sub(n) {
            let bar = bars[i].1;
            let Some(ohlc) = bar.ohlc else { continue };
            let window = &bars[i - n..=i + n];
            let side = |j: usize| window[j].1.ohlc;
            let mut high = true;
            let mut low = true;
            for j in 0..window.len() {
                let Some(other) = side(j) else { continue };
                match j.cmp(&n) {
                    std::cmp::Ordering::Less => {
                        high &= ohlc.high > other.high;
                        low &= ohlc.low < other.low;
                    }
                    std::cmp::Ordering::Greater => {
                        high &= ohlc.high >= other.high;
                        low &= ohlc.low <= other.low;
                    }
                    std::cmp::Ordering::Equal => {}
                }
            }
            let mut coverage = COMPLETE;
            for (_, other) in window {
                coverage.partial_start |= other.coverage.partial_start;
                coverage.feed_gap |= other.coverage.feed_gap;
            }
            for (side, price, is) in [(Side::High, ohlc.high, high), (Side::Low, ohlc.low, low)] {
                if is {
                    swings.push((
                        i + n,
                        Swing {
                            feature,
                            timeframe: bar.timeframe,
                            side,
                            price,
                            swing_time: bar.open_time,
                            confirmed_bar_end: bars[i + n].1.end(),
                            known_at: events[bars[i + n].0].time(),
                            coverage,
                        },
                    ));
                }
            }
        }
        swings
    }

    /// The touches of an active `level` (decision 5): closed bars opening at
    /// or after its confirmation, among the first `known` bars.
    fn reference_touches(bars: &[(usize, Bar)], known: usize, level: &Swing) -> u32 {
        let tolerance = i128::from(level.price.units()).abs() * 5;
        let count = bars[..known]
            .iter()
            .filter(|(_, bar)| bar.open_time >= level.confirmed_bar_end)
            .filter_map(|(_, bar)| bar.ohlc)
            .filter(|ohlc| {
                let price = i128::from(level.price.units());
                let distance = match level.side {
                    Side::High => price - i128::from(ohlc.high.units()),
                    Side::Low => i128::from(ohlc.low.units()) - price,
                };
                distance >= 0 && distance * 10_000 <= tolerance
            })
            .count();
        u32::try_from(count).unwrap()
    }

    /// Decisions 2–8 transcribed offline over every closed bar and trade of
    /// a tape: the facts of each event as display lines, and the final
    /// registry of each timeframe.
    fn reference(events: &[MarketEvent], closed: &[Vec<Bar>]) -> (Vec<Vec<String>>, Vec<String>) {
        let mut finals = Vec::new();
        // Facts per (event, position): position orders facts in an event.
        let mut facts: Vec<(usize, Timeframe, EventTime, usize, String)> = Vec::new();
        for (swing_def, levels_def) in catalog::STRUCTURE_SWING
            .into_iter()
            .zip(catalog::STRUCTURE_LEVELS)
        {
            let (timeframe, swing_def) = swing_def;
            let feature = levels_def.1.key;
            let bars = bars_of(closed, timeframe);
            let swings = reference_swings(events, &bars, swing_def.key);
            // Active levels, as the swings that made them.
            let mut active: Vec<Swing> = Vec::new();
            let mut pending: Vec<RefSweep> = Vec::new();
            let mut resolved: Vec<Sweep> = Vec::new();
            let make_level = |swing: &Swing, known: usize| Level {
                side: swing.side,
                price: swing.price,
                swing_time: swing.swing_time,
                confirmed_bar_end: swing.confirmed_bar_end,
                touches: reference_touches(&bars, known, swing),
                coverage: swing.coverage,
            };
            let mut next_bar = 0;
            for (index, event) in events.iter().enumerate() {
                // Closed bars of this timeframe, in close order.
                while next_bar < bars.len() && bars[next_bar].0 == index {
                    let bar = bars[next_bar].1;
                    // Resolutions: the window is K bars from the sweep bar.
                    let mut still = Vec::new();
                    for sweep in pending.drain(..) {
                        let first = timeframe.open_of(sweep.time).unwrap();
                        let k = (bar.open_time.as_millis() - first.as_millis())
                            / timeframe.millis()
                            + 1;
                        let window: Vec<Bar> = bars
                            .iter()
                            .map(|(_, bar)| *bar)
                            .filter(|other| {
                                other.open_time >= first && other.open_time <= bar.open_time
                            })
                            .collect();
                        assert_eq!(i64::try_from(window.len()).unwrap(), k);
                        let mut extreme = sweep.price;
                        let mut coverage = sweep.level.coverage;
                        for other in &window {
                            coverage.partial_start |= other.coverage.partial_start;
                            coverage.feed_gap |= other.coverage.feed_gap;
                            if let Some(ohlc) = other.ohlc {
                                extreme = match sweep.level.side {
                                    Side::High => extreme.max(ohlc.high),
                                    Side::Low => extreme.min(ohlc.low),
                                };
                            }
                        }
                        let inside = bar.ohlc.is_some_and(|ohlc| match sweep.level.side {
                            Side::High => ohlc.close <= sweep.level.price,
                            Side::Low => ohlc.close >= sweep.level.price,
                        });
                        let outcome = if inside {
                            SweepOutcome::Sfp
                        } else if k == i64::from(SFP_WINDOW_BARS) {
                            SweepOutcome::Break
                        } else {
                            still.push(sweep);
                            continue;
                        };
                        let done = Sweep {
                            feature,
                            timeframe,
                            level: sweep.level,
                            time: sweep.time,
                            price: sweep.price,
                            extreme,
                            window_bars: u32::try_from(k).unwrap(),
                            outcome,
                            resolved_bar_end: Some(bar.end()),
                            known_at: event.time(),
                            coverage,
                        };
                        let fact = if inside {
                            StructureEvent::Sfp(done)
                        } else {
                            StructureEvent::Break(done)
                        };
                        facts.push((index, timeframe, bar.end(), facts.len(), fact.to_string()));
                        if resolved.len() == MAX_LEVELS {
                            let oldest = (0..resolved.len())
                                .min_by_key(|&i| (resolved[i].time, resolved[i].level.price, i))
                                .unwrap();
                            resolved.remove(oldest);
                        }
                        resolved.push(done);
                    }
                    pending = still;
                    // Confirmations of the swings this bar completes.
                    for (_, swing) in swings.iter().filter(|(confirm, _)| *confirm == next_bar) {
                        let same: Vec<usize> = (0..active.len())
                            .filter(|&i| active[i].side == swing.side)
                            .collect();
                        if same.len() == MAX_LEVELS {
                            let oldest = *same
                                .iter()
                                .min_by_key(|&&i| (active[i].confirmed_bar_end, active[i].price))
                                .unwrap();
                            active.remove(oldest);
                        }
                        active.push(*swing);
                        facts.push((
                            index,
                            timeframe,
                            bar.end(),
                            facts.len(),
                            StructureEvent::Swing(*swing).to_string(),
                        ));
                    }
                    next_bar += 1;
                }
                // The trade's sweeps.
                let MarketEvent::Trade(trade) = event else {
                    continue;
                };
                let mut swept: Vec<Swing> = active
                    .iter()
                    .copied()
                    .filter(|level| match level.side {
                        Side::High => trade.price > level.price,
                        Side::Low => trade.price < level.price,
                    })
                    .collect();
                active.retain(|level| match level.side {
                    Side::High => trade.price <= level.price,
                    Side::Low => trade.price >= level.price,
                });
                swept.sort_by_key(|level| {
                    let crossing = match level.side {
                        Side::High => level.price.units(),
                        Side::Low => -level.price.units(),
                    };
                    (crossing, level.confirmed_bar_end)
                });
                for level in swept {
                    let sweep = RefSweep {
                        level: make_level(&level, next_bar),
                        time: trade.time,
                        price: trade.price,
                    };
                    pending.push(sweep);
                    let fact = StructureEvent::Sweep(Sweep {
                        feature,
                        timeframe,
                        level: sweep.level,
                        time: trade.time,
                        price: trade.price,
                        extreme: trade.price,
                        window_bars: 0,
                        outcome: SweepOutcome::Pending,
                        resolved_bar_end: None,
                        known_at: trade.time,
                        coverage: sweep.level.coverage,
                    });
                    facts.push((index, timeframe, trade.time, facts.len(), fact.to_string()));
                }
            }
            // The final registry, as the engine displays it.
            let mut registry = LevelRegistry::new(feature, timeframe);
            for level in &active {
                let level = make_level(level, bars.len());
                let list = match level.side {
                    Side::High => &mut registry.highs,
                    Side::Low => &mut registry.lows,
                };
                list.push(level);
                list.sort_by_key(|level| (level.price, level.confirmed_bar_end));
            }
            registry.pending = pending
                .iter()
                .map(|sweep| Sweep {
                    feature,
                    timeframe,
                    level: sweep.level,
                    time: sweep.time,
                    price: sweep.price,
                    extreme: sweep.price,
                    window_bars: 0,
                    outcome: SweepOutcome::Pending,
                    resolved_bar_end: None,
                    known_at: sweep.time,
                    coverage: sweep.level.coverage,
                })
                .collect();
            registry.resolved = resolved;
            finals.push(registry.to_string());
        }
        // Within an event: the closed bars in (end, timeframe) order —
        // each bar's resolutions before its confirmations — then the sweeps,
        // timeframe ascending. Resolutions and confirmations are timed at
        // their bar's end, sweeps at the trade.
        facts.sort_by_key(|(index, timeframe, time, position, line)| {
            (
                *index,
                line.starts_with("sweep "),
                *time,
                *timeframe,
                *position,
            )
        });
        let mut by_event = vec![Vec::new(); events.len()];
        for (index, _, _, _, line) in facts {
            by_event[index].push(line);
        }
        (by_event, finals)
    }

    /// A random walk on a whole-USDT grid, so equal extremes and touches are
    /// common: trades seconds to minutes apart, jumps of hours, trades gaps
    /// and mark prices.
    fn walk_tape(seed: u64, len: usize) -> Vec<MarketEvent> {
        let mut lcg = Lcg(seed);
        let mut time = lcg.below(DAY as u64);
        let mut price = 60_000;
        let mut trade_id = 0;
        let mut events = Vec::new();
        while events.len() < len {
            time += if lcg.below(60) == 0 {
                lcg.below(8 * 3_600_000)
            } else {
                lcg.below(120_000)
            };
            match lcg.below(50) {
                0 => {
                    let start = time + lcg.below(120_000);
                    time = start + lcg.below(1_800_000);
                    events.push(gap(Stream::Trades, start, time, GapReason::Disconnected));
                }
                1 => events.push(mark(time, 1)),
                _ => {
                    price += lcg.below(9) - 4;
                    trade_id += 1;
                    events.push(trade_at(time, trade_id, usdt(price)));
                }
            }
        }
        events
    }

    /// The random tapes of the reference and look-ahead tests: the bar
    /// tests' tapes (uniform prices, every bar wide) and coarse walks.
    fn random_tapes() -> Vec<Vec<MarketEvent>> {
        let mut tapes: Vec<Vec<MarketEvent>> = [21, 22, 23]
            .into_iter()
            .map(|seed| random_tape(seed, 6_000))
            .collect();
        tapes.extend([1, 2, 3].into_iter().map(|seed| walk_tape(seed, 6_000)));
        tapes
    }

    /// Runs `events` and returns the engine, the bars and the facts of every
    /// event.
    fn trace(
        events: &[MarketEvent],
    ) -> (MarketStateEngine, Vec<Vec<Bar>>, Vec<Vec<StructureEvent>>) {
        let mut engine = MarketStateEngine::new();
        let mut closed = Vec::new();
        let mut facts = Vec::new();
        for event in events {
            engine.apply(event).unwrap();
            closed.push(engine.closed_bars().to_vec());
            facts.push(engine.structure_events().to_vec());
        }
        (engine, closed, facts)
    }

    #[test]
    fn random_tapes_match_the_written_definitions() {
        let mut counts = [0_usize; 6];
        let mut pending = 0;
        for (tape_index, tape) in random_tapes().iter().enumerate() {
            let (engine, closed, facts) = trace(tape);
            let (expected, finals) = reference(tape, &closed);
            for (index, (facts, expected)) in facts.iter().zip(&expected).enumerate() {
                let lines: Vec<String> = facts.iter().map(ToString::to_string).collect();
                assert_eq!(&lines, expected, "tape {tape_index}, event {index}");
                let sweeps = facts
                    .iter()
                    .filter(|fact| matches!(fact, StructureEvent::Sweep(_)))
                    .count();
                if sweeps > 1 {
                    counts[4] += 1;
                }
                for fact in facts {
                    match fact {
                        StructureEvent::Swing(_) => counts[0] += 1,
                        StructureEvent::Sweep(sweep) => {
                            counts[1] += 1;
                            if sweep.level.touches > 0 {
                                counts[5] += 1;
                            }
                        }
                        StructureEvent::Sfp(_) => counts[2] += 1,
                        StructureEvent::Break(_) => counts[3] += 1,
                    }
                }
            }
            for ((timeframe, structure), expected) in engine.state().structure.iter().zip(&finals) {
                let registry = structure.levels.ready().unwrap_or_else(|| {
                    panic!("tape {tape_index}: {timeframe} structure is not ready")
                });
                assert_eq!(
                    &registry.to_string(),
                    expected,
                    "tape {tape_index}, {timeframe}"
                );
                pending += registry.pending().len();
            }
        }
        // Every rule fired, often: swings, sweeps, SFPs, breaks, multi-level
        // sweeps, and sweeps of touched levels.
        let [swings, sweeps, sfps, breaks, multi, touched] = counts;
        assert!(swings > 2_000, "{counts:?}");
        assert!(sfps > 200 && breaks > 200, "{counts:?}");
        // Every sweep resolved, or is pending at the end.
        assert_eq!(sweeps, sfps + breaks + pending, "{counts:?}");
        assert!(multi > 50 && touched > 200, "{counts:?}");
    }

    #[test]
    fn no_structure_event_is_visible_before_the_event_that_emits_it() {
        // Facts defined by X but emitted by a re-priced event, over all tapes.
        let mut emitted_after = 0;
        for (tape_index, tape) in random_tapes().iter().enumerate() {
            let mut engine = MarketStateEngine::new();
            for event in tape {
                engine.apply(event).unwrap();
                let now = event.time();
                let closed = engine.closed_bars();
                for fact in engine.structure_events() {
                    // 1. Visible from the event that emitted it on, never
                    // earlier, and defined at or before it…
                    assert_eq!(fact.time(), now, "tape {tape_index}: {fact}");
                    assert!(bar_time(fact) <= now, "tape {tape_index}: {fact}");
                    let timeframe = fact.timeframe();
                    let closing = || {
                        closed
                            .iter()
                            .find(|bar| bar.timeframe == timeframe && bar.end() == bar_time(fact))
                    };
                    // 2. …at the end of the N-th bar after a swing, the
                    // sweeping trade, or the end of the resolving bar.
                    match fact {
                        StructureEvent::Swing(swing) => {
                            let bar = closing().unwrap_or_else(|| panic!("{fact}"));
                            assert_eq!(
                                bar.open_time.as_millis() - swing.swing_time.as_millis(),
                                SWING_BARS as i64 * timeframe.millis()
                            );
                            assert_eq!(swing.swing_time.as_millis() % timeframe.millis(), 0);
                        }
                        StructureEvent::Sweep(sweep) => {
                            let MarketEvent::Trade(trade) = event else {
                                panic!("{fact} without a trade")
                            };
                            assert_eq!((sweep.time, sweep.price), (trade.time, trade.price));
                            assert!(sweep.level.confirmed_bar_end <= sweep.time);
                        }
                        StructureEvent::Sfp(sweep) | StructureEvent::Break(sweep) => {
                            assert!(closing().is_some(), "{fact}");
                            assert!(sweep.time < bar_time(fact));
                            assert!(sweep.window_bars <= SFP_WINDOW_BARS);
                        }
                    }
                }
                // 3. No state lists anything before its time.
                let as_of = engine.state().as_of.unwrap();
                for (_, structure) in engine.state().structure.iter() {
                    if let FeatureValue::Ready(swings) = &structure.swings {
                        for swing in [swings.high, swings.low].into_iter().flatten() {
                            assert!(swing.confirmed_bar_end <= swing.known_at);
                            assert!(swing.known_at <= as_of);
                        }
                    }
                    if let FeatureValue::Ready(registry) = &structure.levels {
                        for sweep in registry.pending().iter().chain(registry.resolved()) {
                            assert!(sweep.known_at <= as_of);
                            if let Some(end) = sweep.resolved_bar_end {
                                assert!(end <= sweep.known_at);
                            }
                        }
                        for level in registry.levels() {
                            assert!(level.created_at <= as_of, "{level:?}");
                            assert!(level.age_ms(as_of) >= 0);
                        }
                    }
                }
            }
            // 4. Re-pricing every trade from a bar boundary X on changes no
            // swing, SFP or break whose bar ends by X, and no sweep before
            // X — those emitted by a re-priced event included.
            let boundary = Timeframe::M15.open_of(tape[tape.len() / 2].time()).unwrap();
            let mut lcg = Lcg(0x6d69_6500_0000_0021 + tape_index as u64);
            let mutated: Vec<MarketEvent> = tape
                .iter()
                .map(|event| match event {
                    MarketEvent::Trade(trade) if trade.time >= boundary => {
                        let shift = (lcg.below(4_001) - 2_000) * SCALE / 100;
                        MarketEvent::Trade(Trade {
                            price: Price::from_units(trade.price.units() + shift),
                            ..*trade
                        })
                    }
                    other => other.clone(),
                })
                .collect();
            let visible = |facts: &[Vec<StructureEvent>]| -> Vec<(usize, String)> {
                facts
                    .iter()
                    .enumerate()
                    .flat_map(|(index, facts)| {
                        facts
                            .iter()
                            .filter(|fact| match fact {
                                StructureEvent::Sweep(sweep) => sweep.time < boundary,
                                _ => bar_time(fact) <= boundary,
                            })
                            .map(move |fact| (index, fact.to_string()))
                    })
                    .collect()
            };
            let (_, _, before) = trace(tape);
            let (_, _, after) = trace(&mutated);
            let original = visible(&before);
            assert!(
                original.len() > 100,
                "tape {tape_index}: {}",
                original.len()
            );
            assert_eq!(original, visible(&after), "tape {tape_index}");
            emitted_after += original
                .iter()
                .filter(|(index, _)| tape[*index].time() >= boundary)
                .count();
            // The re-pricing does change what comes after X.
            assert_ne!(after, before, "tape {tape_index}");
        }
        assert!(emitted_after > 0);
    }

    /// The trades gap of the golden tape: 10:00 to 10:45 UTC on day 9.
    const GOLDEN_GAP_START: i64 = 9 * DAY + 10 * 3_600_000;
    const GOLDEN_GAP_END: i64 = GOLDEN_GAP_START + 45 * 60_000;

    /// The structure golden tape: 13:00 UTC on day 0 (a partial day) to
    /// 00:00 UTC on day 20. A random walk in cents with drifting trends,
    /// trades 20 s to 160 s apart, and a trades gap on day 9.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0037);
        let mut events = Vec::new();
        let mut time = 13 * 3_600_000;
        let mut cents = 62_000 * 100;
        let mut trend = 0;
        let mut trade_id = 0;
        let mut gapped = false;
        while time < 20 * DAY {
            time += 20_000 + lcg.below(140_000);
            if !gapped && time >= GOLDEN_GAP_START {
                events.push(gap(
                    Stream::Trades,
                    GOLDEN_GAP_START,
                    GOLDEN_GAP_END,
                    GapReason::Disconnected,
                ));
                time = GOLDEN_GAP_END + 1 + lcg.below(30_000);
                gapped = true;
            }
            if lcg.below(400) == 0 {
                trend = lcg.below(7) - 3;
            }
            cents += trend + lcg.below(1_001) - 500;
            trade_id += 1;
            events.push(trade_at(
                time,
                trade_id,
                Price::from_units(cents * (SCALE / 100)),
            ));
        }
        events
    }

    /// The golden lines of `timeframe` — its swings, or its sweeps and
    /// resolutions — as `<event time> <fact>`, and its final registry.
    fn golden_lines(timeframe: Timeframe, swings: bool) -> (Vec<String>, String) {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            for fact in engine.structure_events() {
                let is_swing = matches!(fact, StructureEvent::Swing(_));
                if fact.timeframe() == timeframe && is_swing == swings {
                    lines.push(format!("{} {fact}", event.time()));
                }
            }
        }
        let registry = registry_of(&engine, timeframe).to_string();
        (lines, registry)
    }

    /// The digest of every golden line.
    fn digest(lines: &[String]) -> String {
        let mut hasher = Fingerprinter::new();
        hasher.write_len(lines.len());
        for line in lines {
            hasher.write_str(line);
        }
        hasher.finish().to_string()
    }

    /// Checks a golden: the line count, the first lines verbatim, the digest
    /// of all of them and, for levels, the final registry.
    fn assert_golden(
        timeframe: Timeframe,
        swings: bool,
        count: usize,
        first: &[&str],
        all: &str,
        registry: &str,
    ) {
        let (lines, final_registry) = golden_lines(timeframe, swings);
        let head: Vec<&str> = lines.iter().take(12).map(String::as_str).collect();
        assert_eq!(head, first, "{timeframe}");
        assert_eq!(
            (lines.len(), digest(&lines).as_str()),
            (count, all),
            "{timeframe}"
        );
        if !swings {
            assert_eq!(final_registry, registry, "{timeframe}");
        }
    }

    #[test]
    fn golden_structure_swing_15m_v1() {
        assert_golden(
            Timeframe::M15,
            true,
            348,
            &GOLDEN_SWING_15M,
            "4d4c5a9d49759812",
            "",
        );
    }

    #[test]
    fn golden_structure_levels_15m_v1() {
        assert_golden(
            Timeframe::M15,
            false,
            642,
            &GOLDEN_LEVELS_15M,
            "c9ce15cf7b9884e7",
            GOLDEN_REGISTRY_15M,
        );
    }

    #[test]
    fn golden_structure_swing_1h_v1() {
        assert_golden(
            Timeframe::H1,
            true,
            87,
            &GOLDEN_SWING_1H,
            "e98543069e2daa25",
            "",
        );
    }

    #[test]
    fn golden_structure_levels_1h_v1() {
        assert_golden(
            Timeframe::H1,
            false,
            152,
            &GOLDEN_LEVELS_1H,
            "a2396c586df1c651",
            GOLDEN_REGISTRY_1H,
        );
    }

    #[test]
    fn golden_structure_swing_4h_v1() {
        assert_golden(
            Timeframe::H4,
            true,
            17,
            &GOLDEN_SWING_4H,
            "2ac87cc657ed3da3",
            "",
        );
    }

    #[test]
    fn golden_structure_levels_4h_v1() {
        assert_golden(
            Timeframe::H4,
            false,
            26,
            &GOLDEN_LEVELS_4H,
            "c6c0daa1720b9fe1",
            GOLDEN_REGISTRY_4H,
        );
    }

    #[test]
    fn golden_structure_swing_1d_v1() {
        assert_golden(
            Timeframe::D1,
            true,
            3,
            &GOLDEN_SWING_1D,
            "1156d278f18be5d7",
            "",
        );
    }

    #[test]
    fn golden_structure_levels_1d_v1() {
        assert_golden(
            Timeframe::D1,
            false,
            4,
            &GOLDEN_LEVELS_1D,
            "a19e1ce72cea73a6",
            GOLDEN_REGISTRY_1D,
        );
    }

    const GOLDEN_SWING_15M: [&str; 12] = [
        "57647333ms swing 15m high price=62041.32000000 bar=54000000ms confirmed=57600000ms complete structure.swing.15m@1",
        "64843655ms swing 15m low price=61989.45000000 bar=61200000ms confirmed=64800000ms complete structure.swing.15m@1",
        "68473429ms swing 15m low price=61984.23000000 bar=64800000ms confirmed=68400000ms complete structure.swing.15m@1",
        "69345091ms swing 15m high price=62007.47000000 bar=65700000ms confirmed=69300000ms complete structure.swing.15m@1",
        "78440182ms swing 15m low price=62014.25000000 bar=74700000ms confirmed=78300000ms complete structure.swing.15m@1",
        "81012076ms swing 15m high price=62055.56000000 bar=77400000ms confirmed=81000000ms complete structure.swing.15m@1",
        "84624322ms swing 15m low price=62023.29000000 bar=81000000ms confirmed=84600000ms complete structure.swing.15m@1",
        "88211872ms swing 15m high price=62072.03000000 bar=84600000ms confirmed=88200000ms complete structure.swing.15m@1",
        "91856933ms swing 15m low price=62037.29000000 bar=88200000ms confirmed=91800000ms complete structure.swing.15m@1",
        "94562247ms swing 15m high price=62093.41000000 bar=90900000ms confirmed=94500000ms complete structure.swing.15m@1",
        "97233178ms swing 15m low price=62070.53000000 bar=93600000ms confirmed=97200000ms complete structure.swing.15m@1",
        "102643012ms swing 15m high price=62124.87000000 bar=99000000ms confirmed=102600000ms complete structure.swing.15m@1",
    ];
    const GOLDEN_LEVELS_15M: [&str; 12] = [
        "65533324ms sweep 15m low level=61989.45000000 bar=61200000ms confirmed=64800000ms touches=0 at=65533324ms price=61986.71000000 extreme=61986.71000000 bars=0 pending complete structure.levels.15m@1",
        "66606989ms sfp 15m low level=61989.45000000 bar=61200000ms confirmed=64800000ms touches=0 at=65533324ms price=61986.71000000 extreme=61984.23000000 bars=2 sfp resolved=66600000ms complete structure.levels.15m@1",
        "69818319ms sweep 15m high level=62007.47000000 bar=65700000ms confirmed=69300000ms touches=0 at=69818319ms price=62007.71000000 extreme=62007.71000000 bars=0 pending complete structure.levels.15m@1",
        "71109668ms break 15m high level=62007.47000000 bar=65700000ms confirmed=69300000ms touches=0 at=69818319ms price=62007.71000000 extreme=62027.16000000 bars=2 break resolved=71100000ms complete structure.levels.15m@1",
        "76338550ms sweep 15m high level=62041.32000000 bar=54000000ms confirmed=57600000ms touches=11 at=76338550ms price=62045.58000000 extreme=62045.58000000 bars=0 pending complete structure.levels.15m@1",
        "76525361ms sfp 15m high level=62041.32000000 bar=54000000ms confirmed=57600000ms touches=11 at=76338550ms price=62045.58000000 extreme=62045.58000000 bars=1 sfp resolved=76500000ms complete structure.levels.15m@1",
        "83338318ms sweep 15m high level=62055.56000000 bar=77400000ms confirmed=81000000ms touches=2 at=83338318ms price=62055.59000000 extreme=62055.59000000 bars=0 pending complete structure.levels.15m@1",
        "84624322ms break 15m high level=62055.56000000 bar=77400000ms confirmed=81000000ms touches=2 at=83338318ms price=62055.59000000 extreme=62064.40000000 bars=2 break resolved=84600000ms complete structure.levels.15m@1",
        "90700635ms sweep 15m high level=62072.03000000 bar=84600000ms confirmed=88200000ms touches=2 at=90700635ms price=62073.51000000 extreme=62073.51000000 bars=0 pending complete structure.levels.15m@1",
        "91856933ms break 15m high level=62072.03000000 bar=84600000ms confirmed=88200000ms touches=2 at=90700635ms price=62073.51000000 extreme=62093.41000000 bars=2 break resolved=91800000ms complete structure.levels.15m@1",
        "95568906ms sweep 15m high level=62093.41000000 bar=90900000ms confirmed=94500000ms touches=1 at=95568906ms price=62094.92000000 extreme=62094.92000000 bars=0 pending complete structure.levels.15m@1",
        "97233178ms break 15m high level=62093.41000000 bar=90900000ms confirmed=94500000ms touches=1 at=95568906ms price=62094.92000000 extreme=62110.15000000 bars=2 break resolved=97200000ms complete structure.levels.15m@1",
    ];
    const GOLDEN_REGISTRY_15M: &str = "highs=62269.84000000/5,62286.90000000/5,62301.02000000/13,62322.97000000/5 lows=61949.95000000/0,61994.41000000/7,61999.60000000/3,62014.78000000/5,62038.37000000/1,62068.43000000/14,62073.42000000/2,62103.26000000/0,62138.62000000/12,62159.51000000/19,62160.30000000/18,62171.17000000/42,62186.25000000/19,62187.11000000/13,62200.48000000/12,62219.82000000/32,62222.78000000/20,62224.63000000/3 pending=- swept=low:62219.23000000:sfp:62219.05000000,low:62210.98000000:break:62207.37000000,high:62241.06000000:break:62260.84000000,high:62243.18000000:break:62260.84000000,high:62247.25000000:break:62260.84000000,high:62262.64000000:break:62274.16000000,high:62271.25000000:sfp:62274.16000000,high:62285.27000000:sfp:62286.37000000,high:62320.65000000:sfp:62322.97000000,low:62304.66000000:break:62283.58000000,low:62295.62000000:break:62283.58000000,low:62264.84000000:break:62258.32000000,low:62259.58000000:sfp:62258.32000000,high:62286.46000000:sfp:62286.70000000,high:62286.70000000:sfp:62286.90000000,low:62261.02000000:sfp:62258.00000000,low:62247.75000000:break:62235.04000000,low:62240.58000000:sfp:62235.04000000,high:62259.54000000:sfp:62269.84000000,low:62230.75000000:sfp:62224.63000000 structure.levels.15m@1";
    const GOLDEN_SWING_1H: [&str; 12] = [
        "79249692ms swing 1h low price=61984.23000000 bar=64800000ms confirmed=79200000ms complete structure.swing.1h@1",
        "144024080ms swing 1h high price=62226.24000000 bar=129600000ms confirmed=144000000ms complete structure.swing.1h@1",
        "154864642ms swing 1h low price=62166.93000000 bar=140400000ms confirmed=154800000ms complete structure.swing.1h@1",
        "172863782ms swing 1h high price=62219.09000000 bar=158400000ms confirmed=172800000ms complete structure.swing.1h@1",
        "183683150ms swing 1h low price=62166.89000000 bar=169200000ms confirmed=183600000ms complete structure.swing.1h@1",
        "194508857ms swing 1h high price=62213.04000000 bar=180000000ms confirmed=194400000ms complete structure.swing.1h@1",
        "205266691ms swing 1h low price=62186.71000000 bar=190800000ms confirmed=205200000ms complete structure.swing.1h@1",
        "212505146ms swing 1h high price=62240.38000000 bar=198000000ms confirmed=212400000ms complete structure.swing.1h@1",
        "223235409ms swing 1h low price=62175.78000000 bar=208800000ms confirmed=223200000ms complete structure.swing.1h@1",
        "234074694ms swing 1h high price=62248.64000000 bar=219600000ms confirmed=234000000ms complete structure.swing.1h@1",
        "255661507ms swing 1h low price=62143.56000000 bar=241200000ms confirmed=255600000ms complete structure.swing.1h@1",
        "273603971ms swing 1h high price=62218.68000000 bar=259200000ms confirmed=273600000ms complete structure.swing.1h@1",
    ];
    const GOLDEN_LEVELS_1H: [&str; 12] = [
        "171233403ms sweep 1h low level=62166.93000000 bar=140400000ms confirmed=154800000ms touches=4 at=171233403ms price=62166.89000000 extreme=62166.89000000 bars=0 pending complete structure.levels.1h@1",
        "172863782ms sfp 1h low level=62166.93000000 bar=140400000ms confirmed=154800000ms touches=4 at=171233403ms price=62166.89000000 extreme=62166.89000000 bars=1 sfp resolved=172800000ms complete structure.levels.1h@1",
        "196187182ms sweep 1h high level=62213.04000000 bar=180000000ms confirmed=194400000ms touches=0 at=196187182ms price=62214.31000000 extreme=62214.31000000 bars=0 pending complete structure.levels.1h@1",
        "196411285ms sweep 1h high level=62219.09000000 bar=158400000ms confirmed=172800000ms touches=6 at=196411285ms price=62220.49000000 extreme=62220.49000000 bars=0 pending complete structure.levels.1h@1",
        "196798099ms sweep 1h high level=62226.24000000 bar=129600000ms confirmed=144000000ms touches=13 at=196798099ms price=62227.54000000 extreme=62227.54000000 bars=0 pending complete structure.levels.1h@1",
        "201647892ms break 1h high level=62213.04000000 bar=180000000ms confirmed=194400000ms touches=0 at=196187182ms price=62214.31000000 extreme=62240.38000000 bars=2 break resolved=201600000ms complete structure.levels.1h@1",
        "201647892ms break 1h high level=62219.09000000 bar=158400000ms confirmed=172800000ms touches=6 at=196411285ms price=62220.49000000 extreme=62240.38000000 bars=2 break resolved=201600000ms complete structure.levels.1h@1",
        "201647892ms sfp 1h high level=62226.24000000 bar=129600000ms confirmed=144000000ms touches=13 at=196798099ms price=62227.54000000 extreme=62240.38000000 bars=2 sfp resolved=201600000ms complete structure.levels.1h@1",
        "209740020ms sweep 1h low level=62186.71000000 bar=190800000ms confirmed=205200000ms touches=1 at=209740020ms price=62182.46000000 extreme=62182.46000000 bars=0 pending complete structure.levels.1h@1",
        "216118841ms sfp 1h low level=62186.71000000 bar=190800000ms confirmed=205200000ms touches=1 at=209740020ms price=62182.46000000 extreme=62175.78000000 bars=2 sfp resolved=216000000ms complete structure.levels.1h@1",
        "220737590ms sweep 1h high level=62240.38000000 bar=198000000ms confirmed=212400000ms touches=2 at=220737590ms price=62242.07000000 extreme=62242.07000000 bars=0 pending complete structure.levels.1h@1",
        "223235409ms sfp 1h high level=62240.38000000 bar=198000000ms confirmed=212400000ms touches=2 at=220737590ms price=62242.07000000 extreme=62248.64000000 bars=1 sfp resolved=223200000ms complete structure.levels.1h@1",
    ];
    const GOLDEN_REGISTRY_1H: &str = "highs=62286.90000000/2,62322.97000000/0 lows=61797.69000000/12,61811.00000000/0,61994.41000000/0,62068.43000000/2,62073.42000000/0,62159.51000000/5,62186.25000000/4,62200.48000000/4,62222.78000000/3 pending=- swept=high:61933.05000000:sfp:61939.91000000,high:61939.23000000:sfp:61939.91000000,high:61926.28000000:break:61966.90000000,high:61963.74000000:sfp:61966.90000000,high:61998.44000000:sfp:62002.33000000,high:62066.17000000:break:62101.21000000,high:62066.99000000:break:62101.21000000,high:62096.01000000:sfp:62102.03000000,high:62112.78000000:sfp:62113.99000000,high:62113.99000000:sfp:62130.19000000,high:62119.20000000:sfp:62130.19000000,high:62206.65000000:break:62224.09000000,high:62218.68000000:sfp:62224.09000000,high:62224.09000000:sfp:62237.68000000,high:62248.64000000:sfp:62248.71000000,low:62191.04000000:sfp:62190.51000000,high:62257.51000000:sfp:62260.08000000,low:62189.95000000:sfp:62188.14000000,high:62260.08000000:sfp:62271.25000000,high:62271.25000000:sfp:62285.27000000 structure.levels.1h@1";
    const GOLDEN_SWING_4H: [&str; 12] = [
        "187241939ms swing 4h high price=62226.24000000 bar=129600000ms confirmed=187200000ms complete structure.swing.4h@1",
        "273603971ms swing 4h high price=62248.64000000 bar=216000000ms confirmed=273600000ms complete structure.swing.4h@1",
        "475317810ms swing 4h low price=61947.62000000 bar=417600000ms confirmed=475200000ms complete structure.swing.4h@1",
        "633605489ms swing 4h high price=62096.01000000 bar=576000000ms confirmed=633600000ms complete structure.swing.4h@1",
        "734425315ms swing 4h high price=61998.44000000 bar=676800000ms confirmed=734400000ms complete structure.swing.4h@1",
        "820835004ms swing 4h low price=61818.93000000 bar=763200000ms confirmed=820800000ms feed_gap structure.swing.4h@1",
        "921640894ms swing 4h high price=61946.77000000 bar=864000000ms confirmed=921600000ms complete structure.swing.4h@1",
        "950494110ms swing 4h low price=61844.90000000 bar=892800000ms confirmed=950400000ms complete structure.swing.4h@1",
        "1036825462ms swing 4h high price=61963.74000000 bar=979200000ms confirmed=1036800000ms complete structure.swing.4h@1",
        "1080081240ms swing 4h low price=61824.55000000 bar=1022400000ms confirmed=1080000000ms complete structure.swing.4h@1",
        "1108902383ms swing 4h high price=61939.23000000 bar=1051200000ms confirmed=1108800000ms complete structure.swing.4h@1",
        "1166485816ms swing 4h high price=61933.05000000 bar=1108800000ms confirmed=1166400000ms complete structure.swing.4h@1",
    ];
    const GOLDEN_LEVELS_4H: [&str; 12] = [
        "196798099ms sweep 4h high level=62226.24000000 bar=129600000ms confirmed=187200000ms touches=0 at=196798099ms price=62227.54000000 extreme=62227.54000000 bars=0 pending complete structure.levels.4h@1",
        "201647892ms sfp 4h high level=62226.24000000 bar=129600000ms confirmed=187200000ms touches=0 at=196798099ms price=62227.54000000 extreme=62240.38000000 bars=1 sfp resolved=201600000ms complete structure.levels.4h@1",
        "636919313ms sweep 4h low level=61947.62000000 bar=417600000ms confirmed=475200000ms touches=0 at=636919313ms price=61947.04000000 extreme=61947.04000000 bars=0 pending complete structure.levels.4h@1",
        "662460441ms break 4h low level=61947.62000000 bar=417600000ms confirmed=475200000ms touches=0 at=636919313ms price=61947.04000000 extreme=61895.24000000 bars=2 break resolved=662400000ms complete structure.levels.4h@1",
        "982129853ms sweep 4h high level=61946.77000000 bar=864000000ms confirmed=921600000ms touches=3 at=982129853ms price=61946.83000000 extreme=61946.83000000 bars=0 pending complete structure.levels.4h@1",
        "993607231ms sfp 4h high level=61946.77000000 bar=864000000ms confirmed=921600000ms touches=3 at=982129853ms price=61946.83000000 extreme=61963.74000000 bars=1 sfp resolved=993600000ms complete structure.levels.4h@1",
        "1030309756ms sweep 4h low level=61844.90000000 bar=892800000ms confirmed=950400000ms touches=2 at=1030309756ms price=61843.19000000 extreme=61843.19000000 bars=0 pending complete structure.levels.4h@1",
        "1051300499ms sfp 4h low level=61844.90000000 bar=892800000ms confirmed=950400000ms touches=2 at=1030309756ms price=61843.19000000 extreme=61824.55000000 bars=2 sfp resolved=1051200000ms complete structure.levels.4h@1",
        "1155722652ms sweep 4h low level=61824.55000000 bar=1022400000ms confirmed=1080000000ms touches=2 at=1155722652ms price=61823.32000000 extreme=61823.32000000 bars=0 pending complete structure.levels.4h@1",
        "1166485816ms sfp 4h low level=61824.55000000 bar=1022400000ms confirmed=1080000000ms touches=2 at=1155722652ms price=61823.32000000 extreme=61823.32000000 bars=1 sfp resolved=1166400000ms complete structure.levels.4h@1",
        "1171786093ms sweep 4h low level=61818.93000000 bar=763200000ms confirmed=820800000ms touches=4 at=1171786093ms price=61816.60000000 extreme=61816.60000000 bars=0 pending feed_gap structure.levels.4h@1",
        "1180840452ms sfp 4h low level=61818.93000000 bar=763200000ms confirmed=820800000ms touches=4 at=1171786093ms price=61816.60000000 extreme=61814.63000000 bars=1 sfp resolved=1180800000ms feed_gap structure.levels.4h@1",
    ];
    const GOLDEN_REGISTRY_4H: &str = "highs=62322.97000000/0 lows=61797.69000000/5,61811.00000000/0,62186.25000000/0 pending=- swept=high:62226.24000000:sfp:62240.38000000,low:61947.62000000:break:61895.24000000,high:61946.77000000:sfp:61963.74000000,low:61844.90000000:sfp:61824.55000000,low:61824.55000000:sfp:61823.32000000,low:61818.93000000:sfp:61814.63000000,high:61877.48000000:sfp:61883.00000000,high:61963.74000000:sfp:61966.90000000,high:61933.05000000:break:62002.33000000,high:61939.23000000:break:62002.33000000,high:61998.44000000:sfp:62002.33000000,high:62096.01000000:sfp:62102.03000000,high:62248.64000000:sfp:62257.51000000 structure.levels.4h@1";
    const GOLDEN_SWING_1D: [&str; 3] = [
        "1036825462ms swing 1d low price=61818.93000000 bar=691200000ms confirmed=1036800000ms feed_gap structure.swing.1d@1",
        "1296024422ms swing 1d high price=61963.74000000 bar=950400000ms confirmed=1296000000ms feed_gap structure.swing.1d@1",
        "1468881188ms swing 1d low price=61797.69000000 bar=1123200000ms confirmed=1468800000ms complete structure.swing.1d@1",
    ];
    const GOLDEN_LEVELS_1D: [&str; 4] = [
        "1171786093ms sweep 1d low level=61818.93000000 bar=691200000ms confirmed=1036800000ms touches=1 at=1171786093ms price=61816.60000000 extreme=61816.60000000 bars=0 pending feed_gap structure.levels.1d@1",
        "1209699888ms sfp 1d low level=61818.93000000 bar=691200000ms confirmed=1036800000ms touches=1 at=1171786093ms price=61816.60000000 extreme=61797.69000000 bars=1 sfp resolved=1209600000ms feed_gap structure.levels.1d@1",
        "1323882276ms sweep 1d high level=61963.74000000 bar=950400000ms confirmed=1296000000ms touches=0 at=1323882276ms price=61965.64000000 extreme=61965.64000000 bars=0 pending feed_gap structure.levels.1d@1",
        "1468881188ms break 1d high level=61963.74000000 bar=950400000ms confirmed=1296000000ms touches=0 at=1323882276ms price=61965.64000000 extreme=62164.17000000 bars=2 break resolved=1468800000ms feed_gap structure.levels.1d@1",
    ];
    const GOLDEN_REGISTRY_1D: &str = "highs=- lows=61797.69000000/0 pending=- swept=low:61818.93000000:sfp:61797.69000000,high:61963.74000000:break:62164.17000000 structure.levels.1d@1";
}
