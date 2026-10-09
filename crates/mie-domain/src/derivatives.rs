//! Derivatives context: open interest, funding and liquidations (brief §8,
//! Market State & Regime brief; ADR-042, proposed).
//!
//! - **Open interest** twice, because live and archive sources differ
//!   (decisions 1 and 2):
//!   - `derivatives.oi.sample@1` ([`OiSample`]): the last sample at its
//!     source resolution, with the exact step (ΔOI, elapsed time) against
//!     the previous sample of the same resolution. OI velocity is derived
//!     from the step.
//!   - `derivatives.oi.5m@1` ([`OiGridPoint`]): the latest sample at or
//!     before each UTC 5-minute boundary, with ΔOI against the previous
//!     boundary. Live (10 s) and archive (5 min) data both produce it.
//!
//!   Every value carries its source `resolution_ms`, so research never
//!   mixes the two silently.
//! - **Mark price and indicative funding** `derivatives.mark@1`
//!   ([`MarkState`]), with the basis and the time to the next funding
//!   derived exactly (decision 3).
//! - **Settled funding** `derivatives.funding.settled@1`
//!   ([`SettledFunding`], decision 4).
//! - **Liquidation windows** `derivatives.liq.window.<5m|15m|1h>@1`
//!   ([`LiquidationWindow`]): count and filled quantity by side over the
//!   last 5, 15 or 60 closed 1m bars, on the clock of the order-flow
//!   windows (ADR-035). A lower bound: the exchange stream is throttled
//!   (decision 5).
//!
//! A value becomes visible at the ordering time of the event that carries
//! it (ADR-028, decision 6). Sums and deltas are exact on fixed point
//! (ADR-027) with checked arithmetic; overflow rejects the event. Floats are
//! derived on demand in a fixed operation order and never stored, so the
//! state stays `Eq` (decision 7).
//!
//! Nothing here names or encodes a direction or a signal (ADR-012,
//! ADR-023, ADR-024).

use crate::bars::{Bar, Timeframe};
use crate::event::{
    Aggressor, FeedGap, FundingSettlement, Liquidation, MarkPrice, MarketEvent, OpenInterest,
    Stream,
};
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::num::{Price, Qty, Rate, SCALE};
use crate::time::EventTime;
use std::fmt;

/// The longest elapsed time beyond the source resolution that still makes
/// a step: `derivatives.oi.sample@1` parameter `step_tolerance_ms`
/// (ADR-042, decision 1).
pub const OI_STEP_TOLERANCE_MS: i64 = 15_000;

/// The open-interest grid: `derivatives.oi.5m@1` parameter `grid_ms`
/// (ADR-042, decision 2).
pub const OI_GRID: Timeframe = Timeframe::M5;

/// The oldest sample a grid boundary may take and still be ready:
/// `derivatives.oi.5m@1` parameter `max_age_ms` (ADR-042, decision 2).
pub const OI_MAX_AGE_MS: i64 = 60_000;

/// Liquidation minutes kept, keyed by minute: 60 for the longest window
/// plus one, so a minute is overwritten only by one at least 61 minutes
/// newer, which no window that can still be computed reaches back to.
const RING: usize = 61;

/// The length of a minute in milliseconds.
const MINUTE_MS: i64 = 60_000;

/// Writes `value` or `-` for `None`.
fn write_opt<T: fmt::Display>(f: &mut fmt::Formatter<'_>, value: Option<T>) -> fmt::Result {
    match value {
        Some(value) => write!(f, "{value}"),
        None => f.write_str("-"),
    }
}

/// `delta` per minute over `elapsed_ms`, in BTC: `(delta / 1e8) × 60 000 /
/// elapsed_ms`, in this order (ADR-042, decisions 1 and 2).
fn per_minute(delta: Qty, elapsed_ms: u64) -> f64 {
    (delta.units() as f64 / SCALE as f64) * 60_000.0 / elapsed_ms as f64
}

/// The step of an open-interest sample against the previous one
/// (ADR-042, decision 1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OiStep {
    /// Ordering time of the previous sample.
    pub previous_time: EventTime,
    /// The exact ΔOI: this sample's open interest minus the previous one.
    pub delta: Qty,
    /// Milliseconds since the previous sample; positive and at most
    /// `resolution_ms +` [`OI_STEP_TOLERANCE_MS`].
    pub elapsed_ms: u64,
}

/// The last open-interest sample: `derivatives.oi.sample@1` (ADR-042,
/// decision 1).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `time=20000ms oi=80000.00000000 res=10000ms prev=10000ms elapsed=10000ms
/// delta=1.50000000 vel_per_min=0.09 derivatives.oi.sample@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OiSample {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Ordering time of the sample (ADR-028): archive samples at the end of
    /// their interval, live samples at their delivery time.
    pub time: EventTime,
    /// Open interest in BTC.
    pub open_interest: Qty,
    /// Sampling resolution of the source in milliseconds: 10 000 live,
    /// 300 000 archive.
    pub resolution_ms: u32,
    /// The step against the previous sample; `None` when the chain broke
    /// (first sample, resolution change, open-interest gap, or an elapsed
    /// time outside `(0, resolution_ms + step_tolerance_ms]`).
    pub step: Option<OiStep>,
}

impl OiSample {
    /// OI velocity in BTC per minute: `(delta / 1e8) × 60 000 /
    /// elapsed_ms`; `None` without a step.
    pub fn velocity_per_minute(&self) -> Option<f64> {
        self.step
            .map(|step| per_minute(step.delta, step.elapsed_ms))
    }
}

impl fmt::Display for OiSample {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "time={} oi={} res={}ms ",
            self.time, self.open_interest, self.resolution_ms
        )?;
        match self.step {
            Some(step) => write!(
                f,
                "prev={} elapsed={}ms delta={} vel_per_min=",
                step.previous_time, step.elapsed_ms, step.delta
            )?,
            None => f.write_str("prev=- elapsed=- delta=- vel_per_min=")?,
        }
        write_opt(f, self.velocity_per_minute())?;
        write!(f, " {}", self.feature)
    }
}

/// Open interest at a UTC 5-minute boundary: `derivatives.oi.5m@1`
/// (ADR-042, decision 2).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `boundary=300000ms oi=80000.00000000 sample=295000ms res=10000ms
/// delta=1.50000000 vel_per_min=0.3 derivatives.oi.5m@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OiGridPoint {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The boundary, a multiple of the grid.
    pub boundary: EventTime,
    /// Open interest of the latest sample at or before `boundary`.
    pub open_interest: Qty,
    /// Ordering time of that sample; at most [`OI_MAX_AGE_MS`] before
    /// `boundary`.
    pub sample_time: EventTime,
    /// Sampling resolution of that sample's source in milliseconds.
    pub resolution_ms: u32,
    /// ΔOI against the previous boundary; `None` unless that boundary is
    /// one grid step earlier, was ready and has the same resolution.
    pub delta: Option<Qty>,
}

impl OiGridPoint {
    /// OI velocity in BTC per minute over the grid step: `(delta / 1e8) ×
    /// 60 000 / grid_ms`; `None` without a delta.
    pub fn velocity_per_minute(&self) -> Option<f64> {
        // The grid is a positive whole number of milliseconds.
        let grid_ms = OI_GRID.millis().unsigned_abs();
        self.delta.map(|delta| per_minute(delta, grid_ms))
    }
}

impl fmt::Display for OiGridPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "boundary={} oi={} sample={} res={}ms delta=",
            self.boundary, self.open_interest, self.sample_time, self.resolution_ms
        )?;
        write_opt(f, self.delta)?;
        f.write_str(" vel_per_min=")?;
        write_opt(f, self.velocity_per_minute())?;
        write!(f, " {}", self.feature)
    }
}

/// The last mark and index price with the indicative funding rate:
/// `derivatives.mark@1` (ADR-042, decision 3).
///
/// `Display` prints the canonical line the golden tests pin, ending with
/// the feature key; its time to the next funding is taken at the value's
/// own `time`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MarkState {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Exchange event time of the mark price.
    pub time: EventTime,
    /// Mark price.
    pub mark_price: Price,
    /// Index price.
    pub index_price: Price,
    /// Indicative funding rate for the next settlement — an estimate, not
    /// the settled rate.
    pub funding_rate: Rate,
    /// When the next funding settles.
    pub next_funding_time: EventTime,
}

impl MarkState {
    /// The exact basis `mark − index`; `None` if it leaves the `i64` range.
    pub fn basis(&self) -> Option<Price> {
        self.mark_price
            .units()
            .checked_sub(self.index_price.units())
            .map(Price::from_units)
    }

    /// Milliseconds from `at` to the next funding, `next_funding_time −
    /// at`; negative once it passed, `None` if it leaves the `i64` range.
    /// Consumers pass [`MarketState::as_of`](crate::state::MarketState).
    pub fn time_to_next_funding_ms(&self, at: EventTime) -> Option<i64> {
        self.next_funding_time
            .as_millis()
            .checked_sub(at.as_millis())
    }
}

impl fmt::Display for MarkState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "time={} mark={} index={} basis=",
            self.time, self.mark_price, self.index_price
        )?;
        write_opt(f, self.basis())?;
        write!(
            f,
            " funding={} next_funding={} to_funding=",
            self.funding_rate, self.next_funding_time
        )?;
        write_opt(
            f,
            self.time_to_next_funding_ms(self.time)
                .map(|millis| format!("{millis}ms")),
        )?;
        write!(f, " {}", self.feature)
    }
}

/// The last settled funding rate: `derivatives.funding.settled@1` (ADR-042,
/// decision 4).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `time=28800000ms rate=0.00010000 derivatives.funding.settled@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SettledFunding {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Funding time.
    pub time: EventTime,
    /// The settled rate.
    pub rate: Rate,
}

impl fmt::Display for SettledFunding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "time={} rate={} {}", self.time, self.rate, self.feature)
    }
}

/// Liquidations by side over the last closed minutes:
/// `derivatives.liq.window.<tf>@1` (ADR-042, decision 5).
///
/// A lower bound on liquidation activity: the exchange stream is throttled.
/// `Display` prints the canonical line the golden tests pin, ending with
/// the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidationWindow {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The window length.
    pub timeframe: Timeframe,
    /// Exclusive end of the window's last minute.
    pub end: EventTime,
    /// Liquidation orders that closed a long (`Sell` orders).
    pub long_count: u64,
    /// Filled quantity of those orders.
    pub long_qty: Qty,
    /// Liquidation orders that closed a short (`Buy` orders).
    pub short_count: u64,
    /// Filled quantity of those orders.
    pub short_qty: Qty,
    /// The window contains the minute of the first liquidations-stream
    /// event, so liquidations before consumption started are missing.
    pub partial_start: bool,
    /// The window overlaps a liquidations feed gap.
    pub feed_gap: bool,
}

impl fmt::Display for LiquidationWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let coverage = match (self.partial_start, self.feed_gap) {
            (false, false) => "complete",
            (true, false) => "partial_start",
            (false, true) => "feed_gap",
            (true, true) => "partial_start+feed_gap",
        };
        write!(
            f,
            "{} end={} long={}/{} short={}/{} {coverage} {}",
            self.timeframe,
            self.end,
            self.long_count,
            self.long_qty,
            self.short_count,
            self.short_qty,
            self.feature
        )
    }
}

/// The liquidation window of every length, in [`catalog::LIQ_WINDOWS`]
/// order: the `derivatives.liq.window.*@1` features of the Market State.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiquidationWindows {
    values: [FeatureValue<LiquidationWindow>; 3],
}

impl Default for LiquidationWindows {
    fn default() -> Self {
        Self::new()
    }
}

impl LiquidationWindows {
    /// Every window warming up, for [`catalog::LIQ_WINDOWS`].
    pub fn new() -> Self {
        Self {
            values: catalog::LIQ_WINDOWS.map(|(timeframe, _)| FeatureValue::WarmingUp {
                observed: 0,
                required: window_minutes(timeframe),
            }),
        }
    }

    /// The window of length `timeframe`, if the set computes it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&FeatureValue<LiquidationWindow>> {
        catalog::LIQ_WINDOWS
            .iter()
            .position(|(candidate, _)| *candidate == timeframe)
            .map(|index| &self.values[index])
    }

    /// Every window, shortest first, with its feature
    /// (`derivatives.liq.window.<label>@1`) and value.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (Timeframe, FeatureKey, &FeatureValue<LiquidationWindow>)> {
        catalog::LIQ_WINDOWS
            .iter()
            .zip(&self.values)
            .map(|((timeframe, definition), value)| (*timeframe, definition.key, value))
    }
}

/// The number of 1m bars in a window of length `timeframe`.
fn window_minutes(timeframe: Timeframe) -> u64 {
    // Every window length is a positive whole number of minutes.
    (timeframe.millis() / Timeframe::M1.millis()).unsigned_abs()
}

/// The derivatives features of the Market State (ADR-042).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Derivatives {
    /// `derivatives.oi.sample@1`, warming up until the first open-interest
    /// sample.
    pub oi: FeatureValue<OiSample>,
    /// `derivatives.oi.5m@1`, warming up until the first closed boundary;
    /// `Unavailable(InputInvalid)` when the boundary's sample is stale.
    pub oi_5m: FeatureValue<OiGridPoint>,
    /// `derivatives.mark@1`, warming up until the first mark price.
    pub mark: FeatureValue<MarkState>,
    /// `derivatives.funding.settled@1`, warming up until the first
    /// settlement.
    pub funding_settled: FeatureValue<SettledFunding>,
    /// `derivatives.liq.window.<5m|15m|1h>@1`, each warming up until its
    /// window of closed minutes since the first liquidations-stream event is
    /// full. A lower bound.
    pub liquidations: LiquidationWindows,
}

impl Default for Derivatives {
    fn default() -> Self {
        Self::new()
    }
}

impl Derivatives {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            oi: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            oi_5m: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            mark: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            funding_settled: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            liquidations: LiquidationWindows::new(),
        }
    }
}

/// One open-interest observation, as the tracker keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OiObservation {
    time: EventTime,
    open_interest: Qty,
    resolution_ms: u32,
}

impl OiObservation {
    fn of(oi: &OpenInterest) -> Self {
        Self {
            time: oi.time,
            open_interest: oi.open_interest,
            resolution_ms: oi.resolution_ms,
        }
    }
}

/// The last closed grid boundary: the reference of the next one's delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct GridClose {
    boundary: EventTime,
    sample: OiObservation,
    /// Whether the boundary's sample was fresh enough.
    ready: bool,
}

/// The liquidations of one minute, and whether a gap overlaps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LiqMinute {
    /// Open time of the minute: the slot's key.
    open: EventTime,
    long_count: u64,
    long_qty: Qty,
    short_count: u64,
    short_qty: Qty,
    feed_gap: bool,
}

impl LiqMinute {
    fn empty(open: EventTime) -> Self {
        Self {
            open,
            long_count: 0,
            long_qty: Qty::from_units(0),
            short_count: 0,
            short_qty: Qty::from_units(0),
            feed_gap: false,
        }
    }

    /// Counts `liquidation` on its side, or `None` on overflow.
    fn add(&mut self, liquidation: &Liquidation) -> Option<()> {
        match liquidation.aggressor {
            // A sell order closes a long.
            Aggressor::Sell => {
                self.long_count = self.long_count.checked_add(1)?;
                self.long_qty = self.long_qty.checked_add(liquidation.filled_qty)?;
            }
            // A buy order closes a short.
            Aggressor::Buy => {
                self.short_count = self.short_count.checked_add(1)?;
                self.short_qty = self.short_qty.checked_add(liquidation.filled_qty)?;
            }
        }
        Some(())
    }
}

/// The liquidation minutes, one slot per minute modulo [`RING`]. A slot
/// holds the minute its key names; a stale or empty slot reads as a minute
/// without liquidations or gaps.
///
/// Keyed by minute rather than pushed per closed bar, so liquidations that
/// arrive before the trades stream closes their minute (a trades outage)
/// keep their own minute.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LiqRing {
    minutes: [Option<LiqMinute>; RING],
}

impl LiqRing {
    fn new() -> Self {
        Self {
            minutes: [None; RING],
        }
    }

    /// The slot of the minute opening at `open`.
    fn slot(open: EventTime) -> usize {
        let minute = open.as_millis().div_euclid(MINUTE_MS);
        // `rem_euclid` of a positive `RING` is in `0..RING`.
        usize::try_from(minute.rem_euclid(RING as i64)).unwrap_or(0)
    }

    /// The minute opening at `open`, if the ring holds it.
    fn get(&self, open: EventTime) -> Option<&LiqMinute> {
        self.minutes[Self::slot(open)]
            .as_ref()
            .filter(|minute| minute.open == open)
    }

    /// The minute opening at `open`, replacing whatever older minute its
    /// slot held.
    fn entry(&mut self, open: EventTime) -> &mut LiqMinute {
        let slot = &mut self.minutes[Self::slot(open)];
        if slot.is_none_or(|minute| minute.open != open) {
            *slot = Some(LiqMinute::empty(open));
        }
        slot.get_or_insert(LiqMinute::empty(open))
    }
}

/// The small, per-event part of the tracker: copied on every event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct DerivativesHead {
    /// The current feature values.
    derivatives: Derivatives,
    /// The last open-interest sample.
    last_oi: Option<OiObservation>,
    /// Whether an open-interest gap arrived since `last_oi`.
    oi_gap: bool,
    /// The last closed grid boundary.
    grid: Option<GridClose>,
    /// Open time of the minute of the first liquidations-stream event;
    /// `None` before it.
    liq_start: Option<EventTime>,
    /// Closed 1m bars from `liq_start` on.
    closed_minutes: u64,
}

/// The derivatives engine state (ADR-042): the last open-interest sample
/// and grid boundary, and the liquidation minutes. Engine state; the Market
/// State exposes only [`Derivatives`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DerivativesTracker {
    head: DerivativesHead,
    ring: LiqRing,
}

/// The tracker after one event, committed with the bars it was computed
/// from.
pub(crate) struct DerivativesStep {
    head: DerivativesHead,
    /// `None` when the event was not on the liquidations stream: the ring
    /// is unchanged.
    ring: Option<LiqRing>,
}

impl Default for DerivativesTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl DerivativesTracker {
    /// An empty tracker, every feature warming up.
    pub(crate) fn new() -> Self {
        Self {
            head: DerivativesHead {
                derivatives: Derivatives::new(),
                last_oi: None,
                oi_gap: false,
                grid: None,
                liq_start: None,
                closed_minutes: 0,
            },
            ring: LiqRing::new(),
        }
    }

    /// The current feature values.
    pub(crate) fn derivatives(&self) -> Derivatives {
        self.head.derivatives
    }

    /// Steps the derivatives with `event`, given the bars it closed
    /// (`closed`, in close order), on copies (ADR-042). The ring of
    /// liquidation minutes is copied only for a liquidations-stream event.
    ///
    /// Order: closed 1m bars from the first liquidations-stream event on
    /// are counted and the windows recomputed, then the event applies by
    /// kind: a liquidation counts in its minute; a liquidations gap flags
    /// the minutes and windows it overlaps; an open-interest sample steps
    /// the native chain and closes grid boundaries; an open-interest gap
    /// breaks the chain; a mark price or a settlement replaces its value.
    ///
    /// # Errors
    ///
    /// [`DerivativesError::Overflow`] if a sum, count, delta or time
    /// difference leaves its integer range; nothing is committed then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
    ) -> Result<DerivativesStep, DerivativesError> {
        let mut head = self.head;
        if let Some(start) = head.liq_start {
            let mut end = None;
            for bar in closed
                .iter()
                .filter(|bar| bar.timeframe == Timeframe::M1 && bar.open_time >= start)
            {
                head.closed_minutes = head
                    .closed_minutes
                    .checked_add(1)
                    .ok_or(DerivativesError::Overflow)?;
                end = Some(bar.end());
            }
            if let Some(end) = end {
                head.derivatives.liquidations =
                    windows(&self.ring, end, start, head.closed_minutes)?;
            }
        }

        let mut ring = None;
        match event {
            MarketEvent::Liquidation(liquidation) => {
                let open = minute_of(liquidation.time)?;
                head.liq_start.get_or_insert(open);
                let mut copy = self.ring.clone();
                copy.entry(open)
                    .add(liquidation)
                    .ok_or(DerivativesError::Overflow)?;
                ring = Some(copy);
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Liquidations => {
                let start = *head.liq_start.get_or_insert(minute_of(gap.end)?);
                let mut copy = self.ring.clone();
                head.liquidation_gap(gap, start, &mut copy)?;
                ring = Some(copy);
            }
            MarketEvent::OpenInterest(oi) => head.open_interest(oi)?,
            MarketEvent::FeedGap(gap) if gap.stream == Stream::OpenInterest => {
                head.oi_gap = true;
            }
            MarketEvent::MarkPrice(mark) => head.mark(mark),
            MarketEvent::FundingSettlement(settlement) => head.settlement(settlement),
            // Other streams carry no derivatives context.
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Trade(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::Kline(_) => {}
        }
        Ok(DerivativesStep { head, ring })
    }

    /// Commits a step computed by [`Self::step`].
    pub(crate) fn commit(&mut self, step: DerivativesStep) {
        self.head = step.head;
        if let Some(ring) = step.ring {
            self.ring = ring;
        }
    }
}

/// The open time of the minute containing `time`.
fn minute_of(time: EventTime) -> Result<EventTime, DerivativesError> {
    Timeframe::M1
        .open_of(time)
        .ok_or(DerivativesError::Overflow)
}

/// `time + millis`, checked.
fn shift(time: EventTime, millis: i64) -> Result<EventTime, DerivativesError> {
    time.as_millis()
        .checked_add(millis)
        .map(EventTime::from_millis)
        .ok_or(DerivativesError::Overflow)
}

impl DerivativesHead {
    /// Steps the native chain and closes grid boundaries with one
    /// open-interest sample (ADR-042, decisions 1 and 2).
    fn open_interest(&mut self, oi: &OpenInterest) -> Result<(), DerivativesError> {
        let overflow = || DerivativesError::Overflow;
        let current = OiObservation::of(oi);
        let previous = self.last_oi;

        let step = match previous {
            Some(previous)
                if !self.oi_gap
                    && oi.resolution_ms > 0
                    && previous.resolution_ms == oi.resolution_ms =>
            {
                let elapsed = oi
                    .time
                    .as_millis()
                    .checked_sub(previous.time.as_millis())
                    .ok_or_else(overflow)?;
                let limit = i64::from(oi.resolution_ms) + OI_STEP_TOLERANCE_MS;
                if elapsed > 0 && elapsed <= limit {
                    Some(OiStep {
                        previous_time: previous.time,
                        delta: oi
                            .open_interest
                            .checked_sub(previous.open_interest)
                            .ok_or_else(overflow)?,
                        elapsed_ms: elapsed.unsigned_abs(),
                    })
                } else {
                    None
                }
            }
            _ => None,
        };
        self.derivatives.oi = FeatureValue::Ready(OiSample {
            feature: catalog::DERIVATIVES_OI_SAMPLE_V1.key,
            time: oi.time,
            open_interest: oi.open_interest,
            resolution_ms: oi.resolution_ms,
            step,
        });

        let boundary = OI_GRID.open_of(oi.time).ok_or_else(overflow)?;
        let newer = |grid: Option<GridClose>, boundary: EventTime| {
            grid.is_none_or(|last| boundary > last.boundary)
        };
        if newer(self.grid, boundary) {
            // The boundary before closes first, with the previous sample, so
            // this one's delta has its reference.
            let before = shift(boundary, -OI_GRID.millis())?;
            if newer(self.grid, before)
                && let Some(previous) = previous.filter(|previous| previous.time <= before)
            {
                self.close_boundary(before, previous)?;
            }
            let sample = if oi.time == boundary {
                Some(current)
            } else {
                previous.filter(|previous| previous.time <= boundary)
            };
            // No sample at or before the boundary: nothing to close.
            if let Some(sample) = sample {
                self.close_boundary(boundary, sample)?;
            }
        }
        self.last_oi = Some(current);
        self.oi_gap = false;
        Ok(())
    }

    /// Closes the grid boundary `boundary` with `sample`, the latest sample
    /// at or before it (ADR-042, decision 2).
    fn close_boundary(
        &mut self,
        boundary: EventTime,
        sample: OiObservation,
    ) -> Result<(), DerivativesError> {
        let overflow = || DerivativesError::Overflow;
        let age = boundary
            .as_millis()
            .checked_sub(sample.time.as_millis())
            .ok_or_else(overflow)?;
        let ready = age <= OI_MAX_AGE_MS;
        let reference = shift(boundary, -OI_GRID.millis())?;
        let delta = match self.grid {
            Some(last)
                if ready
                    && last.ready
                    && last.boundary == reference
                    && last.sample.resolution_ms == sample.resolution_ms =>
            {
                Some(
                    sample
                        .open_interest
                        .checked_sub(last.sample.open_interest)
                        .ok_or_else(overflow)?,
                )
            }
            _ => None,
        };
        self.grid = Some(GridClose {
            boundary,
            sample,
            ready,
        });
        self.derivatives.oi_5m = if ready {
            FeatureValue::Ready(OiGridPoint {
                feature: catalog::DERIVATIVES_OI_5M_V1.key,
                boundary,
                open_interest: sample.open_interest,
                sample_time: sample.time,
                resolution_ms: sample.resolution_ms,
                delta,
            })
        } else {
            FeatureValue::Unavailable {
                reason: Unavailability::InputInvalid,
            }
        };
        Ok(())
    }

    /// Replaces the mark value (ADR-042, decision 3).
    fn mark(&mut self, mark: &MarkPrice) {
        self.derivatives.mark = FeatureValue::Ready(MarkState {
            feature: catalog::DERIVATIVES_MARK_V1.key,
            time: mark.time,
            mark_price: mark.mark_price,
            index_price: mark.index_price,
            funding_rate: mark.funding_rate,
            next_funding_time: mark.next_funding_time,
        });
    }

    /// Replaces the settled funding value (ADR-042, decision 4).
    fn settlement(&mut self, settlement: &FundingSettlement) {
        self.derivatives.funding_settled = FeatureValue::Ready(SettledFunding {
            feature: catalog::DERIVATIVES_FUNDING_SETTLED_V1.key,
            time: settlement.time,
            rate: settlement.rate,
        });
    }

    /// Flags what a liquidations gap overlaps (ADR-042, decision 5): every
    /// ready window, and every minute from `start` (the stream's first
    /// minute) on that a window can still reach.
    ///
    /// The minutes of the ready windows end before the gap's end, so the
    /// windows gain the flag directly. Windows computed later end at or
    /// after the minute of the gap's end and reach back at most 60
    /// minutes from there, which bounds the minutes flagged in the ring.
    fn liquidation_gap(
        &mut self,
        gap: &FeedGap,
        start: EventTime,
        ring: &mut LiqRing,
    ) -> Result<(), DerivativesError> {
        let overlaps = |from: EventTime, to: EventTime| gap.start < to && gap.end >= from;
        for (timeframe, value) in catalog::LIQ_WINDOWS
            .iter()
            .map(|(timeframe, _)| *timeframe)
            .zip(&mut self.derivatives.liquidations.values)
        {
            if let FeatureValue::Ready(window) = value {
                let from = shift(window.end, -timeframe.millis())?;
                if overlaps(from, window.end) {
                    window.feed_gap = true;
                }
            }
        }
        let last = minute_of(gap.end)?;
        let reach = shift(last, -Timeframe::H1.millis())?;
        let mut open = minute_of(gap.start)?.max(reach).max(start);
        while open <= last {
            ring.entry(open).feed_gap = true;
            open = shift(open, MINUTE_MS)?;
        }
        Ok(())
    }
}

/// Every liquidation window after the latest closed minute, which ends at
/// `end`; `start` is the stream's first minute.
fn windows(
    ring: &LiqRing,
    end: EventTime,
    start: EventTime,
    closed_minutes: u64,
) -> Result<LiquidationWindows, DerivativesError> {
    let mut values = LiquidationWindows::new().values;
    for ((timeframe, definition), value) in catalog::LIQ_WINDOWS.iter().zip(&mut values) {
        let required = window_minutes(*timeframe);
        *value = if closed_minutes < required {
            FeatureValue::WarmingUp {
                observed: closed_minutes,
                required,
            }
        } else {
            FeatureValue::Ready(window(ring, *timeframe, definition.key, end, start)?)
        };
    }
    Ok(LiquidationWindows { values })
}

/// The window of length `timeframe` ending at `end`; at least a window's
/// worth of minutes closed since `start`.
fn window(
    ring: &LiqRing,
    timeframe: Timeframe,
    feature: FeatureKey,
    end: EventTime,
    start: EventTime,
) -> Result<LiquidationWindow, DerivativesError> {
    let overflow = || DerivativesError::Overflow;
    let from = shift(end, -timeframe.millis())?;
    let mut sum = LiquidationWindow {
        feature,
        timeframe,
        end,
        long_count: 0,
        long_qty: Qty::from_units(0),
        short_count: 0,
        short_qty: Qty::from_units(0),
        partial_start: from <= start,
        feed_gap: false,
    };
    let mut open = from;
    while open < end {
        if let Some(minute) = ring.get(open) {
            sum.long_count = sum
                .long_count
                .checked_add(minute.long_count)
                .ok_or_else(overflow)?;
            sum.long_qty = sum
                .long_qty
                .checked_add(minute.long_qty)
                .ok_or_else(overflow)?;
            sum.short_count = sum
                .short_count
                .checked_add(minute.short_count)
                .ok_or_else(overflow)?;
            sum.short_qty = sum
                .short_qty
                .checked_add(minute.short_qty)
                .ok_or_else(overflow)?;
            sum.feed_gap |= minute.feed_gap;
        }
        open = shift(open, MINUTE_MS)?;
    }
    Ok(sum)
}

/// Why the derivatives could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DerivativesError {
    /// A liquidation sum or count, a ΔOI or a time difference left its
    /// integer range (ADR-027).
    Overflow,
}

impl fmt::Display for DerivativesError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("a derivatives value leaves its integer range"),
        }
    }
}

impl std::error::Error for DerivativesError {}
