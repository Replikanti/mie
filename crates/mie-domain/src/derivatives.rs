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
use crate::fingerprint::Fingerprinter;
use crate::num::{Price, Qty, Rate, SCALE};
use crate::state_hash::StateEncode;
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

impl StateEncode for OiStep {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            previous_time,
            delta,
            elapsed_ms,
        } = self;
        previous_time.encode(f);
        delta.encode(f);
        elapsed_ms.encode(f);
    }
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

impl StateEncode for OiSample {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            time,
            open_interest,
            resolution_ms,
            step,
        } = self;
        feature.encode(f);
        time.encode(f);
        open_interest.encode(f);
        resolution_ms.encode(f);
        step.encode(f);
    }
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

impl StateEncode for OiGridPoint {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            boundary,
            open_interest,
            sample_time,
            resolution_ms,
            delta,
        } = self;
        feature.encode(f);
        boundary.encode(f);
        open_interest.encode(f);
        sample_time.encode(f);
        resolution_ms.encode(f);
        delta.encode(f);
    }
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

impl StateEncode for MarkState {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            time,
            mark_price,
            index_price,
            funding_rate,
            next_funding_time,
        } = self;
        feature.encode(f);
        time.encode(f);
        mark_price.encode(f);
        index_price.encode(f);
        funding_rate.encode(f);
        next_funding_time.encode(f);
    }
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

impl StateEncode for SettledFunding {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            time,
            rate,
        } = self;
        feature.encode(f);
        time.encode(f);
        rate.encode(f);
    }
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

impl StateEncode for LiquidationWindow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            timeframe,
            end,
            long_count,
            long_qty,
            short_count,
            short_qty,
            partial_start,
            feed_gap,
        } = self;
        feature.encode(f);
        timeframe.encode(f);
        end.encode(f);
        long_count.encode(f);
        long_qty.encode(f);
        short_count.encode(f);
        short_qty.encode(f);
        partial_start.encode(f);
        feed_gap.encode(f);
    }
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

impl StateEncode for LiquidationWindows {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { values } = self;
        values.encode(f);
    }
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

impl StateEncode for Derivatives {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            oi,
            oi_5m,
            mark,
            funding_settled,
            liquidations,
        } = self;
        oi.encode(f);
        oi_5m.encode(f);
        mark.encode(f);
        funding_settled.encode(f);
        liquidations.encode(f);
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::{Lcg, random_tape};
    use crate::event::GapReason;
    use crate::event::samples::{gap, t, trade};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;
    const MINUTE: i64 = 60_000;
    const GRID: i64 = 300_000;
    const LIVE: u32 = 10_000;
    const ARCHIVE: u32 = 300_000;

    fn qty(text: &str) -> Qty {
        text.parse().unwrap()
    }

    fn oi(millis: i64, open_interest: &str, resolution_ms: u32) -> MarketEvent {
        MarketEvent::OpenInterest(OpenInterest {
            time: t(millis),
            open_interest: qty(open_interest),
            resolution_ms,
        })
    }

    fn liquidation(millis: i64, aggressor: Aggressor, filled: &str) -> MarketEvent {
        MarketEvent::Liquidation(Liquidation {
            time: t(millis),
            aggressor,
            price: Price::from_units(6_350_000_000_000),
            avg_price: Price::from_units(6_350_100_000_000),
            filled_qty: qty(filled),
        })
    }

    fn mark(millis: i64, mark: &str, index: &str, rate: &str, next: i64) -> MarketEvent {
        MarketEvent::MarkPrice(MarkPrice {
            time: t(millis),
            mark_price: mark.parse().unwrap(),
            index_price: index.parse().unwrap(),
            funding_rate: rate.parse().unwrap(),
            next_funding_time: t(next),
        })
    }

    fn settlement(millis: i64, rate: &str) -> MarketEvent {
        MarketEvent::FundingSettlement(FundingSettlement {
            time: t(millis),
            rate: rate.parse().unwrap(),
        })
    }

    fn warming<T>(observed: u64, required: u64) -> FeatureValue<T> {
        FeatureValue::WarmingUp { observed, required }
    }

    fn show<T: fmt::Display>(value: &FeatureValue<T>) -> String {
        match value {
            FeatureValue::Ready(value) => value.to_string(),
            FeatureValue::WarmingUp { observed, required } => {
                format!("warming {observed}/{required}")
            }
            FeatureValue::Unavailable { reason } => format!("unavailable {reason:?}"),
        }
    }

    /// The engine's derivatives after each event.
    fn states(events: &[MarketEvent]) -> Vec<Derivatives> {
        let mut engine = MarketStateEngine::new();
        events
            .iter()
            .map(|event| {
                engine.apply(event).unwrap();
                engine.state().derivatives
            })
            .collect()
    }

    fn sample(derivatives: &Derivatives) -> OiSample {
        *derivatives.oi.ready().unwrap()
    }

    fn step(derivatives: &Derivatives) -> Option<OiStep> {
        sample(derivatives).step
    }

    fn grid(derivatives: &Derivatives) -> OiGridPoint {
        *derivatives.oi_5m.ready().unwrap()
    }

    #[test]
    fn constants_match_the_catalog() {
        use crate::feature::{FeatureDefinition, ParamValue, WarmUp};
        let int = |definition: &FeatureDefinition, name: &str| {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        assert_eq!(
            int(&catalog::DERIVATIVES_OI_SAMPLE_V1, "step_tolerance_ms"),
            Some(ParamValue::Int(OI_STEP_TOLERANCE_MS))
        );
        assert_eq!(
            int(&catalog::DERIVATIVES_OI_5M_V1, "grid_ms"),
            Some(ParamValue::Int(OI_GRID.millis()))
        );
        assert_eq!(
            int(&catalog::DERIVATIVES_OI_5M_V1, "max_age_ms"),
            Some(ParamValue::Int(OI_MAX_AGE_MS))
        );
        for (timeframe, definition) in catalog::LIQ_WINDOWS {
            assert_eq!(
                int(definition, "window_ms"),
                Some(ParamValue::Int(timeframe.millis()))
            );
            assert_eq!(
                definition.warm_up,
                WarmUp::Samples(u32::try_from(window_minutes(timeframe)).unwrap())
            );
        }
        // The ring outlives the longest window by one minute.
        assert_eq!(
            RING as u64,
            catalog::LIQ_WINDOWS
                .map(|(timeframe, _)| window_minutes(timeframe))
                .into_iter()
                .max()
                .unwrap()
                + 1
        );
    }

    #[test]
    fn native_step_is_exact_on_a_live_chain() {
        let states = states(&[
            oi(10_000, "80000.5", LIVE),
            oi(20_000, "80001.75", LIVE),
            oi(31_500, "80000.00000001", LIVE),
        ]);
        assert_eq!(step(&states[0]), None);
        assert_eq!(
            step(&states[1]),
            Some(OiStep {
                previous_time: t(10_000),
                delta: qty("1.25"),
                elapsed_ms: 10_000,
            })
        );
        assert_eq!(
            step(&states[2]),
            Some(OiStep {
                previous_time: t(20_000),
                delta: qty("-1.74999999"),
                elapsed_ms: 11_500,
            })
        );
        let value = sample(&states[2]);
        assert_eq!(value.feature, catalog::DERIVATIVES_OI_SAMPLE_V1.key);
        assert_eq!(
            (value.time, value.open_interest, value.resolution_ms),
            (t(31_500), qty("80000.00000001"), LIVE)
        );
        assert_eq!(
            value.to_string(),
            "time=31500ms oi=80000.00000001 res=10000ms prev=20000ms elapsed=11500ms \
             delta=-1.74999999 vel_per_min=-9.130434730434782 derivatives.oi.sample@1"
        );
        assert_eq!(
            sample(&states[0]).to_string(),
            "time=10000ms oi=80000.50000000 res=10000ms prev=- elapsed=- delta=- \
             vel_per_min=- derivatives.oi.sample@1"
        );
    }

    #[test]
    fn velocity_follows_its_documented_expression() {
        let states = states(&[oi(0, "100", LIVE), oi(7_000, "103.12345678", LIVE)]);
        let value = sample(&states[1]);
        let expected = (312_345_678_f64 / 100_000_000_f64) * 60_000.0 / 7_000.0;
        assert_eq!(
            value.velocity_per_minute().map(f64::to_bits),
            Some(expected.to_bits())
        );
        assert_eq!(sample(&states[0]).velocity_per_minute(), None);
    }

    #[test]
    fn a_resolution_change_restarts_the_chain() {
        let states = states(&[
            oi(10_000, "100", LIVE),
            oi(20_000, "101", LIVE),
            // Archive spacing: the first sample at the new resolution has
            // no step, the next one does.
            oi(300_000, "102", ARCHIVE),
            oi(600_000, "104", ARCHIVE),
            // And back.
            oi(610_000, "105", LIVE),
            oi(620_000, "106", LIVE),
        ]);
        let steps: Vec<Option<i64>> = states
            .iter()
            .map(|state| step(state).map(|step| step.delta.units()))
            .collect();
        assert_eq!(
            steps,
            [
                None,
                Some(100_000_000),
                None,
                Some(200_000_000),
                None,
                Some(100_000_000)
            ]
        );
        assert_eq!(step(&states[3]).unwrap().elapsed_ms, 300_000);
    }

    #[test]
    fn an_oi_gap_breaks_the_chain_exactly_once() {
        let states = states(&[
            oi(10_000, "100", LIVE),
            gap(
                Stream::OpenInterest,
                12_000,
                15_000,
                GapReason::Disconnected,
            ),
            oi(20_000, "101", LIVE),
            oi(30_000, "102", LIVE),
            // Gaps on other streams do not break it.
            gap(Stream::Trades, 31_000, 32_000, GapReason::Disconnected),
            gap(
                Stream::Liquidations,
                32_000,
                33_000,
                GapReason::Disconnected,
            ),
            gap(Stream::MarkPrice, 33_000, 34_000, GapReason::Disconnected),
            oi(40_000, "103", LIVE),
        ]);
        // The level stays ready through the gap.
        assert_eq!(sample(&states[1]).time, t(10_000));
        assert_eq!(step(&states[2]), None);
        assert!(step(&states[3]).is_some());
        assert_eq!(
            step(&states[7]),
            Some(OiStep {
                previous_time: t(30_000),
                delta: qty("1"),
                elapsed_ms: 10_000,
            })
        );
    }

    #[test]
    fn the_step_tolerance_is_inclusive() {
        let limit = i64::from(LIVE) + OI_STEP_TOLERANCE_MS;
        let at_limit = states(&[oi(0, "1", LIVE), oi(limit, "2", LIVE)]);
        assert_eq!(step(&at_limit[1]).unwrap().elapsed_ms, 25_000);
        let beyond = states(&[oi(0, "1", LIVE), oi(limit + 1, "2", LIVE)]);
        assert_eq!(step(&beyond[1]), None);
        // One missing archive row (600 s) breaks the archive chain.
        let missing = states(&[oi(0, "1", ARCHIVE), oi(600_000, "2", ARCHIVE)]);
        assert_eq!(step(&missing[1]), None);
    }

    #[test]
    fn an_equal_time_sample_has_no_step() {
        let states = states(&[oi(10_000, "100", LIVE), oi(10_000, "101", LIVE)]);
        assert_eq!(sample(&states[1]).open_interest, qty("101"));
        assert_eq!(step(&states[1]), None);
    }

    #[test]
    fn a_zero_resolution_never_steps() {
        let states = states(&[oi(0, "1", 0), oi(1, "2", 0)]);
        assert_eq!(step(&states[1]), None);
    }

    /// Live samples every 10 s with up to 8 s of re-time jitter, from
    /// `from` for `count` samples.
    fn live_tape(lcg: &mut Lcg, from: i64, count: i64) -> Vec<MarketEvent> {
        let mut level = 80_000 * SCALE;
        (0..count)
            .map(|i| {
                level += lcg.below(2 * SCALE as u64 + 1) - SCALE;
                MarketEvent::OpenInterest(OpenInterest {
                    time: t(from + i * 10_000 + lcg.below(8_000)),
                    open_interest: Qty::from_units(level),
                    resolution_ms: LIVE,
                })
            })
            .collect()
    }

    /// The latest sample at or before `boundary`, from samples in order.
    fn latest_at(samples: &[OpenInterest], boundary: EventTime) -> Option<OpenInterest> {
        samples
            .iter()
            .take_while(|sample| sample.time <= boundary)
            .last()
            .copied()
    }

    #[test]
    fn grid_takes_the_latest_sample_at_or_before_each_boundary() {
        let mut lcg = Lcg(0x6d69_6500_0000_0019);
        let tape = live_tape(&mut lcg, DAY - GRID - 25_000, 100);
        let mut engine = MarketStateEngine::new();
        let mut seen: Vec<OpenInterest> = Vec::new();
        let (mut boundaries, mut deltas) = (Vec::new(), 0);
        for event in &tape {
            let MarketEvent::OpenInterest(sample) = event else {
                unreachable!("an open-interest tape")
            };
            engine.apply(event).unwrap();
            seen.push(*sample);
            let state = engine.state();
            // Point in time: the native value is the event just consumed.
            assert_eq!(Some(sample.time), state.as_of);
            assert_eq!(self::sample(&state.derivatives).time, sample.time);
            let boundary = OI_GRID.open_of(sample.time).unwrap();
            let Some(expected) = latest_at(&seen, boundary) else {
                assert_eq!(state.derivatives.oi_5m, warming(0, 1));
                continue;
            };
            let point = grid(&state.derivatives);
            assert_eq!(point.boundary, boundary);
            assert!(point.sample_time <= point.boundary);
            assert_eq!(
                (point.sample_time, point.open_interest, point.resolution_ms),
                (expected.time, expected.open_interest, LIVE)
            );
            let before = latest_at(&seen, t(boundary.as_millis() - GRID));
            let delta = before.map(|before| {
                Qty::from_units(expected.open_interest.units() - before.open_interest.units())
            });
            assert_eq!(point.delta, delta, "{point}");
            if boundaries.last() != Some(&boundary) {
                boundaries.push(boundary);
                deltas += usize::from(point.delta.is_some());
            }
        }
        // 23:55, 00:00, 00:05 and 00:10 closed; all but the first have a
        // delta.
        assert_eq!(boundaries.len(), 4);
        assert_eq!(deltas, 3);
    }

    #[test]
    fn grid_takes_archive_samples_at_their_boundary() {
        let states = states(&[
            oi(DAY, "100", ARCHIVE),
            oi(DAY + GRID, "101.5", ARCHIVE),
            oi(DAY + 2 * GRID, "101", ARCHIVE),
        ]);
        let points: Vec<String> = states.iter().map(|state| grid(state).to_string()).collect();
        assert_eq!(
            points,
            [
                "boundary=86400000ms oi=100.00000000 sample=86400000ms res=300000ms delta=- \
                 vel_per_min=- derivatives.oi.5m@1",
                "boundary=86700000ms oi=101.50000000 sample=86700000ms res=300000ms \
                 delta=1.50000000 vel_per_min=0.3 derivatives.oi.5m@1",
                "boundary=87000000ms oi=101.00000000 sample=87000000ms res=300000ms \
                 delta=-0.50000000 vel_per_min=-0.1 derivatives.oi.5m@1",
            ]
        );
        let velocity = grid(&states[1]).velocity_per_minute().unwrap();
        assert_eq!(
            velocity.to_bits(),
            ((150_000_000_f64 / 100_000_000_f64) * 60_000.0 / 300_000.0).to_bits()
        );
    }

    #[test]
    fn samples_around_a_boundary_go_to_the_right_one() {
        let b = DAY;
        // B − 1: belongs to the interval before, and is B's level.
        let states = states(&[
            oi(b - GRID - 20_000, "1", LIVE),
            oi(b - 1, "2", LIVE),
            oi(b + 9_000, "3", LIVE),
        ]);
        let point = grid(&states[1]);
        assert_eq!(
            (point.boundary, point.sample_time),
            (t(b - GRID), t(b - GRID - 20_000))
        );
        let point = grid(&states[2]);
        assert_eq!(
            (point.boundary, point.sample_time, point.open_interest),
            (t(b), t(b - 1), qty("2"))
        );
        // B: the sample closes B itself.
        let states = self::states(&[oi(b - 10_000, "1", LIVE), oi(b, "2", LIVE)]);
        let point = grid(&states[1]);
        assert_eq!(
            (point.boundary, point.sample_time, point.open_interest),
            (t(b), t(b), qty("2"))
        );
        // B + 1: B takes the sample before.
        let states = self::states(&[oi(b - 10_000, "1", LIVE), oi(b + 1, "2", LIVE)]);
        let point = grid(&states[1]);
        assert_eq!(
            (point.boundary, point.sample_time, point.open_interest),
            (t(b), t(b - 10_000), qty("1"))
        );
        // No sample at or before a boundary: nothing closes.
        let states = self::states(&[oi(b + 1, "1", LIVE), oi(b + 10_000, "2", LIVE)]);
        assert_eq!(states[1].oi_5m, warming(0, 1));
    }

    #[test]
    fn a_resolution_change_has_no_grid_delta() {
        let states = states(&[
            oi(DAY - 5_000, "100", LIVE),
            oi(DAY + 5_000, "101", LIVE),
            oi(DAY + GRID, "102", ARCHIVE),
            oi(DAY + 2 * GRID, "104", ARCHIVE),
        ]);
        assert_eq!(grid(&states[1]).boundary, t(DAY));
        assert_eq!(grid(&states[2]).delta, None);
        assert_eq!(grid(&states[2]).resolution_ms, ARCHIVE);
        assert_eq!(grid(&states[3]).delta, Some(qty("2")));
    }

    #[test]
    fn a_feed_hole_makes_the_stale_boundary_unavailable() {
        let hole_end = DAY + 3_600_000;
        let mut events = vec![
            oi(DAY - 5_000, "100", LIVE),
            oi(DAY + 5_000, "101", LIVE),
            oi(DAY + 290_000, "102", LIVE),
            gap(
                Stream::OpenInterest,
                DAY + 295_000,
                hole_end,
                GapReason::Disconnected,
            ),
        ];
        events.extend((0..61).map(|i| oi(hole_end + 5_000 + i * 10_000, "103", LIVE)));
        let states = states(&events);
        // The first sample after the hole closes 01:00 with the sample of
        // 00:04:50: an hour old.
        assert_eq!(
            states[4].oi_5m,
            FeatureValue::Unavailable {
                reason: Unavailability::InputInvalid
            }
        );
        // 01:05 is ready without a delta, 01:10 has one.
        let first = states
            .iter()
            .map(|state| state.oi_5m)
            .position(|value| {
                value
                    .ready()
                    .is_some_and(|point| point.boundary == t(hole_end + GRID))
            })
            .unwrap();
        assert_eq!(grid(&states[first]).delta, None);
        let second = states
            .iter()
            .position(|state| {
                state
                    .oi_5m
                    .ready()
                    .is_some_and(|point| point.boundary == t(hole_end + 2 * GRID))
            })
            .unwrap();
        assert_eq!(grid(&states[second]).delta, Some(qty("0")));
        // The native chain broke once at the gap.
        assert_eq!(step(&states[4]), None);
        assert!(step(&states[5]).is_some());
    }

    #[test]
    fn a_sample_a_year_later_closes_in_constant_time() {
        let year = 365 * DAY;
        let states = states(&[
            oi(DAY, "100", ARCHIVE),
            oi(DAY + year, "101", ARCHIVE),
            oi(DAY + year + GRID, "102", ARCHIVE),
        ]);
        // The sample lands on its own boundary: ready, with no delta, since
        // the previous boundary closed with a year-old sample.
        let point = grid(&states[1]);
        assert_eq!((point.boundary, point.delta), (t(DAY + year), None));
        assert_eq!(grid(&states[2]).delta, Some(qty("1")));
        // Off the boundary, the year-old sample is stale.
        let states = self::states(&[oi(DAY, "100", ARCHIVE), oi(DAY + year + 1, "101", LIVE)]);
        assert_eq!(
            states[1].oi_5m,
            FeatureValue::Unavailable {
                reason: Unavailability::InputInvalid
            }
        );
    }

    #[test]
    fn freshness_is_inclusive() {
        let fresh = states(&[oi(DAY - OI_MAX_AGE_MS, "1", LIVE), oi(DAY + 1, "2", LIVE)]);
        assert_eq!(grid(&fresh[1]).sample_time, t(DAY - OI_MAX_AGE_MS));
        let stale = states(&[
            oi(DAY - OI_MAX_AGE_MS - 1, "1", LIVE),
            oi(DAY + 1, "2", LIVE),
        ]);
        assert!(!stale[1].oi_5m.is_ready());
    }

    #[test]
    fn no_value_exists_before_the_event_that_carries_it() {
        let states = states(&[
            trade(1_000, 1),
            settlement(2_000, "0.0001"),
            mark(3_000, "63500", "63490.5", "0.00012", 28_800_000),
            oi(4_000, "100", LIVE),
        ]);
        assert_eq!(states[0], Derivatives::new());
        assert_eq!(states[1].mark, warming(0, 1));
        assert!(states[1].funding_settled.is_ready());
        assert_eq!(states[2].oi, warming(0, 1));
        assert!(states[2].mark.is_ready());
        assert!(states[3].oi.is_ready());
        assert_eq!(states[3].oi_5m, warming(0, 1));
    }

    #[test]
    fn mark_derives_basis_and_time_to_funding_exactly() {
        let states = states(&[
            mark(1_000, "63500.1", "63490.25", "0.00012", 28_800_000),
            settlement(28_800_003, "0.0001"),
            settlement(57_600_001, "-0.00002"),
        ]);
        let value = *states[0].mark.ready().unwrap();
        assert_eq!(value.feature, catalog::DERIVATIVES_MARK_V1.key);
        assert_eq!(value.basis(), Some("9.85".parse().unwrap()));
        assert_eq!(value.time_to_next_funding_ms(t(1_000)), Some(28_799_000));
        assert_eq!(value.time_to_next_funding_ms(t(28_800_005)), Some(-5));
        assert_eq!(
            value.to_string(),
            "time=1000ms mark=63500.10000000 index=63490.25000000 basis=9.85000000 \
             funding=0.00012000 next_funding=28800000ms to_funding=28799000ms \
             derivatives.mark@1"
        );
        let extreme = MarkState {
            mark_price: Price::from_units(i64::MIN),
            index_price: Price::from_units(1),
            next_funding_time: t(i64::MAX),
            ..value
        };
        assert_eq!(extreme.basis(), None);
        assert_eq!(extreme.time_to_next_funding_ms(t(-1)), None);
        assert!(extreme.to_string().contains("basis=- "));
        // Settled funding follows the last settlement.
        let settled = *states[2].funding_settled.ready().unwrap();
        assert_eq!(
            settled.to_string(),
            "time=57600001ms rate=-0.00002000 derivatives.funding.settled@1"
        );
        assert_eq!(
            states[1].funding_settled.ready().unwrap().rate,
            "0.0001".parse().unwrap()
        );
        // A settlement leaves the mark value alone.
        assert_eq!(states[2].mark, states[0].mark);
    }

    fn window(derivatives: &Derivatives, timeframe: Timeframe) -> FeatureValue<LiquidationWindow> {
        *derivatives.liquidations.get(timeframe).unwrap()
    }

    fn ready_5m(derivatives: &Derivatives) -> LiquidationWindow {
        *window(derivatives, Timeframe::M5).ready().unwrap()
    }

    /// The (long count, long qty, short count, short qty, partial_start,
    /// feed_gap) of a window.
    fn sums(window: &LiquidationWindow) -> (u64, Qty, u64, Qty, bool, bool) {
        (
            window.long_count,
            window.long_qty,
            window.short_count,
            window.short_qty,
            window.partial_start,
            window.feed_gap,
        )
    }

    /// A trade at the start of every minute from `from` to `to` minutes, so
    /// every minute closes.
    fn minute_trades(from: i64, to: i64) -> Vec<MarketEvent> {
        (from..to)
            .map(|minute| trade(minute * MINUTE + 100, minute.unsigned_abs() + 1))
            .collect()
    }

    fn sorted(mut events: Vec<MarketEvent>) -> Vec<MarketEvent> {
        events.sort();
        events
    }

    #[test]
    fn liquidations_count_by_side_over_closed_minutes() {
        let mut events = minute_trades(0, 9);
        events.extend([
            // Minute 1: a long and a short closed.
            liquidation(MINUTE + 5_000, Aggressor::Sell, "0.5"),
            liquidation(MINUTE + 6_000, Aggressor::Buy, "0.25"),
            // Minute 2 is empty; minute 3 has two longs.
            liquidation(3 * MINUTE + 1_000, Aggressor::Sell, "1"),
            liquidation(3 * MINUTE + 59_999, Aggressor::Sell, "0.125"),
            // Minute 6: a short.
            liquidation(6 * MINUTE, Aggressor::Buy, "2"),
        ]);
        let events = sorted(events);
        let states = states(&events);
        let at = |millis: i64| {
            let index = events
                .iter()
                .rposition(|event| event.time() <= t(millis))
                .unwrap();
            states[index]
        };
        // The stream starts in minute 1, which closes at the trade of
        // minute 2.
        assert_eq!(window(&at(MINUTE + 5_000), Timeframe::M5), warming(0, 5));
        assert_eq!(window(&at(2 * MINUTE + 100), Timeframe::M5), warming(1, 5));
        assert_eq!(window(&at(5 * MINUTE + 100), Timeframe::M5), warming(4, 5));
        // Minutes 1–5.
        let first = ready_5m(&at(6 * MINUTE + 100));
        assert_eq!(sums(&first), (3, qty("1.625"), 1, qty("0.25"), true, false));
        assert_eq!(first.end, t(6 * MINUTE));
        assert_eq!(first.feature, catalog::DERIVATIVES_LIQ_WINDOW_5M_V1.key);
        assert_eq!(
            first.to_string(),
            "5m end=360000ms long=3/1.62500000 short=1/0.25000000 partial_start \
             derivatives.liq.window.5m@1"
        );
        // Minutes 2–6: minute 1 left, the developing minute 6 is not in
        // until it closes.
        let second = ready_5m(&at(7 * MINUTE + 100));
        assert_eq!(sums(&second), (2, qty("1.125"), 1, qty("2"), false, false));
        // Minutes 3–7.
        let third = ready_5m(&at(8 * MINUTE + 100));
        assert_eq!(sums(&third), (2, qty("1.125"), 1, qty("2"), false, false));
        assert_eq!(
            window(&at(8 * MINUTE + 100), Timeframe::M15),
            warming(7, 15)
        );
        // A liquidation never touches the windows of closed minutes.
        assert_eq!(
            at(6 * MINUTE).liquidations,
            at(5 * MINUTE + 100).liquidations
        );
    }

    #[test]
    fn windows_wait_for_the_first_liquidations_stream_event() {
        // Archive-like: trades for 20 minutes, no liquidations stream.
        let quiet = states(&minute_trades(0, 20));
        assert!(
            quiet
                .iter()
                .all(|state| state.liquidations == LiquidationWindows::new())
        );
        // A gap alone starts the stream: its end's minute is the first.
        let mut events = minute_trades(0, 20);
        events.push(gap(
            Stream::Liquidations,
            2 * MINUTE + 10_000,
            3 * MINUTE + 10_000,
            GapReason::Disconnected,
        ));
        let events = sorted(events);
        let states = states(&events);
        let last = states.last().unwrap();
        // Minutes 3 to 18 closed: 16.
        assert_eq!(window(last, Timeframe::H1), warming(16, 60));
        let fifteen = *window(last, Timeframe::M15).ready().unwrap();
        // Minutes 4–18: after the gap, complete and empty.
        assert_eq!(sums(&fifteen), (0, qty("0"), 0, qty("0"), false, false));
        let index = events
            .iter()
            .position(|event| event.time() == t(8 * MINUTE + 100))
            .unwrap();
        // Minutes 3–7: the first minute is partial and inside the gap.
        assert_eq!(
            sums(&ready_5m(&states[index])),
            (0, qty("0"), 0, qty("0"), true, true)
        );
    }

    #[test]
    fn a_gap_flags_minutes_that_already_closed() {
        let mut events = minute_trades(0, 12);
        events.push(liquidation(30_000, Aggressor::Sell, "1"));
        // Known at 09:00.5, reaching back into minute 4.
        events.push(gap(
            Stream::Liquidations,
            4 * MINUTE + 30_000,
            9 * MINUTE + 500,
            GapReason::Disconnected,
        ));
        let events = sorted(events);
        let states = states(&events);
        let gap_index = events
            .iter()
            .position(|event| matches!(event, MarketEvent::FeedGap(_)))
            .unwrap();
        // Before the gap: minutes 4–8 complete.
        let before = ready_5m(&states[gap_index - 1]);
        assert_eq!(before.end, t(9 * MINUTE));
        assert!(!before.feed_gap);
        // The gap event recomputes the current windows.
        let after = ready_5m(&states[gap_index]);
        assert_eq!(
            after,
            LiquidationWindow {
                feed_gap: true,
                ..before
            }
        );
        assert_eq!(window(&states[gap_index], Timeframe::M15), warming(9, 15));
        // Later windows carry the flag until minute 9 leaves.
        let flagged: Vec<(i64, bool)> = states[gap_index + 1..]
            .iter()
            .map(|state| {
                let window = ready_5m(state);
                (window.end.as_millis() / MINUTE, window.feed_gap)
            })
            .collect();
        assert_eq!(flagged, [(10, true), (11, true)]);
        // The first minute's long is gone by then.
        assert_eq!(ready_5m(states.last().unwrap()).long_count, 0);
    }

    #[test]
    fn a_gap_flags_the_minutes_windows_reach_later() {
        let mut events = minute_trades(0, 3);
        events.push(liquidation(30_000, Aggressor::Buy, "1"));
        events.push(gap(
            Stream::Liquidations,
            MINUTE + 1_000,
            MINUTE + 2_000,
            GapReason::Disconnected,
        ));
        // A trades outage from minute 3 to 10; liquidations go on.
        events.push(liquidation(5 * MINUTE + 1_000, Aggressor::Sell, "0.5"));
        events.push(liquidation(7 * MINUTE + 1_000, Aggressor::Sell, "0.25"));
        events.push(gap(
            Stream::Liquidations,
            6 * MINUTE,
            6 * MINUTE + 1_000,
            GapReason::Disconnected,
        ));
        events.push(trade(10 * MINUTE + 100, 100));
        let events = sorted(events);
        let states = states(&events);
        let last = states.last().unwrap();
        // Minutes 5–9: both liquidations kept their minutes although no
        // trade closed them before minute 10; the gap in minute 6 flags it.
        assert_eq!(
            sums(&ready_5m(last)),
            (2, qty("0.75"), 0, qty("0"), false, true)
        );
        // Minutes 0–9 closed; minute 0 is the first, minute 1 has a gap.
        let fifteen = window(last, Timeframe::M15);
        assert_eq!(fifteen, warming(10, 15));
    }

    #[test]
    fn rejects_a_liquidation_that_overflows_a_minute() {
        let mut tracker = DerivativesTracker::new();
        let step = tracker
            .step(&liquidation(1_000, Aggressor::Sell, "1"), &[])
            .unwrap();
        tracker.commit(step);
        let huge = MarketEvent::Liquidation(Liquidation {
            time: t(2_000),
            aggressor: Aggressor::Sell,
            price: Price::from_units(1),
            avg_price: Price::from_units(1),
            filled_qty: Qty::from_units(i64::MAX),
        });
        assert!(matches!(
            tracker.step(&huge, &[]),
            Err(DerivativesError::Overflow)
        ));
        // The other side still fits.
        let short = MarketEvent::Liquidation(Liquidation {
            aggressor: Aggressor::Buy,
            ..match huge {
                MarketEvent::Liquidation(liquidation) => liquidation,
                _ => unreachable!("a liquidation"),
            }
        });
        let step = tracker.step(&short, &[]).unwrap();
        tracker.commit(step);
        assert_eq!(
            DerivativesError::Overflow.to_string(),
            "a derivatives value leaves its integer range"
        );
    }

    #[test]
    fn windows_fold_their_minutes_on_random_tapes() {
        for seed in [21, 22, 23] {
            let mut lcg = Lcg(seed ^ 0x6c69_7100);
            let base = random_tape(seed, 3_000);
            let mut events = Vec::with_capacity(base.len() * 2);
            let mut previous = i64::MIN;
            for event in base {
                let time = event.time().as_millis();
                // Inject between distinct milliseconds only, so the order is
                // unambiguous.
                if time > previous + 2 {
                    match lcg.below(12) {
                        0..=4 => {
                            let aggressor = if lcg.below(2) == 0 {
                                Aggressor::Buy
                            } else {
                                Aggressor::Sell
                            };
                            events.push(MarketEvent::Liquidation(Liquidation {
                                time: t(time - 1),
                                aggressor,
                                price: Price::from_units(6_000_000_000_000),
                                avg_price: Price::from_units(6_000_000_000_000),
                                filled_qty: Qty::from_units(1 + lcg.below(500_000_000)),
                            }));
                        }
                        5 => {
                            let end = time - 1;
                            let start = (end - lcg.below(5_400_000)).max(previous + 1);
                            events.push(gap(
                                Stream::Liquidations,
                                start,
                                end,
                                GapReason::Disconnected,
                            ));
                        }
                        _ => {}
                    }
                }
                previous = time;
                events.push(event);
            }

            let mut engine = MarketStateEngine::new();
            let mut liquidations: Vec<Liquidation> = Vec::new();
            let mut gaps: Vec<FeedGap> = Vec::new();
            let mut start: Option<EventTime> = None;
            let mut closed: u64 = 0;
            let mut end: Option<EventTime> = None;
            let (mut checked, mut flagged, mut counted) = (0, 0, 0);
            for event in &events {
                engine.apply(event).unwrap();
                match event {
                    MarketEvent::Liquidation(liquidation) => {
                        start.get_or_insert(Timeframe::M1.open_of(liquidation.time).unwrap());
                        liquidations.push(*liquidation);
                    }
                    MarketEvent::FeedGap(gap) if gap.stream == Stream::Liquidations => {
                        start.get_or_insert(Timeframe::M1.open_of(gap.end).unwrap());
                        gaps.push(*gap);
                    }
                    _ => {}
                }
                if let Some(start) = start {
                    for bar in engine.closed_bars() {
                        if bar.timeframe == Timeframe::M1 && bar.open_time >= start {
                            closed += 1;
                            end = Some(bar.end());
                        }
                    }
                }
                let state = engine.state();
                for (timeframe, feature, value) in state.derivatives.liquidations.iter() {
                    let span = window_minutes(timeframe);
                    let (Some(start), Some(end)) = (start, end) else {
                        assert_eq!(*value, warming(closed, span), "seed {seed}");
                        continue;
                    };
                    if closed < span {
                        assert_eq!(*value, warming(closed, span), "seed {seed}");
                        continue;
                    }
                    let window = value.ready().unwrap();
                    let from = end.as_millis() - timeframe.millis();
                    let mut expected = LiquidationWindow {
                        feature,
                        timeframe,
                        end,
                        long_count: 0,
                        long_qty: Qty::from_units(0),
                        short_count: 0,
                        short_qty: Qty::from_units(0),
                        partial_start: from <= start.as_millis(),
                        feed_gap: gaps
                            .iter()
                            .any(|gap| gap.start < end && gap.end.as_millis() >= from),
                    };
                    for liquidation in liquidations.iter().filter(|liquidation| {
                        liquidation.time.as_millis() >= from && liquidation.time < end
                    }) {
                        let units = liquidation.filled_qty.units();
                        match liquidation.aggressor {
                            Aggressor::Sell => {
                                expected.long_count += 1;
                                expected.long_qty =
                                    Qty::from_units(expected.long_qty.units() + units);
                            }
                            Aggressor::Buy => {
                                expected.short_count += 1;
                                expected.short_qty =
                                    Qty::from_units(expected.short_qty.units() + units);
                            }
                        }
                    }
                    assert_eq!(*window, expected, "seed {seed}");
                    checked += 1;
                    flagged += usize::from(window.feed_gap);
                    counted += usize::from(window.long_count + window.short_count > 0);
                }
            }
            assert!(checked > 1_000, "seed {seed}: {checked}");
            assert!(flagged > 100, "seed {seed}: {flagged}");
            assert!(counted > 100, "seed {seed}: {counted}");
        }
    }

    /// The OI gap of the golden tape: 00:01:01 to 00:01:04 UTC on day 1,
    /// between two live samples.
    const OI_GAP: (i64, i64) = (DAY + 61_000, DAY + 64_000);
    /// The liquidations gap of the golden tape: 00:37:30 to 00:40:10 UTC on
    /// day 1, known after minutes it overlaps closed.
    const LIQ_GAP: (i64, i64) = (DAY + 37 * MINUTE + 30_000, DAY + 40 * MINUTE + 10_000);

    /// The derivatives golden tape: 23:50 UTC on day 0 to 01:06 UTC on day 1.
    ///
    /// - Trades in every minute from an LCG on a random walk, so every 1m bar
    ///   closes.
    /// - Live open interest every 10 s with up to 8 s of re-time jitter from
    ///   23:54:35 to 00:05:35 across the 23:55, 00:00 and 00:05 boundaries,
    ///   with an OI gap; then a hole, one live sample at 00:20:05, and archive
    ///   samples on the boundaries from 00:30 to 01:05.
    /// - Mark prices every five minutes and around the 00:00 funding, and one
    ///   settlement at 00:00:00.007.
    /// - Liquidations on both sides from 23:52, with empty minutes and a
    ///   liquidations gap.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0013);
        let mut events = Vec::new();
        let side = |lcg: &mut Lcg| {
            if lcg.below(2) == 0 {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            }
        };
        let mut walk = 62_500 * SCALE;
        let mut trade_id = 0;
        for open in (DAY - 10 * MINUTE..DAY + 66 * MINUTE).step_by(60_000) {
            for k in 0..1 + lcg.below(2) {
                walk += lcg.below(2 * SCALE as u64 + 1) - SCALE;
                trade_id += 1;
                let aggressor = side(&mut lcg);
                events.push(MarketEvent::Trade(crate::event::Trade {
                    time: t(open + 1_000 + k * 25_000 + lcg.below(20_000)),
                    trade_id,
                    price: Price::from_units(walk),
                    qty: Qty::from_units(1 + lcg.below(50_000_000)),
                    aggressor,
                }));
            }
            // Liquidations from 23:52, none from 00:10 to 00:12.
            let quiet = (DAY + 10 * MINUTE..DAY + 13 * MINUTE).contains(&open);
            if open >= DAY - 8 * MINUTE && !quiet {
                for k in 0..lcg.below(3) {
                    let aggressor = side(&mut lcg);
                    events.push(MarketEvent::Liquidation(Liquidation {
                        time: t(open + 5_000 + k * 25_000 + lcg.below(20_000)),
                        aggressor,
                        price: Price::from_units(walk),
                        avg_price: Price::from_units(walk + 10 * SCALE),
                        filled_qty: Qty::from_units(100_000 + lcg.below(80_000_000)),
                    }));
                }
            }
        }
        // Closes 01:05.
        events.push(trade(DAY + 66 * MINUTE + 500, trade_id + 1));
        events.extend(live_tape(&mut lcg, DAY - GRID - 25_000, 67));
        events.push(gap(
            Stream::OpenInterest,
            OI_GAP.0,
            OI_GAP.1,
            GapReason::Disconnected,
        ));
        // One live sample after the hole: its boundary's sample is stale.
        events.push(oi(DAY + 20 * MINUTE + 5_000, "80050", LIVE));
        let mut level = 80_100 * SCALE;
        for boundary in (DAY + 30 * MINUTE..=DAY + 65 * MINUTE).step_by(300_000) {
            level += lcg.below(40 * SCALE as u64 + 1) - 20 * SCALE;
            events.push(MarketEvent::OpenInterest(OpenInterest {
                time: t(boundary),
                open_interest: Qty::from_units(level),
                resolution_ms: ARCHIVE,
            }));
        }
        let funding = DAY;
        let next = DAY + 8 * 3_600_000;
        for (millis, mark_price, index, rate, next_funding) in [
            (
                DAY - 10 * MINUTE + 2_000,
                "62500.4",
                "62498.1",
                "0.00010000",
                funding,
            ),
            (
                DAY - 5 * MINUTE + 2_000,
                "62501.9",
                "62499.95",
                "0.00010312",
                funding,
            ),
            (DAY - 1_000, "62497.5", "62498.25", "0.00009871", funding),
            (DAY + 1_000, "62496.8", "62497.1", "0.00004210", next),
            (
                DAY + 5 * MINUTE + 2_000,
                "62499.0",
                "62498.0",
                "0.00004300",
                next,
            ),
            (
                DAY + 30 * MINUTE + 2_000,
                "62502.5",
                "62500.0",
                "0.00005000",
                next,
            ),
            (
                DAY + 60 * MINUTE + 2_000,
                "62490.0",
                "62491.5",
                "-0.00001000",
                next,
            ),
        ] {
            events.push(mark(millis, mark_price, index, rate, next_funding));
        }
        events.push(settlement(funding + 7, "0.00009871"));
        events.push(gap(
            Stream::Liquidations,
            LIQ_GAP.0,
            LIQ_GAP.1,
            GapReason::Disconnected,
        ));
        events.sort();
        events
    }

    /// One line per event of the golden tape that changed the value of
    /// `pick`: the event time, then the value. `ready_only` skips values that
    /// are not ready.
    fn golden_lines<T: fmt::Display + PartialEq>(
        pick: impl Fn(&Derivatives) -> FeatureValue<T>,
        ready_only: bool,
    ) -> Vec<String> {
        let mut engine = MarketStateEngine::new();
        let mut last = pick(&engine.state().derivatives);
        let mut lines = Vec::new();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            let value = pick(&engine.state().derivatives);
            if value != last && (!ready_only || value.is_ready()) {
                lines.push(format!("{} {}", event.time(), show(&value)));
            }
            last = value;
        }
        lines
    }

    #[test]
    fn golden_derivatives_oi_sample_v1() {
        assert_eq!(
            golden_lines(|derivatives| derivatives.oi, false),
            GOLDEN_OI_SAMPLE
        );
    }

    #[test]
    fn golden_derivatives_oi_5m_v1() {
        assert_eq!(
            golden_lines(|derivatives| derivatives.oi_5m, false),
            GOLDEN_OI_5M
        );
    }

    #[test]
    fn golden_derivatives_mark_v1() {
        assert_eq!(
            golden_lines(|derivatives| derivatives.mark, false),
            GOLDEN_MARK
        );
    }

    #[test]
    fn golden_derivatives_funding_settled_v1() {
        assert_eq!(
            golden_lines(|derivatives| derivatives.funding_settled, false),
            GOLDEN_FUNDING_SETTLED
        );
    }

    /// The golden lines of the liquidation window of length `timeframe`,
    /// once ready.
    fn golden_window_lines(timeframe: Timeframe) -> Vec<String> {
        golden_lines(
            |derivatives| *derivatives.liquidations.get(timeframe).unwrap(),
            true,
        )
    }

    #[test]
    fn golden_derivatives_liq_window_5m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M5), GOLDEN_LIQ_WINDOW_5M);
    }

    #[test]
    fn golden_derivatives_liq_window_15m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M15), GOLDEN_LIQ_WINDOW_15M);
    }

    #[test]
    fn golden_derivatives_liq_window_1h_v1() {
        assert_eq!(golden_window_lines(Timeframe::H1), GOLDEN_LIQ_WINDOW_1H);
    }

    const GOLDEN_OI_SAMPLE: [&str; 76] = [
        "86075494ms time=86075494ms oi=79999.00806099 res=10000ms prev=- elapsed=- delta=- vel_per_min=- derivatives.oi.sample@1",
        "86085704ms time=86085704ms oi=79998.11586471 res=10000ms prev=86075494ms elapsed=10210ms delta=-0.89219628 vel_per_min=-5.243073143976494 derivatives.oi.sample@1",
        "86097347ms time=86097347ms oi=79998.49018763 res=10000ms prev=86085704ms elapsed=11643ms delta=0.37432292 vel_per_min=1.929002422056171 derivatives.oi.sample@1",
        "86106851ms time=86106851ms oi=79999.41269957 res=10000ms prev=86097347ms elapsed=9504ms delta=0.92251194 vel_per_min=5.823939015151516 derivatives.oi.sample@1",
        "86119010ms time=86119010ms oi=79998.88850651 res=10000ms prev=86106851ms elapsed=12159ms delta=-0.52419306 vel_per_min=-2.5866916358253143 derivatives.oi.sample@1",
        "86128313ms time=86128313ms oi=79999.83103534 res=10000ms prev=86119010ms elapsed=9303ms delta=0.94252883 vel_per_min=6.078870235407933 derivatives.oi.sample@1",
        "86141656ms time=86141656ms oi=79999.21990160 res=10000ms prev=86128313ms elapsed=13343ms delta=-0.61113374 vel_per_min=-2.74810945064828 derivatives.oi.sample@1",
        "86152308ms time=86152308ms oi=79998.71260229 res=10000ms prev=86141656ms elapsed=10652ms delta=-0.50729931 vel_per_min=-2.857487664288396 derivatives.oi.sample@1",
        "86156673ms time=86156673ms oi=79998.36697282 res=10000ms prev=86152308ms elapsed=4365ms delta=-0.34562947 vel_per_min=-4.750920549828178 derivatives.oi.sample@1",
        "86165570ms time=86165570ms oi=79998.14961955 res=10000ms prev=86156673ms elapsed=8897ms delta=-0.21735327 vel_per_min=-1.4657970327076542 derivatives.oi.sample@1",
        "86182401ms time=86182401ms oi=79998.68647099 res=10000ms prev=86165570ms elapsed=16831ms delta=0.53685144 vel_per_min=1.9137951636860553 derivatives.oi.sample@1",
        "86185694ms time=86185694ms oi=79997.76856373 res=10000ms prev=86182401ms elapsed=3293ms delta=-0.91790726 vel_per_min=-16.724699544488306 derivatives.oi.sample@1",
        "86200903ms time=86200903ms oi=79997.77772605 res=10000ms prev=86185694ms elapsed=15209ms delta=0.00916232 vel_per_min=0.036145650601617466 derivatives.oi.sample@1",
        "86209544ms time=86209544ms oi=79997.11694179 res=10000ms prev=86200903ms elapsed=8641ms delta=-0.66078426 vel_per_min=-4.588248536049068 derivatives.oi.sample@1",
        "86222532ms time=86222532ms oi=79997.74332852 res=10000ms prev=86209544ms elapsed=12988ms delta=0.62638673 vel_per_min=2.8936867724052973 derivatives.oi.sample@1",
        "86229639ms time=86229639ms oi=79998.74294120 res=10000ms prev=86222532ms elapsed=7107ms delta=0.99961268 vel_per_min=8.439110848459267 derivatives.oi.sample@1",
        "86241144ms time=86241144ms oi=79997.92056435 res=10000ms prev=86229639ms elapsed=11505ms delta=-0.82237685 vel_per_min=-4.288797131681878 derivatives.oi.sample@1",
        "86245293ms time=86245293ms oi=79997.59516529 res=10000ms prev=86241144ms elapsed=4149ms delta=-0.32539906 vel_per_min=-4.705698626174983 derivatives.oi.sample@1",
        "86260837ms time=86260837ms oi=79996.67102398 res=10000ms prev=86245293ms elapsed=15544ms delta=-0.92414131 vel_per_min=-3.567194969119918 derivatives.oi.sample@1",
        "86270354ms time=86270354ms oi=79997.50569594 res=10000ms prev=86260837ms elapsed=9517ms delta=0.83467196 vel_per_min=5.262195818009877 derivatives.oi.sample@1",
        "86278040ms time=86278040ms oi=79997.81737616 res=10000ms prev=86270354ms elapsed=7686ms delta=0.31168022 vel_per_min=2.4331008587041376 derivatives.oi.sample@1",
        "86288535ms time=86288535ms oi=79997.63745694 res=10000ms prev=86278040ms elapsed=10495ms delta=-0.17991922 vel_per_min=-1.0285996379228204 derivatives.oi.sample@1",
        "86301286ms time=86301286ms oi=79998.13712440 res=10000ms prev=86288535ms elapsed=12751ms delta=0.49966746 vel_per_min=2.3511918751470473 derivatives.oi.sample@1",
        "86308643ms time=86308643ms oi=79998.19674112 res=10000ms prev=86301286ms elapsed=7357ms delta=0.05961672 vel_per_min=0.48620405056408866 derivatives.oi.sample@1",
        "86318251ms time=86318251ms oi=79998.31781805 res=10000ms prev=86308643ms elapsed=9608ms delta=0.12107693 vel_per_min=0.7561007285595337 derivatives.oi.sample@1",
        "86329449ms time=86329449ms oi=79997.36545652 res=10000ms prev=86318251ms elapsed=11198ms delta=-0.95236153 vel_per_min=-5.102847990712627 derivatives.oi.sample@1",
        "86341964ms time=86341964ms oi=79996.70499754 res=10000ms prev=86329449ms elapsed=12515ms delta=-0.66045898 vel_per_min=-3.166403419896125 derivatives.oi.sample@1",
        "86352671ms time=86352671ms oi=79995.80602690 res=10000ms prev=86341964ms elapsed=10707ms delta=-0.89897064 vel_per_min=-5.037661193611656 derivatives.oi.sample@1",
        "86360758ms time=86360758ms oi=79996.55761341 res=10000ms prev=86352671ms elapsed=8087ms delta=0.75158651 vel_per_min=5.5762570298009155 derivatives.oi.sample@1",
        "86365254ms time=86365254ms oi=79996.99732048 res=10000ms prev=86360758ms elapsed=4496ms delta=0.43970707 vel_per_min=5.867976912811387 derivatives.oi.sample@1",
        "86380700ms time=86380700ms oi=79997.49129208 res=10000ms prev=86365254ms elapsed=15446ms delta=0.49397160 vel_per_min=1.9188330959471709 derivatives.oi.sample@1",
        "86389608ms time=86389608ms oi=79997.17774589 res=10000ms prev=86380700ms elapsed=8908ms delta=-0.31354619 vel_per_min=-2.111896205657836 derivatives.oi.sample@1",
        "86396987ms time=86396987ms oi=79996.55817752 res=10000ms prev=86389608ms elapsed=7379ms delta=-0.61956837 vel_per_min=-5.037823851470389 derivatives.oi.sample@1",
        "86410004ms time=86410004ms oi=79997.45997671 res=10000ms prev=86396987ms elapsed=13017ms delta=0.90179919 vel_per_min=4.156714404240608 derivatives.oi.sample@1",
        "86421912ms time=86421912ms oi=79997.31224991 res=10000ms prev=86410004ms elapsed=11908ms delta=-0.14772680 vel_per_min=-0.7443406113537118 derivatives.oi.sample@1",
        "86425252ms time=86425252ms oi=79996.37109509 res=10000ms prev=86421912ms elapsed=3340ms delta=-0.94115482 vel_per_min=-16.906972814371258 derivatives.oi.sample@1",
        "86440576ms time=86440576ms oi=79995.44232005 res=10000ms prev=86425252ms elapsed=15324ms delta=-0.92877504 vel_per_min=-3.636550665622553 derivatives.oi.sample@1",
        "86447094ms time=86447094ms oi=79994.77496856 res=10000ms prev=86440576ms elapsed=6518ms delta=-0.66735149 vel_per_min=-6.1431557839828175 derivatives.oi.sample@1",
        "86455942ms time=86455942ms oi=79994.05533415 res=10000ms prev=86447094ms elapsed=8848ms delta=-0.71963441 vel_per_min=-4.87998017631103 derivatives.oi.sample@1",
        "86465461ms time=86465461ms oi=79994.00789596 res=10000ms prev=- elapsed=- delta=- vel_per_min=- derivatives.oi.sample@1",
        "86480648ms time=86480648ms oi=79993.73480346 res=10000ms prev=86465461ms elapsed=15187ms delta=-0.27309250 vel_per_min=-1.078919470599855 derivatives.oi.sample@1",
        "86492945ms time=86492945ms oi=79993.89028763 res=10000ms prev=86480648ms elapsed=12297ms delta=0.15548417 vel_per_min=0.7586444010734326 derivatives.oi.sample@1",
        "86496233ms time=86496233ms oi=79994.83667783 res=10000ms prev=86492945ms elapsed=3288ms delta=0.94639020 vel_per_min=17.26989416058394 derivatives.oi.sample@1",
        "86509185ms time=86509185ms oi=79995.05696790 res=10000ms prev=86496233ms elapsed=12952ms delta=0.22029007 vel_per_min=1.0204913681284744 derivatives.oi.sample@1",
        "86518871ms time=86518871ms oi=79994.43830807 res=10000ms prev=86509185ms elapsed=9686ms delta=-0.61865983 vel_per_min=-3.832292979558125 derivatives.oi.sample@1",
        "86530183ms time=86530183ms oi=79995.11755743 res=10000ms prev=86518871ms elapsed=11312ms delta=0.67924936 vel_per_min=3.6028077793493636 derivatives.oi.sample@1",
        "86537029ms time=86537029ms oi=79994.93315842 res=10000ms prev=86530183ms elapsed=6846ms delta=-0.18439901 vel_per_min=-1.616117528483786 derivatives.oi.sample@1",
        "86548587ms time=86548587ms oi=79995.06702947 res=10000ms prev=86537029ms elapsed=11558ms delta=0.13387105 vel_per_min=0.6949526734729192 derivatives.oi.sample@1",
        "86560753ms time=86560753ms oi=79994.10591789 res=10000ms prev=86548587ms elapsed=12166ms delta=-0.96111158 vel_per_min=-4.739988065099458 derivatives.oi.sample@1",
        "86566142ms time=86566142ms oi=79994.87244596 res=10000ms prev=86560753ms elapsed=5389ms delta=0.76652807 vel_per_min=8.534363369827426 derivatives.oi.sample@1",
        "86578654ms time=86578654ms oi=79995.36838876 res=10000ms prev=86566142ms elapsed=12512ms delta=0.49594280 vel_per_min=2.378242327365729 derivatives.oi.sample@1",
        "86592727ms time=86592727ms oi=79994.65506623 res=10000ms prev=86578654ms elapsed=14073ms delta=-0.71332253 vel_per_min=-3.041238669793221 derivatives.oi.sample@1",
        "86599002ms time=86599002ms oi=79993.80509482 res=10000ms prev=86592727ms elapsed=6275ms delta=-0.84997141 vel_per_min=-8.127216669322708 derivatives.oi.sample@1",
        "86605592ms time=86605592ms oi=79993.06001192 res=10000ms prev=86599002ms elapsed=6590ms delta=-0.74508290 vel_per_min=-6.7837593323217 derivatives.oi.sample@1",
        "86617464ms time=86617464ms oi=79993.19915287 res=10000ms prev=86605592ms elapsed=11872ms delta=0.13914095 vel_per_min=0.703205609838275 derivatives.oi.sample@1",
        "86625705ms time=86625705ms oi=79993.70578324 res=10000ms prev=86617464ms elapsed=8241ms delta=0.50663037 vel_per_min=3.688608445576993 derivatives.oi.sample@1",
        "86642006ms time=86642006ms oi=79993.28732897 res=10000ms prev=86625705ms elapsed=16301ms delta=-0.41845427 vel_per_min=-1.5402279737439422 derivatives.oi.sample@1",
        "86650171ms time=86650171ms oi=79992.64359109 res=10000ms prev=86642006ms elapsed=8165ms delta=-0.64373788 vel_per_min=-4.730468193508879 derivatives.oi.sample@1",
        "86660888ms time=86660888ms oi=79991.79941684 res=10000ms prev=86650171ms elapsed=10717ms delta=-0.84417425 vel_per_min=-4.7261785014463005 derivatives.oi.sample@1",
        "86672710ms time=86672710ms oi=79991.74091052 res=10000ms prev=86660888ms elapsed=11822ms delta=-0.05850632 vel_per_min=-0.29693615293520553 derivatives.oi.sample@1",
        "86676714ms time=86676714ms oi=79991.34184684 res=10000ms prev=86672710ms elapsed=4004ms delta=-0.39906368 vel_per_min=-5.979975224775224 derivatives.oi.sample@1",
        "86686858ms time=86686858ms oi=79991.68086558 res=10000ms prev=86676714ms elapsed=10144ms delta=0.33901874 vel_per_min=2.00523702681388 derivatives.oi.sample@1",
        "86702400ms time=86702400ms oi=79992.39015504 res=10000ms prev=86686858ms elapsed=15542ms delta=0.70928946 vel_per_min=2.738216934757432 derivatives.oi.sample@1",
        "86712272ms time=86712272ms oi=79993.01969946 res=10000ms prev=86702400ms elapsed=9872ms delta=0.62954442 vel_per_min=3.826242423014587 derivatives.oi.sample@1",
        "86719192ms time=86719192ms oi=79992.48273078 res=10000ms prev=86712272ms elapsed=6920ms delta=-0.53696868 vel_per_min=-4.655797803468207 derivatives.oi.sample@1",
        "86729769ms time=86729769ms oi=79993.22442119 res=10000ms prev=86719192ms elapsed=10577ms delta=0.74169041 vel_per_min=4.207376817623144 derivatives.oi.sample@1",
        "86737270ms time=86737270ms oi=79993.97968884 res=10000ms prev=86729769ms elapsed=7501ms delta=0.75526765 vel_per_min=6.041335688574856 derivatives.oi.sample@1",
        "87605000ms time=87605000ms oi=80050.00000000 res=10000ms prev=- elapsed=- delta=- vel_per_min=- derivatives.oi.sample@1",
        "88200000ms time=88200000ms oi=80087.21513612 res=300000ms prev=- elapsed=- delta=- vel_per_min=- derivatives.oi.sample@1",
        "88500000ms time=88500000ms oi=80068.33629372 res=300000ms prev=88200000ms elapsed=300000ms delta=-18.87884240 vel_per_min=-3.77576848 derivatives.oi.sample@1",
        "88800000ms time=88800000ms oi=80049.43989299 res=300000ms prev=88500000ms elapsed=300000ms delta=-18.89640073 vel_per_min=-3.7792801460000005 derivatives.oi.sample@1",
        "89100000ms time=89100000ms oi=80049.79024115 res=300000ms prev=88800000ms elapsed=300000ms delta=0.35034816 vel_per_min=0.070069632 derivatives.oi.sample@1",
        "89400000ms time=89400000ms oi=80030.70091682 res=300000ms prev=89100000ms elapsed=300000ms delta=-19.08932433 vel_per_min=-3.8178648660000003 derivatives.oi.sample@1",
        "89700000ms time=89700000ms oi=80022.99858186 res=300000ms prev=89400000ms elapsed=300000ms delta=-7.70233496 vel_per_min=-1.540466992 derivatives.oi.sample@1",
        "90000000ms time=90000000ms oi=80036.74687727 res=300000ms prev=89700000ms elapsed=300000ms delta=13.74829541 vel_per_min=2.7496590820000004 derivatives.oi.sample@1",
        "90300000ms time=90300000ms oi=80055.81897087 res=300000ms prev=90000000ms elapsed=300000ms delta=19.07209360 vel_per_min=3.81441872 derivatives.oi.sample@1",
    ];

    const GOLDEN_OI_5M: [&str; 12] = [
        "86106851ms boundary=86100000ms oi=79998.49018763 sample=86097347ms res=10000ms delta=- vel_per_min=- derivatives.oi.5m@1",
        "86410004ms boundary=86400000ms oi=79996.55817752 sample=86396987ms res=10000ms delta=-1.93201011 vel_per_min=-0.386402022 derivatives.oi.5m@1",
        "86702400ms boundary=86700000ms oi=79991.68086558 sample=86686858ms res=10000ms delta=-4.87731194 vel_per_min=-0.9754623880000001 derivatives.oi.5m@1",
        "87605000ms unavailable InputInvalid",
        "88200000ms boundary=88200000ms oi=80087.21513612 sample=88200000ms res=300000ms delta=- vel_per_min=- derivatives.oi.5m@1",
        "88500000ms boundary=88500000ms oi=80068.33629372 sample=88500000ms res=300000ms delta=-18.87884240 vel_per_min=-3.77576848 derivatives.oi.5m@1",
        "88800000ms boundary=88800000ms oi=80049.43989299 sample=88800000ms res=300000ms delta=-18.89640073 vel_per_min=-3.7792801460000005 derivatives.oi.5m@1",
        "89100000ms boundary=89100000ms oi=80049.79024115 sample=89100000ms res=300000ms delta=0.35034816 vel_per_min=0.070069632 derivatives.oi.5m@1",
        "89400000ms boundary=89400000ms oi=80030.70091682 sample=89400000ms res=300000ms delta=-19.08932433 vel_per_min=-3.8178648660000003 derivatives.oi.5m@1",
        "89700000ms boundary=89700000ms oi=80022.99858186 sample=89700000ms res=300000ms delta=-7.70233496 vel_per_min=-1.540466992 derivatives.oi.5m@1",
        "90000000ms boundary=90000000ms oi=80036.74687727 sample=90000000ms res=300000ms delta=13.74829541 vel_per_min=2.7496590820000004 derivatives.oi.5m@1",
        "90300000ms boundary=90300000ms oi=80055.81897087 sample=90300000ms res=300000ms delta=19.07209360 vel_per_min=3.81441872 derivatives.oi.5m@1",
    ];

    const GOLDEN_MARK: [&str; 7] = [
        "85802000ms time=85802000ms mark=62500.40000000 index=62498.10000000 basis=2.30000000 funding=0.00010000 next_funding=86400000ms to_funding=598000ms derivatives.mark@1",
        "86102000ms time=86102000ms mark=62501.90000000 index=62499.95000000 basis=1.95000000 funding=0.00010312 next_funding=86400000ms to_funding=298000ms derivatives.mark@1",
        "86399000ms time=86399000ms mark=62497.50000000 index=62498.25000000 basis=-0.75000000 funding=0.00009871 next_funding=86400000ms to_funding=1000ms derivatives.mark@1",
        "86401000ms time=86401000ms mark=62496.80000000 index=62497.10000000 basis=-0.30000000 funding=0.00004210 next_funding=115200000ms to_funding=28799000ms derivatives.mark@1",
        "86702000ms time=86702000ms mark=62499.00000000 index=62498.00000000 basis=1.00000000 funding=0.00004300 next_funding=115200000ms to_funding=28498000ms derivatives.mark@1",
        "88202000ms time=88202000ms mark=62502.50000000 index=62500.00000000 basis=2.50000000 funding=0.00005000 next_funding=115200000ms to_funding=26998000ms derivatives.mark@1",
        "90002000ms time=90002000ms mark=62490.00000000 index=62491.50000000 basis=-1.50000000 funding=-0.00001000 next_funding=115200000ms to_funding=25198000ms derivatives.mark@1",
    ];

    const GOLDEN_FUNDING_SETTLED: [&str; 1] =
        ["86400007ms time=86400007ms rate=0.00009871 derivatives.funding.settled@1"];

    const GOLDEN_LIQ_WINDOW_5M: [&str; 71] = [
        "86221450ms 5m end=86220000ms long=3/1.91489221 short=3/0.90503789 partial_start derivatives.liq.window.5m@1",
        "86282519ms 5m end=86280000ms long=3/1.43982238 short=4/1.46845934 complete derivatives.liq.window.5m@1",
        "86351687ms 5m end=86340000ms long=3/1.43982238 short=4/1.46845934 complete derivatives.liq.window.5m@1",
        "86408150ms 5m end=86400000ms long=3/1.43982238 short=3/1.65981989 complete derivatives.liq.window.5m@1",
        "86476239ms 5m end=86460000ms long=2/0.83510616 short=5/2.97653926 complete derivatives.liq.window.5m@1",
        "86530779ms 5m end=86520000ms long=1/0.27376090 short=4/2.46799630 complete derivatives.liq.window.5m@1",
        "86591990ms 5m end=86580000ms long=0/0.00000000 short=3/1.90457485 complete derivatives.liq.window.5m@1",
        "86655737ms 5m end=86640000ms long=0/0.00000000 short=4/2.68004275 complete derivatives.liq.window.5m@1",
        "86711058ms 5m end=86700000ms long=2/1.34047304 short=3/2.09218727 complete derivatives.liq.window.5m@1",
        "86777676ms 5m end=86760000ms long=3/1.72753107 short=2/1.55054134 complete derivatives.liq.window.5m@1",
        "86835384ms 5m end=86820000ms long=4/2.13764621 short=2/1.55054134 complete derivatives.liq.window.5m@1",
        "86891955ms 5m end=86880000ms long=6/2.87921442 short=2/1.55054134 complete derivatives.liq.window.5m@1",
        "86957075ms 5m end=86940000ms long=6/2.87921442 short=1/0.77507344 complete derivatives.liq.window.5m@1",
        "87016691ms 5m end=87000000ms long=5/1.70802972 short=1/0.77507344 complete derivatives.liq.window.5m@1",
        "87073390ms 5m end=87060000ms long=4/1.32097169 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87128345ms 5m end=87120000ms long=3/0.91085655 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87187134ms 5m end=87180000ms long=1/0.16928834 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87247606ms 5m end=87240000ms long=1/0.16928834 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87306224ms 5m end=87300000ms long=1/0.53763462 short=1/0.09606135 complete derivatives.liq.window.5m@1",
        "87366450ms 5m end=87360000ms long=1/0.53763462 short=1/0.09606135 complete derivatives.liq.window.5m@1",
        "87427310ms 5m end=87420000ms long=3/1.67938409 short=1/0.09606135 complete derivatives.liq.window.5m@1",
        "87496465ms 5m end=87480000ms long=4/2.07221010 short=2/0.35632936 complete derivatives.liq.window.5m@1",
        "87547578ms 5m end=87540000ms long=4/2.07221010 short=2/0.35632936 complete derivatives.liq.window.5m@1",
        "87603979ms 5m end=87600000ms long=5/1.95696595 short=1/0.26026801 complete derivatives.liq.window.5m@1",
        "87679176ms 5m end=87660000ms long=6/2.59058122 short=1/0.26026801 complete derivatives.liq.window.5m@1",
        "87727765ms 5m end=87720000ms long=4/1.44883175 short=1/0.26026801 complete derivatives.liq.window.5m@1",
        "87784261ms 5m end=87780000ms long=3/1.05600574 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87849010ms 5m end=87840000ms long=3/1.05600574 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87913023ms 5m end=87900000ms long=2/1.37893678 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "87980359ms 5m end=87960000ms long=2/0.78285775 short=1/0.58893781 complete derivatives.liq.window.5m@1",
        "88033279ms 5m end=88020000ms long=2/0.78285775 short=1/0.58893781 complete derivatives.liq.window.5m@1",
        "88088965ms 5m end=88080000ms long=2/0.78285775 short=1/0.58893781 complete derivatives.liq.window.5m@1",
        "88151877ms 5m end=88140000ms long=2/0.78285775 short=1/0.58893781 complete derivatives.liq.window.5m@1",
        "88205074ms 5m end=88200000ms long=2/0.38719679 short=1/0.58893781 complete derivatives.liq.window.5m@1",
        "88270385ms 5m end=88260000ms long=1/0.34966055 short=2/0.68476538 complete derivatives.liq.window.5m@1",
        "88339326ms 5m end=88320000ms long=1/0.34966055 short=2/0.68476538 complete derivatives.liq.window.5m@1",
        "88383289ms 5m end=88380000ms long=1/0.34966055 short=2/0.68476538 complete derivatives.liq.window.5m@1",
        "88454033ms 5m end=88440000ms long=1/0.34966055 short=3/0.92430760 complete derivatives.liq.window.5m@1",
        "88512885ms 5m end=88500000ms long=0/0.00000000 short=3/0.92430760 complete derivatives.liq.window.5m@1",
        "88580109ms 5m end=88560000ms long=0/0.00000000 short=1/0.23954222 complete derivatives.liq.window.5m@1",
        "88622292ms 5m end=88620000ms long=1/0.32189143 short=1/0.23954222 complete derivatives.liq.window.5m@1",
        "88688131ms 5m end=88680000ms long=1/0.32189143 short=1/0.23954222 complete derivatives.liq.window.5m@1",
        "88741090ms 5m end=88740000ms long=2/0.85230198 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "88810000ms 5m end=88740000ms long=2/0.85230198 short=0/0.00000000 feed_gap derivatives.liq.window.5m@1",
        "88814106ms 5m end=88800000ms long=3/1.16504440 short=1/0.43431009 feed_gap derivatives.liq.window.5m@1",
        "88879811ms 5m end=88860000ms long=3/1.16504440 short=1/0.43431009 feed_gap derivatives.liq.window.5m@1",
        "88936217ms 5m end=88920000ms long=3/1.18236504 short=2/0.89735207 feed_gap derivatives.liq.window.5m@1",
        "88993191ms 5m end=88980000ms long=5/2.20065227 short=2/0.89735207 feed_gap derivatives.liq.window.5m@1",
        "89057383ms 5m end=89040000ms long=4/1.67024172 short=2/0.89735207 feed_gap derivatives.liq.window.5m@1",
        "89112611ms 5m end=89100000ms long=5/1.83141982 short=1/0.46304198 feed_gap derivatives.liq.window.5m@1",
        "89167760ms 5m end=89160000ms long=5/1.83141982 short=2/0.71401436 complete derivatives.liq.window.5m@1",
        "89224688ms 5m end=89220000ms long=4/1.49220775 short=2/0.39147036 complete derivatives.liq.window.5m@1",
        "89292424ms 5m end=89280000ms long=3/0.63279521 short=3/0.80856109 complete derivatives.liq.window.5m@1",
        "89346703ms 5m end=89340000ms long=3/0.63279521 short=3/0.80856109 complete derivatives.liq.window.5m@1",
        "89414613ms 5m end=89400000ms long=2/0.86951785 short=4/1.50061168 complete derivatives.liq.window.5m@1",
        "89470488ms 5m end=89460000ms long=2/0.86951785 short=4/1.60081214 complete derivatives.liq.window.5m@1",
        "89529538ms 5m end=89520000ms long=2/0.86951785 short=3/1.46031416 complete derivatives.liq.window.5m@1",
        "89597688ms 5m end=89580000ms long=2/0.92018890 short=2/1.04322343 complete derivatives.liq.window.5m@1",
        "89658321ms 5m end=89640000ms long=2/0.92018890 short=2/1.04322343 complete derivatives.liq.window.5m@1",
        "89703339ms 5m end=89700000ms long=1/0.20954574 short=1/0.35117284 complete derivatives.liq.window.5m@1",
        "89776890ms 5m end=89760000ms long=1/0.20954574 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "89839098ms 5m end=89820000ms long=1/0.20954574 short=2/0.90577099 complete derivatives.liq.window.5m@1",
        "89890860ms 5m end=89880000ms long=1/0.62703065 short=2/0.90577099 complete derivatives.liq.window.5m@1",
        "89941944ms 5m end=89940000ms long=3/0.98753618 short=2/0.90577099 complete derivatives.liq.window.5m@1",
        "90016389ms 5m end=90000000ms long=3/0.98753618 short=2/0.90577099 complete derivatives.liq.window.5m@1",
        "90066010ms 5m end=90060000ms long=3/0.98753618 short=2/0.90577099 complete derivatives.liq.window.5m@1",
        "90126781ms 5m end=90120000ms long=4/1.22858737 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "90186942ms 5m end=90180000ms long=3/0.60155672 short=0/0.00000000 complete derivatives.liq.window.5m@1",
        "90253574ms 5m end=90240000ms long=1/0.24105119 short=1/0.68899126 complete derivatives.liq.window.5m@1",
        "90316474ms 5m end=90300000ms long=1/0.24105119 short=1/0.68899126 complete derivatives.liq.window.5m@1",
        "90360500ms 5m end=90360000ms long=1/0.24105119 short=1/0.68899126 complete derivatives.liq.window.5m@1",
    ];

    const GOLDEN_LIQ_WINDOW_15M: [&str; 61] = [
        "86835384ms 15m end=86820000ms long=8/4.32629932 short=9/4.92357553 partial_start derivatives.liq.window.15m@1",
        "86891955ms 15m end=86880000ms long=9/4.31903680 short=9/4.92357553 complete derivatives.liq.window.15m@1",
        "86957075ms 15m end=86940000ms long=9/4.31903680 short=9/4.92357553 complete derivatives.liq.window.15m@1",
        "87016691ms 15m end=87000000ms long=10/4.48832514 short=7/4.52708060 complete derivatives.liq.window.15m@1",
        "87073390ms 15m end=87060000ms long=9/3.88360892 short=7/4.52708060 complete derivatives.liq.window.15m@1",
        "87128345ms 15m end=87120000ms long=8/3.32226366 short=6/4.01853764 complete derivatives.liq.window.15m@1",
        "87187134ms 15m end=87180000ms long=7/3.04850276 short=5/3.45511619 complete derivatives.liq.window.15m@1",
        "87247606ms 15m end=87240000ms long=7/3.04850276 short=5/3.45511619 complete derivatives.liq.window.15m@1",
        "87306224ms 15m end=87300000ms long=8/3.58613738 short=5/2.96332206 complete derivatives.liq.window.15m@1",
        "87366450ms 15m end=87360000ms long=8/3.58613738 short=3/1.64660269 complete derivatives.liq.window.15m@1",
        "87427310ms 15m end=87420000ms long=10/4.72788685 short=3/1.64660269 complete derivatives.liq.window.15m@1",
        "87496465ms 15m end=87480000ms long=11/5.12071286 short=4/1.90687070 complete derivatives.liq.window.15m@1",
        "87547578ms 15m end=87540000ms long=11/5.12071286 short=3/1.13140280 complete derivatives.liq.window.15m@1",
        "87603979ms 15m end=87600000ms long=11/4.20263029 short=3/1.13140280 complete derivatives.liq.window.15m@1",
        "87679176ms 15m end=87660000ms long=11/4.44918753 short=2/0.35632936 complete derivatives.liq.window.15m@1",
        "87727765ms 15m end=87720000ms long=10/4.03907239 short=2/0.35632936 complete derivatives.liq.window.15m@1",
        "87784261ms 15m end=87780000ms long=8/3.29750418 short=2/0.35632936 complete derivatives.liq.window.15m@1",
        "87849010ms 15m end=87840000ms long=8/3.29750418 short=2/0.35632936 complete derivatives.liq.window.15m@1",
        "87913023ms 15m end=87900000ms long=8/3.87353735 short=2/0.35632936 complete derivatives.liq.window.15m@1",
        "87980359ms 15m end=87960000ms long=9/3.91107359 short=3/0.94526717 complete derivatives.liq.window.15m@1",
        "88033279ms 15m end=88020000ms long=9/3.91107359 short=3/0.94526717 complete derivatives.liq.window.15m@1",
        "88088965ms 15m end=88080000ms long=9/3.91107359 short=3/0.94526717 complete derivatives.liq.window.15m@1",
        "88151877ms 15m end=88140000ms long=9/3.91107359 short=3/0.94526717 complete derivatives.liq.window.15m@1",
        "88205074ms 15m end=88200000ms long=9/3.72309952 short=2/0.84920582 complete derivatives.liq.window.15m@1",
        "88270385ms 15m end=88260000ms long=9/3.72309952 short=4/1.53397120 complete derivatives.liq.window.15m@1",
        "88339326ms 15m end=88320000ms long=7/2.58135005 short=4/1.53397120 complete derivatives.liq.window.15m@1",
        "88383289ms 15m end=88380000ms long=6/2.18852404 short=3/1.27370319 complete derivatives.liq.window.15m@1",
        "88454033ms 15m end=88440000ms long=6/2.18852404 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88512885ms 15m end=88500000ms long=4/1.76613357 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88580109ms 15m end=88560000ms long=3/1.13251830 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88622292ms 15m end=88620000ms long=4/1.45440973 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88688131ms 15m end=88680000ms long=4/1.45440973 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88741090ms 15m end=88740000ms long=5/1.98482028 short=4/1.51324541 complete derivatives.liq.window.15m@1",
        "88810000ms 15m end=88740000ms long=5/1.98482028 short=4/1.51324541 feed_gap derivatives.liq.window.15m@1",
        "88814106ms 15m end=88800000ms long=5/1.55224119 short=5/1.94755550 feed_gap derivatives.liq.window.15m@1",
        "88879811ms 15m end=88860000ms long=4/1.51470495 short=4/1.35861769 feed_gap derivatives.liq.window.15m@1",
        "88936217ms 15m end=88920000ms long=5/1.85391702 short=5/1.82165967 feed_gap derivatives.liq.window.15m@1",
        "88993191ms 15m end=88980000ms long=7/2.87220425 short=5/1.82165967 feed_gap derivatives.liq.window.15m@1",
        "89057383ms 15m end=89040000ms long=7/2.87220425 short=5/1.82165967 feed_gap derivatives.liq.window.15m@1",
        "89112611ms 15m end=89100000ms long=8/2.99646422 short=5/1.82165967 feed_gap derivatives.liq.window.15m@1",
        "89167760ms 15m end=89160000ms long=8/2.99646422 short=4/1.38786667 feed_gap derivatives.liq.window.15m@1",
        "89224688ms 15m end=89220000ms long=8/2.99646422 short=5/1.52836465 feed_gap derivatives.liq.window.15m@1",
        "89292424ms 15m end=89280000ms long=9/3.15533891 short=6/1.94545538 feed_gap derivatives.liq.window.15m@1",
        "89346703ms 15m end=89340000ms long=9/3.15533891 short=5/1.70591316 feed_gap derivatives.liq.window.15m@1",
        "89414613ms 15m end=89400000ms long=10/3.86598207 short=6/2.39796375 feed_gap derivatives.liq.window.15m@1",
        "89470488ms 15m end=89460000ms long=10/3.86598207 short=7/2.74913659 feed_gap derivatives.liq.window.15m@1",
        "89529538ms 15m end=89520000ms long=9/3.54409064 short=7/2.74913659 feed_gap derivatives.liq.window.15m@1",
        "89597688ms 15m end=89580000ms long=10/3.75363638 short=7/2.74913659 feed_gap derivatives.liq.window.15m@1",
        "89658321ms 15m end=89640000ms long=9/3.22322583 short=7/2.74913659 feed_gap derivatives.liq.window.15m@1",
        "89703339ms 15m end=89700000ms long=8/2.91048341 short=6/2.31482650 feed_gap derivatives.liq.window.15m@1",
        "89776890ms 15m end=89760000ms long=8/2.91048341 short=6/2.31482650 complete derivatives.liq.window.15m@1",
        "89839098ms 15m end=89820000ms long=7/2.57127134 short=7/2.75755551 complete derivatives.liq.window.15m@1",
        "89890860ms 15m end=89880000ms long=6/2.18001476 short=7/2.75755551 complete derivatives.liq.window.15m@1",
        "89941944ms 15m end=89940000ms long=8/2.54052029 short=7/2.75755551 complete derivatives.liq.window.15m@1",
        "90016389ms 15m end=90000000ms long=6/2.06659977 short=7/2.75755551 complete derivatives.liq.window.15m@1",
        "90066010ms 15m end=90060000ms long=6/2.06659977 short=6/2.50658313 complete derivatives.liq.window.15m@1",
        "90126781ms 15m end=90120000ms long=7/2.30765096 short=5/2.36608515 complete derivatives.liq.window.15m@1",
        "90186942ms 15m end=90180000ms long=6/2.14877627 short=4/1.94899442 complete derivatives.liq.window.15m@1",
        "90253574ms 15m end=90240000ms long=6/2.14877627 short=5/2.63798568 complete derivatives.liq.window.15m@1",
        "90316474ms 15m end=90300000ms long=5/1.43813311 short=4/1.94593509 complete derivatives.liq.window.15m@1",
        "90360500ms 15m end=90360000ms long=5/1.43813311 short=3/1.59476225 complete derivatives.liq.window.15m@1",
    ];

    const GOLDEN_LIQ_WINDOW_1H: [&str; 15] = [
        "89529538ms 1h end=89520000ms long=31/13.36387208 short=22/9.54228689 partial_start+feed_gap derivatives.liq.window.1h@1",
        "89597688ms 1h end=89580000ms long=31/12.82458709 short=22/9.54228689 feed_gap derivatives.liq.window.1h@1",
        "89658321ms 1h end=89640000ms long=31/12.82458709 short=22/9.54228689 feed_gap derivatives.liq.window.1h@1",
        "89703339ms 1h end=89700000ms long=31/12.82458709 short=20/9.14579196 feed_gap derivatives.liq.window.1h@1",
        "89776890ms 1h end=89760000ms long=30/12.21987087 short=20/9.14579196 feed_gap derivatives.liq.window.1h@1",
        "89839098ms 1h end=89820000ms long=29/11.65852561 short=21/9.54301999 feed_gap derivatives.liq.window.1h@1",
        "89890860ms 1h end=89880000ms long=29/12.01179536 short=20/8.97959854 feed_gap derivatives.liq.window.1h@1",
        "89941944ms 1h end=89940000ms long=31/12.37230089 short=20/8.97959854 feed_gap derivatives.liq.window.1h@1",
        "90016389ms 1h end=90000000ms long=31/12.37230089 short=19/8.39174306 feed_gap derivatives.liq.window.1h@1",
        "90066010ms 1h end=90060000ms long=31/12.37230089 short=17/7.07502369 feed_gap derivatives.liq.window.1h@1",
        "90126781ms 1h end=90120000ms long=32/12.61335208 short=17/7.07502369 feed_gap derivatives.liq.window.1h@1",
        "90186942ms 1h end=90180000ms long=32/12.61335208 short=17/7.07502369 feed_gap derivatives.liq.window.1h@1",
        "90253574ms 1h end=90240000ms long=32/12.61335208 short=17/6.98854705 feed_gap derivatives.liq.window.1h@1",
        "90316474ms 1h end=90300000ms long=30/11.27287904 short=17/6.98854705 feed_gap derivatives.liq.window.1h@1",
        "90360500ms 1h end=90360000ms long=29/10.88582101 short=16/6.21347361 feed_gap derivatives.liq.window.1h@1",
    ];
}
