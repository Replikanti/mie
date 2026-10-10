//! Order flow and aggression state (Market State & Regime brief, "Order
//! Flow / Aggression State"; brief §8; ADR-021, ADR-023, ADR-035).
//!
//! Per-bar aggressive buy and sell volume and delta are part of every bar
//! (`bars.time.<tf>@1`, ADR-031). This module adds what spans bars:
//!
//! - **CVD**, under two explicit anchors (ADR-035, decision 2):
//!   - `flow.cvd.continuous@1` ([`Cvd`]): the sum of signed aggressor
//!     quantity since the first consumed trade. It runs through trades gaps
//!     and counts them, so its level is relative to the run.
//!   - `flow.cvd.utc_day@1` ([`DayCvd`]): the sum since 00:00 UTC — the delta
//!     of the developing daily bar, with its coverage.
//! - **Rolling aggression windows** `flow.window.<5m|15m|1h>@1`
//!   ([`AggressionWindow`]): the last 5, 15 or 60 closed 1m bars, stepped
//!   once per closed minute; the developing minute is never included
//!   (decision 1). Each carries buy and sell volume, delta, volume and trade
//!   count, large-print activity (decision 4), trade intensity (decision 5)
//!   and the price response to the net aggression (decision 6).
//!
//! Sums are exact on fixed point (ADR-027) with checked arithmetic;
//! overflow rejects the event. Ratios are floats derived on demand in a
//! fixed operation order and never stored, so the state stays `Eq`.
//!
//! Everything here measures aggression, never direction (ADR-023): no value
//! names or encodes a side bias or a signal (ADR-024).

use crate::bars::{Bar, BarSet, Coverage, Timeframe};
use crate::event::{Aggressor, MarketEvent, Stream, Trade};
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::Fingerprinter;
use crate::num::{Price, Qty, SCALE};
use crate::state_hash::StateEncode;
use crate::time::EventTime;
use std::fmt;

/// The large-print threshold in whole USDT: `flow.window.<tf>@1` parameter
/// `large_notional_usdt` (ADR-035, decision 4). A print is large when
/// `|price × qty|` is at or above it.
pub const LARGE_NOTIONAL_USDT: i64 = 100_000;

/// [`LARGE_NOTIONAL_USDT`] in the `1 / SCALE²` units of an exact
/// `price × qty` product.
const LARGE_NOTIONAL_UNITS: u128 = LARGE_NOTIONAL_USDT as u128 * SCALE as u128 * SCALE as u128;

/// Closed minutes kept: 60 for the longest window plus the minute before it,
/// whose close is the window's reference price.
const RING: usize = 61;

/// Whether `trade` is a large print (ADR-035, decision 4): its exact
/// notional `|price × qty|` is at or above [`LARGE_NOTIONAL_USDT`].
fn is_large(trade: &Trade) -> bool {
    let notional = i128::from(trade.price.units()) * i128::from(trade.qty.units());
    notional.unsigned_abs() >= LARGE_NOTIONAL_UNITS
}

/// Writes `value` or `-` for `None`.
fn write_opt<T: fmt::Display>(f: &mut fmt::Formatter<'_>, value: Option<T>) -> fmt::Result {
    match value {
        Some(value) => write!(f, "{value}"),
        None => f.write_str("-"),
    }
}

/// The continuous CVD: `flow.cvd.continuous@1` (ADR-035, decision 2).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `cvd=1.15000000 anchor=1000ms gaps=0 flow.cvd.continuous@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cvd {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Σ signed aggressor quantity since `anchor`: buy `+qty`, sell `−qty`.
    pub cvd: Qty,
    /// Time of the first consumed trade, where the sum starts.
    pub anchor: EventTime,
    /// Trades feed gaps consumed since `anchor`. The sum runs through them,
    /// so it misses whatever traded inside them; two values differ by the
    /// exact market delta only when `anchor` and `gaps` agree.
    pub gaps: u64,
}

impl StateEncode for Cvd {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            cvd,
            anchor,
            gaps,
        } = self;
        feature.encode(f);
        cvd.encode(f);
        anchor.encode(f);
        gaps.encode(f);
    }
}

impl fmt::Display for Cvd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "cvd={} anchor={} gaps={} {}",
            self.cvd, self.anchor, self.gaps, self.feature
        )
    }
}

/// The CVD since 00:00 UTC: `flow.cvd.utc_day@1` (ADR-035, decision 2).
///
/// `Display` prints the canonical line the golden tests pin, such as
/// `day=0ms cvd=1.15000000 partial_start flow.cvd.utc_day@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DayCvd {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Open time of the UTC day.
    pub day_open: EventTime,
    /// Σ signed aggressor quantity since `day_open`: the delta of the
    /// developing daily bar.
    pub cvd: Qty,
    /// The daily bar's coverage: an incomplete day misses trades.
    pub coverage: Coverage,
}

impl StateEncode for DayCvd {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            day_open,
            cvd,
            coverage,
        } = self;
        feature.encode(f);
        day_open.encode(f);
        cvd.encode(f);
        coverage.encode(f);
    }
}

impl fmt::Display for DayCvd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "day={} cvd={} {} {}",
            self.day_open, self.cvd, self.coverage, self.feature
        )
    }
}

/// Aggression over the last closed minutes: `flow.window.<tf>@1` (ADR-035,
/// decisions 1 and 3–6).
///
/// The exact fields are stored; the floats are derived on demand in a fixed
/// operation order, so they are the same everywhere. `Display` prints the
/// canonical line the golden tests pin, ending with the feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggressionWindow {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The window length.
    pub timeframe: Timeframe,
    /// Exclusive end of the window's last minute.
    pub end: EventTime,
    /// Volume of trades whose aggressor bought.
    pub buy_volume: Qty,
    /// Volume of trades whose aggressor sold.
    pub sell_volume: Qty,
    /// `buy_volume − sell_volume`: aggression, not direction (ADR-023).
    pub delta: Qty,
    /// Traded volume.
    pub volume: Qty,
    /// Number of trades.
    pub trade_count: u64,
    /// Number of large prints (decision 4).
    pub large_count: u64,
    /// Volume of large prints whose aggressor bought.
    pub large_buy_volume: Qty,
    /// Volume of large prints whose aggressor sold.
    pub large_sell_volume: Qty,
    /// The last trade price before the window starts; `None` until the run
    /// has a trade before it (decision 6).
    pub reference_price: Option<Price>,
    /// The last trade price at the window's end minus `reference_price`;
    /// `None` without a reference.
    pub displacement: Option<Price>,
    /// The OR of the window's minutes' coverage (decision 3).
    pub coverage: Coverage,
}

impl StateEncode for AggressionWindow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            timeframe,
            end,
            buy_volume,
            sell_volume,
            delta,
            volume,
            trade_count,
            large_count,
            large_buy_volume,
            large_sell_volume,
            reference_price,
            displacement,
            coverage,
        } = self;
        feature.encode(f);
        timeframe.encode(f);
        end.encode(f);
        buy_volume.encode(f);
        sell_volume.encode(f);
        delta.encode(f);
        volume.encode(f);
        trade_count.encode(f);
        large_count.encode(f);
        large_buy_volume.encode(f);
        large_sell_volume.encode(f);
        reference_price.encode(f);
        displacement.encode(f);
        coverage.encode(f);
    }
}

impl AggressionWindow {
    /// The window length in whole minutes.
    fn minutes(&self) -> f64 {
        // Whole minutes: exact in f64.
        (self.timeframe.millis() / Timeframe::M1.millis()) as f64
    }

    /// `delta / volume`, in [−1, 1]; `None` when the volume is 0.
    pub fn imbalance(&self) -> Option<f64> {
        (self.volume.units() != 0).then(|| self.delta.units() as f64 / self.volume.units() as f64)
    }

    /// Trades per minute over the window's nominal span, whatever its
    /// coverage (decision 5).
    pub fn trades_per_minute(&self) -> f64 {
        self.trade_count as f64 / self.minutes()
    }

    /// Volume in BTC per minute over the window's nominal span, whatever its
    /// coverage (decision 5).
    pub fn volume_per_minute(&self) -> f64 {
        (self.volume.units() as f64 / SCALE as f64) / self.minutes()
    }

    /// The share of the volume traded in large prints; `None` when the
    /// volume is 0.
    pub fn large_volume_share(&self) -> Option<f64> {
        let large =
            i128::from(self.large_buy_volume.units()) + i128::from(self.large_sell_volume.units());
        (self.volume.units() != 0).then(|| large as f64 / self.volume.units() as f64)
    }

    /// `displacement / delta`: USDT of price displacement per BTC of net
    /// aggression (decision 6); `None` when the delta is 0 or there is no
    /// displacement. A negative value means price moved against the net
    /// aggression — a measurement, never a direction (ADR-023).
    pub fn price_response(&self) -> Option<f64> {
        let displacement = self.displacement?;
        (self.delta.units() != 0).then(|| displacement.units() as f64 / self.delta.units() as f64)
    }
}

impl fmt::Display for AggressionWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} end={} buy={} sell={} delta={} vol={} trades={} large={} \
             large_buy={} large_sell={} ref=",
            self.timeframe,
            self.end,
            self.buy_volume,
            self.sell_volume,
            self.delta,
            self.volume,
            self.trade_count,
            self.large_count,
            self.large_buy_volume,
            self.large_sell_volume
        )?;
        write_opt(f, self.reference_price)?;
        f.write_str(" disp=")?;
        write_opt(f, self.displacement)?;
        f.write_str(" imbalance=")?;
        write_opt(f, self.imbalance())?;
        write!(
            f,
            " trades_per_min={} vol_per_min={} large_share=",
            self.trades_per_minute(),
            self.volume_per_minute()
        )?;
        write_opt(f, self.large_volume_share())?;
        f.write_str(" response=")?;
        write_opt(f, self.price_response())?;
        write!(f, " {} {}", self.coverage, self.feature)
    }
}

/// The aggression window of every length, in [`catalog::FLOW_WINDOWS`]
/// order: the `flow.window.*@1` features of the Market State.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AggressionWindows {
    values: [FeatureValue<AggressionWindow>; 3],
}

impl StateEncode for AggressionWindows {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self { values } = self;
        values.encode(f);
    }
}

impl Default for AggressionWindows {
    fn default() -> Self {
        Self::new()
    }
}

impl AggressionWindows {
    /// Every window warming up, for [`catalog::FLOW_WINDOWS`].
    pub fn new() -> Self {
        Self {
            values: catalog::FLOW_WINDOWS.map(|(timeframe, _)| FeatureValue::WarmingUp {
                observed: 0,
                required: window_minutes(timeframe),
            }),
        }
    }

    /// The window of length `timeframe`, if the set computes it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&FeatureValue<AggressionWindow>> {
        catalog::FLOW_WINDOWS
            .iter()
            .position(|(candidate, _)| *candidate == timeframe)
            .map(|index| &self.values[index])
    }

    /// Every window, shortest first, with its feature
    /// (`flow.window.<label>@1`) and value.
    pub fn iter(
        &self,
    ) -> impl Iterator<Item = (Timeframe, FeatureKey, &FeatureValue<AggressionWindow>)> {
        catalog::FLOW_WINDOWS
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

/// The order-flow features of the Market State (ADR-035): aggression, not
/// direction (ADR-023).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderFlow {
    /// `flow.cvd.continuous@1`, warming up until the first trade.
    pub cvd: FeatureValue<Cvd>,
    /// `flow.cvd.utc_day@1`, warming up until the first trades-stream event.
    pub cvd_utc_day: FeatureValue<DayCvd>,
    /// `flow.window.<5m|15m|1h>@1`, each warming up until its window of
    /// closed minutes is full.
    pub windows: AggressionWindows,
}

impl StateEncode for OrderFlow {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            cvd,
            cvd_utc_day,
            windows,
        } = self;
        cvd.encode(f);
        cvd_utc_day.encode(f);
        windows.encode(f);
    }
}

impl Default for OrderFlow {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderFlow {
    /// Every feature warming up.
    pub fn new() -> Self {
        Self {
            cvd: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            cvd_utc_day: FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            windows: AggressionWindows::new(),
        }
    }
}

/// The large prints of one minute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LargeTally {
    /// Open time of the minute.
    open: EventTime,
    count: u64,
    buy_volume: Qty,
    sell_volume: Qty,
}

impl LargeTally {
    fn empty(open: EventTime) -> Self {
        Self {
            open,
            count: 0,
            buy_volume: Qty::from_units(0),
            sell_volume: Qty::from_units(0),
        }
    }

    /// Counts `trade` if it is large, or `None` on overflow.
    fn add(mut self, trade: &Trade) -> Option<Self> {
        if is_large(trade) {
            self.count = self.count.checked_add(1)?;
            match trade.aggressor {
                Aggressor::Buy => self.buy_volume = self.buy_volume.checked_add(trade.qty)?,
                Aggressor::Sell => self.sell_volume = self.sell_volume.checked_add(trade.qty)?,
            }
        }
        Some(self)
    }
}

/// One closed minute, as the windows need it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Minute {
    /// Exclusive end of the minute.
    end: EventTime,
    buy_volume: Qty,
    sell_volume: Qty,
    delta: Qty,
    volume: Qty,
    trade_count: u64,
    large: LargeTally,
    /// The last trade price at the minute's end, carried over empty
    /// minutes; `None` before the run's first trade.
    close_after: Option<Price>,
    coverage: Coverage,
}

impl Minute {
    const NONE: Self = Self {
        end: EventTime::from_millis(0),
        buy_volume: Qty::from_units(0),
        sell_volume: Qty::from_units(0),
        delta: Qty::from_units(0),
        volume: Qty::from_units(0),
        trade_count: 0,
        large: LargeTally {
            open: EventTime::from_millis(0),
            count: 0,
            buy_volume: Qty::from_units(0),
            sell_volume: Qty::from_units(0),
        },
        close_after: None,
        coverage: Coverage {
            partial_start: false,
            feed_gap: false,
        },
    };
}

/// The latest closed minutes, oldest overwritten first.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MinuteRing {
    minutes: [Minute; RING],
    /// Where the next minute goes.
    next: usize,
}

impl MinuteRing {
    fn new() -> Self {
        Self {
            minutes: [Minute::NONE; RING],
            next: 0,
        }
    }

    fn push(&mut self, minute: Minute) {
        self.minutes[self.next] = minute;
        self.next = (self.next + 1) % RING;
    }

    /// The minute `back` closes before the latest one (`0` is the latest);
    /// `back < RING`, and only minutes that were pushed are meaningful.
    fn back(&self, back: usize) -> &Minute {
        &self.minutes[(self.next + RING - 1 - back) % RING]
    }
}

/// The small, per-event part of the tracker: copied on every event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlowHead {
    /// The current feature values.
    flow: OrderFlow,
    /// Large prints of the developing minute.
    tally: Option<LargeTally>,
    /// Closed 1m bars consumed.
    closed_minutes: u64,
}

/// The order-flow engine state: running CVD, the developing minute's large
/// prints and the last [`RING`] closed minutes (ADR-035). Engine state; the
/// Market State exposes only [`OrderFlow`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FlowTracker {
    head: FlowHead,
    ring: MinuteRing,
}

/// The tracker after one event, committed with the bars it was computed
/// from.
pub(crate) struct FlowStep {
    head: FlowHead,
    /// `None` when the event closed no 1m bar: the ring is unchanged.
    ring: Option<MinuteRing>,
}

impl Default for FlowTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FlowTracker {
    /// An empty tracker, every feature warming up.
    pub(crate) fn new() -> Self {
        Self {
            head: FlowHead {
                flow: OrderFlow::new(),
                tally: None,
                closed_minutes: 0,
            },
            ring: MinuteRing::new(),
        }
    }

    /// The current feature values.
    pub(crate) fn flow(&self) -> OrderFlow {
        self.head.flow
    }

    /// Steps the order flow with `event`, given the bars it closed
    /// (`closed`, in close order) and the bar set after it (`bars`), on
    /// copies (ADR-035). The ring of minutes is copied only when a 1m bar
    /// closed.
    ///
    /// Order: closed 1m bars fold into the ring first (the first takes the
    /// developing minute's large prints), then the windows are recomputed,
    /// then a trade enters the CVD and the new developing minute's tally,
    /// then the UTC-day CVD is read from the developing daily bar.
    ///
    /// # Errors
    ///
    /// [`FlowError::Overflow`] if a sum leaves the `i64` range; nothing is
    /// committed then.
    pub(crate) fn step(
        &self,
        event: &MarketEvent,
        closed: &[Bar],
        bars: &BarSet,
    ) -> Result<FlowStep, FlowError> {
        let mut head = self.head;
        let mut minutes = closed
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M1)
            .peekable();
        let ring = if minutes.peek().is_some() {
            let mut ring = self.ring.clone();
            for bar in minutes {
                head.fold(bar, &mut ring)?;
            }
            head.flow.windows = windows(&ring, head.closed_minutes)?;
            Some(ring)
        } else {
            None
        };

        match event {
            MarketEvent::Trade(trade) => head.add_trade(trade)?,
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                if let FeatureValue::Ready(cvd) = &mut head.flow.cvd {
                    cvd.gaps = cvd.gaps.checked_add(1).ok_or(FlowError::Overflow)?;
                }
            }
            // Other streams carry no aggressor flow.
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }

        if let Some(day) = bars.get(Timeframe::D1) {
            head.flow.cvd_utc_day = day.developing().map(|bar| DayCvd {
                feature: catalog::FLOW_CVD_UTC_DAY_V1.key,
                day_open: bar.open_time,
                cvd: bar.delta,
                coverage: bar.coverage,
            });
        }
        Ok(FlowStep { head, ring })
    }

    /// Commits a step computed by [`Self::step`].
    pub(crate) fn commit(&mut self, step: FlowStep) {
        self.head = step.head;
        if let Some(ring) = step.ring {
            self.ring = ring;
        }
    }
}

impl FlowHead {
    /// Folds one closed 1m bar into `ring`, with the developing minute's
    /// large prints if they belong to it.
    fn fold(&mut self, bar: &Bar, ring: &mut MinuteRing) -> Result<(), FlowError> {
        let large = match self.tally.take() {
            Some(tally) if tally.open == bar.open_time => tally,
            _ => LargeTally::empty(bar.open_time),
        };
        // The latest minute's; `None` before the first minute.
        let carried = ring.back(0).close_after;
        ring.push(Minute {
            end: bar.end(),
            buy_volume: bar.buy_volume,
            sell_volume: bar.sell_volume,
            delta: bar.delta,
            volume: bar.volume,
            trade_count: bar.trade_count,
            large,
            close_after: bar.ohlc.map(|ohlc| ohlc.close).or(carried),
            coverage: bar.coverage,
        });
        self.closed_minutes = self
            .closed_minutes
            .checked_add(1)
            .ok_or(FlowError::Overflow)?;
        Ok(())
    }

    /// Adds `trade` to the CVD and to the developing minute's tally.
    fn add_trade(&mut self, trade: &Trade) -> Result<(), FlowError> {
        let cvd = match self.flow.cvd {
            FeatureValue::Ready(cvd) => cvd,
            FeatureValue::WarmingUp { .. } | FeatureValue::Unavailable { .. } => Cvd {
                feature: catalog::FLOW_CVD_CONTINUOUS_V1.key,
                cvd: Qty::from_units(0),
                anchor: trade.time,
                gaps: 0,
            },
        };
        let sum = match trade.aggressor {
            Aggressor::Buy => cvd.cvd.checked_add(trade.qty),
            Aggressor::Sell => cvd.cvd.checked_sub(trade.qty),
        }
        .ok_or(FlowError::Overflow)?;
        let open = Timeframe::M1
            .open_of(trade.time)
            .ok_or(FlowError::Overflow)?;
        let tally = match self.tally {
            Some(tally) if tally.open == open => tally,
            _ => LargeTally::empty(open),
        };
        self.tally = Some(tally.add(trade).ok_or(FlowError::Overflow)?);
        self.flow.cvd = FeatureValue::Ready(Cvd { cvd: sum, ..cvd });
        Ok(())
    }
}

/// Every window after the latest closed minute.
fn windows(ring: &MinuteRing, closed_minutes: u64) -> Result<AggressionWindows, FlowError> {
    let mut values = AggressionWindows::new().values;
    for ((timeframe, definition), value) in catalog::FLOW_WINDOWS.iter().zip(&mut values) {
        let required = window_minutes(*timeframe);
        *value = if closed_minutes < required {
            FeatureValue::WarmingUp {
                observed: closed_minutes,
                required,
            }
        } else {
            FeatureValue::Ready(window(ring, *timeframe, definition.key, closed_minutes)?)
        };
    }
    Ok(AggressionWindows { values })
}

/// The window of length `timeframe` over the latest closed minutes; at
/// least a window's worth have closed.
fn window(
    ring: &MinuteRing,
    timeframe: Timeframe,
    feature: FeatureKey,
    closed_minutes: u64,
) -> Result<AggressionWindow, FlowError> {
    let overflow = || FlowError::Overflow;
    let required = window_minutes(timeframe);
    let span = usize::try_from(required).map_err(|_| overflow())?;
    let zero = Qty::from_units(0);
    let mut sum = AggressionWindow {
        feature,
        timeframe,
        end: ring.back(0).end,
        buy_volume: zero,
        sell_volume: zero,
        delta: zero,
        volume: zero,
        trade_count: 0,
        large_count: 0,
        large_buy_volume: zero,
        large_sell_volume: zero,
        reference_price: None,
        displacement: None,
        coverage: Coverage::default(),
    };
    for back in 0..span {
        let minute = ring.back(back);
        sum.buy_volume = sum
            .buy_volume
            .checked_add(minute.buy_volume)
            .ok_or_else(overflow)?;
        sum.sell_volume = sum
            .sell_volume
            .checked_add(minute.sell_volume)
            .ok_or_else(overflow)?;
        sum.delta = sum.delta.checked_add(minute.delta).ok_or_else(overflow)?;
        sum.volume = sum.volume.checked_add(minute.volume).ok_or_else(overflow)?;
        sum.trade_count = sum
            .trade_count
            .checked_add(minute.trade_count)
            .ok_or_else(overflow)?;
        sum.large_count = sum
            .large_count
            .checked_add(minute.large.count)
            .ok_or_else(overflow)?;
        sum.large_buy_volume = sum
            .large_buy_volume
            .checked_add(minute.large.buy_volume)
            .ok_or_else(overflow)?;
        sum.large_sell_volume = sum
            .large_sell_volume
            .checked_add(minute.large.sell_volume)
            .ok_or_else(overflow)?;
        sum.coverage.partial_start |= minute.coverage.partial_start;
        sum.coverage.feed_gap |= minute.coverage.feed_gap;
    }
    // The minute before the window holds the last trade price at its start.
    if closed_minutes > required {
        sum.reference_price = ring.back(span).close_after;
    }
    if let (Some(reference), Some(last)) = (sum.reference_price, ring.back(0).close_after) {
        let displacement = i128::from(last.units()) - i128::from(reference.units());
        sum.displacement = Some(Price::from_units(
            i64::try_from(displacement).map_err(|_| overflow())?,
        ));
    }
    Ok(sum)
}

/// Why the order flow could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowError {
    /// A CVD, window sum, count or displacement left its integer range
    /// (ADR-027).
    Overflow,
}

impl fmt::Display for FlowError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => f.write_str("an order-flow value leaves its integer range"),
        }
    }
}

impl std::error::Error for FlowError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::tests::{Lcg, random_tape};
    use crate::event::GapReason;
    use crate::event::samples::{gap, mark, snapshot, t};
    use crate::state::MarketStateEngine;

    const DAY: i64 = 86_400_000;
    const MINUTE: i64 = 60_000;
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

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    fn qty(text: &str) -> Qty {
        text.parse().unwrap()
    }

    fn trade(
        millis: i64,
        trade_id: u64,
        at: &str,
        size: &str,
        aggressor: Aggressor,
    ) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: price(at),
            qty: qty(size),
            aggressor,
        })
    }

    fn buy(millis: i64, trade_id: u64, at: &str, size: &str) -> MarketEvent {
        trade(millis, trade_id, at, size, Aggressor::Buy)
    }

    fn sell(millis: i64, trade_id: u64, at: &str, size: &str) -> MarketEvent {
        trade(millis, trade_id, at, size, Aggressor::Sell)
    }

    fn warming<T>(observed: u64, required: u64) -> FeatureValue<T> {
        FeatureValue::WarmingUp { observed, required }
    }

    /// The engine's order flow after each event.
    fn flows(events: &[MarketEvent]) -> Vec<OrderFlow> {
        let mut engine = MarketStateEngine::new();
        events
            .iter()
            .map(|event| {
                engine.apply(event).unwrap();
                engine.state().flow
            })
            .collect()
    }

    fn window_5m(flow: &OrderFlow) -> FeatureValue<AggressionWindow> {
        *flow.windows.get(Timeframe::M5).unwrap()
    }

    fn ready_5m(flow: &OrderFlow) -> AggressionWindow {
        *window_5m(flow).ready().unwrap()
    }

    #[test]
    fn cvd_is_exact_and_counts_trades_gaps_after_the_anchor() {
        let events = [
            gap(Stream::OrderBook, 0, 100, GapReason::Disconnected),
            // A trades gap before the first trade sets no anchor and counts
            // nothing.
            gap(Stream::Trades, 100, 200, GapReason::Disconnected),
            buy(1_000, 1, "63500", "0.5"),
            sell(2_000, 2, "63501", "0.25"),
            gap(Stream::Trades, 2_500, 3_000, GapReason::SequenceBreak),
            gap(Stream::OrderBook, 3_000, 3_500, GapReason::Disconnected),
            buy(4_000, 3, "63502", "1.0"),
            sell(5_000, 4, "63499", "0.1"),
        ];
        let flows = flows(&events);
        assert_eq!(flows[0].cvd, warming(0, 1));
        assert_eq!(flows[1].cvd, warming(0, 1));
        let cvd = |index: usize| *flows[index].cvd.ready().unwrap();
        assert_eq!(
            (cvd(2).cvd, cvd(2).anchor, cvd(2).gaps),
            (qty("0.5"), t(1_000), 0)
        );
        assert_eq!(cvd(3).cvd, qty("0.25"));
        assert_eq!(cvd(4).gaps, 1);
        // An order-book gap does not count.
        assert_eq!(cvd(5), cvd(4));
        assert_eq!(cvd(6).cvd, qty("1.25"));
        let last = cvd(7);
        assert_eq!(
            last,
            Cvd {
                feature: catalog::FLOW_CVD_CONTINUOUS_V1.key,
                cvd: Qty::from_units(115_000_000),
                anchor: t(1_000),
                gaps: 1,
            }
        );
        assert_eq!(
            last.to_string(),
            "cvd=1.15000000 anchor=1000ms gaps=1 flow.cvd.continuous@1"
        );
    }

    #[test]
    fn utc_day_cvd_is_the_developing_daily_delta() {
        let events = [
            mark(DAY - 3_000, 1),
            buy(DAY - 2_000, 1, "63500", "0.5"),
            sell(DAY - 1_000, 2, "63500", "0.2"),
            // The first trades-stream event of the new day resets it.
            gap(
                Stream::Trades,
                DAY - 500,
                DAY + 100,
                GapReason::Disconnected,
            ),
            buy(DAY + 1_000, 3, "63500", "0.1"),
            sell(DAY + 2_000, 4, "63500", "0.4"),
            snapshot(DAY + 2_500, 10),
        ];
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        for event in &events {
            engine.apply(event).unwrap();
            let state = engine.state();
            let day = state.bars.get(Timeframe::D1).unwrap().developing();
            assert_eq!(
                state
                    .flow
                    .cvd_utc_day
                    .map(|cvd| (cvd.day_open, cvd.cvd, cvd.coverage)),
                day.map(|bar| (bar.open_time, bar.delta, bar.coverage)),
                "{event:?}"
            );
            lines.push(match state.flow.cvd_utc_day {
                FeatureValue::Ready(cvd) => cvd.to_string(),
                other => format!("{other:?}"),
            });
        }
        assert_eq!(
            lines,
            [
                "WarmingUp { observed: 0, required: 1 }",
                "day=0ms cvd=0.50000000 partial_start flow.cvd.utc_day@1",
                "day=0ms cvd=0.30000000 partial_start flow.cvd.utc_day@1",
                "day=86400000ms cvd=0.00000000 feed_gap flow.cvd.utc_day@1",
                "day=86400000ms cvd=0.10000000 feed_gap flow.cvd.utc_day@1",
                "day=86400000ms cvd=-0.30000000 feed_gap flow.cvd.utc_day@1",
                "day=86400000ms cvd=-0.30000000 feed_gap flow.cvd.utc_day@1",
            ]
        );
        // The continuous CVD runs on across the day and counts the gap.
        let cvd = *engine.state().flow.cvd.ready().unwrap();
        assert_eq!((cvd.cvd, cvd.gaps), (qty("0"), 1));
        // A trade exactly at midnight opens the new day, complete.
        let flows = flows(&[
            buy(DAY - 1, 1, "63500", "0.5"),
            sell(DAY, 2, "63500", "0.2"),
        ]);
        let day = *flows[1].cvd_utc_day.ready().unwrap();
        assert_eq!(
            (day.day_open, day.cvd, day.coverage),
            (t(DAY), qty("-0.2"), COMPLETE)
        );
    }

    #[test]
    fn large_prints_meet_the_threshold_exactly() {
        let print = |at: &str, size: &str, aggressor| Trade {
            time: t(0),
            trade_id: 1,
            price: price(at),
            qty: qty(size),
            aggressor,
        };
        // 62 500 × 1.6 = 100 000 USDT exactly; one unit of 1e-8 BTC less is
        // 99 999.99999375.
        assert!(is_large(&print("62500", "1.6", Aggressor::Buy)));
        assert!(is_large(&print("62500", "1.6", Aggressor::Sell)));
        assert!(!is_large(&print("62500", "1.59999999", Aggressor::Buy)));
        assert!(!is_large(&print("62499.99999999", "1.6", Aggressor::Buy)));
        assert!(is_large(&print("100000", "1", Aggressor::Buy)));
        // The extremes do not overflow the `i128` product.
        let extreme = Trade {
            price: Price::from_units(i64::MIN),
            qty: Qty::from_units(i64::MIN),
            ..print("1", "1", Aggressor::Sell)
        };
        assert!(is_large(&extreme));
        assert_eq!(LARGE_NOTIONAL_UNITS, 100_000 * 10_u128.pow(16));

        // Through the engine: sides are kept apart, and a print on a minute
        // boundary belongs to the minute it opens.
        let flows = flows(&[
            buy(1_000, 1, "62500", "1.6"),
            buy(2_000, 2, "62500", "1.59999999"),
            sell(3_000, 3, "62500", "2.0"),
            buy(59_999, 4, "62500", "1.6"),
            sell(60_000, 5, "62500", "1.6"),
            buy(300_000, 6, "62500", "0.1"),
            buy(360_000, 7, "62500", "0.1"),
        ]);
        let first = ready_5m(&flows[5]);
        assert_eq!(
            (
                first.large_count,
                first.large_buy_volume,
                first.large_sell_volume
            ),
            (4, qty("3.2"), qty("3.6"))
        );
        assert_eq!(
            (first.buy_volume, first.sell_volume),
            (qty("4.79999999"), qty("3.6"))
        );
        // Minute 0 leaves the window: the boundary print stays with minute 1.
        let second = ready_5m(&flows[6]);
        assert_eq!(
            (
                second.large_count,
                second.large_buy_volume,
                second.large_sell_volume
            ),
            (1, qty("0"), qty("1.6"))
        );
    }

    #[test]
    fn constants_match_the_catalog() {
        for (timeframe, definition) in catalog::FLOW_WINDOWS {
            let large = definition
                .params
                .iter()
                .find(|param| param.name == "large_notional_usdt")
                .map(|param| param.value);
            assert_eq!(
                large,
                Some(crate::feature::ParamValue::Int(LARGE_NOTIONAL_USDT)),
                "{timeframe}"
            );
            assert_eq!(
                definition.warm_up,
                crate::feature::WarmUp::Samples(u32::try_from(window_minutes(timeframe)).unwrap())
            );
        }
        assert_eq!(
            catalog::FLOW_WINDOWS.map(|(timeframe, _)| window_minutes(timeframe)),
            [5, 15, 60]
        );
        // The ring holds the longest window plus its reference minute.
        assert_eq!(RING, 61);
    }

    /// One trade per minute, with an empty minute 3, a trades gap in minute
    /// 6 and an empty minute 7.
    fn window_tape() -> Vec<MarketEvent> {
        vec![
            buy(1_000, 1, "100", "1.0"),
            sell(61_000, 2, "101", "0.5"),
            buy(121_000, 3, "102", "0.25"),
            buy(241_000, 4, "103", "1.0"),
            sell(242_000, 5, "104", "1.0"),
            buy(301_000, 6, "99", "2.0"),
            sell(361_000, 7, "98", "0.5"),
            gap(Stream::Trades, 361_500, 362_000, GapReason::Disconnected),
            buy(481_000, 8, "97", "0.5"),
            buy(541_000, 9, "96", "0.1"),
            buy(601_000, 10, "95", "0.1"),
            buy(661_000, 11, "94", "0.1"),
            buy(721_000, 12, "93", "0.1"),
            buy(781_000, 13, "92", "0.1"),
        ]
    }

    #[test]
    fn windows_warm_up_then_slide_over_closed_minutes() {
        let flows = flows(&window_tape());
        // Closed minutes: 0 after the first trade, 1, 2, 4 after the trade
        // in minute 4, 4, 5, ...
        let observed = [0, 1, 2, 4, 4];
        for (flow, observed) in flows.iter().zip(observed) {
            assert_eq!(window_5m(flow), warming(observed, 5));
            assert_eq!(
                flow.windows.get(Timeframe::M15),
                Some(&warming(observed, 15))
            );
            assert_eq!(
                flow.windows.get(Timeframe::H1),
                Some(&warming(observed, 60))
            );
        }
        // Minutes 0–4: no minute before the window, so no reference.
        let first = ready_5m(&flows[5]);
        assert_eq!(
            first,
            AggressionWindow {
                feature: catalog::FLOW_WINDOW_5M_V1.key,
                timeframe: Timeframe::M5,
                end: t(300_000),
                buy_volume: qty("2.25"),
                sell_volume: qty("1.5"),
                delta: qty("0.75"),
                volume: qty("3.75"),
                trade_count: 5,
                large_count: 0,
                large_buy_volume: qty("0"),
                large_sell_volume: qty("0"),
                reference_price: None,
                displacement: None,
                coverage: PARTIAL,
            }
        );
        assert_eq!(first.price_response(), None);
        // Minutes 1–5: minute 0 left; its close is the reference.
        let second = ready_5m(&flows[6]);
        assert_eq!(
            (second.buy_volume, second.sell_volume, second.delta),
            (qty("3.25"), qty("1.5"), qty("1.75"))
        );
        assert_eq!((second.trade_count, second.coverage), (5, COMPLETE));
        assert_eq!(
            (second.reference_price, second.displacement),
            (Some(price("100")), Some(price("-1")))
        );
        // The gap does not step the windows; it closes no minute.
        assert_eq!(flows[7].windows, flows[6].windows);
        // Minutes 3–7: the gapped minute 6 and the empty minute 7 count
        // (zeros, and the gap flag); the last price is carried over minute 7.
        let third = ready_5m(&flows[8]);
        assert_eq!(third.end, t(480_000));
        assert_eq!(
            (
                third.buy_volume,
                third.sell_volume,
                third.delta,
                third.volume
            ),
            (qty("3"), qty("1.5"), qty("1.5"), qty("4.5"))
        );
        assert_eq!((third.trade_count, third.coverage), (4, GAP));
        assert_eq!(
            (third.reference_price, third.displacement),
            (Some(price("102")), Some(price("-4")))
        );
        // Never back to warming up; complete again once the gap left.
        for flow in &flows[8..] {
            assert!(window_5m(flow).is_ready());
        }
        let last = ready_5m(&flows[13]);
        assert_eq!((last.end, last.coverage), (t(780_000), COMPLETE));
        assert_eq!(last.trade_count, 5);
    }

    #[test]
    fn derived_floats_follow_their_documented_expressions() {
        let flows = flows(&window_tape());
        let window = ready_5m(&flows[6]);
        let (delta, volume) = (window.delta.units(), window.volume.units());
        assert_eq!(window.imbalance(), Some(delta as f64 / volume as f64));
        assert_eq!(window.imbalance(), Some(175_000_000_f64 / 475_000_000_f64));
        assert_eq!(window.trades_per_minute(), 5_f64 / 5_f64);
        assert_eq!(
            window.volume_per_minute(),
            (475_000_000_f64 / 100_000_000_f64) / 5_f64
        );
        assert_eq!(window.large_volume_share(), Some(0_f64 / 475_000_000_f64));
        assert_eq!(
            window.price_response(),
            Some(-100_000_000_f64 / 175_000_000_f64)
        );
        assert_eq!(
            window.to_string(),
            "5m end=360000ms buy=3.25000000 sell=1.50000000 delta=1.75000000 \
             vol=4.75000000 trades=5 large=0 large_buy=0.00000000 \
             large_sell=0.00000000 ref=100.00000000 disp=-1.00000000 \
             imbalance=0.3684210526315789 trades_per_min=1 vol_per_min=0.95 \
             large_share=0 response=-0.5714285714285714 complete flow.window.5m@1"
        );
        let large = AggressionWindow {
            large_buy_volume: qty("1"),
            large_sell_volume: qty("0.5"),
            ..window
        };
        assert_eq!(
            large.large_volume_share(),
            Some(150_000_000_f64 / 475_000_000_f64)
        );
        // A 15-minute window divides by 15.
        let long = AggressionWindow {
            timeframe: Timeframe::M15,
            ..window
        };
        assert_eq!(long.trades_per_minute(), 5_f64 / 15_f64);
        assert_eq!(long.volume_per_minute(), 4.75 / 15_f64);
        // No volume: no ratios over it.
        let idle = AggressionWindow {
            buy_volume: qty("0"),
            sell_volume: qty("0"),
            delta: qty("0"),
            volume: qty("0"),
            trade_count: 0,
            ..window
        };
        assert_eq!(idle.imbalance(), None);
        assert_eq!(idle.large_volume_share(), None);
        assert_eq!(idle.price_response(), None);
        assert!(
            idle.to_string()
                .contains("imbalance=- trades_per_min=0 vol_per_min=0 large_share=- response=-")
        );
    }

    #[test]
    fn no_response_without_net_aggression() {
        let flows = flows(&[
            buy(1_000, 1, "100", "1.0"),
            buy(61_000, 2, "101", "0.5"),
            sell(62_000, 3, "102", "0.5"),
            buy(361_000, 4, "102", "0.1"),
        ]);
        // Minutes 1–5: balanced aggression, price displaced by 2.
        let window = ready_5m(&flows[3]);
        assert_eq!(window.delta, qty("0"));
        assert_eq!(window.displacement, Some(price("2")));
        assert_eq!(window.imbalance(), Some(0.0));
        assert_eq!(window.price_response(), None);
    }

    /// The last price of `trades` (sorted by time) before `time`.
    fn last_price_before(trades: &[Trade], time: EventTime) -> Option<Price> {
        let index = trades.partition_point(|trade| trade.time < time);
        index.checked_sub(1).map(|last| trades[last].price)
    }

    #[test]
    fn windows_fold_their_minutes_on_random_tapes() {
        let threshold = i128::from(LARGE_NOTIONAL_USDT) * i128::from(SCALE) * i128::from(SCALE);
        for seed in [11, 12, 13] {
            let tape = random_tape(seed, 4_000);
            let mut engine = MarketStateEngine::new();
            let mut minutes: Vec<Bar> = Vec::new();
            let mut trades: Vec<Trade> = Vec::new();
            let mut cvd: i128 = 0;
            let (mut checked, mut boundaries) = (0, 0);
            for event in &tape {
                engine.apply(event).unwrap();
                let state = engine.state();
                if let MarketEvent::Trade(trade) = event {
                    trades.push(*trade);
                    cvd += match trade.aggressor {
                        Aggressor::Buy => i128::from(trade.qty.units()),
                        Aggressor::Sell => -i128::from(trade.qty.units()),
                    };
                }
                if !trades.is_empty() {
                    let value = state.flow.cvd.ready().unwrap();
                    assert_eq!(i128::from(value.cvd.units()), cvd, "seed {seed}");
                }
                let closed = engine.closed_bars();
                let before = minutes.len();
                minutes.extend(closed.iter().filter(|bar| bar.timeframe == Timeframe::M1));
                if minutes.len() == before {
                    continue;
                }
                for (timeframe, feature, value) in state.flow.windows.iter() {
                    let span = usize::try_from(window_minutes(timeframe)).unwrap();
                    let window = match value {
                        FeatureValue::Ready(window) => window,
                        other => {
                            assert!(minutes.len() < span, "seed {seed}");
                            assert_eq!(*other, warming(minutes.len() as u64, span as u64));
                            continue;
                        }
                    };
                    assert_eq!(window.feature, feature);
                    let parts = &minutes[minutes.len() - span..];
                    let start = parts[0].open_time;
                    assert_eq!(window.end, parts[span - 1].end(), "seed {seed}");
                    let mut expected = Bar::empty(Timeframe::M1, start);
                    for part in parts {
                        expected.buy_volume =
                            expected.buy_volume.checked_add(part.buy_volume).unwrap();
                        expected.sell_volume =
                            expected.sell_volume.checked_add(part.sell_volume).unwrap();
                        expected.delta = expected.delta.checked_add(part.delta).unwrap();
                        expected.volume = expected.volume.checked_add(part.volume).unwrap();
                        expected.trade_count += part.trade_count;
                        expected.coverage.partial_start |= part.coverage.partial_start;
                        expected.coverage.feed_gap |= part.coverage.feed_gap;
                    }
                    let sums = |w: &AggressionWindow| {
                        (
                            w.buy_volume,
                            w.sell_volume,
                            w.delta,
                            w.volume,
                            w.trade_count,
                            w.coverage,
                        )
                    };
                    let folded = (
                        expected.buy_volume,
                        expected.sell_volume,
                        expected.delta,
                        expected.volume,
                        expected.trade_count,
                        expected.coverage,
                    );
                    assert_eq!(sums(window), folded, "seed {seed} {timeframe}");
                    // An independent classification of the window's trades.
                    let from = trades.partition_point(|trade| trade.time < start);
                    let to = trades.partition_point(|trade| trade.time < window.end);
                    let (mut count, mut large_buy, mut large_sell) = (0, 0, 0);
                    for trade in &trades[from..to] {
                        let notional =
                            i128::from(trade.price.units()) * i128::from(trade.qty.units());
                        if notional.abs() >= threshold {
                            count += 1;
                            match trade.aggressor {
                                Aggressor::Buy => large_buy += trade.qty.units(),
                                Aggressor::Sell => large_sell += trade.qty.units(),
                            }
                        }
                    }
                    assert_eq!(
                        (
                            window.large_count,
                            window.large_buy_volume.units(),
                            window.large_sell_volume.units()
                        ),
                        (count, large_buy, large_sell),
                        "seed {seed} {timeframe}"
                    );
                    assert!(window.large_buy_volume <= window.buy_volume);
                    assert!(window.large_sell_volume <= window.sell_volume);
                    // Reference and displacement from the trades themselves.
                    let reference = if minutes.len() > span {
                        last_price_before(&trades, start)
                    } else {
                        None
                    };
                    assert_eq!(window.reference_price, reference, "seed {seed}");
                    let displacement = reference.zip(last_price_before(&trades, window.end)).map(
                        |(reference, last)| Price::from_units(last.units() - reference.units()),
                    );
                    assert_eq!(window.displacement, displacement, "seed {seed}");
                    // On its own boundary the window is that timeframe's bar.
                    if let Some(bar) = closed
                        .iter()
                        .find(|bar| bar.timeframe == timeframe && bar.end() == window.end)
                    {
                        let bar_sums = (
                            bar.buy_volume,
                            bar.sell_volume,
                            bar.delta,
                            bar.volume,
                            bar.trade_count,
                            bar.coverage,
                        );
                        assert_eq!(sums(window), bar_sums, "seed {seed} {bar}");
                        boundaries += 1;
                    }
                    checked += 1;
                }
            }
            assert!(checked > 1_000, "seed {seed}: {checked}");
            assert!(boundaries > 100, "seed {seed}: {boundaries}");
            assert!(
                trades.iter().any(|trade| (i128::from(trade.price.units())
                    * i128::from(trade.qty.units()))
                .abs()
                    >= threshold),
                "seed {seed}: the tape has large prints"
            );
        }
    }

    /// The empty minute of the golden tape: 00:10 UTC on day 1.
    const EMPTY_MINUTE: i64 = DAY + 10 * MINUTE;
    /// The trades gap of the golden tape: 00:20:30 to 00:22:10 UTC on day 1,
    /// across three minutes.
    const GAP_START: i64 = DAY + 20 * MINUTE + 30_000;
    const GAP_END: i64 = DAY + 22 * MINUTE + 10_000;

    /// The order-flow golden tape: 23:50 UTC on day 0 to 01:06 UTC on day 1.
    /// A partial first minute, bulk trades from an LCG on a random walk,
    /// prints at and one unit below the large threshold on both sides, a
    /// trade exactly at midnight, an empty minute, and a trades gap across
    /// minutes.
    fn golden_tape() -> Vec<MarketEvent> {
        let mut lcg = Lcg(0x6d69_6500_0000_0017);
        let mut events = Vec::new();
        let mut walk = 62_500 * SCALE;
        let mut trade_id = 0;
        let mut push = |events: &mut Vec<MarketEvent>, millis, price, units, aggressor| {
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: t(millis),
                trade_id,
                price: Price::from_units(price),
                qty: Qty::from_units(units),
                aggressor,
            }));
        };
        let side = |lcg: &mut Lcg| {
            if lcg.below(2) == 0 {
                Aggressor::Buy
            } else {
                Aggressor::Sell
            }
        };
        // 62 500 USDT: 1.6 BTC is exactly 100 000 USDT.
        let level = 62_500 * SCALE;
        let at_threshold = 160_000_000;
        for open in (DAY - 10 * MINUTE..DAY + 66 * MINUTE).step_by(60_000) {
            if open == EMPTY_MINUTE || open == GAP_START - 30_000 + MINUTE {
                continue;
            }
            let first = match open {
                DAY => 0,
                _ if open == DAY - 10 * MINUTE => 12_345,
                _ if open == GAP_END - 10_000 => 11_000,
                _ => 1_000,
            };
            for k in 0..1 + lcg.below(4) {
                walk += lcg.below(2 * SCALE as u64 + 1) - SCALE;
                let offset = if k == 0 {
                    first
                } else {
                    first + k * 7_000 + lcg.below(6_000)
                };
                let units = 1 + lcg.below(50_000_000);
                let aggressor = side(&mut lcg);
                push(&mut events, open + offset, walk, units, aggressor);
            }
            match open - DAY {
                // 23:55: at the threshold and one unit below, buying.
                -300_000 => {
                    push(
                        &mut events,
                        open + 40_000,
                        level,
                        at_threshold,
                        Aggressor::Buy,
                    );
                    push(
                        &mut events,
                        open + 41_000,
                        level,
                        at_threshold - 1,
                        Aggressor::Buy,
                    );
                }
                // 00:30: at the threshold and one unit below, selling.
                1_800_000 => {
                    push(
                        &mut events,
                        open + 40_000,
                        level,
                        at_threshold,
                        Aggressor::Sell,
                    );
                    push(
                        &mut events,
                        open + 41_000,
                        level,
                        at_threshold - 1,
                        Aggressor::Sell,
                    );
                }
                // 00:45 and 00:58: well above it.
                2_700_000 => push(
                    &mut events,
                    open + 40_000,
                    walk,
                    300_000_000,
                    Aggressor::Sell,
                ),
                3_480_000 => push(
                    &mut events,
                    open + 40_000,
                    walk,
                    250_000_000,
                    Aggressor::Buy,
                ),
                _ => {}
            }
            if open == GAP_START - 30_000 {
                events.push(gap(
                    Stream::Trades,
                    GAP_START,
                    GAP_END,
                    GapReason::Disconnected,
                ));
            }
        }
        // Closes 01:05.
        push(
            &mut events,
            DAY + 66 * MINUTE + 500,
            walk,
            1_000_000,
            Aggressor::Buy,
        );
        events
    }

    /// One line per event of the golden tape that closed a minute, from the
    /// first ready value of `pick`: the event time, then the value.
    fn golden_lines<T: fmt::Display>(pick: impl Fn(&OrderFlow) -> FeatureValue<T>) -> Vec<String> {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        for event in golden_tape() {
            engine.apply(&event).unwrap();
            let closed_a_minute = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::M1);
            if let (true, FeatureValue::Ready(value)) =
                (closed_a_minute, pick(&engine.state().flow))
            {
                lines.push(format!("{} {value}", event.time()));
            }
        }
        lines
    }

    #[test]
    fn golden_flow_cvd_continuous_v1() {
        assert_eq!(golden_lines(|flow| flow.cvd), GOLDEN_CVD_CONTINUOUS);
    }

    #[test]
    fn golden_flow_cvd_utc_day_v1() {
        assert_eq!(golden_lines(|flow| flow.cvd_utc_day), GOLDEN_CVD_UTC_DAY);
    }

    /// The golden lines of the window of length `timeframe`.
    fn golden_window_lines(timeframe: Timeframe) -> Vec<String> {
        golden_lines(|flow| *flow.windows.get(timeframe).unwrap())
    }

    #[test]
    fn golden_flow_window_5m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M5), GOLDEN_WINDOW_5M);
    }

    #[test]
    fn golden_flow_window_15m_v1() {
        assert_eq!(golden_window_lines(Timeframe::M15), GOLDEN_WINDOW_15M);
    }

    #[test]
    fn golden_flow_window_1h_v1() {
        // Every 5th minute from the first ready value, and the last.
        let lines = golden_window_lines(Timeframe::H1);
        let last = lines.len() - 1;
        let pinned: Vec<&String> = lines
            .iter()
            .enumerate()
            .filter(|(index, _)| index % 5 == 0 || *index == last)
            .map(|(_, line)| line)
            .collect();
        assert_eq!(lines.len(), 17);
        assert_eq!(pinned, GOLDEN_WINDOW_1H);
    }

    const GOLDEN_CVD_CONTINUOUS: [&str; 74] = [
        "85861000ms cvd=-0.08869658 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "85921000ms cvd=0.23590607 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "85981000ms cvd=0.08223059 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86041000ms cvd=0.73269223 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86101000ms cvd=1.66367150 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86161000ms cvd=5.06425022 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86221000ms cvd=5.47278507 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86281000ms cvd=5.99491879 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86341000ms cvd=5.28282172 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86400000ms cvd=5.23709407 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86461000ms cvd=5.60336764 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86521000ms cvd=5.79025422 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86581000ms cvd=6.40268313 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86641000ms cvd=6.85183244 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86701000ms cvd=7.12943873 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86761000ms cvd=7.02992223 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86821000ms cvd=6.94007784 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86881000ms cvd=7.91524242 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "86941000ms cvd=7.21944032 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87061000ms cvd=6.85067456 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87121000ms cvd=6.76821884 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87181000ms cvd=7.34868255 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87241000ms cvd=6.69608230 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87301000ms cvd=6.36567246 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87361000ms cvd=6.49457493 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87421000ms cvd=7.22383257 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87481000ms cvd=7.19524640 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87541000ms cvd=6.61737186 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87601000ms cvd=7.06508990 anchor=85812345ms gaps=0 flow.cvd.continuous@1",
        "87730000ms cvd=7.06508990 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "87781000ms cvd=7.68715547 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "87841000ms cvd=8.19952006 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "87901000ms cvd=7.30458249 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "87961000ms cvd=8.39582793 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88021000ms cvd=8.99244388 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88081000ms cvd=9.11373048 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88141000ms cvd=10.07567268 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88201000ms cvd=9.46419841 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88261000ms cvd=6.55650309 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88321000ms cvd=6.82714874 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88381000ms cvd=5.92277574 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88441000ms cvd=6.42187605 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88501000ms cvd=6.09127547 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88561000ms cvd=6.36741173 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88621000ms cvd=5.86497403 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88681000ms cvd=6.67829164 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88741000ms cvd=6.66419985 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88801000ms cvd=6.72502665 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88861000ms cvd=7.29210703 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88921000ms cvd=6.85332228 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "88981000ms cvd=7.31029064 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89041000ms cvd=7.57631844 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89101000ms cvd=7.64328190 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89161000ms cvd=5.04483185 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89221000ms cvd=4.59345917 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89281000ms cvd=4.37124701 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89341000ms cvd=4.29045583 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89401000ms cvd=5.18750553 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89461000ms cvd=5.68005108 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89521000ms cvd=5.83782983 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89581000ms cvd=6.52475148 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89641000ms cvd=6.18312694 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89701000ms cvd=6.37546663 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89761000ms cvd=6.89446078 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89821000ms cvd=7.06689129 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89881000ms cvd=6.61537891 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "89941000ms cvd=9.47695344 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90001000ms cvd=9.72453880 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90061000ms cvd=9.65224017 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90121000ms cvd=10.51323923 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90181000ms cvd=10.15396627 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90241000ms cvd=10.45829859 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90301000ms cvd=11.07137159 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
        "90360500ms cvd=10.48330959 anchor=85812345ms gaps=1 flow.cvd.continuous@1",
    ];
    const GOLDEN_CVD_UTC_DAY: [&str; 74] = [
        "85861000ms day=0ms cvd=-0.08869658 partial_start flow.cvd.utc_day@1",
        "85921000ms day=0ms cvd=0.23590607 partial_start flow.cvd.utc_day@1",
        "85981000ms day=0ms cvd=0.08223059 partial_start flow.cvd.utc_day@1",
        "86041000ms day=0ms cvd=0.73269223 partial_start flow.cvd.utc_day@1",
        "86101000ms day=0ms cvd=1.66367150 partial_start flow.cvd.utc_day@1",
        "86161000ms day=0ms cvd=5.06425022 partial_start flow.cvd.utc_day@1",
        "86221000ms day=0ms cvd=5.47278507 partial_start flow.cvd.utc_day@1",
        "86281000ms day=0ms cvd=5.99491879 partial_start flow.cvd.utc_day@1",
        "86341000ms day=0ms cvd=5.28282172 partial_start flow.cvd.utc_day@1",
        "86400000ms day=86400000ms cvd=-0.04572765 complete flow.cvd.utc_day@1",
        "86461000ms day=86400000ms cvd=0.32054592 complete flow.cvd.utc_day@1",
        "86521000ms day=86400000ms cvd=0.50743250 complete flow.cvd.utc_day@1",
        "86581000ms day=86400000ms cvd=1.11986141 complete flow.cvd.utc_day@1",
        "86641000ms day=86400000ms cvd=1.56901072 complete flow.cvd.utc_day@1",
        "86701000ms day=86400000ms cvd=1.84661701 complete flow.cvd.utc_day@1",
        "86761000ms day=86400000ms cvd=1.74710051 complete flow.cvd.utc_day@1",
        "86821000ms day=86400000ms cvd=1.65725612 complete flow.cvd.utc_day@1",
        "86881000ms day=86400000ms cvd=2.63242070 complete flow.cvd.utc_day@1",
        "86941000ms day=86400000ms cvd=1.93661860 complete flow.cvd.utc_day@1",
        "87061000ms day=86400000ms cvd=1.56785284 complete flow.cvd.utc_day@1",
        "87121000ms day=86400000ms cvd=1.48539712 complete flow.cvd.utc_day@1",
        "87181000ms day=86400000ms cvd=2.06586083 complete flow.cvd.utc_day@1",
        "87241000ms day=86400000ms cvd=1.41326058 complete flow.cvd.utc_day@1",
        "87301000ms day=86400000ms cvd=1.08285074 complete flow.cvd.utc_day@1",
        "87361000ms day=86400000ms cvd=1.21175321 complete flow.cvd.utc_day@1",
        "87421000ms day=86400000ms cvd=1.94101085 complete flow.cvd.utc_day@1",
        "87481000ms day=86400000ms cvd=1.91242468 complete flow.cvd.utc_day@1",
        "87541000ms day=86400000ms cvd=1.33455014 complete flow.cvd.utc_day@1",
        "87601000ms day=86400000ms cvd=1.78226818 complete flow.cvd.utc_day@1",
        "87730000ms day=86400000ms cvd=1.78226818 feed_gap flow.cvd.utc_day@1",
        "87781000ms day=86400000ms cvd=2.40433375 feed_gap flow.cvd.utc_day@1",
        "87841000ms day=86400000ms cvd=2.91669834 feed_gap flow.cvd.utc_day@1",
        "87901000ms day=86400000ms cvd=2.02176077 feed_gap flow.cvd.utc_day@1",
        "87961000ms day=86400000ms cvd=3.11300621 feed_gap flow.cvd.utc_day@1",
        "88021000ms day=86400000ms cvd=3.70962216 feed_gap flow.cvd.utc_day@1",
        "88081000ms day=86400000ms cvd=3.83090876 feed_gap flow.cvd.utc_day@1",
        "88141000ms day=86400000ms cvd=4.79285096 feed_gap flow.cvd.utc_day@1",
        "88201000ms day=86400000ms cvd=4.18137669 feed_gap flow.cvd.utc_day@1",
        "88261000ms day=86400000ms cvd=1.27368137 feed_gap flow.cvd.utc_day@1",
        "88321000ms day=86400000ms cvd=1.54432702 feed_gap flow.cvd.utc_day@1",
        "88381000ms day=86400000ms cvd=0.63995402 feed_gap flow.cvd.utc_day@1",
        "88441000ms day=86400000ms cvd=1.13905433 feed_gap flow.cvd.utc_day@1",
        "88501000ms day=86400000ms cvd=0.80845375 feed_gap flow.cvd.utc_day@1",
        "88561000ms day=86400000ms cvd=1.08459001 feed_gap flow.cvd.utc_day@1",
        "88621000ms day=86400000ms cvd=0.58215231 feed_gap flow.cvd.utc_day@1",
        "88681000ms day=86400000ms cvd=1.39546992 feed_gap flow.cvd.utc_day@1",
        "88741000ms day=86400000ms cvd=1.38137813 feed_gap flow.cvd.utc_day@1",
        "88801000ms day=86400000ms cvd=1.44220493 feed_gap flow.cvd.utc_day@1",
        "88861000ms day=86400000ms cvd=2.00928531 feed_gap flow.cvd.utc_day@1",
        "88921000ms day=86400000ms cvd=1.57050056 feed_gap flow.cvd.utc_day@1",
        "88981000ms day=86400000ms cvd=2.02746892 feed_gap flow.cvd.utc_day@1",
        "89041000ms day=86400000ms cvd=2.29349672 feed_gap flow.cvd.utc_day@1",
        "89101000ms day=86400000ms cvd=2.36046018 feed_gap flow.cvd.utc_day@1",
        "89161000ms day=86400000ms cvd=-0.23798987 feed_gap flow.cvd.utc_day@1",
        "89221000ms day=86400000ms cvd=-0.68936255 feed_gap flow.cvd.utc_day@1",
        "89281000ms day=86400000ms cvd=-0.91157471 feed_gap flow.cvd.utc_day@1",
        "89341000ms day=86400000ms cvd=-0.99236589 feed_gap flow.cvd.utc_day@1",
        "89401000ms day=86400000ms cvd=-0.09531619 feed_gap flow.cvd.utc_day@1",
        "89461000ms day=86400000ms cvd=0.39722936 feed_gap flow.cvd.utc_day@1",
        "89521000ms day=86400000ms cvd=0.55500811 feed_gap flow.cvd.utc_day@1",
        "89581000ms day=86400000ms cvd=1.24192976 feed_gap flow.cvd.utc_day@1",
        "89641000ms day=86400000ms cvd=0.90030522 feed_gap flow.cvd.utc_day@1",
        "89701000ms day=86400000ms cvd=1.09264491 feed_gap flow.cvd.utc_day@1",
        "89761000ms day=86400000ms cvd=1.61163906 feed_gap flow.cvd.utc_day@1",
        "89821000ms day=86400000ms cvd=1.78406957 feed_gap flow.cvd.utc_day@1",
        "89881000ms day=86400000ms cvd=1.33255719 feed_gap flow.cvd.utc_day@1",
        "89941000ms day=86400000ms cvd=4.19413172 feed_gap flow.cvd.utc_day@1",
        "90001000ms day=86400000ms cvd=4.44171708 feed_gap flow.cvd.utc_day@1",
        "90061000ms day=86400000ms cvd=4.36941845 feed_gap flow.cvd.utc_day@1",
        "90121000ms day=86400000ms cvd=5.23041751 feed_gap flow.cvd.utc_day@1",
        "90181000ms day=86400000ms cvd=4.87114455 feed_gap flow.cvd.utc_day@1",
        "90241000ms day=86400000ms cvd=5.17547687 feed_gap flow.cvd.utc_day@1",
        "90301000ms day=86400000ms cvd=5.78854987 feed_gap flow.cvd.utc_day@1",
        "90360500ms day=86400000ms cvd=5.20048787 feed_gap flow.cvd.utc_day@1",
    ];
    const GOLDEN_WINDOW_5M: [&str; 70] = [
        "86101000ms 5m end=86100000ms buy=2.97423580 sell=1.77243759 delta=1.20179821 vol=4.74667339 trades=15 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=- disp=- imbalance=0.2531874665174719 trades_per_min=3 vol_per_min=0.9493346779999999 large_share=0 response=- partial_start flow.window.5m@1",
        "86161000ms 5m end=86160000ms buy=6.60451125 sell=2.08388104 delta=4.52063021 vol=8.68839229 trades=18 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62499.54754731 disp=0.45245269 imbalance=0.5203068714108442 trades_per_min=3.6 vol_per_min=1.737678458 large_share=0.18415374750533967 response=0.10008619793743315 complete flow.window.5m@1",
        "86221000ms 5m end=86220000ms buy=6.18761659 sell=1.63127203 delta=4.55634456 vol=7.81888862 trades=16 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.94677118 disp=-0.17760784 imbalance=0.5827355755324726 trades_per_min=3.2 vol_per_min=1.563777724 large_share=0.20463266299859378 response=-0.03898033558726296 complete flow.window.5m@1",
        "86281000ms 5m end=86280000ms buy=7.32328818 sell=0.83651621 delta=6.48677197 vol=8.15980439 trades=16 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.44862542 disp=-1.51839131 imbalance=0.7949666021344416 trades_per_min=3.2 vol_per_min=1.631960878 large_share=0.1960831318408602 response=-0.234075024838587 complete flow.window.5m@1",
        "86341000ms 5m end=86340000ms buy=6.30735786 sell=0.92857619 delta=5.37878167 vol=7.23593405 trades=14 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.50417283 disp=-2.89691228 imbalance=0.7433431030234445 trades_per_min=2.8 vol_per_min=1.44718681 large_share=0.22111865433599412 response=-0.5385814962814804 complete flow.window.5m@1",
        "86400000ms 5m end=86400000ms buy=5.36738759 sell=1.28636408 delta=4.08102351 vol=6.65375167 trades=12 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.34732620 disp=-1.82437630 imbalance=0.613341722445136 trades_per_min=2.4 vol_per_min=1.330750334 large_share=0.24046584233282634 response=-0.44703890960922205 complete flow.window.5m@1",
        "86461000ms 5m end=86460000ms buy=1.31537917 sell=0.96282774 delta=0.35255143 vol=2.27820691 trades=8 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62500.00000000 disp=-4.42765411 imbalance=0.1547495218509367 trades_per_min=1.6 vol_per_min=0.45564138200000004 large_share=0 response=-12.558888528689275 complete flow.window.5m@1",
        "86521000ms 5m end=86520000ms buy=1.50194516 sell=0.96282774 delta=0.53911742 vol=2.46477290 trades=8 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62497.76916334 disp=-3.03931114 imbalance=0.21872904396181897 trades_per_min=1.6 vol_per_min=0.49295458 large_share=0 response=-5.637568045937006 complete flow.window.5m@1",
        "86581000ms 5m end=86580000ms buy=0.97399392 sell=0.96282774 delta=0.01116618 vol=1.93682166 trades=7 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.93023411 disp=-2.08984700 imbalance=0.005765208140020491 trades_per_min=1.4 vol_per_min=0.387364332 large_share=0 response=-187.15863437630415 complete flow.window.5m@1",
        "86641000ms 5m end=86640000ms buy=1.46970839 sell=0.43019680 delta=1.03951159 vol=1.89990519 trades=8 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.60726055 disp=-1.91234608 imbalance=0.547138665377297 trades_per_min=1.6 vol_per_min=0.379981038 large_share=0 response=-1.8396582572013458 complete flow.window.5m@1",
        "86701000ms 5m end=86700000ms buy=1.61473837 sell=0.04572765 delta=1.56901072 vol=1.66046602 trades=8 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.52294990 disp=-3.29410629 imbalance=0.9449219081279363 trades_per_min=1.6 vol_per_min=0.33209320400000003 large_share=0 response=-2.0994797855810696 complete flow.window.5m@1",
        "86761000ms 5m end=86760000ms buy=2.42625440 sell=0.49006033 delta=1.93619407 vol=2.91631473 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.57234589 disp=-6.63118217 imbalance=0.6639180778680908 trades_per_min=2.2 vol_per_min=0.583262946 large_share=0 response=-3.4248540850039895 complete flow.window.5m@1",
        "86821000ms 5m end=86820000ms buy=2.35739252 sell=0.99711941 delta=1.36027311 vol=3.35451193 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.72985220 disp=-5.04615954 imbalance=0.4055055216333662 trades_per_min=2.6 vol_per_min=0.6709023860000001 large_share=0 response=-3.7096664654350184 complete flow.window.5m@1",
        "86881000ms 5m end=86880000ms buy=2.30381394 sell=1.02068232 delta=1.28313162 vol=3.32449626 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.84038711 disp=-3.29157182 imbalance=0.3859627202588581 trades_per_min=2.8 vol_per_min=0.664899252 large_share=0 response=-2.565264364695494 complete flow.window.5m@1",
        "86941000ms 5m end=86940000ms buy=2.22912228 sell=1.45738001 delta=0.77174227 vol=3.68650229 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.69491447 disp=-2.30941871 imbalance=0.20934268021301025 trades_per_min=2.8 vol_per_min=0.737300458 large_share=0 response=-2.992474041884475 complete flow.window.5m@1",
        "87061000ms 5m end=87060000ms buy=1.52092118 sell=1.62634373 delta=-0.10542255 vol=3.14726491 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62488.94116372 disp=1.05088494 imbalance=-0.03349656066924471 trades_per_min=2.6 vol_per_min=0.629452982 large_share=0 response=-9.968312661759747 complete flow.window.5m@1",
        "87121000ms 5m end=87120000ms buy=1.25225920 sell=1.33647568 delta=-0.08421648 vol=2.58873488 trades=12 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.68369266 disp=0.49587266 imbalance=-0.032531906086883645 trades_per_min=2.4 vol_per_min=0.517746976 large_share=0 response=-5.888071550841356 complete flow.window.5m@1",
        "87181000ms 5m end=87180000ms buy=0.82030022 sell=1.42411820 delta=-0.60381798 vol=2.24441842 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.54881529 disp=0.22782903 imbalance=-0.26903093229826547 trades_per_min=2.2 vol_per_min=0.448883684 large_share=0 response=-0.3773140872684845 complete flow.window.5m@1",
        "87241000ms 5m end=87240000ms buy=1.06077155 sell=1.35749293 delta=-0.29672138 vol=2.41826448 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.38549576 disp=1.84500488 imbalance=-0.12270013576017127 trades_per_min=2.2 vol_per_min=0.483652896 large_share=0 response=-6.217970811540442 complete flow.window.5m@1",
        "87301000ms 5m end=87300000ms buy=1.18036368 sell=1.92523851 delta=-0.74487483 vol=3.10560219 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.99204866 disp=1.97552780 imbalance=-0.23984875860742486 trades_per_min=2.2 vol_per_min=0.6211204379999999 large_share=0 response=-2.6521607663934623 complete flow.window.5m@1",
        "87361000ms 5m end=87360000ms buy=1.69878062 sell=2.22480819 delta=-0.52602757 vol=3.92358881 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.99204866 disp=1.87693950 imbalance=-0.13406796570000412 trades_per_min=2.8 vol_per_min=0.784717762 large_share=0 response=-3.56813902358768 complete flow.window.5m@1",
        "87421000ms 5m end=87420000ms buy=2.37919643 sell=2.26869361 delta=0.11050282 vol=4.64789004 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.17956532 disp=-0.59429280 imbalance=0.023774835258365965 trades_per_min=3.2 vol_per_min=0.929578008 large_share=0 response=-5.3780781341145865 complete flow.window.5m@1",
        "87481000ms 5m end=87480000ms buy=2.49091912 sell=2.62276665 delta=-0.13184753 vol=5.11368577 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.77664432 disp=-1.66726340 imbalance=-0.0257832678678651 trades_per_min=3.2 vol_per_min=1.022737154 large_share=0 response=12.64538971643989 complete flow.window.5m@1",
        "87541000ms 5m end=87540000ms buy=2.26611728 sell=3.08499201 delta=-0.81887473 vol=5.35110929 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.23050064 disp=-1.39353427 imbalance=-0.1530289675694514 trades_per_min=3.2 vol_per_min=1.070221858 large_share=0 response=1.7017673386990462 complete flow.window.5m@1",
        "87601000ms 5m end=87600000ms buy=2.78816417 sell=1.89138537 delta=0.89677880 vol=4.67954954 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.96757646 disp=-1.24338634 imbalance=0.19163784726168323 trades_per_min=3.2 vol_per_min=0.935909908 large_share=0 response=-1.3865028254459182 complete flow.window.5m@1",
        "87730000ms 5m end=87720000ms buy=1.56058171 sell=1.48541890 delta=0.07516281 vol=3.04600061 trades=10 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.58527252 disp=1.52905320 imbalance=0.024675901164707907 trades_per_min=2 vol_per_min=0.6092001220000001 large_share=0 response=20.343214948988734 feed_gap flow.window.5m@1",
        "87781000ms 5m end=87780000ms buy=2.22437108 sell=1.36091619 delta=0.86345489 vol=3.58528727 trades=12 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.10938092 disp=1.35799053 imbalance=0.24083283290155993 trades_per_min=2.4 vol_per_min=0.717057454 large_share=0 response=1.5727405632041762 feed_gap flow.window.5m@1",
        "87841000ms 5m end=87840000ms buy=2.22811834 sell=0.52861841 delta=1.69949993 vol=2.75673675 trades=12 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.83696637 disp=-0.06900943 imbalance=0.6164897428091384 trades_per_min=2.4 vol_per_min=0.55134735 large_share=0 response=-0.04060572688579046 feed_gap flow.window.5m@1",
        "87901000ms 5m end=87900000ms buy=1.47520592 sell=1.06308813 delta=0.41211779 vol=2.53829405 trades=12 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.72419012 disp=-1.53802030 imbalance=0.16236014499580928 trades_per_min=2.4 vol_per_min=0.50765881 large_share=0 response=-3.7319920113130762 feed_gap flow.window.5m@1",
        "87961000ms 5m end=87960000ms buy=2.07113270 sell=1.23571333 delta=0.83541937 vol=3.30684603 trades=15 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.11432572 disp=-2.96030841 imbalance=0.2526332833222356 trades_per_min=3 vol_per_min=0.661369206 large_share=0 response=-3.5434998472683246 feed_gap flow.window.5m@1",
        "88021000ms 5m end=88020000ms buy=2.96594093 sell=1.23571333 delta=1.73022760 vol=4.20165426 trades=17 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.11432572 disp=-2.14628008 imbalance=0.41179675740383265 trades_per_min=3.4 vol_per_min=0.8403308519999999 large_share=0 response=-1.2404611277730166 feed_gap flow.window.5m@1",
        "88081000ms 5m end=88080000ms buy=2.26537246 sell=0.89493757 delta=1.37043489 vol=3.16031003 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.46737145 disp=-2.01718567 imbalance=0.43363938252602385 trades_per_min=2.8 vol_per_min=0.6320620060000001 large_share=0 response=-1.4719310524850984 complete flow.window.5m@1",
        "88141000ms 5m end=88140000ms buy=2.50888605 sell=0.89493757 delta=1.61394848 vol=3.40382362 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.76795694 disp=-1.01412527 imbalance=0.4741574946824066 trades_per_min=2.8 vol_per_min=0.680764724 large_share=0 response=-0.628350460108863 complete flow.window.5m@1",
        "88201000ms 5m end=88200000ms buy=2.77109019 sell=0.60225933 delta=2.16883086 vol=3.37334952 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.18616982 disp=1.16537837 imbalance=0.6429309643549773 trades_per_min=2.6 vol_per_min=0.674669904 large_share=0 response=0.5373302231599564 complete flow.window.5m@1",
        "88261000ms 5m end=88260000ms buy=2.52639302 sell=4.08858828 delta=-1.56219526 vol=6.61498130 trades=15 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.15401731 disp=11.84598269 imbalance=-0.23616019292450607 trades_per_min=3 vol_per_min=1.32299626 large_share=0.24187521134791418 response=-7.582907843415169 complete flow.window.5m@1",
        "88321000ms 5m end=88320000ms buy=1.84977387 sell=4.08858828 delta=-2.23881441 vol=5.93836215 trades=14 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.96804564 disp=2.76825481 imbalance=-0.37700873632302806 trades_per_min=2.8 vol_per_min=1.18767243 large_share=0.269434561177782 response=-1.2364824871749864 complete flow.window.5m@1",
        "88381000ms 5m end=88380000ms buy=1.92329314 sell=4.57536364 delta=-2.65207050 vol=6.49865678 trades=16 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.45018578 disp=3.10727209 imbalance=-0.4080951787086069 trades_per_min=3.2 vol_per_min=1.2997313560000001 large_share=0.2462047241707078 response=-1.1716400789496357 complete flow.window.5m@1",
        "88441000ms 5m end=88440000ms buy=1.23933999 sell=4.99296128 delta=-3.75362129 vol=6.23230127 trades=14 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62489.75383167 disp=1.44468642 imbalance=-0.6022849550082806 trades_per_min=2.8 vol_per_min=1.246460254 large_share=0.2567269986933735 response=-0.38487804399681463 complete flow.window.5m@1",
        "88501000ms 5m end=88500000ms buy=1.59125571 sell=5.62108781 delta=-4.02983210 vol=7.21234352 trades=15 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.35154819 disp=1.28477913 imbalance=-0.5587410096073738 trades_per_min=3 vol_per_min=1.442468704 large_share=0.22184190139628845 response=-0.31881703706712744 complete flow.window.5m@1",
        "88561000ms 5m end=88560000ms buy=2.24902296 sell=1.89672989 delta=0.35229307 vol=4.14575285 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62500.00000000 disp=-6.32840501 imbalance=0.08497686252570508 trades_per_min=2.6 vol_per_min=0.82915057 large_share=0 response=-17.96346720643696 complete flow.window.5m@1",
        "88621000ms 5m end=88620000ms buy=2.09031125 sell=2.50808510 delta=-0.41777385 vol=4.59839635 trades=15 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.73630045 disp=1.75166564 imbalance=-0.09085207498479334 trades_per_min=3 vol_per_min=0.9196792699999999 large_share=0 response=-4.192856111027533 complete flow.window.5m@1",
        "88681000ms 5m end=88680000ms buy=2.82052483 sell=2.29506495 delta=0.52545988 vol=5.11558978 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.55745787 disp=-0.39396521 imbalance=0.10271736057772796 trades_per_min=3.2 vol_per_min=1.0231179559999999 large_share=0 response=-0.7497531685958593 complete flow.window.5m@1",
        "88741000ms 5m end=88740000ms buy=3.15810060 sell=2.06500893 delta=1.09309167 vol=5.22310953 trades=17 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.19851809 disp=-1.29143158 imbalance=0.20927986742793808 trades_per_min=3.4 vol_per_min=1.0446219060000002 large_share=0 response=-1.181448560485325 complete flow.window.5m@1",
        "88801000ms 5m end=88800000ms buy=2.48385325 sell=1.51359549 delta=0.97025776 vol=3.99744874 trades=15 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.63632732 disp=-1.84572194 imbalance=0.24271924997842498 trades_per_min=3 vol_per_min=0.799489748 large_share=0 response=-1.9023006216409957 complete flow.window.5m@1",
        "88861000ms 5m end=88860000ms buy=1.74558930 sell=1.44043682 delta=0.30515248 vol=3.18602612 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.67159499 disp=-3.51061250 imbalance=0.09577839870314685 trades_per_min=2.6 vol_per_min=0.637205224 large_share=0 response=-11.504453445700326 complete flow.window.5m@1",
        "88921000ms 5m end=88920000ms buy=2.06158717 sell=1.24901497 delta=0.81257220 vol=3.31060214 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.48796609 disp=-2.86968257 imbalance=0.24544544032705784 trades_per_min=2.6 vol_per_min=0.6621204279999999 large_share=0 response=-3.5316031855384664 complete flow.window.5m@1",
        "88981000ms 5m end=88980000ms buy=1.06072794 sell=1.09031612 delta=-0.02958818 vol=2.15104406 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.16349266 disp=0.64378034 imbalance=-0.013755264501648563 trades_per_min=2.2 vol_per_min=0.43020881199999994 large_share=0 response=-21.758024319170694 complete flow.window.5m@1",
        "89041000ms 5m end=89040000ms buy=1.76322214 sell=1.00713048 delta=0.75609166 vol=2.77035262 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.90708651 disp=4.22950316 imbalance=0.272922535038157 trades_per_min=2.6 vol_per_min=0.554070524 large_share=0 response=5.593902675768174 complete flow.window.5m@1",
        "89101000ms 5m end=89100000ms buy=2.15578958 sell=0.85110355 delta=1.30468603 vol=3.00689313 trades=15 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.79060538 disp=5.15211155 imbalance=0.43389837070797393 trades_per_min=3 vol_per_min=0.601378626 large_share=0 response=3.948928272037986 complete flow.window.5m@1",
        "89161000ms 5m end=89160000ms buy=2.21607849 sell=4.39475652 delta=-2.17867803 vol=6.61083501 trades=17 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62490.16098249 disp=4.12027251 imbalance=-0.3295617008599342 trades_per_min=3.4 vol_per_min=1.322167002 large_share=0.45380046476156116 response=-1.8911800886889194 complete flow.window.5m@1",
        "89221000ms 5m end=89220000ms buy=2.52904925 sell=4.42844303 delta=-1.89939378 vol=6.95749228 trades=17 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62490.61828352 disp=3.84902899 imbalance=-0.2729997682440835 trades_per_min=3.4 vol_per_min=1.391498456 large_share=0.43118984244133435 response=-2.026451297529257 complete flow.window.5m@1",
        "89281000ms 5m end=89280000ms buy=2.52904925 sell=4.77183516 delta=-2.24278591 vol=7.30088441 trades=16 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.80727300 disp=1.82578605 imbalance=-0.3071937294238028 trades_per_min=3.2 vol_per_min=1.460176882 large_share=0.41090912162517035 response=-0.8140705904470391 complete flow.window.5m@1",
        "89341000ms 5m end=89340000ms buy=1.48897928 sell=4.88969134 delta=-3.40071206 vol=6.37867062 trades=13 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62494.13658967 disp=-0.09021409 imbalance=-0.5331380569075379 trades_per_min=2.6 vol_per_min=1.275734124 large_share=0.47031743426187445 response=0.02652800013888856 complete flow.window.5m@1",
        "89401000ms 5m end=89400000ms buy=1.54477099 sell=4.85687803 delta=-3.31210704 vol=6.40164902 trades=13 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62494.94271693 disp=-1.39866345 imbalance=-0.5173834163123177 trades_per_min=2.6 vol_per_min=1.280329804 large_share=0.4686292532794933 response=0.4222881184419692 complete flow.window.5m@1",
        "89461000ms 5m end=89460000ms buy=1.67046664 sell=1.29710784 delta=0.37335880 vol=2.96757448 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.28125500 disp=0.77367359 imbalance=0.1258127816222493 trades_per_min=2.2 vol_per_min=0.5935148960000001 large_share=0 response=2.072198619665587 complete flow.window.5m@1",
        "89521000ms 5m end=89520000ms buy=1.47163139 sell=0.84348797 delta=0.62814342 vol=2.31511936 trades=9 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.46731251 disp=0.02975315 imbalance=0.2713222613282453 trades_per_min=1.8 vol_per_min=0.46302387200000006 large_share=0 response=0.04736680995559899 complete flow.window.5m@1",
        "89581000ms 5m end=89580000ms buy=2.58092985 sell=0.38503948 delta=2.19589037 vol=2.96596933 trades=12 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.63305905 disp=0.11017151 imbalance=0.7403617926150302 trades_per_min=2.4 vol_per_min=0.593193866 large_share=0 response=0.050171680474194166 complete flow.window.5m@1",
        "89641000ms 5m end=89640000ms buy=2.58092985 sell=0.64157306 delta=1.93935679 vol=3.22250291 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.04637558 disp=-1.57530687 imbalance=0.6018169243484096 trades_per_min=2.6 vol_per_min=0.644500582 large_share=0 response=-0.8122831642546805 complete flow.window.5m@1",
        "89701000ms 5m end=89700000ms buy=2.05562668 sell=0.76931427 delta=1.28631241 vol=2.82494095 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.54405348 disp=0.06734756 imbalance=0.4553413443916412 trades_per_min=2.2 vol_per_min=0.56498819 large_share=0 response=0.05235707863535267 complete flow.window.5m@1",
        "89761000ms 5m end=89760000ms buy=2.42170689 sell=0.76931427 delta=1.65239262 vol=3.19102116 trades=11 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.05492859 disp=-1.13776522 imbalance=0.5178256542805251 trades_per_min=2.2 vol_per_min=0.638204232 large_share=0 response=-0.6885562221888888 complete flow.window.5m@1",
        "89821000ms 5m end=89820000ms buy=2.42896655 sell=0.98000667 delta=1.44895988 vol=3.40897322 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.49706566 disp=0.62357287 imbalance=0.4250429048545004 trades_per_min=2.6 vol_per_min=0.681794644 large_share=0 response=0.4303589620438628 complete flow.window.5m@1",
        "89881000ms 5m end=89880000ms buy=1.59891133 sell=1.40527967 delta=0.19363166 vol=3.00419100 trades=13 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.74323056 disp=1.20012419 imbalance=0.06445384464569663 trades_per_min=2.6 vol_per_min=0.6008382 large_share=0 response=6.197975010904725 complete flow.window.5m@1",
        "89941000ms 5m end=89940000ms buy=4.73604598 sell=1.29413622 delta=3.44190976 vol=6.03018220 trades=15 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62492.47106871 disp=2.57555141 imbalance=0.5707803920087191 trades_per_min=3 vol_per_min=1.2060364399999999 large_share=0.41458117136162154 response=0.7482913817008381 complete flow.window.5m@1",
        "90001000ms 5m end=90000000ms buy=4.73604598 sell=1.27912781 delta=3.45691817 vol=6.01517379 trades=14 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62493.61140104 disp=1.37615214 imbalance=0.5746996330757719 trades_per_min=2.8 vol_per_min=1.203034758 large_share=0.4156155893876509 response=0.3980864088547401 complete flow.window.5m@1",
        "90061000ms 5m end=90060000ms buy=4.69735454 sell=1.65644577 delta=3.04090877 vol=6.35380031 trades=16 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62493.91716337 disp=-1.13483047 imbalance=0.4785968430915324 trades_per_min=3.2 vol_per_min=1.270760062 large_share=0.3934653086382565 response=-0.3731879368416567 complete flow.window.5m@1",
        "90121000ms 5m end=90120000ms buy=4.95092504 sell=1.67904888 delta=3.27187616 vol=6.62997392 trades=17 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62495.12063853 disp=-2.58195928 imbalance=0.4934975913148087 trades_per_min=3.4 vol_per_min=1.3259947840000001 large_share=0.3770753897626192 response=-0.789137227003115 complete flow.window.5m@1",
        "90181000ms 5m end=90180000ms buy=4.78403391 sell=1.73680433 delta=3.04722958 vol=6.52083824 trades=15 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62494.94335475 disp=-1.11785669 imbalance=0.46730642102233777 trades_per_min=3 vol_per_min=1.304167648 large_share=0.38338629298677407 response=-0.36684360684107037 complete flow.window.5m@1",
        "90241000ms 5m end=90240000ms buy=2.13839504 sell=1.36920204 delta=0.76919300 vol=3.50759708 trades=14 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.04662012 disp=-0.81875922 imbalance=0.21929343150211542 trades_per_min=2.8 vol_per_min=0.701519416 large_share=0 response=-1.0644392499671733 complete flow.window.5m@1",
        "90301000ms 5m end=90300000ms buy=2.72679321 sell=1.56900517 delta=1.15778804 vol=4.29579838 trades=17 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.98755318 disp=-1.38680405 imbalance=0.26951638265667394 trades_per_min=3.4 vol_per_min=0.859159676 large_share=0 response=-1.1978047812620347 complete flow.window.5m@1",
        "90360500ms 5m end=90360000ms buy=2.37931708 sell=1.78974921 delta=0.58956787 vol=4.16906629 trades=16 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.78233290 disp=1.72233947 imbalance=0.14141484663224196 trades_per_min=3.2 vol_per_min=0.833813258 large_share=0 response=2.9213591134130157 complete flow.window.5m@1",
    ];
    const GOLDEN_WINDOW_15M: [&str; 60] = [
        "86701000ms 15m end=86700000ms buy=9.95636176 sell=3.10452932 delta=6.85183244 vol=13.06089108 trades=35 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=- disp=- imbalance=0.5246068126616672 trades_per_min=2.3333333333333335 vol_per_min=0.870726072 large_share=0.12250312709904324 response=- partial_start flow.window.15m@1",
        "86761000ms 15m end=86760000ms buy=10.34614482 sell=3.53676911 delta=6.80937571 vol=13.88291393 trades=37 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62499.54754731 disp=-10.60638359 imbalance=0.49048605676978363 trades_per_min=2.466666666666667 vol_per_min=0.9255275953333334 large_share=0.11524958002818937 response=-1.5576146832996531 complete flow.window.15m@1",
        "86821000ms 15m end=86820000ms buy=10.04695427 sell=3.59121918 delta=6.45573509 vol=13.63817345 trades=37 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.94677118 disp=-8.26307852 imbalance=0.47335774938395436 trades_per_min=2.466666666666667 vol_per_min=0.9092115633333333 large_share=0.11731776295893935 response=-1.2799593547138564 complete flow.window.15m@1",
        "86881000ms 15m end=86880000ms buy=10.60109604 sell=2.82002627 delta=7.78106977 vol=13.42112231 trades=37 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.44862542 disp=-6.89981013 imbalance=0.5797629728925404 trades_per_min=2.466666666666667 vol_per_min=0.8947414873333333 large_share=0.11921506734260587 response=-0.8867431258105786 complete flow.window.15m@1",
        "86941000ms 15m end=86940000ms buy=10.00618853 sell=2.81615300 delta=7.19003553 vol=12.82234153 trades=36 large=1 large_buy=1.60000000 large_sell=0.00000000 ref=62497.50417283 disp=-7.11867707 imbalance=0.5607427873589014 trades_per_min=2.4 vol_per_min=0.8548227686666666 large_share=0.12478220114918433 response=-0.990075367541334 complete flow.window.15m@1",
        "87061000ms 15m end=87060000ms buy=5.26255475 sell=3.07923180 delta=2.18332295 vol=8.34178655 trades=32 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62500.00000000 disp=-10.00795134 imbalance=0.26173325544993714 trades_per_min=2.1333333333333333 vol_per_min=0.5561191033333334 large_share=0 response=-4.583816306240907 complete flow.window.15m@1",
        "87121000ms 15m end=87120000ms buy=5.11159688 sell=3.29642283 delta=1.81517405 vol=8.40801971 trades=33 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62497.76916334 disp=-7.58959802 imbalance=0.21588603649931262 trades_per_min=2.2 vol_per_min=0.5605346473333334 large_share=0 response=-4.181195748143271 complete flow.window.15m@1",
        "87181000ms 15m end=87180000ms buy=4.09810808 sell=3.40762826 delta=0.69047982 vol=7.50573634 trades=32 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.93023411 disp=-5.15358979 imbalance=0.09199361511278452 trades_per_min=2.1333333333333333 vol_per_min=0.5003824226666667 large_share=0 response=-7.463780462114013 complete flow.window.15m@1",
        "87241000ms 15m end=87240000ms buy=4.75960222 sell=3.24506974 delta=1.51453248 vol=8.00467196 trades=33 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.60726055 disp=-2.37675991 imbalance=0.18920606460430142 trades_per_min=2.2 vol_per_min=0.5336447973333333 large_share=0 response=-1.5693026999328532 complete flow.window.15m@1",
        "87301000ms 15m end=87300000ms buy=5.12753926 sell=4.08737022 delta=1.04016904 vol=9.21490948 trades=36 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.52294990 disp=-3.55537344 imbalance=0.11287892108518031 trades_per_min=2.4 vol_per_min=0.6143272986666666 large_share=0 response=-3.4180727394078176 complete flow.window.15m@1",
        "87361000ms 15m end=87360000ms buy=5.64595620 sell=4.34121225 delta=1.30474395 vol=9.98716845 trades=38 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62495.57234589 disp=-3.70335773 imbalance=0.13064202897268645 trades_per_min=2.533333333333333 vol_per_min=0.66581123 large_share=0 response=-2.8383789248457525 complete flow.window.15m@1",
        "87421000ms 15m end=87420000ms buy=5.98884815 sell=4.60228870 delta=1.38655945 vol=10.59113685 trades=41 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62494.72985220 disp=-5.14457968 imbalance=0.13091696100593772 trades_per_min=2.7333333333333334 vol_per_min=0.70607579 large_share=0 response=-3.7103203039725416 complete flow.window.15m@1",
        "87481000ms 15m end=87480000ms buy=5.61503328 sell=5.06756717 delta=0.54746611 vol=10.68260045 trades=41 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62493.84038711 disp=-4.73100619 imbalance=0.05124839336287262 trades_per_min=2.7333333333333334 vol_per_min=0.7121733633333334 large_share=0 response=-8.64164211004769 complete flow.window.15m@1",
        "87541000ms 15m end=87540000ms buy=5.55601111 sell=5.89986495 delta=-0.34385384 vol=11.45587606 trades=41 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.69491447 disp=-1.85794810 imbalance=-0.030015499312236796 trades_per_min=2.7333333333333334 vol_per_min=0.7637250706666666 large_share=0 response=5.403307696083894 complete flow.window.15m@1",
        "87601000ms 15m end=87600000ms buy=6.30096506 sell=5.93302794 delta=0.36793712 vol=12.23399300 trades=44 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.22884361 disp=-1.50465349 imbalance=0.030074982060231684 trades_per_min=2.933333333333333 vol_per_min=0.8155995333333333 large_share=0 response=-4.089431069091371 complete flow.window.15m@1",
        "87730000ms 15m end=87720000ms buy=5.19203734 sell=5.09058819 delta=0.10144915 vol=10.28262553 trades=38 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.68369266 disp=1.43063306 imbalance=0.009866074545262565 trades_per_min=2.533333333333333 vol_per_min=0.6855083686666668 large_share=0 response=14.101971874579531 feed_gap flow.window.15m@1",
        "87781000ms 15m end=87780000ms buy=5.53559042 sell=5.40780104 delta=0.12778938 vol=10.94339146 trades=39 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.54881529 disp=-0.08144384 imbalance=0.011677310499865826 trades_per_min=2.6 vol_per_min=0.7295594306666667 large_share=0 response=-0.6373287044666779 feed_gap flow.window.15m@1",
        "87841000ms 15m end=87840000ms buy=5.55500717 sell=4.97110335 delta=0.58390382 vol=10.52611052 trades=39 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.38549576 disp=0.38246118 imbalance=0.05547194463620357 trades_per_min=2.6 vol_per_min=0.7017407013333333 large_share=0 response=0.6550071551167451 feed_gap flow.window.15m@1",
        "87901000ms 15m end=87900000ms buy=5.44373377 sell=4.87971201 delta=0.56402176 vol=10.32344578 trades=39 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.99204866 disp=-0.80587884 imbalance=0.05463502904163071 trades_per_min=2.6 vol_per_min=0.6882297186666667 large_share=0 response=-1.4288080658448354 feed_gap flow.window.15m@1",
        "87961000ms 15m end=87960000ms buy=6.03966055 sell=5.20701687 delta=0.83264368 vol=11.24667742 trades=43 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62489.99204866 disp=-1.83803135 imbalance=0.07403463697814497 trades_per_min=2.8666666666666667 vol_per_min=0.7497784946666666 large_share=0 response=-2.2074644822861083 feed_gap flow.window.15m@1",
        "88021000ms 15m end=88020000ms buy=6.90571907 sell=4.98982584 delta=1.91589323 vol=11.89554491 trades=43 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.17956532 disp=-1.21151968 imbalance=0.16105972820037884 trades_per_min=2.8666666666666667 vol_per_min=0.7930363273333333 large_share=0 response=-0.6323523988860277 feed_gap flow.window.15m@1",
        "88081000ms 15m end=88080000ms buy=6.98066266 sell=4.87862041 delta=2.10204225 vol=11.85928307 trades=42 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62490.77664432 disp=-2.32645854 imbalance=0.1772486783216652 trades_per_min=2.8 vol_per_min=0.7906188713333333 large_share=0 response=-1.1067610748547039 feed_gap flow.window.15m@1",
        "88141000ms 15m end=88140000ms buy=7.00312167 sell=4.50854799 delta=2.49457368 vol=11.51166966 trades=42 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62492.23050064 disp=-2.47666897 imbalance=0.2166995539029392 trades_per_min=2.8 vol_per_min=0.767444644 large_share=0 response=-0.992822537115841 feed_gap flow.window.15m@1",
        "88201000ms 15m end=88200000ms buy=7.03446028 sell=3.55673283 delta=3.47772745 vol=10.59119311 trades=41 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=62491.96757646 disp=-1.61602827 imbalance=0.32836030972907077 trades_per_min=2.7333333333333334 vol_per_min=0.7060795406666667 large_share=0 response=-0.46467938998497427 feed_gap flow.window.15m@1",
        "88261000ms 15m end=88260000ms buy=6.86727295 sell=7.07079696 delta=-0.20352401 vol=13.93806991 trades=44 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62491.86898816 disp=8.13101184 imbalance=-0.014602022468977557 trades_per_min=2.933333333333333 vol_per_min=0.9292046606666666 large_share=0.11479351232497871 response=-39.951118494569755 feed_gap flow.window.15m@1",
        "88321000ms 15m end=88320000ms buy=6.37629651 sell=6.80972051 delta=-0.43342400 vol=13.18601702 trades=41 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62489.58527252 disp=2.15102793 imbalance=-0.032869971223501424 trades_per_min=2.7333333333333334 vol_per_min=0.8790678013333333 large_share=0.12134065939496262 response=-4.96287222211968 feed_gap flow.window.15m@1",
        "88381000ms 15m end=88380000ms buy=6.41303668 sell=6.83121740 delta=-0.41818072 vol=13.24425408 trades=42 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62489.10938092 disp=2.44807695 imbalance=-0.03157450147619034 trades_per_min=2.8 vol_per_min=0.882950272 large_share=0.1208071055066923 response=-5.854112427756115 feed_gap flow.window.15m@1",
        "88441000ms 15m end=88440000ms buy=5.97634438 sell=6.41651726 delta=-0.44017288 vol=12.39286164 trades=40 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.83696637 disp=0.36155172 imbalance=-0.03551825984882052 trades_per_min=2.6666666666666665 vol_per_min=0.826190776 large_share=0.12910658139164055 response=-0.8213857246271056 feed_gap flow.window.15m@1",
        "88501000ms 15m end=88500000ms buy=5.83755182 sell=7.28643527 delta=-1.44888345 vol=13.12398709 trades=40 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.72419012 disp=0.91213720 imbalance=-0.11039963999233102 trades_per_min=2.6666666666666665 vol_per_min=0.8749324726666666 large_share=0.12191417052056853 response=-0.6295449092195787 feed_gap flow.window.15m@1",
        "88561000ms 15m end=88560000ms buy=6.84654868 sell=7.22103150 delta=-0.37448282 vol=14.06758018 trades=43 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62491.11432572 disp=2.55726927 imbalance=-0.0266202726558762 trades_per_min=2.8666666666666667 vol_per_min=0.9378386786666667 large_share=0.11373668957470978 response=-6.8288026403988304 feed_gap flow.window.15m@1",
        "88621000ms 15m end=88620000ms buy=6.90602605 sell=7.83238671 delta=-0.92636066 vol=14.73841276 trades=46 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62491.11432572 disp=2.37364037 imbalance=-0.06285348870905146 trades_per_min=3.066666666666667 vol_per_min=0.9825608506666667 large_share=0.10855985824622814 response=-2.5623285535463047 feed_gap flow.window.15m@1",
        "88681000ms 15m end=88680000ms buy=7.00919043 sell=7.76536616 delta=-0.75617573 vol=14.77455659 trades=46 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.46737145 disp=0.69612121 imbalance=-0.051180942412296114 trades_per_min=3.066666666666667 vol_per_min=0.9849704393333333 large_share=0.10829428215009464 response=-0.9205812648866686 complete flow.window.15m@1",
        "88741000ms 15m end=88740000ms buy=6.90632664 sell=7.95290778 delta=-1.04658114 vol=14.85923442 trades=45 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.76795694 disp=-0.86087043 imbalance=-0.07043304590385485 trades_per_min=3 vol_per_min=0.990615628 large_share=0.10767714908962316 response=0.8225548857110114 complete flow.window.15m@1",
        "88801000ms 15m end=88800000ms buy=6.84619915 sell=7.73694263 delta=-0.89074348 vol=14.58314178 trades=43 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62489.18616982 disp=0.60443556 imbalance=-0.06108035520998686 trades_per_min=2.8666666666666667 vol_per_min=0.972209452 large_share=0.10971572683976195 response=-0.6785742175738406 complete flow.window.15m@1",
        "88861000ms 15m end=88860000ms buy=6.52100528 sell=7.42575499 delta=-0.90474971 vol=13.94676027 trades=41 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.15401731 disp=2.00696518 imbalance=-0.06487167575011311 trades_per_min=2.7333333333333334 vol_per_min=0.929784018 large_share=0.11472198338718559 response=-2.2182545712006916 complete flow.window.15m@1",
        "88921000ms 15m end=88920000ms buy=6.00167229 sell=7.84568835 delta=-1.84401606 vol=13.84736064 trades=42 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.96804564 disp=1.65023788 imbalance=-0.1331673311571959 trades_per_min=2.8 vol_per_min=0.923157376 large_share=0.1155454849192113 response=-0.8949151343074528 complete flow.window.15m@1",
        "88981000ms 15m end=88980000ms buy=5.80454591 sell=7.96074471 delta=-2.15619880 vol=13.76529062 trades=43 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62488.45018578 disp=3.35708722 imbalance=-0.15664026714170456 trades_per_min=2.8666666666666667 vol_per_min=0.9176860413333333 large_share=0.11623437849363764 response=-1.5569469846657924 complete flow.window.15m@1",
        "89041000ms 15m end=89040000ms buy=6.16066273 sell=8.06510069 delta=-1.90443796 vol=14.22576342 trades=44 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62489.75383167 disp=4.38275800 imbalance=-0.13387246109565906 trades_per_min=2.933333333333333 vol_per_min=0.948384228 large_share=0.11247199554510798 response=-2.3013393410830774 complete flow.window.15m@1",
        "89101000ms 15m end=89100000ms buy=6.23089854 sell=7.98578685 delta=-1.75488831 vol=14.21668539 trades=45 large=1 large_buy=0.00000000 large_sell=1.60000000 ref=62490.35154819 disp=4.59116874 imbalance=-0.1234386400105883 trades_per_min=3 vol_per_min=0.947779026 large_share=0.11254381426527439 response=-2.616217062839743 complete flow.window.15m@1",
        "89161000ms 15m end=89160000ms buy=6.21069075 sell=7.73192323 delta=-1.52123248 vol=13.94261398 trades=43 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62500.00000000 disp=-5.71874500 imbalance=-0.10910669134081556 trades_per_min=2.8666666666666667 vol_per_min=0.9295075986666668 large_share=0.21516768694187144 response=3.7592840510478713 complete flow.window.15m@1",
        "89221000ms 15m end=89220000ms buy=6.68094767 sell=8.18554310 delta=-1.50459543 vol=14.86649077 trades=45 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.73630045 disp=2.73101206 imbalance=-0.10120716807198475 trades_per_min=3 vol_per_min=0.9910993846666667 large_share=0.2017961095468396 response=-1.8151138874587702 complete flow.window.15m@1",
        "89281000ms 15m end=89280000ms buy=6.41030202 sell=8.15721623 delta=-1.74691421 vol=14.56751825 trades=43 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.55745787 disp=2.07560118 imbalance=-0.11991845007642259 trades_per_min=2.8666666666666667 vol_per_min=0.9711678833333333 large_share=0.2059376174112567 response=-1.1881528973308884 complete flow.window.15m@1",
        "89341000ms 15m end=89340000ms buy=6.41030202 sell=7.96183075 delta=-1.55152873 vol=14.37213277 trades=43 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.19851809 disp=2.84785749 imbalance=-0.1079539658330056 trades_per_min=2.8666666666666667 vol_per_min=0.9581421846666667 large_share=0.20873728680423262 response=-1.8355170838505839 complete flow.window.15m@1",
        "89401000ms 15m end=89400000ms buy=6.18441382 sell=7.22157707 delta=-1.03716325 vol=13.40599089 trades=43 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.63632732 disp=1.90772616 imbalance=-0.07736565379688991 trades_per_min=2.8666666666666667 vol_per_min=0.893732726 large_share=0.22378054890651952 response=-1.8393692217690898 complete flow.window.15m@1",
        "89461000ms 15m end=89460000ms buy=5.63213443 sell=7.13230118 delta=-1.50016675 vol=12.76443561 trades=41 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62493.67159499 disp=1.38333360 imbalance=-0.11752707255029195 trades_per_min=2.7333333333333334 vol_per_min=0.850962374 large_share=0.23502801781926966 response=-0.9221198910054499 complete flow.window.15m@1",
        "89521000ms 15m end=89520000ms buy=6.06226781 sell=6.52094597 delta=-0.45867816 vol=12.58321378 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62493.48796609 disp=1.00909957 imbalance=-0.03645159082722029 trades_per_min=2.6 vol_per_min=0.8388809186666666 large_share=0.23841286116971622 response=-2.2000166085954476 complete flow.window.15m@1",
        "89581000ms 15m end=89580000ms buy=6.17070704 sell=6.24719076 delta=-0.07648372 vol=12.41789780 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.16349266 disp=2.57973790 imbalance=-0.006159151994309375 trades_per_min=2.6 vol_per_min=0.8278598533333333 large_share=0.24158678452000146 response=-33.729241987706665 complete flow.window.15m@1",
        "89641000ms 15m end=89640000ms buy=5.83313127 sell=6.53839488 delta=-0.70526361 vol=12.37152615 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62489.90708651 disp=2.56398220 imbalance=-0.05700700151694704 trades_per_min=2.6 vol_per_min=0.82476841 large_share=0.24249231369082141 response=-3.6354948187387692 complete flow.window.15m@1",
        "89701000ms 15m end=89700000ms buy=5.75618725 sell=6.47729585 delta=-0.72110860 vol=12.23348310 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62489.79060538 disp=3.82079566 imbalance=-0.05894548544396158 trades_per_min=2.6 vol_per_min=0.81556554 large_share=0.24522860541655547 response=-5.298502416972977 complete flow.window.15m@1",
        "89761000ms 15m end=89760000ms buy=6.30825202 sell=6.46117863 delta=-0.15292661 vol=12.76943065 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62490.16098249 disp=3.75618088 imbalance=-0.011975992837237423 trades_per_min=2.6 vol_per_min=0.8512953766666667 large_share=0.2349360815080663 response=-24.561983555379932 complete flow.window.15m@1",
        "89821000ms 15m end=89820000ms buy=6.42964719 sell=6.25193767 delta=0.17770952 vol=12.68158486 trades=39 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62490.61828352 disp=4.50235501 imbalance=0.014013194877599866 trades_per_min=2.6 vol_per_min=0.8454389906666666 large_share=0.23656349211229424 response=25.33547448667916 complete flow.window.15m@1",
        "89881000ms 15m end=89880000ms buy=6.70889043 sell=6.56215431 delta=0.14673612 vol=13.27104474 trades=41 large=1 large_buy=0.00000000 large_sell=3.00000000 ref=62491.80727300 disp=3.13608175 imbalance=0.011056862731969059 trades_per_min=2.7333333333333334 vol_per_min=0.884736316 large_share=0.2260560535191143 response=21.372254834051766 complete flow.window.15m@1",
        "89941000ms 15m end=89940000ms buy=8.80595511 sell=6.82540062 delta=1.98055449 vol=15.63135573 trades=41 large=2 large_buy=2.50000000 large_sell=3.00000000 ref=62494.13658967 disp=0.91003045 imbalance=0.126703948410494 trades_per_min=2.7333333333333334 vol_per_min=1.042090382 large_share=0.3518568763325048 response=0.45948266235280405 complete flow.window.15m@1",
        "90001000ms 15m end=90000000ms buy=8.33644365 sell=6.90532011 delta=1.43112354 vol=15.24176376 trades=38 large=2 large_buy=2.50000000 large_sell=3.00000000 ref=62494.94271693 disp=0.04483625 imbalance=0.09389487742591807 trades_per_min=2.533333333333333 vol_per_min=1.016117584 large_share=0.36085062638446247 response=0.03132940570595324 complete flow.window.15m@1",
        "90061000ms 15m end=90060000ms buy=8.78952807 sell=3.72286788 delta=5.06666019 vol=12.51239595 trades=38 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62494.28125500 disp=-1.49892210 imbalance=0.4049312545931701 trades_per_min=2.533333333333333 vol_per_min=0.83415973 large_share=0.19980186128940397 response=-0.2958402663273931 complete flow.window.15m@1",
        "90121000ms 15m end=90120000ms buy=8.85152298 sell=3.50254352 delta=5.34897946 vol=12.35406650 trades=39 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62494.46731251 disp=-1.92863326 imbalance=0.43297318012655994 trades_per_min=2.6 vol_per_min=0.8236044333333333 large_share=0.20236251763741114 response=-0.3605609769905529 complete flow.window.15m@1",
        "90181000ms 15m end=90180000ms buy=8.96387509 sell=3.52712348 delta=5.43675161 vol=12.49099857 trades=40 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62493.63305905 disp=0.19243901 imbalance=0.43525356115704045 trades_per_min=2.6666666666666665 vol_per_min=0.832733238 large_share=0.20014412666768883 response=0.035395954018947726 complete flow.window.15m@1",
        "90241000ms 15m end=90240000ms buy=9.45537087 sell=3.30491132 delta=6.15045955 vol=12.76028219 trades=42 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62494.04637558 disp=0.18148532 imbalance=0.4820002769860374 trades_per_min=2.8 vol_per_min=0.8506854793333333 large_share=0.1959204320700058 response=0.029507603216413318 complete flow.window.15m@1",
        "90301000ms 15m end=90300000ms buy=9.51846587 sell=3.61744725 delta=5.90101862 vol=13.13591312 trades=42 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62493.54405348 disp=0.05669565 imbalance=0.44922789653773226 trades_per_min=2.8 vol_per_min=0.8757275413333333 large_share=0.19031794570821584 response=0.009607773445730968 complete flow.window.15m@1",
        "90360500ms 15m end=90360000ms buy=9.49837851 sell=4.21550925 delta=5.28286926 vol=13.71388776 trades=43 large=1 large_buy=2.50000000 large_sell=0.00000000 ref=62495.05492859 disp=-0.55025622 imbalance=0.38522039500781213 trades_per_min=2.8666666666666667 vol_per_min=0.9142591840000001 large_share=0.182296956468601 response=-0.1041585912728039 complete flow.window.15m@1",
    ];
    const GOLDEN_WINDOW_1H: [&str; 5] = [
        "89401000ms 1h end=89400000ms buy=28.27929246 sell=23.54556960 delta=4.73372286 vol=51.82486206 trades=162 large=3 large_buy=1.60000000 large_sell=4.60000000 ref=- disp=- imbalance=0.09134077104767889 trades_per_min=2.7 vol_per_min=0.863747701 large_share=0.11963369999561171 response=- partial_start+feed_gap flow.window.1h@1",
        "89701000ms 1h end=89700000ms buy=27.36068334 sell=22.54244628 delta=4.81823706 vol=49.90312962 trades=158 large=3 large_buy=1.60000000 large_sell=4.60000000 ref=62497.34732620 disp=-3.73592516 imbalance=0.09655180139381407 trades_per_min=2.6333333333333333 vol_per_min=0.831718827 large_share=0.12424070488587526 response=-0.7753718037277311 feed_gap flow.window.1h@1",
        "90001000ms 1h end=90000000ms buy=26.72934173 sell=22.53521001 delta=4.19413172 vol=49.26455174 trades=160 large=3 large_buy=2.50000000 large_sell=4.60000000 ref=62495.52294990 disp=-0.53539672 imbalance=0.0851348803930069 trades_per_min=2.6666666666666665 vol_per_min=0.8210758623333334 large_share=0.14411985391587773 response=-0.1276537685850267 feed_gap flow.window.1h@1",
        "90301000ms 1h end=90300000ms buy=27.84139657 sell=24.05848753 delta=3.78290904 vol=51.89988410 trades=169 large=3 large_buy=2.50000000 large_sell=4.60000000 ref=62492.22884361 disp=1.37190552 imbalance=0.07288858357970784 trades_per_min=2.816666666666667 vol_per_min=0.8649980683333334 large_share=0.13680184692358494 response=0.3626588705923524 feed_gap flow.window.1h@1",
        "90360500ms 1h end=90360000ms buy=27.46651065 sell=24.16648920 delta=3.30002145 vol=51.63299985 trades=168 large=3 large_buy=2.50000000 large_sell=4.60000000 ref=62488.94116372 disp=5.56350865 imbalance=0.06391302964357977 trades_per_min=2.8 vol_per_min=0.8605499974999999 large_share=0.13750895784917289 response=1.6859007537663127 feed_gap flow.window.1h@1",
    ];
}
