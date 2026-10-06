//! Event-time bars on a fixed set of timeframes (ADR-031, proposed).
//!
//! Bars are the base for volatility, structure, volume profile and
//! multi-timeframe context (Market State & Regime brief, brief §8). They are
//! built from trades only — never from klines (ADR-028) — and close in event
//! time, so live processing and replay produce the same bars (ADR-019).
//!
//! - **Timeframes**: [`Timeframe::ALL`] — 1m, 5m, 15m, 1h, 4h, 1d. Each bar
//!   covers the half-open interval `[open, open + timeframe)`, aligned to the
//!   Unix epoch; every timeframe divides a day, so that is also UTC-midnight
//!   alignment. A trade belongs to the bar containing its trade time.
//! - **Every timeframe is built from trades directly**, not rolled up from a
//!   shorter one.
//! - **Closing**: only trades-stream events close bars — a trade, or a
//!   [`FeedGap`] on [`Stream::Trades`] — when their ordering time (ADR-028)
//!   is at or after a bar's end. Every elapsed interval yields a bar, so a
//!   series has no holes: an interval without trades is an empty bar (no
//!   OHLC, zero volume).
//! - **Completeness** ([`Coverage`]): the bar containing the first consumed
//!   trades-stream event is `partial_start`; a bar overlapping a trades gap
//!   `[start, end]` is `feed_gap`. Closed bars are immutable, so a late gap
//!   reaching back before them marks only bars that are still open.
//! - **No look-ahead**: a closed bar is never revised, and the developing
//!   bar holds exactly the trades consumed in its interval so far.
//! - **Bounded jumps**: one event closes at most [`MAX_BARS_PER_EVENT`] bars
//!   per series; a longer jump in event time is rejected before any bar is
//!   built.

use crate::event::{Aggressor, FeedGap, Kline, MarketEvent, Stream, Trade};
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::num::{Price, Qty};
use crate::time::EventTime;
use std::fmt;

/// The most bars one event may close in one series: 31 days of 1m bars
/// (ADR-031).
///
/// A week-long trades outage (10 080 1m bars) stays well inside. A longer
/// jump in event time — such as a microsecond timestamp read as
/// milliseconds — is rejected before any bar is built, instead of
/// materializing millions of empty bars.
pub const MAX_BARS_PER_EVENT: u64 = 44_640;

/// A bar timeframe (ADR-031).
///
/// Declared shortest first: the derived [`Ord`] breaks ties between bars
/// that close at the same instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Timeframe {
    /// One minute.
    M1,
    /// Five minutes.
    M5,
    /// Fifteen minutes.
    M15,
    /// One hour.
    H1,
    /// Four hours.
    H4,
    /// One day, opening at 00:00 UTC.
    D1,
}

impl Timeframe {
    /// Every timeframe, shortest first.
    pub const ALL: [Self; 6] = [Self::M1, Self::M5, Self::M15, Self::H1, Self::H4, Self::D1];

    /// The length of a bar in milliseconds.
    pub const fn millis(self) -> i64 {
        match self {
            Self::M1 => 60_000,
            Self::M5 => 300_000,
            Self::M15 => 900_000,
            Self::H1 => 3_600_000,
            Self::H4 => 14_400_000,
            Self::D1 => 86_400_000,
        }
    }

    /// The label used in feature ids and Binance kline intervals: `1m`,
    /// `5m`, `15m`, `1h`, `4h`, `1d`.
    pub const fn label(self) -> &'static str {
        match self {
            Self::M1 => "1m",
            Self::M5 => "5m",
            Self::M15 => "15m",
            Self::H1 => "1h",
            Self::H4 => "4h",
            Self::D1 => "1d",
        }
    }

    /// The open time of the bar containing `time`, or `None` if it falls
    /// before the first representable bar.
    pub fn open_of(self, time: EventTime) -> Option<EventTime> {
        let millis = time.as_millis();
        millis
            .checked_sub(millis.rem_euclid(self.millis()))
            .map(EventTime::from_millis)
    }

    /// The timeframe of an exchange kline: its interval
    /// `close_time - open_time + 1 ms` is one of [`Self::ALL`] and its open
    /// time is aligned to it. `None` for any other interval.
    pub fn of_kline(kline: &Kline) -> Option<Self> {
        let open = kline.open_time.as_millis();
        let span = kline
            .close_time
            .as_millis()
            .checked_sub(open)?
            .checked_add(1)?;
        Self::ALL
            .into_iter()
            .find(|timeframe| timeframe.millis() == span && open.rem_euclid(span) == 0)
    }

    /// The exclusive end of the bar opening at `open`, if representable.
    fn end_of(self, open: EventTime) -> Option<EventTime> {
        open.as_millis()
            .checked_add(self.millis())
            .map(EventTime::from_millis)
    }
}

impl fmt::Display for Timeframe {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// Open, high, low and close of a bar with at least one trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ohlc {
    /// Price of the first trade.
    pub open: Price,
    /// Highest trade price.
    pub high: Price,
    /// Lowest trade price.
    pub low: Price,
    /// Price of the last trade.
    pub close: Price,
}

/// Why a bar may not hold every trade of its interval. Both `false` means
/// complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Coverage {
    /// The bar contains the first consumed trades-stream event, so trades
    /// before consumption started are missing.
    pub partial_start: bool,
    /// The bar overlaps a trades feed gap.
    pub feed_gap: bool,
}

impl Coverage {
    /// Whether neither flag is set.
    pub fn is_complete(self) -> bool {
        !self.partial_start && !self.feed_gap
    }
}

impl fmt::Display for Coverage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match (self.partial_start, self.feed_gap) {
            (false, false) => "complete",
            (true, false) => "partial_start",
            (false, true) => "feed_gap",
            (true, true) => "partial_start+feed_gap",
        })
    }
}

/// One time bar built from trades (ADR-031).
///
/// `Display` prints the canonical line the golden tests pin: timeframe, open
/// time, `ohlc=O/H/L/C` (`-` when empty), volumes, delta, trade count and
/// coverage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Bar {
    /// The timeframe.
    pub timeframe: Timeframe,
    /// Inclusive start of the interval.
    pub open_time: EventTime,
    /// Prices; `None` for an interval without trades — no price is invented.
    pub ohlc: Option<Ohlc>,
    /// Traded base volume.
    pub volume: Qty,
    /// Volume of trades whose aggressor bought (Binance taker-buy volume).
    pub buy_volume: Qty,
    /// Volume of trades whose aggressor sold.
    pub sell_volume: Qty,
    /// `buy_volume - sell_volume`: aggression, not direction (ADR-023).
    pub delta: Qty,
    /// Number of trades.
    pub trade_count: u64,
    /// Completeness.
    pub coverage: Coverage,
}

impl Bar {
    /// An empty, complete bar.
    pub fn empty(timeframe: Timeframe, open_time: EventTime) -> Self {
        Self {
            timeframe,
            open_time,
            ohlc: None,
            volume: Qty::from_units(0),
            buy_volume: Qty::from_units(0),
            sell_volume: Qty::from_units(0),
            delta: Qty::from_units(0),
            trade_count: 0,
            coverage: Coverage::default(),
        }
    }

    /// Exclusive end of the interval, `open_time + timeframe`. Bars built by
    /// the Market State always have a representable end (an event that would
    /// need another is rejected); for others it saturates.
    pub fn end(&self) -> EventTime {
        EventTime::from_millis(
            self.open_time
                .as_millis()
                .saturating_add(self.timeframe.millis()),
        )
    }

    /// Whether the bar holds every trade of its interval.
    pub fn is_complete(&self) -> bool {
        self.coverage.is_complete()
    }

    /// Whether the interval had no trade.
    pub fn is_empty(&self) -> bool {
        self.ohlc.is_none()
    }

    /// Folds one trade in, or `None` on overflow (the bar is then
    /// unspecified and must be discarded).
    fn add_trade(&mut self, trade: &Trade) -> Option<()> {
        let price = trade.price;
        self.ohlc = Some(match self.ohlc {
            None => Ohlc {
                open: price,
                high: price,
                low: price,
                close: price,
            },
            Some(ohlc) => Ohlc {
                high: ohlc.high.max(price),
                low: ohlc.low.min(price),
                close: price,
                ..ohlc
            },
        });
        self.volume = self.volume.checked_add(trade.qty)?;
        match trade.aggressor {
            Aggressor::Buy => self.buy_volume = self.buy_volume.checked_add(trade.qty)?,
            Aggressor::Sell => self.sell_volume = self.sell_volume.checked_add(trade.qty)?,
        }
        self.delta = self.buy_volume.checked_sub(self.sell_volume)?;
        self.trade_count = self.trade_count.checked_add(1)?;
        Some(())
    }

    /// Marks the bar if it overlaps `gap`.
    fn mark(&mut self, gap: Option<&FeedGap>) {
        if let Some(gap) = gap
            && self.open_time <= gap.end
            && gap.start < self.end()
        {
            self.coverage.feed_gap = true;
        }
    }
}

impl fmt::Display for Bar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} ohlc=", self.timeframe, self.open_time)?;
        match self.ohlc {
            Some(ohlc) => write!(f, "{}/{}/{}/{}", ohlc.open, ohlc.high, ohlc.low, ohlc.close)?,
            None => f.write_str("-")?,
        }
        write!(
            f,
            " vol={} buy={} sell={} delta={} trades={} {}",
            self.volume,
            self.buy_volume,
            self.sell_volume,
            self.delta,
            self.trade_count,
            self.coverage
        )
    }
}

/// The bars of one timeframe: the feature `bars.time.<label>@1`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarSeries {
    timeframe: Timeframe,
    feature: FeatureKey,
    last_closed: FeatureValue<Bar>,
    developing: FeatureValue<Bar>,
}

/// The validity of a bar that has not appeared yet.
const NOT_YET: FeatureValue<Bar> = FeatureValue::WarmingUp {
    observed: 0,
    required: 1,
};

impl BarSeries {
    fn new(timeframe: Timeframe, feature: FeatureKey) -> Self {
        Self {
            timeframe,
            feature,
            last_closed: NOT_YET,
            developing: NOT_YET,
        }
    }

    /// The timeframe.
    pub fn timeframe(&self) -> Timeframe {
        self.timeframe
    }

    /// The feature these bars are (`bars.time.<label>@1`).
    pub fn feature(&self) -> FeatureKey {
        self.feature
    }

    /// The most recently closed bar; warming up until the first close.
    pub fn last_closed(&self) -> &FeatureValue<Bar> {
        &self.last_closed
    }

    /// The bar of the interval containing the last trades-stream event, with
    /// the trades consumed in it so far; warming up until the first
    /// trades-stream event.
    pub fn developing(&self) -> &FeatureValue<Bar> {
        &self.developing
    }

    /// Updates the series with `event`, appending the bars it closes to
    /// `closed` in close order. On error the series and `closed` are
    /// unspecified and must be discarded.
    fn apply(&mut self, event: &MarketEvent, closed: &mut Vec<Bar>) -> Result<(), BarError> {
        match event {
            MarketEvent::Trade(trade) => {
                let mut bar = self.advance(trade.time, None, closed)?;
                bar.add_trade(trade).ok_or(BarError::Overflow)?;
                self.developing = FeatureValue::Ready(bar);
            }
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                let bar = self.advance(gap.end, Some(gap), closed)?;
                self.developing = FeatureValue::Ready(bar);
            }
            // Other streams never close or mark bars (ADR-031).
            MarketEvent::FeedGap(_)
            | MarketEvent::BookSnapshot(_)
            | MarketEvent::Liquidation(_)
            | MarketEvent::BookUpdate(_)
            | MarketEvent::MarkPrice(_)
            | MarketEvent::FundingSettlement(_)
            | MarketEvent::OpenInterest(_)
            | MarketEvent::Kline(_) => {}
        }
        Ok(())
    }

    /// Closes every bar that ends at or before `time` and returns the bar
    /// containing it, marking each open bar that overlaps `gap`. Opens the
    /// first bar (`partial_start`) on the first trades-stream event.
    ///
    /// The number of bars to close is checked against
    /// [`MAX_BARS_PER_EVENT`] first, in constant time, so a too-long jump
    /// builds nothing.
    fn advance(
        &mut self,
        time: EventTime,
        gap: Option<&FeedGap>,
        closed: &mut Vec<Bar>,
    ) -> Result<Bar, BarError> {
        let timeframe = self.timeframe;
        let mut bar = match self.developing {
            FeatureValue::Ready(bar) => bar,
            FeatureValue::WarmingUp { .. } | FeatureValue::Unavailable { .. } => {
                let open = timeframe.open_of(time).ok_or(BarError::Overflow)?;
                timeframe.end_of(open).ok_or(BarError::Overflow)?;
                let mut first = Bar::empty(timeframe, open);
                first.coverage.partial_start = true;
                first
            }
        };
        let bars = bars_to_close(&bar, time);
        if bars > MAX_BARS_PER_EVENT {
            return Err(BarError::TooManyBars {
                timeframe,
                from: bar.open_time,
                bars,
            });
        }
        bar.mark(gap);
        while bar.end() <= time {
            let open = bar.end();
            timeframe.end_of(open).ok_or(BarError::Overflow)?;
            closed.push(bar);
            self.last_closed = FeatureValue::Ready(bar);
            bar = Bar::empty(timeframe, open);
            bar.mark(gap);
        }
        Ok(bar)
    }
}

/// How many bars reaching `time` closes, starting with `developing`:
/// `0` if `time` is inside it, else one per elapsed interval. Saturates at
/// `u64::MAX` for a span beyond the `i64` range, which exceeds any bound.
fn bars_to_close(developing: &Bar, time: EventTime) -> u64 {
    let Some(span) = time.as_millis().checked_sub(developing.end().as_millis()) else {
        return u64::MAX;
    };
    if span < 0 {
        return 0;
    }
    let elapsed = span / developing.timeframe.millis();
    u64::try_from(elapsed).map_or(u64::MAX, |elapsed| elapsed.saturating_add(1))
}

/// The bar series of every timeframe, in [`Timeframe::ALL`] order: the
/// `bars.time.*@1` features of the Market State.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarSet {
    series: [BarSeries; 6],
}

impl Default for BarSet {
    fn default() -> Self {
        Self::new()
    }
}

impl BarSet {
    /// Empty series for [`catalog::BARS_TIME`].
    pub fn new() -> Self {
        Self {
            series: catalog::BARS_TIME
                .map(|(timeframe, definition)| BarSeries::new(timeframe, definition.key)),
        }
    }

    /// The series of `timeframe`, if the set computes it.
    pub fn get(&self, timeframe: Timeframe) -> Option<&BarSeries> {
        self.series
            .iter()
            .find(|series| series.timeframe == timeframe)
    }

    /// Every series, shortest timeframe first.
    pub fn iter(&self) -> impl Iterator<Item = &BarSeries> {
        self.series.iter()
    }

    /// Updates every series with `event` and appends the bars it closes to
    /// `closed`, sorted by `(end, timeframe)`. On error `self` and `closed`
    /// are unspecified: the caller works on a copy and discards it. Series
    /// are applied shortest first, so a too-long jump is reported for the
    /// series that closes the most bars, before any is built.
    pub(crate) fn apply(
        &mut self,
        event: &MarketEvent,
        closed: &mut Vec<Bar>,
    ) -> Result<(), BarError> {
        let first = closed.len();
        for series in &mut self.series {
            series.apply(event, closed)?;
        }
        closed[first..].sort_by_key(|bar| (bar.end(), bar.timeframe));
        Ok(())
    }
}

/// Why bars could not take an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BarError {
    /// A bar's time or quantity arithmetic left the `i64` range.
    Overflow,
    /// The event would close more than [`MAX_BARS_PER_EVENT`] bars of one
    /// series.
    TooManyBars {
        /// The series.
        timeframe: Timeframe,
        /// Open time of its developing bar.
        from: EventTime,
        /// How many bars the event would close.
        bars: u64,
    },
}

/// A field in which a bar and an exchange kline disagree.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum KlineField {
    /// The kline does not cover the bar's interval: its open time differs,
    /// or its close time is not the bar's end minus one millisecond.
    Interval,
    /// Open price.
    Open,
    /// High price.
    High,
    /// Low price.
    Low,
    /// Close price.
    Close,
    /// Traded volume.
    Volume,
    /// Taker-buy volume against [`Bar::buy_volume`].
    TakerBuyVolume,
}

impl fmt::Display for KlineField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Interval => "interval",
            Self::Open => "open",
            Self::High => "high",
            Self::Low => "low",
            Self::Close => "close",
            Self::Volume => "volume",
            Self::TakerBuyVolume => "taker_buy_volume",
        })
    }
}

/// The fields in which `bar` disagrees with the exchange `kline` for its
/// interval; empty means they match (ADR-031).
///
/// An empty bar has no prices, so only the volumes are compared (an exchange
/// kline carries the previous close there). The trade count is never
/// compared: bars count (aggregate) trades, klines count raw trades.
pub fn kline_mismatches(bar: &Bar, kline: &Kline) -> Vec<KlineField> {
    let mut fields = Vec::new();
    let last_millis = bar.end().as_millis().saturating_sub(1);
    if kline.open_time != bar.open_time || kline.close_time.as_millis() != last_millis {
        fields.push(KlineField::Interval);
    }
    if let Some(ohlc) = bar.ohlc {
        for (field, ours, theirs) in [
            (KlineField::Open, ohlc.open, kline.open),
            (KlineField::High, ohlc.high, kline.high),
            (KlineField::Low, ohlc.low, kline.low),
            (KlineField::Close, ohlc.close, kline.close),
        ] {
            if ours != theirs {
                fields.push(field);
            }
        }
    }
    if bar.volume != kline.volume {
        fields.push(KlineField::Volume);
    }
    if bar.buy_volume != kline.taker_buy_volume {
        fields.push(KlineField::TakerBuyVolume);
    }
    fields
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event::GapReason;
    use crate::event::samples::{gap, kline, mark, t};

    const DAY: i64 = 86_400_000;

    fn trade(
        millis: i64,
        trade_id: u64,
        price: i64,
        qty: i64,
        aggressor: Aggressor,
    ) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
            aggressor,
        })
    }

    fn buy(millis: i64, trade_id: u64) -> MarketEvent {
        trade(
            millis,
            trade_id,
            6_354_210_000_000,
            1_500_000,
            Aggressor::Buy,
        )
    }

    /// Applies `events` in order and returns the set and the bars each event
    /// closed.
    fn run(events: &[MarketEvent]) -> (BarSet, Vec<Vec<Bar>>) {
        let mut bars = BarSet::new();
        let closed = events
            .iter()
            .map(|event| {
                let mut closed = Vec::new();
                bars.apply(event, &mut closed).unwrap();
                closed
            })
            .collect();
        (bars, closed)
    }

    fn series(bars: &BarSet, timeframe: Timeframe) -> &BarSeries {
        bars.get(timeframe).unwrap()
    }

    fn developing(bars: &BarSet, timeframe: Timeframe) -> Bar {
        *series(bars, timeframe).developing().ready().unwrap()
    }

    fn of(closed: &[Bar], timeframe: Timeframe) -> Vec<Bar> {
        closed
            .iter()
            .filter(|bar| bar.timeframe == timeframe)
            .copied()
            .collect()
    }

    #[test]
    fn timeframes_are_epoch_aligned_and_labelled() {
        let labels: Vec<_> = Timeframe::ALL.iter().map(|tf| tf.label()).collect();
        assert_eq!(labels, ["1m", "5m", "15m", "1h", "4h", "1d"]);
        let millis: Vec<_> = Timeframe::ALL.iter().map(|tf| tf.millis()).collect();
        assert_eq!(
            millis,
            [60_000, 300_000, 900_000, 3_600_000, 14_400_000, 86_400_000]
        );
        // Shortest first, so the derived order is the close-order tie-break.
        assert!(Timeframe::ALL.windows(2).all(|pair| pair[0] < pair[1]));
        for timeframe in Timeframe::ALL {
            // Dividing a day makes epoch alignment UTC-midnight alignment.
            assert_eq!(DAY % timeframe.millis(), 0, "{timeframe}");
            assert_eq!(timeframe.to_string(), timeframe.label());
        }
        let open = |timeframe: Timeframe, millis| timeframe.open_of(t(millis)).unwrap();
        assert_eq!(open(Timeframe::M1, 0), t(0));
        assert_eq!(open(Timeframe::M1, 59_999), t(0));
        assert_eq!(open(Timeframe::M1, 60_000), t(60_000));
        assert_eq!(open(Timeframe::M1, -1), t(-60_000));
        assert_eq!(open(Timeframe::H4, 5 * 3_600_000), t(4 * 3_600_000));
        // 2024-03-09 13:45:12.345 UTC opens its daily bar at 00:00 UTC.
        assert_eq!(open(Timeframe::D1, 1_709_991_912_345), t(1_709_942_400_000));
        assert_eq!(Timeframe::M1.open_of(t(i64::MIN)), None);
        assert_eq!(Timeframe::M1.end_of(t(i64::MAX - 59_999)), None);
    }

    #[test]
    fn klines_map_to_their_timeframe() {
        let interval = |open: i64, close: i64| {
            let MarketEvent::Kline(kline) = kline(open, close) else {
                unreachable!("samples::kline builds a kline")
            };
            Timeframe::of_kline(&kline)
        };
        assert_eq!(interval(0, 59_999), Some(Timeframe::M1));
        assert_eq!(interval(60_000, 119_999), Some(Timeframe::M1));
        assert_eq!(interval(900_000, 1_799_999), Some(Timeframe::M15));
        assert_eq!(interval(DAY, 2 * DAY - 1), Some(Timeframe::D1));
        // A 3m kline, a misaligned one and an inverted one have no timeframe.
        assert_eq!(interval(0, 179_999), None);
        assert_eq!(interval(1_000, 60_999), None);
        assert_eq!(interval(60_000, 0), None);
        assert_eq!(interval(i64::MIN, i64::MAX), None);
    }

    #[test]
    fn a_trade_on_the_boundary_opens_the_next_bar() {
        let (bars, closed) = run(&[buy(1_000, 1), buy(59_999, 2), buy(60_000, 3)]);
        assert!(closed[0].is_empty() && closed[1].is_empty());
        let [closed_bar] = closed[2][..] else {
            panic!("one bar closes: {:?}", closed[2])
        };
        assert_eq!(closed_bar.timeframe, Timeframe::M1);
        assert_eq!(closed_bar.open_time, t(0));
        assert_eq!(closed_bar.end(), t(60_000));
        assert_eq!(closed_bar.trade_count, 2);
        assert_eq!(
            series(&bars, Timeframe::M1).last_closed(),
            &FeatureValue::Ready(closed_bar)
        );
        let next = developing(&bars, Timeframe::M1);
        assert_eq!((next.open_time, next.trade_count), (t(60_000), 1));
        assert!(next.is_complete());
        // The 5m bar still holds all three trades.
        assert_eq!(developing(&bars, Timeframe::M5).trade_count, 3);
        assert!(!series(&bars, Timeframe::M5).last_closed().is_ready());
    }

    #[test]
    fn empty_intervals_become_empty_complete_bars() {
        let (bars, closed) = run(&[buy(10_000, 1), buy(200_000, 2)]);
        let minutes = of(&closed[1], Timeframe::M1);
        assert_eq!(minutes.len(), 3);
        assert_eq!(minutes[0].trade_count, 1);
        assert!(minutes[0].coverage.partial_start);
        for (bar, open) in minutes[1..].iter().zip([60_000, 120_000]) {
            assert_eq!(*bar, Bar::empty(Timeframe::M1, t(open)));
            assert!(bar.is_empty() && bar.is_complete());
        }
        assert_eq!(
            minutes[1].to_string(),
            "1m 60000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 \
             delta=0.00000000 trades=0 complete"
        );
        assert_eq!(developing(&bars, Timeframe::M1).open_time, t(180_000));
    }

    #[test]
    fn a_trades_gap_marks_every_bar_it_overlaps() {
        let (bars, closed) = run(&[
            buy(10_000, 1),
            buy(70_000, 2),
            gap(Stream::Trades, 90_000, 250_000, GapReason::Disconnected),
        ]);
        // The developing bar, the empty bars inside the gap and the bar
        // containing its end are all marked.
        let minutes = of(&closed[2], Timeframe::M1);
        let opens: Vec<_> = minutes.iter().map(|bar| bar.open_time).collect();
        assert_eq!(opens, [t(60_000), t(120_000), t(180_000)]);
        assert!(minutes.iter().all(|bar| bar.coverage.feed_gap));
        assert!(!minutes[0].coverage.partial_start);
        assert_eq!(minutes[0].trade_count, 1);
        let open = developing(&bars, Timeframe::M1);
        assert_eq!(open.open_time, t(240_000));
        assert!(open.coverage.feed_gap);
        // Higher timeframes' developing bars are marked too.
        for timeframe in &Timeframe::ALL[1..] {
            let bar = developing(&bars, *timeframe);
            assert_eq!(
                bar.coverage,
                Coverage {
                    partial_start: true,
                    feed_gap: true
                },
                "{timeframe}"
            );
        }
        // The first bar after the gap's end is complete again.
        let (_, closed) = run(&[
            buy(10_000, 1),
            buy(70_000, 2),
            gap(Stream::Trades, 90_000, 250_000, GapReason::Disconnected),
            buy(300_000, 3),
            buy(360_000, 4),
        ]);
        assert!(closed[3].iter().all(|bar| bar.coverage.feed_gap));
        assert_eq!(of(&closed[3], Timeframe::M5).len(), 1);
        let [after] = closed[4][..] else {
            panic!("one bar closes: {:?}", closed[4])
        };
        assert_eq!(after.open_time, t(300_000));
        assert!(after.is_complete());
    }

    #[test]
    fn a_gap_marks_only_the_bars_it_overlaps() {
        let (bars, closed) = run(&[
            buy(10_000, 1),
            gap(Stream::Trades, 130_000, 130_000, GapReason::SequenceBreak),
        ]);
        let minutes = of(&closed[1], Timeframe::M1);
        assert_eq!(minutes.len(), 2);
        assert_eq!(
            minutes[0].coverage,
            Coverage {
                partial_start: true,
                feed_gap: false
            }
        );
        assert!(minutes[1].is_complete() && minutes[1].is_empty());
        let open = developing(&bars, Timeframe::M1);
        assert_eq!(open.open_time, t(120_000));
        assert!(open.coverage.feed_gap);
        // A gap starting exactly at a bar's end does not touch that bar.
        let (bars, closed) = run(&[
            buy(10_000, 1),
            gap(Stream::Trades, 60_000, 70_000, GapReason::Disconnected),
        ]);
        assert!(!closed[1][0].coverage.feed_gap);
        assert!(developing(&bars, Timeframe::M1).coverage.feed_gap);
        // A gap ending exactly on a boundary closes the bar before it and
        // marks the one it ends in.
        let (bars, closed) = run(&[
            buy(10_000, 1),
            gap(Stream::Trades, 20_000, 60_000, GapReason::Disconnected),
        ]);
        assert!(closed[1][0].coverage.feed_gap);
        let open = developing(&bars, Timeframe::M1);
        assert_eq!(open.open_time, t(60_000));
        assert!(open.coverage.feed_gap);
    }

    #[test]
    fn other_streams_neither_close_nor_mark() {
        let events = [
            buy(10_000, 1),
            kline(0, 59_999),
            mark(60_000, 1),
            gap(Stream::OrderBook, 0, 120_000, GapReason::Disconnected),
            gap(Stream::Klines, 0, 130_000, GapReason::MissingData),
            buy(130_500, 2),
        ];
        let (bars, closed) = run(&events);
        let (before, _) = run(&events[..1]);
        for (event, closed) in events[1..5].iter().zip(&closed[1..5]) {
            assert!(closed.is_empty(), "{event:?}");
        }
        let (after_others, _) = run(&events[..5]);
        assert_eq!(after_others, before);
        let minutes = of(&closed[5], Timeframe::M1);
        assert_eq!(minutes.len(), 2);
        assert!(minutes.iter().all(|bar| !bar.coverage.feed_gap));
        assert!(developing(&bars, Timeframe::M1).is_complete());
    }

    #[test]
    fn partial_start_marks_the_first_bar_of_every_timeframe() {
        // Nothing opens before the first trades-stream event.
        let (bars, _) = run(&[mark(500, 1), kline(0, 59_999)]);
        for series in bars.iter() {
            assert_eq!(series.developing(), &NOT_YET);
            assert_eq!(series.last_closed(), &NOT_YET);
        }
        let (bars, _) = run(&[mark(500, 1), buy(1_000, 1)]);
        for series in bars.iter() {
            let bar = series.developing().ready().unwrap();
            assert_eq!(bar.open_time, t(0));
            assert_eq!(
                bar.coverage,
                Coverage {
                    partial_start: true,
                    feed_gap: false
                }
            );
            assert_eq!(series.last_closed(), &NOT_YET);
        }
        // A trades gap as the first event opens the first bars too.
        let (bars, _) = run(&[gap(Stream::Trades, 500, 1_000, GapReason::Disconnected)]);
        for series in bars.iter() {
            let bar = series.developing().ready().unwrap();
            assert_eq!(
                bar.coverage,
                Coverage {
                    partial_start: true,
                    feed_gap: true
                }
            );
            assert!(bar.is_empty());
        }
    }

    #[test]
    fn bars_close_in_end_then_timeframe_order() {
        // 00:59:59.999, then 01:00:00.000.
        let (_, closed) = run(&[buy(3_599_999, 1), buy(3_600_000, 2)]);
        let order: Vec<_> = closed[1]
            .iter()
            .map(|bar| (bar.end(), bar.timeframe))
            .collect();
        assert_eq!(
            order,
            [
                (t(3_600_000), Timeframe::M1),
                (t(3_600_000), Timeframe::M5),
                (t(3_600_000), Timeframe::M15),
                (t(3_600_000), Timeframe::H1),
            ]
        );
        // Across ends: a jump closes earlier ends first.
        let (_, closed) = run(&[buy(0, 1), buy(600_000, 2)]);
        let order: Vec<_> = closed[1]
            .iter()
            .map(|bar| (bar.end(), bar.timeframe))
            .collect();
        let mut sorted = order.clone();
        sorted.sort();
        assert_eq!(order, sorted);
        assert_eq!(order.len(), 10 + 2);
        assert_eq!(
            order[4..6],
            [(t(300_000), Timeframe::M1), (t(300_000), Timeframe::M5)]
        );
    }

    #[test]
    fn a_seven_day_gap_closes_every_elapsed_bar() {
        let (_, closed) = run(&[
            buy(0, 1),
            gap(Stream::Trades, 1, 7 * DAY, GapReason::Disconnected),
        ]);
        let counts: Vec<_> = Timeframe::ALL
            .iter()
            .map(|timeframe| of(&closed[1], *timeframe).len())
            .collect();
        assert_eq!(counts, [10_080, 2_016, 672, 168, 42, 7]);
        assert!(closed[1].iter().all(|bar| bar.coverage.feed_gap));
    }

    #[test]
    fn a_late_event_gap_marks_only_open_bars() {
        let (bars, closed) = run(&[
            buy(10_000, 1),
            buy(61_000, 2),
            gap(Stream::Trades, 30_000, 61_001, GapReason::LateEvent),
        ]);
        // The gap reaches back into the closed minute, which stays as it was.
        assert!(closed[2].is_empty());
        let earlier = of(&closed[1], Timeframe::M1);
        assert_eq!(
            series(&bars, Timeframe::M1).last_closed(),
            &FeatureValue::Ready(earlier[0])
        );
        assert!(!earlier[0].coverage.feed_gap);
        // Open bars are marked.
        for timeframe in Timeframe::ALL {
            assert!(
                developing(&bars, timeframe).coverage.feed_gap,
                "{timeframe}"
            );
        }
    }

    #[test]
    fn overflow_is_an_error() {
        let mut bars = BarSet::new();
        assert_eq!(
            bars.apply(&buy(i64::MAX, 1), &mut Vec::new()),
            Err(BarError::Overflow)
        );
        let mut bars = BarSet::new();
        bars.apply(&trade(0, 1, 1, i64::MAX, Aggressor::Sell), &mut Vec::new())
            .unwrap();
        assert_eq!(
            bars.apply(&trade(1, 2, 1, 1, Aggressor::Sell), &mut Vec::new()),
            Err(BarError::Overflow)
        );
        // Delta overflows although both volumes fit.
        let mut bars = BarSet::new();
        bars.apply(&trade(0, 1, 1, i64::MIN, Aggressor::Buy), &mut Vec::new())
            .unwrap();
        assert_eq!(
            bars.apply(&trade(1, 2, 1, 1, Aggressor::Sell), &mut Vec::new()),
            Err(BarError::Overflow)
        );
    }

    #[test]
    fn one_event_closes_at_most_the_bound() {
        let bound = i64::try_from(MAX_BARS_PER_EVENT).unwrap();
        // 31 days after a trade at 0: exactly the bound of 1m bars closes.
        for last in [bound * 60_000, (bound + 1) * 60_000 - 1] {
            let (_, closed) = run(&[buy(0, 1), buy(last, 2)]);
            assert_eq!(of(&closed[1], Timeframe::M1).len(), 44_640);
        }
        let mut bars = BarSet::new();
        bars.apply(&buy(0, 1), &mut Vec::new()).unwrap();
        let before = bars;
        let too_far = (bound + 1) * 60_000;
        let too_many = Err(BarError::TooManyBars {
            timeframe: Timeframe::M1,
            from: t(0),
            bars: 44_641,
        });
        assert_eq!(bars.apply(&buy(too_far, 2), &mut Vec::new()), too_many);
        let mut bars = before;
        let gap = gap(Stream::Trades, 1, too_far, GapReason::Disconnected);
        assert_eq!(bars.apply(&gap, &mut Vec::new()), too_many);
        // Nothing was built for the rejected events.
        let mut bars = before;
        let mut closed = Vec::new();
        bars.apply(&buy(too_far, 2), &mut closed).unwrap_err();
        assert!(closed.is_empty());
    }

    #[test]
    fn far_jumps_are_rejected_without_building_bars() {
        for (from, to) in [
            // A plausible ms timestamp after a trade at the epoch.
            (0, 1_700_000_000_000),
            // A microsecond timestamp read as milliseconds.
            (1_700_000_000_000, 1_700_000_000_000_000),
            (-4_000_000_000_000_000_000, i64::MAX - DAY),
        ] {
            let mut bars = BarSet::new();
            bars.apply(&buy(from, 1), &mut Vec::new()).unwrap();
            let mut closed = Vec::new();
            let result = bars.apply(&buy(to, 2), &mut closed);
            assert!(
                matches!(result, Err(BarError::TooManyBars { timeframe: Timeframe::M1, bars, .. }) if bars > MAX_BARS_PER_EVENT),
                "{from} -> {to}: {result:?}"
            );
            assert!(closed.is_empty());
        }
        let bar = Bar::empty(Timeframe::M1, t(i64::MIN + 60_000));
        assert_eq!(bars_to_close(&bar, t(i64::MAX)), u64::MAX);
        assert_eq!(bars_to_close(&bar, t(i64::MIN + 60_000)), 0);
    }

    /// Deterministic 64-bit LCG (Knuth's MMIX constants) for test tapes.
    pub(crate) struct Lcg(pub(crate) u64);

    impl Lcg {
        pub(crate) fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 11
        }

        pub(crate) fn below(&mut self, bound: u64) -> i64 {
            i64::try_from(self.next() % bound).unwrap()
        }
    }

    /// A multi-day tape: trades with jumps of up to six hours, trades gaps
    /// that never reach back before the last event, and mark prices.
    pub(crate) fn random_tape(seed: u64, len: usize) -> Vec<MarketEvent> {
        let mut lcg = Lcg(seed);
        let mut time = lcg.below(DAY as u64);
        let mut trade_id = 0;
        let mut events = Vec::new();
        while events.len() < len {
            time += if lcg.below(50) == 0 {
                lcg.below(6 * 3_600_000)
            } else {
                lcg.below(90_000)
            };
            match lcg.below(40) {
                0 => {
                    let start = time + lcg.below(120_000);
                    time = start + lcg.below(1_800_000);
                    events.push(gap(Stream::Trades, start, time, GapReason::Disconnected));
                }
                1 => events.push(mark(time, 1)),
                _ => {
                    trade_id += 1;
                    let aggressor = if lcg.below(2) == 0 {
                        Aggressor::Buy
                    } else {
                        Aggressor::Sell
                    };
                    events.push(trade(
                        time,
                        trade_id,
                        6_000_000_000_000 + lcg.below(100_000_000_000),
                        1 + lcg.below(500_000_000),
                        aggressor,
                    ));
                }
            }
        }
        events
    }

    /// Folds bars of one interval into the bar of `timeframe` opening at
    /// `open`: the independent oracle for higher timeframes.
    fn fold(timeframe: Timeframe, open: EventTime, parts: &[Bar]) -> Bar {
        let mut bar = Bar::empty(timeframe, open);
        for part in parts {
            if let Some(ohlc) = part.ohlc {
                bar.ohlc = Some(match bar.ohlc {
                    None => ohlc,
                    Some(sum) => Ohlc {
                        open: sum.open,
                        high: sum.high.max(ohlc.high),
                        low: sum.low.min(ohlc.low),
                        close: ohlc.close,
                    },
                });
            }
            bar.volume = bar.volume.checked_add(part.volume).unwrap();
            bar.buy_volume = bar.buy_volume.checked_add(part.buy_volume).unwrap();
            bar.sell_volume = bar.sell_volume.checked_add(part.sell_volume).unwrap();
            bar.delta = bar.delta.checked_add(part.delta).unwrap();
            bar.trade_count += part.trade_count;
            bar.coverage.partial_start |= part.coverage.partial_start;
            bar.coverage.feed_gap |= part.coverage.feed_gap;
        }
        bar
    }

    #[test]
    fn developing_bars_never_look_ahead() {
        let tape = random_tape(0x6d69_6500_0000_0015, 3_000);
        let mut bars = BarSet::new();
        let mut trades: Vec<Trade> = Vec::new();
        let mut last_trades_event = None;
        for event in &tape {
            let mut closed = Vec::new();
            bars.apply(event, &mut closed).unwrap();
            if let MarketEvent::Trade(trade) = event {
                trades.push(*trade);
            }
            if event.stream() == Stream::Trades {
                last_trades_event = Some(event.time());
            }
            for bar in &closed {
                assert!(bar.end() <= event.time(), "{bar} closed by {event:?}");
            }
            let Some(now) = last_trades_event else {
                continue;
            };
            for series in bars.iter() {
                let bar = series.developing().ready().unwrap();
                // Only trades-stream events move the developing bar (D2).
                assert!(bar.open_time <= now && now < bar.end(), "{bar} at {now}");
                let mut expected = Bar::empty(bar.timeframe, bar.open_time);
                for trade in trades
                    .iter()
                    .filter(|trade| bar.open_time <= trade.time && trade.time < bar.end())
                {
                    expected.add_trade(trade).unwrap();
                }
                expected.coverage = bar.coverage;
                assert_eq!(*bar, expected, "after {event:?}");
            }
        }
    }

    #[test]
    fn higher_timeframes_fold_their_minutes() {
        for seed in [1, 2, 3] {
            let tape = random_tape(seed, 4_000);
            let (_, closed) = run(&tape);
            let closed: Vec<Bar> = closed.into_iter().flatten().collect();
            let minutes = of(&closed, Timeframe::M1);
            let mut checked = 0;
            for timeframe in &Timeframe::ALL[1..] {
                for bar in of(&closed, *timeframe) {
                    // Minutes close in order, so the interval is a slice.
                    let from = minutes.partition_point(|minute| minute.open_time < bar.open_time);
                    let to = minutes.partition_point(|minute| minute.end() <= bar.end());
                    let parts = &minutes[from..to];
                    assert_eq!(bar, fold(*timeframe, bar.open_time, parts), "seed {seed}");
                    checked += 1;
                }
            }
            assert!(of(&closed, Timeframe::D1).len() >= 2, "seed {seed}");
            assert!(checked > 100, "seed {seed}");
        }
    }

    #[test]
    fn kline_comparison_names_each_differing_field() {
        let (bars, _) = run(&[
            trade(60_000, 1, 100, 5, Aggressor::Buy),
            trade(70_000, 2, 103, 2, Aggressor::Sell),
            trade(80_000, 3, 98, 1, Aggressor::Buy),
            trade(119_999, 4, 101, 4, Aggressor::Sell),
        ]);
        let bar = developing(&bars, Timeframe::M1);
        let exact = Kline {
            open_time: t(60_000),
            close_time: t(119_999),
            open: Price::from_units(100),
            high: Price::from_units(103),
            low: Price::from_units(98),
            close: Price::from_units(101),
            volume: Qty::from_units(12),
            taker_buy_volume: Qty::from_units(6),
            trade_count: 4,
        };
        assert_eq!(kline_mismatches(&bar, &exact), []);
        // The trade count is not compared.
        let recounted = Kline {
            trade_count: 3,
            ..exact
        };
        assert_eq!(kline_mismatches(&bar, &recounted), []);
        let p = Price::from_units;
        let q = Qty::from_units;
        for (kline, field) in [
            (
                Kline {
                    open_time: t(0),
                    ..exact
                },
                KlineField::Interval,
            ),
            (
                Kline {
                    close_time: t(120_000),
                    ..exact
                },
                KlineField::Interval,
            ),
            (
                Kline {
                    open: p(99),
                    ..exact
                },
                KlineField::Open,
            ),
            (
                Kline {
                    high: p(104),
                    ..exact
                },
                KlineField::High,
            ),
            (
                Kline {
                    low: p(97),
                    ..exact
                },
                KlineField::Low,
            ),
            (
                Kline {
                    close: p(102),
                    ..exact
                },
                KlineField::Close,
            ),
            (
                Kline {
                    volume: q(13),
                    ..exact
                },
                KlineField::Volume,
            ),
            (
                Kline {
                    taker_buy_volume: q(7),
                    ..exact
                },
                KlineField::TakerBuyVolume,
            ),
        ] {
            assert_eq!(kline_mismatches(&bar, &kline), [field], "{field}");
        }
        let all_off = Kline {
            open: p(1),
            high: p(1),
            low: p(1),
            close: p(1),
            volume: q(1),
            taker_buy_volume: q(1),
            ..exact
        };
        assert_eq!(
            kline_mismatches(&bar, &all_off),
            [
                KlineField::Open,
                KlineField::High,
                KlineField::Low,
                KlineField::Close,
                KlineField::Volume,
                KlineField::TakerBuyVolume,
            ]
        );

        // An empty bar is compared on volumes only.
        let empty = Bar::empty(Timeframe::M1, t(120_000));
        let flat = Kline {
            open_time: t(120_000),
            close_time: t(179_999),
            volume: q(0),
            taker_buy_volume: q(0),
            trade_count: 0,
            ..exact
        };
        assert_eq!(kline_mismatches(&empty, &flat), []);
        assert_eq!(
            kline_mismatches(
                &empty,
                &Kline {
                    volume: q(1),
                    ..flat
                }
            ),
            [KlineField::Volume]
        );
        let names: Vec<_> = [
            KlineField::Interval,
            KlineField::Open,
            KlineField::High,
            KlineField::Low,
            KlineField::Close,
            KlineField::Volume,
            KlineField::TakerBuyVolume,
        ]
        .iter()
        .map(ToString::to_string)
        .collect();
        assert_eq!(
            names,
            [
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "taker_buy_volume"
            ]
        );
    }

    /// The golden tape, scaled to `timeframe`: in quarters `u` of a bar, a
    /// partial first bar, a trade exactly on a boundary, an empty bar, a
    /// trades gap across two bars, and a clean bar to finish.
    pub(crate) fn golden_tape(timeframe: Timeframe) -> Vec<MarketEvent> {
        let u = timeframe.millis() / 4;
        let price = |text: &str| text.parse::<Price>().unwrap().units();
        let qty = |text: &str| text.parse::<Qty>().unwrap().units();
        vec![
            trade(u + 1, 1, price("63500.0"), qty("0.5"), Aggressor::Buy),
            trade(2 * u, 2, price("63501.5"), qty("0.25"), Aggressor::Sell),
            mark(2 * u, 1),
            trade(3 * u, 3, price("63499.0"), qty("1.0"), Aggressor::Buy),
            trade(4 * u, 4, price("63500.5"), qty("0.1"), Aggressor::Sell),
            trade(7 * u, 5, price("63502.0"), qty("0.2"), Aggressor::Buy),
            trade(12 * u + 1, 6, price("63501.0"), qty("0.3"), Aggressor::Sell),
            gap(Stream::Trades, 13 * u, 17 * u, GapReason::Disconnected),
            trade(17 * u, 7, price("63500.0"), qty("0.4"), Aggressor::Buy),
            trade(20 * u, 8, price("63500.25"), qty("0.6"), Aggressor::Buy),
            trade(23 * u, 9, price("63499.75"), qty("0.05"), Aggressor::Sell),
            trade(24 * u, 10, price("63498.0"), qty("0.01"), Aggressor::Buy),
        ]
    }

    /// The closed `timeframe` bars of its golden tape, one `Display` line
    /// each.
    fn golden_lines(timeframe: Timeframe) -> Vec<String> {
        let (_, closed) = run(&golden_tape(timeframe));
        closed
            .into_iter()
            .flatten()
            .filter(|bar| bar.timeframe == timeframe)
            .map(|bar| bar.to_string())
            .collect()
    }

    #[test]
    fn golden_bars_time_1m_v1() {
        assert_eq!(
            golden_lines(Timeframe::M1),
            [
                "1m 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "1m 60000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "1m 120000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "1m 180000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "1m 240000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "1m 300000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }

    #[test]
    fn golden_bars_time_5m_v1() {
        assert_eq!(
            golden_lines(Timeframe::M5),
            [
                "5m 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "5m 300000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "5m 600000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "5m 900000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "5m 1200000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "5m 1500000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }

    #[test]
    fn golden_bars_time_15m_v1() {
        assert_eq!(
            golden_lines(Timeframe::M15),
            [
                "15m 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "15m 900000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "15m 1800000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "15m 2700000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "15m 3600000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "15m 4500000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }

    #[test]
    fn golden_bars_time_1h_v1() {
        assert_eq!(
            golden_lines(Timeframe::H1),
            [
                "1h 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "1h 3600000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "1h 7200000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "1h 10800000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "1h 14400000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "1h 18000000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }

    #[test]
    fn golden_bars_time_4h_v1() {
        assert_eq!(
            golden_lines(Timeframe::H4),
            [
                "4h 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "4h 14400000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "4h 28800000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "4h 43200000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "4h 57600000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "4h 72000000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }

    #[test]
    fn golden_bars_time_1d_v1() {
        assert_eq!(
            golden_lines(Timeframe::D1),
            [
                "1d 0ms ohlc=63500.00000000/63501.50000000/63499.00000000/63499.00000000 vol=1.75000000 buy=1.50000000 sell=0.25000000 delta=1.25000000 trades=3 partial_start",
                "1d 86400000ms ohlc=63500.50000000/63502.00000000/63500.50000000/63502.00000000 vol=0.30000000 buy=0.20000000 sell=0.10000000 delta=0.10000000 trades=2 complete",
                "1d 172800000ms ohlc=- vol=0.00000000 buy=0.00000000 sell=0.00000000 delta=0.00000000 trades=0 complete",
                "1d 259200000ms ohlc=63501.00000000/63501.00000000/63501.00000000/63501.00000000 vol=0.30000000 buy=0.00000000 sell=0.30000000 delta=-0.30000000 trades=1 feed_gap",
                "1d 345600000ms ohlc=63500.00000000/63500.00000000/63500.00000000/63500.00000000 vol=0.40000000 buy=0.40000000 sell=0.00000000 delta=0.40000000 trades=1 feed_gap",
                "1d 432000000ms ohlc=63500.25000000/63500.25000000/63499.75000000/63499.75000000 vol=0.65000000 buy=0.60000000 sell=0.05000000 delta=0.55000000 trades=2 complete",
            ]
        );
    }
}
