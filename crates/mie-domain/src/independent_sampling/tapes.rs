//! A seeded tape generator for the #146 edge classes, on the in-house
//! [`Lcg`].
//!
//! The recipes come from the ADRs the classes are defined by, not from the
//! sampler:
//!
//! - **S1, bridge (class 1)** on `trigger.cvd_divergence.15m@1`: a run of
//!   15m bars whose lows rise strictly (so no swing low forms, ADR-037 D2)
//!   while every fourth bar's high is a swing high. The high-side pair
//!   (h0, h1) diverges and opens the episode C; (h1, h2) is a lower high and
//!   does not diverge; (h2, h3) diverges after C's data and opens O; then a
//!   drop makes the next swing low, and the low-side pair (l_prev, l_cur),
//!   whose earlier anchor lies inside C's data, diverges after O opened. Its
//!   span overlaps O and C. CVD is steered by one large trade per segment,
//!   because ADR-051 D4's comparison is strict.
//! - **S2, padded edges (class 2):** adaptive. The builder drives a
//!   `MarketStateEngine` while it generates, and puts a minute's last trade
//!   exactly on the padded edge (`low − w` or `high + w`, `w` = 6 bps of the
//!   level price, ADR-044 D3) of a Ready prior-day profile level, or one
//!   unit inside or outside it.
//! - **S3, out-of-order spans (class 3):** S1's low-side pair, and the
//!   interleaved sides of every divergence id.
//! - **S4, empty-only close (class 4):** a trades `FeedGap` closes the last
//!   minute with trades (a gap's event time is its `end`), and the next
//!   trade comes at least two minutes later (ADR-054 D3, ADR-031).
//! - **S5, confirmation across a trades gap (class 5):** a diverging swing
//!   high whose three confirming 15m bars fall inside a trades gap, so the
//!   gap event closes the confirming bar (ADR-054 D4, ADR-037 D3).
//! - **S6, truncation boundary (class 6):** late observers that start at an
//!   event on a minute boundary equal to the `first` of an opener, picked by
//!   a model-only first pass.
//! - **Fuzz:** a random walk with regime switches, trades gaps under a
//!   minute, of several minutes and of 45 minutes or more, open interest on
//!   and off its 5-minute boundary, and trades at exact minute starts.

use super::harness::{Start, Tape};
use super::model::{Model, PADDING_BPS};
use crate::bars::tests::Lcg;
use crate::event::{Aggressor, FeedGap, GapReason, MarketEvent, OpenInterest, Stream, Trade};
use crate::feature::FeatureValue;
use crate::num::{Price, Qty, SCALE};
use crate::order::CanonicalKey;
use crate::state::MarketStateEngine;
use crate::time::EventTime;

const MINUTE: i64 = 60_000;
const BAR_15M: i64 = 15 * MINUTE;
const HOUR: i64 = 3_600_000;
const DAY: i64 = 86_400_000;
const OI_GRID: i64 = 300_000;
/// One cent in price units.
const CENT: i64 = SCALE / 100;
/// One USDT in price units.
const USD: i64 = SCALE;

/// Builds one tape in canonical order; optionally applies every event to an
/// engine as it goes (S2 reads the committed state).
pub(super) struct TapeBuilder {
    lcg: Lcg,
    events: Vec<MarketEvent>,
    engine: Option<MarketStateEngine>,
    last_key: Option<CanonicalKey>,
    /// The time of the last trade, ms.
    time: i64,
    /// The last trade price, in price units.
    price: i64,
    trade_id: u64,
    /// The next open-interest boundary and the open interest, in units.
    boundary: i64,
    oi: i64,
    /// Random-walk regime: trend and noise in cents per trade, spacing in ms.
    trend: i64,
    noise: i64,
    spacing: (i64, i64),
}

/// One planned 15m bar: low and high relative to the scenario's base, in
/// cents; the trade slots (of 30) that set them; and a signed steering
/// quantity in BTC (buy positive) traded at the bar's middle.
#[derive(Debug, Clone, Copy)]
struct Bar15 {
    low: i64,
    high: i64,
    low_at: usize,
    high_at: usize,
    steer: i64,
}

const fn bar(low: i64, high: i64, low_at: usize, high_at: usize, steer: i64) -> Bar15 {
    Bar15 {
        low,
        high,
        low_at,
        high_at,
        steer,
    }
}

impl TapeBuilder {
    pub(super) fn new(seed: u64, start: i64, adaptive: bool) -> Self {
        let mut lcg = Lcg(seed);
        let price = (60_000 * 100 + lcg.below(200_000) - 100_000) * CENT;
        Self {
            lcg,
            events: Vec::new(),
            engine: adaptive.then(MarketStateEngine::new),
            last_key: None,
            time: start,
            price,
            trade_id: 0,
            boundary: (start / OI_GRID + 1) * OI_GRID,
            oi: 80_000 * SCALE,
            trend: 0,
            noise: 500,
            spacing: (20_000, 60_000),
        }
    }

    fn push(&mut self, event: MarketEvent) {
        let key = event.canonical_key();
        assert!(
            self.last_key.is_none_or(|last| last < key),
            "the generator emits in canonical order: {key} after {:?}",
            self.last_key
        );
        self.last_key = Some(key);
        if let Some(engine) = self.engine.as_mut() {
            engine.apply(&event).expect("a generated event is accepted");
        }
        self.events.push(event);
    }

    /// Open-interest polls for every boundary before `time`, some of them a
    /// few seconds off the boundary (ADR-042 D2 accepts a sample up to
    /// 60 s after it).
    fn open_interest_until(&mut self, time: i64) {
        while self.boundary < time {
            self.oi += (self.lcg.below(2_001) - 1_000) * 10_000_000;
            let mut at = self.boundary;
            if self.lcg.below(8) == 0 {
                at += 1 + self.lcg.below(9_000);
            }
            if at >= time {
                at = self.boundary;
            }
            self.push(MarketEvent::OpenInterest(OpenInterest {
                time: EventTime::from_millis(at),
                open_interest: Qty::from_units(self.oi),
                resolution_ms: 300_000,
            }));
            self.boundary += OI_GRID;
        }
    }

    /// One trade at `time` (strictly after the last), `price` in units.
    fn trade(&mut self, time: i64, price: i64, qty: i64, aggressor: Aggressor) {
        assert!(
            time > self.time || self.trade_id == 0,
            "trades are strictly ordered"
        );
        self.open_interest_until(time);
        self.trade_id += 1;
        self.time = time;
        self.price = price;
        self.push(MarketEvent::Trade(Trade {
            time: EventTime::from_millis(time),
            trade_id: self.trade_id,
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
            aggressor,
        }));
    }

    fn noise_qty(&mut self) -> (i64, Aggressor) {
        let qty = 1 + self.lcg.below(2_000_000);
        let aggressor = if self.lcg.below(2) == 0 {
            Aggressor::Buy
        } else {
            Aggressor::Sell
        };
        (qty, aggressor)
    }

    /// A random-walk trade at `time`.
    fn walk_trade(&mut self, time: i64) {
        if self.lcg.below(400) == 0 {
            self.trend = self.lcg.below(7) - 3;
        }
        if self.lcg.below(600) == 0 {
            self.noise = 100 + self.lcg.below(1_500);
            self.spacing = match self.lcg.below(3) {
                0 => (2_000, 15_000),
                1 => (20_000, 60_000),
                _ => (30_000, 140_000),
            };
        }
        let step = self.trend + self.lcg.below(2 * self.noise as u64 + 1) - self.noise;
        let price = self.price + step * CENT;
        let (qty, aggressor) = self.noise_qty();
        self.trade(time, price, qty, aggressor);
    }

    /// The next walk trade time: random spacing, sometimes exactly at the
    /// next minute start (class 6 needs events on minute boundaries).
    fn next_time(&mut self) -> i64 {
        let (low, high) = self.spacing;
        if self.lcg.below(10) == 0 {
            let next = (self.time / MINUTE + 1) * MINUTE;
            if next > self.time {
                return next;
            }
        }
        self.time + low + self.lcg.below((high - low) as u64)
    }

    /// Walks until just before `until`.
    pub(super) fn walk(&mut self, until: i64) {
        loop {
            let time = self.next_time();
            if time >= until {
                return;
            }
            self.walk_trade(time);
        }
    }

    /// A trades gap from just after the last trade to `end`, then a trade at
    /// or after `resume`.
    fn gap(&mut self, end: i64, resume: i64) {
        self.open_interest_until(end);
        self.push(MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: EventTime::from_millis(self.time + 1),
            end: EventTime::from_millis(end),
            reason: GapReason::Disconnected,
        }));
        let time = resume.max(end + 1);
        self.walk_trade(time);
    }

    /// Fuzz trades gaps: under a minute, several minutes, or 45 minutes and
    /// more, each with an occasional resume on an exact minute start.
    fn fuzz_gap(&mut self) {
        let length = match self.lcg.below(3) {
            0 => 1_000 + self.lcg.below(50_000),
            1 => 2 * MINUTE + self.lcg.below(8 * MINUTE as u64),
            _ => 45 * MINUTE + self.lcg.below(30 * MINUTE as u64),
        };
        let end = self.time + 1 + length;
        let resume = if self.lcg.below(4) == 0 {
            (end / MINUTE + 1) * MINUTE
        } else {
            end + 1 + self.lcg.below(30_000)
        };
        self.gap(end, resume);
    }

    /// Fuzz: walk to `until` with trades gaps about every `gap_every` ms.
    pub(super) fn fuzz(&mut self, until: i64, gap_every: i64) {
        loop {
            let leg = self.time + gap_every / 2 + self.lcg.below(gap_every as u64);
            if leg >= until {
                self.walk(until);
                return;
            }
            self.walk(leg);
            self.fuzz_gap();
        }
    }

    /// S4: a trades gap that closes the minute of the last trade, then the
    /// next trade two to four minutes later, so it closes empty minutes only.
    pub(super) fn empty_only_close(&mut self) {
        let minute_end = (self.time / MINUTE + 1) * MINUTE;
        let end = minute_end + 1 + self.lcg.below(30_000);
        let resume = minute_end + 2 * MINUTE + 1 + self.lcg.below(2 * MINUTE as u64);
        self.gap(end, resume);
    }

    /// 15m bars from the next 15m boundary, relative to the current price
    /// rounded to the cent; 30 trades a bar.
    fn bars(&mut self, plan: &[Bar15]) -> i64 {
        let open = (self.time / BAR_15M + 1) * BAR_15M;
        self.walk(open);
        let base = self.price / CENT;
        for (index, planned) in plan.iter().enumerate() {
            let bar_open = open + index as i64 * BAR_15M;
            for slot in 0..30 {
                let time =
                    (bar_open + slot as i64 * 30_000 + self.lcg.below(20_000)).max(self.time + 1);
                let (qty, aggressor) = self.noise_qty();
                let cents = if slot == planned.low_at {
                    planned.low
                } else if slot == planned.high_at {
                    planned.high
                } else {
                    planned.low + 1 + self.lcg.below((planned.high - planned.low - 1) as u64)
                };
                let price = (base + cents) * CENT;
                if slot == 15 && planned.steer != 0 {
                    let aggressor = if planned.steer > 0 {
                        Aggressor::Buy
                    } else {
                        Aggressor::Sell
                    };
                    self.trade(time, price, planned.steer.abs() * SCALE, aggressor);
                } else {
                    self.trade(time, price, qty, aggressor);
                }
            }
        }
        open + plan.len() as i64 * BAR_15M
    }

    /// S1: the bridge recipe of the module docs, in cents above the base.
    /// Bars −7..16: the preamble makes a swing low `l_before` below every
    /// later low (so the pair `(l_before, l_prev)` is a higher low and does
    /// not diverge), then the peaks h0, h1, h2, h3 at bars 0, 4, 8, 12, the
    /// swing low `l_prev` at bar 2, lows rising strictly through bar 12, and
    /// the drop to `l_cur` at bar 13.
    pub(super) fn bridge(&mut self) {
        const D: i64 = 100; // one USDT in cents
        let rising = |i: i64| 90 * (i - 2);
        let plan = [
            // Preamble, bars −7..−1.
            bar(2 * D, 6 * D, 3, 20, 0),
            bar(2 * D, 6 * D, 5, 21, 0),
            bar(2 * D, 6 * D, 7, 22, 0),
            bar(-6 * D, 5 * D, 10, 2, 0), // l_before
            bar(3 * D, 6 * D, 4, 20, 0),
            bar(5 * D / 2, 6 * D, 6, 21, 0),
            bar(2 * D, 6 * D, 8, 22, 0),
            // Bar 0: h0. Bar 1 sells 100 BTC, bar 3 buys 50.
            bar(D, 10 * D, 25, 12, 0),
            bar(D / 2, 7 * D / 2, 20, 5, -100),
            bar(0, 3 * D, 12, 25, 0), // l_prev
            bar(rising(3), rising(3) + 3 * D, 3, 20, 50),
            // Bar 4: h1, higher than h0 on lower CVD: (h0, h1) diverges.
            bar(rising(4), 14 * D, 3, 12, 0),
            bar(rising(5), rising(5) + 3 * D, 3, 20, 0),
            bar(rising(6), rising(6) + 3 * D, 3, 20, 250),
            bar(rising(7), rising(7) + 3 * D, 3, 20, 0),
            // Bar 8: h2, a lower high (no divergence), mid-bar so it lies
            // after C's data.
            bar(rising(8), 12 * D, 3, 12, 0),
            bar(rising(9), rising(9) + 3 * D, 3, 20, 0),
            bar(rising(10), rising(10) + 3 * D, 3, 20, -100),
            bar(rising(11), rising(11) + 3 * D, 3, 20, 0),
            // Bar 12: h3, higher than h2 on lower CVD: (h2, h3) diverges.
            bar(rising(12), 16 * D, 3, 12, 0),
            // Bar 13: l_cur, below l_prev on higher CVD; bars 14–16 stay
            // above it and below h3.
            bar(-2 * D, 8 * D, 12, 3, 0),
            bar(-3 * D / 2, 4 * D, 3, 20, 0),
            bar(-D, 3 * D, 3, 20, 0),
            bar(-D / 2, 2 * D, 3, 20, 0),
        ];
        self.bars(&plan);
    }

    /// S5: a swing high h_a, then a higher swing high h_b four bars later on
    /// lower CVD, and a trades gap over h_b's three confirming bars; the gap
    /// event at its end closes them. Bar 2 is a swing low near both.
    pub(super) fn gap_confirmation(&mut self) {
        const D: i64 = 100;
        let plan = [
            bar(2 * D, 5 * D, 3, 20, 0),
            bar(2 * D, 5 * D, 5, 21, 0),
            bar(2 * D, 5 * D, 7, 22, 0),
            bar(3 * D / 2, 10 * D, 3, 12, 0), // h_a
            bar(D, 4 * D, 3, 20, 0),
            bar(0, 3 * D, 12, 20, -100), // swing low
            bar(D, 4 * D, 3, 20, 0),
            bar(2 * D, 14 * D, 3, 12, 0), // h_b
        ];
        let end = self.bars(&plan);
        // The gap starts after h_b's bar and ends past its third confirming
        // bar.
        let gap_end = end + 3 * BAR_15M + 1 + self.lcg.below(5 * MINUTE as u64);
        let resume = gap_end + 1 + self.lcg.below(20_000);
        self.gap(gap_end, resume);
    }

    /// S2: for `minutes` minutes, put every fourth minute's last trade on the
    /// padded edge of the Ready prior-day profile level nearest the current
    /// price (or one unit inside or outside it).
    pub(super) fn padded_edges(&mut self, minutes: i64) {
        let start = (self.time / MINUTE + 1) * MINUTE;
        for minute in 0..minutes {
            let open = start + minute * MINUTE;
            let close = open + MINUTE;
            self.walk(close - 2_000);
            if minute % 4 != 3 {
                continue;
            }
            if let Some(edge) = self.edge_price() {
                let offset = match self.lcg.below(4) {
                    0 => -1,
                    1 => 1,
                    _ => 0,
                };
                let (qty, aggressor) = self.noise_qty();
                self.trade(close - 1_000, edge + offset, qty, aggressor);
            }
        }
    }

    /// The padded edge of the nearest Ready prior-day profile level, within
    /// 40 USDT of the current price.
    fn edge_price(&mut self) -> Option<i64> {
        let engine = self.engine.as_ref().expect("an adaptive builder");
        let FeatureValue::Ready(set) = &engine.state().location.levels else {
            return None;
        };
        let price = self.price;
        set.levels()
            .iter()
            .filter(|level| level.source.id.as_str() == "profile.volume.prior_day")
            .filter_map(|level| {
                let pad = i128::from(level.price.units()).abs() * PADDING_BPS;
                if pad % 10_000 != 0 {
                    return None;
                }
                let w = i64::try_from(pad / 10_000).ok()?;
                let edge = if price < level.low.units() {
                    level.low.units() - w
                } else {
                    level.high.units() + w
                };
                Some(edge)
            })
            .filter(|edge| (edge - price).abs() <= 40 * USD)
            .min_by_key(|edge| (edge - price).abs())
    }

    pub(super) fn finish(self) -> Vec<MarketEvent> {
        self.events
    }

    pub(super) fn time(&self) -> i64 {
        self.time
    }
}

/// An S1 tape: a day and a half of walk, then three bridge recipes four
/// hours apart, each followed by an empty-only close.
pub(super) fn bridge_tape(seed: u64) -> Vec<MarketEvent> {
    let mut builder = TapeBuilder::new(seed, 0, false);
    builder.fuzz(DAY + 12 * HOUR, 10 * HOUR);
    for _ in 0..3 {
        builder.bridge();
        builder.walk(builder.time() + 2 * HOUR);
        builder.empty_only_close();
        builder.walk(builder.time() + 2 * HOUR);
    }
    builder.finish()
}

/// An S2 tape: a day and a quarter of walk, then six hours of padded edges.
pub(super) fn edge_tape(seed: u64) -> Vec<MarketEvent> {
    let mut builder = TapeBuilder::new(seed, 0, true);
    builder.fuzz(DAY + 6 * HOUR, 10 * HOUR);
    builder.padded_edges(6 * 60);
    builder.walk(builder.time() + HOUR);
    builder.finish()
}

/// An S5 tape: a day and a half of walk, then three gap confirmations.
pub(super) fn gap_tape(seed: u64) -> Vec<MarketEvent> {
    let mut builder = TapeBuilder::new(seed, 0, false);
    builder.fuzz(DAY + 12 * HOUR, 10 * HOUR);
    for _ in 0..3 {
        builder.gap_confirmation();
        builder.walk(builder.time() + 3 * HOUR);
    }
    builder.finish()
}

/// A fuzz tape: `days` days of walk with regime switches and trades gaps.
pub(super) fn fuzz_tape(seed: u64, days: i64) -> Vec<MarketEvent> {
    let mut builder = TapeBuilder::new(seed, 13 * HOUR, false);
    builder.fuzz(days * DAY, 4 * HOUR);
    builder.finish()
}

/// S6: late starts at events on a minute boundary that equal the `first` of
/// an opener, picked by a model-only pass; at most `limit`, past the first
/// day.
pub(super) fn truncation_starts(events: &[MarketEvent], limit: usize) -> Vec<usize> {
    let mut engine = MarketStateEngine::new();
    let mut model = Model::new(None);
    let mut starts = Vec::new();
    for event in events {
        if engine.apply(event).is_err() {
            continue;
        }
        let observation = model.observe(event, &engine);
        for assigned in &observation.assigned {
            let first = assigned.span.first.as_millis();
            if !assigned.episode.opener || first % MINUTE != 0 || first < DAY {
                continue;
            }
            let index = events.partition_point(|event| event.time().as_millis() < first);
            if index < events.len()
                && events[index].time().as_millis() == first
                && !starts.contains(&index)
            {
                starts.push(index);
            }
        }
        if starts.len() >= limit {
            break;
        }
    }
    starts
}

/// The default starts of a generated tape: full, `emit_from` at the middle
/// event, and a late observer from it.
fn starts(events: &[MarketEvent]) -> Vec<Start> {
    let middle = events.len() / 2;
    vec![
        Start::Full,
        Start::EmitFrom(events[middle].time()),
        Start::LateAt(middle),
    ]
}

fn tape(name: &str, seed: u64, events: Vec<MarketEvent>) -> Tape {
    let starts = starts(&events);
    Tape {
        name: name.into(),
        seed,
        events,
        starts,
    }
}

/// The scenario seeds and the fuzz base seed of the corpus.
pub(super) const BRIDGE_SEEDS: [u64; 2] = [0x6d69_6501_4601_0001, 0x6d69_6501_4601_0002];
pub(super) const EDGE_SEEDS: [u64; 1] = [0x6d69_6501_4602_0001];
pub(super) const GAP_SEEDS: [u64; 1] = [0x6d69_6501_4605_0001];
pub(super) const FUZZ_BASE: u64 = 0x6d69_6501_46f0_0000;
pub(super) const FUZZ_TAPES: u64 = 3;

/// The generated corpus: the pinned scenario seeds, then the fuzz batch,
/// each tape with its starts; the fuzz tapes add S6's truncation starts.
pub(super) fn corpus() -> Vec<Tape> {
    let mut tapes = Vec::new();
    for seed in BRIDGE_SEEDS {
        tapes.push(tape("S1 bridge", seed, bridge_tape(seed)));
    }
    for seed in EDGE_SEEDS {
        tapes.push(tape("S2 padded edges", seed, edge_tape(seed)));
    }
    for seed in GAP_SEEDS {
        tapes.push(tape("S5 gap confirmation", seed, gap_tape(seed)));
    }
    for k in 0..FUZZ_TAPES {
        let seed = FUZZ_BASE + k;
        let mut fuzz = tape("fuzz", seed, fuzz_tape(seed, 3));
        let late = truncation_starts(&fuzz.events, 2);
        fuzz.starts.extend(late.into_iter().map(Start::LateAt));
        tapes.push(fuzz);
    }
    tapes
}
