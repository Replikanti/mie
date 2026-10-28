//! The seasonal volatility profile of the design year (issue #155 M1, AC 1):
//! a report-only measurement that sizes the mechanism a labeling v3 would
//! rest on before any design choice is made. It computes no labeling v3
//! label and reads no data of the evaluation year: ADR-057's prediction is
//! pre-registered on that year, so the only real-data inputs before its
//! acceptance run are the engine's bars and labeling v2, whose counts on
//! the design year ADR-056 published.
//!
//! **Periods.** The design period is the #12 year, 2025-10-01 …
//! 2026-09-30, where ADR-056 saw the 4h hump; the profile is estimated
//! there. The evaluation period is ADR-051's year, 2024-10-01 …
//! 2025-09-30; every test here that replays the archive refuses a window
//! that overlaps it.
//!
//! `measure_seasonal_vol` (`--ignored`) replays the archive as
//! `label_measure`'s `measure_labels` does (aggTrades with the ADR-050 fold
//! and `metrics`), one event at a time through `label_contexts` with the
//! engine, the sampler and a forward labeler under `Labeling::V2`
//! (asserted), as `label_measure`'s noise run does, and reads:
//!
//! - **the volatility input**, per UTC 5-minute slot, from the engine's
//!   closed, complete 1m bars: the sum of the squared log close-to-close
//!   returns whose later bar opens in the slot and the minutes they span (an
//!   empty bar between two priced ones adds its minute to the next return;
//!   an incomplete bar ends the run of returns), and the sum of the squared
//!   ln(high ÷ low) of the slot's priced bars with their count (Parkinson);
//! - **v2's counts by hour**: per UTC hour of the decision, the `Neither`
//!   and the races that ran (upper + lower + neither) per horizon, and the
//!   year's `Unavailable` per horizon;
//! - **the replay identity** (asserted on the design-year window): v2's
//!   year totals equal ADR-056's published `Neither` and `Unavailable`
//!   counts.
//!
//! Run it with `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=2025-10-01
//! MIE_MEASURE_TO=2026-09-30 MIE_MEASURE_EVENTS=603323345 cargo test
//! --release -p mie-cli --test season_measure -- --ignored --exact
//! --nocapture measure_seasonal_vol`; `MIE_MEASURE_TO` is included, as in
//! `mie replay`. An optional `MIE_MEASURE_SLOTS_OUT=<file>` writes the
//! per-slot and per-hour rows, and an optional
//! `MIE_MEASURE_TABLES_OUT=<file>` the integer candidate tables.
//! `measure_seasonal_vol_from` reruns the analysis from the rows file
//! (`MIE_MEASURE_SLOTS=<file>`, optional `MIE_MEASURE_TABLES_OUT`), in
//! seconds. `measure_seasonal_vol_klines` reads the same slot sums from the
//! archive's 1m klines (`MIE_MEASURE_RAW_ROOT`, `MIE_MEASURE_FROM`,
//! `MIE_MEASURE_TO`) as a cross-check, since exchange klines can differ from
//! the engine's bars (the ADR-031 kline cross-check); with
//! `MIE_MEASURE_SLOTS` set it also compares the two hour-of-week profiles.
//!
//! **The analysis** (f64, test code only; every number is printed):
//!
//! - **Profiles**: hour-of-week (168 buckets, Monday 00:00 UTC = 0),
//!   hour-of-day (24), weekday (7) and 5-minute-of-day (288, diagnostic
//!   only), each normalized to a mean of 1 over its buckets and printed as
//!   relative variance and as ln σ, by four estimators:
//!   - *pooled*: the bucket's summed squared returns over its summed
//!     minutes;
//!   - *deflated*: the same, with each slot's squared returns divided by a
//!     28-day level (the pooled variance per minute of the 28 whole days
//!     centred on the slot's day, the window shifted inside the data at
//!     its ends): 4 whole weeks, so every hour-of-week weighs equally in
//!     the level, and slow volatility regimes drop out of the profile;
//!   - *geometric*: the bucket's mean of ln(realized variance per minute)
//!     of its hours (of its slots for the 5-minute profile), which damps
//!     single violent hours;
//!   - *Parkinson*: the pooled form over the squared 1m log ranges.
//! - **Variance shares**: the adjusted R² of hourly ln σ, raw and deflated,
//!   held by hour-of-day, weekday, their separable (additive) sum and the
//!   full hour-of-week, whose excess over the separable sum is the
//!   interaction; and the sub-hour share: the 5-minute-of-day R² of the
//!   deflated 5-minute ln σ less its hour-of-day R².
//! - **Precision**: the SE of every hour-of-week ln σ from a circular block
//!   bootstrap over the UTC days (56-day blocks, 10 000 resamples,
//!   SplitMix64 at seed 155, the issue number, named before the run); the
//!   28-day level is the year's and is not re-estimated per resample.
//! - **Stability** (a proxy for the transfer error to another year): the
//!   hour-of-week profile of the first 26 weeks (182 days; 2025-10-01 …
//!   2026-03-31 on the design year) against the rest: the correlation, the
//!   maximum and RMS difference of ln σ, and each difference over twice
//!   the year's bootstrap SE (a half holds half the days, so its SE is
//!   about √2 times the year's, and the difference of two halves about 2
//!   times).
//! - **DST**: the deflated hour-of-day profile on US daylight-saving days
//!   (second Sunday of March to the day before the first Sunday of
//!   November, by UTC date) against the rest, their correlation as is and
//!   with the standard-time profile moved one hour earlier, and the
//!   weekday US-open peak in 12:00 … 17:00 UTC of each, 5-minute resolution.
//! - **Mechanism size per horizon**: over every minute t of the data,
//!   s_H(t) = F_H(t) ÷ L_H(t), F_H(t) = √(the mean relative variance over
//!   [t, t + H)) and L_H(t) the Wilder(14) average (ADR-033: the mean of the
//!   first 14, then (13 · L + x) ÷ 14) of √(the mean relative variance over
//!   each UTC-aligned H-bar closed by t), warmed up over the 15 days before
//!   the data (the profile repeats every week); the mean, SD and quantiles
//!   of ln s_H per horizon, for every estimator and each profile shape:
//!   hour-of-day, weekday, separable 24 × 7 (the product of the two,
//!   renormalized) and the full hour-of-week.
//! - **Predicted v3 shape**: from v2's published N(H) on the design year
//!   (ADR-056 *Measurement*), the SD of ln ρ_H under ADR-056's lognormal-ρ
//!   model (median 1, its exit-time series g(ρ)); remove var(ln s_H),
//!   assuming the seasonal and the remaining parts independent; map back
//!   through g to the predicted N_v3(H) and its max ÷ min, printed next to
//!   v2's 1.5811 and the gate 1.5811 · e^(−2 · 0.0582) (two of ADR-056's
//!   bootstrap SEs of ln(max ÷ min) below v2).
//! - **Mechanism check from the v2 labels**: per horizon, over the 168
//!   hour-of-week buckets, ln N_v2(H | bucket) against the bucket's mean
//!   ln s_H: the correlation and the OLS slope. A seasonal hump predicts a
//!   negative slope (a forward window more volatile than its ATR has fewer
//!   `Neither`).
//! - **Integer candidate tables**: per estimator and shape (the four, plus
//!   the 5-minute diagnostic), the relative variance per 10⁶ with a mean of
//!   exactly 10⁶ (largest-remainder rounding), written to
//!   `MIE_MEASURE_TABLES_OUT`, not to stdout, so ADR-057 and the code copy
//!   one printed line rather than a hand-made table.
//!
//! In CI: the hour-of-week index across the week wrap and before 1970, the
//! slot builder, the slot file round trip, the normalization, a planted
//! hour-of-week profile recovered within its bootstrap SE, the mechanism's
//! exact zero under a flat profile and at 1d under a daily one, ADR-056's
//! g(ρ) values and lognormal inversion, the US daylight-saving days of the
//! design year, the candidate tables' exact mean, and the season run on the
//! seeded tape of `common::tapes` against `label_window` under labeling v2.

mod common;

use common::bootstrap::{SplitMix64, normal, resample};
use common::tapes::{TAPE_DAYS, TAPE_START, synthetic_tape};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::{civil_from_days, day_label, days_from_civil};
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::conditional::{finish_labels, label_contexts, label_window};
use mie_cli::replay::parse_bound;
use mie_domain::bars::{Bar, Timeframe};
use mie_domain::conditional::label::{
    ForwardLabel, ForwardLabeler, HORIZONS, LABELING_V2, LabelProvenance, Labeling, Race,
};
use mie_domain::conditional::{ContextProvenance, ContextSampler};
use mie_domain::event::{FeedGap, MarketEvent};
use mie_domain::research::DataVersion;
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::collections::VecDeque;
use std::time::Instant;

const MINUTE_MS: i64 = 60_000;
const SLOT_MS: i64 = 5 * MINUTE_MS;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;
const SLOTS_PER_HOUR: usize = 12;
const SLOTS_PER_DAY: usize = 288;
const MINUTES_PER_DAY: i64 = 1_440;
const WEEK_HOURS: usize = 168;

/// ADR-055 D3: the horizons in minutes, shortest first.
const HORIZON_MINUTES: [i64; 5] = [5, 15, 60, 240, 1_440];

/// The design period (#12 year), where the profile is estimated.
const DESIGN: (&str, &str) = ("2025-10-01", "2026-09-30");

/// The evaluation period (ADR-051's year): no test here reads it.
const EVALUATION: (&str, &str) = ("2024-10-01", "2025-09-30");

/// ADR-056 *Measurement*: labeling v2 on the design year, per horizon: the
/// `Neither` and `Unavailable` counts, the races that ran, and N(H) =
/// `Neither` ÷ the races that ran, in percent.
const V2_NEITHER: [u64; 5] = [43_925, 44_593, 56_372, 68_977, 56_383];
const V2_UNAVAILABLE: [u64; 5] = [74, 224, 899, 3_670, 22_724];
const V2_RAN: [u64; 5] = [561_620, 561_465, 560_765, 557_794, 538_492];
const V2_N_PERCENT: [f64; 5] = [7.821, 7.942, 10.053, 12.366, 10.471];

/// ADR-056 *Measurement*: v2's max ÷ min N(H) on the design year and the
/// bootstrap SE of its log.
const V2_SPREAD: f64 = 1.5811;
const V2_SPREAD_SE: f64 = 0.0582;

/// The plan's go/no-go gate: the predicted v3 spread must lie this many of
/// v2's SEs of ln(max ÷ min) below v2's spread.
const GATE_SE: f64 = 2.0;

/// The bootstrap of the profile's SEs: 56-day blocks (whole weeks, so a
/// block keeps the weekly cycle intact), 10 000 resamples, and the seed
/// named before the run: the issue number.
const BLOCK_DAYS: usize = 56;
const RESAMPLES: usize = 10_000;
const SEED: u64 = 155;

/// The deflated estimator's level window, in days: 4 whole weeks.
const LEVEL_DAYS: usize = 28;

/// The stability split: the first 26 weeks against the rest.
const HALF_DAYS: usize = 182;

/// ADR-033 decision 2: the ATR length, the Wilder length of L_H.
const ATR_LENGTH: usize = 14;

/// Days of profile before the data that warm L_H up: one more than the 14
/// daily bars ATR_1d needs.
const WARM_DAYS: i64 = 15;

/// The quantiles of ln s_H the report prints.
const QUANTILES: [f64; 7] = [0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99];

/// The US-open search window of the DST diagnostic, in 5-minute slots of
/// the day: 12:00 … 17:00 UTC.
const OPEN_SLOTS: std::ops::Range<usize> = 144..204;

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

// ---------------------------------------------------------------------------
// Calendar
// ---------------------------------------------------------------------------

/// The hour of the week of `ms`, Monday 00:00 UTC = 0: 1970-01-01 was a
/// Thursday, hour 72 of its week.
fn hour_of_week(ms: i64) -> usize {
    (ms.div_euclid(HOUR_MS) + 72).rem_euclid(WEEK_HOURS as i64) as usize
}

/// The weekday of a day since 1970-01-01, Monday = 0.
fn weekday(day: i64) -> usize {
    (day + 3).rem_euclid(7) as usize
}

/// The first Sunday on or after `day`.
fn sunday_from(day: i64) -> i64 {
    day + (6 - weekday(day) as i64)
}

/// US daylight-saving time by UTC date: from the second Sunday of March to
/// the day before the first Sunday of November.
fn us_dst(day: i64) -> bool {
    let (year, _, _) = civil_from_days(day);
    let start = sunday_from(days_from_civil(year, 3, 1)) + 7;
    let end = sunday_from(days_from_civil(year, 11, 1));
    (start..end).contains(&day)
}

// ---------------------------------------------------------------------------
// The measured inputs
// ---------------------------------------------------------------------------

/// Per UTC 5-minute slot, what the estimators read from the closed 1m
/// bars.
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Slot {
    /// The sum of the squared log returns between consecutive priced,
    /// complete 1m bars' closes whose later bar opens in the slot.
    rv: f64,
    /// The minutes those returns span.
    minutes: u64,
    /// The sum of the squared ln(high ÷ low) of the slot's priced,
    /// complete 1m bars.
    range2: f64,
    /// Those bars.
    bars: u64,
}

/// The [`Slot`] of every 5-minute slot from the first closed bar's slot
/// on.
#[derive(Debug, Default)]
struct Slots {
    /// The first slot, in slots since 1970-01-01.
    first: Option<i64>,
    slots: Vec<Slot>,
    /// The open time and close of the last priced, complete 1m bar, while
    /// no incomplete one has closed since.
    last: Option<(i64, f64)>,
    /// Closed 1m bars left out as incomplete (`partial_start`,
    /// `feed_gap`).
    incomplete: u64,
}

impl Slots {
    /// The row of the slot of `open`; bars close in time order.
    fn row(&mut self, open: i64) -> usize {
        let slot = open.div_euclid(SLOT_MS);
        let first = *self.first.get_or_insert(slot);
        let row = usize::try_from(slot - first).expect("bars close in time order");
        if row >= self.slots.len() {
            self.slots.resize(row + 1, Slot::default());
        }
        row
    }

    /// One closed 1m bar opening at `open`, with its (high, low, close)
    /// when it traded. An incomplete bar ends the run of returns: the next
    /// priced bar starts a new one.
    fn bar(&mut self, open: i64, prices: Option<(f64, f64, f64)>, complete: bool) {
        if !complete {
            self.incomplete += 1;
            self.last = None;
            return;
        }
        let Some((high, low, close)) = prices else {
            return;
        };
        let row = self.row(open);
        let last = self.last.replace((open, close));
        let slot = &mut self.slots[row];
        let range = (high / low).ln();
        slot.range2 += range * range;
        slot.bars += 1;
        if let Some((before, previous)) = last {
            let ret = (close / previous).ln();
            slot.rv += ret * ret;
            slot.minutes +=
                u64::try_from((open - before) / MINUTE_MS).expect("bars close in time order");
        }
    }

    /// One closed engine bar; only 1m bars count.
    fn engine_bar(&mut self, bar: &Bar) {
        if bar.timeframe != Timeframe::M1 {
            return;
        }
        let prices = bar.ohlc.map(|ohlc| {
            (
                ohlc.high.units() as f64,
                ohlc.low.units() as f64,
                ohlc.close.units() as f64,
            )
        });
        self.bar(bar.open_time.as_millis(), prices, bar.is_complete());
    }
}

/// Labeling v2's `Neither` and races that ran per horizon, per UTC hour of
/// the decision, from the first label's hour on.
#[derive(Debug, Default, PartialEq)]
struct Hourly {
    /// The first hour, in hours since 1970-01-01.
    first: Option<i64>,
    neither: Vec<[u64; 5]>,
    ran: Vec<[u64; 5]>,
}

impl Hourly {
    /// The row of the hour of `decision`; labels come in record order, so
    /// in decision order.
    fn row(&mut self, decision: i64) -> usize {
        let hour = decision.div_euclid(HOUR_MS);
        let first = *self.first.get_or_insert(hour);
        let row = usize::try_from(hour - first).expect("labels come in decision order");
        if row >= self.ran.len() {
            self.neither.resize(row + 1, [0; 5]);
            self.ran.resize(row + 1, [0; 5]);
        }
        row
    }

    fn add(&mut self, label: &ForwardLabel) {
        let row = self.row(label.event.time.as_millis());
        for (h, horizon) in label.horizons.iter().enumerate() {
            match horizon.race {
                Race::Neither => {
                    self.neither[row][h] += 1;
                    self.ran[row][h] += 1;
                }
                Race::Upper { .. } | Race::Lower { .. } => self.ran[row][h] += 1,
                Race::Unavailable | Race::Censored(_) => {}
            }
        }
    }

    fn totals(&self) -> ([u64; 5], [u64; 5]) {
        let sum = |rows: &[[u64; 5]]| -> [u64; 5] {
            std::array::from_fn(|h| rows.iter().map(|row| row[h]).sum())
        };
        (sum(&self.neither), sum(&self.ran))
    }

    /// The `Neither` and races that ran per hour-of-week bucket.
    fn by_hour_of_week(&self) -> ([[u64; 5]; WEEK_HOURS], [[u64; 5]; WEEK_HOURS]) {
        let (mut neither, mut ran) = ([[0; 5]; WEEK_HOURS], [[0; 5]; WEEK_HOURS]);
        if let Some(first) = self.first {
            for (i, (n, r)) in self.neither.iter().zip(&self.ran).enumerate() {
                let bucket = hour_of_week((first + i as i64) * HOUR_MS);
                for h in 0..5 {
                    neither[bucket][h] += n[h];
                    ran[bucket][h] += r[h];
                }
            }
        }
        (neither, ran)
    }
}

/// Everything the analysis reads, and what the rows file holds.
#[derive(Debug, Default)]
struct Inputs {
    slots: Slots,
    hourly: Hourly,
    /// The year's `Unavailable` races per horizon.
    unavailable: [u64; 5],
}

/// The first line of a rows file.
const ROWS_HEADER: &str = "# season_measure rows: slot <slot> <rv> <minutes> <range2> <bars> | \
                           hour <hour> <neither x5> <ran x5> | unavailable <x5> | incomplete <n>";

/// The inputs as text, so the analysis can be rerun without the replay;
/// slots and hours count from 1970-01-01.
fn rows_text(inputs: &Inputs) -> String {
    let mut text = format!("{ROWS_HEADER}\n");
    let join = |row: &[u64; 5]| row.map(|n| n.to_string()).join("\t");
    if let Some(first) = inputs.slots.first {
        for (i, slot) in inputs.slots.slots.iter().enumerate() {
            text.push_str(&format!(
                "slot\t{}\t{}\t{}\t{}\t{}\n",
                first + i as i64,
                slot.rv,
                slot.minutes,
                slot.range2,
                slot.bars
            ));
        }
    }
    if let Some(first) = inputs.hourly.first {
        for (i, (neither, ran)) in inputs
            .hourly
            .neither
            .iter()
            .zip(&inputs.hourly.ran)
            .enumerate()
        {
            text.push_str(&format!(
                "hour\t{}\t{}\t{}\n",
                first + i as i64,
                join(neither),
                join(ran)
            ));
        }
    }
    text.push_str(&format!("unavailable\t{}\n", join(&inputs.unavailable)));
    text.push_str(&format!("incomplete\t{}\n", inputs.slots.incomplete));
    text
}

/// Reads [`rows_text`] back.
fn parse_rows(text: &str) -> Inputs {
    let mut inputs = Inputs::default();
    for line in text.lines().filter(|line| !line.starts_with('#')) {
        let fields: Vec<&str> = line.split('\t').collect();
        let number = |i: usize| -> u64 { fields[i].parse().expect("a count") };
        let index = || -> i64 { fields[1].parse().expect("an index") };
        match fields[0] {
            "slot" => {
                let row = inputs.slots.row(index() * SLOT_MS);
                inputs.slots.slots[row] = Slot {
                    rv: fields[2].parse().expect("a variance"),
                    minutes: number(3),
                    range2: fields[4].parse().expect("a variance"),
                    bars: number(5),
                };
            }
            "hour" => {
                let row = inputs.hourly.row(index() * HOUR_MS);
                inputs.hourly.neither[row] = std::array::from_fn(|h| number(2 + h));
                inputs.hourly.ran[row] = std::array::from_fn(|h| number(7 + h));
            }
            "unavailable" => inputs.unavailable = std::array::from_fn(|h| number(1 + h)),
            "incomplete" => inputs.slots.incomplete = number(1),
            other => panic!("unknown line kind {other}"),
        }
    }
    inputs
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// A provider of one event.
struct One(Option<MarketEvent>);

impl MarketDataProvider for One {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.take())
    }
}

/// One drive: the engine, the sampler and a labeler under labeling v2,
/// with the engine's closed 1m bars per slot and v2's counts per hour. It
/// computes no labeling v3 label.
struct SeasonRun {
    engine: MarketStateEngine,
    sampler: ContextSampler,
    labeler: ForwardLabeler,
    events: u64,
    rejections: u64,
    labels: u64,
    inputs: Inputs,
}

impl SeasonRun {
    fn new() -> Self {
        Self {
            engine: MarketStateEngine::new(),
            sampler: ContextSampler::new(None),
            labeler: ForwardLabeler::new(Labeling::V2),
            events: 0,
            rejections: 0,
            labels: 0,
            inputs: Inputs::default(),
        }
    }

    fn step(&mut self, event: MarketEvent) {
        let mut labels = Vec::new();
        let report = label_contexts(
            &mut One(Some(event)),
            &mut self.engine,
            &mut self.sampler,
            &mut self.labeler,
            |_| {},
            |_| {},
            |label| labels.push(label),
        )
        .expect("a one-event provider cannot fail");
        self.events += report.events;
        self.rejections += report.rejections;
        // A rejected event leaves the last accepted event's bars there.
        if report.rejections == 0 {
            for bar in self.engine.closed_bars() {
                self.inputs.slots.engine_bar(bar);
            }
        }
        for label in &labels {
            self.add(label);
        }
    }

    fn finish(&mut self, trailing: &[FeedGap]) {
        let mut labels = Vec::new();
        finish_labels(&mut self.labeler, trailing, |label| labels.push(label))
            .expect("the trailing gaps keep the driver contract (ADR-055 D6)");
        for label in &labels {
            self.add(label);
        }
    }

    fn add(&mut self, label: &ForwardLabel) {
        self.labels += 1;
        self.inputs.hourly.add(label);
        for (h, horizon) in label.horizons.iter().enumerate() {
            self.inputs.unavailable[h] += u64::from(horizon.race == Race::Unavailable);
        }
    }
}

/// Prints v2's year totals next to ADR-056's published counts; whether the
/// `Neither` and `Unavailable` counts equal them.
fn report_identity(inputs: &Inputs) -> bool {
    let (neither, ran) = inputs.hourly.totals();
    println!("labeling v2 per horizon, against ADR-056 *Measurement* (the design year):");
    for (h, horizon) in HORIZONS.iter().enumerate() {
        let n = if ran[h] == 0 {
            "-".to_owned()
        } else {
            format!("{:.3} %", neither[h] as f64 * 100.0 / ran[h] as f64)
        };
        println!(
            "  {horizon}: neither {} (published {}), ran {} (published {}), N {n} (published {} \
             %), unavailable {} (published {})",
            neither[h],
            V2_NEITHER[h],
            ran[h],
            V2_RAN[h],
            V2_N_PERCENT[h],
            inputs.unavailable[h],
            V2_UNAVAILABLE[h]
        );
    }
    let holds = neither == V2_NEITHER && inputs.unavailable == V2_UNAVAILABLE;
    println!("replay identity (neither and unavailable equal the published counts): {holds}");
    holds
}

// ---------------------------------------------------------------------------
// The year in whole days
// ---------------------------------------------------------------------------

/// The slots padded to whole UTC days.
struct Year {
    first_day: i64,
    days: usize,
    /// `days` × 288 slots.
    slots: Vec<Slot>,
}

impl Year {
    fn of(slots: &Slots) -> Option<Self> {
        let first = slots.first?;
        let first_day = (first * SLOT_MS).div_euclid(DAY_MS);
        let offset = usize::try_from(first - first_day * SLOTS_PER_DAY as i64).ok()?;
        let days = (offset + slots.slots.len()).div_ceil(SLOTS_PER_DAY);
        let mut padded = vec![Slot::default(); days * SLOTS_PER_DAY];
        padded[offset..offset + slots.slots.len()].copy_from_slice(&slots.slots);
        Some(Self {
            first_day,
            days,
            slots: padded,
        })
    }

    /// The slot sums of `count` slots from slot `from` of day `day`.
    fn sum(&self, day: usize, from: usize, count: usize) -> Slot {
        let start = day * SLOTS_PER_DAY + from;
        self.slots[start..start + count]
            .iter()
            .fold(Slot::default(), |sum, slot| Slot {
                rv: sum.rv + slot.rv,
                minutes: sum.minutes + slot.minutes,
                range2: sum.range2 + slot.range2,
                bars: sum.bars + slot.bars,
            })
    }

    /// Per day, the deflated estimator's level: the pooled variance per
    /// minute over the [`LEVEL_DAYS`] whole days centred on the day,
    /// shifted to stay inside the data; `None` without a nonzero return.
    fn levels(&self) -> Vec<Option<f64>> {
        let daily: Vec<Slot> = (0..self.days)
            .map(|day| self.sum(day, 0, SLOTS_PER_DAY))
            .collect();
        (0..self.days)
            .map(|day| {
                let start = day
                    .saturating_sub(LEVEL_DAYS / 2)
                    .min(self.days.saturating_sub(LEVEL_DAYS));
                let end = (start + LEVEL_DAYS).min(self.days);
                let (rv, minutes) = daily[start..end].iter().fold((0.0, 0), |(rv, minutes), d| {
                    (rv + d.rv, minutes + d.minutes)
                });
                (minutes > 0 && rv > 0.0).then(|| rv / minutes as f64)
            })
            .collect()
    }

    fn day_ms(&self, day: usize) -> i64 {
        (self.first_day + day as i64) * DAY_MS
    }
}

// ---------------------------------------------------------------------------
// Profiles
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Shape {
    HourOfWeek,
    HourOfDay,
    Weekday,
    SlotOfDay,
}

impl Shape {
    const ALL: [Shape; 4] = [
        Shape::HourOfWeek,
        Shape::HourOfDay,
        Shape::Weekday,
        Shape::SlotOfDay,
    ];

    fn buckets(self) -> usize {
        match self {
            Shape::HourOfWeek => WEEK_HOURS,
            Shape::HourOfDay => 24,
            Shape::Weekday => 7,
            Shape::SlotOfDay => SLOTS_PER_DAY,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Shape::HourOfWeek => "hour-of-week",
            Shape::HourOfDay => "hour-of-day",
            Shape::Weekday => "weekday",
            Shape::SlotOfDay => "5-minute-of-day",
        }
    }

    fn bucket(self, ms: i64) -> usize {
        match self {
            Shape::HourOfWeek => hour_of_week(ms),
            Shape::HourOfDay => ms.div_euclid(HOUR_MS).rem_euclid(24) as usize,
            Shape::Weekday => hour_of_week(ms) / 24,
            Shape::SlotOfDay => ms.div_euclid(SLOT_MS).rem_euclid(SLOTS_PER_DAY as i64) as usize,
        }
    }

    /// The slots of one unit: an hour, or a slot for the 5-minute profile.
    fn unit_slots(self) -> usize {
        if self == Shape::SlotOfDay {
            1
        } else {
            SLOTS_PER_HOUR
        }
    }

    fn label(self, bucket: usize) -> String {
        match self {
            Shape::HourOfWeek => format!("{} {:02}h", WEEKDAYS[bucket / 24], bucket % 24),
            Shape::HourOfDay => format!("{bucket:02}h"),
            Shape::Weekday => WEEKDAYS[bucket].to_owned(),
            Shape::SlotOfDay => format!("{:02}:{:02}", bucket / 12, bucket % 12 * 5),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Estimator {
    Pooled,
    Deflated,
    Geometric,
    Parkinson,
}

impl Estimator {
    const ALL: [Estimator; 4] = [
        Estimator::Pooled,
        Estimator::Deflated,
        Estimator::Geometric,
        Estimator::Parkinson,
    ];

    fn name(self) -> &'static str {
        match self {
            Estimator::Pooled => "pooled",
            Estimator::Deflated => "deflated",
            Estimator::Geometric => "geometric",
            Estimator::Parkinson => "parkinson",
        }
    }
}

/// One unit's contribution to its bucket: the bucket, a numerator and a
/// denominator.
type Cell = (usize, f64, f64);

/// Per day, the cells of `estimator` under `shape`. A unit without the
/// estimator's input (no return, no level, no priced bar) adds none.
fn cells(
    year: &Year,
    levels: &[Option<f64>],
    shape: Shape,
    estimator: Estimator,
) -> Vec<Vec<Cell>> {
    let width = shape.unit_slots();
    (0..year.days)
        .map(|day| {
            (0..SLOTS_PER_DAY / width)
                .filter_map(|unit| {
                    let sum = year.sum(day, unit * width, width);
                    let bucket =
                        shape.bucket(year.day_ms(day) + unit as i64 * width as i64 * SLOT_MS);
                    let minutes = sum.minutes as f64;
                    match estimator {
                        Estimator::Pooled => (sum.minutes > 0).then_some((bucket, sum.rv, minutes)),
                        Estimator::Deflated => {
                            let level = levels[day]?;
                            (sum.minutes > 0).then_some((bucket, sum.rv / level, minutes))
                        }
                        Estimator::Geometric => (sum.minutes > 0 && sum.rv > 0.0)
                            .then(|| (bucket, (sum.rv / minutes).ln(), 1.0)),
                        Estimator::Parkinson => {
                            (sum.bars > 0).then_some((bucket, sum.range2, sum.bars as f64))
                        }
                    }
                })
                .collect()
        })
        .collect()
}

/// `values` divided by their mean; `None` when a value is not finite or
/// the mean is not positive.
fn normalized(values: Vec<f64>) -> Option<Vec<f64>> {
    let mean = values.iter().sum::<f64>() / values.len() as f64;
    (mean > 0.0 && values.iter().all(|v| v.is_finite()))
        .then(|| values.iter().map(|v| v / mean).collect())
}

/// The relative variance profile, mean 1 over the buckets, from the cells
/// of `days` (a day may repeat); `None` when a bucket has no cell.
fn profile(
    cells: &[Vec<Cell>],
    buckets: usize,
    estimator: Estimator,
    days: impl IntoIterator<Item = usize>,
) -> Option<Vec<f64>> {
    let (mut num, mut den) = (vec![0.0; buckets], vec![0.0; buckets]);
    for day in days {
        for &(bucket, n, d) in &cells[day] {
            num[bucket] += n;
            den[bucket] += d;
        }
    }
    if den.iter().any(|d| *d <= 0.0) {
        return None;
    }
    let values = num
        .iter()
        .zip(&den)
        .map(|(n, d)| {
            if estimator == Estimator::Geometric {
                (n / d).exp()
            } else {
                n / d
            }
        })
        .collect();
    normalized(values)
}

/// ln σ of a relative variance profile.
fn log_sigma(profile: &[f64]) -> Vec<f64> {
    profile.iter().map(|v| 0.5 * v.ln()).collect()
}

/// The circular block bootstrap of a profile over the UTC days: per
/// bucket the SD of its ln σ over the resamples whose profile is defined,
/// and the resamples that are not.
fn profile_se(
    cells: &[Vec<Cell>],
    buckets: usize,
    estimator: Estimator,
    block: usize,
    resamples: usize,
    seed: u64,
) -> (Vec<Option<f64>>, usize) {
    let days = cells.len();
    if days == 0 || block == 0 {
        return (vec![None; buckets], resamples);
    }
    let mut rng = SplitMix64(seed);
    let (mut n, mut mean, mut m2) = (0_usize, vec![0.0; buckets], vec![0.0; buckets]);
    let mut undefined = 0;
    for _ in 0..resamples {
        let mut drawn = Vec::with_capacity(days);
        resample(&mut rng, days, block, |day| drawn.push(day));
        let Some(p) = profile(cells, buckets, estimator, drawn) else {
            undefined += 1;
            continue;
        };
        n += 1;
        for (b, x) in log_sigma(&p).into_iter().enumerate() {
            // Welford's update.
            let delta = x - mean[b];
            mean[b] += delta / n as f64;
            m2[b] += delta * (x - mean[b]);
        }
    }
    let se = m2
        .iter()
        .map(|m2| (n >= 2).then(|| (m2 / (n - 1) as f64).sqrt()))
        .collect();
    (se, undefined)
}

/// The separable 24 × 7 profile: the product of the hour-of-day and the
/// weekday profiles, renormalized to mean 1 over the week.
fn separable(hours: &[f64], weekdays: &[f64]) -> Option<Vec<f64>> {
    normalized(
        (0..WEEK_HOURS)
            .map(|b| hours[b % 24] * weekdays[b / 24])
            .collect(),
    )
}

/// A profile of `shape` on the hours of the week.
fn on_week(shape: Shape, profile: &[f64]) -> [f64; WEEK_HOURS] {
    std::array::from_fn(|b| match shape {
        Shape::HourOfWeek => profile[b],
        Shape::HourOfDay => profile[b % 24],
        Shape::Weekday => profile[b / 24],
        Shape::SlotOfDay => unreachable!("the 5-minute profile is diagnostic only"),
    })
}

// ---------------------------------------------------------------------------
// Statistics
// ---------------------------------------------------------------------------

fn mean_sd(values: &[f64]) -> (f64, f64) {
    let n = values.len() as f64;
    let mean = values.iter().sum::<f64>() / n;
    let var = values.iter().map(|v| (v - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, var.sqrt())
}

fn correlation(pairs: &[(f64, f64)]) -> Option<f64> {
    let (sxx, sxy, syy) = moments(pairs)?;
    (sxx > 0.0 && syy > 0.0).then(|| sxy / (sxx * syy).sqrt())
}

/// The OLS slope of `y` on `x` with an intercept.
fn slope(pairs: &[(f64, f64)]) -> Option<f64> {
    let (sxx, sxy, _) = moments(pairs)?;
    (sxx > 0.0).then(|| sxy / sxx)
}

/// The centred sums of squares and products of at least two pairs, taken
/// from the first pair, so a constant series has a sum of squares of
/// exactly 0 rather than its mean's rounding.
fn moments(pairs: &[(f64, f64)]) -> Option<(f64, f64, f64)> {
    let &(x0, y0) = pairs.first().filter(|_| pairs.len() >= 2)?;
    let pairs: Vec<(f64, f64)> = pairs.iter().map(|p| (p.0 - x0, p.1 - y0)).collect();
    let n = pairs.len() as f64;
    let (mx, my) = (
        pairs.iter().map(|p| p.0).sum::<f64>() / n,
        pairs.iter().map(|p| p.1).sum::<f64>() / n,
    );
    Some((
        pairs.iter().map(|p| (p.0 - mx).powi(2)).sum(),
        pairs.iter().map(|p| (p.0 - mx) * (p.1 - my)).sum(),
        pairs.iter().map(|p| (p.1 - my).powi(2)).sum(),
    ))
}

/// The `q` quantile of sorted `values`, nearest rank.
fn quantile(sorted: &[f64], q: f64) -> f64 {
    sorted[((sorted.len() - 1) as f64 * q).round() as usize]
}

/// The adjusted R² of `ys` by the means of their groups (`groups` of
/// them).
fn group_r2(ys: &[(f64, usize)], groups: usize) -> Option<f64> {
    let (mut sum, mut count) = (vec![0.0; groups], vec![0_usize; groups]);
    for &(y, g) in ys {
        sum[g] += y;
        count[g] += 1;
    }
    let means: Vec<f64> = sum
        .iter()
        .zip(&count)
        .map(|(s, c)| if *c > 0 { s / *c as f64 } else { 0.0 })
        .collect();
    let used = count.iter().filter(|c| **c > 0).count();
    let residual: f64 = ys.iter().map(|&(y, g)| (y - means[g]).powi(2)).sum();
    adjusted_r2(ys.iter().map(|p| p.0), residual, used)
}

/// The adjusted R² of the additive fit y ≈ a[hour] + b[weekday], by
/// backfitting.
fn additive_r2(ys: &[(f64, usize, usize)]) -> Option<f64> {
    let (mut a, mut b) = ([0.0; 24], [0.0; 7]);
    for _ in 0..100 {
        let (mut sum, mut count) = ([0.0; 24], [0_usize; 24]);
        for &(y, h, w) in ys {
            sum[h] += y - b[w];
            count[h] += 1;
        }
        a = std::array::from_fn(|h| {
            if count[h] > 0 {
                sum[h] / count[h] as f64
            } else {
                0.0
            }
        });
        let (mut sum, mut count) = ([0.0; 7], [0_usize; 7]);
        for &(y, h, w) in ys {
            sum[w] += y - a[h];
            count[w] += 1;
        }
        b = std::array::from_fn(|w| {
            if count[w] > 0 {
                sum[w] / count[w] as f64
            } else {
                0.0
            }
        });
    }
    let residual: f64 = ys.iter().map(|&(y, h, w)| (y - a[h] - b[w]).powi(2)).sum();
    adjusted_r2(ys.iter().map(|p| p.0), residual, 24 + 7 - 1)
}

/// 1 − (residual ÷ (n − k)) ÷ (total ÷ (n − 1)) for `k` fitted means.
fn adjusted_r2(ys: impl Iterator<Item = f64> + Clone, residual: f64, k: usize) -> Option<f64> {
    let n = ys.clone().count();
    if n <= k || n < 2 {
        return None;
    }
    let mean = ys.clone().sum::<f64>() / n as f64;
    let total: f64 = ys.map(|y| (y - mean).powi(2)).sum();
    (total > 0.0).then(|| 1.0 - (residual / (n - k) as f64) / (total / (n - 1) as f64))
}

// ---------------------------------------------------------------------------
// ADR-056's lognormal-ρ model
// ---------------------------------------------------------------------------

/// ADR-056 mechanism step 2: the probability that a race is still open at
/// H, g(ρ) = (4/π) Σₙ≥₀ (−1)ⁿ/(2n+1) · exp(−(2n+1)² π² ρ² / 2). Below
/// ρ = 0.05 the band is 10 window σ wide either side and g is 1 to double
/// precision.
fn g(rho: f64) -> f64 {
    if rho < 0.05 {
        return 1.0;
    }
    let mut sum = 0.0;
    for n in 0..10_000 {
        let k = f64::from(2 * n + 1);
        let term = (-k * k * std::f64::consts::PI.powi(2) * rho * rho / 2.0).exp();
        if term < 1e-20 {
            break;
        }
        sum += if n % 2 == 0 { term / k } else { -term / k };
    }
    (4.0 / std::f64::consts::PI * sum).clamp(0.0, 1.0)
}

/// E[g(ρ)] for ln ρ normal with mean 0 and SD `sd` (Simpson's rule over
/// ±8 SD of the standard normal).
fn expected_g(sd: f64) -> f64 {
    const STEPS: usize = 1_600;
    let (low, high) = (-8.0, 8.0);
    let h = (high - low) / STEPS as f64;
    let f = |z: f64| g((sd * z).exp()) * (-z * z / 2.0).exp() / (std::f64::consts::TAU).sqrt();
    let mut sum = f(low) + f(high);
    for i in 1..STEPS {
        let z = low + i as f64 * h;
        sum += if i % 2 == 1 { 4.0 } else { 2.0 } * f(z);
    }
    sum * h / 3.0
}

/// The SD of ln ρ at which E[g(ρ)] = `share` (bisection on 0 … 4; E[g]
/// rises with the SD there); `None` outside that range.
fn lognormal_sd(share: f64) -> Option<f64> {
    let (mut low, mut high) = (0.0_f64, 4.0_f64);
    if share < expected_g(low) || share > expected_g(high) {
        return None;
    }
    for _ in 0..60 {
        let mid = 0.5 * (low + high);
        if expected_g(mid) < share {
            low = mid;
        } else {
            high = mid;
        }
    }
    Some(0.5 * (low + high))
}

// ---------------------------------------------------------------------------
// The mechanism
// ---------------------------------------------------------------------------

/// ln s_H(t) for every minute t of the data, per horizon, and its mean per
/// hour-of-week bucket.
struct Mechanism {
    ln_s: [Vec<f64>; 5],
    bucket_mean: [[f64; WEEK_HOURS]; 5],
}

/// s_H(t) = F_H(t) ÷ L_H(t) over the `minutes` minutes from minute
/// `first` (minutes since 1970-01-01) under the hour-of-week relative
/// variance `profile`.
fn mechanism(profile: &[f64; WEEK_HOURS], first: i64, minutes: usize) -> Mechanism {
    let start = first - WARM_DAYS * MINUTES_PER_DAY;
    let end = first + minutes as i64 + MINUTES_PER_DAY;
    let mut prefix = Vec::with_capacity((end - start + 1) as usize);
    prefix.push(0.0);
    for minute in start..end {
        let last = *prefix.last().expect("seeded");
        prefix.push(last + profile[hour_of_week(minute * MINUTE_MS)]);
    }
    let mean_over = |from: i64, length: i64| {
        let (a, b) = ((from - start) as usize, (from + length - start) as usize);
        (prefix[b] - prefix[a]) / length as f64
    };
    let mut ln_s: [Vec<f64>; 5] = std::array::from_fn(|_| Vec::with_capacity(minutes));
    let mut bucket_sum = [[0.0; WEEK_HOURS]; 5];
    let mut bucket_count = [[0_u64; WEEK_HOURS]; 5];
    for (h, &length) in HORIZON_MINUTES.iter().enumerate() {
        let (mut samples, mut sum, mut level) = (0, 0.0, None::<f64>);
        for t in start..first + minutes as i64 {
            if t.rem_euclid(length) == 0 && t - length >= start {
                let x = mean_over(t - length, length).sqrt();
                level = match level {
                    Some(level) => Some(((ATR_LENGTH - 1) as f64 * level + x) / ATR_LENGTH as f64),
                    None => {
                        samples += 1;
                        sum += x;
                        (samples == ATR_LENGTH).then(|| sum / ATR_LENGTH as f64)
                    }
                };
            }
            if t < first {
                continue;
            }
            let Some(level) = level else {
                continue;
            };
            let value = mean_over(t, length).sqrt().ln() - level.ln();
            let bucket = hour_of_week(t * MINUTE_MS);
            bucket_sum[h][bucket] += value;
            bucket_count[h][bucket] += 1;
            ln_s[h].push(value);
        }
    }
    let bucket_mean = std::array::from_fn(|h| {
        std::array::from_fn(|b| {
            if bucket_count[h][b] > 0 {
                bucket_sum[h][b] / bucket_count[h][b] as f64
            } else {
                f64::NAN
            }
        })
    });
    Mechanism { ln_s, bucket_mean }
}

/// The relative variance per 10⁶, summing to exactly 10⁶ per bucket:
/// floors, then the missing units to the largest remainders (the lower
/// bucket first on a tie).
fn integer_table(profile: &[f64]) -> Vec<i64> {
    let scaled: Vec<f64> = profile.iter().map(|v| v * 1e6).collect();
    let mut table: Vec<i64> = scaled.iter().map(|v| v.floor() as i64).collect();
    let target = 1_000_000 * profile.len() as i64;
    let missing = target - table.iter().sum::<i64>();
    let mut order: Vec<usize> = (0..profile.len()).collect();
    order.sort_by(|&a, &b| {
        let (ra, rb) = (scaled[a] - scaled[a].floor(), scaled[b] - scaled[b].floor());
        rb.total_cmp(&ra).then(a.cmp(&b))
    });
    for &b in order.iter().take(missing.max(0) as usize) {
        table[b] += 1;
    }
    table
}

// ---------------------------------------------------------------------------
// The report
// ---------------------------------------------------------------------------

fn se_text(se: Option<f64>) -> String {
    se.map_or_else(|| "-".to_owned(), |se| format!("{se:.4}"))
}

fn option_text(value: Option<f64>) -> String {
    value.map_or_else(|| "-".to_owned(), |v| format!("{v:.4}"))
}

/// The profiles of one shape, per estimator.
struct ShapeProfiles {
    shape: Shape,
    cells: Vec<Vec<Vec<Cell>>>,
    profiles: Vec<Option<Vec<f64>>>,
}

impl ShapeProfiles {
    fn of(year: &Year, levels: &[Option<f64>], shape: Shape) -> Self {
        let cells: Vec<Vec<Vec<Cell>>> = Estimator::ALL
            .iter()
            .map(|&e| cells(year, levels, shape, e))
            .collect();
        let profiles = Estimator::ALL
            .iter()
            .zip(&cells)
            .map(|(&e, cells)| profile(cells, shape.buckets(), e, 0..year.days))
            .collect();
        Self {
            shape,
            cells,
            profiles,
        }
    }

    fn get(&self, estimator: Estimator) -> Option<&Vec<f64>> {
        let i = Estimator::ALL.iter().position(|e| *e == estimator)?;
        self.profiles[i].as_ref()
    }

    fn print(&self, ses: Option<&[Vec<Option<f64>>]>) {
        let names: Vec<&str> = Estimator::ALL.iter().map(|e| e.name()).collect();
        println!(
            "{} profile ({} buckets): per estimator ({}) the relative variance and ln sigma{}",
            self.shape.name(),
            self.shape.buckets(),
            names.join(" / "),
            if ses.is_some() {
                " with its bootstrap SE"
            } else {
                ""
            }
        );
        if self.profiles.iter().any(Option::is_none) {
            let missing: Vec<&str> = Estimator::ALL
                .iter()
                .zip(&self.profiles)
                .filter(|(_, p)| p.is_none())
                .map(|(e, _)| e.name())
                .collect();
            println!(
                "  undefined (a bucket without data): {}",
                missing.join(", ")
            );
        }
        for b in 0..self.shape.buckets() {
            let columns: Vec<String> = self
                .profiles
                .iter()
                .enumerate()
                .map(|(e, p)| match p {
                    None => "-".to_owned(),
                    Some(p) => {
                        let se =
                            ses.map_or(String::new(), |ses| format!(" ({})", se_text(ses[e][b])));
                        format!("{:.4} {:+.4}{se}", p[b], 0.5 * p[b].ln())
                    }
                })
                .collect();
            println!("  {}: {}", self.shape.label(b), columns.join(" | "));
        }
    }
}

/// The variance shares of hourly ln σ, raw and deflated, and the sub-hour
/// share.
fn report_shares(year: &Year, levels: &[Option<f64>]) {
    for deflated in [false, true] {
        let mut ys: Vec<(f64, usize, usize, usize)> = Vec::new();
        for (day, day_level) in levels.iter().enumerate() {
            for hour in 0..24 {
                let sum = year.sum(day, hour * SLOTS_PER_HOUR, SLOTS_PER_HOUR);
                if sum.minutes == 0 || sum.rv <= 0.0 {
                    continue;
                }
                let level = if deflated {
                    let Some(level) = *day_level else { continue };
                    level
                } else {
                    1.0
                };
                let ms = year.day_ms(day) + hour as i64 * HOUR_MS;
                let y = 0.5 * (sum.rv / sum.minutes as f64 / level).ln();
                ys.push((
                    y,
                    hour,
                    weekday(year.first_day + day as i64),
                    hour_of_week(ms),
                ));
            }
        }
        let by_hour: Vec<(f64, usize)> = ys.iter().map(|p| (p.0, p.1)).collect();
        let by_day: Vec<(f64, usize)> = ys.iter().map(|p| (p.0, p.2)).collect();
        let by_week: Vec<(f64, usize)> = ys.iter().map(|p| (p.0, p.3)).collect();
        let additive: Vec<(f64, usize, usize)> = ys.iter().map(|p| (p.0, p.1, p.2)).collect();
        let values: Vec<f64> = ys.iter().map(|p| p.0).collect();
        let (separable, full) = (additive_r2(&additive), group_r2(&by_week, WEEK_HOURS));
        println!(
            "variance of hourly ln sigma ({}; {} hours, SD {}): adjusted R^2 hour-of-day {}, \
             weekday {}, separable sum {}, hour-of-week {}; interaction (hour-of-week less \
             separable) {}",
            if deflated {
                "deflated by the 28-day level"
            } else {
                "raw"
            },
            ys.len(),
            if values.len() >= 2 {
                format!("{:.4}", mean_sd(&values).1)
            } else {
                "-".to_owned()
            },
            option_text(group_r2(&by_hour, 24)),
            option_text(group_r2(&by_day, 7)),
            option_text(separable),
            option_text(full),
            option_text(full.zip(separable).map(|(f, s)| f - s))
        );
    }
    let mut ys: Vec<(f64, usize, usize)> = Vec::new();
    for (day, level) in levels.iter().enumerate() {
        let Some(level) = *level else { continue };
        for slot in 0..SLOTS_PER_DAY {
            let s = year.slots[day * SLOTS_PER_DAY + slot];
            if s.minutes == 0 || s.rv <= 0.0 {
                continue;
            }
            ys.push((
                0.5 * (s.rv / s.minutes as f64 / level).ln(),
                slot,
                slot / SLOTS_PER_HOUR,
            ));
        }
    }
    let fine = group_r2(
        &ys.iter().map(|p| (p.0, p.1)).collect::<Vec<_>>(),
        SLOTS_PER_DAY,
    );
    let coarse = group_r2(&ys.iter().map(|p| (p.0, p.2)).collect::<Vec<_>>(), 24);
    println!(
        "sub-hour (deflated 5-minute ln sigma, {} slots): adjusted R^2 5-minute-of-day {}, \
         hour-of-day {}; sub-hour share {} ({} of the 5-minute-of-day R^2)",
        ys.len(),
        option_text(fine),
        option_text(coarse),
        option_text(fine.zip(coarse).map(|(f, c)| f - c)),
        option_text(
            fine.zip(coarse)
                .and_then(|(f, c)| (f > 0.0).then(|| (f - c) / f))
        )
    );
}

/// The first 26 weeks against the rest, per estimator, hour-of-week.
fn report_stability(year: &Year, week: &ShapeProfiles, ses: &[Vec<Option<f64>>]) {
    if year.days <= HALF_DAYS {
        println!(
            "stability: {} days, too few for a split at {HALF_DAYS}",
            year.days
        );
        return;
    }
    println!(
        "stability: hour-of-week ln sigma, days {} .. {} against {} .. {}; z = difference / (2 \
         x the year's bootstrap SE)",
        day_label(year.first_day),
        day_label(year.first_day + HALF_DAYS as i64 - 1),
        day_label(year.first_day + HALF_DAYS as i64),
        day_label(year.first_day + year.days as i64 - 1)
    );
    for (e, estimator) in Estimator::ALL.iter().enumerate() {
        let first = profile(&week.cells[e], WEEK_HOURS, *estimator, 0..HALF_DAYS);
        let rest = profile(&week.cells[e], WEEK_HOURS, *estimator, HALF_DAYS..year.days);
        let (Some(first), Some(rest)) = (first, rest) else {
            println!("  {}: undefined (a bucket without data)", estimator.name());
            continue;
        };
        let (a, b) = (log_sigma(&first), log_sigma(&rest));
        let pairs: Vec<(f64, f64)> = a.iter().copied().zip(b.iter().copied()).collect();
        let diffs: Vec<f64> = pairs.iter().map(|p| p.1 - p.0).collect();
        let max = diffs.iter().fold(0.0_f64, |m, d| m.max(d.abs()));
        let rms = (diffs.iter().map(|d| d * d).sum::<f64>() / diffs.len() as f64).sqrt();
        let zs: Vec<f64> = diffs
            .iter()
            .zip(&ses[e])
            .filter_map(|(d, se)| se.filter(|se| *se > 0.0).map(|se| d / (2.0 * se)))
            .collect();
        let z_max = zs.iter().fold(0.0_f64, |m, z| m.max(z.abs()));
        let z_rms = if zs.is_empty() {
            f64::NAN
        } else {
            (zs.iter().map(|z| z * z).sum::<f64>() / zs.len() as f64).sqrt()
        };
        println!(
            "  {}: correlation {}, max |difference| {max:.4}, RMS difference {rms:.4}; max |z| \
             {z_max:.2}, RMS z {z_rms:.2} over {} buckets",
            estimator.name(),
            option_text(correlation(&pairs)),
            zs.len()
        );
    }
}

/// The DST diagnostic on the deflated estimator.
fn report_dst(year: &Year, hours: &ShapeProfiles, slots: &ShapeProfiles) {
    let e = Estimator::ALL
        .iter()
        .position(|e| *e == Estimator::Deflated)
        .expect("an estimator");
    let day = |d: usize| year.first_day + d as i64;
    let dst: Vec<usize> = (0..year.days).filter(|&d| us_dst(day(d))).collect();
    let standard: Vec<usize> = (0..year.days).filter(|&d| !us_dst(day(d))).collect();
    println!(
        "DST: {} US daylight-saving days, {} standard-time days (deflated estimator)",
        dst.len(),
        standard.len()
    );
    let of = |days: &[usize]| {
        profile(
            &hours.cells[e],
            24,
            Estimator::Deflated,
            days.iter().copied(),
        )
    };
    let (Some(on), Some(off)) = (of(&dst), of(&standard)) else {
        println!("  hour-of-day: undefined on one of the two sets");
        return;
    };
    let (on, off) = (log_sigma(&on), log_sigma(&off));
    for h in 0..24 {
        println!(
            "  {h:02}h: ln sigma DST {:+.4}, standard {:+.4}",
            on[h], off[h]
        );
    }
    let same: Vec<(f64, f64)> = (0..24).map(|h| (on[h], off[h])).collect();
    let moved: Vec<(f64, f64)> = (0..24).map(|h| (on[h], off[(h + 1) % 24])).collect();
    println!(
        "  correlation DST with standard: as is {}, standard moved one hour earlier {}",
        option_text(correlation(&same)),
        option_text(correlation(&moved))
    );
    let weekdays = |days: &[usize]| -> Vec<usize> {
        days.iter()
            .copied()
            .filter(|&d| weekday(day(d)) < 5)
            .collect()
    };
    let peak = |days: Vec<usize>| -> Option<usize> {
        let p = profile(&slots.cells[e], SLOTS_PER_DAY, Estimator::Deflated, days)?;
        OPEN_SLOTS.max_by(|a, b| p[*a].total_cmp(&p[*b]))
    };
    match (peak(weekdays(&dst)), peak(weekdays(&standard))) {
        (Some(a), Some(b)) => println!(
            "  weekday US-open peak (12:00 .. 17:00 UTC, 5-minute): DST {}, standard {}, shift \
             {} min",
            Shape::SlotOfDay.label(a),
            Shape::SlotOfDay.label(b),
            (b as i64 - a as i64) * 5
        ),
        _ => println!("  weekday US-open peak: undefined on one of the two sets"),
    }
}

/// Mechanism size, prediction and mechanism check for every estimator and
/// shape; returns nothing, prints everything.
fn report_mechanism(year: &Year, profiles: &[ShapeProfiles], hourly: Option<&Hourly>) {
    let sd_v2: [Option<f64>; 5] = std::array::from_fn(|h| lognormal_sd(V2_N_PERCENT[h] / 100.0));
    let gate = V2_SPREAD * (-GATE_SE * V2_SPREAD_SE).exp();
    println!(
        "lognormal-rho model (ADR-056): SD of ln rho under v2 per horizon {} (from N {:?} %); \
         v2 spread {V2_SPREAD}, gate {gate:.4} (= {V2_SPREAD} x e^(-{GATE_SE} x {V2_SPREAD_SE}))",
        sd_v2.map(option_text).join(" / "),
        V2_N_PERCENT
    );
    let shape = |s: Shape| {
        profiles
            .iter()
            .find(|p| p.shape == s)
            .expect("every shape is profiled")
    };
    println!(
        "ln s_H quantiles below are at {}",
        QUANTILES.map(|q| format!("{} %", q * 100.0)).join(" / ")
    );
    let by_week = hourly.map(Hourly::by_hour_of_week);
    let first = year.first_day * MINUTES_PER_DAY;
    let minutes = year.days * MINUTES_PER_DAY as usize;
    for estimator in Estimator::ALL {
        let (Some(week), Some(hours), Some(weekdays)) = (
            shape(Shape::HourOfWeek).get(estimator),
            shape(Shape::HourOfDay).get(estimator),
            shape(Shape::Weekday).get(estimator),
        ) else {
            println!("mechanism, {}: a profile is undefined", estimator.name());
            continue;
        };
        let Some(both) = separable(hours, weekdays) else {
            println!(
                "mechanism, {}: the separable profile is undefined",
                estimator.name()
            );
            continue;
        };
        let shapes: [(&str, [f64; WEEK_HOURS]); 4] = [
            ("hour-of-day (24)", on_week(Shape::HourOfDay, hours)),
            ("weekday (7)", on_week(Shape::Weekday, weekdays)),
            ("separable 24x7", on_week(Shape::HourOfWeek, &both)),
            ("hour-of-week (168)", on_week(Shape::HourOfWeek, week)),
        ];
        for (name, profile) in &shapes {
            let m = mechanism(profile, first, minutes);
            println!("mechanism, {} estimator, {name}:", estimator.name());
            let mut predicted = [None::<f64>; 5];
            for (h, horizon) in HORIZONS.iter().enumerate() {
                let values = &m.ln_s[h];
                if values.len() < 2 {
                    println!("  {horizon}: too few minutes");
                    continue;
                }
                let (mean, sd) = mean_sd(values);
                let mut sorted = values.clone();
                sorted.sort_by(f64::total_cmp);
                let quantiles: Vec<String> = QUANTILES
                    .iter()
                    .map(|q| format!("{:.4}", quantile(&sorted, *q)))
                    .collect();
                let sd_v3 = sd_v2[h].map(|v2| (v2 * v2 - sd * sd).max(0.0).sqrt());
                predicted[h] = sd_v3.map(|sd| expected_g(sd) * 100.0);
                println!(
                    "  {horizon}: ln s mean {mean:+.5}, SD {sd:.5}, quantiles {}; SD of ln rho v2 {} \
                     -> v3 {}{}; predicted N_v3 {} % (v2 {} %)",
                    quantiles.join(" "),
                    option_text(sd_v2[h]),
                    option_text(sd_v3),
                    if sd_v2[h].is_some_and(|v2| sd > v2) {
                        " (seasonal part exceeds v2's SD; clamped at 0)"
                    } else {
                        ""
                    },
                    predicted[h].map_or_else(|| "-".to_owned(), |n| format!("{n:.3}")),
                    V2_N_PERCENT[h]
                );
            }
            let defined: Vec<f64> = predicted.iter().flatten().copied().collect();
            if defined.len() == 5 {
                let spread = defined.iter().fold(f64::MIN, |m, v| m.max(*v))
                    / defined.iter().fold(f64::MAX, |m, v| m.min(*v));
                println!(
                    "  predicted v3 max / min {spread:.4} against v2 {V2_SPREAD}; below the gate \
                     {gate:.4}: {}",
                    spread < gate
                );
            }
            if let Some((neither, ran)) = &by_week {
                let mut line = Vec::new();
                for h in 0..5 {
                    let pairs: Vec<(f64, f64)> = (0..WEEK_HOURS)
                        .filter(|&b| neither[b][h] > 0 && ran[b][h] > 0)
                        .filter(|&b| m.bucket_mean[h][b].is_finite())
                        .map(|b| {
                            (
                                m.bucket_mean[h][b],
                                (neither[b][h] as f64 / ran[b][h] as f64).ln(),
                            )
                        })
                        .collect();
                    line.push(format!(
                        "{} r {} slope {} ({} buckets)",
                        HORIZONS[h],
                        option_text(correlation(&pairs)),
                        option_text(slope(&pairs)),
                        pairs.len()
                    ));
                }
                println!(
                    "  mechanism check, ln N_v2 by hour-of-week against mean ln s: {}",
                    line.join("; ")
                );
            }
        }
    }
}

/// The integer candidate tables, one line per estimator and shape.
fn tables_text(profiles: &[ShapeProfiles]) -> String {
    let mut text = String::from(
        "# season_measure candidate tables: table <estimator> <shape> <relative variance per \
         10^6, mean 10^6, one per bucket>\n",
    );
    let mut line = |estimator: Estimator, name: &str, profile: &[f64]| {
        let values: Vec<String> = integer_table(profile).iter().map(i64::to_string).collect();
        text.push_str(&format!(
            "table\t{}\t{name}\t{}\n",
            estimator.name(),
            values.join("\t")
        ));
    };
    for estimator in Estimator::ALL {
        for shapes in profiles {
            if let Some(p) = shapes.get(estimator) {
                line(estimator, shapes.shape.name(), p);
            }
        }
        let get = |s: Shape| profiles.iter().find(|p| p.shape == s)?.get(estimator);
        if let (Some(h), Some(w)) = (get(Shape::HourOfDay), get(Shape::Weekday))
            && let Some(both) = separable(h, w)
        {
            line(estimator, "separable-24x7", &both);
        }
    }
    text
}

/// The whole analysis of the slot sums (and of v2's hourly counts, when
/// there are any).
fn report(slots: &Slots, hourly: Option<&Hourly>, resamples: usize, tables_out: Option<&str>) {
    let Some(year) = Year::of(slots) else {
        println!("no closed 1m bar: nothing to analyse");
        return;
    };
    let with_returns = year.slots.iter().filter(|s| s.minutes > 0).count();
    let minutes: u64 = year.slots.iter().map(|s| s.minutes).sum();
    println!(
        "slots: {} UTC days {} .. {}, {} of {} slots with 1m returns, {minutes} minutes of \
         returns, {} incomplete 1m bars left out",
        year.days,
        day_label(year.first_day),
        day_label(year.first_day + year.days as i64 - 1),
        with_returns,
        year.slots.len(),
        slots.incomplete
    );
    let levels = year.levels();
    let profiles: Vec<ShapeProfiles> = Shape::ALL
        .iter()
        .map(|&s| ShapeProfiles::of(&year, &levels, s))
        .collect();
    let week = &profiles[0];
    println!(
        "precision: circular block bootstrap over {} UTC days, {BLOCK_DAYS}-day blocks, \
         {resamples} resamples, SplitMix64 seed {SEED}",
        year.days
    );
    let ses: Vec<Vec<Option<f64>>> = Estimator::ALL
        .iter()
        .zip(&week.cells)
        .map(|(&e, cells)| {
            let (se, undefined) = profile_se(cells, WEEK_HOURS, e, BLOCK_DAYS, resamples, SEED);
            println!("  {}: {undefined} undefined resamples", e.name());
            se
        })
        .collect();
    week.print(Some(&ses));
    for shapes in &profiles[1..] {
        shapes.print(None);
    }
    report_shares(&year, &levels);
    report_stability(&year, week, &ses);
    report_dst(&year, &profiles[1], &profiles[3]);
    report_mechanism(&year, &profiles, hourly);
    let tables = tables_text(&profiles);
    match tables_out {
        Some(path) => {
            std::fs::write(path, &tables).unwrap();
            println!(
                "integer candidate tables: {} lines written to {path}",
                tables.lines().count() - 1
            );
        }
        None => println!(
            "integer candidate tables: {} lines; set MIE_MEASURE_TABLES_OUT to write them",
            tables.lines().count() - 1
        ),
    }
}

// ---------------------------------------------------------------------------
// The measurements
// ---------------------------------------------------------------------------

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

/// The process's peak resident set (`VmHWM`), where the kernel reports it.
fn peak_rss() -> String {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|status| {
            status
                .lines()
                .find_map(|line| line.strip_prefix("VmHWM:").map(|v| v.trim().to_owned()))
        })
        .unwrap_or_else(|| "unknown".to_owned())
}

/// The bounds of `period`, end excluded.
fn bounds(period: (&str, &str)) -> (i64, i64) {
    (
        parse_bound("from", period.0, false).unwrap(),
        parse_bound("to", period.1, true).unwrap(),
    )
}

/// The measured window from the environment; panics before any read when
/// it overlaps the evaluation period (the hold-out of ADR-057).
fn window() -> (i64, i64) {
    let from = parse_bound("MIE_MEASURE_FROM", &env("MIE_MEASURE_FROM"), false).unwrap();
    let to = parse_bound("MIE_MEASURE_TO", &env("MIE_MEASURE_TO"), true).unwrap();
    let (start, end) = bounds(EVALUATION);
    assert!(
        to <= start || end <= from,
        "the window {from}..{to} ms overlaps the evaluation period {} .. {}: #155 reads only \
         the design period before ADR-057's acceptance run",
        EVALUATION.0,
        EVALUATION.1
    );
    let design = bounds(DESIGN);
    println!(
        "window {from}..{to} ms ({}the design period {} .. {})",
        if (from, to) == design { "" } else { "not " },
        DESIGN.0,
        DESIGN.1
    );
    (from, to)
}

#[test]
#[ignore = "measures the design year's seasonal volatility for #155; see the module docs"]
fn measure_seasonal_vol() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let (from, to) = window();
    let expected_events: Option<u64> = std::env::var("MIE_MEASURE_EVENTS")
        .ok()
        .map(|events| events.parse().expect("MIE_MEASURE_EVENTS is a count"));
    let store = ParquetRawStore::new(&raw_root);
    // aggTrades with the ADR-050 fold (its D8 default) and metrics for the
    // open interest: the replay of `measure_labels`, so v2's labels are
    // the ones ADR-056 published.
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::AggTrades, ArchiveStream::Metrics],
        TradeCorrection::Fold,
    );
    let Replay {
        mut stream,
        dataset,
    } = replay
        .replay(ReplayWindow {
            start: EventTime::from_millis(from),
            end: EventTime::from_millis(to),
        })
        .unwrap();
    let started = Instant::now();
    let mut run = SeasonRun::new();
    assert_eq!(run.labeler.labeling(), Labeling::V2, "labeling v2 only");
    println!(
        "provenance {}",
        LabelProvenance::new(
            ContextProvenance::new(DataVersion::from(&dataset), &run.engine),
            Labeling::V2
        )
    );
    while let Some(event) = stream.next_event().unwrap() {
        run.step(event);
    }
    let trailing = stream.trailing_gaps().to_vec();
    println!("trailing gaps {trailing:?}");
    run.finish(&trailing);
    println!(
        "events {}, rejections {}, labels {}; labeler {:?}",
        run.events,
        run.rejections,
        run.labels,
        run.labeler.stats()
    );
    let identity = report_identity(&run.inputs);
    if let Ok(path) = std::env::var("MIE_MEASURE_SLOTS_OUT") {
        std::fs::write(&path, rows_text(&run.inputs)).unwrap();
        println!("per-slot and per-hour rows written to {path}");
    }
    let tables = std::env::var("MIE_MEASURE_TABLES_OUT").ok();
    report(
        &run.inputs.slots,
        Some(&run.inputs.hourly),
        RESAMPLES,
        tables.as_deref(),
    );
    println!(
        "wall {:.1} s, peak RSS {}",
        started.elapsed().as_secs_f64(),
        peak_rss()
    );
    if let Some(expected) = expected_events {
        println!("events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    if (from, to) == bounds(DESIGN) {
        assert!(identity, "v2's counts differ from ADR-056's published ones");
    }
}

#[test]
#[ignore = "reruns the seasonal analysis from a rows file; see the module docs"]
fn measure_seasonal_vol_from() {
    let path = env("MIE_MEASURE_SLOTS");
    let inputs = parse_rows(&std::fs::read_to_string(&path).unwrap());
    let started = Instant::now();
    report_identity(&inputs);
    let tables = std::env::var("MIE_MEASURE_TABLES_OUT").ok();
    report(
        &inputs.slots,
        Some(&inputs.hourly),
        RESAMPLES,
        tables.as_deref(),
    );
    println!("wall {:.1} s", started.elapsed().as_secs_f64());
}

#[test]
#[ignore = "reads the seasonal profile from the archive's 1m klines; see the module docs"]
fn measure_seasonal_vol_klines() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let (from, to) = window();
    let store = ParquetRawStore::new(&raw_root);
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::Klines(Timeframe::M1)],
        TradeCorrection::Fold,
    );
    let Replay { mut stream, .. } = replay
        .replay(ReplayWindow {
            start: EventTime::from_millis(from),
            end: EventTime::from_millis(to),
        })
        .unwrap();
    let started = Instant::now();
    let mut slots = Slots::default();
    let mut klines = 0_u64;
    while let Some(event) = stream.next_event().unwrap() {
        let MarketEvent::Kline(kline) = event else {
            continue;
        };
        let open = kline.open_time.as_millis();
        if kline.close_time.as_millis() + 1 - open != MINUTE_MS {
            continue;
        }
        klines += 1;
        // An exchange kline without trades carries the previous close: an
        // empty bar.
        let prices = (kline.trade_count > 0).then(|| {
            (
                kline.high.units() as f64,
                kline.low.units() as f64,
                kline.close.units() as f64,
            )
        });
        slots.bar(open, prices, true);
    }
    println!("{klines} 1m klines");
    report(&slots, None, RESAMPLES, None);
    if let Ok(path) = std::env::var("MIE_MEASURE_SLOTS") {
        let engine = parse_rows(&std::fs::read_to_string(&path).unwrap());
        let of = |slots: &Slots| -> Option<Vec<f64>> {
            let year = Year::of(slots)?;
            let levels = year.levels();
            let cells = cells(&year, &levels, Shape::HourOfWeek, Estimator::Pooled);
            profile(&cells, WEEK_HOURS, Estimator::Pooled, 0..year.days).map(|p| log_sigma(&p))
        };
        match (of(&slots), of(&engine.slots)) {
            (Some(k), Some(e)) => {
                let pairs: Vec<(f64, f64)> = k.iter().copied().zip(e.iter().copied()).collect();
                let max = pairs.iter().fold(0.0_f64, |m, p| m.max((p.0 - p.1).abs()));
                println!(
                    "klines against the engine's bars ({path}), pooled hour-of-week ln sigma: \
                     correlation {}, max |difference| {max:.4}",
                    option_text(correlation(&pairs))
                );
            }
            _ => println!("klines against the engine's bars: a profile is undefined"),
        }
    }
    println!(
        "wall {:.1} s, peak RSS {}",
        started.elapsed().as_secs_f64(),
        peak_rss()
    );
}

// ---------------------------------------------------------------------------
// CI
// ---------------------------------------------------------------------------

#[test]
fn the_hour_of_week_starts_monday_00_utc_and_wraps() {
    // 1970-01-05 was a Monday.
    let monday = 4 * DAY_MS;
    assert_eq!(hour_of_week(monday), 0);
    assert_eq!(hour_of_week(monday - MINUTE_MS), 167, "Sunday 23:59");
    assert_eq!(hour_of_week(monday + 7 * DAY_MS - 1), 167);
    assert_eq!(hour_of_week(monday + 7 * DAY_MS), 0);
    // Before 1970: 1969-12-31 23:59:59.999 was a Wednesday, 23h.
    assert_eq!(hour_of_week(-1), 2 * 24 + 23);
    assert_eq!(hour_of_week(-7 * DAY_MS + monday), 0);
    // The design year starts on a Wednesday; 2025-10-06 is a Monday.
    let (start, _) = bounds(DESIGN);
    assert_eq!(weekday(start / DAY_MS), 2);
    assert_eq!(hour_of_week(start + 5 * DAY_MS), 0);
    assert_eq!(hour_of_week(start + 5 * DAY_MS - MINUTE_MS), 167);
    assert_eq!(Shape::Weekday.bucket(start), 2);
    assert_eq!(Shape::HourOfDay.bucket(start + 13 * HOUR_MS + 1), 13);
    assert_eq!(
        Shape::SlotOfDay.bucket(start + 13 * HOUR_MS + 35 * MINUTE_MS),
        163
    );
    assert_eq!(Shape::SlotOfDay.label(163), "13:35");
    assert_eq!(Shape::HourOfWeek.label(167), "Sun 23h");
}

#[test]
fn us_daylight_saving_days_of_the_design_year_are_the_plans() {
    let (start, end) = bounds(DESIGN);
    let (first, last) = (start / DAY_MS, end / DAY_MS - 1);
    let dst: Vec<i64> = (first..=last).filter(|&d| us_dst(d)).collect();
    let day = |text: &str| bounds((text, text)).0 / DAY_MS;
    // 2025-10-01 .. 11-01 and 2026-03-08 .. 09-30.
    let expected: Vec<i64> = (day("2025-10-01")..=day("2025-11-01"))
        .chain(day("2026-03-08")..=day("2026-09-30"))
        .collect();
    assert_eq!(dst, expected);
    assert!(!us_dst(day("2025-11-02")) && !us_dst(day("2026-03-07")));
}

#[test]
fn slots_read_complete_1m_returns_and_ranges() {
    let t = 100 * DAY_MS;
    let mut slots = Slots::default();
    // An incomplete first bar, then three priced bars with an empty one
    // between the second and the third.
    slots.bar(t, Some((101.0, 99.0, 100.0)), false);
    slots.bar(t + MINUTE_MS, Some((102.0, 100.0, 100.0)), true);
    slots.bar(t + 2 * MINUTE_MS, Some((110.0, 100.0, 110.0)), true);
    slots.bar(t + 3 * MINUTE_MS, None, true);
    slots.bar(t + 4 * MINUTE_MS, Some((121.0, 110.0, 121.0)), true);
    // The next slot: an incomplete bar ends the run.
    slots.bar(t + 5 * MINUTE_MS, None, false);
    slots.bar(t + 6 * MINUTE_MS, Some((121.0, 121.0, 121.0)), true);
    assert_eq!(slots.incomplete, 2);
    assert_eq!(slots.first, Some(t / SLOT_MS));
    let first = slots.slots[0];
    let ln = |x: f64| x.ln();
    assert_eq!(first.minutes, 1 + 2);
    assert!(
        (first.rv - (ln(1.1).powi(2) * 2.0)).abs() < 1e-15,
        "{first:?}"
    );
    assert_eq!(first.bars, 3);
    let ranges = ln(1.02).powi(2) + ln(1.1).powi(2) + ln(1.1).powi(2);
    assert!((first.range2 - ranges).abs() < 1e-15);
    assert_eq!(
        slots.slots[1],
        Slot {
            rv: 0.0,
            minutes: 0,
            range2: 0.0,
            bars: 1
        }
    );
}

#[test]
fn the_rows_file_round_trips() {
    let mut inputs = Inputs::default();
    let t = 200 * DAY_MS;
    inputs.slots.bar(t, Some((101.0, 99.0, 100.0)), true);
    inputs
        .slots
        .bar(t + 61 * MINUTE_MS, Some((101.3, 99.1, 100.7)), true);
    inputs
        .slots
        .bar(t + 62 * MINUTE_MS, Some((101.0, 99.0, 100.0)), false);
    inputs.hourly.row(t + 2 * HOUR_MS);
    inputs.hourly.neither[0] = [1, 2, 3, 4, 5];
    inputs.hourly.ran[0] = [10, 20, 30, 40, 50];
    let row = inputs.hourly.row(t + 4 * HOUR_MS);
    inputs.hourly.ran[row] = [7; 5];
    inputs.unavailable = [9, 8, 7, 6, 5];
    let text = rows_text(&inputs);
    let read = parse_rows(&text);
    assert_eq!(read.slots.first, inputs.slots.first);
    assert_eq!(read.slots.slots, inputs.slots.slots);
    assert_eq!(read.slots.incomplete, 1);
    assert_eq!(read.hourly, inputs.hourly);
    assert_eq!(read.unavailable, inputs.unavailable);
    assert_eq!(rows_text(&read), text);
}

/// A year of `days` whole days from 2025-10-06 (a Monday), every minute a
/// return of variance `level · planted[hour of week] · 10⁻⁶`, with
/// Gaussian noise from `seed`.
fn planted_year(days: usize, planted: &[f64; WEEK_HOURS], seed: u64) -> Year {
    let first_day = bounds(("2025-10-06", "2025-10-06")).0 / DAY_MS;
    let mut rng = SplitMix64(seed);
    let mut slots = Vec::with_capacity(days * SLOTS_PER_DAY);
    for day in 0..days {
        for slot in 0..SLOTS_PER_DAY {
            let ms = (first_day + day as i64) * DAY_MS + slot as i64 * SLOT_MS;
            let variance = planted[hour_of_week(ms)] * 1e-6;
            let mut draw = || (0..5).map(|_| normal(&mut rng).powi(2)).sum::<f64>() * variance;
            slots.push(Slot {
                rv: draw(),
                minutes: 5,
                range2: draw(),
                bars: 5,
            });
        }
    }
    Year {
        first_day,
        days,
        slots,
    }
}

/// A planted hour-of-week profile: a daily wave and a weekend dip.
fn planted_profile() -> [f64; WEEK_HOURS] {
    let raw: Vec<f64> = (0..WEEK_HOURS)
        .map(|b| {
            let wave = 0.3 * (std::f64::consts::TAU * (b % 24) as f64 / 24.0).sin();
            let weekend = if b >= 120 { -0.3 } else { 0.0 };
            (2.0 * (wave + weekend)).exp()
        })
        .collect();
    let p = normalized(raw).unwrap();
    std::array::from_fn(|b| p[b])
}

#[test]
fn profiles_are_normalized_to_a_mean_of_one() {
    let year = planted_year(28, &planted_profile(), 3);
    let levels = year.levels();
    for shape in Shape::ALL {
        for estimator in Estimator::ALL {
            let cells = cells(&year, &levels, shape, estimator);
            let p = profile(&cells, shape.buckets(), estimator, 0..year.days).unwrap();
            assert_eq!(p.len(), shape.buckets());
            let mean = p.iter().sum::<f64>() / p.len() as f64;
            assert!((mean - 1.0).abs() < 1e-12, "{shape:?} {estimator:?} {mean}");
        }
    }
    let both = separable(&[2.0; 24], &[0.5; 7]).unwrap();
    assert!(both.iter().all(|v| (v - 1.0).abs() < 1e-15));
    assert_eq!(normalized(vec![0.0, 0.0]), None);
    // A constant series has no correlation and a line has slope 2.
    let constant: Vec<(f64, f64)> = (0..168).map(|b| (b as f64, 0.1_f64.ln())).collect();
    assert_eq!(correlation(&constant), None);
    let line: Vec<(f64, f64)> = (0..10).map(|x| (x as f64, 2.0 * x as f64 + 1.0)).collect();
    assert!((slope(&line).unwrap() - 2.0).abs() < 1e-12);
    assert!((correlation(&line).unwrap() - 1.0).abs() < 1e-12);
}

#[test]
fn a_planted_hour_of_week_profile_is_recovered_within_its_bootstrap_se() {
    let planted = planted_profile();
    let truth = log_sigma(&planted);
    // 16 weeks: each bucket holds 16 hours of 60 one-minute returns, so the
    // SE of its ln sigma is near sqrt(2 / 960) / 2 = 0.0228 (the hourly
    // logs of the geometric estimator give the same).
    let year = planted_year(112, &planted, 155);
    let theory = (2.0_f64 / 960.0).sqrt() / 2.0;
    let levels = year.levels();
    for estimator in Estimator::ALL {
        let cells = cells(&year, &levels, Shape::HourOfWeek, estimator);
        let p = profile(&cells, WEEK_HOURS, estimator, 0..year.days).unwrap();
        // The planted days are independent, so weekly blocks suffice here
        // (16 of them; two 56-day blocks would leave the SE itself noisy).
        let (ses, undefined) = profile_se(&cells, WEEK_HOURS, estimator, 7, 400, SEED);
        assert_eq!(undefined, 0);
        let ses: Vec<f64> = ses.into_iter().map(Option::unwrap).collect();
        let errors: Vec<f64> = log_sigma(&p)
            .iter()
            .zip(&truth)
            .map(|(x, t)| x - t)
            .collect();
        let worst = errors.iter().fold(0.0_f64, |m, e| m.max(e.abs()));
        assert!(worst < 4.5 * theory, "{estimator:?}: max error {worst}");
        let mean_se = ses.iter().sum::<f64>() / ses.len() as f64;
        assert!(
            (0.8 * theory..1.2 * theory).contains(&mean_se),
            "{estimator:?}: mean SE {mean_se}"
        );
        let zs: Vec<f64> = errors.iter().zip(&ses).map(|(e, se)| e / se).collect();
        let rms = (zs.iter().map(|z| z * z).sum::<f64>() / zs.len() as f64).sqrt();
        assert!((0.75..1.35).contains(&rms), "{estimator:?}: RMS z {rms}");
    }
    // Fixed seed, fixed draws.
    let cells = cells(&year, &levels, Shape::HourOfWeek, Estimator::Pooled);
    let a = profile_se(&cells, WEEK_HOURS, Estimator::Pooled, BLOCK_DAYS, 50, SEED);
    let b = profile_se(&cells, WEEK_HOURS, Estimator::Pooled, BLOCK_DAYS, 50, SEED);
    assert_eq!(a, b);
}

#[test]
fn a_flat_profile_has_no_mechanism_and_a_daily_one_none_at_1d() {
    let first = bounds(DESIGN).0 / MINUTE_MS;
    let flat = mechanism(&[1.0; WEEK_HOURS], first, 21 * MINUTES_PER_DAY as usize);
    for h in 0..5 {
        assert_eq!(flat.ln_s[h].len(), 21 * 1_440);
        assert!(flat.ln_s[h].iter().all(|v| *v == 0.0), "{h}");
        assert_eq!(mean_sd(&flat.ln_s[h]).1, 0.0);
    }
    // A pure hour-of-day profile: every whole UTC day holds each hour once,
    // so F_1d and every 1d bar's factor are 1.
    let daily = planted_profile();
    let hours: Vec<f64> = (0..24).map(|h| daily[h]).collect();
    let hours = normalized(hours).unwrap();
    let m = mechanism(&on_week(Shape::HourOfDay, &hours), first, 14 * 1_440);
    assert!(m.ln_s[4].iter().all(|v| v.abs() < 1e-12));
    let (_, sd) = mean_sd(&m.ln_s[3]);
    assert!(sd > 0.01, "4h windows see the daily wave: {sd}");
    // The mechanism is invariant to the profile's scale: only ratios
    // enter s_H.
    let doubled = mechanism(
        &on_week(
            Shape::HourOfDay,
            &hours.iter().map(|v| 2.0 * v).collect::<Vec<_>>(),
        ),
        first,
        14 * 1_440,
    );
    for h in 0..5 {
        for (a, b) in m.ln_s[h].iter().zip(&doubled.ln_s[h]) {
            assert!((a - b).abs() < 1e-12);
        }
    }
}

#[test]
fn the_exit_time_series_and_the_lognormal_inversion_are_adr_056s() {
    // ADR-056 mechanism step 2.
    let percent = |rho: f64| (g(rho) * 1000.0).round() / 10.0;
    assert_eq!(
        [percent(1.0), percent(0.8), percent(0.7), percent(0.6)],
        [0.9, 5.4, 11.3, 21.5]
    );
    assert!((g(1.0) * 100.0 - 0.92).abs() < 0.005, "{}", g(1.0));
    assert_eq!(g(0.01), 1.0);
    assert!(g(5.0) < 1e-50);
    // Step 1: ATR(14) noise of 0.0573 lifts 0.92 % to about 1.03 %.
    assert!((expected_g(0.0573) * 100.0 - 1.03).abs() < 0.01);
    // *Accept when*: N = 10.05 % needs an SD of 0.53.
    let sd = lognormal_sd(0.1005).unwrap();
    assert!((sd - 0.53).abs() < 0.005, "{sd}");
    assert!((expected_g(sd) - 0.1005).abs() < 1e-9);
    assert_eq!(lognormal_sd(0.001), None);
    // v2's published N(H) all invert.
    for n in V2_N_PERCENT {
        assert!(lognormal_sd(n / 100.0).is_some());
    }
}

#[test]
fn candidate_tables_keep_the_mean_exactly() {
    let p = normalized((1..=7).map(f64::from).collect()).unwrap();
    let table = integer_table(&p);
    assert_eq!(table.iter().sum::<i64>(), 7_000_000);
    for (t, v) in table.iter().zip(&p) {
        assert!((*t as f64 - v * 1e6).abs() <= 1.0);
    }
    let planted = planted_profile();
    assert_eq!(integer_table(&planted).iter().sum::<i64>(), 168_000_000);
    assert_eq!(integer_table(&[1.0; 24]), vec![1_000_000; 24]);
}

/// A replay source over a fixed tape with a fixed dataset version.
struct History(Vec<MarketEvent>, DatasetVersion);

/// The events of a [`History`] replay.
struct Feed(VecDeque<MarketEvent>);

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

impl HistoricalDataProvider for History {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        Ok(Replay {
            stream: Feed(
                self.0
                    .iter()
                    .filter(|event| window.contains(event.time()))
                    .cloned()
                    .collect(),
            ),
            dataset: self.1.clone(),
        })
    }
}

#[test]
fn the_season_run_counts_labeling_v2_and_engine_bars() {
    let tape = synthetic_tape();
    let mut run = SeasonRun::new();
    assert_eq!(run.labeler.labeling(), Labeling::V2);
    for event in tape.iter().cloned() {
        run.step(event);
    }
    run.finish(&[]);

    // `label_window` under labeling v2 over the same tape counts the same
    // hours.
    let dataset = DatasetVersion::from_hex(&"5b".repeat(32)).unwrap();
    let history = History(tape, dataset);
    let window = ReplayWindow {
        start: EventTime::from_millis(TAPE_START),
        end: EventTime::from_millis(TAPE_START + 60 * DAY_MS),
    };
    let (mut hourly, mut unavailable) = (Hourly::default(), [0_u64; 5]);
    let (provenance, windowed) = label_window(
        &history,
        window,
        window,
        Labeling::V2,
        |_| Vec::new(),
        |_| {},
        |label| {
            hourly.add(&label);
            for (h, horizon) in label.horizons.iter().enumerate() {
                unavailable[h] += u64::from(horizon.race == Race::Unavailable);
            }
        },
    )
    .unwrap();
    assert_eq!(provenance.labeling, LABELING_V2);
    assert_eq!(run.inputs.hourly, hourly);
    assert_eq!(run.inputs.unavailable, unavailable);
    assert_eq!(windowed.labels, run.labeler.stats());
    let (_, ran) = run.inputs.hourly.totals();
    assert!(ran.iter().all(|r| *r > 0), "{ran:?}");

    // Every tape day has 1m returns; the first 1m bar is partial and the
    // trades gap of day 9 leaves bars incomplete.
    let year = Year::of(&run.inputs.slots).unwrap();
    assert_eq!(year.first_day, TAPE_START / DAY_MS);
    assert_eq!(year.days, TAPE_DAYS as usize);
    for day in 0..year.days {
        assert!(year.sum(day, 0, SLOTS_PER_DAY).minutes > 0, "day {day}");
    }
    assert!(run.inputs.slots.incomplete >= 2);

    // The identity is the design year's; the tape's counts are not.
    assert!(!report_identity(&run.inputs));
    // The whole report runs on three weeks (no stability split), with few
    // resamples; the rows file reruns to the same text.
    report(&run.inputs.slots, Some(&run.inputs.hourly), 20, None);
    let text = rows_text(&run.inputs);
    assert_eq!(rows_text(&parse_rows(&text)), text);
}
