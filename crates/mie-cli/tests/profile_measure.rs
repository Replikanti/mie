//! The #79 measurements behind the ADR-036 parameters over the archive
//! backfill (not run in CI): an `ArchiveReplay` of aggTrades over a window
//! read from the environment drives one `MarketStateEngine`, and the exact
//! profile logic ([`shape_levels`], the engine's own code at other
//! [`ProfileShape`]s) prints the pre-registered rules and tables of the #79
//! plan:
//!
//! - **A0**, a valid run: events, domain rejections, and on every completed
//!   day the tool's recomputation at [`ProfileShape::V1`] over its own
//!   binning against the engine's `prior_day` and `composite_5d`, field by
//!   field.
//! - **A1**, no `OutOfRange` among the prior-day and composite profiles.
//! - **A2**, `bin_size · 10 000 ≤ P_min · w`, `w` being
//!   [`TOLERANCE_BPS`].
//! - **A3**, the median LVN count per profile is at least 1 in each half.
//! - **A4**, the split-half κ of HVNs and LVNs: each day's trades split by
//!   aggregate-id parity into two half-profiles, a node reproduced when the
//!   other half has a node of the same kind within `w` of its price, against
//!   the chance share of the profile's bin range within `w` of the other
//!   half's nodes; `κ ≥ 0.5` in each of the 8 cells.
//! - The recorded tables: span, value-area width and node counts per
//!   profile and half; the sensitivity of bin size, kernel, prominence,
//!   value area and composite length, one at a time with the others at
//!   `@1`; the mean daily volume per UTC weekday and **C1** (both weekend
//!   days below every weekday, in both halves).
//!
//! Trades are binned at 5 USDT per UTC day and aggregate-id parity, and
//! re-binned exactly at each day close (`k.div_euclid(m)`, m ∈ {1, 2, 4, 5,
//! 10}, i.e. 5, 10, 20, 25 and 50 USDT). The window is split into halves at
//! the UTC day open nearest below its midpoint; a profile belongs to the
//! half of its last day. **A5** is compared outside the tool: with
//! `MIE_MEASURE_PROFILE_OUT=<file>` the tool writes the engine's
//! `prior_day` line for every completed day, whose first 13 fields
//! `tools/reference/volume_profile_reference.py` must reproduce.
//!
//! Run it with
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! profile_measure -- --ignored --nocapture`; `MIE_MEASURE_TO` is
//! included, as in `mie replay`.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::replay::parse_bound;
use mie_domain::bars::Timeframe;
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureValue, Unavailability};
use mie_domain::location::TOLERANCE_BPS;
use mie_domain::num::{Price, Qty, SCALE};
use mie_domain::profile::{
    BIN_SIZE, COMPOSITE_SESSIONS, MAX_BINS, NODE_KERNEL, NODE_PROMINENCE_PCT, ProfileLevels,
    ProfileNode, ProfileShape, VALUE_AREA_PCT, VolumeProfile, shape_levels,
};
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use std::collections::{BTreeMap, VecDeque};
use std::io::Write;
use std::time::Instant;

const DAY_MS: i64 = 86_400_000;

/// The window W of the #79 pre-registration and its event count in the
/// #12 import (`docs/data-availability.md`), checked by A0 when the run is
/// over W.
const W_FROM: &str = "2025-10-01";
const W_TO: &str = "2026-09-30";
const W_EVENTS: u64 = 603_218_226;

/// The bin trades are collected at, in USDT: it divides every compared size.
const FINE_USDT: i64 = 5;

/// The compared bin sizes as multiples of [`FINE_USDT`]: 5, 10, 20, 25 and
/// 50 USDT.
const MULTIPLES: [i64; 5] = [1, 2, 4, 5, 10];

/// The index of `@1`'s 10 USDT in [`MULTIPLES`].
const V1_MULTIPLE: usize = 1;

/// The compared kernels, `triangular_k`, `none` being `k = 1`.
const KERNELS: [(&str, &[i64]); 5] = [
    ("none", &[1]),
    ("t3", &[1, 2, 1]),
    ("t5", &NODE_KERNEL),
    ("t7", &[1, 2, 3, 4, 3, 2, 1]),
    ("t9", &[1, 2, 3, 4, 5, 4, 3, 2, 1]),
];

/// The compared node prominences, in percent.
const PROMINENCES: [i64; 5] = [5, 10, 15, 20, 30];

/// The compared value areas, in percent.
const VALUE_AREAS: [i64; 4] = [60, 68, 70, 80];

/// The compared composite lengths, in UTC days.
const COMPOSITES: [usize; 4] = [3, 5, 7, 14];

/// The κ threshold of A4.
const KAPPA_PASS: f64 = 0.5;

const WEEKDAYS: [&str; 7] = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];

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

fn pct(part: u64, whole: u64) -> String {
    if whole == 0 {
        return "-".to_owned();
    }
    format!("{:.1} %", 100.0 * part as f64 / whole as f64)
}

fn half_label(half: usize) -> &'static str {
    if half == 0 {
        "first half"
    } else {
        "second half"
    }
}

fn verdict(pass: bool) -> &'static str {
    if pass { "PASS" } else { "FAIL" }
}

/// Whether `price` lies within `w` of `reference`: `location::within`
/// (ADR-044 D3), `|price − reference| · 10 000 ≤ |reference| · w`.
fn within(price: Price, reference: Price) -> bool {
    let gap = (i128::from(price.units()) - i128::from(reference.units())).abs();
    gap * 10_000 <= i128::from(reference.units()).abs() * i128::from(TOLERANCE_BPS)
}

/// The share of the bins of `[low, high)` (width `bin_units`) whose
/// midpoint `m` has a node of `nodes` within `w` of it: the chance that a
/// node placed on a random bin of the range is reproduced. `nodes` is by
/// price ascending.
///
/// `within(y, m)` holds exactly for `m ∈ [10⁴·y / (10⁴ + w), 10⁴·y / (10⁴
/// − w)]` (positive prices), an interval of bin indices per node; the
/// intervals ascend with `y` and are merged.
fn chance_share(nodes: &[ProfileNode], low: Price, high: Price, bin_units: i64) -> f64 {
    let bins = i128::from((high.units() - low.units()) / bin_units);
    if bins <= 0 {
        return 0.0;
    }
    let (b, w) = (i128::from(bin_units), i128::from(TOLERANCE_BPS));
    let centre = i128::from(low.units()) + b / 2;
    let ceil = |a: i128, d: i128| -((-a).div_euclid(d));
    let (mut covered, mut last) = (0_i128, -1_i128);
    for node in nodes {
        let y = i128::from(node.price.units());
        let from = ceil(10_000 * y - centre * (10_000 + w), b * (10_000 + w)).max(0);
        let to = (10_000 * y - centre * (10_000 - w))
            .div_euclid(b * (10_000 - w))
            .min(bins - 1);
        let from = from.max(last + 1);
        if to >= from {
            covered += to - from + 1;
        }
        last = last.max(to);
    }
    covered as f64 / bins as f64
}

/// The pooled κ of one cell: `(observed − chance) / (nodes − chance)`.
#[derive(Debug, Clone, Default)]
struct Kappa {
    nodes: u64,
    observed: u64,
    chance: f64,
}

impl Kappa {
    /// Adds the nodes of `tested`, each reproduced when `other` has a node
    /// within `w` of it, with the chance share of `other` over the range.
    fn add(
        &mut self,
        tested: &[ProfileNode],
        other: &[ProfileNode],
        range: &ProfileLevels,
        bin_units: i64,
    ) {
        self.nodes += tested.len() as u64;
        self.observed += tested
            .iter()
            .filter(|node| other.iter().any(|o| within(o.price, node.price)))
            .count() as u64;
        self.chance += chance_share(other, range.low, range.high, bin_units) * tested.len() as f64;
    }

    fn value(&self) -> f64 {
        (self.observed as f64 - self.chance) / (self.nodes as f64 - self.chance)
    }
}

/// What one parameter row measures, per profile and half.
#[derive(Debug, Clone, Default)]
struct Cell {
    /// Ready profiles.
    profiles: u64,
    out_of_range: u64,
    input_invalid: u64,
    span_bins: Vec<f64>,
    va_bins: Vec<f64>,
    va_bps: Vec<f64>,
    hvn: Vec<f64>,
    lvn: Vec<f64>,
    /// HVN, LVN.
    kappa: [Kappa; 2],
    /// `|POC − POC at 10 USDT|` in USDT.
    poc_shift: Vec<f64>,
    /// Profiles whose kernel resolution `r_k · bin` is within `w · POC`.
    resolved: u64,
    /// Composites whose POC lies more than `w` from the prior-day POC.
    poc_far: u64,
    /// Next-day 1m closes, and those inside the prior-day value area.
    next_closes: u64,
    next_inside: u64,
}

impl Cell {
    fn line(&mut self) -> String {
        let max = |values: &[f64]| values.iter().copied().fold(f64::NAN, f64::max);
        let min = |values: &[f64]| values.iter().copied().fold(f64::NAN, f64::min);
        let (span_max, va_min, hvn_max, lvn_max, shift_max) = (
            max(&self.span_bins),
            min(&self.va_bins),
            max(&self.hvn),
            max(&self.lvn),
            max(&self.poc_shift),
        );
        let span = (
            quantile(&mut self.span_bins, 0.5),
            quantile(&mut self.span_bins, 0.99),
        );
        let va_p10 = quantile(&mut self.va_bins, 0.1);
        let va_bps = [0.1, 0.5, 0.9].map(|q| quantile(&mut self.va_bps, q));
        let hvn = [0.1, 0.5, 0.9].map(|q| quantile(&mut self.hvn, q));
        let lvn = [0.1, 0.5, 0.9].map(|q| quantile(&mut self.lvn, q));
        let shift_p50 = quantile(&mut self.poc_shift, 0.5);
        format!(
            "n {} (OutOfRange {}, InputInvalid {}); span bins p50/p99/max \
             {:.0}/{:.0}/{span_max:.0}; va bins min/p10 {va_min:.0}/{va_p10:.0}; \
             va bps p10/p50/p90 {:.1}/{:.1}/{:.1}; hvn p10/p50/p90/max \
             {:.0}/{:.0}/{:.0}/{hvn_max:.0}; lvn p10/p50/p90/max \
             {:.0}/{:.0}/{:.0}/{lvn_max:.0}; κ hvn {:.3} lvn {:.3}; poc shift vs \
             10 USDT p50/max {shift_p50:.0}/{shift_max:.0} USDT; r_k·bin ≤ w·POC {}; \
             composite POC beyond w of prior POC {}; next-day closes in value {}",
            self.profiles,
            self.out_of_range,
            self.input_invalid,
            span.0,
            span.1,
            va_bps[0],
            va_bps[1],
            va_bps[2],
            hvn[0],
            hvn[1],
            hvn[2],
            lvn[0],
            lvn[1],
            lvn[2],
            self.kappa[0].value(),
            self.kappa[1].value(),
            pct(self.resolved, self.profiles),
            pct(self.poc_far, self.profiles),
            pct(self.next_inside, self.next_closes),
        )
    }
}

/// One parameter row: a shape, its bin multiple and composite length.
struct Config {
    label: String,
    shape: ProfileShape,
    /// Index into [`MULTIPLES`].
    multiple: usize,
    /// The composite's length in days.
    sessions: usize,
    /// The kernel resolution `r_k = (k + 3) / 2` in bins.
    resolution: i64,
    /// Per profile (prior day, composite), per half.
    cells: [[Cell; 2]; 2],
    /// The last prior-day value area, `(next day open, val, vah, half)`.
    pending: Option<(i64, Price, Price, usize)>,
}

impl Config {
    fn new(
        label: String,
        multiple: usize,
        kernel: &'static [i64],
        prominence: i64,
        value_area: i64,
        sessions: usize,
    ) -> Self {
        let bin = Price::from_units(FINE_USDT * MULTIPLES[multiple] * SCALE);
        Self {
            label,
            shape: ProfileShape::new(bin, MAX_BINS, value_area, prominence, kernel).unwrap(),
            multiple,
            sessions,
            resolution: i64::try_from((kernel.len() + 3) / 2).unwrap(),
            cells: Default::default(),
            pending: None,
        }
    }

    fn bin_units(&self) -> i64 {
        FINE_USDT * MULTIPLES[self.multiple] * SCALE
    }
}

/// The parameter rows, one family at a time, each with the others at `@1`.
fn configs() -> Vec<Config> {
    let (bin, kernel, prominence, value_area, sessions) = (
        V1_MULTIPLE,
        &NODE_KERNEL[..],
        NODE_PROMINENCE_PCT,
        VALUE_AREA_PCT,
        COMPOSITE_SESSIONS,
    );
    let mut rows = Vec::new();
    for (index, m) in MULTIPLES.iter().enumerate() {
        let label = format!("bin {} USDT", FINE_USDT * m);
        rows.push(Config::new(
            label, index, kernel, prominence, value_area, sessions,
        ));
    }
    for (name, k) in KERNELS {
        let label = format!("kernel {name}");
        rows.push(Config::new(label, bin, k, prominence, value_area, sessions));
    }
    for p in PROMINENCES {
        let label = format!("prominence {p} %");
        rows.push(Config::new(label, bin, kernel, p, value_area, sessions));
    }
    for va in VALUE_AREAS {
        let label = format!("value area {va} %");
        rows.push(Config::new(label, bin, kernel, prominence, va, sessions));
    }
    for n in COMPOSITES {
        let label = format!("composite {n} days");
        rows.push(Config::new(label, bin, kernel, prominence, value_area, n));
    }
    rows
}

/// One completed UTC day: its histograms at every multiple, as
/// `[full, even ids, odd ids]`.
struct Day {
    open: i64,
    maps: Vec<[BTreeMap<i64, Qty>; 3]>,
    volume: Qty,
}

/// Merges `fine` into `into`, re-binned to multiple `m`.
fn rebin(into: &mut BTreeMap<i64, Qty>, fine: &BTreeMap<i64, Qty>, m: i64) {
    for (bin, volume) in fine {
        let slot = into.entry(bin.div_euclid(m)).or_insert(Qty::from_units(0));
        *slot = slot.checked_add(*volume).expect("bin volume overflow");
    }
}

impl Day {
    fn new(open: i64, halves: [BTreeMap<i64, Qty>; 2]) -> Self {
        let maps = MULTIPLES
            .iter()
            .map(|m| {
                let mut maps: [BTreeMap<i64, Qty>; 3] = Default::default();
                for (parity, fine) in halves.iter().enumerate() {
                    rebin(&mut maps[0], fine, *m);
                    rebin(&mut maps[1 + parity], fine, *m);
                }
                maps
            })
            .collect();
        let volume = halves
            .iter()
            .flat_map(BTreeMap::values)
            .try_fold(Qty::from_units(0), |sum, volume| sum.checked_add(*volume))
            .expect("day volume overflow");
        Self { open, maps, volume }
    }
}

/// The levels part of an engine profile.
fn levels_of(profile: &VolumeProfile) -> ProfileLevels {
    ProfileLevels {
        total_volume: profile.total_volume,
        low: profile.low,
        high: profile.high,
        poc: profile.poc,
        poc_volume: profile.poc_volume,
        val: profile.val,
        vah: profile.vah,
        value_area_volume: profile.value_area_volume,
        hvn: profile.hvn.clone(),
        lvn: profile.lvn.clone(),
    }
}

/// The profile of the last `sessions` days under `shape`, histogram `which`
/// (0 full, 1 even ids, 2 odd ids).
fn profile_of(
    days: &VecDeque<Day>,
    sessions: usize,
    shape: &ProfileShape,
    multiple: usize,
    which: usize,
) -> FeatureValue<ProfileLevels> {
    let histograms: Vec<&BTreeMap<i64, Qty>> = days
        .iter()
        .skip(days.len() - sessions)
        .map(|day| &day.maps[multiple][which])
        .collect();
    shape_levels(shape, &histograms).expect("profile overflow")
}

/// Measures every row on the profiles that end with the newest day.
fn measure(rows: &mut [Config], days: &VecDeque<Day>, half: usize) {
    let newest = days.back().expect("a day").open;
    for row in rows.iter_mut() {
        let bin_units = row.bin_units();
        let mut prior_poc = None;
        for kind in 0..2 {
            let sessions = if kind == 0 { 1 } else { row.sessions };
            if days.len() < sessions {
                continue;
            }
            let cell = &mut row.cells[kind][half];
            let levels = match profile_of(days, sessions, &row.shape, row.multiple, 0) {
                FeatureValue::Ready(levels) => levels,
                FeatureValue::Unavailable {
                    reason: Unavailability::OutOfRange,
                } => {
                    cell.out_of_range += 1;
                    continue;
                }
                _ => {
                    cell.input_invalid += 1;
                    continue;
                }
            };
            cell.profiles += 1;
            let width = |a: Price, b: Price| (b.units() - a.units()) / bin_units;
            cell.span_bins.push(width(levels.low, levels.high) as f64);
            cell.va_bins.push(width(levels.val, levels.vah) as f64);
            cell.va_bps.push(
                (levels.vah.units() - levels.val.units()) as f64 * 10_000.0
                    / levels.val.units() as f64,
            );
            cell.hvn.push(levels.hvn.len() as f64);
            cell.lvn.push(levels.lvn.len() as f64);
            if let FeatureValue::Ready(reference) =
                profile_of(days, sessions, &ProfileShape::V1, V1_MULTIPLE, 0)
            {
                cell.poc_shift
                    .push((levels.poc.units() - reference.poc.units()).abs() as f64 / SCALE as f64);
            }
            let resolution = i128::from(row.resolution) * i128::from(bin_units) * 10_000;
            if resolution <= i128::from(levels.poc.units()) * i128::from(TOLERANCE_BPS) {
                cell.resolved += 1;
            }
            match (kind, prior_poc) {
                (0, _) => {
                    prior_poc = Some(levels.poc);
                    row.pending = Some((newest + DAY_MS, levels.val, levels.vah, half));
                }
                (_, Some(prior)) if !within(levels.poc, prior) => cell.poc_far += 1,
                _ => {}
            }
            let nodes = |which| match profile_of(days, sessions, &row.shape, row.multiple, which) {
                FeatureValue::Ready(half) => (half.hvn, half.lvn),
                _ => (Vec::new(), Vec::new()),
            };
            let (even, odd) = (nodes(1), nodes(2));
            cell.kappa[0].add(&even.0, &odd.0, &levels, bin_units);
            cell.kappa[0].add(&odd.0, &even.0, &levels, bin_units);
            cell.kappa[1].add(&even.1, &odd.1, &levels, bin_units);
            cell.kappa[1].add(&odd.1, &even.1, &levels, bin_units);
        }
    }
}

#[test]
#[ignore = "measures the archive for ADR-036 (#79); see the module docs"]
fn measure_profile() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let from = parse_bound("MIE_MEASURE_FROM", &env("MIE_MEASURE_FROM"), false).unwrap();
    let to = parse_bound("MIE_MEASURE_TO", &env("MIE_MEASURE_TO"), true).unwrap();
    let is_w = from == parse_bound("W_FROM", W_FROM, false).unwrap()
        && to == parse_bound("W_TO", W_TO, true).unwrap();
    let mid = from + (to - from) / 2;
    let split = mid - (mid - from).rem_euclid(DAY_MS);
    let mut profile_out = std::env::var("MIE_MEASURE_PROFILE_OUT")
        .ok()
        .map(|path| std::io::BufWriter::new(std::fs::File::create(path).unwrap()));
    let window = ReplayWindow {
        start: EventTime::from_millis(from),
        end: EventTime::from_millis(to),
    };
    let store = ParquetRawStore::new(&raw_root);
    let replay = ArchiveReplay::new(&store, "BTCUSDT", &[ArchiveStream::AggTrades]);
    let mut stream = replay.replay(window).unwrap().stream;
    let started = Instant::now();

    let fine = ProfileShape::new(
        Price::from_units(FINE_USDT * SCALE),
        MAX_BINS,
        VALUE_AREA_PCT,
        NODE_PROMINENCE_PCT,
        &NODE_KERNEL,
    )
    .unwrap();
    let mut engine = MarketStateEngine::new();
    let mut rows = configs();
    let mut developing: BTreeMap<i64, [BTreeMap<i64, Qty>; 2]> = BTreeMap::new();
    let mut days: VecDeque<Day> = VecDeque::new();
    let keep = COMPOSITES.into_iter().max().unwrap();
    let (mut events, mut rejections) = (0_u64, 0_u64);
    let (mut compared, mut mismatches) = ([0_u64; 2], [0_u64; 2]);
    let mut price_range = [(i64::MAX, i64::MIN); 2];
    let mut weekday = [[(0_f64, 0_u64); 7]; 2];

    while let Some(event) = stream.next_event().unwrap() {
        events += 1;
        if engine.apply(&event).is_err() {
            rejections += 1;
            continue;
        }
        if let MarketEvent::Trade(trade) = &event
            && trade.qty.units() > 0
        {
            let open = Timeframe::D1.open_of(trade.time).unwrap().as_millis();
            let half = usize::from(open >= split);
            let range = &mut price_range[half];
            *range = (
                range.0.min(trade.price.units()),
                range.1.max(trade.price.units()),
            );
            let parity = usize::try_from(trade.trade_id % 2).unwrap();
            let slot = developing.entry(open).or_default()[parity]
                .entry(fine.bin_of(trade.price))
                .or_insert(Qty::from_units(0));
            *slot = slot.checked_add(trade.qty).expect("bin volume overflow");
        }
        let closed = engine.closed_bars();
        if closed.is_empty() {
            continue;
        }
        let mut day_closed = false;
        for bar in closed {
            match bar.timeframe {
                Timeframe::M1 => {
                    let (Some(ohlc), time) = (bar.ohlc, bar.open_time.as_millis()) else {
                        continue;
                    };
                    for row in &mut rows {
                        if let Some((start, val, vah, half)) = row.pending
                            && (start..start + DAY_MS).contains(&time)
                        {
                            let cell = &mut row.cells[0][half];
                            cell.next_closes += 1;
                            cell.next_inside += u64::from(val <= ohlc.close && ohlc.close < vah);
                        }
                    }
                }
                Timeframe::D1 => {
                    let open = bar.open_time.as_millis();
                    let halves = developing.remove(&open).unwrap_or_default();
                    let day = Day::new(open, halves);
                    let half = usize::from(open >= split);
                    let slot = &mut weekday[half]
                        [usize::try_from((open.div_euclid(DAY_MS) + 3).rem_euclid(7)).unwrap()];
                    slot.0 += day.volume.units() as f64 / SCALE as f64;
                    slot.1 += 1;
                    if days.len() == keep {
                        days.pop_front();
                    }
                    days.push_back(day);
                    measure(&mut rows, &days, half);
                    day_closed = true;
                }
                _ => {}
            }
        }
        if !day_closed {
            continue;
        }
        // A0: the tool's `@1` recomputation against the engine.
        let profiles = &engine.state().profile;
        let newest = days.back().unwrap();
        let half = usize::from(newest.open >= split);
        let tool = profile_of(&days, 1, &ProfileShape::V1, V1_MULTIPLE, 0);
        compared[0] += 1;
        if profiles.prior_day.clone().map(|p| levels_of(&p)) != tool {
            mismatches[0] += 1;
        }
        if days.len() >= COMPOSITE_SESSIONS {
            let tool = profile_of(&days, COMPOSITE_SESSIONS, &ProfileShape::V1, V1_MULTIPLE, 0);
            compared[1] += 1;
            if profiles.composite_5d.clone().map(|p| levels_of(&p)) != tool {
                mismatches[1] += 1;
            }
        }
        if let Some(out) = &mut profile_out {
            match &profiles.prior_day {
                FeatureValue::Ready(prior) => writeln!(out, "{prior}").unwrap(),
                other => writeln!(out, "day={}ms half={half} {other:?}", newest.open).unwrap(),
            }
        }
    }
    if let Some(out) = &mut profile_out {
        out.flush().unwrap();
    }
    let elapsed = started.elapsed();

    println!(
        "window {} .. {} ({:.0} days), halves split at {}, w = {TOLERANCE_BPS} bps",
        EventTime::from_millis(from),
        EventTime::from_millis(to),
        (to - from) as f64 / DAY_MS as f64,
        EventTime::from_millis(split)
    );
    println!(
        "A0: {events} events{}, {rejections} domain rejections; engine/tool mismatches prior_day {} of {}, composite_5d {} of {}",
        if is_w {
            format!(" (W: {W_EVENTS} expected)")
        } else {
            " (not W: count unchecked)".to_owned()
        },
        mismatches[0],
        compared[0],
        mismatches[1],
        compared[1]
    );
    println!("Prices per half: P_min and P_max, and each compared bin in bps there");
    for (half, (low, high)) in price_range.iter().enumerate() {
        let bps = |price: i64| {
            MULTIPLES
                .iter()
                .map(|m| {
                    format!(
                        "{} USDT {:.2}",
                        FINE_USDT * m,
                        (FINE_USDT * m * SCALE) as f64 * 10_000.0 / price as f64
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };
        println!(
            "  {}: P_min {} ({}); P_max {} ({})",
            half_label(half),
            Price::from_units(*low),
            bps(*low),
            Price::from_units(*high),
            bps(*high)
        );
    }
    let profiles = ["prior_day", "composite"];
    let v1_row = rows
        .iter()
        .position(|row| row.label == format!("bin {} USDT", FINE_USDT * MULTIPLES[V1_MULTIPLE]))
        .unwrap();
    println!("Recorded at @1 (bin 10 USDT, t5, 10 %, 70 %, 5 days), per profile and half:");
    for (kind, profile) in profiles.iter().enumerate() {
        for half in 0..2 {
            let line = rows[v1_row].cells[kind][half].line();
            println!("  {profile} {}: {line}", half_label(half));
        }
    }
    println!("  the medians the dropped 1-8 criterion would have used are the hvn/lvn p50 above");
    println!("Sensitivity, one parameter at a time, the others at @1:");
    for row in &mut rows {
        println!(
            "  {} (r_k {} bins, {} USDT):",
            row.label,
            row.resolution,
            row.resolution * row.bin_units() / SCALE
        );
        for (kind, profile) in profiles.iter().enumerate() {
            for half in 0..2 {
                let line = row.cells[kind][half].line();
                println!("    {profile} {}: {line}", half_label(half));
            }
        }
    }
    println!("Mean daily volume per UTC weekday (BTC):");
    let mut c1 = true;
    for (half, days) in weekday.iter().enumerate() {
        let means: Vec<f64> = days.iter().map(|(sum, n)| sum / *n as f64).collect();
        let columns: Vec<String> = WEEKDAYS
            .iter()
            .zip(&means)
            .zip(days)
            .map(|((name, mean), (_, n))| format!("{name} {mean:.0} ({n})"))
            .collect();
        println!("  {}: {}", half_label(half), columns.join(", "));
        let lowest_weekday = means[..5].iter().copied().fold(f64::INFINITY, f64::min);
        c1 &= means[5] < lowest_weekday && means[6] < lowest_weekday;
    }
    println!(
        "wall time {:.1} s, {events} events, {rejections} domain rejections",
        elapsed.as_secs_f64()
    );

    // The rules at `@1`.
    let v1 = &rows[v1_row];
    let a0 =
        rejections == 0 && mismatches == [0, 0] && compared[0] > 0 && (!is_w || events == W_EVENTS);
    let oor: u64 = v1
        .cells
        .iter()
        .flatten()
        .map(|cell| cell.out_of_range)
        .sum();
    let p_min = price_range.iter().map(|range| range.0).min().unwrap();
    let a2 = |bin_units: i64| {
        i128::from(bin_units) * 10_000 <= i128::from(p_min) * i128::from(TOLERANCE_BPS)
    };
    let mut a3 = Vec::new();
    let mut a4 = Vec::new();
    for (kind, profile) in profiles.iter().enumerate() {
        for half in 0..2 {
            let mut cell = v1.cells[kind][half].clone();
            let median = quantile(&mut cell.lvn, 0.5);
            a3.push((
                format!("{profile} {}", half_label(half)),
                median >= 1.0,
                format!("{median:.0}"),
            ));
            for (node, name) in ["hvn", "lvn"].iter().enumerate() {
                let kappa = cell.kappa[node].value();
                a4.push((
                    format!("{name} {profile} {}", half_label(half)),
                    kappa >= KAPPA_PASS,
                    format!("{kappa:.3}"),
                ));
            }
        }
    }
    let cells = |rule: &[(String, bool, String)]| {
        rule.iter()
            .map(|(name, pass, value)| {
                format!("{name} {value}{}", if *pass { "" } else { " (fails)" })
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    println!(
        "Bin sizes against A2 (bin · 10 000 ≤ P_min · w, P_min {}):",
        Price::from_units(p_min)
    );
    for m in MULTIPLES {
        println!(
            "  {} USDT: {}",
            FINE_USDT * m,
            verdict(a2(FINE_USDT * m * SCALE))
        );
    }
    println!("A0 {}: valid run", verdict(a0));
    println!(
        "A1 {}: {oor} OutOfRange among the @1 prior-day and composite profiles",
        verdict(oor == 0)
    );
    println!(
        "A2 {}: {} · 10 000 ≤ {} · {TOLERANCE_BPS}",
        verdict(a2(BIN_SIZE.units())),
        BIN_SIZE,
        Price::from_units(p_min)
    );
    println!(
        "A3 {}: median LVN count per profile: {}",
        verdict(a3.iter().all(|cell| cell.1)),
        cells(&a3)
    );
    println!(
        "A4 {}: κ ≥ {KAPPA_PASS}: {}",
        verdict(a4.iter().all(|cell| cell.1)),
        cells(&a4)
    );
    println!(
        "A5 compared outside the tool: MIE_MEASURE_PROFILE_OUT against tools/reference/volume_profile_reference.py"
    );
    println!(
        "C1 {}: Saturday and Sunday below every weekday in both halves",
        if c1 { "HOLDS" } else { "FAILS" }
    );
    assert!(
        mismatches == [0, 0] && rejections == 0,
        "A0 void: the tool disagrees with the engine or the engine rejected events"
    );
}

#[test]
fn the_chance_share_matches_a_bin_by_bin_count() {
    // Against the definition: a bin counts when a node is within `w` of
    // its midpoint.
    let node = |units: i64| ProfileNode {
        price: Price::from_units(units),
        low: Price::from_units(units),
        high: Price::from_units(units),
        volume: Qty::from_units(1),
        prominence_permille: 1000,
    };
    for (low, bins, bin_units, prices) in [
        (60_000, 400, 10, vec![60_005, 60_155, 60_175, 63_995]),
        (100_000, 50, 50, vec![100_020, 101_000]),
        (20_000, 1_000, 5, vec![20_000, 20_100, 24_999]),
        (60_000, 10, 10, vec![]),
    ] {
        let unit = |usdt: i64| usdt * SCALE;
        let nodes: Vec<ProfileNode> = prices.iter().map(|price| node(unit(*price))).collect();
        let (low, high) = (
            Price::from_units(unit(low)),
            Price::from_units(unit(low) + bins * unit(bin_units)),
        );
        let counted = (0..bins)
            .filter(|i| {
                let mid =
                    Price::from_units(low.units() + i * unit(bin_units) + unit(bin_units) / 2);
                nodes.iter().any(|node| within(node.price, mid))
            })
            .count();
        let share = chance_share(&nodes, low, high, unit(bin_units));
        assert_eq!(share, counted as f64 / bins as f64, "{prices:?}");
    }
}
