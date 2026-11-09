//! The P1 replication of #205 (ADR-064): the coverage and measurement
//! entries over the 2020-01-01 … 2024-09-30 window, and their fixture
//! tests.
//!
//! The module reuses the ADR-058 tool around it unchanged (through
//! `super::`): the P1 population, the chaining of episodes and the unit
//! count (D6), the strata, the stratified effect E (D4) and the paired day
//! bootstrap (D5). It adds what ADR-064 pre-registers:
//!
//! - the window and provenance guard (D1), run by both entries before they
//!   print anything;
//! - the 48 contexts: P1-buy and P1-sell × effort window × regime × 4h and
//!   1d (D2);
//! - the per-label taker fee, T2 on the units' representatives, the T3
//!   rotation null within blocks of whole weeks, Benjamini–Hochberg over the
//!   48 contexts and the reading (D3, D4);
//! - a coverage path whose input types carry no outcome, so the compiler
//!   keeps it blind (D5), with its own twin.
//!
//! Every constant and function that decides the replication lives in one
//! region between the `ADR-064 frozen decisions` markers, whose SHA-256 is
//! pinned next to the texts' and printed by both entries (D6).
//!
//! The queries are `docs/research/205-p1-replication.sql`, run after the
//! ADR-058 file. The two ignored entries:
//!
//! ```sh
//! MIE_OUTCOME_DATASET=<dir> cargo test --release -p mie-cli \
//!     --test order_flow_study replication::coverage_p1_replication -- --ignored --nocapture
//! MIE_OUTCOME_DATASET=<dir> cargo test --release -p mie-cli \
//!     --test order_flow_study replication::measure_p1_replication -- --ignored --nocapture
//! ```
//!
//! The measurement (#206) runs only on the dataset [`REPLICATION_PINS`]
//! names (set by commit 2 of #205 from the coverage run); with an empty pin
//! it panics "not pinned" before it opens a dataset.

use super::*;
use mie_adapter_parquet::outcome::OutcomeManifest;
use sha2::{Digest, Sha256};
use std::fmt::Write as _;

// ===========================================================================
// ADR-064 frozen decisions: begin
//
// Every constant and function that decides the replication (D1 guard, D2
// grid, D3 statistics, D4 reading). `the_frozen_texts_equal_their_pins`
// pins the SHA-256 of the text from this marker to the end marker
// (D6), and both entries print it.

// --- D1: the window and the provenance.

/// D1: records = replay = 2020-01-01 … 2024-09-30, `[start, end)` in ms.
const REPLICATION_RECORDS: (i64, i64) = (1_577_836_800_000, 1_727_740_800_000);

/// D1: the calibration slice starts 2024-10-01 (`docs/direction.md`).
const CALIBRATION_START_MS: i64 = 1_727_740_800_000;

/// D1: the calibration slice's content hash, which no input may carry.
const CALIBRATION_CONTENT: &str =
    "c20761ef7e96acd21e3f6dae04cb9f2f20efb34c058dda1e775264e608f892ac";

/// D1: `aggTrades` only, folded.
const REPLICATION_SOURCE: &str =
    "archive binance-archive/BTCUSDT streams aggTrades correction fold";

/// D1: engine, sampling and labeling with their fingerprints, ADR-058 D1's.
const REPLICATION_PROVENANCE: [&str; 5] = [
    "4c844593779da2e3-1",
    "conditional.sampling@1",
    "2c71fff23e8b0415",
    "conditional.labels@2",
    "6f90bff079e746f2",
];

// --- The inputs and the D3 settings.

/// The `docs/direction.md` inputs the ADR names: c, μ in R, the taker
/// round trip in bps, and the false discovery rate (addendum 2026-11-08).
const CONFIDENCE: f64 = 0.95;

const MU: f64 = 0.10;

const FEE_BPS: f64 = 10.0;

const FDR: f64 = 0.05;

/// D3 T2: resamples, the primary block and the two sensitivity blocks in
/// days, and the seed.
const T2_RESAMPLES: usize = 10_000;

const T2_BLOCKS: [usize; 3] = [56, 28, 112];

const REPLICATION_SEED: u64 = 205;

/// D3 T3: draws, and the primary and sensitivity block lengths in weeks.
const T3_DRAWS: usize = 1_000_000;

const NULL_WEEKS: [i64; 2] = [8, 4];

const WEEK_MS: i64 = 7 * DAY_MS;

/// The replication horizons (D2) as indices into [`HORIZONS`]: 4h, 1d.
const REP_HORIZONS: [usize; 2] = [3, 4];

/// The P1 pairs, in context order; their index is the context's side.
const P1_PAIRS: [&str; 2] = ["P1-buy", "P1-sell"];

/// The regime index of `not_ready` in [`REGIMES`]: in no context.
const NOT_READY: usize = 4;

// --- D1: the guard.

/// Whether `code` is a clean `git:` line: 40 lowercase hex, no `-dirty`.
fn clean_code(code: &str) -> bool {
    code.strip_prefix("git:").is_some_and(|hex| {
        hex.len() == 40
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

/// D1's manifest checks, shared by coverage and measurement.
fn guard(manifest: &OutcomeManifest) -> Result<(), String> {
    if manifest.replay.1 > CALIBRATION_START_MS || manifest.records.1 > CALIBRATION_START_MS {
        return Err(format!(
            "replay {:?} or records {:?} reach the calibration slice at {CALIBRATION_START_MS}",
            manifest.replay, manifest.records
        ));
    }
    if manifest.records != REPLICATION_RECORDS || manifest.replay != REPLICATION_RECORDS {
        return Err(format!(
            "replay {:?} and records {:?} are not the window {REPLICATION_RECORDS:?}",
            manifest.replay, manifest.records
        ));
    }
    if manifest.source != REPLICATION_SOURCE {
        return Err(format!(
            "source {:?}, not {REPLICATION_SOURCE:?}",
            manifest.source
        ));
    }
    let provenance = [
        manifest.engine.as_str(),
        manifest.sampling.as_str(),
        manifest.sampling_fingerprint.as_str(),
        manifest.labeling.as_str(),
        manifest.labeling_fingerprint.as_str(),
    ];
    if provenance != REPLICATION_PROVENANCE {
        return Err(format!(
            "provenance {provenance:?}, not ADR-058 D1's {REPLICATION_PROVENANCE:?}"
        ));
    }
    if !clean_code(&manifest.code) {
        return Err(format!("code {:?} is not a clean git line", manifest.code));
    }
    if manifest.content == CALIBRATION_CONTENT {
        return Err("the dataset is the calibration slice".to_owned());
    }
    Ok(())
}

// --- D2: the grid.

/// One of the 48 contexts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Context {
    /// Index into [`P1_PAIRS`].
    side: usize,
    /// Index into [`WINDOWS`].
    window: usize,
    /// Index into [`REGIME_SPLITS`].
    regime: usize,
    /// Index into [`REP_HORIZONS`].
    horizon: usize,
}

impl Context {
    fn label(&self) -> String {
        format!(
            "{} w={} {} {}",
            P1_PAIRS[self.side],
            WINDOWS[self.window].0,
            REGIME_SPLITS[self.regime],
            self.horizon_name()
        )
    }

    fn horizon_name(&self) -> &'static str {
        HORIZONS[REP_HORIZONS[self.horizon]].0
    }

    fn horizon_ms(&self) -> i64 {
        HORIZONS[REP_HORIZONS[self.horizon]].1
    }

    /// σ: +1 when the hypothesis barrier is the upper one (P1-sell), −1
    /// when it is the lower one (P1-buy, ADR-058 D3).
    fn sigma(&self) -> f64 {
        if self.side == 0 { -1.0 } else { 1.0 }
    }

    /// The ADR-058 population (pair × window) in `populations()`.
    fn population(&self) -> usize {
        self.side * WINDOWS.len() + self.window
    }
}

/// The 48 contexts in their fixed order: side, window, regime, horizon.
fn contexts() -> Vec<Context> {
    let mut all = Vec::with_capacity(48);
    for side in 0..P1_PAIRS.len() {
        for window in 0..WINDOWS.len() {
            for regime in 0..REGIME_SPLITS.len() {
                for horizon in 0..REP_HORIZONS.len() {
                    all.push(Context {
                        side,
                        window,
                        regime,
                        horizon,
                    });
                }
            }
        }
    }
    all
}

// --- D3: T2, the fee and the information figures.

/// ADR-058 D6 stage 2 as [`unit_count`], returning the counted
/// representatives' indices into `members` instead of their count.
fn representatives(members: &[(i64, u64, bool)], horizon_ms: i64) -> Vec<usize> {
    let mut seen = BTreeSet::new();
    let mut out = Vec::new();
    let mut last: Option<i64> = None;
    for (i, &(decision, episode, ran)) in members.iter().enumerate() {
        if !ran || !seen.insert(episode) {
            continue;
        }
        if last.is_none_or(|last| decision >= last + horizon_ms) {
            out.push(i);
            last = Some(decision);
        }
    }
    out
}

/// The weeks of a records window.
fn weeks_of(window: (i64, i64)) -> f64 {
    (window.1 - window.0) as f64 / WEEK_MS as f64
}

fn units_per_week(units: u64, weeks: f64) -> f64 {
    units as f64 / weeks
}

/// The per-label fee in R: the round trip in bps over the label's own
/// barrier half-width in bps.
fn label_fee(fee_bps: f64, half_bps: f64) -> f64 {
    fee_bps / half_bps
}

/// s = sign(E); none when E is 0 or undefined.
fn direction(effect: Option<f64>) -> Option<f64> {
    match effect {
        Some(e) if e > 0.0 => Some(1.0),
        Some(e) if e < 0.0 => Some(-1.0),
        _ => None,
    }
}

/// T2's net outcome of a trade held to H in direction s: s·g − f, where
/// the gross g in R toward the hypothesis barrier is +1 / −1 when the
/// hypothesis / counter barrier came first (the barriers are symmetric,
/// ADR-056 D4′), and for `neither` the mark to market at H, σ·m, with
/// m = (last − reference) / (upper − reference) the window's last trade in
/// half-widths toward the upper barrier and σ = +1 for an upper hypothesis,
/// −1 for a lower one. A `neither` without a last trade is marked at the
/// reference (m = 0). E and T3 keep ADR-058 D4's 0 for `neither`.
fn net_outcome(outcome: Outcome, s: f64, sigma: f64, mark: Option<f64>, fee: f64) -> f64 {
    let gross = match outcome {
        Outcome::Hypothesis => 1.0,
        Outcome::Counter => -1.0,
        Outcome::Neither => sigma * mark.unwrap_or(0.0),
        other => panic!("a unit's race ran, not {other:?}"),
    };
    s * gross - fee
}

/// The units' net outcomes per day: `(day, Σ net, count)`, by day.
fn net_days(nets: &[(usize, f64)]) -> Vec<(usize, f64, i64)> {
    let mut map: BTreeMap<usize, (f64, i64)> = BTreeMap::new();
    for &(day, net) in nets {
        let entry = map.entry(day).or_default();
        entry.0 += net;
        entry.1 += 1;
    }
    map.into_iter().map(|(d, (s, n))| (d, s, n)).collect()
}

/// T2's net expectancy under every resample's day weights; `None` when a
/// resample draws no unit.
fn t2_values(days: &[(usize, f64, i64)], weights: &[Vec<u32>]) -> Vec<Option<f64>> {
    weights
        .iter()
        .map(|w| {
            let (mut sum, mut n) = (0.0, 0i64);
            for &(day, s, count) in days {
                let k = w[day];
                if k > 0 {
                    sum += f64::from(k) * s;
                    n += i64::from(k) * count;
                }
            }
            (n > 0).then(|| sum / n as f64)
        })
        .collect()
}

/// The maximum drawdown of the cumulative sum of `nets`, from 0.
fn max_drawdown(nets: &[f64]) -> f64 {
    let (mut cum, mut peak, mut drawdown) = (0.0f64, 0.0f64, 0.0f64);
    for x in nets {
        cum += x;
        peak = peak.max(cum);
        drawdown = drawdown.max(peak - cum);
    }
    drawdown
}

/// Net R per year at `units_per_week` units a week (the addendum's
/// arithmetic: μ × 2 a week × 52 = 10.4 R).
fn net_r_per_year(expectancy: f64, units_per_week: f64) -> f64 {
    expectancy * units_per_week * 52.0
}

// --- D3: Benjamini–Hochberg.

/// Benjamini–Hochberg at level `q` over `M` p-values: reject the k
/// smallest, k the largest rank with p₍ₖ₎ ≤ k·q/M. Ties keep the input
/// order (the context order), which does not change the decisions.
fn bh<const M: usize>(p: &[f64; M], q: f64) -> [bool; M] {
    let mut order: [usize; M] = std::array::from_fn(|i| i);
    order.sort_by(|&a, &b| p[a].total_cmp(&p[b]));
    let mut k = 0;
    for (rank, &i) in order.iter().enumerate() {
        if p[i] <= (rank + 1) as f64 * q / M as f64 {
            k = rank + 1;
        }
    }
    let mut out = [false; M];
    for &i in &order[..k] {
        out[i] = true;
    }
    out
}

// --- D3: T3, the rotation null within blocks of whole weeks.

/// The race of a baseline record when it did not run.
const NOT_RAN: i8 = i8::MIN;

/// The baseline as the null reads it, in minute order.
#[derive(Clone, Debug, Default)]
struct NullFrame {
    /// Strictly increasing: one baseline record per closed minute.
    minute_end: Vec<i64>,
    /// Index into [`REGIMES`].
    regime: Vec<u8>,
    stratum: Vec<u16>,
    /// Per replication horizon: +1 upper, −1 lower, 0 neither, or
    /// [`NOT_RAN`].
    race: [Vec<i8>; 2],
}

impl NullFrame {
    /// The stratum tilts (n_upper − n_lower) / n per horizon, regime label
    /// and stratum among the frame's races that ran; `None` when none ran.
    fn tilts(&self) -> Vec<Option<f64>> {
        let mut counts = vec![(0i64, 0i64); REP_HORIZONS.len() * REGIME_SPLITS.len() * STRATA];
        for j in 0..self.minute_end.len() {
            let r = usize::from(self.regime[j]);
            if r == NOT_READY {
                continue;
            }
            for h in 0..REP_HORIZONS.len() {
                let d = self.race[h][j];
                if d != NOT_RAN {
                    let c = &mut counts[strata_index(h, r, usize::from(self.stratum[j]))];
                    c.0 += i64::from(d);
                    c.1 += 1;
                }
            }
        }
        counts
            .into_iter()
            .map(|(diff, n)| (n > 0).then(|| diff as f64 / n as f64))
            .collect()
    }

    fn find(&self, minute_end: i64) -> Option<usize> {
        self.minute_end.binary_search(&minute_end).ok()
    }
}

/// The block layout: whole weeks from `origin`, cut into blocks of `weeks`
/// weeks, the last one shorter; the days after the last whole week never
/// rotate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Layout {
    origin: i64,
    whole_weeks: i64,
    weeks: i64,
}

impl Layout {
    fn new(window: (i64, i64), weeks: i64) -> Self {
        Self {
            origin: window.0,
            whole_weeks: (window.1 - window.0).div_euclid(WEEK_MS),
            weeks,
        }
    }

    /// The rotating blocks' lengths in weeks.
    fn lengths(&self) -> Vec<i64> {
        let mut lengths = vec![self.weeks; (self.whole_weeks / self.weeks) as usize];
        if self.whole_weeks % self.weeks > 0 {
            lengths.push(self.whole_weeks % self.weeks);
        }
        lengths
    }

    /// The rotating block of `t` with its start and length in weeks;
    /// `None` outside the whole weeks (the fixed tail).
    fn block(&self, t: i64) -> Option<(usize, i64, i64)> {
        let offset = t - self.origin;
        if offset < 0 || offset >= self.whole_weeks * WEEK_MS {
            return None;
        }
        let b = offset / (self.weeks * WEEK_MS);
        let start = self.origin + b * self.weeks * WEEK_MS;
        Some((
            b as usize,
            start,
            self.weeks.min(self.whole_weeks - b * self.weeks),
        ))
    }

    /// `t` rotated by `k` whole weeks, circular within its block.
    fn rotate(&self, t: i64, k: i64) -> i64 {
        match self.block(t) {
            None => t,
            Some((_, start, length)) => {
                start + (t - start + k * WEEK_MS).rem_euclid(length * WEEK_MS)
            }
        }
    }
}

/// Per block and rotation: Σ y and the count of the eligible
/// pseudo-members. The last block is the fixed tail (identity only).
#[derive(Clone, Debug, PartialEq)]
struct RotationTable {
    blocks: Vec<Vec<(f64, i64)>>,
}

impl RotationTable {
    fn at(&self, rotations: &[usize]) -> (f64, i64) {
        let (mut sum, mut n) = (0.0, 0);
        for (block, &k) in self.blocks.iter().zip(rotations) {
            let (s, c) = block[k];
            sum += s;
            n += c;
        }
        (sum, n)
    }

    /// The observed E (every block at the identity).
    fn observed(&self) -> Option<f64> {
        let (sum, n) = self.at(&vec![0; self.blocks.len()]);
        (n > 0).then(|| sum / n as f64)
    }
}

/// The rotation table of one context: each member label at minute `t`
/// moved to `rotate(t, k)` for every k of its block; a pseudo-member is
/// the baseline record there when it is eligible (the context's regime,
/// the race at H ran, a stratum holding a baseline race), and adds
/// y = σ·(d − tilt(stratum)) with the observed tilts.
fn rotation_table(
    frame: &NullFrame,
    tilts: &[Option<f64>],
    layout: &Layout,
    labels: &[i64],
    regime: usize,
    h: usize,
    sigma: f64,
) -> RotationTable {
    let lengths = layout.lengths();
    let mut blocks: Vec<Vec<(f64, i64)>> = lengths
        .iter()
        .map(|&l| vec![(0.0, 0); l as usize])
        .collect();
    blocks.push(vec![(0.0, 0)]);
    let tail = lengths.len();
    for &t in labels {
        let (b, length) = layout
            .block(t)
            .map_or((tail, 1), |(b, _, length)| (b, length));
        for k in 0..length {
            let Some(j) = frame.find(layout.rotate(t, k)) else {
                continue;
            };
            let d = frame.race[h][j];
            if usize::from(frame.regime[j]) != regime || d == NOT_RAN {
                continue;
            }
            let Some(tilt) = tilts[strata_index(h, regime, usize::from(frame.stratum[j]))] else {
                continue;
            };
            let cell = &mut blocks[b][k as usize];
            cell.0 += sigma * (f64::from(d) - tilt);
            cell.1 += 1;
        }
    }
    RotationTable { blocks }
}

/// The seed of draw `draw`'s own stream: the draw-th output of
/// SplitMix64(`seed`), so a draw does not depend on the draws before it.
fn draw_seed(seed: u64, draw: usize) -> u64 {
    SplitMix64(seed.wrapping_add((draw as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))).next()
}

/// The rotations of one draw: k uniform in 0 … L−1 per rotating block, in
/// block order, then 0 for the fixed tail.
fn draw_rotations(lengths: &[i64], seed: u64, draw: usize) -> Vec<usize> {
    let mut rng = SplitMix64(draw_seed(seed, draw));
    let mut ks: Vec<usize> = lengths.iter().map(|&l| rng.below(l as usize)).collect();
    ks.push(0);
    ks
}

/// p = min(1, 2·min(p⁺, p⁻)) from the counts of draws at or above and at
/// or below the observed E.
fn two_sided(above: u64, below: u64, draws: usize) -> f64 {
    let b = draws as f64 + 1.0;
    let plus = (1.0 + above as f64) / b;
    let minus = (1.0 + below as f64) / b;
    (2.0 * plus.min(minus)).min(1.0)
}

/// The two-sided rotation p of every table (`None` when its observed E is
/// undefined) from `draws` draws, split over `threads` scoped threads; the
/// counts do not depend on the split. A draw with no eligible
/// pseudo-member counts toward both tails.
fn null_p(
    tables: &[RotationTable],
    lengths: &[i64],
    draws: usize,
    seed: u64,
    threads: usize,
) -> Vec<Option<f64>> {
    let observed: Vec<Option<f64>> = tables.iter().map(RotationTable::observed).collect();
    let chunk = draws.div_ceil(threads.max(1)).max(1);
    let parts: Vec<Vec<(u64, u64)>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..draws)
            .step_by(chunk)
            .map(|from| {
                let observed = &observed;
                scope.spawn(move || {
                    let mut counts = vec![(0u64, 0u64); tables.len()];
                    for draw in from..(from + chunk).min(draws) {
                        let ks = draw_rotations(lengths, seed, draw);
                        for (c, table) in tables.iter().enumerate() {
                            let Some(e) = observed[c] else { continue };
                            let (sum, n) = table.at(&ks);
                            if n == 0 {
                                counts[c].0 += 1;
                                counts[c].1 += 1;
                                continue;
                            }
                            let star = sum / n as f64;
                            counts[c].0 += u64::from(star >= e);
                            counts[c].1 += u64::from(star <= e);
                        }
                    }
                    counts
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut totals = vec![(0u64, 0u64); tables.len()];
    for part in parts {
        for (c, (above, below)) in part.into_iter().enumerate() {
            totals[c].0 += above;
            totals[c].1 += below;
        }
    }
    observed
        .iter()
        .zip(totals)
        .map(|(e, (above, below))| e.map(|_| two_sided(above, below, draws)))
        .collect()
}

// --- D3, D4: the measurement and its reading.

/// The measurement's settings: draws and threads are parameters so the
/// fixtures run small.
#[derive(Clone, Copy, Debug, PartialEq)]
struct RunSettings {
    /// μ, which sets n_min: [`MU`] in the run, larger in the fixtures so
    /// that a few days reach it.
    mu: f64,
    t2_resamples: usize,
    d7_resamples: usize,
    draws: usize,
    threads: usize,
}

const RUN: RunSettings = RunSettings {
    mu: MU,
    t2_resamples: T2_RESAMPLES,
    d7_resamples: RESAMPLES,
    draws: T3_DRAWS,
    threads: 4,
};

/// A context's reading (D4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RepReading {
    /// `true`: in the hypothesis direction.
    Replicated(bool),
    NotMeasurable,
    NoEffect,
    NotNetPositive,
    Insufficient,
}

impl RepReading {
    fn name(self) -> &'static str {
        match self {
            Self::Replicated(true) => "replicated (hypothesis)",
            Self::Replicated(false) => "replicated (counter)",
            Self::NotMeasurable => "not measurable",
            Self::NoEffect => "no effect",
            Self::NotNetPositive => "not net positive",
            Self::Insufficient => "insufficient",
        }
    }
}

/// D4: T1, then T3 after BH, then T2 in direction s.
fn read(
    units: u64,
    n_min: u64,
    rejected: bool,
    s: Option<f64>,
    t2: Option<&Interval>,
) -> RepReading {
    if units < n_min {
        return RepReading::NotMeasurable;
    }
    let (Some(s), Some(t2)) = (s, t2) else {
        return RepReading::NoEffect;
    };
    if !rejected {
        return RepReading::NoEffect;
    }
    match t2.reading {
        Reading::Insufficient => RepReading::Insufficient,
        Reading::Hypothesis => RepReading::Replicated(s > 0.0),
        _ => RepReading::NotNetPositive,
    }
}

/// One context's measured result.
#[derive(Clone, Debug)]
struct ContextResult {
    ran: u64,
    dropped: i64,
    episodes: u64,
    units: u64,
    t: Option<f64>,
    t_b: Option<f64>,
    effect: Option<f64>,
    s: Option<f64>,
    /// T3 at 8 and 4 weeks; `None` when not measurable or E undefined.
    p: [Option<f64>; 2],
    /// BH at 8 and 4 weeks.
    rejected: [bool; 2],
    /// T2's net expectancy and its intervals at 56, 28 and 112 days.
    expectancy: Option<f64>,
    t2: Vec<Interval>,
    max_drawdown: Option<f64>,
    /// ADR-058 D7's 56-day reading of E.
    d7: Option<Interval>,
    reading: RepReading,
}

/// The measurement of the 48 contexts.
fn measure_contexts(measured: &Measured, run: &RunSettings) -> Vec<ContextResult> {
    let inputs = &measured.inputs;
    let n_min = n_min(CONFIDENCE, run.mu);
    let populations = populations();
    let episodes = member_episodes(inputs, &populations);
    let cells = replication_cells(inputs);
    let days: BTreeSet<i64> = inputs
        .baseline
        .iter()
        .map(|b| b.decision)
        .chain(inputs.members.iter().map(|m| m.decision))
        .map(|ms| ms.div_euclid(DAY_MS))
        .collect();
    let day_index: BTreeMap<i64, usize> = days.iter().enumerate().map(|(i, d)| (*d, i)).collect();
    let baseline_days = BaselineDays::new(inputs, &day_index);
    let prepared: Vec<Prepared> = cells
        .iter()
        .map(|cell| prepare(inputs, cell, &day_index))
        .collect();
    let ones = vec![1u32; day_index.len()];
    let mut totals = Vec::new();
    baseline_days.totals(&ones, &mut totals);

    // Counts, E and the units' representatives.
    let mut results: Vec<ContextResult> = Vec::new();
    let mut nets: Vec<Vec<(usize, f64)>> = Vec::new();
    for (c, (context, cell)) in contexts().iter().zip(&cells).enumerate() {
        let (episodes_ran, units) = cell_units(inputs, &episodes, cell);
        let point = estimate(&prepared[c], &ones, &totals);
        let n = point.n();
        let effect = point.effect();
        let s = direction(effect);
        let tuples: Vec<(i64, u64, bool)> = cell
            .members
            .iter()
            .map(|&i| {
                let m = &inputs.members[i];
                (m.decision, episodes[i], m.outcomes[cell.horizon].ran())
            })
            .collect();
        let reps: Vec<usize> = representatives(&tuples, context.horizon_ms())
            .into_iter()
            .map(|k| cell.members[k])
            .collect();
        assert_eq!(reps.len() as u64, units, "{}", context.label());
        let unit_nets: Vec<(usize, f64)> = match s {
            Some(s) if units >= n_min => reps
                .iter()
                .map(|&i| {
                    let m = &inputs.members[i];
                    let half = measured.half_bps[i][context.horizon].expect("barriers");
                    (
                        day_index[&m.decision.div_euclid(DAY_MS)],
                        net_outcome(
                            m.outcomes[cell.horizon],
                            s,
                            context.sigma(),
                            measured.marks[i][context.horizon],
                            label_fee(FEE_BPS, half),
                        ),
                    )
                })
                .collect(),
            _ => Vec::new(),
        };
        let ran = cell
            .members
            .iter()
            .filter(|&&i| inputs.members[i].outcomes[cell.horizon].ran())
            .count() as u64;
        results.push(ContextResult {
            ran,
            dropped: point.dropped,
            episodes: episodes_ran,
            units,
            t: (n > 0).then(|| (point.hypothesis - point.counter) as f64 / n as f64),
            t_b: (n > 0).then(|| point.baseline_sum / n as f64),
            effect,
            s,
            p: [None, None],
            rejected: [false, false],
            expectancy: (!unit_nets.is_empty())
                .then(|| unit_nets.iter().map(|(_, x)| x).sum::<f64>() / unit_nets.len() as f64),
            t2: Vec::new(),
            max_drawdown: (!unit_nets.is_empty())
                .then(|| max_drawdown(&unit_nets.iter().map(|(_, x)| *x).collect::<Vec<_>>())),
            d7: None,
            reading: RepReading::NotMeasurable,
        });
        nets.push(unit_nets);
    }

    // T3 and BH, at 8 and 4 weeks.
    let frame = null_frame(measured);
    let tilts = frame.tilts();
    for (n, weeks) in NULL_WEEKS.iter().enumerate() {
        let layout = Layout::new(measured.window, *weeks);
        let measurable: Vec<usize> = (0..results.len())
            .filter(|&c| results[c].units >= n_min && results[c].effect.is_some())
            .collect();
        let tables: Vec<RotationTable> = measurable
            .iter()
            .map(|&c| {
                let context = contexts()[c];
                let labels: Vec<i64> = cells[c]
                    .members
                    .iter()
                    .map(|&i| inputs.members[i].minute_end.unwrap())
                    .collect();
                rotation_table(
                    &frame,
                    &tilts,
                    &layout,
                    &labels,
                    context.regime,
                    context.horizon,
                    context.sigma(),
                )
            })
            .collect();
        for (k, &c) in measurable.iter().enumerate() {
            let rotation = tables[k].observed().unwrap();
            let effect = results[c].effect.unwrap();
            assert!(
                (rotation - effect).abs() <= 1e-9,
                "{}: E {effect} at the identity rotation {rotation}",
                contexts()[c].label()
            );
        }
        let p = null_p(
            &tables,
            &layout.lengths(),
            run.draws,
            REPLICATION_SEED,
            run.threads,
        );
        let mut all = [1.0f64; 48];
        for (k, &c) in measurable.iter().enumerate() {
            results[c].p[n] = p[k];
            all[c] = p[k].unwrap_or(1.0);
        }
        for (c, rejected) in bh(&all, FDR).into_iter().enumerate() {
            results[c].rejected[n] = rejected;
        }
    }

    // T2 at 56, 28 and 112 days.
    let settings = Settings {
        confidence: CONFIDENCE,
        mu: run.mu,
        fee_bps: FEE_BPS,
        resamples: run.t2_resamples,
        blocks: [T2_BLOCKS[0], T2_BLOCKS[1]],
        seed: REPLICATION_SEED,
        threads: run.threads,
    };
    let per_day: Vec<Vec<(usize, f64, i64)>> = nets.iter().map(|n| net_days(n)).collect();
    for block in T2_BLOCKS {
        let weights = day_weights(day_index.len().max(1), block, &settings);
        for (c, result) in results.iter_mut().enumerate() {
            if nets[c].is_empty() {
                continue;
            }
            let values = t2_values(&per_day[c], &weights);
            result
                .t2
                .push(interval(&values, result.units, n_min, CONFIDENCE));
        }
    }

    // ADR-058 D7's 56-day reading of E, with ADR-058 D5's settings.
    let d7_settings = Settings {
        resamples: run.d7_resamples,
        blocks: BLOCKS,
        seed: SEED,
        ..settings
    };
    let weights = day_weights(day_index.len().max(1), BLOCKS[0], &d7_settings);
    let resampled = bootstrap(&baseline_days, &prepared, &weights, run.threads);
    for (c, result) in results.iter_mut().enumerate() {
        if result.units >= n_min {
            result.d7 = Some(interval(&resampled[c], result.units, n_min, CONFIDENCE));
        }
        result.reading = read(
            result.units,
            n_min,
            result.rejected[0],
            result.s,
            result.t2.first(),
        );
    }
    results
}

// --- D6: the measurement entry's path, with the pre-registered settings.

/// The #206 measurement of the replication dataset in `dir`: the pins of
/// the pre-registration record and the settings [`RUN`], both fixed here.
fn measure_replication(dir: &Path) -> Result<String, String> {
    measure_run(dir, &REPLICATION_PINS, &RUN)
}

/// The measurement run (#206): the pins first, then, on the replication
/// dataset, the pre-registered settings [`RUN`] and a clean build, then the
/// frozen hashes, the guard, the counts, the record sets and the twin; only
/// then the tables. Prints nothing. Fixtures pass their own pins and
/// settings.
fn measure_run(dir: &Path, pins: &Pins, run: &RunSettings) -> Result<String, String> {
    let (Some(data), Some(content), Some(contexts_pin), Some(ran_pin)) =
        (pins.data, pins.content, pins.contexts, pins.ran)
    else {
        panic!("ADR-064 measurement: not pinned (the pre-registration record sets the pins)");
    };
    if *pins == REPLICATION_PINS {
        if *run != RUN {
            return Err(format!(
                "the replication dataset is measured only with RUN, not {run:?}"
            ));
        }
        if !clean_code(conditional::CODE_VERSION) {
            return Err(format!(
                "the replication dataset is measured only from a clean build, not {}",
                conditional::CODE_VERSION
            ));
        }
    }
    for (what, text, pinned) in [
        ("028 queries", sha256(STUDY_SQL), STUDY_SQL_SHA256),
        (
            "205 queries",
            sha256(REPLICATION_SQL),
            REPLICATION_SQL_SHA256,
        ),
        (
            "ADR-064 frozen text",
            sha256(frozen(ADR_064)),
            ADR_FROZEN_SHA256,
        ),
        (
            "replication.rs frozen decisions",
            sha256(frozen_code(THIS_FILE)),
            CODE_FROZEN_SHA256,
        ),
    ] {
        if text != pinned {
            return Err(format!("{what} sha256 {text}, pinned {pinned}"));
        }
    }
    let (manifest, views) = open_guarded(dir)?;
    if manifest.data != data || manifest.content != content {
        return Err(format!(
            "data {} and content {} are not the pinned {data} and {content}",
            manifest.data, manifest.content
        ));
    }
    views
        .set_threads(run.threads as u32)
        .map_err(|e| e.to_string())?;
    create_replication(&views)?;
    let (contexts_count, effort, ran) = dataset_counts(&views);
    if contexts_count != contexts_pin || effort != 3 * contexts_pin.0 || ran != ran_pin {
        return Err(format!(
            "counts {contexts_count:?}, effort {effort}, ran {ran:?}; pinned {contexts_pin:?}, \
             {}, {ran_pin:?}",
            3 * contexts_pin.0
        ));
    }
    let difference = record_set_difference(&views);
    if difference > 0 {
        return Err(format!(
            "rep_coverage_members and the P1 rows of study_members differ in {difference} rows"
        ));
    }
    let measured = load_measured(&views, manifest.records);
    let mismatches = p1_twin_mismatches(&views, &measured.inputs);
    if mismatches > 0 {
        return Err(format!(
            "the ADR-058 twin disagrees on the P1 rows ({mismatches}); nothing is printed"
        ));
    }
    let results = measure_contexts(&measured, run);
    let mut out = String::new();
    let _ = writeln!(out, "# P1 replication (#206, ADR-064)\n");
    out.push_str(&identity(&manifest));
    let _ = writeln!(
        out,
        "queries sha256 028 {STUDY_SQL_SHA256}, 205 {REPLICATION_SQL_SHA256}; ADR-064 frozen \
         {ADR_FROZEN_SHA256}; replication.rs frozen decisions sha256 {}; record sets equal; \
         ADR-058 twin mismatches 0\n",
        sha256(frozen_code(THIS_FILE))
    );
    out.push_str(&measurement_body(&measured, &results, run));
    Ok(out)
}

// ADR-064 frozen decisions: end
// ===========================================================================

// ---------------------------------------------------------------------------
// The texts ADR-064 freezes, their hashes, and the dataset pins.

/// The replication queries (ADR-064 D5, *Changes*).
const REPLICATION_SQL: &str = include_str!("../../../../docs/research/205-p1-replication.sql");

/// The pre-registration (ADR-064 D6 freezes its text up to the record).
const ADR_064: &str = include_str!("../../../../docs/adr/064-p1-replication-pre-registration.md");

/// D6: SHA-256 of `028-order-flow-interactions.sql` (ADR-058 *Acceptance*),
/// of `205-p1-replication.sql`, and of ADR-064's frozen text.
const STUDY_SQL_SHA256: &str = "dd32d605081d6c80992655e16f453d143ee4e3ba5b17f4bbe399fcee0e7144a1";
const REPLICATION_SQL_SHA256: &str =
    "5e742fed2fba222bb29f501de22bddb3c5d2a399186c495fd574118df7ec74c3";
const ADR_FROZEN_SHA256: &str = "6ed0f6afb3688c99ebc501eb1530e0cd088122fa3e7a1e4a067bc08fd6dcafae";

/// This file: D6 freezes its decision region, between the markers.
const THIS_FILE: &str = include_str!("replication.rs");

/// D6: SHA-256 of [`frozen_code`] of this file.
const CODE_FROZEN_SHA256: &str = "ece00cf22aaaa527f2c7f83fe5eb08978267c5599eac203781de5e39ccbd1f7c";

/// The measurement pins (D1), set by commit 2 of #205 from the coverage
/// run: data version, content hash, contexts / baseline / trigger records,
/// and the races that ran at 4h and 1d.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Pins {
    data: Option<&'static str>,
    content: Option<&'static str>,
    contexts: Option<(u64, u64, u64)>,
    ran: Option<[u64; 2]>,
}

const UNPINNED: Pins = Pins {
    data: None,
    content: None,
    contexts: None,
    ran: None,
};

/// The replication dataset of #205's coverage run (ADR-064
/// *Pre-registration record*, `docs/research/205-p1-replication-coverage.md`).
const REPLICATION_PINS: Pins = Pins {
    data: Some("7e92345c663e72e096cbdf2921f60eaa8d9cbae291cd2f1f82bdd64306d27d10"),
    content: Some("e985ea3e9efa9cc9aaf15dbc013fe76eb76711e811a4d3f414e79b591402049f"),
    contexts: Some((2_638_166, 2_498_200, 139_966)),
    ran: Some([2_634_304, 2_613_978]),
};

// ---------------------------------------------------------------------------
// Opening a dataset behind the guard; hashes and counts.

/// Verifies `dir`, applies the guard and opens the views; prints nothing.
fn open_guarded(dir: &Path) -> Result<(OutcomeManifest, OutcomeViews), String> {
    let mut text = Vec::new();
    if !conditional::verify(dir, &mut text)? {
        return Err(format!(
            "verify failed: {}",
            String::from_utf8_lossy(&text).trim_end()
        ));
    }
    let manifest = OutcomeDataset::open(dir)
        .map_err(|e| e.to_string())?
        .manifest()
        .clone();
    guard(&manifest)?;
    let views = OutcomeViews::open(dir).map_err(|e| e.to_string())?;
    Ok((manifest, views))
}

/// The study views, then the replication views, without exclusions.
fn create_replication(views: &OutcomeViews) -> Result<(), String> {
    create_study(views, &[]);
    views
        .create_views(REPLICATION_SQL)
        .map_err(|e| e.to_string())
}

fn sha256(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .fold(String::new(), |mut s, b| {
            let _ = write!(s, "{b:02x}");
            s
        })
}

/// D6: the frozen text, from `## Context` to the record.
/// D6: the decision region of `source`, from the begin marker's line to the
/// end of the end marker's line. The markers are built with `concat!` so
/// that this function's own text does not contain them.
fn frozen_code(source: &str) -> &str {
    let begin = concat!("// ADR-064 frozen decisions", ": begin");
    let end = concat!("// ADR-064 frozen decisions", ": end");
    let start = source.find(begin).expect("the begin marker");
    assert_eq!(source.matches(begin).count(), 1, "one begin marker");
    assert_eq!(source.matches(end).count(), 1, "one end marker");
    let stop = source.find(end).expect("the end marker") + end.len();
    assert!(start < stop);
    &source[start..stop]
}

fn frozen(adr: &str) -> &str {
    let start = adr.find("\n## Context\n").expect("a Context section") + 1;
    let end = adr
        .find("\n## Pre-registration record\n")
        .expect("a Pre-registration record section")
        + 1;
    &adr[start..end]
}

/// The dataset counts the pins hold: contexts, baseline, trigger, effort
/// rows, and the races that ran at 4h and 1d. Reads no outcome.
fn dataset_counts(views: &OutcomeViews) -> ((u64, u64, u64), u64, [u64; 2]) {
    let mut contexts = (0, 0, 0);
    each_row(
        &tsv_lines(
            views,
            "SELECT count(*) AS contexts, count(*) FILTER (WHERE sample = 'baseline') AS baseline, \
             count(*) FILTER (WHERE sample = 'trigger') AS trigger FROM contexts",
        ),
        |get| {
            contexts = (
                get("contexts").parse().unwrap(),
                get("baseline").parse().unwrap(),
                get("trigger").parse().unwrap(),
            );
        },
    );
    let effort = tsv_lines(views, "SELECT count(*) AS rows FROM effort")[1]
        .parse()
        .unwrap();
    let mut ran = [0, 0];
    each_row(
        &tsv_lines(
            views,
            "SELECT count(*) FILTER (WHERE ran AND horizon = '4h') AS ran_4h, \
             count(*) FILTER (WHERE ran AND horizon = '1d') AS ran_1d FROM rep_ran",
        ),
        |get| {
            ran = [
                get("ran_4h").parse().unwrap(),
                get("ran_1d").parse().unwrap(),
            ]
        },
    );
    (contexts, effort, ran)
}

// ---------------------------------------------------------------------------
// Coverage (D5): types without an outcome.

/// A P1 row as coverage reads it (`rep_coverage_members`): no race, no
/// mapped outcome, only whether each race ran.
#[derive(Clone, Debug, PartialEq)]
struct CoverageMember {
    side: usize,
    window: usize,
    record: u64,
    decision: i64,
    minute_end: i64,
    /// Index into [`REGIMES`].
    regime: usize,
    /// Index into [`LOCATIONS`].
    location: usize,
    ran: [bool; 2],
    half_bps: [Option<f64>; 2],
}

/// A baseline record as coverage reads it (`rep_coverage_baseline`).
#[derive(Clone, Debug, PartialEq)]
struct CoverageBaseline {
    decision: i64,
    regime: usize,
    location: usize,
    ran: [bool; 2],
    half_bps: [Option<f64>; 2],
}

/// One row of `rep_coverage_exclusions`.
#[derive(Clone, Debug, PartialEq)]
struct CoverageExclusion {
    window: String,
    records: u64,
    not_ready: u64,
    incomplete: u64,
}

#[derive(Clone, Debug, Default)]
struct CoverageInputs {
    members: Vec<CoverageMember>,
    baseline: Vec<CoverageBaseline>,
    exclusions: Vec<CoverageExclusion>,
    /// The records window, for the weeks and the disjoint-window ceiling.
    window: (i64, i64),
}

fn flag(cell: &str) -> bool {
    match cell {
        "true" => true,
        "false" => false,
        other => panic!("flag {other}"),
    }
}

fn location_index(location: &str) -> usize {
    LOCATIONS
        .iter()
        .position(|l| *l == location)
        .unwrap_or_else(|| panic!("location {location}"))
}

fn window_index(window: &str) -> usize {
    WINDOWS
        .iter()
        .position(|(w, _)| *w == window)
        .unwrap_or_else(|| panic!("window {window}"))
}

fn side_index(pair: &str) -> usize {
    P1_PAIRS
        .iter()
        .position(|p| *p == pair)
        .unwrap_or_else(|| panic!("pair {pair}"))
}

/// `ran_<h>` and `half_bps_<h>` at the replication horizons.
fn ran_widths(get: &dyn Fn(&str) -> String) -> ([bool; 2], [Option<f64>; 2]) {
    let ran = [flag(&get("ran_4h")), flag(&get("ran_1d"))];
    let half = |name: &str| opt(get(name)).map(|v| v.parse::<f64>().unwrap());
    (ran, [half("half_bps_4h"), half("half_bps_1d")])
}

const COVERAGE_MEMBERS: &str = "SELECT * FROM rep_coverage_members ORDER BY pair, \
     list_position(['5m', '15m', '1h'], effort_window), record";

/// Reads the coverage views, streamed. Selects from `rep_coverage_*` only.
fn load_coverage(views: &OutcomeViews, window: (i64, i64)) -> CoverageInputs {
    let mut inputs = CoverageInputs {
        window,
        ..CoverageInputs::default()
    };
    each_selected(views, COVERAGE_MEMBERS, |get| {
        let (ran, half_bps) = ran_widths(get);
        inputs.members.push(CoverageMember {
            side: side_index(&get("pair")),
            window: window_index(&get("effort_window")),
            record: get("record").parse().unwrap(),
            decision: get("decision_ms").parse().unwrap(),
            minute_end: get("minute_end_ms").parse().unwrap(),
            regime: regime_index(&get("regime")),
            location: location_index(&get("location")),
            ran,
            half_bps,
        });
    });
    each_selected(
        views,
        "SELECT * FROM rep_coverage_baseline ORDER BY record",
        |get| {
            let (ran, half_bps) = ran_widths(get);
            inputs.baseline.push(CoverageBaseline {
                decision: get("decision_ms").parse().unwrap(),
                regime: regime_index(&get("regime")),
                location: location_index(&get("location")),
                ran,
                half_bps,
            });
        },
    );
    each_selected(
        views,
        "SELECT * FROM rep_coverage_exclusions ORDER BY window_order",
        |get| {
            inputs.exclusions.push(CoverageExclusion {
                window: get("effort_window"),
                records: get("records").parse().unwrap(),
                not_ready: get("not_ready").parse().unwrap(),
                incomplete: get("incomplete").parse().unwrap(),
            });
        },
    );
    inputs
}

/// The episode of every coverage member within its pair × window, formed
/// before the regime split (ADR-058 D6, D8).
fn coverage_episodes(members: &[CoverageMember]) -> Vec<u64> {
    let mut episodes = vec![0; members.len()];
    for side in 0..P1_PAIRS.len() {
        for (w, (_, span)) in WINDOWS.iter().enumerate() {
            let indices: Vec<usize> = (0..members.len())
                .filter(|&i| members[i].side == side && members[i].window == w)
                .collect();
            assert!(
                indices
                    .windows(2)
                    .all(|p| members[p[0]].record < members[p[1]].record),
                "members in record order"
            );
            let ends: Vec<i64> = indices.iter().map(|&i| members[i].minute_end).collect();
            for (k, episode) in chain_episodes(&ends, *span, &vec![None; ends.len()])
                .into_iter()
                .enumerate()
            {
                episodes[indices[k]] = episode;
            }
        }
    }
    episodes
}

/// The index of `(horizon, regime label, stratum)` in a regime-restricted
/// per-stratum array.
fn strata_index(horizon: usize, regime: usize, stratum: usize) -> usize {
    (horizon * REGIME_SPLITS.len() + regime) * STRATA + stratum
}

/// Baseline races that ran per horizon, regime label and stratum.
fn ran_strata(baseline: &[CoverageBaseline]) -> Vec<u32> {
    let mut counts = vec![0u32; REP_HORIZONS.len() * REGIME_SPLITS.len() * STRATA];
    for record in baseline.iter().filter(|b| b.regime != NOT_READY) {
        let s = stratum(record.decision, LOCATIONS[record.location]);
        for h in 0..REP_HORIZONS.len() {
            if record.ran[h] {
                counts[strata_index(h, record.regime, s)] += 1;
            }
        }
    }
    counts
}

/// One context's coverage.
#[derive(Clone, Debug, PartialEq)]
struct ContextCoverage {
    /// Members whose race at H ran.
    ran: u64,
    /// Of those, the ones whose stratum holds no baseline race of the
    /// context's regime that ran (ADR-058 D4).
    dropped: u64,
    episodes: u64,
    units: u64,
    /// The units' representatives, as indices into the members.
    representatives: Vec<usize>,
    /// The median per-label fee of the representatives, in R.
    fee: Option<f64>,
}

/// The members of `context`, as indices in input order.
fn coverage_members(members: &[CoverageMember], context: &Context) -> Vec<usize> {
    (0..members.len())
        .filter(|&i| {
            let m = &members[i];
            m.side == context.side && m.window == context.window && m.regime == context.regime
        })
        .collect()
}

/// D5: every context's coverage, from outcome-free inputs.
fn coverage_of(inputs: &CoverageInputs) -> Vec<ContextCoverage> {
    let episodes = coverage_episodes(&inputs.members);
    let strata = ran_strata(&inputs.baseline);
    contexts()
        .iter()
        .map(|context| {
            let h = context.horizon;
            let indices = coverage_members(&inputs.members, context);
            let tuples: Vec<(i64, u64, bool)> = indices
                .iter()
                .map(|&i| {
                    (
                        inputs.members[i].decision,
                        episodes[i],
                        inputs.members[i].ran[h],
                    )
                })
                .collect();
            let (episodes, units) = unit_count(&tuples, context.horizon_ms());
            let reps: Vec<usize> = representatives(&tuples, context.horizon_ms())
                .into_iter()
                .map(|k| indices[k])
                .collect();
            assert_eq!(reps.len() as u64, units, "{}", context.label());
            let ran: Vec<&CoverageMember> = indices
                .iter()
                .map(|&i| &inputs.members[i])
                .filter(|m| m.ran[h])
                .collect();
            let dropped = ran
                .iter()
                .filter(|m| {
                    let s = stratum(m.decision, LOCATIONS[m.location]);
                    strata[strata_index(h, context.regime, s)] == 0
                })
                .count() as u64;
            let mut fees: Vec<f64> = reps
                .iter()
                .map(|&i| {
                    let half = inputs.members[i].half_bps[h].expect("a race that ran has barriers");
                    label_fee(FEE_BPS, half)
                })
                .collect();
            fees.sort_by(f64::total_cmp);
            ContextCoverage {
                ran: ran.len() as u64,
                dropped,
                episodes,
                units,
                representatives: reps,
                fee: (!fees.is_empty()).then(|| rank(&fees, 0.5)),
            }
        })
        .collect()
}

/// The coverage report's body: everything but the dataset identity line.
fn coverage_body(
    inputs: &CoverageInputs,
    results: &[ContextCoverage],
    counts: ((u64, u64, u64), u64, [u64; 2]),
) -> String {
    let mut out = String::new();
    let n_min = n_min(CONFIDENCE, MU);
    let weeks = weeks_of(inputs.window);
    let days = (inputs.window.1 - inputs.window.0) / DAY_MS;
    let ((all, baseline, trigger), effort, ran) = counts;
    let _ = writeln!(
        out,
        "queries docs/research/028-order-flow-interactions.sql sha256 {}\n\
         queries docs/research/205-p1-replication.sql sha256 {}\n\
         ADR-064 frozen text sha256 {}\n\
         replication.rs frozen decisions sha256 {}\n",
        sha256(STUDY_SQL),
        sha256(REPLICATION_SQL),
        sha256(frozen(ADR_064)),
        sha256(frozen_code(THIS_FILE))
    );
    let _ = writeln!(
        out,
        "pins: contexts {all}, baseline {baseline}, trigger {trigger}, effort rows {effort}; \
         races that ran 4h {}, 1d {}\n",
        ran[0], ran[1]
    );
    let _ = writeln!(out, "## Inputs\n");
    let _ = writeln!(
        out,
        "c = {CONFIDENCE}, μ = {MU} R, z = {:.6}, n_min = {n_min}; fee {FEE_BPS} bps round trip \
         per label; window {} … {} ms, {days} days, {weeks:.3} weeks. No outcome is read.\n",
        z_of(CONFIDENCE),
        inputs.window.0,
        inputs.window.1
    );
    let _ = writeln!(out, "## Horizons\n");
    let _ = writeln!(
        out,
        "| H | baseline races that ran | baselines with barriers | median half-width (bps) | \
         fee at the median (R) | disjoint windows in the window |\n|---|--:|--:|--:|--:|--:|"
    );
    for (h, &index) in REP_HORIZONS.iter().enumerate() {
        let (name, ms) = HORIZONS[index];
        let ran = inputs.baseline.iter().filter(|b| b.ran[h]).count();
        let mut widths: Vec<f64> = inputs
            .baseline
            .iter()
            .filter_map(|b| b.half_bps[h])
            .collect();
        widths.sort_by(f64::total_cmp);
        let median = (!widths.is_empty()).then(|| rank(&widths, 0.5));
        let _ = writeln!(
            out,
            "| {name} | {ran} | {} | {} | {} | {} |",
            widths.len(),
            median.map_or_else(|| "–".to_owned(), |m| format!("{m:.2}")),
            median.map_or_else(|| "–".to_owned(), |m| format!("{:.4}", FEE_BPS / m)),
            days * DAY_MS / ms
        );
    }
    let _ = writeln!(out, "\n## Exclusions before any race is read\n");
    let _ = writeln!(
        out,
        "| universe | window | records | effort not ready | incomplete coverage |\n|---|---|--:|--:|--:|"
    );
    for e in &inputs.exclusions {
        let _ = writeln!(
            out,
            "| baseline | {} | {} | {} | {} |",
            e.window, e.records, e.not_ready, e.incomplete
        );
    }
    let _ = writeln!(
        out,
        "\n| pair | window | P1 rows | regime not ready (in no context) |\n|---|---|--:|--:|"
    );
    for (side, pair) in P1_PAIRS.iter().enumerate() {
        for (w, (window, _)) in WINDOWS.iter().enumerate() {
            let rows = inputs
                .members
                .iter()
                .filter(|m| m.side == side && m.window == w);
            let (total, not_ready) = rows.fold((0, 0), |(t, n), m| {
                (t + 1, n + u64::from(m.regime == NOT_READY))
            });
            let _ = writeln!(out, "| {pair} | {window} | {total} | {not_ready} |");
        }
    }
    let _ = writeln!(out, "\n## Contexts\n");
    let _ = writeln!(
        out,
        "| pair | w | regime | H | members that ran | dropped | episodes | units | units/week | \
         measurable | median fee (R) |\n|---|---|---|---|--:|--:|--:|--:|--:|---|--:|"
    );
    for (context, result) in contexts().iter().zip(results) {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {} |",
            P1_PAIRS[context.side],
            WINDOWS[context.window].0,
            REGIME_SPLITS[context.regime],
            context.horizon_name(),
            result.ran,
            result.dropped,
            result.episodes,
            result.units,
            units_per_week(result.units, weeks),
            if result.units >= n_min { "yes" } else { "no" },
            result
                .fee
                .map_or_else(|| "–".to_owned(), |f| format!("{f:.4}"))
        );
    }
    let _ = writeln!(out, "\n## Summary\n");
    for (h, &index) in REP_HORIZONS.iter().enumerate() {
        let measurable = contexts()
            .iter()
            .zip(results)
            .filter(|(c, r)| c.horizon == h && r.units >= n_min)
            .count();
        let _ = writeln!(
            out,
            "- {}: {measurable} of 24 contexts measurable (units ≥ {n_min}).",
            HORIZONS[index].0
        );
    }
    let measurable = results.iter().filter(|r| r.units >= n_min).count();
    let _ = writeln!(
        out,
        "- All: {measurable} of 48; the rest are `not measurable` and are not read (ADR-064 S2)."
    );
    out
}

/// The dataset identity line of a report.
fn identity(manifest: &OutcomeManifest) -> String {
    format!(
        "dataset content {}, data {}, engine {}, {} {}, {} {}, code {}\nsource {}; replay {:?}; records {:?}\n\
         test binary code {}\n",
        manifest.content,
        manifest.data,
        manifest.engine,
        manifest.sampling,
        manifest.sampling_fingerprint,
        manifest.labeling,
        manifest.labeling_fingerprint,
        manifest.code,
        manifest.source,
        manifest.replay,
        manifest.records,
        conditional::CODE_VERSION
    )
}

/// The coverage run: guard, coverage, twin; the identity line and the
/// body, or why it stopped. Prints nothing.
fn coverage_run(dir: &Path, threads: u32) -> Result<(String, String), String> {
    let (manifest, views) = open_guarded(dir)?;
    views.set_threads(threads).map_err(|e| e.to_string())?;
    create_replication(&views)?;
    let inputs = load_coverage(&views, manifest.records);
    let results = coverage_of(&inputs);
    let mismatches = coverage_mismatches(&inputs, &results, &coverage_twin(&views), true);
    if mismatches > 0 {
        return Err(format!(
            "the coverage twin disagrees ({mismatches} mismatches); nothing is printed"
        ));
    }
    let counts = dataset_counts(&views);
    let mut body = coverage_body(&inputs, &results, counts);
    body.push_str("\ncoverage twin mismatches 0\n");
    Ok((identity(&manifest), body))
}

// ---------------------------------------------------------------------------
// The coverage twin (D5): from `contexts`, `effort` and its own projection
// of whether each race ran, written from the ADR text, without the study
// SQL or the tool's episode code.

/// What the coverage twin finds: the P1 records per pair × window, and per
/// context the members that ran, the episodes and the units.
#[derive(Clone, Debug, Default, PartialEq)]
struct CoverageTwin {
    members: Vec<Vec<u64>>,
    cells: Vec<(u64, u64, u64)>,
}

fn coverage_twin(views: &OutcomeViews) -> CoverageTwin {
    struct TwinRec {
        baseline: bool,
        decision: i64,
        minute_end: Option<i64>,
        regime: usize,
        /// Per window: the aggressor's side index when the P1 condition
        /// holds on a covered window.
        p1: [Option<usize>; 3],
        ran: [bool; 2],
    }
    let mut records: Vec<TwinRec> = Vec::new();
    each_selected(
        views,
        "SELECT record, sample, decision_time, minute_end, regime FROM contexts ORDER BY record",
        |get| {
            assert_eq!(get("record").parse::<usize>().unwrap(), records.len());
            records.push(TwinRec {
                baseline: get("sample") == "baseline",
                decision: get("decision_time").parse().unwrap(),
                minute_end: opt(get("minute_end")).map(|ms| ms.parse().unwrap()),
                regime: opt(get("regime")).map_or(NOT_READY, |r| regime_index(&r)),
                p1: [None; 3],
                ran: [false; 2],
            });
        },
    );
    each_selected(
        views,
        "SELECT record, \"window\", state, aggressor, effort, effort_high, progress, move_median, \
         partial_start, feed_gap FROM effort ORDER BY record",
        |get| {
            let record: usize = get("record").parse().unwrap();
            let covered = get("state") == "ready"
                && get("partial_start") == "false"
                && get("feed_gap") == "false";
            let p1 = covered
                && units_of(&get("effort")) >= units_of(&get("effort_high"))
                && units_of(&get("progress")) <= units_of(&get("move_median"));
            if p1 {
                let side = match get("aggressor").as_str() {
                    "buy" => 0,
                    "sell" => 1,
                    other => panic!("aggressor {other}"),
                };
                records[record].p1[window_index(&get("window"))] = Some(side);
            }
        },
    );
    each_selected(
        views,
        "SELECT record, horizon, race IN ('upper', 'lower', 'neither') AS ran FROM outcomes \
         WHERE horizon IN ('4h', '1d') ORDER BY record, horizon_ms",
        |get| {
            let record: usize = get("record").parse().unwrap();
            let h = usize::from(get("horizon") == "1d");
            records[record].ran[h] = flag(&get("ran"));
        },
    );
    let mut twin = CoverageTwin::default();
    let mut by_population: Vec<Vec<(usize, u64)>> = Vec::new();
    for side in 0..P1_PAIRS.len() {
        for (w, (_, span)) in WINDOWS.iter().enumerate() {
            let mine: Vec<usize> = (0..records.len())
                .filter(|&i| records[i].baseline && records[i].p1[w] == Some(side))
                .collect();
            // Episodes: the hull of the members' spans [end − w, end − 1 ms]
            // (ADR-054 D4's join rule).
            let mut hull: Option<(i64, i64)> = None;
            let mut count = 0u64;
            let mut episodes = Vec::with_capacity(mine.len());
            for &i in &mine {
                let end = records[i].minute_end.unwrap();
                let (first, last) = (end - span, end - 1);
                match &mut hull {
                    Some((open_first, open_last)) if first <= *open_last && *open_first <= last => {
                        *open_first = (*open_first).min(first);
                        *open_last = (*open_last).max(last);
                    }
                    _ => {
                        count += 1;
                        hull = Some((first, last));
                    }
                }
                episodes.push((i, count));
            }
            twin.members.push(mine.iter().map(|&i| i as u64).collect());
            by_population.push(episodes);
        }
    }
    for context in contexts() {
        let episodes = &by_population[context.population()];
        let h = context.horizon;
        let mut first: BTreeMap<u64, i64> = BTreeMap::new();
        let mut ran = 0;
        for &(i, episode) in episodes {
            let r = &records[i];
            if r.regime == context.regime && r.ran[h] {
                ran += 1;
                let entry = first.entry(episode).or_insert(r.decision);
                *entry = (*entry).min(r.decision);
            }
        }
        let mut reps: Vec<i64> = first.values().copied().collect();
        reps.sort_unstable();
        let mut units = 0;
        let mut next = i64::MIN;
        for d in &reps {
            if *d >= next {
                units += 1;
                next = d + context.horizon_ms();
            }
        }
        twin.cells.push((ran, reps.len() as u64, units));
    }
    twin
}

/// Mismatches between the coverage tool and its twin; prints the first few
/// unless `quiet`.
fn coverage_mismatches(
    inputs: &CoverageInputs,
    results: &[ContextCoverage],
    twin: &CoverageTwin,
    quiet: bool,
) -> u64 {
    let mut count = 0u64;
    let mut report = |what: String| {
        count += 1;
        if !quiet && count <= 5 {
            println!("coverage mismatch: {what}");
        }
    };
    for (side, pair) in P1_PAIRS.iter().enumerate() {
        for (w, (window, _)) in WINDOWS.iter().enumerate() {
            let tool: Vec<u64> = inputs
                .members
                .iter()
                .filter(|m| m.side == side && m.window == w)
                .map(|m| m.record)
                .collect();
            let twin = &twin.members[side * WINDOWS.len() + w];
            if &tool != twin {
                report(format!(
                    "{pair} w={window}: {} rows, twin {}",
                    tool.len(),
                    twin.len()
                ));
            }
        }
    }
    for ((context, result), twin) in contexts().iter().zip(results).zip(&twin.cells) {
        let tool = (result.ran, result.episodes, result.units);
        if tool != *twin {
            report(format!("{}: {tool:?}, twin {twin:?}", context.label()));
        }
    }
    count
}

// ---------------------------------------------------------------------------
// The measurement (D3, D4; #206).

/// What the measurement reads: the ADR-058 inputs restricted to the P1
/// rows and the 4h / 1d races, with each member's half-widths and each
/// baseline record's minute end.
struct Measured {
    inputs: Inputs,
    half_bps: Vec<[Option<f64>; 2]>,
    /// Per member: the mark to market at 4h and 1d (`rep_marks`).
    marks: Vec<[Option<f64>; 2]>,
    minute_ends: Vec<i64>,
    window: (i64, i64),
}

const UNREAD: Outcome = Outcome::Unavailable;

/// Reads `rep_members` and `rep_baseline`. Measurement only.
fn load_measured(views: &OutcomeViews, window: (i64, i64)) -> Measured {
    let mut measured = Measured {
        inputs: Inputs::default(),
        half_bps: Vec::new(),
        marks: Vec::new(),
        minute_ends: Vec::new(),
        window,
    };
    each_selected(
        views,
        "SELECT * FROM rep_members ORDER BY pair, \
         list_position(['5m', '15m', '1h'], effort_window), record",
        |get| {
            let (_, half_bps) = ran_widths(get);
            measured.inputs.members.push(Member {
                pair: get("pair"),
                window: Some(get("effort_window")),
                trigger: None,
                record: get("record").parse().unwrap(),
                decision: get("decision_ms").parse().unwrap(),
                minute_end: Some(get("minute_end_ms").parse().unwrap()),
                side: get("side"),
                upper: match get("hypothesis").as_str() {
                    "upper" => true,
                    "lower" => false,
                    other => panic!("hypothesis {other}"),
                },
                regime: get("regime"),
                location: get("location"),
                episode: None,
                cvd: None,
                outcomes: [
                    UNREAD,
                    UNREAD,
                    UNREAD,
                    Outcome::parse(&get("out_4h")),
                    Outcome::parse(&get("out_1d")),
                ],
            });
            measured.half_bps.push(half_bps);
            let mark = |name: &str| opt(get(name)).map(|v| v.parse::<f64>().unwrap());
            measured.marks.push([mark("mark_4h"), mark("mark_1d")]);
        },
    );
    each_selected(views, "SELECT * FROM rep_baseline ORDER BY record", |get| {
        measured.inputs.baseline.push(Baseline {
            record: get("record").parse().unwrap(),
            decision: get("decision_ms").parse().unwrap(),
            regime: get("regime"),
            location: get("location"),
            auction_state: None,
            auction_side: None,
            races: [
                StoredRace::Unavailable,
                StoredRace::Unavailable,
                StoredRace::Unavailable,
                StoredRace::parse(&get("race_4h")),
                StoredRace::parse(&get("race_1d")),
            ],
        });
        measured
            .minute_ends
            .push(get("minute_end_ms").parse().unwrap());
    });
    measured
}

/// The 48 contexts as ADR-058 cells: the population, the regime split
/// (its baseline restricted to the regime, ADR-058 D4) and the horizon.
fn replication_cells(inputs: &Inputs) -> Vec<Cell> {
    let populations = populations();
    contexts()
        .iter()
        .map(|context| {
            let population = &populations[context.population()];
            assert_eq!(
                population.label(),
                format!("{} w={}", P1_PAIRS[context.side], WINDOWS[context.window].0)
            );
            let split = Split::Regime(REGIME_SPLITS[context.regime]);
            Cell {
                population: context.population(),
                split,
                horizon: REP_HORIZONS[context.horizon],
                members: (0..inputs.members.len())
                    .filter(|&i| {
                        population.holds(&inputs.members[i]) && split.holds(&inputs.members[i])
                    })
                    .collect(),
            }
        })
        .collect()
}

/// The null frame of the measured baseline.
fn null_frame(measured: &Measured) -> NullFrame {
    let baseline = &measured.inputs.baseline;
    assert!(
        measured.minute_ends.windows(2).all(|p| p[0] < p[1]),
        "one baseline record per minute, in minute order"
    );
    let race = |h: usize| -> Vec<i8> {
        baseline
            .iter()
            .map(|b| match b.races[REP_HORIZONS[h]] {
                StoredRace::Upper => 1,
                StoredRace::Lower => -1,
                StoredRace::Neither => 0,
                StoredRace::Censored | StoredRace::Unavailable => NOT_RAN,
            })
            .collect()
    };
    NullFrame {
        minute_end: measured.minute_ends.clone(),
        regime: baseline
            .iter()
            .map(|b| regime_index(&b.regime) as u8)
            .collect(),
        stratum: baseline
            .iter()
            .map(|b| stratum(b.decision, &b.location) as u16)
            .collect(),
        race: [race(0), race(1)],
    }
}

/// The measurement report (D4; S2: a context that is not measurable shows
/// only its counts).
fn measurement_body(measured: &Measured, results: &[ContextResult], run: &RunSettings) -> String {
    let mut out = String::new();
    let n_min = n_min(CONFIDENCE, run.mu);
    let weeks = weeks_of(measured.window);
    let _ = writeln!(out, "## Inputs\n");
    let _ = writeln!(
        out,
        "c = {CONFIDENCE}, μ = {} R, z = {:.6}, n_min = {n_min}; fee {FEE_BPS} bps round trip \
         per label; q = {FDR} over 48 contexts; T3 {} draws, blocks {} and {} weeks, seed \
         {REPLICATION_SEED}; T2 {} resamples, blocks {} / {} / {} days, seed {REPLICATION_SEED}; \
         ADR-058 D7 {} resamples, block {} days, seed {SEED}; {weeks:.3} weeks.\n",
        run.mu,
        z_of(CONFIDENCE),
        run.draws,
        NULL_WEEKS[0],
        NULL_WEEKS[1],
        run.t2_resamples,
        T2_BLOCKS[0],
        T2_BLOCKS[1],
        T2_BLOCKS[2],
        run.d7_resamples,
        BLOCKS[0]
    );
    let _ = writeln!(out, "## Contexts\n");
    let _ = writeln!(
        out,
        "| pair | w | regime | H | members that ran | dropped | episodes | units | T | T_b | E | s | \
         p (8w) | BH | p (4w) | net expectancy (R) | T2 interval (56d) | units/week | net R/year | \
         max drawdown (R) | E reading (ADR-058 D7, 56d) | reading |\n\
         |---|---|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|--:|--:|---|--:|--:|--:|---|---|"
    );
    let dash = || "–".to_owned();
    for (context, r) in contexts().iter().zip(results) {
        let counts = format!(
            "| {} | {} | {} | {} | {} | {} | {} | {} |",
            P1_PAIRS[context.side],
            WINDOWS[context.window].0,
            REGIME_SPLITS[context.regime],
            context.horizon_name(),
            r.ran,
            r.dropped,
            r.episodes,
            r.units
        );
        if r.reading == RepReading::NotMeasurable {
            let _ = writeln!(out, "{counts}{} not measurable |", " – |".repeat(13));
            continue;
        }
        let p = |p: Option<f64>| p.map_or_else(dash, |p| format!("{p:.6}"));
        let mut flags = Vec::new();
        if r.rejected[0] != r.rejected[1] {
            flags.push("null-sensitive".to_owned());
        }
        let passes = |i: &Interval| i.reading == Reading::Hypothesis;
        if let Some(primary) = r.t2.first() {
            let other: Vec<String> = r.t2[1..]
                .iter()
                .zip(&T2_BLOCKS[1..])
                .filter(|(i, _)| passes(i) != passes(primary))
                .map(|(_, b)| format!("{b}d"))
                .collect();
            if !other.is_empty() {
                flags.push(format!("block-sensitive ({})", other.join(", ")));
            }
            if primary.undefined > 0 {
                flags.push(format!("undefined {}", primary.undefined));
            }
        }
        let upw = units_per_week(r.units, weeks);
        let _ = writeln!(
            out,
            "{counts} {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2} | {} | {} | {} | {} |",
            fixed(r.t),
            fixed(r.t_b),
            fixed(r.effect),
            r.s.map_or_else(dash, |s| format!("{s:+}")),
            p(r.p[0]),
            if r.rejected[0] { "reject" } else { "–" },
            p(r.p[1]),
            fixed(r.expectancy),
            r.t2.first().map_or_else(dash, |i| if i.low.is_nan() {
                dash()
            } else {
                format!("[{:.4}, {:.4}]", i.low, i.high)
            }),
            upw,
            fixed(r.expectancy.map(|e| net_r_per_year(e, upw))),
            fixed(r.max_drawdown),
            r.d7.map_or_else(dash, |i| i.reading.name().to_owned()),
            if flags.is_empty() {
                r.reading.name().to_owned()
            } else {
                format!("{} ({})", r.reading.name(), flags.join(", "))
            }
        );
    }
    let replicated = results
        .iter()
        .filter(|r| matches!(r.reading, RepReading::Replicated(_)))
        .count();
    let measurable = results
        .iter()
        .filter(|r| r.reading != RepReading::NotMeasurable)
        .count();
    let _ = writeln!(
        out,
        "\n## Summary\n\n- measurable: {measurable} of 48; replicated: {replicated} of 48 \
         (BH at q = {FDR}, m = 48, unread contexts at p = 1)."
    );
    out
}

/// The record set of `rep_coverage_members` against the P1 rows of
/// `study_members`: rows in one and not the other. Measurement only.
fn record_set_difference(views: &OutcomeViews) -> u64 {
    let count = |sql: &str| -> u64 {
        tsv_lines(views, &format!("SELECT count(*) AS n FROM ({sql})"))[1]
            .parse()
            .unwrap()
    };
    let coverage = "SELECT pair, effort_window, record FROM rep_coverage_members";
    let study = "SELECT pair, effort_window, record FROM study_members \
                 WHERE pair IN ('P1-buy', 'P1-sell')";
    count(&format!("{coverage} EXCEPT ALL {study}"))
        + count(&format!("{study} EXCEPT ALL {coverage}"))
}

/// The ADR-058 twin on the P1 rows: `study_baseline` and the P1 rows of
/// `study_members` line by line, and the 48 cells' units. Measurement
/// only.
fn p1_twin_mismatches(views: &OutcomeViews, inputs: &Inputs) -> u64 {
    let records = twin_records(views, &[]);
    let members: Vec<TMember> = twin_members(&records)
        .into_iter()
        .filter(|m| P1_PAIRS.contains(&m.pair))
        .collect();
    let twin = twin_lines(&records, &members);
    let study_baseline = tsv_lines(views, "SELECT * FROM study_baseline ORDER BY record");
    let study_members = tsv_lines(
        views,
        "SELECT * FROM study_members WHERE pair IN ('P1-buy', 'P1-sell') ORDER BY pair, \
         list_position(['5m', '15m', '1h'], effort_window), trigger NULLS FIRST, record",
    );
    let mut count = 0u64;
    for (got, want) in [
        (&study_baseline, &twin.baseline),
        (&study_members, &twin.members),
    ] {
        for index in 0..got.len().max(want.len()) {
            if got.get(index) != want.get(index) {
                count += 1;
            }
        }
    }
    let units = twin_units(&records, &members);
    let populations = populations();
    let episodes = member_episodes(inputs, &populations);
    for (context, cell) in contexts().iter().zip(replication_cells(inputs)) {
        let key = (
            populations[cell.population].label(),
            "regime".to_owned(),
            REGIME_SPLITS[context.regime].to_owned(),
            context.horizon_name(),
        );
        if units[&key] != cell_units(inputs, &episodes, &cell) {
            count += 1;
        }
    }
    count
}

// ---------------------------------------------------------------------------
// The entries (run by the main session; see the module docs).

#[test]
#[ignore = "the #205 coverage run on the replication dataset; reads no outcome"]
fn coverage_p1_replication() {
    let dir = std::path::PathBuf::from(env("MIE_OUTCOME_DATASET"))
        .canonicalize()
        .expect("MIE_OUTCOME_DATASET exists");
    let (identity, body) = coverage_run(&dir, 4).unwrap_or_else(|error| panic!("refused: {error}"));
    println!("# P1 replication coverage (#205, ADR-064)\n");
    print!("{identity}\n{body}");
}

#[test]
#[ignore = "the #206 measurement on the pinned replication dataset; reads outcomes"]
fn measure_p1_replication() {
    let dir = std::path::PathBuf::from(env("MIE_OUTCOME_DATASET"));
    let text = measure_replication(&dir).unwrap_or_else(|error| panic!("refused: {error}"));
    print!("{text}");
}

// ---------------------------------------------------------------------------
// Fixtures.

/// 2020-01-01 00:00 UTC, a Wednesday: the window's first minute.
const W0: i64 = REPLICATION_RECORDS.0;

/// The replication dataset header, which passes the guard.
fn rep_header() -> OutcomeHeader {
    let window = ReplayWindow {
        start: EventTime::from_millis(REPLICATION_RECORDS.0),
        end: EventTime::from_millis(REPLICATION_RECORDS.1),
    };
    OutcomeHeader {
        source: REPLICATION_SOURCE.to_owned(),
        replay: window,
        records: window,
        provenance: LabelProvenance::new(
            ContextProvenance::new(
                DataVersion::from_hex(&"5d".repeat(32)).unwrap(),
                &MarketStateEngine::new(),
            ),
            Labeling::CURRENT,
        ),
        code: format!("git:{}", "0".repeat(40)),
    }
}

/// The barrier offset of a fixture record at horizon `h`: its own width,
/// so the per-label fee varies.
fn rep_offset(rec: &Rec, h: usize) -> i64 {
    (40 + (rec.seq % 61) as i64) * USDT * (h as i64 + 1)
}

/// The last trade of a fixture record's window at horizon `h`, in price
/// units from the reference: −½, −¼, 0, ¼ or ½ of its half-width, by
/// record.
fn rep_drift(rec: &Rec, h: usize) -> i64 {
    rep_offset(rec, h) * ((rec.seq % 5) as i64 - 2) / 4
}

/// `super::label` with per-record barriers and last trades
/// ([`rep_drift`]); with `scramble`, the race times, excursions and last
/// trades are random (they are outcomes the coverage must not read).
fn rep_label(rec: &Rec, clear: [bool; 5], scramble: Option<&mut Lcg>) -> ForwardLabel {
    let mut label = label(rec, clear);
    let mut scramble = scramble;
    for (h, horizon) in label.horizons.iter_mut().enumerate() {
        if let Some(barriers) = &mut horizon.barriers {
            let offset = rep_offset(rec, h);
            *barriers = Barriers {
                lower: Price::from_units(R - offset),
                upper: Price::from_units(R + offset),
            };
            horizon.last = Some(Price::from_units(R + rep_drift(rec, h)));
        }
        if let Some(lcg) = scramble.as_deref_mut() {
            let at = EventTime::from_millis(rec.decision + 1 + lcg.below(HORIZONS[h].1 as u64));
            horizon.race = match horizon.race {
                Race::Upper { .. } => Race::Upper {
                    at,
                    same_ms: lcg.below(2) == 0,
                },
                Race::Lower { .. } => Race::Lower {
                    at,
                    same_ms: lcg.below(2) == 0,
                },
                other => other,
            };
            let excursion = |lcg: &mut Lcg| Excursion {
                size: Price::from_units(1 + lcg.below(500) * USDT),
                at: Some(EventTime::from_millis(
                    rec.decision + 1 + lcg.below(HORIZONS[h].1 as u64),
                )),
            };
            horizon.up = excursion(lcg);
            horizon.down = excursion(lcg);
            if horizon.barriers.is_some() {
                let offset = rep_offset(rec, h);
                horizon.last = Some(Price::from_units(
                    R - offset + 1 + lcg.below(2 * offset as u64 - 1),
                ));
            }
        }
    }
    label
}

/// Writes `recs` (sorted here) as a new dataset `dir` under `header`.
fn write_rep_dataset(dir: &Path, recs: &mut [Rec], header: OutcomeHeader, scramble: Option<u64>) {
    recs.sort_by_key(|rec| (rec.decision, rec.seq));
    let flags = clear_flags(recs);
    let mut lcg = scramble.map(Lcg);
    let mut writer = OutcomeWriter::create(dir, header).unwrap();
    for (rec, clear) in recs.iter().zip(flags) {
        writer.record(&context_record(rec)).unwrap();
        writer.label(&rep_label(rec, clear, lcg.as_mut())).unwrap();
    }
    writer.finish(recs.len() as u64, 0).unwrap();
}

/// `days` days of baseline records every `step` minutes from `W0`: a
/// persistent regime, random location bins, and per effort window P1
/// conditions held for runs of up to 40 steps, with incomplete coverage now
/// and then; races at random, a few censored or unavailable.
fn rep_recs(days: i64, step: i64, seed: u64) -> Vec<Rec> {
    use Aggressor::{Buy, Sell};
    let mut lcg = Lcg(seed);
    let levels: [&'static [LevelKind]; 4] = [
        &[],
        &[LevelKind::Vah],
        &[LevelKind::StructuralLow],
        &[LevelKind::Val, LevelKind::StructuralHigh],
    ];
    let mut regime = 40.0;
    let mut held: [Option<(Aggressor, i64)>; 3] = [None; 3];
    let mut recs = Vec::new();
    for slot in 0..days * 1_440 / step {
        let minute = slot * step;
        let decision = W0 + minute * MINUTE_MS + 1 + lcg.below(59_000);
        let mut rec = Rec::baseline(decision, slot as u64 + 1);
        if lcg.below(150) == 0 {
            regime = [10.0, 40.0, 60.0, 90.0][lcg.below(4) as usize];
        }
        rec.regime = (lcg.below(40) != 0).then_some(regime);
        rec.levels = (lcg.below(12) != 0).then(|| levels[lcg.below(4) as usize]);
        for (w, hold) in held.iter_mut().enumerate() {
            *hold = match *hold {
                Some((a, until)) if minute < until => Some((a, until)),
                _ if lcg.below(25) == 0 => Some((
                    if lcg.below(2) == 0 { Buy } else { Sell },
                    minute + step * (1 + lcg.below(40)),
                )),
                _ => None,
            };
            rec.effort[w] = match *hold {
                Some((aggressor, _)) if lcg.below(30) == 0 => FlowSpec::Ready {
                    aggressor,
                    effort: EFFORT_HIGH,
                    progress: 0,
                    partial_start: true,
                    feed_gap: false,
                },
                Some((aggressor, _)) => ready_flow(
                    aggressor,
                    EFFORT_HIGH + lcg.below(300),
                    lcg.below(MOVE_MEDIAN as u64 + 1),
                ),
                None => match lcg.below(12) {
                    0 => FlowSpec::Warming,
                    1 => FlowSpec::Unavailable,
                    _ => ready_flow(
                        if lcg.below(2) == 0 { Buy } else { Sell },
                        lcg.below(EFFORT_HIGH as u64),
                        lcg.below(MOVE_HIGH as u64),
                    ),
                },
            };
        }
        rec.races = std::array::from_fn(|_| match lcg.below(30) {
            0 => R5::Censored,
            1 => R5::Unavailable,
            k if k < 12 => R5::Up,
            k if k < 24 => R5::Down,
            _ => R5::Neither,
        });
        recs.push(rec);
    }
    recs
}

/// A replication fixture dataset in a temp directory.
struct RepBuilt {
    _temp: TempDir,
    dir: std::path::PathBuf,
}

fn rep_built(recs: &mut [Rec], header: OutcomeHeader, scramble: Option<u64>) -> RepBuilt {
    let temp = TempDir::new("p1-replication");
    let dir = temp.path().join("dataset");
    write_rep_dataset(&dir, recs, header, scramble);
    RepBuilt { _temp: temp, dir }
}

/// The views of a fixture with the study and replication views.
fn rep_views(dir: &Path) -> OutcomeViews {
    let views = OutcomeViews::open(dir).unwrap();
    create_replication(&views).unwrap();
    views
}

/// Small settings for the fixture measurements.
const FIXTURE_RUN: RunSettings = RunSettings {
    mu: MU,
    t2_resamples: 200,
    d7_resamples: 200,
    draws: 999,
    threads: 2,
};

// ---------------------------------------------------------------------------
// Tests: freeze, guard, the ADR-058 regression.

#[test]
fn the_frozen_texts_equal_their_pins() {
    assert_eq!(sha256(STUDY_SQL), STUDY_SQL_SHA256, "028 queries");
    assert_eq!(
        sha256(REPLICATION_SQL),
        REPLICATION_SQL_SHA256,
        "205 queries"
    );
    assert_eq!(
        sha256(frozen(ADR_064)),
        ADR_FROZEN_SHA256,
        "ADR-064 frozen text"
    );
    assert_eq!(
        sha256(frozen_code(THIS_FILE)),
        CODE_FROZEN_SHA256,
        "replication.rs frozen decisions"
    );
    let text = frozen(ADR_064);
    assert!(text.starts_with("## Context\n"));
    assert!(text.contains("\n## Accept when\n"));
    assert!(!text.contains("## Pre-registration record"));
}

#[test]
fn the_code_freeze_holds_every_decision_and_catches_a_change() {
    let region = frozen_code(THIS_FILE);
    // The D3 numbers are the ADR's.
    assert_eq!((CONFIDENCE, MU, FEE_BPS, FDR), (0.95, 0.10, 10.0, 0.05));
    assert_eq!((T2_RESAMPLES, T2_BLOCKS), (10_000, [56, 28, 112]));
    assert_eq!(
        (T3_DRAWS, NULL_WEEKS, REPLICATION_SEED),
        (1_000_000, [8, 4], 205)
    );
    assert_eq!(
        (RUN.mu, RUN.draws, RUN.t2_resamples, RUN.d7_resamples),
        (MU, T3_DRAWS, T2_RESAMPLES, RESAMPLES)
    );
    // Every decision is inside the region, so any change to one moves its
    // hash: the review's mutations (q 0.20, 10³ draws, 2-week blocks, a
    // 7-day T2 block) and the reading, BH, the null and the fee.
    for (definition, mutated) in [
        ("const FDR: f64 = 0.05;", "const FDR: f64 = 0.20;"),
        (
            "const T3_DRAWS: usize = 1_000_000;",
            "const T3_DRAWS: usize = 1_000;",
        ),
        (
            "const NULL_WEEKS: [i64; 2] = [8, 4];",
            "const NULL_WEEKS: [i64; 2] = [2, 4];",
        ),
        (
            "const T2_BLOCKS: [usize; 3] = [56, 28, 112];",
            "const T2_BLOCKS: [usize; 3] = [7, 28, 112];",
        ),
        ("const FEE_BPS: f64 = 10.0;", "const FEE_BPS: f64 = 4.0;"),
        (
            "if p[i] <= (rank + 1) as f64 * q / M as f64 {",
            "if p[i] < (rank + 1) as f64 * q / M as f64 {",
        ),
        (
            "Reading::Hypothesis => RepReading::Replicated(s > 0.0),",
            "_ => RepReading::Replicated(s > 0.0),",
        ),
        (
            "Outcome::Neither => sigma * mark.unwrap_or(0.0),",
            "Outcome::Neither => 0.0,",
        ),
        ("(2.0 * plus.min(minus)).min(1.0)", "plus.min(minus)"),
        ("result.rejected[0],", "result.rejected[1],"),
        (
            "measure_run(dir, &REPLICATION_PINS, &RUN)",
            "measure_run(dir, &REPLICATION_PINS, &RunSettings { draws: 1_000, ..RUN })",
        ),
        ("if *run != RUN {", "if false {"),
    ] {
        assert_eq!(region.matches(definition).count(), 1, "{definition}");
        let changed = region.replacen(definition, mutated, 1);
        assert_ne!(sha256(&changed), CODE_FROZEN_SHA256, "{mutated}");
    }
    for item in [
        "fn guard(",
        "fn contexts(",
        "fn representatives(",
        "fn label_fee(",
        "fn net_outcome(",
        "fn t2_values(",
        "fn bh<",
        "fn rotation_table(",
        "fn null_p(",
        "fn read(",
        "fn measure_contexts(",
        "fn measure_replication(",
        "fn measure_run(",
    ] {
        assert!(region.contains(item), "{item} is frozen");
    }
}

#[test]
fn the_adr_058_report_is_unchanged() {
    // The SHA-256 of the ADR-058 fixture report of
    // `the_report_is_deterministic_across_runs_and_threads`, taken on main
    // before this module existed.
    let mut recs = random_recs(2, 5);
    recs.extend(edge_recs());
    let built = built(&mut recs, &[]);
    let settings = test_settings(0.3, 1);
    let (populations, results) = study(&built.inputs, &settings);
    let text = report(&built.inputs, &settings, &populations, &results);
    assert_eq!(
        sha256(&text),
        "cdb779136c619eaff8a5014155c8317b4d7e397b85d498151726d832e54223f5"
    );
}

/// The manifest of a guard-passing fixture.
fn passing_manifest() -> (RepBuilt, OutcomeManifest) {
    let mut recs = rep_recs(1, 30, 1);
    let built = rep_built(&mut recs, rep_header(), None);
    let manifest = OutcomeDataset::open(&built.dir).unwrap().manifest().clone();
    (built, manifest)
}

#[test]
fn the_guard_refuses_every_wrong_manifest() {
    let (_built, manifest) = passing_manifest();
    assert_eq!(guard(&manifest), Ok(()));
    type Mutation = fn(&mut OutcomeManifest);
    let cases: [(&str, Mutation); 6] = [
        ("records 1 ms past the window", |m| m.records.1 += 1),
        ("replay end after the calibration start", |m| {
            m.replay.1 = CALIBRATION_START_MS + 1;
        }),
        ("streams aggTrades,metrics", |m| {
            m.source = "archive binance-archive/BTCUSDT streams aggTrades,metrics correction fold"
                .to_owned();
        }),
        ("another engine", |m| {
            m.engine = "0123456789abcdef-1".to_owned()
        }),
        ("-dirty", |m| m.code.push_str("-dirty")),
        ("the calibration content", |m| {
            m.content = CALIBRATION_CONTENT.to_owned();
        }),
    ];
    for (name, mutate) in cases {
        let mut wrong = manifest.clone();
        mutate(&mut wrong);
        assert!(guard(&wrong).is_err(), "{name}");
    }
    // Records that end inside the window are refused too: the window is
    // exact.
    let mut short = manifest.clone();
    short.records.1 -= DAY_MS;
    assert!(guard(&short).is_err());
    let mut unknown = manifest;
    unknown.code = "unknown".to_owned();
    assert!(guard(&unknown).is_err());
}

#[test]
fn the_coverage_entry_refuses_a_wrong_dataset_before_any_output() {
    for header in [
        OutcomeHeader {
            source: "archive binance-archive/BTCUSDT streams aggTrades,metrics correction fold"
                .to_owned(),
            ..rep_header()
        },
        OutcomeHeader {
            code: format!("git:{}-dirty", "0".repeat(40)),
            ..rep_header()
        },
        OutcomeHeader {
            records: ReplayWindow {
                start: EventTime::from_millis(REPLICATION_RECORDS.0),
                end: EventTime::from_millis(REPLICATION_RECORDS.1 + 1),
            },
            replay: ReplayWindow {
                start: EventTime::from_millis(REPLICATION_RECORDS.0),
                end: EventTime::from_millis(REPLICATION_RECORDS.1 + 1),
            },
            ..rep_header()
        },
    ] {
        let mut recs = rep_recs(1, 30, 2);
        let built = rep_built(&mut recs, header, None);
        // The entry returns its text only once every check passed; a
        // refusal carries no table.
        let refusal = coverage_run(&built.dir, 1).unwrap_err();
        assert!(!refusal.contains('|'), "{refusal}");
    }
}

#[test]
fn the_pins_are_the_coverage_runs() {
    let coverage = include_str!("../../../../docs/research/205-p1-replication-coverage.md");
    let pins = REPLICATION_PINS;
    let (contexts, baseline, trigger) = pins.contexts.expect("pinned");
    let ran = pins.ran.expect("pinned");
    for line in [
        format!(
            "dataset content {}, data {}, engine {}",
            pins.content.expect("pinned"),
            pins.data.expect("pinned"),
            REPLICATION_PROVENANCE[0]
        ),
        format!(
            "pins: contexts {contexts}, baseline {baseline}, trigger {trigger}, effort rows {}; \
             races that ran 4h {}, 1d {}",
            3 * contexts,
            ran[0],
            ran[1]
        ),
        format!("028-order-flow-interactions.sql sha256 {STUDY_SQL_SHA256}"),
        format!("205-p1-replication.sql sha256 {REPLICATION_SQL_SHA256}"),
        format!("ADR-064 frozen text sha256 {ADR_FROZEN_SHA256}"),
        format!("replication.rs frozen decisions sha256 {CODE_FROZEN_SHA256}"),
        "coverage twin mismatches 0".to_owned(),
    ] {
        assert!(coverage.contains(&line), "{line}");
    }
    assert_eq!(contexts, baseline + trigger);
}

#[test]
fn the_replication_dataset_is_measured_only_with_the_pre_registered_settings() {
    // The review's mutation: the entry passes RUN with 10³ draws. On the
    // replication pins the frozen measure_run refuses it before it opens
    // the dataset (the path does not exist).
    for run in [
        RunSettings {
            draws: 1_000,
            ..RUN
        },
        RunSettings { mu: 0.2, ..RUN },
        RunSettings {
            t2_resamples: 100,
            ..RUN
        },
        FIXTURE_RUN,
    ] {
        let refusal =
            measure_run(Path::new("no-such-dataset"), &REPLICATION_PINS, &run).unwrap_err();
        assert!(refusal.contains("only with RUN"), "{refusal}");
    }
    // With RUN the check passes and the next step (a clean build, or the
    // hashes, or opening the dataset) is what refuses.
    let refusal = measure_run(Path::new("no-such-dataset"), &REPLICATION_PINS, &RUN).unwrap_err();
    assert!(!refusal.contains("only with RUN"), "{refusal}");
    // The entry goes through the frozen path.
    let entry = &THIS_FILE[THIS_FILE.find("fn measure_p1_replication()").unwrap()..];
    let entry = &entry[..entry.find("\n}\n").unwrap()];
    assert!(entry.contains("measure_replication(&dir)"), "{entry}");
    assert!(!entry.contains("measure_run") && !entry.contains("RunSettings"));
}

#[test]
#[should_panic(expected = "not pinned")]
fn the_unpinned_measurement_panics_before_it_opens_the_dataset() {
    // The path does not exist: opening it first would fail otherwise.
    let _ = measure_run(Path::new("no-such-dataset"), &UNPINNED, &FIXTURE_RUN);
}

// ---------------------------------------------------------------------------
// Tests: BH.

#[test]
fn bh_rejects_four_of_the_1995_example() {
    // Benjamini and Hochberg (1995), section 4: 15 p-values; at q = 0.05
    // the step-up rejects 4, Bonferroni (0.05/15) 3.
    let p = [
        0.0001, 0.0004, 0.0019, 0.0095, 0.0201, 0.0278, 0.0298, 0.0344, 0.0459, 0.3240, 0.4262,
        0.5719, 0.6528, 0.7590, 1.0000,
    ];
    let rejected = bh(&p, 0.05);
    assert_eq!(rejected.iter().filter(|r| **r).count(), 4);
    assert!(rejected[..4].iter().all(|r| *r));
    assert_eq!(p.iter().filter(|p| **p <= 0.05 / 15.0).count(), 3);
}

#[test]
fn bh_edges() {
    assert!(bh(&[1.0; 48], 0.05).iter().all(|r| !r));
    // p exactly k·q/m is rejected.
    for k in 1..=48usize {
        let mut p = [1.0; 48];
        for (i, value) in p.iter_mut().enumerate().take(k) {
            *value = (i + 1) as f64 * 0.05 / 48.0;
        }
        let rejected = bh(&p, 0.05);
        assert_eq!(rejected.iter().filter(|r| **r).count(), k, "k = {k}");
    }
    // Shuffled input, the same decisions.
    let mut lcg = Lcg(205);
    // 20 p-values at most 0.002, which BH rejects, and 28 from 0.1.
    let p: [f64; 48] = std::array::from_fn(|i| {
        if i % 12 < 5 {
            (1 + lcg.below(200)) as f64 / 100_000.0
        } else {
            0.1 + lcg.below(9_000) as f64 / 10_000.0
        }
    });
    let rejected = bh(&p, 0.05);
    assert_eq!(rejected.iter().filter(|r| **r).count(), 20);
    let mut order: Vec<usize> = (0..48).collect();
    for i in (1..48).rev() {
        order.swap(i, lcg.below(i as u64 + 1) as usize);
    }
    let shuffled: [f64; 48] = std::array::from_fn(|i| p[order[i]]);
    let again = bh(&shuffled, 0.05);
    for i in 0..48 {
        assert_eq!(again[i], rejected[order[i]]);
    }
    // m stays 48 with unread contexts at p = 1: 0.05/48 = 0.00104.
    let mut p = [1.0; 48];
    p[17] = 0.001;
    assert!(bh(&p, 0.05)[17]);
    p[17] = 0.0011;
    assert!(!bh(&p, 0.05)[17]);
    assert!(bh(&[0.0011], 0.05)[0], "m = 1 would reject it");
}

// ---------------------------------------------------------------------------
// Tests: the rotation null.

#[test]
fn the_replication_layout_has_the_pre_registered_blocks() {
    let eight = Layout::new(REPLICATION_RECORDS, 8);
    let mut lengths = vec![8; 30];
    lengths.push(7);
    assert_eq!(eight.lengths(), lengths);
    let four = Layout::new(REPLICATION_RECORDS, 4);
    let mut lengths = vec![4; 61];
    lengths.push(3);
    assert_eq!(four.lengths(), lengths);
    // The tail: 2024-09-25 … 09-30 never rotates.
    let tail = W0 + 247 * WEEK_MS;
    assert_eq!(tail, 1_727_222_400_000, "2024-09-25 00:00 UTC");
    assert_eq!((REPLICATION_RECORDS.1 - tail) / DAY_MS, 6);
    assert_eq!(eight.block(tail - 1).map(|b| b.0), Some(30));
    assert_eq!(eight.block(tail), None);
    for k in 0..8 {
        assert_eq!(eight.rotate(tail + 5 * HOUR_MS, k), tail + 5 * HOUR_MS);
    }
}

#[test]
fn every_pseudo_member_keeps_its_minute_of_the_utc_week() {
    let mut lcg = Lcg(7);
    for weeks in NULL_WEEKS {
        let layout = Layout::new(REPLICATION_RECORDS, weeks);
        for _ in 0..2_000 {
            let t = W0 + lcg.below((REPLICATION_RECORDS.1 - W0) as u64) / MINUTE_MS * MINUTE_MS;
            let block = layout.block(t);
            for k in 0..weeks {
                let moved = layout.rotate(t, k);
                assert_eq!((moved - t).rem_euclid(WEEK_MS), 0);
                assert_eq!(layout.block(moved).map(|b| b.0), block.map(|b| b.0));
                if k == 0 {
                    assert_eq!(moved, t);
                }
            }
            if let Some((_, _, length)) = block {
                let all: BTreeSet<i64> = (0..length).map(|k| layout.rotate(t, k)).collect();
                assert_eq!(all.len() as i64, length, "a rotation is a bijection");
            }
        }
    }
}

/// A frame of one regime and stratum with a race per slot.
fn simple_frame(minute_ends: Vec<i64>, races: Vec<i8>) -> NullFrame {
    let n = minute_ends.len();
    NullFrame {
        minute_end: minute_ends,
        regime: vec![0; n],
        stratum: vec![0; n],
        race: [races, vec![NOT_RAN; n]],
    }
}

/// The exact p of `table` over every rotation of every block.
fn exact_p(table: &RotationTable) -> f64 {
    let e = table.observed().unwrap();
    let lengths: Vec<usize> = table.blocks.iter().map(Vec::len).collect();
    let total: usize = lengths.iter().product();
    let (mut above, mut below) = (0, 0);
    for config in 0..total {
        let mut rest = config;
        let ks: Vec<usize> = lengths
            .iter()
            .map(|&l| {
                let k = rest % l;
                rest /= l;
                k
            })
            .collect();
        let (sum, n) = table.at(&ks);
        let star = sum / n as f64;
        above += usize::from(star >= e);
        below += usize::from(star <= e);
    }
    (2.0 * (above.min(below) as f64 / total as f64)).min(1.0)
}

/// Two blocks of `l` weeks; one member at minute 10 of week 0 and one at
/// week `l`; the races of minute 10 of every week are `races`.
fn two_block_table(l: i64, races: &[i8]) -> RotationTable {
    let minute = 10 * MINUTE_MS;
    let slots: Vec<i64> = (0..2 * l).map(|w| W0 + w * WEEK_MS + minute).collect();
    let frame = simple_frame(slots.clone(), races.to_vec());
    let layout = Layout::new((W0, W0 + 2 * l * WEEK_MS), l);
    rotation_table(
        &frame,
        &frame.tilts(),
        &layout,
        &[slots[0], slots[l as usize]],
        0,
        0,
        1.0,
    )
}

#[test]
fn exhaustive_rotation_p_equals_the_hand_computed_p() {
    // L = 2, races +1, −1, +1, −1: tilt 0, E = 1; the four configurations
    // give 1, 0, 0, −1, so p⁺ = 1/4 and p = 1/2.
    let table = two_block_table(2, &[1, -1, 1, -1]);
    assert_eq!(table.observed(), Some(1.0));
    assert_eq!(exact_p(&table), 0.5);
    // L = 3, races +1, 0, −1 | +1, −1, −1: tilt −1/6, E = 1 + 1/6, the
    // only configuration that reaches it is the identity: p = 2/9.
    let table = two_block_table(3, &[1, 0, -1, 1, -1, -1]);
    assert!((table.observed().unwrap() - 7.0 / 6.0).abs() < 1e-12);
    assert!((exact_p(&table) - 2.0 / 9.0).abs() < 1e-12);
    // The Monte Carlo p approaches it.
    let p = null_p(
        std::slice::from_ref(&table),
        &[3, 3],
        20_000,
        REPLICATION_SEED,
        2,
    )[0]
    .unwrap();
    assert!((p - 2.0 / 9.0).abs() < 0.015, "{p}");
}

#[test]
fn only_eligible_positions_count_and_the_tail_stays_fixed() {
    // Window: two whole weeks and 3 days; blocks of 2 weeks, so one block
    // and a 3-day tail.
    let window = (W0, W0 + 2 * WEEK_MS + 3 * DAY_MS);
    let layout = Layout::new(window, 2);
    assert_eq!(layout.lengths(), [2]);
    let at = |week: i64, hour: i64| W0 + week * WEEK_MS + hour * HOUR_MS;
    let minute_ends = vec![at(0, 1), at(0, 2), at(0, 3), at(1, 1), at(1, 3), at(2, 1)];
    let mut frame = simple_frame(minute_ends, vec![1, 1, 1, -1, 1, 1]);
    // Week 1 hour 3: another regime; week 1 hour 2: no record at all.
    frame.regime[4] = 1;
    let tilts = frame.tilts();
    let tilt = tilts[strata_index(0, 0, 0)].unwrap();
    let table = rotation_table(
        &frame,
        &tilts,
        &layout,
        &[at(0, 1), at(0, 2), at(0, 3), at(2, 1)],
        0,
        0,
        1.0,
    );
    // Identity: hours 1, 2, 3 of week 0 (all +1); rotated by a week: only
    // hour 1 (−1) is eligible.
    assert_eq!(table.blocks[0][0].1, 3);
    assert_eq!(table.blocks[0][1].1, 1);
    assert!((table.blocks[0][1].0 - (-1.0 - tilt)).abs() < 1e-12);
    // The tail member never moves.
    assert_eq!(table.blocks[1].len(), 1);
    assert_eq!(table.blocks[1][0].1, 1);
}

/// One synthetic null study on hourly slots over `blocks` blocks of
/// `weeks` weeks: outcomes ±1 in runs (a flip with probability 1/25 an
/// hour), member labels in runs (a start with probability 1/100 an hour, a
/// stop with 1/12) independent of them, or, with `planted`, the outcome +1
/// at a member with probability 0.9.
fn synthetic(seed: u64, weeks: i64, blocks: i64, planted: bool) -> (NullFrame, Layout, Vec<i64>) {
    let mut lcg = Lcg(seed);
    let hours = blocks * weeks * 168;
    let slots: Vec<i64> = (0..hours).map(|h| W0 + h * HOUR_MS).collect();
    let mut races = Vec::with_capacity(slots.len());
    let mut labels = Vec::new();
    let (mut d, mut on) = (1i8, false);
    for &t in &slots {
        if lcg.below(25) == 0 {
            d = -d;
        }
        on = if on {
            lcg.below(12) != 0
        } else {
            lcg.below(100) == 0
        };
        let race = if on && planted && lcg.below(10) != 0 {
            1
        } else {
            d
        };
        races.push(race);
        if on {
            labels.push(t);
        }
    }
    let layout = Layout::new((W0, W0 + hours * HOUR_MS), weeks);
    (simple_frame(slots, races), layout, labels)
}

/// The free-shuffle p: as many pseudo-members as eligible labels, drawn
/// without replacement from every eligible slot.
fn free_shuffle_p(frame: &NullFrame, labels: &[i64], draws: usize, seed: u64) -> f64 {
    let tilt = frame.tilts()[strata_index(0, 0, 0)].unwrap();
    let y: Vec<f64> = frame.race[0].iter().map(|&d| f64::from(d) - tilt).collect();
    let observed: Vec<f64> = labels.iter().map(|&t| y[frame.find(t).unwrap()]).collect();
    let e = observed.iter().sum::<f64>() / observed.len() as f64;
    let mut rng = SplitMix64(seed);
    let mut pool: Vec<usize> = (0..y.len()).collect();
    let (mut above, mut below) = (0, 0);
    for _ in 0..draws {
        let mut sum = 0.0;
        for i in 0..observed.len() {
            let j = i + rng.below(pool.len() - i);
            pool.swap(i, j);
            sum += y[pool[i]];
        }
        let star = sum / observed.len() as f64;
        above += u64::from(star >= e);
        below += u64::from(star <= e);
    }
    two_sided(above, below, draws)
}

/// The smallest k with P(X > k) ≤ `tail` for X ~ Binomial(n, p).
fn binomial_bound(n: u64, p: f64, tail: f64) -> u64 {
    let mut pmf = (1.0 - p).powi(n as i32);
    let mut cdf = pmf;
    let mut k = 0;
    while 1.0 - cdf > tail {
        pmf *= (n - k) as f64 / (k + 1) as f64 * p / (1.0 - p);
        k += 1;
        cdf += pmf;
    }
    k
}

#[test]
fn the_rotation_null_holds_its_level_where_a_free_shuffle_does_not() {
    // Under H0 with clustered labels and autocorrelated outcomes: the
    // rotation rejects at 0.05 within a binomial bound over the seeds; a
    // free shuffle, whose null is as narrow as for independent members,
    // rejects far more often. This is why D3 rotates.
    let seeds = 100;
    let bound = binomial_bound(seeds, 0.05, 0.001);
    let (mut rotation, mut free) = (0, 0);
    for seed in 0..seeds {
        let (frame, layout, labels) = synthetic(seed, 4, 12, false);
        let table = rotation_table(&frame, &frame.tilts(), &layout, &labels, 0, 0, 1.0);
        let p = null_p(&[table], &layout.lengths(), 199, seed, 1)[0].unwrap();
        rotation += u64::from(p <= 0.05);
        free += u64::from(free_shuffle_p(&frame, &labels, 199, seed) <= 0.05);
    }
    assert!(
        rotation <= bound,
        "rotation rejected {rotation} of {seeds}, bound {bound}"
    );
    assert!(
        free > bound,
        "the free shuffle rejected only {free} of {seeds}, bound {bound}"
    );
}

#[test]
fn a_planted_effect_reads_p_below_1e_3() {
    let (frame, layout, labels) = synthetic(3, 4, 12, true);
    let table = rotation_table(&frame, &frame.tilts(), &layout, &labels, 0, 0, 1.0);
    assert!(table.observed().unwrap() > 0.2);
    let p = null_p(&[table], &layout.lengths(), 9_999, REPLICATION_SEED, 2)[0].unwrap();
    assert!(p < 1e-3, "{p}");
}

#[test]
fn the_rotation_p_is_pinned_and_does_not_depend_on_threads() {
    let tables: Vec<RotationTable> = (0..3)
        .map(|seed| {
            let (frame, layout, labels) = synthetic(seed, 4, 12, false);
            rotation_table(&frame, &frame.tilts(), &layout, &labels, 0, 0, 1.0)
        })
        .collect();
    let lengths = Layout::new((W0, W0 + 48 * WEEK_MS), 4).lengths();
    let one = null_p(&tables, &lengths, 999, REPLICATION_SEED, 1);
    assert_eq!(one, null_p(&tables, &lengths, 999, REPLICATION_SEED, 4));
    assert_eq!(one, null_p(&tables, &lengths, 999, REPLICATION_SEED, 7));
    assert_eq!(one, [Some(0.392), Some(0.606), Some(0.11)]);
}

// ---------------------------------------------------------------------------
// Tests: T2 and the reading.

#[test]
fn the_per_label_fee_and_the_direction() {
    assert_eq!(label_fee(10.0, 39.0), 10.0 / 39.0);
    assert_eq!(label_fee(10.0, 109.0), 10.0 / 109.0);
    assert_eq!(direction(Some(0.02)), Some(1.0));
    assert_eq!(direction(Some(-0.02)), Some(-1.0));
    assert_eq!(direction(Some(0.0)), None);
    assert_eq!(direction(None), None);
    assert_eq!(net_outcome(Outcome::Hypothesis, 1.0, 1.0, None, 0.1), 0.9);
    assert_eq!(net_outcome(Outcome::Counter, 1.0, -1.0, None, 0.1), -1.1);
    assert_eq!(net_outcome(Outcome::Counter, -1.0, 1.0, None, 0.1), 0.9);
    // A barrier race ignores the mark.
    assert_eq!(
        net_outcome(Outcome::Hypothesis, 1.0, 1.0, Some(-0.7), 0.1),
        0.9
    );
}

#[test]
fn a_neither_exit_is_marked_to_market_in_t2() {
    let fee = 0.25;
    // P1-sell (σ = +1, hypothesis upper) traded toward the hypothesis
    // (s = +1): the last trade drifting up, with the hypothesis, gains;
    // drifting down, against it, loses; at the reference only the fee.
    assert_eq!(
        net_outcome(Outcome::Neither, 1.0, 1.0, Some(0.5), fee),
        0.25
    );
    assert_eq!(
        net_outcome(Outcome::Neither, 1.0, 1.0, Some(-0.5), fee),
        -0.75
    );
    assert_eq!(
        net_outcome(Outcome::Neither, 1.0, 1.0, Some(0.0), fee),
        -0.25
    );
    // P1-buy (σ = −1, hypothesis lower): a fall is with the hypothesis.
    assert_eq!(
        net_outcome(Outcome::Neither, 1.0, -1.0, Some(-0.5), fee),
        0.25
    );
    assert_eq!(
        net_outcome(Outcome::Neither, 1.0, -1.0, Some(0.5), fee),
        -0.75
    );
    // Traded toward the counter barrier (s = −1), the signs turn.
    assert_eq!(
        net_outcome(Outcome::Neither, -1.0, -1.0, Some(-0.5), fee),
        -0.75
    );
    assert_eq!(
        net_outcome(Outcome::Neither, -1.0, 1.0, Some(-0.5), fee),
        0.25
    );
    // Without a last trade: at the reference.
    assert_eq!(net_outcome(Outcome::Neither, 1.0, 1.0, None, fee), -0.25);
}

#[test]
fn the_marks_read_each_labels_last_trade() {
    let mut recs = rep_recs(3, 5, 12);
    let built = rep_built(&mut recs, rep_header(), None);
    let views = rep_views(&built.dir);
    let measured = load_measured(&views, REPLICATION_RECORDS);
    // (with the hypothesis, against it, at the reference) among the
    // `neither` exits seen.
    let mut seen = [0; 3];
    for (i, member) in measured.inputs.members.iter().enumerate() {
        let rec = &recs[member.record as usize];
        for (h, &index) in REP_HORIZONS.iter().enumerate() {
            let want = rep_drift(rec, index) as f64 / rep_offset(rec, index) as f64;
            let Some(mark) = measured.marks[i][h] else {
                assert_eq!(rec.races[index], R5::Unavailable);
                continue;
            };
            assert!((mark - want).abs() < 1e-12, "{mark} {want}");
            if member.outcomes[index] != Outcome::Neither {
                continue;
            }
            let sigma = if member.upper { 1.0 } else { -1.0 };
            let net = net_outcome(Outcome::Neither, 1.0, sigma, Some(mark), 0.0);
            seen[if net > 0.0 {
                0
            } else if net < 0.0 {
                1
            } else {
                2
            }] += 1;
        }
    }
    assert!(seen.iter().all(|n| *n > 10), "{seen:?}");
}

#[test]
fn the_fee_reads_each_labels_own_barriers() {
    let mut recs = rep_recs(2, 5, 3);
    let built = rep_built(&mut recs, rep_header(), None);
    let views = rep_views(&built.dir);
    let inputs = load_coverage(&views, REPLICATION_RECORDS);
    let by_record: BTreeMap<u64, &Rec> = recs
        .iter()
        .enumerate()
        .map(|(i, rec)| (i as u64, rec))
        .collect();
    let mut checked = 0;
    for member in inputs.members.iter().take(200) {
        let rec = by_record[&member.record];
        for (h, &index) in REP_HORIZONS.iter().enumerate() {
            let want = rep_offset(rec, index) as f64 * 10_000.0 / R as f64;
            match member.half_bps[h] {
                Some(half) => {
                    assert!((half - want).abs() < 1e-9, "{half} {want}");
                    assert!((label_fee(FEE_BPS, half) - FEE_BPS / want).abs() < 1e-12);
                    checked += 1;
                }
                None => assert_eq!(rec.races[index], R5::Unavailable),
            }
        }
    }
    assert!(checked > 100, "{checked}");
}

#[test]
fn net_expectancy_is_over_the_units_representatives() {
    // Episode 0 at 0 and +30 min, episode 1 at +2 h, episode 2 at +5 h, at
    // H = 4h: representatives at 0 and +5 h (episode 1 lies within H).
    let h = 4 * HOUR_MS;
    let members = [
        (W0, 0, true),
        (W0 + 30 * MINUTE_MS, 0, true),
        (W0 + 2 * HOUR_MS, 1, true),
        (W0 + 5 * HOUR_MS, 2, true),
    ];
    assert_eq!(representatives(&members, h), [0, 3]);
    assert_eq!(unit_count(&members, h), (3, 2));
    let outcomes = [
        Outcome::Hypothesis,
        Outcome::Counter,
        Outcome::Counter,
        Outcome::Neither,
    ];
    let nets: Vec<f64> = representatives(&members, h)
        .iter()
        .map(|&i| net_outcome(outcomes[i], 1.0, 1.0, Some(0.0), 0.25))
        .collect();
    assert_eq!(nets, [0.75, -0.25]);
    // The representatives are the unit count's, on random members.
    let mut lcg = Lcg(205);
    for _ in 0..300 {
        let mut members = Vec::new();
        let (mut t, mut episode) = (W0, 0);
        for _ in 0..1 + lcg.below(30) {
            t += lcg.below(300) * MINUTE_MS;
            episode += u64::from(lcg.below(3) == 0);
            members.push((t, episode, lcg.below(4) != 0));
        }
        for (_, ms) in HORIZONS {
            let reps = representatives(&members, ms);
            assert_eq!(reps.len() as u64, unit_count(&members, ms).1);
            assert!(
                reps.windows(2)
                    .all(|p| members[p[1]].0 - members[p[0]].0 >= ms)
            );
        }
    }
}

#[test]
fn the_t2_bootstrap_is_deterministic_and_counts_undefined_resamples() {
    let settings = Settings {
        confidence: CONFIDENCE,
        mu: MU,
        fee_bps: FEE_BPS,
        resamples: 500,
        blocks: [56, 28],
        seed: REPLICATION_SEED,
        threads: 1,
    };
    // 400 days, a unit on every day with net +0.2 or −0.1.
    let nets: Vec<(usize, f64)> = (0..400)
        .map(|d| (d, if d % 3 == 0 { -0.1 } else { 0.2 }))
        .collect();
    let days = net_days(&nets);
    let weights = day_weights(400, 56, &settings);
    let values = t2_values(&days, &weights);
    assert_eq!(values, t2_values(&days, &day_weights(400, 56, &settings)));
    let t2 = interval(&values, 400, 385, CONFIDENCE);
    assert_eq!(t2.reading, Reading::Hypothesis, "{t2:?}");
    assert_eq!(t2.undefined, 0);
    // Units on 2 of 400 days, one-day blocks: most resamples draw none, so
    // too many are undefined and T2 reads `insufficient`.
    let sparse = net_days(&[(10, 0.5), (300, 0.5)]);
    let values = t2_values(&sparse, &day_weights(400, 1, &settings));
    let t2 = interval(&values, 400, 385, CONFIDENCE);
    assert!(t2.undefined > 25);
    assert_eq!(t2.reading, Reading::Insufficient);
    assert_eq!(
        read(400, 385, true, Some(1.0), Some(&t2)),
        RepReading::Insufficient
    );
}

#[test]
fn the_reading_arithmetic() {
    // The addendum: μ at 2 a week is 10.4 R a year.
    assert!((net_r_per_year(0.10, 2.0) - 10.4).abs() < 1e-12);
    let weeks = weeks_of(REPLICATION_RECORDS);
    assert!((weeks - 247.857_142_857).abs() < 1e-6);
    assert!((units_per_week(385, weeks) - 1.553_314).abs() < 1e-6);
    assert_eq!(max_drawdown(&[1.0, -2.0, 0.5, -1.0, 3.0]), 2.5);
    assert_eq!(max_drawdown(&[-1.0, -1.0]), 2.0);
    assert_eq!(max_drawdown(&[1.0, 1.0]), 0.0);
    let pass = Interval {
        low: 0.01,
        high: 0.2,
        undefined: 0,
        reading: Reading::Hypothesis,
    };
    let fail = Interval {
        reading: Reading::Inconclusive,
        low: -0.01,
        ..pass
    };
    assert_eq!(
        read(384, 385, true, Some(1.0), Some(&pass)),
        RepReading::NotMeasurable
    );
    assert_eq!(
        read(385, 385, false, Some(1.0), Some(&pass)),
        RepReading::NoEffect
    );
    assert_eq!(read(385, 385, true, None, None), RepReading::NoEffect);
    assert_eq!(
        read(385, 385, true, Some(-1.0), Some(&pass)),
        RepReading::Replicated(false)
    );
    assert_eq!(
        read(385, 385, true, Some(1.0), Some(&pass)),
        RepReading::Replicated(true)
    );
    assert_eq!(
        read(385, 385, true, Some(1.0), Some(&fail)),
        RepReading::NotNetPositive
    );
}

// ---------------------------------------------------------------------------
// Tests: the grid.

#[test]
fn the_grid_has_48_contexts_in_a_fixed_order() {
    let all = contexts();
    assert_eq!(all.len(), 48);
    assert_eq!(all[0].label(), "P1-buy w=5m low 4h");
    assert_eq!(all[1].label(), "P1-buy w=5m low 1d");
    assert_eq!(all[2].label(), "P1-buy w=5m medium 4h");
    assert_eq!(all[47].label(), "P1-sell w=1h extreme 1d");
    let labels: BTreeSet<String> = all.iter().map(Context::label).collect();
    assert_eq!(labels.len(), 48);
    assert_eq!(all[0].sigma(), -1.0, "P1-buy expects the lower barrier");
    assert_eq!(all[47].sigma(), 1.0);
}

fn coverage_member(k: i64, regime: usize) -> CoverageMember {
    CoverageMember {
        side: 0,
        window: 0,
        record: k as u64,
        decision: W0 + k * MINUTE_MS + 500,
        minute_end: W0 + k * MINUTE_MS,
        regime,
        location: 1,
        ran: [true, true],
        half_bps: [Some(40.0), Some(100.0)],
    }
}

#[test]
fn a_regime_not_ready_member_is_in_no_context() {
    let inputs = CoverageInputs {
        members: vec![coverage_member(0, NOT_READY)],
        window: REPLICATION_RECORDS,
        ..CoverageInputs::default()
    };
    let results = coverage_of(&inputs);
    assert!(results.iter().all(|r| r.ran == 0 && r.units == 0));
    let body = coverage_body(&inputs, &results, ((0, 0, 0), 0, [0, 0]));
    assert!(body.contains("| P1-buy | 5m | 1 | 1 |"), "{body}");
}

#[test]
fn an_episode_whose_opener_lies_in_another_regime_is_kept() {
    // One P1-buy episode at w = 5m, three members a minute apart, the
    // opener `low` (0), the others `high` (2).
    let inputs = CoverageInputs {
        members: vec![
            coverage_member(0, 0),
            coverage_member(1, 2),
            coverage_member(2, 2),
        ],
        window: REPLICATION_RECORDS,
        ..CoverageInputs::default()
    };
    assert_eq!(coverage_episodes(&inputs.members), [0, 0, 0]);
    let results = coverage_of(&inputs);
    let high_4h = contexts()
        .iter()
        .position(|c| c.label() == "P1-buy w=5m high 4h")
        .unwrap();
    let r = &results[high_4h];
    assert_eq!((r.ran, r.episodes, r.units), (2, 1, 1));
    assert_eq!(r.representatives, [1]);
    // No baseline at all: both are dropped from E, still counted.
    assert_eq!(r.dropped, 2);
    assert_eq!(r.fee, Some(0.25));
}

// ---------------------------------------------------------------------------
// Tests: coverage blindness.

#[test]
fn the_coverage_views_carry_no_outcome() {
    let mut recs = rep_recs(1, 30, 4);
    let built = rep_built(&mut recs, rep_header(), None);
    let views = rep_views(&built.dir);
    let columns = |view: &str| -> Vec<String> {
        let mut names = Vec::new();
        each_row(&tsv_lines(&views, &format!("DESCRIBE {view}")), |get| {
            names.push(get("column_name"));
        });
        names
    };
    let widths = [
        "record",
        "decision_ms",
        "minute_end_ms",
        "regime",
        "location",
        "ran_4h",
        "ran_1d",
        "half_bps_4h",
        "half_bps_1d",
    ];
    assert_eq!(
        columns("rep_ran"),
        ["record", "horizon", "upper", "reference", "ran"]
    );
    assert_eq!(
        columns("rep_coverage_widths"),
        ["record", "ran_4h", "ran_1d", "half_bps_4h", "half_bps_1d"]
    );
    let mut members = vec!["pair", "effort_window"];
    members.extend(widths);
    assert_eq!(columns("rep_coverage_members"), members);
    assert_eq!(columns("rep_coverage_baseline"), widths);
    assert_eq!(
        columns("rep_coverage_exclusions"),
        [
            "effort_window",
            "window_order",
            "records",
            "not_ready",
            "incomplete"
        ]
    );
    for view in [
        "rep_ran",
        "rep_coverage_widths",
        "rep_coverage_members",
        "rep_coverage_baseline",
        "rep_coverage_exclusions",
    ] {
        for column in columns(view) {
            assert!(
                !column.contains("race") && !column.starts_with("out_"),
                "{view}.{column}"
            );
        }
    }
    // The coverage chain never names an outcome-carrying view, a mark or a
    // race beyond `rep_ran`'s projection to `ran`.
    let mut coverage_views = 0;
    for statement in REPLICATION_SQL.split("CREATE OR REPLACE VIEW ").skip(1) {
        let name = statement.split_whitespace().next().unwrap();
        if !(name == "rep_ran" || name.starts_with("rep_coverage_")) {
            continue;
        }
        coverage_views += 1;
        let body: String = statement
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .collect::<Vec<_>>()
            .join("\n");
        for forbidden in [
            "study_members",
            "study_baseline",
            "study_races",
            "labeled",
            "rep_marks",
            "rep_members",
            "rep_baseline",
            "last",
            "race_at",
            "up_at",
            "down_at",
        ] {
            assert!(!body.contains(forbidden), "{name} names {forbidden}");
        }
        let races = body.matches("race").count();
        if name == "rep_ran" {
            assert_eq!(races, 1, "{body}");
            assert!(body.contains("race IN ('upper', 'lower', 'neither') AS ran"));
        } else {
            assert_eq!(races, 0, "{body}");
        }
    }
    assert_eq!(coverage_views, 5);
}

/// The coverage body of a fixture written from `recs` (cloned) with races
/// mapped by `map` and the scramble seed.
fn coverage_body_of(recs: &[Rec], map: impl Fn(R5) -> R5, scramble: Option<u64>) -> String {
    let mut recs: Vec<Rec> = recs.to_vec();
    for rec in &mut recs {
        rec.races = rec.races.map(&map);
    }
    let built = rep_built(&mut recs, rep_header(), scramble);
    coverage_run(&built.dir, 2).unwrap().1
}

#[test]
fn the_coverage_output_does_not_move_with_the_outcomes() {
    let recs = rep_recs(5, 3, 5);
    let original = coverage_body_of(&recs, |r| r, None);
    assert!(original.contains("| yes |") || original.contains("| no |"));
    // Upper and lower swapped on every race that ran.
    let swapped = coverage_body_of(
        &recs,
        |r| match r {
            R5::Up => R5::Down,
            R5::Down => R5::Up,
            other => other,
        },
        None,
    );
    assert_eq!(swapped, original);
    // Neither and a barrier swapped on the uncensored windows.
    let neither = coverage_body_of(
        &recs,
        |r| match r {
            R5::Neither => R5::Up,
            R5::Up => R5::Neither,
            other => other,
        },
        None,
    );
    assert_eq!(neither, original);
    // Random race times and excursions.
    for seed in [1, 2] {
        assert_eq!(coverage_body_of(&recs, |r| r, Some(seed)), original);
    }
}

#[test]
fn coverage_units_equal_the_measurement_paths_and_the_twins() {
    let mut recs = rep_recs(6, 2, 6);
    let built = rep_built(&mut recs, rep_header(), None);
    let views = rep_views(&built.dir);
    let coverage = load_coverage(&views, REPLICATION_RECORDS);
    let results = coverage_of(&coverage);
    assert!(results.iter().filter(|r| r.units > 3).count() > 20);
    // The coverage twin.
    let twin = coverage_twin(&views);
    assert_eq!(coverage_mismatches(&coverage, &results, &twin, false), 0);
    // A twin that sees one member fewer disagrees.
    let mut short = coverage.clone();
    let dropped = short
        .members
        .iter()
        .position(|m| m.ran[0] && m.regime != NOT_READY)
        .unwrap();
    short.members.remove(dropped);
    assert!(coverage_mismatches(&short, &coverage_of(&short), &twin, true) > 0);
    // The record set of rep_coverage_members is the P1 rows of
    // study_members.
    assert_eq!(record_set_difference(&views), 0);
    // The measurement path's cell_units for all 48 contexts.
    let measured = load_measured(&views, REPLICATION_RECORDS);
    let populations = populations();
    let episodes = member_episodes(&measured.inputs, &populations);
    for (result, cell) in results.iter().zip(replication_cells(&measured.inputs)) {
        assert_eq!(
            (result.episodes, result.units),
            cell_units(&measured.inputs, &episodes, &cell)
        );
    }
    // The ADR-058 twin on the P1 rows.
    assert_eq!(p1_twin_mismatches(&views, &measured.inputs), 0);
}

#[test]
fn the_coverage_output_is_the_same_across_threads() {
    let mut recs = rep_recs(3, 2, 8);
    let built = rep_built(&mut recs, rep_header(), None);
    assert_eq!(coverage_run(&built.dir, 1), coverage_run(&built.dir, 4));
}

// ---------------------------------------------------------------------------
// Tests: the measurement on a fixture.

#[test]
fn the_measurement_runs_on_a_pinned_fixture_and_is_deterministic() {
    let mut recs = rep_recs(16, 2, 9);
    let built = rep_built(&mut recs, rep_header(), None);
    let manifest = OutcomeDataset::open(&built.dir).unwrap().manifest().clone();
    let views = rep_views(&built.dir);
    let (contexts_count, _, ran) = dataset_counts(&views);
    let leak = |s: &str| -> &'static str { Box::leak(s.to_owned().into_boxed_str()) };
    let pins = Pins {
        data: Some(leak(&manifest.data)),
        content: Some(leak(&manifest.content)),
        contexts: Some(contexts_count),
        ran: Some(ran),
    };
    // A fixture cannot reach n_min = 385 units at 4h in 16 days, so every
    // context reads `not measurable` and shows only its counts (S2).
    let text = measure_run(&built.dir, &pins, &FIXTURE_RUN).unwrap();
    assert!(text.contains("measurable: 0 of 48"), "{text}");
    assert_eq!(text.matches("not measurable |").count(), 48);
    // A pin that does not match refuses.
    let wrong = Pins {
        ran: Some([ran[0] + 1, ran[1]]),
        ..pins
    };
    assert!(measure_run(&built.dir, &wrong, &FIXTURE_RUN).is_err());
}

#[test]
fn the_measured_statistics_are_consistent_and_thread_independent() {
    // E at the identity rotation equals the ADR-058 E for every context
    // that has one, and the whole measurement, run with a μ large enough
    // for a fixture to reach n_min, does not depend on the threads.
    let mut recs = rep_recs(16, 2, 10);
    let built = rep_built(&mut recs, rep_header(), None);
    let views = rep_views(&built.dir);
    let measured = load_measured(&views, REPLICATION_RECORDS);
    let frame = null_frame(&measured);
    let tilts = frame.tilts();
    let cells = replication_cells(&measured.inputs);
    let day_index: BTreeMap<i64, usize> = measured
        .inputs
        .baseline
        .iter()
        .map(|b| b.decision)
        .chain(measured.inputs.members.iter().map(|m| m.decision))
        .map(|ms| ms.div_euclid(DAY_MS))
        .collect::<BTreeSet<i64>>()
        .iter()
        .enumerate()
        .map(|(i, d)| (*d, i))
        .collect();
    let baseline_days = BaselineDays::new(&measured.inputs, &day_index);
    let ones = vec![1u32; day_index.len()];
    let mut totals = Vec::new();
    baseline_days.totals(&ones, &mut totals);
    let layout = Layout::new(REPLICATION_RECORDS, 8);
    let mut tables = Vec::new();
    let mut compared = 0;
    for (context, cell) in contexts().iter().zip(&cells) {
        let effect =
            estimate(&prepare(&measured.inputs, cell, &day_index), &ones, &totals).effect();
        let labels: Vec<i64> = cell
            .members
            .iter()
            .map(|&i| measured.inputs.members[i].minute_end.unwrap())
            .collect();
        let table = rotation_table(
            &frame,
            &tilts,
            &layout,
            &labels,
            context.regime,
            context.horizon,
            context.sigma(),
        );
        assert_eq!(
            table.observed().is_some(),
            effect.is_some(),
            "{}",
            context.label()
        );
        if let (Some(a), Some(b)) = (table.observed(), effect) {
            assert!((a - b).abs() < 1e-9, "{}: {a} {b}", context.label());
            compared += 1;
            tables.push(table);
        }
    }
    assert!(compared > 20, "{compared}");
    let lengths = layout.lengths();
    let one = null_p(&tables, &lengths, 299, REPLICATION_SEED, 1);
    assert_eq!(one, null_p(&tables, &lengths, 299, REPLICATION_SEED, 3));
    // The whole measurement is deterministic across threads.
    let run = |threads: usize| {
        let settings = RunSettings {
            mu: 0.6,
            threads,
            ..FIXTURE_RUN
        };
        let results = measure_contexts(&measured, &settings);
        let body = measurement_body(&measured, &results, &settings);
        (results, body)
    };
    let (results, one) = run(1);
    assert_eq!(one, run(4).1);
    // Some contexts reach n_min = 11 and are read through T3, BH and T2.
    let read_contexts: Vec<&ContextResult> = results
        .iter()
        .filter(|r| r.reading != RepReading::NotMeasurable)
        .collect();
    assert!(read_contexts.len() > 10, "{one}");
    for r in &read_contexts {
        assert!(r.p[0].is_some() && r.p[1].is_some(), "{r:?}");
        assert_eq!(r.t2.len(), 3);
        assert!(r.expectancy.is_some() && r.d7.is_some());
    }
    // Random races carry no effect: nothing replicates.
    assert!(
        !results
            .iter()
            .any(|r| matches!(r.reading, RepReading::Replicated(_))),
        "{one}"
    );
}
