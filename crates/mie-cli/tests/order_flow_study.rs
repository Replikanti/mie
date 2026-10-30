//! The order-flow interaction study of #28 (ADR-058): the blind measurement
//! tool and its fixture tests.
//!
//! The populations come from the committed study queries
//! `docs/research/028-order-flow-interactions.sql` (ADR-058 D3), run over
//! the DuckDB views of one verified format-2 outcome dataset (ADR-057 D7,
//! ADR-058 D2). Rust does what the SQL does not (ADR-058 D4–D7):
//!
//! - the stratum of a record: its New York hour of week (168, the clock of
//!   `common::clock`, #155) × its ADR-051 R1 location-bin set;
//! - per cell (population × horizon, or a D8 split of it): the shares of
//!   the hypothesis barrier, the counter barrier and `neither` among the
//!   races that ran, the tilt T, the directly standardized baseline tilt
//!   T_b and the effect E = T − T_b;
//! - the episodes of each population and the independent units of each
//!   cell (D6): episodes first (the trigger's episode id, or ADR-054 D4's
//!   join rule over the members' effort spans, plus P3's failure runs),
//!   then the greedy horizon thinning of ADR-055 D9 over each episode's
//!   earliest member whose race ran;
//! - n_min = ⌈(z/μ)²⌉ with z from Acklam's normal quantile (D6);
//! - a paired circular block bootstrap over the UTC days of the decision
//!   times, 56-day blocks and 28-day blocks for sensitivity (D5), from per
//!   day × stratum counts;
//! - the reading of every cell and the multiplicity line of every family
//!   (D7), printed as markdown tables (D9). Nothing per record is printed.
//!
//! An independent **twin**, written from the ADR-058 text, rebuilds the
//! study views and every cell's unit count from the dataset's base views
//! (`contexts`, `effort`, `outcomes`, `passages`); the tests and the
//! measurement compare it row by row with the SQL and with the tool.
//!
//! - **Fixture tests** write datasets with `OutcomeWriter` from hand-built
//!   records at every population edge and from seeded random records, and
//!   test the unit count, n_min, the clock, the bootstrap and the
//!   determinism of the output.
//! - **`measure_order_flow_study`** (ignored; ADR-058, run once in #28 M3)
//!   verifies the dataset in `MIE_OUTCOME_DATASET`, asserts the D1
//!   provenance and the ADR-057 reconciliation counts, compares the twin
//!   (0 mismatches, or it stops before printing anything), and prints the
//!   pre-registered tables for the inputs `MIE_STUDY_CONFIDENCE` (c),
//!   `MIE_STUDY_MU` (μ, in R), `MIE_STUDY_FEE_BPS` (the round-trip fee, for
//!   the fee in R per horizon) and `MIE_STUDY_EXCLUDE` (`from..to` ms
//!   ranges, comma-separated, possibly empty):
//!
//! ```sh
//! MIE_OUTCOME_DATASET=<dir> MIE_STUDY_CONFIDENCE=<c> MIE_STUDY_MU=<mu> \
//!     MIE_STUDY_FEE_BPS=<bps> MIE_STUDY_EXCLUDE= \
//!     cargo test --release -p mie-cli --test order_flow_study \
//!     measure_order_flow_study -- --ignored --nocapture
//! ```

mod common;

use common::TempDir;
use common::bootstrap::{SplitMix64, resample};
use common::clock::on_clock;
use common::tapes::Lcg;
use common::year::{YEAR_CONTEXTS, YEAR_HORIZONS, YEAR_PROVENANCE, YEAR_RECORDS};
use mie_adapter_duckdb::OutcomeViews;
use mie_adapter_parquet::outcome::{OutcomeDataset, OutcomeHeader, OutcomeWriter};
use mie_cli::conditional;
use mie_domain::bars::{Coverage, Timeframe};
use mie_domain::candidate::effort::EffortResult;
use mie_domain::candidate::{ContextLevel, ContextLevels, LocationContext};
use mie_domain::conditional::label::{
    Barriers, Censor, CensorReason, Excursion, ForwardLabel, HORIZONS as LABEL_HORIZONS,
    HorizonLabel, LabelProvenance, Labeling, Passage, PassageLabel, Race,
};
use mie_domain::conditional::{
    ContextProvenance, ContextRecord, Episode, EpisodeId, Sample, StateSnapshot,
};
use mie_domain::event::Aggressor;
use mie_domain::feature::{FeatureDefinition, FeatureValue, Unavailability, catalog};
use mie_domain::flow::DayCvd;
use mie_domain::location::{AuctionState, AuctionStatus, LevelKind, Origin, Position, Region};
use mie_domain::num::{Price, Qty};
use mie_domain::order::{CanonicalKey, EventKind};
use mie_domain::regime::{AtrPercentile, Regime};
use mie_domain::research::DataVersion;
use mie_domain::state::MarketStateEngine;
use mie_domain::structure::{Level, Side, Sweep, SweepOutcome};
use mie_domain::time::EventTime;
use mie_domain::trigger::{Bias, TriggerEvent, TriggerEvidence, TriggerFamily};
use mie_ports::outbound::ReplayWindow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::path::Path;

/// The committed study queries (ADR-058 D3).
const STUDY_SQL: &str = include_str!("../../../docs/research/028-order-flow-interactions.sql");

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

/// ADR-055 D3: the horizons, shortest first, with their lengths.
const HORIZONS: [(&str, i64); 5] = [
    ("5m", 300_000),
    ("15m", 900_000),
    ("1h", 3_600_000),
    ("4h", 14_400_000),
    ("1d", 86_400_000),
];

/// The effort windows of `catalog::CANDIDATE_EFFORT_RESULT`, with their
/// lengths: the chaining span w of ADR-058 D6.
const WINDOWS: [(&str, i64); 3] = [("5m", 300_000), ("15m", 900_000), ("1h", 3_600_000)];

/// The regime values of the study rows, `not_ready` last; the labels
/// before it are the D8 split values and the baseline restrictions of
/// regime-split cells.
const REGIMES: [&str; 5] = ["low", "medium", "high", "extreme", "not_ready"];

/// The regime split values (ADR-058 D8): the four labels.
const REGIME_SPLITS: [&str; 4] = ["low", "medium", "high", "extreme"];

/// The location-bin sets of the strata (ADR-058 D4).
const LOCATIONS: [&str; 5] = [
    "not_ready",
    "neither",
    "value_edge",
    "structural_extreme",
    "both",
];

/// The location split values (ADR-057 D7's bins): a record in both bins is
/// in both split cells (ADR-051 R1).
const BINS: [&str; 4] = ["value_edge", "structural_extreme", "neither", "not_ready"];

/// The aggressor split values of P2 and P3.
const SIDES: [&str; 2] = ["buy", "sell"];

/// The CVD-agreement split values of P1 and P2: the aggressor against the
/// sign of the UTC-day CVD. A member whose CVD is not ready or incomplete
/// reads `unknown` and is in no CVD cell.
const CVD: [&str; 3] = ["with", "against", "flat"];

/// The trigger ids of P4, one cell each.
const P4_TRIGGERS: [&str; 5] = [
    "trigger.failed_auction@1",
    "trigger.sfp.15m@1",
    "trigger.sfp.1h@1",
    "trigger.sfp.4h@1",
    "trigger.sfp.1d@1",
];

/// Strata: 168 hours of the week × 5 location-bin sets.
const STRATA: usize = 168 * LOCATIONS.len();

/// The bootstrap settings of ADR-058 D5.
const RESAMPLES: usize = 10_000;
const BLOCKS: [usize; 2] = [56, 28];
const SEED: u64 = 28;

// ---------------------------------------------------------------------------
// The inputs, read from the study views.

/// A race as stored (ADR-055 D4).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StoredRace {
    Upper,
    Lower,
    Neither,
    Censored,
    Unavailable,
}

impl StoredRace {
    fn parse(text: &str) -> Self {
        match text {
            "upper" => Self::Upper,
            "lower" => Self::Lower,
            "neither" => Self::Neither,
            "censored" => Self::Censored,
            "unavailable" => Self::Unavailable,
            other => panic!("race {other}"),
        }
    }
}

/// A race mapped to a pair's hypothesis (the SQL's `study_map`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Outcome {
    Hypothesis,
    Counter,
    Neither,
    Censored,
    Unavailable,
}

impl Outcome {
    fn parse(text: &str) -> Self {
        match text {
            "hypothesis" => Self::Hypothesis,
            "counter" => Self::Counter,
            "neither" => Self::Neither,
            "censored" => Self::Censored,
            "unavailable" => Self::Unavailable,
            other => panic!("outcome {other}"),
        }
    }

    fn ran(self) -> bool {
        matches!(self, Self::Hypothesis | Self::Counter | Self::Neither)
    }
}

/// A baseline record (`study_baseline`).
#[derive(Clone, Debug, PartialEq)]
struct Baseline {
    record: u64,
    decision: i64,
    regime: String,
    location: String,
    auction_state: Option<String>,
    auction_side: Option<String>,
    races: [StoredRace; 5],
}

/// A population member (`study_members`).
#[derive(Clone, Debug, PartialEq)]
struct Member {
    pair: String,
    window: Option<String>,
    trigger: Option<String>,
    record: u64,
    decision: i64,
    side: String,
    /// Whether the hypothesis barrier is the upper one.
    upper: bool,
    regime: String,
    location: String,
    episode: Option<String>,
    cvd: Option<String>,
    outcomes: [Outcome; 5],
}

/// One row of `study_exclusions`.
#[derive(Clone, Debug, PartialEq)]
struct Exclusion {
    universe: String,
    window: Option<String>,
    trigger: Option<String>,
    records: u64,
    not_ready: u64,
    incomplete: u64,
    no_side: u64,
}

/// What the study reads (ADR-058 D9 prints nothing per record).
#[derive(Clone, Debug, Default)]
struct Inputs {
    baseline: Vec<Baseline>,
    members: Vec<Member>,
    exclusions: Vec<Exclusion>,
    /// Per horizon: the baselines with barriers and the median half-width
    /// in bps.
    barriers: Vec<(String, u64, f64)>,
    /// P4's SFP cells: trigger, horizon, `extreme_vs_opposite`, records.
    sfp_extreme: Vec<(String, String, String, u64)>,
}

/// The TSV lines (header first) of the study views the twin rebuilds.
#[derive(Clone, Debug, Default, PartialEq)]
struct Lines {
    baseline: Vec<String>,
    members: Vec<String>,
    exclusions: Vec<String>,
}

/// The TSV of `sql` as lines, header first.
fn tsv_lines(views: &OutcomeViews, sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    views.select_tsv(sql, &mut out).unwrap();
    String::from_utf8(out)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect()
}

/// Calls `f` with each data row of `lines` as named cells.
fn each_row(lines: &[String], mut f: impl FnMut(&dyn Fn(&str) -> String)) {
    let names: BTreeMap<&str, usize> = lines[0]
        .split('\t')
        .enumerate()
        .map(|(index, name)| (name, index))
        .collect();
    for line in &lines[1..] {
        let cells: Vec<&str> = line.split('\t').collect();
        let get = |name: &str| {
            let index = names
                .get(name)
                .unwrap_or_else(|| panic!("no column {name}"));
            cells[*index].to_owned()
        };
        f(&get);
    }
}

/// A `Write` that hands every complete line of a TSV to `f` as it
/// arrives, so a base view of the year is never held whole.
struct Rows<F: FnMut(&str)> {
    buffer: Vec<u8>,
    f: F,
}

impl<F: FnMut(&str)> std::io::Write for Rows<F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        let mut start = 0;
        while let Some(end) = self.buffer[start..].iter().position(|b| *b == b'\n') {
            (self.f)(std::str::from_utf8(&self.buffer[start..start + end]).unwrap());
            start += end + 1;
        }
        self.buffer.drain(..start);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Calls `f` with each data row of the query `sql`, streamed, as named
/// cells.
fn each_selected(views: &OutcomeViews, sql: &str, mut f: impl FnMut(&dyn Fn(&str) -> String)) {
    let mut names: Option<BTreeMap<String, usize>> = None;
    let mut sink = Rows {
        buffer: Vec::new(),
        f: |line: &str| match &names {
            None => {
                names = Some(
                    line.split('\t')
                        .enumerate()
                        .map(|(index, name)| (name.to_owned(), index))
                        .collect(),
                );
            }
            Some(names) => {
                let cells: Vec<&str> = line.split('\t').collect();
                let get = |name: &str| {
                    let index = names
                        .get(name)
                        .unwrap_or_else(|| panic!("no column {name}"));
                    cells[*index].to_owned()
                };
                f(&get);
            }
        },
    };
    views.select_tsv(sql, &mut sink).unwrap();
}

fn opt(cell: String) -> Option<String> {
    (cell != "NULL").then_some(cell)
}

/// Creates the study views (after the dataset's own) and fills
/// `study_exclude` with `exclude`.
fn create_study(views: &OutcomeViews, exclude: &[(i64, i64)]) {
    views.create_views(STUDY_SQL).unwrap();
    views.create_views("DELETE FROM study_exclude;").unwrap();
    for (from, to) in exclude {
        views
            .create_views(&format!("INSERT INTO study_exclude VALUES ({from}, {to});"))
            .unwrap();
    }
}

/// Reads the study views.
fn load(views: &OutcomeViews) -> (Inputs, Lines) {
    let lines = Lines {
        baseline: tsv_lines(views, "SELECT * FROM study_baseline ORDER BY record"),
        members: tsv_lines(
            views,
            "SELECT * FROM study_members ORDER BY pair, \
             list_position(['5m', '15m', '1h'], effort_window), trigger NULLS FIRST, record",
        ),
        exclusions: tsv_lines(
            views,
            "SELECT * FROM study_exclusions ORDER BY universe, window_order, \
             trigger NULLS FIRST",
        ),
    };
    let mut inputs = Inputs::default();
    let races = |get: &dyn Fn(&str) -> String, prefix: &str| {
        std::array::from_fn(|h| get(&format!("{prefix}{}", HORIZONS[h].0)))
    };
    each_row(&lines.baseline, |get| {
        let races: [String; 5] = races(get, "race_");
        inputs.baseline.push(Baseline {
            record: get("record").parse().unwrap(),
            decision: get("decision_ms").parse().unwrap(),
            regime: get("regime"),
            location: get("location"),
            auction_state: opt(get("auction_state")),
            auction_side: opt(get("auction_side")),
            races: races.map(|race| StoredRace::parse(&race)),
        });
    });
    each_row(&lines.members, |get| {
        let outcomes: [String; 5] = races(get, "out_");
        inputs.members.push(Member {
            pair: get("pair"),
            window: opt(get("effort_window")),
            trigger: opt(get("trigger")),
            record: get("record").parse().unwrap(),
            decision: get("decision_ms").parse().unwrap(),
            side: get("side"),
            upper: match get("hypothesis").as_str() {
                "upper" => true,
                "lower" => false,
                other => panic!("hypothesis {other}"),
            },
            regime: get("regime"),
            location: get("location"),
            episode: opt(get("episode")),
            cvd: opt(get("cvd")),
            outcomes: outcomes.map(|outcome| Outcome::parse(&outcome)),
        });
    });
    each_row(&lines.exclusions, |get| {
        inputs.exclusions.push(Exclusion {
            universe: get("universe"),
            window: opt(get("effort_window")),
            trigger: opt(get("trigger")),
            records: get("records").parse().unwrap(),
            not_ready: get("not_ready").parse().unwrap(),
            incomplete: get("incomplete").parse().unwrap(),
            no_side: get("no_side").parse().unwrap(),
        });
    });
    let barriers = tsv_lines(views, "SELECT * FROM study_barriers ORDER BY horizon_ms");
    each_row(&barriers, |get| {
        inputs.barriers.push((
            get("horizon"),
            get("barriers").parse().unwrap(),
            get("median_half_width_bps").parse().unwrap(),
        ));
    });
    let sfp = tsv_lines(
        views,
        "SELECT * FROM study_sfp_extreme ORDER BY trigger, horizon_ms, extreme_vs_opposite",
    );
    each_row(&sfp, |get| {
        inputs.sfp_extreme.push((
            get("trigger"),
            get("horizon"),
            get("extreme_vs_opposite"),
            get("records").parse().unwrap(),
        ));
    });
    (inputs, lines)
}

// ---------------------------------------------------------------------------
// Populations, cells and the independent units (ADR-058 D3, D6, D8).

/// One population of ADR-058 D3, in pre-registered order.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Population {
    pair: &'static str,
    /// The effort window's index in [`WINDOWS`] (P1, P2, P3, P1-trig).
    window: Option<usize>,
    trigger: Option<String>,
}

impl Population {
    fn label(&self) -> String {
        match (&self.window, &self.trigger) {
            (Some(w), None) => format!("{} w={}", self.pair, WINDOWS[*w].0),
            (_, Some(trigger)) => format!("{} {trigger}", self.pair),
            (None, None) => self.pair.to_owned(),
        }
    }

    fn holds(&self, member: &Member) -> bool {
        member.pair == self.pair
            && member.window.as_deref() == self.window.map(|w| WINDOWS[w].0)
            && member.trigger == self.trigger
    }

    /// Whether its episodes come from the record's episode id (trigger
    /// records) rather than from the chaining rule (baseline records).
    fn trigger_episodes(&self) -> bool {
        matches!(self.pair, "P4" | "P1-trig")
    }
}

/// Every population, in the order the report prints them.
fn populations() -> Vec<Population> {
    let mut all = Vec::new();
    for pair in ["P1-buy", "P1-sell", "P2", "P3"] {
        for w in 0..WINDOWS.len() {
            all.push(Population {
                pair,
                window: Some(w),
                trigger: None,
            });
        }
    }
    for trigger in P4_TRIGGERS {
        all.push(Population {
            pair: "P4",
            window: None,
            trigger: Some(trigger.to_owned()),
        });
    }
    for (w, (window, _)) in WINDOWS.iter().enumerate() {
        all.push(Population {
            pair: "P1-trig",
            window: Some(w),
            trigger: Some(format!("trigger.effort_result.{window}@1")),
        });
    }
    all
}

/// A secondary split (ADR-058 D8), or the primary cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Split {
    All,
    Regime(&'static str),
    Location(&'static str),
    Side(&'static str),
    Cvd(&'static str),
}

impl Split {
    fn dimension(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Regime(_) => "regime",
            Self::Location(_) => "location",
            Self::Side(_) => "side",
            Self::Cvd(_) => "cvd",
        }
    }

    fn value(self) -> &'static str {
        match self {
            Self::All => "all",
            Self::Regime(v) | Self::Location(v) | Self::Side(v) | Self::Cvd(v) => v,
        }
    }

    fn holds(self, member: &Member) -> bool {
        match self {
            Self::All => true,
            Self::Regime(v) => member.regime == v,
            Self::Location(v) => in_bin(&member.location, v),
            Self::Side(v) => member.side == v,
            Self::Cvd(v) => member.cvd.as_deref() == Some(v),
        }
    }

    /// The baseline restriction: 0 for the whole baseline, else 1 + the
    /// regime's index. A location split needs none: the stratum holds the
    /// bin set, and every bin set of a member in bin `v` lies in `v`.
    fn restriction(self) -> usize {
        match self {
            Self::Regime(v) => 1 + REGIMES.iter().position(|r| *r == v).unwrap(),
            _ => 0,
        }
    }
}

/// Whether a record with the bin set `location` lies in the bin `bin`.
fn in_bin(location: &str, bin: &str) -> bool {
    location == bin || (location == "both" && matches!(bin, "value_edge" | "structural_extreme"))
}

/// The splits of a population: the primary cell, then the D8 dimensions
/// that apply to its pair.
fn splits(population: &Population) -> Vec<Split> {
    let mut all = vec![Split::All];
    all.extend(REGIME_SPLITS.map(Split::Regime));
    all.extend(BINS.map(Split::Location));
    if matches!(population.pair, "P2" | "P3") {
        all.extend(SIDES.map(Split::Side));
    }
    if matches!(population.pair, "P1-buy" | "P1-sell" | "P2") {
        all.extend(CVD.map(Split::Cvd));
    }
    all
}

/// A cell: a population at one horizon, primary or split.
#[derive(Clone, Debug)]
struct Cell {
    population: usize,
    split: Split,
    horizon: usize,
    /// Indices into `Inputs::members`, in record order.
    members: Vec<usize>,
}

/// The P3 failure run of every baseline record in a held failure: a
/// maximal stretch of consecutive baseline records with the same
/// `failed_breakout`/`failed_reclaim` state and the same side (ADR-058 D6).
fn failure_runs(baseline: &[Baseline]) -> BTreeMap<u64, u64> {
    let mut runs = BTreeMap::new();
    let mut previous: Option<(&str, &str)> = None;
    let mut run = 0u64;
    for record in baseline {
        let key = match (
            record.auction_state.as_deref(),
            record.auction_side.as_deref(),
        ) {
            (Some(state @ ("failed_breakout" | "failed_reclaim")), Some(side)) => {
                Some((state, side))
            }
            _ => None,
        };
        if let Some(key) = key {
            if previous != Some(key) {
                run += 1;
            }
            runs.insert(record.record, run);
        }
        previous = key;
    }
    runs
}

/// ADR-058 D6 stage 1 for members of a baseline population, in record
/// order: a member joins the open episode when its effort span `[d − w,
/// d − 1 ms]` overlaps the previous member's (it lies less than `w` after
/// it), or when both lie in one failure run (`runs`, P3 only).
fn chain_episodes(decisions: &[i64], span: i64, runs: &[Option<u64>]) -> Vec<u64> {
    let mut episodes = Vec::with_capacity(decisions.len());
    let mut episode = 0u64;
    for i in 0..decisions.len() {
        if i > 0 {
            let overlaps = decisions[i] - decisions[i - 1] < span;
            let same_run = runs[i].is_some() && runs[i] == runs[i - 1];
            if !overlaps && !same_run {
                episode += 1;
            }
        }
        episodes.push(episode);
    }
    episodes
}

/// ADR-058 D6 stage 2: the cell's episodes with a race that ran, and its
/// units. `members` are in decision order with their episode numbers and
/// whether their race at the horizon ran; each episode is represented by
/// its earliest member that ran, and the representatives are thinned
/// greedily at `horizon_ms` (ADR-055 D9).
fn unit_count(members: &[(i64, u64, bool)], horizon_ms: i64) -> (u64, u64) {
    let mut seen = BTreeSet::new();
    let mut episodes = 0;
    let mut units = 0;
    let mut last: Option<i64> = None;
    for &(decision, episode, ran) in members {
        if !ran || !seen.insert(episode) {
            continue;
        }
        episodes += 1;
        if last.is_none_or(|last| decision >= last + horizon_ms) {
            units += 1;
            last = Some(decision);
        }
    }
    (episodes, units)
}

/// The episode number of every member (by index into `Inputs::members`)
/// within its population.
fn member_episodes(inputs: &Inputs, populations: &[Population]) -> Vec<u64> {
    let runs = failure_runs(&inputs.baseline);
    let mut episodes = vec![0u64; inputs.members.len()];
    for population in populations {
        let indices: Vec<usize> = (0..inputs.members.len())
            .filter(|&i| population.holds(&inputs.members[i]))
            .collect();
        if population.trigger_episodes() {
            let mut ids: BTreeMap<&str, u64> = BTreeMap::new();
            for &i in &indices {
                let id = inputs.members[i]
                    .episode
                    .as_deref()
                    .expect("trigger records carry an episode");
                let next = ids.len() as u64;
                episodes[i] = *ids.entry(id).or_insert(next);
            }
        } else {
            let span = WINDOWS[population.window.unwrap()].1;
            let decisions: Vec<i64> = indices
                .iter()
                .map(|&i| inputs.members[i].decision)
                .collect();
            let member_runs: Vec<Option<u64>> = indices
                .iter()
                .map(|&i| {
                    (population.pair == "P3")
                        .then(|| runs.get(&inputs.members[i].record).copied())
                        .flatten()
                })
                .collect();
            for (k, episode) in chain_episodes(&decisions, span, &member_runs)
                .into_iter()
                .enumerate()
            {
                episodes[indices[k]] = episode;
            }
        }
    }
    episodes
}

/// Every cell, in report order: per population, per split, per horizon.
fn cells(inputs: &Inputs, populations: &[Population]) -> Vec<Cell> {
    let mut all = Vec::new();
    for (p, population) in populations.iter().enumerate() {
        let indices: Vec<usize> = (0..inputs.members.len())
            .filter(|&i| population.holds(&inputs.members[i]))
            .collect();
        for split in splits(population) {
            let members: Vec<usize> = indices
                .iter()
                .copied()
                .filter(|&i| split.holds(&inputs.members[i]))
                .collect();
            for horizon in 0..HORIZONS.len() {
                all.push(Cell {
                    population: p,
                    split,
                    horizon,
                    members: members.clone(),
                });
            }
        }
    }
    all
}

/// The `(episodes, units)` of `cell`.
fn cell_units(inputs: &Inputs, episodes: &[u64], cell: &Cell) -> (u64, u64) {
    let members: Vec<(i64, u64, bool)> = cell
        .members
        .iter()
        .map(|&i| {
            let member = &inputs.members[i];
            (
                member.decision,
                episodes[i],
                member.outcomes[cell.horizon].ran(),
            )
        })
        .collect();
    unit_count(&members, HORIZONS[cell.horizon].1)
}

// ---------------------------------------------------------------------------
// n_min (ADR-058 D6).

/// Φ⁻¹(p) by Acklam's rational approximation (relative error below
/// 1.15e-9 over (0, 1)), pinned so that n_min needs no new crate.
fn normal_quantile(p: f64) -> f64 {
    const A: [f64; 6] = [
        -3.969_683_028_665_376e1,
        2.209_460_984_245_205e2,
        -2.759_285_104_469_687e2,
        1.383_577_518_672_69e2,
        -3.066_479_806_614_716e1,
        2.506_628_277_459_239,
    ];
    const B: [f64; 5] = [
        -5.447_609_879_822_406e1,
        1.615_858_368_580_409e2,
        -1.556_989_798_598_866e2,
        6.680_131_188_771_972e1,
        -1.328_068_155_288_572e1,
    ];
    const C: [f64; 6] = [
        -7.784_894_002_430_293e-3,
        -3.223_964_580_411_365e-1,
        -2.400_758_277_161_838,
        -2.549_732_539_343_734,
        4.374_664_141_464_968,
        2.938_163_982_698_783,
    ];
    const D: [f64; 4] = [
        7.784_695_709_041_462e-3,
        3.224_671_290_700_398e-1,
        2.445_134_137_142_996,
        3.754_408_661_907_416,
    ];
    assert!(p > 0.0 && p < 1.0, "p = {p}");
    let low = 0.024_25;
    let tail = |q: f64| {
        (((((C[0] * q + C[1]) * q + C[2]) * q + C[3]) * q + C[4]) * q + C[5])
            / ((((D[0] * q + D[1]) * q + D[2]) * q + D[3]) * q + 1.0)
    };
    if p < low {
        tail((-2.0 * p.ln()).sqrt())
    } else if p > 1.0 - low {
        -tail((-2.0 * (1.0 - p).ln()).sqrt())
    } else {
        let q = p - 0.5;
        let r = q * q;
        (((((A[0] * r + A[1]) * r + A[2]) * r + A[3]) * r + A[4]) * r + A[5]) * q
            / (((((B[0] * r + B[1]) * r + B[2]) * r + B[3]) * r + B[4]) * r + 1.0)
    }
}

/// z = Φ⁻¹((1 + c) / 2).
fn z_of(confidence: f64) -> f64 {
    normal_quantile(f64::midpoint(1.0, confidence))
}

/// n_min = ⌈(z/μ)²⌉: `docs/direction.md` T1 with σ ≤ 1 (ADR-058 D6).
fn n_min(confidence: f64, mu: f64) -> u64 {
    let n = (z_of(confidence) / mu).powi(2).ceil();
    n as u64
}

// ---------------------------------------------------------------------------
// Strata and the bootstrap (ADR-058 D4, D5).

/// The New York hour of week of `ms`, Monday 00:00 = 0 (1970-01-01 was a
/// Thursday, hour 72 of its week).
fn ny_hour_of_week(ms: i64) -> usize {
    (on_clock(ms, true).div_euclid(HOUR_MS) + 72).rem_euclid(168) as usize
}

/// The stratum of a record: New York hour of week × location-bin set.
fn stratum(decision: i64, location: &str) -> usize {
    let bins = LOCATIONS
        .iter()
        .position(|l| *l == location)
        .unwrap_or_else(|| panic!("location {location}"));
    ny_hour_of_week(decision) * LOCATIONS.len() + bins
}

fn regime_index(regime: &str) -> usize {
    REGIMES
        .iter()
        .position(|r| *r == regime)
        .unwrap_or_else(|| panic!("regime {regime}"))
}

/// The index of `(horizon, restriction, stratum)` in a totals array:
/// restriction 0 the whole baseline, 1 + i the regime `REGIMES[i]`.
fn total_index(horizon: usize, restriction: usize, stratum: usize) -> usize {
    (horizon * (1 + REGIMES.len()) + restriction) * STRATA + stratum
}

const TOTALS: usize = HORIZONS.len() * (1 + REGIMES.len()) * STRATA;

/// The baseline per UTC day, sparse: `(total index with a regime
/// restriction, upper − lower, races that ran)`.
struct BaselineDays {
    days: Vec<Vec<(u32, i64, i64)>>,
}

impl BaselineDays {
    fn new(inputs: &Inputs, day_index: &BTreeMap<i64, usize>) -> Self {
        let mut per_day: Vec<BTreeMap<u32, (i64, i64)>> = vec![BTreeMap::new(); day_index.len()];
        for record in &inputs.baseline {
            let day = day_index[&record.decision.div_euclid(DAY_MS)];
            let s = stratum(record.decision, &record.location);
            let r = 1 + regime_index(&record.regime);
            for (h, race) in record.races.iter().enumerate() {
                let diff = match race {
                    StoredRace::Upper => 1,
                    StoredRace::Lower => -1,
                    StoredRace::Neither => 0,
                    StoredRace::Censored | StoredRace::Unavailable => continue,
                };
                let entry = per_day[day].entry(total_index(h, r, s) as u32).or_default();
                entry.0 += diff;
                entry.1 += 1;
            }
        }
        Self {
            days: per_day
                .into_iter()
                .map(|day| day.into_iter().map(|(k, (d, n))| (k, d, n)).collect())
                .collect(),
        }
    }

    /// The baseline totals under day `weights` (all 1 for the point
    /// estimate), the whole-baseline rows summed from the regime rows.
    fn totals(&self, weights: &[u32], totals: &mut Vec<(i64, i64)>) {
        totals.clear();
        totals.resize(TOTALS, (0, 0));
        for (day, entries) in self.days.iter().enumerate() {
            let w = i64::from(weights[day]);
            if w == 0 {
                continue;
            }
            for &(key, diff, n) in entries {
                let total = &mut totals[key as usize];
                total.0 += w * diff;
                total.1 += w * n;
            }
        }
        for h in 0..HORIZONS.len() {
            for s in 0..STRATA {
                let mut all = (0, 0);
                for r in 1..=REGIMES.len() {
                    let total = totals[total_index(h, r, s)];
                    all.0 += total.0;
                    all.1 += total.1;
                }
                totals[total_index(h, 0, s)] = all;
            }
        }
    }
}

/// A cell's ran members aggregated per day and stratum: hypothesis,
/// counter and neither counts, and Σσ (σ = +1 when the hypothesis barrier
/// is the upper one, −1 when it is the lower one).
#[derive(Clone, Debug, Default)]
struct Prepared {
    totals_offset: usize,
    entries: Vec<(u32, u32, i64, i64, i64, i64)>,
}

fn prepare(inputs: &Inputs, cell: &Cell, day_index: &BTreeMap<i64, usize>) -> Prepared {
    let mut map: BTreeMap<(u32, u32), (i64, i64, i64, i64)> = BTreeMap::new();
    for &i in &cell.members {
        let member = &inputs.members[i];
        let outcome = member.outcomes[cell.horizon];
        if !outcome.ran() {
            continue;
        }
        let day = day_index[&member.decision.div_euclid(DAY_MS)] as u32;
        let s = stratum(member.decision, &member.location) as u32;
        let entry = map.entry((day, s)).or_default();
        match outcome {
            Outcome::Hypothesis => entry.0 += 1,
            Outcome::Counter => entry.1 += 1,
            _ => entry.2 += 1,
        }
        entry.3 += if member.upper { 1 } else { -1 };
    }
    Prepared {
        totals_offset: total_index(cell.horizon, cell.split.restriction(), 0),
        entries: map
            .into_iter()
            .map(|((d, s), (h, c, n, sig))| (d, s, h, c, n, sig))
            .collect(),
    }
}

/// A cell's estimate under day `weights`: counts over the kept members
/// (those whose stratum holds a baseline race), the dropped ones, and
/// Σσ·tilt(stratum).
#[derive(Clone, Copy, Debug, Default, PartialEq)]
struct Estimate {
    hypothesis: i64,
    counter: i64,
    neither: i64,
    dropped: i64,
    baseline_sum: f64,
}

impl Estimate {
    fn n(&self) -> i64 {
        self.hypothesis + self.counter + self.neither
    }

    /// E = T − T_b, `None` without a kept race.
    fn effect(&self) -> Option<f64> {
        let n = self.n();
        (n > 0).then(|| {
            let t = (self.hypothesis - self.counter) as f64 / n as f64;
            t - self.baseline_sum / n as f64
        })
    }
}

fn estimate(prepared: &Prepared, weights: &[u32], totals: &[(i64, i64)]) -> Estimate {
    let mut e = Estimate::default();
    for &(day, s, h, c, n, sig) in &prepared.entries {
        let w = i64::from(weights[day as usize]);
        if w == 0 {
            continue;
        }
        let (diff, races) = totals[prepared.totals_offset + s as usize];
        if races == 0 {
            e.dropped += w * (h + c + n);
            continue;
        }
        e.hypothesis += w * h;
        e.counter += w * c;
        e.neither += w * n;
        e.baseline_sum += (w * sig) as f64 * diff as f64 / races as f64;
    }
    e
}

/// The settings of one study run.
#[derive(Clone, Copy, Debug)]
struct Settings {
    confidence: f64,
    mu: f64,
    fee_bps: f64,
    resamples: usize,
    blocks: [usize; 2],
    seed: u64,
    threads: usize,
}

/// The day weights of every resample of one block length: a circular block
/// resample of the study's days from one SplitMix64 stream (D5).
fn day_weights(days: usize, block: usize, settings: &Settings) -> Vec<Vec<u32>> {
    let mut rng = SplitMix64(settings.seed);
    (0..settings.resamples)
        .map(|_| {
            let mut weights = vec![0u32; days];
            resample(&mut rng, days, block, |day| weights[day] += 1);
            weights
        })
        .collect()
}

/// Every cell's E under every resample (`None` when undefined), resamples
/// split over `threads` scoped threads; the result does not depend on the
/// thread count.
fn bootstrap(
    baseline: &BaselineDays,
    prepared: &[Prepared],
    weights: &[Vec<u32>],
    threads: usize,
) -> Vec<Vec<Option<f64>>> {
    let chunk = weights.len().div_ceil(threads.max(1)).max(1);
    let parts: Vec<Vec<Vec<Option<f64>>>> = std::thread::scope(|scope| {
        let handles: Vec<_> = weights
            .chunks(chunk)
            .map(|weights| {
                scope.spawn(move || {
                    let mut totals = Vec::new();
                    let mut out = vec![Vec::with_capacity(weights.len()); prepared.len()];
                    for w in weights {
                        baseline.totals(w, &mut totals);
                        for (c, cell) in prepared.iter().enumerate() {
                            out[c].push(estimate(cell, w, &totals).effect());
                        }
                    }
                    out
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    let mut all = vec![Vec::with_capacity(weights.len()); prepared.len()];
    for part in parts {
        for (c, values) in part.into_iter().enumerate() {
            all[c].extend(values);
        }
    }
    all
}

/// The nearest-rank quantile `q` of the sorted `values`: the
/// ⌈q·n⌉-th, at least the first.
fn rank(sorted: &[f64], q: f64) -> f64 {
    let k = ((q * sorted.len() as f64).ceil() as usize).max(1);
    sorted[k - 1]
}

/// A reading (ADR-058 D7).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reading {
    Hypothesis,
    Counter,
    Inconclusive,
    Insufficient,
}

impl Reading {
    fn name(self) -> &'static str {
        match self {
            Self::Hypothesis => "hypothesis",
            Self::Counter => "counter",
            Self::Inconclusive => "inconclusive",
            Self::Insufficient => "insufficient",
        }
    }
}

/// The interval of E from one block length's resamples, and the reading.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Interval {
    low: f64,
    high: f64,
    undefined: usize,
    reading: Reading,
}

fn interval(values: &[Option<f64>], units: u64, n_min: u64, confidence: f64) -> Interval {
    let mut defined: Vec<f64> = values.iter().filter_map(|v| *v).collect();
    defined.sort_by(f64::total_cmp);
    let undefined = values.len() - defined.len();
    let tail = (1.0 - confidence) / 2.0;
    let too_many = undefined as f64 > tail * values.len() as f64;
    let (low, high) = if defined.is_empty() {
        (f64::NAN, f64::NAN)
    } else {
        (rank(&defined, tail), rank(&defined, 1.0 - tail))
    };
    let reading = if units < n_min || too_many || defined.is_empty() {
        Reading::Insufficient
    } else if low > 0.0 {
        Reading::Hypothesis
    } else if high < 0.0 {
        Reading::Counter
    } else {
        Reading::Inconclusive
    };
    Interval {
        low,
        high,
        undefined,
        reading,
    }
}

/// One cell's result.
#[derive(Clone, Debug)]
struct CellResult {
    cell: Cell,
    censored: u64,
    unavailable: u64,
    episodes: u64,
    units: u64,
    point: Estimate,
    effect: Option<f64>,
    primary: Interval,
    sensitivity: Interval,
}

/// The study: every cell's result.
fn study(inputs: &Inputs, settings: &Settings) -> (Vec<Population>, Vec<CellResult>) {
    let populations = populations();
    let episodes = member_episodes(inputs, &populations);
    let cells = cells(inputs, &populations);
    let days: BTreeSet<i64> = inputs
        .baseline
        .iter()
        .map(|b| b.decision)
        .chain(inputs.members.iter().map(|m| m.decision))
        .map(|ms| ms.div_euclid(DAY_MS))
        .collect();
    let day_index: BTreeMap<i64, usize> = days.iter().enumerate().map(|(i, d)| (*d, i)).collect();
    let baseline = BaselineDays::new(inputs, &day_index);
    let prepared: Vec<Prepared> = cells
        .iter()
        .map(|cell| prepare(inputs, cell, &day_index))
        .collect();
    let ones = vec![1u32; day_index.len()];
    let mut totals = Vec::new();
    baseline.totals(&ones, &mut totals);
    let n_min = n_min(settings.confidence, settings.mu);
    let resampled: Vec<Vec<Vec<Option<f64>>>> = settings
        .blocks
        .iter()
        .map(|block| {
            let weights = day_weights(day_index.len().max(1), *block, settings);
            bootstrap(&baseline, &prepared, &weights, settings.threads)
        })
        .collect();
    let results = cells
        .into_iter()
        .enumerate()
        .map(|(c, cell)| {
            let (episodes, units) = cell_units(inputs, &episodes, &cell);
            let point = estimate(&prepared[c], &ones, &totals);
            let count = |outcome: Outcome| {
                cell.members
                    .iter()
                    .filter(|&&i| inputs.members[i].outcomes[cell.horizon] == outcome)
                    .count() as u64
            };
            CellResult {
                censored: count(Outcome::Censored),
                unavailable: count(Outcome::Unavailable),
                episodes,
                units,
                point,
                effect: point.effect(),
                primary: interval(&resampled[0][c], units, n_min, settings.confidence),
                sensitivity: interval(&resampled[1][c], units, n_min, settings.confidence),
                cell,
            }
        })
        .collect();
    (populations, results)
}

// ---------------------------------------------------------------------------
// The report (ADR-058 D9).

fn fixed(value: Option<f64>) -> String {
    value.map_or_else(|| "–".to_owned(), |v| format!("{v:.4}"))
}

fn share(count: i64, n: i64) -> String {
    fixed((n > 0).then(|| count as f64 / n as f64))
}

/// The table row of one cell.
fn row(result: &CellResult) -> String {
    let p = &result.point;
    let n = p.n();
    let t = (n > 0).then(|| (p.hypothesis - p.counter) as f64 / n as f64);
    let tb = (n > 0).then(|| p.baseline_sum / n as f64);
    let interval = if result.primary.low.is_nan() {
        "–".to_owned()
    } else {
        format!("[{:.4}, {:.4}]", result.primary.low, result.primary.high)
    };
    let mut flags = Vec::new();
    if result.primary.reading != result.sensitivity.reading {
        flags.push(format!(
            "block-sensitive (28d: {})",
            result.sensitivity.reading.name()
        ));
    }
    if result.primary.undefined > 0 {
        flags.push(format!("undefined {}", result.primary.undefined));
    }
    format!(
        "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |",
        result.cell.split.value(),
        HORIZONS[result.cell.horizon].0,
        n,
        result.censored,
        result.unavailable,
        p.dropped,
        result.episodes,
        result.units,
        share(p.hypothesis, n),
        share(p.counter, n),
        share(p.neither, n),
        fixed(t),
        fixed(tb),
        fixed(result.effect),
        interval,
        if flags.is_empty() {
            result.primary.reading.name().to_owned()
        } else {
            format!("{} ({})", result.primary.reading.name(), flags.join(", "))
        },
    )
}

const TABLE_HEAD: &str = "| split | H | n | censored | unavailable | dropped | episodes | units | P_h | P_c | P_n | T | T_b | E | interval | reading |\n\
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|--:|---|---|";

/// The markdown report of a study run.
fn report(
    inputs: &Inputs,
    settings: &Settings,
    populations: &[Population],
    results: &[CellResult],
) -> String {
    let mut out = String::new();
    let n_min = n_min(settings.confidence, settings.mu);
    let days: BTreeSet<i64> = inputs
        .baseline
        .iter()
        .map(|b| b.decision.div_euclid(DAY_MS))
        .collect();
    let _ = writeln!(out, "## Inputs\n");
    let _ = writeln!(
        out,
        "c = {}, μ = {} R, z = {:.6}, n_min = {n_min}; fee {} bps round trip; \
         {} resamples, blocks {} and {} days, seed {}; {} baseline days.\n",
        settings.confidence,
        settings.mu,
        z_of(settings.confidence),
        settings.fee_bps,
        settings.resamples,
        settings.blocks[0],
        settings.blocks[1],
        settings.seed,
        days.len()
    );
    let _ = writeln!(out, "## Horizons\n");
    let _ = writeln!(
        out,
        "| H | baselines with barriers | median half-width (bps) | fee (R) | disjoint windows in the days read |\n|---|--:|--:|--:|--:|"
    );
    for (horizon, ms) in HORIZONS {
        let (barriers, median) = inputs
            .barriers
            .iter()
            .find(|(name, _, _)| name == horizon)
            .map_or((0, f64::NAN), |(_, n, m)| (*n, *m));
        let _ = writeln!(
            out,
            "| {horizon} | {barriers} | {median:.2} | {:.2} | {} |",
            settings.fee_bps / median,
            days.len() as i64 * DAY_MS / ms
        );
    }
    let _ = writeln!(out, "\n## Exclusions before any race is read\n");
    let _ = writeln!(
        out,
        "| universe | window | trigger | records | effort not ready | incomplete coverage | no failed side |\n|---|---|---|--:|--:|--:|--:|"
    );
    for e in &inputs.exclusions {
        let _ = writeln!(
            out,
            "| {} | {} | {} | {} | {} | {} | {} |",
            e.universe,
            e.window.as_deref().unwrap_or("–"),
            e.trigger.as_deref().unwrap_or("–"),
            e.records,
            e.not_ready,
            e.incomplete,
            e.no_side
        );
    }
    for (p, population) in populations.iter().enumerate() {
        let _ = writeln!(out, "\n## {}\n", population.label());
        let mut dimension = "";
        for result in results.iter().filter(|r| r.cell.population == p) {
            let this = result.cell.split.dimension();
            if this != dimension {
                dimension = this;
                let title = if this == "all" {
                    "primary".to_owned()
                } else {
                    format!("by {this}")
                };
                let _ = writeln!(out, "\n### {title}\n\n{TABLE_HEAD}");
            }
            let _ = writeln!(out, "{}", row(result));
        }
    }
    let _ = writeln!(
        out,
        "\n## P4 SFP cells: extreme_vs_opposite\n\n| trigger | H | value | records |\n|---|---|---|--:|"
    );
    for (trigger, horizon, value, records) in &inputs.sfp_extreme {
        let _ = writeln!(out, "| {trigger} | {horizon} | {value} | {records} |");
    }
    let _ = writeln!(
        out,
        "\n## Multiplicity\n\n| family | cells | read (N) | intervals excluding 0 (K) | expected by chance (1 − c)·N |\n|---|---|--:|--:|--:|"
    );
    for pair in ["P1-buy", "P1-sell", "P2", "P3", "P4", "P1-trig"] {
        for primary in [true, false] {
            let read: Vec<&CellResult> = results
                .iter()
                .filter(|r| {
                    populations[r.cell.population].pair == pair
                        && (r.cell.split == Split::All) == primary
                        && r.primary.reading != Reading::Insufficient
                })
                .collect();
            let excluding = read
                .iter()
                .filter(|r| matches!(r.primary.reading, Reading::Hypothesis | Reading::Counter))
                .count();
            let _ = writeln!(
                out,
                "| {pair} | {} | {} | {excluding} | {:.1} |",
                if primary { "primary" } else { "secondary" },
                read.len(),
                (1.0 - settings.confidence) * read.len() as f64
            );
        }
    }
    out
}

// ---------------------------------------------------------------------------
// The twin (ADR-058 D3, D6), written from the ADR text: the study views and
// every cell's units from the base views, without the study SQL or the
// tool's unit code.

/// An effort row as stored, prices and quantities in units.
#[derive(Clone, Debug, Default)]
struct TEffort {
    state: String,
    aggressor: Option<String>,
    effort: Option<i64>,
    effort_high: Option<i64>,
    progress: Option<i64>,
    move_median: Option<i64>,
    move_high: Option<i64>,
    partial_start: Option<bool>,
    feed_gap: Option<bool>,
}

impl TEffort {
    /// Ready with complete coverage.
    fn covered(&self) -> bool {
        self.state == "ready" && self.partial_start == Some(false) && self.feed_gap == Some(false)
    }

    fn high_effort(&self) -> bool {
        self.effort.unwrap() >= self.effort_high.unwrap()
    }
}

/// A context record with what the study reads of it.
#[derive(Clone, Debug, Default)]
struct TRecord {
    record: u64,
    baseline: bool,
    trigger: Option<String>,
    episode: Option<String>,
    decision: i64,
    regime: String,
    location: String,
    auction_state: Option<String>,
    auction_side: Option<String>,
    cvd: Option<i64>,
    cvd_complete: bool,
    effort: Vec<TEffort>,
    races: Vec<String>,
    level: Option<i64>,
    extreme: Option<i64>,
}

/// A printed decimal of scale 8 in units.
fn units_of(cell: &str) -> i64 {
    let (negative, digits) = cell.strip_prefix('-').map_or((false, cell), |d| (true, d));
    let (whole, fraction) = digits.split_once('.').unwrap();
    assert_eq!(fraction.len(), 8, "{cell}");
    let value = whole.parse::<i64>().unwrap() * 100_000_000 + fraction.parse::<i64>().unwrap();
    if negative { -value } else { value }
}

fn opt_units(cell: String) -> Option<i64> {
    opt(cell).map(|c| units_of(&c))
}

fn opt_flag(cell: String) -> Option<bool> {
    opt(cell).map(|c| c.parse().unwrap())
}

/// The records from the base views, those in an `exclude` range dropped.
fn twin_records(views: &OutcomeViews, exclude: &[(i64, i64)]) -> Vec<TRecord> {
    let mut records: Vec<TRecord> = Vec::new();
    each_selected(
        views,
        "SELECT record, sample, trigger, episode, decision_time, regime, levels_ready, \
         at_value_edge, at_structural_extreme, auction_state, auction_side, cvd_utc_day, \
         cvd_partial_start, cvd_feed_gap FROM contexts ORDER BY record",
        |get| {
            let ready: bool = get("levels_ready").parse().unwrap();
            let edge = get("at_value_edge") == "true";
            let structural = get("at_structural_extreme") == "true";
            let location = match (ready, edge, structural) {
                (false, _, _) => "not_ready",
                (true, true, true) => "both",
                (true, true, false) => "value_edge",
                (true, false, true) => "structural_extreme",
                (true, false, false) => "neither",
            };
            let cvd = opt_units(get("cvd_utc_day"));
            records.push(TRecord {
                record: get("record").parse().unwrap(),
                baseline: get("sample") == "baseline",
                trigger: opt(get("trigger")),
                episode: opt(get("episode")),
                decision: get("decision_time").parse().unwrap(),
                regime: opt(get("regime")).unwrap_or_else(|| "not_ready".to_owned()),
                location: location.to_owned(),
                auction_state: opt(get("auction_state")),
                auction_side: opt(get("auction_side")),
                cvd,
                cvd_complete: cvd.is_some()
                    && opt_flag(get("cvd_partial_start")) == Some(false)
                    && opt_flag(get("cvd_feed_gap")) == Some(false),
                ..TRecord::default()
            });
        },
    );
    each_selected(
        views,
        "SELECT record, \"window\", state, aggressor, effort, effort_high, progress, \
         move_median, move_high, partial_start, feed_gap FROM effort ORDER BY record",
        |get| {
            let record: usize = get("record").parse().unwrap();
            let window = get("window");
            let position = WINDOWS.iter().position(|(w, _)| *w == window).unwrap();
            let target = &mut records[record].effort;
            if target.len() < WINDOWS.len() {
                target.resize(WINDOWS.len(), TEffort::default());
            }
            target[position] = TEffort {
                state: get("state"),
                aggressor: opt(get("aggressor")),
                effort: opt_units(get("effort")),
                effort_high: opt_units(get("effort_high")),
                progress: opt_units(get("progress")),
                move_median: opt_units(get("move_median")),
                move_high: opt_units(get("move_high")),
                partial_start: opt_flag(get("partial_start")),
                feed_gap: opt_flag(get("feed_gap")),
            };
        },
    );
    each_selected(
        views,
        "SELECT record, race FROM outcomes ORDER BY record, horizon_ms",
        |get| {
            let record: usize = get("record").parse().unwrap();
            records[record].races.push(get("race"));
        },
    );
    each_selected(
        views,
        "SELECT record, target, price FROM passages ORDER BY record, target",
        |get| {
            let record: usize = get("record").parse().unwrap();
            let price = units_of(&get("price"));
            match get("target").as_str() {
                "level" => records[record].level = Some(price),
                _ => records[record].extreme = Some(price),
            }
        },
    );
    records
        .into_iter()
        .filter(|r| {
            !exclude
                .iter()
                .any(|(from, to)| (*from..*to).contains(&r.decision))
        })
        .collect()
}

/// A member as the twin derives it from the ADR text.
#[derive(Clone, Debug)]
struct TMember {
    pair: &'static str,
    window: Option<usize>,
    trigger: Option<String>,
    record: usize,
    side: String,
    hypothesis: &'static str,
}

/// The barrier a side's aggression or failure points to.
fn toward(side: &str) -> &'static str {
    match side {
        "buy" | "above" => "upper",
        _ => "lower",
    }
}

fn away(side: &str) -> &'static str {
    match toward(side) {
        "upper" => "lower",
        _ => "upper",
    }
}

/// The populations of ADR-058 D3 over `records` (indices into it).
fn twin_members(records: &[TRecord]) -> Vec<TMember> {
    let mut members = Vec::new();
    for (i, r) in records.iter().enumerate() {
        if r.baseline {
            for (w, effort) in r.effort.iter().enumerate() {
                if !effort.covered() || !effort.high_effort() {
                    continue;
                }
                let aggressor = effort.aggressor.clone().unwrap();
                let progress = effort.progress.unwrap();
                // P1 (absorption): progress at most the median move; the
                // hypothesis is the barrier away from the aggressor.
                if progress <= effort.move_median.unwrap() {
                    members.push(TMember {
                        pair: if aggressor == "buy" {
                            "P1-buy"
                        } else {
                            "P1-sell"
                        },
                        window: Some(w),
                        trigger: None,
                        record: i,
                        side: aggressor.clone(),
                        hypothesis: away(&aggressor),
                    });
                }
                // P2 (continuation): progress at least the high move.
                if progress >= effort.move_high.unwrap() {
                    members.push(TMember {
                        pair: "P2",
                        window: Some(w),
                        trigger: None,
                        record: i,
                        side: aggressor.clone(),
                        hypothesis: toward(&aggressor),
                    });
                }
                // P3 (no acceptance): aggression in the probe's direction
                // during a held failure; the probe went toward the failure's
                // side for failed_breakout and away from it for
                // failed_reclaim (ADR-044 decision 8).
                let probe = match (r.auction_state.as_deref(), r.auction_side.as_deref()) {
                    (Some("failed_breakout"), Some(side @ ("above" | "below"))) => {
                        Some(toward(side))
                    }
                    (Some("failed_reclaim"), Some(side @ ("above" | "below"))) => Some(away(side)),
                    _ => None,
                };
                if probe == Some(toward(&aggressor)) {
                    members.push(TMember {
                        pair: "P3",
                        window: Some(w),
                        trigger: None,
                        record: i,
                        side: aggressor.clone(),
                        hypothesis: away(&aggressor),
                    });
                }
            }
            continue;
        }
        let trigger = r.trigger.clone().unwrap();
        if trigger == "trigger.failed_auction@1" {
            if let Some(side @ ("above" | "below")) = r.auction_side.as_deref() {
                members.push(TMember {
                    pair: "P4",
                    window: None,
                    trigger: Some(trigger.clone()),
                    record: i,
                    side: side.to_owned(),
                    hypothesis: away(side),
                });
            }
        } else if P4_TRIGGERS.contains(&trigger.as_str())
            && let (Some(level), Some(extreme)) = (r.level, r.extreme)
            && level != extreme
        {
            let side = if extreme > level { "above" } else { "below" };
            members.push(TMember {
                pair: "P4",
                window: None,
                trigger: Some(trigger.clone()),
                record: i,
                side: side.to_owned(),
                hypothesis: away(side),
            });
        }
        for (w, (window, _)) in WINDOWS.iter().enumerate() {
            if trigger == format!("trigger.effort_result.{window}@1") && r.effort[w].covered() {
                let aggressor = r.effort[w].aggressor.clone().unwrap();
                members.push(TMember {
                    pair: "P1-trig",
                    window: Some(w),
                    trigger: Some(trigger.clone()),
                    record: i,
                    hypothesis: away(&aggressor),
                    side: aggressor,
                });
            }
        }
    }
    members.sort_by(|a, b| {
        (a.pair, a.window, &a.trigger, a.record).cmp(&(b.pair, b.window, &b.trigger, b.record))
    });
    members
}

fn cell_text(value: Option<&str>) -> &str {
    value.unwrap_or("NULL")
}

/// The CVD agreement of P1 and P2 members (ADR-058 D8).
fn twin_cvd(record: &TRecord, side: &str) -> &'static str {
    match record.cvd {
        Some(cvd) if record.cvd_complete => match cvd.signum() {
            0 => "flat",
            sign if (sign > 0) == (side == "buy") => "with",
            _ => "against",
        },
        _ => "unknown",
    }
}

fn twin_outcome(race: &str, hypothesis: &str) -> &'static str {
    match race {
        "upper" | "lower" if race == hypothesis => "hypothesis",
        "upper" | "lower" => "counter",
        "neither" => "neither",
        "censored" => "censored",
        _ => "unavailable",
    }
}

/// The study views' lines as the twin derives them.
fn twin_lines(records: &[TRecord], members: &[TMember]) -> Lines {
    let mut lines = Lines::default();
    lines.baseline.push(
        "record\tdecision_ms\tregime\tlocation\tauction_state\tauction_side\t\
         race_5m\trace_15m\trace_1h\trace_4h\trace_1d"
            .to_owned(),
    );
    for r in records.iter().filter(|r| r.baseline) {
        lines.baseline.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}",
            r.record,
            r.decision,
            r.regime,
            r.location,
            cell_text(r.auction_state.as_deref()),
            cell_text(r.auction_side.as_deref()),
            r.races.join("\t")
        ));
    }
    lines.members.push(
        "pair\teffort_window\ttrigger\trecord\tdecision_ms\tside\thypothesis\tregime\t\
         location\tepisode\tcvd\tout_5m\tout_15m\tout_1h\tout_4h\tout_1d"
            .to_owned(),
    );
    for m in members {
        let r = &records[m.record];
        let cvd = matches!(m.pair, "P1-buy" | "P1-sell" | "P2").then(|| twin_cvd(r, &m.side));
        let outcomes: Vec<&str> = r
            .races
            .iter()
            .map(|race| twin_outcome(race, m.hypothesis))
            .collect();
        lines.members.push(format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}",
            m.pair,
            cell_text(m.window.map(|w| WINDOWS[w].0)),
            cell_text(m.trigger.as_deref()),
            r.record,
            r.decision,
            m.side,
            m.hypothesis,
            r.regime,
            r.location,
            cell_text(r.episode.as_deref()),
            cell_text(cvd),
            outcomes.join("\t")
        ));
    }
    lines.exclusions.push(
        "universe\teffort_window\twindow_order\ttrigger\trecords\tnot_ready\tincomplete\tno_side"
            .to_owned(),
    );
    // Key: universe, window order, trigger ('' sorts first, as NULL does).
    let mut rows: BTreeMap<(String, usize, String), [u64; 4]> = BTreeMap::new();
    for r in records {
        for (w, (window, _)) in WINDOWS.iter().enumerate() {
            let universe = if r.baseline {
                Some(("baseline".to_owned(), String::new()))
            } else {
                r.trigger
                    .as_ref()
                    .filter(|t| **t == format!("trigger.effort_result.{window}@1"))
                    .map(|t| ("P1-trig".to_owned(), t.clone()))
            };
            if let Some((universe, trigger)) = universe {
                let e = &r.effort[w];
                let row = rows.entry((universe, w + 1, trigger)).or_default();
                row[0] += 1;
                row[1] += u64::from(e.state != "ready");
                row[2] += u64::from(e.state == "ready" && !e.covered());
            }
        }
        if let Some(trigger) = r
            .trigger
            .as_ref()
            .filter(|t| P4_TRIGGERS.contains(&t.as_str()))
        {
            let row = rows
                .entry(("P4".to_owned(), 0, trigger.clone()))
                .or_default();
            row[0] += 1;
        }
    }
    for m in members.iter().filter(|m| m.pair == "P4") {
        let trigger = m.trigger.clone().unwrap();
        rows.get_mut(&("P4".to_owned(), 0, trigger)).unwrap()[3] += 1;
    }
    for ((universe, order, trigger), counts) in rows {
        let (window, no_side) = if universe == "P4" {
            ("NULL", counts[0] - counts[3])
        } else {
            (WINDOWS[order - 1].0, 0)
        };
        lines.exclusions.push(format!(
            "{universe}\t{window}\t{order}\t{}\t{}\t{}\t{}\t{no_side}",
            if trigger.is_empty() { "NULL" } else { &trigger },
            counts[0],
            counts[1],
            counts[2]
        ));
    }
    lines
}

/// The twin's units of every cell, keyed by population label, split and
/// horizon: episodes as the hull of the members' effort spans (ADR-054 D4's
/// join rule) or the trigger's episode id, P3's held failures merged; the
/// representative of an episode its earliest member that ran in the cell;
/// then the greedy thinning.
fn twin_units(
    records: &[TRecord],
    members: &[TMember],
) -> BTreeMap<(String, String, String, &'static str), (u64, u64)> {
    // The failure run of each baseline record in a held failure.
    let mut runs: BTreeMap<usize, u64> = BTreeMap::new();
    let mut run = 0;
    let mut previous: Option<(String, String)> = None;
    for (i, r) in records.iter().enumerate().filter(|(_, r)| r.baseline) {
        let key = match (&r.auction_state, &r.auction_side) {
            (Some(state), Some(side)) if state.starts_with("failed_") => {
                Some((state.clone(), side.clone()))
            }
            _ => None,
        };
        if key.is_some() {
            if key != previous {
                run += 1;
            }
            runs.insert(i, run);
        }
        previous = key;
    }
    let mut out = BTreeMap::new();
    for population in populations() {
        let mine: Vec<&TMember> = members
            .iter()
            .filter(|m| {
                m.pair == population.pair
                    && m.window == population.window
                    && m.trigger == population.trigger
            })
            .collect();
        // Episode of each member: an index into `hulls`.
        let mut episode = Vec::with_capacity(mine.len());
        if population.trigger_episodes() {
            let mut ids: Vec<&str> = Vec::new();
            for m in &mine {
                let id = records[m.record].episode.as_deref().unwrap();
                let k = ids.iter().position(|x| *x == id).unwrap_or_else(|| {
                    ids.push(id);
                    ids.len() - 1
                });
                episode.push(k);
            }
        } else {
            let w = WINDOWS[population.window.unwrap()].1;
            // The open episode: its data hull and its failure runs.
            let mut hull: Option<(i64, i64, BTreeSet<u64>)> = None;
            let mut count = 0;
            for m in &mine {
                let d = records[m.record].decision;
                let (first, last) = (d - w, d - 1);
                let run = if population.pair == "P3" {
                    runs.get(&m.record).copied()
                } else {
                    None
                };
                let joins = hull
                    .as_ref()
                    .is_some_and(|(open_first, open_last, open_runs)| {
                        (first <= *open_last && *open_first <= last)
                            || run.is_some_and(|run| open_runs.contains(&run))
                    });
                if joins {
                    let (open_first, open_last, open_runs) = hull.as_mut().unwrap();
                    *open_first = (*open_first).min(first);
                    *open_last = (*open_last).max(last);
                    open_runs.extend(run);
                } else {
                    count += 1;
                    hull = Some((first, last, run.into_iter().collect()));
                }
                episode.push(count);
            }
        }
        for split in splits(&population) {
            for (h, (horizon, horizon_ms)) in HORIZONS.iter().enumerate() {
                // Earliest ran member of each episode in the cell.
                let mut first: BTreeMap<usize, i64> = BTreeMap::new();
                for (k, m) in mine.iter().enumerate() {
                    let r = &records[m.record];
                    let in_cell = match split {
                        Split::All => true,
                        Split::Regime(v) => r.regime == v,
                        Split::Location(v) => {
                            r.location == v
                                || (r.location == "both"
                                    && (v == "value_edge" || v == "structural_extreme"))
                        }
                        Split::Side(v) => m.side == v,
                        Split::Cvd(v) => twin_cvd(r, &m.side) == v,
                    };
                    let ran = matches!(r.races[h].as_str(), "upper" | "lower" | "neither");
                    if in_cell && ran {
                        let entry = first.entry(episode[k]).or_insert(r.decision);
                        *entry = (*entry).min(r.decision);
                    }
                }
                let mut representatives: Vec<i64> = first.values().copied().collect();
                representatives.sort_unstable();
                let mut units = 0;
                let mut next = i64::MIN;
                for d in &representatives {
                    if *d >= next {
                        units += 1;
                        next = d + horizon_ms;
                    }
                }
                out.insert(
                    (
                        population.label(),
                        split.dimension().to_owned(),
                        split.value().to_owned(),
                        *horizon,
                    ),
                    (representatives.len() as u64, units),
                );
            }
        }
    }
    out
}

/// Mismatches between the study views and the twin, and between the tool's
/// and the twin's units; prints the first few unless `quiet`.
fn twin_mismatches(
    views: &OutcomeViews,
    exclude: &[(i64, i64)],
    inputs: &Inputs,
    lines: &Lines,
    quiet: bool,
) -> u64 {
    let records = twin_records(views, exclude);
    let members = twin_members(&records);
    let twin = twin_lines(&records, &members);
    let mut count = 0u64;
    let mut report = |what: &str, got: &str, want: &str| {
        count += 1;
        if !quiet && count <= 5 {
            println!("{what} mismatch:\n  study {got}\n  twin  {want}");
        }
    };
    for (what, got, want) in [
        ("study_baseline", &lines.baseline, &twin.baseline),
        ("study_members", &lines.members, &twin.members),
        ("study_exclusions", &lines.exclusions, &twin.exclusions),
    ] {
        for index in 0..got.len().max(want.len()) {
            let g = got.get(index).map_or("(missing)", String::as_str);
            let w = want.get(index).map_or("(missing)", String::as_str);
            if g != w {
                report(what, g, w);
            }
        }
    }
    let twin_units = twin_units(&records, &members);
    let populations = populations();
    let episodes = member_episodes(inputs, &populations);
    let cells = cells(inputs, &populations);
    assert_eq!(cells.len(), twin_units.len(), "the cells");
    for cell in &cells {
        let key = (
            populations[cell.population].label(),
            cell.split.dimension().to_owned(),
            cell.split.value().to_owned(),
            HORIZONS[cell.horizon].0,
        );
        let tool = cell_units(inputs, &episodes, cell);
        let twin = twin_units[&key];
        if tool != twin {
            report(
                "units",
                &format!("{key:?} {tool:?}"),
                &format!("{key:?} {twin:?}"),
            );
        }
    }
    count
}

// ---------------------------------------------------------------------------
// Fixture datasets.

/// 2026-01-05 00:00 UTC, a Monday: the fixtures' first day.
const T0: i64 = 1_767_571_200_000;
/// The fixtures' reference price, 85 000 USDT in units.
const R: i64 = 8_500_000_000_000;
/// One USDT in price units (ADR-027).
const USDT: i64 = 100_000_000;
/// The fixtures' effort yardsticks: `effort_high` in quantity units and the
/// median and high moves in price units.
const EFFORT_HIGH: i64 = 500;
const MOVE_MEDIAN: i64 = 10 * USDT;
const MOVE_HIGH: i64 = 60 * USDT;

/// A fixture race.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum R5 {
    Up,
    Down,
    Neither,
    Censored,
    Unavailable,
}

/// One effort window of a fixture record.
#[derive(Clone, Copy, Debug)]
enum FlowSpec {
    Warming,
    Unavailable,
    Ready {
        aggressor: Aggressor,
        effort: i64,
        progress: i64,
        partial_start: bool,
        feed_gap: bool,
    },
}

/// What a fixture record samples.
#[derive(Clone, Copy, Debug)]
enum RecKind {
    Baseline,
    /// A trigger of `definition`, in the episode its record `opener`
    /// (decision, seq) opened.
    Trigger {
        definition: &'static FeatureDefinition,
        family: TriggerFamily,
        opener: (i64, u64),
    },
}

/// A fixture record and its label.
#[derive(Clone, Debug)]
struct Rec {
    decision: i64,
    seq: u64,
    kind: RecKind,
    regime: Option<f64>,
    levels: Option<&'static [LevelKind]>,
    effort: [FlowSpec; 3],
    cvd: Option<(i64, bool, bool)>,
    auction: Option<(AuctionState, Option<Region>)>,
    races: [R5; 5],
    /// The level and extreme passage prices of a trigger.
    passages: Option<(i64, i64)>,
}

impl Rec {
    fn baseline(decision: i64, seq: u64) -> Self {
        Self {
            decision,
            seq,
            kind: RecKind::Baseline,
            regime: Some(10.0),
            levels: Some(&[]),
            effort: [FlowSpec::Warming; 3],
            cvd: None,
            auction: None,
            races: [R5::Neither; 5],
            passages: None,
        }
    }

    fn trigger(
        decision: i64,
        seq: u64,
        id: &'static FeatureDefinition,
        opener: (i64, u64),
    ) -> Self {
        let family = if id.key.to_string().starts_with("trigger.sfp.") {
            TriggerFamily::Sfp
        } else if id.key.to_string().starts_with("trigger.effort_result.") {
            TriggerFamily::EffortVsResult
        } else {
            TriggerFamily::FailedAuction
        };
        Self {
            kind: RecKind::Trigger {
                definition: id,
                family,
                opener,
            },
            ..Self::baseline(decision, seq)
        }
    }

    fn key(&self) -> CanonicalKey {
        CanonicalKey {
            time: EventTime::from_millis(self.decision),
            kind: EventKind::Trade,
            seq: self.seq,
        }
    }

    fn is_opener(&self) -> bool {
        match self.kind {
            RecKind::Baseline => false,
            RecKind::Trigger { opener, .. } => opener == (self.decision, self.seq),
        }
    }
}

fn ready_flow(aggressor: Aggressor, effort: i64, progress: i64) -> FlowSpec {
    FlowSpec::Ready {
        aggressor,
        effort,
        progress,
        partial_start: false,
        feed_gap: false,
    }
}

fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

fn bias(percentile: Option<f64>) -> Bias {
    static FRESH: std::sync::OnceLock<Bias> = std::sync::OnceLock::new();
    let mut bias = *FRESH.get_or_init(|| Bias::read(MarketStateEngine::new().state()));
    bias.regime = percentile.map_or_else(warming, |percentile| {
        FeatureValue::Ready(Regime::new(
            AtrPercentile::new(percentile).unwrap(),
            catalog::VOLATILITY_REGIME_1H_V1.key,
        ))
    });
    bias
}

fn no_gaps() -> Coverage {
    Coverage {
        partial_start: false,
        feed_gap: false,
    }
}

fn context_record(rec: &Rec) -> ContextRecord {
    let key = rec.key();
    let time = key.time;
    let context = LocationContext {
        anchor: Price::from_units(R),
        levels: rec.levels.map_or_else(warming, |kinds| {
            FeatureValue::Ready(ContextLevels {
                feature: catalog::LOCATION_LEVELS_V2.key,
                levels: kinds
                    .iter()
                    .map(|kind| ContextLevel {
                        kind: *kind,
                        source: catalog::LOCATION_LEVELS_V2.key,
                        side: None,
                        anchor: None,
                        price: Price::from_units(R),
                        low: Price::from_units(R - 10),
                        high: Price::from_units(R + 10),
                    })
                    .collect(),
            })
        }),
        auction: rec.auction.map_or_else(warming, |(state, side)| {
            FeatureValue::Ready(AuctionStatus {
                feature: catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key,
                reference_start: EventTime::from_millis(time.as_millis() - DAY_MS),
                val: Price::from_units(R - 500 * USDT),
                vah: Price::from_units(R + 500 * USDT),
                state,
                side,
                accepted: Region::In,
                origin: Origin::Start,
                position: Position::In,
                probe: None,
                failure: None,
                last_close: Price::from_units(R),
                bar_end: time,
                known_at: time,
                coverage: no_gaps(),
            })
        }),
    };
    let effort = std::array::from_fn(|w| {
        let (timeframe, definition) = catalog::CANDIDATE_EFFORT_RESULT[w];
        match rec.effort[w] {
            FlowSpec::Warming => warming(),
            FlowSpec::Unavailable => FeatureValue::Unavailable {
                reason: Unavailability::InputInvalid,
            },
            FlowSpec::Ready {
                aggressor,
                effort,
                progress,
                partial_start,
                feed_gap,
            } => FeatureValue::Ready(EffortResult {
                feature: definition.key,
                timeframe,
                end: time,
                aggressor,
                effort: Qty::from_units(effort),
                progress: Price::from_units(progress),
                effort_high: Qty::from_units(EFFORT_HIGH),
                effort_median: Qty::from_units(EFFORT_HIGH / 2),
                move_high: Price::from_units(MOVE_HIGH),
                move_median: Price::from_units(MOVE_MEDIAN),
                coverage: Coverage {
                    partial_start,
                    feed_gap,
                },
                active: effort >= EFFORT_HIGH && progress <= MOVE_MEDIAN,
            }),
        }
    });
    let state = StateSnapshot {
        last_price: FeatureValue::Ready(Price::from_units(R)),
        atr: FeatureValue::Ready(Price::from_units(400 * USDT)),
        aggression: [warming(), warming(), warming()],
        cvd_utc_day: rec
            .cvd
            .map_or_else(warming, |(cvd, partial_start, feed_gap)| {
                FeatureValue::Ready(DayCvd {
                    feature: catalog::FLOW_CVD_UTC_DAY_V1.key,
                    day_open: EventTime::from_millis(time.as_millis().div_euclid(DAY_MS) * DAY_MS),
                    cvd: Qty::from_units(cvd),
                    coverage: Coverage {
                        partial_start,
                        feed_gap,
                    },
                })
            }),
        effort,
        liquidity: [warming(), warming(), warming()],
        oi_5m: warming(),
    };
    let sample = match rec.kind {
        RecKind::Baseline => Sample::Baseline {
            minute_end: EventTime::from_millis(time.as_millis() - 1),
        },
        RecKind::Trigger {
            definition,
            family,
            opener,
        } => {
            let sweep = Sweep {
                feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
                timeframe: Timeframe::M15,
                level: Level {
                    side: Side::High,
                    price: Price::from_units(R),
                    swing_time: EventTime::from_millis(time.as_millis() - HOUR_MS),
                    confirmed_bar_end: EventTime::from_millis(time.as_millis() - HOUR_MS),
                    known_at: EventTime::from_millis(time.as_millis() - HOUR_MS),
                    touches: 1,
                    coverage: no_gaps(),
                },
                time,
                price: Price::from_units(R + USDT),
                extreme: Price::from_units(R + 2 * USDT),
                window_bars: 1,
                outcome: SweepOutcome::Sfp,
                resolved_bar_end: Some(time),
                known_at: time,
                coverage: no_gaps(),
            };
            Sample::Trigger {
                trigger: Box::new(TriggerEvent {
                    feature: definition.key,
                    family,
                    known_at: time,
                    inputs: definition.inputs,
                    evidence: TriggerEvidence::Sfp(sweep),
                    context: context.clone(),
                    bias: bias(None),
                }),
                episode: Episode {
                    id: EpisodeId {
                        trigger: definition.key,
                        opened_at: CanonicalKey {
                            time: EventTime::from_millis(opener.0),
                            kind: EventKind::Trade,
                            seq: opener.1,
                        },
                        ordinal: 0,
                    },
                    opener: rec.is_opener(),
                    truncated: false,
                    bridge: false,
                },
            }
        }
    };
    ContextRecord {
        event: key,
        sample,
        context,
        bias: bias(rec.regime),
        state,
    }
}

/// The ADR-055 D9 clear flags of `recs` (in record order) per horizon: the
/// greedy rule over the baseline stream and over each trigger id's openers.
fn clear_flags(recs: &[Rec]) -> Vec<[bool; 5]> {
    let mut flags = vec![[false; 5]; recs.len()];
    for (h, (_, horizon_ms)) in HORIZONS.iter().enumerate() {
        let mut last: BTreeMap<String, i64> = BTreeMap::new();
        for (i, rec) in recs.iter().enumerate() {
            let stream = match rec.kind {
                RecKind::Baseline => "baseline".to_owned(),
                RecKind::Trigger { definition, .. } if rec.is_opener() => {
                    definition.key.to_string()
                }
                RecKind::Trigger { .. } => continue,
            };
            if last
                .get(&stream)
                .is_none_or(|l| rec.decision >= l + horizon_ms)
            {
                flags[i][h] = true;
                last.insert(stream, rec.decision);
            }
        }
    }
    flags
}

fn label(rec: &Rec, clear: [bool; 5]) -> ForwardLabel {
    let horizons = std::array::from_fn(|h| {
        let at = EventTime::from_millis(rec.decision + 1);
        let gap = Censor {
            at,
            reason: CensorReason::Gap,
        };
        let race = match rec.races[h] {
            R5::Up => Race::Upper { at, same_ms: false },
            R5::Down => Race::Lower { at, same_ms: false },
            R5::Neither => Race::Neither,
            R5::Censored => Race::Censored(gap),
            R5::Unavailable => Race::Unavailable,
        };
        let offset = 100 * USDT * (h as i64 + 1);
        HorizonLabel {
            horizon: LABEL_HORIZONS[h],
            clear: clear[h],
            barriers: (rec.races[h] != R5::Unavailable).then(|| Barriers {
                lower: Price::from_units(R - offset),
                upper: Price::from_units(R + offset),
            }),
            race,
            up: Excursion::NONE,
            down: Excursion::NONE,
            last: Some(Price::from_units(R)),
            censored: (rec.races[h] == R5::Censored).then_some(gap),
        }
    });
    let passage = |price: i64| PassageLabel {
        price: Price::from_units(price),
        passage: Passage::NotWithin,
    };
    ForwardLabel {
        event: rec.key(),
        ordinal: 0,
        reference: Some(Price::from_units(R)),
        atr: Some(Price::from_units(400 * USDT)),
        horizons,
        level: rec.passages.map(|(level, _)| passage(level)),
        extreme: rec.passages.map(|(_, extreme)| passage(extreme)),
    }
}

/// Writes `recs` (sorted here by decision and seq) as a new dataset `dir`.
fn write_dataset(dir: &Path, recs: &mut [Rec]) {
    recs.sort_by_key(|rec| (rec.decision, rec.seq));
    let first = recs.first().map_or(T0, |rec| rec.decision);
    let last = recs.last().map_or(T0, |rec| rec.decision);
    let header = OutcomeHeader {
        source: "archive binance-archive/BTCUSDT streams aggTrades correction fold".to_owned(),
        replay: ReplayWindow {
            start: EventTime::from_millis(first - DAY_MS),
            end: EventTime::from_millis(last + 2 * DAY_MS),
        },
        records: ReplayWindow {
            start: EventTime::from_millis(first),
            end: EventTime::from_millis(last + 1),
        },
        provenance: LabelProvenance::new(
            ContextProvenance::new(
                DataVersion::from_hex(&"5d".repeat(32)).unwrap(),
                &MarketStateEngine::new(),
            ),
            Labeling::CURRENT,
        ),
        code: "unknown".to_owned(),
    };
    let flags = clear_flags(recs);
    let mut writer = OutcomeWriter::create(dir, header).unwrap();
    for (rec, clear) in recs.iter().zip(flags) {
        writer.record(&context_record(rec)).unwrap();
        writer.label(&label(rec, clear)).unwrap();
    }
    writer.finish(recs.len() as u64, 0).unwrap();
}

/// `days` days of seeded random records from `T0`: a baseline per minute
/// with random effort (about half of it at or above `effort_high`), CVD,
/// regime, location and races, held auction failures of random length,
/// and triggers of every P4 and P1-trig id in episodes of a few members.
/// `trigger.sfp.1h@1` always has a side and races that ran.
fn random_recs(days: i64, seed: u64) -> Vec<Rec> {
    let mut lcg = Lcg(seed);
    let mut recs = Vec::new();
    let levels: [&[LevelKind]; 5] = [
        &[],
        &[LevelKind::Vah],
        &[LevelKind::StructuralLow],
        &[LevelKind::Val, LevelKind::StructuralHigh],
        &[LevelKind::Poc],
    ];
    let mut failure: Option<(AuctionState, Region, i64)> = None;
    let mut regime = 40.0;
    let mut openers: BTreeMap<String, (i64, u64)> = BTreeMap::new();
    let mut seq = 0u64;
    for minute in 0..days * 1_440 {
        let decision = T0 + minute * MINUTE_MS + 1_000;
        seq += 1;
        let mut rec = Rec::baseline(decision, seq);
        if lcg.below(120) == 0 {
            regime = [10.0, 40.0, 60.0, 90.0][lcg.below(4) as usize];
        }
        rec.regime = (lcg.below(50) != 0).then_some(regime);
        rec.levels = (lcg.below(10) != 0).then(|| levels[lcg.below(5) as usize]);
        for w in 0..WINDOWS.len() {
            rec.effort[w] = match lcg.below(40) {
                0..=3 => FlowSpec::Warming,
                4 => FlowSpec::Unavailable,
                _ => FlowSpec::Ready {
                    aggressor: if lcg.below(2) == 0 {
                        Aggressor::Buy
                    } else {
                        Aggressor::Sell
                    },
                    effort: lcg.below(2 * EFFORT_HIGH as u64),
                    progress: (lcg.below(161) - 80) * USDT,
                    partial_start: lcg.below(30) == 0,
                    feed_gap: lcg.below(30) == 0,
                },
            };
        }
        rec.cvd = (lcg.below(5) != 0).then(|| {
            (
                (lcg.below(3) - 1) * lcg.below(1_000),
                lcg.below(20) == 0,
                lcg.below(20) == 0,
            )
        });
        failure = match failure {
            Some((state, side, until)) if minute < until => Some((state, side, until)),
            _ if lcg.below(90) == 0 => Some((
                [AuctionState::FailedBreakout, AuctionState::FailedReclaim][lcg.below(2) as usize],
                [Region::Above, Region::Below][lcg.below(2) as usize],
                minute + 20 + lcg.below(200),
            )),
            _ => None,
        };
        rec.auction = match failure {
            Some((state, side, _)) => Some((state, Some(side))),
            None if lcg.below(10) == 0 => None,
            None => Some((AuctionState::InsideValue, None)),
        };
        let race = |lcg: &mut Lcg| match lcg.below(40) {
            0 => R5::Censored,
            1 => R5::Unavailable,
            k if k < 15 => R5::Up,
            k if k < 30 => R5::Down,
            _ => R5::Neither,
        };
        rec.races = std::array::from_fn(|_| race(&mut lcg));
        let baseline = rec.clone();
        recs.push(rec);
        // Triggers at this minute.
        let mut ids: Vec<&'static FeatureDefinition> = Vec::new();
        if lcg.below(60) == 0 {
            ids.push(&catalog::TRIGGER_FAILED_AUCTION_V1);
        }
        for id in [
            &catalog::TRIGGER_SFP_15M_V1,
            &catalog::TRIGGER_SFP_1H_V1,
            &catalog::TRIGGER_SFP_4H_V1,
            &catalog::TRIGGER_SFP_1D_V1,
        ] {
            if lcg.below(45) == 0 {
                ids.push(id);
            }
        }
        for (w, id) in [
            &catalog::TRIGGER_EFFORT_RESULT_5M_V1,
            &catalog::TRIGGER_EFFORT_RESULT_15M_V1,
            &catalog::TRIGGER_EFFORT_RESULT_1H_V1,
        ]
        .into_iter()
        .enumerate()
        {
            if matches!(baseline.effort[w], FlowSpec::Ready { .. }) && lcg.below(25) == 0 {
                ids.push(id);
            }
        }
        for id in ids {
            seq += 1;
            let name = id.key.to_string();
            let opener = match openers.get(&name) {
                Some(opener) if lcg.below(2) == 0 => *opener,
                _ => {
                    openers.insert(name.clone(), (decision, seq));
                    (decision, seq)
                }
            };
            let mut rec = Rec::trigger(decision, seq, id, opener);
            rec.regime = baseline.regime;
            rec.levels = baseline.levels;
            rec.effort = baseline.effort;
            rec.cvd = baseline.cvd;
            rec.races = std::array::from_fn(|_| race(&mut lcg));
            if name == "trigger.failed_auction@1" {
                rec.auction = match lcg.below(6) {
                    0 => None,
                    _ => Some((
                        AuctionState::FailedBreakout,
                        Some([Region::Above, Region::Below][lcg.below(2) as usize]),
                    )),
                };
            } else if name.starts_with("trigger.sfp.") {
                let extreme = R + (lcg.below(3) - 1) * USDT;
                rec.passages = (lcg.below(8) != 0).then_some((R, extreme));
                if name == "trigger.sfp.1h@1" {
                    rec.passages = Some((R, R + if lcg.below(2) == 0 { USDT } else { -USDT }));
                    rec.races = std::array::from_fn(|_| {
                        [R5::Up, R5::Down, R5::Neither][lcg.below(3) as usize]
                    });
                }
            }
            recs.push(rec);
        }
    }
    recs
}

/// A fixture dataset in a temp directory, opened with the study views.
struct Built {
    _temp: TempDir,
    views: OutcomeViews,
    inputs: Inputs,
    lines: Lines,
}

fn built(recs: &mut [Rec], exclude: &[(i64, i64)]) -> Built {
    let temp = TempDir::new("order-flow-study");
    let dir = temp.path().join("dataset");
    write_dataset(&dir, recs);
    let mut report = Vec::new();
    assert!(
        conditional::verify(&dir, &mut report).unwrap(),
        "{}",
        String::from_utf8_lossy(&report)
    );
    let views = OutcomeViews::open(&dir).unwrap();
    create_study(&views, exclude);
    let (inputs, lines) = load(&views);
    Built {
        _temp: temp,
        views,
        inputs,
        lines,
    }
}

/// Settings for the fixture runs: few resamples, the pre-registered blocks
/// and seed.
fn test_settings(mu: f64, threads: usize) -> Settings {
    Settings {
        confidence: 0.95,
        mu,
        fee_bps: 10.0,
        resamples: 200,
        blocks: BLOCKS,
        seed: SEED,
        threads,
    }
}

/// The hand-built records at every population edge (ADR-058 D3), on
/// 2026-01-05 from 10:00 UTC, one minute apart, named for
/// [`population_edges_follow_the_adr`]. No baseline lies in the bin set
/// `both`, so a trigger there has an empty stratum.
fn edge_recs() -> Vec<Rec> {
    use Aggressor::{Buy, Sell};
    let at = |k: i64| T0 + 10 * HOUR_MS + k * MINUTE_MS;
    let mut recs = Vec::new();
    let base = |k: i64, effort: [FlowSpec; 3]| {
        let mut rec = Rec::baseline(at(k), k as u64 * 10);
        rec.effort = effort;
        rec
    };
    let w = FlowSpec::Warming;
    // E1: effort exactly effort_high, progress exactly the median move,
    // buy: P1-buy at 5m; races up, down, neither, censored, unavailable.
    let mut e1 = base(1, [ready_flow(Buy, EFFORT_HIGH, MOVE_MEDIAN), w, w]);
    e1.races = [R5::Up, R5::Down, R5::Neither, R5::Censored, R5::Unavailable];
    recs.push(e1);
    // E2: effort one unit below effort_high: no member.
    recs.push(base(2, [ready_flow(Buy, EFFORT_HIGH - 1, 0), w, w]));
    // E3: progress exactly the high move, sell: P2 at 15m.
    recs.push(base(3, [w, ready_flow(Sell, EFFORT_HIGH, MOVE_HIGH), w]));
    // E4, E5: incomplete coverage (partial start, feed gap) at 5m.
    for (k, partial_start, feed_gap) in [(4, true, false), (5, false, true)] {
        recs.push(base(
            k,
            [
                FlowSpec::Ready {
                    aggressor: Buy,
                    effort: EFFORT_HIGH,
                    progress: 0,
                    partial_start,
                    feed_gap,
                },
                w,
                w,
            ],
        ));
    }
    // E6: not ready at 5m (unavailable).
    recs.push(base(6, [FlowSpec::Unavailable, w, w]));
    // E7–E10: held failures at 1h, progress between the median and the
    // high move (so no P1 or P2): FB above + buy, FR above + sell, FR
    // above + buy, FB below + sell.
    for (k, state, side, aggressor) in [
        (7, AuctionState::FailedBreakout, Region::Above, Buy),
        (8, AuctionState::FailedReclaim, Region::Above, Sell),
        (9, AuctionState::FailedReclaim, Region::Above, Buy),
        (10, AuctionState::FailedBreakout, Region::Below, Sell),
    ] {
        let mut rec = base(k, [w, w, ready_flow(aggressor, EFFORT_HIGH, 30 * USDT)]);
        rec.auction = Some((state, Some(side)));
        recs.push(rec);
    }
    // T1: SFP 15m, extreme above the level, in bin set `both`.
    let mut t1 = Rec::trigger(at(11), 111, &catalog::TRIGGER_SFP_15M_V1, (at(11), 111));
    t1.passages = Some((R, R + USDT));
    t1.levels = Some(&[LevelKind::Vah, LevelKind::StructuralHigh]);
    recs.push(t1);
    // T2: SFP 1h, extreme below the level.
    let mut t2 = Rec::trigger(at(12), 121, &catalog::TRIGGER_SFP_1H_V1, (at(12), 121));
    t2.passages = Some((R, R - USDT));
    recs.push(t2);
    // T3: SFP 4h, extreme at the level: no side.
    let mut t3 = Rec::trigger(at(13), 131, &catalog::TRIGGER_SFP_4H_V1, (at(13), 131));
    t3.passages = Some((R, R));
    recs.push(t3);
    // T4: failed auction below; T5: its auction not ready (no side).
    let mut t4 = Rec::trigger(
        at(14),
        141,
        &catalog::TRIGGER_FAILED_AUCTION_V1,
        (at(14), 141),
    );
    t4.auction = Some((AuctionState::FailedBreakout, Some(Region::Below)));
    recs.push(t4);
    recs.push(Rec::trigger(
        at(15),
        151,
        &catalog::TRIGGER_FAILED_AUCTION_V1,
        (at(14), 141),
    ));
    // T6: effort trigger 15m, sell; T7: effort trigger 1h, partial start.
    let mut t6 = Rec::trigger(
        at(16),
        161,
        &catalog::TRIGGER_EFFORT_RESULT_15M_V1,
        (at(16), 161),
    );
    t6.effort = [w, ready_flow(Sell, EFFORT_HIGH, 0), w];
    recs.push(t6);
    let mut t7 = Rec::trigger(
        at(17),
        171,
        &catalog::TRIGGER_EFFORT_RESULT_1H_V1,
        (at(17), 171),
    );
    t7.effort = [
        w,
        w,
        FlowSpec::Ready {
            aggressor: Buy,
            effort: EFFORT_HIGH,
            progress: 0,
            partial_start: true,
            feed_gap: false,
        },
    ];
    recs.push(t7);
    // A plain baseline at each trigger's minute, so every other stratum
    // has a baseline race.
    for k in 11..=17 {
        recs.push(base(k, [w, w, w]));
    }
    recs
}

// ---------------------------------------------------------------------------
// Fixture tests.

#[test]
fn the_study_queries_match_the_twin() {
    let mut recs = random_recs(2, 28);
    recs.extend(edge_recs());
    let built = built(&mut recs, &[]);
    assert!(
        built.inputs.members.len() > 1_000,
        "{}",
        built.inputs.members.len()
    );
    for pair in ["P1-buy", "P1-sell", "P2", "P3", "P4", "P1-trig"] {
        assert!(
            built.inputs.members.iter().any(|m| m.pair == pair),
            "no {pair} member"
        );
    }
    assert_eq!(
        twin_mismatches(&built.views, &[], &built.inputs, &built.lines, false),
        0
    );
    // An excluded range drops its records from every study view, before
    // any is read; the twin drops the same.
    let exclude = [(T0 + 6 * HOUR_MS, T0 + 30 * HOUR_MS)];
    create_study(&built.views, &exclude);
    let (inputs, lines) = load(&built.views);
    assert!(inputs.baseline.len() < built.inputs.baseline.len());
    let inside = |ms: i64| (exclude[0].0..exclude[0].1).contains(&ms);
    assert!(!inputs.baseline.iter().any(|b| inside(b.decision)));
    assert!(!inputs.members.iter().any(|m| inside(m.decision)));
    assert_eq!(
        twin_mismatches(&built.views, &exclude, &inputs, &lines, false),
        0
    );
}

#[test]
fn population_edges_follow_the_adr() {
    let mut recs = edge_recs();
    let built = built(&mut recs, &[]);
    let at = |k: i64| T0 + 10 * HOUR_MS + k * MINUTE_MS;
    // (pair, window, decision) → (side, hypothesis upper?, outcomes).
    let find = |pair: &str, window: Option<&str>, k: i64| {
        built
            .inputs
            .members
            .iter()
            .find(|m| m.pair == pair && m.window.as_deref() == window && m.decision == at(k))
    };
    use Outcome::{Censored, Counter, Hypothesis, Neither, Unavailable};
    let e1 = find("P1-buy", Some("5m"), 1).expect("E1 is a P1-buy member");
    assert_eq!((e1.side.as_str(), e1.upper), ("buy", false));
    assert_eq!(
        e1.outcomes,
        [Counter, Hypothesis, Neither, Censored, Unavailable]
    );
    assert!(
        find("P1-buy", Some("5m"), 2).is_none(),
        "E2 below effort_high"
    );
    let e3 = find("P2", Some("15m"), 3).expect("E3 is a P2 member");
    assert_eq!((e3.side.as_str(), e3.upper), ("sell", false));
    assert!(find("P1-buy", Some("5m"), 4).is_none() && find("P1-buy", Some("5m"), 5).is_none());
    // P3: failed_breakout above with buying, failed_reclaim above with
    // selling (the probe went down), failed_breakout below with selling;
    // failed_reclaim above with buying aggresses against the probe.
    let p3: Vec<(i64, &str, bool)> = built
        .inputs
        .members
        .iter()
        .filter(|m| m.pair == "P3")
        .map(|m| ((m.decision - at(0)) / MINUTE_MS, m.side.as_str(), m.upper))
        .collect();
    assert_eq!(
        p3,
        [(7, "buy", false), (8, "sell", true), (10, "sell", true)]
    );
    assert!(
        built.inputs.members.iter().all(|m| m.decision != at(9)),
        "E9"
    );
    // P4: SFP sides from the extreme, the failed auction's from its side.
    let p4: Vec<(&str, &str, bool)> = built
        .inputs
        .members
        .iter()
        .filter(|m| m.pair == "P4")
        .map(|m| (m.trigger.as_deref().unwrap(), m.side.as_str(), m.upper))
        .collect();
    assert_eq!(
        p4,
        [
            ("trigger.failed_auction@1", "below", true),
            ("trigger.sfp.15m@1", "above", false),
            ("trigger.sfp.1h@1", "below", true),
        ]
    );
    let t6 = find("P1-trig", Some("15m"), 16).expect("T6 is a P1-trig member");
    assert_eq!((t6.side.as_str(), t6.upper), ("sell", true));
    assert!(find("P1-trig", Some("1h"), 17).is_none());
    // Exclusions: per universe, effort not ready, incomplete, no side.
    // Universe, window, trigger, then records, not ready, incomplete and
    // no side.
    type Row<'a> = (&'a str, Option<&'a str>, Option<&'a str>, [u64; 4]);
    let exclusions: Vec<Row> = built
        .inputs
        .exclusions
        .iter()
        .map(|e| {
            (
                e.universe.as_str(),
                e.window.as_deref(),
                e.trigger.as_deref(),
                [e.records, e.not_ready, e.incomplete, e.no_side],
            )
        })
        .collect();
    let baseline = 17;
    assert_eq!(
        exclusions,
        [
            (
                "P1-trig",
                Some("15m"),
                Some("trigger.effort_result.15m@1"),
                [1, 0, 0, 0]
            ),
            (
                "P1-trig",
                Some("1h"),
                Some("trigger.effort_result.1h@1"),
                [1, 0, 1, 0]
            ),
            ("P4", None, Some("trigger.failed_auction@1"), [2, 0, 0, 1]),
            ("P4", None, Some("trigger.sfp.15m@1"), [1, 0, 0, 0]),
            ("P4", None, Some("trigger.sfp.1h@1"), [1, 0, 0, 0]),
            ("P4", None, Some("trigger.sfp.4h@1"), [1, 0, 0, 1]),
            // 5m: E1–E5 ready (E4, E5 incomplete), E6 unavailable and the
            // rest warming.
            ("baseline", Some("5m"), None, [baseline, 13, 2, 0]),
            ("baseline", Some("15m"), None, [baseline, 16, 0, 0]),
            ("baseline", Some("1h"), None, [baseline, 13, 0, 0]),
        ]
    );
    assert_eq!(
        twin_mismatches(&built.views, &[], &built.inputs, &built.lines, false),
        0
    );
}

#[test]
fn an_empty_stratum_drops_its_members_and_counts_them() {
    let mut recs = edge_recs();
    let built = built(&mut recs, &[]);
    let (populations, results) = study(&built.inputs, &test_settings(0.1, 1));
    let sfp15 = populations
        .iter()
        .position(|p| p.trigger.as_deref() == Some("trigger.sfp.15m@1"))
        .unwrap();
    let primary: Vec<&CellResult> = results
        .iter()
        .filter(|r| r.cell.population == sfp15 && r.cell.split == Split::All)
        .collect();
    assert_eq!(primary.len(), 5);
    for result in primary {
        // T1's race ran at every horizon (neither), but its stratum (bin set
        // `both`) holds no baseline race: dropped, counted, not estimated.
        assert_eq!(result.point.dropped, 1);
        assert_eq!(result.point.n(), 0);
        assert_eq!(result.effect, None);
        assert_eq!(result.primary.reading, Reading::Insufficient);
    }
}

#[test]
fn a_condition_held_for_three_hours_is_one_episode_and_one_unit() {
    // A P1 condition at w = 1h on every minute for 3 h.
    let decisions: Vec<i64> = (0..=180).map(|m| T0 + m * MINUTE_MS).collect();
    let episodes = chain_episodes(&decisions, HOUR_MS, &vec![None; decisions.len()]);
    assert!(episodes.iter().all(|e| *e == 0));
    let members: Vec<(i64, u64, bool)> = decisions
        .iter()
        .zip(&episodes)
        .map(|(d, e)| (*d, *e, true))
        .collect();
    for (_, horizon_ms) in HORIZONS {
        assert_eq!(unit_count(&members, horizon_ms), (1, 1));
    }
}

#[test]
fn members_w_apart_open_two_episodes_and_closer_ones_chain() {
    for (w_name, w) in WINDOWS {
        let apart = chain_episodes(&[T0, T0 + w], w, &[None, None]);
        assert_eq!(apart, [0, 1], "{w_name}: exactly w apart");
        let closer = chain_episodes(&[T0, T0 + w - MINUTE_MS], w, &[None, None]);
        assert_eq!(closer, [0, 0], "{w_name}: w − 1 min apart");
    }
}

#[test]
fn representatives_h_apart_count_twice_and_closer_ones_once() {
    for (h_name, h) in HORIZONS {
        let members = [(T0, 0, true), (T0 + h, 1, true)];
        assert_eq!(unit_count(&members, h), (2, 2), "{h_name}: exactly H");
        let members = [(T0, 0, true), (T0 + h - MINUTE_MS, 1, true)];
        assert_eq!(unit_count(&members, h), (2, 1), "{h_name}: H − 1 min");
    }
}

#[test]
fn an_episode_is_represented_by_its_earliest_member_that_ran() {
    let h = HORIZONS[2].1;
    // Episode 0's first member is censored: its second, 30 min later,
    // represents it, so episode 1 at +65 min is less than H after it.
    let members = [
        (T0, 0, false),
        (T0 + 30 * MINUTE_MS, 0, true),
        (T0 + 65 * MINUTE_MS, 1, true),
    ];
    assert_eq!(unit_count(&members, h), (2, 1));
    // Had the first member run, episode 1 would be H after it.
    let members = [(T0, 0, true), (T0 + 65 * MINUTE_MS, 1, true)];
    assert_eq!(unit_count(&members, h), (2, 2));
    // An episode without a member that ran adds nothing.
    let members = [
        (T0, 0, false),
        (T0 + 10 * MINUTE_MS, 0, false),
        (T0 + 2 * h, 1, true),
    ];
    assert_eq!(unit_count(&members, h), (1, 1));
}

#[test]
fn a_split_cell_keeps_an_episode_whose_opener_is_in_the_other_split() {
    // One P1-buy episode at w = 5m: three members a minute apart, the first
    // in the `low` regime, the others `high`.
    let member = |k: i64, regime: &str| Member {
        pair: "P1-buy".to_owned(),
        window: Some("5m".to_owned()),
        trigger: None,
        record: k as u64,
        decision: T0 + k * MINUTE_MS,
        side: "buy".to_owned(),
        upper: false,
        regime: regime.to_owned(),
        location: "neither".to_owned(),
        episode: None,
        cvd: Some("with".to_owned()),
        outcomes: [Outcome::Neither; 5],
    };
    let inputs = Inputs {
        members: vec![member(0, "low"), member(1, "high"), member(2, "high")],
        ..Inputs::default()
    };
    let populations = populations();
    let episodes = member_episodes(&inputs, &populations);
    assert_eq!(episodes, [0, 0, 0]);
    let cells = cells(&inputs, &populations);
    let high = cells
        .iter()
        .find(|c| c.split == Split::Regime("high") && c.horizon == 0 && !c.members.is_empty())
        .unwrap();
    assert_eq!(high.members, [1, 2]);
    assert_eq!(cell_units(&inputs, &episodes, high), (1, 1));
}

#[test]
fn bursts_in_one_held_failure_are_one_p3_episode() {
    // Two P3 bursts at w = 5m, 2 h apart (disjoint windows): in one held
    // failure they are one episode; in two failures, two.
    let baseline = |k: i64, state: &str| Baseline {
        record: k as u64,
        decision: T0 + k * MINUTE_MS,
        regime: "low".to_owned(),
        location: "neither".to_owned(),
        auction_state: Some(state.to_owned()),
        auction_side: Some("above".to_owned()),
        races: [StoredRace::Neither; 5],
    };
    let member = |k: i64| Member {
        pair: "P3".to_owned(),
        window: Some("5m".to_owned()),
        trigger: None,
        record: k as u64,
        decision: T0 + k * MINUTE_MS,
        side: "buy".to_owned(),
        upper: false,
        regime: "low".to_owned(),
        location: "neither".to_owned(),
        episode: None,
        cvd: None,
        outcomes: [Outcome::Neither; 5],
    };
    let held: Vec<Baseline> = (0..=120).map(|k| baseline(k, "failed_breakout")).collect();
    let mut broken = held.clone();
    broken[60].auction_state = Some("inside_value".to_owned());
    for (stream, expected) in [(held, [0u64, 0]), (broken, [0, 1])] {
        let inputs = Inputs {
            baseline: stream,
            members: vec![member(0), member(120)],
            ..Inputs::default()
        };
        assert_eq!(member_episodes(&inputs, &populations()), expected);
    }
}

#[test]
fn the_unit_count_reads_no_outcome() {
    let mut recs = random_recs(2, 7);
    let built = built(&mut recs, &[]);
    let populations = populations();
    let units = |inputs: &Inputs| {
        let episodes = member_episodes(inputs, &populations);
        cells(inputs, &populations)
            .iter()
            .map(|cell| cell_units(inputs, &episodes, cell))
            .collect::<Vec<_>>()
    };
    let mut swapped = built.inputs.clone();
    for member in &mut swapped.members {
        for outcome in &mut member.outcomes {
            *outcome = match *outcome {
                Outcome::Hypothesis => Outcome::Counter,
                Outcome::Counter => Outcome::Hypothesis,
                other => other,
            };
        }
    }
    let original = units(&built.inputs);
    assert!(original.iter().any(|(_, units)| *units > 1));
    assert_eq!(units(&swapped), original);
}

#[test]
fn greedy_units_are_the_brute_force_maximum() {
    let mut lcg = Lcg(28);
    for _ in 0..300 {
        let n = 1 + lcg.below(12) as usize;
        let h = [5, 15, 60][lcg.below(3) as usize] * MINUTE_MS;
        let mut members: Vec<(i64, u64, bool)> = Vec::new();
        let mut t = T0;
        let mut episode = 0;
        for _ in 0..n {
            t += lcg.below(40) * MINUTE_MS;
            if lcg.below(3) == 0 {
                episode += 1;
            }
            members.push((t, episode, lcg.below(4) != 0));
        }
        // The representatives: each episode's earliest member that ran.
        let mut seen = BTreeSet::new();
        let reps: Vec<i64> = members
            .iter()
            .filter(|(_, e, ran)| *ran && seen.insert(*e))
            .map(|(d, _, _)| *d)
            .collect();
        let mut best = 0;
        for mask in 0u32..1 << reps.len() {
            let chosen: Vec<i64> = (0..reps.len())
                .filter(|i| mask & (1 << i) != 0)
                .map(|i| reps[i])
                .collect();
            if chosen.windows(2).all(|p| p[1] - p[0] >= h) {
                best = best.max(chosen.len() as u64);
            }
        }
        assert_eq!(
            unit_count(&members, h),
            (reps.len() as u64, best),
            "{members:?}"
        );
    }
}

#[test]
fn units_equal_outcome_summarys_clear_for_a_trigger_whose_openers_ran() {
    // trigger.sfp.1h@1 always has a side and races that ran, so every
    // opener represents its episode (ADR-058 D6), and `clear` is the same
    // greedy rule over the openers (ADR-055 D9).
    let mut recs = random_recs(3, 11);
    let built = built(&mut recs, &[]);
    let populations = populations();
    let episodes = member_episodes(&built.inputs, &populations);
    let p = populations
        .iter()
        .position(|p| p.trigger.as_deref() == Some("trigger.sfp.1h@1"))
        .unwrap();
    let mut summary = Vec::new();
    built
        .views
        .query_tsv("outcome_summary", Some(1), &mut summary)
        .unwrap();
    let summary: Vec<String> = String::from_utf8(summary)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    let mut clear: BTreeMap<String, u64> = BTreeMap::new();
    each_row(&summary, |get| {
        if get("split") == "trigger" && get("trigger") == "trigger.sfp.1h@1" {
            clear.insert(get("horizon"), get("clear").parse().unwrap());
        }
    });
    for cell in cells(&built.inputs, &populations)
        .iter()
        .filter(|c| c.population == p && c.split == Split::All)
    {
        let (episodes_ran, units) = cell_units(&built.inputs, &episodes, cell);
        assert!(episodes_ran > 3, "{episodes_ran}");
        assert_eq!(
            units, clear[HORIZONS[cell.horizon].0],
            "{}",
            HORIZONS[cell.horizon].0
        );
    }
}

#[test]
fn n_min_follows_t1() {
    assert!((z_of(0.95) - 1.959_964).abs() < 1e-6);
    assert!((z_of(0.90) - 1.644_854).abs() < 1e-6);
    assert!((normal_quantile(0.01) + 2.326_348).abs() < 1e-6);
    assert!((normal_quantile(0.5)).abs() < 1e-12);
    assert_eq!(n_min(0.95, 0.10), 385);
    assert_eq!(n_min(0.90, 0.10), 271);
    // The gate: n_min − 1 units read `insufficient`, n_min units read the
    // interval.
    let values: Vec<Option<f64>> = (1..=100).map(|k| Some(f64::from(k) / 100.0)).collect();
    assert_eq!(
        interval(&values, 384, 385, 0.95).reading,
        Reading::Insufficient
    );
    let read = interval(&values, 385, 385, 0.95);
    assert_eq!(read.reading, Reading::Hypothesis);
    assert_eq!((read.low, read.high), (0.03, 0.98));
    // Too many undefined resamples: insufficient whatever the units.
    let mut sparse = values.clone();
    for value in sparse.iter_mut().take(3) {
        *value = None;
    }
    assert_eq!(
        interval(&sparse, 1_000, 385, 0.95).reading,
        Reading::Insufficient
    );
    sparse[2] = Some(0.5);
    assert_ne!(
        interval(&sparse, 1_000, 385, 0.95).reading,
        Reading::Insufficient
    );
}

#[test]
fn the_new_york_hour_of_week_crosses_both_dst_switches() {
    let day = |y: i64, m: i64, d: i64| {
        mie_adapter_binance::archive::catalog::days_from_civil(y, m, d) * DAY_MS
    };
    // The #155 clock switches at the UTC date: Sunday 2026-03-08 is
    // daylight saving from 00:00 UTC (06:59 UTC reads 02:59), Saturday
    // 03-07 is not (14:00 UTC reads 09:00).
    let switch = day(2026, 3, 8);
    assert_eq!(
        ny_hour_of_week(switch + 6 * HOUR_MS + 59 * MINUTE_MS),
        6 * 24 + 2
    );
    assert_eq!(ny_hour_of_week(day(2026, 3, 7) + 14 * HOUR_MS), 5 * 24 + 9);
    assert_eq!(ny_hour_of_week(day(2026, 3, 9) + 13 * HOUR_MS), 9);
    assert_eq!(ny_hour_of_week(day(2026, 3, 6) + 14 * HOUR_MS), 4 * 24 + 9);
    // Sunday 2025-11-02 is standard time from 00:00 UTC (12:00 UTC reads
    // 07:00); Monday 13:00 UTC is 08:00 EST; the Friday before, 13:00 UTC
    // is 09:00 EDT.
    assert_eq!(ny_hour_of_week(day(2025, 11, 2) + 12 * HOUR_MS), 6 * 24 + 7);
    assert_eq!(ny_hour_of_week(day(2025, 11, 3) + 13 * HOUR_MS), 8);
    assert_eq!(
        ny_hour_of_week(day(2025, 10, 31) + 13 * HOUR_MS),
        4 * 24 + 9
    );
    // The stratum puts the hour before the bin set.
    assert_eq!(stratum(day(2025, 11, 3) + 13 * HOUR_MS, "both"), 8 * 5 + 4);
}

/// A synthetic study: `days` days of hourly baselines with balanced races,
/// and P2 members at 5m every second hour whose race is the hypothesis
/// barrier with probability `p_hypothesis`.
fn planted(days: i64, p_hypothesis: u64, seed: u64) -> Inputs {
    let mut lcg = Lcg(seed);
    let mut inputs = Inputs::default();
    for hour in 0..days * 24 {
        let decision = T0 + hour * HOUR_MS;
        let mut race = || {
            if lcg.below(2) == 0 {
                StoredRace::Upper
            } else {
                StoredRace::Lower
            }
        };
        let races = [
            race(),
            StoredRace::Neither,
            StoredRace::Neither,
            StoredRace::Neither,
            StoredRace::Neither,
        ];
        inputs.baseline.push(Baseline {
            record: hour as u64,
            decision,
            regime: "low".to_owned(),
            location: "neither".to_owned(),
            auction_state: None,
            auction_side: None,
            races,
        });
        if hour % 2 == 0 {
            let upper = lcg.below(2) == 0;
            let outcome = if (lcg.below(100) as u64) < p_hypothesis {
                Outcome::Hypothesis
            } else {
                Outcome::Counter
            };
            inputs.members.push(Member {
                pair: "P2".to_owned(),
                window: Some("5m".to_owned()),
                trigger: None,
                record: hour as u64,
                decision,
                side: if upper { "buy" } else { "sell" }.to_owned(),
                upper,
                regime: "low".to_owned(),
                location: "neither".to_owned(),
                episode: None,
                cvd: Some("unknown".to_owned()),
                outcomes: [outcome; 5],
            });
        }
    }
    inputs
}

fn p2_5m_primary(results: &[CellResult], populations: &[Population]) -> CellResult {
    results
        .iter()
        .find(|r| {
            populations[r.cell.population]
                == Population {
                    pair: "P2",
                    window: Some(0),
                    trigger: None,
                }
                && r.cell.split == Split::All
                && r.cell.horizon == 0
        })
        .unwrap()
        .clone()
}

#[test]
fn a_planted_tilt_is_recovered_and_a_planted_null_is_not() {
    let settings = Settings {
        resamples: 400,
        ..test_settings(0.5, 4)
    };
    let tilt = planted(120, 75, 1);
    let (populations, results) = study(&tilt, &settings);
    let cell = p2_5m_primary(&results, &populations);
    assert!(cell.units >= n_min(0.95, 0.5));
    assert_eq!(cell.primary.reading, Reading::Hypothesis, "{cell:?}");
    assert!(cell.primary.low > 0.2, "{cell:?}");
    let null = planted(120, 50, 2);
    let (populations, results) = study(&null, &settings);
    let cell = p2_5m_primary(&results, &populations);
    assert_eq!(cell.primary.reading, Reading::Inconclusive, "{cell:?}");
    assert!(cell.primary.low < 0.0 && cell.primary.high > 0.0);
}

#[test]
fn the_report_is_deterministic_across_runs_and_threads() {
    let mut recs = random_recs(2, 5);
    recs.extend(edge_recs());
    let built = built(&mut recs, &[]);
    let run = |threads: usize| {
        let settings = test_settings(0.3, threads);
        let (populations, results) = study(&built.inputs, &settings);
        report(&built.inputs, &settings, &populations, &results)
    };
    let one = run(1);
    assert_eq!(one, run(1), "same seed, same output");
    assert_eq!(one, run(4), "the thread count does not change the output");
    // Blind: no record, decision time or episode id in the report.
    for member in built.inputs.members.iter().take(50) {
        assert!(!one.contains(&member.decision.to_string()));
        if let Some(episode) = &member.episode {
            assert!(!one.contains(episode.as_str()));
        }
    }
}

#[test]
fn duckdb_threads_do_not_change_the_populations() {
    // 30 days: more than 40 960 records, so `effort` and `outcomes` have
    // several 122 880-row row groups and DuckDB scans them in parallel.
    let mut recs = random_recs(30, 3);
    assert!(recs.len() > 43_000, "{}", recs.len());
    let built = built(&mut recs, &[]);
    built.views.set_threads(1).unwrap();
    let (_, one) = load(&built.views);
    built.views.set_threads(4).unwrap();
    let (_, four) = load(&built.views);
    assert_eq!(one, four);
}

/// `from..to` millisecond ranges, comma-separated (MIE_STUDY_EXCLUDE).
fn parse_exclude(text: &str) -> Result<Vec<(i64, i64)>, String> {
    text.split(',')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (from, to) = part
                .split_once("..")
                .ok_or_else(|| format!("{part:?} is not from..to"))?;
            let parse = |s: &str| {
                s.parse::<i64>()
                    .map_err(|_| format!("{s:?} is not a millisecond time"))
            };
            let (from, to) = (parse(from)?, parse(to)?);
            if from >= to {
                return Err(format!("{part:?} is empty"));
            }
            Ok((from, to))
        })
        .collect()
}

#[test]
fn exclusion_ranges_parse_strictly() {
    assert_eq!(parse_exclude(""), Ok(vec![]));
    assert_eq!(parse_exclude("1..5, 7..9"), Ok(vec![(1, 5), (7, 9)]));
    for bad in ["5..1", "3..3", "x..2", "1-2"] {
        assert!(parse_exclude(bad).is_err(), "{bad}");
    }
}

// ---------------------------------------------------------------------------
// The M3 measurement (ADR-058; run once by the main session).

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

/// Asserts the ADR-057 *Acceptance* counts of the #12 year on the whole
/// dataset, before any exclusion (ADR-058 D1).
fn reconcile(views: &OutcomeViews) {
    let lines = tsv_lines(
        views,
        "SELECT count(*) AS contexts, count(*) FILTER (WHERE sample = 'baseline') AS baseline, \
         count(*) FILTER (WHERE sample = 'trigger') AS trigger FROM contexts",
    );
    each_row(&lines, |get| {
        let counts: (u64, u64, u64) = (
            get("contexts").parse().unwrap(),
            get("baseline").parse().unwrap(),
            get("trigger").parse().unwrap(),
        );
        assert_eq!(counts, YEAR_CONTEXTS, "contexts, baseline, trigger");
    });
    let lines = tsv_lines(
        views,
        "SELECT o.horizon, o.horizon_ms, \
         count(*) FILTER (WHERE o.race = 'neither') AS neither, \
         count(*) FILTER (WHERE o.race = 'unavailable') AS unavailable, \
         count(*) FILTER (WHERE o.censor_reason = 'end_of_data') AS end_of_data, \
         count(*) FILTER (WHERE o.clear AND c.sample = 'baseline') AS clear_baseline, \
         count(*) FILTER (WHERE o.clear AND c.sample = 'trigger') AS clear_trigger \
         FROM outcomes o JOIN contexts c USING (record) \
         GROUP BY o.horizon, o.horizon_ms ORDER BY o.horizon_ms",
    );
    let mut horizons = Vec::new();
    each_row(&lines, |get| {
        let counts: [u64; 5] = [
            "neither",
            "unavailable",
            "end_of_data",
            "clear_baseline",
            "clear_trigger",
        ]
        .map(|c| get(c).parse().unwrap());
        horizons.push((get("horizon"), counts));
    });
    let expected: Vec<(String, [u64; 5])> = YEAR_HORIZONS
        .iter()
        .map(|(h, c)| ((*h).to_owned(), *c))
        .collect();
    assert_eq!(horizons, expected, "the ADR-056 Measurement counts");
    let effort = tsv_lines(views, "SELECT count(*) AS rows FROM effort");
    assert_eq!(effort[1], (3 * YEAR_CONTEXTS.0).to_string(), "effort rows");
}

#[test]
#[ignore = "runs the pre-registered ADR-058 study on a built dataset; see the module docs"]
fn measure_order_flow_study() {
    use sha2::{Digest, Sha256};
    let dir = std::path::PathBuf::from(env("MIE_OUTCOME_DATASET"))
        .canonicalize()
        .expect("MIE_OUTCOME_DATASET exists");
    let confidence: f64 = env("MIE_STUDY_CONFIDENCE").parse().unwrap();
    let mu: f64 = env("MIE_STUDY_MU").parse().unwrap();
    let fee_bps: f64 = env("MIE_STUDY_FEE_BPS").parse().unwrap();
    let exclude = parse_exclude(&env("MIE_STUDY_EXCLUDE")).unwrap();
    assert!(
        confidence > 0.0 && confidence < 1.0,
        "confidence {confidence}"
    );
    assert!(mu > 0.0, "mu {mu}");
    assert!(fee_bps >= 0.0, "fee {fee_bps}");

    let mut report_text = Vec::new();
    assert!(
        conditional::verify(&dir, &mut report_text).unwrap(),
        "{}",
        String::from_utf8_lossy(&report_text)
    );
    let dataset = OutcomeDataset::open(&dir).unwrap();
    let manifest = dataset.manifest();
    let provenance = [
        manifest.data.as_str(),
        manifest.engine.as_str(),
        manifest.sampling.as_str(),
        manifest.sampling_fingerprint.as_str(),
        manifest.labeling.as_str(),
        manifest.labeling_fingerprint.as_str(),
    ];
    assert_eq!(provenance, YEAR_PROVENANCE, "ADR-058 D1 provenance");
    assert_eq!(manifest.records, YEAR_RECORDS, "the #12 year only");
    let views = OutcomeViews::open(&dir).unwrap();
    views.set_threads(4).unwrap();
    reconcile(&views);

    create_study(&views, &exclude);
    let (inputs, lines) = load(&views);
    let mismatched = twin_mismatches(&views, &exclude, &inputs, &lines, true);
    assert_eq!(
        mismatched, 0,
        "the study views or units disagree with the twin; nothing is printed"
    );

    let settings = Settings {
        confidence,
        mu,
        fee_bps,
        resamples: RESAMPLES,
        blocks: BLOCKS,
        seed: SEED,
        threads: 4,
    };
    let (populations, results) = study(&inputs, &settings);
    let queries: String =
        Sha256::digest(STUDY_SQL.as_bytes())
            .iter()
            .fold(String::new(), |mut s, b| {
                let _ = write!(s, "{b:02x}");
                s
            });
    println!("# Order-flow interaction study (#28, ADR-058)\n");
    println!(
        "dataset content {}, data {}, engine {}, {} {}, {} {}, code {}",
        manifest.content,
        manifest.data,
        manifest.engine,
        manifest.sampling,
        manifest.sampling_fingerprint,
        manifest.labeling,
        manifest.labeling_fingerprint,
        manifest.code
    );
    println!("queries docs/research/028-order-flow-interactions.sql sha256 {queries}");
    println!(
        "excluded {}; twin mismatches 0\n",
        if exclude.is_empty() {
            "none".to_owned()
        } else {
            format!("{exclude:?}")
        }
    );
    print!("{}", report(&inputs, &settings, &populations, &results));
}
