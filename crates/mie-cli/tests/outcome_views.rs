//! The DuckDB views of the conditional outcome dataset (#27 M2, ADR-057 D7,
//! D8) against an independent Rust model, and the `mie conditional query`
//! and `views` commands.
//!
//! The model is written from the ADR-057 D7 text, not from `views.sql`. Its
//! input is one [`MRecord`] per context record with its outcomes and
//! passages, loaded two ways: from the records and labels a fixture hands
//! the writer, and from the dataset's base views (`contexts`, `outcomes`,
//! `passages`), whose equality the fixture tests assert. From that input it
//! derives every computed column of `labeled` (the groups, location bins,
//! offset, excursion ratios, time to event, `level_within`,
//! `extreme_vs_opposite`) and every row of `outcome_summary(n_min)`, printed
//! as `OutcomeViews::query_tsv` prints values, and compares them line by
//! line with what DuckDB returns.
//!
//! - **Fixture tests** build a dataset with `OutcomeWriter` from hand-built
//!   records whose labels hit the edge cases of the D7 case tables (both
//!   location bins, levels and regime not ready, a missing reference,
//!   ties in the race millisecond with and without `same_ms`, extremes
//!   touched before, at and after the race and the window end, censored
//!   passages before and after the race, censored windows, co-emitted
//!   triggers of one family), checked against explicit expectations, plus
//!   seeded random records the model checks; and the same TSV at DuckDB 1
//!   and 4 threads.
//! - **`measure_outcome_views`** (ignored; ADR-057 D9, run in M3) opens the
//!   dataset in `MIE_OUTCOME_DATASET`, verifies it, and asserts 0 model
//!   mismatches, equal view digests at 1 and 4 threads, and the
//!   reconciliation with the counts ADR-054 and ADR-056 published for the
//!   #12 year. It is blind: it prints only those counts, the mismatch count
//!   and a row count and SHA-256 digest per view, never a share, a quantile
//!   or a per-context outcome.
//!
//! ```sh
//! MIE_OUTCOME_DATASET=<dir> cargo test --release -p mie-cli \
//!     --test outcome_views measure_outcome_views -- --ignored --nocapture
//! ```

mod common;

use common::TempDir;
use common::tapes::Lcg;
use mie_adapter_duckdb::{OutcomeViews, VIEWS, VIEWS_SQL};
use mie_adapter_parquet::outcome::{OutcomeHeader, OutcomeWriter};
use mie_cli::conditional;
use mie_domain::bars::{Coverage, Timeframe};
use mie_domain::candidate::{ContextLevel, ContextLevels, LocationContext};
use mie_domain::conditional::label::{
    Barriers, Censor, CensorReason, Excursion, ForwardLabel, HORIZONS, HorizonLabel,
    LabelProvenance, Labeling, Passage, PassageLabel, Race,
};
use mie_domain::conditional::{
    ContextProvenance, ContextRecord, Episode, EpisodeId, Sample, StateSnapshot,
};
use mie_domain::feature::{FeatureDefinition, FeatureValue, catalog};
use mie_domain::location::LevelKind;
use mie_domain::num::Price;
use mie_domain::order::{CanonicalKey, EventKind};
use mie_domain::regime::{AtrPercentile, Regime, RegimeLabel};
use mie_domain::research::DataVersion;
use mie_domain::state::MarketStateEngine;
use mie_domain::structure::{Level, Side, Sweep, SweepOutcome};
use mie_domain::time::EventTime;
use mie_domain::trigger::{Bias, TriggerEvent, TriggerEvidence, TriggerFamily};
use mie_ports::outbound::ReplayWindow;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::io::Write;
use std::path::Path;

/// 2026-10-04 00:00 UTC, the fixture's first decision.
const T0: i64 = 1_791_072_000_000;
/// The fixture's reference price, 85 000 USDT in units.
const R: i64 = 8_500_000_000_000;
/// One USDT in price units (ADR-027).
const USDT: i64 = 100_000_000;
/// The `outcome_summary` threshold of the thread-count comparison: any
/// value serves, the comparison does not read the flag.
const N_MIN: u64 = 3;

// ---------------------------------------------------------------------------
// Fixture records and labels.

fn t(millis: i64) -> EventTime {
    EventTime::from_millis(millis)
}

fn p(units: i64) -> Price {
    Price::from_units(units)
}

fn key(time: i64, seq: u64) -> CanonicalKey {
    CanonicalKey {
        time: t(time),
        kind: EventKind::Trade,
        seq,
    }
}

fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

fn ready_or_warming<T>(value: Option<T>) -> FeatureValue<T> {
    value.map_or_else(warming, FeatureValue::Ready)
}

fn snapshot(reference: Option<i64>) -> StateSnapshot {
    StateSnapshot {
        last_price: ready_or_warming(reference.map(p)),
        atr: ready_or_warming(reference.map(|_| p(400 * USDT))),
        aggression: [warming(), warming(), warming()],
        cvd_utc_day: warming(),
        effort: [warming(), warming(), warming()],
        liquidity: [warming(), warming(), warming()],
        oi_5m: warming(),
    }
}

fn context(levels: Option<&[LevelKind]>) -> LocationContext {
    LocationContext {
        anchor: p(R),
        levels: ready_or_warming(levels.map(|kinds| {
            ContextLevels {
                feature: catalog::LOCATION_LEVELS_V2.key,
                levels: kinds
                    .iter()
                    .map(|kind| ContextLevel {
                        kind: *kind,
                        source: catalog::LOCATION_LEVELS_V2.key,
                        side: None,
                        anchor: None,
                        price: p(R),
                        low: p(R - 10),
                        high: p(R + 10),
                    })
                    .collect(),
            }
        })),
        auction: warming(),
    }
}

fn bias(percentile: Option<f64>) -> Bias {
    // A fresh engine's bias, built once: a new engine per record would
    // dominate the fixture's build time.
    static FRESH: std::sync::OnceLock<Bias> = std::sync::OnceLock::new();
    let mut bias = *FRESH.get_or_init(|| Bias::read(MarketStateEngine::new().state()));
    bias.regime = ready_or_warming(percentile.map(|percentile| {
        Regime::new(
            AtrPercentile::new(percentile).unwrap(),
            catalog::VOLATILITY_REGIME_1H_V1.key,
        )
    }));
    bias
}

/// What a fixture record samples.
#[derive(Clone, Copy)]
enum Kind {
    Baseline,
    /// A trigger of `definition` in `family`, in the episode opened at
    /// `opened` (the record's own event when it opens it).
    Trigger {
        definition: &'static FeatureDefinition,
        family: TriggerFamily,
        opened: CanonicalKey,
    },
}

/// The three trigger ids of the fixture: two of one family, so the family
/// branch pools them.
const TRIGGERS: [(&FeatureDefinition, TriggerFamily); 3] = [
    (&catalog::TRIGGER_SFP_15M_V1, TriggerFamily::Sfp),
    (&catalog::TRIGGER_SFP_1H_V1, TriggerFamily::Sfp),
    (
        &catalog::TRIGGER_FAILED_AUCTION_V1,
        TriggerFamily::FailedAuction,
    ),
];

fn sfp(at: CanonicalKey) -> Sweep {
    let coverage = Coverage {
        partial_start: false,
        feed_gap: false,
    };
    Sweep {
        feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
        timeframe: Timeframe::M15,
        level: Level {
            side: Side::High,
            price: p(R),
            swing_time: t(T0 - 3_600_000),
            confirmed_bar_end: t(T0 - 2_700_000),
            known_at: t(T0 - 2_700_000),
            touches: 1,
            coverage,
        },
        time: at.time,
        price: p(R + USDT),
        extreme: p(R + 2 * USDT),
        window_bars: 1,
        outcome: SweepOutcome::Sfp,
        resolved_bar_end: Some(at.time),
        known_at: at.time,
        coverage,
    }
}

/// A context record. The writer stores only the trigger's id and family,
/// so every trigger carries the same SFP evidence.
fn record(
    event: CanonicalKey,
    kind: Kind,
    regime: Option<f64>,
    levels: Option<&[LevelKind]>,
    reference: Option<i64>,
) -> ContextRecord {
    let context = context(levels);
    let sample = match kind {
        Kind::Baseline => Sample::Baseline {
            minute_end: t(event.time.as_millis() - 1),
        },
        Kind::Trigger {
            definition,
            family,
            opened,
        } => Sample::Trigger {
            trigger: Box::new(TriggerEvent {
                feature: definition.key,
                family,
                known_at: event.time,
                inputs: definition.inputs,
                evidence: TriggerEvidence::Sfp(sfp(event)),
                context: context.clone(),
                bias: bias(None),
            }),
            episode: Episode {
                id: EpisodeId {
                    trigger: definition.key,
                    opened_at: opened,
                    ordinal: 0,
                },
                opener: opened == event,
                truncated: false,
                bridge: false,
            },
        },
    };
    ContextRecord {
        event,
        sample,
        context,
        bias: bias(regime),
        state: snapshot(reference),
    }
}

/// One horizon's race: what the fixture specifies of a `HorizonLabel`.
#[derive(Clone, Copy)]
enum R5 {
    Up(i64, bool),
    Down(i64, bool),
    Neither,
    Censored(i64),
    Unavailable,
}

/// A horizon label at `position` around `reference`: barriers 100 USDT ×
/// (position + 1) away, unless the race is unavailable; excursions `up`
/// and `down` in USDT; the window censored at `censor` (gap).
fn horizon(
    position: usize,
    reference: i64,
    race: R5,
    censor: Option<i64>,
    excursions: (i64, i64),
    clear: bool,
) -> HorizonLabel {
    let gap = |at: i64| Censor {
        at: t(at),
        reason: CensorReason::Gap,
    };
    let race = match race {
        R5::Up(at, same_ms) => Race::Upper { at: t(at), same_ms },
        R5::Down(at, same_ms) => Race::Lower { at: t(at), same_ms },
        R5::Neither => Race::Neither,
        R5::Censored(at) => Race::Censored(gap(at)),
        R5::Unavailable => Race::Unavailable,
    };
    let offset = 100 * USDT * (position as i64 + 1);
    let excursion = |usdt: i64| {
        if usdt == 0 {
            Excursion::NONE
        } else {
            Excursion {
                size: p(usdt * USDT),
                at: Some(t(T0)),
            }
        }
    };
    HorizonLabel {
        horizon: HORIZONS[position],
        clear,
        barriers: (!matches!(race, Race::Unavailable)).then(|| Barriers {
            lower: p(reference - offset),
            upper: p(reference + offset),
        }),
        race,
        up: excursion(excursions.0),
        down: excursion(excursions.1),
        last: Some(p(reference)),
        censored: match race {
            Race::Censored(censor) => Some(censor),
            _ => censor.map(gap),
        },
    }
}

/// A passage at `price` units.
#[derive(Clone, Copy)]
enum P4 {
    AtDecision,
    Touched(i64),
    NotWithin,
    Censored(i64),
}

fn passage(price: i64, passage: P4) -> PassageLabel {
    PassageLabel {
        price: p(price),
        passage: match passage {
            P4::AtDecision => Passage::AtDecision,
            P4::Touched(at) => Passage::Touched { at: t(at) },
            P4::NotWithin => Passage::NotWithin,
            P4::Censored(at) => Passage::Censored(Censor {
                at: t(at),
                reason: CensorReason::Gap,
            }),
        },
    }
}

/// A fixture: records and their labels, in record order.
#[derive(Default)]
struct Fixture {
    records: Vec<ContextRecord>,
    labels: Vec<ForwardLabel>,
}

impl Fixture {
    fn push(
        &mut self,
        record: ContextRecord,
        horizons: [HorizonLabel; 5],
        level: Option<PassageLabel>,
        extreme: Option<PassageLabel>,
    ) {
        let ordinal = match (self.records.last(), self.labels.last()) {
            (Some(last), Some(label)) if last.event == record.event => label.ordinal + 1,
            _ => 0,
        };
        self.labels.push(ForwardLabel {
            event: record.event,
            ordinal,
            reference: record.state.last_price.ready().copied(),
            atr: record.state.atr.ready().copied(),
            horizons,
            level,
            extreme,
        });
        self.records.push(record);
    }
}

/// Five horizon labels from five races around `reference`, no window
/// censor, excursions 50 up / 30 down USDT, every horizon clear.
fn races(reference: i64, races: [R5; 5]) -> [HorizonLabel; 5] {
    std::array::from_fn(|i| horizon(i, reference, races[i], None, (50, 30), true))
}

/// The hand-built records, each named for the expectations in
/// [`edge_cases_follow_the_case_tables`]. Decisions are one minute apart;
/// every passage time is relative to its record's decision `d`.
fn edge_cases(fixture: &mut Fixture) {
    use LevelKind::{StructuralHigh, Vah};
    let e1 = key(T0, 1);
    let sfp15 = Kind::Trigger {
        definition: TRIGGERS[0].0,
        family: TRIGGERS[0].1,
        opened: e1,
    };
    // B1: a baseline in both location bins, regime ready.
    fixture.push(
        record(
            e1,
            Kind::Baseline,
            Some(10.0),
            Some(&[Vah, StructuralHigh]),
            Some(R),
        ),
        races(R, [R5::Neither; 5]),
        None,
        None,
    );
    // T1 (co-emitted with B1): extreme above the reference, touched at
    // d + 20 s; level below, touched at d + 10 min.
    let d = T0;
    fixture.push(
        record(e1, sfp15, None, Some(&[]), Some(R)),
        races(
            R,
            [
                R5::Down(d + 20_000, false),
                R5::Down(d + 30_000, false),
                R5::Down(d + 10_000, false),
                R5::Up(d + 20_000, true),
                R5::Up(d + 20_000, false),
            ],
        ),
        Some(passage(R - 5 * USDT, P4::Touched(d + 600_000))),
        Some(passage(R + 5 * USDT, P4::Touched(d + 20_000))),
    );
    // T2 (co-emitted, the other SFP id): extreme below, censored at
    // d + 50 s; level above, censored at d + 15 min; the 1h window
    // censored after its race resolved.
    fixture.push(
        record(
            e1,
            Kind::Trigger {
                definition: TRIGGERS[1].0,
                family: TRIGGERS[1].1,
                opened: e1,
            },
            Some(90.0),
            None,
            Some(R),
        ),
        [
            horizon(0, R, R5::Up(d + 40_000, false), None, (50, 30), true),
            horizon(1, R, R5::Up(d + 50_000, false), None, (50, 30), true),
            horizon(
                2,
                R,
                R5::Down(d + 10_000, false),
                Some(d + 900_000),
                (50, 30),
                false,
            ),
            horizon(3, R, R5::Censored(d + 50_000), None, (50, 30), false),
            horizon(4, R, R5::Unavailable, None, (0, 0), false),
        ],
        Some(passage(R + 7 * USDT, P4::Censored(d + 900_000))),
        Some(passage(R - 6 * USDT, P4::Censored(d + 50_000))),
    );
    // B2: levels and regime not ready.
    let d = T0 + 60_000;
    let e2 = key(d, 2);
    fixture.push(
        record(e2, Kind::Baseline, None, None, Some(R)),
        races(R, [R5::Up(d + 1, false); 5]),
        None,
        None,
    );
    // T3: extreme and level at the decision price.
    fixture.push(
        record(
            e2,
            Kind::Trigger {
                definition: TRIGGERS[2].0,
                family: TRIGGERS[2].1,
                opened: e2,
            },
            Some(50.0),
            Some(&[Vah]),
            Some(R),
        ),
        races(
            R,
            [
                R5::Neither,
                R5::Unavailable,
                R5::Up(d + 5, false),
                R5::Down(d + 5, true),
                R5::Censored(d + 7),
            ],
        ),
        Some(passage(R, P4::AtDecision)),
        Some(passage(R, P4::AtDecision)),
    );
    // B3: levels ready, in neither bin.
    let d = T0 + 120_000;
    let e3 = key(d, 3);
    fixture.push(
        record(e3, Kind::Baseline, Some(70.0), Some(&[]), Some(R)),
        races(R, [R5::Down(d + 2, true); 5]),
        None,
        None,
    );
    // T4 (same episode as T1, not its opener): extreme above, touched at
    // d + 200 s.
    fixture.push(
        record(e3, sfp15, Some(70.0), Some(&[StructuralHigh]), Some(R)),
        races(
            R,
            [
                R5::Neither,
                R5::Censored(d + 100_000),
                R5::Censored(d + 300_000),
                R5::Neither,
                R5::Down(d + 200_000, false),
            ],
        ),
        Some(passage(R + 3 * USDT, P4::NotWithin)),
        Some(passage(R + 9 * USDT, P4::Touched(d + 200_000))),
    );
    // T5: extreme touched exactly at the 5m window end; level touched at
    // the 15m end.
    let d = T0 + 180_000;
    let e4 = key(d, 4);
    fixture.push(
        record(e4, sfp15, Some(30.0), Some(&[Vah]), Some(R)),
        races(
            R,
            [
                R5::Neither,
                R5::Down(d + 400_000, false),
                R5::Up(d + 400_000, false),
                R5::Neither,
                R5::Down(d + 300_000, true),
            ],
        ),
        Some(passage(R - 2 * USDT, P4::Touched(d + 900_000))),
        Some(passage(R + 4 * USDT, P4::Touched(d + 300_000))),
    );
    // T6: extreme never reached within the day.
    fixture.push(
        record(e4, sfp15, Some(30.0), Some(&[Vah]), Some(R)),
        races(
            R,
            [
                R5::Down(d + 10, false),
                R5::Neither,
                R5::Censored(d + 20),
                R5::Up(d + 30, false),
                R5::Unavailable,
            ],
        ),
        None,
        Some(passage(R + 4 * USDT, P4::NotWithin)),
    );
    // B4: no reference (before the first trade): every race unavailable.
    let d = T0 + 240_000;
    fixture.push(
        record(key(d, 5), Kind::Baseline, Some(30.0), Some(&[]), None),
        std::array::from_fn(|i| {
            let mut label = horizon(i, R, R5::Unavailable, None, (0, 0), false);
            label.last = None;
            label
        }),
        None,
        None,
    );
}

/// `events` seeded random decisions after the edge cases: each a baseline
/// (three in four), up to two co-emitted triggers, random regimes, bins,
/// races, ties, censors and passages. Times sit on a 30 s grid, so race
/// times, passage times and window ends coincide often.
fn random_records(fixture: &mut Fixture, events: usize, seed: u64) {
    let mut lcg = Lcg(seed);
    let kinds: [Option<&[LevelKind]>; 5] = [
        None,
        Some(&[]),
        Some(&[LevelKind::Vah]),
        Some(&[LevelKind::StructuralLow]),
        Some(&[LevelKind::Val, LevelKind::StructuralHigh]),
    ];
    let mut episodes = [None::<CanonicalKey>; 3];
    for i in 0..events {
        let d = T0 + 600_000 + i as i64 * 60_000;
        let event = key(d, 100 + i as u64);
        let mut kinds_here: Vec<Kind> = Vec::new();
        if lcg.below(4) != 0 {
            kinds_here.push(Kind::Baseline);
        }
        let mut first = lcg.below(3) as usize;
        for _ in 0..lcg.below(3) {
            let (definition, family) = TRIGGERS[first];
            if lcg.below(3) == 0 || episodes[first].is_none() {
                episodes[first] = Some(event);
            }
            kinds_here.push(Kind::Trigger {
                definition,
                family,
                opened: episodes[first].unwrap(),
            });
            first = (first + 1) % 3;
        }
        if kinds_here.is_empty() {
            kinds_here.push(Kind::Baseline);
        }
        for kind in kinds_here {
            let is_trigger = matches!(kind, Kind::Trigger { .. });
            let reference =
                (is_trigger || lcg.below(20) != 0).then(|| R + (lcg.below(2001) - 1000) * USDT);
            let regime = (lcg.below(5) != 0).then(|| lcg.below(100) as f64 + 0.5);
            let levels = kinds[lcg.below(5) as usize];
            let horizons: [HorizonLabel; 5] = std::array::from_fn(|position| {
                let h = HORIZONS[position].millis();
                let Some(reference) = reference else {
                    let mut label = horizon(position, R, R5::Unavailable, None, (0, 0), false);
                    label.last = None;
                    return label;
                };
                let race = match lcg.below(6) {
                    0 | 5 => R5::Up(grid(&mut lcg, d, h - 1).max(d + 1), lcg.below(5) == 0),
                    1 => R5::Down(grid(&mut lcg, d, h - 1).max(d + 1), lcg.below(5) == 0),
                    2 => R5::Neither,
                    3 => R5::Censored(grid(&mut lcg, d, h - 1)),
                    _ => R5::Unavailable,
                };
                let censor = (lcg.below(8) == 0).then(|| grid(&mut lcg, d, h - 1));
                let excursions = (lcg.below(40), lcg.below(40));
                horizon(
                    position,
                    reference,
                    race,
                    censor,
                    excursions,
                    lcg.below(3) == 0,
                )
            });
            let passage_of = |lcg: &mut Lcg| {
                let reference = reference.unwrap_or(R);
                let price = reference + (lcg.below(41) - 20) * USDT;
                let at = d + 30_000 * lcg.below(40);
                match lcg.below(5) {
                    0 => None,
                    1 => Some(passage(reference, P4::AtDecision)),
                    2 if price != reference => Some(passage(price, P4::Touched(at))),
                    3 if price != reference => Some(passage(price, P4::NotWithin)),
                    4 if price != reference => Some(passage(price, P4::Censored(at))),
                    _ => Some(passage(reference, P4::AtDecision)),
                }
            };
            let (level, extreme) = if is_trigger {
                (passage_of(&mut lcg), passage_of(&mut lcg))
            } else {
                (None, None)
            };
            fixture.push(
                record(event, kind, regime, levels, reference),
                horizons,
                level,
                extreme,
            );
        }
    }
}

/// A time in `[d, d + span]` on the 30 s grid from `d`.
fn grid(lcg: &mut Lcg, d: i64, span: i64) -> i64 {
    d + 30_000 * lcg.below(span as u64 / 30_000 + 1)
}

/// The edge cases followed by `events` random decisions.
fn fixture(events: usize) -> Fixture {
    let mut fixture = Fixture::default();
    edge_cases(&mut fixture);
    random_records(&mut fixture, events, 27);
    fixture
}

/// Writes `fixture` into the new dataset directory `dir`.
fn write_dataset(dir: &Path, fixture: &Fixture) {
    let provenance = LabelProvenance::new(
        ContextProvenance::new(
            DataVersion::from_hex(&"5d".repeat(32)).unwrap(),
            &MarketStateEngine::new(),
        ),
        Labeling::CURRENT,
    );
    let header = OutcomeHeader {
        source: "archive binance-archive/BTCUSDT streams aggTrades correction fold".to_owned(),
        replay: ReplayWindow {
            start: t(T0 - 86_400_000),
            end: t(T0 + 40 * 86_400_000),
        },
        records: ReplayWindow {
            start: t(T0),
            end: t(T0 + 40 * 86_400_000),
        },
        provenance,
        code: "unknown".to_owned(),
    };
    let mut writer = OutcomeWriter::create(dir, header).unwrap();
    for (record, label) in fixture.records.iter().zip(&fixture.labels) {
        writer.record(record).unwrap();
        writer.label(label).unwrap();
    }
    writer.finish(1_000, 0).unwrap();
}

// ---------------------------------------------------------------------------
// The model's input.

/// A passage, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MPassage {
    price: i64,
    passage: String,
    at: Option<i64>,
    censor_at: Option<i64>,
}

/// An outcome row, as stored (the fields the views read).
#[derive(Debug, Clone, PartialEq, Eq)]
struct MOutcome {
    horizon: String,
    horizon_ms: i64,
    clear: bool,
    upper: Option<i64>,
    race: String,
    race_at: Option<i64>,
    same_ms: Option<bool>,
    race_censor_at: Option<i64>,
    up: i64,
    down: i64,
    censor_reason: Option<String>,
}

/// A context record with its outcomes and passages, as stored.
#[derive(Debug, Clone, PartialEq, Eq)]
struct MRecord {
    record: u64,
    decision: i64,
    event_kind: String,
    event_seq: u64,
    trigger: Option<String>,
    family: Option<String>,
    episode: Option<String>,
    regime: Option<String>,
    levels_ready: bool,
    value_edge: Option<bool>,
    structural: Option<bool>,
    reference: Option<i64>,
    level: Option<MPassage>,
    extreme: Option<MPassage>,
    outcomes: Vec<MOutcome>,
}

const fn kind_name(kind: EventKind) -> &'static str {
    match kind {
        EventKind::FeedGap => "feed_gap",
        EventKind::BookSnapshot => "book_snapshot",
        EventKind::Trade => "trade",
        EventKind::Liquidation => "liquidation",
        EventKind::BookUpdate => "book_update",
        EventKind::MarkPrice => "mark_price",
        EventKind::FundingSettlement => "funding_settlement",
        EventKind::OpenInterest => "open_interest",
        EventKind::Kline => "kline",
    }
}

const fn regime_name(label: RegimeLabel) -> &'static str {
    match label {
        RegimeLabel::Low => "low",
        RegimeLabel::Medium => "medium",
        RegimeLabel::High => "high",
        RegimeLabel::Extreme => "extreme",
    }
}

fn m_passage(label: Option<&PassageLabel>) -> Option<MPassage> {
    let label = label?;
    let (name, at, censor_at) = match label.passage {
        Passage::AtDecision => ("at_decision", None, None),
        Passage::Touched { at } => ("touched", Some(at.as_millis()), None),
        Passage::NotWithin => ("not_within", None, None),
        Passage::Censored(censor) => ("censored", None, Some(censor.at.as_millis())),
    };
    Some(MPassage {
        price: label.price.units(),
        passage: name.to_owned(),
        at,
        censor_at,
    })
}

/// The model input from the records and labels handed to the writer.
fn from_fixture(fixture: &Fixture) -> Vec<MRecord> {
    fixture
        .records
        .iter()
        .zip(&fixture.labels)
        .enumerate()
        .map(|(number, (record, label))| {
            let (trigger, family, episode) = match &record.sample {
                Sample::Baseline { .. } => (None, None, None),
                Sample::Trigger { trigger, episode } => (
                    Some(trigger.feature.to_string()),
                    Some(trigger.family.as_str().to_owned()),
                    Some(episode.id.to_string()),
                ),
            };
            let bins = record.context.levels.ready().map(|levels| {
                let kinds: Vec<LevelKind> = levels.levels.iter().map(|l| l.kind).collect();
                (
                    kinds
                        .iter()
                        .any(|k| matches!(k, LevelKind::Vah | LevelKind::Val)),
                    kinds
                        .iter()
                        .any(|k| matches!(k, LevelKind::StructuralHigh | LevelKind::StructuralLow)),
                )
            });
            MRecord {
                record: number as u64,
                decision: record.event.time.as_millis(),
                event_kind: kind_name(record.event.kind).to_owned(),
                event_seq: record.event.seq,
                trigger,
                family,
                episode,
                regime: record
                    .bias
                    .regime
                    .ready()
                    .map(|regime| regime_name(regime.label).to_owned()),
                levels_ready: bins.is_some(),
                value_edge: bins.map(|b| b.0),
                structural: bins.map(|b| b.1),
                reference: record.state.last_price.ready().map(|price| price.units()),
                level: m_passage(label.level.as_ref()),
                extreme: m_passage(label.extreme.as_ref()),
                outcomes: label
                    .horizons
                    .iter()
                    .map(|h| {
                        let (race, race_at, same_ms, race_censor_at) = match h.race {
                            Race::Upper { at, same_ms } => {
                                ("upper", Some(at.as_millis()), Some(same_ms), None)
                            }
                            Race::Lower { at, same_ms } => {
                                ("lower", Some(at.as_millis()), Some(same_ms), None)
                            }
                            Race::Neither => ("neither", None, None, None),
                            Race::Censored(c) => ("censored", None, None, Some(c.at.as_millis())),
                            Race::Unavailable => ("unavailable", None, None, None),
                        };
                        MOutcome {
                            horizon: h.horizon.label().to_owned(),
                            horizon_ms: h.horizon.millis(),
                            clear: h.clear,
                            upper: h.barriers.map(|b| b.upper.units()),
                            race: race.to_owned(),
                            race_at,
                            same_ms,
                            race_censor_at,
                            up: h.up.size.units(),
                            down: h.down.size.units(),
                            censor_reason: h.censored.map(|c| c.reason.as_str().to_owned()),
                        }
                    })
                    .collect(),
            }
        })
        .collect()
}

/// A `Write` that hands every complete line to `f`.
struct Lines<F: FnMut(&str)> {
    buffer: Vec<u8>,
    f: F,
}

impl<F: FnMut(&str)> Lines<F> {
    fn new(f: F) -> Self {
        Self {
            buffer: Vec::new(),
            f,
        }
    }
}

impl<F: FnMut(&str)> Write for Lines<F> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.buffer.extend_from_slice(bytes);
        let mut start = 0;
        while let Some(end) = self.buffer[start..].iter().position(|b| *b == b'\n') {
            let line = std::str::from_utf8(&self.buffer[start..start + end]).unwrap();
            (self.f)(line);
            start += end + 1;
        }
        self.buffer.drain(..start);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Calls `f` with the header and each row of `view`'s TSV, as named cells.
fn each_row(
    views: &OutcomeViews,
    view: &str,
    n_min: Option<u64>,
    mut f: impl FnMut(&dyn Fn(&str) -> String),
) {
    let mut header: Option<BTreeMap<String, usize>> = None;
    let mut sink = Lines::new(|line: &str| match &header {
        None => {
            header = Some(
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
                    .unwrap_or_else(|| panic!("{view} has no column {name}"));
                cells[*index].to_owned()
            };
            f(&get);
        }
    });
    views.query_tsv(view, n_min, &mut sink).unwrap();
}

fn opt(cell: String) -> Option<String> {
    (cell != "NULL").then_some(cell)
}

fn opt_int(cell: String) -> Option<i64> {
    opt(cell).map(|c| c.parse().unwrap())
}

fn opt_bool(cell: String) -> Option<bool> {
    opt(cell).map(|c| c.parse().unwrap())
}

/// A printed decimal of scale 8 in units.
fn units(cell: &str) -> i64 {
    let (negative, digits) = cell.strip_prefix('-').map_or((false, cell), |d| (true, d));
    let (whole, fraction) = digits.split_once('.').unwrap();
    assert_eq!(fraction.len(), 8, "{cell}");
    let value = whole.parse::<i64>().unwrap() * USDT + fraction.parse::<i64>().unwrap();
    if negative { -value } else { value }
}

fn opt_units(cell: String) -> Option<i64> {
    opt(cell).map(|c| units(&c))
}

/// The model input read back through the base views.
fn from_views(views: &OutcomeViews) -> Vec<MRecord> {
    let mut records: Vec<MRecord> = Vec::new();
    each_row(views, "contexts", None, |get| {
        records.push(MRecord {
            record: get("record").parse().unwrap(),
            decision: get("decision_time").parse().unwrap(),
            event_kind: get("event_kind"),
            event_seq: get("event_seq").parse().unwrap(),
            trigger: opt(get("trigger")),
            family: opt(get("trigger_family")),
            episode: opt(get("episode")),
            regime: opt(get("regime")),
            levels_ready: get("levels_ready").parse().unwrap(),
            value_edge: opt_bool(get("at_value_edge")),
            structural: opt_bool(get("at_structural_extreme")),
            reference: opt_units(get("reference")),
            level: None,
            extreme: None,
            outcomes: Vec::new(),
        });
    });
    for (number, record) in records.iter().enumerate() {
        assert_eq!(
            record.record, number as u64,
            "contexts are numbered in order"
        );
    }
    each_row(views, "outcomes", None, |get| {
        let record: usize = get("record").parse().unwrap();
        records[record].outcomes.push(MOutcome {
            horizon: get("horizon"),
            horizon_ms: get("horizon_ms").parse().unwrap(),
            clear: get("clear").parse().unwrap(),
            upper: opt_units(get("upper")),
            race: get("race"),
            race_at: opt_int(get("race_at")),
            same_ms: opt_bool(get("same_ms")),
            race_censor_at: opt_int(get("race_censor_at")),
            up: units(&get("up")),
            down: units(&get("down")),
            censor_reason: opt(get("censor_reason")),
        });
    });
    each_row(views, "passages", None, |get| {
        let record: usize = get("record").parse().unwrap();
        let passage = Some(MPassage {
            price: units(&get("price")),
            passage: get("passage"),
            at: opt_int(get("at")),
            censor_at: opt_int(get("censor_at")),
        });
        match get("target").as_str() {
            "level" => records[record].level = passage,
            "extreme" => records[record].extreme = passage,
            other => panic!("unknown passage target {other}"),
        }
    });
    records
}

// ---------------------------------------------------------------------------
// The model (ADR-057 D7).

/// The printed decimal of `units` at scale 8.
fn decimal(units: i64) -> String {
    let sign = if units < 0 { "-" } else { "" };
    let magnitude = units.unsigned_abs();
    format!(
        "{sign}{}.{:08}",
        magnitude / 100_000_000,
        magnitude % 100_000_000
    )
}

fn cell<T: ToString>(value: Option<T>) -> String {
    value.map_or_else(|| "NULL".to_owned(), |v| v.to_string())
}

/// The computed columns of one `labeled` row.
#[derive(Debug, Clone, PartialEq)]
struct Derived {
    trigger: String,
    family: String,
    regime: String,
    bins: Vec<&'static str>,
    offset: Option<i64>,
    up_x: Option<f64>,
    down_x: Option<f64>,
    time_to_event: Option<i64>,
    level_within: Option<&'static str>,
    extreme_vs_opposite: Option<&'static str>,
}

/// The ADR-051 R1 bins of a record.
fn bins(record: &MRecord) -> Vec<&'static str> {
    if !record.levels_ready {
        return vec!["not_ready"];
    }
    let mut bins = Vec::new();
    if record.value_edge == Some(true) {
        bins.push("value_edge");
    }
    if record.structural == Some(true) {
        bins.push("structural_extreme");
    }
    if bins.is_empty() {
        bins.push("neither");
    }
    bins
}

/// `level_within` against the window end `end` (D7).
fn level_within(level: Option<&MPassage>, end: i64) -> Option<&'static str> {
    let level = level?;
    Some(match level.passage.as_str() {
        "at_decision" => "at_decision",
        "touched" if level.at.unwrap() < end => "touched",
        "touched" | "not_within" => "not_within",
        "censored" if level.censor_at.unwrap() >= end => "not_within",
        "censored" => "unknown",
        other => panic!("unknown passage {other}"),
    })
}

/// `extreme_vs_opposite` of one horizon (the D7 case table).
fn extreme_vs_opposite(record: &MRecord, outcome: &MOutcome, end: i64) -> Option<&'static str> {
    let extreme = record.extreme.as_ref()?;
    if outcome.race == "unavailable" {
        return None;
    }
    if extreme.passage == "at_decision" {
        return Some("extreme_first");
    }
    let touched = (extreme.passage == "touched").then(|| extreme.at.unwrap());
    let censored = (extreme.passage == "censored").then(|| extreme.censor_at.unwrap());
    Some(match outcome.race.as_str() {
        "neither" => match (touched, censored) {
            (Some(at), _) if at < end => "extreme_first",
            (_, Some(at)) if at < end => "unknown",
            _ => "neither",
        },
        "censored" => match touched {
            Some(at) if at < outcome.race_censor_at.unwrap() => "extreme_first",
            _ => "unknown",
        },
        race @ ("upper" | "lower") => {
            let won = outcome.race_at.unwrap();
            let extreme_above = extreme.price > record.reference.unwrap();
            if (race == "upper") == extreme_above {
                match touched {
                    Some(at) if at < won => "extreme_first",
                    Some(at) if at == won && outcome.same_ms == Some(false) => "extreme_first",
                    _ => "undetermined",
                }
            } else {
                match (touched, censored) {
                    (Some(at), _) if at < won => "extreme_first",
                    (Some(at), _) if at == won => "undetermined",
                    (_, Some(at)) if at <= won => "unknown",
                    _ => "opposite_first",
                }
            }
        }
        other => panic!("unknown race {other}"),
    })
}

fn derive(record: &MRecord, outcome: &MOutcome) -> Derived {
    let end = record.decision + outcome.horizon_ms;
    let offset = outcome.upper.zip(record.reference).map(|(u, r)| u - r);
    let ratio_of = |size: i64| {
        offset
            .filter(|_| outcome.race != "unavailable" && outcome.censor_reason.is_none())
            .map(|offset| size as f64 / offset as f64)
    };
    Derived {
        trigger: record
            .trigger
            .clone()
            .unwrap_or_else(|| "baseline".to_owned()),
        family: record
            .family
            .clone()
            .unwrap_or_else(|| "baseline".to_owned()),
        regime: record
            .regime
            .clone()
            .unwrap_or_else(|| "not_ready".to_owned()),
        bins: bins(record),
        offset,
        up_x: ratio_of(outcome.up),
        down_x: ratio_of(outcome.down),
        time_to_event: matches!(outcome.race.as_str(), "upper" | "lower")
            .then(|| outcome.race_at.unwrap() - record.decision),
        level_within: level_within(record.level.as_ref(), end),
        extreme_vs_opposite: extreme_vs_opposite(record, outcome, end),
    }
}

/// The `labeled` columns the model checks.
const LABELED_CHECKED: [&str; 12] = [
    "record",
    "horizon",
    "group_trigger",
    "group_family",
    "group_regime",
    "location_bins",
    "offset",
    "up_x",
    "down_x",
    "time_to_event_ms",
    "level_within",
    "extreme_vs_opposite",
];

/// The model's `labeled` line for the checked columns.
fn labeled_line(record: &MRecord, outcome: &MOutcome) -> String {
    let d = derive(record, outcome);
    [
        record.record.to_string(),
        outcome.horizon.clone(),
        d.trigger,
        d.family,
        d.regime,
        format!("[{}]", d.bins.join(",")),
        cell(d.offset.map(decimal)),
        cell(d.up_x),
        cell(d.down_x),
        cell(d.time_to_event),
        cell(d.level_within),
        cell(d.extreme_vs_opposite),
    ]
    .join("\t")
}

/// One group's accumulators.
#[derive(Default)]
struct Group {
    samples: u64,
    events: BTreeSet<(i64, String, u64)>,
    episodes: BTreeSet<String>,
    clear: u64,
    races: BTreeMap<String, u64>,
    same_ms: u64,
    tte: Vec<i64>,
    up_x: Vec<f64>,
    down_x: Vec<f64>,
    level: BTreeMap<&'static str, u64>,
    extreme: BTreeMap<&'static str, u64>,
}

/// The nearest-rank quantile `q` of the sorted `values`.
fn rank<T: Copy>(sorted: &[T], q: f64) -> Option<T> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
    Some(sorted[rank - 1])
}

fn five<T: Copy + ToString>(sorted: &[T]) -> Vec<String> {
    vec![
        sorted.len().to_string(),
        cell(sorted.first().copied()),
        cell(rank(sorted, 0.25)),
        cell(rank(sorted, 0.5)),
        cell(rank(sorted, 0.75)),
        cell(sorted.last().copied()),
    ]
}

/// The `outcome_summary` header, pinned.
const SUMMARY_COLUMNS: [&str; 48] = [
    "horizon",
    "horizon_ms",
    "split",
    "trigger",
    "family",
    "regime",
    "location",
    "samples",
    "events",
    "episodes",
    "clear",
    "meets_min_sample",
    "n_upper",
    "n_lower",
    "n_neither",
    "n_censored",
    "n_unavailable",
    "n_same_ms",
    "share_upper",
    "share_lower",
    "share_neither",
    "tte_n",
    "tte_min",
    "tte_p25",
    "tte_p50",
    "tte_p75",
    "tte_max",
    "up_x_n",
    "up_x_min",
    "up_x_p25",
    "up_x_p50",
    "up_x_p75",
    "up_x_max",
    "down_x_n",
    "down_x_min",
    "down_x_p25",
    "down_x_p50",
    "down_x_p75",
    "down_x_max",
    "level_at_decision",
    "level_touched",
    "level_not_within",
    "level_unknown",
    "extreme_first",
    "opposite_first",
    "extreme_neither",
    "extreme_undetermined",
    "extreme_unknown",
];

/// The model's `outcome_summary(n_min)`: header and rows, in the view's
/// order.
fn summary_lines(records: &[MRecord], n_min: u64) -> Vec<String> {
    // Key: horizon_ms, split, trigger, family, regime, location, horizon.
    type Key = (i64, String, String, String, String, String, String);
    let mut groups: BTreeMap<Key, Group> = BTreeMap::new();
    // Key: horizon_ms, family, horizon; value: samples and decisions.
    type Families = BTreeMap<(i64, String, String), (u64, BTreeSet<(i64, String, u64)>)>;
    let mut families = Families::new();
    for record in records {
        for outcome in &record.outcomes {
            let d = derive(record, outcome);
            let event = (record.decision, record.event_kind.clone(), record.event_seq);
            let locations: Vec<&str> = std::iter::once("all")
                .chain(d.bins.iter().copied())
                .collect();
            for location in locations {
                for (by_trigger, by_regime) in
                    [(true, true), (true, false), (false, true), (false, false)]
                {
                    let split: Vec<&str> = [
                        by_trigger.then_some("trigger"),
                        by_regime.then_some("regime"),
                        (location != "all").then_some("location"),
                    ]
                    .into_iter()
                    .flatten()
                    .collect();
                    let split = if split.is_empty() {
                        "all".to_owned()
                    } else {
                        split.join(",")
                    };
                    let all = || "all".to_owned();
                    let key = (
                        outcome.horizon_ms,
                        split,
                        if by_trigger { d.trigger.clone() } else { all() },
                        if by_trigger { d.family.clone() } else { all() },
                        if by_regime { d.regime.clone() } else { all() },
                        location.to_owned(),
                        outcome.horizon.clone(),
                    );
                    let group = groups.entry(key).or_default();
                    group.samples += 1;
                    group.events.insert(event.clone());
                    if let Some(episode) = &record.episode {
                        group.episodes.insert(episode.clone());
                    }
                    group.clear += u64::from(outcome.clear);
                    *group.races.entry(outcome.race.clone()).or_default() += 1;
                    group.same_ms += u64::from(outcome.same_ms == Some(true));
                    group.tte.extend(d.time_to_event);
                    group.up_x.extend(d.up_x);
                    group.down_x.extend(d.down_x);
                    if let Some(level) = d.level_within {
                        *group.level.entry(level).or_default() += 1;
                    }
                    if let Some(extreme) = d.extreme_vs_opposite {
                        *group.extreme.entry(extreme).or_default() += 1;
                    }
                }
            }
            if record.trigger.is_some() {
                let family = families
                    .entry((
                        outcome.horizon_ms,
                        d.family.clone(),
                        outcome.horizon.clone(),
                    ))
                    .or_default();
                family.0 += 1;
                family.1.insert(event);
            }
        }
    }
    let mut rows: Vec<(Key, Vec<String>)> = Vec::new();
    for (key, mut group) in groups {
        group.tte.sort_unstable();
        group.up_x.sort_by(f64::total_cmp);
        group.down_x.sort_by(f64::total_cmp);
        let race = |name: &str| group.races.get(name).copied().unwrap_or(0);
        let ran = race("upper") + race("lower") + race("neither");
        let share = |n: u64| cell((ran > 0).then(|| n as f64 / ran as f64));
        let mut cells = vec![
            key.6.clone(),
            key.0.to_string(),
            key.1.clone(),
            key.2.clone(),
            key.3.clone(),
            key.4.clone(),
            key.5.clone(),
            group.samples.to_string(),
            group.events.len().to_string(),
            group.episodes.len().to_string(),
            group.clear.to_string(),
            (group.clear >= n_min).to_string(),
            race("upper").to_string(),
            race("lower").to_string(),
            race("neither").to_string(),
            race("censored").to_string(),
            race("unavailable").to_string(),
            group.same_ms.to_string(),
            share(race("upper")),
            share(race("lower")),
            share(race("neither")),
        ];
        cells.extend(five(&group.tte));
        cells.extend(five(&group.up_x));
        cells.extend(five(&group.down_x));
        for name in ["at_decision", "touched", "not_within", "unknown"] {
            cells.push(group.level.get(name).copied().unwrap_or(0).to_string());
        }
        for name in [
            "extreme_first",
            "opposite_first",
            "neither",
            "undetermined",
            "unknown",
        ] {
            cells.push(group.extreme.get(name).copied().unwrap_or(0).to_string());
        }
        rows.push((key, cells));
    }
    for ((horizon_ms, family, horizon), (samples, events)) in families {
        let key = (
            horizon_ms,
            "family".to_owned(),
            "all".to_owned(),
            family.clone(),
            "all".to_owned(),
            "all".to_owned(),
            horizon.clone(),
        );
        let mut cells = vec![
            horizon,
            horizon_ms.to_string(),
            "family".to_owned(),
            "all".to_owned(),
            family,
            "all".to_owned(),
            "all".to_owned(),
            samples.to_string(),
            events.len().to_string(),
        ];
        cells.resize(SUMMARY_COLUMNS.len(), "NULL".to_owned());
        rows.push((key, cells));
    }
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let mut lines = vec![SUMMARY_COLUMNS.join("\t")];
    lines.extend(rows.into_iter().map(|(_, cells)| cells.join("\t")));
    lines
}

/// Mismatches between DuckDB's views and the model: the checked `labeled`
/// columns row by row, and every `outcome_summary(n_min)` line. Prints the
/// first few (fixture data only; the measurement passes `quiet`).
fn mismatches(views: &OutcomeViews, records: &[MRecord], n_min: u64, quiet: bool) -> u64 {
    let mut expected = records
        .iter()
        .flat_map(|record| record.outcomes.iter().map(move |o| labeled_line(record, o)));
    let mut count = 0u64;
    let mut report = |what: &str, got: &str, want: &str| {
        count += 1;
        if !quiet && count <= 5 {
            println!("{what} mismatch:\n  views {got}\n  model {want}");
        }
    };
    each_row(views, "labeled", None, |get| {
        let got = LABELED_CHECKED.map(get).join("\t");
        let want = expected.next().unwrap_or_default();
        if got != want {
            report("labeled", &got, &want);
        }
    });
    for extra in expected {
        report("labeled", "(missing)", &extra);
    }
    let want = summary_lines(records, n_min);
    let mut got = Vec::new();
    views
        .query_tsv("outcome_summary", Some(n_min), &mut got)
        .unwrap();
    let got = String::from_utf8(got).unwrap();
    let got: Vec<&str> = got.lines().collect();
    for index in 0..got.len().max(want.len()) {
        let (g, w) = (
            got.get(index).copied().unwrap_or("(missing)"),
            want.get(index).map_or("(missing)", String::as_str),
        );
        if g != w {
            report("outcome_summary", g, w);
        }
    }
    count
}

/// The TSV of `view` at `threads` DuckDB threads.
fn tsv(views: &OutcomeViews, view: &str, n_min: Option<u64>, threads: u32) -> String {
    views.set_threads(threads).unwrap();
    let mut out = Vec::new();
    views.query_tsv(view, n_min, &mut out).unwrap();
    String::from_utf8(out).unwrap()
}

// ---------------------------------------------------------------------------
// Fixture tests.

/// A built fixture dataset: its directory and the fixture.
struct Built {
    _temp: TempDir,
    dir: std::path::PathBuf,
    fixture: Fixture,
}

fn built(events: usize) -> Built {
    let temp = TempDir::new("outcome-views");
    let dir = temp.path().join("dataset");
    let fixture = fixture(events);
    write_dataset(&dir, &fixture);
    let mut report = Vec::new();
    assert!(conditional::verify(&dir, &mut report).unwrap());
    Built {
        _temp: temp,
        dir,
        fixture,
    }
}

#[test]
fn views_match_the_independent_model() {
    // 8 000 decisions give more than 10 000 records (asserted) and five
    // labeled rows each: every case-table branch many times over. The
    // model runs unoptimized in the test profile, so this size bounds its
    // run time.
    let built = built(8_000);
    let views = OutcomeViews::open(&built.dir).unwrap();
    let model = from_fixture(&built.fixture);
    assert!(model.len() > 10_000, "{}", model.len());
    assert_eq!(from_views(&views), model, "the base views hold the input");
    // The threshold is the median clear count of the groups, so the flag
    // is checked on both sides of it.
    let clear = SUMMARY_COLUMNS.iter().position(|c| *c == "clear").unwrap();
    let mut clears: Vec<u64> = summary_lines(&model, 0)
        .iter()
        .skip(1)
        .filter_map(|line| line.split('\t').nth(clear)?.parse().ok())
        .collect();
    clears.sort_unstable();
    let n_min = clears[clears.len() / 2];
    assert!(clears[0] < n_min, "{clears:?}");
    assert_eq!(mismatches(&views, &model, n_min, false), 0);
}

#[test]
fn views_are_identical_at_one_and_four_threads() {
    // 16 000 decisions give more than 24 576 records, so more than 122 880
    // outcome rows (asserted): two Parquet row groups, so DuckDB scans
    // `outcomes` on two threads.
    let built = built(16_000);
    let outcomes = 5 * built.fixture.records.len();
    assert!(outcomes > 122_880, "{outcomes}");
    let views = OutcomeViews::open(&built.dir).unwrap();
    for (view, n_min) in VIEWS.map(|(view, _)| (view, (view == "outcome_summary").then_some(N_MIN)))
    {
        assert_eq!(
            tsv(&views, view, n_min, 1),
            tsv(&views, view, n_min, 4),
            "{view}"
        );
    }
}

/// `level_within` and `extreme_vs_opposite` of each hand-built record per
/// horizon (5m, 15m, 1h, 4h, 1d), from the D7 case tables.
#[test]
fn edge_cases_follow_the_case_tables() {
    let built = built(0);
    let views = OutcomeViews::open(&built.dir).unwrap();
    let mut got: BTreeMap<u64, Vec<(String, String, String, String)>> = BTreeMap::new();
    each_row(&views, "labeled", None, |get| {
        got.entry(get("record").parse().unwrap())
            .or_default()
            .push((
                get("level_within"),
                get("extreme_vs_opposite"),
                get("up_x"),
                get("location_bins"),
            ));
    });
    let column = |record: u64, pick: fn(&(String, String, String, String)) -> &String| {
        got[&record].iter().map(pick).cloned().collect::<Vec<_>>()
    };
    let level = |record| column(record, |row| &row.0);
    let extreme = |record| column(record, |row| &row.1);
    let s = |cells: [&str; 5]| cells.map(str::to_owned).to_vec();
    // T1: the opposite barrier won at the extreme's ms (undetermined),
    // after it (extreme first), before it (opposite first); the extreme's
    // side won in the same ms with and without same_ms.
    assert_eq!(
        extreme(1),
        s([
            "undetermined",
            "extreme_first",
            "opposite_first",
            "undetermined",
            "extreme_first"
        ])
    );
    // T1's level is touched at d + 10 min: not within 5m, within the rest.
    assert_eq!(
        level(1),
        s(["not_within", "touched", "touched", "touched", "touched"])
    );
    // T2: the extreme's censor after the opposite win (opposite first) and
    // at it (unknown); the extreme's side won without a touch
    // (undetermined); a censored race without a touch (unknown); an
    // unavailable race (NULL).
    assert_eq!(
        extreme(2),
        s([
            "opposite_first",
            "unknown",
            "undetermined",
            "unknown",
            "NULL"
        ])
    );
    // T2's level is censored at the 15m end: not within 5m and 15m,
    // unknown beyond.
    assert_eq!(
        level(2),
        s(["not_within", "not_within", "unknown", "unknown", "unknown"])
    );
    // T2's 1h window is censored after its race: no excursion ratio.
    assert_eq!(got[&2][2].2, "NULL");
    assert_ne!(got[&2][0].2, "NULL");
    // T3: at the decision price, except where the race is unavailable.
    assert_eq!(
        extreme(4),
        s([
            "extreme_first",
            "NULL",
            "extreme_first",
            "extreme_first",
            "extreme_first"
        ])
    );
    assert_eq!(level(4), s(["at_decision"; 5]));
    // T4: touched before the 5m end in a neither race; a race censored
    // before and after the touch; the opposite won at the touch's ms.
    assert_eq!(
        extreme(6),
        s([
            "extreme_first",
            "unknown",
            "extreme_first",
            "extreme_first",
            "undetermined"
        ])
    );
    assert_eq!(level(6), s(["not_within"; 5]));
    // T5: touched exactly at the 5m end (neither); the opposite won after
    // the touch; the extreme's side won after the touch.
    assert_eq!(
        extreme(7),
        s([
            "neither",
            "extreme_first",
            "extreme_first",
            "extreme_first",
            "undetermined"
        ])
    );
    // T5's level is touched exactly at the 15m end.
    assert_eq!(
        level(7),
        s(["not_within", "not_within", "touched", "touched", "touched"])
    );
    // T6: never reached: the opposite first, neither, a censored race
    // unknown, the extreme's side undetermined, unavailable NULL.
    assert_eq!(
        extreme(8),
        s([
            "opposite_first",
            "neither",
            "unknown",
            "undetermined",
            "NULL"
        ])
    );
    assert_eq!(level(8), s(["NULL"; 5]));
    // Baselines carry no passages; the bins of B1 (both), B2 (not ready),
    // B3 (neither), and B4 has no ratio (no reference, no race).
    assert_eq!(extreme(0), s(["NULL"; 5]));
    assert_eq!(got[&0][0].3, "[value_edge,structural_extreme]");
    assert_eq!(got[&3][0].3, "[not_ready]");
    assert_eq!(got[&5][0].3, "[neither]");
    assert_eq!(got[&9][0].2, "NULL");

    // The location sets count B1 in both bins; the others count it once.
    let model = from_fixture(&built.fixture);
    assert_eq!(mismatches(&views, &model, 1, false), 0);
    let mut rows = Vec::new();
    each_row(&views, "outcome_summary", Some(1), |get| {
        if get("horizon") == "5m" && get("trigger") == "all" && get("regime") == "all" {
            rows.push((
                get("split"),
                get("family"),
                get("location"),
                get("samples"),
                get("events"),
            ));
        }
    });
    let row = |split: &str, family: &str, location: &str| {
        rows.iter()
            .find(|r| r.0 == split && r.1 == family && r.2 == location)
            .map(|r| (r.3.clone(), r.4.clone()))
            .unwrap()
    };
    // Ten records on five decisions.
    assert_eq!(row("all", "all", "all"), ("10".to_owned(), "5".to_owned()));
    // value_edge holds B1, T3, T5, T6; structural_extreme holds B1, T4.
    assert_eq!(row("location", "all", "value_edge").0, "4");
    assert_eq!(row("location", "all", "structural_extreme").0, "2");
    // The SFP family pools T1, T2 (co-emitted on e1), T4, T5, T6: five
    // samples on three decisions.
    assert_eq!(
        row("family", "sfp", "all"),
        ("5".to_owned(), "3".to_owned())
    );
}

/// Every brief §12 output maps to a column, and the `labeled` and
/// `outcome_summary` columns are pinned.
#[test]
fn the_columns_carry_the_brief_section_12_outputs() {
    let built = built(0);
    let views = OutcomeViews::open(&built.dir).unwrap();
    let header = |view: &str, n_min: Option<u64>| {
        tsv(&views, view, n_min, 1)
            .lines()
            .next()
            .unwrap()
            .split('\t')
            .map(str::to_owned)
            .collect::<Vec<_>>()
    };
    let labeled = header("labeled", None);
    assert_eq!(
        labeled,
        [
            "record",
            "decision_time",
            "event_kind",
            "event_seq",
            "ordinal",
            "sample",
            "minute_end",
            "trigger",
            "trigger_family",
            "episode",
            "opener",
            "truncated",
            "bridge",
            "regime",
            "regime_percentile",
            "levels_ready",
            "at_value_edge",
            "at_structural_extreme",
            "auction_state",
            "anchor",
            "reference",
            "atr_1h",
            "horizon",
            "horizon_ms",
            "clear",
            "lower",
            "upper",
            "race",
            "race_at",
            "same_ms",
            "race_censor_at",
            "race_censor_reason",
            "up",
            "up_at",
            "down",
            "down_at",
            "last",
            "censor_at",
            "censor_reason",
            "level_price",
            "level_passage",
            "extreme_price",
            "extreme_passage",
            "group_trigger",
            "group_family",
            "group_regime",
            "location_bins",
            "offset",
            "up_x",
            "down_x",
            "time_to_event_ms",
            "level_within",
            "extreme_vs_opposite",
        ]
        .map(str::to_owned)
    );
    let summary = header("outcome_summary", Some(1));
    assert_eq!(summary, SUMMARY_COLUMNS.map(str::to_owned));
    // Brief §12, output by output (ADR-057 D7).
    let section_12: [(&str, &[&str]); 8] = [
        (
            "upside/downside threshold first",
            &["race", "n_upper", "n_lower", "share_upper"],
        ),
        ("MFE / MAE", &["up", "down", "up_x", "down_x"]),
        ("time-to-event", &["time_to_event_ms", "tte_p50"]),
        (
            "level revisit probability",
            &["level_within", "level_touched"],
        ),
        (
            "invalidation probability",
            &["extreme_vs_opposite", "opposite_first"],
        ),
        (
            "excursion distribution",
            &["up_x_p25", "up_x_p75", "down_x_min", "down_x_max"],
        ),
        ("outcome conditional on volatility regime", &["regime"]),
        (
            "outcome conditional on location and trigger",
            &["location", "trigger", "family"],
        ),
    ];
    for (output, columns) in section_12 {
        for column in columns {
            assert!(
                labeled.iter().any(|c| c == column) || summary.iter().any(|c| c == column),
                "§12 {output}: no column {column}"
            );
        }
    }
    assert!(summary.iter().any(|c| c == "meets_min_sample"));
}

#[test]
fn query_verifies_first_and_views_prints_the_sql() {
    let built = built(0);
    let mut out = Vec::new();
    assert!(conditional::query(&built.dir, "contexts", None, &mut out).unwrap());
    let text = String::from_utf8(out).unwrap();
    assert_eq!(text.lines().count(), 1 + built.fixture.records.len());
    assert!(text.starts_with("record\tdecision_time\t"));

    let mut out = Vec::new();
    assert!(!conditional::query(&built.dir, "outcome_summary", None, &mut out).unwrap());
    assert!(String::from_utf8(out).unwrap().starts_with("FAIL: "));
    let mut out = Vec::new();
    assert!(!conditional::query(&built.dir, "nope", None, &mut out).unwrap());

    // A tampered table fails verification before DuckDB reads it.
    let path = built.dir.join("passages.parquet");
    let mut bytes = std::fs::read(&path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 1;
    let mut permissions = std::fs::metadata(&path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    std::fs::set_permissions(&path, permissions).unwrap();
    std::fs::write(&path, bytes).unwrap();
    let mut out = Vec::new();
    assert!(!conditional::query(&built.dir, "contexts", None, &mut out).unwrap());
    let text = String::from_utf8(out).unwrap();
    assert!(
        text.starts_with("FAIL: ") && text.contains("integrity"),
        "{text}"
    );

    let mut out = Vec::new();
    conditional::views(&mut out).unwrap();
    assert_eq!(out, VIEWS_SQL.as_bytes());
}

/// DuckDB rewrites a leading `~` to the home directory and strips a
/// `file:/` prefix; Rust takes both literally. `mie conditional query`
/// must read the directory it verified, never the rewritten one: each case
/// puts the verified dataset at the literal relative path and a decoy with
/// more records where DuckDB's rewrite would point.
#[test]
fn query_reads_the_verified_directory_not_a_rewritten_one() {
    let temp = TempDir::new("rewrite");
    let cwd = temp.path().join("cwd");
    let home = temp.path().join("home");
    let verified = fixture(0);
    let decoy = fixture(3);
    assert_ne!(verified.records.len(), decoy.records.len());
    let rows = |arg: &str| {
        let output = std::process::Command::new(env!("CARGO_BIN_EXE_mie"))
            .args(["conditional", "query", arg, "contexts"])
            .current_dir(&cwd)
            .env("HOME", &home)
            .output()
            .unwrap();
        let stdout = String::from_utf8(output.stdout).unwrap();
        assert!(output.status.success(), "{arg}: {stdout}");
        stdout.lines().count() - 1
    };
    // `~/ds`: Rust reads `<cwd>/~/ds`, DuckDB would read `$HOME/ds`.
    std::fs::create_dir_all(cwd.join("~")).unwrap();
    std::fs::create_dir_all(&home).unwrap();
    write_dataset(&cwd.join("~").join("ds"), &verified);
    write_dataset(&home.join("ds"), &decoy);
    assert_eq!(rows("~/ds"), verified.records.len());
    // `file:<abs>`: Rust reads `<cwd>/file:<abs>`, DuckDB would read `<abs>`.
    let target = temp.path().join("abs");
    write_dataset(&target, &decoy);
    let relative = target.strip_prefix("/").unwrap();
    std::fs::create_dir_all(cwd.join("file:").join(relative.parent().unwrap())).unwrap();
    write_dataset(&cwd.join("file:").join(relative), &verified);
    assert_eq!(
        rows(&format!("file:{}", target.display())),
        verified.records.len()
    );
}

// ---------------------------------------------------------------------------
// The M3 measurement (ADR-057 D9).

/// The published counts of the #12 year (ADR-054 *Acceptance*, ADR-056
/// *Measurement*): contexts, baseline, trigger.
const YEAR_CONTEXTS: (u64, u64, u64) = (561_695, 525_599, 36_096);

/// Per horizon: `neither`, `unavailable`, windows censored `end_of_data`,
/// clear baseline, clear trigger (ADR-056 *Measurement*, the year table).
const YEAR_HORIZONS: [(&str, [u64; 5]); 5] = [
    ("5m", [43_925, 74, 5, 94_333, 25_519]),
    ("15m", [44_593, 224, 17, 33_758, 22_828]),
    ("1h", [56_372, 899, 62, 8_678, 16_654]),
    ("4h", [68_977, 3_670, 245, 2_185, 9_815]),
    ("1d", [56_383, 22_724, 1_548, 365, 3_604]),
];

/// A `Write` that counts lines and hashes bytes.
#[derive(Default)]
struct Digest256 {
    hasher: Sha256,
    lines: u64,
}

impl Write for Digest256 {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.hasher.update(bytes);
        self.lines += bytes.iter().filter(|b| **b == b'\n').count() as u64;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[test]
#[ignore = "measures the views on a built dataset for ADR-057 D9; see the module docs"]
fn measure_outcome_views() {
    let dir = std::path::PathBuf::from(
        std::env::var("MIE_OUTCOME_DATASET").expect("set MIE_OUTCOME_DATASET"),
    );
    let mut report = Vec::new();
    assert!(
        conditional::verify(&dir, &mut report).unwrap(),
        "{}",
        String::from_utf8_lossy(&report)
    );
    let views = OutcomeViews::open(&dir).unwrap();
    let model = from_views(&views);

    // Any threshold serves the digests and the model comparison: they
    // compare thread counts and the model, and nothing reads the flag.
    let n_min = 1;
    let mut digests: Vec<Vec<(String, u64, String)>> = Vec::new();
    for threads in [1, 4] {
        views.set_threads(threads).unwrap();
        let mut row = Vec::new();
        for (view, _) in VIEWS {
            let mut digest = Digest256::default();
            views
                .query_tsv(
                    view,
                    (view == "outcome_summary").then_some(n_min),
                    &mut digest,
                )
                .unwrap();
            let hex: String = digest
                .hasher
                .finalize()
                .iter()
                .fold(String::new(), |mut s, b| {
                    let _ = write!(s, "{b:02x}");
                    s
                });
            row.push((view.to_owned(), digest.lines - 1, hex));
        }
        digests.push(row);
    }
    views.set_threads(4).unwrap();
    let mismatched = mismatches(&views, &model, n_min, true);

    // Reconciliation, from the views' own summary and base rows.
    let (mut contexts, mut baseline, mut trigger) = (0u64, 0u64, 0u64);
    for record in &model {
        contexts += 1;
        if record.trigger.is_some() {
            trigger += 1;
        } else {
            baseline += 1;
        }
    }
    let mut per_horizon: BTreeMap<String, [u64; 5]> = BTreeMap::new();
    each_row(&views, "outcome_summary", Some(n_min), |get| {
        let counts = per_horizon.entry(get("horizon")).or_default();
        match (get("split").as_str(), get("trigger").as_str()) {
            ("all", _) => {
                counts[0] = get("n_neither").parse().unwrap();
                counts[1] = get("n_unavailable").parse().unwrap();
            }
            ("trigger", "baseline") => counts[3] = get("clear").parse().unwrap(),
            ("trigger", _) => counts[4] += get("clear").parse::<u64>().unwrap(),
            _ => {}
        }
    });
    for record in &model {
        for outcome in &record.outcomes {
            if outcome.censor_reason.as_deref() == Some("end_of_data") {
                per_horizon.get_mut(&outcome.horizon).unwrap()[2] += 1;
            }
        }
    }
    let outcomes: u64 = model.iter().map(|r| r.outcomes.len() as u64).sum();

    println!("contexts {contexts} baseline {baseline} trigger {trigger} outcomes {outcomes}");
    println!("horizon neither unavailable end_of_data clear_baseline clear_trigger");
    for (horizon, _) in YEAR_HORIZONS {
        let c = per_horizon[horizon];
        println!("{horizon} {} {} {} {} {}", c[0], c[1], c[2], c[3], c[4]);
    }
    for (threads, row) in [1, 4].iter().zip(&digests) {
        for (view, rows, hex) in row {
            println!("threads {threads} {view} rows {rows} sha256 {hex}");
        }
    }
    println!("model mismatches {mismatched}");

    assert_eq!(mismatched, 0, "views and model disagree");
    assert_eq!(
        digests[0], digests[1],
        "the views depend on the thread count"
    );
    assert_eq!((contexts, baseline, trigger), YEAR_CONTEXTS);
    assert_eq!(outcomes, 5 * YEAR_CONTEXTS.0);
    for (horizon, expected) in YEAR_HORIZONS {
        assert_eq!(per_horizon[horizon], expected, "{horizon}");
    }
}
