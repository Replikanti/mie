//! The ADR-053 measurement (issue #24, M3; run by M4): an independent model
//! of the ADR-053 decision text, D3–D7, run next to one
//! `MarketStateEngine`, that checks the engine's bias and trigger facts and
//! values and reports what *Accept when* asks for.
//!
//! The model is written from the ADR text, not from the engine's internals.
//! It reads the engine's upstream facts and committed state — the
//! structure, location and candidate facts of each event, the regime, the
//! CVD divergences, the level registry, the auction state and the
//! 5-minute open interest — and rebuilds from them:
//!
//! - the bias (D3): the regime copied, and per structure timeframe and side
//!   the step of the last swing pair by the sign of its price change;
//! - every location context (D4): the registry levels whose zone, padded by
//!   6 bps of the level's price (ADR-044 D3), contains the anchor, without
//!   the levels the D4 key names for the swept swing of an SFP or for both
//!   swings of a CVD divergence;
//! - the rules of D5 (failed auction, failed reclaim, breakout + acceptance,
//!   SFP), D6 (the candidate families behind the D4 gate) and D7 (OI
//!   expansion followed by rejection: the point at an event is the held
//!   ready point of the boundary 25 s before it; a sweep records its start;
//!   the probe record follows the committed status after the location
//!   facts), in D2's emission order;
//! - every value: the last trigger of each id, with its fact input's
//!   validity (D1).
//!
//! Reported:
//!
//! - **T0** (asserted): no domain rejection, and at every event the
//!   engine's trigger facts equal the model's — in order, on every field
//!   (rule key, family, `known_at`, inputs, evidence, context, bias); at
//!   every event the bias value and every id's validity equal the model's;
//!   and at every event with a trigger, and at the end, every id's last
//!   trigger does.
//! - **Reachability** (asserted by the test that owns the source): on the
//!   archive, every id but the live-only absorption ids fires at least
//!   once; on the live store, each absorption id does. The ids with no
//!   trigger are listed before the assertion, so a stopped run still
//!   reports them.
//! - Recorded only: triggers per id, per half and per regime label (from
//!   the event's bias); the gate pass rate per candidate id; the CVD
//!   divergences per timeframe that only the previous-swing skip stops
//!   (D4); per family and level kind, the share of trigger contexts that
//!   list the kind; per OI id, the rejections with no start point, no end
//!   point, a resolution mismatch, a shared boundary, open interest not
//!   higher, a change out of range, and those that fired; the replay's
//!   wall time.
//!
//! Every count is printed before any assertion.
//!
//! `measure_triggers` (`--ignored`) runs the archive: aggTrades with the
//! ADR-050 fold (its D8 default) and `metrics` for the open interest. Run
//! it with `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! trigger_measure -- --ignored --exact --nocapture measure_triggers`
//! (`--exact` keeps the live test out); `MIE_MEASURE_TO` is included, as in
//! `mie replay`, and an optional `MIE_MEASURE_EVENTS` asserts the replay's
//! event count. The halves split at the window's midpoint,
//! `from + (to − from) / 2`; a trigger belongs to the half of its
//! `known_at`. M4 runs the #12 development year, 2025-10-01 … 2026-09-30,
//! with `MIE_MEASURE_EVENTS=603323345` (ADR-051 R0's count of the same
//! replay).
//!
//! `measure_triggers_live` (`--ignored`) runs every cleanly ended run of a
//! `mie ingest` capture alone through `LiveReplay::run_replay`, each into a
//! fresh engine, so absorption is checked where depth exists:
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test trigger_measure -- --ignored --exact
//! --nocapture measure_triggers_live`.
//!
//! Memory: the model streams. It holds the last trigger of each id, the
//! two OI points, one record per pending sweep, one probe record and count
//! tables; nothing grows with the window.
//!
//! `model_reproduces_the_engine_on_a_synthetic_tape` runs T0 and every
//! report section in CI on a seeded tape with live-like open interest and
//! an open-interest hole, and asserts that every compared kind occurred —
//! both OI rejections and a shared boundary included — so T0 cannot pass
//! vacuously; `model_reproduces_absorption_on_a_synthetic_book_tape` does
//! the same for the absorption ids on a seeded tape with a book.

mod common;

use common::tapes::{TAPE_DAYS, TAPE_START, book_tape, synthetic_tape};
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_cli::journal::{JournaledRun, read_runs};
use mie_cli::replay::parse_bound;
use mie_domain::bars::Timeframe;
use mie_domain::candidate::absorption::Absorption;
use mie_domain::candidate::divergence::CvdComparison;
use mie_domain::candidate::effort::{EffortResult, Exhaustion};
use mie_domain::candidate::{CandidateEvent, ContextLevel, ContextLevels, LocationContext};
use mie_domain::derivatives::OiGridPoint;
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureDefinition, FeatureKey, FeatureValue, Input, catalog};
use mie_domain::location::{
    AuctionState, AuctionStatus, LevelKind, LevelSide, LocationEvent, LocationLevel, LocationState,
    Region,
};
use mie_domain::num::{Price, Qty};
use mie_domain::regime::{Regime, RegimeLabel};
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::structure::{STRUCTURE_TIMEFRAMES, Side, StructureEvent, Sweep};
use mie_domain::time::EventTime;
use mie_domain::trigger::{
    Bias, OiExpansion, OiRejection, StepAt, SwingSequence, SwingStep, Trigger, TriggerEvent,
    TriggerEvidence, TriggerFamily, TriggerValue,
};
use mie_ports::outbound::{HistoricalDataProvider, MarketDataProvider, ReplayWindow};
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;

/// The open-interest grid (ADR-042 D2).
const GRID_MS: i64 = 300_000;

/// D7: the point at an event at `t` is that of the boundary of
/// `t − 25 000 ms`.
const OI_LAG_MS: i64 = 25_000;

/// D4: a level's zone is padded by 6 bps of its price, the registry
/// tolerance of ADR-044 D3.
const ZONE_BPS: i128 = 6;

/// D4: the level kinds the candidate gate admits.
const ADMITTED: [LevelKind; 9] = [
    LevelKind::Poc,
    LevelKind::Vah,
    LevelKind::Val,
    LevelKind::Hvn,
    LevelKind::Lvn,
    LevelKind::StructuralHigh,
    LevelKind::StructuralLow,
    LevelKind::SfpRejectionZone,
    LevelKind::Vwap,
];

/// Every level kind, for the context report.
const KINDS: [LevelKind; 12] = [
    LevelKind::Poc,
    LevelKind::Vah,
    LevelKind::Val,
    LevelKind::Hvn,
    LevelKind::Lvn,
    LevelKind::StructuralHigh,
    LevelKind::StructuralLow,
    LevelKind::LiquidityCluster,
    LevelKind::PriorSweep,
    LevelKind::SfpRejectionZone,
    LevelKind::Vwap,
    LevelKind::ValidatedReference,
];

/// The regime labels, in order; a fifth column counts unlabelled triggers.
const LABELS: [RegimeLabel; 4] = [
    RegimeLabel::Low,
    RegimeLabel::Medium,
    RegimeLabel::High,
    RegimeLabel::Extreme,
];

/// The OI outcomes of a rejection (D7), in report order.
const OUTCOMES: [&str; 7] = [
    "no_start",
    "no_end",
    "resolution_mismatch",
    "shared_boundary",
    "not_higher",
    "out_of_range",
    "fired",
];

/// Every trigger id with its family, in a fixed order.
fn ids() -> Vec<(TriggerFamily, &'static FeatureDefinition)> {
    let mut ids = vec![
        (
            TriggerFamily::FailedAuction,
            &catalog::TRIGGER_FAILED_AUCTION_V1,
        ),
        (
            TriggerFamily::FailedReclaim,
            &catalog::TRIGGER_FAILED_RECLAIM_V1,
        ),
        (
            TriggerFamily::BreakoutAcceptance,
            &catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1,
        ),
    ];
    ids.extend(
        catalog::TRIGGER_SFP
            .iter()
            .map(|(_, definition)| (TriggerFamily::Sfp, *definition)),
    );
    ids.extend(
        catalog::TRIGGER_CVD_DIVERGENCE
            .iter()
            .map(|(_, definition)| (TriggerFamily::CvdDivergence, *definition)),
    );
    ids.extend(
        catalog::TRIGGER_EFFORT_RESULT
            .iter()
            .map(|(_, definition)| (TriggerFamily::EffortVsResult, *definition)),
    );
    ids.push((TriggerFamily::Exhaustion, &catalog::TRIGGER_EXHAUSTION_V1));
    ids.extend(
        catalog::TRIGGER_ABSORPTION
            .iter()
            .map(|(_, definition)| (TriggerFamily::Absorption, *definition)),
    );
    ids.extend(
        catalog::TRIGGER_OI_EXPANSION_REJECTION
            .iter()
            .map(|(_, definition)| (TriggerFamily::OiExpansionRejection, *definition)),
    );
    ids.push((
        TriggerFamily::OiExpansionRejection,
        &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1,
    ));
    ids
}

/// The number of trigger ids.
const N_IDS: usize = 23;

/// The position of the id `key` in [`ids`].
fn id_index(key: FeatureKey) -> Option<usize> {
    ids()
        .iter()
        .position(|(_, definition)| definition.key == key)
}

/// The position of `timeframe` among the structure timeframes.
fn structure_index(timeframe: Timeframe) -> Option<usize> {
    STRUCTURE_TIMEFRAMES
        .iter()
        .position(|candidate| *candidate == timeframe)
}

/// The position of `family` in [`TriggerFamily::ALL`].
fn family_index(family: TriggerFamily) -> usize {
    TriggerFamily::ALL
        .iter()
        .position(|candidate| *candidate == family)
        .expect("a family")
}

/// The level side of a swing side.
fn level_side(side: Side) -> LevelSide {
    match side {
        Side::High => LevelSide::High,
        Side::Low => LevelSide::Low,
    }
}

// ---------------------------------------------------------------------------
// The bias (D3)
// ---------------------------------------------------------------------------

/// The model's bias: the regime copied, and per structure timeframe the
/// step of each side's last swing pair with its validity.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ModelBias {
    regime: FeatureValue<Regime>,
    swings: [FeatureValue<SwingSequence>; 4],
}

impl ModelBias {
    /// D3 on `state`: a step is `Higher`, `Equal` or `Lower` by the sign of
    /// the pair's price change, at the later swing's bar time.
    fn read(state: &MarketState) -> Self {
        let step = |comparison: &CvdComparison| {
            let change = comparison.pair.price_change.units();
            StepAt {
                step: if change > 0 {
                    SwingStep::Higher
                } else if change == 0 {
                    SwingStep::Equal
                } else {
                    SwingStep::Lower
                },
                swing_time: comparison.pair.current.swing_time,
            }
        };
        Self {
            regime: state.regime,
            swings: STRUCTURE_TIMEFRAMES.map(|timeframe| {
                let divergences = state
                    .candidates
                    .divergences(timeframe)
                    .expect("every structure timeframe has divergences");
                follow(
                    &divergences.cvd,
                    divergences.cvd.ready().map_or(
                        SwingSequence {
                            high: None,
                            low: None,
                        },
                        |divergence| SwingSequence {
                            high: divergence.high.as_ref().map(step),
                            low: divergence.low.as_ref().map(step),
                        },
                    ),
                )
            }),
        }
    }

    /// Whether `bias` is this one under `bias.context@1`.
    fn matches(&self, bias: &Bias) -> bool {
        bias.feature == catalog::BIAS_CONTEXT_V1.key
            && bias.regime == self.regime
            && bias
                .iter_swings()
                .map(|(_, swings)| *swings)
                .eq(self.swings)
    }

    /// The regime label column: the label, or 4 when unlabelled.
    fn label(&self) -> usize {
        self.regime.ready().map_or(LABELS.len(), |regime| {
            LABELS
                .iter()
                .position(|label| *label == regime.label)
                .expect("a label")
        })
    }
}

/// `value` with `input`'s validity (D1).
fn follow<T, U>(input: &FeatureValue<T>, value: U) -> FeatureValue<U> {
    match input {
        FeatureValue::Ready(_) => FeatureValue::Ready(value),
        FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
            observed: *observed,
            required: *required,
        },
        FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason: *reason },
    }
}

/// Whether two values have the same validity, counts and reason included.
fn same_validity<T, U>(a: &FeatureValue<T>, b: &FeatureValue<U>) -> bool {
    follow(a, ()) == follow(b, ())
}

// ---------------------------------------------------------------------------
// Location contexts (D4)
// ---------------------------------------------------------------------------

/// A swing whose levels a context leaves out: its timeframe, side, price
/// and swing-bar open.
#[derive(Debug, Clone, Copy)]
struct OwnSwing {
    timeframe: Timeframe,
    side: LevelSide,
    price: Price,
    swing_time: EventTime,
}

/// D4's key: whether `level` marks `swing`'s extreme in any structure
/// registry — a `structure.levels.<tf>` source, the swing's side and price,
/// and a swing bar that lies within the swing's bar or holds it.
fn names_swing(level: &LocationLevel, swing: &OwnSwing) -> bool {
    let Some((timeframe, _)) = catalog::STRUCTURE_LEVELS
        .iter()
        .find(|(_, definition)| definition.key.id == level.source.id)
    else {
        return false;
    };
    let Some(open) = level.anchor else {
        return false;
    };
    let (open, end) = (open.as_millis(), open.as_millis() + timeframe.millis());
    let swing_open = swing.swing_time.as_millis();
    let swing_end = swing_open + swing.timeframe.millis();
    level.side == Some(swing.side)
        && level.price == swing.price
        && ((open <= swing_open && swing_end <= end) || (swing_open <= open && end <= swing_end))
}

/// ADR-044 D3's padded zone: whether `price` lies in `[low − w, high + w]`
/// with `w` = 6 bps of the level's price, exact.
fn in_zone(level: &LocationLevel, price: Price) -> bool {
    let price = i128::from(price.units());
    let reach = i128::from(level.price.units()).abs() * ZONE_BPS;
    (i128::from(level.low.units()) - price) * 10_000 <= reach
        && (price - i128::from(level.high.units())) * 10_000 <= reach
}

/// D4's context of `anchor` in `location`, without the levels that name
/// any of `own`.
fn context(anchor: Price, own: &[OwnSwing], location: &LocationState) -> LocationContext {
    let levels = match &location.levels {
        FeatureValue::Ready(set) => FeatureValue::Ready(ContextLevels {
            feature: set.feature,
            levels: set
                .levels()
                .iter()
                .filter(|level| in_zone(level, anchor))
                .filter(|level| !own.iter().any(|swing| names_swing(level, swing)))
                .map(|level| ContextLevel {
                    kind: level.kind,
                    source: level.source,
                    side: level.side,
                    anchor: level.anchor,
                    price: level.price,
                    low: level.low,
                    high: level.high,
                })
                .collect(),
        }),
        other => follow(
            other,
            ContextLevels {
                feature: catalog::LOCATION_LEVELS_V2.key,
                levels: Vec::new(),
            },
        ),
    };
    LocationContext {
        anchor,
        levels,
        auction: location.auction,
    }
}

/// D4's gate: the context's levels are ready and list an admitted kind.
fn passes(context: &LocationContext) -> bool {
    context.levels.ready().is_some_and(|levels| {
        levels
            .levels
            .iter()
            .any(|level| ADMITTED.contains(&level.kind))
    })
}

// ---------------------------------------------------------------------------
// The model (D5–D7)
// ---------------------------------------------------------------------------

/// A trigger the model expects, every field but the bias, which the model
/// checks against [`ModelBias`].
#[derive(Debug)]
struct Expected {
    feature: FeatureKey,
    family: TriggerFamily,
    known_at: EventTime,
    inputs: &'static [Input],
    evidence: TriggerEvidence,
    context: LocationContext,
}

/// A recorded sweep (D7): the swept level's key and the point at the
/// sweeping trade, if there was one.
#[derive(Debug, Clone, Copy)]
struct SweepRecord {
    timeframe: Timeframe,
    side: Side,
    swing_time: EventTime,
    start: Option<OiGridPoint>,
}

/// The probe record (D7): the committed probe's identity and the point at
/// its first close.
#[derive(Debug, Clone, Copy, PartialEq)]
struct ProbeRecord {
    reference_start: EventTime,
    toward: Region,
    since: EventTime,
    start: Option<OiGridPoint>,
}

/// The model's state: the last trigger of each id and the D7 records.
#[derive(Debug, Default)]
struct Model {
    auction: [Option<Trigger<AuctionStatus>>; 3],
    sfp: [Option<Trigger<Sweep>>; 4],
    cvd: [Option<Trigger<CvdComparison>>; 4],
    effort: [Option<Trigger<EffortResult>>; 3],
    exhaustion: Option<Trigger<Exhaustion>>,
    absorption: [Option<Trigger<Absorption>>; 3],
    oi: [Option<Trigger<OiExpansion>>; 4],
    oi_prior_day: Option<Trigger<OiExpansion>>,
    /// The last two ready OI points, newest first.
    held: [Option<OiGridPoint>; 2],
    sweeps: Vec<SweepRecord>,
    probe: Option<ProbeRecord>,
}

/// What the model counts besides T0.
#[derive(Debug, Default)]
struct Stats {
    /// Candidates per candidate id, and those that passed the gate.
    gate: [[u64; 2]; N_IDS],
    /// CVD divergences per timeframe that pass with only the current swing
    /// skipped and fail with the pair skipped (D4).
    pair_only: [u64; 4],
    /// OI outcomes per OI id (the four SFP timeframes, then the failed
    /// auction), in [`OUTCOMES`] order.
    oi: [[u64; 7]; 5],
}

impl Model {
    /// D7: takes the committed `derivatives.oi.5m@1`; a ready point on a
    /// later boundary than the newest held one shifts it in.
    fn observe(&mut self, value: &FeatureValue<OiGridPoint>) {
        if let FeatureValue::Ready(point) = value
            && self.held[0].is_none_or(|newest| point.boundary > newest.boundary)
        {
            self.held = [Some(*point), self.held[0]];
        }
    }

    /// D7: the held point of the boundary of `time − 25 000 ms`.
    fn point_at(&self, time: EventTime) -> Option<OiGridPoint> {
        let lagged = time.as_millis() - OI_LAG_MS;
        let boundary = lagged - lagged.rem_euclid(GRID_MS);
        self.held
            .into_iter()
            .flatten()
            .find(|point| point.boundary.as_millis() == boundary)
    }

    /// D7's outcome of a rejection with `start` and `end`: the outcome
    /// index in [`OUTCOMES`] and, when it fired, the expansion.
    fn judge(
        rejection: OiRejection,
        start: Option<OiGridPoint>,
        end: Option<OiGridPoint>,
    ) -> (usize, Option<OiExpansion>) {
        let Some(start) = start else {
            return (0, None);
        };
        let Some(end) = end else {
            return (1, None);
        };
        if start.resolution_ms != end.resolution_ms {
            return (2, None);
        }
        if end.boundary <= start.boundary {
            return (3, None);
        }
        let change =
            i128::from(end.open_interest.units()) - i128::from(start.open_interest.units());
        if change <= 0 {
            return (4, None);
        }
        let Ok(change) = i64::try_from(change) else {
            return (5, None);
        };
        (
            6,
            Some(OiExpansion {
                rejection,
                start,
                end,
                change: Qty::from_units(change),
            }),
        )
    }

    /// D5 and D7 on one structure fact.
    fn structure(
        &mut self,
        fact: &StructureEvent,
        location: &LocationState,
        expected: &mut Vec<Expected>,
        stats: &mut Stats,
    ) {
        let key = |sweep: &Sweep| (sweep.timeframe, sweep.level.side, sweep.level.swing_time);
        let take = |sweeps: &mut Vec<SweepRecord>, sweep: &Sweep| {
            sweeps
                .iter()
                .position(|record| (record.timeframe, record.side, record.swing_time) == key(sweep))
                .map(|index| sweeps.remove(index))
        };
        let sweep = match fact {
            StructureEvent::Sweep(sweep) => {
                take(&mut self.sweeps, sweep);
                let start = self.point_at(sweep.known_at);
                self.sweeps.push(SweepRecord {
                    timeframe: sweep.timeframe,
                    side: sweep.level.side,
                    swing_time: sweep.level.swing_time,
                    start,
                });
                return;
            }
            StructureEvent::Break(sweep) => {
                take(&mut self.sweeps, sweep);
                return;
            }
            StructureEvent::Swing(_) => return,
            StructureEvent::Sfp(sweep) => sweep,
        };
        let record = take(&mut self.sweeps, sweep);
        let Some(position) = structure_index(sweep.timeframe) else {
            return;
        };
        let anchor = sweep.level.price;
        let own = OwnSwing {
            timeframe: sweep.timeframe,
            side: level_side(sweep.level.side),
            price: sweep.level.price,
            swing_time: sweep.level.swing_time,
        };
        let context = context(anchor, &[own], location);
        let definition = catalog::TRIGGER_SFP[position].1;
        self.sfp[position] = Some(Trigger {
            known_at: sweep.known_at,
            anchor,
            evidence: *sweep,
        });
        expected.push(Expected {
            feature: definition.key,
            family: TriggerFamily::Sfp,
            known_at: sweep.known_at,
            inputs: definition.inputs,
            evidence: TriggerEvidence::Sfp(*sweep),
            context: context.clone(),
        });
        let (outcome, expansion) = Self::judge(
            OiRejection::Sfp(*sweep),
            record.and_then(|record| record.start),
            self.point_at(sweep.known_at),
        );
        stats.oi[position][outcome] += 1;
        if let Some(expansion) = expansion {
            let definition = catalog::TRIGGER_OI_EXPANSION_REJECTION[position].1;
            self.oi[position] = Some(Trigger {
                known_at: sweep.known_at,
                anchor,
                evidence: expansion,
            });
            expected.push(Expected {
                feature: definition.key,
                family: TriggerFamily::OiExpansionRejection,
                known_at: sweep.known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::OiExpansion(expansion),
                context,
            });
        }
    }

    /// D5 and D7 on one location fact.
    fn location(
        &mut self,
        fact: &LocationEvent,
        location: &LocationState,
        expected: &mut Vec<Expected>,
        stats: &mut Stats,
    ) {
        let LocationEvent::Auction {
            from,
            to,
            known_at,
            status,
            ..
        } = fact
        else {
            return;
        };
        let edge = |region: Region| match region {
            Region::Above => Some(status.vah),
            Region::Below => Some(status.val),
            Region::In => None,
        };
        let failing = status.failure.filter(|failure| failure.at == *known_at);
        let (position, anchor) = match (to, failing) {
            (AuctionState::FailedBreakout, Some(failure)) => (0, edge(failure.toward)),
            (AuctionState::FailedReclaim, Some(_)) => (1, edge(status.accepted)),
            (AuctionState::Acceptance, _) if *from == Some(AuctionState::Breakout) => {
                (2, edge(status.accepted))
            }
            _ => return,
        };
        let Some(anchor) = anchor else {
            return;
        };
        let (family, definition) = ids()[position];
        let context = context(anchor, &[], location);
        self.auction[position] = Some(Trigger {
            known_at: *known_at,
            anchor,
            evidence: *status,
        });
        expected.push(Expected {
            feature: definition.key,
            family,
            known_at: *known_at,
            inputs: definition.inputs,
            evidence: TriggerEvidence::Auction(*status),
            context: context.clone(),
        });
        if family != TriggerFamily::FailedAuction {
            return;
        }
        let toward = failing.map(|failure| failure.toward);
        let start = self
            .probe
            .filter(|probe| {
                probe.reference_start == status.reference_start && Some(probe.toward) == toward
            })
            .and_then(|probe| probe.start);
        let (outcome, expansion) = Self::judge(
            OiRejection::FailedAuction(*status),
            start,
            self.point_at(*known_at),
        );
        stats.oi[4][outcome] += 1;
        if let Some(expansion) = expansion {
            let definition = &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1;
            self.oi_prior_day = Some(Trigger {
                known_at: *known_at,
                anchor,
                evidence: expansion,
            });
            expected.push(Expected {
                feature: definition.key,
                family: TriggerFamily::OiExpansionRejection,
                known_at: *known_at,
                inputs: definition.inputs,
                evidence: TriggerEvidence::OiExpansion(expansion),
                context,
            });
        }
    }

    /// D7's hook after the location facts: the record follows the committed
    /// probe toward above or below, recorded with the point at its first
    /// close the first time its identity is seen.
    fn hook(&mut self, auction: &FeatureValue<AuctionStatus>) {
        let probe = auction.ready().and_then(|status| {
            status
                .probe
                .filter(|probe| probe.toward != Region::In)
                .map(|probe| (status.reference_start, probe.toward, probe.since))
        });
        self.probe = match (probe, self.probe) {
            (None, _) => None,
            (Some(identity), Some(held))
                if (held.reference_start, held.toward, held.since) == identity =>
            {
                Some(held)
            }
            (Some((reference_start, toward, since)), _) => Some(ProbeRecord {
                reference_start,
                toward,
                since,
                start: self.point_at(since),
            }),
        };
    }

    /// D6 with the D4 gate on one candidate fact.
    fn candidate(
        &mut self,
        fact: &CandidateEvent,
        location: &LocationState,
        expected: &mut Vec<Expected>,
        stats: &mut Stats,
    ) {
        let known_at = fact.time();
        let (definition, family, context) = match fact {
            CandidateEvent::OiDivergence(_) => return,
            CandidateEvent::CvdDivergence(event) => {
                let Some(position) = structure_index(event.timeframe) else {
                    return;
                };
                let pair = event.comparison.pair;
                let swing = |anchor: &mie_domain::candidate::divergence::SwingAnchor| OwnSwing {
                    timeframe: event.timeframe,
                    side: level_side(pair.side),
                    price: anchor.price,
                    swing_time: anchor.swing_time,
                };
                let both = context(
                    pair.current.price,
                    &[swing(&pair.previous), swing(&pair.current)],
                    location,
                );
                if !passes(&both)
                    && passes(&context(
                        pair.current.price,
                        &[swing(&pair.current)],
                        location,
                    ))
                {
                    stats.pair_only[position] += 1;
                }
                let definition = catalog::TRIGGER_CVD_DIVERGENCE[position].1;
                if passes(&both) {
                    self.cvd[position] = Some(Trigger {
                        known_at,
                        anchor: pair.current.price,
                        evidence: event.comparison,
                    });
                }
                (definition, TriggerFamily::CvdDivergence, both)
            }
            CandidateEvent::EffortVsResult(event) => {
                let Some(position) = catalog::CANDIDATE_EFFORT_RESULT
                    .iter()
                    .position(|(_, definition)| definition.key == event.feature)
                else {
                    return;
                };
                if passes(&event.context) {
                    self.effort[position] = Some(Trigger {
                        known_at,
                        anchor: event.context.anchor,
                        evidence: event.value,
                    });
                }
                (
                    catalog::TRIGGER_EFFORT_RESULT[position].1,
                    TriggerFamily::EffortVsResult,
                    event.context.clone(),
                )
            }
            CandidateEvent::Exhaustion(event) => {
                if passes(&event.context) {
                    self.exhaustion = Some(Trigger {
                        known_at,
                        anchor: event.context.anchor,
                        evidence: event.value,
                    });
                }
                (
                    &catalog::TRIGGER_EXHAUSTION_V1,
                    TriggerFamily::Exhaustion,
                    event.context.clone(),
                )
            }
            CandidateEvent::Absorption(event) => {
                let Some(position) = catalog::CANDIDATE_ABSORPTION
                    .iter()
                    .position(|(_, definition)| definition.key == event.feature)
                else {
                    return;
                };
                if passes(&event.context) {
                    self.absorption[position] = Some(Trigger {
                        known_at,
                        anchor: event.context.anchor,
                        evidence: event.value,
                    });
                }
                (
                    catalog::TRIGGER_ABSORPTION[position].1,
                    TriggerFamily::Absorption,
                    event.context.clone(),
                )
            }
        };
        let index = id_index(definition.key).expect("a trigger id");
        stats.gate[index][0] += 1;
        if !passes(&context) {
            return;
        }
        stats.gate[index][1] += 1;
        expected.push(Expected {
            feature: definition.key,
            family,
            known_at,
            inputs: definition.inputs,
            evidence: TriggerEvidence::Candidate(Box::new(fact.clone())),
            context,
        });
    }
}

// ---------------------------------------------------------------------------
// T0
// ---------------------------------------------------------------------------

/// T0's comparison and its counts.
#[derive(Debug, Default)]
struct Check {
    mismatches: u64,
    notes: Vec<String>,
    /// Trigger facts compared, per id.
    facts: [u64; N_IDS],
    /// Events whose every value was compared.
    values: u64,
}

impl Check {
    fn note(&mut self, note: String) {
        self.mismatches += 1;
        if self.notes.len() < 20 {
            self.notes.push(note);
        }
    }

    /// T0 on the trigger facts of one event.
    fn facts(
        &mut self,
        at: EventTime,
        actual: &[TriggerEvent],
        expected: &[Expected],
        bias: &ModelBias,
    ) {
        if actual.len() != expected.len() {
            self.note(format!(
                "{at}: {} trigger facts, the model expects {}: {:?} vs {:?}",
                actual.len(),
                expected.len(),
                actual.iter().map(|fact| fact.feature).collect::<Vec<_>>(),
                expected.iter().map(|fact| fact.feature).collect::<Vec<_>>()
            ));
            return;
        }
        for (fact, model) in actual.iter().zip(expected) {
            let same = fact.feature == model.feature
                && fact.family == model.family
                && fact.known_at == model.known_at
                && fact.inputs == model.inputs
                && fact.evidence == model.evidence
                && fact.context == model.context
                && bias.matches(&fact.bias);
            if same {
                if let Some(index) = id_index(fact.feature) {
                    self.facts[index] += 1;
                }
            } else {
                self.note(format!("{at}: engine {fact}\n  model {model:?}"));
            }
        }
    }

    /// T0 on the values: the bias and every id's validity at every event,
    /// and every id's last trigger when `full`.
    fn values(
        &mut self,
        at: EventTime,
        state: &MarketState,
        model: &Model,
        bias: &ModelBias,
        full: bool,
    ) {
        let triggers = &state.triggers;
        if !bias.matches(triggers.bias()) {
            self.note(format!("{at}: bias {}", triggers.bias()));
        }
        let mut wrong: Vec<String> = Vec::new();
        fn check<E: PartialEq, I>(
            wrong: &mut Vec<String>,
            name: FeatureKey,
            value: Option<&FeatureValue<TriggerValue<E>>>,
            input: &FeatureValue<I>,
            last: &Option<Trigger<E>>,
            full: bool,
        ) {
            let Some(value) = value else {
                wrong.push(format!("{name} missing"));
                return;
            };
            if !same_validity(value, input) {
                wrong.push(format!("{name} validity"));
            } else if full
                && let FeatureValue::Ready(value) = value
                && (value.feature != name || value.last != *last)
            {
                wrong.push(format!("{name} last"));
            }
        }
        for (position, value) in triggers.iter_auction().map(|(_, value)| value).enumerate() {
            check(
                &mut wrong,
                ids()[position].1.key,
                Some(value),
                &state.location.auction,
                &model.auction[position],
                full,
            );
        }
        for (position, timeframe) in STRUCTURE_TIMEFRAMES.into_iter().enumerate() {
            let levels = &state.structure.get(timeframe).expect("structure").levels;
            check(
                &mut wrong,
                catalog::TRIGGER_SFP[position].1.key,
                triggers.sfp(timeframe),
                levels,
                &model.sfp[position],
                full,
            );
            check(
                &mut wrong,
                catalog::TRIGGER_OI_EXPANSION_REJECTION[position].1.key,
                triggers.oi_expansion_rejection(timeframe),
                levels,
                &model.oi[position],
                full,
            );
            check(
                &mut wrong,
                catalog::TRIGGER_CVD_DIVERGENCE[position].1.key,
                triggers.cvd_divergence(timeframe),
                &state
                    .candidates
                    .divergences(timeframe)
                    .expect("divergences")
                    .cvd,
                &model.cvd[position],
                full,
            );
        }
        for (position, (timeframe, _)) in catalog::TRIGGER_EFFORT_RESULT.iter().enumerate() {
            check(
                &mut wrong,
                catalog::TRIGGER_EFFORT_RESULT[position].1.key,
                triggers.effort_result(*timeframe),
                state.candidates.effort_result(*timeframe).expect("effort"),
                &model.effort[position],
                full,
            );
            check(
                &mut wrong,
                catalog::TRIGGER_ABSORPTION[position].1.key,
                triggers.absorption(*timeframe),
                state.candidates.absorption(*timeframe).expect("absorption"),
                &model.absorption[position],
                full,
            );
        }
        check(
            &mut wrong,
            catalog::TRIGGER_EXHAUSTION_V1.key,
            Some(triggers.exhaustion()),
            state.candidates.exhaustion(),
            &model.exhaustion,
            full,
        );
        check(
            &mut wrong,
            catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key,
            Some(triggers.oi_expansion_rejection_prior_day()),
            &state.location.auction,
            &model.oi_prior_day,
            full,
        );
        if full {
            self.values += 1;
        }
        if !wrong.is_empty() {
            self.note(format!("{at}: values {wrong:?}"));
        }
    }
}

// ---------------------------------------------------------------------------
// The run
// ---------------------------------------------------------------------------

/// One replay: the engine, the model and the counts.
struct Run {
    engine: MarketStateEngine,
    model: Model,
    stats: Stats,
    check: Check,
    mid: i64,
    events: u64,
    rejections: u64,
    /// Triggers per id, half and regime label (the fifth: unlabelled).
    counts: [[[u64; 5]; 2]; N_IDS],
    /// Per family: triggers, and those whose context lists each kind.
    kinds: [(u64, [u64; 12]); 9],
}

impl Run {
    fn new(mid: i64) -> Self {
        Self {
            engine: MarketStateEngine::new(),
            model: Model::default(),
            stats: Stats::default(),
            check: Check::default(),
            mid,
            events: 0,
            rejections: 0,
            counts: [[[0; 5]; 2]; N_IDS],
            kinds: [(0, [0; 12]); 9],
        }
    }

    fn step(&mut self, event: &MarketEvent) {
        self.events += 1;
        if self.engine.apply(event).is_err() {
            self.rejections += 1;
            return;
        }
        let at = event.time();
        let state = self.engine.state();
        let bias = ModelBias::read(state);
        self.model.observe(&state.derivatives.oi_5m);
        let mut expected = Vec::new();
        for fact in self.engine.structure_events() {
            self.model
                .structure(fact, &state.location, &mut expected, &mut self.stats);
        }
        for fact in self.engine.location_events() {
            self.model
                .location(fact, &state.location, &mut expected, &mut self.stats);
        }
        self.model.hook(&state.location.auction);
        for fact in self.engine.candidate_events() {
            self.model
                .candidate(fact, &state.location, &mut expected, &mut self.stats);
        }
        let actual = self.engine.trigger_events();
        self.check.facts(at, actual, &expected, &bias);
        let full = !actual.is_empty() || !expected.is_empty();
        self.check.values(at, state, &self.model, &bias, full);
        let half = usize::from(at.as_millis() >= self.mid);
        for fact in actual {
            if let Some(index) = id_index(fact.feature) {
                self.counts[index][half][bias.label()] += 1;
            }
            let (total, kinds) = &mut self.kinds[family_index(fact.family)];
            *total += 1;
            if let Some(levels) = fact.context.levels.ready() {
                for (count, kind) in kinds.iter_mut().zip(KINDS) {
                    if levels.levels.iter().any(|level| level.kind == kind) {
                        *count += 1;
                    }
                }
            }
        }
    }

    /// The last values, compared in full, and T0's verdict line.
    fn finish(&mut self) {
        if let Some(at) = self.engine.state().as_of {
            let state = self.engine.state();
            let bias = ModelBias::read(state);
            self.check.values(at, state, &self.model, &bias, true);
        }
    }

    /// Triggers of the id at `index`, over both halves and every label.
    fn total(&self, index: usize) -> u64 {
        self.counts[index].iter().flatten().sum()
    }
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn share(part: u64, whole: u64) -> String {
    if whole == 0 {
        "-".to_owned()
    } else {
        format!("{:.4}", part as f64 / whole as f64)
    }
}

fn report_t0(run: &Run) {
    println!(
        "T0: events {}, rejections {}, mismatches {}, values compared at {} events",
        run.events, run.rejections, run.check.mismatches, run.check.values
    );
    for note in &run.check.notes {
        println!("T0 mismatch: {note}");
    }
}

fn report(run: &Run, wall: Duration) {
    report_t0(run);
    let ids = ids();
    println!("triggers per id: half 1 | half 2, each low/medium/high/extreme/unlabelled");
    for (index, (_, definition)) in ids.iter().enumerate() {
        let [first, second] = run.counts[index];
        println!(
            "  {} total {}: {:?} | {:?} (facts compared {})",
            definition.key,
            run.total(index),
            first,
            second,
            run.check.facts[index]
        );
    }
    println!("gate pass rate per candidate id (D4):");
    for (index, (family, definition)) in ids.iter().enumerate() {
        if matches!(
            family,
            TriggerFamily::CvdDivergence
                | TriggerFamily::EffortVsResult
                | TriggerFamily::Exhaustion
                | TriggerFamily::Absorption
        ) {
            let [seen, passed] = run.stats.gate[index];
            println!(
                "  {} candidates {seen} passed {passed} rate {}",
                definition.key,
                share(passed, seen)
            );
        }
    }
    println!(
        "CVD divergences only the previous-swing skip stops (D4), 15m/1h/4h/1d: {:?}",
        run.stats.pair_only
    );
    println!("context share per family and level kind:");
    for (family, (total, kinds)) in TriggerFamily::ALL.iter().zip(&run.kinds) {
        let shares: Vec<String> = KINDS
            .iter()
            .zip(kinds)
            .map(|(kind, count)| format!("{kind}={}", share(*count, *total)))
            .collect();
        println!("  {family} triggers {total}: {}", shares.join(" "));
    }
    println!("OI outcomes per id (D7): {}", OUTCOMES.join("/"));
    let oi_ids = catalog::TRIGGER_OI_EXPANSION_REJECTION
        .iter()
        .map(|(_, definition)| definition.key)
        .chain([catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key]);
    for (key, outcomes) in oi_ids.zip(&run.stats.oi) {
        println!("  {key}: {outcomes:?}");
    }
    println!("wall {:.1} s", wall.as_secs_f64());
}

/// The ids among `wanted` with no trigger, listed for the report.
fn unreached(run: &Run, wanted: impl Fn(TriggerFamily) -> bool) -> Vec<FeatureKey> {
    ids()
        .iter()
        .enumerate()
        .filter(|(index, (family, _))| wanted(*family) && run.total(*index) == 0)
        .map(|(_, (_, definition))| definition.key)
        .collect()
}

#[test]
#[ignore = "measures the archive for ADR-053; see the module docs"]
fn measure_triggers() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let from = parse_bound("MIE_MEASURE_FROM", &env("MIE_MEASURE_FROM"), false).unwrap();
    let to = parse_bound("MIE_MEASURE_TO", &env("MIE_MEASURE_TO"), true).unwrap();
    let expected_events: Option<u64> = std::env::var("MIE_MEASURE_EVENTS")
        .ok()
        .map(|events| events.parse().expect("MIE_MEASURE_EVENTS is a count"));
    let window = ReplayWindow {
        start: EventTime::from_millis(from),
        end: EventTime::from_millis(to),
    };
    let store = ParquetRawStore::new(&raw_root);
    // aggTrades with the ADR-050 fold (its D8 default), and metrics for the
    // 5-minute open interest (ADR-042 D2).
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::AggTrades, ArchiveStream::Metrics],
        TradeCorrection::Fold,
    );
    let mut stream = replay.replay(window).unwrap().stream;
    let started = Instant::now();
    let mut run = Run::new(from + (to - from) / 2);
    while let Some(event) = stream.next_event().unwrap() {
        run.step(&event);
    }
    run.finish();
    println!("window {from}..{to} ms, split {} ms", run.mid);
    report(&run, started.elapsed());
    let zeros = unreached(&run, |family| family != TriggerFamily::Absorption);
    println!("reachability: ids without a trigger {zeros:?}");
    if let Some(expected) = expected_events {
        println!("T0: events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    assert_eq!(run.rejections, 0, "the engine rejected events");
    assert_eq!(
        run.check.mismatches, 0,
        "the model disagrees with the engine"
    );
    assert!(zeros.is_empty(), "ids that never fired: {zeros:?}");
}

#[test]
#[ignore = "checks the triggers against a live capture for ADR-053; see the module docs"]
fn measure_triggers_live() {
    let raw_root = env("MIE_MEASURE_RAW_ROOT");
    let journal = std::path::PathBuf::from(env("MIE_MEASURE_JOURNAL"));
    let runs: Vec<JournaledRun> = read_runs(&journal, "binance-um").unwrap();
    let store = ParquetRawStore::new(&raw_root);
    let live = LiveReplay::new(
        &store,
        "binance-um",
        "BTCUSDT",
        runs.iter().map(JournaledRun::live_run).collect(),
    );
    let (mut mismatches, mut rejections) = (0, 0);
    let mut absorption = [0_u64; 3];
    let absorption_ids: Vec<usize> = catalog::TRIGGER_ABSORPTION
        .iter()
        .map(|(_, definition)| id_index(definition.key).expect("an id"))
        .collect();
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        // One run, no halves: every count lands in the first.
        let mut run = Run::new(i64::MAX);
        let started = Instant::now();
        while let Some(event) = replay.next_event().unwrap() {
            run.step(&event);
        }
        run.finish();
        println!("run {id}:");
        report(&run, started.elapsed());
        mismatches += run.check.mismatches;
        rejections += run.rejections;
        for (count, index) in absorption.iter_mut().zip(&absorption_ids) {
            *count += run.total(*index);
        }
    }
    println!(
        "live T0: mismatches {mismatches}, rejections {rejections}; absorption triggers \
         5m/15m/1h {absorption:?}"
    );
    assert_eq!(rejections, 0, "the engine rejected events");
    assert_eq!(mismatches, 0, "the model disagrees with the engine");
    assert!(
        absorption.iter().all(|count| *count > 0),
        "an absorption id never fired on the live store: {absorption:?}"
    );
}

#[test]
fn model_reproduces_the_engine_on_a_synthetic_tape() {
    let mut run = Run::new(TAPE_START + TAPE_DAYS * DAY_MS / 2);
    let started = Instant::now();
    for event in synthetic_tape() {
        run.step(&event);
    }
    run.finish();
    // The report runs too, so the archive run cannot fail on it.
    report(&run, started.elapsed());
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(run.check.mismatches, 0, "{:#?}", run.check.notes);
    // Every compared kind occurred, so T0 did not pass vacuously.
    let fired = |key: FeatureKey| run.check.facts[id_index(key).expect("an id")];
    let oi = &run.stats.oi;
    let stopped: u64 = run
        .stats
        .gate
        .iter()
        .map(|[seen, passed]| seen - passed)
        .sum();
    let kinds = [
        (
            "failed auctions",
            fired(catalog::TRIGGER_FAILED_AUCTION_V1.key),
        ),
        (
            "failed reclaims",
            fired(catalog::TRIGGER_FAILED_RECLAIM_V1.key),
        ),
        (
            "breakouts accepted",
            fired(catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key),
        ),
        ("15m SFPs", fired(catalog::TRIGGER_SFP_15M_V1.key)),
        ("1h SFPs", fired(catalog::TRIGGER_SFP_1H_V1.key)),
        (
            "15m CVD divergences",
            fired(catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.key),
        ),
        (
            "5m effort vs result",
            fired(catalog::TRIGGER_EFFORT_RESULT_5M_V1.key),
        ),
        ("exhaustion", fired(catalog::TRIGGER_EXHAUSTION_V1.key)),
        (
            "15m OI expansions",
            fired(catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1.key),
        ),
        (
            "failed-auction OI expansions",
            fired(catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key),
        ),
        ("OI shared boundaries", oi.iter().map(|row| row[3]).sum()),
        (
            "OI rejections without a start",
            oi.iter().map(|row| row[0]).sum(),
        ),
        (
            "OI rejections not higher",
            oi.iter().map(|row| row[4]).sum(),
        ),
        ("candidates the gate stopped", stopped),
        ("events with every value compared", run.check.values),
    ];
    for (kind, count) in kinds {
        assert!(count > 0, "the tape compared no {kind}");
    }
}

#[test]
fn model_reproduces_absorption_on_a_synthetic_book_tape() {
    let mut run = Run::new(i64::MAX);
    for event in book_tape() {
        run.step(&event);
    }
    run.finish();
    report_t0(&run);
    assert_eq!(run.rejections, 0, "the engine rejected the tape");
    assert_eq!(run.check.mismatches, 0, "{:#?}", run.check.notes);
    for (_, definition) in catalog::TRIGGER_ABSORPTION {
        let index = id_index(definition.key).expect("an id");
        assert!(run.check.facts[index] > 0, "{}", definition.key);
    }
}

#[test]
fn the_model_constants_are_the_engines() {
    // ADR-053 D4, D7: the model's lag, grid, padding and gate are the
    // numbers the definitions carry.
    use mie_domain::feature::ParamValue;
    assert_eq!(ids().len(), N_IDS);
    let param = |definition: &FeatureDefinition, name: &str| {
        definition
            .params
            .iter()
            .find(|param| param.name == name)
            .map(|param| param.value)
    };
    for (_, definition) in catalog::TRIGGER_OI_EXPANSION_REJECTION {
        assert_eq!(
            param(definition, "oi_lag_ms"),
            Some(ParamValue::Int(OI_LAG_MS))
        );
    }
    assert_eq!(
        param(
            &catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1,
            "oi_lag_ms"
        ),
        Some(ParamValue::Int(OI_LAG_MS))
    );
    assert_eq!(
        param(&catalog::DERIVATIVES_OI_5M_V1, "grid_ms"),
        Some(ParamValue::Int(GRID_MS))
    );
    assert_eq!(i128::from(mie_domain::location::TOLERANCE_BPS), ZONE_BPS);
    let gate: Vec<String> = ADMITTED.iter().map(ToString::to_string).collect();
    assert_eq!(
        param(&catalog::TRIGGER_EXHAUSTION_V1, "gate_kinds"),
        Some(ParamValue::Text(
            "poc,vah,val,hvn,lvn,structural_high,structural_low,sfp_zone,vwap"
        ))
    );
    assert_eq!(
        gate.join(","),
        "poc,vah,val,hvn,lvn,structural_high,structural_low,sfp_zone,vwap"
    );
}
