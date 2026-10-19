//! The ADR-054 measurement (issue #25, M2; run by M3): an independent model
//! of the ADR-054 decision text, D1–D4, run next to the context sampler,
//! that checks every record it emits and reports what *Accept when* asks
//! for.
//!
//! The sampler runs through the app driver, `mie_app::conditional::
//! sample_contexts`, one event at a time: the engine and the sampler are the
//! caller's, so a drive per event is the same drive as one over the stream
//! (pinned by `mie-app`), and the model reads the engine after each event.
//! The model is written from the ADR text, not from the sampler's
//! internals. It reads what D6 lets the sampler read — the committed state,
//! the closed bars and the trigger events of each accepted event — and
//! rebuilds from them:
//!
//! - the baseline (D1, D3): a sample at every accepted event that closes a
//!   1m bar, once the first 1m bar with trades has closed; its
//!   `minute_end` and anchor are the end and close of the latest closed 1m
//!   bar with trades, its context the ADR-053 D4 context of the anchor
//!   without exclusions (the registry levels whose zone, padded by 6 bps of
//!   the level's price, ADR-044 D3, contains the anchor, and the auction
//!   status) and its bias `Bias::read`;
//! - the trigger samples (D1): each trigger event as emitted, with its own
//!   context and bias, after the baseline, in emission order;
//! - the snapshot (D2): the 13 features of the D2 table, read from the
//!   state by name;
//! - the episodes (D4): the span table per trigger id, the two-sided join
//!   rule, the hull of the closed episodes for bridges, the identity with
//!   its ordinal, the `opener` and `truncated` flags, and the counts per id
//!   over emitted samples (`SamplingStats`).
//!
//! Reported:
//!
//! - **T0** (asserted): no domain rejection on the archive, and at every
//!   event the sampler's records equal the model's, in order and on every
//!   field; at a rejected event, no record; at the end, the sampler's
//!   counts equal the model's.
//! - **Grid invariant** (asserted): every event with a trigger record has a
//!   baseline record (D3).
//! - **Reachability** (asserted by the test that owns the source): on the
//!   archive, every id but the live-only absorption ids has an episode; on
//!   the live store, each absorption id has one. The ids without one are
//!   listed before the assertion.
//! - Recorded only: per id, samples, episodes, samples per episode
//!   (p50/p90/p99/max), episode duration (p50/p99/max), bridges and
//!   truncated episodes; per family, the events at which two or more of
//!   its ids emit and their samples (cross-id co-emissions, D4); the
//!   baseline samples in total, per regime label and per ADR-051 location
//!   bin, the instants that close only empty minutes and those that close
//!   several minutes; the wall time and the peak RSS.
//!
//! An episode's duration is reported twice: its **member span**, from the
//! event that opened it to the event of its last member (how long it kept
//! chaining), and its **data hull**, `last − first + 1 ms` of its data (how
//! much input it covers). Both are in minutes.
//!
//! Every count is printed before any assertion.
//!
//! `measure_contexts` (`--ignored`) runs the archive: aggTrades with the
//! ADR-050 fold (its D8 default) and `metrics` for the open interest, as the
//! ADR-053 measurement. Run it with `MIE_MEASURE_RAW_ROOT=<raw root>
//! MIE_MEASURE_FROM=<YYYY-MM-DD> MIE_MEASURE_TO=<YYYY-MM-DD> cargo test
//! --release -p mie-cli --test context_measure -- --ignored --exact
//! --nocapture measure_contexts` (`--exact` keeps the live test out);
//! `MIE_MEASURE_TO` is included, as in `mie replay`, and an optional
//! `MIE_MEASURE_EVENTS` asserts the replay's event count. M3 runs the #12
//! development year, 2025-10-01 … 2026-09-30, with
//! `MIE_MEASURE_EVENTS=603323345` (ADR-051 R0's count of the same replay).
//!
//! `measure_contexts_live` (`--ignored`) runs every cleanly ended run of a
//! `mie ingest` capture alone through `LiveReplay::run_replay`, each into a
//! fresh engine and sampler, so absorption is sampled where depth exists:
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test context_measure -- --ignored --exact
//! --nocapture measure_contexts_live`.
//!
//! Memory: records stream through the check and are dropped. The model
//! holds one open episode, the closed hull and the last opening per id, and
//! count tables; the per-episode distributions hold three numbers per
//! episode, which grows with the episode count, not with the event count.
//! `run_replay` holds one live run in memory (#140).
//!
//! `model_reproduces_the_sampler_on_a_synthetic_tape` runs T0, the grid
//! invariant and every report section in CI on the seeded tapes of
//! `trigger_measure.rs` (`common::tapes`), and asserts that every compared
//! kind occurred, so T0 cannot pass vacuously: a multi-member episode of
//! each windowed family (effort vs result, absorption, exhaustion), a chained
//! divergence pair, an SFP chain, an emitted truncated episode, records
//! held back before `emit_from`, an event that closes several minutes, and
//! a rejected event after which the engine still shows the previous event's
//! output. It also checks that `sample_window` over the same tape emits the
//! same records.

mod common;

use common::tapes::{TAPE_DAYS, TAPE_START, book_tape, synthetic_tape};
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::conditional::{sample_contexts, sample_window};
use mie_cli::journal::{JournaledRun, read_runs};
use mie_cli::replay::parse_bound;
use mie_domain::bars::Timeframe;
use mie_domain::candidate::{CandidateEvent, ContextLevel, ContextLevels, LocationContext};
use mie_domain::conditional::{
    ContextProvenance, ContextRecord, ContextSampler, Episode, EpisodeId, SAMPLING_V1,
    SNAPSHOT_FEATURES, Sample, SamplingStats, StateSnapshot, TRIGGER_IDS, TriggerStats,
};
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureKey, FeatureValue, catalog};
use mie_domain::fingerprint::Fingerprinter;
use mie_domain::location::{LevelKind, LocationLevel, LocationState};
use mie_domain::order::CanonicalKey;
use mie_domain::regime::RegimeLabel;
use mie_domain::research::DataVersion;
use mie_domain::state::{MarketState, MarketStateEngine};
use mie_domain::time::EventTime;
use mie_domain::trigger::{Bias, OiRejection, TriggerEvent, TriggerEvidence, TriggerFamily};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;

/// D4: the deciding 1m bar of an auction trigger.
const MINUTE_MS: i64 = 60_000;

/// The regime labels, in order; a fifth column counts unlabelled samples.
const LABELS: [RegimeLabel; 4] = [
    RegimeLabel::Low,
    RegimeLabel::Medium,
    RegimeLabel::High,
    RegimeLabel::Extreme,
];

/// ADR-051 R1's location bins, and a fourth column for a context whose
/// levels are not ready.
const BINS: [&str; 4] = [
    "value_edge",
    "structural_extreme",
    "neither",
    "levels_not_ready",
];

/// The snapshot windows of D2, shortest first.
const SNAPSHOT_WINDOWS: [Timeframe; 3] = [Timeframe::M5, Timeframe::M15, Timeframe::H1];

/// D1 by ADR-053 D4: a level's zone is padded by 6 bps of its price, the
/// registry tolerance of ADR-044 D3.
const ZONE_BPS: i128 = 6;

// ---------------------------------------------------------------------------
// The baseline context (D1)
// ---------------------------------------------------------------------------

/// ADR-044 D3's padded zone: whether `price` lies in `[low − w, high + w]`
/// with `w` = 6 bps of the level's price, exact.
fn in_zone(level: &LocationLevel, price: mie_domain::num::Price) -> bool {
    in_padded_zone(level.price, level.low, level.high, price)
}

/// [`in_zone`] on the level's price and zone edges.
fn in_padded_zone(
    reference: mie_domain::num::Price,
    low: mie_domain::num::Price,
    high: mie_domain::num::Price,
    price: mie_domain::num::Price,
) -> bool {
    let price = i128::from(price.units());
    let reach = i128::from(reference.units()).abs() * ZONE_BPS;
    (i128::from(low.units()) - price) * 10_000 <= reach
        && (price - i128::from(high.units())) * 10_000 <= reach
}

/// The context of a baseline anchor (D1): every registry level whose padded
/// zone holds it, nothing excluded, and the auction status. Levels that are
/// not ready pass their validity on with an empty list.
fn baseline_context(anchor: mie_domain::num::Price, location: &LocationState) -> LocationContext {
    let levels = match &location.levels {
        FeatureValue::Ready(set) => FeatureValue::Ready(ContextLevels {
            feature: set.feature,
            levels: set
                .levels()
                .iter()
                .filter(|level| in_zone(level, anchor))
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
        FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
            observed: *observed,
            required: *required,
        },
        FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason: *reason },
    };
    LocationContext {
        anchor,
        levels,
        auction: location.auction,
    }
}

// ---------------------------------------------------------------------------
// The span table (D4)
// ---------------------------------------------------------------------------

/// D4's rows, by trigger id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Row {
    /// Failed auction, failed reclaim, breakout + acceptance: the deciding
    /// bar.
    Auction,
    /// `trigger.sfp.<tf>@1`: the sweep to its resolving bar.
    Sfp,
    /// `trigger.cvd_divergence.<tf>@1`: the swing pair to the confirmation.
    Divergence,
    /// `trigger.effort_result.<w>@1`: the window's minutes.
    Effort,
    /// `trigger.absorption.<w>@1`: its effort window's minutes.
    Absorption,
    /// `trigger.exhaustion@1`: the expansion window to the decay window.
    Exhaustion,
    /// `trigger.oi_expansion_rejection.*`: the rejection and both points.
    Oi,
}

/// Every trigger id with its family and D4 row, in a fixed order.
fn ids() -> Vec<(FeatureKey, TriggerFamily, Row)> {
    let mut ids = vec![
        (
            catalog::TRIGGER_FAILED_AUCTION_V1.key,
            TriggerFamily::FailedAuction,
            Row::Auction,
        ),
        (
            catalog::TRIGGER_FAILED_RECLAIM_V1.key,
            TriggerFamily::FailedReclaim,
            Row::Auction,
        ),
        (
            catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key,
            TriggerFamily::BreakoutAcceptance,
            Row::Auction,
        ),
    ];
    let mut add = |table: &[(Timeframe, &mie_domain::feature::FeatureDefinition)],
                   family: TriggerFamily,
                   row: Row| {
        ids.extend(
            table
                .iter()
                .map(|(_, definition)| (definition.key, family, row)),
        );
    };
    add(&catalog::TRIGGER_SFP, TriggerFamily::Sfp, Row::Sfp);
    add(
        &catalog::TRIGGER_CVD_DIVERGENCE,
        TriggerFamily::CvdDivergence,
        Row::Divergence,
    );
    add(
        &catalog::TRIGGER_EFFORT_RESULT,
        TriggerFamily::EffortVsResult,
        Row::Effort,
    );
    add(
        &catalog::TRIGGER_ABSORPTION,
        TriggerFamily::Absorption,
        Row::Absorption,
    );
    add(
        &catalog::TRIGGER_OI_EXPANSION_REJECTION,
        TriggerFamily::OiExpansionRejection,
        Row::Oi,
    );
    ids.push((
        catalog::TRIGGER_EXHAUSTION_V1.key,
        TriggerFamily::Exhaustion,
        Row::Exhaustion,
    ));
    ids.push((
        catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key,
        TriggerFamily::OiExpansionRejection,
        Row::Oi,
    ));
    ids
}

/// The position of `key` in [`ids`].
fn id_index(key: FeatureKey) -> Option<usize> {
    ids().iter().position(|(id, _, _)| *id == key)
}

/// The position of `family` in [`TriggerFamily::ALL`].
fn family_index(family: TriggerFamily) -> usize {
    TriggerFamily::ALL
        .iter()
        .position(|candidate| *candidate == family)
        .expect("a family")
}

/// A span `[first, last]` in ms, both inclusive.
type Span = (i64, i64);

/// A sweep's span: the sweeping trade to the end of the resolving bar,
/// exclusive (D4; the trigger fires on a resolved sweep, ADR-053 D5).
fn sweep_span(sweep: &mie_domain::structure::Sweep) -> Result<Span, String> {
    let resolved = sweep
        .resolved_bar_end
        .ok_or_else(|| format!("an SFP trigger on an unresolved sweep at {}", sweep.time))?;
    Ok((sweep.time.as_millis(), resolved.as_millis() - 1))
}

/// The deciding 1m bar of an auction status.
fn auction_span(status: &mie_domain::location::AuctionStatus) -> Span {
    let end = status.bar_end.as_millis();
    (end - MINUTE_MS, end - 1)
}

/// D4's span of `trigger`, by its id's row; an error when the evidence is
/// not the kind the row reads.
fn span(trigger: &TriggerEvent, row: Row) -> Result<Span, String> {
    let wrong = || format!("{} carries unexpected evidence", trigger.feature);
    let candidate = match &trigger.evidence {
        TriggerEvidence::Candidate(fact) => Some(fact.as_ref()),
        _ => None,
    };
    match (row, &trigger.evidence, candidate) {
        (Row::Auction, TriggerEvidence::Auction(status), _) => Ok(auction_span(status)),
        (Row::Sfp, TriggerEvidence::Sfp(sweep), _) => sweep_span(sweep),
        (Row::Divergence, _, Some(CandidateEvent::CvdDivergence(event))) => {
            let pair = &event.comparison.pair;
            Ok((
                pair.previous.time.as_millis(),
                pair.current.known_at.as_millis() - 1,
            ))
        }
        (Row::Effort, _, Some(CandidateEvent::EffortVsResult(event))) => {
            let end = event.value.end.as_millis();
            Ok((end - event.value.timeframe.millis(), end - 1))
        }
        (Row::Absorption, _, Some(CandidateEvent::Absorption(event))) => {
            let end = event.value.effort.end.as_millis();
            Ok((end - event.value.effort.timeframe.millis(), end - 1))
        }
        (Row::Exhaustion, _, Some(CandidateEvent::Exhaustion(event))) => {
            let expansion = &event.value.expansion;
            Ok((
                expansion.end.as_millis() - expansion.timeframe.millis(),
                event.value.end.as_millis() - 1,
            ))
        }
        (Row::Oi, TriggerEvidence::OiExpansion(expansion), _) => {
            let (first, last) = match &expansion.rejection {
                OiRejection::Sfp(sweep) => sweep_span(sweep)?,
                OiRejection::FailedAuction(status) => auction_span(status),
            };
            Ok((
                first.min(expansion.start.boundary.as_millis()),
                last.max(expansion.end.boundary.as_millis()),
            ))
        }
        _ => Err(wrong()),
    }
}

/// Whether two spans share a millisecond.
fn overlaps(a: Span, b: Span) -> bool {
    a.0 <= b.1 && b.0 <= a.1
}

/// The hull of two spans.
fn hull(a: Span, b: Span) -> Span {
    (a.0.min(b.0), a.1.max(b.1))
}

// ---------------------------------------------------------------------------
// The model (D1–D4)
// ---------------------------------------------------------------------------

/// The open episode of one id.
#[derive(Debug, Clone, Copy)]
struct Open {
    id: EpisodeId,
    data: Span,
    truncated: bool,
    /// Emitted samples.
    emitted: u64,
    /// The event times of the opener and of the last member.
    opened_ms: i64,
    last_member_ms: i64,
}

/// One id's episode state and counts.
#[derive(Debug, Default)]
struct Slot {
    open: Option<Open>,
    /// The hull of the closed episodes' data.
    closed: Option<Span>,
    /// The last event at which the id opened an episode, and how many.
    opened: Option<(CanonicalKey, u32)>,
    /// Counts over emitted samples, as `SamplingStats` keeps them.
    stats: TriggerStats,
    /// Per emitted episode: samples, member span and data hull (ms).
    sizes: Vec<u64>,
    member_spans: Vec<i64>,
    hulls: Vec<i64>,
    /// Emitted samples of multi-member episodes, for the CI twin.
    multi: u64,
    /// Samples held back before `emit_from`.
    held_back: u64,
}

impl Slot {
    /// Records `open`'s distributions when it closes or the run ends.
    fn close(&mut self, open: Open) {
        if open.emitted > 0 {
            self.sizes.push(open.emitted);
            self.member_spans.push(open.last_member_ms - open.opened_ms);
            self.hulls.push(open.data.1 - open.data.0 + 1);
            if open.emitted > 1 {
                self.multi += 1;
            }
        }
    }
}

/// The model of D1–D4.
struct Model {
    emit_from: Option<i64>,
    /// The first event the model observed.
    observed_from: Option<i64>,
    /// The latest closed 1m bar with trades: its end and close.
    last_minute: Option<(i64, mie_domain::num::Price)>,
    slots: Vec<Slot>,
}

/// What one event gave the model beyond its records.
#[derive(Debug, Default)]
struct Observed {
    /// Closed 1m bars, and those with trades.
    minutes: usize,
    minutes_with_trades: usize,
}

impl Model {
    fn new(emit_from: Option<EventTime>) -> Self {
        Self {
            emit_from: emit_from.map(|from| from.as_millis()),
            observed_from: None,
            last_minute: None,
            slots: ids().iter().map(|_| Slot::default()).collect(),
        }
    }

    /// D2: the 13 features of the table, read from the state by name.
    fn snapshot(state: &MarketState) -> StateSnapshot {
        StateSnapshot {
            last_price: state.last_trade_price,
            atr: state.atr,
            aggression: SNAPSHOT_WINDOWS.map(|window| {
                *state
                    .flow
                    .windows
                    .get(window)
                    .expect("a D2 aggression window")
            }),
            cvd_utc_day: state.flow.cvd_utc_day,
            effort: SNAPSHOT_WINDOWS.map(|window| {
                *state
                    .candidates
                    .effort_result(window)
                    .expect("a D2 effort window")
            }),
            liquidity: SNAPSHOT_WINDOWS.map(|window| {
                *state
                    .book
                    .windows
                    .get(window)
                    .expect("a D2 liquidity window")
            }),
            oi_5m: state.derivatives.oi_5m,
        }
    }

    /// The records of the accepted event `key`, as D1–D4 describe them.
    fn observe(
        &mut self,
        key: CanonicalKey,
        engine: &MarketStateEngine,
        notes: &mut Vec<String>,
    ) -> (Vec<ContextRecord>, Observed) {
        let at = key.time.as_millis();
        let observed_from = *self.observed_from.get_or_insert(at);
        let emitting = self.emit_from.is_none_or(|from| at >= from);
        let state = engine.state();
        let mut observed = Observed::default();
        let mut latest: Option<(i64, mie_domain::num::Price)> = None;
        for bar in engine
            .closed_bars()
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M1)
        {
            observed.minutes += 1;
            if let Some(ohlc) = bar.ohlc {
                observed.minutes_with_trades += 1;
                let end = bar.open_time.as_millis() + MINUTE_MS;
                if latest.is_none_or(|(seen, _)| end > seen) {
                    latest = Some((end, ohlc.close));
                }
            }
        }
        if latest.is_some() {
            self.last_minute = latest;
        }
        let mut records = Vec::new();
        let triggers = engine.trigger_events();
        if (observed.minutes == 0 || self.last_minute.is_none()) && triggers.is_empty() {
            return (records, observed);
        }
        let snapshot = Self::snapshot(state);
        if observed.minutes > 0
            && let Some((end, close)) = self.last_minute
            && emitting
        {
            records.push(ContextRecord {
                event: key,
                sample: Sample::Baseline {
                    minute_end: EventTime::from_millis(end),
                },
                context: baseline_context(close, &state.location),
                bias: Bias::read(state),
                state: snapshot,
            });
        }
        let ids = ids();
        for trigger in triggers {
            let Some(index) = id_index(trigger.feature) else {
                notes.push(format!("{at}: {} is not a trigger id", trigger.feature));
                continue;
            };
            let span = match span(trigger, ids[index].2) {
                Ok(span) => span,
                Err(note) => {
                    notes.push(format!("{at}: {note}"));
                    continue;
                }
            };
            let slot = &mut self.slots[index];
            let bridge = slot.closed.is_some_and(|closed| overlaps(closed, span));
            let joined = match &mut slot.open {
                Some(open) if overlaps(open.data, span) => {
                    open.data = hull(open.data, span);
                    open.last_member_ms = at;
                    Some(Episode {
                        id: open.id,
                        opener: false,
                        truncated: open.truncated,
                        bridge,
                    })
                }
                _ => None,
            };
            let episode = match joined {
                Some(episode) => episode,
                None => {
                    let ordinal = match slot.opened {
                        Some((opened_at, count)) if opened_at == key => count,
                        _ => 0,
                    };
                    slot.opened = Some((key, ordinal + 1));
                    if let Some(open) = slot.open.take() {
                        slot.closed = Some(slot.closed.map_or(open.data, |c| hull(c, open.data)));
                        slot.close(open);
                    }
                    let id = EpisodeId {
                        trigger: trigger.feature,
                        opened_at: key,
                        ordinal,
                    };
                    let truncated = span.0 < observed_from;
                    slot.open = Some(Open {
                        id,
                        data: span,
                        truncated,
                        emitted: 0,
                        opened_ms: at,
                        last_member_ms: at,
                    });
                    Episode {
                        id,
                        opener: true,
                        truncated,
                        bridge,
                    }
                }
            };
            if !emitting {
                slot.held_back += 1;
                continue;
            }
            let open = slot.open.as_mut().expect("the trigger's episode is open");
            open.emitted += 1;
            slot.stats.samples += 1;
            if open.emitted == 1 {
                slot.stats.episodes += 1;
                slot.stats.truncated += u64::from(episode.truncated);
            }
            slot.stats.bridges += u64::from(episode.bridge);
            records.push(ContextRecord {
                event: key,
                sample: Sample::Trigger {
                    trigger: Box::new(trigger.clone()),
                    episode,
                },
                context: trigger.context.clone(),
                bias: trigger.bias,
                state: snapshot,
            });
        }
        (records, observed)
    }

    /// Closes every open episode for the distributions.
    fn finish(&mut self) {
        for slot in &mut self.slots {
            if let Some(open) = slot.open.take() {
                slot.close(open);
            }
        }
    }

    /// The counts the sampler keeps, from the model.
    fn stats(&self, records: u64) -> SamplingStats {
        let mut stats = SamplingStats {
            records,
            ..SamplingStats::default()
        };
        for ((key, _, _), slot) in ids().iter().zip(&self.slots) {
            let position = TRIGGER_IDS
                .iter()
                .position(|(id, _)| id == key)
                .expect("every id has a stats slot");
            stats.per_trigger[position] = slot.stats;
            stats.episodes += slot.stats.episodes;
            stats.bridges += slot.stats.bridges;
            stats.truncated += slot.stats.truncated;
        }
        stats
    }
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

/// Mismatches between the sampler and the model.
#[derive(Debug, Default)]
struct Check {
    mismatches: u64,
    notes: Vec<String>,
}

impl Check {
    fn note(&mut self, note: String) {
        self.mismatches += 1;
        if self.notes.len() < 20 {
            self.notes.push(note);
        }
    }

    /// Compares one event's records, naming the fields that differ.
    fn records(&mut self, at: i64, actual: &[ContextRecord], expected: &[ContextRecord]) {
        if actual.len() != expected.len() {
            self.note(format!(
                "{at}: the sampler emitted {} records, the model expects {}",
                actual.len(),
                expected.len()
            ));
            return;
        }
        for (index, (a, e)) in actual.iter().zip(expected).enumerate() {
            if a == e {
                continue;
            }
            let mut fields = Vec::new();
            if a.event != e.event {
                fields.push("event");
            }
            match (&a.sample, &e.sample) {
                (Sample::Baseline { .. }, Sample::Baseline { .. }) => {
                    if a.sample != e.sample {
                        fields.push("minute_end");
                    }
                }
                (
                    Sample::Trigger {
                        trigger: actual_trigger,
                        episode: ae,
                    },
                    Sample::Trigger {
                        trigger: expected_trigger,
                        episode: ee,
                    },
                ) => {
                    if actual_trigger != expected_trigger {
                        fields.push("trigger");
                    }
                    if ae.id != ee.id {
                        fields.push("episode id");
                    }
                    if ae.opener != ee.opener {
                        fields.push("opener");
                    }
                    if ae.truncated != ee.truncated {
                        fields.push("truncated");
                    }
                    if ae.bridge != ee.bridge {
                        fields.push("bridge");
                    }
                }
                _ => fields.push("sample kind"),
            }
            if a.context != e.context {
                fields.push("context");
            }
            if a.bias != e.bias {
                fields.push("bias");
            }
            if a.state != e.state {
                fields.push("state");
            }
            self.note(format!("{at}: record {index} differs in {fields:?}: {a}"));
        }
    }
}

/// The baseline counts of the report.
#[derive(Debug, Default)]
struct Baselines {
    total: u64,
    labels: [u64; 5],
    bins: [u64; 4],
    /// Instants that close only empty minutes.
    empty_only: u64,
    /// Instants that close more than one minute.
    several: u64,
}

/// One drive: the engine, the sampler, the model and the counts.
struct Run {
    engine: MarketStateEngine,
    sampler: ContextSampler,
    model: Model,
    check: Check,
    /// Events delivered, rejected ones included, and the rejections.
    events: u64,
    rejections: u64,
    /// Events warmed into the engine before the sampler observed.
    warmed: u64,
    /// Rejected events after which the engine still showed bars or
    /// triggers: where an unguarded sampler would repeat records.
    rejected_with_output: u64,
    records: u64,
    /// Events with a trigger record but no baseline record (D3).
    grid_violations: u64,
    baselines: Baselines,
    /// Per family: events at which two or more of its ids emit, and their
    /// samples.
    coemissions: [(u64, u64); 9],
    /// The digest of every record's line, in order.
    digest: Fingerprinter,
}

impl Run {
    fn new(emit_from: Option<EventTime>) -> Self {
        Self {
            engine: MarketStateEngine::new(),
            sampler: ContextSampler::new(emit_from),
            model: Model::new(emit_from),
            check: Check::default(),
            events: 0,
            rejections: 0,
            warmed: 0,
            rejected_with_output: 0,
            records: 0,
            grid_violations: 0,
            baselines: Baselines::default(),
            coemissions: [(0, 0); 9],
            digest: Fingerprinter::new(),
        }
    }

    /// Drives `event` into the engine alone: the sampler and the model
    /// observe later, so episodes reaching back here are truncated.
    fn warm(&mut self, event: MarketEvent) {
        self.warmed += 1;
        mie_app::drive_tolerant(&mut One(Some(event)), &mut self.engine, |_| {})
            .expect("a one-event provider cannot fail");
    }

    /// Drives `event` through the sampler and checks its records.
    fn step(&mut self, event: MarketEvent) {
        let key = event.canonical_key();
        let at = key.time.as_millis();
        let mut actual = Vec::new();
        let report = sample_contexts(
            &mut One(Some(event)),
            &mut self.engine,
            &mut self.sampler,
            |_| {},
            |record| actual.push(record),
        )
        .expect("a one-event provider cannot fail");
        self.events += report.events;
        if report.rejections > 0 {
            self.rejections += report.rejections;
            if !self.engine.closed_bars().is_empty() || !self.engine.trigger_events().is_empty() {
                self.rejected_with_output += 1;
            }
            if !actual.is_empty() {
                self.check.note(format!(
                    "{at}: {} records at a rejected event",
                    actual.len()
                ));
            }
            return;
        }
        let mut notes = Vec::new();
        let (expected, observed) = self.model.observe(key, &self.engine, &mut notes);
        for note in notes {
            self.check.note(note);
        }
        self.check.records(at, &actual, &expected);
        self.count(&actual, &observed);
    }

    /// The report's counts over one event's records.
    fn count(&mut self, records: &[ContextRecord], observed: &Observed) {
        let mut baseline = false;
        let mut triggers = false;
        let mut families: [(Vec<FeatureKey>, u64); 9] = Default::default();
        for record in records {
            self.records += 1;
            self.digest.write_str(&record.to_string());
            match &record.sample {
                Sample::Baseline { .. } => {
                    baseline = true;
                    let counts = &mut self.baselines;
                    counts.total += 1;
                    counts.labels[label(&record.bias)] += 1;
                    for (count, hit) in counts.bins.iter_mut().zip(bins(&record.context)) {
                        *count += u64::from(hit);
                    }
                    if observed.minutes_with_trades == 0 {
                        counts.empty_only += 1;
                    }
                    if observed.minutes > 1 {
                        counts.several += 1;
                    }
                }
                Sample::Trigger { trigger, .. } => {
                    triggers = true;
                    let (keys, samples) = &mut families[family_index(trigger.family)];
                    if !keys.contains(&trigger.feature) {
                        keys.push(trigger.feature);
                    }
                    *samples += 1;
                }
            }
        }
        if triggers && !baseline {
            self.grid_violations += 1;
        }
        for ((keys, samples), counts) in families.iter().zip(&mut self.coemissions) {
            if keys.len() > 1 {
                counts.0 += 1;
                counts.1 += samples;
            }
        }
    }

    /// Closes the model and compares the counts.
    fn finish(&mut self) {
        self.model.finish();
        let expected = self.model.stats(self.records);
        let actual = self.sampler.stats();
        if actual != expected {
            self.check.note(format!(
                "the sampler's counts differ: {actual:?} vs the model's {expected:?}"
            ));
        }
    }

    /// The model's slot of `key`.
    fn slot(&self, key: FeatureKey) -> &Slot {
        &self.model.slots[id_index(key).expect("an id")]
    }
}

/// The regime label column of `bias`: the label, or 4 when unlabelled.
fn label(bias: &Bias) -> usize {
    bias.regime.ready().map_or(LABELS.len(), |regime| {
        LABELS
            .iter()
            .position(|label| *label == regime.label)
            .expect("a label")
    })
}

/// ADR-051 R1's bins of a context: `value_edge` for a VAH or VAL,
/// `structural_extreme` for a structural high or low, both when both,
/// `neither` when neither; the fourth column when its levels are not ready.
fn bins(context: &LocationContext) -> [bool; 4] {
    let Some(levels) = context.levels.ready() else {
        return [false, false, false, true];
    };
    let mut bins = [false; 4];
    for level in &levels.levels {
        match level.kind {
            LevelKind::Vah | LevelKind::Val => bins[0] = true,
            LevelKind::StructuralHigh | LevelKind::StructuralLow => bins[1] = true,
            _ => {}
        }
    }
    bins[2] = !bins[0] && !bins[1];
    bins
}

/// The nearest-rank quantile `q` of `values`.
fn quantile<T: Copy + Ord>(values: &[T], q: f64) -> Option<T> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let rank = ((q * sorted.len() as f64).ceil() as usize).max(1);
    sorted.get(rank - 1).copied()
}

/// `p50/p90/p99/max` of `values`, or `-`.
fn quantiles<T: Copy + Ord + std::fmt::Display>(values: &[T], qs: &[f64]) -> String {
    let shown: Vec<String> = qs
        .iter()
        .map(|q| quantile(values, *q).map_or("-".to_owned(), |v| v.to_string()))
        .collect();
    shown.join("/")
}

/// Minutes of each value in ms, one decimal, as `quantiles` shows them.
fn minutes(values: &[i64], qs: &[f64]) -> String {
    let shown: Vec<String> = qs
        .iter()
        .map(|q| {
            quantile(values, *q).map_or("-".to_owned(), |ms| format!("{:.1}", ms as f64 / 6e4))
        })
        .collect();
    shown.join("/")
}

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

fn report_t0(run: &Run) {
    println!(
        "T0: events {} (warmed {}), rejections {} (with output {}), records {}, mismatches {}, \
         grid violations {}",
        run.events + run.warmed,
        run.warmed,
        run.rejections,
        run.rejected_with_output,
        run.records,
        run.check.mismatches,
        run.grid_violations
    );
    for note in &run.check.notes {
        println!("T0 mismatch: {note}");
    }
}

fn report(run: &Run, wall: Duration) {
    report_t0(run);
    println!(
        "per id: samples, episodes, samples per episode p50/p90/p99/max, member span and data \
         hull in min p50/p99/max, bridges, truncated, held back"
    );
    for ((key, _, _), slot) in ids().iter().zip(&run.model.slots) {
        let stats = slot.stats;
        println!(
            "  {key}: samples {} episodes {} per-episode {} span {} hull {} bridges {} \
             truncated {} held-back {}",
            stats.samples,
            stats.episodes,
            quantiles(&slot.sizes, &[0.5, 0.9, 0.99, 1.0]),
            minutes(&slot.member_spans, &[0.5, 0.99, 1.0]),
            minutes(&slot.hulls, &[0.5, 0.99, 1.0]),
            stats.bridges,
            stats.truncated,
            slot.held_back
        );
    }
    println!("cross-id co-emissions per family: events, samples");
    for (family, (events, samples)) in TriggerFamily::ALL.iter().zip(&run.coemissions) {
        println!("  {family}: {events} {samples}");
    }
    let baselines = &run.baselines;
    println!(
        "baseline samples {}: per label low/medium/high/extreme/unlabelled {:?}; per bin {} \
         {:?}; closing only empty minutes {}; closing several minutes {}",
        baselines.total,
        baselines.labels,
        BINS.join("/"),
        baselines.bins,
        baselines.empty_only,
        baselines.several
    );
    println!("wall {:.1} s, peak RSS {}", wall.as_secs_f64(), peak_rss());
}

/// The ids among `wanted` with no emitted episode, listed for the report.
fn unreached(run: &Run, wanted: impl Fn(TriggerFamily) -> bool) -> Vec<FeatureKey> {
    ids()
        .iter()
        .zip(&run.model.slots)
        .filter(|((_, family, _), slot)| wanted(*family) && slot.stats.episodes == 0)
        .map(|((key, _, _), _)| *key)
        .collect()
}

#[test]
#[ignore = "measures the archive for ADR-054; see the module docs"]
fn measure_contexts() {
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
    let Replay {
        mut stream,
        dataset,
    } = replay.replay(window).unwrap();
    let started = Instant::now();
    let mut run = Run::new(None);
    // D5, as `sample_window` builds it.
    println!(
        "provenance {}",
        ContextProvenance::new(DataVersion::from(&dataset), &run.engine)
    );
    while let Some(event) = stream.next_event().unwrap() {
        run.step(event);
    }
    run.finish();
    println!("window {from}..{to} ms");
    report(&run, started.elapsed());
    let zeros = unreached(&run, |family| family != TriggerFamily::Absorption);
    println!("reachability: ids without an episode {zeros:?}");
    if let Some(expected) = expected_events {
        println!("T0: events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    assert_eq!(run.rejections, 0, "the engine rejected events");
    assert_eq!(
        run.check.mismatches, 0,
        "the model disagrees with the sampler"
    );
    assert_eq!(
        run.grid_violations, 0,
        "a trigger instant is not a baseline instant"
    );
    assert!(zeros.is_empty(), "ids without an episode: {zeros:?}");
}

#[test]
#[ignore = "samples the contexts of a live capture for ADR-054; see the module docs"]
fn measure_contexts_live() {
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
    let (mut mismatches, mut rejections, mut violations) = (0, 0, 0);
    let mut absorption = [0_u64; 3];
    let absorption_ids: Vec<FeatureKey> = catalog::TRIGGER_ABSORPTION
        .iter()
        .map(|(_, definition)| definition.key)
        .collect();
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        let mut run = Run::new(None);
        println!(
            "run {id}: provenance {}",
            ContextProvenance::new(DataVersion::from(replay.dataset()), &run.engine)
        );
        let started = Instant::now();
        while let Some(event) = replay.next_event().unwrap() {
            run.step(event);
        }
        run.finish();
        report(&run, started.elapsed());
        mismatches += run.check.mismatches;
        rejections += run.rejections;
        violations += run.grid_violations;
        for (count, key) in absorption.iter_mut().zip(&absorption_ids) {
            *count += run.slot(*key).stats.episodes;
        }
    }
    println!(
        "live T0: mismatches {mismatches}, rejections {rejections}, grid violations \
         {violations}; absorption episodes 5m/15m/1h {absorption:?}"
    );
    assert_eq!(rejections, 0, "the engine rejected events");
    assert_eq!(mismatches, 0, "the model disagrees with the sampler");
    assert_eq!(violations, 0, "a trigger instant is not a baseline instant");
    assert!(
        absorption.iter().all(|count| *count > 0),
        "an absorption id has no episode on the live store: {absorption:?}"
    );
}

// ---------------------------------------------------------------------------
// CI twins
// ---------------------------------------------------------------------------

/// The synthetic tape with one rejected event: a repeat of the first trade
/// after day 2 that opens a new minute, right after it, so the engine
/// still shows the bar that trade closed.
fn tape_with_a_repeat() -> Vec<MarketEvent> {
    let mut tape = synthetic_tape();
    let mut previous_minute = None;
    let index = tape
        .iter()
        .position(|event| {
            let MarketEvent::Trade(trade) = event else {
                return false;
            };
            let minute = trade.time.as_millis() / MINUTE_MS;
            let opens = previous_minute.is_some_and(|previous| minute > previous);
            previous_minute = Some(minute);
            opens && trade.time.as_millis() >= TAPE_START + 2 * DAY_MS
        })
        .expect("the tape has trades after day 2");
    let repeat = tape[index].clone();
    tape.insert(index + 1, repeat);
    tape
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

/// The emitted samples of multi-member episodes over the ids of `family`.
fn multi(run: &Run, family: TriggerFamily) -> u64 {
    ids()
        .iter()
        .zip(&run.model.slots)
        .filter(|((_, of, _), _)| *of == family)
        .map(|(_, slot)| slot.multi)
        .sum()
}

#[test]
fn model_reproduces_the_sampler_on_a_synthetic_tape() {
    // A: the tape with a repeat, observed and emitted from its start.
    let tape = tape_with_a_repeat();
    let started = Instant::now();
    let mut full = Run::new(None);
    for event in tape.iter().cloned() {
        full.step(event);
    }
    full.finish();
    report(&full, started.elapsed());

    // B: the engine warms up alone for four days, the sampler observes
    // from there and emits six hours later.
    let observe_from = TAPE_START + 4 * DAY_MS;
    let emit_from = EventTime::from_millis(observe_from + 6 * HOUR_MS);
    let mut late = Run::new(Some(emit_from));
    for event in synthetic_tape() {
        if event.time().as_millis() < observe_from {
            late.warm(event);
        } else {
            late.step(event);
        }
    }
    late.finish();
    report_t0(&late);

    // C: absorption on the book tape.
    let mut book = Run::new(None);
    for event in book_tape() {
        book.step(event);
    }
    book.finish();
    report(&book, Duration::ZERO);

    // `sample_window` over A's tape emits A's records.
    let dataset = DatasetVersion::from_hex(&"5a".repeat(32)).unwrap();
    let history = History(tape, dataset.clone());
    let window = ReplayWindow {
        start: EventTime::from_millis(TAPE_START),
        end: EventTime::from_millis(TAPE_START + (TAPE_DAYS + 1) * DAY_MS),
    };
    let mut digest = Fingerprinter::new();
    let mut records = 0_u64;
    let (provenance, windowed) = sample_window(&history, window, None, |record| {
        records += 1;
        digest.write_str(&record.to_string());
    })
    .unwrap();
    println!(
        "sample_window: provenance {provenance}, events {}, rejections {}, records {records}",
        windowed.events, windowed.rejections
    );

    for run in [&full, &late, &book] {
        assert_eq!(run.check.mismatches, 0, "{:#?}", run.check.notes);
        assert_eq!(
            run.grid_violations, 0,
            "a trigger instant is not a baseline instant"
        );
    }
    assert_eq!(full.rejections, 1, "the repeat is the one rejection");
    assert_eq!(late.rejections + book.rejections, 0);
    assert_eq!(
        (windowed.events, windowed.rejections, records),
        (full.events, full.rejections, full.records)
    );
    assert_eq!(digest.finish(), full.digest.finish());
    assert_eq!(windowed.stats, full.sampler.stats());
    assert_eq!(provenance.data, DataVersion::from(&dataset));
    assert_eq!(provenance.policy, SAMPLING_V1);

    // Every compared kind occurred, so T0 did not pass vacuously.
    let truncated_episodes: u64 = late
        .model
        .slots
        .iter()
        .map(|slot| slot.stats.truncated)
        .sum();
    let held_back: u64 = late.model.slots.iter().map(|slot| slot.held_back).sum();
    let kinds = [
        (
            "multi-member effort-vs-result episodes",
            multi(&full, TriggerFamily::EffortVsResult),
        ),
        (
            "multi-member absorption episodes",
            multi(&book, TriggerFamily::Absorption),
        ),
        (
            "multi-member exhaustion episodes",
            multi(&full, TriggerFamily::Exhaustion),
        ),
        (
            "chained CVD divergences",
            multi(&full, TriggerFamily::CvdDivergence),
        ),
        ("chained SFPs", multi(&full, TriggerFamily::Sfp)),
        ("emitted truncated episodes", truncated_episodes),
        ("trigger samples held back before emit_from", held_back),
        (
            "baseline instants closing several minutes",
            full.baselines.several,
        ),
        ("rejected events with output", full.rejected_with_output),
    ];
    for (kind, count) in kinds {
        assert!(count > 0, "the tapes compared no {kind}");
    }
}

#[test]
fn the_model_zone_includes_its_exact_padded_edges() {
    // The vectors of mie-domain's `zone_contains_includes_its_exact_padded_edges`,
    // so the model and the domain agree at the boundary: a zone
    // 59 990 … 60 010 USDT around 60 000 USDT, padded by 6 bps of 60 000 =
    // 36 USDT exactly, on both bounds just inside, exactly on and just
    // outside the padded edge.
    use mie_domain::num::{Price, SCALE};
    let (reference, low, high) = (60_000 * SCALE, 59_990 * SCALE, 60_010 * SCALE);
    let reach = 36 * SCALE;
    assert_eq!(i128::from(reference) * ZONE_BPS, i128::from(reach) * 10_000);
    let vectors = [
        (low - reach + 1, true),
        (low - reach, true),
        (low - reach - 1, false),
        (high + reach - 1, true),
        (high + reach, true),
        (high + reach + 1, false),
    ];
    for (units, inside) in vectors {
        assert_eq!(
            in_padded_zone(
                Price::from_units(reference),
                Price::from_units(low),
                Price::from_units(high),
                Price::from_units(units),
            ),
            inside,
            "{units}"
        );
    }
}

#[test]
fn the_model_tables_are_the_adrs() {
    // D2's table, by name, is the sampler's snapshot list.
    assert_eq!(
        SNAPSHOT_FEATURES,
        [
            catalog::TRADE_LAST_PRICE_V1.key,
            catalog::VOLATILITY_ATR_1H_V1.key,
            catalog::FLOW_WINDOW_5M_V1.key,
            catalog::FLOW_WINDOW_15M_V1.key,
            catalog::FLOW_WINDOW_1H_V1.key,
            catalog::FLOW_CVD_UTC_DAY_V1.key,
            catalog::CANDIDATE_EFFORT_RESULT_5M_V1.key,
            catalog::CANDIDATE_EFFORT_RESULT_15M_V1.key,
            catalog::CANDIDATE_EFFORT_RESULT_1H_V1.key,
            catalog::BOOK_LIQUIDITY_WINDOW_5M_V1.key,
            catalog::BOOK_LIQUIDITY_WINDOW_15M_V1.key,
            catalog::BOOK_LIQUIDITY_WINDOW_1H_V1.key,
            catalog::DERIVATIVES_OI_5M_V1.key,
        ]
    );
    // The zone padding is the registry's tolerance.
    assert_eq!(i128::from(mie_domain::location::TOLERANCE_BPS), ZONE_BPS);
    // D4's table covers exactly the sampler's trigger ids.
    let mut model: Vec<FeatureKey> = ids().iter().map(|(key, _, _)| *key).collect();
    let mut sampler: Vec<FeatureKey> = TRIGGER_IDS.iter().map(|(key, _)| *key).collect();
    model.sort();
    sampler.sort();
    assert_eq!(model, sampler);
}
