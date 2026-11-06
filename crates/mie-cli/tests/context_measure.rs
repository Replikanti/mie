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
//! - **Time of day** (recorded only, #147): the baseline and trigger
//!   samples per UTC hour of the record's event time and per session
//!   (`asia` 00–08, `europe` 08–13, `us` 13–21, `late` 21–24 UTC), each
//!   with its n and share, the share ratio triggers / baseline, and per
//!   family its n and the share of the family's own total.
//! - **Effective sample size** (recorded only, #147): on the minute grid
//!   of the baseline samples, the 1m log return of the anchor and the four
//!   bin indicators, each with its sample ACF (divisor n) at the checkpoint
//!   lags 1, 5, 15, 60, 240, 1 440 and 10 080 and Geyer's (1992) initial
//!   positive sequence ESS, searched up to K = min(10 080, ⌊n/4⌋) lags and
//!   marked `capped` when the rule has not stopped by K. It is the
//!   effective count for the mean of each series, not the number of
//!   independent forward outcomes at a horizon.
//! - **Reconciliation** (asserted): for the baseline and each id, the hour
//!   and the session counts add up to the samples; the present grid
//!   minutes plus the repeats add up to the baseline samples.
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
//! `run_replay` holds about one seal interval of a live run's records
//! (ADR-063; the whole run before #140). The minute grid holds
//! 9 bytes a minute, about 5 MB for the year; the ESS reads one O(n) pass
//! per lag summed and per checkpoint. The time-of-day and ESS sections need
//! nothing new: the run commands above are unchanged.
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

use common::bootstrap::SplitMix64;
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
// Time of day and effective sample size (#147)
// ---------------------------------------------------------------------------

/// The four UTC sessions, `[start, end)` in UTC hours, a partition of the
/// day so their totals reconcile with the hour table. Tokyo and Hong Kong
/// trade 00–08 UTC (no DST); London's hours contain 08–13 in BST and GMT,
/// before the US open; the NYSE session (13:30–20:00 UTC in EDT, 14:30–21:00
/// in EST) lies inside 13–21 in both regimes; no major cash market is open
/// 21–24. Hours 07, 13 and 20 change local meaning with DST.
const SESSIONS: [(&str, usize, usize); 4] = [
    ("asia", 0, 8),
    ("europe", 8, 13),
    ("us", 13, 21),
    ("late", 21, 24),
];

/// The longest ACF lag read: one week in minutes, the longest regular
/// cycle measured on this data (#155's hour-of-week cycle); a series also
/// stops at ⌊n/4⌋, Box and Jenkins' limit for reading a sample ACF.
const MAX_LAG: usize = 10_080;

/// The ACF lags printed, in minutes: the ADR-031 bar spans and the ADR-055
/// horizons, plus one day and one week, where a periodic component that
/// stopped Geyer's sum early would show.
const CHECKPOINTS: [usize; 7] = [1, 5, 15, 60, 240, 1_440, 10_080];

/// The UTC hour of `ms`, times before the epoch included.
fn utc_hour(ms: i64) -> usize {
    (ms.rem_euclid(DAY_MS) / HOUR_MS) as usize
}

/// The position in [`SESSIONS`] of the session holding `hour`.
fn session(hour: usize) -> usize {
    SESSIONS
        .iter()
        .position(|(_, start, end)| (*start..*end).contains(&hour))
        .expect("an hour of the day")
}

/// 24 hour counts summed into the [`SESSIONS`].
fn per_session(hours: &[u64]) -> Vec<u64> {
    let mut sessions = vec![0; SESSIONS.len()];
    for (hour, count) in hours.iter().enumerate() {
        sessions[session(hour)] += count;
    }
    sessions
}

/// Emitted samples per UTC hour of the record's event time: the baseline,
/// and the triggers per id in [`ids`] order. Every trigger instant is a
/// baseline instant (D3), so both kinds share one clock.
#[derive(Debug)]
struct TimeOfDay {
    baseline: [u64; 24],
    triggers: Vec<[u64; 24]>,
}

impl TimeOfDay {
    fn new() -> Self {
        Self {
            baseline: [0; 24],
            triggers: vec![[0; 24]; ids().len()],
        }
    }

    fn add_baseline(&mut self, ms: i64) {
        self.baseline[utc_hour(ms)] += 1;
    }

    fn add_trigger(&mut self, id_index: usize, ms: i64) {
        self.triggers[id_index][utc_hour(ms)] += 1;
    }

    /// The trigger hours per family, in [`TriggerFamily::ALL`] order.
    fn families(&self) -> Vec<Vec<u64>> {
        let mut families = vec![vec![0; 24]; TriggerFamily::ALL.len()];
        for ((_, family, _), hours) in ids().iter().zip(&self.triggers) {
            for (sum, count) in families[family_index(*family)].iter_mut().zip(hours) {
                *sum += count;
            }
        }
        families
    }
}

/// The present bit of a [`MinuteGrid`] mask; bits 0–3 are the [`BINS`].
const PRESENT: u8 = 1 << 4;

/// The baseline samples on the minute grid: one slot per minute from the
/// first sample's `minute_end`, holding ln of the anchor's units (NaN when
/// absent) and a mask of the present bit and the bins, 9 bytes a minute. A
/// minute no sample names (no trades, D1) stays absent; a sample whose
/// `minute_end` is already filled, or lies before the first, is counted in
/// `repeats` and stays out of the series.
#[derive(Debug, Default)]
struct MinuteGrid {
    first: Option<i64>,
    log_anchor: Vec<f64>,
    masks: Vec<u8>,
    present: u64,
    repeats: u64,
}

impl MinuteGrid {
    fn add(&mut self, minute_end: i64, anchor_units: i64, bins: [bool; 4]) {
        let offset = minute_end - *self.first.get_or_insert(minute_end);
        let Ok(index) = usize::try_from(offset / MINUTE_MS) else {
            self.repeats += 1;
            return;
        };
        if index >= self.masks.len() {
            self.log_anchor.resize(index + 1, f64::NAN);
            self.masks.resize(index + 1, 0);
        }
        if self.masks[index] & PRESENT != 0 {
            self.repeats += 1;
            return;
        }
        self.log_anchor[index] = (anchor_units as f64).ln();
        self.masks[index] = bins
            .iter()
            .enumerate()
            .fold(PRESENT, |mask, (bit, hit)| mask | (u8::from(*hit) << bit));
        self.present += 1;
    }

    fn is_present(&self, index: usize) -> bool {
        self.masks[index] & PRESENT != 0
    }

    /// The 1m log return of the anchor, ln(anchor_t / anchor_{t−1}), at
    /// every minute whose predecessor is present too.
    fn returns(&self) -> Vec<Option<f64>> {
        (0..self.masks.len())
            .map(|i| {
                (i > 0 && self.is_present(i) && self.is_present(i - 1))
                    .then(|| self.log_anchor[i] - self.log_anchor[i - 1])
            })
            .collect()
    }

    /// The 0/1 indicator of `BINS[bin]` at every present minute.
    fn indicator(&self, bin: usize) -> Vec<Option<f64>> {
        self.masks
            .iter()
            .map(|mask| (mask & PRESENT != 0).then(|| f64::from((mask >> bin) & 1)))
            .collect()
    }
}

/// The sample ACF of a series with absent values: centred on the mean of
/// the present values, divisor n (the present count) at every lag, the
/// biased, positive semi-definite estimator Geyer's rule is defined on. A
/// lag-k product counts only when both values are present: absent values
/// are 0.0 after centring, so their products vanish.
struct Acf {
    centred: Vec<f64>,
    n: usize,
    sum_squares: f64,
}

impl Acf {
    /// `None` for a series without two distinct present values.
    fn new(series: &[Option<f64>]) -> Option<Self> {
        let first = series.iter().flatten().next()?;
        if series.iter().flatten().all(|value| value == first) {
            return None;
        }
        let (n, sum) = series
            .iter()
            .flatten()
            .fold((0_usize, 0.0), |(n, sum), value| (n + 1, sum + value));
        let mean = sum / n as f64;
        let centred: Vec<f64> = series
            .iter()
            .map(|value| value.map_or(0.0, |value| value - mean))
            .collect();
        let sum_squares = centred.iter().map(|x| x * x).sum();
        Some(Self {
            centred,
            n,
            sum_squares,
        })
    }

    /// ρ_k = c_k / c_0; the divisor n of both cancels. One O(len) pass.
    fn rho(&self, k: usize) -> f64 {
        let Some(lagged) = self.centred.get(k..) else {
            return 0.0;
        };
        let products: f64 = self.centred.iter().zip(lagged).map(|(a, b)| a * b).sum();
        products / self.sum_squares
    }
}

/// Geyer's (1992) initial positive sequence over ρ_k.
#[derive(Debug, Clone, Copy)]
struct Geyer {
    /// τ = −1 + 2 Σ Γ_m = 1 + 2 Σ_{k=1}^{2M+1} ρ_k, the inflation n / ESS.
    tau: f64,
    /// The last lag summed, 2M + 1.
    truncation_lag: usize,
    /// No pair sum ≤ 0 by `max_lag`: τ leaves positive dependence out and
    /// the ESS is an upper bound.
    capped: bool,
}

/// Γ_m = ρ_{2m} + ρ_{2m+1}, summed from m = 0 while Γ_m > 0 and 2m + 1 ≤
/// `max_lag`. `rho` is read lazily, each lag once, from ρ_0 = 1. `None`
/// when Γ_0 ≤ 0 or `max_lag` < 1. The plain sequence, not the monotone
/// variant, which lowers later Γ_m and so raises the ESS.
fn geyer(mut rho: impl FnMut(usize) -> f64, max_lag: usize) -> Option<Geyer> {
    let mut sum = 0.0;
    let mut m = 0;
    let capped = loop {
        if 2 * m + 1 > max_lag {
            break true;
        }
        let gamma = rho(2 * m) + rho(2 * m + 1);
        if gamma <= 0.0 {
            break false;
        }
        sum += gamma;
        m += 1;
    };
    (m > 0).then(|| Geyer {
        tau: -1.0 + 2.0 * sum,
        truncation_lag: 2 * m - 1,
        capped,
    })
}

/// The effective sample size of one series.
#[derive(Debug)]
struct Ess {
    /// Present values.
    n: usize,
    /// K = min(`max_lag`, ⌊n/4⌋).
    max_lag: usize,
    tau: f64,
    /// n / τ, not capped at n: a negatively correlated series has more.
    ess: f64,
    truncation_lag: usize,
    capped: bool,
    /// ρ at the [`CHECKPOINTS`] ≤ K.
    checkpoints: Vec<(usize, f64)>,
}

/// The ESS of `series` (the issue's n / (1 + 2 Σ ρ_k), truncated by
/// [`geyer`]); `None` for a constant series, n < 4, or Γ_0 ≤ 0. The cost is
/// O(len) per lag summed and per checkpoint.
fn ess(series: &[Option<f64>], max_lag: usize) -> Option<Ess> {
    let acf = Acf::new(series)?;
    let n = acf.n;
    if n < 4 {
        return None;
    }
    let max_lag = max_lag.min(n / 4);
    let result = geyer(|k| acf.rho(k), max_lag)?;
    let checkpoints = CHECKPOINTS
        .iter()
        .filter(|lag| **lag <= max_lag)
        .map(|lag| (*lag, acf.rho(*lag)))
        .collect();
    Some(Ess {
        n,
        max_lag,
        tau: result.tau,
        ess: n as f64 / result.tau,
        truncation_lag: result.truncation_lag,
        capped: result.capped,
        checkpoints,
    })
}

/// The equalities of the time-of-day and grid counts with the run's
/// totals; one line per failed one, naming it.
fn reconcile(run: &Run) -> Vec<String> {
    let mut gaps = Vec::new();
    let mut check = |what: String, hours: &[u64], total: u64| {
        let by_hour: u64 = hours.iter().sum();
        let by_session: u64 = per_session(hours).iter().sum();
        if by_hour != total || by_session != total {
            gaps.push(format!(
                "{what}: hours {by_hour}, sessions {by_session}, samples {total}"
            ));
        }
    };
    check(
        "baseline".to_owned(),
        &run.time_of_day.baseline,
        run.baselines.total,
    );
    for (((key, _, _), hours), slot) in ids()
        .iter()
        .zip(&run.time_of_day.triggers)
        .zip(&run.model.slots)
    {
        check(key.to_string(), hours, slot.stats.samples);
    }
    let grid = &run.grid;
    if grid.present + grid.repeats != run.baselines.total {
        gaps.push(format!(
            "minute grid: present {} + repeats {} vs baseline samples {}",
            grid.present, grid.repeats, run.baselines.total
        ));
    }
    gaps
}

/// One time-of-day table: per row, the baseline and all-trigger n and
/// share, the share ratio triggers / baseline, and per family its n and the
/// share of that family's own total (`-` without samples).
fn print_time_table(labels: &[String], baseline: &[u64], families: &[Vec<u64>]) {
    let share = |n: u64, total: u64| (total > 0).then(|| n as f64 / total as f64);
    let percent =
        |share: Option<f64>| share.map_or("-".to_owned(), |s| format!("{:.2}%", 100.0 * s));
    let triggers: Vec<u64> = (0..labels.len())
        .map(|row| families.iter().map(|family| family[row]).sum())
        .collect();
    let baseline_total: u64 = baseline.iter().sum();
    let trigger_total: u64 = triggers.iter().sum();
    let family_totals: Vec<u64> = families.iter().map(|family| family.iter().sum()).collect();
    for (row, label) in labels.iter().enumerate() {
        let b = share(baseline[row], baseline_total);
        let t = share(triggers[row], trigger_total);
        let ratio = match (t, b) {
            (Some(t), Some(b)) if b > 0.0 => format!("{:.2}", t / b),
            _ => "-".to_owned(),
        };
        let per_family: Vec<String> = TriggerFamily::ALL
            .iter()
            .zip(families)
            .zip(&family_totals)
            .map(|((family, counts), total)| {
                format!(
                    "{family} {} {}",
                    counts[row],
                    percent(share(counts[row], *total))
                )
            })
            .collect();
        println!(
            "  {label}: baseline {} {} triggers {} {} ratio {ratio} | {}",
            baseline[row],
            percent(b),
            triggers[row],
            percent(t),
            per_family.join(" | ")
        );
    }
}

/// The hour table, the session table and the ESS of the grid's series.
fn report_time_of_day(run: &Run) {
    let time = &run.time_of_day;
    let families = time.families();
    println!(
        "per UTC hour of the event time: baseline n share, all triggers n share, share ratio \
         triggers/baseline | per family n and share of the family's total"
    );
    let hours: Vec<String> = (0..24).map(|hour| format!("{hour:02}")).collect();
    print_time_table(&hours, &time.baseline, &families);
    let bounds: Vec<String> = SESSIONS
        .iter()
        .map(|(name, start, end)| format!("{name} {start:02}-{end:02}"))
        .collect();
    println!(
        "per session, [start, end) UTC hours {}; same columns",
        bounds.join(", ")
    );
    let names: Vec<String> = SESSIONS
        .iter()
        .map(|(name, _, _)| (*name).to_owned())
        .collect();
    let session_families: Vec<Vec<u64>> =
        families.iter().map(|family| per_session(family)).collect();
    print_time_table(&names, &per_session(&time.baseline), &session_families);

    let grid = &run.grid;
    let lags: Vec<String> = CHECKPOINTS.iter().map(usize::to_string).collect();
    println!(
        "effective sample size over the minute grid ({} minutes, {} present, {} repeats): \
         sample ACF with divisor n, centred on the mean of the present minutes, a lag product \
         only where both minutes are present; Geyer (1992) initial positive sequence, summed \
         while rho(2m) + rho(2m+1) > 0; K = min({MAX_LAG}, n/4); rho at lags {} (those <= K); \
         tau = n / ESS",
        grid.masks.len(),
        grid.present,
        grid.repeats,
        lags.join("/")
    );
    let mut series = vec![("return_1m".to_owned(), grid.returns())];
    series.extend(
        BINS.iter()
            .enumerate()
            .map(|(bin, name)| ((*name).to_owned(), grid.indicator(bin))),
    );
    for (name, values) in series {
        match ess(&values, MAX_LAG) {
            Some(e) => {
                let rho: Vec<String> = e
                    .checkpoints
                    .iter()
                    .map(|(lag, rho)| format!("{lag}:{rho:.4}"))
                    .collect();
                println!(
                    "  {name}: n {} K {} rho {} truncation lag {} tau {:.3} ESS {:.1}{}",
                    e.n,
                    e.max_lag,
                    rho.join(" "),
                    e.truncation_lag,
                    e.tau,
                    e.ess,
                    if e.capped { " capped" } else { "" }
                );
            }
            None => println!("  {name}: n {} -", values.iter().flatten().count()),
        }
    }
    println!(
        "caveat: the ESS is the effective count for the mean of each series (e.g. a bin's \
         share), not the number of independent forward outcomes at a horizon; those come from \
         ADR-055 D9 thinning or ADR-058 D6 units"
    );
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
    /// Emitted samples per UTC hour, and the baseline's minute grid (#147).
    time_of_day: TimeOfDay,
    grid: MinuteGrid,
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
            time_of_day: TimeOfDay::new(),
            grid: MinuteGrid::default(),
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
                Sample::Baseline { minute_end } => {
                    baseline = true;
                    let hits = bins(&record.context);
                    self.time_of_day.add_baseline(record.event.time.as_millis());
                    self.grid
                        .add(minute_end.as_millis(), record.context.anchor.units(), hits);
                    let counts = &mut self.baselines;
                    counts.total += 1;
                    counts.labels[label(&record.bias)] += 1;
                    for (count, hit) in counts.bins.iter_mut().zip(hits) {
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
                    if let Some(index) = id_index(trigger.feature) {
                        self.time_of_day
                            .add_trigger(index, record.event.time.as_millis());
                    }
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
    let gaps = reconcile(run);
    if gaps.is_empty() {
        println!("reconciliation: hours, sessions and the minute grid add up");
    } else {
        println!("reconciliation: {}", gaps.join("; "));
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
    report_time_of_day(run);
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
    let gaps = reconcile(&run);
    assert!(
        gaps.is_empty(),
        "the time-of-day counts do not reconcile: {gaps:?}"
    );
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
    let (mut mismatches, mut rejections, mut violations, mut unreconciled) = (0, 0, 0, 0);
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
        unreconciled += reconcile(&run).len();
        for (count, key) in absorption.iter_mut().zip(&absorption_ids) {
            *count += run.slot(*key).stats.episodes;
        }
    }
    println!(
        "live T0: mismatches {mismatches}, rejections {rejections}, grid violations \
         {violations}, unreconciled counts {unreconciled}; absorption episodes 5m/15m/1h \
         {absorption:?}"
    );
    assert_eq!(rejections, 0, "the engine rejected events");
    assert_eq!(mismatches, 0, "the model disagrees with the sampler");
    assert_eq!(violations, 0, "a trigger instant is not a baseline instant");
    assert_eq!(unreconciled, 0, "the time-of-day counts do not reconcile");
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
        let gaps = reconcile(run);
        assert!(
            gaps.is_empty(),
            "the time-of-day counts do not reconcile: {gaps:?}"
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

#[test]
fn utc_hours_and_sessions_partition_the_day() {
    let day = 20_000 * DAY_MS;
    let cases = [
        (day, 0, "asia"),
        (day + 13 * HOUR_MS - 1, 12, "europe"),
        (day + 13 * HOUR_MS, 13, "us"),
        (day + 21 * HOUR_MS - 1, 20, "us"),
        (day + 21 * HOUR_MS, 21, "late"),
        (day + DAY_MS - 1, 23, "late"),
        (0, 0, "asia"),
        (-1, 23, "late"),
        (-DAY_MS, 0, "asia"),
    ];
    for (ms, hour, name) in cases {
        assert_eq!(utc_hour(ms), hour, "{ms}");
        assert_eq!(SESSIONS[session(hour)].0, name, "{ms}");
    }
    // The sessions run contiguously from 0 to 24, so every hour lies in
    // exactly one.
    assert_eq!(SESSIONS[0].1, 0);
    assert_eq!(SESSIONS[SESSIONS.len() - 1].2, 24);
    for pair in SESSIONS.windows(2) {
        assert_eq!(pair[0].2, pair[1].1, "{pair:?}");
    }
    for hour in 0..24 {
        let holding = SESSIONS
            .iter()
            .filter(|(_, start, end)| (*start..*end).contains(&hour))
            .count();
        assert_eq!(holding, 1, "hour {hour}");
    }
}

#[test]
fn time_of_day_counts_reconcile() {
    let day = 20_000 * DAY_MS;
    let at = |hour: i64, minute: i64| day + hour * HOUR_MS + minute * MINUTE_MS;
    let mut run = Run::new(None);
    // Nine baseline samples: eight distinct minutes, two per session, and
    // a repeat of the 23:30 minute.
    let baseline = [0, 7, 8, 12, 13, 20, 21, 23];
    for hour in baseline {
        run.time_of_day.add_baseline(at(hour, 30));
        run.grid
            .add(at(hour, 30), 60_000, [false, false, true, false]);
    }
    run.grid
        .add(at(23, 30), 60_000, [true, false, false, false]);
    run.baselines.total = 9;
    run.time_of_day.add_baseline(at(23, 30));
    // Three samples of the first id: two at 13, one at 22.
    for hour in [13, 13, 22] {
        run.time_of_day.add_trigger(0, at(hour, 59));
    }
    run.model.slots[0].stats.samples = 3;

    let mut hours = [0_u64; 24];
    for hour in [0, 7, 8, 12, 13, 20, 21, 23, 23] {
        hours[hour] += 1;
    }
    assert_eq!(run.time_of_day.baseline, hours);
    assert_eq!(per_session(&run.time_of_day.baseline), [2, 2, 2, 3]);
    let mut trigger = [0_u64; 24];
    trigger[13] = 2;
    trigger[22] = 1;
    assert_eq!(run.time_of_day.triggers[0], trigger);
    assert_eq!(per_session(&run.time_of_day.triggers[0]), [0, 0, 2, 1]);
    let first = ids()[0].1;
    assert_eq!(
        run.time_of_day.families()[family_index(first)],
        trigger.to_vec()
    );
    assert_eq!((run.grid.present, run.grid.repeats), (8, 1));
    assert_eq!(reconcile(&run), Vec::<String>::new());

    // Each perturbed total is named, so the assertion is not vacuous.
    run.baselines.total += 1;
    let gaps = reconcile(&run);
    assert!(
        gaps.iter().any(|gap| gap.starts_with("baseline:")),
        "{gaps:?}"
    );
    assert!(
        gaps.iter().any(|gap| gap.starts_with("minute grid:")),
        "{gaps:?}"
    );
    run.baselines.total -= 1;
    run.model.slots[0].stats.samples += 1;
    let gaps = reconcile(&run);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert!(gaps[0].starts_with(&format!("{}:", ids()[0].0)), "{gaps:?}");
    run.model.slots[0].stats.samples -= 1;
    run.grid.repeats += 1;
    let gaps = reconcile(&run);
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert!(gaps[0].starts_with("minute grid:"), "{gaps:?}");
}

#[test]
fn minute_grid_marks_gaps_and_repeats() {
    let start = 20_000 * DAY_MS + MINUTE_MS;
    let mut grid = MinuteGrid::default();
    // Minutes 0, 1, 3 (2 skipped), 3 again and 4; the repeat's anchor and
    // bins are ignored.
    grid.add(start, 100, [true, false, false, false]);
    grid.add(start + MINUTE_MS, 110, [false, true, false, false]);
    grid.add(start + 3 * MINUTE_MS, 120, [false, false, true, false]);
    grid.add(start + 3 * MINUTE_MS, 999, [false, false, false, true]);
    grid.add(start + 4 * MINUTE_MS, 132, [true, true, false, false]);
    // A minute before the first is a repeat too.
    grid.add(start - MINUTE_MS, 90, [false, false, false, true]);
    assert_eq!((grid.masks.len(), grid.present, grid.repeats), (5, 4, 2));
    let returns = grid.returns();
    let expected = [
        None,
        Some((110.0_f64 / 100.0).ln()),
        None,
        None,
        Some((132.0_f64 / 120.0).ln()),
    ];
    assert_eq!(returns.len(), expected.len());
    for (got, want) in returns.iter().zip(expected) {
        match (got, want) {
            (Some(got), Some(want)) => assert!((got - want).abs() < 1e-12, "{got} vs {want}"),
            _ => assert_eq!(got.is_none(), want.is_none(), "{returns:?}"),
        }
    }
    assert_eq!(
        grid.indicator(0),
        [Some(1.0), Some(0.0), None, Some(0.0), Some(1.0)]
    );
    assert_eq!(
        grid.indicator(1),
        [Some(0.0), Some(1.0), None, Some(0.0), Some(1.0)]
    );
    assert_eq!(
        grid.indicator(2),
        [Some(0.0), Some(0.0), None, Some(1.0), Some(0.0)]
    );
    assert_eq!(
        grid.indicator(3),
        [Some(0.0), Some(0.0), None, Some(0.0), Some(0.0)]
    );
}

#[test]
fn acf_matches_a_hand_computation() {
    // 1..5: mean 3, centred −2, −1, 0, 1, 2, Σx² = 10; lag 1 sums 4 and lag
    // 2 sums −1, over divisor n at both (n − k would give 0.5 at lag 1).
    let acf = Acf::new(&[1.0, 2.0, 3.0, 4.0, 5.0].map(Some)).unwrap();
    assert_eq!(acf.n, 5);
    assert!((acf.rho(0) - 1.0).abs() < 1e-12);
    assert!((acf.rho(1) - 0.4).abs() < 1e-12, "{}", acf.rho(1));
    assert!((acf.rho(2) + 0.1).abs() < 1e-12, "{}", acf.rho(2));
    // 1, 2, 3, 4 and an absent fifth: mean 2.5, centred −1.5, −0.5, 0.5,
    // 1.5, Σx² = 5; lag 1 sums 1.25 and lag 2 −1.5, the absent value
    // contributing nothing.
    let acf = Acf::new(&[Some(1.0), Some(2.0), Some(3.0), Some(4.0), None]).unwrap();
    assert_eq!(acf.n, 4);
    assert!((acf.rho(1) - 0.25).abs() < 1e-12, "{}", acf.rho(1));
    assert!((acf.rho(2) + 0.3).abs() < 1e-12, "{}", acf.rho(2));
    assert_eq!(acf.rho(5), 0.0);
}

#[test]
fn geyer_ess_on_known_autocorrelations() {
    // ρ_k = φ^k up to lag 60 and 0 beyond: the pair (62, 63) is the first
    // zero sum, so the rule stops at lag 61 instead of running to K.
    let geometric = |phi: f64| move |k: usize| if k <= 60 { phi.powi(k as i32) } else { 0.0 };
    let positive = geyer(geometric(0.5), MAX_LAG).unwrap();
    assert!((positive.tau - 3.0).abs() < 1e-9, "{positive:?}");
    assert_eq!(positive.truncation_lag, 61);
    assert!(!positive.capped);
    // φ = −0.5: τ = 1/3, so the ESS is 3n, not capped at n.
    let negative = geyer(geometric(-0.5), MAX_LAG).unwrap();
    assert!((negative.tau - 1.0 / 3.0).abs() < 1e-9, "{negative:?}");
    assert!(!negative.capped);
    assert!(1_000.0 / negative.tau > 2_999.0);
    // ρ = 1, 0.5, −0.6, 0, …: Γ_0 = 1.5, Γ_1 = −0.6 stops it at lag 1.
    let early = geyer(|k| [1.0, 0.5, -0.6].get(k).copied().unwrap_or(0.0), MAX_LAG).unwrap();
    assert_eq!(early.truncation_lag, 1);
    assert!((early.tau - 2.0).abs() < 1e-12, "{early:?}");
    assert!(!early.capped);
    // ρ_k = 0.999^k never turns: capped at K = 100, the last pair (98, 99).
    let slow = geyer(|k| 0.999_f64.powi(k as i32), 100).unwrap();
    assert!(slow.capped);
    assert_eq!(slow.truncation_lag, 99);
    // Γ_0 ≤ 0 has no ESS.
    assert!(geyer(|k| if k == 0 { 1.0 } else { -1.0 }, MAX_LAG).is_none());
    // A constant series has no ACF.
    assert!(ess(&[Some(1.0); 100], MAX_LAG).is_none());
    assert!(ess(&[Some(1.0), None, Some(1.0), Some(1.0), Some(1.0)], MAX_LAG).is_none());
}

#[test]
fn geyer_ess_of_a_two_state_markov_chain() {
    // A 0/1 chain switching with p = 0.1 has ρ_k = (1 − 2p)^k = 0.8^k, so
    // τ = (1 − p) / p = 9 and ESS = n / 9. The relative SE of τ̂ is about
    // √(2(2M+1)/n) ≈ 3 % at the expected truncation, so ±10 % is about
    // 3 SE; the seed makes the test deterministic.
    let n = 200_000;
    let mut rng = SplitMix64(147);
    let mut state = 0_u8;
    let series: Vec<Option<f64>> = (0..n)
        .map(|_| {
            if rng.below(10) == 0 {
                state ^= 1;
            }
            Some(f64::from(state))
        })
        .collect();
    let result = ess(&series, MAX_LAG).unwrap();
    println!("{result:?}");
    assert_eq!(result.n, n);
    assert_eq!(result.max_lag, MAX_LAG);
    assert!(!result.capped, "{result:?}");
    assert!((result.tau - 9.0).abs() < 0.9, "{result:?}");
    let expected = n as f64 / 9.0;
    assert!((result.ess - expected).abs() < 0.1 * expected, "{result:?}");
}

#[test]
fn geyer_ess_clamps_the_lag_to_a_quarter_of_the_series() {
    // K = min(max_lag, ⌊n/4⌋): on a short series the quarter binds.
    let series: Vec<Option<f64>> = (0..100_u32).map(|i| Some(f64::from(i * 37 % 11))).collect();
    let result = ess(&series, MAX_LAG).unwrap();
    assert_eq!(result.max_lag, 25, "{result:?}");
}
