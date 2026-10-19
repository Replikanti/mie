//! Context records and their sampling policy (Conditional Behavior brief,
//! brief §12, ADR-006; ADR-054, proposed).
//!
//! Conditional Behavior measures what tends to happen after a measurable
//! context. This module defines that unit: a [`ContextRecord`], taken from
//! the engine's committed state after an accepted event, and the policy
//! `conditional.sampling@1` ([`SAMPLING_V1`]) that decides when one is
//! taken. Records are research facts, never Market State: nothing here
//! feeds back into the engine, the catalog or the state hash.
//!
//! - **Trigger samples** (ADR-054 D3): one per [`TriggerEvent`] (ADR-053),
//!   carried unchanged with its location context and bias, plus its
//!   [`Episode`] ([`episode`], D4).
//! - **Baseline samples** (D3): one per accepted event that closes at least
//!   one 1m bar, once the first 1m bar with trades has closed: the instants
//!   at which every trigger family is decided. The anchor is the close of
//!   the last closed 1m bar with trades, the anchor of the effort-vs-result,
//!   exhaustion and absorption contexts (ADR-051 D3), so a baseline sample
//!   and a candidate at one event read the same context.
//! - **Location zone events are not sampled** (D3): location without a
//!   trigger is the baseline split by its context.
//!
//! Every record carries the event's [`CanonicalKey`] (its time is the
//! decision time, the leakage boundary for outcome labels), the location
//! context and bias, and a [`StateSnapshot`] ([`snapshot`], D2). Run
//! provenance comes once per run as a [`ContextProvenance`] (D5).
//!
//! Within one event the baseline sample comes first, then the trigger
//! samples in emission order (ADR-053 D2).
//!
//! [`ContextSampler`] is pure (D6): fixed per-id episode slots, no clock,
//! no hash-ordered collection, one code path for live and replay
//! (ADR-019). It reads only [`MarketStateEngine::state`],
//! [`MarketStateEngine::closed_bars`] and
//! [`MarketStateEngine::trigger_events`], and must be called after accepted
//! events only: after a rejected event those still show the previous
//! event. Records stream to a sink; nothing is collected.
//!
//! No type, field or `Display` text says bullish, bearish, long, short,
//! signal, reversal, continuation or entry, and nothing carries an
//! execution meaning (ADR-010, ADR-054 D1).

pub mod episode;
pub mod snapshot;

pub use episode::{
    Assignment, Episode, EpisodeId, EpisodeTracker, EvidenceSpan, SpanRule, TRIGGER_IDS,
    evidence_span, trigger_position,
};
pub use snapshot::{SNAPSHOT_FEATURES, StateSnapshot};

use crate::bars::Timeframe;
use crate::candidate::LocationContext;
use crate::event::MarketEvent;
use crate::feature::FeatureKey;
use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::num::Price;
use crate::order::CanonicalKey;
use crate::research::{DataVersion, EngineKey, PipelineKey};
use crate::state::MarketStateEngine;
use crate::state_hash::STATE_HASH_ENCODING;
use crate::time::EventTime;
use crate::trigger::{Bias, TriggerEvent};
use std::fmt;

/// The sampling policy (ADR-054 D5): the baseline grid, the episode rule,
/// the span table and the snapshot list. Any change to them is a new
/// version.
pub const SAMPLING_V1: PipelineKey = PipelineKey::new("conditional.sampling", 1);

/// The canonical parameter text of [`SAMPLING_V1`], which
/// [`policy_fingerprint`] hashes:
///
/// ```text
/// mie-sampling-policy 1
/// policy conditional.sampling@1
/// baseline every_event_closing_a_1m_bar anchor=last_1m_close_with_trades
/// episode scope=trigger_id join=span_overlaps_episode_data bridge=span_overlaps_closed_data
/// span <trigger id> <span rule>        (one line per TRIGGER_IDS entry)
/// snapshot <feature>,<feature>,…       (SNAPSHOT_FEATURES)
/// ```
///
/// Every line ends in `\n`.
pub fn policy_text() -> String {
    let mut text = String::from("mie-sampling-policy 1\n");
    text.push_str(&format!("policy {SAMPLING_V1}\n"));
    text.push_str("baseline every_event_closing_a_1m_bar anchor=last_1m_close_with_trades\n");
    text.push_str(
        "episode scope=trigger_id join=span_overlaps_episode_data bridge=span_overlaps_closed_data\n",
    );
    for (key, rule) in TRIGGER_IDS {
        text.push_str(&format!("span {key} {rule}\n"));
    }
    let snapshot: Vec<String> = SNAPSHOT_FEATURES.iter().map(ToString::to_string).collect();
    text.push_str(&format!("snapshot {}\n", snapshot.join(",")));
    text
}

/// FNV-1a 64 ([`Fingerprinter::write_str`]) of [`policy_text`].
pub fn policy_fingerprint() -> Fingerprint {
    let mut hasher = Fingerprinter::new();
    hasher.write_str(&policy_text());
    hasher.finish()
}

/// Where a run of records comes from (ADR-054 D5), delivered once per run.
///
/// `Display` prints `data=<64 hex> engine=<engine key> policy=<id@N>
/// policy_fingerprint=<16 hex>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextProvenance {
    /// The version of the raw data replayed (ADR-030).
    pub data: DataVersion,
    /// The engine: its feature-set version and the state-hash encoding.
    pub engine: EngineKey,
    /// The sampling policy: [`SAMPLING_V1`].
    pub policy: PipelineKey,
    /// [`policy_fingerprint`].
    pub policy_fingerprint: Fingerprint,
}

impl ContextProvenance {
    /// The provenance of records sampled from `engine` over the raw data
    /// `data` under [`SAMPLING_V1`].
    pub fn new(data: DataVersion, engine: &MarketStateEngine) -> Self {
        Self {
            data,
            engine: EngineKey {
                features: engine.state().feature_set,
                state_encoding: STATE_HASH_ENCODING,
            },
            policy: SAMPLING_V1,
            policy_fingerprint: policy_fingerprint(),
        }
    }
}

impl fmt::Display for ContextProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "data={} engine={} policy={} policy_fingerprint={}",
            self.data, self.engine, self.policy, self.policy_fingerprint
        )
    }
}

/// What a record samples (ADR-054 D1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sample {
    /// A trigger event, as emitted (ADR-053), and its episode (D4). Boxed:
    /// several times the size of a baseline sample.
    Trigger {
        /// The trigger event.
        trigger: Box<TriggerEvent>,
        /// Its episode.
        episode: Episode,
    },
    /// An unconditional baseline sample (D3).
    Baseline {
        /// End of the 1m bar whose close set the anchor: the grid point,
        /// so consumers can subsample.
        minute_end: EventTime,
    },
}

/// One context record (ADR-054 D1).
///
/// `Display` prints the event key, then `baseline minute_end=<time>` with
/// the context and the bias, or `trigger <episode>` with the trigger event
/// (whose line carries its context and bias), then the snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextRecord {
    /// The emitting event. Its time is the decision time: the record reads
    /// nothing after this event.
    pub event: CanonicalKey,
    /// What is sampled.
    pub sample: Sample,
    /// Where: the trigger event's own context, or for a baseline sample the
    /// context of its anchor without exclusions.
    pub context: LocationContext,
    /// The bias (ADR-053 D3): the trigger event's own, or
    /// [`Bias::read`] for a baseline sample.
    pub bias: Bias,
    /// The declared Market State subset (D2).
    pub state: StateSnapshot,
}

impl fmt::Display for ContextRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ", self.event)?;
        match &self.sample {
            Sample::Baseline { minute_end } => write!(
                f,
                "baseline minute_end={minute_end} {} {}",
                self.context, self.bias
            )?,
            Sample::Trigger { trigger, episode } => write!(f, "trigger {episode} {trigger}")?,
        }
        write!(f, " {}", self.state)
    }
}

/// Counts over the emitted trigger samples of one trigger id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TriggerStats {
    /// Trigger samples emitted.
    pub samples: u64,
    /// Distinct episodes with at least one emitted sample.
    pub episodes: u64,
    /// Emitted samples that are bridges (ADR-054 D4).
    pub bridges: u64,
    /// Distinct emitted episodes that are truncated.
    pub truncated: u64,
}

/// Counts over the emitted records: totals, and per trigger id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SamplingStats {
    /// Records emitted, trigger and baseline.
    pub records: u64,
    /// Distinct episodes with at least one emitted sample.
    pub episodes: u64,
    /// Emitted trigger samples that are bridges (ADR-054 D4).
    pub bridges: u64,
    /// Distinct emitted episodes that are truncated.
    pub truncated: u64,
    /// The counts of each trigger id, in [`TRIGGER_IDS`] order.
    pub per_trigger: [TriggerStats; TRIGGER_IDS.len()],
}

impl SamplingStats {
    /// The counts of the trigger id `trigger`, if it is one.
    pub fn trigger(&self, trigger: FeatureKey) -> Option<&TriggerStats> {
        trigger_position(trigger).map(|position| &self.per_trigger[position])
    }
}

/// The last closed 1m bar with trades.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct LastMinute {
    /// End of the bar.
    end: EventTime,
    /// Its close.
    close: Price,
}

/// Turns the engine's output after each accepted event into context
/// records (ADR-054 D3, D4, D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextSampler {
    /// Records of events before this time are tracked, not emitted.
    emit_from: Option<EventTime>,
    /// The episode state.
    episodes: EpisodeTracker,
    /// The time of the first observed event.
    observed_from: Option<EventTime>,
    /// The anchor of the baseline samples.
    last_minute: Option<LastMinute>,
    /// Counts over the emitted records.
    stats: SamplingStats,
}

impl ContextSampler {
    /// A sampler that emits the records of events at or after `emit_from`
    /// (all with `None`). Earlier events still drive the episodes and the
    /// anchor, so a caller can warm a prefix up and emit from a window
    /// start.
    pub fn new(emit_from: Option<EventTime>) -> Self {
        Self {
            emit_from,
            episodes: EpisodeTracker::new(),
            observed_from: None,
            last_minute: None,
            stats: SamplingStats::default(),
        }
    }

    /// Counts over the records emitted so far.
    pub fn stats(&self) -> SamplingStats {
        self.stats
    }

    /// Observes `event`, which `engine` has just accepted, and passes its
    /// records to `emit`: the baseline sample if it closed a 1m bar, then
    /// one trigger sample per trigger event in emission order. Call it
    /// after every accepted event and never after a rejected one (module
    /// docs).
    pub fn observe(
        &mut self,
        event: &MarketEvent,
        engine: &MarketStateEngine,
        mut emit: impl FnMut(ContextRecord),
    ) {
        let key = event.canonical_key();
        let observed_from = *self.observed_from.get_or_insert(key.time);
        let emitting = self.emit_from.is_none_or(|from| key.time >= from);
        let state = engine.state();
        // The anchor follows the same rule as the candidate stage's
        // (ADR-051 D3): the last closed 1m bar with trades.
        let mut minute = false;
        for bar in engine
            .closed_bars()
            .iter()
            .filter(|bar| bar.timeframe == Timeframe::M1)
        {
            minute = true;
            if let Some(ohlc) = bar.ohlc {
                self.last_minute = Some(LastMinute {
                    end: bar.end(),
                    close: ohlc.close,
                });
            }
        }
        let baseline = self.last_minute.filter(|_| minute);
        let triggers = engine.trigger_events();
        if baseline.is_none() && triggers.is_empty() {
            return;
        }
        let snapshot = StateSnapshot::read(state);
        if let Some(last) = baseline
            && emitting
        {
            self.stats.records += 1;
            emit(ContextRecord {
                event: key,
                sample: Sample::Baseline {
                    minute_end: last.end,
                },
                context: LocationContext::read(last.close, None, &state.location),
                bias: Bias::read(state),
                state: snapshot,
            });
        }
        for trigger in triggers {
            let assignment = self.episodes.assign(trigger, key, observed_from);
            if !emitting {
                continue;
            }
            let episode = assignment.episode;
            let first = self.episodes.first_emission(trigger.feature);
            let stats = &mut self.stats;
            stats.records += 1;
            let per = trigger_position(trigger.feature)
                .map(|position| &mut stats.per_trigger[position])
                .expect("every trigger id has a stats slot");
            per.samples += 1;
            if first {
                stats.episodes += 1;
                per.episodes += 1;
                stats.truncated += u64::from(episode.truncated);
                per.truncated += u64::from(episode.truncated);
            }
            stats.bridges += u64::from(episode.bridge);
            per.bridges += u64::from(episode.bridge);
            emit(ContextRecord {
                event: key,
                sample: Sample::Trigger {
                    trigger: Box::new(trigger.clone()),
                    episode: assignment.episode,
                },
                context: trigger.context.clone(),
                bias: trigger.bias,
                state: snapshot,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::CandidateEvent;
    use crate::event::samples::t;
    use crate::event::{Aggressor, FeedGap, GapReason, OpenInterest, Stream, Trade};
    use crate::feature::{FeatureKey, catalog};
    use crate::num::{Qty, SCALE};
    use crate::structure::STRUCTURE_TIMEFRAMES;
    use crate::trigger::{OiRejection, TriggerEvidence, TriggerFamily};

    const MINUTE: i64 = 60_000;
    const DAY: i64 = 86_400_000;

    /// Whole USDT as a price.
    fn usdt(whole: i64) -> Price {
        Price::from_units(whole * SCALE)
    }

    /// A trade at `millis`.
    fn trade(millis: i64, trade_id: u64, price: Price) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price,
            qty: Qty::from_units(10_000_000),
            aggressor: Aggressor::Buy,
        })
    }

    /// The canonical key of a trade at `millis` with id `seq`.
    fn key(millis: i64, seq: u64) -> CanonicalKey {
        trade(millis, seq, usdt(60_000)).canonical_key()
    }

    /// Replays `events` through a fresh engine and one sampler, returning
    /// every record with the index of the event that emitted it.
    fn sample(events: &[MarketEvent]) -> (Vec<(usize, ContextRecord)>, SamplingStats) {
        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut records = Vec::new();
        for (index, event) in events.iter().enumerate() {
            engine.apply(event).unwrap();
            sampler.observe(event, &engine, |record| records.push((index, record)));
        }
        (records, sampler.stats())
    }

    fn is_baseline(record: &ContextRecord) -> bool {
        matches!(record.sample, Sample::Baseline { .. })
    }

    fn trigger_of(record: &ContextRecord) -> Option<(&TriggerEvent, &Episode)> {
        match &record.sample {
            Sample::Trigger { trigger, episode } => Some((trigger, episode)),
            Sample::Baseline { .. } => None,
        }
    }

    fn candidate_of(trigger: &TriggerEvent) -> Option<&CandidateEvent> {
        match &trigger.evidence {
            TriggerEvidence::Candidate(fact) => Some(fact),
            _ => None,
        }
    }

    /// The words no record may contain (ADR-054 D1, the ADR-053 rule).
    const FORBIDDEN: [&str; 8] = [
        "bull",
        "bear",
        "long",
        "short",
        "signal",
        "reversal",
        "continuation",
        "entry",
    ];

    fn forbidden_in(line: &str) -> Option<&'static str> {
        let lower = line.to_lowercase();
        FORBIDDEN.into_iter().find(|word| lower.contains(word))
    }

    /// One replay of the candidate golden tape (ADR-051, also the trigger
    /// golden tape) with three samplers: one over every event, one that
    /// emits from day 10 on, and one that observes only from day 10 on.
    /// Holds what the tests read; the baseline records themselves are
    /// checked as they stream, not kept (a test-only cache: the tape takes
    /// seconds to replay).
    struct GoldenRun {
        /// The index and time of the first event at or after day 10.
        split: (usize, EventTime),
        /// Every trigger event of the engine, with its event index and key.
        fired: Vec<(usize, CanonicalKey, TriggerEvent)>,
        /// Every trigger record of the full sampler, with its event index
        /// and the context of its anchor re-read without exclusions.
        triggers: Vec<(usize, ContextRecord, LocationContext)>,
        /// The baseline records at events that emitted a trigger.
        baselines_at_triggers: Vec<(usize, ContextRecord)>,
        /// The indexes of the events with a baseline record.
        baseline_events: Vec<usize>,
        /// The full sampler's counts.
        stats: SamplingStats,
        /// Records of the day-10 emitting sampler, and whether they equal
        /// the full sampler's records of the same events.
        windowed: (usize, bool),
        /// The trigger records of the day-10 observing sampler.
        late: Vec<(usize, ContextRecord)>,
        /// Its counts.
        late_stats: SamplingStats,
        /// The count and digest of every full record's line, and the first
        /// trigger record's line.
        golden: (usize, String, String),
        /// Record lines with a forbidden word.
        forbidden: Vec<String>,
    }

    fn golden_run() -> &'static GoldenRun {
        static RUN: std::sync::OnceLock<GoldenRun> = std::sync::OnceLock::new();
        RUN.get_or_init(|| {
            let tape = crate::candidate::tests::golden_tape();
            let split_index = tape
                .iter()
                .position(|event| event.time().as_millis() >= 10 * DAY)
                .unwrap();
            let split_time = tape[split_index].time();
            let mut engine = MarketStateEngine::new();
            let mut full = ContextSampler::new(None);
            let mut windowed = ContextSampler::new(Some(split_time));
            let mut late = ContextSampler::new(None);
            let mut hasher = Fingerprinter::new();
            let mut run = GoldenRun {
                split: (split_index, split_time),
                fired: Vec::new(),
                triggers: Vec::new(),
                baselines_at_triggers: Vec::new(),
                baseline_events: Vec::new(),
                stats: SamplingStats::default(),
                windowed: (0, true),
                late: Vec::new(),
                late_stats: SamplingStats::default(),
                golden: (0, String::new(), String::new()),
                forbidden: Vec::new(),
            };
            for (index, event) in tape.iter().enumerate() {
                engine.apply(event).unwrap();
                let key = event.canonical_key();
                run.fired.extend(
                    engine
                        .trigger_events()
                        .iter()
                        .map(|fact| (index, key, fact.clone())),
                );
                let mut records = Vec::new();
                full.observe(event, &engine, |record| records.push(record));
                let mut emitted = Vec::new();
                windowed.observe(event, &engine, |record| emitted.push(record));
                run.windowed.0 += emitted.len();
                if event.time() >= split_time {
                    run.windowed.1 &= emitted == records;
                } else {
                    run.windowed.1 &= emitted.is_empty();
                }
                if index >= split_index {
                    late.observe(event, &engine, |record| {
                        if !is_baseline(&record) {
                            run.late.push((index, record));
                        }
                    });
                }
                let fired = !engine.trigger_events().is_empty();
                for record in records {
                    let line = record.to_string();
                    hasher.write_str(&line);
                    run.golden.0 += 1;
                    if forbidden_in(&line).is_some() {
                        run.forbidden.push(line.clone());
                    }
                    match trigger_of(&record) {
                        Some((trigger, _)) => {
                            if run.golden.2.is_empty() {
                                run.golden.2 = line;
                            }
                            let reread = LocationContext::read(
                                trigger.anchor(),
                                None,
                                &engine.state().location,
                            );
                            run.triggers.push((index, record, reread));
                        }
                        None => {
                            run.baseline_events.push(index);
                            if fired {
                                run.baselines_at_triggers.push((index, record));
                            }
                        }
                    }
                }
            }
            run.stats = full.stats();
            run.late_stats = late.stats();
            run.golden.1 = hasher.finish().to_string();
            run
        })
    }

    /// The first trigger event of the golden tape that `pick` accepts.
    fn template(pick: impl Fn(&TriggerEvent) -> bool) -> TriggerEvent {
        golden_run()
            .fired
            .iter()
            .map(|(_, _, fact)| fact)
            .find(|fact| pick(fact))
            .expect("the golden tape has the template")
            .clone()
    }

    /// An effort-vs-result trigger of window `window` whose window ends at
    /// `end` ms.
    fn effort_at(window: Timeframe, end: i64) -> TriggerEvent {
        let mut trigger = template(|fact| {
            matches!(candidate_of(fact), Some(CandidateEvent::EffortVsResult(edge))
                if edge.value.timeframe == window)
        });
        let TriggerEvidence::Candidate(fact) = &mut trigger.evidence else {
            unreachable!()
        };
        let CandidateEvent::EffortVsResult(edge) = fact.as_mut() else {
            unreachable!()
        };
        edge.value.end = t(end);
        trigger
    }

    /// A 15m CVD divergence trigger on the swing anchors at `previous` and
    /// `current` ms, the later swing confirmed by an event 1 ms after its
    /// anchor, so its span is `[previous, current]`.
    fn divergence(previous: i64, current: i64) -> TriggerEvent {
        let mut trigger =
            template(|fact| fact.feature == catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.key);
        let TriggerEvidence::Candidate(fact) = &mut trigger.evidence else {
            unreachable!()
        };
        let CandidateEvent::CvdDivergence(event) = fact.as_mut() else {
            unreachable!()
        };
        event.comparison.pair.previous.time = t(previous);
        event.comparison.pair.current.time = t(current);
        event.comparison.pair.current.known_at = t(current + 1);
        trigger
    }

    /// A 15m SFP template with its sweep at `sweep` ms, resolved by the bar
    /// ending at `resolved` ms.
    fn sfp(sweep: i64, resolved: i64) -> TriggerEvent {
        let mut trigger = template(|fact| fact.feature == catalog::TRIGGER_SFP_15M_V1.key);
        let TriggerEvidence::Sfp(swept) = &mut trigger.evidence else {
            unreachable!()
        };
        swept.time = t(sweep);
        swept.resolved_bar_end = Some(t(resolved));
        trigger
    }

    /// A 15m OI-expansion trigger on the SFP `sfp(sweep, resolved)` with
    /// its points on the boundaries `start` and `end` ms.
    fn oi_expansion(sweep: i64, resolved: i64, start: i64, end: i64) -> TriggerEvent {
        let mut trigger =
            template(|fact| fact.feature == catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1.key);
        let TriggerEvidence::OiExpansion(expansion) = &mut trigger.evidence else {
            unreachable!()
        };
        let TriggerEvidence::Sfp(swept) = sfp(sweep, resolved).evidence else {
            unreachable!()
        };
        expansion.rejection = OiRejection::Sfp(swept);
        expansion.start.boundary = t(start);
        expansion.end.boundary = t(end);
        trigger
    }

    /// Assigns `triggers`, the n-th at `key(times[n], n)`, and returns the
    /// assignments.
    fn assign_all(triggers: &[(i64, TriggerEvent)]) -> Vec<Assignment> {
        let mut tracker = EpisodeTracker::new();
        triggers
            .iter()
            .zip(1..)
            .map(|((time, trigger), seq)| tracker.assign(trigger, key(*time, seq), t(0)))
            .collect()
    }

    /// The number of distinct episodes among `assignments`.
    fn episodes(assignments: &[Assignment]) -> usize {
        let mut ids: Vec<EpisodeId> = assignments.iter().map(|a| a.episode.id).collect();
        ids.sort();
        ids.dedup();
        ids.len()
    }

    #[test]
    fn baseline_samples_once_per_event_that_closes_a_1m_bar() {
        let events = [
            trade(1_000, 1, usdt(60_000)),
            trade(20_000, 2, usdt(60_010)),
            // Closes minute 0: the first baseline, anchored at its close.
            trade(MINUTE + 1_000, 3, usdt(60_020)),
            trade(MINUTE + 30_000, 4, usdt(60_030)),
            // Not on the trades stream: closes nothing.
            MarketEvent::OpenInterest(OpenInterest {
                time: t(MINUTE + 40_000),
                open_interest: Qty::from_units(80_000 * SCALE),
                resolution_ms: 300_000,
            }),
            // A 30-minute gap: one event closes minutes 1 … 30, one sample
            // anchored at minute 1, the last with trades.
            trade(31 * MINUTE + 1_000, 5, usdt(60_040)),
            trade(31 * MINUTE + 2_000, 6, usdt(60_050)),
        ];
        let (records, stats) = sample(&events);
        let baselines: Vec<(usize, EventTime, Price)> = records
            .iter()
            .filter_map(|(index, record)| match record.sample {
                Sample::Baseline { minute_end } => {
                    Some((*index, minute_end, record.context.anchor))
                }
                Sample::Trigger { .. } => None,
            })
            .collect();
        assert_eq!(
            baselines,
            [
                (2, t(MINUTE), usdt(60_010)),
                (5, t(2 * MINUTE), usdt(60_030))
            ]
        );
        assert_eq!(stats.records, 2);
        for (index, record) in &records {
            assert_eq!(record.event, events[*index].canonical_key());
        }
    }

    #[test]
    fn an_event_that_closes_only_empty_minutes_keeps_the_anchor() {
        // ADR-054 D3: a trades gap closes the minute with trades; the trade
        // after it closes only empty minutes and still samples, at the
        // anchor and `minute_end` of the last minute with trades.
        let events = [
            trade(1_000, 1, usdt(60_000)),
            trade(20_000, 2, usdt(60_010)),
            MarketEvent::FeedGap(FeedGap {
                stream: Stream::Trades,
                start: t(30_000),
                end: t(5 * MINUTE + 10_000),
                reason: GapReason::Disconnected,
            }),
            trade(10 * MINUTE + 1_000, 3, usdt(60_500)),
        ];
        let mut engine = MarketStateEngine::new();
        let mut sampler = ContextSampler::new(None);
        let mut baselines = Vec::new();
        for (index, event) in events.iter().enumerate() {
            engine.apply(event).unwrap();
            let minutes: Vec<bool> = engine
                .closed_bars()
                .iter()
                .filter(|bar| bar.timeframe == Timeframe::M1)
                .map(|bar| bar.ohlc.is_some())
                .collect();
            sampler.observe(event, &engine, |record| {
                if let Sample::Baseline { minute_end } = record.sample {
                    baselines.push((index, minute_end, record.context.anchor, minutes.clone()));
                }
            });
        }
        assert_eq!(baselines.len(), 2, "{baselines:?}");
        let (gap, gap_end, gap_anchor, gap_minutes) = &baselines[0];
        assert_eq!((*gap, *gap_end, *gap_anchor), (2, t(MINUTE), usdt(60_010)));
        assert!(gap_minutes[0], "{gap_minutes:?}");
        let (after, after_end, after_anchor, after_minutes) = &baselines[1];
        assert_eq!(*after, 3);
        assert!(!after_minutes.is_empty() && after_minutes.iter().all(|with| !with));
        // The anchor and `minute_end` repeat.
        assert_eq!((*after_end, *after_anchor), (t(MINUTE), usdt(60_010)));
    }

    #[test]
    fn divergence_spans_run_through_the_confirming_bars() {
        // ADR-054 D4: the span of a divergence ends where the later swing's
        // confirming bar ends, not at its anchor trade.
        let mut checked = 0;
        for (_, _, fact) in &golden_run().fired {
            let Some(CandidateEvent::CvdDivergence(event)) = candidate_of(fact) else {
                continue;
            };
            let pair = &event.comparison.pair;
            let span = evidence_span(fact);
            assert_eq!(span.first, pair.previous.time);
            assert_eq!(span.last, t(pair.current.known_at.as_millis() - 1));
            assert_eq!(pair.current.known_at, fact.known_at);
            assert!(span.last > pair.current.time, "{fact}");
            checked += 1;
        }
        assert!(checked > 100, "{checked}");
    }

    #[test]
    fn stats_count_per_trigger_id() {
        let run = golden_run();
        let stats = run.stats;
        let per = stats.per_trigger;
        assert_eq!(
            per.iter().map(|id| id.samples).sum::<u64>(),
            run.triggers.len() as u64
        );
        assert_eq!(
            per.iter().map(|id| id.episodes).sum::<u64>(),
            stats.episodes
        );
        assert_eq!(per.iter().map(|id| id.bridges).sum::<u64>(), stats.bridges);
        assert_eq!(
            per.iter().map(|id| id.truncated).sum::<u64>(),
            stats.truncated
        );
        for (key, _) in TRIGGER_IDS {
            let records: Vec<&Episode> = run
                .triggers
                .iter()
                .filter_map(|(_, record, _)| trigger_of(record))
                .filter(|(trigger, _)| trigger.feature == key)
                .map(|(_, episode)| episode)
                .collect();
            let counted = stats.trigger(key).unwrap();
            assert_eq!(counted.samples, records.len() as u64, "{key}");
            assert_eq!(
                counted.episodes,
                records.iter().filter(|episode| episode.opener).count() as u64,
                "{key}"
            );
            assert_eq!(
                counted.bridges,
                records.iter().filter(|episode| episode.bridge).count() as u64,
                "{key}"
            );
        }
        assert_eq!(stats.trigger(catalog::BIAS_CONTEXT_V1.key), None);
    }

    #[test]
    fn baseline_context_matches_the_candidate_contexts() {
        // ADR-054 D1: the baseline anchor is the anchor of the
        // effort-vs-result, exhaustion and absorption contexts (ADR-051 D3).
        let run = golden_run();
        let mut checked = 0;
        for (index, record, _) in &run.triggers {
            let (trigger, _) = trigger_of(record).unwrap();
            let Some(fact @ (CandidateEvent::EffortVsResult(_) | CandidateEvent::Exhaustion(_))) =
                candidate_of(trigger)
            else {
                continue;
            };
            let (_, baseline) = run
                .baselines_at_triggers
                .iter()
                .find(|(at, _)| at == index)
                .expect("a candidate event closes a 1m bar");
            assert_eq!(&baseline.context, fact.context(), "{record}");
            assert_eq!(baseline.context.anchor, trigger.anchor());
            checked += 1;
        }
        assert!(checked > 100, "{checked}");
    }

    #[test]
    fn every_trigger_instant_is_a_baseline_instant() {
        let run = golden_run();
        assert!(run.fired.len() > 1_000, "{}", run.fired.len());
        for (index, _, fact) in &run.fired {
            assert!(run.baseline_events.binary_search(index).is_ok(), "{fact}");
        }
    }

    #[test]
    fn trigger_records_carry_the_trigger_event_unchanged() {
        let run = golden_run();
        assert_eq!(run.triggers.len(), run.fired.len());
        let mut excluded = [0, 0];
        for ((_, record, reread), (_, key, fact)) in run.triggers.iter().zip(&run.fired) {
            let (trigger, _) = trigger_of(record).unwrap();
            assert_eq!(trigger, fact);
            assert_eq!(record.event, *key);
            assert_eq!(record.context, fact.context);
            assert_eq!(record.bias, fact.bias);
            // ADR-053 D4: the SFP and CVD contexts leave the swing's own
            // levels out; a re-read would put them back.
            if record.context != *reread {
                match fact.family {
                    TriggerFamily::Sfp => excluded[0] += 1,
                    TriggerFamily::CvdDivergence => excluded[1] += 1,
                    _ => {}
                }
            }
        }
        assert!(excluded.iter().all(|count| *count > 0), "{excluded:?}");
    }

    #[test]
    fn baseline_bias_equals_the_trigger_bias_at_one_event() {
        let run = golden_run();
        let mut checked = 0;
        for (index, baseline) in &run.baselines_at_triggers {
            for (_, record, _) in run.triggers.iter().filter(|(at, ..)| at == index) {
                assert_eq!(baseline.bias, record.bias, "{record}");
                assert_eq!(baseline.event, record.event);
                checked += 1;
            }
        }
        assert_eq!(checked, run.triggers.len());
    }

    #[test]
    fn effort_edges_share_an_episode_only_when_their_windows_overlap() {
        let end = DAY;
        // 4 minutes apart, the windows share a minute: one episode.
        let joined = assign_all(&[
            (end, effort_at(Timeframe::M5, end)),
            (end + 4 * MINUTE, effort_at(Timeframe::M5, end + 4 * MINUTE)),
        ]);
        assert_eq!(episodes(&joined), 1);
        assert!(joined[0].episode.opener && !joined[1].episode.opener);
        assert_eq!(
            joined[1].span,
            EvidenceSpan {
                first: t(end - MINUTE),
                last: t(end + 4 * MINUTE - 1),
            }
        );
        // 5 minutes apart, the half-open windows touch: two episodes.
        let apart = assign_all(&[
            (end, effort_at(Timeframe::M5, end)),
            (end + 5 * MINUTE, effort_at(Timeframe::M5, end + 5 * MINUTE)),
        ]);
        assert_eq!(episodes(&apart), 2);
        assert!(apart.iter().all(|a| a.episode.opener && !a.episode.bridge));
    }

    #[test]
    fn divergences_sharing_a_swing_chain() {
        let hour = 3_600_000;
        let chained = assign_all(&[
            (3 * hour, divergence(hour, 2 * hour)),
            (5 * hour, divergence(2 * hour, 4 * hour)),
            (7 * hour, divergence(5 * hour, 6 * hour)),
        ]);
        assert_eq!(chained[0].episode.id, chained[1].episode.id);
        assert_ne!(chained[1].episode.id, chained[2].episode.id);
        assert_eq!(
            chained[1].span,
            EvidenceSpan {
                first: t(2 * hour),
                last: t(4 * hour),
            }
        );
    }

    #[test]
    fn overlapping_sfp_excursions_chain_and_disjoint_ones_do_not() {
        let quarter = 15 * MINUTE;
        let assignments = assign_all(&[
            (2 * quarter, sfp(quarter + 1_000, 2 * quarter)),
            // Swept before the first resolved: overlapping excursions.
            (3 * quarter, sfp(2 * quarter - 1_000, 3 * quarter)),
            // Swept at the first millisecond after: disjoint.
            (5 * quarter, sfp(3 * quarter, 5 * quarter)),
        ]);
        assert_eq!(
            assignments[0].span,
            EvidenceSpan {
                first: t(quarter + 1_000),
                last: t(2 * quarter - 1),
            }
        );
        assert_eq!(assignments[0].episode.id, assignments[1].episode.id);
        assert_ne!(assignments[1].episode.id, assignments[2].episode.id);
    }

    /// Every trigger event of the location golden tape (ADR-044), on which
    /// every auction state occurs.
    fn auction_records() -> Vec<(usize, ContextRecord)> {
        let (records, _) = sample(&crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7));
        records
            .into_iter()
            .filter(|(_, record)| {
                trigger_of(record).is_some_and(|(trigger, _)| {
                    matches!(trigger.evidence, TriggerEvidence::Auction(_))
                })
            })
            .collect()
    }

    #[test]
    fn auction_triggers_are_one_episode_each() {
        // A failure or an acceptance ends its probe (ADR-044 D8), so no two
        // triggers of one auction id share a deciding bar.
        let records = auction_records();
        let families: Vec<TriggerFamily> = records
            .iter()
            .map(|(_, record)| trigger_of(record).unwrap().0.family)
            .collect();
        for family in [
            TriggerFamily::FailedAuction,
            TriggerFamily::FailedReclaim,
            TriggerFamily::BreakoutAcceptance,
        ] {
            assert!(families.contains(&family), "{family}");
        }
        for (_, record) in &records {
            let (trigger, episode) = trigger_of(record).unwrap();
            assert!(episode.opener, "{record}");
            assert_eq!(episode.id.ordinal, 0, "{record}");
            let TriggerEvidence::Auction(status) = &trigger.evidence else {
                unreachable!()
            };
            assert_eq!(
                evidence_span(trigger),
                EvidenceSpan {
                    first: t(status.bar_end.as_millis() - MINUTE),
                    last: t(status.bar_end.as_millis() - 1),
                }
            );
        }
        // Two consecutive deciding bars of one id: two episodes.
        let (_, record) = &records[0];
        let (trigger, _) = trigger_of(record).unwrap();
        let mut next = trigger.clone();
        let TriggerEvidence::Auction(status) = &mut next.evidence else {
            unreachable!()
        };
        let end = status.bar_end.as_millis();
        status.bar_end = t(end + MINUTE);
        let assignments = assign_all(&[(end, trigger.clone()), (end + MINUTE, next)]);
        assert_eq!(episodes(&assignments), 2);
    }

    #[test]
    fn oi_episodes_use_the_union_of_rejection_and_grid_points() {
        let quarter = 15 * MINUTE;
        let five = 5 * MINUTE;
        let first = oi_expansion(
            quarter + 1_000,
            2 * quarter,
            quarter - five,
            2 * quarter - five,
        );
        assert_eq!(
            evidence_span(&first),
            EvidenceSpan {
                first: t(quarter - five),
                last: t(2 * quarter - 1),
            }
        );
        // The second excursion starts after the first resolved, but its
        // start point lies inside the first's span: the union chains them.
        let chained = assign_all(&[
            (2 * quarter, first.clone()),
            (
                4 * quarter,
                oi_expansion(
                    3 * quarter,
                    4 * quarter,
                    2 * quarter - five,
                    4 * quarter - five,
                ),
            ),
        ]);
        assert_eq!(episodes(&chained), 1);
        // A start point after it: two episodes.
        let apart = assign_all(&[
            (2 * quarter, first),
            (
                4 * quarter,
                oi_expansion(3 * quarter, 4 * quarter, 2 * quarter, 4 * quarter - five),
            ),
        ]);
        assert_eq!(episodes(&apart), 2);
    }

    #[test]
    fn episodes_are_per_id() {
        let end = DAY;
        let mut tracker = EpisodeTracker::new();
        let at = key(end, 1);
        let short = tracker.assign(&effort_at(Timeframe::M5, end), at, t(0));
        let long_window = tracker.assign(&effort_at(Timeframe::M15, end), at, t(0));
        assert_ne!(short.episode.id, long_window.episode.id);
        assert!(short.episode.opener && long_window.episode.opener);
        assert_eq!(short.episode.id.opened_at, long_window.episode.id.opened_at);
        assert_eq!(
            (short.episode.id.ordinal, long_window.episode.id.ordinal),
            (0, 0)
        );
    }

    #[test]
    fn one_event_can_open_two_episodes_of_one_id() {
        // Two disjoint spans of one id at one event, in both emission
        // orders: the same two episodes, told apart by the ordinal. (Real
        // triggers of one id at one event share their `last` instant and
        // join one episode; the ordinal keeps the id unique regardless.)
        let hour = 3_600_000;
        let at = key(10 * hour, 1);
        let pairs = [divergence(hour, 2 * hour), divergence(3 * hour, 4 * hour)];
        for order in [[0, 1], [1, 0]] {
            let mut tracker = EpisodeTracker::new();
            let [first, second] = order.map(|index| tracker.assign(&pairs[index], at, t(0)));
            assert_ne!(first.episode.id, second.episode.id, "{order:?}");
            assert_eq!(
                (first.episode.id.ordinal, second.episode.id.ordinal),
                (0, 1)
            );
            assert!(first.episode.opener && second.episode.opener, "{order:?}");
            assert!(!first.episode.bridge && !second.episode.bridge, "{order:?}");
            // The next event starts counting again.
            let later = tracker.assign(&divergence(5 * hour, 6 * hour), key(11 * hour, 2), t(0));
            assert_eq!(later.episode.id.ordinal, 0);
        }
    }

    #[test]
    fn a_span_wholly_before_the_open_episode_opens_its_own() {
        // The join rule is a real overlap: a span that ends before the open
        // episode's data starts shares nothing with it.
        let hour = 3_600_000;
        let assignments = assign_all(&[
            (3 * hour, divergence(hour, 2 * hour)),
            (6 * hour, divergence(4 * hour, 5 * hour)),
            (7 * hour, divergence(30 * MINUTE, 50 * MINUTE)),
        ]);
        assert_eq!(episodes(&assignments), 3);
        assert!(assignments[2].episode.opener);
        // It overlaps no closed episode either.
        assert!(!assignments[2].episode.bridge);
    }

    #[test]
    fn a_trigger_reaching_into_the_closed_episode_is_a_bridge() {
        let hour = 3_600_000;
        let assignments = assign_all(&[
            (3 * hour, divergence(hour, 2 * hour)),
            (6 * hour, divergence(4 * hour, 5 * hour)),
            // The other side's pair reaches back into the first episode.
            (7 * hour, divergence(90 * MINUTE, 6 * hour)),
        ]);
        assert_eq!(episodes(&assignments), 2);
        assert!(!assignments[2].episode.opener);
        assert!(assignments[2].episode.bridge);
        assert!(!assignments[1].episode.bridge);
    }

    #[test]
    fn distinct_episodes_share_no_input_data_except_reported_bridges() {
        let run = golden_run();
        let observed_from = crate::candidate::tests::golden_tape()[0].time();
        let mut tracker = EpisodeTracker::new();
        let assigned: Vec<(FeatureKey, Assignment)> = run
            .fired
            .iter()
            .map(|(_, key, fact)| (fact.feature, tracker.assign(fact, *key, observed_from)))
            .collect();
        let mut multi = 0;
        let mut crossing: Vec<usize> = Vec::new();
        // The data of every episode: a member overlaps the data it joins.
        let mut data: Vec<(EpisodeId, EvidenceSpan)> = Vec::new();
        for (later, (feature, assignment)) in assigned.iter().enumerate() {
            multi += usize::from(!assignment.episode.opener);
            // Under the @1 span table one event opens at most one episode
            // of an id.
            assert_eq!(assignment.episode.id.ordinal, 0);
            match data.iter_mut().find(|(id, _)| *id == assignment.episode.id) {
                Some((_, span)) => {
                    assert!(!assignment.episode.opener);
                    assert!(
                        span.overlaps(assignment.span),
                        "{feature} {}",
                        assignment.span
                    );
                    span.first = span.first.min(assignment.span.first);
                    span.last = span.last.max(assignment.span.last);
                }
                None => {
                    assert!(assignment.episode.opener);
                    data.push((assignment.episode.id, assignment.span));
                }
            }
            for (earlier_feature, earlier) in &assigned[..later] {
                if earlier_feature != feature
                    || earlier.episode.id == assignment.episode.id
                    || !earlier.span.overlaps(assignment.span)
                {
                    continue;
                }
                if crossing.last() != Some(&later) {
                    crossing.push(later);
                }
                assert!(assignment.episode.bridge, "{feature} {}", assignment.span);
            }
        }
        assert!(multi > 100, "{multi}");
        // Every sample that crosses into another episode is a bridge; the
        // check is conservative, so there may be more bridges.
        assert!(crossing.len() as u64 <= run.stats.bridges);
        assert_eq!(
            run.stats.bridges,
            assigned.iter().filter(|(_, a)| a.episode.bridge).count() as u64
        );
    }

    #[test]
    fn records_up_to_t_do_not_depend_on_events_after_t() {
        // Two tapes that differ only after `t` (every later trade 1 000 USDT
        // higher) give identical records up to `t`, episodes included.
        let cut = 20 * DAY + 12 * 3_600_000;
        let tape: Vec<MarketEvent> = crate::candidate::tests::golden_tape()
            .into_iter()
            .filter(|event| event.time().as_millis() <= cut + 2 * DAY)
            .collect();
        let other: Vec<MarketEvent> = tape
            .iter()
            .map(|event| match event {
                MarketEvent::Trade(trade) if trade.time.as_millis() > cut => {
                    MarketEvent::Trade(Trade {
                        price: Price::from_units(trade.price.units() + 1_000 * SCALE),
                        ..*trade
                    })
                }
                other => other.clone(),
            })
            .collect();
        let mut engines = [MarketStateEngine::new(), MarketStateEngine::new()];
        let mut samplers = [ContextSampler::new(None), ContextSampler::new(None)];
        let (mut before, mut differ_after) = (0, 0);
        for (a, b) in tape.iter().zip(&other) {
            let mut records = [Vec::new(), Vec::new()];
            for (side, event) in [a, b].into_iter().enumerate() {
                engines[side].apply(event).unwrap();
                samplers[side].observe(event, &engines[side], |record| records[side].push(record));
            }
            if a.time().as_millis() <= cut {
                assert_eq!(records[0], records[1], "{}", a.time());
                before += records[0].len();
            } else if records[0] != records[1] {
                differ_after += 1;
            }
        }
        assert!(before > 10_000, "{before}");
        assert!(differ_after > 0);
    }

    #[test]
    fn an_episode_open_at_the_first_event_is_truncated() {
        let run = golden_run();
        let (_, split) = run.split;
        let mut truncated = 0;
        for (_, record) in &run.late {
            let (trigger, episode) = trigger_of(record).unwrap();
            if episode.opener {
                assert_eq!(
                    episode.truncated,
                    evidence_span(trigger).first < split,
                    "{record}"
                );
                truncated += usize::from(episode.truncated);
            }
        }
        assert!(truncated > 0);
        assert_eq!(run.late_stats.truncated, truncated as u64);
        // `emit_from` suppresses records, not tracking: the windowed
        // sampler's records are the full sampler's from day 10 on.
        let (windowed, equal) = run.windowed;
        assert!(equal);
        assert!(windowed > 10_000, "{windowed}");
        // Observed from the first event, nothing reaches before it.
        assert_eq!(run.stats.truncated, 0);
    }

    /// ADR-054 D2 on one state: every snapshot value equals its state field,
    /// and every ready value names its [`SNAPSHOT_FEATURES`] key. Returns
    /// which keys were ready.
    fn check_snapshot(state: &crate::state::MarketState, ready: &mut [bool; 13]) {
        let snapshot = StateSnapshot::read(state);
        assert_eq!(snapshot.last_price, state.last_trade_price);
        assert_eq!(snapshot.atr, state.atr);
        assert_eq!(snapshot.cvd_utc_day, state.flow.cvd_utc_day);
        assert_eq!(snapshot.oi_5m, state.derivatives.oi_5m);
        let mut keys: Vec<Option<FeatureKey>> = vec![
            snapshot
                .last_price
                .ready()
                .map(|_| catalog::TRADE_LAST_PRICE_V1.key),
            snapshot
                .atr
                .ready()
                .map(|_| catalog::VOLATILITY_ATR_1H_V1.key),
        ];
        for ((timeframe, key, value), copied) in state.flow.windows.iter().zip(&snapshot.aggression)
        {
            assert_eq!(value, copied, "{timeframe}");
            keys.push(copied.ready().map(|window| {
                assert_eq!(window.feature, key);
                window.feature
            }));
        }
        keys.push(snapshot.cvd_utc_day.ready().map(|cvd| cvd.feature));
        for ((timeframe, value, _), copied) in state.candidates.iter_windows().zip(&snapshot.effort)
        {
            assert_eq!(value, copied, "{timeframe}");
            keys.push(copied.ready().map(|effort| effort.feature));
        }
        for ((timeframe, key, value), copied) in state.book.windows.iter().zip(&snapshot.liquidity)
        {
            assert_eq!(value, copied, "{timeframe}");
            keys.push(copied.ready().map(|window| {
                assert_eq!(window.feature, key);
                window.feature
            }));
        }
        keys.push(snapshot.oi_5m.ready().map(|point| point.feature));
        assert_eq!(keys.len(), SNAPSHOT_FEATURES.len());
        for ((position, key), expected) in keys.into_iter().enumerate().zip(SNAPSHOT_FEATURES) {
            if let Some(key) = key {
                assert_eq!(key, expected);
                ready[position] = true;
            }
        }
    }

    #[test]
    fn snapshot_features_are_current_and_copied_verbatim() {
        for (position, key) in SNAPSHOT_FEATURES.iter().enumerate() {
            assert!(catalog::CURRENT.contains(key), "{key}");
            assert!(!SNAPSHOT_FEATURES[..position].contains(key), "{key}");
        }
        // The book tape readies the liquidity windows, three days of the
        // golden tape the open interest; both the rest.
        let mut ready = [false; 13];
        let golden: Vec<MarketEvent> = crate::candidate::tests::golden_tape()
            .into_iter()
            .take_while(|event| event.time().as_millis() < 3 * DAY)
            .collect();
        for tape in [crate::candidate::tests::book_tape(), golden] {
            let mut engine = MarketStateEngine::new();
            let mut sampler = ContextSampler::new(None);
            for event in &tape {
                engine.apply(event).unwrap();
                let mut records = Vec::new();
                sampler.observe(event, &engine, |record| records.push(record));
                if !records.is_empty() {
                    check_snapshot(engine.state(), &mut ready);
                }
                for record in records {
                    assert_eq!(record.state, StateSnapshot::read(engine.state()));
                }
            }
        }
        assert_eq!(ready, [true; 13]);
    }

    #[test]
    fn trigger_ids_are_the_catalog_trigger_ids() {
        let catalog_ids: Vec<FeatureKey> = catalog::CURRENT
            .iter()
            .copied()
            .filter(|key| key.id.as_str().starts_with("trigger."))
            .collect();
        let ids: Vec<FeatureKey> = TRIGGER_IDS.iter().map(|(key, _)| *key).collect();
        assert_eq!(ids, catalog_ids);
        // Each id's evidence follows the span rule its slot declares.
        for (_, _, fact) in &golden_run().fired {
            let (_, rule) = TRIGGER_IDS
                .iter()
                .find(|(key, _)| *key == fact.feature)
                .unwrap();
            assert_eq!(*rule, SpanRule::of(&fact.evidence), "{fact}");
        }
        for (_, record) in auction_records() {
            let (trigger, _) = trigger_of(&record).unwrap();
            assert_eq!(SpanRule::of(&trigger.evidence), SpanRule::DecidingMinute);
        }
        assert_eq!(
            STRUCTURE_TIMEFRAMES.len() * 3 + 3 + 3 + 1 + 3 + 1,
            TRIGGER_IDS.len()
        );
    }

    #[test]
    fn sampling_policy_fingerprint_golden() {
        let text = policy_text();
        assert!(text.starts_with(
            "mie-sampling-policy 1\npolicy conditional.sampling@1\nbaseline every_event_closing_a_1m_bar anchor=last_1m_close_with_trades\nepisode scope=trigger_id join=span_overlaps_episode_data bridge=span_overlaps_closed_data\nspan trigger.failed_auction@1 deciding_minute\n"
        ));
        assert!(text.contains(
            "span trigger.oi_expansion_rejection.prior_day@1 oi_union\nsnapshot trade.last_price@1,volatility.atr.1h@1,flow.window.5m@1,"
        ));
        assert_eq!(text.lines().count(), 4 + TRIGGER_IDS.len() + 1);
        assert_eq!(policy_fingerprint().to_string(), GOLDEN_POLICY_FINGERPRINT);
    }

    #[test]
    fn provenance_names_the_data_engine_and_policy() {
        let engine = MarketStateEngine::new();
        let data = DataVersion::from_hex(&"0f".repeat(32)).unwrap();
        let provenance = ContextProvenance::new(data.clone(), &engine);
        assert_eq!(provenance.data, data);
        assert_eq!(provenance.engine.features, engine.state().feature_set);
        assert_eq!(provenance.engine.state_encoding, STATE_HASH_ENCODING);
        assert_eq!(provenance.policy, SAMPLING_V1);
        assert_eq!(provenance.policy_fingerprint, policy_fingerprint());
        assert_eq!(
            provenance.to_string(),
            format!(
                "data={} engine={} policy=conditional.sampling@1 policy_fingerprint={GOLDEN_POLICY_FINGERPRINT}",
                "0f".repeat(32),
                provenance.engine
            )
        );
    }

    #[test]
    fn no_record_text_names_a_direction_or_an_execution() {
        let run = golden_run();
        assert!(run.forbidden.is_empty(), "{:?}", run.forbidden.first());
        let (records, _) = sample(&crate::candidate::tests::book_tape());
        let mut lines: Vec<String> = records
            .iter()
            .map(|(_, record)| record.to_string())
            .collect();
        lines.extend(
            auction_records()
                .iter()
                .map(|(_, record)| record.to_string()),
        );
        lines.push(policy_text());
        for family in TriggerFamily::ALL {
            let prefix = format!(" {family} evidence=");
            let seen = lines.iter().any(|line| line.contains(&prefix))
                || run
                    .triggers
                    .iter()
                    .any(|(_, record, _)| record.to_string().contains(&prefix));
            assert!(seen, "{family}");
        }
        for line in &lines {
            assert_eq!(forbidden_in(line), None, "{line}");
        }
    }

    #[test]
    fn golden_context_records() {
        let (count, digest, first) = &golden_run().golden;
        assert_eq!(first, GOLDEN_FIRST_TRIGGER_RECORD);
        assert_eq!((*count, digest.as_str()), GOLDEN_RECORDS);
    }

    #[test]
    #[ignore = "prints the goldens: cargo test -p mie-domain print_context_golden -- --ignored --nocapture"]
    fn print_context_golden() {
        let run = golden_run();
        println!("policy {}", policy_fingerprint());
        println!("records {} {}", run.golden.0, run.golden.1);
        println!("first {}", run.golden.2);
        println!("stats {:?}", run.stats);
    }

    const GOLDEN_POLICY_FINGERPRINT: &str = "2c71fff23e8b0415";
    const GOLDEN_RECORDS: (usize, &str) = (46_403, "2cc22dddefd1246f");
    const GOLDEN_FIRST_TRIGGER_RECORD: &str = "62161380ms Trade seq 163 trigger episode=trigger.sfp.15m@1/62161380ms/163/0 opener sfp evidence=[15m low level=61969.20000000 bar=55800000ms confirmed=59400000ms touches=2 at=61514129ms price=61966.54000000 extreme=61966.54000000 bars=1 sfp resolved=62100000ms complete structure.levels.15m@1] trigger.sfp.15m@1 ctx=61969.20000000 levels=vwap@61984.17149090 location.levels@2 auction=warming bias regime=warming 15m=[high=- low=-] 1h=warming 4h=warming 1d=warming bias.context@1 state trade.last_price@1=[61977.49000000] volatility.atr.1h@1=warming flow.window.5m@1=[5m end=62160000ms buy=0.46222275 sell=1.05082227 delta=-0.58859952 vol=1.51304502 trades=2 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=61972.31000000 disp=3.88000000 imbalance=-0.38901652774350365 trades_per_min=0.4 vol_per_min=0.302609004 large_share=0 response=-6.591918389603851 complete flow.window.5m@1] flow.window.15m@1=[15m end=62160000ms buy=4.11021922 sell=2.08457016 delta=2.02564906 vol=6.19478938 trades=10 large=0 large_buy=0.00000000 large_sell=0.00000000 ref=61978.59000000 disp=-2.40000000 imbalance=0.32699240212102254 trades_per_min=0.6666666666666666 vol_per_min=0.41298595866666665 large_share=0 response=-1.184805427254018 complete flow.window.15m@1] flow.window.1h@1=[1h end=62160000ms buy=24.14170459 sell=15.20346730 delta=8.93823729 vol=39.34517189 trades=40 large=6 large_buy=1.86027866 large_sell=8.99881232 ref=61978.70000000 disp=-2.51000000 imbalance=0.2271749457592724 trades_per_min=0.6666666666666666 vol_per_min=0.6557528648333334 large_share=0.2759955150370039 response=-0.28081599520837963 complete flow.window.1h@1] flow.cvd.utc_day@1=[day=0ms cvd=-7.67851777 partial_start flow.cvd.utc_day@1] candidate.effort_result.5m@1=warming candidate.effort_result.15m@1=warming candidate.effort_result.1h@1=warming book.liquidity.window.5m@1=warming book.liquidity.window.15m@1=warming book.liquidity.window.1h@1=warming derivatives.oi.5m@1=[boundary=62100000ms oi=80066.10000000 sample=62100000ms res=300000ms delta=-42.20000000 vel_per_min=-8.44 derivatives.oi.5m@1]";
}
