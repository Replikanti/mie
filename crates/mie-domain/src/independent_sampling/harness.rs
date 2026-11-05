//! Runs a tape through one engine and several (sampler, model) pairs, and
//! compares every record, every evidence span and the final counts.
//!
//! Edge classes are counted from the model's view (its spans, its episode
//! state before each assignment), never from the sampler's.

use super::model::{Assigned, Model, Observation, PADDING_BPS, Span};
use crate::candidate::CandidateEvent;
use crate::conditional::{ContextRecord, ContextSampler, Sample, evidence_span};
use crate::event::{MarketEvent, Stream};
use crate::feature::FeatureValue;
use crate::state::MarketStateEngine;
use crate::structure::SWING_BARS;
use crate::time::EventTime;
use crate::trigger::{TriggerEvent, TriggerEvidence};

/// How a (sampler, model) pair is started on a tape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Start {
    /// Observes and emits from the tape's first event.
    Full,
    /// Observes from the first event, emits from this time on.
    EmitFrom(EventTime),
    /// A late observer: observes and emits from this event index on.
    LateAt(usize),
}

/// One tape of the comparison.
#[derive(Debug, Clone)]
pub(super) struct Tape {
    pub(super) name: String,
    pub(super) seed: u64,
    pub(super) events: Vec<MarketEvent>,
    pub(super) starts: Vec<Start>,
}

/// What a comparison saw, summed over every (tape, start) pair: the record
/// kinds compared (to rule out a vacuous pass) and the edge classes of #146.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Seen {
    pub(super) events: u64,
    pub(super) rejected: u64,
    pub(super) records: u64,
    pub(super) baseline: u64,
    pub(super) trigger: u64,
    pub(super) non_opener: u64,
    pub(super) truncated: u64,
    /// Events whose records a pair held back before its `emit_from`.
    pub(super) held_back: u64,
    /// Class 1: emitted bridge samples.
    pub(super) bridge: u64,
    /// Class 2: baseline anchors exactly on a Ready level's padded edge.
    pub(super) padded_edge: u64,
    /// Class 3: spans whose `first` is earlier than the previous span of
    /// their id.
    pub(super) out_of_order: u64,
    /// Class 4: baseline events that close 1m bars, all of them empty.
    pub(super) empty_only: u64,
    /// Class 5: CVD divergences whose current swing's confirmation interval
    /// intersects a trades gap.
    pub(super) gap_confirmation: u64,
    /// Class 6: openers with `first == observed_from`.
    pub(super) first_at_observed: u64,
    /// Reported only: spans wholly before their id's open episode.
    pub(super) wholly_before: u64,
    /// Reported only: SFP spans out of order (class 3 on SFP ids).
    pub(super) sfp_out_of_order: u64,
}

impl Seen {
    pub(super) fn add(&mut self, other: &Self) {
        let Self {
            events,
            rejected,
            records,
            baseline,
            trigger,
            non_opener,
            truncated,
            held_back,
            bridge,
            padded_edge,
            out_of_order,
            empty_only,
            gap_confirmation,
            first_at_observed,
            wholly_before,
            sfp_out_of_order,
        } = other;
        self.events += events;
        self.rejected += rejected;
        self.records += records;
        self.baseline += baseline;
        self.trigger += trigger;
        self.non_opener += non_opener;
        self.truncated += truncated;
        self.held_back += held_back;
        self.bridge += bridge;
        self.padded_edge += padded_edge;
        self.out_of_order += out_of_order;
        self.empty_only += empty_only;
        self.gap_confirmation += gap_confirmation;
        self.first_at_observed += first_at_observed;
        self.wholly_before += wholly_before;
        self.sfp_out_of_order += sfp_out_of_order;
    }
}

struct Pair {
    start: Start,
    sampler: Option<ContextSampler>,
    model: Option<Model>,
}

impl Pair {
    fn new(start: Start) -> Self {
        Self {
            start,
            sampler: None,
            model: None,
        }
    }

    /// Starts the pair at event `index` if its start says so.
    fn begin(&mut self, index: usize) {
        if self.sampler.is_some() {
            return;
        }
        let emit_from = match self.start {
            Start::Full => None,
            Start::EmitFrom(time) => Some(time),
            Start::LateAt(at) if at == index => None,
            Start::LateAt(_) => return,
        };
        self.sampler = Some(ContextSampler::new(emit_from));
        self.model = Some(Model::new(emit_from));
    }
}

/// The trades gaps and trade times of a tape, for class 5.
struct TradesTimeline {
    gaps: Vec<(EventTime, EventTime)>,
    trades: Vec<EventTime>,
}

impl TradesTimeline {
    fn of(events: &[MarketEvent]) -> Self {
        let mut gaps = Vec::new();
        let mut trades = Vec::new();
        for event in events {
            match event {
                MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                    gaps.push((gap.start, gap.end));
                }
                MarketEvent::Trade(trade) => trades.push(trade.time),
                _ => {}
            }
        }
        trades.sort();
        Self { gaps, trades }
    }

    fn intersects_gap(&self, from: EventTime, to: EventTime) -> bool {
        self.gaps
            .iter()
            .any(|&(start, end)| start <= to && from <= end)
    }

    /// Trades with `from ≤ time < to`.
    fn trades_in(&self, from: EventTime, to: EventTime) -> usize {
        let lo = self.trades.partition_point(|&time| time < from);
        let hi = self.trades.partition_point(|&time| time < to);
        hi.saturating_sub(lo)
    }
}

fn describe(records: &[ContextRecord]) -> String {
    records
        .iter()
        .map(|record| format!("  {record}\n  {record:?}"))
        .collect::<Vec<_>>()
        .join("\n")
}

/// Runs `tape` and panics on the first mismatch between a sampler and its
/// model. Returns what the comparison saw.
pub(super) fn run(tape: &Tape) -> Seen {
    let timeline = TradesTimeline::of(&tape.events);
    let mut engine = MarketStateEngine::new();
    let mut pairs: Vec<Pair> = tape.starts.iter().map(|&start| Pair::new(start)).collect();
    let mut seen = Seen::default();
    for (index, event) in tape.events.iter().enumerate() {
        seen.events += 1;
        // ADR-054 D6: the sampler is called for accepted events only.
        if engine.apply(event).is_err() {
            seen.rejected += 1;
            continue;
        }
        let where_ = || {
            format!(
                "tape {} seed {:#x} event {index} key {}",
                tape.name,
                tape.seed,
                event.canonical_key()
            )
        };
        for pair in &mut pairs {
            pair.begin(index);
            let (Some(sampler), Some(model)) = (pair.sampler.as_mut(), pair.model.as_mut()) else {
                continue;
            };
            let mut got = Vec::new();
            sampler.observe(event, &engine, |record| got.push(record));
            let want = model.observe(event, &engine);
            if got != want.records {
                panic!(
                    "{}, start {:?}: record mismatch\nsampler ({}):\n{}\nmodel ({}):\n{}",
                    where_(),
                    pair.start,
                    got.len(),
                    describe(&got),
                    want.records.len(),
                    describe(&want.records),
                );
            }
            for (trigger, assigned) in engine.trigger_events().iter().zip(&want.assigned) {
                let span = evidence_span(trigger);
                let model_span = assigned.span;
                assert!(
                    (span.first, span.last) == (model_span.first, model_span.last),
                    "{}: span of {trigger}: sampler {span}, model {model_span:?}",
                    where_(),
                );
            }
            classify(&mut seen, &want, &engine, &timeline, &where_());
        }
    }
    for pair in &pairs {
        let (Some(sampler), Some(model)) = (pair.sampler.as_ref(), pair.model.as_ref()) else {
            panic!("tape {}: start {:?} never began", tape.name, pair.start);
        };
        compare_counts(tape, pair.start, &sampler.stats(), model);
    }
    seen
}

fn compare_counts(
    tape: &Tape,
    start: Start,
    stats: &crate::conditional::SamplingStats,
    model: &Model,
) {
    let counts = model.counts();
    let context = || format!("tape {} seed {:#x} start {start:?}", tape.name, tape.seed);
    assert_eq!(stats.records, counts.records, "{}: records", context());
    let mut episodes = 0;
    let mut bridges = 0;
    let mut truncated = 0;
    for (key, ids) in &counts.per_id {
        let got = stats
            .trigger(*key)
            .unwrap_or_else(|| panic!("{}: no stats for {key}", context()));
        let want = (
            ids.samples,
            ids.episodes.len() as u64,
            ids.bridges,
            ids.truncated.len() as u64,
        );
        assert_eq!(
            (got.samples, got.episodes, got.bridges, got.truncated),
            want,
            "{}: (samples, episodes, bridges, truncated) of {key}",
            context(),
        );
        episodes += want.1;
        bridges += want.2;
        truncated += want.3;
    }
    assert_eq!(
        (stats.episodes, stats.bridges, stats.truncated),
        (episodes, bridges, truncated),
        "{}: total (episodes, bridges, truncated)",
        context(),
    );
}

/// Whether `price` lies exactly on the padded edge of a Ready level:
/// `low − w` or `high + w`, `w` = 6 bps of the level price, exact.
fn on_padded_edge(engine: &MarketStateEngine, price: crate::num::Price) -> bool {
    let FeatureValue::Ready(set) = &engine.state().location.levels else {
        return false;
    };
    let p = i128::from(price.units());
    set.levels().iter().any(|level| {
        let pad = i128::from(level.price.units()).abs() * PADDING_BPS;
        let below = i128::from(level.low.units()) - p;
        let above = p - i128::from(level.high.units());
        (below > 0 && below * 10_000 == pad) || (above > 0 && above * 10_000 == pad)
    })
}

fn is_sfp(trigger: &TriggerEvent) -> bool {
    matches!(trigger.evidence, TriggerEvidence::Sfp(_))
}

fn classify(
    seen: &mut Seen,
    observation: &Observation,
    engine: &MarketStateEngine,
    timeline: &TradesTimeline,
    where_: &str,
) {
    if !observation.emitted {
        if observation.anchor.is_some() || !observation.assigned.is_empty() {
            seen.held_back += 1;
        }
        return;
    }
    for record in &observation.records {
        seen.records += 1;
        match &record.sample {
            Sample::Baseline { .. } => seen.baseline += 1,
            Sample::Trigger { episode, .. } => {
                seen.trigger += 1;
                seen.non_opener += u64::from(!episode.opener);
                seen.truncated += u64::from(episode.truncated);
                seen.bridge += u64::from(episode.bridge);
            }
        }
    }
    if let Some(anchor) = observation.anchor {
        seen.padded_edge += u64::from(on_padded_edge(engine, anchor));
        if observation.closed_minutes > 0 && observation.closed_with_trades == 0 {
            seen.empty_only += 1;
        }
    }
    for (trigger, assigned) in engine.trigger_events().iter().zip(&observation.assigned) {
        classify_trigger(seen, trigger, assigned, timeline, where_);
    }
}

fn classify_trigger(
    seen: &mut Seen,
    trigger: &TriggerEvent,
    assigned: &Assigned,
    timeline: &TradesTimeline,
    where_: &str,
) {
    let Span { first, last } = assigned.span;
    if assigned
        .previous
        .is_some_and(|previous| first < previous.first)
    {
        seen.out_of_order += 1;
        seen.sfp_out_of_order += u64::from(is_sfp(trigger));
    }
    if assigned.open_before.is_some_and(|open| last < open.first) {
        seen.wholly_before += 1;
    }
    if assigned.episode.opener && first == assigned.observed_from {
        seen.first_at_observed += 1;
    }
    if let TriggerEvidence::Candidate(candidate) = &trigger.evidence
        && let CandidateEvent::CvdDivergence(event) = candidate.as_ref()
    {
        let current = event.comparison.pair.current;
        let tf = event.timeframe.millis();
        let swing_end = EventTime::from_millis(current.swing_time.as_millis() + tf);
        if timeline.intersects_gap(swing_end, current.known_at) {
            seen.gap_confirmation += 1;
            // ADR-054 D4: "no trade lies between that bar's end and this
            // event (it is the first trades-stream event after it)".
            let confirming_end = EventTime::from_millis(
                current.swing_time.as_millis() + tf * (SWING_BARS as i64 + 1),
            );
            assert_eq!(
                timeline.trades_in(confirming_end, current.known_at),
                0,
                "{where_}: a trade between the confirming bar end {confirming_end} and \
                 known_at {} of {trigger}",
                current.known_at,
            );
        }
    }
}
