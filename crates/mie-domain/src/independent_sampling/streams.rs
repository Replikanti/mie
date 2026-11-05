//! Seeded synthetic trigger streams for the ADR-054 D4 episode rule.
//!
//! Two templates from the candidate golden replay, one `trigger.sfp.15m@1`
//! (a span of variable width, `sweep.time` to `resolved_bar_end − 1 ms`) and
//! one `trigger.effort_result.5m@1` (a fixed 5-minute window), are retimed
//! onto a coarse millisecond grid, so overlaps, touches, containments,
//! spans wholly before the open episode and out-of-order spans are all
//! frequent. Each trigger goes to the sampler's public `EpisodeTracker` (the
//! system under test) and to the model's `Episodes`; the episode and the
//! span must be equal.
//!
//! On engine tapes, a span never ends before its id's open episode starts
//! (every `last` is at most the emitting event and grows in emission order
//! within an id), so a one-sided join is equivalent there. These streams
//! are where it is observable.

use super::model::{Assigned, Episodes, Span};
use crate::bars::tests::Lcg;
use crate::candidate::CandidateEvent;
use crate::conditional::{EpisodeTracker, evidence_span};
use crate::event::MarketEvent;
use crate::feature::FeatureKey;
use crate::order::{CanonicalKey, EventKind};
use crate::state::MarketStateEngine;
use crate::time::EventTime;
use crate::trigger::{TriggerEvent, TriggerEvidence};

/// The grid the retimed spans sit on.
const GRID_MS: i64 = 60_000;

/// The two templates.
#[derive(Debug, Clone)]
pub(super) struct Templates {
    sfp: TriggerEvent,
    effort: TriggerEvent,
}

const SFP_15M: FeatureKey = FeatureKey::new("trigger.sfp.15m", 1);
const EFFORT_5M: FeatureKey = FeatureKey::new("trigger.effort_result.5m", 1);

impl Templates {
    /// The first `trigger.sfp.15m@1` and `trigger.effort_result.5m@1` of
    /// `tape`.
    pub(super) fn from_tape(tape: &[MarketEvent]) -> Self {
        let mut engine = MarketStateEngine::new();
        let (mut sfp, mut effort) = (None, None);
        for event in tape {
            engine.apply(event).expect("the golden tape is accepted");
            for trigger in engine.trigger_events() {
                if trigger.feature == SFP_15M && sfp.is_none() {
                    sfp = Some(trigger.clone());
                }
                if trigger.feature == EFFORT_5M && effort.is_none() {
                    effort = Some(trigger.clone());
                }
            }
            if let (Some(sfp), Some(effort)) = (&sfp, &effort) {
                return Self {
                    sfp: sfp.clone(),
                    effort: effort.clone(),
                };
            }
        }
        panic!("the tape has no 15m SFP or no 5m effort trigger");
    }

    /// The SFP template with the span `[first, last]`.
    fn sfp(&self, first: i64, last: i64) -> TriggerEvent {
        let mut trigger = self.sfp.clone();
        let TriggerEvidence::Sfp(sweep) = &mut trigger.evidence else {
            panic!("an SFP template");
        };
        sweep.time = EventTime::from_millis(first);
        sweep.resolved_bar_end = Some(EventTime::from_millis(last + 1));
        trigger
    }

    /// The effort template whose 5-minute window ends at `end`.
    fn effort(&self, end: i64) -> TriggerEvent {
        let mut trigger = self.effort.clone();
        let TriggerEvidence::Candidate(candidate) = &mut trigger.evidence else {
            panic!("an effort template");
        };
        let CandidateEvent::EffortVsResult(event) = candidate.as_mut() else {
            panic!("an effort template");
        };
        event.value.end = EventTime::from_millis(end);
        trigger
    }
}

/// The stream cases of #146 (plan change 5), counted from the model's view.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct Cases {
    pub(super) assignments: u64,
    /// Overlaps the open episode without touching or containment.
    pub(super) overlap: u64,
    /// `a.last + 1 ms == b.first` against the open episode, either way.
    pub(super) touch: u64,
    /// One of the span and the open episode contains the other.
    pub(super) containment: u64,
    /// The span lies wholly before the open episode.
    pub(super) wholly_before: u64,
    /// `first` earlier than the id's previous span.
    pub(super) out_of_order: u64,
    /// Two disjoint spans opened at one event, the later span second.
    pub(super) disjoint_in_order: u64,
    /// Two disjoint spans opened at one event, the earlier span second.
    pub(super) disjoint_reversed: u64,
    /// An opener with `first == observed_from`.
    pub(super) first_at_observed: u64,
    pub(super) bridges: u64,
    pub(super) truncated: u64,
    /// An opener that misses the hull of the id's earlier closed episodes
    /// but meets it once the episode it closes is added: the case that
    /// decides the model's bridge reading (only earlier closed episodes
    /// count).
    pub(super) bridge_reading: u64,
}

fn grid(lcg: &mut Lcg, cells: u64) -> i64 {
    lcg.below(cells) * GRID_MS
}

/// One seeded stream: `len` events, each emitting one or two triggers of
/// the two ids, compared one by one. Panics on the first mismatch.
pub(super) fn run_stream(templates: &Templates, seed: u64, len: usize) -> Cases {
    let mut lcg = Lcg(seed);
    let mut tracker = EpisodeTracker::new();
    let mut model = Episodes::new();
    let mut cases = Cases::default();
    // The first observed event, on the grid: some spans start before it,
    // some exactly at it.
    let observed_from = 50 * GRID_MS;
    let mut now = observed_from;
    for step in 0..len {
        now += grid(&mut lcg, 4);
        let key = CanonicalKey {
            time: EventTime::from_millis(now),
            kind: EventKind::Trade,
            seq: step as u64 + 1,
        };
        let mut batch = Vec::new();
        match lcg.below(6) {
            // Two disjoint SFP spans at one event, in a random order.
            0 => {
                let a_first = now - grid(&mut lcg, 40);
                let a_last = a_first + grid(&mut lcg, 5) + GRID_MS - 1;
                let b_first = a_last + 1 + grid(&mut lcg, 3) + GRID_MS;
                let b_last = b_first + grid(&mut lcg, 5) + GRID_MS - 1;
                let (a, b) = (
                    templates.sfp(a_first, a_last),
                    templates.sfp(b_first, b_last),
                );
                if lcg.below(2) == 0 {
                    batch.extend([a, b]);
                } else {
                    batch.extend([b, a]);
                }
            }
            // An SFP span anywhere near the stream's recent past, some
            // starting exactly at `observed_from` early on.
            1 | 2 => {
                let first = if step < 8 && lcg.below(2) == 0 {
                    observed_from
                } else {
                    now - grid(&mut lcg, 60)
                };
                let last = first + grid(&mut lcg, 20) + GRID_MS - 1;
                batch.push(templates.sfp(first, last));
            }
            // A span that touches, contains or lies wholly before the open
            // episode's neighbourhood: a short one far back.
            3 => {
                let first = now - grid(&mut lcg, 120);
                batch.push(templates.sfp(first, first + GRID_MS - 1));
            }
            // Effort windows ending on the grid, so neighbours touch.
            _ => {
                let end = now - grid(&mut lcg, 10);
                batch.push(templates.effort(end));
                if lcg.below(3) == 0 {
                    batch.push(templates.effort(end + 5 * GRID_MS));
                }
            }
        }
        let mut opened_here: Vec<Span> = Vec::new();
        for trigger in &batch {
            let got = tracker.assign(trigger, key, EventTime::from_millis(observed_from));
            let want = model.assign(trigger, key, EventTime::from_millis(observed_from));
            let span = evidence_span(trigger);
            assert!(
                got.episode == want.episode
                    && (got.span.first, got.span.last) == (want.span.first, want.span.last)
                    && (span.first, span.last) == (want.span.first, want.span.last),
                "stream seed {seed:#x} step {step} {}: tracker {} {}, model {} {:?}",
                trigger.feature,
                got.episode,
                got.span,
                want.episode,
                want.span,
            );
            count(
                &mut cases,
                &want,
                &mut opened_here,
                trigger.feature == SFP_15M,
            );
        }
    }
    cases
}

fn count(cases: &mut Cases, assigned: &Assigned, opened_here: &mut Vec<Span>, sfp: bool) {
    let span = assigned.span;
    cases.assignments += 1;
    cases.bridges += u64::from(assigned.episode.bridge);
    cases.truncated += u64::from(assigned.episode.truncated);
    if let Some(open) = assigned.open_before {
        let touches = open.last.as_millis() + 1 == span.first.as_millis()
            || span.last.as_millis() + 1 == open.first.as_millis();
        let contains = (open.first <= span.first && span.last <= open.last)
            || (span.first <= open.first && open.last <= span.last);
        cases.touch += u64::from(touches);
        cases.containment += u64::from(contains);
        cases.overlap += u64::from(span.meets(open) && !contains);
        cases.wholly_before += u64::from(span.last < open.first);
        if let Some(closed) = assigned.closed_before
            && assigned.episode.opener
            && !span.meets(closed)
            && span.meets(closed.hull(open))
        {
            cases.bridge_reading += 1;
        }
    }
    if assigned
        .previous
        .is_some_and(|previous| span.first < previous.first)
    {
        cases.out_of_order += 1;
    }
    if assigned.episode.opener && span.first == assigned.observed_from {
        cases.first_at_observed += 1;
    }
    if sfp && assigned.episode.opener {
        if assigned.episode.id.ordinal == 1
            && let Some(before) = opened_here.last()
        {
            if before.last < span.first {
                cases.disjoint_in_order += 1;
            } else if span.last < before.first {
                cases.disjoint_reversed += 1;
            }
        }
        opened_here.push(span);
    }
}

impl Cases {
    pub(super) fn add(&mut self, other: &Self) {
        let Self {
            assignments,
            overlap,
            touch,
            containment,
            wholly_before,
            out_of_order,
            disjoint_in_order,
            disjoint_reversed,
            first_at_observed,
            bridges,
            truncated,
            bridge_reading,
        } = other;
        self.assignments += assignments;
        self.overlap += overlap;
        self.touch += touch;
        self.containment += containment;
        self.wholly_before += wholly_before;
        self.out_of_order += out_of_order;
        self.disjoint_in_order += disjoint_in_order;
        self.disjoint_reversed += disjoint_reversed;
        self.first_at_observed += first_at_observed;
        self.bridges += bridges;
        self.truncated += truncated;
        self.bridge_reading += bridge_reading;
    }
}
