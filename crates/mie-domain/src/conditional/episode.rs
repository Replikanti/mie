//! Episodes: overlap control without a time constant (ADR-054 D4).
//!
//! Each trigger is reduced to its **evidence span** `[first, last]`, the
//! data time of the first and the last input its evidence was built from,
//! read from the evidence's own fields ([`evidence_span`]). Within one
//! trigger id, a trigger joins the id's open episode when `first ≤
//! open.last`, and the episode's `last` becomes the later of the two;
//! otherwise it opens a new episode. Every sample is kept; counts are taken
//! over episodes.
//!
//! Streaming cannot merge two closed episodes, so a trigger that joins the
//! open episode while its `first` reaches back to the previous closed
//! episode's data is a **bridge**: [`EpisodeTracker::assign`] reports it,
//! and outside bridges two episodes of one id share no input data.

use crate::candidate::CandidateEvent;
use crate::feature::{FeatureKey, catalog};
use crate::location::AuctionStatus;
use crate::order::CanonicalKey;
use crate::structure::Sweep;
use crate::time::EventTime;
use crate::trigger::{OiRejection, TriggerEvent, TriggerEvidence};
use std::fmt;

/// The length of the deciding bar of an auction trigger: the 1m bar whose
/// close changed the label (ADR-044 classifies at 1m closes).
const MINUTE_MS: i64 = 60_000;

/// How the evidence span of a trigger id is read from its evidence (ADR-054
/// D4). The names are part of the sampling policy text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpanRule {
    /// The deciding 1m bar: `[bar_end − 60 000 ms, bar_end − 1 ms]`.
    DecidingMinute,
    /// The sweep's excursion: `[sweep.time, resolved_bar_end − 1 ms]`.
    SweepToResolution,
    /// The swing pair's anchor trades: `[previous.time, current.time]`.
    SwingPair,
    /// The effort window's minutes: `[end − w, end − 1 ms]`.
    EffortWindow,
    /// The expansion window to the decay window's end:
    /// `[expansion.end − expansion.timeframe, end − 1 ms]`.
    Exhaustion,
    /// The union of the rejection's span and both open-interest boundaries.
    OiUnion,
}

impl SpanRule {
    /// The rule of `evidence`.
    pub fn of(evidence: &TriggerEvidence) -> Self {
        match evidence {
            TriggerEvidence::Auction(_) => Self::DecidingMinute,
            TriggerEvidence::Sfp(_) => Self::SweepToResolution,
            TriggerEvidence::Candidate(fact) => match fact.as_ref() {
                CandidateEvent::CvdDivergence(_) | CandidateEvent::OiDivergence(_) => {
                    Self::SwingPair
                }
                CandidateEvent::EffortVsResult(_) | CandidateEvent::Absorption(_) => {
                    Self::EffortWindow
                }
                CandidateEvent::Exhaustion(_) => Self::Exhaustion,
            },
            TriggerEvidence::OiExpansion(_) => Self::OiUnion,
        }
    }

    /// The snake_case name the policy text uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DecidingMinute => "deciding_minute",
            Self::SweepToResolution => "sweep_to_resolution",
            Self::SwingPair => "swing_pair",
            Self::EffortWindow => "effort_window",
            Self::Exhaustion => "exhaustion",
            Self::OiUnion => "oi_union",
        }
    }
}

impl fmt::Display for SpanRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Every trigger id with the span rule of its evidence, in catalog order:
/// the episode slots (ADR-054 D4, D6).
pub const TRIGGER_IDS: [(FeatureKey, SpanRule); 23] = [
    (
        catalog::TRIGGER_FAILED_AUCTION_V1.key,
        SpanRule::DecidingMinute,
    ),
    (
        catalog::TRIGGER_FAILED_RECLAIM_V1.key,
        SpanRule::DecidingMinute,
    ),
    (
        catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key,
        SpanRule::DecidingMinute,
    ),
    (catalog::TRIGGER_SFP_15M_V1.key, SpanRule::SweepToResolution),
    (catalog::TRIGGER_SFP_1H_V1.key, SpanRule::SweepToResolution),
    (catalog::TRIGGER_SFP_4H_V1.key, SpanRule::SweepToResolution),
    (catalog::TRIGGER_SFP_1D_V1.key, SpanRule::SweepToResolution),
    (
        catalog::TRIGGER_CVD_DIVERGENCE_15M_V1.key,
        SpanRule::SwingPair,
    ),
    (
        catalog::TRIGGER_CVD_DIVERGENCE_1H_V1.key,
        SpanRule::SwingPair,
    ),
    (
        catalog::TRIGGER_CVD_DIVERGENCE_4H_V1.key,
        SpanRule::SwingPair,
    ),
    (
        catalog::TRIGGER_CVD_DIVERGENCE_1D_V1.key,
        SpanRule::SwingPair,
    ),
    (
        catalog::TRIGGER_EFFORT_RESULT_5M_V1.key,
        SpanRule::EffortWindow,
    ),
    (
        catalog::TRIGGER_EFFORT_RESULT_15M_V1.key,
        SpanRule::EffortWindow,
    ),
    (
        catalog::TRIGGER_EFFORT_RESULT_1H_V1.key,
        SpanRule::EffortWindow,
    ),
    (catalog::TRIGGER_EXHAUSTION_V1.key, SpanRule::Exhaustion),
    (
        catalog::TRIGGER_ABSORPTION_5M_V1.key,
        SpanRule::EffortWindow,
    ),
    (
        catalog::TRIGGER_ABSORPTION_15M_V1.key,
        SpanRule::EffortWindow,
    ),
    (
        catalog::TRIGGER_ABSORPTION_1H_V1.key,
        SpanRule::EffortWindow,
    ),
    (
        catalog::TRIGGER_OI_EXPANSION_REJECTION_15M_V1.key,
        SpanRule::OiUnion,
    ),
    (
        catalog::TRIGGER_OI_EXPANSION_REJECTION_1H_V1.key,
        SpanRule::OiUnion,
    ),
    (
        catalog::TRIGGER_OI_EXPANSION_REJECTION_4H_V1.key,
        SpanRule::OiUnion,
    ),
    (
        catalog::TRIGGER_OI_EXPANSION_REJECTION_1D_V1.key,
        SpanRule::OiUnion,
    ),
    (
        catalog::TRIGGER_OI_EXPANSION_REJECTION_PRIOR_DAY_V1.key,
        SpanRule::OiUnion,
    ),
];

/// The data time of the first and the last input a trigger's evidence was
/// built from, both inclusive (ADR-054 D4).
///
/// `Display` prints `[<first>,<last>]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EvidenceSpan {
    /// The first input's data time.
    pub first: EventTime,
    /// The last input's data time.
    pub last: EventTime,
}

impl EvidenceSpan {
    /// Whether the two spans share a millisecond.
    pub fn overlaps(self, other: Self) -> bool {
        self.first <= other.last && other.first <= self.last
    }
}

impl fmt::Display for EvidenceSpan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{},{}]", self.first, self.last)
    }
}

/// `time − millis`, saturating.
fn minus(time: EventTime, millis: i64) -> EventTime {
    EventTime::from_millis(time.as_millis().saturating_sub(millis))
}

/// The span of an auction status: its deciding 1m bar.
fn auction_span(status: &AuctionStatus) -> EvidenceSpan {
    EvidenceSpan {
        first: minus(status.bar_end, MINUTE_MS),
        last: minus(status.bar_end, 1),
    }
}

/// The span of an SFP: the sweeping trade to the end of the resolving bar.
fn sweep_span(sweep: &Sweep) -> EvidenceSpan {
    EvidenceSpan {
        first: sweep.time,
        // A resolved sweep has a resolving bar; the visibility time bounds
        // it otherwise.
        last: sweep
            .resolved_bar_end
            .map_or(sweep.known_at, |end| minus(end, 1)),
    }
}

/// The evidence span of `trigger` (ADR-054 D4), read from its evidence's
/// own fields: one arm per [`TriggerEvidence`] variant, with no constant
/// but the deciding minute of the auction rules.
pub fn evidence_span(trigger: &TriggerEvent) -> EvidenceSpan {
    match &trigger.evidence {
        TriggerEvidence::Auction(status) => auction_span(status),
        TriggerEvidence::Sfp(sweep) => sweep_span(sweep),
        TriggerEvidence::Candidate(fact) => match fact.as_ref() {
            CandidateEvent::CvdDivergence(event) => EvidenceSpan {
                first: event.comparison.pair.previous.time,
                last: event.comparison.pair.current.time,
            },
            CandidateEvent::OiDivergence(event) => EvidenceSpan {
                first: event.comparison.pair.previous.time,
                last: event.comparison.pair.current.time,
            },
            CandidateEvent::EffortVsResult(event) => EvidenceSpan {
                first: minus(event.value.end, event.value.timeframe.millis()),
                last: minus(event.value.end, 1),
            },
            CandidateEvent::Absorption(event) => EvidenceSpan {
                first: minus(
                    event.value.effort.end,
                    event.value.effort.timeframe.millis(),
                ),
                last: minus(event.value.effort.end, 1),
            },
            CandidateEvent::Exhaustion(event) => EvidenceSpan {
                first: minus(
                    event.value.expansion.end,
                    event.value.expansion.timeframe.millis(),
                ),
                last: minus(event.value.end, 1),
            },
        },
        TriggerEvidence::OiExpansion(expansion) => {
            let rejection = match &expansion.rejection {
                OiRejection::Sfp(sweep) => sweep_span(sweep),
                OiRejection::FailedAuction(status) => auction_span(status),
            };
            EvidenceSpan {
                first: rejection.first.min(expansion.start.boundary),
                last: rejection.last.max(expansion.end.boundary),
            }
        }
    }
}

/// The identity of an episode (ADR-054 D4): its trigger id, the event that
/// opened it and how many episodes the id had opened at that event before
/// it. One close can confirm a high and a low swing with disjoint spans, so
/// one event can open two episodes of one id.
///
/// `Display` prints `<trigger>/<opened_at time>/<seq>/<ordinal>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct EpisodeId {
    /// The trigger id.
    pub trigger: FeatureKey,
    /// The event that opened it.
    pub opened_at: CanonicalKey,
    /// Episodes the id opened at `opened_at` before this one, from 0.
    pub ordinal: u32,
}

impl fmt::Display for EpisodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}/{}/{}/{}",
            self.trigger, self.opened_at.time, self.opened_at.seq, self.ordinal
        )
    }
}

/// The episode of one trigger sample (ADR-054 D4). Consumers count
/// `DISTINCT id`.
///
/// `Display` prints `episode=<id>` and the flags that are set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Episode {
    /// The episode.
    pub id: EpisodeId,
    /// The sample opened the episode.
    pub opener: bool,
    /// The opener's `first` is earlier than the first event the sampler
    /// observed: earlier members may be missing.
    pub truncated: bool,
}

impl fmt::Display for Episode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "episode={}", self.id)?;
        if self.opener {
            f.write_str(" opener")?;
        }
        if self.truncated {
            f.write_str(" truncated")?;
        }
        Ok(())
    }
}

/// What [`EpisodeTracker::assign`] decided for one trigger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Assignment {
    /// The trigger's episode.
    pub episode: Episode,
    /// The trigger's evidence span.
    pub span: EvidenceSpan,
    /// The trigger joined the open episode with a `first` at or before the
    /// previous closed episode's `last`.
    pub bridge: bool,
}

/// The open episode of one trigger id.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Open {
    /// Its identity.
    id: EpisodeId,
    /// The latest `last` of its members.
    last: EventTime,
    /// Its opener's `first` precedes the first observed event.
    truncated: bool,
    /// A sample of it has been emitted, for [`EpisodeTracker::first_emission`].
    emitted: bool,
}

/// The episode state of one trigger id.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct Slot {
    /// The open episode.
    open: Option<Open>,
    /// The `last` of the previous closed episode, for bridges.
    closed_last: Option<EventTime>,
    /// The last event at which the id opened an episode, and how many it
    /// opened there.
    opened: Option<(CanonicalKey, u32)>,
}

/// One open episode plus the previous closed episode's `last` per trigger
/// id, in fixed [`TRIGGER_IDS`] slots (ADR-054 D4, D6).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpisodeTracker {
    slots: [Slot; TRIGGER_IDS.len()],
}

impl Default for EpisodeTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl EpisodeTracker {
    /// No episode yet.
    pub fn new() -> Self {
        Self {
            slots: [Slot::default(); TRIGGER_IDS.len()],
        }
    }

    /// The slot of `trigger`.
    ///
    /// # Panics
    ///
    /// If `trigger` is not in [`TRIGGER_IDS`]; a test pins that every
    /// trigger id of the catalog is.
    fn slot(&mut self, trigger: FeatureKey) -> &mut Slot {
        let position = TRIGGER_IDS
            .iter()
            .position(|(key, _)| *key == trigger)
            .expect("every trigger id has an episode slot");
        &mut self.slots[position]
    }

    /// Assigns `trigger`, emitted by the event `event`, to an episode of its
    /// id (the join rule, module docs). `observed_from` is the time of the
    /// first event the caller observed: an episode whose opener reaches
    /// before it is truncated.
    pub fn assign(
        &mut self,
        trigger: &TriggerEvent,
        event: CanonicalKey,
        observed_from: EventTime,
    ) -> Assignment {
        let span = evidence_span(trigger);
        let slot = self.slot(trigger.feature);
        if let Some(open) = &mut slot.open
            && span.first <= open.last
        {
            open.last = open.last.max(span.last);
            return Assignment {
                episode: Episode {
                    id: open.id,
                    opener: false,
                    truncated: open.truncated,
                },
                span,
                bridge: slot.closed_last.is_some_and(|last| span.first <= last),
            };
        }
        let ordinal = match slot.opened {
            Some((at, opened)) if at == event => opened,
            _ => 0,
        };
        slot.opened = Some((event, ordinal + 1));
        slot.closed_last = slot.open.map(|open| open.last);
        let id = EpisodeId {
            trigger: trigger.feature,
            opened_at: event,
            ordinal,
        };
        let truncated = span.first < observed_from;
        slot.open = Some(Open {
            id,
            last: span.last,
            truncated,
            emitted: false,
        });
        Assignment {
            episode: Episode {
                id,
                opener: true,
                truncated,
            },
            span,
            bridge: false,
        }
    }

    /// Whether a sample of `trigger`'s open episode is emitted for the first
    /// time; marks it emitted.
    pub(crate) fn first_emission(&mut self, trigger: FeatureKey) -> bool {
        match &mut self.slot(trigger).open {
            Some(open) if !open.emitted => {
                open.emitted = true;
                true
            }
            _ => false,
        }
    }
}
