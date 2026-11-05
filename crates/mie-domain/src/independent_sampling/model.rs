//! An independent model of ADR-054 D1–D4, written from the ADR text and the
//! definitions it cites, without the sampler's source (#146, protocol in the
//! module root).
//!
//! Every rule below quotes the ADR sentence it implements. Where the text
//! leaves a choice, the model takes one reading, marked `ADR reading:`, and
//! the record-by-record comparison decides.

use crate::bars::Timeframe;
use crate::candidate::{CandidateEvent, ContextLevel, ContextLevels, LocationContext};
use crate::conditional::{ContextRecord, Episode, EpisodeId, Sample, StateSnapshot};
use crate::event::MarketEvent;
use crate::feature::{FeatureKey, catalog};
use crate::location::{AuctionStatus, LocationLevel};
use crate::num::Price;
use crate::order::CanonicalKey;
use crate::state::{MarketState, MarketStateEngine};
use crate::structure::Sweep;
use crate::time::EventTime;
use crate::trigger::{Bias, OiRejection, TriggerEvent, TriggerEvidence};
use std::collections::BTreeSet;

/// ADR-044 D3: "Location tolerance `w` = 6 bps of the level price. Exact:
/// `|p − L| · 10 000 ≤ |L| · 6`, in `i128`."
pub(super) const PADDING_BPS: i128 = 6;

/// ADR-054 D4, auction ids: "the deciding 1m bar: `bar_end − 60 000 ms`".
const DECIDING_MINUTE_MS: i64 = 60_000;

/// An evidence span `[first, last]`, both inclusive (ADR-054 D4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Span {
    pub(super) first: EventTime,
    pub(super) last: EventTime,
}

impl Span {
    /// D4 join rule: "its span overlaps the episode's data:
    /// `first ≤ open.last` and `open.first ≤ last`".
    pub(super) fn meets(self, other: Self) -> bool {
        self.first <= other.last && other.first <= self.last
    }

    /// D4: "The episode's data becomes the hull of both."
    pub(super) fn hull(self, other: Self) -> Self {
        Self {
            first: self.first.min(other.first),
            last: self.last.max(other.last),
        }
    }
}

fn ms(time: EventTime, delta: i64) -> EventTime {
    EventTime::from_millis(time.as_millis() + delta)
}

/// The span rule of one trigger id, by the id's family (the D4 table).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Rule {
    /// "auction ids (failed auction, failed reclaim, breakout + acceptance)".
    Auction,
    /// "`trigger.sfp.<tf>@1`".
    Sfp,
    /// "`trigger.cvd_divergence.<tf>@1`".
    Divergence,
    /// "`trigger.effort_result.<w>@1`".
    Effort,
    /// "`trigger.absorption.<w>@1` (via `Absorption.effort`)".
    Absorption,
    /// "`trigger.exhaustion@1`".
    Exhaustion,
    /// "`trigger.oi_expansion_rejection.<tf>@1` and `.prior_day@1`".
    Oi,
}

fn rule_of(id: &str) -> Option<Rule> {
    let rule = match id {
        "trigger.failed_auction" | "trigger.failed_reclaim" | "trigger.breakout_acceptance" => {
            Rule::Auction
        }
        "trigger.exhaustion" => Rule::Exhaustion,
        _ if id.starts_with("trigger.sfp.") => Rule::Sfp,
        _ if id.starts_with("trigger.cvd_divergence.") => Rule::Divergence,
        _ if id.starts_with("trigger.effort_result.") => Rule::Effort,
        _ if id.starts_with("trigger.absorption.") => Rule::Absorption,
        _ if id.starts_with("trigger.oi_expansion_rejection.") => Rule::Oi,
        _ => return None,
    };
    Some(rule)
}

/// The trigger ids, in catalog order: ADR-054 D6 "fixed per-id episode
/// slots in catalog order". Read from `catalog::CURRENT`, every key whose id
/// is a trigger id of the D4 table.
pub(super) fn trigger_ids() -> Vec<FeatureKey> {
    catalog::CURRENT
        .iter()
        .copied()
        .filter(|key| rule_of(key.id.as_str()).is_some())
        .collect()
}

/// D4 auction row: `[bar_end − 60 000 ms, bar_end − 1 ms]`.
fn auction_span(status: &AuctionStatus) -> Span {
    Span {
        first: ms(status.bar_end, -DECIDING_MINUTE_MS),
        last: ms(status.bar_end, -1),
    }
}

/// D4 SFP row: `[sweep.time, resolved_bar_end − 1 ms]`, "(`known_at` if
/// unresolved)".
///
/// ADR reading: "`known_at` if unresolved" names the `last` instant itself,
/// with no `− 1 ms` ("the event that emitted the fact"). D4 also states the
/// sweep is always resolved when its trigger fires, so the comparison
/// cannot exercise this arm.
fn sweep_span(sweep: &Sweep) -> Span {
    Span {
        first: sweep.time,
        last: match sweep.resolved_bar_end {
            Some(end) => ms(end, -1),
            None => sweep.known_at,
        },
    }
}

/// The D4 table, read from the trigger's own evidence fields. Panics when
/// the evidence does not have the shape the id's row names: the model then
/// does not know the id.
pub(super) fn span_of(trigger: &TriggerEvent) -> Span {
    let rule = rule_of(trigger.feature.id.as_str())
        .unwrap_or_else(|| panic!("{} is not a D4 trigger id", trigger.feature));
    let shape = || -> ! { panic!("{}: evidence {:?}", trigger.feature, trigger.evidence) };
    match (rule, &trigger.evidence) {
        (Rule::Auction, TriggerEvidence::Auction(status)) => auction_span(status),
        (Rule::Sfp, TriggerEvidence::Sfp(sweep)) => sweep_span(sweep),
        (Rule::Divergence, TriggerEvidence::Candidate(candidate)) => match candidate.as_ref() {
            // "`pair.previous.time`" … "`pair.current.known_at − 1 ms`".
            CandidateEvent::CvdDivergence(event) => Span {
                first: event.comparison.pair.previous.time,
                last: ms(event.comparison.pair.current.known_at, -1),
            },
            _ => shape(),
        },
        (Rule::Effort, TriggerEvidence::Candidate(candidate)) => match candidate.as_ref() {
            // "`end − w` (`w` = the window's `timeframe`)" … "`end − 1 ms`".
            CandidateEvent::EffortVsResult(event) => Span {
                first: ms(event.value.end, -event.value.timeframe.millis()),
                last: ms(event.value.end, -1),
            },
            _ => shape(),
        },
        (Rule::Absorption, TriggerEvidence::Candidate(candidate)) => match candidate.as_ref() {
            // The effort row "via `Absorption.effort`".
            CandidateEvent::Absorption(event) => {
                let effort = &event.value.effort;
                Span {
                    first: ms(effort.end, -effort.timeframe.millis()),
                    last: ms(effort.end, -1),
                }
            }
            _ => shape(),
        },
        (Rule::Exhaustion, TriggerEvidence::Candidate(candidate)) => match candidate.as_ref() {
            // "`expansion.end − expansion.timeframe`" … "`end − 1 ms`".
            CandidateEvent::Exhaustion(event) => {
                let expansion = &event.value.expansion;
                Span {
                    first: ms(expansion.end, -expansion.timeframe.millis()),
                    last: ms(event.value.end, -1),
                }
            }
            _ => shape(),
        },
        (Rule::Oi, TriggerEvidence::OiExpansion(expansion)) => {
            // "the earlier of the rejection's `first` and `start.boundary`",
            // "the later of the rejection's `last` and `end.boundary`". The
            // rejection's own span is its row: an SFP or a failed auction.
            let rejection = match &expansion.rejection {
                OiRejection::Sfp(sweep) => sweep_span(sweep),
                OiRejection::FailedAuction(status) => auction_span(status),
            };
            Span {
                first: rejection.first.min(expansion.start.boundary),
                last: rejection.last.max(expansion.end.boundary),
            }
        }
        _ => shape(),
    }
}

/// The open episode of one id.
#[derive(Debug, Clone, Copy)]
struct Open {
    id: EpisodeId,
    data: Span,
    truncated: bool,
}

/// The episode stream of one trigger id (D4: "One episode stream per
/// trigger id").
#[derive(Debug, Clone)]
struct Stream {
    trigger: FeatureKey,
    open: Option<Open>,
    /// The hull of the data of the id's closed episodes.
    closed: Option<Span>,
    /// The span of the id's previous trigger, in emission order.
    previous: Option<Span>,
    /// The event at which the id last opened episodes, and how many.
    opened_at: Option<(CanonicalKey, u32)>,
}

/// What the model knows about one trigger's assignment: the episode, the
/// span and the id's state before the assignment, so the harness can
/// classify edge cases from the model's view.
#[derive(Debug, Clone, Copy)]
pub(super) struct Assigned {
    pub(super) episode: Episode,
    pub(super) span: Span,
    /// The id's open episode data before this trigger.
    pub(super) open_before: Option<Span>,
    /// The hull of the id's closed episodes before this trigger.
    pub(super) closed_before: Option<Span>,
    /// The id's previous span in emission order.
    pub(super) previous: Option<Span>,
    pub(super) observed_from: EventTime,
}

/// The D4 episode rule on its own, per id.
#[derive(Debug, Clone)]
pub(super) struct Episodes {
    streams: Vec<Stream>,
}

impl Episodes {
    pub(super) fn new() -> Self {
        Self {
            streams: trigger_ids()
                .into_iter()
                .map(|trigger| Stream {
                    trigger,
                    open: None,
                    closed: None,
                    previous: None,
                    opened_at: None,
                })
                .collect(),
        }
    }

    /// Assigns `trigger`, emitted by the event `key`, to its id's episode.
    /// `observed_from` is the time of the first event the sampler observed.
    pub(super) fn assign(
        &mut self,
        trigger: &TriggerEvent,
        key: CanonicalKey,
        observed_from: EventTime,
    ) -> Assigned {
        let span = span_of(trigger);
        let stream = self
            .streams
            .iter_mut()
            .find(|stream| stream.trigger == trigger.feature)
            .unwrap_or_else(|| panic!("{} has no episode stream", trigger.feature));
        let open_before = stream.open.map(|open| open.data);
        let closed_before = stream.closed;
        let previous = stream.previous.replace(span);
        // D4 Bridges: "a trigger whose span overlaps the hull of the data of
        // its id's closed episodes may share data with one of them".
        //
        // ADR reading: "its id's closed episodes" are the episodes closed when
        // the trigger arrives. An opener closes the open episode, whose data
        // it does not overlap by the join rule, so that episode cannot share
        // data with it ("may share data with one of them"); it is not counted.
        let bridge = closed_before.is_some_and(|closed| span.meets(closed));
        let episode = match stream.open.as_mut() {
            // "a trigger joins its id's open episode when its span overlaps
            // the episode's data".
            Some(open) if span.meets(open.data) => {
                open.data = open.data.hull(span);
                Episode {
                    id: open.id,
                    opener: false,
                    truncated: open.truncated,
                    bridge,
                }
            }
            // "Otherwise the trigger opens a new episode, and the open one
            // closes."
            _ => {
                if let Some(open) = stream.open.take() {
                    stream.closed = Some(match stream.closed {
                        Some(closed) => closed.hull(open.data),
                        None => open.data,
                    });
                }
                // Identity: "how many episodes the id had opened at that event
                // before it".
                let ordinal = match stream.opened_at {
                    Some((at, count)) if at == key => count,
                    _ => 0,
                };
                stream.opened_at = Some((key, ordinal + 1));
                let id = EpisodeId {
                    trigger: trigger.feature,
                    opened_at: key,
                    ordinal,
                };
                // Truncated: "the opener's `first` is earlier than the first
                // event the sampler observed".
                let truncated = span.first < observed_from;
                stream.open = Some(Open {
                    id,
                    data: span,
                    truncated,
                });
                Episode {
                    id,
                    opener: true,
                    truncated,
                    bridge,
                }
            }
        };
        Assigned {
            episode,
            span,
            open_before,
            closed_before,
            previous,
            observed_from,
        }
    }
}

/// D2: each snapshot value "is copied from the committed state with its
/// `FeatureValue` validity", by the D2 table, in field order; the arrays
/// list 5m, 15m, 1h.
pub(super) fn snapshot(state: &MarketState) -> StateSnapshot {
    const WINDOWS: [Timeframe; 3] = [Timeframe::M5, Timeframe::M15, Timeframe::H1];
    StateSnapshot {
        // `trade.last_price@1`.
        last_price: state.last_trade_price,
        // `volatility.atr.1h@1`.
        atr: state.atr,
        // `flow.window.<5m|15m|1h>@1`.
        aggression: WINDOWS.map(|tf| *state.flow.windows.get(tf).expect("aggression window")),
        // `flow.cvd.utc_day@1`.
        cvd_utc_day: state.flow.cvd_utc_day,
        // `candidate.effort_result.<5m|15m|1h>@1`.
        effort: WINDOWS.map(|tf| *state.candidates.effort_result(tf).expect("effort window")),
        // `book.liquidity.window.<5m|15m|1h>@1`.
        liquidity: WINDOWS.map(|tf| *state.book.windows.get(tf).expect("liquidity window")),
        // `derivatives.oi.5m@1`.
        oi_5m: state.derivatives.oi_5m,
    }
}

/// Whether the zone of `level`, padded by `w` = 6 bps of the level's price
/// (ADR-044 D3), contains the point `price`: ADR-051 D3, "every registry
/// level whose zone, padded by the registry's own tolerance, contains the
/// anchor price", zones closed `[low, high]` (ADR-044 D5). Outside the zone
/// the gap to its nearer edge must satisfy `gap · 10 000 ≤ |L| · 6`.
pub(super) fn padded_contains(level: &LocationLevel, price: Price) -> bool {
    let p = i128::from(price.units());
    let low = i128::from(level.low.units());
    let high = i128::from(level.high.units());
    let gap = if p < low {
        low - p
    } else if p > high {
        p - high
    } else {
        0
    };
    gap * 10_000 <= i128::from(level.price.units()).abs() * PADDING_BPS
}

/// D1: "A baseline record reads the context with" the ADR-051 D3 read at
/// the anchor with no exclusion: the anchor, the registry levels whose
/// padded zone contains it in registry order, and a copy of the auction
/// state. Re-derived here from those definitions, without calling the
/// crate's reader (#146, decision 6).
pub(super) fn baseline_context(anchor: Price, state: &MarketState) -> LocationContext {
    LocationContext {
        anchor,
        levels: state.location.levels.clone().map(|set| ContextLevels {
            feature: set.feature,
            levels: set
                .levels()
                .iter()
                .filter(|level| padded_contains(level, anchor))
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
        auction: state.location.auction,
    }
}

/// Per-id counts over emitted records: `samples`, distinct `episodes` and
/// `truncated` episodes with at least one emitted sample, and emitted
/// `bridges` samples (ADR-054 D4, D6).
#[derive(Debug, Clone, Default)]
pub(super) struct IdCounts {
    pub(super) samples: u64,
    pub(super) episodes: BTreeSet<EpisodeId>,
    pub(super) truncated: BTreeSet<EpisodeId>,
    pub(super) bridges: u64,
}

/// The model's counts over its emitted records.
#[derive(Debug, Clone)]
pub(super) struct Counts {
    pub(super) records: u64,
    pub(super) per_id: Vec<(FeatureKey, IdCounts)>,
}

/// What one observed event produced in the model.
#[derive(Debug, Clone)]
pub(super) struct Observation {
    /// The emitted records, in D1 order.
    pub(super) records: Vec<ContextRecord>,
    /// Every trigger's assignment, in emission order, emitted or not.
    pub(super) assigned: Vec<Assigned>,
    /// 1m bars the event closed, and how many of them had trades.
    pub(super) closed_minutes: usize,
    pub(super) closed_with_trades: usize,
    /// The baseline anchor price, when the event took a baseline sample.
    pub(super) anchor: Option<Price>,
    /// Whether the event's records were emitted (`emit_from`).
    pub(super) emitted: bool,
}

/// The model sampler.
#[derive(Debug, Clone)]
pub(super) struct Model {
    emit_from: Option<EventTime>,
    observed_from: Option<EventTime>,
    /// D1: "the close of the last closed 1m bar with trades", with that
    /// bar's end.
    anchor: Option<(Price, EventTime)>,
    episodes: Episodes,
    counts: Counts,
}

impl Model {
    /// D6: `new(emit_from)` "lets a caller drive a warm prefix through the
    /// engine and the episode tracker while emitting records only from the
    /// window start."
    pub(super) fn new(emit_from: Option<EventTime>) -> Self {
        Self {
            emit_from,
            observed_from: None,
            anchor: None,
            episodes: Episodes::new(),
            counts: Counts {
                records: 0,
                per_id: trigger_ids()
                    .into_iter()
                    .map(|key| (key, IdCounts::default()))
                    .collect(),
            },
        }
    }

    pub(super) fn counts(&self) -> &Counts {
        &self.counts
    }

    /// The records of one accepted event, read from the engine's committed
    /// state, `closed_bars()` and `trigger_events()` (D6).
    pub(super) fn observe(
        &mut self,
        event: &MarketEvent,
        engine: &MarketStateEngine,
    ) -> Observation {
        let key = event.canonical_key();
        let observed_from = *self.observed_from.get_or_insert(key.time);
        // ADR reading: "emits the records of events at or after `emit_from`"
        // compares the event's time.
        let emitted = self.emit_from.is_none_or(|from| key.time >= from);
        let state = engine.state();
        let snapshot = snapshot(state);

        // D3: "every accepted event that closes at least one 1m bar …, once
        // per event even when a trades gap closes many minutes at once."
        let mut closed_minutes = 0;
        let mut closed_with_trades = 0;
        for bar in engine.closed_bars() {
            if bar.timeframe != Timeframe::M1 {
                continue;
            }
            closed_minutes += 1;
            if let Some(ohlc) = bar.ohlc {
                closed_with_trades += 1;
                // "An event that closes only empty minutes keeps the anchor and
                // `minute_end` of the last minute with trades."
                let end = bar.end();
                if self.anchor.is_none_or(|(_, last)| end > last) {
                    self.anchor = Some((ohlc.close, end));
                }
            }
        }

        let mut records = Vec::new();
        let mut anchor = None;
        // "Sampling starts with the first closed 1m bar with trades, so that
        // an anchor exists."
        if closed_minutes > 0
            && let Some((price, minute_end)) = self.anchor
        {
            anchor = Some(price);
            // D1: "the baseline sample first".
            records.push(ContextRecord {
                event: key,
                sample: Sample::Baseline { minute_end },
                context: baseline_context(price, state),
                bias: Bias::read(state),
                state: snapshot,
            });
        }

        let mut assigned = Vec::new();
        // D1: "then one trigger sample per trigger event in emission order".
        for trigger in engine.trigger_events() {
            let assignment = self.episodes.assign(trigger, key, observed_from);
            assigned.push(assignment);
            // "A trigger record uses the trigger event's own values, never a
            // re-read."
            records.push(ContextRecord {
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

        if !emitted {
            records.clear();
        }
        self.count(&records);
        Observation {
            records,
            assigned,
            closed_minutes,
            closed_with_trades,
            anchor,
            emitted,
        }
    }

    fn count(&mut self, records: &[ContextRecord]) {
        for record in records {
            self.counts.records += 1;
            if let Sample::Trigger { trigger, episode } = &record.sample {
                let (_, counts) = self
                    .counts
                    .per_id
                    .iter_mut()
                    .find(|(key, _)| *key == trigger.feature)
                    .expect("a trigger id");
                counts.samples += 1;
                counts.episodes.insert(episode.id);
                if episode.truncated {
                    counts.truncated.insert(episode.id);
                }
                if episode.bridge {
                    counts.bridges += 1;
                }
            }
        }
    }
}
