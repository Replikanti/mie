//! Forward outcome labels of context records (ADR-055, proposed; #26).
//!
//! A label says what the trade path did after a record's decision event,
//! and nothing else: it reads the trades strictly after the record's
//! [`CanonicalKey`] (ADR-028) and two values the record holds at decision
//! time, `trade.last_price@1` (the **reference**) and `volatility.atr.1h@1`
//! (ADR-054 D2). It never sees the engine, and no engine, catalog or
//! sampler code reads a label (D11, enforced by `tools/check-architecture.sh`).
//!
//! - **One label per record** (D1), keyed `(event, ordinal)`, emitted in
//!   record order to a sink separate from the records.
//! - **Windows** (D2): horizon H reads the trades with a key greater than the
//!   record's event and a time before `event.time + H`, for H in
//!   [`HORIZONS`] (D3).
//! - **Races** (D4, D5): one symmetric barrier pair per horizon, `reference
//!   ± b_H · ATR` with `b_H` = [`BARRIER_MULTIPLE_E5`] per 10⁵. A trade
//!   touches the upper barrier at or above it and the lower one at or below
//!   it; the first touch in canonical order wins, and `same_ms` says the
//!   other barrier was touched later in the same millisecond.
//! - **Excursions** (D7): the highest and lowest trade beyond the
//!   reference, with the time of the earliest trade at each, and the last
//!   trade of the window.
//! - **Passages** (D8): for a trigger record, the first passage within 1d
//!   to the evidence's `level` and `extreme` ([`evidence_prices`]).
//! - **Censoring** (D6): a trades gap, a stale reference, a rejected
//!   trades-stream event and the end of data censor what is not resolved
//!   strictly before the censor time; a censored label is never dropped.
//!   Until a label is emitted, a censor also reaches its closed windows: a
//!   trades outage is silent until trades resume (ADR-028). Emitted labels
//!   are immutable: a later gap that reaches back into one is counted in
//!   [`LabelStats::reach_backs`].
//! - **Thinning** (D9): a `clear` flag per horizon picks records whose
//!   windows are disjoint, read from decision times and episode flags only.
//!
//! [`ForwardLabeler`] is pure: no clock, no hash-ordered collection.
//! Window extremes come from one monotonic deque each for the maximum and
//! the minimum per horizon, pending touches from ordered sets keyed by
//! price, so a trade costs amortized O(1) per horizon plus O(log n) per
//! touch. It holds about one day of records and at most one day of trades.
//!
//! No type, field or `Display` text names a direction or an execution
//! (ADR-010, ADR-054 D1): the barriers are upper and lower, the excursions
//! up and down. Which side counts as favorable belongs to the hypothesis
//! (#28).

use super::{ContextProvenance, ContextRecord, Sample, TRIGGER_IDS, trigger_position};
use crate::bars::Timeframe;
use crate::candidate::CandidateEvent;
use crate::event::{MarketEvent, Stream};
use crate::feature::catalog;
use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::location::{AuctionStatus, Region};
use crate::num::Price;
use crate::order::CanonicalKey;
use crate::research::PipelineKey;
use crate::structure::Sweep;
use crate::time::EventTime;
use crate::trigger::{OiRejection, TriggerEvent, TriggerEvidence};
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::fmt;

#[cfg(test)]
mod tests;

/// The labeling (ADR-055 D10): reference, window, horizons, barriers, tie
/// rule, censoring, excursions, passages and thinning. Any change to them
/// is a new version.
pub const LABELING_V1: PipelineKey = PipelineKey::new("conditional.labels", 1);

/// The horizons (ADR-055 D3): the bar timeframes trigger evidence is built
/// on (ADR-053), widest last.
pub const HORIZONS: [Timeframe; 5] = [
    Timeframe::M5,
    Timeframe::M15,
    Timeframe::H1,
    Timeframe::H4,
    Timeframe::D1,
];

/// The barrier multiple `b_H = √(π · H / 32 h)` of each horizon in
/// [`HORIZONS`], per 10⁵ of the ATR (ADR-055 D4): a driftless Brownian
/// null leaves the band `± b_H · ATR` after H / 4 on average.
pub const BARRIER_MULTIPLE_E5: [i64; 5] = [9_045, 15_666, 31_333, 62_666, 153_499];

/// The denominator of [`BARRIER_MULTIPLE_E5`].
const MULTIPLE_SCALE: i64 = 100_000;

/// The position in [`HORIZONS`] of the window passages are read in: 1d.
const PASSAGE_HORIZON: usize = 4;

/// Pending-touch targets beyond the race of a horizon (`0..5`).
const LEVEL: u8 = 5;
/// See [`LEVEL`].
const EXTREME: u8 = 6;

/// The canonical parameter text of [`LABELING_V1`], which
/// [`labeling_fingerprint`] hashes: one line per decision of ADR-055 D2–D9.
/// Every line ends in `\n`.
pub fn labeling_text() -> String {
    let horizons: Vec<String> = HORIZONS.iter().map(ToString::to_string).collect();
    let multiples: Vec<String> = BARRIER_MULTIPLE_E5
        .iter()
        .map(ToString::to_string)
        .collect();
    let mut text = String::from("mie-labeling 1\n");
    text.push_str(&format!("labeling {LABELING_V1}\n"));
    text.push_str(&format!(
        "reference {} path=trades window=after_decision_key_before_decision_time_plus_h\n",
        catalog::TRADE_LAST_PRICE_V1.key
    ));
    text.push_str(&format!("horizons {}\n", horizons.join(",")));
    text.push_str(&format!(
        "barriers atr={} multiple_e5={} scale={MULTIPLE_SCALE} offset=round_half_even unavailable=atr_not_ready,zero_offset\n",
        catalog::VOLATILITY_ATR_1H_V1.key,
        multiples.join(",")
    ));
    text.push_str("touch upper=price_at_or_above lower=price_at_or_below tie=earlier_canonical_key same_ms=other_barrier_later_in_the_millisecond\n");
    text.push_str("excursion up=max0_high_minus_reference down=max0_reference_minus_low at=earliest_trade_at_the_extreme last=last_trade\n");
    text.push_str("censor gap=trades_feed_gap_at_max_start_decision stale_reference=trades_gap_since_last_trade_or_no_trade rejected=trades_stream_event_at_max_time_decision end_of_data=last_event keep=resolved_strictly_before until=emitted closed_windows=too earliest=wins late_gap=immutable_reach_back\n");
    text.push_str(&format!(
        "passage window={} side=fixed_at_decision equal=at_decision\n",
        HORIZONS[PASSAGE_HORIZON]
    ));
    text.push_str("passage sfp level=sweep.level.price extreme=sweep.extreme\n");
    text.push_str("passage auction level=vah_if_side_above,val_if_side_below extreme=none\n");
    text.push_str("passage cvd_divergence,oi_divergence level=pair.current.price extreme=pair.current.price\n");
    text.push_str("passage exhaustion level=none extreme=value.extreme\n");
    text.push_str("passage effort_vs_result,absorption level=none extreme=none\n");
    text.push_str("passage oi_expansion_rejection as_rejection\n");
    text.push_str(
        "thinning baseline=greedy_per_horizon trigger=greedy_per_horizon_over_openers_per_id\n",
    );
    text
}

/// FNV-1a 64 ([`Fingerprinter::write_str`]) of [`labeling_text`].
pub fn labeling_fingerprint() -> Fingerprint {
    let mut hasher = Fingerprinter::new();
    hasher.write_str(&labeling_text());
    hasher.finish()
}

/// Where a run of labels comes from (ADR-055 D10), delivered once per run:
/// the provenance of the records and the labeling.
///
/// `Display` prints the context provenance, then `labeling=<id@N>
/// labeling_fingerprint=<16 hex>`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LabelProvenance {
    /// The provenance of the labeled records (ADR-054 D5).
    pub context: ContextProvenance,
    /// The labeling: [`LABELING_V1`].
    pub labeling: PipelineKey,
    /// [`labeling_fingerprint`].
    pub labeling_fingerprint: Fingerprint,
}

impl LabelProvenance {
    /// The provenance of labels of the records `context` describes, under
    /// [`LABELING_V1`].
    pub fn new(context: ContextProvenance) -> Self {
        Self {
            context,
            labeling: LABELING_V1,
            labeling_fingerprint: labeling_fingerprint(),
        }
    }
}

impl fmt::Display for LabelProvenance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} labeling={} labeling_fingerprint={}",
            self.context, self.labeling, self.labeling_fingerprint
        )
    }
}

/// `b_H · atr` for the horizon at `position` in [`HORIZONS`], rounded half
/// to even to the price unit (ADR-055 D4); `None` when it rounds to 0.
pub fn barrier_offset(atr: Price, position: usize) -> Option<Price> {
    let product = i128::from(atr.units()) * i128::from(BARRIER_MULTIPLE_E5[position]);
    let scale = i128::from(MULTIPLE_SCALE);
    let (quotient, remainder) = (product.div_euclid(scale), product.rem_euclid(scale));
    let rounded = match (2 * remainder).cmp(&scale) {
        Ordering::Less => quotient,
        Ordering::Greater => quotient + 1,
        Ordering::Equal => quotient + (quotient & 1),
    };
    i64::try_from(rounded)
        .ok()
        .filter(|units| *units > 0)
        .map(Price::from_units)
}

/// The two barriers of one race (ADR-055 D4).
///
/// `Display` prints `<lower>..<upper>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Barriers {
    /// `reference − offset`: touched by a trade at or below it.
    pub lower: Price,
    /// `reference + offset`: touched by a trade at or above it.
    pub upper: Price,
}

impl Barriers {
    /// The barriers around `reference` for the horizon at `position`, or
    /// `None` without a ready ATR, with an offset that rounds to 0, or if a
    /// barrier leaves the price range.
    fn around(reference: Price, atr: Option<Price>, position: usize) -> Option<Self> {
        let offset = barrier_offset(atr?, position)?.units();
        Some(Self {
            lower: Price::from_units(reference.units().checked_sub(offset)?),
            upper: Price::from_units(reference.units().checked_add(offset)?),
        })
    }
}

impl fmt::Display for Barriers {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}..{}", self.lower, self.upper)
    }
}

/// Why a label component is censored (ADR-055 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CensorReason {
    /// A `FeedGap` on the trades stream, delivered while the label was open.
    Gap,
    /// A trades gap was delivered after the last trade, at or before the
    /// decision, or no trade preceded it: the path from the reference to the
    /// decision is unknown.
    StaleReference,
    /// The engine rejected a trades-stream event while the label was open.
    Rejected,
    /// The data ended before the window did.
    EndOfData,
}

impl CensorReason {
    /// The snake_case name `Display` uses.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Gap => "gap",
            Self::StaleReference => "stale_reference",
            Self::Rejected => "rejected",
            Self::EndOfData => "end_of_data",
        }
    }
}

impl fmt::Display for CensorReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A censor (ADR-055 D6): from `at` on the path is unknown; what was
/// resolved strictly before `at` stands.
///
/// `Display` prints `<reason>@<at>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Censor {
    /// The censor time: the later of the cause's time and the decision time.
    pub at: EventTime,
    /// The cause.
    pub reason: CensorReason,
}

impl fmt::Display for Censor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.reason, self.at)
    }
}

/// The barrier race of one horizon (ADR-055 D4, D5).
///
/// `Display` prints `upper@<at>`, `lower@<at>` (each with `/same_ms` when
/// set), `neither`, `censored(<censor>)` or `unavailable`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Race {
    /// The upper barrier was touched first, by a trade at `at`.
    Upper {
        /// Time of the touching trade.
        at: EventTime,
        /// The lower barrier was also touched later in that millisecond.
        same_ms: bool,
    },
    /// The lower barrier was touched first, by a trade at `at`.
    Lower {
        /// Time of the touching trade.
        at: EventTime,
        /// The upper barrier was also touched later in that millisecond.
        same_ms: bool,
    },
    /// The window closed with full coverage and neither barrier touched.
    Neither,
    /// Neither barrier was touched strictly before the censor.
    Censored(Censor),
    /// No race: the ATR was not ready at decision, or the offset rounds
    /// to 0.
    Unavailable,
}

impl fmt::Display for Race {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let flag = |same_ms: bool| if same_ms { "/same_ms" } else { "" };
        match self {
            Self::Upper { at, same_ms } => write!(f, "upper@{at}{}", flag(*same_ms)),
            Self::Lower { at, same_ms } => write!(f, "lower@{at}{}", flag(*same_ms)),
            Self::Neither => f.write_str("neither"),
            Self::Censored(censor) => write!(f, "censored({censor})"),
            Self::Unavailable => f.write_str("unavailable"),
        }
    }
}

/// How far the window's trades went beyond the reference on one side
/// (ADR-055 D7).
///
/// `Display` prints `<size>@<at>`, or `<size>` without a trade beyond the
/// reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Excursion {
    /// `max(0, highest − reference)` up, `max(0, reference − lowest)` down.
    pub size: Price,
    /// Time of the earliest trade at that extreme; `None` when `size` is 0.
    pub at: Option<EventTime>,
}

impl Excursion {
    /// No trade beyond the reference.
    pub const NONE: Self = Self {
        size: Price::from_units(0),
        at: None,
    };
}

impl fmt::Display for Excursion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.at {
            Some(at) => write!(f, "{}@{at}", self.size),
            None => write!(f, "{}", self.size),
        }
    }
}

/// The label of one horizon (ADR-055 D4–D7, D9).
///
/// `Display` prints `[<horizon>`, ` clear` when set, the barriers (`-`
/// without), the race, the excursions, the last trade (`-` without), and
/// ` censored=<censor>` when the window is censored, then `]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HorizonLabel {
    /// The horizon.
    pub horizon: Timeframe,
    /// The record is clear at this horizon (D9): its window is disjoint
    /// from every earlier clear record's of its stream.
    pub clear: bool,
    /// The race's barriers; `None` when the race is unavailable.
    pub barriers: Option<Barriers>,
    /// The race.
    pub race: Race,
    /// The excursion above the reference.
    pub up: Excursion,
    /// The excursion below the reference.
    pub down: Excursion,
    /// The price of the window's last trade; `None` without a trade.
    pub last: Option<Price>,
    /// The window is censored: the excursions and `last` cover the trades
    /// strictly before the censor time only, so the excursions are lower
    /// bounds.
    pub censored: Option<Censor>,
}

impl fmt::Display for HorizonLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}", self.horizon)?;
        if self.clear {
            f.write_str(" clear")?;
        }
        match self.barriers {
            Some(barriers) => write!(f, " barriers={barriers}")?,
            None => f.write_str(" barriers=-")?,
        }
        write!(f, " race={} up={} down={}", self.race, self.up, self.down)?;
        match self.last {
            Some(last) => write!(f, " last={last}")?,
            None => f.write_str(" last=-")?,
        }
        if let Some(censor) = self.censored {
            write!(f, " censored={censor}")?;
        }
        f.write_str("]")
    }
}

/// The first passage to one evidence price within the 1d window (ADR-055
/// D8). Its side is fixed at decision: a price above the reference is
/// reached by a trade at or above it, one below by a trade at or below it.
///
/// `Display` prints `at_decision`, `touched@<at>`, `not_within` or
/// `censored(<censor>)`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Passage {
    /// The price equals the reference.
    AtDecision,
    /// First reached by a trade at `at`.
    Touched {
        /// Time of the trade.
        at: EventTime,
    },
    /// The 1d window closed with full coverage before it was reached.
    NotWithin,
    /// Not reached strictly before the censor.
    Censored(Censor),
}

impl fmt::Display for Passage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AtDecision => f.write_str("at_decision"),
            Self::Touched { at } => write!(f, "touched@{at}"),
            Self::NotWithin => f.write_str("not_within"),
            Self::Censored(censor) => write!(f, "censored({censor})"),
        }
    }
}

/// One evidence price and its passage (ADR-055 D8).
///
/// `Display` prints `<price>:<passage>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PassageLabel {
    /// The price, read from the trigger's evidence.
    pub price: Price,
    /// Its first passage.
    pub passage: Passage,
}

impl fmt::Display for PassageLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.price, self.passage)
    }
}

/// The forward outcome label of one context record (ADR-055 D1).
///
/// `Display` prints the event key, `#<ordinal>`, `ref=` and `atr=` (`-`
/// when missing), one [`HorizonLabel`] per horizon, then `level=` and
/// `extreme=` as [`PassageLabel`]s (`-` when missing).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardLabel {
    /// The labeled record's event: the decision event.
    pub event: CanonicalKey,
    /// The record's position among the records of its event, in ADR-054
    /// D1 order (the baseline sample first, then the triggers in emission
    /// order): with `event`, the join key to the record.
    pub ordinal: u32,
    /// The record's `trade.last_price@1`; `None` before the first trade.
    pub reference: Option<Price>,
    /// The record's `volatility.atr.1h@1`; `None` while not ready.
    pub atr: Option<Price>,
    /// One label per horizon, in [`HORIZONS`] order.
    pub horizons: [HorizonLabel; 5],
    /// The passage to the evidence's level; `None` for a baseline sample
    /// and for evidence without one.
    pub level: Option<PassageLabel>,
    /// The passage to the evidence's extreme; as for `level`.
    pub extreme: Option<PassageLabel>,
}

impl fmt::Display for ForwardLabel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} #{}", self.event, self.ordinal)?;
        let mut price = |name: &str, price: Option<Price>| match price {
            Some(price) => write!(f, " {name}={price}"),
            None => write!(f, " {name}=-"),
        };
        price("ref", self.reference)?;
        price("atr", self.atr)?;
        for horizon in &self.horizons {
            write!(f, " {horizon}")?;
        }
        for (name, passage) in [("level", &self.level), ("extreme", &self.extreme)] {
            match passage {
                Some(passage) => write!(f, " {name}={passage}")?,
                None => write!(f, " {name}=-")?,
            }
        }
        Ok(())
    }
}

/// The evidence prices of a trigger (ADR-055 D8), known at decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct EvidencePrices {
    /// The level the evidence is about.
    pub level: Option<Price>,
    /// The farthest price the evidence reached.
    pub extreme: Option<Price>,
}

/// The prices of an SFP: the swept level's price and the sweep's extreme.
fn sweep_prices(sweep: &Sweep) -> EvidencePrices {
    EvidencePrices {
        level: Some(sweep.level.price),
        extreme: Some(sweep.extreme),
    }
}

/// The prices of an auction status: the value edge on its side, no
/// extreme.
fn auction_prices(status: &AuctionStatus) -> EvidencePrices {
    EvidencePrices {
        level: match status.side {
            Some(Region::Above) => Some(status.vah),
            Some(Region::Below) => Some(status.val),
            Some(Region::In) | None => None,
        },
        extreme: None,
    }
}

/// The evidence prices of `trigger` (ADR-055 D8): one arm per evidence
/// variant, each reading a field of the trigger's own evidence.
pub fn evidence_prices(trigger: &TriggerEvent) -> EvidencePrices {
    match &trigger.evidence {
        TriggerEvidence::Sfp(sweep) => sweep_prices(sweep),
        TriggerEvidence::Auction(status) => auction_prices(status),
        TriggerEvidence::Candidate(fact) => match fact.as_ref() {
            CandidateEvent::CvdDivergence(event) => {
                let price = event.comparison.pair.current.price;
                EvidencePrices {
                    level: Some(price),
                    extreme: Some(price),
                }
            }
            // No trigger family is built on an OI divergence (ADR-053 D7);
            // the arm keeps the function total, with the CVD rule.
            CandidateEvent::OiDivergence(event) => {
                let price = event.comparison.pair.current.price;
                EvidencePrices {
                    level: Some(price),
                    extreme: Some(price),
                }
            }
            CandidateEvent::Exhaustion(event) => EvidencePrices {
                level: None,
                extreme: Some(event.value.extreme),
            },
            CandidateEvent::EffortVsResult(_) | CandidateEvent::Absorption(_) => {
                EvidencePrices::default()
            }
        },
        TriggerEvidence::OiExpansion(expansion) => match &expansion.rejection {
            OiRejection::Sfp(sweep) => sweep_prices(sweep),
            OiRejection::FailedAuction(status) => auction_prices(status),
        },
    }
}

/// Counts over the labeler's run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LabelStats {
    /// Records opened.
    pub opened: u64,
    /// Labels emitted.
    pub emitted: u64,
    /// Trades gaps and rejected trades-stream events whose time reaches
    /// back into the window of an already emitted label, which stays as it
    /// was (ADR-055 D6).
    pub reach_backs: u64,
}

/// One observed trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Tick {
    /// Its canonical key; its time is the trade time.
    key: CanonicalKey,
    /// Its price.
    price: Price,
}

/// The extremes and the last trade of a stretch of trades.
#[derive(Debug, Clone, Copy, Default)]
struct Stretch {
    /// The highest price and the time of its earliest trade.
    high: Option<(Price, EventTime)>,
    /// The lowest price and the time of its earliest trade.
    low: Option<(Price, EventTime)>,
    /// The last trade's price.
    last: Option<Price>,
}

impl Stretch {
    /// Adds a trade earlier than every trade added so far: the stretch is
    /// read newest first, so ties move the extreme's time earlier.
    fn add_earlier(&mut self, tick: Tick) {
        self.last.get_or_insert(tick.price);
        if self.high.is_none_or(|(price, _)| tick.price >= price) {
            self.high = Some((tick.price, tick.key.time));
        }
        if self.low.is_none_or(|(price, _)| tick.price <= price) {
            self.low = Some((tick.price, tick.key.time));
        }
    }
}

impl HorizonLabel {
    /// Sets the excursions and the last trade from `stretch`.
    fn read(&mut self, reference: Price, stretch: &Stretch) {
        self.up = match stretch.high {
            Some((high, at)) if high > reference => Excursion {
                size: Price::from_units(high.units() - reference.units()),
                at: Some(at),
            },
            _ => Excursion::NONE,
        };
        self.down = match stretch.low {
            Some((low, at)) if low < reference => Excursion {
                size: Price::from_units(reference.units() - low.units()),
                at: Some(at),
            },
            _ => Excursion::NONE,
        };
        self.last = stretch.last;
    }
}

/// A label whose windows are not all closed.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Open {
    /// The label, filled in as its components resolve. While a horizon is
    /// open its race is `Neither` until touched and its passages
    /// `NotWithin`, which is what they stay if the window closes.
    label: ForwardLabel,
    /// `event.time + H` per horizon: the exclusive window ends.
    ends: [EventTime; 5],
    /// Which horizons are still open.
    open: [bool; 5],
}

impl Open {
    /// Every window is closed: the label is final.
    fn complete(&self) -> bool {
        !self.open.contains(&true)
    }

    /// Whether a censor at `at` reaches the horizon at `position`: its
    /// window ends after `at`, open or already closed, since the label is
    /// not emitted yet, and no earlier censor already cut it (ADR-055 D6).
    fn reaches(&self, position: usize, at: EventTime) -> bool {
        at < self.ends[position]
            && self.label.horizons[position]
                .censored
                .is_none_or(|earlier| at < earlier.at)
    }

    /// Censors every horizon the censor [`reaches`](Self::reaches), with
    /// `read` the trades of the window strictly before it, and returns
    /// which open horizons it closed. A closed window is censored too: an
    /// event of another stream at or after its end does not prove its
    /// trades were delivered, since a trades outage is silent until trades
    /// resume (ADR-028). A race touched strictly before the censor keeps
    /// its result, an unavailable race stays unavailable; the passages are
    /// censored with the 1d window, unless reached strictly before it (a
    /// price at the reference counts as reached at the decision).
    fn censor(&mut self, censor: Censor, read: &Stretch) -> [bool; 5] {
        let decision = self.label.event.time;
        let mut closed = [false; 5];
        for (position, closed) in closed.iter_mut().enumerate() {
            if !self.reaches(position, censor.at) {
                continue;
            }
            let horizon = &mut self.label.horizons[position];
            if let Some(reference) = self.label.reference {
                horizon.read(reference, read);
            }
            horizon.censored = Some(censor);
            horizon.race = match horizon.race {
                race @ (Race::Upper { at, .. } | Race::Lower { at, .. }) if at < censor.at => race,
                Race::Unavailable => Race::Unavailable,
                _ => Race::Censored(censor),
            };
            if position == PASSAGE_HORIZON {
                for passage in [&mut self.label.level, &mut self.label.extreme]
                    .into_iter()
                    .flatten()
                {
                    passage.passage = match passage.passage {
                        Passage::Touched { at } if at < censor.at => passage.passage,
                        Passage::AtDecision if decision < censor.at => Passage::AtDecision,
                        _ => Passage::Censored(censor),
                    };
                }
            }
            *closed = self.open[position];
            self.open[position] = false;
        }
        closed
    }
}

/// The open labels of one horizon, as a sliding window over the trades.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Lane {
    /// The first label with this horizon open: the one whose window closes
    /// next. Windows close in record order, since decision times do not
    /// decrease.
    front: Option<u64>,
    /// Trades after the front's event with non-increasing prices: the
    /// front is the highest price since any of them, ties kept so the
    /// earliest trade at the maximum leads.
    high: VecDeque<u64>,
    /// As `high`, with non-decreasing prices.
    low: VecDeque<u64>,
}

/// A pending touch: the price, the label's sequence number and the target
/// (a horizon's race `0..5`, [`LEVEL`] or [`EXTREME`]).
type Touch = (Price, u64, u8);

/// Labels context records from the trades after their decision (ADR-055).
///
/// Drive it with every accepted event in canonical order: call
/// [`observe`](Self::observe) with the event first, then
/// [`open`](Self::open) with each record the sampler emitted at that
/// event, in emission order. Report rejected events to
/// [`reject`](Self::reject), and call [`finish`](Self::finish) at the end
/// of the data. Labels come out in record order, each once.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ForwardLabeler {
    /// Labels not yet emitted, in record order.
    labels: VecDeque<Open>,
    /// The sequence number of `labels[0]`.
    first_label: u64,
    /// The trades after the oldest unemitted label's event.
    ticks: VecDeque<Tick>,
    /// The sequence number of `ticks[0]`.
    first_tick: u64,
    /// One sliding window per horizon.
    lanes: [Lane; 5],
    /// Pending touches by a trade at or above the price.
    above: BTreeSet<Touch>,
    /// Pending touches by a trade at or below the price.
    below: BTreeSet<Touch>,
    /// A trades gap was delivered after the last trade.
    stale: bool,
    /// The time of the last observed event.
    last_event: Option<EventTime>,
    /// The event of the last opened record and its ordinal.
    ordinal: Option<(CanonicalKey, u32)>,
    /// Per horizon, the decision time of the last clear baseline record.
    clear_baseline: [Option<EventTime>; 5],
    /// Per trigger id (in [`TRIGGER_IDS`] order) and horizon, the decision
    /// time of the last clear episode opener.
    clear_trigger: [[Option<EventTime>; 5]; TRIGGER_IDS.len()],
    /// The latest window end, or censor time, of the emitted labels.
    emitted_until: Option<EventTime>,
    /// Counts.
    stats: LabelStats,
}

impl ForwardLabeler {
    /// A labeler with nothing observed.
    pub fn new() -> Self {
        Self::default()
    }

    /// Counts so far.
    pub fn stats(&self) -> LabelStats {
        self.stats
    }

    /// Opens the label of `record`, which the sampler emitted at the event
    /// just passed to [`observe`](Self::observe).
    pub fn open(&mut self, record: &ContextRecord) {
        let event = record.event;
        let ordinal = match self.ordinal {
            Some((key, ordinal)) if key == event => ordinal + 1,
            _ => 0,
        };
        self.ordinal = Some((event, ordinal));
        let reference = record.state.last_price.ready().copied();
        let atr = record.state.atr.ready().copied();
        let clear = self.thin(record);
        let prices = match &record.sample {
            Sample::Trigger { trigger, .. } => evidence_prices(trigger),
            Sample::Baseline { .. } => EvidencePrices::default(),
        };
        let horizons = std::array::from_fn(|position| {
            let barriers =
                reference.and_then(|reference| Barriers::around(reference, atr, position));
            HorizonLabel {
                horizon: HORIZONS[position],
                clear: clear[position],
                barriers,
                race: if barriers.is_some() {
                    Race::Neither
                } else {
                    Race::Unavailable
                },
                up: Excursion::NONE,
                down: Excursion::NONE,
                last: None,
                censored: None,
            }
        });
        let passage = |price: Option<Price>| {
            price.map(|price| PassageLabel {
                price,
                passage: if Some(price) == reference {
                    Passage::AtDecision
                } else {
                    Passage::NotWithin
                },
            })
        };
        let mut open = Open {
            label: ForwardLabel {
                event,
                ordinal,
                reference,
                atr,
                horizons,
                level: passage(prices.level),
                extreme: passage(prices.extreme),
            },
            ends: HORIZONS.map(|horizon| {
                EventTime::from_millis(event.time.as_millis().saturating_add(horizon.millis()))
            }),
            open: [true; 5],
        };
        self.stats.opened += 1;
        let seq = self.first_label + self.labels.len() as u64;
        match reference {
            Some(reference) if !self.stale => {
                for (position, horizon) in open.label.horizons.iter().enumerate() {
                    if let Some(barriers) = horizon.barriers {
                        self.above.insert((barriers.upper, seq, position as u8));
                        self.below.insert((barriers.lower, seq, position as u8));
                    }
                }
                for (target, passage) in [(LEVEL, open.label.level), (EXTREME, open.label.extreme)]
                {
                    match passage {
                        Some(passage) if passage.price > reference => {
                            self.above.insert((passage.price, seq, target));
                        }
                        Some(passage) if passage.price < reference => {
                            self.below.insert((passage.price, seq, target));
                        }
                        _ => {}
                    }
                }
                for lane in &mut self.lanes {
                    lane.front.get_or_insert(seq);
                }
            }
            // The path from the reference to the decision is unknown: the
            // whole label is censored at the decision.
            _ => {
                open.censor(
                    Censor {
                        at: event.time,
                        reason: CensorReason::StaleReference,
                    },
                    &Stretch::default(),
                );
            }
        }
        self.labels.push_back(open);
    }

    /// Observes `event`, which the engine has accepted, and passes every
    /// label it completes to `emit`, in record order.
    pub fn observe(&mut self, event: &MarketEvent, mut emit: impl FnMut(ForwardLabel)) {
        let key = event.canonical_key();
        if let MarketEvent::FeedGap(gap) = event
            && gap.stream == Stream::Trades
        {
            // Censor before closing: a gap ordered at or after a window's
            // end may still have started inside it.
            self.reach_back(gap.start);
            self.censor(gap.start, CensorReason::Gap);
            self.stale = true;
        }
        self.close_until(key.time);
        if let MarketEvent::Trade(trade) = event {
            self.trade(key, trade.price);
            self.stale = false;
        }
        self.last_event = Some(key.time);
        self.emit_complete(&mut emit);
    }

    /// Reports an event the engine rejected. A trades-stream event censors
    /// the unemitted labels at the later of its time and their decision time
    /// (ADR-055 D6); events of other streams are ignored, since labels read
    /// trades only. Labels it completes are emitted by the next
    /// [`observe`](Self::observe) or [`finish`](Self::finish).
    pub fn reject(&mut self, event: &MarketEvent) {
        if event.stream() != Stream::Trades {
            return;
        }
        self.reach_back(event.time());
        self.censor(event.time(), CensorReason::Rejected);
    }

    /// Ends the data: censors every open component at the last observed
    /// event and emits every remaining label, in record order.
    pub fn finish(&mut self, mut emit: impl FnMut(ForwardLabel)) {
        let last = self
            .last_event
            .or_else(|| self.labels.back().map(|open| open.label.event.time));
        if let Some(last) = last {
            self.censor(last, CensorReason::EndOfData);
        }
        self.emit_complete(&mut emit);
        debug_assert!(self.labels.is_empty(), "every window closes at the end");
    }

    /// The D9 `clear` flags of `record`, per horizon.
    fn thin(&mut self, record: &ContextRecord) -> [bool; 5] {
        let slots = match &record.sample {
            Sample::Baseline { .. } => Some(&mut self.clear_baseline),
            Sample::Trigger { trigger, episode } if episode.opener => {
                trigger_position(trigger.feature).map(|position| &mut self.clear_trigger[position])
            }
            Sample::Trigger { .. } => None,
        };
        let Some(slots) = slots else {
            return [false; 5];
        };
        let time = record.event.time;
        std::array::from_fn(|position| {
            let clear = slots[position].is_none_or(|last| {
                time.as_millis() >= last.as_millis().saturating_add(HORIZONS[position].millis())
            });
            if clear {
                slots[position] = Some(time);
            }
            clear
        })
    }

    /// Counts a cause at `time` that reaches into an emitted label.
    fn reach_back(&mut self, time: EventTime) {
        if self.emitted_until.is_some_and(|until| time < until) {
            self.stats.reach_backs += 1;
        }
    }

    /// The index in `labels` of the label `seq`.
    fn index(&self, seq: u64) -> usize {
        usize::try_from(seq - self.first_label).expect("an unemitted label")
    }

    /// The trade `seq`.
    fn tick(&self, seq: u64) -> Tick {
        self.ticks[usize::try_from(seq - self.first_tick).expect("a held trade")]
    }

    /// Censors every component of every unemitted label whose window
    /// reaches past `max(start, decision time)`, closed windows included
    /// (ADR-055 D6). Reads the held trades once, newest first, for every
    /// label at once: they reach back to the oldest unemitted label.
    fn censor(&mut self, start: EventTime, reason: CensorReason) {
        let mut before = self.ticks.partition_point(|tick| tick.key.time < start);
        let mut stretch = Stretch::default();
        for index in (0..self.labels.len()).rev() {
            let seq = self.first_label + index as u64;
            let open = &mut self.labels[index];
            let decision = open.label.event;
            let at = start.max(decision.time);
            if !(0..HORIZONS.len()).any(|position| open.reaches(position, at)) {
                continue;
            }
            // The trades after the decision and strictly before `at`: none
            // when `at` is the decision time.
            let read = if decision.time < start {
                while before > 0 && self.ticks[before - 1].key > decision {
                    before -= 1;
                    stretch.add_earlier(self.ticks[before]);
                }
                stretch
            } else {
                Stretch::default()
            };
            let closed = open.censor(Censor { at, reason }, &read);
            for (position, closed) in closed.into_iter().enumerate() {
                if closed {
                    self.forget(seq, position);
                }
            }
        }
        for position in 0..HORIZONS.len() {
            self.advance(position);
        }
    }

    /// Closes every window that ends at or before `now`: an event at or
    /// after a window's end proves every trade before it was delivered.
    fn close_until(&mut self, now: EventTime) {
        for position in 0..HORIZONS.len() {
            while let Some(seq) = self.lanes[position].front {
                let index = self.index(seq);
                if self.labels[index].ends[position] > now {
                    break;
                }
                let lane = &self.lanes[position];
                let decision = self.labels[index].label.event;
                let high = lane.high.front().map(|&trade| self.tick(trade));
                let low = lane.low.front().map(|&trade| self.tick(trade));
                let stretch = Stretch {
                    high: high.map(|tick| (tick.price, tick.key.time)),
                    low: low.map(|tick| (tick.price, tick.key.time)),
                    // Every held trade is earlier than this window's end.
                    last: self
                        .ticks
                        .back()
                        .filter(|tick| tick.key > decision)
                        .map(|tick| tick.price),
                };
                let open = &mut self.labels[index];
                let reference = open.label.reference.expect("open labels have a reference");
                open.label.horizons[position].read(reference, &stretch);
                open.open[position] = false;
                self.forget(seq, position);
                self.advance(position);
            }
        }
    }

    /// Removes the pending touches of the label `seq` that end with the
    /// window at `position`: its race, and its passages for the 1d window.
    fn forget(&mut self, seq: u64, position: usize) {
        let label = &self.labels[self.index(seq)].label;
        if let Some(barriers) = label.horizons[position].barriers {
            self.above.remove(&(barriers.upper, seq, position as u8));
            self.below.remove(&(barriers.lower, seq, position as u8));
        }
        if position == PASSAGE_HORIZON {
            for (target, passage) in [(LEVEL, label.level), (EXTREME, label.extreme)] {
                if let Some(passage) = passage {
                    self.above.remove(&(passage.price, seq, target));
                    self.below.remove(&(passage.price, seq, target));
                }
            }
        }
    }

    /// Moves the lane at `position` to its first label with the horizon
    /// open, and drops the trades at or before that label's event.
    fn advance(&mut self, position: usize) {
        let Some(mut seq) = self.lanes[position].front else {
            return;
        };
        let end = self.first_label + self.labels.len() as u64;
        while seq < end && !self.labels[self.index(seq)].open[position] {
            seq += 1;
        }
        if seq == end {
            let lane = &mut self.lanes[position];
            lane.front = None;
            lane.high.clear();
            lane.low.clear();
            return;
        }
        let decision = self.labels[self.index(seq)].label.event;
        let (ticks, first_tick) = (&self.ticks, self.first_tick);
        let after =
            |trade: &u64| ticks[usize::try_from(*trade - first_tick).unwrap()].key > decision;
        let lane = &mut self.lanes[position];
        lane.front = Some(seq);
        while lane.high.front().is_some_and(|trade| !after(trade)) {
            lane.high.pop_front();
        }
        while lane.low.front().is_some_and(|trade| !after(trade)) {
            lane.low.pop_front();
        }
    }

    /// Takes in a trade: holds it, feeds the sliding windows and resolves
    /// the touches it makes, in canonical order.
    fn trade(&mut self, key: CanonicalKey, price: Price) {
        let seq = self.first_tick + self.ticks.len() as u64;
        self.ticks.push_back(Tick { key, price });
        let (ticks, first_tick) = (&self.ticks, self.first_tick);
        let price_of = |trade: u64| ticks[usize::try_from(trade - first_tick).unwrap()].price;
        for lane in &mut self.lanes {
            if lane.front.is_none() {
                continue;
            }
            while lane
                .high
                .back()
                .is_some_and(|&trade| price_of(trade) < price)
            {
                lane.high.pop_back();
            }
            lane.high.push_back(seq);
            while lane
                .low
                .back()
                .is_some_and(|&trade| price_of(trade) > price)
            {
                lane.low.pop_back();
            }
            lane.low.push_back(seq);
        }
        while let Some(&touch) = self.above.first()
            && touch.0 <= price
        {
            self.above.pop_first();
            self.touch(touch, true, key.time);
        }
        while let Some(&touch) = self.below.last()
            && touch.0 >= price
        {
            self.below.pop_last();
            self.touch(touch, false, key.time);
        }
    }

    /// Applies one touch by a trade at `at`: the upper side when `above`.
    fn touch(&mut self, (_, seq, target): Touch, above: bool, at: EventTime) {
        let index = self.index(seq);
        let label = &mut self.labels[index].label;
        let passage = match target {
            LEVEL => &mut label.level,
            EXTREME => &mut label.extreme,
            race => {
                let race = &mut label.horizons[usize::from(race)].race;
                match race {
                    Race::Neither if above => *race = Race::Upper { at, same_ms: false },
                    Race::Neither => *race = Race::Lower { at, same_ms: false },
                    // The other barrier, later in the winner's millisecond.
                    Race::Upper { at: first, same_ms } if !above && *first == at => {
                        *same_ms = true;
                    }
                    Race::Lower { at: first, same_ms } if above && *first == at => {
                        *same_ms = true;
                    }
                    _ => {}
                }
                return;
            }
        };
        if let Some(passage) = passage {
            passage.passage = Passage::Touched { at };
        }
    }

    /// Emits the complete labels at the front, then drops the trades no
    /// unemitted label can read.
    fn emit_complete(&mut self, emit: &mut impl FnMut(ForwardLabel)) {
        while self.labels.front().is_some_and(Open::complete) {
            let open = self.labels.pop_front().expect("a front label");
            self.first_label += 1;
            let until = open.label.horizons[PASSAGE_HORIZON]
                .censored
                .map_or(open.ends[PASSAGE_HORIZON], |censor| censor.at);
            self.emitted_until = Some(self.emitted_until.map_or(until, |at| at.max(until)));
            self.stats.emitted += 1;
            emit(open.label);
        }
        match self.labels.front().map(|open| open.label.event) {
            Some(oldest) => {
                while self.ticks.front().is_some_and(|tick| tick.key <= oldest) {
                    self.ticks.pop_front();
                    self.first_tick += 1;
                }
            }
            None => {
                self.first_tick += self.ticks.len() as u64;
                self.ticks.clear();
            }
        }
    }
}
