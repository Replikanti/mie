//! The ADR-055 measurement (issue #26, M2; run by M3): an independent model
//! of the ADR-055 decision text, D1–D9, run next to the forward labeler,
//! that checks the labels it emits and reports what *Accept when* asks for.
//!
//! The labeler runs through the app driver, `mie_app::conditional::
//! label_contexts`, one event at a time: the engine, the sampler and the
//! labeler are the caller's, so a drive per event is the same drive as one
//! over the stream (pinned by `mie-app`), and the model sees which labels
//! each event emitted. The replay ends with `finish_labels` and the
//! provider's trailing gaps, as ADR-055 D6 states the driver contract. The
//! model is written from the ADR text, not from the labeler's internals.
//! It reads the accepted and rejected events, the records the sampler
//! emits, and the trailing gaps, and rebuilds from them:
//!
//! - the join key (D1): the record's event and its ordinal among the
//!   records of that event;
//! - the window (D2): the accepted trades with a canonical key after the
//!   record's event and a time before `event.time + H`, for H in D3;
//! - the barriers (D4): `reference ± b_H · ATR`, `b_H` per 10⁵ rounded half
//!   to even to the price unit, unavailable without a ready ATR, at a zero
//!   offset or outside the price range;
//! - the race and its tie rule (D5): the first touching trade in canonical
//!   order, `same_ms` when a later trade of its millisecond touches the
//!   other barrier;
//! - censoring (D6): trades gaps at `max(start, decision)`, the stale
//!   reference, rejected trades-stream events at `max(decision, min(t, the
//!   last accepted trades-stream event))`, the end of data at the last
//!   accepted trades-stream event, each reaching every horizon whose
//!   window ends after its time, closed or not, as long as the label is not
//!   emitted, the earliest censor winning; the emission point (all windows
//!   closed and a trades-stream event at or after the label's until, in
//!   record order); and the reach-backs into emitted labels;
//! - the excursions and the last trade (D7), over the trades strictly
//!   before the censor;
//! - the passages (D8): the evidence table, the side fixed at decision,
//!   `AtDecision` for a price at the reference;
//! - the clear flags (D9): the greedy rule per horizon over the baseline
//!   records and over the episode openers of each trigger id.
//!
//! The model holds every record not yet labeled, about a day of them, with
//! its censors, and the accepted trades after the oldest one it checks:
//! at most a day of trades, plus those of a trades outage over a 1d end.
//! A label is built when the model emits it, by one scan of its window.
//!
//! Reported:
//!
//! - **T0** (asserted): at every event the labeler emits the labels the
//!   model emits there, in order, with the same join keys; every checked
//!   label equals the model's on every field; at the end, the labeler's
//!   counts (`LabelStats`: opened, emitted, reach-backs) equal the model's.
//!   On the archive the checked labels are every trigger record's and the
//!   baseline records' whose `minute_end` falls on a whole UTC hour, chosen
//!   by decision time only (ADR-055 *Accept when*); elsewhere every label.
//! - **Completeness** (asserted): exactly one label per record, in record
//!   order.
//! - Recorded only, over every label: per horizon the `Neither` share (next
//!   to the 0.92 % null prediction of D4), the shares of windows censored
//!   per reason, the unavailable races, the `same_ms` races and the clear
//!   records per stream kind; the reach-backs; the wall time and the peak
//!   RSS. Deliberately not reported: which barrier came first, excursion or
//!   time quantiles, and any split by trigger, regime or location (*Accept
//!   when*).
//!
//! Every count is printed before any assertion.
//!
//! `measure_labels` (`--ignored`) runs the archive: aggTrades with the
//! ADR-050 fold (its D8 default) and `metrics` for the open interest, as the
//! ADR-054 measurement. Run it with `MIE_MEASURE_RAW_ROOT=<raw root>
//! MIE_MEASURE_FROM=<YYYY-MM-DD> MIE_MEASURE_TO=<YYYY-MM-DD> cargo test
//! --release -p mie-cli --test label_measure -- --ignored --exact
//! --nocapture measure_labels` (`--exact` keeps the live test out);
//! `MIE_MEASURE_TO` is included, as in `mie replay`, and an optional
//! `MIE_MEASURE_EVENTS` asserts the replay's event count. M3 runs the #12
//! development year, 2025-10-01 … 2026-09-30, with
//! `MIE_MEASURE_EVENTS=603323345` (ADR-051 R0's count of the same replay).
//!
//! `measure_labels_live` (`--ignored`) runs every cleanly ended run of a
//! `mie ingest` capture alone through `LiveReplay::run_replay`, each into a
//! fresh engine, sampler and labeler, and checks every label:
//! `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_JOURNAL=<journal> cargo test
//! --release -p mie-cli --test label_measure -- --ignored --exact
//! --nocapture measure_labels_live`. A whole clean run has no gap still
//! open at its end, so it has no trailing gaps.
//!
//! `model_reproduces_the_labeler_on_a_synthetic_tape` runs T0, completeness
//! and the report in CI on the seeded three-week tape of `common::tapes`
//! (its trades gap on day 9 censors the windows it falls in, and the 1d
//! windows before it complete), with a same-millisecond burst, a late trades gap and
//! three rejected trades (a repeat, a time jump, an out-of-order trade at a
//! window end) added: once checking every label with a trailing trades
//! gap, once with the archive's selection and none. It asserts that every
//! compared kind occurred, so T0 cannot pass vacuously, and that
//! `label_window` over the same tape emits the same labels.

mod common;

use common::tapes::{TAPE_DAYS, TAPE_START, synthetic_tape};
use mie_adapter_binance::LiveReplay;
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::conditional::{finish_labels, label_contexts, label_window};
use mie_cli::journal::{JournaledRun, read_runs};
use mie_cli::replay::parse_bound;
use mie_domain::candidate::CandidateEvent;
use mie_domain::conditional::label::{
    BARRIER_MULTIPLE_E5, Barriers, Censor, CensorReason, Excursion, ForwardLabel, ForwardLabeler,
    HORIZONS, HorizonLabel, LABELING_V1, LabelProvenance, LabelStats, Passage, PassageLabel, Race,
    barrier_offset,
};
use mie_domain::conditional::{ContextProvenance, ContextRecord, ContextSampler, Sample};
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream, Trade};
use mie_domain::feature::FeatureKey;
use mie_domain::fingerprint::Fingerprinter;
use mie_domain::location::{AuctionStatus, Region};
use mie_domain::num::{Price, SCALE};
use mie_domain::order::CanonicalKey;
use mie_domain::research::DataVersion;
use mie_domain::state::MarketStateEngine;
use mie_domain::structure::Sweep;
use mie_domain::time::EventTime;
use mie_domain::trigger::{OiRejection, TriggerEvent, TriggerEvidence};
use mie_ports::outbound::{
    HistoricalDataProvider, MarketDataProvider, ProviderError, Replay, ReplayWindow,
};
use mie_ports::raw::DatasetVersion;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 86_400_000;

/// D3: the horizons, in ms, shortest first.
const HORIZON_MS: [i64; 5] = [5 * MINUTE_MS, 15 * MINUTE_MS, HOUR_MS, 4 * HOUR_MS, DAY_MS];

/// D4: `b_H = √(π · H / 32 h)` per 10⁵, per horizon.
const MULTIPLE_E5: [i64; 5] = [9_045, 15_666, 31_333, 62_666, 153_499];

/// D8: the passages are read in the 1d window, the last horizon.
const PASSAGE: usize = 4;

/// D4's null prediction of the `Neither` share, in percent.
const NULL_NEITHER_PERCENT: f64 = 0.92;

/// The censor reasons, in the order the report prints them.
const REASONS: [CensorReason; 4] = [
    CensorReason::Gap,
    CensorReason::StaleReference,
    CensorReason::Rejected,
    CensorReason::EndOfData,
];

// ---------------------------------------------------------------------------
// The model's rules (D4, D8, D9)
// ---------------------------------------------------------------------------

/// D4: `b_H · ATR`, `b_H` per 10⁵, rounded half to even to the price unit;
/// `None` at 0.
fn offset(atr: Price, position: usize) -> Option<i64> {
    let product = i128::from(atr.units()) * i128::from(MULTIPLE_E5[position]);
    let (quotient, remainder) = (product.div_euclid(100_000), product.rem_euclid(100_000));
    let half_up = 2 * remainder > 100_000 || (2 * remainder == 100_000 && quotient % 2 != 0);
    i64::try_from(quotient + i128::from(half_up))
        .ok()
        .filter(|offset| *offset > 0)
}

/// D4: the barriers of the horizon at `position`; `None` without a reference
/// or a ready ATR, at a zero offset or outside the price range.
fn barriers(reference: Option<Price>, atr: Option<Price>, position: usize) -> Option<Barriers> {
    let reference = reference?.units();
    let offset = offset(atr?, position)?;
    Some(Barriers {
        lower: Price::from_units(reference.checked_sub(offset)?),
        upper: Price::from_units(reference.checked_add(offset)?),
    })
}

/// D8's row of an SFP: the swept level's price and the sweep's extreme.
fn sfp_prices(sweep: &Sweep) -> (Option<Price>, Option<Price>) {
    (Some(sweep.level.price), Some(sweep.extreme))
}

/// D8's row of an auction: the value edge on the status's side, no extreme.
fn auction_prices(status: &AuctionStatus) -> (Option<Price>, Option<Price>) {
    let level = match status.side {
        Some(Region::Above) => Some(status.vah),
        Some(Region::Below) => Some(status.val),
        Some(Region::In) | None => None,
    };
    (level, None)
}

/// D8: the `level` and `extreme` of a trigger, by the table.
fn evidence(trigger: &TriggerEvent) -> (Option<Price>, Option<Price>) {
    match &trigger.evidence {
        TriggerEvidence::Sfp(sweep) => sfp_prices(sweep),
        TriggerEvidence::Auction(status) => auction_prices(status),
        TriggerEvidence::Candidate(fact) => match fact.as_ref() {
            CandidateEvent::CvdDivergence(event) => {
                let price = event.comparison.pair.current.price;
                (Some(price), Some(price))
            }
            CandidateEvent::OiDivergence(event) => {
                let price = event.comparison.pair.current.price;
                (Some(price), Some(price))
            }
            CandidateEvent::Exhaustion(event) => (None, Some(event.value.extreme)),
            CandidateEvent::EffortVsResult(_) | CandidateEvent::Absorption(_) => (None, None),
        },
        TriggerEvidence::OiExpansion(expansion) => match &expansion.rejection {
            OiRejection::Sfp(sweep) => sfp_prices(sweep),
            OiRejection::FailedAuction(status) => auction_prices(status),
        },
    }
}

/// D9's greedy rule on one stream's slots: clear at a horizon when it is the
/// stream's first record or at least H after its last clear one.
fn greedy(slots: &mut [Option<i64>; 5], time: i64) -> [bool; 5] {
    std::array::from_fn(|position| {
        let clear = slots[position].is_none_or(|last| time >= last + HORIZON_MS[position]);
        if clear {
            slots[position] = Some(time);
        }
        clear
    })
}

// ---------------------------------------------------------------------------
// The model (D1–D9)
// ---------------------------------------------------------------------------

/// A record whose label the model has not emitted yet.
#[derive(Debug, Clone)]
struct Pending {
    event: CanonicalKey,
    ordinal: u32,
    baseline: bool,
    reference: Option<Price>,
    atr: Option<Price>,
    level: Option<Price>,
    extreme: Option<Price>,
    clear: [bool; 5],
    /// `event.time + H`, exclusive.
    ends: [i64; 5],
    /// The earliest censor that reached each horizon so far.
    censors: [Option<Censor>; 5],
    /// The model builds and compares this label.
    checked: bool,
}

impl Pending {
    /// D6: the 1d censor time, or the 1d window end.
    fn until(&self) -> i64 {
        self.censors[PASSAGE].map_or(self.ends[PASSAGE], |censor| censor.at.as_millis())
    }
}

/// A label the model emits: its join key, its stream kind, and the label
/// itself when checked.
#[derive(Debug)]
struct Expected {
    key: (CanonicalKey, u32),
    baseline: bool,
    label: Option<ForwardLabel>,
}

/// The model of D1–D9.
struct Model {
    /// Which records to check.
    pick: fn(&ContextRecord) -> bool,
    pending: VecDeque<Pending>,
    /// The events of the pending checked records, in order.
    checked_events: VecDeque<CanonicalKey>,
    /// The accepted trades after the oldest pending checked record's event.
    trades: VecDeque<(CanonicalKey, Price)>,
    /// The latest accepted event's time: every window ending at or before
    /// it is closed (D2).
    latest: Option<i64>,
    /// The latest accepted trades-stream event's time (a trade, or a trades
    /// gap's end).
    trades_until: Option<i64>,
    /// The last accepted trades-stream event is a gap (D6, stale reference).
    gap_since_trade: bool,
    /// The latest until of the emitted labels.
    emitted_until: Option<i64>,
    stats: LabelStats,
    /// The event of the last record and its ordinal.
    last_record: Option<(CanonicalKey, u32)>,
    /// D9's slots: the baseline stream, and one per trigger id.
    clear_baseline: [Option<i64>; 5],
    clear_triggers: Vec<(FeatureKey, [Option<i64>; 5])>,
}

impl Model {
    fn new(pick: fn(&ContextRecord) -> bool) -> Self {
        Self {
            pick,
            pending: VecDeque::new(),
            checked_events: VecDeque::new(),
            trades: VecDeque::new(),
            latest: None,
            trades_until: None,
            gap_since_trade: false,
            emitted_until: None,
            stats: LabelStats::default(),
            last_record: None,
            clear_baseline: [None; 5],
            clear_triggers: Vec::new(),
        }
    }

    /// An accepted event, then the labels it emits, in record order.
    fn accepted(&mut self, event: &MarketEvent, out: &mut Vec<Expected>) {
        let key = event.canonical_key();
        let time = key.time.as_millis();
        match event {
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => {
                self.cause(gap.start.as_millis(), CensorReason::Gap, true);
                self.gap_since_trade = true;
            }
            MarketEvent::Trade(trade) => {
                self.gap_since_trade = false;
                if !self.checked_events.is_empty() {
                    self.trades.push_back((key, trade.price));
                }
            }
            _ => {}
        }
        self.latest = self.latest.max(Some(time));
        if event.stream() == Stream::Trades {
            self.trades_until = self.trades_until.max(Some(time));
        }
        self.emit(out, false);
    }

    /// A rejected event (D6): on the trades stream it censors at
    /// `max(decision, min(t, the last accepted trades-stream event))`, the
    /// decision when there is none; it closes nothing and emits nothing.
    fn rejected(&mut self, event: &MarketEvent) {
        if event.stream() != Stream::Trades {
            return;
        }
        let time = event.time().as_millis();
        let start = self.trades_until.map_or(i64::MIN, |until| until.min(time));
        self.cause(start, CensorReason::Rejected, true);
    }

    /// A record the sampler emitted at the last accepted event.
    fn open(&mut self, record: &ContextRecord) {
        let event = record.event;
        let ordinal = match self.last_record {
            Some((last, ordinal)) if last == event => ordinal + 1,
            _ => 0,
        };
        self.last_record = Some((event, ordinal));
        let time = event.time.as_millis();
        let ((level, extreme), clear, baseline) = match &record.sample {
            Sample::Baseline { .. } => ((None, None), greedy(&mut self.clear_baseline, time), true),
            Sample::Trigger { trigger, episode } => {
                let clear = if episode.opener {
                    let position = match self
                        .clear_triggers
                        .iter()
                        .position(|(id, _)| *id == trigger.feature)
                    {
                        Some(position) => position,
                        None => {
                            self.clear_triggers.push((trigger.feature, [None; 5]));
                            self.clear_triggers.len() - 1
                        }
                    };
                    greedy(&mut self.clear_triggers[position].1, time)
                } else {
                    [false; 5]
                };
                (evidence(trigger), clear, false)
            }
        };
        let reference = record.state.last_price.ready().copied();
        // D6: no trade before the decision, or a trades gap since the last.
        let stale = reference.is_none() || self.gap_since_trade;
        let censor = Censor {
            at: event.time,
            reason: CensorReason::StaleReference,
        };
        let checked = (self.pick)(record);
        if checked {
            self.checked_events.push_back(event);
        }
        self.pending.push_back(Pending {
            event,
            ordinal,
            baseline,
            reference,
            atr: record.state.atr.ready().copied(),
            level,
            extreme,
            clear,
            ends: HORIZON_MS.map(|horizon| time + horizon),
            censors: [stale.then_some(censor); 5],
            checked,
        });
        self.stats.opened += 1;
    }

    /// The end of the data (D6): the trades-stream trailing gaps in
    /// canonical order, then the end-of-data censor at the last accepted
    /// trades-stream event, and every remaining label.
    fn finish(&mut self, trailing: &[FeedGap], out: &mut Vec<Expected>) {
        let mut gaps: Vec<MarketEvent> = trailing
            .iter()
            .filter(|gap| gap.stream == Stream::Trades)
            .map(|gap| MarketEvent::FeedGap(*gap))
            .collect();
        gaps.sort();
        for gap in &gaps {
            self.accepted(gap, out);
        }
        let known = self.trades_until.unwrap_or(i64::MIN);
        self.cause(known, CensorReason::EndOfData, false);
        self.emit(out, true);
    }

    /// A censor from `start` on (D6): at `max(start, decision)` for every
    /// pending label, reaching each horizon whose window ends after it
    /// unless an earlier censor did. A cause that starts before an emitted
    /// label's until reaches back, once.
    fn cause(&mut self, start: i64, reason: CensorReason, reaches_back: bool) {
        if reaches_back && self.emitted_until.is_some_and(|until| start < until) {
            self.stats.reach_backs += 1;
        }
        for pending in &mut self.pending {
            let at = start.max(pending.event.time.as_millis());
            for (end, censor) in pending.ends.iter().zip(&mut pending.censors) {
                if at < *end && censor.is_none_or(|earlier| at < earlier.at.as_millis()) {
                    *censor = Some(Censor {
                        at: EventTime::from_millis(at),
                        reason,
                    });
                }
            }
        }
    }

    /// Emits the labels at the front that are ready (D6), all of them at
    /// the end of the data: every window closed, by an event at or after its
    /// end or by a censor, and a trades-stream event at or after its until.
    fn emit(&mut self, out: &mut Vec<Expected>, end: bool) {
        while let Some(front) = self.pending.front() {
            let closed = (0..5).all(|position| {
                front.censors[position].is_some()
                    || self.latest.is_some_and(|at| at >= front.ends[position])
            });
            let ready = closed && self.trades_until.is_some_and(|at| at >= front.until());
            if !(end || ready) {
                break;
            }
            let pending = self.pending.pop_front().expect("a front record");
            self.emitted_until = self.emitted_until.max(Some(pending.until()));
            self.stats.emitted += 1;
            let label = pending.checked.then(|| self.label(&pending));
            if pending.checked {
                self.checked_events.pop_front();
                match self.checked_events.front() {
                    Some(oldest) => {
                        while self.trades.front().is_some_and(|(key, _)| key <= oldest) {
                            self.trades.pop_front();
                        }
                    }
                    None => self.trades.clear(),
                }
            }
            out.push(Expected {
                key: (pending.event, pending.ordinal),
                baseline: pending.baseline,
                label,
            });
        }
    }

    /// The label of `pending`, by one scan of its window (D2, D4–D8): the
    /// held trades after its event, each horizon's up to its window end or
    /// its censor, whichever is first.
    fn label(&self, pending: &Pending) -> ForwardLabel {
        let decision = pending.event;
        let cuts: [i64; 5] = std::array::from_fn(|position| {
            pending.censors[position].map_or(pending.ends[position], |censor| {
                censor.at.as_millis().min(pending.ends[position])
            })
        });
        let bars: [Option<Barriers>; 5] =
            std::array::from_fn(|position| barriers(pending.reference, pending.atr, position));
        #[derive(Default, Clone, Copy)]
        struct Scan {
            high: Option<(Price, EventTime)>,
            low: Option<(Price, EventTime)>,
            last: Option<Price>,
            /// The first touch: upper or lower, and its time.
            first: Option<(bool, EventTime)>,
            same_ms: bool,
        }
        let mut scans = [Scan::default(); 5];
        let mut reached = [None::<EventTime>; 2];
        let targets = [pending.level, pending.extreme];
        let horizon_end = *cuts.iter().max().expect("five horizons");
        let first = self.trades.partition_point(|(key, _)| *key <= decision);
        for &(key, price) in self.trades.range(first..) {
            let time = key.time.as_millis();
            if time >= horizon_end {
                break;
            }
            for position in 0..5 {
                if time >= cuts[position] {
                    continue;
                }
                let scan = &mut scans[position];
                if scan.high.is_none_or(|(high, _)| price > high) {
                    scan.high = Some((price, key.time));
                }
                if scan.low.is_none_or(|(low, _)| price < low) {
                    scan.low = Some((price, key.time));
                }
                scan.last = Some(price);
                if let Some(bars) = bars[position] {
                    let (upper, lower) = (price >= bars.upper, price <= bars.lower);
                    match scan.first {
                        None if upper || lower => scan.first = Some((upper, key.time)),
                        Some((was_upper, at))
                            if at == key.time && (if was_upper { lower } else { upper }) =>
                        {
                            scan.same_ms = true;
                        }
                        _ => {}
                    }
                }
            }
            if time < cuts[PASSAGE]
                && let Some(reference) = pending.reference
            {
                for (target, reached) in targets.iter().zip(&mut reached) {
                    if let Some(target) = *target
                        && reached.is_none()
                        && ((target > reference && price >= target)
                            || (target < reference && price <= target))
                    {
                        *reached = Some(key.time);
                    }
                }
            }
        }
        let horizons = std::array::from_fn(|position| {
            let scan = scans[position];
            let censored = pending.censors[position];
            let race = match (bars[position], scan.first) {
                (None, _) => Race::Unavailable,
                (Some(_), Some((true, at))) => Race::Upper {
                    at,
                    same_ms: scan.same_ms,
                },
                (Some(_), Some((false, at))) => Race::Lower {
                    at,
                    same_ms: scan.same_ms,
                },
                (Some(_), None) => censored.map_or(Race::Neither, Race::Censored),
            };
            let (mut up, mut down) = (Excursion::NONE, Excursion::NONE);
            if let Some(reference) = pending.reference {
                if let Some((high, at)) = scan.high
                    && high > reference
                {
                    up = Excursion {
                        size: Price::from_units(high.units() - reference.units()),
                        at: Some(at),
                    };
                }
                if let Some((low, at)) = scan.low
                    && low < reference
                {
                    down = Excursion {
                        size: Price::from_units(reference.units() - low.units()),
                        at: Some(at),
                    };
                }
            }
            HorizonLabel {
                horizon: HORIZONS[position],
                clear: pending.clear[position],
                barriers: bars[position],
                race,
                up,
                down,
                last: scan.last,
                censored,
            }
        });
        let censored = pending.censors[PASSAGE];
        let passage = |price: Option<Price>, reached: Option<EventTime>| {
            price.map(|price| PassageLabel {
                price,
                passage: if Some(price) == pending.reference {
                    // Reached at the decision: a censor after it keeps it.
                    match censored {
                        Some(censor) if censor.at <= decision.time => Passage::Censored(censor),
                        _ => Passage::AtDecision,
                    }
                } else if let Some(at) = reached {
                    Passage::Touched { at }
                } else {
                    censored.map_or(Passage::NotWithin, Passage::Censored)
                },
            })
        };
        ForwardLabel {
            event: decision,
            ordinal: pending.ordinal,
            reference: pending.reference,
            atr: pending.atr,
            horizons,
            level: passage(pending.level, reached[0]),
            extreme: passage(pending.extreme, reached[1]),
        }
    }
}

/// The archive's selection (ADR-055 *Accept when*): every trigger record,
/// and the baseline records whose `minute_end` falls on a whole UTC hour.
fn hourly_and_triggers(record: &ContextRecord) -> bool {
    match &record.sample {
        Sample::Trigger { .. } => true,
        Sample::Baseline { minute_end } => minute_end.as_millis() % HOUR_MS == 0,
    }
}

/// Every record.
fn every(_: &ContextRecord) -> bool {
    true
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

/// Mismatches between the labeler and the model.
#[derive(Debug, Default)]
struct Check {
    mismatches: u64,
    /// Of them, labels out of record order or at the wrong event.
    order: u64,
    notes: Vec<String>,
}

impl Check {
    fn note(&mut self, note: String) {
        self.mismatches += 1;
        if self.notes.len() < 20 {
            self.notes.push(note);
        }
    }
}

/// The report's counts over every label, per horizon.
#[derive(Debug, Default)]
struct Counts {
    neither: [u64; 5],
    unavailable: [u64; 5],
    same_ms: [u64; 5],
    /// Windows censored, per reason in [`REASONS`] order.
    censored: [[u64; 4]; 5],
    clear_baseline: [u64; 5],
    clear_trigger: [u64; 5],
}

/// What the checked labels showed, so T0 cannot pass vacuously.
#[derive(Debug, Default)]
struct Kinds {
    upper: u64,
    lower: u64,
    neither: u64,
    unavailable: u64,
    censored_races: u64,
    same_ms: u64,
    /// Windows censored, per reason in [`REASONS`] order.
    censors: [u64; 4],
    touched: u64,
    not_within: u64,
    censored_passages: u64,
    clear: u64,
}

impl Kinds {
    fn add(&mut self, label: &ForwardLabel) {
        for horizon in &label.horizons {
            match horizon.race {
                Race::Upper { same_ms, .. } => {
                    self.upper += 1;
                    self.same_ms += u64::from(same_ms);
                }
                Race::Lower { same_ms, .. } => {
                    self.lower += 1;
                    self.same_ms += u64::from(same_ms);
                }
                Race::Neither => self.neither += 1,
                Race::Censored(_) => self.censored_races += 1,
                Race::Unavailable => self.unavailable += 1,
            }
            if let Some(censor) = horizon.censored {
                self.censors[reason_index(censor.reason)] += 1;
            }
            self.clear += u64::from(horizon.clear);
        }
        for passage in [label.level, label.extreme].into_iter().flatten() {
            match passage.passage {
                Passage::Touched { .. } => self.touched += 1,
                Passage::NotWithin => self.not_within += 1,
                Passage::Censored(_) => self.censored_passages += 1,
                Passage::AtDecision => {}
            }
        }
    }
}

fn reason_index(reason: CensorReason) -> usize {
    REASONS
        .iter()
        .position(|known| *known == reason)
        .expect("a censor reason")
}

/// One drive: the engine, the sampler, the labeler, the model and the
/// counts.
struct Run {
    engine: MarketStateEngine,
    sampler: ContextSampler,
    labeler: ForwardLabeler,
    model: Model,
    check: Check,
    /// Events delivered, rejected ones included, and the rejections.
    events: u64,
    rejections: u64,
    records: u64,
    labels: u64,
    checked: u64,
    counts: Counts,
    kinds: Kinds,
    /// The digest of every label's line, in order.
    digest: Fingerprinter,
}

impl Run {
    fn new(pick: fn(&ContextRecord) -> bool) -> Self {
        Self {
            engine: MarketStateEngine::new(),
            sampler: ContextSampler::new(None),
            labeler: ForwardLabeler::new(),
            model: Model::new(pick),
            check: Check::default(),
            events: 0,
            rejections: 0,
            records: 0,
            labels: 0,
            checked: 0,
            counts: Counts::default(),
            kinds: Kinds::default(),
            digest: Fingerprinter::new(),
        }
    }

    /// Drives `event` through the labeler and checks the labels it emits.
    fn step(&mut self, event: MarketEvent) {
        let at = event.time().as_millis();
        let (mut records, mut labels) = (Vec::new(), Vec::new());
        let report = label_contexts(
            &mut One(Some(event.clone())),
            &mut self.engine,
            &mut self.sampler,
            &mut self.labeler,
            |_| {},
            |record| records.push(record),
            |label| labels.push(label),
        )
        .expect("a one-event provider cannot fail");
        self.events += report.events;
        let mut expected = Vec::new();
        if report.rejections > 0 {
            self.rejections += report.rejections;
            self.model.rejected(&event);
        } else {
            self.model.accepted(&event, &mut expected);
        }
        self.compare(&at.to_string(), &labels, &expected);
        for record in &records {
            self.model.open(record);
        }
        self.records += records.len() as u64;
    }

    /// Ends the replay with its trailing gaps and compares the counts.
    fn finish(&mut self, trailing: &[FeedGap]) {
        let mut labels = Vec::new();
        finish_labels(&mut self.labeler, trailing, |label| labels.push(label))
            .expect("the trailing gaps keep the driver contract (ADR-055 D6)");
        let mut expected = Vec::new();
        self.model.finish(trailing, &mut expected);
        self.compare("finish", &labels, &expected);
        let (actual, model) = (self.labeler.stats(), self.model.stats);
        if actual != model {
            self.check.note(format!(
                "the labeler's counts differ: {actual:?} vs the model's {model:?}"
            ));
        }
    }

    /// Compares the labels one step emitted with the model's.
    fn compare(&mut self, at: &str, actual: &[ForwardLabel], expected: &[Expected]) {
        if actual.len() != expected.len() {
            self.check.order += 1;
            self.check.note(format!(
                "{at}: the labeler emitted {} labels, the model expects {}",
                actual.len(),
                expected.len()
            ));
        }
        for (label, expected) in actual.iter().zip(expected) {
            if (label.event, label.ordinal) != expected.key {
                self.check.order += 1;
                self.check.note(format!(
                    "{at}: label {} #{} where the model emits {} #{}",
                    label.event, label.ordinal, expected.key.0, expected.key.1
                ));
                continue;
            }
            self.count(label, expected.baseline);
            if let Some(model) = &expected.label {
                self.checked += 1;
                self.kinds.add(label);
                if label != model {
                    self.check
                        .note(format!("{at}: label differs:\n  {label}\n  {model}"));
                }
            }
        }
        for label in actual {
            self.labels += 1;
            self.digest.write_str(&label.to_string());
        }
    }

    /// The report's counts over one label.
    fn count(&mut self, label: &ForwardLabel, baseline: bool) {
        let counts = &mut self.counts;
        for (position, horizon) in label.horizons.iter().enumerate() {
            match horizon.race {
                Race::Neither => counts.neither[position] += 1,
                Race::Unavailable => counts.unavailable[position] += 1,
                Race::Upper { same_ms, .. } | Race::Lower { same_ms, .. } => {
                    counts.same_ms[position] += u64::from(same_ms);
                }
                Race::Censored(_) => {}
            }
            if let Some(censor) = horizon.censored {
                counts.censored[position][reason_index(censor.reason)] += 1;
            }
            if horizon.clear {
                if baseline {
                    counts.clear_baseline[position] += 1;
                } else {
                    counts.clear_trigger[position] += 1;
                }
            }
        }
    }

    /// Completeness: one label per record, in record order, and the
    /// labeler's counts say so.
    fn complete(&self) -> bool {
        let stats = self.labeler.stats();
        self.check.order == 0
            && self.labels == self.records
            && stats.opened == self.records
            && stats.emitted == self.records
    }
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

/// `count` of `total` in percent, three decimals.
fn share(count: u64, total: u64) -> String {
    if total == 0 {
        return "-".to_owned();
    }
    format!("{:.3} %", count as f64 * 100.0 / total as f64)
}

fn report_t0(run: &Run) {
    println!(
        "T0: events {}, rejections {}, records {}, labels {}, checked {}, mismatches {} (order \
         {}); complete {}; labeler {:?}",
        run.events,
        run.rejections,
        run.records,
        run.labels,
        run.checked,
        run.check.mismatches,
        run.check.order,
        run.complete(),
        run.labeler.stats()
    );
    for note in &run.check.notes {
        println!("T0 mismatch: {note}");
    }
}

fn report(run: &Run, wall: Duration) {
    report_t0(run);
    let counts = &run.counts;
    let total = run.labels;
    let reasons: Vec<&str> = REASONS.iter().map(|reason| reason.as_str()).collect();
    println!(
        "per horizon, of {total} labels: neither (null {NULL_NEITHER_PERCENT} %), censored \
         windows {}, unavailable, same_ms, clear baseline/trigger",
        reasons.join("/")
    );
    for (position, horizon) in HORIZONS.iter().enumerate() {
        let censored: Vec<String> = counts.censored[position]
            .iter()
            .map(|count| format!("{count} ({})", share(*count, total)))
            .collect();
        println!(
            "  {horizon}: neither {} ({}); censored {}; unavailable {} ({}); same_ms {}; clear \
             {}/{}",
            counts.neither[position],
            share(counts.neither[position], total),
            censored.join(" / "),
            counts.unavailable[position],
            share(counts.unavailable[position], total),
            counts.same_ms[position],
            counts.clear_baseline[position],
            counts.clear_trigger[position]
        );
    }
    println!("reach-backs {}", run.labeler.stats().reach_backs);
    println!("wall {:.1} s, peak RSS {}", wall.as_secs_f64(), peak_rss());
}

#[test]
#[ignore = "measures the archive for ADR-055; see the module docs"]
fn measure_labels() {
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
    // 5-minute open interest (ADR-042 D2), as the ADR-054 measurement.
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
    let mut run = Run::new(hourly_and_triggers);
    // D10, as `label_window` builds it.
    println!(
        "provenance {}",
        LabelProvenance::new(ContextProvenance::new(
            DataVersion::from(&dataset),
            &run.engine
        ))
    );
    while let Some(event) = stream.next_event().unwrap() {
        run.step(event);
    }
    let trailing = stream.trailing_gaps().to_vec();
    println!("trailing gaps {trailing:?}");
    run.finish(&trailing);
    println!("window {from}..{to} ms");
    report(&run, started.elapsed());
    if let Some(expected) = expected_events {
        println!("T0: events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    assert_eq!(
        run.check.mismatches, 0,
        "the model disagrees with the labeler"
    );
    assert!(run.complete(), "not exactly one label per record");
}

#[test]
#[ignore = "labels the contexts of a live capture for ADR-055; see the module docs"]
fn measure_labels_live() {
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
    let (mut mismatches, mut incomplete, mut labels) = (0, 0, 0);
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        let mut run = Run::new(every);
        println!(
            "run {id}: provenance {}",
            LabelProvenance::new(ContextProvenance::new(
                DataVersion::from(replay.dataset()),
                &run.engine
            ))
        );
        let started = Instant::now();
        while let Some(event) = replay.next_event().unwrap() {
            run.step(event);
        }
        run.finish(&[]);
        report(&run, started.elapsed());
        mismatches += run.check.mismatches;
        incomplete += u64::from(!run.complete());
        labels += run.labels;
    }
    println!("live T0: labels {labels}, mismatches {mismatches}, incomplete runs {incomplete}");
    assert_eq!(mismatches, 0, "the model disagrees with the labeler");
    assert_eq!(incomplete, 0, "not exactly one label per record");
}

// ---------------------------------------------------------------------------
// CI twin
// ---------------------------------------------------------------------------

/// The first trade of `tape` at or after `from_ms`, with its index.
fn trade_from(tape: &[MarketEvent], from_ms: i64) -> (usize, Trade) {
    tape.iter()
        .enumerate()
        .find_map(|(index, event)| match event {
            MarketEvent::Trade(trade) if trade.time.as_millis() >= from_ms => Some((index, *trade)),
            _ => None,
        })
        .expect("the tape has a trade there")
}

/// The synthetic tape, its trade ids times four so ids are free between
/// trades, with two additions in canonical order:
///
/// - day 5: two trades in the millisecond of a trade, 400 USDT above and
///   below it, so the open races they resolve set `same_ms`;
/// - day 15: a trades gap delivered after a trade, starting 30 hours
///   before it (a late detection, ADR-048), which censors the open labels
///   from their decisions and reaches back into emitted ones;
///
/// and three trades the engine rejects, each censoring the day of labels
/// then open (ADR-055 D6):
///
/// - day 2: a repeat of a trade right after it, dated at the last accepted
///   trade;
/// - day 7: a trade 40 days ahead (a time jump past `MAX_BARS_PER_EVENT`),
///   which censors at the last accepted trade, not at its own time;
/// - day 11: a trade dated 15 minutes after a decision but delivered five
///   minutes later still (out of order), which censors at its own time,
///   exactly at that decision's 15m window end, so the window stays
///   complete while the later ones are censored.
fn twin_tape() -> Vec<MarketEvent> {
    let mut tape: Vec<MarketEvent> = synthetic_tape()
        .into_iter()
        .map(|event| match event {
            MarketEvent::Trade(trade) => MarketEvent::Trade(Trade {
                trade_id: trade.trade_id * 4,
                ..trade
            }),
            other => other,
        })
        .collect();
    let (index, at) = trade_from(&tape, TAPE_START + 5 * DAY_MS + 3 * HOUR_MS);
    for (step, usdt) in [(1, 400), (2, -400)] {
        tape.insert(
            index + step,
            MarketEvent::Trade(Trade {
                trade_id: at.trade_id + step as u64,
                price: Price::from_units(at.price.units() + usdt * SCALE),
                ..at
            }),
        );
    }
    let (index, at) = trade_from(&tape, TAPE_START + 15 * DAY_MS + 6 * HOUR_MS);
    let late = at.time.as_millis();
    tape.insert(
        index + 1,
        MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: EventTime::from_millis(late - 30 * HOUR_MS),
            end: EventTime::from_millis(late + 1),
            reason: GapReason::LateEvent,
        }),
    );
    assert!(tape.is_sorted(), "the additions keep the canonical order");

    let (index, _) = trade_from(&tape, TAPE_START + 2 * DAY_MS);
    let repeat = tape[index].clone();
    tape.insert(index + 1, repeat);
    // A decision: a trade in a later minute than the trade before it closes
    // a 1m bar, so a baseline record is taken at it.
    let trades: Vec<Trade> = tape
        .iter()
        .filter_map(|event| match event {
            MarketEvent::Trade(trade) if trade.time.as_millis() >= TAPE_START + 11 * DAY_MS => {
                Some(*trade)
            }
            _ => None,
        })
        .take(100)
        .collect();
    let decision = trades
        .windows(2)
        .find(|pair| pair[1].time.as_millis() / MINUTE_MS > pair[0].time.as_millis() / MINUTE_MS)
        .map(|pair| pair[1])
        .expect("a trade that closes a minute");
    let refused = decision.time.as_millis() + 15 * MINUTE_MS;
    let (index, _) = trade_from(&tape, refused + 5 * MINUTE_MS);
    tape.insert(
        index + 1,
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(refused),
            trade_id: decision.trade_id + 1,
            ..decision
        }),
    );
    // Last, so no search above finds the jumped trade.
    let (index, at) = trade_from(&tape, TAPE_START + 7 * DAY_MS);
    tape.insert(
        index + 1,
        MarketEvent::Trade(Trade {
            time: EventTime::from_millis(at.time.as_millis() + 40 * DAY_MS),
            trade_id: at.trade_id + 1,
            ..at
        }),
    );
    tape
}

/// The trailing gaps of a provider whose window ends at `end`: the open
/// interest's, which the labeler never takes, and, with `trades`, the
/// trades stream's from its last trade.
fn trailing_of(tape: &[MarketEvent], end: i64, trades: bool) -> Vec<FeedGap> {
    let last = |stream: Stream| {
        tape.iter()
            .rev()
            .find(|event| event.stream() == stream)
            .expect("an event of the stream")
            .time()
    };
    let gap = |stream: Stream| FeedGap {
        stream,
        start: last(stream),
        end: EventTime::from_millis(end - 1),
        reason: GapReason::MissingData,
    };
    let mut gaps = vec![gap(Stream::OpenInterest)];
    if trades {
        gaps.push(gap(Stream::Trades));
    }
    gaps
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

#[test]
fn model_reproduces_the_labeler_on_a_synthetic_tape() {
    let tape = twin_tape();
    let end = TAPE_START + (TAPE_DAYS + 1) * DAY_MS;

    // A: every label checked; the provider's window ends a day after the
    // tape, so a trailing trades gap from the last trade cuts the last day.
    let trailing = trailing_of(&tape, end, true);
    let started = Instant::now();
    let mut full = Run::new(every);
    for event in tape.iter().cloned() {
        full.step(event);
    }
    full.finish(&trailing);
    report(&full, started.elapsed());

    // B: the archive's selection, so the model holds trades for the checked
    // records only, and no trailing trades gap, so the end of the data
    // cuts the last day.
    let mut hourly = Run::new(hourly_and_triggers);
    for event in tape.iter().cloned() {
        hourly.step(event);
    }
    hourly.finish(&trailing_of(&tape, end, false));
    report_t0(&hourly);

    // `label_window` over the tape emits A's labels; its window holds the
    // jumped trade too.
    let dataset = DatasetVersion::from_hex(&"5b".repeat(32)).unwrap();
    let history = History(tape, dataset.clone());
    let window = ReplayWindow {
        start: EventTime::from_millis(TAPE_START),
        end: EventTime::from_millis(TAPE_START + 60 * DAY_MS),
    };
    let mut digest = Fingerprinter::new();
    let (mut records, mut labels) = (0_u64, 0_u64);
    let (provenance, windowed) = label_window(
        &history,
        window,
        window,
        |_| trailing.clone(),
        |_| records += 1,
        |label| {
            labels += 1;
            digest.write_str(&label.to_string());
        },
    )
    .unwrap();
    println!(
        "label_window: provenance {provenance}, events {}, rejections {}, records {records}, \
         labels {labels}",
        windowed.events, windowed.rejections
    );

    for run in [&full, &hourly] {
        assert_eq!(run.check.mismatches, 0, "{:#?}", run.check.notes);
        assert!(run.complete(), "not exactly one label per record");
        assert_eq!(run.rejections, 3, "the three refused trades");
    }
    assert_eq!(full.checked, full.labels);
    assert!(0 < hourly.checked && hourly.checked < hourly.labels);
    assert_eq!(
        (windowed.events, windowed.rejections, records, labels),
        (full.events, full.rejections, full.records, full.labels)
    );
    assert_eq!(digest.finish(), full.digest.finish());
    assert_eq!(windowed.labels, full.labeler.stats());
    assert_eq!(provenance.context.data, DataVersion::from(&dataset));
    assert_eq!(provenance.labeling, LABELING_V1);

    // Every compared kind occurred, so T0 did not pass vacuously.
    let kinds = &full.kinds;
    let checks = [
        ("upper races", kinds.upper),
        ("lower races", kinds.lower),
        ("neither races", kinds.neither),
        ("unavailable races", kinds.unavailable),
        ("censored races", kinds.censored_races),
        ("same_ms races", kinds.same_ms),
        ("gap censors", kinds.censors[0]),
        ("stale references", kinds.censors[1]),
        ("rejection censors", kinds.censors[2]),
        (
            "end-of-data censors (selection run)",
            hourly.kinds.censors[3],
        ),
        ("touched passages", kinds.touched),
        ("passages not within 1d", kinds.not_within),
        ("censored passages", kinds.censored_passages),
        ("clear horizons", kinds.clear),
        ("reach-backs", full.labeler.stats().reach_backs),
    ];
    for (kind, count) in checks {
        assert!(count > 0, "the tape compared no {kind}");
    }
    assert_eq!(kinds.censors[3], 0, "the trailing trades gap names the cut");
}

#[test]
fn the_model_constants_are_the_adrs() {
    // D3's horizons and D4's multiples, as the model writes them.
    assert_eq!(HORIZON_MS, HORIZONS.map(|horizon| horizon.millis()));
    assert_eq!(MULTIPLE_E5, BARRIER_MULTIPLE_E5);
    for (multiple, horizon) in MULTIPLE_E5.iter().zip(HORIZON_MS) {
        let derived = (std::f64::consts::PI * horizon as f64 / (32.0 * HOUR_MS as f64)).sqrt();
        assert_eq!(*multiple, (derived * 1e5).round() as i64);
    }
    // D4's rounding: half to even at the price unit, and 0 is no offset.
    let atr = Price::from_units;
    assert_eq!(offset(atr(50_000), 0), Some(4_522)); // 4 522.5
    assert_eq!(offset(atr(150_000), 0), Some(13_568)); // 13 567.5
    assert_eq!(offset(atr(10), 0), Some(1)); // 0.9045
    assert_eq!(offset(atr(5), 0), None); // 0.45225
    // The model's offsets agree with the labeler's on a sweep of ATRs.
    for units in (0..2_000).chain((0..2_000).map(|k| 1_000_000_000 + 7 * k)) {
        for position in 0..5 {
            assert_eq!(
                offset(atr(units), position),
                barrier_offset(atr(units), position).map(Price::units),
                "{units} {position}"
            );
        }
    }
}
