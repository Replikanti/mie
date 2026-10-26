//! The ADR-056 measurement (issue #151; the ADR-055 one of #26 M2/M3 with
//! the labeling v2 barrier scale): an independent model of the ADR-055
//! decision text, D1–D9, with D4 replaced by ADR-056 D4′, run next to the
//! forward labeler under `Labeling::V2`, that checks the labels it emits and
//! reports what ADR-056 *Accept when* asks for.
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
//! - the barriers (ADR-056 D4′): `reference ± b · ATR_H`, `b` = 31 333 per
//!   10⁵ rounded half to even to the price unit, ATR_H the ATR(14) of the
//!   closed bars of H's timeframe after the decision event, unavailable
//!   without a ready ATR_H, at a zero offset or outside the price range.
//!   The model builds its own bars per horizon timeframe from the ADR-031
//!   text (UTC-aligned `[open, open + tf)`, closed by trades-stream events
//!   only, the first bar `partial_start`, a trades gap marking the open bars
//!   it overlaps `feed_gap`, empty bars) and its own Wilder ATR from ADR-033
//!   decisions 2 and 6; it shares no bar or ATR code with the labeler;
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
//! - **Scale cross-check** (asserted): the model's ATR_1h equals the
//!   record's `state.atr` at every record, in readiness and value, and the
//!   labeler's `scale_breaks` is 0 (it is part of the counts T0 compares;
//!   the model has none).
//! - **Prediction** (reported; ADR-056 reads it): per horizon N(H) =
//!   `Neither` ÷ the races that ran (upper + lower + neither), next to the
//!   pre-registered N(1h) = 10.05 % (v1's measured 1h share) and v1's
//!   published share, and max_H N(H) ÷ min_H N(H) against the miss bound
//!   1.62; the standard error of its log from a circular block bootstrap
//!   over the UTC days of the decisions (28-day blocks, 10 000 resamples,
//!   seed 151, at most 2.3 % of the resamples undefined) and whether the
//!   log-spread lies outside two of them; the miss (both); and the shape of
//!   N over the horizons, each of its four steps a rise or a fall only
//!   outside two bootstrap SEs of its own log-difference and flat
//!   otherwise, with the reason ADR-056 *Reading of a miss* names for it
//!   when the flat steps alone stay within 1.62. The 1h `Neither` and
//!   `Unavailable` counts are printed for the comparison with ADR-055's
//!   published ones (the 1h identity).
//! - Recorded only, over every label: per horizon the `Neither` share of
//!   all labels, the shares of windows censored per reason, the unavailable
//!   races, the `same_ms` races and the clear records per stream kind; the
//!   reach-backs and the scale breaks; the wall time and the peak RSS.
//!   Deliberately not reported: which barrier came first, excursion or time
//!   quantiles, and any split by trigger, regime or location (ADR-055
//!   restriction, kept by ADR-056 *Accept when*).
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
//! `MIE_MEASURE_EVENTS` asserts the replay's event count. ADR-056's
//! acceptance run, like ADR-055's M3, runs the #12 development year,
//! 2025-10-01 … 2026-09-30, with `MIE_MEASURE_EVENTS=603323345` (ADR-051
//! R0's count of the same replay).
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
//! compared kind occurred, so T0 cannot pass vacuously, that every horizon
//! has races with a ready scale (1d from day 15, after 15 daily bars), that
//! the scale cross-check held at every record, that the bootstrap's days
//! hold exactly the counts N(H) is read from, and that `label_window` over
//! the same tape emits the same labels. The bootstrap itself (its
//! generator, its seed, its width under serial dependence and its limit on
//! undefined resamples), the miss rule, the step bands and the shape cases
//! have their own tests.

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
    HORIZONS, HorizonLabel, LABELING_V2, LabelProvenance, LabelStats, Labeling, Passage,
    PassageLabel, Race, SCALE_MULTIPLE_E5, barrier_offset,
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

/// ADR-055 D4 (v1): `b_H = √(π · H / 32 h)` per 10⁵, per horizon. The model
/// labels v2; the v1 multiples stay for the constants check.
const MULTIPLE_E5: [i64; 5] = [9_045, 15_666, 31_333, 62_666, 153_499];

/// ADR-056 D4′: `b = √(π / 32)` per 10⁵, at every horizon.
const SCALE_E5: i64 = 31_333;

/// ADR-033 decision 2: the ATR length.
const ATR_LENGTH: i128 = 14;

/// D8: the passages are read in the 1d window, the last horizon.
const PASSAGE: usize = 4;

/// The null prediction of the `Neither` share, in percent: under ADR-056
/// D4′ the mean exit time is H / 4 at every H, as under ADR-055 D4.
const NULL_NEITHER_PERCENT: f64 = 0.92;

/// ADR-056 *Accept when*: the predicted N(H), `Neither` over the races that
/// ran, in percent, at every H: v1's measured 1h share (ADR-055
/// *Measurement*).
const PREDICTED_NEITHER_PERCENT: f64 = 10.05;

/// ADR-056 *Accept when*: max_H N(H) ÷ min_H N(H) above this misses, if
/// the spread is also outside the bootstrap's noise band.
const MISS_RATIO: f64 = 1.62;

/// ADR-056 *Accept when*: the circular block bootstrap of the spread over
/// UTC days: the block length in days (two ATR_1d memories of 14 days),
/// the resamples, the seed (named before the run), and the width in
/// standard errors of the spread's band and of each step's band.
const BLOCK_DAYS: usize = 28;
const RESAMPLES: usize = 10_000;
const SEED: u64 = 151;
const BAND_SE: f64 = 2.0;

/// ADR-056 *Accept when*: at most this share of the resamples, per mille,
/// may have an undefined log-spread (no race or no `Neither` at a horizon
/// of the pair) for the SE to be taken over the rest: 2.3 %, the one-sided
/// normal tail beyond the band's two SE.
const MAX_UNDEFINED_PER_MILLE: usize = 23;

/// ADR-055 *Measurement*: v1's `Neither` share of all labels in the year,
/// per horizon, in percent.
const V1_NEITHER_PERCENT: [f64; 5] = [16.338, 12.548, 10.036, 9.281, 6.191];

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

/// D4 / D4′: `multiple_e5 / 10⁵ · scale`, rounded half to even to the price
/// unit; `None` at 0.
fn offset(scale: Price, multiple_e5: i64) -> Option<i64> {
    let product = i128::from(scale.units()) * i128::from(multiple_e5);
    let (quotient, remainder) = (product.div_euclid(100_000), product.rem_euclid(100_000));
    let half_up = 2 * remainder > 100_000 || (2 * remainder == 100_000 && quotient % 2 != 0);
    i64::try_from(quotient + i128::from(half_up))
        .ok()
        .filter(|offset| *offset > 0)
}

/// D4′: the barriers `reference ± b · ATR_H`; `None` without a reference or
/// a ready ATR_H, at a zero offset or outside the price range.
fn barriers(reference: Option<Price>, scale: Option<Price>) -> Option<Barriers> {
    let reference = reference?.units();
    let offset = offset(scale?, SCALE_E5)?;
    Some(Barriers {
        lower: Price::from_units(reference.checked_sub(offset)?),
        upper: Price::from_units(reference.checked_add(offset)?),
    })
}

/// `numerator / denominator` rounded half to even; `denominator > 0`.
fn half_even(numerator: i128, denominator: i128) -> i128 {
    let (quotient, remainder) = (
        numerator.div_euclid(denominator),
        numerator.rem_euclid(denominator),
    );
    let up = 2 * remainder > denominator || (2 * remainder == denominator && quotient % 2 != 0);
    quotient + i128::from(up)
}

/// One bar of the model's own builder (ADR-031): its open time, its high,
/// low and close (none for an interval without trades), and its coverage.
#[derive(Debug, Clone, Copy)]
struct ModelBar {
    open: i64,
    hlc: Option<(i64, i64, i64)>,
    partial_start: bool,
    feed_gap: bool,
}

/// ADR-033 decisions 2 and 6, from the text: ATR(14) of one bar series.
#[derive(Debug, Clone, Copy, Default)]
struct ModelAtr {
    /// The previous close.
    anchor: Option<i64>,
    /// Samples since the series (re)started.
    samples: i128,
    /// The sum of the first true ranges.
    sum: i128,
    /// The ATR, from the 14th sample on.
    atr: Option<i128>,
}

impl ModelAtr {
    /// Decision 6's table, then decision 2's true range and Wilder step.
    fn close(&mut self, bar: &ModelBar) {
        let complete = !bar.partial_start && !bar.feed_gap;
        let range = match (bar.hlc, complete, self.anchor) {
            // Empty, incomplete: a break, back to warming up.
            (None, false, _) => {
                *self = Self::default();
                return;
            }
            // Trades, incomplete, no anchor: anchors only.
            (Some((_, _, close)), false, None) => {
                self.anchor = Some(close);
                return;
            }
            // Empty, complete, no anchor: skipped.
            (None, true, None) => return,
            // Empty, complete, anchor: TR 0, the anchor stays.
            (None, true, Some(_)) => 0,
            // Trades, no anchor (complete): TR = H − L.
            (Some((high, low, _)), _, None) => i128::from(high) - i128::from(low),
            // Trades, anchor: TR with the previous close.
            (Some((high, low, _)), _, Some(previous)) => {
                let (high, low, previous) =
                    (i128::from(high), i128::from(low), i128::from(previous));
                (high - low)
                    .max((high - previous).abs())
                    .max((low - previous).abs())
            }
        };
        if let Some((_, _, close)) = bar.hlc {
            self.anchor = Some(close);
        }
        self.samples += 1;
        self.atr = match self.atr {
            Some(atr) => Some(half_even((ATR_LENGTH - 1) * atr + range, ATR_LENGTH)),
            None => {
                self.sum += range;
                (self.samples == ATR_LENGTH).then(|| half_even(self.sum, ATR_LENGTH))
            }
        };
    }

    /// The ATR as a price, once ready.
    fn ready(&self) -> Option<Price> {
        self.atr
            .map(|atr| Price::from_units(i64::try_from(atr).expect("an ATR in the price range")))
    }
}

/// ADR-031, from the text: the bars of one timeframe, and their ATR.
#[derive(Debug, Clone, Copy)]
struct ModelSeries {
    /// The timeframe, in ms.
    tf: i64,
    /// The bar the next trade goes to; none before the first trades-stream
    /// event.
    developing: Option<ModelBar>,
    atr: ModelAtr,
}

impl ModelSeries {
    fn new(tf: i64) -> Self {
        Self {
            tf,
            developing: None,
            atr: ModelAtr::default(),
        }
    }

    /// Decision 6: whether a gap `[start, end]` overlaps the bar at `open`.
    fn overlaps(&self, open: i64, gap: Option<(i64, i64)>) -> bool {
        gap.is_some_and(|(start, end)| open <= end && start < open + self.tf)
    }

    /// A trades-stream event at `time` (a trade's time or a gap's end):
    /// closes every bar ending at or before it (decision 4), each into the
    /// ATR, marking every open bar a gap overlaps (decision 6), and adds a
    /// trade's price to the bar containing it.
    fn event(&mut self, time: i64, gap: Option<(i64, i64)>, price: Option<i64>) {
        let mut bar = self.developing.unwrap_or(ModelBar {
            open: time.div_euclid(self.tf) * self.tf,
            hlc: None,
            partial_start: true,
            feed_gap: false,
        });
        bar.feed_gap |= self.overlaps(bar.open, gap);
        while bar.open + self.tf <= time {
            self.atr.close(&bar);
            let open = bar.open + self.tf;
            bar = ModelBar {
                open,
                hlc: None,
                partial_start: false,
                feed_gap: self.overlaps(open, gap),
            };
        }
        if let Some(price) = price {
            bar.hlc = Some(match bar.hlc {
                None => (price, price, price),
                Some((high, low, _)) => (high.max(price), low.min(price), price),
            });
        }
        self.developing = Some(bar);
    }
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
    /// The record's ATR(1h), which the label carries.
    atr: Option<Price>,
    /// D4′: ATR_H per horizon after the decision event, from the model's
    /// own bars.
    scales: [Option<Price>; 5],
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
    /// D4′: the bars and the ATR of each horizon's timeframe, fed with every
    /// accepted event.
    series: [ModelSeries; 5],
    /// The scale cross-check: records whose `state.atr` differs from the
    /// model's ATR_1h, in readiness or value, a few of them noted.
    scale_mismatches: u64,
    scale_notes: Vec<String>,
    /// Records whose ATR_1h the cross-check compared while ready.
    scale_ready: u64,
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
            series: HORIZON_MS.map(ModelSeries::new),
            scale_mismatches: 0,
            scale_notes: Vec::new(),
            scale_ready: 0,
        }
    }

    /// An accepted event, then the labels it emits, in record order.
    fn accepted(&mut self, event: &MarketEvent, out: &mut Vec<Expected>) {
        let key = event.canonical_key();
        let time = key.time.as_millis();
        // ADR-031 decision 4: only trades-stream events reach the bars.
        let bar_input = match event {
            MarketEvent::Trade(trade) => Some((time, None, Some(trade.price.units()))),
            MarketEvent::FeedGap(gap) if gap.stream == Stream::Trades => Some((
                gap.end.as_millis(),
                Some((gap.start.as_millis(), gap.end.as_millis())),
                None,
            )),
            _ => None,
        };
        if let Some((at, gap, price)) = bar_input {
            for series in &mut self.series {
                series.event(at, gap, price);
            }
        }
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
        let scales = self.series.map(|series| series.atr.ready());
        // The scale cross-check: the 1h scale is the engine's ATR(1h).
        let atr = record.state.atr.ready().copied();
        if scales[2] != atr {
            self.scale_mismatches += 1;
            if self.scale_notes.len() < 20 {
                self.scale_notes.push(format!(
                    "{event}: the model's ATR_1h {:?}, the record's {:?}",
                    scales[2], atr
                ));
            }
        } else if atr.is_some() {
            self.scale_ready += 1;
        }
        self.pending.push_back(Pending {
            event,
            ordinal,
            baseline,
            reference,
            atr,
            scales,
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
            std::array::from_fn(|position| barriers(pending.reference, pending.scales[position]));
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
    /// Races that ran: upper, lower or neither.
    ran: [u64; 5],
    unavailable: [u64; 5],
    same_ms: [u64; 5],
    /// Windows censored, per reason in [`REASONS`] order.
    censored: [[u64; 4]; 5],
    clear_baseline: [u64; 5],
    clear_trigger: [u64; 5],
}

/// The same `Neither` and races-that-ran counts per UTC day of the
/// decision, from the first label's day on, empty days included: the units
/// the bootstrap resamples (ADR-056 *Accept when*).
#[derive(Debug, Default)]
struct Daily {
    first: Option<i64>,
    neither: Vec<[u64; 5]>,
    ran: Vec<[u64; 5]>,
}

impl Daily {
    /// The row of the day of `decision`; labels come in record order, so
    /// in decision order.
    fn row(&mut self, decision: i64) -> usize {
        let day = decision.div_euclid(DAY_MS);
        let first = *self.first.get_or_insert(day);
        let row = usize::try_from(day - first).expect("labels come in decision order");
        if row >= self.ran.len() {
            self.neither.resize(row + 1, [0; 5]);
            self.ran.resize(row + 1, [0; 5]);
        }
        row
    }
}

/// SplitMix64 (Steele, Lea and Flood 2014): a fully specified generator,
/// so the seed fixes every resample.
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform in `0..n` from the high half of a 128-bit product (bias
    /// below n / 2⁶⁴).
    fn below(&mut self, n: usize) -> usize {
        ((u128::from(self.next()) * n as u128) >> 64) as usize
    }
}

/// The bootstrap's standard error of one log-spread, and how many of its
/// resamples left the log-spread undefined.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Band {
    /// `None` when more than [`MAX_UNDEFINED_PER_MILLE`] of the resamples
    /// are undefined, or fewer than two are defined.
    se: Option<f64>,
    undefined: usize,
}

/// ADR-056 *Accept when*: the standard error of the log-spread
/// ln(N(hi) ÷ N(lo)) between the horizons at `pair`, from a circular block
/// bootstrap (Politis and Romano 1992) over the days of `daily`: each
/// resample joins blocks of `block` consecutive days, wrapping at the end,
/// from uniform starts until it holds as many days as the run, and sums
/// both horizons over the same days. A resample with no race or no
/// `Neither` at either horizon has no log-spread; the SE is taken over the
/// others while they are at most [`MAX_UNDEFINED_PER_MILLE`] of all. The
/// draws depend on the seed and the number of days only, so every pair of
/// one run sees the same resamples.
fn bootstrap_se(
    daily: &Daily,
    pair: (usize, usize),
    block: usize,
    resamples: usize,
    seed: u64,
) -> Band {
    let days = daily.ran.len();
    if days == 0 || block == 0 || resamples < 2 {
        return Band {
            se: None,
            undefined: resamples,
        };
    }
    let mut rng = SplitMix64(seed);
    let mut spreads = Vec::with_capacity(resamples);
    let mut undefined = 0;
    for _ in 0..resamples {
        let mut sums = [0_u64; 4];
        let mut filled = 0;
        while filled < days {
            let start = rng.below(days);
            let take = block.min(days - filled);
            for k in 0..take {
                let day = (start + k) % days;
                sums[0] += daily.neither[day][pair.0];
                sums[1] += daily.ran[day][pair.0];
                sums[2] += daily.neither[day][pair.1];
                sums[3] += daily.ran[day][pair.1];
            }
            filled += take;
        }
        if sums.contains(&0) {
            undefined += 1;
            continue;
        }
        let share = |neither: u64, ran: u64| neither as f64 / ran as f64;
        spreads.push((share(sums[0], sums[1]) / share(sums[2], sums[3])).ln());
    }
    let defined = spreads.len();
    if !bootstrap_limit_holds(undefined, resamples) || defined < 2 {
        return Band {
            se: None,
            undefined,
        };
    }
    let mean = spreads.iter().sum::<f64>() / defined as f64;
    let squares: f64 = spreads.iter().map(|s| (s - mean) * (s - mean)).sum();
    Band {
        se: Some((squares / (defined - 1) as f64).sqrt()),
        undefined,
    }
}

/// ADR-056 *Accept when*: at most [`MAX_UNDEFINED_PER_MILLE`] of the
/// `resamples` are undefined.
fn bootstrap_limit_holds(undefined: usize, resamples: usize) -> bool {
    undefined * 1000 <= resamples * MAX_UNDEFINED_PER_MILLE
}

/// ADR-056 *Accept when*: a log-difference lies outside its band when it
/// exceeds [`BAND_SE`] standard errors; an undefined SE (more undefined
/// resamples than the limit) counts as outside.
fn outside(log_difference: f64, se: Option<f64>) -> bool {
    se.is_none_or(|se| log_difference.abs() > BAND_SE * se)
}

/// ADR-056 *Miss criterion*: not accepted when max ÷ min exceeds
/// [`MISS_RATIO`] and its log lies outside the spread's band; either
/// alone does not miss.
fn miss(ratio: f64, se: Option<f64>) -> bool {
    ratio > MISS_RATIO && outside(ratio.ln(), se)
}

/// One step of N between adjacent horizons, read against its own band.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Step {
    Rise,
    Fall,
    Flat,
}

impl Step {
    /// ADR-056 *Reading of a miss*: the step from N = `from` to N = `to`
    /// rises or falls only outside its band, else it is flat. Equal shares
    /// (both 0 included) are flat; a step from or to 0 has an infinite log
    /// and is read by its sign against the band.
    fn of(from: f64, to: f64, se: Option<f64>) -> Self {
        if from == to {
            return Self::Flat;
        }
        let log_difference = to.ln() - from.ln();
        if !outside(log_difference, se) {
            Self::Flat
        } else if log_difference > 0.0 {
            Self::Rise
        } else {
            Self::Fall
        }
    }

    fn letter(self) -> char {
        match self {
            Self::Rise => 'U',
            Self::Fall => 'D',
            Self::Flat => 'F',
        }
    }
}

/// The four steps of N over the horizons, shortest first, each against
/// the SE of its own log-difference.
fn steps(shares: &[f64; 5], ses: &[Option<f64>; 4]) -> [Step; 4] {
    std::array::from_fn(|i| Step::of(shares[i], shares[i + 1], ses[i]))
}

/// ADR-056 *Reading of a miss*: the case no reading is pre-registered for.
const OTHER: &str = "other: no pre-registered reading";

/// ADR-056 *Reading of a miss*: the reading the four steps of N, 5m→15m
/// to 4h→1d, name. Each case is a set of step sequences, the sets are
/// disjoint, and every sequence no case names is [`OTHER`].
fn shape(steps: [Step; 4]) -> &'static str {
    use Step::{Fall, Flat, Rise};
    let [s0, s1, s2, s3] = steps;
    let no_rise = !steps.contains(&Rise);
    let no_fall = !steps.contains(&Fall);
    let at = |step: Step| (0..4).filter(move |&i| steps[i] == step);
    // 24 h seasonality: 5m→15m flat, rises only into 1h or 4h (steps 1, 2),
    // falls only out of them (steps 2, 3), at least one of each, every
    // rise before every fall.
    let day_cycle = s0 == Flat
        && !no_rise
        && !no_fall
        && at(Rise).all(|i| i == 1 || i == 2)
        && at(Fall).all(|i| i >= 2)
        && at(Rise).max() < at(Fall).min();
    // Jumps: falls from 5m on, then only flat steps; a fall after a flat
    // step is not a jump excess, which only shrinks with H.
    let falls_from_5m = steps
        .iter()
        .skip_while(|&&step| step == Fall)
        .all(|&step| step == Flat);
    if s0 == Fall && falls_from_5m {
        "fall from 5m: jumps"
    } else if no_fall && s1 == Rise && (s0 == Rise || s3 == Rise) {
        "rise below 1h: finite volatility memory"
    } else if [s0, s1] == [Flat, Flat] && s2 != Fall && s3 == Rise {
        "rise above 1h only: the week"
    } else if steps == [Rise, Fall, Flat, Flat] {
        "peak at 15m: intraday sessions"
    } else if day_cycle {
        "peak at 1h or 4h: 24 h seasonality"
    } else if s0 == Fall && s1 == Rise && s2 != Fall && s3 == Rise {
        "fall from 5m, rise below 1h: jumps and finite volatility memory"
    } else if s0 == Fall && s1 != Rise && s3 == Rise {
        "fall from 5m, rise above 1h only: jumps and the week"
    } else {
        OTHER
    }
}

/// ADR-056 *Reading of a miss*: ln(max ÷ min) of the series rebuilt from
/// the flat steps alone, every step that cleared its band set to 0 in log.
/// Equal shares (both 0 included) add 0; any other flat step joins two
/// positive shares, since a step from or to 0 is never flat.
fn flat_spread(shares: &[f64; 5], steps: [Step; 4]) -> f64 {
    let (mut level, mut low, mut high) = (0.0_f64, 0.0_f64, 0.0_f64);
    for (i, step) in steps.into_iter().enumerate() {
        if step == Step::Flat && shares[i] != shares[i + 1] {
            level += shares[i + 1].ln() - shares[i].ln();
        }
        low = low.min(level);
        high = high.max(level);
    }
    high - low
}

/// ADR-056 *Reading of a miss*: the case of the steps names its reading
/// only when the miss lies in the steps that cleared their bands, i.e. the
/// flat steps alone stay within [`MISS_RATIO`]; otherwise [`OTHER`].
fn reading(shares: &[f64; 5], steps: [Step; 4]) -> &'static str {
    if flat_spread(shares, steps) > MISS_RATIO.ln() {
        OTHER
    } else {
        shape(steps)
    }
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
    /// Per horizon, races with a ready scale, and the earliest decision of
    /// one.
    available: [u64; 5],
    first_available: [Option<i64>; 5],
}

impl Kinds {
    fn add(&mut self, label: &ForwardLabel) {
        for (position, horizon) in label.horizons.iter().enumerate() {
            if horizon.barriers.is_some() {
                self.available[position] += 1;
                let decision = label.event.time.as_millis();
                let first = &mut self.first_available[position];
                *first = Some(first.map_or(decision, |first| first.min(decision)));
            }
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
    daily: Daily,
    kinds: Kinds,
    /// The digest of every label's line, in order.
    digest: Fingerprinter,
}

impl Run {
    fn new(pick: fn(&ContextRecord) -> bool) -> Self {
        Self {
            engine: MarketStateEngine::new(),
            sampler: ContextSampler::new(None),
            labeler: ForwardLabeler::new(Labeling::V2),
            model: Model::new(pick),
            check: Check::default(),
            events: 0,
            rejections: 0,
            records: 0,
            labels: 0,
            checked: 0,
            counts: Counts::default(),
            daily: Daily::default(),
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
        let daily = &mut self.daily;
        let day = daily.row(label.event.time.as_millis());
        for (position, horizon) in label.horizons.iter().enumerate() {
            match horizon.race {
                Race::Neither => {
                    counts.neither[position] += 1;
                    counts.ran[position] += 1;
                    daily.neither[day][position] += 1;
                    daily.ran[day][position] += 1;
                }
                Race::Unavailable => counts.unavailable[position] += 1,
                Race::Upper { same_ms, .. } | Race::Lower { same_ms, .. } => {
                    counts.same_ms[position] += u64::from(same_ms);
                    counts.ran[position] += 1;
                    daily.ran[day][position] += 1;
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

    /// The scale cross-check (ADR-056 *Accept when*): the model's ATR_1h is
    /// every record's `state.atr`, and the labeler's scales never broke.
    fn scales_hold(&self) -> bool {
        self.model.scale_mismatches == 0 && self.labeler.stats().scale_breaks == 0
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
    println!(
        "scale cross-check: ATR_1h mismatches {} (ready and equal at {} records); scale breaks \
         {}; holds {}",
        run.model.scale_mismatches,
        run.model.scale_ready,
        run.labeler.stats().scale_breaks,
        run.scales_hold()
    );
    for note in &run.model.scale_notes {
        println!("scale mismatch: {note}");
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
    println!(
        "reach-backs {}, scale breaks {}",
        run.labeler.stats().reach_backs,
        run.labeler.stats().scale_breaks
    );
    println!(
        "1h identity: 1h neither {} and unavailable {}; ADR-055 v1 published 56 372 and 899 \
         (year), 49 and 847 (live store)",
        counts.neither[2], counts.unavailable[2]
    );
    println!(
        "prediction: N(H) = neither / races that ran (upper + lower + neither) = \
         {PREDICTED_NEITHER_PERCENT} % at every H (v1's 1h); miss when max / min > {MISS_RATIO}"
    );
    let mut shares = [None; 5];
    for (position, horizon) in HORIZONS.iter().enumerate() {
        let (neither, ran) = (counts.neither[position], counts.ran[position]);
        println!(
            "  {horizon}: N {} = {neither} of {ran} races that ran (predicted \
             {PREDICTED_NEITHER_PERCENT} %); v1's share of all labels {} %",
            share(neither, ran),
            V1_NEITHER_PERCENT[position]
        );
        if ran > 0 {
            shares[position] = Some(neither as f64 / ran as f64);
        }
    }
    let ran: Vec<(usize, f64)> = shares
        .iter()
        .enumerate()
        .filter_map(|(position, share)| share.map(|share| (position, share)))
        .collect();
    let (Some(hi), Some(lo)) = (
        ran.iter()
            .copied()
            .reduce(|a, b| if b.1 > a.1 { b } else { a }),
        ran.iter()
            .copied()
            .reduce(|a, b| if b.1 < a.1 { b } else { a }),
    ) else {
        println!("spread: no horizon with races");
        println!("wall {:.1} s, peak RSS {}", wall.as_secs_f64(), peak_rss());
        return;
    };
    let ratio = hi.1 / lo.1;
    println!(
        "spread: max / min N(H) over the {} horizons with races = {ratio:.4} ({} / {}; miss \
         bound {MISS_RATIO}; ADR-056 evaluates it on the year only)",
        ran.len(),
        HORIZONS[hi.0],
        HORIZONS[lo.0]
    );
    let band = bootstrap_se(&run.daily, (hi.0, lo.0), BLOCK_DAYS, RESAMPLES, SEED);
    let se_text = |band: Band| {
        let se = band
            .se
            .map_or_else(|| "undefined".to_owned(), |se| format!("{se:.4}"));
        format!("{se} ({} undefined resamples)", band.undefined)
    };
    println!(
        "noise band: circular block bootstrap over {} UTC days, block {BLOCK_DAYS} d, \
         {RESAMPLES} resamples, seed {SEED}, at most {MAX_UNDEFINED_PER_MILLE} per mille \
         undefined: SE of ln(max / min) {}; ln(max / min) {:.4}; outside the {BAND_SE}-SE band \
         {}; miss {}",
        run.daily.ran.len(),
        se_text(band),
        ratio.ln(),
        outside(ratio.ln(), band.se),
        miss(ratio, band.se)
    );
    if let [Some(a), Some(b), Some(c), Some(d), Some(e)] = shares {
        let shares = [a, b, c, d, e];
        // The same resamples for every step: the draws depend on the seed
        // and the days only.
        let bands: [Band; 4] = std::array::from_fn(|i| {
            bootstrap_se(&run.daily, (i + 1, i), BLOCK_DAYS, RESAMPLES, SEED)
        });
        let steps = steps(&shares, &bands.map(|band| band.se));
        for (i, step) in steps.iter().enumerate() {
            println!(
                "  step {} -> {}: ln {:.4}, SE {}: {}",
                HORIZONS[i],
                HORIZONS[i + 1],
                shares[i + 1].ln() - shares[i].ln(),
                se_text(bands[i]),
                step.letter()
            );
        }
        let letters: String = steps.iter().map(|step| step.letter()).collect();
        println!(
            "flat steps alone: ln(max / min) {:.4}; shape: {letters}: {}",
            flat_spread(&shares, steps),
            reading(&shares, steps)
        );
    }
    println!("wall {:.1} s, peak RSS {}", wall.as_secs_f64(), peak_rss());
}

#[test]
#[ignore = "measures the archive for ADR-056; see the module docs"]
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
        LabelProvenance::new(
            ContextProvenance::new(DataVersion::from(&dataset), &run.engine),
            Labeling::V2
        )
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
    assert!(run.scales_hold(), "the scale cross-check failed");
}

#[test]
#[ignore = "labels the contexts of a live capture for ADR-056; see the module docs"]
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
    let (mut mismatches, mut incomplete, mut labels, mut scales) = (0, 0, 0, 0);
    for journaled in &runs {
        let id = &journaled.parameters.run_id;
        let Ok(mut replay) = live.run_replay(id) else {
            println!("run {id}: not replayable, skipped");
            continue;
        };
        let mut run = Run::new(every);
        println!(
            "run {id}: provenance {}",
            LabelProvenance::new(
                ContextProvenance::new(DataVersion::from(replay.dataset()), &run.engine),
                Labeling::V2
            )
        );
        let started = Instant::now();
        while let Some(event) = replay.next_event().unwrap() {
            run.step(event);
        }
        run.finish(&[]);
        report(&run, started.elapsed());
        mismatches += run.check.mismatches;
        incomplete += u64::from(!run.complete());
        scales += u64::from(!run.scales_hold());
        labels += run.labels;
    }
    println!(
        "live T0: labels {labels}, mismatches {mismatches}, incomplete runs {incomplete}, runs \
         failing the scale cross-check {scales}"
    );
    assert_eq!(mismatches, 0, "the model disagrees with the labeler");
    assert_eq!(incomplete, 0, "not exactly one label per record");
    assert_eq!(scales, 0, "the scale cross-check failed");
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
        Labeling::V2,
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
        assert!(run.scales_hold(), "{:#?}", run.model.scale_notes);
        assert!(run.model.scale_ready > 1_000, "{}", run.model.scale_ready);
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
    assert_eq!(provenance.labeling, LABELING_V2);

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
    // Every horizon has races with a ready scale (ADR-056 D4′); 1d from
    // day 15, once the partial first day and 14 daily samples have closed.
    for (position, horizon) in HORIZONS.iter().enumerate() {
        assert!(kinds.available[position] > 0, "no {horizon} race ran");
    }
    let first_daily = kinds.first_available[4].expect("a 1d race");
    assert!(
        (TAPE_START + 15 * DAY_MS..TAPE_START + 16 * DAY_MS).contains(&first_daily),
        "{first_daily}"
    );
    assert_eq!(kinds.censors[3], 0, "the trailing trades gap names the cut");
    // The bootstrap's days hold exactly the counts N(H) is read from, over
    // every day of the tape.
    for position in 0..5 {
        let sum = |rows: &[[u64; 5]]| rows.iter().map(|row| row[position]).sum::<u64>();
        assert_eq!(sum(&full.daily.neither), full.counts.neither[position]);
        assert_eq!(sum(&full.daily.ran), full.counts.ran[position]);
    }
    assert_eq!(full.daily.ran.len(), TAPE_DAYS as usize + 1);
}

#[test]
fn the_model_constants_are_the_adrs() {
    // D3's horizons and ADR-055 D4's multiples, as the model writes them.
    assert_eq!(HORIZON_MS, HORIZONS.map(|horizon| horizon.millis()));
    assert_eq!(MULTIPLE_E5, BARRIER_MULTIPLE_E5);
    for (multiple, horizon) in MULTIPLE_E5.iter().zip(HORIZON_MS) {
        let derived = (std::f64::consts::PI * horizon as f64 / (32.0 * HOUR_MS as f64)).sqrt();
        assert_eq!(*multiple, (derived * 1e5).round() as i64);
    }
    // ADR-056 D4′: b = √(π / 32) at every horizon, v1's 1h multiple.
    assert_eq!(SCALE_E5, SCALE_MULTIPLE_E5);
    assert_eq!(
        SCALE_E5,
        ((std::f64::consts::PI / 32.0).sqrt() * 1e5).round() as i64
    );
    assert_eq!(SCALE_E5, MULTIPLE_E5[2]);
    // The prediction is v1's 1h share over the races that ran (ADR-055
    // *Measurement*: 56 372 neither, 899 unavailable and at most 62
    // censored of 561 695), and the miss bound is √(16.338 / 6.191) rounded
    // down.
    let ran = |censored: f64| 561_695.0 - 899.0 - censored;
    for censored in [0.0, 62.0] {
        let share = 100.0 * 56_372.0 / ran(censored);
        assert!((share - PREDICTED_NEITHER_PERCENT).abs() < 0.005, "{share}");
    }
    let spread = (V1_NEITHER_PERCENT[0] / V1_NEITHER_PERCENT[4]).sqrt();
    assert_eq!((spread * 100.0).floor() / 100.0, MISS_RATIO);
    // The rounding: half to even at the price unit, and 0 is no offset.
    let atr = Price::from_units;
    assert_eq!(offset(atr(50_000), MULTIPLE_E5[0]), Some(4_522)); // 4 522.5
    assert_eq!(offset(atr(150_000), MULTIPLE_E5[0]), Some(13_568)); // 13 567.5
    assert_eq!(offset(atr(10), MULTIPLE_E5[0]), Some(1)); // 0.9045
    assert_eq!(offset(atr(5), MULTIPLE_E5[0]), None); // 0.45225
    assert_eq!(offset(atr(50_000), SCALE_E5), Some(15_666)); // 15 666.5
    assert_eq!(offset(atr(150_000), SCALE_E5), Some(47_000)); // 46 999.5
    // The model's offsets agree with the labeler's on a sweep of scales.
    for units in (0..2_000).chain((0..2_000).map(|k| 1_000_000_000 + 7 * k)) {
        for multiple in MULTIPLE_E5.into_iter().chain([SCALE_E5]) {
            assert_eq!(
                offset(atr(units), multiple),
                barrier_offset(atr(units), multiple).map(Price::units),
                "{units} {multiple}"
            );
        }
    }
}

#[test]
fn the_model_atr_follows_adr_033() {
    // Decision 6's table on 1h bars, decision 2's seed and Wilder step.
    let bar = |hlc: Option<(i64, i64, i64)>, partial_start, feed_gap| ModelBar {
        open: 0,
        hlc,
        partial_start,
        feed_gap,
    };
    let mut atr = ModelAtr::default();
    // Trades, incomplete, no anchor: anchors only.
    atr.close(&bar(Some((110, 90, 105)), true, false));
    assert_eq!((atr.samples, atr.anchor), (0, Some(105)));
    // Trades, complete, anchor: TR with the previous close (130 − 105).
    atr.close(&bar(Some((130, 120, 125)), false, false));
    assert_eq!((atr.samples, atr.sum), (1, 25));
    // Empty, complete, anchor: TR 0, the anchor stays.
    atr.close(&bar(None, false, false));
    assert_eq!((atr.samples, atr.sum, atr.anchor), (2, 25, Some(125)));
    // Empty, incomplete: a break.
    atr.close(&bar(None, false, true));
    assert_eq!((atr.samples, atr.anchor, atr.atr), (0, None, None));
    // Empty, complete, no anchor: skipped.
    atr.close(&bar(None, false, false));
    assert_eq!(atr.samples, 0);
    // Trades, complete, no anchor: TR = H − L; 14 of them seed the mean.
    for _ in 0..14 {
        atr.close(&bar(Some((1_014, 1_000, 1_000)), false, false));
    }
    assert_eq!(atr.ready(), Some(Price::from_units(14)));
    // (13 · 14 + 21) / 14 = 14.5: a tie, to the even 14.
    atr.close(&bar(Some((1_021, 1_000, 1_000)), false, false));
    assert_eq!(atr.ready(), Some(Price::from_units(14)));
    // The bars: the first is partial, a gap marks the open bars it overlaps,
    // and every elapsed interval closes.
    let mut series = ModelSeries::new(5 * MINUTE_MS);
    series.event(7 * MINUTE_MS, None, Some(100));
    let first = series.developing.unwrap();
    assert_eq!((first.open, first.partial_start), (5 * MINUTE_MS, true));
    series.event(21 * MINUTE_MS, Some((8 * MINUTE_MS, 21 * MINUTE_MS)), None);
    let developing = series.developing.unwrap();
    assert_eq!(developing.open, 20 * MINUTE_MS);
    assert!(developing.feed_gap && developing.hlc.is_none());
    // [5, 10) anchored; [10, 15) and [15, 20) were empty and gapped: breaks.
    assert_eq!((series.atr.samples, series.atr.anchor), (0, None));
}

/// `days` of `ran` races per horizon, `neither_of(day, horizon)` of them
/// `Neither`.
fn daily_of(days: usize, ran: u64, neither_of: impl Fn(usize, usize) -> u64) -> Daily {
    Daily {
        first: Some(0),
        neither: (0..days)
            .map(|day| std::array::from_fn(|horizon| neither_of(day, horizon)))
            .collect(),
        ran: vec![[ran; 5]; days],
    }
}

#[test]
fn the_bootstrap_is_seeded_and_widens_under_serial_dependence() {
    // SplitMix64's published output for seed 0.
    let mut rng = SplitMix64(0);
    assert_eq!(
        [rng.next(), rng.next(), rng.next()],
        [
            0xE220_A839_7B1D_CDAF,
            0x6E78_9E6A_A1B9_65F4,
            0x06C4_5D18_8009_454F
        ]
    );
    // The band's numbers are ADR-056's: two ATR_1d memories per block.
    assert_eq!(BLOCK_DAYS, 2 * ATR_LENGTH as usize);
    assert_eq!((RESAMPLES, SEED, BAND_SE), (10_000, 151, 2.0));

    // 364 days = 13 blocks; horizon 4 is flat at 10 %, horizon 0 is 15 % or
    // 5 % a day, with the same mean 10 % either way.
    let high = |high: bool| if high { 15 } else { 5 };
    let alternating = daily_of(
        364,
        100,
        |day, h| if h == 0 { high(day % 2 == 0) } else { 10 },
    );
    let persistent = daily_of(
        364,
        100,
        |day, h| {
            if h == 0 { high(day / 28 % 2 == 0) } else { 10 }
        },
    );
    let se = |daily: &Daily, seed| bootstrap_se(daily, (0, 4), BLOCK_DAYS, RESAMPLES, seed).se;
    // Every 28-day block of the alternating days holds 14 high days, so
    // every resample is the same: no noise. Runs of 28 high or low days
    // are what a block sees whole, so the persistent days spread.
    let flat = se(&alternating, SEED).unwrap();
    let wide = se(&persistent, SEED).unwrap();
    assert!(flat < 1e-9, "{flat}");
    assert!(wide > 0.04, "{wide}");
    // The seed fixes every resample; another seed draws others.
    assert_eq!(se(&persistent, SEED).unwrap().to_bits(), wide.to_bits());
    assert_ne!(se(&persistent, SEED + 1).unwrap().to_bits(), wide.to_bits());
    // No `Neither` at a horizon of the pair: every resample undefined.
    let none = daily_of(364, 100, |_, h| if h == 0 { 0 } else { 10 });
    let band = bootstrap_se(&none, (0, 4), BLOCK_DAYS, RESAMPLES, SEED);
    assert_eq!(
        band,
        Band {
            se: None,
            undefined: RESAMPLES
        }
    );
    assert_eq!(se(&Daily::default(), SEED), None);
}

#[test]
fn a_few_undefined_resamples_are_dropped_and_more_leave_the_se_undefined() {
    // ADR-056 *Accept when*: at most 2.3 % (230 of 10 000) undefined.
    assert_eq!(MAX_UNDEFINED_PER_MILLE, 23);
    let limit = |undefined| bootstrap_limit_holds(undefined, RESAMPLES);
    assert!(limit(230));
    assert!(!limit(231));
    // Horizon 0's `Neither` falls on the first `days` days of 364 only, so
    // a resample whose 13 blocks all miss those days has none.
    let clustered = |days: usize| {
        let daily = daily_of(364, 100, |day, h| match h {
            0 if day < days => 10,
            0 => 0,
            _ => 10,
        });
        bootstrap_se(&daily, (0, 4), BLOCK_DAYS, RESAMPLES, SEED)
    };
    // A long cluster: a few resamples miss it, under the limit, so the SE
    // is taken over the others.
    let few = clustered(120);
    assert!((1..=230).contains(&few.undefined), "{few:?}");
    assert!(few.se.is_some_and(|se| se > 0.0), "{few:?}");
    // One day of `Neither`: most resamples miss it.
    let many = clustered(1);
    assert!(many.undefined > 230, "{many:?}");
    assert_eq!(many.se, None);
    // An undefined SE counts as outside the band; a defined one only past
    // two SE.
    assert!(outside(0.01, None));
    assert!(outside(0.5, Some(0.2)) && outside(-0.5, Some(0.2)));
    assert!(!outside(0.4, Some(0.2)) && !outside(-0.4, Some(0.2)));
}

#[test]
fn the_miss_needs_the_bound_and_the_band() {
    // ln 1.7 = 0.531: outside two SE of 0.2, inside two SE of 0.3.
    assert!(miss(1.7, Some(0.2)));
    assert!(!miss(1.7, Some(0.3)), "inside the band");
    // ln 1.6 = 0.470 is far outside two SE of 0.01, but within the bound.
    assert!(!miss(1.6, Some(0.01)), "within the bound");
    assert!(
        !miss(MISS_RATIO, Some(0.01)),
        "the bound itself does not miss"
    );
    // An undefined SE is outside the band; the bound still applies.
    assert!(miss(1.7, None));
    assert!(!miss(1.5, None));
}

#[test]
fn steps_rise_or_fall_only_outside_their_band() {
    use Step::{Fall, Flat, Rise};
    // ln 1.2 = 0.182 against two SE of 0.05 and of 0.1.
    assert_eq!(Step::of(10.0, 12.0, Some(0.05)), Rise);
    assert_eq!(Step::of(12.0, 10.0, Some(0.05)), Fall);
    assert_eq!(Step::of(10.0, 12.0, Some(0.1)), Flat);
    assert_eq!(Step::of(12.0, 10.0, Some(0.1)), Flat);
    // Equal shares are flat, 0 included; a step from or to 0 has an
    // infinite log; an undefined SE reads the sign.
    assert_eq!(Step::of(0.0, 0.0, None), Flat);
    assert_eq!(Step::of(10.0, 10.0, None), Flat);
    assert_eq!(Step::of(0.0, 0.1, Some(1.0)), Rise);
    assert_eq!(Step::of(0.1, 0.0, Some(1.0)), Fall);
    assert_eq!(Step::of(10.0, 10.1, None), Rise);
}

/// The reading of `letters`, U/D/F for rise/fall/flat, 5m→15m first.
fn shape_of(letters: &str) -> &'static str {
    let steps: Vec<Step> = letters
        .chars()
        .map(|letter| match letter {
            'U' => Step::Rise,
            'D' => Step::Fall,
            _ => Step::Flat,
        })
        .collect();
    shape(steps.try_into().expect("four steps"))
}

#[test]
fn every_step_sequence_has_one_reading() {
    // ADR-056 *Reading of a miss*: each reading's sequences, and `other`
    // for the remaining 51 of the 81.
    let readings: [(&str, &[&str]); 7] = [
        ("fall from 5m: jumps", &["DFFF", "DDFF", "DDDF", "DDDD"]),
        (
            "rise below 1h: finite volatility memory",
            &["UUFF", "UUUF", "UUFU", "UUUU", "FUFU", "FUUU"],
        ),
        ("rise above 1h only: the week", &["FFFU", "FFUU"]),
        ("peak at 15m: intraday sessions", &["UDFF"]),
        (
            "peak at 1h or 4h: 24 h seasonality",
            &["FUDF", "FUDD", "FUFD", "FUUD", "FFUD"],
        ),
        (
            "fall from 5m, rise below 1h: jumps and finite volatility memory",
            &["DUFU", "DUUU"],
        ),
        (
            "fall from 5m, rise above 1h only: jumps and the week",
            &["DFFU", "DFUU", "DFDU", "DDFU", "DDUU", "DDDU"],
        ),
    ];
    let mut named = 0;
    for (reading, sequences) in readings {
        for letters in sequences {
            assert_eq!(shape_of(letters), reading, "{letters}");
            named += 1;
        }
    }
    assert_eq!(named, 26);
    let mut other = 0;
    for code in 0..81 {
        let letters: String = (0..4)
            .map(|k| ['U', 'D', 'F'][code / 3_usize.pow(k) % 3])
            .collect();
        let reading = shape_of(&letters);
        if reading == OTHER {
            other += 1;
        } else {
            assert!(
                readings
                    .iter()
                    .any(|(name, sequences)| *name == reading
                        && sequences.contains(&letters.as_str())),
                "{letters}: {reading}"
            );
        }
    }
    assert_eq!(other, 55);
    assert_eq!(shape_of("FFFF"), OTHER);
    // A fall after a flat step is no jump signature.
    for letters in ["DFDF", "DFFD", "DDFD", "DFDD"] {
        assert_eq!(shape_of(letters), OTHER, "{letters}");
    }
}

#[test]
fn noisy_shares_name_a_mechanism_only_past_their_bands() {
    // The review's simulation (PR #153): per-horizon relative SE 1.5 times
    // the binomial one at 10 % for 5m to 4h, 0.26 at 1d; a step's SE is
    // the root sum of squares of its two horizons'.
    let horizon_se = [0.015, 0.0255, 0.051, 0.102, 0.26];
    let ses: [Option<f64>; 4] =
        std::array::from_fn(|i| Some(f64::hypot(horizon_se[i], horizon_se[i + 1])));
    let read = |shares: [f64; 5]| reading(&shares, steps(&shares, &ses));
    let cases = [
        // A flat truth with noise, including a 1d excursion past 1.62
        // (15.9 / 9.6 = 1.66) that stays inside its step's band. Raw signs
        // read both as an interior trough at 4h.
        ([10.2, 10.0, 9.9, 9.7, 11.5], OTHER),
        ([10.1, 10.0, 9.8, 9.6, 15.9], OTHER),
        // The week on 1d only; raw signs read an interior trough at 1h.
        ([10.1, 9.9, 9.8, 10.2, 18.5], "rise above 1h only: the week"),
        // The week on 4h and 1d; raw signs read a monotone rise.
        (
            [9.9, 10.0, 10.1, 13.0, 23.0],
            "rise above 1h only: the week",
        ),
        // Jumps with a flat tail; raw signs read an interior trough at 4h.
        ([18.2, 12.9, 10.4, 9.9, 10.6], "fall from 5m: jumps"),
        // The review's round-3 case (PR #153): 5m 7 % high, inside what
        // the bound allows, and 1d 40 % low. The miss lies in the flat
        // 4h→1d step (ln 0.6 = −0.51, past ln 1.62 alone), so DFFF does
        // not read as jumps.
        ([10.7, 10.0, 10.0, 10.0, 6.0], OTHER),
        // The same with 1d past its band: DFFD, a fall after flat steps.
        ([10.7, 10.0, 10.0, 10.0, 5.5], OTHER),
    ];
    let letters = |shares: [f64; 5]| -> String {
        steps(&shares, &ses)
            .iter()
            .map(|step| step.letter())
            .collect()
    };
    let one_d_low = [10.7, 10.0, 10.0, 10.0, 6.0];
    assert_eq!(letters(one_d_low), "DFFF");
    assert!(miss(
        10.7 / 6.0,
        Some(f64::hypot(horizon_se[0], horizon_se[4]))
    ));
    assert!(flat_spread(&one_d_low, steps(&one_d_low, &ses)) > MISS_RATIO.ln());
    assert_eq!(letters([10.7, 10.0, 10.0, 10.0, 5.5]), "DFFD");
    for (shares, reading) in cases {
        assert_eq!(read(shares), reading, "{shares:?}");
    }
}
