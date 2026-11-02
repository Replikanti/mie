//! The ADR-059 measurement (#38 M1, plan item 7): the alert volume of
//! `alert.policy@1` on a replay, before any delivery budget, monitored-set
//! narrowing, report cadence or regime hysteresis is decided (gate G).
//!
//! The tool drives the alert decision path of live ingestion,
//! [`drive_alerts_observed`], over the replay with a counting gateway that
//! keeps the message stream (per message: its event time and, per alert,
//! the kind, the level kind and price, the trigger id or the two regime
//! labels — a few MB for a year), plus two more monitors for A2. Reported,
//! every figure before any assertion:
//!
//! - **A1** (asserted): no message rendered in the window breaks the
//!   ADR-059 D4 wording rule ([`wording_hits`]); also printed: messages
//!   over the length bound or not ASCII (both 0 by construction).
//! - **A2** (asserted): a monitor seeded at the window start and one seeded
//!   at the second half's first accepted event — from the engine's state
//!   before it, read at the last event that closed a bar or announced a
//!   feed gap, the only events that change what a seed reads — both
//!   emitting from the split emit identical alerts
//!   at every event (ADR-059 D2g). Both emit from the split: a monitor that
//!   emitted before it alerts the leave of a visit it saw entered, which the
//!   later seed by design does not (D2b).
//! - *Volume:* messages per UTC day p50/p95/max per half, in total, per
//!   alert kind (a message counts once for each kind it carries) and per
//!   trigger id; the most messages in any sliding 60-minute event-time
//!   window; the largest queue and send delay a FIFO sending one message
//!   per 3 s would reach.
//! - *Visits* per monitored kind (POC, VAH, VAL): alerted enters per UTC
//!   day, visit duration p50/p95/max, and the re-entry gap (a level's
//!   `ZoneLeft` to its next `ZoneEntered` on the same UTC day) as deciles.
//! - *Regime:* `RegimeChanged` per UTC day per half; the dwell between
//!   flips in 1h closes as deciles; the share of flips reverted at the next
//!   1h close.
//! - *Silence:* the longest message-free event-time stretch of each UTC
//!   day, p50/p95 per half.
//! - *Option table* (post-processing of the kept stream, no second
//!   replay): messages per UTC day p50/p95/max per half for every
//!   combination of {POC zone alerts kept, dropped} × {`RegimeChanged` as
//!   alert, report-only, a minimum dwell of k closes for each distinct
//!   dwell decile k} × {no merge gap, a merge gap g for each distinct
//!   re-entry-gap decile g}, plus the cadence rows "+ report at every 4h
//!   close" and "+ report at every 1h close". Merge gap g: a re-entry
//!   within g of the same level's `ZoneLeft` alerts neither the leave nor
//!   the re-entry, so every alerted leave is delayed by g. Dwell k: a flip
//!   is alerted at the k-th 1h close that holds the new label. A delayed
//!   alert is counted as a message of its own, so every row is an upper
//!   bound. The one-factor rows are printed; the whole grid goes to
//!   `MIE_MEASURE_TABLE_OUT=<file>` (TSV) when set.
//! - *Cost:* events, rejections, replay wall time.
//!
//! Halves split at `from + (to − from) / 2`; an alert or message belongs to
//! the half of its event time, a UTC day to the half that holds its first
//! millisecond. Warm-up days — before the regime and the prior-day profile
//! are both ready after the day's first accepted event — are counted
//! separately and kept out of every per-day figure: a warming monitor
//! would bias them low.
//!
//! `measure_alerts` (`--ignored`) runs the archive as `trigger_measure.rs`
//! does: aggTrades with the ADR-050 fold (its D8 default) and `metrics`.
//! Run it with `MIE_MEASURE_RAW_ROOT=<raw root> MIE_MEASURE_FROM=<YYYY-MM-DD>
//! MIE_MEASURE_TO=<YYYY-MM-DD> cargo test --release -p mie-cli --test
//! alert_measure -- --ignored --exact --nocapture measure_alerts`;
//! `MIE_MEASURE_TO` is included, as in `mie replay`, an optional
//! `MIE_MEASURE_EVENTS` asserts the replay's event count, and an optional
//! `MIE_MEASURE_TABLE_OUT` writes the option grid. The #12 development
//! year is 2025-10-01 … 2026-09-30 with `MIE_MEASURE_EVENTS=603323345`
//! (ADR-051 R0's count of the same replay).
//!
//! `measure_alerts_on_a_synthetic_tape` runs the whole tool in CI on the
//! seeded synthetic tape and asserts that every alert kind occurred, so A1
//! and A2 cannot pass vacuously; `a2_holds_across_a_trades_hole_before_the_split`
//! runs it with a two-hour trades hole after the regime is ready and before
//! the split, so the regime re-warms across the split (ADR-059 D2e);
//! `option_table_on_a_synthetic_stream`
//! checks the post-processing against hand-computed values.

mod common;

use common::tapes::{TAPE_DAYS, TAPE_START, synthetic_tape};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::fold::TradeCorrection;
use mie_adapter_binance::archive::replay::ArchiveReplay;
use mie_adapter_parquet::ParquetRawStore;
use mie_app::alert::{MAX_MESSAGE_CHARS, drive_alerts_observed, wording_hits};
use mie_cli::alerts::quantiles;
use mie_cli::replay::parse_bound;
use mie_domain::alert::{ALERT_TRIGGERS, Alert, AlertKind, AlertMonitor};
use mie_domain::bars::Timeframe;
use mie_domain::event::{FeedGap, GapReason, MarketEvent, Stream as EventStream};
use mie_domain::location::LevelKind;
use mie_domain::regime::RegimeLabel;
use mie_domain::state::MarketStateEngine;
use mie_domain::time::EventTime;
use mie_ports::outbound::{
    AlertError, AlertGateway, AlertMessage, HistoricalDataProvider, MarketDataProvider,
    ProviderError, ReplayWindow,
};
use std::collections::VecDeque;
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const HOUR_MS: i64 = 3_600_000;

/// The FIFO pacing: one message per 3 s, the Telegram pacing ADR-059 D5
/// derives from the Bot API limits (plan item 11).
const PACING_MS: i64 = 3_000;

/// The sliding window of the burst figure.
const BURST_MS: i64 = HOUR_MS;

/// The monitored kinds, in report order.
const KINDS: [LevelKind; 3] = [LevelKind::Poc, LevelKind::Vah, LevelKind::Val];

/// One alert of a kept message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Item {
    Entered {
        kind: LevelKind,
        price: i64,
    },
    Left {
        kind: LevelKind,
        price: i64,
    },
    /// The position of the trigger id in [`ALERT_TRIGGERS`].
    Trigger(usize),
    Regime {
        from: RegimeLabel,
        to: RegimeLabel,
    },
    Report,
}

impl Item {
    fn kind(&self) -> AlertKind {
        match self {
            Self::Entered { .. } => AlertKind::ZoneEntered,
            Self::Left { .. } => AlertKind::ZoneLeft,
            Self::Trigger(_) => AlertKind::TriggerFired,
            Self::Regime { .. } => AlertKind::RegimeChanged,
            Self::Report => AlertKind::Report,
        }
    }

    fn of(alert: &Alert) -> Self {
        match alert {
            Alert::ZoneEntered { level, .. } => Self::Entered {
                kind: level.kind,
                price: level.price.units(),
            },
            Alert::ZoneLeft { level, .. } => Self::Left {
                kind: level.kind,
                price: level.price.units(),
            },
            Alert::TriggerFired(trigger) => Self::Trigger(
                ALERT_TRIGGERS
                    .iter()
                    .position(|key| *key == trigger.feature)
                    .expect("only alerted ids fire"),
            ),
            Alert::RegimeChanged { from, to, .. } => Self::Regime {
                from: *from,
                to: to.label,
            },
            Alert::Report(_) => Self::Report,
        }
    }
}

/// One kept message.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Kept {
    at: i64,
    items: Vec<Item>,
}

/// The kept stream of a run and what the post-processing needs.
#[derive(Debug, Clone, Default)]
struct Stream {
    from: i64,
    to: i64,
    mid: i64,
    messages: Vec<Kept>,
    /// The event time of every accepted event that closed a 1h, 4h or 1d
    /// bar.
    h1: Vec<i64>,
    h4: Vec<i64>,
    d1: Vec<i64>,
    /// The first UTC day counted in the per-day figures.
    first_counted_day: Option<i64>,
}

impl Stream {
    fn new(from: i64, to: i64) -> Self {
        Self {
            from,
            to,
            mid: from + (to - from) / 2,
            ..Self::default()
        }
    }

    /// The window's UTC days: counted ones per half, and the warm-up count.
    fn days(&self) -> ([Vec<i64>; 2], usize) {
        let (first, last) = (day_of(self.from), day_of(self.to - 1));
        let mut halves = [Vec::new(), Vec::new()];
        let mut warm = 0;
        for day in first..=last {
            if self.first_counted_day.is_none_or(|counted| day < counted) {
                warm += 1;
            } else {
                halves[usize::from(day * DAY_MS >= self.mid)].push(day);
            }
        }
        (halves, warm)
    }

    /// Per half, the count of `times` on each counted day.
    fn per_day(&self, times: &[i64]) -> [Vec<u64>; 2] {
        let (halves, _) = self.days();
        halves.map(|days| {
            let mut counts = vec![0_u64; days.len()];
            for time in times {
                if let Some(index) = days.iter().position(|day| *day == day_of(*time)) {
                    counts[index] += 1;
                }
            }
            counts
        })
    }

    /// p50/p95/max per half of [`Self::per_day`].
    fn daily(&self, times: &[i64]) -> [(u64, u64, u64); 2] {
        self.per_day(times).map(|counts| quantiles(&counts))
    }

    fn times(&self, pick: impl Fn(&Kept) -> bool) -> Vec<i64> {
        self.messages
            .iter()
            .filter(|kept| pick(kept))
            .map(|kept| kept.at)
            .collect()
    }

    /// The ordinal (from 1) of the 1h close at `at`.
    fn close_at(&self, at: i64) -> usize {
        self.h1.binary_search(&at).expect("a flip is a 1h close") + 1
    }

    /// Every flip: its 1h close ordinal, labels, and the closes it held
    /// (to the next flip, or censored at the last close).
    fn flips(&self) -> Vec<Flip> {
        let mut flips: Vec<Flip> = Vec::new();
        for kept in &self.messages {
            for item in &kept.items {
                if let Item::Regime { from, to } = item {
                    flips.push(Flip {
                        close: self.close_at(kept.at),
                        from: *from,
                        to: *to,
                        held: 0,
                        next: false,
                    });
                }
            }
        }
        let total = self.h1.len();
        for index in 0..flips.len() {
            let (held, next) = match flips.get(index + 1) {
                Some(next) => (next.close - flips[index].close, true),
                None => (total + 1 - flips[index].close, false),
            };
            flips[index].held = held;
            flips[index].next = next;
        }
        flips
    }

    /// The dwell of every flip followed by another, in 1h closes.
    fn dwells(&self) -> Vec<u64> {
        self.flips()
            .iter()
            .filter(|flip| flip.next)
            .map(|flip| flip.held as u64)
            .collect()
    }

    /// Flips reverted at the next 1h close, and all flips.
    fn reverted(&self) -> (usize, usize) {
        let flips = self.flips();
        let reverted = flips
            .windows(2)
            .filter(|pair| pair[0].held == 1 && pair[1].to == pair[0].from)
            .count();
        (reverted, flips.len())
    }

    /// Every zone item in time order: message, item, time, level, enter.
    fn zone_items(&self) -> Vec<ZoneItem> {
        let mut items = Vec::new();
        for (message, kept) in self.messages.iter().enumerate() {
            for (item, alert) in kept.items.iter().enumerate() {
                let (kind, price, enter) = match *alert {
                    Item::Entered { kind, price } => (kind, price, true),
                    Item::Left { kind, price } => (kind, price, false),
                    _ => continue,
                };
                items.push(ZoneItem {
                    message,
                    item,
                    at: kept.at,
                    level: (kind, price),
                    enter,
                });
            }
        }
        items
    }

    /// Per monitored kind: visit durations, and the re-entry gaps (a leave
    /// to the next enter of the same level on the same UTC day).
    fn visits(&self, kind: LevelKind) -> (Vec<u64>, Vec<u64>) {
        let items: Vec<ZoneItem> = self
            .zone_items()
            .into_iter()
            .filter(|item| item.level.0 == kind)
            .collect();
        let (mut durations, mut gaps) = (Vec::new(), Vec::new());
        for (index, item) in items.iter().enumerate() {
            let next = items[index + 1..]
                .iter()
                .find(|other| other.level == item.level);
            match (item.enter, next) {
                (true, Some(next)) if !next.enter => durations.push((next.at - item.at) as u64),
                (false, Some(next)) if next.enter && day_of(next.at) == day_of(item.at) => {
                    gaps.push((next.at - item.at) as u64);
                }
                _ => {}
            }
        }
        (durations, gaps)
    }

    /// The re-entry gaps of every monitored kind.
    fn gaps(&self) -> Vec<u64> {
        KINDS.iter().flat_map(|kind| self.visits(*kind).1).collect()
    }

    /// The message times under `option` (module docs): every message with a
    /// kept item at its own time, and every delayed alert as a message of
    /// its own.
    fn simulate(&self, option: &Policy) -> Vec<i64> {
        let mut dropped: Vec<Vec<bool>> = self
            .messages
            .iter()
            .map(|kept| vec![false; kept.items.len()])
            .collect();
        let mut delayed: Vec<i64> = Vec::new();
        for (message, kept) in self.messages.iter().enumerate() {
            for (item, alert) in kept.items.iter().enumerate() {
                if option.drop_poc
                    && matches!(
                        alert,
                        Item::Entered {
                            kind: LevelKind::Poc,
                            ..
                        } | Item::Left {
                            kind: LevelKind::Poc,
                            ..
                        }
                    )
                {
                    dropped[message][item] = true;
                }
            }
        }
        // Regime rows.
        let flips = self.flips();
        let mut flip_index = 0;
        for (message, kept) in self.messages.iter().enumerate() {
            for (item, alert) in kept.items.iter().enumerate() {
                if !matches!(alert, Item::Regime { .. }) {
                    continue;
                }
                let flip = &flips[flip_index];
                flip_index += 1;
                match option.regime {
                    RegimeRule::Alert => {}
                    RegimeRule::ReportOnly => dropped[message][item] = true,
                    RegimeRule::Dwell(k) if k <= 1 => {}
                    RegimeRule::Dwell(k) => {
                        dropped[message][item] = true;
                        if flip.held >= k {
                            delayed.push(self.h1[flip.close + k - 2]);
                        }
                    }
                }
            }
        }
        // Merge gap.
        if let Some(gap) = option.merge {
            let gap = i64::try_from(gap).expect("a gap in ms");
            let items: Vec<ZoneItem> = self
                .zone_items()
                .into_iter()
                .filter(|zone| !dropped[zone.message][zone.item])
                .collect();
            let mut merged = vec![false; items.len()];
            for (index, item) in items.iter().enumerate() {
                if item.enter || merged[index] {
                    continue;
                }
                let next = items[index + 1..]
                    .iter()
                    .position(|other| other.level == item.level)
                    .map(|offset| index + 1 + offset);
                dropped[item.message][item.item] = true;
                match next {
                    Some(next)
                        if items[next].enter
                            && items[next].at - item.at <= gap
                            && day_of(items[next].at) == day_of(item.at) =>
                    {
                        merged[next] = true;
                        dropped[items[next].message][items[next].item] = true;
                    }
                    _ => delayed.push(item.at + gap),
                }
            }
        }
        let mut times: Vec<i64> = self
            .messages
            .iter()
            .zip(&dropped)
            .filter(|(_, dropped)| dropped.iter().any(|d| !d))
            .map(|(kept, _)| kept.at)
            .collect();
        times.extend(delayed);
        let cadence = match option.cadence {
            Cadence::Daily => &[][..],
            Cadence::H4 => &self.h4[..],
            Cadence::H1 => &self.h1[..],
        };
        times.extend(cadence.iter().filter(|at| !self.d1.contains(at)));
        times.retain(|at| self.from <= *at && *at < self.to);
        times.sort_unstable();
        times
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Flip {
    close: usize,
    from: RegimeLabel,
    to: RegimeLabel,
    held: usize,
    next: bool,
}

#[derive(Debug, Clone, Copy)]
struct ZoneItem {
    message: usize,
    item: usize,
    at: i64,
    level: (LevelKind, i64),
    enter: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RegimeRule {
    Alert,
    ReportOnly,
    /// Alerted at the k-th close that holds the new label.
    Dwell(usize),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cadence {
    Daily,
    H4,
    H1,
}

/// One row of the option table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Policy {
    drop_poc: bool,
    regime: RegimeRule,
    /// The merge gap in ms.
    merge: Option<u64>,
    cadence: Cadence,
}

/// A row of the option table: the policy and its messages per UTC day
/// (p50, p95, max) per half.
type Row = (Policy, [(u64, u64, u64); 2]);

const BASE: Policy = Policy {
    drop_poc: false,
    regime: RegimeRule::Alert,
    merge: None,
    cadence: Cadence::Daily,
};

impl Policy {
    fn label(&self) -> String {
        let regime = match self.regime {
            RegimeRule::Alert => "alert".to_owned(),
            RegimeRule::ReportOnly => "report_only".to_owned(),
            RegimeRule::Dwell(k) => format!("dwell_{k}"),
        };
        let merge = self
            .merge
            .map_or("none".to_owned(), |gap| format!("{}s", gap / 1_000));
        let cadence = match self.cadence {
            Cadence::Daily => "1d",
            Cadence::H4 => "+4h",
            Cadence::H1 => "+1h",
        };
        format!(
            "poc={}\tregime={regime}\tmerge={merge}\treport={cadence}",
            if self.drop_poc { "dropped" } else { "kept" }
        )
    }
}

fn day_of(at: i64) -> i64 {
    at.div_euclid(DAY_MS)
}

/// The distinct values at the deciles 10 … 90 of `values`, nearest rank.
fn deciles(values: &[u64]) -> Vec<u64> {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mut out: Vec<u64> = (1..=9)
        .filter_map(|q| {
            let index = (q * sorted.len()).div_ceil(10);
            index.checked_sub(1).map(|index| sorted[index])
        })
        .collect();
    out.dedup();
    out
}

/// Every row of the option table: the full grid, then the cadence rows.
fn policies(stream: &Stream) -> Vec<Policy> {
    let mut regimes = vec![RegimeRule::Alert, RegimeRule::ReportOnly];
    regimes.extend(
        deciles(&stream.dwells())
            .into_iter()
            .map(|k| RegimeRule::Dwell(usize::try_from(k).expect("a dwell"))),
    );
    let mut merges = vec![None];
    merges.extend(deciles(&stream.gaps()).into_iter().map(Some));
    let mut rows = Vec::new();
    for drop_poc in [false, true] {
        for regime in &regimes {
            for merge in &merges {
                rows.push(Policy {
                    drop_poc,
                    regime: *regime,
                    merge: *merge,
                    cadence: Cadence::Daily,
                });
            }
        }
    }
    for cadence in [Cadence::H4, Cadence::H1] {
        rows.push(Policy { cadence, ..BASE });
    }
    rows
}

/// Whether a row changes one factor of the base at most.
fn one_factor(policy: &Policy) -> bool {
    [
        policy.drop_poc,
        policy.regime != RegimeRule::Alert,
        policy.merge.is_some(),
        policy.cadence != Cadence::Daily,
    ]
    .iter()
    .filter(|changed| **changed)
    .count()
        <= 1
}

/// The most messages in any `span`-long window.
fn burst(times: &[i64], span: i64) -> usize {
    let mut start = 0;
    let mut best = 0;
    for end in 0..times.len() {
        while times[end] - times[start] >= span {
            start += 1;
        }
        best = best.max(end - start + 1);
    }
    best
}

/// The largest queue (the arriving message included) and send delay of a
/// FIFO sending one message per `pacing`.
fn fifo(times: &[i64], pacing: i64) -> (usize, i64) {
    let mut sends: VecDeque<i64> = VecDeque::new();
    let mut last: Option<i64> = None;
    let (mut queue, mut delay) = (0, 0);
    for at in times {
        while sends.front().is_some_and(|send| send <= at) {
            sends.pop_front();
        }
        let send = last.map_or(*at, |last| (*at).max(last + pacing));
        sends.push_back(send);
        last = Some(send);
        queue = queue.max(sends.len());
        delay = delay.max(send - at);
    }
    (queue, delay)
}

/// The longest message-free stretch of each counted day, per half.
fn silence(stream: &Stream, times: &[i64]) -> [Vec<u64>; 2] {
    let (halves, _) = stream.days();
    halves.map(|days| {
        days.iter()
            .map(|day| {
                let mut edges = vec![day * DAY_MS];
                edges.extend(times.iter().filter(|at| day_of(**at) == *day));
                edges.push((day + 1) * DAY_MS);
                edges
                    .windows(2)
                    .map(|pair| (pair[1] - pair[0]) as u64)
                    .max()
                    .unwrap_or(0)
            })
            .collect()
    })
}

/// The counting gateway: keeps the stream and checks every text (A1).
#[derive(Default)]
struct Counting {
    messages: Vec<Kept>,
    wording: u64,
    oversize: u64,
    not_ascii: u64,
    notes: Vec<String>,
}

impl AlertGateway for Counting {
    fn deliver(&mut self, message: &AlertMessage) -> Result<(), AlertError> {
        let hits = wording_hits(&message.text);
        if !hits.is_empty() {
            self.wording += 1;
            if self.notes.len() < 5 {
                self.notes.push(format!("{hits:?} in {}", message.text));
            }
        }
        self.oversize += u64::from(message.text.chars().count() > MAX_MESSAGE_CHARS);
        self.not_ascii += u64::from(!message.text.is_ascii());
        self.messages.push(Kept {
            at: message.known_at.as_millis(),
            items: message.alerts.iter().map(Item::of).collect(),
        });
        Ok(())
    }
}

/// What a run measured.
struct Run {
    stream: Stream,
    gateway: Counting,
    events: u64,
    rejections: u64,
    /// A2: events compared, alerts compared, mismatching events, notes.
    a2: (u64, u64, u64, Vec<String>),
    wall: Duration,
}

/// Drives `provider` over `[from, to)` through the alert path.
fn measure<P: MarketDataProvider>(provider: &mut P, from: i64, to: i64) -> Run {
    let started = Instant::now();
    let mut stream = Stream::new(from, to);
    let mid = EventTime::from_millis(stream.mid);
    let mut engine = MarketStateEngine::new();
    let fresh = engine.state().clone();
    let mut monitor = AlertMonitor::new(None, &fresh);
    // A2: seeded at the window start, and at the split from the state
    // before its first accepted event; both emit from the split.
    let mut early = AlertMonitor::new(Some(mid), &fresh);
    let mut candidate = AlertMonitor::new(Some(mid), &fresh);
    let mut late: Option<AlertMonitor> = None;
    let mut a2 = (0_u64, 0_u64, 0_u64, Vec::new());
    let mut gateway = Counting::default();
    let mut last_day = None;
    let report = drive_alerts_observed(
        provider,
        &mut engine,
        &mut monitor,
        |_| {},
        &mut gateway,
        |event: &MarketEvent, engine: &MarketStateEngine| {
            let at = event.time().as_millis();
            let mut early_alerts = Vec::new();
            early.observe(event, engine, |alert| early_alerts.push(alert));
            if event.time() >= mid {
                let late = late.get_or_insert_with(|| candidate.clone());
                let mut late_alerts = Vec::new();
                late.observe(event, engine, |alert| late_alerts.push(alert));
                a2.0 += 1;
                a2.1 += early_alerts.len() as u64;
                if early_alerts != late_alerts {
                    a2.2 += 1;
                    if a2.3.len() < 5 {
                        a2.3.push(format!(
                            "{}: {:?} vs {:?}",
                            event.canonical_key(),
                            early_alerts
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>(),
                            late_alerts
                                .iter()
                                .map(ToString::to_string)
                                .collect::<Vec<_>>()
                        ));
                    }
                }
            }
            let closed = engine.closed_bars();
            for (timeframe, times) in [
                (Timeframe::H1, &mut stream.h1),
                (Timeframe::H4, &mut stream.h4),
                (Timeframe::D1, &mut stream.d1),
            ] {
                if closed.iter().any(|bar| bar.timeframe == timeframe) {
                    times.push(at);
                }
            }
            // The seed reads the regime and the in-zone flags, which change
            // only at an event that closes a bar or announces a feed gap
            // (a gap can send the regime back to warming up).
            if event.time() < mid
                && (!closed.is_empty() || matches!(event, MarketEvent::FeedGap(_)))
            {
                candidate = AlertMonitor::new(Some(mid), engine.state());
            }
            let day = day_of(at);
            let first_of_day = last_day != Some(day);
            last_day = Some(day);
            let state = engine.state();
            if stream.first_counted_day.is_none()
                && state.regime.is_ready()
                && state.profile.prior_day.is_ready()
            {
                stream.first_counted_day = Some(if first_of_day { day } else { day + 1 });
            }
        },
    )
    .expect("the replay streams");
    stream.messages = std::mem::take(&mut gateway.messages);
    Run {
        stream,
        gateway,
        events: report.events,
        rejections: report.rejections,
        a2,
        wall: started.elapsed(),
    }
}

fn row(figures: [(u64, u64, u64); 2]) -> String {
    let [(a50, a95, amax), (b50, b95, bmax)] = figures;
    format!("{a50}\t{a95}\t{amax}\t{b50}\t{b95}\t{bmax}")
}

/// Prints every figure. Returns the one-factor and full-grid rows.
fn report(run: &Run) -> Vec<Row> {
    let stream = &run.stream;
    let (halves, warm) = stream.days();
    println!(
        "window {}..{} ms, split {} ms; counted days {} + {}, warm-up days {warm}",
        stream.from,
        stream.to,
        stream.mid,
        halves[0].len(),
        halves[1].len()
    );
    println!(
        "A1: messages {}, wording-rule hits {}, over {MAX_MESSAGE_CHARS} chars {}, not ASCII {}",
        stream.messages.len(),
        run.gateway.wording,
        run.gateway.oversize,
        run.gateway.not_ascii
    );
    for note in &run.gateway.notes {
        println!("  {note}");
    }
    println!(
        "A2: events compared {}, alerts {}, mismatching events {}",
        run.a2.0, run.a2.1, run.a2.2
    );
    for note in &run.a2.3 {
        println!("  {note}");
    }
    println!("volume: messages per UTC day p50 p95 max, first half then second");
    let all = stream.times(|_| true);
    println!("  total\t{}", row(stream.daily(&all)));
    for kind in AlertKind::ALL {
        let times = stream.times(|kept| kept.items.iter().any(|item| item.kind() == kind));
        println!("  {kind}\t{}", row(stream.daily(&times)));
    }
    for (index, key) in ALERT_TRIGGERS.iter().enumerate() {
        let times = stream.times(|kept| kept.items.contains(&Item::Trigger(index)));
        println!("  {key}\t{}", row(stream.daily(&times)));
    }
    let (queue, delay) = fifo(&all, PACING_MS);
    println!(
        "  most messages in 60 min {}; FIFO at one per 3 s: queue {queue}, delay {delay} ms",
        burst(&all, BURST_MS)
    );
    println!("visits: enters per UTC day p50 p95 max per half; duration p50 p95 max (ms)");
    for kind in KINDS {
        let enters = stream.times(|kept| {
            kept.items
                .iter()
                .any(|item| matches!(item, Item::Entered { kind: k, .. } if *k == kind))
        });
        let (durations, gaps) = stream.visits(kind);
        let (d50, d95, dmax) = quantiles(&durations);
        println!(
            "  {kind}\tenters {}\tduration {d50} {d95} {dmax}\tre-entry gap deciles {:?}",
            row(stream.daily(&enters)),
            deciles(&gaps)
        );
    }
    let flips = stream.times(|kept| {
        kept.items
            .iter()
            .any(|i| i.kind() == AlertKind::RegimeChanged)
    });
    let (reverted, all_flips) = stream.reverted();
    println!(
        "regime: flips per UTC day {}; dwell deciles (1h closes) {:?}; reverted at the next close {reverted} of {all_flips}",
        row(stream.daily(&flips)),
        deciles(&stream.dwells())
    );
    let [first, second] = silence(stream, &all);
    let (s1, s2) = (quantiles(&first), quantiles(&second));
    println!(
        "silence: longest message-free stretch per UTC day p50 p95 (ms): {} {} / {} {}",
        s1.0, s1.1, s2.0, s2.1
    );
    let rows: Vec<Row> = policies(stream)
        .into_iter()
        .map(|policy| (policy, stream.daily(&stream.simulate(&policy))))
        .collect();
    println!("options (one factor; messages per UTC day p50 p95 max per half):");
    for (policy, figures) in rows.iter().filter(|(policy, _)| one_factor(policy)) {
        println!("  {}\t{}", policy.label(), row(*figures));
    }
    println!(
        "cost: events {}, rejections {}, wall {:.1} s",
        run.events,
        run.rejections,
        run.wall.as_secs_f64()
    );
    rows
}

fn env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("set {name}"))
}

fn write_table(rows: &[Row]) {
    let Ok(path) = std::env::var("MIE_MEASURE_TABLE_OUT") else {
        return;
    };
    let mut text = String::from(
        "poc\tregime\tmerge\treport\th1_p50\th1_p95\th1_max\th2_p50\th2_p95\th2_max\n",
    );
    for (policy, figures) in rows {
        text.push_str(&format!("{}\t{}\n", policy.label(), row(*figures)));
    }
    std::fs::write(&path, text).expect("write the option table");
    println!("option table: {} rows written to {path}", rows.len());
}

#[test]
#[ignore = "measures the alert volume of a real archive window for ADR-059; see the module docs"]
fn measure_alerts() {
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
    // 5-minute open interest (ADR-042 D2), as trigger_measure.rs replays.
    let replay = ArchiveReplay::new(
        &store,
        "BTCUSDT",
        &[ArchiveStream::AggTrades, ArchiveStream::Metrics],
        TradeCorrection::Fold,
    );
    let opened = replay.replay(window).unwrap();
    println!("dataset {}", opened.dataset);
    let mut stream = opened.stream;
    let run = measure(&mut stream, from, to);
    let rows = report(&run);
    write_table(&rows);
    if let Some(expected) = expected_events {
        println!("events {} (expected {expected})", run.events);
        assert_eq!(run.events, expected, "the replay's event count differs");
    }
    assert_eq!(run.gateway.wording, 0, "A1: the wording rule is broken");
    assert_eq!(run.a2.2, 0, "A2: the seeds disagree");
    assert!(run.a2.0 > 0, "A2 compared nothing");
}

struct Feed(VecDeque<MarketEvent>);

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

#[test]
fn measure_alerts_on_a_synthetic_tape() {
    let (from, to) = (TAPE_START, TAPE_START + TAPE_DAYS * DAY_MS);
    // The tape's last trade can land after its last day.
    let mut feed = Feed(
        synthetic_tape()
            .into_iter()
            .filter(|event| event.time().as_millis() < to)
            .collect(),
    );
    let run = measure(&mut feed, from, to);
    let rows = report(&run);
    assert_eq!(run.rejections, 0);
    assert_eq!(run.gateway.wording, 0, "{:?}", run.gateway.notes);
    assert_eq!((run.gateway.oversize, run.gateway.not_ascii), (0, 0));
    assert_eq!(run.a2.2, 0, "{:?}", run.a2.3);
    // Every kind occurred, in both halves where A2 compares, so neither A1
    // nor A2 passed vacuously.
    for kind in AlertKind::ALL {
        let times = run
            .stream
            .times(|kept| kept.items.iter().any(|item| item.kind() == kind));
        assert!(
            times.iter().any(|at| *at >= run.stream.mid),
            "no {kind} after the split"
        );
    }
    for (index, key) in ALERT_TRIGGERS.iter().enumerate() {
        assert!(
            run.stream
                .messages
                .iter()
                .any(|kept| kept.items.contains(&Item::Trigger(index))),
            "{key} never fired"
        );
    }
    assert!(run.a2.1 > 0);
    // The regime is warming for about nine days: those days are warm-up.
    let (_, warm) = run.stream.days();
    assert!((8..=11).contains(&warm), "{warm}");
    assert!(!run.stream.dwells().is_empty() && !run.stream.gaps().is_empty());
    // The base row is the total.
    let all = run.stream.times(|_| true);
    assert_eq!(rows[0].0, BASE);
    assert_eq!(rows[0].1, run.stream.daily(&all));
    assert_eq!(run.stream.simulate(&BASE), all);
}

#[test]
fn a2_holds_across_a_trades_hole_before_the_split() {
    // The synthetic tape with a two-hour trades hole at day 9 12:00, after
    // the regime is ready and before the split at day 10 12:00: the regime
    // warms up again until about day 18.5, so the first label after the
    // re-warm is compared by monitors seeded before the hole and during it.
    let (from, to) = (TAPE_START, TAPE_START + TAPE_DAYS * DAY_MS);
    let (start, end) = (
        from + 9 * DAY_MS + 12 * HOUR_MS,
        from + 9 * DAY_MS + 14 * HOUR_MS - 1,
    );
    let mut events: Vec<MarketEvent> = synthetic_tape()
        .into_iter()
        .filter(|event| {
            let at = event.time().as_millis();
            at < to && !(matches!(event, MarketEvent::Trade(_)) && start <= at && at <= end)
        })
        .collect();
    events.push(MarketEvent::FeedGap(FeedGap {
        stream: EventStream::Trades,
        start: EventTime::from_millis(start),
        end: EventTime::from_millis(end),
        reason: GapReason::MissingData,
    }));
    events.sort();
    let run = measure(&mut Feed(events.into()), from, to);
    assert_eq!(run.rejections, 0);
    let flips = |pick: &dyn Fn(i64) -> bool| {
        run.stream
            .times(|kept| {
                kept.items
                    .iter()
                    .any(|i| i.kind() == AlertKind::RegimeChanged)
            })
            .into_iter()
            .filter(|at| pick(*at))
            .count()
    };
    assert!(
        flips(&|at| at < start) > 0,
        "the regime is not ready before the hole"
    );
    assert!(
        flips(&|at| at >= run.stream.mid) > 0,
        "no regime change after the re-warm"
    );
    assert_eq!(run.a2.2, 0, "{:?}", run.a2.3);
    assert!(run.a2.1 > 0);
}

/// A hand-built stream over four days, day 0 warming up; the split at the
/// start of day 2. Times are in minutes past day 0's start.
fn hand_stream() -> Stream {
    let min = |minutes: i64| minutes * 60_000;
    let day = |d: i64, minutes: i64| d * DAY_MS + min(minutes);
    let poc = |enter: bool| {
        if enter {
            Item::Entered {
                kind: LevelKind::Poc,
                price: 100,
            }
        } else {
            Item::Left {
                kind: LevelKind::Poc,
                price: 100,
            }
        }
    };
    let vah = |enter: bool| {
        if enter {
            Item::Entered {
                kind: LevelKind::Vah,
                price: 200,
            }
        } else {
            Item::Left {
                kind: LevelKind::Vah,
                price: 200,
            }
        }
    };
    let regime = |from, to| Item::Regime { from, to };
    use RegimeLabel::{High, Low, Medium};
    let mut stream = Stream::new(0, 4 * DAY_MS);
    stream.first_counted_day = Some(1);
    // 1h closes one minute past every hour.
    stream.h1 = (0..96).map(|h| h * HOUR_MS + min(1)).collect();
    stream.h4 = (0..24).map(|h| h * 4 * HOUR_MS + min(1)).collect();
    stream.d1 = (0..4).map(|d| d * DAY_MS + min(1)).collect();
    let kept = |at: i64, items: Vec<Item>| Kept { at, items };
    stream.messages = vec![
        // Day 0: warm-up, not counted.
        kept(day(0, 30), vec![poc(true)]),
        // Day 1.
        kept(day(1, 1), vec![Item::Report]),
        kept(day(1, 10), vec![poc(true)]),
        kept(day(1, 20), vec![poc(false)]),
        // Re-entry 5 min later.
        kept(day(1, 25), vec![poc(true), vah(true)]),
        kept(day(1, 61), vec![regime(Low, Medium)]),
        // Reverted at the next close.
        kept(day(1, 121), vec![regime(Medium, Low)]),
        kept(day(1, 200), vec![poc(false)]),
        // Re-entry 30 min later.
        kept(day(1, 230), vec![poc(true)]),
        kept(day(1, 300), vec![vah(false), Item::Trigger(0)]),
        // Day 2: second half.
        kept(day(2, 1), vec![Item::Report]),
        kept(day(2, 421), vec![regime(Low, High)]),
        kept(day(2, 600), vec![poc(false)]),
        // Day 3: one message.
        kept(day(3, 1), vec![Item::Report]),
    ];
    stream
}

#[test]
fn option_table_on_a_synthetic_stream() {
    let stream = hand_stream();
    let min = |minutes: i64| minutes * 60_000;
    // Days: 0 warm-up; 1 in the first half; 2 and 3 in the second.
    let (halves, warm) = stream.days();
    assert_eq!((halves, warm), ([vec![1], vec![2, 3]], 1));
    let all = stream.times(|_| true);
    assert_eq!(stream.per_day(&all), [vec![9], vec![3, 1]]);
    assert_eq!(stream.daily(&all), [(9, 9, 9), (1, 3, 3)]);

    // Re-entry gaps: POC 5 and 30 min on day 1; VAH none.
    assert_eq!(
        stream.visits(LevelKind::Poc).1,
        vec![min(5) as u64, min(30) as u64]
    );
    assert_eq!(
        stream.visits(LevelKind::Vah),
        (vec![(min(300) - min(25)) as u64], vec![])
    );
    assert_eq!(deciles(&stream.gaps()), vec![min(5) as u64, min(30) as u64]);
    // Flips at the 1h closes 26 (day 1 01:01), 27 and 56 (day 2 07:01) of
    // 96; the last holds to the end, 41 closes.
    let flips = stream.flips();
    assert_eq!(
        flips.iter().map(|f| (f.close, f.held)).collect::<Vec<_>>(),
        vec![(26, 1), (27, 29), (56, 41)]
    );
    assert_eq!(stream.dwells(), vec![1, 29]);
    assert_eq!(deciles(&stream.dwells()), vec![1, 29]);
    assert_eq!(stream.reverted(), (1, 3));

    let at = |times: Vec<i64>| stream.per_day(&times);
    // POC dropped: day 1 loses its four POC-only messages (10, 20, 200,
    // 230); 25 keeps its VAH. Day 2 loses 600.
    let poc = Policy {
        drop_poc: true,
        ..BASE
    };
    assert_eq!(at(stream.simulate(&poc)), [vec![5], vec![2, 1]]);
    // Report-only regime: day 1 loses two, day 2 one.
    let report_only = Policy {
        regime: RegimeRule::ReportOnly,
        ..BASE
    };
    assert_eq!(at(stream.simulate(&report_only)), [vec![7], vec![2, 1]]);
    // Dwell 1 is the base; dwell 29: the reverted flip is silent, the
    // second is alerted at its 29th close (close 55, day 2 06:01), the
    // third (held 41) at close 84 (day 3 11:01).
    let dwell = |k| Policy {
        regime: RegimeRule::Dwell(k),
        ..BASE
    };
    assert_eq!(stream.simulate(&dwell(1)), stream.simulate(&BASE));
    assert_eq!(at(stream.simulate(&dwell(29))), [vec![7], vec![3, 2]]);
    // Merge gap 5 min: the 20/25 pair merges (25 keeps its VAH enter); the
    // other leaves are delayed 5 min as messages of their own.
    let merge = |g: i64| Policy {
        merge: Some(min(g) as u64),
        ..BASE
    };
    let merged = stream.simulate(&merge(5));
    assert!(!merged.contains(&(DAY_MS + min(20))));
    assert!(merged.contains(&(DAY_MS + min(25))));
    assert!(merged.contains(&(DAY_MS + min(205))));
    assert!(merged.contains(&(DAY_MS + min(305))));
    assert!(merged.contains(&(DAY_MS + min(300))), "its trigger stays");
    // Day 1: 1, 10, 25, 61, 121, 205 (delayed), 230, 300, 305 → 9.
    assert_eq!(at(merged), [vec![9], vec![3, 1]]);
    // Merge gap 30 min: both POC re-entries merge, the VAH leave is
    // delayed to 330. Day 1: 1, 10, 25 (VAH), 61, 121, 300, 330 → 7.
    assert_eq!(at(stream.simulate(&merge(30))), [vec![7], vec![3, 1]]);
    // Cadence: every 4h close but the daily one adds 5 a day, every 1h
    // close 23.
    let h4 = Policy {
        cadence: Cadence::H4,
        ..BASE
    };
    let h1 = Policy {
        cadence: Cadence::H1,
        ..BASE
    };
    assert_eq!(at(stream.simulate(&h4)), [vec![14], vec![8, 6]]);
    assert_eq!(at(stream.simulate(&h1)), [vec![32], vec![26, 24]]);

    // The grid: 2 POC × (alert, report-only, dwell 1, dwell 29) × (none,
    // 5 min, 30 min), plus two cadence rows; nine rows change one factor.
    let rows = policies(&stream);
    assert_eq!(rows.len(), 2 * 4 * 3 + 2);
    assert_eq!(rows.iter().filter(|p| one_factor(p)).count(), 9);

    // Bursts and the FIFO.
    let times = [0, 1_000, 2_000, 10_000, HOUR_MS + 1_000];
    assert_eq!(burst(&times, HOUR_MS), 4);
    assert_eq!(fifo(&times, PACING_MS), (2, 4_000));
    // Silence of day 1: from 05:00 to the day's end.
    let [first, _] = silence(&stream, &all);
    assert_eq!(first, vec![(DAY_MS - min(300)) as u64]);
}
