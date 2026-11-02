//! Renders and delivers the alerts of ADR-059 (proposed; brief §17,
//! ADR-010).
//!
//! The decision is the domain's ([`AlertMonitor`]); this module bundles the
//! alerts of one engine event into one [`AlertMessage`], renders its plain
//! text ([`render`]) and hands it to an [`AlertGateway`] (ADR-059 D1, D3).
//! Live ingestion and the replay dry-run share it, so the alert log of a
//! replay is what live would have sent (ADR-019).
//!
//! [`drive_alerts`] runs the monitor through the one rejection policy of
//! long runs, [`drive_tolerant_observed`] (ADR-039 D9), and guards it as
//! [`sample_contexts`](crate::conditional::sample_contexts) does: the
//! monitor sees **accepted events only**. A gateway error is counted and
//! the drive goes on. [`drive_chain_alerts`] is the same on the history
//! chain, where an uncrossable hole restarts a fresh engine (ADR-052 D4);
//! the monitor then closes its open visits without an alert (ADR-059 D2b).
//!
//! [`alert_window`] and [`alert_chain_window`] open a replay window with
//! its [`AlertProvenance`] and drive a fresh engine and monitor through it,
//! emitting from `emit_from`. [`ReportService`] implements
//! [`GenerateMarketReport`].
//!
//! The text follows the wording rule of ADR-059 D4: no word of the
//! ADR-053 naming list and no trade-instruction word ([`wording_hits`]),
//! no price but levels and rule thresholds, and the event time in UTC in
//! every message. Plain ASCII without markup, at most
//! [`MAX_MESSAGE_CHARS`] characters by construction.

use crate::{ChainDrive, drive_chain_observed, drive_tolerant, drive_tolerant_observed};
use mie_domain::alert::{
    Alert, AlertKind, AlertMonitor, AlertProvenance, CloseTest, Guide, MarketReport,
};
use mie_domain::event::MarketEvent;
use mie_domain::feature::{FeatureKey, FeatureValue, catalog};
use mie_domain::location::{AuctionState, LevelKind, LocationLevel, Region};
use mie_domain::num::Price;
use mie_domain::research::DataVersion;
use mie_domain::state::{MarketStateEngine, StateError};
use mie_domain::time::EventTime;
use mie_domain::trigger::{TriggerEvent, TriggerEvidence};
use mie_ports::inbound::{GenerateMarketReport, UseCaseError};
use mie_ports::outbound::{
    AlertGateway, AlertMessage, HistoricalDataProvider, MarketDataProvider, ProviderError, Replay,
    ReplayWindow,
};
use std::cell::Cell;
use std::fmt::Write as _;

/// The most characters a message may have: Telegram's limit on a message
/// text, which ADR-059 D4 adopts for every gateway.
pub const MAX_MESSAGE_CHARS: usize = 4_096;

/// The words no message may contain (ADR-059 D4): the ADR-053 naming list
/// and the trade-instruction words. Matched as whole words, case-blind,
/// with a plural `s`; `take profit` as two consecutive words.
pub const FORBIDDEN_WORDS: [&str; 19] = [
    "bullish",
    "bearish",
    "bull",
    "bear",
    "long",
    "short",
    "signal",
    "reversal",
    "continuation",
    "entry",
    "buy",
    "sell",
    "order",
    "position",
    "stop",
    "target",
    "leverage",
    "size",
    "take profit",
];

/// The forbidden words of [`FORBIDDEN_WORDS`] in `text`, in text order.
pub fn wording_hits(text: &str) -> Vec<&'static str> {
    let lower = text.to_ascii_lowercase();
    let words: Vec<&str> = lower
        .split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .collect();
    let mut hits = Vec::new();
    for (index, word) in words.iter().enumerate() {
        for forbidden in FORBIDDEN_WORDS {
            let found = match forbidden.split_once(' ') {
                Some((first, second)) => {
                    *word == first && words.get(index + 1).is_some_and(|next| *next == second)
                }
                None => {
                    *word == forbidden
                        || word.strip_suffix('s') == Some(forbidden)
                        || (forbidden == "entry" && *word == "entries")
                }
            };
            if found {
                hits.push(forbidden);
            }
        }
    }
    hits
}

/// `YYYY-MM-DD HH:MM:SS UTC` of an event time (Howard Hinnant's
/// `civil_from_days`); milliseconds are dropped.
pub fn utc(time: EventTime) -> String {
    let millis = time.as_millis();
    let days = millis.div_euclid(86_400_000);
    let seconds = millis.rem_euclid(86_400_000) / 1_000;
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    format!(
        "{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02} UTC",
        seconds / 3_600,
        seconds / 60 % 60,
        seconds % 60
    )
}

/// A price without trailing zeros: `61036.6`, `61000`.
fn amount(price: Price) -> String {
    let text = price.to_string();
    match text.split_once('.') {
        Some((whole, fraction)) => {
            let fraction = fraction.trim_end_matches('0');
            if fraction.is_empty() {
                whole.to_owned()
            } else {
                format!("{whole}.{fraction}")
            }
        }
        None => text,
    }
}

fn kind_name(kind: LevelKind) -> &'static str {
    match kind {
        LevelKind::Poc => "POC",
        LevelKind::Vah => "VAH",
        LevelKind::Val => "VAL",
        _ => "level",
    }
}

fn kind_text(kind: LevelKind) -> &'static str {
    match kind {
        LevelKind::Poc => "prior-day point of control",
        LevelKind::Vah => "prior-day value-area high",
        LevelKind::Val => "prior-day value-area low",
        _ => "level",
    }
}

fn zone(level: &LocationLevel) -> String {
    format!("{}-{}", amount(level.low), amount(level.high))
}

fn region_text(region: Region) -> &'static str {
    match region {
        Region::Above => "above value",
        Region::In => "inside value",
        Region::Below => "below value",
    }
}

fn probe_text(toward: Region) -> &'static str {
    match toward {
        Region::Above => "above value",
        Region::In => "back into value",
        Region::Below => "below value",
    }
}

fn close_text(test: &CloseTest) -> String {
    match *test {
        CloseTest::Above(price) => format!("above {}", amount(price)),
        CloseTest::AtOrAbove(price) => format!("at or above {}", amount(price)),
        CloseTest::Below(price) => format!("below {}", amount(price)),
        CloseTest::Between { low, high } => {
            format!("between {} and {}", amount(low), amount(high))
        }
        CloseTest::Within { low, high } => {
            format!("at or above {} and below {}", amount(low), amount(high))
        }
    }
}

fn trigger_name(key: FeatureKey) -> &'static str {
    if key == catalog::TRIGGER_FAILED_AUCTION_V1.key {
        "Failed auction"
    } else if key == catalog::TRIGGER_FAILED_RECLAIM_V1.key {
        "Failed reclaim"
    } else if key == catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key {
        "Breakout acceptance"
    } else {
        "Trigger"
    }
}

fn trigger_meaning(key: FeatureKey) -> &'static str {
    if key == catalog::TRIGGER_FAILED_AUCTION_V1.key {
        "a probe beyond value closed back inside value"
    } else if key == catalog::TRIGGER_FAILED_RECLAIM_V1.key {
        "a probe back into value closed back outside it"
    } else if key == catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key {
        "the probe beyond value counted its acceptance closes; that region is now accepted"
    } else {
        "a rule fired"
    }
}

fn state_text(state: AuctionState) -> String {
    state.to_string().replace('_', " ")
}

fn guide_lines(guide: &Guide, out: &mut String) {
    let _ = writeln!(
        out,
        "  Accepted region: {}. A probe {} starts with a 1m close {}.",
        region_text(guide.accepted),
        probe_text(guide.toward),
        close_text(&guide.starts)
    );
    let _ = writeln!(
        out,
        "  {} ({}) if a 1m close is back {} ({}) before {} closes count {}.",
        trigger_name(guide.failure),
        guide.failure,
        region_text(guide.accepted),
        close_text(&guide.fails),
        guide.closes,
        close_text(&guide.counts)
    );
    match guide.acceptance {
        Some(key) => {
            let _ = writeln!(
                out,
                "  {} ({key}) at the {}th close counted {}.",
                trigger_name(key),
                guide.closes,
                close_text(&guide.counts)
            );
        }
        None => {
            let _ = writeln!(
                out,
                "  Value accepted (no @1 trigger) at the {}th close counted {}.",
                guide.closes,
                close_text(&guide.counts)
            );
        }
    }
    out.push_str("  Each outcome invalidates the other.\n");
}

/// The edge a trigger is anchored at: VAH or VAL of its auction status.
fn trigger_edge(trigger: &TriggerEvent) -> &'static str {
    match &trigger.evidence {
        TriggerEvidence::Auction(status) if trigger.anchor() == status.vah => "VAH",
        TriggerEvidence::Auction(status) if trigger.anchor() == status.val => "VAL",
        _ => "anchor",
    }
}

fn report_lines(report: &MarketReport, out: &mut String) {
    match report.as_of {
        Some(as_of) => {
            let _ = writeln!(out, "- Market report as of {}", utc(as_of));
        }
        None => out.push_str("- Market report: no event yet\n"),
    }
    let not_ready = |ready: bool| if ready { "warming up" } else { "unavailable" };
    match &report.last_price {
        FeatureValue::Ready(price) => {
            let _ = writeln!(out, "  Last price {}", amount(*price));
        }
        other => {
            let _ = writeln!(
                out,
                "  Last price {}",
                not_ready(matches!(other, FeatureValue::WarmingUp { .. }))
            );
        }
    }
    match &report.regime {
        FeatureValue::Ready(regime) => {
            let _ = writeln!(
                out,
                "  Volatility regime {}, ATR percentile {} ({})",
                regime.label,
                regime.atr_percentile.value(),
                regime.feature
            );
        }
        other => {
            let _ = writeln!(
                out,
                "  Volatility regime {}",
                not_ready(matches!(other, FeatureValue::WarmingUp { .. }))
            );
        }
    }
    match &report.vwap {
        FeatureValue::Ready(vwap) => {
            let _ = writeln!(out, "  UTC-day VWAP {}", amount(vwap.price));
        }
        other => {
            let _ = writeln!(
                out,
                "  UTC-day VWAP {}",
                not_ready(matches!(other, FeatureValue::WarmingUp { .. }))
            );
        }
    }
    match &report.auction {
        FeatureValue::Ready(status) => {
            let _ = writeln!(
                out,
                "  Prior-day auction: {}; accepted region {} (value {} to {})",
                state_text(status.state),
                region_text(status.accepted),
                amount(status.val),
                amount(status.vah)
            );
        }
        other => {
            let _ = writeln!(
                out,
                "  Prior-day auction {}",
                not_ready(matches!(other, FeatureValue::WarmingUp { .. }))
            );
        }
    }
    match &report.levels {
        FeatureValue::Ready(levels) if !levels.is_empty() => {
            out.push_str("  Monitored prior-day levels:\n");
            for level in levels {
                let _ = writeln!(
                    out,
                    "    {} {} zone {}{}",
                    kind_name(level.kind),
                    amount(level.price),
                    zone(level),
                    if level.in_zone { ", in zone" } else { "" }
                );
            }
        }
        FeatureValue::Ready(_) => out.push_str("  Monitored prior-day levels: none yet\n"),
        other => {
            let _ = writeln!(
                out,
                "  Monitored prior-day levels {}",
                not_ready(matches!(other, FeatureValue::WarmingUp { .. }))
            );
        }
    }
    let active: Vec<String> = report
        .active()
        .map(|level| format!("{} {}", kind_name(level.kind), amount(level.price)))
        .collect();
    let _ = writeln!(
        out,
        "  Active monitors: {}",
        if active.is_empty() {
            "none".to_owned()
        } else {
            active.join(", ")
        }
    );
}

/// The text of one alert, a block of lines each ending in `\n`.
fn alert_lines(alert: &Alert) -> String {
    let mut out = String::new();
    match alert {
        Alert::ZoneEntered {
            level,
            cause,
            guide,
            ..
        } => {
            let how = match cause {
                mie_domain::location::EnterCause::Arrived => "price arrived",
                mie_domain::location::EnterCause::Created => "level formed with price in its zone",
            };
            let _ = writeln!(
                out,
                "- Entered the {} zone {} ({} {}), {how}.",
                kind_name(level.kind),
                zone(level),
                kind_text(level.kind),
                amount(level.price)
            );
            match (level.kind, guide) {
                (_, Some(guide)) => guide_lines(guide, &mut out),
                (LevelKind::Vah | LevelKind::Val, None) => out.push_str(
                    "  No guide: the prior-day auction state is warming up or unavailable.\n",
                ),
                _ => {}
            }
        }
        Alert::ZoneLeft { level, .. } => {
            let _ = writeln!(
                out,
                "- Left the {} zone {} ({} {}): a whole 1m bar closed away from it.",
                kind_name(level.kind),
                zone(level),
                kind_text(level.kind),
                amount(level.price)
            );
        }
        Alert::TriggerFired(trigger) => {
            let _ = writeln!(
                out,
                "- {} ({}) at {} {}: {}.",
                trigger_name(trigger.feature),
                trigger.feature,
                trigger_edge(trigger),
                amount(trigger.anchor()),
                trigger_meaning(trigger.feature)
            );
        }
        Alert::RegimeChanged { from, to, .. } => {
            let _ = writeln!(
                out,
                "- Volatility regime changed: {from} -> {}, ATR percentile {} ({}).",
                to.label,
                to.atr_percentile.value(),
                to.feature
            );
        }
        Alert::Report(report) => report_lines(report, &mut out),
    }
    out
}

/// The footer of every message.
const FOOTER: &str = "Informational only.\n";

/// The plain text of the alerts of one event (ADR-059 D4): a header with
/// the event time in UTC, one block per alert in bundling order, a footer.
/// Blocks that would take the text past [`MAX_MESSAGE_CHARS`] are replaced
/// by one line counting them, so the bound holds by construction. Empty
/// for no alerts.
pub fn render(alerts: &[Alert]) -> String {
    let Some(first) = alerts.first() else {
        return String::new();
    };
    let mut text = format!("MIE alert {} (event time)\n", utc(first.known_at()));
    // Room for the omission line and the footer.
    let reserve = FOOTER.len() + 64;
    for (index, alert) in alerts.iter().enumerate() {
        let block = alert_lines(alert);
        if text.len() + block.len() + reserve > MAX_MESSAGE_CHARS {
            let _ = writeln!(
                text,
                "- {} more alerts of this event are in the decision log.",
                alerts.len() - index
            );
            break;
        }
        text.push_str(&block);
    }
    text.push_str(FOOTER);
    text
}

/// What one alert drive did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AlertReport {
    /// Events delivered, rejected ones included.
    pub events: u64,
    /// Events the engine rejected; the monitor saw none of them.
    pub rejections: u64,
    /// Messages handed to the gateway: one per event with alerts.
    pub messages: u64,
    /// Alerts per kind, in [`AlertKind::ALL`] order.
    pub alerts: [u64; 5],
    /// Messages the gateway did not take.
    pub delivery_errors: u64,
    /// The times of the history chain's restarts (ADR-052 D4); empty off
    /// the chain.
    pub restarts: Vec<EventTime>,
}

/// Bundles the alerts of one accepted event into a message and delivers it.
struct Bundler<'a, G: ?Sized> {
    monitor: &'a mut AlertMonitor,
    gateway: &'a mut G,
    messages: u64,
    alerts: [u64; 5],
    delivery_errors: u64,
}

impl<G: AlertGateway + ?Sized> Bundler<'_, G> {
    fn observe(&mut self, event: &MarketEvent, engine: &MarketStateEngine) {
        let mut alerts = Vec::new();
        self.monitor
            .observe(event, engine, |alert| alerts.push(alert));
        if alerts.is_empty() {
            return;
        }
        for alert in &alerts {
            let kind = AlertKind::ALL
                .iter()
                .position(|kind| *kind == alert.kind())
                .expect("every kind is listed");
            self.alerts[kind] += 1;
        }
        let message = AlertMessage {
            known_at: event.time(),
            event: event.canonical_key(),
            text: render(&alerts),
            alerts,
        };
        self.messages += 1;
        if self.gateway.deliver(&message).is_err() {
            self.delivery_errors += 1;
        }
    }
}

/// Feeds every event from `provider` into `engine`, every accepted one into
/// `monitor`, and delivers one message per event with alerts to `gateway`
/// (ADR-059 D1, D3). A domain rejection does not stop the drive:
/// `on_rejection` gets the error, the monitor does not see the event, and
/// driving resumes (ADR-039 D9). A delivery error is counted and driving
/// resumes.
///
/// # Errors
///
/// The first provider failure. Events consumed before it stay applied, and
/// their messages are delivered.
pub fn drive_alerts<P, G>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    monitor: &mut AlertMonitor,
    on_rejection: impl FnMut(&StateError),
    gateway: &mut G,
) -> Result<AlertReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
    G: AlertGateway + ?Sized,
{
    drive_alerts_observed(provider, engine, monitor, on_rejection, gateway, |_, _| {})
}

/// [`drive_alerts`], calling `observe` after each accepted event, once its
/// message is delivered: a measurement reads the same engine there.
///
/// # Errors
///
/// As [`drive_alerts`].
pub fn drive_alerts_observed<P, G>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    monitor: &mut AlertMonitor,
    mut on_rejection: impl FnMut(&StateError),
    gateway: &mut G,
    mut observe: impl FnMut(&MarketEvent, &MarketStateEngine),
) -> Result<AlertReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
    G: AlertGateway + ?Sized,
{
    // Set by a rejection, cleared by the observer that follows it: the
    // driver calls `on_rejection` and then `observe` for the same event.
    let rejected = Cell::new(false);
    let mut rejections = 0;
    let mut bundler = Bundler {
        monitor,
        gateway,
        messages: 0,
        alerts: [0; 5],
        delivery_errors: 0,
    };
    let events = drive_tolerant_observed(
        provider,
        engine,
        |error| {
            rejected.set(true);
            rejections += 1;
            on_rejection(error);
        },
        |event, engine| {
            if !rejected.replace(false) {
                bundler.observe(event, engine);
                observe(event, engine);
            }
        },
    )?;
    Ok(AlertReport {
        events,
        rejections,
        messages: bundler.messages,
        alerts: bundler.alerts,
        delivery_errors: bundler.delivery_errors,
        restarts: Vec::new(),
    })
}

/// [`drive_alerts`] on the history chain ([`drive_chain_observed`],
/// ADR-052 D4): an uncrossable hole restarts a fresh engine, and the
/// monitor, which carries on, closes its open visits without an alert
/// (ADR-059 D2b). The restarts are in [`AlertReport::restarts`].
///
/// # Errors
///
/// As [`drive_alerts`].
pub fn drive_chain_alerts<P, G>(
    provider: &mut P,
    engine: &mut MarketStateEngine,
    monitor: &mut AlertMonitor,
    mut on_rejection: impl FnMut(&StateError),
    gateway: &mut G,
) -> Result<AlertReport, ProviderError>
where
    P: MarketDataProvider + ?Sized,
    G: AlertGateway + ?Sized,
{
    let rejected = Cell::new(false);
    let mut rejections = 0;
    let mut bundler = Bundler {
        monitor,
        gateway,
        messages: 0,
        alerts: [0; 5],
        delivery_errors: 0,
    };
    let ChainDrive { events, restarts } = drive_chain_observed(
        provider,
        engine,
        |error| {
            rejected.set(true);
            rejections += 1;
            on_rejection(error);
        },
        |event, engine| {
            if !rejected.replace(false) {
                bundler.observe(event, engine);
            }
        },
    )?;
    Ok(AlertReport {
        events,
        rejections,
        messages: bundler.messages,
        alerts: bundler.alerts,
        delivery_errors: bundler.delivery_errors,
        restarts,
    })
}

/// Decides the alerts of the replay of `window` from `history` in a fresh
/// engine and a fresh [`AlertMonitor::new`]`(emit_from, …)`, delivering
/// them to `gateway`: events before `emit_from` warm the engine and the
/// monitor up without a message. Returns the run's provenance and what the
/// drive did.
///
/// # Errors
///
/// [`ProviderError`] when the replay cannot be opened or its stream fails.
pub fn alert_window<H, G>(
    history: &H,
    window: ReplayWindow,
    emit_from: Option<EventTime>,
    gateway: &mut G,
) -> Result<(AlertProvenance, AlertReport), ProviderError>
where
    H: HistoricalDataProvider,
    G: AlertGateway + ?Sized,
{
    let Replay {
        mut stream,
        dataset,
    } = history.replay(window)?;
    let mut engine = MarketStateEngine::new();
    let provenance = AlertProvenance::new(DataVersion::from(&dataset), &engine);
    let mut monitor = AlertMonitor::new(emit_from, engine.state());
    let report = drive_alerts(&mut stream, &mut engine, &mut monitor, |_| {}, gateway)?;
    Ok((provenance, report))
}

/// [`alert_window`] on the history chain, through [`drive_chain_alerts`].
///
/// # Errors
///
/// As [`alert_window`].
pub fn alert_chain_window<H, G>(
    history: &H,
    window: ReplayWindow,
    emit_from: Option<EventTime>,
    gateway: &mut G,
) -> Result<(AlertProvenance, AlertReport), ProviderError>
where
    H: HistoricalDataProvider,
    G: AlertGateway + ?Sized,
{
    let Replay {
        mut stream,
        dataset,
    } = history.replay(window)?;
    let mut engine = MarketStateEngine::new();
    let provenance = AlertProvenance::new(DataVersion::from(&dataset), &engine);
    let mut monitor = AlertMonitor::new(emit_from, engine.state());
    let report = drive_chain_alerts(&mut stream, &mut engine, &mut monitor, |_| {}, gateway)?;
    Ok((provenance, report))
}

/// [`GenerateMarketReport`] over any [`HistoricalDataProvider`]: replays
/// the window into a fresh engine with the rejection policy of
/// [`ReplayService`](crate::ReplayService), or on the history chain.
#[derive(Debug)]
pub struct ReportService<H> {
    history: H,
    chain: bool,
}

impl<H: HistoricalDataProvider> ReportService<H> {
    /// The service over a historical data source.
    pub fn new(history: H) -> Self {
        Self {
            history,
            chain: false,
        }
    }

    /// The service over the history chain: an uncrossable hole restarts a
    /// fresh engine (ADR-052 D4).
    pub fn chain(history: H) -> Self {
        Self {
            history,
            chain: true,
        }
    }
}

impl<H: HistoricalDataProvider> GenerateMarketReport for ReportService<H> {
    fn report(&self, window: ReplayWindow) -> Result<MarketReport, UseCaseError> {
        let Replay { mut stream, .. } = self.history.replay(window)?;
        let mut engine = MarketStateEngine::new();
        if self.chain {
            drive_chain_observed(&mut stream, &mut engine, |_| {}, |_, _| {})?;
        } else {
            drive_tolerant(&mut stream, &mut engine, |_| {})?;
        }
        Ok(MarketReport::read(engine.state()))
    }
}

#[cfg(test)]
mod tests;
