use super::*;
use mie_domain::event::{Aggressor, Trade};
use mie_domain::num::{Qty, SCALE};
use mie_ports::outbound::AlertError;
use mie_ports::raw::DatasetVersion;
use std::collections::VecDeque;

const DAY: i64 = 86_400_000;
const HOUR: i64 = 3_600_000;

/// 2026-01-01 00:00 UTC.
const START: i64 = 1_767_225_600_000;

struct Feed(VecDeque<MarketEvent>);

impl MarketDataProvider for Feed {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        Ok(self.0.pop_front())
    }
}

fn feed(events: &[MarketEvent]) -> Feed {
    Feed(events.iter().cloned().collect())
}

/// A replay source over fixed events, with a fixed dataset version.
struct History(Vec<MarketEvent>, DatasetVersion);

impl HistoricalDataProvider for History {
    type Stream = Feed;

    fn replay(&self, window: ReplayWindow) -> Result<Replay<Feed>, ProviderError> {
        let events: Vec<MarketEvent> = self
            .0
            .iter()
            .filter(|event| window.contains(event.time()))
            .cloned()
            .collect();
        Ok(Replay {
            stream: feed(&events),
            dataset: self.1.clone(),
        })
    }
}

/// A seeded generator, so the tape needs no `rand`.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: u64) -> i64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        i64::try_from((self.0 >> 11) % bound).unwrap()
    }
}

/// Three weeks of trades around 62 000 USDT, 10 s to 60 s apart, on a walk
/// whose trend cycles hour by hour, so price leaves value and comes back:
/// every alert kind occurs, the regime from about day 9.
fn tape() -> &'static [MarketEvent] {
    static TAPE: std::sync::OnceLock<Vec<MarketEvent>> = std::sync::OnceLock::new();
    TAPE.get_or_init(|| {
        let mut lcg = Lcg(0x6d69_6500_0038_0001);
        let mut events = Vec::new();
        let mut time = START + 123_456;
        let mut cents: i64 = 62_000 * 100;
        let mut trade_id = 0;
        while time < START + 21 * DAY {
            time += 10_000 + lcg.below(50_000);
            let trend = ((time - START) / HOUR * 7 % 9) - 4;
            cents += trend * 100 + lcg.below(2_401) - 1_200;
            trade_id += 1;
            events.push(MarketEvent::Trade(Trade {
                time: EventTime::from_millis(time),
                trade_id,
                price: Price::from_units(cents * (SCALE / 100)),
                qty: Qty::from_units(1 + lcg.below(100_000_000)),
                aggressor: if lcg.below(2) == 0 {
                    Aggressor::Buy
                } else {
                    Aggressor::Sell
                },
            }));
        }
        events
    })
}

/// A gateway that records every message, refusing those `refuse` picks.
#[derive(Default)]
struct Recording {
    messages: Vec<AlertMessage>,
    refused: u64,
    refuse_every: Option<u64>,
}

impl AlertGateway for Recording {
    fn deliver(&mut self, message: &AlertMessage) -> Result<(), AlertError> {
        self.messages.push(message.clone());
        if let Some(every) = self.refuse_every
            && (self.messages.len() as u64).is_multiple_of(every)
        {
            self.refused += 1;
            return Err(AlertError::Unavailable("refused by the test".to_owned()));
        }
        Ok(())
    }
}

/// Every message of the tape, cached: the tape takes a moment to replay.
fn messages() -> &'static [AlertMessage] {
    static MESSAGES: std::sync::OnceLock<Vec<AlertMessage>> = std::sync::OnceLock::new();
    MESSAGES.get_or_init(|| {
        let mut engine = MarketStateEngine::new();
        let mut monitor = AlertMonitor::new(None, engine.state());
        let mut gateway = Recording::default();
        let report = drive_alerts(
            &mut feed(tape()),
            &mut engine,
            &mut monitor,
            |error| panic!("the tape is rejected: {error}"),
            &mut gateway,
        )
        .unwrap();
        assert_eq!(report.messages, gateway.messages.len() as u64);
        gateway.messages
    })
}

/// The first alert `pick` accepts.
fn first(pick: impl Fn(&Alert) -> bool) -> Alert {
    messages()
        .iter()
        .flat_map(|message| &message.alerts)
        .find(|alert| pick(alert))
        .expect("the tape has the alert")
        .clone()
}

fn entered(kind: LevelKind, guided: bool) -> Alert {
    first(|alert| {
        matches!(alert, Alert::ZoneEntered { level, guide, .. }
            if level.kind == kind && guide.is_some() == guided)
    })
}

fn trigger(key: FeatureKey) -> Alert {
    first(|alert| matches!(alert, Alert::TriggerFired(t) if t.feature == key))
}

#[test]
fn utc_prints_the_civil_time() {
    assert_eq!(utc(EventTime::from_millis(0)), "1970-01-01 00:00:00 UTC");
    assert_eq!(
        utc(EventTime::from_millis(
            START + 13 * HOUR + 5 * 60_000 + 7_999
        )),
        "2026-01-01 13:05:07 UTC"
    );
    // 2024-02-29, a leap day.
    assert_eq!(
        utc(EventTime::from_millis(1_709_164_800_000)),
        "2024-02-29 00:00:00 UTC"
    );
    assert_eq!(utc(EventTime::from_millis(-1)), "1969-12-31 23:59:59 UTC");
}

#[test]
fn the_wording_guard_finds_whole_words() {
    assert_eq!(wording_hits("Go Short here"), vec!["short"]);
    assert_eq!(
        wording_hits("set a take  profit; two entries"),
        vec!["take profit", "entry"]
    );
    assert_eq!(
        wording_hits("stops, Targets, BUY"),
        vec!["stop", "target", "buy"]
    );
    assert_eq!(wording_hits("bullish/bearish"), vec!["bullish", "bearish"]);
    // Words that merely contain one are fine.
    assert!(wording_hits("the longest shortest border, a resize, entered").is_empty());
}

/// The texts every alert kind renders to (ADR-059 D4, #38 AC 1).
#[test]
fn every_alert_kind_renders_to_its_golden() {
    let cases = [
        (
            entered(LevelKind::Vah, true),
            concat!(
                "MIE alert 2026-01-02 00:06:41 UTC (event time)\n",
                "- Entered the VAH zone 62350-62360 (prior-day value-area high 62360), price arrived.\n",
                "  Accepted region: inside value. A probe above value starts with a 1m close above 62397.416.\n",
                "  Failed auction (trigger.failed_auction@1) if a 1m close is back inside value (between 61827.074 and 62322.584) before 60 closes count at or above 62360.\n",
                "  Breakout acceptance (trigger.breakout_acceptance@1) at the 60th close counted at or above 62360.\n",
                "  Each outcome invalidates the other.\n",
                "Informational only.\n",
            ),
        ),
        (
            entered(LevelKind::Val, true),
            concat!(
                "MIE alert 2026-01-03 02:00:48 UTC (event time)\n",
                "- Entered the VAL zone 61970-61980 (prior-day value-area low 61970), price arrived.\n",
                "  Accepted region: inside value. A probe below value starts with a 1m close below 61932.818.\n",
                "  Failed auction (trigger.failed_auction@1) if a 1m close is back inside value (between 62007.182 and 62372.554) before 60 closes count below 61970.\n",
                "  Breakout acceptance (trigger.breakout_acceptance@1) at the 60th close counted below 61970.\n",
                "  Each outcome invalidates the other.\n",
                "Informational only.\n",
            ),
        ),
        (
            entered(LevelKind::Vah, false),
            concat!(
                "MIE alert 2026-01-08 00:00:09 UTC (event time)\n",
                "- Entered the VAH zone 60900-60910 (prior-day value-area high 60910), level formed with price in its zone.\n",
                "  No guide: the prior-day auction state is warming up or unavailable.\n",
                "Informational only.\n",
            ),
        ),
        (
            entered(LevelKind::Poc, false),
            concat!(
                "MIE alert 2026-01-02 03:24:38 UTC (event time)\n",
                "- Entered the POC zone 62090-62100 (prior-day point of control 62095), price arrived.\n",
                "Informational only.\n",
            ),
        ),
        (
            first(|alert| alert.kind() == AlertKind::ZoneLeft),
            concat!(
                "MIE alert 2026-01-02 00:28:17 UTC (event time)\n",
                "- Left the VAH zone 62350-62360 (prior-day value-area high 62360): a whole 1m bar closed away from it.\n",
                "Informational only.\n",
            ),
        ),
        (
            trigger(catalog::TRIGGER_FAILED_AUCTION_V1.key),
            concat!(
                "MIE alert 2026-01-02 11:31:11 UTC (event time)\n",
                "- Failed auction (trigger.failed_auction@1) at VAH 62360: a probe beyond value closed back inside value.\n",
                "Informational only.\n",
            ),
        ),
        (
            trigger(catalog::TRIGGER_FAILED_RECLAIM_V1.key),
            concat!(
                "MIE alert 2026-01-03 07:31:19 UTC (event time)\n",
                "- Failed reclaim (trigger.failed_reclaim@1) at VAH 62410: a probe back into value closed back outside it.\n",
                "Informational only.\n",
            ),
        ),
        (
            trigger(catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key),
            concat!(
                "MIE alert 2026-01-02 01:24:14 UTC (event time)\n",
                "- Breakout acceptance (trigger.breakout_acceptance@1) at VAH 62360: the probe beyond value counted its acceptance closes; that region is now accepted.\n",
                "Informational only.\n",
            ),
        ),
        (
            first(|alert| alert.kind() == AlertKind::RegimeChanged),
            concat!(
                "MIE alert 2026-01-10 04:00:55 UTC (event time)\n",
                "- Volatility regime changed: EXTREME -> HIGH, ATR percentile 56.5 (volatility.regime.1h@1).\n",
                "Informational only.\n",
            ),
        ),
        (
            first(|alert| matches!(alert, Alert::Report(r) if r.regime.is_ready())),
            concat!(
                "MIE alert 2026-01-10 00:00:18 UTC (event time)\n",
                "- Market report as of 2026-01-10 00:00:18 UTC\n",
                "  Last price 61256.09\n",
                "  Volatility regime EXTREME, ATR percentile 92 (volatility.regime.1h@1)\n",
                "  UTC-day VWAP warming up\n",
                "  Prior-day auction warming up\n",
                "  Monitored prior-day levels:\n",
                "    VAL 61110 zone 61110-61120\n",
                "    POC 61575 zone 61570-61580\n",
                "    VAH 61610 zone 61600-61610\n",
                "  Active monitors: none\n",
                "Informational only.\n",
            ),
        ),
    ];
    let mut failures = Vec::new();
    for (alert, golden) in cases {
        let text = render(std::slice::from_ref(&alert));
        if text != golden {
            failures.push(format!("{alert}\n---\n{text}---"));
        }
    }
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}

#[test]
fn a_bundled_message_renders_to_its_golden() {
    // The first message with three alerts or more: a rollover's report with
    // the new day's zone changes.
    let message = messages()
        .iter()
        .find(|message| message.alerts.len() >= 3)
        .expect("the tape bundles");
    assert_eq!(message.text, render(&message.alerts));
    assert_eq!(
        message.text,
        concat!(
            "MIE alert 2026-01-11 00:00:29 UTC (event time)\n",
            "- Entered the POC zone 61300-61310 (prior-day point of control 61305), level formed with price in its zone.\n",
            "- Volatility regime changed: HIGH -> EXTREME, ATR percentile 92.5 (volatility.regime.1h@1).\n",
            "- Market report as of 2026-01-11 00:00:29 UTC\n",
            "  Last price 61320.9\n",
            "  Volatility regime EXTREME, ATR percentile 92.5 (volatility.regime.1h@1)\n",
            "  UTC-day VWAP warming up\n",
            "  Prior-day auction warming up\n",
            "  Monitored prior-day levels:\n",
            "    VAL 61010 zone 61010-61020\n",
            "    POC 61305 zone 61300-61310, in zone\n",
            "    VAH 61460 zone 61450-61460\n",
            "  Active monitors: POC 61305\n",
            "Informational only.\n",
        ),
        "\n{}",
        message.text
    );
}

#[test]
fn no_message_breaks_the_wording_rule() {
    let messages = messages();
    let mut kinds = [0_u64; 5];
    for message in messages {
        assert!(!message.alerts.is_empty());
        for alert in &message.alerts {
            assert_eq!(alert.known_at(), message.known_at, "{alert}");
            kinds[AlertKind::ALL
                .iter()
                .position(|k| *k == alert.kind())
                .unwrap()] += 1;
        }
        let text = &message.text;
        assert_eq!(wording_hits(text), Vec::<&str>::new(), "{text}");
        assert!(text.is_ascii(), "{text}");
        assert!(text.len() <= MAX_MESSAGE_CHARS, "{text}");
        assert!(
            text.starts_with(&format!(
                "MIE alert {} (event time)\n",
                utc(message.known_at)
            )),
            "{text}"
        );
        assert!(text.ends_with(FOOTER), "{text}");
    }
    assert!(kinds.iter().all(|count| *count > 0), "{kinds:?}");
}

#[test]
fn the_worst_case_message_stays_within_the_limit() {
    let mut alerts = vec![
        first(|alert| alert.kind() == AlertKind::ZoneLeft),
        first(|alert| alert.kind() == AlertKind::ZoneLeft),
        first(|alert| alert.kind() == AlertKind::ZoneLeft),
        entered(LevelKind::Vah, true),
        entered(LevelKind::Val, true),
        entered(LevelKind::Poc, false),
    ];
    // An event that closes many bars after a hole can fire several auction
    // triggers; far more than any event can.
    for _ in 0..60 {
        alerts.push(trigger(catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key));
    }
    alerts.push(first(|alert| alert.kind() == AlertKind::RegimeChanged));
    alerts.push(first(
        |alert| matches!(alert, Alert::Report(r) if r.regime.is_ready()),
    ));
    let text = render(&alerts);
    assert!(text.len() <= MAX_MESSAGE_CHARS, "{}", text.len());
    assert!(
        text.contains("more alerts of this event are in the decision log"),
        "{text}"
    );
    assert!(wording_hits(&text).is_empty());
    // Every block fits on its own, so a short bundle omits nothing.
    let short = render(&alerts[..6]);
    assert!(!short.contains("decision log"), "{short}");
    assert_eq!(render(&[]), "");
}

#[test]
fn drive_alerts_delivers_one_message_per_event_and_counts_gateway_errors() {
    let expected = messages();
    // A repeated trade after every event with a message: the engine rejects
    // it (ids never fall), so the monitor must not see it.
    let mut events = Vec::new();
    let mut with_alerts = expected.iter().map(|m| m.event).peekable();
    let mut repeats = 0;
    for event in tape() {
        events.push(event.clone());
        if with_alerts.peek() == Some(&event.canonical_key()) {
            with_alerts.next();
            events.push(event.clone());
            repeats += 1;
        }
    }
    let mut engine = MarketStateEngine::new();
    let mut monitor = AlertMonitor::new(None, engine.state());
    let mut gateway = Recording {
        refuse_every: Some(3),
        ..Recording::default()
    };
    let mut rejected = 0;
    let report = drive_alerts(
        &mut feed(&events),
        &mut engine,
        &mut monitor,
        |_| rejected += 1,
        &mut gateway,
    )
    .unwrap();
    assert_eq!(rejected, repeats);
    assert_eq!(report.rejections, repeats);
    assert_eq!(report.events, events.len() as u64);
    // The refusals neither stopped the drive nor changed a message.
    assert_eq!(gateway.messages, expected);
    assert_eq!(report.messages, expected.len() as u64);
    assert_eq!(report.delivery_errors, gateway.refused);
    assert!(gateway.refused > 0);
    let alerts: u64 = expected.iter().map(|m| m.alerts.len() as u64).sum();
    assert_eq!(report.alerts.iter().sum::<u64>(), alerts);
    // One message per event.
    let mut keys: Vec<_> = expected.iter().map(|m| m.event).collect();
    keys.dedup();
    assert_eq!(keys.len(), expected.len());
}

#[test]
fn a_window_emits_from_its_start_with_provenance() {
    let dataset = DatasetVersion::from_hex(&"ab".repeat(32)).unwrap();
    let history = History(tape().to_vec(), dataset.clone());
    let emit_from = EventTime::from_millis(START + 12 * DAY);
    let window = ReplayWindow {
        start: EventTime::from_millis(START),
        end: EventTime::from_millis(START + 21 * DAY),
    };
    let mut gateway = Recording::default();
    let (provenance, report) =
        alert_window(&history, window, Some(emit_from), &mut gateway).unwrap();
    assert_eq!(provenance.data, DataVersion::from(&dataset));
    assert_eq!(provenance.policy, mie_domain::alert::ALERT_POLICY_V1);
    assert_eq!(
        provenance.policy_fingerprint,
        mie_domain::alert::policy_fingerprint()
    );
    assert_eq!(report.messages, gateway.messages.len() as u64);
    assert!(gateway.messages.iter().all(|m| m.known_at >= emit_from));
    // The messages from the split equal the full run's, apart from leaves
    // of visits entered before it.
    let full: Vec<&AlertMessage> = messages()
        .iter()
        .filter(|m| m.known_at >= emit_from)
        .collect();
    assert!(gateway.messages.len() <= full.len());
    for message in &gateway.messages {
        let same = full.iter().find(|m| m.event == message.event).unwrap();
        for alert in &message.alerts {
            assert!(same.alerts.contains(alert), "{alert}");
        }
    }
    // On the chain driver, the same events without a hole give the same
    // messages.
    let mut chained = Recording::default();
    let (_, chain_report) =
        alert_chain_window(&history, window, Some(emit_from), &mut chained).unwrap();
    assert_eq!(chained.messages, gateway.messages);
    assert!(chain_report.restarts.is_empty());
}

#[test]
fn the_report_service_reports_the_market_at_the_window_end() {
    let dataset = DatasetVersion::from_hex(&"cd".repeat(32)).unwrap();
    let history = || History(tape().to_vec(), dataset.clone());
    let end = START + 15 * DAY + 7 * HOUR;
    let window = ReplayWindow {
        start: EventTime::from_millis(START),
        end: EventTime::from_millis(end),
    };
    let mut engine = MarketStateEngine::new();
    for event in tape().iter().filter(|e| e.time().as_millis() < end) {
        engine.apply(event).unwrap();
    }
    let expected = MarketReport::read(engine.state());
    assert_eq!(
        ReportService::new(history()).report(window).unwrap(),
        expected
    );
    assert_eq!(
        ReportService::chain(history()).report(window).unwrap(),
        expected
    );
    assert!(expected.as_of.is_some() && expected.regime.is_ready());
}
