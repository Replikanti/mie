use super::*;
use crate::bars::{Bar, Coverage, Ohlc};
use crate::event::samples::t;
use crate::feature::Unavailability;
use crate::location::{AuctionClassifier, AuctionState};
use crate::num::Qty;
use crate::profile::VolumeProfile;

const MINUTE: i64 = 60_000;
const DAY: i64 = 86_400_000;

fn price(text: &str) -> Price {
    text.parse().unwrap()
}

#[test]
fn policy_text_and_fingerprint_are_pinned() {
    assert_eq!(
        policy_text(),
        "mie-alert-policy 1\n\
         policy alert.policy@1\n\
         levels source=profile.volume.prior_day@1 kinds=poc,vah,val\n\
         zone_entered on=entered causes=arrived,created\n\
         zone_left on=left causes=departed requires=emitted_zone_entered silent=retired,vanished\n\
         guide on=zone_entered kinds=vah,val requires=location.auction.prior_day@1 \
         tolerance_bps=6 acceptance_closes=60\n\
         trigger ids=trigger.failed_auction@1,trigger.failed_reclaim@1,\
         trigger.breakout_acceptance@1\n\
         regime feature=volatility.regime.1h@1 \
         on=label_change_between_consecutive_ready_values not_ready=forget hysteresis=none\n\
         report on=1d_close\n\
         seed regime=ready_label visits=in_zone_not_alerted\n\
         bundle per=event order=zone_left,zone_entered,trigger,regime,report\n"
    );
    // Any change of the policy text is `alert.policy@2` (ADR-059 D2).
    assert_eq!(policy_fingerprint().to_string(), "3b72bf634edcf2dc");
}

#[test]
fn tolerance_is_the_floor_of_six_bps() {
    assert_eq!(tolerance(price("61000")), price("36.6"));
    assert_eq!(tolerance(price("60000")), price("36"));
    // ⌊0.00000001 · 6 / 10 000⌋ = 0 on the grid.
    assert_eq!(tolerance(Price::from_units(1)), Price::from_units(0));
    assert_eq!(tolerance(price("-61000")), price("36.6"));
}

// --- Guides against the classifier ------------------------------------

/// A prior-day profile with value `[60000, 61000)`.
fn prior() -> FeatureValue<VolumeProfile> {
    FeatureValue::Ready(VolumeProfile {
        feature: catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key,
        start: t(0),
        end: t(DAY),
        sessions: 1,
        total_volume: Qty::from_units(100),
        low: price("60000"),
        high: price("61000"),
        poc: price("60500"),
        poc_volume: Qty::from_units(100),
        val: price("60000"),
        vah: price("61000"),
        value_area_volume: Qty::from_units(70),
        hvn: Vec::new(),
        lvn: Vec::new(),
        coverage: Coverage::default(),
    })
}

/// The engine's classifier on [`prior`], stepped close by close.
#[derive(Clone)]
struct Path {
    classifier: AuctionClassifier,
    minute: i64,
}

impl Path {
    /// A classifier whose first close, at `first`, sets the accepted region.
    fn new(first: Price) -> Self {
        let mut classifier = AuctionClassifier::new(TOLERANCE_BPS, ACCEPTANCE_CLOSES);
        classifier.step(&[Bar::empty(Timeframe::D1, t(0))], &prior(), t(DAY), |_| {});
        let mut path = Self {
            classifier,
            minute: 0,
        };
        path.close(first);
        path
    }

    fn close(&mut self, close: Price) -> AuctionStatus {
        let bar = Bar {
            ohlc: Some(Ohlc {
                open: close,
                high: close,
                low: close,
                close,
            }),
            ..Bar::empty(Timeframe::M1, t(DAY + self.minute * MINUTE))
        };
        self.minute += 1;
        let known_at = t(bar.end().as_millis() + 1_000);
        let mut status = None;
        self.classifier
            .step(&[bar], &prior(), known_at, |classified| {
                status = Some(classified.status);
            });
        status.expect("a close under a ready reference is classified")
    }

    fn status(&self) -> AuctionStatus {
        *self.classifier.status().ready().unwrap()
    }
}

/// A registry level of `kind` at `price`.
fn level(kind: LevelKind, price: Price) -> LocationLevel {
    LocationLevel {
        kind,
        source: MONITORED_SOURCE,
        side: None,
        anchor: None,
        price,
        low: price,
        high: price,
        known_at: t(DAY),
        touches: 0,
        in_zone: true,
        distance: Price::from_units(0),
        confluence: 0,
        strength: None,
    }
}

/// The bounds a test turns on, each probed at one unit below, on and above.
fn bounds(test: &CloseTest) -> Vec<Price> {
    match *test {
        CloseTest::Above(p) | CloseTest::AtOrAbove(p) | CloseTest::Below(p) => vec![p],
        CloseTest::Between { low, high } | CloseTest::Within { low, high } => vec![low, high],
    }
}

fn around(test: &CloseTest) -> Vec<Price> {
    bounds(test)
        .into_iter()
        .flat_map(|p| [-1, 0, 1].map(|d| shift(p, d)))
        .collect()
}

/// A close well inside `region`.
fn inside(region: Region) -> Price {
    match region {
        Region::Above => price("61500"),
        Region::In => price("60500"),
        Region::Below => price("59500"),
    }
}

#[test]
fn guides_reproduce_the_classifier_from_every_accepted_region() {
    let cases = [
        (LevelKind::Vah, Region::In, Region::Above),
        (LevelKind::Vah, Region::Above, Region::In),
        (LevelKind::Vah, Region::Below, Region::Above),
        (LevelKind::Val, Region::In, Region::Below),
        (LevelKind::Val, Region::Below, Region::In),
        (LevelKind::Val, Region::Above, Region::Below),
    ];
    for (edge, accepted, toward) in cases {
        let path = Path::new(inside(accepted));
        let status = path.status();
        assert_eq!(status.accepted, accepted);
        let edge_price = if edge == LevelKind::Vah {
            status.vah
        } else {
            status.val
        };
        let guide = Guide::at(&level(edge, edge_price), &status).unwrap();
        let case = format!("{edge} from {accepted}: {guide}");
        assert_eq!((guide.accepted, guide.toward), (accepted, toward), "{case}");

        // A probe toward `toward` starts exactly on `starts`.
        for close in around(&guide.starts) {
            let status = path.clone().close(close);
            assert_eq!(
                status.probe.map(|probe| probe.toward) == Some(toward),
                guide.starts.holds(close),
                "{case} close {close}"
            );
        }
        let mut probing = path.clone();
        probing.close(inside(toward));
        // A close fails it exactly on `fails`, and the failure is the
        // guide's trigger.
        for close in around(&guide.fails) {
            let status = probing.clone().close(close);
            assert_eq!(
                status.failure.is_some(),
                guide.fails.holds(close),
                "{case} close {close}"
            );
            if let Some(failure) = status.failure {
                let expected = if guide.failure == catalog::TRIGGER_FAILED_AUCTION_V1.key {
                    AuctionState::FailedBreakout
                } else {
                    AuctionState::FailedReclaim
                };
                assert_eq!(failure.kind(), expected, "{case}");
            }
        }
        // A close counts toward acceptance exactly on `counts`, unless it
        // fails or retargets the probe.
        for close in around(&guide.counts) {
            let status = probing.clone().close(close);
            if let Some(probe) = status.probe.filter(|probe| probe.toward == toward) {
                assert_eq!(
                    probe.closes == 2,
                    guide.counts.holds(close),
                    "{case} {close}"
                );
            }
        }
        // At the N-th counted close the region is accepted; out of value
        // that is the breakout + acceptance the guide names.
        let mut accepting = probing.clone();
        let mut last = accepting.status();
        for _ in 1..guide.closes {
            last = accepting.close(inside(toward));
        }
        assert_eq!(last.accepted, toward, "{case}");
        assert_eq!(
            guide.acceptance.is_some(),
            last.state == AuctionState::Acceptance,
            "{case}"
        );
        assert_eq!(guide.closes, ACCEPTANCE_CLOSES);
    }
}

#[test]
fn a_guide_prints_its_exact_prices() {
    let path = Path::new(inside(Region::In));
    let status = path.status();
    let guide = Guide::at(&level(LevelKind::Vah, status.vah), &status).unwrap();
    assert_eq!(
        guide.to_string(),
        "edge=vah accepted=in toward=above starts=>61036.60000000 counts=>=61000.00000000 \
         fails=(60036.00000000,60963.40000000) failure=trigger.failed_auction@1 \
         acceptance=trigger.breakout_acceptance@1 closes=60"
    );
    let path = Path::new(inside(Region::Above));
    let status = path.status();
    let guide = Guide::at(&level(LevelKind::Vah, status.vah), &status).unwrap();
    assert_eq!(
        guide.to_string(),
        "edge=vah accepted=above toward=in starts=(60036.00000000,60963.40000000) \
         counts=[60000.00000000,61000.00000000) fails=>61036.60000000 \
         failure=trigger.failed_reclaim@1 acceptance=value closes=60"
    );
    // No guide at a POC, or for a level of another reference.
    assert_eq!(
        Guide::at(&level(LevelKind::Poc, price("60500")), &status),
        None
    );
    assert_eq!(
        Guide::at(&level(LevelKind::Vah, price("61010")), &status),
        None
    );
}

// --- The monitor on tapes ---------------------------------------------

/// One alert with the index of the event that emitted it.
type Emitted = Vec<(usize, Alert)>;

/// One replay of the candidate golden tape (ADR-051, also the trigger
/// tape): 45 days with every auction trigger and a ready regime from about
/// day 9. Holds what the tests read.
struct GoldenRun {
    /// Every alert of a monitor over every event.
    all: Emitted,
    /// The event index of the emit split: the first event from day 20 on
    /// while a visit is open.
    split: usize,
    /// Alerts of a monitor seeded fresh that emits from the split.
    from_split: Emitted,
    /// Alerts of a monitor seeded at day 15 that emits from the split.
    seeded_late: Emitted,
    /// Alerts of a monitor seeded at the split event itself.
    seeded_at_split: Emitted,
    /// Per event: the monitored zone facts, the trigger ids, the ready
    /// regime label, whether a 1d bar closed, whether the auction was ready.
    facts: Vec<EventFacts>,
}

#[derive(Default)]
struct EventFacts {
    entered: Vec<(LocationLevel, EnterCause)>,
    left: Vec<(LocationLevel, LeaveCause)>,
    triggers: Vec<FeatureKey>,
    regime: Option<RegimeLabel>,
    day_closed: bool,
    auction_ready: bool,
}

fn golden_run() -> &'static GoldenRun {
    static RUN: std::sync::OnceLock<GoldenRun> = std::sync::OnceLock::new();
    RUN.get_or_init(|| {
        let tape = crate::candidate::tests::golden_tape();
        let late = tape
            .iter()
            .position(|event| event.time().as_millis() >= 15 * DAY)
            .unwrap();
        // Pass 1: every alert and the facts behind them.
        let mut engine = MarketStateEngine::new();
        let fresh = engine.state().clone();
        let mut monitor = AlertMonitor::new(None, &fresh);
        let mut all = Vec::new();
        let mut facts_list = Vec::new();
        for (index, event) in tape.iter().enumerate() {
            engine.apply(event).unwrap();
            monitor.observe(event, &engine, |alert| all.push((index, alert)));
            let mut facts = EventFacts::default();
            for fact in engine.location_events() {
                match fact {
                    LocationEvent::Entered { level, cause, .. } if is_monitored(level) => {
                        facts.entered.push((*level, *cause));
                    }
                    LocationEvent::Left { level, cause, .. } if is_monitored(level) => {
                        facts.left.push((*level, *cause));
                    }
                    _ => {}
                }
            }
            facts.triggers = engine.trigger_events().iter().map(|t| t.feature).collect();
            facts.regime = engine.state().regime.ready().map(|regime| regime.label);
            facts.day_closed = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::D1);
            facts.auction_ready = engine.state().location.auction.is_ready();
            facts_list.push(facts);
        }
        // The split: the first event from day 20 on while an alerted visit
        // is open, so a leave spans it.
        let mut open: Vec<(LevelKind, Price)> = Vec::new();
        let mut split = None;
        for (index, event) in tape.iter().enumerate() {
            if event.time().as_millis() >= 20 * DAY && !open.is_empty() {
                split = Some(index);
                break;
            }
            // Retirements close a visit too, without an alert.
            for (level, _) in &facts_list[index].left {
                open.retain(|visit| *visit != (level.kind, level.price));
            }
            for (level, _) in &facts_list[index].entered {
                open.push((level.kind, level.price));
            }
        }
        let split = split.expect("a visit is open after day 20");
        let split_time = tape[split].time();
        // Pass 2: monitors that emit from the split, seeded at three points.
        let mut engine = MarketStateEngine::new();
        let mut split_monitor = AlertMonitor::new(Some(split_time), &fresh);
        let mut late_monitor = None;
        let mut at_split_monitor = None;
        let mut run = GoldenRun {
            all,
            split,
            from_split: Vec::new(),
            seeded_late: Vec::new(),
            seeded_at_split: Vec::new(),
            facts: facts_list,
        };
        for (index, event) in tape.iter().enumerate() {
            if index == late {
                late_monitor = Some(AlertMonitor::new(Some(split_time), engine.state()));
            }
            if index == split {
                at_split_monitor = Some(AlertMonitor::new(Some(split_time), engine.state()));
            }
            engine.apply(event).unwrap();
            split_monitor.observe(event, &engine, |alert| run.from_split.push((index, alert)));
            if let Some(monitor) = &mut late_monitor {
                monitor.observe(event, &engine, |alert| run.seeded_late.push((index, alert)));
            }
            if let Some(monitor) = &mut at_split_monitor {
                monitor.observe(event, &engine, |alert| {
                    run.seeded_at_split.push((index, alert));
                });
            }
        }
        run
    })
}

fn of_kind(alerts: &Emitted, kind: AlertKind) -> Vec<&(usize, Alert)> {
    alerts
        .iter()
        .filter(|(_, alert)| alert.kind() == kind)
        .collect()
}

#[test]
fn zone_entered_follows_every_monitored_enter_of_both_causes() {
    let run = golden_run();
    let expected: Vec<(usize, LocationLevel, EnterCause)> = run
        .facts
        .iter()
        .enumerate()
        .flat_map(|(index, facts)| facts.entered.iter().map(move |(l, c)| (index, *l, *c)))
        .collect();
    let alerted: Vec<(usize, LocationLevel, EnterCause)> =
        of_kind(&run.all, AlertKind::ZoneEntered)
            .into_iter()
            .map(|(index, alert)| match alert {
                Alert::ZoneEntered { level, cause, .. } => (*index, *level, *cause),
                _ => unreachable!(),
            })
            .collect();
    assert_eq!(alerted, expected);
    for cause in [EnterCause::Arrived, EnterCause::Created] {
        assert!(
            expected.iter().any(|(_, _, c)| *c == cause),
            "no monitored enter {cause:?} on the tape"
        );
    }
    for kind in MONITORED_KINDS {
        assert!(
            expected.iter().any(|(_, level, _)| level.kind == kind),
            "{kind}"
        );
    }
    // Guides at VAH and VAL while the auction is ready; none at a POC.
    let mut guided = 0;
    for (index, alert) in of_kind(&run.all, AlertKind::ZoneEntered) {
        let Alert::ZoneEntered { level, guide, .. } = alert else {
            unreachable!()
        };
        let expected = level.kind != LevelKind::Poc && run.facts[*index].auction_ready;
        assert_eq!(guide.is_some(), expected, "{alert}");
        if let Some(guide) = guide {
            guided += 1;
            assert_eq!(guide.edge, level.kind);
        }
    }
    assert!(guided > 0);
}

#[test]
fn zone_left_follows_departures_of_alerted_visits_only() {
    let run = golden_run();
    // From a fresh engine every visit starts with an alerted enter, so every
    // monitored departure is alerted and every retirement is silent.
    let departed: Vec<(usize, LocationLevel)> = run
        .facts
        .iter()
        .enumerate()
        .flat_map(|(index, facts)| {
            facts
                .left
                .iter()
                .filter(|(_, cause)| *cause == LeaveCause::Departed)
                .map(move |(level, _)| (index, *level))
        })
        .collect();
    let alerted: Vec<(usize, LocationLevel)> = of_kind(&run.all, AlertKind::ZoneLeft)
        .into_iter()
        .map(|(index, alert)| match alert {
            Alert::ZoneLeft { level, .. } => (*index, *level),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(alerted, departed);
    let retired = run
        .facts
        .iter()
        .flat_map(|facts| &facts.left)
        .filter(|(_, cause)| *cause == LeaveCause::Retired)
        .count();
    assert!(retired > 0, "no monitored level retired on the tape");

    // Emitting from day 20: a visit entered before it leaves silently.
    let split = run.split;
    let silent: Vec<_> = run
        .all
        .iter()
        .filter(|(index, alert)| {
            *index >= split
                && !run
                    .from_split
                    .iter()
                    .any(|(i, other)| i == index && other == alert)
        })
        .collect();
    assert!(!silent.is_empty(), "no visit spans the split");
    for (index, alert) in &silent {
        let Alert::ZoneLeft { level, .. } = alert else {
            panic!("only a leave may differ: {alert}");
        };
        // Its enter came before the split.
        let entered = run.all.iter().rev().find(|(i, other)| {
            i < index
                && matches!(other, Alert::ZoneEntered { level: l, .. }
                    if l.kind == level.kind && l.price == level.price)
        });
        assert!(entered.is_some_and(|(i, _)| *i < split), "{alert}");
    }
    assert!(run.from_split.iter().all(|(index, _)| *index >= split));
}

#[test]
fn only_the_three_auction_triggers_are_alerted() {
    let run = golden_run();
    let expected: Vec<(usize, FeatureKey)> = run
        .facts
        .iter()
        .enumerate()
        .flat_map(|(index, facts)| {
            facts
                .triggers
                .iter()
                .filter(|key| ALERT_TRIGGERS.contains(key))
                .map(move |key| (index, *key))
        })
        .collect();
    let alerted: Vec<(usize, FeatureKey)> = of_kind(&run.all, AlertKind::TriggerFired)
        .into_iter()
        .map(|(index, alert)| match alert {
            Alert::TriggerFired(trigger) => (*index, trigger.feature),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(alerted, expected);
    // Every id fires on the CLI's synthetic tape (alert_measure.rs); here
    // at least one does.
    assert!(!expected.is_empty(), "no auction trigger fired");
    let others = run
        .facts
        .iter()
        .flat_map(|facts| &facts.triggers)
        .filter(|key| !ALERT_TRIGGERS.contains(key))
        .count();
    assert!(
        others > 0,
        "no other trigger id fired, so the filter is untested"
    );
}

#[test]
fn regime_changes_between_ready_values_only() {
    let run = golden_run();
    let mut last = None;
    let mut expected = Vec::new();
    for (index, facts) in run.facts.iter().enumerate() {
        if let Some(label) = facts.regime
            && let Some(from) = last
            && from != label
        {
            expected.push((index, from, label));
        }
        // A not-ready stretch forgets the label (D2e).
        last = facts.regime;
    }
    let alerted: Vec<(usize, RegimeLabel, RegimeLabel)> =
        of_kind(&run.all, AlertKind::RegimeChanged)
            .into_iter()
            .map(|(index, alert)| match alert {
                Alert::RegimeChanged { from, to, .. } => (*index, *from, to.label),
                _ => unreachable!(),
            })
            .collect();
    assert_eq!(alerted, expected);
    assert!(alerted.len() > 1, "{alerted:?}");
    // No change before the first ready value.
    let first_ready = run.facts.iter().position(|f| f.regime.is_some()).unwrap();
    assert!(first_ready > 0);
    assert!(alerted.iter().all(|(index, ..)| *index > first_ready));
}

#[test]
fn a_report_comes_with_every_1d_close_only() {
    let run = golden_run();
    let expected: Vec<usize> = run
        .facts
        .iter()
        .enumerate()
        .filter(|(_, facts)| facts.day_closed)
        .map(|(index, _)| index)
        .collect();
    let reports = of_kind(&run.all, AlertKind::Report);
    let alerted: Vec<usize> = reports.iter().map(|(index, _)| *index).collect();
    assert_eq!(alerted, expected);
    assert_eq!(alerted.len(), 45);
    // A report reads the levels of the day it opens: monitored ones only.
    let (_, Alert::Report(report)) = reports.last().unwrap() else {
        unreachable!()
    };
    let levels = report.levels.ready().unwrap();
    assert_eq!(levels.len(), 3, "{report}");
    assert!(levels.iter().all(is_monitored));
}

#[test]
fn alerts_of_an_event_come_in_bundling_order() {
    let run = golden_run();
    for pair in run.all.windows(2) {
        let [(a, first), (b, second)] = pair else {
            unreachable!()
        };
        if a == b {
            assert!(first.kind() <= second.kind(), "{first} / {second}");
        }
        assert_eq!(first.known_at(), first.known_at());
    }
    // Some event bundles several alerts.
    assert!(run.all.windows(2).any(|pair| pair[0].0 == pair[1].0));
}

#[test]
fn alerts_do_not_depend_on_where_observation_started() {
    let run = golden_run();
    // D2g: seeded fresh at the tape's start, at day 15, or at the split
    // event itself, a monitor emitting from the split emits the same alerts.
    assert!(!run.from_split.is_empty());
    assert_eq!(run.seeded_late, run.from_split);
    assert_eq!(run.seeded_at_split, run.from_split);
    for kind in AlertKind::ALL {
        assert!(
            run.from_split.iter().any(|(_, alert)| alert.kind() == kind),
            "no {kind} after the split, so the comparison is vacuous for it"
        );
    }
}

#[test]
fn canonical_lines_name_no_direction() {
    let run = golden_run();
    for (_, alert) in &run.all {
        let line = alert.to_string();
        let lower = line.to_lowercase();
        // The trigger event's own line carries the bias; the rest is ours.
        let own = match alert {
            Alert::TriggerFired(trigger) => lower.replace(&trigger.to_string().to_lowercase(), ""),
            _ => lower,
        };
        for word in [
            "bull", "bear", "long", "short", "signal", "reversal", "entry",
        ] {
            assert!(!own.contains(word), "{word} in {line}");
        }
    }
}

#[test]
fn a_restarted_engine_closes_visits_silently() {
    let tape = crate::location::tests::walk_tape(7, 4);
    let mut engine = MarketStateEngine::new();
    let mut monitor = AlertMonitor::new(None, engine.state());
    let mut open: Option<LocationLevel> = None;
    let mut restart = None;
    for (index, event) in tape.iter().enumerate() {
        engine.apply(event).unwrap();
        monitor.observe(event, &engine, |alert| match alert {
            Alert::ZoneEntered { level, .. } => open = Some(level),
            Alert::ZoneLeft { .. } => open = None,
            _ => {}
        });
        if event.time().as_millis() > 2 * DAY + 12 * 3_600_000 && open.is_some() {
            restart = Some(index + 1);
            break;
        }
    }
    let (restart, open) = (restart.expect("a visit is open after day 2"), open.unwrap());
    // ADR-052 D4: a fresh engine takes over.
    let mut engine = MarketStateEngine::new();
    let mut after = Vec::new();
    for event in &tape[restart..] {
        engine.apply(event).unwrap();
        monitor.observe(event, &engine, |alert| after.push(alert));
    }
    let left_unentered = after.iter().position(|alert| {
        matches!(alert, Alert::ZoneLeft { level, .. }
            if level.kind == open.kind && level.price == open.price)
    });
    let reentered = after.iter().position(|alert| {
        matches!(alert, Alert::ZoneEntered { level, .. }
            if level.kind == open.kind && level.price == open.price)
    });
    match (left_unentered, reentered) {
        (Some(left), Some(entered)) => assert!(entered < left),
        (Some(_), None) => panic!("the restarted visit of {open} was alerted as left"),
        _ => {}
    }
    // The fresh engine's own visits are alerted as usual.
    assert!(
        after
            .iter()
            .any(|alert| alert.kind() == AlertKind::ZoneEntered)
    );
}

#[test]
fn a_report_reads_values_that_are_not_ready() {
    let engine = MarketStateEngine::new();
    let report = MarketReport::read(engine.state());
    assert_eq!(
        report.to_string(),
        "as_of=- price=warming regime=warming vwap=warming auction=warming levels=warming"
    );
    assert_eq!(report.active().count(), 0);
    let mut state = engine.state().clone();
    state.location.auction = FeatureValue::Unavailable {
        reason: Unavailability::InputInvalid,
    };
    assert!(
        MarketReport::read(&state)
            .to_string()
            .contains("auction=unavailable")
    );
}

/// The candidate golden tape with its trades removed for two hours from
/// hour `hole_hour` and a trades gap announcing the hole: the regime warms
/// up again after it (ADR-033). Returns the tape and the index of the
/// first event after the hole.
fn tape_with_hole(hole_hour: i64) -> (Vec<MarketEvent>, usize) {
    use crate::event::{FeedGap, GapReason, Stream};
    let mut tape = crate::candidate::tests::golden_tape();
    let (start, end) = (hole_hour * 3_600_000, (hole_hour + 2) * 3_600_000 - 1);
    tape.retain(|event| {
        let at = event.time().as_millis();
        !(matches!(event, MarketEvent::Trade(_)) && start <= at && at <= end)
    });
    let after = tape
        .iter()
        .position(|event| event.time().as_millis() > end)
        .unwrap();
    tape.insert(
        after,
        MarketEvent::FeedGap(FeedGap {
            stream: Stream::Trades,
            start: t(start),
            end: t(end),
            reason: GapReason::MissingData,
        }),
    );
    (tape, after + 1)
}

#[test]
fn a_regime_hole_before_the_split_does_not_make_alerts_depend_on_the_seed() {
    // A two-hour trades hole at hour 500 (day 20 20:00), the split three
    // days later: the regime is ready before the hole, warming up at the
    // split, and its first ready label after the re-warm differs from the
    // last one before the hole, so a remembered label would alert a change
    // the later seed cannot see.
    let (tape, after) = tape_with_hole(500);
    let split = tape
        .iter()
        .position(|event| event.time().as_millis() >= tape[after].time().as_millis() + 3 * DAY)
        .unwrap();
    let split_time = tape[split].time();
    let mut engine = MarketStateEngine::new();
    let mut early = AlertMonitor::new(Some(split_time), engine.state());
    let mut late = None;
    let (mut from_start, mut from_split) = (Vec::new(), Vec::new());
    let mut before_hole = None;
    let mut first_after = None;
    let mut ready_at_split = true;
    for (index, event) in tape.iter().enumerate() {
        if index == after - 1 {
            before_hole = engine.state().regime.ready().map(|regime| regime.label);
        }
        if index > split && first_after.is_none() {
            first_after = engine.state().regime.ready().map(|regime| regime.label);
        }
        if index == split {
            ready_at_split = engine.state().regime.is_ready();
            late = Some(AlertMonitor::new(Some(split_time), engine.state()));
        }
        engine.apply(event).unwrap();
        early.observe(event, &engine, |alert| from_start.push((index, alert)));
        if let Some(monitor) = &mut late {
            monitor.observe(event, &engine, |alert| from_split.push((index, alert)));
        }
    }
    assert!(
        before_hole.is_some() && !ready_at_split,
        "the hole does not reset the regime"
    );
    assert!(
        first_after.is_some() && first_after != before_hole,
        "{before_hole:?}"
    );
    let changes = |alerts: &Emitted| {
        alerts
            .iter()
            .filter(|(_, alert)| alert.kind() == AlertKind::RegimeChanged)
            .count()
    };
    assert!(
        changes(&from_split) > 0,
        "the regime never changes after the re-warm"
    );
    assert_eq!(from_start, from_split);
}

#[test]
fn a_chain_restart_does_not_carry_the_regime_label() {
    // ADR-052 D4: a fresh engine takes over at hour 500 with the monitor
    // that observed the replaced one; it must emit what a monitor seeded
    // from the fresh engine emits.
    let tape = crate::candidate::tests::golden_tape();
    let restart = tape
        .iter()
        .position(|event| event.time().as_millis() >= 500 * 3_600_000)
        .unwrap();
    let restart_time = tape[restart].time();
    let mut engine = MarketStateEngine::new();
    let mut carried = AlertMonitor::new(Some(restart_time), engine.state());
    for event in &tape[..restart] {
        engine.apply(event).unwrap();
        carried.observe(event, &engine, |_| {});
    }
    assert!(engine.state().regime.is_ready());
    let mut engine = MarketStateEngine::new();
    let mut fresh = AlertMonitor::new(Some(restart_time), engine.state());
    let (mut a, mut b) = (Vec::new(), Vec::new());
    for (index, event) in tape[restart..].iter().enumerate() {
        engine.apply(event).unwrap();
        carried.observe(event, &engine, |alert| a.push((index, alert)));
        fresh.observe(event, &engine, |alert| b.push((index, alert)));
    }
    assert!(
        a.iter()
            .any(|(_, alert)| alert.kind() == AlertKind::RegimeChanged),
        "no regime change after the restart"
    );
    assert_eq!(a, b);
}
