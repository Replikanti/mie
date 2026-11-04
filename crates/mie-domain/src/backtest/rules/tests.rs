//! Tests of the rule registry v1 (ADR-060 D2–D5): the pinned table, strict
//! resolution and its error order, the extreme partition, the readers, each
//! rule's evaluation by hand-computed levels, and a smoke run through the
//! simulator.

use super::*;
use crate::backtest::Simulator;
use crate::backtest::simulator::tests::costs;
use crate::backtest::{SimulationStats, SkipCounts};
use crate::candidate::divergence::{DivergenceEvent, OiComparison};
use crate::event::MarketEvent;
use crate::event::samples::t;
use crate::feature::{FeatureSet, FeatureValue};
use crate::order::EventKind;
use crate::regime::{AtrPercentile, Regime};
use crate::research::spec::tests::golden;
use crate::state::MarketStateEngine;
use crate::trigger::TriggerFamily;
use std::collections::BTreeSet;
use std::mem::discriminant;

// ---------------------------------------------------------------------
// Specs
// ---------------------------------------------------------------------

/// The `features` line of the closure of `keys`, its version computed
/// through [`FeatureSet::new`].
fn features_line(keys: &[&str]) -> String {
    let registry = catalog::registry();
    let mut closure = BTreeSet::new();
    for text in keys {
        let key = registry.resolve(text).unwrap().key;
        closure.extend(registry.closure(key).unwrap());
    }
    let keys: Vec<FeatureKey> = closure.into_iter().collect();
    let set = FeatureSet::new(&registry, &keys).unwrap();
    format!("features {} {set}", set.version())
}

/// The `features` line of `catalog::current_set()`.
fn current_features_line() -> String {
    let set = catalog::current_set();
    format!("features {} {set}", set.version())
}

/// The spec of ADR-060 *Accept when* 1, with a placeholder data version.
fn accept_spec() -> String {
    let features = features_line(&[
        "trigger.effort_result.5m@1",
        "trade.last_price@1",
        "volatility.atr.1h@1",
        "volatility.regime.1h@1",
    ]);
    format!(
        "mie-experiment 1\n\
         hypothesis effort_result.with\n\
         sample 1759276800000 1790812800000\n\
         data {data}\n\
         {features}\n\
         state-filter none\n\
         regime-filter none\n\
         location location.any@1\n\
         trigger trigger.fires@1 feature=feature:trigger.effort_result.5m@1\n\
         entry entry.next_trade@1 side=text:with\n\
         invalidation invalidation.atr@1 atr=feature:volatility.atr.1h@1 multiple=rate:0.31333000\n\
         target target.r_multiple@1 max_hold_ms=int:3600000 r=rate:1.00000000\n\
         fees maker=0.00020000 taker=0.00050000\n\
         slippage slippage.fixed@1 rate=rate:0.00000000\n\
         funding funding.recorded@1\n\
         latency 0\n",
        data = "0f".repeat(32)
    )
}

/// The sample length of [`accept_spec`] in ms.
const SAMPLE_MS: i64 = 1_790_812_800_000 - 1_759_276_800_000;

/// `text` with the line of each `(key, line)` replaced by `line`.
fn with_lines(text: &str, lines: &[(&str, &str)]) -> String {
    text.lines()
        .map(|current| {
            let key = current.split(' ').next().unwrap_or_default();
            lines
                .iter()
                .find(|(k, _)| *k == key)
                .map_or(current, |(_, line)| *line)
                .to_owned()
                + "\n"
        })
        .collect()
}

fn parse(text: &str) -> ExperimentSpec {
    ExperimentSpec::parse(text, &catalog::registry()).expect("the spec parses")
}

fn errors_of(text: &str) -> Vec<ResolutionError> {
    resolve(&parse(text)).expect_err("the spec does not resolve")
}

fn rate(text: &str) -> Rate {
    text.parse().unwrap()
}

fn key(text: &str) -> FeatureKey {
    catalog::registry().resolve(text).unwrap().key
}

// ---------------------------------------------------------------------
// The table
// ---------------------------------------------------------------------

/// A registry row: slot, `id@N` and the parameters.
type Row = (SpecField, String, Vec<(&'static str, ParamKind)>);

#[test]
fn registry_v1_is_d2s_table() {
    use ParamKind::*;
    let rows: Vec<Row> = REGISTRY_V1
        .iter()
        .map(|schema| {
            (
                schema.slot,
                format!("{}@{}", schema.id, schema.version),
                schema.params.iter().map(|p| (p.name, p.kind)).collect(),
            )
        })
        .collect();
    let expected: Vec<Row> = vec![
        (SpecField::Location, "location.any@1".into(), vec![]),
        (
            SpecField::Trigger,
            "trigger.fires@1".into(),
            vec![("feature", TriggerFeature)],
        ),
        (
            SpecField::Entry,
            "entry.next_trade@1".into(),
            vec![("side", Side)],
        ),
        (
            SpecField::Invalidation,
            "invalidation.atr@1".into(),
            vec![("atr", AtrFeature), ("multiple", PositiveRate)],
        ),
        (
            SpecField::Invalidation,
            "invalidation.beyond_extreme@1".into(),
            vec![("atr", AtrFeature), ("buffer", NonNegativeRate)],
        ),
        (
            SpecField::Target,
            "target.r_multiple@1".into(),
            vec![("max_hold_ms", Hold), ("r", PositiveRate)],
        ),
        (
            SpecField::Slippage,
            "slippage.fixed@1".into(),
            vec![("rate", Fraction)],
        ),
        (SpecField::Funding, "funding.recorded@1".into(), vec![]),
        (SpecField::Funding, "funding.none@1".into(), vec![]),
    ];
    assert_eq!(rows, expected);
    for schema in REGISTRY_V1 {
        assert!(
            schema.params.windows(2).all(|w| w[0].name < w[1].name),
            "{schema}"
        );
    }
    let types: Vec<_> = [
        TriggerFeature,
        AtrFeature,
        Side,
        PositiveRate,
        NonNegativeRate,
        Fraction,
        Hold,
    ]
    .map(ParamKind::type_name)
    .into();
    assert_eq!(
        types,
        ["feature", "feature", "text", "rate", "rate", "rate", "int"]
    );
}

// ---------------------------------------------------------------------
// Resolution
// ---------------------------------------------------------------------

#[test]
fn the_acceptance_spec_resolves() {
    let plan = resolve(&parse(&accept_spec())).unwrap();
    let expected = Plan {
        trigger: key("trigger.effort_result.5m@1"),
        side: SideRule::With,
        invalidation: Invalidation::Atr {
            atr: key("volatility.atr.1h@1"),
            multiple: rate("0.31333"),
        },
        target: rate("1"),
        costs: costs(0, 3_600_000, "0", "0.0005", FundingRule::Recorded),
        regime_features: vec![key("volatility.regime.1h@1")],
        regime_filters: Vec::new(),
    };
    assert_eq!(plan, expected);
    assert_eq!(plan.trigger(), key("trigger.effort_result.5m@1"));
    assert_eq!(plan.costs(), &expected.costs);
    assert_eq!(plan.regime_features(), [key("volatility.regime.1h@1")]);
}

#[test]
fn slippage_and_funding_resolve_into_the_costs() {
    for (slippage, funding, rule) in [
        ("0.00010000", "funding funding.none@1", FundingRule::None),
        (
            "0.99999999",
            "funding funding.recorded@1",
            FundingRule::Recorded,
        ),
    ] {
        let text = with_lines(
            &accept_spec(),
            &[
                (
                    "slippage",
                    &format!("slippage slippage.fixed@1 rate=rate:{slippage}"),
                ),
                ("funding", funding),
                ("latency", "latency 250"),
            ],
        );
        let plan = resolve(&parse(&text)).unwrap();
        assert_eq!(
            plan.costs(),
            &costs(250, 3_600_000, slippage, "0.0005", rule),
            "{funding}"
        );
    }
    // The bounds of the ranges themselves resolve.
    let text = with_lines(
        &accept_spec(),
        &[
            (
                "target",
                &format!(
                    "target target.r_multiple@1 max_hold_ms=int:{SAMPLE_MS} r=rate:0.00000001"
                ),
            ),
            (
                "invalidation",
                "invalidation invalidation.atr@1 atr=feature:volatility.atr.1h@1 multiple=rate:0.00000001",
            ),
            ("entry", "entry entry.next_trade@1 side=text:against"),
        ],
    );
    let plan = resolve(&parse(&text)).unwrap();
    assert_eq!(plan.costs().max_hold_ms(), SAMPLE_MS);
    assert_eq!(plan.side, SideRule::Against);
}

/// The spec of [`accept_spec`] on the current set with an SFP trigger, so
/// `beyond_extreme` applies.
fn extreme_spec() -> String {
    with_lines(
        &accept_spec(),
        &[
            ("features", &current_features_line()),
            (
                "trigger",
                "trigger trigger.fires@1 feature=feature:trigger.sfp.1h@1",
            ),
        ],
    )
}

#[test]
fn each_d2_error_is_its_own() {
    let accept = accept_spec();
    let extreme = extreme_spec();
    let atr = "atr=feature:volatility.atr.1h@1";
    let cases: Vec<(&str, String, ResolutionError, String)> =
        vec![
        (
            "unknown rule",
            with_lines(&accept, &[("location", "location location.at_level@1")]),
            ResolutionError::UnknownRule {
                field: SpecField::Location,
                rule: "location.at_level@1".into(),
            },
            "location: location.at_level@1: unknown rule (location rules: location.any@1)".into(),
        ),
        (
            "unknown version",
            with_lines(&accept, &[("entry", "entry entry.next_trade@2 side=text:with")]),
            ResolutionError::UnknownVersion {
                field: SpecField::Entry,
                rule: "entry.next_trade@2".into(),
            },
            "entry: entry.next_trade@2: unknown version (entry rules: entry.next_trade@1)".into(),
        ),
        (
            "unknown parameter",
            with_lines(&accept, &[("location", "location location.any@1 radius=int:1")]),
            ResolutionError::UnknownParam {
                field: SpecField::Location,
                rule: "location.any@1".into(),
                param: "radius".into(),
            },
            "location: location.any@1: unknown parameter \"radius\" (the rule is \
             `location.any@1`)"
                .into(),
        ),
        (
            "missing parameter",
            with_lines(&accept, &[("entry", "entry entry.next_trade@1")]),
            ResolutionError::MissingParam {
                field: SpecField::Entry,
                rule: "entry.next_trade@1".into(),
                param: param("side", ParamKind::Side),
            },
            "entry: entry.next_trade@1: missing parameter side: add `side=text:<with|against>`"
                .into(),
        ),
        (
            "wrong type",
            with_lines(&accept, &[("entry", "entry entry.next_trade@1 side=int:1")]),
            ResolutionError::WrongType {
                field: SpecField::Entry,
                rule: "entry.next_trade@1".into(),
                param: param("side", ParamKind::Side),
                expected: "text",
                found: "int",
            },
            "entry: entry.next_trade@1: side has type int, expected text \
             (`side=text:<with|against>`)"
                .into(),
        ),
        (
            "out of range",
            with_lines(
                &accept,
                &[(
                    "invalidation",
                    &format!("invalidation invalidation.atr@1 {atr} multiple=rate:0"),
                )],
            ),
            ResolutionError::OutOfRange {
                field: SpecField::Invalidation,
                rule: "invalidation.atr@1".into(),
                param: param("multiple", ParamKind::PositiveRate),
                value: SpecValue::Rate(rate("0")),
            },
            "invalidation: invalidation.atr@1: multiple=rate:0.00000000 is out of range \
             (`multiple=rate:<decimal above 0>`)"
                .into(),
        ),
        (
            "wrong family",
            with_lines(
                &accept,
                &[(
                    "invalidation",
                    "invalidation invalidation.atr@1 atr=feature:trade.last_price@1 \
                     multiple=rate:0.31333000",
                )],
            ),
            ResolutionError::WrongFamily {
                field: SpecField::Invalidation,
                rule: "invalidation.atr@1".into(),
                param: param("atr", ParamKind::AtrFeature),
                feature: key("trade.last_price@1"),
            },
            "invalidation: invalidation.atr@1: atr=feature:trade.last_price@1 is not a \
             volatility.atr.* feature with a reader \
             (`atr=feature:<volatility.atr.* id@N of the features line>`)"
                .into(),
        ),
        (
            "no extreme",
            with_lines(
                &accept,
                &[(
                    "invalidation",
                    &format!("invalidation invalidation.beyond_extreme@1 {atr} buffer=rate:0"),
                )],
            ),
            ResolutionError::NoExtreme {
                field: SpecField::Invalidation,
                rule: "invalidation.beyond_extreme@1".into(),
                trigger: key("trigger.effort_result.5m@1"),
            },
            "invalidation: invalidation.beyond_extreme@1: trigger.effort_result.5m@1 has no \
             extreme; use invalidation.atr@1, which takes every trigger"
                .into(),
        ),
        (
            "latency not below the hold",
            with_lines(&accept, &[("latency", "latency 3600000")]),
            ResolutionError::LatencyNotBelowHold {
                field: SpecField::Target,
                rule: "target.r_multiple@1".into(),
                latency_ms: 3_600_000,
                max_hold_ms: 3_600_000,
            },
            "target: target.r_multiple@1: latency 3600000 is not below max_hold_ms 3600000, so \
             no entry can fill; raise max_hold_ms or lower latency"
                .into(),
        ),
        (
            "state-filter rule",
            with_lines(&accept, &[("state-filter", "state-filter session.asia@1")]),
            ResolutionError::StateFilterRule {
                field: SpecField::StateFilter,
                rule: "session.asia@1".into(),
            },
            "state-filter: session.asia@1: registry v1 has no state-filter rule; use \
             `state-filter none`"
                .into(),
        ),
    ];
    let mut kinds = Vec::new();
    for (name, text, expected, display) in cases {
        assert_eq!(errors_of(&text), vec![expected.clone()], "{name}");
        assert_eq!(expected.to_string(), display, "{name}");
        kinds.push(discriminant(&expected));
    }
    for (i, kind) in kinds.iter().enumerate() {
        assert!(!kinds[..i].contains(kind), "case {i} repeats a variant");
    }
    assert_eq!(kinds.len(), 10);

    // Every range of D2, each its own single-line mutation.
    let out_of_range = |base: &str, key: &str, line: &str, name: &'static str, kind, value| {
        let error = ResolutionError::OutOfRange {
            field: SpecField::ALL
                .into_iter()
                .find(|field| field.key() == key)
                .unwrap(),
            rule: line.split(' ').nth(1).unwrap().to_owned(),
            param: param(name, kind),
            value,
        };
        assert_eq!(
            errors_of(&with_lines(base, &[(key, line)])),
            vec![error],
            "{line}"
        );
    };
    out_of_range(
        &extreme,
        "invalidation",
        &format!("invalidation invalidation.beyond_extreme@1 {atr} buffer=rate:-0.00000001"),
        "buffer",
        ParamKind::NonNegativeRate,
        SpecValue::Rate(rate("-0.00000001")),
    );
    for (hold, r) in [
        ("0", "1"),
        (&(SAMPLE_MS + 1).to_string() as &str, "1"),
        ("3600000", "0"),
    ] {
        let line = format!("target target.r_multiple@1 max_hold_ms=int:{hold} r=rate:{r}");
        let (name, kind, value) = if r == "0" {
            ("r", ParamKind::PositiveRate, SpecValue::Rate(rate("0")))
        } else {
            (
                "max_hold_ms",
                ParamKind::Hold,
                SpecValue::Int(hold.parse().unwrap()),
            )
        };
        out_of_range(&accept, "target", &line, name, kind, value);
    }
    out_of_range(
        &accept,
        "slippage",
        "slippage slippage.fixed@1 rate=rate:1",
        "rate",
        ParamKind::Fraction,
        SpecValue::Rate(rate("1")),
    );
    out_of_range(
        &accept,
        "entry",
        "entry entry.next_trade@1 side=text:long",
        "side",
        ParamKind::Side,
        SpecValue::Text("long".into()),
    );
    // The trigger's feature parameter of another family.
    assert_eq!(
        errors_of(&with_lines(
            &accept,
            &[(
                "trigger",
                "trigger trigger.fires@1 feature=feature:volatility.atr.1h@1"
            )]
        )),
        vec![ResolutionError::WrongFamily {
            field: SpecField::Trigger,
            rule: "trigger.fires@1".into(),
            param: param("feature", ParamKind::TriggerFeature),
            feature: key("volatility.atr.1h@1"),
        }]
    );
}

#[test]
fn the_golden_spec_lists_its_errors_in_line_order() {
    let spec = parse(&golden());
    assert_eq!(spec.id().to_string(), "3f902847001e1998");
    let errors = resolve(&spec).unwrap_err();
    let texts: Vec<String> = errors.iter().map(ToString::to_string).collect();
    assert_eq!(
        texts,
        [
            "location: location.at_level@1: unknown rule (location rules: location.any@1)",
            "trigger: trigger.failed_auction@1: unknown rule (trigger rules: trigger.fires@1)",
            "entry: entry.next_trade@1: missing parameter side: add `side=text:<with|against>`",
            "invalidation: invalidation.beyond_extreme@1: missing parameter atr: add \
             `atr=feature:<volatility.atr.* id@N of the features line>`",
            "invalidation: invalidation.beyond_extreme@1: missing parameter buffer: add \
             `buffer=rate:<decimal at or above 0>`",
            "target: target.level@1: unknown rule (target rules: target.r_multiple@1)",
        ]
    );
    let kinds: Vec<_> = errors.iter().map(discriminant).collect();
    let unknown = discriminant(&errors[0]);
    let missing = discriminant(&errors[2]);
    assert_eq!(
        kinds,
        [unknown, unknown, missing, missing, missing, unknown]
    );
}

#[test]
fn errors_follow_the_canonical_line_order_whatever_the_input_order() {
    let atr = "atr=feature:volatility.atr.1h@1";
    let text = with_lines(
        &accept_spec(),
        &[
            ("funding", "funding funding.daily@1"),
            ("slippage", "slippage slippage.fixed@1 rate=rate:1"),
            (
                "target",
                "target target.r_multiple@1 max_hold_ms=int:0 r=rate:0 zeta=int:1",
            ),
            (
                "invalidation",
                &format!("invalidation invalidation.beyond_extreme@1 {atr} buffer=rate:0"),
            ),
            ("state-filter", "state-filter zeta.rule@1"),
        ],
    );
    // The same lines in reverse input order, plus a second state filter.
    let mut lines: Vec<&str> = text.lines().collect();
    let header = lines.remove(0);
    lines.reverse();
    let reversed = format!(
        "{header}\nstate-filter alpha.rule@2\n{}\n",
        lines.join("\n")
    );
    let errors = errors_of(&reversed);
    let summary: Vec<(SpecField, &str, String)> = errors
        .iter()
        .map(|error| {
            let detail = match error {
                ResolutionError::UnknownParam { param, .. } => param.clone(),
                ResolutionError::OutOfRange { param, .. } => param.name.to_owned(),
                _ => String::new(),
            };
            (error.field(), error.rule(), detail)
        })
        .collect();
    assert_eq!(
        summary,
        [
            (SpecField::StateFilter, "alpha.rule@2", String::new()),
            (SpecField::StateFilter, "zeta.rule@1", String::new()),
            (
                SpecField::Invalidation,
                "invalidation.beyond_extreme@1",
                String::new()
            ),
            (
                SpecField::Target,
                "target.r_multiple@1",
                "max_hold_ms".into()
            ),
            (SpecField::Target, "target.r_multiple@1", "r".into()),
            (SpecField::Target, "target.r_multiple@1", "zeta".into()),
            (SpecField::Slippage, "slippage.fixed@1", "rate".into()),
            (SpecField::Funding, "funding.daily@1", String::new()),
        ]
    );
    assert!(matches!(errors[2], ResolutionError::NoExtreme { .. }));
    // An out-of-range hold skips the latency check; an unresolved trigger
    // skips the extreme check.
    let text = with_lines(
        &accept_spec(),
        &[
            (
                "trigger",
                "trigger trigger.fires@2 feature=feature:trigger.effort_result.5m@1",
            ),
            (
                "invalidation",
                &format!("invalidation invalidation.beyond_extreme@1 {atr} buffer=rate:0"),
            ),
            (
                "target",
                "target target.r_multiple@1 max_hold_ms=int:0 r=rate:1",
            ),
            ("latency", "latency 5"),
        ],
    );
    let kinds: Vec<SpecField> = errors_of(&text)
        .iter()
        .map(ResolutionError::field)
        .collect();
    assert_eq!(kinds, [SpecField::Trigger, SpecField::Target]);
}

/// A regime feature the backtest has no reader for, registered beside the
/// catalog by a caller-built registry.
static REGIME_4H: crate::feature::FeatureDefinition = crate::feature::FeatureDefinition {
    key: FeatureKey::new("volatility.regime.4h", 1),
    params: &[],
    inputs: &[],
    warm_up: crate::feature::WarmUp::None,
};

/// A spec parsed under a registry other than the catalog's may list a
/// regime feature without a reader: `resolve` reports it on the `features`
/// line, ahead of the other errors, and never panics (review of PR #178).
#[test]
fn an_unreadable_regime_feature_is_an_error_not_a_panic() {
    let mut definitions = catalog::DEFINITIONS.to_vec();
    definitions.push(&REGIME_4H);
    let registry = crate::feature::FeatureRegistry::new(&definitions).unwrap();
    let mut closure = BTreeSet::new();
    for text in [
        "trigger.effort_result.5m@1",
        "trade.last_price@1",
        "volatility.atr.1h@1",
        "volatility.regime.1h@1",
        "volatility.regime.4h@1",
    ] {
        closure.extend(
            registry
                .closure(registry.resolve(text).unwrap().key)
                .unwrap(),
        );
    }
    let keys: Vec<FeatureKey> = closure.into_iter().collect();
    let set = FeatureSet::new(&registry, &keys).unwrap();
    let text = with_lines(
        &accept_spec(),
        &[
            ("features", &format!("features {} {set}", set.version())),
            ("regime-filter", "regime-filter volatility.regime.4h@1 HIGH"),
            ("location", "location location.at_level@1"),
        ],
    );
    let spec = ExperimentSpec::parse(&text, &registry).expect("the spec parses");
    let errors = resolve(&spec).unwrap_err();
    assert_eq!(
        errors,
        [
            ResolutionError::UnreadableRegime {
                field: SpecField::Features,
                rule: "volatility.regime.4h@1".into(),
            },
            ResolutionError::UnknownRule {
                field: SpecField::Location,
                rule: "location.at_level@1".into(),
            },
        ]
    );
    assert_eq!(
        errors[0].to_string(),
        "features: volatility.regime.4h@1: the backtest cannot read this regime feature \
         (readable: volatility.regime.1h@1)"
    );
    // Alone, it is the only error.
    let text = with_lines(&text, &[("location", "location location.any@1")]);
    let spec = ExperimentSpec::parse(&text, &registry).unwrap();
    assert_eq!(resolve(&spec).unwrap_err(), [errors[0].clone()]);
}

// ---------------------------------------------------------------------
// The extreme partition and the readers
// ---------------------------------------------------------------------

/// The trigger events of a replay of `tape`.
fn fired(tape: &[MarketEvent]) -> Vec<TriggerEvent> {
    let mut engine = MarketStateEngine::new();
    let mut triggers = Vec::new();
    for event in tape {
        engine.apply(event).unwrap();
        triggers.extend(engine.trigger_events().iter().cloned());
    }
    triggers
}

#[test]
fn beyond_extreme_takes_exactly_the_d2_ids() {
    let registry = catalog::registry();
    let triggers: Vec<FeatureKey> = registry
        .definitions()
        .map(|definition| definition.key)
        .filter(|key| is_trigger(*key))
        .collect();
    assert_eq!(triggers.len(), 23);
    let base = with_lines(
        &accept_spec(),
        &[
            ("features", &current_features_line()),
            (
                "invalidation",
                "invalidation invalidation.beyond_extreme@1 atr=feature:volatility.atr.1h@1 \
                 buffer=rate:0",
            ),
        ],
    );
    let mut refused = Vec::new();
    for &trigger in &triggers {
        let line = format!("trigger trigger.fires@1 feature=feature:{trigger}");
        let result = resolve(&parse(&with_lines(&base, &[("trigger", &line)])));
        match result {
            Ok(plan) => {
                assert!(has_extreme(trigger), "{trigger}");
                assert_eq!(plan.trigger(), trigger);
            }
            Err(errors) => {
                assert!(!has_extreme(trigger), "{trigger}");
                assert_eq!(
                    errors,
                    [ResolutionError::NoExtreme {
                        field: SpecField::Invalidation,
                        rule: "invalidation.beyond_extreme@1".into(),
                        trigger,
                    }]
                );
                refused.push(trigger.to_string());
            }
        }
    }
    refused.sort();
    assert_eq!(
        refused,
        [
            "trigger.absorption.15m@1",
            "trigger.absorption.1h@1",
            "trigger.absorption.5m@1",
            "trigger.breakout_acceptance@1",
            "trigger.effort_result.15m@1",
            "trigger.effort_result.1h@1",
            "trigger.effort_result.5m@1",
            "trigger.failed_auction@1",
            "trigger.failed_reclaim@1",
            "trigger.oi_expansion_rejection.prior_day@1",
        ]
    );
    assert_eq!(triggers.len() - refused.len(), 13);
}

#[test]
fn the_extreme_partition_matches_the_evidence_prices() {
    let mut seen = BTreeSet::new();
    for tape in [
        crate::candidate::tests::golden_tape(),
        crate::candidate::tests::book_tape(),
        crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7),
    ] {
        for trigger in fired(&tape) {
            assert_eq!(
                has_extreme(trigger.feature),
                evidence_prices(&trigger).extreme.is_some(),
                "{trigger}"
            );
            seen.insert(trigger.feature);
        }
    }
    // Both sides of the partition are exercised.
    assert!(seen.iter().any(|&key| has_extreme(key)));
    assert!(seen.iter().any(|&key| !has_extreme(key)));
}

#[test]
fn the_readers_cover_the_registered_atr_and_regime_features() {
    let registry = catalog::registry();
    let registered = |prefix: &str| -> Vec<FeatureKey> {
        registry
            .definitions()
            .map(|definition| definition.key)
            .filter(|key| key.id.as_str().starts_with(prefix))
            .collect()
    };
    let atr: Vec<FeatureKey> = ATR_READERS.iter().map(|(key, _)| *key).collect();
    let regime: Vec<FeatureKey> = REGIME_READERS.iter().map(|(key, _)| *key).collect();
    assert_eq!(atr, registered("volatility.atr."));
    assert_eq!(regime, registered("volatility.regime."));
    // Each reader reads its own field.
    let state = state_with(Some(100), Some(7), Some(RegimeLabel::High));
    assert_eq!(atr_1h(&state), Some(Price::from_units(7)));
    assert_eq!(regime_1h(&state), Some(RegimeLabel::High));
    let state = state_with(Some(100), None, None);
    assert_eq!((atr_1h(&state), regime_1h(&state)), (None, None));
}

// ---------------------------------------------------------------------
// Evaluation
// ---------------------------------------------------------------------

fn warming<T>() -> FeatureValue<T> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

/// A fresh engine's state with the reference, the ATR (both in price
/// units) and the 1h regime label set; `None` leaves a value warming.
fn state_with(
    reference: Option<i64>,
    atr: Option<i64>,
    regime: Option<RegimeLabel>,
) -> MarketState {
    let mut state = MarketStateEngine::new().state().clone();
    state.last_trade_price = reference.map_or_else(warming, |units| {
        FeatureValue::Ready(Price::from_units(units))
    });
    state.atr = atr.map_or_else(warming, |units| {
        FeatureValue::Ready(Price::from_units(units))
    });
    state.regime = regime.map_or_else(warming, |label| {
        FeatureValue::Ready(Regime {
            atr_percentile: AtrPercentile::new(50.0).unwrap(),
            label,
            feature: catalog::VOLATILITY_REGIME_1H_V1.key,
        })
    });
    state
}

/// A plan on `trigger.effort_result.5m@1` with the 1h ATR, no regime
/// feature and no filter.
fn plan(side: SideRule, invalidation: Invalidation, target: &str) -> Plan {
    Plan {
        trigger: key("trigger.effort_result.5m@1"),
        side,
        invalidation,
        target: rate(target),
        costs: costs(0, 3_600_000, "0", "0", FundingRule::Recorded),
        regime_features: Vec::new(),
        regime_filters: Vec::new(),
    }
}

fn atr_rule(multiple: &str) -> Invalidation {
    Invalidation::Atr {
        atr: catalog::VOLATILITY_ATR_1H_V1.key,
        multiple: rate(multiple),
    }
}

fn extreme_rule(buffer: &str) -> Invalidation {
    Invalidation::BeyondExtreme {
        atr: catalog::VOLATILITY_ATR_1H_V1.key,
        buffer: rate(buffer),
    }
}

fn decision() -> CanonicalKey {
    CanonicalKey {
        time: t(1_000),
        kind: EventKind::Trade,
        seq: 1,
    }
}

/// The admission of the one emission `trigger`, retagged with the plan's
/// trigger id.
fn admit(plan: &Plan, state: &MarketState, trigger: &TriggerEvent) -> Admission {
    let mut trigger = trigger.clone();
    trigger.feature = plan.trigger;
    let intents = plan.intents(decision(), state, &[trigger]);
    assert_eq!(intents.len(), 1);
    intents[0].admission
}

/// `Admission::Levels` with these units.
fn levels(side: PositionSide, reference: i64, stop: i64, target: i64) -> Admission {
    let price = Price::from_units;
    Admission::Levels(Levels::new(side, price(reference), price(stop), price(target)).unwrap())
}

/// One emitted trigger of each evidence kind, from the tapes of the
/// trigger tests.
struct Evidence {
    auction: TriggerEvent,
    sfp: TriggerEvent,
    cvd: TriggerEvent,
    effort: TriggerEvent,
    exhaustion: TriggerEvent,
    absorption: TriggerEvent,
    oi_sfp: TriggerEvent,
}

fn candidate_of(trigger: &TriggerEvent) -> Option<&CandidateEvent> {
    match &trigger.evidence {
        TriggerEvidence::Candidate(fact) => Some(fact.as_ref()),
        _ => None,
    }
}

fn evidence() -> Evidence {
    let golden = fired(&crate::candidate::tests::golden_tape());
    let book = fired(&crate::candidate::tests::book_tape());
    let walk = fired(&crate::location::tests::walk_tape(0x6d69_6500_0000_0044, 7));
    let find = |triggers: &[TriggerEvent], pick: &dyn Fn(&TriggerEvent) -> bool| {
        triggers
            .iter()
            .find(|trigger| pick(trigger))
            .cloned()
            .expect("the tape has the evidence")
    };
    Evidence {
        auction: find(&walk, &|trigger| {
            trigger.family == TriggerFamily::FailedAuction
                && matches!(trigger.evidence, TriggerEvidence::Auction(_))
        }),
        sfp: find(&golden, &|trigger| {
            matches!(trigger.evidence, TriggerEvidence::Sfp(_))
        }),
        cvd: find(&golden, &|trigger| {
            matches!(
                candidate_of(trigger),
                Some(CandidateEvent::CvdDivergence(_))
            )
        }),
        effort: find(&golden, &|trigger| {
            matches!(
                candidate_of(trigger),
                Some(CandidateEvent::EffortVsResult(_))
            )
        }),
        exhaustion: find(&golden, &|trigger| {
            matches!(candidate_of(trigger), Some(CandidateEvent::Exhaustion(_)))
        }),
        absorption: find(&book, &|trigger| {
            matches!(candidate_of(trigger), Some(CandidateEvent::Absorption(_)))
        }),
        oi_sfp: find(&golden, &|trigger| {
            matches!(&trigger.evidence, TriggerEvidence::OiExpansion(expansion)
                if matches!(expansion.rejection, OiRejection::Sfp(_)))
        }),
    }
}

/// `trigger` with its evidence side set to up or down (ADR-060 D3), or to
/// `side` for an auction status.
fn sided(trigger: &TriggerEvent, up: bool) -> TriggerEvent {
    let level = if up { Side::High } else { Side::Low };
    let aggressor = if up { Aggressor::Buy } else { Aggressor::Sell };
    let region = Some(if up { Region::Above } else { Region::Below });
    let mut trigger = trigger.clone();
    match &mut trigger.evidence {
        TriggerEvidence::Auction(status) => status.side = region,
        TriggerEvidence::Sfp(sweep) => sweep.level.side = level,
        TriggerEvidence::Candidate(fact) => match fact.as_mut() {
            CandidateEvent::CvdDivergence(event) => event.comparison.pair.side = level,
            CandidateEvent::OiDivergence(event) => event.comparison.pair.side = level,
            CandidateEvent::EffortVsResult(event) => event.value.aggressor = aggressor,
            CandidateEvent::Exhaustion(event) => event.value.expansion.aggressor = aggressor,
            CandidateEvent::Absorption(event) => event.value.effort.aggressor = aggressor,
        },
        TriggerEvidence::OiExpansion(expansion) => match &mut expansion.rejection {
            OiRejection::Sfp(sweep) => sweep.level.side = level,
            OiRejection::FailedAuction(status) => status.side = region,
        },
    }
    trigger
}

#[test]
fn trigger_fires_decides_at_each_emission_of_its_id_in_order() {
    let evidence = evidence();
    let plan = plan(SideRule::With, atr_rule("1"), "1");
    let state = state_with(Some(1_000), Some(10), None);
    let mut up = sided(&evidence.effort, true);
    up.feature = plan.trigger;
    let mut down = sided(&evidence.effort, false);
    down.feature = plan.trigger;
    let mut other = up.clone();
    other.feature = key("trigger.effort_result.15m@1");
    let intents = plan.intents(decision(), &state, &[other.clone(), up, other, down]);
    let sides: Vec<_> = intents
        .iter()
        .map(|intent| match intent.admission {
            Admission::Levels(levels) => levels.side(),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(sides, [PositionSide::Long, PositionSide::Short]);
    for intent in &intents {
        assert_eq!(intent.decision, decision());
        assert_eq!(intent.reference, Some(Price::from_units(1_000)));
        assert!(intent.regime.is_empty());
    }
    assert!(plan.intents(decision(), &state, &[]).is_empty());
}

#[test]
fn d3_maps_each_evidence_side() {
    let evidence = evidence();
    let Some(CandidateEvent::CvdDivergence(cvd)) = candidate_of(&evidence.cvd).cloned() else {
        unreachable!()
    };
    let mut oi_divergence = evidence.cvd.clone();
    oi_divergence.evidence =
        TriggerEvidence::Candidate(Box::new(CandidateEvent::OiDivergence(DivergenceEvent {
            feature: cvd.feature,
            timeframe: cvd.timeframe,
            known_at: cvd.known_at,
            swings: cvd.swings,
            cvd: cvd.cvd,
            comparison: OiComparison {
                pair: cvd.comparison.pair,
                previous_oi: None,
                current_oi: None,
                oi_change: None,
                divergent: false,
            },
            context: cvd.context.clone(),
        })));
    let TriggerEvidence::Auction(status) = evidence.auction.evidence.clone() else {
        unreachable!()
    };
    let mut oi_auction = evidence.oi_sfp.clone();
    let TriggerEvidence::OiExpansion(expansion) = &mut oi_auction.evidence else {
        unreachable!()
    };
    expansion.rejection = OiRejection::FailedAuction(status);
    let rows = [
        ("auction", &evidence.auction),
        ("sfp", &evidence.sfp),
        ("cvd_divergence", &evidence.cvd),
        ("oi_divergence", &oi_divergence),
        ("effort_result", &evidence.effort),
        ("exhaustion", &evidence.exhaustion),
        ("absorption", &evidence.absorption),
        ("oi_expansion via sfp", &evidence.oi_sfp),
        ("oi_expansion via failed auction", &oi_auction),
    ];
    let state = state_with(Some(1_000), Some(10), None);
    for (name, trigger) in rows {
        for (rule, up, down) in [
            (SideRule::With, PositionSide::Long, PositionSide::Short),
            (SideRule::Against, PositionSide::Short, PositionSide::Long),
        ] {
            let plan = plan(rule, atr_rule("1"), "1");
            for (is_up, side) in [(true, up), (false, down)] {
                let sigma = side.sign();
                assert_eq!(
                    admit(&plan, &state, &sided(trigger, is_up)),
                    levels(side, 1_000, 1_000 - sigma * 10, 1_000 + sigma * 10),
                    "{name} {rule:?} up={is_up}"
                );
            }
        }
    }
    // An auction inside value, or without a side, gives no side.
    for (name, trigger) in [
        ("auction", &evidence.auction),
        ("oi via auction", &oi_auction),
    ] {
        for region in [Some(Region::In), None] {
            let mut trigger = trigger.clone();
            match &mut trigger.evidence {
                TriggerEvidence::Auction(status) => status.side = region,
                TriggerEvidence::OiExpansion(expansion) => {
                    let OiRejection::FailedAuction(status) = &mut expansion.rejection else {
                        unreachable!()
                    };
                    status.side = region;
                }
                _ => unreachable!(),
            }
            let plan = plan(SideRule::With, atr_rule("1"), "1");
            assert_eq!(
                admit(&plan, &state, &trigger),
                Admission::NoSide,
                "{name} {region:?}"
            );
        }
    }
}

const LABELS: [RegimeLabel; 4] = [
    RegimeLabel::Low,
    RegimeLabel::Medium,
    RegimeLabel::High,
    RegimeLabel::Extreme,
];

#[test]
fn location_any_and_the_regime_filters_follow_adr_040_d1() {
    let evidence = evidence();
    let trigger = sided(&evidence.effort, true);
    let regime = catalog::VOLATILITY_REGIME_1H_V1.key;
    let base = Plan {
        regime_features: vec![regime],
        ..plan(SideRule::With, atr_rule("1"), "1")
    };
    let states: Vec<Option<RegimeLabel>> = LABELS.map(Some).into_iter().chain([None]).collect();
    // `regime-filter none` and `location.any@1` admit every decision,
    // unready labels included.
    for &label in &states {
        let state = state_with(Some(1_000), Some(10), label);
        assert_ne!(
            admit(&base, &state, &trigger),
            Admission::Filtered,
            "{label:?}"
        );
        let intents = base.intents(
            decision(),
            &state,
            &[{
                let mut trigger = trigger.clone();
                trigger.feature = base.trigger;
                trigger
            }],
        );
        assert_eq!(intents[0].regime, [label]);
    }
    // Every non-empty label subset against every label and an unready one.
    let mut cases = 0;
    for mask in 1..16_u8 {
        let labels: BTreeSet<RegimeLabel> = LABELS
            .into_iter()
            .enumerate()
            .filter(|(i, _)| mask & (1 << i) != 0)
            .map(|(_, label)| label)
            .collect();
        let plan = Plan {
            regime_filters: vec![RegimeFilter {
                feature: regime,
                labels: labels.clone(),
            }],
            ..base.clone()
        };
        for &label in &states {
            let state = state_with(Some(1_000), Some(10), label);
            let admitted = label.is_some_and(|label| labels.contains(&label));
            assert_eq!(
                admit(&plan, &state, &trigger) != Admission::Filtered,
                admitted,
                "{labels:?} {label:?}"
            );
            cases += 1;
        }
    }
    assert_eq!(cases, 75);
    // Filtered comes first: it holds without a reference, a side or an ATR.
    let plan = Plan {
        regime_filters: vec![RegimeFilter {
            feature: regime,
            labels: [RegimeLabel::High].into(),
        }],
        ..base
    };
    let mut no_side = evidence.auction.clone();
    let TriggerEvidence::Auction(status) = &mut no_side.evidence else {
        unreachable!()
    };
    status.side = None;
    let state = state_with(None, None, Some(RegimeLabel::Low));
    assert_eq!(admit(&plan, &state, &no_side), Admission::Filtered);
}

/// Hand-computed offsets ⟨m·ATR⟩ in price units, half to even:
/// - m = 0.5, ATR = 3 → 1.5 → 2 (a tie rounds up to even);
/// - m = 0.5, ATR = 5 → 2.5 → 2 (a tie rounds down to even);
/// - m = 0.31333, ATR = 100 000 → 31 333 exactly (the labels' 1h offset);
/// - m = 0.4, ATR = 1 → 0.4 → 0: no risk.
#[test]
fn invalidation_atr_offsets_the_reference() {
    let evidence = evidence();
    let up = sided(&evidence.effort, true);
    let down = sided(&evidence.effort, false);
    let long = |multiple: &str, atr: i64, reference: i64| {
        admit(
            &plan(SideRule::With, atr_rule(multiple), "1"),
            &state_with(Some(reference), Some(atr), None),
            &up,
        )
    };
    let short = |multiple: &str, atr: i64, reference: i64| {
        admit(
            &plan(SideRule::With, atr_rule(multiple), "1"),
            &state_with(Some(reference), Some(atr), None),
            &down,
        )
    };
    assert_eq!(
        long("0.5", 3, 1_000),
        levels(PositionSide::Long, 1_000, 998, 1_002)
    );
    assert_eq!(
        long("0.5", 5, 1_000),
        levels(PositionSide::Long, 1_000, 998, 1_002)
    );
    assert_eq!(
        short("0.5", 3, 1_000),
        levels(PositionSide::Short, 1_000, 1_002, 998)
    );
    assert_eq!(
        long("0.31333", 100_000, 10_000_000),
        levels(PositionSide::Long, 10_000_000, 9_968_667, 10_031_333)
    );
    // An unready ATR and an offset that rounds to 0.
    assert_eq!(
        admit(
            &plan(SideRule::With, atr_rule("1"), "1"),
            &state_with(Some(1_000), None, None),
            &up
        ),
        Admission::NoRisk
    );
    assert_eq!(long("0.4", 1, 1_000), Admission::NoRisk);
    // A stop outside the i64 range, either way.
    assert_eq!(short("1", 2, i64::MAX - 1), Admission::NoRisk);
    assert_eq!(long("1", 2, i64::MIN + 1), Admission::NoRisk);
    // A negative stop is in the range, as for ADR-055's barriers.
    assert_eq!(long("1", 2, 1), levels(PositionSide::Long, 1, -1, 3));
}

/// `trigger` (an SFP) with its sweep extreme at `extreme` units.
fn with_extreme(trigger: &TriggerEvent, extreme: i64) -> TriggerEvent {
    let mut trigger = trigger.clone();
    let TriggerEvidence::Sfp(sweep) = &mut trigger.evidence else {
        unreachable!()
    };
    sweep.extreme = Price::from_units(extreme);
    trigger
}

/// Hand-computed: a swept high faded (`against`, short) at reference 1 000
/// with the extreme at 1 010, ATR 10 and b = 0.25: ⟨2.5⟩ = 2, so the stop is
/// 1 012, R = 12 and the target 988 at k = 1.
#[test]
fn invalidation_beyond_extreme_offsets_the_extreme() {
    let evidence = evidence();
    let high = sided(&evidence.sfp, true);
    let fade = plan(SideRule::Against, extreme_rule("0.25"), "1");
    let state = state_with(Some(1_000), Some(10), None);
    assert_eq!(
        admit(&fade, &state, &with_extreme(&high, 1_010)),
        levels(PositionSide::Short, 1_000, 1_012, 988)
    );
    // At b = 0 the stop is the extreme itself.
    let fade0 = plan(SideRule::Against, extreme_rule("0"), "1");
    assert_eq!(
        admit(&fade0, &state, &with_extreme(&high, 1_010)),
        levels(PositionSide::Short, 1_000, 1_010, 990)
    );
    // A stop on the wrong side is `stop_side`, but an unready ATR comes
    // first, even at b = 0 (D2, D5).
    let follow0 = plan(SideRule::With, extreme_rule("0"), "1");
    assert_eq!(
        admit(&follow0, &state, &with_extreme(&high, 1_010)),
        Admission::StopSide
    );
    assert_eq!(
        admit(
            &follow0,
            &state_with(Some(1_000), None, None),
            &with_extreme(&high, 1_010)
        ),
        Admission::NoRisk
    );
    // A stop equal to the reference is `stop_side`: R must be > 0.
    assert_eq!(
        admit(&fade0, &state, &with_extreme(&high, 1_000)),
        Admission::StopSide
    );
    // A stop beyond the i64 range is `no_risk`.
    assert_eq!(
        admit(&fade, &state, &with_extreme(&high, i64::MAX)),
        Admission::NoRisk
    );
}

/// Hand-computed targets reference + σ⟨k·R⟩:
/// - k = 1: symmetric, R = 10 → 1 010;
/// - k = 0.5, R = 3 → 1.5 → 2, and R = 5 → 2.5 → 2 (both tie directions);
/// - k = 0.4, R = 1 → 0: no risk.
#[test]
fn target_r_multiple_scales_the_risk() {
    let evidence = evidence();
    let up = sided(&evidence.effort, true);
    let long = |target: &str, atr: i64, reference: i64| {
        admit(
            &plan(SideRule::With, atr_rule("1"), target),
            &state_with(Some(reference), Some(atr), None),
            &up,
        )
    };
    assert_eq!(
        long("1", 10, 1_000),
        levels(PositionSide::Long, 1_000, 990, 1_010)
    );
    assert_eq!(
        long("0.5", 3, 1_000),
        levels(PositionSide::Long, 1_000, 997, 1_002)
    );
    assert_eq!(
        long("0.5", 5, 1_000),
        levels(PositionSide::Long, 1_000, 995, 1_002)
    );
    assert_eq!(
        long("2.5", 2, 1_000),
        levels(PositionSide::Long, 1_000, 998, 1_005)
    );
    assert_eq!(long("0.4", 1, 1_000), Admission::NoRisk);
    // A target outside the i64 range.
    assert_eq!(long("1", 2, i64::MAX - 1), Admission::NoRisk);
    // R outside the i64 range: the stop is in range, the distance is not.
    let state = state_with(Some(i64::MAX), Some(10), None);
    let high = sided(&evidence.sfp, true);
    assert_eq!(
        admit(
            &plan(SideRule::With, extreme_rule("0"), "1"),
            &state,
            &with_extreme(&high, -10)
        ),
        Admission::NoRisk
    );
}

#[test]
fn both_stages_keep_d4s_order() {
    let evidence = evidence();
    let plan = plan(SideRule::With, atr_rule("1"), "1");
    let mut no_side = evidence.auction.clone();
    let TriggerEvidence::Auction(status) = &mut no_side.evidence else {
        unreachable!()
    };
    status.side = Some(Region::In);
    no_side.feature = plan.trigger;
    let mut sided_up = sided(&evidence.effort, true);
    sided_up.feature = plan.trigger;
    // No reference: `no_side` still counts as such, and the `NoRisk`
    // placeholder of a sided emission counts as `stale_reference`.
    let state = state_with(None, Some(10), None);
    let intents = plan.intents(decision(), &state, &[no_side, sided_up]);
    assert_eq!(intents[0].admission, Admission::NoSide);
    assert_eq!(intents[1].admission, Admission::NoRisk);
    assert_eq!(intents[1].reference, None);
    let mut simulator = Simulator::new(*plan.costs());
    for intent in intents {
        simulator.decide(intent);
    }
    assert_eq!(
        simulator.stats(),
        SimulationStats {
            triggers: 2,
            skipped: SkipCounts {
                no_side: 1,
                stale_reference: 1,
                ..SkipCounts::default()
            },
            ..SimulationStats::default()
        }
    );
}

/// The candidate golden tape through the engine, the simulator and the
/// evaluator, as #170's driver will run them, on an S6-like spec with the
/// regime split.
#[test]
fn a_replay_runs_through_the_simulator() {
    let tape = crate::candidate::tests::golden_tape();
    let id = "trigger.effort_result.5m@1";
    let text = with_lines(
        &accept_spec(),
        &[(
            "regime-filter",
            "regime-filter volatility.regime.1h@1 LOW,MEDIUM,HIGH",
        )],
    );
    let plan = resolve(&parse(&text)).unwrap();
    let mut engine = MarketStateEngine::new();
    let mut simulator = Simulator::new(*plan.costs());
    let mut emissions = 0;
    for event in &tape {
        match engine.apply(event) {
            Ok(()) => {
                simulator.observe(event);
                emissions += engine
                    .trigger_events()
                    .iter()
                    .filter(|trigger| trigger.feature.to_string() == id)
                    .count();
                let intents = plan.intents(
                    event.canonical_key(),
                    engine.state(),
                    engine.trigger_events(),
                );
                for intent in intents {
                    assert_eq!(intent.regime.len(), 1);
                    simulator.decide(intent);
                }
            }
            Err(_) => simulator.reject(event),
        }
    }
    let (records, stats) = simulator.finish();
    assert!(emissions > 0);
    assert_eq!(stats.triggers, emissions as u64);
    assert_eq!(
        stats.triggers,
        stats.filtered + stats.skipped.total() + records.len() as u64
    );
    assert!(!records.is_empty());
    for record in &records {
        let sigma = record.side.sign();
        assert!(sigma * (record.reference.units() - record.stop.units()) > 0);
        assert_eq!(
            record.target.units() - record.reference.units(),
            record.reference.units() - record.stop.units()
        );
    }
}
