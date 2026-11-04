//! The rule registry v1 (ADR-060 D2): resolution of a spec's rule
//! references, and the evaluation of D3–D5 at each decision.
//!
//! - *Resolution.* [`resolve`] checks every rule reference of an
//!   [`ExperimentSpec`] against [`REGISTRY_V1`] and either lists every
//!   [`ResolutionError`] or builds a typed [`Plan`]. Parsing stays
//!   registry-free: which texts parse is unchanged (ADR-040 D3), and a spec
//!   that parses may still not resolve (D1, weak spot 3).
//! - *Evaluation.* [`Plan::intents`] turns each emission of the plan's
//!   trigger id into the [`TradeIntent`] that [`Simulator::decide`]
//!   takes: D4's checks 1, 2, 4 and 5, with the levels of D5. The
//!   simulator adds checks 3 and 6, so D4's order holds in one place.
//! - *Strict and data-driven.* The registry is one table of rows (slot,
//!   `id@N`, each parameter's name, type and range), and one generic
//!   resolver reads it, so every rule gets the same unknown, missing, type
//!   and range checks in the same order. Every parameter is required and
//!   none has a default (D2).
//! - *Exact.* Every level is integer fixed point with one half-to-even
//!   rounding per offset, the rule of ADR-055 D4's barriers (D5).
//!
//! [`Simulator::decide`]: super::Simulator::decide

use super::{Admission, Costs, FundingRule, Levels, LevelsError, PositionSide, TradeIntent};
use crate::candidate::CandidateEvent;
use crate::event::Aggressor;
use crate::feature::{FeatureKey, catalog};
use crate::location::{AuctionStatus, Region};
use crate::num::{Price, Rate, SCALE, div_half_even};
use crate::order::CanonicalKey;
use crate::regime::RegimeLabel;
use crate::research::{ExperimentSpec, RegimeFilter, RuleRef, SpecField, SpecValue};
use crate::state::MarketState;
use crate::structure::{Side, Sweep};
use crate::trigger::{OiRejection, TriggerEvent, TriggerEvidence, evidence_prices};
use std::fmt;

#[cfg(test)]
mod tests;

/// The type and the valid range of one rule parameter (ADR-060 D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamKind {
    /// `feature:<id@N>`: a `trigger.` member of the spec's set.
    TriggerFeature,
    /// `feature:<id@N>`: a `volatility.atr.` member of the spec's set that
    /// the backtest can read.
    AtrFeature,
    /// `text:with` or `text:against` (D3).
    Side,
    /// `rate:<x>` with x > 0.
    PositiveRate,
    /// `rate:<x>` with x ≥ 0.
    NonNegativeRate,
    /// `rate:<x>` with 0 ≤ x < 1.
    Fraction,
    /// `int:<n>` with 1 ≤ n ≤ sample end − start, in ms.
    Hold,
}

impl ParamKind {
    /// The type token the value must carry.
    pub const fn type_name(self) -> &'static str {
        match self {
            Self::TriggerFeature | Self::AtrFeature => "feature",
            Self::Side => "text",
            Self::PositiveRate | Self::NonNegativeRate | Self::Fraction => "rate",
            Self::Hold => "int",
        }
    }

    /// The value's syntax and range, as error messages suggest it.
    pub const fn syntax(self) -> &'static str {
        match self {
            Self::TriggerFeature => "feature:<trigger.* id@N of the features line>",
            Self::AtrFeature => "feature:<volatility.atr.* id@N of the features line>",
            Self::Side => "text:<with|against>",
            Self::PositiveRate => "rate:<decimal above 0>",
            Self::NonNegativeRate => "rate:<decimal at or above 0>",
            Self::Fraction => "rate:<decimal from 0, below 1>",
            Self::Hold => "int:<ms from 1 to the sample length>",
        }
    }

    /// The family a feature value must belong to, for the error text.
    const fn family(self) -> &'static str {
        match self {
            Self::TriggerFeature => "trigger.*",
            Self::AtrFeature => "volatility.atr.* feature with a reader",
            _ => "",
        }
    }

    /// Checks `value` against the type and the range; `sample_ms` is the
    /// sample length, the bound of [`ParamKind::Hold`].
    fn check(self, value: &SpecValue, sample_ms: i128) -> Result<(), Problem> {
        let wrong_type = Problem::WrongType {
            found: value.type_name(),
        };
        let in_range = match (self, value) {
            (Self::TriggerFeature, SpecValue::Feature(key)) => {
                return is_trigger(*key)
                    .then_some(())
                    .ok_or(Problem::WrongFamily { feature: *key });
            }
            (Self::AtrFeature, SpecValue::Feature(key)) => {
                return atr_reader(*key)
                    .map(|_| ())
                    .ok_or(Problem::WrongFamily { feature: *key });
            }
            (Self::Side, SpecValue::Text(text)) => SideRule::from_text(text).is_some(),
            (Self::PositiveRate, SpecValue::Rate(rate)) => rate.units() > 0,
            (Self::NonNegativeRate, SpecValue::Rate(rate)) => rate.units() >= 0,
            (Self::Fraction, SpecValue::Rate(rate)) => (0..SCALE).contains(&rate.units()),
            (Self::Hold, SpecValue::Int(ms)) => (1..=sample_ms).contains(&i128::from(*ms)),
            _ => return Err(wrong_type),
        };
        in_range.then_some(()).ok_or(Problem::OutOfRange)
    }
}

/// Why one parameter value failed [`ParamKind::check`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Problem {
    WrongType { found: &'static str },
    OutOfRange,
    WrongFamily { feature: FeatureKey },
}

/// One parameter of a rule: its name and kind (ADR-060 D2).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParamSchema {
    /// The parameter name.
    pub name: &'static str,
    /// Its type and range.
    pub kind: ParamKind,
}

/// One row of ADR-060 D2's table: a rule of one slot at one version, with
/// its parameters sorted by name, all required.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RuleSchema {
    /// The spec line the rule belongs on.
    pub slot: SpecField,
    /// The rule id.
    pub id: &'static str,
    /// The rule version.
    pub version: u32,
    /// The parameters, sorted by name; none has a default.
    pub params: &'static [ParamSchema],
}

impl RuleSchema {
    fn param(&self, name: &str) -> Option<&'static ParamSchema> {
        self.params.iter().find(|param| param.name == name)
    }
}

impl fmt::Display for RuleSchema {
    /// `id@N` and each parameter as `name=<syntax>`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.version)?;
        for param in self.params {
            write!(f, " {}={}", param.name, param.kind.syntax())?;
        }
        Ok(())
    }
}

const fn param(name: &'static str, kind: ParamKind) -> ParamSchema {
    ParamSchema { name, kind }
}

/// The rule registry v1: ADR-060 D2's table, in slot order. `state-filter`
/// has no rule (#31), and `regime-filter` stays ADR-040 D1's line.
pub const REGISTRY_V1: &[RuleSchema] = &[
    RuleSchema {
        slot: SpecField::Location,
        id: "location.any",
        version: 1,
        params: &[],
    },
    RuleSchema {
        slot: SpecField::Trigger,
        id: "trigger.fires",
        version: 1,
        params: &[param("feature", ParamKind::TriggerFeature)],
    },
    RuleSchema {
        slot: SpecField::Entry,
        id: "entry.next_trade",
        version: 1,
        params: &[param("side", ParamKind::Side)],
    },
    RuleSchema {
        slot: SpecField::Invalidation,
        id: "invalidation.atr",
        version: 1,
        params: &[
            param("atr", ParamKind::AtrFeature),
            param("multiple", ParamKind::PositiveRate),
        ],
    },
    RuleSchema {
        slot: SpecField::Invalidation,
        id: "invalidation.beyond_extreme",
        version: 1,
        params: &[
            param("atr", ParamKind::AtrFeature),
            param("buffer", ParamKind::NonNegativeRate),
        ],
    },
    RuleSchema {
        slot: SpecField::Target,
        id: "target.r_multiple",
        version: 1,
        params: &[
            param("max_hold_ms", ParamKind::Hold),
            param("r", ParamKind::PositiveRate),
        ],
    },
    RuleSchema {
        slot: SpecField::Slippage,
        id: "slippage.fixed",
        version: 1,
        params: &[param("rate", ParamKind::Fraction)],
    },
    RuleSchema {
        slot: SpecField::Funding,
        id: "funding.recorded",
        version: 1,
        params: &[],
    },
    RuleSchema {
        slot: SpecField::Funding,
        id: "funding.none",
        version: 1,
        params: &[],
    },
];

/// The rules of `slot`, in registry order, as `id@N` for the error text.
fn rules_of(slot: SpecField) -> String {
    let rules: Vec<String> = REGISTRY_V1
        .iter()
        .filter(|schema| schema.slot == slot)
        .map(|schema| format!("{}@{}", schema.id, schema.version))
        .collect();
    rules.join(", ")
}

/// Reads one ATR feature from the state: `Some` when it is `Ready`.
type AtrReader = fn(&MarketState) -> Option<Price>;

/// Reads one regime feature's label from the state: `Some` when it is
/// `Ready`.
type RegimeReader = fn(&MarketState) -> Option<RegimeLabel>;

fn atr_1h(state: &MarketState) -> Option<Price> {
    state.atr.ready().copied()
}

fn regime_1h(state: &MarketState) -> Option<RegimeLabel> {
    state.regime.ready().map(|regime| regime.label)
}

/// The ATR features the backtest reads, with the state field of each. The
/// `atr=` family check is membership here, so an ATR feature registered
/// later is refused until it gets a reader (the coverage test fails first).
const ATR_READERS: [(FeatureKey, AtrReader); 1] = [(catalog::VOLATILITY_ATR_1H_V1.key, atr_1h)];

/// The regime features the backtest reads, with the state field of each.
const REGIME_READERS: [(FeatureKey, RegimeReader); 1] =
    [(catalog::VOLATILITY_REGIME_1H_V1.key, regime_1h)];

fn atr_reader(key: FeatureKey) -> Option<AtrReader> {
    ATR_READERS
        .iter()
        .find(|(feature, _)| *feature == key)
        .map(|&(_, read)| read)
}

/// The reader of a regime feature of a spec's set.
///
/// # Panics
///
/// Never: a spec lists only registered features (ADR-040 D4), and every
/// registered `volatility.regime.*` feature has a reader, which the
/// coverage test pins (the pattern of `StateSnapshot::read`).
fn regime_reader(key: FeatureKey) -> RegimeReader {
    REGIME_READERS
        .iter()
        .find(|(feature, _)| *feature == key)
        .map(|&(_, read)| read)
        .expect("every registered regime feature has a reader")
}

fn is_trigger(key: FeatureKey) -> bool {
    key.id.as_str().starts_with("trigger.")
}

/// Whether a trigger id has an ADR-055 D8 extreme, so that
/// `invalidation.beyond_extreme@1` applies (ADR-060 D2): the SFP and CVD
/// divergence families, exhaustion, and the OI-expansion rejections of the
/// four structure timeframes. Every other id, including one registered
/// later, is refused until it is classified here.
fn has_extreme(trigger: FeatureKey) -> bool {
    let id = trigger.id.as_str();
    id.starts_with("trigger.sfp.")
        || id.starts_with("trigger.cvd_divergence.")
        || matches!(
            id,
            "trigger.exhaustion"
                | "trigger.oi_expansion_rejection.15m"
                | "trigger.oi_expansion_rejection.1h"
                | "trigger.oi_expansion_rejection.4h"
                | "trigger.oi_expansion_rejection.1d"
        )
}

/// Why a spec's rules do not resolve under registry v1 (ADR-060 D2). Each
/// names its spec line and the rule's `id@N` as written.
///
/// `Display` writes `<key>: <id@N>: <reason>` with a hint, the ADR-040 D1
/// style.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolutionError {
    /// No rule of this id belongs on the line.
    UnknownRule {
        /// The line.
        field: SpecField,
        /// The rule as written, `id@N`.
        rule: String,
    },
    /// The rule exists on the line, but not at this version.
    UnknownVersion {
        /// The line.
        field: SpecField,
        /// The rule as written, `id@N`.
        rule: String,
    },
    /// The rule has no parameter of this name.
    UnknownParam {
        /// The line.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The parameter name as written.
        param: String,
    },
    /// A required parameter is not given.
    MissingParam {
        /// The line.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The missing parameter.
        param: ParamSchema,
    },
    /// A parameter value of another type.
    WrongType {
        /// The line.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The parameter.
        param: ParamSchema,
        /// The type the parameter takes.
        expected: &'static str,
        /// The type given.
        found: &'static str,
    },
    /// A parameter value outside its domain of definition (D2).
    OutOfRange {
        /// The line.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The parameter.
        param: ParamSchema,
        /// The value given.
        value: SpecValue,
    },
    /// A feature parameter of the wrong family.
    WrongFamily {
        /// The line.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The parameter.
        param: ParamSchema,
        /// The feature given.
        feature: FeatureKey,
    },
    /// `invalidation.beyond_extreme@1` on a trigger id without an extreme.
    NoExtreme {
        /// The line: `invalidation`.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The trigger id of the `trigger` line.
        trigger: FeatureKey,
    },
    /// `latency` is not below `max_hold_ms`, so no entry can fill (D6).
    LatencyNotBelowHold {
        /// The line: `target`.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
        /// The spec's latency.
        latency_ms: u32,
        /// The hold.
        max_hold_ms: i64,
    },
    /// A `state-filter` rule: registry v1 has none (#31).
    StateFilterRule {
        /// The line: `state-filter`.
        field: SpecField,
        /// The rule, `id@N`.
        rule: String,
    },
}

impl ResolutionError {
    /// The spec line the error is on.
    pub fn field(&self) -> SpecField {
        match self {
            Self::UnknownRule { field, .. }
            | Self::UnknownVersion { field, .. }
            | Self::UnknownParam { field, .. }
            | Self::MissingParam { field, .. }
            | Self::WrongType { field, .. }
            | Self::OutOfRange { field, .. }
            | Self::WrongFamily { field, .. }
            | Self::NoExtreme { field, .. }
            | Self::LatencyNotBelowHold { field, .. }
            | Self::StateFilterRule { field, .. } => *field,
        }
    }

    /// The rule the error is about, `id@N`.
    pub fn rule(&self) -> &str {
        match self {
            Self::UnknownRule { rule, .. }
            | Self::UnknownVersion { rule, .. }
            | Self::UnknownParam { rule, .. }
            | Self::MissingParam { rule, .. }
            | Self::WrongType { rule, .. }
            | Self::OutOfRange { rule, .. }
            | Self::WrongFamily { rule, .. }
            | Self::NoExtreme { rule, .. }
            | Self::LatencyNotBelowHold { rule, .. }
            | Self::StateFilterRule { rule, .. } => rule,
        }
    }
}

impl fmt::Display for ResolutionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}: ", self.field(), self.rule())?;
        let expect = |param: &ParamSchema| format!("{}={}", param.name, param.kind.syntax());
        match self {
            Self::UnknownRule { field, .. } => {
                write!(f, "unknown rule ({field} rules: {})", rules_of(*field))
            }
            Self::UnknownVersion { field, .. } => {
                write!(f, "unknown version ({field} rules: {})", rules_of(*field))
            }
            Self::UnknownParam { field, rule, param } => {
                let schema = REGISTRY_V1
                    .iter()
                    .find(|schema| {
                        schema.slot == *field
                            && format!("{}@{}", schema.id, schema.version) == *rule
                    })
                    .map(ToString::to_string)
                    .unwrap_or_default();
                write!(f, "unknown parameter {param:?} (the rule is `{schema}`)")
            }
            Self::MissingParam { param, .. } => {
                write!(
                    f,
                    "missing parameter {}: add `{}`",
                    param.name,
                    expect(param)
                )
            }
            Self::WrongType {
                param,
                expected,
                found,
                ..
            } => write!(
                f,
                "{} has type {found}, expected {expected} (`{}`)",
                param.name,
                expect(param)
            ),
            Self::OutOfRange { param, value, .. } => write!(
                f,
                "{}={value} is out of range (`{}`)",
                param.name,
                expect(param)
            ),
            Self::WrongFamily { param, feature, .. } => write!(
                f,
                "{}={} is not a {} (`{}`)",
                param.name,
                SpecValue::Feature(*feature),
                param.kind.family(),
                expect(param)
            ),
            Self::NoExtreme { trigger, .. } => write!(
                f,
                "{trigger} has no extreme; use invalidation.atr@1, which takes every trigger"
            ),
            Self::LatencyNotBelowHold {
                latency_ms,
                max_hold_ms,
                ..
            } => write!(
                f,
                "latency {latency_ms} is not below max_hold_ms {max_hold_ms}, so no entry can \
                 fill; raise max_hold_ms or lower latency"
            ),
            Self::StateFilterRule { .. } => {
                write!(
                    f,
                    "registry v1 has no state-filter rule; use `state-filter none`"
                )
            }
        }
    }
}

impl std::error::Error for ResolutionError {}

/// How the entry side follows the evidence side (ADR-060 D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SideRule {
    /// `with`: up maps to long, down to short.
    With,
    /// `against`: up maps to short, down to long.
    Against,
}

impl SideRule {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "with" => Some(Self::With),
            "against" => Some(Self::Against),
            _ => None,
        }
    }

    fn side(self, evidence: EvidenceSide) -> PositionSide {
        match (self, evidence) {
            (Self::With, EvidenceSide::Up) | (Self::Against, EvidenceSide::Down) => {
                PositionSide::Long
            }
            (Self::With, EvidenceSide::Down) | (Self::Against, EvidenceSide::Up) => {
                PositionSide::Short
            }
        }
    }
}

/// The resolved invalidation rule (ADR-060 D2, D5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Invalidation {
    /// `invalidation.atr@1`: stop = reference ∓ ⟨m·ATR⟩.
    Atr {
        /// The ATR feature.
        atr: FeatureKey,
        /// m > 0.
        multiple: Rate,
    },
    /// `invalidation.beyond_extreme@1`: stop = extreme ∓ ⟨b·ATR⟩.
    BeyondExtreme {
        /// The ATR feature.
        atr: FeatureKey,
        /// b ≥ 0.
        buffer: Rate,
    },
}

impl Invalidation {
    fn atr(self) -> FeatureKey {
        match self {
            Self::Atr { atr, .. } | Self::BeyondExtreme { atr, .. } => atr,
        }
    }
}

/// A spec resolved under registry v1 (ADR-060 D2): everything the
/// evaluation and the simulator need, typed. Built only by [`resolve`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    /// The trigger id whose emissions are the decisions (D4).
    trigger: FeatureKey,
    /// The entry side rule (D3).
    side: SideRule,
    /// The invalidation rule (D5).
    invalidation: Invalidation,
    /// k > 0 of `target.r_multiple@1`.
    target: Rate,
    /// Latency, hold, slippage, taker fee and funding (D2, D6, D7).
    costs: Costs,
    /// The regime features of the spec's set, in id order (D11, D12).
    regime_features: Vec<FeatureKey>,
    /// The spec's regime filters (ADR-040 D1).
    regime_filters: Vec<RegimeFilter>,
}

/// One rule reference checked against its registry row: `valid` when no
/// error was found on it.
struct Checked<'a> {
    schema: &'static RuleSchema,
    rule: &'a RuleRef,
    sample_ms: i128,
    valid: bool,
}

impl<'a> Checked<'a> {
    /// The value of a parameter that passed its checks.
    fn value(&self, name: &str) -> Option<&'a SpecValue> {
        let kind = self.schema.param(name)?.kind;
        self.rule
            .param(name)
            .filter(|value| kind.check(value, self.sample_ms).is_ok())
    }

    fn feature(&self, name: &str) -> Option<FeatureKey> {
        match self.value(name)? {
            SpecValue::Feature(key) => Some(*key),
            _ => None,
        }
    }

    fn rate(&self, name: &str) -> Option<Rate> {
        match self.value(name)? {
            SpecValue::Rate(rate) => Some(*rate),
            _ => None,
        }
    }

    fn int(&self, name: &str) -> Option<i64> {
        match self.value(name)? {
            SpecValue::Int(value) => Some(*value),
            _ => None,
        }
    }

    fn text(&self, name: &str) -> Option<&'a str> {
        match self.value(name)? {
            SpecValue::Text(text) => Some(text),
            _ => None,
        }
    }

    /// The rule, when every check passed.
    fn resolved(&self) -> Option<&Self> {
        self.valid.then_some(self)
    }
}

/// The resolver's state: the sample bound and the errors in line order.
struct Resolver {
    sample_ms: i128,
    errors: Vec<ResolutionError>,
}

impl Resolver {
    /// Checks one rule reference of `field` against [`REGISTRY_V1`]: the
    /// id, the version, then the given parameters in name order, then the
    /// missing ones in name order. `None` for an unknown id or version,
    /// whose parameters are not checked.
    fn check<'a>(&mut self, field: SpecField, rule: &'a RuleRef) -> Option<Checked<'a>> {
        let name = format!("{}@{}", rule.id(), rule.version());
        let mut rows = REGISTRY_V1
            .iter()
            .filter(|schema| schema.slot == field && schema.id == rule.id())
            .peekable();
        if rows.peek().is_none() {
            self.errors
                .push(ResolutionError::UnknownRule { field, rule: name });
            return None;
        }
        let Some(schema) = rows.find(|schema| schema.version == rule.version()) else {
            self.errors
                .push(ResolutionError::UnknownVersion { field, rule: name });
            return None;
        };
        let before = self.errors.len();
        for given in rule.params() {
            let Some(&param) = schema.param(&given.name) else {
                self.errors.push(ResolutionError::UnknownParam {
                    field,
                    rule: name.clone(),
                    param: given.name.clone(),
                });
                continue;
            };
            let rule = name.clone();
            match param.kind.check(&given.value, self.sample_ms) {
                Ok(()) => {}
                Err(Problem::WrongType { found }) => {
                    self.errors.push(ResolutionError::WrongType {
                        field,
                        rule,
                        param,
                        expected: param.kind.type_name(),
                        found,
                    });
                }
                Err(Problem::OutOfRange) => self.errors.push(ResolutionError::OutOfRange {
                    field,
                    rule,
                    param,
                    value: given.value.clone(),
                }),
                Err(Problem::WrongFamily { feature }) => {
                    self.errors.push(ResolutionError::WrongFamily {
                        field,
                        rule,
                        param,
                        feature,
                    });
                }
            }
        }
        for &param in schema.params {
            if rule.param(param.name).is_none() {
                self.errors.push(ResolutionError::MissingParam {
                    field,
                    rule: name.clone(),
                    param,
                });
            }
        }
        Some(Checked {
            schema,
            rule,
            sample_ms: self.sample_ms,
            valid: self.errors.len() == before,
        })
    }
}

/// Resolves `spec`'s rule references under registry v1 (ADR-060 D2).
///
/// # Errors
///
/// Every [`ResolutionError`], in the canonical text's line order: the
/// `state-filter` lines (sorted), then `location`, `trigger`, `entry`,
/// `invalidation`, `target`, `slippage` and `funding`. Within one line the
/// given parameters come first, in name order, then the missing ones, in
/// name order, then the line's cross-rule error:
///
/// - [`ResolutionError::NoExtreme`] on the `invalidation` line, checked only
///   when the trigger resolved;
/// - [`ResolutionError::LatencyNotBelowHold`] on the `target` line, checked
///   only when `max_hold_ms` passed its own checks.
///
/// An unknown id or version skips that rule's parameter checks.
///
/// # Panics
///
/// Never: a spec lists only registered features, each registered regime
/// feature has a reader (the coverage test), and a rule without an error
/// has every value, so once every range is checked [`Costs::new`] cannot
/// refuse them.
pub fn resolve(spec: &ExperimentSpec) -> Result<Plan, Vec<ResolutionError>> {
    let sample = spec.sample();
    let mut resolver = Resolver {
        sample_ms: i128::from(sample.end().as_millis()) - i128::from(sample.start().as_millis()),
        errors: Vec::new(),
    };
    for rule in spec.state_filters() {
        resolver.errors.push(ResolutionError::StateFilterRule {
            field: SpecField::StateFilter,
            rule: format!("{}@{}", rule.id(), rule.version()),
        });
    }
    let location = resolver.check(SpecField::Location, spec.location());
    let location = location.as_ref().and_then(Checked::resolved);

    let trigger = resolver.check(SpecField::Trigger, spec.trigger());
    let trigger = trigger
        .as_ref()
        .and_then(Checked::resolved)
        .and_then(|checked| checked.feature("feature"));

    let entry = resolver.check(SpecField::Entry, spec.entry());
    let side = entry
        .as_ref()
        .and_then(Checked::resolved)
        .and_then(|checked| checked.text("side"))
        .and_then(SideRule::from_text);

    let checked = resolver.check(SpecField::Invalidation, spec.invalidation());
    if let (Some(checked), Some(trigger)) = (&checked, trigger)
        && checked.schema.id == "invalidation.beyond_extreme"
        && !has_extreme(trigger)
    {
        resolver.errors.push(ResolutionError::NoExtreme {
            field: SpecField::Invalidation,
            rule: format!("{}@{}", checked.schema.id, checked.schema.version),
            trigger,
        });
    }
    let invalidation =
        checked
            .as_ref()
            .and_then(Checked::resolved)
            .and_then(|checked| match checked.schema.id {
                "invalidation.atr" => Some(Invalidation::Atr {
                    atr: checked.feature("atr")?,
                    multiple: checked.rate("multiple")?,
                }),
                "invalidation.beyond_extreme" => Some(Invalidation::BeyondExtreme {
                    atr: checked.feature("atr")?,
                    buffer: checked.rate("buffer")?,
                }),
                _ => None,
            });

    let checked = resolver.check(SpecField::Target, spec.target());
    let latency_ms = spec.latency_ms();
    if let Some(checked) = &checked
        && let Some(max_hold_ms) = checked.int("max_hold_ms")
        && i64::from(latency_ms) >= max_hold_ms
    {
        resolver.errors.push(ResolutionError::LatencyNotBelowHold {
            field: SpecField::Target,
            rule: format!("{}@{}", checked.schema.id, checked.schema.version),
            latency_ms,
            max_hold_ms,
        });
    }
    let target = checked
        .as_ref()
        .and_then(Checked::resolved)
        .and_then(|checked| Some((checked.rate("r")?, checked.int("max_hold_ms")?)));

    let slippage = resolver.check(SpecField::Slippage, spec.slippage());
    let slippage = slippage
        .as_ref()
        .and_then(Checked::resolved)
        .and_then(|checked| checked.rate("rate"));

    let funding = resolver.check(SpecField::Funding, spec.funding());
    let funding = funding
        .as_ref()
        .and_then(Checked::resolved)
        .and_then(|checked| match checked.schema.id {
            "funding.recorded" => Some(FundingRule::Recorded),
            "funding.none" => Some(FundingRule::None),
            _ => None,
        });

    let regime_features = spec.regime_features();
    // Fails before any drive, not at the first decision.
    for &key in &regime_features {
        regime_reader(key);
    }
    if !resolver.errors.is_empty() {
        return Err(resolver.errors);
    }
    // Every rule passed its checks, so every value is present.
    let (r, max_hold_ms) = target.expect("the target resolved");
    let costs = Costs::new(
        latency_ms,
        max_hold_ms,
        slippage.expect("the slippage resolved"),
        spec.fees().taker,
        funding.expect("the funding resolved"),
    )
    .expect("resolution checked every cost range");
    debug_assert!(location.is_some(), "the location resolved");
    Ok(Plan {
        trigger: trigger.expect("the trigger resolved"),
        side: side.expect("the entry resolved"),
        invalidation: invalidation.expect("the invalidation resolved"),
        target: r,
        costs,
        regime_features,
        regime_filters: spec.regime_filters().to_vec(),
    })
}

/// The side of a trigger's evidence (ADR-060 D3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EvidenceSide {
    Up,
    Down,
}

impl EvidenceSide {
    const fn of_level(side: Side) -> Self {
        match side {
            Side::High => Self::Up,
            Side::Low => Self::Down,
        }
    }

    const fn of_aggressor(aggressor: Aggressor) -> Self {
        match aggressor {
            Aggressor::Buy => Self::Up,
            Aggressor::Sell => Self::Down,
        }
    }

    fn of_sweep(sweep: &Sweep) -> Self {
        Self::of_level(sweep.level.side)
    }

    /// Above value is up, below is down; inside value or no side gives
    /// none.
    fn of_auction(status: &AuctionStatus) -> Option<Self> {
        match status.side {
            Some(Region::Above) => Some(Self::Up),
            Some(Region::Below) => Some(Self::Down),
            Some(Region::In) | None => None,
        }
    }

    /// D3's table: one arm per evidence variant, each reading the
    /// evidence's own sided fact.
    fn of(evidence: &TriggerEvidence) -> Option<Self> {
        match evidence {
            TriggerEvidence::Auction(status) => Self::of_auction(status),
            TriggerEvidence::Sfp(sweep) => Some(Self::of_sweep(sweep)),
            TriggerEvidence::Candidate(fact) => Some(match fact.as_ref() {
                CandidateEvent::CvdDivergence(event) => Self::of_level(event.comparison.pair.side),
                // No trigger family is built on an OI divergence (ADR-053
                // D7); the arm keeps the function total, with the CVD rule.
                CandidateEvent::OiDivergence(event) => Self::of_level(event.comparison.pair.side),
                CandidateEvent::EffortVsResult(event) => Self::of_aggressor(event.value.aggressor),
                CandidateEvent::Exhaustion(event) => {
                    Self::of_aggressor(event.value.expansion.aggressor)
                }
                CandidateEvent::Absorption(event) => {
                    Self::of_aggressor(event.value.effort.aggressor)
                }
            }),
            TriggerEvidence::OiExpansion(expansion) => match &expansion.rejection {
                OiRejection::Sfp(sweep) => Some(Self::of_sweep(sweep)),
                OiRejection::FailedAuction(status) => Self::of_auction(status),
            },
        }
    }
}

/// ⟨value × rate⟩ in price units: one half-to-even rounding (ADR-060 D2).
fn times_rate(value: i64, rate: Rate) -> i128 {
    div_half_even(
        i128::from(value) * i128::from(rate.units()),
        i128::from(SCALE),
    )
}

/// `units` as a price, or `None` outside the `i64` unit range.
fn price(units: i128) -> Option<Price> {
    i64::try_from(units).ok().map(Price::from_units)
}

impl Plan {
    /// The trigger id whose emissions are the decisions (ADR-060 D4).
    pub fn trigger(&self) -> FeatureKey {
        self.trigger
    }

    /// The resolved costs, for [`Simulator::new`](super::Simulator::new).
    pub fn costs(&self) -> &Costs {
        &self.costs
    }

    /// The regime features of the spec's set, in id order: the labels each
    /// [`TradeIntent::regime`] carries (ADR-060 D11, D12).
    pub fn regime_features(&self) -> &[FeatureKey] {
        &self.regime_features
    }

    /// One intent per emission of the plan's trigger id in `triggers`, in
    /// emission order (ADR-060 D4, D8), each with D4's checks 1, 2, 4 and 5
    /// decided from `state`.
    ///
    /// Driver contract: call it after the decision event is applied to the
    /// engine and after [`Simulator::observe`](super::Simulator::observe)
    /// of that event, with the engine's state and trigger events, then
    /// hand each intent to [`Simulator::decide`](super::Simulator::decide)
    /// in order. Prefix events are never evaluated (D4).
    ///
    /// Without a reference the intent carries [`Admission::NoRisk`] as a
    /// placeholder: the simulator's check 3 (`stale_reference`) comes
    /// first, so it never counts as `no_risk`.
    pub fn intents(
        &self,
        decision: CanonicalKey,
        state: &MarketState,
        triggers: &[TriggerEvent],
    ) -> Vec<TradeIntent> {
        let reference = state.last_trade_price.ready().copied();
        let regime: Vec<Option<RegimeLabel>> = self
            .regime_features
            .iter()
            .map(|&key| regime_reader(key)(state))
            .collect();
        let admitted = self.regime_filters.iter().all(|filter| {
            regime_reader(filter.feature)(state).is_some_and(|label| filter.labels.contains(&label))
        });
        let atr = atr_reader(self.invalidation.atr()).and_then(|read| read(state));
        triggers
            .iter()
            .filter(|trigger| trigger.feature == self.trigger)
            .map(|trigger| TradeIntent {
                decision,
                reference,
                regime: regime.clone(),
                admission: if admitted {
                    self.admission(trigger, reference, atr)
                } else {
                    Admission::Filtered
                },
            })
            .collect()
    }

    /// D4's checks 2, 4 and 5 for one emission that passed the filters
    /// (check 1), with the levels of D5.
    fn admission(
        &self,
        trigger: &TriggerEvent,
        reference: Option<Price>,
        atr: Option<Price>,
    ) -> Admission {
        // Check 2: the side (D3).
        let Some(evidence) = EvidenceSide::of(&trigger.evidence) else {
            return Admission::NoSide;
        };
        let side = self.side.side(evidence);
        // Check 3 belongs to the simulator.
        let Some(reference) = reference else {
            return Admission::NoRisk;
        };
        // Check 4: an unready ATR, under either rule (D2, D5).
        let Some(atr) = atr else {
            return Admission::NoRisk;
        };
        let sigma = i128::from(side.sign());
        let stop = match self.invalidation {
            Invalidation::Atr { multiple, .. } => {
                let offset = times_rate(atr.units(), multiple);
                if offset <= 0 {
                    return Admission::NoRisk;
                }
                i128::from(reference.units()) - sigma * offset
            }
            Invalidation::BeyondExtreme { buffer, .. } => {
                // Resolution admits only ids with an extreme (D2), so a
                // missing one never occurs; it would leave no risk.
                let Some(extreme) = evidence_prices(trigger).extreme else {
                    return Admission::NoRisk;
                };
                i128::from(extreme.units()) - sigma * times_rate(atr.units(), buffer)
            }
        };
        let Some(stop) = price(stop) else {
            return Admission::NoRisk;
        };
        // Check 5: the stop is beyond the reference, equality refused (D5).
        let risk = sigma * (i128::from(reference.units()) - i128::from(stop.units()));
        if risk <= 0 {
            return Admission::StopSide;
        }
        // The target needs R, so its checks follow check 5; each failure
        // leaves no valid risk (D5).
        let Ok(risk) = i64::try_from(risk) else {
            return Admission::NoRisk;
        };
        let offset = times_rate(risk, self.target);
        if offset <= 0 {
            return Admission::NoRisk;
        }
        let Some(target) = price(i128::from(reference.units()) + sigma * offset) else {
            return Admission::NoRisk;
        };
        match Levels::new(side, reference, stop, target) {
            Ok(levels) => Admission::Levels(levels),
            Err(LevelsError::StopSide) => Admission::StopSide,
            Err(LevelsError::TargetSide | LevelsError::Risk) => Admission::NoRisk,
        }
    }
}
