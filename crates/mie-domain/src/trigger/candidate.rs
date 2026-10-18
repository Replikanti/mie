//! The trigger rules on order-flow candidates (ADR-053 D4, D6): CVD
//! divergence, effort vs result, exhaustion and absorption.
//!
//! A candidate is defined anywhere (ADR-051); its trigger is the candidate
//! fact at a defined location (ADR-012). The location gate: the context
//! the rule reads must be ready and list a level of one of the
//! [`GATE_KINDS`] — every kind but liquidity clusters (live only, within
//! 5 bps of mid, so the gate would mean different things live and in
//! archive replays), prior sweeps (traded through, the most numerous) and
//! validated references (no source yet).
//!
//! - A CVD divergence is a swing pair, and the comparison of the two swings
//!   is the candidate. The gate reads the registry at the current swing's
//!   price again, without every level either swing left in any structure
//!   registry ([`Exclude::Pair`]); the trigger carries that context. The
//!   candidate event, its evidence, keeps its own ADR-051 D3 context.
//! - Effort vs result, exhaustion and absorption anchor at the last 1m
//!   close, not at a swing, so no registry level is derived from the fact:
//!   the gate reads the candidate's own context, and the trigger carries
//!   it.
//!
//! No number of its own: the candidate's definition carries every
//! threshold. An open-interest divergence feeds no family (ADR-051).

use super::TriggerFamily;
use crate::candidate::absorption::Absorption;
use crate::candidate::divergence::CvdComparison;
use crate::candidate::effort::{EffortResult, Exhaustion};
use crate::candidate::{CandidateEvent, Exclude, LocationContext};
use crate::feature::{FeatureDefinition, FeatureKey, catalog};
use crate::location::{LevelKind, LevelSide, LocationState};
use crate::num::Price;
use crate::structure::Side;

/// The level kinds the gate admits (ADR-053 D4), in [`LevelKind`]
/// declaration order. The `gate_kinds` parameter of every candidate trigger
/// names them.
pub(super) const GATE_KINDS: [LevelKind; 9] = [
    LevelKind::Poc,
    LevelKind::Vah,
    LevelKind::Val,
    LevelKind::Hvn,
    LevelKind::Lvn,
    LevelKind::StructuralHigh,
    LevelKind::StructuralLow,
    LevelKind::SfpRejectionZone,
    LevelKind::Vwap,
];

/// Whether `context` is at a defined location: its levels are ready and
/// list a kind of [`GATE_KINDS`].
pub(super) fn at_location(context: &LocationContext) -> bool {
    context.levels.ready().is_some_and(|levels| {
        levels
            .levels
            .iter()
            .any(|level| GATE_KINDS.contains(&level.kind))
    })
}

/// The value a candidate trigger updates, with the candidate's hashed value
/// as its evidence (ADR-053 D6).
pub(super) enum Slot {
    /// `trigger.cvd_divergence.<tf>@1`, by position in
    /// [`catalog::TRIGGER_CVD_DIVERGENCE`].
    CvdDivergence(usize, CvdComparison),
    /// `trigger.effort_result.<w>@1`, by position in
    /// [`catalog::TRIGGER_EFFORT_RESULT`].
    EffortResult(usize, EffortResult),
    /// `trigger.exhaustion@1`.
    Exhaustion(Exhaustion),
    /// `trigger.absorption.<w>@1`, by position in
    /// [`catalog::TRIGGER_ABSORPTION`].
    Absorption(usize, Absorption),
}

/// The trigger a candidate fact makes.
pub(super) struct Triggered {
    /// Its family.
    pub(super) family: TriggerFamily,
    /// Its rule.
    pub(super) definition: &'static FeatureDefinition,
    /// The value it updates.
    pub(super) slot: Slot,
    /// Where it happened: the context the gate read, at its anchor.
    pub(super) context: LocationContext,
}

/// The trigger `fact` makes, if any: `None` for an open-interest
/// divergence and for a candidate the gate stops.
pub(super) fn triggered(fact: &CandidateEvent, location: &LocationState) -> Option<Triggered> {
    let (family, definition, slot, context) = match fact {
        CandidateEvent::CvdDivergence(event) => {
            let position = catalog::TRIGGER_CVD_DIVERGENCE
                .iter()
                .position(|(timeframe, _)| *timeframe == event.timeframe)?;
            let pair = event.comparison.pair;
            let context = LocationContext::read(
                pair.current.price,
                Some(Exclude::Pair {
                    side: match pair.side {
                        Side::High => LevelSide::High,
                        Side::Low => LevelSide::Low,
                    },
                    timeframe: event.timeframe,
                    previous: (pair.previous.price, pair.previous.swing_time),
                    current: (pair.current.price, pair.current.swing_time),
                }),
                location,
            );
            (
                TriggerFamily::CvdDivergence,
                catalog::TRIGGER_CVD_DIVERGENCE[position].1,
                Slot::CvdDivergence(position, event.comparison),
                context,
            )
        }
        CandidateEvent::EffortVsResult(event) => {
            let position = window(&catalog::CANDIDATE_EFFORT_RESULT, event.feature)?;
            (
                TriggerFamily::EffortVsResult,
                catalog::TRIGGER_EFFORT_RESULT[position].1,
                Slot::EffortResult(position, event.value),
                event.context.clone(),
            )
        }
        CandidateEvent::Exhaustion(event) => (
            TriggerFamily::Exhaustion,
            &catalog::TRIGGER_EXHAUSTION_V1,
            Slot::Exhaustion(event.value),
            event.context.clone(),
        ),
        CandidateEvent::Absorption(event) => {
            let position = window(&catalog::CANDIDATE_ABSORPTION, event.feature)?;
            (
                TriggerFamily::Absorption,
                catalog::TRIGGER_ABSORPTION[position].1,
                Slot::Absorption(position, event.value),
                event.context.clone(),
            )
        }
        // An open-interest divergence feeds no family (ADR-051).
        CandidateEvent::OiDivergence(_) => return None,
    };
    at_location(&context).then_some(Triggered {
        family,
        definition,
        slot,
        context,
    })
}

/// The position of the candidate `feature` among `windows`.
fn window(
    windows: &[(crate::bars::Timeframe, &FeatureDefinition); 3],
    feature: FeatureKey,
) -> Option<usize> {
    windows
        .iter()
        .position(|(_, definition)| definition.key == feature)
}

impl Triggered {
    /// The trigger's anchor: the current swing's price for a divergence,
    /// the last close otherwise; the price its context was read at.
    pub(super) fn anchor(&self) -> Price {
        self.context.anchor
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::candidate::{ContextLevel, ContextLevels};
    use crate::feature::{FeatureValue, Unavailability};
    use crate::location::AuctionStatus;

    /// A context at 100 units listing one level of each of `kinds`.
    fn context(levels: FeatureValue<Vec<LevelKind>>) -> LocationContext {
        let price = Price::from_units(100);
        let levels = match levels {
            FeatureValue::Ready(kinds) => FeatureValue::Ready(ContextLevels {
                feature: catalog::LOCATION_LEVELS_V2.key,
                levels: kinds
                    .into_iter()
                    .map(|kind| ContextLevel {
                        kind,
                        source: catalog::LOCATION_LEVELS_V2.key,
                        side: None,
                        anchor: None,
                        price,
                        low: price,
                        high: price,
                    })
                    .collect(),
            }),
            FeatureValue::WarmingUp { observed, required } => {
                FeatureValue::WarmingUp { observed, required }
            }
            FeatureValue::Unavailable { reason } => FeatureValue::Unavailable { reason },
        };
        LocationContext {
            anchor: price,
            levels,
            auction: FeatureValue::<AuctionStatus>::WarmingUp {
                observed: 0,
                required: 1,
            },
        }
    }

    #[test]
    fn each_admitted_kind_alone_passes_the_gate() {
        for kind in GATE_KINDS {
            assert!(
                at_location(&context(FeatureValue::Ready(vec![kind]))),
                "{kind}"
            );
        }
    }

    #[test]
    fn the_excluded_kinds_and_an_empty_context_do_not_pass() {
        for kind in [
            LevelKind::PriorSweep,
            LevelKind::LiquidityCluster,
            LevelKind::ValidatedReference,
        ] {
            assert!(
                !at_location(&context(FeatureValue::Ready(vec![kind]))),
                "{kind}"
            );
        }
        assert!(!at_location(&context(FeatureValue::Ready(vec![
            LevelKind::PriorSweep,
            LevelKind::LiquidityCluster,
            LevelKind::ValidatedReference,
        ]))));
        assert!(!at_location(&context(FeatureValue::Ready(Vec::new()))));
    }

    #[test]
    fn levels_that_are_not_ready_do_not_pass() {
        assert!(!at_location(&context(FeatureValue::WarmingUp {
            observed: 3,
            required: 4,
        })));
        assert!(!at_location(&context(FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        })));
    }

    #[test]
    fn the_gate_kinds_are_every_kind_but_three_in_declaration_order() {
        let every = [
            LevelKind::Poc,
            LevelKind::Vah,
            LevelKind::Val,
            LevelKind::Hvn,
            LevelKind::Lvn,
            LevelKind::StructuralHigh,
            LevelKind::StructuralLow,
            LevelKind::LiquidityCluster,
            LevelKind::PriorSweep,
            LevelKind::SfpRejectionZone,
            LevelKind::Vwap,
            LevelKind::ValidatedReference,
        ];
        let admitted: Vec<LevelKind> = every
            .into_iter()
            .filter(|kind| {
                !matches!(
                    kind,
                    LevelKind::LiquidityCluster
                        | LevelKind::PriorSweep
                        | LevelKind::ValidatedReference
                )
            })
            .collect();
        assert_eq!(admitted, GATE_KINDS);
    }

    #[test]
    fn every_candidate_trigger_fingerprints_the_gate_it_applies() {
        // ADR-053 D6: the `gate_kinds` parameter is GATE_KINDS by their
        // `Display` names, so the code and the fingerprint cannot drift; the
        // CVD ids skip the swing pair, the others nothing.
        let names: Vec<String> = GATE_KINDS.iter().map(ToString::to_string).collect();
        let text = names.join(",");
        let param = |definition: &FeatureDefinition, name: &str| {
            definition
                .params
                .iter()
                .find(|param| param.name == name)
                .map(|param| param.value)
        };
        let mut definitions: Vec<(&FeatureDefinition, &str)> = catalog::TRIGGER_CVD_DIVERGENCE
            .iter()
            .map(|(_, definition)| (*definition, "swing_pair"))
            .collect();
        definitions.extend(
            catalog::TRIGGER_EFFORT_RESULT
                .iter()
                .chain(&catalog::TRIGGER_ABSORPTION)
                .map(|(_, definition)| (*definition, "none")),
        );
        definitions.push((&catalog::TRIGGER_EXHAUSTION_V1, "none"));
        assert_eq!(definitions.len(), 11);
        for (definition, skip) in definitions {
            let Some(crate::feature::ParamValue::Text(kinds)) = param(definition, "gate_kinds")
            else {
                panic!("{}", definition.key)
            };
            assert_eq!(kinds, text, "{}", definition.key);
            assert_eq!(
                param(definition, "gate_skip"),
                Some(crate::feature::ParamValue::Text(skip)),
                "{}",
                definition.key
            );
        }
    }
}
