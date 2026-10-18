//! The location context of a candidate event (ADR-051 D3): where the
//! candidate's anchor price sits among the registry levels, and the auction
//! state, read from the committed state of the event that emits it.
//!
//! The context is recorded, never a filter and never a feature input:
//! location gating is a trigger-rule condition (ADR-012, ADR-053 D4), and
//! keeping location out of every candidate definition keeps
//! `location.levels@1` selectable (ADR-044 D10). The trigger events of
//! ADR-053 carry the same context, read by the same code.

use crate::bars::Timeframe;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::location::{
    AuctionStatus, LevelKind, LevelSet, LevelSide, LocationLevel, LocationState, zone_contains,
};
use crate::num::Price;
use crate::time::EventTime;
use std::fmt;

/// The identity and zone of one registry level whose padded zone contains
/// the anchor price: the fields that name the level, without its score
/// components.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextLevel {
    /// Which level.
    pub kind: LevelKind,
    /// The feature that produced it.
    pub source: FeatureKey,
    /// The side, for structure and cluster levels.
    pub side: Option<LevelSide>,
    /// Open time of the swing bar for a structure level; the open of the
    /// UTC day for the VWAP. `None` otherwise.
    pub anchor: Option<EventTime>,
    /// The level's price.
    pub price: Price,
    /// Lower edge of the zone (inclusive).
    pub low: Price,
    /// Upper edge of the zone (inclusive).
    pub high: Price,
}

impl fmt::Display for ContextLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.kind)?;
        if let Some(side) = self.side {
            write!(f, ":{side}")?;
        }
        write!(f, "@{}", self.price)?;
        if (self.low, self.high) != (self.price, self.price) {
            write!(f, "[{},{}]", self.low, self.high)?;
        }
        Ok(())
    }
}

/// The registry levels at the anchor: the registry version that was read
/// and every level whose zone, padded by the registry's tolerance, contains
/// the anchor price, in registry order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextLevels {
    /// The registry read: `location.levels@2`, or `@1` on an engine that
    /// selected it.
    pub feature: FeatureKey,
    /// The levels at the anchor.
    pub levels: Vec<ContextLevel>,
}

/// Where a candidate happened (ADR-051 D3): its anchor price, the registry
/// levels at that price and the auction state. Facts for research and for
/// the trigger rules; a candidate never filters on them.
///
/// `Display` prints `ctx=<anchor>` then the levels (`warming`,
/// `unavailable` or the comma-separated list, `-` when none) and the
/// auction label.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationContext {
    /// The price the levels are tested at: the swing price for a
    /// divergence.
    pub anchor: Price,
    /// The registry levels whose padded zone contains `anchor`, as the
    /// registry value stood.
    pub levels: FeatureValue<ContextLevels>,
    /// A copy of `location.auction.prior_day@1`.
    pub auction: FeatureValue<AuctionStatus>,
}

impl LocationContext {
    /// The context of `anchor` in `location`, without the level `exclude`
    /// names (ADR-051 D3).
    pub(crate) fn read(anchor: Price, exclude: Option<Exclude>, location: &LocationState) -> Self {
        Self {
            anchor,
            levels: match &location.levels {
                FeatureValue::Ready(set) => FeatureValue::Ready(levels_at(set, anchor, exclude)),
                FeatureValue::WarmingUp { observed, required } => FeatureValue::WarmingUp {
                    observed: *observed,
                    required: *required,
                },
                FeatureValue::Unavailable { reason } => {
                    FeatureValue::Unavailable { reason: *reason }
                }
            },
            auction: location.auction,
        }
    }
}

/// The registry levels left out of a context: every level derived from a
/// swing — its structural level, and the prior sweep and SFP zone it
/// becomes when a trade goes beyond it, the confirming trade included —
/// or, for a CVD divergence's trigger, from both swings of its pair. The
/// swing is the event itself, not a place it happened at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Exclude {
    /// A candidate's swing (ADR-051 D3): the levels of the
    /// `structure.levels.<tf>` registry the swing entered, with its side
    /// and swing bar.
    OwnRegistry {
        /// The swing's side.
        side: LevelSide,
        /// The id of the `structure.levels.<tf>` registry the swing
        /// entered.
        source_id: &'static str,
        /// Open time of the swing bar.
        anchor: EventTime,
    },
    /// A swept swing (ADR-053 D4): the levels of every
    /// `structure.levels.<tf>` registry with its side and price whose swing
    /// bar nests with its swing bar — the same extreme, as each structure
    /// timeframe that saw it registered it.
    EveryRegistry {
        /// The swing's side.
        side: LevelSide,
        /// The swing's price.
        price: Price,
        /// The swing's timeframe.
        timeframe: Timeframe,
        /// Open time of the swing bar.
        anchor: EventTime,
    },
    /// Both swings of a CVD divergence pair (ADR-053 D4): every level the
    /// [`EveryRegistry`](Self::EveryRegistry) key names for the current
    /// swing or for the previous one. The comparison of the two is the
    /// candidate, so neither swing is a place it happened at.
    Pair {
        /// The side of both swings.
        side: LevelSide,
        /// The timeframe of both swings.
        timeframe: Timeframe,
        /// The previous swing: its price and the open time of its swing
        /// bar.
        previous: (Price, EventTime),
        /// The current swing: its price and the open time of its swing
        /// bar.
        current: (Price, EventTime),
    },
}

impl Exclude {
    /// Whether `level` is derived from the swing (or, for a pair, from
    /// either swing).
    fn matches(&self, level: &LocationLevel) -> bool {
        match *self {
            Self::OwnRegistry {
                side,
                source_id,
                anchor,
            } => {
                level.side == Some(side)
                    && level.anchor == Some(anchor)
                    && level.source.id.as_str() == source_id
            }
            Self::EveryRegistry {
                side,
                price,
                timeframe,
                anchor,
            } => of_extreme(level, side, timeframe, (price, anchor)),
            Self::Pair {
                side,
                timeframe,
                previous,
                current,
            } => {
                of_extreme(level, side, timeframe, current)
                    || of_extreme(level, side, timeframe, previous)
            }
        }
    }
}

/// Whether `level` marks the extreme of one swing as some structure
/// timeframe registered it (ADR-053 D4): a `structure.levels.<tf>` level
/// of the swing's side and price whose swing bar nests with the swing's
/// bar, given as its price and open time.
fn of_extreme(
    level: &LocationLevel,
    side: LevelSide,
    timeframe: Timeframe,
    (price, anchor): (Price, EventTime),
) -> bool {
    level.side == Some(side)
        && level.price == price
        && level.anchor.is_some_and(|level_anchor| {
            structure_timeframe(level.source).is_some_and(|level_timeframe| {
                nests((level_anchor, level_timeframe), (anchor, timeframe))
            })
        })
}

/// The timeframe of a `structure.levels.<tf>` source, by id: any version
/// of that registry. `None` for every other source.
fn structure_timeframe(source: FeatureKey) -> Option<Timeframe> {
    catalog::STRUCTURE_LEVELS
        .iter()
        .find(|(_, definition)| definition.key.id == source.id)
        .map(|(timeframe, _)| *timeframe)
}

/// Whether one of two bars, each its open time and timeframe, lies within
/// the other. The structure timeframes share the UTC grid, so two of their
/// bars either nest or do not overlap.
fn nests(
    (a, a_timeframe): (EventTime, Timeframe),
    (b, b_timeframe): (EventTime, Timeframe),
) -> bool {
    let (a_open, b_open) = (a.as_millis(), b.as_millis());
    let (a_end, b_end) = (a_open + a_timeframe.millis(), b_open + b_timeframe.millis());
    (a_open <= b_open && b_end <= a_end) || (b_open <= a_open && a_end <= b_end)
}

/// The levels of `set` at `anchor`, in registry order, without `exclude`.
fn levels_at(set: &LevelSet, anchor: Price, exclude: Option<Exclude>) -> ContextLevels {
    ContextLevels {
        feature: set.feature,
        levels: set
            .levels()
            .iter()
            .filter(|level| zone_contains(level, anchor))
            .filter(|level| !exclude.is_some_and(|exclude| exclude.matches(level)))
            .map(|level| ContextLevel {
                kind: level.kind,
                source: level.source,
                side: level.side,
                anchor: level.anchor,
                price: level.price,
                low: level.low,
                high: level.high,
            })
            .collect(),
    }
}

impl fmt::Display for LocationContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ctx={} levels=", self.anchor)?;
        match &self.levels {
            FeatureValue::Ready(levels) => {
                if levels.levels.is_empty() {
                    f.write_str("-")?;
                }
                for (index, level) in levels.levels.iter().enumerate() {
                    if index > 0 {
                        f.write_str(",")?;
                    }
                    write!(f, "{level}")?;
                }
                write!(f, " {}", levels.feature)?;
            }
            FeatureValue::WarmingUp { .. } => f.write_str("warming")?,
            FeatureValue::Unavailable { .. } => f.write_str("unavailable")?,
        }
        match &self.auction {
            FeatureValue::Ready(status) => write!(f, " auction={}", status.state),
            FeatureValue::WarmingUp { .. } => f.write_str(" auction=warming"),
            FeatureValue::Unavailable { .. } => f.write_str(" auction=unavailable"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;
    const M15: i64 = 900_000;

    /// A level of `kind` from `source` with `side`, `anchor` and `price`.
    fn level(
        kind: LevelKind,
        source: FeatureKey,
        side: LevelSide,
        anchor: i64,
        price: i64,
    ) -> LocationLevel {
        LocationLevel {
            kind,
            source,
            side: Some(side),
            anchor: Some(EventTime::from_millis(anchor)),
            price: Price::from_units(price),
            low: Price::from_units(price),
            high: Price::from_units(price + 10),
            known_at: EventTime::from_millis(anchor + HOUR),
            touches: 0,
            in_zone: false,
            distance: Price::from_units(0),
            confluence: 0,
            strength: None,
        }
    }

    /// A 1h swing-high pair: the previous swing at 100 in the hour from
    /// 2h, the current one at 110 in the hour from 7h.
    fn pair() -> Exclude {
        Exclude::Pair {
            side: LevelSide::High,
            timeframe: Timeframe::H1,
            previous: (Price::from_units(100), EventTime::from_millis(2 * HOUR)),
            current: (Price::from_units(110), EventTime::from_millis(7 * HOUR)),
        }
    }

    #[test]
    fn a_pair_names_either_swing_on_every_structure_registry() {
        // ADR-053 D4: the levels of either swing, as any structure
        // timeframe registered its extreme, whatever their kind.
        let m15 = catalog::STRUCTURE_LEVELS_15M_V1.key;
        let h1 = catalog::STRUCTURE_LEVELS_1H_V1.key;
        let h4 = catalog::STRUCTURE_LEVELS_4H_V1.key;
        for matched in [
            // The previous swing's prior sweep and SFP zone on its own
            // registry, and on 15m inside its hour.
            level(LevelKind::PriorSweep, h1, LevelSide::High, 2 * HOUR, 100),
            level(
                LevelKind::SfpRejectionZone,
                h1,
                LevelSide::High,
                2 * HOUR,
                100,
            ),
            level(
                LevelKind::SfpRejectionZone,
                m15,
                LevelSide::High,
                2 * HOUR + 2 * M15,
                100,
            ),
            level(
                LevelKind::PriorSweep,
                m15,
                LevelSide::High,
                2 * HOUR + 3 * M15,
                100,
            ),
            // The current swing on 15m and in the 4h bar that holds it.
            level(
                LevelKind::StructuralHigh,
                m15,
                LevelSide::High,
                7 * HOUR + M15,
                110,
            ),
            level(
                LevelKind::SfpRejectionZone,
                h4,
                LevelSide::High,
                4 * HOUR,
                110,
            ),
        ] {
            assert!(pair().matches(&matched), "{matched:?}");
        }
    }

    #[test]
    fn a_pair_keeps_every_other_place() {
        let m15 = catalog::STRUCTURE_LEVELS_15M_V1.key;
        for kept in [
            // An equal-price swing outside both swings' bars: a place price
            // came back to.
            level(
                LevelKind::SfpRejectionZone,
                m15,
                LevelSide::High,
                4 * HOUR,
                100,
            ),
            level(
                LevelKind::StructuralHigh,
                m15,
                LevelSide::High,
                9 * HOUR,
                110,
            ),
            // The other side at the same price and bar.
            level(
                LevelKind::SfpRejectionZone,
                m15,
                LevelSide::Low,
                2 * HOUR,
                100,
            ),
            // Another price in the same bar.
            level(
                LevelKind::SfpRejectionZone,
                m15,
                LevelSide::High,
                2 * HOUR,
                101,
            ),
            // A non-structure source at the same price, side and bar.
            level(
                LevelKind::Vwap,
                catalog::LOCATION_VWAP_UTC_DAY_V1.key,
                LevelSide::High,
                2 * HOUR,
                100,
            ),
        ] {
            assert!(!pair().matches(&kept), "{kept:?}");
        }
    }

    #[test]
    fn a_pair_is_the_union_of_the_two_swing_keys() {
        // The previous swing's key is the key an SFP of that swing uses
        // (`EveryRegistry`), and so is the current swing's.
        let Exclude::Pair {
            side,
            timeframe,
            previous,
            current,
        } = pair()
        else {
            unreachable!()
        };
        let key = |(price, anchor): (Price, EventTime)| Exclude::EveryRegistry {
            side,
            price,
            timeframe,
            anchor,
        };
        let m15 = catalog::STRUCTURE_LEVELS_15M_V1.key;
        for anchor in (0..40).map(|bar| bar * M15) {
            for price in [100, 110] {
                for side in [LevelSide::High, LevelSide::Low] {
                    let candidate = level(LevelKind::PriorSweep, m15, side, anchor, price);
                    assert_eq!(
                        pair().matches(&candidate),
                        key(previous).matches(&candidate) || key(current).matches(&candidate),
                        "{candidate:?}"
                    );
                }
            }
        }
    }
}
