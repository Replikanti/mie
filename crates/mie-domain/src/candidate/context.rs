//! The location context of a candidate event (ADR-051 D3): where the
//! candidate's anchor price sits among the registry levels, and the auction
//! state, read from the committed state of the event that emits it.
//!
//! The context is recorded, never a filter and never a feature input:
//! location gating is a trigger-rule condition owned by the bias/trigger
//! issue (ADR-012), and keeping location out of every candidate definition
//! keeps `location.levels@1` selectable (ADR-044 D10).

use crate::feature::{FeatureKey, FeatureValue};
use crate::location::{
    AuctionStatus, LevelKind, LevelSet, LevelSide, LocationState, zone_contains,
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

/// The registry levels left out of a context: every level derived from the
/// candidate's own swing (ADR-051 D3) — its structural level, and the prior
/// sweep and SFP zone it becomes when a trade goes beyond it, the
/// confirming trade included. The swing is the candidate itself, not a
/// place it happened at.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Exclude {
    /// The swing's side.
    pub(crate) side: LevelSide,
    /// The id of the `structure.levels.<tf>` registry the swing entered.
    pub(crate) source_id: &'static str,
    /// Open time of the swing bar.
    pub(crate) anchor: EventTime,
}

impl Exclude {
    /// Whether a level of any kind is derived from the swing: same
    /// registry, same side, anchored at the swing bar.
    fn matches(
        &self,
        side: Option<LevelSide>,
        source: FeatureKey,
        anchor: Option<EventTime>,
    ) -> bool {
        side == Some(self.side)
            && anchor == Some(self.anchor)
            && source.id.as_str() == self.source_id
    }
}

/// The levels of `set` at `anchor`, in registry order, without `exclude`.
fn levels_at(set: &LevelSet, anchor: Price, exclude: Option<Exclude>) -> ContextLevels {
    ContextLevels {
        feature: set.feature,
        levels: set
            .levels()
            .iter()
            .filter(|level| zone_contains(level, anchor))
            .filter(|level| {
                !exclude
                    .is_some_and(|exclude| exclude.matches(level.side, level.source, level.anchor))
            })
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
