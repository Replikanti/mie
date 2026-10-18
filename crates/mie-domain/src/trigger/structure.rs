//! The trigger rule on structure facts (ADR-053 D5): the swing-failure
//! pattern of every structure timeframe.
//!
//! A `StructureEvent::Sfp` of `structure.levels.<tf>@1` is a
//! `trigger.sfp.<tf>@1`; a clean break is none. The anchor is the swept
//! level's price, and the location context leaves out every registry level
//! derived from the swept swing itself (ADR-053 D4, the ADR-051 D3 reason):
//! the swing is the fact, not a place it happened at. No confirmation delay
//! beyond ADR-037's SFP window.

use crate::candidate::Exclude;
use crate::feature::{FeatureDefinition, catalog};
use crate::location::LevelSide;
use crate::structure::{STRUCTURE_TIMEFRAMES, Side, StructureEvent, Sweep};

/// The SFP trigger a structure fact makes, if any: the position of its
/// timeframe in [`STRUCTURE_TIMEFRAMES`], its definition and the resolved
/// sweep.
pub(super) fn triggered(
    fact: &StructureEvent,
) -> Option<(usize, &'static FeatureDefinition, &Sweep)> {
    let StructureEvent::Sfp(sweep) = fact else {
        return None;
    };
    let position = STRUCTURE_TIMEFRAMES
        .iter()
        .position(|timeframe| *timeframe == sweep.timeframe)?;
    let (_, definition) = catalog::TRIGGER_SFP[position];
    Some((position, definition, sweep))
}

/// The registry levels of the swept swing: same `structure.levels.<tf>`
/// source, side and swing bar — its structural level, and the prior sweep
/// and SFP zone it became.
pub(super) fn own_swing(sweep: &Sweep) -> Exclude {
    Exclude {
        side: match sweep.level.side {
            Side::High => LevelSide::High,
            Side::Low => LevelSide::Low,
        },
        source_id: sweep.feature.id.as_str(),
        anchor: sweep.level.swing_time,
    }
}
