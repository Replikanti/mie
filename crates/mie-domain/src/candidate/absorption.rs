//! Absorption (ADR-051 D7): `candidate.absorption.<5m|15m|1h>@1`, live
//! only.
//!
//! At each judged effort-vs-result evaluation of a window, the book
//! liquidity window of the same length and the same minute is read on the
//! passive side opposite the aggressor (net buying lifts the ask, net
//! selling hits the bid), inside the [`BAND`]: the condition is the
//! effort-vs-result condition and `added ≥ filled`, at least as much
//! resting size came back as aggression took. ADR-043 D5 books an inferred
//! fill into both `added` and `filled`, so `added − filled` does not depend
//! on how much of the flow was inferred.
//!
//! No evaluation when the book window is warming up or unavailable, ends at
//! another minute, or its band saw an edge beyond the trusted window
//! (`beyond_window`): part of the flow was not observed. Such a minute
//! leaves the value as it was. Archive replays have no book, so absorption
//! stays warming up there.

use super::effort::EffortResult;
use crate::bars::Coverage;
use crate::book::Side;
use crate::event::Aggressor;
use crate::feature::{FeatureKey, FeatureValue, catalog};
use crate::fingerprint::Fingerprinter;
use crate::liquidity::LiquidityWindow;
use crate::num::{Qty, Rate};
use crate::state_hash::StateEncode;
use std::fmt;

/// The band read (ADR-051 D7): 5 bps of mid.
pub const BAND: Rate = catalog::ABSORPTION_BAND;

/// The label of a passive side, as the golden lines print it.
fn side_label(side: Side) -> &'static str {
    match side {
        Side::Bid => "bid",
        Side::Ask => "ask",
    }
}

/// One absorption evaluation: `candidate.absorption.<tf>@1`.
///
/// `Display` prints the effort-vs-result evaluation it read, the passive
/// side, the band, `added` and `filled` there, the book window's coverage,
/// whether the side was replenished, whether the condition holds, and the
/// feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Absorption {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// The judged effort-vs-result evaluation of the window.
    pub effort: EffortResult,
    /// The passive side opposite the aggressor.
    pub passive: Side,
    /// The band read: 5 bps of mid.
    pub band: Rate,
    /// Resting quantity added on the passive side within the band.
    pub added: Qty,
    /// Resting quantity taken by taker trades on that side within the band.
    pub filled: Qty,
    /// The book window's coverage: `partial_start` and `feed_gap` as
    /// `book.liquidity.window.<tf>@1` flags them.
    pub book_coverage: Coverage,
    /// `added ≥ filled`.
    pub replenished: bool,
    /// The effort-vs-result condition and `replenished`.
    pub active: bool,
}

impl StateEncode for Absorption {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            effort,
            passive,
            band,
            added,
            filled,
            book_coverage,
            replenished,
            active,
        } = self;
        feature.encode(f);
        effort.encode(f);
        passive.encode(f);
        band.encode(f);
        added.encode(f);
        filled.encode(f);
        book_coverage.encode(f);
        replenished.encode(f);
        active.encode(f);
    }
}

impl fmt::Display for Absorption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "effort=[{}] passive={} band={} added={} filled={} book={} replenished={} \
             active={} {}",
            self.effort,
            side_label(self.passive),
            self.band,
            self.added,
            self.filled,
            self.book_coverage,
            self.replenished,
            self.active,
            self.feature
        )
    }
}

/// The absorption state of one window. Engine state, updated in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AbsorptionJudge {
    feature: FeatureKey,
    /// Whether the last evaluation met the condition.
    active: bool,
}

impl AbsorptionJudge {
    /// The judge of the window at `position` in
    /// [`catalog::CANDIDATE_ABSORPTION`] order.
    pub(crate) fn new(position: usize) -> Self {
        Self {
            feature: catalog::CANDIDATE_ABSORPTION[position].1.key,
            active: false,
        }
    }

    /// The value before the first evaluation.
    pub(crate) fn initial() -> FeatureValue<Absorption> {
        FeatureValue::WarmingUp {
            observed: 0,
            required: 1,
        }
    }

    /// Absorption at the minute closed last (ADR-051 D7): `effort` is the
    /// window's judged evaluation of this minute, `book` its liquidity
    /// window. Updates `value` and returns the evaluation and whether its
    /// condition just turned true; `None` without an evaluation.
    pub(crate) fn step(
        &mut self,
        effort: Option<&EffortResult>,
        book: &FeatureValue<LiquidityWindow>,
        value: &mut FeatureValue<Absorption>,
    ) -> Option<(Absorption, bool)> {
        let effort = effort?;
        let window = book.ready().filter(|window| window.end == effort.end)?;
        let band = window.band(BAND).filter(|band| !band.beyond_window)?;
        let passive = match effort.aggressor {
            Aggressor::Buy => Side::Ask,
            Aggressor::Sell => Side::Bid,
        };
        let flow = band.side(passive);
        let replenished = flow.added >= flow.filled;
        let active = effort.active && replenished;
        let edge = active && !self.active;
        self.active = active;
        let absorption = Absorption {
            feature: self.feature,
            effort: *effort,
            passive,
            band: BAND,
            added: flow.added,
            filled: flow.filled,
            book_coverage: Coverage {
                partial_start: window.partial_start,
                feed_gap: window.feed_gap,
            },
            replenished,
            active,
        };
        *value = FeatureValue::Ready(absorption);
        Some((absorption, edge))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Timeframe;
    use crate::candidate::effort::tests::result;
    use crate::event::samples::t;
    use crate::feature::Unavailability;
    use crate::liquidity::{BandFlow, SideFlow};

    const END: i64 = 600_000;

    /// A 5m book window ending at `end` whose 5 bps band shows `ask` and
    /// `bid` flow, flagged `beyond` when asked.
    fn book(end: i64, bid: SideFlow, ask: SideFlow, beyond: bool) -> FeatureValue<LiquidityWindow> {
        let band = |band: Rate| BandFlow {
            band,
            bid,
            ask,
            beyond_window: beyond && band == BAND,
        };
        FeatureValue::Ready(LiquidityWindow {
            feature: catalog::BOOK_LIQUIDITY_WINDOW_5M_V1.key,
            timeframe: Timeframe::M5,
            end: t(end),
            bands: catalog::BOOK_BANDS.map(band),
            partial_start: false,
            feed_gap: false,
        })
    }

    fn flow(added: i64, filled: i64, inferred: i64) -> SideFlow {
        SideFlow {
            added: Qty::from_units(added),
            cancelled: Qty::from_units(0),
            filled: Qty::from_units(filled),
            inferred: Qty::from_units(inferred),
        }
    }

    /// A judged 5m evaluation ending at [`END`] that meets the condition,
    /// net buying when `delta > 0`.
    fn effort(delta: i64) -> EffortResult {
        let effort = result(Timeframe::M5, END, delta, 0, [10, 5, 10, 5]);
        assert!(effort.active);
        effort
    }

    fn step(
        effort: &EffortResult,
        book: &FeatureValue<LiquidityWindow>,
    ) -> Option<(Absorption, bool)> {
        let mut judge = AbsorptionJudge::new(0);
        let mut value = AbsorptionJudge::initial();
        let stepped = judge.step(Some(effort), book, &mut value);
        if let Some((absorption, _)) = stepped {
            assert_eq!(value, FeatureValue::Ready(absorption));
        } else {
            assert_eq!(value, AbsorptionJudge::initial());
        }
        stepped
    }

    #[test]
    fn added_equal_to_filled_fires_and_one_unit_less_does_not() {
        for (added, fires) in [(500, true), (499, false)] {
            // Net buying reads the ask; the bid's flow does not matter.
            let window = book(END, flow(0, 900, 0), flow(added, 500, 0), false);
            let (absorption, edge) = step(&effort(20), &window).unwrap();
            assert_eq!(absorption.passive, Side::Ask);
            assert_eq!(
                (absorption.replenished, absorption.active, edge),
                (fires, fires, fires)
            );
        }
        // Net selling reads the bid.
        let window = book(END, flow(500, 500, 0), flow(0, 900, 0), false);
        let (absorption, edge) = step(&effort(-20), &window).unwrap();
        assert_eq!(absorption.passive, Side::Bid);
        assert!(edge);
        // Replenished without the effort-vs-result condition: no candidate.
        let window = book(END, flow(0, 0, 0), flow(500, 500, 0), false);
        let quiet = result(Timeframe::M5, END, 9, 0, [10, 5, 10, 5]);
        let (absorption, edge) = step(&quiet, &window).unwrap();
        assert!(absorption.replenished && !absorption.active && !edge);
    }

    #[test]
    fn inferred_fills_do_not_change_the_decision() {
        // ADR-043 D5 books an inferred fill into `added` and `filled`: the
        // same flow with one more inferred unit on both sides of the
        // comparison decides the same.
        for (added, filled) in [(500, 500), (499, 500)] {
            let plain = book(END, flow(0, 0, 0), flow(added, filled, 0), false);
            let inferred = book(
                END,
                flow(0, 0, 0),
                flow(added + 300, filled + 300, 300),
                false,
            );
            let decide =
                |window| step(&effort(20), &window).map(|(absorption, _)| absorption.active);
            assert_eq!(decide(plain), decide(inferred));
        }
    }

    #[test]
    fn a_band_beyond_the_window_is_not_evaluated() {
        let window = book(END, flow(0, 0, 0), flow(500, 500, 0), true);
        assert_eq!(step(&effort(20), &window), None);
    }

    #[test]
    fn an_unavailable_or_warming_book_is_not_evaluated() {
        for window in [
            FeatureValue::Unavailable {
                reason: Unavailability::OutOfRange,
            },
            FeatureValue::WarmingUp {
                observed: 2,
                required: 5,
            },
            // A window of another minute.
            book(END - 60_000, flow(0, 0, 0), flow(500, 500, 0), false),
        ] {
            assert_eq!(step(&effort(20), &window), None);
        }
        // No judged evaluation: nothing to read.
        let mut judge = AbsorptionJudge::new(0);
        let mut value = AbsorptionJudge::initial();
        let window = book(END, flow(0, 0, 0), flow(500, 500, 0), false);
        assert_eq!(judge.step(None, &window, &mut value), None);
    }

    #[test]
    fn a_persisting_condition_emits_once() {
        let mut judge = AbsorptionJudge::new(0);
        let mut value = AbsorptionJudge::initial();
        let window = book(END, flow(0, 0, 0), flow(500, 500, 0), false);
        let effort = effort(20);
        let edges: Vec<bool> = (0..2)
            .map(|_| judge.step(Some(&effort), &window, &mut value).unwrap().1)
            .collect();
        assert_eq!(edges, [true, false]);
        // A minute without an evaluation does not re-arm.
        assert_eq!(judge.step(None, &window, &mut value), None);
        assert!(!judge.step(Some(&effort), &window, &mut value).unwrap().1);
    }
}
