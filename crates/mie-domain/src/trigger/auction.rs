//! The trigger rules on auction facts (ADR-053 D5): failed auction, failed
//! reclaim and breakout + acceptance.
//!
//! Each reads one `LocationEvent::Auction` of `location.auction.prior_day@1`
//! and its own `status`, never the state's auction value: at a UTC-day
//! rollover the state already holds the new reference, and the old day's
//! last close is visible only in its event (ADR-044 D2). No number of its
//! own: the failure and acceptance rules are ADR-044's.

use super::TriggerFamily;
use crate::feature::{FeatureDefinition, catalog};
use crate::location::{AuctionState, AuctionStatus, Region};
use crate::num::Price;
use crate::time::EventTime;

/// The auction trigger families, in [`Triggers`](super::Triggers) order,
/// with their definitions.
pub(super) const AUCTION_TRIGGERS: [(TriggerFamily, &FeatureDefinition); 3] = [
    (
        TriggerFamily::FailedAuction,
        &catalog::TRIGGER_FAILED_AUCTION_V1,
    ),
    (
        TriggerFamily::FailedReclaim,
        &catalog::TRIGGER_FAILED_RECLAIM_V1,
    ),
    (
        TriggerFamily::BreakoutAcceptance,
        &catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1,
    ),
];

/// The trigger an auction fact makes, if any: the position of its family
/// in [`AUCTION_TRIGGERS`] and its anchor.
///
/// - failed auction: `to` is `FailedBreakout` and the failure is the fact's
///   own close (`failure.at == known_at`); anchored at the crossed edge,
///   VAH toward above, VAL toward below;
/// - failed reclaim: `to` is `FailedReclaim`, the same failure test;
///   anchored at the edge of the accepted region, VAH above, VAL below;
/// - breakout + acceptance: from `Breakout` to `Acceptance`, the probe's
///   `N_acc`-th counted close; anchored at the crossed edge, which the
///   accepted region now is.
pub(super) fn triggered(
    from: Option<AuctionState>,
    to: AuctionState,
    known_at: EventTime,
    status: &AuctionStatus,
) -> Option<(usize, Price)> {
    let failure = status.failure.filter(|failure| failure.at == known_at);
    match to {
        AuctionState::FailedBreakout => {
            let failure = failure?;
            edge(failure.toward, status).map(|anchor| (0, anchor))
        }
        AuctionState::FailedReclaim => {
            failure?;
            edge(status.accepted, status).map(|anchor| (1, anchor))
        }
        AuctionState::Acceptance if from == Some(AuctionState::Breakout) => {
            edge(status.accepted, status).map(|anchor| (2, anchor))
        }
        AuctionState::InsideValue
        | AuctionState::AtValueEdge
        | AuctionState::OutsideValue
        | AuctionState::Breakout
        | AuctionState::Acceptance => None,
    }
}

/// The value edge on the side of `region`: VAH above, VAL below; none
/// inside, which no failed breakout, accepted reclaim region or accepted
/// breakout can be.
fn edge(region: Region, status: &AuctionStatus) -> Option<Price> {
    match region {
        Region::Above => Some(status.vah),
        Region::Below => Some(status.val),
        Region::In => None,
    }
}
