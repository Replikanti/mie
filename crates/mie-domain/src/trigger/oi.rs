//! The trigger rule of open-interest expansion followed by rejection
//! (ADR-053 D7): `trigger.oi_expansion_rejection.<15m|1h|4h|1d>@1`, whose
//! rejection is that timeframe's SFP, and
//! `trigger.oi_expansion_rejection.prior_day@1`, whose rejection is the
//! failed auction of ADR-053 D5.
//!
//! An excursion beyond a level starts at an event — the sweeping trade of
//! an SFP, the 1m close that starts or retargets a probe toward above or
//! below — and is rejected at another, the SFP or the failing close. The
//! trigger fires when the `derivatives.oi.5m@1` point at the rejecting
//! event lies on a later boundary than the point at the starting event,
//! has the same source resolution and strictly higher open interest. It
//! carries both points and the change; no magnitude threshold (ADR-013).
//!
//! **The point at an event** at time `t` is the held ready point of the
//! boundary `⌊(t − OI_LAG_MS) / grid⌋ · grid`, else none ([`OiPoints::at`]):
//! the boundary lies in `[t − 325 000, t − 25 000]` ms. Every structure bar
//! ends on a grid boundary, and the archive orders a boundary's point at
//! the boundary itself while a live feed closes it at its first poll after
//! the boundary; reading the boundary the lag before the event, rather
//! than the newest point, lets both sources select the same boundary
//! whenever the live feed is continuous (ADR-019). Their values still
//! differ by `resolution_ms`, so the rule never compares across a seam.
//!
//! State, rebuilt by replay: the last two ready points seen, one start
//! record per pending sweep with a point, and one probe record.

use crate::bars::Timeframe;
use crate::derivatives::{OI_GRID, OiGridPoint};
use crate::feature::FeatureValue;
use crate::fingerprint::Fingerprinter;
use crate::location::{AuctionStatus, Region};
use crate::num::Qty;
use crate::state_hash::StateEncode;
use crate::structure::{Side, Sweep};
use crate::time::EventTime;
use std::fmt;

/// How long before an event the boundary its point is read at lies:
/// parameter `oi_lag_ms` of every OI-expansion trigger (ADR-053 D7). The
/// longest live spacing ADR-042 D1 still calls continuous: the 10 000 ms
/// resolution of the live feed plus its 15 000 ms `step_tolerance_ms`.
pub const OI_LAG_MS: i64 = 25_000;

/// The rejection an expansion is judged at (ADR-053 D7): the fact the
/// trigger value follows, in its input feature's hashed form.
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OiRejection {
    /// A `structure.levels.<tf>@1` sweep resolved as an SFP.
    Sfp(Sweep),
    /// A `location.auction.prior_day@1` failed auction: the status of the
    /// failing close.
    FailedAuction(AuctionStatus),
}

impl StateEncode for OiRejection {
    /// `write_u8` in declaration order, Sfp 0, FailedAuction 1, then the
    /// fact (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        match self {
            Self::Sfp(sweep) => {
                f.write_u8(0);
                sweep.encode(f);
            }
            Self::FailedAuction(status) => {
                f.write_u8(1);
                status.encode(f);
            }
        }
    }
}

impl fmt::Display for OiRejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sfp(sweep) => write!(f, "sfp {} {sweep}", sweep.timeframe),
            Self::FailedAuction(status) => write!(f, "failed_auction {status}"),
        }
    }
}

/// An open-interest expansion over an excursion that was rejected
/// (ADR-053 D7): the rejection, the points at the excursion's start and at
/// the rejection, and the change between them.
///
/// `Display` prints `<rejection> start=[<point>] end=[<point>]
/// change=<change>`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OiExpansion {
    /// The rejecting fact.
    pub rejection: OiRejection,
    /// The point at the event that started the excursion.
    pub start: OiGridPoint,
    /// The point at the rejecting event: a later boundary, the same
    /// resolution, strictly higher open interest.
    pub end: OiGridPoint,
    /// `end.open_interest − start.open_interest`, positive.
    pub change: Qty,
}

impl StateEncode for OiExpansion {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            rejection,
            start,
            end,
            change,
        } = self;
        rejection.encode(f);
        start.encode(f);
        end.encode(f);
        change.encode(f);
    }
}

impl fmt::Display for OiExpansion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} start=[{}] end=[{}] change={}",
            self.rejection, self.start, self.end, self.change
        )
    }
}

/// The expansion between `start` and `end` judged at `rejection`, if any:
/// both points have the same `resolution_ms`, `end` lies on a later
/// boundary and holds strictly more open interest. A change that does not
/// fit a [`Qty`] (only possible with negative open interest) is none.
pub(super) fn expansion(
    rejection: OiRejection,
    start: OiGridPoint,
    end: OiGridPoint,
) -> Option<OiExpansion> {
    let higher = i128::from(end.open_interest.units()) > i128::from(start.open_interest.units());
    if start.resolution_ms != end.resolution_ms || end.boundary <= start.boundary || !higher {
        return None;
    }
    let change = end.open_interest.checked_sub(start.open_interest)?;
    Some(OiExpansion {
        rejection,
        start,
        end,
        change,
    })
}

/// The last two ready `derivatives.oi.5m@1` points the stage saw, newest
/// first. Two suffice: the boundary [`Self::at`] reads for an event at `t`
/// is at most one grid interval before the newest boundary closed by `t`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(super) struct OiPoints {
    newest: Option<OiGridPoint>,
    previous: Option<OiGridPoint>,
}

impl OiPoints {
    /// Takes the committed value: a ready point on a later boundary than
    /// the newest held shifts it in. A value that stays on the same
    /// boundary, or is not ready, changes nothing.
    pub(super) fn observe(&mut self, value: &FeatureValue<OiGridPoint>) {
        let FeatureValue::Ready(point) = value else {
            return;
        };
        if self
            .newest
            .is_none_or(|newest| point.boundary > newest.boundary)
        {
            self.previous = self.newest;
            self.newest = Some(*point);
        }
    }

    /// The point at an event at `time` (ADR-053 D7): the held point of the
    /// boundary `OI_GRID.open_of(time − OI_LAG_MS)`, else none — also when
    /// that boundary closed unavailable or never closed.
    pub(super) fn at(&self, time: EventTime) -> Option<OiGridPoint> {
        let lagged = time.as_millis().checked_sub(OI_LAG_MS)?;
        let boundary = OI_GRID.open_of(EventTime::from_millis(lagged))?;
        [self.newest, self.previous]
            .into_iter()
            .flatten()
            .find(|point| point.boundary == boundary)
    }
}

/// The start point of a pending sweep, keyed by the swept level: its
/// timeframe, side and swing bar.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SweepStart {
    timeframe: Timeframe,
    side: Side,
    swing_time: EventTime,
    start: OiGridPoint,
}

/// The record key of `sweep`, ordered: timeframe length, side, swing bar.
fn sweep_key(timeframe: Timeframe, side: Side, swing_time: EventTime) -> (i64, Side, EventTime) {
    (timeframe.millis(), side, swing_time)
}

impl SweepStart {
    fn key(&self) -> (i64, Side, EventTime) {
        sweep_key(self.timeframe, self.side, self.swing_time)
    }
}

/// Records the point at `sweep`'s sweeping trade, if there is one, in
/// `starts`, kept ordered by key (ADR-025: no hash collection). A sweep
/// without a point leaves no record, so its SFP has no comparison.
pub(super) fn record_sweep(starts: &mut Vec<SweepStart>, sweep: &Sweep, points: &OiPoints) {
    let Some(start) = points.at(sweep.known_at) else {
        return;
    };
    let record = SweepStart {
        timeframe: sweep.timeframe,
        side: sweep.level.side,
        swing_time: sweep.level.swing_time,
        start,
    };
    match starts.binary_search_by_key(&record.key(), SweepStart::key) {
        // A level is swept once; a repeat would be a new sweep of it.
        Ok(index) => starts[index] = record,
        Err(index) => starts.insert(index, record),
    }
}

/// Removes the record of `sweep` from `starts` at its resolution and
/// returns its start point, if it had one.
pub(super) fn take_sweep(starts: &mut Vec<SweepStart>, sweep: &Sweep) -> Option<OiGridPoint> {
    let key = sweep_key(sweep.timeframe, sweep.level.side, sweep.level.swing_time);
    starts
        .binary_search_by_key(&key, SweepStart::key)
        .ok()
        .map(|index| starts.remove(index).start)
}

/// The probe record (ADR-053 D7): the identity of the committed probe
/// toward above or below — its reference, region and first close — and
/// the point at that close, recorded the first time the stage sees the
/// identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProbeStart {
    reference_start: EventTime,
    toward: Region,
    since: EventTime,
    start: Option<OiGridPoint>,
}

impl ProbeStart {
    /// The record after an event, from `held` (the record before it) and
    /// the committed `auction`: the held record while the committed probe
    /// keeps its identity, a new one with the point at `probe.since` when
    /// the identity is new (a probe starts or retargets), and none without
    /// a probe toward above or below (inside value, accepted, failed,
    /// warming up, unavailable, or a new reference without a probe).
    ///
    /// The status changes only at a classified 1m close, and an event
    /// classifies at most one close with trades (ADR-031), so a new
    /// identity is seen at its own close; a later event at the same
    /// millisecond sees the same identity and records nothing.
    pub(super) fn next(
        held: Option<Self>,
        auction: &FeatureValue<AuctionStatus>,
        points: &OiPoints,
    ) -> Option<Self> {
        let FeatureValue::Ready(status) = auction else {
            return None;
        };
        let probe = status.probe.filter(|probe| probe.toward != Region::In)?;
        match held {
            Some(held)
                if held.reference_start == status.reference_start
                    && held.toward == probe.toward
                    && held.since == probe.since =>
            {
                Some(held)
            }
            _ => Some(Self {
                reference_start: status.reference_start,
                toward: probe.toward,
                since: probe.since,
                start: points.at(probe.since),
            }),
        }
    }

    /// The start point of the probe the failed auction `status` failed, if
    /// `held` is its record: the same reference and region. Read before
    /// [`Self::next`] drops the record in the same event, so a failure at a
    /// UTC-day rollover, classified under the old reference before the
    /// switch, still finds it.
    pub(super) fn failed(held: Option<Self>, status: &AuctionStatus) -> Option<OiGridPoint> {
        let failure = status.failure?;
        let held = held?;
        (held.reference_start == status.reference_start && held.toward == failure.toward)
            .then_some(held.start)
            .flatten()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bars::Coverage;
    use crate::feature::{Unavailability, catalog};
    use crate::location::{AuctionState, Failure, Origin, Position, Probe};
    use crate::num::{Price, SCALE};
    use crate::structure::{Level, SweepOutcome};

    const GRID: i64 = 300_000;
    const B: i64 = 30 * GRID;

    fn t(millis: i64) -> EventTime {
        EventTime::from_millis(millis)
    }

    /// A ready point of `boundary` with `oi` whole BTC at `resolution_ms`.
    fn point(boundary: i64, oi: i64, resolution_ms: u32) -> OiGridPoint {
        OiGridPoint {
            feature: catalog::DERIVATIVES_OI_5M_V1.key,
            boundary: t(boundary),
            open_interest: Qty::from_units(oi * SCALE),
            sample_time: t(boundary),
            resolution_ms,
            delta: None,
        }
    }

    fn sweep() -> Sweep {
        Sweep {
            feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
            timeframe: Timeframe::M15,
            level: Level {
                side: Side::High,
                price: Price::from_units(200 * SCALE),
                swing_time: t(0),
                confirmed_bar_end: t(3_600_000),
                known_at: t(3_600_001),
                touches: 0,
                coverage: Coverage::default(),
            },
            time: t(B + 30_000),
            price: Price::from_units(201 * SCALE),
            extreme: Price::from_units(201 * SCALE),
            window_bars: 0,
            outcome: SweepOutcome::Pending,
            resolved_bar_end: None,
            known_at: t(B + 30_000),
            coverage: Coverage::default(),
        }
    }

    #[test]
    fn an_expansion_needs_a_later_boundary_higher_oi_and_one_resolution() {
        let rejection = OiRejection::Sfp(sweep());
        let start = point(B, 80_000, 300_000);
        let end = point(B + GRID, 80_001, 300_000);
        assert_eq!(
            expansion(rejection, start, end),
            Some(OiExpansion {
                rejection,
                start,
                end,
                change: Qty::from_units(SCALE),
            })
        );
        // Equal open interest is no expansion; nor is a fall.
        assert_eq!(
            expansion(rejection, start, point(B + GRID, 80_000, 300_000)),
            None
        );
        assert_eq!(
            expansion(rejection, start, point(B + GRID, 79_999, 300_000)),
            None
        );
        // The same boundary has no comparison; nor has an earlier one.
        assert_eq!(expansion(rejection, start, point(B, 80_001, 300_000)), None);
        assert_eq!(
            expansion(
                rejection,
                point(B + GRID, 80_000, 300_000),
                point(B, 80_001, 300_000)
            ),
            None
        );
        // No comparison across an archive/live seam.
        assert_eq!(
            expansion(rejection, start, point(B + GRID, 80_001, 10_000)),
            None
        );
        // A change outside `Qty` (negative open interest) is none.
        let low = OiGridPoint {
            open_interest: Qty::from_units(-i64::MAX),
            ..start
        };
        let high = OiGridPoint {
            open_interest: Qty::from_units(i64::MAX),
            ..end
        };
        assert_eq!(expansion(rejection, low, high), None);
    }

    #[test]
    fn the_point_at_an_event_is_the_lagged_boundary() {
        let mut points = OiPoints::default();
        points.observe(&FeatureValue::Ready(point(B - GRID, 1, 300_000)));
        points.observe(&FeatureValue::Ready(point(B, 2, 300_000)));
        // The same boundary again, or a value not ready, changes nothing.
        points.observe(&FeatureValue::Ready(point(B, 3, 300_000)));
        points.observe(&FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        });
        assert_eq!(points.at(t(B + OI_LAG_MS)), Some(point(B, 2, 300_000)));
        assert_eq!(
            points.at(t(B + OI_LAG_MS - 1)),
            Some(point(B - GRID, 1, 300_000))
        );
        assert_eq!(
            points.at(t(B + GRID + OI_LAG_MS - 1)),
            points.at(t(B + OI_LAG_MS))
        );
        // A boundary not held — older, or not closed yet — is none.
        assert_eq!(points.at(t(B - GRID + OI_LAG_MS - 1)), None);
        assert_eq!(points.at(t(B + GRID + OI_LAG_MS)), None);
        // A boundary that closed unavailable leaves a hole: none across it.
        let mut holed = OiPoints::default();
        holed.observe(&FeatureValue::Ready(point(B - 2 * GRID, 1, 300_000)));
        holed.observe(&FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        });
        holed.observe(&FeatureValue::Ready(point(B, 2, 300_000)));
        assert_eq!(holed.at(t(B + OI_LAG_MS - 1)), None);
        assert_eq!(holed.at(t(B + OI_LAG_MS)), Some(point(B, 2, 300_000)));
        // A newer boundary shifts the oldest out.
        holed.observe(&FeatureValue::Ready(point(B + GRID, 3, 300_000)));
        assert_eq!(holed.at(t(B - GRID + OI_LAG_MS)), None);
        assert_eq!(holed.at(t(B + OI_LAG_MS)), Some(point(B, 2, 300_000)));
    }

    #[test]
    fn a_sweep_records_its_start_until_it_resolves() {
        let mut points = OiPoints::default();
        let mut starts = Vec::new();
        // No point at the sweeping trade: no record.
        record_sweep(&mut starts, &sweep(), &points);
        assert!(starts.is_empty());
        points.observe(&FeatureValue::Ready(point(B, 7, 300_000)));
        let low = Sweep {
            level: Level {
                side: Side::Low,
                ..sweep().level
            },
            ..sweep()
        };
        record_sweep(&mut starts, &low, &points);
        record_sweep(&mut starts, &sweep(), &points);
        assert_eq!(starts.len(), 2);
        assert!(starts.windows(2).all(|pair| pair[0].key() < pair[1].key()));
        assert_eq!(
            take_sweep(&mut starts, &sweep()),
            Some(point(B, 7, 300_000))
        );
        assert_eq!(take_sweep(&mut starts, &sweep()), None);
        assert_eq!(take_sweep(&mut starts, &low), Some(point(B, 7, 300_000)));
        assert!(starts.is_empty());
    }

    /// A ready status under the reference starting at `reference`, with
    /// `probe`.
    fn status(reference: i64, probe: Option<Probe>) -> FeatureValue<AuctionStatus> {
        FeatureValue::Ready(AuctionStatus {
            feature: catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key,
            reference_start: t(reference),
            val: Price::from_units(100 * SCALE),
            vah: Price::from_units(200 * SCALE),
            state: AuctionState::Breakout,
            side: probe.map(|probe| probe.toward),
            accepted: Region::In,
            origin: Origin::Start,
            position: Position::Above,
            probe,
            failure: None,
            last_close: Price::from_units(210 * SCALE),
            bar_end: t(B),
            known_at: t(B + 1_000),
            coverage: Coverage::default(),
        })
    }

    fn probe(toward: Region, since: i64) -> Option<Probe> {
        Some(Probe {
            toward,
            since: t(since),
            closes: 1,
        })
    }

    #[test]
    fn the_probe_record_follows_the_committed_probe() {
        let mut points = OiPoints::default();
        points.observe(&FeatureValue::Ready(point(B, 7, 300_000)));
        let since = B + OI_LAG_MS + 1_000;
        let above = status(0, probe(Region::Above, since));
        let held = ProbeStart::next(None, &above, &points).unwrap();
        assert_eq!(held.start, Some(point(B, 7, 300_000)));
        // The same identity keeps its record, also when an OI sample at the
        // same millisecond closed a boundary since.
        points.observe(&FeatureValue::Ready(point(B + GRID, 8, 300_000)));
        assert_eq!(ProbeStart::next(Some(held), &above, &points), Some(held));
        // A count does not change the identity.
        let counted = status(
            0,
            Some(Probe {
                closes: 2,
                ..probe(Region::Above, since).unwrap()
            }),
        );
        assert_eq!(ProbeStart::next(Some(held), &counted, &points), Some(held));
        // A retarget replaces it, with the point at its own close.
        let later = B + GRID + OI_LAG_MS;
        let below = status(0, probe(Region::Below, later));
        let retargeted = ProbeStart::next(Some(held), &below, &points).unwrap();
        assert_eq!(
            (retargeted.toward, retargeted.since, retargeted.start),
            (Region::Below, t(later), Some(point(B + GRID, 8, 300_000)))
        );
        // A probe toward value, none, warming up and a new reference
        // without a probe drop it.
        for auction in [
            status(0, probe(Region::In, later)),
            status(0, None),
            FeatureValue::WarmingUp {
                observed: 0,
                required: 1,
            },
            status(86_400_000, None),
        ] {
            assert_eq!(ProbeStart::next(Some(held), &auction, &points), None);
        }
        // A new reference with a probe is a new identity.
        let renewed = ProbeStart::next(
            Some(held),
            &status(86_400_000, probe(Region::Above, since)),
            &points,
        )
        .unwrap();
        assert_eq!(renewed.reference_start, t(86_400_000));
    }

    #[test]
    fn a_failure_finds_the_record_of_its_reference_and_region() {
        let mut points = OiPoints::default();
        points.observe(&FeatureValue::Ready(point(B, 7, 300_000)));
        let since = B + OI_LAG_MS;
        let held = ProbeStart::next(None, &status(0, probe(Region::Above, since)), &points);
        let FeatureValue::Ready(base) = status(0, None) else {
            unreachable!()
        };
        let failed = |reference: i64, toward: Region| AuctionStatus {
            reference_start: t(reference),
            state: AuctionState::FailedBreakout,
            failure: Some(Failure {
                toward,
                closes: 3,
                at: t(since + 180_000),
                closes_back: 1,
            }),
            ..base
        };
        assert_eq!(
            ProbeStart::failed(held, &failed(0, Region::Above)),
            Some(point(B, 7, 300_000))
        );
        assert_eq!(ProbeStart::failed(held, &failed(0, Region::Below)), None);
        assert_eq!(
            ProbeStart::failed(held, &failed(86_400_000, Region::Above)),
            None
        );
        assert_eq!(ProbeStart::failed(None, &failed(0, Region::Above)), None);
        assert_eq!(ProbeStart::failed(held, &base), None);
    }
}
