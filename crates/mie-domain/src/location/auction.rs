//! The auction classifier `location.auction.prior_day@1` (ADR-044,
//! decisions 8 and 9).
//!
//! Every closed 1m bar with trades is classified against the prior day's
//! value area `[VAL, VAH)` (ADR-036). With the tolerance `w` of decision 3,
//! a close `c` lies in one of three regions or in an edge band:
//!
//! - **Above** `c > VAH + w`; **Below** `c < VAL − w`; **In**
//!   `VAL + w < c < VAH − w`;
//! - otherwise in an edge band ([`Position::LowEdge`], [`Position::HighEdge`]),
//!   neutral.
//!
//! The first close under a reference sets the **accepted** region `A`
//! (origin [`Origin::Start`]); a neutral close counts by the side of the
//! edge. A close in a region `R ≠ A` starts a **probe** toward `R`, or
//! retargets the active one. A probe counts the closes beyond the edge
//! itself (`c ≥ VAH` upward, `c < VAL` downward, `VAL ≤ c < VAH` toward In);
//! a close back in `A` fails it; other neutral closes neither count nor
//! fail. At [`ACCEPTANCE_CLOSES`] counted closes `A` becomes `R` (origin
//! [`Origin::Acceptance`]). A failure keeps its label until as many closes
//! count back toward `A`, or a new probe starts. Time, not volume: a close
//! is one minute.
//!
//! Everything is exact `i128` arithmetic on fixed point; nothing can fail.

use super::{AuctionState, within};
use crate::bars::{Bar, Coverage, Timeframe};
use crate::feature::{FeatureKey, FeatureValue, Unavailability, catalog};
use crate::fingerprint::Fingerprinter;
use crate::num::Price;
use crate::profile::VolumeProfile;
use crate::state_hash::StateEncode;
use crate::time::EventTime;
use std::fmt;

/// Counted closes that accept a probe, and closes back that retire a
/// failure: parameter `acceptance_closes` (decision 9).
pub const ACCEPTANCE_CLOSES: u32 = 60;

/// A region of value (decision 8).
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Region {
    /// Below value, beyond the VAL edge band.
    Below,
    /// Inside value, between the edge bands.
    In,
    /// Above value, beyond the VAH edge band.
    Above,
}

impl StateEncode for Region {
    /// `write_u8` in declaration order: Below 0, In 1, Above 2 (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::Below => 0,
            Self::In => 1,
            Self::Above => 2,
        });
    }
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Below => "below",
            Self::In => "in",
            Self::Above => "above",
        })
    }
}

/// Where a close lies relative to value: a region or an edge band
/// (decision 8).
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Position {
    /// Region Below.
    Below,
    /// Within the tolerance of VAL: neutral.
    LowEdge,
    /// Region In.
    In,
    /// Within the tolerance of VAH: neutral.
    HighEdge,
    /// Region Above.
    Above,
}

impl StateEncode for Position {
    /// `write_u8` in declaration order: Below 0, LowEdge 1, In 2, HighEdge
    /// 3, Above 4 (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::Below => 0,
            Self::LowEdge => 1,
            Self::In => 2,
            Self::HighEdge => 3,
            Self::Above => 4,
        });
    }
}

impl Position {
    /// The region, or `None` in an edge band.
    pub fn region(self) -> Option<Region> {
        match self {
            Self::Below => Some(Region::Below),
            Self::In => Some(Region::In),
            Self::Above => Some(Region::Above),
            Self::LowEdge | Self::HighEdge => None,
        }
    }
}

impl fmt::Display for Position {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Below => "below",
            Self::LowEdge => "val_edge",
            Self::In => "in",
            Self::HighEdge => "vah_edge",
            Self::Above => "above",
        })
    }
}

/// How the accepted region was set (decision 8).
///
/// The declaration order is frozen: it gives the state-hash codes
/// (ADR-041).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Origin {
    /// By the first close under the reference.
    Start,
    /// By a probe that reached [`ACCEPTANCE_CLOSES`] counted closes.
    Acceptance,
}

impl StateEncode for Origin {
    /// `write_u8` in declaration order: Start 0, Acceptance 1 (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::Start => 0,
            Self::Acceptance => 1,
        });
    }
}

/// An active probe toward a region other than the accepted one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Probe {
    /// The region probed.
    pub toward: Region,
    /// Time of the event that closed the probe's first bar.
    pub since: EventTime,
    /// Closes beyond the edge so far, the first included.
    pub closes: u32,
}

impl StateEncode for Probe {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            toward,
            since,
            closes,
        } = self;
        toward.encode(f);
        since.encode(f);
        closes.encode(f);
    }
}

/// A probe that failed: a close came back in the accepted region before
/// [`ACCEPTANCE_CLOSES`] counted closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Failure {
    /// The region the failed probe went toward.
    pub toward: Region,
    /// The probe's counted closes when it failed.
    pub closes: u32,
    /// Time of the event that closed the failing bar.
    pub at: EventTime,
    /// Closes back toward the accepted region since, the failing one
    /// included; the label retires at [`ACCEPTANCE_CLOSES`].
    pub closes_back: u32,
}

impl StateEncode for Failure {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            toward,
            closes,
            at,
            closes_back,
        } = self;
        toward.encode(f);
        closes.encode(f);
        at.encode(f);
        closes_back.encode(f);
    }
}

impl Failure {
    /// [`AuctionState::FailedBreakout`] for a probe out of value,
    /// [`AuctionState::FailedReclaim`] for one back into it.
    pub fn kind(&self) -> AuctionState {
        match self.toward {
            Region::Above | Region::Below => AuctionState::FailedBreakout,
            Region::In => AuctionState::FailedReclaim,
        }
    }
}

/// The auction state after a closed 1m bar: one
/// `location.auction.prior_day@1` value (decisions 8 and 9).
///
/// `Display` prints the canonical line the golden tests pin, ending with the
/// feature key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AuctionStatus {
    /// The feature that produced the value.
    pub feature: FeatureKey,
    /// Open of the reference: the prior UTC day.
    pub reference_start: EventTime,
    /// The reference's VAL (inclusive).
    pub val: Price,
    /// The reference's VAH (exclusive).
    pub vah: Price,
    /// The label (decision 8).
    pub state: AuctionState,
    /// The side of value the label refers to: the probe's or failure's
    /// region out of value, the accepted region outside value, the edge of
    /// an edge band; `None` inside value.
    pub side: Option<Region>,
    /// The accepted region.
    pub accepted: Region,
    /// How it was set.
    pub origin: Origin,
    /// Where the last close lies.
    pub position: Position,
    /// The active probe.
    pub probe: Option<Probe>,
    /// The failure whose label holds.
    pub failure: Option<Failure>,
    /// The last close.
    pub last_close: Price,
    /// End of its bar. A bar time, not a visibility time.
    pub bar_end: EventTime,
    /// Time of the event that closed it: the value's visibility time.
    pub known_at: EventTime,
    /// The reference's coverage OR that of every bar classified under it.
    pub coverage: Coverage,
}

impl StateEncode for AuctionStatus {
    fn encode(&self, f: &mut Fingerprinter) {
        let Self {
            feature,
            reference_start,
            val,
            vah,
            state,
            side,
            accepted,
            origin,
            position,
            probe,
            failure,
            last_close,
            bar_end,
            known_at,
            coverage,
        } = self;
        feature.encode(f);
        reference_start.encode(f);
        val.encode(f);
        vah.encode(f);
        state.encode(f);
        side.encode(f);
        accepted.encode(f);
        origin.encode(f);
        position.encode(f);
        probe.encode(f);
        failure.encode(f);
        last_close.encode(f);
        bar_end.encode(f);
        known_at.encode(f);
        coverage.encode(f);
    }
}

impl fmt::Display for AuctionStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} ref={} val={} vah={} close={} at={}",
            self.state, self.reference_start, self.val, self.vah, self.last_close, self.position
        )?;
        if let Some(side) = self.side {
            write!(f, " side={side}")?;
        }
        let origin = match self.origin {
            Origin::Start => "start",
            Origin::Acceptance => "accepted",
        };
        write!(f, " a={}/{origin}", self.accepted)?;
        if let Some(probe) = self.probe {
            write!(f, " probe={}:{}", probe.toward, probe.closes)?;
        }
        if let Some(failure) = self.failure {
            write!(
                f,
                " failed={}:{}/{}",
                failure.toward, failure.closes, failure.closes_back
            )?;
        }
        write!(f, " {} {}", self.coverage, self.feature)
    }
}

/// The reference value area of the classifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Reference {
    /// No prior day published yet.
    None,
    /// The prior day is unavailable, or its value area is too narrow.
    Invalid,
    /// A usable value area.
    Ready {
        start: EventTime,
        val: Price,
        vah: Price,
        coverage: Coverage,
    },
}

/// A classified close: the label of the close before it under the same
/// reference (`None` for the first), and the status after it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Classified {
    /// The previous label under this reference.
    pub from: Option<AuctionState>,
    /// The status after the close.
    pub status: AuctionStatus,
}

/// The auction classifier (decisions 8 and 9), with its tolerance and
/// acceptance time as parameters so the measurement tool can run the exact
/// logic at other values; the engine runs [`TOLERANCE_BPS`] and
/// [`ACCEPTANCE_CLOSES`].
///
/// [`TOLERANCE_BPS`]: super::TOLERANCE_BPS
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuctionClassifier {
    tolerance_bps: i64,
    acceptance_closes: u32,
    reference: Reference,
    /// The accepted region and its origin; `None` before the first close
    /// under the reference.
    accepted: Option<(Region, Origin)>,
    probe: Option<Probe>,
    failure: Option<Failure>,
    coverage: Coverage,
    last: Option<AuctionState>,
    status: FeatureValue<AuctionStatus>,
}

/// The value before the first close under a ready reference.
fn warming() -> FeatureValue<AuctionStatus> {
    FeatureValue::WarmingUp {
        observed: 0,
        required: 1,
    }
}

impl AuctionClassifier {
    /// A classifier without a reference, warming up.
    pub fn new(tolerance_bps: i64, acceptance_closes: u32) -> Self {
        Self {
            tolerance_bps,
            acceptance_closes,
            reference: Reference::None,
            accepted: None,
            probe: None,
            failure: None,
            coverage: Coverage::default(),
            last: None,
            status: warming(),
        }
    }

    /// The value after the last step.
    pub fn status(&self) -> &FeatureValue<AuctionStatus> {
        &self.status
    }

    /// Steps the classifier through the bars an event closed, in close
    /// order, with `prior_day` the committed prior-day profile after the
    /// event and `known_at` the event's time (decision 2): a 1m bar with
    /// trades is classified against the reference held — the old day's
    /// closes against the old day's reference — and calls `each`; a 1d bar
    /// then switches to `prior_day`. Empty bars neither count nor fail.
    pub fn step(
        &mut self,
        closed: &[Bar],
        prior_day: &FeatureValue<VolumeProfile>,
        known_at: EventTime,
        mut each: impl FnMut(&Classified),
    ) {
        for bar in closed {
            match bar.timeframe {
                Timeframe::M1 => {
                    if let Some(classified) = self.classify(bar, known_at) {
                        each(&classified);
                    }
                }
                Timeframe::D1 => self.reset(prior_day),
                Timeframe::M5 | Timeframe::M15 | Timeframe::H1 | Timeframe::H4 => {}
            }
        }
    }

    /// Switches to `prior_day` as the reference: every probe, failure and
    /// the accepted region reset (decision 8).
    fn reset(&mut self, prior_day: &FeatureValue<VolumeProfile>) {
        *self = Self::new(self.tolerance_bps, self.acceptance_closes);
        match prior_day {
            FeatureValue::Ready(profile) => {
                let (val, vah) = (profile.val, profile.vah);
                // Value must be wider than both edge bands: `VAH − VAL > 2w`.
                let width = i128::from(vah.units()) - i128::from(val.units());
                let bands = (i128::from(vah.units()).abs() + i128::from(val.units()).abs())
                    * i128::from(self.tolerance_bps);
                if width * 10_000 > bands {
                    self.reference = Reference::Ready {
                        start: profile.start,
                        val,
                        vah,
                        coverage: profile.coverage,
                    };
                    self.coverage = profile.coverage;
                } else {
                    self.invalid();
                }
            }
            FeatureValue::Unavailable { .. } => self.invalid(),
            FeatureValue::WarmingUp { .. } => {}
        }
    }

    fn invalid(&mut self) {
        self.reference = Reference::Invalid;
        self.status = FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        };
    }

    /// Where `close` lies relative to `[val, vah)`, exact (decision 3).
    fn position(&self, close: Price, val: Price, vah: Price) -> Position {
        let (c, low, high) = (
            i128::from(close.units()),
            i128::from(val.units()),
            i128::from(vah.units()),
        );
        let w = self.tolerance_bps;
        if !within(c - high, vah, w) {
            Position::Above
        } else if within(high - c, vah, w) {
            Position::HighEdge
        } else if !within(low - c, val, w) {
            Position::Below
        } else if within(c - low, val, w) {
            Position::LowEdge
        } else {
            Position::In
        }
    }

    /// Classifies the closed 1m bar `bar`; `None` without trades or a
    /// usable reference.
    fn classify(&mut self, bar: &Bar, known_at: EventTime) -> Option<Classified> {
        let ohlc = bar.ohlc?;
        let Reference::Ready {
            start, val, vah, ..
        } = self.reference
        else {
            return None;
        };
        let close = ohlc.close;
        let position = self.position(close, val, vah);
        // Beyond the edge itself, toward each region.
        let counts = |toward: Region| match toward {
            Region::Above => close >= vah,
            Region::Below => close < val,
            Region::In => val <= close && close < vah,
        };
        self.coverage.partial_start |= bar.coverage.partial_start;
        self.coverage.feed_gap |= bar.coverage.feed_gap;
        match self.accepted {
            None => {
                let side = if close >= vah {
                    Region::Above
                } else if close < val {
                    Region::Below
                } else {
                    Region::In
                };
                self.accepted = Some((side, Origin::Start));
            }
            Some((accepted, _)) => match (position.region(), self.probe) {
                // A close beyond value toward another region than the probe's
                // starts or retargets one; it counts.
                (Some(region), probe)
                    if region != accepted && probe.is_none_or(|probe| probe.toward != region) =>
                {
                    self.probe = Some(Probe {
                        toward: region,
                        since: known_at,
                        closes: 1,
                    });
                    self.failure = None;
                }
                // Back in the accepted region: the probe fails.
                (Some(region), Some(probe)) if region == accepted => {
                    self.probe = None;
                    self.failure = Some(Failure {
                        toward: probe.toward,
                        closes: probe.closes,
                        at: known_at,
                        closes_back: 1,
                    });
                }
                (_, Some(mut probe)) => {
                    if counts(probe.toward) {
                        probe.closes = probe.closes.saturating_add(1);
                    }
                    self.probe = Some(probe);
                }
                (_, None) => {
                    if let Some(failure) = &mut self.failure
                        && counts(accepted)
                    {
                        failure.closes_back = failure.closes_back.saturating_add(1);
                    }
                }
            },
        }
        if let Some(probe) = self.probe
            && probe.closes >= self.acceptance_closes
        {
            self.accepted = Some((probe.toward, Origin::Acceptance));
            self.probe = None;
            self.failure = None;
        }
        if self
            .failure
            .is_some_and(|failure| failure.closes_back >= self.acceptance_closes)
        {
            self.failure = None;
        }
        // Set above: the first close sets it, later ones keep it.
        let (accepted, origin) = self.accepted.unwrap_or((Region::In, Origin::Start));
        let edge = match position {
            Position::HighEdge => Some(Region::Above),
            Position::LowEdge => Some(Region::Below),
            Position::Below | Position::In | Position::Above => None,
        };
        let (state, side) = match (self.probe, self.failure) {
            (Some(probe), _) if probe.toward != Region::In => {
                (AuctionState::Breakout, Some(probe.toward))
            }
            (None, Some(failure)) => (
                failure.kind(),
                Some(match failure.toward {
                    Region::In => accepted,
                    out => out,
                }),
            ),
            (probe, _) if probe.is_some() || accepted == Region::In => match edge {
                Some(side) => (AuctionState::AtValueEdge, Some(side)),
                None => (AuctionState::InsideValue, None),
            },
            _ => match origin {
                Origin::Acceptance => (AuctionState::Acceptance, Some(accepted)),
                Origin::Start => (AuctionState::OutsideValue, Some(accepted)),
            },
        };
        let status = AuctionStatus {
            feature: catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key,
            reference_start: start,
            val,
            vah,
            state,
            side,
            accepted,
            origin,
            position,
            probe: self.probe,
            failure: self.failure,
            last_close: close,
            bar_end: bar.end(),
            known_at,
            coverage: self.coverage,
        };
        let classified = Classified {
            from: self.last,
            status,
        };
        self.last = Some(state);
        self.status = FeatureValue::Ready(status);
        Some(classified)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::samples::t;
    use crate::event::{Aggressor, MarketEvent, Trade};
    use crate::location::LocationEvent;
    use crate::location::tests::walk_tape;
    use crate::num::Qty;
    use crate::state::MarketStateEngine;

    const MINUTE: i64 = 60_000;
    const DAY: i64 = 86_400_000;

    fn price(text: &str) -> Price {
        text.parse().unwrap()
    }

    /// A prior-day profile with value `[val, vah)`.
    fn prior(val: &str, vah: &str) -> FeatureValue<VolumeProfile> {
        FeatureValue::Ready(VolumeProfile {
            feature: catalog::PROFILE_VOLUME_PRIOR_DAY_V1.key,
            start: t(0),
            end: t(DAY),
            sessions: 1,
            total_volume: Qty::from_units(100),
            low: price(val),
            high: price(vah),
            poc: price(val),
            poc_volume: Qty::from_units(100),
            val: price(val),
            vah: price(vah),
            value_area_volume: Qty::from_units(70),
            hvn: Vec::new(),
            lvn: Vec::new(),
            coverage: Coverage::default(),
        })
    }

    /// A closed 1m bar of day 1 at `minute` closing at `close`.
    fn minute(minute: i64, close: &str) -> Bar {
        let close = price(close);
        Bar {
            ohlc: Some(crate::bars::Ohlc {
                open: close,
                high: close,
                low: close,
                close,
            }),
            ..Bar::empty(Timeframe::M1, t(DAY + minute * MINUTE))
        }
    }

    /// A classifier on value `[60000, 61000)`: `w` is 30 at VAL and 30.5 at
    /// VAH, so In is `(60030, 60969.5)`, the edge bands `[59970, 60030]` and
    /// `[60969.5, 61030.5]`.
    struct Path {
        classifier: AuctionClassifier,
        minute: i64,
    }

    impl Path {
        fn new() -> Self {
            Self::on(&prior("60000", "61000"))
        }

        fn on(reference: &FeatureValue<VolumeProfile>) -> Self {
            let mut classifier = AuctionClassifier::new(5, ACCEPTANCE_CLOSES);
            classifier.reset(reference);
            Self {
                classifier,
                minute: 0,
            }
        }

        /// Closes the next minute at `at`; the status after it.
        fn close(&mut self, at: &str) -> AuctionStatus {
            let bar = minute(self.minute, at);
            let known_at = t(bar.end().as_millis() + 1_000);
            self.minute += 1;
            self.classifier.classify(&bar, known_at).unwrap().status
        }

        /// Closes `n` minutes at `at`; the last status.
        fn closes(&mut self, n: usize, at: &str) -> AuctionStatus {
            let mut status = None;
            for _ in 0..n {
                status = Some(self.close(at));
            }
            status.unwrap()
        }
    }

    #[test]
    fn inside_value() {
        let mut path = Path::new();
        let status = path.close("60500");
        assert_eq!(
            (status.state, status.side),
            (AuctionState::InsideValue, None)
        );
        assert_eq!(
            (status.accepted, status.origin),
            (Region::In, Origin::Start)
        );
        assert_eq!(status.position, Position::In);
        assert_eq!(
            status.to_string(),
            "inside_value ref=0ms val=60000.00000000 vah=61000.00000000 \
             close=60500.00000000 at=in a=in/start complete location.auction.prior_day@1"
        );
    }

    #[test]
    fn at_value_edge_is_neutral_up_to_the_tolerance() {
        let mut path = Path::new();
        path.close("60500");
        // Exactly VAH + w: neutral.
        let edge = path.close("61030.5");
        assert_eq!(edge.position, Position::HighEdge);
        assert_eq!(
            (edge.state, edge.side, edge.probe),
            (AuctionState::AtValueEdge, Some(Region::Above), None)
        );
        // One unit beyond: a probe.
        let beyond = path.close("61030.50000001");
        assert_eq!(beyond.state, AuctionState::Breakout);
        assert_eq!(beyond.probe.map(|probe| probe.closes), Some(1));
        // The low edge band, from inside.
        let mut path = Path::new();
        path.close("60500");
        let low = path.close("59970");
        assert_eq!(
            (low.state, low.side, low.position),
            (
                AuctionState::AtValueEdge,
                Some(Region::Below),
                Position::LowEdge
            )
        );
        assert_eq!(path.close("59969.99999999").state, AuctionState::Breakout);
    }

    #[test]
    fn outside_value_when_the_reference_starts_outside() {
        let mut path = Path::new();
        let status = path.close("61500");
        assert_eq!(
            (status.state, status.side, status.accepted, status.origin),
            (
                AuctionState::OutsideValue,
                Some(Region::Above),
                Region::Above,
                Origin::Start
            )
        );
        // A neutral first close counts by the side of the edge.
        let mut at_vah = Path::new();
        assert_eq!(at_vah.close("61000").state, AuctionState::OutsideValue);
        let mut below_vah = Path::new();
        assert_eq!(
            below_vah.close("60999.99999999").state,
            AuctionState::AtValueEdge
        );
        let mut below = Path::new();
        let status = below.close("59990");
        assert_eq!(
            (status.accepted, status.side),
            (Region::Below, Some(Region::Below))
        );
    }

    #[test]
    fn a_breakout_counts_closes_beyond_the_edge_itself() {
        let mut path = Path::new();
        path.close("60500");
        let started = path.close("61100");
        assert_eq!(
            (started.state, started.side),
            (AuctionState::Breakout, Some(Region::Above))
        );
        assert_eq!(started.probe.unwrap().since, t(DAY + 2 * MINUTE + 1_000));
        // A neutral close at or above VAH counts…
        assert_eq!(path.close("61000").probe.unwrap().closes, 2);
        // …one below VAH neither counts nor fails.
        let neutral = path.close("60990");
        assert_eq!(neutral.state, AuctionState::Breakout);
        assert_eq!(neutral.probe.unwrap().closes, 2);
        assert_eq!(path.close("61100").probe.unwrap().closes, 3);
    }

    #[test]
    fn failed_breakout_and_acceptance_meet_at_the_threshold() {
        // N − 1 counted closes, then a close back in value: failed.
        let mut path = Path::new();
        path.close("60500");
        let held = path.closes(59, "61100");
        assert_eq!(held.probe.unwrap().closes, 59);
        let failed = path.close("60500");
        assert_eq!(
            (failed.state, failed.side, failed.probe),
            (AuctionState::FailedBreakout, Some(Region::Above), None)
        );
        assert_eq!(
            failed.failure,
            Some(Failure {
                toward: Region::Above,
                closes: 59,
                at: t(DAY + 61 * MINUTE + 1_000),
                closes_back: 1,
            })
        );
        // Exactly N: accepted, and a close back in value is a new probe.
        let mut path = Path::new();
        path.close("60500");
        let accepted = path.closes(60, "61100");
        assert_eq!(
            (
                accepted.state,
                accepted.accepted,
                accepted.origin,
                accepted.probe
            ),
            (
                AuctionState::Acceptance,
                Region::Above,
                Origin::Acceptance,
                None
            )
        );
        let back = path.close("60500");
        assert_eq!(back.state, AuctionState::InsideValue);
        assert_eq!(back.probe.map(|probe| probe.toward), Some(Region::In));
        assert_eq!(back.accepted, Region::Above);
    }

    #[test]
    fn failed_reclaim() {
        let mut path = Path::new();
        path.close("61500");
        let reclaim = path.close("60500");
        assert_eq!(
            (reclaim.state, reclaim.probe.map(|probe| probe.toward)),
            (AuctionState::InsideValue, Some(Region::In))
        );
        // The probe toward In counts closes inside the edge itself.
        assert_eq!(path.close("60990").probe.unwrap().closes, 2);
        assert_eq!(path.close("61010").probe.unwrap().closes, 2);
        let failed = path.close("61500");
        assert_eq!(
            (failed.state, failed.side),
            (AuctionState::FailedReclaim, Some(Region::Above))
        );
        assert_eq!(failed.failure.unwrap().kind(), AuctionState::FailedReclaim);
        // A reclaim held for N closes is accepted inside value.
        let mut path = Path::new();
        path.close("61500");
        let inside = path.closes(60, "60500");
        assert_eq!(
            (inside.state, inside.accepted, inside.origin),
            (AuctionState::InsideValue, Region::In, Origin::Acceptance)
        );
    }

    #[test]
    fn a_probe_retargets_through_value() {
        let mut path = Path::new();
        path.close("60500");
        path.closes(3, "61100");
        let down = path.close("59900");
        assert_eq!(
            (down.state, down.side, down.failure),
            (AuctionState::Breakout, Some(Region::Below), None)
        );
        assert_eq!(down.probe.unwrap().closes, 1);
        // A new probe also ends a failure's label.
        let mut path = Path::new();
        path.close("60500");
        path.close("61100");
        assert_eq!(path.close("60500").state, AuctionState::FailedBreakout);
        let again = path.close("59900");
        assert_eq!((again.state, again.failure), (AuctionState::Breakout, None));
    }

    #[test]
    fn a_failure_label_retires_after_n_closes_back() {
        let mut path = Path::new();
        path.close("60500");
        path.close("61100");
        path.close("60500");
        // Neutral closes on the accepted side of the edge count back too.
        let held = path.closes(57, "60990");
        assert_eq!(held.state, AuctionState::FailedBreakout);
        assert_eq!(held.failure.unwrap().closes_back, 58);
        // Neutral closes beyond the edge do not.
        assert_eq!(path.close("61010").failure.unwrap().closes_back, 58);
        assert_eq!(path.close("60500").failure.unwrap().closes_back, 59);
        let retired = path.close("60500");
        assert_eq!(
            (retired.state, retired.failure),
            (AuctionState::InsideValue, None)
        );
    }

    #[test]
    fn a_new_reference_resets_a_probe_and_empty_bars_are_ignored() {
        let mut classifier = AuctionClassifier::new(5, ACCEPTANCE_CLOSES);
        let reference = prior("60000", "61000");
        let day = Bar::empty(Timeframe::D1, t(0));
        let mut seen = Vec::new();
        classifier.step(&[day], &reference, t(DAY + 1), |c| seen.push(*c));
        classifier.step(&[minute(0, "60500")], &reference, t(DAY + 61_000), |c| {
            seen.push(*c);
        });
        classifier.step(&[minute(1, "61100")], &reference, t(DAY + 121_000), |c| {
            seen.push(*c);
        });
        assert_eq!(
            seen.iter()
                .map(|c| (c.from, c.status.state))
                .collect::<Vec<_>>(),
            [
                (None, AuctionState::InsideValue),
                (Some(AuctionState::InsideValue), AuctionState::Breakout)
            ]
        );
        // An empty minute: no close, nothing changes.
        let before = classifier.clone();
        let empty = Bar::empty(Timeframe::M1, t(DAY + 2 * MINUTE));
        classifier.step(&[empty], &reference, t(DAY + 181_000), |_| {
            panic!("an empty bar is never classified")
        });
        assert_eq!(classifier, before);
        // The next day's reference: the probe is gone, the first close
        // starts over.
        let next = prior("62000", "63000");
        let closing = [minute(3, "61100"), Bar::empty(Timeframe::D1, t(DAY))];
        let mut seen = Vec::new();
        classifier.step(&closing, &next, t(2 * DAY + 1), |c| seen.push(*c));
        // The old day's close against the old reference, then the switch.
        assert_eq!(seen[0].status.probe.unwrap().closes, 2);
        assert_eq!(seen[0].status.reference_start, t(0));
        assert_eq!(*classifier.status(), warming());
        let mut seen = Vec::new();
        let first = Bar {
            open_time: t(2 * DAY),
            ..minute(0, "62500")
        };
        classifier.step(&[first], &next, t(2 * DAY + 61_000), |c| seen.push(*c));
        assert_eq!(seen[0].from, None);
        assert_eq!(
            (seen[0].status.state, seen[0].status.probe),
            (AuctionState::InsideValue, None)
        );
    }

    #[test]
    fn a_value_area_within_both_bands_is_unavailable() {
        let unavailable = FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        };
        // 60 ≤ 30 + 30.03: no In region.
        let narrow = Path::on(&prior("60000", "60060"));
        assert_eq!(*narrow.classifier.status(), unavailable);
        let mut narrow = narrow;
        assert!(
            narrow
                .classifier
                .classify(&minute(0, "60030"), t(DAY + 61_000))
                .is_none()
        );
        // 61 > 30 + 30.0305.
        let mut wide = Path::on(&prior("60000", "60061"));
        assert_eq!(wide.close("60030.5").position, Position::In);
        // An unavailable prior day; one still warming up.
        let empty = Path::on(&FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        });
        assert_eq!(*empty.classifier.status(), unavailable);
        let mut cold = Path::on(&FeatureValue::WarmingUp {
            observed: 0,
            required: 1,
        });
        assert_eq!(*cold.classifier.status(), warming());
        assert!(
            cold.classifier
                .classify(&minute(0, "60500"), t(DAY + 61_000))
                .is_none()
        );
    }

    /// A trade at `millis` and `price` (whole USDT).
    fn trade(millis: i64, trade_id: u64, at: i64) -> MarketEvent {
        MarketEvent::Trade(Trade {
            time: t(millis),
            trade_id,
            price: Price::from_units(at * crate::num::SCALE),
            qty: Qty::from_units(10_000_000),
            aggressor: Aggressor::Buy,
        })
    }

    #[test]
    fn the_engine_reaches_failure_and_acceptance_through_apply() {
        let mut engine = MarketStateEngine::new();
        let mut trade_id = 0;
        let mut next = |millis: i64, at: i64| {
            trade_id += 1;
            trade(millis, trade_id, at)
        };
        // Day 0: one trade a minute, sweeping 60 000 … 60 990.
        for m in 0..1_440 {
            engine
                .apply(&next(m * MINUTE + 1_000, 60_000 + (m % 100) * 10))
                .unwrap();
        }
        let mut path = vec![60_500; 10];
        path.extend([62_000; 5]);
        path.extend([60_500; 5]);
        path.extend([62_000; 61]);
        let mut transitions = Vec::new();
        let mut last = None;
        for (m, at) in (0..).zip(path) {
            engine.apply(&next(DAY + m * MINUTE + 1_000, at)).unwrap();
            for fact in engine.location_events() {
                if let LocationEvent::Auction {
                    from, to, known_at, ..
                } = fact
                {
                    assert_eq!(*from, last);
                    assert_eq!(*known_at, t(DAY + m * MINUTE + 1_000));
                    transitions.push((*from, *to));
                    last = Some(*to);
                }
            }
            match &engine.state().location.auction {
                FeatureValue::Ready(status) => assert_eq!(Some(status.state), last),
                other => assert_eq!(*other, warming(), "minute {m}"),
            }
        }
        let status = engine.state().location.auction.ready().unwrap();
        let prior = engine.state().profile.prior_day.ready().unwrap();
        assert_eq!((status.val, status.vah), (prior.val, prior.vah));
        assert_eq!(
            transitions,
            [
                (None, AuctionState::InsideValue),
                (Some(AuctionState::InsideValue), AuctionState::Breakout),
                (Some(AuctionState::Breakout), AuctionState::FailedBreakout),
                (Some(AuctionState::FailedBreakout), AuctionState::Breakout),
                (Some(AuctionState::Breakout), AuctionState::Acceptance),
            ]
        );
    }

    #[test]
    fn every_state_occurs_on_the_golden_tape() {
        let mut engine = MarketStateEngine::new();
        let mut seen = [0_u32; 7];
        for event in walk_tape(0x6d69_6500_0000_0044, 7) {
            engine.apply(&event).unwrap();
            if engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::M1 && bar.ohlc.is_some())
                && let FeatureValue::Ready(status) = &engine.state().location.auction
            {
                seen[AuctionState::ALL
                    .iter()
                    .position(|s| *s == status.state)
                    .unwrap()] += 1;
            }
        }
        assert!(seen.iter().all(|count| *count > 0), "{seen:?}");
    }

    #[test]
    fn golden_location_auction_prior_day_v1() {
        let mut engine = MarketStateEngine::new();
        let mut lines = Vec::new();
        let mut facts = Fingerprinter::new();
        let mut transitions = 0;
        for event in walk_tape(0x6d69_6500_0000_0044, 7) {
            engine.apply(&event).unwrap();
            for fact in engine.location_events() {
                if let LocationEvent::Auction { .. } = fact {
                    facts.write_str(&format!("{} {fact}", fact.time()));
                    transitions += 1;
                }
            }
            let closed = engine
                .closed_bars()
                .iter()
                .any(|bar| bar.timeframe == Timeframe::H1);
            if let (true, FeatureValue::Ready(status)) = (closed, &engine.state().location.auction)
                && event.time().as_millis() % (3 * 3_600_000) < 60_000
            {
                lines.push(format!("{} {status}", event.time()));
            }
        }
        lines.push(format!("transitions={transitions} {}", facts.finish()));
        assert_eq!(lines, GOLDEN_AUCTION);
    }

    const GOLDEN_AUCTION: [&str; 42] = [
        "97208387ms inside_value ref=0ms val=61610.00000000 vah=62040.00000000 close=61946.48000000 at=in a=in/accepted partial_start location.auction.prior_day@1",
        "108015517ms acceptance ref=0ms val=61610.00000000 vah=62040.00000000 close=61548.21000000 at=below side=below a=below/accepted partial_start location.auction.prior_day@1",
        "118812749ms acceptance ref=0ms val=61610.00000000 vah=62040.00000000 close=61109.13000000 at=below side=below a=below/accepted partial_start location.auction.prior_day@1",
        "129605687ms inside_value ref=0ms val=61610.00000000 vah=62040.00000000 close=61805.33000000 at=in a=in/accepted partial_start location.auction.prior_day@1",
        "140410678ms inside_value ref=0ms val=61610.00000000 vah=62040.00000000 close=61816.05000000 at=in a=below/accepted probe=in:26 partial_start location.auction.prior_day@1",
        "151231916ms at_value_edge ref=0ms val=61610.00000000 vah=62040.00000000 close=61584.30000000 at=val_edge side=below a=in/accepted partial_start location.auction.prior_day@1",
        "162047320ms acceptance ref=0ms val=61610.00000000 vah=62040.00000000 close=62259.46000000 at=above side=above a=above/accepted partial_start location.auction.prior_day@1",
        "183627795ms inside_value ref=86400000ms val=61450.00000000 vah=62040.00000000 close=61488.85000000 at=in a=in/start complete location.auction.prior_day@1",
        "194439682ms acceptance ref=86400000ms val=61450.00000000 vah=62040.00000000 close=62297.24000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "216021184ms inside_value ref=86400000ms val=61450.00000000 vah=62040.00000000 close=61646.12000000 at=in a=in/accepted feed_gap location.auction.prior_day@1",
        "226818478ms acceptance ref=86400000ms val=61450.00000000 vah=62040.00000000 close=62340.63000000 at=above side=above a=above/accepted feed_gap location.auction.prior_day@1",
        "237635334ms at_value_edge ref=86400000ms val=61450.00000000 vah=62040.00000000 close=62031.15000000 at=vah_edge side=above a=in/accepted feed_gap location.auction.prior_day@1",
        "248434807ms inside_value ref=86400000ms val=61450.00000000 vah=62040.00000000 close=61530.85000000 at=in a=in/accepted feed_gap location.auction.prior_day@1",
        "270010394ms acceptance ref=172800000ms val=61800.00000000 vah=62230.00000000 close=61749.93000000 at=below side=below a=below/accepted feed_gap location.auction.prior_day@1",
        "280800925ms breakout ref=172800000ms val=61800.00000000 vah=62230.00000000 close=61585.08000000 at=below side=below a=in/accepted probe=below:54 feed_gap location.auction.prior_day@1",
        "291613559ms inside_value ref=172800000ms val=61800.00000000 vah=62230.00000000 close=62125.39000000 at=in a=in/accepted feed_gap location.auction.prior_day@1",
        "302429455ms acceptance ref=172800000ms val=61800.00000000 vah=62230.00000000 close=61641.91000000 at=below side=below a=below/accepted feed_gap location.auction.prior_day@1",
        "313205237ms breakout ref=172800000ms val=61800.00000000 vah=62230.00000000 close=61616.90000000 at=below side=below a=in/accepted probe=below:23 feed_gap location.auction.prior_day@1",
        "324024477ms breakout ref=172800000ms val=61800.00000000 vah=62230.00000000 close=62339.87000000 at=above side=above a=in/accepted probe=above:56 feed_gap location.auction.prior_day@1",
        "334842741ms inside_value ref=172800000ms val=61800.00000000 vah=62230.00000000 close=61834.09000000 at=in a=below/accepted probe=in:14 feed_gap location.auction.prior_day@1",
        "356420672ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62585.16000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "367223179ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62367.24000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "378032091ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62262.84000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "388829346ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62816.00000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "399648570ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62480.24000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "410418754ms inside_value ref=259200000ms val=61560.00000000 vah=62020.00000000 close=61877.94000000 at=in a=above/accepted probe=in:15 complete location.auction.prior_day@1",
        "421216413ms acceptance ref=259200000ms val=61560.00000000 vah=62020.00000000 close=62288.45000000 at=above side=above a=above/accepted complete location.auction.prior_day@1",
        "442801824ms outside_value ref=345600000ms val=62070.00000000 vah=62610.00000000 close=61488.78000000 at=below side=below a=below/start complete location.auction.prior_day@1",
        "453605289ms inside_value ref=345600000ms val=62070.00000000 vah=62610.00000000 close=62133.85000000 at=in a=in/accepted complete location.auction.prior_day@1",
        "464444445ms acceptance ref=345600000ms val=62070.00000000 vah=62610.00000000 close=61780.84000000 at=below side=below a=below/accepted complete location.auction.prior_day@1",
        "475232978ms acceptance ref=345600000ms val=62070.00000000 vah=62610.00000000 close=61579.16000000 at=below side=below a=below/accepted complete location.auction.prior_day@1",
        "486004969ms inside_value ref=345600000ms val=62070.00000000 vah=62610.00000000 close=62175.37000000 at=in a=in/accepted complete location.auction.prior_day@1",
        "496845764ms acceptance ref=345600000ms val=62070.00000000 vah=62610.00000000 close=61826.93000000 at=below side=below a=below/accepted complete location.auction.prior_day@1",
        "507608576ms acceptance ref=345600000ms val=62070.00000000 vah=62610.00000000 close=61499.20000000 at=below side=below a=below/accepted complete location.auction.prior_day@1",
        "529207211ms inside_value ref=432000000ms val=61690.00000000 vah=62150.00000000 close=62006.19000000 at=in a=in/accepted complete location.auction.prior_day@1",
        "540003207ms breakout ref=432000000ms val=61690.00000000 vah=62150.00000000 close=61623.42000000 at=below side=below a=in/accepted probe=below:3 complete location.auction.prior_day@1",
        "550825728ms acceptance ref=432000000ms val=61690.00000000 vah=62150.00000000 close=62162.26000000 at=vah_edge side=above a=above/accepted complete location.auction.prior_day@1",
        "561611926ms acceptance ref=432000000ms val=61690.00000000 vah=62150.00000000 close=61608.87000000 at=below side=below a=below/accepted complete location.auction.prior_day@1",
        "572420013ms breakout ref=432000000ms val=61690.00000000 vah=62150.00000000 close=61480.85000000 at=below side=below a=in/accepted probe=below:32 complete location.auction.prior_day@1",
        "583211605ms failed_breakout ref=432000000ms val=61690.00000000 vah=62150.00000000 close=62093.80000000 at=in side=above a=in/accepted failed=above:55/25 complete location.auction.prior_day@1",
        "594000623ms acceptance ref=432000000ms val=61690.00000000 vah=62150.00000000 close=61690.44000000 at=val_edge side=below a=below/accepted complete location.auction.prior_day@1",
        "transitions=201 493672b2c1b67c3f",
    ];
}
