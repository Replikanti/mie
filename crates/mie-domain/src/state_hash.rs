//! The Market State hash: one stable identity for a [`MarketState`]
//! (ADR-041).
//!
//! The equivalence harness (#13, ADR-019) compares the state live ingestion
//! reached with the state a replay of the same events reaches, at regular
//! checkpoints. Both sides compare a [`StateHash`]: a FNV-1a 64
//! [`Fingerprint`] over an explicit byte encoding of every public field of
//! [`MarketState`], written with the [`Fingerprinter`] writers (ADR-029). The
//! engine's internal trackers (ATR window, flow, profile, structure,
//! derivatives, liquidity, location, candidate and trigger trackers) are not hashed: the public state is
//! the contract, and a hidden divergence shows up in it at a later
//! checkpoint. The order book is public state (`book.l2@1`, ADR-043), so
//! every level of it is hashed.
//!
//! **Encoding v1.** A header, `write_str("mie-market-state")` then
//! `write_u32(`[`STATE_HASH_ENCODING`]`)`, then the fields of
//! [`MarketState`] in declaration order: `feature_set`, `as_of`,
//! `last_trade_price`, `bars`, `motion`, `atr`, `regime`, `flow`, `profile`,
//! `structure`, `derivatives`, `book`, `location`, `trade_count`,
//! `candidates` (appended by ADR-051 D1), `triggers` (appended last by
//! ADR-053 D1). Every value is written by these rules:
//!
//! | Type | Encoding |
//! |---|---|
//! | `EventTime` | `write_i64` of the epoch ms |
//! | `Price`, `Qty`, `Rate` | `write_i64` of the `10^-8` units (ADR-027) |
//! | `u64` / `u32` / `bool` | `write_u64` / `write_u32` / `write_u8` (0, 1) |
//! | `f64` | `write_u64` of its bit pattern, so `-0.0` and `0.0` differ |
//! | `Option<T>` | `write_u8`: 0 for `None`; 1 for `Some`, then the value |
//! | `FeatureValue<T>` | `write_u8`: 0 `WarmingUp`, then `observed` and `required` (`u64`); 1 `Ready`, then the value; 2 `Unavailable`, then the reason (`InputInvalid` 0, `OutOfRange` 1) |
//! | arrays, `Vec`, slices | `write_len`, then the elements in stored order |
//! | `FeatureKey` | the ADR-029 encoding: `write_str` of the id, `write_u32` of the version |
//! | `FeatureSetVersion` | `write_u64` of the fingerprint value |
//! | enums | an explicit match, never a cast, with codes in declaration order: `Timeframe` M1 0, M5 1, M15 2, H1 3, H4 4, D1 5; `RegimeLabel` Low 0, Medium 1, High 2, Extreme 3; `Side` High 0, Low 1; `SweepOutcome` Pending 0, Sfp 1, Break 2; `book::Side` Bid 0, Ask 1; `Aggressor` Buy 0, Sell 1; `LevelKind` Poc 0, Vah 1, Val 2, Hvn 3, Lvn 4, StructuralHigh 5, StructuralLow 6, LiquidityCluster 7, PriorSweep 8, SfpRejectionZone 9, Vwap 10, ValidatedReference 11; `AuctionState` InsideValue 0, AtValueEdge 1, OutsideValue 2, Breakout 3, FailedBreakout 4, FailedReclaim 5, Acceptance 6; `LevelSide` High 0, Low 1, Bid 2, Ask 3; `Region` Below 0, In 1, Above 2; `Position` Below 0, LowEdge 1, In 2, HighEdge 3, Above 4; `Origin` Start 0, Acceptance 1; `SwingStep` Higher 0, Equal 1, Lower 2; `TriggerFamily` FailedAuction 0, Sfp 1, Absorption 2, Exhaustion 3, EffortVsResult 4, CvdDivergence 5, OiExpansionRejection 6, FailedReclaim 7, BreakoutAcceptance 8 |
//! | `OrderBook` | the chain (`write_u8`: `Straddle` 0, `Chained` 1, `Invalid` 2, then the id as `u64` unless invalid), the trusted window (two `Option<Price>`), then the bids and the asks, each best first as `write_len` followed by price and quantity per level |
//! | `Level` (in the book features) | price, then quantity |
//! | structs | every field, through exhaustive destructuring, in declaration order as of encoding v1 |
//!
//! Each type's encoder sits next to the type, so private fields stay
//! private, and destructures it without `..`: a new field does not compile
//! until it is encoded.
//!
//! **Versioning.** Two state hashes are comparable only when both their
//! [`STATE_HASH_ENCODING`] and their [`FeatureSetVersion`] are equal. A new
//! feature changes the feature-set version anyway (ADR-029), whether it is
//! a new [`MarketState`] field (a new family) or a field appended last to a
//! family's struct (`profile.volume.composite_7d@1` in
//! [`VolumeProfiles`]); it extends the encoder in the same change, without
//! an encoding bump, and every field encoded before it is written as
//! before (ADR-041 D2). Changing how an already-encoded field is written
//! (its writer, an enum code or the order) bumps [`STATE_HASH_ENCODING`].
//!
//! [`FeatureSetVersion`]: crate::feature::FeatureSetVersion
//! [`VolumeProfiles`]: crate::profile::VolumeProfiles

use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::state::MarketState;
use std::fmt;

/// The encoding version written into the header (module docs).
pub const STATE_HASH_ENCODING: u32 = 1;

/// The identity of a [`MarketState`]; displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateHash(Fingerprint);

impl StateHash {
    /// Wraps a fingerprint, such as one recorded in a journal.
    pub const fn from_fingerprint(fingerprint: Fingerprint) -> Self {
        Self(fingerprint)
    }

    /// The underlying fingerprint.
    pub const fn fingerprint(self) -> Fingerprint {
        self.0
    }
}

impl fmt::Display for StateHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// Writes a value of the state by the rules of the module docs.
pub(crate) trait StateEncode {
    /// Writes `self` into `f`.
    fn encode(&self, f: &mut Fingerprinter);
}

impl MarketState {
    /// The hash of this state (encoding v1, module docs).
    pub fn state_hash(&self) -> StateHash {
        let mut f = header();
        let MarketState {
            feature_set,
            as_of,
            last_trade_price,
            bars,
            motion,
            atr,
            regime,
            flow,
            profile,
            structure,
            derivatives,
            book,
            location,
            trade_count,
            candidates,
            triggers,
        } = self;
        feature_set.encode(&mut f);
        as_of.encode(&mut f);
        last_trade_price.encode(&mut f);
        bars.encode(&mut f);
        motion.encode(&mut f);
        atr.encode(&mut f);
        regime.encode(&mut f);
        flow.encode(&mut f);
        profile.encode(&mut f);
        structure.encode(&mut f);
        derivatives.encode(&mut f);
        book.encode(&mut f);
        location.encode(&mut f);
        trade_count.encode(&mut f);
        candidates.encode(&mut f);
        triggers.encode(&mut f);
        StateHash(f.finish())
    }
}

/// A fingerprinter with the header written.
fn header() -> Fingerprinter {
    let mut f = Fingerprinter::new();
    f.write_str("mie-market-state");
    f.write_u32(STATE_HASH_ENCODING);
    f
}

impl StateEncode for u64 {
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u64(*self);
    }
}

impl StateEncode for u32 {
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u32(*self);
    }
}

impl StateEncode for bool {
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(u8::from(*self));
    }
}

impl StateEncode for f64 {
    /// The bit pattern: exact, and `-0.0` differs from `0.0`.
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u64(self.to_bits());
    }
}

impl<T: StateEncode> StateEncode for Option<T> {
    fn encode(&self, f: &mut Fingerprinter) {
        match self {
            None => f.write_u8(0),
            Some(value) => {
                f.write_u8(1);
                value.encode(f);
            }
        }
    }
}

impl<T: StateEncode> StateEncode for [T] {
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_len(self.len());
        for value in self {
            value.encode(f);
        }
    }
}

impl<T: StateEncode, const N: usize> StateEncode for [T; N] {
    fn encode(&self, f: &mut Fingerprinter) {
        self.as_slice().encode(f);
    }
}

impl<T: StateEncode> StateEncode for Vec<T> {
    fn encode(&self, f: &mut Fingerprinter) {
        self.as_slice().encode(f);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::book::{OrderBook, Side};
    use crate::candidate::divergence::{
        CvdComparison, Divergence, OiComparison, SwingAnchor, SwingPair,
    };
    use crate::derivatives::{LiquidationWindows, MarkState, OiSample, OiStep, SettledFunding};
    use crate::event::samples::{one_of_each, trade};
    use crate::event::{BookSnapshot, BookUpdate, Level, MarketEvent};
    use crate::feature::{
        FeatureKey, FeatureSet, FeatureSetVersion, FeatureValue, Unavailability, catalog,
    };
    use crate::flow::Cvd;
    use crate::num::{Price, Qty};
    use crate::profile::{ProfileNode, VolumeProfile, VolumeProfiles};
    use crate::regime::{AtrPercentile, Regime};
    use crate::state::MarketStateEngine;
    use crate::time::EventTime;

    fn engine_after(events: &[MarketEvent]) -> MarketStateEngine {
        let mut engine = MarketStateEngine::new();
        for event in events {
            engine.apply(event).unwrap_or_else(|e| panic!("{e}"));
        }
        engine
    }

    /// One event of every kind, then trades that close a 1m and a 5m bar.
    fn tape() -> Vec<MarketEvent> {
        let mut events = one_of_each(1_000);
        events.push(trade(61_000, 8));
        events.push(trade(301_000, 9));
        events
    }

    /// A named change to one field of a state.
    type Edit<'a> = Box<dyn Fn(&mut MarketState) + 'a>;

    fn encoded(value: &impl StateEncode) -> Fingerprint {
        let mut f = Fingerprinter::new();
        value.encode(&mut f);
        f.finish()
    }

    #[test]
    fn the_golden_values_of_encoding_v1_are_pinned() {
        // Both values change with every feature-set change, a feature
        // appended to a family's struct included (update them in that
        // change), and otherwise only with an encoding bump (module docs).
        assert_eq!(
            MarketStateEngine::new().state().state_hash().to_string(),
            "eb34096e448e1a60"
        );
        assert_eq!(
            engine_after(&tape()).state().state_hash().to_string(),
            "a7173b1696ab434a"
        );
    }

    /// The hash `state` has without `profile.composite_7d` and
    /// `candidates`: every other field written exactly as
    /// [`MarketState::state_hash`] writes it. This is encoding v1 as it
    /// stood before #96 appended the composite.
    fn hash_without_composite_7d(state: &MarketState) -> StateHash {
        let mut f = header();
        let MarketState {
            feature_set,
            as_of,
            last_trade_price,
            bars,
            motion,
            atr,
            regime,
            flow,
            profile,
            structure,
            derivatives,
            book,
            location,
            trade_count,
            candidates: _,
            triggers: _,
        } = state;
        let VolumeProfiles {
            utc_day,
            prior_day,
            composite_5d,
            composite_7d: _,
        } = profile;
        feature_set.encode(&mut f);
        as_of.encode(&mut f);
        last_trade_price.encode(&mut f);
        bars.encode(&mut f);
        motion.encode(&mut f);
        atr.encode(&mut f);
        regime.encode(&mut f);
        flow.encode(&mut f);
        utc_day.encode(&mut f);
        prior_day.encode(&mut f);
        composite_5d.encode(&mut f);
        structure.encode(&mut f);
        derivatives.encode(&mut f);
        book.encode(&mut f);
        location.encode(&mut f);
        trade_count.encode(&mut f);
        StateHash(f.finish())
    }

    #[test]
    fn appending_composite_7d_leaves_the_pre_96_hashes_intact() {
        // The default set before #96: `location.levels@1` and no
        // `profile.volume.composite_7d@1` (nor the candidates of #22 or the
        // bias and triggers of #24). It
        // still builds, to the version the pre-#96 pins recorded.
        let keys: Vec<FeatureKey> = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key])
            .unwrap()
            .definitions()
            .map(|definition| definition.key)
            .filter(|key| *key != catalog::PROFILE_VOLUME_COMPOSITE_7D_V1.key)
            .filter(|key| !is_candidate(*key) && !is_bias_or_trigger(*key))
            .collect();
        let pre_96 = FeatureSet::new(&catalog::registry(), &keys).unwrap();
        assert_eq!(pre_96.version().to_string(), "be668af626d0ce5a");
        // The `location.levels@1` engine reaches the pre-#96 state on every
        // other field: under the pre-#96 stamp and without the appended
        // field, it gives the two encoding-v1 goldens pinned before #96.
        let features = catalog::current_set_with(&[catalog::LOCATION_LEVELS_V1.key]).unwrap();
        for (events, golden) in [
            (Vec::new(), "cc347065b37129d0"),
            (tape(), "a4468a96437546b7"),
        ] {
            let mut engine = MarketStateEngine::with_features(features.clone()).unwrap();
            for event in &events {
                engine.apply(event).unwrap();
            }
            let mut state = engine.state().clone();
            state.feature_set = pre_96.version();
            assert_eq!(hash_without_composite_7d(&state).to_string(), golden);
            // The appended field is written: the full hash differs.
            assert_ne!(state.state_hash(), hash_without_composite_7d(&state));
        }
    }

    /// Whether `key` is an order-flow candidate (#22, ADR-051).
    fn is_candidate(key: FeatureKey) -> bool {
        key.id.as_str().starts_with("candidate.")
    }

    /// Whether `key` is the bias or a trigger (#24, ADR-053).
    fn is_bias_or_trigger(key: FeatureKey) -> bool {
        let id = key.id.as_str();
        id.starts_with("bias.") || id.starts_with("trigger.")
    }

    /// The hash `state` has without `candidates`: every other field written
    /// exactly as [`MarketState::state_hash`] writes it. This is encoding v1
    /// as it stood before #22 appended the field.
    fn hash_without_candidates(state: &MarketState) -> StateHash {
        let mut f = header();
        let MarketState {
            feature_set,
            as_of,
            last_trade_price,
            bars,
            motion,
            atr,
            regime,
            flow,
            profile,
            structure,
            derivatives,
            book,
            location,
            trade_count,
            candidates: _,
            triggers: _,
        } = state;
        feature_set.encode(&mut f);
        as_of.encode(&mut f);
        last_trade_price.encode(&mut f);
        bars.encode(&mut f);
        motion.encode(&mut f);
        atr.encode(&mut f);
        regime.encode(&mut f);
        flow.encode(&mut f);
        profile.encode(&mut f);
        structure.encode(&mut f);
        derivatives.encode(&mut f);
        book.encode(&mut f);
        location.encode(&mut f);
        trade_count.encode(&mut f);
        StateHash(f.finish())
    }

    #[test]
    fn appending_candidates_leaves_the_pre_22_hashes_intact() {
        // ADR-041 D2, ADR-051 D1: the set before #22 is CURRENT without the
        // candidate keys (nor the bias and triggers of #24, built on them).
        // It still builds, to the version pinned before #22.
        let keys: Vec<FeatureKey> = catalog::CURRENT
            .iter()
            .copied()
            .filter(|key| !is_candidate(*key) && !is_bias_or_trigger(*key))
            .collect();
        assert_eq!(keys.len() + 15 + 8, catalog::CURRENT.len());
        let pre_22 = FeatureSet::new(&catalog::registry(), &keys).unwrap();
        assert_eq!(pre_22.version().to_string(), "685b83d7bf13b54c");
        // The engine reaches the pre-#22 state on every other field: under
        // the pre-#22 stamp and without the appended field, it gives the
        // two encoding-v1 goldens pinned before #22.
        for (events, golden) in [
            (Vec::new(), "212f9b3749af5471"),
            (tape(), "2e1f872d7f95c19f"),
        ] {
            let mut state = engine_after(&events).state().clone();
            state.feature_set = pre_22.version();
            assert_eq!(hash_without_candidates(&state).to_string(), golden);
            // The appended field is written: the full hash differs.
            assert_ne!(state.state_hash(), hash_without_candidates(&state));
        }
    }

    /// The hash `state` has without `triggers`: every other field written
    /// exactly as [`MarketState::state_hash`] writes it. This is encoding v1
    /// as it stood before #24 appended the field.
    fn hash_without_triggers(state: &MarketState) -> StateHash {
        let mut f = header();
        let MarketState {
            feature_set,
            as_of,
            last_trade_price,
            bars,
            motion,
            atr,
            regime,
            flow,
            profile,
            structure,
            derivatives,
            book,
            location,
            trade_count,
            candidates,
            triggers: _,
        } = state;
        feature_set.encode(&mut f);
        as_of.encode(&mut f);
        last_trade_price.encode(&mut f);
        bars.encode(&mut f);
        motion.encode(&mut f);
        atr.encode(&mut f);
        regime.encode(&mut f);
        flow.encode(&mut f);
        profile.encode(&mut f);
        structure.encode(&mut f);
        derivatives.encode(&mut f);
        book.encode(&mut f);
        location.encode(&mut f);
        trade_count.encode(&mut f);
        candidates.encode(&mut f);
        StateHash(f.finish())
    }

    #[test]
    fn appending_triggers_leaves_the_pre_24_hashes_intact() {
        // ADR-041 D2, ADR-053 D1: the set before #24 is CURRENT without the
        // bias and trigger keys. It still builds, to the version pinned
        // before #24.
        let keys: Vec<FeatureKey> = catalog::CURRENT
            .iter()
            .copied()
            .filter(|key| !is_bias_or_trigger(*key))
            .collect();
        assert_eq!(keys.len() + 8, catalog::CURRENT.len());
        let pre_24 = FeatureSet::new(&catalog::registry(), &keys).unwrap();
        assert_eq!(pre_24.version().to_string(), "8e6f1b978d12a884");
        // The engine reaches the pre-#24 state on every other field: under
        // the pre-#24 stamp and without the appended field, it gives the
        // two encoding-v1 goldens pinned before #24.
        for (events, golden) in [
            (Vec::new(), "cb45d06de49e3e6d"),
            (tape(), "4df7c63b5cecaeb7"),
        ] {
            let mut state = engine_after(&events).state().clone();
            state.feature_set = pre_24.version();
            assert_eq!(hash_without_triggers(&state).to_string(), golden);
            // The appended field is written: the full hash differs.
            assert_ne!(state.state_hash(), hash_without_triggers(&state));
        }
    }

    #[test]
    fn the_header_is_written_through_the_fingerprinter() {
        let mut expected = Fingerprinter::new();
        expected.write_str("mie-market-state");
        expected.write_u32(1);
        assert_eq!(header().finish(), expected.finish());
    }

    #[test]
    fn every_public_field_counts() {
        let engine = engine_after(&tape());
        let base = engine.state().clone();
        let reference = base.state_hash();
        let m1 = base.bars.get(crate::bars::Timeframe::M1).unwrap();
        assert!(m1.last_closed().ready().is_some());

        // The same bars with another close price of the 5m bar's last trade.
        let mut other_tape = tape();
        let MarketEvent::Trade(mut t) = other_tape[other_tape.len() - 2].clone() else {
            unreachable!()
        };
        t.price = Price::from_units(t.price.units() + 1);
        let last = other_tape.len() - 2;
        other_tape[last] = MarketEvent::Trade(t);
        let other_bars = engine_after(&other_tape).state().bars;
        assert_ne!(other_bars, base.bars);

        let feature = FeatureKey::new("flow.cvd.continuous", 1);
        let cvd = |units: i64| Cvd {
            feature,
            cvd: Qty::from_units(units),
            anchor: EventTime::from_millis(1_000),
            gaps: 0,
        };
        let regime = |percentile: f64| {
            let atr_percentile = AtrPercentile::new(percentile).unwrap();
            FeatureValue::Ready(Regime {
                atr_percentile,
                label: atr_percentile.label(),
                feature: FeatureKey::new("volatility.regime.1h", 1),
            })
        };
        let node = |volume: i64| ProfileNode {
            price: Price::from_units(5),
            low: Price::from_units(0),
            high: Price::from_units(10),
            volume: Qty::from_units(volume),
            prominence_permille: 1_000,
        };
        let profile = |volume: i64| VolumeProfile {
            feature: FeatureKey::new("profile.volume.utc_day", 1),
            start: EventTime::from_millis(0),
            end: EventTime::from_millis(60_000),
            sessions: 1,
            total_volume: Qty::from_units(100),
            low: Price::from_units(0),
            high: Price::from_units(10),
            poc: Price::from_units(5),
            poc_volume: Qty::from_units(100),
            val: Price::from_units(0),
            vah: Price::from_units(10),
            value_area_volume: Qty::from_units(100),
            hvn: vec![node(volume)],
            lvn: Vec::new(),
            coverage: crate::bars::Coverage::default(),
        };

        let mut edits: Vec<(&str, Edit<'_>)> = vec![
            (
                "feature_set",
                Box::new(|s| {
                    s.feature_set = FeatureSetVersion::from_fingerprint(Fingerprint::from_raw(1));
                }),
            ),
            (
                "as_of",
                Box::new(|s| s.as_of = Some(EventTime::from_millis(301_001))),
            ),
            (
                "last_trade_price",
                Box::new(|s| {
                    s.last_trade_price = FeatureValue::WarmingUp {
                        observed: 0,
                        required: 1,
                    };
                }),
            ),
            ("trade_count", Box::new(|s| s.trade_count += 1)),
            ("bars", Box::new(move |s| s.bars = other_bars)),
            (
                "flow",
                Box::new(move |s| s.flow.cvd = FeatureValue::Ready(cvd(1))),
            ),
            (
                "flow value",
                Box::new(move |s| s.flow.cvd = FeatureValue::Ready(cvd(2))),
            ),
            (
                "profile",
                Box::new(move |s| s.profile.utc_day = FeatureValue::Ready(profile(100))),
            ),
            (
                "profile node",
                Box::new(move |s| s.profile.utc_day = FeatureValue::Ready(profile(99))),
            ),
            (
                "profile composite_7d",
                Box::new(move |s| s.profile.composite_7d = FeatureValue::Ready(profile(100))),
            ),
            ("regime", Box::new(move |s| s.regime = regime(50.0))),
            (
                "regime percentile",
                Box::new(move |s| s.regime = regime(50.5)),
            ),
            ("regime zero", Box::new(move |s| s.regime = regime(0.0))),
            (
                "regime minus zero",
                Box::new(move |s| s.regime = regime(-0.0)),
            ),
            (
                "derivatives oi",
                Box::new(|s| {
                    s.derivatives.oi = s.derivatives.oi.map(|sample| OiSample {
                        resolution_ms: sample.resolution_ms + 1,
                        ..sample
                    });
                }),
            ),
            (
                "derivatives oi step",
                Box::new(|s| {
                    s.derivatives.oi = s.derivatives.oi.map(|sample| OiSample {
                        step: Some(OiStep {
                            previous_time: EventTime::from_millis(0),
                            delta: Qty::from_units(0),
                            elapsed_ms: 1_000,
                        }),
                        ..sample
                    });
                }),
            ),
            (
                "derivatives oi grid",
                Box::new(|s| {
                    s.derivatives.oi_5m = FeatureValue::Unavailable {
                        reason: Unavailability::InputInvalid,
                    };
                }),
            ),
            (
                "derivatives mark",
                Box::new(|s| {
                    s.derivatives.mark = s.derivatives.mark.map(|mark| MarkState {
                        index_price: Price::from_units(mark.index_price.units() + 1),
                        ..mark
                    });
                }),
            ),
            (
                "derivatives funding",
                Box::new(|s| {
                    s.derivatives.funding_settled =
                        s.derivatives.funding_settled.map(|funding| SettledFunding {
                            time: EventTime::from_millis(funding.time.as_millis() + 1),
                            ..funding
                        });
                }),
            ),
            (
                "derivatives liquidations",
                Box::new(|s| s.derivatives.liquidations = LiquidationWindows::new()),
            ),
        ];
        // The tape's update misses the snapshot's straddle, so the book ends
        // invalid. Books that differ in one level's quantity, the chain or a
        // window bound only.
        assert_eq!(
            base.book.l2,
            FeatureValue::Unavailable {
                reason: Unavailability::InputInvalid
            }
        );
        let books = book_variants();
        for (name, book) in books {
            edits.push((
                name,
                Box::new(move |s| s.book.l2 = FeatureValue::Ready(book.clone())),
            ));
        }
        edits.push((
            "book depth",
            Box::new(|s| {
                s.book.depth = FeatureValue::Unavailable {
                    reason: Unavailability::OutOfRange,
                };
            }),
        ));
        edits.push((
            "book clusters",
            Box::new(|s| {
                s.book.clusters = FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1,
                };
            }),
        ));
        assert!(base.location.vwap.is_ready());
        edits.push((
            "location vwap",
            Box::new(|s| {
                s.location.vwap = FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1,
                };
            }),
        ));
        assert!(base.location.levels.is_ready());
        edits.push((
            "location levels",
            Box::new(|s| {
                s.location.levels = FeatureValue::WarmingUp {
                    observed: 0,
                    required: 1,
                };
            }),
        ));
        // The tape closes no day: the auction state is still warming up.
        assert!(!base.location.auction.is_ready());
        edits.push((
            "location auction",
            Box::new(|s| {
                s.location.auction = FeatureValue::Unavailable {
                    reason: Unavailability::InputInvalid,
                };
            }),
        ));
        // The tape gives every derivatives value but the grid (no sample at
        // or before a boundary yet) something to change.
        assert!(base.derivatives.oi.is_ready());
        assert!(base.derivatives.mark.is_ready());
        assert!(base.derivatives.funding_settled.is_ready());
        assert_ne!(base.derivatives.liquidations, LiquidationWindows::new());
        // The tape confirms no swing: every candidate is still warming up.
        assert_eq!(base.candidates, crate::candidate::Candidates::new());
        let comparisons = candidate_comparisons();
        for (name, timeframe, cvd, oi) in comparisons {
            edits.push((
                name,
                Box::new(move |s| {
                    let divergences = s.candidates.divergences_mut(timeframe).unwrap();
                    if let Some(cvd) = cvd {
                        divergences.cvd = cvd;
                    }
                    if let Some(oi) = oi {
                        divergences.oi = oi;
                    }
                }),
            ));
        }
        edits.extend(flow_candidate_edits());
        // The tape closes no 1h bar, confirms no swing and closes no day:
        // the bias and every trigger value are as they start.
        assert_eq!(base.triggers, crate::trigger::Triggers::new());
        edits.extend(trigger_edits());
        let mut seen = vec![("base", reference)];
        for (name, edit) in &edits {
            let mut state = base.clone();
            edit(&mut state);
            let hash = state.state_hash();
            if let Some((other, _)) = seen.iter().find(|(_, h)| *h == hash) {
                panic!("{name} hashes like {other}");
            }
            seen.push((name, hash));
        }
    }

    /// One candidate value per edit, each differing from the others in one
    /// respect only: the family, the timeframe, readiness, a side's
    /// comparison, or one field of that comparison.
    #[allow(clippy::type_complexity)]
    fn candidate_comparisons() -> Vec<(
        &'static str,
        crate::bars::Timeframe,
        Option<FeatureValue<Divergence<CvdComparison>>>,
        Option<FeatureValue<Divergence<OiComparison>>>,
    )> {
        use crate::bars::{Coverage, Timeframe};
        use crate::structure::Side;
        let anchor = |price: i64| SwingAnchor {
            swing_time: EventTime::from_millis(0),
            known_at: EventTime::from_millis(1),
            price: Price::from_units(price),
            time: EventTime::from_millis(2),
            cvd: Qty::from_units(3),
            gaps: 0,
            coverage: Coverage::default(),
        };
        let pair = |price: i64, span_ms: u64| SwingPair {
            side: Side::High,
            previous: anchor(1),
            current: anchor(price),
            price_change: Price::from_units(price - 1),
            cvd_change: Qty::from_units(0),
            gaps_between: 0,
            span_ms,
            coverage: Coverage::default(),
        };
        let cvd = |high: Option<CvdComparison>, low: Option<CvdComparison>| {
            Some(FeatureValue::Ready(Divergence {
                feature: catalog::CANDIDATE_DIVERGENCE_CVD_15M_V1.key,
                high,
                low,
            }))
        };
        let compared = |price: i64, span_ms: u64, divergent: bool| CvdComparison {
            pair: pair(price, span_ms),
            divergent,
        };
        let base_oi = OiComparison {
            pair: pair(2, 0),
            previous_oi: None,
            current_oi: None,
            oi_change: None,
            divergent: false,
        };
        let oi_of = |comparison: OiComparison| {
            Some(FeatureValue::Ready(Divergence {
                feature: catalog::CANDIDATE_DIVERGENCE_OI_15M_V1.key,
                high: Some(comparison),
                low: None,
            }))
        };
        let oi = oi_of(base_oi);
        let point = crate::derivatives::OiGridPoint {
            feature: catalog::DERIVATIVES_OI_5M_V1.key,
            boundary: EventTime::from_millis(0),
            open_interest: Qty::from_units(5),
            sample_time: EventTime::from_millis(0),
            resolution_ms: 300_000,
            delta: None,
        };
        let with_pair = |edit: fn(&mut SwingPair)| {
            let mut comparison = compared(2, 0, false);
            edit(&mut comparison.pair);
            cvd(Some(comparison), None)
        };
        let mut edits = vec![
            (
                "candidates oi previous point",
                Timeframe::M15,
                None,
                oi_of(OiComparison {
                    previous_oi: Some(point),
                    ..base_oi
                }),
            ),
            (
                "candidates oi current point",
                Timeframe::M15,
                None,
                oi_of(OiComparison {
                    current_oi: Some(point),
                    ..base_oi
                }),
            ),
            (
                "candidates oi change",
                Timeframe::M15,
                None,
                oi_of(OiComparison {
                    oi_change: Some(Qty::from_units(0)),
                    ..base_oi
                }),
            ),
            (
                "candidates oi divergent",
                Timeframe::M15,
                None,
                oi_of(OiComparison {
                    divergent: true,
                    ..base_oi
                }),
            ),
            (
                "candidates anchor cvd",
                Timeframe::M15,
                with_pair(|pair| pair.current.cvd = Qty::from_units(4)),
                None,
            ),
            (
                "candidates anchor coverage",
                Timeframe::M15,
                with_pair(|pair| pair.current.coverage.feed_gap = true),
                None,
            ),
            (
                "candidates pair coverage",
                Timeframe::M15,
                with_pair(|pair| pair.coverage.partial_start = true),
                None,
            ),
            (
                "candidates gaps between",
                Timeframe::M15,
                with_pair(|pair| pair.gaps_between = 1),
                None,
            ),
        ];
        edits.extend(vec![
            (
                "candidates cvd ready",
                Timeframe::M15,
                cvd(None, None),
                None,
            ),
            ("candidates cvd 1h", Timeframe::H1, cvd(None, None), None),
            ("candidates oi", Timeframe::M15, None, oi),
            (
                "candidates cvd high",
                Timeframe::M15,
                cvd(Some(compared(2, 0, false)), None),
                None,
            ),
            (
                "candidates cvd low",
                Timeframe::M15,
                cvd(None, Some(compared(2, 0, false))),
                None,
            ),
            (
                "candidates cvd price",
                Timeframe::M15,
                cvd(Some(compared(3, 0, false)), None),
                None,
            ),
            (
                "candidates cvd span",
                Timeframe::M15,
                cvd(Some(compared(2, 1, false)), None),
                None,
            ),
            (
                "candidates cvd divergent",
                Timeframe::M15,
                cvd(Some(compared(2, 0, true)), None),
                None,
            ),
        ]);
        edits
    }

    /// One effort-vs-result, exhaustion or absorption value per edit
    /// (ADR-051 D5–D7), each differing from the others in one respect only:
    /// the family, the window, readiness, or one field of the value.
    fn flow_candidate_edits() -> Vec<(&'static str, Edit<'static>)> {
        use crate::bars::{Coverage, Timeframe};
        use crate::candidate::absorption::Absorption;
        use crate::candidate::effort::{EffortResult, Exhaustion};
        use crate::event::Aggressor;
        use crate::num::Rate;
        let result = EffortResult {
            feature: catalog::CANDIDATE_EFFORT_RESULT_5M_V1.key,
            timeframe: Timeframe::M5,
            end: EventTime::from_millis(300_000),
            aggressor: Aggressor::Buy,
            effort: Qty::from_units(10),
            progress: Price::from_units(1),
            effort_high: Qty::from_units(8),
            effort_median: Qty::from_units(4),
            move_high: Price::from_units(6),
            move_median: Price::from_units(2),
            coverage: Coverage::default(),
            active: true,
        };
        let efforts = [
            ("candidates effort ready", result),
            (
                "candidates effort end",
                EffortResult {
                    end: EventTime::from_millis(360_000),
                    ..result
                },
            ),
            (
                "candidates effort aggressor",
                EffortResult {
                    aggressor: Aggressor::Sell,
                    ..result
                },
            ),
            (
                "candidates effort effort",
                EffortResult {
                    effort: Qty::from_units(11),
                    ..result
                },
            ),
            (
                "candidates effort progress",
                EffortResult {
                    progress: Price::from_units(0),
                    ..result
                },
            ),
            (
                "candidates effort high",
                EffortResult {
                    effort_high: Qty::from_units(9),
                    ..result
                },
            ),
            (
                "candidates effort median",
                EffortResult {
                    effort_median: Qty::from_units(5),
                    ..result
                },
            ),
            (
                "candidates move high",
                EffortResult {
                    move_high: Price::from_units(7),
                    ..result
                },
            ),
            (
                "candidates move median",
                EffortResult {
                    move_median: Price::from_units(3),
                    ..result
                },
            ),
            (
                "candidates effort coverage",
                EffortResult {
                    coverage: Coverage {
                        partial_start: false,
                        feed_gap: true,
                    },
                    ..result
                },
            ),
            (
                "candidates effort active",
                EffortResult {
                    active: false,
                    ..result
                },
            ),
            (
                "candidates effort timeframe",
                EffortResult {
                    timeframe: Timeframe::M15,
                    ..result
                },
            ),
            (
                "candidates effort feature",
                EffortResult {
                    feature: catalog::CANDIDATE_EFFORT_RESULT_15M_V1.key,
                    ..result
                },
            ),
        ];
        let mut edits: Vec<(&'static str, Edit<'static>)> = Vec::new();
        for (name, value) in efforts {
            edits.push((
                name,
                Box::new(move |s| {
                    *s.candidates.windows_mut(Timeframe::M5).unwrap().0 =
                        FeatureValue::Ready(value);
                }),
            ));
        }
        edits.push((
            "candidates effort 1h",
            Box::new(move |s| {
                *s.candidates.windows_mut(Timeframe::H1).unwrap().0 = FeatureValue::Ready(result);
            }),
        ));
        edits.push((
            "candidates effort warming",
            Box::new(|s| {
                *s.candidates.windows_mut(Timeframe::M5).unwrap().0 = FeatureValue::WarmingUp {
                    observed: 1,
                    required: 1_440,
                };
            }),
        ));
        let exhaustion = Exhaustion {
            feature: catalog::CANDIDATE_EXHAUSTION_V1.key,
            end: EventTime::from_millis(300_000),
            expansion: result,
            decay: result,
            decay_aggression: Qty::from_units(3),
            extreme: Price::from_units(100),
            close: Price::from_units(99),
            expanded: true,
            decayed: true,
            at_extreme: true,
            active: true,
        };
        let other = EffortResult {
            effort: Qty::from_units(12),
            ..result
        };
        let exhaustions = [
            ("candidates exhaustion ready", exhaustion),
            (
                "candidates exhaustion end",
                Exhaustion {
                    end: EventTime::from_millis(360_000),
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion expansion",
                Exhaustion {
                    expansion: other,
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion decay",
                Exhaustion {
                    decay: other,
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion decay aggression",
                Exhaustion {
                    decay_aggression: Qty::from_units(-3),
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion extreme",
                Exhaustion {
                    extreme: Price::from_units(101),
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion close",
                Exhaustion {
                    close: Price::from_units(98),
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion expanded",
                Exhaustion {
                    expanded: false,
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion decayed",
                Exhaustion {
                    decayed: false,
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion at extreme",
                Exhaustion {
                    at_extreme: false,
                    ..exhaustion
                },
            ),
            (
                "candidates exhaustion active",
                Exhaustion {
                    active: false,
                    ..exhaustion
                },
            ),
        ];
        for (name, value) in exhaustions {
            edits.push((
                name,
                Box::new(move |s| {
                    *s.candidates.exhaustion_mut() = FeatureValue::Ready(value);
                }),
            ));
        }
        edits.push((
            "candidates exhaustion warming",
            Box::new(|s| {
                *s.candidates.exhaustion_mut() = FeatureValue::WarmingUp {
                    observed: 1,
                    required: 6,
                };
            }),
        ));
        let absorption = Absorption {
            feature: catalog::CANDIDATE_ABSORPTION_5M_V1.key,
            effort: result,
            passive: Side::Ask,
            band: Rate::from_units(50_000),
            added: Qty::from_units(5),
            filled: Qty::from_units(4),
            book_coverage: Coverage::default(),
            replenished: true,
            active: true,
        };
        let absorptions = [
            ("candidates absorption ready", absorption),
            (
                "candidates absorption effort",
                Absorption {
                    effort: other,
                    ..absorption
                },
            ),
            (
                "candidates absorption passive",
                Absorption {
                    passive: Side::Bid,
                    ..absorption
                },
            ),
            (
                "candidates absorption band",
                Absorption {
                    band: Rate::from_units(20_000),
                    ..absorption
                },
            ),
            (
                "candidates absorption added",
                Absorption {
                    added: Qty::from_units(6),
                    ..absorption
                },
            ),
            (
                "candidates absorption filled",
                Absorption {
                    filled: Qty::from_units(3),
                    ..absorption
                },
            ),
            (
                "candidates absorption book coverage",
                Absorption {
                    book_coverage: Coverage {
                        partial_start: true,
                        feed_gap: false,
                    },
                    ..absorption
                },
            ),
            (
                "candidates absorption replenished",
                Absorption {
                    replenished: false,
                    ..absorption
                },
            ),
            (
                "candidates absorption active",
                Absorption {
                    active: false,
                    ..absorption
                },
            ),
        ];
        for (name, value) in absorptions {
            edits.push((
                name,
                Box::new(move |s| {
                    *s.candidates.windows_mut(Timeframe::M5).unwrap().1 =
                        FeatureValue::Ready(value);
                }),
            ));
        }
        edits.push((
            "candidates absorption 15m",
            Box::new(move |s| {
                *s.candidates.windows_mut(Timeframe::M15).unwrap().1 =
                    FeatureValue::Ready(absorption);
            }),
        ));
        edits
    }

    /// One bias or trigger value per edit (ADR-053 D1, D3), each differing
    /// from the others in one respect only: the component, the timeframe or
    /// family, readiness, or one field of the value.
    fn trigger_edits() -> Vec<(&'static str, Edit<'static>)> {
        use crate::bars::{Coverage, Timeframe};
        use crate::location::{AuctionState, AuctionStatus, Origin, Position, Region};
        use crate::structure::{Level, Side, Sweep, SweepOutcome};
        use crate::trigger::{StepAt, SwingSequence, SwingStep, Trigger, TriggerValue};
        let time = EventTime::from_millis;
        let step = |step: SwingStep, at: i64| StepAt {
            step,
            swing_time: time(at),
        };
        let sequence = |high: Option<StepAt>, low: Option<StepAt>| {
            FeatureValue::Ready(SwingSequence { high, low })
        };
        let swings = |timeframe: Timeframe, value: FeatureValue<SwingSequence>| -> Edit<'static> {
            Box::new(move |s: &mut MarketState| {
                *s.triggers.bias_mut().swings_mut(timeframe).unwrap() = value;
            })
        };
        let status = AuctionStatus {
            feature: catalog::LOCATION_AUCTION_PRIOR_DAY_V1.key,
            reference_start: time(0),
            val: Price::from_units(100),
            vah: Price::from_units(200),
            state: AuctionState::FailedBreakout,
            side: Some(Region::Above),
            accepted: Region::In,
            origin: Origin::Start,
            position: Position::In,
            probe: None,
            failure: None,
            last_close: Price::from_units(150),
            bar_end: time(60_000),
            known_at: time(60_001),
            coverage: Coverage::default(),
        };
        let auction = |position: usize, last: Option<Trigger<AuctionStatus>>| -> Edit<'static> {
            let feature = [
                catalog::TRIGGER_FAILED_AUCTION_V1.key,
                catalog::TRIGGER_FAILED_RECLAIM_V1.key,
                catalog::TRIGGER_BREAKOUT_ACCEPTANCE_V1.key,
            ][position];
            Box::new(move |s: &mut MarketState| {
                s.triggers.auction_mut()[position] =
                    FeatureValue::Ready(TriggerValue { feature, last });
            })
        };
        let fired = |known_at: i64, anchor: i64, status: AuctionStatus| {
            Some(Trigger {
                known_at: time(known_at),
                anchor: Price::from_units(anchor),
                evidence: status,
            })
        };
        let sweep = Sweep {
            feature: catalog::STRUCTURE_LEVELS_15M_V1.key,
            timeframe: Timeframe::M15,
            level: Level {
                side: Side::High,
                price: Price::from_units(200),
                swing_time: time(0),
                confirmed_bar_end: time(3_600_000),
                known_at: time(3_600_001),
                touches: 0,
                coverage: Coverage::default(),
            },
            time: time(3_700_000),
            price: Price::from_units(201),
            extreme: Price::from_units(201),
            window_bars: 1,
            outcome: SweepOutcome::Sfp,
            resolved_bar_end: Some(time(4_500_000)),
            known_at: time(4_500_001),
            coverage: Coverage::default(),
        };
        let sfp = |position: usize, last: Option<Trigger<Sweep>>| -> Edit<'static> {
            let feature = catalog::TRIGGER_SFP[position].1.key;
            Box::new(move |s: &mut MarketState| {
                s.triggers.sfp_mut()[position] =
                    FeatureValue::Ready(TriggerValue { feature, last });
            })
        };
        let resolved = Some(Trigger {
            known_at: time(4_500_001),
            anchor: Price::from_units(200),
            evidence: sweep,
        });
        let percentile = AtrPercentile::new(50.0).unwrap();
        let regime = Regime {
            atr_percentile: percentile,
            label: percentile.label(),
            feature: catalog::VOLATILITY_REGIME_1H_V1.key,
        };
        vec![
            (
                "triggers bias regime",
                Box::new(move |s: &mut MarketState| {
                    s.triggers.bias_mut().regime = FeatureValue::Ready(regime);
                }),
            ),
            (
                "triggers bias regime count",
                Box::new(|s: &mut MarketState| {
                    s.triggers.bias_mut().regime = FeatureValue::WarmingUp {
                        observed: 1,
                        required: 214,
                    };
                }),
            ),
            (
                "triggers bias feature",
                Box::new(|s: &mut MarketState| {
                    s.triggers.bias_mut().feature = FeatureKey::new("bias.context", 2);
                }),
            ),
            (
                "triggers bias swings 15m",
                swings(Timeframe::M15, sequence(None, None)),
            ),
            (
                "triggers bias swings 1d",
                swings(Timeframe::D1, sequence(None, None)),
            ),
            (
                "triggers bias swings unavailable",
                swings(
                    Timeframe::M15,
                    FeatureValue::Unavailable {
                        reason: Unavailability::OutOfRange,
                    },
                ),
            ),
            (
                "triggers bias high step",
                swings(
                    Timeframe::M15,
                    sequence(Some(step(SwingStep::Higher, 0)), None),
                ),
            ),
            (
                "triggers bias low step",
                swings(
                    Timeframe::M15,
                    sequence(None, Some(step(SwingStep::Higher, 0))),
                ),
            ),
            (
                "triggers bias step kind",
                swings(
                    Timeframe::M15,
                    sequence(Some(step(SwingStep::Equal, 0)), None),
                ),
            ),
            (
                "triggers bias step time",
                swings(
                    Timeframe::M15,
                    sequence(Some(step(SwingStep::Higher, 1)), None),
                ),
            ),
            ("triggers failed auction ready", auction(0, None)),
            ("triggers failed reclaim ready", auction(1, None)),
            ("triggers breakout acceptance ready", auction(2, None)),
            (
                "triggers auction last",
                auction(0, fired(60_001, 200, status)),
            ),
            (
                "triggers auction known at",
                auction(0, fired(60_002, 200, status)),
            ),
            (
                "triggers auction anchor",
                auction(0, fired(60_001, 100, status)),
            ),
            (
                "triggers auction evidence",
                auction(
                    0,
                    fired(
                        60_001,
                        200,
                        AuctionStatus {
                            failure: None,
                            last_close: Price::from_units(151),
                            ..status
                        },
                    ),
                ),
            ),
            (
                "triggers auction warming",
                Box::new(|s: &mut MarketState| {
                    s.triggers.auction_mut()[0] = FeatureValue::Unavailable {
                        reason: Unavailability::InputInvalid,
                    };
                }),
            ),
            ("triggers sfp 15m ready", sfp(0, None)),
            ("triggers sfp 1d ready", sfp(3, None)),
            ("triggers sfp last", sfp(0, resolved)),
            (
                "triggers sfp evidence",
                sfp(
                    0,
                    Some(Trigger {
                        known_at: time(4_500_001),
                        anchor: Price::from_units(200),
                        evidence: Sweep {
                            extreme: Price::from_units(202),
                            ..sweep
                        },
                    }),
                ),
            ),
        ]
    }

    /// Five valid books, each differing from the first in one respect only:
    /// a level's quantity, the chain id, the chain state or the trusted
    /// window.
    fn book_variants() -> Vec<(&'static str, OrderBook)> {
        let level = |price: i64, qty: i64| Level {
            price: Price::from_units(price),
            qty: Qty::from_units(qty),
        };
        let book = |id: u64, deepest: i64, best_qty: i64, chained: bool| {
            let mut book = OrderBook::new();
            book.apply(&MarketEvent::BookSnapshot(BookSnapshot {
                time: EventTime::from_millis(1_000),
                last_update_id: id,
                bids: vec![level(100, best_qty), level(deepest, 5)],
                asks: vec![level(101, 7)],
            }));
            if chained {
                // Removes the deepest bid; the window keeps its bound.
                book.apply(&MarketEvent::BookUpdate(BookUpdate {
                    time: EventTime::from_millis(1_000),
                    first_update_id: id,
                    last_update_id: id + 1,
                    prev_update_id: 0,
                    bids: vec![level(deepest, 0)],
                    asks: Vec::new(),
                }));
            }
            assert!(book.is_valid());
            book
        };
        vec![
            ("book l2", book(10, 98, 3, false)),
            ("book level qty", book(10, 98, 4, false)),
            ("book chain id", book(11, 98, 3, false)),
            ("book chained", book(10, 98, 3, true)),
            ("book window", book(10, 97, 3, true)),
        ]
    }

    #[test]
    fn book_encoding_is_explicit() {
        // Straddle 0 + id, the window, then the levels best first.
        let (_, book) = book_variants().remove(0);
        let mut expected = Fingerprinter::new();
        expected.write_u8(0);
        expected.write_u64(10);
        expected.write_u8(1);
        expected.write_i64(98);
        expected.write_u8(1);
        expected.write_i64(101);
        expected.write_len(2);
        for (price, qty) in [(100, 3), (98, 5)] {
            expected.write_i64(price);
            expected.write_i64(qty);
        }
        expected.write_len(1);
        expected.write_i64(101);
        expected.write_i64(7);
        assert_eq!(encoded(&book), expected.finish());
        assert_ne!(encoded(&Side::Bid), encoded(&Side::Ask));
        let mut bid = Fingerprinter::new();
        bid.write_u8(0);
        assert_eq!(encoded(&Side::Bid), bid.finish());
        // An invalid book writes its state alone, then the empty window and
        // sides.
        let mut invalid = Fingerprinter::new();
        invalid.write_u8(2);
        invalid.write_u8(0);
        invalid.write_u8(0);
        invalid.write_len(0);
        invalid.write_len(0);
        assert_eq!(encoded(&OrderBook::new()), invalid.finish());
    }

    #[test]
    fn aggressor_codes_follow_the_declaration_order() {
        use crate::event::Aggressor;
        for (aggressor, code) in [(Aggressor::Buy, 0), (Aggressor::Sell, 1)] {
            let mut expected = Fingerprinter::new();
            expected.write_u8(code);
            assert_eq!(encoded(&aggressor), expected.finish(), "{aggressor:?}");
        }
    }

    #[test]
    fn floats_hash_by_their_bit_pattern() {
        assert_ne!(encoded(&0.0_f64), encoded(&-0.0_f64));
        let mut expected = Fingerprinter::new();
        expected.write_u64(1.5_f64.to_bits());
        assert_eq!(encoded(&1.5_f64), expected.finish());
    }

    #[test]
    fn feature_value_tags_are_distinct_and_written_first() {
        let warming: FeatureValue<u64> = FeatureValue::WarmingUp {
            observed: 0,
            required: 0,
        };
        let ready: FeatureValue<u64> = FeatureValue::Ready(0);
        let input_invalid: FeatureValue<u64> = FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        };
        let out_of_range: FeatureValue<u64> = FeatureValue::Unavailable {
            reason: Unavailability::OutOfRange,
        };
        let hashes = [
            encoded(&warming),
            encoded(&ready),
            encoded(&input_invalid),
            encoded(&out_of_range),
        ];
        for (i, a) in hashes.iter().enumerate() {
            for b in &hashes[i + 1..] {
                assert_ne!(a, b);
            }
        }
        let mut expected = Fingerprinter::new();
        expected.write_u8(0);
        expected.write_u64(0);
        expected.write_u64(0);
        assert_eq!(hashes[0], expected.finish());
        let mut expected = Fingerprinter::new();
        expected.write_u8(1);
        expected.write_u64(0);
        assert_eq!(hashes[1], expected.finish());
        let mut expected = Fingerprinter::new();
        expected.write_u8(2);
        expected.write_u8(1);
        assert_eq!(hashes[3], expected.finish());
    }

    #[test]
    fn none_and_some_differ_and_lengths_separate_sequences() {
        assert_ne!(encoded(&None::<u64>), encoded(&Some(0_u64)));
        let pair = |a: Vec<u64>, b: Vec<u64>| {
            let mut f = Fingerprinter::new();
            a.encode(&mut f);
            b.encode(&mut f);
            f.finish()
        };
        assert_ne!(pair(vec![1, 2], vec![3]), pair(vec![1], vec![2, 3]));
        assert_ne!(pair(vec![], vec![1]), pair(vec![1], vec![]));
        assert_eq!(encoded(&[7_u64, 8]), encoded(&vec![7_u64, 8]));
        assert_eq!(encoded(&true), {
            let mut f = Fingerprinter::new();
            f.write_u8(1);
            f.finish()
        });
    }

    #[test]
    fn equal_events_give_equal_hashes_shown_as_16_hex_digits() {
        let a = engine_after(&tape()).state().state_hash();
        let b = engine_after(&tape()).state().state_hash();
        assert_eq!(a, b);
        assert_ne!(a, MarketStateEngine::new().state().state_hash());
        let shown = a.to_string();
        assert_eq!(shown.len(), 16);
        assert!(
            shown
                .bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(StateHash::from_fingerprint(a.fingerprint()), a);
    }
}
