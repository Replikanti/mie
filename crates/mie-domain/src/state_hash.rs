//! The Market State hash: one stable identity for a [`MarketState`]
//! (ADR-041).
//!
//! The equivalence harness (#13, ADR-019) compares the state live ingestion
//! reached with the state a replay of the same events reaches, at regular
//! checkpoints. Both sides compare a [`StateHash`]: a FNV-1a 64
//! [`Fingerprint`] over an explicit byte encoding of every public field of
//! [`MarketState`], written with the [`Fingerprinter`] writers (ADR-029). The
//! engine's internal trackers (ATR window, flow, profile and structure
//! trackers) are not hashed: the public state is the contract, and a hidden
//! divergence shows up in it at a later checkpoint.
//!
//! **Encoding v1.** A header, `write_str("mie-market-state")` then
//! `write_u32(`[`STATE_HASH_ENCODING`]`)`, then the fields of
//! [`MarketState`] in declaration order: `feature_set`, `as_of`,
//! `last_trade_price`, `bars`, `motion`, `atr`, `regime`, `flow`, `profile`,
//! `structure`, `trade_count`. Every value is written by these rules:
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
//! | enums | an explicit match, never a cast, with codes in declaration order: `Timeframe` M1 0, M5 1, M15 2, H1 3, H4 4, D1 5; `RegimeLabel` Low 0, Medium 1, High 2, Extreme 3; `Side` High 0, Low 1; `SweepOutcome` Pending 0, Sfp 1, Break 2 |
//! | structs | every field, through exhaustive destructuring, in declaration order as of encoding v1 |
//!
//! Each type's encoder sits next to the type, so private fields stay
//! private, and destructures it without `..`: a new field does not compile
//! until it is encoded.
//!
//! **Versioning.** Two state hashes are comparable only when both their
//! [`STATE_HASH_ENCODING`] and their [`FeatureSetVersion`] are equal. A new
//! feature family — a new [`MarketState`] field — changes the feature-set
//! version anyway (ADR-029); it extends the encoder in the same change,
//! without an encoding bump. Changing how an already-encoded field is
//! written (its writer, an enum code or the order) bumps
//! [`STATE_HASH_ENCODING`].
//!
//! [`FeatureSetVersion`]: crate::feature::FeatureSetVersion

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
            trade_count,
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
        trade_count.encode(&mut f);
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
    use crate::event::MarketEvent;
    use crate::event::samples::{one_of_each, trade};
    use crate::feature::{FeatureKey, FeatureSetVersion, FeatureValue, Unavailability};
    use crate::flow::Cvd;
    use crate::num::{Price, Qty};
    use crate::profile::{ProfileNode, VolumeProfile};
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
        // Both values change with every feature-set change (update them in
        // that change) and otherwise only with an encoding bump (module
        // docs).
        assert_eq!(
            MarketStateEngine::new().state().state_hash().to_string(),
            "f86c23cd8121054d"
        );
        assert_eq!(
            engine_after(&tape()).state().state_hash().to_string(),
            "5b7cf2028ee66394"
        );
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

        let edits: Vec<(&str, Edit<'_>)> = vec![
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
        ];
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
