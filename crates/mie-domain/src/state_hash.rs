//! The Market State hash: one stable identity for a [`MarketState`]
//! (ADR-041).
//!
//! The equivalence harness (#13, ADR-019) compares the state live ingestion
//! reached with the state a replay of the same events reaches, at regular
//! checkpoints. Both sides compare a [`StateHash`]: a FNV-1a 64
//! [`Fingerprint`] over an explicit byte encoding of every public field of
//! [`MarketState`], written with the [`Fingerprinter`] writers (ADR-029). The
//! engine's internal trackers (ATR window, flow, profile, structure,
//! derivatives, liquidity and location trackers) are not hashed: the public state is
//! the contract, and a hidden divergence shows up in it at a later
//! checkpoint. The order book is public state (`book.l2@1`, ADR-043), so
//! every level of it is hashed.
//!
//! **Encoding v1.** A header, `write_str("mie-market-state")` then
//! `write_u32(`[`STATE_HASH_ENCODING`]`)`, then the fields of
//! [`MarketState`] in declaration order: `feature_set`, `as_of`,
//! `last_trade_price`, `bars`, `motion`, `atr`, `regime`, `flow`, `profile`,
//! `structure`, `derivatives`, `book`, `location`, `trade_count`. Every
//! value is written
//! by these rules:
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
//! | enums | an explicit match, never a cast, with codes in declaration order: `Timeframe` M1 0, M5 1, M15 2, H1 3, H4 4, D1 5; `RegimeLabel` Low 0, Medium 1, High 2, Extreme 3; `Side` High 0, Low 1; `SweepOutcome` Pending 0, Sfp 1, Break 2; `book::Side` Bid 0, Ask 1; `LevelKind` Poc 0, Vah 1, Val 2, Hvn 3, Lvn 4, StructuralHigh 5, StructuralLow 6, LiquidityCluster 7, PriorSweep 8, SfpRejectionZone 9, Vwap 10, ValidatedReference 11; `AuctionState` InsideValue 0, AtValueEdge 1, OutsideValue 2, Breakout 3, FailedBreakout 4, FailedReclaim 5, Acceptance 6; `LevelSide` High 0, Low 1, Bid 2, Ask 3; `Region` Below 0, In 1, Above 2; `Position` Below 0, LowEdge 1, In 2, HighEdge 3, Above 4; `Origin` Start 0, Acceptance 1 |
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
            derivatives,
            book,
            location,
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
        derivatives.encode(&mut f);
        book.encode(&mut f);
        location.encode(&mut f);
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
    use crate::book::{OrderBook, Side};
    use crate::derivatives::{LiquidationWindows, MarkState, OiSample, OiStep, SettledFunding};
    use crate::event::samples::{one_of_each, trade};
    use crate::event::{BookSnapshot, BookUpdate, Level, MarketEvent};
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
            "cc347065b37129d0"
        );
        assert_eq!(
            engine_after(&tape()).state().state_hash().to_string(),
            "a4468a96437546b7"
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
