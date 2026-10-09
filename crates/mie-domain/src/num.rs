//! Fixed-point prices, quantities and rates (ADR-027).
//!
//! Exchange values arrive as decimal strings. Holding them as integers at one
//! fixed scale keeps sums (delta, CVD, volume at price) exact and makes price
//! levels comparable with `==`, so feature values cannot drift between live
//! processing and replay. Ratios and statistics are derived from these exact
//! inputs at the feature level.
//!
//! Parsing is exact or it fails: [`FromStr`] accepts `-?[0-9]+(\.[0-9]+)?`
//! and rejects any value it cannot hold without rounding (ADR-027).

use crate::fingerprint::Fingerprinter;
use crate::state_hash::StateEncode;
use std::fmt;
use std::str::FromStr;

/// Decimal places carried by [`Price`], [`Qty`] and [`Rate`].
pub const DECIMALS: u32 = 8;

/// Units per whole number: `10^DECIMALS`.
pub const SCALE: i64 = 10_i64.pow(DECIMALS);

/// A price in the quote currency (USDT), as a count of `1 / SCALE` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(i64);

impl Price {
    /// Creates a price from a count of `1 / SCALE` units.
    pub const fn from_units(units: i64) -> Self {
        Self(units)
    }

    /// The count of `1 / SCALE` units.
    pub const fn units(self) -> i64 {
        self.0
    }
}

/// A quantity in the base currency (BTC), as a count of `1 / SCALE` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Qty(i64);

impl Qty {
    /// Creates a quantity from a count of `1 / SCALE` units.
    pub const fn from_units(units: i64) -> Self {
        Self(units)
    }

    /// The count of `1 / SCALE` units.
    pub const fn units(self) -> i64 {
        self.0
    }

    /// The exact sum, or `None` if it leaves the `i64` range of units
    /// (ADR-027: arithmetic is explicit about overflow).
    pub const fn checked_add(self, other: Self) -> Option<Self> {
        match self.0.checked_add(other.0) {
            Some(units) => Some(Self(units)),
            None => None,
        }
    }

    /// The exact difference, or `None` if it leaves the `i64` range of units
    /// (ADR-027).
    pub const fn checked_sub(self, other: Self) -> Option<Self> {
        match self.0.checked_sub(other.0) {
            Some(units) => Some(Self(units)),
            None => None,
        }
    }
}

/// A dimensionless ratio, such as a funding rate, as a count of `1 / SCALE`
/// units (ADR-027): `0.0001` (0.01 %) is `10_000` units.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Rate(i64);

impl Rate {
    /// Creates a rate from a count of `1 / SCALE` units.
    pub const fn from_units(units: i64) -> Self {
        Self(units)
    }

    /// The count of `1 / SCALE` units.
    pub const fn units(self) -> i64 {
        self.0
    }
}

impl StateEncode for Price {
    /// `write_i64` of the units (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_i64(self.0);
    }
}

impl StateEncode for Qty {
    /// `write_i64` of the units (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_i64(self.0);
    }
}

impl StateEncode for Rate {
    /// `write_i64` of the units (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_i64(self.0);
    }
}

impl fmt::Display for Price {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(self.0, f)
    }
}

impl fmt::Display for Qty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(self.0, f)
    }
}

impl fmt::Display for Rate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt_fixed(self.0, f)
    }
}

impl FromStr for Price {
    type Err = ParseDecimalError;

    /// Parses an exact decimal string; see [`ParseDecimalError`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_units(s).map(Self)
    }
}

impl FromStr for Qty {
    type Err = ParseDecimalError;

    /// Parses an exact decimal string; see [`ParseDecimalError`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_units(s).map(Self)
    }
}

impl FromStr for Rate {
    type Err = ParseDecimalError;

    /// Parses an exact decimal string; see [`ParseDecimalError`].
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_units(s).map(Self)
    }
}

/// Why a decimal string was rejected (ADR-027).
///
/// The grammar is `-?[0-9]+(\.[0-9]+)?` over ASCII bytes: no `+`, no exponent,
/// no whitespace, no separators, at least one digit on each side of the point.
/// Leading zeros and `-0` are accepted. When several problems apply, the first
/// in the order `Empty`, `Malformed`, `TooManyDecimals`, `Overflow` is
/// reported.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseDecimalError {
    /// The input is the empty string.
    Empty,
    /// The input does not match the grammar.
    Malformed,
    /// A non-zero digit follows the 8th decimal place, so the value is not
    /// representable without rounding. Zeros there are accepted: they do not
    /// change the value.
    TooManyDecimals,
    /// The value is outside the `i64` range of `1 / SCALE` units.
    Overflow,
}

impl fmt::Display for ParseDecimalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Empty => "the decimal string is empty",
            Self::Malformed => "the decimal string does not match -?[0-9]+(.[0-9]+)?",
            Self::TooManyDecimals => "the decimal has a non-zero digit beyond 8 decimal places",
            Self::Overflow => "the decimal is outside the range of 64-bit fixed point",
        };
        f.write_str(reason)
    }
}

impl std::error::Error for ParseDecimalError {}

/// Parses `-?[0-9]+(\.[0-9]+)?` into a count of `1 / SCALE` units.
///
/// Byte-wise over ASCII with checked arithmetic only. The magnitude is
/// accumulated unsigned, so `i64::MIN` parses although its magnitude has no
/// positive `i64` counterpart.
fn parse_units(s: &str) -> Result<i64, ParseDecimalError> {
    if s.is_empty() {
        return Err(ParseDecimalError::Empty);
    }
    let bytes = s.as_bytes();
    let (negative, body) = match bytes.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, bytes),
    };
    let (int, frac) = match body.iter().position(|&b| b == b'.') {
        Some(point) => (&body[..point], Some(&body[point + 1..])),
        None => (body, None),
    };
    let is_digits = |part: &[u8]| !part.is_empty() && part.iter().all(u8::is_ascii_digit);
    if !is_digits(int) || frac.is_some_and(|frac| !is_digits(frac)) {
        return Err(ParseDecimalError::Malformed);
    }
    let frac = frac.unwrap_or_default();
    let places = DECIMALS as usize;
    let (kept, beyond) = frac.split_at(frac.len().min(places));
    if beyond.iter().any(|&b| b != b'0') {
        return Err(ParseDecimalError::TooManyDecimals);
    }

    let mut magnitude: u64 = 0;
    for &digit in int.iter().chain(kept) {
        magnitude = magnitude
            .checked_mul(10)
            .and_then(|m| m.checked_add(u64::from(digit - b'0')))
            .ok_or(ParseDecimalError::Overflow)?;
    }
    for _ in kept.len()..places {
        magnitude = magnitude
            .checked_mul(10)
            .ok_or(ParseDecimalError::Overflow)?;
    }
    let units = if negative {
        0_i64.checked_sub_unsigned(magnitude)
    } else {
        0_i64.checked_add_unsigned(magnitude)
    };
    units.ok_or(ParseDecimalError::Overflow)
}

fn fmt_fixed(units: i64, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    let sign = if units < 0 { "-" } else { "" };
    let abs = units.unsigned_abs();
    let scale = SCALE.unsigned_abs();
    write!(
        f,
        "{sign}{}.{:0width$}",
        abs / scale,
        abs % scale,
        width = DECIMALS as usize
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use ParseDecimalError::{Empty, Malformed, Overflow, TooManyDecimals};

    /// Deterministic 64-bit LCG (Knuth's MMIX constants) for test inputs.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0
        }
    }

    /// An independent model of the grammar, built on `str` and `i128`, used
    /// as the oracle for the parser.
    fn reference(s: &str) -> Result<i64, ParseDecimalError> {
        if s.is_empty() {
            return Err(Empty);
        }
        let (negative, body) = match s.strip_prefix('-') {
            Some(rest) => (true, rest),
            None => (false, s),
        };
        let (int, frac) = match body.split_once('.') {
            Some((int, frac)) => (int, Some(frac)),
            None => (body, None),
        };
        let digits = |part: &str| !part.is_empty() && part.chars().all(|c| c.is_ascii_digit());
        if !digits(int) || frac.is_some_and(|frac| !digits(frac)) {
            return Err(Malformed);
        }
        let frac = frac.unwrap_or("");
        if frac.chars().skip(8).any(|c| c != '0') {
            return Err(TooManyDecimals);
        }
        let int = int.trim_start_matches('0');
        if int.len() > 20 {
            return Err(Overflow);
        }
        let padded: String = frac.chars().chain(std::iter::repeat('0')).take(8).collect();
        let magnitude: i128 = format!("{int}{padded}").parse().unwrap();
        let value = if negative { -magnitude } else { magnitude };
        i64::try_from(value).map_err(|_| Overflow)
    }

    fn parse(s: &str) -> Result<i64, ParseDecimalError> {
        let price = s.parse::<Price>().map(Price::units);
        assert_eq!(
            s.parse::<Qty>().map(Qty::units),
            price,
            "Qty vs Price on {s:?}"
        );
        assert_eq!(
            s.parse::<Rate>().map(Rate::units),
            price,
            "Rate vs Price on {s:?}"
        );
        price
    }

    fn assert_round_trips(units: i64) {
        for text in [
            Price::from_units(units).to_string(),
            Qty::from_units(units).to_string(),
            Rate::from_units(units).to_string(),
        ] {
            assert_eq!(parse(&text), Ok(units), "round trip of {text}");
        }
    }

    #[test]
    fn displays_all_decimal_places() {
        assert_eq!(
            Price::from_units(6_354_210_000_000).to_string(),
            "63542.10000000"
        );
        assert_eq!(Qty::from_units(1_500_000).to_string(), "0.01500000");
        assert_eq!(Qty::from_units(0).to_string(), "0.00000000");
        assert_eq!(Rate::from_units(10_000).to_string(), "0.00010000");
    }

    #[test]
    fn displays_negative_values_and_extremes() {
        assert_eq!(Qty::from_units(-1_500_000).to_string(), "-0.01500000");
        assert_eq!(
            Qty::from_units(i64::MIN).to_string(),
            "-92233720368.54775808"
        );
        assert_eq!(Rate::from_units(-2_233).to_string(), "-0.00002233");
    }

    #[test]
    fn parses_exact_values() {
        for (text, units) in [
            ("0", 0),
            ("1", 100_000_000),
            ("0.1", 10_000_000),
            ("63542.10", 6_354_210_000_000),
            ("63542.1", 6_354_210_000_000),
            ("0.00000001", 1),
            ("007.50", 750_000_000),
            ("-0", 0),
            ("-0.0", 0),
            ("-0.00002233", -2_233),
            ("-1", -100_000_000),
            ("12345678.87654321", 1_234_567_887_654_321),
        ] {
            assert_eq!(parse(text), Ok(units), "{text}");
        }
    }

    #[test]
    fn digits_beyond_the_eighth_decimal_must_be_zero() {
        for text in [
            "0.000000001",
            "1.0000000010",
            "1.000000001",
            "-0.000000009",
            "63542.100000000000001",
        ] {
            assert_eq!(parse(text), Err(TooManyDecimals), "{text}");
        }
        for (text, units) in [
            ("1.000000000", 100_000_000),
            ("63542.1000000000000", 6_354_210_000_000),
            // The non-zero digit is the 8th decimal; only a zero follows it.
            ("1.000000010", 100_000_001),
            ("0.000000010", 1),
            ("-0.000000010000", -1),
        ] {
            assert_eq!(parse(text), Ok(units), "{text}");
        }
    }

    #[test]
    fn overflow_boundaries_are_exact() {
        assert_eq!(parse("92233720368.54775807"), Ok(i64::MAX));
        assert_eq!(parse("92233720368.547758070000"), Ok(i64::MAX));
        assert_eq!(parse("92233720368.54775808"), Err(Overflow));
        assert_eq!(parse("-92233720368.54775808"), Ok(i64::MIN));
        assert_eq!(parse("-92233720368.54775809"), Err(Overflow));
        assert_eq!(parse("92233720369"), Err(Overflow));
        // The unsigned accumulator's own limits: u64::MAX and u64::MAX + 1.
        assert_eq!(parse("184467440737.09551615"), Err(Overflow));
        assert_eq!(parse("184467440737.09551616"), Err(Overflow));
        assert_eq!(parse("-184467440737.09551616"), Err(Overflow));
        assert_eq!(parse(&"9".repeat(30)), Err(Overflow));
        assert_eq!(parse(&format!("{}1", "0".repeat(100))), Ok(100_000_000));
        assert_eq!(
            parse(&format!("-{}92233720368.54775808", "0".repeat(100))),
            Ok(i64::MIN)
        );
    }

    #[test]
    fn rejects_empty_and_malformed_input() {
        assert_eq!(parse(""), Err(Empty));
        for text in [
            "-", ".", "-.", "+1", "1.", ".5", " 1", "1 ", "1e8", "1E8", "1_000", "1,5", "0x10",
            "1.2.3", "--1", "-.5", "1.-5", "1-", "NaN", "inf", "-inf", "\u{FF11}", "\u{0663}",
            "1\n", "\t1", "1.5\u{0}",
        ] {
            assert_eq!(parse(text), Err(Malformed), "{text:?}");
        }
    }

    #[test]
    fn display_round_trips_for_every_type() {
        for units in [
            0,
            1,
            -1,
            SCALE - 1,
            -(SCALE - 1),
            SCALE,
            -SCALE,
            i64::MAX,
            i64::MIN,
            i64::MAX - 1,
            i64::MIN + 1,
        ] {
            assert_round_trips(units);
        }
        let mut lcg = Lcg(0x6d69_6500_0000_0027);
        for _ in 0..10_000 {
            let raw = lcg.next();
            // Spread magnitudes: shift by 0..=63 bits so small values occur too.
            let shift = (lcg.next() % 64) as u32;
            let units = i64::from_ne_bytes(raw.to_ne_bytes()) >> shift;
            assert_round_trips(units);
        }
    }

    #[test]
    fn exhaustive_short_inputs_match_the_reference_model() {
        const ALPHABET: &[u8] = b"019.-+e ";
        let mut accepted = 0_u32;
        let mut total = 0_u32;
        for len in 0..=6_u32 {
            let base = ALPHABET.len();
            for mut n in 0..base.pow(len) {
                let mut text = String::new();
                for _ in 0..len {
                    text.push(char::from(ALPHABET[n % base]));
                    n /= base;
                }
                let parsed = parse(&text);
                assert_eq!(parsed, reference(&text), "{text:?}");
                if let Ok(units) = parsed {
                    accepted += 1;
                    assert_round_trips(units);
                }
                total += 1;
            }
        }
        // 8^0 + … + 8^6 inputs; the accepted count pins the grammar.
        assert_eq!(total, 299_593);
        assert_eq!(accepted, 3_039);
    }

    #[test]
    fn very_long_inputs_fail_without_panicking() {
        let ones = "1".repeat(10_000);
        assert_eq!(parse(&ones), Err(Overflow));
        assert_eq!(parse(&format!("-{ones}")), Err(Overflow));
        assert_eq!(parse(&format!("0.{}", &ones[2..])), Err(TooManyDecimals));
        assert_eq!(parse(&format!("{ones}.{ones}")), Err(TooManyDecimals));
        assert_eq!(parse(&format!("0.{}", "0".repeat(9_998))), Ok(0));
        assert_eq!(parse(&format!("{}.5", "0".repeat(9_998))), Ok(50_000_000));
        assert_eq!(parse(&format!("{ones}x")), Err(Malformed));
    }

    #[test]
    fn qty_arithmetic_is_exact_or_none() {
        let q = Qty::from_units;
        assert_eq!(q(1_500_000).checked_add(q(2_500_000)), Some(q(4_000_000)));
        assert_eq!(q(1_500_000).checked_sub(q(2_500_000)), Some(q(-1_000_000)));
        assert_eq!(q(i64::MAX).checked_add(q(0)), Some(q(i64::MAX)));
        assert_eq!(q(i64::MAX - 1).checked_add(q(1)), Some(q(i64::MAX)));
        assert_eq!(q(i64::MAX).checked_add(q(1)), None);
        assert_eq!(q(i64::MIN).checked_add(q(-1)), None);
        assert_eq!(q(i64::MIN + 1).checked_sub(q(1)), Some(q(i64::MIN)));
        assert_eq!(q(i64::MIN).checked_sub(q(1)), None);
        assert_eq!(q(0).checked_sub(q(i64::MIN)), None);
        assert_eq!(q(-1).checked_sub(q(i64::MIN)), Some(q(i64::MAX)));
    }

    #[test]
    fn errors_describe_themselves() {
        for err in [Empty, Malformed, TooManyDecimals, Overflow] {
            assert!(!err.to_string().is_empty(), "{err:?}");
        }
    }
}
