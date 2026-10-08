//! Experiments and their results (ADR-040; Strategy Research Lab brief,
//! "experiment definition"; ADR-007, ADR-020).
//!
//! Every research claim is traceable: a formal experiment specification
//! goes in, an immutable result with its provenance comes out.
//!
//! - [`ExperimentSpec`] ([`spec`]) — the brief's experiment fields, parsed
//!   and validated from **spec text v1**, which is at once the authoring
//!   input for humans and agents, the identity and the stored form.
//! - [`ExperimentId`] — FNV-1a 64 ([`crate::fingerprint`]) over the
//!   canonical spec text, so the same experiment has the same id everywhere.
//! - [`ExperimentResult`] ([`result`]) — what a deterministic pipeline
//!   produced for a spec, keyed by `(experiment id, pipeline id@N)` and
//!   stored as **result text v1**. Only the pipeline writes results
//!   (ADR-020); agents may propose specs.
//!
//! Slots whose semantics later issues own (location, trigger, entry,
//! invalidation, target, slippage, funding, Market State filters) are
//! versioned [`RuleRef`]s: `id@N` plus typed parameters, checked for syntax
//! and feature membership only. Rule registries can join later without
//! changing spec text v1.
//!
//! # Spec text v1
//!
//! ```text
//! mie-experiment 1
//! hypothesis vah.failed_auction.short
//! sample 1759276800000 1761955200000
//! data 0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f
//! features 4a14739bcf46afb3 bars.time.1h@1,bars.time.1m@1,profile.volume.utc_day@1,volatility.atr.1h@1,volatility.regime.1h@1
//! state-filter none
//! regime-filter volatility.regime.1h@1 HIGH,EXTREME
//! location location.at_level@1 level=text:vah tolerance=rate:0.00100000
//! trigger trigger.failed_auction@1
//! entry entry.next_trade@1
//! invalidation invalidation.beyond_extreme@1
//! target target.level@1 level=text:poc
//! fees maker=0.00020000 taker=0.00050000
//! slippage slippage.fixed@1 rate=rate:0.00010000
//! funding funding.recorded@1
//! latency 250
//! ```
//!
//! This is the canonical form of the spec the tests pin; its id is
//! `3f902847001e1998`.
//!
//! Input:
//! - the first line is exactly `mie-experiment 1`;
//! - one `<key> <args>` per line, arguments separated by single ASCII
//!   spaces, `\n` line endings, printable ASCII only;
//! - blank lines and lines starting with `#` are ignored; keys may come in
//!   any order.
//!
//! Required exactly once: `hypothesis`, `sample`, `data`, `features`,
//! `location`, `trigger`, `entry`, `invalidation`, `target`, `fees`,
//! `slippage`, `funding`, `latency`. Required at least once: `state-filter`
//! and `regime-filter`; `none` as the only line of its kind means
//! "explicitly unfiltered", `none` mixed with filters is an error, and a
//! filter line given twice is a duplicate.
//!
//! | Key | Arguments |
//! |---|---|
//! | `hypothesis` | an id under [`crate::feature::is_valid_id`] |
//! | `sample` | `<start_ms> <end_ms>`, event time, half-open, `start < end` |
//! | `data` | the dataset version, 64 lowercase hex |
//! | `features` | `<16 hex> <id@N,…\|none>`: the list resolves through the [`FeatureRegistry`](crate::feature::FeatureRegistry) into a [`FeatureSet`](crate::feature::FeatureSet), so it is closed over its upstream features; the hex must equal its [`FeatureSetVersion`](crate::feature::FeatureSetVersion) |
//! | `state-filter` | a rule reference, or `none` |
//! | `regime-filter` | `<id@N> <LABEL,…>` or `none`: a `volatility.regime.*` feature of the spec's set and ADR-017 labels, non-empty, without repeats |
//! | `location`, `trigger`, `entry`, `invalidation`, `target`, `slippage`, `funding` | a rule reference |
//! | `fees` | `maker=<decimal> taker=<decimal>`, fractions of notional, `0 <= fee < 1` |
//! | `latency` | milliseconds, an integer from 0 to 3 600 000 |
//!
//! A **rule reference** is `<id>@<N>` followed by ` <name>=<type>:<value>`
//! pairs. The id and version follow the feature key grammar; names follow
//! `[a-z][a-z0-9_]*` and are unique. Types: `int` (`i64`), `bool` (`true`
//! or `false`), `text` (`[A-Za-z0-9_.-]{1,64}`), `price`, `qty` and `rate`
//! (exact decimals, ADR-027) and `feature` (`id@N`, registered and a member
//! of the spec's feature set). Rule ids are not resolved: no rule registry
//! exists yet.
//!
//! The **canonical text** is the header, then the keys in the order of the
//! table above (`hypothesis` first, `latency` last), filter lines sorted
//! bytewise, parameters sorted by name, integers in plain decimal, decimals
//! in their 8-place form (`0.00050000`), labels in `LOW`, `MEDIUM`, `HIGH`,
//! `EXTREME` order and features sorted by id; every line ends in `\n`, no
//! comments, no blank lines. Equivalent inputs render byte-identical text.
//!
//! **Identity.** [`ExperimentId`] is FNV-1a 64 of
//! `Fingerprinter::write_str(canonical text)`, displayed as 16 lowercase
//! hex digits. Extension rule (as in ADR-029): a field added later is a new
//! line kind, rendered only when it differs from its default, so every
//! existing id stays valid.
//!
//! # Result text v1
//!
//! ```text
//! mie-result 1
//! experiment <16 hex>
//! pipeline research.replay_summary@1
//! outcome replay-summary
//! stream <events>:<16 hex>
//! rejections <n>
//! spec
//! <canonical spec text, verbatim>
//! ```
//!
//! The `outcome` line tags the kind and the kind's own lines follow it; a
//! new kind adds a tag, and the lines of an existing kind never change. A
//! result whose `experiment` line differs from the id of its embedded spec
//! does not parse, and neither does any text that is not canonical.

pub mod result;
pub mod spec;

pub use result::{ExperimentResult, Outcome, PipelineKey, ResultKey, ResultParseError};
pub use spec::{ExperimentSpec, FeeSchedule, RegimeFilter, SamplePeriod, SpecError, SpecField};

use crate::feature::{FeatureKey, is_valid_id};
use crate::fingerprint::Fingerprint;
use crate::num::{Price, Qty, Rate};
use std::fmt;

/// The hypothesis an experiment tests, such as `vah.failed_auction.short`;
/// links experiments of one hypothesis. Same grammar as a feature id
/// ([`is_valid_id`]).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HypothesisId(String);

impl HypothesisId {
    /// Parses a hypothesis id; `None` if `text` breaks the grammar.
    pub fn new(text: &str) -> Option<Self> {
        is_valid_id(text).then(|| Self(text.to_owned()))
    }

    /// The id as text.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for HypothesisId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The version of the raw data an experiment reads: 64 lowercase hex
/// characters. It mirrors the raw store's dataset version (ADR-030), which
/// the ports own.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DataVersion(String);

impl DataVersion {
    /// Parses 64 lowercase hex characters; `None` for any other text.
    pub fn from_hex(hex: &str) -> Option<Self> {
        (hex.len() == 64 && hex.bytes().all(is_lower_hex)).then(|| Self(hex.to_owned()))
    }

    /// The version as 64 lowercase hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DataVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The identity of an experiment: the fingerprint of its canonical spec
/// text (module docs). Displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExperimentId(Fingerprint);

impl ExperimentId {
    /// Wraps a fingerprint.
    pub const fn from_fingerprint(fingerprint: Fingerprint) -> Self {
        Self(fingerprint)
    }

    /// Parses 16 lowercase hex digits; `None` for any other text.
    pub fn from_hex(hex: &str) -> Option<Self> {
        parse_hex16(hex).map(|value| Self(Fingerprint::from_raw(value)))
    }

    /// The underlying fingerprint.
    pub const fn fingerprint(self) -> Fingerprint {
        self.0
    }
}

impl fmt::Display for ExperimentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// A typed rule parameter value (module docs, "rule reference").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SpecValue {
    /// `int:<i64>`.
    Int(i64),
    /// `bool:true` or `bool:false`.
    Bool(bool),
    /// `text:<[A-Za-z0-9_.-]{1,64}>`.
    Text(String),
    /// `price:<decimal>` (ADR-027).
    Price(Price),
    /// `qty:<decimal>` (ADR-027).
    Qty(Qty),
    /// `rate:<decimal>` (ADR-027).
    Rate(Rate),
    /// `feature:<id@N>`, a member of the spec's feature set.
    Feature(FeatureKey),
}

impl fmt::Display for SpecValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(value) => write!(f, "int:{value}"),
            Self::Bool(value) => write!(f, "bool:{value}"),
            Self::Text(value) => write!(f, "text:{value}"),
            Self::Price(value) => write!(f, "price:{value}"),
            Self::Qty(value) => write!(f, "qty:{value}"),
            Self::Rate(value) => write!(f, "rate:{value}"),
            Self::Feature(value) => write!(f, "feature:{value}"),
        }
    }
}

/// A named rule parameter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleParam {
    /// The name, `[a-z][a-z0-9_]*`.
    pub name: String,
    /// The typed value.
    pub value: SpecValue,
}

/// A versioned rule reference: `id@N` plus typed parameters sorted by name
/// (module docs). Displays in its canonical form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuleRef {
    id: String,
    version: u32,
    params: Vec<RuleParam>,
}

impl RuleRef {
    /// The rule id.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The rule version, from 1.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The parameters, sorted by name.
    pub fn params(&self) -> &[RuleParam] {
        &self.params
    }

    /// The value of parameter `name`, if given.
    pub fn param(&self, name: &str) -> Option<&SpecValue> {
        self.params
            .iter()
            .find(|param| param.name == name)
            .map(|param| &param.value)
    }

    /// The features the parameters reference.
    pub(crate) fn features(&self) -> impl Iterator<Item = FeatureKey> + '_ {
        self.params.iter().filter_map(|param| match param.value {
            SpecValue::Feature(key) => Some(key),
            _ => None,
        })
    }
}

impl fmt::Display for RuleRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.version)?;
        for param in &self.params {
            write!(f, " {}={}", param.name, param.value)?;
        }
        Ok(())
    }
}

fn is_lower_hex(byte: u8) -> bool {
    byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
}

/// Parses exactly 16 lowercase hex digits.
pub(crate) fn parse_hex16(text: &str) -> Option<u64> {
    if text.len() != 16 || !text.bytes().all(is_lower_hex) {
        return None;
    }
    u64::from_str_radix(text, 16).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hypothesis_ids_follow_the_feature_id_grammar() {
        assert_eq!(
            HypothesisId::new("vah.failed_auction.short").map(|h| h.to_string()),
            Some("vah.failed_auction.short".to_owned())
        );
        for bad in ["", "Vah", "vah.", "vah..x", "vah x", "vah@1"] {
            assert_eq!(HypothesisId::new(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn data_versions_are_64_lowercase_hex() {
        let hex = "0f".repeat(32);
        assert_eq!(DataVersion::from_hex(&hex).unwrap().as_str(), hex);
        assert_eq!(DataVersion::from_hex(&"0F".repeat(32)), None);
        assert_eq!(DataVersion::from_hex(&"0f".repeat(31)), None);
        assert_eq!(DataVersion::from_hex(&format!("{hex}0")), None);
    }

    #[test]
    fn experiment_ids_round_trip_through_16_hex_digits() {
        let id = ExperimentId::from_fingerprint(Fingerprint::from_raw(0xab));
        assert_eq!(id.to_string(), "00000000000000ab");
        assert_eq!(ExperimentId::from_hex("00000000000000ab"), Some(id));
        assert_eq!(id.fingerprint().value(), 0xab);
        for bad in [
            "ab",
            "00000000000000AB",
            "+0000000000000ab",
            "00000000000000abc",
        ] {
            assert_eq!(ExperimentId::from_hex(bad), None, "{bad:?}");
        }
    }

    #[test]
    fn values_display_with_their_type() {
        let key = FeatureKey::new("volatility.atr.1h", 1);
        let cases = [
            (SpecValue::Int(-3), "int:-3"),
            (SpecValue::Bool(true), "bool:true"),
            (SpecValue::Text("vah".to_owned()), "text:vah"),
            (
                SpecValue::Price(Price::from_units(150_000_000)),
                "price:1.50000000",
            ),
            (SpecValue::Qty(Qty::from_units(1)), "qty:0.00000001"),
            (
                SpecValue::Rate(Rate::from_units(100_000)),
                "rate:0.00100000",
            ),
            (SpecValue::Feature(key), "feature:volatility.atr.1h@1"),
        ];
        for (value, text) in cases {
            assert_eq!(value.to_string(), text);
        }
    }
}
