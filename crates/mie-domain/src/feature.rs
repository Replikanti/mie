//! Feature identity, versioning and validity (ADR-029).
//!
//! Every derived feature is a registered, immutable definition `id@version`
//! (Market State & Regime brief: "all features require deterministic
//! definitions"; ADR-013, ADR-022). Research results stay reproducible after
//! definitions evolve because an old version is never edited or deleted: a
//! change ships as a new version next to it.
//!
//! - [`FeatureDefinition`] — id, version, exact parameters, inputs (raw
//!   streams or upstream features at exact versions) and warm-up. Its
//!   [`fingerprint`](FeatureDefinition::fingerprint) covers all of them.
//! - [`FeatureRegistry`] — every definition ever shipped
//!   ([`catalog::DEFINITIONS`]), validated, and checked against the
//!   append-only lock table ([`catalog::LOCK`]) that makes a published
//!   `id@version` immutable.
//! - [`FeatureSet`] — the features a Market State computes, one version per
//!   id and closed over its upstream features. Its [`FeatureSetVersion`] is
//!   stamped on [`MarketState`](crate::state::MarketState) and recorded by
//!   experiments.
//! - [`FeatureValue`] — a value with its validity: research reads only
//!   [`Ready`](FeatureValue::Ready) values, never a warming or unavailable
//!   one.
//!
//! # Definition encoding v1
//!
//! [`FeatureDefinition::fingerprint`] is FNV-1a 64
//! ([`crate::fingerprint`]) over, in this order:
//!
//! 1. the encoding version, `1u8`;
//! 2. the id (`str`), then the version (`u32`);
//! 3. the parameter count (`u32`), then per parameter its name (`str`), its
//!    type tag (`u8`: `Int` 1, `Bool` 2, `Text` 3, `Price` 4, `Qty` 5,
//!    `Rate` 6) and its value (`Int`, `Price`, `Qty`, `Rate`: `i64` units;
//!    `Bool`: `u8` 0 or 1; `Text`: `str`);
//! 4. the input count (`u32`), then per input a tag (`u8`) and its value:
//!    `Stream` 1 followed by the stream ordinal (`u8`, frozen by ADR-028), or
//!    `Feature` 2 followed by the upstream id (`str`) and version (`u32`);
//! 5. the warm-up tag (`u8`) and value: `None` 0; `Samples` 1 followed by
//!    `u32`; `Span` 2 followed by the milliseconds (`u64`).
//!
//! Extension rule: a field added to [`FeatureDefinition`] later is appended
//! with its own tag, and only when it differs from its default, so every
//! existing fingerprint stays valid.
//!
//! # Adding a feature
//!
//! 1. Pick an id `<family>.<name>[.<qualifier>]`. Families: `trade`, `bars`,
//!    `volatility`, `flow`, `book`, `derivatives`, `profile`, `structure`,
//!    `candidate`. Parameterizations that must coexist (ATR on 5m and on 1h)
//!    get distinct ids, and the timeframe also stays a parameter so the
//!    fingerprint covers it. Ids are never renamed or reused.
//! 2. Declare `<NAME>_V1` in [`catalog`]: parameters sorted by name with exact
//!    values (a time span is an `Int` whose name ends in `_ms`), inputs
//!    (streams, or upstream features at exact versions) and warm-up. Its doc
//!    names what a warm-up sample is and the gap policy.
//! 3. Add it to [`catalog::DEFINITIONS`]. If it is computed by default, also
//!    add it to [`catalog::CURRENT`].
//! 4. Append its [`catalog::LOCK`] line. The failing `catalog_matches_lock`
//!    test prints the fingerprint and the line to add; then update the line
//!    count and digest pinned by `lock_table_is_pinned`.
//! 5. Expose the value on [`MarketState`](crate::state::MarketState) as
//!    [`FeatureValue<T>`]. It is ready only after its warm-up and goes back to
//!    `WarmingUp` according to its gap policy.
//! 6. Encode the family's state in the Market State hash
//!    ([`state_hash`](crate::state_hash), ADR-041): an encoder next to each
//!    new type, through exhaustive destructuring, and the new field in
//!    `MarketState::state_hash`. Then update the pinned state-hash golden
//!    values; a new feature set changes them.
//! 7. Add a golden-output test per version on a fixed tape. It pins the
//!    behaviour the fingerprint cannot see.
//!    A new version is built on one version of each upstream id, so its
//!    dependency closure fits one feature set (the registry rejects a
//!    closure that needs two versions of one id).
//! 8. Any change to parameters, inputs, an upstream version, the warm-up or
//!    the outputs means a new `_V{n+1}` const and `LOCK` line. The old
//!    version stays in `DEFINITIONS` and `LOCK`, stays computable, and keeps
//!    its golden test. Never edit or delete a `LOCK` line.

pub mod catalog;

use crate::event::Stream;
use crate::fingerprint::{Fingerprint, Fingerprinter};
use crate::num::{Price, Qty, Rate};
use crate::state_hash::StateEncode;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

/// Version byte of the definition and feature-set encodings (module docs).
const ENCODING_V1: u8 = 1;

/// Maximum length of a [`FeatureId`] in bytes.
pub const MAX_ID_LEN: usize = 64;

/// Whether `id` is a valid [`FeatureId`]: `[a-z][a-z0-9_]*(\.[a-z0-9_]+)*`,
/// at most [`MAX_ID_LEN`] bytes. No uppercase, `@`, `,` or empty segment.
pub const fn is_valid_id(id: &str) -> bool {
    let bytes = id.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_ID_LEN || !bytes[0].is_ascii_lowercase() {
        return false;
    }
    let mut after_dot = false;
    let mut i = 1;
    while i < bytes.len() {
        let byte = bytes[i];
        if byte == b'.' {
            if after_dot {
                return false;
            }
            after_dot = true;
        } else if byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' {
            after_dot = false;
        } else {
            return false;
        }
        i += 1;
    }
    !after_dot
}

/// Splits the canonical text `id@N` into the id and the version: an id
/// valid under [`is_valid_id`] and a version written as a decimal integer
/// from 1 without sign or leading zeros that fits a `u32`. `None` for any
/// other text.
///
/// Feature keys and the versioned rule and pipeline references of
/// experiments ([`crate::research`]) share this grammar.
pub(crate) fn parse_versioned_id(text: &str) -> Option<(&str, u32)> {
    let (id, version) = text.split_once('@')?;
    let canonical_number = !version.is_empty()
        && version.bytes().all(|byte| byte.is_ascii_digit())
        && !version.starts_with('0');
    if !is_valid_id(id) || !canonical_number {
        return None;
    }
    let version: u32 = version.parse().ok()?;
    Some((id, version))
}

/// The stable identity of a feature, such as `volatility.atr_5m`.
///
/// Ids are never renamed or reused. The grammar is checked by
/// [`FeatureId::new`] (see [`is_valid_id`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeatureId(&'static str);

impl FeatureId {
    /// Creates an id.
    ///
    /// # Panics
    ///
    /// If `id` breaks the grammar of [`is_valid_id`] — a compile error when
    /// evaluated in a `const` item.
    pub const fn new(id: &'static str) -> Self {
        assert!(is_valid_id(id), "invalid feature id");
        Self(id)
    }

    /// The id as text.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for FeatureId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// The version of a feature definition: an integer from 1, contiguous per id
/// (1, 2, 3 …). The registry rejects 0 and gaps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeatureVersion(u32);

impl FeatureVersion {
    /// Wraps a version number.
    pub const fn new(version: u32) -> Self {
        Self(version)
    }

    /// The version number.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl fmt::Display for FeatureVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A feature at an exact version, `id@version`; orders by (id, version).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeatureKey {
    /// The feature.
    pub id: FeatureId,
    /// Its version.
    pub version: FeatureVersion,
}

impl FeatureKey {
    /// Creates `id@version`.
    ///
    /// # Panics
    ///
    /// If `id` breaks the grammar of [`is_valid_id`] (see [`FeatureId::new`]).
    pub const fn new(id: &'static str, version: u32) -> Self {
        Self {
            id: FeatureId::new(id),
            version: FeatureVersion::new(version),
        }
    }

    /// The ADR-029 encoding: `write_str` of the id, `write_u32` of the
    /// version. Definitions and the state hash (ADR-041) share it.
    pub(crate) fn encode(self, hasher: &mut Fingerprinter) {
        hasher.write_str(self.id.as_str());
        hasher.write_u32(self.version.get());
    }
}

impl StateEncode for FeatureKey {
    /// The ADR-029 encoding of [`FeatureKey::encode`] (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        FeatureKey::encode(*self, f);
    }
}

impl fmt::Display for FeatureKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", self.id, self.version)
    }
}

/// An exact parameter value. No `f64`: ADR-027 keeps floats for derived
/// statistics, and a parameter must fingerprint the same everywhere.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParamValue {
    /// An integer. A time span is an `Int` whose parameter name ends in
    /// `_ms`.
    Int(i64),
    /// A flag.
    Bool(bool),
    /// A symbolic choice, such as a timeframe or a method.
    Text(&'static str),
    /// A price (ADR-027).
    Price(Price),
    /// A quantity (ADR-027).
    Qty(Qty),
    /// A rate (ADR-027).
    Rate(Rate),
}

impl ParamValue {
    fn encode(self, hasher: &mut Fingerprinter) {
        match self {
            Self::Int(value) => {
                hasher.write_u8(1);
                hasher.write_i64(value);
            }
            Self::Bool(value) => {
                hasher.write_u8(2);
                hasher.write_u8(u8::from(value));
            }
            Self::Text(value) => {
                hasher.write_u8(3);
                hasher.write_str(value);
            }
            Self::Price(value) => {
                hasher.write_u8(4);
                hasher.write_i64(value.units());
            }
            Self::Qty(value) => {
                hasher.write_u8(5);
                hasher.write_i64(value.units());
            }
            Self::Rate(value) => {
                hasher.write_u8(6);
                hasher.write_i64(value.units());
            }
        }
    }
}

/// A named parameter of a feature definition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Param {
    /// The name; a definition lists its parameters sorted by name.
    pub name: &'static str,
    /// The exact value.
    pub value: ParamValue,
}

/// What a feature is computed from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Input {
    /// A raw market-data stream.
    Stream(Stream),
    /// An upstream feature at an exact version, so a version bump upstream
    /// propagates to every feature built on it.
    Feature(FeatureKey),
}

impl Input {
    fn encode(self, hasher: &mut Fingerprinter) {
        match self {
            Self::Stream(stream) => {
                hasher.write_u8(1);
                // The only enum cast in the encoding: ADR-028 freezes the
                // `Stream` ordinals.
                hasher.write_u8(stream as u8);
            }
            Self::Feature(key) => {
                hasher.write_u8(2);
                key.encode(hasher);
            }
        }
    }
}

impl fmt::Display for Input {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Stream(stream) => write!(f, "stream {stream:?}"),
            Self::Feature(key) => write!(f, "feature {key}"),
        }
    }
}

/// How much input a feature needs before its value is ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WarmUp {
    /// Ready from the first update.
    None,
    /// Ready after this many of the feature's own updates (closed bars,
    /// trades, snapshots); the definition's doc names which.
    Samples(u32),
    /// Ready once its inputs cover this much event time.
    Span {
        /// The span in milliseconds.
        millis: u64,
    },
}

impl WarmUp {
    fn encode(self, hasher: &mut Fingerprinter) {
        match self {
            Self::None => hasher.write_u8(0),
            Self::Samples(samples) => {
                hasher.write_u8(1);
                hasher.write_u32(samples);
            }
            Self::Span { millis } => {
                hasher.write_u8(2);
                hasher.write_u64(millis);
            }
        }
    }
}

/// An immutable feature definition, `id@version`. Constructible in `const`
/// items; the catalog holds every version ever shipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeatureDefinition {
    /// `id@version`.
    pub key: FeatureKey,
    /// Exact parameters, strictly sorted by name.
    pub params: &'static [Param],
    /// Raw streams and upstream features, without repeats.
    pub inputs: &'static [Input],
    /// Input needed before the value is ready.
    pub warm_up: WarmUp,
}

impl FeatureDefinition {
    /// The fingerprint of the definition over encoding v1 (module docs).
    /// Any change to the key, a parameter, an input or the warm-up changes
    /// it; the lock table pins it per `id@version`.
    pub fn fingerprint(&self) -> Fingerprint {
        let mut hasher = Fingerprinter::new();
        hasher.write_u8(ENCODING_V1);
        self.key.encode(&mut hasher);
        hasher.write_len(self.params.len());
        for param in self.params {
            hasher.write_str(param.name);
            param.value.encode(&mut hasher);
        }
        hasher.write_len(self.inputs.len());
        for input in self.inputs {
            input.encode(&mut hasher);
        }
        self.warm_up.encode(&mut hasher);
        hasher.finish()
    }
}

/// Why a structurally present feature has no value — not a warm-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Unavailability {
    /// The inputs are not in a valid state, such as an unsynced order book.
    InputInvalid,
    /// The value would fall beyond the range the feature is trusted for.
    OutOfRange,
}

impl StateEncode for Unavailability {
    /// `write_u8`: `InputInvalid` 0, `OutOfRange` 1 (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u8(match self {
            Self::InputInvalid => 0,
            Self::OutOfRange => 1,
        });
    }
}

/// A feature value with its validity.
///
/// Only [`Ready`](Self::Ready) carries a value; no accessor returns it in
/// another state and there is no `Default`, so research cannot consume a
/// warming or unavailable value silently. After a gap a feature goes back to
/// `WarmingUp` according to the gap policy in its definition's doc.
#[must_use]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureValue<T> {
    /// The lookback is not filled yet.
    WarmingUp {
        /// Samples (or milliseconds, for a span warm-up) seen so far.
        observed: u64,
        /// Samples (or milliseconds) needed.
        required: u64,
    },
    /// The value is valid.
    Ready(T),
    /// The inputs are structurally unable to produce a value.
    Unavailable {
        /// Why.
        reason: Unavailability,
    },
}

impl<T> FeatureValue<T> {
    /// The value if it is ready.
    pub fn ready(&self) -> Option<&T> {
        match self {
            Self::Ready(value) => Some(value),
            Self::WarmingUp { .. } | Self::Unavailable { .. } => None,
        }
    }

    /// Whether the value is ready.
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    /// Maps a ready value and keeps any other state as it is.
    pub fn map<U>(self, f: impl FnOnce(T) -> U) -> FeatureValue<U> {
        match self {
            Self::Ready(value) => FeatureValue::Ready(f(value)),
            Self::WarmingUp { observed, required } => {
                FeatureValue::WarmingUp { observed, required }
            }
            Self::Unavailable { reason } => FeatureValue::Unavailable { reason },
        }
    }
}

impl<T: StateEncode> StateEncode for FeatureValue<T> {
    /// `write_u8` of the state, `WarmingUp` 0, `Ready` 1, `Unavailable` 2,
    /// then its content (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        match self {
            Self::WarmingUp { observed, required } => {
                f.write_u8(0);
                f.write_u64(*observed);
                f.write_u64(*required);
            }
            Self::Ready(value) => {
                f.write_u8(1);
                value.encode(f);
            }
            Self::Unavailable { reason } => {
                f.write_u8(2);
                reason.encode(f);
            }
        }
    }
}

/// One line of the append-only lock table: the fingerprint a published
/// `id@version` must keep.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LockEntry {
    /// The locked `id@version`.
    pub key: FeatureKey,
    /// Its fingerprint.
    pub fingerprint: Fingerprint,
}

impl LockEntry {
    /// Creates the lock line `id@version = fingerprint`.
    ///
    /// # Panics
    ///
    /// If `id` breaks the grammar of [`is_valid_id`] (see [`FeatureId::new`]).
    pub const fn new(id: &'static str, version: u32, fingerprint: u64) -> Self {
        Self {
            key: FeatureKey::new(id, version),
            fingerprint: Fingerprint::from_raw(fingerprint),
        }
    }
}

/// Every known feature definition, validated.
#[derive(Debug, Clone)]
pub struct FeatureRegistry {
    definitions: BTreeMap<FeatureKey, &'static FeatureDefinition>,
}

impl FeatureRegistry {
    /// Validates and registers `definitions`.
    ///
    /// # Errors
    ///
    /// The first violation found, checked per definition in the given order,
    /// then across definitions:
    /// - [`RegistryError::ZeroVersion`] — a version 0;
    /// - [`RegistryError::UnsortedParams`] / [`RegistryError::DuplicateParam`]
    ///   — parameters not strictly sorted by name;
    /// - [`RegistryError::DuplicateInput`] — an input listed twice;
    /// - [`RegistryError::DuplicateKey`] — an `id@version` registered twice;
    /// - [`RegistryError::VersionGap`] — versions of an id that do not run
    ///   1, 2, 3 … (a deleted old version);
    /// - [`RegistryError::UnknownInput`] — an upstream feature that is not
    ///   registered;
    /// - [`RegistryError::DependencyCycle`] — features that depend on each
    ///   other, directly or through others;
    /// - [`RegistryError::ConflictingVersions`] — a definition whose
    ///   dependency closure (itself and its upstream features, transitively)
    ///   holds one id at two versions, so no [`FeatureSet`] could compute it.
    pub fn new(definitions: &[&'static FeatureDefinition]) -> Result<Self, RegistryError> {
        let mut registered = BTreeMap::new();
        for &definition in definitions {
            let key = definition.key;
            if key.version.get() == 0 {
                return Err(RegistryError::ZeroVersion { key });
            }
            for pair in definition.params.windows(2) {
                if pair[0].name == pair[1].name {
                    return Err(RegistryError::DuplicateParam {
                        key,
                        name: pair[1].name,
                    });
                }
                if pair[0].name > pair[1].name {
                    return Err(RegistryError::UnsortedParams {
                        key,
                        name: pair[1].name,
                    });
                }
            }
            for (i, input) in definition.inputs.iter().enumerate() {
                if definition.inputs[..i].contains(input) {
                    return Err(RegistryError::DuplicateInput { key, input: *input });
                }
            }
            if registered.insert(key, definition).is_some() {
                return Err(RegistryError::DuplicateKey { key });
            }
        }

        // Keys are sorted by (id, version): each id's versions are adjacent
        // and ascending.
        let mut previous: Option<FeatureKey> = None;
        for &key in registered.keys() {
            let expected = match previous {
                Some(last) if last.id == key.id => last.version.get() + 1,
                _ => 1,
            };
            if key.version.get() != expected {
                return Err(RegistryError::VersionGap {
                    id: key.id,
                    expected: FeatureVersion::new(expected),
                    found: key.version,
                });
            }
            previous = Some(key);
        }

        for (&key, definition) in &registered {
            for input in definition.inputs {
                if let Input::Feature(upstream) = input
                    && !registered.contains_key(upstream)
                {
                    return Err(RegistryError::UnknownInput {
                        feature: key,
                        input: *upstream,
                    });
                }
            }
        }

        let registry = Self {
            definitions: registered,
        };
        registry.check_acyclic()?;
        for &key in registry.definitions.keys() {
            registry.check_closure(key)?;
        }
        Ok(registry)
    }

    /// Every definition stays computable (ADR-029): its closure must fit one
    /// [`FeatureSet`], i.e. hold each id at one version only.
    fn check_closure(&self, feature: FeatureKey) -> Result<(), RegistryError> {
        let closure = self.closure_of(feature);
        // Sorted by (id, version): two versions of one id are adjacent.
        for pair in closure.windows(2) {
            if pair[0].id == pair[1].id {
                return Err(RegistryError::ConflictingVersions {
                    feature,
                    id: pair[0].id,
                    first: pair[0].version,
                    second: pair[1].version,
                });
            }
        }
        Ok(())
    }

    /// `key` and every upstream feature it depends on, transitively, sorted
    /// by (id, version). Requires a validated (acyclic, closed) registry.
    fn closure_of(&self, key: FeatureKey) -> Vec<FeatureKey> {
        let mut closure = BTreeSet::new();
        let mut pending = vec![key];
        while let Some(next) = pending.pop() {
            if closure.insert(next) {
                for input in self.definitions[&next].inputs {
                    if let Input::Feature(upstream) = input {
                        pending.push(*upstream);
                    }
                }
            }
        }
        closure.into_iter().collect()
    }

    /// The dependency closure of `key`: the key itself and every upstream
    /// feature it depends on, transitively, sorted by (id, version) — the
    /// smallest [`FeatureSet`] that computes it. `None` if `key` is not
    /// registered.
    pub fn closure(&self, key: FeatureKey) -> Option<Vec<FeatureKey>> {
        self.definitions
            .contains_key(&key)
            .then(|| self.closure_of(key))
    }

    /// Depth-first search over the upstream edges; every upstream is
    /// registered at this point.
    fn check_acyclic(&self) -> Result<(), RegistryError> {
        /// Visit state: on the current path, or fully explored.
        #[derive(Clone, Copy, PartialEq, Eq)]
        enum Mark {
            OnPath,
            Done,
        }

        fn visit(
            registry: &FeatureRegistry,
            key: FeatureKey,
            marks: &mut BTreeMap<FeatureKey, Mark>,
        ) -> Result<(), RegistryError> {
            match marks.get(&key) {
                Some(Mark::Done) => return Ok(()),
                Some(Mark::OnPath) => return Err(RegistryError::DependencyCycle { key }),
                None => {}
            }
            marks.insert(key, Mark::OnPath);
            for input in registry.definitions[&key].inputs {
                if let Input::Feature(upstream) = input {
                    visit(registry, *upstream, marks)?;
                }
            }
            marks.insert(key, Mark::Done);
            Ok(())
        }

        let mut marks = BTreeMap::new();
        for &key in self.definitions.keys() {
            visit(self, key, &mut marks)?;
        }
        Ok(())
    }

    /// The first unused version of `id`: one past its latest registered
    /// version, or 1.
    fn next_version(&self, id: FeatureId) -> u64 {
        let latest = self
            .definitions
            .keys()
            .filter(|key| key.id == id)
            .map(|key| key.version.get())
            .max()
            .unwrap_or(0);
        u64::from(latest) + 1
    }

    /// The definition of `key`, if registered.
    pub fn get(&self, key: FeatureKey) -> Option<&'static FeatureDefinition> {
        self.definitions.get(&key).copied()
    }

    /// Every registered definition, sorted by (id, version).
    pub fn definitions(&self) -> impl Iterator<Item = &'static FeatureDefinition> + '_ {
        self.definitions.values().copied()
    }

    /// Resolves the canonical text `id@N`, as recorded by an experiment, to
    /// its definition.
    ///
    /// # Errors
    ///
    /// - [`ResolveError::Malformed`] — not `id@N` with a valid id and a
    ///   version written as a decimal integer from 1 without sign or leading
    ///   zeros;
    /// - [`ResolveError::Unknown`] — well formed but not registered.
    pub fn resolve(&self, text: &str) -> Result<&'static FeatureDefinition, ResolveError> {
        let (id, version) = parse_versioned_id(text).ok_or_else(|| ResolveError::Malformed {
            text: text.to_owned(),
        })?;
        self.definitions
            .iter()
            .find(|(key, _)| key.id.as_str() == id && key.version.get() == version)
            .map(|(_, definition)| *definition)
            .ok_or_else(|| ResolveError::Unknown {
                text: text.to_owned(),
            })
    }

    /// Checks every registered definition against the lock table.
    ///
    /// # Errors
    ///
    /// Every violation: lock lines in the given order first, then
    /// unlocked definitions by key:
    /// - [`LockError::Changed`] — a locked `id@version` whose fingerprint
    ///   changed, i.e. edited without a version bump;
    /// - [`LockError::Missing`] — a locked `id@version` that is no longer
    ///   registered;
    /// - [`LockError::Unlocked`] — a registered `id@version` without a lock
    ///   line.
    pub fn verify_lock(&self, lock: &[LockEntry]) -> Result<(), Vec<LockError>> {
        let mut errors = Vec::new();
        for entry in lock {
            match self.get(entry.key) {
                None => errors.push(LockError::Missing { key: entry.key }),
                Some(definition) => {
                    let actual = definition.fingerprint();
                    if actual != entry.fingerprint {
                        errors.push(LockError::Changed {
                            key: entry.key,
                            locked: entry.fingerprint,
                            actual,
                            next: self.next_version(entry.key.id),
                        });
                    }
                }
            }
        }
        for (&key, definition) in &self.definitions {
            if !lock.iter().any(|entry| entry.key == key) {
                errors.push(LockError::Unlocked {
                    key,
                    actual: definition.fingerprint(),
                });
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
}

/// The fingerprint of a [`FeatureSet`]; displays as 16 lowercase hex digits.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct FeatureSetVersion(Fingerprint);

impl FeatureSetVersion {
    /// Wraps a fingerprint, such as one recorded by an experiment.
    pub const fn from_fingerprint(fingerprint: Fingerprint) -> Self {
        Self(fingerprint)
    }

    /// The underlying fingerprint.
    pub const fn fingerprint(self) -> Fingerprint {
        self.0
    }
}

impl StateEncode for FeatureSetVersion {
    /// `write_u64` of the fingerprint value (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_u64(self.0.value());
    }
}

impl fmt::Display for FeatureSetVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The features a Market State computes: one version per id, closed over its
/// upstream features.
///
/// `Display` prints the canonical list `id@N,id@N` (sorted by id), which
/// experiments record next to the [`FeatureSetVersion`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeatureSet {
    /// Sorted by key.
    members: Vec<&'static FeatureDefinition>,
    version: FeatureSetVersion,
}

impl FeatureSet {
    /// Builds the set of `keys` from `registry`, in any order.
    ///
    /// # Errors
    ///
    /// - [`FeatureSetError::Unknown`] — a key that is not registered;
    /// - [`FeatureSetError::DuplicateId`] — an id listed twice, at the same
    ///   or at different versions;
    /// - [`FeatureSetError::MissingDependency`] — an upstream feature that is
    ///   not in the set at exactly the version the member requires.
    pub fn new(registry: &FeatureRegistry, keys: &[FeatureKey]) -> Result<Self, FeatureSetError> {
        let mut members = Vec::with_capacity(keys.len());
        for &key in keys {
            let definition = registry.get(key).ok_or(FeatureSetError::Unknown { key })?;
            members.push(definition);
        }
        members.sort_by_key(|definition| definition.key);
        for pair in members.windows(2) {
            if pair[0].key.id == pair[1].key.id {
                return Err(FeatureSetError::DuplicateId { id: pair[1].key.id });
            }
        }
        for member in &members {
            for input in member.inputs {
                if let Input::Feature(upstream) = input
                    && members
                        .binary_search_by_key(upstream, |definition| definition.key)
                        .is_err()
                {
                    return Err(FeatureSetError::MissingDependency {
                        feature: member.key,
                        requires: *upstream,
                    });
                }
            }
        }

        // Set encoding v1: the version byte, the member count, then per
        // member (sorted by id) its id, version and definition fingerprint.
        let mut hasher = Fingerprinter::new();
        hasher.write_u8(ENCODING_V1);
        hasher.write_len(members.len());
        for member in &members {
            member.key.encode(&mut hasher);
            hasher.write_u64(member.fingerprint().value());
        }
        let version = FeatureSetVersion(hasher.finish());
        Ok(Self { members, version })
    }

    /// The set's fingerprint: independent of the order the keys were given
    /// in, changed by any member's id, version or definition.
    pub fn version(&self) -> FeatureSetVersion {
        self.version
    }

    /// The members, sorted by id.
    pub fn definitions(&self) -> impl Iterator<Item = &'static FeatureDefinition> + '_ {
        self.members.iter().copied()
    }
}

impl fmt::Display for FeatureSet {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (i, member) in self.members.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{}", member.key)?;
        }
        Ok(())
    }
}

/// Why the registry rejected its definitions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegistryError {
    /// The same `id@version` is registered twice.
    DuplicateKey {
        /// The repeated key.
        key: FeatureKey,
    },
    /// A definition has version 0; versions start at 1.
    ZeroVersion {
        /// The offending key.
        key: FeatureKey,
    },
    /// The versions of an id do not run 1, 2, 3 …
    VersionGap {
        /// The feature.
        id: FeatureId,
        /// The version that should come next.
        expected: FeatureVersion,
        /// The version found instead.
        found: FeatureVersion,
    },
    /// Parameters are not sorted by name.
    UnsortedParams {
        /// The definition.
        key: FeatureKey,
        /// The first parameter out of order.
        name: &'static str,
    },
    /// A parameter name appears twice.
    DuplicateParam {
        /// The definition.
        key: FeatureKey,
        /// The repeated name.
        name: &'static str,
    },
    /// An input appears twice.
    DuplicateInput {
        /// The definition.
        key: FeatureKey,
        /// The repeated input.
        input: Input,
    },
    /// An upstream feature is not registered.
    UnknownInput {
        /// The definition.
        feature: FeatureKey,
        /// The missing upstream.
        input: FeatureKey,
    },
    /// Features depend on each other in a cycle.
    DependencyCycle {
        /// A feature on the cycle.
        key: FeatureKey,
    },
    /// A definition's dependency closure needs one id at two versions, so no
    /// feature set can compute it.
    ConflictingVersions {
        /// The definition that cannot be computed.
        feature: FeatureKey,
        /// The id required at two versions.
        id: FeatureId,
        /// The lower required version.
        first: FeatureVersion,
        /// The higher required version.
        second: FeatureVersion,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateKey { key } => write!(f, "{key} is registered twice"),
            Self::ZeroVersion { key } => write!(f, "{key}: versions start at 1"),
            Self::VersionGap {
                id,
                expected,
                found,
            } => write!(
                f,
                "{id}: expected version {expected}, found {found}; old versions are never deleted"
            ),
            Self::UnsortedParams { key, name } => {
                write!(f, "{key}: parameter {name} is out of order; sort by name")
            }
            Self::DuplicateParam { key, name } => {
                write!(f, "{key}: parameter {name} appears twice")
            }
            Self::DuplicateInput { key, input } => write!(f, "{key}: {input} appears twice"),
            Self::UnknownInput { feature, input } => {
                write!(f, "{feature}: upstream {input} is not registered")
            }
            Self::DependencyCycle { key } => {
                write!(f, "{key} depends on itself through its upstream features")
            }
            Self::ConflictingVersions {
                feature,
                id,
                first,
                second,
            } => write!(
                f,
                "{feature} needs both {id}@{first} and {id}@{second} through its upstream features, \
                 so no feature set can compute it; build it on one version of {id}"
            ),
        }
    }
}

impl std::error::Error for RegistryError {}

/// Why `id@N` did not resolve.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResolveError {
    /// Not of the form `id@N`.
    Malformed {
        /// The rejected text.
        text: String,
    },
    /// Well formed, but not a registered definition.
    Unknown {
        /// The rejected text.
        text: String,
    },
}

impl fmt::Display for ResolveError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed { text } => write!(f, "{text:?} is not of the form id@N"),
            Self::Unknown { text } => write!(f, "{text} is not a registered feature"),
        }
    }
}

impl std::error::Error for ResolveError {}

/// A registered definition that disagrees with the lock table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockError {
    /// The definition no longer matches its locked fingerprint.
    Changed {
        /// The locked `id@version`.
        key: FeatureKey,
        /// The fingerprint in the lock table.
        locked: Fingerprint,
        /// The fingerprint of the registered definition.
        actual: Fingerprint,
        /// The first unused version of the id, to ship the change as.
        next: u64,
    },
    /// The definition has no lock line.
    Unlocked {
        /// The unlocked `id@version`.
        key: FeatureKey,
        /// Its fingerprint, for the lock line to append.
        actual: Fingerprint,
    },
    /// A locked `id@version` is no longer registered.
    Missing {
        /// The locked `id@version`.
        key: FeatureKey,
    },
}

impl fmt::Display for LockError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Changed {
                key,
                locked,
                actual,
                next,
            } => write!(
                f,
                "{key} changed without a version bump (locked {locked}, now {actual}): restore it and add {}@{next}",
                key.id
            ),
            Self::Unlocked { key, actual } => write!(
                f,
                "{key} is not locked: append LockEntry::new(\"{}\", {}, 0x{actual})",
                key.id, key.version
            ),
            Self::Missing { key } => write!(
                f,
                "{key} is locked but not registered: old versions stay in the catalog"
            ),
        }
    }
}

impl std::error::Error for LockError {}

/// Why a feature set was rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeatureSetError {
    /// The key is not registered.
    Unknown {
        /// The unknown key.
        key: FeatureKey,
    },
    /// The id is listed twice.
    DuplicateId {
        /// The repeated id.
        id: FeatureId,
    },
    /// A member's upstream feature is not in the set at the required
    /// version.
    MissingDependency {
        /// The member.
        feature: FeatureKey,
        /// The upstream it requires.
        requires: FeatureKey,
    },
}

impl fmt::Display for FeatureSetError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { key } => write!(f, "{key} is not a registered feature"),
            Self::DuplicateId { id } => write!(f, "{id} is in the set twice"),
            Self::MissingDependency { feature, requires } => {
                write!(f, "{feature} requires {requires}, which is not in the set")
            }
        }
    }
}

impl std::error::Error for FeatureSetError {}

#[cfg(test)]
mod tests {
    use super::*;

    // Upstream keys as consts: `&[Input::Feature(..)]` is promoted to
    // `'static` only without a function call.
    const A1: FeatureKey = FeatureKey::new("a", 1);
    const B1: FeatureKey = FeatureKey::new("b", 1);
    const C1: FeatureKey = FeatureKey::new("c", 1);
    const L1: FeatureKey = FeatureKey::new("l", 1);
    const R1: FeatureKey = FeatureKey::new("r", 1);
    const T1: FeatureKey = FeatureKey::new("t", 1);
    const U7: FeatureKey = FeatureKey::new("u", 7);
    const X1: FeatureKey = FeatureKey::new("x", 1);
    const X2: FeatureKey = FeatureKey::new("x", 2);
    const X3: FeatureKey = FeatureKey::new("x", 3);
    const W1: FeatureKey = FeatureKey::new("w", 1);
    const Y1: FeatureKey = FeatureKey::new("y", 1);
    const Z1: FeatureKey = FeatureKey::new("z", 1);

    const N14: &[Param] = &[Param {
        name: "n",
        value: ParamValue::Int(14),
    }];
    const N20: &[Param] = &[Param {
        name: "n",
        value: ParamValue::Int(20),
    }];
    const TRADES: &[Input] = &[Input::Stream(Stream::Trades)];

    /// `x@1`: one parameter `n = 14` over trades.
    const X_V1: FeatureDefinition = FeatureDefinition {
        key: FeatureKey::new("x", 1),
        params: N14,
        inputs: TRADES,
        warm_up: WarmUp::Samples(14),
    };
    /// `x@1` edited in place: `n = 20`.
    const X_V1_EDITED: FeatureDefinition = FeatureDefinition {
        params: N20,
        ..X_V1
    };
    /// The edit shipped properly as `x@2`.
    const X_V2: FeatureDefinition = FeatureDefinition {
        key: FeatureKey::new("x", 2),
        params: N20,
        ..X_V1
    };
    /// `y@1` built on `x@1`.
    const Y_V1: FeatureDefinition = FeatureDefinition {
        key: FeatureKey::new("y", 1),
        params: &[],
        inputs: &[Input::Feature(X1)],
        warm_up: WarmUp::None,
    };
    /// `y@2`: the same, moved to `x@2`.
    const Y_V2: FeatureDefinition = FeatureDefinition {
        key: FeatureKey::new("y", 2),
        inputs: &[Input::Feature(X2)],
        ..Y_V1
    };

    const ALL: &[&FeatureDefinition] = &[&X_V1, &X_V2, &Y_V1, &Y_V2];

    fn leak(definition: FeatureDefinition) -> &'static FeatureDefinition {
        Box::leak(Box::new(definition))
    }

    /// A copy of `base` with the given key.
    fn with_key(base: &FeatureDefinition, id: &'static str, version: u32) -> FeatureDefinition {
        FeatureDefinition {
            key: FeatureKey::new(id, version),
            ..*base
        }
    }

    fn registry(definitions: &[&'static FeatureDefinition]) -> FeatureRegistry {
        FeatureRegistry::new(definitions).unwrap()
    }

    fn lock_of(definitions: &[&FeatureDefinition]) -> Vec<LockEntry> {
        definitions
            .iter()
            .map(|definition| LockEntry {
                key: definition.key,
                fingerprint: definition.fingerprint(),
            })
            .collect()
    }

    fn set(registry: &FeatureRegistry, keys: &[FeatureKey]) -> FeatureSet {
        FeatureSet::new(registry, keys).unwrap()
    }

    // --- identity -------------------------------------------------------

    #[test]
    fn id_grammar() {
        let longest = "a".repeat(MAX_ID_LEN);
        for valid in [
            "trade.count",
            "bars.1m",
            "flow.cvd_session",
            "x",
            "a_.b_c.9",
            longest.as_str(),
        ] {
            assert!(is_valid_id(valid), "{valid:?}");
        }
        let too_long = "a".repeat(MAX_ID_LEN + 1);
        for invalid in [
            "",
            "Trade",
            "trade.Count",
            ".a",
            "a.",
            "a..b",
            "a@b",
            "a,b",
            "a-b",
            "a b",
            "1a",
            "_a",
            "a.b.",
            "café",
            too_long.as_str(),
        ] {
            assert!(!is_valid_id(invalid), "{invalid:?}");
        }
    }

    #[test]
    #[should_panic(expected = "invalid feature id")]
    fn an_invalid_id_does_not_construct() {
        let _ = FeatureId::new("Bad");
    }

    #[test]
    fn keys_display_and_order_by_id_then_version() {
        assert_eq!(FeatureKey::new("bars.1m", 3).to_string(), "bars.1m@3");
        let mut keys = [
            FeatureKey::new("b", 1),
            FeatureKey::new("a", 2),
            FeatureKey::new("a", 10),
            FeatureKey::new("a", 1),
        ];
        keys.sort();
        assert_eq!(
            keys.map(|key| key.to_string()),
            ["a@1", "a@2", "a@10", "b@1"]
        );
        assert_eq!(FeatureId::new("a.b").as_str(), "a.b");
        assert_eq!(FeatureVersion::new(7).get(), 7);
    }

    // --- registry -------------------------------------------------------

    #[test]
    fn accepts_several_versions_of_one_id() {
        let registry = registry(ALL);
        assert_eq!(registry.get(X_V1.key), Some(&X_V1));
        assert_eq!(registry.get(X_V2.key), Some(&X_V2));
        assert_eq!(registry.get(FeatureKey::new("x", 3)), None);
        let keys: Vec<String> = registry
            .definitions()
            .map(|definition| definition.key.to_string())
            .collect();
        assert_eq!(keys, ["x@1", "x@2", "y@1", "y@2"]);
        // Registration order does not matter.
        let reversed = FeatureRegistry::new(&[&Y_V2, &Y_V1, &X_V2, &X_V1]).unwrap();
        assert!(reversed.definitions().eq(registry.definitions()));
    }

    #[test]
    fn rejects_a_duplicate_key() {
        assert_eq!(
            FeatureRegistry::new(&[&X_V1, &X_V2, &X_V1]).unwrap_err(),
            RegistryError::DuplicateKey { key: X_V1.key }
        );
        // Even with a different body: the same key twice is ambiguous.
        assert_eq!(
            FeatureRegistry::new(&[&X_V1, &X_V1_EDITED]).unwrap_err(),
            RegistryError::DuplicateKey { key: X_V1.key }
        );
    }

    #[test]
    fn rejects_version_zero() {
        let zero = leak(with_key(&X_V1, "x", 0));
        assert_eq!(
            FeatureRegistry::new(&[zero, &X_V1]).unwrap_err(),
            RegistryError::ZeroVersion {
                key: FeatureKey::new("x", 0)
            }
        );
    }

    #[test]
    fn rejects_a_version_gap() {
        let x3 = leak(with_key(&X_V1, "x", 3));
        assert_eq!(
            FeatureRegistry::new(&[&X_V1, x3]).unwrap_err(),
            RegistryError::VersionGap {
                id: FeatureId::new("x"),
                expected: FeatureVersion::new(2),
                found: FeatureVersion::new(3),
            }
        );
        // A deleted first version.
        assert_eq!(
            FeatureRegistry::new(&[&X_V2]).unwrap_err(),
            RegistryError::VersionGap {
                id: FeatureId::new("x"),
                expected: FeatureVersion::new(1),
                found: FeatureVersion::new(2),
            }
        );
        // Contiguity is per id: `z` restarts at 1 after `x@2`.
        let z1 = leak(with_key(&X_V1, "z", 1));
        let z2 = leak(with_key(&X_V1, "z", 2));
        let z2_only = FeatureRegistry::new(&[&X_V1, &X_V2, z2]).unwrap_err();
        assert_eq!(
            z2_only,
            RegistryError::VersionGap {
                id: FeatureId::new("z"),
                expected: FeatureVersion::new(1),
                found: FeatureVersion::new(2),
            }
        );
        FeatureRegistry::new(&[&X_V1, &X_V2, z1, z2]).unwrap();
    }

    #[test]
    fn rejects_unsorted_or_duplicate_params() {
        let unsorted = leak(FeatureDefinition {
            params: &[
                Param {
                    name: "b",
                    value: ParamValue::Int(1),
                },
                Param {
                    name: "a",
                    value: ParamValue::Int(1),
                },
            ],
            ..X_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[unsorted]).unwrap_err(),
            RegistryError::UnsortedParams {
                key: X_V1.key,
                name: "a"
            }
        );
        let duplicate = leak(FeatureDefinition {
            params: &[
                Param {
                    name: "a",
                    value: ParamValue::Int(1),
                },
                Param {
                    name: "a",
                    value: ParamValue::Int(2),
                },
            ],
            ..X_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[duplicate]).unwrap_err(),
            RegistryError::DuplicateParam {
                key: X_V1.key,
                name: "a"
            }
        );
        // Byte order: `period_ms` sorts before `period` is wrong, `n` before
        // `period` is right.
        let sorted = leak(FeatureDefinition {
            params: &[
                Param {
                    name: "n",
                    value: ParamValue::Int(1),
                },
                Param {
                    name: "period",
                    value: ParamValue::Int(1),
                },
                Param {
                    name: "period_ms",
                    value: ParamValue::Int(1),
                },
            ],
            ..X_V1
        });
        FeatureRegistry::new(&[sorted]).unwrap();
    }

    #[test]
    fn rejects_a_duplicate_input() {
        let streams = leak(FeatureDefinition {
            inputs: &[
                Input::Stream(Stream::Trades),
                Input::Stream(Stream::OrderBook),
                Input::Stream(Stream::Trades),
            ],
            ..X_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[streams]).unwrap_err(),
            RegistryError::DuplicateInput {
                key: X_V1.key,
                input: Input::Stream(Stream::Trades),
            }
        );
        let upstream = leak(FeatureDefinition {
            inputs: &[Input::Feature(X1), Input::Feature(X1)],
            ..Y_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[&X_V1, upstream]).unwrap_err(),
            RegistryError::DuplicateInput {
                key: Y_V1.key,
                input: Input::Feature(X_V1.key),
            }
        );
    }

    #[test]
    fn rejects_a_closure_with_two_versions_of_one_id() {
        // Review regression: every registered definition must stay
        // computable, i.e. fit one feature set (ADR-029).
        const XV1: FeatureDefinition = FeatureDefinition {
            key: X1,
            params: &[],
            inputs: &[Input::Stream(Stream::Trades)],
            warm_up: WarmUp::None,
        };
        const XV2: FeatureDefinition = FeatureDefinition {
            key: X2,
            warm_up: WarmUp::Samples(2),
            ..XV1
        };
        const YV1: FeatureDefinition = FeatureDefinition {
            key: Y1,
            params: &[],
            inputs: &[Input::Feature(X1)],
            warm_up: WarmUp::None,
        };
        // w@1 on the old y@1 and the latest x@2: its closure needs x@1 and x@2.
        const WV1: FeatureDefinition = FeatureDefinition {
            key: W1,
            params: &[],
            inputs: &[Input::Feature(Y1), Input::Feature(X2)],
            warm_up: WarmUp::None,
        };
        // z@1 directly on two versions of x.
        const ZV1: FeatureDefinition = FeatureDefinition {
            key: Z1,
            params: &[],
            inputs: &[Input::Feature(X1), Input::Feature(X2)],
            warm_up: WarmUp::None,
        };
        let conflict = |feature| RegistryError::ConflictingVersions {
            feature,
            id: FeatureId::new("x"),
            first: FeatureVersion::new(1),
            second: FeatureVersion::new(2),
        };
        assert_eq!(
            FeatureRegistry::new(&[&XV1, &XV2, &YV1, &WV1, &ZV1]).unwrap_err(),
            conflict(W1)
        );
        assert_eq!(
            FeatureRegistry::new(&[&XV1, &XV2, &YV1, &WV1]).unwrap_err(),
            conflict(W1)
        );
        assert_eq!(
            FeatureRegistry::new(&[&XV1, &XV2, &ZV1]).unwrap_err(),
            conflict(Z1)
        );
        // A version built on an older version of its own id conflicts too.
        let x2_on_x1 = leak(FeatureDefinition {
            inputs: &[Input::Feature(X1)],
            ..XV2
        });
        assert_eq!(
            FeatureRegistry::new(&[&XV1, x2_on_x1]).unwrap_err(),
            RegistryError::ConflictingVersions {
                feature: X2,
                id: FeatureId::new("x"),
                first: FeatureVersion::new(1),
                second: FeatureVersion::new(2),
            }
        );
        // Without the offenders the rest is valid, and every definition's
        // closure builds a feature set.
        let registry = registry(&[&XV1, &XV2, &YV1]);
        for definition in registry.definitions() {
            let closure = registry.closure(definition.key).unwrap();
            FeatureSet::new(&registry, &closure).unwrap();
        }
    }

    #[test]
    fn closures_are_the_smallest_computable_sets() {
        let registry = registry(ALL);
        assert_eq!(registry.closure(X_V1.key), Some(vec![X_V1.key]));
        assert_eq!(registry.closure(Y_V2.key), Some(vec![X_V2.key, Y_V2.key]));
        assert_eq!(registry.closure(FeatureKey::new("x", 3)), None);
        // A diamond: the shared upstream appears once.
        let left = leak(with_key(&Y_V1, "l", 1));
        let right = leak(with_key(&Y_V1, "r", 1));
        let top = leak(FeatureDefinition {
            key: T1,
            inputs: &[Input::Feature(L1), Input::Feature(R1)],
            ..Y_V1
        });
        let diamond = self::registry(&[top, left, right, &X_V1]);
        assert_eq!(diamond.closure(T1), Some(vec![L1, R1, T1, X1]));
        for registry in [&registry, &diamond] {
            for definition in registry.definitions() {
                let closure = registry.closure(definition.key).unwrap();
                let set = FeatureSet::new(registry, &closure).unwrap();
                assert!(set.definitions().any(|member| member.key == definition.key));
                // Dropping any upstream breaks the set.
                for dropped in closure.iter().filter(|key| **key != definition.key) {
                    let rest: Vec<FeatureKey> = closure
                        .iter()
                        .copied()
                        .filter(|key| key != dropped)
                        .collect();
                    assert!(FeatureSet::new(registry, &rest).is_err(), "{dropped}");
                }
            }
        }
    }

    #[test]
    fn rejects_an_unknown_upstream() {
        assert_eq!(
            FeatureRegistry::new(&[&Y_V1]).unwrap_err(),
            RegistryError::UnknownInput {
                feature: Y_V1.key,
                input: X_V1.key,
            }
        );
        // The id is registered, the version is not.
        let y_on_x3 = leak(FeatureDefinition {
            inputs: &[Input::Feature(X3)],
            ..Y_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[&X_V1, &X_V2, y_on_x3]).unwrap_err(),
            RegistryError::UnknownInput {
                feature: Y_V1.key,
                input: FeatureKey::new("x", 3),
            }
        );
    }

    #[test]
    fn rejects_a_dependency_cycle() {
        let selfish = leak(FeatureDefinition {
            inputs: &[Input::Feature(X1)],
            ..X_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[selfish]).unwrap_err(),
            RegistryError::DependencyCycle { key: X_V1.key }
        );
        let a = leak(FeatureDefinition {
            key: FeatureKey::new("a", 1),
            inputs: &[Input::Feature(B1)],
            ..X_V1
        });
        let b = leak(FeatureDefinition {
            key: FeatureKey::new("b", 1),
            inputs: &[Input::Feature(C1)],
            ..X_V1
        });
        let c = leak(FeatureDefinition {
            key: FeatureKey::new("c", 1),
            inputs: &[Input::Stream(Stream::Trades), Input::Feature(A1)],
            ..X_V1
        });
        assert_eq!(
            FeatureRegistry::new(&[a, b, c]).unwrap_err(),
            RegistryError::DependencyCycle { key: a.key }
        );
        // A diamond is not a cycle.
        let left = leak(with_key(&Y_V1, "l", 1));
        let right = leak(with_key(&Y_V1, "r", 1));
        let top = leak(FeatureDefinition {
            key: FeatureKey::new("t", 1),
            inputs: &[Input::Feature(L1), Input::Feature(R1)],
            ..Y_V1
        });
        FeatureRegistry::new(&[top, left, right, &X_V1]).unwrap();
    }

    // --- lock -----------------------------------------------------------

    #[test]
    fn a_parameter_change_without_a_version_bump_is_caught() {
        let lock = lock_of(&[&X_V1]);
        let edited = registry(&[&X_V1_EDITED]);
        assert_eq!(
            edited.verify_lock(&lock).unwrap_err(),
            vec![LockError::Changed {
                key: X_V1.key,
                locked: X_V1.fingerprint(),
                actual: X_V1_EDITED.fingerprint(),
                next: 2,
            }]
        );
        // Shipping the change as x@2 keeps x@1 intact.
        let mut appended = lock.clone();
        appended.extend(lock_of(&[&X_V2]));
        assert_eq!(registry(&[&X_V1, &X_V2]).verify_lock(&appended), Ok(()));
        // Editing x@1 while adding x@2 is still caught.
        assert_eq!(
            registry(&[&X_V1_EDITED, &X_V2])
                .verify_lock(&appended)
                .unwrap_err(),
            vec![LockError::Changed {
                key: X_V1.key,
                locked: X_V1.fingerprint(),
                actual: X_V1_EDITED.fingerprint(),
                next: 3,
            }]
        );
        // The advice names the next free version, not x@2, which exists.
        let errors = registry(&[&X_V1_EDITED, &X_V2])
            .verify_lock(&appended)
            .unwrap_err();
        assert!(errors[0].to_string().ends_with("restore it and add x@3"));
    }

    #[test]
    fn every_kind_of_edit_is_caught_by_the_lock() {
        let lock = lock_of(&[&X_V1]);
        let edits = [
            FeatureDefinition {
                params: &[Param {
                    name: "m",
                    value: ParamValue::Int(14),
                }],
                ..X_V1
            },
            FeatureDefinition {
                inputs: &[Input::Stream(Stream::Klines)],
                ..X_V1
            },
            FeatureDefinition {
                warm_up: WarmUp::Samples(15),
                ..X_V1
            },
        ];
        for edit in edits {
            let edit = leak(edit);
            assert!(
                matches!(
                    registry(&[edit]).verify_lock(&lock).unwrap_err()[..],
                    [LockError::Changed { .. }]
                ),
                "{edit:?}"
            );
        }
    }

    #[test]
    fn unlocked_and_missing_versions_are_caught() {
        let lock = lock_of(&[&X_V1]);
        assert_eq!(
            registry(&[&X_V1, &X_V2]).verify_lock(&lock).unwrap_err(),
            vec![LockError::Unlocked {
                key: X_V2.key,
                actual: X_V2.fingerprint(),
            }]
        );
        // The latest version deleted: still a valid registry, but the lock
        // remembers it.
        let full = lock_of(&[&X_V1, &X_V2]);
        assert_eq!(
            registry(&[&X_V1]).verify_lock(&full).unwrap_err(),
            vec![LockError::Missing { key: X_V2.key }]
        );
        // Every violation is reported: lock order first, then unlocked by key.
        let mixed = vec![
            LockEntry::new("y", 1, 0),
            LockEntry::new("gone", 1, 0),
            LockEntry {
                key: X_V1.key,
                fingerprint: X_V1.fingerprint(),
            },
        ];
        assert_eq!(
            registry(ALL).verify_lock(&mixed).unwrap_err(),
            vec![
                LockError::Changed {
                    key: Y_V1.key,
                    locked: Fingerprint::from_raw(0),
                    actual: Y_V1.fingerprint(),
                    next: 3,
                },
                LockError::Missing {
                    key: FeatureKey::new("gone", 1)
                },
                LockError::Unlocked {
                    key: X_V2.key,
                    actual: X_V2.fingerprint(),
                },
                LockError::Unlocked {
                    key: Y_V2.key,
                    actual: Y_V2.fingerprint(),
                },
            ]
        );
    }

    #[test]
    fn a_second_lock_line_for_a_version_does_not_relock_it() {
        // Appending a new fingerprint for x@1 instead of bumping still fails
        // on the original line.
        let mut lock = lock_of(&[&X_V1]);
        lock.extend(lock_of(&[&X_V1_EDITED]));
        assert_eq!(
            registry(&[&X_V1_EDITED]).verify_lock(&lock).unwrap_err(),
            vec![LockError::Changed {
                key: X_V1.key,
                locked: X_V1.fingerprint(),
                actual: X_V1_EDITED.fingerprint(),
                next: 2,
            }]
        );
    }

    // --- fingerprint ------------------------------------------------------

    /// An independent FNV-1a 64 over raw bytes.
    fn reference_fnv(bytes: &[u8]) -> u64 {
        bytes.iter().fold(0xcbf2_9ce4_8422_2325, |state, &byte| {
            (state ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
        })
    }

    fn push_str(bytes: &mut Vec<u8>, text: &str) {
        bytes.extend(u32::try_from(text.len()).unwrap().to_le_bytes());
        bytes.extend(text.as_bytes());
    }

    #[test]
    fn encoding_v1_byte_layout() {
        const FULL: FeatureDefinition = FeatureDefinition {
            key: FeatureKey::new("f.g", 258),
            params: &[
                Param {
                    name: "a",
                    value: ParamValue::Int(-2),
                },
                Param {
                    name: "b",
                    value: ParamValue::Bool(true),
                },
                Param {
                    name: "c",
                    value: ParamValue::Text("1h"),
                },
                Param {
                    name: "d",
                    value: ParamValue::Price(Price::from_units(1)),
                },
                Param {
                    name: "e",
                    value: ParamValue::Qty(Qty::from_units(2)),
                },
                Param {
                    name: "f",
                    value: ParamValue::Rate(Rate::from_units(-3)),
                },
            ],
            inputs: &[Input::Stream(Stream::Klines), Input::Feature(U7)],
            warm_up: WarmUp::Span { millis: 60_000 },
        };
        let mut bytes = vec![1];
        push_str(&mut bytes, "f.g");
        bytes.extend(258_u32.to_le_bytes());
        bytes.extend(6_u32.to_le_bytes());
        push_str(&mut bytes, "a");
        bytes.push(1);
        bytes.extend((-2_i64).to_le_bytes());
        push_str(&mut bytes, "b");
        bytes.extend([2, 1]);
        push_str(&mut bytes, "c");
        bytes.push(3);
        push_str(&mut bytes, "1h");
        push_str(&mut bytes, "d");
        bytes.push(4);
        bytes.extend(1_i64.to_le_bytes());
        push_str(&mut bytes, "e");
        bytes.push(5);
        bytes.extend(2_i64.to_le_bytes());
        push_str(&mut bytes, "f");
        bytes.push(6);
        bytes.extend((-3_i64).to_le_bytes());
        bytes.extend(2_u32.to_le_bytes());
        bytes.extend([1, 6]);
        bytes.push(2);
        push_str(&mut bytes, "u");
        bytes.extend(7_u32.to_le_bytes());
        bytes.push(2);
        bytes.extend(60_000_u64.to_le_bytes());
        assert_eq!(FULL.fingerprint().value(), reference_fnv(&bytes));

        // The other warm-up encodings.
        let mut none = vec![1];
        push_str(&mut none, "x");
        none.extend(1_u32.to_le_bytes());
        none.extend([0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let x_none = FeatureDefinition {
            key: FeatureKey::new("x", 1),
            params: &[],
            inputs: &[],
            warm_up: WarmUp::None,
        };
        assert_eq!(x_none.fingerprint().value(), reference_fnv(&none));
        let mut samples = none[..none.len() - 1].to_vec();
        samples.push(1);
        samples.extend(14_u32.to_le_bytes());
        let x_samples = FeatureDefinition {
            warm_up: WarmUp::Samples(14),
            ..x_none
        };
        assert_eq!(x_samples.fingerprint().value(), reference_fnv(&samples));
    }

    #[test]
    fn stream_ordinals_in_the_encoding_are_frozen() {
        // ADR-028 freezes these; the encoding relies on them.
        let ordinals = [
            (Stream::Trades, 0),
            (Stream::OrderBook, 1),
            (Stream::Liquidations, 2),
            (Stream::MarkPrice, 3),
            (Stream::Funding, 4),
            (Stream::OpenInterest, 5),
            (Stream::Klines, 6),
        ];
        for (stream, ordinal) in ordinals {
            assert_eq!(stream as u8, ordinal, "{stream:?}");
        }
    }

    #[test]
    fn every_field_changes_the_fingerprint() {
        let base = X_V1;
        let variants = [
            with_key(&base, "x2", 1),
            with_key(&base, "x", 2),
            FeatureDefinition {
                params: &[Param {
                    name: "m",
                    value: ParamValue::Int(14),
                }],
                ..base
            },
            FeatureDefinition {
                params: N20,
                ..base
            },
            FeatureDefinition {
                params: &[],
                ..base
            },
            FeatureDefinition {
                params: &[
                    Param {
                        name: "n",
                        value: ParamValue::Int(14),
                    },
                    Param {
                        name: "o",
                        value: ParamValue::Int(14),
                    },
                ],
                ..base
            },
            FeatureDefinition {
                inputs: &[Input::Stream(Stream::OrderBook)],
                ..base
            },
            FeatureDefinition {
                inputs: &[],
                ..base
            },
            FeatureDefinition {
                inputs: &[Input::Stream(Stream::Trades), Input::Stream(Stream::Klines)],
                ..base
            },
            FeatureDefinition {
                inputs: &[Input::Stream(Stream::Klines), Input::Stream(Stream::Trades)],
                ..base
            },
            FeatureDefinition {
                warm_up: WarmUp::Samples(15),
                ..base
            },
            FeatureDefinition {
                warm_up: WarmUp::None,
                ..base
            },
            FeatureDefinition {
                warm_up: WarmUp::Span { millis: 14 },
                ..base
            },
        ];
        let mut fingerprints = vec![base.fingerprint()];
        fingerprints.extend(variants.iter().map(FeatureDefinition::fingerprint));
        let mut distinct = fingerprints.clone();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), fingerprints.len(), "{fingerprints:?}");
    }

    #[test]
    fn param_types_and_values_change_the_fingerprint() {
        let one = |value| FeatureDefinition {
            params: leak_params(value),
            ..X_V1
        };
        let values = [
            ParamValue::Int(1),
            ParamValue::Int(-1),
            ParamValue::Int(0),
            ParamValue::Bool(true),
            ParamValue::Bool(false),
            ParamValue::Text("1"),
            ParamValue::Text(""),
            ParamValue::Text("5m"),
            ParamValue::Price(Price::from_units(1)),
            ParamValue::Qty(Qty::from_units(1)),
            ParamValue::Rate(Rate::from_units(1)),
            ParamValue::Price(Price::from_units(0)),
        ];
        let mut fingerprints: Vec<Fingerprint> = values
            .iter()
            .map(|&value| one(value).fingerprint())
            .collect();
        fingerprints.sort();
        fingerprints.dedup();
        assert_eq!(fingerprints.len(), values.len());
        // In particular the plan's case: an Int of 1 is not a Qty of 1 unit.
        assert_ne!(
            one(ParamValue::Int(1)).fingerprint(),
            one(ParamValue::Qty(Qty::from_units(1))).fingerprint()
        );
    }

    fn leak_params(value: ParamValue) -> &'static [Param] {
        Box::leak(Box::new([Param { name: "n", value }]))
    }

    #[test]
    fn an_upstream_version_changes_the_fingerprint() {
        assert_ne!(Y_V1.fingerprint(), Y_V2.fingerprint());
        let same_key = FeatureDefinition {
            key: Y_V1.key,
            ..Y_V2
        };
        assert_ne!(Y_V1.fingerprint(), same_key.fingerprint());
        // An upstream feature is not a stream with the same tag position.
        let on_stream = FeatureDefinition {
            inputs: TRADES,
            ..Y_V1
        };
        assert_ne!(Y_V1.fingerprint(), on_stream.fingerprint());
    }

    #[test]
    fn equal_definitions_fingerprint_equally() {
        let params = vec![Param {
            name: "n",
            value: ParamValue::Int(14),
        }];
        let rebuilt = FeatureDefinition {
            key: FeatureKey::new("x", 1),
            params: Box::leak(params.into_boxed_slice()),
            inputs: Box::leak(vec![Input::Stream(Stream::Trades)].into_boxed_slice()),
            warm_up: WarmUp::Samples(14),
        };
        assert_eq!(rebuilt, X_V1);
        assert_eq!(rebuilt.fingerprint(), X_V1.fingerprint());
    }

    #[test]
    fn synthetic_definition_fingerprints_are_pinned() {
        // Guards encoding v1 against accidental change.
        assert_eq!(X_V1.fingerprint().to_string(), "1ed1fea3b729f102");
        assert_eq!(Y_V2.fingerprint().to_string(), "68d4e5aa766af708");
    }

    // --- feature set ------------------------------------------------------

    #[test]
    fn a_set_holds_one_version_per_id() {
        let registry = registry(ALL);
        assert_eq!(
            FeatureSet::new(&registry, &[X_V1.key, X_V2.key]).unwrap_err(),
            FeatureSetError::DuplicateId {
                id: FeatureId::new("x")
            }
        );
        assert_eq!(
            FeatureSet::new(&registry, &[X_V1.key, X_V1.key]).unwrap_err(),
            FeatureSetError::DuplicateId {
                id: FeatureId::new("x")
            }
        );
    }

    #[test]
    fn a_set_rejects_an_unknown_key() {
        let registry = registry(ALL);
        assert_eq!(
            FeatureSet::new(&registry, &[X_V1.key, FeatureKey::new("x", 3)]).unwrap_err(),
            FeatureSetError::Unknown {
                key: FeatureKey::new("x", 3)
            }
        );
    }

    #[test]
    fn a_set_is_closed_over_exact_upstream_versions() {
        let registry = registry(ALL);
        assert_eq!(
            FeatureSet::new(&registry, &[Y_V1.key]).unwrap_err(),
            FeatureSetError::MissingDependency {
                feature: Y_V1.key,
                requires: X_V1.key,
            }
        );
        assert_eq!(
            FeatureSet::new(&registry, &[Y_V1.key, X_V2.key]).unwrap_err(),
            FeatureSetError::MissingDependency {
                feature: Y_V1.key,
                requires: X_V1.key,
            }
        );
        set(&registry, &[Y_V1.key, X_V1.key]);
        set(&registry, &[Y_V2.key, X_V2.key]);
        set(&registry, &[X_V2.key]);
    }

    #[test]
    fn the_set_version_ignores_key_order() {
        let registry = registry(ALL);
        let forward = set(&registry, &[X_V1.key, Y_V1.key]);
        let backward = set(&registry, &[Y_V1.key, X_V1.key]);
        assert_eq!(forward.version(), backward.version());
        assert_eq!(forward, backward);
        assert_eq!(forward.to_string(), "x@1,y@1");
        assert_eq!(backward.to_string(), "x@1,y@1");
        let members: Vec<FeatureKey> = backward.definitions().map(|d| d.key).collect();
        assert_eq!(members, [X_V1.key, Y_V1.key]);
    }

    #[test]
    fn the_set_version_tracks_every_member() {
        let registry = registry(ALL);
        let versions = [
            set(&registry, &[]),
            set(&registry, &[X_V1.key]),
            set(&registry, &[X_V2.key]),
            set(&registry, &[X_V1.key, Y_V1.key]),
            set(&registry, &[X_V2.key, Y_V2.key]),
        ]
        .map(|set| set.version());
        let mut distinct = versions.to_vec();
        distinct.sort();
        distinct.dedup();
        assert_eq!(distinct.len(), versions.len());

        // Same keys, different definition content (two registries): the
        // set version still differs, because it covers the fingerprints.
        let original = set(&registry, &[X_V1.key]);
        let edited = set(&self::registry(&[&X_V1_EDITED]), &[X_V1.key]);
        assert_eq!(original.to_string(), edited.to_string());
        assert_ne!(original.version(), edited.version());
    }

    #[test]
    fn the_set_version_is_pinned() {
        // Guards the set encoding v1 against accidental change.
        let registry = registry(ALL);
        let pinned = set(&registry, &[Y_V2.key, X_V2.key]);
        assert_eq!(pinned.to_string(), "x@2,y@2");
        assert_eq!(pinned.version().to_string(), "ee0772ad6c4b0cdb");
        assert_eq!(set(&registry, &[]).to_string(), "");
        assert_eq!(
            FeatureSetVersion::from_fingerprint(pinned.version().fingerprint()),
            pinned.version()
        );
    }

    #[test]
    fn the_set_version_follows_the_documented_layout() {
        let registry = registry(ALL);
        let pinned = set(&registry, &[X_V2.key, Y_V2.key]);
        let mut bytes = vec![1];
        bytes.extend(2_u32.to_le_bytes());
        for definition in [&X_V2, &Y_V2] {
            push_str(&mut bytes, definition.key.id.as_str());
            bytes.extend(definition.key.version.get().to_le_bytes());
            bytes.extend(definition.fingerprint().value().to_le_bytes());
        }
        assert_eq!(
            pinned.version().fingerprint().value(),
            reference_fnv(&bytes)
        );
    }

    // --- values -----------------------------------------------------------

    #[test]
    fn only_ready_values_are_readable() {
        let warming: FeatureValue<i64> = FeatureValue::WarmingUp {
            observed: 3,
            required: 14,
        };
        let unavailable: FeatureValue<i64> = FeatureValue::Unavailable {
            reason: Unavailability::InputInvalid,
        };
        let ready = FeatureValue::Ready(7_i64);
        assert_eq!(warming.ready(), None);
        assert!(!warming.is_ready());
        assert_eq!(unavailable.ready(), None);
        assert!(!unavailable.is_ready());
        assert_eq!(ready.ready(), Some(&7));
        assert!(ready.is_ready());
    }

    #[test]
    fn map_keeps_the_validity() {
        let double = |value: i64| value * 2;
        assert_eq!(
            FeatureValue::Ready(7_i64).map(double),
            FeatureValue::Ready(14)
        );
        assert_eq!(
            FeatureValue::<i64>::WarmingUp {
                observed: 3,
                required: 14
            }
            .map(double),
            FeatureValue::WarmingUp {
                observed: 3,
                required: 14
            }
        );
        for reason in [Unavailability::InputInvalid, Unavailability::OutOfRange] {
            assert_eq!(
                FeatureValue::<i64>::Unavailable { reason }.map(double),
                FeatureValue::Unavailable { reason }
            );
        }
        // The mapping is not called outside Ready.
        let unavailable = FeatureValue::<i64>::Unavailable {
            reason: Unavailability::OutOfRange,
        };
        let mapped = unavailable.map(|_| -> i64 { unreachable!("not ready") });
        assert!(!mapped.is_ready());
    }

    // --- resolve ----------------------------------------------------------

    #[test]
    fn resolves_the_canonical_text() {
        let registry = registry(ALL);
        for definition in ALL {
            let text = definition.key.to_string();
            assert_eq!(registry.resolve(&text), Ok(*definition), "{text}");
        }
        let multi = leak(with_key(&X_V1, "bars.1m", 1));
        let registry = self::registry(&[multi]);
        assert_eq!(registry.resolve("bars.1m@1"), Ok(multi));
    }

    #[test]
    fn rejects_malformed_or_unknown_text() {
        let registry = registry(ALL);
        let malformed = |text: &str| ResolveError::Malformed {
            text: text.to_owned(),
        };
        for text in [
            "",
            "x",
            "x@",
            "@1",
            "x@0",
            "x@a",
            "x@01",
            "x@+1",
            "x@-1",
            "x@ 1",
            "x@1 ",
            "X@1",
            "x@1@1",
            "x@1,y@1",
            "x@4294967296",
        ] {
            assert_eq!(registry.resolve(text), Err(malformed(text)), "{text:?}");
        }
        for text in ["unknown@1", "x@3", "x@4294967295"] {
            assert_eq!(
                registry.resolve(text),
                Err(ResolveError::Unknown {
                    text: text.to_owned()
                }),
                "{text:?}"
            );
        }
    }

    // --- messages ---------------------------------------------------------

    #[test]
    fn errors_describe_themselves() {
        let key = X_V1.key;
        assert_eq!(
            LockError::Changed {
                key,
                locked: Fingerprint::from_raw(1),
                actual: Fingerprint::from_raw(2),
                next: 2,
            }
            .to_string(),
            "x@1 changed without a version bump (locked 0000000000000001, now 0000000000000002): restore it and add x@2"
        );
        assert_eq!(
            RegistryError::ConflictingVersions {
                feature: FeatureKey::new("w", 1),
                id: key.id,
                first: FeatureVersion::new(1),
                second: FeatureVersion::new(2),
            }
            .to_string(),
            "w@1 needs both x@1 and x@2 through its upstream features, \
             so no feature set can compute it; build it on one version of x"
        );
        assert_eq!(
            LockError::Unlocked {
                key,
                actual: Fingerprint::from_raw(0xab),
            }
            .to_string(),
            "x@1 is not locked: append LockEntry::new(\"x\", 1, 0x00000000000000ab)"
        );
        assert_eq!(
            LockError::Missing { key }.to_string(),
            "x@1 is locked but not registered: old versions stay in the catalog"
        );
        assert_eq!(
            RegistryError::VersionGap {
                id: key.id,
                expected: FeatureVersion::new(2),
                found: FeatureVersion::new(3),
            }
            .to_string(),
            "x: expected version 2, found 3; old versions are never deleted"
        );
        assert_eq!(
            RegistryError::DuplicateInput {
                key,
                input: Input::Stream(Stream::Trades),
            }
            .to_string(),
            "x@1: stream Trades appears twice"
        );
        assert_eq!(
            FeatureSetError::MissingDependency {
                feature: Y_V1.key,
                requires: key,
            }
            .to_string(),
            "y@1 requires x@1, which is not in the set"
        );
        assert_eq!(
            ResolveError::Malformed {
                text: "x@".to_owned()
            }
            .to_string(),
            "\"x@\" is not of the form id@N"
        );
        // Every variant has a message.
        let registry_errors = [
            RegistryError::DuplicateKey { key },
            RegistryError::ZeroVersion { key },
            RegistryError::UnsortedParams { key, name: "a" },
            RegistryError::DuplicateParam { key, name: "a" },
            RegistryError::UnknownInput {
                feature: key,
                input: key,
            },
            RegistryError::DependencyCycle { key },
        ];
        for error in registry_errors {
            assert!(error.to_string().starts_with("x@1"), "{error}");
        }
        for error in [
            FeatureSetError::Unknown { key },
            FeatureSetError::DuplicateId { id: key.id },
        ] {
            assert!(error.to_string().starts_with('x'), "{error}");
        }
        assert_eq!(
            ResolveError::Unknown {
                text: "x@9".to_owned()
            }
            .to_string(),
            "x@9 is not a registered feature"
        );
    }
}
