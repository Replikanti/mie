# ADR-029: Feature identity, versioning and the feature-set version

- Status: accepted
- Date: 2026-10-06

## Context

Every derived feature must be reproducible from versioned raw data plus a
versioned feature definition (ADR-003, ADR-022), and a feature earns its
place by measured contribution (ADR-013). Measurements only stay comparable
if the definition behind a number is known exactly, after definitions have
evolved:

- The Market State & Regime brief requires deterministic definitions for all
  features. The Strategy Research Lab brief records the feature version in
  every experiment (#29).
- Eight feature issues (#15–#22) build on this: bars, volatility, order flow,
  the order book, OI/funding, volume profile, structure, candidates. Features
  build on each other (volatility on bars), so a change upstream changes
  everything downstream.
- Indicators with a lookback (ATR percentile(200)) are not valid until the
  lookback is filled, and some features have no valid value at all while
  their inputs are broken (an unsynced order book, #18). Research must never
  consume such a value silently.
- The core is pure and dependency-free (ADR-025), parameter values are exact
  (ADR-027), and live and replay share one domain path (ADR-019).

## Decision

1. **Identity.** A feature is `id@version`. The id follows
   `[a-z][a-z0-9_]*(\.[a-z0-9_]+)*`, at most 64 bytes, shaped
   `<family>.<name>[.<qualifier>]`; ids are never renamed or reused. A
   version is an integer from 1, contiguous per id. Parameterizations that
   must coexist (ATR on 5m and on 1h) get distinct ids.
2. **Definition.** `FeatureDefinition` holds the key, exact parameters
   (`Int`, `Bool`, `Text`, `Price`, `Qty`, `Rate` — no floats; a time span
   is an `Int` named `*_ms`) sorted by name, inputs (raw `Stream`s or
   upstream features at exact versions) and the warm-up (`None`,
   `Samples(n)`, `Span { millis }`).
3. **Immutability.** A published `id@version` never changes. Every definition
   ever shipped stays in `mie_domain::feature::catalog::DEFINITIONS` and stays
   computable; a change ships as `id@version+1`. The registry rejects
   duplicate keys, version 0, version gaps (a deleted old version), unsorted
   or repeated parameters, repeated inputs, unknown upstream features,
   dependency cycles, and a definition whose dependency closure (itself and
   its upstream features, transitively) needs one id at two versions — no
   feature set could compute it, so it would not stay computable. A test
   builds every catalog definition into the feature set of its own closure.
4. **Lock.** `catalog::LOCK` is an append-only table of definition
   fingerprints, one line per `id@version`. `cargo test` checks the catalog
   against it: an edited definition fails as *changed without a version
   bump* (the message names the next free version), a new one as *unlocked*
   (the message prints the line to append), a deleted one as *missing*.
   Deleting a version together with its lock line would pass those checks,
   so the line count and a digest of the whole `LOCK` table are pinned by a
   test as well: appending a line updates the pins in the same PR, and any
   other change to them is a visible, reviewable deletion or edit.
5. **Definition encoding v1.** The fingerprint is FNV-1a 64 (offset basis
   `0xcbf29ce484222325`, prime `0x100000001b3`) over explicit writers:
   `u8`; `u32`, `u64`, `i64` little-endian, fixed width; `str` as a `u32`
   byte length then the UTF-8 bytes; counts as `u32`. In order:
   - the encoding version `1u8`;
   - the id, then the version;
   - the parameter count, then per parameter its name, a type tag (`Int` 1,
     `Bool` 2, `Text` 3, `Price` 4, `Qty` 5, `Rate` 6) and the value
     (`i64` units for the integer and fixed-point types, `u8` 0/1 for
     `Bool`, `str` for `Text`);
   - the input count, then per input a tag and value: `Stream` 1 plus the
     stream ordinal (`u8`, frozen by ADR-028), or `Feature` 2 plus the
     upstream id and version;
   - the warm-up tag and value: `None` 0; `Samples` 1 plus `u32`; `Span` 2
     plus milliseconds as `u64`.

   No fingerprint goes through `std::hash`, `Debug` output or enum
   discriminants that no ADR freezes. **Extension rule:** a field added
   later is appended with its own tag, and only when it differs from its
   default, so existing fingerprints stay valid.
6. **Behaviour the fingerprint cannot see** — the code that computes a
   version — is pinned by one golden-output test per version on a fixed
   tape. The lock catches definition drift, the golden test catches
   implementation drift.
7. **Feature set.** `FeatureSet` is the set of features a Market State
   computes: one version per id, closed over its upstream features at
   exactly the required versions. Its `FeatureSetVersion` is FNV-1a 64 over
   set encoding v1: `1u8`, the member count, then per member sorted by id
   its id, version and definition fingerprint (`u64`). It does not depend on
   the order the keys were given in and changes with any member's content.
   `MarketState.feature_set` carries it; `Display` of the set prints the
   canonical list `id@N,id@N`, which experiments record next to the digest
   and `FeatureRegistry::resolve` parses back. The engine computes
   `catalog::CURRENT`, the latest version of each default feature.
8. **Validity.** A value is `FeatureValue<T>`: `WarmingUp { observed,
   required }`, `Ready(T)`, or `Unavailable { reason }` for inputs that are
   structurally unable to produce a value (`InputInvalid`, `OutOfRange`; #18
   may add reasons). Only `Ready` exposes a value; there is no `Default`.
   After a gap a feature goes back to `WarmingUp` according to the gap
   policy its definition documents.
9. The convention for adding a feature is the checklist "Adding a feature"
   in the `mie_domain::feature` module docs. The first feature is
   `trade.last_price@1`, the skeleton's last trade price.

## Consequences

- Any number produced by the system can name the exact definitions behind
  it, and an old experiment re-runs with the set it recorded.
- One lock line per version, and a failing test until it is added. Editing a
  definition in place fails CI instead of silently changing history.
- The old versions' code paths stay in the codebase for as long as an
  experiment may need them; that cost grows with every bump.
- A 64-bit non-cryptographic fingerprint separates a few hundred definitions
  safely, but it is identity, not tamper evidence: it does not resist a
  deliberately chosen collision.
- The lock catches an edited definition only while its lock line stays.
  Editing or deleting a lock line together with the definition fails the
  pinned lock digest, but a PR can update that pin too; until a CI guard
  keeps `LOCK` append-only against the merge base, review of the pin change
  catches that.
- The fingerprint covers the declaration, not the computation. A change to
  the computing code without a definition change is caught only by the
  golden-output tests.
- Choosing an older feature set for replay (`MarketStateEngine::with_features`)
  lands with the first feature that ships an `@2`; until then the engine
  always computes `CURRENT`.
- `MarketState.trade_count` stays a diagnostic counter, not a feature: it
  depends on where consumption started.

## Alternatives considered

- **Semver versions.** No change to a feature's output is "compatible", and
  minor or patch bumps invite silent drift. Rejected.
- **The content hash as the version.** Self-maintaining, but an opaque
  version is unreadable in experiment logs and cannot express "the next
  version". The hash is kept as the lock, the integer as the version.
- **`#[derive(Hash)]` / `DefaultHasher`.** SipHash output and derived
  encodings are not stable across Rust releases. Rejected.
- **SHA-256 in the core.** Tamper-resistant, but needs a hand-written
  implementation or a dependency in the pure core (ADR-025) for a threat the
  lock does not face. Rejected for now.
- **Compile-time `const` pins next to each definition.** Fails the build
  instead of a test, but needs a `const fn` fingerprint and puts the pin next
  to the code that is edited with it, so both change together. The separate
  append-only table makes such an edit visible as a lock change in review.
- **A CI git-diff guard keeping `LOCK` append-only.** Closes the "edit the
  lock line and its pin too" gap; deferred until real features exist. The
  pinned lock digest makes every such edit a visible pin change in review
  meanwhile.
- **Two-state validity (`WarmingUp` / `Ready` only).** Rejected at plan
  review: #18 needs structural unavailability, and adding a variant later
  would break every feature's `match`.

## Accept when

#15 registers bars and #16 a volatility feature without changing encoding
v1, or the first `@2` ships with its `@1` still computable.
