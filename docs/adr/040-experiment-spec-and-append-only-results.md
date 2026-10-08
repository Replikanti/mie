# ADR-040: Experiment specification, identity and append-only results

- Status: proposed
- Date: 2026-10-09

## Context

#29 makes every research claim traceable: a formal experiment spec goes in,
an immutable result with provenance comes out. The Strategy Research Lab
brief lists the fields of an experiment (Market State filters, regime
filters, location, trigger, entry, invalidation, target/management, fees,
slippage, funding, latency assumptions, sample period, data version,
feature version). ADR-007 asks for reproducible research, ADR-020 lets
agents propose but only the deterministic pipeline write results, and
ADR-029 D7 records an experiment's feature-set version with its canonical
list.

Three things had no decision yet:

- **A spec format** that humans and agents can write, that validates with
  clear errors and that the core can parse. The core takes no dependencies
  (ADR-025), so serde is not available there.
- **An identity** for "the same experiment" that is stable across
  processes, machines and Rust releases.
- **A result store** that never overwrites, and a first pipeline that can
  prove reproduction before the backtest (#30) exists.

Most slots of a spec — location (#23), trigger (#24), Market State filters
(#25), entry, invalidation, target, slippage and funding (#30) — have their
semantics decided by later issues. The spec must name them now without
deciding them.

## Decision

1. **Spec text v1 (D1).** An experiment is a line-based text, owned by
   `mie_domain::research` (grammar in its module docs): the header
   `mie-experiment 1`, then one `<key> <args>` line per field, arguments
   separated by single spaces, printable ASCII, `\n` line endings; blank and
   `#` lines are ignored and keys may come in any order. Fields required
   exactly once: `hypothesis`, `sample`, `data`, `features`, `location`,
   `trigger`, `entry`, `invalidation`, `target`, `fees`, `slippage`,
   `funding`, `latency`. `state-filter` and `regime-filter` are required at
   least once; `none` alone means "explicitly unfiltered", `none` next to
   filters and a repeated filter line are errors. The parser reports every
   error in line order, then every missing field; an error names the field
   and its category (a missing `fees`, `slippage` or `funding` is a missing
   *cost assumption*, a missing `latency` a missing *latency assumption*).
2. **The text is the identity (D2).** The canonical text renders the header,
   then the fields in a fixed order, filter lines sorted bytewise,
   parameters sorted by name, decimals in their 8-place form (ADR-027) and
   regime labels in `LOW`…`EXTREME` order; no comments, no blank lines.
   `ExperimentId` is FNV-1a 64 of `Fingerprinter::write_str(canonical
   text)` (ADR-029 writers), displayed as 16 hex digits. Extension rule, as
   in ADR-029: a later field is a new line kind, rendered only when it
   differs from its default, so every existing id stays valid.
3. **Opaque rule references (D3).** Slots whose semantics later issues own
   are `id@N` references with typed parameters `name=type:value` (`int`,
   `bool`, `text`, `price`, `qty`, `rate`, `feature`). The id and version
   follow the feature-key grammar. They are validated for syntax and for
   feature membership only: a `feature:` parameter must be a registered
   feature in the spec's own set. Rule ids are not resolved; #23/#24/#25/#30
   add registries without touching spec text v1.
4. **The spec's own feature set (D4).** `features <version> <list|none>`
   resolves through the `FeatureRegistry` into a `FeatureSet`, so it must be
   closed over its upstream features, and the declared version must equal
   the computed `FeatureSetVersion`. A run checks that the engine computes
   every member at the same key and definition — a subset check, so adding
   features to the default set never orphans an old spec. A spec on a
   feature version the engine no longer computes fails with
   `FeatureUnavailable` until `MarketStateEngine::with_features` lands
   (ADR-029).
5. **Data version (D5).** `data` is the raw store's dataset version
   (ADR-030). The domain mirrors it as `DataVersion`; the ports convert
   `DatasetVersion` into it, because ADR-030 keeps `DatasetVersion` in the
   ports. A run whose replay opens other raw data fails with
   `DataVersionMismatch` before anything is stored.
6. **Results keyed by experiment and pipeline (D6).** A result is
   `ExperimentResult { spec, pipeline, outcome }`, keyed by
   `(experiment id, pipeline id@N)`. A new pipeline version records next to
   the old result; the same key is never rewritten. Result text v1 is
   `mie-result 1`, `experiment`, `pipeline`, `outcome <kind>` and the kind's
   lines, then `spec` and the canonical spec text verbatim. Outcome kinds
   are tagged; a new kind adds a tag and never changes the lines of an
   existing one. Parsing is strict: an `experiment` line that is not the
   embedded spec's id, or any non-canonical text, does not parse.
7. **Provenance-only first pipeline (D7).** `research.replay_summary@1`
   (`mie_app::ExperimentService`) replays the sample through the domain
   with the live rejection policy (ADR-039 D9) and records the event-stream
   hash (ADR-039 D7) and the domain-rejection count. A run whose result is
   already stored compares it: identical is `Reproduced`, different is
   `Diverged` and nothing is written; a new result is appended and
   `Recorded`. An empty sample fails. #30 adds the backtest pipeline and
   its outcome kind.
8. **Ports (D8).** `ResearchResultReader` (`get`, `by_hypothesis`) and
   `ResearchResultStore: ResearchResultReader` (`append`) are outbound
   ports; `RunResearchExperiment` is the inbound port. The store contract:
   never replace, never delete; a second append of a key is
   `AlreadyStored { identical }`, a different spec under a stored
   experiment id is `Collision`. ADR-020 holds structurally: only `mie-cli`
   hands the writable store to `ExperimentService`; readers get
   `ResearchResultReader` only.
9. **Append-only file adapter (D9).** `mie-adapter-fs` stores
   `<root>/<experiment hex>/<pipeline id>@<N>.result`: the result text and
   the trailer line `sha256 <64 hex>` over every byte before it. A write
   goes to a dot-prefixed temporary file (`create_new`, synced, made
   read-only) that is hard-linked to the final name; a link never replaces
   a name, so of two writers of one key exactly one wins. Reads verify the
   trailer (`Integrity`), parse the text and check that the file sits at
   its own key's path (`Corrupt`). Dot-files and unknown names are ignored,
   never deleted.
10. **CLI (D10).** `mie experiment validate <spec>` prints the id and the
    canonical text, or every error; `mie experiment run <spec> --config …
    --results <dir> [--source live|archive] [--streams …]` prints
    `recorded <key>` or `reproduced <key>`, or `FAIL: <reason>` and exits 1.

## Consequences

- A re-run of a spec on the same data version reproduces the stored result
  byte for byte, and a stored result cannot be overwritten — the store
  refuses, and the files are read-only.
- Spec text v1, result text v1 and the id become history once results
  exist. Golden texts and pinned ids guard them; any change is a new
  version and a new ADR.
- The rule language may be too thin for #30. It can grow by new parameter
  types or line kinds without touching v1; anything that needs to change
  existing lines is a spec text v2.
- FNV-1a 64 can collide. The store compares the stored spec text and
  returns `Collision`; it never overwrites. The SHA-256 trailer, not the
  id, is the tamper evidence.
- A feature version bump of a used feature cannot be re-run until
  `MarketStateEngine::with_features` lands; the error names the feature.
- Results are not type-sealed across crates: the ADR-020 guarantee is the
  wiring in `mie-cli`. #34 adds a no-write-path test for the agent adapter.
- Hard links are unsupported on some network and FUSE filesystems; the
  store then fails with `Io` instead of falling back to a racy
  check-then-rename.
- The `replay_summary` outcome carries no research metric: it proves
  provenance and reproduction, not edge.

## Alternatives considered

- **A TOML or JSON spec parsed with serde in `mie-cli`** — familiar, but
  the core cannot take serde (ADR-025), and the identity would depend on an
  adapter's serializer and its canonicalization.
- **Parquet results in `mie-adapter-parquet`** — one-row files whose bytes
  depend on the writer library rule out byte-exact reproduction checks.
- **Results keyed by experiment id alone** — a new pipeline version would
  have to overwrite or be refused.
- **The engine's whole default set as the spec's feature version** — every
  feature added to the default set would orphan every existing spec.
- **Store only, AC 2 deferred to #30** — leaves reproduction unproven until
  the backtest exists; the provenance pipeline proves it now.
- **Resolving rule ids now** — would decide the semantics of #23/#24/#25/#30
  ahead of their issues.

## Accept when

The backtest pipeline (#30) records its results through this store with an
unchanged spec text v1, and a spec run twice on the 12-month archive
backfill prints `recorded` then `reproduced` with a byte-identical result
file.
