# ADR-057: Conditional outcome dataset — long-format Parquet with a content hash, DuckDB views over it

- Status: proposed
- Date: 2026-10-28

## Context

#27 asks for the first milestone's conditional outcome dataset: the context
records of ADR-054 and their forward outcome labels (ADR-055, labeling v2 of
ADR-056) as versioned Parquet, queryable in DuckDB (ADR-004) through
reusable views. It has two acceptance criteria: a rebuild from the same
inputs gives an identical dataset by content hash, and the views return the
brief §12 outputs for a backfilled period.

Several things are left open by the ADRs this builds on, and each is a
design decision this ADR takes:

- **Format.** ADR-054 D5 and ADR-055 D10 deliver provenance once per run
  and say #27 "copies it into its dataset rows" and "adds the code version".
  Neither fixes the table shape, the column types, the encoding of a
  censored or missing value, or how a dataset is identified.
- **Identity.** ADR-030's dataset version identifies raw data. A derived
  dataset needs its own identity that a rebuild can reproduce.
- **Compositions.** ADR-055 D8 leaves "revisited within H" and "extreme
  reached before the opposite barrier" to "#27's views", and which side
  counts as invalidation to the hypothesis (ADR-014).
- **Source.** ADR-054 *Out of scope* leaves the history-chain driver, which
  would need a sampler reset at every `drive_chain` restart, to "#27's
  choice of source".
- **Engine.** ADR-030 rejected ZSTD for "a C build and a BSD-3-Clause
  license". A pinned DuckDB brings a much larger C++ build with vendored
  BSD-3-Clause code (zstd, snappy, re2). That reasoning needs revisiting,
  not silent overriding.
- **Reading.** ADR-055 *Accept when* holds upper-vs-lower shares, quantiles
  and splits by trigger, regime or location until #27/#28 and the
  calibration slice of `docs/direction.md` are fixed. The views compute
  exactly those, so their acceptance has to be blind.

The issue ships in three steps: **M1** the writer and `mie conditional
build|verify` (D1–D6, this ADR as proposed); **M2** the
`mie-adapter-duckdb` crate with the views (D7, D8); **M3** the acceptance
run on the #12 year and the R0 live store (D9), which flips this ADR.

## Decision

### Approach

- The writer lives in `mie-adapter-parquet` as the `outcome` module. That
  crate already owns the pinned Arrow/Parquet stack, SHA-256 and the seal
  protocol of ADR-030, so the writer adds no dependency and no C++ build.
- `mie-cli` (the composition root) feeds it from the record and label
  closures of `mie_app::conditional::label_window_started`. That function
  is `label_window` plus an `on_start` callback that receives the run's
  `LabelProvenance` after the replay opens and before the first event, so
  the writer has the provenance before row 1. `label_window` delegates to
  it, so its callers do not change.
- No port is added: a port in `mie-ports` that named `ForwardLabel` would
  need an exception to the ADR-055 D11 guard, and the app's closures
  already are the sink (ADR-054 D6). The guard is unchanged: the only core
  file touched is the app's labeling driver, which it already allows.
- The views (M2) live in `mie-adapter-duckdb`, an adapter over the Parquet
  files with no internal dependency, which links a pinned, bundled DuckDB,
  so view results come from one engine version recorded in `Cargo.lock`.

### D1. Tables (long format) and columns

A dataset is one directory with three tables and a manifest (D3):

| Table | Rows | Order |
|---|---|---|
| `contexts.parquet` | one per context record | record order |
| `outcomes.parquet` | one per record × horizon (5 per record) | record order, then `HORIZONS` order |
| `passages.parquet` | one per present `level` or `extreme` passage | record order, `level` before `extreme` |

- **Join key:** `record` (UInt64), the record's 0-based position in the
  record stream. The ADR-055 D1 key stays on `contexts` for replay
  reproduction: `decision_time`, `event_kind`, `event_seq`, `ordinal`
  (computed by the writer as the labeler computes it: 0 for the first
  record of an event, +1 for each further record of the same key).
- **`contexts`:** `record`, the D1 key, then
  - `sample` (`baseline`|`trigger`), `minute_end` (baseline only),
    `trigger` (the trigger's feature key text), `trigger_family`;
  - `episode` (the `EpisodeId` text), `opener`, `truncated`, `bridge`
    (trigger only);
  - `regime` (the bias's regime label when ready) and `regime_percentile`
    (Float64, the `AtrPercentile` value, bit for bit);
  - `levels_ready`, `at_value_edge`, `at_structural_extreme`: ADR-051 R1's
    location bins, read as the context measurement reads them (a VAH or
    VAL among the levels at the anchor; a structural high or low); the two
    bins are NULL while the levels are not ready, and both false means
    `neither`;
  - `auction_state` (when ready), `anchor`, `reference`
    (`state.last_price`), `atr_1h` (`state.atr`).
- **`outcomes`:** `record`, `horizon` (`5m` … `1d`), `horizon_ms`, `clear`,
  `lower`, `upper` (the barriers; NULL when the race is unavailable),
  `race` (`upper`|`lower`|`neither`|`censored`|`unavailable`), `race_at`
  and `same_ms` (upper and lower only), `race_censor_at` and
  `race_censor_reason` (censored only), `up`, `up_at`, `down`, `down_at`
  (the excursions; `*_at` NULL when the size is 0), `last`, `censor_at`
  and `censor_reason` (the window's censor).
- **`passages`:** `record`, `target` (`level`|`extreme`), `price`,
  `passage` (`at_decision`|`touched`|`not_within`|`censored`), `at`
  (touched only), `censor_at`, `censor_reason` (censored only).
- **Provenance:** every table ends with the six run-provenance columns of
  ADR-054 D5 / ADR-055 D10: `data_version`, `engine`, `sampling`,
  `sampling_fingerprint`, `labeling`, `labeling_fingerprint`. They are
  constant within a dataset and dictionary-encoded by Parquet, so a table
  read alone still names its run.
- **A label's reference and ATR** repeat its record's
  (`state.last_price`, `state.atr`; ADR-055 D2). They are stored once, on
  `contexts`, and the writer refuses a label whose values differ, so the
  three tables rebuild every `ForwardLabel` exactly (unit-tested).
- **Not stored: the order-flow snapshot** (aggression, CVD, effort,
  liquidity, OI). ADR-054 D2's own rule applies: a field earns its place
  through a consumer that reads it, and no #27 view does. #28, its
  consumer, adds a table and bumps the format version.
- *Why long:* every view groups by horizon. A wide row with 5 × 13 horizon
  columns would make every view repeat its SQL per horizon or unpivot.
- *Why `record` and not the D1 key as the join:* one integer column joins
  in DuckDB without a composite key, and the D1 key is still there to find
  the event in a replay.

### D2. Encoding

- **Prices** are `Decimal128(18, 8)`: scale 8 is the ADR-027 unit, so every
  value is exact; precision 18 is the largest Parquet stores as a plain
  INT64 (asserted by a test on the physical type). A value beyond
  ±999 999 999 999 999 999 units is a writer error (`OutcomeError::Range`),
  never rounded.
- **Times** are `Timestamp(ms, "UTC")`, event time in ms (ADR-028).
- **Enums** are snake_case text from exhaustive `match`es (or the domain's
  own `as_str`/`label`, which are), never `Debug`; each name table is
  pinned by a test. Regime labels are stored lower case (`low` …
  `extreme`), like every other enum here.
- **Codec** `LZ4_RAW`: pure Rust and already compiled for the raw store
  (ADR-030), and read by DuckDB.
- **Row groups of at most 122 880 rows.** This is DuckDB's own row-group
  size and the unit its Parquet scan hands to one thread, verified in the
  DuckDB sources bundled by `libduckdb-sys` 1.20000.0 (source id
  `d54cbbe3df`):
  - `src/include/duckdb/storage/storage_info.hpp`:
    `#define DEFAULT_ROW_GROUP_SIZE 122880ULL`;
  - `extension/parquet/parquet_extension.cpp`: its Parquet writer defaults
    to `row_group_size = DEFAULT_ROW_GROUP_SIZE`;
  - `extension/parquet/parquet_multi_file_info.cpp`:
    `ParquetReader::TryInitializeScan` gives each scanning thread the next
    row group, and `MaxThreads` of a single file is its row-group count.
  On the #12 year this gives `contexts` 5 row groups (561 695 rows) and
  `outcomes` 23 (2 808 475 rows).
- **Key-value metadata** `mie.outcome.format=1` on every table.
- The encoder is fed batches of 8 192 rows per table.

### D3. Manifest and content hash

`dataset.manifest` is UTF-8 with `\n` line ends, lines in this fixed order:

```text
mie-outcome-dataset 1
source <archive|live> <source>/<instrument> streams <list> correction <fold|none>
replay <start_ms> <end_ms>
records <start_ms> <end_ms>
data <64 hex>
engine <engine key>
sampling <id@N> <16 hex>
labeling <id@N> <16 hex>
events <n> rejections <n>
file contexts.parquet <rows> <sha256>
file outcomes.parquet <rows> <sha256>
file passages.parquet <rows> <sha256>
content <sha256>
code <code version>
```

- `source` for an archive build names the archive streams replayed, sorted
  by name, and the `aggTrades` correction (ADR-050). A live build replays
  the streams its journaled runs captured, which the run journal fixes
  and the data version already pins, and has nothing to correct:
  `streams journaled correction none`.
- `data` is the ADR-030 data version of the replay, `engine` the ADR-041
  engine key, `sampling` and `labeling` the policy keys and fingerprints
  (ADR-054 D5, ADR-055 D10, ADR-056 D10′).
- `content` is the SHA-256 of every manifest byte before its own line.
  Through the `file` lines it covers every table byte.
- **The reproducibility criterion (#27 criterion 1):** two builds whose
  manifests agree on every line before `content` write byte-identical
  tables and the same `content`. Those lines are the raw data version, the
  windows, the source and stream selection, the engine key and the
  sampling and labeling fingerprints: everything the rows are a function
  of. Asserted in CI on a fixture store (M1) and on the #12 year and the R0
  live store (M3).
- **The code version is deliberately outside the hash.** A rebuild on
  another commit that changes no versioned rule must reproduce the hash, so
  cross-commit reproduction can be checked at all. A hash change without a
  version change then exposes an unversioned semantic change or a Parquet
  encoder change (ADR-030's accepted consequence: the encoder writes its
  own version into the footer), and the `code` line names the build that
  explains it.
- The parser is strict: it accepts exactly what the writer renders, and
  `mie conditional verify` recomputes the content hash, each file's
  SHA-256, its row count from the Parquet footer, its format metadata and
  schema, and checks that `outcomes` has five rows per `contexts` row.

### D4. Code version

`crates/mie-cli/build.rs` embeds `MIE_CODE_VERSION`:

- `git:<40 hex>` from `git rev-parse HEAD`, with `-dirty` when
  `git status --porcelain --untracked-files=no` lists a tracked file;
- `unknown` outside a git checkout, without git, or when the workspace
  root is not the top level of the repository git finds
  (`git rev-parse --show-toplevel`): a source tree without its own `.git`,
  extracted or vendored inside another repository, would otherwise name
  that repository's commit and dirty state.
- `git status` runs with `GIT_OPTIONAL_LOCKS=0`, so a build never rewrites
  the index of the checkout it builds.
- The script reruns when `HEAD`, its reflog `logs/HEAD`, the ref it
  points at or `packed-refs` changes (located with `git rev-parse
  --git-path`, which resolves a linked worktree's own `HEAD`), or anything
  under `crates/`, `Cargo.toml` or `Cargo.lock`. The reflog is what sees a
  commit whose branch ref lived only in `packed-refs` (after `git gc` or
  `git pack-refs`) or in a reftable: no loose ref file existed to watch,
  and every commit, checkout and reset appends to it. A docs-only edit after the last build is not noticed
  until the next code edit; the flag is about the code that built the
  rows, which those paths hold.

### D5. Write protocol

- `--out` is created with `create_dir`, which fails if it exists: a
  dataset is never overwritten. Before the replay opens, the build also
  refuses an existing `--out` and a parent that is not an existing
  directory, so a typo fails at once instead of after the whole run.
- Each table is written under a hidden temp name (`.<table>.tmp`), then
  fsynced, made read-only and renamed without replacing; the directory is
  fsynced.
- The manifest is written last, the same way. A directory is a dataset if
  and only if `dataset.manifest` exists, as for an ADR-030 part.
- The labeling run cannot be stopped from its sink, so a write error is
  kept, later writes are skipped, and the build fails at the end without a
  manifest. A label that does not match the oldest unlabeled record (labels
  come in record order, ADR-055 D1), a label without a record, or a record
  still unlabeled at the end is a contract error. A run with no record in
  the records window fails too: a dataset of nothing is a mistaken window,
  not a result.

### D6. Sources and windows

- **`archive` and `live`**, through `label_window_started` under
  `Labeling::CURRENT` (ADR-056 D10′: v2).
- **The history chain is refused.** A `drive_chain` restart after an
  uncrossable hole starts a fresh engine (ADR-052 D4). The sampler's
  episodes and the labeler's windows, bars and scales would then have to
  restart with it, and that reset is a design of its own (ADR-054 *Out of
  scope* left it to this issue). The archive alone covers the development
  year, and the live store covers the R0 runs; a chain is not needed for
  M3.
- **Windows.** `--from`/`--to` is the records window. The replay window
  defaults to it; `--replay-from`/`--replay-to` widen it for warm-up and
  lookahead. No lookahead constant is built in: labels whose window the
  data ends inside are censored `end_of_data` explicitly (ADR-055 D6), and
  a researcher who wants full 1d labels for the last records widens the
  replay by a day.

### D7. Views (M2)

The views are `crates/mie-adapter-duckdb/src/views.sql`, created over one
verified dataset (`OutcomeDataset::open` before any query). They read the
dataset directory from the DuckDB variable `mie_dataset`, so the same file
serves any DuckDB client of the pinned version (`mie conditional views`
prints it).

- **Base views** `contexts`, `outcomes`, `passages` over the files.
- **`labeled`** (record × horizon) joins `contexts`, `outcomes` and the
  `level` and `extreme` passages, and adds:
  - `group_trigger` (the trigger id, or `baseline`), `group_family` (the
    trigger family, or `baseline`), `group_regime` (the regime label, or
    `not_ready`);
  - `location_bins`, a list over `value_edge`, `structural_extreme`,
    `neither` (levels ready, in no bin), `not_ready`;
  - `offset = upper − reference`;
  - `up_x`, `down_x` = excursion ÷ offset, both in price units, so the
    ratio is one IEEE division of two integers. NULL when the race is
    unavailable or the window is censored: those excursions are lower
    bounds (`HorizonLabel::censored`, ADR-055 D6/D7);
  - `time_to_event_ms = race_at − decision_time` for `upper`/`lower`;
  - `level_within` and `extreme_vs_opposite`, the two compositions below.
- All times are compared in milliseconds against the window end
  E = `decision_time + H`, exclusive like the window (ADR-055 D2).

**`level_within`** (the ADR-055 D8 composition "revisited within H"), from
the level's passage, which is read over the 1d window:

| Level passage | `level_within` at H |
|---|---|
| none | NULL |
| `at_decision` | `at_decision` |
| `touched` at a < E | `touched` |
| `touched` at a ≥ E | `not_within` |
| `not_within` | `not_within` |
| `censored` at c ≥ E | `not_within` (not reached strictly before c, so not before E) |
| `censored` at c < E | `unknown` |

**`extreme_vs_opposite`**, a neutral name, because which side counts as
invalidation is the hypothesis's call (ADR-014). It is read from first
touches only: the race stores its winner's first touch, the passage the
extreme's. The opposite barrier is the one across the reference from the
extreme. Write te for the extreme's touch, ce for its censor, t for the
race's touch, c for the race's censor:

| Race | Extreme passage | `extreme_vs_opposite` |
|---|---|---|
| any | none | NULL |
| `unavailable` | any | NULL |
| any other | `at_decision` | `extreme_first` |
| won by the opposite barrier at t | `touched`, te < t | `extreme_first` |
| | `touched`, te = t (same ms, order unknown) | `undetermined` |
| | `touched`, te > t; `not_within` | `opposite_first` |
| | `censored`, ce > t | `opposite_first` (not reached up to ce > t) |
| | `censored`, ce ≤ t | `unknown` (the censor hides whether it came first) |
| won on the extreme's side at t | `touched`, te < t | `extreme_first` |
| | `touched`, te = t and not `same_ms` | `extreme_first` (the opposite barrier was not touched in that ms, nor before) |
| | `touched`, te = t with `same_ms`; te > t; `not_within`; `censored` | `undetermined`: the opposite barrier's first touch after t is not stored |
| `neither` | `touched`, te < E | `extreme_first` |
| | `censored`, ce < E | `unknown` (a labeler does not produce it: a censor before E censors the race too; defined for completeness) |
| | otherwise | `neither` |
| `censored` at c | `touched`, te < c | `extreme_first` (neither barrier was touched before c) |
| | otherwise | `unknown` |

**`outcome_summary(n_min)`**, a table macro with no default for `n_min`:

- It groups by horizon × every combination of {trigger id, regime,
  location}: 8 grouping sets. A dimension a row does not split by reads
  `all`; the column `split` names the dimensions it does (`all`,
  `trigger`, `regime`, `location`, `trigger,regime`, …), and `family`
  repeats the trigger id's family.
- The location sets count one row per bin of the record, the others one
  row per record: both come from one `GROUP BY GROUPING SETS` over the
  union of the plain rows (location `all`) and the unnested bins, with
  location in every set. A record in both bins counts in both location
  groups (ADR-051 R1).
- A family branch (`split` = `family`) pools the trigger ids of one family
  and reports only `samples` and `events` (distinct decisions, so a
  co-emission of two ids counts once); every other column is NULL: a
  family-pooled independent count is undefined (ADR-054 *Consequences*).
  Baselines have no family row; their `trigger` row is the baseline.
- Columns: `samples`, `events` (distinct decision keys), `episodes`
  (distinct episode ids; 0 for baselines), `clear`,
  `meets_min_sample = clear >= n_min`; the counts `n_upper`, `n_lower`,
  `n_neither`, `n_censored`, `n_unavailable`, `n_same_ms`; the shares of
  `upper`, `lower` and `neither` among the races that ran (upper + lower +
  neither, NULL without any); the five-number summaries (min, p25, p50,
  p75, max) of `time_to_event_ms`, `up_x` and `down_x` with the count used
  (`tte_*`, `up_x_*`, `down_x_*`); the `level_within` counts
  (`level_at_decision`, `level_touched`, `level_not_within`,
  `level_unknown`) and the `extreme_vs_opposite` counts (`extreme_first`,
  `opposite_first`, `extreme_neither`, `extreme_undetermined`,
  `extreme_unknown`).
- Rows are ordered by `horizon_ms`, `split`, `trigger`, `family`,
  `regime`, `location`.
- **The minimum-sample flag counts per-horizon `clear` records** (ADR-055
  D9), the only count whose windows are disjoint; a nominal sample count
  over overlapping windows must not be used as a sample size (ADR-054
  *Consequences*). The threshold is a query parameter without a default:
  it belongs to the study (#28), and any default here would be a number
  without a derivation. In a rollup over trigger ids (`trigger` = `all`),
  `clear` is the sum of each stream's clear records, which are disjoint
  within a stream only.
- **Quantiles** are nearest-rank, `list_sort(list(x))[greatest(1,
  ceil(q · n))]`, the repo's convention (the context measurement): an
  observed value, no interpolation.
- **Aggregates are order-independent** (counts, distinct counts, sorted
  lists, nearest-rank). No floating-point sums or means, so results do not
  depend on thread scheduling. Every view ends with `ORDER BY` its key
  columns, and `mie conditional query` orders by them again.
- **Output.** `mie conditional query <dir> <view> [--n-min <n>]` prints TSV
  with a header line: `NULL`, `true`/`false`, integers, doubles in Rust's
  shortest round-trip form, decimals exactly at scale 8, times as epoch
  milliseconds, lists as `[a,b]`.
- Brief §12 mapping: threshold first → `race`, `n_upper`/`n_lower` and
  their shares; MFE/MAE → `up`/`down` and `up_x`/`down_x`; time-to-event →
  `time_to_event_ms` and `tte_*`; level revisit → `level_within`;
  invalidation → `extreme_vs_opposite`; excursion distribution → the
  five-number summaries; conditional on regime, location and trigger → the
  grouping sets. A test pins both schemas and this mapping.

### D8. The DuckDB engine (M2)

- `duckdb = { version = "=1.20000.0", default-features = false, features =
  ["bundled", "parquet"] }`: the crate bundles the DuckDB sources above
  (version string `v2.0.0-dev88614`) and uses `arrow 60`, the workspace's
  major. Pinned exactly, so every view result comes from one engine.
- Not enabled: `download-lib` (network at build time) and `json` (raw-store
  `json_extract` research is out of scope).
- Each connection turns `autoinstall_known_extensions` and
  `autoload_known_extensions` off in its open configuration, before its
  first statement: the bundled build compiles both on, which would mean
  network at query time. A test asserts that a query needing an unlinked
  extension (`json`) fails with DuckDB's catalog error instead.
- The dataset path reaches DuckDB as a string literal with `'` doubled. A
  path holding `*`, `?` or `[` is refused: `read_parquet` would expand it
  as a glob and could read other files than the verified ones.
- **Revisiting ADR-030's C-build argument.** ADR-030 rejected ZSTD because
  the gain (smaller JSON payloads) did not pay for a C build and a new
  license. Here the C++ build is the engine ADR-004 chose; there is no
  pure-Rust DuckDB. The writer stays pure Rust (D2), so the raw store and
  the dataset bytes never depend on the C++ code; only view results do.
- **Licenses.** DuckDB is MIT. `cargo deny check` sees only the Rust
  crates; of the new ones it demands a single exception, `foldhash`
  (Zlib: duckdb → hashlink → hashbrown → foldhash), which `deny.toml`
  records with its reason. The C++ the build compiles or includes, read
  from the headers of the sources `libduckdb-sys` 1.20000.0 bundles
  (`third_party/`):
  - MIT: brotli, fast_float, fmt, fsst, httplib, jaro_winkler, miniz,
    skiplist, utf8proc, vergesort, yyjson;
  - BSD-2-Clause: lz4; simplified BSD: concurrentqueue;
  - BSD-3-Clause: hyperloglog, re2, snappy, zstd (zstd dual GPL-2.0, used
    under BSD);
  - Apache-2.0: fastpforlib, the Thrift runtime and the Parquet Thrift
    types, tdigest; mbedtls (Apache-2.0 OR GPL-2.0-or-later, used under
    Apache-2.0); pcg (Apache-2.0 OR MIT);
  - Zlib: pdqsort; Boost-1.0: ska_sort.
  All are permissive; none is copyleft as used. Distributing the `mie`
  binary carries their notices, as DuckDB's own distribution does.
- **The dev-profile build** (`[profile.dev.package.libduckdb-sys]`):
  `debug = false` keeps DWARF out of the C++ objects and makes the build
  script define `NDEBUG`, so DuckDB's debug-only verification paths stay
  off in tests; `opt-level = 1`, chosen from a measurement with four cores
  (`-j4` under a 400 % CPU quota, the CI runner's 4 vCPUs):

  | opt-level | DuckDB alone, cold | rebuilt under the mie-cli tests | static library | DuckDB-bound view test |
  |---|---|---|---|---|
  | 0 | 790 s | — | 701 MB | 92 s |
  | 1 | — | 1 016 s | 162 MB | 59 s |
  | 2 | 1 225 s | 1 518 s | 163 MB | 50 s |

  1 builds 502 s faster than 2 under the same conditions with nearly its
  query speed; against 0 it cuts the library every CI cache entry and
  every test binary carries from 701 to 162 MB and the DuckDB-bound test
  by a third. Release builds keep the release profile (opt-level 3).
- **Build cost and the CI timeouts.** Every binary that links `mie-cli`
  links DuckDB whole (the bundled build links its extension autoregister
  object whole-archive), and a cold cache compiles DuckDB twice per job:
  the clippy (check) build and the test build each compile and run its
  build script, as for every crate with one (`ring` too). Measured cold on
  the same four cores, from an empty `target/`:

  | Job | Steps | Time | `target/` |
  |---|---|---|---|
  | CI `check` | clippy 1 105 s + test 1 638 s (build 1 377 s, tests 261 s) | 2 743 s (45.7 min) | 13 GB |
  | Release `build` | test, as above, + `cargo build --release` 1 979 s | about 3 617 s (60.3 min) | 14 GB |
  | CI `check`, warm dependency cache | clippy 16 s + test 415 s | 431 s | — |

  The warm row is what `rust-cache` usually restores (dependencies, never
  the workspace crates); the cold rows are what a lockfile change, a new
  toolchain or an evicted cache costs. Provisional timeouts, until the
  runner's speed against these measurements is calibrated on this PR's CI
  runs: `check` 60 min (45.7 min with 30 % headroom, rounded up to 5 min,
  from 15) and release `build` 80 min (60.3 min likewise, from 20).

### D9. Blind acceptance (M3)

ADR-055 *Accept when* holds outcome reading until #27/#28 and the
calibration slice of `docs/direction.md` are fixed. The acceptance proves
the dataset and the views without reading an outcome:

- The #12 development year is built **twice** into two directories, from a
  clean commit (no `-dirty`), with `--source archive --streams
  aggTrades,metrics --trade-correction fold`, records = replay =
  2025-10-01 … 2026-09-30.
- Before anything is read, the provenance must be `data 5d9e27f8…` and
  `engine 4c844593779da2e3-1`, those of the ADR-054 and ADR-056
  measurements. If either differs, the run stops and reports: the
  reconciliation below would be void.
- The same build twice on the R0 live store.
- `verify` on all four, then the ignored tool `measure_outcome_views` (M2)
  on the year. It asserts:
  - equal manifests and `content` for each pair;
  - 0 row mismatches between the views and an independent Rust model of
    `labeled` and `outcome_summary`, and equal view digests with DuckDB at
    1 and at 4 threads;
  - the reconciliation with published counts: 561 695 contexts
    (525 599 baseline + 36 096 trigger, ADR-054 *Acceptance*), 2 808 475
    outcomes, and per horizon the `neither`, `unavailable`,
    `end_of_data`-censored and clear baseline/trigger counts of the
    ADR-056 *Measurement* table.
- It prints only those counts, plus a row count and SHA-256 digest per
  view. No upper/lower share, quantile or per-context outcome is printed;
  the print lines are review items.

### D10. Where each number comes from

| Number | Source | What breaks at another value |
|---|---|---|
| Format version 1 (manifest header, `mie.outcome.format`) | The first format; #28's order-flow table is the next version (D1) | — (a label, bumped on any column or line change) |
| Three tables; five `outcomes` rows per record | D1: one table per grain; `HORIZONS` has five entries (ADR-055 D3) | Two tables (passages folded into contexts): ten nullable passage columns on every baseline row. Another horizon count is another labeling version, not this format |
| `record` from 0 | D1: the position in the record stream, so the first record is row 0 of `contexts` | From 1: off by one against the row index every reader uses |
| Price scale 8 | ADR-027: the price unit is 1e-8 USDT | Below 8: prices rounded, labels no longer rebuildable; above 8: wider numbers for digits that are always 0 |
| Price precision 18; limit ±999 999 999 999 999 999 units (about ±10¹⁰ USDT) | The Parquet decimal encoding: precision ≤ 18 is stored as INT64, the largest plain integer type | 19 or more: FIXED_LEN_BYTE_ARRAY, larger files and slower decoding for headroom no BTCUSDT price or excursion needs (about 10⁵ USDT) |
| 122 880 rows per row group | D2: DuckDB's `DEFAULT_ROW_GROUP_SIZE` and Parquet write default, and the per-thread unit of its Parquet scan, in the bundled sources | parquet-rs's default 1 048 576: `outcomes` has 3 row groups on the year and `contexts` 1, so DuckDB scans them on at most 3 and 1 threads, and the writer buffers 8.5 times more rows. ADR-030's 65 536: 43 row groups for `outcomes`, twice the footer metadata, no gain in parallelism on the 4 to 12 threads this runs on |
| 8 192 rows per encoder batch | A multiple of parquet-rs's 1 024-row write batch, so the encoder's mini-batches split it evenly; bounds the row buffer to a few MB per table | Any change may move page boundaries and so the bytes and the content hash; like a Parquet bump, the `code` line explains it. Much larger: a larger buffer per table for no gain |
| 64 / 16 / 40 hex | SHA-256 (ADR-030), FNV-1a 64 fingerprints (ADR-029), git commit ids (SHA-1) | — (fixed by the hash functions) |
| `LZ4_RAW` | D2: already compiled, pure Rust (ADR-030); DuckDB reads it | ZSTD: smaller files, but a C build in the writer and its bytes then depend on a C library |
| 8 grouping sets | D7: every subset of {trigger id, regime, location}, 2³ | Fewer: a §12 split ("conditional on location and trigger") is missing; the family rollup is a separate branch, not a set |
| p25 / p50 / p75 with min and max | D7: the five-number summary, the shortest description of a distribution's location, spread and range without a parametric assumption | Means or SDs: float sums, order-dependent under threads (D7); more quantiles: more to read before #28 fixes what is read |
| `n_min`: no default | D7: the threshold is #28's | Any default: a number without a derivation |
| `duckdb =1.20000.0` | D8: the release whose `arrow` major (60) is the workspace's | Another version: other view results possible, and another `arrow` major in the lock |
| 1 and 4 threads | D9: the single-threaded reference and the CI runner's vCPUs | — (two values suffice to expose order dependence) |
| 561 695 / 525 599 / 36 096 / 2 808 475 | D9: ADR-054 *Acceptance* and ADR-056 *Measurement* on the same data and engine; 2 808 475 = 5 × 561 695 | — (published counts; a difference is a bug or a provenance change, checked first) |
| Data `5d9e27f8…`, engine `4c844593779da2e3-1` | D9: the provenance of those published counts | — (a different value voids the reconciliation) |
| `opt-level = 1`, `debug = false` for `libduckdb-sys` in the dev profile | D8: the measured build times, library sizes and view-test times at 0, 1 and 2 | 0: a 701 MB library in every cache entry and test binary and a third slower view tests; 2: 502 s more per DuckDB build for 9 s less test time; `debug = true`: DWARF in every object and DuckDB's debug verification paths in every test |
| CI `check` 60 min, release `build` 80 min | D8: the cold jobs measured on four cores, with headroom for the runner | Below a cold job's time: every lockfile change, toolchain update or cache eviction fails CI by timeout, as the old 15 and 20 min would; far above: a hung job holds a runner longer before it fails |
| About 80 min per year build | ADR-056 *Measurement*: the same replay and labeling took 1:20:17 | — (an expectation for scheduling M3, not a criterion) |

There is no other number.

### Out of scope

- A history-chain source (the restart-aware reset, D6).
- Order-flow snapshot columns (#28 extends the format).
- Reading or publishing outcome rates, intervals, bootstrap, and the T1
  threshold value (#28, `docs/direction.md`).
- Labeling v1 datasets and a `--labeling` flag: `Labeling::CURRENT` only.
- Views across several datasets.
- DuckDB `json`/`httpfs` and raw-store SQL research.
- Live egress (#38) and ClickHouse (brief §20).
- Any change to the engine, catalog, sampler, labeler or state hash.

## Consequences

- Records and labels are still not Market State: no feature, catalog,
  lock, state-hash or golden changes, and the ADR-055 D11 guard is
  unchanged. `label_window_started` is an addition to `mie-app`.
- A year dataset is three files of about 0.56 M, 2.8 M and (triggers only)
  well under 0.1 M rows; it streams to disk, and the writer holds at most
  one day of unlabeled records (the labeler's own bound, ADR-055) plus one
  row group per table.
- A Parquet encoder bump changes the bytes and the content hash, as for the
  raw store (ADR-030). The `code` line explains such a change; M1's tests
  pin determinism within one build.
- An archive and a live dataset over overlapping time differ in their
  `source` and `data` lines, and in the liquidity-dependent context fields
  (ADR-054 *Consequences*); they are different datasets.
- M2 adds a C++ engine to every build that links `mie-cli`: build time,
  disk and the vendored-license list of D8 are its costs.
- A failed build leaves an unfinished directory (hidden temp files, no
  manifest). `verify` reports it as incomplete; it is never mistaken for a
  dataset, and the user removes it.

## Alternatives considered

- **Writing the tables with DuckDB `COPY … TO parquet`.** Puts the C++
  build into the writer, and the file bytes then depend on the DuckDB
  version and its threaded write order. Rejected: the content hash would
  not be reproducible across thread counts without extra settings, and the
  raw-data side of the system would depend on C++.
- **Views as `.sql` files for an external `duckdb` CLI.** CI would need a
  network install, and the engine version would not be pinned, so view
  results could change with the user's DuckDB. Rejected.
- **DataFusion or polars.** Contradicts ADR-004. Rejected.
- **A port in `mie-ports` that names `ForwardLabel`.** Needs an exception
  to the label guard; the app's closures already are the sink. Rejected.
- **The code version inside the rows or the hash.** Every commit would
  yield a new identity, and cross-commit reproduction could not be checked.
  Rejected (D3).
- **A wide outcome row** (5 × 13 horizon columns). Every view would repeat
  its SQL per horizon or unpivot. Rejected (D1).
- **Opening the replay twice in the CLI** to learn the data version before
  creating the writer. Doubles the opening cost and opens a window in which
  the two openings could see different data. Rejected for `on_start`.
- **Storing the order-flow snapshot now.** No #27 view reads it (ADR-054
  D2's rule). Deferred to #28.

## Accept when

M3, in the main session, from the merge commit of M2:

- the four builds of D9 pass, each pair has equal manifests and content
  hashes, and `verify` passes on all four;
- the provenance check of D9 holds before anything is read;
- `measure_outcome_views` passes: 0 model mismatches, equal digests at 1
  and 4 threads, and the reconciliation counts of D9;
- the printed lines contain no outcome share, quantile or per-context
  outcome.

The acceptance PR fills in an *Acceptance* section and sets this ADR to
accepted. Every criterion is reachable: the #12 year is imported, and the
R0 live store exists.
