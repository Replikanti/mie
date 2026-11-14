# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the version is below 1.0, a breaking change bumps the minor version and
is marked **Breaking:** under `Changed`. Releases are cut as described in
[`docs/release.md`](docs/release.md).

## [Unreleased]

### Added

- **Storage tiers, step 9: the hot restore (#195, ADR-062 D7, D11,
  proposed).** `mie store restore --config <ingest config>
  [--archive-config <path>] [--record-lost] [--skip-unreadable-journal]`
  restores a lost hot disk from
  the copy targets before `mie ingest` or the importer may start. It
  refuses an intact hot (damage on an intact hot is #217), a missing
  `raw_root`, a root without its marker and roots on one device, and takes
  the copy-ledger, pass and verify locks and every source's writer lock
  without waiting. It restores the journal from the longest copy (copies
  that differ beyond their common prefix stop it; a cut last line gets its
  `\n`; a journal copy that cannot be read and is longer than every
  readable one stops it; `--skip-unreadable-journal` leaves that copy
  out, moved aside and never deleted, with a `FAIL:` line), the import
  ledger from the targets (differing entries decided by
  the manifests, else held; a surviving hot copy of another variant moved
  into `.mie/damaged/`), and the copy ledger by the rebuild's derivation,
  with hot's surviving ledger as the first start candidate and `unlisted`
  holds written without a flag. It then copies back every hot-window
  partition and every partition not recorded removed, then each path of a
  removed partition that no copy target recorded, verifying each file: a
  damaged copy is passed over and named (for `mie store verify`, then
  `mie store repair` when it is a recorded copy on a copy target), and a
  file with no good copy stays recorded so every read of it fails. A file a record names that no root holds stops
  the restore before any write unless `--record-lost`. A path that no
  record names and that cold roots hold in any form is counted: a sealed
  copy (a read-only root's too) or the verified pending manifest and data
  a crashed copy leaves is recorded and copied back from those forms, and
  debris with no text (data or a data temp file alone, a manifest or
  pending manifest that does not parse or cannot be read, a pending
  manifest whose data fails, a repair's evidence) is a lost candidate
  behind `--record-lost`; after a path's copy-back, an unrecorded copy on
  a copy target that the next pass would refuse, and temp names that are
  no verifiable copy, are moved into `.mie/damaged/`, never deleted, so
  the next pass copies the path from hot. A file that cannot be read never
  stops it: it is a damaged copy, named with a `FAIL:` line. So a new
  capture never reuses a path a cold root holds.
  `mie store rebuild` applies the same rule to every path hot lacks, so a
  rebuild after a restore keeps it counted, a recorded path whose roots
  hold only unverified debris stays a lost candidate, and a path recorded
  lost whose roots hold only debris is a report line, no longer a failure
  on every rebuild. Hot's marker is written last, so running the command
  again completes a restore that stopped or crashed; `mie ingest`, the
  importer and `mie store init` name that re-run when `.mie/` exists
  without a marker. Data of a cold copy is
  read only when the ledger does not record it. `mie store repair` and
  `mie store resolve` no longer offer the restore for damage on an intact
  hot and name #217 instead.

- **Storage tiers, step 8b: the replace into quarantine (#211, ADR-062
  D5, D7, D11, proposed).** `mie store resolve --file <archive path>
  --replace` resolves held archive parts whose rows differ from the
  published file, or that the operator decides to replace, without any
  download. It prints the cost first (every moved path with its stream,
  date, event range, rows and SHA-256, and every journaled warm run whose
  prefix covers one, by run id and `prefix_dataset`), then writes a
  supersede record naming the quarantine id `q-<time>` and the moved
  paths, records each path `quarantined` (with its `manifest` when the
  copy ledger has none), renames it on every root into
  `.quarantine/<id>/<path>` (cold roots first, hot last, nothing ever
  deleted) and clears the holds. The moved set is the whole superseded
  generation: the held parts and every other part of the entry they
  supersede that a root holds; a `manifest-conflict` part can be
  replaced. Until the re-import's entry exists every selection covering
  a moved path fails. `mie archive-import` refuses while a replace is
  incomplete, reads the store through an import view that skips the
  quarantined parts no longer on hot, numbers the re-imported parts above
  them, and appends one `replaced` record per moved path once its new
  entry and every part it lists are sealed on hot (also at its next
  start, after a crash). A replace that stopped is completed by `mie
  store resolve --file <archive path>`; without `--file` the command names
  every incomplete replace. A re-admit that finds a difference prints the
  replace's cost and command and still writes nothing. `mie store
  rebuild` reads every root's `.quarantine/` and re-derives the
  `quarantined`, `hold-cleared … replace` and `replaced` records, so a
  rebuild from a full, a stale or an empty start gives the same
  selections (AW12). A read-only cold root does not count toward a
  replace's completion: one whose layout still holds a moved path, for
  example a disk configured after the replace, halts no import, and `mie
  store resolve` and `mie store rebuild` name it in an info line. A new
  replace no longer refuses when a read-only root holds the file, unless
  only read-only roots hold a moved path's data file; the resume refuses,
  and `mie store resolve` and `mie store rebuild` fail, when only
  read-only roots would keep a moved path's data outside quarantine
  (#216).
- **Storage tiers, step 8a: `mie store resolve` (#210, ADR-062 D11,
  proposed).** `mie store resolve --config <ingest config>
  [--archive-config <path>] [--file <archive path>]` resolves held
  archive parts by re-admitting them. Without `--file` it prints the
  archive file each open hold proposes, by the catalog's period rules
  (the `metrics` 23:55 row belongs to the previous day's file,
  `fundingRate` files are monthly), and writes nothing. With `--file` it
  downloads that file once and compares every row, positionally per UTC
  day, with the held parts that propose it, through the importer's own
  row validation and reading each part through the tiered store. Equal:
  it writes the missing import entry, or a supersede record naming every
  variant of a conflicting entry and a superseding entry, then one
  `hold-cleared <path> readmit` record per part; bytes, paths and dataset
  versions stay as they were and the importer runs again. A part held as
  `entry-conflict` always gets a superseding entry, so a rebuild from any
  start clears it; a superseded entry copy on a copy target that no
  longer reads and a malformed name on a copy target only are named and
  block nothing (on hot both are still refused). A
  difference names the first differing row and writes nothing; a
  `manifest-conflict`
  hold, a part without a manifest, a `PENDING` naming the file and a
  superseded entry that would leave a listed part orphaned are refused.
  It takes the archive source's writer lock without waiting (creating it
  when absent), so it excludes the importer, the rebuild and a second
  resolve; a re-run completes an interrupted one. The replace path is
  #211.

- **Storage tiers, step 7: `mie store rebuild` (#193, ADR-062 D7,
  proposed).** `mie store rebuild --config <ingest config>
  [--archive-config <path>] [--record-lost] [--record-unlisted]
  [--replace-hot-ledger]`
  restores a lost or damaged copy ledger while `mie ingest` keeps
  capturing. It requires every root's marker, takes the copy-ledger, pass,
  verify and archive-writer locks without waiting, starts from the longest
  ledger copy on a copy target whose hash chain verifies (or empty, and
  says so), lists every manifest on every root and checks each cold copy's
  data, and cross-checks hot's journal and the import ledger. It records
  every verified copy, every partition with cold copies and no directory
  on hot as removed, and re-derives the archive holds (`entry-conflict`
  and `manifest-conflict` always, `unlisted` only with
  `--record-unlisted`, after a run without it named the parts, the import
  ledger's entry count and the target entries hot lacks; the parts of the
  import `PENDING` names are left for its resume). A file a record names that no
  root holds is reported lost and nothing is written; `--record-lost`
  records it. A live manifest conflict, a missing marker or an I/O error
  on a listing stops the run with nothing written. A readable hot ledger
  is replaced only with `--replace-hot-ledger`; the old ledger and every
  ledger copy that is no prefix of the new one go to `.mie/damaged/<time>/`,
  never deleted, which also decides the copies `mie store repair` refuses
  as divergent. The new ledger is installed by temp name and rename after
  the gate `.mie/rebuilt`: the hot removal then waits until a `mie store
  verify` of every copy target covers the rebuilt ledger, and the
  `replication` journal line names why in `removal_waiting`; the remover
  drops a plan made before the rebuild. A missing hot import-ledger
  directory, links followed, stops the rebuild while archive parts or
  target entries exist, since it would read as empty. The importer now refuses, dry runs
  included, while any hold is open.
- **Storage tiers, step 6: `mie store verify` and `mie store repair` (#192,
  ADR-062 D11, proposed).** `mie store verify --config <ingest config>
  [--archive-config <path>] --period-hours <P>` checks every copy of a
  tiered store: hot's sealed files against the copy ledger's manifests;
  on every copy target with its marker, every recorded copy (present,
  manifest identical to the ledger's, size, SHA-256), the files short of a
  copy, the import-ledger entries byte for byte, the copy-ledger copy (its
  hash chain, naming the first failing record, and a byte prefix of hot's)
  and the journal copy (naming the first differing byte); on read-only
  roots, the copies they hold. It appends each finding on a copy target to
  the ledger, then one `verify` record per target, names targets that did
  not present a marker with their copies whose last good verification is
  older than P, writes nothing to any root, and exits 1 on any finding. It
  takes the new `.mie/verify.lock` and runs beside `mie ingest`. `mie store
  repair [--record-lost]` re-checks every copy the ledger records as
  damaged or missing, records again what came back, replaces the rest
  from the first good copy (hot, then the cold roots in order) after
  moving the damaged bytes aside into `<root>/.mie/damaged/<time>/`, ends
  each with a fresh `copy` record so the partition becomes removable
  again, refills a target added or replaced after files left hot, rebuilds
  ledger and journal copies from hot where that is safe (a ledger copy
  whose only damage is an unterminated tail, such as NULs left by a crash,
  is cut back to its last complete record), refuses divergent ledger
  copies, unattributable journal copies and entry conflicts, reports a
  file with no good copy left as lost and writes nothing (the `lost`
  record only with `--record-lost`; a root that fails to read or no
  longer carries its marker makes the file "not repairable in this run"
  instead), skips a hot copy whose partition a concurrent removal
  recorded removed, and ends with one replication pass. The hot removal
  and the replication pass now treat a copy-ledger finding as voiding that
  target's ledger copy until it is repaired, including a finding a
  verification records while the pass runs.
- **P1 replication on 2020-01 … 2024-09, measured: not replicated (#206,
  ADR-064 accepted).** `docs/research/205-p1-replication.md` holds the one
  pre-registered run of `measure_p1_replication` from #207's merge commit
  and its whole output: 46 of 48 contexts measurable, all 46 read `no
  effect` (no context rejected by Benjamini–Hochberg at q = 0.05), so
  `replicated: 0 of 48` and stop rule S3 applies; nothing is promoted and
  the hold-out stays unread. ADR-064 gains its `## Acceptance` section and
  status `accepted`. Docs only.
- **P1 replication on 2020-01 … 2024-09, pre-registered and blind (#205,
  ADR-064, proposed).** ADR-064 fixes the replication of #28's P1-buy and
  P1-sell before any outcome of the window is read: records = replay =
  2020-01-01 … 2024-09-30 from `aggTrades` only, 48 contexts (side ×
  effort window × regime × 4h and 1d), E exactly as ADR-058 D4, a
  per-label taker fee, T1 on ADR-058 D6 units (n_min 385), T2 on the
  units' representatives, a `neither` exit marked to market at H, with a
  56-day block bootstrap (28 and 112 days for sensitivity, seed 205), T3 as a rotation null by whole weeks within
  8-week blocks (4 weeks for sensitivity, 10⁶ draws, two-sided),
  Benjamini–Hochberg over all 48 at q = 0.05, the readings and stop
  rules, and a freeze of its own text, the queries and the tool's
  decision code (one hashed region, printed by both entries; #206 runs
  from #207's merge commit exactly). `docs/research/205-p1-replication.sql`
  holds a coverage chain that restates P1 without the outcome-carrying
  views and a measurement chain; `crates/mie-cli/tests/order_flow_study/replication.rs`
  holds the ignored entries `coverage_p1_replication` (reads no outcome:
  its input types have no outcome field, and a twin recounts its units)
  and `measure_p1_replication` (runs only on the pinned dataset), both behind a window and provenance guard that refuses the
  calibration slice, another stream set or engine, and a dirty build. The
  fixture tests pin BH on the 1995 example, the rotation null by
  exhaustive enumeration and against a free shuffle on clustered
  synthetic data, the guard, the coverage blindness (column lists and
  outcome-swapping datasets), the frozen hashes and that a changed
  decision moves the code hash, the mark to market, and the ADR-058 report
  byte for byte. The ADR-058 path is unchanged. The window was then
  imported into its own store (1 735 of 1 735 files, `archive-verify`
  PASS, 2 327 594 366 rows, 66.0 GB), built once from that commit
  (content `e985ea3e…`, 2 638 166 records) and covered without reading an
  outcome, again after the review amendment with identical counts: 46 of
  48 contexts are measurable, and P1-buy w=1h high and
  extreme at 1d (354 and 288 units) are not.
  `docs/research/205-p1-replication-coverage.md` holds the coverage
  output, ADR-064's pre-registration record the command log, the tool the
  dataset pins, and `docs/data-availability.md` the imported window. No
  outcome of the window was read.
- **Storage tiers, step 5: hot removal, recall and union part numbering
  in `mie ingest` (#191, ADR-062 D6, D11, proposed).** A tiered store's
  writer (`ParquetRawStore::tiered_writer`, with a `TierContext`: the
  first hot date, a clock and a recall observer; `writer()` refuses a
  tiered store) numbers a new part from the union of hot and the copy
  ledger, lost, held and quarantined entries included, so a path is never
  reused. An append to a partition the ledger records as removed from hot
  first recalls it: every recorded file is copied back from the first
  cold root holding a verified copy (`replica::copy_back`; a damaged copy
  is reported and the next root tried, a differing manifest is
  `Integrity`), then `returned` is appended. Without a readable ledger
  the writer opens only hot-window dates. The new `removal` module plans
  the hot removal from one ledger read and one walk of hot (the date
  before the window, every configured target present, every file with a
  `manifest` record equal to hot's manifest and k `copy` records without a
  later `finding`, inside the prefix both ledger copies hold, no lost,
  held or pending path; a D5 conflict is reported and never removed) and removes under the source lock
  and a non-waiting ledger lock, re-checking the ledger's tail and the
  directory: the `removed` record and fsync, then the rename to
  `.removed-date=…`, then the deletion; a crash is finished by the next
  pass. `mie ingest` on a tiered store runs a replication pass at the
  start and after each seal pass on a `mie-replicate` thread, which plans
  the removal; the remover runs in the seal path for the live source and,
  when the plan names it, the archive source (skipped while an import
  holds its lock). Shutdown writes `run_end` first, then waits at most one
  seal interval for the final pass. New journal lines: `replication`,
  `removal`, `recall` and `copy_ledger_truncated`. `mie archive-import`
  recalls a removed partition and prints a recall line. The replication
  pass holds a target's journal copy on the ledger's live paths as well
  as hot's listing (a target added after a removal), matching lines by
  token. Offline tests cover AW3 with the real remover and recall, AW10
  (a late import under a running replay, the locked archive source, the
  removal again), each eligibility rule, a crash at every removal and
  recall step, a hung copy target and the AW2 regression with one target
  unmounted.
- **Storage tiers, step 4: adoption, `mie store adopt` (#190, ADR-062
  D10, D8, D6 interim guard, proposed).** `mie store adopt --config
  <ingest config> [--archive-config <archive config>] --from <store root>
  [--journal <path>] [--import-ledger <dir>]` adopts a closed single-root
  store into an initialized tiered store without passing its data through
  hot: every partition before the first hot date is recorded `removed`
  first, then the `manifest` record of every source file, both before any
  copy; then every sealed file goes straight to every copy target by the
  D5 copy and gets its `copy` records; the
  hot-window partitions, the import entries and the journal (only into an
  absent journal or a byte prefix of the source's) go to hot; then one
  replication pass runs under the held pass lock. The source is only read.
  The path rule: a path held with identical bytes is skipped, with other
  bytes it is `Integrity` and nothing is written, a new path is adopted,
  so a re-run resumes a crashed adoption and a later run adopts another
  store. It refuses, before writing, a source with `.mie/`, seal debris,
  a part whose data is missing or does not match its manifest, a recorded
  file the source does not list that is not complete (on hot unless its
  partition is removed, and recorded on every copy target; the same
  predicate a PASS asserts at its end; a file that only lacks a copy on
  a target added after its adoption, or only recorded on targets since
  replaced, does not block the run, which fails at its end naming it with
  its remedy: `mie store repair`, #192, when a configured target holds a
  copy, otherwise a re-adoption of each original store), a pending
  import, an orphan archive part or an active writer, a
  `--from` overlapping hot or a cold root, and a copy target without its
  marker. The hot window's first date is `mie_cli::ingest::first_hot_date`
  (today − 7, derived from the seed lookback, never configured). Until the
  recall (#191), a tiered store's writer refuses, with `Integrity` and
  before creating anything, a part in a partition the copy ledger records
  as removed from hot or below a part number the ledger already knows; an
  import refused this way leaves its `PENDING` in place (replaced by
  step 5).
  `replica::list_unsealed` lists a root's seal debris and
  `replica::verify_sealed` checks a part's data against its manifest.
  `tools/tier-reports.sh` prints the AW1 reports (`mie replay --source
  live`, `mie replay --source archive`, `mie equivalence`) for the given
  windows with local paths stripped, so host runs diff against one
  baseline. Offline tests cover AC1, AW1, AW2 (both copy targets gone, the
  ledger deleted, a short hot window), AW4 and a crash at every step.
- **Storage tiers, step 3: the copy protocol and `mie store replicate`
  (#189, ADR-062 D5, D9, D11, proposed).** `mie-adapter-parquet` gains
  `replica`, the copy primitives of the replication pass: the D5 copy of a
  sealed part under the writer's own temp names and step order (streamed,
  fsynced, read back and verified, the manifest published last), so a
  crashed copy is a crashed seal to ADR-030 recovery; the copy of an
  immutable import-ledger entry; the tail append of the journal and the
  copy ledger, which first compares the copy's last record with hot's; and
  the pass lock `.mie/replicate.lock`. `copy_ledger::extend_copy` extends a
  ledger copy to hot's verified prefix, never past a torn trailing record,
  and skips a copy that is up to date. `mie store replicate --config
  <ingest config> [--archive-config <archive config>]` runs one pass:
  sealed files, then import-ledger entries, then the copy-ledger tail with
  one `ledger-copy` record per target, then the journal tail up to its
  length before the listing. It writes only into copy targets that present
  their marker (a target without one is skipped and named), refuses hot
  and the marked targets unless they report distinct devices, copies an
  archive part only once an import entry on hot lists it with its
  manifest's SHA-256 and an entry only after its parts, never copies
  `PENDING`, names every file or entry left below k and exits 1, and never
  removes anything. It re-reads a target's marker before every write to it
  and every record about it, checks hot's manifest against the ledger's
  before copying, stops a target's journal copy before the first line that
  names a live file missing on that target, and treats a failure to read hot as a problem of that
  one file, not of the target. A second pass over unchanged state writes nothing.
  Copy-ledger appends now report a truncation of the trailing record as a
  structured `Truncation` (bytes, offset) in `AppendOutcome`, and in the
  new `AppendError` when the fsync of the truncation or the write after it
  fails; the pass prints it as a report line of its own.
  `IngestConfig::archive_config_at` checks an `--archive-config` as
  `[history] archive_config` is checked.
- **Storage tiers, step 1: copy ledger, root markers and `mie store init`
  (#187, ADR-062 D4, D7, proposed).** `mie-adapter-parquet` gains the
  on-disk formats of a tiered raw store under each root's `.mie/`: the root
  marker (`mie-root 1`, hot's id reserved as `hot`) with an atomic
  `.mie/` creation, and the copy ledger (`mie-copy-ledger 1`), an
  append-only text file with one hash-chained record per line for every
  record kind ADR-062 names, a strict parser, a chain check that names the
  first failing record, lock-free readers that ignore the one incomplete
  trailing record, and appenders serialized by a lock file that truncate at
  most that record and never a complete one before it and report every
  truncation to the caller (as structured fields since step 3), plus a
  whole-run hold. `[paths] cold_roots = [{ path, copy_target }, …]` is accepted in
  the ingest and the archive configuration (at least two copy targets, and
  the same list in both through `[history] archive_config`).
  `mie store init` writes hot's marker and an empty copy ledger and refuses
  a store whose cold roots show that hot was lost; `mie store init-root`
  writes a freshly mounted cold root's marker and refuses a non-empty root
  or one that is not a mount point (on hot's device or on its parent
  directory's). With `cold_roots` configured, `mie ingest` and
  `mie archive-import` refuse to start on a hot without its marker. ADR-062
  D7 now defines each record's chain hash as the SHA-256 of the previous
  chain hash and the record's own text. With `cold_roots` empty nothing
  changes and no `.mie/` is created.
- **Independent check of the ADR-054 sampler (#146).** A test-only module
  `mie_domain::independent_sampling` holds a second model of ADR-054 D1–D4,
  written from the ADR text without the sampler's source, and compares it
  record by record with `ContextSampler` on the candidate, location and
  book golden tapes and on a seeded generated corpus (0 mismatches). The
  generator produces each edge class at least once, checked by a coverage
  test: episode bridges through the engine, baseline anchors on a padded
  zone edge, out-of-order spans, events that close only empty minutes,
  divergences confirmed across a trades gap and openers starting at the
  first observed event. Synthetic trigger streams drive the public
  `EpisodeTracker` against the model's episode rule. No sampler, golden or
  ADR changes.
- **Backtest pipeline `research.backtest@1` (#170, ADR-060 D13, D14,
  proposed).** `mie experiment run <spec> --source history --pipeline
  backtest` simulates the spec's hypothesis on the history chain and
  records the trades and metrics through the append-only result store,
  keyed by experiment, pipeline and engine; a re-run prints `reproduced`.
  Before anything is opened the spec's feature set is checked, its rules
  are resolved under rule registry v1 (every error is printed) and
  `funding.recorded@1` is refused when the chain delivers no settlement
  for the sample: its archive does not replay `fundingRate`, or the sample
  ends after the cutover. The chain drive is the one of
  `research.replay_summary@2`, so the result's provenance lines are
  `@2`'s. The new `BacktestEngine` port and `BacktestService` hold the
  engine; `mie experiment validate` gains an informational second stage
  that prints the resolution (`backtest: …` lines) without changing which
  specs are valid or its exit code. A simulation only: there is no order
  or execution path (ADR-010).
- **Time of day and effective sample size in the context measurement
  (#147 M1).** The ignored tool `crates/mie-cli/tests/context_measure.rs`
  now also reports the baseline and trigger samples per UTC hour of the
  event time and per session (`asia` 00–08, `europe` 08–13, `us` 13–21,
  `late` 21–24 UTC), with shares, the trigger-to-baseline share ratio and
  per-family columns, and the effective sample size of the 1m log return
  and the four location-bin indicators on the baseline's minute grid
  (sample ACF with divisor n, Geyer's initial positive sequence, K =
  min(10 080, n/4), checkpoints up to one week). The hour and session
  counts are reconciled with the existing totals and asserted in both
  ignored runs and the CI twin; known-answer tests cover the session
  partition, the grid, the ACF and the ESS. Report-only: the engine, the
  sampler and the records are unchanged. The runs and the ADR-054
  addendum follow in M2.
- **ADR-054 justification addendum: effective sample size and time of day
  (#147 M2).** Records the year and live-store runs of the M1 tables. Over
  the #12 year a location-bin share rests on about 1 450–2 660 effective
  observations, not 525 599 (τ 197–363, dependence lasting 1.8–3.5 days),
  while the 1m return is close to uncorrelated (τ ≈ 0.96). Triggers
  concentrate in 13–16 UTC (share ratio to the baseline 1.43–1.77) and
  the `us` session holds 42.55 % of them against 33.33 % of the baseline.
  No value, criterion or decision changes.
- **Informational alerts: decision path, dry-run and measurement tool
  (#38 M1, ADR-059, proposed).** `mie_domain::alert` decides alerts after
  each accepted event under `alert.policy@1` (fingerprint pinned): zone
  entered and left for the prior day's POC, VAH and VAL (a leave only after
  an alerted enter; retired levels and chain restarts close silently), a
  side-neutral guide at a VAH or VAL with explicit thresholds from the
  auction classifier's own rules (each outcome the invalidation of the
  other), the failed-auction, failed-reclaim and breakout-acceptance
  triggers, regime label changes, and a market report at every UTC
  rollover; seeded from the engine's state so a warm and a cold start
  agree. `mie-ports` gains the outbound `AlertGateway` (best effort, never
  blocking) and the inbound `GenerateMarketReport`. `mie_app::alert`
  bundles one message per event, renders plain ASCII text under the D4
  wording rule and 4 096 characters, and drives replays and the history
  chain (`alert_window`, `alert_chain_window`, `ReportService`).
  `mie alerts replay` writes a byte-reproducible JSON Lines alert log of a
  window, `mie alerts report` prints the report as of a window's end. The
  ignored tool `crates/mie-cli/tests/alert_measure.rs` measures the volume
  (A1 wording, A2 seeding, messages per day per half and kind, bursts,
  FIFO delay, visits, regime dwell, silence and the option table) for the
  year run that precedes the gate decisions. No Telegram adapter yet;
  nothing is sent.
- **Order-flow study run and ADR-058 accepted (#28 M3).** The
  calibration slice (2024-10-01 … 2024-12-31 of the hold-out archive) was
  built in format 2 and verified, not queried (content `c20761ef…`). From
  main `b648545`, the #12 year was built once in format 2 (content
  `5e8e17b0…`), two builds of the R0 live store were byte-identical,
  `verify` passed on all three, and `measure_outcome_views` found 0 model
  mismatches and ADR-057's digests on the format-1 projection.
  `measure_order_flow_study` ran once with the `docs/direction.md` inputs
  (c = 0.95, μ = 0.10 R, n_min = 385, 10 bps taker round trip). The report
  `docs/research/028-order-flow-interactions.md` records every table, null
  results and `insufficient` cells included: 62 of 100 primary cells were
  read and 17 exclude 0 against 3.1 expected by chance, with P2, P1-sell
  and P1-trig well above chance and P4 at 0 of 11. Every effect is far
  below the taker round trip in R at its horizon, so no cell implies a net
  taker edge at μ = 0.10 R. Nothing is promoted; the cells go to the
  Research Lab (#30, #32) as hypotheses. ADR-058 is `accepted`. No code
  changes.
- **Order-flow conditional study, pre-registered and blind (#28 M1/M2,
  ADR-058, proposed).** ADR-058 fixes the whole study before any outcome
  is read: four hypothesis pairs on identical samples (absorption,
  continuation, no acceptance, failed breakout, plus the effort trigger as
  a secondary pair), the effect against a baseline standardized by New
  York hour of week × location bins, a paired circular block bootstrap
  over days (56-day blocks, 28-day sensitivity, 10 000 resamples, seed
  28), n_min from `docs/direction.md` T1 compared with independent units
  (episodes, then horizon thinning, per cell), the readings, the splits
  and the output. `docs/research/028-order-flow-interactions.sql` holds
  the study queries; `crates/mie-cli/tests/order_flow_study.rs` holds the
  ignored tool `measure_order_flow_study`, which asserts the dataset's
  provenance, the published counts and 0 mismatches against an independent
  twin before it prints the pre-registered tables, and its fixture tests.
  `mie-adapter-duckdb` gains `create_views` and `select_tsv` for a study's
  queries, and every connection now turns DuckDB's external access off
  after creating the views, with only the verified dataset's four tables
  allowed, so a study script can read no other dataset and write no file. `docs/research/README.md` sets the report conventions. The New
  York clock moves from `season_measure.rs` to `tests/common/clock.rs`
  unchanged. The study runs in M3; nothing was read here.

- **ADR-057 is measured and accepted (#27 M3).** The D9 acceptance on
  main `4ac1de8`: `mie conditional build` twice on the #12 year and twice
  on the ADR-051 R0 live store gave byte-identical files and equal content
  hashes per pair (year `47d9cb62…`, live `e14516f2…`), `verify` passed on
  all four, and `measure_outcome_views` found 0 model mismatches, equal
  view digests at DuckDB 1 and 4 threads, and the published counts
  (561 695 contexts, 2 808 475 outcomes, the per-horizon counts of
  ADR-056 *Measurement*). The run was blind: no outcome share, quantile or
  per-context outcome was printed. ADR-057 is `accepted`. No code changes.
- **DuckDB views over the conditional outcome dataset (#27 M2, ADR-057 D7,
  D8).** The new adapter `mie-adapter-duckdb` links DuckDB v2.0.0 from its
  bundled sources (`duckdb =1.20000.0`, only the Parquet reader, no
  `download-lib`), turns extension autoinstall and autoload off, and
  creates the views of `views.sql` over one dataset: the three tables,
  `labeled` (one row per record and horizon with the trigger group,
  ADR-051 R1 location bins, excursions in barrier offsets, time to the
  race, and the ADR-055 D8 compositions `level_within` and
  `extreme_vs_opposite`) and the table macro `outcome_summary(n_min)`
  (counts, race shares, nearest-rank five-number summaries and the
  minimum-clear-sample flag per horizon and per every combination of
  trigger, regime and location, plus a family branch with samples and
  distinct decisions only). Every aggregate is order-independent, so the
  output is the same at any thread count. `mie conditional query <dir>
  <view> [--n-min <n>]` verifies the dataset and prints a view as TSV;
  `mie conditional views` prints the SQL for other DuckDB clients.
  `crates/mie-cli/tests/outcome_views.rs` checks the views against an
  independent Rust model and the D7 case tables, at 1 and 4 threads, and
  holds the ignored `measure_outcome_views` tool for the M3 acceptance.
  `deny.toml` admits `foldhash` (Zlib); ADR-057 D8 lists the licenses of
  the vendored C++ code and the measured build cost.
- **Conditional outcome dataset writer and ADR-057 (proposed; #27 M1).**
  `mie conditional build --config … --from … --to … --out <dir>` labels a
  live or archive replay under the current labeling (v2) and writes the
  context records and their forward labels as three long-format Parquet
  tables (`contexts`, `outcomes` per horizon, `passages`) with the run's
  provenance on every row, prices as exact `Decimal128(18, 8)`, row groups
  of 122 880 rows (DuckDB's own), and a `dataset.manifest` written last:
  source, windows, data version, engine, sampling and labeling, each
  file's rows and SHA-256, a content hash over all of that, and the code
  version (`git:<commit>[-dirty]` from the new `mie-cli` build script)
  outside the hash, so a rebuild from the same inputs reproduces the hash.
  `--replay-from`/`--replay-to` widen the replay for warm-up and
  lookahead; the history chain is refused. `mie conditional verify <dir>`
  re-checks a dataset. The writer is the `outcome` module of
  `mie-adapter-parquet`; `mie_app::conditional::label_window_started`
  hands it the provenance before the first record. ADR-057 also fixes the
  DuckDB views, the pinned bundled engine and the blind acceptance that
  M2 and M3 deliver.
- **Seasonal volatility measurement for labeling v3 (#155 M1).**
  `crates/mie-cli/tests/season_measure.rs` measures, report-only, the
  seasonal volatility profile of the #12 design year before any v3 design
  choice: `measure_seasonal_vol` replays the archive as `measure_labels`
  does, through the engine and a labeler under labeling v2 (whose design-
  year counts ADR-056 published, asserted as the replay identity), and
  reads the engine's complete 1m bars per UTC 5-minute slot and v2's
  `Neither` and races per UTC hour. It prints hour-of-week, hour-of-day,
  weekday and 5-minute profiles by four estimators (pooled, 28-day
  deflated, geometric, Parkinson), the variance shares, bootstrap SEs
  (56-day blocks, 10 000 resamples, seed 155), a half-year stability
  check, a US-DST diagnostic, the size of the mechanism per horizon
  (F_H ÷ L_H, with L_H a Wilder(14) twin of ATR_H's bars), the v3 N(H)
  that ADR-056's lognormal-ρ model predicts from it against the go/no-go
  gate, and the correlation of v2's `Neither` share by hour-of-week with
  the mechanism; integer candidate tables go to a file. `_from` reruns the
  analysis from a rows file, `_klines` cross-checks against the archive's
  1m klines. It computes no labeling v3 label and refuses any window that
  overlaps the evaluation year (2024-10-01 … 2025-09-30). SplitMix64 and
  the block resampler move to `tests/common/bootstrap.rs`, shared with
  `label_measure` (no behaviour change).
- **ADR-056 is measured and accepted (#151).** The *Accept when*
  measurement of `conditional.labels@2` (fingerprint `6f90bff079e746f2`) on
  the #12 year and the ADR-051 R0 live store met T0 (0 mismatches in 44 855
  of 561 695 labels checked in the year, 2 185 of 2 185 on the live store),
  completeness, the ATR_1h scale cross-check and the 1h identity (56 372
  and 899 on the year, 49 and 847 live). The prediction is not missed: N(H)
  spread max ÷ min is 1.5811 (4h over 5m), within the 1.62 bound, though
  outside the 2-SE band (SE 0.0582); a miss needs both. N(H) is not flat
  (steps FUUD, 7.821 % at 5m, 12.366 % at 4h, 10.471 % at 1d against the
  predicted 10.05 %); v2 removes 53 % of v1's log-spread. The shape matches
  the 24 h seasonality reading, which the ADR pre-registers for misses
  only, so it is recorded as an observation and the next version's
  candidate. ADR-056 is `accepted`. No code changes.
- **Labeling v2 and ADR-056 (proposed; part of #151).**
  `conditional.labels@2` keeps every rule of ADR-055 except the barrier
  scale: each horizon's band is `reference ± b · ATR_H`, with ATR_H the
  ATR(14) of ADR-033 over the closed ADR-031 bars of the horizon's own
  timeframe and `b = √(π/32)` (31 333 per 10⁵) at every horizon, so the
  1h band is v1's. ADR-056 states the mechanism behind v1's horizon-
  dependent `Neither` miss (a fixed 1h bar length and 14 h memory scaled by
  √H), derives every number, and pre-registers the prediction (N(H) =
  10.05 % of the races that ran at every H, v1's measured 1h share) and the
  miss criterion before any v2 run on real data: max ÷ min N(H) > 1.62 and
  a log-spread outside two standard errors of a circular 56-day block
  bootstrap over the year's UTC days (10 000 resamples, seed 151, at most
  2.3 % of them undefined), with a reading for every shape of a miss, each
  step of N between horizons a rise or a fall only outside two bootstrap
  SEs of its own and flat otherwise;
  ADR-055 is `superseded by ADR-056`. `ForwardLabeler::new` takes a
  `Labeling` (`V1`, `V2`; `CURRENT` = `V2`); under v2 it builds its own
  `BarSet` and five ATR(14) scales from the accepted events up to and
  including the decision event, and counts bar errors in
  `LabelStats.scale_breaks`. `labeling_text`, `labeling_fingerprint`,
  `LabelProvenance::new` and `mie_app::conditional::label_window` take the
  labeling; `barrier_offset` takes the scale and the multiple. v1 stays
  executable with its fingerprint `604b624cc2522df9` and label goldens.
  `volatility::AtrSeries` is ATR(14) on any timeframe; `AtrRegimeSeries` is
  built on it with bit-identical output. The `label_measure` model builds
  its own bars and Wilder ATR from the ADR-031/033 text, cross-checks its
  ATR_1h against every record's `state.atr`, and reports N(H) next to the
  prediction, the max ÷ min spread, the bootstrap's SE, the miss, the
  steps against their bands and the shape's reading. The noise numbers
  ADR-056 cites (step SEs, the 2-SE noise floor of 1.26, the shape-reading
  rates) and its 56-day block, a whole number of weeks that keeps daily
  volatility's weekly cycle inside a block and cuts the dependence left
  without it (autocorrelation 0.12 at 28 days, none at 56), are measured
  from the engine's bars and labeling v1's per-day counts on the year
  (`measure_label_noise`, `measure_label_noise_from`). No v2 measurement
  on real data yet.
- **ADR-055 is measured and not accepted (#26 M3).** The *Accept when*
  measurement on the #12 year and the ADR-051 R0 live store met T0 (0
  mismatches in 44 855 of 561 695 labels checked in the year, 2 185 of 2 185
  on the live store) and completeness (one label per record, 561 695 and
  2 185). The `Neither` share misses D4's 0.92 % Brownian-null prediction by
  7 to 18 times (16.338 % at 5m down to 6.191 % at 1d) and exceeds the
  10.8 % "what breaks" level at 5m and 15m, so per the ADR's Consequences
  the ADR stays `proposed` and a new labeling version follows (#151); no
  refit. The ADR records the per-horizon tables, the censored shares, the
  clear counts and the cost. No code changes.
- **The labeling driver and the measurement tool (ADR-055 D6, D11,
  proposed; part of #26).** `mie_app::conditional::label_contexts` adds
  the `ForwardLabeler` to the guarded sampling drive in the D11 order: an
  accepted event goes to the labeler first, then to the sampler, whose
  records are opened as labels before they reach their sink; a rejected
  event goes to `ForwardLabeler::reject` only, so it never opens or feeds
  a label, and on the trades stream it censors. The engine, the sampler
  and the labeler are the caller's, so one drive per event equals one over
  the stream. `finish_labels` ends a replay as D6 states the driver
  contract: the provider's trades-stream trailing gaps to `observe`, in
  canonical order, no other stream's, then `finish`; it fails closed with
  `ProviderError::Contract`, labeler untouched, on a trades entry that is
  inverted, starts after the trades known up to then or ends before them
  (the entries skip the engine's checks). `label_window` opens
  a replay window with its `LabelProvenance` (data version, engine key,
  both policy fingerprints), takes the provider's trailing gaps through a
  closure (they are reported out of band, not through the port), and
  labels only the records of a record window while every event up to the
  window's end still feeds the labels, so the last records get their
  lookahead. New ignored tests `measure_labels` (the archive, checking
  every trigger record's label and the hourly baselines') and
  `measure_labels_live` (each cleanly ended live run, every label) in
  `crates/mie-cli/tests/label_measure.rs` check the labels against an
  independent streaming model of ADR-055 D1–D9, including each label's
  emission point, assert completeness, and report the per-horizon
  `Neither` share next to the 0.92 % null prediction, the censored shares
  per reason, the unavailable and `same_ms` races, the clear counts, the
  reach-backs, the wall time and the peak RSS for M3; their CI twin runs
  on the seeded three-week tape with a same-millisecond burst, a late
  trades gap and three kinds of rejected trade. No engine, catalog,
  state-hash or record change; the labeler only gains the read-only
  `ForwardLabeler::trades_known_until`.
- **Forward outcome labels (ADR-055, proposed; part of #26).** The new
  domain module `mie_domain::conditional::label` labels every ADR-054
  context record from the trades strictly after its decision event, in
  canonical order, with the record's `trade.last_price@1` as the reference
  and its `volatility.atr.1h@1` as the scale. `ForwardLabeler` emits one
  `ForwardLabel` per record, in record order, keyed `(event, ordinal)`: per
  horizon (5m, 15m, 1h, 4h, 1d) a race between barriers at reference ±
  b_H · ATR with b_H = √(π · H / 32 h) (`BARRIER_MULTIPLE_E5`, a null mean
  exit time of H/4), ties to the earlier trade id with a `same_ms` flag, the
  up and down excursions with the time of the earliest trade at each, the
  last trade, and a `clear` flag that thins each stream to disjoint
  windows; for trigger records, first passages within 1d to the evidence's
  level and extreme (`evidence_prices`). Trades gaps, a stale reference,
  rejected trades-stream events and the end of data censor what was not
  resolved strictly before the censor time, closed windows of a label not
  yet emitted included (a trades outage is silent until trades resume, so
  a complete label is emitted only after a trades-stream event at or after
  its 1d end or 1d censor); no label is dropped, and an emitted label is never reopened (late gaps are
  counted in `LabelStats.reach_backs`). A rejected trades-stream event
  never extends the known trades coverage but may shorten it: it censors
  at the earlier of its own time and the last accepted trades-stream
  event (`Rejected`), closes no window and counts for no emission, so a
  time jump at the end of the data no longer reports the windows before
  it as fully covered, and an out-of-order trade still cuts the races won
  after its time. The end of the data censors at
  the last accepted trades-stream event, not at the last event of any
  stream, so every label comes out once, in record order, however the data
  ends; the M2 replay driver passes only the trades-stream trailing gaps to
  the labeler, then `finish`.
  `LabelProvenance` adds the labeling
  `conditional.labels@1` and its fingerprint to the records' provenance.
  `tools/check-architecture.sh` now rejects any core file outside the
  labeler and its replay driver that names a label, also through a renamed,
  grouped or `super`/`self` import, and probes its own scan first. No engine, catalog,
  state-hash or record change. The replay driver and the measurement tool
  follow in M2.
- **ADR-054 is `accepted` (#25 M3).** The *Accept when* measurement on the
  #12 year and the ADR-051 R0 live store passed: 603 323 345 events, 0
  mismatches against the independent model, 0 grid violations, every
  archive-reachable trigger id with an episode, and the absorption ids with
  15 / 10 / 2 episodes on the live store. The year yields 525 599 baseline
  samples and 36 096 trigger samples in 25 714 episodes, with no bridge and
  no truncated episode; the ADR records the per-id tables, the cross-id
  co-emissions, the baseline split and the cost, and why absorption has no
  sample in the archive (no book stream). No code changes.
- **The context-sampling driver and the measurement tool (ADR-054 D5, D6,
  proposed; part of #25).** `mie_app::conditional::sample_contexts` runs a
  `ContextSampler` through `drive_tolerant_observed` and feeds it accepted
  events only: after a rejected event the engine still shows the previous
  event's bars and triggers, so an unguarded sampler would repeat its
  records. The engine and the sampler are the caller's, so a drive can
  continue warm ones. `sample_window` opens a replay window, builds the
  run's `ContextProvenance` from `Replay::dataset` and a fresh engine, and
  streams the records, with an optional `emit_from` for a warm prefix.
  New ignored tests `measure_contexts` (the archive) and
  `measure_contexts_live` (each cleanly ended live run) in
  `crates/mie-cli/tests/context_measure.rs` check every record against an
  independent model of ADR-054 D1–D4, assert the grid invariant and the
  reachability of every trigger id, and report the per-id episode tables,
  the cross-id co-emissions, the baseline split per regime label and
  location bin, the wall time and the peak RSS for M3; their
  synthetic-tape twin runs in CI. The seeded tapes of `trigger_measure.rs`
  moved to `crates/mie-cli/tests/common/tapes.rs` unchanged. No engine,
  catalog or state-hash change.
- **Direction criteria ([`docs/direction.md`](docs/direction.md)).** The
  three directions the project can take after milestone 1 (trade it, sell
  the intelligence, sell the research machine), the criteria that select
  each, the methods that compute their statistical thresholds on a reserved
  calibration slice, and the personal inputs the owner sets before the
  first Research Lab result is read. No code changes.
- **Context records and their sampling policy (ADR-054, proposed; part of
  #25).** The new domain module `mie_domain::conditional` turns the
  engine's output after each accepted event into `ContextRecord`s: a
  trigger sample for every ADR-053 trigger event, carried unchanged with
  its context and bias, and an unconditional baseline sample at every
  event that closes a 1m bar, anchored at the last 1m close with trades
  like the candidate contexts. Each record carries the event key (the
  decision time), the location context, the bias and a declared
  `StateSnapshot` of 13 features (`SNAPSHOT_FEATURES`). Overlap control has
  no time constant: within one trigger id, a trigger joins the open
  episode when its evidence span (`evidence_span`, read from the
  evidence's own fields; a divergence's runs through the bars that confirm
  its later swing) overlaps the episode's data; every sample is kept with
  an `Episode { id, opener, truncated, bridge }`, and `SamplingStats`
  counts samples, episodes, bridges and truncated episodes per trigger id.
  Location zone events are not sampled. `ContextSampler` is pure, streams
  its records, and can emit from a window start after a warm prefix;
  `ContextProvenance` carries the data version, the engine key and the
  policy `conditional.sampling@1` with its fingerprint. Records are not
  Market State: no feature, catalog, lock or state hash changes. The app
  driver and the measurement tool follow in M2.
- **ADR-053 is `accepted` (#24 M4).** The *Accept when* measurement on the
  #12 year and the ADR-051 R0 live store passed: 603 323 345 events, 0
  mismatches, every archive-reachable trigger id fired. The measured gate
  pass rates (88.9–94.8 %) and shared-boundary shares (17.9 / 3.2 / 1.2 % of
  15m / 1h / 4h SFPs) are recorded in the ADR. No code changes.
- **OI expansion followed by rejection and the trigger measurement tool
  (ADR-053 D7, part of #24).** `trigger.oi_expansion_rejection.<15m|1h|4h|1d>@1`
  fire on the SFP of their timeframe and
  `trigger.oi_expansion_rejection.prior_day@1` on the failed auction, when
  the `derivatives.oi.5m@1` point at the rejection lies on a later
  boundary than the point at the excursion's start (the sweeping trade, or
  the close where the failed probe started or retargeted), with the same
  resolution and strictly higher open interest; no magnitude threshold.
  The point at an event is the ready point of the boundary 25 s before it
  (`oi_lag_ms`), so the archive and a continuous live feed select the same
  boundaries; this replaces the 360 000 ms age bound ADR-053 first stated.
  Each OI trigger directly follows its rejection's trigger with the same
  anchor, context and bias, and carries the rejection, both points and the
  change (`OiExpansion`, `OiRejection`, `TriggerEvidence::OiExpansion`).
  The OI values are compared by `Triggers::equal_but_registry`. ADR-053 is
  amended (D1, D7, D8, Consequences, *Accept when*) and stays `proposed`.
  New ignored tests `measure_triggers` and `measure_triggers_live` in
  `crates/mie-cli/tests/trigger_measure.rs` check the engine's triggers
  against an independent model of ADR-053 D3–D7 and report the M4
  measurements; their synthetic-tape twins run in CI. Every state hash and
  the feature-set version change again; the pre-#22, pre-#24 and pre-#96
  pins hold.
- **Candidate trigger rules (ADR-053 D4/D6, part of #24).**
  `trigger.cvd_divergence.<15m|1h|4h|1d>@1`,
  `trigger.effort_result.<5m|15m|1h>@1`, `trigger.exhaustion@1` and
  `trigger.absorption.<5m|15m|1h>@1` (live only) fire on the order-flow
  candidates of ADR-051 at a defined location: the context the rule reads
  must be ready and list a level of an admitted kind (every kind but
  liquidity clusters, prior sweeps and validated references, named in the
  `gate_kinds` parameter). A CVD divergence's gate reads the registry at
  its current swing without every level of either swing of its pair, on
  any structure timeframe, and its trigger carries that context; the other
  families gate on the candidate's own context. Each trigger carries the
  full candidate event as evidence; each value holds the last trigger with
  the candidate's hashed value and follows its candidate's validity.
  `TriggerEvidence` gains `Candidate` and is no longer `Copy`. ADR-053 is
  amended (D4, D6, D8) and stays `proposed`. Every state hash and the
  feature-set version change again; the pre-#22, pre-#24 and pre-#96 pins
  hold. The ignored `location_measure` rule-I check now compares only the
  bias and the auction and SFP trigger values, since the candidate trigger
  values depend on the selected registry by design.
- **Bias context and the auction and SFP trigger rules (ADR-053, part
  of #24).** `bias.context@1` records the 1h volatility regime and, per
  structure timeframe and side, the step of the last swing pair
  (`higher`, `equal`, `lower`): explicitly non-authorizing, with no
  aggregate or direction, and read by no trigger rule
  (`Bias::read(&MarketState)` is public). `trigger.failed_auction@1`,
  `trigger.failed_reclaim@1` and `trigger.breakout_acceptance@1` fire on
  the auction facts of `location.auction.prior_day@1`, read from each
  fact's own status and anchored at the crossed or accepted value edge;
  `trigger.sfp.<15m|1h|4h|1d>@1` fires on each SFP, anchored at the swept
  level, its location context without the swept swing's levels on any
  structure timeframe (same side and price, nested swing bar). A new
  last stage of the engine emits them through
  `MarketStateEngine::trigger_events`, each with the rule's feature key,
  its family, the evidence fact, a location context and a bias snapshot;
  the bias and the last trigger of each id are appended last to
  `MarketState` as `triggers`. Every state hash and the feature-set
  version change; the pre-#24 hashes are pinned. ADR-053 is `proposed`
  and also fixes the candidate (M2) and OI-expansion (M3) rules that
  follow.
- **`mie equivalence` compares warm-started runs on the history chain
  (ADR-052 D9, #118).** A run whose `warm_start` is warm is checked against
  one pass over the chain of the configuration's `[history]`: the parts
  before it must reproduce the journaled prefix event-stream hash and
  handoff state hash, else `DIVERGED (warm prefix)` with the journaled and
  the current prefix dataset version printed (never compared: a fast
  restart can move the version without a data change); its own events, the
  chain's recompute of the run, then drive the same engine with a fresh
  recorder and are compared as ADR-041 decision 4 compares a run. A warm
  run under another origin or cutover, or without `[history]`, is `NOT
  COMPARABLE`; under another feature set it compares the events only.
  The report gains `history origin <ms> cutover <ms>: one pass through run
  <id>` and a warm run line with its prefix events. Cold runs are compared
  and printed as before. New: `HistoryChain::to_end_of`,
  `HistoryChainStream::run_of_last_event` and `failed_run`,
  `LiveReplayStream::run_of_last_event` and `failed_run`, and
  `mie_app::equivalence::RunParts` and `compare_prefix`. ADR-041's status
  line records the superseded comparison unit.
- **`mie ingest` warm-starts from the history chain while the capture runs
  (ADR-052 D3, D6–D8, D13, #117).** With `[history]`, a run that starts at
  or after the cutover starts its capture at once, with the core fed the
  raw records (`CoreFeed::Records`, `BinanceLiveProvider::next_record`),
  and replays the chain through its predecessor on a `mie-warm-up` thread
  while the core queues the records. At the handoff the queue, then the
  live records, are recomputed with the chain seeds through the new
  `RunContinuation`, the one recompute the chained replay now uses too,
  joined to the prefix, and drive the warm engine through the new
  `mie_app::equivalence::drive_chain_checkpointed` with a fresh checkpoint
  recorder. New journal records: `warm_start` (the prefix's dataset
  version, event-stream hash, rejections and effective origin, the handoff
  state hash, the chain seeds, the queue's high-water in records and bytes,
  the long-warm-up features still warming up; or the cold reason
  `no-history`, `before-cutover` or `warm-up-failed` with its error),
  `warm_ready` (`ready_after_ms`) and `chain_restart`; `run_start` gains
  `history_origin_ms` and `history_cutover_ms`. A warm run's replay takes
  its seeds from `warm_start`. The memory guard reads the new optional
  `[history] max_run_records`, which has no default: without it every
  chain run starts cold, with the reason journaled. The same limit bounds
  the warm-up's record queue (what it leaves beside the largest replayed
  run); a queue past it abandons the warm-up. `chain_restart` is journaled
  and synced when the restart happens (`drive_chain_checkpointed` takes an
  `on_restart` callback). A failed, refused, overflowing or unfinished
  warm-up starts the run cold; the capture never waits for it.
  Runs without `[history]` capture and feed the core as before. `mie
  equivalence` compares warm runs from #118 on (below).
- **Research on the history chain: `research.replay_summary@2` and
  `mie experiment run --source history` (ADR-052 D10, #119).** With an
  ingest configuration that has `[history]`, the run opens the sample on
  the history chain and checks its data version, drives the chain's prefix
  `[origin, sample start)` into the engine without evaluating it, then
  replays the sample into the same engine as `@1` does, both through
  `mie_app::drive_chain`. The new outcome kind `history-replay-summary`
  records the origin, the effective origin, the cutover, the prefix's
  dataset version, event-stream hash and rejections, the engine key, and the
  state hashes at the sample's start and end. A `@2` result is keyed by
  experiment, pipeline and engine (feature-set version and state-hash
  encoding), stored as `<pipeline id>@<N>-<feature-set hex>-<state
  encoding>.result`, so a build with another engine records next to the
  old result. A sample before the origin fails. `--source live|archive`,
  `@1`, its keys and result text are unchanged.
- **The history chain and `mie replay --source history` (ADR-052, #116).**
  An optional `[history]` section of the ingest configuration (`origin`,
  `cutover`, `archive_config`; the archive configuration must share the raw
  root and the symbol) names the chain: the archive backfill from the origin
  up to the cutover (default streams, `aggTrades` folded), then the
  journaled capture runs that started at or after the cutover, the first
  continuing after the archive's last event as runs continue after each
  other. Every chain run is recomputed with the chain's newest event of
  each series as its seed when its journaled seed does not reach back that
  far, so every hole, the seam included, opens with a `Disconnected` gap,
  also when the archive's last days are missing. A run the cutover falls
  inside is refused by name. Every request
  replays from the origin and is restricted to its window, so adjacent
  windows concatenate exactly; the report has one line per segment, and the
  chain's dataset version covers both. An uncrossable hole (more than
  `MAX_BARS_PER_EVENT` one-minute bars) restarts a fresh engine through the
  new `mie_app::drive_chain`; each restart and the effective origin are
  printed. `--source live|archive` reports are unchanged; `mie experiment
  run` refused `--source history` until #119 (above).
- **Order-flow candidates: effort vs result, exhaustion and absorption
  (ADR-051, #22).** `candidate.effort_result.<5m|15m|1h>@1` judges each
  aggression window, once per closed minute, against its own previous
  1 440 evaluations: aggression at or above the 0.800 quantile with no more
  progress than the median move is a candidate. `B` = 1 440 and `q_hi` =
  0.800 were fixed by the pre-registered rules R1b and R1 on the hold-out
  year 2024-10-01 … 2025-09-30. `candidate.exhaustion@1` is a 1h expansion
  whose 5m aggression decayed to at most its median while the last close
  stays within 6 bps of the extreme; `candidate.absorption.<5m|15m|1h>@1`
  (live only) is effort vs result with the passive side opposite the
  aggressor replenished (fills and `added ≥ filled` within 5 bps; a depth
  outage, which books no fills, is not evaluated). Each emits on the
  rising edge, with a location context at the last close, through
  `MarketStateEngine::candidate_events`; values are part of
  `MarketState.candidates`. Every state hash and the feature-set version
  change. The `candidate_measure` tool checks all three against its model
  (R0), on archive replays and, with `measure_candidates_live`, on live
  captures.
- **Order-flow candidates: CVD and open-interest divergences (ADR-051,
  #22).** `candidate.divergence.cvd.<15m|1h|4h|1d>@1` and
  `candidate.divergence.oi.<15m|1h|4h|1d>@1` compare consecutive confirmed
  swings of one side at their anchors, the first trade that set each swing
  bar's extreme: a further extreme with the CVD the other way, or (on
  either side) with lower open interest, is a candidate. No magnitude
  threshold; each comparison carries its price, CVD and OI changes, the
  trades gaps between the anchors and the span. Values are appended last
  to `MarketState` as `candidates`; the facts, with a location context
  (registry levels at the swing price and the auction state), are exposed
  by `MarketStateEngine::candidate_events`. Candidates carry no direction
  and never gate on location.
- **`--trade-correction fold|none` (ADR-050, #102)** on `mie replay`,
  `mie experiment run` (both with `--source archive`) and
  `mie archive-kline-check` (with `--trade-source aggTrades`). `none`
  delivers `aggTrades` exactly as published and reproduces the dataset
  versions and event-stream hashes recorded before ADR-050; `mie replay`
  prints a fold summary, one line per corrected day and one per corrected
  1m minute.
- **`capture-report` check 7, kline reconciliation (ADR-048).** Closed
  `kline_1m` minutes and `aggTrade` records are reconciled in trade-id
  space per run; a block of minutes whose kline volume differs from its
  aggregate quantity fails the report, with one line per block and a
  totals row. A window with a trade lost inside an aggregate — which keeps
  the aggregate id consecutive and journals no gap — now FAILs, the #9 soak
  window included (2026-10-08, 43.250 BTC). Skipped unless both streams are
  configured.
- **Order-book liquidity state (ADR-043).** The engine owns the L2 book as
  `book.l2@1` and hashes it whole in the Market State; `book.depth@1` (best
  levels, depth and imbalance within 1, 2 and 5 bps of mid),
  `book.clusters@1` (the five largest levels per side within 5 bps with the
  median level quantity) and `book.liquidity.window.<5m|15m|1h>@1`
  (liquidity added, cancelled and filled per side and band, fills matched
  with taker trades). Live only.
- **Location and auction state (ADR-044).** `location.vwap.utc_day@1` (the
  UTC-day VWAP over closed minutes), `location.levels@1` (a level registry
  over the prior-day and 5-day profiles, the structure registries, the book
  clusters and the VWAP, with distance, touches, age, source, confluence
  and cluster strength per level, and Entered/Left zone events) and
  `location.auction.prior_day@1` (the auction state against the prior day's
  value: inside value, at the edge, outside, breakout, failed breakout,
  failed reclaim, acceptance after 60 closes beyond the edge). Transitions
  and zone events are exposed by `MarketStateEngine::location_events`.
  `crates/mie-cli/tests/location_measure.rs` measures the numbers behind
  the tolerance and the acceptance time over the archive.
- **7-day composite profile and `location.levels@2` (ADR-036 decision 2,
  ADR-044 D5, D10).** `profile.volume.composite_7d@1`, the bin-wise sum of
  the last 7 completed UTC days (a 7-day window holds every weekday once),
  and `location.levels@2`, the level registry over the prior-day and 7-day
  composite profiles. `MarketStateEngine::with_features` builds an engine
  for the default set with an older version selected instead, today
  `location.levels@1`, which stays computable; it refuses a set whose
  definitions differ from the catalog's. The experiment service builds its
  engine with it, so a spec on `location.levels@1` runs and reproduces
  (ADR-040 D4).

### Changed

- **Import-ledger generations and current entries (#210, ADR-062 D11,
  proposed; ADR-034 D4 superseded at one point).** An import entry is
  still never edited, but a later one can supersede it: next to
  `F.import`, the import ledger holds `F.<n>.import` and
  `F.<n>.supersede.import` (strict names, `n` from 1 to 4 294 967 295,
  every other `.import` name an error). The current entry of an archive
  file is the highest entry generation at or above the highest supersede
  generation; every reader goes through it: the importer (a file whose
  entries are all superseded is imported fresh), `mie archive-verify`,
  replication (supersede records are copied like entries, superseded
  entries never, and an archive file with a malformed file fails closed),
  verification (supersede records checked like entries, superseded ones
  not), repair, the adoption (refuses a source holding generations) and
  the rebuild. Existing entries keep their names and bytes.
- **The rebuild clears a leftover start hold on positive evidence (#210,
  ADR-062 D7 B7′).** A start hold the sources no longer derive is cleared
  with `hold-cleared <path> readmit` when its archive file has a current
  entry listing it with its manifest's SHA-256 and the hold is `unlisted`,
  or `entry-conflict` with an entry written by a resolve; every other one
  stays held and named, so a full, a stale and an empty start agree after
  a re-admit. The writer's source lock is now taken through
  `removal::take_source_lock`, shared with `mie store resolve`.

- **Breaking: `LiveReplay::run_replay` streams (#140, ADR-063).** It no longer collects and sorts a whole run before the
  recompute. A first pass reads each of the run's files once and checks
  the run with one bit per record, raising every integrity error, with
  the same texts as before, before any event. The recompute then merges
  the files by `receive_seq`, opening each only when its first record can
  be next, so it holds about one seal interval of records instead of the
  whole run (6.1 GB on a 34 h run before, ADR-053). The output is
  unchanged. `RunReplay` becomes `RunReplay<'a>`, borrowing the store
  through a `&dyn RawRecordSource`, so it is no longer `Send`;
  its pipeline counters in `stats()` are final once `next_event` returned
  `Ok(None)` (the record counts stay final at construction); a storage
  fault between the passes now fails `next_event` with
  `ProviderError::Source`. New `RunReplay::peak_buffered_records()`.
  `LiveReplay::replay` and the history chain keep holding one run
  (ADR-052 decision 13).
- **Breaking: conditional outcome dataset format 2 (#28 M1, ADR-058 D2).**
  A new table `effort.parquet` holds each record's three effort-vs-result
  evaluations (5m, 15m, 1h: state, aggressor, effort, `effort_high`,
  progress, the median and high moves, coverage, `active`), and `contexts`
  gains `auction_side`, `cvd_utc_day`, `cvd_partial_start` and
  `cvd_feed_gap`. The manifest reads `mie-outcome-dataset 2` with a
  `file effort.parquet` line, `verify` checks three effort rows per record,
  and the DuckDB views gain an `effort` view. Format-1 datasets are refused
  and must be rebuilt with `mie conditional build`; `measure_outcome_views`
  checks a rebuilt year against ADR-057's published digests on the
  format-1 columns.
- **Breaking: archive replays, experiments and the aggTrades kline check
  fold by default (ADR-050, #102).** Trades the `aggTrades` archive omits
  (2025-10-01 … 2026-06-09, ADR-047) are restored from imported `trades`
  files: each is added to the quantity of the aggregate right below it,
  also across a day boundary, or becomes a `MissingData` gap when its price
  or side differs, when an earlier trade of the same gap did not fold, or
  when the aggregate below lies in a day the replay does not have
  (`unresolved`); a gap the window ends inside is a trailing gap. The day
  after the window is read for the tail of the window's last aggregate and
  never delivered, so a folded quantity does not depend on where the window
  ends; when the store lacks that day's `trades` or `aggTrades`, the tail is
  a trailing gap and a `fold unchecked tail` report line. Where
  `trades` files exist for a window, its archive dataset version and
  event-stream hash change; experiment specs pinned to an earlier version
  run with `--trade-correction none`, and a mismatch under the fold prints
  that hint. `ArchiveReplay::new`, `with_defaults` and `open_requests` take
  an explicit `TradeCorrection`. Reports of `mie replay --source archive`
  and `archive-kline-check` name the correction in their first line.
- **Breaking: the order-book checkpoint audit has a per-side level floor
  and an exact window width (ADR-049, #98).** A checkpoint without a
  mismatch whose bid or ask side compared fewer than `MIN_SIDE_LEVELS` (1)
  levels is now `inconclusive` instead of `matched`; a mismatch still takes
  precedence. `capture-report` counts `inconclusive` on the order-book row,
  does not fail on it, and does not count it as `matched` for the presence
  rule. The window width is measured per side from the fresh best to the
  window edge, in hundredths of a bp, rounded up, and is 0 when the edge
  lies beyond the best price (before: the absolute distance, truncated to
  whole bps). In `book_checkpoint` journal lines, `window_bid_bps` and
  `window_ask_bps` are replaced by `window_bid_cbps` and `window_ask_cbps`,
  and `levels_bid` and `levels_ask` are added (`inconclusive` lines also
  carry `min_side_levels`). Older lines are judged by their `result`, and
  their whole-bp widths stay out of the window statistic. Over the #10 soak
  the audit now gives 809 `matched` and 3 `inconclusive` (an empty side
  after the price outran the previous window), still PASS.

- **The default level registry reads the 7-day composite (ADR-044 D5).**
  `location.levels@2` replaces `@1` in the default feature set, and
  `profile.volume.composite_7d@1` joins it; `composite_5d@1` stays computed.
  The feature-set version and the state-hash goldens move; no
  `STATE_HASH_ENCODING` bump (ADR-041 D2). Runs recorded before this change
  compare events only (ADR-041 D2).

- **`archive-kline-check` reports coverage and an INCONCLUSIVE verdict
  (ADR-045).** A coverage line (window bars compared, bars skipped as
  incomplete, klines without a bar) precedes the verdict. A run with no
  mismatch prints `INCONCLUSIVE` and exits 3 instead of passing when it
  compared fewer bars than the window
  holds (1855 per day), for example because trade or kline days are
  missing at the window's edges, or when it skipped more than the six
  partial-start bars (one per timeframe). Exit 0 (`PASS`) and 1 (`FAIL`)
  keep their meaning.

### Fixed

- **`archive-kline-check --trade-source trades` compared almost no bars
  (#85, ADR-045).** The archive `trades` id space is sparse by design (the
  exchange skips ids its klines do not count), so an id jump in that
  dataset is no longer a `SequenceBreak` gap. Live capture and archive
  `aggTrades` keep the strict rule.
- **Open-interest polling backs off from rate limits (ADR-046, #84).** On a
  418 or 429 the poller honours `Retry-After` (seconds, capped at 3 days);
  without the header it pauses 60 s after a 429 (one `REQUEST_WEIGHT`
  window) and 120 s after a 418 (the shortest ban). Before, the reconnect
  backoff was swallowed by the 10 s poll slot and polling went on into the
  rate limit. Successful polls stay on the 10 s grid.

### Security

- **`zip` 8.6 → 9.0 (RUSTSEC-2026-0367, -0368, -0369).** The three
  advisories concern `extract()` (SUID/SGID bits, symlink targets and
  permissions through symlinks outside the destination), which mie never
  calls: the archive reader opens the one CSV entry with `by_index(0)` and
  streams it. `cargo deny check` fails on them all the same. In 9.0 an
  entry's `name()` returns a `Result`; a name that does not decode is now a
  `FetchError::Zip` like any other unreadable archive.

## [0.1.0] - 2026-10-09

### Added

- **Hexagonal workspace with an enforced crate graph (ADR-025).** `mie-domain`
  (pure, deterministic core), `mie-ports`, `mie-app`, adapter crates and the
  `mie-cli` composition root, checked by `tools/check-architecture.sh`.
- **Domain core.** Exact decimal parsing and fixed-point prices and quantities
  (ADR-027), event time and a canonical multi-stream order (ADR-028), feature
  identity and versioning (ADR-029), event-time bars on six timeframes
  (ADR-031), ATR(14) percentile regime (ADR-033), order-flow CVD and rolling
  aggression windows (ADR-035), volume profile with POC, value area, HVN and
  LVN (ADR-036), market structure with swings, levels, sweeps and SFP
  (ADR-037) and derivatives context: open interest, funding and liquidations
  (ADR-042).
- **Immutable raw store (ADR-030).** Verbatim messages in sealed Parquet files
  with manifests and dataset versions.
- **Binance live capture: `mie ingest`, `mie capture-report` (ADR-032,
  ADR-038).** Raw-first persistence, shared normalization, feed gaps,
  canonical merge, and order-book sync from depth diffs and REST snapshots
  with audited checkpoints.
- **Binance public-archive backfill: `mie archive-import`,
  `mie archive-verify`, `mie archive-kline-check` (ADR-034).**
  Checksum-verified, exactly-once import (re-runs skip imported files, an
  interrupted file resumes) and a cross-check of bars against archive klines.
- **`mie replay` (ADR-039).** Replays a window of the raw store through the
  core; the report carries the dataset version and the event-stream hash, and
  the same window prints the same bytes.
- **`mie equivalence` (ADR-041).** Recomputes every capture run alone into a
  fresh engine and compares its state checkpoints with the ones `mie ingest`
  journaled; the first divergence is reported.
- **`mie experiment validate` / `mie experiment run` (ADR-040).** Experiment
  specs identified by the fingerprint of their canonical form, and an
  append-only research result store with SHA-256 trailers; a re-run on the
  same data version prints `reproduced` instead of `recorded`.
- **Release pipeline.** A pushed `vX.Y.Z` tag builds, tests and publishes the
  Linux x86_64 `mie` binary with a SHA-256 sidecar and these notes as a GitHub
  Release (`.github/workflows/release.yml`, `docs/release.md`).
