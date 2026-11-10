# ADR-062: Raw-data retention and storage tiers

- Status: proposed
- Date: 2026-11-05

Relation to earlier ADRs: this ADR supersedes one point of ADR-034 D4:
an archive file's import entry can be superseded by a later one (D11,
*Held archive parts and their resolution*), where ADR-034 D4 has exactly
one immutable entry per archive file. ADR-034 keeps its status line until
the follow-up PR that implements this changes the importer, as ADR-052
does for the ADRs it supersedes. It builds on ADR-030 (the immutable raw
store and its dataset version), ADR-032 D5 and D10 (the seed lookback,
the seal cadence), ADR-039 (replay over the raw store) and ADR-052 (the
history chain from a fixed origin). The CLAUDE.md invariant 4,
`docs/data-availability.md` and the other accepted ADRs stay as they are
until the follow-up implements D5–D7 and D11.

The decisions record the #134 plan
(<https://github.com/Replikanti/mie/issues/134#issuecomment-6660674077>)
and its STOP 1 operator decisions. Every rate and share below comes from
the M1 measurement
(<https://github.com/Replikanti/mie/issues/134#issuecomment-6662697818>),
cited as M1-n.

## Context

Continuous live capture grows the raw store without bound, and nothing
decides what stays on the capture host, what moves, and what is dropped.
ADR-030 made sealed files immutable, so any move or deletion rule needs a
decision. Production capture will run on a server that is not provisioned
yet, so its disk is an input of this ADR, not a fact.

**Measured growth (M1).** Over the 14 live runs on record (8 journals,
October 2026), all seven default streams together grow the store by
19.2–46.9 MB per captured hour, depending on the session (M1-8). The
all-hours runs give 35.6 MB/h on weekdays and 20.6 MB/h on weekends
(M1-3, M1-4), so 750.5 MB per day weighted over a week, about 274 GB per
year (M1-5). The busiest sessions on record, #99's US-session windows,
give 46.9 MB/h, so 1 126.3 MB per day if every hour were that busy, about
411 GB per year (M1-6). `depth` is 87.1–89.1 % of every store (M1-1). A
full live day seals 3 670 files, data and manifests (M1-9). The archive
store on the host holds 51.0 GB (M1-12).

**What constrains retention.**

- A live replay recomputes a whole run from all of its records in
  `receive_seq` order, because the hold-back watermark depends on every
  stream (ADR-039 D1, D2). A clean run with a missing record, or with a
  record count other than its `run_end` count, is an error.
- Every warm start, every `mie equivalence` pass and every
  `--source history` request replays the whole chain from the 2025-10-01
  origin (ADR-052 D1, D2). The origin is fixed, and moving it needs a new
  ADR.
- Research reads live depth: `book.*` (ADR-043), absorption (ADR-051),
  the slippage calibration (ADR-061 D7), the book audit (ADR-038) and #99's
  per-diff lateness.
- Mie does not monitor mount liveness, and an unmounted mount point is an
  ordinary empty directory.

**The issue's premise, corrected.** #134 lists `trades`/`aggTrades`,
`klines`, open interest and `markPrice` as republished in the archive.
Per `docs/data-availability.md` and the ADR-034 Context:

- `forceOrder` has no USD-M archive dataset.
- `markPrice@1s` (mark, index, indicative funding) has only 1m mark-price
  klines in the archive, and mie does not import them.
- Archive open interest is the 5-minute `metrics` sample, while live polls
  every 10 s, which is another `resolution_ms` (ADR-042).
- `depth` and `depthSnapshot` have only the `bookDepth` percentage bands.

So only `aggTrade` and `kline_1m` have an archive counterpart at their own
resolution, and they are 6.2–9.6 % of the bytes (M1-2).

## Decision

Retention is "keep every sealed file". The policy decides only *where* a
file lives: a writable hot tier and read-only cold roots. A file's identity
(relative path plus SHA-256) does not depend on its location, so moving
data changes no dataset version.

### D1. Retention per stream: everything is kept

| Data | Archive counterpart (at its real resolution) | Consumers | M1 rate | Verdict |
|---|---|---|---|---|
| `aggTrade` (live) | `aggTrades`, per aggregate; the event sets differ (ADR-047, ADR-048, ADR-050) | bars, CVD, every trade feature, candidates, triggers; the run's watermark | 19.1–91.9 MB/24 h | keep |
| `markPrice` (live) | none imported (1m mark-price klines exist, not imported) | `derivatives.mark@1` (live only); the run's watermark | 5.2–5.6 MB/24 h | keep |
| `forceOrder` (live) | none (no USD-M dataset) | `derivatives.liq.window.*` (live only); the run's watermark | 0.3–0.6 MB/24 h | keep |
| `kline_1m` (live) | `klines_1m` | kline cross-check (ADR-031); the run's watermark | 9.6–16.1 MB/24 h | keep |
| `openInterest` (live) | `metrics`, 5 min instead of 10 s (ADR-042) | `derivatives.oi.*`, OI candidates and triggers; the run's watermark | 0.9–1.1 MB/24 h | keep |
| `depth` (live) | only `bookDepth` bands | `book.*`, absorption, slippage calibration, book audit, #99; the run's watermark | 406.5–980.8 MB/24 h | keep |
| `depthSnapshot` (live) | only `bookDepth` bands | the book sequencer (ADR-038); the run's watermark | 18.2–30.5 MB/24 h | keep |
| `aggTrades` (archive) | — | the chain's archive segment (ADR-052 D2); archive research | 41.99 MB/day | keep |
| `klines_1m` … `klines_1d` (archive) | — | the kline cross-check (ADR-031, ADR-045) | 0.17 MB/day together | keep |
| `fundingRate` (archive) | — | `derivatives.funding.settled@1`, the chain | 0.7 MB/year | keep |
| `metrics` (archive) | — | archive open interest, the chain | 0.03 MB/day | keep |
| `bookDepth` (archive) | — | raw DuckDB research only | 0.98 MB/day | keep |
| `trades` (archive, opt-in) | — | the `aggTrades` fold (ADR-050), the kline cross-check | 81.5 MB/day mean, when imported | keep |
| capture journal | — | every live replay (ADR-039 D1), the chain | 5.1–5.6 MB/24 h; 15.1 for #99 (M1-11) | keep, on hot |
| import ledger | — | the exactly-once import (ADR-034 D4) | ≈ 6 KB per imported day (M1-15) | keep, on hot |
| copy ledger (new, D7) | — | `select` for moved partitions, D6, D11 | ≈ 1.6 MB/day, estimated (Numbers table) | keep, on hot |

Rates: live ranges are across the stores with depth (M1 live table);
archive rates are the mean per imported day (M1 archive table).

- **Why the archive is kept although it can be downloaded again.** A
  re-import under a newer `parquet` writes other bytes (ADR-030
  Consequences). That changes the chain's prefix dataset, which turns every
  recorded warm run into `DIVERGED (warm prefix)` (ADR-052 Consequences),
  and it breaks the archive dataset versions that accepted ADRs cite
  (`fa73cb88…`, ADR-030 and ADR-039 Acceptance). Upstream files can also
  change (the `changed` conflict, ADR-034 D4).
- **Why `bookDepth` is kept although nothing in the chain reads it.** It
  is 1.0 % of the archive (M1 archive table). A per-dataset exception
  would need its own deletion path, while the rule "a sealed file is never
  removed from the union of roots" (D5) is what makes immutability
  checkable at all.
- **The hold-out year** (#124) is kept whole. It is a hold-out: re-creating
  it with other bytes would make its single final measurement
  unreproducible.

### D2. Option 3 (drop archive-republished live copies) is rejected, with no exception

- Dropping a stream's live records leaves `receive_seq` holes and a record
  count other than `run_end.records`. ADR-039 D2 makes that an error for
  every clean run, so the run becomes unreplayable as a whole, depth
  included.
- Archive rows cannot replace them. ADR-039 D6 and ADR-052 D2 ("holes stay
  holes", one source per stream) forbid it, and the events would differ:
  97 live aggregates differ from the archive in `q` and `l` (ADR-048),
  archive aggregates omit trades that the fold restores (ADR-047,
  ADR-050), and the `trades` id space differs (ADR-045). Live/replay
  equivalence (ADR-041) compares a live run with its own recompute, so it
  needs the run's own records, never archive rows.
- **Pre-cutover runs are no exception.** They are the acceptance evidence
  of accepted ADRs (the ADR-030 and ADR-039 dataset versions, ADR-038,
  ADR-041, the ADR-051 R0 runs, ADR-061) and the corpus of the still-open
  #99.
- **The rejected saving** is the share of the streams with an archive
  counterpart: 6.2–9.6 % (M1-2), so 47–72 MB per day at the week-weighted
  rate (750.5 × 0.062 … 0.096). It is small because `depth` dominates
  (M1-1), and it would cost every run it touches.

### D3. Option 4 (depth as snapshots or checkpoints only) is rejected

Baseline ADR-022 requires raw order-book events to be preserved. Snapshots
alone remove the book replay that `book.*` (ADR-043), absorption (ADR-051),
the slippage calibration (ADR-061 D7), the book audit (ADR-038) and #99's
per-diff lateness read. They would also change a run's record set, with
the D2 consequence.

### D4. Two tiers: hot and cold

- **Hot** is the writer's `raw_root` on a local filesystem. It must be
  local because the writer lock is `flock` (ADR-030 Consequences).
- **Cold** is an ordered list of filesystem roots with the same relative
  layout, mounted outside mie (operator, STOP 1). Every reader and the
  writer treat them as read-only; only the replicator (D11) writes into
  them. Each root carries a marker file with its root id, written once when
  the root is provisioned.
- **Root markers.** Every root of a tiered store, hot included, carries
  `<root>/.mie/root` (grammar in D7 *Formats and layout*). Hot's marker is
  written by `mie store init` (D7) and always carries the reserved id
  `hot`. A cold root's marker is written by `mie store init-root --config
  <ingest config> --root <path> --id <id>`, once, on a freshly provisioned
  root. It refuses, in this order:
  1. a `--root` that is not lexically one of the configured `cold_roots`;
  2. an id that fails `validate_segment` (1 to 64 characters from
     `[A-Za-z0-9_-]`, the raw store's segment rule, so an id is safe as a
     path segment) or is `hot`;
  3. a hot without its marker: `mie store init` comes first, because the
     device check below compares against hot, and `init`'s cold-marker
     rule (D7) relies on this order;
  4. a root that is missing, is not a directory, or is not empty. The root
     is never created: a created directory lands on the parent's
     filesystem, which is the unmounted-mount-point trap. The only entries
     an empty root may hold are the staging directories of an earlier
     attempt that crashed or failed (the atomic `.mie/` creation, D7);
     the successful attempt removes them. Why: otherwise every retry after
     such an attempt would be refused as "not empty", which names the
     wrong cause;
  5. a root that is not a mount point: its device id (`st_dev`) equals
     hot's, or equals its parent directory's. Why: an unmounted mount
     point is an ordinary empty directory on its parent's filesystem, and
     a marker written there would make it look mounted, so the replicator
     would fill the parent's disk and the real mount, once back, would
     hide the marker and the copies. The parent's filesystem is hot's only
     when the mount points lie on hot's disk, so the hot comparison alone
     misses mount points on another disk (the system disk, say). The
     consequence: a cold root is the mount point itself, never a
     subdirectory of a mounted volume. Every device check goes through one
     probe; only the production host can show distinct devices, so this
     acceptance is verified at provisioning, where the command runs
     against the real mounts;
  6. an id that another configured root's marker already carries (a root
     without a marker is skipped; any other read error refuses).

  The `mie store` command group holds `init`, `init-root`,
  `replicate` (one replication pass, D11; #189) and `adopt` (the
  bootstrap of an existing store, D10; #190); the later steps of the
  follow-up add their verbs to it.
- **Option 1 (everything on the local disk)** is the configuration with
  no cold root, and it stays valid while the hot formula (D10) fits the
  disk. It loses as the policy because the fast local disk would have to
  grow with the whole history (D10: hundreds of GB per year), and because
  one disk is one copy (D11).
- **Option 2 (tiered storage)** is this decision. The cold medium (a disk,
  a NAS, an object store behind a filesystem mount) is a provisioning
  choice outside mie.

### D5. Immutability under tiers

This is how the raw-store immutability invariant reads once files can
live in more than one root (approved as written at STOP 1).

- A sealed file is identified by its relative path and SHA-256, as in its
  manifest and in every dataset version (ADR-030, the `file <path> <rows>
  <sha256>` lines). Its bytes, its manifest and its relative path never
  change, and it is never removed from the union of the roots. The one
  exception is the *replace* resolution of a held archive part (D11): the
  part moves to a quarantine directory, is never deleted, and every
  selection that covers it fails until the re-import's entry replaces it,
  so no selection silently loses it.
- **The copy protocol.** A copy writes the data and the manifest to the
  destination under hidden temp names, fsyncs them and verifies the
  SHA-256 against the manifest. It publishes the manifest last, because "a
  file is sealed iff its manifest exists" (ADR-030), and then records the
  copy in the copy ledger (D7). The hot copy is removed only by D6, after k
  verified copies exist. So at every instant each sealed file is complete
  in at least one root.
  - **Temp names and steps are the seal's** (#189). The copy writes the
    data to `.part-NNNNN.parquet.tmp` and fsyncs it, writes the manifest
    text verbatim to `.part-NNNNN.manifest.tmp` and fsyncs it and the
    directory, reads both back and verifies them against the manifest,
    marks both read-only, renames the data, then the manifest, and fsyncs
    the directory. The data is streamed through a bounded buffer. Why: a
    crashed copy on any root then passes through exactly the intermediate
    states of a crashed seal, which ADR-030 recovery already defines (a
    data temp file alone is discarded, a temp file or published data with
    its pending manifest is rolled forward). That matters once the recall
    and the hot restore copy onto hot, where the writer's recovery runs.
    Rejected, copy temp names of their own (`.copy.tmp`): writer recovery
    ignores names it does not own, so a crashed copy on hot would leave
    debris that blocks D6's "no `.tmp` file" eligibility for good, and the
    copy's states would need a second recovery definition.
  - **A destination that already holds the path.** Its manifest must equal
    the source's byte for byte and its data must verify: the copy then
    only records itself. Data without a manifest (a copy that crashed
    between the two renames) is verified against the source manifest and
    completed by publishing the manifest; data that fails is `Integrity`
    and is never overwritten. A copy's temp debris is removed.
  - **The D5 guard runs before the copy** (#199 review). Hot's manifest
    text is compared with the copy ledger's `manifest` record for the path,
    and a path with two recorded texts is refused, before any byte is
    written to a target. Why: checked after the copy, a hot manifest that
    differs from the recorded one would already sit on the new target, and
    two targets would hold two versions of one path.
  - **The marker is re-read before every write to a target and every
    record about it** (D11 *Replication*; operator, #199 review).
- The same relative path in two roots must be byte-identical, data and
  manifest; anything else is `Integrity`.
- A copy never recompresses, merges parts or re-encodes: any of these
  changes the hashes and so every recorded dataset version.
- Why this reading: the invariant exists so that every derived result
  reproduces from versioned raw data (ADR-003, ADR-022). The dataset
  version names paths and hashes, never a location, so a file that moves
  but keeps its bytes, manifest and path reproduces every result that
  cited it. Rejected, reading immutability as "never moves": it would force
  option 1.

### D6. Hot removal: the unit and when a partition may leave hot

- **The unit is one whole `(source, instrument, stream, date)` partition.**
  A part number is "highest existing + 1" within its partition
  (`writer.rs`, `open_part`), and recovery works per partition
  (`recovery.rs`). A partition split across tiers could reuse a part
  number.
- **Part numbers come from the union.** The writer's next part number is the
  highest part that hot *or* the copy ledger (including its lost, held and
  quarantined entries, D7) knows for the partition, plus one. Why: today
  `open_part` counts only the local partition, so a hot that lost a
  partition (a hot failure, an incomplete restore, D11) would number a new
  part `part-00000` and seal a second file under a path that cold already
  holds, which breaks D5. With the union a path is never reused, whatever
  hot holds.
- **Eligibility.** A partition may leave hot when
  - its date is older than the hot window (D8),
  - it holds no `.tmp` file,
  - the copy ledger records k verified copies (D11) of every sealed file
    in it, and
  - the copy-ledger records of those copies lie in the prefix of the copy
    ledger that k verified ledger copies hold (D11). Why: the surviving
    ledger copies alone then list every removed partition after a hot
    loss, and the rebuild's root listing (D7) is a cross-check, not the
    only source. The cost is that removal waits for the next replication
    pass, at most one seal interval.

  Removal therefore never takes a file below k copies.
- **The source lock.** The remover removes a source's partitions only
  while it holds that source's writer lock (`flock`, ADR-030; the archive
  has its own, ADR-034 D5). The live source's lock is held by `mie ingest`,
  in which the remover runs; for the archive source it tries the lock and
  skips the source for that pass when an import holds it. Why: the
  "no `.tmp` file" check and the rename are otherwise a check-then-act race
  with a writer in another process, which could open a part in the
  partition between the two and lose it with the directory.
- **The removal.** Holding the lock and the copy-ledger lock (D7), the
  remover appends the removal record and fsyncs the ledger. Only then is
  the partition directory renamed atomically to a hidden name outside the
  layout (recovery and `select` read only `date=` entries) and deleted.
  The record-before-rename order is what makes the union `select` exact
  (D7).
- **The writer never appends to a partition that is not on hot.** An
  append to a stream-date that the copy ledger records as removed from hot
  (a late archive file, a catch-up after an importer outage longer than
  the hot window, the 23:55 `metrics` row of a day whose own file arrives
  late, ADR-034 D3) first **recalls** the partition, holding the source
  lock: every sealed file of the partition is copied from a cold copy back
  to hot by the D5 protocol and verified, then a "returned to hot" record
  is appended to the copy ledger. The partition is then an ordinary hot
  partition, the append takes the next part number (union rule above), the
  replicator copies the new part, and D6 removes the partition again once
  it is eligible.
  - Why a recall and not a refusal: a refusal turns every late archive
    file into a permanent hole, which only a new ADR could fill (the
    lost-file rule, D11), although the file was never lost. Rejected,
    gating removal of archive partitions on an import window: a catch-up
    after an outage has no bound, so the window only makes the case rarer.
  - Why the recall may be automatic while repair is explicit (D11): it
    copies verified sealed bytes to a new location and creates no new
    identity, so it cannot spread a shared fault the way a rewrite of bad
    copies could.
  - A late import changes the dataset version of every window that covers
    its day, as any late import does today (ADR-034). Whether such an
    import is wanted for a day inside the chain's archive segment is
    ADR-052's question; this ADR only keeps it possible.
  - Presence on a cold root is not the signal for "removed", because under
    D11 today's partition is on cold too.
  - Without a readable copy ledger (missing, or failing its hash chain
    before the last record) on an intact hot (the degraded start, D7) the
    writer opens only dates inside the hot window, and the importer
    refuses to start. On an intact hot
    D6 never removed those, and the union rule is not needed because hot
    still holds every part of them.
- **As implemented (#191).** The rules above, in the writer of a tiered
  store (`RawWriter`, `mie-adapter-parquet`), the remover (`removal.rs`)
  and `mie ingest`:
  - **Union numbering from a snapshot.** The writer reads the copy ledger
    once when it opens and folds it for its source: the partitions whose
    latest `removed` / `returned` record is `removed`, and per partition
    the highest part number any record names (the `manifest` texts and
    the `copy`, `lost`, `hold`, `hold-cleared`, `quarantined` and
    `replaced` paths). It keeps that snapshot current with its own seals,
    recalls and removals, and a new part takes the larger of hot's next
    number and that highest part plus one. Why the snapshot is enough:
    every writer of `removed` / `returned` records for a source holds that
    source's writer lock (the adoption, the remover, the recall), so none
    runs beside the open writer but through it, and the replicator only
    records parts hot already holds, so it never raises a partition's
    highest part above hot's. The cost: every writer open on a tiered
    store parses the ledger and checks its chain once, about 0.58 GB after
    one year (the Numbers table), once per `mie ingest` start and per
    import, never per part.
  - **The recall.** Before it opens a part in a partition the snapshot
    records as removed, the writer reads the ledger fresh (a recall is
    rare, so a full read is acceptable there) and, when the partition is
    still recorded removed, copies back every recorded path of it with an
    ordinary verdict (`select`'s rules, D7), one function
    (`replica::copy_back`) that the hot restore's step 3 reuses. A held
    path refuses the recall (`mie store resolve`), and so does a path with
    two manifest texts or without its manifest; lost, replaced and
    replacement-pending paths are not copied, since no root holds them in
    the layout, but the numbering still counts them. Each path comes from
    the first cold root in read order that holds a verified copy: a root
    without the copy is skipped as in `read`; a root whose manifest text
    differs from the ledger's is `Integrity` (two versions of one path,
    D5), and no other root is tried; a copy that fails its manifest, or
    that cannot be read, is reported as damaged (the import's output, the
    `recall` journal line) and the next root is tried (operator, #191
    STOP 1 decision 1). Why the fallback: a late import must never stop
    on a fault that a good copy already covers, and the hot restore does
    the same with the same function. The cost: the damage found by a
    recall is not recorded in the ledger; the verification (`mie store
    verify`, #192) checks every copy and finds it again. Only then is
    `returned` appended. Any failure appends nothing and poisons the writer: the partition stays
    recorded removed, the next recall resumes it (files already on hot
    are `AlreadyPresent`), or the remover finishes its removal again.
  - **The degraded writer.** Without a readable copy ledger (missing, or
    failing its chain before the last record) the writer opens only dates
    on or after the first hot date taken when it opened, and refuses an
    earlier one with `Integrity` before anything is created. Why the date
    at open is enough: D6 removes only dates before the window at the
    time of removal, and the window only moves forward, so no date at or
    after the open-time first date was ever removed.
  - **Eligibility is planned by the replication pass and re-checked in
    the seal path** (operator, #191 STOP 1 confirmation 2). The pass of
    `mie ingest` ends with a fresh ledger read, one walk of hot and a pure
    plan over both (rules (a) to (f): the window; not recorded removed;
    every configured copy target presented its marker in the pass; no
    lost, held, conflicting or replacement-pending path in the partition's
    file set, which is hot's sealed files plus every path the ledger
    records there, replaced paths excluded; a `manifest` record whose text
    hot's manifest equals, and a `copy` record per target id with no
    `finding` about that copy after it, for every file in it; and those
    records inside P, the smallest latest `ledger-copy` prefix of those
    ids). A hot manifest that differs from the ledger's is a D5 conflict:
    the partition stays on hot and the pass names it as a failure, since
    removal is the one irreversible step and `select` already fails on the
    path (review of #202, finding 2). A `finding` voids that root's copy
    until a later `copy` record, the same rule the re-check applies to
    findings after the plan (finding 3). The
    seal path re-checks it under the source lock and the ledger lock,
    cheaply: the ledger's tail since the plan (verified onwards from the
    plan's last chain hash, so a replaced ledger drops the plan) names
    neither the partition nor its files except in `manifest`, `copy` and
    `ledger-copy` records, the date is still before the window, and the
    directory holds no seal debris, no file the plan did not see and no
    manifest that differs from the ledger's. Why:
    the pass already folds the whole ledger every 300 s on its own thread,
    while a second full read on the capture thread would mean parsing
    about 0.58 GB per seal pass after a year, where a multi-second stall
    costs irreplaceable live data. Both locks are taken without waiting: a
    held ledger lock skips the pass until the next seal pass.
  - **k counts the ids of the copy targets that presented their marker in
    the planning pass** (operator, #191 STOP 1 confirmation 3), and the pass
    plans only when every one of them was written to the end, its ledger
    copy included: a ledger copy refused as needing the repair holds no
    verified prefix, so its older `ledger-copy` record must not count for
    P (review of #202, finding 1). Why: a target replaced under a new id
    would otherwise still count with the old id's records. The cost: nothing is removed while a target is
    unmounted, and hot grows for the length of the outage, which D10's O
    term sizes.
  - **The hidden name** of a removed partition is `.removed-date=YYYY-MM-DD`
    in its stream directory, so the rename stays on one filesystem and is
    atomic, and recovery, `select` and the replica walks, which read only
    `date=` entries, never see it. A crash after the record leaves the
    partition recorded removed with its directory on hot, or a hidden
    leftover; the next pass finishes both without a second record, and a
    partition with a `returned` record after the plan is left alone.
  - **The archive source's lock is tried only when the plan names an
    archive partition or leftover**, about once per imported day, and
    held for hot-only work that lasts milliseconds. An import that starts
    inside that hold is refused with `Locked`, naming the holder, and a
    re-run succeeds.
  - A single-root store never reads the ledger.
- **As implemented (#192): a damaged ledger copy holds no verified
  prefix.**
  - **L1.** The plan reads one copy state, folded once from its ledger
    read (`verification.rs`), which `mie store verify` and `mie store
    repair` read too. For each target, a `finding <root> … copy-ledger`
    after its latest `ledger-copy` record voids that record until a later
    one, so with it there is no P and no candidate; the re-check treats a
    copy-ledger finding in the tail since the plan as naming every
    candidate. Why: rule (f) requires the records to lie in the prefix
    that k *verified* ledger copies hold, and a copy that a verification
    found damaged holds no verified prefix, while the plan before #192
    read every `ledger-copy` record whatever followed it. L1 only ever
    removes candidates.
  - **L2.** The replication pass never extends a ledger copy with an open
    copy-ledger finding, and never appends its `ledger-copy` record; it
    names the target as needing `mie store repair` and is not complete
    for rule (c) to (f), so `mie ingest` plans no removal. Only the
    targets that the repair's closing pass names as rebuilt are exempt.
    Why: the next pass's `ledger-copy` record would otherwise end L1's
    void with no repair at all, over a copy whose damage may lie before
    its tail, where the pass never looks. The verification takes no pass
    lock (V10), so a finding can land after the pass read the ledger. The
    pass therefore re-reads the records after its read, from its offset
    and chain (as the remover's re-check does), before it extends a copy,
    and appends the `ledger-copy` record with the same check under the
    same hold of the ledger lock (`CopyLedger::append_unless`); a finding
    on the target there holds the copy like one in the fold (review of
    #203, finding 1). Why under the lock: a finding appended between an
    unlocked check and the record would still be closed by it. AW11 pins
    the other order, a pass inside the verification's window.
- **As implemented (#193): rule (g), removal waits after a rebuild.**
  After `mie store rebuild` the plan reads the ledger first and the gate
  `.mie/rebuilt` second (D7 *Formats and layout*). With the gate holding
  L, it plans no candidate unless L ≤ the read's verified length and
  every target id has a `verify` record whose prefix is at least L; the
  `replication` journal line names why in `removal_waiting`. A gate that
  cannot be read or parsed gives no plan and a problem. Why prefix ≥ L is
  exact: a `verify` record's prefix is hot's verified length when that
  verification read its snapshot; every `verify` record inside the start
  copy lies below its own offset, so below L, and the verify lock keeps a
  verification from running across the install, so only one that read
  the rebuilt ledger qualifies. Why the gate is written before the
  ledger's rename: a read that sees the rebuilt ledger always finds its
  gate, and a gate without its ledger only delays removal. A plan made
  before the install is dropped by the remover's re-check, which sees a
  replaced ledger. With L = 1 000: prefix 999 waits, 1 000 and 1 001
  plan, one of two targets verified still waits. Incomplete removals and
  leftovers are not affected: their records are already written.
- **A partition that disappears while it is read** is never read as
  empty; D7 states the rule.

### D7. Resolution: how replay and `mie equivalence` find moved data

- **One multi-root store** in `mie-adapter-parquet` sits behind the
  unchanged `RawRecordSource` port.
  - `select` enumerates the hot partitions as today, plus **every file
    the copy ledger records** (its manifest records and lost entries),
    whatever the partition's state (on hot, removed, returned), and merges
    them by `relative_path`. A file in both is the same file (D5). It never
    lists a cold root. A selection that covers a held archive file (D11),
    or a quarantined one whose replacement entry does not exist yet, fails
    with `Integrity` naming the file and the resolution step, like a lost
    file; it is never answered shorter. A quarantined file whose
    replacement entry exists is not listed: the re-import's parts replace
    it.
  - Why every file and not only the removed partitions: a partition also
    reappears on hot gradually, file by file, in a recall (D6) and in the
    hot restore's step 3 (D11). A `select` that lists hot in the middle of
    that and reads the ledger after the "returned to hot" record would
    otherwise hold only the files already copied back, a shorter selection
    with another dataset version and no error. Every file in the ledger
    was copied to a copy target before its record (D5), so the ledger's
    file list never needs hot. The cost is reading the whole ledger per
    `select` (about 1.6 MB per day of capture, Numbers table); an index is
    an implementation detail. Rejected, staging each recall or restore in
    a hidden directory renamed into place before its record: it closes the
    same race, but only as long as every path that ever writes a partition
    back obeys it, while the union over the ledger holds whatever hot
    shows.
  - **The order is fixed: hot is listed completely first, then the copy
    ledger is read from disk**, fresh for every `select`, never from a
    copy cached at construction. Why: a file's records precede the
    removal of its partition (D6, eligibility), and the removal record
    precedes the rename. A file missing from the hot listing was
    therefore renamed away before the listing ended, so its records are
    in any ledger read after the listing. The other order can lose it: a
    ledger read that ends before a file's records are appended, then a
    hot listing after its partition was removed, holds the file in
    neither source, and the selection would be shorter, with another
    dataset version and no error (for the archive segment, an ADR-052
    hole that "holes stay holes" keeps). A ledger cached in a long-lived
    process has the same defect, over a much longer interval.
  - **A partition that disappears during the hot listing** (its `date=`
    entry was listed, its directory is gone when its files are listed) is
    never an empty partition. Today `sorted_entries` maps `NotFound` to an
    empty list (`mie-adapter-parquet` `lib.rs`); the union store instead
    takes that partition's files from the ledger read that follows, and if
    the ledger records none of its files, `select` fails with
    `Integrity`, naming the partition. **A manifest listed on hot but gone
    when it is read** (the D6 rename can land between the partition's
    listing and the read) is the same case for one file: it comes from the
    ledger, and if the ledger has no record of its path, `select` fails
    with `Integrity`, naming the file (#188).
  - **The merge and its checks run over the selected partitions before the
    event-time filter** (#188). Every file of the selected streams and
    dates, from hot and from the ledger, is merged by path first; a path
    whose hot manifest and ledger manifest differ by a single byte is
    `Integrity` (D5). Why before the filter: a conflict in the event times
    could otherwise drop the file from one side's filtered list and the
    selection would silently follow the other. Only then are the ordinary
    files filtered by event time.
  - **The ledger's view of a path** is folded from its records in file
    order: `manifest` gives the manifest (two different texts are a D5
    conflict), `lost` marks it lost, `hold` holds it, `hold-cleared
    readmit` clears the hold, `hold-cleared replace` clears it and marks a
    replacement pending, `quarantined` marks the replacement pending under
    its quarantine id, and `replaced` names the re-import's entry. `copy`,
    `removed`, `returned`, `ledger-copy`, `finding` and `verify` do not
    change the file list. The verdict, in this precedence: lost, a D5
    conflict, held, replaced (not listed; a replaced path still listed on
    hot is `Integrity`), replacement pending, otherwise an ordinary file. A
    `hold-cleared replace` never makes a path ordinary again: a selection
    covering it fails until the `replaced` record exists (D5). **Write
    order (a precondition for #194):** `replaced` is appended only after
    the re-import's entry and every part it lists are sealed on hot. Why:
    the fold takes `replaced` as its proof that the replacement exists and
    drops the old path at once, so a `replaced` written first would let a
    crash, or a `select` running in between, answer without either file,
    a shorter selection with another dataset version and no error.
  - **Coverage of a path that fails.** A lost, held or pending path with a
    manifest (from hot or the ledger) fails a selection whose window
    overlaps its event times, as an ordinary file would be selected. A
    path no manifest describes (a `lost` record of journal or import
    origin, D7 rebuild step 4) fails every selection of its stream that
    covers its date (operator, #188 STOP 1 decision 2). Why: the `lost`
    grammar carries no event times, and adding them is a ledger format
    change; the whole-day rule fails more selections than strictly needed,
    loudly, and never answers one shorter.
  - **The first error in path order.** When several paths fail, the error
    of the lowest relative path (a partition sorts before its files) is
    returned, so the message does not depend on the listing order.
  - A reader ignores the one incomplete trailing ledger record (the
    single-trailing-record rule, *Formats and layout* below): it can only
    be a record being appended, and a removal record is complete before
    its partition is renamed (D6).
  - `manifest::dataset_version` then sees the same sorted file list, so it
    yields the same version as a single root holding every file.
  - `read` resolves the path on hot, then on each cold root in order. An
    absent copy is skipped. The first present copy is verified as today
    (`manifest::verify`), and a mismatch is `Integrity` (D5), never a
    fallback to the next copy. Any other read error than an absent copy is
    `Io`, naming the root, and no fallback either.
  - This needs no port change, no new crate, no new dependency and no
    ADR-025 change. `mie-cli` wires it, every command through one
    constructor. With `cold_roots` empty that constructor refuses a
    `raw_root` that holds `.mie/` (a hot-only stat): only a tiered store has
    one, and a configuration that omits its `cold_roots` (an archive
    configuration written before the store was tiered, where the key
    defaults to empty) would read hot alone, a shorter selection without an
    error once partitions leave hot (#198 review).
- **The copy ledger** is an append-only text file at
  `<raw_root>/.mie/copy-ledger`: on hot's local filesystem (D4), next to
  the journal and the import ledger, and outside the layout (`select` and
  recovery walk only `source=` entries). Its first line is the header
  `mie-copy-ledger 1`. It records:
  - for each sealed file, the manifest text verbatim, written with its
    first verified copy (by the adoption, before its first copy, D10);
  - for each verified copy, the file, the root id and the time;
  - for each partition removed from hot, the removal (D6), and its return
    (D6 recall, D11 hot restore);
  - for each verified ledger copy, the root, the prefix length and its
    SHA-256 (the input of D6's "inside the prefix that k verified ledger
    copies hold");
  - for each verification pass (D11), the root, the ledger prefix covered
    and the findings;
  - each lost file, each hold and its clearing, each quarantined path and
    its replacement (D11).

  Each record ends with its **chain hash**: the SHA-256 of the previous
  record's chain hash (its 64 lowercase hex characters) followed directly
  by this record's own text (its line without the chain hash). The chain
  before record 1 is the SHA-256 of the header line. Why a chain: with
  k = 2 a mismatch between two copies of an append-only file cannot say
  which one is damaged, while the chain shows in each copy the first record
  that fails, so damage is located without a majority. Each chain hash
  covers its predecessor, so it commits to the whole prefix before it.
  The copy ledger is the only listing of removed partitions that depends on
  no mount, and it is the source of truth for which copies exist.
  - Why this form (operator, #187 STOP 1). The earlier wording, each record
    ending with "the SHA-256 of the ledger up to and including the previous
    record", has three defects. The last record is covered by no hash, so
    damage in it is invisible. A byte flipped in record n fails only record
    n + 1's hash, so damage is reported one record late, and a per-copy
    report of "the first record that fails" names the wrong record. And
    every append re-hashes the whole ledger: after one year about 0.58 GB
    (1.6 MB per day × 365, Numbers table), about 6 100 times a day (1 835
    files × 3 records, a manifest and k = 2 copies, plus 2 × 288 ledger-copy
    records, Numbers table), about 3.5 TB hashed per day. With the chained
    form, a byte flipped in record n fails record n's own hash, and an
    append reads only the last three records. The intent of the approved
    decision, locating damage per copy without a majority, is unchanged.
- **One appender at a time.** Every process that appends to the copy
  ledger (the replicator and the remover in `mie ingest`, the recall in the
  importer, the verification, repair, rebuild and hot-restore commands)
  holds an exclusive `flock` on the copy ledger's lock file for each
  append: read back the last records, write the record, fsync, release
  (the append protocol in *Formats and layout* below).
  The rebuild and the hot restore hold it for their whole run, and refuse
  to start when they cannot take it. Why: two appenders that read the same
  tail hash write two records chained to it, the chain fails on hot and on
  every copy, and the verification reports damage it cannot attribute to
  one copy, which is what the chain is for. A lock is chosen over a single
  writer process because the verification, repair and import run as
  separate commands, also while no capture runs. `flock` is reliable here
  because the ledger is on hot, a local filesystem (D4). Readers take no
  lock (the incomplete-record rule above).
- **Formats and layout.** Format v1 of every file under a root's `.mie/`:
  - **The `.mie/` directory** of every root holds `root`, the root marker.
    On hot it also holds `copy-ledger`, its lock file `copy-ledger.lock`,
    and `replicate.lock`, the replication pass lock (#189): a pass takes an
    exclusive `flock` on it without waiting and holds it for the whole
    pass. Why: two passes at once (the replicator's thread in `mie ingest`
    and a manual `mie store replicate`) would both read a copy's length and
    append the same tail twice, so the copy would stop being a prefix of
    hot's file. Rejected, holding the copy-ledger lock for the pass: the
    pass does cold I/O, and the remover appends from the seal path, so a
    hung mount would stall the capture. On a copy target it also holds,
    from the replicator on, `copy-ledger` (the ledger copy, D11),
    `journal/<journal file name>` (the journal copies) and
    `import-ledger/<segment>/<name>.import` (the import-ledger copies).
    On hot it also holds `verify.lock` (#192): `mie store verify` and
    `mie store repair` take an exclusive `flock` on it without waiting and
    hold it for their whole run. Why: a verification records findings
    only about copies its ledger read already records, and the repair is
    the only writer of a second `copy` record for such a copy, so the lock
    keeps a finding from voiding a copy the repair made after that read
    (D11 *As implemented*, V10). Every root may also hold `damaged/<at_ms>/`
    (#192): `mie store repair` moves each damaged copy there under its
    relative path instead of deleting it, one directory per run, and `mie
    store rebuild` (#193) puts hot's former ledger there as
    `damaged/<at_ms>/copy-ledger` (a hard link) and moves there each copy
    target's ledger copy that is no prefix of the rebuilt ledger. On hot,
    after a rebuild, `.mie/rebuilt` is the removal gate, `mie-rebuilt
    1\nprefix <L>\n` with L the rebuilt ledger's length (D6 rule (g),
    rebuild B10), written by temp name, fsync, rename and directory fsync
    before the ledger's rename; its parser is strict. The rebuild holds,
    for its whole run, the copy-ledger hold, `replicate.lock`,
    `verify.lock` and the archive source's writer lock, each taken without
    waiting. Nothing in mie lists or deletes `damaged/`, and it is outside the layout, so
    `select`, recovery and every walk of the layout never see it. `.mie/`
    is outside the layout, and nothing else in mie writes there.
  - **The hidden directory of a removal**, `.removed-date=YYYY-MM-DD`
    beside the partition on hot (D6 *As implemented*), lies outside the
    layout too: it exists only between the rename and the deletion, or
    after a crash there until the next remove pass deletes it.
  - **The root marker** is the text `mie-root 1\nid <id>\n`: a versioned
    header and one `id` line; the id passes `validate_segment`. The parser
    is strict: exactly that text. The id `hot` is reserved for hot's
    marker. Why reserved: copy-ledger records only ever name copy targets
    (D11, the hot copy never counts), so an operator-chosen hot id would
    carry no information, and a fixed id lets every check recognize hot's
    marker under any spelling of its path.
  - **Records.** One line per record: `<kind> <at_ms> <fields…> <chain>`,
    tokens separated by single spaces, no empty token, `\n` line ends.
    `at_ms` is a canonical decimal UTC epoch millisecond in the raw
    store's event-time range (`0 … 9999-12-31T23:59:59.999Z`, ADR-030),
    supplied by the caller. `chain` is 64 lowercase hex characters. The
    kinds:

    | Kind | Fields after `<kind> <at_ms>` | First written by |
    |---|---|---|
    | `manifest` | the escaped manifest text | the first verified copy; the adoption before its first copy (D10) |
    | `copy` | data path, root id | each verified copy; the repair, for a copy it re-verified or replaced (D11) |
    | `removed` | partition directory | hot removal (D6) |
    | `returned` | partition directory | the recall (D6), the hot restore (D11) |
    | `ledger-copy` | root id, prefix length in bytes, prefix SHA-256 | the replicator (D11) |
    | `finding` | root id, code, subject | the verification (D11) |
    | `verify` | root id, ledger prefix length covered, finding count | the verification |
    | `lost` | data path, origin, SHA-256 or `-` | `mie store repair --record-lost` (D11, origin `verify`), the rebuild (D7 step 4) |
    | `hold` | data path, reason | the hot restore, the rebuild (D11) |
    | `hold-cleared` | data path, resolution | the hold resolution (D11) |
    | `quarantined` | data path, quarantine id | the *replace* resolution (D11) |
    | `replaced` | data path, import entry | the *replace* resolution |

  - **Field values.** A data path is a sealed data file's relative path
    exactly as the layout writes it, a partition directory is
    `source=…/instrument=…/stream=…/date=YYYY-MM-DD` exactly as the
    layout writes it, and root and quarantine ids pass `validate_segment`.
    An import entry is `<segment>/<name>.import`, the name from
    `[A-Za-z0-9._-]` and not starting with `.`, as the import ledger names
    its files (ADR-034 D4). The closed sets: code `missing size sha256
    manifest copies prefix chain stale entry` (D11 *On a finding*); subject
    a data path, `journal`, `copy-ledger` or `entry:<entry>`; origin
    `ledger journal recovery import verify` (rebuild step 4, the lost-file
    rule); reason `unlisted entry-conflict manifest-conflict` (D11, hot
    restore step 5); resolution `readmit replace`. A pass's findings come
    before its `verify` record and carry the same root and `at_ms`.
  - **Escaping** (the `manifest` field only): `%`, the bytes 0x00–0x20 and
    0x7F are written as `%XX` with uppercase hex; every other byte is
    literal, and any other spelling is refused. Why these bytes: the space
    separates tokens and `\n` ends the record, so both must never appear
    raw in a field, the other control bytes are escaped with them so a line
    never holds a byte a text tool could alter (a tab, a `\r`), and `%` is
    the escape character itself. One spelling per byte keeps the line
    canonical, so the chain hash of a given text is unique. The unescaped
    text must parse as a manifest (ADR-030) and is stored verbatim, never
    re-rendered.
  - **The parser is strict**: a line that the renderer cannot produce is
    corrupt, never skipped. A later step adds a kind only together with an
    edit of this ADR and a golden test, and a format change bumps the
    header's version.
  - **The single-trailing-record rule.** Only one append is in flight at a
    time (the lock), so a crash leaves at most one incomplete record, the
    last. A reader ignores exactly that record: the bytes after the last
    `\n`, if there are any, or otherwise the last line when its chain
    fails while the line before it verifies. Any other chain failure is
    `Integrity` naming the ledger, the record number and its byte offset;
    a line whose chain verifies but whose grammar fails is corrupt and is
    never ignored, since only an appender can have written it. The rule
    cannot tell a torn append from damage at rest in the last record; the
    verification's prefix check against the copies (D11) finds the latter.
  - **The lock file** `<raw_root>/.mie/copy-ledger.lock` is separate from
    the ledger. Why: the rebuild replaces the ledger by a temp file and a
    rename (step 6), and a lock on the ledger's inode would stay on the
    replaced file.
  - **The append protocol.** Refuse a missing ledger (an error, never an
    implicit new one), take the lock, and only then open the ledger. Why
    this order: a whole-run holder (the rebuild) may replace the ledger by
    rename while an append waits for the lock, and a descriptor opened
    before the lock would write the record into the replaced file, where
    no reader ever sees it. Under the lock, read back the header and the
    last three records, and decide before
    writing anything. Either truncate at most the one trailing record
    above and append, or refuse with `Integrity` and leave the file
    byte-identical. Then write the line in one write, fsync, release. Why
    three records: the last two are the ones whose chains the decision
    checks (the trailing record and the line before it, which must
    verify), and the third supplies the chain hash the second-last is
    computed from. A complete record before the last is never truncated.
    Every truncation is reported to the caller, which journals it, as
    structured fields (the truncated bytes and the offset the ledger was
    truncated to): in the append's outcome, or, when the fsync of the
    truncation or the write after it fails (a full hot), in the append's
    error, which carries the truncation next to the failure (#189, #197
    review finding 5). The replication pass hands every truncation to its
    observer before it acts on the append's result: `mie store replicate`
    prints it as a report line of its own, before any `FAIL` line, and `mie
    ingest` maps it to a capture-journal line, `copy_ledger_truncated`
    (the ledger, the truncated bytes and the offset), for the passes of
    its replicator, the remover's appends and the live writer's recalls
    alike. The importer prints a recall's truncation as a report line. The
    standalone command never writes the capture journal (operator, #189
    STOP 1). Why: a second process appending to it while `mie ingest`
    writes it through a buffered writer can land inside a half-flushed
    line, and a torn `run_start` drops that run from `mie replay` (ADR-039
    D2). The cost: a truncation by a manual pass lives in that command's
    output only. Damage
    older than the last three records is not seen by an append; it stays
    in the file for readers and the verification to report.
  - **The whole-run hold** of the rebuild and the hot restore takes the
    lock without waiting and refuses to start when another process holds
    it; its appends go through the held lock.
- **Configuration.** An ordered list `cold_roots` (default empty, the read
  order) in `[paths]` of the ingest and the archive configuration
  (`config.rs`, `Paths` and `ArchivePaths`):
  `cold_roots = [{ path = "…", copy_target = true }, …]`. Unknown keys are
  refused. Both keys are required, because either default fails silently:
  `false` hides a forgotten copy target until a file is short of k copies,
  `true` turns a read-only root into a write target. A non-empty list
  needs at least 2 copy targets (k = 2, D11; Numbers table), no path twice
  and no entry equal to `raw_root`. These comparisons are lexical, never
  resolved through the filesystem. Why: loading the configuration must
  never touch a cold mount, since a hung mount would block the start of
  `mie ingest`. Two spellings of one directory are caught by the markers
  instead: `init-root` refuses the second spelling as non-empty, and the
  replicator refuses two roots with one id. The `[history] archive_config`
  check that both configurations name the same `raw_root` (`config.rs`)
  extends to the same `cold_roots`: the same entries in the same order
  (the read order) with the same flags.
- The journal, the import ledger and the copy ledger stay on hot and never
  move. D11 copies them.
- **A file absent from every root is a loud error** (operator, STOP 1).
  When the hot listing or the copy ledger names a file and no root holds
  it, `read` fails with `Integrity`, naming the file and every root it
  tried (the existing `NotFound` → `Integrity` path in `reader.rs`). The
  file is never treated as an empty partition, a hole or a shorter
  selection.
  - Why: mie does not monitor mount liveness, and an unmounted mount point
    reads as an empty directory. A union built from cold listings could
    not tell an outage from "no data". It would turn the outage into
    ADR-052 holes, which "holes stay holes" then keeps, and it would
    change the dataset version without an error, since ADR-052 prints that
    version but never compares it.
  - This replaces the earlier plan rule that "an unreachable cold root is
    an error", which needed a liveness probe.
- **A missing copy ledger is the same error.** A store with `cold_roots`
  configured requires the copy ledger to exist; it is created by an
  explicit init step, never implicitly. The init step, `mie store init
  --config <ingest config>`, also writes hot's own root marker. It
  refuses, in this order: an empty `cold_roots` (a single-root store needs
  no `.mie/`, so a `.mie/` always means a tiered store); with `[history]`,
  a missing `raw_root`, named with the instruction to create it (the
  check that follows resolves both raw roots on disk), and an archive
  configuration that fails the check above; an existing
  `<raw_root>/.mie`; and a configured cold root that carries a marker or a
  copy-ledger copy, where any read error other than "not found" also
  refuses. A cold marker next to a hot without one always means a hot that
  was initialized and then lost, because `init-root` needs hot's marker
  (D4). `init` writes the marker and a header-only ledger into a staging
  directory of its own (a unique name per call), fsyncs them, and renames
  the directory to `.mie`. Why a directory rename: after a crash there is
  either no `.mie/` or a complete one; separate file renames could leave a
  marker without a ledger, which looks like the degraded start below, or a
  ledger without a marker, which looks like a lost hot. Why a unique
  staging name: two concurrent calls sharing one staging directory could
  each delete or fill the other's files and publish an incomplete `.mie/`;
  with separate directories the first rename wins, the second fails on the
  existing `.mie`, and the winner removes every leftover staging
  directory. The same creation writes a cold root's marker (D4). It prints what it found on each cold root, so an
  operator who expected a restore sees that the mounts are absent. A
  missing ledger makes every union `select` fail with `Integrity`. Why: read as empty, it would drop every removed partition
  from every selection without an error, the failure the previous rule
  exists to prevent. Two cases are told apart by hot's marker:
  - **Hot intact, ledger lost** (the marker is there): the degraded start.
    `mie ingest` still starts and captures: the writer opens only
    hot-window dates (D6), the seed scan reads hot alone (*Seeds* below),
    the warm-up fails into a cold run (ADR-052 D8), and the replicator and
    hot removal stop until the rebuild below. The importer, its dry run
    included, refuses on a missing or unreadable ledger (a chain failure
    before the last record included), before it opens anything, since its
    days can lie outside the window. Why the capture may continue: hot
    still holds every part and the journal, so nothing it writes can
    collide with cold.
  - **Hot lost** (no marker, `cold_roots` configured): `mie ingest` and
    the importer, its dry run included, refuse to start before they open
    anything, and name `mie store init` (a new store) and the hot restore
    (D11, a lost hot). The check reads hot's marker only; with
    `cold_roots` empty it makes no filesystem call. A `raw_root` that
    carries a cold root's marker is refused too. Why: on
    an empty hot the writer would number new parts from zero, the journal
    would start as a new file without the earlier runs, and the seed scan
    would find nothing (the three failures D11's hot restore prevents).
    The init step refuses a hot without a marker when a configured cold
    root already holds a copy-ledger copy, so a lost hot cannot be
    re-initialized as a new store by mistake.
- **The copy ledger is rebuilt from manifests when it is lost** (operator,
  STOP 1 approval). The rebuild is an explicit `mie` step that the
  operator runs, never automatic:
  1. **Start** from the longest copy of the copy ledger on a cold root
     whose hash chain verifies (D11 keeps k copies). It lacks at most the
     records since the last replication pass, plus any cold outage (O,
     D10). If no copy verifies, the rebuild starts empty and steps 2–6
     still run: the root listing and the step 4 cross-check against the
     journals and the import ledger are then the only protection against
     leaving a file out, and the rebuild says so in its output.
  2. **Every configured root must present its marker** (D11), otherwise
     the rebuild stops. Why: the rebuild and the hot restore that runs
     it (D11) are the only steps that list cold roots, and the marker is
     what tells an unmounted root from an empty one.
  3. **List every manifest in the layout of every root** (the `.quarantine/`
     directory is outside the layout and skipped here; step 5a reads it)
     and verify each data file against it (`manifest::verify`). Each
     verified copy becomes a copy record, with the manifest text verbatim.
     A file that the starting copy already lists must carry a
     byte-identical manifest text, and the same path with differing
     manifests on two roots is `Integrity` (D5). On a live path the
     rebuild stops on either; on an archive path it is a hold (step 5a,
     D11), because the archive must never stop it (D11 hot restore,
     step 5).
  4. **Cross-check against independent records of every seal:** the
     starting ledger copy, every capture journal's `sealed` records
     and `recovery` `rolled_forward` paths (live files), and every
     import ledger entry's `file` lines (archive files, with their
     SHA-256). For each archive file only its **latest entry that no
     supersede record names** counts; files listed only by a
     superseded entry are expected in quarantine (step 5a), never
     lost. The journals and the import ledger, entries and supersede
     records alike, are k-copied too (D11). A path any of them names
     that no root holds enters the rebuilt ledger as **lost**, with
     what that record knows of it; it is never left out. Why: a
     listing of roots alone cannot tell a missing file from one that
     never existed, and these records were written at seal time. A
     `select` covering a lost file then fails with `Integrity`, as in
     D11's lost-file rule.
  5. **Every partition with verified cold copies and no directory on hot
     is recorded as removed from hot.** Why: an append to it must recall
     it first (D6), and D6 must not try to remove it again. On a lost hot this
     runs inside the hot restore (D11) after the hot-window partitions are
     back on hot, so it never marks those as removed.
  5a. **Archive holds and quarantine are re-derived, never copied from a
     ledger.** The rebuild reads each root's `.quarantine/` directory and
     records every file in it as quarantined under the supersede record
     that names it. It then runs the hot restore's archive checks (D11,
     step 5) against the import ledger as resolved in step 4: parts that
     no current entry lists, conflicting entry copies with no matching
     one, and archive paths with differing manifests become holds again;
     a supersede record with no newer entry for its file makes every
     selection covering its quarantined paths fail, as before the rebuild.
     Why: the durable sources of that state are the import ledger
     (entries and supersede records, immutable, replicated) and the
     quarantine directories (moved within each root, never deleted), while
     hold records in the copy ledger exist only in the copy that may be
     lost. Re-deriving gives the same holds and the same selections
     whether the rebuild starts from a full copy, a stale one or empty.
     Rejected, taking holds from the starting ledger copy: an empty or
     stale start would turn held parts into ordinary files, and a
     superseded entry would turn quarantined parts into lost files that
     fail every selection of the day for good.
  6. **The rebuilt ledger** is written under a temp name, fsynced and
     renamed into place. The rebuild prints the starting copy's length,
     the records it added and every lost file. The next replication pass
     copies it, and hot removal resumes only after the next full
     verification pass.
- **As implemented (#193): `mie store rebuild --config <ingest config>
  [--archive-config <path>] [--record-lost] [--replace-hot-ledger]`.** The
  ledger grammar does not change (header `mie-copy-ledger 1`, no new
  kind). The rules, numbered as in the #193 plan (the tests cite them):
  - **B0 Refusals and locks.** Refused with nothing written: an empty
    `cold_roots`, a hot without its marker, an archive configuration of
    another store; a configured root, copy target or read-only root,
    without its marker or with an unreadable one (step 2: the marker is
    what tells an unmounted root from an empty one), a cold marker with
    hot's id, one id on two roots, hot and the copy targets not on
    pairwise distinct devices. Then, each without waiting and held for the
    whole run, in this order: the copy-ledger hold (*The whole-run hold*),
    the pass lock (no pass extends a ledger copy or appends mid-run), the
    verify lock (no verification reads a snapshot the install replaces,
    which the removal gate below relies on) and the archive source's
    writer lock (no import writes parts, entries or `PENDING` mid-run).
    The live source's lock is not taken: `mie ingest` keeps capturing,
    its degraded writer opens only hot-window dates, and its remover and
    replicator need the ledger or the pass lock. Hot's own ledger: absent,
    or unreadable as `Integrity` or `Corrupt` (the degraded start's
    "missing or unreadable"), proceeds; readable is refused unless
    `--replace-hot-ledger` is given (operator, #193 STOP 1 decision 2: a
    mistyped command must not drop findings and `verify` records); an I/O
    error is refused. Archive paths on any root or in the start copy with
    no archive configuration are refused: without the import ledger
    neither the step 4 cross-check nor step 5a can run.
  - **B1 Read order**, nothing written: every copy target's ledger copy;
    hot's journal up to the length J read now (bytes after J, and a line
    cut at J, are not parsed); every import entry on hot and on the
    targets, and hot's `PENDING`; hot's layout (manifests, `date=`
    directories); each cold root's layout in configuration order, with
    every copy's data checked against that root's own manifest (size
    before SHA-256, streamed). The journal and the entries come before
    the listings because a part is sealed before its `sealed` line and an
    archive part before the entry that lists it, so a "no root holds it"
    verdict never comes from a listing that ran ahead of the record. An
    I/O error on a listing, a manifest, the journal, an entry or a ledger
    copy stops the rebuild, since a skipped directory would turn its files
    into lost candidates; unreadable data is a damaged copy, never an
    absence.
  - **B2 Start (step 1).** The longest verified prefix among the copy
    targets' ledger copies, the incomplete trailing record dropped;
    configuration order breaks a tie; every rejected copy is named. Hot's
    own ledger is never a candidate.
  - **B3, B4, B5 (step 3).** A root *holds* a path when its layout has
    the path's manifest. The path's texts are the distinct texts that
    parse and describe its own location, from every root and the start's
    `manifest` records; a text that does not do both is a damaged copy,
    never a second text. Two texts on a live path stop the rebuild with
    nothing written; on an archive path they are a `manifest-conflict`
    hold. A copy is verified when it sits on a copy target, carries the
    path's one text and its data matches. `manifest` is appended when the
    start lacks one, `copy` per verified copy the start does not record;
    a `copy` record the start holds is never repeated, so an open finding
    stays open until the repair re-verifies the copy. A held conflict gets
    the one text whose SHA-256 its current entry lists. A copy that does
    not verify gets no record and a `FAIL:` line naming `mie store
    verify`, then `mie store repair`: findings are the verification's.
  - **B6 Lost (step 4).** The sources are every path a start record
    names, hot's journal (`sealed` `files[].path`, `recovery`
    `rolled_forward[]`; complete non-JSON lines are counted, not parsed)
    and the file lines of the current entries. A name that passes the
    layout's grammar and that no root holds (no data file and no manifest
    on hot or any cold root) is a candidate, with origin `ledger` (the
    start has its manifest) > `import` > `journal` > `recovery`; the
    output names every source. It is written as `lost` only with
    `--record-lost` (operator, #193 STOP 1 decision 1, as for the
    repair): without it nothing is written, the ledger stays missing and
    every selection keeps failing on it, never shorter. A name outside
    the grammar (look-alike characters, a trailing blank, `..`, an
    absolute path) is reported and never recorded. A path the start
    records lost that a root holds again stays lost. The lines of an
    entry held as `entry-conflict` are no source.
  - **B7 Archive holds (step 5a)** are one pure function shared with the
    hot restore's step 5, which passes no `PENDING`: entry resolution as
    in that step, `unlisted` for an archive part no current entry lists
    except the parts of `PENDING`'s stream dated in the importer's orphan
    window `[first day − 1, last day + 1]` of its period (one function of
    the importer, so the two never disagree), and one reason per path,
    `manifest-conflict` > `entry-conflict` > `unlisted`. A `PENDING` that
    cannot be read or parsed exempts nothing. A hold the start carries
    that the sources no longer derive stays held and is named: no record
    clears a hold before #194.
  - **B8 Removed (step 5).** A partition with a verified copy on a cold
    root and no `date=` directory on hot (a removal's hidden leftover is
    none) is recorded `removed` unless the start already says so. One
    dated on or after the first hot date is recorded too and named as hot
    damage: D6 never removes it, and a running `mie ingest` should be
    restarted so its writer numbers parts from the union. A partition the
    start records removed whose directory is still on hot is left for D6
    to finish.
  - **B9 Install (step 6).** Records per path in path order (`manifest`,
    then `copy` per target in configuration order), then `lost`,
    `removed` and `hold`, each in path order, all with the run's one
    `at_ms`: equal inputs give byte-identical ledgers. Every root's marker
    is re-read first (a mount lost during the listing reads as absent
    files, and `lost` cannot be undone). Then hot's ledger, if any, is
    hard-linked into `.mie/damaged/<at_ms>/copy-ledger` (never a moment
    without a ledger on hot; an existing evidence name stops the
    rebuild), the gate `.mie/rebuilt` is written, and the new ledger goes
    to a temp name, is fsynced, checked to parse whole and renamed into
    place. Last, every copy target's ledger copy that is no byte prefix of
    the new ledger is moved aside into its own
    `.mie/damaged/<at_ms>/copy-ledger`; the next pass copies the rebuilt
    ledger from byte 0. That is how the rebuild decides #192 R6: two valid
    chains are no open question once the rebuilt ledger supersedes both.
    A crash before the gate leaves only the link; after it, the old or no
    ledger next to the gate, which only makes removal wait; a re-run
    completes it.
  - **B10 Removal waits for a verification** (step 6). The gate is
    `mie-rebuilt 1\nprefix <L>\n`, L the rebuilt ledger's length in
    bytes; D6 rule (g). Why a file and not a record kind (operator, #193
    STOP 1 decision 3): a kind is a format change, a header v2 under every
    existing ledger.
  - **B11 Output.** The start, every rejected copy, the journal bytes read
    and the non-JSON lines counted, the records added per kind, every lost
    file with its sources, every hold, the gate, every ledger copy moved
    aside, then `wrote …` or `nothing written`; `PASS` and exit 0 only
    with the ledger written and no `FAIL:` line (no lost file, hold,
    damaged copy, hot-window removal, leftover start hold or differing
    entry copy), as `mie store repair` does.
  - **Cost.** The rebuild reads every copy on every root once, as one
    verification pass does (D11, P's lower bound); replication, hot
    removal and imports pause meanwhile.
  - **Invariance limits of an empty start.** It cannot see what only the
    lost ledger held: a `lost` path a root holds again, a text in the lost
    ledger only, findings and `verify` records. Selections can differ from
    a full start only there; the first contradicts the lost-file rule's
    premise, the others are double faults, and the output names each.
- **Seeds.** The capture start's seed scan (`ingest.rs` `seeds`, window
  `[now − 7 d, now + 1 d)`) calls `select` only and takes
  `max_event_time` from the manifests; it never reads a data file. With a
  readable ledger it uses the union `select`, which lists hot and the
  ledger and never a cold root, so it never touches cold. In the degraded
  start (ledger lost, hot intact) it uses hot's listing alone, which is
  complete for the window because hot holds every date of it (D8). The
  warm-up, replay, equivalence, experiments and research read data files
  through the union. A cold-tier outage at a start therefore gives an
  `Integrity` error in the warm-up, which fails into a cold run (ADR-052
  D8) and never stops the capture.
  - **The fallback rule** (operator, #188 STOP 1 decision 1): in a tiered
    store, any `Integrity` or `Corrupt` error of the union `select` (a
    missing or damaged ledger, a lost file or a D5 conflict inside the
    seed window) makes the seed scan use hot's listing alone; an `Io`
    error still stops the start. `run_start` records which listing gave
    the seeds: `"seed_listing": "union"` or `"hot"`, and with `"hot"`
    `"seed_listing_error"`, the union's error. A single-root store's
    `run_start` carries neither key. Why every such error and not only a
    missing ledger: D8 keeps every date from today − 7 on hot, so on an
    intact hot the hot listing gives the same seeds, and for a lost live
    file the earlier seed only widens the resume's `Disconnected` gap over
    data that is gone; refusing the start instead would lose raw data on
    every `mie ingest` start until the rebuild. The cost is that such a
    start does not stop on the problem: the journal keys record it, and
    the warm-up, replay and equivalence fail on the same store.

### D8. The hot window, derived

What each consumer reads, and the bound that puts on hot:

| Consumer | Reads | Bound on hot |
|---|---|---|
| The live writer | the current date and the next, with the 60 s date grace (ADR-030 D4) | today and tomorrow |
| The archive import | day D-1, plus the 23:55 `metrics` row filed under D (ADR-034 D3) | yesterday and today on a daily cadence; a late or catch-up import of an older day recalls its partition (D6), so it sets no bound |
| The capture's seed scan | 7 days back, 1 day ahead (`SEED_LOOKBACK_MS`, `ingest.rs`), manifests only (D7 *Seeds*) | **today − 7 … tomorrow: the binding bound**, for the degraded start (below) |
| ADR-052 warm-up, `mie equivalence` | the whole chain from the origin, through the union | none. Cold reads lengthen `ready_after_ms`, which ADR-052 D12's 90 min trigger and D13's queue budget see |
| Absorption (#22, ADR-051) | a run of at least 25 h from its start, through the union | none |
| The slippage calibration (ADR-061) | the depth stores, through the union | none |
| #99 | see below | none |

- **Result: hot holds every date from today − 7 (UTC) onward.** The seed
  window starts at `now − 7 d`, which falls on date today − 7 at any time
  of day, so that whole date must be on hot. Its end, `now + 1 d`, covers
  tomorrow, whose partitions are new and never eligible. In bytes, hot
  holds 7 full dates (today − 7 … today − 1, where today − 7 is only
  partly inside the window but a partition is the unit, D6), today's
  partial date (at most one day) and tomorrow's open-kline records (about
  nothing). D6 runs after each seal pass, so the cadence adds at most 300 s.
  H = 8 days of bytes (D10).
- **Why 7 days.** The 7 d is ADR-032's seed lookback, an "engineering
  default, not measured" there. This ADR's own reason is the coupling:
  hot ≥ lookback, so the seed scan's result never depends on the copy
  ledger. A normal start lists hot and the ledger, never a cold root, and
  gets the same seeds at any hot window (D7 *Seeds*). The window matters
  in the degraded start, a lost ledger on an intact hot (D7), where the
  capture must still start and the seed scan can list hot only.
  - Below it, the degraded start's hot listing misses the partitions
    already removed from hot inside the lookback. A stream whose newest
    event lies in one of them gets no seed, its resume no `Disconnected`
    gap, and the delivered events then depend on the tier state, which
    changes ADR-032 D5's semantics. The alternative of refusing the
    degraded start loses raw data until the rebuild.
  - Above it, the only cost is hot storage (D10) and a longer hot restore
    (D11).
  - If a later ADR changes `SEED_LOOKBACK_MS`, the hot window follows.
- **In code** (#190): `mie_cli::ingest::first_hot_date(now)` is the UTC
  date of `max(now − SEED_LOOKBACK_MS, 0)`, the first day of the seed
  window, computed by the same function that opens that window. Nothing
  configures it. `mie store adopt` passes it, from the system clock, to
  the adoption, whose library call takes the first hot date as a
  parameter: that parameter is the seam of the short-window test
  configuration of Accept-when item 2, and the only way to adopt with
  another window.
- **Rejected, a 2-date hot window** (the writer's and the daily import's
  own bound). With a readable ledger it would give the same seeds, but in
  the degraded start a stream quiet for longer than the window would get
  no seed, with the consequence above.
- **#99 contributes no number** (accepted by the operator at STOP 1), so
  #99 still moving does not reopen this decision:
  - its harness is pinned to main `6ff73d1` and reads one `raw_root`; its
    store is a pre-cutover separate root and stays in one root, unmoved,
    until #99 closes (D9);
  - its pending FOMC session adds a run to that store and changes no rate
    class used here (M1-6 is already its rate);
  - a superseded hold-back (ADR-032 D6) changes no raw byte and no seed
    window.

### D9. Pre-cutover stores

Each pre-cutover store (the soak and session stores, 3.7 GB together, M1
live table) is kept whole, root and journal. If one ever moves, it moves
as a whole store that keeps its relative layout. None is merged into the
production root: they have their own journals and multi-day holes
(ADR-052 D2). They are live-only data, so D11 applies: each store gets
whole-store copies on k independent devices, verified against the store's
own manifests and journal. Copying moves nothing, so #99's store stays
where its harness reads it (D8).

### D10. Sizing formula

Inputs:

- r_s: bytes per day of live stream s. Initial values from M1; on the
  server, the trailing rate measured from manifests (Σ `bytes` of the
  parts sealed in the last N days over N days, the M1 method).
- a: archive bytes per imported day, 0 when imports stop. M1-13:
  43.2 MB/day for the default datasets, plus 81.5 MB/day for opt-in
  `trades`.
- H: the hot window in days of bytes, 8 (D8).
- O: the cold-outage days the hot tier must absorb (operator input at
  provisioning, below).
- k: copies of every live-only file on the copy targets, each on an
  independent device, k ≥ 2 (D11). The hot copy never counts towards k.
- k_A: copies of the archive bytes on the copy targets, 2 (D11).
- A: the archive bytes, 51.0 GB on the host today (M1-12).
- S: the pre-cutover stores, 3.7 GB (D9).
- J: the daily growth of the journal, the import ledger and the copy
  ledger: 5.1–15.1 MB (M1-11) + 0.006 MB (M1-15) + about 1.6 MB (copy
  ledger, estimated in the Numbers table).
- P: the verification period (D11, operator input).
- Z: the import staging, one archive zip at a time (ADR-034: one request
  at a time, each zip deleted once imported), so the largest zip the
  importer fetches; the operator reads it from the import log at
  provisioning.
- B: the bytes of the largest single import run on hot. The run holds the
  archive source lock for its whole duration, so D6 cannot remove any of
  its partitions before it ends (D6 *The source lock*). A daily import
  is about one day of a; a 12-month backfill like #12 is the whole year,
  365 × a = 15.8 GB for the default datasets, 45.5 GB with `trades`
  (+ 365 × 81.5 MB = 29.7 GB; M1-13).

Formulas (days counted from the start of production capture, t in days):

- hot ≥ (Σ_s r_s + a) × (H + O) + J × t + Z + B + the filesystem
  reserve. The journal and the ledgers never leave hot, so they grow with
  t.
- each copy device ≥ Σ_s r_s × t + J × t + S, plus A + a × t on the k_A
  archive copy devices. Every one of the k devices holds a full copy; a
  device shared by live and archive copies holds both terms.
- yearly growth over all devices = k × 365 × (Σ_s r_s + J) + k_A × 365 × a.
- O is also the longest time a newly sealed file can sit with a single
  copy (D11).

Initial values at k = k_A = 2 (Numbers table for the arithmetic):

| | Week-weighted (M1-5) | Busiest hours (M1-6) |
|---|---|---|
| hot, without the reserve | 6.35 GB + 0.79 GB × O + 2.6 GB per year | 9.36 GB + 1.17 GB × O + 6.1 GB per year |
| each copy device, year 1 (live + J + S + archive) | 347 GB | 488 GB |
| each copy device, every later year | +292 GB | +433 GB |

The initial values cover the default archive datasets only. Importing
the opt-in `trades` too adds 81.5 MB per imported day (M1-13), so about
+30 GB per year on each archive copy device and 0.65 GB on hot (81.5 × 8).
They also leave out Z and B: a daily import adds about one day of a, and
a large backfill is placed by the bootstrap below instead of through hot.

The busiest-hours column is an upper bound by construction for the hours
on record, not for a crash day: the archive's busiest `aggTrades` day was
5.5 times its mean (M1-14). Provisioning sizes from the busiest column; the
server's trailing rate replaces both once it exists.

**Bootstrap: existing data never passes through hot.** A (51.0 GB) is 5–8
times the initial hot size, so copying it onto hot first would fill it.
After `mie store init` and `mie store init-root`, `mie store adopt
--config <ingest config> [--archive-config <archive config>] --from
<store root> [--journal <path>] [--import-ledger <dir>]` (#190)
therefore *adopts* an existing store: it copies every sealed file from
the store's current location straight to every copy target by the D5
protocol, verifies it, appends the manifest and copy records to the copy
ledger, and records every partition older than the hot window as removed
from hot; the hot-window partitions, the store's import ledger entries
and its journal are copied to hot. The production archive store is
adopted into the production store. The protocol (#190):

- **The source is only read.** Its `.lock` files are opened read-only and
  tried, never created, so an active writer is refused and the source
  stays byte-identical.
- **Refusals before anything is written:** no `cold_roots`; hot without
  its marker; an archive configuration of another store; `--import-ledger`
  without an archive configuration (it names hot's import ledger); a
  `--from` that is missing, is not a directory, holds `.mie/` (a tiered
  hot is partial by construction), or equals, contains or lies inside hot
  or a cold root; a writer on the source.
- **Locks, never waiting, held for the run:** the pass lock, then hot's
  writer of every source the run writes. Why a writer and not a bare
  lock: opening it runs ADR-030 recovery first, which rolls a crashed seal
  on hot forward. Without it the copy onto hot would delete that seal's
  pending files as copy debris and lose a file. A crashed adoption's own
  hot copies are crashed seals by construction (D5), so the next run's
  recovery handles them too.
- **Checks under the locks, still writing nothing:** hot's copy ledger is
  readable; every copy target presents its marker, which is refused, not
  skipped as in the pass, because an adoption must leave every file with
  k copies; ids and devices are distinct (D11); the source holds no seal
  debris (`replica::list_unsealed`), which the adoption cannot recover
  because it never writes the source; every source part's data exists
  and matches its manifest (size, then SHA-256, streamed), so a damaged
  source is refused with nothing written instead of stopping the run
  after records and copies (review of #200); every file the copy ledger
  records and the source does not list is *complete*. Complete is one
  predicate, the end state a passed adoption guarantees: the file is on
  hot unless its partition is recorded removed (counting the `removed`
  records this run will write), and the ledger records a copy of it on
  every copy target. A file published on a target without its `copy`
  record does not count, since nothing would ever record it. Why: write
  (2) below records every manifest before the first copy, so a run that
  stopped and is re-run against another store (a backup lacking a file,
  another `--from`) would otherwise pass and leave a record that keeps
  a recorded file that no root completes for good (its partition's
  recall then fails on it), or a removed file below k, while naming a
  re-run that already passed (review of #200). The refusal names each path and what it
  lacks, and writes nothing. A file on hot that a replication pass left
  short is refused the same way; `mie store replicate` completes it. The
  same predicate is asserted over every recorded file once the closing
  pass has run, and a gap fails the run, so a PASS always leaves every
  adopted partition hot holds whole there and every file at k copies.
  **A target added or replaced after files left hot** (resolved by `mie
  store repair`, #192): a copy target added after an adoption, or a failed
  one replaced under a new id, holds no copy of the files that already
  left hot, and the replication pass copies from hot only. Those files fail the predicate.
  The class is: not on hot (its partition is recorded removed), short of
  a copy on a configured target, and with at least one `copy` record in
  the ledger on any root id, configured or not. Unlike the other gaps it
  does not block the writes: such a file lies in a partition recorded
  removed (so an append recalls it, from a target that holds it) and the
  run never writes its path, so no path is reused and no other bytes get
  in.
  The run adopts what it reads, then fails at the end, naming each such
  path, the targets holding its recorded copy, the targets lacking one,
  and the remedy:
  - when a configured target holds a recorded copy: `mie store repair`,
    which copies it from that target (D11 *As implemented*, R2), after
    which the next adoption passes. The message names both causes, a
    target added or replaced, or an adoption that stopped before it
    reached every target;
  - when its copies are recorded only on roots no longer configured
    (every target replaced or lost, review of #200 finding 7): the
    repair cannot help, since no configured target has a good copy, and
    the message names the only remedy, re-adopting each original store
    in turn, which copies its own files to every configured target.

  A recorded file with no `copy` record at all is the ghost of a stopped
  adoption and still refuses up front (finding 3). Why not refuse up front: with two adopted
  stores (the #22 store and the archive, the production shape), each
  re-adoption would be refused for the other store's files, a deadlock
  that only the repair breaks. Re-adopting every earlier store in turn
  ends with PASS, and so does one repair. Why
  not pass: files would stay below k on the new target with no step that
  ever fills them. A file on hot that only lacks copies still refuses up
  front, with `mie store replicate` as its remedy. The source's import
  ledger holds no `PENDING`; every source archive part is listed by a source entry with
  its manifest's SHA-256 (D11 *Archive parts wait for their import
  entry*), every part an entry lists is in the source or already in the
  store, and an archive part without `--import-ledger` is refused.
- **The path rule** (D5). Each source path's manifest is compared with
  the copy ledger's `manifest` record, hot's manifest and each copy
  target's manifest, wherever they exist; the target check catches a
  published copy that has no record yet. Equal everywhere is an identical
  path, resumed or skipped. Anything else, and a path the ledger records
  as lost, held, quarantined or replaced, is `Integrity` naming the path
  and where it differs, and the run ends with nothing written. Each source
  entry must be absent from hot's import ledger or byte-identical to it,
  and hot's journal absent or a byte prefix of the source journal (the
  empty file included). Why a prefix and not only "absent or empty": a
  re-run then resumes an interrupted journal copy.
- **The write order:** (1) `removed` for every source partition older
  than the hot window that has no directory on hot and is not recorded
  removed yet; (2) the `manifest` record of every source file the ledger
  does not know yet, before any copy; (3) per file in path order, every
  copy target without its `copy` record (the marker re-read before and
  after the copy, then the `copy` record), then hot when the partition is
  on hot's side (a hot-window date, or a directory already on hot, and
  not recorded removed); (4) the source's entries to hot's import ledger,
  after all their parts; (5) the journal, extended as a byte prefix; (6)
  one replication pass under the held pass lock, so the ledger, the
  journal and the entries have k copies before the run returns. Why
  `removed` first: from the first byte on, an append into those dates
  recalls the partition instead of numbering from hot's empty listing
  (D6), so no path can be reused while the adoption runs or after it
  crashed. A `removed` record of an adopted
  partition asserts only that hot holds no copy; adoption deletes
  nothing, and a run that leaves a file below k fails. Why the
  `manifest` records before any copy: from the first publish on, the
  writer's union numbering then numbers a new part of every adopted
  partition above every part the adoption will copy (D6). This is the one place where a `manifest` record
  precedes its first verified copy. Until the re-run completes, a
  selection that covers such a file lists it and its read fails with
  `Integrity` (missing on every root), never a shorter selection. Why
  targets before hot: a crash then leaves extra copies on the targets,
  never a hot file that the ledger does not list.
- **Resuming and failure.** A re-run with the same arguments resumes,
  because identical paths are only completed. A re-run must read the
  store the stopped run read, or one holding at least its recorded
  files: a store without one of them is refused (the check above), and
  its message names the re-run with the store it read. The writes stop at the
  first failure, which is named: a store cannot be half adopted on
  purpose, and going on would only copy more bytes before the same
  failure. The source data is verified twice: in the check phase, so a
  damaged source is refused before anything is written, and again while
  it is streamed (D5), which catches a source that changed in between.
  The cost is reading the source twice (51.0 GB of archive plus the
  live store, D10), once per adoption, which is a one-time bootstrap.
- **Rejected,** writing the adoption into `mie store init` (the wording
  before #190): adopting several stores (the #22 store, then the archive)
  and later backfills needs a verb that runs more than once on an
  initialized store. The hold-out store and the pre-cutover
stores stay separate stores and get whole-store copies (D9), so they never
enter the production hot either. A future backfill of the same size takes
the same path: import into a scratch store elsewhere, then adopt (for days
the store does not hold yet; a path the store already holds with other
bytes is `Integrity`, D5). Why: B for a whole-year import is larger than
hot, while adoption bounds hot by the formula above.

What breaks:

- hot too small: `ENOSPC`, a writer error, the capture stops (ADR-032 D10)
  and raw data is lost;
- a copy device too small or full: copies stop, files stay below k, hot
  removal waits (D6), hot fills, and the same failure follows.

### D11. Copies of live-only data and their verification

- **Scope:** every sealed live file (all seven streams, since no live
  stream has a replacement, D1 and D2), the D9 stores, the capture
  journal, the import ledger and the copy ledger. The archive follows k_A.
- **k ≥ 2 on independent devices** (operator, STOP 1). k counts copies
  on the copy targets; the hot copy never counts, so a file removed from
  hot still has k copies and the D10 per-device sizing holds. Why 2: one
  device failure destroys a single copy. Nothing can re-create a live file
  (D1, D2), and every run and dataset version that contains it would
  become unreplayable. With two copies, a file that has been replicated
  survives any single device failure, hot's included. What a hot failure
  still loses is the single-copy exposure below (the parts sealed since
  the last replication pass, the open parts, and the journal and ledger
  tails since then), and the capture is down until the hot restore below
  ends. Each further
  copy adds a full copy of the live growth (D10: +274 to +411 GB per
  year). "Independent" means a separate disk and, for network storage, a
  separate server, because two copies on one disk or one NAS fail
  together (operator).
- **k_A = 2.** k is the number of copy targets, and every copy target
  receives every file, archive parts included (#189), so k_A = k; at the
  operator's k = 2 that is the k_A = 2 decided here. The operator's k
  covers live-only data. The archive can be
  downloaded again, but not as the same bytes once the writer changed
  (D1), so a lost archive file changes the chain's prefix dataset and the
  versions accepted ADRs cite, the same harm as a lost live file for the
  chain. It is also cheap: A + a × 365 is about 67 GB in year 1 against
  274–411 GB of live growth. Rejected, k_A = 1 with re-download as the
  repair: the re-download would need a new ADR under the lost-file rule
  below, and every warm run since the loss would read `DIVERGED (warm
  prefix)`.
- **What mie can check.** Mie cannot see the devices behind a mount, for
  the same reason that it does not monitor liveness, so independence is a
  provisioning duty. The follow-up refuses a configuration in which two
  copy targets, or a copy target and hot, report the same device id
  (`st_dev`). That catches two copies on one local disk, but not two
  exports of one NAS.
- **Replication.** After each seal pass (300 s, ADR-032 D10), the
  replicator copies every sealed file with fewer than k copies to the copy
  targets by the D5 protocol and records each verified copy in the copy
  ledger. Within a pass it copies the data files first, then the
  import-ledger entries, then the tails of the copy ledger and the journal.
  Why the order: a `sealed` line on a cold journal copy then always names
  a file that is already on cold, so a hot loss drops whole files together
  with their journal lines, and the lost tail never leaves a cold journal
  naming a missing file. To keep that true when a file fails on a target,
  **a target's journal copy advances only up to the first journal line
  that names a live file with no copy record for that target, never
  through that line**, where the live files are those of the pass's hot
  listing and those of the ledger's `manifest` and `copy` records; every
  line before it is still appended, and the pass names the stop as a
  failure (operator, #199 review). Why bounded at that line and not the
  whole tail: one unreadable live part on hot (a bad sector, a part
  failing its manifest) would otherwise keep every later journal line at
  a single copy on every target, with no limit. Why the ledger's paths
  too (#199 review carry-over, #191): once D6 removes partitions, a
  target added after a removal never gets the removed files from hot,
  because the pass copies from hot only, and a hold keyed on hot's
  listing alone would let their `sealed` lines through to that target.
  The cost: that target's journal copy stops at the first removed file's
  line until `mie store repair` fills the target, and every pass names
  the stop and fails. `lost` paths are not counted, since no copy of them
  can ever reach a target (the repair records them only with
  `--record-lost`, the rebuild #193 at its step 4). Archive parts are never
  checked: only the capture writes `sealed` lines. The cost is that the
  lines from that line on stay at one copy until the missing file
  reaches the target (the single-copy exposure below). A line names a
  file by token, not by a substring scan per path: for each occurrence of
  `source=` in the line, the longest run of layout-path bytes
  (`[A-Za-z0-9._=/-]`) from there, and every prefix of it that ends with
  `.parquet`, is looked up among the files. It finds the lines the
  substring rule finds for every path the layout can write, since a data
  path consists of those bytes and starts with `source=`. Why: with the
  ledger's paths a new target's set holds every recorded live file (about
  670 000 after a year, 1 835 a day, M1-9), and a scan per path would
  take minutes per pass; the token match is O(line length · log n).
- **Scheduling in `mie ingest`** (#191). On a tiered store a
  `mie-replicate` thread runs one pass when the capture starts (it
  resumes what the previous run left) and one after each seal pass, woken
  by a trigger channel with one pending slot, so a pass that outlasts a
  seal interval leaves one wake-up behind, never a queue. The pass gets
  the device probe of the build (*What mie can check*) and `[history]
  archive_config`; without it `mie ingest` replicates no archive parts,
  as the standalone pass does not. The seal path never does cold I/O: a
  hung or unmounted cold root blocks the thread only, never the capture.
  The remover (D6) runs in the seal path, through the writer that holds
  the live source's lock, before it wakes the thread, with the plan the
  previous pass left. Without a readable ledger the pass refuses and
  gives no plan, so both stop (D7). The journal gets one `replication`
  line per pass (counts, the first problem, the planned candidates),
  `removal` and `recall` lines when something happened, and a
  `copy_ledger_truncated` line per truncation; `read_runs` and the
  capture report ignore all four. On shutdown the capture is joined, the
  writer closed and `run_end` written and synced as before; only then is
  the final pass woken and waited for, at most one seal interval, its
  `replication` line landing after `run_end`. A pass still running then
  is abandoned with a line on stderr, and the process exit ends it.
  Why `run_end` first: a hung mount must not delay the capture's own
  record, or a service manager's stop timeout could kill the process
  before it and the run would replay as crashed (ADR-039 D2). Why
  abandoning is safe: every pass step is crash-safe (a crashed copy is a
  crashed seal, a torn tail a shorter prefix, #189), and the next start's
  first pass resumes it. A failure
  to read hot (one part, its manifest, an entry, the journal) is about
  that source and is named; the other files and targets go on, so one bad
  sector on hot cannot stop the replication of everything else. It writes
  only into a root whose marker it finds. Why: an unmounted mount point is
  a writable empty directory on the hot disk, and a copy written there
  would share hot's device and disappear under the mount when the mount
  returns. **The marker is re-read before every write to a target** (a
  sealed file, an entry, the ledger tail, the journal tail) **and before
  every record about it** (`copy`, `ledger-copy`); a target whose marker
  is gone or carries another id stops for the rest of the pass (operator,
  #199 review). The cost is a few small reads per copied file. Residual:
  a mount lost between a re-read and the write that follows it still
  receives that one write on the parent's filesystem, and a mount lost and
  restored between two re-reads is not seen at all. The device check
  (`st_dev`, *What mie can check*) runs once at the start of the pass, not
  per write: probing the device before each write would leave the same
  window open. Both residual cases are found by the verification (#192),
  which compares every copy with its manifest and every recorded copy with
  the root that should hold it. Sealed files and import-ledger
  entries, which are immutable files (ADR-034 D4), are copied once by the
  D5 protocol.
- **Archive parts wait for their import entry.** An archive part is
  replicated only once the `.import` entry that lists it exists on hot,
  and per archive file the pass copies its parts, then its entry: an entry
  goes to a copy target only once every part it lists has a copy record
  for that target, and an archive part only once an entry on hot lists it
  with its manifest's SHA-256 (another SHA-256 is `Integrity`, and the
  part is not copied). Whether an entry is already on a target is read
  from one listing of the target's entry directory, never by re-reading
  the entries (#189). Why: re-reading every entry would read the whole
  import ledger (2.25 MB at M1-15, and growing) from every target on every
  pass, 288 times a day; the byte comparison of existing entries is the
  verification's job. The
  importer's `PENDING` marker (ADR-034 D4) is not replicated. Why: an
  import that seals several parts of one file (a large `trades` or
  `aggTrades` day, ADR-034 D5's row and byte limits) writes its entry only
  at the end. Parts that reach cold before their entry, followed by a hot
  loss, are parts that no entry lists; the hot restore must hold them
  and halt the importer (step 5). Holding parts until their entry
  exists limits that to the one file whose parts and entry a loss
  separates within a single pass. A replicated `PENDING` does not help:
  the marker is rewritten for every file and removed at the end, so a cold
  copy can be stale or already name the next file. The cost is that a
  file's parts stay at one copy for the duration of that file's import,
  inside the single-copy exposure below.
- **The journal and the copy ledger** are append-only. Each pass appends
  to each copy the bytes the copy lacks, `[len(copy), len(hot))`, and
  fsyncs it, after checking that the copy's last complete record equals
  hot's record at that offset. A crash mid-append leaves a shorter byte
  prefix of hot, which the next pass extends. Why not a whole-file copy
  per pass: after one year the journal alone is 1.9–5.5 GB (M1-11 × 365),
  and copying it every 300 s would write that 288 times a day to every
  target.
  - **The copy-ledger copy ends at hot's verified prefix B** (#189): the
    end of hot's last complete record whose chain verifies, by the
    single-trailing-record rule, so a torn trailing record is never
    copied. B, its SHA-256 and the copied bytes all come from one read of
    hot's ledger. A `ledger-copy <root> <B> <sha256>` record then asserts
    that the copy is B bytes long, that hot's `[0, B)` had that SHA-256,
    and that the appended bytes and the copy's last record were read back
    and match hot's. The comparison of the whole copy with hot's prefix
    belongs to the verification. Why not read the whole copy back on every
    pass: the copy ledger grows about 1.6 MB per day (Numbers table), 0.58
    GB after a year, and reading it back on 288 passes a day for two
    targets would read about 334 GB a day from cold in year 1, growing
    linearly.
  - **The up-to-date rule.** A copy gets no tail and no record when every
    hot record from its length up to B is a `ledger-copy` record and the
    latest `ledger-copy` record for that target has prefix length = the
    copy's length; its last record is still compared with hot's. Why: each
    pass ends with `ledger-copy` records that the copies cannot hold yet.
    Copying them would add new ones on every pass, and a pass over
    unchanged state would never be idle. A crash between a tail append and
    its record leaves no matching record, so the next pass writes it.
- **The copy ledger is covered by the same rule as data** (operator, STOP
  1 approval). Why: it is the only listing of the partitions removed from
  hot (D7), so losing every copy of it would hide those partitions from
  every selection, a loss of reachability as harmful as losing the files
  themselves, and nothing downloads it again. It is replicated with k
  copies on the copy targets and verified every pass of the verification
  below. If it is lost anyway, the D7 rebuild restores it from the
  surviving copies, the manifests on every root, the journals and the
  import ledger.
- **Single-copy exposure.** A newly sealed file has one copy for the open
  part's lifetime, plus the time until the next replication pass, plus any
  cold outage (O, D10). The open part holds at most 300 s of records per
  stream, the ADR-032 D10 crash bound. Why the seal cadence is the
  earliest point: a part can be copied only once it is sealed ("sealed iff
  the manifest exists", ADR-030). The same bound applies to the journal's
  and the copy ledger's tail. An archive part also waits for the end of
  its file's import (*Archive parts wait for their import entry*). Hot
  removal never runs below k (D6).
- **The hot restore after a hot-disk loss.** An explicit `mie` step that
  must finish before `mie ingest` or the importer may start; both refuse a
  hot without its marker (D7). It holds the copy-ledger lock (D7)
  throughout:
  1. Every configured copy target must present its marker, as in the
     rebuild. A new hot marker is written.
  2. **The journal** is restored from the longest copy whose other copies
     are byte prefixes of it. Two copies that differ beyond their common
     prefix stop the restore (damage, explicit repair). **The import
     ledger** is restored as the union of the entries and supersede
     records on the copy targets;
     an entry name with different bytes on two targets is an archive
     finding (step 5), not a stop. **The copy ledger** is restored from
     the longest copy whose hash chain verifies, or the restore starts it
     empty (D7 rebuild, step 1).
  3. **Every hot-window partition (today − 7 onward), and every partition
     the restored ledger does not record as removed from hot,** is copied
     back to hot by the D5 protocol and verified. Why: `open_part` and the
     seed scan read hot, and the partitions not yet removed were still
     waiting for their k copies, so the replicator must find them on hot.
  4. The D7 rebuild steps 2–6 then run against the restored ledger: the
     root listing and the cross-check with the restored journals and import
     ledger add the records lost with the ledger tail, and step 5 marks only
     the partitions that are still off hot after step 3.
  5. **Archive findings never stop the restore; copy damage is repaired,
     not quarantined.**
     - A copy that does not match its manifest's size and SHA-256 (live or
       archive) is a verification finding: the restore copies from a
       matching copy and records the finding for the D11 repair. No
       matching copy left is the lost-file rule. Neither stops the restore.
       Why: the usual cause of two differing copies is one damaged copy,
       which `manifest::verify` identifies, and the good copy is the
       sealed file.
     - Two copies of one import-ledger entry with different bytes: if
       exactly one of them lists files whose SHA-256 match the manifests
       on the roots, it is the entry and the other copy is repaired from
       it. Otherwise the file's parts are **held** (below).
     - One archive path with different manifest texts on two roots (two
       sealed files under one path, not one damaged copy) is **held** too,
       with both copies left in place.
     - Holding records the finding, leaves every byte where it is and
       refuses the importer; it moves and deletes nothing. Only the
       *replace* resolution below ever moves a part, to quarantine.
     - Archive parts that no restored entry lists are **held**. Under the
       replication rule above they come from a *completed* import: its
       parts reached cold in the last pass, its entry, written after them,
       was still on hot only. So it is at most one file, and runs taken
       between that import's end and the loss may cite those parts in
       their dataset versions, a warm run's `prefix_dataset` included.
     - What still stops the restore is live-only and undecidable: journal
       copies that differ beyond their common prefix, or one live path
       with different manifest texts on two roots and no copy-ledger record
       to say which is the sealed one. Nothing can replace live data, and
       a wrong guess would change it.
     - The restore makes no network request.
     - Why archive findings must not stop it: `mie ingest` waits for the
       restore, so every minute it is stopped is irreplaceable live data
       lost, while the archive can be downloaded again (D1). Rejected,
       writing a `PENDING` so that ADR-034 D4's crash rule resumes the
       file: deriving the file from the parts' partitions is wrong for
       `metrics` (one file seals into two dates, its 23:55 row filed under
       the next day, ADR-034 D3) and for monthly `fundingRate`; it needs
       the published `.CHECKSUM`, a network fetch whose failure would stop
       the restore; and if Binance republished the file, the resumed
       import fails the orphan-prefix check (ADR-034 D4) with no defined
       way out.
  6. Only then may `mie ingest` start; the importer starts only once no
     archive finding is open. The restored
     journal still holds every replicated run, so `read_runs` sees them.
     The run that was open at the loss has no `run_end` and replays as a
     crashed run over its contiguous prefix (ADR-039 D2). Records of a run
     whose `run_start` line was in the lost tail are counted and reported
     by replay, as ADR-039 D2 already does for a lost `run_start`, never
     dropped silently. The next run's seeds come from the restored hot and
     ledger, so its resume carries the `Disconnected` gap over the whole
     outage, the restore included. Part numbers continue from the union
     (D6), so no path is reused even if step 3 missed a partition.

  **Held archive parts and their resolution.** A held part stays where
  it is, with its bytes, path and manifest; the copy ledger records the
  hold. Every selection that covers it fails with `Integrity` (D7), the
  union part numbering counts it (D6), and the importer refuses to run
  until every hold is resolved. The live capture is not affected: its
  seed scan selects live streams only, and a warm-up whose archive segment
  covers a held part fails into a cold run (ADR-052 D8). The resolution is
  an explicit `mie` step per archive file, run by the operator, which
  names the file; the step proposes it from the parts' stream and ordering
  times by the catalog's period rules (ADR-034 D3: the `metrics` 23:55 row
  belongs to the previous day's file, `fundingRate` files are monthly), and
  a wrong choice only fails the check below. Two modes:
  - **Re-admit**, tried first. The step downloads the file and checks
    that the held parts equal its rows by ADR-034 D4's prefix rule (here
    all rows). If they do, it writes the missing entry, or a superseding
    entry when a conflicting one exists, listing the held parts, and
    clears the hold. Bytes, paths and dataset versions stay as they were,
    so the runs that cited the parts before the loss still reproduce. Why
    first: it is the only outcome that keeps those runs EQUIVALENT.
  - **Replace**, when the rows differ (the upstream file was republished) or
    the operator decides so. The held parts move, on every root, by a
    rename within the root to `.quarantine/<id>/<relative path>`;
    nothing in mie ever deletes them, and the rebuild and every listing
    of the layout skip `.quarantine/` (D7, rebuild step 3). The step
    first writes a supersede record: an immutable import-ledger file
    naming the archive file, the superseded entry's SHA-256 when an
    entry exists, the quarantine id and the paths moved, replicated like
    the entries (it never edits the old entry). The next import then
    reads the file as not imported (a superseded entry counts as absent,
    which is what keeps it from reporting "the ledger lists …; not
    re-imported", `import.rs`), imports it fresh and writes new parts
    with new numbers. Until that new entry exists, every selection
    covering the quarantined paths fails with `Integrity`, so no
    selection is shorter in between. The supersede record and the
    quarantine directories are what a rebuild re-derives this state from
    (D7, step 5a). The cost, stated in the step's output: every dataset
    version that covered the held parts changes, and warm runs that
    cited them read `DIVERGED (warm prefix)` for good.
  - Why the importer halts instead of resolving on its own: replace
    changes recorded dataset versions, and a hold can be the trace of a
    changed upstream file (ADR-034 D4's `changed` conflict), which ADR-034
    never resolves automatically. Rejected, quarantining held parts at the
    restore and skipping them in `select`: the parts belong to a completed
    import, so every selection covering them would be shorter with no
    error until a re-import, an ADR-052 hole meanwhile, and the runs that
    cited them would diverge even when the upstream file never changed.
    Rejected, quarantining every copy of a path whose copies differ: it
    throws away the good copy of a completed import. Rejected, editing the
    old import entry: entries are immutable (ADR-034 D4), and the
    superseded one is the record of what was imported.
  - **As implemented (#193).** The importer (`mie archive-import`, its dry
    run included) refuses while any hold is open, right after its
    copy-ledger refusal and folded from that same ledger read, so it
    costs no I/O: the message names the count, the first held path in
    path order, its reason and the resolution `mie store resolve` (#194).
    The checks of step 5 are one pure function shared by the rebuild
    (D7 step 5a) and the hot restore, which passes no `PENDING`; a part
    that qualifies for several reasons gets one, `manifest-conflict` >
    `entry-conflict` > `unlisted`.

  Why a restore and not "the capture continues": on an empty hot the
  writer numbers new parts from `part-00000` under paths that cold
  already holds (D5 breaks, and `read` would serve the new bytes to runs
  that cited the old ones), `Journal::open` starts a new journal file
  without the earlier runs (they drop out of `mie replay --source live`
  and `mie equivalence`, and the cold journal copies stop being prefixes
  of hot), and the seed scan finds no seeds, so the resume has no
  `Disconnected` gap. The cost: the capture is down while the restore
  copies about (Σ_s r_s + a) × H, 6.35–9.36 GB (D10), plus the journal
  and the ledgers. That downtime is a hole that the resume's seed gap
  marks.
- **Periodic verification** is a follow-up `mie` command; scheduling it is
  host-ops. It covers every sealed file in the hot listing and the copy
  ledger, on every root that holds or should hold it, and checks for each
  copy:
  - the copy exists;
  - its size and SHA-256 equal the manifest (`manifest::verify`);
  - the root's manifest text is byte-identical to the one in the copy
    ledger;
  - the file has at least k good copies on distinct copy targets (files
    sealed within the last replication pass excepted).

  For the journal and the copy ledger it checks that each copy is a byte
  prefix of the hot file (the SHA-256 of the copy equals the SHA-256 of
  hot's first `len(copy)` bytes), and that the copy ledger's hash chain
  verifies on hot and on every copy. The files grow between passes, so any
  other difference is damage, and the chain shows which copy holds it.
  Import-ledger entries are compared byte for byte. Each pass appends its
  result to the copy ledger, so the time of each copy's last good
  verification is known.
- **P, the verification period.** Every copy's last good verification
  must be younger than P. P is an operator input at provisioning, within
  these bounds:
  - Lower bound: the bytes on the largest copy device (D10) divided by the
    measured read throughput of the slowest root. Below it, a pass cannot
    finish. It grows with t.
  - Above any value, a silently damaged copy stays unnoticed longer. With
    k = 2 that is a window with one good copy, in which a second failure
    loses the file.
  - The ADR has no device failure data to derive one value from, so it
    states the bounds, not a number.
- **On a finding** (a missing copy, a size or SHA-256 mismatch, a
  differing manifest, fewer than k good copies, a broken prefix or hash
  chain, a verification older than P): the command exits non-zero and
  names the file, the root and the finding, so the host's scheduler
  alerts. A bad copy is never read, because `read` raises `Integrity` on
  it (D5).
  - An explicit repair step replaces a missing or bad copy from a good one
    by the D5 protocol. The restored bytes are the sealed bytes, so D5
    holds. A marked copy target without a `copy` record for a recorded
    file counts as a missing copy, even when no verification found it: a
    target added or replaced after the file left hot (D10 *Bootstrap*,
    the class it names) is filled this way, from another target.
  - Repair is explicit, not automatic: several bad copies at once point at
    a shared cause (a failing controller, a wrong mount), and automatic
    rewrites would spread into it.
  - **The lost-file rule.** If no good copy is left, the file is lost. It
    stays in the copy ledger and in every dataset version, every read of
    it is `Integrity`, and nothing re-creates it or downloads it again
    under the same path. Recovering from that needs a new ADR, because any
    substitute changes dataset versions that accepted ADRs cite.
- **As implemented (#192): `mie store verify` and `mie store repair`.**
  The ledger grammar does not change: no new record kind and no header
  bump. Rejected, a `cleared` record kind: it is a format change (header
  v2) for what a fresh `copy` or `ledger-copy` record already says under
  the remover's rule (D6). Rejected, a repair that re-verifies the whole
  store itself: that is a second full read of every target per repair, P's
  lower bound paid twice; the repair works from what the verification
  recorded and re-checks only those copies.
  - **The verification** (`mie store verify --period-hours <P>`) reads the
    roots and appends to hot's ledger only; it takes `.mie/verify.lock`
    (D7), never the pass lock, so it runs beside the replicator of `mie
    ingest` (AW11).
    - **V1 Refusals,** before any cold read: P missing (a usage error) or
      not a whole number of at least 1 hour, or overflowing as
      milliseconds (0 would make every copy stale at once); an empty
      `cold_roots`; a hot without its marker; an archive configuration of
      another store; the verification lock held; hot's ledger missing or
      failing its chain before its last record, naming the record and its
      byte (the hot half of "the chain verifies on hot and on every
      copy"); a marker with hot's id or one id on two roots; two roots on
      one device.
    - **V2 Read order:** hot's sealed listing, then hot's ledger once (the
      snapshot S, verified prefix Vₛ), then hot's import entries. For every
      tail check the copy is read before hot. Why: a copy is always a
      prefix of a hot file that only grows (ledger copies stop at hot's
      verified prefix, and the journal is append-only), so a pass that
      extends both between the two reads can never produce a false
      `prefix`.
    - **V3 Hot, report only:** each sealed file on hot against the ledger's
      `manifest` record and its data against it, and every ordinary
      recorded path hot lacks although its partition is not recorded
      removed. Nothing is recorded: records name copy targets only, and
      the hot copy never counts.
    - **V4 Per copy target** that presents its marker, in configuration
      order, one walk with `at_ms` = now at its start, the marker re-read
      before every record. Data: every path with an ordinary or held
      verdict and a `copy` record for the target in S, one finding per
      copy in the precedence `missing`, `manifest`, `size`, `sha256`, the
      data always checked against the ledger's text; a self-consistent
      other file (data matching its own differing manifest) is `manifest`
      with D5's wording, two sealed files under one path. k: an ordinary
      path without a `copy` record for the target is `copies`, except an
      archive part no hot entry lists yet and a file on hot whose data
      mtime lies within G before now. Entries: each hot entry whose parts
      all have `copy` records for the target older than G, byte for byte
      (`missing entry:<e>`, `entry`). Copy ledger: absent while S holds a
      `ledger-copy` record for the target is `missing`; then `chain`,
      naming the record and its byte; then `prefix` for complete records
      that run past hot's verified prefix or differ from hot's by one
      byte; then `prefix` for an unterminated tail after the last complete
      record that differs from hot's bytes or runs past hot's verified
      prefix. Why the tail separately: a crash during the tail append on a
      filesystem that exposes the new size before the data (XFS, ext4
      with `data=writeback`) leaves NULs there; the tail is no record, so
      R6 can cut it off, while differing complete records are two valid
      chains (review of #203, finding 3). Journal:
      absent while the target has a `copy` record is `missing`; otherwise
      the bytes are compared in bounded chunks, and a difference is
      `prefix` naming the first differing byte. Why bytes instead of the
      SHA-256 of `len(copy)` bytes: the comparison is equivalent and
      locates the damage, which the repair needs. Each recorded finding is
      appended as soon as it is found, then `verify <at_ms> <root> <Vₛ>
      <count>`. Why at once: the remover counts a damaged copy until its
      finding exists, and a walk takes up to P's lower bound. An I/O error
      other than an absent file stops the target: its findings stay, it
      gets no `verify` record, and a `FAIL` line names it.
    - **V5 Unverified targets** (no marker, or stopped) are named, and
      their copies whose last good verification is older than now − P are
      `stale` (the count, the oldest time, the first path). The last good
      verification of a copy is the later of its latest `copy` record and
      the latest pass over its root that covered that record and recorded
      no finding about the file. A target without its marker has no id,
      so the ids S records copies on that no marked target carries stand
      in for it.
    - **V6 Read-only roots** that present a marker: the copies they hold
      are checked as data, reported only.
    - **V7 Not recorded: `copies`, `stale`, and findings on hot or on
      read-only roots.** Why: they are re-derived from S on every run (an
      absent `copy` record, an old `verify` record), so a record adds no
      information, and their count scales with the store. A copy target
      added after one year lacks about 670 000 `copy` records (1 835 data
      files a day, M1-9); at about 185 bytes per `finding` record that is
      about 124 MB per run, about 77 days of ledger growth at 1.6 MB a day
      (Numbers table), which every later fold would read for good.
      Recorded findings are exactly those about a copy the ledger
      records, which is what the remover voids. The codes stay in grammar
      v1.
    - **V8 G = 2 × `capture.seal_interval_secs`,** 600 s at the default
      300 s (ADR-032 D10). Why: a pass runs after every seal pass, and a
      pass running while a part seals leaves one wake-up behind
      (*Scheduling in `mie ingest`*), so a healthy replicator copies a
      part within two intervals. A shorter G reports parts the next pass
      copies anyway; a longer one leaves a stalled replicator unreported
      longer, which is the single-copy exposure above. A future mtime is
      never exempt, so a clock step can add a report but never hide one.
    - **V9 Output:** one summary line per root, then the `FAIL` lines: hot;
      each root in configuration order (data in path order, then entries,
      the copy ledger, the journal); then `stale`; then one line per file
      with fewer than k good copies. That last check runs only when every
      copy target was verified in this run; otherwise the `not verified`
      and `stale` lines stand in for it. Why: an unmounted target would
      otherwise give one line per file of the whole store. `PASS` and exit
      0 only without any `FAIL` line.
    - **V10 Concurrency:** the verification records only about copies S
      records, and the verification lock excludes the repair, the only
      writer of a second `copy` record for such a copy, so a finding can
      never void a copy made after S. The replicator only adds copies S
      does not record.
  - **The repair** (`mie store repair [--record-lost]`) works the list the
    ledger records plus the copies a configured target lacks without a
    record (the comment of 2026-11-07 on #192), and re-checks each item
    first.
    - **R0** The refusals of V1 without P; then the verification lock and
      the pass lock, both without waiting, for the whole run. A copy
      target without its marker is named and never written. The cost of
      the pass lock: replication pauses while the repair runs. For the
      refill of a target added after a year that means copying about
      347–488 GB (D10, the year-1 copy device), while new parts stay at
      one copy and removal waits, as during a cold outage, which D10's O
      term sizes. Rejected, per-file locking: it races the replicator on
      the same target. The adoption makes the same trade (D10).
    - **R1** A data copy with an open finding (a finding after its latest
      `copy` record, the remover's rule): good now, only a fresh `copy`
      record; a self-consistent other file, refused and never moved (D5);
      otherwise a good source (R3), the bad copy moved aside (R5), the D5
      copy, its published text checked against the ledger's, the marker
      re-read, a fresh `copy` record. Why the record: it ends the
      finding's void, so a repaired partition becomes removable again (the
      comment of 2026-11-08 on #192).
    - **R2** A file that left hot with a recorded copy on a configured
      target and none on another is copied there and recorded: the class
      of D10's bootstrap, computed by the adoption's own predicate, so the
      two never disagree.
    - **R3** Sources, in order: hot, then every cold root in configuration
      order, read-only roots included, never the destination. A source
      counts only when its manifest text equals the ledger's byte for byte
      and its data verifies; a source whose own manifest differs is never
      written. A root whose copy cannot be read (an I/O error, EIO from a
      failing disk, say) is no source, and the report names it with its
      error, also when a later root serves; R9 never calls a path lost past
      it (review of #203, finding 2).
    - **R4** A bad hot copy, or a recorded path hot lacks although its
      partition is not recorded removed, is replaced from the first good
      cold copy (`replica::copy_back`) after R5, under the source's writer
      lock taken without waiting; a held lock leaves it unrepaired and
      named. Under that lock the ledger is read again, and a path whose
      partition is now recorded removed is skipped and named. Why: the hot
      removal records `removed` and deletes under the same lock but
      without the pass lock, so a partition the run's first ledger read
      still saw on hot can leave it before R4 lists hot, and R4 would
      re-create files on hot in a removed partition (review of #203,
      finding 4). Hot copies get no record.
    - **R5** The move-aside, into `<root>/.mie/damaged/<run at_ms>/<relative
      path>`: the data first, then its manifest and the seal temp names,
      each an atomic rename on the root's filesystem, the directories
      fsynced. Why not delete: several bad copies point at a shared cause,
      and the bytes are its evidence; the rename costs no I/O. Why not
      overwrite: D5 never overwrites a copy that fails, and a crash in an
      overwrite leaves neither version. Why the data first: `read` skips a
      root without the data, so a concurrent reader falls through to a
      good copy, and a crash after the first rename leaves a manifest
      without data, which the verification reports as `missing`, ADR-030
      recovery refuses on hot, and the next repair completes.
    - **R6** A ledger copy with an open copy-ledger finding whose chain
      fails at or before the first byte f where it differs from hot is
      moved aside and rebuilt as hot's verified bytes up to f (a temp
      name, fsync, read-back, rename, directory fsync); the closing pass
      extends it. A copy whose complete records all equal hot's within
      hot's verified prefix, with only an unterminated tail that differs
      or runs past it (V4), is moved aside and rebuilt the same way up to
      the end of its last complete record. A copy whose complete records
      verify but differ from hot's, or run past hot's verified prefix, is
      refused: both chains are valid,
      so one holds records the other lacks (a damaged last record on hot
      that an append truncated as torn, D7, say), and overwriting it could
      destroy the last copy of a record. `mie store rebuild
      --replace-hot-ledger` decides (#193, D7 *As implemented* B9): the
      rebuilt ledger supersedes both chains, the old hot ledger is linked
      into `.mie/damaged/` and the copy is moved aside.
    - **R7** A journal copy with an open journal finding is repaired only
      when another target's copy is longer than the first differing byte d
      and equals hot over `[0, d]`, so two of three agree; it is rebuilt
      up to the start of the line holding d. Otherwise it is refused: the
      journal has no chain, so one copy against hot cannot be attributed.
      When every copy agrees against hot, hot's journal is named as the
      suspect; the hot restore restores it, never the repair.
    - **R8** An entry copy with an open entry finding is replaced from hot
      only when hot's entry lists every file with the SHA-256 of the
      ledger's `manifest` record (else hot's manifest); otherwise it is an
      entry conflict for the hold resolution (#194). An absent copy is
      left to the closing pass.
    - **R9 The lost-file rule as implemented** (operator, #192 STOP 1
      decision 1). A path with no good source is reported lost only when
      every root the ledger records a copy of it on presents its marker
      again at that moment (not only at the start of the run: an
      unmounted target is an empty mount point, where every copy checks as
      missing) and every root tried as a source was read without error
      (R3). Otherwise it is "not repairable in this run", naming the
      roots, and nothing is recorded (review of #203, finding 2). A lost
      path: nothing is re-created and nothing is written to any root. Only
      `--record-lost`, passed after a run without it reported the path,
      appends `lost <at_ms> <path> verify <sha256>`. Why: a `lost` record
      is irreversible (every selection covering the path fails for good,
      and undoing it needs a new ADR), and a shared-cause read fault must
      not become a permanent loss on its own; without any record, a new
      target's journal copy would stay at the lost file's `sealed` line
      until the rebuild. A recorded copy on a root that is absent or no
      longer configured makes the path "not repairable in this run",
      never lost.
    - **R10** The closing pass runs under the held pass lock, with the
      targets R6 rebuilt exempt from L2 (D6), so the fresh `copy` records
      land inside the prefix k ledger copies hold (rule f) and a refilled
      target's journal copy moves past the refilled files. The repair
      passes only when every item was repaired or re-verified, nothing was
      refused or lost, and the closing pass passed.

### Numbers and where they come from

| Number | Source | Derivation | What breaks at another value |
|---|---|---|---|
| `depth` 87.1–89.1 % of every store with depth | M1-1 | Σ manifest `bytes` of `depth` over the store total, per store | Not chosen. It is why option 3 saves little (D2) and why option 4 would save most but is rejected on ADR-022 (D3) |
| 6.2–9.6 % archive-counterpart share; 47–72 MB/day | M1-2 | `aggTrade` + `kline_1m` share per store; 750.5 × 0.062 … 0.096 | Not chosen. A larger share would still be rejected: the D2 argument is the run's integrity, not the size |
| 750.5 MB per day, ≈ 274 GB/year | M1-5 | (5 × 853.4 + 2 × 493.2) / 7 from the #22 + #22 r2 weekday/weekend rates; × 365 | Below the real rate: hot and the copy devices fill early (D10). Above: unused disk. It covers 60 h of one week, so the server's trailing rate replaces it |
| 1 126.3 MB per day, ≈ 411 GB/year | M1-6 | #99's 46.93 MB/h × 24 | An upper bound only for the hours on record. A crash day can exceed it (M1-14), which hot's O term and the 8-day average absorb |
| 2.0× by hour of day, 2.4× by store | M1-7, M1-8 | max/min hourly rate; max/min store rate | Why no single "720 MB/day" is a sizing input: a rate taken from one store can be off by 2.4× |
| 5.5× busiest archive day | M1-14 | 232.41 / 41.99 MB | Why the busiest column, not the mean, sizes provisioning |
| 3 670 files per live day, ≈ 1.34 M/year | M1-9 | 1 835 data files + 1 835 manifests on 2026-10-17; × 365 | The cold listing load and the per-pass copy count. Fewer, larger files would need merging parts, which changes hashes (D5) |
| Copy ledger ≈ 1.6 MB/day | Estimate from M1-9 and the golden records of grammar v1 (#187) | per file one `manifest` record (479 B in the golden ledger, a `depth` file) and k = 2 `copy` records (176 B each): 831 B × 1 835 files = 1.53 MB; plus one `ledger-copy` record (168 B) per copy target per replication pass, 2 × 288 × 168 B = 0.10 MB | An estimate until the follow-up measures it: record sizes vary by a few bytes with the stream name and the digits of `rows` and `bytes`, and verification records depend on P. Higher only adds hot bytes (D10 J); it is under 0.3 % of the data rate |
| ≥ 2 copy targets in a non-empty `cold_roots` | k = 2 (D11) | k counts copies on the copy targets, and every copy target receives every file, archive parts included, so k equals the number of copy targets and k_A = k (#189) | 1: no file ever reaches k copies, nothing ever leaves hot, and the store is the single copy D11 rejects. More than 2 is allowed: it is a larger k |
| An append reads back the last 3 records | D7 *Formats and layout* | the trailing record and the line before it are checked; the third-last supplies the chain the second-last is computed from | Fewer: the line before the trailing record cannot be verified, so a damaged second-last record could be followed by a truncation of the last. More: only read cost, and still no full-chain check, which stays the verification's job |
| At most 1 trailing record ignored or truncated | D7 *Formats and layout* | one append is in flight at a time under the lock, so a crash leaves at most one incomplete record | More than one: damage before the last record would be silently dropped, which is what the chain exists to report |
| Journal 5.1–5.6 MB/day; 15.1 MB/day for #99; 1.9–5.5 GB per year | M1-11 | journal size / captured hours × 24; 5.1 × 365 = 1.86 GB, 15.1 × 365 = 5.5 GB | The journal never leaves hot, so J × t grows hot; the busiest column uses 15.1. The yearly size is why D11 appends tails instead of copying the file whole every pass |
| Import ledger ≈ 6 KB per imported day | M1-15 | 2.25 MB / 379 dates | Negligible in J |
| A = 51.0 GB | M1-12 | 37 000.3 + 14 002.5 MB (archive + hold-out) | A one-time term of each archive copy device |
| a = 43.2 MB per imported day (+81.5 with `trades`) | M1-13 | 41.99 + 0.98 + 0.17 + 0.03 MB; `trades` mean | 0 if the operator stops importing after the cutover; the chain does not read post-cutover archive days (ADR-052 D2) |
| S = 3.7 GB | M1 live table | Σ of the nine live stores' sealed bytes, 3 742.5 MB | A one-time term of each copy device (D9) |
| Hot window: today − 7 onward | `SEED_LOOKBACK_MS` = 7 d (`ingest.rs`), ADR-032 D5 | the seed window `[now − 7 d, now + 1 d)` touches date today − 7 | A normal start's seeds do not depend on it: the seed scan lists hot and the copy ledger, never a cold root (D7 *Seeds*). Below it, the degraded start (ledger lost, hot intact) lists hot alone and misses seeds of streams whose newest event lies in a removed partition, so their resume has no `Disconnected` gap (D8). Above: hot bytes and a longer hot restore only. It follows the lookback, it is not chosen here |
| +1 d look-ahead | `ingest.rs` seed window (open klines filed at their close) | — | Tomorrow's partitions are new, never eligible (D6), so it adds no bytes worth sizing |
| H = 8 days of bytes | D8 | 7 full dates + today's partial date (≤ 1 d) + tomorrow (≈ 0); removal after each seal pass adds ≤ 300 s | Smaller H in D10 under-sizes hot; it is not a free parameter but follows the window |
| 60 s date grace | ADR-030 D4 | — | Only places yesterday's last records in the window; inside H |
| 300 s seal cadence | ADR-032 D10 | — | Reused, not chosen: it is the earliest a part can be copied, so it bounds single-copy exposure. A shorter cadence means more files (M1-9 scales with it) |
| Shutdown waits ≤ 1 seal interval (300 s) for the final pass | D11 *Scheduling in `mie ingest`* (#191 STOP 1 confirmation 4) | a healthy pass copies at most one interval's seals; a pass longer than an interval is already the hung or backlogged case, where waiting gains nothing | Shorter: more healthy final passes are abandoned, so the last files stay at one copy until the next start's first pass, with no loss. Longer: a stuck process stays alive at exit, against a service manager's stop timeout |
| 25 h absorption run | ADR-051 | — | Read through the union, no hot bound (D8) |
| 90 min warm-up trigger | ADR-052 D12 | — | Cold reads lengthen `ready_after_ms`; the trigger fires on the measured value, so it stays correct (Consequences) |
| k = 2 (copies on the copy targets; hot never counts) | Operator, STOP 1 | one device failure must not lose a replicated file (D11) | k = 1: one device failure makes every run containing the file unreplayable. k = 3: +274 … +411 GB per year per added copy (D10) |
| k_A = 2 | This ADR (D11) | a lost archive file changes the chain's prefix dataset; cost ≈ 67 GB in year 1 | k_A = 1: one device failure turns every warm run `DIVERGED (warm prefix)`. Higher: archive bytes only |
| Z (staging), B (largest import run on hot) | ADR-034 (one zip at a time, deleted once imported); D6 *The source lock* | Z: the largest zip, read from the import log at provisioning. B: one day of a for a daily import; a 12-month backfill is 365 × 43.2 MB = 15.8 GB for the default datasets, + 365 × 81.5 MB = 29.7 GB with `trades`, 45.5 GB (M1-13) | Z too small: the zip being fetched fills hot, the import fails, and the capture's writer can hit `ENOSPC` too (D10 *What breaks*). B larger than hot's headroom fills hot during the import with the same result, hence the bootstrap's adoption path for large imports. Larger Z or B than needed: unused hot disk only |
| Hot restore copies 6.35–9.36 GB | D10, D11 | (Σ_s r_s + a) × H at the two rate columns | The capture's downtime after a hot loss grows with it; a smaller H shortens the restore but weakens the degraded start (D8) |
| Initial hot sizes | D10 | week-weighted: (750.5 + 43.2) × 8 = 6.35 GB, + 0.79 GB per outage day, J (5.6 + 1.6) × 365 = 2.6 GB per year. Busiest: (1 126.3 + 43.2) × 8 = 9.36 GB, + 1.17 GB per outage day, (15.1 + 1.6) × 365 = 6.1 GB per year | See D10 *What breaks* |
| Initial copy-device sizes | D10 | year 1, week-weighted: 273.9 + 2.6 + 51.0 + 15.8 + 3.7 = 347 GB; busiest: 411.1 + 6.1 + 51.0 + 15.8 + 3.7 = 488 GB. Later years: 273.9 + 2.6 + 15.8 = 292 GB; 411.1 + 6.1 + 15.8 = 433 GB | See D10 *What breaks* |
| O (cold-outage days) | Operator input at provisioning | lower bound: the longest cold outage the operator expects to repair (a NAS reboot, a disk swap); upper bound: what hot can hold, (hot − reserve − (Σ r_s + a) × H − J × t) / (Σ r_s + a) | Too small: an outage longer than O fills hot and stops the capture (D10). Larger: more hot disk, and O is also the longest single-copy window (D11) |
| P (verification period) | Operator input at provisioning | lower bound: bytes on the largest copy device / slowest root's measured read throughput | Below the lower bound a pass cannot finish. Larger: a damaged copy stays unnoticed longer, a window with one good copy at k = 2 (D11) |
| Origin 2025-10-01 | ADR-052 D1 | — | Why growth has no bound: nothing before the origin can be deleted while it holds (Consequences) |

### Out of scope

- Implementing the multi-root store, the copy ledger, the replicator,
  hot removal, the verification, repair and rebuild commands, the
  `cold_roots` keys and the writer refusal (the follow-up).
- Scheduling the verification, monitoring mount liveness and choosing the
  devices (host-ops).
- Provisioning or choosing the server, its disks or the cold medium.
- Changing the stream set, the depth cadence, `SEED_LOOKBACK_MS` or the
  hold-back (#99).
- The ADR-052 engine snapshot (D12) and #140.
- Moving or deleting any existing store.

## Consequences

- Growth is linear and unbounded: nothing can be deleted while ADR-052's
  origin holds, and moving the origin needs a new ADR. At k = 2 the copy
  devices grow by 584–866 GB per year together (D10).
- Each day's 3 670 files (M1-9) are copied to every copy target and
  verified within P. Merging parts to reduce that is rejected (D5).
  `select` lists no cold root (D7), so the listing cost falls on the
  verification and the rebuild only.
- The warm-up is slower when its prefix is on cold: after the cutover's
  first week the whole archive segment is. ADR-052 D12's trigger and D13's
  queue budget fire on measured values, so they still fire correctly. The
  cost with a live segment on cold can be measured only once #106's
  cutover exists; it is recorded here for #106 and production, not as an
  acceptance criterion.
- Verification reads compete with warm-up reads; P's lower bound states
  the cost of a pass.
- A cold outage or a full copy device keeps new files at one copy. O
  bounds that window, the verification reports every file below k, and
  hot removal waits (D6).
- A lost copy ledger on an intact hot stops replication, hot removal and
  imports and turns every warm-up cold until the rebuild; the capture
  continues (D6, D7). The operator's rebuild time counts against O.
- A lost hot disk stops the capture until the hot restore ends (D11). The
  restore time is a hole in the live data, marked by the resume's seed gap.
  This is the price of never reusing a path or splitting the journal.
- A late archive import into a removed day recalls the partition to hot
  first (D6), so hot briefly holds that day again.
- After a hot loss, an archive file whose entry copy the loss interrupted
  leaves held parts: selections covering them fail with `Integrity` and
  the importer halts until the operator's resolution step (re-admit, or
  replace with its dataset-version cost); the live capture is not
  affected (D11).
- The follow-up work: the multi-root store (listing order, the
  disappearing-partition rule, `Integrity` on held files), the copy ledger
  with its hash chain and lock, the init step with adoption and markers,
  the replicator (root markers, the device-id check, the copy order,
  archive parts held until their import entry, tail appends), hot removal
  under the source lock, the recall, union part numbering, the
  verification, repair, rebuild and hot-restore commands, the hold with
  its re-admit and replace resolution, superseding import entries (ADR-034
  D4), the start refusal on a hot without marker, the `cold_roots` keys,
  and tests.

## Alternatives considered

- **Option 1, everything on the local disk.** Valid as the configuration
  without cold roots; rejected as the policy (D4).
- **Option 3, drop live copies of archive-republished streams.** Rejected
  for every run (D2).
- **Option 4, depth as snapshots only.** Rejected (D3).
- **A per-stream deletion schedule.** Rejected: no live stream can be
  dropped (D2), and the archive cannot be re-created as the same bytes
  (D1).
- **A per-file move.** Rejected: part numbers are per partition (D6).
- **Recompressing or merging on a move.** Rejected: it changes every hash
  and dataset version (D5); ZSTD is rejected in ADR-030 too.
- **A native object-storage client in mie.** Rejected: new dependencies,
  credentials and `deny.toml` changes, against a filesystem mount outside
  mie. The operator chose the mount at STOP 1.
- **Deletion after N years.** Rejected: it needs a new ADR-052 origin.
- **A hot-only seed scan.** Rejected (D8).
- **A union built from cold listings.** Rejected: an empty mount point
  reads as no data (D7).
- **One copy (k = 1), or two copies on one disk or NAS.** Rejected (D11).
- **A local mirror for hot instead of early replication.** Rejected: mie
  cannot read a mirror's halves separately, so the per-copy SHA-256 check
  cannot reach them.
- **A read that falls back past a bad copy.** Rejected: it would hide a
  lost copy (D7).
- **Automatic repair.** Rejected (D11).
- **Copying the journal and the copy ledger whole on every pass.**
  Rejected for tail appends: the per-pass cost would grow with the file
  (D11).
- **A copy ledger without a hash chain.** Rejected: with k = 2 a prefix
  mismatch cannot say which copy is damaged (D7).
- **A chain hash over the whole ledger prefix.** Rejected for the chained
  hash (operator, #187 STOP 1): it covers no hash of the last record,
  reports damage one record late, and re-hashes the whole ledger on every
  append (D7).
- **Rebuilding the copy ledger from root listings alone.** Rejected: it
  cannot tell a lost file from one that never existed, and an unmounted
  root would read as empty; the rebuild requires markers and cross-checks
  the journals and the import ledger (D7).
- **An implicitly created copy ledger.** Rejected: a missing ledger would
  read as "nothing was ever moved" (D7).
- **Refusing writes to a removed partition.** Rejected for the recall: a
  late archive file would become a permanent hole (D6).
- **Gating removal of archive partitions on an import window.** Rejected:
  a catch-up after an outage has no bound (D6).
- **Reading the copy ledger before listing hot, or caching it.** Rejected:
  a concurrent removal would shrink the selection silently (D7).
- **A single copy-ledger writer process.** Rejected for a lock: the
  verification, repair and import run without a capture (D7).
- **Capturing on an empty hot after a hot loss.** Rejected for the hot
  restore: reused part paths, a split journal and missing seeds (D11).
- **Refusing the degraded start when only the ledger is lost.** Rejected:
  it loses raw data although hot still holds every part and the journal
  (D7, D8).
- **Bootstrapping existing stores through hot.** Rejected for adoption:
  A is 5–8 times the hot size (D10).
- **Union `select` over removed partitions only.** Rejected: a recall or
  restore in progress shows a partial partition on hot (D7).
- **Replicating the importer's `PENDING` marker.** Rejected for holding
  archive parts until their entry exists: a cold copy of the marker can
  be stale or name the next file (D11).
- **A restore that writes `PENDING` for unlisted archive parts.** Rejected
  for the hold and an explicit resolution: it maps parts to files by date,
  which is wrong for `metrics` and `fundingRate`, needs a network fetch
  that can stop the restore and so the live capture, and dead-ends when
  the upstream file was republished (D11).
- **Any archive finding that stops the hot restore.** Rejected: the live
  capture waits for the restore, and archive data can be downloaded again
  while live data cannot (D11).
- **Quarantining held parts at the restore and skipping them in
  `select`.** Rejected: they belong to a completed import, so selections
  would shrink silently and cited runs would diverge (D11).
- **Only a fresh re-import as the resolution.** Rejected for re-admit
  first: a re-import writes new parts and turns every run that cited the
  old ones `DIVERGED (warm prefix)`, even when the upstream file never
  changed (D11).

## Accept when

Reachable on the capture host, without the production server. The
follow-up implementation:

1. takes copies of the #22 r2 store and of the archive store, moves every
   eligible partition to a cold root, and `mie replay --source live`,
   `mie replay --source archive` and `mie equivalence` print byte-identical
   reports (dataset version, event-stream hash) before and after the move;
2. after moving every eligible partition of a copy of the #22 r2 store to
   a cold root, starts `mie ingest` (a) with the cold root unreadable and
   (b) with the copy ledger deleted on the intact hot (the degraded
   start), and journals in both the same seeds as a single-root start
   while the capture runs. In a test configuration whose hot window is
   shorter than the lookback, (b) misses the seed of a stream quiet since
   a removed date, which shows that the bound is the binding one;
3. passes the copy, hot-removal, crash and race tests, including a
   removal that lands between a `select`'s hot listing and its ledger
   read (the selection keeps every file and its dataset version), a
   partition renamed while its files are listed (no empty partition), and
   a `select` that lists hot while a recall or the hot restore has copied
   back only some of a partition's files (the selection still holds every
   file and the same dataset version);
4. with one copy target replaced by an empty directory, prints the
   byte-identical item 1 reports from the other copy. With both copy
   targets empty, `mie replay` and `mie equivalence` fail with `Integrity`
   naming a removed file, and never print a report with fewer files, a
   hole or another dataset version;
5. with one byte flipped in one data copy, the verification exits non-zero
   and names the file, the root and the SHA-256 mismatch. The repair
   restores the copy, the next verification passes, and the item 1 reports
   stay byte-identical;
6. refuses a configuration whose two copy targets are on one filesystem,
   and the replicator refuses a copy target without its marker;
7. with one byte flipped in one copy of the copy ledger, the verification
   names the ledger, the root and the first record whose hash chain fails;
8. with the hot copy ledger deleted, `mie ingest` keeps capturing, the
   warm-up fails into a cold run naming the missing ledger, and `mie
   replay` fails with `Integrity`. The rebuild then restores a ledger under
   which the item 1 reports are byte-identical. With one data file also
   deleted from every root before the rebuild, the rebuild reports it as
   lost (named by its journal `sealed` record), and a replay covering it
   fails with `Integrity` naming it;
9. after a capture and its replication, with hot replaced by an empty
   directory: `mie ingest` and the importer refuse to start and name the
   hot restore. After the restore, a new capture continues each
   partition's part numbers without reusing a path, `mie replay --source
   live` lists every replicated pre-loss run, the new run's seeds equal
   those of a single-root start over the same data, and the item 1
   reports for windows before the loss are byte-identical. With hot lost
   between a multi-part archive file's replicated parts and its entry, and
   with the network unavailable, the restore still completes and `mie
   ingest` starts; the unlisted parts are held, a `mie replay --source
   archive` covering them fails with `Integrity` naming them (never a
   shorter report), and the importer refuses. Re-admit then writes their
   entry and the item 1 archive report is byte-identical to the pre-loss
   one. In a variant whose held parts do not match the file, replace
   quarantines them, supersedes the entry, the replay keeps failing until
   the re-import's entry exists, and the quarantined bytes remain. With one
   copy of an archive part damaged before the restore, the restore uses
   the matching copy and reports the damaged one for repair, holding
   nothing;
10. an archive import into a stream-date removed from hot recalls the
    partition, appends the next part number, and the partition is removed
    again once its new part has k copies; the remover skips the archive
    source while an import holds its lock; a `mie replay --source archive`
    running during the recall prints the item 1 report byte-identical;
11. the replicator and a verification appending to the copy ledger at the
    same time leave its hash chain valid on hot and on every copy;
12. after a *replace* resolution, the copy ledger is rebuilt three times,
    from a full copy, from a stale copy and from empty (no copy
    verifies). Each time a `select` of that day equals the post-replace
    result: `Integrity` naming the quarantined paths before the
    re-import's entry exists, and after it the new parts with the same
    dataset version as before the rebuild, never a lost file and never
    a quarantined part listed. The same three rebuilds with an unresolved
    hold leave the held parts held, never ordinary files.

Flagged now: the warm-up cost with a live segment on cold cannot be
reached until #106's cutover exists (Consequences).
