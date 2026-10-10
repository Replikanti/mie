# ADR-030: Raw store keeps verbatim messages in a Parquet envelope

- Status: accepted
- Date: 2026-10-06
- Amended: 2026-10-10 (#77)

## Context

Raw data is immutable and the source of truth (ADR-003, ADR-022): every
derived feature must be reproducible from versioned raw data. Several
constraints shape how it is stored:

- Receive time is capture metadata only and never orders domain events
  (ADR-028 D1). The ordering time is exchange time.
- Live capture (#9) is raw-first: a message is persisted before it is
  parsed, so a message that fails to parse, or that hits `TooManyDecimals`
  in the fixed-point conversion of ADR-027, is never lost.
- Binance stream schemas are not yet verified against the live feed (#9),
  and the archive importer (#12) and the DuckDB adapter (#27) reuse the
  same store.
- Adapters may not depend on each other (ADR-025), yet live capture, archive
  import and replay must all reach the same store.
- Market-data ports are synchronous and pull-based (ADR-026).

## Decision

1. **Verbatim payloads.** The store keeps every exchange message as its
   exact payload bytes — WebSocket/REST JSON or an archive CSV row — in one
   exchange-agnostic Parquet envelope together with its capture metadata.
   Decimals stay the original strings. Fixed-point conversion (ADR-027)
   happens in normalization, which live (#9) and replay (#11) share.
   Neither fixed-point integers nor per-field columns are stored; DuckDB
   reads fields with `json_extract`.
2. **Placement.** A new adapter crate `mie-adapter-parquet` implements the
   store. Its contract — `RawRecord`, `RawRecordSink`, `RawRecordSource`,
   `SealedFile`, `DatasetVersion` — lives in the pure module
   `mie_ports::raw`. Producers write through the port, and `mie-cli` wires
   the crates; there is no adapter→adapter edge and no ADR-025 amendment.
3. **Codec.** `LZ4_RAW` (pure-Rust `lz4_flex`). The codec is recorded per
   file, so switching later never rewrites sealed files.
4. **Partition time and rotation.** The partition date is the UTC date of
   the record's exchange ordering time (ADR-028), supplied by the producer.
   Receive time is never used. A part is sealed before an append that would
   exceed 1 000 000 rows, 128 MiB of payload or 1 h of event-time span. A
   date's open part is sealed once the same stream passes the end of that
   date by a 60 s grace. A wall-clock seal cadence is the caller's job
   (`seal_all`, #9).
5. **Crash rule.** Recovery rolls a part forward only when a durable pending
   manifest matches its data; otherwise the temp files are discarded and
   reported. A kill loses the records written since the last seal; #9 bounds
   that loss with its seal cadence and turns the report into a feed gap.

### On-disk format

**Layout.** Sealed files are Hive-partitioned, so DuckDB reads them with
`hive_partitioning`:

```text
<root>/source=<s>/instrument=<i>/stream=<st>/date=YYYY-MM-DD/part-NNNNN.parquet
<root>/source=<s>/instrument=<i>/stream=<st>/date=YYYY-MM-DD/part-NNNNN.manifest
```

- In progress: `.part-NNNNN.parquet.tmp` and `.part-NNNNN.manifest.tmp`,
  hidden and never matching `*.parquet`.
- Writer lock: one per source, `<root>/source=<s>/.lock`, so live capture
  and an archive backfill of another source run at the same time.
- Path segments match `[A-Za-z0-9_-]{1,64}`. Parts run from `00000` to
  `99999`; a new part takes the highest existing number plus 1.
- Supported event times: `0 ..= 253 402 300 799 999` ms
  (1970-01-01 … 9999-12-31 UTC).

**Envelope schema v1.** Columns in this fixed order:

| Column | Type | Null |
|---|---|---|
| `event_time` | Timestamp(ms, "UTC") — exchange ordering time | no |
| `receive_time` | Timestamp(ns, "UTC") | yes |
| `receive_seq` | UInt64 | yes |
| `session_id` | Utf8 | yes |
| `payload` | Binary | no |

The three capture columns are either all null (archive) or all set (live).
Parquet key-value metadata: `mie.raw.format=1`, `mie.raw.source`,
`mie.raw.instrument`, `mie.raw.stream`. Writer properties are pinned:
`LZ4_RAW`, row groups of at most 65 536 rows or 32 MiB.

**Manifest.** UTF-8, `\n` line ends, one `key value` per line in exactly
this order:

```text
mie-raw-manifest 1
path <relative path of the parquet file>
source <s>
instrument <i>
stream <st>
date YYYY-MM-DD
part <decimal>
rows <decimal>
bytes <decimal>
min_event_time_ms <decimal>
max_event_time_ms <decimal>
sha256 <64 lowercase hex of the parquet file>
```

Numbers are canonical decimals (no sign, no leading zeros). There are no
wall-clock fields, so the manifest is a pure function of the file. Empty
parts are never written.

**Seal protocol.**

1. Close the `ArrowWriter` (writes the footer) over a hashing writer.
2. Fsync the data temp file.
3. Write and fsync the manifest temp file, then fsync the directory so both
   temp entries are durable.
4. Mark both files read-only.
5. Rename the data temp file to its final name, refusing if it exists.
6. Rename the manifest temp file to its final name, refusing if it exists.
7. Fsync the directory.

A file is sealed if and only if its manifest exists. Every visible
`*.parquet` is complete at every instant.

**Recovery** runs under the source lock whenever a writer opens, partition
by partition in sorted order, and is idempotent:

- final data file plus `.manifest.tmp`: verify the hash, then roll forward;
- data temp file plus a valid, matching manifest temp file: verify the
  hash, then roll forward;
- data temp file without a valid matching manifest temp file: delete both,
  report them as discarded;
- orphan manifest temp file: delete it;
- a manifest without its data file, a data file without any manifest, or a
  hash mismatch during roll-forward: `Integrity` error, nothing deleted;
- unknown files are ignored.

**Dataset version.** The lowercase hex SHA-256 of this text, in this order:

```text
mie-dataset 1
window <start_ms> <end_ms>
stream <source>/<instrument>/<stream>        one line per selected stream, sorted
file <path> <rows> <sha256>                  one line per covered file, sorted by path
```

Both sorts are the bytewise lexicographic order of the rendered lines (the
`<source>/<instrument>/<stream>` text and the relative path), not the
field-wise order of the stream key. For example `binance-um/…` sorts
before `binance/…`, because `-` (0x2D) is below `/` (0x2F). A sealed
file is covered when it overlaps `[start, end)`:
`min_event_time < end && max_event_time >= start`. Every line ends in `\n`.

## Consequences

- Nothing a producer receives is lost to parsing: normalization bugs are
  fixed by replaying the same bytes. Live and replay normalize the same
  bytes with the same code (ADR-019).
- Sealed files are verified on every read (size, SHA-256, rows, event-time
  range, stream metadata), and a replay window has a version id that
  experiments record as provenance.
- Verbatim JSON is larger than typed columns, and DuckDB needs
  `json_extract` to reach fields. The storage cost at BTCUSDT depth rates is
  measured in the #9 soak.
- A kill loses the records written since the last seal.
- A `parquet` upgrade changes the bytes written for the same records, so a
  re-import gets new hashes and a new dataset version. Already sealed files
  stay readable.
- Rewriting a file *and* its manifest consistently cannot be detected
  locally, but it changes the dataset version.
- The `flock`-based writer lock assumes a local filesystem.
- The Arrow stack adds about 50 crates and two per-crate license exceptions
  in `deny.toml`: `tiny-keccak` (CC0-1.0) and `unicode-ident` (Unicode-3.0).

## Alternatives considered

- **Typed per-stream columns** (decimal strings, fixed-point integers, or
  both) — lossless only for the fields picked, they force parsing before
  persisting (a parse failure would lose the message) and freeze stream
  schemas before #9 has verified them.
- **The store inside `mie-adapter-binance`** — merges two technologies into
  one crate, while #12 and #27 reuse the store.
- **A shared raw-schema crate plus an ADR-025 amendment** — a new crate role
  for what a port module already expresses.
- **A per-partition index file** instead of one manifest per sealed file —
  a mutable file that every seal rewrites, against immutability.
- **ZSTD** — better compression for JSON, but a C build and a BSD-3-Clause
  license. **Snappy** — weaker compression than LZ4 for no gain here.

## Accept when

The #9 24 h live soak seals files in this format, and #11 replays them with
a stable dataset version.

Progress 2026-10-07: #11 replays sealed files of both sources with a
dataset version that is stable across repeated replays of a fixture store
(ADR-039). Acceptance waits for the #9 soak.

## Acceptance

Accepted 2026-10-08. The #9 live soak (about 30 h over two runs) sealed its
files in this format, and #11 replayed that window twice with the same
dataset version (`437e2790…`) and event-stream hash; the 12-month archive
backfill replayed twice with the same dataset version (`fa73cb88…`).
Results: <https://github.com/Replikanti/mie/issues/11#issuecomment-6053915512>.

## Justification addendum (2026-10-10)

Added for #77 (audit #70). It explains where the numbers of D4, the
on-disk format and the acceptance come from, and what breaks at other
values. It changes no value, criterion or decision. Figures come from the
#77 measurement comment
(<https://github.com/Replikanti/mie/issues/77#issuecomment-6101927637>,
"M0b") unless stated otherwise.

History, for every number below: the #8 plan (comment 6005908136,
2026-10-05) set the rotation limits, the 60 s grace, the row groups, the
segment rule and the part width without a derivation. Its only recorded
reason for any of them was "`read` returns one file at a time, bounded by
the 128 MiB payload limit".

### D4 rotation: 1 000 000 rows, 128 MiB, 1 h

**Where each limit binds.**

- **Live**, at the 300 s seal cadence (ADR-032 D10): none of the three. A
  5-minute part of depth, the heaviest live stream, holds about 2 940
  rows, 6.7 MB of payload and 2.2 MB of file at the #10 rates (9.8 rows
  and 22.2 kB of payload per second). For a caller that seals less often,
  the span binds first on depth: at 60 min. 128 MiB of payload would bind
  at about 101 min, and 1 000 000 rows only after about 28 h. 128 MiB would
  bind before the span only if the mean depth payload rose to about
  3 730 bytes per row at 10 rows per second, 1.65 times the #10 mean.
- **Archive**, where the span is one day (ADR-034 D5): rows bind. 415 of
  780 aggTrades parts and 80 of 90 trades parts hold exactly 1 000 000
  rows. Payload does not bind: a full aggTrades part carries at most
  66.5 MB, 49.5 % of 128 MiB. At 65.6 bytes per row, 128 MiB would bind at
  about 2.05 M rows.

**What each limit is for.**

- **Rows and bytes bound one `read`.** `ParquetRawStore::read` loads and
  verifies the whole file, then returns every record in one
  `Vec<RawRecord>` (`reader.rs`). Its memory is the file, the decoded
  batches and, per row, a 72-byte `RawRecord` plus its payload. Reading
  the largest aggTrades part (1 000 000 rows, 26.9 MB file, 65.4 MB
  payload) peaks at 182.6 MB RSS. The 128 MiB caps the payload term for
  every stream, including large rows: a depth snapshot is 42 kB. The row
  limit caps the per-row term, which dominates for small rows.
- **The same limits bound two archive costs.** Archive replay holds about
  one file per stream (ADR-039 D5). After an archive crash, recovery
  discards the unsealed part (D5) and the resume appends its rows again
  (ADR-034 D4), so at most one part's rows are written twice. In #12 that
  was one temp part of 4.97 MB (M0b, section 9).
- **The span bounds crash loss for callers that seal rarely or never.** A
  part is sealed once it would span more than 1 h of event time. Without
  `seal_all`, a kill therefore loses at most about 1 h of events per
  stream (D5). Under the live cadence the span is inert. 5 min would
  duplicate that cadence, and 1 day would duplicate the date partition,
  which already seals at midnight. 1 h is a judgment between the two.

**At other values.**

- **10 times the rows alone (10 000 000).** On archive aggTrades, 128 MiB
  of payload then binds at about 2.05 M rows: parts twice as large, and
  a `read` of about twice the memory.
- **Both limits 10 times larger.** The busiest archive day (2026-02-05,
  9 542 097 aggTrades rows) would fit in one part. A `read` would need
  about 10 times the 182.6 MB, roughly 1.8 GB.
- **A tenth of the rows (100 000).** 6 215 aggTrades parts for the year
  instead of 780 (by the per-date row counts): about eight times the
  files and manifests, and the same factor in `file` lines in every
  dataset version.
- **Any change now.** A re-import would write other files. The archive
  dataset version accepted above (`fa73cb88…`) would then no longer
  reproduce by re-import, so a change needs a superseding ADR.

### Date grace: 60 s

The grace keeps a date's open part open for records of that date that
arrive after the same stream has crossed midnight. It has to exceed the
largest backwards step of `event_time` within one stream, in the order
the writer sees the records.

- **Which order.** The writer sees each record before the pipeline, and
  so before the 2000 ms hold-back (ADR-032 D2; `live.rs`, steps 3 and 4).
  The audit premise, a grace "presumed > 2000 ms hold-back", ties the grace
  to the wrong quantity, because the hold-back acts after the writer.
- **Live, measured.** In `receive_seq` order, the largest regression is
  3 958 ms (forceOrder, #9 run 2). Every other stream had 0 in every run
  of #9, #10 and #13. Maximum lateness bounds the regression from above,
  at 10 424 ms (openInterest, #9; ADR-032 *Acceptance*), because a stream's
  own maximum never exceeds the merge watermark. 60 s is about 15 times
  the measured regression and 5.8 times the bound.
- **Archive.** The day's 23:55 open-interest sample in `metrics` is filed
  at exactly the next midnight (ADR-034 D3), and metrics rows are not
  sorted (ADR-034 D5). With a grace of 0, that row would seal the day's
  open part while the file still holds rows of that day, and the next of
  them would open a second part.
- **Live midnights.** At the 300 s cadence, the `seal_all` tick falls in
  the first 60 s after midnight about one midnight in five. It then seals
  the old date before the grace can. That happened in #9 (tick at
  +33.4 s) and #13 (+21.8 s). In #10 the grace path sealed the 2026-10-08
  parts live, at 00:01:00–00:01:10 UTC. The tick at +233 s only reported
  them (file mtimes, M0b section 8). None of the three midnights left a
  straggler part.

**At other values.**

- **Smaller.** Below the largest regression, a straggler opens an extra
  part in its old date after that date was sealed. Nothing is lost. Live
  replay recomputes each run in `receive_seq` order (ADR-039 D1–D2), and
  archive replay opens files in `(min_event_time, path)` order and sorts
  each one (ADR-039 D5). Only the file count grows.
- **Larger.** Above the 300 s cadence the grace is inert live, because
  the tick seals first. It also keeps the old date's part open longer:
  unreadable, since only sealed files are read, and exposed to a crash
  (D5). In the archive, each file's rows are sealed before the next file
  starts (ADR-034 D4), so a larger grace changes nothing there.

### Row groups: at most 65 536 rows or 32 MiB

- **Correction.** The audit row calls this the parquet-rs default. It is
  not. parquet 60 defaults to 1 048 576 rows per row group
  (`DEFAULT_MAX_ROW_GROUP_ROW_COUNT`) and no byte cap
  (`max_row_group_bytes` is `None`). 65 536 is 1/16 of the default, and
  32 MiB has no default to come from.
- **What it bounds.** `ArrowWriter` buffers the row group in progress in
  memory and writes it out once a limit is reached, so the caps bound
  writer memory per open part. Live capture keeps up to 7 open parts, one
  per stream, and more around midnight, when the old date's parts are
  still open. Measured on a full aggTrades part (1 000 000 rows):
  - with the pinned properties, the in-progress buffer peaks at 3.2 MB and
    the write adds 10.4 MB of peak RSS;
  - with the parquet defaults, which give one row group, the buffer peaks
    at 26.0 MB and the write adds 36.1 MB.

  Reader memory is unaffected, because `read` decodes the whole file
  anyway.
- **What it costs.** For that part, 4.7 % more file than one row group
  (26.89 against 25.68 MB). That is the overhead of 16 sets of column
  chunks instead of one.
- **Where it binds.** A full archive part has 16 row groups
  (15 × 65 536 + 16 960). A live 5-minute part has one: about 2 940 depth
  rows.

  The byte cap is an encoded-size estimate in parquet 60: the compressed
  finished pages plus the open page. It does not count payload. At 65 536
  rows it binds only above about 512 encoded bytes per row. Live depth
  encodes to about 755 bytes per row (#10), so a depth part would hit the
  byte cap at about 44 400 rows (about 76 min at #10 rates), beyond the
  1 h span. The byte cap is the guard for heavy rows: depth diffs of up to
  74 kB and snapshots of 42 kB.
- **At other values.** Smaller caps add row groups and metadata per file
  and use less writer memory. Larger caps, up to the default, let writer
  memory per open part grow towards the whole part: 26 MB for a full
  aggTrades part. Any change alters the bytes of re-imported files, and
  with them the archive dataset version, as for the rotation limits.

### Path segments 1…64; parts 00000…99999

- **Part width.** A fixed five-digit width keeps the bytewise order of
  paths equal to the numeric part order. That order is used by the
  reader's sorted directory walk and by the `file` lines of the dataset
  version.

  Five digits is the smallest width that holds the finest cadence a
  caller can configure. `seal_interval_secs` is a positive integer
  (`config.rs`), so at 1 s a stream-date gets up to 86 400 `seal_all`
  parts. Empty parts are never written. The 100 000 numbers from 00000 to
  99999 leave 13 600 for restarts and rotations. Four digits (10 000
  numbers) would run out below a 9 s seal interval
  (86 400 / 10 000 = 8.64).

  In use: the live default makes 288 parts per stream-date, and the
  archive at most 10 (aggTrades) and 20 (trades). Running out is a writer
  error ("no free part number"), which stops live capture (ADR-032 D10).
- **Segment length.** At least 1 character, because an empty Hive value
  breaks `key=value` partitioning. At most 64, which keeps the longest
  directory name (`instrument=` plus 64) at 75 bytes, far under the
  255-byte name limit of common Linux filesystems. The longest segment in
  use is `binance-archive` (15 characters), so 64 is a round bound, not a
  forced one.
- **Charset** `[A-Za-z0-9_-]`. It excludes `/` (the path separator), `=`
  (Hive syntax) and `.` (hidden temp files and `..`), as well as spaces,
  `\` and non-ASCII.
- **At other values.** Low consequence: only paths change. But paths enter
  the dataset version, so any change gives every dataset a new version.

### Storage cost (Consequences)

The Consequences above say that the storage cost at BTCUSDT depth rates
"is measured in the #9 soak". That is the wrong reference. #9 captured no
depth: about 99 MB per day for five streams (ADR-032 *Acceptance*). The
reference comes from the #8 plan's risk note ("measured in the #9
soak"). Depth was measured in the #10 soak: about 640 MB per day of
depth, 20 MB of depth snapshots and 720 MB for all seven streams
(ADR-038 *Acceptance*).

The measured cost of verbatim payloads under `LZ4_RAW` is the payload ÷
file ratio:

| Stream | Payload ÷ file |
|---|---:|
| depth | 3.00 |
| depth snapshots | 2.98 |
| live aggTrade | 3.2–3.5 |
| kline_1m | 3.7–3.9 |
| mark price | 2.8–3.0 |
| archive aggTrades | 2.55 |
| archive trades | 2.80 |
| open interest, liquidations | 0.2–0.6 |

The open-interest and liquidation files are small, so their fixed
per-file overhead dominates. No typed-column baseline exists, so the
extra cost of verbatim storage over typed columns is still unmeasured.

### Accept when: the #9 24 h soak and the #11 replay

- **History.** The #8 plan wrote this criterion and took the 24 h from
  #9's soak.
- **What 24 h proves here.** Every window of 24 h or more contains a UTC
  midnight wherever it starts, so the soak had to exercise sealing across
  a date partition on live data. #9 crossed one midnight (2026-10-08), and
  the 300 s tick sealed it at +33.4 s without a straggler part. The grace
  path was exercised live later, in #10 (section *Date grace*).
- **What it does not stand for.** The 23 h connection rotation is
  ADR-032's criterion, not this ADR's. Nothing in the format depends on
  it.
- **Crash rule (D5).** The seven live recoveries in the soak journals
  were all clean, because no live process was ever killed. In #12, a
  deliberate `kill -9` mid-import made recovery discard one temp part
  (2025-10-03 aggTrades part 00001, 4 974 248 bytes, from the #12 import
  log), and the resume was exact (ADR-034 *Acceptance*). Tests cover the rest:
  - `kill_during_write.rs`: SIGKILL after 8 delays, from 0 to 120 ms;
  - `a_crash_at_every_seal_step_recovers_deterministically` and
    `a_crash_mid_part_discards_the_part` (`writer.rs`).

  A roll-forward from a pending manifest and a kill of a live capture
  have never been observed on real data. The criterion did not ask for
  crash evidence, and this addendum leaves it as it is.
