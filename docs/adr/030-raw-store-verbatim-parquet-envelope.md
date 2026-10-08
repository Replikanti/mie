# ADR-030: Raw store keeps verbatim messages in a Parquet envelope

- Status: proposed
- Date: 2026-10-06

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
