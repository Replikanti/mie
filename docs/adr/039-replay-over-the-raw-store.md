# ADR-039: Replay over the raw store — per-run recompute for live data, canonical merge for the archive

- Status: proposed
- Date: 2026-10-07

## Context

#11 implements the `HistoricalDataProvider` port over the raw store
(ADR-030) and `mie replay`. The port promises that a replay exposes only
what was available at each decision moment and matches live processing of
the same events (ADR-019). Two raw sources exist, and they differ in what
the raw store alone determines:

- **Live capture** (`binance-um`, ADR-032). What live delivered is a
  function of a run's raw records in `receive_seq` order *and* of the run's
  pipeline parameters — `hold_back_ms`, the open-interest re-time allowance
  and the restart seeds — which only the run's journaled `run_start` holds
  (ADR-032 D4). Late events became `LateEvent` gaps, lagging open interest
  was delivered re-timed (ADR-032 D12, which supersedes the issue's "visible
  at response time"), and every restart opened with a seed gap.
- **Archive backfill** (`binance-archive`, ADR-034). Rows carry exchange
  fields only, every row is filed at its ADR-028 ordering time
  (ADR-034 D3), and archive open interest is already ordered at the end of
  its interval. There is nothing to recompute; the canonical order of the
  event set is the answer (ADR-028).

A plain canonical re-merge (sort and dedupe) of live records was considered
and rejected for live data: it would deliver open interest at its exchange
`time`, 4–8 s before live knew it (look-ahead), and late events in place
instead of as the gaps live delivered, so the equivalence harness (#13)
could never match.

ADR-028 left one point open: a gap still open at the end of a replay window
(a trailing gap) "is handled by the replay merge (#11)".

## Decision

1. **Live replay is a per-run recompute (D1).** `LiveReplay`
   (`mie-adapter-binance`, `replay` module) recomputes each capture run with
   the live `Pipeline`, from the run's first record, with the parameters of
   its `run_start` line, and restricts the output to the window afterwards.
   The journal is therefore required to replay live data: `mie replay` reads
   the runs with `journal::read_runs`. Starting a recompute mid-run would
   change what the hold-back released, so mid-window exactness depends on
   recomputing from the run's start.
2. **Run selection and integrity (D2).** A run's extent is
   `[started_at − 300 s, end + 300 s)`, where `end` is its `run_end`, else the
   next run's start (a crash), else open. The margin covers open klines
   (filed at their close time, up to a minute ahead) and lagging open
   interest. The replay recomputes the runs whose extent overlaps the window,
   lazily and one at a time, each from the files of its streams that overlap
   its extent; a run always replays all of its streams, because the
   hold-back watermark depends on all of them. A run's records are those
   whose `session_id` starts with `<run_id>/`; a record without capture
   metadata, or a `receive_seq` stored twice, breaks the contract. The
   sequence must count up from 0. A run that ended cleanly (exit code 0) must
   be complete: a hole, or a record count other than its `run_end` count, is
   an error naming the run and the missing range. A crashed run (no
   `run_end`, or a non-zero exit) is recomputed over its contiguous prefix;
   the records after its first hole are counted as ignored. Two `run_start`
   lines with one run id — a restart within one second, whose session
   prefixes would be ambiguous — are an error. Every capture record in the
   window must belong to a recomputed run; otherwise the replay fails,
   naming the runs, record counts and files. Records of a run whose
   `run_start` was lost cannot be recomputed without its parameters, and they
   never vanish silently from a replay. Outside the window they cannot affect
   its output — live gave every run a fresh pipeline, and chaining (D3)
   reaches only the restart's first seconds — so the replay counts and
   reports them (`mie replay` prints one line per such run) instead of
   failing: a lost journal line must not make an adjacent run unreplayable. `mie ingest` syncs the journal to
   stable storage right after `run_start` and at the end of a run, before the
   first record can be sealed. An open run (no `run_end`, no successor) is
   read only up to the window's end plus the margin: later records cannot
   change the window's output (the margin bounds lateness, as for the
   extent). So the dataset version of a closed window depends only on files
   that can affect it, and stays the same while a later run goes on
   capturing, once that run has sealed past the window's end plus the margin
   (about one seal interval later).
3. **Run chaining (D3).** Each run's output continues after the last event
   delivered before it through a zero hold-back buffer that knows that event
   and the newest delivered open-interest time. In-order output passes
   unchanged. When a fast restart makes runs overlap in event time, the
   later run's events follow ADR-028 D6 and ADR-032 D12: they become
   `LateEvent` gaps or re-timed open interest, never an engine
   `OutOfOrder`. Across runs the newest open-interest time is the delivered
   (possibly re-timed) one, because the exchange time of a re-timed sample is
   not carried on the event (ADR-032 D12 limitation).
4. **Window restriction and trailing gaps (D4).** An event is delivered
   when its ordering time lies in `[start, end)`; a gap counts at its `end`.
   A gap whose `end` is at or after the window's end is **not delivered**:
   live announced it only when the stream resumed, which is after the
   window, so delivering it would be look-ahead. Providers report such
   trailing gaps out of band (`trailing_gaps()`, printed by `mie replay`).
   This settles ADR-028's open trailing-gap point. Consequence: a window
   inside a run delivers exactly the full run's events in it, and adjacent
   windows concatenate to their union.
5. **Archive replay is a streaming canonical merge (D5).** `ArchiveReplay`
   (`archive::replay`) selects each stream over its window widened to whole
   UTC days and opens files in `(min_event_time, path)` order, each once its
   smallest event time is at or below the smallest head of the open files,
   so about one file per stream is in memory. Each file is normalized with
   the import's code, restricted to the window and sorted; an event whose
   ordering time differs from its record's `event_time` breaks the contract,
   and a row that does not normalize fails the replay with its file and
   time. Per raw stream the live `StreamSequencer` drops repeats and turns a
   trade-id jump into `SequenceBreak`; one zero hold-back buffer places
   same-millisecond gaps. Which archive days the store holds is read from
   the sealed files' manifests by **source day**, the UTC day of the archive
   file a row comes from: the day of its ordering time minus one five-minute
   step for `metrics` (ordered at the end of its interval, ADR-034 D3), of
   its ordering time otherwise. A file holds the source days of its first
   and last row. Partition dates would not do, because a metrics file's
   23:55 row is filed in the next day's partition and would make a
   never-imported next day look present. A source day of the widened window
   that no sealed file of a stream holds is a `MissingData` gap from the stream's previous
   event (or the window start) to its first event after the missing days,
   delivered right before that event in place of any sequence-break gap;
   missing days with no later event are trailing (D4). Default streams are
   the configured ones except `trades` and `bookDepth`; `bookDepth` (no
   domain event) and `aggTrades` with `trades` (one trade-id space) are
   refused. `archive-kline-check` reads through it, which replaces the
   temporary window provider of ADR-034 D7; the reported dataset versions
   are now those of the day-widened selections.
6. **One source per replay (D6).** A replay reads either live capture or
   the archive, never both. Stitching or deduplicating live and archive
   data in one stream is out of scope.
7. **Event-stream hash, encoding v1 (D7).** A delivered sequence is
   identified by `EventStreamHash` (`mie-domain`, `event_hash` module): the
   event count and an FNV-1a 64 fingerprint (ADR-029 writers) over the
   header `write_str("mie-event-stream")`, `write_u32(1)`, then per event
   the ADR-028 kind rank as `u8` and every payload field in declaration
   order — times as `i64` ms, `Price`/`Qty`/`Rate` as `i64` units, ids and
   counts as `u64`, the open-interest resolution as `u32`, levels as a count
   and (price, quantity) pairs, `Stream` as its ADR-028 ordinal, `Aggressor`
   (Buy 0, Sell 1) and `GapReason` (Disconnected 0, SequenceBreak 1,
   LateEvent 2, MissingData 3) through explicit matches. Golden values are
   pinned by a test; any change is a new encoding version and a new ADR.
   It displays as `<events>:<16 hex>`.
8. **Provenance in the port (D8).** `HistoricalDataProvider::replay`
   returns `Replay { stream, dataset }`, so no replay can omit the dataset
   version of the raw data it reads (Data Plane design rule 6).
   `ReplayReport` carries the dataset version, the event-stream hash and the
   domain-rejection count.
9. **Replay tolerates domain rejections like ingest (D9).** Live ingestion
   and `ReplayService` share `mie_app::drive_tolerant`: a rejected event is
   counted and driving resumes; a provider failure stops. `mie replay` exits
   1 when any event was rejected, when the replay failed, or when the window
   holds no event; its report has no wall-clock field, so the same window
   prints the same bytes.

## Consequences

- Live replay reproduces what live delivered — gaps, late events and
  re-timed open interest included — and #13 can compare live and replay
  event by event with the hash of D7.
- Live replay needs the journal; a lost journal (or a lost `run_start`)
  makes that live data unreplayable as live delivered it — the replay fails
  loudly, and the raw records remain.
- Memory: one run's records and output at a time (about 1.5 M records for a
  24 h run); the archive merge holds about one file per stream.
- A record outside its run's extent would silently change the recompute;
  the contiguity check from `receive_seq` 0 and the `run_end` count turn
  that into a loud error for clean runs. A crashed run replays only up to
  its first hole.
- The kline check now sees trade-id breaks and missing days as gaps, which
  leave the bars they touch incomplete; a re-run of the accepted three-day
  check confirms the numbers after merge.
- The hash encoding becomes a contract that #13 depends on.
- Replaying a window that a still-capturing run overlaps treats the run as
  crashed (no `run_end`): its sealed prefix up to the window's end plus the
  margin is replayed, and a later replay of the same window can differ until
  the run has sealed past that point.

## Alternatives considered

- **Canonical re-merge of live records** — simple and source-agnostic, but
  look-ahead on open interest and late events delivered in place (Context).
- **Recompute from the window start with seeds** — cheaper for short
  windows, but the hold-back state at the window start is unknown, so the
  output would differ from what live delivered.
- **Deliver trailing gaps** — keeps every gap in band, but announces a loss
  before live could have known it.
- **`std::hash` or a cryptographic hash for the stream** — `std::hash` is
  not stable across Rust releases; SHA-256 would add a core dependency
  (ADR-025) for collision resistance an identity does not need (ADR-029).
- **Stop the replay on a domain rejection** — strict, but differs from what
  ingest does with the same event, so live and replay would diverge on the
  first bad event.

## Accept when

The #9 soak window replays twice with the same event-stream hash and
dataset version, and the 12-month archive backfill replays end to end twice
with the same hash and version.
