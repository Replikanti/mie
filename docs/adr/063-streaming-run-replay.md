# ADR-063: Streaming replay of one cleanly ended run

- Status: accepted
- Date: 2026-11-06

Relation to earlier ADRs: this ADR changes how `LiveReplay::run_replay`
(ADR-041 decision 4, the equivalence harness's unit) reads a run. It
supersedes ADR-041's memory consequence ("one run is held in memory while
it is compared") for `run_replay` only; ADR-041's status line records
that. ADR-039 (the windowed replay) and ADR-052 decision 13 (the history
chain, which holds one run and is guarded by `[history] max_run_records`)
stay as they are. The decisions record the #140 plan
(<https://github.com/Replikanti/mie/issues/140#issuecomment-6677633898>)
and its STOP 1 operator decisions (2026-11-06): the two-pass read, and the
residual growth of one bit per record plus the file manifests, accepted
as a known limit (D4).

## Context

`run_replay` collected every record of the run into one `Vec`, sorted it
by `receive_seq` and recomputed it, so its memory grew linearly with the
run's length. ADR-053's Acceptance measured it directly, at main
`040dfae`, on ADR-051's R0 live store (run `20261016T225606Z`, 34 h 33 min,
2 254 454 records, 2 042 225 events): `measure_triggers_live` peaked at
6.1 GB of anonymous memory, and `measure_candidates_live` the same, since
the cost sits in the shared replay path. That is about 2.7 KB per record
(6.1 GB ÷ 2 254 454). At the store's rate of 65.2 k records per hour
(2 254 454 ÷ 34.55 h), a 7-day run has about 10.96 M records, about
30 GB. ADR-052's 550 MB came from a 1 h 40 min sample and is not a bound
(ADR-052 decision 10, annotated by this change).

The stored layout allows a merge. `mie ingest` assigns `receive_seq` and
appends the record in one step (`on_frame` in `live.rs`), every file
returns its records in append order (the `RawRecordSource::read`
contract), and the writer seals every open part every
`[capture] seal_interval_secs` (`RawWriter::seal_all`, 300 s by default,
ADR-030 D5's crash-loss bound, journaled in `run_start`). So a file's
records are a sorted run of seqs appended within one seal interval.

ADR-053 is not edited: its 6.1 GB is a direct measurement at `040dfae`,
not an extrapolation (the issue's fifth acceptance item asked to check
that), and it already names #140 as the follow-up.

## Decision

1. **D1 Scope: only `run_replay` streams.** `LiveReplay::replay` (windowed,
   chained and crashed runs) and the history chain keep their one-run
   buffer. Why: ADR-052 decision 13 defers the chain's streaming recompute
   to its own trigger and guards it with `max_run_records`, and the
   chain's failure contract (decision 9: "a failing run delivers none of
   its events"), `tail()` and `run_of_last_event()` rest on a loaded run.
   The error texts both paths share are factored into one set of
   constructors, so they cannot drift.

2. **D2 Two passes over the run's files.**
   - **Pass 1, integrity**, inside `run_replay`: every selected file is
     read once, in path order, one at a time. It keeps a bitset of the
     run's seqs and, per file, the run's record count and smallest and
     largest seq. It raises every error the buffered read raised, with the
     same text and precedence: per file an unknown stream, a store
     failure, a record without capture metadata; then the smallest seq
     stored twice, the first hole, the count against `run_end`. The bitset
     covers `[0, min(clean_records, Σ rows of the selection))`, never the
     journal count alone, so a corrupt `run_end` cannot allocate
     gigabytes; seqs above it go into an ordered set that is empty for a
     valid run.
   - **Pass 2, recompute**, lazily from `next_event`: a k-way merge (D3)
     feeds the run's `RunContinuation` one record at a time.
   - Why two passes: a run is replayed in full or not at all, and
     consumers rely on it. The equivalence harness reports a failed
     `run_replay` as NOT COMPARABLE, and the measurement tools skip it as
     "not replayable". With one pass, a hole or a duplicate would surface
     after events were delivered.
   - Cost: one more read of the run's files. Baseline: the live T0 run's
     75.4 s at `040dfae` (ADR-053, Acceptance). Measured: see
     *Measurement*.

3. **D3 The merge order is ascending `receive_seq`.**
   - The files with records of the run are sorted by their first seq. A
     file is opened (read, the run's records kept and sorted by seq, its
     count, first and last seq checked against pass 1) when its first seq
     is at or below the smallest open head, or when no file is open; a
     used-up file is dropped. The sort is a no-op on files `mie ingest`
     wrote.
   - Equivalence: pass 1 proved that the run's seqs are exactly `0..N`,
     once each. A pending file's records are all above its first seq, and
     pending files are opened in first-seq order, so the smallest open
     head is the smallest remaining seq. The merge checks that every
     record it yields carries the next seq. The output is therefore the
     order of the old global `sort_by_key(receive_seq)`, and
     `RunContinuation`'s documented property (pushing records one at a
     time gives exactly the events of a recompute of all of them) makes
     the events identical.

4. **D4 The bound: peak = W + N/8 bytes + O(files).**
   - **W**, the run's records held in open files, plus the one file being
     decoded. Every seal seals every open part, so every part's records
     were appended within one seal interval, and the open files at any
     merge position belong to the interval that holds it. So W is at most
     the run's records of one seal interval, which does not depend on the
     run's length. On the 34 h store, counted from its 415 `sealed`
     journal entries: at most 13 906 records per seal (0.62 % of
     2 254 454), 5 432 on average, at most 5.55 MB of files and 13 files
     per seal (two date partitions at midnight).
   - **General case**, a store written without periodic seals: at most
     one part per open (stream, date) partition, each within ADR-030 D4's
     caps (1 000 000 rows, 128 MiB payload, 1 h span). Decoding one file
     peaks at up to 182.6 MB, which ADR-030 measured for reading a full
     1 000 000-row aggTrades part.
   - **N/8 bytes, the known limit** (accepted at STOP 1): 282 KB at
     2 254 454 records; a 7-day run at 65.2 k records per hour, about
     10.96 M records, needs about 1.37 MB. Against the 2.7 KB per record
     of the buffered read this is about 1/21 600 of it.
   - **O(files), the known limit's second part:** the selection's
     manifests, already held before this change, plus about 40 B per file
     for the plan (2 655 files on the 34 h store, about 106 KB).
   - **The pipeline** is the one live runs for the whole run. Its state is
     bounded by the hold-back and the book buffer, as live.
   - **What breaks at another value.** A longer seal interval grows W up
     to the D4 caps above, never with the run's length. A store whose
     files each hold a whole run (as the older unit fixtures do) makes W
     the whole run. The bound is a property of the part granularity, which
     the writer caps.

5. **D5 The acceptance check** is `peak_buffered_records ≤ the largest
   seal of the run`, counted exactly, with the largest seal summed from
   the run's `sealed` journal entries (one entry holds every part sealed
   since the last seal, cap rotations included). The peak RSS is recorded
   but is not a threshold: RSS includes the tool's own state and the
   allocator's retained memory, and a threshold would be a number without
   a measurement behind it.

6. **D6 Stats and failure after construction.** The record counts of
   `stats()` are final when `run_replay` returns (pass 1); the pipeline
   counters are final once `next_event` returned `Ok(None)`. A pass-2
   read failure, or a file whose records of the run differ from what
   pass 1 found, fails `next_event` with `ProviderError::Source`, and the
   failure sticks. With sealed, SHA-256-verified, immutable files
   (ADR-030) that needs a storage fault between the passes.

## Consequences

- `run_replay`'s memory no longer grows with the run's length, apart from
  the N/8 bytes of D4; the equivalence harness and the live measurement
  tools can replay week-long runs.
- Every file holding records of the run is read twice, and a file holding
  only other runs' records once. The wall-time cost is in *Measurement*.
- **Breaking API:** `RunReplay` becomes `RunReplay<'a>` (it borrows the
  store), the pipeline counters are final only after the drain, and a
  storage fault can now fail the stream midway (D6).
- `LiveReplay::replay` and the history chain still hold one run;
  ADR-052 decision 13 measures its bytes per record on that path, as
  annotated by this change.

## Alternatives considered

- **(a) One pass, errors found mid-stream.** It saves a read, but a hole
  or a duplicate would surface after events were delivered, breaking the
  full-or-nothing contract of D2.
- **(b) Opening files by event time, as the archive merge does (ADR-039
  D5).** A live run's order is receive order. On the 34 h store a record
  arrives up to 149 s after its event time (`max_lateness_ms` 148 997,
  aggTrade, `run_end`), and no bound holds for late records, so a wrong
  guess would reorder silently.
- **(c) Adding `receive_seq` bounds to the manifests.** A storage-format
  change, out of #140's scope. It would still need pass 1 for duplicates
  and holes, and pass 1 yields the bounds anyway.

## Accept when

The measurement below holds on ADR-051's R0 live store: the equivalence
reports of main and this change are byte-identical, the whole-stream event
hash is the same, `measure_triggers_live` passes T0 with 0 rejections and
0 mismatches, and `measure_run_replay_bound` holds the peak at or below
the run's largest seal; the operator approves it at #140's STOP 2.

## Measurement

Measured 2026-11-06 on ADR-051's R0 live store (run `20261016T225606Z`,
34 h 33 min, 2 254 454 records), release builds of main `16c8529` and of
this change, run one after the other on the same host, each command
under an 8 GB memory cap. Peak RSS and wall time come from
`/usr/bin/time -v` on the prebuilt binaries, the same method on both
sides; each is one sample.

| Check | main `16c8529` | This change |
|---|---|---|
| (a) `mie equivalence --from 2026-10-16 --to 2026-10-17`, report | `run 20261016T225606Z: records 2254454, events 2042225, rejections 0, …: EVENTS EQUIVALENT, STATE NOT COMPARABLE` (the run's feature set predates the current one) | byte-identical (`cmp`), SHA-256 `dc702ea2…1aa5` on both |
| (a) peak RSS, wall | 6 228 068 kB, 82.7 s | 51 764 kB, 71.3 s |
| (b) `measure_run_replay_event_hash` | `2042225:e082e405a1420448`, pipeline counters printed | identical line |
| (b) peak RSS, wall | 6 250 824 kB, 30.7 s | 45 500 kB, 26.3 s |
| (c) `measure_triggers_live` T0 | 0 mismatches, 0 rejections, absorption 16 / 15 / 6 | identical output apart from timings |
| (c) peak RSS, wall (the test's `wall` line, which starts after `run_replay` returns) | 6 251 760 kB, 82.1 s (53.5 s) | 48 928 kB, 61.8 s (58.1 s) |
| (c) `measure_candidates_live` | 2 042 225 events, 0 rejections | identical output apart from timings |
| (c) peak RSS, wall | 6 252 968 kB, 72.6 s | 51 336 kB, 58.8 s |
| (d) `measure_run_replay_bound` | (not on main) | peak 13 829 buffered records ≤ largest seal 13 906; 45 MB peak RSS (44 700 kB), 23.4 s |

- **Memory:** the peak RSS falls from 6.2 GB to 45 to 52 MB on every
  consumer, about 1/120. The rest is the tool's own state, the 13 829
  buffered records, the 282 KB bitset, the manifests and the decoding of
  one file.
- **Cost of the second read (D2):** none measurable. Every command is
  faster than on main (a 14 % to 25 % shorter wall time), because the
  two reads cost less than collecting, sorting and freeing 6 GB. ADR-053's
  75.4 s at `040dfae` is the per-run figure of an earlier binary; main's
  same line today is 53.5 s. The `wall` line of the trigger and candidate
  tools now includes the recompute that `run_replay` used to do before
  their timer started, which is why only that line grows.
- **Bound (D5):** the peak of 13 829 records is 0.61 % of the run, below
  its largest seal of 13 906.
