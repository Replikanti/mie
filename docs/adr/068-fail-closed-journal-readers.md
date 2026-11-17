# ADR-068: Journal readers that fail closed on a damaged line

- Status: accepted
- Date: 2026-11-17

Relation to earlier ADRs: this ADR amends ADR-066 D7 for checked lines
of a kind a recomputing reader consumes. ADR-066 D7 kept `read_runs`,
`warm_handoffs`, `capture-report` and the rebuild's B1 blind to the line
check and named "a fail-closed reader" as the follow-up, "whose rule for
an unrestorable line is D8's record". This ADR is that follow-up (#229).
It changes no format: not the journal line (ADR-066 D1), not the
`lost-line` record (ADR-066 D8). The decisions record the #229 plan
(<https://github.com/Replikanti/mie/issues/229#issuecomment-6943375295>)
and its STOP 1 operator decisions (2026-11-17): D4 by kind, D5 for a
single-root store, D6 included, `capture-report` not gated, one PR.

## Context

Since ADR-066 a journal line can say whether its bytes are the bytes
written. `mie store verify` (V3) reports a failed line of hot's journal,
and `mie store repair-hot` and `mie store repair --record-lost-lines`
restore or record it. The readers that recompute from the journal did not
look: `read_runs` read a failed `run_start` with a flipped seed as the
run's parameters, and skipped a failed line that no longer parsed, as it
skips a torn one. So a damaged line could change a replay, an equivalence
verdict or the ingest warm start without any output saying so.

A reader cannot restore a line itself, and should not stop on every
damaged line either: most journal lines are of kinds it ignores (`stats`,
`book`, `sealed`, ...), and once the operator accepted a line's loss with
a record (ADR-066 D8), the record must let the reader go on, or the store
could never be read again.

## Decision

### D1. Gated lines

A complete line (one followed by `\n`) is *gated* when `check_line` says
Failed and the reader cannot tell that it ignores the line's kind: the
line does not parse as a JSON object, its `type` is missing or not a
string, its `type` is a kind the reader consumes, or its `type` names no
kind the writer writes.

- The kinds `read_runs` consumes are its six arms: `run_start`,
  `run_end`, `state_checkpoint`, `warm_start`, `warm_ready`,
  `chain_restart` (`GATED_KINDS`). `warm_handoffs` consumes `warm_start`.
  A test derives the six from the reader's behaviour: a lone line of each
  of them is an error, a lone line of any other kind is not
  (`the_gated_kinds_are_the_arms_read_runs_matches`).
- The kinds the writer writes are `KNOWN_KINDS`, the 25 kinds of the
  module table of `journal.rs`. A test parses the table and compares
  (`known_kinds_match_the_module_table`), and `Journal::write` asserts in
  debug builds that every kind it writes is known, so the list cannot fall
  behind the writer.
- A Failed line of a known kind the reader ignores reads as before.
  Intact and Unchecked lines read as before. The last line without its
  `\n` is never classified (ADR-066 D2): it may be in the middle of being
  written.

**Why the parsed `type` of a damaged line can be trusted.** Damage in
place changes bytes and inserts none, so a name can only turn into a name
of its own length. Between a consumed kind and an ignored one of equal
length, the nearest pair differs by 13 bits in 6 bytes (`run_end` and
`removal`). For `warm_handoffs` the nearest pair is `warm_start` and
`warm_ready`, 9 bits in 4 bytes. These are the XOR popcounts over all
equal-length pairs of the 25 names, recomputed by
`kind_names_are_far_apart_in_bits`. So a single-bit flip, or any damage
of fewer than 13 bits (9), cannot rename a consumed kind into an ignored
one, and a renamed line whose name is no known kind is gated. A kind
added later that comes closer lowers this bound; the test pins it, so
such a kind fails the test and needs this ADR revisited.

**Cost.** The reader parses first, and only a gated candidate pays a CRC:
a line of a consumed kind, of no known kind, or that does not parse.
Lines of the six kinds are almost all `state_checkpoint` lines: at the
default state checkpoint interval of 60 s (`state_checkpoint_interval_secs`
in `config.rs`) that is 86 400 / 60 = 1 440 a day, beside a handful of
`run_start`, `warm_start`, `warm_ready`, `run_end` and `chain_restart`
lines per run. Out of the 14 805–16 701 lines a day measured on three
host soaks (ADR-066 D1) that is 1 440 / 16 701 = 8.6 % to 1 440 / 14 805
= 9.7 %. After a year, 365 × 1 440 = 525 600 lines × 1.2–1.4 µs (ADR-066's
`check_line` measurement on a 343-byte line, an upper bound for the
shorter checkpoint lines) = 0.63–0.74 s per read, on top of the JSON
parse every line already pays. What breaks at another value: a 10 s
interval makes it 8 640 lines a day and 3.8–4.4 s per read of a year's
journal; a reader that checked every line would pay the CRC on all
5.4–6.1 M lines of a year (ADR-066 D1's rate × 365), 6.5–8.5 s per read,
for lines whose damage cannot change what it reads.

### D2. The stop

The reader stops on the first gated line in journal order that no
lost-line record matches, and returns an error naming the journal, the
line number, its byte offset and both repairs:

> journal `<path>` line `<n>` at byte `<o>` fails its check (ADR-066 D2)
> and is not recorded lost, so the journal is not read (ADR-068 D2):
> `<FAILED_LINE_REMEDY>`; `mie store verify` names which applies

`FAILED_LINE_REMEDY` is the text verify V3 printed before this ADR:
`mie store repair-hot --scope journal` (with `mie ingest` stopped) when a
copy holds the line intact, otherwise `mie store repair
--record-lost-lines` after checking for a shared cause. V3 now prints the
constant, so its output stays byte-identical and the two texts cannot
drift apart.

The reader does not look for the line on the copies itself. That would
cost k = 2 more journal reads (ADR-062 D11's copy targets) on the cold
roots per replay, and it would turn a reader into a silent repair. The
gate takes priority over structural errors: an unrecorded gated line
stops the read even where a later structural error, or an exclusion
(D4), would otherwise decide.

### D3. Records

A gated line is matched against the `lost-line` records of hot's copy
ledger (ADR-066 D8): a record matches when its offset is the line's first
byte and its SHA-256 is the line's (`LostLine::matches`,
`copy_ledger::line_sha256`). The line number does not enter the match; a
note shows both numbers when they differ (ADR-066 D8). Two records of
one line name the first in ledger order.

- **Read lazily, at most once.** The ledger is read only when a gated line
  exists, and once per read however many there are. Every tiered `select`
  already reads the whole ledger, about 1.65 MB per day of capture
  (ADR-062, as ADR-066 *Consequences* counts it), so this adds at most one
  more read, and only while a gated line exists.
- **No records, the stricter outcome.** When the ledger is absent, cannot
  be read or does not fold, or the journal's file name is not UTF-8 (which
  no record can name), no record is used: every gated line stops, and the
  stop says why. This is the outcome `repair-hot` already takes when it
  cannot read the ledger (ADR-066 D8).
- `lost_line_records` moves from `repair_hot.rs` to `journal.rs`, so the
  readers and `repair-hot` read the records the same way.

### D4. A recorded line

A line a record matches is read without its bytes. What the loss does to
the runs follows from two questions: can the line's own claims be trusted,
and what does losing a line of that kind mean?

- **Neighbours.** A neighbour is the nearest line on each side that
  parses with a string `run_id`, does not fail its check (checked lazily)
  and is not itself read without its bytes. The unterminated last line
  counts when it parses.
- **Attribution by position.** Every checked line is written under the
  journal lock (#226, merged before #231), so a run's lines are contiguous
  and a lost line belongs to the run on one side or the other.
- **Trusted.** A line is trusted when it parses, its `type` is one of the
  six, at least one neighbour exists, and its `run_id` equals the run id
  of every neighbour that exists. One agreeing neighbour of two is not
  enough: the line could be the other run's. One exception, **a run
  boundary**: a line of a kind that marks its run (`run_end`,
  `state_checkpoint`, `chain_restart`, below) is trusted when its
  before-neighbour is of its run and its after-neighbour *opens* another
  run, a `recovery` or a `run_start` (`mie ingest` writes those two
  before any other line of a run). A trusted line acts by its kind:
  - `run_start` (the run's parameters are lost), `warm_start` (its warm
    seeds are lost, ADR-052 D3) and `warm_ready` (no reader consumes it,
    but it is 9 bits from `warm_start`, D1) **exclude** the run.
  - `run_end`: the run reads without its end, as a crashed run (ADR-039
    D2), and is **marked**.
  - `state_checkpoint`: the run reads without that checkpoint and is
    marked.
  - `chain_restart`: the run reads without that restart and is marked. No
    reader consumes the restarts `read_runs` collects.
- **Untrusted.** Any other recorded line excludes the runs of both
  neighbours (none when it has none).
- **Why a run boundary counts as agreement** (review round 1 of #235,
  operator decision). A run's last line is its `run_end` unless a
  replication pass follows it, and only a tiered store has a replicator
  (`ingest/tiers.rs`): in a single-root journal, where D5 reads every
  failed line, A's `run_end` is followed directly by B's `recovery`.
  Without the exception a lost `run_end` would exclude A and the
  innocent B. The exception is safe against one damage event: the line's
  own run id agrees with the line before it, and the line after it is
  another run's opener only if it was written so. A neighbour never fails
  its check, so a checked opener's kind is as written; an unchecked
  (legacy) line would need at least 23 bits of damage to become a
  `run_start` (its nearest name of equal length is `connected`), and no
  other kind has `recovery`'s length (`kind_names_are_far_apart_in_bits`).
  An after-neighbour of another run that
  opens nothing (an ordinary line of B) is no boundary, and the line
  stays untrusted. Residual: multi-bit damage that turns the lost line
  into a mark line of the run before it, followed by the `run_start` of
  a short-lived third run, would mark the wrong run; no variant of the
  rule excludes that third run either, since its run id is only in the
  damaged bytes.
- **An excluded run** is in the state ADR-039 D2 defines for a run whose
  `run_start` was lost: its structural lines are skipped (so they raise no
  orphan errors), its in-window records fail a replay, and records outside
  the window are counted and printed, now naming the excluding line
  instead of "no run_start in the journal". `RunsRead::excluded` lists it,
  with its `run_start`'s time, symbol and source when that line was read.
- **Equivalence.** A run marked for a lost `run_end` or
  `state_checkpoint` is NOT COMPARABLE: `compare` matches checkpoints by
  index (`mie-app` `equivalence.rs`), so a run read without one would read
  as DIVERGED, and without its `run_end` it has no totals. A run marked
  only for a `chain_restart` is judged as before. An excluded run that
  may have started in the window is NOT COMPARABLE, after the judged
  runs: its `run_start`'s time, or, when that line is lost, the time of
  its first readable line (`ExcludedRun::started_at_ms`, the fallback the
  ingest memory guard uses too); a run with no readable line at all
  counts, so the window fails closed. The fallback is the run's
  `recovery`, written just before its `run_start`, so it can sit
  milliseconds before the start: a run starting within that gap after
  `from` is judged outside the window (residual).
- **The ingest warm start.** The memory guard reads the journal through
  the gate. A gated line, or an excluded run that may lie in the chain
  (it started at or after the cutover, by the same `started_at_ms`, or
  its start is not known), fails
  the warm-up: the run starts cold with ADR-052's `warm-up-failed`, its
  `error` naming the line, and the capture never stops.

Why by kind and not "exclude the run for every recorded line": the
simpler rule would exclude a run for a lost `state_checkpoint`, which is
1 440 of about 1 444 structural lines a day (D1), and an excluded run in
the chain keeps every later warm start cold until the cutover moves past
it. Reading the run without the checkpoint loses nothing a recompute
needs.

### D5. A single-root store

A single-root store (`paths.cold_roots` empty) keeps no copy to restore a
line from and no copy ledger to record it in: `verify`, `repair`,
`repair-hot` and `resolve` all refuse it. So a gated line there is read
by D4 as if it were recorded, with the note "not recordable: a
single-root store keeps no copy to restore it from and no copy ledger to
record it". The reader never stops on it, because nothing could ever
unblock it. Every caller's `raw_store` already refuses a tiered hot whose
configuration omits its cold roots, so an empty `cold_roots` means a
single-root store.

### D6. A member damaged mid-run

A complete line that is no JSON object, carries no member of the check's
shape (Unchecked), and whose two immediate neighbours are Intact lines of
the same run, excludes that run and is named. No crash leaves such a
line: a crash tears only a run's last line, and #225 ends it before the
next run's first line, which is another run's. The rule closes a
single-bit bypass of the gate: damage to the member's fixed bytes, or a
hex digit turned into `"`, a control byte or invalid UTF-8, makes a line
Unchecked and unparseable, and without D6 it would be skipped silently.

- **How many bits.** Of the 104 bits of the member's 13 fixed bytes
  (`,"crc32c":"` and `"}`), 65 leave the line Unchecked and no JSON (the
  others turn a letter of the name into another letter, which still
  parses and changes nothing the reader uses). Per hex digit it is 2 bits
  for `0`–`9` and `b`, 3 for `2` and 1 for `a`, `c`–`f`. So 73–89 bits a
  line, 65 + 8 × 28 / 16 = 79 for uniform digits
  (`member_damage_bit_positions`). At the mean line lengths of 193.2–342.6
  B (ADR-066 D1), 8·L = 1 546–2 741 bits, that is 79 / 2 741 = 2.9 % to
  79 / 1 546 = 5.1 % of a line's single-bit flips (2.7–5.8 % over 73–89).
  The property test over a two-run journal counts 1 110 such flips on its
  14 lines between two Intact lines of their run, 79.3 a line
  (`MEMBER_DAMAGE_EXCLUSIONS`).
- Lines written before ADR-066 never qualify: their neighbours are
  Unchecked.
- The same damage on a run's last line stays a residual: it cannot be
  told apart from a torn line (`a_damaged_member_at_a_run_boundary_reads_as_today`).

### D7. Notes

Every reader prints one `journal: <note>` line per line read without its
bytes (recorded, single-root or D6), in journal order, after its header
lines. A note names the line, its offset, `recorded lost at <at_ms>` (with
both numbers when the record's line number differs) or why it is not
recordable or is a D6 line, and the effect (marked or excluded runs). A
journal with no failed and no member-damaged line gets no note, so its
output is byte-identical to before this ADR
(`legacy_journal_replay_report_is_pinned`, captured from `main` before
the change).

### D8. `warm_handoffs`

`mie store resolve`'s *replace* reads every warm run's handoff for its
cost (ADR-062 RQ5) with the gate of D1, `warm_start` as the consumed
kind. An unrecorded gated line stops the replace with "nothing is
written", as an unreadable journal already does. A recorded line or a D6
line adds a cost line, "if it was a warm run's warm_start, that run is not
named above", and the replace proceeds: the operator accepted the loss,
and the cost says what it may hide.

### D9. ADR-066 D7

ADR-066 D7 said the readers ignore the check and there is no gate. For
checked lines of a kind `read_runs` or `warm_handoffs` consumes this ADR
replaces that; for the other kinds, for `capture-report` and for the
rebuild's B1, D7 stands. ADR-066's status line records the change.

### Reader verdicts

| Reader | Verdict |
|---|---|
| `mie replay --source live` | gated; notes after the header; an unjournaled run that is excluded names the line |
| the history chain: `replay`, `experiment`, `alerts` with `--source history`, the equivalence warm pass, the ingest warm-up | gated; `history_chain` returns the read, and the caller prints its notes |
| `mie equivalence` | gated; D4's NOT COMPARABLE; prints its own notes once and ignores the warm pass's |
| `mie experiment run --source live` | gated; notes before the outcome |
| `mie conditional` (live) | gated; notes go to the report, never into the dataset's provenance |
| `mie alerts replay`, `mie alerts report` (live) | both gated; notes ahead of the report |
| the ingest warm start (memory guard) | gated (D4 *The ingest warm start*); on success the notes go to stderr as `mie ingest: journal: …`; the journal format does not change |
| `warm_handoffs` (the replace's cost) | gated on `warm_start` (D8) |
| `capture-report` | not gated: a PASS/FAIL report, not a recompute. It reads mostly kinds the gate ignores, and verify V3 already reports failed lines (STOP 1 decision 4, pinned by `capture_report_is_not_gated`) |
| the rebuild's B1 | not gated: it reads only `sealed` and `recovery`, kinds the gate ignores; a rebuild must run on a store whose journal is damaged, and a damaged path is a name no root holds, which B1 already reports (`parse_journal_ignores_line_checks`) |

## Consequences

- A journal without a failed or member-damaged line reads, replays and
  compares exactly as before; its ledger is never read
  (`no_failed_line_reads_no_ledger`).
- In a tiered store, an unrecorded gated line stops every recompute until
  `repair-hot` restores it or `repair --record-lost-lines` records it.
  Every ingest warm start is cold meanwhile, and the cold `warm_start`
  names the line; the capture keeps running.
- A recorded line that excludes a run in the chain keeps warm starts cold
  until the cutover moves past that run, and a marked run makes every
  equivalence window that holds it fail for good, since records are never
  removed. Both are named in every output.
- **Two journal parses** where a line is read without its bytes: the
  first pass gates and reads the structure until the first such line; an
  exclusion can reach back to a run read earlier, so the structure is then
  read again with the exclusions. A journal without one is parsed once.
- **Residuals.** A kind renamed by at least 13 bits (9 for
  `warm_handoffs`, D1); member damage on a run's last line (D6); two
  faults, one in the member and one in a value, which read as Unchecked
  (ADR-066 D2's residual); and the 2^-32 chance that the CRC misses damage
  (ADR-066 D1). A whole line deleted cleanly is not seen here either
  (ADR-066 *Residuals*).
- `read_runs` takes the configuration's `Paths` instead of the journal's
  path (it needs hot and the cold roots for D3 and D5) and returns
  `RunsRead`; `JournaledRun` gains `lost`.

## Alternatives considered

- **An operator override flag** on each reader, to read past a failed
  line. The record already is the operator's consent (ADR-066 D8), and it
  is durable, replicated and checked by verify; a flag would be a second,
  per-invocation consent, and the ingest warm start runs unattended, so it
  could never pass one.
- **Skipping a failed line**, as a torn line is skipped. A skipped
  `warm_start` silently recomputes a warm run with `run_start`'s cold
  seeds (`JournaledRun::live_run`), and a skipped `run_start` turns its
  run's records into unattributed ones: the output changes and nothing
  says why.
- **Excluding the run for every recorded line** (D4's simpler rule): a
  lost checkpoint, the most frequent structural line, would make warm
  starts cold until the cutover moves past its run, for a line no
  recompute needs.
- **Reading the copies** to restore a failed line in the reader: k more
  journal reads per replay on the cold roots, and a reader that repairs
  silently (D2).
- **Gating every failed line**, whatever its kind: the readers would stop
  on damage to lines they never read (most of the journal), and pay a CRC
  on every line (D1 *Cost*).
- **Blocking a single-root store** (D5's alternative): nothing could ever
  unblock it, since it has no copy and no ledger. Keeping it ungated
  would read a damaged `run_start` as written.
- **A gated `capture-report`**, counting failed lines as FAIL problems:
  verify V3 already reports them, and the report is a soak's verdict over
  mostly ignored kinds, not a recompute.
