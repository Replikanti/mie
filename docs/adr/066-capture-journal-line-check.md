# ADR-066: A per-line check in the capture journal

- Status: proposed
- Date: 2026-11-16

Relation to earlier ADRs: this ADR gives the capture journal (ADR-039 D1,
`journal.rs`) a per-line check, so that `mie store verify`, `mie store
repair` (ADR-062 D11 R7 with ADR-065 D8) and `mie store repair-hot`
(ADR-065 D2, D3) can attribute damage to one journal line by the line's
own bytes. Lines without the check keep ADR-065's byte rules unchanged.
The decisions record the #224 plan
(<https://github.com/Replikanti/mie/issues/224#issuecomment-6903444109>)
and its STOP 1 operator decisions (2026-11-16). Delivery is three PRs:
the line format (D1, D2, D7), then attribution between hot and the copies
(D4, D5, D6's V4), then hot's own failed lines and the lost-line record
(D6's V3, D8). This ADR stays `proposed` until the third; the second may
still edit it. At acceptance, the third records two changes on earlier
status lines: ADR-065's (D2 and D8 for checked lines) and ADR-062's (D11
R7's reason "the journal has no chain" for checked lines, and the copy
ledger's `lost-line` kind with its place in D7's fold).

## Context

The journal is one JSON object per line, appended, with no integrity of
its own. ADR-062 D11 R7 records the consequence: with one copy against
hot, damage cannot be attributed, "since the journal has no chain".
ADR-065 then attributes damage by bytes: a strict majority of hot and the
k = 2 copies at every byte (D8), the copies agreeing against hot over its
whole length (D2 *damaged*), and a fork guard that errs toward stopping
(D3). Three states stay out of reach of a byte vote:

1. **One copy against hot.** With the second copy shorter than the
   differing byte, ADR-065 decides *one-witness* and stops; R7 refuses
   one against one.
2. **Hot's newest lines.** Lines past every copy's end have no witness, so
   damage there is invisible to every command.
3. **A fork that looks like damage.** ADR-065 D3 guards a fork by the
   shapes a crash leaves, and its blind spot is an equal-length fork
   whose records end at C's `\n` positions. Whether hot or C holds the
   true line is a heuristic call.

A line that can say whether its own bytes are the bytes written answers
all three, without a copy for the second.

## Decision

### D1. The line format

The writer (`Journal::write`) serializes a line as before, compact, with
`serde_json::to_vec` (keys sorted, since `serde_json` runs without
`preserve_order`). It computes CRC-32C over those bytes, drops the
closing `}`, appends `,"crc32c":"<8 lowercase hex>"}` and `\n`, and
writes all of it with one `write_all` (`with_check`). The member is 20
bytes and always the line's last; it is appended as bytes, so key order
does not matter, and verification needs no JSON parse: it removes the
member, restores the `}` and recomputes.

- **CRC-32C** is the Castagnoli CRC of iSCSI: reflected polynomial
  0x82F63B78, init and xorout 0xFFFFFFFF. It is pinned by the standard
  check value (`"123456789"` → e3069283) and the three vectors of RFC 3720
  B.4 (32 × 0x00 → 8a9136aa, 32 × 0xff → 62a8ab43, 0..=31 → 46dd794e), all
  recomputed independently of this code. The hex keeps its leading zeros
  (`%08x`).
- **The name `crc32c` is reserved at every depth.** The writer drops a
  top-level caller field of that name, and no line type nests one. JSON
  escapes every `"` inside a string, so a string value cannot hold the
  shape either. So a torn line, which ends before its final `}`, can
  never end in the member's shape.
- **The check is never added to lines that already exist.** A journal
  written before this ADR keeps its bytes, and its lines stay Unchecked
  (D2).
- **It is no MAC.** Anyone with write access can recompute it. It guards
  against accidental damage (bit flips, torn or zeroed regions, a
  misdirected write), which is what the tiered store's copies guard
  against too.

Why 32 bits of CRC-32C:

- Every degree-32 CRC detects every single-bit error and every burst
  error up to 32 bits. Random damage escapes with probability 2^-32 ≈
  2.3×10^-10 per damaged line. An escape makes a damaged line read as
  written, and D4/D5 then see two lines that both verify, which is a
  fork, and stop: an escape costs a stop in every single-fault case, not
  a wrong write.
- CRC-32C over CRC-32/IEEE: the same width, but a larger Hamming distance
  at the line lengths here (Koopman, "32-Bit Cyclic Redundancy Codes for
  Internet Applications", DSN 2002; literature, not measured here).
- No crate: the table-driven CRC is about 30 lines of std-only code with
  a `const` table, pinned by the vectors above. Rust std has no CRC, and
  a crate would be a new dependency of `mie-cli` for 30 lines.

**Overhead**, measured 2026-11-16 on the journals of three host soaks
(#10, #22 and #99: 8 440, 17 764 and 98 546 lines): the mean line is
193.2–342.6 B, so +20 B is +5.8–10.4 % (20 / 342.6, 20 / 193.2). At the
measured 14 805–16 701 lines a day that is 0.30–0.33 MB a day, 108–122 MB
a year: +2.0 % on ADR-062 D10's busiest J of 6.12 GB a year (0.122 /
6.12). What breaks at another width: a wider check costs only bytes and
changes no decision (D4/D5 stop on an escape anyway); a narrower one (16
bits) lets 1 in 65 536 damaged lines read as written.

**Throughput**: 199–257 MB/s of CRC, measured in release mode
(*Consequences*).

### D2. Line verdicts

`check_line(body)` classifies one line, where the body is the bytes
between two `\n`, without them (split at `\n` only, as ADR-065 D3 splits;
a `\r` before the `\n` is part of the body):

- **Intact**: the body is at least 22 bytes and at most
  `MAX_CHECKED_LINE`, it ends with the 21-byte shape `,"crc32c":"` + 8
  lowercase hex digits + `"}`, and the CRC of the body without the member
  (and with `}` restored) equals the hex.
- **Failed**: the shape is there and the CRC differs.
- **Unchecked**: no shape. That covers a line from before this ADR; a
  torn line (a cut before the final `}` never ends in the full shape); a
  trailing `\r`; damage that hit the member's fixed bytes or turned a hex
  digit into anything but a lowercase hex digit; a look-alike name
  (Cyrillic `с`, a zero-width space) or a blank after `:`; and a body
  longer than `MAX_CHECKED_LINE`.

A last line without its `\n` is never classified: it may be in the middle
of being written. Unchecked makes no claim, so ADR-065's byte rules apply
to it unchanged. The 22-byte minimum is `{` plus the shape: the check is
over bytes, not JSON, and the shortest body it can carry is the object
`{}`'s.

**`MAX_CHECKED_LINE` = 1 048 576 bytes.** It is about 450 times the
longest line measured in the three soak journals (2 318 B, a `stats`
line). The line types with growing lists (`sealed`, `recovery`, `recall`)
would need that much growth to lose their check. Why a cap at all: verify
and repair stream lines beside a live capture, and a zero-filled region
with no `\n` would otherwise be read into memory whole. Below the longest
real line, real lines would lose attribution; above it, only the memory
per streamed line grows.

### D3. #225 first

`Journal::open` ends a torn last line with `\n` before it appends (#225,
merged before this ADR's first PR). So no binary that writes checked
lines can glue a record onto torn bytes, and three verdicts suffice: no
"glued" verdict, which would need a bounded search for a checked suffix,
with numbers of its own and a miss case. A downgrade to a binary from
before #225 can glue only an unchecked record onto torn bytes, and a line
that ends in an unchecked record is Unchecked (D2).

### D4. `repair-hot`: rule X7

For ADR-065 D2's states where the copies agree, a difference [d, e]
exists and hot reads in full. Hot's lines from s (the start of the line
holding d) through the line that holds e are read line by line. For each
hot line H that differs from C, X7 takes C's line that holds H's first
differing byte:

- **V**: H verifies. With C's line Intact → *fork*. With C's line Failed
  → *copy fails*. With C's bytes there Unchecked → *unattributed, hot
  verifies*.
- **F**: H is Failed and C holds an Intact line with the same start and
  end.
- **U**: everything else (H Unchecked, or H Failed without a same-span
  Intact C line, for example a flipped `\n`).

The decision comes ahead of ADR-065 D2's states, in this order:

1. Some differing hot line verifies: **copy-damaged** when every differing
   line is *copy fails* (stop: `mie store verify`, then `mie store
   repair`, whose R7 heals it under D5), otherwise **diverged** (the stop
   names the first verifying hot line and C's verdict there, and offers
   `--keep-hot-journal` as today).
2. Every differing line is F, and ADR-065 D3's fork guard finds no fork →
   new state **line-damaged**. `fork_shaped` runs here at any lengths: its
   reads stay below min(L_h, L_C), because e < min(L_h, L_C), so its
   precondition L = L_h = L_C is relaxed.
3. Otherwise → ADR-065 D2 and D3, unchanged.

In *line-damaged*, the write is C when L_C ≥ L_h (plus H3's `\n` when C
ends inside a line), and C followed by hot's bytes `[L_C, L_h)` when L_h >
L_C. That equals hot with bytes [d, e] taken from C, since the `\n`
positions agree there. One witness suffices, because each replaced line is
attributed by its own check, and hot's bytes past L_C are kept. Evidence
and the write order are ADR-065 D6 (`<journal>.damaged-<at_ms>`,
`replace_with_evidence`). *line-damaged* is not in `keeps_hot()`. Hot's
Failed lines past L_C (no copy holds them) are named and kept, and verify
FAILs on them until they are recorded (D6, D8).

The invariant behind the table: **an automatic decision never overwrites a
hot line that verifies.** It closes ADR-065 D3's blind spot (an
equal-length fork whose records end at C's `\n` positions) for checked
lines.

Worked geometry (k = 2): hot 1 000 B in five 200-B lines (starts 0, 200,
400, 600, 800), cold-a 1 000 B, cold-b 400 B, hot flipped at 450 → d = e =
450, L_2 = 400 ≤ e. ADR-065 decides *one-witness*; X7 decides
*line-damaged*, and hot becomes cold-a's 1 000 B. Hot 1 200 B with the
same copies → hot becomes cold-a[0, 1 000) followed by hot[1 000, 1 200);
a further flip at 1 050 is named (no copy holds it) and kept. Hot 800 B
under copies of 1 000 B each with a flip at 450 → hot becomes C, 1 000 B
(ADR-065: *diverged*).

### D5. ADR-065 D8 with line checks

One function decides for R7 and for `repair-hot`'s `r7_refuses` and
*behind* test, so the two commands never disagree. It walks the bytes p ≥
u where some witness contradicts hot (or holds a byte past hot's end):

- p ≥ L_h → refused, as D8.
- Hot's line holding p is Failed → refused ("hot's line fails its
  check"), even when hot holds a 2-of-3 majority. The one exception is a
  line a lost-line record matches (D8). This closes the case ADR-065 D9
  cannot see: a lagging copy that took hot's damage while the good copy
  was unmounted.
- A contradicting witness line (the witness's line that holds its first
  differing byte) verifies → refused. If hot's line also verifies, that is
  a fork. If hot's line carries no check, a verifying copy line is never
  overwritten by bytes that do not verify.
- Hot's line Intact and every contradicting witness line Failed → settled
  for hot, with no count.
- Otherwise → ADR-065 D8's majority over this line, unchanged. That
  covers every case where hot's line or a witness line is Unchecked, so a
  journal with no checked lines gets exactly D8.
- Then the walk continues after hot's line.

ADR-065 D9's side effect (at k = 2 a damaged copy holds a healthy lagging
copy back until `--keep-hot-journal`) is resolved through R7 when the
damaged copy's line fails its check. The pass guard itself is unchanged.

Threshold arithmetic at k = 2, hot 1 000 B flipped or contradicted at p =
450, the strict majority of ADR-065 D8 (2·with_hot > holding):

- Unchecked line, hot and cold-b (1 000 B, equal to hot) against cold-a:
  with_hot 2 of 3 holding, 2·2 = 4 > 3 → hot wins, as today.
- Unchecked line, cold-b only 400 B (shorter than p): hot against cold-a
  alone, 2·1 = 2 ≤ 2 → refused, as today.
- Checked line, the same 400-B geometry: hot Intact and cold-a Failed →
  settled for hot, no count.
- Checked line, hot Failed, cold-a holding hot's bytes and cold-b
  contradicting: with_hot 2 of 3, a majority, and still refused.

### D6. `verify`

- **V4** (attribution between hot and a copy): the existing `prefix`
  finding (its FAIL and its recorded finding) gains a clause from the two
  lines' verdicts; the copy is read before hot, as V2 orders. Copy damage
  → `mie store repair`. Hot's damage → `mie store repair-hot --scope
  journal` with `mie ingest` stopped. Both verify → a fork. Hot's line
  recorded lost (D8) → `mie store repair` makes the copy hot's bytes.
- **V3** (hot alone) streams hot's journal. A last line without `\n` is
  not classified. Every complete Failed line is either a **FAIL** when no
  lost-line record matches it ("hot's journal line n at byte o fails its
  check (ADR-066) and is not recorded lost: when a copy holds it intact,
  `mie store repair-hot --scope journal` (with `mie ingest` stopped)
  restores it; otherwise, after checking for a shared cause, `mie store
  repair --record-lost-lines` records its loss"), or a **listed note**
  when a record matches it ("recorded lost at <at_ms>"). A record that
  matches no line of hot's journal is a note ("stale: its bytes
  changed"). A summary line counts the lines checked, intact, without the
  check, failed and recorded.
- Nothing V3 finds is appended to the ledger (V7: findings on hot are not
  recorded). So every unrecorded failed line FAILs again on every run, and
  a recorded one never turns verify red again.

### D7. Readers

`read_runs`, `warm_handoffs`, `capture-report` and the rebuild's B1
ignore the member: they index named keys, so their code does not change,
and replay and equivalence outputs are byte-identical with and without
the member (#224 AC 8). There is no gate in `read_runs` on a Failed line
(STOP 1 decision 2): verify reports the line, and a fail-closed reader is
a follow-up whose rule for an unrestorable line is D8's record.

### D8. The lost-line record

A failed hot line that no copy can restore follows the store's lost-file
pattern (ADR-062 D11, `mie store repair --record-lost`, `Kind::Lost`;
STOP 1 decision 3).

- **Grammar.** A new copy-ledger kind, `lost-line <at_ms> <journal>
  <line> <offset> <sha256> <chain>`:
  - `journal`: the file name of `paths.journal`, escaped as manifest text
    is (`%`, 0x00–0x20 and 0x7F as `%XX`), so any UTF-8 name is one token;
    a name that is not UTF-8 is refused.
  - `line`: the 1-based number of `\n`-split lines.
  - `offset`: the line's first byte. The parser refuses `line` 1 with
    `offset` ≠ 0, and `offset` 0 with `line` ≠ 1.
  - `sha256`: the SHA-256 of the line's bytes without its `\n`, computed
    by `copy_ledger::line_sha256`, so `mie-cli` takes no `sha2` edge.
  - Why these fields: `offset` and `sha256` are the match key; `line` is
    what an operator reads; `journal` ties the record to one journal,
    since several configurations may share a store.
  - Why SHA-256 and not the line's CRC: the record names the exact bytes
    the operator accepted, and every digest in the ledger is already
    SHA-256 (`manifest`, `ledger-copy`, `lost`), which the grammar checks.
  - Why the copy ledger and not a sidecar: the ledger is hash-chained,
    replicated to every copy target, read by verify, and carried through a
    hot restore. That is why the lost-file record lives there.
  - The grammar change follows the ledger module's own rule: a new kind
    comes with an ADR edit and a golden test.
- **Fold.** `CopyState::fold` collects the records per journal name. No
  file list or path verdict changes: ADR-062 D7's select fold adds
  `lost-line` to the kinds that change nothing, beside `finding` and
  `verify`. A record matches hot's line when that line starts at the
  recorded offset and its bytes have the recorded digest. The line number
  does not enter the match: an earlier `\n` flip shifts the numbers, not
  the bytes the operator accepted. When the two numbers differ, the note
  shows both.
- **Writer: `mie store repair --record-lost-lines`.** Why `repair` and not
  `repair-hot`: the record writes no journal byte, so `mie ingest` need not
  stop, while `repair-hot --scope journal` takes the writer and journal
  locks, and every minute of a stopped capture is lost live data. The
  repair already holds the verify and pass locks, and `--record-lost` is
  its flag for the same act on a sealed file. Why a flag of its own: a lost
  file fails every selection that covers it, while a lost line changes no
  selection, so each is a separate consent. With the flag, the repair
  streams hot's journal (one read of L) and records every recordable line;
  without it, it names those lines and appends nothing. It is idempotent:
  a line a record already matches gets no second record.
- **Recordable**, all four required, otherwise refused with nothing
  appended:
  1. The line is complete and Failed, and no record matches it.
  2. Every configured copy target presents its marker, re-read at the
     decision as R9 does for a lost file; otherwise "not recordable in
     this run".
  3. Its journal copy reads; otherwise "not recordable in this run".
  4. Each copy's bytes over the line's span are absent, equal to hot's, or
     a complete Failed line. A copy that holds the line Intact, or other
     bytes that do not fail their check (Unchecked, or a cut line that
     differs), refuses and names `mie store repair-hot --scope journal`.

  Why so strict: once recorded, D5 lets R7 write hot's accepted bytes over
  the copies the recording judged, so the recording must rule out every
  copy that could still hold the true line. Worked example: hot 1 200 B in
  200-B lines, its failed line 6 at bytes [1 000, 1 199) with its `\n` at
  1 199. A copy of 1 000 B (nothing of the line), a copy of 1 100 B equal
  to hot, and a copy of 1 200 B with another complete Failed line are
  recordable; a copy of 1 100 B with other bytes in [1 000, 1 100), and a
  copy of 1 200 B with the line Intact (naming repair-hot), refuse.
- **When the bytes change later.** Damaged further: the digest no longer
  matches, so the line FAILs again; recording it again appends a new
  record, and the old one becomes a stale note. Restored (repair-hot
  replaced it and it verifies), or the journal replaced whole (ADR-065
  *damaged*, `--keep-hot-journal` on a copy, the restore's H3): the record
  matches nothing and is a stale note. Records are never removed (the
  ledger is append-only).
- **D5 with a record.** Hot's line is Failed but recorded: settled for hot
  when every contradicting witness line is Failed; a verifying witness
  line still refuses; a stale record settles nothing. So R7 brings a
  differently damaged copy to hot's accepted bytes, and V4's FAIL clears.
  `repair-hot --scope journal` reads the same records for the shared
  function, so the two commands agree; when it cannot read the ledger it
  uses no records, the stricter outcome, and says so.
- **Rebuild and restore.** B2 carries a `lost-line` record from its start,
  like every start record. The B-derivation never creates one: it does not
  read the copies' journals, and the record is an operator's acceptance,
  not a derivable fact (as `lost` needs `--record-lost`). A record that sat
  in hot's ledger tail, which no ledger copy held, is gone after a
  rebuild: the line FAILs again and is recorded again. The hot restore
  (#195) derives the same way; the restored journal is a copy, so a
  recorded line keeps its offset and bytes, and its record still matches.
- **Downgrade.** The strict ledger parser of a binary from before the
  third PR refuses a ledger that holds a `lost-line` record (`Corrupt`).
  After the first record, such a binary fails every tiered `select`,
  verify and repair on this store. That is the ledger's policy for every
  added kind, and no record exists until the operator runs the flag.

## Consequences

- **Throughput**, measured once (2026-11-16) for this ADR: the code of
  `journal.rs` (`crc32c`, `with_check`, `check_line`) copied unchanged
  into a benchmark compiled at opt-level 3 (cargo's release profile,
  rustc 1.98.0), three runs on the development host (a 15 W laptop CPU,
  load average 5.8 from other work). CRC-32C over 1 GiB: 199–257 MB/s.
  On a 343-byte line (the largest mean of D1), 2 000 000 times:
  `with_check` 1.3–1.9 µs a line, `check_line` 1.2–1.4 µs a line, 241–295
  MB/s. The CRC is table-driven, one byte per step. Writer: at most 16 701
  lines a day (D1) × 1.9 µs = 32 ms of CPU a day, negligible beside the
  capture. Scans: a scan that classifies every line (D6's V3, D8's flag)
  runs at 241–295 MB/s of CRC, above the 150 MB/s disk rate ADR-062 D11
  step 4 assumes (not measured), so the disk stays the bound. What breaks
  at another value: on a disk faster than about 240 MB/s the CRC becomes
  the bound of those scans (after a year, 5.5 GB takes about 23 s at 240
  MB/s instead of 37 s at 150 MB/s); a slicing-by-8 table would lift it
  (several-fold in the literature, not measured here) with no format
  change, since the CRC's value is the same.
- **Size**: +20 B per line, +5.8–10.4 % of the journal, +2.0 % of the
  busiest J (D1).
- **Permanent formats.** The line member (first PR) and the `lost-line`
  kind (third PR) are permanent once on disk. Each lands alone in its own
  PR, stated normatively here and pinned by golden tests
  (`a_written_line_has_its_golden_bytes`, and the record's golden text in
  the third PR).
- **Fixture churn.** Every journal `ingest` writes gains 20 B per line, so
  tests with byte offsets or journal sizes may shift; in the first PR no
  existing test needed a change. ADR-065's offline tests, which pin the
  byte rules on journals `ingest` wrote, move to journals stripped of the
  check in the second PR, so they keep pinning the fallback.
- **ADR-065 D3's reason for its torn-record shapes** ("`Journal::write`
  writes the object and its `\n` separately") no longer describes the
  writer, which now hands the line and its `\n` to the `BufWriter` in one
  `write_all`. The shapes still occur: the `BufWriter` flushes at buffer
  boundaries, not at line ends (#225), and old journals keep them.
- **Cost of the checks.** `verify`'s V3 and the flag's scan each add one
  read of hot's journal: after a year L = 1.9–5.5 GB (ADR-062 M1-11 ×
  365), about 13–37 s at the assumed 150 MB/s, with the capture running,
  on top of the 4·L that verify already reads at k = 2. D5 scans like
  ADR-065 D8 ((k+1)·(L−u), 3·L at k = 2), plus one line per
  contradiction; over unchecked contradicted lines it runs D8's majority
  again, so the worst case is 6·L. `repair-hot`'s journal scope adds one
  read of the copy ledger, about 1.65 MB per day of capture (ADR-062).
- **Residuals.** A check that misses (2^-32 per damaged line) ends in a
  stop in single-fault cases. A whole line deleted or reordered cleanly is
  not caught by its own check (the alternative a chain would catch); it
  still stops as a fork, since the lines on both sides verify. Attribution
  where the copies differ from each other goes no further than D5: hot
  Failed with one copy holding hot's damage and the other Intact stays
  ADR-065's *two-fault*, now with the line verdicts in its stop text.
  Lines written before this ADR are never protected.

## Alternatives considered

- **A hash chain across lines.** Each line would carry a hash of its
  predecessor's. It also catches a line deleted or reordered cleanly, but
  that is a fault storage damage does not produce, and a cleanly deleted
  line still stops here as a fork. It lost because it puts a read of hot's
  last line on the writer's open path, where #225's torn line sits, and
  because one damaged line would fail its successor too, so a single flip
  would look like two damaged lines.
- **A per-run digest in `run_end`.** It identifies a damaged run, not a
  line, and a crashed run (no `run_end`) has none; hot's newest lines
  (state 2) would stay unverified until the run ends.
- **A sidecar checksum file.** A second file to write, sync, replicate and
  keep in step with the journal; a crash between the two writes is a
  mismatch with no attribution. The member keeps the check inside the
  line it checks.
- **A 64-bit hash** (SHA-256 truncated; `sha2` is in the lock but would be
  a new edge for `mie-cli`). 8 more bytes per line (+2.3–4.1 % at the
  measured means) and no decision changes, since an escape already ends
  in a stop (D1).
- **The check as a non-JSON prefix of the line** (`<hex> {…}`). Every
  reader would have to strip it, and an old binary would skip every new
  line as no JSON. The member keeps every line a JSON object that old and
  new readers read alike.
- **A "glued" verdict without #225**, searching a line for a checked
  suffix: a bounded candidate search with numbers of its own and a miss
  case. #225 makes the case impossible for checked lines (D3).
- **A lost-line sidecar** instead of a ledger kind: no hash chain, no
  replication, no restore path; the copy ledger already has all three
  (D8).
- **A lost-line flag on `repair-hot`**: it would stop the capture for an
  act that writes no journal byte (D8).

## Accept when

The third PR merges with these offline tests green in its CI (the
names of the #224 plan), and the first two PRs' tests stay green:

- D1, D2 (first PR): the CRC vectors, the golden line bytes, every
  written line verifying, the verdict edge cases (minimum shape, the cap,
  every torn cut, every single-bit flip of a 300-byte line, damage in the
  member, look-alikes, 256 values at each shape byte and 1 000 random
  bodies, Unicode), `ingest_journal_lines_all_carry_the_check`.
- D7 (first PR): `mixed_journal_reads_as_without_the_check`,
  `replay_is_identical_without_the_check`,
  `equivalence_is_identical_without_the_check` (#224 AC 8).
- D4, D5, D6 V4 (second PR):
  `ac224_a_damaged_checked_line_in_hot_is_repaired_from_one_copy` (AC 5),
  `ac224_a_fork_is_detected_from_the_checks_and_nothing_is_written`
  (AC 6), `ac224_the_217_rules_still_decide_old_lines`, the `x7_*` and
  `d5_*` tests, the differential test of D5 against ADR-065 D8 on
  unchecked journals, and `v4_names_the_damaged_side`.
- D6 V3, D8 (third PR): `v3_hot_tail_lines_verify_without_a_copy` (AC 7),
  the `v3_*` record tests, the `record_lost_lines_*` tests, the D5 record
  tests, and the rebuild's `rebuild_carries_lost_line_records_from_its_start`
  and `rebuild_derives_no_lost_line_record`.
