# ADR-065: Repair of hot's journal and import ledger on an intact hot

- Status: accepted
- Date: 2026-11-15

Relation to earlier ADRs: this ADR decides the three pointers ADR-062 D11
leaves to #217 (the restore's H0, resolve's RA5 and the repair's R7). It
supersedes two parts of ADR-062 D11, and ADR-062's status line records
both: R7's reason "preferring the copies over hot's live journal is the
guess step 5 rejects", for the one state where two copies contradict hot
over its whole length (D2), and R7's agreement condition, where the
agreeing copy contradicts hot past the first differing byte (D8).
Everything else in ADR-062 stands. CLAUDE.md invariant 4 is untouched: no
sealed data file is moved or written. ADR-034 D4 (an import-ledger file is
immutable) holds in the sense it protects: hot's entry gets back the bytes
the copy targets hold, byte for byte, and the damaged bytes are kept. The
decisions record the #217 plan
(<https://github.com/Replikanti/mie/issues/217#issuecomment-6882706224>)
and its STOP 1 operator decisions (2026-11-14/15).

## Context

ADR-062 D11 gives the tiered store three repair paths and leaves a gap
between them. `mie store repair` replaces damaged copies on the copy
targets from hot and never rewrites hot. `mie store restore` rebuilds a
lost hot and refuses a hot that carries its marker. Neither acts when hot
is intact but one of hot's own files is damaged: the journal (the
capture's run record, which every live replay reads, ADR-039 D1) or an
import-ledger file (the exactly-once record of an archive import, ADR-034
D4). #195 STOP 1 decision 2 deferred that case to #217, and three texts
point there: the restore's H0 refusal, R7's "hot's journal is the
suspect", and RA5's refusal of a hot file that does not read.

Four states are dead ends without this ADR:

1. **R7's suspect state.** Every copy agrees with the others against hot
   at the same byte. R7 refuses, since one copy against hot cannot be
   attributed, and nothing else acts.
2. **A fork.** Hot's journal is fsynced only after `run_start` and
   `warm_start` and at the end of a run (`journal.rs` module docs), and a
   replication pass copies flushed bytes. So a power loss can drop lines
   on hot that the copies already hold, and the next `mie ingest` appends
   after the shorter file. Each side then holds bytes the other lacks,
   and every pass refuses the tail, since `extend_prefix` compares the
   copy's last line with hot. This is reasoning from the code; no host run
   reproduced a fork.
3. **R8's refusal of hot's entry.** When hot's entry lists a file with
   another SHA-256 than the copy ledger, R8 named an entry conflict for
   `mie store resolve`; resolve acts only on open holds, and on an intact
   hot nothing derives one.
4. **R7 can copy hot's damage into a copy** (read from the code, not
   run). R7 rebuilds a copy damaged at d when another copy equals hot
   over `[0, d]`, and the closing pass then extends the rebuilt copy from
   hot up to hot's end. With hot damaged at x > d and this copy damaged at
   d (two faults), the rebuilt copy takes hot's byte at x; at the next
   repair it agrees with hot at x, so the last good copy is rebuilt the
   same way.

## Decision

### D1. The command, its scopes, refusals and locks

`mie store repair-hot --config <ingest config> [--archive-config <path>]
--scope journal|import-ledger [--keep-hot-journal]`, one scope per run. It
is built like the restore: refusals and whole-run locks, then every read,
then a pure decision, then the writes. It is all or nothing: a difference
it cannot attribute stops the run before the first write, naming both
sides. It never deletes.

Refused, nothing written: a missing or unknown `--scope`;
`--keep-hot-journal` with `--scope import-ledger`; an empty `cold_roots`;
a hot without its marker or with a cold root's (`check_hot`, which names
`mie store restore`); a cold root with hot's id, one id on two roots, hot
and the copy targets not on pairwise distinct devices; journal scope: a
`paths.journal` without a file name, or that is a dangling link; the
import-ledger scope: no archive configuration or another store's, a
missing hot copy ledger (it names `mie store rebuild`, since the entry
test reads the ledger's manifest records), an import-ledger path that is a
dangling link. A copy target without a readable marker is skipped and
named, never read or written (R0's rule): an unmounted mount point reads
as an empty directory. Read-only roots are never witnesses: they are not
replication targets, so their bytes were never read from this hot.

Locks, each taken without waiting and held for the whole run, so a
concurrent writer refuses the run instead of racing it:

- the pass lock: no replication pass extends a copy while it is read, and
  it excludes `mie store adopt`, the journal's only other writer (`adopt.rs`
  step 5);
- the verify lock: no verification reads a copy this run rebuilds;
- journal scope: the writer lock of `instrument.source`, which excludes
  `mie ingest`: replacing the journal under a live `BufWriter` would lose
  its next lines;
- import-ledger scope: the archive source's writer lock, which excludes the
  importer, `mie store resolve` and the hot removal of archive partitions.

The import-ledger scope leaves the capture running: no live writer touches
the import ledger, and every minute of a stopped capture is irreplaceable
live data (ADR-062 D11 step 5). The copy-ledger hold is not taken: nothing
here appends to the copy ledger.

Why a command of its own: rejected as flags on `mie store repair`, since
the repair runs beside a live capture (R0 takes only the verify and pass
locks) and the journal scope needs `mie ingest` stopped; folded in, every
repair would need a stopped capture, or the hot step would depend on a
lock it may not get mid-run. The repair also works item by item, while
this needs all or nothing. Rejected as a mode of `mie store restore`: its
H5 to H7 (the copy-ledger derivation, the copy-back, the marker-last
commit) mean nothing on an intact hot.

### D2. The journal rule

The witnesses are the marked copy targets' `.mie/journal/<file name of
paths.journal>`. C is the longest witness that reads in full,
configuration order on a tie, and every other readable witness must be
its byte prefix (the restore's `choose_journal`, with unreadable
candidates left out); an unreadable witness is no witness and is named.
Hot's journal is `paths.journal` canonicalized, so a link is followed and
its target replaced. d and e are the first and the last byte where hot
differs from C over min(L_h, L_C); a read error on hot's journal counts as
a difference at every byte. L_2 is the second-longest witness's length;
lls(C) is the start of C's last complete line, the region a pass compares.

- **agree:** no difference and L_C ≤ L_h. Nothing is written; the next pass
  extends the copies.
- **behind:** no difference and L_h < L_C. Hot is extended to C. One
  witness suffices, as in the restore's H3: a copy's bytes were read from
  this hot, and hot's absence contradicts none of them.
- **damaged:** a difference, L_C = L_h, e < L_2, and no fork shape (D3).
  Hot's journal is replaced by C. Every byte where hot differs then has two
  independent witnesses that agree against it.
- Stops, nothing written, both sides named (hot's path, length, d, the line
  start s and hot's byte at d; each witness's root, path, length and byte
  at d):
  - *copy-damaged:* the copies differ, and some copy A is consistent with
    hot, every copy that contradicts A first differs from it below L_h,
    and every copy consistent with A is consistent with hot. One fault, on
    the copies: `mie store verify`, then `mie store repair`, whose R7
    rebuilds each differing copy with A as the agreeing copy (which D8
    allows, since A contradicts hot nowhere), then this command again. C
    is tried as A first; trying every copy makes the result independent of
    the configuration order on a tie.
  - *two-fault:* the copies differ otherwise. It names every witness's
    first difference against C and against hot, and `--keep-hot-journal`
    (D4), never `mie store repair` first, since R7 compares against hot.
    With k ≥ 3 copy targets configured it also names a run with a
    `--config` that lists only copy targets whose journals are byte
    prefixes of one another, at least `MIN_COPY_TARGETS` = 2 of them, then
    `mie store verify` and `mie store repair` with the full configuration,
    which rebuild the left-out copy from the repaired hot.
  - *hot-ahead:* L_C < L_h and e < lls(C). The next pass can extend the
    copies, since it compares only their last line: stop `mie ingest`, run
    `mie store replicate` (the standalone pass writes no journal line, so
    it brings the copies to L_h), run this again.
  - *diverged:* L_C < L_h with e ≥ lls(C) (the pass refuses the tail), or
    L_h < L_C with a difference, or a fork shape at equal length. Each side
    holds bytes the other lacks. It names `--keep-hot-journal`.
  - *one-witness:* L_C = L_h and e ≥ L_2: one copy against hot cannot be
    attributed, since the journal has no hash chain (ADR-062 D11 R7).
- A written journal that ends inside a line gets one `\n`, by H3's rule:
  the next `run_start` must parse as a line of its own. Every copy stays a
  byte prefix.

Why two witnesses, at hot's full length. The operator's k = 2 copies on
independent devices (ADR-062 D11) make hot and the copies three sources,
and two that agree byte for byte against the third outweigh it: the
majority R7 already applies to the copies, applied to hot. With one
witness, one damaged or stale copy (a target restored from an old backup)
would overwrite the only holder of hot's newest lines: the guess D11 step
5 rejects, and the guess R7's reason names; that reason stands for every
state except *damaged*. With three witnesses the rule could never fire at
the operator's k = 2. Why L_C = L_h: bytes hot holds past every copy have
hot as their only witness. Replacing hot by C would drop them, and keeping
them (C, then hot's tail) means guessing where hot's own bytes start,
which a fork defeats (Alternatives).

Worked examples (k = 2), each a test: hot 1 000 bytes and copies 1 000
and 1 000 with d = e = 10 is *damaged*; against copies 1 000 and 500, e =
499 is *damaged* and e = 500 *one-witness*; hot 400 under copies 1 000
and 500 is *behind*; hot 1 200 against copies 1 000 with lls(C) = 900 is
*hot-ahead* for e = 899 and *diverged* for e = 900; hot 800 under copies
1 000 with d = 700 is *diverged*.

### D3. The fork guard

A fork of equal length would read as damage under D2, and the operator
asked for a guard that errs toward stopping (STOP 1 decision 3). Before
*damaged* writes, `fork_shaped` reads hot's lines from s, the start of the
line holding d (which hot and C share, since they agree before d),
through the line holding e. Lines split at `\n` only, as `read_runs`
splits them; a `\r` before it is JSON whitespace.

- A **record** is a complete line that parses, without its `\n`, as a JSON
  object whose `type` and `run_id` are strings and whose `at_ms` is an
  integer: the envelope `Journal::write` gives every line. Not the line
  type table, since a type added later must not turn a fork into damage.
  Not `read_runs`' field checks: they are stricter, so they would err
  toward replacing, and a fork's lines come from the same writer.
- A **torn record** is a last line without its `\n` that starts with `{`
  and whose parse fails at the end of the input (`serde_json` `Eof`), or a
  whole record whose `\n` is missing: the two shapes a crash leaves, since
  `Journal::write` writes the object and its `\n` separately. Zero bytes
  fail with a syntax error instead.
- The first line is **glued** when it is no record but its suffix from
  some p with s < p ≤ d is one (or a torn record, when it is also the
  last line): the shape the next run leaves after a torn line, since
  `Journal::open` appends without a separator.
- **Fork-shaped** ⇔ (1) every line is a record, except that the first may
  be glued and the last torn; and (2) the `\n` positions of hot and C over
  `[d, e]` differ, or the first line is glued through a complete record
  whose `run_id` differs from that of C's line at s, itself a complete
  record. A read error counts as a fork.

Why `run_id`: a torn line means its run died, and the next line comes
from a new `mie ingest` start with a new run id (`read_runs` refuses two
`run_start`s of one id). A substitution that makes a glued shape, a nested
object's closing `}` turned into a blank so that the root's members end up
inside it, keeps C's `run_id`. Why damage stays *damaged*: a substitution
that keeps every `\n` position, even one that leaves the JSON valid (a
digit flip), fails (2); zero bytes, other unparseable bytes and a flipped
`\n` fail (1), since a flipped `\n` merges two lines into one that does
not parse or splits one into two that do not, and p ≤ d keeps the merged
line's second half out of the glued test. The guard does not apply to an
unreadable hot: there are no bytes to classify.

What it cannot see: an equal-length fork whose new lines are complete
records ending at exactly C's `\n` positions over `[d, e]`, with no torn
line glued in front. That needs the new run's first line, a `run_start`,
to end at the same byte as the copies' line there, and every following
line to do the same. Then *damaged* applies, and hot's bytes stay in
`<journal>.damaged-<at_ms>`, named in the report, never deleted.

### D4. `--keep-hot-journal`

It acts only in *diverged*, *one-witness*, *copy-damaged* and *two-fault*,
and only when hot's journal reads in full; otherwise it is refused with
nothing written. It never overrides *agree*, *behind*, *damaged* or
*hot-ahead*: there, the evidence decides, or a pass will. Per witness that
is no prefix of hot's journal, in configuration order, the marker is
re-read, then R7's rebuild runs: the copy is moved to
`<target>/.mie/damaged/<at_ms>/.mie/journal/<name>` and rebuilt from hot
up to the start of the line holding its first difference; the next pass
extends it. Hot is not written. Why a flag (STOP 1 decision 4): as with
`--skip-unreadable-journal`, a permanent state must not leave the operator
without a mie command, and no journal line leaves the journal unless a
human asks. The copies' bytes stay in `.mie/damaged/`.

### D5. The import-ledger test

Per import-ledger name, over hot's copy and every copy target's (an
unreadable file is a damaged copy, never a source), with the generations
taken from the union of the names (G4), a copy is *good* when:

- a current entry parses, binds and lists every file with the SHA-256 of
  the copy ledger's `manifest` record, else hot's sealed manifest. That is
  R8's test, factored into one function both commands call
  (`entry_mismatches`), so they never disagree about which bytes are the
  entry;
- a superseded entry parses, binds, and a supersede record of the same
  archive file names its SHA-256 for its name;
- a supersede record parses and names its own file (G3).

Hot's good copy is kept, and a differing target copy is named for R8
(verify, then repair). Otherwise, when the good target copies form
exactly one byte variant, that variant is written to hot: created for an
absent name, replaced (D6) for a present one. No good copy, or two good
variants, stops the run naming every copy with its state. Why R8's test
and not the restore's root manifests: on an intact hot the copy ledger is
present and verified, and R8 already decides the reverse direction (target
copies from hot) by it. Why no majority vote: R8 keeps hot's entry
whenever it passes the test, so a majority rule would let the two
commands disagree.

A malformed `.import` name on hot (no leading `.`, ending in `.import`,
failing `LedgerName::parse`) is moved to the evidence directory: on one,
the readers fail the whole archive file closed. Every other malformed name
and `PENDING` are named and left in place, since both readers skip them.

With no good copy anywhere, the stop names the closure by hand (the plan's
residual 3; read from the code, and pinned end to end for a current
entry):

- a current entry (generation 0, or above a re-admit's record, `quarantine
  -`): with the importer stopped, move the name aside by hand on hot (to
  the evidence name of D6) and on every copy target (to R8's
  `.mie/damaged/<at_ms>/.mie/import-ledger/<segment>/<name>`); then `mie
  store rebuild --replace-hot-ledger --record-unlisted` (the parts become
  `unlisted` holds); then `mie store resolve --file <archive path>`, which
  writes the entry from the held parts after its row check, byte-identical
  to the importer's (RA5 (a)). When `PENDING` names the archive file, the
  importer's resume writes the entry after the moves instead, and resolve
  refuses (RA4);
- a superseded entry: the moves alone, since no reader uses one whose
  bytes differ from what its record names;
- a supersede record, or an entry after a replace's record: no move, since
  no mie command recreates them (moving a record aside can change the
  current entry and drops the quarantine's moved set; RA5 (b) resumes the
  replace instead of writing the entry). The stop names the operator's
  step: restore the file from a backup kept outside mie, or write it again
  from its fields (the format has a fixed key order and no wall-clock
  field, so a correct re-derivation reproduces the bytes); then this
  command checks it on hot, and verify and repair bring the targets in
  line. Until then every reader fails the archive file closed, so no
  selection reads a wrong generation.

### D6. Evidence and the write order

One clock read gives `at_ms`. Every decision is made and every evidence
name checked before the first write; an existing evidence name stops the
run, since evidence is never overwritten, and a re-run gets a new
`at_ms`. Evidence for hot's own files lies beside them:
`<paths.journal>.damaged-<at_ms>`, and `<import-ledger
dir>.damaged-<at_ms>/<segment>/<name>`. Not hot's `.mie/damaged/`: a hard
link needs the file's own filesystem, and neither path has to share
`raw_root`'s.

The replace (`replica::replace_with_evidence`): hard-link the file to its
evidence name (no replace); write the new bytes to `.<name>.replace.tmp`,
fsync and read them back; make an import-ledger file read-only; rename it
over the file and fsync the directory. A crash leaves the old file (maybe
with the link and a temp name, which the readers skip) or the new one,
never no file, and a re-run completes it; the report names an earlier
run's evidence link. Rejected, R7's move-then-rebuild for hot: a crash
after the move would leave hot without a journal, and the next `mie
ingest` would start a new one, the split journal ADR-062 rejects.

Order: the journal scope writes its one replace (or extension), or D4's
rebuilds. The import-ledger scope moves the malformed names first, then
writes the names in name order.

### D7. The messages

Every text that named #217 names this command. The restore's H0 refusal
names `mie store repair-hot --scope journal|import-ledger`; R7's suspect
state, `--scope journal` with `mie ingest` stopped; R8's refusal of hot's
entry first names `--scope import-ledger`, then the hold resolution;
resolve's RA5 remedy names `--scope import-ledger` for hot's own file; and
the problems the pass, the verification and the repair report on hot's
import ledger name it at their call sites (not inside the shared reader,
which the adoption also uses on another store).

### D8. R7's agreeing copy

In `mie store repair`, another copy counts as agreeing only when it is
longer than d, equals hot over `[0, d]`, and has no difference against
hot over min(its length, L_h). Why: the closing pass extends the rebuilt
copy from hot up to L_h, so R7 vouches for hot's bytes past d too, and a
copy that contradicts hot there cannot. A copy that agrees with hot over
`[0, d]` but differs later is named in the refusal with its first
differing byte, and the text names `mie store repair-hot --scope journal`.
Single-fault repairs are unchanged: there the other copies are byte
prefixes of hot.

## Consequences

- **Downtime of the journal scope.** *damaged* reads C into memory,
  compares the other witness and hot against it, then writes the new
  journal and reads it back: at k = 2, 4·L read and L written, 5·L in all.
  D3 reads hot's region once more, one line at a time, and stops at the
  first line that is neither a record nor allowed: a few lines for a local
  substitution, at most L when the difference spans the file, 6·L in the
  worst case. After one year L = 1.9–5.5 GB (ADR-062 M1-11 × 365). At an
  assumed 150 MB/s (not measured; the assumption of ADR-062 D11 step 4),
  5·L = 9.5–27.5 GB takes about 1.1–3.1 min of `mie ingest` downtime per
  year of journal age, and 6·L = 11.4–33.0 GB about 1.3–3.7 min. A slower
  disk scales these linearly; correctness does not depend on them.
- **Memory:** up to L_C, 5.5 GB at the busiest M1-11 rate after a year,
  the bound the restore and the rebuild already accept; D3 adds one line.
- **The import-ledger scope** reads about 2.25 MB (ADR-062 M1-15) and never
  stops the capture.
- **D8** adds at most one read of L per rebuilt journal copy, about 13–37 s
  after a year at the same assumed rate, with the capture running (the
  repair takes no writer lock). It refuses a repair R7 used to do only when
  the agreeing copy contradicts hot past d, which takes two faults.
- **A fork read as damage** (D3's blind spot) replaces hot; hot's bytes
  stay in `<journal>.damaged-<at_ms>`, named in the report.
- **Damage read as a fork** only stops: it needs every hot line of the
  region to be a complete JSON object with the envelope while a `\n` moved,
  or a glued record of another `run_id`, not a shape of bit flips, zero
  fills or torn writes.
- **Two-fault residuals, each closed:** (1) k ≥ 3 with two copies agreeing
  against hot and a third damaged differently is unreachable at the
  operator's k = 2 (it needs three copy targets); if k is raised, the
  *two-fault* stop names the reduced configuration, and D8 keeps `mie store
  repair` from copying hot's damage meanwhile. (2) Damage inside the
  copies' last line with hot longer is *diverged*, closed by
  `--keep-hot-journal`. (3) An import-ledger file bad everywhere: the
  closure by hand of D5.
- **Relation to #224.** The per-line integrity field #224 plans is the
  stronger remedy: a line that carries its own check identifies its damage
  without a vote. D2 and D3 remain the fallback for every line written
  before #224 lands. #217 plans no #224 work.

## Alternatives considered

- **A splice** (C, then hot's bytes after L_C). It would handle
  *hot-ahead* without a pass, but it cannot tell a fork, or damage that
  runs to the copies' end (a zero-filled tail after a power loss), from a
  substitution. A wrong splice cuts a `run_start` line, and `read_runs`
  then drops the run.
- **No fork guard** (decision 3 as first proposed). An equal-length fork
  would replace hot and move its newer lines out of the journal. The
  operator asked for the guard.
- **A majority vote for entries.** R8 keeps hot's entry whenever it passes
  the test, so a majority rule would let the two commands disagree.
- **Evidence in hot's `.mie/damaged/`.** A hard link needs the file's own
  filesystem (D6).
- **Deleting the old file.** Rejected for R5's reasons: the bytes are
  evidence of a shared cause, and a crash during an overwrite leaves
  neither version.
- **A line-level merge of a forked journal.** Out of scope: it decides
  which run's lines are live, which no byte comparison can, and #224's
  integrity field is the planned basis for it.

## Accept when

Every item is an offline test the PR's CI runs, and the command changes
no capture, removal or replication path, so the status is `accepted` at
merge:

- AC1: hot's journal in R7's suspect state is restored by `--scope
  journal`, byte-identical to both copies, the flipped file kept as
  `<journal>.damaged-<at_ms>`, and the next verify passes
  (`ac1_hot_journal_r7_suspect_is_restored_and_kept_aside`).
- AC2: hot's import-ledger file that does not parse, cannot be read, or
  lists a file with another SHA-256 is restored by `--scope import-ledger`
  and kept aside (`ac2_hot_import_ledger_file_is_restored_and_kept_aside`).
- AC3: a fork, of unequal and of equal length, and an entry with no good
  copy anywhere stop, naming both sides, with nothing written
  (`ac3_a_fork_stops_naming_both_sides_and_writes_nothing`,
  `ac3_an_entry_without_a_good_copy_stops_and_writes_nothing`).
- D8: `r7_a_copy_is_not_rebuilt_while_hot_is_contested_beyond_d`.

## Acceptance

Met by the PR that adds this ADR: the four offline tests above, the pure
tests of every D2, D3 and D5 outcome with the numbers of D2, and the run
tests of the refusals, the locks, a crash after each write step with its
re-run, the evidence names, the path forms and refused writes, all pass
in its CI.
