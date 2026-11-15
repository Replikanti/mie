# ADR-065: Repair of hot's journal and import ledger on an intact hot

- Status: accepted
- Date: 2026-11-15

Relation to earlier ADRs: this ADR decides the three pointers ADR-062 D11
leaves to #217 (the restore's H0, resolve's RA5 and the repair's R7). It
supersedes two parts of ADR-062 D11, and ADR-062's status line records
both: R7's reason "preferring the copies over hot's live journal is the
guess step 5 rejects", for the one state where two copies contradict hot
over its whole length (D2), and R7's agreement condition, which D8
replaces by hot's strict byte-wise majority over every byte the rebuild
writes.
Everything else in ADR-062 stands. CLAUDE.md invariant 4 is untouched: no
sealed data file is moved or written. ADR-034 D4 (an import-ledger file is
immutable) holds in the sense it protects: hot's entry gets back the bytes
the copy targets hold, byte for byte, and the damaged bytes are kept. The
decisions record the #217 plan
(<https://github.com/Replikanti/mie/issues/217#issuecomment-6882706224>)
and its STOP 1 operator decisions (2026-11-14/15); D1's journal lock, D2's
*behind* completion, D3's later glued lines and D8's majority rule come
from the adversarial review of #226 (round 1); D2's *behind-copy-damaged*,
D4's named drops and D9's pass guard from its round 2, approved by the
operator at STOP 2 (2026-11-15); D2's limits on *behind-copy-damaged* (no
byte past hot's end from a copy that differs below it, no fork) and D9's
order (the guard runs only for a copy the pass accepts) from its round 3.

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
- journal scope: the journal lock, `<journal>.lock` beside the file
  `paths.journal` names (a link followed). The source's lock alone leaves
  a gap: `mie ingest` releases it when its raw store closes, then writes
  `sealed`, `run_end` and, after its final pass, `replication`. Taken in
  that gap, a replace renames C over the journal while `run_end` goes into
  the old file, which only the evidence name still reaches, and the run
  replays as crashed (ADR-039 D2); an extension can interleave with a
  line. So `mie ingest` holds the journal lock from the journal's open
  until its last line (the journal value owns it; an abandoned final pass
  keeps it until the process exits), and this command takes it without
  waiting. Why a lock of its own and not the source's lock held longer:
  the source's lock guards the raw store's files, and the importer and
  the hot removal take other sources' locks; the journal has writers of
  its own (`mie ingest`, this command, the restore's H3), and a lock
  beside it follows a `paths.journal` outside `raw_root`. `mie store
  restore` takes it too: a capture that runs from before a hot loss keeps
  appending to the journal H3 extends, and its source lock is on the lost
  hot, not the restored one;
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
- **behind:** no difference and L_h < L_C, or L_h = L_C with C ending
  inside a line. Hot is extended to C. One witness suffices, as in the
  restore's H3: a copy's bytes were read from this hot, and hot's absence
  contradicts none of them. The second form is the state the extension
  leaves when its closing `\n` (below) did not land: the extension is
  appended in bounded chunks and the `\n` after it, so no single write
  holds both, and a crash or a failed append between them leaves hot
  equal to C. The re-run decides *behind* there, appends nothing and
  writes the `\n`. It cannot tell this from a cut line of a crashed `mie
  ingest` that a pass already copied, and need not: there too the `\n`
  makes the next `run_start` a line of its own, and `read_runs` skips the
  cut line either way. A hot that holds a cut line past every copy's end
  (L_h > L_C) stays *agree*: that is the capture's own torn line, #225.
- **behind-copy-damaged:** the copies differ, every copy reads, some copy
  is longer than hot, and hot holds a strict majority at every byte below
  L_h (D8's rule, `first_contested_file` bounded to `[0, L_h)`). Hot is
  extended to the end of the longest copy *consistent with hot* (no
  difference with hot below L_h), with one `\n` when it ends inside a
  line. One witness suffices past L_h, as in *behind*. The copies that
  differ from hot below L_h are copy damage, each named for `mie store
  verify` and `mie store repair`, whose R7 then rebuilds it from the
  extended hot. It does not apply, and the state is one of the stops
  below (*two-fault* wherever a copy is longer than hot, since R7 refuses
  that copy), with a clause naming why hot was not extended, when (1) a
  copy that differs from hot below L_h holds a byte past the end of every
  consistent copy; (2) two copies hold different bytes at a byte past L_h
  (`first_disagreement_file`), consistent or not; or (3) `fork_shaped`
  (D3) calls the region of a differing copy against hot a fork, read over
  `[d, e]` of that copy against hot. Why (review of #226 round 2, finding
  2): hot that lost its last line at a power loss after a pass copied it
  (the state *behind* exists for) plus one bit flip on a copy are two
  faults that the byte-wise majority attributes completely: below L_h,
  hot and the intact copy outvote the flipped one; past L_h, every copy
  agrees. Before, R7 refused both copies at L_h (a copy longer than hot
  gets d = L_h, so it never weighed the flip), this command decided
  *two-fault*, and the only remedy it named, `--keep-hot-journal`, would
  have dropped the 356 bytes both copies hold. Why the limits (review of
  #226 round 3, finding 1): "one witness suffices" holds for a copy read
  from this hot, and a copy that differs from hot below L_h may not be
  one. The reviewer's fork, at k = 2: hot loses its last line O (356
  bytes) at a power loss after a pass copied it to cold-b; the capture
  restarts and writes a 48-byte `run_start` N at the same offset while
  cold-b is unmounted, and the pass extends lagging cold-a with N (D9's
  *What it cannot see*). With cold-b back, hot and cold-a outvote cold-b
  at N, and only cold-b holds bytes past L_h. The round-2 form appended
  cold-b's bytes `[L_h, L_b)`, the last 308 bytes of O from mid-line, to
  hot, refused `--keep-hot-journal`, and verify and repair then spread
  the splice to cold-b, so O's `run_end` left every journal without an
  operator flag (the splice the Alternatives reject). Limit (1) stops it:
  no consistent copy is longer than hot. Limit (3) covers the same fork
  where a consistent copy does hold more: the restarted run's N is longer
  than O and a second line M reached cold-a before hot lost it too; then
  every byte past L_h is cold-a's, cold-b ends below L_h, and only the
  fork shape of cold-b's region (its `\n` ends O inside N) tells that the
  repair would drop O. Limit (2) replaces the round-2 partial extension
  to the line of the first disagreement: past L_h no journal outvotes
  another, so the operator decides. All three err toward stopping (STOP 1
  decision 3), and each stop offers `--keep-hot-journal` and names what
  it drops (STOP 1 decision 4, D4).
- **damaged:** a difference, L_C = L_h, e < L_2, and no fork shape (D3).
  Hot's journal is replaced by C. Every byte where hot differs then has two
  independent witnesses that agree against it.
- Stops, nothing written, both sides named (hot's path, length, d, the line
  start s and hot's byte at d; each witness's root, path, length and byte
  at d):
  - *copy-damaged:* the copies differ, and R7 rebuilds every copy that is
    no byte prefix of hot's journal under D8: the same rule
    (`first_contested_file`) over the same witnesses, every copy target's
    journal. The faults are on the copies: `mie store verify`, then `mie
    store repair`, which rebuilds each differing copy, then this command
    again. Sharing the rule keeps the two commands from sending the
    operator in a circle: a stop that names the repair names one that
    acts.
  - *two-fault:* the copies differ otherwise: R7 refuses at least one
    copy. It names every witness's
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
1 000 with d = 700 is *diverged*. With the copies differing: hot intact,
cold-a damaged at 110 and cold-b at 10 is *copy-damaged* (two of three
for hot at every byte); hot damaged at 110, cold-b at 10 and cold-a only
100 bytes long is *two-fault* (at 110 only hot and cold-b hold a byte).
Hot 800 under copies of 1 000 with cold-a damaged at 10 is
*behind-copy-damaged*, extended to 1 000; with cold-b cut at 900, the
bytes past it are damaged cold-a's alone, *two-fault* (limit 1); with
cold-b also damaged at 950 or 850, *two-fault* (limit 2); hot damaged at
300 as well is *two-fault* (both copies against hot at 300). Hot of 600
common bytes plus a 100-byte line of run B, against cold-a equal to hot
and cold-b holding a 300-byte line of run A there, is *two-fault* (limits
1 and 3); with hot's run-B line 300 bytes, a further 100-byte line on
cold-a, and cold-b holding a 100-byte run-A line, *two-fault* (limit 3
alone).

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
- A line is **glued** when it is no record but its suffix from some p is
  one (or a torn record, when it is the journal's last line): the shape
  the next run leaves after a torn line, since `Journal::open` appends
  without a separator. For the first line s < p ≤ d: its bytes before p
  are C's, the start of C's line at s. For every later line the bytes
  before p must be a torn record themselves, since they need not be C's:
  a run killed mid-line inside the region, with the next run glued on.
- **Fork-shaped** ⇔ (1) every line is a record or glued, except that the
  last may be torn; and (2) the `\n` positions of hot and C over `[d, e]`
  differ, or a line is glued through a complete record whose `run_id`
  differs from that of C's line at the same start, itself a complete
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

Why every line and not only the first (review of #226, finding 4): a fork
can hold a second torn line, when the run that followed the first torn
line died mid-line too. With the first-line rule, that line read as no
record and the guard answered "no fork", although its `\n` positions
differ; the guard errs toward stopping, so it must recognise the shape
wherever it lies. The torn-prefix condition keeps one damage shape out: a
`\n` turned into a byte that is neither JSON whitespace nor `\n` leaves
the merged line's first part with trailing characters, a syntax error, so
no glued line. A `\n` turned into a blank does read as glued (the blank is
trailing whitespace), together with another difference earlier in the
region: two faults read as a fork, which stops.

Before *behind-copy-damaged* (D2, limit 3) the same test runs over each
copy that differs from hot below L_h, with that copy as C and d, e its
first and last difference with hot over min(L_h, L_copy).

What it cannot see: an equal-length fork whose new lines are complete
records ending at exactly C's `\n` positions over `[d, e]`, with no torn
line glued in front of any of them. That needs the new run's first line,
a `run_start`, to end at the same byte as the copies' line there, and
every following line to do the same. Then *damaged* applies, and hot's
bytes stay in `<journal>.damaged-<at_ms>`, named in the report, never
deleted.

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

Every stop that offers the flag names what it drops (review of #226 round
2): per copy it rebuilds, the bytes from the start of hot's line that
holds the copy's first difference to the copy's end. It is never offered
where hot holds a strict majority at every byte it holds and the bytes
past its end come from copies consistent with it, with no disagreement
and no fork: that state is *behind-copy-damaged*, which appends them, and
the flag is refused there. Where hot's own bytes are contradicted or tied
(a fork, hot's damage, one copy against hot), or the bytes past hot's end
are unattributed (D2's limits), it stays the operator's explicit choice,
also when two copies agree on bytes it drops: in a fork both sides hold
live lines, and without the flag the state would have no mie command at
all.

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

### D8. R7's majority rule

In `mie store repair`, R7 rebuilds the journal copy on target i that
first differs from hot at d (d = L_h when the copy is longer than hot and
consistent with it): it moves the copy aside, writes hot's bytes up to u,
the start of hot's line holding d, and the closing pass extends it from
hot to L_h. So the repair overwrites copy i's bytes `[u, L_i)` and writes
hot's bytes `[u, L_h)` into it. The rule:

- The **witnesses** are hot, copy i and every other copy target's journal
  copy, each as it was before this repair wrote anything: a copy R7
  already rebuilt in this run counts by its `.mie/damaged/` evidence copy.
  A journal **holds a byte** at p when it is longer than p. An absent copy
  holds none; a copy that cannot be read refuses the rebuild, since it
  cannot be counted.
- Copy i is rebuilt only when, at every p ≥ u where some witness holds a
  byte that hot does not (another byte, or any byte at p ≥ L_h), more than
  half of the witnesses that hold a byte at p, hot included, hold hot's
  byte. Hot holds nothing at p ≥ L_h, so a copy byte past hot's end always
  refuses: hot is behind there, and the rebuild would drop the copy's
  bytes or the next pass would write later hot bytes over them.
- A byte only hot and one copy hold, where they differ, is one against
  one: refused. Bytes only hot holds are no contest: that is the pass's
  ordinary extension, as before.
- The refusal names the first contested byte and its count, or hot's
  journal as the suspect when every other copy holding byte d agrees with
  copy i against hot there, and `mie store repair-hot --scope journal`.

At the operator's k = 2 this means two of three at every contradicted
byte. Why each part. The operator's goal (review of #226): a repair must
never write hot's damage into a copy, and must not lose a repair that is
valid. The rule checks exactly the bytes the repair writes, so every byte
it changes in a copy is backed by more sources than contradict it. The
rule before #217 checked `[0, d]` only, and the closing pass then copied
hot's damage past d into the copy (Context 4). The first form of D8 (an
agreeing copy consistent with hot over min(its length, L_h)) was too weak
and too strict. Too weak: with cold-b damaged at 10, hot at x in a later
line and cold-a, the agreeing copy, shorter than x, cold-a vouched for
nothing at x, yet cold-b was rebuilt and took hot's byte x. Too strict:
with hot intact, cold-b damaged at 10 and cold-a at x, every byte has two
of three for hot, yet both copies were refused. Why the witnesses as they
were before the run: R7 works copy by copy, and a copy rebuilt first is
hot's prefix, which would count for hot at its own former damage. Why
hot must hold a strict majority and a tie refuses: one against one
cannot be attributed without a hash chain (ADR-062 D11 R7), and two
against two at k ≥ 3 is the same tie. Why bytes before u do not count:
copy i equals hot there, so the rebuild writes its own bytes back.

What it could not see without D9: a copy shorter than a contested byte is
a byte prefix of hot, so every pass extended it from hot, the repair's
closing pass included, since the pass compares only a copy's last line
(ADR-062 D11). In the too-weak case above, R7 refused cold-b, but the
closing pass extended cold-a over x with hot's damaged byte, and the next
`mie store repair` counted cold-a's copy of that byte as a second witness
for hot and rebuilt cold-b with it (probe runs of #226, review rounds 1
and 2; the same sequence on main before #217). Bytes a pass copied from
hot are no independent witness of hot, and no byte comparison can tell
them from bytes copied before the damage. D9 keeps the pass from writing
them over a contradiction.

### D9. The pass guard

Every pass extends the journal copies in one place, `Pass::copy_journal_tail`:
`mie store replicate`, the capture's replication thread, the repair's
closing pass and the adoption's pass. Before it extends copy i from its
length L_i to `end`, it compares hot's bytes `[L_i, min(L_j, end))` with
every other copy j of the pass's targets that is longer than copy i, each
at its length from before the pass extended any copy. At the first byte p
where some copy j holds another byte than hot, copy i is extended only to
the start of hot's line that holds p (never below L_i: a copy that ends
inside that line is only compared), and the pass reports a problem naming
p, hot's byte, copy j's root, path and byte, and the next step: `mie store
verify`, then `mie store repair`, whose R7 rebuilds copy j where hot holds
a strict majority at p; where R7 refuses, one against one, `mie store
repair-hot --scope journal` with `mie ingest` stopped, which stops naming
both sides. A copy j that cannot be read is named and not counted.

Why (review of #226 round 2, finding 1; operator decision at STOP 2): with
hot bit-rotted at x while cold-a lagged below x and cold-b held the true
byte, each step doing what the previous one named ended with hot's damage
in cold-b: repair-hot stopped *one-witness*, the repair's closing pass (or
any capture pass) extended cold-a over x, repair-hot then decided
*copy-damaged*, and the next repair rebuilt cold-b from hot. With the
guard, cold-a stays before x's line, the state stays one against one, R7
refuses cold-b and repair-hot stops *one-witness* again, and the only
command that writes hot's byte into cold-b is `--keep-hot-journal`, the
operator's explicit choice (D4), whose stop names the bytes it drops.
Rejected: a guard in R7 alone (the closing pass is not the only pass; the
capture's thread extends the copy just the same), and comparing every copy
over its whole length on every pass (L per copy and pass, for bytes a pass
never writes).

Why lengths from before the pass and only longer copies: a copy extended
earlier in the same pass holds hot's bytes, no witness, and comparing with
it would read the extension twice for nothing; bytes below L_i are not
written. Why the line start: the copy stays a byte prefix of hot that ends
at a line end, the shape a pass leaves, and the next pass compares its
last line.

Cost: at equal lengths, the steady state in which every pass extends all
copies from one length, nothing is compared and nothing is read (pinned by
a test counter). A lagging copy reads hot's and each longer copy's bytes
over their overlap, 2× the overlap per longer copy, on every pass while it
lags. The guard runs only for a copy the extension accepts (no longer
than the pass's end, its last line hot's: the checks `extend_prefix` makes
before it appends, run first and read only): a copy the pass refuses
anyway reads its last line and nothing for the guard,
however far it lags (pinned by the counter; review of #226 round 3,
finding 2). Before, such a copy paid 2× its gap on every pass until the
repair, for bytes never written. A new or replaced target (L_i = 0)
reads about 2·L once per longer copy, on the pass that copies its L bytes
anyway: 2·L at k = 2, (k−1)·2·L at k ≥ 3, since with no copy differing
each longer copy is compared in full. After a year at k = 2 that is
3.8–11 GB, about 25–73 s at the assumed 150 MB/s, with the capture
running; each further copy target adds as much.

Side effect: a damaged copy j holds a healthy lagging copy i back until j
is healed, reported on every pass. At k ≥ 3, R7 heals j when a third copy
covers p. At the operator's k = 2 only hot and j hold p, so R7 refuses and
repair-hot stops *one-witness*: the operator decides with
`--keep-hot-journal`, which rebuilds j from hot. The guard errs toward
stopping, as D3 does.

What it cannot see: a copy the pass does not compare, on a target that
lost its marker (skipped, R0), was stopped in the pass, or does not read.
A lagging copy extended while the only contradicting copy is unmounted
takes hot's byte, and once that mount returns, the next repair counts it.
Holding every copy back while a target is missing would stop replication
exactly when one disk is lost, the case k = 2 exists for.

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
- **Downtime where the copies differ.** Every stop or decision there first
  compares each witness with hot and with each other witness, then runs
  `first_contested_file` (hot and all k copies) once per witness that is no
  byte prefix of hot: about k·(k+1)·L reads, 6·L at k = 2, 11.4–33.0 GB
  after a year, about 1.3–3.7 min of `mie ingest` downtime at the assumed
  150 MB/s (review of #226 round 2). *behind-copy-damaged* adds one bounded
  `first_contested_file` below L_h ((k+1)·L_h), the disagreement scan past
  L_h, reads hot into memory (L_h), and per copy that differs from hot
  below L_h reads that copy into memory and compares it with hot (2·L)
  before D3 reads hot's region (at most L): about 13·L at k = 2 in all,
  25–72 GB, about 2.7–7.9 min per year of journal age.
- **Memory:** up to L_C, 5.5 GB at the busiest M1-11 rate after a year,
  the bound the restore and the rebuild already accept; D3 adds one line.
  *behind-copy-damaged* holds hot's journal and the consistent copy's
  bytes past it, up to L, and one differing copy at a time for D3, up to
  L more.
- **The import-ledger scope** reads about 2.25 MB (ADR-062 M1-15) and never
  stops the capture.
- **D8** reads hot and every copy over `[u, max length)` once per rebuilt
  journal copy: at k = 2 at most 3·L, 5.7–16.5 GB after a year, about
  38–110 s at the same assumed rate, with the capture running (the repair
  takes no writer lock). A copy whose damage lies near the end reads only
  the tail. It refuses a repair the rule before #217 did only where hot
  holds no strict majority at a byte the repair writes, which takes two
  faults, and it heals two damaged copies on an intact hot, which the
  first form of D8 refused.
- **A fork read as damage** (D3's blind spot) replaces hot; hot's bytes
  stay in `<journal>.damaged-<at_ms>`, named in the report.
- **Damage read as a fork** only stops: it needs every hot line of the
  region to be a complete JSON object with the envelope while a `\n` moved,
  or a glued record of another `run_id`, not a shape of bit flips, zero
  fills or torn writes. D3's later glued lines add one two-fault shape: a
  `\n` turned into a blank in a later line of a region that differs
  earlier too.
- **The journal lock** is one more file beside the journal,
  `<journal>.lock`, created by the first `mie ingest` after this change;
  nothing reads its bytes. A `mie ingest` refuses to start while this
  command or the restore holds it, as it refuses on the source's lock. It
  also refuses to start when the lock file cannot be created (a journal
  directory without write permission, though the journal file itself is
  writable) or is held by another `mie ingest` that shares `paths.journal`
  (another `instrument.source`, which `read_runs`' per-source filter
  allows). The second refusal is intended: two `BufWriter`s appending to
  one file can interleave a line longer than their buffer. A filesystem
  without `flock` support (some NFS setups) fails the same way, with the
  lock's error (reasoned from `flock(2)`, not run). Each refusal names the
  lock file and its error (review of #226 round 2, finding 3).
- **The pass guard (D9)** reports a lagging copy held back by a copy that
  contradicts hot on every pass until the repair or this command settles
  it; at equal lengths it reads nothing.
- **Two-fault residuals, each closed:** (1) k ≥ 3 with two copies agreeing
  against hot and a third damaged differently is unreachable at the
  operator's k = 2 (it needs three copy targets); if k is raised, the
  *two-fault* stop names the reduced configuration, and D8 with D9 keeps
  `mie store repair` from copying hot's damage meanwhile, short of D9's
  *What it cannot see*. (2) Damage inside the copies' last line with hot longer is *diverged*, closed by
  `--keep-hot-journal`. (3) An import-ledger file bad everywhere: the
  closure by hand of D5. (4) Hot behind (its last line lost) plus one copy
  fault: *behind-copy-damaged* extends hot from the copies, then `mie store
  verify` and `mie store repair` rebuild the damaged copy from it; all
  three journals end equal and true
  (`behind_hot_plus_one_copy_fault_extends_hot_and_the_repair_heals_the_copy`).
  (5) Hot damaged while a copy lags below the damage: D9 keeps the lagging
  copy from vouching for hot's byte, so the state stays *one-witness*,
  closed by `--keep-hot-journal` as the operator's explicit choice, whose
  stop names the bytes it drops
  (`guard_hot_damage_never_reaches_the_good_copy_through_a_lagging_copy`).
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

Every item is an offline test the PR's CI runs. The command changes no
removal path; the replication path changes only by D9's guard, which
writes nothing new (it stops an extension earlier) and reads nothing at
equal lengths; the capture changes by taking the journal lock at its
journal's open, a refusal the offline tests cover. So the status is
`accepted` at merge:

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
- D8: `r7_a_copy_is_not_rebuilt_while_hot_is_contested_beyond_d`,
  `r7_a_byte_only_hot_and_the_copy_hold_is_contested`,
  `r7_two_copy_faults_on_an_intact_hot_heal_both_copies`.
- D9: `the_guard_holds_a_lagging_copy_before_a_byte_another_copy_contradicts`,
  `the_guard_compares_nothing_at_equal_lengths`,
  `the_guard_reads_nothing_for_a_copy_the_extension_refuses`, and the sequence of
  review round 2 end to end
  (`guard_hot_damage_never_reaches_the_good_copy_through_a_lagging_copy`).
- D2 *behind-copy-damaged*:
  `x2_behind_with_a_copy_fault_takes_the_agreed_tail`,
  `x2_a_fork_is_never_behind_copy_damaged`,
  `behind_hot_plus_one_copy_fault_extends_hot_and_the_repair_heals_the_copy`,
  `behind_hot_is_not_extended_where_the_copies_disagree_past_its_end`,
  `behind_hot_takes_no_byte_only_a_differing_copy_holds`,
  `behind_hot_stops_where_a_shorter_copy_is_a_fork`,
  `behind_hot_outvoted_below_its_end_is_not_extended`, and the sequence of
  review round 3 end to end
  (`a_fork_through_an_unmounted_copy_stops_and_the_flag_keeps_hot`).

## Acceptance

Met by the PR that adds this ADR: the offline tests above, the pure
tests of every D2, D3 and D5 outcome with the numbers of D2, the D8 rule
in memory and over files, and the run tests of the refusals, the locks
(the journal lock against an open journal), a crash after each write step with its
re-run, the evidence names, the path forms and refused writes, all pass
in its CI.
