# ADR-067: A self-consistent other file on a copy target

- Status: accepted
- Date: 2026-11-17

Relation to earlier ADRs: this ADR adds a second exception to ADR-062 D5's
"a sealed file is never removed from the union of the roots", beside the
*replace* resolution's quarantine (ADR-062 D11). It supersedes three
statements of ADR-062: R1's and R2b's "a self-consistent other file is
refused and never moved" (D11 *As implemented (#192)* and the #219
addendum), and the *Consequences* bullet and H7's sentence that leave
such a file on a copy target one copy short for good. It adds the
copy-ledger kind `displaced`, which ADR-062 D7's fold counts among the
kinds that change no file list, beside `finding`, `verify` and
`lost-line` (ADR-066 D8). ADR-062's status line records all of this, as
it records ADR-065 and ADR-066. CLAUDE.md invariant 4 is restated with
both exceptions (D8). The decisions record the #227 plan
(<https://github.com/Replikanti/mie/issues/227#issuecomment-6934042130>)
and its STOP 1 operator decisions (2026-11-17): the move is gated behind
`--move-other-files`; it is recorded with the new kind `displaced`,
which carries the full manifest text; R1, R2 and R2b are in scope; no
replay-safe quarantine.

## Context

A copy target R can hold, under a path P that hot holds or that the copy
ledger records, a sealed file whose manifest text differs from P's text
(the ledger's `manifest` record, else hot's manifest) while its data
verifies against its own manifest: `CopyCheck::Manifest { other_file:
true }`. That takes an earlier D5 violation: P sealed twice with different
bytes (a capture restarted on a lost hot before #195, which reused part
numbers), or a file copied in by hand. Then:

- every replication pass refuses the D5 copy of P to R ("its manifest
  differs from the source's") and reports `P: j of k copies`;
- `mie store repair` refused it and never moved it (R1, and R2b since
  #219);
- the hot restore leaves it in place with a `FAIL:` line naming D5 (H6
  k1);
- the hot removal never plans P's partition, since P lacks one of its k
  copies (ADR-062 D6 rule (e)).

So P stays one copy short on R, and its partition stays on hot for good.
ADR-062 H7 lists this as the one data state without a mie command. For a
path that left hot, an other file on the first root in read order makes
every read of P fail: the tiered `read` checks the first copy it finds
against the selected text and never falls back to the next root. Moving
the other file away is the only way to give R the recorded copy, and it
moves a sealed file, which D5 forbids except for the *replace*
resolution.

## Decision

### D1. The canonical version

The ledger's `manifest` record of P, else hot's manifest: the text R1,
R2b and V3 already read.

- Why: it is the only text a selection of this store can name for P. The
  union `select` takes hot's manifest, else the ledger's record, and
  fails when the two differ (`tiers.rs`, the conflict check in `select`);
  the tiered `read` serves only bytes with the selected SHA-256 and never
  falls back (`reader.rs`, `read_tiered`).
- Rejected, the target's file as canonical: it would put a second text
  into the ledger, which is a `conflict` verdict and fails every
  selection that covers P.
- Weak spot: the target's file may be the older version (sealed first,
  then P sealed again after a hot loss before #195). A result cited before
  the second seal named the older file. This store cannot reproduce it,
  whether the file moves or not (D3), and the record keeps the file
  findable.

### D2. Where the other file goes

Into `<root>/.mie/damaged/<run at_ms>/<relative path>` on the same root,
by R5's own `replica::move_aside`: the data, then the manifest, then the
seal temp names, each a rename that refuses to replace, with directory
fsyncs. It is never deleted.

- Why there: every reader ignores it. `select` and recovery walk only
  `source=` (`layout.rs`); the rebuild and the restore read
  `.mie/damaged/` as names only, and such a name "names a path but never
  holds it" and never supplies a text (ADR-062 D7 B6a item 3). A rename
  on one filesystem costs no data I/O, and a crash leaves either name,
  never neither.
- Rejected, the root's `.quarantine/<id>/`: the rebuild's Q2 puts every
  part found there into its set Q (`archive_checks.rs`), and Q3 then
  skips B4–B8 for that path. After any later rebuild P's canonical copies
  would lose their `copy` records, a `quarantined` record would fail
  every selection that covers P, and a live path there is a permanent
  `FAIL:` ("no archive part").
- Rejected, a new `.mie/<name>/` directory: every rule that reads
  evidence (B6a, the restore's evidence read) would need a new audit, to
  buy a clearer name the record already gives.

### D3. Dataset versions that name the other file

A dataset version can name the other file's SHA-256 in two ways, and
neither is a selection of this store:

- (a) a version recorded before P was sealed again;
- (b) a single-root configuration whose `raw_root` is the copy target
  itself, which `store::check_hot` allows when `cold_roots` is empty: an
  ad-hoc use, like the DuckDB glob in `docs/data-availability.md`.

The tiered `select` never lists a cold root's manifest, and the tiered
`read` never serves bytes whose SHA-256 differs from the selected one. So
a version of kind (a) already replays to another dataset version
(DIVERGED), before the move and after it, and the move changes no version
a selection of this store can reach. Experiments record only the
version's hash (`data` in `mie-domain`'s `research.rs`), so no record
tells the repair which version names the file, and a rule "refuse the
move for a version that names it" cannot be checked.

Decision: no replay-safe quarantine and no such refusal. The bytes and the
manifest stay on the same root and are never deleted, and the record
carries the exact text, so its `file <path> <rows> <sha256>` line can be
matched against a recorded dataset text by hand. In case (b) the file
served changes from the other file to the canonical one, which makes the
target agree with the store.

### D4. The record and the flag

- **The record**: a new copy-ledger kind `displaced <at_ms> <root> <dir>
  <text> <chain>`.
  - `root`: the copy target's id, checked as a `copy` record's root is.
  - `dir`: the decimal name of the `.mie/damaged/` directory, which is the
    run's one clock read; a canonical decimal in `0 ..=
    MAX_EVENT_TIME_MS`, like `at_ms`.
  - `text`: the other file's manifest, escaped as a `manifest` record's
    text, and it must parse.
  - Why the full text and not `<rows> <sha256>`: under D5 a sealed file's
    identity is its manifest, byte for byte. An other file can share its
    rows and SHA-256 with the canonical file, for example a text that
    differs only in an event-time line, and #219's fixture differs only in
    `rows`, over the same data. A record keyed on rows and SHA-256 would
    also match the canonical copy the repair places there afterwards, and
    the completion step (D5, R5b) would then move it.
  - Why the copy ledger: it is hash-chained, replicated to every copy
    target, read by verify, and carried by the rebuild and the restore,
    which is ADR-066 D8's reasoning for `lost-line`. The record changes
    no file list and no path verdict.
  - Appended before the move, so a crash between the two leaves a record
    the next repair completes (D5, R5b), never a moved file without a
    record.
- **The flag**: `mie store repair --move-other-files` gates the move, as
  `--record-lost` gates `lost` records (ADR-062 D11) and
  `--record-lost-lines` gates `lost-line` records (ADR-066 D8). Without
  it each such file is a problem that names the flag and the other
  file's rows and SHA-256, and nothing is written. Why: the file is
  evidence of an earlier D5 violation (a path sealed twice), so an
  operator looks before its location changes.

### D5. The repair rules R5a and R5b

R1 (an open finding), R2 (a refill of a path that left hot) and R2b (an
unrecorded form of a path hot holds) call two rules.

- **R5b Completion**, first, for every path and marked target they visit
  (R2b: only forms it judges): when a `displaced` record of that root
  carries a text equal, byte for byte, to the target's manifest of the
  path, the marker is re-read and `move_aside` moves the path's names into
  `damaged_dir(root, record.dir)`. No new record and no flag: the record
  is the operator's consent, but only under gate 1 below: for a held path
  the completion is a named problem naming `mie store resolve`, and
  nothing moves. It covers two crash points: after the
  record, and between the data rename and the manifest rename. In the
  second, the manifest alone is left in place, and without R5b R1 or
  R2b would judge it "a manifest without its data" and move it into a new
  run's directory, splitting the evidence over two directories. A taken
  name in the record's directory is `Integrity`: named, nothing
  overwritten.
- **R5a Displacement**, for a form judged a self-consistent other file.
  The gates, in order; each one that fails is a named problem and nothing
  is written:
  1. the path's verdict is ordinary (R1, R2), or none, ordinary or
     without a `manifest` record (R2b). A held path's other file stays
     refused, naming `mie store resolve`: the two texts of a
     `manifest-conflict` hold are the resolution's to decide (#211 RQ2);
  2. R2b: the archive-entry gate (a current import entry on hot lists
     an archive part with the text's SHA-256), and hot's copy checks good
     against the text, since the closing pass copies only from hot;
  3. R1 and R2: a good source exists (R3), else R9; then hot holds none
     of the path's data and manifest, or its copy checks good against the
     text (a manifest without its data and an unreadable copy count as not
     good);
  4. the flag is given;
  5. the marker is re-read;
  6. none of the path's four names (`D`, `M`, `D~`, `M~`) already exists
     below `damaged_dir(root, run_ms)`.

  Then: the `displaced` record, the move, and a report line (no problem).
  R1 and R2 go on to their D5 copy from the source and its `copy` record;
  R2b leaves the path to the closing pass, which copies it from hot and
  records it.

### D6. Verify

V4 lists every `displaced` record of a marked copy target as a note
naming the path, rows, SHA-256 and directory. The note becomes a `FAIL:`
("the recorded move did not complete; `mie store repair` completes it")
while the target's manifest of the path still equals the record's text.
For a held path that `FAIL:` names `mie store resolve` first; for a lost,
conflicting, replaced or pending path, which no repair rule visits, it
stays a note that says so.
An I/O error reading it stops the root, as V4 already does. A
self-consistent other file's finding names the remedy, `mie store repair
--move-other-files`.

### D7. Rebuild, restore, writer and removal

- **Rebuild**: a `displaced` record names its text's path for B6
  (`named_path`), so such a path counts for `start_named` and the
  sources; the record is carried from the start like every record and
  never derived. A rebuild from an empty start therefore loses a record
  that only hot's ledger tail held; the evidence bytes stay, and since
  evidence never supplies a text, the derived selection is the same.
- **Restore**: H6 k1 and the rebuild's H5 b only name the remedy (`mie
  store repair --move-other-files`); neither moves anything.
- **Writer**: the text's path raises its partition's highest part, so no
  path any record names is ever reused (ADR-062 D6 union numbering).
- **Removal**: a `displaced` record after the plan whose text names a
  file of a planned partition stops that partition's removal: the copies
  the plan counted may have changed (fail closed, as for every record
  that names a planned file).
- **Select**: the record is ignored, beside `lost-line`.

### D8. Invariant 4

CLAUDE.md invariant 4 reads: a sealed file is never removed from the
union of the roots, with two exceptions: the *replace* resolution (ADR-062
D5), and a copy target's self-consistent other file under a recorded
path, which `mie store repair --move-other-files` moves into that root's
`.mie/damaged/<at_ms>/`, recorded in the copy ledger and never deleted,
while the path keeps its recorded file in the union. Why this is still
the invariant's purpose: the moved file was never part of any version a
selection of this store can reach (D3), its bytes are kept on the same
root, and the record names it.

## Consequences

- P goes from k − 1 copies to k on R, and its partition becomes a removal
  candidate again once every other rule of ADR-062 D6 holds.
- For a path that left hot, an other file on the first root in read order
  no longer makes every read fail once it moves.
- **Downgrade**: a binary built before this ADR refuses a ledger that
  holds a `displaced` record (`Corrupt`, the grammar is strict) and then
  fails every tiered `select`, `verify` and `repair` on that store. This
  is the ledger's policy for every added kind (ADR-066 D8). No record
  exists until an operator passes the flag; CHANGELOG.md says so.
- Writing the record before the move means a failed rename (EXDEV,
  EACCES, a name taken in a race) leaves a record whose file is still in
  place. Verify then fails naming the repair, and the next repair
  completes the move. Gate 6 removes the common cause before the record.
- The same root keeps the other file's bytes: about one part per moved
  file (Numbers).
- Out of scope: an other file on hot itself (R4 still refuses it: hot is
  the source of every pass, and a text mismatch between hot and the
  ledger is a `select` conflict, which needs its own decision); read-only
  roots, which are never written (ADR-062 D11); a data file without a
  manifest, which with no text cannot be shown to be an other file and
  which #219's rule moves without a record; deriving `displaced` records
  in a rebuild; deleting anything.

### Numbers and where they come from

There is no threshold and no tunable value. The sizes:

| Number | Source | Derivation | What changes it |
|--------|--------|------------|-----------------|
| A `displaced` record ≈ 501 B | The golden text of the copy-ledger tests (`golden_text_is_pinned_byte_for_byte_with_its_chain`), a `depth` path | 111 B of fixed fields (the kind, two 13-digit decimals, the root `cold-b`, four separators, the 64-hex chain and its `\n`) + 390 B of escaped manifest text (each of the manifest's 12 line ends and 11 inner spaces costs 3 B as `%0A` / `%20`, at a 4-digit `rows` and a 7-digit `bytes`) | The stream name and the digits of `rows`, `bytes` and the root id, a few bytes either way; a `manifest` record of the same file is 491.5 B on average (#190), so the two are the same order |
| One record per moved file per copy target | D4 | Records appear only when an operator passes the flag over an other file, never per pass | No steady growth: next to the 1.65 MB a day of the copy ledger (ADR-062), any count of such events is noise |
| Up to four renames per move, no data I/O | `move_aside` | `D`, `M`, `D~`, `M~`, each a rename on one filesystem plus two directory fsyncs | Nothing: a rename never copies bytes |
| One D5 copy per moved path | R1, R2: the repair's copy; R2b: the closing pass | One part, at most `RotationPolicy`'s 128 MiB payload; #99's `depth` parts average 3.6 MB (ADR-062 M1) | A larger part limit raises the bound linearly |
| One part's bytes of extra disk per moved file on its root | D2 | The other file stays on the root it was on | The operator may delete evidence by hand after checking it; mie never does |

## Alternatives considered

- **An addendum to ADR-062 D5** instead of a new ADR: an addendum may
  change no normative statement (`docs/adr/README.md`), and this one
  changes D5's "never removed from the union of the roots".
- **A replay-safe quarantine** that keeps the other file selectable: D3
  shows no selection of this store can reach it today. It would add a
  second generation of live paths that nothing reads, and it is the only
  option that would make #227 LARGE.
- **Refusing the move for a file a dataset version names**: no record
  tells the repair which version names the file (D3), so the rule cannot
  be checked.
- **The root's `.quarantine/<id>/`** and **a new `.mie/<name>/`
  directory**: D2.
- **No record**, only the report line and the moved manifest as the
  trail (STOP 1 decision 2's alternative): verify could not name an
  incomplete move, a crash between the renames would split the evidence,
  and an audit would have to list every root's `.mie/damaged/`.
- **A record keyed on rows and SHA-256**: it would match the canonical
  copy the repair places afterwards (D4).
- **Moving without a flag**: the file is evidence of an earlier D5
  violation; a repair run for an unrelated finding would move it before
  anyone looked (D4).
- **The target's file as canonical**: D1.

## Accept when

The PR that implements it merges with these offline tests green in its CI
(the names of the #227 plan):

- D4 grammar: `golden_text_is_pinned_byte_for_byte_with_its_chain`,
  `displaced_record_round_trips`, `displaced_grammar_refusals`.
- D7 folds: `copy_state_folds_displaced_records_per_root`,
  `a_displaced_record_changes_no_selection`, the writer's kind table in
  `union_numbering_counts_the_ledger_when_hot_lost_a_partition`,
  `a_displaced_record_after_the_plan_stops_the_removal`.
- D5: `ac227_r2b_other_file_moves_with_the_flag_and_reaches_k` (#227 AC
  2), `ac227_hot_bad_moves_nothing_and_r4_repairs_hot` (AC 3),
  `ac227_dataset_version_is_unchanged_by_the_move`,
  `ac227_a_removed_path_reads_again_after_the_move`,
  `ac227_held_path_other_file_is_refused_naming_resolve`,
  `r1_other_file_without_the_flag_is_named_and_never_moved`,
  `r1_other_file_moves_with_the_flag_and_is_repaired_from_a_good_copy`,
  `r2b_other_file_without_the_flag_is_named_and_never_moved`, the `r5a_*`
  and `r5b_*` tests, and `a_crash_at_every_repair_step_is_completed_by_the_next_run`
  with its `displaced` step.
- Review of the PR (#234): `r5a_r1_never_moves_a_copy_of_hots_own_other_file`,
  `r5b_a_held_path_is_not_completed_before_resolve`,
  `r5b_a_lost_path_is_a_note_and_never_moved`.
- D6, D7: `v4_a_displaced_record_is_a_note`,
  `v4_an_incomplete_move_fails_naming_the_repair`,
  `rebuild_carries_displaced_records_from_its_start`,
  `ac227_rebuild_after_a_move_derives_the_same_selection`.

## As implemented (#227)

One PR: the kind (`copy_ledger.rs`, `verification.rs`, `tiers.rs`,
`removal.rs`), R5a and R5b with `--move-other-files` (`repair.rs`,
`main.rs`), verify's note and `FAIL:` (`verify.rs`), the rebuild's
`named_path`, and the wording of the restore's H6 k1 and the rebuild's
H5 b. ADR-062 D11 *As implemented (#192)* keeps its text as history; this
section, ADR-062's status line and the repair module docs carry the rule.

Two rules came from the adversarial review of the PR (#234):

- **Hot's copy in R1 and R2** (finding 1). When hot itself held a
  self-consistent other file under the path, and a copy target held a
  byte-identical copy of hot's file, R1 with the flag moved the target's
  copy: hot's bytes lost their second copy in the layout, and the
  hot-versus-ledger state this ADR leaves out of scope was pre-empted by
  an unrelated repair run. R2b already refused it (gate 2), R1 and R2 did
  not. Gate 3 now also requires hot to hold none of the path or a copy
  that checks good against the text, checked after the source and before
  the flag, so a refusal never names a flag that would not act. A damaged
  hot copy is refused the same way: R4 repairs it in the same run, and
  the next run moves the target's file, the order R2b already follows.
  Verify's finding names the flag with that condition.
- **R5b outside gate 1** (finding 2). R5b ran wherever R1, R2 and R2b
  visit, which takes held paths (R1, R2b) and never lost ones. So a
  record appended while the path was ordinary, with the move not done (a
  crash after the record, a failed rename), could be completed after the
  path became held, before `mie store resolve`; and once the path was
  recorded lost, verify failed on every run naming `mie store repair`,
  which never visits it. Decision: R5b runs under gate 1 (held: a named
  problem naming `mie store resolve`, nothing moved; a re-admit makes the
  path ordinary again and R1 completes the move), and verify's `FAIL:`
  names the command that acts for the path's verdict: `mie store repair`
  (ordinary, none, without a `manifest` record), `mie store resolve` first
  (held), and a note for a lost, conflicting, replaced or pending path.
  Rejected, a new R5b sweep that completes a lost path's move: the
  lost-file rule writes nothing for a lost path (ADR-062 D11), and the
  move would buy nothing, since the path has no recorded file to copy
  there and every selection that covers it fails. The file stays in
  place on its root, never deleted, which keeps D5's union whole; the
  note and the record name it.
