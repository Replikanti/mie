//! `mie store repair-hot` offline (ADR-065, #217): the acceptance end to
//! end on the adopted store of `mie store adopt` offline, whose copies equal
//! hot after the adoption's pass. AC1: hot's journal in the R7 suspect
//! state is restored and kept aside. AC2: a damaged hot import-ledger file
//! is restored and kept aside. AC3: a fork, and an entry without a good
//! copy, stop naming both sides and write nothing. The journal tests of
//! ADR-065 run on journals stripped of the line check (ADR-066,
//! `Fixture::new_legacy`), so they pin the byte rules that still decide
//! lines without it; the `ac224_*` tests pin the checks.

mod common;

use common::resolve::*;
use common::tiers::*;
use common::{D0, HOUR};
use mie_adapter_binance::archive::ledger::{LedgerName, classify};
use mie_adapter_parquet::replica::{self, TailSource};
use mie_cli::journal::read_runs;
use mie_cli::store::repair_hot::{Flags as HotFlags, Scope};
use mie_cli::store::{self, rebuild::Flags};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

/// When the damage is found.
const VERIFIED: i64 = D0 + 2 * HOUR;
/// When the repairs run.
const REPAIRED: i64 = D0 + 3 * HOUR;

fn repair_hot(fixture: &Fixture, scope: Scope, keep: bool, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::repair_hot(
        &fixture.config,
        Some(&fixture.dst_archive),
        scope,
        HotFlags {
            keep_hot_journal: keep,
        },
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn repair(fixture: &Fixture, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::repair(
        &fixture.config,
        Some(&fixture.dst_archive),
        store::repair::Flags::default(),
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn writable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
}

fn flip(path: &Path, offset: usize) {
    writable(path);
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

fn copy_of(fixture: &Fixture, root: &str) -> PathBuf {
    cold(fixture, root).join(".mie/journal/journal.jsonl")
}

/// The runs `read_runs` lists, as text.
fn runs(fixture: &Fixture) -> String {
    format!(
        "{:?}",
        read_runs(&fixture.config.paths.journal, "binance-um")
            .map(|runs| runs.iter().map(|r| r.started_at_ms).collect::<Vec<_>>())
    )
}

/// One journal record of exactly `len` bytes, its `\n` included.
fn record(run: &str, len: usize) -> Vec<u8> {
    let mut bytes =
        format!("{{\"type\":\"stats\",\"run_id\":\"{run}\",\"at_ms\":1,\"pad\":\"").into_bytes();
    assert!(bytes.len() + 3 <= len, "{len} is too short");
    bytes.resize(len - 3, b'*');
    bytes.extend_from_slice(b"\"}\n");
    bytes
}

fn append(path: &Path, bytes: &[u8]) {
    let mut all = fs::read(path).unwrap();
    all.extend_from_slice(bytes);
    fs::write(path, all).unwrap();
}

/// AC1: one flipped byte in hot's journal is the R7 suspect state; verify
/// fails and repair refuses both copies naming this command, which
/// restores hot's journal from the copies and keeps the flipped file.
#[test]
fn ac1_hot_journal_r7_suspect_is_restored_and_kept_aside() {
    let (_dir, fixture) = adopted_legacy("hot-ac1");
    let journal = fixture.config.paths.journal.clone();
    let listed = runs(&fixture);
    assert!(listed.starts_with("Ok("), "{listed}");
    flip(&journal, 10);
    let flipped = fs::read(&journal).unwrap();
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    let (pass, out) = repair(&fixture, VERIFIED + 1);
    assert!(!pass, "{out}");
    for root in ["cold-a", "cold-b"] {
        assert_fail_line(
            &out,
            &[
                root,
                "refused",
                "hot's journal is the suspect",
                "mie store repair-hot --scope journal",
            ],
        );
    }
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(pass, "{out}");
    assert!(out.contains("journal: decision damaged"), "{out}");
    let hot = fs::read(&journal).unwrap();
    for root in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, root)).unwrap(), hot, "{root}");
        // The round trip: the next pass only compares each copy.
        assert_eq!(
            replica::extend_prefix(
                TailSource::File(&journal),
                &copy_of(&fixture, root),
                hot.len() as u64
            )
            .unwrap(),
            0
        );
    }
    let mut aside = journal.as_os_str().to_owned();
    aside.push(format!(".damaged-{REPAIRED}"));
    assert_eq!(fs::read(PathBuf::from(aside)).unwrap(), flipped);
    let (pass, out) = verify(&fixture, REPAIRED + 1);
    assert!(pass, "{out}");
    assert_eq!(runs(&fixture), listed);
}

/// The first `aggTrades` entry on hot.
fn first_entry(fixture: &Fixture) -> (String, PathBuf) {
    let dir = fixture.archive_config.paths.import_ledger.join("aggTrades");
    let name = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .filter(|name| !name.starts_with('.'))
        .min()
        .unwrap();
    (format!("aggTrades/{name}"), dir.join(name))
}

/// AC2: hot's first `aggTrades` entry rotten, unreadable, or listing one
/// file with a rewritten SHA-256 (the hot side of an entry conflict): the
/// FAIL lines of the pass, the verification or the repair name this
/// command, which restores the entry from the copies, read-only, and keeps
/// the old bytes; then the pass and the verification pass.
#[test]
fn ac2_hot_import_ledger_file_is_restored_and_kept_aside() {
    for (state, tag) in [(0, "rotten"), (1, "unreadable"), (2, "sha")] {
        let (_dir, fixture) = adopted(&format!("hot-ac2-{tag}"));
        let (name, path) = first_entry(&fixture);
        let good = fs::read(&path).unwrap();
        let old = match state {
            0 => {
                writable(&path);
                fs::write(&path, b"rotten\n").unwrap();
                b"rotten\n".to_vec()
            }
            1 => {
                fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
                good.clone()
            }
            _ => {
                let text = String::from_utf8(good.clone()).unwrap();
                let line = text.lines().find(|l| l.starts_with("file ")).unwrap();
                let sha = line.rsplit(' ').next().unwrap();
                let other: String = sha
                    .chars()
                    .map(|c| if c == '0' { '1' } else { '0' })
                    .collect();
                let changed = text.replace(sha, &other);
                writable(&path);
                fs::write(&path, &changed).unwrap();
                changed.into_bytes()
            }
        };
        let needle = "mie store repair-hot --scope import-ledger";
        if state < 2 {
            let (pass, out) = replicate(&fixture, VERIFIED);
            assert!(!pass, "{tag}: {out}");
            assert_fail_line(&out, &[&name, needle]);
            let (pass, out) = verify(&fixture, VERIFIED + 1);
            assert!(!pass, "{tag}: {out}");
            assert_fail_line(&out, &[&name, needle]);
        } else {
            let (pass, out) = verify(&fixture, VERIFIED);
            assert!(!pass, "{tag}: {out}");
            let (pass, out) = repair(&fixture, VERIFIED + 1);
            assert!(!pass, "{tag}: {out}");
            assert_fail_line(&out, &[&name, "refused", needle]);
        }
        let (pass, out) = repair_hot(&fixture, Scope::ImportLedger, false, REPAIRED);
        assert!(pass, "{tag}: {out}");
        assert_eq!(fs::read(&path).unwrap(), good, "{tag}");
        assert!(
            fs::metadata(&path).unwrap().permissions().readonly(),
            "{tag}"
        );
        for root in ["cold-a", "cold-b"] {
            let copy = cold(&fixture, root).join(".mie/import-ledger").join(&name);
            assert_eq!(fs::read(copy).unwrap(), good, "{tag} {root}");
        }
        let evidence = PathBuf::from(format!(
            "{}.damaged-{REPAIRED}",
            fixture.archive_config.paths.import_ledger.display()
        ))
        .join(&name);
        writable(&evidence);
        assert_eq!(fs::read(&evidence).unwrap(), old, "{tag}");
        // The round trip: the archive file's hot files classify.
        let dir = path.parent().unwrap();
        let files: Vec<(LedgerName, Vec<u8>)> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".import") && !n.starts_with('.'))
            .filter(|n| *n == name.split_once('/').unwrap().1)
            .map(|n| {
                (
                    LedgerName::parse(&n).unwrap(),
                    fs::read(dir.join(&n)).unwrap(),
                )
            })
            .collect();
        assert!(
            classify("aggTrades", &files).unwrap().current.is_some(),
            "{tag}"
        );
        let (pass, out) = replicate(&fixture, REPAIRED + 1);
        assert!(pass, "{tag}: {out}");
        let (pass, out) = verify(&fixture, REPAIRED + 2);
        assert!(pass, "{tag}: {out}");
    }
}

/// AC3: a fork stops, naming hot and both copies with d and s, and writes
/// nothing: with hot longer (line Y against the copies' X), and at equal
/// length, where X2a finds two lines of another run in place of X. Then
/// `--keep-hot-journal` keeps hot's lines: both copies are cut back to s,
/// with X kept in their `.mie/damaged/`, and the pass brings them to hot.
#[test]
fn ac3_a_fork_stops_naming_both_sides_and_writes_nothing() {
    let (_dir, fixture) = adopted_legacy("hot-ac3-fork");
    let s = fs::metadata(&fixture.config.paths.journal).unwrap().len();
    let x = record("x-run", 120);
    for root in ["cold-a", "cold-b"] {
        append(&copy_of(&fixture, root), &x);
    }
    append(&fixture.config.paths.journal, &record("y-run", 180));
    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision diverged"), "{out}");
    let journal = fixture.config.paths.journal.display().to_string();
    assert_fail_line(
        &out,
        &[
            "diverged",
            &journal,
            "cold-a",
            "cold-b",
            &format!("in the line from byte {s}"),
            "--keep-hot-journal",
        ],
    );
    assert!(
        out.ends_with("nothing written\n") || out.contains("nothing written\nFAIL"),
        "{out}"
    );
    assert_eq!(files(&fixture.dir), before);

    // Equal length: two lines of another run whose lengths add up to X's.
    let (_dir2, other) = adopted_legacy("hot-ac3-equal");
    for root in ["cold-a", "cold-b"] {
        append(&copy_of(&other, root), &x);
    }
    let mut two = record("z-run", 55);
    two.extend(record("z-run", 65));
    append(&other.config.paths.journal, &two);
    let before = files(&other.dir);
    let (pass, out) = repair_hot(&other, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision diverged"), "{out}");
    assert_fail_line(&out, &["a fork of equal length", "--keep-hot-journal"]);
    assert_eq!(files(&other.dir), before);

    // Decision 4: keep hot's lines.
    let (pass, out) = repair_hot(&fixture, Scope::Journal, true, REPAIRED + 1);
    assert!(pass, "{out}");
    let common = &fs::read(&fixture.config.paths.journal).unwrap()[..s as usize];
    for root in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, root)).unwrap(), common, "{root}");
        let aside = cold(&fixture, root).join(format!(
            ".mie/damaged/{}/.mie/journal/journal.jsonl",
            REPAIRED + 1
        ));
        let kept = fs::read(aside).unwrap();
        assert!(kept.ends_with(&x), "{root}");
    }
    let (pass, out) = replicate(&fixture, REPAIRED + 2);
    assert!(pass, "{out}");
    let hot = fs::read(&fixture.config.paths.journal).unwrap();
    for root in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, root)).unwrap(), hot, "{root}");
    }
    let (pass, out) = verify(&fixture, REPAIRED + 3);
    assert!(pass, "{out}");
}

fn rewrite_sha(path: &Path) {
    writable(path);
    let text = fs::read_to_string(path).unwrap();
    let line = text.lines().find(|l| l.starts_with("file ")).unwrap();
    let sha = line.rsplit(' ').next().unwrap();
    let other: String = sha
        .chars()
        .map(|c| if c == '0' { '1' } else { '0' })
        .collect();
    fs::write(path, text.replace(sha, &other)).unwrap();
}

/// AC3 for the import ledger, the plan's residual 3: the 2026-10-03
/// `metrics` entry on hot and both targets, then hot's copy rotten and
/// both target copies listing a file with a rewritten SHA-256. No copy is
/// good: the run stops naming every copy and the closure, writes nothing;
/// the closure by hand then re-admits the entry, byte-identical to the
/// importer's.
#[test]
fn ac3_an_entry_without_a_good_copy_stops_and_writes_nothing() {
    let (_dir, fixture) = adopted("hot-ac3-entry");
    let day = day_index("2026-10-03");
    run_import(&fixture, &metrics_day(day, false)).unwrap();
    let (pass, out) = replicate(&fixture, D0 + 2 * HOUR);
    assert!(pass, "{out}");
    let e0 = format!("{}.import", metrics_name());
    let name = format!("metrics/{e0}");
    let hot = ledger_file(&fixture, &e0);
    let imported = fs::read(&hot).unwrap();
    writable(&hot);
    fs::write(&hot, b"rotten\n").unwrap();
    for root in ["cold-a", "cold-b"] {
        rewrite_sha(&target_file(&fixture, root, &e0));
    }
    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::ImportLedger, false, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            &name,
            "hot (",
            "does not parse",
            "cold-a (",
            "cold-b (",
            "with another SHA-256",
            "no copy is good",
            "mie store rebuild --replace-hot-ledger --record-unlisted",
            &format!("mie store resolve --file {}", metrics_file()),
        ],
    );
    assert!(out.contains("nothing written"), "{out}");
    assert_eq!(files(&fixture.dir), before);

    // The closure: the moves by hand, the rebuild, the re-admit.
    let at = REPAIRED + 1;
    let hot_aside = PathBuf::from(format!(
        "{}.damaged-{at}",
        fixture.archive_config.paths.import_ledger.display()
    ))
    .join(&name);
    fs::create_dir_all(hot_aside.parent().unwrap()).unwrap();
    fs::rename(&hot, &hot_aside).unwrap();
    for root in ["cold-a", "cold-b"] {
        let aside = cold(&fixture, root)
            .join(format!(".mie/damaged/{at}/.mie/import-ledger"))
            .join(&name);
        fs::create_dir_all(aside.parent().unwrap()).unwrap();
        fs::rename(target_file(&fixture, root, &e0), aside).unwrap();
    }
    let flags = Flags {
        record_unlisted: true,
        replace_hot_ledger: true,
        ..Flags::default()
    };
    let (_, out) = rebuild(&fixture, flags, REBUILT + HOUR);
    assert!(out.contains("unlisted"), "{out}");
    let (pass, out) = resolve(&fixture, Some(&metrics_file()), RESOLVED + HOUR);
    assert!(pass, "{out}");
    assert_eq!(fs::read(&hot).unwrap(), imported);
    let (pass, out) = replicate(&fixture, RESOLVED + 2 * HOUR);
    assert!(pass, "{out}");
    let (pass, out) = verify(&fixture, RESOLVED + 3 * HOUR);
    assert!(pass, "{out}");
}

/// The pass guard (ADR-065 D9, review of #226 round 2, finding 1): hot's
/// journal bit-rots at x in its second line while cold-a lags at its first
/// line and cold-b holds the true bytes. Every step does what the one
/// before it named: repair-hot stops *one-witness*, verify, repair (which
/// refuses cold-b, one against one), repair-hot again, verify, repair.
/// Before the guard, the repair's closing pass extended cold-a over x with
/// hot's damage, repair-hot then decided *copy-damaged*, and the second
/// repair rebuilt cold-b with hot's byte. Now no pass extends cold-a over
/// a byte cold-b contradicts, so the state stays one against one and
/// cold-b keeps the true byte; the one-witness stop names what
/// `--keep-hot-journal` would drop.
#[test]
fn guard_hot_damage_never_reaches_the_good_copy_through_a_lagging_copy() {
    let (_dir, fixture) = adopted_legacy("hot-guard-lagging");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let first_nl = good.iter().position(|b| *b == b'\n').unwrap();
    let x = first_nl + 10;
    let lagging = copy_of(&fixture, "cold-a");
    writable(&lagging);
    fs::write(&lagging, &good[..=first_nl]).unwrap();
    flip(&journal, x);
    let bad = fs::read(&journal).unwrap()[x];
    assert_ne!(bad, good[x]);

    let mut t = REPAIRED;
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, t);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision one-witness"), "{out}");
    assert_fail_line(
        &out,
        &[
            "one copy against hot",
            "`--keep-hot-journal` would drop",
            "the copy on cold-b from byte",
        ],
    );
    for _ in 0..2 {
        t += HOUR;
        let _ = verify(&fixture, t);
        t += HOUR;
        let (pass, out) = repair(&fixture, t);
        assert!(!pass, "{out}");
        assert_eq!(
            fs::read(&lagging).unwrap(),
            &good[..=first_nl],
            "cold-a is held at its line\n{out}"
        );
        assert!(
            out.contains(&format!(
                "the journal copy on cold-a stops before byte {}",
                first_nl + 1
            )) && out.contains(&format!("at byte {x}")),
            "{out}"
        );
        assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), good);
        t += HOUR;
        let (pass, out) = repair_hot(&fixture, Scope::Journal, false, t);
        assert!(!pass, "{out}");
        assert!(out.contains("journal: decision one-witness"), "{out}");
    }
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), good);
}

/// Hot *behind* plus one copy fault (ADR-065 D2, review of #226 round 2,
/// finding 2): hot's journal lost its last line (unsynced at a power loss
/// after a pass copied it), cold-a has one bit flipped at byte 10, cold-b
/// is the true journal. Hot holds two of three at byte 10, and past hot's
/// end both copies agree: repair-hot extends hot from the copies and
/// leaves cold-a to the repair, never offering `--keep-hot-journal`; then
/// verify and repair heal cold-a, and all three journals are the true one.
#[test]
fn behind_hot_plus_one_copy_fault_extends_hot_and_the_repair_heals_the_copy() {
    let (_dir, fixture) = adopted_legacy("hot-behind-copy-fault");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let listed = runs(&fixture);
    let cut = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    writable(&journal);
    fs::write(&journal, &good[..cut]).unwrap();
    flip(&copy_of(&fixture, "cold-a"), 10);

    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(pass, "{out}");
    assert!(
        out.contains("journal: decision behind-copy-damaged"),
        "{out}"
    );
    assert!(!out.contains("--keep-hot-journal"), "{out}");
    assert!(
        out.lines().any(
            |line| line.starts_with("journal: the journal copy on cold-a (")
                && line.contains(
                    "differs from hot's journal at byte 10, where hot holds a strict majority"
                )
                && line.contains("`mie store repair`")
        ),
        "{out}"
    );
    assert_eq!(fs::read(&journal).unwrap(), good);

    let (pass, out) = verify(&fixture, REPAIRED + HOUR);
    assert!(!pass, "{out}");
    let (pass, out) = repair(&fixture, REPAIRED + 2 * HOUR);
    assert!(pass, "{out}");
    for root in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, root)).unwrap(), good, "{root}");
    }
    assert_eq!(fs::read(&journal).unwrap(), good);
    let (pass, out) = verify(&fixture, REPAIRED + 3 * HOUR);
    assert!(pass, "{out}");
    assert_eq!(runs(&fixture), listed);
}

/// No *behind-copy-damaged* where the copies disagree past hot's end
/// (ADR-065 D2, review of #226 round 3): hot lost its last two lines,
/// cold-a has a bit flipped at 10, and cold-b one in the last line. Past
/// hot's end no journal outvotes another, so hot takes nothing: *two-fault*,
/// naming the byte and what `--keep-hot-journal` would drop; nothing is
/// written.
#[test]
fn behind_hot_is_not_extended_where_the_copies_disagree_past_its_end() {
    let (_dir, fixture) = adopted_legacy("hot-behind-disagree");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let last = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    let cut = good[..last - 1].iter().rposition(|b| *b == b'\n').unwrap() + 1;
    writable(&journal);
    fs::write(&journal, &good[..cut]).unwrap();
    flip(&copy_of(&fixture, "cold-a"), 10);
    let q = last + 5;
    flip(&copy_of(&fixture, "cold-b"), q);

    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision two-fault"), "{out}");
    assert_fail_line(
        &out,
        &[
            "hot's journal is behind the copies, but it is not extended past its end",
            &format!("the journal copies disagree at byte {q}"),
            "`--keep-hot-journal` would drop",
        ],
    );
    assert_eq!(files(&fixture.dir), before);
}

/// Bytes past hot's end come only from copies with no difference below it
/// (ADR-065 D2, review of #226 round 3, finding 1): hot lost its last two
/// lines, cold-a has a bit flipped at 10 and holds the whole journal, and
/// cold-b lost its last line. Only cold-a, which differs from hot, holds
/// the last line, so hot takes nothing: *two-fault*, nothing written.
#[test]
fn behind_hot_takes_no_byte_only_a_differing_copy_holds() {
    let (_dir, fixture) = adopted_legacy("hot-behind-differing-tail");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let last = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    let cut = good[..last - 1].iter().rposition(|b| *b == b'\n').unwrap() + 1;
    writable(&journal);
    fs::write(&journal, &good[..cut]).unwrap();
    flip(&copy_of(&fixture, "cold-a"), 10);
    let b = copy_of(&fixture, "cold-b");
    writable(&b);
    fs::write(&b, &good[..last]).unwrap();

    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision two-fault"), "{out}");
    assert_fail_line(
        &out,
        &[
            "hot's journal is behind the copies, but it is not extended past its end",
            &format!(
                "the copy on cold-a differs from hot's journal at byte 10 and holds bytes past \
                 byte {last}"
            ),
            "`--keep-hot-journal` would drop",
        ],
    );
    assert_eq!(files(&fixture.dir), before);
}

/// Review of #226 round 3, finding 1, end to end (k = 2): hot loses its
/// last line O at a power loss after a pass copied it to cold-b; the
/// capture restarts and writes a shorter line N at the same offset while
/// cold-b is unmounted, and the pass extends lagging cold-a with N (D9's
/// *What it cannot see*). cold-b returns. Hot and cold-a outvote cold-b at
/// N, and only cold-b holds bytes past hot's end, a fork: repair-hot stops
/// *two-fault* and writes nothing, never appending a fragment of O to hot.
/// `--keep-hot-journal` keeps hot: cold-b is rebuilt up to the line start,
/// O kept in its `.mie/damaged/`, and the next pass writes N there.
#[test]
fn a_fork_through_an_unmounted_copy_stops_and_the_flag_keeps_hot() {
    let (_dir, fixture) = adopted_legacy("hot-fork-unmounted");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let cut = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    let first_nl = good.iter().position(|b| *b == b'\n').unwrap();
    let o = good[cut..].to_vec();
    let n: &[u8] = b"{\"type\":\"run_start\",\"run_id\":\"r-new\",\"at_ms\":1}\n";
    assert!(n.len() < o.len());
    let a = copy_of(&fixture, "cold-a");
    writable(&a);
    fs::write(&a, &good[..=first_nl]).unwrap();
    writable(&journal);
    let mut hot = good[..cut].to_vec();
    hot.extend_from_slice(n);
    fs::write(&journal, &hot).unwrap();
    let b_root = cold(&fixture, "cold-b");
    let parked = b_root.with_extension("parked");
    fs::rename(&b_root, &parked).unwrap();
    let _ = replicate(&fixture, REPAIRED);
    fs::rename(&parked, &b_root).unwrap();
    assert_eq!(fs::read(&a).unwrap(), hot);
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), good);

    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED + 1);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision two-fault"), "{out}");
    assert_fail_line(
        &out,
        &[
            "hot's journal is behind the copies, but it is not extended past its end",
            "the copy on cold-b differs from hot's journal",
            &format!(
                "`--keep-hot-journal` would drop the copy on cold-b from byte {cut} to its end \
                 at byte {}",
                good.len()
            ),
        ],
    );
    assert_eq!(files(&fixture.dir), before);
    assert_eq!(fs::read(&journal).unwrap(), hot);

    let (pass, out) = repair_hot(&fixture, Scope::Journal, true, REPAIRED + 2);
    assert!(pass, "{out}");
    assert_eq!(fs::read(&journal).unwrap(), hot);
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), &good[..cut]);
    let aside = b_root.join(format!(
        ".mie/damaged/{}/.mie/journal/journal.jsonl",
        REPAIRED + 2
    ));
    assert_eq!(fs::read(aside).unwrap(), good);
    let _ = replicate(&fixture, REPAIRED + 3);
    for root in ["cold-a", "cold-b"] {
        assert_eq!(fs::read(copy_of(&fixture, root)).unwrap(), hot, "{root}");
    }
    assert_eq!(fs::read(&journal).unwrap(), hot);
}

/// The fork guard before *behind-copy-damaged* (ADR-065 D2, D3; review of
/// #226 round 3): as above, but the restarted capture's first line N is
/// longer than O, and a second line M reached cold-a before hot lost it at
/// a second power loss. Every byte past hot's end is cold-a's, consistent
/// with hot, and cold-b ends below hot's end; but cold-b differs from hot
/// at N in the shape of a fork (its `\n` ends O inside N). Taking M and
/// leaving cold-b to the repair would drop O's `run_end` with no flag
/// given, so it stops *two-fault*, nothing written.
#[test]
fn behind_hot_stops_where_a_shorter_copy_is_a_fork() {
    let (_dir, fixture) = adopted_legacy("hot-behind-fork");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let cut = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    let n = record("r-new", good.len() - cut + 100);
    let m = record("r-new", 120);
    let mut hot = good[..cut].to_vec();
    hot.extend_from_slice(&n);
    let mut longer = hot.clone();
    longer.extend_from_slice(&m);
    writable(&journal);
    fs::write(&journal, &hot).unwrap();
    let a = copy_of(&fixture, "cold-a");
    writable(&a);
    fs::write(&a, &longer).unwrap();
    assert_eq!(fs::read(copy_of(&fixture, "cold-b")).unwrap(), good);

    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision two-fault"), "{out}");
    assert_fail_line(
        &out,
        &[
            "hot's journal is behind the copies, but it is not extended past its end",
            "the copy on cold-b differs from hot's journal",
            "in the shape of a fork",
            "`--keep-hot-journal` would drop",
        ],
    );
    assert_eq!(files(&fixture.dir), before);
}

/// No *behind-copy-damaged* where hot is outvoted below its end (ADR-065
/// D2): hot lost its last line and has a bit flipped at x, where both
/// copies agree against it, and cold-a has one at 10. Hot holds no strict
/// majority at x, so nothing is extended: *two-fault*, nothing written.
#[test]
fn behind_hot_outvoted_below_its_end_is_not_extended() {
    let (_dir, fixture) = adopted_legacy("hot-behind-outvoted");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let cut = good[..good.len() - 1]
        .iter()
        .rposition(|b| *b == b'\n')
        .unwrap()
        + 1;
    writable(&journal);
    fs::write(&journal, &good[..cut]).unwrap();
    let x = good.iter().position(|b| *b == b'\n').unwrap() + 10;
    flip(&journal, x);
    flip(&copy_of(&fixture, "cold-a"), 10);
    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision two-fault"), "{out}");
    assert_fail_line(
        &out,
        &["`--keep-hot-journal` would drop", "cold-a", "cold-b"],
    );
    assert_eq!(files(&fixture.dir), before);
}

/// One checked journal record of exactly `len` bytes, its `\n` included
/// (ADR-066 D1).
fn checked_record(run: &str, len: usize) -> Vec<u8> {
    let object = record(run, len - 20);
    let mut bytes = mie_cli::journal::with_check(&object[..object.len() - 1]);
    bytes.push(b'\n');
    assert_eq!(bytes.len(), len);
    bytes
}

/// #224 AC 5: a damaged checked line in hot's journal is repaired from one
/// copy. Hot's second line flips a bit while cold-b lags at hot's first
/// line, so only cold-a holds the line: ADR-065 alone stops
/// *one-witness* (the guard test above), and the line's own check
/// attributes it: *line-damaged*, hot is cold-a's journal again, its
/// former bytes kept, the runs unchanged, and the next verification
/// passes.
#[test]
fn ac224_a_damaged_checked_line_in_hot_is_repaired_from_one_copy() {
    let (_dir, fixture) = adopted("hot-ac224-5");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    let listed = runs(&fixture);
    let first_nl = good.iter().position(|b| *b == b'\n').unwrap();
    let lagging = copy_of(&fixture, "cold-b");
    writable(&lagging);
    fs::write(&lagging, &good[..=first_nl]).unwrap();
    flip(&journal, first_nl + 10);
    let flipped = fs::read(&journal).unwrap();

    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(pass, "{out}");
    assert!(out.contains("journal: decision line-damaged"), "{out}");
    assert!(
        out.contains(&format!(
            "repaired from the copy on cold-a: its line(s) from byte(s) {} fail their check",
            first_nl + 1
        )) || out.contains(&format!(
            "replaced by the copy on cold-a: its line(s) from byte(s) {} fail their check",
            first_nl + 1
        )),
        "{out}"
    );
    assert_eq!(fs::read(&journal).unwrap(), good);
    let mut aside = journal.as_os_str().to_owned();
    aside.push(format!(".damaged-{REPAIRED}"));
    assert_eq!(fs::read(PathBuf::from(aside)).unwrap(), flipped);
    assert_eq!(runs(&fixture), listed);
    let (pass, out) = verify(&fixture, REPAIRED + 1);
    assert!(pass, "{out}");
}

/// #224 AC 6: a fork is detected from the checks and nothing is written.
/// The copies hold a line X of run x after the common journal; hot holds a
/// line Y of run y of X's length, so the `\n` positions agree: ADR-065
/// D3's blind spot, where the byte rules call it *damaged* and would
/// replace Y (shown on the same journal without checks). With checks
/// both lines verify: *diverged*, naming the verifying hot line, nothing
/// written.
#[test]
fn ac224_a_fork_is_detected_from_the_checks_and_nothing_is_written() {
    let (_dir, fixture) = adopted("hot-ac224-6");
    let s = fs::metadata(&fixture.config.paths.journal).unwrap().len();
    for root in ["cold-a", "cold-b"] {
        let copy = copy_of(&fixture, root);
        writable(&copy);
        append(&copy, &checked_record("x-run", 120));
    }
    append(&fixture.config.paths.journal, &checked_record("y-run", 120));
    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision diverged"), "{out}");
    assert_fail_line(
        &out,
        &[
            &format!("hot's line from byte {s} verifies its check"),
            "never overwrites a hot line that verifies",
            "--keep-hot-journal",
        ],
    );
    assert_eq!(files(&fixture.dir), before);

    // The same fork without checks: the byte rules cannot tell, and decide
    // *damaged*; run with `--keep-hot-journal`, which *damaged* refuses, so
    // the decision shows and nothing is written.
    let (_dir, legacy) = adopted_legacy("hot-ac224-6-legacy");
    for root in ["cold-a", "cold-b"] {
        let copy = copy_of(&legacy, root);
        writable(&copy);
        append(&copy, &record("x-run", 120));
    }
    append(&legacy.config.paths.journal, &record("y-run", 120));
    let (pass, out) = repair_hot(&legacy, Scope::Journal, true, REPAIRED);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision damaged"), "{out}");
}

/// #224: ADR-065's rules still decide lines without checks. On a journal
/// stripped of its checks, one flipped byte with two full copies is
/// *damaged* (not *line-damaged*) and restored; with cold-b lagging at the
/// first line, *one-witness*, nothing written, and the stop says the
/// lines carry no check.
#[test]
fn ac224_the_217_rules_still_decide_old_lines() {
    let (_dir, fixture) = adopted_legacy("hot-ac224-legacy");
    let journal = fixture.config.paths.journal.clone();
    let good = fs::read(&journal).unwrap();
    assert!(!String::from_utf8_lossy(&good).contains("crc32c"));
    flip(&journal, 10);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED);
    assert!(pass, "{out}");
    assert!(out.contains("journal: decision damaged"), "{out}");
    assert_eq!(fs::read(&journal).unwrap(), good);

    let first_nl = good.iter().position(|b| *b == b'\n').unwrap();
    let lagging = copy_of(&fixture, "cold-b");
    writable(&lagging);
    fs::write(&lagging, &good[..=first_nl]).unwrap();
    flip(&journal, first_nl + 10);
    let before = files(&fixture.dir);
    let (pass, out) = repair_hot(&fixture, Scope::Journal, false, REPAIRED + 1);
    assert!(!pass, "{out}");
    assert!(out.contains("journal: decision one-witness"), "{out}");
    assert_fail_line(&out, &["the differing lines carry no check"]);
    assert_eq!(files(&fixture.dir), before);
}
