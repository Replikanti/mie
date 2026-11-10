//! `mie store repair` offline (ADR-062 D11, #192): AW5 end to end, the
//! removability of a repaired partition (the comment of 2026-11-08), the
//! refill of an added or replaced target (the comment of 2026-11-07), the
//! lost-file rule, the ledger, journal and entry copies, the D5
//! discriminator, hot copies, and the refusals. The fixture is the adopted
//! store of `mie store adopt` offline.

mod common;

use common::tiers::*;
use common::{D0, HOUR, MapProbe, TempDir};
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, FindingCode, Kind, Record, Subject};
use mie_adapter_parquet::removal::RemovalPlan;
use mie_adapter_parquet::replica;
use mie_adapter_parquet::root_marker;
use mie_adapter_parquet::{RotationPolicy, TierContext};
use mie_cli::config::{ColdRoot, IngestConfig};
use mie_cli::store::{self, AdoptRequest, DeviceProbe};
use mie_ports::raw::{RawRecordSource, RawStoreError};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// When the damage is found.
const VERIFIED: i64 = D0 + 2 * HOUR;
/// When the repair runs: the name of its `.mie/damaged/` directories.
const REPAIRED: i64 = D0 + 3 * HOUR;
/// A hot window that starts a day later than the adoption's, so the
/// adopted hot-window partitions are removal candidates.
const LATER_FIRST_HOT: &str = "2026-10-07";

fn adopted(tag: &str) -> (TempDir, Fixture) {
    let dir = TempDir::new(tag);
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    (dir, fixture)
}

fn verify(fixture: &Fixture, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::verify(
        &fixture.config,
        Some(&fixture.dst_archive),
        "24",
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn repair_with(
    config: &IngestConfig,
    archive: &Path,
    probe: &dyn DeviceProbe,
    now: i64,
    record_lost: bool,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::repair(config, Some(archive), record_lost, probe, &|| now, &mut out).unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn repair(fixture: &Fixture, now: i64) -> (bool, String) {
    repair_with(
        &fixture.config,
        &fixture.dst_archive,
        &fixture.probe(),
        now,
        false,
    )
}

fn replicate(fixture: &Fixture, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::replicate(
        &fixture.config,
        Some(&fixture.dst_archive),
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn records(fixture: &Fixture) -> Vec<Record> {
    copy_ledger::read(&fixture.hot()).unwrap().unwrap().records
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

fn cold(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.dir.join(name)
}

fn date(path: &str) -> &str {
    path.split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .unwrap()
}

fn manifest_of(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap())
}

/// Every file that left hot (on cold-a, dated before the first hot date).
fn removed_files(fixture: &Fixture) -> Vec<String> {
    replica::list_sealed(&cold(fixture, "cold-a"))
        .unwrap()
        .into_iter()
        .filter(|path| date(path) < FIRST_HOT)
        .collect()
}

fn removed_file(fixture: &Fixture) -> String {
    removed_files(fixture)
        .into_iter()
        .find(|path| path.starts_with("source=binance-um"))
        .unwrap()
}

#[track_caller]
fn assert_fail_line(out: &str, needles: &[&str]) {
    assert!(
        out.lines()
            .any(|line| line.starts_with("FAIL: ") && needles.iter().all(|n| line.contains(n))),
        "no FAIL line with {needles:?} in\n{out}"
    );
}

/// The index of the first record matching `matches`.
fn position(records: &[Record], matches: impl Fn(&Kind) -> bool) -> Option<usize> {
    records.iter().position(|record| matches(&record.kind))
}

/// One `mie ingest`-style pass with the removal plan for the later hot
/// window; `None` when the pass gives no plan.
fn plan(fixture: &Fixture, now: i64) -> Option<RemovalPlan> {
    struct Ignore;
    impl store::PassObserver for Ignore {
        fn ledger_truncated(&mut self, _: &Path, _: copy_ledger::Truncation) {}
    }
    store::replicate::run_planned(
        &fixture.config,
        Some(fixture.archive_config.clone()),
        &fixture.probe(),
        &|| now,
        &CopyLedger::at(&fixture.hot()),
        &mut Ignore,
        LATER_FIRST_HOT,
    )
    .1
}

fn planned(plan: &Option<RemovalPlan>) -> Vec<String> {
    plan.iter()
        .flat_map(|plan| plan.candidates.iter().map(|c| c.partition.clone()))
        .collect()
}

/// AW5 offline: one flipped byte in cold-a's copy of a removed file;
/// verify fails; repair passes, cold-a's copy is byte-identical to cold-b's,
/// the bad bytes sit under `cold-a/.mie/damaged/`, a fresh `copy` record
/// follows the finding and the closing pass writes `ledger-copy`; the next
/// verify passes, and the AW1 reports equal the baseline taken before the
/// damage.
#[test]
fn aw5_end_to_end() {
    let (dir, fixture) = adopted("repair-aw5");
    let baseline = reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive);
    let path = removed_file(&fixture);
    let copy = cold(&fixture, "cold-a").join(&path);
    flip(&copy, 40);
    let damaged = fs::read(&copy).unwrap();
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "cold-a", "sha256"]);

    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(
        out.contains(&format!("{path} on ")) && out.contains("repaired from"),
        "{out}"
    );
    assert_eq!(
        fs::read(&copy).unwrap(),
        fs::read(cold(&fixture, "cold-b").join(&path)).unwrap()
    );
    let aside = replica::damaged_dir(&cold(&fixture, "cold-a"), REPAIRED).join(&path);
    assert_eq!(fs::read(aside).unwrap(), damaged);
    let records = records(&fixture);
    let finding = position(&records, |kind| {
        matches!(kind, Kind::Finding { root, code: FindingCode::Sha256, subject: Subject::File(p) }
            if root == "cold-a" && *p == path)
    })
    .unwrap();
    let copied = finding
        + 1
        + position(
            &records[finding + 1..],
            |kind| matches!(kind, Kind::Copy { path: p, root } if *p == path && root == "cold-a"),
        )
        .expect("a fresh copy record after the finding");
    assert!(
        position(&records[copied..], |kind| {
            matches!(kind, Kind::LedgerCopy { root, .. } if root == "cold-a")
        })
        .is_some(),
        "the closing pass records the ledger copy"
    );

    let (pass, out) = verify(&fixture, REPAIRED + HOUR);
    assert!(pass, "{out}");
    assert_eq!(
        reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive),
        baseline
    );

    // A second run is idle: once a pass has copied the verification's
    // records, it repairs nothing and appends nothing.
    replicate(&fixture, REPAIRED + HOUR + 1);
    let ledger = fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    let (pass, out) = repair(&fixture, REPAIRED + 2 * HOUR);
    assert!(pass, "{out}");
    assert!(!out.contains("repaired") && !out.contains("moved"), "{out}");
    assert_eq!(
        fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap(),
        ledger
    );
}

/// The comment of 2026-11-08: a pre-window hot partition whose file has a
/// finding leaves the removal plan, and the repair's fresh `copy` record
/// brings it back. A missing copy that came back is only re-verified: a
/// fresh record, no byte written.
#[test]
fn a_repaired_partition_is_removable_again() {
    let (_dir, fixture) = adopted("repair-removable");
    let before = planned(&plan(&fixture, D0 + HOUR));
    assert!(!before.is_empty());
    let partition = before[0].clone();
    let path = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with(&format!("{partition}/")))
        .unwrap();
    flip(&cold(&fixture, "cold-a").join(&path), 40);
    assert!(!verify(&fixture, VERIFIED).0);
    assert!(!planned(&plan(&fixture, VERIFIED + 1)).contains(&partition));
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    assert!(planned(&plan(&fixture, REPAIRED + 1)).contains(&partition));

    // The variant: a copy that went missing and came back.
    let (_dir, fixture) = adopted("repair-came-back");
    let path = removed_file(&fixture);
    let data = cold(&fixture, "cold-b").join(&path);
    let away = fixture.dir.join("away.parquet");
    fs::rename(&data, &away).unwrap();
    assert!(!verify(&fixture, VERIFIED).0);
    fs::rename(&away, &data).unwrap();
    let tree = files(&cold(&fixture, "cold-b"));
    let ledger_len = records(&fixture).len();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    assert!(out.contains("re-verified good"), "{out}");
    assert!(!replica::damaged_dir(&cold(&fixture, "cold-b"), REPAIRED).exists());
    let after: Vec<Record> = records(&fixture).split_off(ledger_len);
    assert!(
        after.iter().any(
            |r| matches!(&r.kind, Kind::Copy { path: p, root } if *p == path && root == "cold-b")
        ),
        "{after:?}"
    );
    // Only the ledger copy changed on cold-b.
    let strip = |tree: Vec<(String, Vec<u8>)>| -> Vec<(String, Vec<u8>)> {
        tree.into_iter()
            .filter(|(name, _)| name != ".mie/copy-ledger")
            .collect()
    };
    assert_eq!(strip(files(&cold(&fixture, "cold-b"))), strip(tree));
}

/// The configuration, archive configuration and probe of the adopted
/// store with the copy targets `targets` (names under the fixture
/// directory), each initialized with its name as id when it is new.
fn with_targets(fixture: &Fixture, targets: &[&str]) -> (IngestConfig, PathBuf, MapProbe) {
    let cold_roots: Vec<ColdRoot> = targets
        .iter()
        .map(|name| ColdRoot {
            path: cold(fixture, name),
            copy_target: true,
        })
        .collect();
    let mut devices = vec![(fixture.hot(), 1)];
    for (i, root) in cold_roots.iter().enumerate() {
        if !root.path.exists() {
            fs::create_dir_all(&root.path).unwrap();
            let id = root.path.file_name().unwrap().to_str().unwrap().to_owned();
            root_marker::create_meta(&root.path, &id, false).unwrap();
        }
        devices.push((root.path.clone(), i as u64 + 2));
    }
    let name = targets.join("-");
    let dst = fixture.dir.join("dst");
    let ingest = ingest_toml(
        &dst.join(format!("ingest-{name}.toml")),
        &fixture.hot(),
        &fixture.config.paths.journal,
        &cold_roots,
    );
    let archive = dst.join(format!("archive-{name}.toml"));
    archive_toml(&archive, &dst, &fixture.hot(), &cold_roots);
    (ingest, archive, MapProbe(devices))
}

/// The comment of 2026-11-07: after a PASSing adoption a target is added
/// (cold-c) or one replaced under a new id (cold-b2); an adoption fails
/// naming `mie store repair`; the repair copies every file that left hot
/// to the new target, checked against the ledger's manifest text, and its
/// journal copy is complete; the next adoption passes.
#[test]
fn an_added_or_replaced_target_is_refilled_and_the_next_adoption_passes() {
    for (tag, targets, new) in [
        (
            "repair-added",
            ["cold-a", "cold-b", "cold-c"].as_slice(),
            "cold-c",
        ),
        (
            "repair-replaced",
            ["cold-a", "cold-b2"].as_slice(),
            "cold-b2",
        ),
    ] {
        let (_dir, fixture) = adopted(tag);
        let (config, archive, probe) = with_targets(&fixture, targets);
        // The pass fills the new target from hot; files that left hot stay
        // short of it.
        let mut out = Vec::new();
        store::replicate(&config, Some(&archive), &probe, &|| D0 + HOUR, &mut out).unwrap();
        let empty = fixture.dir.join("empty");
        fs::create_dir_all(&empty).unwrap();
        let request = AdoptRequest {
            from: empty.clone(),
            archive_config: Some(archive.clone()),
            ..AdoptRequest::default()
        };
        let mut out = Vec::new();
        let passed = store::adopt(
            &config,
            &request,
            FIRST_HOT,
            &probe,
            &|| D0 + HOUR,
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(!passed, "{tag}: {text}");
        assert_fail_line(&text, &["Fill it with `mie store repair`"]);

        let (pass, out) = repair_with(&config, &archive, &probe, REPAIRED, false);
        assert!(pass, "{tag}: {out}");
        for path in removed_files(&fixture) {
            for file in [path.clone(), manifest_of(&path)] {
                assert_eq!(
                    fs::read(cold(&fixture, new).join(&file)).unwrap(),
                    fs::read(cold(&fixture, "cold-a").join(&file)).unwrap(),
                    "{tag}: {file}"
                );
            }
        }
        assert_eq!(
            fs::read(cold(&fixture, new).join(".mie/journal/journal.jsonl")).unwrap(),
            fs::read(&fixture.config.paths.journal).unwrap(),
            "{tag}"
        );
        let mut out = Vec::new();
        let passed = store::adopt(
            &config,
            &request,
            FIRST_HOT,
            &probe,
            &|| REPAIRED + 1,
            &mut out,
        )
        .unwrap();
        let text = String::from_utf8(out).unwrap();
        assert!(passed, "{tag}: {text}");
    }
}

/// Every tree a repair must leave alone, lock files left out: hot (its
/// ledger and import ledger included) and both targets.
fn trees(fixture: &Fixture) -> Vec<Vec<(String, Vec<u8>)>> {
    ["dst", "cold-a", "cold-b"]
        .iter()
        .map(|name| files(&fixture.dir.join(name)))
        .collect()
}

/// The lost-file rule: every copy of a removed file damaged and hot
/// without it; the repair reports it lost, exits 1 and leaves every root
/// byte-identical, the ledger included. With `--record-lost` there is
/// exactly one `lost … verify <sha>` record, and a selection that covers
/// the file fails with `Integrity` naming it. A recorded copy on an absent
/// target makes the file "not repairable", not lost.
#[test]
fn the_lost_file_rule_writes_nothing_and_records_only_with_the_flag() {
    let (_dir, fixture) = adopted("repair-lost");
    let path = removed_file(&fixture);
    for root in ["cold-a", "cold-b"] {
        flip(&cold(&fixture, root).join(&path), 40);
    }
    assert!(!verify(&fixture, VERIFIED).0);
    // The pass copies the findings to the ledger copies first, so the
    // closing pass has nothing left to write.
    replicate(&fixture, VERIFIED + 1);
    let before = trees(&fixture);
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&format!("{path} is lost"), "Nothing is recorded"]);
    assert_eq!(trees(&fixture), before);

    let (pass, out) = repair_with(
        &fixture.config,
        &fixture.dst_archive,
        &fixture.probe(),
        REPAIRED + 1,
        true,
    );
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&format!("{path} is lost"), "--record-lost"]);
    let sha = replica::parse_manifest(
        &fs::read_to_string(cold(&fixture, "cold-a").join(manifest_of(&path))).unwrap(),
    )
    .unwrap()
    .sha256;
    let lost: Vec<Kind> = records(&fixture)
        .into_iter()
        .map(|r| r.kind)
        .filter(|kind| matches!(kind, Kind::Lost { .. }))
        .collect();
    assert_eq!(
        lost,
        [Kind::Lost {
            path: path.clone(),
            origin: copy_ledger::LostOrigin::Verify,
            sha256: Some(sha),
        }]
    );
    let failed = selections()
        .iter()
        .filter_map(|selection| fixture.tiered().select(selection).err())
        .find(|error| matches!(error, RawStoreError::Integrity(m) if m.contains(&path)));
    assert!(failed.is_some(), "no selection fails naming {path}");

    // A recorded copy on an absent target: not repairable, not lost.
    let (_dir, fixture) = adopted("repair-absent");
    let path = removed_file(&fixture);
    flip(&cold(&fixture, "cold-a").join(&path), 40);
    fs::remove_file(cold(&fixture, "cold-b").join(".mie/root")).unwrap();
    assert!(!verify(&fixture, VERIFIED).0);
    let (pass, out) = repair_with(
        &fixture.config,
        &fixture.dst_archive,
        &fixture.probe(),
        REPAIRED,
        true,
    );
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "not repairable in this run", "cold-b"]);
    assert!(
        !records(&fixture)
            .iter()
            .any(|r| matches!(r.kind, Kind::Lost { .. }))
    );
}

/// The ledger copy: a chain failure is repaired; before the repair the
/// pass refuses that copy (L2) and gives no removal plan (L1), after it
/// the chain is valid on cold-a and the plan has candidates again. A copy
/// with a valid chain that differs from hot's is refused, untouched.
#[test]
fn a_ledger_copy_is_rebuilt_and_a_divergent_one_refused() {
    let (_dir, fixture) = adopted("repair-ledger");
    let copy = copy_ledger::ledger_path(&cold(&fixture, "cold-a"));
    let bytes = fs::read(&copy).unwrap();
    let mut start = copy_ledger::HEADER.len();
    for _ in 0..2 {
        start += bytes[start..].iter().position(|&b| b == b'\n').unwrap() + 1;
    }
    flip(&copy, start + 3);
    assert!(!verify(&fixture, VERIFIED).0);
    let (pass, out) = replicate(&fixture, VERIFIED + 1);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &["cold-a", "open copy-ledger finding", "mie store repair"],
    );
    assert!(planned(&plan(&fixture, VERIFIED + 2)).is_empty());

    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    let repaired = fs::read(&copy).unwrap();
    assert_eq!(copy_ledger::check_chain(&repaired).failure, None);
    let hot = fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    assert!(hot.starts_with(&repaired));
    assert!(
        replica::damaged_dir(&cold(&fixture, "cold-a"), REPAIRED)
            .join(".mie/copy-ledger")
            .exists()
    );
    assert!(!planned(&plan(&fixture, REPAIRED + 1)).is_empty());

    // A valid chain that differs: hot's first three records, then another.
    let (_dir, fixture) = adopted("repair-ledger-diverged");
    let hot = fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    let mut end = copy_ledger::HEADER.len();
    for _ in 0..3 {
        end += hot[end..].iter().position(|&b| b == b'\n').unwrap() + 1;
    }
    let other = fixture.dir.join("other");
    fs::create_dir_all(other.join(".mie")).unwrap();
    fs::write(copy_ledger::ledger_path(&other), &hot[..end]).unwrap();
    CopyLedger::at(&other)
        .append(&Record {
            at_ms: D0 + 999,
            kind: Kind::Returned {
                partition: "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-01"
                    .to_owned(),
            },
        })
        .unwrap();
    let copy = copy_ledger::ledger_path(&cold(&fixture, "cold-a"));
    writable(&copy);
    fs::copy(copy_ledger::ledger_path(&other), &copy).unwrap();
    let diverged = fs::read(&copy).unwrap();
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["copy ledger on copy target", "cold-a", "prefix:"]);
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["cold-a", "refused", "#193"]);
    assert_eq!(fs::read(&copy).unwrap(), diverged);
}

/// The journal copy: repaired when another copy agrees with hot past the
/// difference; refused, naming hot's journal as the suspect, when every
/// copy differs from hot at the same byte.
#[test]
fn a_journal_copy_is_repaired_by_majority_and_refused_against_hot() {
    let (_dir, fixture) = adopted("repair-journal");
    let copy = cold(&fixture, "cold-a").join(".mie/journal/journal.jsonl");
    flip(&copy, 10);
    assert!(!verify(&fixture, VERIFIED).0);
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    assert_eq!(
        fs::read(&copy).unwrap(),
        fs::read(&fixture.config.paths.journal).unwrap()
    );
    assert!(verify(&fixture, REPAIRED + 1).0);

    let (_dir, fixture) = adopted("repair-journal-hot");
    flip(&fixture.config.paths.journal, 10);
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    let copies: Vec<Vec<u8>> = ["cold-a", "cold-b"]
        .iter()
        .map(|root| fs::read(cold(&fixture, root).join(".mie/journal/journal.jsonl")).unwrap())
        .collect();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    for root in ["cold-a", "cold-b"] {
        assert_fail_line(
            &out,
            &[root, "refused", "byte 10", "hot's journal is the suspect"],
        );
    }
    for (root, before) in ["cold-a", "cold-b"].iter().zip(copies) {
        assert_eq!(
            fs::read(cold(&fixture, root).join(".mie/journal/journal.jsonl")).unwrap(),
            before
        );
    }
}

/// The entry copy: replaced from hot; refused as an entry conflict when
/// hot's entry lists a file with another SHA-256 than the ledger's.
#[test]
fn an_entry_copy_is_replaced_from_hot_unless_hot_cannot_vouch_for_it() {
    let entry_dir =
        |fixture: &Fixture, root: &str| cold(fixture, root).join(".mie/import-ledger/aggTrades");
    let first_entry = |dir: &Path| {
        fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .min()
            .unwrap()
    };
    let (_dir, fixture) = adopted("repair-entry");
    let name = first_entry(&entry_dir(&fixture, "cold-a"));
    let copy = entry_dir(&fixture, "cold-a").join(&name);
    flip(&copy, 5);
    assert!(!verify(&fixture, VERIFIED).0);
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    let hot_entry = fixture.dir.join("dst/ledger/aggTrades").join(&name);
    assert_eq!(fs::read(&copy).unwrap(), fs::read(&hot_entry).unwrap());

    let (_dir, fixture) = adopted("repair-entry-conflict");
    let name = first_entry(&entry_dir(&fixture, "cold-a"));
    let hot_entry = fixture.dir.join("dst/ledger/aggTrades").join(&name);
    writable(&hot_entry);
    let text = fs::read_to_string(&hot_entry).unwrap();
    let line = text.lines().find(|l| l.starts_with("file ")).unwrap();
    let sha = line.rsplit(' ').next().unwrap();
    let other: String = sha
        .chars()
        .map(|c| if c == '0' { '1' } else { '0' })
        .collect();
    fs::write(&hot_entry, text.replace(sha, &other)).unwrap();
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    let before = fs::read(entry_dir(&fixture, "cold-a").join(&name)).unwrap();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[&name, "cold-a", "refused", "entry conflict", "#194"],
    );
    assert_eq!(
        fs::read(entry_dir(&fixture, "cold-a").join(&name)).unwrap(),
        before
    );
}

/// The D5 discriminator: a self-consistent other file on cold-a is refused
/// and cold-a is untouched.
#[test]
fn a_self_consistent_other_file_is_refused_and_never_moved() {
    let (_dir, fixture) = adopted("repair-d5");
    let path = removed_file(&fixture);
    let manifest = cold(&fixture, "cold-a").join(manifest_of(&path));
    writable(&manifest);
    let text = fs::read_to_string(&manifest).unwrap();
    let rows = text
        .lines()
        .find_map(|line| line.strip_prefix("rows "))
        .unwrap()
        .to_owned();
    let more = rows.parse::<u64>().unwrap() + 1;
    fs::write(
        &manifest,
        text.replace(&format!("rows {rows}\n"), &format!("rows {more}\n")),
    )
    .unwrap();
    assert!(!verify(&fixture, VERIFIED).0);
    let before = files(&cold(&fixture, "cold-a"));
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[&path, "cold-a", "refused", "self-consistent other file"],
    );
    let strip = |tree: Vec<(String, Vec<u8>)>| -> Vec<(String, Vec<u8>)> {
        tree.into_iter()
            .filter(|(name, _)| name != ".mie/copy-ledger")
            .collect()
    };
    assert_eq!(strip(files(&cold(&fixture, "cold-a"))), strip(before));
}

/// The tiered writer of `source`, as a `Result`.
fn try_writer(fixture: &Fixture, source: &str) -> Result<mie_adapter_parquet::RawWriter, String> {
    let ctx = TierContext {
        first_hot_date: FIRST_HOT.to_owned(),
        now_ms: Arc::new(|| D0 + 10),
        on_recall: None,
    };
    fixture
        .tiered()
        .tiered_writer(source, RotationPolicy::default(), ctx)
        .map_err(|e| e.to_string())
}

/// Hot copies: a damaged hot archive part is replaced from a copy target;
/// refused, naming the holder, while a writer holds the archive source.
/// A hot manifest without its data (a crash after the move-aside's first
/// rename) is refused by writer recovery and completed by the repair.
#[test]
fn a_hot_copy_is_replaced_from_a_target_under_the_writer_lock() {
    let (_dir, fixture) = adopted("repair-hot");
    let part = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-archive"))
        .unwrap();
    let on_hot = fixture.hot().join(&part);
    flip(&on_hot, 40);
    let held = try_writer(&fixture, ARCHIVE_SOURCE).unwrap();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[&part, "on hot", "a writer holds source binance-archive"],
    );
    drop(held);
    let (pass, out) = repair(&fixture, REPAIRED + 1);
    assert!(pass, "{out}");
    assert_eq!(
        fs::read(&on_hot).unwrap(),
        fs::read(cold(&fixture, "cold-a").join(&part)).unwrap()
    );

    let live = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|p| p.starts_with("source=binance-um"))
        .unwrap();
    fs::rename(fixture.hot().join(&live), fixture.dir.join("away.parquet")).unwrap();
    let Err(refused) = try_writer(&fixture, "binance-um") else {
        panic!("recovery refuses a manifest without its data");
    };
    assert!(refused.contains("integrity"), "{refused}");
    let (pass, out) = repair(&fixture, REPAIRED + 2);
    assert!(pass, "{out}");
    assert!(try_writer(&fixture, "binance-um").is_ok());
    assert_eq!(
        fs::read(fixture.hot().join(&live)).unwrap(),
        fs::read(cold(&fixture, "cold-a").join(&live)).unwrap()
    );
}

/// R0: refused while the verification lock or the pass lock is held. A
/// read-only root that holds the only good copy is a source and is never
/// written.
#[test]
fn the_locks_refuse_and_a_read_only_source_is_never_written() {
    let (_dir, fixture) = adopted("repair-locks");
    let held = replica::try_verify_lock(&fixture.hot()).unwrap();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &["another mie store verify or mie store repair holds"],
    );
    drop(held);
    let held = replica::try_replication_lock(&fixture.hot()).unwrap();
    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["another replication pass holds"]);
    drop(held);

    // A read-only root with the only good copy of a removed file.
    let path = removed_file(&fixture);
    let ro = fixture.dir.join("ro");
    fs::create_dir_all(&ro).unwrap();
    root_marker::create_meta(&ro, "ro", false).unwrap();
    for file in [path.clone(), manifest_of(&path)] {
        let target = ro.join(&file);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(cold(&fixture, "cold-a").join(&file), &target).unwrap();
    }
    for root in ["cold-a", "cold-b"] {
        flip(&cold(&fixture, root).join(&path), 40);
    }
    assert!(!verify(&fixture, VERIFIED).0);
    let mut cold_roots = fixture.config.paths.cold_roots.clone();
    cold_roots.push(ColdRoot {
        path: ro.clone(),
        copy_target: false,
    });
    let dst = fixture.dir.join("dst");
    let config = ingest_toml(
        &dst.join("ingest-ro.toml"),
        &fixture.hot(),
        &fixture.config.paths.journal,
        &cold_roots,
    );
    let archive = dst.join("archive-ro.toml");
    archive_toml(&archive, &dst, &fixture.hot(), &cold_roots);
    let ro_tree = files(&ro);
    let (pass, out) = repair_with(&config, &archive, &fixture.probe(), REPAIRED + 1, false);
    assert!(pass, "{out}");
    assert_eq!(files(&ro), ro_tree);
    for root in ["cold-a", "cold-b"] {
        assert_eq!(
            fs::read(cold(&fixture, root).join(&path)).unwrap(),
            fs::read(ro.join(&path)).unwrap()
        );
    }
}

/// Review of #203, finding 2 (R9): the only good copy of a removed file
/// sits on cold-b, which fails to read (EIO from a failing disk; here a
/// mode-000 file). The repair names cold-b's read error and reports the
/// file "not repairable in this run", never lost: with `--record-lost` no
/// `lost` record is appended, and cold-b's copy stays byte-identical. Once
/// the read error is gone, the next repair repairs cold-a from cold-b.
#[test]
fn a_read_error_on_the_only_good_copy_is_not_repairable_never_lost() {
    let (_dir, fixture) = adopted("repair-read-error");
    let path = removed_file(&fixture);
    flip(&cold(&fixture, "cold-a").join(&path), 40);
    assert!(!verify(&fixture, VERIFIED).0);
    let good = cold(&fixture, "cold-b").join(&path);
    let good_bytes = fs::read(&good).unwrap();
    fs::set_permissions(&good, fs::Permissions::from_mode(0o000)).unwrap();
    let (pass, out) = repair_with(
        &fixture.config,
        &fixture.dst_archive,
        &fixture.probe(),
        REPAIRED,
        true,
    );
    writable(&good);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            &path,
            "not repairable in this run",
            "cold-b",
            "cannot be read",
        ],
    );
    assert!(!out.contains(&format!("{path} is lost")), "{out}");
    assert_eq!(fs::read(&good).unwrap(), good_bytes);
    assert!(
        !records(&fixture)
            .iter()
            .any(|r| matches!(r.kind, Kind::Lost { .. })),
        "a file whose good copy is intact on cold-b was recorded lost"
    );

    let (pass, out) = repair(&fixture, REPAIRED + 1);
    assert!(pass, "{out}");
    assert_eq!(
        fs::read(cold(&fixture, "cold-a").join(&path)).unwrap(),
        good_bytes
    );
}

/// Review of #203, finding 3 (R6): cold-a's ledger copy holds every
/// complete record of hot's but NULs in place of its last line (a crash
/// during `extend_prefix` on a filesystem that exposes the new size before
/// the data). Verify records it, the pass refuses the copy, and the repair
/// rebuilds it up to its last complete record; the closing pass extends
/// it, the removal plan has candidates again and the next verify passes.
#[test]
fn a_ledger_copy_with_a_damaged_tail_is_rebuilt_and_extended() {
    let (_dir, fixture) = adopted("repair-ledger-tail");
    let copy = copy_ledger::ledger_path(&cold(&fixture, "cold-a"));
    let bytes = fs::read(&copy).unwrap();
    let last_start = bytes[..bytes.len() - 1]
        .iter()
        .rposition(|&b| b == b'\n')
        .unwrap()
        + 1;
    let mut damaged = bytes[..last_start].to_vec();
    damaged.extend_from_slice(&[0u8; 40]);
    writable(&copy);
    fs::write(&copy, &damaged).unwrap();
    let (pass, out) = verify(&fixture, VERIFIED);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["copy ledger on copy target", "cold-a", "prefix:"]);
    assert!(planned(&plan(&fixture, VERIFIED + 1)).is_empty());

    let (pass, out) = repair(&fixture, REPAIRED);
    assert!(pass, "{out}");
    let repaired = fs::read(&copy).unwrap();
    assert_eq!(copy_ledger::check_chain(&repaired).failure, None);
    assert_eq!(copy_ledger::check_chain(&repaired).tail_bytes, 0);
    let hot = fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    assert!(hot.starts_with(&repaired));
    assert!(repaired.len() > last_start, "the closing pass extended it");
    assert_eq!(
        fs::read(
            replica::damaged_dir(&cold(&fixture, "cold-a"), REPAIRED).join(".mie/copy-ledger")
        )
        .unwrap(),
        damaged
    );
    assert!(!planned(&plan(&fixture, REPAIRED + 1)).is_empty());
    let (pass, out) = verify(&fixture, REPAIRED + 2);
    assert!(pass, "{out}");
}
