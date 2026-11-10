//! `mie store verify` offline (ADR-062 D11, #192): AW5's finding, AW7,
//! one test per finding kind, the records a pass appends and the ones it
//! never appends (V7), an I/O stop, and the refusals (V1). The fixture is
//! the adopted store of `mie store adopt` offline: two copy targets, hot
//! holding the hot window, older partitions recorded removed.

mod common;

use common::tiers::*;
use common::{D0, HOUR, TempDir};
use mie_adapter_parquet::copy_ledger::{self, FindingCode, Kind, Record, Subject};
use mie_adapter_parquet::replica;
use mie_adapter_parquet::root_marker;
use mie_cli::config::ColdRoot;
use mie_cli::store;
use mie_domain::time::EventTime;
use mie_ports::raw::{RawRecord, RawRecordSink, RawStreamKey};
use std::fs;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;

/// "Now" of most verifications: well past G after the adoption's records.
const NOW: i64 = D0 + 2 * HOUR;

/// An adopted fixture under a fresh directory.
fn adopted(tag: &str) -> (TempDir, Fixture) {
    let dir = TempDir::new(tag);
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    (dir, fixture)
}

/// One verification at `now` with P = `period` hours.
fn verify_at(fixture: &Fixture, now: i64, period: &str) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::verify(
        &fixture.config,
        Some(&fixture.dst_archive),
        period,
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

/// Every `finding` record as (root, code, subject).
fn findings(fixture: &Fixture) -> Vec<(String, FindingCode, Subject)> {
    records(fixture)
        .into_iter()
        .filter_map(|record| match record.kind {
            Kind::Finding {
                root,
                code,
                subject,
            } => Some((root, code, subject)),
            _ => None,
        })
        .collect()
}

/// Every `verify` record as (at_ms, root, findings).
fn verifies(fixture: &Fixture) -> Vec<(i64, String, u64)> {
    records(fixture)
        .into_iter()
        .filter_map(|record| match record.kind {
            Kind::Verify { root, findings, .. } => Some((record.at_ms, root, findings)),
            _ => None,
        })
        .collect()
}

fn writable(path: &Path) {
    fs::set_permissions(path, fs::Permissions::from_mode(0o644)).unwrap();
}

/// Flips one bit of the byte at `offset` of `path`.
fn flip(path: &Path, offset: usize) {
    writable(path);
    let mut bytes = fs::read(path).unwrap();
    bytes[offset] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

fn cold(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.dir.join(name)
}

/// A live file of a partition the adoption recorded removed from hot.
fn removed_file(fixture: &Fixture) -> String {
    replica::list_sealed(&cold(fixture, "cold-a"))
        .unwrap()
        .into_iter()
        .find(|path| path.starts_with("source=binance-um") && date(path) < FIRST_HOT)
        .expect("a removed live file")
}

fn date(path: &str) -> &str {
    path.split('/')
        .find_map(|segment| segment.strip_prefix("date="))
        .unwrap()
}

fn manifest_of(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap())
}

#[track_caller]
fn assert_fail_line(out: &str, needles: &[&str]) {
    assert!(
        out.lines()
            .any(|line| line.starts_with("FAIL: ") && needles.iter().all(|n| line.contains(n))),
        "no FAIL line with {needles:?} in\n{out}"
    );
}

/// A clean store passes, and each pass over a copy target ends with one
/// `verify` record, `findings 0`; a second run passes too.
#[test]
fn a_clean_store_passes_with_one_verify_record_per_target() {
    let (_dir, fixture) = adopted("verify-clean");
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(out.starts_with("hot "), "{out}");
    let verify = verifies(&fixture);
    assert_eq!(
        verify,
        [(NOW, "cold-a".to_owned(), 0), (NOW, "cold-b".to_owned(), 0)]
    );
    assert!(findings(&fixture).is_empty());
    let (pass, out) = verify_at(&fixture, NOW + 1, "24");
    assert!(pass, "{out}");
    assert_eq!(verifies(&fixture).len(), 4);
}

/// AW5's finding: one flipped byte in one data copy names the file, the
/// root and the SHA-256 mismatch; the finding and the `verify` record share
/// root and `at_ms`.
#[test]
fn aw5_a_flipped_byte_names_the_file_root_and_sha256() {
    let (_dir, fixture) = adopted("verify-aw5");
    let path = removed_file(&fixture);
    flip(&cold(&fixture, "cold-a").join(&path), 40);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass && !out.contains("\nPASS\n"), "{out}");
    assert_fail_line(&out, &[&path, "cold-a", "sha256"]);
    assert_eq!(
        findings(&fixture),
        [(
            "cold-a".to_owned(),
            FindingCode::Sha256,
            Subject::File(path.clone())
        )]
    );
    let finding_at = records(&fixture)
        .iter()
        .find(|r| matches!(r.kind, Kind::Finding { .. }))
        .unwrap()
        .at_ms;
    assert_eq!(finding_at, NOW);
    assert!(verifies(&fixture).contains(&(NOW, "cold-a".to_owned(), 1)));
    assert!(verifies(&fixture).contains(&(NOW, "cold-b".to_owned(), 0)));
    // The k line names the file too: one good copy of two.
    assert_fail_line(&out, &[&path, "1 of 2 good copies"]);
}

/// One finding per kind: `missing`, `size` and `manifest` (an absent
/// manifest, and the D5 variant: a self-consistent other file).
#[test]
fn missing_size_and_manifest_findings() {
    type Damage = fn(&Path, &str);
    let cases: [(&str, Damage, FindingCode, &str); 4] = [
        (
            "missing",
            |root, path| fs::remove_file(root.join(path)).unwrap(),
            FindingCode::Missing,
            "missing: no data file",
        ),
        (
            "size",
            |root, path| {
                let data = root.join(path);
                writable(&data);
                fs::OpenOptions::new()
                    .append(true)
                    .open(&data)
                    .unwrap()
                    .write_all(b"x")
                    .unwrap();
            },
            FindingCode::Size,
            "size: its size differs",
        ),
        (
            "manifest",
            |root, path| fs::remove_file(root.join(manifest_of(path))).unwrap(),
            FindingCode::Manifest,
            "manifest: its manifest is absent or differs",
        ),
        (
            "other-file",
            |root, path| {
                let manifest = root.join(manifest_of(path));
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
            },
            FindingCode::Manifest,
            "two sealed files under one path",
        ),
    ];
    for (tag, damage, code, text) in cases {
        let (_dir, fixture) = adopted(&format!("verify-{tag}"));
        let path = removed_file(&fixture);
        damage(&cold(&fixture, "cold-b"), &path);
        let (pass, out) = verify_at(&fixture, NOW, "24");
        assert!(!pass, "{tag}: {out}");
        assert_fail_line(&out, &[&path, "cold-b", text]);
        assert_eq!(
            findings(&fixture),
            [("cold-b".to_owned(), code, Subject::File(path.clone()))],
            "{tag}"
        );
    }
}

/// `copies`: a file on hot copied to cold-a only is named for cold-b once
/// it is older than G, and never recorded (V7); within G it is exempt.
#[test]
fn copies_names_a_file_on_one_target_only_past_g() {
    let (_dir, fixture) = adopted("verify-copies");
    let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    let mut writer = fixture.writer("binance-um");
    writer
        .append(
            &key,
            RawRecord {
                event_time: EventTime::from_millis(D0 + 5 * HOUR),
                capture: None,
                payload: b"late".to_vec(),
            },
        )
        .unwrap();
    writer.close().unwrap();
    let new = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|path| !before.contains(path))
        .unwrap();
    // A pass while cold-b is unmounted copies it to cold-a only.
    let marker = cold(&fixture, "cold-b").join(".mie/root");
    let aside = cold(&fixture, "cold-b").join("root.aside");
    fs::rename(&marker, &aside).unwrap();
    let mut out = Vec::new();
    store::replicate(
        &fixture.config,
        Some(&fixture.dst_archive),
        &fixture.probe(),
        &|| D0 + HOUR,
        &mut out,
    )
    .unwrap();
    fs::rename(&aside, &marker).unwrap();

    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&new, "cold-b", "copies:"]);
    assert_fail_line(
        &out,
        &[&new, "1 of 2 good copies", "cold-b (no copy record)"],
    );
    assert!(
        !findings(&fixture)
            .iter()
            .any(|(_, code, _)| matches!(code, FindingCode::Copies | FindingCode::Stale)),
        "{:?}",
        findings(&fixture)
    );

    // Within G of the data's mtime it is exempt.
    let mtime = fs::metadata(fixture.hot().join(&new))
        .unwrap()
        .modified()
        .unwrap()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    let (_, out) = verify_at(&fixture, mtime + 1_000, "100000");
    assert!(
        !out.lines()
            .any(|l| l.starts_with("FAIL: ") && l.contains(&new)),
        "{out}"
    );
}

/// `prefix` on the journal: the line names the first differing byte.
#[test]
fn a_journal_copy_that_is_no_prefix_names_the_byte() {
    let (_dir, fixture) = adopted("verify-journal");
    let copy = cold(&fixture, "cold-a").join(".mie/journal/journal.jsonl");
    flip(&copy, 10);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &["journal on copy target", "cold-a", "prefix: byte 10"],
    );
    assert_eq!(
        findings(&fixture),
        [("cold-a".to_owned(), FindingCode::Prefix, Subject::Journal)]
    );
}

/// `entry`: an import-ledger entry copy with other bytes.
#[test]
fn an_entry_copy_that_differs_is_an_entry_finding() {
    let (_dir, fixture) = adopted("verify-entry");
    let dir = cold(&fixture, "cold-a").join(".mie/import-ledger/aggTrades");
    let name = fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .min()
        .unwrap();
    flip(&dir.join(&name), 5);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    let entry = format!("aggTrades/{name}");
    assert_fail_line(&out, &[&entry, "cold-a", "entry: the copy differs"]);
    assert_eq!(
        findings(&fixture),
        [(
            "cold-a".to_owned(),
            FindingCode::Entry,
            Subject::Entry(entry)
        )]
    );
}

/// AW7: one flipped byte in a copy of the copy ledger names the ledger,
/// the root and the first record whose chain fails.
#[test]
fn aw7_a_flipped_byte_in_a_ledger_copy_names_the_ledger_root_and_first_failing_record() {
    let (_dir, fixture) = adopted("verify-aw7");
    let copy = copy_ledger::ledger_path(&cold(&fixture, "cold-b"));
    let bytes = fs::read(&copy).unwrap();
    // The byte offset of record 3's line.
    let mut start = copy_ledger::HEADER.len();
    for _ in 0..2 {
        start += bytes[start..].iter().position(|&b| b == b'\n').unwrap() + 1;
    }
    flip(&copy, start + 3);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            "copy ledger on copy target",
            "cold-b",
            &format!("the hash chain fails at record 3 (byte {start})"),
        ],
    );
    assert_eq!(
        findings(&fixture),
        [("cold-b".to_owned(), FindingCode::Chain, Subject::CopyLedger)]
    );
}

/// `stale`: with cold-b's marker gone and its last verification older than
/// now − P, the line names cold-b, `stale`, the count and the first path,
/// and nothing is appended about cold-b; within P only the "not verified"
/// line.
#[test]
fn stale_names_an_unverified_target_whose_last_verification_is_older_than_p() {
    let (_dir, fixture) = adopted("verify-stale");
    assert!(verify_at(&fixture, NOW, "24").0);
    let before = records(&fixture).len();
    fs::remove_file(cold(&fixture, "cold-b").join(".mie/root")).unwrap();

    let (pass, out) = verify_at(&fixture, NOW + 3 * HOUR, "4");
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["cold-b", "not verified in this run"]);
    assert!(!out.contains("stale:"), "{out}");

    let count = replica::list_sealed(&cold(&fixture, "cold-b"))
        .unwrap()
        .len();
    let first = replica::list_sealed(&cold(&fixture, "cold-b"))
        .unwrap()
        .into_iter()
        .min()
        .unwrap();
    let (pass, out) = verify_at(&fixture, NOW + 3 * HOUR, "2");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            "cold-b",
            "stale:",
            &format!("{count} cop(ies) on cold-b"),
            &format!("the oldest at {NOW} ms"),
            &format!("the first {first}"),
        ],
    );
    let appended: Vec<Record> = records(&fixture).split_off(before);
    assert!(
        appended.iter().all(|record| match &record.kind {
            Kind::Finding { root, .. } | Kind::Verify { root, .. } => root == "cold-a",
            _ => false,
        }),
        "{appended:?}"
    );
}

/// V7: findings on hot and on a read-only cold root are reported, never
/// recorded.
#[test]
fn hot_and_read_only_findings_are_reported_not_recorded() {
    let (_dir, fixture) = adopted("verify-hot-ro");
    let on_hot = replica::list_sealed(&fixture.hot()).unwrap().remove(0);
    flip(&fixture.hot().join(&on_hot), 40);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&on_hot, "on hot", "sha256"]);
    assert!(findings(&fixture).is_empty(), "{:?}", findings(&fixture));

    // A read-only root holding a damaged copy of a removed file.
    let ro = fixture.dir.join("ro");
    fs::create_dir_all(&ro).unwrap();
    root_marker::create_meta(&ro, "ro", false).unwrap();
    let path = removed_file(&fixture);
    for file in [path.clone(), manifest_of(&path)] {
        let target = ro.join(&file);
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::copy(cold(&fixture, "cold-a").join(&file), &target).unwrap();
    }
    flip(&ro.join(&path), 40);
    let mut config = fixture.config.clone();
    config.paths.cold_roots.push(ColdRoot {
        path: ro.clone(),
        copy_target: false,
    });
    let mut out = Vec::new();
    let pass = store::verify(&config, None, "24", &fixture.probe(), &|| NOW, &mut out).unwrap();
    let out = String::from_utf8(out).unwrap();
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "read-only root", "(ro)", "sha256"]);
    assert!(
        !findings(&fixture)
            .iter()
            .any(|(root, _, _)| root == "ro" || root == "hot"),
        "{:?}",
        findings(&fixture)
    );
}

/// V4's I/O stop: a directory where a data file should be stops the
/// target: no `verify` record for it, and a line naming it.
#[test]
fn an_io_error_stops_the_target_without_a_verify_record() {
    let (_dir, fixture) = adopted("verify-io");
    let path = removed_file(&fixture);
    let data = cold(&fixture, "cold-a").join(&path);
    fs::remove_file(&data).unwrap();
    fs::create_dir(&data).unwrap();
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &["cold-a", "not verified in this run", "not a regular file"],
    );
    assert_eq!(verifies(&fixture), [(NOW, "cold-b".to_owned(), 0)]);
}

/// V1: the refusals print `FAIL:` and append nothing.
#[test]
fn the_refusals_append_nothing() {
    let (_dir, fixture) = adopted("verify-refusals");
    let ledger = fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap();
    for (period, needle) in [
        ("0", "--period-hours \"0\""),
        ("01", "--period-hours \"01\""),
        ("x", "--period-hours \"x\""),
        ("-1", "--period-hours \"-1\""),
        ("", "--period-hours \"\""),
        ("9999999999999999", "--period-hours \"9999999999999999\""),
    ] {
        let (pass, out) = verify_at(&fixture, NOW, period);
        assert!(!pass, "{period}: {out}");
        assert_fail_line(&out, &[needle]);
    }
    let held = replica::try_verify_lock(&fixture.hot()).unwrap();
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &["another mie store verify or mie store repair holds"],
    );
    drop(held);
    assert_eq!(
        fs::read(copy_ledger::ledger_path(&fixture.hot())).unwrap(),
        ledger
    );

    // A chain failure on hot names the record and its byte offset.
    let path = copy_ledger::ledger_path(&fixture.hot());
    let mut start = copy_ledger::HEADER.len();
    start += ledger[start..].iter().position(|&b| b == b'\n').unwrap() + 1;
    flip(&path, start + 2);
    let (pass, out) = verify_at(&fixture, NOW, "24");
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[&format!("the hash chain fails at record 2 (byte {start})")],
    );
}

#[test]
fn usage_errors_exit_2() {
    for args in [
        vec!["store", "verify"],
        vec!["store", "verify", "--config", "x.toml"],
        vec!["store", "verify", "--config", "x.toml", "--period-hours"],
        vec!["store", "repair"],
        vec![
            "store",
            "repair",
            "--config",
            "x.toml",
            "--period-hours",
            "1",
        ],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_mie"))
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}");
    }
}
