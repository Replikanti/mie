//! `mie store rebuild` offline (ADR-062 D7, #193): AW8 end to end, full,
//! stale and empty starts, the removal gate, the importer's halt on holds,
//! `PENDING` parts, and the copy-ledger copy #192 R6 defers to the rebuild.
//! The fixture is the adopted store of `mie store adopt` offline.

mod common;

use common::tiers::*;
use common::{D0, HOUR, TempDir};
use mie_adapter_binance::archive::catalog::{DAY_MS, Period};
use mie_adapter_binance::archive::ledger::{LedgerDir, Pending};
use mie_adapter_binance::archive::{ARCHIVE_SOURCE, ArchiveStream};
use mie_adapter_binance::transport::HttpGet;
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, HoldReason, Kind, Record};
use mie_adapter_parquet::removal::RemovalPlan;
use mie_adapter_parquet::verification::CopyState;
use mie_adapter_parquet::{replica, root_marker};
use mie_cli::archive::{self as archive_cmd, ImportRequest};
use mie_cli::replay::{self, ReplayRequest, ReplaySource};
use mie_cli::store::{self, rebuild::Flags};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawRecord, RawRecordSink, RawRecordSource, RawStreamKey};
use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;

/// When the rebuild runs.
const REBUILT: i64 = D0 + 3 * HOUR;
/// A hot window a day later than the adoption's, so the adopted
/// hot-window partitions are removal candidates.
const LATER_FIRST_HOT: &str = "2026-10-07";

fn adopted(tag: &str) -> (TempDir, Fixture) {
    let dir = TempDir::new(tag);
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    (dir, fixture)
}

fn rebuild(fixture: &Fixture, flags: Flags, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::rebuild(
        &fixture.config,
        Some(&fixture.dst_archive),
        flags,
        FIRST_HOT,
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

fn record_lost() -> Flags {
    Flags {
        record_lost: true,
        ..Flags::default()
    }
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

fn repair(fixture: &Fixture, now: i64) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::repair(
        &fixture.config,
        Some(&fixture.dst_archive),
        false,
        &fixture.probe(),
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
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

fn cold(fixture: &Fixture, name: &str) -> PathBuf {
    fixture.dir.join(name)
}

fn hot_ledger(fixture: &Fixture) -> PathBuf {
    copy_ledger::ledger_path(&fixture.hot())
}

#[track_caller]
fn assert_fail_line(out: &str, needles: &[&str]) {
    assert!(
        out.lines()
            .any(|line| line.starts_with("FAIL: ") && needles.iter().all(|n| line.contains(n))),
        "no FAIL line with {needles:?} in\n{out}"
    );
}

/// Damages the hash chain of both copy targets' ledger copies, so no copy
/// verifies and the rebuild starts empty.
fn damage_both_copies(fixture: &Fixture) {
    for name in ["cold-a", "cold-b"] {
        let path = copy_ledger::ledger_path(&cold(fixture, name));
        let mut bytes = fs::read(&path).unwrap();
        bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
        fs::write(&path, bytes).unwrap();
    }
}

/// Cuts both ledger copies to the records before the middle `removed`
/// record: a stale start that lacks the last pass and half the removals.
fn stale_copies(fixture: &Fixture) {
    for name in ["cold-a", "cold-b"] {
        let path = copy_ledger::ledger_path(&cold(fixture, name));
        let bytes = fs::read(&path).unwrap();
        let ledger = copy_ledger::parse(&path, &bytes).unwrap();
        let removed: Vec<usize> = ledger
            .records
            .iter()
            .enumerate()
            .filter(|(_, r)| matches!(r.kind, Kind::Removed { .. }))
            .map(|(i, _)| i)
            .collect();
        assert!(removed.len() >= 2, "{removed:?}");
        let cut = ledger.ends[removed[removed.len() / 2] - 1] as usize;
        fs::write(&path, &bytes[..cut]).unwrap();
    }
}

/// A live data file that left hot, and the journal line that names it.
fn removed_live_file(fixture: &Fixture) -> (String, usize) {
    let path = replica::list_sealed(&cold(fixture, "cold-a"))
        .unwrap()
        .into_iter()
        .find(|path| path.starts_with("source=binance-um") && date_of(path) < FIRST_HOT)
        .unwrap();
    let journal = fs::read_to_string(&fixture.config.paths.journal).unwrap();
    let line = journal
        .lines()
        .position(|line| line.contains("\"sealed\"") && line.contains(&path))
        .expect("a sealed line names it")
        + 1;
    (path, line)
}

fn manifest_of(path: &str) -> String {
    format!("{}.manifest", path.strip_suffix(".parquet").unwrap())
}

/// Deletes `path`'s data and manifest from both copy targets.
fn delete_from_cold(fixture: &Fixture, path: &str) {
    for name in ["cold-a", "cold-b"] {
        for file in [path.to_owned(), manifest_of(path)] {
            fs::remove_file(cold(fixture, name).join(file)).unwrap();
        }
    }
}

/// Every selection's result (dataset version and files, or the error
/// text), in [`selections`] order.
fn selected(fixture: &Fixture) -> Vec<String> {
    let store = fixture.tiered();
    selections()
        .iter()
        .map(|selection| match store.select(selection) {
            Ok(dataset) => format!("{dataset:?}"),
            Err(error) => format!("error: {error}"),
        })
        .collect()
}

/// The held paths of hot's ledger.
fn held(fixture: &Fixture) -> BTreeSet<String> {
    let ledger = copy_ledger::read(&fixture.hot()).unwrap().unwrap();
    CopyState::fold(&ledger)
        .unwrap()
        .held()
        .into_iter()
        .map(|(path, _)| path)
        .collect()
}

/// Seals one archive `aggTrades` part on hot that no import entry lists.
fn seal_unlisted_archive_part(fixture: &Fixture) -> String {
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    let key = RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, "aggTrades").unwrap();
    let mut writer = fixture.writer(ARCHIVE_SOURCE);
    writer
        .append(
            &key,
            RawRecord {
                event_time: EventTime::from_millis(D0 + 20 * HOUR),
                capture: None,
                payload: b"9,1.0,1.0,9,9,1,false".to_vec(),
            },
        )
        .unwrap();
    writer.close().unwrap();
    replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .find(|path| !before.contains(path))
        .unwrap()
}

/// AW8 offline: after the ledger is lost, a rebuild from the copies, and
/// again from an empty start, passes; the next pass and a verification
/// pass, and the AW1 reports equal the baseline. Without B8 the
/// verification fails on the partitions hot lacks.
#[test]
fn aw8_reports_are_byte_identical_after_the_rebuild() {
    let (dir, fixture) = adopted("rebuild-aw8-reports");
    let baseline = reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive);
    let original = fs::read(hot_ledger(&fixture)).unwrap();
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    let rebuilt = fs::read(hot_ledger(&fixture)).unwrap();
    assert!(original.starts_with(&rebuilt[..copy_ledger::HEADER.len()]));
    assert_pass(&replicate(&fixture, REBUILT + 1));
    let (pass, out) = verify(&fixture, REBUILT + 2);
    assert!(pass, "{out}");
    assert_eq!(
        reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive),
        baseline
    );

    // From an empty start: every record re-derived.
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    damage_both_copies(&fixture);
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT + HOUR);
    assert!(pass && out.ends_with("PASS\n"), "{out}");
    assert!(out.contains("started empty"), "{out}");
    assert!(out.contains("moved aside"), "{out}");
    assert_pass(&replicate(&fixture, REBUILT + HOUR + 1));
    let (pass, out) = verify(&fixture, REBUILT + HOUR + 2);
    assert!(pass, "{out}");
    assert_eq!(
        reports(dir.path(), &fixture.dst_ingest, &fixture.dst_archive),
        baseline
    );
}

/// AW8 offline, the lost-file variant: a file deleted from every root is
/// reported with its journal line and nothing is written; with
/// `--record-lost` its `lost` record is written. A replay covering it then
/// fails with `Integrity` naming it, while a selection that does not
/// cover it passes.
#[test]
fn aw8_a_file_missing_from_every_root_is_reported_then_recorded_lost() {
    let (_dir, fixture) = adopted("rebuild-aw8-lost");
    let (path, line) = removed_live_file(&fixture);
    delete_from_cold(&fixture, &path);
    fs::remove_file(hot_ledger(&fixture)).unwrap();

    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "no root holds it"]);
    assert!(out.contains(&format!("journal line {line} ")), "{out}");
    assert!(out.contains("nothing written"), "{out}");
    assert!(!hot_ledger(&fixture).exists());

    let (pass, out) = rebuild(&fixture, record_lost(), REBUILT + 1);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&path, "recorded lost"]);
    assert!(out.contains("wrote "), "{out}");
    let records = copy_ledger::read(&fixture.hot()).unwrap().unwrap().records;
    assert!(
        records
            .iter()
            .any(|r| matches!(&r.kind, Kind::Lost { path: p, .. } if *p == path)),
        "{records:?}"
    );

    let day = date_of(&path);
    let start = D0 + (day_index(day) - day_index("2026-10-06")) * DAY_MS;
    let replay_out = |from: i64, to: i64| {
        let mut out = Vec::new();
        let outcome = replay::run(
            &ReplayRequest {
                source: ReplaySource::Live(fixture.config.clone()),
                window: ReplayWindow {
                    start: EventTime::from_millis(from),
                    end: EventTime::from_millis(to),
                },
            },
            &mut out,
        )
        .unwrap();
        (outcome.pass, String::from_utf8(out).unwrap())
    };
    let (pass, out) = replay_out(start, start + DAY_MS);
    assert!(!pass, "{out}");
    assert!(out.contains("integrity") && out.contains(&path), "{out}");
    let window = selections()
        .into_iter()
        .find(|selection| {
            selection.window().start.as_millis() >= D0
                && selection.window().end.as_millis() <= D0 + 2 * DAY_MS
        })
        .unwrap();
    assert!(fixture.tiered().select(&window).is_ok());
}

/// Days since 1970-01-01 of a `YYYY-MM-DD` date.
fn day_index(date: &str) -> i64 {
    mie_adapter_binance::archive::catalog::parse_day(date).unwrap()
}

/// Full, stale and empty starts give the same selections and the same
/// held paths as the intact ledger, without and with an unlisted held
/// part.
#[test]
fn rebuilds_from_full_stale_and_empty_agree() {
    for with_hold in [false, true] {
        let mut expected: Option<(Vec<String>, BTreeSet<String>)> = None;
        for start in ["full", "stale", "empty"] {
            let (_dir, fixture) = adopted(&format!("rebuild-starts-{start}-{with_hold}"));
            if with_hold {
                let part = seal_unlisted_archive_part(&fixture);
                CopyLedger::at(&fixture.hot())
                    .append(&Record {
                        at_ms: D0 + 2,
                        kind: Kind::Hold {
                            path: part,
                            reason: HoldReason::Unlisted,
                        },
                    })
                    .unwrap();
                // The pass copies the ledger tail; the unlisted part waits
                // for an entry, which fails the pass.
                let (_, out) = replicate(&fixture, D0 + 3);
                assert!(out.contains("waiting for its import entry"), "{out}");
            }
            let before = (selected(&fixture), held(&fixture));
            assert_eq!(before.1.len(), usize::from(with_hold));
            fs::remove_file(hot_ledger(&fixture)).unwrap();
            match start {
                "stale" => stale_copies(&fixture),
                "empty" => damage_both_copies(&fixture),
                _ => {}
            }
            // The full start holds the part already; the stale and the
            // empty one re-derive the hold, recorded with the flag.
            let flags = Flags {
                record_unlisted: with_hold,
                ..Flags::default()
            };
            let (pass, out) = rebuild(&fixture, flags, REBUILT);
            assert_eq!(pass, !with_hold, "{start}: {out}");
            assert!(out.contains("wrote "), "{start}: {out}");
            if start == "empty" {
                assert!(out.contains("started empty"), "{out}");
            }
            let after = (selected(&fixture), held(&fixture));
            assert_eq!(after, before, "{start} {with_hold}: {out}");
            match &expected {
                None => expected = Some(after),
                Some(expected) => assert_eq!(&after, expected, "{start} {with_hold}"),
            }
        }
    }
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

/// B10: after a rebuild the plan has no candidate, also after a
/// verification of one target, and has them once both are verified.
#[test]
fn removal_waits_for_a_full_verification_after_a_rebuild() {
    let (_dir, fixture) = adopted("rebuild-gate");
    let control = plan(&fixture, D0 + HOUR).unwrap();
    assert!(!control.candidates.is_empty());
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    assert_pass(&rebuild(&fixture, Flags::default(), REBUILT));

    let waiting = plan(&fixture, REBUILT + 1).unwrap();
    assert!(waiting.candidates.is_empty(), "{waiting:?}");
    assert!(
        waiting
            .waiting
            .as_deref()
            .is_some_and(|w| w.contains("cold-a") && w.contains("cold-b"))
    );

    // A verification of cold-a alone: cold-b's marker is unreadable to it.
    let marker = root_marker::marker_path(&cold(&fixture, "cold-b"));
    let aside = fixture.dir.join("marker-b");
    fs::rename(&marker, &aside).unwrap();
    let (_, out) = verify(&fixture, REBUILT + 2);
    fs::rename(&aside, &marker).unwrap();
    assert!(out.contains("cold-b"), "{out}");
    let waiting = plan(&fixture, REBUILT + 3).unwrap();
    assert!(waiting.candidates.is_empty(), "{waiting:?}");
    assert!(
        waiting
            .waiting
            .as_deref()
            .is_some_and(|w| w.contains("cold-b") && !w.contains("cold-a"))
    );

    let (pass, out) = verify(&fixture, REBUILT + 4);
    assert!(pass, "{out}");
    let planned = plan(&fixture, REBUILT + 5).unwrap();
    assert_eq!(planned.waiting, None);
    assert_eq!(
        planned
            .candidates
            .iter()
            .map(|c| &c.partition)
            .collect::<Vec<_>>(),
        control
            .candidates
            .iter()
            .map(|c| &c.partition)
            .collect::<Vec<_>>()
    );
}

/// The import request of one `metrics` day.
fn metrics_day(day: i64, dry_run: bool) -> ImportRequest {
    ImportRequest {
        from_day: day,
        to_day: day,
        streams: Some(vec![ArchiveStream::Metrics]),
        dry_run,
    }
}

fn run_import(fixture: &Fixture, request: &ImportRequest) -> Result<String, String> {
    let mut out = Vec::new();
    archive_cmd::import(
        &fixture.archive_config,
        request,
        &transports(&fixture.archive),
        &AtomicBool::new(false),
        &mut out,
    )
    .map(|_| String::from_utf8(out.clone()).unwrap())
}

/// The `metrics` file of 2026-10-03 imported into hot, then its entry
/// removed: its parts are the orphans an import that crashed between its
/// seal and its entry leaves. With `pending`, its `PENDING` is written too.
fn interrupted_import(fixture: &Fixture, pending: bool) -> (i64, Vec<String>) {
    let day = day_index("2026-10-03");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(fixture, &metrics_day(day, false)).unwrap();
    let parts: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        // The recall brought 2026-10-03's earlier parts back to hot.
        .filter(|path| !cold(fixture, "cold-a").join(path).exists())
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    let ledgers = LedgerDir::new(&fixture.archive_config.paths.import_ledger);
    let name = ArchiveStream::Metrics.file_name(SYMBOL, Period::Day(day));
    fs::remove_file(ledgers.ledger_path(ArchiveStream::Metrics, &name)).unwrap();
    if pending {
        let path = ArchiveStream::Metrics.archive_path(SYMBOL, Period::Day(day));
        let (_, body) = fixture
            .archive
            .get(&format!("{BASE}/{path}.CHECKSUM"))
            .unwrap();
        let sha = String::from_utf8(body).unwrap()[..64].to_owned();
        ledgers
            .set_pending(&Pending {
                archive: path,
                sha256: sha,
                stream: RawStreamKey::new(ARCHIVE_SOURCE, SYMBOL, "metrics").unwrap(),
                period: Period::Day(day),
            })
            .unwrap();
    }
    (day, parts)
}

/// D7 step 5a: the parts of the import `PENDING` names are not held, and
/// the next import resumes them.
#[test]
fn pending_parts_are_not_held_and_the_next_import_resumes_them() {
    let (_dir, fixture) = adopted("rebuild-pending");
    let (day, parts) = interrupted_import(&fixture, true);
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    assert!(pass, "{out}");
    for part in &parts {
        assert!(out.contains(&format!("{part}: not held")), "{out}");
    }
    assert!(held(&fixture).is_empty());
    let out = run_import(&fixture, &metrics_day(day, false)).unwrap();
    assert!(out.contains("resumed"), "{out}");
}

#[test]
fn without_pending_the_same_parts_are_held_and_the_importer_refuses() {
    let (_dir, fixture) = adopted("rebuild-no-pending");
    let (day, parts) = interrupted_import(&fixture, false);
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    // Without --record-unlisted the parts are named and nothing is written.
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    assert!(!pass && out.contains("nothing written"), "{out}");
    assert!(!hot_ledger(&fixture).exists());
    for part in &parts {
        assert_fail_line(&out, &[part, "--record-unlisted"]);
    }
    let (pass, out) = rebuild(&fixture, record_unlisted(), REBUILT + 1);
    assert!(!pass, "{out}");
    assert_eq!(held(&fixture), parts.iter().cloned().collect());
    let error = run_import(&fixture, &metrics_day(day, false)).unwrap_err();
    assert!(
        error.contains("held") && error.contains(&parts[0]),
        "{error}"
    );
}

/// H1: an open hold refuses the importer, dry runs included, before it
/// opens anything, naming the count, the first held path, its reason and
/// the resolution.
#[test]
fn the_importer_refuses_while_a_hold_is_open() {
    let (_dir, fixture) = adopted("rebuild-importer-hold");
    let part = seal_unlisted_archive_part(&fixture);
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let (pass, out) = rebuild(&fixture, record_unlisted(), REBUILT);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&part, "held (unlisted)"]);
    let before = files(&fixture.dir);
    for dry_run in [true, false] {
        let error =
            run_import(&fixture, &metrics_day(day_index("2026-10-03"), dry_run)).unwrap_err();
        assert!(
            error.contains("1 archive part(s) are held")
                && error.contains(&part)
                && error.contains("unlisted")
                && error.contains("mie store resolve"),
            "{error}"
        );
        assert_eq!(files(&fixture.dir), before, "dry run {dry_run}");
    }
}

/// #192 R6: a ledger copy whose chain verifies but diverges from hot's is
/// refused by the repair, naming the rebuild; the rebuild refuses a
/// readable hot ledger without the flag, decides with it and moves the
/// copy aside; the repair then closes the open copy-ledger finding (#192
/// L2), and a verification and the next pass pass.
#[test]
fn a_diverging_ledger_copy_is_decided_by_the_rebuild() {
    let (_dir, fixture) = adopted("rebuild-diverged");
    let hot = fs::read(hot_ledger(&fixture)).unwrap();
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
    let mut permissions = fs::metadata(&copy).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&copy, permissions).unwrap();
    fs::copy(copy_ledger::ledger_path(&other), &copy).unwrap();
    let diverged = fs::read(&copy).unwrap();
    let (pass, out) = verify(&fixture, D0 + 2 * HOUR);
    assert!(!pass, "{out}");
    let (pass, out) = repair(&fixture, D0 + 3 * HOUR);
    assert!(!pass, "{out}");
    assert_fail_line(
        &out,
        &[
            "cold-a",
            "refused",
            "mie store rebuild --replace-hot-ledger",
        ],
    );

    let (pass, out) = rebuild(&fixture, Flags::default(), D0 + 4 * HOUR);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["readable", "--replace-hot-ledger"]);
    let flags = Flags {
        replace_hot_ledger: true,
        ..Flags::default()
    };
    let (pass, out) = rebuild(&fixture, flags, D0 + 5 * HOUR);
    assert!(pass, "{out}");
    let aside = replica::damaged_dir(&cold(&fixture, "cold-a"), D0 + 5 * HOUR).join("copy-ledger");
    assert_eq!(fs::read(&aside).unwrap(), diverged);
    let linked = replica::damaged_dir(&fixture.hot(), D0 + 5 * HOUR).join("copy-ledger");
    assert!(linked.exists());
    // The start copy holds the verification's open copy-ledger finding on
    // cold-a, which only the repair closes (#192 L2): its R6 finds the copy
    // absent, and its closing pass copies the rebuilt ledger.
    let (pass, out) = replicate(&fixture, D0 + 5 * HOUR + 1);
    assert!(!pass, "{out}");
    assert_fail_line(&out, &["cold-a", "open copy-ledger finding"]);
    let (pass, out) = repair(&fixture, D0 + 6 * HOUR);
    assert!(pass, "{out}");
    let (pass, out) = verify(&fixture, D0 + 7 * HOUR);
    assert!(pass, "{out}");
    assert_pass(&replicate(&fixture, D0 + 7 * HOUR + 1));
}

/// Review of #209, finding 2: a hot import-ledger directory that is
/// missing (an unmounted disk, a wrong `import_ledger`) would read as
/// empty and turn every archive part only hot holds into a permanent
/// `unlisted` hold. The rebuild stops with nothing written instead, and
/// with the directory back it passes with no hold and the importer runs.
#[test]
fn a_missing_import_ledger_stops_the_rebuild() {
    let (_dir, fixture) = adopted("rebuild-missing-import-ledger");
    let day = day_index("2026-10-03");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(&fixture, &metrics_day(day, false)).unwrap();
    let parts: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .filter(|path| !cold(&fixture, "cold-a").join(path).exists())
        .collect();
    assert!(!parts.is_empty());
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let ledger_dir = fixture.archive_config.paths.import_ledger.clone();
    let away = ledger_dir.with_extension("unmounted");
    fs::rename(&ledger_dir, &away).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    fs::rename(&away, &ledger_dir).unwrap();
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&ledger_dir.display().to_string(), "does not exist"]);
    assert!(out.contains("nothing written"), "{out}");
    assert!(!hot_ledger(&fixture).exists());

    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT + 1);
    assert!(pass, "{out}");
    assert!(held(&fixture).is_empty());
    run_import(&fixture, &metrics_day(day_index("2026-10-04"), true)).unwrap();
}

fn record_unlisted() -> Flags {
    Flags {
        record_unlisted: true,
        ..Flags::default()
    }
}

/// Re-review of #209, bypass (a): the import-ledger path exists but is an
/// empty directory, as an unmounted mount point is. Without
/// `--record-unlisted` nothing is installed and the output names the
/// directory, its entry count and the target entries hot lacks, so no
/// `unlisted` hold is written from an absence the operator never saw.
#[test]
fn an_empty_import_ledger_writes_no_unlisted_hold_without_the_flag() {
    let (_dir, fixture) = adopted("rebuild-empty-import-ledger");
    let day = day_index("2026-10-03");
    run_import(&fixture, &metrics_day(day, false)).unwrap();
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let ledger_dir = fixture.archive_config.paths.import_ledger.clone();
    let away = ledger_dir.with_extension("unmounted");
    fs::rename(&ledger_dir, &away).unwrap();
    fs::create_dir(&ledger_dir).unwrap();
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT);
    fs::remove_dir(&ledger_dir).unwrap();
    fs::rename(&away, &ledger_dir).unwrap();
    assert!(!pass, "{out}");
    assert!(out.contains("nothing written"), "{out}");
    assert!(!hot_ledger(&fixture).exists(), "{out}");
    assert_fail_line(
        &out,
        &[
            &ledger_dir.display().to_string(),
            "holds 0 entries",
            "5 entries",
            "--record-unlisted",
        ],
    );
    // With the directory back, the rebuild passes, holds nothing and the
    // importer runs.
    let (pass, out) = rebuild(&fixture, Flags::default(), REBUILT + 1);
    assert!(pass, "{out}");
    assert!(held(&fixture).is_empty());
    run_import(&fixture, &metrics_day(day_index("2026-10-04"), true)).unwrap();
}

/// Re-review of #209, bypass (b): the import-ledger path is a symlink to an
/// unmounted disk. It is followed, so it is missing, and the rebuild stops
/// with nothing written.
#[test]
fn a_dangling_import_ledger_symlink_stops_the_rebuild() {
    let (_dir, fixture) = adopted("rebuild-dangling-import-ledger");
    let day = day_index("2026-10-03");
    run_import(&fixture, &metrics_day(day, false)).unwrap();
    fs::remove_file(hot_ledger(&fixture)).unwrap();
    let ledger_dir = fixture.archive_config.paths.import_ledger.clone();
    let away = ledger_dir.with_extension("unmounted");
    fs::rename(&ledger_dir, &away).unwrap();
    std::os::unix::fs::symlink(ledger_dir.with_extension("disk-not-mounted"), &ledger_dir).unwrap();
    let (pass, out) = rebuild(&fixture, record_unlisted(), REBUILT);
    fs::remove_file(&ledger_dir).unwrap();
    fs::rename(&away, &ledger_dir).unwrap();
    assert!(!pass, "{out}");
    assert_fail_line(&out, &[&ledger_dir.display().to_string(), "does not exist"]);
    assert!(out.contains("nothing written"), "{out}");
    assert!(!hot_ledger(&fixture).exists());
}
