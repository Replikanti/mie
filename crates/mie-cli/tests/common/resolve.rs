//! The `mie store resolve` fixtures shared by its offline tests (#210,
//! #211): the adopted store, the command wrappers, the missing-entry and
//! the conflict states, and the stale and empty rebuild starts.

use super::tiers::*;
use super::{D0, HOUR, MapProbe, TempDir};
use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::archive::catalog::Period;
use mie_adapter_parquet::copy_ledger::{self, HoldReason, Kind};
use mie_adapter_parquet::replica;
use mie_cli::store::{self, rebuild::Flags};
use std::fs;
use std::path::{Path, PathBuf};

/// When the rebuild that writes the holds runs.
pub const REBUILT: i64 = D0 + 3 * HOUR;
/// When resolve runs.
pub const RESOLVED: i64 = D0 + 4 * HOUR;
/// G = 2 × the default seal interval of 300 s (V8).
pub const G: i64 = 600_000;

pub fn adopted(tag: &str) -> (TempDir, Fixture) {
    let dir = TempDir::new(tag);
    let fixture = Fixture::new(dir.path());
    assert_pass(&fixture.adopt(&fixture.request()));
    (dir, fixture)
}

pub fn resolve_with(
    fixture: &Fixture,
    file: Option<&str>,
    probe: &MapProbe,
    archive: Option<&Path>,
    now: i64,
) -> (bool, String) {
    resolve_full(fixture, file, false, probe, archive, now)
}

/// `mie store resolve`, `--replace` with `replace`.
pub fn resolve_full(
    fixture: &Fixture,
    file: Option<&str>,
    replace: bool,
    probe: &MapProbe,
    archive: Option<&Path>,
    now: i64,
) -> (bool, String) {
    let mut out = Vec::new();
    let pass = store::resolve(
        &fixture.config,
        archive,
        file,
        replace,
        &transports(&fixture.archive),
        probe,
        &|| now,
        &mut out,
    )
    .unwrap();
    (pass, String::from_utf8(out).unwrap())
}

pub fn resolve(fixture: &Fixture, file: Option<&str>, now: i64) -> (bool, String) {
    resolve_with(
        fixture,
        file,
        &fixture.probe(),
        Some(&fixture.dst_archive),
        now,
    )
}

pub fn rebuild(fixture: &Fixture, flags: Flags, now: i64) -> (bool, String) {
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

pub fn replicate(fixture: &Fixture, now: i64) -> (bool, String) {
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

pub fn verify(fixture: &Fixture, now: i64) -> (bool, String) {
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

pub fn hot_ledger(fixture: &Fixture) -> PathBuf {
    copy_ledger::ledger_path(&fixture.hot())
}

#[track_caller]
pub fn assert_fail_line(out: &str, needles: &[&str]) {
    assert!(
        out.lines()
            .any(|line| line.starts_with("FAIL: ") && needles.iter().all(|n| line.contains(n))),
        "no FAIL line with {needles:?} in\n{out}"
    );
}

/// The metrics file of 2026-10-03.
pub fn metrics_file() -> String {
    ArchiveStream::Metrics.archive_path(SYMBOL, Period::Day(day_index("2026-10-03")))
}

pub fn metrics_name() -> String {
    ArchiveStream::Metrics.file_name(SYMBOL, Period::Day(day_index("2026-10-03")))
}

/// Hot's metrics import-ledger directory and one of its files.
pub fn ledger_file(fixture: &Fixture, name: &str) -> PathBuf {
    fixture
        .archive_config
        .paths
        .import_ledger
        .join("metrics")
        .join(name)
}

/// A copy target's copy of one metrics import-ledger file.
pub fn target_file(fixture: &Fixture, root: &str, name: &str) -> PathBuf {
    cold(fixture, root)
        .join(".mie/import-ledger/metrics")
        .join(name)
}

/// The missing-entry state: 2026-10-03's metrics imported, its entry
/// removed, the copy ledger rebuilt with `--record-unlisted`, so both of
/// its parts are held `unlisted`. Returns the baseline selections, the
/// parts and the removed entry's bytes.
pub fn missing_entry(fixture: &Fixture) -> (Vec<String>, Vec<String>, Vec<u8>) {
    let (_, parts, entry) = interrupted_import_saving(fixture, false);
    let baseline = selected(fixture);
    fs::remove_file(hot_ledger(fixture)).unwrap();
    let flags = Flags {
        record_unlisted: true,
        ..Flags::default()
    };
    let (pass, out) = rebuild(fixture, flags, REBUILT);
    assert!(!pass, "{out}");
    assert_eq!(held(fixture), parts.iter().cloned().collect(), "{out}");
    (baseline, parts, entry)
}

/// The conflict state: 2026-10-03's metrics imported and replicated, then
/// cold-a's copy of its entry rewritten with another published SHA-256, so
/// both variants list files matching the manifests; hot's copy ledger is
/// lost and the rebuild holds both parts as `entry-conflict`. Returns the
/// pre-conflict selections and the parts.
pub fn conflict(fixture: &Fixture) -> (Vec<String>, Vec<String>) {
    let day = day_index("2026-10-03");
    let before = replica::list_sealed(&fixture.hot()).unwrap();
    run_import(fixture, &metrics_day(day, false)).unwrap();
    let parts: Vec<String> = replica::list_sealed(&fixture.hot())
        .unwrap()
        .into_iter()
        .filter(|path| !before.contains(path))
        .filter(|path| !cold(fixture, "cold-a").join(path).exists())
        .collect();
    assert_eq!(parts.len(), 2, "{parts:?}");
    let (_, out) = replicate(fixture, D0 + 2 * HOUR);
    let e0 = format!("{}.import", metrics_name());
    let copy = target_file(fixture, "cold-a", &e0);
    assert!(copy.exists(), "{out}");
    let baseline = selected(fixture);
    let text = fs::read_to_string(&copy).unwrap();
    let sha = text
        .lines()
        .find_map(|line| line.strip_prefix("sha256 "))
        .unwrap()
        .to_owned();
    let other = format!(
        "{}{}",
        &sha[..63],
        if sha.ends_with('0') { '1' } else { '0' }
    );
    let mut permissions = fs::metadata(&copy).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(&copy, permissions).unwrap();
    fs::write(&copy, text.replace(&sha, &other)).unwrap();
    fs::remove_file(hot_ledger(fixture)).unwrap();
    let (pass, out) = rebuild(fixture, Flags::default(), REBUILT);
    assert!(!pass, "{out}");
    assert_eq!(held(fixture), parts.iter().cloned().collect(), "{out}");
    let ledger = copy_ledger::read(&fixture.hot()).unwrap().unwrap();
    assert!(
        ledger.records.iter().all(|r| match &r.kind {
            Kind::Hold { reason, .. } => *reason == HoldReason::EntryConflict,
            _ => true,
        }),
        "{out}"
    );
    (baseline, parts)
}

/// Damages the hash chain of both copy targets' ledger copies.
pub fn damage_both_copies(fixture: &Fixture) {
    for name in ["cold-a", "cold-b"] {
        let path = copy_ledger::ledger_path(&cold(fixture, name));
        let mut bytes = fs::read(&path).unwrap();
        bytes[copy_ledger::HEADER.len() + 3] ^= 0x01;
        fs::write(&path, bytes).unwrap();
    }
}

/// Cuts both ledger copies at the end of their last `hold` record: a stale
/// start between the rebuild's holds and resolve's clearing.
pub fn cut_after_last_hold(fixture: &Fixture) {
    for name in ["cold-a", "cold-b"] {
        let path = copy_ledger::ledger_path(&cold(fixture, name));
        let bytes = fs::read(&path).unwrap();
        let ledger = copy_ledger::parse(&path, &bytes).unwrap();
        let last = ledger
            .records
            .iter()
            .rposition(|r| matches!(r.kind, Kind::Hold { .. }))
            .unwrap();
        assert!(
            ledger.records[last + 1..]
                .iter()
                .any(|r| matches!(r.kind, Kind::HoldCleared { .. })),
            "the copy holds the clearing"
        );
        fs::write(&path, &bytes[..ledger.ends[last] as usize]).unwrap();
    }
}
