//! The copy primitives: the D5 copy under every crash, the destination
//! states, the immutable copy, the tail append and the pass lock.

use super::*;
use crate::recovery;
use crate::sorted_entries;
use crate::testutil::{TempDir, TieredFixture};

/// [`super::copy_sealed`] with the error only; the side is checked where
/// it matters.
fn copy_sealed(
    source_root: &Path,
    dest_root: &Path,
    data_path: &str,
) -> Result<(CopyOutcome, String), RawStoreError> {
    super::copy_sealed(source_root, dest_root, data_path).map_err(|e| e.error)
}

/// [`super::copy_immutable`] with the error only.
fn copy_immutable(source: &Path, dest: &Path) -> Result<CopyOutcome, RawStoreError> {
    super::copy_immutable(source, dest).map_err(|e| e.error)
}

/// [`super::extend_prefix`] with the error only.
fn extend_prefix(source: TailSource<'_>, dest: &Path, end: u64) -> Result<u64, RawStoreError> {
    super::extend_prefix(source, dest, end).map_err(|e| e.error)
}

/// Every file below `root` with its bytes, and every directory, sorted.
fn snapshot(root: &Path) -> Vec<(String, Option<Vec<u8>>)> {
    fn walk(dir: &Path, prefix: &str, out: &mut Vec<(String, Option<Vec<u8>>)>) {
        for name in sorted_entries(dir).unwrap() {
            let path = dir.join(&name);
            let rel = format!("{prefix}{name}");
            if path.is_dir() {
                out.push((rel.clone(), None));
                walk(&path, &format!("{rel}/"), out);
            } else {
                out.push((rel, Some(fs::read(&path).unwrap())));
            }
        }
    }
    let mut out = Vec::new();
    walk(root, "", &mut out);
    out
}

/// The files of `partition` on `root`, with their bytes.
fn partition_files(root: &Path, partition: &str) -> Vec<(String, Vec<u8>)> {
    let dir = layout::resolve(root, partition);
    sorted_entries(&dir)
        .unwrap()
        .into_iter()
        .map(|name| {
            let bytes = fs::read(dir.join(&name)).unwrap();
            (name, bytes)
        })
        .collect()
}

/// The first sealed part of the fixture and its partition.
fn first_part(fixture: &TieredFixture) -> (String, String) {
    let path = list_sealed(&fixture.hot).unwrap().remove(0);
    let partition = path.rsplit_once('/').unwrap().0.to_owned();
    (path, partition)
}

/// Makes `path` writable again, so a test can damage it.
fn writable(path: &Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
}

/// Flips one bit of the byte at `at` of the file `path`.
fn flip_file(path: &Path, at: usize) {
    writable(path);
    let mut bytes = fs::read(path).unwrap();
    bytes[at] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

/// Copies every entry of `from` into the new directory `to`.
fn copy_dir(from: &Path, to: &Path) {
    fs::create_dir_all(to).unwrap();
    for name in sorted_entries(from).unwrap() {
        fs::copy(from.join(&name), to.join(&name)).unwrap();
    }
}

#[test]
fn list_sealed_lists_the_layout_only() {
    let fixture = TieredFixture::new("replica-list");
    let paths = list_sealed(&fixture.hot).unwrap();
    // Two streams, four dates, three parts each.
    assert_eq!(paths.len(), 24);
    let mut sorted = paths.clone();
    sorted.sort();
    assert_eq!(paths, sorted);
    for path in &paths {
        assert!(layout::parse_data_path(path).is_ok(), "{path}");
    }

    // Names outside the layout are skipped: metadata, quarantine, debris
    // and unknown entries, a stream directory that is a file, and a data
    // file without its manifest (not sealed).
    let hot = &fixture.hot;
    let (_, partition) = first_part(&fixture);
    let dir = layout::resolve(hot, &partition);
    fs::write(dir.join(".part-00042.parquet.tmp"), b"x").unwrap();
    fs::write(dir.join("part-00043.parquet"), b"x").unwrap();
    fs::write(dir.join("README"), b"x").unwrap();
    fs::create_dir_all(hot.join(".quarantine/q-1/source=binance-um")).unwrap();
    fs::create_dir_all(hot.join("source=bad source")).unwrap();
    fs::write(
        hot.join("source=binance-um/instrument=BTCUSDT/stream=notes"),
        b"",
    )
    .unwrap();
    fs::create_dir_all(hot.join("source=binance-um/instrument=BTCUSDT/stream=x/date=2026-13-01"))
        .unwrap();
    assert_eq!(list_sealed(hot).unwrap(), paths);

    // A root without any layout lists nothing.
    assert_eq!(list_sealed(&fixture.cold[0]).unwrap(), Vec::<String>::new());
}

#[test]
fn list_unsealed_names_each_kind_of_seal_debris_and_only_those() {
    let fixture = TieredFixture::new("replica-unsealed");
    let hot = &fixture.hot;
    assert_eq!(list_unsealed(hot).unwrap(), Vec::<String>::new());

    let (_, partition) = first_part(&fixture);
    let dir = layout::resolve(hot, &partition);
    // The three kinds of debris, in two partitions.
    fs::write(dir.join(".part-00042.parquet.tmp"), b"x").unwrap();
    fs::write(dir.join(".part-00043.manifest.tmp"), b"x").unwrap();
    fs::write(dir.join("part-00044.parquet"), b"x").unwrap();
    let last = list_sealed(hot).unwrap().pop().unwrap();
    let other = last.rsplit_once('/').unwrap().0.to_owned();
    fs::write(
        layout::resolve(hot, &other).join(".part-00007.parquet.tmp"),
        b"x",
    )
    .unwrap();
    // Not debris: names the store does not own, a manifest without its
    // data (a sealed state recovery refuses; `list_sealed` lists it), and
    // anything outside the layout.
    fs::write(dir.join("README"), b"x").unwrap();
    fs::write(dir.join("part-00045.manifest"), b"x").unwrap();
    fs::create_dir_all(hot.join(".quarantine/q-1")).unwrap();
    fs::write(hot.join(".quarantine/q-1/.part-00001.parquet.tmp"), b"x").unwrap();
    fs::write(hot.join(".mie/.part-00001.parquet.tmp"), b"x").unwrap();

    let mut expected = vec![
        format!("{partition}/.part-00042.parquet.tmp"),
        format!("{partition}/.part-00043.manifest.tmp"),
        format!("{partition}/part-00044.parquet"),
        format!("{other}/.part-00007.parquet.tmp"),
    ];
    expected.sort();
    assert_eq!(list_unsealed(hot).unwrap(), expected);
    // A root without any layout holds no debris.
    assert_eq!(
        list_unsealed(&fixture.cold[0]).unwrap(),
        Vec::<String>::new()
    );
}

#[test]
fn verify_sealed_refuses_missing_or_differing_data() {
    let fixture = TieredFixture::new("replica-verify");
    let (path, _) = first_part(&fixture);
    let (_, file) = sealed_manifest(&fixture.hot, &path).unwrap().unwrap();
    verify_sealed(&fixture.hot, &file).unwrap();
    let data = layout::resolve(&fixture.hot, &path);
    flip_file(&data, 10);
    let error = verify_sealed(&fixture.hot, &file).unwrap_err();
    assert!(
        matches!(&error, RawStoreError::Integrity(m) if m.contains("does not match its manifest")),
        "{error:?}"
    );
    fs::remove_file(&data).unwrap();
    let error = verify_sealed(&fixture.hot, &file).unwrap_err();
    assert!(
        matches!(&error, RawStoreError::Integrity(m) if m.contains("without its data")),
        "{error:?}"
    );
}

#[test]
fn a_copy_is_byte_identical_and_a_second_copy_only_finds_it() {
    let fixture = TieredFixture::new("replica-copy");
    let (path, partition) = first_part(&fixture);
    let (outcome, text) = copy_sealed(&fixture.hot, &fixture.cold[0], &path).unwrap();
    assert_eq!(outcome, CopyOutcome::Copied);
    assert_eq!(
        text,
        fs::read_to_string(layout::resolve(
            &fixture.hot,
            &TieredFixture::manifest_of(&path)
        ))
        .unwrap()
    );
    let copied = partition_files(&fixture.cold[0], &partition);
    let source: Vec<_> = partition_files(&fixture.hot, &partition)
        .into_iter()
        .filter(|(name, _)| copied.iter().any(|(n, _)| n == name))
        .collect();
    assert_eq!(copied.len(), 2, "data and manifest only: {copied:?}");
    assert_eq!(copied, source);
    for (name, _) in &copied {
        let path = layout::resolve(&fixture.cold[0], &partition).join(name);
        assert!(
            fs::metadata(&path).unwrap().permissions().readonly(),
            "{name}"
        );
    }

    let before = snapshot(&fixture.cold[0]);
    let (outcome, again) = copy_sealed(&fixture.hot, &fixture.cold[0], &path).unwrap();
    assert_eq!((outcome, again), (CopyOutcome::AlreadyPresent, text));
    assert_eq!(snapshot(&fixture.cold[0]), before);
}

/// The acceptance item of #189: a crash at every copy step leaves no
/// destination with a manifest whose data does not verify, the next call
/// completes the copy byte-identically, and writer recovery reads every
/// crashed state as a crashed seal: no part or the sealed part, never
/// `Integrity` (the temp names #191 and #195 rely on).
#[test]
fn a_crash_at_every_copy_step_is_completed_and_recovers_as_a_seal() {
    for step in CopyStep::ALL {
        let fixture = TieredFixture::new("replica-crash");
        let (path, partition) = first_part(&fixture);
        let dest = &fixture.cold[0];
        let (_, _, part) = layout::parse_data_path(&path).unwrap();
        let (_, file) = sealed_manifest(&fixture.hot, &path).unwrap().unwrap();

        CRASH_AFTER.with(|crash| crash.set(Some(step)));
        let crashed = copy_sealed(&fixture.hot, dest, &path);
        assert!(
            matches!(crashed, Err(RawStoreError::Io(_))),
            "{step:?}: {crashed:?}"
        );
        assert_eq!(CRASH_AFTER.with(std::cell::Cell::get), None, "{step:?}");

        // Never a manifest without data that verifies against it.
        let dest_partition = layout::resolve(dest, &partition);
        if dest_partition.join(PartFile::Manifest.name(part)).exists() {
            assert!(
                matches_manifest(&dest_partition.join(PartFile::Data.name(part)), &file).unwrap(),
                "{step:?}"
            );
        }

        // Writer recovery over the same crashed state.
        let scratch = TempDir::new("replica-crash-recovery");
        copy_dir(
            &dest_partition,
            &layout::resolve(scratch.path(), &partition),
        );
        let report = recovery::recover(scratch.path(), "binance-um").unwrap();
        let recovered = partition_files(scratch.path(), &partition);
        let sealed: Vec<_> = partition_files(&fixture.hot, &partition)
            .into_iter()
            .filter(|(name, _)| {
                *name == PartFile::Data.name(part) || *name == PartFile::Manifest.name(part)
            })
            .collect();
        match step {
            CopyStep::WriteData => {
                assert!(recovered.is_empty(), "{step:?}: {recovered:?}");
                assert_eq!(report.discarded.len(), 1, "{step:?}");
            }
            _ => assert_eq!(recovered, sealed, "{step:?}: {report:?}"),
        }

        // The next call completes the copy.
        let (outcome, _) = copy_sealed(&fixture.hot, dest, &path).unwrap();
        let expected = match step {
            CopyStep::PublishManifest | CopyStep::SyncDir => CopyOutcome::AlreadyPresent,
            _ => CopyOutcome::Copied,
        };
        assert_eq!(outcome, expected, "{step:?}");
        assert_eq!(partition_files(dest, &partition), sealed, "{step:?}");
    }
}

/// Damage done to a destination partition.
type Damage<'a> = dyn Fn(&Path) + 'a;

#[test]
fn a_differing_destination_is_integrity_and_stays_untouched() {
    let fixture = TieredFixture::new("replica-differ");
    let (path, partition) = first_part(&fixture);
    let (_, _, part) = layout::parse_data_path(&path).unwrap();
    let manifest_name = PartFile::Manifest.name(part);
    let data_name = PartFile::Data.name(part);

    // One differing manifest byte; differing data under the right
    // manifest; data without a manifest that fails verification.
    let cases: [(&str, &Damage<'_>); 3] = [
        ("manifest", &|dir: &Path| {
            flip_file(&dir.join(&manifest_name), 30)
        }),
        ("data", &|dir: &Path| flip_file(&dir.join(&data_name), 10)),
        ("data without manifest", &|dir: &Path| {
            fs::remove_file(dir.join(&manifest_name)).unwrap();
            flip_file(&dir.join(&data_name), 10);
        }),
    ];
    for (what, damage) in cases {
        let dest = TempDir::new("replica-differ-dest");
        copy_sealed(&fixture.hot, dest.path(), &path).unwrap();
        damage(&layout::resolve(dest.path(), &partition));
        let before = snapshot(dest.path());
        let result = copy_sealed(&fixture.hot, dest.path(), &path);
        let Err(RawStoreError::Integrity(detail)) = &result else {
            panic!("{what}: {result:?}");
        };
        assert!(
            detail.contains(&path) && detail.contains(&dest.path().display().to_string()),
            "{what}: {detail}"
        );
        assert_eq!(snapshot(dest.path()), before, "{what}");
    }

    // A manifest without its data on the destination.
    let dest = TempDir::new("replica-differ-no-data");
    copy_sealed(&fixture.hot, dest.path(), &path).unwrap();
    fs::remove_file(layout::resolve(dest.path(), &partition).join(&data_name)).unwrap();
    assert!(matches!(
        copy_sealed(&fixture.hot, dest.path(), &path),
        Err(RawStoreError::Integrity(_))
    ));
}

#[test]
fn a_source_that_fails_its_own_manifest_is_never_published() {
    let fixture = TieredFixture::new("replica-bad-source");
    let (path, partition) = first_part(&fixture);
    flip_file(&layout::resolve(&fixture.hot, &path), 10);
    let dest = &fixture.cold[0];
    let result = copy_sealed(&fixture.hot, dest, &path);
    let Err(RawStoreError::Integrity(detail)) = &result else {
        panic!("{result:?}");
    };
    assert!(
        detail.contains(&fixture.hot.display().to_string()),
        "{detail}"
    );
    assert_eq!(partition_files(dest, &partition), Vec::new());

    // A source without its manifest or without its data.
    let fixture = TieredFixture::new("replica-missing-source");
    let (path, _) = first_part(&fixture);
    fs::remove_file(layout::resolve(&fixture.hot, &path)).unwrap();
    assert!(matches!(
        copy_sealed(&fixture.hot, &fixture.cold[0], &path),
        Err(RawStoreError::Integrity(_))
    ));
    fs::remove_file(layout::resolve(
        &fixture.hot,
        &TieredFixture::manifest_of(&path),
    ))
    .unwrap();
    assert!(matches!(
        copy_sealed(&fixture.hot, &fixture.cold[0], &path),
        Err(RawStoreError::Integrity(_))
    ));
    // A path outside the layout.
    assert!(matches!(
        copy_sealed(&fixture.hot, &fixture.cold[0], ".mie/root"),
        Err(RawStoreError::Invalid(_))
    ));
}

#[test]
fn the_immutable_copy_survives_a_crash_at_every_step() {
    let dir = TempDir::new("replica-immutable");
    let source = dir
        .path()
        .join("hot/aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import");
    fs::create_dir_all(source.parent().unwrap()).unwrap();
    fs::write(&source, b"mie-archive-import 1\narchive x\n").unwrap();
    let steps = [
        CopyStep::WriteData,
        CopyStep::Verify,
        CopyStep::MarkReadOnly,
        CopyStep::PublishData,
        CopyStep::SyncDir,
    ];
    for step in steps {
        let dest = dir.path().join(format!(
            "cold-{step:?}/.mie/import-ledger/aggTrades/entry.import"
        ));
        CRASH_AFTER.with(|crash| crash.set(Some(step)));
        assert!(
            matches!(copy_immutable(&source, &dest), Err(RawStoreError::Io(_))),
            "{step:?}"
        );
        assert_eq!(CRASH_AFTER.with(std::cell::Cell::get), None, "{step:?}");
        // Absent or complete, never partial.
        if dest.exists() {
            assert_eq!(fs::read(&dest).unwrap(), fs::read(&source).unwrap());
        }
        let outcome = copy_immutable(&source, &dest).unwrap();
        let expected = if matches!(step, CopyStep::PublishData | CopyStep::SyncDir) {
            CopyOutcome::AlreadyPresent
        } else {
            CopyOutcome::Copied
        };
        assert_eq!(outcome, expected, "{step:?}");
        assert_eq!(fs::read(&dest).unwrap(), fs::read(&source).unwrap());
        assert!(fs::metadata(&dest).unwrap().permissions().readonly());
        assert_eq!(
            sorted_entries(dest.parent().unwrap()).unwrap(),
            ["entry.import"],
            "{step:?}: no temp file is left"
        );
    }

    // A differing destination is refused and left alone.
    let dest = dir.path().join("cold-differ/entry.import");
    fs::create_dir_all(dest.parent().unwrap()).unwrap();
    fs::write(&dest, b"mie-archive-import 1\narchive y\n").unwrap();
    assert!(matches!(
        copy_immutable(&source, &dest),
        Err(RawStoreError::Integrity(_))
    ));
    assert_eq!(
        fs::read(&dest).unwrap(),
        b"mie-archive-import 1\narchive y\n"
    );
}

/// Three journal-like lines and a partial fourth.
const TAIL_SOURCE: &[u8] = b"{\"type\":\"run_start\",\"run_id\":\"r1\"}\n\
{\"type\":\"sealed\",\"path\":\"source=binance-um/a\"}\n\
{\"type\":\"sealed\",\"path\":\"source=binance-um/b\"}\n\
{\"type\":\"run_e";

#[test]
fn a_tail_append_extends_a_byte_prefix_across_crashes() {
    let dir = TempDir::new("replica-tail");
    let source = dir.path().join("journal.jsonl");
    fs::write(&source, TAIL_SOURCE).unwrap();
    let end = TAIL_SOURCE.len() as u64;

    // A missing copy is created; a crash mid-write leaves a byte prefix,
    // which the next call extends.
    let dest = dir.path().join("cold/.mie/journal/journal.jsonl");
    TAIL_CRASH_AFTER.with(|crash| crash.set(Some(TailStep::PartialWrite)));
    assert!(matches!(
        extend_prefix(TailSource::File(&source), &dest, end),
        Err(RawStoreError::Io(_))
    ));
    let partial = fs::read(&dest).unwrap();
    assert!(!partial.is_empty() && partial.len() < TAIL_SOURCE.len());
    assert_eq!(partial, TAIL_SOURCE[..partial.len()]);
    assert_eq!(
        extend_prefix(TailSource::File(&source), &dest, end).unwrap(),
        end - partial.len() as u64
    );
    assert_eq!(fs::read(&dest).unwrap(), TAIL_SOURCE);
    // At the end it only compares.
    assert_eq!(
        extend_prefix(TailSource::File(&source), &dest, end).unwrap(),
        0
    );

    // A crash after the fsync, before the read-back: complete.
    let dest = dir.path().join("cold-sync/journal.jsonl");
    TAIL_CRASH_AFTER.with(|crash| crash.set(Some(TailStep::Sync)));
    assert!(extend_prefix(TailSource::File(&source), &dest, end).is_err());
    assert_eq!(fs::read(&dest).unwrap(), TAIL_SOURCE);
    assert_eq!(
        extend_prefix(TailSource::File(&source), &dest, end).unwrap(),
        0
    );

    // Bytes in memory as the source, in steps.
    let dest = dir.path().join("cold-bytes/copy-ledger");
    for end in [10, 40, 41, TAIL_SOURCE.len() as u64] {
        extend_prefix(TailSource::Bytes(TAIL_SOURCE), &dest, end).unwrap();
        assert_eq!(fs::read(&dest).unwrap(), TAIL_SOURCE[..end as usize]);
    }
}

#[test]
fn a_damaged_tail_or_a_longer_copy_is_refused_untouched() {
    let dir = TempDir::new("replica-tail-damage");
    let lines = &TAIL_SOURCE[..TAIL_SOURCE.iter().rposition(|&b| b == b'\n').unwrap() + 1];
    let last_start = lines[..lines.len() - 1]
        .iter()
        .rposition(|&b| b == b'\n')
        .unwrap()
        + 1;
    let end = TAIL_SOURCE.len() as u64;
    // A flipped byte in the copy's last record, in its partial tail, and a
    // copy that is longer than the prefix it copies.
    let cases: [(&str, Vec<u8>, u64); 3] = [
        ("last record", flipped(lines, last_start + 3), end),
        (
            "partial tail",
            flipped(&TAIL_SOURCE[..lines.len() + 4], lines.len() + 2),
            end,
        ),
        ("longer", TAIL_SOURCE.to_vec(), lines.len() as u64),
    ];
    for (what, copy, end) in cases {
        let dest = dir.path().join(format!("{what}.jsonl"));
        fs::write(&dest, &copy).unwrap();
        let result = extend_prefix(TailSource::Bytes(TAIL_SOURCE), &dest, end);
        let Err(RawStoreError::Integrity(detail)) = &result else {
            panic!("{what}: {result:?}");
        };
        assert!(
            detail.contains(&dest.display().to_string()),
            "{what}: {detail}"
        );
        assert_eq!(fs::read(&dest).unwrap(), copy, "{what}: untouched");
    }
    // The offset of the first differing byte is named.
    let dest = dir.path().join("last record.jsonl");
    let detail = extend_prefix(TailSource::Bytes(TAIL_SOURCE), &dest, end)
        .unwrap_err()
        .to_string();
    assert!(
        detail.contains(&format!("byte {}", last_start + 3)),
        "{detail}"
    );
}

/// `bytes` with one bit of the byte at `at` flipped.
fn flipped(bytes: &[u8], at: usize) -> Vec<u8> {
    let mut bytes = bytes.to_vec();
    bytes[at] ^= 0x01;
    bytes
}

#[test]
fn the_replication_lock_refuses_a_second_holder() {
    let fixture = TieredFixture::new("replica-lock");
    let lock = try_replication_lock(&fixture.hot).unwrap();
    assert!(matches!(
        try_replication_lock(&fixture.hot),
        Err(RawStoreError::Locked(_))
    ));
    drop(lock);
    let again = try_replication_lock(&fixture.hot).unwrap();
    drop(again);
    assert!(replication_lock_path(&fixture.hot).exists());
}

/// #220: a pass lock dropped while another descriptor of its open file
/// description is open (a child process started by another thread, between
/// its fork and its exec) is free at once; while the guard lives, the lock
/// stays held after that descriptor closes.
#[test]
fn the_replication_lock_is_free_after_its_drop_beside_a_duplicate() {
    let dir = TempDir::new("replica-lock-duplicate");
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    let held = try_replication_lock(dir.path()).unwrap();
    drop(held.duplicate_lock());
    assert!(matches!(
        try_replication_lock(dir.path()),
        Err(RawStoreError::Locked(_))
    ));
    let child = held.duplicate_lock();
    drop(held);
    let again = try_replication_lock(dir.path());
    assert!(again.is_ok(), "{again:?}");
    drop(child);
}

/// Review of #199, finding 2: a failure names the side it happened on, so
/// the pass can tell one unreadable hot file from a failing target.
#[test]
fn every_failure_names_its_side() {
    use std::os::unix::fs::PermissionsExt;
    let side = |result: Result<(), CopyError>| result.unwrap_err().side;

    // The source: unreadable, failing its manifest, missing.
    let fixture = TieredFixture::new("replica-side");
    let (path, _) = first_part(&fixture);
    let data = layout::resolve(&fixture.hot, &path);
    fs::set_permissions(&data, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&data).is_err() {
        assert_eq!(
            side(super::copy_sealed(&fixture.hot, &fixture.cold[0], &path).map(|_| ())),
            Side::Source
        );
    }
    fs::set_permissions(&data, fs::Permissions::from_mode(0o444)).unwrap();
    flip_file(&data, 10);
    assert_eq!(
        side(super::copy_sealed(&fixture.hot, &fixture.cold[0], &path).map(|_| ())),
        Side::Source
    );
    let missing = fixture.hot.join("missing.import");
    let dest = fixture.cold[0].join(".mie/import-ledger/a/missing.import");
    assert_eq!(
        side(super::copy_immutable(&missing, &dest).map(|_| ())),
        Side::Source
    );
    let journal = fixture.hot.join("journal.jsonl");
    fs::write(&journal, TAIL_SOURCE).unwrap();
    let copy = fixture.cold[0].join(".mie/journal/journal.jsonl");
    assert_eq!(
        side(super::extend_prefix(TailSource::File(&journal), &copy, 10_000).map(|_| ())),
        Side::Source
    );

    // The destination: a differing copy of a part, of an entry, of a tail.
    let fixture = TieredFixture::new("replica-side-dest");
    let (path, partition) = first_part(&fixture);
    super::copy_sealed(&fixture.hot, &fixture.cold[0], &path).unwrap();
    let (_, _, part) = layout::parse_data_path(&path).unwrap();
    flip_file(
        &layout::resolve(&fixture.cold[0], &partition).join(PartFile::Data.name(part)),
        10,
    );
    assert_eq!(
        side(super::copy_sealed(&fixture.hot, &fixture.cold[0], &path).map(|_| ())),
        Side::Destination
    );
    let entry = fixture.hot.join("entry.import");
    fs::write(&entry, b"a").unwrap();
    let dest = fixture.cold[0].join("entry.import");
    fs::write(&dest, b"b").unwrap();
    assert_eq!(
        side(super::copy_immutable(&entry, &dest).map(|_| ())),
        Side::Destination
    );
    let copy = fixture.cold[0].join("journal.jsonl");
    fs::write(&copy, flipped(TAIL_SOURCE, TAIL_SOURCE.len() - 2)).unwrap();
    assert_eq!(
        side(
            super::extend_prefix(
                TailSource::Bytes(TAIL_SOURCE),
                &copy,
                TAIL_SOURCE.len() as u64
            )
            .map(|_| ())
        ),
        Side::Destination
    );
}

/// Every class of [`check_sealed_copy`] (#192 V4), in its precedence, the
/// data always checked against the ledger's text.
#[test]
fn check_sealed_copy_classifies_every_copy() {
    let fixture = TieredFixture::new("replica-check");
    let (path, _) = first_part(&fixture);
    let manifest = TieredFixture::manifest_of(&path);
    let text = fs::read_to_string(layout::resolve(&fixture.hot, &manifest)).unwrap();
    let [a, _] = &fixture.cold;
    let check = |root: &Path| check_sealed_copy(root, &path, &text).unwrap();
    assert_eq!(check(a), CopyCheck::Missing);
    copy_sealed(&fixture.hot, a, &path).unwrap();
    assert_eq!(check(a), CopyCheck::Good);
    assert_eq!(check(&fixture.hot), CopyCheck::Good);

    let data = layout::resolve(a, &path);
    let manifest_file = layout::resolve(a, &manifest);
    let original = fs::read(&data).unwrap();
    flip_file(&data, 30);
    assert_eq!(check(a), CopyCheck::Sha256);
    fs::write(&data, [original.as_slice(), b"x"].concat()).unwrap();
    assert_eq!(check(a), CopyCheck::Size);
    fs::write(&data, &original).unwrap();

    // Another manifest: one row more, so the data still matches it (a
    // self-consistent other file), or another SHA-256 (not).
    writable(&manifest_file);
    let mut other = manifest::parse(&text).unwrap();
    other.rows += 1;
    fs::write(&manifest_file, manifest::render(&other)).unwrap();
    assert_eq!(check(a), CopyCheck::Manifest { other_file: true });
    other.sha256 = "0".repeat(64);
    fs::write(&manifest_file, manifest::render(&other)).unwrap();
    assert_eq!(check(a), CopyCheck::Manifest { other_file: false });
    fs::remove_file(&manifest_file).unwrap();
    assert_eq!(check(a), CopyCheck::Manifest { other_file: false });
    // Missing comes first: a manifest without its data.
    fs::write(&manifest_file, &text).unwrap();
    fs::remove_file(&data).unwrap();
    assert_eq!(check(a), CopyCheck::Missing);
    // A directory where the data should be is an I/O error, not a finding.
    fs::create_dir(&data).unwrap();
    assert!(matches!(
        check_sealed_copy(a, &path, &text),
        Err(RawStoreError::Io(_))
    ));
}

/// The move-aside renames the data first, then its manifest and the seal
/// temp names, keeps the bytes and the relative path, and refuses to
/// replace what an earlier run moved.
#[test]
fn move_aside_keeps_the_bytes_data_first() {
    let fixture = TieredFixture::new("replica-move-aside");
    let (path, partition) = first_part(&fixture);
    let [a, _] = &fixture.cold;
    copy_sealed(&fixture.hot, a, &path).unwrap();
    let part = layout::parse_data_path(&path).unwrap().2;
    let tmp = layout::resolve(a, &partition).join(PartFile::DataTmp.name(part));
    fs::write(&tmp, b"debris").unwrap();
    let before = partition_files(a, &partition);
    let aside = damaged_dir(a, 7);
    assert!(aside.ends_with(".mie/damaged/7"));
    let moved = move_aside(a, &path, &aside).unwrap();
    let names: Vec<String> = [PartFile::Data, PartFile::Manifest, PartFile::DataTmp]
        .iter()
        .map(|kind| format!("{partition}/{}", kind.name(part)))
        .collect();
    assert_eq!(moved, names);
    assert!(partition_files(a, &partition).is_empty());
    assert_eq!(partition_files(&aside, &partition), before);
    // Nothing left to move; a second copy moved into the same directory
    // is refused rather than replacing the first.
    assert!(move_aside(a, &path, &aside).unwrap().is_empty());
    copy_sealed(&fixture.hot, a, &path).unwrap();
    assert!(matches!(
        move_aside(a, &path, &aside),
        Err(RawStoreError::Integrity(_))
    ));
}

/// #211: the quarantine move completes a half-moved part (data moved, the
/// manifest not) and never overwrites a quarantined copy; the listing
/// reads every id directory, names a bad id and refuses a `.quarantine`
/// that is no directory.
#[test]
fn quarantine_moves_and_listing() {
    let fixture = TieredFixture::new("replica-quarantine");
    let (path, partition) = first_part(&fixture);
    let hot = &fixture.hot;
    let text = fs::read_to_string(layout::resolve(
        hot,
        &format!("{}.manifest", path.strip_suffix(".parquet").unwrap()),
    ))
    .unwrap();
    let dir = quarantine_dir(hot, "q-7");
    assert!(dir.ends_with(".quarantine/q-7"));
    // A stop after the data's rename: the manifest is still in place.
    let part = layout::parse_data_path(&path).unwrap().2;
    let data = layout::resolve(hot, &partition).join(PartFile::Data.name(part));
    let dest = layout::resolve(&dir, &partition);
    fs::create_dir_all(&dest).unwrap();
    fs::rename(&data, dest.join(PartFile::Data.name(part))).unwrap();
    let listing = list_quarantine(hot).unwrap();
    assert_eq!(
        listing.ids["q-7"][&path],
        QuarantinedCopy {
            text: None,
            data: true,
            unreadable: None,
        }
    );
    // The resume completes it.
    let moved = move_aside(hot, &path, &dir).unwrap();
    assert_eq!(
        moved,
        vec![format!("{partition}/{}", PartFile::Manifest.name(part))]
    );
    let listing = list_quarantine(hot).unwrap();
    assert_eq!(
        listing.ids["q-7"][&path],
        QuarantinedCopy {
            text: Some(text),
            data: true,
            unreadable: None,
        }
    );
    // A name in both places: `Integrity`, nothing overwritten.
    let before = partition_files(&dir, &partition);
    fs::create_dir_all(layout::resolve(hot, &partition)).unwrap();
    fs::write(data.clone(), b"another").unwrap();
    assert!(matches!(
        move_aside(hot, &path, &dir),
        Err(RawStoreError::Integrity(_))
    ));
    assert_eq!(partition_files(&dir, &partition), before);
    assert_eq!(fs::read(&data).unwrap(), b"another");
    // Bad ids are named and count for nothing.
    for bad in ["q\u{2010}1", "q-1 ", "q\u{200b}-1"] {
        fs::create_dir_all(hot.join(QUARANTINE_DIR).join(bad)).unwrap();
    }
    let listing = list_quarantine(hot).unwrap();
    assert_eq!(listing.bad_names.len(), 3, "{listing:?}");
    assert_eq!(listing.ids.keys().collect::<Vec<_>>(), vec!["q-7"]);
    // A root without `.quarantine/` lists empty; a file there is refused.
    let [a, _] = &fixture.cold;
    assert_eq!(list_quarantine(a).unwrap(), QuarantineListing::default());
    fs::write(a.join(QUARANTINE_DIR), b"").unwrap();
    assert!(matches!(
        list_quarantine(a),
        Err(RawStoreError::Integrity(_))
    ));
}

/// The first difference, the start of a line, and the rebuild of a tail
/// copy, across a crash after either step.
#[test]
fn a_tail_copy_is_rebuilt_from_a_prefix_across_crashes() {
    let dir = TempDir::new("replica-rebuild");
    let source = dir.path().join("journal.jsonl");
    let text = b"{\"a\":1}\n{\"b\":22}\n{\"c\":333}\n";
    fs::write(&source, text).unwrap();
    let copy = dir.path().join("cold/.mie/journal/journal.jsonl");
    fs::create_dir_all(copy.parent().unwrap()).unwrap();
    let mut damaged = text.to_vec();
    damaged[12] ^= 0x01;
    fs::write(&copy, &damaged).unwrap();
    let len = text.len() as u64;
    assert_eq!(
        first_difference(&copy, TailSource::File(&source), len).unwrap(),
        Some(12)
    );
    assert_eq!(
        first_difference(&source, TailSource::Bytes(text), len).unwrap(),
        None
    );
    assert_eq!(line_start(TailSource::File(&source), 12).unwrap(), 8);
    assert_eq!(line_start(TailSource::Bytes(text), 8).unwrap(), 8);
    assert_eq!(line_start(TailSource::Bytes(text), 3).unwrap(), 0);

    for (i, step) in [
        Some(RebuildStep::MoveAside),
        Some(RebuildStep::WriteTemp),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        fs::write(&copy, &damaged).unwrap();
        let aside = dir
            .path()
            .join(format!("cold/.mie/damaged/{i}/journal.jsonl"));
        REBUILD_CRASH_AFTER.with(|crash| crash.set(step));
        let result = rebuild_prefix(TailSource::File(&source), &copy, 8, &aside);
        assert_eq!(fs::read(&aside).unwrap(), damaged, "{step:?}");
        if step.is_some() {
            assert!(result.is_err(), "{step:?}");
            // The copy is absent (the next pass creates it), and a re-run
            // removes the temp name and rebuilds.
            assert!(!copy.exists(), "{step:?}");
            let again = dir
                .path()
                .join(format!("cold/.mie/damaged/{i}-again/journal.jsonl"));
            rebuild_prefix(TailSource::File(&source), &copy, 8, &again).unwrap();
            assert!(!again.exists(), "nothing was left to move aside");
        } else {
            result.unwrap();
        }
        assert_eq!(fs::read(&copy).unwrap(), &text[..8], "{step:?}");
        let tmp = copy.with_file_name(".journal.jsonl.rebuild.tmp");
        assert!(!tmp.exists(), "{step:?}");
    }
}

/// The verification lock refuses a second holder.
#[test]
fn the_verify_lock_refuses_a_second_holder() {
    let dir = TempDir::new("replica-verify-lock");
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    let held = try_verify_lock(dir.path()).unwrap();
    assert!(matches!(
        try_verify_lock(dir.path()),
        Err(RawStoreError::Locked(_))
    ));
    drop(held);
    try_verify_lock(dir.path()).unwrap();
}

/// #220: the verification lock, as
/// `the_replication_lock_is_free_after_its_drop_beside_a_duplicate`.
#[test]
fn the_verify_lock_is_free_after_its_drop_beside_a_duplicate() {
    let dir = TempDir::new("replica-verify-lock-duplicate");
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    let held = try_verify_lock(dir.path()).unwrap();
    drop(held.duplicate_lock());
    assert!(matches!(
        try_verify_lock(dir.path()),
        Err(RawStoreError::Locked(_))
    ));
    let child = held.duplicate_lock();
    drop(held);
    let again = try_verify_lock(dir.path());
    assert!(again.is_ok(), "{again:?}");
    drop(child);
}

/// #195 H6: a cold copy whose manifest text differs from the ledger's
/// stops the copy back with [`OtherText::Stop`] (the recall, the repair),
/// and with [`OtherText::PassOver`] (the hot restore) it is a damaged copy
/// handed to `observe`, the next root supplying the file.
#[test]
fn copy_back_stops_or_passes_over_another_text() {
    let fixture = TieredFixture::new("replica-copy-back-text");
    let (path, partition) = first_part(&fixture);
    let text = fs::read_to_string(layout::resolve(
        &fixture.hot,
        &TieredFixture::manifest_of(&path),
    ))
    .unwrap();
    for root in &fixture.cold {
        copy_sealed(&fixture.hot, root, &path).unwrap();
    }
    // cold-a's manifest carries another text (one byte of its rows).
    let other = text.replacen("rows 2", "rows 3", 1);
    assert_ne!(other, text);
    let manifest = layout::resolve(&fixture.cold[0], &TieredFixture::manifest_of(&path));
    writable(&manifest);
    fs::write(&manifest, &other).unwrap();
    let hot = TempDir::new("replica-copy-back-text-hot");
    let roots = fixture.cold.to_vec();

    let mut seen = Vec::new();
    let error = copy_back(
        &roots,
        hot.path(),
        &path,
        &text,
        OtherText::Stop,
        SourceForms::Sealed,
        &mut |d| seen.push(d.clone()),
    )
    .unwrap_err();
    assert!(
        matches!(&error, RawStoreError::Integrity(m) if m.contains("the recall stops")),
        "{error}"
    );
    assert!(seen.is_empty());
    assert!(!layout::resolve(hot.path(), &partition).exists());

    let copied = copy_back(
        &roots,
        hot.path(),
        &path,
        &text,
        OtherText::PassOver,
        SourceForms::Sealed,
        &mut |d| seen.push(d.clone()),
    )
    .unwrap();
    assert_eq!(
        (copied.root.as_path(), copied.outcome),
        (fixture.cold[1].as_path(), CopyOutcome::Copied)
    );
    assert_eq!(seen.len(), 1);
    assert_eq!(seen[0].root, fixture.cold[0]);
    assert!(
        matches!(&seen[0].error, RawStoreError::Integrity(m) if m.contains("damaged copy")),
        "{:?}",
        seen[0]
    );
    assert_eq!(copied.damaged, seen);
    assert_eq!(
        fs::read_to_string(layout::resolve(
            hot.path(),
            &TieredFixture::manifest_of(&path)
        ))
        .unwrap(),
        text
    );
}

// ------------------------------- crash forms as copy-back sources (#195 B6a)

/// Every file and directory below `root` with its bytes and its mode bits.
fn snapshot_modes(root: &Path) -> Vec<(String, Option<Vec<u8>>, u32)> {
    use std::os::unix::fs::PermissionsExt;
    snapshot(root)
        .into_iter()
        .map(|(rel, bytes)| {
            let mode = fs::metadata(layout::resolve(root, &rel))
                .unwrap()
                .permissions()
                .mode();
            (rel, bytes, mode)
        })
        .collect()
}

/// The sealed data and manifest of `path` on the fixture's hot, by name.
fn sealed_pair(fixture: &TieredFixture, path: &str, partition: &str) -> Vec<(String, Vec<u8>)> {
    let (_, _, part) = layout::parse_data_path(path).unwrap();
    partition_files(&fixture.hot, partition)
        .into_iter()
        .filter(|(name, _)| {
            *name == PartFile::Data.name(part) || *name == PartFile::Manifest.name(part)
        })
        .collect()
}

/// One `copy_back` of `path` from `cold` onto a new, empty hot with
/// `forms`; asserts that `cold` is unchanged (names, bytes, modes) and
/// returns whether it copied the sealed pair byte-identically.
fn roll_forward(
    fixture: &TieredFixture,
    cold: &Path,
    path: &str,
    partition: &str,
    forms: SourceForms,
    case: &str,
) -> bool {
    let text = fs::read_to_string(layout::resolve(
        &fixture.hot,
        &TieredFixture::manifest_of(path),
    ))
    .unwrap();
    let before = snapshot_modes(cold);
    let hot = TempDir::new("replica-roll-forward-hot");
    let result = copy_back(
        &[cold.to_path_buf()],
        hot.path(),
        path,
        &text,
        OtherText::PassOver,
        forms,
        &mut |_| {},
    );
    assert_eq!(snapshot_modes(cold), before, "{case} {forms:?}");
    match result {
        Ok(copied) => {
            assert_eq!(copied.outcome, CopyOutcome::Copied, "{case} {forms:?}");
            assert_eq!(
                partition_files(hot.path(), partition),
                sealed_pair(fixture, path, partition),
                "{case} {forms:?}"
            );
            true
        }
        Err(error) => {
            assert!(
                matches!(&error, RawStoreError::Integrity(m) if m.contains("no cold root holds a verified copy")),
                "{case} {forms:?}: {error}"
            );
            let dir = layout::resolve(hot.path(), partition);
            assert!(
                !dir.exists() || partition_files(hot.path(), partition).is_empty(),
                "{case} {forms:?}"
            );
            false
        }
    }
}

/// #195 H6 i: a D5 copy to cold crashed after each of its steps, then a
/// copy back. `CrashForms` rolls every state forward that holds a text and
/// verified data (`M`, else `M~`; `D`, else `D~`); `Sealed` keeps today's
/// behaviour and copies only a published manifest with its data. The same
/// for the copy's data-present path and for the reordered state `M` +
/// `D~` that a power loss can leave.
#[test]
fn copy_back_rolls_forward_from_every_copy_step() {
    for step in CopyStep::ALL {
        for forms in [SourceForms::Sealed, SourceForms::CrashForms] {
            let fixture = TieredFixture::new("replica-roll-forward");
            let (path, partition) = first_part(&fixture);
            let cold = fixture.cold[0].clone();
            CRASH_AFTER.with(|crash| crash.set(Some(step)));
            assert!(copy_sealed(&fixture.hot, &cold, &path).is_err(), "{step:?}");
            let expected = match step {
                CopyStep::WriteData => false,
                CopyStep::PublishManifest | CopyStep::SyncDir => true,
                _ => forms == SourceForms::CrashForms,
            };
            let case = format!("fresh path, {step:?}");
            assert_eq!(
                roll_forward(&fixture, &cold, &path, &partition, forms, &case),
                expected,
                "{case} {forms:?}"
            );
        }
    }
    // The data-present path: the data is in place, the manifest is
    // published through its temp name.
    for step in [
        CopyStep::WriteManifest,
        CopyStep::Verify,
        CopyStep::MarkReadOnly,
        CopyStep::PublishManifest,
    ] {
        for forms in [SourceForms::Sealed, SourceForms::CrashForms] {
            let fixture = TieredFixture::new("replica-roll-forward-present");
            let (path, partition) = first_part(&fixture);
            let cold = fixture.cold[0].clone();
            fs::create_dir_all(layout::resolve(&cold, &partition)).unwrap();
            fs::copy(
                layout::resolve(&fixture.hot, &path),
                layout::resolve(&cold, &path),
            )
            .unwrap();
            CRASH_AFTER.with(|crash| crash.set(Some(step)));
            assert!(copy_sealed(&fixture.hot, &cold, &path).is_err(), "{step:?}");
            let expected = step == CopyStep::PublishManifest || forms == SourceForms::CrashForms;
            let case = format!("data present, {step:?}");
            assert_eq!(
                roll_forward(&fixture, &cold, &path, &partition, forms, &case),
                expected,
                "{case} {forms:?}"
            );
        }
    }
    // The reordered state: the manifest's rename survived, the data's did
    // not.
    for forms in [SourceForms::Sealed, SourceForms::CrashForms] {
        let fixture = TieredFixture::new("replica-roll-forward-reordered");
        let (path, partition) = first_part(&fixture);
        let cold = fixture.cold[0].clone();
        fs::create_dir_all(layout::resolve(&cold, &partition)).unwrap();
        fs::copy(
            layout::resolve(&fixture.hot, &path),
            temp_path(&cold, &path, TempName::Data).unwrap(),
        )
        .unwrap();
        fs::copy(
            layout::resolve(&fixture.hot, &TieredFixture::manifest_of(&path)),
            layout::resolve(&cold, &TieredFixture::manifest_of(&path)),
        )
        .unwrap();
        assert_eq!(
            roll_forward(&fixture, &cold, &path, &partition, forms, "reordered"),
            forms == SourceForms::CrashForms,
            "reordered {forms:?}"
        );
    }
}

/// #195 H6 i: the copy back never writes a cold root, whichever forms it
/// reads and whether it copies, passes a root over or fails: names, bytes
/// and modes stay equal on every root.
#[test]
fn copy_back_never_writes_a_cold_root() {
    let fixture = TieredFixture::new("replica-copy-back-read-only");
    let paths = list_sealed(&fixture.hot).unwrap();
    let (cold_a, cold_b) = (&fixture.cold[0], &fixture.cold[1]);
    // cold-a: the first part as `D~` + `M~` (a copy crashed after
    // `MarkReadOnly`), the second with flipped data; cold-b: the second
    // sealed, the third as `D~` one byte short beside its `M~`.
    CRASH_AFTER.with(|crash| crash.set(Some(CopyStep::MarkReadOnly)));
    assert!(copy_sealed(&fixture.hot, cold_a, &paths[0]).is_err());
    copy_sealed(&fixture.hot, cold_a, &paths[1]).unwrap();
    flip_file(&layout::resolve(cold_a, &paths[1]), 0);
    copy_sealed(&fixture.hot, cold_b, &paths[1]).unwrap();
    CRASH_AFTER.with(|crash| crash.set(Some(CopyStep::MarkReadOnly)));
    assert!(copy_sealed(&fixture.hot, cold_b, &paths[2]).is_err());
    let short = temp_path(cold_b, &paths[2], TempName::Data).unwrap();
    writable(&short);
    let bytes = fs::read(&short).unwrap();
    fs::write(&short, &bytes[..bytes.len() - 1]).unwrap();
    let roots = vec![cold_a.clone(), cold_b.clone()];
    let before = (snapshot_modes(cold_a), snapshot_modes(cold_b));
    for forms in [SourceForms::Sealed, SourceForms::CrashForms] {
        for path in &paths[..3] {
            let text = fs::read_to_string(layout::resolve(
                &fixture.hot,
                &TieredFixture::manifest_of(path),
            ))
            .unwrap();
            let hot = TempDir::new("replica-copy-back-read-only-hot");
            let _ = copy_back(
                &roots,
                hot.path(),
                path,
                &text,
                OtherText::PassOver,
                forms,
                &mut |_| {},
            );
            assert_eq!(
                (snapshot_modes(cold_a), snapshot_modes(cold_b)),
                before,
                "{path} {forms:?}"
            );
        }
    }
}

/// #195 B6a inputs: `list_temps` names each part with a data temp file
/// or a pending manifest (its text, `Some(None)` when it is not UTF-8),
/// `list_evidence` each part a name below `.mie/damaged/<dir>/` names, by
/// directory; look-alike names, an empty `date=` directory (row 14) and
/// everything outside the layout count for nothing.
#[test]
fn list_temps_and_evidence_classify_every_name() {
    let root = TempDir::new("replica-temps");
    let root = root.path();
    let partition = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-06";
    let path = |n: u32| format!("{partition}/part-{n:05}.parquet");
    let dir = layout::resolve(root, partition);
    fs::create_dir_all(&dir).unwrap();
    for (name, bytes) in [
        (".part-00042.parquet.tmp", b"d".to_vec()),
        (".part-00043.manifest.tmp", b"text 43".to_vec()),
        (".part-00044.manifest.tmp", vec![0xff, 0xfe]),
        (".part-00045.parquet.tmp", b"d".to_vec()),
        (".part-00045.manifest.tmp", b"text 45".to_vec()),
        ("part-00049.parquet", b"d".to_vec()),
        ("part-00049.manifest", b"m".to_vec()),
        (".part\u{2010}00046.parquet.tmp", b"d".to_vec()),
        (".part-00047.parquet.tmp ", b"d".to_vec()),
        (".part-000\u{200B}48.manifest.tmp", b"d".to_vec()),
    ] {
        fs::write(dir.join(name), bytes).unwrap();
    }
    fs::create_dir_all(layout::resolve(
        root,
        "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-10-09",
    ))
    .unwrap();
    let temps = list_temps(root).unwrap();
    let expected: std::collections::BTreeMap<String, TempFiles> = [
        (
            path(42),
            TempFiles {
                data: true,
                manifest: None,
                unreadable: None,
            },
        ),
        (
            path(43),
            TempFiles {
                data: false,
                manifest: Some(Some("text 43".to_owned())),
                unreadable: None,
            },
        ),
        (
            path(44),
            TempFiles {
                data: false,
                manifest: Some(None),
                unreadable: None,
            },
        ),
        (
            path(45),
            TempFiles {
                data: true,
                manifest: Some(Some("text 45".to_owned())),
                unreadable: None,
            },
        ),
    ]
    .into_iter()
    .collect();
    assert_eq!(temps, expected);
    assert_eq!(
        temp_path(root, &path(45), TempName::Manifest).unwrap(),
        dir.join(".part-00045.manifest.tmp")
    );
    assert_eq!(
        temp_path(root, &path(45), TempName::Data).unwrap(),
        dir.join(".part-00045.parquet.tmp")
    );

    // Evidence.
    assert!(list_evidence(root).unwrap().is_empty());
    let damaged = root.join(META_DIR).join(DAMAGED_DIR);
    for (at, name) in [
        ("111", "part-00003.parquet"),
        ("222", ".part-00003.manifest.tmp"),
        ("222", "part-00004.manifest"),
        ("222", ".part\u{2010}00005.parquet.tmp"),
        ("333", "README"),
    ] {
        let at_dir = layout::resolve(&damaged.join(at), partition);
        fs::create_dir_all(&at_dir).unwrap();
        fs::write(at_dir.join(name), b"x").unwrap();
    }
    fs::write(damaged.join("333").join("copy-ledger"), b"x").unwrap();
    fs::create_dir_all(damaged.join("444/import-ledger/aggTrades")).unwrap();
    fs::write(
        damaged.join("444/import-ledger/aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import"),
        b"x",
    )
    .unwrap();
    fs::write(damaged.join("555"), b"a file, no directory").unwrap();
    let evidence = list_evidence(root).unwrap();
    let set = |dirs: &[&str]| dirs.iter().map(|d| (*d).to_owned()).collect();
    let expected: std::collections::BTreeMap<String, std::collections::BTreeSet<String>> =
        [(path(3), set(&["111", "222"])), (path(4), set(&["222"]))]
            .into_iter()
            .collect();
    assert_eq!(evidence, expected);
    // Evidence is no temp name, and no temp name is evidence.
    assert_eq!(list_temps(root).unwrap(), temps);
}

/// The first and the last differing byte (#217): across chunks, at the
/// first and the last byte, a single byte, and none.
#[test]
fn difference_bounds_first_last_and_none() {
    let dir = TempDir::new("replica-bounds");
    let path = dir.path().join("hot.jsonl");
    // Longer than three chunks, so the bounds cross chunk borders.
    let source: Vec<u8> = (0..400u32).map(|i| b'a' + (i % 26) as u8).collect();
    let len = source.len() as u64;
    let bounds = |copy: &[u8]| {
        fs::write(&path, copy).unwrap();
        difference_bounds(&path, TailSource::Bytes(&source), len).unwrap()
    };
    assert_eq!(bounds(&source), None);
    assert_eq!(bounds(&flipped(&source, 10)), Some((10, 10)));
    assert_eq!(bounds(&flipped(&flipped(&source, 0), 399)), Some((0, 399)));
    assert_eq!(
        bounds(&flipped(&flipped(&flipped(&source, 50), 200), 300)),
        Some((50, 300))
    );
    // Only `[0, len)` counts.
    fs::write(&path, flipped(&source, 399)).unwrap();
    assert_eq!(
        difference_bounds(&path, TailSource::Bytes(&source), 399).unwrap(),
        None
    );
    // A missing copy is the destination's side, a short source the source's.
    let error =
        difference_bounds(&dir.path().join("absent"), TailSource::Bytes(&source), len).unwrap_err();
    assert_eq!(error.side, Side::Destination);
    fs::write(&path, &source).unwrap();
    let error = difference_bounds(&path, TailSource::Bytes(&source[..10]), len).unwrap_err();
    assert_eq!(error.side, Side::Source);
}

/// The replace of one of hot's files (#217, ADR-065 D6) across a crash
/// after either step: the old file or the new one is always there, the
/// evidence holds the old bytes, and a re-run with a new evidence name
/// completes it.
#[test]
fn replace_with_evidence_crash_after_each_step() {
    let dir = TempDir::new("replica-replace");
    let dest = dir.path().join("journal.jsonl");
    let old = b"{\"a\":1}\n{\"b\":2X}\n".to_vec();
    let new = b"{\"a\":1}\n{\"b\":22}".to_vec();
    let tmp = dir.path().join(".journal.jsonl.replace.tmp");
    for (i, step) in [
        Some(ReplaceEvidenceStep::Linked),
        Some(ReplaceEvidenceStep::Temp),
        None,
    ]
    .into_iter()
    .enumerate()
    {
        fs::write(&dest, &old).unwrap();
        let aside = dir.path().join(format!("journal.jsonl.damaged-{i}"));
        let result = replace_with_evidence(
            TailSource::Bytes(&new),
            &dest,
            new.len() as u64,
            true,
            &aside,
            false,
            &mut |reached| {
                if Some(reached) == step {
                    Err(RawStoreError::Io(format!(
                        "simulated crash after {reached:?}"
                    )))
                } else {
                    Ok(())
                }
            },
        );
        // The evidence holds the old bytes in every case.
        assert_eq!(fs::read(&aside).unwrap(), old, "{step:?}");
        if step.is_some() {
            assert!(result.is_err(), "{step:?}");
            // Hot still holds its file: the old one.
            assert_eq!(fs::read(&dest).unwrap(), old, "{step:?}");
            let again = dir.path().join(format!("journal.jsonl.damaged-{i}-again"));
            replace_with_evidence(
                TailSource::Bytes(&new),
                &dest,
                new.len() as u64,
                true,
                &again,
                false,
                &mut |_| Ok(()),
            )
            .unwrap();
            assert_eq!(fs::read(&again).unwrap(), old, "{step:?}");
        } else {
            result.unwrap();
        }
        let mut expected = new.clone();
        expected.push(b'\n');
        assert_eq!(fs::read(&dest).unwrap(), expected, "{step:?}");
        assert!(!tmp.exists(), "{step:?}");
        // A link, not a copy: the evidence is the old inode, linked once
        // more by the re-run after a crash.
        use std::os::unix::fs::MetadataExt;
        let links = if step.is_some() { 2 } else { 1 };
        assert_eq!(fs::metadata(&aside).unwrap().nlink(), links, "{step:?}");
    }
}

/// An existing evidence name is refused before anything is written.
#[test]
fn replace_with_evidence_refuses_an_existing_aside() {
    let dir = TempDir::new("replica-replace-aside");
    let dest = dir.path().join("journal.jsonl");
    fs::write(&dest, b"old\n").unwrap();
    let aside = dir.path().join("journal.jsonl.damaged-1");
    fs::write(&aside, b"earlier evidence\n").unwrap();
    let before = snapshot(dir.path());
    let error = replace_with_evidence(
        TailSource::Bytes(b"new\n"),
        &dest,
        4,
        false,
        &aside,
        false,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert_eq!(error.side, Side::Destination);
    assert!(
        matches!(error.error, RawStoreError::Integrity(_)),
        "{error}"
    );
    assert_eq!(snapshot(dir.path()), before);
}

/// An import-ledger file ends read-only, an unreadable one is linked aside
/// all the same, a missing `dest` is refused (the replace never creates a
/// file), and a short source writes nothing over `dest`.
#[test]
fn replace_with_evidence_read_only_and_missing_dest() {
    use std::os::unix::fs::PermissionsExt;
    let dir = TempDir::new("replica-replace-ro");
    let dest = dir.path().join("ledger/aggTrades/F.zip.import");
    fs::create_dir_all(dest.parent().unwrap()).unwrap();
    fs::write(&dest, b"rotten\n").unwrap();
    fs::set_permissions(&dest, fs::Permissions::from_mode(0o000)).unwrap();
    let aside = dir.path().join("ledger.damaged-7/aggTrades/F.zip.import");
    let good = b"mie-archive-import 1\n".to_vec();
    replace_with_evidence(
        TailSource::Bytes(&good),
        &dest,
        good.len() as u64,
        false,
        &aside,
        true,
        &mut |_| Ok(()),
    )
    .unwrap();
    assert_eq!(fs::read(&dest).unwrap(), good);
    assert!(fs::metadata(&dest).unwrap().permissions().readonly());
    fs::set_permissions(&aside, fs::Permissions::from_mode(0o644)).unwrap();
    assert_eq!(fs::read(&aside).unwrap(), b"rotten\n");

    let missing = dir.path().join("ledger/aggTrades/G.zip.import");
    let error = replace_with_evidence(
        TailSource::Bytes(&good),
        &missing,
        good.len() as u64,
        false,
        &dir.path().join("ledger.damaged-8/aggTrades/G.zip.import"),
        true,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert!(matches!(error.error, RawStoreError::Invalid(_)), "{error}");
    assert!(!missing.exists());
    assert!(!dir.path().join("ledger.damaged-8").exists());

    let before = fs::read(&dest).unwrap();
    let error = replace_with_evidence(
        TailSource::Bytes(&good[..3]),
        &dest,
        good.len() as u64,
        false,
        &dir.path().join("ledger.damaged-9/aggTrades/F.zip.import"),
        true,
        &mut |_| Ok(()),
    )
    .unwrap_err();
    assert_eq!(error.side, Side::Source);
    assert_eq!(fs::read(&dest).unwrap(), before);
}

/// ADR-065 D8 (review of #226, findings 1 and 2): hot needs a strict
/// majority of the journals that hold a byte, wherever one contradicts
/// it. Two of three hold; one against one, one of three, two of four and
/// a byte past hot's end do not; bytes only hot holds are no contest. The
/// file form agrees with the in-memory form across chunk boundaries (97
/// bytes under test), skips an absent witness and names an unreadable
/// one.
#[test]
fn first_contested_needs_a_strict_majority_for_hot() {
    let hot: Vec<u8> = (0..300u32).map(|i| b'a' + (i % 26) as u8).collect();
    let at = |bytes: &[u8], i: usize| flipped(bytes, i);
    let (b10, b250) = (at(&hot, 10), at(&hot, 250));
    let contested = |witnesses: &[&[u8]], from: u64| first_contested(&hot, witnesses, from);
    assert_eq!(contested(&[&hot, &b10], 0), None);
    assert_eq!(contested(&[&b250, &b10], 0), None);
    assert_eq!(
        contested(&[&hot[..100], &b250], 0),
        Some(Contested {
            at: 250,
            covering: 2,
            with_hot: 1
        })
    );
    assert_eq!(
        contested(&[&b10, &b10], 0),
        Some(Contested {
            at: 10,
            covering: 3,
            with_hot: 1
        })
    );
    assert_eq!(contested(&[&b10, &b10], 11), None, "before `from`");
    assert_eq!(
        contested(&[&b10, &b10, &hot], 0),
        Some(Contested {
            at: 10,
            covering: 4,
            with_hot: 2
        })
    );
    assert_eq!(contested(&[&hot[..100], &hot[..200]], 0), None);
    let mut longer = hot.clone();
    longer.push(b'\n');
    assert_eq!(
        contested(&[&hot, &longer], 0),
        Some(Contested {
            at: 300,
            covering: 1,
            with_hot: 0
        })
    );

    let dir = TempDir::new("replica-contested");
    let write = |name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        fs::write(&path, bytes).unwrap();
        path
    };
    let hot_path = write("hot", &hot);
    let cases: [&[&[u8]]; 4] = [
        &[&hot, &b10],
        &[&hot[..100], &b250],
        &[&b10, &b10, &hot],
        &[&hot, &longer],
    ];
    for (n, witnesses) in cases.iter().enumerate() {
        let paths: Vec<PathBuf> = witnesses
            .iter()
            .enumerate()
            .map(|(w, bytes)| write(&format!("{n}-{w}"), bytes))
            .collect();
        let refs: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
        for from in [0, 96, 97, 251] {
            assert_eq!(
                first_contested_file(&hot_path, &refs, from).unwrap(),
                first_contested(&hot, witnesses, from),
                "case {n} from {from}"
            );
        }
    }
    let absent = dir.path().join("absent");
    let b250_path = write("b250", &b250);
    assert_eq!(
        first_contested_file(&hot_path, &[&b250_path, &absent], 0).unwrap(),
        Some(Contested {
            at: 250,
            covering: 2,
            with_hot: 1
        })
    );
    let error = first_contested_file(&hot_path, &[dir.path()], 0).unwrap_err();
    assert_eq!(error.side, Side::Destination, "{error}");
    let error = first_contested_file(&absent, &[&b250_path], 0).unwrap_err();
    assert_eq!(error.side, Side::Source, "{error}");
}

/// The bounded forms (ADR-065 D2, D9; review of #226 round 2): a
/// difference is looked for in `[from, to)` only, a contest below `until`
/// only, and a disagreement between witnesses is a byte two of them that
/// hold it hold differently (hot takes no part). The file forms agree with
/// the in-memory ones across chunk boundaries (97 bytes under test).
#[test]
fn the_bounded_difference_contest_and_disagreement() {
    let hot: Vec<u8> = (0..300u32).map(|i| b'a' + (i % 26) as u8).collect();
    let dir = TempDir::new("replica-bounded");
    let write = |name: &str, bytes: &[u8]| {
        let path = dir.path().join(name);
        fs::write(&path, bytes).unwrap();
        path
    };
    let hot_path = write("hot", &hot);
    let b10_250 = flipped(&flipped(&hot, 10), 250);
    let b10_250_path = write("b10-250", &b10_250);
    let source = TailSource::File(&hot_path);
    assert_eq!(
        first_difference_in(&b10_250_path, source, 0, 300).unwrap(),
        Some(10)
    );
    assert_eq!(
        first_difference_in(&b10_250_path, source, 11, 300).unwrap(),
        Some(250)
    );
    assert_eq!(
        first_difference_in(&b10_250_path, source, 11, 250).unwrap(),
        None,
        "`to` is exclusive"
    );
    assert_eq!(
        first_difference_in(&b10_250_path, source, 300, 300).unwrap(),
        None,
        "an empty range reads nothing"
    );

    // Hot 200 bytes long under two copies of 300: the bytes past its end
    // are contested, the ones below are not.
    let short_path = write("short", &hot[..200]);
    let b10_path = write("b10", &flipped(&hot, 10));
    let full_path = write("full", &hot);
    let copies = [b10_path.as_path(), full_path.as_path()];
    assert_eq!(
        first_contested_file(&short_path, &copies, 0)
            .unwrap()
            .map(|c| c.at),
        Some(200)
    );
    for until in [97, 194, 200] {
        assert_eq!(
            first_contested_file_before(&short_path, &copies, 0, until).unwrap(),
            None,
            "until {until}"
        );
    }
    assert_eq!(
        first_contested_file_before(&short_path, &copies, 0, 201)
            .unwrap()
            .map(|c| c.at),
        Some(200)
    );

    let b250 = flipped(&hot, 250);
    // (witnesses, from, the first disagreement)
    type Case<'a> = (&'a [&'a [u8]], u64, Option<u64>);
    let cases: [Case; 6] = [
        (&[&hot, &hot], 0, None),
        (&[&hot, &b250], 0, Some(250)),
        (&[&hot, &b250], 251, None),
        (&[&hot[..260], &b250], 0, Some(250)),
        (&[&hot[..100], &b250], 0, None),
        (&[&b250], 0, None),
    ];
    for (n, (witnesses, from, expected)) in cases.iter().enumerate() {
        assert_eq!(first_disagreement(witnesses, *from), *expected, "case {n}");
        let paths: Vec<PathBuf> = witnesses
            .iter()
            .enumerate()
            .map(|(w, bytes)| write(&format!("d{n}-{w}"), bytes))
            .collect();
        let mut refs: Vec<&Path> = paths.iter().map(PathBuf::as_path).collect();
        let absent = dir.path().join("absent");
        refs.push(&absent);
        for from in [*from, from + 96, from + 97] {
            assert_eq!(
                first_disagreement_file(&refs, from).unwrap(),
                first_disagreement(witnesses, from),
                "case {n} from {from}"
            );
        }
    }
    let error = first_disagreement_file(&[dir.path()], 0).unwrap_err();
    assert_eq!(error.side, Side::Destination, "{error}");
}

/// The journals of the contradiction tests (ADR-066 D5): 300 bytes of hot,
/// written beside each witness, in a directory that lives as long as the
/// returned value.
struct Contradiction {
    dir: TempDir,
    hot: Vec<u8>,
    hot_path: PathBuf,
}

impl Contradiction {
    fn new(tag: &str) -> Self {
        let dir = TempDir::new(tag);
        let hot: Vec<u8> = (0..300u32).map(|i| b'a' + (i % 26) as u8).collect();
        let hot_path = dir.path().join("hot");
        fs::write(&hot_path, &hot).unwrap();
        Self { dir, hot, hot_path }
    }

    fn write(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.dir.path().join(name);
        fs::write(&path, bytes).unwrap();
        path
    }

    /// The first contradiction in `[from, until)` with `witnesses`.
    fn first(&self, witnesses: &[&Path], from: u64, until: u64) -> Option<u64> {
        first_contradiction_file_before(&self.hot_path, witnesses, from, until).unwrap()
    }
}

/// A witness that differs from hot where hot holds a strict majority is
/// no contest under ADR-065 D8, and still a contradiction: D5 judges its
/// line before it counts. Across a chunk boundary (97 bytes under test)
/// too.
#[test]
fn contradiction_finds_a_byte_hot_holds_by_majority() {
    let c = Contradiction::new("replica-contradiction-majority");
    for at in [10, 96, 97, 250] {
        let other = c.write(&format!("b{at}"), &flipped(&c.hot, at));
        let agree = c.write("agree", &c.hot);
        let witnesses = [other.as_path(), agree.as_path()];
        assert_eq!(
            first_contested_file(&c.hot_path, &witnesses, 0).unwrap(),
            None,
            "{at}"
        );
        assert_eq!(c.first(&witnesses, 0, u64::MAX), Some(at as u64), "{at}");
    }
}

/// Any byte a witness holds past hot's end contradicts it; a witness that
/// ends at hot's end does not.
#[test]
fn contradiction_past_hot_end() {
    let c = Contradiction::new("replica-contradiction-end");
    let mut longer = c.hot.clone();
    longer.extend_from_slice(b"\n{");
    let longer = c.write("longer", &longer);
    let equal = c.write("equal", &c.hot);
    assert_eq!(c.first(&[&equal, &longer], 0, u64::MAX), Some(300));
    assert_eq!(c.first(&[&equal, &longer], 299, u64::MAX), Some(300));
    assert_eq!(c.first(&[&equal, &longer], 301, u64::MAX), Some(301));
    assert_eq!(c.first(&[&equal], 0, u64::MAX), None);
}

/// Byte prefixes of hot, an empty witness, an absent one and no witness
/// at all contradict nothing; an unreadable witness is an error on the
/// destination side, an unreadable hot on the source side.
#[test]
fn contradiction_none_for_prefixes_and_absent_witnesses() {
    let c = Contradiction::new("replica-contradiction-none");
    let a = c.write("a", &c.hot[..100]);
    let b = c.write("b", &c.hot[..200]);
    let empty = c.write("empty", b"");
    let absent = c.dir.path().join("absent");
    assert_eq!(c.first(&[&a, &b, &empty, &absent], 0, u64::MAX), None);
    assert_eq!(c.first(&[], 0, u64::MAX), None);
    let error =
        first_contradiction_file_before(&c.hot_path, &[c.dir.path()], 0, u64::MAX).unwrap_err();
    assert_eq!(error.side, Side::Destination, "{error}");
    let error = first_contradiction_file_before(&absent, &[&a], 0, u64::MAX).unwrap_err();
    assert_eq!(error.side, Side::Source, "{error}");
}

/// `from` is inclusive and `until` exclusive: a witness flipped at 10 and
/// 250 contradicts at 10 from 0 and from 10, at 250 from 11, at nothing
/// below 250, and an empty range reads nothing.
#[test]
fn contradiction_bounds_are_from_inclusive_until_exclusive() {
    let c = Contradiction::new("replica-contradiction-bounds");
    let both = c.write("both", &flipped(&flipped(&c.hot, 10), 250));
    let both = [both.as_path()];
    assert_eq!(c.first(&both, 0, u64::MAX), Some(10));
    assert_eq!(c.first(&both, 10, u64::MAX), Some(10));
    assert_eq!(c.first(&both, 11, u64::MAX), Some(250));
    assert_eq!(c.first(&both, 11, 250), None, "`until` is exclusive");
    assert_eq!(c.first(&both, 11, 251), Some(250));
    assert_eq!(c.first(&both, 0, 10), None);
    assert_eq!(c.first(&both, 250, 250), None, "an empty range");
    assert_eq!(c.first(&both, 251, u64::MAX), None);
}

/// The journal lock (review of #226, finding 3): one holder at a time,
/// free after its drop, beside the file a link names, and its directory
/// created when absent.
#[test]
fn the_journal_lock_refuses_a_second_holder() {
    let dir = TempDir::new("replica-journal-lock");
    let journal = dir.path().join("logs/journal.jsonl");
    let lock = try_journal_lock(&journal).unwrap();
    assert!(dir.path().join("logs/journal.jsonl.lock").exists());
    assert!(matches!(
        try_journal_lock(&journal),
        Err(RawStoreError::Locked(_))
    ));
    drop(lock);
    fs::write(&journal, b"").unwrap();
    let link = dir.path().join("link.jsonl");
    std::os::unix::fs::symlink(&journal, &link).unwrap();
    let by_link = try_journal_lock(&link).unwrap();
    assert!(matches!(
        try_journal_lock(&journal),
        Err(RawStoreError::Locked(_))
    ));
    drop(by_link);
    drop(try_journal_lock(&journal).unwrap());
}
