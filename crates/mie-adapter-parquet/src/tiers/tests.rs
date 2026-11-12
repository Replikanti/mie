//! The union `select` and the tiered `read` against a single-root
//! reference holding every file (ADR-062 D5, D6, D7, D11).

use super::{AFTER_HOT_LISTING, BEFORE_MANIFEST_READ, BEFORE_PARTITION_FILES};
use crate::copy_ledger::{self, HoldReason, Kind, LostOrigin, Resolution};
use crate::layout::{self, DAY_MS};
use crate::manifest;
use crate::reader::ParquetRawStore;
use crate::testutil::{FIXTURE_D0 as D0, FIXTURE_DATES, TieredFixture};
use mie_domain::time::EventTime;
use mie_ports::outbound::ReplayWindow;
use mie_ports::raw::{RawDataset, RawRecordSource, RawSelection, RawStoreError, RawStreamKey};
use std::collections::BTreeSet;
use std::fs;

const HOUR: i64 = 3_600_000;

fn selection(streams: &[RawStreamKey], start: i64, end: i64) -> RawSelection {
    RawSelection::new(
        streams.iter().cloned().collect::<BTreeSet<_>>(),
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        },
    )
    .expect("a valid selection")
}

fn whole(streams: &[RawStreamKey]) -> RawSelection {
    selection(streams, 0, layout::MAX_EVENT_TIME_MS)
}

fn all() -> RawSelection {
    whole(&TieredFixture::streams())
}

/// Every window class of the plan, over both streams and each one alone.
fn windows() -> Vec<RawSelection> {
    let mut ranges = vec![
        // The whole range, and wider than the data.
        (0, layout::MAX_EVENT_TIME_MS),
        (D0 - 3 * DAY_MS, D0 + 10 * DAY_MS),
        // Mid-day starts and ends, across dates.
        (D0 + 4 * HOUR, D0 + 2 * DAY_MS + 7 * HOUR),
        (D0 + DAY_MS + 3 * HOUR, D0 + DAY_MS + 3 * HOUR + 1),
        (D0 + 2 * DAY_MS + 14 * HOUR, D0 + 3 * DAY_MS + 2 * HOUR),
        // An end on a day boundary.
        (D0 + 5 * HOUR, D0 + 2 * DAY_MS),
        // No data: after the last record of a date, and before the data.
        (D0 + 16 * HOUR, D0 + DAY_MS),
        (D0 - DAY_MS, D0),
    ];
    // Each single day, one before and one after the data included.
    for day in -1..=FIXTURE_DATES {
        ranges.push((D0 + day * DAY_MS, D0 + (day + 1) * DAY_MS));
    }
    let [a, b] = TieredFixture::streams();
    let mut out = Vec::new();
    for (start, end) in ranges {
        for streams in [vec![a.clone(), b.clone()], vec![a.clone()], vec![b.clone()]] {
            out.push(selection(&streams, start, end));
        }
    }
    out
}

/// `select` of every window equals the reference's, and every file reads
/// back equal.
fn assert_matches_reference(fx: &TieredFixture) {
    let (store, reference) = (fx.store(), fx.reference());
    for selection in windows() {
        assert_eq!(
            store.select(&selection),
            reference.select(&selection),
            "{selection:?}"
        );
    }
    let everything = reference.select(&all()).unwrap();
    assert!(everything.files.len() >= 2 * FIXTURE_DATES as usize * 2);
    for file in &everything.files {
        assert_eq!(
            store.read(file),
            reference.read(file),
            "{}",
            file.relative_path
        );
    }
}

fn integrity(result: Result<RawDataset, RawStoreError>) -> String {
    match result {
        Err(RawStoreError::Integrity(message)) => message,
        other => panic!("expected an Integrity error, got {other:?}"),
    }
}

/// Sealed files are read-only; tests that damage one make it writable.
fn writable(path: &std::path::Path) {
    let mut permissions = fs::metadata(path).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    permissions.set_readonly(false);
    fs::set_permissions(path, permissions).unwrap();
}

fn flip_byte(path: &std::path::Path) {
    writable(path);
    let mut bytes = fs::read(path).unwrap();
    let middle = bytes.len() / 2;
    bytes[middle] ^= 0x01;
    fs::write(path, bytes).unwrap();
}

fn a() -> RawStreamKey {
    TieredFixture::streams()[0].clone()
}

fn b() -> RawStreamKey {
    TieredFixture::streams()[1].clone()
}

#[test]
fn single_root_is_unchanged() {
    let fx = TieredFixture::new("tiers-single");
    assert_eq!(
        ParquetRawStore::tiered(&fx.hot, Vec::new()),
        ParquetRawStore::new(&fx.hot)
    );
    assert!(!ParquetRawStore::tiered(&fx.hot, Vec::new()).is_tiered());
    assert!(
        ParquetRawStore::tiered(&fx.hot, Vec::new())
            .cold_roots()
            .is_empty()
    );
    assert_eq!(fx.store().cold_roots(), &fx.cold[..]);

    // The ledger records a removed partition: a single root never reads
    // it, so the files are gone from its selection.
    let partition = TieredFixture::partition(&a(), 0);
    let removed = TieredFixture::data_paths(&fx.hot, &partition).len();
    assert_eq!(removed, 3);
    fx.copy(&partition);
    fx.remove(&partition);
    let single = ParquetRawStore::new(&fx.hot);
    let files = single.select(&all()).unwrap().files;
    assert!(
        files
            .iter()
            .all(|f| !f.relative_path.starts_with(&partition))
    );
    assert_eq!(
        files.len() + removed,
        fx.reference().select(&all()).unwrap().files.len()
    );
    // And it never fails on a missing ledger.
    fs::remove_file(copy_ledger::ledger_path(&fx.hot)).unwrap();
    assert_eq!(single.select(&all()).unwrap().files, files);
}

#[test]
fn layouts_match_the_single_root_reference() {
    // All on hot, with a header-only ledger.
    let fx = TieredFixture::new("tiers-all-hot");
    assert_matches_reference(&fx);

    // Mixed: hot-only, copied and removed partitions.
    let fx = TieredFixture::new("tiers-mixed");
    for key in [a(), b()] {
        fx.copy(&TieredFixture::partition(&key, 0));
        fx.remove(&TieredFixture::partition(&key, 0));
        fx.copy(&TieredFixture::partition(&key, 1));
    }
    fx.copy(&TieredFixture::partition(&a(), 2));
    fx.remove(&TieredFixture::partition(&a(), 2));
    assert_matches_reference(&fx);

    // Every old partition removed: only the newest date stays on hot.
    let fx = TieredFixture::new("tiers-all-removed");
    for key in [a(), b()] {
        for day in 0..FIXTURE_DATES - 1 {
            fx.copy(&TieredFixture::partition(&key, day));
            fx.remove(&TieredFixture::partition(&key, day));
        }
    }
    assert_matches_reference(&fx);
}

#[test]
fn the_ledger_is_read_fresh_per_select() {
    let fx = TieredFixture::new("tiers-fresh");
    let store = fx.store();
    let expected = fx.reference().select(&all()).unwrap();
    assert_eq!(store.select(&all()).unwrap(), expected);
    let partition = TieredFixture::partition(&b(), 2);
    fx.copy(&partition);
    fx.remove(&partition);
    assert_eq!(store.select(&all()).unwrap(), expected);
}

#[test]
fn a_removal_between_hot_listing_and_ledger_read_keeps_every_file() {
    let fx = TieredFixture::new("tiers-race");
    let partition = TieredFixture::partition(&a(), 1);
    let mover = fx.clone();
    AFTER_HOT_LISTING.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            mover.copy(&partition);
            mover.remove(&partition);
        }));
    });
    let result = fx.store().select(&all());
    assert!(
        AFTER_HOT_LISTING.with(|hook| hook.borrow().is_none()),
        "the hook ran"
    );
    assert_eq!(result, fx.reference().select(&all()));
    assert!(!layout::resolve(&fx.hot, &TieredFixture::partition(&a(), 1)).exists());
}

#[test]
fn a_crash_between_the_removed_record_and_the_rename() {
    let fx = TieredFixture::new("tiers-crash");
    let partition = TieredFixture::partition(&b(), 0);
    fx.copy(&partition);
    fx.append(Kind::Removed {
        partition: partition.clone(),
    });
    assert_matches_reference(&fx);
}

#[test]
fn a_vanishing_partition() {
    // Recorded: removed by D6 while it is listed.
    let fx = TieredFixture::new("tiers-vanish-recorded");
    let target = TieredFixture::partition(&a(), 2);
    fx.copy(&target);
    let mover = fx.clone();
    BEFORE_PARTITION_FILES.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |partition: &str| {
            if partition == TieredFixture::partition(&a(), 2) {
                mover.remove(partition);
            }
        }));
    });
    let result = fx.store().select(&all());
    BEFORE_PARTITION_FILES.with(|hook| hook.borrow_mut().take());
    assert_eq!(result, fx.reference().select(&all()));

    // Unrecorded: deleted outside the protocol.
    let fx = TieredFixture::new("tiers-vanish-unrecorded");
    let hot = fx.hot.clone();
    BEFORE_PARTITION_FILES.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |partition: &str| {
            if partition == TieredFixture::partition(&a(), 2) {
                fs::remove_dir_all(layout::resolve(&hot, partition)).unwrap();
            }
        }));
    });
    let result = fx.store().select(&all());
    BEFORE_PARTITION_FILES.with(|hook| hook.borrow_mut().take());
    let message = integrity(result);
    assert!(
        message.contains(&format!("partition {target} disappeared")),
        "{message}"
    );

    // One file, recorded and not, through the manifest-read hook.
    for recorded in [true, false] {
        let fx = TieredFixture::new("tiers-vanish-file");
        let partition = TieredFixture::partition(&b(), 1);
        let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
        if recorded {
            fx.copy(&partition);
        }
        let (hot, target) = (fx.hot.clone(), path.clone());
        BEFORE_MANIFEST_READ.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |data_path: &str| {
                if data_path == target {
                    fs::remove_file(layout::resolve(&hot, &TieredFixture::manifest_of(&target)))
                        .unwrap();
                    fs::remove_file(layout::resolve(&hot, &target)).unwrap();
                }
            }));
        });
        let result = fx.store().select(&all());
        BEFORE_MANIFEST_READ.with(|hook| hook.borrow_mut().take());
        if recorded {
            assert_eq!(result, fx.reference().select(&all()));
        } else {
            let message = integrity(result);
            assert!(
                message.starts_with(&format!("{path} disappeared")),
                "{message}"
            );
        }
    }
}

#[test]
fn a_partial_return() {
    let fx = TieredFixture::new("tiers-return");
    let partition = TieredFixture::partition(&a(), 1);
    fx.copy(&partition);
    fx.remove(&partition);
    fx.return_some(&partition, 1);
    assert_eq!(TieredFixture::data_paths(&fx.hot, &partition).len(), 1);
    assert_matches_reference(&fx);
}

#[test]
fn the_incomplete_trailing_record_is_ignored() {
    let fx = TieredFixture::new("tiers-trailing");
    let partition = TieredFixture::partition(&a(), 0);
    fx.copy(&partition);
    fx.remove(&partition);
    let ledger = copy_ledger::ledger_path(&fx.hot);
    let mut bytes = fs::read(&ledger).unwrap();
    bytes.extend_from_slice(b"removed 1791244800999 source=binance-um/instr");
    fs::write(&ledger, &bytes).unwrap();
    assert_eq!(fx.store().select(&all()), fx.reference().select(&all()));

    // A chain failure before the last record.
    let header = copy_ledger::HEADER.len();
    let mut bytes = fs::read(&ledger).unwrap();
    bytes[header + 5] ^= 0x01;
    fs::write(&ledger, &bytes).unwrap();
    let message = integrity(fx.store().select(&all()));
    assert!(
        message.contains("the hash chain fails at record 1"),
        "{message}"
    );
}

#[test]
fn a_missing_ledger() {
    let fx = TieredFixture::new("tiers-no-ledger");
    let ledger = copy_ledger::ledger_path(&fx.hot);
    fs::remove_file(&ledger).unwrap();
    let store = fx.store();
    let mut windows = windows();
    // A window outside the supported range lists nothing and still fails.
    windows.push(selection(
        &TieredFixture::streams(),
        layout::MAX_EVENT_TIME_MS + 1,
        layout::MAX_EVENT_TIME_MS + 2,
    ));
    for selection in windows {
        let message = integrity(store.select(&selection));
        assert!(
            message.contains(&format!("copy ledger {} does not exist", ledger.display())),
            "{message}"
        );
        assert_eq!(
            store.select_hot(&selection),
            fx.reference().select(&selection)
        );
    }
}

#[test]
fn d5_manifest_conflicts() {
    let partition = TieredFixture::partition(&a(), 1);

    // Hot's manifest differs from the ledger's: in the hash, and in the
    // event times, where the window covers only the ledger's.
    for shift_times in [false, true] {
        let fx = TieredFixture::new("tiers-d5-hot");
        fx.copy(&partition);
        let path = TieredFixture::data_paths(&fx.hot, &partition)[0].clone();
        let manifest_path = layout::resolve(&fx.hot, &TieredFixture::manifest_of(&path));
        let original = manifest::parse(&fs::read_to_string(&manifest_path).unwrap()).unwrap();
        let mut modified = original.clone();
        if shift_times {
            modified.min_event_time =
                EventTime::from_millis(original.min_event_time.as_millis() + 1);
            modified.max_event_time =
                EventTime::from_millis(original.max_event_time.as_millis() + 1);
        } else {
            modified.sha256 = "0".repeat(64);
        }
        writable(&manifest_path);
        fs::write(&manifest_path, manifest::render(&modified)).unwrap();
        let only_the_ledger = selection(
            &[a()],
            original.min_event_time.as_millis(),
            original.min_event_time.as_millis() + 1,
        );
        for selection in [all(), only_the_ledger] {
            let message = integrity(fx.store().select(&selection));
            assert!(
                message.starts_with(&format!("{path}: hot's manifest differs")),
                "{message}"
            );
        }
    }

    // Two ledger manifest records of one path differ.
    let fx = TieredFixture::new("tiers-d5-ledger");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[0].clone();
    let text =
        fs::read_to_string(layout::resolve(&fx.hot, &TieredFixture::manifest_of(&path))).unwrap();
    let mut modified = manifest::parse(&text).unwrap();
    modified.rows += 1;
    fx.append(Kind::Manifest {
        text: manifest::render(&modified),
    });
    fx.remove(&partition);
    let message = integrity(fx.store().select(&all()));
    assert!(
        message.starts_with(&format!("{path}: the copy ledger holds two different")),
        "{message}"
    );
}

#[test]
fn read_order() {
    let fx = TieredFixture::new("tiers-read");
    let partition = TieredFixture::partition(&b(), 3);
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[0].clone();
    let file = fx
        .reference()
        .select(&all())
        .unwrap()
        .files
        .into_iter()
        .find(|f| f.relative_path == path)
        .unwrap();
    let expected = fx.reference().read(&file).unwrap();
    let store = fx.store();
    let on = |root: &std::path::Path| layout::resolve(root, &path);
    let pristine = fs::read(on(&fx.hot)).unwrap();

    // A bad cold-a copy behind a good hot one is never read.
    flip_byte(&on(&fx.cold[0]));
    assert_eq!(store.read(&file).unwrap(), expected);

    // A bad hot copy is an error, although both cold copies are good.
    writable(&on(&fx.cold[0]));
    fs::write(on(&fx.cold[0]), &pristine).unwrap();
    flip_byte(&on(&fx.hot));
    assert!(matches!(
        store.read(&file),
        Err(RawStoreError::Integrity(_))
    ));

    // Hot absent, cold-a bad: no fallback to cold-b.
    fs::remove_file(on(&fx.hot)).unwrap();
    flip_byte(&on(&fx.cold[0]));
    assert!(matches!(
        store.read(&file),
        Err(RawStoreError::Integrity(_))
    ));

    // Hot and cold-a absent: cold-b answers.
    fs::remove_file(on(&fx.cold[0])).unwrap();
    assert_eq!(store.read(&file).unwrap(), expected);

    // Absent everywhere: every root is named.
    fs::remove_file(on(&fx.cold[1])).unwrap();
    match store.read(&file) {
        Err(RawStoreError::Integrity(message)) => {
            assert!(message.starts_with(&format!("{path} is missing on every root: ")));
            for root in std::iter::once(&fx.hot).chain(&fx.cold) {
                assert!(message.contains(&root.display().to_string()), "{message}");
            }
        }
        other => panic!("{other:?}"),
    }
}

/// QA of #198: a hot copy that exists but cannot be read is `Io`, naming
/// the root, and never a fallback to a good cold copy. Skipped where
/// permissions do not stop reads (root).
#[test]
fn read_order_an_unreadable_hot_copy_is_io_with_no_fallback() {
    use std::os::unix::fs::PermissionsExt;
    let fx = TieredFixture::new("tiers-read-unreadable");
    let partition = TieredFixture::partition(&a(), 2);
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[0].clone();
    let file = fx
        .reference()
        .select(&all())
        .unwrap()
        .files
        .into_iter()
        .find(|f| f.relative_path == path)
        .unwrap();
    let hot_copy = layout::resolve(&fx.hot, &path);
    fs::set_permissions(&hot_copy, fs::Permissions::from_mode(0o000)).unwrap();
    if fs::read(&hot_copy).is_ok() {
        eprintln!("skipped: permissions do not stop reads here (running as root)");
        return;
    }
    match fx.store().read(&file) {
        Err(RawStoreError::Io(message)) => {
            assert!(
                message.contains(&hot_copy.display().to_string()),
                "{message}"
            );
            assert!(
                message.contains(&format!("on root {}", fx.hot.display())),
                "{message}"
            );
        }
        other => panic!("expected Io with no fallback, got {other:?}"),
    }
    // Both cold copies are good: a fallback would have answered.
    for root in &fx.cold {
        let cold_only = ParquetRawStore::new(root);
        assert_eq!(cold_only.read(&file), fx.reference().read(&file));
    }
}

#[test]
fn lost_held_and_quarantined_files() {
    let partition = TieredFixture::partition(&a(), 1);
    let covering = selection(&[a()], D0 + DAY_MS, D0 + 2 * DAY_MS);
    let elsewhere = selection(&TieredFixture::streams(), D0 + 3 * DAY_MS, D0 + 4 * DAY_MS);
    let other_stream = selection(&[b()], D0 + DAY_MS, D0 + 2 * DAY_MS);
    let passes_elsewhere = |fx: &TieredFixture| {
        for selection in [&elsewhere, &other_stream] {
            assert_eq!(
                fx.store().select(selection),
                fx.reference().select(selection)
            );
        }
    };

    // Lost, with a manifest record.
    let fx = TieredFixture::new("tiers-lost");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[0].clone();
    fx.remove(&partition);
    fx.append(Kind::Lost {
        path: path.clone(),
        origin: LostOrigin::Verify,
        sha256: None,
    });
    let message = integrity(fx.store().select(&covering));
    assert!(message.starts_with(&format!("{path} is lost")), "{message}");
    assert!(message.contains("lost-file rule"), "{message}");
    passes_elsewhere(&fx);

    // Lost without any manifest: the whole stream and date is covered,
    // even a window that overlaps no file of it.
    let fx = TieredFixture::new("tiers-lost-bare");
    let bare = format!("{partition}/part-00099.parquet");
    fx.append(Kind::Lost {
        path: bare.clone(),
        origin: LostOrigin::Journal,
        sha256: None,
    });
    let empty_part_of_the_day = selection(&[a()], D0 + DAY_MS + 16 * HOUR, D0 + 2 * DAY_MS);
    for selection in [&covering, &empty_part_of_the_day] {
        let message = integrity(fx.store().select(selection));
        assert!(message.starts_with(&format!("{bare} is lost")), "{message}");
    }
    passes_elsewhere(&fx);

    // Held, then readmitted.
    let fx = TieredFixture::new("tiers-held");
    let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
    fx.append(Kind::Hold {
        path: path.clone(),
        reason: HoldReason::Unlisted,
    });
    let message = integrity(fx.store().select(&covering));
    assert!(
        message.starts_with(&format!("{path} is held (unlisted)")),
        "{message}"
    );
    assert!(message.contains("mie store resolve"), "{message}");
    passes_elsewhere(&fx);
    fx.append(Kind::HoldCleared {
        path: path.clone(),
        resolution: Resolution::Readmit,
    });
    assert_matches_reference(&fx);

    // Held, resolved by replace: pending until `replaced`, through
    // quarantine.
    let fx = TieredFixture::new("tiers-replace");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
    fx.append(Kind::Hold {
        path: path.clone(),
        reason: HoldReason::ManifestConflict,
    });
    fx.append(Kind::HoldCleared {
        path: path.clone(),
        resolution: Resolution::Replace,
    });
    let message = integrity(fx.store().select(&covering));
    assert!(
        message.contains("its hold was resolved by replace"),
        "{message}"
    );
    assert!(message.contains("re-import"), "{message}");
    fx.quarantine(&path, "q-0001");
    fx.append(Kind::Quarantined {
        path: path.clone(),
        quarantine: "q-0001".to_owned(),
    });
    let message = integrity(fx.store().select(&covering));
    assert!(message.contains("quarantined under q-0001"), "{message}");
    assert!(message.contains("re-import"), "{message}");
    passes_elsewhere(&fx);
    fx.append(Kind::Replaced {
        path: path.clone(),
        entry: "2026-10/aggTrade-2026-10-07.import".to_owned(),
    });
    for selection in [all(), covering.clone()] {
        let mut expected = fx.reference().select(&selection).unwrap().files;
        expected.retain(|f| f.relative_path != path);
        let got = fx.store().select(&selection).unwrap();
        assert_eq!(got.files, expected);
        assert_eq!(
            got.version,
            manifest::dataset_version(selection.window(), selection.streams(), &expected)
        );
    }

    // A replaced path still on hot.
    let fx = TieredFixture::new("tiers-replaced-on-hot");
    let path = TieredFixture::data_paths(&fx.hot, &partition)[2].clone();
    fx.append(Kind::Replaced {
        path: path.clone(),
        entry: "2026-10/aggTrade-2026-10-07.import".to_owned(),
    });
    let message = integrity(fx.store().select(&all()));
    assert!(message.contains("but still listed on hot"), "{message}");
}

#[test]
fn the_first_error_in_path_order() {
    let fx = TieredFixture::new("tiers-first-error");
    // markPrice sorts after aggTrade, so the later record names the lower
    // path.
    let later = format!("{}/part-00042.parquet", TieredFixture::partition(&b(), 0));
    let lower = format!("{}/part-00042.parquet", TieredFixture::partition(&a(), 2));
    for path in [&later, &lower] {
        fx.append(Kind::Lost {
            path: path.clone(),
            origin: LostOrigin::Import,
            sha256: None,
        });
    }
    let message = integrity(fx.store().select(&all()));
    assert!(
        message.starts_with(&format!("{lower} is lost")),
        "{message}"
    );
}

// ------------------------------------------- AW3 with the real remover

/// The real remover (#191): the plan over the fixture's current state and
/// one remove pass of `binance-um`, with days 0 to 2 before the hot window.
fn remove_for_real(fx: &TieredFixture) -> crate::removal::RemovalReport {
    let first = TieredFixture::date(3);
    let ledger = copy_ledger::read(&fx.hot).unwrap().unwrap();
    let scan = crate::removal::scan_hot(&fx.hot, &first).unwrap();
    let plan = crate::removal::plan(
        &ledger,
        &scan,
        &TieredFixture::target_ids(),
        2,
        &first,
        None,
    )
    .unwrap();
    crate::removal::remove_with_lock(&fx.hot, "binance-um", &plan, &first, D0 + 40 * DAY_MS)
}

#[test]
fn a_real_removal_between_hot_listing_and_ledger_read_keeps_every_file() {
    let fx = TieredFixture::new("tiers-real-race");
    let partition = TieredFixture::partition(&a(), 1);
    fx.copy(&partition);
    fx.ledger_copies();
    let mover = fx.clone();
    let removed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = std::rc::Rc::clone(&removed);
    AFTER_HOT_LISTING.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            *seen.borrow_mut() = remove_for_real(&mover).removed;
        }));
    });
    let result = fx.store().select(&all());
    assert_eq!(*removed.borrow(), vec![partition.clone()]);
    assert_eq!(result, fx.reference().select(&all()));
    assert!(!layout::resolve(&fx.hot, &partition).exists());
    assert_matches_reference(&fx);
}

#[test]
fn a_partition_removed_while_its_files_are_listed_is_never_empty() {
    let fx = TieredFixture::new("tiers-real-vanish");
    let target = TieredFixture::partition(&b(), 2);
    fx.copy(&target);
    fx.ledger_copies();
    let mover = fx.clone();
    let removed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let seen = std::rc::Rc::clone(&removed);
    BEFORE_PARTITION_FILES.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move |partition: &str| {
            if partition == TieredFixture::partition(&b(), 2) {
                *seen.borrow_mut() = remove_for_real(&mover).removed;
            }
        }));
    });
    let result = fx.store().select(&all());
    BEFORE_PARTITION_FILES.with(|hook| hook.borrow_mut().take());
    assert_eq!(*removed.borrow(), vec![target.clone()]);
    assert_eq!(result, fx.reference().select(&all()));
}

#[test]
fn a_select_during_a_recall_holds_every_file() {
    use crate::writer::{RecallEvent, RecallObserver, RotationPolicy};
    use mie_ports::raw::RawRecordSink;
    use std::sync::{Arc, Mutex};

    let fx = TieredFixture::new("tiers-recall-select");
    let partition = TieredFixture::partition(&a(), 0);
    assert_eq!(TieredFixture::data_paths(&fx.hot, &partition).len(), 3);
    fx.copy(&partition);
    fx.ledger_copies();
    assert_eq!(remove_for_real(&fx).removed, vec![partition.clone()]);

    let (hot, cold, reference) = (
        fx.hot.clone(),
        fx.cold.to_vec(),
        fx.reference_root().to_path_buf(),
    );
    let checked = Arc::new(Mutex::new(Vec::new()));
    let log = Arc::clone(&checked);
    let on_recall: RecallObserver = Arc::new(move |event| {
        if !matches!(
            event,
            RecallEvent::Copied { .. } | RecallEvent::Returned { .. }
        ) {
            return;
        }
        let store = ParquetRawStore::tiered(&hot, cold.clone());
        let expected = ParquetRawStore::new(&reference);
        for selection in windows() {
            // Files and dataset version.
            assert_eq!(
                store.select(&selection),
                expected.select(&selection),
                "{event:?} {selection:?}"
            );
        }
        log.lock().unwrap().push(event.clone());
    });
    let mut ctx = TieredFixture::context(&TieredFixture::date(3));
    ctx.on_recall = Some(on_recall);
    let mut writer = fx
        .store()
        .tiered_writer("binance-um", RotationPolicy::default(), ctx)
        .unwrap();
    writer
        .append(&a(), crate::testutil::record(D0 + 10, 7))
        .unwrap();
    writer.close().unwrap();
    let checked = checked.lock().unwrap();
    assert_eq!(checked.len(), 4, "three copies and the return: {checked:?}");
}

/// #211 I2: the import view skips a path only while its replacement is
/// pending under a `quarantined` record and hot does not list it; a
/// `hold-cleared replace` alone, a quarantined path still on hot, an open
/// hold and a lost record still fail there, and the strict `select` fails
/// on all of them.
#[test]
fn the_import_view_skips_only_a_quarantined_pending_path() {
    let partition = TieredFixture::partition(&a(), 1);
    let covering = selection(&[a()], D0 + DAY_MS, D0 + 2 * DAY_MS);
    let import = |fx: &TieredFixture| fx.store().import_view().select(&covering);

    // Quarantined and gone from hot: absent in the import view only.
    let fx = TieredFixture::new("tiers-import-view");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
    fx.append(Kind::Hold {
        path: path.clone(),
        reason: HoldReason::Unlisted,
    });
    fx.append(Kind::Quarantined {
        path: path.clone(),
        quarantine: "q-0001".to_owned(),
    });
    // Held still: both fail.
    assert!(integrity(import(&fx)).contains("is held"));
    fx.append(Kind::HoldCleared {
        path: path.clone(),
        resolution: Resolution::Replace,
    });
    // Still on hot: both fail.
    assert!(integrity(import(&fx)).contains("quarantined under q-0001"));
    fx.quarantine(&path, "q-0001");
    let mut expected = fx.reference().select(&covering).unwrap().files;
    expected.retain(|f| f.relative_path != path);
    let got = import(&fx).unwrap();
    assert_eq!(got.files, expected);
    assert_eq!(
        got.version,
        manifest::dataset_version(covering.window(), covering.streams(), &expected)
    );
    assert!(integrity(fx.store().select(&covering)).contains("quarantined under q-0001"));
    // The single-root view is the single-root `select`.
    let reference = fx.reference();
    assert_eq!(
        reference.import_view().select(&covering),
        reference.select(&covering)
    );

    // `hold-cleared replace` without `quarantined`, gone from hot.
    let fx = TieredFixture::new("tiers-import-view-cleared");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
    fx.append(Kind::Hold {
        path: path.clone(),
        reason: HoldReason::Unlisted,
    });
    fx.append(Kind::HoldCleared {
        path: path.clone(),
        resolution: Resolution::Replace,
    });
    fx.quarantine(&path, "q-0002");
    assert!(integrity(import(&fx)).contains("its hold was resolved by replace"));

    // Lost, then quarantined: lost wins.
    let fx = TieredFixture::new("tiers-import-view-lost");
    fx.copy(&partition);
    let path = TieredFixture::data_paths(&fx.hot, &partition)[1].clone();
    fx.quarantine(&path, "q-0003");
    fx.append(Kind::Lost {
        path: path.clone(),
        origin: LostOrigin::Verify,
        sha256: None,
    });
    fx.append(Kind::Quarantined {
        path: path.clone(),
        quarantine: "q-0003".to_owned(),
    });
    assert!(integrity(import(&fx)).contains("is lost"));
}
