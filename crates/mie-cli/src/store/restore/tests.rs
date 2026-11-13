//! The restore's pure plans (#195): the journal choice (H3), the
//! import-ledger plan (H4) and the copy-back plan (H6). The names follow
//! the plan's edge cases.

use super::*;
use crate::store::archive_checks::tests::{NAME, S1, entry, part, sha, supersede, text};
use crate::store::archive_checks::{ArchiveInputs, PartTexts, check};

/// H3 over in-memory candidates: `(label, bytes)`, every one readable.
fn choose(candidates: &[(&str, &[u8])]) -> Result<Option<usize>, String> {
    let read: Vec<(&str, Option<&[u8]>)> = candidates
        .iter()
        .map(|(label, bytes)| (*label, Some(*bytes)))
        .collect();
    choose_with(&read, false)
        .map(|choice| choice.chosen)
        .map_err(|lines| lines.join("\n"))
}

/// H3′ over in-memory candidates: `(label, bytes)`, `None` for one whose
/// read fails, with or without `--skip-unreadable-journal`.
fn choose_with(
    candidates: &[(&str, Option<&[u8]>)],
    skip: bool,
) -> Result<JournalChoice, Vec<String>> {
    choose_sized(
        &candidates
            .iter()
            .map(|(label, bytes)| (*label, bytes.map_or(0, <[u8]>::len) as u64, *bytes))
            .collect::<Vec<_>>(),
        skip,
        &[],
    )
}

/// H3′ over `(label, len, bytes)`: `None` bytes fail to read; `fail_compare`
/// lists the candidates whose read fails only in the comparison with C.
fn choose_sized(
    candidates: &[(&str, u64, Option<&[u8]>)],
    skip: bool,
    fail_compare: &[usize],
) -> Result<JournalChoice, Vec<String>> {
    let list: Vec<JournalCandidate> = candidates
        .iter()
        .map(|(label, len, _)| JournalCandidate {
            label: (*label).to_owned(),
            len: *len,
        })
        .collect();
    choose_journal(
        &list,
        skip,
        &mut |i| {
            candidates[i]
                .2
                .filter(|_| !fail_compare.contains(&i))
                .map(<[u8]>::to_vec)
                .ok_or_else(|| "Permission denied (os error 13)".to_owned())
        },
        &mut |i, c, len| {
            if fail_compare.contains(&i) {
                return Err("Input/output error (os error 5)".to_owned());
            }
            let a = candidates[i].2.ok_or("Permission denied (os error 13)")?;
            Ok((0..len as usize).find(|&k| a[k] != c[k]).map(|k| k as u64))
        },
        &|i| format!("aside-{i}"),
    )
}

/// A journal of `n` bytes of complete JSON lines and padding.
fn journal(n: usize) -> Vec<u8> {
    let line = b"{\"type\":\"run_start\",\"run_id\":\"r\"}\n";
    line.iter().copied().cycle().take(n).collect()
}

/// Candidates of 0, 100 and 250 bytes, each a prefix of the next: the 250
/// bytes win.
#[test]
fn j_longest_prefix_wins() {
    let full = journal(250);
    let cases: [&[(&str, &[u8])]; 2] = [
        &[
            ("hot", &full[..0]),
            ("cold-a", &full[..100]),
            ("cold-b", &full),
        ],
        &[("cold-a", &full), ("cold-b", &full[..100])],
    ];
    assert_eq!(choose(cases[0]), Ok(Some(2)));
    assert_eq!(choose(cases[1]), Ok(Some(0)));
    // Hot's own surviving journal, when it is the longest, is C (kept).
    assert_eq!(
        choose(&[("hot", &full), ("cold-a", &full[..100])]),
        Ok(Some(0))
    );
    // On equal lengths and bytes the first (hot) is C.
    assert_eq!(choose(&[("hot", &full), ("cold-a", &full)]), Ok(Some(0)));
}

/// Two 250-byte copies that differ at byte 0, or only at byte 249, stop
/// the restore.
#[test]
fn j_equal_length_divergence_stops() {
    let a = journal(250);
    for at in [0usize, 249] {
        let mut b = a.clone();
        b[at] ^= 0x01;
        let why = choose(&[("cold-a", &a), ("cold-b", &b)]).unwrap_err();
        assert!(
            why.contains("cold-a") && why.contains("cold-b") && why.contains(&format!("byte {at}")),
            "{why}"
        );
    }
}

/// A 100-byte copy that differs from a 250-byte copy at byte 37 stops the
/// restore, naming byte 37; so does hot's surviving journal (H3).
#[test]
fn j_divergence_beyond_common_prefix_stops() {
    let long = journal(250);
    let mut short = long[..100].to_vec();
    short[37] ^= 0x01;
    let why = choose(&[("cold-a", &short), ("cold-b", &long)]).unwrap_err();
    assert!(why.contains("byte 37") && why.contains("cold-a"), "{why}");
    // j_surviving_hot_divergence_stops, pure part.
    let why = choose(&[("hot", &short), ("cold-a", &long)]).unwrap_err();
    assert!(why.contains("byte 37") && why.contains("hot"), "{why}");
}

/// No candidate: no journal.
#[test]
fn j_no_journal_anywhere() {
    assert_eq!(choose(&[]), Ok(None));
}

/// A copy of `name` on `root`.
fn copy(root: &str, bytes: &[u8]) -> LedgerFileCopy {
    LedgerFileCopy {
        root: root.to_owned(),
        path: PathBuf::from(format!("/{root}/x")),
        bytes: bytes.to_vec(),
    }
}

/// The decided variants as the derivation hands them over: the archive
/// checks over these copies and parts.
fn chosen(
    files: &BTreeMap<String, Vec<LedgerFileCopy>>,
    parts: &[(String, String)],
) -> BTreeMap<String, Vec<u8>> {
    let inputs = ArchiveInputs {
        entries: files
            .iter()
            .map(|(name, copies)| {
                let copies = copies
                    .iter()
                    .map(|c| crate::store::archive_checks::EntryCopy {
                        root: c.root.clone(),
                        bytes: c.bytes.clone(),
                    })
                    .collect();
                (name.clone(), copies)
            })
            .collect(),
        parts: parts
            .iter()
            .map(|(path, text)| {
                (
                    path.clone(),
                    PartTexts {
                        roots: vec![("cold-a".to_owned(), text.clone())],
                        start: Vec::new(),
                    },
                )
            })
            .collect(),
        pending: None,
    };
    check(&inputs).chosen
}

fn two_parts() -> (String, String, Vec<(String, String)>) {
    let (a, b) = (
        part("aggTrades", "2026-10-01", 0),
        part("aggTrades", "2026-10-01", 1),
    );
    let parts = vec![
        (a.clone(), text(&a, &sha(&a))),
        (b.clone(), text(&b, &sha(&b))),
    ];
    (a, b, parts)
}

/// `F.import` identical on both targets is written to hot.
#[test]
fn i_identical_entries_are_restored() {
    let (a, b, parts) = two_parts();
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let files = BTreeMap::from([(
        NAME.to_owned(),
        vec![copy("cold-a", &good), copy("cold-b", &good)],
    )]);
    let plan = plan_import_ledger(&files, &chosen(&files, &parts));
    assert_eq!(plan.writes, [(NAME.to_owned(), PathBuf::from("/cold-a/x"))]);
    assert!(
        plan.set_aside.is_empty() && plan.problems.is_empty(),
        "{plan:?}"
    );
    // Hot already holds it: nothing to write (the re-run).
    let mut files = files;
    files.get_mut(NAME).unwrap().insert(0, copy(HOT_ID, &good));
    let plan = plan_import_ledger(&files, &chosen(&files, &parts));
    assert_eq!(plan, ImportPlan::default());
}

/// An entry on one target only is written.
#[test]
fn i_target_only_entry_is_restored() {
    let (a, b, parts) = two_parts();
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let files = BTreeMap::from([(NAME.to_owned(), vec![copy("cold-b", &good)])]);
    let plan = plan_import_ledger(&files, &chosen(&files, &parts));
    assert_eq!(plan.writes, [(NAME.to_owned(), PathBuf::from("/cold-b/x"))]);
}

/// Two variants, exactly one matching the manifests: it is written, the
/// other is named for repair by the checks, nothing is held.
#[test]
fn i_one_matching_variant_wins() {
    let (a, b, parts) = two_parts();
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let wrong = entry(&[(&a, &sha("other")), (&b, &sha(&b))]);
    let files = BTreeMap::from([(
        NAME.to_owned(),
        vec![copy("cold-a", &wrong), copy("cold-b", &good)],
    )]);
    let decided = chosen(&files, &parts);
    assert_eq!(decided.get(NAME), Some(&good));
    let plan = plan_import_ledger(&files, &decided);
    assert_eq!(plan.writes, [(NAME.to_owned(), PathBuf::from("/cold-b/x"))]);
    assert!(plan.problems.is_empty(), "{plan:?}");
}

/// Both or neither matching: nothing written for the name (the derivation
/// holds its parts `entry-conflict`).
#[test]
fn i_undecidable_variants_hold() {
    let (a, b, parts) = two_parts();
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    // Both match: the same files, another archive SHA-256.
    let also_good = String::from_utf8(good.clone())
        .unwrap()
        .replace(&sha("zip"), &sha("zip2"))
        .into_bytes();
    assert_ne!(also_good, good);
    let wrong = entry(&[(&a, &sha("x")), (&b, &sha(&b))]);
    let other = entry(&[(&a, &sha("y")), (&b, &sha(&b))]);
    for (x, y) in [(&good, &also_good), (&wrong, &other)] {
        let files = BTreeMap::from([(NAME.to_owned(), vec![copy("cold-a", x), copy("cold-b", y)])]);
        let decided = chosen(&files, &parts);
        assert!(decided.is_empty(), "{decided:?}");
        let plan = plan_import_ledger(&files, &decided);
        assert!(plan.writes.is_empty(), "{plan:?}");
        assert!(
            plan.problems
                .iter()
                .any(|p| p.contains(NAME) && p.contains("no variant")),
            "{plan:?}"
        );
    }
}

/// A surviving hot copy of the losing variant is set aside before the
/// winner is written.
#[test]
fn i_surviving_losing_entry_is_set_aside() {
    let (a, b, parts) = two_parts();
    let good = entry(&[(&a, &sha(&a)), (&b, &sha(&b))]);
    let wrong = entry(&[(&a, &sha("other")), (&b, &sha(&b))]);
    let files = BTreeMap::from([(
        NAME.to_owned(),
        vec![
            copy(HOT_ID, &wrong),
            copy("cold-a", &good),
            copy("cold-b", &good),
        ],
    )]);
    let plan = plan_import_ledger(&files, &chosen(&files, &parts));
    assert_eq!(plan.set_aside, [NAME.to_owned()]);
    assert_eq!(plan.writes, [(NAME.to_owned(), PathBuf::from("/cold-a/x"))]);
}

/// Differing supersede-record copies: hot keeps its own when it parses;
/// otherwise the first target copy that parses and names its own file.
#[test]
fn i_differing_supersede_records() {
    let record = supersede();
    let rotten = b"mie-archive-supersede 1\nnot a record\n".to_vec();
    // Another file's record: parses, does not bind (G3).
    let foreign = String::from_utf8(record.clone())
        .unwrap()
        .replace("2026-10-01.zip\n", "2026-10-02.zip\n")
        .replace("period 2026-10-01", "period 2026-10-02")
        .into_bytes();
    let plan = |copies: Vec<LedgerFileCopy>| {
        let files = BTreeMap::from([(S1.to_owned(), copies)]);
        plan_import_ledger(&files, &BTreeMap::new())
    };
    // Hot's parses: kept, nothing written.
    let kept = plan(vec![copy(HOT_ID, &record), copy("cold-a", &rotten)]);
    assert!(
        kept.writes.is_empty() && kept.set_aside.is_empty(),
        "{kept:?}"
    );
    // Hot's does not: set aside, the first target copy that binds wins.
    let replaced = plan(vec![
        copy(HOT_ID, &rotten),
        copy("cold-a", &foreign),
        copy("cold-b", &record),
    ]);
    assert_eq!(replaced.set_aside, [S1.to_owned()]);
    assert_eq!(
        replaced.writes,
        [(S1.to_owned(), PathBuf::from("/cold-b/x"))]
    );
    // Hot lacks it: the first target copy that binds.
    let restored = plan(vec![copy("cold-a", &record), copy("cold-b", &rotten)]);
    assert_eq!(
        restored.writes,
        [(S1.to_owned(), PathBuf::from("/cold-a/x"))]
    );
    // No copy binds: named, nothing written.
    let none = plan(vec![copy("cold-a", &rotten), copy("cold-b", &foreign)]);
    assert!(none.writes.is_empty(), "{none:?}");
    assert!(none.problems.iter().any(|p| p.contains(S1)), "{none:?}");
}

/// The first hot date of `now` as production derives it.
fn first_hot(now_ms: i64) -> String {
    crate::ingest::first_hot_date(now_ms)
}

/// The window starts on `first_hot_date(now)`, the date of now − 7 days:
/// with now = 2026-10-12T00:00:00.000Z or 23:59:59.999Z, `date=2026-10-05`
/// is planned and `date=2026-10-04` is not, unless the start does not
/// record it removed.
#[test]
fn c_window_bound_is_inclusive() {
    let day =
        |date: &str| format!("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date={date}");
    // 2026-10-12T00:00:00.000Z.
    let midnight = 1_791_763_200_000;
    for now in [midnight, midnight + 86_400_000 - 1] {
        let first = first_hot(now);
        assert_eq!(first, "2026-10-05", "{now}");
        let (old, edge) = (day("2026-10-04"), day("2026-10-05"));
        let all_removed = plan_copy_back([old.as_str(), edge.as_str()], &first, &|_| true);
        assert_eq!(all_removed, BTreeSet::from([edge.clone()]));
        let none_removed = plan_copy_back([old.as_str(), edge.as_str()], &first, &|_| false);
        assert_eq!(none_removed, BTreeSet::from([old, edge]));
    }
}

/// An old partition the start records removed stays on cold.
#[test]
fn c_removed_partitions_stay_on_cold() {
    let old = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-09-01";
    assert!(plan_copy_back([old], "2026-10-05", &|p| p == old).is_empty());
}

/// An old partition whose `removed` record was in the lost tail is copied
/// back (no `removed` record is derived for it: H5 e, in the derivation's
/// tests).
#[test]
fn c_unremoved_old_partition_is_copied_back() {
    let old = "source=binance-um/instrument=BTCUSDT/stream=aggTrade/date=2026-09-01";
    assert_eq!(
        plan_copy_back([old], "2026-10-05", &|_| false),
        BTreeSet::from([old.to_owned()])
    );
}

// ------------------------------------------------- H3′ (amendment 2)

/// H3′ over unreadable candidates. C is a readable copy of 900 bytes
/// unless a case says otherwise; each case without the flag, then with it.
#[test]
fn j_unreadable_candidates() {
    let c = journal(1_000);
    let read = |len: usize| Some(&c[..len]);
    let left = |choice: &JournalChoice| -> Vec<(usize, bool)> {
        choice
            .left_out
            .iter()
            .map(|out| (out.index, out.by_flag))
            .collect()
    };
    // 900 bytes, unreadable: left out (U), flag or no flag.
    for skip in [false, true] {
        let choice = choose_sized(
            &[("cold-a", 900, None), ("cold-b", 900, read(900))],
            skip,
            &[],
        )
        .unwrap();
        assert_eq!(choice.chosen, Some(1));
        assert_eq!(left(&choice), vec![(0, false)]);
        assert_eq!(choice.bytes, c[..900]);
    }
    // 901 bytes: a stop; with the flag left out, "its last 1 byte(s)".
    let cands = [("cold-a", 901, None), ("cold-b", 900, read(900))];
    let stop = choose_sized(&cands, false, &[]).unwrap_err();
    assert_eq!(stop.len(), 1, "{stop:?}");
    assert!(
        stop[0].contains("cold-a")
            && stop[0].contains("901 bytes")
            && stop[0].contains("its last 1 byte(s)")
            && stop[0].contains("--skip-unreadable-journal"),
        "{stop:?}"
    );
    let choice = choose_sized(&cands, true, &[]).unwrap();
    assert_eq!(choice.chosen, Some(1));
    assert_eq!(left(&choice), vec![(0, true)]);
    // Two, of 1 000 and 950 bytes: a stop naming both; with the flag both
    // left out.
    let cands = [
        ("cold-a", 1_000, None),
        ("cold-b", 950, None),
        ("hot", 900, read(900)),
    ];
    let stop = choose_sized(&cands, false, &[]).unwrap_err();
    assert_eq!(stop.len(), 2, "{stop:?}");
    assert!(
        stop[0].contains("cold-a") && stop[1].contains("cold-b"),
        "{stop:?}"
    );
    assert!(stop[0].contains("its last 100 byte(s)"), "{stop:?}");
    let choice = choose_sized(&cands, true, &[]).unwrap();
    assert_eq!(choice.chosen, Some(2));
    assert_eq!(left(&choice), vec![(0, true), (1, true)]);
    // Hot's journal, 1 000 bytes, tied with a readable target copy (C):
    // hot's left out (U), flag or no flag.
    for skip in [false, true] {
        let choice = choose_sized(
            &[("hot", 1_000, None), ("cold-a", 1_000, read(1_000))],
            skip,
            &[],
        )
        .unwrap();
        assert_eq!(choice.chosen, Some(1));
        assert_eq!(left(&choice), vec![(0, false)]);
    }
    // A 600-byte one whose read fails in the comparison with C: left out
    // (U).
    for skip in [false, true] {
        let choice = choose_sized(
            &[("cold-a", 600, read(600)), ("cold-b", 900, read(900))],
            skip,
            &[0],
        )
        .unwrap();
        assert_eq!(choice.chosen, Some(1));
        assert_eq!(left(&choice), vec![(0, false)]);
    }
    // The only candidate, 0 bytes: left out (U); no C, the journal stays
    // absent.
    for skip in [false, true] {
        let choice = choose_sized(&[("cold-a", 0, None)], skip, &[]).unwrap();
        assert_eq!(choice.chosen, None);
        assert_eq!(left(&choice), vec![(0, false)]);
        assert!(choice.bytes.is_empty());
    }
    // The only candidate, 1 byte: a stop (L = 0, "none reads"); with the
    // flag left out, no C.
    let stop = choose_sized(&[("cold-a", 1, None)], false, &[]).unwrap_err();
    assert!(
        stop[0].contains("none reads") && stop[0].contains("its last 1 byte(s)"),
        "{stop:?}"
    );
    let choice = choose_sized(&[("cold-a", 1, None)], true, &[]).unwrap();
    assert_eq!(choice.chosen, None);
    assert_eq!(left(&choice), vec![(0, true)]);
    // N = 5 000, L = 4 200: "its last 800 byte(s)".
    let long = journal(5_000);
    let stop = choose_sized(
        &[
            ("cold-a", 5_000, None),
            ("cold-b", 4_200, Some(&long[..4_200])),
        ],
        false,
        &[],
    )
    .unwrap_err();
    assert!(stop[0].contains("its last 800 byte(s)"), "{stop:?}");
}

// --------------------------------------------------- H6 J (amendment 2)

/// The live path `n` of 2026-10-06.
fn j_path(date: &str, n: u32) -> String {
    format!("source=binance-um/instrument=BTCUSDT/stream=aggTrade/date={date}/part-{n:05}.parquet")
}

/// The copy state of a ledger of `kinds`.
fn state_of(kinds: &[Kind]) -> CopyState {
    let mut builder = copy_ledger::LedgerBuilder::empty();
    for kind in kinds {
        builder
            .push(&Record {
                at_ms: 1,
                kind: kind.clone(),
            })
            .unwrap();
    }
    let ledger = copy_ledger::parse(Path::new("l"), &builder.finish()).unwrap();
    CopyState::fold(&ledger).unwrap()
}

/// J over the installed ledger and a names oracle: P lies in a partition
/// recorded removed and dated before the window (first hot date
/// 2026-10-07), unless a case says otherwise.
#[test]
fn j_plan_from_the_installed_ledger() {
    use mie_adapter_parquet::copy_ledger::{HoldReason, LostOrigin};
    let first_hot = "2026-10-07";
    let p = j_path("2026-10-06", 3);
    let partition = partition_of(&p).to_owned();
    let t = text(&p, &sha("a"));
    let manifest = Kind::Manifest { text: t.clone() };
    let copy = |root: &str| Kind::Copy {
        path: p.clone(),
        root: root.to_owned(),
    };
    let removed = Kind::Removed {
        partition: partition.clone(),
    };
    use Held::{Data, Manifest, PendingManifest, VerifiedOnHot};
    type Case<'a> = (&'a str, Vec<Kind>, Vec<Held>, bool);
    let cases: Vec<Case> = vec![
        (
            "no copy; M~ (+ D~) on cold-a",
            vec![manifest.clone(), removed.clone()],
            vec![PendingManifest],
            true,
        ),
        (
            "no copy; M + D~ (row 3)",
            vec![manifest.clone(), removed.clone()],
            vec![Manifest],
            true,
        ),
        (
            "no copy; M + D on a read-only root",
            vec![manifest.clone(), removed.clone()],
            vec![Manifest, Data],
            true,
        ),
        (
            "copy on cold-b; M + D on cold-b",
            vec![manifest.clone(), copy("cold-b"), removed.clone()],
            vec![Manifest, Data],
            false,
        ),
        (
            "copy on both; no D anywhere; M~ on cold-a",
            vec![
                manifest.clone(),
                copy("cold-a"),
                copy("cold-b"),
                removed.clone(),
            ],
            vec![PendingManifest],
            true,
        ),
        (
            "copy on both; D on cold-b",
            vec![
                manifest.clone(),
                copy("cold-a"),
                copy("cold-b"),
                removed.clone(),
            ],
            vec![Manifest, Data],
            false,
        ),
        (
            "no M and no M~ on any root",
            vec![manifest.clone(), removed.clone()],
            vec![Data],
            false,
        ),
        (
            "no copy; no M or M~ on any cold root; hot's copy verifies (review round 4)",
            vec![manifest.clone(), removed.clone()],
            vec![VerifiedOnHot],
            true,
        ),
        (
            "no copy; truncated D on cold-b; hot's copy verifies (review round 4)",
            vec![manifest.clone(), removed.clone()],
            vec![Data, VerifiedOnHot],
            true,
        ),
        (
            "copy on both; D on cold-b; hot's copy verifies (a pass recorded it)",
            vec![
                manifest.clone(),
                copy("cold-a"),
                copy("cold-b"),
                removed.clone(),
            ],
            vec![Manifest, Data, VerifiedOnHot],
            false,
        ),
        (
            "verdict lost",
            vec![
                removed.clone(),
                Kind::Lost {
                    path: p.clone(),
                    origin: LostOrigin::Verify,
                    sha256: None,
                },
            ],
            vec![Manifest, PendingManifest],
            false,
        ),
        (
            "verdict held",
            vec![
                manifest.clone(),
                removed.clone(),
                Kind::Hold {
                    path: p.clone(),
                    reason: HoldReason::Unlisted,
                },
            ],
            vec![Manifest, PendingManifest],
            false,
        ),
        (
            "partition returned",
            vec![
                manifest.clone(),
                removed.clone(),
                Kind::Returned {
                    partition: partition.clone(),
                },
            ],
            vec![PendingManifest],
            false,
        ),
    ];
    for (case, kinds, held, expected) in cases {
        let state = state_of(&kinds);
        let plan = plan_copy_back([partition.as_str()], first_hot, &|part| {
            state.is_removed(part)
        });
        let found = plan_unreplicated(&state, &plan, &|path, form| {
            path == p && held.contains(&form)
        });
        assert_eq!(found == vec![p.clone()], expected, "{case}: {found:?}");
        assert!(expected || found.is_empty(), "{case}: {found:?}");
    }
    // Verdict replaced: no.
    let replaced = state_of(&[
        manifest.clone(),
        removed.clone(),
        Kind::Replaced {
            path: p.clone(),
            entry: "aggTrades/x.1.import".to_owned(),
        },
    ]);
    if replaced.verdict(&p) == Some(PathVerdict::Replaced) {
        let plan = BTreeSet::new();
        assert!(plan_unreplicated(&replaced, &plan, &|_, _| true).is_empty());
    }
    // Dated 2026-10-07 with first hot date 2026-10-07: the whole partition
    // is in the plan (the bound is inclusive), so not in J.
    let q = j_path("2026-10-07", 0);
    let state = state_of(&[
        Kind::Manifest {
            text: text(&q, &sha("a")),
        },
        Kind::Removed {
            partition: partition_of(&q).to_owned(),
        },
    ]);
    let plan = plan_copy_back([partition_of(&q)], first_hot, &|part| {
        state.is_removed(part)
    });
    assert!(plan.contains(partition_of(&q)));
    assert!(plan_unreplicated(&state, &plan, &|_, _| true).is_empty());
}
