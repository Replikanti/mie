//! The copy-ledger grammar, its hash chain and the trailing-record rule.

use super::*;
use crate::layout::PartFile;
use crate::testutil::{TempDir, stream};
use mie_domain::time::EventTime;
use mie_ports::raw::SealedFile;

/// 2026-10-06T01:00:00Z.
const AT: i64 = 1_791_248_400_000;

const PATH: &str =
    "source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet";
const PARTITION: &str = "source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06";
const ENTRY: &str = "aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import";

/// The header and one record of every kind, chain hashes included. Pinned
/// byte for byte: a change here is a format change (ADR-062 D7).
const GOLDEN: &str = concat!(
    "mie-copy-ledger 1\n",
    "manifest 1791248400000 mie-raw-manifest%201%0Apath%20source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet%0Asource%20binance-um%0Ainstrument%20BTCUSDT%0Astream%20depth%0Adate%202026-10-06%0Apart%203%0Arows%204217%0Abytes%201048576%0Amin_event_time_ms%201791244800000%0Amax_event_time_ms%201791248399999%0Asha256%2053791ac4060bf10774bb05e40a8afaa60f490a0cb329ef0be31f7610f938b364%0A 3685772b6fc77b73d7a3a11b2a4e055684d77f8280094556c9c6b46f106516c0\n",
    "copy 1791248400000 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet cold-a e5312262b79d05f5c9f34cd9c00d8feb279f2c8c1cf37ae4b3fc19abc2161b23\n",
    "ledger-copy 1791248400001 cold-a 1234 2f05a2ba928cca2e20c7b97b9d5d6e9c284dd315201131800d67d67884057983 01654b9a497a651dcdc5860cb2200285e440b7015e9b7904bb05b5202fe316da\n",
    "removed 1791248400002 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06 b4b6c3e5a4ec98634da6fa4921dddd4a63a36e628633d6d537c88c1cd11ccbd9\n",
    "returned 1791248400003 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06 f681c8dc2848289df72bfdf67dd3cc248d53292a070cb0e4123b0f4eb9da2d8c\n",
    "finding 1791248400004 cold-b sha256 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet 1487e7216d48440c3e73808bb71567dc5a35edd11c15731b8f4ddd20a4aad74c\n",
    "verify 1791248400004 cold-b 2048 1 369a6d2824e839b27cec524d39c4d944225e476dfa5619cfe58c14b685d25729\n",
    "lost 1791248400005 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet journal 53791ac4060bf10774bb05e40a8afaa60f490a0cb329ef0be31f7610f938b364 c70816796368d1bdb90786659ab06fb1f1fe21bd863491ffa9dd8b1cab188413\n",
    "hold 1791248400006 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet unlisted 6532783fb1f6f3b6b306e8abbad4040195a87b945ea73003d5a2a201cedc005e\n",
    "hold-cleared 1791248400007 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet replace 84c011cbd6b3e85a71051fc4f7d4df2dfe03a18ca55d9b86928bbc531e9c17f0\n",
    "quarantined 1791248400008 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet q-20261006 80366222b7c66e39d7fc50a728f536808fc7272c3466bf68b8772ed565ea81cc\n",
    "replaced 1791248400009 source=binance-um/instrument=BTCUSDT/stream=depth/date=2026-10-06/part-00003.parquet aggTrades/BTCUSDT-aggTrades-2026-10-05.zip.import 5be25e1dc82313355937b86a02e10cac057a25e64f90f520966b79ef083845f1\n",
);

/// The manifest of a fixture `depth` file.
fn manifest_text() -> String {
    let stream = stream("depth");
    manifest::render(&SealedFile {
        relative_path: layout::file_path(&stream, "2026-10-06", PartFile::Data, 3),
        stream,
        date: "2026-10-06".to_owned(),
        part: 3,
        rows: 4_217,
        bytes: 1_048_576,
        min_event_time: EventTime::from_millis(1_791_244_800_000),
        max_event_time: EventTime::from_millis(1_791_248_399_999),
        sha256: sha256_hex(b"depth part 3"),
    })
}

fn record(offset: i64, kind: Kind) -> Record {
    Record {
        at_ms: AT + offset,
        kind,
    }
}

/// One record of every kind, in the order D5, D6, D7 and D11 write them.
fn golden_records() -> Vec<Record> {
    vec![
        record(
            0,
            Kind::Manifest {
                text: manifest_text(),
            },
        ),
        record(
            0,
            Kind::Copy {
                path: PATH.to_owned(),
                root: "cold-a".to_owned(),
            },
        ),
        record(
            1,
            Kind::LedgerCopy {
                root: "cold-a".to_owned(),
                prefix_bytes: 1_234,
                prefix_sha256: sha256_hex(b"ledger prefix"),
            },
        ),
        record(
            2,
            Kind::Removed {
                partition: PARTITION.to_owned(),
            },
        ),
        record(
            3,
            Kind::Returned {
                partition: PARTITION.to_owned(),
            },
        ),
        record(
            4,
            Kind::Finding {
                root: "cold-b".to_owned(),
                code: FindingCode::Sha256,
                subject: Subject::File(PATH.to_owned()),
            },
        ),
        record(
            4,
            Kind::Verify {
                root: "cold-b".to_owned(),
                prefix_bytes: 2_048,
                findings: 1,
            },
        ),
        record(
            5,
            Kind::Lost {
                path: PATH.to_owned(),
                origin: LostOrigin::Journal,
                sha256: Some(sha256_hex(b"depth part 3")),
            },
        ),
        record(
            6,
            Kind::Hold {
                path: PATH.to_owned(),
                reason: HoldReason::Unlisted,
            },
        ),
        record(
            7,
            Kind::HoldCleared {
                path: PATH.to_owned(),
                resolution: Resolution::Replace,
            },
        ),
        record(
            8,
            Kind::Quarantined {
                path: PATH.to_owned(),
                quarantine: "q-20261006".to_owned(),
            },
        ),
        record(
            9,
            Kind::Replaced {
                path: PATH.to_owned(),
                entry: ENTRY.to_owned(),
            },
        ),
    ]
}

/// A store root with `.mie/` and a header-only ledger.
fn store(tag: &str) -> TempDir {
    let dir = TempDir::new(tag);
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    fs::write(ledger_path(dir.path()), HEADER).unwrap();
    dir
}

fn append_all(root: &Path, records: &[Record]) {
    let ledger = CopyLedger::at(root);
    for record in records {
        assert_eq!(ledger.append(record).unwrap().truncation, None);
    }
}

/// Lowercase hex, written here independently of the module.
fn hex_of(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A ledger of `bodies` with correct chains, computed independently.
fn ledger_of(bodies: &[&str]) -> Vec<u8> {
    let mut text = HEADER.to_owned();
    let mut previous = hex_of(&Sha256::digest(HEADER.as_bytes()));
    for body in bodies {
        let mut hasher = Sha256::new();
        hasher.update(previous.as_bytes());
        hasher.update(body.as_bytes());
        previous = hex_of(&hasher.finalize());
        text.push_str(&format!("{body} {previous}\n"));
    }
    text.into_bytes()
}

/// The line spans `(offset, length without the line end)` of a ledger.
fn spans(bytes: &[u8]) -> Vec<(usize, usize)> {
    let mut spans = Vec::new();
    let mut offset = HEADER.len();
    while let Some(len) = bytes[offset..].iter().position(|&b| b == b'\n') {
        spans.push((offset, len));
        offset += len + 1;
    }
    spans
}

fn parsed(bytes: &[u8]) -> Result<Ledger, RawStoreError> {
    parse(Path::new("copy-ledger"), bytes)
}

#[test]
fn golden_text_is_pinned_byte_for_byte_with_its_chain() {
    let dir = store("ledger-golden");
    let records = golden_records();
    let kinds: std::collections::BTreeSet<&str> = records.iter().map(|r| r.kind.name()).collect();
    assert_eq!(kinds.len(), 12, "one record of every kind");
    append_all(dir.path(), &records);
    let text = fs::read_to_string(ledger_path(dir.path())).unwrap();
    assert_eq!(text, GOLDEN);

    // The chain recomputed from the definition, not through the module.
    let mut previous = hex_of(&Sha256::digest(b"mie-copy-ledger 1\n"));
    let mut lines = GOLDEN.split_terminator('\n');
    assert_eq!(lines.next(), Some("mie-copy-ledger 1"));
    for line in lines {
        let (body, chain) = line.rsplit_once(' ').unwrap();
        let mut hasher = Sha256::new();
        hasher.update(previous.as_bytes());
        hasher.update(body.as_bytes());
        assert_eq!(chain, hex_of(&hasher.finalize()), "{body}");
        previous = chain.to_owned();
    }

    // parse(render(r)) == r for every record.
    let ledger = read(dir.path()).unwrap().unwrap();
    assert_eq!(ledger.records, records);
    assert_eq!(ledger.ignored_bytes, 0);
    assert_eq!(check_chain(GOLDEN.as_bytes()).failure, None);
}

#[test]
fn every_vocabulary_value_round_trips() {
    let mut records = Vec::new();
    for (i, &code) in FindingCode::ALL.iter().enumerate() {
        for subject in [
            Subject::File(PATH.to_owned()),
            Subject::Journal,
            Subject::CopyLedger,
            Subject::Entry(ENTRY.to_owned()),
        ] {
            records.push(record(
                i as i64,
                Kind::Finding {
                    root: "hot".to_owned(),
                    code,
                    subject,
                },
            ));
        }
    }
    for &origin in LostOrigin::ALL {
        for sha256 in [None, Some(sha256_hex(b"x"))] {
            records.push(record(
                0,
                Kind::Lost {
                    path: PATH.to_owned(),
                    origin,
                    sha256,
                },
            ));
        }
    }
    for &reason in HoldReason::ALL {
        records.push(record(
            0,
            Kind::Hold {
                path: PATH.to_owned(),
                reason,
            },
        ));
    }
    for &resolution in Resolution::ALL {
        records.push(record(
            0,
            Kind::HoldCleared {
                path: PATH.to_owned(),
                resolution,
            },
        ));
    }
    records.push(Record {
        at_ms: 0,
        kind: Kind::Verify {
            root: "cold-a".to_owned(),
            prefix_bytes: 0,
            findings: 0,
        },
    });
    records.push(Record {
        at_ms: layout::MAX_EVENT_TIME_MS,
        kind: Kind::Copy {
            path: PATH.to_owned(),
            root: "a".repeat(64),
        },
    });
    for record in records {
        let body = record.body().unwrap();
        assert_eq!(parse_body(&body).unwrap(), record, "{body}");
    }
    assert_eq!(
        Subject::File(PATH.to_owned()).render().unwrap(),
        PATH.to_owned()
    );
    assert_eq!(
        Subject::Entry(ENTRY.to_owned()).render().unwrap(),
        format!("entry:{ENTRY}")
    );
}

#[test]
fn records_outside_the_grammar_are_never_written() {
    let dir = store("ledger-invalid");
    let before = fs::read(ledger_path(dir.path())).unwrap();
    let bad = [
        record(
            0,
            Kind::Copy {
                path: PATH.to_owned(),
                root: "cold/a".to_owned(),
            },
        ),
        record(
            0,
            Kind::Copy {
                path: PATH.replace(".parquet", ".manifest"),
                root: "cold-a".to_owned(),
            },
        ),
        record(
            0,
            Kind::Manifest {
                text: "not a manifest\n".to_owned(),
            },
        ),
        Record {
            at_ms: -1,
            kind: Kind::Removed {
                partition: PARTITION.to_owned(),
            },
        },
        record(
            0,
            Kind::LedgerCopy {
                root: "cold-a".to_owned(),
                prefix_bytes: 1,
                prefix_sha256: sha256_hex(b"x").to_uppercase(),
            },
        ),
        record(
            0,
            Kind::Replaced {
                path: PATH.to_owned(),
                entry: "aggTrades/.x.import".to_owned(),
            },
        ),
    ];
    for record in bad {
        assert!(
            matches!(
                CopyLedger::at(dir.path())
                    .append(&record)
                    .map_err(|e| e.error),
                Err(RawStoreError::Invalid(_))
            ),
            "{record:?}"
        );
    }
    assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), before);
    assert!(!lock_path(dir.path()).exists());
}

#[test]
fn manifest_text_is_escaped_and_stored_verbatim() {
    let text = manifest_text();
    let escaped = escape(&text);
    assert!(escaped.starts_with("mie-raw-manifest%201%0Apath%20source=binance-um/"));
    assert!(!escaped.contains([' ', '\n']));
    assert_eq!(unescape(&escaped).unwrap(), text);
    assert_eq!(escape("50% \t\u{7f}é"), "50%25%20%09%7Fé");
    assert_eq!(unescape("50%25%20%09%7Fé").unwrap(), "50% \t\u{7f}é");
}

#[test]
fn non_canonical_lines_are_refused() {
    let manifest = format!("manifest {AT} {}", escape(&manifest_text()));
    let copy = format!("copy {AT} {PATH} cold-a");
    let sha = sha256_hex(b"x");
    let other_path = PATH.replace("part-00003", "part-3");
    let bodies = [
        // Spacing.
        format!("copy  {AT} {PATH} cold-a"),
        format!("{copy} "),
        format!(" {copy}"),
        // Time.
        format!("copy 0{AT} {PATH} cold-a"),
        format!("copy 253402300800000 {PATH} cold-a"),
        format!("copy -1 {PATH} cold-a"),
        format!("copy +{AT} {PATH} cold-a"),
        format!("copy {PATH} cold-a"),
        // Hex.
        format!("ledger-copy {AT} cold-a 12 {}", sha.to_uppercase()),
        format!("lost {AT} {PATH} journal {}", sha.to_uppercase()),
        format!("lost {AT} {PATH} journal {}", &sha[1..]),
        format!("lost {AT} {PATH} journal --"),
        format!("verify {AT} cold-a 012 0"),
        // Vocabulary.
        format!("copied {AT} {PATH} cold-a"),
        format!("finding {AT} cold-a broken {PATH}"),
        format!("finding {AT} cold-a missing something"),
        format!("finding {AT} cold-a missing entry:aggTrades/x.zip"),
        format!("finding {AT} cold-a missing entry:aggTrades/.x.import"),
        format!("finding {AT} cold-a missing entry:aggTrades/.import"),
        format!("finding {AT} cold-a missing Journal"),
        format!("hold {AT} {PATH} other"),
        format!("lost {AT} {PATH} network -"),
        format!("hold-cleared {AT} {PATH} delete"),
        format!("copy {AT} {PATH}"),
        format!("copy {AT} {PATH} cold-a extra"),
        format!("removed {AT} {PARTITION} extra"),
        // Escaping.
        manifest.replacen("depth", "%64epth", 1),
        manifest.replacen("%0A", "%0a", 1),
        format!("{manifest}%"),
        format!("{manifest}%2"),
        manifest.replacen("%20", "\t", 1),
        // Paths and ids.
        format!("copy {AT} {other_path} cold-a"),
        format!("copy {AT} {} cold-a", PATH.replace(".parquet", ".manifest")),
        format!("removed {AT} {}", PARTITION.replace("10-06", "13-06")),
        format!("removed {AT} {PATH}"),
        format!("copy {AT} {PATH} cold.a"),
        format!("quarantined {AT} {PATH} q/1"),
        format!("replaced {AT} {PATH} aggTrades/x.zip"),
        // Content.
        format!("manifest {AT} {}", escape("hello\n")),
        format!(
            "manifest {AT} {}",
            escape(&manifest_text().replace("rows 4217", "rows 04217"))
        ),
    ];
    for body in &bodies {
        for ledger in [
            ledger_of(&[body]),
            ledger_of(&[body, &copy]),
            ledger_of(&[&copy, body, &copy]),
        ] {
            assert_eq!(check_chain(&ledger).failure, None, "{body:?}");
            assert!(
                matches!(parsed(&ledger), Err(RawStoreError::Corrupt(_))),
                "{body:?}: {:?}",
                parsed(&ledger)
            );
        }
    }

    // An uppercase chain and a `\r\n` line end are refused by the chain
    // check: the chain field is compared byte for byte (a flipped letter
    // case is a one-bit flip), so they can never verify. Before another
    // record they are an integrity failure.
    let good = ledger_of(&[&copy, &copy]);
    let spans = spans(&good);
    let (offset, len) = spans[0];
    let mut upper = good.clone();
    upper[offset + len - CHAIN_LEN..offset + len].make_ascii_uppercase();
    let mut crlf = good[..offset + len].to_vec();
    crlf.extend_from_slice(b"\r");
    crlf.extend_from_slice(&good[offset + len..]);
    for ledger in [upper, crlf] {
        assert_eq!(
            check_chain(&ledger).failure,
            Some(ChainFailure {
                record: 1,
                offset: offset as u64
            })
        );
        assert!(matches!(parsed(&ledger), Err(RawStoreError::Integrity(_))));
    }
}

/// Flips byte `at` to a different byte of the same class (a hex digit stays
/// a hex digit, so the field keeps its shape).
fn flip(bytes: &mut [u8], at: usize) {
    bytes[at] = match bytes[at] {
        b'0' => b'1',
        b'a'..=b'f' | b'1'..=b'9' => b'0',
        other => other ^ 0x01,
    };
}

#[test]
fn a_flipped_byte_in_record_n_is_reported_as_record_n() {
    let dir = store("ledger-flip-base");
    let records = golden_records();
    append_all(dir.path(), &records);
    let original = fs::read(ledger_path(dir.path())).unwrap();
    let spans = spans(&original);
    let count = spans.len();
    assert!(count >= 6);
    let extra = record(
        100,
        Kind::Copy {
            path: PATH.to_owned(),
            root: "cold-b".to_owned(),
        },
    );

    for (index, &(offset, len)) in spans.iter().enumerate() {
        let n = index + 1;
        for in_chain in [false, true] {
            let at = if in_chain {
                offset + len - 1
            } else {
                offset + 1
            };
            let mut damaged = original.clone();
            flip(&mut damaged, at);
            let what = format!("record {n}, {}", if in_chain { "chain" } else { "body" });

            // The chain check names record n and its offset.
            let check = check_chain(&damaged);
            assert_eq!(
                check.failure,
                Some(ChainFailure {
                    record: n as u64,
                    offset: offset as u64
                }),
                "{what}"
            );
            assert_eq!(check.records, count as u64);

            // A reader ignores only a damaged last record.
            match parsed(&damaged) {
                Ok(ledger) => {
                    assert_eq!(n, count, "{what}");
                    assert_eq!(ledger.records, records[..count - 1], "{what}");
                    assert_eq!(ledger.ignored_bytes, len as u64 + 1);
                }
                Err(RawStoreError::Integrity(detail)) => {
                    assert!(n < count, "{what}");
                    assert!(detail.contains(&format!("record {n} ")), "{what}: {detail}");
                }
                Err(other) => panic!("{what}: {other}"),
            }

            // An appender truncates only a damaged last record. Damage it
            // sees before the last record is refused with the file
            // untouched; it reads only the last records, so older damage
            // is left for the readers and the verification to report, and
            // still never truncated.
            let target = store("ledger-flip");
            fs::write(ledger_path(target.path()), &damaged).unwrap();
            let result = CopyLedger::at(target.path())
                .append(&extra)
                .map_err(|e| e.error);
            let after = fs::read(ledger_path(target.path())).unwrap();
            let seen_by_appender = n == count - 1 || (in_chain && n == count - 2);
            if n == count {
                let outcome = result.unwrap();
                assert_eq!(
                    outcome.truncation,
                    Some(Truncation {
                        bytes: len as u64 + 1,
                        offset: offset as u64
                    }),
                    "{what}"
                );
                assert_eq!(outcome.offset, offset as u64);
                assert_eq!(&after[..offset], &original[..offset]);
                let check = check_chain(&after);
                assert_eq!((check.failure, check.records), (None, count as u64));
                let mut expected = records[..count - 1].to_vec();
                expected.push(extra.clone());
                assert_eq!(parsed(&after).unwrap().records, expected);
            } else if seen_by_appender {
                assert!(
                    matches!(result, Err(RawStoreError::Integrity(_))),
                    "{what}: {result:?}"
                );
                assert_eq!(after, damaged, "{what}: the file is untouched");
            } else {
                assert_eq!(result.unwrap().truncation, None, "{what}");
                assert_eq!(&after[..damaged.len()], &damaged[..], "{what}");
                assert_eq!(check_chain(&after).failure, check.failure, "{what}");
            }
        }
    }
}

#[test]
fn an_incomplete_trailing_record_is_ignored_then_truncated() {
    let records = golden_records();
    let (base_records, rest) = records.split_at(5);
    let base_dir = store("ledger-tail-base");
    append_all(base_dir.path(), base_records);
    let base = fs::read(ledger_path(base_dir.path())).unwrap();
    // The next record's line, as an append would write it.
    append_all(base_dir.path(), &rest[..1]);
    let full = fs::read(ledger_path(base_dir.path())).unwrap();
    let line = &full[base.len()..full.len() - 1];

    let check_tail = |tail: &[u8], what: &str| {
        let mut bytes = base.clone();
        bytes.extend_from_slice(tail);
        let ledger = parsed(&bytes).unwrap();
        assert_eq!(ledger.records, base_records, "{what}");
        assert_eq!(ledger.ignored_bytes, tail.len() as u64, "{what}");

        let dir = store("ledger-tail");
        fs::write(ledger_path(dir.path()), &bytes).unwrap();
        let outcome = CopyLedger::at(dir.path()).append(&rest[1]).unwrap();
        assert_eq!(
            outcome.truncation,
            Some(Truncation {
                bytes: tail.len() as u64,
                offset: base.len() as u64
            }),
            "{what}"
        );
        assert_eq!(outcome.offset, base.len() as u64, "{what}");
        let after = fs::read(ledger_path(dir.path())).unwrap();
        assert_eq!(check_chain(&after).failure, None, "{what}");
        let mut expected = base_records.to_vec();
        expected.push(rest[1].clone());
        assert_eq!(parsed(&after).unwrap().records, expected, "{what}");
    };
    // Every proper prefix of a record, without its line end.
    for k in 1..=line.len() {
        check_tail(&line[..k], &format!("prefix of {k} bytes"));
    }
    // A complete last line whose chain fails.
    let mut wrong = line.to_vec();
    flip(&mut wrong, line.len() - 1);
    wrong.push(b'\n');
    check_tail(&wrong, "a complete last line with a wrong chain");

    // A partial tail after a damaged complete line is refused, and the file
    // stays byte-identical.
    let mut bytes = base.clone();
    bytes.extend_from_slice(&wrong);
    bytes.extend_from_slice(b"copy 1");
    assert!(matches!(parsed(&bytes), Err(RawStoreError::Integrity(_))));
    let dir = store("ledger-tail-damaged");
    fs::write(ledger_path(dir.path()), &bytes).unwrap();
    assert!(matches!(
        CopyLedger::at(dir.path())
            .append(&rest[1])
            .map_err(|e| e.error),
        Err(RawStoreError::Integrity(_))
    ));
    assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), bytes);

    // A header-only ledger with a partial first record.
    let dir = store("ledger-tail-first");
    let mut bytes = HEADER.as_bytes().to_vec();
    bytes.extend_from_slice(b"manifest 17912");
    fs::write(ledger_path(dir.path()), &bytes).unwrap();
    assert_eq!(parsed(&bytes).unwrap().records, []);
    let outcome = CopyLedger::at(dir.path()).append(&records[0]).unwrap();
    assert_eq!(
        outcome.truncation,
        Some(Truncation {
            bytes: 14,
            offset: HEADER.len() as u64
        })
    );
    assert_eq!(read(dir.path()).unwrap().unwrap().records, records[..1]);
}

#[test]
fn appends_read_back_over_several_tail_steps() {
    // Manifest records are ten times the unit tests' read-back step: the
    // appender reads back through several steps and still finds the last
    // records.
    let dir = store("ledger-long");
    let mut records = Vec::new();
    for i in 0..5 {
        let mut text = manifest_text();
        text = text.replace("rows 4217", &format!("rows {}", 4_217 + i));
        records.push(record(i, Kind::Manifest { text }));
    }
    append_all(dir.path(), &records);
    let mut bytes = fs::read(ledger_path(dir.path())).unwrap();
    let spans = spans(&bytes);
    let (offset, len) = spans[spans.len() - 1];
    // Damage the last record: it is truncated.
    flip(&mut bytes, offset + len - 1);
    fs::write(ledger_path(dir.path()), &bytes).unwrap();
    let outcome = CopyLedger::at(dir.path()).append(&records[0]).unwrap();
    assert_eq!(
        outcome.truncation,
        Some(Truncation {
            bytes: len as u64 + 1,
            offset: offset as u64
        })
    );
    let after = fs::read(ledger_path(dir.path())).unwrap();
    assert_eq!(check_chain(&after).failure, None);
}

#[test]
fn a_bad_header_is_corrupt_and_a_missing_ledger_is_never_created() {
    let copy = format!("copy {AT} {PATH} cold-a");
    let good = ledger_of(&[&copy]);
    let headless = good[HEADER.len()..].to_vec();
    let mut wrong_version = good.clone();
    wrong_version[16] = b'2';
    for bytes in [
        headless,
        wrong_version,
        b"mie-copy".to_vec(),
        Vec::new(),
        b"mie-copy-ledger 1".to_vec(),
    ] {
        assert!(
            matches!(parsed(&bytes), Err(RawStoreError::Corrupt(_))),
            "{bytes:?}"
        );
        assert_eq!(
            check_chain(&bytes).failure,
            Some(ChainFailure {
                record: 0,
                offset: 0
            })
        );
        let dir = store("ledger-header");
        fs::write(ledger_path(dir.path()), &bytes).unwrap();
        assert!(matches!(
            CopyLedger::at(dir.path())
                .append(&golden_records()[1])
                .map_err(|e| e.error),
            Err(RawStoreError::Corrupt(_))
        ));
        assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), bytes);
    }

    let dir = TempDir::new("ledger-missing");
    assert_eq!(read(dir.path()).unwrap(), None);
    fs::create_dir(dir.path().join(META_DIR)).unwrap();
    assert_eq!(read(dir.path()).unwrap(), None);
    let result = CopyLedger::at(dir.path())
        .append(&golden_records()[1])
        .map_err(|e| e.error);
    assert!(
        matches!(result, Err(RawStoreError::Integrity(_))),
        "{result:?}"
    );
    assert_eq!(
        fs::read_dir(dir.path().join(META_DIR)).unwrap().count(),
        0,
        "neither the ledger nor its lock file is created"
    );
}

#[test]
fn a_whole_run_hold_excludes_other_holders_in_the_same_process() {
    let dir = store("ledger-hold");
    let ledger = CopyLedger::at(dir.path());
    let hold = ledger.try_hold().unwrap();
    assert!(matches!(
        CopyLedger::at(dir.path()).try_hold(),
        Err(RawStoreError::Locked(_))
    ));
    hold.append(&golden_records()[1]).unwrap();
    drop(hold);
    let again = ledger.try_hold().unwrap();
    drop(again);
    ledger.append(&golden_records()[2]).unwrap();
    assert_eq!(read(dir.path()).unwrap().unwrap().records.len(), 2);
}

/// #220: a whole-run hold dropped while another descriptor of its open
/// file description is open (a child process started by another thread,
/// between its fork and its exec) is free at once; while the hold lives,
/// the lock stays held after that descriptor closes.
#[test]
fn a_ledger_hold_is_free_after_its_drop_beside_a_duplicate() {
    let dir = store("ledger-hold-duplicate");
    let ledger = CopyLedger::at(dir.path());
    let hold = ledger.try_hold().unwrap();
    drop(hold.duplicate_lock());
    assert!(matches!(ledger.try_hold(), Err(RawStoreError::Locked(_))));
    let child = hold.duplicate_lock();
    drop(hold);
    let again = ledger.try_hold();
    assert!(again.is_ok(), "{again:?}");
    drop(child);
}

#[test]
fn a_failure_after_a_truncation_carries_the_truncation() {
    // Review of #197, finding 5: the truncation must reach the caller as
    // structured fields even when a step after it fails (a full hot, D10),
    // whether the write of the record fails or the fsync of the truncation.
    for (what, hook) in [
        ("the write", &FAIL_NEXT_WRITE),
        ("the fsync of the truncation", &FAIL_NEXT_TRUNCATE_SYNC),
    ] {
        let dir = store("ledger-truncate-then-fail");
        let records = golden_records();
        append_all(dir.path(), &records[..4]);
        let mut bytes = fs::read(ledger_path(dir.path())).unwrap();
        let spans = spans(&bytes);
        let (offset, len) = spans[spans.len() - 1];
        flip(&mut bytes, offset + len - 1);
        fs::write(ledger_path(dir.path()), &bytes).unwrap();

        hook.with(|fail| fail.set(true));
        let error = CopyLedger::at(dir.path()).append(&records[4]).unwrap_err();
        assert_eq!(
            error.truncation,
            Some(Truncation {
                bytes: len as u64 + 1,
                offset: offset as u64
            }),
            "{what}"
        );
        assert!(
            matches!(error.error, RawStoreError::Io(_)),
            "{what}: {error}"
        );
        let text = error.to_string();
        assert!(
            text.contains(&format!("({} bytes at byte {offset})", len + 1))
                && text.contains("truncated"),
            "{what}: {text}"
        );
        // The truncation is durable and the next append starts from it.
        assert_eq!(
            fs::read(ledger_path(dir.path())).unwrap(),
            &bytes[..offset],
            "{what}"
        );
        let outcome = CopyLedger::at(dir.path()).append(&records[4]).unwrap();
        assert_eq!(outcome.truncation, None, "{what}");
        assert_eq!(outcome.offset, offset as u64, "{what}");
    }

    // Without a truncation the error carries none and says nothing about
    // one, and an append without a truncation never fsyncs one.
    let dir = store("ledger-fail-without-truncation");
    let records = golden_records();
    append_all(dir.path(), &records[..2]);
    FAIL_NEXT_WRITE.with(|fail| fail.set(true));
    let error = CopyLedger::at(dir.path()).append(&records[2]).unwrap_err();
    assert_eq!(error.truncation, None);
    assert!(!error.to_string().contains("truncated"), "{error}");
    FAIL_NEXT_TRUNCATE_SYNC.with(|fail| fail.set(true));
    assert_eq!(
        CopyLedger::at(dir.path())
            .append(&records[2])
            .unwrap()
            .truncation,
        None
    );
    FAIL_NEXT_TRUNCATE_SYNC.with(|fail| fail.set(false));
}

/// A hot store (`<dir>/hot`) with a ledger of `records` and two copy
/// targets with `.mie/` (`<dir>/cold-a`, `<dir>/cold-b`).
fn replicated(tag: &str, records: &[Record]) -> (TempDir, PathBuf, [PathBuf; 2]) {
    let dir = TempDir::new(tag);
    let hot = dir.path().join("hot");
    let cold = [dir.path().join("cold-a"), dir.path().join("cold-b")];
    for root in std::iter::once(&hot).chain(&cold) {
        fs::create_dir_all(root.join(META_DIR)).unwrap();
    }
    fs::write(ledger_path(&hot), HEADER).unwrap();
    append_all(&hot, records);
    (dir, hot, cold)
}

/// One pass's step 3 over `targets`: extend each copy, then append its
/// `ledger-copy` record when it was not current. Returns whether each copy
/// was current.
fn ledger_step(hot: &Path, targets: &[(&Path, &str)], at_ms: i64) -> Vec<bool> {
    targets
        .iter()
        .map(|(root, id)| {
            let copy = extend_copy(hot, root, id).unwrap();
            if !copy.current {
                CopyLedger::at(hot)
                    .append(&Record {
                        at_ms,
                        kind: Kind::LedgerCopy {
                            root: (*id).to_owned(),
                            prefix_bytes: copy.prefix_bytes,
                            prefix_sha256: copy.prefix_sha256,
                        },
                    })
                    .unwrap();
            }
            copy.current
        })
        .collect()
}

#[test]
fn a_ledger_copy_ends_at_hots_verified_prefix() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-copy-torn", &records[..4]);
    let verified = fs::read(ledger_path(&hot)).unwrap();
    // The next record's line, torn: partial bytes, then a complete line
    // whose chain fails.
    append_all(&hot, &records[4..5]);
    let full = fs::read(ledger_path(&hot)).unwrap();
    let line = full[verified.len()..].to_vec();
    let mut wrong = line.clone();
    flip(&mut wrong, line.len() - 2);
    for (what, tail) in [("partial", &line[..20]), ("failing chain", &wrong[..])] {
        let mut bytes = verified.clone();
        bytes.extend_from_slice(tail);
        fs::write(ledger_path(&hot), &bytes).unwrap();
        let _ = fs::remove_file(ledger_path(&cold[0]));
        let copy = extend_copy(&hot, &cold[0], "cold-a").unwrap();
        assert!(!copy.current, "{what}");
        assert_eq!(copy.prefix_bytes, verified.len() as u64, "{what}");
        assert_eq!(copy.appended_bytes, verified.len() as u64, "{what}");
        assert_eq!(
            copy.prefix_sha256,
            hex_of(&Sha256::digest(&verified)),
            "{what}"
        );
        assert_eq!(fs::read(ledger_path(&cold[0])).unwrap(), verified, "{what}");
    }
}

#[test]
fn ledger_copies_converge_over_two_targets_and_two_passes() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-copy-converge", &records[..3]);
    let targets = [(cold[0].as_path(), "cold-a"), (cold[1].as_path(), "cold-b")];
    assert_eq!(ledger_step(&hot, &targets, AT), [false, false]);
    let after_pass_1 = fs::read(ledger_path(&hot)).unwrap();
    let copies = cold
        .clone()
        .map(|root| fs::read(ledger_path(&root)).unwrap());
    // Each copy is a prefix of hot's; cold-b's holds cold-a's record.
    for copy in &copies {
        assert_eq!(&after_pass_1[..copy.len()], &copy[..]);
    }
    assert!(copies[1].len() > copies[0].len());

    // A pass over unchanged state copies nothing and appends nothing.
    assert_eq!(ledger_step(&hot, &targets, AT + 1), [true, true]);
    assert_eq!(fs::read(ledger_path(&hot)).unwrap(), after_pass_1);
    for (root, copy) in cold.iter().zip(&copies) {
        assert_eq!(&fs::read(ledger_path(root)).unwrap(), copy);
    }

    // A new record: both copies are extended again, through the earlier
    // `ledger-copy` records.
    append_all(&hot, &records[3..4]);
    assert_eq!(ledger_step(&hot, &targets, AT + 2), [false, false]);
    let hot_bytes = fs::read(ledger_path(&hot)).unwrap();
    let copy_b = fs::read(ledger_path(&cold[1])).unwrap();
    assert_eq!(&hot_bytes[..copy_b.len()], &copy_b[..]);
    assert_eq!(ledger_step(&hot, &targets, AT + 3), [true, true]);
}

#[test]
fn a_crash_between_the_tail_and_its_record_is_completed_by_the_next_pass() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-copy-crash", &records[..3]);
    // The tail is appended, the record never written.
    let copy = extend_copy(&hot, &cold[0], "cold-a").unwrap();
    assert!(!copy.current);
    // The next pass appends nothing to the copy but writes the record.
    let again = extend_copy(&hot, &cold[0], "cold-a").unwrap();
    assert!(!again.current);
    assert_eq!(again.appended_bytes, 0);
    assert_eq!(again.prefix_bytes, copy.prefix_bytes);
    let targets = [(cold[0].as_path(), "cold-a")];
    assert_eq!(ledger_step(&hot, &targets, AT), [false]);
    assert_eq!(ledger_step(&hot, &targets, AT + 1), [true]);

    // A crash mid-append: a shorter byte prefix, extended by the next pass.
    let copy_path = ledger_path(&cold[1]);
    let hot_bytes = fs::read(ledger_path(&hot)).unwrap();
    fs::write(&copy_path, &hot_bytes[..HEADER.len() + 30]).unwrap();
    let copy = extend_copy(&hot, &cold[1], "cold-b").unwrap();
    assert!(!copy.current);
    assert_eq!(fs::read(&copy_path).unwrap(), hot_bytes);
}

#[test]
fn a_damaged_ledger_copy_is_refused_even_when_current() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-copy-damage", &records[..3]);
    let targets = [(cold[0].as_path(), "cold-a")];
    ledger_step(&hot, &targets, AT);
    let copy_path = ledger_path(&cold[0]);
    let mut copy = fs::read(&copy_path).unwrap();
    let last = copy.len() - 5;
    flip(&mut copy, last);
    fs::write(&copy_path, &copy).unwrap();
    for _ in 0..2 {
        assert!(matches!(
            extend_copy(&hot, &cold[0], "cold-a"),
            Err(CopyError {
                side: replica::Side::Destination,
                error: RawStoreError::Integrity(_)
            })
        ));
        assert_eq!(fs::read(&copy_path).unwrap(), copy, "untouched");
        // Also when hot has new records to copy.
        append_all(&hot, &records[3..4]);
    }
    // A hot without its ledger.
    fs::remove_file(ledger_path(&hot)).unwrap();
    assert!(matches!(
        extend_copy(&hot, &cold[1], "cold-b"),
        Err(CopyError {
            side: replica::Side::Source,
            error: RawStoreError::Integrity(_)
        })
    ));
    assert!(!ledger_path(&cold[1]).exists());
}

/// #191: a reader's ledger carries every record's end, its verified length
/// and its last chain, and a hold returns the records after such a prefix,
/// refusing a ledger that changed under it.
#[test]
fn records_after_a_plans_prefix() {
    let dir = store("records-after");
    let records = golden_records();
    let ledger = read(dir.path()).unwrap().unwrap();
    assert_eq!(ledger.verified_len, HEADER.len() as u64);
    assert_eq!(ledger.last_chain, genesis());
    assert!(ledger.ends.is_empty());
    let hold = CopyLedger::at(dir.path()).try_hold().unwrap();
    // From the header-only prefix.
    assert!(
        hold.records_after(ledger.verified_len, &ledger.last_chain)
            .unwrap()
            .is_empty()
    );
    for record in &records[..4] {
        hold.append(record).unwrap();
    }
    assert_eq!(
        hold.records_after(ledger.verified_len, &ledger.last_chain)
            .unwrap(),
        records[..4].to_vec()
    );
    drop(hold);

    // From a later prefix; the ends match the file.
    let bytes = fs::read(ledger_path(dir.path())).unwrap();
    let ledger = read(dir.path()).unwrap().unwrap();
    assert_eq!(ledger.verified_len, bytes.len() as u64);
    assert_eq!(*ledger.ends.last().unwrap(), bytes.len() as u64);
    for (i, &end) in ledger.ends.iter().enumerate() {
        assert_eq!(bytes[end as usize - 1], b'\n', "record {i}");
    }
    append_all(dir.path(), &records[4..6]);
    let hold = CopyLedger::at(dir.path()).try_hold().unwrap();
    assert_eq!(
        hold.records_after(ledger.verified_len, &ledger.last_chain)
            .unwrap(),
        records[4..6].to_vec()
    );

    // An incomplete trailing record is ignored.
    let path = ledger_path(dir.path());
    let mut tail = fs::read(&path).unwrap();
    tail.extend_from_slice(b"removed 17912484");
    fs::write(&path, &tail).unwrap();
    assert_eq!(
        hold.records_after(ledger.verified_len, &ledger.last_chain)
            .unwrap()
            .len(),
        2
    );

    // Another chain before the offset, a shorter file, a broken tail.
    let other = genesis();
    for (what, chain, offset) in [
        ("chain", other.as_str(), ledger.verified_len),
        ("shorter", ledger.last_chain.as_str(), tail.len() as u64 + 1),
    ] {
        let error = hold.records_after(offset, chain).unwrap_err();
        assert!(
            matches!(&error, RawStoreError::Integrity(m) if m.contains("changed since the plan")),
            "{what}: {error:?}"
        );
    }
    let mut broken = fs::read(&path).unwrap();
    flip(&mut broken, ledger.verified_len as usize + 3);
    fs::write(&path, &broken).unwrap();
    assert!(matches!(
        hold.records_after(ledger.verified_len, &ledger.last_chain),
        Err(RawStoreError::Integrity(_))
    ));
}

/// Every class of [`check_copy`] (#192 V4), in its precedence: absent,
/// good (a partial tail included), chain, diverged, longer.
#[test]
fn check_copy_classifies_every_ledger_copy() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-check-copy", &records[..6]);
    let [a, _] = &cold;
    assert_eq!(check_copy(&hot, a).unwrap(), LedgerCopyCheck::Absent);
    let hot_bytes = fs::read(ledger_path(&hot)).unwrap();
    let write = |bytes: &[u8]| fs::write(ledger_path(a), bytes).unwrap();

    write(&hot_bytes);
    assert_eq!(check_copy(&hot, a).unwrap(), LedgerCopyCheck::Good);
    // A copy that stopped mid-record is a prefix, no chain failure.
    write(&hot_bytes[..hot_bytes.len() - 7]);
    assert_eq!(check_copy(&hot, a).unwrap(), LedgerCopyCheck::Good);

    // A flipped byte in record 3 fails its chain there.
    let (offset, _) = spans(&hot_bytes)[2];
    let mut damaged = hot_bytes.clone();
    flip(&mut damaged, offset + 4);
    write(&damaged);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Chain {
            record: 3,
            offset: offset as u64
        }
    );

    // A valid chain that leaves hot's after record 4.
    let (end4, len4) = spans(&hot_bytes)[3];
    let prefix = &hot_bytes[..end4 + len4 + 1];
    let other = store("ledger-check-copy-other");
    fs::write(ledger_path(other.path()), prefix).unwrap();
    append_all(
        other.path(),
        &[record(
            50,
            Kind::Removed {
                partition: PARTITION.to_owned(),
            },
        )],
    );
    let diverged = fs::read(ledger_path(other.path())).unwrap();
    write(&diverged);
    let first = diverged
        .iter()
        .zip(&hot_bytes)
        .position(|(x, y)| x != y)
        .unwrap();
    assert!(first > end4 + len4, "it differs after record 4");
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Diverged {
            offset: first as u64
        }
    );

    // Hot's verified prefix plus a complete record hot lacks.
    let mut longer = hot_bytes.clone();
    let hot_ledger = parsed(&hot_bytes).unwrap();
    let body = format!("removed {} {PARTITION}", AT + 99);
    longer.extend_from_slice(
        format!(
            "{body} {}\n",
            chain_of(hot_ledger.last_chain.as_bytes(), body.as_bytes())
        )
        .as_bytes(),
    );
    write(&longer);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Longer {
            len: longer.len() as u64,
            verified: hot_bytes.len() as u64
        }
    );

    // The copy is read before hot: hot growing in between never makes a
    // good copy look longer or diverged.
    write(&hot_bytes);
    append_all(&hot, &records[6..8]);
    assert_eq!(check_copy(&hot, a).unwrap(), LedgerCopyCheck::Good);
}

/// Review of #203, finding 3: a ledger copy whose complete records all
/// equal hot's but whose unterminated tail does not (NULs where the last
/// line should be, left by a crash during `extend_prefix` on a filesystem
/// that exposes the extended size before the data: XFS, ext4 with
/// `data=writeback`) is [`LedgerCopyCheck::Tail`], which the repair
/// rebuilds, never `Diverged`: the tail is no record. A differing complete
/// record before such a tail is still `Diverged`, and a tail that runs past
/// hot's verified prefix into hot's own ignored trailing record is a tail
/// too, never `Longer`.
#[test]
fn a_damaged_unterminated_tail_is_a_tail_not_a_divergence() {
    let records = golden_records();
    let (_dir, hot, cold) = replicated("ledger-check-copy-tail", &records[..6]);
    let [a, _] = &cold;
    let hot_bytes = fs::read(ledger_path(&hot)).unwrap();
    let write = |bytes: &[u8]| fs::write(ledger_path(a), bytes).unwrap();
    let (last_start, _) = spans(&hot_bytes)[spans(&hot_bytes).len() - 1];

    // Every complete record of hot but the last, then 40 NULs.
    let mut nul_tail = hot_bytes[..last_start].to_vec();
    nul_tail.extend_from_slice(&[0u8; 40]);
    write(&nul_tail);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Tail {
            offset: last_start as u64
        }
    );

    // A tail that equals hot's bytes up to its last byte, then differs.
    let mut late = hot_bytes[..last_start + 9].to_vec();
    late.push(hot_bytes[last_start + 9] ^ 0x01);
    write(&late);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Tail {
            offset: last_start as u64
        }
    );

    // A differing complete record before the damaged tail stays Diverged:
    // its chain verifies (another valid chain), so it may be the last copy
    // of a record.
    let (start2, _) = spans(&hot_bytes)[1];
    let other = store("ledger-check-copy-tail-other");
    fs::write(ledger_path(other.path()), &hot_bytes[..start2]).unwrap();
    append_all(
        other.path(),
        &[record(
            50,
            Kind::Removed {
                partition: PARTITION.to_owned(),
            },
        )],
    );
    let mut diverged = fs::read(ledger_path(other.path())).unwrap();
    let first = diverged
        .iter()
        .zip(&hot_bytes)
        .position(|(x, y)| x != y)
        .unwrap();
    diverged.extend_from_slice(&[0u8; 12]);
    write(&diverged);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Diverged {
            offset: first as u64
        }
    );

    // Hot with an ignored partial trailing record; a copy holding hot's
    // verified prefix plus those partial bytes has only a tail past the
    // verified prefix, no record: a tail, never Longer.
    let mut torn = hot_bytes.clone();
    torn.extend_from_slice(b"removed 17");
    fs::write(ledger_path(&hot), &torn).unwrap();
    write(&torn);
    assert_eq!(
        check_copy(&hot, a).unwrap(),
        LedgerCopyCheck::Tail {
            offset: hot_bytes.len() as u64
        }
    );
    // The same copy cut back to a byte prefix of hot's verified prefix is
    // good.
    write(&hot_bytes[..last_start + 9]);
    assert_eq!(check_copy(&hot, a).unwrap(), LedgerCopyCheck::Good);
}

/// `append_unless` (review of #203, finding 1): the veto sees exactly the
/// records after the caller's read; a veto writes nothing, no veto
/// appends; a ledger changed before the read offset fails the append.
#[test]
fn append_unless_vetoes_on_the_records_after_the_read() {
    let records = golden_records();
    let dir = store("ledger-append-unless");
    append_all(dir.path(), &records[..3]);
    let at_read = read(dir.path()).unwrap().unwrap();
    append_all(dir.path(), &records[3..5]);
    let ledger = CopyLedger::at(dir.path());
    let before = fs::read(ledger_path(dir.path())).unwrap();
    let seen = std::cell::RefCell::new(Vec::new());
    let outcome = ledger
        .append_unless(
            &records[5],
            at_read.verified_len,
            &at_read.last_chain,
            &|tail| {
                seen.borrow_mut().extend_from_slice(tail);
                Some("held".to_owned())
            },
        )
        .unwrap();
    assert_eq!(outcome, Guarded::Vetoed("held".to_owned()));
    assert_eq!(*seen.borrow(), records[3..5]);
    assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), before);

    let outcome = ledger
        .append_unless(
            &records[5],
            at_read.verified_len,
            &at_read.last_chain,
            &|_| None,
        )
        .unwrap();
    assert!(matches!(outcome, Guarded::Appended(_)), "{outcome:?}");
    assert_eq!(read(dir.path()).unwrap().unwrap().records, records[..6]);

    let mut changed = fs::read(ledger_path(dir.path())).unwrap();
    // A byte of the chain hash that ends at the read offset.
    flip(&mut changed, at_read.verified_len as usize - 5);
    fs::write(ledger_path(dir.path()), &changed).unwrap();
    let error = ledger
        .append_unless(
            &records[6],
            at_read.verified_len,
            &at_read.last_chain,
            &|_| None,
        )
        .unwrap_err();
    assert!(
        matches!(&error.error, RawStoreError::Integrity(_)),
        "{error:?}"
    );
    assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), changed);
}

// ------------------------------------------------- the rebuild's writes (#193)

/// The builder (#193): from header-only, it writes byte for byte what the
/// appender writes; from a verified prefix with a torn trailing record, it
/// drops the torn bytes and continues the prefix's chain, and the result
/// parses back to the same records.
#[test]
fn builder_appends_records_that_parse_back() {
    let records = golden_records();
    let mut built = LedgerBuilder::empty();
    for record in &records {
        built.push(record).unwrap();
    }
    assert_eq!(built.added(), records.len() as u64);
    assert_eq!(built.finish(), GOLDEN.as_bytes());

    let dir = store("ledger-builder");
    append_all(dir.path(), &records[..4]);
    let mut prefix = fs::read(ledger_path(dir.path())).unwrap();
    let complete = prefix.len() as u64;
    prefix.extend_from_slice(b"removed 1 torn");
    let mut built = LedgerBuilder::from_prefix(Path::new("copy-ledger"), &prefix).unwrap();
    assert_eq!(built.len(), complete);
    assert_eq!(built.added(), 0);
    for record in &records[4..] {
        built.push(record).unwrap();
    }
    let bytes = built.finish();
    assert_eq!(bytes, GOLDEN.as_bytes());
    assert_eq!(parsed(&bytes).unwrap().records, records);

    // A record outside the grammar is refused and adds nothing.
    let mut built = LedgerBuilder::empty();
    let bad = record(
        0,
        Kind::Copy {
            path: "not/a/data/path".to_owned(),
            root: "cold-a".to_owned(),
        },
    );
    assert!(matches!(built.push(&bad), Err(RawStoreError::Invalid(_))));
    assert_eq!(built.finish(), HEADER.as_bytes());

    // A prefix that fails its chain before its last record is no start.
    let mut damaged = GOLDEN.as_bytes().to_vec();
    flip(&mut damaged, HEADER.len() + 3);
    assert!(matches!(
        LedgerBuilder::from_prefix(Path::new("copy-ledger"), &damaged),
        Err(RawStoreError::Integrity(_))
    ));
}

/// Every file under `.mie/` of `root` with its bytes, the lock file left
/// out.
fn meta_files(root: &Path) -> Vec<(String, Vec<u8>)> {
    fn walk(dir: &Path, base: &Path, out: &mut Vec<(String, Vec<u8>)>) {
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                walk(&path, base, out);
            } else if !path.to_string_lossy().ends_with(".lock") {
                let rel = path.strip_prefix(base).unwrap().display().to_string();
                out.push((rel, fs::read(&path).unwrap()));
            }
        }
    }
    let mut out = Vec::new();
    walk(&root.join(META_DIR), root, &mut out);
    out.sort();
    out
}

/// `replace` (#193 B9 step 5): bytes that fail their chain, their grammar,
/// or end in an incomplete record are refused with nothing touched; valid
/// bytes are installed after the old ledger is linked aside and the gate
/// written.
#[test]
fn replace_installs_only_a_ledger_that_parses() {
    let records = golden_records();
    let dir = store("ledger-replace");
    append_all(dir.path(), &records[..2]);
    let old = fs::read(ledger_path(dir.path())).unwrap();
    let hold = CopyLedger::at(dir.path()).try_hold().unwrap();
    let aside = dir.path().join(".mie/damaged/7/copy-ledger");
    let before = meta_files(dir.path());

    let mut flipped = GOLDEN.as_bytes().to_vec();
    flip(&mut flipped, HEADER.len() + 3);
    let mut torn = GOLDEN.as_bytes().to_vec();
    torn.extend_from_slice(b"removed 1 torn");
    for bad in [
        flipped,
        ledger_of(&["bogus 1 x"]),
        torn,
        b"mie-copy-ledger 2\n".to_vec(),
        Vec::new(),
    ] {
        let error = hold.replace(&bad, Some(&aside)).unwrap_err();
        assert!(matches!(error, RawStoreError::Invalid(_)), "{error:?}");
        assert_eq!(meta_files(dir.path()), before);
    }

    hold.replace(GOLDEN.as_bytes(), Some(&aside)).unwrap();
    assert_eq!(
        fs::read(ledger_path(dir.path())).unwrap(),
        GOLDEN.as_bytes()
    );
    assert_eq!(fs::read(&aside).unwrap(), old);
    assert_eq!(
        crate::rebuilt::read(dir.path()).unwrap(),
        Some(GOLDEN.len() as u64)
    );
    // Appends through the hold land in the installed ledger.
    hold.append(&records[0]).unwrap();
    assert_eq!(read(dir.path()).unwrap().unwrap().records.len(), 13);

    // Without a ledger on hot nothing is linked.
    let empty = TempDir::new("ledger-replace-none");
    fs::create_dir(empty.path().join(META_DIR)).unwrap();
    let hold = CopyLedger::at(empty.path()).try_hold().unwrap();
    let aside = empty.path().join(".mie/damaged/7/copy-ledger");
    hold.replace(HEADER.as_bytes(), Some(&aside)).unwrap();
    assert!(!aside.exists());
    assert_eq!(
        fs::read(ledger_path(empty.path())).unwrap(),
        HEADER.as_bytes()
    );
}

/// A crash after every step of `replace` (#193 B9 crash analysis) leaves
/// the old ledger in place, never a torn one, with at most the evidence
/// link, the gate and a temp file; a re-run at a later `at_ms` installs.
#[test]
fn a_crash_after_the_gate_before_the_rename_reruns_cleanly() {
    let records = golden_records();
    for step in ReplaceStep::ALL {
        let dir = store(&format!("ledger-replace-crash-{step:?}"));
        append_all(dir.path(), &records[..2]);
        let old = fs::read(ledger_path(dir.path())).unwrap();
        let hold = CopyLedger::at(dir.path()).try_hold().unwrap();
        REPLACE_CRASH_AFTER.with(|crash| crash.set(Some(step)));
        let first = dir.path().join(".mie/damaged/7/copy-ledger");
        assert!(hold.replace(GOLDEN.as_bytes(), Some(&first)).is_err());
        assert_eq!(REPLACE_CRASH_AFTER.with(std::cell::Cell::get), None);
        assert_eq!(fs::read(ledger_path(dir.path())).unwrap(), old, "{step:?}");
        assert_eq!(fs::read(&first).unwrap(), old, "{step:?}");
        let gate = crate::rebuilt::read(dir.path()).unwrap();
        if step == ReplaceStep::Link {
            assert_eq!(gate, None, "{step:?}");
        } else {
            assert_eq!(gate, Some(GOLDEN.len() as u64), "{step:?}");
        }

        let second = dir.path().join(".mie/damaged/8/copy-ledger");
        hold.replace(GOLDEN.as_bytes(), Some(&second)).unwrap();
        assert_eq!(
            fs::read(ledger_path(dir.path())).unwrap(),
            GOLDEN.as_bytes()
        );
        assert_eq!(fs::read(&second).unwrap(), old, "{step:?}");
        let names: Vec<String> = fs::read_dir(dir.path().join(META_DIR))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.ends_with(".tmp"))
            .collect();
        assert_eq!(names, Vec::<String>::new(), "{step:?}");
    }
}

/// An evidence name that exists already stops `replace` before anything
/// is written: evidence is never overwritten (#193 B9 step 3).
#[test]
fn existing_evidence_is_never_overwritten() {
    let dir = store("ledger-replace-evidence");
    append_all(dir.path(), &golden_records()[..2]);
    let aside = dir.path().join(".mie/damaged/7/copy-ledger");
    fs::create_dir_all(aside.parent().unwrap()).unwrap();
    fs::write(&aside, b"earlier evidence").unwrap();
    let before = meta_files(dir.path());
    let hold = CopyLedger::at(dir.path()).try_hold().unwrap();
    let error = hold.replace(GOLDEN.as_bytes(), Some(&aside)).unwrap_err();
    assert!(matches!(error, RawStoreError::Integrity(_)), "{error:?}");
    assert_eq!(meta_files(dir.path()), before);
    assert_eq!(fs::read(&aside).unwrap(), b"earlier evidence");
}
