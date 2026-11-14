//! The archive checks of the copy-ledger rebuild (ADR-062 D7 step 5a, #193
//! B7), shared with the hot restore's step 5 (D11, #195).
//!
//! One pure function, [`check`], over what the caller read: every copy of
//! every import-ledger entry, every archive part with the manifest texts
//! the roots (and the starting ledger) hold for it, and `PENDING`. Holds
//! are re-derived from these durable sources, never copied from a ledger,
//! so a full, a stale and an empty start give the same holds (D7 step 5a).
//! The hot restore calls it with `pending` = `None`: the restore never
//! resumes an import.
//!
//! - **Entry resolution.** Byte-identical copies are the current entry.
//!   Differing copies with exactly one variant that parses and lists only
//!   files whose SHA-256 equals a manifest text on some root: that variant
//!   is current, and every other copy is named (for `mie store repair` R8
//!   or the restore). Otherwise every part any parseable variant lists is
//!   held as `entry-conflict`. An entry found on the copy targets only
//!   counts as current (the union, as in the restore's step 2) and is
//!   named, since the importer reads hot's import ledger.
//! - **`unlisted`:** an archive part that no current entry lists, except
//!   the parts of `PENDING`'s stream dated in the importer's orphan window
//!   ([`orphan_days`]): the next import resumes those (ADR-034 D4). A
//!   `PENDING` that cannot be read or parsed exempts nothing and is named.
//! - **`manifest-conflict`:** a part whose texts differ (D5): two sealed
//!   files under one path, every copy left in place.
//! - **Precedence:** one reason per path, `manifest-conflict` >
//!   `entry-conflict` > `unlisted`.
//!
//! - **Generations** (ADR-062 D7 step 4, D11, proposed; #210 C7). The
//!   union's names are grouped by archive file (`<segment>`, F). Only the
//!   current generation's entry name (the highest entry generation at or
//!   above the highest supersede generation, over hot and the targets)
//!   goes through entry resolution; a superseded entry is no current
//!   entry, no conflict and no source of lost files. Copies of one
//!   supersede record that differ, or that do not parse, are named for
//!   `mie store repair` (R8), and its generation still counts; a record on
//!   the targets only counts and is named. A malformed name is named and
//!   counts for nothing.
//!
//! **Quarantine** ([`quarantine`], ADR-062 D7 step 5a, D11; #211 Q2–Q5),
//! one pure function shared with the hot restore (#195):
//!
//! - **Q2 The set Q**: every `moved` path of every supersede record with a
//!   quarantine (the union over hot and the targets: hot's copy when it
//!   parses, else the first target copy that parses), plus every path
//!   found in a root's `.quarantine/<id>/`. A moved path outside the
//!   layout's grammar, or no archive part, is a problem and never
//!   recorded. A path takes its record's id; a path found only in a
//!   quarantine directory takes that directory's id and is named. An id
//!   directory whose name is no valid segment counts for nothing.
//! - **Q3** The caller skips every Q path in B4 to B8: no `copy` record,
//!   no damage line, no lost candidate, no hold, no `removed` partition.
//! - **Q4 Records per Q path**: `manifest` when the start lacks a text
//!   (the text rule over the layouts' and the quarantines' texts, the
//!   superseded entry breaking a tie, read-only layouts included);
//!   `hold-cleared <path> replace` when the start holds it open, a record
//!   names it and it is complete (no layout of hot or of a copy target
//!   holds it; a read-only root never counts, #216); `quarantined <path>
//!   <id>` unless the start has it pending under that id; `replaced
//!   <path> <segment>/<E_g>` when it is complete, a record S_n names it,
//!   an entry of the lowest generation g ≥ n exists in the union and the
//!   start has no `replaced` for it.
//! - **Q5 Problems**, none derived from an absence, so the ledger is
//!   still installed: an incomplete path (the replace stopped between
//!   renames on hot or a copy target, which are named; an open start hold
//!   stays open), a path no root's layout or quarantine holds (its bytes
//!   are gone), a quarantined path no record names, and a complete path
//!   whose data file no quarantine of hot or of a copy target holds while
//!   a read-only root's layout does (h = 0, r ≥ 1, review of #221: the
//!   superseded bytes would stay only on roots mie never writes; the
//!   problem names `copy_target = true` and the resume). A read-only root
//!   whose layout holds a Q path gets an info line besides. Q4's records do
//!   not change with that problem: `complete` stays the commit point of
//!   I1 and I3, so a rebuild derives what the importer wrote (AW12).

use super::quarantine::{read_only_alone_line, read_only_line, superseded_shas, text_rule};
use super::replicate::is_archive_part;
use mie_adapter_binance::archive::catalog::parse_day;
use mie_adapter_binance::archive::import::orphan_days;
use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerKind, LedgerName, Pending, SupersedeRecord, current_generation,
    replacement_generation,
};
use mie_adapter_parquet::copy_ledger::{self, HoldReason, Kind, Resolution};
use mie_adapter_parquet::replica::{self, QuarantineListing};
use mie_adapter_parquet::verification::CopyState;
use mie_ports::raw::RawStreamKey;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
pub(crate) mod tests;

/// One copy of an import-ledger entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryCopy {
    /// The root that holds it: `hot`, or a copy target's id.
    pub root: String,
    /// Its bytes.
    pub bytes: Vec<u8>,
}

/// The manifest texts of one archive part.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartTexts {
    /// Per root that holds the part (hot first, then the cold roots in
    /// configuration order): its manifest text, which parses and describes
    /// the part's own location. A damaged text is no text.
    pub roots: Vec<(String, String)>,
    /// The starting ledger's `manifest` texts of the part.
    pub start: Vec<String>,
}

/// What [`check`] reads.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveInputs {
    /// Every copy of every entry, by name `<segment>/<file>.import`: hot's
    /// first, then the copy targets' in configuration order.
    pub entries: BTreeMap<String, Vec<EntryCopy>>,
    /// Every archive part on any root or recorded by the starting ledger.
    pub parts: BTreeMap<String, PartTexts>,
    /// Hot's `PENDING`: `None` when there is none, an error text when it
    /// cannot be read or parsed.
    pub pending: Option<Result<Pending, String>>,
}

/// What [`check`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ArchiveFindings {
    /// The current entries by name, with the files they list as `(data
    /// path, SHA-256)`.
    pub current: BTreeMap<String, Vec<(String, String)>>,
    /// The held parts with their one reason.
    pub holds: BTreeMap<String, HoldReason>,
    /// The parts `PENDING` exempts from `unlisted`.
    pub exempt: BTreeSet<String>,
    /// Every finding to report: differing entry copies, an entry on the
    /// targets only, conflicting entries, unparseable entries and
    /// `PENDING`.
    pub problems: Vec<String>,
    /// Per current entry name that entry resolution decided, the bytes of
    /// its current variant: the only one, or the one whose files match the
    /// manifests. The hot restore writes them to hot (#195 H4); an entry
    /// held `entry-conflict` has none.
    pub chosen: BTreeMap<String, Vec<u8>>,
}

/// The archive checks (module docs).
pub fn check(inputs: &ArchiveInputs) -> ArchiveFindings {
    let mut found = ArchiveFindings::default();
    let mut conflicting: BTreeSet<String> = BTreeSet::new();
    // Per (segment, archive file): every name with its generation and kind.
    let mut files: BTreeMap<(&str, String), Vec<(LedgerName, &str)>> = BTreeMap::new();
    for name in inputs.entries.keys() {
        let parsed = name
            .split_once('/')
            .ok_or_else(|| format!("{name:?} has no segment"))
            .and_then(|(segment, file)| {
                LedgerName::parse(file)
                    .map(|parsed| (segment, parsed))
                    .map_err(|e| e.to_string())
            });
        match parsed {
            Ok((segment, parsed)) => files
                .entry((segment, parsed.file.clone()))
                .or_default()
                .push((parsed, name.as_str())),
            Err(error) => found.problems.push(format!(
                "import-ledger file {name}: {error}; it counts for nothing (ADR-062 D11)"
            )),
        }
    }
    for names in files.values() {
        let current = current_generation(names.iter().map(|(n, _)| (n.generation, n.kind)));
        for (parsed, name) in names {
            let copies = &inputs.entries[*name];
            match parsed.kind {
                LedgerKind::Supersede => check_record(name, copies, &mut found),
                LedgerKind::Entry if Some(parsed.generation) == current => {
                    resolve(name, copies, inputs, &mut found, &mut conflicting);
                }
                LedgerKind::Entry => {}
            }
        }
    }
    let listed: BTreeSet<&str> = found
        .current
        .values()
        .flatten()
        .map(|(path, _)| path.as_str())
        .collect();
    let pending = match &inputs.pending {
        None => None,
        Some(Ok(pending)) => Some(pending),
        Some(Err(error)) => {
            found.problems.push(format!(
                "the import ledger's PENDING cannot be read or parsed ({error}): it exempts \
                 nothing, so every archive part no entry lists is held (ADR-034 D4, ADR-062 D11)"
            ));
            None
        }
    };
    for (path, texts) in &inputs.parts {
        let distinct: BTreeSet<&str> = texts
            .roots
            .iter()
            .map(|(_, text)| text.as_str())
            .chain(texts.start.iter().map(String::as_str))
            .collect();
        let reason = if distinct.len() > 1 {
            Some(HoldReason::ManifestConflict)
        } else if conflicting.contains(path) {
            Some(HoldReason::EntryConflict)
        } else if listed.contains(path.as_str()) {
            None
        } else if pending.is_some_and(|pending| resumes(pending, path)) {
            found.exempt.insert(path.clone());
            None
        } else {
            Some(HoldReason::Unlisted)
        };
        if let Some(reason) = reason {
            found.holds.insert(path.clone(), reason);
        }
    }
    for path in &conflicting {
        if !inputs.parts.contains_key(path) {
            found.problems.push(format!(
                "{path}: listed by a conflicting import entry, and no root holds it; it is \
                 neither held nor recorded lost (ADR-062 D11)"
            ));
        }
    }
    found
}

/// A supersede record's copies: named when they are not on hot, differ or
/// do not parse. Its generation counts whatever its bytes (G4 goes by
/// names).
fn check_record(name: &str, copies: &[EntryCopy], found: &mut ArchiveFindings) {
    if copies.is_empty() {
        return;
    }
    if !copies.iter().any(|copy| copy.root == "hot") {
        let roots: Vec<&str> = copies.iter().map(|copy| copy.root.as_str()).collect();
        found.problems.push(format!(
            "supersede record {name}: on {} only, not on hot, where the importer reads it; it \
             counts (ADR-062 D11)",
            roots.join(", ")
        ));
    }
    let mut variants: Vec<&[u8]> = copies.iter().map(|copy| copy.bytes.as_slice()).collect();
    variants.sort_unstable();
    variants.dedup();
    if variants.len() > 1 {
        let roots: Vec<String> = copies
            .iter()
            .map(|copy| {
                let variant = variants
                    .iter()
                    .position(|v| *v == copy.bytes.as_slice())
                    .unwrap_or(0);
                format!("{} (variant {})", copy.root, variant + 1)
            })
            .collect();
        found.problems.push(format!(
            "supersede record {name}: its copies differ ({}); `mie store repair` replaces a \
             damaged copy from hot (R8), and its generation still counts (ADR-062 D11)",
            roots.join(", ")
        ));
    }
    for copy in copies {
        let parses = std::str::from_utf8(&copy.bytes)
            .ok()
            .is_some_and(|text| SupersedeRecord::parse(text).is_ok());
        if !parses {
            found.problems.push(format!(
                "supersede record {name} on {}: it does not parse; its generation still counts \
                 (ADR-062 D11)",
                copy.root
            ));
        }
    }
}

/// Entry resolution for the copies of `name`.
fn resolve(
    name: &str,
    copies: &[EntryCopy],
    inputs: &ArchiveInputs,
    found: &mut ArchiveFindings,
    conflicting: &mut BTreeSet<String>,
) {
    if copies.is_empty() {
        return;
    }
    if !copies.iter().any(|copy| copy.root == "hot") {
        let roots: Vec<&str> = copies.iter().map(|copy| copy.root.as_str()).collect();
        found.problems.push(format!(
            "import entry {name}: on {} only, not on hot, where the importer reads it; it counts \
             as current (ADR-062 D11)",
            roots.join(", ")
        ));
    }
    // The distinct variants, in the order of their first copy.
    let mut variants: Vec<(&[u8], Vec<&str>)> = Vec::new();
    for copy in copies {
        match variants.iter_mut().find(|(bytes, _)| *bytes == copy.bytes) {
            Some((_, roots)) => roots.push(&copy.root),
            None => variants.push((&copy.bytes, vec![&copy.root])),
        }
    }
    let parsed: Vec<Option<Vec<(String, String)>>> = variants
        .iter()
        .map(|(bytes, _)| {
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| ImportLedger::parse(text).ok())
                .map(|ledger| {
                    ledger
                        .files
                        .into_iter()
                        .map(|file| (file.relative_path, file.sha256))
                        .collect()
                })
        })
        .collect();
    let roots_of = |i: usize| variants[i].1.join(", ");
    if variants.len() == 1 {
        found.chosen.insert(name.to_owned(), variants[0].0.to_vec());
        match &parsed[0] {
            Some(files) => {
                found.current.insert(name.to_owned(), files.clone());
            }
            None => found.problems.push(format!(
                "import entry {name} on {}: it cannot be parsed, so it lists nothing (ADR-034 D4)",
                roots_of(0)
            )),
        }
        return;
    }
    let matching: Vec<usize> = (0..variants.len())
        .filter(|&i| {
            parsed[i].as_ref().is_some_and(|files| {
                files
                    .iter()
                    .all(|(path, sha)| on_some_root(inputs, path, sha))
            })
        })
        .collect();
    if let [only] = matching[..] {
        found
            .current
            .insert(name.to_owned(), parsed[only].clone().unwrap_or_default());
        found
            .chosen
            .insert(name.to_owned(), variants[only].0.to_vec());
        for i in (0..variants.len()).filter(|&i| i != only) {
            found.problems.push(format!(
                "import entry {name} on {}: it differs from the copy on {}, whose files match the \
                 manifests on the roots; `mie store repair` replaces it (ADR-062 D11)",
                roots_of(i),
                roots_of(only)
            ));
        }
        return;
    }
    let variants_text: Vec<String> = (0..variants.len())
        .map(|i| {
            let state = if parsed[i].is_some() {
                "parses"
            } else {
                "does not parse"
            };
            format!("{} ({state})", roots_of(i))
        })
        .collect();
    found.problems.push(format!(
        "import entry {name}: its copies differ ({}) and not exactly one of them lists files \
         matching the manifests on the roots, so every part they list is held as entry-conflict \
         (ADR-062 D11)",
        variants_text.join("; ")
    ));
    for files in parsed.iter().flatten() {
        conflicting.extend(files.iter().map(|(path, _)| path.clone()));
    }
}

/// Whether some root holds a manifest text of `path` with the SHA-256
/// `sha`.
fn on_some_root(inputs: &ArchiveInputs, path: &str, sha: &str) -> bool {
    inputs.parts.get(path).is_some_and(|texts| {
        texts
            .roots
            .iter()
            .any(|(_, text)| replica::parse_manifest(text).is_ok_and(|file| file.sha256 == sha))
    })
}

/// Whether `path` is a part of `pending`'s stream dated inside the
/// importer's orphan window of its period.
fn resumes(pending: &Pending, path: &str) -> bool {
    let mut segments = path.split('/');
    let mut value = |prefix: &str| segments.next().and_then(|s| s.strip_prefix(prefix));
    let (Some(source), Some(instrument), Some(stream), Some(date)) = (
        value("source="),
        value("instrument="),
        value("stream="),
        value("date="),
    ) else {
        return false;
    };
    let (first, last) = orphan_days(pending.period);
    RawStreamKey::new(source, instrument, stream).is_ok_and(|key| key == pending.stream)
        && parse_day(date).is_some_and(|day| (first..=last).contains(&day))
}

// ----------------------------------------------------------- quarantine

/// One root's `.quarantine/` directory as the caller read it (Q1).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RootQuarantine {
    /// `hot`, or a cold root's id.
    pub root: String,
    /// Its listing.
    pub listing: QuarantineListing,
}

/// What [`quarantine`] reads.
#[derive(Debug, Clone, Copy)]
pub struct QuarantineInputs<'a> {
    /// Every copy of every import-ledger file by name, hot's first.
    pub entries: &'a BTreeMap<String, Vec<EntryCopy>>,
    /// Hot's quarantine first, then every cold root's in configuration
    /// order.
    pub roots: &'a [RootQuarantine],
    /// Per data path, the roots whose layout holds it (a manifest or a
    /// data file), with the text when the manifest parses and describes
    /// the path.
    pub layout: &'a BTreeMap<String, Vec<(String, Option<String>)>>,
    /// The marker ids of the configured read-only roots: their layouts
    /// feed the text rule but never the commit point (#216). Matched
    /// exactly; `hot` always counts.
    pub read_only: &'a BTreeSet<String>,
    /// Per data path, the roots of `layout` known to hold its manifest
    /// without its data file (the data checked and missing). Every other
    /// root of `layout` counts as holding the data, so an unchecked copy
    /// fails closed in Q5's h = 0 rule (review of #221).
    pub without_data: &'a BTreeMap<String, BTreeSet<String>>,
    /// The starting ledger, folded.
    pub start: &'a CopyState,
}

/// One path of the set Q.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QuarantinedPath {
    /// Its quarantine id.
    pub id: String,
    /// The supersede record naming it, `<segment>/<name>`; `None` when
    /// only a quarantine directory holds it.
    pub record: Option<String>,
    /// No layout of hot or of a copy target holds it.
    pub complete: bool,
}

/// What [`quarantine`] found.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuarantineFindings {
    /// The set Q.
    pub paths: BTreeMap<String, QuarantinedPath>,
    /// The records to add, in Q6's order: `manifest`, `hold-cleared`,
    /// `quarantined`, `replaced`, each group in path order.
    pub records: Vec<Kind>,
    /// One info line per (Q path, read-only root whose layout holds it),
    /// in path order, then layout order; then one report line per
    /// quarantine id.
    pub lines: Vec<String>,
    /// Every Q5 problem.
    pub problems: Vec<String>,
}

/// A supersede record with a quarantine, as Q2 reads it.
struct Moving<'a> {
    /// `<segment>/<name>`.
    label: String,
    segment: &'a str,
    name: LedgerName,
    record: SupersedeRecord,
}

/// Q2 to Q5 (module docs), pure.
pub fn quarantine(inputs: &QuarantineInputs<'_>) -> QuarantineFindings {
    let mut found = QuarantineFindings::default();
    // Q2: the records, the union over hot and the targets.
    let mut records: Vec<Moving<'_>> = Vec::new();
    for (label, copies) in inputs.entries {
        let Some((segment, file)) = label.split_once('/') else {
            continue;
        };
        let Ok(name) = LedgerName::parse(file) else {
            continue;
        };
        if name.kind != LedgerKind::Supersede {
            continue;
        }
        let parse = |copy: &EntryCopy| {
            std::str::from_utf8(&copy.bytes)
                .ok()
                .and_then(|text| SupersedeRecord::parse(text).ok())
        };
        let record = copies
            .iter()
            .filter(|copy| copy.root == "hot")
            .find_map(parse)
            .or_else(|| copies.iter().find_map(parse));
        if let Some(record) = record
            && record.quarantine.is_some()
        {
            records.push(Moving {
                label: label.clone(),
                segment,
                name,
                record,
            });
        }
    }
    // Per Q path: its record (the highest generation naming it) or its
    // directory's id.
    let mut by_path: BTreeMap<String, (String, Option<usize>)> = BTreeMap::new();
    for (i, moving) in records.iter().enumerate() {
        let quarantine = moving.record.quarantine.as_ref().expect("filtered above");
        for path in &quarantine.moved {
            if !copy_ledger::is_data_path(path) || !is_archive_part(path) {
                found.problems.push(format!(
                    "{path:?}, moved by supersede record {}, is no archive data path of the \
                     layout: reported, never recorded (ADR-062 D7 step 5a)",
                    moving.label
                ));
                continue;
            }
            let newer = by_path.get(path).is_none_or(|(_, known)| {
                known.is_none_or(|j| records[j].name.generation < moving.name.generation)
            });
            if newer {
                by_path.insert(path.clone(), (quarantine.id.clone(), Some(i)));
            }
        }
    }
    for root in inputs.roots {
        for name in &root.listing.bad_names {
            found.problems.push(format!(
                "{}/{name:?} on {}: no valid quarantine id (a look-alike or invisible character, \
                 a blank); its files count for nothing (ADR-062 D7 step 5a)",
                replica::QUARANTINE_DIR,
                root.root
            ));
        }
        for (id, parts) in &root.listing.ids {
            for path in parts.keys() {
                if !by_path.contains_key(path) {
                    by_path.insert(path.clone(), (id.clone(), None));
                }
            }
        }
    }

    // Q4, Q5.
    let mut groups: [Vec<Kind>; 4] = Default::default();
    let mut per_id: BTreeMap<&str, (usize, usize)> = BTreeMap::new();
    for (path, (id, record)) in &by_path {
        let layout: Vec<&(String, Option<String>)> =
            inputs.layout.get(path).into_iter().flatten().collect();
        // Hot and the copy targets are the commit point; a read-only root
        // is never written, so its copy can never move (#216).
        let (counted, read_only): (Vec<&str>, Vec<&str>) = layout
            .iter()
            .map(|(root, _)| root.as_str())
            .partition(|root| *root == "hot" || !inputs.read_only.contains(*root));
        let complete = counted.is_empty();
        let quarantined: Vec<(&str, Option<&str>)> = inputs
            .roots
            .iter()
            .filter_map(|root| {
                root.listing
                    .ids
                    .get(id)
                    .and_then(|parts| parts.get(path))
                    .map(|copy| (root.root.as_str(), copy.text.as_deref()))
            })
            .collect();
        let moving = record.map(|i| &records[i]);
        let counts = per_id.entry(id).or_default();
        if complete {
            counts.0 += 1;
        } else {
            counts.1 += 1;
        }
        // The text rule.
        if inputs.start.ledger_text(path).is_none() {
            let texts = layout
                .iter()
                .filter_map(|(_, text)| text.as_deref())
                .chain(quarantined.iter().filter_map(|(_, text)| *text))
                .filter(|text| {
                    replica::parse_manifest(text).is_ok_and(|file| file.relative_path == *path)
                });
            let listed = moving.map_or_else(BTreeSet::new, |moving| {
                let files: Vec<(LedgerName, Vec<u8>)> =
                    files_of(inputs.entries, moving.segment, &moving.name.file);
                superseded_shas(&moving.record, moving.segment, &files, path)
            });
            if let Some(text) = text_rule(texts, &listed) {
                groups[0].push(Kind::Manifest { text });
            }
        }
        if moving.is_some() && complete && inputs.start.hold_reason(path).is_some() {
            groups[1].push(Kind::HoldCleared {
                path: path.clone(),
                resolution: Resolution::Replace,
            });
        }
        if inputs.start.quarantine(path) != Some(id.as_str()) {
            groups[2].push(Kind::Quarantined {
                path: path.clone(),
                quarantine: id.clone(),
            });
        }
        if let Some(moving) = moving
            && complete
            && inputs.start.replaced(path).is_none()
        {
            let names: Vec<(u32, LedgerKind)> =
                names_of(inputs.entries, moving.segment, &moving.name.file);
            if let Some(g) = replacement_generation(moving.name.generation, names) {
                groups[3].push(Kind::Replaced {
                    path: path.clone(),
                    entry: format!(
                        "{}/{}",
                        moving.segment,
                        LedgerName::entry(&moving.name.file, g).render()
                    ),
                });
            }
        }
        match moving {
            None => found.problems.push(format!(
                "{path} is in {}/{id}/ and no supersede record names it: it is recorded \
                 quarantined under {id} and never listed again (ADR-062 D7 step 5a)",
                replica::QUARANTINE_DIR
            )),
            Some(moving) if !complete => {
                found.problems.push(format!(
                    "{path}: the replace of {} under {id} stopped between renames ({} still \
                     hold it); `mie store resolve --file {}` completes it, and its hold stays \
                     open until then (ADR-062 D11)",
                    moving.record.archive,
                    counted.join(", "),
                    moving.record.archive
                ));
            }
            Some(_) => {}
        }
        for root in &read_only {
            found.lines.push(read_only_line(path, root, id));
        }
        // The h = 0 rule (review of #221, finding 1): a complete path whose
        // data file no quarantine of hot or of a copy target holds, while a
        // read-only layout does. An incomplete one already has its problem,
        // and the resume it names applies the rule.
        let without_data = inputs.without_data.get(path);
        let read_only_data: Vec<String> = read_only
            .iter()
            .filter(|root| without_data.is_none_or(|roots| !roots.contains(**root)))
            .map(|root| (*root).to_owned())
            .collect();
        let writable_data = inputs.roots.iter().any(|root| {
            (root.root == "hot" || !inputs.read_only.contains(&root.root))
                && root
                    .listing
                    .ids
                    .get(id)
                    .and_then(|parts| parts.get(path))
                    .is_some_and(|copy| copy.data)
        });
        if let Some(moving) = moving
            && complete
            && !writable_data
            && !read_only_data.is_empty()
        {
            found.problems.push(read_only_alone_line(
                path,
                &read_only_data,
                id,
                &moving.record.archive,
            ));
        }
        if complete && quarantined.is_empty() && read_only.is_empty() {
            found.problems.push(format!(
                "{path}: quarantined under {id}, and no root's layout or quarantine holds it: its \
                 bytes are gone; nothing in mie deletes them (ADR-062 D5)"
            ));
        }
        found.paths.insert(
            path.clone(),
            QuarantinedPath {
                id: id.clone(),
                record: moving.map(|moving| moving.label.clone()),
                complete,
            },
        );
    }
    for (id, (complete, incomplete)) in per_id {
        found.lines.push(format!(
            "quarantine {id}: {} path(s), {complete} complete, {incomplete} incomplete",
            complete + incomplete
        ));
    }
    found.records = groups.into_iter().flatten().collect();
    found
}

/// Every copy of the import-ledger files of archive file `file` in
/// `segment`, as (name, bytes).
fn files_of(
    entries: &BTreeMap<String, Vec<EntryCopy>>,
    segment: &str,
    file: &str,
) -> Vec<(LedgerName, Vec<u8>)> {
    let mut files = Vec::new();
    for (label, copies) in entries {
        if let Some((s, name)) = label.split_once('/')
            && s == segment
            && let Ok(name) = LedgerName::parse(name)
            && name.file == file
        {
            files.extend(copies.iter().map(|copy| (name.clone(), copy.bytes.clone())));
        }
    }
    files
}

/// The generation and kind of every import-ledger name of `file` in
/// `segment`, over hot and the targets.
fn names_of(
    entries: &BTreeMap<String, Vec<EntryCopy>>,
    segment: &str,
    file: &str,
) -> Vec<(u32, LedgerKind)> {
    entries
        .keys()
        .filter_map(|label| {
            let (s, name) = label.split_once('/')?;
            let name = LedgerName::parse(name).ok()?;
            (s == segment && name.file == file).then_some((name.generation, name.kind))
        })
        .collect()
}
