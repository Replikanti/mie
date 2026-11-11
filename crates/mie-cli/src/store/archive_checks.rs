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
//! The quarantine listings join these inputs with the replace resolution
//! (#211).

use mie_adapter_binance::archive::catalog::parse_day;
use mie_adapter_binance::archive::import::orphan_days;
use mie_adapter_binance::archive::ledger::{
    ImportLedger, LedgerKind, LedgerName, Pending, SupersedeRecord, current_generation,
};
use mie_adapter_parquet::copy_ledger::HoldReason;
use mie_adapter_parquet::replica;
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
