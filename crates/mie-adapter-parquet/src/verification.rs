//! The copy state of a tiered store, folded from one copy-ledger read
//! (ADR-062 D6, D11, #192).
//!
//! One definition, read by the hot removal ([`removal::plan`](crate::removal::plan)),
//! the replication pass (its copy-ledger hold), `mie store verify` and
//! `mie store repair`, so the four never disagree on which copy counts.
//! The records are folded in file order:
//!
//! - **A data copy** (path, root) counts from its first `copy` record after
//!   the latest `finding` about that file on that root: any finding voids
//!   the copy until a later `copy` record (the remover's rule, review of
//!   #202, finding 3). A finding after the latest `copy` record is *open*;
//!   only a `copy` record closes it.
//! - **A ledger copy** of root R counts from its latest `ledger-copy R`
//!   record, unless a `finding R … copy-ledger` follows it (L1 of #192):
//!   then the finding is open and R holds no verified prefix until a later
//!   `ledger-copy R` record.
//! - **A journal or entry finding** on R is open until a later `verify R`
//!   pass that recorded no finding about the same subject. Neither has a
//!   copy record of its own, so a clean pass is what closes it.
//! - **A verification pass** is the `verify R` record together with the
//!   `finding R` records before it that carry the same `at_ms`; findings of
//!   a pass that never wrote its `verify` record (it stopped on an I/O
//!   error) belong to no pass.
//!
//! The path verdicts are those of the union `select` ([`tiers`](crate::tiers)).

use crate::copy_ledger::{FindingCode, Kind, Ledger, Record, Subject};
use crate::manifest;
use crate::tiers::{self, LedgerView, Verdict};
use mie_ports::raw::RawStoreError;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
mod tests;

/// One recorded path's verdict, in `select`'s precedence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathVerdict {
    /// An ordinary file with a `manifest` record.
    Ordinary,
    /// Recorded without a `manifest` record.
    NoManifest,
    /// `lost`.
    Lost,
    /// Two different manifest texts (D5).
    Conflict,
    /// Held.
    Held,
    /// Replaced by a re-import's entry.
    Replaced,
    /// A replacement is pending.
    Pending,
}

impl From<Verdict> for PathVerdict {
    fn from(verdict: Verdict) -> Self {
        match verdict {
            Verdict::Ordinary(_) => Self::Ordinary,
            Verdict::NoManifest => Self::NoManifest,
            Verdict::Lost => Self::Lost,
            Verdict::Conflict => Self::Conflict,
            Verdict::Held(_) => Self::Held,
            Verdict::Replaced => Self::Replaced,
            Verdict::Pending => Self::Pending,
        }
    }
}

/// What the ledger records of one data copy (path, root).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataCopy {
    /// The end of the first `copy` record after the latest finding about
    /// this copy: `None` while a finding is open, or before any `copy`.
    pub valid_end: Option<u64>,
    /// The end and `at_ms` of the latest `copy` record, if any.
    pub latest: Option<(u64, i64)>,
    /// The code of the open finding, if one is open.
    pub open: Option<FindingCode>,
}

/// One verification pass over a root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifyPass {
    /// Its `at_ms`, shared with its findings.
    pub at_ms: i64,
    /// The ledger prefix it covered, in bytes.
    pub prefix_bytes: u64,
    /// The data paths it recorded findings about.
    pub files: BTreeSet<String>,
}

/// The copy state of one ledger read (module docs).
#[derive(Debug, Clone)]
pub struct CopyState {
    view: LedgerView,
    removed: BTreeSet<String>,
    manifest_end: BTreeMap<String, u64>,
    copies: BTreeMap<String, BTreeMap<String, DataCopy>>,
    /// Per root: the latest `ledger-copy` prefix, `None` once a copy-ledger
    /// finding voids it.
    ledger_copies: BTreeMap<String, Option<u64>>,
    open_ledger: BTreeSet<String>,
    open_journal: BTreeSet<String>,
    open_entries: BTreeMap<String, BTreeSet<String>>,
    verifies: BTreeMap<String, Vec<VerifyPass>>,
}

/// The roots with an open copy-ledger finding: a `finding R … copy-ledger`
/// after R's latest `ledger-copy R` record (L1 of #192). The replication
/// pass reads this from its own ledger read (L2).
pub fn open_ledger_findings(records: &[Record]) -> BTreeSet<String> {
    let mut open = BTreeSet::new();
    for record in records {
        match &record.kind {
            Kind::LedgerCopy { root, .. } => {
                open.remove(root);
            }
            Kind::Finding {
                root,
                subject: Subject::CopyLedger,
                ..
            } => {
                open.insert(root.clone());
            }
            _ => {}
        }
    }
    open
}

impl CopyState {
    /// Folds `ledger` (module docs).
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Corrupt`] for a `manifest` text that does not
    /// parse; the ledger parser has already refused it.
    pub fn fold(ledger: &Ledger) -> Result<Self, RawStoreError> {
        let mut state = Self {
            view: LedgerView::fold(ledger)?,
            removed: tiers::removed_partitions(ledger),
            manifest_end: BTreeMap::new(),
            copies: BTreeMap::new(),
            ledger_copies: BTreeMap::new(),
            open_ledger: BTreeSet::new(),
            open_journal: BTreeSet::new(),
            open_entries: BTreeMap::new(),
            verifies: BTreeMap::new(),
        };
        // The findings of the pass in progress per root, with its `at_ms`.
        let mut pending: BTreeMap<String, (i64, Vec<Subject>)> = BTreeMap::new();
        for (record, &end) in ledger.records.iter().zip(&ledger.ends) {
            match &record.kind {
                Kind::Manifest { text } => {
                    let path = manifest::parse(text)?.relative_path;
                    state.manifest_end.entry(path).or_insert(end);
                }
                Kind::Copy { path, root } => {
                    let copy = state.data_copy(path, root);
                    copy.valid_end.get_or_insert(end);
                    copy.latest = Some((end, record.at_ms));
                    copy.open = None;
                }
                Kind::LedgerCopy {
                    root, prefix_bytes, ..
                } => {
                    state
                        .ledger_copies
                        .insert(root.clone(), Some(*prefix_bytes));
                    state.open_ledger.remove(root);
                }
                Kind::Finding {
                    root,
                    code,
                    subject,
                } => {
                    let slot = pending
                        .entry(root.clone())
                        .or_insert_with(|| (record.at_ms, Vec::new()));
                    if slot.0 != record.at_ms {
                        *slot = (record.at_ms, Vec::new());
                    }
                    slot.1.push(subject.clone());
                    match subject {
                        Subject::File(path) => {
                            let copy = state.data_copy(path, root);
                            copy.valid_end = None;
                            copy.open = Some(*code);
                        }
                        Subject::CopyLedger => {
                            if let Some(prefix) = state.ledger_copies.get_mut(root) {
                                *prefix = None;
                            }
                            state.open_ledger.insert(root.clone());
                        }
                        Subject::Journal => {
                            state.open_journal.insert(root.clone());
                        }
                        Subject::Entry(entry) => {
                            state
                                .open_entries
                                .entry(root.clone())
                                .or_default()
                                .insert(entry.clone());
                        }
                    }
                }
                Kind::Verify {
                    root, prefix_bytes, ..
                } => {
                    let subjects = match pending.remove(root) {
                        Some((at_ms, subjects)) if at_ms == record.at_ms => subjects,
                        _ => Vec::new(),
                    };
                    if !subjects.contains(&Subject::Journal) {
                        state.open_journal.remove(root);
                    }
                    if let Some(open) = state.open_entries.get_mut(root) {
                        open.retain(|entry| subjects.contains(&Subject::Entry(entry.clone())));
                    }
                    let files = subjects
                        .into_iter()
                        .filter_map(|subject| match subject {
                            Subject::File(path) => Some(path),
                            _ => None,
                        })
                        .collect();
                    state
                        .verifies
                        .entry(root.clone())
                        .or_default()
                        .push(VerifyPass {
                            at_ms: record.at_ms,
                            prefix_bytes: *prefix_bytes,
                            files,
                        });
                }
                _ => {}
            }
        }
        Ok(state)
    }

    fn data_copy(&mut self, path: &str, root: &str) -> &mut DataCopy {
        self.copies
            .entry(path.to_owned())
            .or_default()
            .entry(root.to_owned())
            .or_insert(DataCopy {
                valid_end: None,
                latest: None,
                open: None,
            })
    }

    /// The path verdicts, as the union `select` folds them.
    pub(crate) fn view(&self) -> &LedgerView {
        &self.view
    }

    /// Every recorded path, in path order, with its verdict.
    pub fn paths(&self) -> impl Iterator<Item = (&str, PathVerdict)> {
        self.view
            .all()
            .map(|(path, verdict)| (path, PathVerdict::from(verdict)))
    }

    /// The verdict of `path`, `None` when the ledger does not record it.
    pub fn verdict(&self, path: &str) -> Option<PathVerdict> {
        self.view.verdict_of(path).map(PathVerdict::from)
    }

    /// The ledger's first manifest text of `path`, whatever its verdict.
    pub fn ledger_text(&self, path: &str) -> Option<&str> {
        self.view.text(path)
    }

    /// The end of the first `manifest` record of `path`.
    pub fn manifest_end(&self, path: &str) -> Option<u64> {
        self.manifest_end.get(path).copied()
    }

    /// Whether the partition directory `partition` is recorded removed.
    pub fn is_removed(&self, partition: &str) -> bool {
        self.removed.contains(partition)
    }

    /// The copy (path, root), if any record names it.
    pub fn copy(&self, path: &str, root: &str) -> Option<&DataCopy> {
        self.copies.get(path).and_then(|roots| roots.get(root))
    }

    /// Every root a record names a copy of `path` on, with its state.
    pub fn copies_of(&self, path: &str) -> impl Iterator<Item = (&str, &DataCopy)> {
        self.copies
            .get(path)
            .into_iter()
            .flatten()
            .map(|(root, copy)| (root.as_str(), copy))
    }

    /// Every path with a record about a copy of it, in path order.
    pub fn copied_paths(&self) -> impl Iterator<Item = &str> {
        self.copies.keys().map(String::as_str)
    }

    /// Whether any `copy` record names a copy on `root`.
    pub fn has_copies_on(&self, root: &str) -> bool {
        self.copies
            .values()
            .any(|roots| roots.get(root).is_some_and(|copy| copy.latest.is_some()))
    }

    /// The prefix length of root `root`'s latest `ledger-copy` record,
    /// unless a copy-ledger finding voids it (L1).
    pub fn ledger_copy(&self, root: &str) -> Option<u64> {
        self.ledger_copies.get(root).copied().flatten()
    }

    /// Whether any `ledger-copy` record names `root`.
    pub fn has_ledger_copy_record(&self, root: &str) -> bool {
        self.ledger_copies.contains_key(root)
    }

    /// Whether `root` has an open copy-ledger finding.
    pub fn open_ledger_finding(&self, root: &str) -> bool {
        self.open_ledger.contains(root)
    }

    /// Whether `root` has an open journal finding.
    pub fn open_journal_finding(&self, root: &str) -> bool {
        self.open_journal.contains(root)
    }

    /// The entries `<segment>/<name>.import` with an open finding on
    /// `root`, sorted.
    pub fn open_entry_findings(&self, root: &str) -> Vec<String> {
        self.open_entries
            .get(root)
            .map(|open| open.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Every verification pass over `root`, in ledger order.
    pub fn verifies(&self, root: &str) -> &[VerifyPass] {
        self.verifies.get(root).map_or(&[], Vec::as_slice)
    }

    /// The last good verification of the copy (`path`, `root`), as
    /// `at_ms`: the later of its latest `copy` record and the latest
    /// `verify root` pass whose prefix covers that record and that recorded
    /// no finding about `path`. `None` without a `copy` record.
    pub fn last_good_verification(&self, path: &str, root: &str) -> Option<i64> {
        let (end, copied_at) = self.copy(path, root)?.latest?;
        let verified = self
            .verifies(root)
            .iter()
            .rev()
            .find(|pass| pass.prefix_bytes >= end && !pass.files.contains(path))
            .map(|pass| pass.at_ms);
        Some(verified.map_or(copied_at, |at| at.max(copied_at)))
    }
}
