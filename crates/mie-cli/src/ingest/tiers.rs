//! The tier side of `mie ingest` on a tiered store (ADR-062 D6, D11).
//!
//! - **The replicator thread** `mie-replicate` runs one replication pass
//!   ([`replicate::run_planned`]) when the capture starts and after each
//!   seal pass, woken by a trigger
//!   channel with one pending slot: a pass that runs longer than a seal
//!   interval leaves one wake-up behind, never a queue. A hung or unmounted
//!   cold root therefore blocks this thread only, never the capture. Each
//!   pass ends with a `replication` journal line, and every truncation of
//!   the copy ledger's trailing record becomes a `copy_ledger_truncated`
//!   line. When every configured copy target presented its marker, the pass
//!   also leaves the hot removal's plan in a shared slot.
//! - **The remover** runs in the seal path, on the capture thread, through
//!   `TieredSink`: after each seal pass it takes the latest plan and
//!   removes the live source's partitions through the writer that holds the
//!   source lock ([`RawWriter::remove_partitions`]); only when the plan
//!   names an archive partition or leftover does it also try the archive
//!   source's lock ([`removal::remove_with_lock`]). It touches only hot, and
//!   it takes the copy-ledger lock without waiting, so it never stalls the
//!   capture. Every outcome goes to the journal (`removal`), never to the
//!   capture as an error.
//! - **The recall** of the live writer journals a `recall` line per
//!   partition (`RecallTally`).

use crate::config::IngestConfig;
use crate::journal::{
    Journal, copy_ledger_truncated_json, recall_json, removal_json, replication_json,
};
use crate::store::DeviceProbe;
use crate::store::replicate::{self, PassObserver};
use mie_adapter_binance::archive::ARCHIVE_SOURCE;
use mie_adapter_binance::transport::Clock;
use mie_adapter_parquet::copy_ledger::{self, CopyLedger, Truncation};
use mie_adapter_parquet::removal::{self, RemovalPlan, RemovalReport};
use mie_adapter_parquet::{RawWriter, RecallEvent, RecallObserver};
use mie_ports::raw::{RawRecord, RawRecordSink, RawStoreError, RawStreamKey, SealedFile};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

/// The plan the replicator leaves for the remover.
pub(crate) type PlanSlot = Arc<Mutex<Option<RemovalPlan>>>;

/// The capture's sink on a tiered store: the live writer, then the remover
/// and the replicator's trigger after each seal pass (module docs).
pub(crate) struct TieredSink {
    pub(crate) writer: RawWriter,
    pub(crate) plan: PlanSlot,
    pub(crate) trigger: SyncSender<()>,
    pub(crate) journal: Arc<Mutex<Journal>>,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) hot: PathBuf,
}

impl TieredSink {
    /// Seals everything and returns the trigger, so the caller can wake the
    /// final pass after `run_end`.
    pub(crate) fn close(self) -> (Result<Vec<SealedFile>, RawStoreError>, SyncSender<()>) {
        (self.writer.close(), self.trigger)
    }
}

impl RawRecordSink for TieredSink {
    fn append(&mut self, stream: &RawStreamKey, record: RawRecord) -> Result<(), RawStoreError> {
        self.writer.append(stream, record)
    }

    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError> {
        let sealed = self.writer.seal_all()?;
        // The remover first: the pass the previous trigger started has
        // usually finished its appends by now, so the ledger lock is free.
        let plan = self.plan.lock().unwrap_or_else(|p| p.into_inner()).take();
        if let Some(plan) = plan {
            let now_ms = self.clock.now_utc_ns().div_euclid(1_000_000);
            let first = super::first_hot_date(now_ms);
            for report in remove_pass(&mut self.writer, &plan, &self.hot, &first, now_ms) {
                journal_removal(&self.journal, &*self.clock, &self.hot, &report);
            }
        }
        // One pending trigger is enough.
        match self.trigger.try_send(()) {
            Ok(()) | Err(TrySendError::Full(())) | Err(TrySendError::Disconnected(())) => {}
        }
        Ok(sealed)
    }
}

/// The seal-path remover over `plan` (module docs): the live source through
/// `writer`, then the archive source under its own lock when the plan names
/// it. Returns one report per source it ran for.
pub fn remove_pass(
    writer: &mut RawWriter,
    plan: &RemovalPlan,
    hot: &Path,
    first_hot_date: &str,
    now_ms: i64,
) -> Vec<RemovalReport> {
    let mut reports = vec![writer.remove_partitions(plan, first_hot_date, now_ms)];
    if plan.names_source(ARCHIVE_SOURCE) {
        reports.push(removal::remove_with_lock(
            hot,
            ARCHIVE_SOURCE,
            plan,
            first_hot_date,
            now_ms,
        ));
    }
    reports
}

/// Journals a removal report, when something happened, and its truncations.
fn journal_removal(
    journal: &Mutex<Journal>,
    clock: &dyn Clock,
    hot: &Path,
    report: &RemovalReport,
) {
    let mut journal = journal.lock().unwrap_or_else(|p| p.into_inner());
    let ledger = copy_ledger::ledger_path(hot);
    for truncation in &report.truncations {
        journal.write(
            clock.now_utc_ns(),
            "copy_ledger_truncated",
            copy_ledger_truncated_json(&ledger, *truncation),
        );
    }
    if !report.is_empty() {
        journal.write(clock.now_utc_ns(), "removal", removal_json(report));
    }
}

/// Journals every copy-ledger truncation of a pass.
struct TruncationJournal {
    journal: Arc<Mutex<Journal>>,
    clock: Arc<dyn Clock>,
}

impl PassObserver for TruncationJournal {
    fn ledger_truncated(&mut self, ledger: &Path, truncation: Truncation) {
        self.journal
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .write(
                self.clock.now_utc_ns(),
                "copy_ledger_truncated",
                copy_ledger_truncated_json(ledger, truncation),
            );
    }
}

/// Starts the `mie-replicate` thread (module docs). It runs one pass per
/// trigger and ends when the trigger's sender is dropped. The archive
/// configuration is `[history] archive_config`; without it, `mie ingest`
/// does not replicate archive parts, as the standalone pass does not.
///
/// # Errors
///
/// A description when the thread cannot be spawned.
pub(crate) fn spawn_replicator(
    config: &IngestConfig,
    probe: Arc<dyn DeviceProbe>,
    clock: Arc<dyn Clock>,
    journal: Arc<Mutex<Journal>>,
    plan: PlanSlot,
    trigger: Receiver<()>,
) -> Result<JoinHandle<()>, String> {
    let config = config.clone();
    thread::Builder::new()
        .name("mie-replicate".to_owned())
        .spawn(move || {
            let archive = replicate::archive_config(&config, None);
            let ledger = CopyLedger::at(&config.paths.raw_root);
            let mut observer = TruncationJournal {
                journal: Arc::clone(&journal),
                clock: Arc::clone(&clock),
            };
            let now_ms = || clock.now_utc_ns().div_euclid(1_000_000);
            while trigger.recv().is_ok() {
                let (report, planned) = match &archive {
                    Ok(archive) => replicate::run_planned(
                        &config,
                        archive.clone(),
                        probe.as_ref(),
                        &now_ms,
                        &ledger,
                        &mut observer,
                        &super::first_hot_date(now_ms()),
                    ),
                    Err(error) => (
                        replicate::PassReport {
                            problems: vec![error.clone()],
                            ..replicate::PassReport::default()
                        },
                        None,
                    ),
                };
                journal.lock().unwrap_or_else(|p| p.into_inner()).write(
                    clock.now_utc_ns(),
                    "replication",
                    replication_json(&report, planned.as_ref()),
                );
                *plan.lock().unwrap_or_else(|p| p.into_inner()) = planned;
            }
        })
        .map_err(|e| format!("spawn the replicator thread: {e}"))
}

/// One recalled partition, as the recall line reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct Recalled {
    /// The partition directory.
    pub(crate) partition: String,
    /// The files copied back.
    pub(crate) files: usize,
    /// Per cold root (its path), the files copied from it.
    pub(crate) from: BTreeMap<String, usize>,
    /// The damaged copies passed over: path, root, error, and the root the
    /// file was copied from instead.
    pub(crate) damaged: Vec<(String, String, String, String)>,
}

/// Folds the recall events of a writer into one [`Recalled`] per
/// partition, handed out at its `returned` event.
#[derive(Debug, Default)]
pub(crate) struct RecallTally {
    open: BTreeMap<String, Recalled>,
    /// Damaged copies whose replacement is not copied yet: (partition,
    /// path) → (root, error).
    pending: BTreeMap<(String, String), Vec<(String, String)>>,
}

impl RecallTally {
    /// Takes one event; returns the partition's summary at `Returned`.
    /// [`RecallEvent::LedgerTruncated`] is the caller's.
    pub(crate) fn observe(&mut self, event: &RecallEvent) -> Option<Recalled> {
        match event {
            RecallEvent::Damaged {
                partition,
                path,
                root,
                error,
            } => {
                self.pending
                    .entry((partition.clone(), path.clone()))
                    .or_default()
                    .push((root.display().to_string(), error.clone()));
                None
            }
            RecallEvent::Copied {
                partition,
                path,
                root,
            } => {
                let root = root.display().to_string();
                let entry = self
                    .open
                    .entry(partition.clone())
                    .or_insert_with(|| Recalled {
                        partition: partition.clone(),
                        ..Recalled::default()
                    });
                *entry.from.entry(root.clone()).or_default() += 1;
                for (damaged_root, error) in self
                    .pending
                    .remove(&(partition.clone(), path.clone()))
                    .unwrap_or_default()
                {
                    entry
                        .damaged
                        .push((path.clone(), damaged_root, error, root.clone()));
                }
                None
            }
            RecallEvent::Returned { partition, files } => {
                let mut done = self.open.remove(partition).unwrap_or_else(|| Recalled {
                    partition: partition.clone(),
                    ..Recalled::default()
                });
                done.files = *files;
                Some(done)
            }
            RecallEvent::LedgerTruncated(_) => None,
        }
    }
}

/// The live writer's recall observer: a `recall` line per partition and a
/// `copy_ledger_truncated` line per truncation.
pub(crate) fn recall_journal(
    journal: Arc<Mutex<Journal>>,
    clock: Arc<dyn Clock>,
    hot: &Path,
) -> RecallObserver {
    let tally = Mutex::new(RecallTally::default());
    let ledger = copy_ledger::ledger_path(hot);
    Arc::new(move |event: &RecallEvent| {
        let line = match event {
            RecallEvent::LedgerTruncated(truncation) => Some((
                "copy_ledger_truncated",
                copy_ledger_truncated_json(&ledger, *truncation),
            )),
            _ => tally
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .observe(event)
                .map(|recalled| ("recall", recall_json(&recalled))),
        };
        if let Some((kind, fields)) = line {
            journal.lock().unwrap_or_else(|p| p.into_inner()).write(
                clock.now_utc_ns(),
                kind,
                fields,
            );
        }
    })
}
