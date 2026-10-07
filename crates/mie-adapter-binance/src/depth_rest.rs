//! The order-book snapshot fetcher (ADR-038).
//!
//! Fetches `GET /fapi/v1/depth?symbol=<symbol>&limit=<limit>` in two cases:
//!
//! - **Sync**: the capture thread asked for a snapshot because the book is
//!   unsynced ([`Pipeline::book_snapshot_wanted`]). Requests arrive through a
//!   capacity-1 channel, polled between 100 ms sleep slices. A request stays
//!   pending across failed fetches until one succeeds.
//! - **Checkpoint**: `checkpoint_interval` has passed since the last
//!   successful fetch. The capture audits the book against it and may
//!   re-anchor on it.
//!
//! Two requests are never sent within `min_spacing`, and a 418 or 429 (rate
//! limit) pushes the next request back by an exponential backoff, as for
//! open interest. A 2xx body is forwarded verbatim as a frame of the
//! `depthSnapshot` stream, in session `<run_id>/depthSnapshot/<ordinal>`;
//! the ordinal grows after every failed fetch. Non-2xx responses and
//! transport failures are journaled through
//! [`CaptureEvent::DepthSnapshotFetch`] and never persisted.
//!
//! The trigger decides only *when* a snapshot is fetched. What the pipeline
//! does with it depends on the persisted records alone (ADR-038).
//!
//! [`Pipeline::book_snapshot_wanted`]: crate::Pipeline::book_snapshot_wanted

use crate::live::{CaptureEvent, CountedSender, Inbound, SnapshotTrigger};
use crate::stream::BinanceStream;
use crate::transport::{Clock, HttpGet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time::Duration;

/// Longest single sleep: the request channel is polled at this cadence.
const SLEEP_SLICE_NS: u64 = 100_000_000;

/// Everything the fetcher needs.
pub(crate) struct DepthSnapshotTask {
    pub url: String,
    pub run_id: String,
    pub checkpoint_interval: Duration,
    pub min_spacing: Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub http: Arc<dyn HttpGet>,
    pub clock: Arc<dyn Clock>,
    pub tx: CountedSender<Inbound>,
    /// Sync requests (want ids) from the capture thread.
    pub requests: Receiver<u64>,
    pub shutdown: Arc<AtomicBool>,
}

fn nanos(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

impl DepthSnapshotTask {
    /// Runs until shutdown or until the capture thread is gone.
    pub(crate) fn run(self) {
        let interval = nanos(self.checkpoint_interval);
        let spacing = nanos(self.min_spacing);
        let mut ordinal: u64 = 1;
        let mut backoff = self.backoff_initial;
        let mut sync_pending = false;
        let mut last_success = self.clock.monotonic_ns();
        let mut last_request: Option<u64> = None;
        let mut not_before: u64 = 0;
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return;
            }
            // A gone capture thread surfaces at the next send.
            while self.requests.try_recv().is_ok() {
                sync_pending = true;
            }
            let now = self.clock.monotonic_ns();
            let due = if sync_pending {
                now
            } else {
                last_success.saturating_add(interval)
            };
            let allowed = last_request
                .map_or(0, |last| last.saturating_add(spacing))
                .max(not_before);
            let at = due.max(allowed);
            if now < at {
                let slice = (at - now).min(SLEEP_SLICE_NS);
                self.clock.sleep(Duration::from_nanos(slice));
                continue;
            }

            let trigger = if sync_pending {
                SnapshotTrigger::Sync
            } else {
                SnapshotTrigger::Checkpoint
            };
            last_request = Some(self.clock.monotonic_ns());
            let request_ns = self.clock.now_utc_ns();
            let result = self.http.get(&self.url);
            let response_ns = self.clock.now_utc_ns();
            let (status, error, persisted) = match result {
                Ok((status, body)) if (200..300).contains(&status) => {
                    let frame = Inbound::Frame {
                        stream: BinanceStream::DepthSnapshot,
                        session_id: format!(
                            "{}/{}/{ordinal}",
                            self.run_id,
                            BinanceStream::DepthSnapshot.raw_name()
                        ),
                        receive_time_ns: response_ns,
                        bytes: body,
                    };
                    if self.tx.send(frame).is_err() {
                        return;
                    }
                    sync_pending = false;
                    last_success = self.clock.monotonic_ns();
                    backoff = self.backoff_initial;
                    (Some(status), None, true)
                }
                Ok((status, body)) => {
                    ordinal += 1;
                    if status == 418 || status == 429 {
                        not_before = self.clock.monotonic_ns().saturating_add(nanos(backoff));
                        backoff = (backoff * 2).min(self.backoff_max);
                    }
                    let detail = String::from_utf8_lossy(&body).chars().take(200).collect();
                    (Some(status), Some(detail), false)
                }
                Err(error) => {
                    ordinal += 1;
                    (None, Some(error), false)
                }
            };
            let fetch = CaptureEvent::DepthSnapshotFetch {
                trigger,
                request_ns,
                response_ns,
                status,
                error,
                persisted,
            };
            if self.tx.send(Inbound::Event(fetch)).is_err() {
                return;
            }
        }
    }
}
