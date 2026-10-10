//! The open-interest REST poller.
//!
//! Polls `GET /fapi/v1/openInterest` at wall-clock multiples of
//! [`OI_POLL_INTERVAL_MS`], so the cadence is fixed and the samples are
//! comparable across runs. A 2xx body is forwarded verbatim as a frame of
//! the `openInterest` stream; its exchange `time` field orders it.
//!
//! Every poll is reported as a [`CaptureEvent::OiPoll`] with the request and
//! response times and the status, for the capture journal; the raw envelope
//! (ADR-030 schema v1) stays unchanged. A failed poll (non-2xx or transport
//! error) is never persisted and starts a new session, so the pipeline turns
//! the missed samples into a `Disconnected` gap on the next success. A 418 or
//! 429 (rate limit) pushes the next poll back by an exponential backoff.

use crate::live::{CaptureEvent, CountedSender, Inbound};
use crate::stream::{BinanceStream, OI_POLL_INTERVAL_MS};
use crate::transport::{Clock, HttpGet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const SLEEP_SLICE_NS: i64 = 100_000_000;
const NS_PER_MS: i64 = 1_000_000;

/// Everything the poller needs.
pub(crate) struct OiTask {
    pub url: String,
    pub run_id: String,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub http: Arc<dyn HttpGet>,
    pub clock: Arc<dyn Clock>,
    pub tx: CountedSender<Inbound>,
    pub shutdown: Arc<AtomicBool>,
}

impl OiTask {
    /// Runs until shutdown or until the capture thread is gone.
    pub(crate) fn run(self) {
        let interval = i64::from(OI_POLL_INTERVAL_MS);
        let mut ordinal: u64 = 1;
        let mut backoff = self.backoff_initial;
        let mut not_before_ms = 0_i64;
        loop {
            let now_ms = self.clock.now_utc_ns().div_euclid(NS_PER_MS);
            let earliest = now_ms.max(not_before_ms);
            let slot_ms = earliest.div_euclid(interval) * interval
                + if earliest.rem_euclid(interval) == 0 {
                    0
                } else {
                    interval
                };
            if !self.sleep_until(slot_ms * NS_PER_MS) {
                return;
            }
            not_before_ms = slot_ms + 1;

            let request_time_ns = self.clock.now_utc_ns();
            let result = self
                .http
                .get_reply(&self.url)
                .map(|reply| (reply.status, reply.body));
            let response_time_ns = self.clock.now_utc_ns();
            let (status, error, persisted) = match result {
                Ok((status, body)) if (200..300).contains(&status) => {
                    let frame = Inbound::Frame {
                        stream: BinanceStream::OpenInterest,
                        session_id: format!(
                            "{}/{}/{ordinal}",
                            self.run_id,
                            BinanceStream::OpenInterest.raw_name()
                        ),
                        receive_time_ns: response_time_ns,
                        bytes: body,
                    };
                    if self.tx.send(frame).is_err() {
                        return;
                    }
                    backoff = self.backoff_initial;
                    (Some(status), None, true)
                }
                Ok((status, body)) => {
                    ordinal += 1;
                    if status == 418 || status == 429 {
                        let delay_ms = i64::try_from(backoff.as_millis()).unwrap_or(i64::MAX);
                        not_before_ms = not_before_ms.max(
                            response_time_ns
                                .div_euclid(NS_PER_MS)
                                .saturating_add(delay_ms),
                        );
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
            let poll = CaptureEvent::OiPoll {
                request_time_ns,
                response_time_ns,
                status,
                error,
                persisted,
            };
            if self.tx.send(Inbound::Event(poll)).is_err() {
                return;
            }
        }
    }

    /// Sleeps until the wall clock reaches `deadline_ns`; `false` on
    /// shutdown.
    fn sleep_until(&self, deadline_ns: i64) -> bool {
        loop {
            if self.shutdown.load(Ordering::Relaxed) {
                return false;
            }
            let left = deadline_ns.saturating_sub(self.clock.now_utc_ns());
            if left <= 0 {
                return true;
            }
            let slice = left.min(SLEEP_SLICE_NS);
            self.clock
                .sleep(Duration::from_nanos(u64::try_from(slice).unwrap_or(0)));
        }
    }
}
