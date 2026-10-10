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
//! the missed samples into a `Disconnected` gap on the next success.
//!
//! A 418 or 429 (rate limit) pauses polling, measured from the response
//! (ADR-046). A `Retry-After` in delay-seconds sets the pause, capped at
//! [`RETRY_AFTER_MAX_S`]; without a usable one the pause is
//! [`PAUSE_AFTER_429_MS`] or [`PAUSE_AFTER_418_MS`]. The next poll is the
//! first 10 s slot at or after the pause's end, so polls stay on the grid.

use crate::live::{CaptureEvent, CountedSender, Inbound};
use crate::stream::{BinanceStream, OI_POLL_INTERVAL_MS};
use crate::transport::{Clock, HttpGet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

const SLEEP_SLICE_NS: i64 = 100_000_000;
const NS_PER_MS: i64 = 1_000_000;

/// The longest `Retry-After` honoured, in seconds (ADR-046 D3): 3 days,
/// Binance's longest documented IP ban. On a 418 the header counts down to
/// the ban's end, so a shorter cap would poll into an active ban; a longer
/// one would let a corrupt value keep open interest dark past any
/// documented ban.
const RETRY_AFTER_MAX_S: u64 = 259_200;

/// The pause after a 429 without a usable `Retry-After` (ADR-046 D4): one
/// window of the 1-minute `REQUEST_WEIGHT` limit, so no second poll lands
/// in the window that answered 429.
const PAUSE_AFTER_429_MS: i64 = 60_000;

/// The pause after a 418 without a usable `Retry-After` (ADR-046 D4):
/// Binance's shortest documented IP ban, 2 minutes. A shorter pause polls
/// inside every possible ban.
const PAUSE_AFTER_418_MS: i64 = 120_000;

/// A `Retry-After` value in delay-seconds (RFC 9110 §10.2.3,
/// `1*DIGIT`) as milliseconds, capped at [`RETRY_AFTER_MAX_S`]; `None`
/// for any other form, such as an HTTP-date, a sign or a decimal
/// (ADR-046 D1, D3).
fn retry_after_ms(value: &str) -> Option<i64> {
    let digits = value.trim_ascii();
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    // Only digits remain, so a parse failure is an overflow: saturate.
    let seconds = digits
        .parse::<u64>()
        .map_or(RETRY_AFTER_MAX_S, |s| s.min(RETRY_AFTER_MAX_S));
    i64::try_from(seconds * 1_000).ok()
}

/// How long polling pauses after `status`, measured from the response:
/// `None` unless it is a rate limit (418 or 429). A usable `Retry-After`
/// replaces the fallback, also when it is shorter (ADR-046 D2, D4).
fn rate_limit_pause_ms(status: u16, retry_after: Option<&str>) -> Option<i64> {
    let fallback = match status {
        418 => PAUSE_AFTER_418_MS,
        429 => PAUSE_AFTER_429_MS,
        _ => return None,
    };
    Some(retry_after.and_then(retry_after_ms).unwrap_or(fallback))
}

/// Everything the poller needs.
pub(crate) struct OiTask {
    pub url: String,
    pub run_id: String,
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
            let result = self.http.get_reply(&self.url);
            let response_time_ns = self.clock.now_utc_ns();
            let (status, error, persisted) = match result {
                Ok(reply) if (200..300).contains(&reply.status) => {
                    let frame = Inbound::Frame {
                        stream: BinanceStream::OpenInterest,
                        session_id: format!(
                            "{}/{}/{ordinal}",
                            self.run_id,
                            BinanceStream::OpenInterest.raw_name()
                        ),
                        receive_time_ns: response_time_ns,
                        bytes: reply.body,
                    };
                    if self.tx.send(frame).is_err() {
                        return;
                    }
                    (Some(reply.status), None, true)
                }
                Ok(reply) => {
                    ordinal += 1;
                    if let Some(pause_ms) =
                        rate_limit_pause_ms(reply.status, reply.retry_after.as_deref())
                    {
                        not_before_ms = not_before_ms.max(
                            response_time_ns
                                .div_euclid(NS_PER_MS)
                                .saturating_add(pause_ms),
                        );
                    }
                    let detail = String::from_utf8_lossy(&reply.body)
                        .chars()
                        .take(200)
                        .collect();
                    (Some(reply.status), Some(detail), false)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_after_reads_delay_seconds_capped_at_three_days() {
        assert_eq!(retry_after_ms("30"), Some(30_000));
        assert_eq!(retry_after_ms(" 30 "), Some(30_000));
        assert_eq!(retry_after_ms("0"), Some(0));
        assert_eq!(retry_after_ms("259200"), Some(259_200_000));
        assert_eq!(retry_after_ms("259201"), Some(259_200_000));
        assert_eq!(retry_after_ms("99999999999999999999999"), Some(259_200_000));
    }

    #[test]
    fn retry_after_rejects_every_other_form() {
        for value in [
            "",
            "abc",
            "1.5",
            "-5",
            "+5",
            "Wed, 21 Oct 2026 07:28:00 GMT",
        ] {
            assert_eq!(retry_after_ms(value), None, "{value:?}");
        }
    }

    #[test]
    fn rate_limit_pause_prefers_a_usable_retry_after() {
        assert_eq!(rate_limit_pause_ms(429, None), Some(60_000));
        assert_eq!(rate_limit_pause_ms(418, None), Some(120_000));
        assert_eq!(rate_limit_pause_ms(429, Some("30")), Some(30_000));
        assert_eq!(rate_limit_pause_ms(418, Some("0")), Some(0));
        assert_eq!(rate_limit_pause_ms(429, Some("abc")), Some(60_000));
        assert_eq!(rate_limit_pause_ms(418, Some("1.5")), Some(120_000));
        assert_eq!(rate_limit_pause_ms(503, Some("30")), None);
        assert_eq!(rate_limit_pause_ms(200, None), None);
    }
}
