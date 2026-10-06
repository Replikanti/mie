//! The connection loop of one WebSocket stream.
//!
//! One thread per stream: connect, forward every data frame with its
//! receive time and session, keep the connection alive, and reconnect.
//!
//! - **Session**: every successful connection gets the session id
//!   `<run_id>/<raw stream name>/<connection ordinal>`, ordinals from 1. The
//!   pipeline turns each session change into a `Disconnected` gap.
//! - **Liveness**: the client pings every `ping_interval` (the server
//!   answers with a pong; it also pings on its own and tungstenite answers).
//!   A connection that delivers no frame of any kind for `liveness_timeout`
//!   is dropped and reconnected.
//! - **Planned rotation**: a connection is closed and immediately replaced
//!   once it is `max_age` old. The caller staggers `max_age` per stream, so
//!   streams never rotate together and every rotation happens before the
//!   exchange's 24 h connection limit.
//! - **Backoff**: after a failed connect or an unplanned disconnect the loop
//!   waits `backoff_initial`, doubling up to `backoff_max`, without jitter.
//!   It resets once a connection stayed up for [`HEALTHY_AFTER`].
//!
//! Lifecycle events travel through the same channel as the frames, so the
//! capture thread sees them in order with the data.

use crate::live::{CaptureEvent, CountedSender, Inbound};
use crate::stream::BinanceStream;
use crate::transport::{Clock, ReadOutcome, WsConnector};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// How long a connection must stay up for the backoff to reset.
pub const HEALTHY_AFTER: Duration = Duration::from_secs(60);

/// Longest single sleep, so a sleeping thread notices shutdown quickly.
const SLEEP_SLICE: Duration = Duration::from_millis(100);

/// Everything one stream's connection loop needs.
pub(crate) struct WsTask {
    pub stream: BinanceStream,
    pub url: String,
    pub run_id: String,
    pub ping_interval: Duration,
    pub liveness_timeout: Duration,
    pub max_age: Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub connector: Arc<dyn WsConnector>,
    pub clock: Arc<dyn Clock>,
    pub tx: CountedSender<Inbound>,
    pub shutdown: Arc<AtomicBool>,
}

/// Why a connection ended.
enum End {
    Shutdown,
    Rotation,
    Lost(String),
}

impl WsTask {
    /// Runs until shutdown or until the capture thread is gone.
    pub(crate) fn run(self) {
        let mut ordinal: u64 = 0;
        let mut backoff = self.backoff_initial;
        while !self.shutdown.load(Ordering::Relaxed) {
            let mut connection = match self.connector.connect(&self.url) {
                Ok(connection) => connection,
                Err(error) => {
                    let failed = CaptureEvent::ConnectFailed {
                        stream: self.stream,
                        error,
                    };
                    if !self.event(failed) || !self.back_off(&mut backoff) {
                        return;
                    }
                    continue;
                }
            };
            ordinal += 1;
            let session_id = format!("{}/{}/{ordinal}", self.run_id, self.stream.raw_name());
            let connected = CaptureEvent::Connected {
                stream: self.stream,
                session_id: session_id.clone(),
            };
            if !self.event(connected) {
                connection.close();
                return;
            }
            let connected_at = self.clock.monotonic_ns();
            let mut last_frame = connected_at;
            let mut last_ping = connected_at;
            let end = loop {
                if self.shutdown.load(Ordering::Relaxed) {
                    break End::Shutdown;
                }
                let now = self.clock.monotonic_ns();
                if elapsed(connected_at, now) >= self.max_age {
                    break End::Rotation;
                }
                if elapsed(last_frame, now) >= self.liveness_timeout {
                    break End::Lost(format!(
                        "no frame for {} ms",
                        self.liveness_timeout.as_millis()
                    ));
                }
                if elapsed(last_ping, now) >= self.ping_interval {
                    if let Err(error) = connection.ping() {
                        break End::Lost(error);
                    }
                    last_ping = now;
                }
                match connection.read() {
                    ReadOutcome::Frame(bytes) => {
                        let frame = Inbound::Frame {
                            stream: self.stream,
                            session_id: session_id.clone(),
                            receive_time_ns: self.clock.now_utc_ns(),
                            bytes,
                        };
                        last_frame = self.clock.monotonic_ns();
                        if self.tx.send(frame).is_err() {
                            break End::Shutdown;
                        }
                    }
                    ReadOutcome::Control => last_frame = self.clock.monotonic_ns(),
                    ReadOutcome::Timeout => {}
                    ReadOutcome::Closed(reason) | ReadOutcome::Error(reason) => {
                        break End::Lost(reason);
                    }
                }
            };
            connection.close();
            if elapsed(connected_at, self.clock.monotonic_ns()) >= HEALTHY_AFTER {
                backoff = self.backoff_initial;
            }
            match end {
                End::Shutdown => return,
                End::Rotation => {
                    let rotated = CaptureEvent::PlannedRotation {
                        stream: self.stream,
                        session_id,
                    };
                    if !self.event(rotated) {
                        return;
                    }
                }
                End::Lost(reason) => {
                    let lost = CaptureEvent::Disconnected {
                        stream: self.stream,
                        session_id,
                        reason,
                    };
                    if !self.event(lost) || !self.back_off(&mut backoff) {
                        return;
                    }
                }
            }
        }
    }

    /// Sends a lifecycle event; `false` when the capture thread is gone.
    fn event(&self, event: CaptureEvent) -> bool {
        self.tx.send(Inbound::Event(event)).is_ok()
    }

    /// Announces and sleeps the current backoff, then doubles it. `false`
    /// on shutdown or when the capture thread is gone.
    fn back_off(&self, backoff: &mut Duration) -> bool {
        let delay = *backoff;
        *backoff = (*backoff * 2).min(self.backoff_max);
        let announced = self.event(CaptureEvent::Backoff {
            stream: self.stream,
            delay_ms: u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
        });
        announced && sleep_unless_shutdown(self.clock.as_ref(), delay, &self.shutdown)
    }
}

fn elapsed(from_ns: u64, to_ns: u64) -> Duration {
    Duration::from_nanos(to_ns.saturating_sub(from_ns))
}

/// Sleeps `duration` of monotonic time in short slices; `false` if shutdown
/// was requested meanwhile.
pub(crate) fn sleep_unless_shutdown(
    clock: &dyn Clock,
    duration: Duration,
    shutdown: &AtomicBool,
) -> bool {
    let start = clock.monotonic_ns();
    loop {
        if shutdown.load(Ordering::Relaxed) {
            return false;
        }
        let done = elapsed(start, clock.monotonic_ns());
        if done >= duration {
            return true;
        }
        clock.sleep((duration - done).min(SLEEP_SLICE));
    }
}
