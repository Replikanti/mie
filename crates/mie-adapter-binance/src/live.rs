//! Live capture: raw first, then the pipeline, then the core (ADR-026,
//! ADR-030, ADR-032).
//!
//! [`start`] spawns one thread per WebSocket stream (module `ws`), one
//! open-interest poller (module `rest`) and one capture thread. The
//! producers hand frames and lifecycle events to the capture thread through
//! a bounded channel. The capture thread is the only writer, and for every
//! frame it
//!
//! 1. assigns `receive_seq`, a per-run counter over all streams in
//!    processing order;
//! 2. computes the record's `event_time` with
//!    [`record_time`], falling back to the
//!    receive time (counted as a time fallback);
//! 3. appends the record to the [`RawRecordSink`];
//! 4. pushes it through the [`Pipeline`];
//! 5. sends the released events into the bounded core channel, which
//!    [`BinanceLiveProvider`] drains.
//!
//! So an event reaches the core only after its record was appended, and a
//! frame that fails to normalize is still persisted. The sink is sealed every
//! `seal_interval` and at shutdown, which bounds the crash loss of
//! ADR-030 D5. A failed append or seal stops the capture, and the provider
//! then fails with [`ProviderError::Source`].
//!
//! Shutdown: set the shared flag. The producers stop within about a second,
//! the capture thread drains, releases the hold-back, seals, and sends an end
//! marker; the provider then returns `Ok(None)`. [`CaptureHandle::join`]
//! hands the sink back.

use crate::normalize::record_time;
use crate::pipeline::{Pipeline, PipelineStats};
use crate::rest::OiTask;
use crate::stream::BinanceStream;
use crate::transport::{Clock, HttpGet, WsConnector};
use crate::ws::WsTask;
use mie_domain::event::{FeedGap, MarketEvent};
use mie_domain::time::EventTime;
use mie_ports::outbound::{MarketDataProvider, ProviderError};
use mie_ports::raw::{Capture, RawRecord, RawRecordSink, RawStreamKey, SealedFile};
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TrySendError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// How often the capture thread wakes up without input, to keep the seal
/// and stats cadence.
const CAPTURE_TICK: Duration = Duration::from_millis(50);

/// Everything a live capture run is configured with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveConfig {
    /// Exchange symbol, for example `BTCUSDT`; also the raw instrument.
    pub symbol: String,
    /// Raw-store source, for example `binance-um`.
    pub source: String,
    /// Identifies the run in session ids, see [`run_id`].
    pub run_id: String,
    /// WebSocket base URL; the stream path is appended after a `/`.
    pub ws_base_url: String,
    /// REST base URL, without a trailing path.
    pub rest_base_url: String,
    /// The captured streams, each at most once. Their position staggers the
    /// planned reconnects.
    pub streams: Vec<BinanceStream>,
    /// Canonical merge hold-back in exchange milliseconds (ADR-028 D6).
    pub hold_back_ms: i64,
    /// Wall-clock cadence of `seal_all`.
    pub seal_interval: Duration,
    /// Age at which a connection is replaced (before the 24 h limit).
    pub max_connection_age: Duration,
    /// Added to `max_connection_age` per stream position.
    pub rotation_stagger: Duration,
    /// Client ping cadence.
    pub ping_interval: Duration,
    /// A connection silent for this long is dropped.
    pub liveness_timeout: Duration,
    /// First reconnect backoff.
    pub backoff_initial: Duration,
    /// Largest reconnect backoff.
    pub backoff_max: Duration,
    /// Capacity of the producer → capture channel.
    pub inbound_capacity: usize,
    /// Capacity of the capture → core channel.
    pub core_capacity: usize,
    /// Cadence of [`CaptureEvent::Stats`].
    pub stats_interval: Duration,
    /// Last persisted event time per stream from the previous run; the
    /// first event of each seeded stream opens with a restart gap.
    pub seeds: BTreeMap<BinanceStream, EventTime>,
}

impl LiveConfig {
    /// The defaults for `BTCUSDT` on Binance USDⓈ-M (endpoints verified
    /// 2026-10-06, ADR-032), with every stream and no seeds.
    pub fn new(run_id: &str) -> Self {
        Self {
            symbol: "BTCUSDT".to_owned(),
            source: "binance-um".to_owned(),
            run_id: run_id.to_owned(),
            ws_base_url: "wss://fstream.binance.com/market/ws".to_owned(),
            rest_base_url: "https://fapi.binance.com".to_owned(),
            streams: BinanceStream::ALL.to_vec(),
            hold_back_ms: 2_000,
            seal_interval: Duration::from_secs(300),
            max_connection_age: Duration::from_secs(82_800),
            rotation_stagger: Duration::from_secs(300),
            ping_interval: Duration::from_secs(30),
            liveness_timeout: Duration::from_secs(90),
            backoff_initial: Duration::from_millis(250),
            backoff_max: Duration::from_secs(30),
            inbound_capacity: 65_536,
            core_capacity: 65_536,
            stats_interval: Duration::from_secs(60),
            seeds: BTreeMap::new(),
        }
    }

    fn validate(&self) -> Result<Vec<RawStreamKey>, StartError> {
        let invalid = |detail: String| Err(StartError(detail));
        if self.run_id.is_empty() || self.run_id.contains('/') {
            return invalid(format!(
                "run id {:?} must be non-empty without '/'",
                self.run_id
            ));
        }
        if self.streams.is_empty() {
            return invalid("no stream configured".to_owned());
        }
        let mut seen = Vec::new();
        for stream in &self.streams {
            if seen.contains(stream) {
                return invalid(format!("stream {} configured twice", stream.raw_name()));
            }
            seen.push(*stream);
        }
        let durations = [
            ("seal_interval", self.seal_interval),
            ("max_connection_age", self.max_connection_age),
            ("ping_interval", self.ping_interval),
            ("liveness_timeout", self.liveness_timeout),
            ("backoff_initial", self.backoff_initial),
            ("backoff_max", self.backoff_max),
            ("stats_interval", self.stats_interval),
        ];
        if let Some((name, _)) = durations.iter().find(|(_, d)| d.is_zero()) {
            return invalid(format!("{name} must be positive"));
        }
        if self.inbound_capacity == 0 || self.core_capacity == 0 {
            return invalid("channel capacities must be positive".to_owned());
        }
        if self.hold_back_ms < 0 {
            return invalid("hold_back_ms must not be negative".to_owned());
        }
        self.streams
            .iter()
            .map(|s| {
                RawStreamKey::new(&self.source, &self.symbol, s.raw_name())
                    .map_err(|e| StartError(e.to_string()))
            })
            .collect()
    }
}

/// The run id for a run started at `utc_ns`: the UTC start second as
/// `YYYYMMDDTHHMMSSZ`.
pub fn run_id(utc_ns: i64) -> String {
    let secs = utc_ns.div_euclid(1_000_000_000);
    let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (year, month, day) = civil_from_days(days);
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        rem / 3_600,
        rem % 3_600 / 60,
        rem % 60
    )
}

/// Proleptic Gregorian date of a day count since 1970-01-01 (Howard
/// Hinnant's `civil_from_days`).
fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    (year, month, day)
}

/// What the capture reports to its observer, in processing order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CaptureEvent {
    /// A WebSocket connection is up.
    Connected {
        /// The stream.
        stream: BinanceStream,
        /// The new session.
        session_id: String,
    },
    /// A connect attempt failed.
    ConnectFailed {
        /// The stream.
        stream: BinanceStream,
        /// What failed.
        error: String,
    },
    /// A connection was lost (closed, failed or silent).
    Disconnected {
        /// The stream.
        stream: BinanceStream,
        /// The ended session.
        session_id: String,
        /// Why it ended.
        reason: String,
    },
    /// A connection was replaced on schedule.
    PlannedRotation {
        /// The stream.
        stream: BinanceStream,
        /// The ended session.
        session_id: String,
    },
    /// The stream waits before reconnecting.
    Backoff {
        /// The stream.
        stream: BinanceStream,
        /// The wait.
        delay_ms: u64,
    },
    /// One open-interest poll.
    OiPoll {
        /// When the request was sent, UTC ns.
        request_time_ns: i64,
        /// When the response (or failure) arrived, UTC ns.
        response_time_ns: i64,
        /// HTTP status; `None` on a transport failure.
        status: Option<u16>,
        /// Failure detail.
        error: Option<String>,
        /// Whether the body was persisted (2xx only).
        persisted: bool,
    },
    /// A gap was delivered to the core.
    Gap(FeedGap),
    /// Files were sealed.
    Sealed(Vec<SealedFile>),
    /// A persisted frame failed normalization.
    NormalizeError {
        /// The stream.
        stream: BinanceStream,
        /// The record's `receive_seq`.
        receive_seq: u64,
        /// What failed.
        error: String,
    },
    /// Periodic counters.
    Stats(CaptureStats),
}

/// Receives capture events on the capture thread.
pub trait CaptureObserver: Send {
    /// Called once per event; `at_utc_ns` is when the capture thread
    /// processed it.
    fn on_event(&mut self, at_utc_ns: i64, event: &CaptureEvent);
}

/// Throughput and backpressure of one channel (ADR-026).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ChannelStats {
    /// Configured capacity.
    pub capacity: usize,
    /// Messages sent.
    pub sent: u64,
    /// Largest observed number of queued messages.
    pub high_water: u64,
    /// Total time senders spent blocked on a full channel, in ns.
    pub blocked_ns: u64,
}

/// Counters of a capture run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureStats {
    /// Records appended to the sink.
    pub records: u64,
    /// Records whose `event_time` fell back to the receive time.
    pub time_fallbacks: u64,
    /// Files sealed so far.
    pub sealed_files: u64,
    /// Events waiting in the hold-back.
    pub buffered: u64,
    /// Pipeline counters.
    pub pipeline: PipelineStats,
    /// Producer → capture channel.
    pub inbound: ChannelStats,
    /// Capture → core channel.
    pub core: ChannelStats,
}

/// The final counters of a capture run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CaptureSummary {
    /// The counters at the end of the run.
    pub stats: CaptureStats,
    /// Whether the core stopped consuming before the end of the capture.
    pub core_gone: bool,
}

/// Why [`start`] refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StartError(pub String);

impl fmt::Display for StartError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "cannot start live capture: {}", self.0)
    }
}

impl std::error::Error for StartError {}

/// Why the capture stopped early.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CaptureError {
    /// What failed.
    pub message: String,
    /// The counters when it failed.
    pub summary: Box<CaptureSummary>,
}

impl fmt::Display for CaptureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "live capture failed: {}", self.message)
    }
}

impl std::error::Error for CaptureError {}

/// A message from a producer thread to the capture thread.
pub(crate) enum Inbound {
    Frame {
        stream: BinanceStream,
        session_id: String,
        receive_time_ns: i64,
        bytes: Vec<u8>,
    },
    Event(CaptureEvent),
}

/// A message from the capture thread to the provider.
enum CoreMsg {
    Event(MarketEvent),
    End,
    Failed(String),
}

/// Queue depth and blocked time of one channel.
#[derive(Debug)]
pub(crate) struct Gauge {
    capacity: usize,
    depth: AtomicI64,
    high_water: AtomicI64,
    sent: AtomicU64,
    blocked_ns: AtomicU64,
}

impl Gauge {
    fn new(capacity: usize) -> Arc<Self> {
        Arc::new(Self {
            capacity,
            depth: AtomicI64::new(0),
            high_water: AtomicI64::new(0),
            sent: AtomicU64::new(0),
            blocked_ns: AtomicU64::new(0),
        })
    }

    fn received(&self) {
        self.depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn stats(&self) -> ChannelStats {
        ChannelStats {
            capacity: self.capacity,
            sent: self.sent.load(Ordering::Relaxed),
            high_water: u64::try_from(self.high_water.load(Ordering::Relaxed)).unwrap_or(0),
            blocked_ns: self.blocked_ns.load(Ordering::Relaxed),
        }
    }
}

/// A bounded sender that measures depth and blocked time.
pub(crate) struct CountedSender<T> {
    tx: SyncSender<T>,
    gauge: Arc<Gauge>,
    clock: Arc<dyn Clock>,
}

impl<T> Clone for CountedSender<T> {
    fn clone(&self) -> Self {
        Self {
            tx: self.tx.clone(),
            gauge: Arc::clone(&self.gauge),
            clock: Arc::clone(&self.clock),
        }
    }
}

impl<T> CountedSender<T> {
    /// Sends, blocking while the channel is full. `Err` when the receiver
    /// is gone.
    pub(crate) fn send(&self, msg: T) -> Result<(), ()> {
        match self.tx.try_send(msg) {
            Ok(()) => {}
            Err(TrySendError::Full(msg)) => {
                let start = self.clock.monotonic_ns();
                self.tx.send(msg).map_err(|_| ())?;
                let blocked = self.clock.monotonic_ns().saturating_sub(start);
                self.gauge.blocked_ns.fetch_add(blocked, Ordering::Relaxed);
            }
            Err(TrySendError::Disconnected(_)) => return Err(()),
        }
        self.gauge.sent.fetch_add(1, Ordering::Relaxed);
        let depth = self.gauge.depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.gauge.high_water.fetch_max(depth, Ordering::Relaxed);
        Ok(())
    }
}

fn channel<T>(
    capacity: usize,
    clock: &Arc<dyn Clock>,
) -> (CountedSender<T>, Receiver<T>, Arc<Gauge>) {
    let (tx, rx) = mpsc::sync_channel(capacity);
    let gauge = Gauge::new(capacity);
    let sender = CountedSender {
        tx,
        gauge: Arc::clone(&gauge),
        clock: Arc::clone(clock),
    };
    (sender, rx, gauge)
}

/// The live [`MarketDataProvider`]: blocks until the capture thread
/// delivers the next canonical event.
pub struct BinanceLiveProvider {
    rx: Receiver<CoreMsg>,
    gauge: Arc<Gauge>,
    ended: bool,
    failure: Option<String>,
}

impl fmt::Debug for BinanceLiveProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BinanceLiveProvider")
            .field("ended", &self.ended)
            .field("failure", &self.failure)
            .finish_non_exhaustive()
    }
}

impl MarketDataProvider for BinanceLiveProvider {
    fn next_event(&mut self) -> Result<Option<MarketEvent>, ProviderError> {
        if let Some(failure) = &self.failure {
            return Err(ProviderError::Source(failure.clone()));
        }
        if self.ended {
            return Ok(None);
        }
        let failure = match self.rx.recv() {
            Ok(CoreMsg::Event(event)) => {
                self.gauge.received();
                return Ok(Some(event));
            }
            Ok(CoreMsg::End) => {
                self.ended = true;
                return Ok(None);
            }
            Ok(CoreMsg::Failed(message)) => message,
            Err(_) => "the capture stopped without an end marker".to_owned(),
        };
        self.failure = Some(failure.clone());
        Err(ProviderError::Source(failure))
    }
}

/// The running capture threads.
pub struct CaptureHandle<S> {
    capture: JoinHandle<Result<(S, CaptureSummary), CaptureError>>,
    producers: Vec<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,
}

impl<S> fmt::Debug for CaptureHandle<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CaptureHandle")
            .field("producers", &self.producers.len())
            .finish_non_exhaustive()
    }
}

impl<S> CaptureHandle<S> {
    /// Requests shutdown (the same flag [`start`] was given).
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Waits for every thread and hands the sink back. Call it after
    /// shutdown was requested, or after the provider returned `Ok(None)` or
    /// an error. Drop the provider first if the core stopped consuming.
    ///
    /// # Errors
    ///
    /// [`CaptureError`] when the capture stopped on a sink failure or a
    /// thread panicked; the sink is lost then (its unsealed parts are
    /// discarded by the next writer's recovery).
    pub fn join(self) -> Result<(S, CaptureSummary), CaptureError> {
        let result = self.capture.join().unwrap_or_else(|_| {
            Err(CaptureError {
                message: "the capture thread panicked".to_owned(),
                summary: Box::default(),
            })
        });
        // Producers stop on the shutdown flag or once the capture thread has
        // dropped its receiver.
        self.shutdown.store(true, Ordering::Relaxed);
        for producer in self.producers {
            let _ = producer.join();
        }
        result
    }
}

/// Starts a live capture into `sink`.
///
/// # Errors
///
/// [`StartError`] for an invalid configuration or when a thread cannot be
/// spawned.
#[allow(clippy::too_many_arguments)]
pub fn start<S>(
    config: LiveConfig,
    sink: S,
    connector: Arc<dyn WsConnector>,
    http: Arc<dyn HttpGet>,
    clock: Arc<dyn Clock>,
    observer: Box<dyn CaptureObserver>,
    shutdown: Arc<AtomicBool>,
) -> Result<(BinanceLiveProvider, CaptureHandle<S>), StartError>
where
    S: RawRecordSink + Send + 'static,
{
    let keys = config.validate()?;
    let (inbound_tx, inbound_rx, inbound_gauge) = channel(config.inbound_capacity, &clock);
    let (core_tx, core_rx, core_gauge) = channel(config.core_capacity, &clock);
    let spawn_error = |e: std::io::Error| StartError(format!("spawn thread: {e}"));

    let capture = CaptureLoop {
        keys: config.streams.iter().copied().zip(keys).collect(),
        sink: Some(sink),
        pipeline: Pipeline::new(&config.symbol, config.hold_back_ms, &config.seeds),
        observer,
        clock: Arc::clone(&clock),
        rx: inbound_rx,
        inbound_gauge,
        core_tx,
        core_gauge: Arc::clone(&core_gauge),
        core_gone: false,
        shutdown: Arc::clone(&shutdown),
        seal_interval: config.seal_interval,
        stats_interval: config.stats_interval,
        next_seq: 0,
        stats: CaptureStats::default(),
    };
    let capture = thread::Builder::new()
        .name("mie-capture".to_owned())
        .spawn(move || capture.run())
        .map_err(spawn_error)?;

    let mut producers = Vec::new();
    for (index, &stream) in config.streams.iter().enumerate() {
        let tx = inbound_tx.clone();
        let task_shutdown = Arc::clone(&shutdown);
        let clock = Arc::clone(&clock);
        let name = format!("mie-{}", stream.raw_name());
        let handle = match stream.ws_path(&config.symbol) {
            Some(path) => {
                let stagger = config
                    .rotation_stagger
                    .saturating_mul(u32::try_from(index).unwrap_or(u32::MAX));
                let task = WsTask {
                    stream,
                    url: format!("{}/{path}", config.ws_base_url.trim_end_matches('/')),
                    run_id: config.run_id.clone(),
                    ping_interval: config.ping_interval,
                    liveness_timeout: config.liveness_timeout,
                    max_age: config.max_connection_age.saturating_add(stagger),
                    backoff_initial: config.backoff_initial,
                    backoff_max: config.backoff_max,
                    connector: Arc::clone(&connector),
                    clock,
                    tx,
                    shutdown: task_shutdown,
                };
                thread::Builder::new().name(name).spawn(move || task.run())
            }
            None => {
                let task = OiTask {
                    url: format!(
                        "{}/fapi/v1/openInterest?symbol={}",
                        config.rest_base_url.trim_end_matches('/'),
                        config.symbol
                    ),
                    run_id: config.run_id.clone(),
                    backoff_initial: config.backoff_initial,
                    backoff_max: config.backoff_max,
                    http: Arc::clone(&http),
                    clock,
                    tx,
                    shutdown: task_shutdown,
                };
                thread::Builder::new().name(name).spawn(move || task.run())
            }
        };
        match handle {
            Ok(handle) => producers.push(handle),
            Err(error) => {
                shutdown.store(true, Ordering::Relaxed);
                return Err(spawn_error(error));
            }
        }
    }
    drop(inbound_tx);

    let provider = BinanceLiveProvider {
        rx: core_rx,
        gauge: core_gauge,
        ended: false,
        failure: None,
    };
    let handle = CaptureHandle {
        capture,
        producers,
        shutdown,
    };
    Ok((provider, handle))
}

/// The capture thread's state.
struct CaptureLoop<S> {
    keys: BTreeMap<BinanceStream, RawStreamKey>,
    /// `None` only after a sink failure.
    sink: Option<S>,
    pipeline: Pipeline,
    observer: Box<dyn CaptureObserver>,
    clock: Arc<dyn Clock>,
    rx: Receiver<Inbound>,
    inbound_gauge: Arc<Gauge>,
    core_tx: CountedSender<CoreMsg>,
    core_gauge: Arc<Gauge>,
    core_gone: bool,
    shutdown: Arc<AtomicBool>,
    seal_interval: Duration,
    stats_interval: Duration,
    next_seq: u64,
    stats: CaptureStats,
}

impl<S: RawRecordSink> CaptureLoop<S> {
    fn run(mut self) -> Result<(S, CaptureSummary), CaptureError> {
        let mut next_seal = self.after(self.seal_interval);
        let mut next_stats = self.after(self.stats_interval);
        loop {
            match self.rx.recv_timeout(CAPTURE_TICK) {
                Ok(Inbound::Frame {
                    stream,
                    session_id,
                    receive_time_ns,
                    bytes,
                }) => {
                    self.inbound_gauge.received();
                    if let Err(message) = self.on_frame(stream, session_id, receive_time_ns, bytes)
                    {
                        return Err(self.fail(message));
                    }
                }
                Ok(Inbound::Event(event)) => {
                    self.inbound_gauge.received();
                    self.observe(&event);
                }
                Err(RecvTimeoutError::Timeout) => {}
                Err(RecvTimeoutError::Disconnected) => break,
            }
            let now = self.clock.monotonic_ns();
            if now >= next_seal {
                if let Err(message) = self.seal() {
                    return Err(self.fail(message));
                }
                next_seal = self.after(self.seal_interval);
            }
            if now >= next_stats {
                let stats = CaptureEvent::Stats(self.snapshot());
                self.observe(&stats);
                next_stats = self.after(self.stats_interval);
            }
        }
        // Every producer has stopped: release the hold-back, seal, report.
        let tail = self.pipeline.finish();
        self.deliver(tail);
        if let Err(message) = self.seal() {
            return Err(self.fail(message));
        }
        let stats = self.snapshot();
        self.observe(&CaptureEvent::Stats(stats.clone()));
        if !self.core_gone && self.core_tx.send(CoreMsg::End).is_err() {
            self.core_gone = true;
        }
        let summary = CaptureSummary {
            stats,
            core_gone: self.core_gone,
        };
        match self.sink.take() {
            Some(sink) => Ok((sink, summary)),
            None => Err(CaptureError {
                message: "the sink was lost".to_owned(),
                summary: Box::new(summary),
            }),
        }
    }

    fn after(&self, interval: Duration) -> u64 {
        let interval = u64::try_from(interval.as_nanos()).unwrap_or(u64::MAX);
        self.clock.monotonic_ns().saturating_add(interval)
    }

    fn on_frame(
        &mut self,
        stream: BinanceStream,
        session_id: String,
        receive_time_ns: i64,
        bytes: Vec<u8>,
    ) -> Result<(), String> {
        let receive_seq = self.next_seq;
        self.next_seq += 1;
        let event_time = match record_time(stream, &bytes) {
            Some(time) => time,
            None => {
                self.stats.time_fallbacks += 1;
                EventTime::from_millis(receive_time_ns.div_euclid(1_000_000))
            }
        };
        let record = RawRecord {
            event_time,
            capture: Some(Capture {
                receive_time_ns,
                receive_seq,
                session_id,
            }),
            payload: bytes,
        };
        let key = self
            .keys
            .get(&stream)
            .ok_or_else(|| format!("frame of unconfigured stream {}", stream.raw_name()))?;
        let sink = self.sink.as_mut().ok_or("the sink was lost")?;
        sink.append(key, record.clone())
            .map_err(|e| format!("append {key} receive_seq {receive_seq}: {e}"))?;
        self.stats.records += 1;
        match self.pipeline.push(stream, &record) {
            Ok(events) => self.deliver(events),
            Err(error) => self.observe(&CaptureEvent::NormalizeError {
                stream,
                receive_seq,
                error: error.to_string(),
            }),
        }
        Ok(())
    }

    fn deliver(&mut self, events: Vec<MarketEvent>) {
        for event in events {
            if let MarketEvent::FeedGap(gap) = &event {
                self.observe(&CaptureEvent::Gap(*gap));
            }
            if !self.core_gone && self.core_tx.send(CoreMsg::Event(event)).is_err() {
                // The core stopped consuming: keep persisting until the
                // producers stop.
                self.core_gone = true;
                self.shutdown.store(true, Ordering::Relaxed);
            }
        }
    }

    fn seal(&mut self) -> Result<(), String> {
        let sink = self.sink.as_mut().ok_or("the sink was lost")?;
        let files = sink.seal_all().map_err(|e| format!("seal: {e}"))?;
        if !files.is_empty() {
            self.stats.sealed_files += files.len() as u64;
            self.observe(&CaptureEvent::Sealed(files));
        }
        Ok(())
    }

    fn snapshot(&self) -> CaptureStats {
        CaptureStats {
            buffered: self.pipeline.buffered() as u64,
            pipeline: self.pipeline.stats(),
            inbound: self.inbound_gauge.stats(),
            core: self.core_gauge.stats(),
            ..self.stats.clone()
        }
    }

    fn observe(&mut self, event: &CaptureEvent) {
        let now = self.clock.now_utc_ns();
        self.observer.on_event(now, event);
    }

    /// Stops the capture after a sink failure: producers stop, the core
    /// gets the failure.
    fn fail(mut self, message: String) -> CaptureError {
        self.shutdown.store(true, Ordering::Relaxed);
        self.sink = None;
        if !self.core_gone {
            let _ = self.core_tx.send(CoreMsg::Failed(message.clone()));
        }
        let summary = CaptureSummary {
            stats: self.snapshot(),
            core_gone: self.core_gone,
        };
        CaptureError {
            message,
            summary: Box::new(summary),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_ids_are_utc_start_seconds() {
        // 2026-10-06T07:00:00Z.
        assert_eq!(run_id(1_791_270_000_000_000_000), "20261006T070000Z");
        assert_eq!(run_id(0), "19700101T000000Z");
        // 2024-02-29T23:59:59.999Z, a leap day.
        assert_eq!(run_id(1_709_251_199_999_000_000), "20240229T235959Z");
    }

    #[test]
    fn configs_are_validated() {
        assert!(LiveConfig::new("r").validate().is_ok());
        let mut twice = LiveConfig::new("r");
        twice.streams = vec![BinanceStream::AggTrade, BinanceStream::AggTrade];
        assert!(twice.validate().is_err());
        let mut slash = LiveConfig::new("a/b");
        assert!(slash.validate().is_err());
        slash.run_id = "r".to_owned();
        slash.ping_interval = Duration::ZERO;
        assert!(slash.validate().is_err());
        let mut bad_symbol = LiveConfig::new("r");
        bad_symbol.symbol = "BTC/USDT".to_owned();
        assert!(bad_symbol.validate().is_err());
    }
}
