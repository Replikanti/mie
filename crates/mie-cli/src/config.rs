//! The configuration files (TOML) of `mie ingest` ([`IngestConfig`]) and of
//! the `mie archive-*` commands ([`ArchiveConfig`]).
//!
//! Ingest: `[instrument]`, `[paths]` and `[binance]` are required; `[capture]`
//! and `binance.streams` have defaults. Unknown keys are rejected. Public market
//! data needs no secrets; a future secret is read from the environment,
//! never from this file.

use mie_adapter_binance::archive::ArchiveStream;
use mie_adapter_binance::{BinanceStream, LiveConfig, OI_POLL_INTERVAL_MS};
use mie_domain::bars::Timeframe;
use mie_domain::time::EventTime;
use mie_ports::raw::validate_segment;
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// Binance's limit on the lifetime of one WebSocket connection.
const CONNECTION_LIMIT_SECS: u64 = 86_400;

/// The whole configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestConfig {
    /// What is captured.
    pub instrument: Instrument,
    /// Where it goes.
    pub paths: Paths,
    /// The exchange endpoints.
    pub binance: Binance,
    /// Capture tuning; every key has a default.
    #[serde(default)]
    pub capture: CaptureSettings,
}

/// `[instrument]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Instrument {
    /// Exchange symbol, also the raw-store instrument, e.g. `BTCUSDT`.
    pub symbol: String,
    /// Raw-store source, e.g. `binance-um`.
    pub source: String,
}

/// `[paths]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Paths {
    /// Root directory of the raw store.
    pub raw_root: PathBuf,
    /// The capture journal (JSON Lines, appended to).
    pub journal: PathBuf,
}

/// `[binance]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binance {
    /// WebSocket base URL; the stream path is appended.
    pub ws_base_url: String,
    /// REST base URL.
    pub rest_base_url: String,
    /// Raw stream names to capture; default: all five.
    #[serde(default = "all_streams")]
    pub streams: Vec<String>,
}

fn all_streams() -> Vec<String> {
    BinanceStream::ALL
        .iter()
        .map(|s| s.raw_name().to_owned())
        .collect()
}

/// `[capture]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CaptureSettings {
    /// Wall-clock seal cadence; bounds the crash loss (ADR-030 D5).
    pub seal_interval_secs: u64,
    /// Canonical merge hold-back in exchange ms (ADR-028 D6).
    pub hold_back_ms: u64,
    /// How late an open-interest sample may be and still be delivered,
    /// re-timed (ADR-032 D12); one poll interval by default.
    pub oi_retime_allowance_ms: u64,
    /// Planned reconnect age of the first stream.
    pub max_connection_age_secs: u64,
    /// Added to the reconnect age per stream position.
    pub rotation_stagger_secs: u64,
    /// Client ping cadence.
    pub ping_interval_secs: u64,
    /// A silent connection is dropped after this long.
    pub liveness_timeout_secs: u64,
    /// First reconnect backoff.
    pub backoff_initial_ms: u64,
    /// Largest reconnect backoff.
    pub backoff_max_ms: u64,
    /// Producer → capture channel capacity.
    pub inbound_channel_capacity: usize,
    /// Capture → core channel capacity.
    pub core_channel_capacity: usize,
    /// Journal stats cadence.
    pub stats_interval_secs: u64,
}

impl Default for CaptureSettings {
    fn default() -> Self {
        Self {
            seal_interval_secs: 300,
            hold_back_ms: 2_000,
            oi_retime_allowance_ms: u64::from(OI_POLL_INTERVAL_MS),
            max_connection_age_secs: 82_800,
            rotation_stagger_secs: 300,
            ping_interval_secs: 30,
            liveness_timeout_secs: 90,
            backoff_initial_ms: 250,
            backoff_max_ms: 30_000,
            inbound_channel_capacity: 65_536,
            core_channel_capacity: 65_536,
            stats_interval_secs: 60,
        }
    }
}

/// Why a configuration was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid configuration: {}", self.0)
    }
}

impl std::error::Error for ConfigError {}

impl IngestConfig {
    /// Reads and validates the file at `path`.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when the file cannot be read, parsed or validated.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("read {}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// Parses and validates TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for unknown or missing keys and invalid values.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text).map_err(|e| ConfigError(e.to_string()))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        let segment = |name: &str, value: &str| {
            validate_segment(value).map_err(|e| ConfigError(format!("{name}: {e}")))
        };
        segment("instrument.symbol", &self.instrument.symbol)?;
        segment("instrument.source", &self.instrument.source)?;
        let streams = self.streams()?;
        if streams.is_empty() {
            return Err(ConfigError("binance.streams is empty".to_owned()));
        }
        for (name, url) in [
            ("binance.ws_base_url", &self.binance.ws_base_url),
            ("binance.rest_base_url", &self.binance.rest_base_url),
        ] {
            if !url.contains("://") {
                return Err(ConfigError(format!("{name} {url:?} is not a URL")));
            }
        }
        let c = &self.capture;
        let positive = [
            ("seal_interval_secs", c.seal_interval_secs),
            ("max_connection_age_secs", c.max_connection_age_secs),
            ("ping_interval_secs", c.ping_interval_secs),
            ("liveness_timeout_secs", c.liveness_timeout_secs),
            ("backoff_initial_ms", c.backoff_initial_ms),
            ("backoff_max_ms", c.backoff_max_ms),
            ("stats_interval_secs", c.stats_interval_secs),
            (
                "inbound_channel_capacity",
                c.inbound_channel_capacity as u64,
            ),
            ("core_channel_capacity", c.core_channel_capacity as u64),
        ];
        if let Some((name, _)) = positive.iter().find(|(_, v)| *v == 0) {
            return Err(ConfigError(format!("capture.{name} must be positive")));
        }
        if c.backoff_max_ms < c.backoff_initial_ms {
            return Err(ConfigError(
                "capture.backoff_max_ms is below backoff_initial_ms".to_owned(),
            ));
        }
        let last_rotation = c.max_connection_age_secs.saturating_add(
            c.rotation_stagger_secs
                .saturating_mul(streams.len().saturating_sub(1) as u64),
        );
        if last_rotation >= CONNECTION_LIMIT_SECS {
            return Err(ConfigError(format!(
                "the last staggered reconnect at {last_rotation} s is not before the \
                 exchange's {CONNECTION_LIMIT_SECS} s connection limit"
            )));
        }
        Ok(())
    }

    /// The configured streams, in configuration order.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for an unknown or repeated stream name.
    pub fn streams(&self) -> Result<Vec<BinanceStream>, ConfigError> {
        let mut streams = Vec::new();
        for name in &self.binance.streams {
            let stream = BinanceStream::from_raw_name(name)
                .ok_or_else(|| ConfigError(format!("unknown stream {name:?}")))?;
            if streams.contains(&stream) {
                return Err(ConfigError(format!("stream {name:?} listed twice")));
            }
            streams.push(stream);
        }
        Ok(streams)
    }

    /// The live capture configuration of a run.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when the streams do not validate.
    pub fn live_config(
        &self,
        run_id: &str,
        seeds: BTreeMap<BinanceStream, EventTime>,
    ) -> Result<LiveConfig, ConfigError> {
        let c = &self.capture;
        let mut live = LiveConfig::new(run_id);
        live.symbol = self.instrument.symbol.clone();
        live.source = self.instrument.source.clone();
        live.ws_base_url = self.binance.ws_base_url.clone();
        live.rest_base_url = self.binance.rest_base_url.clone();
        live.streams = self.streams()?;
        live.hold_back_ms = i64::try_from(c.hold_back_ms)
            .map_err(|_| ConfigError("capture.hold_back_ms is too large".to_owned()))?;
        live.oi_retime_ms = i64::try_from(c.oi_retime_allowance_ms)
            .map_err(|_| ConfigError("capture.oi_retime_allowance_ms is too large".to_owned()))?;
        live.seal_interval = Duration::from_secs(c.seal_interval_secs);
        live.max_connection_age = Duration::from_secs(c.max_connection_age_secs);
        live.rotation_stagger = Duration::from_secs(c.rotation_stagger_secs);
        live.ping_interval = Duration::from_secs(c.ping_interval_secs);
        live.liveness_timeout = Duration::from_secs(c.liveness_timeout_secs);
        live.backoff_initial = Duration::from_millis(c.backoff_initial_ms);
        live.backoff_max = Duration::from_millis(c.backoff_max_ms);
        live.inbound_capacity = c.inbound_channel_capacity;
        live.core_capacity = c.core_channel_capacity;
        live.stats_interval = Duration::from_secs(c.stats_interval_secs);
        live.seeds = seeds;
        Ok(live)
    }
}

/// The `mie archive-*` configuration file (TOML).
///
/// `[instrument]` and `[paths]` are required, `[archive]` has defaults.
/// Unknown keys are rejected, a `source` key among them: the archive source
/// is fixed in code (`binance-archive`), so archive provenance can never
/// land in the live source.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveConfig {
    /// What is imported.
    pub instrument: ArchiveInstrument,
    /// Where it goes.
    pub paths: ArchivePaths,
    /// The archive and how politely it is read.
    #[serde(default)]
    pub archive: ArchiveSettings,
}

/// `[instrument]` of the archive configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchiveInstrument {
    /// Exchange symbol, also the raw-store instrument, e.g. `BTCUSDT`.
    pub symbol: String,
}

/// `[paths]` of the archive configuration.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArchivePaths {
    /// Root directory of the raw store, shared with live capture.
    pub raw_root: PathBuf,
    /// The import ledger directory.
    pub import_ledger: PathBuf,
    /// Download staging; each zip is removed once imported.
    pub staging: PathBuf,
}

/// `[archive]`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ArchiveSettings {
    /// Archive base URL.
    pub base_url: String,
    /// Datasets to import: `aggTrades`, `klines`, `fundingRate`, `metrics`,
    /// `bookDepth`, `trades`.
    pub streams: Vec<String>,
    /// Kline intervals imported for `klines`.
    pub kline_intervals: Vec<String>,
    /// Least time between two request starts.
    pub request_interval_ms: u64,
    /// Most attempts per request.
    pub max_attempts: u32,
    /// First retry wait.
    pub backoff_initial_ms: u64,
    /// Longest retry wait.
    pub backoff_max_ms: u64,
}

impl Default for ArchiveSettings {
    fn default() -> Self {
        Self {
            base_url: "https://data.binance.vision".to_owned(),
            streams: ["aggTrades", "klines", "fundingRate", "metrics", "bookDepth"]
                .map(str::to_owned)
                .to_vec(),
            kline_intervals: Timeframe::ALL.map(|t| t.label().to_owned()).to_vec(),
            request_interval_ms: 100,
            max_attempts: 5,
            backoff_initial_ms: 1_000,
            backoff_max_ms: 60_000,
        }
    }
}

impl ArchiveConfig {
    /// Reads and validates the file at `path`.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] when the file cannot be read, parsed or validated.
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("read {}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// Parses and validates TOML text.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for unknown or missing keys and invalid values.
    pub fn parse(text: &str) -> Result<Self, ConfigError> {
        let config: Self = toml::from_str(text).map_err(|e| ConfigError(e.to_string()))?;
        validate_segment(&config.instrument.symbol)
            .map_err(|e| ConfigError(format!("instrument.symbol: {e}")))?;
        let a = &config.archive;
        if !a.base_url.contains("://") {
            return Err(ConfigError(format!(
                "archive.base_url {:?} is not a URL",
                a.base_url
            )));
        }
        if a.max_attempts == 0 || a.backoff_initial_ms == 0 {
            return Err(ConfigError(
                "archive.max_attempts and archive.backoff_initial_ms must be positive".to_owned(),
            ));
        }
        if a.backoff_max_ms < a.backoff_initial_ms {
            return Err(ConfigError(
                "archive.backoff_max_ms is below backoff_initial_ms".to_owned(),
            ));
        }
        if config.streams()?.is_empty() {
            return Err(ConfigError("archive.streams is empty".to_owned()));
        }
        Ok(config)
    }

    /// The configured streams, in import order.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for an unknown or repeated stream or interval.
    pub fn streams(&self) -> Result<Vec<ArchiveStream>, ConfigError> {
        Self::expand(&self.archive.streams, &self.archive.kline_intervals)
    }

    /// Expands dataset names into streams: `klines` becomes one stream per
    /// configured interval, `klines_<interval>` names one, any other name a
    /// raw stream name. The result is in import order.
    ///
    /// # Errors
    ///
    /// [`ConfigError`] for an unknown or repeated name or interval.
    pub fn expand(
        names: &[String],
        intervals: &[String],
    ) -> Result<Vec<ArchiveStream>, ConfigError> {
        let mut timeframes = Vec::new();
        for label in intervals {
            let timeframe = Timeframe::ALL
                .into_iter()
                .find(|t| t.label() == label)
                .ok_or_else(|| ConfigError(format!("unknown kline interval {label:?}")))?;
            if timeframes.contains(&timeframe) {
                return Err(ConfigError(format!(
                    "kline interval {label:?} listed twice"
                )));
            }
            timeframes.push(timeframe);
        }
        let mut streams = BTreeSet::new();
        for name in names {
            let expanded: Vec<ArchiveStream> = if name == "klines" {
                timeframes
                    .iter()
                    .map(|&t| ArchiveStream::Klines(t))
                    .collect()
            } else {
                vec![
                    ArchiveStream::from_raw_name(name)
                        .ok_or_else(|| ConfigError(format!("unknown archive stream {name:?}")))?,
                ]
            };
            for stream in expanded {
                if !streams.insert(stream) {
                    return Err(ConfigError(format!("stream {stream} listed twice")));
                }
            }
        }
        Ok(ArchiveStream::ALL
            .into_iter()
            .filter(|s| streams.contains(s))
            .collect())
    }
}
