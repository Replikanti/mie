//! Raw record store contract (ADR-003, ADR-022, ADR-030).
//!
//! The raw store keeps every exchange message verbatim: the exact payload
//! bytes (WebSocket/REST JSON or an archive CSV row) plus capture metadata.
//! It is the source of truth every derived feature is reproduced from.
//! Parsing and the fixed-point conversion of ADR-027 happen in
//! normalization, never before persisting, so a message that fails to parse
//! is still kept.
//!
//! Producers (live capture, archive import) write through [`RawRecordSink`];
//! replay and research read through [`RawRecordSource`]. Both sides meet in
//! this module, so no adapter depends on another (ADR-025).
//!
//! Sealed files are immutable. Each one is described by a [`SealedFile`]
//! (its manifest), and any replay window resolves to a [`RawDataset`] whose
//! [`DatasetVersion`] identifies exactly the files it covers.

use crate::outbound::ReplayWindow;
use mie_domain::research::DataVersion;
use mie_domain::time::EventTime;
use std::collections::BTreeSet;
use std::fmt;

/// Longest allowed path segment (source, instrument or stream name).
pub const MAX_SEGMENT_LEN: usize = 64;

/// Checks that `segment` can name a source, instrument or stream.
///
/// A segment is 1 to [`MAX_SEGMENT_LEN`] ASCII characters from
/// `[A-Za-z0-9_-]`, so it is safe as a path segment and as a Hive partition
/// value on every platform (ADR-030).
///
/// # Errors
///
/// [`RawStoreError::Invalid`] when the segment is empty, too long or contains
/// any other character.
pub fn validate_segment(segment: &str) -> Result<(), RawStoreError> {
    if segment.is_empty() || segment.len() > MAX_SEGMENT_LEN {
        return Err(RawStoreError::Invalid(format!(
            "segment {segment:?} must be 1 to {MAX_SEGMENT_LEN} characters"
        )));
    }
    if !segment
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(RawStoreError::Invalid(format!(
            "segment {segment:?} may contain only A-Z, a-z, 0-9, '_' and '-'"
        )));
    }
    Ok(())
}

/// Identifies one raw stream: who published it, for which instrument, and
/// which feed (for example `binance-um` / `BTCUSDT` / `aggTrade`).
///
/// Ordered by source, then instrument, then stream.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RawStreamKey {
    source: String,
    instrument: String,
    stream: String,
}

impl RawStreamKey {
    /// Creates a key from three validated segments.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Invalid`] when a segment fails [`validate_segment`].
    pub fn new(source: &str, instrument: &str, stream: &str) -> Result<Self, RawStoreError> {
        validate_segment(source)?;
        validate_segment(instrument)?;
        validate_segment(stream)?;
        Ok(Self {
            source: source.to_owned(),
            instrument: instrument.to_owned(),
            stream: stream.to_owned(),
        })
    }

    /// The publishing source, for example `binance-um`.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The instrument, for example `BTCUSDT`.
    pub fn instrument(&self) -> &str {
        &self.instrument
    }

    /// The feed, for example `aggTrade`.
    pub fn stream(&self) -> &str {
        &self.stream
    }
}

impl fmt::Display for RawStreamKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}/{}", self.source, self.instrument, self.stream)
    }
}

/// Capture metadata of a live message (ADR-028 D1: metadata only).
///
/// Archive records have none: the public archive carries exchange fields
/// only.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Capture {
    /// Local receive time in nanoseconds since the Unix epoch (UTC). Used for
    /// latency research; it never orders anything.
    pub receive_time_ns: i64,
    /// Position of the message in the capture session's arrival order.
    pub receive_seq: u64,
    /// Identifies the connection or capture session the message came from.
    pub session_id: String,
}

/// One exchange message, stored verbatim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawRecord {
    /// The exchange ordering time of ADR-028, supplied by the producer. It
    /// selects the record's UTC date partition. Receive time never does.
    pub event_time: EventTime,
    /// Capture metadata; `None` for archive records.
    pub capture: Option<Capture>,
    /// The exact message bytes as received, never re-encoded.
    pub payload: Vec<u8>,
}

/// The manifest of one sealed, immutable file (ADR-030).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedFile {
    /// Path relative to the store root, with `/` separators.
    pub relative_path: String,
    /// The stream the file belongs to.
    pub stream: RawStreamKey,
    /// UTC date partition, `YYYY-MM-DD`.
    pub date: String,
    /// Part number within the date partition.
    pub part: u32,
    /// Number of records.
    pub rows: u64,
    /// File size in bytes.
    pub bytes: u64,
    /// Smallest record event time.
    pub min_event_time: EventTime,
    /// Largest record event time.
    pub max_event_time: EventTime,
    /// SHA-256 of the file, 64 lowercase hex characters.
    pub sha256: String,
}

/// What a replay or research run reads: a set of streams over a window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawSelection {
    streams: BTreeSet<RawStreamKey>,
    window: ReplayWindow,
}

impl RawSelection {
    /// Selects `streams` over the half-open `window`.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Invalid`] when `streams` is empty or the window is
    /// empty (`start >= end`).
    pub fn new(
        streams: BTreeSet<RawStreamKey>,
        window: ReplayWindow,
    ) -> Result<Self, RawStoreError> {
        if streams.is_empty() {
            return Err(RawStoreError::Invalid(
                "a selection needs at least one stream".to_owned(),
            ));
        }
        if window.start >= window.end {
            return Err(RawStoreError::Invalid(format!(
                "empty window [{}, {})",
                window.start, window.end
            )));
        }
        Ok(Self { streams, window })
    }

    /// The selected streams, in order.
    pub fn streams(&self) -> &BTreeSet<RawStreamKey> {
        &self.streams
    }

    /// The selected window.
    pub fn window(&self) -> ReplayWindow {
        self.window
    }
}

/// Identifies the exact raw data behind a selection: the lowercase hex
/// SHA-256 of a canonical text over the window, the streams and the sealed
/// files it covers (ADR-030). Experiments record it as provenance.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DatasetVersion(String);

impl DatasetVersion {
    /// Parses 64 lowercase hex characters.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Invalid`] for any other input.
    pub fn from_hex(hex: &str) -> Result<Self, RawStoreError> {
        if is_sha256_hex(hex) {
            Ok(Self(hex.to_owned()))
        } else {
            Err(RawStoreError::Invalid(format!(
                "dataset version {hex:?} is not 64 lowercase hex characters"
            )))
        }
    }

    /// The version as 64 lowercase hex characters.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for DatasetVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// The domain's mirror of a dataset version, as experiments record it
/// (ADR-040). ADR-030 keeps [`DatasetVersion`] in the ports; the domain
/// cannot depend on them.
impl From<&DatasetVersion> for DataVersion {
    fn from(version: &DatasetVersion) -> Self {
        DataVersion::from_hex(version.as_str())
            .expect("a dataset version is 64 lowercase hex characters")
    }
}

/// Whether `text` is a SHA-256 digest in the store's notation: exactly 64
/// lowercase hex characters.
pub fn is_sha256_hex(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

/// The sealed files a selection covers, and their version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawDataset {
    /// Identifies exactly `files` for the selection.
    pub version: DatasetVersion,
    /// The sealed files overlapping the window, sorted by path.
    pub files: Vec<SealedFile>,
}

/// Where producers write raw records.
///
/// Records are buffered into open parts and become visible only once sealed.
/// Sealing is all-or-nothing: a sealed file is complete and immutable, and an
/// interrupted writer loses at most the records written since the last seal.
pub trait RawRecordSink {
    /// Appends one record to `stream`.
    ///
    /// # Errors
    ///
    /// [`RawStoreError`] when the record is rejected or the store fails.
    fn append(&mut self, stream: &RawStreamKey, record: RawRecord) -> Result<(), RawStoreError>;

    /// Seals every open part, so all records appended so far are durable.
    /// Returns the files sealed since the previous call, rotation seals
    /// included, in seal order.
    ///
    /// # Errors
    ///
    /// [`RawStoreError`] when sealing fails.
    fn seal_all(&mut self) -> Result<Vec<SealedFile>, RawStoreError>;
}

/// Where replay and research read raw records.
pub trait RawRecordSource {
    /// Resolves a selection to the sealed files it covers and their version.
    ///
    /// # Errors
    ///
    /// [`RawStoreError`] when the store cannot be listed or a manifest is
    /// malformed or misplaced.
    fn select(&self, selection: &RawSelection) -> Result<RawDataset, RawStoreError>;

    /// Reads one sealed file, verifying it against `file` before returning
    /// anything. Records come back in stored (append) order.
    ///
    /// # Errors
    ///
    /// [`RawStoreError::Integrity`] when the file does not match its
    /// manifest; other variants when it cannot be read or decoded.
    fn read(&self, file: &SealedFile) -> Result<Vec<RawRecord>, RawStoreError>;
}

/// Why the raw store refused or failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RawStoreError {
    /// The caller passed something the store does not accept.
    Invalid(String),
    /// Another writer holds the source.
    Locked(String),
    /// The underlying storage failed.
    Io(String),
    /// Stored data does not match its manifest (tampering or corruption of a
    /// sealed file).
    Integrity(String),
    /// Stored data or metadata is malformed or misplaced.
    Corrupt(String),
}

impl fmt::Display for RawStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Invalid(detail) => write!(f, "invalid raw store request: {detail}"),
            Self::Locked(detail) => write!(f, "raw store is locked: {detail}"),
            Self::Io(detail) => write!(f, "raw store I/O failed: {detail}"),
            Self::Integrity(detail) => write!(f, "raw store integrity check failed: {detail}"),
            Self::Corrupt(detail) => write!(f, "raw store data is corrupt: {detail}"),
        }
    }
}

impl std::error::Error for RawStoreError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_dataset_version_converts_to_the_domain_mirror() {
        let hex = "0f".repeat(32);
        let dataset = DatasetVersion::from_hex(&hex).unwrap();
        let mirror = DataVersion::from(&dataset);
        assert_eq!(mirror.as_str(), dataset.as_str());
        assert_eq!(mirror, DataVersion::from_hex(&hex).unwrap());
    }

    fn window(start: i64, end: i64) -> ReplayWindow {
        ReplayWindow {
            start: EventTime::from_millis(start),
            end: EventTime::from_millis(end),
        }
    }

    #[test]
    fn segments_accept_exchange_names() {
        for ok in ["binance-um", "BTCUSDT", "aggTrade", "kline_1m"] {
            assert_eq!(validate_segment(ok), Ok(()), "{ok}");
        }
        assert_eq!(validate_segment(&"a".repeat(64)), Ok(()));
    }

    #[test]
    fn segments_reject_unsafe_names() {
        let long = "a".repeat(65);
        for bad in [
            "",
            "a/b",
            "a=b",
            "..",
            ".x",
            "a b",
            "čau",
            "a\\b",
            long.as_str(),
        ] {
            assert!(
                matches!(validate_segment(bad), Err(RawStoreError::Invalid(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn stream_keys_validate_and_order() {
        assert!(RawStreamKey::new("binance-um", "BTC/USDT", "aggTrade").is_err());
        let a = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let b = RawStreamKey::new("binance-um", "BTCUSDT", "depth").unwrap();
        assert!(a < b);
        assert_eq!(a.to_string(), "binance-um/BTCUSDT/aggTrade");
        assert_eq!(
            (a.source(), a.instrument(), a.stream()),
            ("binance-um", "BTCUSDT", "aggTrade")
        );
    }

    #[test]
    fn selections_reject_empty_streams_and_windows() {
        let key = RawStreamKey::new("binance-um", "BTCUSDT", "aggTrade").unwrap();
        let one = BTreeSet::from([key]);
        assert!(matches!(
            RawSelection::new(BTreeSet::new(), window(0, 1)),
            Err(RawStoreError::Invalid(_))
        ));
        assert!(matches!(
            RawSelection::new(one.clone(), window(5, 5)),
            Err(RawStoreError::Invalid(_))
        ));
        assert!(matches!(
            RawSelection::new(one.clone(), window(6, 5)),
            Err(RawStoreError::Invalid(_))
        ));
        let selection = RawSelection::new(one.clone(), window(5, 6)).unwrap();
        assert_eq!(selection.streams(), &one);
        assert_eq!(selection.window(), window(5, 6));
    }

    #[test]
    fn dataset_versions_are_lowercase_sha256_hex() {
        let hex = "0123456789abcdef".repeat(4);
        assert_eq!(DatasetVersion::from_hex(&hex).unwrap().as_str(), hex);
        assert_eq!(DatasetVersion::from_hex(&hex).unwrap().to_string(), hex);
        let upper = hex.to_ascii_uppercase();
        for bad in [&upper, &hex[..63], &format!("{hex}0"), &"g".repeat(64), ""] {
            assert!(
                matches!(
                    DatasetVersion::from_hex(bad),
                    Err(RawStoreError::Invalid(_))
                ),
                "{bad:?}"
            );
        }
    }
}
