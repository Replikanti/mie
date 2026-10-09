//! Event time.
//!
//! The domain never reads a clock: "now" is the time of the latest event it
//! has consumed. That is what lets live processing and historical replay
//! produce identical results (ADR-019).

use crate::fingerprint::Fingerprinter;
use crate::state_hash::StateEncode;
use std::fmt;

/// Exchange event time in milliseconds since the Unix epoch (UTC).
///
/// Millisecond resolution matches the timestamps Binance USDⓈ-M futures
/// publishes. The order of distinct events that share a timestamp is the
/// canonical order of ADR-028 (`Ord for MarketEvent`), not part of this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EventTime(i64);

impl EventTime {
    /// Creates an event time from milliseconds since the Unix epoch.
    pub const fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Milliseconds since the Unix epoch.
    pub const fn as_millis(self) -> i64 {
        self.0
    }
}

impl StateEncode for EventTime {
    /// `write_i64` of the epoch milliseconds (ADR-041).
    fn encode(&self, f: &mut Fingerprinter) {
        f.write_i64(self.0);
    }
}

impl fmt::Display for EventTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ms", self.0)
    }
}
