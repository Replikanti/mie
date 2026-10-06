//! Historical backfill from the Binance public data archive
//! (`data.binance.vision`, USDⓈ-M futures; ADR-034).
//!
//! Every CSV row of a checksum-verified archive file is stored verbatim as
//! one raw record without capture metadata, under the fixed source
//! [`ARCHIVE_SOURCE`], so archive provenance never mixes with live capture
//! (ADR-003, ADR-022, ADR-030).
//!
//! | Module | Role |
//! |---|---|
//! | [`catalog`] | Datasets, archive paths, UTC days and months |
//! | [`normalize`] | Archive row → domain events, shared with replay (#11) |
//! | [`fetch`] | Paced, retried, checksum-verified downloads and zip lines |

pub mod catalog;
pub mod fetch;
pub mod normalize;

/// The raw-store source of every archive record. Fixed in code, never
/// configurable, so archive data can never land in the live source.
pub const ARCHIVE_SOURCE: &str = "binance-archive";

pub use catalog::{ArchiveStream, Period, PeriodKind};
