//! Raw-store adapter: immutable, verbatim Parquet files (ADR-003, ADR-022,
//! ADR-030).
//!
//! Implements the raw store ports of [`mie_ports::raw`]:
//!
//! - [`RawWriter`] ([`RawRecordSink`](mie_ports::raw::RawRecordSink)) buffers
//!   records into open parts and seals them with a temp file, fsync, a
//!   durable pending manifest and an atomic rename. A sealed file never
//!   changes again, and a killed writer never leaves a partial one visible.
//! - [`ParquetRawStore`] ([`RawRecordSource`](mie_ports::raw::RawRecordSource))
//!   resolves a replay window to its sealed files and dataset version, and
//!   reads each file only after verifying it against its manifest.
//!
//! Every message is stored verbatim: the exact payload bytes plus capture
//! metadata in one exchange-agnostic envelope (schema v1). Parsing and
//! fixed-point conversion (ADR-027) belong to normalization, shared by live
//! and replay. Only `mie-cli` wires this adapter (ADR-025).
//!
//! The layout is Hive-partitioned, so DuckDB reads sealed files directly:
//!
//! ```sql
//! SELECT event_time, json_extract_string(decode(payload), '$.p') AS price
//! FROM read_parquet(
//!     '<root>/source=binance-um/instrument=BTCUSDT/stream=aggTrade/*/*.parquet',
//!     hive_partitioning = true)
//! WHERE date = '2026-10-06'
//! ORDER BY event_time;
//! ```
//!
//! Files in progress are hidden (`.part-NNNNN.parquet.tmp`) and never match
//! `*.parquet`.

// Format pieces only; the writer and the reader that use them land in the
// next commit of the same PR.
#![allow(dead_code)]

mod layout;
mod manifest;
mod schema;
