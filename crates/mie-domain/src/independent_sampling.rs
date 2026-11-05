//! An independent check of the ADR-054 context sampler (#146).
//!
//! The sampler (`crate::conditional`) and the M2 measurement model share an
//! author, so a shared misreading of ADR-054 D1–D4 would pass their twin
//! check. This module holds a second model, written from the ADR text
//! alone, and compares it record by record with the sampler.
//!
//! - [`model`]: the independent model of D1–D4.
mod model;
