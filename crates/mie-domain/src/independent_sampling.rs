//! An independent check of the ADR-054 context sampler (#146).
//!
//! The sampler (`crate::conditional`) and the M2 measurement model share an
//! author, so a shared misreading of ADR-054 D1–D4 would pass their twin
//! check, and the golden tapes produce no episode bridge. This module holds
//! a second model, written from the ADR text alone, a seeded tape generator
//! that produces each edge class the sampler has to get right, and a
//! harness that compares the two record by record.
//!
//! **Independence protocol.** `model.rs` was written from ADR-054 and the
//! definitions it cites (ADR-028, -031, -035, -037, -044 D3, -051 D2–D6,
//! -053 D2–D7) and the public record types, without the sampler's function
//! bodies or tests, the M2 model, the app driver or their PRs. Its first
//! commit is the freeze point; every later edit names the ADR sentence that
//! justifies it. It imports only the record types from `crate::conditional`
//! and re-derives the baseline zone filter from ADR-044 D3 instead of
//! calling the crate's location reader; a test pins both.
//!
//! - [`model`]: the independent model of D1–D4, and the D4 episode rule on
//!   its own.
//! - [`tapes`]: the seeded generator, one scenario per edge class plus fuzz
//!   tapes, and the corpus with its sampler starts.
//! - [`streams`]: synthetic trigger streams that drive the public episode
//!   tracker against the model's rule, where a one-sided join is
//!   observable (on engine tapes it is equivalent).
//! - [`harness`]: one engine, several (sampler, model) pairs, every record,
//!   span and count compared, edge classes counted from the model's view.
//! - [`tests`]: the comparisons, the coverage of every class, determinism
//!   and the independence guard.
mod harness;
mod model;
mod streams;
mod tapes;
mod tests;
