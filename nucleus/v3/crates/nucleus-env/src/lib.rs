//! Environment abstraction for Nucleus v3 (PLAN-v3 §3.2).
//!
//! Every file, clock and randomness access made by the engine goes through
//! this crate, so the same engine runs on a real filesystem, in memory, in
//! the deterministic simulator and in the browser. The trait and backends
//! land in cards A-1a and A-1b, specified by `docs/S-STORE.md` (card S-1).
#![deny(clippy::indexing_slicing)]
