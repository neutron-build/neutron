//! G2: the isolation checker (Elle-style) over the real `nucleus-txn` public
//! API. Test code only: the checker reads a drained history and never an
//! engine internal.
//!
//! - `history`: the append-only recorder, the history types, the compact
//!   trace format, and a builder for hand-made histories.
//! - `check`: the Adya DSG checker, the per-level verdicts and the outcome
//!   cross-checks (40001 / 40P01 / 23505 / 23503).
//! - `runner`: sessions over a shared `Core<MemKv>` (real threads, the real
//!   parker), the seeded workload generator and the harness-level bug
//!   injections.
//! - `inject`: the anomaly-injection suite (the checker's proof of life).
//! - `soak`: the bounded multi-level soak.

mod g2 {
    pub mod check;
    pub mod history;
    pub mod runner;
}
