//! E1′ arm (b) (card X-2; PLAN-v3 §4 E1′, §2 N2/N3): timestamp-in-key
//! versions on the X-0 B+tree — the v2 MVCC model without intents.
//!
//! This crate exists to be measured against arm (c) (card X-3) and to
//! pass the e1-mvcc behaviour suite, including the seeded S13
//! differential against the reference engine. It adds no behaviour the
//! [`TxnEngine`](e1_mvcc::api::TxnEngine) trait does not name and owns
//! none of the shared txn components (the sequencer, snapshot registry,
//! write set, lock table and commit protocol are e1-mvcc's, unchanged —
//! the lock table as cards X-1a-F1/F1-F2 left it, per the X-1a verdict).
//!
//! Invariants owned crate-wide (detailed in [`engine`]'s docs, pinned by
//! the suite's named catchers):
//! - B-ENC-ORDER / B-ENC-ROUNDTRIP — the encoded tree order is
//!   `(user key asc, ts desc)` and decodes exactly ([`encode`]);
//! - B-STORE / B-SEEK / B-SCAN-SKIP / B-NEWEST / B-GC / B-HOOKS /
//!   B-FAIL-STOP — see [`engine`];
//! - no `unsafe`, no `unwrap`/`expect`/`panic!` outside tests, every
//!   pub item documented (PLAN-v3 §6.1, §1.6).

mod encode;
mod engine;

pub use engine::{ArmB, ArmBTxn};
