//! E1′ harness: the `TxnEngine` trait, shared txn components, reference
//! engine and behaviour suite (card X-1a; PLAN-v3 §4 E1′, §2 N2/N3, §3.4).
//!
//! PLAN-v3's E1′ experiment decides N3, the MVCC scheme: arm (b) is
//! timestamp-in-key versions on the X-0 B+tree (card X-2), arm (c) is
//! newest-in-place + undo (card X-3). Both arms run with **identical
//! write-path components** — the sequencer, snapshot registry, write set,
//! lock table and commit protocol of [`common`] — so that only the
//! version layout differs between them.
//!
//! This card builds the shared half plus:
//! - [`reference::RefEngine`], a correct-by-construction engine with no
//!   tree (the differential oracle);
//! - [`suite`], the behaviour suite every arm must pass, including the
//!   seeded differential S13 against the reference.
//!
//! The transactional semantics imitated here are v2's C-T0 (§3 commit
//! pipeline, §4 visibility, §5 write path and EPQ, §9 GC) under N2's
//! shape: private write sets, an in-memory lock table, versions applied
//! at commit. SERIALIZABLE/SSI is out of scope for E1′ (card Goal).
//!
//! Invariants owned crate-wide:
//! - every pub item is documented; no `unsafe`; no `unwrap`/`expect`/
//!   `panic!` outside tests (PLAN-v3 §6.1, §1.6);
//! - the mutation evidence of card X-1a (`RELEASE-BEFORE-VISIBLE`,
//!   `RR-NO-CONFLICT`, `SP-ROLLBACK-PARTIAL`, `VISIBLE-JUMPS`,
//!   `GC-TOO-EAGER`) is caught by the suite scenarios named in the card;
//! - arms (X-2, X-3) implement [`api::TxnEngine`] with the [`common`]
//!   components and pass [`suite::run_all`] unchanged.

pub mod api;
pub mod common;
pub mod reference;
pub mod suite;

pub use api::{EngineStats, Isolation, SavepointId, Ts, TxnEngine, TxnErr, TxnId, Wait};
