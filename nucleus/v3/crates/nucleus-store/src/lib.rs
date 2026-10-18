//! Storage engine for Nucleus v3 (PLAN-v3 §3.3).
//!
//! One page file plus a `wal/` segment directory: pages addressed by logical
//! page id through a persisted page map, a buffer manager, B+trees, a logical
//! redo-only WAL, copy-on-write checkpoints and recovery. Normative design:
//! `docs/S-STORE.md` (card S-1). Implementation: Phase A cards.
#![deny(clippy::indexing_slicing)]
