//! E0 micro-spike: a slotted-page B+tree on a simple buffer pool
//! (card X-0; PLAN-v3 §4 E0, design §3.3).
//!
//! This crate answers one question: can a **safe-Rust** page / buffer-pool /
//! B+tree reach the E0 targets (PLAN-v3 §4: ours ≥ 0.8× SQLite per core on
//! point and range lookups in memory)? It is a spike, not production code:
//! the buffer pool is deliberately simple (N8), there is no WAL, no merge on
//! delete and no overflow pages. It is also the substrate for the E1′ arms
//! (cards X-1a…X-3, in memory) and for E4 (card X-5, which implements
//! [`store::PageStore`] over a page map).
//!
//! Invariants owned by this crate as a whole:
//! - no `unsafe` anywhere (N8/N9: `unsafe_code = "forbid"` via the workspace
//!   lints), and no panics on data-reachable paths: every corrupt byte
//!   sequence surfaces as [`store::StoreError::Corrupt`];
//! - [`store::PageStore`] is the binding I/O surface for X-5; the pool and
//!   the tree above it never touch `std::fs` directly;
//! - keys and values are uninterpreted byte strings ordered by `memcmp`
//!   (PLAN-v3 §3.3: "keys are memcomparable bytes").
//!
//! Module map: [`store`] (page ids, errors, [`store::PageStore`],
//! [`store::MemStore`], [`store::FileStore`]), [`pool`] (the N8 buffer
//! manager: frames, pin counts, clock eviction), [`page`] (the 8 KiB slotted
//! page format), [`tree`] (the B+tree with latch coupling and cursor-stack
//! range scans).

pub mod page;
pub mod pool;
pub mod store;
pub mod tree;

pub use pool::BufferPool;
pub use store::{FileStore, MemStore, PageId, PageStore, StoreError};
pub use tree::{BTree, TreeStats};
