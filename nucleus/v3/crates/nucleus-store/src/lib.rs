//! Storage engine for Nucleus v3 (PLAN-v3 §3.3).
//!
//! One page file plus a `wal/` segment directory: pages addressed by logical
//! page id through a persisted page map, a buffer manager, B+trees, a logical
//! redo-only WAL, copy-on-write checkpoints and recovery. Normative design:
//! `docs/S-STORE.md` (card S-1). Implementation: Phase A cards.
//!
//! # Seed status (card A-0)
//!
//! The modules below are the E0 spike's page, pool and tree, copied with
//! provenance from `spikes/e0-btree` (PLAN-v3 §9 A-0). They meet E0's
//! invariants (T-ORDER, T-COUPLING, T-ATOMIC, P-PARSE, P-LAYOUT, P-ORDER,
//! P-SPLIT, each stated in its module), not yet S-STORE. The known
//! departures, each with the card that removes it:
//!
//! - page format: a 16-byte header with kinds 0/1 and no CRC, id or epoch
//!   (S-STORE §3.1, §3.3), and limits sized for two maximum cells, not four
//!   (§3.2): A-2;
//! - ids: [`store::PageStore`] hands out dense page ids that are never freed
//!   and are addressed directly, with no page map, free set or slot reuse
//!   (§4): A-2, A-3c;
//! - pool: `std::sync::RwLock` frame latches, which are not task-fair
//!   (§5.1, §5.3); victims picked by pin count, not `try_latch` (§5.2);
//!   dirty victims written back in place under the same id, not to a fresh
//!   slot (§5.2, I-NO-INPLACE); no epoch gate or `write_guard` (§5.6):
//!   A-3a, A-3b, A-3c, A-7a;
//! - tree: the root id in a `RwLock<PageId>` with the T-ROOT restart, not a
//!   stable root (§6.3, I-ROOT-STABLE); the split separator is the right
//!   page's first key, not the shortest separator (§6.4); keys are plain
//!   user keys, not versioned entries (§7): A-4a; no pack or page free,
//!   T-NOMERGE (§6.5): A-4b; the range iterator latches its whole cursor
//!   stack for its lifetime, not parked cursors (§6.6): A-4e; no prefix
//!   truncation: A-4d; no overflow chains (§6.7): A-5;
//! - no WAL, checkpoint or recovery (§12–§14): A-6a onward.
#![deny(clippy::indexing_slicing)]

// Provenance (card A-0): the module list and re-exports are
// nucleus/v3/spikes/e0-btree/src/lib.rs at 7ccd1cff8ef926f95a240b15d4b9c6a346251efe
// lines 27-34, without the spike's file-backed store, which is not copied.

pub mod page;
pub mod pool;
pub mod store;
pub mod tree;

pub use pool::BufferPool;
pub use store::{MemStore, PageId, PageStore, StoreError};
pub use tree::{BTree, TreeStats};
