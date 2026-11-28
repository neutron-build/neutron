//! Storage engine for Nucleus v3 (PLAN-v3 §3.3).
//!
//! One page file plus a `wal/` segment directory: pages addressed by logical
//! page id through a persisted page map, a buffer manager, B+trees, a logical
//! redo-only WAL, copy-on-write checkpoints and recovery. Normative design:
//! `docs/S-STORE.md` (card S-1). Implementation: Phase A cards.
//!
//! Invariants owned by this crate as a whole (the E0 spike's crate doc,
//! restated by card A-2):
//! - no `unsafe` (`unsafe_code = "forbid"`), and no panic on a path data can
//!   reach: every malformed byte sequence is `StoreError::Corrupt`, which is
//!   XX001 (S-STORE §3.1, §3.3; PLAN-v3 §6.1);
//! - no storage code calls `std::fs` or `std::time`: all I/O goes through
//!   `nucleus-env` (S-STORE §2; PLAN-v3 §3.2);
//! - B+tree keys are uninterpreted byte strings ordered by `memcmp`
//!   (S-STORE §6.1).
//!
//! # What is built
//!
//! - `crc`, `meta` and `open` (card A-2-0): CRC32C; page 0, the two meta
//!   copies, the replica region and the open rule (S-STORE §2), as codecs
//!   with no I/O.
//! - `page`'s header, `verify` and size limits, and `map` (card A-2): the
//!   page header (§3.1), I-INTEGRITY (§3.3), `K_MAX` and `V_INLINE` (§3.2),
//!   and the page-map pages and their positions (§4.1), as codecs with no
//!   I/O.
//! - `buf` (card A-3a): the buffer manager's frames, page table and pins
//!   (S-STORE §5.1, §5.4): a fixed pool, a page table that never grows,
//!   and loads through a page's current reference, checked by
//!   `page::verify` (§3.3). No tree uses it yet.
//! - `buf`'s write-back (card A-3c): the epoch gate (S-STORE §5.6), the
//!   current page map with its growth (§4.1), §4.1's MapLeaf pins, slot and
//!   LPID allocation with the two-generation pending frees (§4.2–§4.5,
//!   §13 step 6), and dirty eviction to fresh slots (§5.2), in pools built
//!   with `Pool::with_writeback`.
//! - `buf`'s frame acquisition and pool sizing (card A-3b-0): clock
//!   eviction of clean victims with `try_latch`, the wait for an unpin,
//!   and the minimum pool size at open with R-1-memory's values
//!   (S-STORE §5.2, §5.4).
//! - `buf`'s frame latch (card A-3b): A-4c0's task-fair `latch::Latch`
//!   with a bypass bound of 1 and a fixed waiter queue, beside a content
//!   cell, and `buf` on the store's sync layer (S-STORE §5.3, §15).
//! - `page`'s slotted body, `pool`, `store` and `tree` (card A-0, re-based
//!   on §3.1 by card A-2): the E0 spike's B+tree, copied with provenance
//!   from `spikes/e0-btree`. They meet E0's invariants (T-ORDER, T-COUPLING,
//!   T-ATOMIC, P-PARSE, P-LAYOUT, P-ORDER, P-SPLIT, each stated in its
//!   module), not yet S-STORE's.
//!
//! # Seed status (card A-0; completed by card A-2)
//!
//! The known departures from S-STORE, each with the card that removes it:
//!
//! - the seed tree's pages are neither sealed nor checked: it writes every
//!   page with `id` 0, `epoch` 0 and `crc` 0 through `pool`, which never
//!   calls `page::seal` or `page::verify` (§3.1, §3.3): A-4a moves the tree
//!   onto `buf`, whose every load is checked (card A-3a) and whose dirty
//!   eviction seals what it writes (card A-3c); A-7b (the checkpoint's image
//!   writes) seals its own;
//! - ids: the seed tree's [`store::PageStore`] hands out dense page ids
//!   that are addressed directly (§4.1–§4.5): A-4a moves the tree onto
//!   `buf`, which names a page by LPID or map position and loads it through
//!   its current reference (card A-3a); write-back pools hold the current
//!   page map (§4.1), LPID allocation (§4.2), the free set in memory (§4.3)
//!   and slot reuse (§4.4, §4.5) (card A-3c), persisted by A-7b; LPID
//!   freeing: A-4b; `pool`'s recycle list of unpublished
//!   reserved ids (card X-0-F2), which S-STORE does not have, goes with
//!   `pool`: A-4a;
//! - the file header is encoded and selected (`meta`, `open`) but never
//!   written or read: A-7c (step 4's meta write), A-7d (the open); map and
//!   free-set persistence: A-7b;
//! - pool: the seed tree runs on `pool`, whose victims are picked by pin
//!   count and whose dirty victims are written back in place under the same
//!   id (§5.2, I-NO-INPLACE): A-4a moves the tree onto `buf` and retires
//!   `pool`. `buf` (card A-3c): in a pool built with `Pool::new`, and in a
//!   write-back pool for a dirty set from before the last flip, dirty
//!   victims are not evicted, so a pool whose unpinned frames are all dirty
//!   answers `PoolStarved` (§5.2): A-3c-C;
//!   no current page map behind its `CurrentRef` in a pool built with
//!   `Pool::new` (card A-3c builds one for `Pool::with_writeback`); no poison
//!   check at a pin or in its load wait (§9.2): A-3c-B; no epoch-copy
//!   reserve, image-record list or step 2, and no eviction of a dirty victim
//!   from before the last flip (§5.2, §5.6, §13): A-3c-C; no `write_guard`
//!   (§5.6): A-7a;
//! - latch scope: `BufferPool`'s guards keep a latch past the call, and
//!   `fetch_*` reads the store while ancestors are latched (§5.3 "No latch
//!   escapes a call"; §5.6's restart rule): A-4a retires `pool`; `buf`
//!   leaves both to its callers (its blocking `pin` may read a page, so a
//!   caller under latches uses it only where §5.3 allows a read), which
//!   A-4c's tracker checks;
//! - fail-stop: no poison mark, and `StoreError` has no SQLSTATE mapping
//!   (XX000, XX001, 58030, 53100, 53200; §9.2): A-3c-B, A-4c;
//! - tree: the root id in a `RwLock<PageId>` with the T-ROOT restart, not a
//!   stable root (§6.3): A-4a; the split separator is the right page's first
//!   key, not the shortest separator, and no prune-before-split decision
//!   (§6.4): A-4a; keys are plain user keys, not versioned entries (§7):
//!   A-4a; no pack or page free, T-NOMERGE (§6.5): A-4b; the range iterator
//!   latches its whole cursor stack for its lifetime, not parked cursors
//!   (§6.6): A-4e; no prefix truncation: A-4d; no overflow chains, so a
//!   value longer than `V_INLINE` is refused (§6.7): A-5;
//! - no tree directory or root table (§6.8): A-7b;
//! - WAL: the record codec (§12.2, §12.3) is card A-6a's; segments, the
//!   writer and the walk (§12.1, §12.4–§12.6): A-6b, A-6c, A-6d; the
//!   checkpoint (§13): A-7a, A-7b, A-7c; recovery (§14): A-7d.
//!
//! Every later A-card that removes a departure deletes or narrows its line.
#![deny(clippy::indexing_slicing)]

// Provenance (card A-0): the module list and re-exports are
// nucleus/v3/spikes/e0-btree/src/lib.rs at 7ccd1cff8ef926f95a240b15d4b9c6a346251efe
// lines 27-34, without the spike's file-backed store, which is not copied.

pub mod buf;
mod codec;
pub mod crc;
pub mod map;
pub mod meta;
pub mod open;
pub mod page;
pub mod pool;
pub mod store;
pub mod tree;
pub mod wal;

pub use pool::BufferPool;
pub use store::{MemStore, PageId, PageStore, StoreError};
pub use tree::{BTree, TreeStats};

// Card A-4c0: the sync facade and the task-fair latch of S-STORE 5.3.
pub mod latch;
pub mod sync;
