#![deny(clippy::indexing_slicing)]
//! The E0 B+tree (card X-0 Work 5; PLAN-v3 §3.3 "B+tree").
//!
//! Keys and values are byte strings compared by `memcmp`. Range scans use a
//! cursor stack of shared-latched pages — **no leaf sibling links**, so a
//! future copy-on-write never has to fix siblings (PLAN-v3 §3.3).
//!
//! Invariants owned here:
//! - T-ORDER: within a leaf, keys are strictly ascending; within an inner
//!   page, separators are strictly ascending, and every key in the subtree
//!   below child *i* lies in `[sep(i-1), sep(i))` (child 0 is unbounded
//!   below, the right-most child is unbounded above).
//! - T-COUPLING: writers take exclusive latches top-down and release every
//!   ancestor once the latched child is *safe* — has room for one more
//!   worst-case cell (card X-0 Work 5). While any node below is unsafe,
//!   every ancestor stays latched, so a split always finds its parent's
//!   latch already held, in top-down order; the root page id lives in a
//!   `RwLock<PageId>` for root splits. A split that arrives at a non-root
//!   node with no ancestor still latched is therefore proof the coupling
//!   discipline was broken somewhere, and returns `Corrupt` — card X-0-F1
//!   deleted the `find_parent` re-derivation fallback that silently
//!   repaired (and so masked) that state, since in correct code it is
//!   unreachable dead code (PLAN-v3 §6.1 "no dead code").
//! - T-ROOT: readers re-check the root id after latching the root and
//!   restart if a root split raced them; a page latched as a stale root is
//!   still a consistent half-tree, but restarting keeps single-key
//!   read-your-writes for the owning thread.
//! - T-NOMERGE: delete removes a cell in place; under-full and empty pages
//!   remain (no merge in the spike, card X-0 Work 5). Pages are never
//!   freed *from the tree*, so every page id the tree has published stays
//!   valid forever — which is what makes the cursor stack and stale-root
//!   reads safe. (Pre-allocated pages a cascade did not consume are
//!   recycled by the pool *before* ever becoming reachable — card X-0-F2
//!   — so recycling cannot invalidate a published id.)
//! - T-ATOMIC (card X-0-F2): an insert that returns an error other than
//!   `Corrupt` has changed nothing visible — the old value of an upsert
//!   stays readable, and every page is as it was. This holds because
//!   whether the insert splits at all is decided first, on an owned copy
//!   of the leaf running the exact page routines the real path will run
//!   (the gate rework's fit test); only for an insert that provably
//!   splits are the pages the cascade could need pre-allocated, before
//!   the leaf's first mutation (Work 1). An upsert removes its old cell
//!   only once those pages are held or the replacement is known to fit
//!   (Work 2), and a page is marked dirty before its first mutation
//!   (Work 3); from the reservation on, only `Corrupt` can be returned
//!   (`Poisoned`, the one other error reachable that late, presupposes
//!   another thread already panicked inside a lock and is fail-stop like
//!   `Corrupt`). `delete` mutates only the leaf it
//!   descends to and keeps no fallible call between that mutation and
//!   `mark_dirty`. Unused reserved pages are recycled by the pool
//!   (`BufferPool::release_new`), so a reservation that comes to nothing
//!   does not grow the store.
//!
//! Known spike limits, on purpose: no merge, no overflow pages (values are
//! capped at `page::MAX_VALUE`), no prefix truncation, no WAL.

use std::collections::HashSet;
use std::sync::RwLock;

use crate::page::{self, PageKind};
use crate::pool::{BufferPool, ReadGuard, WriteGuard};
use crate::store::{PageId, PageStore, StoreError};

/// Root re-checks before giving up (a root split must land within a few
/// latch hand-offs; more than this means something is very wrong).
const ROOT_RETRIES: u32 = 64;

/// A B+tree over any [`PageStore`], addressed through a
/// [`BufferPool`].
pub struct BTree<S: PageStore> {
    pool: BufferPool<S>,
    root: RwLock<PageId>,
}

/// Aggregate tree shape returned by [`BTree::check`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TreeStats {
    /// Height in pages (a lone root leaf has height 1).
    pub height: usize,
    /// Number of reachable pages.
    pub pages: usize,
    /// Occupied bytes (slots and cells) divided by the usable page bytes
    /// across all pages; holes left by deletes count as unoccupied.
    pub fill_factor: f64,
}

/// One entry of the `check` DFS stack: a page to validate, the key
/// bounds `[lo, hi)` its whole subtree must respect — inherited
/// unbroken from the separators above it, and open only at the root
/// (T-ORDER, card X-0-F3) — and its depth.
struct CheckEntry {
    id: PageId,
    lo: Option<Vec<u8>>,
    hi: Option<Vec<u8>>,
    depth: usize,
}

impl<S: PageStore> BTree<S> {
    /// Create a tree with a fresh empty-leaf root.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] from the pool if the capacity or page size
    /// is out of range.
    pub fn new(store: S, capacity: usize) -> Result<Self, StoreError> {
        let pool = BufferPool::new(store, capacity)?;
        let (id, mut guard) = pool.new_page()?;
        page::init(&mut guard, PageKind::Leaf)?;
        guard.mark_dirty();
        drop(guard);
        Ok(Self {
            pool,
            root: RwLock::new(id),
        })
    }

    /// Open a tree over an existing store at a known root page (the
    /// `file.rs` reopen path; a real meta slot is S-1's work, not this
    /// spike's).
    ///
    /// # Errors
    /// Any pool error, or [`StoreError::Corrupt`] if the root page does
    /// not parse.
    pub fn open(store: S, capacity: usize, root: PageId) -> Result<Self, StoreError> {
        let pool = BufferPool::new(store, capacity)?;
        let guard = pool.fetch_read(root)?;
        page::header(&guard)?;
        drop(guard);
        Ok(Self {
            pool,
            root: RwLock::new(root),
        })
    }

    /// The current root page id (used by tests to reopen a file store).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the root lock was poisoned.
    pub fn root_page(&self) -> Result<PageId, StoreError> {
        self.root_id()
    }

    /// Write every dirty resident page back (see [`BufferPool::flush_all`]).
    ///
    /// # Errors
    /// Any store error from the write-back.
    pub fn flush_all(&self) -> Result<(), StoreError> {
        self.pool.flush_all()
    }

    /// Consume the tree and hand back the store.
    pub fn into_store(self) -> S {
        self.pool.into_store()
    }

    fn root_id(&self) -> Result<PageId, StoreError> {
        self.root
            .read()
            .map(|g| *g)
            .map_err(|e| StoreError::Poisoned(e.to_string()))
    }

    /// Point lookup.
    ///
    /// # Errors
    /// Any pool/store error, or [`StoreError::Corrupt`] for structurally
    /// bad pages.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        let mut tries = 0u32;
        loop {
            let rid = self.root_id()?;
            let mut guard = self.pool.fetch_read(rid)?;
            if rid != self.root_id()? {
                Self::bump_root_tries(&mut tries)?;
                continue;
            }
            // Readers couple shared latches top-down and release the parent
            // once the child is latched (card X-0 Work 5).
            loop {
                match page::kind(&guard)? {
                    PageKind::Leaf => {
                        return Ok(match page::leaf_find(&guard, key)? {
                            Ok(i) => Some(page::leaf_cell(&guard, i)?.1.to_vec()),
                            Err(_) => None,
                        });
                    }
                    PageKind::Inner => {
                        let child = page::inner_route(&guard, key)?.1;
                        let next = self.pool.fetch_read(child)?;
                        drop(guard);
                        guard = next;
                    }
                }
            }
        }
    }

    /// Upsert: stores `value` under `key` and returns the previous value.
    ///
    /// Atomic (T-ATOMIC, card X-0-F2): an error other than `Corrupt`
    /// leaves the tree exactly as it was, so the old value stays
    /// readable after a failed upsert. `tests/faults.rs` pins this with
    /// a fault-injecting store failing every `allocate`, `read` and
    /// `write` call in turn.
    ///
    /// # Errors
    /// [`StoreError::KeyTooLarge`] / [`StoreError::ValueTooLarge`] for
    /// oversized inputs; any pool/store error otherwise.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        self.insert_descend(key, value, false)
    }

    /// Test-only chaos hook (card X-0-F1; card X-0-F1-F1 gates it behind
    /// the non-default `test-hooks` feature): [`Self::insert`], except the
    /// descent treats **every** child as safe and releases each ancestor's
    /// latch as soon as the child is latched — the exact shape of the
    /// G-SAFE-LAX and COUPLING-EARLY-RELEASE mutants of the card's
    /// evidence list. A split of a non-root leaf then reaches
    /// `propagate_split` with an empty retained stack, which must
    /// surface as [`StoreError::Corrupt`] now that the `find_parent`
    /// fallback is gone (before X-0-F1 the fallback silently repaired this
    /// state and masked those mutants). Never call outside tests; the tree
    /// is left damaged when this returns the error. The `test-hooks`
    /// feature never reaches crates outside this workspace, nor any normal
    /// (non-test) build — this crate's own tests get it via the self
    /// dev-dependency in `Cargo.toml`. Under `cargo test --workspace`,
    /// however, that feature is unified into the e0-btree copy that other
    /// workspace crates' tests link, so a workspace crate's tests could
    /// call this hook; no workspace crate other than e0-btree may
    /// reference `insert_early_release`.
    ///
    /// # Errors
    /// Same as [`Self::insert`]; plus [`StoreError::Corrupt`] naming the
    /// broken coupling invariant when a split loses its parent.
    #[cfg(feature = "test-hooks")]
    pub fn insert_early_release(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        self.insert_descend(key, value, true)
    }

    /// The shared descent of [`Self::insert`] and its test chaos hook:
    /// with `early_release` set, the [`page::is_safe`] test is skipped and
    /// every child is reported safe (see `insert_early_release`
    /// (test-hooks only)).
    ///
    /// # Errors
    /// [`StoreError::KeyTooLarge`] / [`StoreError::ValueTooLarge`] for
    /// oversized inputs; any pool/store error otherwise.
    fn insert_descend(
        &self,
        key: &[u8],
        value: &[u8],
        early_release: bool,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        if key.len() > page::MAX_KEY {
            return Err(StoreError::KeyTooLarge(key.len()));
        }
        if value.len() > page::MAX_VALUE {
            return Err(StoreError::ValueTooLarge(value.len()));
        }
        let mut tries = 0u32;
        loop {
            let rid = self.root_id()?;
            let mut guard = self.pool.fetch_write(rid)?;
            if rid != self.root_id()? {
                Self::bump_root_tries(&mut tries)?;
                continue;
            }
            let mut path: Vec<PageId> = Vec::new();
            let mut guards: Vec<WriteGuard<'_, S>> = Vec::new();
            let mut nid = rid;
            loop {
                match page::kind(&guard)? {
                    PageKind::Inner => {
                        let child_id = page::inner_route(&guard, key)?.1;
                        let child = self.pool.fetch_write(child_id)?;
                        // Latch coupling (T-COUPLING): the child's safety
                        // decides whether the ancestors may be released. A
                        // safe child cannot split, so nothing can propagate
                        // above it.
                        let safe = early_release || page::is_safe(&child)?;
                        let parent = guard;
                        if safe {
                            for g in guards.drain(..) {
                                drop(g);
                            }
                            path.clear();
                            drop(parent);
                        } else {
                            guards.push(parent);
                            path.push(nid);
                        }
                        guard = child;
                        nid = child_id;
                    }
                    PageKind::Leaf => {
                        return self.leaf_insert_step(
                            guard,
                            nid,
                            &mut path,
                            &mut guards,
                            key,
                            value,
                        );
                    }
                }
            }
        }
    }

    /// Delete `key`, returning the value it had.
    ///
    /// # Errors
    /// [`StoreError::KeyTooLarge`] for oversized keys; any pool/store
    /// error otherwise.
    pub fn delete(&self, key: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
        if key.len() > page::MAX_KEY {
            return Err(StoreError::KeyTooLarge(key.len()));
        }
        let mut tries = 0u32;
        loop {
            let rid = self.root_id()?;
            let mut guard = self.pool.fetch_write(rid)?;
            if rid != self.root_id()? {
                Self::bump_root_tries(&mut tries)?;
                continue;
            }
            // Delete never changes anything above the leaf (T-NOMERGE), so
            // each parent is released as soon as its child is latched.
            loop {
                match page::kind(&guard)? {
                    PageKind::Leaf => {
                        return Ok(match page::leaf_find(&guard, key)? {
                            Ok(i) => {
                                let old = page::leaf_cell(&guard, i)?.1.to_vec();
                                page::leaf_remove(&mut guard, i)?;
                                guard.mark_dirty();
                                Some(old)
                            }
                            Err(_) => None,
                        });
                    }
                    PageKind::Inner => {
                        let child = page::inner_route(&guard, key)?.1;
                        let next = self.pool.fetch_write(child)?;
                        drop(guard);
                        guard = next;
                    }
                }
            }
        }
    }

    /// Iterate keys in `[lo, hi)` in ascending order (lo inclusive, hi
    /// exclusive). The iterator holds shared latches on the whole cursor
    /// stack for its lifetime: the stack can then never go stale mid-scan
    /// (which is the point of not having sibling links, PLAN-v3 §3.3), at
    /// the cost of delaying writers on that path. Do not mutate the tree
    /// while holding the iterator in the same thread.
    ///
    /// # Errors
    /// Any pool/store error during positioning.
    pub fn range(&self, lo: &[u8], hi: &[u8]) -> Result<RangeIter<'_, S>, StoreError> {
        let mut tries = 0u32;
        loop {
            let rid = self.root_id()?;
            let guard = self.pool.fetch_read(rid)?;
            if rid != self.root_id()? {
                Self::bump_root_tries(&mut tries)?;
                continue;
            }
            let mut stack = Vec::new();
            let mut cur = guard;
            loop {
                match page::kind(&cur)? {
                    PageKind::Leaf => {
                        let start = match page::leaf_find(&cur, lo)? {
                            Ok(i) => i,
                            Err(p) => p,
                        };
                        stack.push(CursorEntry {
                            guard: cur,
                            idx: start,
                        });
                        return Ok(RangeIter {
                            tree: self,
                            stack,
                            hi: hi.to_vec(),
                            done: false,
                        });
                    }
                    PageKind::Inner => {
                        let (ci, child) = page::inner_route(&cur, lo)?;
                        let next = self.pool.fetch_read(child)?;
                        stack.push(CursorEntry {
                            guard: cur,
                            idx: ci,
                        });
                        cur = next;
                    }
                }
            }
        }
    }

    /// Validate the whole tree and return its shape, enforcing T-ORDER
    /// end to end (tightened by card X-0-F3, X-0 verdict finding 3):
    ///
    /// * keys strictly ascending within each leaf, and every key inside
    ///   the `[lo, hi)` bounds its page inherited — child 0 of an inner
    ///   page inherits the parent's `lo` (it is **not** unbounded
    ///   below), the right-most child inherits the parent's `hi`, and
    ///   the bounds start open at the root;
    /// * separators strictly ascending within each inner page, and every
    ///   separator inside the page's own inherited `[lo, hi)`;
    /// * every page reachable exactly once, no cell overlaps, and all
    ///   leaves at the same depth.
    ///
    /// Call it quiescently (tests do); under concurrency it may see a
    /// torn multi-page view even when both interleavings are legal.
    ///
    /// # Errors
    /// A human-readable description of the first violation found.
    pub fn check(&self) -> Result<TreeStats, String> {
        let ps = self.pool.page_size();
        let root = self.root_id().map_err(|e| e.to_string())?;
        let mut visited: HashSet<PageId> = HashSet::new();
        // (page id, low bound, high bound, depth); bounds are exclusive of
        // `hi` and inclusive of `lo` (T-ORDER).
        let mut stack: Vec<CheckEntry> = vec![CheckEntry {
            id: root,
            lo: None,
            hi: None,
            depth: 1,
        }];
        let mut height = 0usize;
        let mut occupied = 0usize;
        let mut leaf_depth: Option<usize> = None;
        while let Some(CheckEntry { id, lo, hi, depth }) = stack.pop() {
            if !visited.insert(id) {
                return Err(format!("page {id:?} is reachable more than once"));
            }
            let g = self
                .pool
                .fetch_read(id)
                .map_err(|e| format!("fetch {id:?}: {e}"))?;
            let h = page::header(&g).map_err(|e| format!("page {id:?}: {e}"))?;
            if h.nslots > 0 && page::slot(&g, 0).is_err() {
                return Err(format!("page {id:?}: first slot invalid"));
            }
            // no cell overlaps; cells live in [free_end, ps)
            let mut spans: Vec<(usize, usize)> = Vec::with_capacity(h.nslots);
            for i in 0..h.nslots {
                let s = page::slot(&g, i).map_err(|e| format!("page {id:?}: {e}"))?;
                spans.push(s);
                occupied += page::SLOT + s.1;
            }
            spans.sort_unstable();
            let mut prev_end = h.free_end;
            for (off, len) in spans.iter() {
                if *off < prev_end || off + len > ps {
                    return Err(format!(
                        "page {id:?}: cell at {off}+{len} overlaps (previous end {prev_end})"
                    ));
                }
                prev_end = off + len;
            }
            match h.kind {
                PageKind::Leaf => {
                    let mut prev: Option<Vec<u8>> = None;
                    for i in 0..h.nslots {
                        let (k, _) =
                            page::leaf_cell(&g, i).map_err(|e| format!("page {id:?}: {e}"))?;
                        if let Some(p) = prev.as_deref()
                            && p >= k
                        {
                            return Err(format!("page {id:?}: keys out of order at slot {i}"));
                        }
                        if let Some(l) = lo.as_deref()
                            && k < l
                        {
                            return Err(format!("page {id:?}: key below the subtree bound"));
                        }
                        if let Some(hb) = hi.as_deref()
                            && k >= hb
                        {
                            return Err(format!("page {id:?}: key above the subtree bound"));
                        }
                        prev = Some(k.to_vec());
                    }
                    match leaf_depth {
                        None => leaf_depth = Some(depth),
                        Some(d) if d != depth => {
                            return Err(format!("leaves at different depths: {d} and {depth}"));
                        }
                        _ => {}
                    }
                    height = height.max(depth);
                }
                PageKind::Inner => {
                    if h.nslots == 0 {
                        return Err(format!("inner page {id:?} has no cells"));
                    }
                    let mut prev_sep: Option<Vec<u8>> = None;
                    for i in 0..h.nslots {
                        let (child, sep) =
                            page::inner_cell(&g, i).map_err(|e| format!("page {id:?}: {e}"))?;
                        if let Some(p) = prev_sep.as_deref()
                            && p >= sep
                        {
                            return Err(format!(
                                "page {id:?}: separators out of order at cell {i}"
                            ));
                        }
                        // T-ORDER, tightened by card X-0-F3 (X-0 verdict
                        // finding 3): a separator must also sit inside the
                        // bounds this page inherited from its own parent.
                        // One outside `[lo, hi)` misroutes every lookup
                        // that passes through the page, and the child
                        // bounds derived from it would hide the damage a
                        // level further down.
                        if let Some(l) = lo.as_deref()
                            && sep < l
                        {
                            return Err(format!(
                                "page {id:?}: separator at cell {i} below the subtree bound"
                            ));
                        }
                        if let Some(hb) = hi.as_deref()
                            && sep >= hb
                        {
                            return Err(format!(
                                "page {id:?}: separator at cell {i} above the subtree bound"
                            ));
                        }
                        // Child 0 has no separator to its left, so it
                        // inherits the page's own lower bound (card X-0-F3
                        // Work 1; before, it started at `None` and a key
                        // below every ancestor separator passed). Later
                        // children take the previous separator, whose
                        // standing against `lo` the check above just
                        // established.
                        let child_lo = prev_sep.clone().or_else(|| lo.clone());
                        let child_hi = Some(sep.to_vec());
                        stack.push(CheckEntry {
                            id: child,
                            lo: child_lo,
                            hi: child_hi,
                            depth: depth + 1,
                        });
                        prev_sep = Some(sep.to_vec());
                    }
                    // The right-most child keeps the last separator as its
                    // lower bound: the checks above already proved that
                    // separator `>= lo`, so taking the max with `lo` would
                    // add nothing (card X-0-F3 Work 1).
                    stack.push(CheckEntry {
                        id: h.right_child,
                        lo: prev_sep,
                        hi: hi.clone(),
                        depth: depth + 1,
                    });
                    height = height.max(depth);
                }
            }
        }
        let usable = ps - page::HEADER;
        let pages = visited.len();
        let fill_factor = occupied as f64 / (pages.max(1) as f64 * usable as f64);
        Ok(TreeStats {
            height,
            pages,
            fill_factor,
        })
    }

    fn bump_root_tries(tries: &mut u32) -> Result<(), StoreError> {
        *tries += 1;
        if *tries >= ROOT_RETRIES {
            return Err(StoreError::Corrupt(format!(
                "the root changed under us {ROOT_RETRIES} times"
            )));
        }
        Ok(())
    }

    /// The leaf step of [`Self::insert`]: upsert in place, or split the
    /// leaf with the pending cell and repair the ancestors. Card X-0-F2
    /// makes the step atomic (T-ATOMIC): whether the insert splits is
    /// decided first on an owned copy of the leaf (the gate rework's fit
    /// test), only then are the pages the cascade could need reserved,
    /// and from the reservation on only `Corrupt` (or a poisoned lock)
    /// can be returned.
    #[allow(clippy::type_complexity)]
    fn leaf_insert_step<'s>(
        &'s self,
        mut leaf: WriteGuard<'s, S>,
        leaf_id: PageId,
        path: &mut Vec<PageId>,
        guards: &mut Vec<WriteGuard<'s, S>>,
        key: &[u8],
        value: &[u8],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        // Work 1 (card X-0-F2) + the gate rework: reserve only when this
        // insert really splits. A safe leaf fits any worst-case cell, so
        // the owned-copy fit test is skipped for it; an unsafe leaf gets
        // the exact test, so the reservation — and the eviction churn it
        // costs — happens on genuine splits, not on every insert into a
        // half-full leaf that merely fails `is_safe`.
        let splits = if page::is_safe(&leaf)? {
            false
        } else {
            Self::insert_splits(&leaf, key, value)?
        };
        let mut reserve = if splits {
            self.reserve_split_pages(leaf_id, path)?
        } else {
            SplitReserve::empty()
        };
        // Work 3: every path below mutates the leaf — the in-place
        // insert, the upsert's remove, or the split's rewrite — so mark
        // it dirty before the first of those, and no error return can
        // leave a modified page clean.
        leaf.mark_dirty();
        match page::leaf_find(&leaf, key)? {
            Ok(i) => {
                let old = page::leaf_cell(&leaf, i)?.1.to_vec();
                // Work 2: the replacement may not fit once values grow,
                // and removing the old cell first is safe only because
                // the copy above already decided the question — either
                // the replacement fits (`splits == false`: the insert
                // below cannot be refused) or the split's pages are
                // reserved (`splits == true`). (Before card X-0-F2 the
                // remove came first with nothing held, and a failed
                // `new_page` lost the key.)
                page::leaf_remove(&mut leaf, i)?;
                if !splits {
                    if !page::leaf_insert(&mut leaf, i, key, value)? {
                        return Err(StoreError::Corrupt(
                            "in-place upsert refused after the copy accepted it".into(),
                        ));
                    }
                    reserve.release(&self.pool);
                    return Ok(Some(old));
                }
                let out = self.split_and_propagate(
                    leaf,
                    leaf_id,
                    path,
                    guards,
                    key,
                    value,
                    Some(old),
                    &mut reserve,
                );
                reserve.release(&self.pool);
                out
            }
            Err(pos) => {
                if !splits {
                    if !page::leaf_insert(&mut leaf, pos, key, value)? {
                        return Err(StoreError::Corrupt(
                            "in-place insert refused after the copy accepted it".into(),
                        ));
                    }
                    reserve.release(&self.pool);
                    return Ok(None);
                }
                let out = self.split_and_propagate(
                    leaf,
                    leaf_id,
                    path,
                    guards,
                    key,
                    value,
                    None,
                    &mut reserve,
                );
                reserve.release(&self.pool);
                out
            }
        }
    }

    /// Does `(key, value)` fit in `leaf` without a split? (Card X-0-F2
    /// gate rework, option (b): reserve only when the insert will really
    /// split.)
    ///
    /// The answer is computed on an **owned copy** of the leaf by
    /// running the exact page routines the real path will run — the
    /// upsert's `leaf_remove` first, then `leaf_insert` — so the fit
    /// decision cannot drift from the insert logic it gates: the real
    /// insert is refused only if this copy's insert was refused, and a
    /// refusal is exactly what makes the caller reserve and split. The
    /// copy is invisible to the tree: nothing visible changes, whatever
    /// it answers.
    ///
    /// # Errors
    /// [`StoreError::Corrupt`] if the leaf does not parse — the same
    /// error the real path would return for the same bytes, and that
    /// before any visible mutation.
    fn insert_splits(
        leaf: &WriteGuard<'_, S>,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool, StoreError> {
        let mut scratch = vec![0u8; leaf.len()];
        scratch.copy_from_slice(leaf);
        match page::leaf_find(&scratch, key)? {
            Ok(i) => {
                page::leaf_remove(&mut scratch, i)?;
                Ok(!page::leaf_insert(&mut scratch, i, key, value)?)
            }
            Err(pos) => Ok(!page::leaf_insert(&mut scratch, pos, key, value)?),
        }
    }

    /// Card X-0-F2 Work 1, option (a) — **pre-allocation**: acquire,
    /// before the leaf's first mutation, every page the split cascade
    /// could need in the worst case:
    ///
    /// * one page for the leaf's new right half;
    /// * one page per retained ancestor in `path` (each may split and
    ///   need a right half of its own);
    /// * one page for a new root, if the cascade can reach the root —
    ///   which it can exactly when the root is a retained ancestor, or
    ///   the leaf itself is the root. A released ancestor below a safe
    ///   child can never be reached again (T-COUPLING), so no root page
    ///   is reserved for it.
    ///
    /// Called only for an insert that provably splits — the caller's
    /// owned-copy fit test (gate rework on card X-0-F2, option (b))
    /// decided that — so the reservation's cost in pinned frames and
    /// evicted pages is paid on real splits, not on every insert into a
    /// leaf that merely fails [`page::is_safe`]. Pages the cascade does
    /// not consume are released and their ids recycled by the pool
    /// ([`BufferPool::release_new`]), so reservations that come to
    /// nothing do not grow the store.
    ///
    /// Reserving a frame per possible level raises the minimum pool
    /// size (card X-0-F2): a fully cascading insert pins its whole
    /// descent path (`h` frames on a tree of height `h`) plus the
    /// reservation (`h + 1` at worst), so a single writer needs at
    /// least `2·h + 1` frames — PLAN-v3 §3.3's
    /// `threads × (tree height + 2)` becomes
    /// `threads × (2·height + 1)` for inserts that may split.
    ///
    /// # Errors
    /// [`StoreError::PoolStarved`], or any store error from
    /// [`BufferPool::new_page`] — with the partial reservation already
    /// released, so the pool and the tree are exactly as they were
    /// (T-ATOMIC).
    fn reserve_split_pages<'s>(
        &'s self,
        leaf_id: PageId,
        path: &[PageId],
    ) -> Result<SplitReserve<'s, S>, StoreError> {
        let rid = self.root_id()?;
        let root_in_path = path.first() == Some(&rid);
        let leaf_is_root = leaf_id == rid;
        let count = 1 + path.len() + usize::from(root_in_path || leaf_is_root);
        SplitReserve::acquire(&self.pool, count)
    }

    /// Split the full leaf (with the pending cell) and install the new
    /// right page in the leaf's parent, splitting upward as needed.
    ///
    /// The right page comes from `reserve` (card X-0-F2 Work 1): by the
    /// time the leaf is rewritten, every page the whole cascade could
    /// need is already held, so this step cannot fail with an
    /// allocation error partway through.
    #[allow(clippy::type_complexity, clippy::too_many_arguments)]
    fn split_and_propagate<'s>(
        &'s self,
        mut leaf: WriteGuard<'s, S>,
        leaf_id: PageId,
        path: &mut Vec<PageId>,
        guards: &mut Vec<WriteGuard<'s, S>>,
        key: &[u8],
        value: &[u8],
        old: Option<Vec<u8>>,
        reserve: &mut SplitReserve<'s, S>,
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let (right_id, mut right) = reserve.take()?;
        let sep = page::leaf_split(&mut leaf, &mut right, (key, value))?;
        // The leaf was marked dirty before its first mutation (Work 3);
        // the fresh right page is installed dirty by `new_page`.
        drop(right);
        self.propagate_split(leaf_id, right_id, sep, Some(leaf), path, guards, reserve)?;
        Ok(old)
    }

    /// Insert `(right_id, sep)` as the sibling pointer of `left_id` in its
    /// parent, cascading splits toward the root.
    ///
    /// **Reservation strategy (card X-0-F2 Work 1, option (a)):
    /// pre-allocation.** Every page this cascade could need — the new
    /// halves at each level and a new root if the cascade reaches the
    /// root — was acquired before the leaf's first mutation and arrives
    /// in `reserve`; nothing below allocates, so no pool or store error
    /// can strike after a page has been mutated, and an insert that
    /// returns a non-`Corrupt` error has changed nothing visible
    /// (T-ATOMIC). The reservation was taken only because the caller's
    /// owned-copy fit test proved this insert splits (gate rework,
    /// option (b)); unused reserved pages are released and their ids
    /// recycled by the pool afterwards. The cost is pin occupancy
    /// rather than an error window: a fully cascading insert pins up to
    /// `2·h + 1` frames on a tree of height `h`, which is the new
    /// minimum pool size for one writer — see
    /// [`Self::reserve_split_pages`].
    ///
    /// By T-COUPLING every ancestor of an unsafe node is still latched in
    /// `guards`, in top-down acquisition order, so the parent latch is
    /// always already held — except for the root, which is split under the
    /// `root` lock. A non-root node that arrives here with no ancestor
    /// latched is proof the coupling discipline was broken (the
    /// COUPLING-EARLY-RELEASE and G-SAFE-LAX mutation shapes) and returns
    /// [`StoreError::Corrupt`]: card X-0-F1 deleted the `find_parent`
    /// fallback that re-derived the parent by a fresh descent, because in
    /// correct code that path is unreachable dead code (PLAN-v3 §6.1) and
    /// in incorrect code it silently repaired — and so masked — the bug: a
    /// racing writer makes a re-derived parent stale, and the resulting
    /// misrouting is exactly what the model and concurrency tests must be
    /// able to catch. The child latch is always released before any upward
    /// latch is taken: latches are ordered top-down only, and holding a
    /// child while blocking on an ancestor can deadlock.
    #[allow(clippy::too_many_arguments)]
    fn propagate_split<'s>(
        &'s self,
        mut left_id: PageId,
        mut right_id: PageId,
        mut sep: Vec<u8>,
        mut child_guard: Option<WriteGuard<'s, S>>,
        path: &mut Vec<PageId>,
        guards: &mut Vec<WriteGuard<'s, S>>,
        reserve: &mut SplitReserve<'s, S>,
    ) -> Result<(), StoreError> {
        loop {
            let parent = match (guards.pop(), path.pop()) {
                (Some(g), Some(pid)) => Some((pid, g)),
                (None, None) => {
                    if left_id == self.root_id()? {
                        None
                    } else {
                        // T-COUPLING broken: the separator descent must
                        // always find this child's parent latched in
                        // `guards`; no correct execution reaches a
                        // non-root split without it (card X-0-F1).
                        return Err(StoreError::Corrupt(
                            "find_parent fallback: separator descent lost \
                             the child — tree is inconsistent"
                                .into(),
                        ));
                    }
                }
                (g, id) => {
                    let had_guard = g.is_some();
                    let had_id = id.is_some();
                    return Err(StoreError::Corrupt(format!(
                        "descent path desync (guard={had_guard} id={had_id})"
                    )));
                }
            };
            let Some((pid, mut pg)) = parent else {
                // Root split: a new root over (left_id, right_id), from
                // the reservation (card X-0-F2 Work 1) — the allocation
                // that used to happen here, after the child split, is
                // what could fail and strand the halves.
                let (new_id, mut new_root) = reserve.take()?;
                page::init(&mut new_root, PageKind::Inner)?;
                if !page::inner_insert(&mut new_root, 0, left_id, &sep)? {
                    return Err(StoreError::Corrupt(
                        "a fresh root refused its first cell".into(),
                    ));
                }
                page::set_right_child(&mut new_root, right_id)?;
                // Fresh from `new_page`, so already dirty.
                drop(new_root);
                let mut w = self
                    .root
                    .write()
                    .map_err(|e| StoreError::Poisoned(e.to_string()))?;
                *w = new_id;
                drop(w);
                drop(child_guard);
                return Ok(());
            };
            // Work 3 (card X-0-F2): the parent is mutated on every path
            // from here — its right-most child is replaced or the left
            // half's bound is lowered, and a full parent is itself split
            // — so it is marked dirty before the first of those edits.
            pg.mark_dirty();
            // A split of child `left_id` into (left_id, right_id) at
            // separator `sep` narrows `left_id`'s key range to end at
            // `sep`. Under this format's routing — cell (child, s) means
            // "keys in [previous separator, s) go to child" — that means:
            //
            //  * if `left_id` was the parent's right-most child, it had no
            //    upper bound: it becomes an ordinary cell (left_id, sep)
            //    and `right_id` becomes the new right-most child;
            //  * otherwise `left_id`'s cell separator is lowered to `sep`
            //    and the new cell (right_id, old bound) is inserted right
            //    after it — the old upper bound now belongs to `right_id`,
            //    whose range is [sep, old bound).
            let left_is_rightmost = page::right_child(&pg)? == left_id;
            let (cell_child, cell_sep) = if left_is_rightmost {
                page::set_right_child(&mut pg, right_id)?;
                (left_id, sep.clone())
            } else {
                let Some(lpos) = page::inner_child_index(&pg, left_id)? else {
                    return Err(StoreError::Corrupt(format!(
                        "parent {pid:?} has no cell for child {left_id:?}"
                    )));
                };
                let (_, old_bound) = page::inner_cell(&pg, lpos)?;
                let old_bound = old_bound.to_vec();
                if !page::inner_replace_sep(&mut pg, lpos, &sep)? {
                    return Err(StoreError::Corrupt(
                        "could not lower the left half's bound".into(),
                    ));
                }
                (right_id, old_bound)
            };
            let pos = match page::inner_cell_position(&pg, &cell_sep)? {
                Err(p) => p,
                Ok(_) => {
                    return Err(StoreError::Corrupt(
                        "separator to insert already present in parent".into(),
                    ));
                }
            };
            if page::inner_insert(&mut pg, pos, cell_child, &cell_sep)? {
                drop(pg);
                drop(child_guard);
                return Ok(());
            }
            // The parent is full: split it with the pending cell and carry
            // the middle separator upward. The right half comes from the
            // reservation (card X-0-F2 Work 1) — this mid-cascade
            // allocation, after the bound was lowered and with the child
            // below already split, is what used to be able to fail.
            let (pr_id, mut pr) = reserve.take()?;
            let up = page::inner_split(&mut pg, &mut pr, (cell_child, &cell_sep))?;
            // Both halves are mutated here; `pg` was marked dirty above
            // and `pr` is fresh from `new_page`, so both are dirty.
            drop(pr);
            child_guard = Some(pg);
            left_id = pid;
            right_id = pr_id;
            sep = up;
        }
    }
}

/// The pages pre-allocated for one insert's split cascade (card X-0-F2
/// Work 1, option (a) — pre-allocation; see
/// [`BTree::reserve_split_pages`] for how many and what that does to the
/// minimum pool size).
///
/// Held from before the leaf's first mutation until the insert returns;
/// pages the cascade does not consume are released then, so they return
/// to the pool as clean empty victims **and their ids are recycled**
/// (gate rework on card X-0-F2): a reservation that came to nothing
/// leaves the store's page count where it was. A `Corrupt` return
/// bypasses the release — those guards then simply drop, leaving every
/// frame with its table entry and its bytes intact, and their ids stay
/// allocated but unreachable (never recycled: the tree is damaged
/// already and T-ATOMIC exempts `Corrupt`), which is acceptable because
/// `Corrupt` itself declares the tree unusable.
struct SplitReserve<'s, S: PageStore> {
    pages: Vec<(PageId, WriteGuard<'s, S>)>,
}

impl<'s, S: PageStore> SplitReserve<'s, S> {
    /// A reserve holding nothing — for a leaf that passed
    /// [`page::is_safe`] and so cannot split.
    fn empty() -> Self {
        Self { pages: Vec::new() }
    }

    /// Acquire `count` fresh pages, releasing everything acquired so far
    /// if any allocation fails, so a failed reservation changes nothing
    /// (T-ATOMIC, card X-0-F2 Work 1).
    ///
    /// # Errors
    /// [`StoreError::PoolStarved`] or any store error from
    /// [`BufferPool::new_page`], unchanged.
    fn acquire(pool: &'s BufferPool<S>, count: usize) -> Result<Self, StoreError> {
        let mut pages = Vec::with_capacity(count);
        for _ in 0..count {
            match pool.new_page() {
                Ok(page) => pages.push(page),
                Err(e) => {
                    for (_, guard) in pages.drain(..) {
                        // Best-effort: release only fails on a poisoned
                        // table mutex, and a dropped guard leaves the
                        // pool consistent (see the type doc).
                        let _ = pool.release_new(guard);
                    }
                    return Err(e);
                }
            }
        }
        Ok(Self { pages })
    }

    /// Take the next reserved page, in allocation order: the leaf's
    /// right half first, then the ancestors' halves bottom-up, the new
    /// root last. Cannot fail while the reservation count is right,
    /// which [`BTree::reserve_split_pages`] derives from the descent
    /// path — an exhausted reserve is an internal invariant break and
    /// returns [`StoreError::Corrupt`].
    fn take(&mut self) -> Result<(PageId, WriteGuard<'s, S>), StoreError> {
        if self.pages.is_empty() {
            return Err(StoreError::Corrupt(
                "split cascade needed more pages than were reserved".into(),
            ));
        }
        Ok(self.pages.remove(0))
    }

    /// Release the pages the cascade did not consume; the pool resets
    /// their frames and recycles their ids (gate rework on card X-0-F2),
    /// so an unused reservation leaves the store's page count unchanged.
    /// Best-effort on purpose: a poisoned table mutex is ignored (a
    /// dropped guard leaves the pool consistent, see the type doc),
    /// because failing an insert that already mutated pages would
    /// itself violate T-ATOMIC.
    fn release(&mut self, pool: &BufferPool<S>) {
        for (_, guard) in self.pages.drain(..) {
            let _ = pool.release_new(guard);
        }
    }
}

/// One page of the range cursor stack: the latched page and the child
/// index the cursor is currently inside (for a leaf: the next slot to
/// read).
struct CursorEntry<'a, S: PageStore> {
    guard: ReadGuard<'a, S>,
    idx: usize,
}

/// Ascending iterator over `[lo, hi)`; see [`BTree::range`]. Item errors
/// surface structural corruption per yield instead of aborting iteration
/// state.
pub struct RangeIter<'a, S: PageStore> {
    tree: &'a BTree<S>,
    stack: Vec<CursorEntry<'a, S>>,
    hi: Vec<u8>,
    done: bool,
}

impl<S: PageStore> Iterator for RangeIter<'_, S> {
    type Item = Result<(Vec<u8>, Vec<u8>), StoreError>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if self.done {
                return None;
            }
            // Yield from the leaf on top of the stack.
            let leaf_exhausted = {
                let top = match self.stack.last_mut() {
                    Some(t) => t,
                    None => {
                        self.done = true;
                        return None;
                    }
                };
                match page::header(&top.guard) {
                    Err(e) => return Some(Err(e)),
                    Ok(h) => {
                        if h.kind != PageKind::Leaf {
                            return Some(Err(StoreError::Corrupt(
                                "cursor stack top is not a leaf".into(),
                            )));
                        }
                        if top.idx < h.nslots {
                            let (k, v) = match page::leaf_cell(&top.guard, top.idx) {
                                Ok(kv) => kv,
                                Err(e) => return Some(Err(e)),
                            };
                            if k >= self.hi.as_slice() {
                                self.done = true;
                                return None;
                            }
                            top.idx += 1;
                            return Some(Ok((k.to_vec(), v.to_vec())));
                        }
                        true // exhausted
                    }
                }
            };
            if !leaf_exhausted {
                continue;
            }
            // Walk up to the nearest ancestor with a next child.
            self.stack.pop();
            loop {
                let next_child = {
                    let top = match self.stack.last_mut() {
                        Some(t) => t,
                        None => {
                            self.done = true;
                            return None;
                        }
                    };
                    let h = match page::header(&top.guard) {
                        Ok(h) => h,
                        Err(e) => return Some(Err(e)),
                    };
                    if h.kind != PageKind::Inner {
                        return Some(Err(StoreError::Corrupt(
                            "cursor stack inner entry is a leaf".into(),
                        )));
                    }
                    if top.idx < h.nslots {
                        top.idx += 1;
                        let child = if top.idx == h.nslots {
                            h.right_child
                        } else {
                            match page::inner_cell(&top.guard, top.idx) {
                                Ok((c, _)) => c,
                                Err(e) => return Some(Err(e)),
                            }
                        };
                        Some(child)
                    } else {
                        None // this ancestor is exhausted too
                    }
                };
                match next_child {
                    None => {
                        self.stack.pop();
                    }
                    Some(child) => {
                        // Descend to the left-most leaf under the next
                        // child; the first key of the next leaf is part of
                        // the scan (the RANGE-SKIPS-BOUNDARY mutation in
                        // the card's evidence list breaks exactly here).
                        let mut cur = match self.tree.pool.fetch_read(child) {
                            Ok(g) => g,
                            Err(e) => return Some(Err(e)),
                        };
                        loop {
                            match page::kind(&cur) {
                                Err(e) => return Some(Err(e)),
                                Ok(PageKind::Leaf) => {
                                    self.stack.push(CursorEntry { guard: cur, idx: 0 });
                                    break;
                                }
                                Ok(PageKind::Inner) => {
                                    let first = match page::inner_cell(&cur, 0) {
                                        Ok((c, _)) => c,
                                        Err(e) => return Some(Err(e)),
                                    };
                                    let next = match self.tree.pool.fetch_read(first) {
                                        Ok(g) => g,
                                        Err(e) => return Some(Err(e)),
                                    };
                                    self.stack.push(CursorEntry { guard: cur, idx: 0 });
                                    cur = next;
                                }
                            }
                        }
                        break;
                    }
                }
            }
        }
    }
}
