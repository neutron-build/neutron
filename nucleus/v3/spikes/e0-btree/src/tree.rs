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
//!   `RwLock<PageId>` for root splits.
//! - T-ROOT: readers re-check the root id after latching the root and
//!   restart if a root split raced them; a page latched as a stale root is
//!   still a consistent half-tree, but restarting keeps single-key
//!   read-your-writes for the owning thread.
//! - T-NOMERGE: delete removes a cell in place; under-full and empty pages
//!   remain (no merge in the spike, card X-0 Work 5). Pages are never
//!   freed, so page ids stay valid forever — which is what makes the
//!   cursor stack and stale-root reads safe.
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

/// One entry of the `check` DFS stack: a page to validate, the key bounds
/// its subtree must respect (T-ORDER) and its depth.
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
    /// # Errors
    /// [`StoreError::KeyTooLarge`] / [`StoreError::ValueTooLarge`] for
    /// oversized inputs; any pool/store error otherwise.
    pub fn insert(&self, key: &[u8], value: &[u8]) -> Result<Option<Vec<u8>>, StoreError> {
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
                        let safe = page::is_safe(&child)?;
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

    /// Validate the whole tree and return its shape: keys sorted within
    /// pages, separators bounding their subtrees (T-ORDER), every page
    /// reachable exactly once, no cell overlaps, all leaves at the same
    /// depth. Call it quiescently (tests do); under concurrency it may see
    /// a torn multi-page view even when both interleavings are legal.
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
                        let child_lo = prev_sep.clone();
                        let child_hi = Some(sep.to_vec());
                        stack.push(CheckEntry {
                            id: child,
                            lo: child_lo,
                            hi: child_hi,
                            depth: depth + 1,
                        });
                        prev_sep = Some(sep.to_vec());
                    }
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
    /// leaf with the pending cell and repair the ancestors.
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
        match page::leaf_find(&leaf, key)? {
            Ok(i) => {
                let old = page::leaf_cell(&leaf, i)?.1.to_vec();
                page::leaf_remove(&mut leaf, i)?;
                if page::leaf_insert(&mut leaf, i, key, value)? {
                    leaf.mark_dirty();
                    return Ok(Some(old));
                }
                // The replacement is larger than the old cell and even a
                // compacting insert cannot fit it: split. The key is
                // currently absent from the leaf (removed above), so the
                // split's pending insert reinstates it.
                self.split_and_propagate(leaf, leaf_id, path, guards, key, value, Some(old))
            }
            Err(pos) => {
                if page::leaf_insert(&mut leaf, pos, key, value)? {
                    leaf.mark_dirty();
                    return Ok(None);
                }
                self.split_and_propagate(leaf, leaf_id, path, guards, key, value, None)
            }
        }
    }

    /// Split the full leaf (with the pending cell) and install the new
    /// right page in the leaf's parent, splitting upward as needed.
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
    ) -> Result<Option<Vec<u8>>, StoreError> {
        let (right_id, mut right) = self.pool.new_page()?;
        let sep = page::leaf_split(&mut leaf, &mut right, (key, value))?;
        leaf.mark_dirty();
        right.mark_dirty();
        drop(right);
        self.propagate_split(leaf_id, right_id, sep, Some(leaf), path, guards)?;
        Ok(old)
    }

    /// Insert `(right_id, sep)` as the sibling pointer of `left_id` in its
    /// parent, cascading splits toward the root.
    ///
    /// By T-COUPLING every ancestor of an unsafe node is still latched in
    /// `guards`, in top-down acquisition order, so the parent latch is
    /// always already held — except for the root, which is split under the
    /// `root` lock. If a parent is missing for a non-root node, the
    /// ancestors were released early (the COUPLING-EARLY-RELEASE mutation
    /// of the card's evidence list): the child latch is dropped and the
    /// parent re-derived by a fresh descent, which is deliberately *not* a
    /// repair — racing writers make the re-derived parent stale, and the
    /// resulting misrouting is what the concurrency tests must catch. The
    /// child latch is always released before any upward latch is taken:
    /// latches are ordered top-down only, and holding a child while
    /// blocking on an ancestor can deadlock.
    fn propagate_split<'s>(
        &'s self,
        mut left_id: PageId,
        mut right_id: PageId,
        mut sep: Vec<u8>,
        mut child_guard: Option<WriteGuard<'s, S>>,
        path: &mut Vec<PageId>,
        guards: &mut Vec<WriteGuard<'s, S>>,
    ) -> Result<(), StoreError> {
        loop {
            let parent = match (guards.pop(), path.pop()) {
                (Some(g), Some(pid)) => Some((pid, g)),
                (None, None) => {
                    if left_id == self.root_id()? {
                        None
                    } else {
                        drop(child_guard.take());
                        Some(self.find_parent(left_id, &sep)?)
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
                // Root split: a new root over (left_id, right_id).
                let (new_id, mut new_root) = self.pool.new_page()?;
                page::init(&mut new_root, PageKind::Inner)?;
                if !page::inner_insert(&mut new_root, 0, left_id, &sep)? {
                    return Err(StoreError::Corrupt(
                        "a fresh root refused its first cell".into(),
                    ));
                }
                page::set_right_child(&mut new_root, right_id)?;
                new_root.mark_dirty();
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
                pg.mark_dirty();
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
                pg.mark_dirty();
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
                pg.mark_dirty();
                drop(pg);
                drop(child_guard);
                return Ok(());
            }
            // The parent is full: split it with the pending cell and carry
            // the middle separator upward.
            let (pr_id, mut pr) = self.pool.new_page()?;
            let up = page::inner_split(&mut pg, &mut pr, (cell_child, &cell_sep))?;
            pg.mark_dirty();
            pr.mark_dirty();
            drop(pr);
            child_guard = Some(pg);
            left_id = pid;
            right_id = pr_id;
            sep = up;
        }
    }

    /// Re-derive the current parent of `target` by descending from the
    /// root along `key` (which routes into `target`'s subtree) until a
    /// node's routed child is `target`. Returns that node latched
    /// exclusive with its id. Holding no other latches, this descent is
    /// deadlock-free (top-down order).
    fn find_parent(
        &self,
        target: PageId,
        key: &[u8],
    ) -> Result<(PageId, WriteGuard<'_, S>), StoreError> {
        let mut tries = 0u32;
        loop {
            let rid = self.root_id()?;
            let mut guard = self.pool.fetch_write(rid)?;
            if rid != self.root_id()? {
                Self::bump_root_tries(&mut tries)?;
                continue;
            }
            let mut nid = rid;
            loop {
                match page::kind(&guard)? {
                    PageKind::Leaf => {
                        return Err(StoreError::Corrupt(format!(
                            "page {target:?} is not on the path of key; the tree was \
                             restructured under a released ancestor latch"
                        )));
                    }
                    PageKind::Inner => {
                        let (_, child) = page::inner_route(&guard, key)?;
                        if child == target {
                            return Ok((nid, guard));
                        }
                        let next = self.pool.fetch_write(child)?;
                        drop(guard);
                        guard = next;
                        nid = child;
                    }
                }
            }
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
