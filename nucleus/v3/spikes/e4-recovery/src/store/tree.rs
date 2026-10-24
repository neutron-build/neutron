#![deny(clippy::indexing_slicing)]
//! The B+tree (S-STORE §6–§8; card X-5 Work 3.6–3.7, 3.9): slotted pages
//! with the N3 = (b) versioned entry layout, a stable root (§6.3), split
//! and merge, the tree directory (§6.8) and the GC sweep (§10.2, §10.4).
//!
//! Adapted from spikes/e0-btree/src/tree.rs@391e816c (changed: stable
//! roots instead of the T-ROOT restart — a root split moves the cells
//! into two **new** pages and rewrites the root in place; latch coupling
//! replaced by a per-tree sim mutex per card 3.6; merge added; pages
//! travel as owned copies between the pool primitives). No overflow
//! chains (values ≤ `V_INLINE`, declared), no inner redistribution (card
//! 3.6: "merge-only").
//!
//! Entries are inserted only by apply, replay and bulk load, and removed
//! only by GC and DropTree (§6.5). I-ENTRY-UNIQUE: an identical re-insert
//! is a no-op; a different value is XX001 (§9.1; the two mutants flip
//! each direction).

use std::collections::BTreeMap;

use super::{page, DirEntry, Fail, Mutant, StoreInner, Cx, CATALOG, DIR_TREE};

/// The pack factor (§6.5).
pub const F_PACK: f64 = 0.85;

// ---- N3 = (b) entry keys (§7.2) ----

/// `entry_key(k, ts) = escape(k) ‖ 0x00 0x00 ‖ be64(!ts)`.
pub fn enc_ekey(k: &[u8], ts: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(k.len() + 10);
    for &b in k {
        if b == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
    out.extend_from_slice(&(!ts).to_be_bytes());
    out
}

/// `prefix(k) = escape(k) ‖ 0x00 0x00`.
pub fn enc_prefix(k: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(k.len() + 2);
    for &b in k {
        if b == 0 {
            out.push(0);
            out.push(0xFF);
        } else {
            out.push(b);
        }
    }
    out.push(0);
    out.push(0);
    out
}

/// Decode an entry key from the right: `(user key, ts)`.
pub fn dec_ekey(e: &[u8]) -> Option<(Vec<u8>, u64)> {
    if e.len() < 10 {
        return None;
    }
    let n = e.len() - 10;
    let body = e.get(..n)?;
    if e.get(n) != Some(&0) || e.get(n + 1) != Some(&0) {
        return None;
    }
    let mut tsb = [0u8; 8];
    tsb.copy_from_slice(e.get(n + 2..n + 10)?);
    let ts = !u64::from_be_bytes(tsb);
    let mut k = Vec::with_capacity(n);
    let mut i = 0usize;
    while i < body.len() {
        let b = *body.get(i)?;
        if b == 0 {
            if *body.get(i + 1)? != 0xFF {
                return None;
            }
            k.push(0);
            i += 2;
        } else {
            k.push(b);
            i += 1;
        }
    }
    Some((k, ts))
}

// ---- page cells ----

type Cells = Vec<(Vec<u8>, Vec<u8>)>;
type Seps = Vec<(u64, Vec<u8>)>;

fn leaf_cells(p: &[u8]) -> Result<Cells, Fail> {
    let s = page::slotted(p).map_err(|()| Fail::Corrupt("bad slotted page".into()))?;
    let mut out = Vec::with_capacity(s.n);
    for i in 0..s.n {
        let (k, v) =
            page::leaf_cell(p, i).map_err(|()| Fail::Corrupt("bad leaf cell".into()))?;
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}

fn inner_seps(p: &[u8]) -> Result<(Seps, u64), Fail> {
    let s = page::slotted(p).map_err(|()| Fail::Corrupt("bad slotted page".into()))?;
    let mut out = Vec::with_capacity(s.n);
    for i in 0..s.n {
        let (c, k) = page::inner_cell(p, i)
            .map_err(|()| Fail::Corrupt("bad inner cell".into()))?;
        out.push((c, k.to_vec()));
    }
    Ok((
        out,
        page::right_child(p).map_err(|()| Fail::Corrupt("bad inner".into()))?,
    ))
}

fn used(cells: &[(Vec<u8>, Vec<u8>)]) -> usize {
    cells
        .iter()
        .map(|(k, v)| page::SLOT + 6 + k.len() + v.len())
        .sum()
}

fn used_seps(seps: &[(u64, Vec<u8>)]) -> usize {
    seps.iter().map(|(_, s)| page::SLOT + 10 + s.len()).sum()
}

/// The usable body bytes of a page (§1 `U`, conservatively).
fn u_page() -> usize {
    page::PAGE - page::HDR - 16 * 2
}

fn cells_refs(cells: &[(Vec<u8>, Vec<u8>)]) -> Vec<(&[u8], &[u8])> {
    cells.iter().map(|(k, v)| (k.as_slice(), v.as_slice())).collect()
}

fn seps_refs(seps: &[(u64, Vec<u8>)]) -> Vec<(u64, &[u8])> {
    seps.iter().map(|(c, s)| (*c, s.as_slice())).collect()
}

thread_local! {
    /// The LPIDs pinned by the current operation (unpinned at op end).
    static PINS: std::cell::RefCell<Vec<u64>> = const { std::cell::RefCell::new(Vec::new()) };
}

/// Public leaf-cell decoder for amcheck.
pub fn leaf_cells_pub(p: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Fail> {
    leaf_cells(p)
}

/// Public inner-separator decoder for amcheck.
pub fn inner_seps_pub(p: &[u8]) -> Result<(Vec<(u64, Vec<u8>)>, u64), Fail> {
    inner_seps(p)
}

fn pin_note(l: u64) {
    PINS.with(|p| {
        let mut v = p.borrow_mut();
        if !v.contains(&l) {
            v.push(l);
        }
    });
}

fn pins_take() -> Vec<u64> {
    PINS.with(|p| std::mem::take(&mut *p.borrow_mut()))
}

fn unpin_all(st: &mut StoreInner, cx: &Cx) {
    for l in pins_take() {
        st.unpin(cx, l);
    }
}

impl StoreInner {
    // ---- descent ----

    /// Descend to the leaf for `key`, pinning the path (root first).
    fn descend(&mut self, cx: &Cx, root: u64, key: &[u8]) -> Result<Vec<(u64, Vec<u8>)>, Fail> {
        let mut path = Vec::new();
        let mut cur = root;
        for _ in 0..24 {
            let (bytes, _) = self.fetch_page(cx, cur)?;
            pin_note(cur);
            let kind = bytes.get(1).copied().unwrap_or(0);
            path.push((cur, bytes));
            if kind == page::KIND_LEAF {
                return Ok(path);
            }
            let (seps, right) = inner_seps(
                path.last()
                    .and_then(|p| Some(p.1.as_slice()))
                    .unwrap_or(&[]),
            )?;
            let mut next = right;
            for (child, sep) in seps.iter() {
                if sep.as_slice() > key {
                    next = *child;
                    break;
                }
            }
            cur = next;
        }
        Err(Fail::Corrupt("descent did not terminate".into()))
    }

    // ---- insert (§9.1) ----

    /// Insert one entry into the tree at `root` (one gated operation).
    /// Returns `Ok(true)` if inserted, `Ok(false)` for the identical-entry
    /// no-op (I-ENTRY-UNIQUE).
    pub fn tree_insert(
        &mut self,
        cx: &Cx,
        tree_mu: u64,
        root: u64,
        ekey: &[u8],
        value: &[u8],
    ) -> Result<bool, Fail> {
        self.alive()?;
        self.gate_shared(cx)?;
        if let Err(e) = self.tree_lock(cx, tree_mu) {
            self.gate_release(cx);
            return Err(e);
        }
        let r = self.insert_locked(cx, root, ekey, value);
        unpin_all(self, cx);
        self.tree_unlock(cx, tree_mu);
        self.gate_release(cx);
        r
    }

    fn insert_locked(
        &mut self,
        cx: &Cx,
        root: u64,
        ekey: &[u8],
        value: &[u8],
    ) -> Result<bool, Fail> {
        let path = self.descend(cx, root, ekey)?;
        let (leaf_lpid, leaf_bytes) = path
            .last()
            .cloned()
            .ok_or_else(|| Fail::Invalid("empty path".into()))?;
        let mut all = leaf_cells(&leaf_bytes)?;
        let pos = all
            .iter()
            .position(|(k, _)| k.as_slice() > ekey)
            .unwrap_or(all.len());
        if pos < all.len() && all.get(pos).map(|c| c.0.as_slice()) == Some(ekey) {
            // §9.1: identical → no-op; different → XX001.
            let old = all.get(pos).map(|c| c.1.clone()).unwrap_or_default();
            if old == value {
                if matches!(self.cfg.mutant, Some(Mutant::ReplayDupConflict)) {
                    return Err(Fail::Corrupt("identical re-insert as conflict".into()));
                }
                return Ok(false);
            }
            if matches!(self.cfg.mutant, Some(Mutant::DupDifferentAccepted)) {
                return Ok(false);
            }
            return Err(Fail::Corrupt("different value at the same entry key".into()));
        }
        all.insert(pos, (ekey.to_vec(), value.to_vec()));
        if used(&all) <= u_page() {
            self.store_page(cx, leaf_lpid, page::build_leaf(leaf_lpid, self.e, &cells_refs(&all)))?;
            return Ok(true);
        }
        // Split (§6.4): partition near half, both sides non-empty.
        let n = all.len();
        if n < 2 {
            return Err(Fail::Corrupt("split with one cell".into()));
        }
        let mut best = 1usize;
        let mut best_diff = usize::MAX;
        for m in 1..n {
            let l: usize = all.iter().take(m).map(|(k, v)| 6 + k.len() + v.len()).sum();
            let r: usize = all.iter().skip(m).map(|(k, v)| 6 + k.len() + v.len()).sum();
            let d = l.abs_diff(r);
            if d < best_diff {
                best_diff = d;
                best = m;
            }
        }
        let left: Cells = all.iter().take(best).cloned().collect();
        let right: Cells = all.iter().skip(best).cloned().collect();
        let sep = right
            .first()
            .map(|c| c.0.clone())
            .ok_or_else(|| Fail::Corrupt("empty right half".into()))?;
        if path.len() == 1 {
            // Root split (§6.3): the cells move into two new pages and the
            // root is rewritten in place as an inner page.
            let l = self.alloc_lpid();
            let r = self.alloc_lpid();
            self.store_page(cx, l, page::build_leaf(l, self.e, &cells_refs(&left)))?;
            self.store_page(cx, r, page::build_leaf(r, self.e, &cells_refs(&right)))?;
            pin_note(l);
            pin_note(r);
            self.store_page(
                cx,
                root,
                page::build_inner(root, self.e, &[(l, sep.as_slice())], r),
            )?;
            self.count_split();
            return Ok(true);
        }
        let r = self.alloc_lpid();
        self.store_page(cx, leaf_lpid, page::build_leaf(leaf_lpid, self.e, &cells_refs(&left)))?;
        self.store_page(cx, r, page::build_leaf(r, self.e, &cells_refs(&right)))?;
        pin_note(r);
        self.count_split();
        self.propagate(cx, root, &path[..path.len() - 1], r, sep.clone())?;
        Ok(true)
    }

    fn propagate(
        &mut self,
        cx: &Cx,
        root: u64,
        inner_path: &[(u64, Vec<u8>)],
        mut add_child: u64,
        mut add_sep: Vec<u8>,
    ) -> Result<(), Fail> {
        let mut d = inner_path.len();
        while d > 0 {
            d -= 1;
            let (lpid, bytes) = inner_path
                .get(d)
                .cloned()
                .ok_or_else(|| Fail::Invalid("path".to_owned()))?;
            let (mut seps, right) = inner_seps(&bytes)?;
            let pos = seps
                .iter()
                .position(|(_, s)| s.as_slice() > add_sep.as_slice())
                .unwrap_or(seps.len());
            seps.insert(pos, (add_child, add_sep.clone()));
            if used_seps(&seps) <= u_page() {
                self.store_page(
                    cx,
                    lpid,
                    page::build_inner(lpid, self.e, &seps_refs(&seps), right),
                )?;
                return Ok(());
            }
            // Split this inner page.
            let n = seps.len();
            let mut best = 1usize;
            let mut best_diff = usize::MAX;
            for m in 1..n {
                let l: usize = seps.iter().take(m).map(|(_, s)| 10 + s.len()).sum();
                let r: usize = seps.iter().skip(m).map(|(_, s)| 10 + s.len()).sum();
                let dd = l.abs_diff(r);
                if dd < best_diff {
                    best_diff = dd;
                    best = m;
                }
            }
            let up_child = seps.get(best).map(|(c, _)| *c).unwrap_or(0);
            let up_sep = seps
                .get(best)
                .map(|(_, s)| s.clone())
                .unwrap_or_else(|| add_sep.clone());
            let left_seps: Seps = seps.iter().take(best).cloned().collect();
            let right_seps: Seps = seps.iter().skip(best + 1).cloned().collect();
            if d == 0 {
                // The root splits: two new children, the root rewritten
                // in place (§6.3).
                let l = self.alloc_lpid();
                let r = self.alloc_lpid();
                self.store_page(
                    cx,
                    l,
                    page::build_inner(l, self.e, &seps_refs(&left_seps), up_child),
                )?;
                self.store_page(
                    cx,
                    r,
                    page::build_inner(r, self.e, &seps_refs(&right_seps), right),
                )?;
                pin_note(l);
                pin_note(r);
                let upc = up_sep.clone();
                self.store_page(
                    cx,
                    root,
                    page::build_inner(root, self.e, &[(l, upc.as_slice())], r),
                )?;
                self.count_split();
                return Ok(());
            }
            let r = self.alloc_lpid();
            self.store_page(
                cx,
                lpid,
                page::build_inner(lpid, self.e, &seps_refs(&left_seps), up_child),
            )?;
            self.store_page(
                cx,
                r,
                page::build_inner(r, self.e, &seps_refs(&right_seps), right),
            )?;
            pin_note(r);
            self.count_split();
            add_child = r;
            add_sep = up_sep;
        }
        Ok(())
    }

    fn count_split(&mut self) {
        if self.ckpt_open {
            self.ckpt_splits += 1;
        }
    }

    fn count_merge(&mut self) {
        if self.ckpt_open {
            self.ckpt_merges += 1;
        }
    }

    // ---- reads (§8) ----

    /// All raw entries of a tree in key order (the 2.3 raw scan;
    /// materialised — declared spike limit).
    pub fn tree_raw(&mut self, cx: &Cx, root: u64) -> Result<Vec<(Vec<u8>, u8, Vec<u8>)>, Fail> {
        let mut out = Vec::new();
        let mut to_visit: Vec<u64> = vec![root];
        while let Some(l) = to_visit.pop() {
            let (bytes, _) = self.fetch_page(cx, l)?;
            let kind = bytes.get(1).copied().unwrap_or(0);
            if kind == page::KIND_LEAF {
                for (k, v) in leaf_cells(&bytes)? {
                    let Some((_, ts)) = dec_ekey(&k) else {
                        return Err(Fail::Corrupt("undecodable entry key".into()));
                    };
                    if ts == 0 || ts == u64::MAX {
                        return Err(Fail::Corrupt("reserved ts".into()));
                    }
                    let tag = v.first().copied().unwrap_or(255);
                    if tag > 1 {
                        return Err(Fail::Corrupt("reserved tag".into()));
                    }
                    out.push((k, tag, v.get(1..).unwrap_or(&[]).to_vec()));
                }
                self.unpin(cx, l);
            } else {
                let (seps, right) = inner_seps(&bytes)?;
                self.unpin(cx, l);
                to_visit.push(right);
                for (c, _) in seps.into_iter().rev() {
                    to_visit.push(c);
                }
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    /// The versioned scan at snapshot `S` (§8.3): per user key the newest
    /// version `≤ S`, tombstones as explicit `None` (the model's shape).
    pub fn tree_scan(
        &mut self,
        cx: &Cx,
        root: u64,
        s: u64,
    ) -> Result<std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>, Fail> {
        let raw = self.tree_raw(cx, root)?;
        let mut out: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> =
            std::collections::BTreeMap::new();
        for (k, tag, val) in raw {
            let Some((uk, ts)) = dec_ekey(&k) else {
                return Err(Fail::Corrupt("undecodable".into()));
            };
            if ts > s || out.contains_key(&uk) {
                continue;
            }
            out.insert(uk, if tag == 0 { Some(val) } else { None });
        }
        Ok(out)
    }

    // ---- GC (§10.2, §10.4) and merge (§6.5 case 1) ----

    /// One GC sweep step over the leaf holding `resume`: apply the §10.2
    /// rule with watermark `w` (the tombstone elided last, deferred when
    /// its group continues past the leaf), then merge with the right
    /// sibling when the pair fits in `F_PACK·U`. One gated operation.
    /// Returns the resume key for the next step (empty = done).
    pub fn gc_step(
        &mut self,
        cx: &Cx,
        tree: u64,
        root: u64,
        w: u64,
        resume: &[u8],
    ) -> Result<Vec<u8>, Fail> {
        self.alive()?;
        self.gate_shared(cx)?;
        if let Err(e) = self.tree_lock(cx, tree) {
            self.gate_release(cx);
            return Err(e);
        }
        let r = self.gc_locked(cx, root, w, resume);
        unpin_all(self, cx);
        self.tree_unlock(cx, tree);
        self.gate_release(cx);
        if self.gc_w_used < w {
            self.gc_w_used = w;
        }
        r
    }

    fn gc_locked(&mut self, cx: &Cx, root: u64, w: u64, resume: &[u8]) -> Result<Vec<u8>, Fail> {
        let path = self.descend(cx, root, resume)?;
        let (leaf_lpid, leaf_bytes) = path
            .last()
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let cells = leaf_cells(&leaf_bytes)?;
        if cells.is_empty() {
            self.root_collapse(cx, root)?;
            return Ok(Vec::new());
        }
        let mut keep: Cells = Vec::new();
        let mut i = 0usize;
        while i < cells.len() {
            let (k, _) = cells
                .get(i)
                .cloned()
                .ok_or_else(|| Fail::Invalid("cell".into()))?;
            let Some((uk, _)) = dec_ekey(&k) else {
                return Err(Fail::Corrupt("undecodable".into()));
            };
            let prefix = enc_prefix(&uk);
            let mut j = i;
            let mut group: Cells = Vec::new();
            while let Some(c) = cells.get(j) {
                if c.0.starts_with(&prefix) {
                    group.push(c.clone());
                    j += 1;
                } else {
                    break;
                }
            }
            // The first version ≤ w is the group's newest ≤ w (ts
            // descending within a group).
            let newest_le_w = group
                .iter()
                .position(|c| dec_ekey(&c.0).map(|(_, ts)| ts <= w).unwrap_or(false));
            match newest_le_w {
                Some(gi) => {
                    let tomb = group
                        .get(gi)
                        .map(|c| c.1.first().copied().unwrap_or(255))
                        == Some(1);
                    let group_ends_here = j < cells.len() || path_merges_here(path.as_slice());
                    for (gj, c) in group.iter().enumerate() {
                        if gj < gi {
                            keep.push(c.clone()); // ts > w
                        } else if gj == gi {
                            if !tomb {
                                keep.push(c.clone());
                            } else if group_ends_here {
                                // Elide (deleted last: whole-page stores make
                                // the in-op order invisible; the deferral
                                // preserves I-GC-ORDER across steps).
                            } else {
                                keep.push(c.clone()); // defer the elision
                            }
                        }
                        // gj > gi: older than the newest ≤ w — deleted.
                    }
                }
                None => {
                    let mut kg: Cells = group.clone();
                    keep.append(&mut kg);
                }
            }
            i = j;
        }
        self.store_page(
            cx,
            leaf_lpid,
            page::build_leaf(leaf_lpid, self.e, &cells_refs(&keep)),
        )?;
        // Merge with the right sibling when the pair fits (§6.5 case 1).
        let merged_past = self.try_merge_right(cx, root, &path)?;
        Ok(merged_past.unwrap_or_else(|| next_after(cells.last().map(|c| c.0.clone()))))
    }

    /// Merge the path's leaf with its right sibling when the pair fits.
    /// Returns the resume-past key when a merge happened.
    fn try_merge_right(
        &mut self,
        cx: &Cx,
        root: u64,
        path: &[(u64, Vec<u8>)],
    ) -> Result<Option<Vec<u8>>, Fail> {
        if path.len() < 2 {
            return Ok(None);
        }
        let (parent_lpid, parent_bytes) = path
            .get(path.len() - 2)
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let (leaf_lpid, leaf_bytes) = path
            .last()
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let (seps, _pright) = inner_seps(&parent_bytes)?;
        let Some(idx) = seps.iter().position(|(c, _)| *c == leaf_lpid) else {
            return Ok(None);
        };
        let Some((right_lpid, _)) = seps.get(idx + 1) else {
            return Ok(None);
        };
        let (right_bytes, _) = self.fetch_page(cx, *right_lpid)?;
        pin_note(*right_lpid);
        let left_cells = leaf_cells(&leaf_bytes)?;
        let right_cells = leaf_cells(&right_bytes)?;
        let mut merged: Cells = left_cells.clone();
        merged.extend(right_cells.iter().cloned());
        if (used(&merged) as f64) > F_PACK * u_page() as f64 {
            return Ok(None);
        }
        self.store_page(
            cx,
            leaf_lpid,
            page::build_leaf(leaf_lpid, self.e, &cells_refs(&merged)),
        )?;
        let (mut pseps, pright) = inner_seps(&parent_bytes)?;
        if idx < pseps.len() {
            pseps.remove(idx);
        }
        self.store_page(
            cx,
            parent_lpid,
            page::build_inner(parent_lpid, self.e, &seps_refs(&pseps), pright),
        )?;
        self.free_lpid(*right_lpid);
        self.count_merge();
        self.root_collapse(cx, root)?;
        Ok(Some(next_after(merged.last().map(|c| c.0.clone()))))
    }

    /// Root collapse (§6.3): an inner root with one child copies the child
    /// up and frees it.
    fn root_collapse(&mut self, cx: &Cx, root: u64) -> Result<(), Fail> {
        let (bytes, _) = self.fetch_page(cx, root)?;
        if bytes.get(1) != Some(&page::KIND_INNER) {
            self.unpin(cx, root);
            return Ok(());
        }
        let (seps, right) = inner_seps(&bytes)?;
        if !seps.is_empty() || right == 0 {
            self.unpin(cx, root);
            return Ok(());
        }
        let child = right;
        let (cbytes, _) = self.fetch_page(cx, child)?;
        pin_note(child);
        let kind = cbytes.get(1).copied().unwrap_or(0);
        let rebuilt = if kind == page::KIND_LEAF {
            let cells = leaf_cells(&cbytes)?;
            page::build_leaf(root, self.e, &cells_refs(&cells))
        } else {
            let (cseps, cright) = inner_seps(&cbytes)?;
            page::build_inner(root, self.e, &seps_refs(&cseps), cright)
        };
        self.store_page(cx, root, rebuilt)?;
        self.free_lpid(child);
        Ok(())
    }

    // ---- directory (§6.8) ----

    /// `CreateTree(id)`: idempotent; allocates a root leaf and inserts the
    /// directory entry. The root is written out immediately so the map has
    /// a home (fresh pages start dirty in the pool otherwise).
    pub fn dir_create_tree(&mut self, cx: &Cx, id: u64) -> Result<(), Fail> {
        if self.dir.contains_key(&id) {
            return Ok(()); // idempotent (§6.8)
        }
        self.gate_shared(cx)?;
        if let Err(e) = self.tree_lock(cx, DIR_TREE) {
            self.gate_release(cx);
            return Err(e);
        }
        let root = self.alloc_lpid();
        let empty = page::build_leaf(root, self.e, &[]);
        self.store_page(cx, root, empty.clone())?;
        let r = self.dir_write_locked(cx, id, root, false);
        unpin_all(self, cx);
        self.tree_unlock(cx, DIR_TREE);
        self.gate_release(cx);
        match r {
            Ok(()) => {
                self.dir.insert(id, DirEntry { root, dropping: false });
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    /// Write one directory entry (holds DIR_TREE's mutex + gate).
    fn dir_write_locked(&mut self, cx: &Cx, id: u64, root: u64, dropping: bool) -> Result<(), Fail> {
        let dir_root = self.meta.dir_root;
        let ekey = id.to_be_bytes().to_vec();
        let mut val = Vec::new();
        val.extend_from_slice(&root.to_le_bytes());
        val.push(u8::from(dropping));
        self.insert_locked(cx, dir_root, &ekey, &val).map(|_| ())
    }

    /// Read the whole directory from its tree (open/recovery).
    pub fn dir_load(&mut self, cx: &Cx) -> Result<BTreeMap<u64, DirEntry>, Fail> {
        let dir_root = self.meta.dir_root;
        if dir_root == 0 {
            return Ok(BTreeMap::new());
        }
        let raw = self.tree_raw(cx, dir_root)?;
        let mut out = BTreeMap::new();
        for (k, _tag, v) in raw {
            if k.len() != 8 || v.len() != 9 {
                return Err(Fail::Corrupt("bad directory entry".into()));
            }
            let mut rb = [0u8; 8];
            if let Some(s) = v.get(0..8) {
                rb.copy_from_slice(s);
            }
            let id = u64::from_be_bytes(k.try_into().map_err(|_| Fail::Corrupt("id".into()))?);
            out.insert(
                id,
                DirEntry {
                    root: u64::from_le_bytes(rb),
                    dropping: v.get(8) == Some(&1),
                },
            );
        }
        Ok(out)
    }

    /// `DropTree(id)` (§6.8): set `dropping`, free the tree's pages
    /// bottom-up in bounded gated steps (one leaf per step, the root
    /// last), then remove the entry. DROP-ROOT-FIRST frees the root first
    /// (the mutant's leak).
    pub fn dir_drop_tree(&mut self, cx: &Cx, id: u64) -> Result<bool, Fail> {
        let Some(entry) = self.dir.get(&id).copied() else {
            return Ok(false); // dropping an absent id does nothing
        };
        if !entry.dropping {
            self.gate_shared(cx)?;
            if let Err(e) = self.tree_lock(cx, DIR_TREE) {
                self.gate_release(cx);
                return Err(e);
            }
            let w = self.dir_write_locked(cx, id, entry.root, true);
            unpin_all(self, cx);
            self.tree_unlock(cx, DIR_TREE);
            self.gate_release(cx);
            w?;
            if let Some(e) = self.dir.get_mut(&id) {
                e.dropping = true;
            }
        }
        if matches!(self.cfg.mutant, Some(Mutant::DropRootFirst)) {
            self.gate_shared(cx)?;
            if let Err(e) = self.tree_lock(cx, id) {
                self.gate_release(cx);
                return Err(e);
            }
            let root = self.dir.get(&id).map(|e| e.root).unwrap_or(0);
            self.free_lpid(root);
            unpin_all(self, cx);
            self.tree_unlock(cx, id);
            self.gate_release(cx);
            self.dir.remove(&id);
            return Ok(true);
        }
        loop {
            let root = self.dir.get(&id).map(|e| e.root).unwrap_or(0);
            self.gate_shared(cx)?;
            if let Err(e) = self.tree_lock(cx, id) {
                self.gate_release(cx);
                return Err(e);
            }
            let walk = self.drop_walk_step(cx, root);
            unpin_all(self, cx);
            self.tree_unlock(cx, id);
            self.gate_release(cx);
            if walk?.is_none() {
                break;
            }
        }
        // Remove the entry.
        self.gate_shared(cx)?;
        if let Err(e) = self.tree_lock(cx, DIR_TREE) {
            self.gate_release(cx);
            return Err(e);
        }
        let dir_root = self.meta.dir_root;
        let ekey = id.to_be_bytes().to_vec();
        let r = self.dir_remove_locked(cx, dir_root, &ekey);
        unpin_all(self, cx);
        self.tree_unlock(cx, DIR_TREE);
        self.gate_release(cx);
        r?;
        self.dir.remove(&id);
        Ok(true)
    }

    /// One DropTree walk step: free the leftmost leaf (the root last).
    /// `Ok(None)` = the tree is gone.
    fn drop_walk_step(&mut self, cx: &Cx, root: u64) -> Result<Option<()>, Fail> {
        let path = self.descend(cx, root, &[])?;
        if path.len() == 1 {
            self.free_lpid(root);
            return Ok(None);
        }
        let (leaf_lpid, _) = path
            .last()
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let (parent_lpid, parent_bytes) = path
            .get(path.len() - 2)
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let (mut seps, right) = inner_seps(&parent_bytes)?;
        if let Some(idx) = seps.iter().position(|(c, _)| *c == leaf_lpid) {
            seps.remove(idx);
            self.store_page(
                cx,
                parent_lpid,
                page::build_inner(parent_lpid, self.e, &seps_refs(&seps), right),
            )?;
        } else if right == leaf_lpid {
            self.store_page(
                cx,
                parent_lpid,
                page::build_inner(parent_lpid, self.e, &seps_refs(&seps), 0),
            )?;
        } else {
            return Err(Fail::Corrupt("leaf not found under parent".into()));
        }
        self.free_lpid(leaf_lpid);
        Ok(Some(()))
    }

    /// Remove an unversioned directory entry by key (holds DIR_TREE).
    fn dir_remove_locked(&mut self, cx: &Cx, dir_root: u64, ekey: &[u8]) -> Result<(), Fail> {
        let path = self.descend(cx, dir_root, ekey)?;
        let (leaf_lpid, leaf_bytes) = path
            .last()
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".into()))?;
        let cells = leaf_cells(&leaf_bytes)?;
        let kept: Cells = cells.iter().filter(|c| c.0.as_slice() != ekey).cloned().collect();
        if kept.len() == cells.len() {
            return Ok(()); // absent: idempotent
        }
        self.store_page(cx, leaf_lpid, page::build_leaf(leaf_lpid, self.e, &cells_refs(&kept)))?;
        Ok(())
    }

    /// Resume `dropping` walks at open (§14 step 6).
    pub fn resume_drops(&mut self, cx: &Cx) -> Result<Vec<u64>, Fail> {
        let ids: Vec<u64> = self
            .dir
            .iter()
            .filter(|(_, e)| e.dropping)
            .map(|(id, _)| *id)
            .collect();
        for id in ids.iter().copied() {
            self.dir_drop_tree(cx, id)?;
        }
        Ok(ids)
    }

    /// The catalog tree's root (tree id 1).
    pub fn catalog_root(&self) -> u64 {
        self.dir.get(&CATALOG).map(|e| e.root).unwrap_or(0)
    }

    /// The persisted map radix's depth (Q16).
    pub fn map_depth(&self) -> u8 {
        let cap = self.cfg.map_leaf_cap;
        let n = (self.meta.next_lpid + cap - 1) / cap;
        let fan = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
        let mut depth = 1u8;
        let mut level = n;
        while level > 1 {
            level = level.div_ceil(fan);
            depth += 1;
        }
        depth
    }
}

/// The GC deferral proof: a group continuing into the right sibling is not
/// ended at this leaf. This helper conservatively reports false (defer);
/// the merge path re-processes merged leaves, which completes them.
fn path_merges_here(_path: &[(u64, Vec<u8>)]) -> bool {
    false
}

/// The resume key strictly after `k` (an appended 0xFF exceeds every
/// extension of k in memcmp order).
fn next_after(k: Option<Vec<u8>>) -> Vec<u8> {
    let mut v = k.unwrap_or_default();
    v.push(0xFF);
    v
}
