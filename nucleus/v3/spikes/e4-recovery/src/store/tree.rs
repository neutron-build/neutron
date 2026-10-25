#![deny(clippy::indexing_slicing)]
//! The B+tree (S-STORE §6–§8; card X-5 Work 3.6–3.7, 3.9): slotted pages
//! with the N3 = (b) versioned entry layout, a stable root (§6.3), split
//! and merge, the tree directory (§6.8) and the GC sweep (§10.2, §10.4).
//!
//! Adapted from spikes/e0-btree/src/tree.rs@391e816c (changed: stable
//! roots instead of the T-ROOT restart — a root split moves the cells
//! into two **new** pages and rewrites the root in place; latch coupling
//! replaced by a per-tree sim mutex per card 3.6; merge added; the whole
//! module converted to the S-BATON step discipline of card X-5b Work 11 —
//! descents keep an owned `(lpid, bytes)` path across the pool
//! primitives' yields and re-validate nothing, because the per-tree mutex
//! means no other task mutates this tree mid-operation, while the
//! checkpointer and evictor only move frames and change the epoch —
//! handled by the map-reference reads of `fetch_page`). No overflow
//! chains (values ≤ `V_INLINE`, declared), no inner redistribution (card
//! 3.6: "merge-only").
//!
//! Entries are inserted only by apply, replay and bulk load, and removed
//! only by GC and DropTree (§6.5). I-ENTRY-UNIQUE: an identical re-insert
//! is a no-op; a different value is XX001 (§9.1; the two mutants flip
//! each direction).
//!
//! Yield points: every `fetch_page`/`store_page` (inside), each gate and
//! mutex acquisition, and the named park points between a split's page
//! mutations (the D5 split parking, card Work 13).

use std::collections::BTreeMap;

use super::page;
use super::prims::{
    PinGuard, alloc_lpid, fetch_page, free_lpid, gate_release, gate_shared, store_page, tree_lock,
    tree_unlock,
};
use super::{CATALOG, Cx, DIR_TREE, Fail, Mutant, StoreHost, StoreInner};

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

/// Decode an entry key from the right: `(user key, ts)` (§7.2).
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

// ---- page cells (pure decoders over owned bytes) ----

type Cells = Vec<(Vec<u8>, Vec<u8>)>;
type Seps = Vec<(u64, Vec<u8>)>;

/// A leaf's cells as owned `(entry key, value)` pairs, in page order.
pub fn leaf_cells(p: &[u8]) -> Result<Cells, Fail> {
    let s = page::slotted(p).map_err(|()| Fail::Corrupt("bad slotted page".into()))?;
    let mut out = Vec::with_capacity(s.n);
    for i in 0..s.n {
        let (k, v) = page::leaf_cell(p, i).map_err(|()| Fail::Corrupt("bad leaf cell".into()))?;
        out.push((k.to_vec(), v.to_vec()));
    }
    Ok(out)
}

/// An inner page's separators and rightmost child.
pub fn inner_seps(p: &[u8]) -> Result<(Seps, u64), Fail> {
    let s = page::slotted(p).map_err(|()| Fail::Corrupt("bad slotted page".into()))?;
    let mut out = Vec::with_capacity(s.n);
    for i in 0..s.n {
        let (c, k) = page::inner_cell(p, i).map_err(|()| Fail::Corrupt("bad inner cell".into()))?;
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
        .map(|(k, v)| page::SLOT + page::LEAF_HDR + k.len() + v.len())
        .sum()
}

fn used_seps(seps: &[(u64, Vec<u8>)]) -> usize {
    seps.iter()
        .map(|(_, s)| page::SLOT + page::INNER_HDR + s.len())
        .sum()
}

/// The usable body bytes of a page (§1 `U`, conservatively: body minus
/// the leaf `n` field and the inner right-child field).
fn u_page() -> usize {
    page::PAGE - page::HDR - 16
}

fn cells_refs(cells: &[(Vec<u8>, Vec<u8>)]) -> Vec<(&[u8], &[u8])> {
    cells
        .iter()
        .map(|(k, v)| (k.as_slice(), v.as_slice()))
        .collect()
}

fn seps_refs(seps: &[(u64, Vec<u8>)]) -> Vec<(u64, &[u8])> {
    seps.iter().map(|(c, s)| (*c, s.as_slice())).collect()
}

// ---- descent ----

/// Descend to the leaf for `key`, pinning the path root-first (the
/// per-tree mutex makes the owned path stable across the pool's yields).
fn descend<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
    key: &[u8],
) -> Result<Vec<(u64, Vec<u8>)>, Fail> {
    let mut path = Vec::new();
    let mut cur = root;
    for _ in 0..24 {
        let (bytes, _) = fetch_page(cx, cur, true)?;
        pins.note(cur);
        let kind = page::header_kind(&bytes);
        path.push((cur, bytes.clone()));
        if kind == page::KIND_LEAF {
            return Ok(path);
        }
        // Convention: cell `(c, s)` holds keys < s; `right` holds >=
        // s_last — so the first separator above `key` picks its cell.
        let (seps, right) = inner_seps(&bytes)?;
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

/// Insert one entry into the tree rooted at `root` (one gated operation,
/// the per-tree sim mutex held). `replace` allows overwriting a different
/// value at the same entry key — reserved for a chunked transaction's
/// superseding replay ops (§14); ordinary commits keep I-ENTRY-UNIQUE.
/// Returns `Ok(true)` if inserted, `Ok(false)` for the identical-entry
/// no-op.
pub fn tree_insert<H: StoreHost>(
    cx: &Cx<'_, H>,
    tree_mu: u64,
    root: u64,
    ekey: &[u8],
    value: &[u8],
    replace: bool,
) -> Result<bool, Fail> {
    cx.with("insert:alive", |c| c.store().alive())?;
    gate_shared(cx)?;
    if let Err(e) = tree_lock(cx, tree_mu) {
        gate_release(cx);
        return Err(e);
    }
    let mut pins = PinGuard::new(cx);
    let r = insert_locked(cx, &mut pins, root, ekey, value, replace);
    drop(pins);
    tree_unlock(cx, tree_mu);
    gate_release(cx);
    r
}

fn insert_locked<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
    ekey: &[u8],
    value: &[u8],
    replace: bool,
) -> Result<bool, Fail> {
    let (e, mutant) = cx.with("insert:epoch", |c| {
        let st = c.store();
        Ok((st.e, st.cfg.mutant))
    })?;
    let path = descend(cx, pins, root, ekey)?;
    let (leaf_lpid, leaf_bytes) = path
        .last()
        .cloned()
        .ok_or_else(|| Fail::Invalid("empty path".into()))?;
    let mut all = leaf_cells(&leaf_bytes)?;
    let pos = all
        .iter()
        .position(|(k, _)| k.as_slice() > ekey)
        .unwrap_or(all.len());
    // The equal entry, if any, sits just **below** `pos`: `pos` is the
    // first strictly-greater position, so an existing copy of `ekey`
    // is at `pos − 1` (checking `pos` instead let every identical
    // replay insert a duplicate — prototype deviation fixed in B5).
    if pos > 0 && all.get(pos - 1).map(|c| c.0.as_slice()) == Some(ekey) {
        // §9.1: identical → no-op; different → XX001 (or a superseding
        // replace for chunked replay).
        let old = all.get(pos - 1).map(|c| c.1.clone()).unwrap_or_default();
        if old == value {
            if matches!(mutant, Some(Mutant::ReplayDupConflict)) {
                return Err(Fail::Corrupt("identical re-insert as conflict".into()));
            }
            return Ok(false);
        }
        if replace {
            if let Some(c) = all.get_mut(pos - 1) {
                c.1 = value.to_vec();
            }
            store_page(
                cx,
                leaf_lpid,
                page::build_leaf(leaf_lpid, e, &cells_refs(&all)),
            )?;
            return Ok(true);
        }
        if matches!(mutant, Some(Mutant::DupDifferentAccepted)) {
            return Ok(false);
        }
        return Err(Fail::Corrupt(
            "different value at the same entry key".into(),
        ));
    }
    all.insert(pos, (ekey.to_vec(), value.to_vec()));
    if used(&all) <= u_page() {
        store_page(
            cx,
            leaf_lpid,
            page::build_leaf(leaf_lpid, e, &cells_refs(&all)),
        )?;
        return Ok(true);
    }
    // Split (§6.4): partition near half, both sides non-empty. The park
    // point between the page mutations is the D5 split parking (card
    // Work 13): a flip requested mid-split waits (the gate is held).
    let n = all.len();
    if n < 2 {
        return Err(Fail::Corrupt("split with one cell".into()));
    }
    let mut best = 1usize;
    let mut best_diff = usize::MAX;
    for m in 1..n {
        let l: usize = all
            .iter()
            .take(m)
            .map(|(k, v)| page::LEAF_HDR + k.len() + v.len())
            .sum();
        let r: usize = all
            .iter()
            .skip(m)
            .map(|(k, v)| page::LEAF_HDR + k.len() + v.len())
            .sum();
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
        // root is rewritten in place as an inner page. The park points
        // sit between the page mutations (the D5 split parking, card
        // Work 13): a flip requested mid-split waits (the gate is
        // held).
        let l = alloc_lpid(cx)?;
        let r = alloc_lpid(cx)?;
        store_page(cx, l, page::build_leaf(l, e, &cells_refs(&left)))?;
        pins.note(l);
        cx.park("split:mid")?;
        store_page(cx, r, page::build_leaf(r, e, &cells_refs(&right)))?;
        pins.note(r);
        cx.park("split:mid")?;
        store_page(
            cx,
            root,
            page::build_inner(root, e, &[(l, sep.as_slice())], r),
        )?;
        cx.park("split:mid")?;
        count_split(cx)?;
        return Ok(true);
    }
    let r = alloc_lpid(cx)?;
    store_page(
        cx,
        leaf_lpid,
        page::build_leaf(leaf_lpid, e, &cells_refs(&left)),
    )?;
    cx.park("split:mid")?;
    store_page(cx, r, page::build_leaf(r, e, &cells_refs(&right)))?;
    pins.note(r);
    count_split(cx)?;
    if let Some((_leaf, inner)) = path.split_last() {
        link_split(cx, pins, root, inner, leaf_lpid, sep.clone(), r, e)?;
    }
    Ok(true)
}

/// Link a just-split page into its parents. `x` is the split page (its
/// left part), `up` the new separator between the halves and `n` the new
/// right page. Under the cell convention (a cell `(c, s)` holds keys
/// below `s`, and `right` holds the keys at or above the last
/// separator): if `x` is cell `i` of its parent, cell i's separator
/// becomes `up` and `(n, old s_i)` is inserted after it; if `x` is the
/// parent's right child, `(x, up)` is appended and `n` becomes right.
/// The parent may itself split, cascading upward to the root.
#[allow(clippy::too_many_arguments)]
fn link_split<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
    inner_path: &[(u64, Vec<u8>)],
    x: u64,
    up: Vec<u8>,
    n: u64,
    e: u32,
) -> Result<(), Fail> {
    let mut add_child = n;
    let mut add_sep = up;
    let mut target = x;
    let mut d = inner_path.len();
    while d > 0 {
        d -= 1;
        let (lpid, bytes) = inner_path
            .get(d)
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".to_owned()))?;
        let (mut seps, right) = inner_seps(&bytes)?;
        // Apply the split link for `target` on this page.
        match seps.iter().position(|(c, _)| *c == target) {
            Some(i) => {
                let old: (u64, Vec<u8>) = seps
                    .get(i)
                    .map(|(c, s)| (*c, s.clone()))
                    .ok_or_else(|| Fail::Invalid("seps".to_owned()))?;
                if let Some(cell) = seps.get_mut(i) {
                    cell.1 = add_sep.clone();
                }
                let old_s = old.1;
                seps.insert(i + 1, (add_child, old_s));
            }
            None => {
                // `target` is this page's right child.
                seps.push((target, add_sep.clone()));
                let _ = right;
            }
        }
        let new_right = if seps.iter().any(|(c, _)| *c == target) && right == target {
            add_child // right became a cell; the new page is right
        } else if right != target {
            right
        } else {
            add_child
        };
        if used_seps(&seps) <= u_page() {
            store_page(
                cx,
                lpid,
                page::build_inner(lpid, e, &seps_refs(&seps), new_right),
            )?;
            return Ok(());
        }
        // This page splits. With cells [(c_0,s_0)..(c_{n-1},s_{n-1})] +
        // right R and child ranges c_i: [s_{i-1}, s_i), R: [s_last, ∞):
        // a split at m keeps children 0..m-1 on this page — cells
        // [0..m-1) with c_{m-1} as its new `right` (its upper bound
        // s_{m-1} rises to the parent as the up-separator) — and moves
        // cells [m..) with R onto the new page.
        let (m, up_sep, left_cells, left_right, right_cells) = split_inner_cells(&seps)?;
        if d == 0 {
            // The root splits (§6.3): two new children, the root
            // rewritten in place.
            let l = alloc_lpid(cx)?;
            cx.park("split:mid")?;
            let r = alloc_lpid(cx)?;
            store_page(
                cx,
                l,
                page::build_inner(l, e, &seps_refs(&left_cells), left_right),
            )?;
            pins.note(l);
            cx.park("split:mid")?;
            store_page(
                cx,
                r,
                page::build_inner(r, e, &seps_refs(&right_cells), new_right),
            )?;
            pins.note(r);
            cx.park("split:mid")?;
            let upc = up_sep.clone();
            store_page(
                cx,
                root,
                page::build_inner(root, e, &[(l, upc.as_slice())], r),
            )?;
            count_split(cx)?;
            return Ok(());
        }
        let r = alloc_lpid(cx)?;
        store_page(
            cx,
            lpid,
            page::build_inner(lpid, e, &seps_refs(&left_cells), left_right),
        )?;
        cx.park("split:mid")?;
        store_page(
            cx,
            r,
            page::build_inner(r, e, &seps_refs(&right_cells), new_right),
        )?;
        pins.note(r);
        count_split(cx)?;
        let _ = m;
        add_child = r;
        add_sep = up_sep;
        target = lpid;
    }
    Ok(())
}

/// Partition inner cells near half by bytes: the split at `m` keeps
/// children `0..m-1` left. Returns `(m, up-separator s_{m-1}, left
/// cells [0..m-1), left page's right child c_{m-1}, right cells [m..))`.
#[allow(clippy::type_complexity)]
fn split_inner_cells(seps: &Seps) -> Result<(usize, Vec<u8>, Seps, u64, Seps), Fail> {
    let n = seps.len();
    if n < 2 {
        return Err(Fail::Corrupt("inner split with one cell".into()));
    }
    // m ranges 1..n: both pages keep at least one child (m = n would
    // empty the right page's cells).
    let mut best = 1usize;
    let mut best_diff = usize::MAX;
    for m in 1..n {
        let l: usize = seps
            .iter()
            .take(m)
            .map(|(_, s)| page::INNER_HDR + s.len())
            .sum();
        let r: usize = seps
            .iter()
            .skip(m)
            .map(|(_, s)| page::INNER_HDR + s.len())
            .sum();
        let dd = l.abs_diff(r);
        if dd < best_diff {
            best_diff = dd;
            best = m;
        }
    }
    let (left_right, up) = seps
        .get(best - 1)
        .map(|(c, s)| (*c, s.clone()))
        .ok_or_else(|| Fail::Invalid("seps".to_owned()))?;
    let left_cells: Seps = seps.iter().take(best - 1).cloned().collect();
    let right_cells: Seps = seps.iter().skip(best).cloned().collect();
    Ok((best, up, left_cells, left_right, right_cells))
}

fn count_split<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    cx.with("count_split", |c| {
        let st = c.store();
        if st.ckpt_open {
            st.ckpt_splits += 1;
        }
        Ok(())
    })
}

fn count_merge<H: StoreHost>(cx: &Cx<'_, H>) -> Result<(), Fail> {
    cx.with("count_merge", |c| {
        let st = c.store();
        if st.ckpt_open {
            st.ckpt_merges += 1;
        }
        Ok(())
    })
}

// ---- reads (§8) ----

/// One raw entry of a versioned tree: `(entry key bytes, value tag, value
/// bytes)`.
pub type RawCell = (Vec<u8>, u8, Vec<u8>);

/// All raw cells of a tree in key order: `(key, value)` pairs as stored.
/// `versioned` entries are validated (decode + tag, §7); a directory's
/// unversioned entries are returned verbatim. Materialised — declared
/// spike limit. Not gated; fetches run `inside_op = false` (§5.2's
/// outside-eviction rule).
#[allow(clippy::type_complexity)]
pub fn raw_cells<H: StoreHost>(
    cx: &Cx<'_, H>,
    root: u64,
    versioned: bool,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Fail> {
    let mut out = Vec::new();
    let mut to_visit: Vec<u64> = vec![root];
    while let Some(l) = to_visit.pop() {
        let (bytes, _) = fetch_page(cx, l, false)?;
        let kind = page::header_kind(&bytes);
        if kind == page::KIND_LEAF {
            for (k, v) in leaf_cells(&bytes)? {
                if versioned {
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
                }
                out.push((k, v));
            }
            super::prims::unpin(cx, l);
        } else {
            let (seps, right) = inner_seps(&bytes)?;
            super::prims::unpin(cx, l);
            to_visit.push(right);
            for (c, _) in seps.into_iter().rev() {
                to_visit.push(c);
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

/// All raw entries of a versioned tree in key order (the 2.3 raw scan).
pub fn tree_raw<H: StoreHost>(cx: &Cx<'_, H>, root: u64) -> Result<Vec<RawCell>, Fail> {
    let mut out = Vec::new();
    for (k, v) in raw_cells(cx, root, true)? {
        let tag = v.first().copied().unwrap_or(255);
        let val = v.get(1..).unwrap_or(&[]).to_vec();
        out.push((k, tag, val));
    }
    Ok(out)
}

/// The versioned scan at snapshot `S` (§8.3): per user key the newest
/// version `≤ S`, tombstones as explicit `None` (the model's shape).
pub fn tree_scan<H: StoreHost>(
    cx: &Cx<'_, H>,
    root: u64,
    s: u64,
) -> Result<std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>>, Fail> {
    let raw = tree_raw(cx, root)?;
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

/// One GC sweep step over the leaf holding `resume`: apply the §10.2 rule
/// with watermark `w` (the tombstone elided last, deferred when its group
/// continues past the leaf), then merge with the right sibling when the
/// pair fits in `F_PACK·U`. One gated operation. Returns the resume key
/// for the next step (empty = done). `gc_w_used` advances inside the
/// gate, so a concurrent flip cannot miss it (§10.5).
pub fn gc_step<H: StoreHost>(
    cx: &Cx<'_, H>,
    tree: u64,
    root: u64,
    w: u64,
    resume: &[u8],
) -> Result<Vec<u8>, Fail> {
    cx.with("gc:alive", |c| c.store().alive())?;
    gate_shared(cx)?;
    if let Err(e) = tree_lock(cx, tree) {
        gate_release(cx);
        return Err(e);
    }
    let mut pins = PinGuard::new(cx);
    let r = gc_locked(cx, &mut pins, root, w, resume);
    let bump = r.is_ok();
    drop(pins);
    if bump {
        cx.with("gc:advance", |c| {
            let st = c.store();
            if st.gc_w_used < w {
                st.gc_w_used = w;
            }
            Ok(())
        })?;
    }
    tree_unlock(cx, tree);
    gate_release(cx);
    r
}

fn gc_locked<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
    w: u64,
    resume: &[u8],
) -> Result<Vec<u8>, Fail> {
    let e = cx.with("gc:epoch", |c| Ok(c.store().e))?;
    let path = descend(cx, pins, root, resume)?;
    let (leaf_lpid, leaf_bytes) = path
        .last()
        .cloned()
        .ok_or_else(|| Fail::Invalid("path".into()))?;
    let cells = leaf_cells(&leaf_bytes)?;
    if cells.is_empty() {
        root_collapse(cx, pins, root)?;
        return Ok(Vec::new());
    }
    // The sweep's end: the resume key is past everything this leaf holds
    // and the path is the rightmost one (no sibling anywhere above).
    let exhausted = cells
        .last()
        .map(|c| c.0.as_slice() < resume)
        .unwrap_or(false);
    if exhausted && path_is_rightmost(&path)? {
        root_collapse(cx, pins, root)?;
        return Ok(Vec::new());
    }
    // §10.4's per-step shape: the leaf L **and its right sibling R** (one
    // gated operation): a group spanning the L/R boundary is decided as a
    // whole, so a deferred tombstone's older versions cannot survive
    // beside it (I-GC-ORDER, I-GC-COMPLETE).
    let (sibling, n_left) = if path.len() >= 2 {
        match path.get(path.len() - 2) {
            Some((_p_lpid, p_bytes)) => {
                let (seps, right) = inner_seps(p_bytes)?;
                match seps.iter().position(|(c, _)| *c == leaf_lpid) {
                    Some(idx) => {
                        let r = match seps.get(idx + 1) {
                            Some((r, _)) => Some(*r),
                            None => {
                                if right != 0 && right != leaf_lpid {
                                    Some(right)
                                } else {
                                    None
                                }
                            }
                        };
                        (r, cells.len())
                    }
                    None => (None, cells.len()),
                }
            }
            None => (None, cells.len()),
        }
    } else {
        (None, cells.len())
    };
    let mut cells_all: Cells = cells.clone();
    let mut sibling_bytes = None;
    if let Some(r) = sibling {
        let (rb, _) = fetch_page(cx, r, true)?;
        pins.note(r);
        let rc = leaf_cells(&rb)?;
        let n = cells_all.len();
        cells_all.extend(rc.iter().cloned());
        sibling_bytes = Some((r, rb, n));
    }
    // Whether the last processed page has a page to its right (its group
    // may continue): the sibling's own right neighbour, or the path's
    // rightmost status when there is no sibling.
    let right_continues = match (sibling, path_is_rightmost(&path)?) {
        (Some(_), true) => {
            // The path is rightmost above L, and a sibling exists: the
            // sibling is the last leaf under the path's parent — but the
            // parent's own right sibling may hold more.
            true
        }
        (Some(_), false) => true,
        (None, true) => false,
        (None, false) => true,
    };
    let mut keep: Cells = Vec::new();
    let mut i = 0usize;
    while i < cells_all.len() {
        let k = cells_all
            .get(i)
            .map(|c| c.0.clone())
            .ok_or_else(|| Fail::Invalid("cell".into()))?;
        let Some((uk, _)) = dec_ekey(&k) else {
            return Err(Fail::Corrupt("undecodable".into()));
        };
        let prefix = enc_prefix(&uk);
        let mut j = i;
        let mut group: Cells = Vec::new();
        while let Some(c) = cells_all.get(j) {
            if c.0.starts_with(&prefix) {
                group.push(c.clone());
                j += 1;
            } else {
                break;
            }
        }
        // The first version ≤ w is the group's newest ≤ w (ts descending
        // within a group).
        let newest_le_w = group
            .iter()
            .position(|c| dec_ekey(&c.0).map(|(_, ts)| ts <= w).unwrap_or(false));
        // The last group of the step defers its elision when a page right
        // of the last processed page exists (its group may continue);
        // conservative (I-GC-ORDER), and the next sweep elides it once
        // the older versions are gone.
        let last_group = j >= cells_all.len();
        let page_continues = last_group && right_continues;
        match newest_le_w {
            Some(gi) => {
                let tomb = group.get(gi).map(|c| c.1.first().copied().unwrap_or(255)) == Some(1);
                let group_ends_here = !last_group || !page_continues;
                for (gj, c) in group.iter().enumerate() {
                    if gj < gi {
                        keep.push(c.clone()); // ts > w
                    } else if gj == gi {
                        if !tomb {
                            keep.push(c.clone());
                        } else if group_ends_here {
                            // Elided; deleted last within the one gated
                            // mutation that removes the group's older
                            // versions too.
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
    // The pack step (§6.5 case 1): when the pair of kept halves fits in
    // `F_PACK·U`, L absorbs R's part and R is unlinked and freed;
    // otherwise each page keeps its part.
    let (keep_l_part, keep_r_part): (Cells, Cells) = match sibling_bytes.as_ref() {
        Some(_) => {
            let kl: Cells = keep.iter().take(n_left).cloned().collect();
            let kr: Cells = keep.iter().skip(n_left).cloned().collect();
            (kl, kr)
        }
        None => (keep.clone(), Vec::new()),
    };
    let absorb = match sibling_bytes.as_ref() {
        Some(_) => {
            let mut combined: Cells = keep_l_part.clone();
            combined.extend(keep_r_part.iter().cloned());
            (used(&combined) as f64) <= F_PACK * u_page() as f64
        }
        None => false,
    };
    let keep_l: Cells = if absorb {
        let mut combined: Cells = keep_l_part.clone();
        combined.extend(keep_r_part.iter().cloned());
        combined
    } else {
        keep_l_part.clone()
    };
    store_page(
        cx,
        leaf_lpid,
        page::build_leaf(leaf_lpid, e, &cells_refs(&keep_l)),
    )?;
    if let Some((r_lpid, r_bytes, _n)) = sibling_bytes {
        if absorb {
            // Unlink R from the parent under the cell convention.
            let (parent_lpid, parent_bytes) = path
                .get(path.len() - 2)
                .cloned()
                .ok_or_else(|| Fail::Invalid("path".into()))?;
            let (pseps, pright) = inner_seps(&parent_bytes)?;
            let found = pseps.iter().position(|(c, _)| *c == leaf_lpid);
            if let Some(idx) = found {
                match pseps.get(idx + 1).map(|(_, s)| s.clone()) {
                    Some(s_next) => {
                        let mut q = pseps.clone();
                        if let Some(cell) = q.get_mut(idx) {
                            cell.1 = s_next;
                        }
                        if idx + 1 < q.len() {
                            q.remove(idx + 1);
                        }
                        store_page(
                            cx,
                            parent_lpid,
                            page::build_inner(parent_lpid, e, &seps_refs(&q), pright),
                        )?;
                    }
                    None => {
                        let mut q = pseps.clone();
                        if idx < q.len() {
                            q.remove(idx);
                        }
                        store_page(
                            cx,
                            parent_lpid,
                            page::build_inner(parent_lpid, e, &seps_refs(&q), leaf_lpid),
                        )?;
                    }
                }
            }
            free_lpid(cx, r_lpid)?;
            count_merge(cx)?;
            let _ = r_bytes;
        } else {
            store_page(
                cx,
                r_lpid,
                page::build_leaf(r_lpid, e, &cells_refs(&keep_r_part)),
            )?;
        }
    }
    root_collapse(cx, pins, root)?;
    Ok(next_after(cells_all.last().map(|c| c.0.clone())))
}

/// Whether every page of the descent path is its parent's rightmost
/// child (the path ends at the tree's rightmost leaf).
fn path_is_rightmost(path: &[(u64, Vec<u8>)]) -> Result<bool, Fail> {
    for w in path.windows(2) {
        let (_parent_lpid, parent_bytes) =
            w.first().ok_or_else(|| Fail::Invalid("path".to_owned()))?;
        let (child_lpid, _) = w.get(1).ok_or_else(|| Fail::Invalid("path".to_owned()))?;
        let (seps, right) = inner_seps(parent_bytes)?;
        if seps.iter().any(|(c, _)| c == child_lpid) {
            return Ok(false);
        }
        if right != *child_lpid {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Root collapse (§6.3): an inner root with one child copies the child up
/// and frees it.
fn root_collapse<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
) -> Result<(), Fail> {
    let e = cx.with("collapse:epoch", |c| Ok(c.store().e))?;
    let (bytes, _) = fetch_page(cx, root, true)?;
    if page::header_kind(&bytes) != page::KIND_INNER {
        super::prims::unpin(cx, root);
        return Ok(());
    }
    let (seps, right) = inner_seps(&bytes)?;
    if !seps.is_empty() || right == 0 {
        super::prims::unpin(cx, root);
        return Ok(());
    }
    let child = right;
    let (cbytes, _) = fetch_page(cx, child, true)?;
    pins.note(child);
    let kind = page::header_kind(&cbytes);
    let rebuilt = if kind == page::KIND_LEAF {
        let cells = leaf_cells(&cbytes)?;
        page::build_leaf(root, e, &cells_refs(&cells))
    } else {
        let (cseps, cright) = inner_seps(&cbytes)?;
        page::build_inner(root, e, &seps_refs(&cseps), cright)
    };
    store_page(cx, root, rebuilt)?;
    free_lpid(cx, child)?;
    Ok(())
}

// ---- directory (§6.8) ----

/// `CreateTree(id)`: idempotent; allocates a root leaf and inserts the
/// directory entry. The root is written out immediately (a fresh page
/// starts dirty in the pool otherwise; the map needs a home for the
/// directory to survive before the first checkpoint adopts it).
pub fn dir_create_tree<H: StoreHost>(cx: &Cx<'_, H>, id: u64) -> Result<(), Fail> {
    let exists = cx.with("dir_create:check", |c| Ok(c.store().dir.contains_key(&id)))?;
    if exists {
        return Ok(()); // idempotent (§6.8)
    }
    gate_shared(cx)?;
    if let Err(e2) = tree_lock(cx, DIR_TREE) {
        gate_release(cx);
        return Err(e2);
    }
    let mut pins = PinGuard::new(cx);
    let root = alloc_lpid(cx)?;
    let e = cx.with("dir_create:epoch", |c| Ok(c.store().e))?;
    let empty = page::build_leaf(root, e, &[]);
    store_page(cx, root, empty.clone())?;
    let dir_root = cx.with("dir_create:dr", |c| Ok(c.store().meta.dir_root))?;
    let r = dir_put_locked(cx, &mut pins, dir_root, id, root, false, e);
    drop(pins);
    tree_unlock(cx, DIR_TREE);
    gate_release(cx);
    r?;
    // The in-memory directory sees the entry.
    cx.with("dir_create:install", |c| {
        c.store().dir.insert(
            id,
            super::DirEntry {
                root,
                dropping: false,
            },
        );
        Ok(())
    })?;
    Ok(())
}

/// Write one directory entry (unversioned: a replace, not a versioned
/// insert). Holds DIR_TREE's mutex and the gate (caller).
fn dir_put_locked<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    dir_root: u64,
    id: u64,
    root: u64,
    dropping: bool,
    e: u32,
) -> Result<(), Fail> {
    let ekey = id.to_be_bytes().to_vec();
    let mut val = Vec::new();
    val.extend_from_slice(&root.to_le_bytes());
    val.push(u8::from(dropping));
    let path = descend(cx, pins, dir_root, &ekey)?;
    let (leaf_lpid, leaf_bytes) = path
        .last()
        .cloned()
        .ok_or_else(|| Fail::Invalid("path".into()))?;
    let mut cells = leaf_cells(&leaf_bytes)?;
    let pos = cells
        .iter()
        .position(|(k, _)| k.as_slice() > ekey.as_slice())
        .unwrap_or(cells.len());
    let hit = pos < cells.len() && cells.get(pos).map(|c| c.0.as_slice()) == Some(ekey.as_slice());
    if hit {
        if let Some(c) = cells.get_mut(pos) {
            c.1 = val;
        }
    } else {
        cells.insert(pos, (ekey, val));
    }
    if used(&cells) > u_page() {
        return Err(Fail::Invalid("directory leaf overflow".into()));
    }
    store_page(
        cx,
        leaf_lpid,
        page::build_leaf(leaf_lpid, e, &cells_refs(&cells)),
    )
}

/// Read the whole directory from its tree at `dir_root` (open/recovery;
/// the caller passes the parsed meta's root — the host's store may not be
/// installed yet). Quiescent caller only (open's single task).
pub fn dir_load<H: StoreHost>(
    cx: &Cx<'_, H>,
    dir_root: u64,
) -> Result<BTreeMap<u64, super::DirEntry>, Fail> {
    if dir_root == 0 {
        return Ok(BTreeMap::new());
    }
    let raw = raw_cells(cx, dir_root, false)?;
    let mut out = BTreeMap::new();
    for (k, v) in raw {
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
            super::DirEntry {
                root: u64::from_le_bytes(rb),
                dropping: v.get(8) == Some(&1),
            },
        );
    }
    Ok(out)
}

/// `DropTree(id)` (§6.8): set `dropping`, free the tree's pages bottom-up
/// in bounded gated steps (one leaf per step, the root last), then remove
/// the entry. DROP-ROOT-FIRST frees the root first (the mutant's leak:
/// amcheck's LPID exactness fails).
pub fn dir_drop_tree<H: StoreHost>(cx: &Cx<'_, H>, id: u64) -> Result<bool, Fail> {
    let entry = cx.with("drop:entry", |c| Ok(c.store().dir.get(&id).copied()))?;
    let Some(entry) = entry else {
        return Ok(false); // dropping an absent id does nothing
    };
    if !entry.dropping {
        // Step 1: the entry becomes `dropping` in one directory mutation.
        gate_shared(cx)?;
        if let Err(e2) = tree_lock(cx, DIR_TREE) {
            gate_release(cx);
            return Err(e2);
        }
        let mut pins = PinGuard::new(cx);
        let dir_root = cx.with("drop:dr", |c| Ok(c.store().meta.dir_root))?;
        let e = cx.with("drop:epoch", |c| Ok(c.store().e))?;
        let w = dir_put_locked(cx, &mut pins, dir_root, id, entry.root, true, e);
        drop(pins);
        tree_unlock(cx, DIR_TREE);
        gate_release(cx);
        w?;
        cx.with("drop:mark", |c| {
            if let Some(en) = c.store().dir.get_mut(&id) {
                en.dropping = true;
            }
            Ok(())
        })?;
    }
    if cx.with("drop:mut", |c| {
        Ok(matches!(c.store().cfg.mutant, Some(Mutant::DropRootFirst)))
    })? {
        // DROP-ROOT-FIRST: free the root immediately and remove the
        // entry (the children leak; I-LPID-EXACT fails).
        gate_shared(cx)?;
        if let Err(e2) = tree_lock(cx, id) {
            gate_release(cx);
            return Err(e2);
        }
        let root = cx.with("drop:root2", |c| {
            Ok(c.store().dir.get(&id).map(|en| en.root).unwrap_or(0))
        })?;
        free_lpid(cx, root)?;
        tree_unlock(cx, id);
        gate_release(cx);
        dir_remove_entry(cx, id)?;
        return Ok(true);
    }
    // Step 2: the bounded walk, one gated step per leaf, the root last.
    loop {
        let root = cx.with("drop:root3", |c| {
            Ok(c.store().dir.get(&id).map(|en| en.root).unwrap_or(0))
        })?;
        gate_shared(cx)?;
        if let Err(e2) = tree_lock(cx, id) {
            gate_release(cx);
            return Err(e2);
        }
        let mut pins = PinGuard::new(cx);
        let walk = drop_walk_step(cx, &mut pins, root);
        drop(pins);
        tree_unlock(cx, id);
        gate_release(cx);
        if walk?.is_none() {
            break;
        }
    }
    // Step 3: remove the entry.
    dir_remove_entry(cx, id)?;
    Ok(true)
}
/// One DropTree walk step: free the leftmost leaf (the root last).
/// Freeing an only child empties its parent, which is freed in the same
/// step, cascading upward; the root is freed only when nothing remains
/// (I-LPID-EXACT: every unlink and its free happen together).
/// `Ok(None)` = the tree is gone.
fn drop_walk_step<H: StoreHost>(
    cx: &Cx<'_, H>,
    pins: &mut PinGuard<'_, H>,
    root: u64,
) -> Result<Option<()>, Fail> {
    let e = cx.with("dropwalk:epoch", |c| Ok(c.store().e))?;
    let path = descend(cx, pins, root, &[])?;
    if path.len() == 1 {
        free_lpid(cx, root)?;
        return Ok(None);
    }
    let mut level = path.len() - 1;
    loop {
        let child_lpid = path
            .get(level)
            .map(|(l, _)| *l)
            .ok_or_else(|| Fail::Invalid("path".to_owned()))?;
        let (parent_lpid, parent_bytes) = path
            .get(level - 1)
            .cloned()
            .ok_or_else(|| Fail::Invalid("path".to_owned()))?;
        let (mut seps, right) = inner_seps(&parent_bytes)?;
        match seps.iter().position(|(c, _)| *c == child_lpid) {
            Some(idx) => {
                seps.remove(idx);
                store_page(
                    cx,
                    parent_lpid,
                    page::build_inner(parent_lpid, e, &seps_refs(&seps), right),
                )?;
                free_lpid(cx, child_lpid)?;
                return Ok(Some(()));
            }
            None if right == child_lpid => {
                if level == 1 {
                    // The parent is the root and the tree is gone.
                    free_lpid(cx, child_lpid)?;
                    free_lpid(cx, root)?;
                    return Ok(None);
                }
                // An only child: free it and the now-empty parent, then
                // unlink the parent one level up.
                free_lpid(cx, child_lpid)?;
                free_lpid(cx, parent_lpid)?;
                level -= 1;
            }
            None => {
                return Err(Fail::Corrupt("leaf not found under parent".into()));
            }
        }
    }
}

/// Remove a directory entry and the in-memory dir record (idempotent).
fn dir_remove_entry<H: StoreHost>(cx: &Cx<'_, H>, id: u64) -> Result<(), Fail> {
    gate_shared(cx)?;
    if let Err(e) = tree_lock(cx, DIR_TREE) {
        gate_release(cx);
        return Err(e);
    }
    let mut pins = PinGuard::new(cx);
    let dir_root = cx.with("drop:dr2", |c| Ok(c.store().meta.dir_root))?;
    let e = cx.with("drop:epoch2", |c| Ok(c.store().e))?;
    let ekey = id.to_be_bytes().to_vec();
    let path = descend(cx, &mut pins, dir_root, &ekey)?;
    let (leaf_lpid, leaf_bytes) = path
        .last()
        .cloned()
        .ok_or_else(|| Fail::Invalid("path".into()))?;
    let cells = leaf_cells(&leaf_bytes)?;
    let kept: Cells = cells
        .iter()
        .filter(|c| c.0.as_slice() != ekey.as_slice())
        .cloned()
        .collect();
    if kept.len() != cells.len() {
        store_page(
            cx,
            leaf_lpid,
            page::build_leaf(leaf_lpid, e, &cells_refs(&kept)),
        )?;
    }
    drop(pins);
    tree_unlock(cx, DIR_TREE);
    gate_release(cx);
    cx.with("drop:rm", |c| {
        c.store().dir.remove(&id);
        Ok(())
    })?;
    Ok(())
}

/// Resume `dropping` walks at open (§14 step 6). Returns the resumed ids.
pub fn resume_drops<H: StoreHost>(cx: &Cx<'_, H>) -> Result<Vec<u64>, Fail> {
    let ids: Vec<u64> = cx.with("resume_drops", |c| {
        Ok(c.store()
            .dir
            .iter()
            .filter(|(_, en)| en.dropping)
            .map(|(id, _)| *id)
            .collect())
    })?;
    for id in ids.iter().copied() {
        dir_drop_tree(cx, id)?;
    }
    Ok(ids)
}

/// A tree's root LPID (step read).
pub fn dir_root_of<H: StoreHost>(cx: &Cx<'_, H>, tree: u64) -> Result<u64, Fail> {
    cx.with("dir_root_of", |c| {
        c.store()
            .dir
            .get(&tree)
            .map(|en| en.root)
            .ok_or_else(|| Fail::Corrupt(format!("tree {tree} missing in directory")))
    })
}

/// Install a directory entry in memory (open/recovery replay of
/// CreateTree).
pub fn dir_install<H: StoreHost>(cx: &Cx<'_, H>, id: u64, entry: super::DirEntry) {
    cx.with("dir_install", |c| {
        c.store().dir.insert(id, entry);
        Ok(())
    })
    .ok();
}

/// The catalog tree's root (tree id 1).
pub fn catalog_root(st: &StoreInner) -> u64 {
    st.dir.get(&CATALOG).map(|en| en.root).unwrap_or(0)
}

/// The persisted map radix's depth (Q16), from `next_lpid` and the
/// configured leaf cap.
pub fn map_depth(st: &StoreInner) -> u8 {
    let cap = st.cfg.map_leaf_cap;
    let n = st.meta.next_lpid.div_ceil(cap);
    let fan = ((page::PAGE - page::HDR) / page::MAP_ENTRY) as u64;
    let mut depth = 1u8;
    let mut level = n;
    while level > 1 {
        level = level.div_ceil(fan);
        depth += 1;
    }
    depth
}

/// The resume key strictly after `k` (an appended 0xFF exceeds every
/// extension of k in memcmp order).
fn next_after(k: Option<Vec<u8>>) -> Vec<u8> {
    let mut v = k.unwrap_or_default();
    v.push(0xFF);
    v
}
