#![deny(clippy::indexing_slicing)]
//! The E0 slotted page (card X-0 Work 4; PLAN-v3 §3.3 "leaves and inner
//! nodes use a slotted layout").
//!
//! Layout, all little-endian:
//!
//! ```text
//! offset  size  field
//!      0     1  kind (0 = leaf, 1 = inner)
//!      1     1  reserved (0)
//!      2     2  nslots
//!      4     2  free_start   first byte after the slot array
//!      6     2  free_end     first byte of the cell area
//!      8     8  right-most child page id (inner pages only)
//!     16   ...  slot array: (offset u16, len u16) per cell, growing up
//! free_end      cell area, cells growing DOWN from the page end
//! ```
//!
//! Leaf cell: `klen u16, vlen u32, key, value`. Inner cell:
//! `klen u16, child u64, key`. All accessors are free functions over
//! `&[u8]`/`&mut [u8]`.
//!
//! Invariants owned here:
//! - P-PARSE: every accessor validates the header and the slot/cell bounds
//!   and returns [`StoreError::Corrupt`] on bad bytes — no indexing panic
//!   is possible anywhere in this module (`clippy::indexing_slicing` is
//!   denied at module level, PLAN-v3 §6.1).
//! - P-LAYOUT: `HEADER + SLOT*nslots <= free_start <= free_end <= len`,
//!   every cell lies inside `[free_end, len)` and cells never overlap.
//! - P-ORDER: slots are stored in ascending key order (leaf) / separator
//!   order (inner). Removing a cell clears its slot; the hole in the cell
//!   area is reclaimed by [`compact`], which the insert path runs lazily
//!   when raw space runs out.
//! - P-SPLIT: [`leaf_split`] and [`inner_split`] partition by cell bytes so
//!   that both halves can always accept one more maximum-size cell; the
//!   separator of a leaf split is the first key of the right page (card
//!   X-0 Work 5), and an inner split moves its middle separator up.

use crate::store::{PageId, StoreError};

/// Header size in bytes.
pub const HEADER: usize = 16;
/// One slot: `(offset u16, len u16)`.
pub const SLOT: usize = 4;
/// Leaf cell header: `klen u16, vlen u32`.
const LEAF_HDR: usize = 6;
/// Inner cell header: `klen u16, child u64`.
const INNER_HDR: usize = 10;

/// Largest key accepted by the tree (leaf and inner cells).
///
/// Chosen so that one worst-case leaf cell plus its slot
/// (`MAX_KEY + MAX_VALUE + LEAF_HDR + SLOT` = 4058 bytes) fits **twice**
/// into the smallest accepted page: that is what makes P-SPLIT always
/// satisfiable without overflow pages.
pub const MAX_KEY: usize = 2000;
/// Largest value accepted by the tree (card X-0 Work 5; no overflow pages
/// in the spike, so anything larger is `ValueTooLarge`).
pub const MAX_VALUE: usize = 2048;
/// Worst-case cost of a leaf cell including its slot.
pub const MAX_LEAF_COST: usize = SLOT + LEAF_HDR + MAX_KEY + MAX_VALUE;
/// Worst-case cost of an inner cell including its slot.
pub const MAX_INNER_COST: usize = SLOT + INNER_HDR + MAX_KEY + 8;
/// Smallest page the format can split safely: the header plus two
/// worst-case leaf cells.
pub const MIN_PAGE: usize = HEADER + 2 * MAX_LEAF_COST;

/// Page kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PageKind {
    /// A leaf holds key/value cells.
    Leaf,
    /// An inner page holds (child, separator) cells plus a right-most child.
    Inner,
}

fn corrupt(msg: &str) -> StoreError {
    StoreError::Corrupt(msg.to_owned())
}

fn u16_rd(buf: &[u8], off: usize) -> Result<u16, StoreError> {
    let b = buf
        .get(off..off + 2)
        .ok_or_else(|| corrupt("u16 read out of bounds"))?;
    let mut a = [0u8; 2];
    a.copy_from_slice(b);
    Ok(u16::from_le_bytes(a))
}

fn u32_rd(buf: &[u8], off: usize) -> Result<u32, StoreError> {
    let b = buf
        .get(off..off + 4)
        .ok_or_else(|| corrupt("u32 read out of bounds"))?;
    let mut a = [0u8; 4];
    a.copy_from_slice(b);
    Ok(u32::from_le_bytes(a))
}

fn u64_rd(buf: &[u8], off: usize) -> Result<u64, StoreError> {
    let b = buf
        .get(off..off + 8)
        .ok_or_else(|| corrupt("u64 read out of bounds"))?;
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    Ok(u64::from_le_bytes(a))
}

fn u16_wr(buf: &mut [u8], off: usize, v: u16) -> Result<(), StoreError> {
    let b = buf
        .get_mut(off..off + 2)
        .ok_or_else(|| corrupt("u16 write out of bounds"))?;
    b.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn u32_wr(buf: &mut [u8], off: usize, v: u32) -> Result<(), StoreError> {
    let b = buf
        .get_mut(off..off + 4)
        .ok_or_else(|| corrupt("u32 write out of bounds"))?;
    b.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

fn u64_wr(buf: &mut [u8], off: usize, v: u64) -> Result<(), StoreError> {
    let b = buf
        .get_mut(off..off + 8)
        .ok_or_else(|| corrupt("u64 write out of bounds"))?;
    b.copy_from_slice(&v.to_le_bytes());
    Ok(())
}

/// The validated header of a page.
#[derive(Debug, Clone, Copy)]
pub struct Header {
    /// Page kind.
    pub kind: PageKind,
    /// Number of slots/cells.
    pub nslots: usize,
    /// First byte after the slot array.
    pub free_start: usize,
    /// First byte of the cell area.
    pub free_end: usize,
    /// Right-most child (only meaningful for inner pages).
    pub right_child: PageId,
}

/// Parse and validate the header (P-PARSE, P-LAYOUT).
pub fn header(buf: &[u8]) -> Result<Header, StoreError> {
    let len = buf.len();
    if len < HEADER {
        return Err(corrupt("page shorter than the header"));
    }
    let kind = match buf.first().copied().unwrap_or(255) {
        0 => PageKind::Leaf,
        1 => PageKind::Inner,
        other => return Err(corrupt(&format!("unknown page kind {other}"))),
    };
    let nslots = u16_rd(buf, 2)? as usize;
    let free_start = u16_rd(buf, 4)? as usize;
    let free_end = u16_rd(buf, 6)? as usize;
    let right_child = PageId(u64_rd(buf, 8)?);
    if HEADER + SLOT * nslots > free_start {
        return Err(corrupt("slot array overlaps the cell area"));
    }
    if free_start > free_end || free_end > len {
        return Err(corrupt("free space out of order"));
    }
    Ok(Header {
        kind,
        nslots,
        free_start,
        free_end,
        right_child,
    })
}

/// Initialize an empty page of the given kind.
pub fn init(buf: &mut [u8], kind: PageKind) -> Result<(), StoreError> {
    if buf.len() < MIN_PAGE || buf.len() > u16::MAX as usize {
        return Err(corrupt("page size outside the accepted range"));
    }
    let k = match kind {
        PageKind::Leaf => 0u8,
        PageKind::Inner => 1u8,
    };
    if let Some(b) = buf.get_mut(0) {
        *b = k;
    }
    if let Some(b) = buf.get_mut(1) {
        *b = 0;
    }
    u16_wr(buf, 2, 0)?;
    u16_wr(buf, 4, HEADER as u16)?;
    u16_wr(buf, 6, buf.len() as u16)?;
    u64_wr(buf, 8, 0)?;
    Ok(())
}

/// The page kind.
pub fn kind(buf: &[u8]) -> Result<PageKind, StoreError> {
    Ok(header(buf)?.kind)
}

/// Raw slot `i` as `(offset, len)` within the cell area (P-LAYOUT).
pub fn slot(buf: &[u8], i: usize) -> Result<(usize, usize), StoreError> {
    let h = header(buf)?;
    if i >= h.nslots {
        return Err(corrupt("slot index out of range"));
    }
    let off = u16_rd(buf, HEADER + SLOT * i)? as usize;
    let len = u16_rd(buf, HEADER + SLOT * i + 2)? as usize;
    if off < h.free_end || off + len > buf.len() {
        return Err(corrupt("cell outside the cell area"));
    }
    Ok((off, len))
}

/// Leaf cell `i` as `(key, value)` byte strings.
pub fn leaf_cell(buf: &[u8], i: usize) -> Result<(&[u8], &[u8]), StoreError> {
    let (off, len) = slot(buf, i)?;
    let cell = buf
        .get(off..off + len)
        .ok_or_else(|| corrupt("cell out of bounds"))?;
    let klen = u16_rd(cell, 0)? as usize;
    let vlen = u32_rd(cell, 2)? as usize;
    if LEAF_HDR + klen + vlen != len {
        return Err(corrupt("leaf cell length mismatch"));
    }
    let key = cell
        .get(LEAF_HDR..LEAF_HDR + klen)
        .ok_or_else(|| corrupt("key out of bounds"))?;
    let val = cell
        .get(LEAF_HDR + klen..LEAF_HDR + klen + vlen)
        .ok_or_else(|| corrupt("value out of bounds"))?;
    Ok((key, val))
}

/// Inner cell `i` as `(child page id, separator key)`.
pub fn inner_cell(buf: &[u8], i: usize) -> Result<(PageId, &[u8]), StoreError> {
    let (off, len) = slot(buf, i)?;
    let cell = buf
        .get(off..off + len)
        .ok_or_else(|| corrupt("cell out of bounds"))?;
    let klen = u16_rd(cell, 0)? as usize;
    let child = PageId(u64_rd(cell, 2)?);
    if INNER_HDR + klen != len {
        return Err(corrupt("inner cell length mismatch"));
    }
    let key = cell
        .get(INNER_HDR..)
        .ok_or_else(|| corrupt("key out of bounds"))?;
    Ok((child, key))
}

/// Bytes free between the slot array and the cell area (holes not included).
pub fn free_bytes(buf: &[u8]) -> Result<usize, StoreError> {
    let h = header(buf)?;
    Ok(h.free_end - h.free_start)
}

/// Total hole bytes: cell-area bytes not covered by any live cell. Removed
/// cells leave holes until [`compact`] reclaims them.
pub fn hole_bytes(buf: &[u8]) -> Result<usize, StoreError> {
    let h = header(buf)?;
    let mut live = 0usize;
    for i in 0..h.nslots {
        let (_, len) = slot(buf, i)?;
        live += len;
    }
    Ok(buf.len() - h.free_end - live)
}

/// Right-most child of an inner page.
pub fn right_child(buf: &[u8]) -> Result<PageId, StoreError> {
    let h = header(buf)?;
    if h.kind != PageKind::Inner {
        return Err(corrupt("right-most child of a leaf"));
    }
    Ok(h.right_child)
}

/// Does this page have room for one more worst-case cell of its kind?
///
/// This is the latch-coupling safety test of card X-0 Work 5: a writer may
/// release an ancestor's latch only once the child passes this test.
pub fn is_safe(buf: &[u8]) -> Result<bool, StoreError> {
    let h = header(buf)?;
    let need = match h.kind {
        PageKind::Leaf => MAX_LEAF_COST,
        PageKind::Inner => MAX_INNER_COST,
    };
    Ok(h.free_end - h.free_start >= need)
}

/// Binary-search a leaf for `key`: `Ok(i)` if it equals slot `i`'s key,
/// `Err(pos)` with the insertion position otherwise (P-ORDER).
pub fn leaf_find(buf: &[u8], key: &[u8]) -> Result<Result<usize, usize>, StoreError> {
    let h = header(buf)?;
    if h.kind != PageKind::Leaf {
        return Err(corrupt("leaf_find on an inner page"));
    }
    let mut lo = 0usize;
    let mut hi = h.nslots;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (midkey, _) = leaf_cell(buf, mid)?;
        if midkey < key {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo < h.nslots {
        let (k, _) = leaf_cell(buf, lo)?;
        if k == key {
            return Ok(Ok(lo));
        }
    }
    Ok(Err(lo))
}

/// Route `key` through an inner page: returns the child index
/// (`0..=nslots`; `nslots` means the right-most child) and the child id.
///
/// Routing rule: the first cell whose separator is strictly greater than
/// the key wins; a key equal to a separator belongs to the subtree at or
/// right of it, because separators are always the first key of the page to
/// their right (P-SPLIT).
pub fn inner_route(buf: &[u8], key: &[u8]) -> Result<(usize, PageId), StoreError> {
    let h = header(buf)?;
    if h.kind != PageKind::Inner {
        return Err(corrupt("inner_route on a leaf"));
    }
    let mut lo = 0usize;
    let mut hi = h.nslots;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (_, midsep) = inner_cell(buf, mid)?;
        if midsep > key {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    if lo < h.nslots {
        let (child, _) = inner_cell(buf, lo)?;
        Ok((lo, child))
    } else {
        Ok((lo, h.right_child))
    }
}

/// Find the cell whose child pointer is `child`, if present. Inner pages
/// point at each child at most once.
pub fn inner_child_index(buf: &[u8], child: PageId) -> Result<Option<usize>, StoreError> {
    let h = header(buf)?;
    if h.kind != PageKind::Inner {
        return Err(corrupt("inner_child_index on a leaf"));
    }
    for i in 0..h.nslots {
        if inner_cell(buf, i)?.0 == child {
            return Ok(Some(i));
        }
    }
    Ok(None)
}

/// Replace the separator of cell `i`, keeping its position and child. The
/// page is compacted if the new separator is longer; returns `false` only
/// if even compaction cannot fit it (practically impossible: the old cell
/// is removed first). On `false` the cell is left as it was.
pub fn inner_replace_sep(buf: &mut [u8], i: usize, sep: &[u8]) -> Result<bool, StoreError> {
    let (child, old) = inner_cell(buf, i)?;
    let old = old.to_vec();
    if old == sep {
        return Ok(true);
    }
    remove_slot(buf, i)?;
    if inner_insert(buf, i, child, sep)? {
        return Ok(true);
    }
    // restore the original cell; the caller treats this as an error
    if !inner_insert(buf, i, child, &old)? {
        return Err(corrupt("could not restore an inner cell"));
    }
    Ok(false)
}

/// Binary-search an inner page for `sep`: `Ok(i)` if cell `i`'s separator
/// equals it, `Err(pos)` with the insertion position otherwise. The tree
/// uses this to place a new child pointer after a split.
pub fn inner_cell_position(buf: &[u8], sep: &[u8]) -> Result<Result<usize, usize>, StoreError> {
    let h = header(buf)?;
    if h.kind != PageKind::Inner {
        return Err(corrupt("inner_cell_position on a leaf"));
    }
    let mut lo = 0usize;
    let mut hi = h.nslots;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let (_, msep) = inner_cell(buf, mid)?;
        if msep < sep {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if lo < h.nslots {
        let (_, s) = inner_cell(buf, lo)?;
        if s == sep {
            return Ok(Ok(lo));
        }
    }
    Ok(Err(lo))
}

/// Write leaf cell `(key, value)` at `pos` (its sorted position; the key
/// must not already be present). Compacts first when only holes are
/// missing. Returns `false` when there is no room even after compaction —
/// the caller must split.
pub fn leaf_insert(
    buf: &mut [u8],
    pos: usize,
    key: &[u8],
    value: &[u8],
) -> Result<bool, StoreError> {
    let need = SLOT + LEAF_HDR + key.len() + value.len();
    if !has_room(buf, need)? {
        compact(buf)?;
        if !has_room(buf, need)? {
            return Ok(false);
        }
    }
    let h = header(buf)?;
    let cell_len = LEAF_HDR + key.len() + value.len();
    let cell_off = h.free_end - cell_len;
    {
        let cell = buf
            .get_mut(cell_off..cell_off + cell_len)
            .ok_or_else(|| corrupt("cell write out of bounds"))?;
        u16_wr(cell, 0, key.len() as u16)?;
        u32_wr(cell, 2, value.len() as u32)?;
        cell
            .get_mut(LEAF_HDR..LEAF_HDR + key.len())
            .ok_or_else(|| corrupt("key write out of bounds"))?
            .copy_from_slice(key);
        cell
            .get_mut(LEAF_HDR + key.len()..)
            .ok_or_else(|| corrupt("value write out of bounds"))?
            .copy_from_slice(value);
    }
    shift_slots(buf, pos, h.nslots, 1)?;
    u16_wr(buf, HEADER + SLOT * pos, cell_off as u16)?;
    u16_wr(buf, HEADER + SLOT * pos + 2, cell_len as u16)?;
    u16_wr(buf, 2, (h.nslots + 1) as u16)?;
    u16_wr(buf, 4, (h.free_start + SLOT) as u16)?;
    u16_wr(buf, 6, cell_off as u16)?;
    Ok(true)
}

/// Write inner cell `(child, sep)` at `pos` (its sorted position). Returns
/// `false` when there is no room even after compaction — the caller must
/// split.
pub fn inner_insert(
    buf: &mut [u8],
    pos: usize,
    child: PageId,
    sep: &[u8],
) -> Result<bool, StoreError> {
    let need = SLOT + INNER_HDR + sep.len();
    if !has_room(buf, need)? {
        compact(buf)?;
        if !has_room(buf, need)? {
            return Ok(false);
        }
    }
    let h = header(buf)?;
    let cell_len = INNER_HDR + sep.len();
    let cell_off = h.free_end - cell_len;
    {
        let cell = buf
            .get_mut(cell_off..cell_off + cell_len)
            .ok_or_else(|| corrupt("cell write out of bounds"))?;
        u16_wr(cell, 0, sep.len() as u16)?;
        u64_wr(cell, 2, child.0)?;
        cell
            .get_mut(INNER_HDR..)
            .ok_or_else(|| corrupt("separator write out of bounds"))?
            .copy_from_slice(sep);
    }
    shift_slots(buf, pos, h.nslots, 1)?;
    u16_wr(buf, HEADER + SLOT * pos, cell_off as u16)?;
    u16_wr(buf, HEADER + SLOT * pos + 2, cell_len as u16)?;
    u16_wr(buf, 2, (h.nslots + 1) as u16)?;
    u16_wr(buf, 4, (h.free_start + SLOT) as u16)?;
    u16_wr(buf, 6, cell_off as u16)?;
    Ok(true)
}

/// Remove slot `i` from a leaf. The cell's bytes stay as a hole until
/// [`compact`] runs (P-LAYOUT).
pub fn leaf_remove(buf: &mut [u8], i: usize) -> Result<(), StoreError> {
    remove_slot(buf, i)
}

/// Remove slot `i` from an inner page. Unused by the tree (inner cells are
/// never removed in this spike — no merges); part of the format contract.
pub fn inner_remove(buf: &mut [u8], i: usize) -> Result<(), StoreError> {
    remove_slot(buf, i)
}

/// Set the right-most child of an inner page.
pub fn set_right_child(buf: &mut [u8], child: PageId) -> Result<(), StoreError> {
    if header(buf)?.kind != PageKind::Inner {
        return Err(corrupt("right-most child of a leaf"));
    }
    u64_wr(buf, 8, child.0)
}

fn remove_slot(buf: &mut [u8], i: usize) -> Result<(), StoreError> {
    let h = header(buf)?;
    if i >= h.nslots {
        return Err(corrupt("slot index out of range"));
    }
    shift_slots(buf, i + 1, h.nslots, -1)?;
    u16_wr(buf, 2, (h.nslots - 1) as u16)?;
    u16_wr(buf, 4, (h.free_start - SLOT) as u16)?;
    Ok(())
}

/// Shift the slot entries in `[from, to)` by one slot: `delta = 1` moves
/// them up (opening a hole at `from`), `delta = -1` moves them down over
/// the entry at `from - 1`.
fn shift_slots(buf: &mut [u8], from: usize, to: usize, delta: i32) -> Result<(), StoreError> {
    if delta == 0 || from >= to {
        return Ok(());
    }
    let base = HEADER + SLOT * from;
    let end = HEADER + SLOT * to;
    if delta > 0 {
        buf.copy_within(base..end, base + SLOT);
    } else {
        buf.copy_within(base..end, base - SLOT);
    }
    Ok(())
}

fn has_room(buf: &[u8], need: usize) -> Result<bool, StoreError> {
    Ok(free_bytes(buf)? >= need)
}

/// Defragment: rewrite all cells contiguously at the page end, in slot
/// order, and rebuild the slot array (card X-0 Work 4).
pub fn compact(buf: &mut [u8]) -> Result<(), StoreError> {
    let h = header(buf)?;
    let ps = buf.len();
    let mut cells: Vec<(usize, usize)> = Vec::with_capacity(h.nslots);
    for i in 0..h.nslots {
        cells.push(slot(buf, i)?);
    }
    // Place the cells from the page end downwards, in slot order, through a
    // scratch copy: source and target ranges may overlap.
    let mut scratch = vec![0u8; ps];
    let mut cur = ps;
    for &(off, len) in cells.iter() {
        cur -= len;
        let src = buf
            .get(off..off + len)
            .ok_or_else(|| corrupt("compaction source out of bounds"))?;
        let dst = scratch
            .get_mut(cur..cur + len)
            .ok_or_else(|| corrupt("compaction target out of bounds"))?;
        dst.copy_from_slice(src);
    }
    let cell_area = scratch
        .get(cur..)
        .ok_or_else(|| corrupt("compaction scratch out of bounds"))?;
    let target = buf
        .get_mut(cur..)
        .ok_or_else(|| corrupt("compaction target out of bounds"))?;
    target.copy_from_slice(cell_area);
    // Cells were placed from the page end downwards in slot order, so slot
    // i's offset counts down from the end with the running length.
    let mut top = ps;
    for (i, &(_, len)) in cells.iter().enumerate() {
        top -= len;
        u16_wr(buf, HEADER + SLOT * i, top as u16)?;
        u16_wr(buf, HEADER + SLOT * i + 2, len as u16)?;
    }
    debug_assert_eq!(top, cur);
    u16_wr(buf, 2, h.nslots as u16)?;
    u16_wr(buf, 4, (HEADER + SLOT * h.nslots) as u16)?;
    u16_wr(buf, 6, cur as u16)?;
    Ok(())
}

/// A leaf cell lifted out of a page (for splitting and rebuilding).
type LeafBytes = (Vec<u8>, Vec<u8>);
/// An inner cell lifted out of a page.
type InnerBytes = (PageId, Vec<u8>);

/// Split a full leaf, including the pending insert `(key, value)` that did
/// not fit, into `left` (rewritten) and `right` (uninitialized).
///
/// Both pages end up non-empty; every cell including the pending one lands
/// on exactly one side. Returns the separator, which is **the first key of
/// the right page** (card X-0 Work 5; the SPLIT-OFF-BY-ONE mutation of the
/// card's evidence list is exactly this choice being wrong).
///
/// The split point minimises the byte difference between the halves, which
/// keeps P-SPLIT satisfiable: each side's cost stays near half the total
/// and the page always had room for the cells it keeps plus the pending
/// one, since the pre-split total was below capacity.
pub fn leaf_split(
    left: &mut [u8],
    right: &mut [u8],
    pending: (&[u8], &[u8]),
) -> Result<Vec<u8>, StoreError> {
    if header(left)?.kind != PageKind::Leaf {
        return Err(corrupt("leaf_split on an inner page"));
    }
    let mut cells: Vec<LeafBytes> = Vec::new();
    let h = header(left)?;
    for i in 0..h.nslots {
        let (k, v) = leaf_cell(left, i)?;
        cells.push((k.to_vec(), v.to_vec()));
    }
    let pos = match leaf_find(left, pending.0)? {
        Ok(_) => return Err(corrupt("pending insert already present")),
        Err(p) => p,
    };
    cells.insert(pos, (pending.0.to_vec(), pending.1.to_vec()));
    let n = cells.len();
    if n < 2 {
        return Err(corrupt("leaf split with fewer than two cells"));
    }
    let costs: Vec<usize> = cells
        .iter()
        .map(|(k, v)| SLOT + LEAF_HDR + k.len() + v.len())
        .collect();
    // m = number of cells kept on the left; both sides must be non-empty.
    let m = split_at_half(&costs, 1, n - 1)?;
    init(right, PageKind::Leaf)?;
    rebuild_leaf(left, cells.iter().take(m))?;
    rebuild_leaf(right, cells.iter().skip(m))?;
    let sep = cells
        .get(m)
        .map(|(k, _)| k.clone())
        .ok_or_else(|| corrupt("split point out of range"))?;
    Ok(sep)
}

/// Split a full inner page, including the pending `(child, sep)` cell that
/// did not fit, into `left` (rewritten) and `right` (uninitialized).
///
/// The separator at the split point moves up (returned); its child becomes
/// the right-most child of the left page; the old right-most child carries
/// over to the right page. Both sides keep at least one cell.
pub fn inner_split(
    left: &mut [u8],
    right: &mut [u8],
    pending: (PageId, &[u8]),
) -> Result<Vec<u8>, StoreError> {
    let h = header(left)?;
    if h.kind != PageKind::Inner {
        return Err(corrupt("inner_split on a leaf"));
    }
    let mut cells: Vec<InnerBytes> = Vec::new();
    for i in 0..h.nslots {
        let (child, key) = inner_cell(left, i)?;
        cells.push((child, key.to_vec()));
    }
    let mut pos = cells.len();
    for (i, (_, key)) in cells.iter().enumerate() {
        if key.as_slice() > pending.1 {
            pos = i;
            break;
        }
    }
    cells.insert(pos, (pending.0, pending.1.to_vec()));
    let n = cells.len();
    if n < 3 {
        return Err(corrupt("inner split with fewer than three cells"));
    }
    let costs: Vec<usize> = cells
        .iter()
        .map(|(_, k)| SLOT + INNER_HDR + k.len())
        .collect();
    let total: usize = costs.iter().sum();
    // s = cells kept on the left; cells[s] moves up; cells[s+1..] go right.
    // s ranges 1..=n-2 so both sides keep at least one cell.
    let mut best = 1usize;
    let mut best_diff = usize::MAX;
    let mut prefix = 0usize;
    for i in 0..n {
        if i >= 1 && i <= n - 2 {
            let right = total - prefix - costs.get(i).copied().unwrap_or(0);
            let diff = prefix.abs_diff(right);
            if diff < best_diff {
                best_diff = diff;
                best = i;
            }
        }
        prefix += costs.get(i).copied().unwrap_or(0);
    }
    let s = best;
    let (up_child, up_sep) = cells
        .get(s)
        .map(|(c, k)| (*c, k.clone()))
        .ok_or_else(|| corrupt("inner split point out of range"))?;
    let old_right = h.right_child;
    init(right, PageKind::Inner)?;
    rebuild_inner(left, cells.iter().take(s), up_child)?;
    rebuild_inner(right, cells.iter().skip(s + 1), old_right)?;
    Ok(up_sep)
}

/// Pick `m` (cells for the left page) in `[lo, hi]` minimising the byte
/// difference `|left - right|`, with `left = sum(costs[..m])`.
fn split_at_half(costs: &[usize], lo: usize, hi: usize) -> Result<usize, StoreError> {
    let total: usize = costs.iter().sum();
    let mut best = lo;
    let mut best_diff = usize::MAX;
    let mut prefix = 0usize;
    for i in 0..=costs.len() {
        if i >= lo && i <= hi {
            let diff = prefix.abs_diff(total - prefix);
            if diff < best_diff {
                best_diff = diff;
                best = i;
            }
        }
        if i < costs.len() {
            prefix += costs.get(i).copied().unwrap_or(0);
        }
    }
    if best < lo || best > hi {
        return Err(corrupt("degenerate split window"));
    }
    Ok(best)
}

/// Rebuild a leaf page from cells that arrive in ascending key order.
fn rebuild_leaf<'a, I>(buf: &mut [u8], cells: I) -> Result<(), StoreError>
where
    I: Iterator<Item = &'a LeafBytes>,
{
    init(buf, PageKind::Leaf)?;
    for (k, v) in cells {
        let pos = match leaf_find(buf, k)? {
            Ok(_) => return Err(corrupt("duplicate key in rebuild")),
            Err(p) => p,
        };
        if !leaf_insert(buf, pos, k, v)? {
            return Err(corrupt("rebuild overflow"));
        }
    }
    Ok(())
}

/// Rebuild an inner page from cells that arrive in ascending separator
/// order, plus its right-most child.
fn rebuild_inner<'a, I>(buf: &mut [u8], cells: I, rightmost: PageId) -> Result<(), StoreError>
where
    I: Iterator<Item = &'a InnerBytes>,
{
    init(buf, PageKind::Inner)?;
    let mut last: Option<Vec<u8>> = None;
    for (child, sep) in cells {
        if let Some(l) = last.as_deref()
            && l >= sep.as_slice()
        {
            return Err(corrupt("inner rebuild out of order"));
        }
        last = Some(sep.clone());        let pos = header(buf)?.nslots;
        if !inner_insert(buf, pos, *child, sep)? {
            return Err(corrupt("inner rebuild overflow"));
        }
    }
    set_right_child(buf, rightmost)?;
    Ok(())
}
