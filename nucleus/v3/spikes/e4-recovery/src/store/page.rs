#![deny(clippy::indexing_slicing)]
// The unit `Err(())` of every decoder **is** XX001: the failure carries
// no payload (the caller adds its context when converting to
// `Fail::Corrupt`), so a custom error type would add nothing.
#![allow(clippy::result_unit_err)]
//! The page codec (S-STORE §2.1, §3.1, §3.3; card X-5 Work 3.1–3.2).
//!
//! Page size is 4096 everywhere. Slots 0–2 are fixed (page 0, metas);
//! every page in slots ≥ 3 carries the 24-byte §3.1 header:
//! `crc u32, kind u8, flags u8, _ u16, id u64, epoch u32, _ u32` — and the
//! crc covers bytes `4..page_size`. Kinds: 3 Leaf, 4 Inner, 6 MapLeaf, 7
//! MapInner, 8 FreeSetLeaf, 9 FreeSetInner (5 Overflow unused: values are
//! always inline in this spike).
//!
//! Leaf/inner bodies are the e0-btree slotted layout
//! (from spikes/e0-btree/src/page.rs@391e816c; changed: kinds and the
//! §3.1 header added; the inner body keeps its rightmost child at body
//! offset 0, before the slot array, so the header's `id` field is not
//! overloaded): the slot array grows up and the cells grow down; leaf
//! cells are `klen u16, vlen u32, key, value`; inner cells are
//! `klen u16, child u64, key`.
//!
//! Invariants owned here:
//! - P-PARSE: every accessor validates bounds and returns `Err(())`
//!   (XX001) on bad bytes — decoders never panic and every malformed
//!   input is XX001 (§3.1).
//! - P-INTEGRITY: [`check`] performs §3.3's four checks against a
//!   reference `(slot, epoch, crc)` and expected id. Each of the four is
//!   its own mutant site (CRC-SKIP, REFCRC-SKIP, EPOCH-SKIP+REFCRC,
//!   ID-SKIP+REFCRC; the singles are also runnable for hit-rate
//!   reporting).

use crate::crc::crc32c;
use crate::store::Mutant;

/// The fixed page size of the spike (§2.1 minimum, so trees are deep with
/// little data).
pub const PAGE: usize = 4096;
/// The §3.1 header size.
pub const HDR: usize = 24;
/// One slot-array entry: `(offset u16, len u16)`.
pub const SLOT: usize = 4;
/// Leaf cell header: `klen u16, vlen u32`.
pub const LEAF_HDR: usize = 6;
/// Inner cell header: `klen u16, child u64`.
pub const INNER_HDR: usize = 10;
/// MapLeaf entry: `slot u64, epoch u32, crc u32` (§4.1).
pub const MAP_ENTRY: usize = 16;
/// The inner body's rightmost-child field: `u64` at body offset 0.
const INNER_RIGHT: usize = 8;

/// Page kinds (§3.1): a leaf of versioned entries.
pub const KIND_LEAF: u8 = 3;
/// Page kinds (§3.1): an inner tree page of separators.
pub const KIND_INNER: u8 = 4;
/// Page kinds (§3.1): a MapLeaf of the page-map radix (§4.1).
pub const KIND_MAP_LEAF: u8 = 6;
/// Page kinds (§3.1): a MapInner of the page-map radix (§4.1).
pub const KIND_MAP_INNER: u8 = 7;
/// Page kinds (§3.1): a FreeSetLeaf of the free-set radix (§4.3).
pub const KIND_FREE_LEAF: u8 = 8;
/// Page kinds (§3.1): a FreeSetInner of the free-set radix (§4.3).
pub const KIND_FREE_INNER: u8 = 9;

/// Largest accepted entry key (derived so ≥ 4 maximum cells fit; §3.2).
pub const K_MAX: usize = 512;
/// Largest accepted inline value (the workload uses 8–400 B; no overflow
/// chains in this spike, §3.2).
pub const V_INLINE: usize = 400;

/// A page reference: `(slot, epoch, crc)` (§3.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Ref {
    /// The slot the page image lives at.
    pub slot: u64,
    /// The content epoch of that image.
    pub epoch: u32,
    /// The header CRC of those exact bytes.
    pub crc: u32,
}

fn rd_u16(b: &[u8], off: usize) -> Result<u16, ()> {
    let s: [u8; 2] = b.get(off..off + 2).ok_or(())?.try_into().map_err(|_| ())?;
    Ok(u16::from_le_bytes(s))
}
fn rd_u32(b: &[u8], off: usize) -> Result<u32, ()> {
    let s: [u8; 4] = b.get(off..off + 4).ok_or(())?.try_into().map_err(|_| ())?;
    Ok(u32::from_le_bytes(s))
}
fn rd_u64(b: &[u8], off: usize) -> Result<u64, ()> {
    let s: [u8; 8] = b.get(off..off + 8).ok_or(())?.try_into().map_err(|_| ())?;
    Ok(u64::from_le_bytes(s))
}
fn wr_u16(b: &mut [u8], off: usize, v: u16) -> Option<()> {
    let s = b.get_mut(off..off + 2)?;
    s.copy_from_slice(&v.to_le_bytes());
    Some(())
}
fn wr_u32(b: &mut [u8], off: usize, v: u32) -> Option<()> {
    let s = b.get_mut(off..off + 4)?;
    s.copy_from_slice(&v.to_le_bytes());
    Some(())
}
fn wr_u64(b: &mut [u8], off: usize, v: u64) -> Option<()> {
    let s = b.get_mut(off..off + 8)?;
    s.copy_from_slice(&v.to_le_bytes());
    Some(())
}

/// Build a page: header + a slotted leaf body of the given cells
/// (`(key, value)` pairs, ascending). Returns the encoded page.
pub fn build_leaf(id: u64, epoch: u32, cells: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    let mut free_end = PAGE;
    let mut slots: Vec<(usize, usize)> = Vec::new();
    for (k, v) in cells {
        let len = LEAF_HDR + k.len() + v.len();
        free_end = free_end.saturating_sub(len);
        let c = p.get_mut(free_end..free_end + len);
        if let Some(c) = c {
            wr_u16(c, 0, k.len() as u16);
            wr_u32(c, 2, v.len() as u32);
            if let Some(kd) = c.get_mut(LEAF_HDR..LEAF_HDR + k.len()) {
                kd.copy_from_slice(k);
            }
            if let Some(vd) = c.get_mut(LEAF_HDR + k.len()..) {
                vd.copy_from_slice(v);
            }
        }
        slots.push((free_end, len));
    }
    // Body: `n u16` at 24, slot array from 26 up.
    wr_u16(&mut p, HDR, cells.len() as u16);
    let mut at = HDR + 2;
    for (off, len) in slots {
        wr_u16(&mut p, at, off as u16);
        wr_u16(&mut p, at + 2, len as u16);
        at += SLOT;
    }
    finish(&mut p, KIND_LEAF, id, epoch);
    p
}

/// Build a page: header + a slotted inner body of `(child, separator)`
/// cells plus the rightmost child (kept at body offset 0, before the slot
/// array, so it never collides with the header's `id`).
pub fn build_inner(id: u64, epoch: u32, seps: &[(u64, &[u8])], right: u64) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    let mut free_end = PAGE;
    let mut slots: Vec<(usize, usize)> = Vec::new();
    for (child, k) in seps {
        let len = INNER_HDR + k.len();
        free_end = free_end.saturating_sub(len);
        let c = p.get_mut(free_end..free_end + len);
        if let Some(c) = c {
            wr_u16(c, 0, k.len() as u16);
            wr_u64(c, 2, *child);
            if let Some(kd) = c.get_mut(INNER_HDR..) {
                kd.copy_from_slice(k);
            }
        }
        slots.push((free_end, len));
    }
    wr_u64(&mut p, HDR, right);
    wr_u16(&mut p, HDR + INNER_RIGHT, seps.len() as u16);
    let mut at = HDR + INNER_RIGHT + 2;
    for (off, len) in slots {
        wr_u16(&mut p, at, off as u16);
        wr_u16(&mut p, at + 2, len as u16);
        at += SLOT;
    }
    finish(&mut p, KIND_INNER, id, epoch);
    p
}

/// Stamp the header (kind, id, epoch) and the CRC over `4..PAGE`.
fn finish(p: &mut [u8], kind: u8, id: u64, epoch: u32) {
    if let Some(k) = p.get_mut(4) {
        *k = kind;
    }
    wr_u16(p, 6, 0);
    wr_u64(p, 8, id);
    wr_u32(p, 16, epoch);
    wr_u32(p, 20, 0);
    let crc = crc32c(0, p.get(4..).unwrap_or(&[]));
    wr_u32(p, 0, crc);
}

/// A decoded slotted body.
#[derive(Debug, Clone)]
pub struct Slotted {
    /// The page kind.
    pub kind: u8,
    /// The number of cells.
    pub n: usize,
}

/// The slot-array base of a slotted page by kind (`Err` on other kinds).
fn slot_base(kind: u8) -> Result<usize, ()> {
    match kind {
        KIND_LEAF => Ok(HDR + 2),
        KIND_INNER => Ok(HDR + INNER_RIGHT + 2),
        _ => Err(()),
    }
}

/// Parse and validate a slotted page's structure (P-PARSE): right length,
/// a tree kind, `n` in bounds, every cell inside the page and non-empty,
/// cells not overlapping the slot array, and cell offsets strictly
/// descending (the builder grows down).
pub fn slotted(p: &[u8]) -> Result<Slotted, ()> {
    if p.len() != PAGE {
        return Err(());
    }
    let kind = *p.get(4).ok_or(())?;
    let base = slot_base(kind)?;
    let n = rd_u16(p, base - 2)? as usize;
    if base + SLOT * n > PAGE {
        return Err(());
    }
    let mut prev_end = PAGE;
    for i in 0..n {
        let off = rd_u16(p, base + SLOT * i)? as usize;
        let len = rd_u16(p, base + SLOT * i + 2)? as usize;
        if off + len > PAGE || len < LEAF_HDR || off < base + SLOT * n || off >= prev_end {
            return Err(());
        }
        prev_end = off;
    }
    Ok(Slotted { kind, n })
}

/// Leaf cell `i` as `(key, value)`.
pub fn leaf_cell(p: &[u8], i: usize) -> Result<(&[u8], &[u8]), ()> {
    let kind = *p.get(4).ok_or(())?;
    if kind != KIND_LEAF {
        return Err(());
    }
    let base = slot_base(kind)?;
    let off = rd_u16(p, base + SLOT * i)? as usize;
    let len = rd_u16(p, base + SLOT * i + 2)? as usize;
    let cell = p.get(off..off + len).ok_or(())?;
    let klen = rd_u16(cell, 0)? as usize;
    let vlen = rd_u32(cell, 2)? as usize;
    if LEAF_HDR + klen + vlen != len {
        return Err(());
    }
    Ok((
        cell.get(LEAF_HDR..LEAF_HDR + klen).ok_or(())?,
        cell.get(LEAF_HDR + klen..).ok_or(())?,
    ))
}

/// Inner cell `i` as `(child, separator)`.
pub fn inner_cell(p: &[u8], i: usize) -> Result<(u64, &[u8]), ()> {
    let kind = *p.get(4).ok_or(())?;
    if kind != KIND_INNER {
        return Err(());
    }
    let base = slot_base(kind)?;
    let off = rd_u16(p, base + SLOT * i)? as usize;
    let len = rd_u16(p, base + SLOT * i + 2)? as usize;
    let cell = p.get(off..off + len).ok_or(())?;
    let klen = rd_u16(cell, 0)? as usize;
    let child = rd_u64(cell, 2)?;
    if INNER_HDR + klen != len {
        return Err(());
    }
    Ok((child, cell.get(INNER_HDR..).ok_or(())?))
}

/// The rightmost child of an inner page (body offset 0).
pub fn right_child(p: &[u8]) -> Result<u64, ()> {
    if p.get(4).copied() != Some(KIND_INNER) {
        return Err(());
    }
    rd_u64(p, HDR)
}

/// The header kind byte (§3.1: at offset 4, after the u32 crc).
pub fn header_kind(p: &[u8]) -> u8 {
    p.get(4).copied().unwrap_or(0)
}

/// The header CRC field.
pub fn header_crc(p: &[u8]) -> u32 {
    rd_u32(p, 0).unwrap_or(0)
}

/// The header id field.
pub fn header_id(p: &[u8]) -> u64 {
    rd_u64(p, 8).unwrap_or(0)
}

/// The header epoch field.
pub fn header_epoch(p: &[u8]) -> u32 {
    rd_u32(p, 16).unwrap_or(0)
}

/// §3.3's four checks on every read through a reference (P-INTEGRITY).
/// `expected_id` is the LPID for kinds 3–5 and the radix position for
/// kinds 6–9. Returns `Err(())` (XX001) on any mismatch; the bytes are
/// never served. Each check is a separate mutant site.
pub fn check(p: &[u8], r: &Ref, expected_id: u64, mutant: Option<Mutant>) -> Result<(), ()> {
    if p.len() != PAGE {
        return Err(());
    }
    let m = mutant;
    // (1) the header crc equals the CRC32C of the bytes.
    if !matches!(m, Some(Mutant::CrcSkip)) && crc32c(0, p.get(4..).unwrap_or(&[])) != rd_u32(p, 0)?
    {
        return Err(());
    }
    // (2) the header crc equals the reference's crc.
    if !matches!(
        m,
        Some(Mutant::RefCrcSkip) | Some(Mutant::EpochSkipRefCrc) | Some(Mutant::IdSkipRefCrc)
    ) && rd_u32(p, 0)? != r.crc
    {
        return Err(());
    }
    // (3) the header id equals the expected id.
    if !matches!(m, Some(Mutant::IdSkip) | Some(Mutant::IdSkipRefCrc))
        && rd_u64(p, 8)? != expected_id
    {
        return Err(());
    }
    // (4) the header epoch equals the reference's epoch.
    if !matches!(m, Some(Mutant::EpochSkip) | Some(Mutant::EpochSkipRefCrc))
        && rd_u32(p, 16)? != r.epoch
    {
        return Err(());
    }
    Ok(())
}

/// Encode a MapLeaf body: `cap` entries of `(slot u64, epoch u32, crc u32)`
/// for LPIDs `[base, base+cap)`, `slot = 0` meaning unmapped.
pub fn build_map_leaf(base: u64, cap: u64, entries: &[Option<Ref>]) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    for i in 0..cap as usize {
        let e = entries.get(i).copied().flatten().unwrap_or_default();
        let at = HDR + i * MAP_ENTRY;
        wr_u64(&mut p, at, e.slot);
        wr_u32(&mut p, at + 8, e.epoch);
        wr_u32(&mut p, at + 12, e.crc);
    }
    finish(&mut p, KIND_MAP_LEAF, base, 0);
    p
}

/// Decode a MapLeaf body into `cap` optional refs (P-PARSE: bounds and
/// kind validated; §3.3 via [`check`]).
pub fn parse_map_leaf(
    p: &[u8],
    base: u64,
    cap: u64,
    r: &Ref,
    mutant: Option<Mutant>,
) -> Result<Vec<Option<Ref>>, ()> {
    check(p, r, base, mutant)?;
    if p.get(4).copied() != Some(KIND_MAP_LEAF) {
        return Err(());
    }
    if HDR + cap as usize * MAP_ENTRY > PAGE {
        return Err(());
    }
    let mut out = Vec::with_capacity(cap as usize);
    for i in 0..cap as usize {
        let at = HDR + i * MAP_ENTRY;
        let slot = rd_u64(p, at)?;
        let epoch = rd_u32(p, at + 8)?;
        let crc = rd_u32(p, at + 12)?;
        out.push(if slot == 0 {
            None
        } else {
            Some(Ref { slot, epoch, crc })
        });
    }
    Ok(out)
}

/// Encode a FreeSetLeaf body: `bits` bits starting at bit index `base`.
pub fn build_free_leaf(base: u64, bits: u64, set: &dyn Fn(u64) -> bool) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    for i in 0..bits {
        if set(base + i) {
            let byte = HDR + (i / 8) as usize;
            if let Some(b) = p.get_mut(byte) {
                *b |= 1 << (i % 8);
            }
        }
    }
    finish(&mut p, KIND_FREE_LEAF, base, 0);
    p
}

/// Decode a FreeSetLeaf body into a bool vec of `bits` entries (P-PARSE).
pub fn parse_free_leaf(
    p: &[u8],
    base: u64,
    bits: u64,
    r: &Ref,
    mutant: Option<Mutant>,
) -> Result<Vec<bool>, ()> {
    check(p, r, base, mutant)?;
    if p.get(4).copied() != Some(KIND_FREE_LEAF) {
        return Err(());
    }
    if HDR + (bits as usize).div_ceil(8) > PAGE {
        return Err(());
    }
    let mut out = Vec::with_capacity(bits as usize);
    for i in 0..bits {
        let byte = p.get(HDR + (i / 8) as usize).copied().unwrap_or(0);
        out.push(byte & (1 << (i % 8)) != 0);
    }
    Ok(out)
}

/// Encode a MapInner/FreeSetInner body: refs of children at fixed stride.
pub fn build_radix_inner(base: u64, kind: u8, refs: &[Option<Ref>]) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    for (i, e) in refs.iter().enumerate() {
        let e = e.unwrap_or_default();
        let at = HDR + i * MAP_ENTRY;
        wr_u64(&mut p, at, e.slot);
        wr_u32(&mut p, at + 8, e.epoch);
        wr_u32(&mut p, at + 12, e.crc);
    }
    finish(&mut p, kind, base, 0);
    p
}

/// Decode an inner radix page (P-PARSE; `n` refs at fixed stride).
pub fn parse_radix_inner(
    p: &[u8],
    base: u64,
    n: usize,
    r: &Ref,
    mutant: Option<Mutant>,
) -> Result<Vec<Option<Ref>>, ()> {
    let kind = p.get(4).copied().ok_or(())?;
    if kind != KIND_MAP_INNER && kind != KIND_FREE_INNER {
        return Err(());
    }
    check(p, r, base, mutant)?;
    if HDR + n * MAP_ENTRY > PAGE {
        return Err(());
    }
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let at = HDR + i * MAP_ENTRY;
        let slot = rd_u64(p, at)?;
        let epoch = rd_u32(p, at + 8)?;
        let crc = rd_u32(p, at + 12)?;
        out.push(if slot == 0 {
            None
        } else {
            Some(Ref { slot, epoch, crc })
        });
    }
    Ok(out)
}

// ---- meta (§2.2) and page 0 (§2.1) ----

/// Meta magic.
pub const META_MAGIC: &[u8; 8] = b"NUCMETA\0";
/// Page 0 magic.
pub const P0_MAGIC: &[u8; 8] = b"NUCLEUS\0";
/// The meta record (all §2.2 fields).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Meta {
    /// `ckpt_id` k.
    pub ckpt_id: u64,
    /// The apply frontier at the flip.
    pub t: u64,
    /// The image's GC watermark.
    pub gc_w: u64,
    /// The epoch (low 32 bits).
    pub epoch: u32,
    /// The page-map root ref.
    pub map_root: Ref,
    /// The free-set root ref.
    pub free_root: Ref,
    /// The directory root LPID.
    pub dir_root: u64,
    /// One past the highest LPID ever allocated.
    pub next_lpid: u64,
    /// The file length in slots covered by the image's free set.
    pub slot_count: u64,
    /// Replay start (§12.6).
    pub wal_start_lsn: u64,
}

/// Serialize a meta into a page image (with its CRC).
pub fn build_meta(m: &Meta) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    if let Some(s) = p.get_mut(0..8) {
        s.copy_from_slice(META_MAGIC);
    }
    wr_u32(&mut p, 8, 1); // format_version
    wr_u64(&mut p, 12, m.ckpt_id);
    wr_u64(&mut p, 20, m.t);
    wr_u64(&mut p, 28, m.gc_w);
    wr_u32(&mut p, 36, m.epoch);
    wr_u64(&mut p, 40, m.map_root.slot);
    wr_u32(&mut p, 48, m.map_root.epoch);
    wr_u32(&mut p, 52, m.map_root.crc);
    wr_u64(&mut p, 56, m.free_root.slot);
    wr_u32(&mut p, 64, m.free_root.epoch);
    wr_u32(&mut p, 68, m.free_root.crc);
    wr_u64(&mut p, 72, m.dir_root);
    wr_u64(&mut p, 80, m.next_lpid);
    wr_u64(&mut p, 88, m.slot_count);
    wr_u64(&mut p, 96, m.wal_start_lsn);
    // The CRC covers the fields (12..104), not itself.
    let crc = crc32c(0, p.get(12..104).unwrap_or(&[]));
    wr_u32(&mut p, 104, crc);
    p
}

/// Parse and validate a meta page image (P-PARSE).
pub fn parse_meta(p: &[u8]) -> Result<Meta, ()> {
    if p.len() != PAGE || p.get(0..8) != Some(META_MAGIC) {
        return Err(());
    }
    let crc = rd_u32(p, 104)?;
    if crc != crc32c(0, p.get(12..104).unwrap_or(&[])) {
        return Err(());
    }
    Ok(Meta {
        ckpt_id: rd_u64(p, 12)?,
        t: rd_u64(p, 20)?,
        gc_w: rd_u64(p, 28)?,
        epoch: rd_u32(p, 36)?,
        map_root: Ref {
            slot: rd_u64(p, 40)?,
            epoch: rd_u32(p, 48)?,
            crc: rd_u32(p, 52)?,
        },
        free_root: Ref {
            slot: rd_u64(p, 56)?,
            epoch: rd_u32(p, 64)?,
            crc: rd_u32(p, 68)?,
        },
        dir_root: rd_u64(p, 72)?,
        next_lpid: rd_u64(p, 80)?,
        slot_count: rd_u64(p, 88)?,
        wal_start_lsn: rd_u64(p, 96)?,
    })
}

/// Serialize page 0 (§2.1).
pub fn build_page0(uuid: u128, wal_seg: u32) -> Vec<u8> {
    let mut p = vec![0u8; PAGE];
    if let Some(s) = p.get_mut(0..8) {
        s.copy_from_slice(P0_MAGIC);
    }
    wr_u32(&mut p, 8, 1);
    wr_u32(&mut p, 12, PAGE as u32);
    if let Some(b) = p.get_mut(16) {
        *b = 1; // checksum_alg: CRC32C
    }
    wr_u32(&mut p, 20, wal_seg);
    wr_u64(&mut p, 24, uuid as u64);
    wr_u64(&mut p, 32, (uuid >> 64) as u64);
    let crc = crc32c(0, p.get(8..40).unwrap_or(&[]));
    wr_u32(&mut p, 40, crc);
    p
}

/// Validate page 0 (P-PARSE: magic, CRC and page size).
pub fn parse_page0(p: &[u8]) -> Result<(), ()> {
    if p.len() != PAGE || p.get(0..8) != Some(P0_MAGIC) {
        return Err(());
    }
    let crc = rd_u32(p, 40)?;
    if crc != crc32c(0, p.get(8..40).unwrap_or(&[])) {
        return Err(());
    }
    if rd_u32(p, 12)? != PAGE as u32 {
        return Err(());
    }
    Ok(())
}
