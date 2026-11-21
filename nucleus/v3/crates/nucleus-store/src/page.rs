// Provenance (card A-0): nucleus/v3/spikes/e0-btree/src/page.rs at 7ccd1cff8ef926f95a240b15d4b9c6a346251efe lines 1-849.
// Seed copy; the spike file stays the E0 evidence record (PLAN-v3 §9 A-0).
// Edits (card A-2): new §3.1 page header, seal/verify and §3.2 size limits; the slotted body moved under it (offsets, u32 free pointers), E0's constants replaced, A-0-F2's checked subtractions, a u16 fit check in compact; unit tests rebuilt.
#![deny(clippy::indexing_slicing)]
//! The page header (S-STORE §3.1), the check of a page read through a
//! reference (§3.3, I-INTEGRITY), the size limits (§3.2, I-KEY-SIZE), and
//! the E0 slotted page of Leaf and Inner pages under that header (card X-0
//! Work 4; PLAN-v3 §3.3 "leaves and inner nodes use a slotted layout").
//! Card A-2 pins every byte below.
//!
//! The page header, on every page in slots ≥ 3, all little-endian:
//!
//! ```text
//! offset  size  field
//!      0     4  crc     CRC32C of bytes 4..page_size
//!      4     1  kind    3 Leaf | 4 Inner | 5 Overflow | 6 MapLeaf |
//!                       7 MapInner | 8 FreeSetLeaf | 9 FreeSetInner
//!      5     1  flags   0
//!      6     2  reserved (0)
//!      8     8  id      LPID (kinds 3-5); radix position (kinds 6-9, `map`)
//!     16     4  epoch   content epoch, low 32 bits
//!     20     4  reserved (0)
//! ```
//!
//! The slotted body of a Leaf or Inner page (kinds 3 and 4), after it:
//!
//! ```text
//! offset  size  field
//!     24     2  nslots
//!     26     2  reserved (0)
//!     28     4  free_start   first byte after the slot array
//!     32     4  free_end     first byte of the cell area; page_size if empty
//!     36     8  right-most child page id (inner pages; 0 in a leaf)
//!     44   ...  slot array: (offset u16, len u16) per cell, growing up
//! free_end      cell area, cells growing DOWN from the page end
//! ```
//!
//! Leaf cell: `klen u16, vlen u32, key, value`. Inner cell:
//! `klen u16, child u64, key`. All accessors are free functions over
//! `&[u8]`/`&mut [u8]`.
//!
//! Invariants owned here:
//! - The header bytes (§3.1: "Card A-2 pins the bytes and fuzzes the
//!   decoders. A decoder never panics: every malformed byte sequence is
//!   XX001"): [`read_header`] accepts exactly a page whose length is one of
//!   `meta::PAGE_SIZES`, whose kind is 3–9 and whose flags and reserved
//!   bytes are 0, and returns [`StoreError::Corrupt`] for every other byte
//!   sequence; a header it accepts writes back to the same bytes.
//! - I-INTEGRITY (§3.3): [`verify`] makes the four checks of a page read
//!   through a reference, each its own `Corrupt`, in a fixed order (the
//!   length, the self CRC, the reference's crc, the expected id, the
//!   reference's epoch, then the header's structure), and hands back
//!   nothing of the page until all of them pass.
//! - I-KEY-SIZE and the size limits (§3.2): [`k_max`] and [`v_inline`] are
//!   derived so that four maximum cells, slots included, fill a page's
//!   usable bytes `U` exactly at every allowed page size; [`check_key_size`]
//!   is the store's size check, refusing an over-size key with
//!   [`StoreError::KeyTooLarge`] (54000).
//! - P-PARSE: every accessor validates the header and the slot/cell bounds
//!   and returns [`StoreError::Corrupt`] on bad bytes — no indexing panic
//!   is possible anywhere in this module (`clippy::indexing_slicing` is
//!   denied at module level, PLAN-v3 §6.1).
//! - P-LAYOUT: `HEADER + SLOT*nslots <= free_start <= free_end <= len`,
//!   every cell lies inside `[free_end, len)` and cells never overlap.
//!   Every in-page offset field addresses the whole page at every allowed
//!   page size (§3.1): `free_start` and `free_end` are u32, since an empty
//!   65536-byte page's `free_end` is 65536, while a slot's u16 `offset`
//!   and `len` hold every cell, which starts below the page size and is at
//!   most `⌊U / 4⌋` bytes.
//! - P-ORDER: slots are stored in ascending key order (leaf) / separator
//!   order (inner). Removing a cell clears its slot; the hole in the cell
//!   area is reclaimed by [`compact`], which the insert path runs lazily
//!   when raw space runs out.
//! - P-SPLIT: [`leaf_split`] and [`inner_split`] partition by cell bytes so
//!   that both halves can always accept one more maximum-size cell; the
//!   separator of a leaf split is the first key of the right page (card
//!   X-0 Work 5), and an inner split moves its middle separator up.
//!
//! The seed writes its pages with `id` 0, `epoch` 0 and `crc` 0 and never
//! seals or verifies them; A-3a and A-3c call [`seal`] and [`verify`]
//! (lib.rs, "Seed status").

use crate::codec::{get_u8, get_u16, get_u32, get_u64, put_u8, put_u16, put_u32, put_u64};
use crate::crc::crc32c;
use crate::meta::{self, Ref};
use crate::store::{PageId, StoreError};

/// The page header's size: every page in slots ≥ 3 starts with it, and its
/// kind-specific body follows (S-STORE §3.1).
pub const PAGE_HEADER: usize = 24;
/// Where the slot array starts: the 24-byte page header (§3.1) plus the
/// 20-byte B+tree header of a Leaf or Inner page (card A-2), so 44. `U`
/// is the page size minus this (§1, [`usable`]).
pub const HEADER: usize = 44;
/// One slot: `(offset u16, len u16)`.
pub const SLOT: usize = 4;
/// Leaf cell header: `klen u16, vlen u32`.
const LEAF_HDR: usize = 6;
/// Inner cell header: `klen u16, child u64`.
const INNER_HDR: usize = 10;

/// Header `crc` (u32): CRC32C of bytes `CRC_COVERS..page_size` (§3.1).
const CRC_AT: usize = 0;
/// The first byte the header `crc` covers: every byte after the field.
const CRC_COVERS: usize = 4;
/// Header `kind` (u8).
const KIND_AT: usize = 4;
/// Header `flags` (u8), 0.
const FLAGS_AT: usize = 5;
/// The header's reserved u16, 0.
const RESERVED_U16_AT: usize = 6;
/// Header `id` (u64).
const ID_AT: usize = 8;
/// Header `epoch` (u32).
const EPOCH_AT: usize = 16;
/// The header's reserved u32, 0.
const RESERVED_U32_AT: usize = 20;
/// B+tree header `nslots` (u16).
const NSLOTS_AT: usize = 24;
/// The B+tree header's reserved u16, 0.
const BODY_RESERVED_AT: usize = 26;
/// B+tree header `free_start` (u32).
const FREE_START_AT: usize = 28;
/// B+tree header `free_end` (u32).
const FREE_END_AT: usize = 32;
/// B+tree header right-most child (u64).
const RIGHT_CHILD_AT: usize = 36;

/// The bytes an entry key adds to its user key besides the escape's
/// doubled zeros: the `0x00 0x00` terminator and the 8-byte ts suffix
/// (S-STORE §7.2, `|entry_key| = |k| + zeros(k) + 10`).
const ENTRY_KEY_EXTRA: usize = 10;

/// The kind byte of the page header (S-STORE §3.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum Kind {
    /// A B+tree leaf, with the slotted body (§3.1, §6).
    Leaf = 3,
    /// A B+tree inner page, with the slotted body (§3.1, §6).
    Inner = 4,
    /// A page of an overflow chain (§6.7); its body is card A-5's.
    Overflow = 5,
    /// A page-map leaf (§4.1); its body is `map`'s.
    MapLeaf = 6,
    /// A page-map inner page (§4.1); its body is `map`'s.
    MapInner = 7,
    /// A free-set leaf (§4.3); its body is card A-7b's.
    FreeSetLeaf = 8,
    /// A free-set inner page (§4.3); its body is card A-7b's.
    FreeSetInner = 9,
}

impl Kind {
    /// The kind a header `kind` byte names; `None` outside 3–9 (§3.1).
    pub fn from_byte(b: u8) -> Option<Kind> {
        match b {
            3 => Some(Kind::Leaf),
            4 => Some(Kind::Inner),
            5 => Some(Kind::Overflow),
            6 => Some(Kind::MapLeaf),
            7 => Some(Kind::MapInner),
            8 => Some(Kind::FreeSetLeaf),
            9 => Some(Kind::FreeSetInner),
            _ => None,
        }
    }
}

/// A decoded page header (S-STORE §3.1). `flags` and the reserved bytes
/// are not kept: they are 0 in every header [`read_header`] accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageHeader {
    /// The `crc` field: CRC32C of bytes `4..page_size` when sealed.
    pub crc: u32,
    /// The page kind.
    pub kind: Kind,
    /// The LPID (kinds 3–5) or the radix position (kinds 6–9,
    /// `map::position_id`).
    pub id: u64,
    /// The content epoch, low 32 bits (§1).
    pub epoch: u32,
}

/// Whether `len` is one of the allowed page sizes (S-STORE §2.1,
/// `meta::PAGE_SIZES`).
pub(crate) fn is_page_len(len: usize) -> bool {
    u32::try_from(len).is_ok_and(meta::is_page_size)
}

/// Decode a page's header (S-STORE §3.1): `Corrupt` unless the page's
/// length is an allowed page size, its kind is 3–9, and its `flags` and
/// both reserved fields are 0. The `crc`, `id` and `epoch` are returned
/// as stored; [`verify`] is what compares them.
///
/// # Errors
/// [`StoreError::Corrupt`] (XX001) for every byte sequence that is not a
/// header, never a panic.
pub fn read_header(page: &[u8]) -> Result<PageHeader, StoreError> {
    let len = page.len();
    if !is_page_len(len) {
        return Err(StoreError::Corrupt(format!(
            "a page of {len} bytes is not an allowed page size"
        )));
    }
    let crc = get_u32(page, CRC_AT)?;
    let kind_byte = get_u8(page, KIND_AT)?;
    let kind = Kind::from_byte(kind_byte)
        .ok_or_else(|| StoreError::Corrupt(format!("page kind {kind_byte} is not 3-9")))?;
    let flags = get_u8(page, FLAGS_AT)?;
    if flags != 0 {
        return Err(StoreError::Corrupt(format!(
            "page flags {flags:#04x} are not 0"
        )));
    }
    if get_u16(page, RESERVED_U16_AT)? != 0 {
        return Err(corrupt("page header bytes 6..8 are not 0"));
    }
    let id = get_u64(page, ID_AT)?;
    let epoch = get_u32(page, EPOCH_AT)?;
    if get_u32(page, RESERVED_U32_AT)? != 0 {
        return Err(corrupt("page header bytes 20..24 are not 0"));
    }
    Ok(PageHeader {
        crc,
        kind,
        id,
        epoch,
    })
}

/// Write a page header's bytes 4..24 (S-STORE §3.1): `kind`, `flags` 0,
/// the reserved u16 0, `id`, `epoch` and the reserved u32 0. The `crc`
/// field is left as it is; [`seal`] writes it once the body is final.
///
/// # Errors
/// [`StoreError::Invalid`] if the page's length is not an allowed page
/// size (a caller's bug); the page is then unchanged.
pub fn write_header(page: &mut [u8], kind: Kind, id: u64, epoch: u32) -> Result<(), StoreError> {
    if !is_page_len(page.len()) {
        return Err(StoreError::Invalid(format!(
            "a page of {} bytes is not an allowed page size",
            page.len()
        )));
    }
    put_u8(page, KIND_AT, kind as u8)?;
    put_u8(page, FLAGS_AT, 0)?;
    put_u16(page, RESERVED_U16_AT, 0)?;
    put_u64(page, ID_AT, id)?;
    put_u32(page, EPOCH_AT, epoch)?;
    put_u32(page, RESERVED_U32_AT, 0)?;
    Ok(())
}

/// Seal a page (S-STORE §3.1): write the CRC32C of bytes `4..page_size`
/// into the `crc` field and return it, for the reference that will name
/// the page (§3.3).
///
/// # Errors
/// [`StoreError::Invalid`] if the page's length is not an allowed page
/// size (a caller's bug); the page is then unchanged.
pub fn seal(page: &mut [u8]) -> Result<u32, StoreError> {
    if !is_page_len(page.len()) {
        return Err(StoreError::Invalid(format!(
            "a page of {} bytes is not an allowed page size",
            page.len()
        )));
    }
    let covered = page
        .get(CRC_COVERS..)
        .ok_or_else(|| StoreError::Invalid("a page shorter than its crc field".into()))?;
    let crc = crc32c(0, covered);
    put_u32(page, CRC_AT, crc)?;
    Ok(crc)
}

/// The check of a page read through a reference (S-STORE §3.3,
/// I-INTEGRITY). `reference` is the page-map entry or parent ref the page
/// was read through and `expected_id` the id the reader expects; the
/// reference's `slot` is where the page was read from and is not compared.
///
/// In this order, each its own check: the length is an allowed page size;
/// (1) the header `crc` equals the CRC32C of bytes `4..`; (2) it equals
/// `reference.crc`; (3) the header `id` equals `expected_id`; (4) the
/// header `epoch` equals `reference.epoch`; then [`read_header`]'s checks.
/// A torn, lost (stale) or misdirected write fails one of them, and no byte
/// of the body is decoded or handed back before all of them pass ("the
/// bytes are never served").
///
/// # Errors
/// [`StoreError::Corrupt`] (XX001) naming the first check that failed.
pub fn verify(page: &[u8], reference: &Ref, expected_id: u64) -> Result<PageHeader, StoreError> {
    let len = page.len();
    if !is_page_len(len) {
        return Err(StoreError::Corrupt(format!(
            "a page of {len} bytes is not an allowed page size"
        )));
    }
    let stored = get_u32(page, CRC_AT)?;
    let covered = page
        .get(CRC_COVERS..)
        .ok_or_else(|| corrupt("a page shorter than its crc field"))?;
    let computed = crc32c(0, covered);
    if stored != computed {
        return Err(StoreError::Corrupt(format!(
            "page crc {stored:#010x} does not match the CRC32C of its bytes, {computed:#010x}"
        )));
    }
    if stored != reference.crc {
        return Err(StoreError::Corrupt(format!(
            "page crc {stored:#010x} does not match the reference's crc {:#010x}",
            reference.crc
        )));
    }
    let id = get_u64(page, ID_AT)?;
    if id != expected_id {
        return Err(StoreError::Corrupt(format!(
            "page id {id:#x} is not the expected id {expected_id:#x}"
        )));
    }
    let epoch = get_u32(page, EPOCH_AT)?;
    if epoch != reference.epoch {
        return Err(StoreError::Corrupt(format!(
            "page epoch {epoch} is not the reference's epoch {}",
            reference.epoch
        )));
    }
    read_header(page)
}

/// `U` (S-STORE §1): the usable bytes of a B+tree page, `page_size` minus
/// the page header (§3.1) and the B+tree header, so `page_size − 44`.
/// 4052, 8148, 16340, 32724 and 65492 at the allowed sizes; 0 below 44.
pub fn usable(page_size: usize) -> usize {
    page_size.saturating_sub(HEADER)
}

/// `Q = ⌊U / 4⌋`: the budget of one maximum cell, its slot included, so
/// that four of them fit in `U` (§3.2: "at least **4** cells of maximum
/// size").
fn cell_budget(page_size: usize) -> usize {
    usable(page_size) / 4
}

/// `K_MAX(page_size)` (S-STORE §3.2): the largest key field of a cell, so
/// the bound on an encoded entry key (§7.2). `⌊(Q − 10) / 3⌋`, 10 being a
/// leaf cell's slot and `klen`/`vlen` header: a third of a maximum leaf
/// cell's key-and-value budget, card A-2's pin under §3.2's delegation.
/// 334, 675, 1358, 2723 and 5454 at 4, 8, 16, 32 and 64 KiB.
pub fn k_max(page_size: usize) -> usize {
    cell_budget(page_size).saturating_sub(SLOT + LEAF_HDR) / 3
}

/// `V_INLINE(page_size)` (S-STORE §3.2): the largest value field of a
/// cell, so the bound on an inline encoded entry value (§7.3: the tag
/// byte and its body, a provisional value's header included). `Q − 10 −
/// K_MAX`: 669, 1352, 2717, 5448 and 10909 at 4, 8, 16, 32 and 64 KiB.
pub fn v_inline(page_size: usize) -> usize {
    cell_budget(page_size)
        .saturating_sub(SLOT + LEAF_HDR)
        .saturating_sub(k_max(page_size))
}

/// The worst-case cost of a leaf cell, its slot included: a
/// [`k_max`]-byte key and a [`v_inline`]-byte value, which is exactly `Q`
/// (§3.2). [`is_safe`] tests a leaf against it.
pub fn max_leaf_cost(page_size: usize) -> usize {
    SLOT + LEAF_HDR + k_max(page_size) + v_inline(page_size)
}

/// The worst-case cost of an inner cell, its slot included: a
/// [`k_max`]-byte separator, at most `Q` (§3.2); an inner page holds
/// eleven at every allowed size. [`is_safe`] tests an inner page against
/// it.
pub fn max_inner_cost(page_size: usize) -> usize {
    SLOT + INNER_HDR + k_max(page_size)
}

/// The size of `entry_key(k, ts)` for the user key `k` (S-STORE §7.2):
/// `|k| + zeros(k) + 10`, each zero byte escaped to two.
pub fn entry_key_len(user_key: &[u8]) -> usize {
    let zeros = user_key.iter().filter(|&&b| b == 0).count();
    user_key
        .len()
        .saturating_add(zeros)
        .saturating_add(ENTRY_KEY_EXTRA)
}

/// The store's size check (S-STORE §3.2, I-KEY-SIZE), which the write path
/// calls when a write enters the write set (card B-1a): `Ok` with the
/// entry key's size when it is at most [`k_max`] of the page size.
///
/// # Errors
/// [`StoreError::KeyTooLarge`] with the entry key's size when it exceeds
/// `K_MAX` (54000, §3.2); [`StoreError::Invalid`] if `page_size` is not
/// an allowed page size.
pub fn check_key_size(page_size: usize, user_key: &[u8]) -> Result<usize, StoreError> {
    if !is_page_len(page_size) {
        return Err(StoreError::Invalid(format!(
            "page size {page_size} is not an allowed page size"
        )));
    }
    let len = entry_key_len(user_key);
    if len > k_max(page_size) {
        return Err(StoreError::KeyTooLarge(len));
    }
    Ok(len)
}

/// The kind of a slotted page: the header `kind`s 3 and 4 (§3.1).
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

/// Parse and validate the header (P-PARSE, P-LAYOUT): the page header
/// through [`read_header`], whose kind must be 3 (Leaf) or 4 (Inner), then
/// the B+tree header after it.
pub fn header(buf: &[u8]) -> Result<Header, StoreError> {
    let len = buf.len();
    let kind = match read_header(buf)?.kind {
        Kind::Leaf => PageKind::Leaf,
        Kind::Inner => PageKind::Inner,
        other => {
            return Err(corrupt(&format!(
                "page kind {} has no slotted body",
                other as u8
            )));
        }
    };
    let nslots = u16_rd(buf, NSLOTS_AT)? as usize;
    if u16_rd(buf, BODY_RESERVED_AT)? != 0 {
        return Err(corrupt("B+tree header bytes 26..28 are not 0"));
    }
    let free_start = u32_rd(buf, FREE_START_AT)? as usize;
    let free_end = u32_rd(buf, FREE_END_AT)? as usize;
    let right_child = PageId(u64_rd(buf, RIGHT_CHILD_AT)?);
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

/// Initialize an empty page of the given kind: the page header with kind 3
/// or 4, `id` 0, `epoch` 0 and `crc` 0 (the seed's pages are never sealed;
/// lib.rs, "Seed status"), and an empty B+tree header (`free_start` 44,
/// `free_end` the page size, right-most child 0).
///
/// # Errors
/// [`StoreError::Invalid`] if the page's length is not one of
/// `meta::PAGE_SIZES`.
pub fn init(buf: &mut [u8], kind: PageKind) -> Result<(), StoreError> {
    let len = buf.len();
    if !is_page_len(len) {
        return Err(StoreError::Invalid(format!(
            "a page of {len} bytes is not an allowed page size"
        )));
    }
    let k = match kind {
        PageKind::Leaf => Kind::Leaf,
        PageKind::Inner => Kind::Inner,
    };
    u32_wr(buf, CRC_AT, 0)?;
    write_header(buf, k, 0, 0)?;
    u16_wr(buf, NSLOTS_AT, 0)?;
    u16_wr(buf, BODY_RESERVED_AT, 0)?;
    u32_wr(buf, FREE_START_AT, HEADER as u32)?;
    // An allowed page size is at most 65536, so it fits the u32 field.
    u32_wr(buf, FREE_END_AT, len as u32)?;
    u64_wr(buf, RIGHT_CHILD_AT, 0)?;
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
///
/// The subtraction is checked (card A-0-F2): [`header`] already refuses
/// `free_start > free_end`, and this keeps P-PARSE from resting on that one
/// comparison.
pub fn free_bytes(buf: &[u8]) -> Result<usize, StoreError> {
    let h = header(buf)?;
    h.free_end
        .checked_sub(h.free_start)
        .ok_or_else(|| corrupt("free_start lies past free_end"))
}

/// Total hole bytes: cell-area bytes not covered by any live cell. Removed
/// cells leave holes until [`compact`] reclaims them.
///
/// The subtraction is checked (card X-0-F4 Work 3): P-LAYOUT says cells
/// never overlap, but corrupt bytes can say otherwise, and slots whose
/// lengths sum past the cell area are [`StoreError::Corrupt`], never an
/// arithmetic panic (P-PARSE).
pub fn hole_bytes(buf: &[u8]) -> Result<usize, StoreError> {
    let h = header(buf)?;
    let mut live = 0usize;
    for i in 0..h.nslots {
        let (_, len) = slot(buf, i)?;
        live += len;
    }
    buf.len()
        .checked_sub(h.free_end)
        .and_then(|area| area.checked_sub(live))
        .ok_or_else(|| corrupt("live cells exceed the cell area"))
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
/// release an ancestor's latch only once the child passes this test. The
/// worst-case cell is [`max_leaf_cost`] or [`max_inner_cost`] of the
/// page's own size, and the subtraction is checked (card A-0-F2).
pub fn is_safe(buf: &[u8]) -> Result<bool, StoreError> {
    let h = header(buf)?;
    let need = match h.kind {
        PageKind::Leaf => max_leaf_cost(buf.len()),
        PageKind::Inner => max_inner_cost(buf.len()),
    };
    let free = h
        .free_end
        .checked_sub(h.free_start)
        .ok_or_else(|| corrupt("free_start lies past free_end"))?;
    Ok(free >= need)
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
        cell.get_mut(LEAF_HDR..LEAF_HDR + key.len())
            .ok_or_else(|| corrupt("key write out of bounds"))?
            .copy_from_slice(key);
        cell.get_mut(LEAF_HDR + key.len()..)
            .ok_or_else(|| corrupt("value write out of bounds"))?
            .copy_from_slice(value);
    }
    shift_slots(buf, pos, h.nslots, 1)?;
    u16_wr(buf, HEADER + SLOT * pos, cell_off as u16)?;
    u16_wr(buf, HEADER + SLOT * pos + 2, cell_len as u16)?;
    u16_wr(buf, NSLOTS_AT, (h.nslots + 1) as u16)?;
    u32_wr(buf, FREE_START_AT, (h.free_start + SLOT) as u32)?;
    u32_wr(buf, FREE_END_AT, cell_off as u32)?;
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
        cell.get_mut(INNER_HDR..)
            .ok_or_else(|| corrupt("separator write out of bounds"))?
            .copy_from_slice(sep);
    }
    shift_slots(buf, pos, h.nslots, 1)?;
    u16_wr(buf, HEADER + SLOT * pos, cell_off as u16)?;
    u16_wr(buf, HEADER + SLOT * pos + 2, cell_len as u16)?;
    u16_wr(buf, NSLOTS_AT, (h.nslots + 1) as u16)?;
    u32_wr(buf, FREE_START_AT, (h.free_start + SLOT) as u32)?;
    u32_wr(buf, FREE_END_AT, cell_off as u32)?;
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
    u64_wr(buf, RIGHT_CHILD_AT, child.0)
}

fn remove_slot(buf: &mut [u8], i: usize) -> Result<(), StoreError> {
    let h = header(buf)?;
    if i >= h.nslots {
        return Err(corrupt("slot index out of range"));
    }
    shift_slots(buf, i + 1, h.nslots, -1)?;
    u16_wr(buf, NSLOTS_AT, (h.nslots - 1) as u16)?;
    u32_wr(buf, FREE_START_AT, (h.free_start - SLOT) as u32)?;
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
///
/// The running subtraction is checked (card X-0-F4 Work 3): overlapping
/// slots can sum past the page size, which is [`StoreError::Corrupt`],
/// never an arithmetic panic (P-PARSE). On error the page is unmodified.
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
        cur = cur
            .checked_sub(len)
            .ok_or_else(|| corrupt("cells overflow the page during compaction"))?;
        // `cur` becomes this cell's offset in its u16 slot field (card
        // A-2). Only an empty cell at the end of a 65536-byte page reaches
        // 65536, and no well-formed cell is empty, so this refuses corrupt
        // bytes before any byte of the page is written; it is also why the
        // offsets written below fit.
        if cur > usize::from(u16::MAX) {
            return Err(corrupt("a compacted cell's offset does not fit its slot"));
        }
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
    u16_wr(buf, NSLOTS_AT, h.nslots as u16)?;
    u32_wr(buf, FREE_START_AT, (HEADER + SLOT * h.nslots) as u32)?;
    u32_wr(buf, FREE_END_AT, cur as u32)?;
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
        last = Some(sep.clone());
        let pos = header(buf)?.nslots;
        if !inner_insert(buf, pos, *child, sep)? {
            return Err(corrupt("inner rebuild overflow"));
        }
    }
    set_right_child(buf, rightmost)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    // Corrupt-input arithmetic of `compact` and `hole_bytes` (card X-0-F4
    // Work 3; X-0 finding 4). `cargo test` builds this module in debug,
    // where the plain subtractions of the COMPACT-UNCHECKED and
    // HOLE-UNCHECKED mutants panic instead of returning `Corrupt`.

    use super::*;

    /// A leaf page whose three slots alias the same 3000 bytes at the start
    /// of the cell area: each cell is individually inside
    /// `[free_end, len)`, so `slot` accepts it, but the lengths sum to
    /// 9000 — past both the 8192-byte page (`compact`'s budget) and the
    /// 4096-byte cell area (`hole_bytes`'s). Only bytes that violate
    /// P-LAYOUT's "cells never overlap" can look like this.
    fn overlapping_slots_page() -> Vec<u8> {
        let mut buf = vec![0u8; 8192];
        init(&mut buf, PageKind::Leaf).unwrap();
        u16_wr(&mut buf, NSLOTS_AT, 3).unwrap();
        u32_wr(&mut buf, FREE_START_AT, (HEADER + 3 * SLOT) as u32).unwrap();
        u32_wr(&mut buf, FREE_END_AT, 4096).unwrap();
        for i in 0..3 {
            u16_wr(&mut buf, HEADER + SLOT * i, 4096).unwrap();
            u16_wr(&mut buf, HEADER + SLOT * i + 2, 3000).unwrap();
        }
        buf
    }

    #[test]
    fn compact_rejects_slots_summing_past_the_page() {
        let mut buf = overlapping_slots_page();
        let before = buf.clone();
        match compact(&mut buf) {
            Err(StoreError::Corrupt(msg)) => {
                assert!(msg.contains("overflow"), "message was {msg:?}");
            }
            other => panic!("expected Corrupt, got {other:?}"),
        }
        // A page `compact` rejects must come back unmodified: the failing
        // subtraction happens before any byte is written through `buf`.
        assert_eq!(buf, before, "compact modified a page it rejected");
    }

    #[test]
    fn hole_bytes_rejects_slots_summing_past_the_cell_area() {
        let buf = overlapping_slots_page();
        match hole_bytes(&buf) {
            Err(StoreError::Corrupt(msg)) => {
                assert!(msg.contains("exceed"), "message was {msg:?}");
            }
            other => panic!("expected Corrupt, got {other:?}"),
        }
    }
}
