//! The page map's pages and their positions (S-STORE §3.1, §4.1), as
//! codecs with no I/O (card A-2).
//!
//! # Invariants this module owns
//!
//! - **Positions** (§3.1, §4.1). A map or free-set page's header `id` is
//!   its radix position: its level, counted from the leaf level (0), and
//!   its index at that level. Card A-2 pins the encoding [`position_id`]:
//!   `radix << 63 | level << 56 | index`, with `level ≤ 127` and
//!   `index < 2^56`. [`position_of`] inverts it, so no two positions of
//!   either radix share an id; and since levels count from the leaves, a
//!   new root level changes no page's id (§4.1 "Growth"). A position's id
//!   can equal an LPID (the MapLeaf at index `i` has id `i`): §5.1's page
//!   table keys the two apart, as §13 step 2 keys image records.
//! - **Geometry** (§4.1). The fanout `f` is [`max_fanout`],
//!   `⌊(page_size − 24) / 16⌋` (510 at 8 KiB), or a smaller test fanout
//!   (`f ≥ 2`) for a whole store. The MapLeaf at index `i` maps LPIDs
//!   `[i·f, (i+1)·f)`; the MapInner at level `L` and index `j` covers
//!   MapLeaf indexes `[j·f^L, (j+1)·f^L)`; the root is a MapInner at level
//!   1 or above. [`MapGeometry`]'s arithmetic is checked and never wraps.
//! - **The map page body** (§4.1). A MapLeaf (kind 6, level 0) or MapInner
//!   (kind 7, level ≥ 1) holds `f` 16-byte refs `(slot u64, epoch u32,
//!   crc u32)` after the 24-byte header, the ref at index `i` at byte
//!   `24 + 16·i`: a MapLeaf's are entries for its LPIDs, a MapInner's its
//!   children's refs. A ref with slot 0 is unmapped (an entry) or null (a
//!   child) and is all zero, and every byte after the `f` refs is zero.
//!   [`check_map_page`], run after `page::verify`, refuses every other body
//!   with `StoreError::Corrupt` (XX001), never a panic.
//!
//! Not here: the free set's FreeSetLeaf and FreeSetInner bodies (kinds 8
//! and 9; card A-7b, which uses [`Radix::FreeSet`] positions); map
//! residency, growth and the image map (card A-3c).

use crate::meta::Ref;
use crate::page::{self, Kind, PAGE_HEADER};
use crate::store::StoreError;

/// The size of one ref: `slot u64, epoch u32, crc u32` (§4.1, §2.2).
const REF_LEN: usize = 16;

/// The bit of a position id that names its radix.
const RADIX_SHIFT: u32 = 63;

/// The lowest bit of a position id's 7-bit level.
const LEVEL_SHIFT: u32 = 56;

/// The largest level a position id holds.
const MAX_LEVEL: u8 = 127;

/// One past the largest index a position id holds: `2^56`.
const INDEX_LIMIT: u64 = 1 << 56;

/// The two radix trees whose pages carry a position as their `id`
/// (§3.1: kinds 6–9).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Radix {
    /// The page map (§4.1): MapLeaf and MapInner pages.
    Map = 0,
    /// The free set (§4.3): FreeSetLeaf and FreeSetInner pages (card
    /// A-7b).
    FreeSet = 1,
}

/// The `id` of the page at `(level, index)` of `radix` (S-STORE §3.1):
/// `radix << 63 | level << 56 | index`, card A-2's encoding, which gives no
/// two positions one id.
///
/// # Errors
/// [`StoreError::Invalid`] when `level > 127` or `index ≥ 2^56`: such a
/// position has no id, and the encoding refuses it rather than wrap. From
/// 8 KiB pages up, every `u64` LPID's MapLeaf index is below `2^56`.
pub fn position_id(radix: Radix, level: u8, index: u64) -> Result<u64, StoreError> {
    if level > MAX_LEVEL {
        return Err(StoreError::Invalid(format!(
            "map level {level} is above {MAX_LEVEL}"
        )));
    }
    if index >= INDEX_LIMIT {
        return Err(StoreError::Invalid(format!(
            "map index {index:#x} is not below 2^56"
        )));
    }
    let r: u64 = match radix {
        Radix::Map => 0,
        Radix::FreeSet => 1,
    };
    Ok((r << RADIX_SHIFT) | (u64::from(level) << LEVEL_SHIFT) | index)
}

/// The position an `id` encodes: the inverse of [`position_id`], total
/// over `u64`.
pub fn position_of(id: u64) -> (Radix, u8, u64) {
    let radix = if id >> RADIX_SHIFT == 0 {
        Radix::Map
    } else {
        Radix::FreeSet
    };
    // The mask keeps the level's 7 bits, so the cast cannot truncate.
    let level = ((id >> LEVEL_SHIFT) & u64::from(MAX_LEVEL)) as u8;
    (radix, level, id & (INDEX_LIMIT - 1))
}

/// The fanout of a map page at `page_size` (S-STORE §4.1):
/// `⌊(page_size − 24) / 16⌋`, the refs that fit after the page header. 254,
/// 510, 1022, 2046 and 4094 at 4, 8, 16, 32 and 64 KiB.
pub fn max_fanout(page_size: usize) -> u64 {
    // A usize quotient always fits a u64.
    (page_size.saturating_sub(PAGE_HEADER) / REF_LEN) as u64
}

/// The shape of a store's map (S-STORE §4.1): its page size and fanout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MapGeometry {
    /// The store's page size, one of `meta::PAGE_SIZES`.
    page_size: usize,
    /// The fanout `f`: `2 ≤ f ≤ max_fanout(page_size)`.
    f: u64,
}

impl MapGeometry {
    /// The geometry of a store of `page_size` pages, at the full fanout
    /// [`max_fanout`].
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `page_size` is not an allowed page size.
    pub fn new(page_size: usize) -> Result<Self, StoreError> {
        Self::with_fanout(page_size, max_fanout(page_size))
    }

    /// The geometry of a test build's store, at a smaller fanout `f` (§4.1:
    /// "A test build may set a smaller fanout (`f ≥ 2`) for the whole
    /// store ... nothing else changes, and only a build with the same
    /// fanout opens such a file").
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `page_size` is not an allowed page size,
    /// or `f` is below 2 or above [`max_fanout`] of it.
    pub fn with_fanout(page_size: usize, f: u64) -> Result<Self, StoreError> {
        if !page::is_page_len(page_size) {
            return Err(StoreError::Invalid(format!(
                "page size {page_size} is not an allowed page size"
            )));
        }
        let max = max_fanout(page_size);
        if f < 2 || f > max {
            return Err(StoreError::Invalid(format!(
                "fanout {f} is outside 2..={max} at {page_size}-byte pages"
            )));
        }
        Ok(Self { page_size, f })
    }

    /// The fanout `f`: the refs one map page holds (§4.1).
    pub fn fanout(&self) -> u64 {
        self.f
    }

    /// Where an LPID's map entry is (§4.1): the index of its MapLeaf,
    /// `⌊lpid / f⌋`, and the entry within it, `lpid mod f`.
    pub fn lpid_entry(&self, lpid: u64) -> (u64, usize) {
        // The entry is below f ≤ 4094, so the cast cannot truncate.
        (lpid / self.f, (lpid % self.f) as usize)
    }

    /// The MapLeaf indexes a map page at `level` covers (§4.1): `f^level`;
    /// `None` when that overflows a `u64`.
    pub fn leaf_cover(&self, level: u8) -> Option<u64> {
        self.f.checked_pow(u32::from(level))
    }

    /// The position of child `slot` of the MapInner at `(level, index)`
    /// (§4.1): `(level − 1, index·f + slot)`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `level` is 0 (a MapLeaf has no child),
    /// `slot ≥ f`, or the index overflows a `u64`.
    pub fn child(&self, level: u8, index: u64, slot: u64) -> Result<(u8, u64), StoreError> {
        let below = level
            .checked_sub(1)
            .ok_or_else(|| StoreError::Invalid("a MapLeaf (level 0) has no child".into()))?;
        if slot >= self.f {
            return Err(StoreError::Invalid(format!(
                "child slot {slot} is not below the fanout {}",
                self.f
            )));
        }
        let child = index
            .checked_mul(self.f)
            .and_then(|i| i.checked_add(slot))
            .ok_or_else(|| {
                StoreError::Invalid(format!(
                    "child {slot} of map index {index:#x} overflows a u64"
                ))
            })?;
        Ok((below, child))
    }

    /// The parent of the map page at `(level, index)` (§4.1): its position
    /// `(level + 1, ⌊index / f⌋)` and the page's slot in it, `index mod f`.
    /// The level saturates at 255; no level above 127 has an id
    /// ([`position_id`]), so a saturated parent is never encoded.
    pub fn parent(&self, level: u8, index: u64) -> (u8, u64, u64) {
        (level.saturating_add(1), index / self.f, index % self.f)
    }

    /// The level of the smallest root that covers MapLeaf `leaf_index`
    /// (§4.1): the smallest `L ≥ 1` with `leaf_index < f^L`. The root is
    /// always a MapInner, at level 1 or above; `L` is at most 64.
    pub fn root_level_for(&self, leaf_index: u64) -> u8 {
        let mut level: u8 = 1;
        // f^level, or None once it exceeds every u64 index.
        let mut cover = Some(self.f);
        while let Some(c) = cover {
            if leaf_index < c {
                break;
            }
            cover = c.checked_mul(self.f);
            level = level.saturating_add(1);
        }
        level
    }

    /// The byte offset of ref `i` (`24 + 16·i`), for `i < f`.
    fn ref_offset(&self, i: u64) -> Option<usize> {
        if i >= self.f {
            return None;
        }
        usize::try_from(i)
            .ok()?
            .checked_mul(REF_LEN)?
            .checked_add(PAGE_HEADER)
    }

    /// The first byte after the `f` refs: `24 + 16·f`.
    fn refs_end(&self) -> Option<usize> {
        usize::try_from(self.f)
            .ok()?
            .checked_mul(REF_LEN)?
            .checked_add(PAGE_HEADER)
    }
}

/// The page kind of a map page at `level` (§3.1, §4.1): MapLeaf at level 0,
/// MapInner above.
fn map_kind(level: u8) -> Kind {
    if level == 0 {
        Kind::MapLeaf
    } else {
        Kind::MapInner
    }
}

/// Make `page` an empty map page at `(level, index)` (S-STORE §4.1): every
/// byte zero, then the header with kind MapLeaf at level 0 or MapInner
/// above, `id` = [`position_id`]`(Radix::Map, level, index)` and `epoch`.
/// Every ref is then null. Sealing is the writer's, once the page is
/// final (`page::seal`).
///
/// # Errors
/// [`StoreError::Invalid`] if the page is not `geom`'s page size or the
/// position has no id; the page is then unchanged.
pub fn init_map_page(
    page: &mut [u8],
    geom: &MapGeometry,
    level: u8,
    index: u64,
    epoch: u32,
) -> Result<(), StoreError> {
    if page.len() != geom.page_size {
        return Err(StoreError::Invalid(format!(
            "a {}-byte page is not a map page of {} bytes",
            page.len(),
            geom.page_size
        )));
    }
    let id = position_id(Radix::Map, level, index)?;
    page.fill(0);
    page::write_header(page, map_kind(level), id, epoch)
}

/// Ref `i` of a map page (S-STORE §4.1): a MapLeaf's entry for LPID
/// `index·f + i`, or a MapInner's ref to child `i`. Every 16 bytes are some
/// ref; [`check_map_page`] judges the page as a whole.
///
/// # Errors
/// [`StoreError::Invalid`] if `i ≥ f`; [`StoreError::Corrupt`] if the page
/// is not `geom`'s page size.
pub fn map_ref(page: &[u8], geom: &MapGeometry, i: u64) -> Result<Ref, StoreError> {
    let at = geom.ref_offset(i).ok_or_else(|| {
        StoreError::Invalid(format!("map ref {i} is not below the fanout {}", geom.f))
    })?;
    if page.len() != geom.page_size {
        return Err(StoreError::Corrupt(format!(
            "a {}-byte page is not a map page of {} bytes",
            page.len(),
            geom.page_size
        )));
    }
    page.get(at..)
        .and_then(|rest| rest.first_chunk::<REF_LEN>())
        .map(Ref::decode)
        .ok_or_else(|| StoreError::Corrupt(format!("map ref {i} runs past the page")))
}

/// Set ref `i` of a map page to `r` (S-STORE §4.1); `Ref::NULL` unmaps an
/// entry or nulls a child.
///
/// # Errors
/// [`StoreError::Invalid`] if `i ≥ f`, if `r` has slot 0 but is not
/// `Ref::NULL` (an unmapped ref is all zero), or if the page is not
/// `geom`'s page size; the page is then unchanged.
pub fn set_map_ref(page: &mut [u8], geom: &MapGeometry, i: u64, r: Ref) -> Result<(), StoreError> {
    let at = geom.ref_offset(i).ok_or_else(|| {
        StoreError::Invalid(format!("map ref {i} is not below the fanout {}", geom.f))
    })?;
    if r.is_null() && r != Ref::NULL {
        return Err(StoreError::Invalid(format!(
            "a ref with slot 0 must be all zero, not {r:?}"
        )));
    }
    if page.len() != geom.page_size {
        return Err(StoreError::Invalid(format!(
            "a {}-byte page is not a map page of {} bytes",
            page.len(),
            geom.page_size
        )));
    }
    let dst = page
        .get_mut(at..)
        .and_then(|rest| rest.first_chunk_mut::<REF_LEN>())
        .ok_or_else(|| StoreError::Invalid(format!("map ref {i} runs past the page")))?;
    *dst = r.encode();
    Ok(())
}

/// The body check of a map page read at `(level, index)` (S-STORE §4.1),
/// run after `page::verify`: the page's header decodes, its kind is
/// MapLeaf at level 0 and MapInner above, its `id` is the position's, every
/// ref with slot 0 is all zero, and every byte after the `f` refs is zero.
///
/// # Errors
/// [`StoreError::Corrupt`] (XX001) for any page that is not such a map page,
/// never a panic; [`StoreError::Invalid`] if the position has no id.
pub fn check_map_page(
    page: &[u8],
    geom: &MapGeometry,
    level: u8,
    index: u64,
) -> Result<(), StoreError> {
    let id = position_id(Radix::Map, level, index)?;
    if page.len() != geom.page_size {
        return Err(StoreError::Corrupt(format!(
            "a {}-byte page is not a map page of {} bytes",
            page.len(),
            geom.page_size
        )));
    }
    let h = page::read_header(page)?;
    let kind = map_kind(level);
    if h.kind != kind {
        return Err(StoreError::Corrupt(format!(
            "a map page at level {level} has kind {:?}, not {kind:?}",
            h.kind
        )));
    }
    if h.id != id {
        return Err(StoreError::Corrupt(format!(
            "map page id {:#x} is not the id {id:#x} of level {level}, index {index:#x}",
            h.id
        )));
    }
    let end = geom
        .refs_end()
        .ok_or_else(|| StoreError::Corrupt("the map refs overflow the address space".into()))?;
    let refs = page
        .get(PAGE_HEADER..end)
        .ok_or_else(|| StoreError::Corrupt("the map refs run past the page".into()))?;
    let (chunks, _) = refs.as_chunks::<REF_LEN>();
    for (i, chunk) in chunks.iter().enumerate() {
        let r = Ref::decode(chunk);
        if r.is_null() && r != Ref::NULL {
            return Err(StoreError::Corrupt(format!(
                "map ref {i} has slot 0 but is not all zero: {r:?}"
            )));
        }
    }
    let tail = page.get(end..).unwrap_or_default();
    if let Some(at) = tail.iter().position(|&b| b != 0) {
        return Err(StoreError::Corrupt(format!(
            "byte {} after the map refs is not zero",
            end.saturating_add(at)
        )));
    }
    Ok(())
}
