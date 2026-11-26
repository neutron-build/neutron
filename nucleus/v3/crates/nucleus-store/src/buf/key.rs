//! A page's key in the buffer pool's page table (S-STORE §5.1).
//!
//! Invariants owned here:
//! - **Keyed apart** (§5.1: "A page is an LPID or a MapLeaf's map position,
//!   the two keyed apart, as §13 step 2 keys image records"): [`PageKey`]'s
//!   two variants never compare equal, although the MapLeaf at index `i` has
//!   header id `i` (card A-2's `map::position_id(Map, 0, i)`), the id LPID
//!   `i` has too (test `key_namespaces`; mutant NAMESPACE-MERGED, S-STORE's
//!   PAGE-KEY-MERGED).
//! - **The expected id** (§3.3): [`PageKey::header_id`] is the header `id`
//!   every load of the key checks, and the `id` every creation writes; LPID 0
//!   is null (§1) and has none.
//! - **The kinds a key admits** (§3.1): an LPID names a Leaf, Inner or
//!   Overflow page (kinds 3–5) and a MapLeaf position a MapLeaf (kind 6), so
//!   a load whose bytes carry another kind is refused like any other §3.3
//!   mismatch (mutant KIND-UNCHECKED).
//! - **Placement** ([`PageKey::hash64`]): the bucket a key lands in is the
//!   same on every target, so the chained table's layout, which test
//!   `table_chains` builds on, does not depend on the platform.

use crate::map::{self, Radix};
use crate::page::Kind;
use crate::store::StoreError;

/// The tag that keeps a MapLeaf's index apart from an LPID with the same
/// number before the hash's finaliser mixes it (the 64-bit golden ratio).
const MAPLEAF_TAG: u64 = 0x9E37_79B9_7F4A_7C15;

/// A page's key in the pool's page table (S-STORE §5.1): an LPID, or the
/// index of a MapLeaf (its position at level 0, §4.1), the two keyed apart.
///
/// MapInner pages live outside the pool (§4.1, §5.7) and have no key here;
/// FreeSet pages are not pooled at card A-3a.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PageKey {
    /// A logical page id (§1): a B+tree leaf or inner page, or an overflow
    /// page (kinds 3–5, §3.1). LPID 0 is null and is never a key.
    Lpid(u64),
    /// The MapLeaf at this index (kind 6, level 0, §4.1); it maps LPIDs
    /// `[i·f, (i+1)·f)`.
    MapLeaf(u64),
}

impl PageKey {
    /// The header `id` a page of this key carries (S-STORE §3.1, §3.3): the
    /// LPID itself, or `map::position_id(Radix::Map, 0, i)` for the MapLeaf
    /// at index `i`.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] for `Lpid(0)` (§1: LPID 0 is null) and for a
    /// MapLeaf index of `2^56` or more, which has no position id (card A-2).
    pub fn header_id(&self) -> Result<u64, StoreError> {
        match *self {
            PageKey::Lpid(0) => Err(StoreError::Invalid(
                "LPID 0 is null and names no page (S-STORE §1)".into(),
            )),
            PageKey::Lpid(x) => Ok(x),
            PageKey::MapLeaf(i) => map::position_id(Radix::Map, 0, i),
        }
    }

    /// Whether a page of `kind` may be this key's page (S-STORE §3.1): an
    /// LPID admits `Leaf`, `Inner` and `Overflow`, and a MapLeaf position
    /// admits `MapLeaf` only.
    pub fn admits(&self, kind: Kind) -> bool {
        match self {
            PageKey::Lpid(_) => matches!(kind, Kind::Leaf | Kind::Inner | Kind::Overflow),
            PageKey::MapLeaf(_) => kind == Kind::MapLeaf,
        }
    }

    /// The key's 64-bit hash, which places it in a bucket of the page table
    /// and nothing more (equality is the enum's): the pair `(tag, value)`
    /// is `(0, x)` for `Lpid(x)` and `(0x9E37_79B9_7F4A_7C15, i)` for
    /// `MapLeaf(i)`; their xor goes through MurmurHash3's 64-bit finaliser.
    /// Pinned (card A-3a, Work 5), so placement is the same on every target.
    pub(crate) fn hash64(&self) -> u64 {
        let (tag, value) = match *self {
            PageKey::Lpid(x) => (0, x),
            PageKey::MapLeaf(i) => (MAPLEAF_TAG, i),
        };
        let mut k = value ^ tag;
        k ^= k >> 33;
        k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
        k ^= k >> 33;
        k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        k ^= k >> 33;
        k
    }
}
