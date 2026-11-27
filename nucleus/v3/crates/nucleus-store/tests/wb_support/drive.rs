//! The stand-ins S1 and S3 of S-STORE §15 (card A-3c, Work 13.3 and 13.4):
//! the flip, and the callers that later cards replace (A-7c's checkpoint, the
//! tree's allocation and restart of A-4a, the free of A-4b). Each driver is
//! doc-commented with the card it stands for. Nothing here is store code: a
//! driver calls the pool the way its card says that caller will.

use nucleus_store::StoreError;
use nucleus_store::buf::{CreateOutcome, EvictOutcome, Hold, LpidAlloc, PageKey, Pin, Pool};
use nucleus_store::page::Kind;
use nucleus_store::sync::Primitives;

use super::file::WbFile;
use super::{MARK, frame_of, pin, said};

/// How many times a driver may go round a loop that a correct build leaves at
/// once or after a few restarts: a count, never a clock. More is a test
/// failure ("restart bound exceeded").
pub const BOUND: usize = 16;

/// An `Invalid` error with `what`: the form a driver's own refusals take.
fn invalid(what: &str) -> StoreError {
    StoreError::Invalid(what.to_string())
}

/// S1, the flip (S-STORE §15 "Stand-ins" S1): `gate_exclusive()`;
/// `record_image_root(map_shape()?.root_level)`; `advance_epoch()`; drop the
/// guard; the new epoch. Nothing else of §13 step 0 item 2.
pub fn s1_flip<P: Primitives>(pool: &Pool<WbFile, P>) -> Result<u32, StoreError> {
    let mut flip = pool.gate_exclusive()?;
    let shape = pool.map_shape()?;
    flip.record_image_root(shape.root_level)?;
    flip.advance_epoch()?;
    drop(flip);
    pool.epoch()
}

/// S3, the restart's half that makes a MapLeaf resident, in a gated
/// operation of its own (S-STORE §5.6 "Restart"; card A-4a's restart will do
/// this): the frame first, holding no latch; the growth call; then
/// `latch_leaf_parent`, and the MapLeaf is created in that frame only if no
/// frame for it is in the page table and the ref is still null; else the
/// unused frame is given back and the MapLeaf pinned, the MapInner's latch
/// released before any load or wait (§5.3, §5.4).
pub fn s3_mapleaf_pin<'p, P: Primitives>(
    pool: &'p Pool<WbFile, P>,
    leaf: u64,
) -> Result<Pin<'p, WbFile, P>, StoreError> {
    let g2 = pool.gate_shared()?;
    let key = PageKey::MapLeaf(leaf);
    let res = pool.reserve_with(&Hold::gated(&g2))?;
    pool.grow_map(leaf, &g2)?;
    let lp = pool.latch_leaf_parent(leaf)?;
    if let Some(p) = pool.pin_resident(key)? {
        drop(lp);
        drop(res);
        drop(g2);
        return Ok(p);
    }
    let absent = pool.frame_of(key)?.is_none();
    if absent && lp.leaf_ref().is_null() {
        match pool.create(res, key, Kind::MapLeaf, g2.epoch())? {
            CreateOutcome::Created(c) => {
                let p = c.release();
                drop(lp);
                drop(g2);
                Ok(p)
            }
            // Another thread loads the MapLeaf: the latch goes before the
            // wait for that load.
            CreateOutcome::Loading(res) => {
                drop(res);
                drop(lp);
                pin(pool, key, &Hold::gated(&g2))
            }
            // Another restart made it first: its pin.
            CreateOutcome::Exists(p, res) => {
                drop(res);
                drop(lp);
                drop(g2);
                Ok(p)
            }
        }
    } else {
        drop(res);
        drop(lp);
        pin(pool, key, &Hold::gated(&g2))
    }
}

/// S3, a MapLeaf made and left empty, dirty and unpinned: the restart's half
/// with the pin dropped.
pub fn s3_make_mapleaf<P: Primitives>(pool: &Pool<WbFile, P>, leaf: u64) -> Result<(), StoreError> {
    let p = s3_mapleaf_pin(pool, leaf)?;
    drop(p);
    Ok(())
}

/// S3, a new page (S-STORE §5.6 "Restart"; card A-4a's allocation and
/// restart will do this): repeat within [`BOUND`]: `g = gate_shared()`;
/// `alloc_lpids(1, &held, &g)`.
/// - `Taken`: `res = reserve_with(&Hold::gated(&g))`; `create(res, Lpid(x),
///   Kind::Leaf, g.epoch())` must be `Created`; write `marker` at `MARK`
///   through the created page; release its latch, drop the allocation's
///   MapLeaf pins and `held` (the page's §4.1 pin keeps its MapLeaf
///   resident); drop `g`; `(x, pin)`.
/// - `Loading(res)` from `create`: `wait_settled(Lpid(x))`, then `create`
///   again with `res` (§5.4 "The load": the creation "waits for that load to
///   end and looks again"). `Exists` for an LPID is damage (A-3a OBL-6).
/// - `Restart { leaf, emptied }`: drop the pins of `emptied`; drop `g`; pin
///   `leaf` as [`s3_mapleaf_pin`] does and repeat.
pub fn s3_new_page<'p, P: Primitives>(
    pool: &'p Pool<WbFile, P>,
    marker: u8,
) -> Result<(u64, Pin<'p, WbFile, P>), StoreError> {
    let mut held: Vec<(u64, Pin<'p, WbFile, P>)> = Vec::new();
    let mut leaves: Vec<u64> = Vec::new();
    for _ in 0..BOUND {
        let g = pool.gate_shared()?;
        match pool.alloc_lpids(1, &leaves, &g)? {
            LpidAlloc::Taken { lpids, pins } => {
                let x = *lpids
                    .first()
                    .ok_or_else(|| invalid("the allocator took no LPID"))?;
                let key = PageKey::Lpid(x);
                let mut res = pool.reserve_with(&Hold::gated(&g))?;
                let mut page = None;
                for _ in 0..BOUND {
                    match pool.create(res, key, Kind::Leaf, g.epoch())? {
                        CreateOutcome::Created(c) => {
                            page = Some(c);
                            break;
                        }
                        CreateOutcome::Loading(r) => {
                            pool.wait_settled(key)?;
                            res = r;
                        }
                        CreateOutcome::Exists(..) => {
                            return Err(invalid(
                                "an LPID the allocator handed out is held live by a frame (damage)",
                            ));
                        }
                    }
                }
                let mut page = page.ok_or_else(|| invalid("create loop bound exceeded"))?;
                *page
                    .get_mut(MARK)
                    .ok_or_else(|| invalid("a page shorter than the marker's byte"))? = marker;
                let p = page.release();
                drop(pins);
                drop(held);
                drop(g);
                return Ok((x, p));
            }
            LpidAlloc::Restart(r) => {
                drop(g);
                let mut kept = Vec::new();
                for (leaf, p) in held {
                    if r.emptied.contains(&leaf) {
                        drop(p);
                    } else {
                        kept.push((leaf, p));
                    }
                }
                held = kept;
                held.push((r.leaf, s3_mapleaf_pin(pool, r.leaf)?));
                leaves.clear();
                leaves.extend(held.iter().map(|(leaf, _)| *leaf));
            }
        }
    }
    Err(invalid("restart bound exceeded"))
}

/// S3, a first touch (as `write_guard`'s will be where no copy is needed):
/// latch the page exclusively, `mark_dirty(pool.epoch())`, write `marker` at
/// `MARK`.
pub fn s3_touch<P: Primitives>(
    pool: &Pool<WbFile, P>,
    p: &Pin<'_, WbFile, P>,
    marker: u8,
) -> Result<(), StoreError> {
    let mut g = p.latch_exclusive()?;
    let e = pool.epoch()?;
    g.mark_dirty(e)?;
    *g.get_mut(MARK)
        .ok_or_else(|| invalid("a page shorter than the marker's byte"))? = marker;
    Ok(())
}

/// S3, the free (S-STORE §4.2 "Freeing"; card A-4b's free; A-3a OBL-5), in
/// one gated operation: `g = gate_shared()`; pin `MapLeaf(x / fanout)` and
/// `Lpid(x)` under the gate; latch the page exclusively;
/// `old = set_entry(x, Ref::NULL, &g)`; `mark_freed()`; drop the latch;
/// `supersede(old, &g)`; `release_lpid(x)`; drop the pins and `g`.
pub fn s3_free<P: Primitives>(
    pool: &Pool<WbFile, P>,
    x: u64,
    fanout: u64,
) -> Result<nucleus_store::meta::Ref, StoreError> {
    let g = pool.gate_shared()?;
    let leaf = pin(pool, PageKey::MapLeaf(x / fanout), &Hold::gated(&g))?;
    let page = pin(pool, PageKey::Lpid(x), &Hold::gated(&g))?;
    let mut guard = page.latch_exclusive()?;
    let old = pool.set_entry(x, nucleus_store::meta::Ref::NULL, &g)?;
    guard.mark_freed()?;
    drop(guard);
    pool.supersede(old, &g)?;
    pool.release_lpid(x)?;
    drop(page);
    drop(leaf);
    drop(g);
    Ok(old)
}

/// S3, step 6's release, as A-7c's checkpoint will call it (S-STORE §13 step
/// 6): `release_step6(slots)`.
pub fn s3_release<P: Primitives>(pool: &Pool<WbFile, P>, slots: &[u64]) -> Result<(), StoreError> {
    pool.release_step6(slots)
}

/// Evict the frame holding `key` (this card's own call, not a stand-in):
/// `evict_frame(frame_of(key), &Hold::nothing())`; an `Obtained`
/// reservation is dropped by the caller.
pub fn evict<P: Primitives>(
    pool: &Pool<WbFile, P>,
    key: PageKey,
) -> Result<EvictOutcome<'_, WbFile, P>, StoreError> {
    let f = frame_of(pool, key)?;
    pool.evict_frame(f, &Hold::nothing())
}

/// Evict the frame holding `key` if one does, and say whether a frame left
/// the page table.
pub fn evict_if_resident<P: Primitives>(
    pool: &Pool<WbFile, P>,
    key: PageKey,
) -> Result<bool, StoreError> {
    if pool.frame_of(key)?.is_none() {
        return Ok(false);
    }
    Ok(matches!(evict(pool, key)?, EvictOutcome::Obtained(_)))
}

/// Evict `key` and require that its frame left the page table.
pub fn evict_out<P: Primitives>(pool: &Pool<WbFile, P>, key: PageKey) -> Result<(), StoreError> {
    match evict(pool, key)? {
        EvictOutcome::Obtained(res) => {
            drop(res);
            Ok(())
        }
        other => Err(StoreError::Invalid(format!(
            "evicting {key:?} gave {other:?} where Obtained was expected"
        ))),
    }
}

/// The file's bytes of `slot`, read without a log entry; `Err` if the read
/// fails.
pub fn slot_bytes(file: &WbFile, slot: u64) -> Result<Vec<u8>, String> {
    file.peek(slot * u64::from(super::PS), super::PS as usize)
        .map_err(said)
}

/// The header's epoch of the page written at `slot`: the `epoch` the write
/// log recorded for the write at that slot's offset, or `None`.
pub fn epoch_written_at(file: &WbFile, slot: u64) -> Option<u32> {
    let off = slot * u64::from(super::PS);
    file.writes()
        .into_iter()
        .rev()
        .find(|w| w.offset == off)
        .map(|w| w.epoch)
}
