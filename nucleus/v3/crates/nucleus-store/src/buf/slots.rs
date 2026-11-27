//! Slot allocation (card A-3c, Work 4; S-STORE §4.3, §4.4, §4.5, §2.4 and
//! §13 step 6): the in-memory slot bitmap, lowest free first, file growth,
//! the reserved slots that are never free, the superseded home, and the
//! two-generation pending frees, with step 6's release as one call.
//!
//! Invariants owned here, each rule quoting S-STORE:
//! - **S1, lowest free first** (§4.5: "Slots are allocated lowest-free-first,
//!   which keeps the file dense and reuses freed space before the file
//!   grows"): `alloc` takes the lowest free slot of `[3, slot_count)`.
//! - **S2, growth** (§4.5: "When no slot is free, the file grows by
//!   `max(1 MiB, file_len / 16)`, rounded down to a whole number of slots
//!   ... The new slots are free"): one `set_len` under the allocator's own
//!   mutex, so two allocators never grow twice. "A failed extension ... is
//!   fail-stop" (§4.5) is A-3c-B's; here its error is returned and the
//!   allocator's state is unchanged.
//! - **S3, reserved slots are never free** (§2.4: "reserved: never a page
//!   home ..., and referenced by every image, so never free in a slot
//!   bitmap"; §4.3): no path sets one's bit, and `supersede` and
//!   `release_step6` refuse one.
//! - **S4, the superseded home** (§4.4: "A slot that only the current map
//!   referenced is reusable as soon as the page's new home is written ...
//!   a writer tells by its ref's epoch, the current one"): a superseded home
//!   is freed iff its entry's epoch is `e`. Every other home may be
//!   referenced by an image and only step 6 frees it.
//! - **S5, two-generation pending frees** (§13 step 6; I-TWO-GEN): "move to
//!   free the slots pending since the previous step 6 ... make pending the
//!   slots that image `k−1` references and image `k` does not".
//! - **A leaf lock** (R-1-memory §4.3 item 1: "Slot allocation takes no
//!   free-set lock and obtains no frame"; §4.2: "slot allocation ... does
//!   not take it"): under the allocator's mutex no other lock is taken,
//!   nothing waits and no hook fires; the slot hook fires after it is
//!   released.
//!
//! Tests `slot_table`, `slot_growth`, `slot_given_init`, `row_i_no_inplace`,
//! `row_i_free_safe`, `t_two_gen`, `t_replica_region`, `evict_steps`; mutants
//! WB-SLOT-NOT-LOWEST, WB-GROW-ONE-SLOT, WB-SUPERSEDE-NEVER,
//! WB-PENDING-NEVER-FREED, FREE-EARLY, FREE-ONE-GEN, REPLICA-SLOT-FREE.

use core::ops::Range;

use nucleus_env::File;

use super::bitmap::Bitmap;
use super::sync::{Arc, Mutex, MutexGuard, Primitives, StdPrimitives};
use crate::meta::Ref;
use crate::store::StoreError;

/// The lowest slot that can be a page's home: slots 0–2 are page 0 and the
/// two meta slots (S-STORE §1, §2, §4.4).
const FIRST_PAGE_SLOT: u64 = 3;

/// The growth floor of S-STORE §4.5: `max(1 MiB, file_len / 16)`.
const GROWTH_FLOOR: u64 = 1 << 20;

/// A hook called with every slot `alloc` hands out (card A-3c, Work 10; the
/// seam of S-STORE §4.4's `t_two_gen`, "with an allocation hook").
pub type SlotHook = Arc<dyn Fn(u64) + Send + Sync>;

/// What a new allocator's bitmaps hold (card A-3c, Work 4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SlotInit {
    /// A fresh file (§2.3): every slot of `[3, slot_count)` is free but the
    /// reserved ones, and nothing is pending.
    Fresh,
    /// The open's state (A-7d; §14 step 2): the free slots and the slots
    /// pending since the previous step 6. Validated: every slot in
    /// `[3, slot_count)`, none reserved, the two lists disjoint, no
    /// duplicate.
    Given {
        /// The slots the image's bitmap marks free.
        free: Vec<u64>,
        /// The slots pending since the previous step 6 (§14 step 2).
        pending: Vec<u64>,
    },
}

/// What a slot is, to the allocator (the diagnostics of Work 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotUse {
    /// Slots 0–2: page 0 and the two meta slots (§2).
    Fixed,
    /// A slot of the replica region (§2.4): never a home, never free.
    Reserved,
    /// Allocatable now.
    Free,
    /// Referenced by the previous image and not the current one, until the
    /// next step 6 frees it (§13 step 6).
    Pending,
    /// Some page's home.
    Used,
    /// At or past `slot_count`.
    PastEnd,
}

/// The allocator's counters (the diagnostics of Work 4).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SlotStats {
    /// The file's length in slots.
    pub slot_count: u64,
    /// How many slots are free.
    pub free: u64,
    /// How many slots are pending.
    pub pending: u64,
    /// How many times the file grew (S2).
    pub growths: u64,
}

/// The allocator's state, under its one mutex.
#[derive(Debug)]
struct SlotState {
    slot_count: u64,
    free: Bitmap,
    pending: Bitmap,
    growths: u64,
}

/// The slot allocator of a write-back pool (S-STORE §4.3, §4.4, §4.5): one
/// bit per slot of `[3, slot_count)`, `set = free`, and the pending
/// generation's bitmap. Public so that this crate's tests drive it alone.
pub struct SlotAlloc<P: Primitives = StdPrimitives> {
    page_size: u32,
    /// The reserved slots of the replica region (§2.4), never free.
    reserved: Range<u64>,
    state: Mutex<P, SlotState>,
    /// The slot hook (card A-3c, `test-hooks`).
    #[cfg(feature = "test-hooks")]
    hook: Mutex<P, Option<SlotHook>>,
}

/// The error a poisoned mutex gives.
fn poisoned() -> StoreError {
    StoreError::Poisoned("the slot allocator's mutex was poisoned by a panic".into())
}

/// Whether `slot` can be a page's home at all (§4.4: "Slots 0–2 and the
/// reserved slots (§2.4) are never page homes").
fn is_page_slot(slot: u64, reserved: &Range<u64>) -> bool {
    slot >= FIRST_PAGE_SLOT && !reserved.contains(&slot)
}

impl<P: Primitives> SlotAlloc<P> {
    /// An allocator for `page_size`-byte slots, `slot_count` of them, with
    /// `reserved` the reserved slots of the replica region (the pool passes
    /// `meta::reserved_slots(page_size)`), holding what `init` says.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if the page size is not allowed, the reserved
    /// range is empty or lies past `slot_count` (§2.4: "the reserved slots
    /// always lie in `[3, slot_count)`"), or `init` names a slot outside
    /// `[3, slot_count)`, a reserved one, one in both lists or one twice;
    /// [`StoreError::OutOfMemory`] if a bitmap cannot be allocated.
    pub fn new(
        page_size: u32,
        slot_count: u64,
        reserved: Range<u64>,
        init: SlotInit,
    ) -> Result<SlotAlloc<P>, StoreError> {
        let bad = |what: &str| StoreError::Invalid(format!("slot allocator: {what}"));
        if !crate::meta::is_page_size(page_size) {
            return Err(bad("the page size is not one of S-STORE §2.1's"));
        }
        if reserved.is_empty() || reserved.start < FIRST_PAGE_SLOT || slot_count < reserved.end {
            return Err(bad(
                "the reserved slots do not lie in [3, slot_count) (S-STORE §2.4)",
            ));
        }
        let mut free = Bitmap::with_len(slot_count)?;
        let mut pending = Bitmap::with_len(slot_count)?;
        match init {
            SlotInit::Fresh => {
                for s in FIRST_PAGE_SLOT..slot_count {
                    if !reserved.contains(&s) {
                        free.set(s, true)?;
                    }
                }
            }
            SlotInit::Given {
                free: given_free,
                pending: given_pending,
            } => {
                // A scratch bitmap tells a repeated slot and one in both
                // lists; it is dropped before the allocator is returned.
                let mut seen = Bitmap::with_len(slot_count)?;
                for (list, what) in [(&given_free, "free"), (&given_pending, "pending")] {
                    for &s in list {
                        if s >= slot_count || !is_page_slot(s, &reserved) {
                            return Err(bad(&format!(
                                "a {what} slot {s} is not a page slot of [3, {slot_count})"
                            )));
                        }
                        if seen.get(s) {
                            return Err(bad(&format!(
                                "slot {s} is listed twice, or in both the free and the pending list"
                            )));
                        }
                        seen.set(s, true)?;
                    }
                }
                for &s in &given_free {
                    free.set(s, true)?;
                }
                for &s in &given_pending {
                    pending.set(s, true)?;
                }
            }
        }
        Ok(SlotAlloc {
            page_size,
            reserved,
            state: Mutex::new(SlotState {
                slot_count,
                free,
                pending,
                growths: 0,
            }),
            #[cfg(feature = "test-hooks")]
            hook: Mutex::new(None),
        })
    }

    /// The state, under its mutex.
    fn lock(&self) -> Result<MutexGuard<'_, P, SlotState>, StoreError> {
        self.state.lock().map_err(|_| poisoned())
    }

    /// The lowest free slot, cleared (S1), or `None` when none is free.
    fn take_lowest(s: &mut SlotState) -> Result<Option<u64>, StoreError> {
        let Some(slot) = s.free.first_set_in(FIRST_PAGE_SLOT, s.slot_count) else {
            return Ok(None);
        };
        s.free.set(slot, false)?;
        Ok(Some(slot))
    }

    /// S2: grow the file by `max(1 MiB, file_len / 16)` rounded down to
    /// whole slots, and mark the new slots free. Called with no slot free
    /// and the mutex held.
    fn grow<H: File>(&self, s: &mut SlotState, file: &H) -> Result<(), StoreError> {
        let ps = u64::from(self.page_size);
        let past = || StoreError::Invalid("the page file's length passed u64".into());
        let old = s.slot_count.checked_mul(ps).ok_or_else(past)?;
        let grow_bytes = old.checked_div(16).ok_or_else(past)?.max(GROWTH_FLOOR);
        // No allowed page size is above 1 MiB, so the floor alone is at
        // least 16 slots; a quotient of 0 would never end a retry.
        let grow_slots = grow_bytes
            .checked_div(ps)
            .filter(|g| *g > 0)
            .ok_or_else(|| StoreError::Invalid("a file growth shorter than one slot".into()))?;
        let new_count = s.slot_count.checked_add(grow_slots).ok_or_else(past)?;
        let new_len = new_count.checked_mul(ps).ok_or_else(past)?;
        // The bitmap's words are reserved before the one I/O call, so a
        // failed reservation changes nothing; a failed `set_len` then leaves
        // only clear bits past `slot_count`, which no reader can see.
        s.free.grow_to(new_count)?;
        s.pending.grow_to(new_count)?;
        file.set_len(new_len).map_err(StoreError::Env)?;
        // None of the new slots is reserved, since `reserved.end` was at or
        // below the old `slot_count`.
        for slot in s.slot_count..new_count {
            s.free.set(slot, true)?;
        }
        s.slot_count = new_count;
        s.growths = s.growths.saturating_add(1);
        Ok(())
    }

    /// Allocate a slot (S-STORE §4.4: "Every page write (eviction or
    /// checkpoint) goes to a fresh slot"): the lowest free one (S1), growing
    /// the file first if none is free (S2). The slot hook fires with the
    /// slot after the mutex is released and before the slot is returned.
    ///
    /// # Errors
    /// [`StoreError::Env`] (58030, 53100) if `set_len` fails;
    /// [`StoreError::OutOfMemory`] if the bitmap cannot grow: the
    /// allocator's state is then unchanged;
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn alloc<H: File>(&self, file: &H) -> Result<u64, StoreError> {
        let slot = {
            let mut s = self.lock()?;
            match Self::take_lowest(&mut s)? {
                Some(slot) => slot,
                None => {
                    self.grow(&mut s, file)?;
                    Self::take_lowest(&mut s)?.ok_or_else(|| {
                        StoreError::Invalid("a grown slot allocator has no free slot".into())
                    })?
                }
            }
        };
        self.fire(slot);
        Ok(slot)
    }

    /// S4: a page's home `old` is superseded, its new home written (Work 7)
    /// or its mapping removed (A-4b's free), in the epoch `e` the caller
    /// holds the gate for. S-STORE §4.4: "A slot that only the current map
    /// referenced is reusable as soon as the page's new home is written ...
    /// which a writer tells by its ref's epoch, the current one, since every
    /// image is built from content of an earlier epoch".
    ///
    /// Why `old.epoch == e` decides it: an entry's epoch is the content
    /// epoch of the bytes at that home, and image `k` holds only epoch-`≤ k`
    /// content (§1, §13 step 2), so a home whose entry carries the current
    /// epoch was written in this epoch with this epoch's content and no
    /// image references it. Every other home may be referenced by an image,
    /// and only step 6 frees it (§13 step 6).
    ///
    /// # Errors
    /// [`StoreError::Invalid`] if `old.slot` is fixed, reserved, past
    /// `slot_count` or already free, or if `old.epoch > e`; nothing changes.
    pub fn supersede(&self, old: Ref, e: u32) -> Result<(), StoreError> {
        if old.slot == 0 {
            return Ok(());
        }
        let bad = |what: &str| StoreError::Invalid(format!("superseded home {old:?}: {what}"));
        let mut s = self.lock()?;
        if !is_page_slot(old.slot, &self.reserved) {
            return Err(bad("its slot is fixed or reserved (S-STORE §4.4)"));
        }
        if old.slot >= s.slot_count {
            return Err(bad("its slot is past slot_count"));
        }
        if s.free.get(old.slot) {
            return Err(bad("its slot is already free"));
        }
        match old.epoch.cmp(&e) {
            core::cmp::Ordering::Equal => {
                // A freed slot is no longer pending either: a slot is in at
                // most one of the two sets.
                s.pending
                    .set(old.slot, false)
                    .map_err(|_| bad("not a slot"))?;
                s.free.set(old.slot, true).map_err(|_| bad("not a slot"))
            }
            core::cmp::Ordering::Less => Ok(()),
            core::cmp::Ordering::Greater => Err(bad("its epoch is above the current one")),
        }
    }

    /// S5: step 6's release, the call A-7c's checkpoint makes with "the slots
    /// that image `k−1` references and image `k` does not" (S-STORE §13
    /// step 6): first every slot pending since the previous step 6 becomes
    /// free, then `newly_pending` is exactly what is pending.
    ///
    /// # Errors
    /// [`StoreError::Invalid`], changing nothing, if a slot of
    /// `newly_pending` is fixed, reserved, past `slot_count`, free, already
    /// pending or listed twice. The call allocates nothing. A pending slot
    /// is one image `k−2` referenced and image `k−1` does not, so it is
    /// never among those image `k−1` references; taking it would leave it
    /// both free (the old generation's move) and pending, and `alloc` could
    /// hand it out while an image may still read it.
    pub fn release_step6(&self, newly_pending: &[u64]) -> Result<(), StoreError> {
        let bad = |what: String| StoreError::Invalid(format!("step 6's release: {what}"));
        let mut s = self.lock()?;
        for &slot in newly_pending {
            if !is_page_slot(slot, &self.reserved) {
                return Err(bad(format!(
                    "slot {slot} is fixed or reserved, and a reserved slot is never free (S-STORE §2.4)"
                )));
            }
            if slot >= s.slot_count {
                return Err(bad(format!("slot {slot} is past slot_count")));
            }
            if s.free.get(slot) {
                return Err(bad(format!("slot {slot} is free")));
            }
            if s.pending.get(slot) {
                return Err(bad(format!(
                    "slot {slot} is already pending, and this release frees it"
                )));
            }
        }
        // A duplicate is told with the free bitmap as a scratch set, which
        // allocates nothing: every listed slot was just found not free, so
        // setting each in turn finds a repeat, and clearing them all
        // restores the bitmap exactly. All of this is under the mutex.
        let mut twice = false;
        for &slot in newly_pending {
            if s.free.get(slot) {
                twice = true;
                break;
            }
            s.free
                .set(slot, true)
                .map_err(|_| bad("a slot past the bitmap".into()))?;
        }
        for &slot in newly_pending {
            s.free
                .set(slot, false)
                .map_err(|_| bad("a slot past the bitmap".into()))?;
        }
        if twice {
            return Err(bad("a slot is listed twice".into()));
        }
        // Move the old generation to free, then make `newly_pending` the
        // pending generation: one bit per slot (R-1-memory §5.2).
        let mut at = s.pending.first_set_in(0, s.slot_count);
        while let Some(slot) = at {
            s.pending.set(slot, false)?;
            s.free.set(slot, true)?;
            at = s.pending.first_set_in(slot.saturating_add(1), s.slot_count);
        }
        for &slot in newly_pending {
            s.pending.set(slot, true)?;
        }
        Ok(())
    }

    /// What slot `s` is (the diagnostics of Work 4).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn state(&self, s: u64) -> Result<SlotUse, StoreError> {
        let held = self.lock()?;
        Ok(if s < FIRST_PAGE_SLOT {
            SlotUse::Fixed
        } else if self.reserved.contains(&s) {
            SlotUse::Reserved
        } else if s >= held.slot_count {
            SlotUse::PastEnd
        } else if held.free.get(s) {
            SlotUse::Free
        } else if held.pending.get(s) {
            SlotUse::Pending
        } else {
            SlotUse::Used
        })
    }

    /// The counters (the diagnostics of Work 4).
    ///
    /// # Errors
    /// [`StoreError::Poisoned`] if the mutex was poisoned.
    pub fn stats(&self) -> Result<SlotStats, StoreError> {
        let held = self.lock()?;
        Ok(SlotStats {
            slot_count: held.slot_count,
            free: held.free.count_ones(),
            pending: held.pending.count_ones(),
            growths: held.growths,
        })
    }

    /// Install or remove the slot hook (`test-hooks` only; Work 10).
    #[cfg(feature = "test-hooks")]
    pub fn set_hook(&self, hook: Option<SlotHook>) {
        if let Ok(mut slot) = self.hook.lock() {
            *slot = hook;
        }
    }

    /// Fire the slot hook with `slot`, if one is set (`test-hooks` only).
    fn fire(&self, slot: u64) {
        #[cfg(feature = "test-hooks")]
        {
            let hook = self.hook.lock().ok().and_then(|s| s.clone());
            if let Some(hook) = hook {
                hook(slot);
            }
        }
        #[cfg(not(feature = "test-hooks"))]
        let _ = slot;
    }
}

impl<P: Primitives> core::fmt::Debug for SlotAlloc<P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // The state is not shown: reading it takes the mutex.
        f.debug_struct("SlotAlloc")
            .field("page_size", &self.page_size)
            .field("reserved", &self.reserved)
            .finish_non_exhaustive()
    }
}
