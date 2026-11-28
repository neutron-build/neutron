//! Dirty eviction (card A-3c, Work 7; S-STORE §5.2, §4.1, §4.4, §5.4 "What
//! a pin finds", I-PIN-CURRENT, I-NO-INPLACE): a dirty victim is written to a
//! fresh slot, its entry, or for a MapLeaf its MapInner ref, is set before
//! its frame leaves the page table, under the gate, its old home is
//! superseded by §4.4's rule, and §4.1's pin ends.
//!
//! Invariants owned here, each step quoting S-STORE:
//! - **E0, the victim.** "The evictor takes victims with `try_latch` and
//!   never blocks on a victim" (§5.2): a frame that is not an unpinned
//!   `Resident` page is `NotVictim`; a clean one, a freed page's included
//!   (§4.2: "no eviction ... ever writes it"), leaves with no write.
//! - **E1, the gate.** "This is a gated operation (§5.6), but it never blocks
//!   on the gate: an eviction made inside a gated operation runs under that
//!   operation's hold; any other eviction ... only *tries* to take the gate
//!   shared" (§5.2). A failed try is `FlipPending`: the victim is released
//!   and nothing is written.
//! - **E2, the epoch.** "Dirty victims from the previous epoch" (§5.2) need
//!   the image-record list, A-3c-C's: until then they are passed over
//!   (`PrevEpoch`).
//! - **E3, the seal.** "A page's writer, a dirty eviction (§5.2) ..., sets
//!   `epoch` to the content epoch of the bytes it writes before it seals the
//!   page" (§3.1). The victim's header is the engine's own (§9.2: "a page
//!   that a dirty eviction has sealed" is an engine invariant to check).
//! - **E4, the slot and the write.** "Every page write (eviction or
//!   checkpoint) goes to a fresh slot" (§4.4): the slot `alloc` returned,
//!   never another (I-NO-INPLACE). A failed write leaves the victim dirty and
//!   in the table.
//! - **E5, an LPID victim.** "Its map entry is updated in memory to
//!   `(new slot, content epoch, crc)` before its frame leaves the page
//!   table" (§5.2): entry, clean, superseded home, `EvictEntrySet`, then
//!   removal; a pin that arrived meanwhile keeps it (`Kept`).
//! - **E6, a MapLeaf victim.** §4.1's two-step write: "Under the page's latch
//!   the writer writes the bytes, keeping the frame pinned and in the page
//!   table; it then releases the page's latch, latches the parent
//!   exclusively and sets the ref. An eviction drops the frame only after
//!   that, and only if no one has dirtied or pinned it meanwhile." Nothing
//!   between `mark_written` and the ref's update can refuse on a correct
//!   build: E2 lets only a MapLeaf of content epoch `e` reach E6, and the
//!   parent's update runs no touch check (a MapInner is never copied, §5.6
//!   "When it copies"). What can still fail there is engine damage (a
//!   missing MapInner path, a poisoned mutex), which A-3c-B makes
//!   fail-stop, so the half-written state is never used.
//! - **E8, holds.** "Under a victim's latch takes only map latches" (§5.3):
//!   the victim's MapLeaf's latch or, the victim's released, its MapInner's
//!   mutex; the slot mutex; the page table. No tree latch, no free-set lock,
//!   no wait for a frame.
//! - **The eviction's pins fire nothing** (Work 6.5): E5's and E6's pins are
//!   taken by crate-private twins of `pin_resident` and `pin_frame`, since
//!   neither is an operation's own pin in §5.4's count.
//! - **A write that cleans a victim and leaves it in the table counts as an
//!   unheld event** (card A-3c, Work 8.3; A-3b-0's A6′): `Kept` after a
//!   write bumps the generation and wakes the acquisitions waiting for an
//!   unpin.
//!
//! Tests `evict_steps`, `mapleaf_two_step`, `mappin_residency`,
//! `entry_roundtrip`, `row_i_no_inplace`, `t_pin_load_race_h`,
//! `evict_holds_gate`, `freed_frame_not_written`; mutants INPLACE-EVICT,
//! EVICT-UNLINK-BEFORE-ENTRY, WB-PREV-EPOCH-EVICTED, WB-ENTRY-BEFORE-WRITE,
//! WB-MAPLEAF-UNPINNED-GAP, WB-EVICT-FREED-WRITTEN, MAP-DROP-BEFORE-REF,
//! WB-NO-GATE, WB-GATE-BLOCKING.

use nucleus_env::File;

use super::frame::{FrameId, FrameReservation, Pin, Removal, VictimGuard};
use super::gate::GateHold;
use super::key::PageKey;
use super::pool::{HookPoint, Pool};
use super::sync::{Primitives, StdPrimitives};
use super::wb::Hold;
use crate::meta::Ref;
use crate::page;
use crate::store::StoreError;

/// What [`Pool::evict_frame`] did (card A-3c, Work 7).
#[must_use]
pub enum EvictOutcome<'p, H: File, P: Primitives = StdPrimitives> {
    /// The frame left the page table and is `Reserved` for the caller:
    /// written if it was dirty.
    Obtained(FrameReservation<'p, H, P>),
    /// The frame stays in the page table: a pin arrived meanwhile (clean
    /// after a write).
    Kept,
    /// The frame is pinned, latched, or not `Resident`.
    NotVictim,
    /// The victim is dirty and a flip holds the gate or waits for it: the
    /// caller restarts (S-STORE §5.2).
    FlipPending,
    /// The victim is dirty from before the current epoch: it waits for
    /// A-3c-C's image record.
    PrevEpoch,
}

impl<H: File, P: Primitives> core::fmt::Debug for EvictOutcome<'_, H, P> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            EvictOutcome::Obtained(r) => f.debug_tuple("Obtained").field(r).finish(),
            EvictOutcome::Kept => f.write_str("Kept"),
            EvictOutcome::NotVictim => f.write_str("NotVictim"),
            EvictOutcome::FlipPending => f.write_str("FlipPending"),
            EvictOutcome::PrevEpoch => f.write_str("PrevEpoch"),
        }
    }
}

impl<H: File, P: Primitives> Pool<H, P> {
    /// Evict frame `f` (card A-3c, Work 7): the one entry, which A-3b's
    /// clock calls per candidate (Work 8) and a test calls directly. A clean
    /// victim leaves with no write; a dirty one is written to a fresh slot
    /// under the gate, the caller's hold or a try that never blocks, its
    /// entry or ref is set before its frame leaves the table, and §4.1's pin
    /// ends. It never blocks on a victim.
    ///
    /// # Errors
    /// [`StoreError::Invalid`] for a dirty frame of a pool built with
    /// [`Pool::new`], or where E3's checks or the touch check of an LPID
    /// victim's MapLeaf fail (before anything is changed but the slot);
    /// [`StoreError::Env`] if the write fails (the victim then stays dirty
    /// and in the table); [`StoreError::OutOfMemory`] if the slot allocator
    /// cannot grow; [`StoreError::Poisoned`] if a mutex was poisoned.
    pub fn evict_frame(
        &self,
        f: FrameId,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<EvictOutcome<'_, H, P>, StoreError> {
        let Some(v) = self.try_victim(f)? else {
            return Ok(EvictOutcome::NotVictim);
        };
        self.evict_victim(v, hold)
    }

    /// E1–E9 on a guard `try_victim` granted (Work 7), which A-3b's clock
    /// holds for a dirty candidate (Work 8.2). The gate hold, the caller's
    /// or the try's, lasts until the frame is obtained or kept (E9).
    pub(crate) fn evict_victim<'p>(
        &'p self,
        mut v: VictimGuard<'p, H, P>,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<EvictOutcome<'p, H, P>, StoreError> {
        // E0: a clean victim, a freed page's included, is dropped with no
        // write. This is A-3b's clean path, reached here from a direct call.
        if !v.is_dirty() {
            return Ok(match v.remove_from_table()? {
                Removal::Removed(res) => EvictOutcome::Obtained(res),
                Removal::Kept => EvictOutcome::Kept,
            });
        }
        if self.wb().is_none() {
            // A pool built with `Pool::new` writes nothing (Notes OP-8); its
            // clock passes a dirty frame over before it comes here.
            return Err(StoreError::Invalid(
                "dirty eviction: the pool was not built with write-back (card A-3c)".into(),
            ));
        }
        let f = v.frame();
        let key = v.key();

        // E1: the gate. Under the caller's hold if it has one; else a try
        // that never blocks.
        let tried;
        let gate: &GateHold<'_, H, P> = match hold.gate {
            Some(g) => g,
            None => {
                tried = self.gate_try_shared()?;
                match &tried {
                    Some(h) => h,
                    None => return Ok(EvictOutcome::FlipPending),
                }
            }
        };
        let e = gate.epoch();

        // E2: a victim from before the epoch is A-3c-C's.
        let ce = v.content_epoch();
        if ce < e {
            return Ok(EvictOutcome::PrevEpoch);
        }

        // E3: the seal. The bytes are the engine's own.
        let id = key.header_id()?;
        let h = page::read_header(&v)
            .map_err(|err| StoreError::Invalid(format!("a dirty victim's header does not decode, which the engine made (S-STORE §9.2): {err}")))?;
        if !key.admits(h.kind) || h.id != id {
            return Err(StoreError::Invalid(format!(
                "a dirty victim for {key:?} carries kind {:?} and id {:#x}, which its key does not admit (S-STORE §9.2)",
                h.kind, h.id
            )));
        }
        page::write_header(&mut v, h.kind, h.id, ce)?;
        let crc = page::seal(&mut v)?;

        // E4: the slot and the write. An error returns it with the victim
        // still dirty and in the table; the slot stays allocated, referenced
        // by nothing, and a crash reclaims it.
        let s = self.slots()?.alloc(self.file().as_ref())?;
        self.fire(HookPoint::EvictWrite, key, f);
        let ps = u64::from(self.page_size());
        let off = s
            .checked_mul(ps)
            .ok_or_else(|| StoreError::Invalid("a page write's offset passed u64".into()))?;
        self.file().write_at(off, &v).map_err(StoreError::Env)?;
        self.count_wb_write()?;
        let home = Ref {
            slot: s,
            epoch: ce,
            crc,
        };

        match key {
            // E5: an LPID victim. The entry, then clean.
            PageKey::Lpid(x) => {
                let old = self.set_entry_locked(x, home, e)?;
                v.mark_written(home)?;
                self.slots()?.supersede(old, e)?;
                self.fire(HookPoint::EvictEntrySet, key, f);
                Ok(match v.remove_from_table()? {
                    Removal::Removed(res) => EvictOutcome::Obtained(res),
                    Removal::Kept => {
                        self.cleaned_victim_left()?;
                        EvictOutcome::Kept
                    }
                })
            }
            // E6: a MapLeaf victim, by §4.1's two-step write.
            PageKey::MapLeaf(i) => {
                v.mark_written(home)?;
                let p = self.pin_frame_internal(f, key)?;
                drop(v);
                let old = self.cmap()?.set_mapleaf_ref(i, home, e)?;
                self.slots()?.supersede(old, e)?;
                self.fire(HookPoint::EvictEntrySet, key, f);
                drop(p);
                let Some(again) = self.try_victim(f)? else {
                    return Ok(EvictOutcome::Kept);
                };
                Ok(match again.remove_from_table()? {
                    Removal::Removed(res) => EvictOutcome::Obtained(res),
                    Removal::Kept => {
                        self.cleaned_victim_left()?;
                        EvictOutcome::Kept
                    }
                })
            }
        }
        // E9: a gate hold E1 took is released here, with `tried`.
    }

    /// A write that cleaned a victim and left it in the table (a `Kept` after
    /// E5 or E6) counts as an unheld event, as U1–U4 do (card A-3c, Work
    /// 8.3): under the table mutex, bump the generation and wake the
    /// acquisitions waiting for an unpin.
    fn cleaned_victim_left(&self) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        self.became_unheld(&mut t);
        Ok(())
    }

    /// Work 6.5: E5's pin of the victim's MapLeaf, a crate-private twin of
    /// [`Pool::pin_resident`]. It pins a frame the victim's §4.1 pin already
    /// holds and takes no frame, so it is not an operation's pin and fires
    /// no `Pinned`.
    pub(crate) fn pin_resident_internal(
        &self,
        key: PageKey,
    ) -> Result<Option<Pin<'_, H, P>>, StoreError> {
        key.header_id()?;
        let mut t = self.lock_table()?;
        let Some(f) = t.lookup(key)? else {
            return Ok(None);
        };
        if t.meta(f)?.state != super::frame::FrameState::Resident {
            return Ok(None);
        }
        t.raise(f)?;
        drop(t);
        Ok(Some(Pin::quiet(self, f, key)))
    }

    /// Work 6.5: E6's pin on the MapLeaf victim, a crate-private twin of
    /// A-3a's `VictimGuard::pin_frame`: that frame is the one the acquisition
    /// is obtaining, not an extra one, so it fires no `Pinned`. Called with
    /// the frame's exclusive latch held.
    pub(crate) fn pin_frame_internal(
        &self,
        f: FrameId,
        key: PageKey,
    ) -> Result<Pin<'_, H, P>, StoreError> {
        let mut t = self.lock_table()?;
        t.raise(f)?;
        drop(t);
        Ok(Pin::quiet(self, f, key))
    }
}
