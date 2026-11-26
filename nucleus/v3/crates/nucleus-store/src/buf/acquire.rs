//! The frame acquisition (S-STORE §5.2 "Victim choice", §5.3's pool-floor
//! sentences in "Why latching cannot deadlock", §5.4 "Below the minimum"; card
//! A-3b-0, Work 5): how a caller obtains a frame once the free list is empty.
//! Steps A1–A7 below are the card's, in its order, and are normative.
//!
//! Invariants owned here:
//! - **Never blocks on a victim** (§5.2: "The evictor takes victims with
//!   `try_latch` and never blocks on a victim: if the latch is not free, it
//!   moves on. Pinned frames are skipped"): the sweep (A2) looks at a frame
//!   under the table mutex and latches it only through `try_victim`, holding
//!   no mutex, and only an unpinned one. Tests `clock_skips`,
//!   `acquire_kept_continues`; mutants KEPT-ENDS-SWEEP.
//! - **Clean victims only, dropped with no write** (§5.2 "Clean victims are
//!   dropped"; §4.2: a freed page's frame leaves "as a clean victim, with no
//!   write"): a dirty victim is passed over at this card, and A-3c writes it.
//!   Tests `clock_evicts_freed_unwritten`, `pool_starved_all_dirty`; mutants
//!   DIRTY-NOT-SKIPPED, EVICT-FREED-WRITES.
//! - **The second chance** (§5.2 "Clock"): the sweep takes `2F` hand positions,
//!   so every frame is visited twice, and a frame whose bit is set loses it
//!   at the first visit and is taken at the second. Tests `clock_order`;
//!   mutants CLOCK-NO-SECOND-CHANCE, SWEEP-ONE-ROTATION, HAND-STAYS.
//! - **The decision is exact at this card**: `PoolStarved` only when every
//!   unpinned frame is dirty, a wait only when every frame is held (§5.4: "A
//!   thread that cannot pin a frame because every frame is pinned waits for
//!   an unpin"), and a retry when a frame was momentarily latched or pinned
//!   (§5.3 "The fairness bound"). It rests on the acquisition's own pass
//!   (A6′; gate ruling A-3b-0 R1), never on the shared hand alone. The
//!   argument is [`Pool::acquire`]'s. Tests `pool_starved_all_dirty`,
//!   `acquire_vs_acquire`, `acquire_shared_hand`,
//!   `acquire_shared_hand_mirrored`, `sweep_end_rechecks_generation`,
//!   `unpin_wait_then_served`, `own_pass_rechecks`; mutants BUSY-AS-HELD,
//!   OWN-PASS-SKIPPED, PASS-NONE-NOT-BUSY, PASS-FRAME0-ONLY, PASS-NO-GEN,
//!   PASS-NO-SCAN (BUSY-AS-STARVED and DECISION-NO-GEN are equivalent since
//!   A6′).
//! - **`try_reserve` never waits nor retries** (§5.3: "with no evictable frame
//!   it releases its latches and restarts"). Test `try_reserve_never_waits`;
//!   mutant TRY-WAITS.
//! - **No lost wake-up** (card A-3b-0, Work 5, A7): a waiter reads the
//!   generation under the same hold of the mutex in which it registers, and
//!   re-tests it after every wake. Tests `unpin_wait_then_served`,
//!   `acquire_vs_acquire`; the notify and count mutants are liveness ones,
//!   which card A-3b judges under the interleaving harness.

use nucleus_env::File;

use super::frame::{FrameId, FrameReservation, FrameState, Removal};
use super::key::PageKey;
use super::pool::{HookPoint, Pool, poisoned_table};
use super::sync::yield_now;
use crate::store::StoreError;

/// The frame a hook of the acquisition fires with when it has none (card
/// A-3b-0, Work 6): no frame has it, since `frames ≤ 2^31`.
const NO_FRAME: FrameId = FrameId(u32::MAX);

/// What one step of the sweep (A2) found under the table mutex.
enum Step {
    /// Not a victim at this visit: not `Resident`, pinned, or given its
    /// second chance.
    Passed,
    /// `Resident`, unpinned and its bit clear: a candidate.
    Candidate(FrameId),
}

/// How a sweep (A2) ended.
enum Sweep<'p, H: File> {
    /// It took no victim; `true` if a frame was momentarily latched or pinned.
    Over(bool),
    /// A clean victim left the page table and is the caller's reservation.
    Found(FrameReservation<'p, H>),
}

/// How the own pass (A6′) ended.
enum Pass<'p, H: File> {
    /// A clean victim left the page table and is the caller's reservation.
    Found(FrameReservation<'p, H>),
    /// Look again from A1: a frame was momentarily latched or pinned, or one
    /// may now be had.
    Retry,
    /// Every frame that was `Resident` with no count was latched by this
    /// acquisition and found dirty, and nothing has changed since A1.
    Starved,
}

impl<H: File> Pool<H> {
    /// Obtain a frame (card A-3b-0, Work 5; S-STORE §5.2, §5.4): the free list
    /// first (A1), then the clock over the clean, unpinned frames (A2), then
    /// the decision (A4–A7). `for_key` is the key a pin's miss (P3) loads, and
    /// names the acquisition in its hooks; `None` for a plain reservation.
    ///
    /// [`Pool::reserve`] is `acquire(true, None)` and may wait for an unpin;
    /// [`Pool::try_reserve`] is `acquire(false, None)` and never does.
    ///
    /// # Errors
    /// [`StoreError::PoolStarved`] when `wait` is set, no frame is free and
    /// every unpinned frame is dirty (at this card; A-3c writes them);
    /// [`StoreError::Invalid`] or [`StoreError::Poisoned`] as `try_victim`,
    /// `remove_from_table` and the table mutex give them.
    pub(crate) fn acquire(
        &self,
        wait: bool,
        for_key: Option<PageKey>,
    ) -> Result<Option<FrameReservation<'_, H>>, StoreError> {
        let key = for_key.unwrap_or(PageKey::Lpid(0));
        loop {
            // A1: the free list.
            let (h0, g0) = {
                let mut t = self.lock_table()?;
                if let Some(f) = t.reserve_free()? {
                    drop(t);
                    return Ok(Some(FrameReservation::reserved(self, f)));
                }
                (t.hand, t.unheld_events)
            };

            // A2: the sweep. `busy` says whether a frame was momentarily
            // latched or pinned while this acquisition looked at it.
            let busy = match self.sweep(h0, key)? {
                Sweep::Found(res) => return Ok(Some(res)),
                Sweep::Over(busy) => busy,
            };

            // A3.
            self.fire(HookPoint::SweepEnded, key, NO_FRAME);

            // A4.
            if !wait {
                return Ok(None);
            }

            // A5, A6 and A7's registration, under one hold of the table mutex:
            // the generation and the scan are read at one instant, and a
            // waiter registers in the hold that reads the generation it will
            // re-test, so no unheld event is lost.
            let mut t = self.lock_table()?;
            if t.free_len() != 0 || t.unheld_events != g0 {
                // A5: a frame is free, or a held frame became unheld since
                // A1, so one the sweep passed over may now be had.
                continue;
            }
            if t.has_unpinned_resident() {
                // A6: some frame is unpinned, so not every frame is held.
                // The hand is shared, so what the sweep saw does not decide:
                // with no frame busy, the acquisition looks itself (A6′).
                drop(t);
                if busy {
                    // Another thread's brief latch on a victim, or a pin whose
                    // drop moved the generation (§5.3's fairness bound): never
                    // block on it, and look again.
                    yield_now();
                    continue;
                }
                match self.own_pass(g0, key)? {
                    Pass::Found(res) => return Ok(Some(res)),
                    Pass::Retry => continue,
                    Pass::Starved => return Err(StoreError::PoolStarved),
                }
            }
            // A7: every frame is held. Wait for an unpin.
            t.unpin_waiters = t
                .unpin_waiters
                .checked_add(1)
                .ok_or_else(|| StoreError::Invalid("unpin waiters passed u32".into()))?;
            let g1 = t.unheld_events;
            drop(t);
            self.fire(HookPoint::UnpinWait, key, NO_FRAME);
            self.wait_unheld(g1)?;
        }
    }

    /// A2, the sweep (card A-3b-0, Work 5): from wherever the hand is, take
    /// steps until it has moved `2F` positions since `h0`, counting other
    /// acquirers' steps. At each step, under the table mutex, the hand names
    /// one frame and advances; a frame that is not `Resident` or is pinned is
    /// passed, a referenced one loses its bit, and any other is latched with
    /// `try_victim` (never blocking), which decides:
    /// - its latch was not free, or it was pinned meanwhile: `busy`;
    /// - dirty: passed over, since this card writes no victim (A-3c does);
    /// - clean: out of the page table by `remove_from_table`. A `Kept` answer
    ///   (a pin arrived) is `busy` and the sweep goes on; a `Removed` one is
    ///   returned to the caller.
    ///
    /// Each step begins with `SweepStep` (card A-3b-0, Work 6; gate ruling
    /// A-3b-0 R1), fired before the table mutex is taken and so before the
    /// test that ends the sweep: with `key`, the key the acquisition is for
    /// or none, and no frame, holding nothing.
    ///
    /// # Why the decision is exact at this card
    /// The sweep ends after the hand has moved `2F` positions since A1,
    /// counting other acquirers' steps, so every frame was visited twice by
    /// someone during it. A frame that is unheld, clean and not re-pinned
    /// throughout loses its bit at the first visit and, at the second, is
    /// taken by whichever acquirer visits it. The bit is set only by a pin,
    /// and every way a frame becomes `Resident` with no count passes through
    /// U1–U4 (module doc of `frame`), so if nothing became unheld since A1
    /// (`g0`), every frame that is unheld at A5 was unheld through the whole
    /// sweep. The shared hand alone does not decide: both of a frame's visits
    /// in the window may be another acquirer's, and that acquirer may hold
    /// the frame's latch between `try_victim` and R2 when this one decides.
    /// So `PoolStarved` rests on the acquisition's own pass (A6′, card
    /// A-3b-0, Work 5; gate ruling A-3b-0 R1). It is returned only if the
    /// pass latched every frame that was `Resident` with no count and found
    /// each one dirty, met no latch it could not take and no `Kept`, and
    /// then, under one hold of the mutex, the generation is still `g0`, no
    /// frame is free and some frame is `Resident` with no count. A frame that
    /// is unheld at that hold has been unheld and `Resident`, with the same
    /// key, since A1, because every way back to unheld passes through U1–U4.
    /// The pass latched it and found it dirty. Only a holder of its exclusive
    /// latch can clean it: a pin holder, whose unpin would have moved the
    /// generation, or a victim's holder. At this card no store path cleans a
    /// victim and leaves it in the table (only tests call
    /// `VictimGuard::mark_written`). So every unpinned frame is dirty. Dirty
    /// frames leave the page table only by A-3c's dirty eviction, so
    /// `PoolStarved` is the right answer here, and A-3c replaces it. With
    /// `busy` true at A6 or A6′, a frame was momentarily latched or pinned by
    /// another thread: an evictor's brief victim latch, or a pin whose drop
    /// moves the generation. A7 waits exactly when every frame is held (§5.4:
    /// "every frame is pinned").
    fn sweep(&self, h0: u64, key: PageKey) -> Result<Sweep<'_, H>, StoreError> {
        let mut busy = false;
        loop {
            self.fire(HookPoint::SweepStep, key, NO_FRAME);
            let step = {
                let mut t = self.lock_table()?;
                let sweep = u64::try_from(t.frames())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(2);
                if t.hand.wrapping_sub(h0) >= sweep {
                    None
                } else {
                    let f = t.clock_frame()?;
                    let m = *t.meta(f)?;
                    if m.state != FrameState::Resident || m.pins > 0 {
                        Some(Step::Passed)
                    } else if m.referenced {
                        t.meta_mut(f)?.referenced = false;
                        Some(Step::Passed)
                    } else {
                        Some(Step::Candidate(f))
                    }
                }
            };
            match step {
                None => return Ok(Sweep::Over(busy)),
                Some(Step::Passed) => {}
                Some(Step::Candidate(f)) => match self.try_victim(f)? {
                    None => busy = true,
                    Some(v) if v.is_dirty() => drop(v),
                    Some(v) => {
                        let k = v.key();
                        match v.remove_from_table()? {
                            Removal::Removed(res) => {
                                self.evicted(k, f)?;
                                return Ok(Sweep::Found(res));
                            }
                            Removal::Kept => busy = true,
                        }
                    }
                },
            }
        }
    }

    /// A6′, the own pass (card A-3b-0, Work 5; gate ruling A-3b-0 R1): the
    /// acquisition's own visit of every frame, in frame order, without moving
    /// the hand and without reading or changing a reference bit. A frame that
    /// is `Resident` with no count is offered to `try_victim` (never blocking)
    /// and the answer is read exactly as A2 reads it: its latch was not free,
    /// or a pin arrived, so `busy`; dirty, so passed over (A-3c writes it);
    /// clean, so out of the page table, counted and announced as A2's victim
    /// is, and returned. The pass fires no `SweepEnded`.
    ///
    /// After the last frame, `busy` means another thread held a latch or a pin
    /// on a frame this acquisition looked at (§5.3 "The fairness bound"), so
    /// it yields and looks again from A1. Otherwise it fires `PassEnded` with
    /// `key` (card A-3b-0-F4, Work 1), holding nothing. Then, under one hold
    /// of the table mutex, a free frame, a moved generation (`g0`) or no
    /// unpinned frame left each makes it look again. Only when none does is
    /// every unpinned frame dirty by this acquisition's own observation, and
    /// `PoolStarved` is the answer (the argument is in [`Pool::sweep`]'s doc,
    /// "Why the decision is exact at this card").
    fn own_pass(&self, g0: u64, key: PageKey) -> Result<Pass<'_, H>, StoreError> {
        let mut busy = false;
        for i in 0..self.frames() {
            let f = FrameId(i);
            let unpinned = {
                let t = self.lock_table()?;
                let m = *t.meta(f)?;
                m.state == FrameState::Resident && m.pins == 0
            };
            if !unpinned {
                continue;
            }
            match self.try_victim(f)? {
                None => busy = true,
                Some(v) if v.is_dirty() => drop(v),
                Some(v) => {
                    let k = v.key();
                    match v.remove_from_table()? {
                        Removal::Removed(res) => {
                            self.evicted(k, f)?;
                            return Ok(Pass::Found(res));
                        }
                        Removal::Kept => busy = true,
                    }
                }
            }
        }
        if busy {
            yield_now();
            return Ok(Pass::Retry);
        }
        self.fire(HookPoint::PassEnded, key, NO_FRAME);
        let look_again = {
            let t = self.lock_table()?;
            t.free_len() != 0 || t.unheld_events != g0 || !t.has_unpinned_resident()
        };
        Ok(if look_again {
            Pass::Retry
        } else {
            Pass::Starved
        })
    }

    /// A2's last step, after a clean victim left the page table (card
    /// A-3b-0, Work 5): count the eviction under the table mutex, then fire
    /// `Evicted` holding only the reservation of that frame.
    fn evicted(&self, k: PageKey, f: FrameId) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        t.evictions = t.evictions.saturating_add(1);
        drop(t);
        self.fire(HookPoint::Evicted, k, f);
        Ok(())
    }

    /// A7's wait (card A-3b-0, Work 5): the caller is registered as a waiter
    /// and has read the generation `g1`. Sleep on `unheld` until a held frame
    /// becomes unheld (the generation moves), re-testing after every wake;
    /// then the caller is no longer a waiter. A poisoned mutex ends the wait
    /// with its error and still takes the caller off the count.
    fn wait_unheld(&self, g1: u64) -> Result<(), StoreError> {
        let mut t = match self.lock_table() {
            Ok(t) => t,
            Err(e) => {
                self.unregister_waiter();
                return Err(e);
            }
        };
        loop {
            if t.unheld_events != g1 {
                break;
            }
            match self.wait_for_unheld(t) {
                Ok(g) => t = g,
                Err(poisoned) => {
                    let mut t = poisoned;
                    t.unpin_waiters = t.unpin_waiters.saturating_sub(1);
                    return Err(poisoned_table());
                }
            }
        }
        t.unpin_waiters = t.unpin_waiters.saturating_sub(1);
        Ok(())
    }

    /// Take the caller off the waiter count when it cannot lock the table.
    fn unregister_waiter(&self) {
        let mut t = self.lock_table_anyway();
        t.unpin_waiters = t.unpin_waiters.saturating_sub(1);
    }
}
