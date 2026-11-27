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
//!   write"): in a pool built with `Pool::new` a dirty victim is passed over
//!   as at card A-3b; in a write-back pool (card A-3c, Work 8.2) it is
//!   written by `evict_victim` (E1–E9), and the acquisition has its frame
//!   when that answers `Obtained`.
//!   Tests `clock_evicts_freed_unwritten`, `pool_starved_all_dirty`,
//!   `row_i_pool_bound_e4`; mutants DIRTY-NOT-SKIPPED, EVICT-FREED-WRITES,
//!   EVICT-SKIP-DIRTY, POOL-GROW.
//! - **A pending flip is a restart, never a wait** (card A-3c, Work 8.3;
//!   S-STORE §5.2: "If no victim is clean, the thread releases its latches
//!   and restarts (§5.3)"): when the sweep passed a dirty victim over as
//!   `FlipPending` and found no clean one, the acquisition answers
//!   [`Acquired::FlipPending`] whatever the caller holds, unless a frame
//!   came free meanwhile; it never waits for an unpin under a pending flip.
//!   Tests `t_pin_load_race_k`, `t_pin_load_race_k_reload`; mutants
//!   WB-GATE-BLOCKING, WB-NO-GATE, WB-GATE-WRITER-STARVE.
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
//!   which card A-3b judges under the interleaving harness (its
//!   `harness_unheld_pin_drop`, `harness_unheld_reservation_drop` and
//!   `harness_unheld_failed_load`, where a lost wake-up is a `Deadlock`).
//!
//! Card A-3b (Work 5): every lock, wait and yield here is the sync layer's,
//! over the pool's backend `P`, so the interleaving harness runs this code
//! (S-STORE §15). No step moved. The table mutex can no longer be taken
//! "anyway": on a table mutex a panic poisoned, a waiter returns the error
//! with its count still raised, since the sync layer gives no way into a
//! poisoned mutex and the store is poisoned (§9.2).
//!
//! Card A-3c (Work 8.2 and 8.3): the acquisition takes the caller's `Hold`,
//! so that a dirty victim it meets is evicted under a gated caller's hold or
//! a gate try that never blocks; `FlipPending`, `PrevEpoch` and `Kept` move
//! the clock on, as a latched or pinned frame does. A write that cleans a
//! victim and leaves it in the table counts as an unheld event, which
//! `evict_victim` does, so A6′'s argument holds (verdict A-3b-0 (0f83f9c1),
//! follow-up A-3b-0-F1).

use nucleus_env::File;

use super::evict::EvictOutcome;
use super::frame::{FrameId, FrameReservation, FrameState, Removal};
use super::key::PageKey;
use super::pool::{HookPoint, Pool};
use super::sync::{Primitives, yield_now};
use super::wb::Hold;
use crate::store::StoreError;

/// The frame a hook of the acquisition fires with when it has none (card
/// A-3b-0, Work 6): no frame has it, since `frames ≤ 2^31`.
const NO_FRAME: FrameId = FrameId(u32::MAX);

/// A6's and A6′'s yield before the acquisition looks again (card A-3b-0,
/// Work 5), except for an acquisition made while its thread holds a page
/// being loaded, which looks again at once (card A-3c, Work 8; S-STORE §5.3
/// "No latch escapes a call": "a storage call never yields (an `await`, or
/// an N10 cooperative step boundary) while it holds a latch, nor while it
/// holds a page being loaded"). Card A-3c's nested MapLeaf pin (P5's
/// `current_ref`, Work 3.3) is the first acquisition that runs inside a
/// load. It still waits for no other thread's call to end: the latch it
/// found busy is another thread's momentary `try_victim` or pin. Mutant
/// LOAD-ACROSS-YIELD is judged by `t_latch_scope` (A-4c).
fn yield_unless_in_load<H: File, P: Primitives>(hold: &Hold<'_, '_, H, P>) {
    if !hold.in_load {
        yield_now::<P>();
    }
}

/// What the acquisition answered (card A-3c, Work 8.3).
pub(crate) enum Acquired<'p, H: File, P: Primitives> {
    /// A frame, reserved for the caller.
    Frame(FrameReservation<'p, H, P>),
    /// The try form found no frame it could take without blocking
    /// (§5.3: the caller releases its latches and restarts).
    None,
    /// A dirty victim was passed over as `FlipPending` and no clean victim
    /// was found (§5.2: the caller restarts; Work 8.4 says who waits).
    FlipPending,
}

/// What one step of the sweep (A2) found under the table mutex.
enum Step {
    /// Not a victim at this visit: not `Resident`, pinned, or given its
    /// second chance.
    Passed,
    /// `Resident`, unpinned and its bit clear: a candidate.
    Candidate(FrameId),
}

/// How a sweep (A2) ended.
enum Sweep<'p, H: File, P: Primitives> {
    /// It took no victim: `busy` if a frame was momentarily latched, pinned
    /// or kept; `flip_pending` if a dirty victim was passed over as such.
    Over { busy: bool, flip_pending: bool },
    /// A victim left the page table and is the caller's reservation.
    Found(FrameReservation<'p, H, P>),
}

/// What taking one candidate (A2's or the own pass's) did.
enum Taken<'p, H: File, P: Primitives> {
    /// A victim left the page table and is the caller's reservation.
    Got(FrameReservation<'p, H, P>),
    /// Its latch was not free, or a pin arrived.
    Busy,
    /// It is not a victim now, or a dirty one that could not be written.
    Passed,
    /// A dirty one passed over for a pending flip (card A-3c).
    FlipPending,
}

/// How the own pass (A6′) ended.
enum Pass<'p, H: File, P: Primitives> {
    /// A victim left the page table and is the caller's reservation.
    Found(FrameReservation<'p, H, P>),
    /// Look again from A1: a frame was momentarily latched or pinned, or one
    /// may now be had.
    Retry,
    /// Every frame that was `Resident` with no count was latched by this
    /// acquisition and found dirty, and nothing has changed since A1.
    Starved,
    /// The pass passed a dirty victim over as `FlipPending` (card A-3c,
    /// Work 8.3).
    FlipPending,
}

impl<H: File, P: Primitives> Pool<H, P> {
    /// Obtain a frame (card A-3b-0, Work 5; S-STORE §5.2, §5.4): the free list
    /// first (A1), then the clock over the clean, unpinned frames (A2), then
    /// the decision (A4–A7). `for_key` is the key a pin's miss (P3) loads, and
    /// names the acquisition in its hooks; `None` for a plain reservation.
    ///
    /// [`Pool::reserve_with`] is `acquire(true, None, hold)` and may wait for
    /// an unpin; [`Pool::try_reserve_with`] is `acquire(false, None, hold)`
    /// and never does. `hold` is what the caller holds (card A-3c): a dirty
    /// victim is evicted under a gated caller's hold or a try that never
    /// blocks.
    ///
    /// # Errors
    /// [`StoreError::PoolStarved`] when `wait` is set, no frame is free and
    /// every unpinned frame is dirty and cannot be written (a pool built
    /// with `Pool::new`, or a dirty set from before a flip, A-3c-C's);
    /// [`StoreError::Invalid`], [`StoreError::Env`] or
    /// [`StoreError::OutOfMemory`] as a dirty eviction gives them;
    /// [`StoreError::Invalid`] or [`StoreError::Poisoned`] as `try_victim`,
    /// `remove_from_table` and the table mutex give them.
    pub(crate) fn acquire(
        &self,
        wait: bool,
        for_key: Option<PageKey>,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<Acquired<'_, H, P>, StoreError> {
        let key = for_key.unwrap_or(PageKey::Lpid(0));
        loop {
            // A1: the free list.
            let (h0, g0) = {
                let mut t = self.lock_table()?;
                if let Some(f) = t.reserve_free()? {
                    drop(t);
                    return Ok(Acquired::Frame(FrameReservation::reserved(self, f)));
                }
                (t.hand, t.unheld_events)
            };

            // A2: the sweep. `busy` says whether a frame was momentarily
            // latched, pinned or kept while this acquisition looked at it;
            // `flip_pending`, whether a dirty victim was passed over for a
            // pending flip (card A-3c).
            let (busy, flip_pending) = match self.sweep(h0, key, hold)? {
                Sweep::Found(res) => return Ok(Acquired::Frame(res)),
                Sweep::Over { busy, flip_pending } => (busy, flip_pending),
            };

            // A3.
            self.fire(HookPoint::SweepEnded, key, NO_FRAME);

            // A4.
            if !wait {
                return Ok(Acquired::None);
            }

            // Card A-3c, Work 8.3: a flip is pending and the sweep found no
            // clean victim. The thread releases what it holds and restarts
            // (S-STORE §5.2), whatever the hold says, unless a frame is free
            // or became unheld since A1, which A5 would look again at.
            if flip_pending {
                let look_again = {
                    let t = self.lock_table()?;
                    t.free_len() != 0 || t.unheld_events != g0
                };
                if !look_again {
                    return Ok(Acquired::FlipPending);
                }
                continue;
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
                    // block on it, and look again. Without yielding when the
                    // thread holds a page being loaded (the nested MapLeaf
                    // pin of a load; card A-3c, Work 8): S-STORE §5.3, "a
                    // storage call never yields ... while it holds a page
                    // being loaded".
                    yield_unless_in_load(hold);
                    continue;
                }
                match self.own_pass(g0, key, hold)? {
                    Pass::Found(res) => return Ok(Acquired::Frame(res)),
                    Pass::Retry => continue,
                    Pass::Starved => return Err(StoreError::PoolStarved),
                    Pass::FlipPending => return Ok(Acquired::FlipPending),
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
    /// - dirty: passed over in a pool built with `Pool::new`, which writes
    ///   no victim; in a write-back pool written by `evict_victim` (card
    ///   A-3c, Work 8.2), which answers as a clean victim's removal does, or
    ///   `FlipPending`, which the sweep notes;
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
    /// generation, or a victim's holder. At card A-3b-0 no store path cleans
    /// a victim and leaves it in the table (only tests call
    /// `VictimGuard::mark_written`). Card A-3c adds one, E5's and E6's
    /// `Kept`, and closes the gap it would open: that path counts an unheld
    /// event, and a `Kept` is `busy` besides. So every unpinned frame is
    /// dirty and unwritable, in a pool built with `Pool::new` or from before
    /// a flip (A-3c-C's), when `PoolStarved` is the answer; a dirty frame
    /// that can be written is written. With
    /// `busy` true at A6 or A6′, a frame was momentarily latched or pinned by
    /// another thread: an evictor's brief victim latch, or a pin whose drop
    /// moves the generation. A7 waits exactly when every frame is held (§5.4:
    /// "every frame is pinned").
    fn sweep(
        &self,
        h0: u64,
        key: PageKey,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<Sweep<'_, H, P>, StoreError> {
        let mut busy = false;
        let mut flip_pending = false;
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
                None => return Ok(Sweep::Over { busy, flip_pending }),
                Some(Step::Passed) => {}
                Some(Step::Candidate(f)) => match self.take_candidate(f, hold)? {
                    Taken::Got(res) => return Ok(Sweep::Found(res)),
                    Taken::Busy => busy = true,
                    Taken::Passed => {}
                    Taken::FlipPending => flip_pending = true,
                },
            }
        }
    }

    /// A6′, the own pass (card A-3b-0, Work 5; gate ruling A-3b-0 R1): the
    /// acquisition's own visit of every frame, in frame order, without moving
    /// the hand and without reading or changing a reference bit. A frame that
    /// is `Resident` with no count is offered to `try_victim` (never blocking)
    /// and the answer is read exactly as A2 reads it ([`Pool::take_candidate`]).
    /// The pass fires no `SweepEnded`.
    ///
    /// After the last frame, `busy` means another thread held a latch or a pin
    /// on a frame this acquisition looked at (§5.3 "The fairness bound"), so
    /// it yields and looks again from A1. Otherwise it fires `PassEnded` with
    /// `key` (card A-3b-0-F4, Work 1), holding nothing. Then, under one hold
    /// of the table mutex, a free frame, a moved generation (`g0`) or no
    /// unpinned frame left each makes it look again. Only when none does is
    /// every unpinned frame dirty by this acquisition's own observation, and
    /// `PoolStarved` is the answer (the argument is in [`Pool::sweep`]'s doc,
    /// "Why the decision is exact at this card"). Card A-3c, Work 8.3: a
    /// dirty victim passed over for a pending flip makes the answer
    /// `FlipPending` instead, unless there is reason to look again.
    fn own_pass(
        &self,
        g0: u64,
        key: PageKey,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<Pass<'_, H, P>, StoreError> {
        let mut busy = false;
        let mut flip_pending = false;
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
            match self.take_candidate(f, hold)? {
                Taken::Got(res) => return Ok(Pass::Found(res)),
                Taken::Busy => busy = true,
                Taken::Passed => {}
                Taken::FlipPending => flip_pending = true,
            }
        }
        if busy {
            // As at A6: no yield while the thread holds a page being loaded.
            yield_unless_in_load(hold);
            return Ok(Pass::Retry);
        }
        self.fire(HookPoint::PassEnded, key, NO_FRAME);
        let look_again = {
            let t = self.lock_table()?;
            t.free_len() != 0 || t.unheld_events != g0 || !t.has_unpinned_resident()
        };
        Ok(if look_again {
            Pass::Retry
        } else if flip_pending {
            Pass::FlipPending
        } else {
            Pass::Starved
        })
    }

    /// Take one candidate the clock named, or A6′ found unpinned (card
    /// A-3b-0's reading of `try_victim`, with card A-3c's Work 8.2): its
    /// latch was not free, or a pin arrived, so `Busy`; clean, so out of the
    /// page table, counted and announced; dirty, so written by
    /// `evict_victim` in a write-back pool (`Kept` is `Busy`; `FlipPending`
    /// and the rest move the clock on) and passed over in any other.
    fn take_candidate(
        &self,
        f: FrameId,
        hold: &Hold<'_, '_, H, P>,
    ) -> Result<Taken<'_, H, P>, StoreError> {
        let Some(v) = self.try_victim(f)? else {
            return Ok(Taken::Busy);
        };
        let k = v.key();
        if !v.is_dirty() {
            return Ok(match v.remove_from_table()? {
                Removal::Removed(res) => {
                    self.evicted(k, f)?;
                    Taken::Got(res)
                }
                Removal::Kept => Taken::Busy,
            });
        }
        if self.wb().is_none() {
            // A pool built with `Pool::new` writes nothing: A-3b's
            // behaviour, unchanged (card A-3c, Work 9).
            drop(v);
            return Ok(Taken::Passed);
        }
        Ok(match self.evict_victim(v, hold)? {
            EvictOutcome::Obtained(res) => {
                self.evicted(k, f)?;
                Taken::Got(res)
            }
            EvictOutcome::Kept => Taken::Busy,
            EvictOutcome::FlipPending => Taken::FlipPending,
            EvictOutcome::PrevEpoch | EvictOutcome::NotVictim => Taken::Passed,
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
    /// then the caller is no longer a waiter.
    ///
    /// Card A-3b, Work 5: a poisoned table mutex ends the wait with its
    /// error, and the caller's count stays raised, since the sync layer
    /// gives no way into a poisoned mutex (its F5). The pool is then
    /// unusable, rightly: the panic poisoned the store (S-STORE §9.2).
    fn wait_unheld(&self, g1: u64) -> Result<(), StoreError> {
        let mut t = self.lock_table()?;
        loop {
            if t.unheld_events != g1 {
                break;
            }
            t = self.wait_for_unheld(t)?;
        }
        t.unpin_waiters = t.unpin_waiters.saturating_sub(1);
        Ok(())
    }
}
