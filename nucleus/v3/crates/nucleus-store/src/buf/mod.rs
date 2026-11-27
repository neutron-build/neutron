//! The buffer manager's frames, page table and pins (S-STORE §5.1–§5.4;
//! card A-3a): a fixed pool whose page table never grows, pins that look in
//! the page table first (§5.4 "What a pin finds"), loads through a page's
//! current reference checked by I-INTEGRITY (§3.3, §4.1, §4.4), the frame
//! state machine, creation under the creator's latch (§6.8 "Root names"), and
//! the step by which a frame leaves the page table, which dirty eviction
//! (A-3c) and freeing (A-4b) build on. Its frame acquisition and pool sizing
//! are card A-3b-0's (`acquire`, `sizing`), whose own module docs state the
//! invariants they own: the clock's second chance, the wait for an unpin and
//! the minimum pool size.
//!
//! No tree uses this module yet: card A-4a moves the tree onto it and
//! retires the seed's `pool` (lib.rs, "Seed status").
//!
//! Card A-3c: the write-back, in pools built with `Pool::with_writeback`, each
//! part in its own module with the invariants it owns: the epoch gate
//! (`gate`, S-STORE §5.6), the current page map and its growth (`cmap`,
//! §4.1), slot allocation with the two-generation pending frees (`slots`,
//! §4.3–§4.5, §13 step 6), LPID allocation under the free-set lock (`lpids`,
//! §4.2, §5.6), dirty eviction to fresh slots (`evict`, §5.2), the caller's
//! `Hold` and the pool's write-back state (`wb`), and the bitmap both free
//! sets use (`bitmap`). A pool built with `Pool::new` keeps A-3a's and A-3b's
//! behaviour exactly: it never writes, takes no §4.1 pin, and answers
//! `Invalid` to every write-back call.
//!
//! Card A-3b: every frame's latch is card A-4c0's task-fair `latch::Latch`
//! (S-STORE §5.3 "Fair latches"), with bypass bound [`FRAME_LATCH_BOUND`] and
//! a fixed waiter queue, beside the frame's content in a cell of the store's
//! sync layer; and the pool itself runs on that layer, `Pool<H, P>` over the
//! backend `P`, the platform's in production and a model checker's in a
//! harness test (S-STORE §15 "The interleaving harness"). So a harness test
//! runs the pool's own waits: `tests/buf_latch.rs` holds `t_root_latch_fair`'s
//! three variants, judged by the harness's monitor, and the pool's waits,
//! where a lost wake-up is a `Deadlock` and not a hang.
//!
//! # Invariants owned here
//!
//! Each names the tests that pin it and the mutants they kill. Test names in
//! `t_pin_load_race_*` are S-STORE §15.1's `t_pin_load_race` at A-3a (on
//! stand-in S3: a test map, and test-only writers, freers, creators and
//! evictor), whose mutant names map to card A-3a's as follows:
//! LOAD-REF-BEFORE-PUBLISH is LOAD-RESOLVE-BEFORE-PUBLISH; LOADING-SERVED is
//! LOADING-AS-RESIDENT and PIN-RESIDENT-LOADING; FAILED-LOAD-KEPT is
//! FAILED-LOAD-KEEPS-KEY; MISS-TWO-FRAMES is MISS-NO-RECHECK;
//! CREATE-BESIDE-FREED is CREATE-FREED-NEW-FRAME; PAGE-KEY-MERGED is
//! NAMESPACE-MERGED.
//!
//! - **BUF-FIXED** (§5.1 "It never grows"; §5.7 "page table | O(F)";
//!   R-1-memory §3, §5.1): `Pool::new` allocates the frames, their buffers,
//!   the page table's buckets and the free list, each with
//!   `try_reserve_exact` (`OutOfMemory`, 53200, on failure), and no `buf`
//!   call allocates or frees memory after it, but for an error's message.
//!   The five states partition the `F` frames: a frame leaves the free list
//!   only through `reserve`, and returns to it when its reservation is
//!   dropped, when a load ends with no count left on it, or when the last
//!   waiter drops its count on an Orphan. Tests `pool_fixed`, `pool_config`,
//!   the model tests, `pool_never_grows` (card A-3b-0); mutants
//!   RESERVATION-LEAK, ORPHAN-LEAK, UNMAPPED-LEAVES-FRAME, POOL-GROW.
//! - **BUF-ONE-FRAME** (§5.1: "The table holds at most one frame for a page"):
//!   a key is in the page table under at most one frame; a frame is in the
//!   table if and only if its state is `Loading` or `Resident`; a frame's
//!   state, key and pin count change only under the table mutex. Tests
//!   `table_chains`, `table_chain_ends`, `hash_placement`, `key_namespaces`,
//!   `double_miss_one_frame`, `t_pin_load_race_e`, `t_pin_load_race_i`, the
//!   model tests; mutants NAMESPACE-MERGED, TABLE-UNLINK-HEAD-ONLY,
//!   TABLE-INSERT-OVERWRITES, MISS-NO-RECHECK, CREATE-OVER-LOADING,
//!   CREATE-OVER-LIVE, CREATE-FREED-NEW-FRAME.
//! - **BUF-TABLE-LEAF** (§4.2, §6.8; C-T0 §2.6): the table mutex is a leaf
//!   lock: while it is held no latch is taken (not even with `try_`), no
//!   `CurrentRef` is called, no I/O is made, no hook fires, no yield is made
//!   and nothing is waited on (the condition-variable wait releases it). Tests
//!   `independent_loads`, `hooks_fire_unlocked`, `acquire_hooks_unlocked`
//!   (and the model's check that no reference read finds the mutex held);
//!   mutant TABLE-HELD-ACROSS-IO.
//! - **BUF-TABLE-FIRST** (§5.4 "What a pin finds"): a pin reads a key's
//!   current reference only when the key is not in the table; it returns
//!   `Unmapped` only when the key was not in the table and its reference has
//!   slot 0, and then it holds no frame and no pin and leaves nothing in the
//!   table. Tests `pin_finds`, `evict_sets_entry_first`,
//!   `t_pin_load_race_j`; mutants ROOT-PIN-MAP-FIRST (S-STORE's, judged here
//!   at draft 7.20 through `t_pin_load_race` (j), and at A-7b),
//!   UNMAPPED-LEAVES-FRAME.
//! - **BUF-LOAD** (§5.4 "The load", I-PIN-CURRENT; §3.3; §4.4): a load
//!   publishes its frame as `Loading` under the key before it reads the
//!   reference; it reads only a page slot; it makes the frame `Resident` only
//!   with bytes that pass `page::verify` against that reference and the key's
//!   header id and whose kind the key admits; on any failure, an error of
//!   its `CurrentRef` included, it removes the frame from the table and
//!   serves nothing; a second pin of a `Loading` key waits for that load and
//!   never reads the slot itself, and `pin_resident` never returns a
//!   `Loading` frame. Tests `load_integrity`, `load_waiter_shares_one_read`,
//!   `stale_load_race`, `failed_load_wakes_waiters`,
//!   `unmapped_load_wakes_waiters`, `t_pin_load_race_a` to `_d`,
//!   `current_ref_error_ends_load`, `reads_counted`, `load_resets_flags`,
//!   `home_slot_bounds`, `key_rules`; mutants LOAD-RESOLVE-BEFORE-PUBLISH,
//!   LOADING-AS-RESIDENT, PIN-RESIDENT-LOADING, WAITER-RETURNS-ORPHAN,
//!   ORPHAN-LEAK, FAILED-LOAD-KEEPS-KEY, VERIFY-SKIPPED, KIND-UNCHECKED,
//!   HOME-SLOT-UNCHECKED, SHORT-READ-SERVED.
//! - **BUF-PINNED-STAYS** (§5.3 "Pins guarantee residency, not identity"): a
//!   frame with a live `Pin` is `Resident` under that pin's key until the pin
//!   drops. Tests `remove_rules`, `remove_vs_pin`, `evict_sets_entry_first`
//!   (b); mutants TRY-VICTIM-IGNORES-PINS, REMOVE-IGNORES-PINS,
//!   PIN-FRAME-NO-COUNT.
//! - **BUF-LEAVE-CLEAN** (§5.2, §5.4: "that write sets the entry before the
//!   frame leaves the page table"; I-PIN-CURRENT): a frame leaves the table by
//!   `remove_from_table` only when it is unpinned and clean; it becomes clean
//!   only by `mark_written` (its bytes sealed for the home its caller wrote,
//!   §3.3's four checks) or by `mark_freed` (§4.2: a freed page's frame is
//!   never written); a page never written since its creation never leaves
//!   the table. Tests `remove_rules`, `mark_written_checks`, `freed_frame`,
//!   `evict_sets_entry_first`, `mark_written_more`, `mark_written_epoch`,
//!   `clock_evicts_freed_unwritten`, `pool_starved_all_dirty` (card A-3b-0);
//!   mutants REMOVE-DIRTY, WRITTEN-UNCHECKED, FREED-STAYS-DIRTY,
//!   EVICT-FREED-WRITES, DIRTY-NOT-SKIPPED.
//! - **BUF-BORN-LATCHED** (§6.8 "Root names"; §15 `t_root_name_reuse` case
//!   D): a created page that enters the table does so under its creator's
//!   exclusive latch, already zeroed and with its header (`kind`, `id`,
//!   `epoch`) written. A freed page's frame is already in the table and is
//!   reused in place (§4.2): its creator claims it in the hold that finds it
//!   freed and latches it only afterwards, and until then the frame stays
//!   open to §6.8's stale-name window, in which a stale name can latch it
//!   and read the old, freed bytes (`is_freed()` is true). No other thread
//!   reads the new page before the creator releases that latch. Either way a
//!   creation of a key a frame holds makes no second frame (§5.4). Tests
//!   `born_latched`, `create_outcomes`, `create_vs_loading`,
//!   `create_vs_freed`, `t_pin_load_race_f`, `t_pin_load_race_g`,
//!   `create_resets_flags`; mutants CREATE-PUBLISH-EARLY,
//!   CREATE-OVER-LOADING, CREATE-FREED-NEW-FRAME, CREATE-FREED-CLAIM-LATE.
//! - **BUF-NO-WRAP**: a pin count never wraps; a pin that would raise it past
//!   `u64::MAX` is refused with `Invalid` and changes nothing. Test
//!   `pin_count_overflow`; mutant PIN-WRAP.
//! - **BUF-TASK-FAIR** (§5.3 "Fair latches"; card A-3b): a frame latch is
//!   task-fair with bypass bound [`FRAME_LATCH_BOUND`]: a waiting exclusive
//!   request blocks new shared grants, a waiter is overtaken by at most `K`
//!   later requests, and `try_latch` never succeeds over a queued exclusive
//!   waiter; the latch makes every lock, wait and wake through the store's
//!   sync layer; no latch allocates after the pool is built (each frame's
//!   queue is allocated with it, `threads` entries, one byte each). Tests
//!   `t_root_latch_fair_reader_stream`, `t_root_latch_fair_writer`,
//!   `t_root_latch_fair_latch_level`, `frame_latch_pinned`,
//!   `pool_fixed`; mutants LATCH-READER-PREF, LATCH-WRITER-PREF,
//!   TRYLATCH-BYPASS, FRAME-BOUND-ZERO, FRAME-CAPACITY-ONE.
//! - **A cell is never waited on** (card A-3b; the sync layer's F7): the
//!   frame's content is taken only under its latch's grant, and each guard
//!   releases the cell before the latch. Test `harness_cell_handoff`; mutant
//!   CELL-RELEASE-ORDER.
//! - **No lost wake-up in the pool's own waits** (cards A-3a and A-3b-0;
//!   judged here since card A-3b): a pin's wait for a load and the frame
//!   acquisition's wait for an unpin end. Tests `harness_load_waiter_woken`,
//!   `harness_unheld_pin_drop`, `harness_unheld_reservation_drop`,
//!   `harness_unheld_failed_load`; mutants LOAD-NO-NOTIFY,
//!   WAIT-NO-RECHECK, UNPIN-NO-NOTIFY, WAIT-NO-REGISTER,
//!   RESERVATION-DROP-NO-NOTIFY, CLEANUP-NO-NOTIFY, each a `Deadlock`.
//!
//! # What later cards add (not here)
//!
//! Card A-3c-B: the poison mark in every acquisition and wait predicate here
//! (§9.2), the broadcast, and the mapping of this module's `Invalid` (every
//! one an engine invariant violation when an engine caller meets it) to XX000
//! (§9.2 at draft 7.20). Card A-3c-C: the epoch-copy reserve sized from
//! `PoolSizing::r_epoch`, the image-record list and step 2, and the write of
//! a dirty victim from before the last flip (§5.2), which `evict` now passes
//! over (`PrevEpoch`); until it lands `acquire`'s `PoolStarved` remains the
//! answer when every unpinned frame is dirty and unwritable. Card A-7a:
//! `write_guard`, whose copy decision `cmap`'s touch check stands in for.

mod acquire;
mod bitmap;
mod cmap;
mod evict;
mod frame;
mod gate;
mod key;
mod lpids;
mod pool;
mod sizing;
mod slots;
mod sync;
mod table;
mod wb;

pub use cmap::{LeafParent, MapShape};
pub use evict::EvictOutcome;
pub use frame::{
    CreatedPage, ExclusiveGuard, FRAME_LATCH_BOUND, FrameId, FrameReservation, FrameState, Pin,
    Removal, SharedGuard, VictimGuard,
};
pub use gate::{FlipGuard, GateHold, ImageRoot};
pub use key::PageKey;
pub use lpids::{AllocRestart, LpidAlloc, LpidStats};
pub use pool::{
    CreateOutcome, CurrentRef, FrameInfo, Hook, HookPoint, PinOutcome, Pool, PoolConfig,
    PoolFootprint, PoolStats,
};
pub use sizing::{
    H_MIN, IMAGE_RECORD_BYTES, OWN_PINNING_THREADS, PoolSizing, STEP2_RESERVE_FRAMES,
};
pub use slots::{SlotAlloc, SlotHook, SlotInit, SlotStats, SlotUse};
pub use wb::{Hold, MapPinInfo, Restart, WriteBackConfig};
