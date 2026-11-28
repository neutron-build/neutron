//! Card A-3c's tests under A-4c0's interleaving harness (Work 14.4; S-STORE
//! §15 "The interleaving harness"): `t_pin_load_race` (k) and its
//! card-local reload variant (S-THETA-F2 (v)), `t_freeset_lock_order` and its
//! boundary variant (I-LATCH), and I-POOL-BOUND's E4 shape.
//!
//! Every test is a scenario generic over `H: Harness`, run through `check`
//! under a stated exploration and step budget; the body builds its fixture
//! inside each execution, on the model checker's backend, so the harness runs
//! the pool's own code: the page table's mutex, both condition variables, the
//! gate, the map's latches, the free-set lock and the yields of the frame
//! acquisition. A pass needs every execution to pass and end within the
//! budget; a `Failure` of any class fails the test. `WbFile`'s own locks are
//! std's, held inside one call, so the model checker sees each as one step.
//! Every pause is placed by a hook and every wait is on a facade condition
//! variable: no test reads a clock.
//!
//! Mutants: LOAD-KEPT-ACROSS-RESTART, WB-GATE-BLOCKING, WB-GATE-WRITER-
//! STARVE (`t_pin_load_race_k`), RELOAD-BEFORE-FLIP (its variant),
//! FREESET-LOCK-IO, FREESET-LOCK-INVERT (`t_freeset_lock_order` and its
//! boundary variant), EVICT-SKIP-DIRTY, POOL-GROW (`row_i_pool_bound_e4`).

mod harness;
mod wb_support;

use std::collections::BTreeSet;
use std::fmt::Debug;
use std::sync::Arc;

use harness::{Bounds, Explore, Harness, Shuttle, check};
use nucleus_store::buf::{
    FrameId, FrameInfo, FrameState, Hold, HookPoint, LpidAlloc, PageKey, PinOutcome, Pool,
    PoolConfig,
};
use nucleus_store::meta::Ref;
use nucleus_store::page;
use nucleus_store::sync::Primitives;
use wb_support::drive::{
    evict_out, s1_flip, s3_free, s3_make_mapleaf, s3_mapleaf_pin, s3_new_page,
};
use wb_support::file::WbFile;
use wb_support::model::{Event, ModelPauser};
use wb_support::track::Tracker;
use wb_support::{H_MIN_TEST, MARK, PS, min_f, pin, pin_read, wb_pool};

use HookPoint::{
    GateExclusiveWaiting, GateSharedBlocking, LoadPublished, LoadWaiting, MapLatchWaiting,
    Restarting, UnpinWait, WaitWoken,
};

/// The key with no page: what the gate's hooks fire with.
const NO_KEY: PageKey = PageKey::Lpid(0);

/// The key of an LPID.
fn lpid(x: u64) -> PageKey {
    PageKey::Lpid(x)
}

/// The key of a MapLeaf.
fn mapleaf(i: u64) -> PageKey {
    PageKey::MapLeaf(i)
}

/// An error as text.
fn said<E: Debug>(e: E) -> String {
    format!("{e:?}")
}

/// A pool on the backend `H` behind an `Arc`, as every thread of a scenario
/// shares it.
type Shared<H> = Arc<Pool<WbFile, H>>;

/// The model checker's identity of the running thread (the `Tracker`'s tag).
fn shuttle_tag() -> usize {
    shuttle::current::get_current_task().map_or(usize::MAX, usize::from)
}

/// Sends `Done(name)` when dropped: at the end of a thread's closure, and on
/// its way out of a panic.
struct DoneOnDrop<H: Harness> {
    pz: Arc<ModelPauser<H>>,
    name: &'static str,
}

impl<H: Harness> Drop for DoneOnDrop<H> {
    fn drop(&mut self) {
        self.pz.done(self.name);
    }
}

/// Whether `e` is `Arrived(point, key, _)`.
fn arrived(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Arrived(p, k, _) if *p == point && *k == key)
}

/// Whether `e` is `Seen(point, key, _)`.
fn seen(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Seen(p, k, _) if *p == point && *k == key)
}

// ------------------------------------------------------------- t_pin_load_race (k)

/// The fixture of `t_pin_load_race_k` and its variant (card A-3c, Work
/// 14.4): `n` 3, `F` 46, fanout 64, `slot_count` 600, epoch 1. Page `L` =
/// LPID 1 (marker `0x11`) under MapLeaf 0 = `M`; `L` evicted, then `M`; then
/// MapLeafs 1..46 made by `s3_make_mapleaf`, empty, dirty and unpinned, so
/// that all 46 frames hold dirty pages and `L` and `M` are not resident.
fn race_k_fixture<H: Harness>() -> Result<Shared<H>, String> {
    assert_eq!(min_f(3), 46);
    let (_file, pool) = wb_pool::<H>(46, 64, 600, 1, H_MIN_TEST)?;
    let (l, p) = s3_new_page(&pool, 0x11).map_err(said)?;
    if l != 1 {
        return Err(format!("L is LPID {l}, not 1"));
    }
    drop(p);
    evict_out(&pool, lpid(1)).map_err(said)?;
    evict_out(&pool, mapleaf(0)).map_err(said)?;
    for i in 1..=46u64 {
        s3_make_mapleaf(&pool, i).map_err(said)?;
    }
    if pool.frame_of(lpid(1)).map_err(said)?.is_some()
        || pool.frame_of(mapleaf(0)).map_err(said)?.is_some()
    {
        return Err("L or M is resident".to_string());
    }
    let st = pool.stats().map_err(said)?;
    if (st.resident, st.free) != (46, 0) {
        return Err(format!("the fixture's pool is {st:?}"));
    }
    Ok(Arc::new(pool))
}

/// T1 of `t_pin_load_race_k`: holds nothing; pins `L`, reads its marker and
/// drops the pin; answers the marker and the epoch after its pin.
fn spawn_t1<H: Harness>(
    pz: &Arc<ModelPauser<H>>,
    pool: &Shared<H>,
) -> <H as Harness>::Handle<Result<(u8, u32), String>> {
    let pz = Arc::clone(pz);
    let pool = Arc::clone(pool);
    H::spawn(move || {
        let _done = DoneOnDrop { pz, name: "T1" };
        let p = match pool.pin_page(lpid(1), &Hold::nothing()).map_err(said)? {
            PinOutcome::Pinned(p) => p,
            PinOutcome::Unmapped => return Err("T1 found L unmapped".to_string()),
        };
        let g = p.latch_shared().map_err(said)?;
        let m = g.get(MARK).copied().unwrap_or(0);
        drop(g);
        drop(p);
        let e = pool.epoch().map_err(said)?;
        Ok((m, e))
    })
}

/// T2 of `t_pin_load_race_k`: takes the gate shared, pins `L` under it,
/// reads its marker, drops both.
fn spawn_t2<H: Harness>(
    pz: &Arc<ModelPauser<H>>,
    pool: &Shared<H>,
) -> <H as Harness>::Handle<Result<u8, String>> {
    let pz = Arc::clone(pz);
    let pool = Arc::clone(pool);
    H::spawn(move || {
        let _done = DoneOnDrop { pz, name: "T2" };
        let g = pool.gate_shared().map_err(said)?;
        let p = match pool.pin_page(lpid(1), &Hold::gated(&g)).map_err(said)? {
            PinOutcome::Pinned(p) => p,
            PinOutcome::Unmapped => return Err("T2 found L unmapped".to_string()),
        };
        let g2 = p.latch_shared().map_err(said)?;
        let m = g2.get(MARK).copied().unwrap_or(0);
        drop(g2);
        drop(p);
        drop(g);
        Ok(m)
    })
}

/// S1 of `t_pin_load_race_k`: the flip.
fn spawn_s1<H: Harness>(
    pz: &Arc<ModelPauser<H>>,
    pool: &Shared<H>,
) -> <H as Harness>::Handle<Result<(), String>> {
    let pz = Arc::clone(pz);
    let pool = Arc::clone(pool);
    H::spawn(move || {
        let _done = DoneOnDrop { pz, name: "S1" };
        s1_flip(&pool).map(|_| ()).map_err(said)
    })
}

/// What the controller of `t_pin_load_race_k` recorded.
struct Seen {
    /// While T1 was at its `Restarting`: the frame holding `L`, if any, T1's
    /// own frame, and that frame's state and count (the non-reload form).
    at_restart: Option<(Option<FrameId>, FrameId, FrameInfo)>,
    /// T1's first event after its first `Restarting` (the reload variant).
    first_after: Option<Event>,
}

/// The scenario of `t_pin_load_race_k` (I-PIN-CURRENT (k); S1 for the flip)
/// and of its card-local reload variant. With `reload`, `Restarting` is
/// reported and not paused, T2 is paused at `WaitWoken`, and the controller
/// waits for T1's next event after its first `Restarting`.
fn race_k<H: Harness>(reload: bool) -> Result<Seen, String> {
    let pool = race_k_fixture::<H>()?;
    let l = lpid(1);
    let pz = Arc::new(ModelPauser::<H>::new());
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, l)?;
    if reload {
        pz.report(Restarting, l)?;
        pz.pause_at(WaitWoken, l)?;
        pz.report(GateSharedBlocking, NO_KEY)?;
    } else {
        pz.pause_at(Restarting, l)?;
    }
    pz.report(LoadWaiting, l)?;
    pz.report(GateExclusiveWaiting, NO_KEY)?;

    // Spawn T1 → Arrived(LoadPublished): T1 holds its count on L's Loading
    // frame, which it obtained by a dirty eviction under a gate try; no
    // latch, no gate, no mutex.
    let t1 = spawn_t1(&pz, &pool);
    let mut t1_frame = None;
    let mut problem = match pz.next_event()? {
        ev if arrived(&ev, LoadPublished, l) => {
            t1_frame = match ev {
                Event::Arrived(_, _, f) => Some(f),
                _ => None,
            };
            None
        }
        ev => Some(format!(
            "T1's first event was {ev:?}, not Arrived(LoadPublished)"
        )),
    };
    // Spawn T2 → Seen(LoadWaiting): T2 holds the gate shared and its count
    // on L's frame.
    let mut t2 = None;
    if problem.is_none() {
        t2 = Some(spawn_t2(&pz, &pool));
        if let ev = pz.next_event()?
            && !seen(&ev, LoadWaiting, l)
        {
            problem = Some(format!(
                "T2's first event was {ev:?}, not Seen(LoadWaiting)"
            ));
        }
    }
    // Spawn S1 → Seen(GateExclusiveWaiting): S1 holds its registration.
    let mut s1 = None;
    if problem.is_none() {
        s1 = Some(spawn_s1(&pz, &pool));
        if let ev = pz.next_event()?
            && !seen(&ev, GateExclusiveWaiting, NO_KEY)
        {
            problem = Some(format!(
                "S1's first event was {ev:?}, not Seen(GateExclusiveWaiting)"
            ));
        }
    }
    let mut out = Seen {
        at_restart: None,
        first_after: None,
    };
    // Release T1.
    if problem.is_none() {
        pz.release(LoadPublished, l)?;
        if !reload {
            // Arrived(Restarting): T1 holds no latch, no gate and no page
            // being loaded. T2 and S1 may end first: once T1's load has
            // ended, T2 goes on to its own load under its gate hold and S1's
            // flip follows, and nothing makes T1 reach its hook before them.
            let mut dones = BTreeSet::new();
            let mut others = 0;
            while problem.is_none() {
                match pz.next_event()? {
                    ev if arrived(&ev, Restarting, l) => {
                        let f_t1 = t1_frame.ok_or("T1's frame was not recorded")?;
                        out.at_restart = Some((
                            pool.frame_of(l).map_err(said)?,
                            f_t1,
                            pool.frame_info(f_t1).map_err(said)?,
                        ));
                        break;
                    }
                    Event::Done(n) => {
                        dones.insert(n);
                    }
                    ev => {
                        others += 1;
                        if others > 16 {
                            problem = Some(format!("an unexpected event: {ev:?}"));
                        }
                    }
                }
            }
            // Done("T2") and Done("S1"), in either order.
            let mut others = 0;
            while dones.len() < 2 && problem.is_none() {
                match pz.next_event()? {
                    Event::Done(n) => {
                        dones.insert(n);
                    }
                    ev => {
                        others += 1;
                        if others > 16 {
                            problem = Some(format!("an unexpected event: {ev:?}"));
                        }
                    }
                }
            }
        } else {
            // T1's first Restarting, as Seen; T2's Arrived(WaitWoken) may
            // come before or after it. Either thread may fire first:
            // `end_load` wakes the waiters and returns, T1 then fires
            // Restarting, while T2 must take the table mutex again and
            // leave its wait before it fires WaitWoken (verdict A-3c R2).
            let mut woken = false;
            let mut others = 0;
            loop {
                match pz.next_event()? {
                    ev if seen(&ev, Restarting, l) => break,
                    ev if !woken && arrived(&ev, WaitWoken, l) => woken = true,
                    ev => {
                        others += 1;
                        if others > 16 {
                            problem = Some(format!("an unexpected event: {ev:?}"));
                            break;
                        }
                    }
                }
            }
            if problem.is_none() {
                // T1's next: GateSharedBlocking, a second Restarting, or a
                // LoadPublished of L. T2's Arrived(WaitWoken), if it did
                // not come before T1's Restarting, is skipped here: it is
                // T2's, not T1's (WaitWoken is a one-shot pause, so it
                // comes once).
                let mut ev = pz.next_event()?;
                if !woken && arrived(&ev, WaitWoken, l) {
                    ev = pz.next_event()?;
                }
                out.first_after = Some(ev);
            }
        }
    }
    // Release everything, join, and only then judge.
    pz.release_all()?;
    let r1 = H::join(t1);
    let r2 = t2.map(H::join);
    let r3 = s1.map(H::join);
    if let Some(problem) = problem {
        return Err(problem);
    }
    let (m1, e1) = r1??;
    let m2 = r2.ok_or("T2 was not started")???;
    r3.ok_or("S1 was not started")???;
    if !(m1 == 0x11 && m2 == 0x11) {
        return Err(format!(
            "T1 read {m1:#x} and T2 read {m2:#x}, where both must read 0x11"
        ));
    }
    if e1 != 2 {
        return Err(format!(
            "T1 read epoch {e1} after its pin, where the flip made it 2"
        ));
    }
    Ok(out)
}

/// The exploration of `t_pin_load_race_k`.
const K_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_000B,
        depth: 3,
        executions: 1000,
    },
    max_steps: 200_000,
};

/// The exploration of `t_pin_load_race_k_reload`.
const K_RELOAD_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_000C,
        depth: 3,
        executions: 1000,
    },
    max_steps: 200_000,
};

/// `t_pin_load_race_k` (I-PIN-CURRENT (k); card A-3c, Work 14.4; S1 for the
/// flip). Mutants LOAD-KEPT-ACROSS-RESTART (a `Deadlock` in every
/// execution, and `frame_of(L)` is `Some` at `Restarting`),
/// WB-GATE-BLOCKING (T1 blocks holding L's Loading frame: a `Deadlock`),
/// WB-GATE-WRITER-STARVE.
#[test]
fn t_pin_load_race_k() {
    let report = check(K_BOUNDS, move || {
        let seen = race_k::<Shuttle>(false)?;
        match seen.at_restart {
            Some((at, f_t1, info)) => {
                // T1's loader's count has left its frame at Restarting. T2
                // may already have taken that frame, the only one free once
                // T1's load ended, for its own load of L under its gate
                // hold: then L is in T1's old frame with T2's one count, and
                // in no other frame.
                if info.pins > 1 || (info.state == FrameState::Loading && info.key != Some(lpid(1)))
                {
                    return Err(format!(
                        "T1's load was still kept at Restarting: L in {at:?}, T1's frame \
                         {f_t1:?} is {info:?}"
                    ));
                }
                match at {
                    Some(f) if f != f_t1 => Err(format!(
                        "L is in {f:?} at Restarting, not in T1's old frame {f_t1:?}"
                    )),
                    _ => Ok(()),
                }
            }
            None => Err("the controller did not reach Restarting".to_string()),
        }
    })
    .map_err(|f| format!("t_pin_load_race_k: {f}"))
    .unwrap();
    println!(
        "t_pin_load_race_k: PCT depth 3 seed 0xA3C0_000B, {} executions (max_steps {})",
        report.executions, K_BOUNDS.max_steps
    );
    assert_eq!(report.executions, 1000, "PCT's executions");
}

/// `t_pin_load_race_k_reload` (card-local until an S-STORE card adds
/// S-THETA-F2 (v); card A-3c, Work 14.4): a thread whose load ended for a
/// pending flip loads again only once the flip has run. Mutant
/// RELOAD-BEFORE-FLIP fails it: a second `Restarting` before the flip.
#[test]
fn t_pin_load_race_k_reload() {
    check(K_RELOAD_BOUNDS, move || {
        let seen = race_k::<Shuttle>(true)?;
        match seen.first_after {
            Some(Event::Seen(GateSharedBlocking, k, FrameId(1))) if k == NO_KEY => Ok(()),
            other => Err(format!(
                "T1's event after its first Restarting was {other:?}, not GateSharedBlocking \
                 with frame 1"
            )),
        }
    })
    .map_err(|f| format!("t_pin_load_race_k_reload: {f}"))
    .unwrap();
}

// ---------------------------------------------------------- t_freeset_lock_order

/// The fixture of `t_freeset_lock_order` and its boundary variant (card
/// A-3c, Work 14.4): `n` 3, `F` 46, fanout 4, `slot_count` 600, epoch 1.
/// Without `boundary`: pages for LPIDs 1..35 (MapLeafs 0..8, all dirty);
/// `s3_free(5)`; pages 4, 6, 7, 8 and page 5's freed frame evicted, then
/// MapLeaf 1, the MapLeaf of the lowest free LPID; MapLeafs 9..16 made,
/// empty and dirty, so that all 46 frames are taken by dirty pages and
/// pinned MapLeafs. (The card names pages 4, 6, 7 and page 5's frame, then
/// MapLeaf 1, and MapLeafs 9..15: page 8 is dirty under MapLeaf 1, whose
/// §4.1 pin keeps the MapLeaf from leaving, so page 8 goes too and one more
/// MapLeaf fills its frame.) With `boundary`: no LPID freed and MapLeaf 1 not evicted;
/// `next_lpid` 36, a MapLeaf boundary at fanout 4; MapLeaf 9 not made;
/// MapLeafs 11 and 12 made instead, to fill the pool.
fn freeset_fixture<H: Harness>(boundary: bool) -> Result<Shared<H>, String> {
    assert_eq!(min_f(3), 46);
    let (_file, pool) = wb_pool::<H>(46, 4, 600, 1, H_MIN_TEST)?;
    for m in 1..=35u64 {
        let (x, p) = s3_new_page(&pool, u8::try_from(m).unwrap_or(u8::MAX)).map_err(said)?;
        if x != m {
            return Err(format!("page {m} got LPID {x}"));
        }
        drop(p);
    }
    if !boundary {
        s3_free(&pool, 5, 4).map_err(said)?;
        for x in [4u64, 6, 7, 8] {
            evict_out(&pool, lpid(x)).map_err(said)?;
        }
        // Page 5's freed frame, clean: it leaves with no write.
        evict_out(&pool, lpid(5)).map_err(said)?;
        evict_out(&pool, mapleaf(1)).map_err(said)?;
        if pool.lpid_stats().map_err(said)?.next_lpid != 36
            || !pool.lpid_is_free(5).map_err(said)?
        {
            return Err("LPID 5 is not the one free LPID".to_string());
        }
        for i in 9..=16u64 {
            s3_make_mapleaf(&pool, i).map_err(said)?;
        }
    } else {
        for i in [11u64, 12] {
            s3_make_mapleaf(&pool, i).map_err(said)?;
        }
        if pool.lpid_stats().map_err(said)?.free != 0 {
            return Err("an LPID is free in the boundary fixture".to_string());
        }
    }
    if pool.lpid_stats().map_err(said)?.next_lpid != 36 || pool.stats().map_err(said)?.free != 0 {
        return Err("the fixture's pool is not full at `next_lpid` 36".to_string());
    }
    Ok(Arc::new(pool))
}

/// A of `t_freeset_lock_order` (holds the gate, page 8 and its latch at its
/// allocations): the first allocation restarts at the MapLeaf of the lowest
/// free LPID (or of `next_lpid`); A makes that MapLeaf resident as the
/// restart does and retries, taking LPID 5 (36 in the boundary variant).
fn freeset_a<H: Primitives>(pool: &Pool<WbFile, H>, boundary: bool) -> Result<(), String> {
    let (want_leaf, want_lpid) = if boundary { (9u64, 36u64) } else { (1, 5) };
    let g = pool.gate_shared().map_err(said)?;
    let p = pin(pool, lpid(8), &Hold::gated(&g)).map_err(said)?;
    // The allocation holds P's latch.
    let gl = p.latch_exclusive().map_err(said)?;
    match pool.alloc_lpids(1, &[], &g).map_err(said)? {
        LpidAlloc::Restart(r) => {
            if (r.leaf, r.emptied.as_slice()) != (want_leaf, &[]) {
                return Err(format!("A's first allocation restarted at {r:?}"));
            }
        }
        LpidAlloc::Taken { lpids, .. } => {
            return Err(format!(
                "A's first allocation took {lpids:?} under a cold MapLeaf"
            ));
        }
    }
    drop(gl);
    drop(p);
    let mut kept = Vec::new();
    if boundary {
        // As Work 13.4's restart does: the frame first, the growth call,
        // `latch_leaf_parent`, the page-table and ref check, `create`.
        kept.push(s3_mapleaf_pin(pool, 9).map_err(said)?);
        let g = pool.gate_shared().map_err(said)?;
        let p = pin(pool, lpid(8), &Hold::gated(&g)).map_err(said)?;
        let gl = p.latch_exclusive().map_err(said)?;
        match pool.alloc_lpids(1, &[9], &g).map_err(said)? {
            LpidAlloc::Taken { lpids, pins } => {
                if lpids != vec![want_lpid] || !pins.is_empty() {
                    return Err(format!("A's retry took {lpids:?} with {} pins", pins.len()));
                }
            }
            LpidAlloc::Restart(r) => return Err(format!("A's retry restarted at {r:?}")),
        }
        drop(gl);
        drop(p);
        drop(g);
    } else {
        // A cold load: its frame comes from a dirty eviction under a gate
        // try.
        kept.push(pin(pool, mapleaf(1), &Hold::nothing()).map_err(said)?);
        let p = pin(pool, lpid(8), &Hold::gated(&g)).map_err(said)?;
        let gl = p.latch_exclusive().map_err(said)?;
        match pool.alloc_lpids(1, &[1], &g).map_err(said)? {
            LpidAlloc::Taken { lpids, pins } => {
                if lpids != vec![want_lpid] || !pins.is_empty() {
                    return Err(format!("A's retry took {lpids:?} with {} pins", pins.len()));
                }
            }
            LpidAlloc::Restart(r) => return Err(format!("A's retry restarted at {r:?}")),
        }
        drop(gl);
        drop(p);
        drop(g);
    }
    drop(kept);
    Ok(())
}

/// The scenario of `t_freeset_lock_order`: A allocates under the gate and a
/// page's latch while B, holding nothing, dirty-evicts the frames of LPIDs
/// 1, 2 and 3, each a slot allocation; the `Tracker` is the hook.
fn freeset_order<H: Harness>(boundary: bool) -> Result<(), String> {
    let pool = freeset_fixture::<H>(boundary)?;
    let tracker = Tracker::new(shuttle_tag);
    pool.set_hook(Some(tracker.hook()));
    let a = {
        let pool = Arc::clone(&pool);
        H::spawn(move || freeset_a(&pool, boundary))
    };
    let b = {
        let pool = Arc::clone(&pool);
        H::spawn(move || {
            // A's own acquisition evicts one dirty page, which may be one
            // of these: a page already gone is passed over, and at least
            // one dirty eviction is made.
            let mut made = 0;
            for x in [1u64, 2, 3] {
                if let Some(f) = pool.frame_of(lpid(x)).map_err(said)?
                    && let nucleus_store::buf::EvictOutcome::Obtained(res) =
                        pool.evict_frame(f, &Hold::nothing()).map_err(said)?
                {
                    drop(res);
                    made += 1;
                }
            }
            if made == 0 {
                return Err("B evicted nothing".to_string());
            }
            Ok::<(), String>(())
        })
    };
    let (r_a, r_b) = (H::join(a), H::join(b));
    let violations = tracker.violations();
    let (freeset_sections, table_sections) = tracker.sections();
    r_a??;
    r_b??;
    // The tracker judges by the seams, so they must have fired: A's two
    // allocations are two free-set sections, and every table access a
    // table section (Work 10).
    if freeset_sections < 2 || table_sections == 0 {
        return Err(format!(
            "the seams fired {freeset_sections} free-set and {table_sections} table sections"
        ));
    }
    if violations.is_empty() {
        Ok(())
    } else {
        Err(format!(
            "the tracker recorded lock order violations: {violations:?}"
        ))
    }
}

/// The exploration of `t_freeset_lock_order`.
const FREESET_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_0F01,
        depth: 3,
        executions: 1000,
    },
    max_steps: 400_000,
};

/// The exploration of `t_freeset_lock_order_boundary`.
const FREESET_BOUNDARY_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_0F02,
        depth: 3,
        executions: 1000,
    },
    max_steps: 400_000,
};

/// `t_freeset_lock_order` (I-LATCH, full; card A-3c, Work 14.4): allocations
/// pin MapLeafs under the free-set lock while dirty evictions allocate slots
/// and a pool at its minimum forces eviction; no page read, eviction, page
/// creation or frame latch is taken while the lock is held, and no take of
/// the lock by a thread holding the page table. Mutant FREESET-LOCK-IO (a)
/// fails it in every execution: the tracker sees `LoadRead` in a free-set
/// section. FREESET-LOCK-INVERT fails it in every execution: B's first
/// dirty eviction fires `FreesetLocking` inside a page-table section.
#[test]
fn t_freeset_lock_order() {
    check(FREESET_BOUNDS, move || freeset_order::<Shuttle>(false))
        .map_err(|f| format!("t_freeset_lock_order: {f}"))
        .unwrap();
}

/// `t_freeset_lock_order_boundary` (card A-3c, Work 14.4): the variant with
/// no free LPID and `next_lpid` at a MapLeaf boundary. Mutant
/// FREESET-LOCK-IO (b) fails it in every execution: the MapLeaf is created
/// under the lock, and the tracker sees `CreateReady` (or the reservation's
/// eviction, `EvictWrite`) in a free-set section.
#[test]
fn t_freeset_lock_order_boundary() {
    check(FREESET_BOUNDARY_BOUNDS, move || {
        freeset_order::<Shuttle>(true)
    })
    .map_err(|f| format!("t_freeset_lock_order_boundary: {f}"))
    .unwrap();
}

// ------------------------------------------------------------- I-POOL-BOUND, E4

/// The scenario of `row_i_pool_bound_e4` (card A-3c, Work 14.4): one model
/// thread; `n` 1, `F` 18, fanout 4, `slot_count` 600, epoch 1. 72 (`4 × F`)
/// iterations of `s3_new_page` with the pin dropped; the pool never grows.
fn pool_bound_e4<H: Primitives>() -> Result<(), String> {
    let (n, f) = (1u32, 18u32);
    assert_eq!(min_f(n), f);
    let want = Pool::<WbFile, H>::bytes_for(PoolConfig {
        frames: f,
        page_size: PS,
        callers: n,
    })
    .map_err(said)?;
    let (_file, pool) = wb_pool::<H>(f, 4, 600, 1, H_MIN_TEST)?;
    if pool.stats().map_err(said)?.frames != f {
        return Err("the pool's frame count is not F".to_string());
    }
    if pool.footprint() != want {
        return Err(format!(
            "the pool's footprint {:?} is not bytes_for's {want:?}",
            pool.footprint()
        ));
    }
    let footprint = pool.footprint();
    let mut markers = Vec::new();
    for i in 0..72u64 {
        let marker = u8::try_from(i % 251).unwrap_or(u8::MAX);
        let (x, p) = s3_new_page(&pool, marker).map_err(said)?;
        if x != i + 1 {
            return Err(format!("iteration {i} got LPID {x}"));
        }
        drop(p);
        if pool.stats().map_err(said)?.frames != f || pool.footprint() != footprint {
            return Err(format!("the pool changed at iteration {i}"));
        }
        markers.push(marker);
    }
    let shape = pool.map_shape().map_err(said)?;
    if shape.root_level != 3 {
        return Err(format!("the map grew to level {}, not 3", shape.root_level));
    }
    for (i, marker) in markers.iter().enumerate() {
        let got = pin_read(&pool, lpid(u64::try_from(i).unwrap_or(u64::MAX) + 1)).map_err(said)?;
        if got != *marker {
            return Err(format!(
                "LPID {} reads back {got:#x}, not its marker {marker:#x}",
                i + 1
            ));
        }
    }
    Ok(())
}

/// The exploration of `row_i_pool_bound_e4`: one thread, so one execution,
/// and the search completes.
const E4_BOUNDS: Bounds = Bounds {
    explore: Explore::Exhaustive { max_executions: 10 },
    max_steps: 3_000_000,
};

/// `row_i_pool_bound_e4` (I-POOL-BOUND, E4 shape: "a dirty set of 4× the
/// pool completes", "the pool never grows"; card A-3c, Work 14.4). Mutant
/// EVICT-SKIP-DIRTY fails it (a deadlock the model checker reports);
/// POOL-GROW fails it at the first frame-count check.
#[test]
fn row_i_pool_bound_e4() {
    let report = check(E4_BOUNDS, pool_bound_e4::<Shuttle>)
        .map_err(|f| format!("row_i_pool_bound_e4: {f}"))
        .unwrap();
    println!(
        "row_i_pool_bound_e4: EXHAUSTIVE, {} executions (max_steps {})",
        report.executions, E4_BOUNDS.max_steps
    );
    assert_eq!(report.executions, 1, "one thread: one execution");
}

// ------------------------------------------------- the waits this card adds

/// The scenario of `dirty_pin_end_wakes_waiter` (card A-3c, Work 6.3, with
/// A-3b-0's A7; the implementer's review). `n` 1, `F` 18, fanout 4,
/// `slot_count` 600, epoch 1. Page `x` (LPID 1) dirty and pinned by the
/// controller; its MapLeaf `M` held only by `x`'s §4.1 pin; the other 16
/// frames reserved by the controller: every frame is held. W, holding
/// nothing, reserves a frame and waits at A7 for an unpin (paused at
/// `UnpinWait`, registered, the generation read). The controller then ends
/// `x`'s §4.1 pin through `x`'s exclusive latch, by `mark_freed` or, with
/// `by_write`, by `mark_written` after sealing the page for a home, keeping
/// its pin on `x`, and releases W. `M` has become an unpinned victim, so W
/// must wake and take `M`'s frame: `Done("W")` is the next event. Without
/// the unheld event at the pin's end, W stays blocked with every other thread
/// waiting for it, and the model checker reports a `Deadlock`.
fn dirty_pin_end<H: Harness>(by_write: bool) -> Result<(), String> {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<H>(18, 4, 600, 1, H_MIN_TEST)?;
    let pool: Shared<H> = Arc::new(pool);
    let (x, p) = s3_new_page(&*pool, 0x51).map_err(said)?;
    if x != 1 {
        return Err(format!("x is LPID {x}, not 1"));
    }
    let m = pool
        .frame_of(mapleaf(0))
        .map_err(said)?
        .ok_or("MapLeaf 0 is not resident")?;
    let mut reserved = Vec::new();
    for _ in 0..16 {
        reserved.push(pool.reserve().map_err(said)?);
    }
    let held = (
        pool.stats().map_err(said)?.free,
        pool.frame_info(m).map_err(said)?.pins,
        pool.map_pin_info(m).map_err(said)?.dirty_pins,
    );
    if held != (0, 1, 1) {
        return Err(format!(
            "the fixture's (free frames, M's count, M's §4.1 pins) are {held:?}"
        ));
    }
    let pz = Arc::new(ModelPauser::<H>::new());
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(UnpinWait, NO_KEY)?;
    let w = {
        let pz = Arc::clone(&pz);
        let pool = Arc::clone(&pool);
        H::spawn(move || {
            let _done = DoneOnDrop { pz, name: "W" };
            pool.reserve_with(&Hold::nothing())
                .map(|r| r.frame())
                .map_err(said)
        })
    };
    let mut problem = None;
    let mut after = None;
    match pz.next_event()? {
        ev if arrived(&ev, UnpinWait, NO_KEY) => {
            // W is registered at A7 and has read the generation. End x's
            // §4.1 pin, M's only count, keeping the pin on x.
            let mut g = p.latch_exclusive().map_err(said)?;
            if by_write {
                let crc = page::seal(&mut g).map_err(said)?;
                g.mark_written(Ref {
                    slot: 3,
                    epoch: 1,
                    crc,
                })
                .map_err(said)?;
            } else {
                g.mark_freed().map_err(said)?;
            }
            drop(g);
            pz.release(UnpinWait, NO_KEY)?;
            after = Some(pz.next_event()?);
        }
        ev => {
            problem = Some(format!(
                "W's first event was {ev:?}, not Arrived(UnpinWait)"
            ))
        }
    }
    pz.release_all()?;
    drop(p);
    let r = H::join(w);
    drop(reserved);
    if let Some(problem) = problem {
        return Err(problem);
    }
    if after != Some(Event::Done("W")) {
        return Err(format!("after the pin's end the next event was {after:?}"));
    }
    let got = r??;
    if got != m {
        return Err(format!("W took {got:?}, not M's frame {m:?}"));
    }
    Ok(())
}

/// The exploration of `dirty_pin_end_wakes_waiter`, for each of its forms.
const DIRTY_PIN_END_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_0D01,
        depth: 3,
        executions: 300,
    },
    max_steps: 200_000,
};

/// `dirty_pin_end_wakes_waiter` (card A-3c, Work 6.3; the implementer's
/// review): a §4.1 pin that was its MapLeaf's last count is an unheld event
/// (U1–U4's second half), so an acquisition waiting at A7 for an unpin wakes
/// when that MapLeaf becomes a victim. Both ends of the pin: `mark_freed`
/// and `mark_written`. Without the event each fails in every execution with
/// a `Deadlock`: the hooks place W's registration before the pin's end.
#[test]
fn dirty_pin_end_wakes_waiter() {
    for by_write in [false, true] {
        let report = check(DIRTY_PIN_END_BOUNDS, move || {
            dirty_pin_end::<Shuttle>(by_write)
        })
        .map_err(|f| format!("dirty_pin_end_wakes_waiter (by_write {by_write}): {f}"))
        .unwrap();
        assert_eq!(report.executions, 300, "PCT's executions");
    }
}

/// The scenario of `map_latch_wait` (card A-3c, Work 3.1's map latch; the
/// implementer's review): the controller holds MapLeaf 0's parent latched
/// (`latch_leaf_parent`); W asks for the same latch and fires
/// `MapLatchWaiting`, its request counted, holding nothing; the controller
/// releases W and gives the latch back; W is then granted it and reads the
/// MapLeaf's ref, null (never written). A give-back that wakes no waiter
/// leaves W blocked wherever it reached its wait before the give-back: a
/// `Deadlock`.
fn map_latch_scenario<H: Harness>() -> Result<(), String> {
    let (_file, pool) = wb_pool::<H>(18, 4, 600, 1, H_MIN_TEST)?;
    let pool: Shared<H> = Arc::new(pool);
    let mut lp = Some(pool.latch_leaf_parent(0).map_err(said)?);
    let pz = Arc::new(ModelPauser::<H>::new());
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(MapLatchWaiting, mapleaf(0))?;
    let w = {
        let pz = Arc::clone(&pz);
        let pool = Arc::clone(&pool);
        H::spawn(move || {
            let _done = DoneOnDrop { pz, name: "W" };
            let lp = pool.latch_leaf_parent(0).map_err(said)?;
            let r = lp.leaf_ref();
            drop(lp);
            Ok::<Ref, String>(r)
        })
    };
    let mut problem = None;
    let mut after = None;
    match pz.next_event()? {
        ev if arrived(&ev, MapLatchWaiting, mapleaf(0)) => {
            pz.release(MapLatchWaiting, mapleaf(0))?;
            drop(lp.take());
            after = Some(pz.next_event()?);
        }
        ev => {
            problem = Some(format!(
                "W's first event was {ev:?}, not Arrived(MapLatchWaiting)"
            ));
        }
    }
    pz.release_all()?;
    drop(lp);
    let r = H::join(w);
    if let Some(problem) = problem {
        return Err(problem);
    }
    if after != Some(Event::Done("W")) {
        return Err(format!("after the give-back the next event was {after:?}"));
    }
    let leaf_ref = r??;
    if !leaf_ref.is_null() {
        return Err(format!("MapLeaf 0's ref is {leaf_ref:?}, not null"));
    }
    Ok(())
}

/// The exploration of `map_latch_wait`.
const MAP_LATCH_BOUNDS: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA3C0_0B0C,
        depth: 3,
        executions: 1000,
    },
    max_steps: 100_000,
};

/// `map_latch_wait` (card A-3c, Work 3.1; the implementer's review): the
/// wait path of a MapInner's map latch (`BodyCell`), which no other test
/// reaches. The hook places W's counted request before the give-back; whether
/// W is already blocked when the latch is given back is the schedule's, so a
/// lost wake-up fails the executions where it is (the RESULT reports how
/// many of the 1000).
#[test]
fn map_latch_wait() {
    let report = check(MAP_LATCH_BOUNDS, map_latch_scenario::<Shuttle>)
        .map_err(|f| format!("map_latch_wait: {f}"))
        .unwrap();
    assert_eq!(report.executions, 1000, "PCT's executions");
}
