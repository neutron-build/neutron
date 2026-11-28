//! `t_pool_floor_scattered` (S-STORE §15.1 I-POOL-BOUND, MAPPIN-KEPT; card
//! A-3c, Work 14.5; R-1-memory §4.3 and §4.4): a pool of exactly the
//! minimum, filled with dirty pages under MapLeafs of their own, each held
//! only by its page's §4.1 pin; no frame acquisition waits for an unpin.
//! S3 for the fill's MapLeafs; OS threads.
//!
//! Numbers, computed here from R-1-memory: `n` 3 (A, B, G), `n_s` 0, `s` 1,
//! `H_min` 5, `R_EPOCH = n + s + 1 = 5`, `R_MAP = n(H_min + 2) + R_EPOCH =
//! 26`, `F = 3n(H_min + 2) + R_EPOCH + R_MAP = 94`. Fanout 2,
//! `slot_count` 600, epoch 1; no flip, no checkpoint, no other caller.
//!
//! Every wait is on an event a hook or a thread sends, or on a release gate
//! the controller opens; every bound is a count.

mod buf_support;
mod wb_support;

use std::sync::{Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;

use buf_support::{Event, Pauser, spawn};
use nucleus_store::StoreError;
use nucleus_store::buf::{Hold, HookPoint, MapPinInfo, PageKey, Pool};
use nucleus_store::sync::StdPrimitives;
use wb_support::drive::{evict_if_resident, s3_new_page, s3_touch};
use wb_support::file::WbFile;
use wb_support::{frame_of, pin, wb_pool};

use HookPoint::UnpinWait;

/// The key with no page: what the frame acquisition's hook fires with.
const NO_KEY: PageKey = PageKey::Lpid(0);

/// The key of an LPID.
fn lpid(x: u64) -> PageKey {
    PageKey::Lpid(x)
}

/// The key of a MapLeaf.
fn mapleaf(i: u64) -> PageKey {
    PageKey::MapLeaf(i)
}

/// Lock a test mutex, recovering from poison.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A thread's release gate: it waits here until the controller opens it.
#[derive(Default)]
struct Release {
    open: Mutex<bool>,
    wake: Condvar,
}

impl Release {
    /// Wait until the gate is open.
    fn wait(&self) {
        let mut open = lock(&self.open);
        while !*open {
            open = self.wake.wait(open).unwrap_or_else(PoisonError::into_inner);
        }
    }

    /// Open the gate.
    fn release(&self) {
        *lock(&self.open) = true;
        self.wake.notify_all();
    }
}

/// What a party holds: its pins (path pages and the reservation's MapLeafs)
/// and its reservations.
type Terms<'p> = (
    Vec<nucleus_store::buf::Pin<'p, WbFile, StdPrimitives>>,
    Vec<nucleus_store::buf::FrameReservation<'p, WbFile, StdPrimitives>>,
);

/// One party's three per-thread terms, as the test makes them through the
/// pool's own calls: `H_min + 2` fill pages pinned and touched (the path
/// term), `H_min + 2` reservations (the reservation's pages) and `H_min + 2`
/// MapLeafs of the fill's tail pinned by loads (the reservation's MapLeafs).
/// `base` names the first MapLeaf whose fill page is pinned and `leafs` the
/// MapLeafs to pin. The party holds `3 × (H_min + 2)` frames, pins and
/// reservations only: no latch, no gate.
fn take_terms(
    pool: &Pool<WbFile, StdPrimitives>,
    base: u64,
    leafs: std::ops::Range<u64>,
    h_min: u64,
    marker: u8,
) -> Result<Terms<'_>, StoreError> {
    let per = h_min + 2;
    let mut pins = Vec::new();
    for k in 0..per {
        // The fill page of MapLeaf `base + k` is LPID `2(base + k) + 1`.
        let p = pin(pool, lpid(2 * (base + k) + 1), &Hold::nothing())?;
        s3_touch(
            pool,
            &p,
            marker.wrapping_add(u8::try_from(k).unwrap_or(u8::MAX)),
        )?;
        pins.push(p);
    }
    let mut reservations = Vec::new();
    for _ in 0..per {
        reservations.push(pool.reserve_with(&Hold::nothing())?);
    }
    // The reservation's MapLeafs: the fill's tail, loads.
    for j in leafs {
        pins.push(pin(pool, mapleaf(j), &Hold::nothing())?);
    }
    Ok((pins, reservations))
}

/// `t_pool_floor_scattered`: MAPPIN-KEPT fails it (`AcquireWaiting` fires).
#[test]
fn t_pool_floor_scattered() {
    // The numbers (R-1-memory §4.2–§4.4), not asked of the build.
    let (n, n_s, s, h_min) = (3u64, 0u64, 1u64, 5u64);
    let r_epoch = n + n_s + s + 1;
    let r_map = n * (h_min + 2) + r_epoch;
    let f = 3 * n * (h_min + 2) + r_epoch + r_map;
    assert_eq!((r_epoch, r_map, f), (5, 26, 94));
    let h_min = u32::try_from(h_min).unwrap();
    assert_eq!(f_min_check(n, h_min), f);
    let f = u32::try_from(f).unwrap();
    assert_eq!(nucleus_store::buf::H_MIN, h_min);
    let (file, pool) = wb_pool::<StdPrimitives>(f, 2, 600, 1, h_min).unwrap();
    assert_eq!(
        pool.sizing().minimum,
        u64::from(f),
        "a pool of exactly the minimum"
    );
    assert_eq!(pool.frames(), f);

    // Setup: LPIDs 1..123 allocated and created by `s3_new_page` (MapLeafs
    // 0..61 made by the growth call), each touched.
    for m in 1..=123u8 {
        let (x, p) = s3_new_page(&pool, m).unwrap();
        assert_eq!(u64::from(m), x);
        s3_touch(&pool, &p, m.wrapping_add(1)).unwrap();
        drop(p);
    }
    assert_eq!(pool.lpid_stats().unwrap().next_lpid, 124);
    assert_eq!(pool.map_shape().unwrap().root_level, 6);
    // Every page and every MapLeaf evicted: all 94 frames free, every page
    // and MapLeaf with a home.
    for x in 1..=123u64 {
        evict_if_resident(&pool, lpid(x)).unwrap();
    }
    for i in 0..=61u64 {
        evict_if_resident(&pool, mapleaf(i)).unwrap();
    }
    assert_eq!(pool.stats().unwrap().free, f);
    assert_eq!(
        pool.wb_writes().unwrap(),
        123 + 62,
        "every page and MapLeaf written"
    );

    // The fill: for i in 0..=46, pin MapLeaf i (a load), pin page 2i + 1 (a
    // load), touch it (its §4.1 pin on MapLeaf i), drop both pins.
    for i in 0..=46u64 {
        let m = pin(&pool, mapleaf(i), &Hold::nothing()).unwrap();
        let p = pin(&pool, lpid(2 * i + 1), &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x80 + u8::try_from(i).unwrap()).unwrap();
        drop(p);
        drop(m);
    }
    // The layout, before the threads start: 47 dirty unpinned pages and
    // their 47 MapLeafs, each MapLeaf held only by its page's §4.1 pin;
    // MapLeafs 48..61 are not resident.
    let st = pool.stats().unwrap();
    assert_eq!((st.resident, st.free, st.reserved), (f, 0, 0));
    for i in 0..=46u64 {
        let fm = frame_of(&pool, mapleaf(i)).unwrap();
        assert_eq!(
            pool.map_pin_info(fm).unwrap(),
            MapPinInfo {
                dirty_pins: 1,
                holds_dirty_pin: false
            },
            "MapLeaf {i} is held only by its page's §4.1 pin"
        );
        let fp = frame_of(&pool, lpid(2 * i + 1)).unwrap();
        assert_eq!(
            pool.map_pin_info(fp).unwrap(),
            MapPinInfo {
                dirty_pins: 0,
                holds_dirty_pin: true
            }
        );
    }
    for i in 48..=61u64 {
        assert_eq!(pool.frame_of(mapleaf(i)).unwrap(), None);
    }

    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    // The pool's wait hook: a frame acquisition about to wait for an unpin.
    pz.report(UnpinWait, NO_KEY);
    let a_release = Release::default();
    let b_release = Release::default();
    let g_release = Release::default();
    let per = 7usize;
    let a_work = || -> Result<(), StoreError> {
        let (pins, reservations) = take_terms(&pool, 1, 48..55, u64::from(h_min), 0xA0)?;
        assert_eq!(pins.len() + reservations.len(), 3 * per);
        pz.done("A ready");
        a_release.wait();
        drop(pins);
        drop(reservations);
        Ok(())
    };
    let b_work = || -> Result<(), StoreError> {
        let (pins, reservations) = take_terms(&pool, 15, 55..62, u64::from(h_min), 0xB0)?;
        assert_eq!(pins.len() + reservations.len(), 3 * per);
        pz.done("B ready");
        b_release.wait();
        drop(pins);
        drop(reservations);
        Ok(())
    };
    let g_work = || -> Result<(), StoreError> {
        // Holding the gate shared, so that a frame acquisition it cannot
        // complete waits for an unpin rather than restarts, as a gated
        // operation's past its first mutation does.
        let g = pool.gate_shared()?;
        let mut reservations = Vec::new();
        for _ in 0..3 * per {
            reservations.push(pool.reserve_with(&Hold::mutated(&g))?);
        }
        pz.done("G");
        g_release.wait();
        drop(reservations);
        drop(g);
        Ok(())
    };
    let (fired, events, r_a, r_b, r_g) = thread::scope(|s| {
        let mut events = Vec::new();
        let a = spawn(s, &pz, "A", a_work);
        // "A ready", or a wait for an unpin.
        events.push(pz.next_event());
        let mut b = None;
        let mut g = None;
        if events.last() == Some(&Event::Done("A ready")) {
            b = Some(spawn(s, &pz, "B", b_work));
            events.push(pz.next_event());
            if events.last() == Some(&Event::Done("B ready")) {
                g = Some(spawn(s, &pz, "G", g_work));
                // `Done("G")`, or a wait for an unpin, from any thread.
                let mut others = 0;
                loop {
                    let ev = pz.next_event();
                    let stop = ev == Event::Done("G") || matches!(ev, Event::Seen(UnpinWait, _, _));
                    events.push(ev);
                    if stop {
                        break;
                    }
                    others += 1;
                    assert!(others < 64, "an unexpected event: {ev:?}");
                }
            }
        }
        let fired = events
            .iter()
            .any(|e| matches!(e, Event::Seen(UnpinWait, _, _)));
        // On `Seen`, record it and release A and B (their drops let G's wait
        // end); release all.
        a_release.release();
        b_release.release();
        g_release.release();
        pz.release_all();
        let r_a = a.join();
        let r_b = b.map(|h| h.join());
        let r_g = g.map(|h| h.join());
        (fired, events, r_a, r_b, r_g)
    });
    assert!(!fired, "AcquireWaiting fired: {events:?}");
    r_a.unwrap().unwrap();
    r_b.unwrap().unwrap().unwrap();
    r_g.unwrap().unwrap().unwrap();
    assert_eq!(pool.stats().unwrap().frames, f);
    let _ = file;
}

/// `F_min` for `n` callers at height floor `h_min`, with `n_s = 0` and
/// `s = 1` (R-1-memory §4.2).
fn f_min_check(n: u64, h_min: u32) -> u64 {
    let per_thread = u64::from(h_min) + 2;
    let r_epoch = n + 2;
    let r_map = n * per_thread + r_epoch;
    3 * n * per_thread + r_epoch + r_map
}
