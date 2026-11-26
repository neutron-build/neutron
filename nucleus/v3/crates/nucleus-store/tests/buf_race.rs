//! Card A-3a's race tests of `buf` (Work 12), and S-STORE §15.1's
//! `t_pin_load_race` at A-3a (draft 7.20, I-PIN-CURRENT: cases (a) to (g),
//! (i) and (j) on stand-in S3, a test map and test-only callers).
//!
//! Each test: one fixture, `std::thread::scope`, a `Pauser` installed with
//! `set_hook`. Every pause names what each party holds there. The controller
//! records what it sees in locals, releases every paused thread, joins every
//! thread, and only then asserts; while a thread is paused it calls the pool
//! only in ways that cannot wait on what that thread holds (`frame_of`,
//! `frame_info`, `pin_resident`, `table_free`, `try_latch_*`, `stats`, or
//! calls on another key's or its own reserved frame). When an event other
//! than the expected one comes, it records it, releases all and joins. No
//! test waits on time: every wait is on an event a hook or a thread sends.

mod buf_support;

use std::thread;

use buf_support::{
    Event, FaultFile, MARK, Made, Pauser, SPOIL, TestMap, create_as_allocation, evict_to, fixture,
    marker, offset, page, put, put_leaf, spawn,
};
use nucleus_env::File;
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, FrameId, FrameInfo, FrameState, HookPoint, PageKey, Pin, PinOutcome, Pool,
    Removal,
};
use nucleus_store::meta::Ref;
use nucleus_store::page::Kind;

use HookPoint::{
    CreateReady, FreedClaimed, LoadPublished, LoadRead, LoadResolved, LoadWaiting, MissReserved,
    Pinned, RemoveLatched, WaitWoken,
};

const L1: PageKey = PageKey::Lpid(1);
const L2: PageKey = PageKey::Lpid(2);

/// Pin `key`, read its frame and marker, drop the pin; `None` if unmapped.
fn pin_read(
    pool: &Pool<FaultFile>,
    map: &TestMap,
    key: PageKey,
) -> Result<Option<(FrameId, u8)>, StoreError> {
    match pool.pin(key, map)? {
        PinOutcome::Pinned(p) => Ok(Some((p.frame(), marker(&p)?))),
        PinOutcome::Unmapped => Ok(None),
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

/// The frame an `Arrived` or `Seen` event names.
fn frame_of_event(e: &Event) -> Option<FrameId> {
    match e {
        Event::Arrived(_, _, f) | Event::Seen(_, _, f) => Some(*f),
        _ => None,
    }
}

/// Whether `r` is a `Corrupt` error.
fn is_corrupt<T>(r: &Result<T, StoreError>) -> bool {
    matches!(r, Err(StoreError::Corrupt(_)))
}

#[test]
fn load_waiter_shares_one_read() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, L1);
    pz.report(LoadWaiting, L1);
    let (first, probe_none, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut probe_none, mut second, mut t2) = (None, None, None);
        if arrived(&first, LoadResolved, L1) {
            probe_none = Some(pool.pin_resident(L1).map(|p| p.is_none()));
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            // Either T2 waits (holding its count on T1's frame; no latch, no
            // mutex) or it ends.
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, probe_none, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, L1), "{first:?}");
    assert!(matches!(probe_none, Some(Ok(true))), "probe {probe_none:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    let (f1, m1) = r1.unwrap().unwrap().unwrap();
    let (f2, m2) = r2.unwrap().unwrap().unwrap().unwrap();
    assert_eq!(f1, f2);
    assert_eq!((m1, m2), (0x11, 0x11));
    assert_eq!(file.reads(), 1);
    assert_eq!(pool.stats().unwrap().reads, 1);
}

#[test]
fn double_miss_one_frame() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(MissReserved, L1);
    let (first, at_miss, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its reservation, not in the table; no latch, no mutex.
        let first = pz.next_event();
        let (mut at_miss, mut second, mut t2) = (None, None, None);
        if arrived(&first, MissReserved, L1) {
            at_miss = Some((
                frame_of_event(&first).map(|f| pool.frame_info(f)),
                pool.frame_of(L1).map_err(|e| e.to_string()),
            ));
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, at_miss, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, MissReserved, L1), "{first:?}");
    let (info, in_table) = at_miss.unwrap();
    assert_eq!(info.unwrap().unwrap().state, FrameState::Reserved);
    assert_eq!(in_table, Ok(None));
    assert_eq!(second, Some(Event::Done("T2")));
    let (f1, m1) = r1.unwrap().unwrap().unwrap();
    let (f2, m2) = r2.unwrap().unwrap().unwrap().unwrap();
    assert_eq!(f1, f2, "T1 is served T2's frame");
    assert_eq!((m1, m2), (0x11, 0x11));
    assert_eq!(file.reads(), 1);
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.reserved, st.resident), (2, 0, 1));
}

#[test]
fn stale_load_race() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0xA1).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, L1);
    pz.report(LoadWaiting, L1);
    let t2_work = || -> Result<(), StoreError> {
        let PinOutcome::Pinned(p) = pool.pin(L1, &map)? else {
            return Err(StoreError::Invalid("L1 unmapped".into()));
        };
        {
            let mut g = p.latch_exclusive()?;
            g.mark_dirty(1)?;
            g[MARK] = 0xB2;
        }
        let f = p.frame();
        drop(p);
        // T1 may still hold its pin; the eviction is then skipped.
        evict_to(&pool, &map, f, 11)?;
        Ok(())
    };
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadResolved, L1) {
            t2 = Some(spawn(s, &pz, "T2", t2_work));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, L1), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    r1.unwrap().unwrap();
    r2.unwrap().unwrap().unwrap();
    let p = match pool.pin(L1, &map) {
        Ok(PinOutcome::Pinned(p)) => p,
        other => panic!("expected Pinned, got {other:?}"),
    };
    assert_eq!(marker(&p).unwrap(), 0xB2);
}

#[test]
fn failed_load_wakes_waiters() {
    let (file, map, pool) = fixture(2).unwrap();
    let (mut b, crc) = page(Kind::Leaf, 1, 1, 0x11).unwrap();
    b[SPOIL] ^= 0xFF;
    file.write_at(offset(10), &b).unwrap();
    map.set(
        L1,
        Ref {
            slot: 10,
            epoch: 1,
            crc,
        },
    );
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadRead, L1);
    pz.pause_at(WaitWoken, L1);
    pz.report(LoadWaiting, L1);
    let (first, second, rest, at_woken, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its count and the frame's exclusive latch; no mutex.
        let first = pz.next_event();
        let (mut second, mut rest, mut at_woken, mut t2) = (None, Vec::new(), None, None);
        if arrived(&first, LoadRead, L1) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            // T2 holds its count on the Loading frame; no latch, no mutex.
            let e = pz.next_event();
            second = Some(e);
            if seen(&e, LoadWaiting, L1) {
                pz.release(LoadRead, L1);
                // Until both Done("T1") and Arrived(WaitWoken) have come; at
                // WaitWoken T2 holds its count; no latch, no mutex.
                let (mut t1_done, mut woken) = (false, None);
                while !(t1_done && woken.is_some()) {
                    let e = pz.next_event();
                    rest.push(e);
                    if e == Event::Done("T1") && !t1_done {
                        t1_done = true;
                    } else if arrived(&e, WaitWoken, L1) && woken.is_none() {
                        woken = frame_of_event(&e);
                    } else {
                        break;
                    }
                }
                if t1_done && let Some(f) = woken {
                    at_woken = Some((
                        pool.frame_info(f).map_err(|e| e.to_string()),
                        pool.frame_of(L1).map_err(|e| e.to_string()),
                    ));
                }
            }
        }
        pz.release_all();
        (
            first,
            second,
            rest,
            at_woken,
            t1.join(),
            t2.map(|h| h.join()),
        )
    });
    assert!(arrived(&first, LoadRead, L1), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    let (info, in_table) = at_woken.unwrap_or_else(|| panic!("events after release: {rest:?}"));
    let info = info.unwrap();
    assert_eq!((info.state, info.pins), (FrameState::Orphan, 1), "{info:?}");
    assert_eq!(in_table, Ok(None));
    assert!(is_corrupt(&r1.unwrap()));
    assert!(is_corrupt(&r2.unwrap().unwrap()));
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.orphan, st.loading), (2, 0, 0));
    assert_eq!(file.reads(), 2);
}

#[test]
fn unmapped_load_wakes_waiters() {
    let (file, map, pool) = fixture(2).unwrap();
    let l9 = PageKey::Lpid(9);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, l9);
    pz.report(LoadWaiting, l9);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, l9));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadPublished, l9) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, l9)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, l9), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, l9)),
        "{second:?}"
    );
    assert_eq!(r1.unwrap().unwrap(), None);
    assert_eq!(r2.unwrap().unwrap().unwrap(), None);
    assert_eq!(pool.stats().unwrap().free, 2);
    assert_eq!(pool.frame_of(l9).unwrap(), None);
    assert_eq!(map.lookups(), 2);
    assert_eq!(file.reads(), 0);
}

#[test]
fn create_vs_loading() {
    let (_file, map, pool) = fixture(3).unwrap();
    let l7 = PageKey::Lpid(7);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, l7);
    let (first, outcome, r1, r3) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, l7));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut outcome, mut t3) = (None, None);
        if arrived(&first, LoadPublished, l7) {
            // The controller latches only its own reserved frame, inside
            // `create`.
            outcome = Some(pool.create(pool.reserve().unwrap(), l7, Kind::Leaf, 1));
            t3 = Some(spawn(s, &pz, "T3", || pool.wait_settled(l7)));
        }
        pz.release_all();
        (first, outcome, t1.join(), t3.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, l7), "{first:?}");
    let Some(Ok(CreateOutcome::Loading(res))) = outcome else {
        panic!("the first create must give Loading, got {outcome:?}");
    };
    assert_eq!(r1.unwrap().unwrap(), None);
    r3.unwrap().unwrap().unwrap();
    let c = match pool.create(res, l7, Kind::Leaf, 1) {
        Ok(CreateOutcome::Created(c)) => c,
        other => panic!("expected Created, got {other:?}"),
    };
    let info = pool.frame_info(c.frame()).unwrap();
    assert_eq!((info.state, info.pins), (FrameState::Resident, 1));
    assert!(c.is_dirty());
}

#[test]
fn born_latched() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let l3 = PageKey::Lpid(3);
    let f0 = match pool.pin(L1, &map) {
        Ok(PinOutcome::Pinned(p)) => p.frame(),
        other => panic!("expected Pinned, got {other:?}"),
    };
    let v = pool.try_victim(f0).unwrap().unwrap();
    let Removal::Removed(res) = v.remove_from_table().unwrap() else {
        panic!("a clean, unpinned frame is removed");
    };
    // f0 tops the free list, still holding L1's bytes.
    drop(res);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(CreateReady, l3);
    let (first, in_table, resident, latch_probe, made) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || {
            let res = pool.reserve()?;
            match pool.create(res, l3, Kind::Leaf, 1)? {
                CreateOutcome::Created(c) => Ok(c.release()),
                _ => Err(StoreError::Invalid("not created".into())),
            }
        });
        // T1 holds f0's exclusive latch; no pin, no mutex.
        let first = pz.next_event();
        let (mut in_table, mut resident, mut latch_probe) = (None, None, None);
        if arrived(&first, CreateReady, l3) {
            in_table = Some(pool.frame_of(l3).map_err(|e| e.to_string()));
            let p = pool.pin_resident(l3);
            resident = Some(p.as_ref().map(Option::is_some).map_err(|e| e.to_string()));
            if let Ok(Some(p)) = p {
                latch_probe = Some(match p.try_latch_shared() {
                    Ok(Some(g)) => Some(g[8..16].to_vec()),
                    _ => None,
                });
            }
        }
        pz.release_all();
        (first, in_table, resident, latch_probe, t1.join())
    });
    assert!(arrived(&first, CreateReady, l3), "{first:?}");
    assert_eq!(frame_of_event(&first), Some(f0));
    assert_eq!(
        in_table,
        Some(Ok(None)),
        "the page is not in the table at CreateReady"
    );
    assert_eq!(resident, Some(Ok(false)), "probe {latch_probe:?}");
    let pin = made.unwrap().unwrap();
    assert_eq!(pin.frame(), f0);
    let g = pin.latch_shared().unwrap();
    assert_eq!(g[4], 3);
    assert_eq!(g[8..16], 3u64.to_le_bytes());
    assert_eq!(g[16..20], 1u32.to_le_bytes());
    assert!(g[24..].iter().all(|&b| b == 0));
}

#[test]
fn remove_vs_pin() {
    let (file, map, pool) = fixture(2).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let f = match pool.pin(L1, &map) {
        Ok(PinOutcome::Pinned(p)) => p.frame(),
        other => panic!("expected Pinned, got {other:?}"),
    };
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(RemoveLatched, L1);
    let (first, p, kept) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || {
            let v = pool.try_victim(f)?.ok_or(StoreError::PoolStarved)?;
            Ok::<bool, StoreError>(match v.remove_from_table()? {
                Removal::Kept => true,
                Removal::Removed(res) => {
                    drop(res);
                    false
                }
            })
        });
        // T1 holds f's exclusive latch; no pin, no mutex.
        let first = pz.next_event();
        let mut p = None;
        if arrived(&first, RemoveLatched, L1) {
            p = Some(pool.pin_resident(L1));
        }
        pz.release_all();
        (first, p, t1.join())
    });
    assert!(arrived(&first, RemoveLatched, L1), "{first:?}");
    let p = p
        .unwrap()
        .unwrap()
        .expect("L1 is resident while T1 holds its latch");
    assert!(
        kept.unwrap().unwrap(),
        "T1 must keep the frame it found pinned"
    );
    assert_eq!(pool.frame_of(L1).unwrap(), Some(p.frame()));
    let info = pool.frame_info(f).unwrap();
    assert_eq!((info.state, info.pins), (FrameState::Resident, 1));
    assert_eq!(marker(&p).unwrap(), 0x11);
}

#[test]
fn independent_loads() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    put_leaf(&file, &map, 2, 11, 0x22).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadRead, L1);
    let (first, free, l2, r1) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its count and f1's exclusive latch; no mutex.
        let first = pz.next_event();
        let (mut free, mut l2) = (None, None);
        if arrived(&first, LoadRead, L1) {
            let f = pool.table_free();
            free = Some(f);
            if f {
                l2 = Some(pin_read(&pool, &map, L2));
            }
        }
        pz.release_all();
        (first, free, l2, t1.join())
    });
    assert!(arrived(&first, LoadRead, L1), "{first:?}");
    assert_eq!(
        free,
        Some(true),
        "the table mutex is free during a load's I/O"
    );
    assert_eq!(l2.unwrap().unwrap().unwrap().1, 0x22);
    assert_eq!(r1.unwrap().unwrap().unwrap().1, 0x11);
}

// ---- S-STORE §15.1's `t_pin_load_race` at A-3a (draft 7.20, §15
// I-PIN-CURRENT). Hooks: *published* is `LoadPublished`, *resolved*
// `LoadResolved`, *missed* `MissReserved`, *waiting* `LoadWaiting`.

/// (a) T1 paused at *resolved*; T2 writes `m2` and a dirty eviction moves
/// `L` to a fresh slot, `s1` unchanged. T2 must wait first, and a later pin
/// reads `m2`. LOAD-REF-BEFORE-PUBLISH fails it.
#[test]
fn t_pin_load_race_a() {
    let (file, map, pool) = fixture(3).unwrap();
    let (s1, m1, m2, fresh) = (10, 0xA1, 0xB2, 11);
    put_leaf(&file, &map, 1, s1, m1).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, L1);
    pz.report(LoadWaiting, L1);
    let t2_work = || -> Result<(), StoreError> {
        let PinOutcome::Pinned(p) = pool.pin(L1, &map)? else {
            return Err(StoreError::Invalid("L unmapped".into()));
        };
        {
            let mut g = p.latch_exclusive()?;
            g.mark_dirty(1)?;
            g[MARK] = m2;
        }
        let f = p.frame();
        drop(p);
        evict_to(&pool, &map, f, fresh)?;
        Ok(())
    };
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its pin on its Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadResolved, L1) {
            t2 = Some(spawn(s, &pz, "T2", t2_work));
            // T2 waiting (its pin on T1's frame), or T2's end.
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, L1), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    r1.unwrap().unwrap();
    r2.unwrap().unwrap().unwrap();
    assert_eq!(pin_read(&pool, &map, L1).unwrap().unwrap().1, m2);
    // s1 stayed as it was: still m1's page.
    let (b, _) = page(Kind::Leaf, 1, 1, m1).unwrap();
    let mut on_disk = vec![0u8; 4096];
    file.read_at(offset(s1), &mut on_disk).unwrap();
    assert_eq!(on_disk, b);
}

/// (b) T1 paused at *published*: a probe that may not wait finds `L` not
/// resident; T2 must wait first, and is served T1's frame with `m1`, one
/// slot read in all. LOADING-SERVED fails it.
#[test]
fn t_pin_load_race_b() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, L1);
    pz.report(LoadWaiting, L1);
    let (first, at_pause, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its pin on its Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut at_pause, mut second, mut t2) = (None, None, None);
        if arrived(&first, LoadPublished, L1) {
            let info =
                frame_of_event(&first).map(|f| pool.frame_info(f).map_err(|e| e.to_string()));
            let probe = pool
                .pin_resident(L1)
                .map(|p| p.is_none())
                .map_err(|e| e.to_string());
            at_pause = Some((info, probe));
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, at_pause, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, L1), "{first:?}");
    let (info, probe_none) = at_pause.unwrap();
    assert_eq!(
        info.unwrap().unwrap(),
        FrameInfo {
            state: FrameState::Loading,
            key: Some(L1),
            pins: 1,
            freed: false,
            // Card A-3b-0: the clock's bit is set only when the page is
            // served, so a frame being loaded is not referenced.
            referenced: false
        }
    );
    assert_eq!(probe_none, Ok(true), "a page being loaded is not resident");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    let (f1, m1) = r1.unwrap().unwrap().unwrap();
    let (f2, m2) = r2.unwrap().unwrap().unwrap().unwrap();
    assert_eq!((f2, m2), (f1, m1));
    assert_eq!(m1, 0x11);
    assert_eq!(file.reads(), 1);
}

/// (c) A slot that fails §3.3: a pin and a second pin each read it and get
/// XX001; T2, which waited on T1's failing load (T1 paused at *resolved*
/// until T2 is *waiting*), gets XX001 by a load of its own; no frame is left.
/// FAILED-LOAD-KEPT fails it.
#[test]
fn t_pin_load_race_c() {
    let (file, map, pool) = fixture(2).unwrap();
    map.set(L1, put(&file, 10, Kind::Leaf, 1, 1, 0x11).unwrap());
    file.write_at(offset(10) + SPOIL as u64, &[0xFF]).unwrap();
    assert!(is_corrupt(&pool.pin(L1, &map)));
    assert!(is_corrupt(&pool.pin(L1, &map)));
    assert_eq!(file.reads(), 2);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, L1);
    pz.report(LoadWaiting, L1);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadResolved, L1) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, L1), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    assert!(is_corrupt(&r1.unwrap()));
    assert!(is_corrupt(&r2.unwrap().unwrap()));
    // T2's own load: four reads of slot 10 in all.
    assert_eq!(file.read_log(), vec![(offset(10), 4096); 4]);
    assert_eq!(pool.frame_of(L1).unwrap(), None);
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.loading, st.orphan), (2, 0, 0));
}

/// (d) An unmapped LPID: T1 and T2, which waited on T1's load (T1 paused at
/// *published* until T2 is *waiting*), both find it unmapped, with no slot
/// read and nothing left in the table.
#[test]
fn t_pin_load_race_d() {
    let (file, map, pool) = fixture(2).unwrap();
    let k = PageKey::Lpid(9);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, k);
    pz.report(LoadWaiting, k);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, k));
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadPublished, k) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, k)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, k), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, k)),
        "{second:?}"
    );
    assert_eq!(r1.unwrap().unwrap(), None);
    assert_eq!(r2.unwrap().unwrap().unwrap(), None);
    assert_eq!(file.reads(), 0);
    assert_eq!(pool.frame_of(k).unwrap(), None);
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.loading, st.orphan), (2, 0, 0));
}

/// (e) T1 paused at *missed*, holding the frame it obtained, not in the
/// table; T2 pins `L` and ends; T1 is served T2's frame: one frame, one slot
/// read. MISS-TWO-FRAMES fails it.
#[test]
fn t_pin_load_race_e() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(MissReserved, L1);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, MissReserved, L1) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, MissReserved, L1), "{first:?}");
    assert_eq!(second, Some(Event::Done("T2")));
    let (f1, _) = r1.unwrap().unwrap().unwrap();
    let (f2, _) = r2.unwrap().unwrap().unwrap().unwrap();
    assert_eq!(f1, f2);
    assert_eq!(file.reads(), 1);
    assert_eq!(pool.stats().unwrap().resident, 1);
}

/// (f) T1 pins an unmapped LPID `K` and is paused at *published*; T3
/// creates `K` as an allocation does (`create`, then `wait_settled` on
/// `Loading`, then `create` again). T3 must wait first; T1 finds `K`
/// unmapped; afterwards one frame holds `K`, with T3's page. The step budget:
/// T3 makes two `create` calls. CREATE-OVER-LOADING fails it.
#[test]
fn t_pin_load_race_f() {
    let (_file, map, pool) = fixture(3).unwrap();
    let k = PageKey::Lpid(7);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, k);
    pz.report(LoadWaiting, k);
    let t3_work = || -> Result<(FrameId, u32), StoreError> {
        match create_as_allocation(&pool, k, Kind::Leaf, 1)? {
            Made::Created(mut c, attempts) => {
                c.mark_dirty(1)?;
                c[MARK] = 0xF3;
                Ok((c.frame(), attempts))
            }
            Made::Exists(..) => Err(StoreError::Invalid("K exists".into())),
        }
    };
    let (first, second, r1, r3) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, k));
        // T1 holds its pin on its Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t3) = (None, None);
        if arrived(&first, LoadPublished, k) {
            t3 = Some(spawn(s, &pz, "T3", t3_work));
            // T3 waiting (holding the frame it obtained), or T3's end.
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t3.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, k), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, k)),
        "{second:?}"
    );
    assert_eq!(r1.unwrap().unwrap(), None, "T1 finds K unmapped");
    let (f3, attempts) = r3.unwrap().unwrap().unwrap();
    assert_eq!(attempts, 2);
    assert_eq!(pool.frame_of(k).unwrap(), Some(f3));
    let p = pool.pin_resident(k).unwrap().unwrap();
    assert_eq!(marker(&p).unwrap(), 0xF3);
    let st = pool.stats().unwrap();
    assert_eq!((st.resident, st.free), (1, 2));
}

/// (g) `K` created, then freed (§4.2), with a pin kept on its frame as a
/// stale name's would be; a creation of `K` makes its page in that same
/// frame. CREATE-BESIDE-FREED fails it.
#[test]
fn t_pin_load_race_g() {
    let (_file, map, pool) = fixture(3).unwrap();
    let k = PageKey::Lpid(3);
    let stale = match create_as_allocation(&pool, k, Kind::Leaf, 1).unwrap() {
        Made::Created(mut c, _) => {
            c.mark_dirty(1).unwrap();
            c[MARK] = 0x61;
            c.release()
        }
        Made::Exists(..) => panic!("K is new"),
    };
    // The free, as A-4b's will make it: the entry unmapped, the page freed.
    map.clear(k);
    stale.latch_exclusive().unwrap().mark_freed().unwrap();
    let made = match create_as_allocation(&pool, k, Kind::Leaf, 2).unwrap() {
        Made::Created(mut c, _) => {
            c[MARK] = 0x62;
            c.release()
        }
        Made::Exists(..) => panic!("a freed page's frame is remade"),
    };
    assert_eq!(made.frame(), stale.frame());
    let g = stale.latch_shared().unwrap();
    assert_eq!((g[MARK], g.content_epoch(), g.is_freed()), (0x62, 2, false));
    let st = pool.stats().unwrap();
    assert_eq!((st.resident, st.free), (1, 2));
}

/// (i) LPID 5 and the MapLeaf at index 5 are each served their own page in
/// their own frame. PAGE-KEY-MERGED fails it.
#[test]
fn t_pin_load_race_i() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 5, 10, 0x55).unwrap();
    let m5 = PageKey::MapLeaf(5);
    map.set(m5, put(&file, 11, Kind::MapLeaf, 5, 1, 0x66).unwrap());
    let a = pin_read(&pool, &map, PageKey::Lpid(5)).unwrap().unwrap();
    let b = pin_read(&pool, &map, m5).unwrap().unwrap();
    assert_ne!(a.0, b.0);
    assert_eq!((a.1, b.1), (0x55, 0x66));
}

/// (j) A page created and never written, its creator's pin dropped, is found
/// by a pin, not as unmapped. ROOT-PIN-MAP-FIRST fails it.
#[test]
fn t_pin_load_race_j() {
    let (_file, map, pool) = fixture(3).unwrap();
    let k = PageKey::Lpid(4);
    let f = match create_as_allocation(&pool, k, Kind::Leaf, 1).unwrap() {
        Made::Created(c, _) => c.release().frame(),
        Made::Exists(..) => panic!("K is new"),
    };
    assert_eq!(map.get(k), Ref::NULL);
    let found = pin_read(&pool, &map, k).unwrap();
    assert_eq!(found.map(|(g, _)| g), Some(f));
}

/// Beyond the card's list: the hooks of a pin that waits for a load (card
/// A-3a's hook table): `LoadWaiting`, then once the load ends `WaitWoken`
/// and `Pinned`, and `Unpinned` when each pin drops; counted after the join.
#[test]
fn hooks_on_wait() {
    use HookPoint::{Pinned, Unpinned};
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, L1);
    for point in [LoadWaiting, WaitWoken, Pinned, Unpinned] {
        pz.report(point, L1);
    }
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, &map, L1));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadPublished, L1) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, &map, L1)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, L1), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, L1)),
        "{second:?}"
    );
    r1.unwrap().unwrap().unwrap();
    r2.unwrap().unwrap().unwrap().unwrap();
    let rest = pz.drain();
    let count = |p: HookPoint| rest.iter().filter(|e| seen(e, p, L1)).count();
    assert_eq!(
        (
            count(LoadWaiting),
            count(WaitWoken),
            count(Pinned),
            count(Unpinned)
        ),
        (0, 1, 2, 2),
        "{rest:?}"
    );
}

// ---- Card A-3a-F2 (verdict A-3a, findings 2 and 4): the claim a creation
// makes on a freed key's frame, pinned, and what follows the claim on error.
// `create_vs_freed` runs on threads under a `Pauser`; `create_freed_poison`
// is one thread, with a hook that records and never pauses (card A-3a-F2-F1).

/// What creator A saw of the page `create` gave it, read before A wrote to
/// it (`create_vs_freed`).
#[derive(Debug)]
struct NewPage {
    /// The page's frame.
    frame: FrameId,
    /// Bytes 4..24 of the page: the kind, the id and the epoch.
    header: Vec<u8>,
    /// Whether the body (bytes 24..) is all zero.
    body_zero: bool,
    /// `is_freed()`.
    freed: bool,
    /// `is_dirty()`.
    dirty: bool,
    /// `content_epoch()`.
    epoch: u32,
}

/// The freed frame of `create_vs_freed` and `create_freed_poison`, as
/// `t_pin_load_race_g` makes it: `k` created in epoch 1 at the first
/// attempt, marked 0x81, unmapped, then freed under a pin that is kept on
/// its frame as a stale name's would be. No hook is set. Returns the pin and
/// the frame.
///
/// # Errors
/// Whatever the pool returns, or [`StoreError::Invalid`] if `k` was not
/// created by the first `create`.
fn freed_frame<'p>(
    pool: &'p Pool<FaultFile>,
    map: &TestMap,
    k: PageKey,
) -> Result<(Pin<'p, FaultFile>, FrameId), StoreError> {
    let Made::Created(mut c, attempts) = create_as_allocation(pool, k, Kind::Leaf, 1)? else {
        return Err(StoreError::Invalid(format!("{k:?} is not new")));
    };
    if attempts != 1 {
        return Err(StoreError::Invalid(format!(
            "{k:?} took {attempts} create calls"
        )));
    }
    c.mark_dirty(1)?;
    c[MARK] = 0x81;
    let stale = c.release();
    map.clear(k);
    stale.latch_exclusive()?.mark_freed()?;
    let g = stale.frame();
    Ok((stale, g))
}

/// A stale name's read of a pinned page through a shared latch that cannot
/// wait (`create_vs_freed`'s first pause): `None` if the latch is held,
/// else `(is_freed(), the byte at MARK, content_epoch())`.
///
/// # Errors
/// [`StoreError::Poisoned`] if the latch was poisoned.
fn stale_marker(p: &Pin<'_, FaultFile>) -> Result<Option<(bool, u8, u32)>, StoreError> {
    match p.try_latch_shared()? {
        Some(gr) => Ok(Some((gr.is_freed(), gr[MARK], gr.content_epoch()))),
        None => Ok(None),
    }
}

/// Whether `p`'s frame refuses its shared latch (`Ok(None)` from
/// `try_latch_shared`: an exclusive latch is held on the frame).
///
/// # Errors
/// [`StoreError::Poisoned`] if the latch was poisoned.
fn latch_refused(p: &Pin<'_, FaultFile>) -> Result<bool, StoreError> {
    p.try_latch_shared().map(|held| held.is_none())
}

/// Whether a pin of `k`'s resident frame refuses its shared latch.
///
/// # Errors
/// Whatever `pin_resident` and the latch return, or a message if `k` is not
/// resident.
fn resident_latch_refused(pool: &Pool<FaultFile>, k: PageKey) -> Result<bool, String> {
    match pool.pin_resident(k).map_err(|e| e.to_string())? {
        Some(p) => latch_refused(&p).map_err(|e| e.to_string()),
        None => Err(format!("{k:?} is not resident")),
    }
}

/// `create_vs_freed`'s record of a creation of `k`: the outcome's name, the
/// frame it names (`Exists`: its pin's frame; `Created` and `Loading`: the
/// outcome's own) and whether the outcome's pin is refused the frame's
/// shared latch. Everything the outcome gave is dropped before this
/// returns.
fn create_named(
    pool: &Pool<FaultFile>,
    k: PageKey,
    epoch: u32,
) -> Result<(&'static str, FrameId, bool), String> {
    let out = pool
        .reserve()
        .and_then(|res| pool.create(res, k, Kind::Leaf, epoch))
        .map_err(|e| e.to_string())?;
    Ok(match out {
        CreateOutcome::Exists(p, _res) => (
            "Exists",
            p.frame(),
            latch_refused(&p).map_err(|e| e.to_string())?,
        ),
        // A created page's pin sits under the outcome's own exclusive latch.
        CreateOutcome::Created(c) => ("Created", c.frame(), true),
        // A creation told `Loading` holds no pin.
        CreateOutcome::Loading(res) => ("Loading", res.frame(), false),
    })
}

/// C4's claim is made in the hold that decides (verdict A-3a, finding 2;
/// §4.2, §5.4, §6.8), so a second creation of the freed key's frame gets
/// `Exists` and never remakes the page over its first creator's. Creator A
/// pauses twice: at `FreedClaimed`, where it holds only its pin on the
/// claimed frame, and at `Pinned`, where it holds `Created`. At the first
/// pause the table's mirror already says live while a stale name still reads
/// the old, freed page (§6.8's window); at both, the controller's creation
/// returns `Exists` while A is paused, so without waiting for anything A
/// holds. While A is paused, the controller calls the pool only in ways that
/// cannot wait on what A holds. CREATE-FREED-CLAIM-LATE fails at the first
/// pause.
#[test]
fn create_vs_freed() {
    const K: PageKey = PageKey::Lpid(8);
    let (file, map, pool) = fixture(3).unwrap();
    // `stale` is kept to the end of the test: the stale name's pin on `g`.
    let (stale, g) = freed_frame(&pool, &map, K).unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(FreedClaimed, K);
    let (
        first,
        table_free,
        stats1,
        info1,
        stale_read,
        first_create,
        second,
        info2,
        latch_held,
        second_create,
        a,
    ) = thread::scope(|s| {
        let a = spawn(s, &pz, "A", || {
            let res = pool.reserve()?;
            match pool.create(res, K, Kind::Leaf, 2)? {
                CreateOutcome::Created(mut c) => {
                    let seen = NewPage {
                        frame: c.frame(),
                        header: c[4..24].to_vec(),
                        body_zero: c[24..].iter().all(|&b| b == 0),
                        freed: c.is_freed(),
                        dirty: c.is_dirty(),
                        epoch: c.content_epoch(),
                    };
                    c[MARK] = 0x82;
                    Ok((seen, c.release()))
                }
                CreateOutcome::Exists(..) | CreateOutcome::Loading(..) => {
                    Err(StoreError::Invalid("A must remake the freed page".into()))
                }
            }
        });
        // A holds its pin on g, beside the stale name's; no latch, no mutex.
        let first = pz.next_event();
        let (mut table_free, mut info1, mut stale_read, mut first_create) =
            (None, None, None, None);
        let (mut second, mut info2, mut latch_held, mut second_create) = (None, None, None, None);
        let mut stats1 = None;
        if arrived(&first, FreedClaimed, K) && frame_of_event(&first) == Some(g) {
            // The controller goes on only if the table mutex is free: a
            // firing under it must fail the test, not hang it.
            let free = pool.table_free();
            table_free = Some(free);
            if free {
                // Inside the guard: `stats()` takes the table mutex, which
                // `table_free()` has just found free.
                stats1 = Some(
                    pool.stats()
                        .map(|s| (s.free, s.reserved))
                        .map_err(|e| e.to_string()),
                );
                info1 = Some(pool.frame_info(g).map_err(|e| e.to_string()));
                // A stale name's read of the claimed frame, through a pin
                // that cannot wait; the guard and the pin are dropped here.
                stale_read = Some(
                    pool.pin_resident(K)
                        .and_then(|p| p.as_ref().map(stale_marker).unwrap_or(Ok(None)))
                        .map_err(|e| e.to_string()),
                );
                // The controller is creator B.
                first_create =
                    Some(create_named(&pool, K, 2).map(|(name, frame, _)| (name, frame)));
                // Armed after the controller's own creation, whose `Pinned`
                // firing must not take the pause.
                pz.pause_at(Pinned, K);
                pz.release(FreedClaimed, K);
                let e = pz.next_event();
                second = Some(e);
                if arrived(&e, Pinned, K) && frame_of_event(&e) == Some(g) {
                    // A holds Created: g's exclusive latch and its pin.
                    info2 = Some(pool.frame_info(g).map_err(|e| e.to_string()));
                    latch_held = Some(resident_latch_refused(&pool, K));
                    // Only on a frame the mirror calls live: on a freed one a
                    // creation latches that frame, which A holds here.
                    if info2
                        .as_ref()
                        .is_some_and(|i| matches!(i, Ok(info) if !info.freed))
                    {
                        second_create = Some(create_named(&pool, K, 3));
                    }
                }
            }
        }
        pz.release_all();
        (
            first,
            table_free,
            stats1,
            info1,
            stale_read,
            first_create,
            second,
            info2,
            latch_held,
            second_create,
            a.join(),
        )
    });
    assert!(arrived(&first, FreedClaimed, K), "{first:?}");
    assert_eq!(frame_of_event(&first), Some(g));
    assert_eq!(
        table_free,
        Some(true),
        "the hook fires holding no table mutex"
    );
    assert_eq!(
        stats1,
        Some(Ok((2, 0))),
        "the creator's reservation is back on the free list when FreedClaimed fires"
    );
    assert_eq!(
        info1,
        Some(Ok(FrameInfo {
            state: FrameState::Resident,
            key: Some(K),
            pins: 2,
            freed: false,
            // Card A-3b-0: the remade page's pin sets the clock's bit.
            referenced: true
        })),
        "the claim is made in the hold that decided"
    );
    assert_eq!(
        stale_read,
        Some(Ok(Some((true, 0x81, 1)))),
        "until its creator latches it, the claimed frame is open to a stale name and still \
         holds the old, freed page"
    );
    assert_eq!(
        first_create,
        Some(Ok(("Exists", g))),
        "a second creation of a claimed frame gets Exists, without waiting"
    );
    assert!(
        second.as_ref().is_some_and(|e| arrived(e, Pinned, K)),
        "{second:?}"
    );
    assert_eq!(second.as_ref().and_then(frame_of_event), Some(g));
    assert_eq!(
        info2,
        Some(Ok(FrameInfo {
            state: FrameState::Resident,
            key: Some(K),
            pins: 2,
            freed: false,
            // Card A-3b-0: the remade page's pin sets the clock's bit.
            referenced: true
        })),
        "the frame a creator holds Created on is live by the table's mirror"
    );
    assert_eq!(
        latch_held,
        Some(Ok(true)),
        "A holds the frame's exclusive latch"
    );
    assert_eq!(
        second_create,
        Some(Ok(("Exists", g, true))),
        "Exists while the creator holds the latch"
    );
    let (seen, a_pin) = a.unwrap().unwrap();
    assert_eq!(seen.frame, g);
    assert_eq!(
        seen.header,
        [3, 0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0],
        "kind 3, id 8, epoch 2"
    );
    assert!(seen.body_zero, "{seen:?}");
    assert_eq!((seen.freed, seen.dirty, seen.epoch), (false, true, 2));
    assert_eq!(a_pin.frame(), g);
    assert_eq!(marker(&a_pin).unwrap(), 0x82);
    assert_eq!(pz.drain(), vec![Event::Done("A")]);
    assert_eq!(pool.frame_of(K).unwrap(), Some(g));
    assert_eq!(stale.frame(), g, "the stale name's pin, kept to the end");
    assert_eq!(
        pool.frame_info(g).unwrap(),
        FrameInfo {
            state: FrameState::Resident,
            key: Some(K),
            pins: 2,
            freed: false,
            // Card A-3b-0: the remade page's pin sets the clock's bit.
            referenced: true
        },
        "stale's pin and A's"
    );
    let st = pool.stats().unwrap();
    assert_eq!(
        (st.free, st.reserved, st.loading, st.resident, st.orphan),
        (2, 0, 0, 1, 0)
    );
    assert_eq!(file.reads(), 0);
}

/// The freed branch's error path gives the claim back (verdict A-3a, finding
/// 2; card A-3a-F2): with `g`'s latch poisoned, a creation of `K` returns
/// `Poisoned` and the table's mirror says the frame is freed again, as its
/// content still is (the latch's failing path writes nothing; the content
/// cannot be read through the poisoned latch, so the test judges the mirror).
/// The give-back comes before the claim's pin is dropped: a hook installed
/// for the call records the mirror at `Unpinned`, which fires holding no
/// mutex (card A-3a-F2-F1). The frame keeps its key and its one pin, and the
/// creation's reservation is back on the free list. FREED-STAYS-CLAIMED and
/// GIVEBACK-AFTER-PIN-DROP fail it.
#[test]
fn create_freed_poison() {
    use std::sync::{Arc, Mutex};

    use nucleus_store::buf::Hook;

    use HookPoint::Unpinned;

    const K: PageKey = PageKey::Lpid(8);
    let (_file, map, pool) = fixture(3).unwrap();
    let pool = Arc::new(pool);
    let (stale, g) = freed_frame(&pool, &map, K).unwrap();
    // Poison g's latch with a panic under it, inside `catch_unwind`. The
    // panic hook is not replaced (it is process-wide), and the table mutex
    // is not held here: only the latch is poisoned.
    let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _held = stale.latch_exclusive().unwrap();
        panic!("poison the freed frame's latch for create_freed_poison");
    }));
    assert!(
        poisoned.is_err(),
        "the panic under the latch was not caught"
    );
    let before = pool.stats().unwrap();
    // A hook for the one call (card A-3a-F2-F1): at `Unpinned` for `(K, g)`,
    // the creator's pin dropping, it records the table's mirror. `Unpinned`
    // fires holding no mutex, so `frame_info` can take the table mutex there.
    // The pool is a strong clone inside the hook: the cycle it makes with
    // the pool's hook slot ends at the `set_hook(None)` after the call.
    let seen: Arc<Mutex<Vec<Result<FrameInfo, String>>>> = Arc::default();
    let sink = Arc::clone(&seen);
    let in_hook = Arc::clone(&pool);
    let hook: Hook = Arc::new(move |point, key, frame| {
        if point == Unpinned && key == K && frame == g {
            let info = in_hook.frame_info(g).map_err(|e| e.to_string());
            sink.lock().unwrap().push(info);
        }
    });
    pool.set_hook(Some(hook));
    let made = pool.create(pool.reserve().unwrap(), K, Kind::Leaf, 2);
    pool.set_hook(None);
    assert!(matches!(made, Err(StoreError::Poisoned(_))), "{made:?}");
    let seen: Vec<Result<FrameInfo, String>> = seen.lock().unwrap().clone();
    assert_eq!(
        seen.len(),
        1,
        "one Unpinned for (K, g) in the call: {seen:?}"
    );
    assert!(
        matches!(seen.as_slice(), [Ok(info)] if info.freed),
        "the claim is given back before its pin drops: {seen:?}"
    );
    assert_eq!(
        pool.frame_info(g).unwrap(),
        FrameInfo {
            state: FrameState::Resident,
            key: Some(K),
            pins: 1,
            freed: true,
            // Card A-3b-0: the claim's pin set the clock's bit, and the
            // give-back leaves it set.
            referenced: true
        },
        "the claim is given back; only stale counts"
    );
    assert_eq!(pool.frame_of(K).unwrap(), Some(g));
    let st = pool.stats().unwrap();
    assert_eq!(
        (st.free, st.reserved, st.loading, st.resident, st.orphan),
        (2, 0, 0, 1, 0)
    );
    assert_eq!(
        st.free, before.free,
        "the reservation's frame is back on the free list"
    );
}
