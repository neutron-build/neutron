//! S-STORE §15.1's `t_pin_load_race` at A-3c (card A-3c, Work 14.3; I-PIN-
//! CURRENT: cases (a) to (j) on OS threads, with A-3c's map and dirty
//! eviction, and S3 for the writes, frees and creations), the gate's rules,
//! an eviction's gate hold under a pending flip, and the map's two growth
//! races (card evidence for A-4a's row).
//!
//! Each test: one fixture (`n` 3, `F` 46 at the lowered floor, computed from
//! R-1-memory §4.2's formula; fanout 4; `slot_count` 600; epoch 1, which no
//! case fills, so no frame acquisition evicts), `std::thread::scope`, a
//! `Pauser` installed with `set_hook`. Every pause names what each party
//! holds there. The controller records what it sees, releases every paused
//! thread, joins, and only then asserts; while a thread is paused it calls
//! the pool only in ways that cannot wait on what that thread holds. When an
//! event other than the expected one comes, it records it, releases all and
//! joins. No test waits on time.
//!
//! Mutants: LOAD-REF-BEFORE-PUBLISH (a), EVICT-UNLINK-BEFORE-ENTRY (h),
//! WB-NO-GATE and OWN-1 (`evict_holds_gate`), WB-GATE-WRITER-STARVE (`gate_rules`),
//! LEVEL-GROW-UNCONDITIONAL (`growth_race_level`),
//! MAPINNER-CREATE-UNCONDITIONAL (`growth_race_inner`).

mod buf_support;
mod wb_support;

use std::sync::Arc;
use std::sync::mpsc;
use std::thread;

use buf_support::{Event, Pauser, spawn};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    EvictOutcome, FrameId, FrameInfo, FrameState, Hold, HookPoint, MapShape, PageKey, Pin,
    PinOutcome, Pool,
};
use nucleus_store::page::{self, Kind};
use nucleus_store::sync::StdPrimitives;
use wb_support::drive::{
    evict, evict_out, s1_flip, s3_free, s3_make_mapleaf, s3_new_page, s3_touch,
};
use wb_support::file::WbFile;
use wb_support::{H_MIN_TEST, MARK, PS, min_f, pin, wb_pool};

use HookPoint::{
    EvictEntrySet, EvictWrite, GateExclusiveWaiting, GateSharedBlocking, GrowLocking,
    LoadPublished, LoadResolved, LoadWaiting, MissReserved, Pinned, Restarting, SweepEnded,
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

/// The fixture of (a) to (j): `n` 3, `F` 46, fanout 4, `slot_count` 600,
/// epoch 1, which no case fills, so no frame acquisition evicts.
fn race_pool() -> Result<(Arc<WbFile>, Pool<WbFile, StdPrimitives>), StoreError> {
    assert_eq!(min_f(3), 46);
    wb_pool::<StdPrimitives>(46, 4, 600, 1, H_MIN_TEST).map_err(StoreError::Invalid)
}

/// "Page `L` at slot `s1` with marker `m1`": `s3_new_page(m1)`, the pin
/// dropped, then `evict(L)`; its MapLeaf stays resident. Returns the LPID
/// and `s1`.
fn page_at_home(pool: &Pool<WbFile, StdPrimitives>, m1: u8) -> Result<(u64, u64), StoreError> {
    let (l, p) = s3_new_page(pool, m1)?;
    drop(p);
    evict_out(pool, lpid(l))?;
    let s1 = pool.entry_of(l)?.slot;
    assert_ne!(s1, 0);
    Ok((l, s1))
}

/// Whether `e` is `Arrived(point, key, _)`.
fn arrived(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Arrived(p, k, _) if *p == point && *k == key)
}

/// Whether `e` is `Seen(point, key, _)`.
fn seen(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Seen(p, k, _) if *p == point && *k == key)
}

/// Whether `e` is `Done(name)`.
fn done(e: &Event, name: &str) -> bool {
    matches!(e, Event::Done(n) if *n == name)
}

/// The frame a `Seen` or `Arrived` event names.
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

/// A sealed `PS`-byte page: zeroed, its header written, `marker` at `MARK`.
fn sealed(kind: Kind, id: u64, epoch: u32, marker: u8) -> Result<(Vec<u8>, u32), StoreError> {
    let mut b = vec![0u8; PS as usize];
    page::write_header(&mut b, kind, id, epoch)?;
    if let Some(at) = b.get_mut(MARK) {
        *at = marker;
    }
    let crc = page::seal(&mut b)?;
    Ok((b, crc))
}

/// Pin `key` holding nothing, read its frame and marker, drop the pin;
/// `None` if unmapped (A-3a's `pin_read` on the pool's own map).
fn pin_read(
    pool: &Pool<WbFile, StdPrimitives>,
    key: PageKey,
) -> Result<Option<(FrameId, u8)>, StoreError> {
    match pool.pin_page(key, &Hold::nothing())? {
        PinOutcome::Pinned(p) => {
            let f = p.frame();
            let m = marker_of(&p)?;
            drop(p);
            Ok(Some((f, m)))
        }
        PinOutcome::Unmapped => Ok(None),
    }
}

/// The marker a pinned page carries, read under the shared latch.
fn marker_of(p: &Pin<'_, WbFile, StdPrimitives>) -> Result<u8, StoreError> {
    let g = p.latch_shared()?;
    Ok(g.get(MARK).copied().unwrap_or(0))
}

/// T2's report in `t_pin_load_race_a`: its pin is dropped and it waits for
/// the controller's message. It goes on the `Pauser`'s channel, the one the
/// controller reads, through `Pauser::done`, its one sender a thread can
/// call; the name tells it from T2's own `Done("T2")`.
const T2_UNPINNED: &str = "T2 unpinned";

/// (a) T1 paused at *resolved* holds its count on the Loading frame; T2
/// waits for that load, is served T1's frame, dirties it, drops its pin,
/// reports and waits; only once T1 has joined, its pin dropped, does T2
/// evict the frame to a fresh slot, `s1` unchanged. T2 must wait first,
/// and a later pin reads `m2`. LOAD-REF-BEFORE-PUBLISH fails it: T2's
/// report comes before any `LoadWaiting`.
///
/// Why T2's eviction waits for a message (verdict A-3c R1; card
/// Amendment 2): T1's `pin_read` releases its shared latch before it drops
/// its pin, and T2 takes the latch as soon as it goes. An eviction before
/// T1's `drop(p)` meets T1's pin and is rightly refused (I-B12: a pinned
/// frame is never a victim). The order comes from an event a thread sends,
/// never from a timed wait or a retry.
#[test]
fn t_pin_load_race_a() {
    let (file, pool) = race_pool().unwrap();
    let (m1, m2) = (0xA1, 0xB2);
    let (l, s1) = page_at_home(&pool, m1).unwrap();
    let key = lpid(l);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, key);
    pz.report(LoadWaiting, key);
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (pool_ref, reporter) = (&pool, pz.clone());
    let t2_work = move || -> Result<(), StoreError> {
        let p = pin(pool_ref, key, &Hold::nothing())?;
        s3_touch(pool_ref, &p, m2)?;
        drop(p);
        // T2 holds nothing: no pin, no latch, no gate, no mutex.
        reporter.done(T2_UNPINNED);
        go_rx
            .recv()
            .map_err(|_| StoreError::Invalid("the controller left".to_string()))?;
        evict_out(pool_ref, key)?;
        Ok(())
    };
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, key));
        // T1 holds its count on the Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadResolved, key) {
            t2 = Some(spawn(s, &pz, "T2", t2_work));
            // The first of T2 waiting (its count on T1's frame) and T2's
            // report (it holds nothing); or T2's end, if it failed first.
            second = Some(pz.next_event());
        }
        pz.release_all();
        // T1 joined: its pin is dropped, so T2's eviction may begin.
        let r1 = t1.join();
        // T2 may have ended without waiting, or never started: then no one
        // receives this, which is no failure.
        let _ = go_tx.send(());
        (first, second, r1, t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, key), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, key)),
        "{second:?}"
    );
    r1.unwrap().unwrap();
    r2.unwrap().unwrap().unwrap();
    assert_eq!(
        pin_read(&pool, key).unwrap().map(|(_, m)| m),
        Some(m2),
        "the last pin reads m2"
    );
    // s1 stayed as it was: still m1's page.
    let (b, _) = sealed(Kind::Leaf, l, 1, m1).unwrap();
    let on_disk = file.peek(s1 * u64::from(PS), PS as usize).unwrap();
    assert_eq!(on_disk, b);
}

/// (b) T1 paused at *published*: a probe that may not wait finds `L` not
/// resident; T2 must wait first, and is served T1's frame with `m1`, one
/// slot read in all.
#[test]
fn t_pin_load_race_b() {
    let (file, pool) = race_pool().unwrap();
    let m1 = 0x11;
    let (l, s1) = page_at_home(&pool, m1).unwrap();
    let key = lpid(l);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, key);
    pz.report(LoadWaiting, key);
    let (first, at_pause, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, key));
        // T1 holds its count on its Loading frame; no latch, no mutex.
        let first = pz.next_event();
        let (mut at_pause, mut second, mut t2) = (None, None, None);
        if arrived(&first, LoadPublished, key) {
            let info =
                frame_of_event(&first).map(|f| pool.frame_info(f).map_err(|e| format!("{e:?}")));
            // A probe that may not wait: a page being loaded is not
            // resident.
            let probe = pool
                .pin_resident(key)
                .map(|p| p.is_none())
                .map_err(|e| format!("{e:?}"));
            at_pause = Some((info, probe));
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, key)));
            // T2 waiting (its count on T1's frame), or T2's end.
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, at_pause, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadPublished, key), "{first:?}");
    let (info, probe_none) = at_pause.unwrap();
    assert_eq!(
        info.unwrap().unwrap(),
        FrameInfo {
            state: FrameState::Loading,
            key: Some(key),
            pins: 1,
            freed: false,
            referenced: false
        }
    );
    assert_eq!(probe_none, Ok(true), "a page being loaded is not resident");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, key)),
        "{second:?}"
    );
    let Some((f1, m)) = r1.unwrap().unwrap() else {
        panic!("T1 was served L");
    };
    let Some((f2, m2)) = r2.unwrap().unwrap().unwrap() else {
        panic!("T2 was served T1's frame");
    };
    assert_eq!((f2, m2), (f1, m));
    assert_eq!(m, m1);
    assert_eq!(file.reads(), vec![(s1 * u64::from(PS), PS as usize)]);
}

/// (c) A slot that fails §3.3: a pin and a second pin each read it and get
/// XX001; T2, which waited on T1's failing load (T1 paused at *resolved*
/// until T2 is *waiting*), gets XX001 by a load of its own; no frame is
/// left.
#[test]
fn t_pin_load_race_c() {
    let (file, pool) = race_pool().unwrap();
    let m1 = 0x11;
    let (l, s1) = page_at_home(&pool, m1).unwrap();
    let key = lpid(l);
    file.poke(s1 * u64::from(PS) + 200, &[0xFF]).unwrap();
    assert!(is_corrupt(&pin_read(&pool, key)));
    assert!(is_corrupt(&pin_read(&pool, key)));
    assert_eq!(file.reads().len(), 2);
    let free_before = pool.stats().unwrap().free;
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadResolved, key);
    pz.report(LoadWaiting, key);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, key));
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadResolved, key) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, key)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, second, t1.join(), t2.map(|h| h.join()))
    });
    assert!(arrived(&first, LoadResolved, key), "{first:?}");
    assert!(
        second.is_some_and(|e| seen(&e, LoadWaiting, key)),
        "{second:?}"
    );
    assert!(is_corrupt(&r1.unwrap()));
    assert!(is_corrupt(&r2.unwrap().unwrap()));
    // T2's own load: four reads of slot s1 in all.
    assert_eq!(file.reads(), vec![(s1 * u64::from(PS), PS as usize); 4]);
    assert_eq!(pool.frame_of(key).unwrap(), None);
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.loading, st.orphan), (free_before, 0, 0));
}

/// (d) An unmapped LPID: T1 and T2, which waited on T1's load (T1 paused at
/// *published* until T2 is *waiting*), both find it unmapped, with no slot
/// read and nothing left in the table.
#[test]
fn t_pin_load_race_d() {
    let (file, pool) = race_pool().unwrap();
    let (_l, _s1) = page_at_home(&pool, 0x11).unwrap();
    // LPID 3 was never allocated: MapLeaf 0 is made and `next_lpid` is 2.
    assert_eq!(pool.lpid_stats().unwrap().next_lpid, 2);
    let free_before = pool.stats().unwrap().free;
    let k = lpid(3);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, k);
    pz.report(LoadWaiting, k);
    let (first, second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, k));
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, LoadPublished, k) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, k)));
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
    assert!(file.reads().is_empty(), "no slot read");
    assert_eq!(pool.frame_of(k).unwrap(), None);
    let st = pool.stats().unwrap();
    assert_eq!((st.free, st.loading, st.orphan), (free_before, 0, 0));
}

/// (e) T1 paused at *missed* (holding its reservation); T2 pins `L` (a
/// whole load) and ends; T1 released is served T2's frame; one read of `s1`
/// in all.
#[test]
fn t_pin_load_race_e() {
    let (file, pool) = race_pool().unwrap();
    let (l, s1) = page_at_home(&pool, 0x11).unwrap();
    let key = lpid(l);
    let resident_before = pool.stats().unwrap().resident;
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(MissReserved, key);
    let (second, r1, r2) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, key));
        // T1 holds the frame it obtained, not in the table.
        let first = pz.next_event();
        let (mut second, mut t2) = (None, None);
        if arrived(&first, MissReserved, key) {
            t2 = Some(spawn(s, &pz, "T2", || pin_read(&pool, key)));
            second = Some(pz.next_event());
        }
        pz.release_all();
        (second, t1.join(), t2.map(|h| h.join()))
    });
    assert_eq!(second, Some(Event::Done("T2")));
    let (f1, _) = r1.unwrap().unwrap().unwrap();
    let (f2, _) = r2.unwrap().unwrap().unwrap().unwrap();
    assert_eq!(f1, f2);
    assert_eq!(file.reads(), vec![(s1 * u64::from(PS), PS as usize)]);
    assert_eq!(pool.stats().unwrap().resident, resident_before + 1);
}

/// (f) `K` = the LPID the allocator hands out next. T1 pins `K` → paused at
/// *published*. T3: `s3_new_page`, whose `create` returns `Loading(res)`,
/// then `wait_settled(Lpid(K))` (which fires `LoadWaiting`), then `create`
/// again. T3 must wait first; T1 gets `Unmapped`; afterwards one frame holds
/// `K`, with T3's marker.
#[test]
fn t_pin_load_race_f() {
    let (_file, pool) = race_pool().unwrap();
    let (l, _s1) = page_at_home(&pool, 0x11).unwrap();
    assert_eq!(l, 1);
    assert_eq!(pool.lpid_stats().unwrap().next_lpid, 2);
    let k = lpid(2);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(LoadPublished, k);
    pz.report(LoadWaiting, k);
    let t3_work = || -> Result<(u64, FrameId), StoreError> {
        let (x, p) = s3_new_page(&pool, 0xF3)?;
        let f = p.frame();
        drop(p);
        Ok((x, f))
    };
    let (first, second, r1, r3) = thread::scope(|s| {
        let t1 = spawn(s, &pz, "T1", || pin_read(&pool, k));
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
    let (x, f3) = r3.unwrap().unwrap().unwrap();
    assert_eq!(x, 2);
    assert_eq!(pool.frame_of(k).unwrap(), Some(f3));
    assert_eq!(pin_read(&pool, k).unwrap().map(|(_, m)| m), Some(0xF3));
    let st = pool.stats().unwrap();
    assert_eq!((st.resident, st.free), (2, 44));
}

/// (g) `K` created, then freed (§4.2), with a pin kept on its frame as a
/// stale name's would be; a creation of `K` makes its page in that same
/// frame.
#[test]
fn t_pin_load_race_g() {
    let (_file, pool) = race_pool().unwrap();
    let (l, _s1) = page_at_home(&pool, 0x11).unwrap();
    assert_eq!(l, 1);
    let (k, p) = s3_new_page(&pool, 0x61).unwrap();
    assert_eq!(k, 2);
    // The kept pin, as a stale name's would be.
    let stale = p;
    let stale_frame = stale.frame();
    // The free, as A-4b's will make it.
    s3_free(&pool, k, 4).unwrap();
    assert!(pool.lpid_is_free(k).unwrap());
    let (k2, made) = s3_new_page(&pool, 0x62).unwrap();
    assert_eq!(k2, k, "the freed LPID, the lowest free");
    assert_eq!(made.frame(), stale_frame);
    drop(made);
    let g = stale.latch_shared().unwrap();
    assert_eq!(
        (g.get(MARK).copied(), g.content_epoch(), g.is_freed()),
        (Some(0x62), 1, false)
    );
}

/// (h) `L` at `s1` with `m1`, then pinned, touched to `m2` and dropped.
/// Pause *evicting*; E: `evict(L)` → arrives (E holds `L`'s exclusive latch
/// and the gate, no map latch; the frame is in the page table). T2: pins
/// `L` (it raises the count and takes no latch), then `latch_shared()` (it
/// waits for E), reads the marker. T2 read `m2`, and E's outcome is `Kept`.
/// EVICT-UNLINK-BEFORE-ENTRY fails it.
#[test]
fn t_pin_load_race_h() {
    let (file, pool) = race_pool().unwrap();
    let (l, s1) = page_at_home(&pool, 0x11).unwrap();
    let key = lpid(l);
    {
        let p = pin(&pool, key, &Hold::nothing()).unwrap();
        s3_touch(&pool, &p, 0x12).unwrap();
    }
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(EvictEntrySet, key);
    pz.report(Pinned, key);
    let t2_work = || -> Result<u8, StoreError> {
        let p = pin(&pool, key, &Hold::nothing())?;
        // It waits for E here.
        marker_of(&p)
    };
    let (first, at_pause, second, m2, r_e) = thread::scope(|s| {
        let e = spawn(s, &pz, "E", || evict(&pool, key));
        // E holds L's exclusive latch and the gate; no map latch; the frame
        // is in the page table.
        let first = pz.next_event();
        let (mut at_pause, mut second, mut t2) = (None, None, None);
        if arrived(&first, EvictEntrySet, key) {
            // Nothing here waits on what E holds: L's frame is looked up,
            // and the entry is read through MapLeaf 0's frame, which E does
            // not hold.
            let frame = pool.frame_of(key).map_err(|e| format!("{e:?}"));
            let entry = pool.entry_of(l).map_err(|e| format!("{e:?}"));
            at_pause = Some((frame_of_event(&first), frame, entry));
            t2 = Some(spawn(s, &pz, "T2", t2_work));
            // T2's pin arrived (Seen(Pinned)), or its end.
            second = Some(pz.next_event());
        }
        pz.release_all();
        (first, at_pause, second, t2.map(|h| h.join()), e.join())
    });
    assert!(arrived(&first, EvictEntrySet, key), "{first:?}");
    let (f_e, frame, entry) = at_pause.unwrap();
    assert_eq!(
        frame,
        Ok(Some(f_e.unwrap())),
        "the frame is in the page table"
    );
    let entry = entry.unwrap();
    let new_home = file
        .writes()
        .into_iter()
        .rev()
        .find(|w| w.kind == 3 && w.id == l)
        .map(|w| w.offset / u64::from(PS))
        .unwrap();
    assert_ne!(new_home, s1);
    assert_eq!((entry.slot, entry.epoch), (new_home, 1), "the entry is set");
    assert_eq!(second, Some(Event::Seen(Pinned, key, f_e.unwrap())));
    assert_eq!(m2.unwrap().unwrap().unwrap(), 0x12);
    assert!(matches!(r_e.unwrap().unwrap(), EvictOutcome::Kept));
}

/// (i) LPID 5 and the MapLeaf at index 5 are each served their own page in
/// their own frame (PAGE-KEY-MERGED is A-3a's; this case runs it with the
/// real map).
#[test]
fn t_pin_load_race_i() {
    let (_file, pool) = race_pool().unwrap();
    let mut last = 0;
    for m in 0x31..=0x35u8 {
        let (x, p) = s3_new_page(&pool, m).unwrap();
        last = x;
        drop(p);
    }
    assert_eq!(last, 5);
    assert_eq!(pool.lpid_stats().unwrap().next_lpid, 6);
    // MapLeaf 5: the growth call adds level 2.
    s3_make_mapleaf(&pool, 5).unwrap();
    assert_eq!(
        pool.map_shape().unwrap(),
        MapShape {
            root_level: 2,
            inners: 3
        }
    );
    let p5 = pin(&pool, lpid(5), &Hold::nothing()).unwrap();
    let m5 = pin(&pool, mapleaf(5), &Hold::nothing()).unwrap();
    assert_ne!(p5.frame(), m5.frame());
    {
        let g = p5.latch_shared().unwrap();
        let h = page::read_header(&g).unwrap();
        assert_eq!((h.kind, h.id), (Kind::Leaf, 5));
        assert_eq!(g.get(MARK).copied(), Some(0x35));
    }
    {
        let g = m5.latch_shared().unwrap();
        let h = page::read_header(&g).unwrap();
        assert_eq!((h.kind, h.id), (Kind::MapLeaf, 5));
        assert_eq!(g.get(MARK).copied(), Some(0), "an empty body");
    }
}

/// (j) A page made by `s3_new_page` and never written, its pin dropped:
/// `pin_page` gives `Pinned` on its frame, not `Unmapped`; `entry_of` is
/// null.
#[test]
fn t_pin_load_race_j() {
    let (_file, pool) = race_pool().unwrap();
    let (x, p) = s3_new_page(&pool, 0x51).unwrap();
    let f = p.frame();
    drop(p);
    assert!(pool.entry_of(x).unwrap().is_null());
    let p = pin(&pool, lpid(x), &Hold::nothing()).unwrap();
    assert_eq!(p.frame(), f);
    assert_eq!(marker_of(&p).unwrap(), 0x51);
}

/// `gate_rules` (card A-3c, Work 14.3; G1, G2, G3). (a) A flip that waits
/// blocks new shared holders and every try; released, it goes through and
/// the waiter is granted at the new epoch. (b) `wait_for_flip` returns at
/// once with no flip, and waits behind a flip that holds the gate (frame 0,
/// "held"). Mutant WB-GATE-WRITER-STARVE fails (a), and so does a
/// registration made after the hook (G2: "The registration precedes the
/// hook"), since the try runs while X is paused there.
#[test]
fn gate_rules() {
    let (_file, pool) = race_pool().unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    // X is paused at its hook, so the try below runs while X can have done
    // nothing past it: only a registration made before the hook refuses it.
    pz.pause_at(GateExclusiveWaiting, NO_KEY);
    pz.report(GateSharedBlocking, NO_KEY);
    // (a) The controller holds the gate.
    let hold = pool.gate_shared().unwrap();
    let (r_x, seen_x, try_after_x, seen_s, r_s) = thread::scope(|s| {
        let x = spawn(s, &pz, "X", || -> Result<(), StoreError> {
            let mut flip = pool.gate_exclusive()?;
            flip.record_image_root(1)?;
            flip.advance_epoch()?;
            drop(flip);
            Ok(())
        });
        let seen_x = pz.next_event();
        // Once a test has seen the hook, every `gate_try_shared` fails
        // until the flip ends.
        let try_after_x = pool.gate_try_shared().unwrap().is_some();
        pz.release(GateExclusiveWaiting, NO_KEY);
        let w = spawn(s, &pz, "S", || -> Result<u32, StoreError> {
            let g = pool.gate_shared()?;
            Ok(g.epoch())
        });
        let seen_s = pz.next_event();
        drop(hold);
        (x.join(), seen_x, try_after_x, seen_s, w.join())
    });
    r_x.unwrap().unwrap();
    assert_eq!(
        seen_x,
        Event::Arrived(GateExclusiveWaiting, NO_KEY, FrameId(u32::MAX))
    );
    assert!(!try_after_x, "no try passes while a flip waits");
    assert_eq!(
        seen_s,
        Event::Seen(GateSharedBlocking, NO_KEY, FrameId(1)),
        "requested: an exclusive request waits"
    );
    assert_eq!(r_s.unwrap().unwrap(), 2, "S is granted at the new epoch");
    assert_eq!(pool.epoch().unwrap(), 2);
    assert_eq!(
        pool.image_root().unwrap(),
        Some(nucleus_store::buf::ImageRoot { image: 1, level: 1 })
    );
    // (b) No flip: at once, and no hook. What (a) left is its threads' ends.
    assert!(
        pz.drain().into_iter().all(|e| matches!(e, Event::Done(_))),
        "no hook of the gate fired after the flip ended"
    );
    assert!(pool.wait_for_flip().is_ok());
    assert!(pz.drain().is_empty());
    let (granted_tx, granted_rx) = mpsc::channel::<()>();
    let (go_tx, go_rx) = mpsc::channel::<()>();
    let (r_x, seen_w, r_w) = thread::scope(|s| {
        let pool = &pool;
        let x = spawn(s, &pz, "X", move || -> Result<(), StoreError> {
            let flip = pool.gate_exclusive()?;
            granted_tx
                .send(())
                .map_err(|_| StoreError::Invalid("the controller left".to_string()))?;
            // It holds the gate until the controller lets go.
            let _ = go_rx.recv();
            drop(flip);
            Ok(())
        });
        granted_rx.recv().unwrap();
        let w = spawn(s, &pz, "W", || pool.wait_for_flip());
        let seen_w = pz.next_event();
        go_tx.send(()).unwrap();
        (x.join(), seen_w, w.join())
    });
    r_x.unwrap().unwrap();
    assert_eq!(
        seen_w,
        Event::Seen(GateSharedBlocking, NO_KEY, FrameId(0)),
        "held: the gate is held exclusively"
    );
    r_w.unwrap().unwrap();
}

/// `evict_holds_gate` (card A-3c, Work 14.3; E1, E5): an eviction for a
/// caller that holds no gate takes the gate by a try, and holds it across
/// the write and the entry set, so a flip registers behind it. Checked
/// twice, on a fresh fixture each: the evictor paused at `EvictWrite`, and
/// paused at `EvictEntrySet` (verdict A-3c, A-3c-F6's OWN-1: once step 2
/// exists, a flip between the write and the entry set would hand E5 a stale
/// epoch). Mutants WB-NO-GATE and OWN-1 (GATE-RELEASED-BEFORE-ENTRY) fail
/// it: the flip ends rather than registers.
#[test]
fn evict_holds_gate() {
    // One check: the evictor paused at `point`, where it holds L's
    // exclusive latch and its gate try-hold, no map latch (E4's
    // `EvictWrite`, E5's `EvictEntrySet`).
    let check_at = |point: HookPoint| {
        let (file, pool) = race_pool().unwrap();
        let (l, _s1) = page_at_home(&pool, 0x11).unwrap();
        let key = lpid(l);
        {
            let p = pin(&pool, key, &Hold::nothing()).unwrap();
            s3_touch(&pool, &p, 0x12).unwrap();
        }
        let pz = Pauser::new();
        pool.set_hook(Some(pz.hook()));
        pz.pause_at(point, key);
        pz.report(GateExclusiveWaiting, NO_KEY);
        let (first, which, r_e) = thread::scope(|s| {
            let e = spawn(s, &pz, "E", || evict(&pool, key));
            // E holds L's latch and its gate try-hold; no map latch.
            let first = pz.next_event();
            let mut which = None;
            let mut x = None;
            if arrived(&first, point, key) {
                x = Some(spawn(s, &pz, "X", || s1_flip(&pool)));
                let mut others = 0;
                loop {
                    let ev = pz.next_event();
                    if seen(&ev, GateExclusiveWaiting, NO_KEY) {
                        which = Some("hook");
                        break;
                    }
                    if done(&ev, "X") {
                        which = Some("done");
                        break;
                    }
                    others += 1;
                    assert!(others < 16, "an unexpected event: {ev:?}");
                }
            }
            pz.release_all();
            if let Some(x) = x {
                let _ = x.join();
            }
            (first, which, e.join())
        });
        assert!(arrived(&first, point, key), "{point:?}: {first:?}");
        assert_eq!(
            which,
            Some("hook"),
            "{point:?}: the flip registered before it could end"
        );
        assert!(matches!(r_e.unwrap().unwrap(), EvictOutcome::Obtained(_)));
        assert_eq!(pool.epoch().unwrap(), 2);
        // L's home from the fixture, and this eviction's write: no other.
        assert_eq!(file.writes().len(), 2);
    };
    check_at(EvictWrite);
    check_at(EvictEntrySet);
}

/// `flip_pending_rechecks_free` (card A-3c, Work 8.3, with A5's re-check;
/// S-STORE §5.2: "If the try fails because a flip is pending, the evictor
/// takes a clean victim. If no victim is clean, the thread ... restarts").
/// `n` 1, `F` 18, fanout 4, `slot_count` 600, epoch 1. Page `V` (LPID 1)
/// dirty and unpinned, its MapLeaf held by `V`'s §4.1 pin, every other frame
/// reserved by the controller, which also holds the gate shared; X's flip
/// waits behind it. T (holding nothing) reserves a frame: its sweep passes
/// `V` over as `FlipPending` (its gate try fails) and finds nothing else;
/// paused at `SweepEnded`, T holds nothing. The controller then frees one
/// reservation and releases T. A frame came free since T's A1, so T looks
/// again and takes it, never restarting: `Done("T")` comes before any
/// `Restarting`. The card's letter without the re-check (`FlipPending` at
/// once) fails it: T fires `Restarting` and waits for the flip.
#[test]
fn flip_pending_rechecks_free() {
    assert_eq!(min_f(1), 18);
    let (_file, pool) = wb_pool::<StdPrimitives>(18, 4, 600, 1, H_MIN_TEST).unwrap();
    let (v, p) = s3_new_page(&pool, 0x61).unwrap();
    drop(p);
    let mut reserved: Vec<_> = (0..16).map(|_| pool.reserve().unwrap()).collect();
    assert_eq!(pool.stats().unwrap().free, 0);
    let m0 = pool.frame_of(mapleaf(0)).unwrap().unwrap();
    assert_eq!(pool.map_pin_info(m0).unwrap().dirty_pins, 1);
    assert_eq!(
        pool.frame_info(m0).unwrap().pins,
        1,
        "only V's §4.1 pin holds M"
    );
    let hold = pool.gate_shared().unwrap();
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.report(GateExclusiveWaiting, NO_KEY);
    pz.pause_at(SweepEnded, NO_KEY);
    pz.report(Restarting, NO_KEY);
    let mut freed = None;
    let (first_x, first_t, decided, r_x, r_t) = thread::scope(|s| {
        let x = spawn(s, &pz, "X", || s1_flip(&pool));
        // X holds its registration: every gate try now fails.
        let first_x = pz.next_event();
        let mut first_t = Event::Disconnected;
        let mut decided = None;
        let mut t = None;
        if seen(&first_x, GateExclusiveWaiting, NO_KEY) {
            t = Some(spawn(s, &pz, "T", || {
                pool.reserve_with(&Hold::nothing()).map(|r| r.frame())
            }));
            // T has passed V over for the pending flip; it holds nothing.
            first_t = pz.next_event();
            if arrived(&first_t, SweepEnded, NO_KEY) {
                let r = reserved.pop().unwrap();
                freed = Some(r.frame());
                drop(r);
                pz.release(SweepEnded, NO_KEY);
                let mut others = 0;
                loop {
                    let ev = pz.next_event();
                    if seen(&ev, Restarting, NO_KEY) {
                        decided = Some("restarting");
                        break;
                    }
                    if done(&ev, "T") {
                        decided = Some("done");
                        break;
                    }
                    others += 1;
                    if others > 16 {
                        break;
                    }
                }
            }
        }
        pz.release_all();
        drop(hold);
        let r_t = t.map(|t| t.join());
        (first_x, first_t, decided, x.join(), r_t)
    });
    assert!(seen(&first_x, GateExclusiveWaiting, NO_KEY), "{first_x:?}");
    assert!(arrived(&first_t, SweepEnded, NO_KEY), "{first_t:?}");
    assert_eq!(
        decided,
        Some("done"),
        "T looked again at the frame that came free instead of restarting"
    );
    assert_eq!(r_x.unwrap().unwrap(), 2);
    let got = r_t.expect("T ran").unwrap().unwrap();
    assert_eq!(Some(got), freed, "T took the frame that came free");
    assert_eq!(v, 1);
    drop(reserved);
}

/// `growth_race_level` (card evidence for LEVEL-GROW-UNCONDITIONAL, A-4a's
/// row; card A-3c, Work 14.3): fanout 2, root at level 1. T2 paused at its
/// first `GrowLocking` (it has read the root as `(R, 1)` and holds its gate
/// and no map latch) while T1 grows the map for leaf 5 to the end; T2 then
/// continues from the new root and changes nothing.
#[test]
fn growth_race_level() {
    assert_eq!(min_f(3), 46);
    let (_file, pool) = wb_pool::<StdPrimitives>(46, 2, 600, 1, H_MIN_TEST)
        .map_err(StoreError::Invalid)
        .unwrap();
    assert_eq!(pool.map_shape().unwrap().root_level, 1);
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(GrowLocking, mapleaf(5));
    let (first, after, t1_id, r2) = thread::scope(|s| {
        let t2 = spawn(s, &pz, "T2", || -> Result<(), StoreError> {
            let g = pool.gate_shared()?;
            pool.grow_map(5, &g)
        });
        let first = pz.next_event();
        let (mut after, mut t1_id, mut t1) = (None, None, None);
        if arrived(&first, GrowLocking, mapleaf(5)) {
            let h = spawn(s, &pz, "T1", || -> Result<Option<u32>, StoreError> {
                let g = pool.gate_shared()?;
                pool.grow_map(5, &g)?;
                pool.leaf_parent_id(5)
            });
            after = Some(pz.next_event());
            t1 = Some(h.join());
        }
        pz.release_all();
        if let Some(joined) = t1 {
            t1_id = joined.ok().and_then(|r| r.ok()).flatten();
        }
        (first, after, t1_id, t2.join())
    });
    assert!(arrived(&first, GrowLocking, mapleaf(5)), "{first:?}");
    assert_eq!(after, Some(Event::Done("T1")));
    r2.unwrap().unwrap();
    assert_eq!(
        pool.map_shape().unwrap(),
        MapShape {
            root_level: 3,
            inners: 5
        }
    );
    let id = pool.leaf_parent_id(5).unwrap();
    assert!(id.is_some());
    assert_eq!(id, t1_id, "leaf 5's parent is the one T1 recorded");
}

/// `growth_race_inner` (card evidence for MAPINNER-CREATE-UNCONDITIONAL,
/// A-4a's row; card A-3c, Work 14.3): fanout 2, grown for leaf 4 first, so
/// that leaf 6's MapInner is the only one missing. T2 paused at its first
/// `GrowLocking` (no level is needed, so it fires before T2 locks the root;
/// T2 holds its gate and no map latch) while T1 grows the map for leaf 6;
/// T2 then finds the MapInner T1 made and makes no other.
#[test]
fn growth_race_inner() {
    assert_eq!(min_f(3), 46);
    let (_file, pool) = wb_pool::<StdPrimitives>(46, 2, 600, 1, H_MIN_TEST)
        .map_err(StoreError::Invalid)
        .unwrap();
    {
        let g = pool.gate_shared().unwrap();
        pool.grow_map(4, &g).unwrap();
    }
    assert_eq!(
        pool.map_shape().unwrap(),
        MapShape {
            root_level: 3,
            inners: 5
        }
    );
    let pz = Pauser::new();
    pool.set_hook(Some(pz.hook()));
    pz.pause_at(GrowLocking, mapleaf(6));
    let (first, after, t1_id, r2) = thread::scope(|s| {
        let t2 = spawn(s, &pz, "T2", || -> Result<(), StoreError> {
            let g = pool.gate_shared()?;
            pool.grow_map(6, &g)
        });
        let first = pz.next_event();
        let (mut after, mut t1_id, mut t1) = (None, None, None);
        if arrived(&first, GrowLocking, mapleaf(6)) {
            let h = spawn(s, &pz, "T1", || -> Result<Option<u32>, StoreError> {
                let g = pool.gate_shared()?;
                pool.grow_map(6, &g)?;
                pool.leaf_parent_id(6)
            });
            after = Some(pz.next_event());
            t1 = Some(h.join());
        }
        pz.release_all();
        if let Some(joined) = t1 {
            t1_id = joined.ok().and_then(|r| r.ok()).flatten();
        }
        (first, after, t1_id, t2.join())
    });
    assert!(arrived(&first, GrowLocking, mapleaf(6)), "{first:?}");
    assert_eq!(after, Some(Event::Done("T1")));
    r2.unwrap().unwrap();
    assert_eq!(
        pool.map_shape().unwrap(),
        MapShape {
            root_level: 3,
            inners: 6
        }
    );
    let id = pool.leaf_parent_id(6).unwrap();
    assert!(id.is_some());
    assert_eq!(id, t1_id, "leaf 6's parent is the one T1 recorded");
}
