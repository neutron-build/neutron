//! Card A-3a's single-threaded tests of `buf` (Work 11): what a pin finds
//! (S-STORE §5.4), loads under I-INTEGRITY (§3.3, §4.4), keys kept apart
//! (§5.1), creation (§5.4, §6.4, §6.8), the step by which a frame leaves the
//! page table (§5.2), `mark_written`'s checks, freed frames (§4.2), pin
//! counts, the fixed pool (§5.1, §5.7; R-1-memory §5.1), the chained table,
//! and the order of an eviction's entry and removal (§5.4, §4.1).
//!
//! Fixtures are at `PS` = 4096, epoch 1, slots in 10..120, clear of the
//! reserved slots 256 and 257. The tests after `evict_sets_entry_first` go
//! beyond the card's list: each pins a rule a systematic mutant sweep of
//! `buf` (every field and comparison) would otherwise leave unjudged.
//!
//! Card A-3b-0 (Work 11) changed only what its frame acquisition and pool
//! sizing answer: `create_outcomes` (e), which asked for a frame with every
//! frame pinned; the `PoolConfig` literals, which name `callers`; the
//! `FrameInfo` and `PoolStats` literals, which name the reference bit and the
//! acquisition's counters; and the pools of fewer frames than §5.4's minimum,
//! which open through the test-only constructor with every assertion kept.

mod buf_support;

use std::sync::{Arc, Mutex};

use buf_support::{
    EventLog, Evicted, FaultFile, MARK, PS, SPOIL, TestMap, drop_clean, evict_to, fixture, marker,
    offset, page, put, put_leaf,
};
use nucleus_env::{EnvError, File};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, CurrentRef, FrameId, FrameInfo, FrameState, Hook, HookPoint, PageKey, Pin,
    PinOutcome, Pool, PoolConfig, PoolStats, Removal,
};
use nucleus_store::map::{self, Radix};
use nucleus_store::meta::Ref;
use nucleus_store::page::{self, Kind};

const L1: PageKey = PageKey::Lpid(1);
const L2: PageKey = PageKey::Lpid(2);
const L4: PageKey = PageKey::Lpid(4);
const L7: PageKey = PageKey::Lpid(7);
const L9: PageKey = PageKey::Lpid(9);
const M0: PageKey = PageKey::MapLeaf(0);

/// The pin out of a `pin` result, or the test fails.
macro_rules! pinned {
    ($e:expr) => {
        match $e {
            Ok(PinOutcome::Pinned(p)) => p,
            other => panic!("expected Pinned, got {other:?}"),
        }
    };
}

/// The created page out of a `create` result, or the test fails.
macro_rules! created {
    ($e:expr) => {
        match $e {
            Ok(CreateOutcome::Created(c)) => c,
            other => panic!("expected Created, got {other:?}"),
        }
    };
}

/// The error out of a result, or the test fails.
macro_rules! error {
    ($e:expr) => {
        match $e {
            Err(e) => e,
            Ok(other) => panic!("expected an error, got {other:?}"),
        }
    };
}

/// A frame that is `Free` in `pool`.
fn free_frame(pool: &Pool<FaultFile>) -> Result<FrameId, StoreError> {
    for i in 0..pool.frames() {
        if pool.frame_info(FrameId(i))?.state == FrameState::Free {
            return Ok(FrameId(i));
        }
    }
    Err(StoreError::Invalid("no free frame".into()))
}

#[test]
fn pin_finds() {
    // L1 at slot 10 (Leaf, marker 0x11); L4 mapped at slot 11, never pinned;
    // M0 at slot 12 (MapLeaf, header id position_id(Map, 0, 0) = 0, marker
    // 0x30); L9 unmapped.
    let fresh = || {
        let (file, map, pool) = fixture(4).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        put_leaf(&file, &map, 4, 11, 0x44).unwrap();
        let id0 = map::position_id(Radix::Map, 0, 0).unwrap();
        assert_eq!(id0, 0);
        map.set(M0, put(&file, 12, Kind::MapLeaf, id0, 1, 0x30).unwrap());
        (file, map, pool)
    };

    // (a) and (g): unmapped, twice; nothing left behind.
    {
        let (file, map, pool) = fresh();
        let out = pool.pin(L9, &map).unwrap();
        assert!(matches!(out, PinOutcome::Unmapped), "{out:?}");
        assert_eq!(pool.frame_of(L9).unwrap(), None);
        assert_eq!(pool.stats().unwrap().free, 4);
        assert_eq!(file.reads(), 0);
        assert_eq!(map.lookups(), 1);
        let out = pool.pin(L9, &map).unwrap();
        assert!(matches!(out, PinOutcome::Unmapped), "{out:?}");
        let s = pool.stats().unwrap();
        assert_eq!((s.free, s.orphan, s.loading), (4, 0, 0));
    }

    // (b), (c), (f) and (i), on one fixture.
    {
        let (file, map, pool) = fresh();
        let p1 = pinned!(pool.pin(L1, &map));
        assert_eq!(p1.key(), L1);
        assert_eq!(file.read_log(), vec![(40_960, 4096)]);
        {
            let g = p1.latch_shared().unwrap();
            assert_eq!(g[MARK], 0x11);
            assert_eq!(g.content_epoch(), 1);
            assert!(!g.is_dirty());
            assert!(!g.is_freed());
        }
        let s = pool.stats().unwrap();
        assert_eq!((s.resident, s.free), (1, 3));

        // (c) the first pin held: the same frame, no read, no lookup.
        let lookups = map.lookups();
        let p2 = pinned!(pool.pin(L1, &map));
        assert_eq!(p2.frame(), p1.frame());
        assert_eq!(file.reads(), 1);
        assert_eq!(map.lookups(), lookups);
        assert_eq!(pool.frame_info(p1.frame()).unwrap().pins, 2);
        drop(p2);

        // (f) pin_resident.
        let p3 = pool.pin_resident(L1).unwrap().expect("L1 is resident");
        assert_eq!(p3.frame(), p1.frame());
        drop(p3);
        let (reads, lookups) = (file.reads(), map.lookups());
        assert!(pool.pin_resident(L9).unwrap().is_none());
        assert!(pool.pin_resident(L4).unwrap().is_none());
        assert_eq!((file.reads(), map.lookups()), (reads, lookups));

        // (i) pin_frame.
        let pf = pool
            .pin_frame(p1.frame())
            .unwrap()
            .expect("a resident frame");
        assert_eq!(pf.key(), L1);
        assert_eq!(pf.frame(), p1.frame());
        drop(pf);
        let free = free_frame(&pool).unwrap();
        assert!(pool.pin_frame(free).unwrap().is_none());
        let res = pool.reserve().unwrap();
        assert!(pool.pin_frame(res.frame()).unwrap().is_none());
    }

    // (d) resident, never written: found, with no reference read.
    {
        let (_file, map, pool) = fresh();
        let c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1));
        let p = c.release();
        let lookups = map.lookups();
        let q = pinned!(pool.pin(L2, &map));
        assert_eq!(q.frame(), p.frame());
        assert_eq!(map.lookups(), lookups);
    }

    // (e) resident, freed: found.
    {
        let (_file, map, pool) = fresh();
        let mut c = created!(pool.create(pool.reserve().unwrap(), PageKey::Lpid(3), Kind::Leaf, 1));
        c.mark_freed().unwrap();
        let p = c.release();
        let q = pinned!(pool.pin(PageKey::Lpid(3), &map));
        assert_eq!(q.frame(), p.frame());
        assert!(pool.frame_info(q.frame()).unwrap().freed);
    }

    // (h) a MapLeaf.
    {
        let (_file, map, pool) = fresh();
        let p = pinned!(pool.pin(M0, &map));
        assert_eq!(marker(&p).unwrap(), 0x30);
    }
}

#[test]
fn load_integrity() {
    // After each error: nothing in the table, both frames free, nothing
    // loading or orphaned, and a good key at another slot still loads.
    let after_error = |file: &FaultFile, map: &TestMap, pool: &Pool<FaultFile>, key: PageKey| {
        assert_eq!(pool.frame_of(key).unwrap(), None);
        let s = pool.stats().unwrap();
        assert_eq!((s.free, s.loading, s.orphan), (2, 0, 0));
        put_leaf(file, map, 5, 12, 0x55).unwrap();
        let p = pinned!(pool.pin(PageKey::Lpid(5), map));
        assert_eq!(marker(&p).unwrap(), 0x55);
    };
    let corrupt = |e: StoreError| assert!(matches!(e, StoreError::Corrupt(_)), "{e:?}");

    // (a) a body byte flipped after sealing.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let (mut b, crc) = page(Kind::Leaf, 7, 1, 0x77).unwrap();
        b[SPOIL] ^= 0xFF;
        file.write_at(offset(10), &b).unwrap();
        map.set(
            L7,
            Ref {
                slot: 10,
                epoch: 1,
                crc,
            },
        );
        corrupt(error!(pool.pin(L7, &map)));
        after_error(&file, &map, &pool, L7);
    }
    // (b) the reference's crc off by one bit.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let r = put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap();
        map.set(
            L7,
            Ref {
                crc: r.crc ^ 1,
                ..r
            },
        );
        corrupt(error!(pool.pin(L7, &map)));
        after_error(&file, &map, &pool, L7);
    }
    // (c) sealed with header id 8, the reference its own crc.
    {
        let (file, map, pool) = fixture(2).unwrap();
        map.set(L7, put(&file, 10, Kind::Leaf, 8, 1, 0x77).unwrap());
        corrupt(error!(pool.pin(L7, &map)));
        after_error(&file, &map, &pool, L7);
    }
    // (d) the reference's epoch 2 against header epoch 1.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let r = put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap();
        map.set(L7, Ref { epoch: 2, ..r });
        corrupt(error!(pool.pin(L7, &map)));
        after_error(&file, &map, &pool, L7);
    }
    // (e) kinds the key does not admit, each sealed with its own crc.
    for (kind, key) in [
        (Kind::MapLeaf, L7),
        (Kind::MapInner, L7),
        (Kind::Leaf, PageKey::MapLeaf(7)),
    ] {
        let (file, map, pool) = fixture(2).unwrap();
        map.set(key, put(&file, 10, kind, 7, 1, 0x77).unwrap());
        corrupt(error!(pool.pin(key, &map)));
        after_error(&file, &map, &pool, key);
    }
    // (f) slots that are never a page home: no read.
    for slot in [1, 2, 256, 257] {
        let (file, map, pool) = fixture(2).unwrap();
        let r = put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap();
        map.set(L7, Ref { slot, ..r });
        corrupt(error!(pool.pin(L7, &map)));
        assert_eq!(file.reads(), 0, "slot {slot}");
        after_error(&file, &map, &pool, L7);
    }
    // (g) a slot whose offset is past u64: no read.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let r = put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap();
        map.set(
            L7,
            Ref {
                slot: u64::MAX / 4096 + 1,
                ..r
            },
        );
        corrupt(error!(pool.pin(L7, &map)));
        assert_eq!(file.reads(), 0);
        after_error(&file, &map, &pool, L7);
    }
    // (h) a slot past the end of a file 20 slots long.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let r = put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap();
        file.set_len(offset(20)).unwrap();
        map.set(L7, Ref { slot: 50, ..r });
        corrupt(error!(pool.pin(L7, &map)));
        after_error(&file, &map, &pool, L7);
    }
    // (i) a failed read.
    {
        let (file, map, pool) = fixture(2).unwrap();
        map.set(L7, put(&file, 10, Kind::Leaf, 7, 1, 0x77).unwrap());
        file.fail_read_at(Some(offset(10)));
        let e = error!(pool.pin(L7, &map));
        assert!(matches!(e, StoreError::Env(EnvError::Io { .. })), "{e:?}");
        file.fail_read_at(None);
        after_error(&file, &map, &pool, L7);
    }
    // (j) stale bytes behind a short read (F 1).
    {
        let (file, map, pool) = fixture(1).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        let f = pinned!(pool.pin(L1, &map)).frame();
        let v = pool.try_victim(f).unwrap().expect("unpinned");
        let Removal::Removed(res) = v.remove_from_table().unwrap() else {
            panic!("a clean, unpinned frame is removed");
        };
        drop(res);
        file.set_len(offset(10)).unwrap();
        corrupt(error!(pool.pin(L1, &map)));
        assert_eq!(pool.frame_of(L1).unwrap(), None);
        let s = pool.stats().unwrap();
        assert_eq!((s.free, s.loading, s.orphan), (1, 0, 0));
        put_leaf(&file, &map, 5, 12, 0x55).unwrap();
        let p = pinned!(pool.pin(PageKey::Lpid(5), &map));
        assert_eq!(marker(&p).unwrap(), 0x55);
    }
}

#[test]
fn key_namespaces() {
    let (file, map, pool) = fixture(4).unwrap();
    put_leaf(&file, &map, 5, 10, 0x55).unwrap();
    let id5 = map::position_id(Radix::Map, 0, 5).unwrap();
    assert_eq!(id5, 5);
    let m5 = PageKey::MapLeaf(5);
    map.set(m5, put(&file, 11, Kind::MapLeaf, id5, 1, 0x66).unwrap());
    let a = pinned!(pool.pin(PageKey::Lpid(5), &map));
    let b = pinned!(pool.pin(m5, &map));
    assert_ne!(a.frame(), b.frame());
    let ga = a.latch_shared().unwrap();
    let gb = b.latch_shared().unwrap();
    assert_eq!((ga[MARK], gb[MARK]), (0x55, 0x66));
    assert_eq!((ga[4], gb[4]), (3, 6));
    assert_eq!(pool.frame_of(PageKey::Lpid(5)).unwrap(), Some(a.frame()));
    assert_eq!(pool.frame_of(m5).unwrap(), Some(b.frame()));
}

#[test]
fn create_outcomes() {
    // (a) and (b), on one fixture.
    {
        let (_file, _map, pool) = fixture(3).unwrap();
        let c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1));
        let f = c.frame();
        assert_eq!(
            pool.frame_info(f).unwrap(),
            FrameInfo {
                state: FrameState::Resident,
                key: Some(L2),
                pins: 1,
                freed: false,
                // Card A-3b-0: a created page's pin sets the clock's bit.
                referenced: true
            }
        );
        assert_eq!(c[4], 3);
        assert_eq!(c[8..16], 2u64.to_le_bytes());
        assert_eq!(c[16..20], 1u32.to_le_bytes());
        assert!(c[24..].iter().all(|&b| b == 0));
        assert!(c.is_dirty());
        assert_eq!(c.content_epoch(), 1);
        let second = pool.pin_resident(L2).unwrap().expect("published");
        assert!(second.try_latch_shared().unwrap().is_none());
        assert!(second.try_latch_exclusive().unwrap().is_none());
        let kept = c.release();
        assert!(second.try_latch_shared().unwrap().is_some());
        assert!(second.try_latch_exclusive().unwrap().is_some());
        drop(second);

        // (b) a live key: Exists, a pin on the same frame.
        let free = pool.stats().unwrap().free;
        let res2 = pool.reserve().unwrap();
        let (p, r) = match pool.create(res2, L2, Kind::Leaf, 1) {
            Ok(CreateOutcome::Exists(p, r)) => (p, r),
            other => panic!("expected Exists, got {other:?}"),
        };
        assert_eq!(p.frame(), kept.frame());
        assert_eq!(pool.frame_info(f).unwrap().pins, 2);
        drop(r);
        assert_eq!(pool.stats().unwrap().free, free);
    }
    // (c) a freed key: remade in the same frame.
    {
        let (_file, _map, pool) = fixture(3).unwrap();
        let p1 = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1)).release();
        p1.latch_exclusive().unwrap().mark_freed().unwrap();
        let free = pool.stats().unwrap().free;
        let c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Inner, 2));
        assert_eq!(c.frame(), p1.frame());
        let info = pool.frame_info(p1.frame()).unwrap();
        assert_eq!((info.pins, info.freed), (2, false));
        assert_eq!(c[4], 4);
        assert_eq!(c[16..20], 2u32.to_le_bytes());
        assert_eq!(pool.stats().unwrap().free, free);
    }
    // (d) refused creations return their reservation.
    for (key, kind) in [
        (PageKey::Lpid(0), Kind::Leaf),
        (PageKey::Lpid(3), Kind::MapLeaf),
        (PageKey::MapLeaf(1), Kind::Leaf),
    ] {
        let (_file, _map, pool) = fixture(3).unwrap();
        let free = pool.stats().unwrap().free;
        let e = error!(pool.create(pool.reserve().unwrap(), key, kind, 1));
        assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
        assert_eq!(pool.stats().unwrap().free, free);
    }
    // (e) every frame pinned: no frame to take without a wait. Card A-3b-0
    // (Work 11): a pin that misses may evict or wait for an unpin, so the
    // form that never waits answers `None`; `reserve` there is
    // `unpin_wait_then_served`'s (buf_evict).
    {
        let (_file, _map, pool) = fixture(3).unwrap();
        let pins: Vec<_> = (2..5)
            .map(|x| {
                created!(pool.create(pool.reserve().unwrap(), PageKey::Lpid(x), Kind::Leaf, 1))
                    .release()
            })
            .collect();
        let log = EventLog::new();
        // The hook fails the test at an `UnpinWait`: no other thread could
        // end that wait.
        pool.set_hook(Some(log.hook_no_wait()));
        let free = pool.stats().unwrap().free;
        let got = pool.try_reserve();
        assert!(matches!(got, Ok(None)), "{got:?}");
        assert!(
            !log.all().iter().any(|&(p, _, _)| p == HookPoint::UnpinWait),
            "{:?}",
            log.all()
        );
        let s = pool.stats().unwrap();
        assert_eq!(
            (s.free, s.reserved, s.unpin_waiters, s.evictions),
            (free, 0, 0, 0)
        );
        pool.set_hook(None);
        drop(pins);
    }
}

#[test]
fn remove_rules() {
    // (a) loaded, unpinned: removed, and a pin reloads.
    {
        let (file, map, pool) = fixture(3).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        let f = pinned!(pool.pin(L1, &map)).frame();
        let v = pool.try_victim(f).unwrap().expect("unpinned");
        let Removal::Removed(res) = v.remove_from_table().unwrap() else {
            panic!("a clean, unpinned frame is removed");
        };
        assert_eq!(pool.frame_of(L1).unwrap(), None);
        let free = pool.stats().unwrap().free;
        drop(res);
        assert_eq!(pool.stats().unwrap().free, free + 1);
        let p = pinned!(pool.pin(L1, &map));
        assert_eq!(file.reads(), 2);
        assert_eq!(marker(&p).unwrap(), 0x11);
    }
    // (b) pinned: not a victim.
    {
        let (file, map, pool) = fixture(3).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        let p = pinned!(pool.pin(L1, &map));
        assert!(pool.try_victim(p.frame()).unwrap().is_none());
    }
    // (c) never written: kept, and still found.
    {
        let (_file, map, pool) = fixture(3).unwrap();
        let f = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1))
            .release()
            .frame();
        let v = pool.try_victim(f).unwrap().expect("unpinned");
        assert!(matches!(v.remove_from_table().unwrap(), Removal::Kept));
        assert_eq!(pool.frame_of(L2).unwrap(), Some(f));
        let p = pinned!(pool.pin(L2, &map));
        assert_eq!(p.frame(), f);
    }
    // (d) dirty after a write: kept.
    {
        let (file, map, pool) = fixture(3).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        let p = pinned!(pool.pin(L1, &map));
        {
            let mut g = p.latch_exclusive().unwrap();
            g.mark_dirty(1).unwrap();
            g[MARK] = 0x12;
        }
        let f = p.frame();
        drop(p);
        let v = pool.try_victim(f).unwrap().expect("unpinned");
        assert!(matches!(v.remove_from_table().unwrap(), Removal::Kept));
        assert_eq!(pool.frame_of(L1).unwrap(), Some(f));
    }
    // (e) a second try_victim while the first guard lives.
    {
        let (file, map, pool) = fixture(3).unwrap();
        put_leaf(&file, &map, 1, 10, 0x11).unwrap();
        let f = pinned!(pool.pin(L1, &map)).frame();
        let v = pool.try_victim(f).unwrap().expect("unpinned");
        assert!(pool.try_victim(f).unwrap().is_none());
        drop(v);
    }
    // (f) a Free frame and a reserved frame are never victims.
    {
        let (_file, _map, pool) = fixture(3).unwrap();
        assert!(pool.try_victim(FrameId(2)).unwrap().is_none());
        let res = pool.reserve().unwrap();
        assert!(pool.try_victim(res.frame()).unwrap().is_none());
    }
}

#[test]
fn mark_written_checks() {
    let (_file, _map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), PageKey::Lpid(3), Kind::Leaf, 1));
    c[MARK] = 0x33;
    c.mark_dirty(1).unwrap();
    let crc = page::seal(&mut c).unwrap();
    let home = |slot, epoch, crc| Ref { slot, epoch, crc };
    for bad in [
        home(0, 1, crc),
        home(1, 1, crc),
        home(256, 1, crc),
        home(20, 1, crc ^ 1),
        home(20, 2, crc),
    ] {
        let e = error!(c.mark_written(bad));
        assert!(matches!(e, StoreError::Invalid(_)), "{bad:?}: {e:?}");
        assert!(c.is_dirty(), "{bad:?}");
    }
    // A body byte changed after sealing.
    c[MARK] = 0x34;
    let e = error!(c.mark_written(home(20, 1, crc)));
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    assert!(c.is_dirty());
    let crc2 = page::seal(&mut c).unwrap();
    c.mark_written(home(20, 1, crc2)).unwrap();
    assert!(!c.is_dirty());
}

#[test]
fn freed_frame() {
    let (file, map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), L4, Kind::Leaf, 1));
    let writes = file.writes();
    c.mark_freed().unwrap();
    assert!(c.is_freed());
    assert!(!c.is_dirty());
    let e = error!(c.mark_dirty(1));
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    assert!(!c.is_dirty());
    let f = c.frame();
    drop(c.release());
    let v = pool.try_victim(f).unwrap().expect("unpinned");
    let Removal::Removed(res) = v.remove_from_table().unwrap() else {
        panic!("a freed, unpinned frame is removed");
    };
    // Beyond the card: the removed frame is Reserved with no key and no
    // freed mark.
    assert_eq!(
        pool.frame_info(res.frame()).unwrap(),
        FrameInfo {
            state: FrameState::Reserved,
            key: None,
            pins: 0,
            freed: false,
            // Card A-3b-0: a frame that becomes `Reserved` loses the clock's
            // bit its creation set.
            referenced: false
        }
    );
    drop(res);
    assert_eq!(file.writes(), writes);
    let out = pool.pin(L4, &map).unwrap();
    assert!(matches!(out, PinOutcome::Unmapped), "{out:?}");
}

#[test]
fn pin_count_overflow() {
    let (file, map, pool) = fixture(1).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let f = pinned!(pool.pin(L1, &map)).frame();
    pool.force_pins(f, u64::MAX).unwrap();
    let invalid = |e: StoreError| assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    invalid(error!(pool.pin_resident(L1)));
    invalid(error!(pool.pin(L1, &map)));
    invalid(error!(pool.pin_frame(f)));
    assert_eq!(pool.frame_info(f).unwrap().pins, u64::MAX);
    pool.force_pins(f, 0).unwrap();
}

#[test]
fn pool_fixed() {
    let (file, map, pool) = fixture(5).unwrap();
    for (lpid, slot) in [(1, 10), (2, 11), (3, 12)] {
        put_leaf(&file, &map, lpid, slot, lpid as u8).unwrap();
    }
    let buckets = pool.stats().unwrap().buckets;
    let footprint = pool.footprint();
    // Fresh slots from 40 up, never a reserved one (256, 257).
    let mut next_slot = 40u64;
    let mut fresh_slot = || {
        while (256..258).contains(&next_slot) {
            next_slot += 1;
        }
        next_slot += 1;
        next_slot - 1
    };
    for round in 0..2000u64 {
        let pins: Vec<_> = [1, 2, 3]
            .map(|x| pinned!(pool.pin(PageKey::Lpid(x), &map)))
            .into_iter()
            .collect();
        let a = PageKey::Lpid(1000 + 2 * round);
        let b = PageKey::Lpid(1001 + 2 * round);
        for key in [a, b] {
            let mut c = created!(pool.create(pool.reserve().unwrap(), key, Kind::Leaf, 1));
            c.mark_dirty(1).unwrap();
            c[MARK] = 0xC0;
            let f = c.release().frame();
            assert_eq!(
                evict_to(&pool, &map, f, fresh_slot()).unwrap(),
                Evicted::Removed
            );
        }
        let p = pinned!(pool.pin(a, &map));
        {
            let mut g = p.latch_exclusive().unwrap();
            map.clear(a);
            g.mark_freed().unwrap();
        }
        drop(p);
        drop(pins);
        for i in 0..pool.frames() {
            drop_clean(&pool, FrameId(i)).unwrap();
        }
        let s = pool.stats().unwrap();
        let PoolStats {
            frames,
            free,
            reserved,
            loading,
            resident,
            orphan,
            reads: _,
            buckets: b2,
            // Card A-3b-0's fields: this loop's reserves come off the free
            // list, so it asserts nothing of them.
            evictions: _,
            clock_hand: _,
            unpin_waiters: _,
        } = s;
        assert_eq!((b2, frames), (buckets, 5), "round {round}");
        assert_eq!(
            free + reserved + loading + resident + orphan,
            5,
            "round {round}"
        );
        assert_eq!(pool.footprint(), footprint, "round {round}");
    }
    for frames in [1u32, 5, 64] {
        for page_size in [4096u32, 65536] {
            // Card A-3b-0 (Work 11): 1 and 5 frames are below §5.4's minimum
            // (34 for one caller), so these pools open through the test-only
            // constructor; the assertions are card A-3a's.
            let cfg = PoolConfig {
                frames,
                page_size,
                callers: 1,
            };
            let pool = Pool::new_below_minimum_for_tests(Arc::new(FaultFile::new().unwrap()), cfg)
                .unwrap();
            let fp = pool.footprint();
            assert_eq!(Pool::<FaultFile>::bytes_for(cfg).unwrap(), fp, "{cfg:?}");
            assert_eq!(fp.page_bytes, u64::from(frames) * u64::from(page_size));
            // Beyond the card: the table's bytes against the type sizes.
            let head = std::mem::size_of::<Option<FrameId>>() as u64;
            let id = std::mem::size_of::<FrameId>() as u64;
            let n = u64::from(frames);
            assert_eq!(
                fp.table_bytes,
                u64::from(frames.next_power_of_two()) * head + n * id
            );
            assert_eq!(pool.stats().unwrap().buckets, frames.next_power_of_two());
        }
    }
}

#[test]
fn pool_config() {
    let new = |frames, page_size| {
        Pool::new(
            Arc::new(FaultFile::new().unwrap()),
            PoolConfig {
                frames,
                page_size,
                callers: 1,
            },
        )
    };
    for (frames, page_size) in [(0, 4096), ((1 << 31) + 1, 4096), (4, 12288)] {
        let e = error!(new(frames, page_size));
        assert!(
            matches!(e, StoreError::Invalid(_)),
            "{frames} {page_size}: {e:?}"
        );
        // Beyond the card: bytes_for refuses what new refuses. It reads
        // neither `callers` (card A-3b-0) nor the minimum.
        let e = error!(Pool::<FaultFile>::bytes_for(PoolConfig {
            frames,
            page_size,
            callers: 1,
        }));
        assert!(
            matches!(e, StoreError::Invalid(_)),
            "{frames} {page_size}: {e:?}"
        );
    }
    // Card A-3b-0 (Work 11): no callers is refused, before any allocation.
    let e = error!(Pool::new(
        Arc::new(FaultFile::new().unwrap()),
        PoolConfig {
            frames: 64,
            page_size: 4096,
            callers: 0,
        }
    ));
    assert!(
        matches!(&e, StoreError::Invalid(m) if m.contains("must each be at least 1")),
        "{e:?}"
    );
    // Beyond the card: the bounds are inclusive, and the accessors say what
    // was asked for. Card A-3b-0 (Work 11): one frame is below §5.4's
    // minimum, so it opens through the test-only constructor.
    let pool = Pool::new_below_minimum_for_tests(
        Arc::new(FaultFile::new().unwrap()),
        PoolConfig {
            frames: 1,
            page_size: 65536,
            callers: 1,
        },
    )
    .unwrap();
    assert_eq!((pool.frames(), pool.page_size()), (1, 65536));
    let top = Pool::<FaultFile>::bytes_for(PoolConfig {
        frames: 1 << 31,
        page_size: 4096,
        callers: 1,
    })
    .unwrap();
    assert_eq!(top.page_bytes, 1 << 43);
    let head = std::mem::size_of::<Option<FrameId>>() as u64;
    let id = std::mem::size_of::<FrameId>() as u64;
    assert_eq!(top.table_bytes, (1u64 << 31) * (head + id));
}

#[test]
fn table_chains() {
    // F 3, so 4 buckets.
    let (file, map, pool) = fixture(3).unwrap();
    assert_eq!(pool.stats().unwrap().buckets, 4);
    let mut keys = (1u64..).map(PageKey::Lpid);
    let k1 = keys.next().unwrap();
    let bucket = pool.bucket_of(k1);
    let same: Vec<PageKey> = (2u64..)
        .map(PageKey::Lpid)
        .filter(|&k| pool.bucket_of(k) == bucket)
        .take(2)
        .collect();
    let (k2, k3) = (same[0], same[1]);
    let k4 = (2u64..)
        .map(PageKey::Lpid)
        .find(|&k| pool.bucket_of(k) != bucket)
        .unwrap();
    for (i, k) in [k1, k2, k3, k4].into_iter().enumerate() {
        let PageKey::Lpid(x) = k else { unreachable!() };
        put_leaf(&file, &map, x, 10 + i as u64, 0x41 + i as u8).unwrap();
    }
    let load = |k: PageKey| pinned!(pool.pin(k, &map)).frame();
    let (f1, f2, f3) = (load(k1), load(k2), load(k3));
    for (k, f) in [(k1, f1), (k2, f2), (k3, f3)] {
        assert_eq!(pool.frame_of(k).unwrap(), Some(f));
    }
    // The chain's middle.
    assert_eq!(drop_clean(&pool, f2).unwrap(), Evicted::Removed);
    let f4 = load(k4);
    assert_eq!(f4, f2);
    assert_eq!(pool.frame_of(k2).unwrap(), None);
    for (k, f) in [(k1, f1), (k3, f3), (k4, f4)] {
        assert_eq!(pool.frame_of(k).unwrap(), Some(f));
    }
    // The chain's head.
    assert_eq!(drop_clean(&pool, f3).unwrap(), Evicted::Removed);
    assert_eq!(pool.frame_of(k3).unwrap(), None);
    assert_eq!(pool.frame_of(k1).unwrap(), Some(f1));
    assert_eq!(drop_clean(&pool, f4).unwrap(), Evicted::Removed);
    assert_eq!(pool.stats().unwrap().free, 2);
    for (i, k) in [k1, k2, k3].into_iter().enumerate() {
        let p = pinned!(pool.pin(k, &map));
        assert_eq!(marker(&p).unwrap(), 0x41 + i as u8);
    }
}

#[test]
fn evict_sets_entry_first() {
    // (a) An LPID page never written, evicted step by step; a pin after
    // each step finds the page, never the LPID unmapped.
    for stop in 1..=7 {
        let (_file, map, pool) = fixture(3).unwrap();
        let mut c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1));
        c[MARK] = 0x22;
        c.mark_dirty(1).unwrap();
        let f = c.release().frame();
        let mut v = None;
        let mut crc = 0;
        let mut home = Ref::NULL;
        let mut res = None;
        let mut kept = None;
        let mut held = None;
        for step in 1..=7 {
            match step {
                1 => v = Some(pool.try_victim(f).unwrap().expect("unpinned")),
                2 => crc = page::seal(v.as_mut().unwrap()).unwrap(),
                3 => pool
                    .file()
                    .write_at(offset(30), v.as_ref().unwrap())
                    .unwrap(),
                4 => {
                    home = Ref {
                        slot: 30,
                        epoch: 1,
                        crc,
                    };
                    map.set(L2, home);
                }
                5 => v.as_mut().unwrap().mark_written(home).unwrap(),
                6 => match v.take().unwrap().remove_from_table().unwrap() {
                    Removal::Removed(r) => res = Some(r),
                    Removal::Kept => kept = Some(()),
                },
                _ => drop(res.take()),
            }
            if step == stop {
                let reads = pool.file().reads();
                let p = pinned!(pool.pin(L2, &map));
                if stop <= 5 {
                    // The frame `v` holds, still in the table: no read.
                    assert_eq!(p.frame(), f, "stop {stop}");
                    assert_eq!(pool.file().reads(), reads, "stop {stop}");
                } else {
                    // A fresh load from slot 30.
                    assert_eq!(pool.file().reads(), reads + 1, "stop {stop}");
                    assert_eq!(
                        pool.file().read_log().last(),
                        Some(&(offset(30), 4096)),
                        "stop {stop}"
                    );
                }
                held = Some(p);
            }
        }
        if stop <= 5 {
            assert!(kept.is_some(), "stop {stop}: the pinned frame is kept");
            let info = pool.frame_info(f).unwrap();
            assert_eq!(info.state, FrameState::Resident, "stop {stop}");
            assert!(!held.as_ref().unwrap().latch_shared().unwrap().is_dirty());
        }
        let p = held.take().unwrap();
        assert_eq!(marker(&p).unwrap(), 0x22, "stop {stop}");
        drop(p);
        let p = pinned!(pool.pin(L2, &map));
        assert_eq!(marker(&p).unwrap(), 0x22, "stop {stop}");
    }

    // (b) A map page's two-step write (§4.1): the pin keeps it in the table
    // until its parent's reference names the new home.
    {
        let (file, map, pool) = fixture(3).unwrap();
        map.set(M0, put(&file, 12, Kind::MapLeaf, 0, 1, 0x30).unwrap());
        let f = {
            let p = pinned!(pool.pin(M0, &map));
            let mut g = p.latch_exclusive().unwrap();
            g.mark_dirty(1).unwrap();
            g[MARK] = 0x31;
            p.frame()
        };
        let mut v = pool.try_victim(f).unwrap().expect("unpinned");
        let crc = page::seal(&mut v).unwrap();
        pool.file().write_at(offset(31), &v).unwrap();
        let home = Ref {
            slot: 31,
            epoch: 1,
            crc,
        };
        v.mark_written(home).unwrap();
        let p = v.pin_frame().unwrap();
        drop(v);
        // The gap: the parent's reference still names slot 12.
        assert!(pool.try_victim(f).unwrap().is_none());
        let q = pinned!(pool.pin(M0, &map));
        assert_eq!(marker(&q).unwrap(), 0x31);
        drop(q);
        map.set(M0, home);
        drop(p);
        assert_eq!(drop_clean(&pool, f).unwrap(), Evicted::Removed);
        let r = pinned!(pool.pin(M0, &map));
        assert_eq!(marker(&r).unwrap(), 0x31);
        assert_eq!(file.read_log().last(), Some(&(offset(31), 4096)));
    }
}

// ---- Beyond the card's list (each pins what a systematic sweep would hit).

/// The hook points fire where card A-3a's hook table places them, in order,
/// and `set_hook(None)` removes the hook.
#[test]
fn hook_placement() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let seen: Arc<Mutex<Vec<(HookPoint, PageKey, FrameId)>>> = Arc::default();
    let sink = Arc::clone(&seen);
    let hook: Hook = Arc::new(move |p, k, f| sink.lock().unwrap().push((p, k, f)));
    pool.set_hook(Some(hook));
    let take = || std::mem::take(&mut *seen.lock().unwrap());
    let f0 = FrameId(0);
    let p = pinned!(pool.pin(L1, &map));
    use HookPoint::*;
    assert_eq!(
        take(),
        vec![
            (MissReserved, L1, f0),
            (LoadPublished, L1, f0),
            (LoadResolved, L1, f0),
            (LoadRead, L1, f0),
            (Pinned, L1, f0)
        ]
    );
    let q = pool.pin_resident(L1).unwrap().unwrap();
    drop(q);
    drop(p);
    assert_eq!(
        take(),
        vec![(Pinned, L1, f0), (Unpinned, L1, f0), (Unpinned, L1, f0)]
    );
    assert!(matches!(pool.pin(L9, &map), Ok(PinOutcome::Unmapped)));
    let f1 = FrameId(1);
    assert_eq!(
        take(),
        vec![(MissReserved, L9, f1), (LoadPublished, L9, f1)]
    );
    let c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1));
    let p2 = c.release();
    assert_eq!(take(), vec![(CreateReady, L2, f1), (Pinned, L2, f1)]);
    let pf = pool.pin_frame(f1).unwrap().unwrap();
    drop(pf);
    assert_eq!(take(), vec![(Pinned, L2, f1), (Unpinned, L2, f1)]);
    // A live key: `Exists` pins the live frame; the reservation's frame
    // (frame 2) fired `CreateReady` first.
    let f2 = FrameId(2);
    match pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1) {
        Ok(CreateOutcome::Exists(p, r)) => drop((p, r)),
        other => panic!("expected Exists, got {other:?}"),
    }
    assert_eq!(
        take(),
        vec![(CreateReady, L2, f2), (Pinned, L2, f1), (Unpinned, L2, f1)]
    );
    // A freed key: remade in its own frame.
    p2.latch_exclusive().unwrap().mark_freed().unwrap();
    let c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 2));
    assert_eq!(c.frame(), f1);
    drop(c);
    drop(p2);
    assert_eq!(
        take(),
        vec![
            (CreateReady, L2, f2),
            (Pinned, L2, f1),
            (Unpinned, L2, f1),
            (Unpinned, L2, f1)
        ]
    );
    let v = pool.try_victim(f0).unwrap().unwrap();
    let vp = v.pin_frame().unwrap();
    assert!(matches!(v.remove_from_table().unwrap(), Removal::Kept));
    drop(vp);
    assert_eq!(
        take(),
        vec![
            (Pinned, L1, f0),
            (RemoveLatched, L1, f0),
            (Unpinned, L1, f0)
        ]
    );
    pool.set_hook(None);
    drop(pinned!(pool.pin(L1, &map)));
    assert_eq!(take(), vec![]);
}

/// `PageKey`'s header ids and kinds (§1, §3.1; card A-2's positions), and
/// the refusals before anything is touched.
#[test]
fn key_rules() {
    assert!(matches!(
        PageKey::Lpid(0).header_id(),
        Err(StoreError::Invalid(_))
    ));
    assert_eq!(PageKey::Lpid(1).header_id().unwrap(), 1);
    assert_eq!(PageKey::Lpid(u64::MAX).header_id().unwrap(), u64::MAX);
    assert_eq!(PageKey::MapLeaf(0).header_id().unwrap(), 0);
    assert_eq!(
        PageKey::MapLeaf((1 << 56) - 1).header_id().unwrap(),
        (1 << 56) - 1
    );
    assert!(matches!(
        PageKey::MapLeaf(1 << 56).header_id(),
        Err(StoreError::Invalid(_))
    ));
    // Every kind against both keys; the match names every kind, so a kind
    // added later fails to compile here.
    let all = [
        Kind::Leaf,
        Kind::Inner,
        Kind::Overflow,
        Kind::MapLeaf,
        Kind::MapInner,
        Kind::FreeSetLeaf,
        Kind::FreeSetInner,
    ];
    for kind in all {
        let (lpid, mapleaf) = match kind {
            Kind::Leaf | Kind::Inner | Kind::Overflow => (true, false),
            Kind::MapLeaf => (false, true),
            Kind::MapInner | Kind::FreeSetLeaf | Kind::FreeSetInner => (false, false),
        };
        assert_eq!(PageKey::Lpid(9).admits(kind), lpid, "{kind:?}");
        assert_eq!(PageKey::MapLeaf(9).admits(kind), mapleaf, "{kind:?}");
    }
    // Refused before the table, the map or a frame is touched.
    let (file, map, pool) = fixture(2).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let before = pool.stats().unwrap();
    for key in [PageKey::Lpid(0), PageKey::MapLeaf(1 << 56)] {
        assert!(
            matches!(pool.pin(key, &map), Err(StoreError::Invalid(_))),
            "{key:?}"
        );
        assert!(
            matches!(pool.pin_resident(key), Err(StoreError::Invalid(_))),
            "{key:?}"
        );
    }
    assert_eq!(map.lookups(), 0);
    assert_eq!(pool.stats().unwrap(), before);
    // An Overflow page is an LPID's: it loads and it is created.
    map.set(
        PageKey::Lpid(6),
        put(&file, 13, Kind::Overflow, 6, 1, 0x66).unwrap(),
    );
    assert_eq!(
        marker(&pinned!(pool.pin(PageKey::Lpid(6), &map))).unwrap(),
        0x66
    );
    let c = created!(pool.create(pool.reserve().unwrap(), PageKey::Lpid(8), Kind::Overflow, 1));
    assert_eq!(c[4], 5);
}

/// Bucket placement is card A-3a's pinned hash (Work 5), checked against
/// an implementation written here from the card's text.
#[test]
fn hash_placement() {
    fn oracle(key: PageKey) -> u64 {
        let (t, v) = match key {
            PageKey::Lpid(x) => (0u64, x),
            PageKey::MapLeaf(i) => (0x9E37_79B9_7F4A_7C15u64, i),
        };
        let mut k = v ^ t;
        k ^= k >> 33;
        k = k.wrapping_mul(0xff51_afd7_ed55_8ccd);
        k ^= k >> 33;
        k = k.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
        k ^= k >> 33;
        k
    }
    for frames in [1u32, 2, 3, 5, 64, 1000] {
        // Card A-3b-0 (Work 11): the pools below §5.4's minimum open through
        // the test-only constructor, which builds the same table.
        let pool = Pool::new_below_minimum_for_tests(
            Arc::new(FaultFile::new().unwrap()),
            PoolConfig {
                frames,
                page_size: PS,
                callers: 1,
            },
        )
        .unwrap();
        let mask = u64::from(frames.next_power_of_two() - 1);
        let keys = (0..300u64)
            .flat_map(|x| [PageKey::Lpid(x), PageKey::MapLeaf(x)])
            .chain([PageKey::Lpid(u64::MAX), PageKey::MapLeaf(u64::MAX)]);
        for key in keys {
            assert_eq!(
                u64::from(pool.bucket_of(key)),
                oracle(key) & mask,
                "{key:?}"
            );
        }
    }
}

/// Slot bounds (§4.4, §2.4): the first page slot and the slots either side
/// of the reserved range load; an offset whose page end is past `u64` is
/// refused unread; `mark_written` takes the same bounds.
#[test]
fn home_slot_bounds() {
    for slot in [3u64, 255, 258] {
        let (file, map, pool) = fixture(2).unwrap();
        put_leaf(&file, &map, 7, slot, 0x77).unwrap();
        assert_eq!(
            marker(&pinned!(pool.pin(L7, &map))).unwrap(),
            0x77,
            "slot {slot}"
        );
    }
    for slot in [u64::MAX / 4096, u64::MAX] {
        let (file, map, pool) = fixture(2).unwrap();
        map.set(
            L7,
            Ref {
                slot,
                epoch: 1,
                crc: 0,
            },
        );
        let e = error!(pool.pin(L7, &map));
        assert!(matches!(e, StoreError::Corrupt(_)), "slot {slot}: {e:?}");
        assert_eq!(file.reads(), 0, "slot {slot}");
    }
    let (_file, _map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), L7, Kind::Leaf, 1));
    let crc = page::seal(&mut c).unwrap();
    for slot in [2u64, 257, u64::MAX / 4096, u64::MAX / 4096 + 1] {
        let e = error!(c.mark_written(Ref {
            slot,
            epoch: 1,
            crc
        }));
        assert!(matches!(e, StoreError::Invalid(_)), "slot {slot}: {e:?}");
        assert!(c.is_dirty(), "slot {slot}");
    }
    for slot in [3u64, 255, 258] {
        c.mark_dirty(1).unwrap();
        c.mark_written(Ref {
            slot,
            epoch: 1,
            crc,
        })
        .unwrap();
        assert!(!c.is_dirty(), "slot {slot}");
    }
}

/// `mark_written` refuses a freed page and a kind the key does not admit
/// (§4.2, §3.1), leaving the flags as they were.
#[test]
fn mark_written_more() {
    let (_file, _map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), L7, Kind::Leaf, 1));
    // A MapLeaf's kind byte under an LPID, sealed: §3.3 passes, the kind not.
    c[4] = 6;
    let crc = page::seal(&mut c).unwrap();
    let e = error!(c.mark_written(Ref {
        slot: 20,
        epoch: 1,
        crc
    }));
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    assert!(c.is_dirty());
    c[4] = 3;
    let crc = page::seal(&mut c).unwrap();
    c.mark_freed().unwrap();
    let e = error!(c.mark_written(Ref {
        slot: 20,
        epoch: 1,
        crc
    }));
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    assert!(c.is_freed());
}

/// A load resets the flags a frame's last use left (P9): a reservation that
/// `create` initialised but never published leaves its frame dirty, and a
/// freed page's frame leaves the table freed; the next load into either is
/// clean and not freed, in the reference's epoch.
#[test]
fn load_resets_flags() {
    let (file, map, pool) = fixture(2).unwrap();
    // The page at slot 10 is in epoch 7.
    map.set(L1, put(&file, 10, Kind::Leaf, 1, 7, 0x11).unwrap());
    put_leaf(&file, &map, 2, 11, 0x22).unwrap();
    let live = created!(pool.create(pool.reserve().unwrap(), L4, Kind::Leaf, 1)).release();
    let res = pool.reserve().unwrap();
    let spare = res.frame();
    let (pin, res) = match pool.create(res, L4, Kind::Leaf, 3) {
        Ok(CreateOutcome::Exists(p, r)) => (p, r),
        other => panic!("expected Exists, got {other:?}"),
    };
    drop(res);
    drop(pin);
    // The spare frame, initialised dirty by `create`, is next off the list.
    let p = pinned!(pool.pin(L1, &map));
    assert_eq!(p.frame(), spare);
    {
        let g = p.latch_shared().unwrap();
        assert!(!g.is_dirty());
        assert!(!g.is_freed());
        assert_eq!(g.content_epoch(), 7);
    }
    drop(p);
    // A freed page's frame leaves the table and is reused by a load.
    let f = live.frame();
    live.latch_exclusive().unwrap().mark_freed().unwrap();
    drop(live);
    assert_eq!(drop_clean(&pool, f).unwrap(), Evicted::Removed);
    let q = pinned!(pool.pin(L2, &map));
    assert_eq!(q.frame(), f);
    let g = q.latch_shared().unwrap();
    assert!(!g.is_freed());
    assert!(!g.is_dirty());
    assert_eq!(g.content_epoch(), 1);
    assert!(!pool.frame_info(f).unwrap().freed);
}

/// Every guard's accessors report its page, and `mark_dirty` sets the
/// content epoch.
#[test]
fn guard_accessors() {
    let (file, map, pool) = fixture(3).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let p = pinned!(pool.pin(L1, &map));
    let f = p.frame();
    {
        let g = p.latch_shared().unwrap();
        assert_eq!((g.key(), g.frame()), (L1, f));
    }
    {
        let mut g = p.latch_exclusive().unwrap();
        assert_eq!((g.key(), g.frame(), g.content_epoch()), (L1, f, 1));
        assert!(!g.is_dirty() && !g.is_freed());
        g.mark_dirty(3).unwrap();
        assert_eq!(g.content_epoch(), 3);
        assert!(g.is_dirty());
    }
    {
        let g = p.try_latch_exclusive().unwrap().unwrap();
        assert_eq!((g.key(), g.frame(), g.content_epoch()), (L1, f, 3));
        let s = p.try_latch_shared().unwrap();
        assert!(s.is_none());
    }
    drop(p);
    let v = pool.try_victim(f).unwrap().unwrap();
    assert_eq!((v.key(), v.frame(), v.content_epoch()), (L1, f, 3));
    assert!(v.is_dirty() && !v.is_freed());
    drop(v);
    let mut c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 4));
    assert_eq!((c.key(), c.content_epoch()), (L2, 4));
    assert!(c.is_dirty() && !c.is_freed());
    c.mark_dirty(5).unwrap();
    assert_eq!(c.content_epoch(), 5);
    c.mark_freed().unwrap();
    assert!(c.is_freed() && !c.is_dirty());
    assert!(matches!(c.mark_dirty(6), Err(StoreError::Invalid(_))));
    let pin: Pin<'_, FaultFile> = c.release();
    assert_eq!(pin.key(), L2);
}

/// Frame ids past the pool, another pool's reservation, and the test seam's
/// state check are refused.
#[test]
fn range_checks() {
    let (file, map, pool) = fixture(2).unwrap();
    let past = FrameId(2);
    let invalid = |e: StoreError| assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    invalid(error!(pool.pin_frame(past)));
    invalid(error!(pool.try_victim(past)));
    invalid(error!(pool.frame_info(past)));
    invalid(error!(pool.frame_info(FrameId(u32::MAX))));
    invalid(error!(pool.force_pins(FrameId(0), 1)));
    let (_file2, _map2, other) = fixture(2).unwrap();
    let theirs = other.reserve().unwrap();
    let free = other.stats().unwrap().free;
    invalid(error!(pool.create(theirs, L2, Kind::Leaf, 1)));
    assert_eq!(other.stats().unwrap().free, free + 1);
    assert_eq!(pool.stats().unwrap().free, 2);
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let p = pinned!(pool.pin(L1, &map));
    assert!(pool.table_free());
    drop(p);
}

/// A chain as long as the pool: a walk reaches its tail, and the tail leaves
/// the table (the card's `table_chains` removes a middle and a head).
#[test]
fn table_chain_ends() {
    let (file, map, pool) = fixture(3).unwrap();
    let bucket = pool.bucket_of(L1);
    let mut keys = vec![L1];
    keys.extend(
        (2u64..)
            .map(PageKey::Lpid)
            .filter(|&k| pool.bucket_of(k) == bucket)
            .take(2),
    );
    for (i, &k) in keys.iter().enumerate() {
        let PageKey::Lpid(x) = k else { unreachable!() };
        put_leaf(&file, &map, x, 10 + i as u64, 0x51 + i as u8).unwrap();
    }
    let frames: Vec<FrameId> = keys
        .iter()
        .map(|&k| pinned!(pool.pin(k, &map)).frame())
        .collect();
    // From its head the chain is keys[2], keys[1], keys[0]: three links.
    for (&k, &f) in keys.iter().zip(&frames) {
        assert_eq!(pool.frame_of(k).unwrap(), Some(f));
    }
    assert_eq!(drop_clean(&pool, frames[0]).unwrap(), Evicted::Removed);
    assert_eq!(pool.frame_of(keys[0]).unwrap(), None);
    assert_eq!(pool.frame_of(keys[1]).unwrap(), Some(frames[1]));
    assert_eq!(pool.frame_of(keys[2]).unwrap(), Some(frames[2]));
    // Reloaded, keys[0] heads the chain, and keys[1] is its tail.
    let p = pinned!(pool.pin(keys[0], &map));
    assert_eq!(marker(&p).unwrap(), 0x51);
    drop(p);
    assert_eq!(drop_clean(&pool, frames[1]).unwrap(), Evicted::Removed);
    assert_eq!(pool.frame_of(keys[1]).unwrap(), None);
    assert_eq!(pool.frame_of(keys[2]).unwrap(), Some(frames[2]));
    assert!(pool.frame_of(keys[0]).unwrap().is_some());
}

/// An error from the `CurrentRef` ends the load before it is returned
/// (S-STORE §5.4 "The load": a load ends, serving nothing, before its thread
/// restarts): no frame is left in the table, none loading, none orphaned.
#[test]
fn current_ref_error_ends_load() {
    struct Failing;
    impl CurrentRef<FaultFile> for Failing {
        fn current_ref(&self, _pool: &Pool<FaultFile>, _key: PageKey) -> Result<Ref, StoreError> {
            Err(StoreError::Corrupt("the MapLeaf failed its check".into()))
        }
    }
    let (file, map, pool) = fixture(2).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let e = error!(pool.pin(L1, &Failing));
    assert!(
        matches!(&e, StoreError::Corrupt(m) if m == "the MapLeaf failed its check"),
        "{e:?}"
    );
    assert_eq!(pool.frame_of(L1).unwrap(), None);
    let s = pool.stats().unwrap();
    assert_eq!((s.free, s.loading, s.orphan, s.reads), (2, 0, 0, 0));
    assert_eq!(file.reads(), 0);
    assert_eq!(marker(&pinned!(pool.pin(L1, &map))).unwrap(), 0x11);
}

/// `stats().reads` counts every slot read a load issues, failed ones
/// included, and nothing else: it stays equal to the page file's read log.
#[test]
fn reads_counted() {
    let (file, map, pool) = fixture(2).unwrap();
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    // L2: a page that fails §3.3. L3: past the end. L5: a failing read.
    // L6: slot 1. L9: unmapped.
    let (mut b, crc) = page(Kind::Leaf, 2, 1, 0x22).unwrap();
    b[SPOIL] ^= 0xFF;
    file.write_at(offset(11), &b).unwrap();
    map.set(
        L2,
        Ref {
            slot: 11,
            epoch: 1,
            crc,
        },
    );
    put_leaf(&file, &map, 5, 12, 0x55).unwrap();
    file.fail_read_at(Some(offset(12)));
    map.set(
        PageKey::Lpid(3),
        Ref {
            slot: 90,
            epoch: 1,
            crc: 0,
        },
    );
    map.set(
        PageKey::Lpid(6),
        Ref {
            slot: 1,
            epoch: 1,
            crc: 0,
        },
    );
    let reads_are = |n: u64| {
        assert_eq!(pool.stats().unwrap().reads, n);
        assert_eq!(file.reads() as u64, n);
    };
    reads_are(0);
    drop(pinned!(pool.pin(L1, &map)));
    reads_are(1);
    drop(pinned!(pool.pin(L1, &map)));
    reads_are(1);
    assert!(matches!(pool.pin(L2, &map), Err(StoreError::Corrupt(_))));
    reads_are(2);
    assert!(matches!(
        pool.pin(PageKey::Lpid(3), &map),
        Err(StoreError::Corrupt(_))
    ));
    reads_are(3);
    assert!(matches!(
        pool.pin(PageKey::Lpid(5), &map),
        Err(StoreError::Env(_))
    ));
    reads_are(4);
    assert!(matches!(
        pool.pin(PageKey::Lpid(6), &map),
        Err(StoreError::Corrupt(_))
    ));
    reads_are(4);
    assert!(matches!(pool.pin(L9, &map), Ok(PinOutcome::Unmapped)));
    reads_are(4);
}

/// Every hook a single thread fires finds the table mutex free, and so does
/// every reference read (BUF-TABLE-LEAF: no hook fires and no `CurrentRef` is
/// called under it). Single-threaded, so no other thread can hold it.
#[test]
fn hooks_fire_unlocked() {
    let (file, map, pool) = fixture(3).unwrap();
    let pool = Arc::new(pool);
    put_leaf(&file, &map, 1, 10, 0x11).unwrap();
    let weak = Arc::downgrade(&pool);
    let fired = Arc::new(Mutex::new(Vec::<(HookPoint, Option<bool>)>::new()));
    let sink = Arc::clone(&fired);
    let hook: Hook = Arc::new(move |p, _k, _f| {
        let free = weak.upgrade().map(|pool| pool.table_free());
        sink.lock().unwrap().push((p, free));
    });
    pool.set_hook(Some(hook));
    let p = pinned!(pool.pin(L1, &map));
    drop(pool.pin_resident(L1).unwrap());
    assert!(matches!(pool.pin(L9, &map), Ok(PinOutcome::Unmapped)));
    let p2 = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1)).release();
    match pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1) {
        Ok(CreateOutcome::Exists(a, b)) => drop((a, b)),
        other => panic!("expected Exists, got {other:?}"),
    }
    p2.latch_exclusive().unwrap().mark_freed().unwrap();
    drop(created!(pool.create(
        pool.reserve().unwrap(),
        L2,
        Kind::Leaf,
        2
    )));
    drop(pool.pin_frame(p2.frame()).unwrap());
    drop(p2);
    drop(p);
    let v = pool.try_victim(FrameId(0)).unwrap().unwrap();
    let vp = v.pin_frame().unwrap();
    assert!(matches!(v.remove_from_table().unwrap(), Removal::Kept));
    drop(vp);
    let v = pool.try_victim(FrameId(0)).unwrap().unwrap();
    assert!(matches!(
        v.remove_from_table().unwrap(),
        Removal::Removed(_)
    ));
    pool.set_hook(None);
    let fired = fired.lock().unwrap().clone();
    use HookPoint::*;
    for point in [
        MissReserved,
        LoadPublished,
        LoadResolved,
        LoadRead,
        Pinned,
        Unpinned,
        CreateReady,
        RemoveLatched,
    ] {
        assert!(
            fired.iter().any(|&(p, _)| p == point),
            "{point:?} never fired"
        );
    }
    assert!(
        fired.iter().all(|&(_, free)| free == Some(true)),
        "{fired:?}"
    );
    assert_eq!(map.locked_lookups(), 0);
}

/// `create` resets the flags a frame's last page left: a freed page's frame,
/// dropped from the table, is made a live page that is not freed.
#[test]
fn create_resets_flags() {
    let (_file, _map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), L2, Kind::Leaf, 1));
    c.mark_freed().unwrap();
    let f = c.release().frame();
    assert_eq!(drop_clean(&pool, f).unwrap(), Evicted::Removed);
    // f tops the free list, its content still marked freed.
    let c = created!(pool.create(pool.reserve().unwrap(), L4, Kind::Leaf, 3));
    assert_eq!(c.frame(), f);
    assert!(!c.is_freed() && c.is_dirty());
    assert_eq!(c.content_epoch(), 3);
    assert_eq!(
        pool.frame_info(f).unwrap(),
        FrameInfo {
            state: FrameState::Resident,
            key: Some(L4),
            pins: 1,
            freed: false,
            // Card A-3b-0: the remade page's pin sets the clock's bit.
            referenced: true
        }
    );
}

/// `mark_written` takes the content epoch as the home's, not only the
/// header's (§5.2: the entry is set to `(new slot, content epoch, crc)`): a
/// page dirtied in epoch 2 whose header still says epoch 1 passes §3.3
/// against a home of epoch 1, and is refused all the same.
#[test]
fn mark_written_epoch() {
    let (_file, _map, pool) = fixture(2).unwrap();
    let mut c = created!(pool.create(pool.reserve().unwrap(), L7, Kind::Leaf, 1));
    c.mark_dirty(2).unwrap();
    assert_eq!(c.content_epoch(), 2);
    let crc = page::seal(&mut c).unwrap();
    let home = Ref {
        slot: 21,
        epoch: 1,
        crc,
    };
    assert!(page::verify(&c, &home, 7).is_ok(), "§3.3 alone would pass");
    let e = error!(c.mark_written(home));
    assert!(matches!(e, StoreError::Invalid(_)), "{e:?}");
    assert!(c.is_dirty());
    c[16..20].copy_from_slice(&2u32.to_le_bytes());
    let crc = page::seal(&mut c).unwrap();
    c.mark_written(Ref {
        slot: 21,
        epoch: 2,
        crc,
    })
    .unwrap();
    assert!(!c.is_dirty());
}
