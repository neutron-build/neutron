//! Card A-3b-0's tests of the frame acquisition (Work 12; S-STORE §5.2
//! "Victim choice", §5.3's pool-floor sentences, §5.4 "Below the minimum";
//! R-1-memory §4.1): the clock's order and what it passes, a freed page's
//! frame leaving with no write (§4.2), the pool that never grows (§15
//! I-POOL-BOUND), a pin's miss, the decision when every unpinned frame is
//! dirty, the form that never waits, and the races of a sweep that ends
//! while another thread holds a latch, a pin or a reservation.
//!
//! Fixtures are at `PS` = 4096, epoch 1. `L1`..`L8` are `Lpid(1..=8)`, Leaf
//! pages at slots 10..17 with markers `0x11..0x18`, mapped through the test
//! map and written with `put`. "Load `L`" means `pin(L)`, then drop the pin.
//! The frames a fresh fixture hands out are 0, 1, 2, ... (card A-3a's free
//! stack).
//!
//! No test waits on time. A single-threaded test's hook fails it at an
//! `UnpinWait`, which no other thread could end; a test whose mutants could
//! make an acquisition retry without bound states a step budget, a count of
//! `SweepEnded` its hook keeps (S-STORE §15 "The step budget"). Every race
//! test follows card A-3a's `Pauser` and its release-then-assert rule: the
//! controller records what it sees, releases every paused thread, joins every
//! thread, and only then asserts; while a thread is paused it calls the pool
//! only in ways that cannot wait on what that thread holds.
//! `acquire_shared_hand` places its interleaving with its own permit hook
//! instead, on the same rule.

mod buf_support;

use std::any::Any;
use std::sync::Arc;
use std::sync::Condvar;
use std::sync::Mutex;
use std::sync::MutexGuard;
use std::sync::PoisonError;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::thread::{Scope, ScopedJoinHandle, ThreadId};

use buf_support::{
    Event, EventLog, FaultFile, MARK, NO_FRAME, NO_KEY, PS, Pauser, TestMap, fixture,
    fixture_at_minimum, marker, minimum_frames, put_leaf, spawn,
};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, FrameId, FrameInfo, FrameReservation, FrameState, Hook, HookPoint, PageKey, Pin,
    PinOutcome, Pool, PoolConfig, PoolStats,
};
use nucleus_store::page::Kind;

use HookPoint::{Evicted, PassEnded, RemoveLatched, SweepEnded, SweepStep, UnpinWait};

/// `Lpid(n)`.
const fn lpid(n: u64) -> PageKey {
    PageKey::Lpid(n)
}

/// `L1`, `L2` and `L3`.
const L1: PageKey = lpid(1);
const L2: PageKey = lpid(2);
const L3: PageKey = lpid(3);

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

/// A store error as text.
fn text(e: StoreError) -> String {
    e.to_string()
}

/// The number of the key an event names (`Lpid(n)` or `MapLeaf(n)`).
fn number(k: PageKey) -> u64 {
    match k {
        PageKey::Lpid(n) | PageKey::MapLeaf(n) => n,
    }
}

/// Write `Lpid(n)`'s Leaf page, marker `0x10 + n`, at slot `9 + n`.
macro_rules! put_l {
    ($file:expr, $map:expr, $n:expr) => {
        put_leaf($file, $map, $n, 9 + $n, (0x10 + $n) as u8).unwrap()
    };
}

/// Load `key`: pin it, then drop the pin. Its frame.
macro_rules! load {
    ($pool:expr, $map:expr, $key:expr) => {
        pinned!($pool.pin($key, $map)).frame()
    };
}

/// The `Evicted` events since the log held `from` entries, as the victim's
/// number and its frame.
fn evictions(log: &EventLog, from: usize) -> Vec<(u64, u32)> {
    log.since(from, Evicted)
        .into_iter()
        .map(|(k, f)| (number(k), f.0))
        .collect()
}

/// `frame_of` for `L1` to `L8`, as frame numbers.
fn frames_of(pool: &Pool<FaultFile>) -> Result<Vec<Option<u32>>, StoreError> {
    (1..=8u64)
        .map(|n| pool.frame_of(lpid(n)).map(|f| f.map(|f| f.0)))
        .collect()
}

/// `clock_order`'s check after a step: the step's `Evicted` events, the
/// clock's hand, the eviction count, and which frame holds each of `L1`..`L8`.
macro_rules! clock_step {
    ($pool:expr, $log:expr, $from:expr, $step:expr, $victims:expr, $clock:expr, $resident:expr $(,)?) => {{
        assert_eq!(
            evictions($log, $from),
            $victims,
            "step {}: the Evicted events",
            $step
        );
        let s = $pool.stats().unwrap();
        assert_eq!(
            (s.clock_hand, s.evictions),
            $clock,
            "step {}: the clock's hand and the eviction count",
            $step
        );
        assert_eq!(
            frames_of($pool).unwrap(),
            $resident.to_vec(),
            "step {}: the resident pages",
            $step
        );
    }};
}

/// Whether `e` is `Arrived(point, key, _)`.
fn arrived(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Arrived(p, k, _) if *p == point && *k == key)
}

/// Whether `e` is `Seen(point, key, _)`.
fn seen(e: &Event, point: HookPoint, key: PageKey) -> bool {
    matches!(e, Event::Seen(p, k, _) if *p == point && *k == key)
}

/// Every event still queued after a join.
fn reported(pz: &Pauser) -> Vec<Event> {
    pz.drain()
}

/// The events but the threads' own `Done`, which each sends as it ends.
fn without_done(events: Vec<Event>) -> Vec<Event> {
    events
        .into_iter()
        .filter(|e| !matches!(e, Event::Done(_)))
        .collect()
}

/// Assert that `events` starts with `want` and return the rest.
fn expect(events: &[Event], want: Event) -> &[Event] {
    assert_eq!(
        events.first(),
        Some(&want),
        "the events did not come in that order: expected {want:?} in {events:?}"
    );
    &events[1..]
}

/// Skip the leading `Seen(point, key)` events; returns the rest and how many
/// were skipped.
fn skip(events: &[Event], point: HookPoint, key: PageKey) -> (&[Event], usize) {
    let n = events.iter().take_while(|e| seen(e, point, key)).count();
    (&events[n..], n)
}

/// The `Pauser`'s hook behind a step budget (S-STORE §15 "The step budget";
/// card A-3b-0, Work 12, "Step budgets"): a thread that fires `SweepEnded`
/// more than `budget` times fails inside the hook, which holds no lock, so a
/// mutant that retries without bound fails the test instead of hanging it.
/// With `after`, only firings once that flag is set are counted.
fn budgeted(pz: &Pauser, budget: usize, after: Option<Arc<AtomicBool>>) -> Hook {
    budgeted_wait(pz, budget, after, false)
}

/// [`budgeted`], and with `no_wait` set a hook that also fails the test at
/// an `UnpinWait`, which no thread of that test may reach: where one did, the
/// thread would hold a real waiter whose wake-up no other thread of the test
/// sends (card A-3b-0, Work 12, "Step budgets"; gate ruling A-3b-0 R2).
fn budgeted_wait(
    pz: &Pauser,
    budget: usize,
    after: Option<Arc<AtomicBool>>,
    no_wait: bool,
) -> Hook {
    let count = Arc::new(AtomicUsize::new(0));
    let pauser = pz.hook();
    Arc::new(move |point, key, frame| {
        assert!(
            !no_wait || point != HookPoint::UnpinWait,
            "this test's acquisition waited for an unpin"
        );
        if point == SweepEnded && after.as_ref().is_none_or(|f| f.load(Ordering::SeqCst)) {
            let n = count.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(
                n <= budget,
                "{n} sweeps ended, past the step budget of {budget}"
            );
        }
        pauser(point, key, frame);
    })
}

/// The clock (S-STORE §5.2; card A-3b-0, Work 3 and Work 5, A2): the hand
/// clears every bit once, then takes the first frame whose bit is clear; a
/// hit or a pin sets the bit again; a pinned frame is passed with its bit
/// untouched. Nine steps on four frames, each checked on its `Evicted`
/// events, the hand, the eviction count and the resident set. Mutants
/// CLOCK-NO-SECOND-CHANCE and REF-NOT-SET-ON-HIT fail step 4,
/// REF-NOT-SET-ON-LOAD step 9, SCAN-CLEARS-PINNED step 7, HAND-STAYS step 2
/// and SWEEP-ONE-ROTATION step 2.
#[test]
fn clock_order() {
    let (file, map, pool) = fixture(4).unwrap();
    for n in 1..=8 {
        put_l!(&file, &map, n);
    }
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));
    let bit = |n: u32| pool.frame_info(FrameId(n)).unwrap().referenced;
    let mut from;

    // 1. Load L1, L2, L3, L4: no eviction; frames 0-3; hand 0.
    from = log.len();
    for n in 1..=4u64 {
        assert_eq!(load!(&pool, &map, lpid(n)), FrameId(n as u32 - 1));
    }
    clock_step!(
        &pool,
        &log,
        from,
        1,
        &[],
        (0, 0),
        [Some(0), Some(1), Some(2), Some(3), None, None, None, None],
    );
    assert!(
        bit(0) && bit(1) && bit(2) && bit(3),
        "each load sets the bit"
    );

    // 2. Load L5: evicts L1 from frame 0, after the hand has cleared every
    //    bit once; hand 1.
    from = log.len();
    assert_eq!(load!(&pool, &map, lpid(5)), FrameId(0));
    clock_step!(
        &pool,
        &log,
        from,
        2,
        &[(1, 0)],
        (1, 1),
        [None, Some(1), Some(2), Some(3), Some(0), None, None, None],
    );

    // 3. Load L2 (a hit, which sets frame 1's bit): no read, no eviction.
    from = log.len();
    let reads = file.reads();
    let p = pinned!(pool.pin(L2, &map));
    assert_eq!(p.frame(), FrameId(1));
    drop(p);
    clock_step!(
        &pool,
        &log,
        from,
        3,
        &[],
        (1, 1),
        [None, Some(1), Some(2), Some(3), Some(0), None, None, None],
    );
    assert_eq!(file.reads(), reads, "step 3: a hit reads nothing");
    assert!(bit(1), "step 3: the hit sets frame 1's bit");

    // 4. Load L6: frame 1's bit is cleared; evicts L3 from frame 2; hand 3.
    from = log.len();
    assert_eq!(load!(&pool, &map, lpid(6)), FrameId(2));
    clock_step!(
        &pool,
        &log,
        from,
        4,
        &[(3, 2)],
        (3, 2),
        [None, Some(1), None, Some(3), Some(0), Some(2), None, None],
    );
    assert!(!bit(1), "step 4: frame 1's bit is cleared");

    // 5. Load L7: evicts L4 from frame 3; hand 0.
    from = log.len();
    assert_eq!(load!(&pool, &map, lpid(7)), FrameId(3));
    clock_step!(
        &pool,
        &log,
        from,
        5,
        &[(4, 3)],
        (0, 3),
        [None, Some(1), None, None, Some(0), Some(2), Some(3), None],
    );

    // 6. Pin L2 and keep it; load L8: frame 1 is skipped, pinned, with its
    //    bit untouched; evicts L5 from frame 0; hand 1. Then drop the L2 pin.
    let p = pinned!(pool.pin(L2, &map));
    assert_eq!(p.frame(), FrameId(1));
    from = log.len();
    assert_eq!(load!(&pool, &map, lpid(8)), FrameId(0));
    clock_step!(
        &pool,
        &log,
        from,
        6,
        &[(5, 0)],
        (1, 4),
        [None, Some(1), None, None, None, Some(2), Some(3), Some(0)],
    );
    assert!(bit(1), "step 6: a pinned frame's bit is untouched");
    drop(p);

    // 7. Load L1: frame 1's bit (set by step 6's pin) is cleared; evicts L6
    //    from frame 2; hand 3.
    from = log.len();
    assert_eq!(load!(&pool, &map, L1), FrameId(2));
    clock_step!(
        &pool,
        &log,
        from,
        7,
        &[(6, 2)],
        (3, 5),
        [Some(2), Some(1), None, None, None, None, Some(3), Some(0)],
    );
    assert!(!bit(1), "step 7: frame 1's bit is cleared");

    // 8. Load L3: evicts L7 from frame 3; hand 0.
    from = log.len();
    assert_eq!(load!(&pool, &map, L3), FrameId(3));
    clock_step!(
        &pool,
        &log,
        from,
        8,
        &[(7, 3)],
        (0, 6),
        [Some(2), Some(1), Some(3), None, None, None, None, Some(0)],
    );

    // 9. Load L4: frame 0's bit (set by L8's load) is cleared; evicts L2
    //    from frame 1; hand 2.
    from = log.len();
    assert_eq!(load!(&pool, &map, lpid(4)), FrameId(1));
    clock_step!(
        &pool,
        &log,
        from,
        9,
        &[(2, 1)],
        (2, 7),
        [Some(2), None, Some(3), Some(1), None, None, None, Some(0)],
    );
    assert!(!bit(0), "step 9: frame 0's bit is cleared");

    // L8, L4, L1, L3 in frames 0-3, each holding its own page.
    for (n, frame) in [(8u64, 0u32), (4, 1), (1, 2), (3, 3)] {
        let p = pinned!(pool.pin(lpid(n), &map));
        assert_eq!(p.frame().0, frame, "L{n}");
        assert_eq!(p.latch_shared().unwrap()[MARK], (0x10 + n) as u8, "L{n}");
        assert_eq!(marker(&p).unwrap(), (0x10 + n) as u8, "L{n}");
        drop(p);
    }
    let s = pool.stats().unwrap();
    assert_eq!((s.frames, s.free, s.evictions), (4, 0, 7));
    // Eleven loads, one slot read each, all of slots 10..17.
    assert_eq!(file.read_log().len(), 11);
    assert_eq!(file.read_log().len() as u64, s.reads);
    assert!(
        file.read_log()
            .iter()
            .all(|&(off, _)| (10..18).contains(&(off / 4096))),
        "{:?}",
        file.read_log()
    );
    assert_eq!(file.writes(), 8, "only the test's eight pages were written");
}

/// The clock never blocks on a victim (S-STORE §5.2: "if the latch is not
/// free, it moves on. Pinned frames are skipped") and passes a dirty one
/// (card A-3b-0, Work 5, A2): with one frame pinned, one dirty and never
/// written, one latched by the test and one clean, a pin's miss takes the
/// clean one, and the file sees no write.
#[test]
fn clock_skips() {
    let (file, map, pool) = fixture(4).unwrap();
    for n in [1u64, 3, 4, 5] {
        put_l!(&file, &map, n);
    }
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));

    // Setup, in this order: frame 0 pinned, frame 1 dirty and never
    // written, frame 2 latched by the test, frame 3 clean.
    let p1 = pinned!(pool.pin(lpid(1), &map));
    assert_eq!(p1.frame(), FrameId(0));
    let c = created!(pool.create(pool.reserve().unwrap(), lpid(20), Kind::Leaf, 1));
    assert_eq!(c.frame(), FrameId(1));
    assert!(c.is_dirty() && !c.is_freed());
    drop(c.release());
    assert_eq!(load!(&pool, &map, lpid(3)), FrameId(2));
    let v = pool
        .try_victim(FrameId(2))
        .unwrap()
        .expect("L3 is unpinned");
    assert_eq!(v.frame(), FrameId(2));
    assert_eq!(load!(&pool, &map, lpid(4)), FrameId(3));
    let writes = file.writes();

    // Pin L5: the only `Evicted` is L4 from frame 3.
    let from = log.len();
    let p5 = pinned!(pool.pin(lpid(5), &map));
    assert_eq!(p5.frame(), FrameId(3));
    assert_eq!(evictions(&log, from), vec![(4, 3)]);
    for (frame, key, pins) in [(0u32, 1u64, 1u64), (1, 20, 0), (2, 3, 0)] {
        let info = pool.frame_info(FrameId(frame)).unwrap();
        assert_eq!(
            (info.state, info.key, info.pins),
            (FrameState::Resident, Some(lpid(key)), pins),
            "frame {frame}"
        );
    }
    assert_eq!(pool.stats().unwrap().clock_hand, 0);
    assert_eq!(pool.stats().unwrap().evictions, 1);
    assert_eq!(file.writes(), writes, "no page was written");
    drop(p5);
    drop(v);
    drop(p1);
}

/// A freed page's frame leaves the page table as a clean victim, with no
/// write (S-STORE §4.2 "Freeing"; card A-3b-0, Work 5, A2). A dirty frame is
/// passed, and with nothing else to take `try_reserve` answers `None` and
/// the dirty page stays in the table (BUF-LEAVE-CLEAN). Mutant
/// EVICT-FREED-WRITES fails it.
#[test]
fn clock_evicts_freed_unwritten() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2, 3] {
        put_l!(&file, &map, n);
    }
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));
    assert_eq!(file.writes(), 3);

    // Frame 0: freed, clean. Frame 1: L1.
    let mut c = created!(pool.create(pool.reserve().unwrap(), lpid(30), Kind::Leaf, 1));
    assert_eq!(c.frame(), FrameId(0));
    c.mark_freed().unwrap();
    assert!(c.is_freed() && !c.is_dirty());
    drop(c.release());
    assert_eq!(load!(&pool, &map, L1), FrameId(1));

    // Pin L2 and keep the pin: the one `Evicted` is Lpid(30) from frame 0,
    // and nothing was written (§4.2: a freed frame leaves "as a clean
    // victim, with no write").
    let from = log.len();
    let p2 = pinned!(pool.pin(L2, &map));
    assert_eq!(p2.frame(), FrameId(0));
    assert_eq!(evictions(&log, from), vec![(30, 0)]);
    assert_eq!(file.writes(), 3);
    drop(p2);

    // Create Lpid(31): its `reserve` evicts L1 from frame 1. Frame 1 is
    // then dirty and never written.
    let from = log.len();
    let c = created!(pool.create(pool.reserve().unwrap(), lpid(31), Kind::Leaf, 1));
    assert_eq!(c.frame(), FrameId(1));
    assert!(c.is_dirty() && !c.is_freed());
    drop(c.release());
    assert_eq!(evictions(&log, from), vec![(1, 1)]);

    // Pin L3 and keep the pin: evicts L2 from frame 0; Lpid(31) is passed
    // over as dirty.
    let from = log.len();
    let p3 = pinned!(pool.pin(L3, &map));
    assert_eq!(p3.frame(), FrameId(0));
    assert_eq!(evictions(&log, from), vec![(2, 0)]);
    assert_eq!(pool.frame_of(lpid(31)).unwrap(), Some(FrameId(1)));

    // Nothing is left to take: `None` after one sweep, and the dirty page
    // stays in the table.
    let from = log.len();
    assert!(matches!(pool.try_reserve(), Ok(None)));
    assert_eq!(evictions(&log, from), vec![]);
    assert_eq!(log.since(from, SweepEnded).len(), 1);
    let info = pool.frame_info(FrameId(1)).unwrap();
    assert_eq!(
        (info.state, info.key, info.pins),
        (FrameState::Resident, Some(lpid(31)), 0)
    );
    assert_eq!(pool.frame_of(lpid(31)).unwrap(), Some(FrameId(1)));
    let s = pool.stats().unwrap();
    assert_eq!((s.evictions, s.unpin_waiters), (3, 0));
    drop(p3);
    // The whole test: only the test's three pages were written.
    assert_eq!(file.writes(), 3);
}

/// I-POOL-BOUND at A-3b-0 (S-STORE §5.1 "It never grows"; §15.1's row,
/// full): a pool at exactly the minimum, 34 frames by the test's own
/// formula, takes four times its frames of clean pages, each pin dropped at
/// once, and never holds more than `F`, in frames or in bytes: after every
/// pin its states sum to `F` and its footprint is still `bytes_for`'s. Each
/// load past the first `F` evicts first. Mutant POOL-GROW fails the
/// footprint equality.
#[test]
fn pool_never_grows() {
    let callers = 1u32;
    let f = minimum_frames(callers);
    assert_eq!(f, 34);
    let cfg = PoolConfig {
        frames: f,
        page_size: PS,
        callers,
    };
    let (file, map, pool) = fixture_at_minimum(callers, PS).unwrap();
    assert_eq!(pool.frames(), f);
    let footprint = Pool::<FaultFile>::bytes_for(cfg).unwrap();
    assert_eq!(pool.footprint(), footprint);
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));
    // 4F clean pages, Lpid(1..=136) at slots 10..145.
    let pages = u64::from(4 * f);
    assert_eq!(pages, 136);
    assert_eq!(9 + pages, 145);
    for n in 1..=pages {
        put_l!(&file, &map, n);
    }
    assert_eq!(pool.stats().unwrap().evictions, 0);
    for round in 0..4 {
        for n in 1..=pages {
            let p = pinned!(pool.pin(lpid(n), &map));
            drop(p);
            let s = pool.stats().unwrap();
            let at = format!("round {round}, L{n}");
            let states = s.free + s.reserved + s.loading + s.resident + s.orphan;
            assert_eq!(s.frames, f, "{at}");
            assert_eq!(states, f, "{at}: {s:?}");
            assert!(s.resident <= f, "{at}: {s:?}");
            assert_eq!(pool.footprint(), footprint, "{at}");
            assert_eq!(s.reads, file.reads() as u64, "{at}");
            // The first F loads fill the pool; every later one evicts first.
            assert_eq!(s.evictions, s.reads.saturating_sub(u64::from(f)), "{at}");
        }
    }
    let s = pool.stats().unwrap();
    assert_eq!(
        (s.reads, s.evictions),
        (4 * pages, 4 * pages - u64::from(f))
    );
    assert_eq!(file.writes() as u64, pages, "the pool wrote nothing");
    assert_eq!(pool.sizing().frames, f);
}

/// A pin's miss takes its frame from the clock (card A-3b-0, Work 5, P3): a
/// pin of an unmapped LPID on a full pool evicts one page, then, the
/// reference's slot being 0, gives the frame back to the free list (U3), so
/// the next miss needs no eviction. Mutant P3-FREE-LIST-ONLY fails it.
#[test]
fn pin_miss_evicts() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2, 3] {
        put_l!(&file, &map, n);
    }
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_no_wait()));
    assert_eq!(load!(&pool, &map, L1), FrameId(0));
    assert_eq!(load!(&pool, &map, L2), FrameId(1));

    // Lpid(9) is unmapped: its frame is evicted for, then returned.
    let from = log.len();
    let out = pool.pin(lpid(9), &map).unwrap();
    assert!(matches!(out, PinOutcome::Unmapped), "{out:?}");
    assert_eq!(evictions(&log, from), vec![(1, 0)]);
    assert_eq!(pool.frame_of(lpid(9)).unwrap(), None);
    assert_eq!(pool.frame_of(L1).unwrap(), None);
    let s = pool.stats().unwrap();
    assert_eq!(
        (s.free, s.reserved, s.loading, s.orphan, s.evictions),
        (1, 0, 0, 0, 1)
    );

    // L3 then comes off the free list: Pinned in frame 0, with no further
    // eviction and no sweep.
    let from = log.len();
    let p = pinned!(pool.pin(L3, &map));
    assert_eq!(p.frame(), FrameId(0));
    assert_eq!(evictions(&log, from), vec![]);
    assert_eq!(log.since(from, SweepEnded).len(), 0);
    assert_eq!(pool.stats().unwrap().evictions, 1);
    drop(p);
}

/// With every unpinned frame dirty the acquisition answers `PoolStarved`
/// (card A-3b-0, Work 5, A6): this card writes no victim, and A-3c's dirty
/// eviction replaces the answer. Step budget: 2 `SweepEnded` in the whole
/// test, one per acquisition. Mutant DIRTY-NOT-SKIPPED makes a third;
/// BUSY-AS-HELD makes the one thread wait for an unpin, which the hook
/// refuses.
#[test]
fn pool_starved_all_dirty() {
    let (_file, _map, pool) = fixture(2).unwrap();
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_sweep_budget(2)));
    for n in [1u64, 2] {
        let c = created!(pool.create(pool.reserve().unwrap(), lpid(n), Kind::Leaf, 1));
        drop(c.release());
    }
    assert_eq!(pool.stats().unwrap().free, 0);

    // `reserve()`: one sweep, then `PoolStarved`.
    let got = pool.reserve().map_err(text);
    assert_eq!(
        got.err().as_deref(),
        Some("buffer pool starved: no unpinned frame after two clock sweeps")
    );
    assert_eq!(log.since(0, SweepEnded).len(), 1);
    // `try_reserve()`: one more, then `None`.
    assert!(matches!(pool.try_reserve(), Ok(None)));
    assert_eq!(log.since(0, SweepEnded).len(), 2);
    // Both pages are still resident.
    for n in [1u64, 2] {
        let info = pool.frame_info(FrameId(n as u32 - 1)).unwrap();
        assert_eq!(
            (info.state, info.key, info.pins),
            (FrameState::Resident, Some(lpid(n)), 0)
        );
    }
    let s = pool.stats().unwrap();
    assert_eq!((s.unpin_waiters, s.evictions), (0, 0));
}

/// `try_reserve` never waits (card A-3b-0, Work 5, A4; S-STORE §5.3): with
/// every frame held it answers `None` after one sweep. `UnpinWait` must not
/// fire. Mutant TRY-WAITS fails it there.
#[test]
fn try_reserve_never_waits() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2] {
        put_l!(&file, &map, n);
    }
    let log = EventLog::new();
    pool.set_hook(Some(log.hook_sweep_budget(1)));
    let p1 = pinned!(pool.pin(L1, &map));
    let p2 = pinned!(pool.pin(L2, &map));
    assert!(matches!(pool.try_reserve(), Ok(None)));
    assert_eq!(log.since(0, SweepEnded).len(), 1);
    assert_eq!(pool.stats().unwrap().unpin_waiters, 0);
    drop((p1, p2));
}

/// A sweep that finds a frame latched passes it and goes on (S-STORE §5.2;
/// card A-3b-0, Work 5, A2: "`None` ... `busy`; next step"), and a pin that
/// arrives at R1 makes the removal `Kept`, which also does not end it. T's
/// sweep, held at X's latch, goes on to evict Y. Mutant KEPT-ENDS-SWEEP ends
/// it there and answers `None`.
#[test]
fn acquire_kept_continues() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2] {
        put_l!(&file, &map, n);
    }
    let (x, y) = (L1, L2);
    let (fx, fy) = (load!(&pool, &map, x), load!(&pool, &map, y));
    assert_eq!((fx, fy), (FrameId(0), FrameId(1)));
    let pz = Pauser::new();
    // The step budget: T's acquisition makes one sweep.
    pool.set_hook(Some(budgeted(&pz, 2, None)));
    pz.pause_at(RemoveLatched, x);
    pz.report(Evicted, x);
    pz.report(Evicted, y);
    let (first, q, got) = thread::scope(|s| {
        let t = spawn(s, &pz, "T", || pool.try_reserve().map_err(text));
        // T holds frame 0's exclusive latch; no pin; no mutex.
        let first = pz.next_event();
        let mut q = None;
        if arrived(&first, RemoveLatched, x) {
            // A table-only call: X is still in the table under the latch.
            q = Some(pool.pin_resident(x).map_err(text));
        }
        pz.release_all();
        (first, q, t.join())
    });
    assert_eq!(first, Event::Arrived(RemoveLatched, x, fx));
    // The controller's pin holds X's frame from R1 on.
    let q = q
        .expect("T reached RemoveLatched")
        .unwrap_or_else(|e| panic!("pin_resident: {e}"))
        .expect("X is resident under its victim's latch");
    let reservation = got
        .unwrap_or_else(|e| panic!("T: {e:?}"))
        .unwrap_or_else(|e| panic!("T: {e}"))
        .expect("T's sweep went on past the kept frame");
    assert_eq!(reservation.frame(), fy, "T got frame 1");
    assert_eq!(pool.frame_of(x).unwrap(), Some(fx));
    assert_eq!(
        pool.frame_info(fx).unwrap(),
        FrameInfo {
            state: FrameState::Resident,
            key: Some(x),
            pins: 1,
            freed: false,
            referenced: true
        },
        "X is resident with count 1"
    );
    assert_eq!(pool.stats().unwrap().evictions, 1);
    assert_eq!(
        without_done(reported(&pz)),
        vec![Event::Seen(Evicted, y, fy)],
        "the only Evicted is Y"
    );
    drop(q);
    drop(reservation);
    assert_eq!(pool.frame_of(y).unwrap(), None);
    assert_eq!(pool.frame_of(x).unwrap(), Some(fx));
}

/// How many sweeps T2 of `acquire_vs_acquire` may end after T1 has ended
/// (its step budget): a correct build needs none.
const LATE_SWEEPS: usize = 8;

/// Two acquisitions at once (card A-3b-0, Work 5, A2 and A5–A7; §5.3 "The
/// fairness bound"): the one that finds a latched frame is busy and retries
/// without blocking on the latch; once that frame is taken and the other is
/// pinned, it waits for an unpin, and the unpin serves it. Mutant
/// BUSY-AS-HELD fails the order of events: it makes `UnpinWait` come before
/// T1's release.
#[test]
fn acquire_vs_acquire() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2] {
        put_l!(&file, &map, n);
    }
    let (x, y) = (L1, L2);
    let fx = load!(&pool, &map, x);
    let p = pinned!(pool.pin(y, &map));
    let fy = p.frame();
    assert_eq!((fx, fy), (FrameId(0), FrameId(1)));
    let pz = Pauser::new();
    let t1_ended = Arc::new(AtomicBool::new(false));
    pool.set_hook(Some(budgeted(
        &pz,
        LATE_SWEEPS,
        Some(Arc::clone(&t1_ended)),
    )));
    pz.pause_at(RemoveLatched, x);
    pz.pause_at(SweepEnded, NO_KEY);
    for point in [SweepEnded, UnpinWait] {
        pz.report(point, NO_KEY);
    }
    pz.report(Evicted, x);
    pz.report(Evicted, y);
    let (events, r1, r2) = thread::scope(|s| {
        let mut events = Vec::new();
        let t1 = spawn(s, &pz, "T1", || pool.try_reserve().map_err(text));
        // T1 holds frame 0's exclusive latch; no pin; no mutex.
        events.push(pz.next_event());
        let mut t2 = None;
        if arrived(&events[0], RemoveLatched, x) {
            t2 = Some(spawn(s, &pz, "T2", || pool.reserve().map_err(text)));
            // T2 holds nothing; its sweep found frame 0 busy.
            events.push(pz.next_event());
            if arrived(&events[1], SweepEnded, NO_KEY) {
                pz.release(SweepEnded, NO_KEY);
                // T2's next event: the end of a further sweep, so it
                // retried. T2 holds nothing at it.
                events.push(pz.next_event());
                if seen(&events[2], SweepEnded, NO_KEY) {
                    // Hold T2 at its next sweep's end until T1 has ended, so
                    // that T1's end comes before T2's wait whatever the
                    // scheduling: the two are not ordered by the pool.
                    pz.pause_at(SweepEnded, NO_KEY);
                    while !events
                        .last()
                        .is_some_and(|e| arrived(e, SweepEnded, NO_KEY))
                    {
                        let e = pz.next_event();
                        let go_on = seen(&e, SweepEnded, NO_KEY);
                        events.push(e);
                        if !go_on {
                            break;
                        }
                    }
                    if arrived(events.last().unwrap(), SweepEnded, NO_KEY) {
                        pz.release(RemoveLatched, x);
                        while !events.last().is_some_and(|e| *e == Event::Done("T1")) {
                            let e = pz.next_event();
                            events.push(e);
                        }
                        t1_ended.store(true, Ordering::SeqCst);
                        // T2 goes on to wait for an unpin, past any retry;
                        // the controller lets it get there before the unpin.
                        pz.release(SweepEnded, NO_KEY);
                        loop {
                            // Past T2's retries, to its wait: anything else
                            // (T2's end, an event nothing should send) ends
                            // the wait.
                            let e = pz.next_event();
                            events.push(e);
                            if !matches!(
                                events.last(),
                                Some(Event::Seen(SweepEnded, NO_KEY, NO_FRAME))
                            ) {
                                break;
                            }
                        }
                    }
                }
            }
        }
        // Let every paused thread go, and unpin Y whatever came.
        pz.release_all();
        drop(p);
        (events, t1.join(), t2.map(|h| h.join()))
    });
    // What came after the controller's last wait.
    let mut events = events;
    events.extend(reported(&pz));

    // The events came in that order.
    let rest = expect(&events, Event::Arrived(RemoveLatched, x, fx));
    let rest = expect(rest, Event::Arrived(SweepEnded, NO_KEY, NO_FRAME));
    let (rest, retried) = skip(rest, SweepEnded, NO_KEY);
    assert!(retried >= 1, "T2 retried: {events:?}");
    // T2 held at a sweep's end.
    let rest = expect(rest, Event::Arrived(SweepEnded, NO_KEY, NO_FRAME));
    let rest = expect(rest, Event::Seen(Evicted, x, fx));
    let rest = expect(rest, Event::Done("T1"));
    let (rest, late) = skip(rest, SweepEnded, NO_KEY);
    assert!(late <= LATE_SWEEPS, "{events:?}");
    let rest = expect(rest, Event::Seen(UnpinWait, NO_KEY, NO_FRAME));
    let rest = expect(rest, Event::Seen(Evicted, y, fy));
    let rest = expect(rest, Event::Done("T2"));
    assert!(rest.is_empty(), "{events:?}");

    let res1 = r1
        .unwrap_or_else(|e| panic!("T1: {e:?}"))
        .unwrap_or_else(|e| panic!("T1: {e}"))
        .expect("T1 got a frame");
    assert_eq!(res1.frame(), fx, "T1 got frame 0");
    let res2 = r2
        .expect("T2 was spawned")
        .unwrap_or_else(|e| panic!("T2: {e:?}"))
        .unwrap_or_else(|e| panic!("T2: {e}"));
    assert_eq!(res2.frame(), fy, "T2 got frame 1");
    drop((res1, res2));
    let s = pool.stats().unwrap();
    assert_eq!((s.evictions, s.unpin_waiters, s.free), (2, 0, 2));
}

/// The end of a sweep rechecks the generation (card A-3b-0, Work 5, A5): a
/// frame unpinned while a sweep ran is taken by the next one without waiting
/// or answering starved. Step budget: 2 sweeps.
#[test]
fn sweep_end_rechecks_generation() {
    let (file, map, pool) = fixture(2).unwrap();
    put_leaf(&file, &map, 2, 11, 0x22).unwrap();
    let y = L2;
    // Frame 0: dirty, unpinned. Frame 1: Y, pinned by the controller.
    let c = created!(pool.create(pool.reserve().unwrap(), lpid(1), Kind::Leaf, 1));
    assert_eq!(c.frame(), FrameId(0));
    drop(c.release());
    let p = pinned!(pool.pin(y, &map));
    let fy = p.frame();
    assert_eq!(fy, FrameId(1));
    let pz = Pauser::new();
    pool.set_hook(Some(budgeted(&pz, 4, None)));
    pz.pause_at(SweepEnded, NO_KEY);
    pz.report(Evicted, y);
    let (first, got) = thread::scope(|s| {
        let t = spawn(s, &pz, "T", || pool.reserve().map_err(text));
        // T holds nothing; its sweep found frame 0 dirty and frame 1 pinned.
        let first = pz.next_event();
        if arrived(&first, SweepEnded, NO_KEY) {
            drop(p); // U1: the pin's own drop, no pool call.
            pz.release(SweepEnded, NO_KEY);
        }
        (first, t.join())
    });
    assert_eq!(first, Event::Arrived(SweepEnded, NO_KEY, NO_FRAME));
    let got = got
        .unwrap_or_else(|e| panic!("T: {e:?}"))
        .unwrap_or_else(|e| panic!("T: {e}"));
    assert_eq!(got.frame(), fy, "T got frame 1");
    assert_eq!(
        without_done(reported(&pz)),
        vec![Event::Seen(Evicted, y, fy)],
        "the one Evicted is Y"
    );
    assert_eq!(pool.frame_of(y).unwrap(), None);
    let s = pool.stats().unwrap();
    assert_eq!((s.evictions, s.unpin_waiters), (1, 0));
    drop(got);
}

/// The waiting form is served by an unpin (card A-3b-0, Work 5, A7; S-STORE
/// §5.4: "A thread that cannot pin a frame because every frame is pinned
/// waits for an unpin"): the waiter holds nothing, is counted in
/// `unpin_waiters`, and takes the frame the unpin freed. Step budget: 2
/// sweeps, one before the wait and one after. Mutant POOL-GROW serves T
/// with no wait.
#[test]
fn unpin_wait_then_served() {
    let (file, map, pool) = fixture(1).unwrap();
    put_l!(&file, &map, 1);
    let p = pinned!(pool.pin(L1, &map));
    let f0 = p.frame();
    assert_eq!(f0, FrameId(0));
    let pz = Pauser::new();
    pool.set_hook(Some(budgeted(&pz, 4, None)));
    pz.report(UnpinWait, NO_KEY);
    pz.report(Evicted, L1);
    let (first, waiters, got) = thread::scope(|s| {
        let t = spawn(s, &pz, "T", || pool.reserve().map_err(text));
        // T holds nothing: it is either counted in `unpin_waiters` or has
        // ended.
        let first = pz.next_event();
        let mut waiters = None;
        if seen(&first, UnpinWait, NO_KEY) {
            // A table-only read.
            waiters = Some(pool.stats().map(|s| s.unpin_waiters).map_err(text));
            drop(p);
        }
        pz.release_all();
        (first, waiters, t.join())
    });
    assert_eq!(
        first,
        Event::Seen(UnpinWait, NO_KEY, NO_FRAME),
        "UnpinWait came first"
    );
    assert_eq!(waiters, Some(Ok(1)), "the count recorded at the wait");
    let got = got
        .unwrap_or_else(|e| panic!("T: {e:?}"))
        .unwrap_or_else(|e| panic!("T: {e}"));
    assert_eq!(got.frame(), f0, "T got frame 0");
    assert_eq!(
        without_done(reported(&pz)),
        vec![Event::Seen(Evicted, L1, f0)]
    );
    drop(got);
    assert_eq!(pool.frame_of(L1).unwrap(), None);
    let s = pool.stats().unwrap();
    assert_eq!((s.unpin_waiters, s.evictions), (0, 1));
}

/// `acquire_shared_hand`'s step budget (card A-3b-0, Work 12; gate ruling
/// A-3b-0 R1): B's `SweepEnded` ≤ 4. A correct build ends two for T_B (the
/// scripted sweep and one more before its wait) and none for T_A; the hook
/// holds T_A to the same bound, so that a thread whose acquisition retries
/// without end fails inside it instead of hanging the test.
const SWEEP_BUDGET: usize = 4;

/// A thread of `acquire_shared_hand`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Who {
    /// T_A: the acquirer that takes the victim.
    A,
    /// T_B: the acquirer that shares T_A's clock hand.
    B,
}

impl Who {
    /// Its place among the two threads.
    fn at(self) -> usize {
        match self {
            Who::A => 0,
            Who::B => 1,
        }
    }
}

/// One firing of the pool's hook, as the controller of `acquire_shared_hand`
/// records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Fired {
    /// The thread that fired.
    who: Who,
    /// The point that fired.
    point: HookPoint,
    /// The key it fired with.
    key: PageKey,
    /// The frame it fired with.
    frame: FrameId,
}

/// What the controller of `acquire_shared_hand` saw come next.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Got {
    /// A thread fired the hook, and is held at that firing until permitted.
    At(Fired),
    /// A thread's closure ended, by its return or by a panic.
    Done(Who),
}

/// What `acquire_shared_hand`'s permit hook and its controller share.
struct Held {
    /// Each thread's id once it has entered, so the hook knows who fires.
    ids: [Option<ThreadId>; 2],
    /// Each thread's permits not yet taken; `None` lets it pass every firing.
    permits: [Option<usize>; 2],
    /// What came, in order: every firing and every end.
    log: Vec<Got>,
}

/// `acquire_shared_hand`'s own controller (card A-3b-0, Work 12; gate ruling
/// A-3b-0 R1), not the `Pauser`: each thread passes one firing of any point
/// per permit, and a firing from any other thread passes at once. Every wait
/// is on one condition variable: a thread's for a permit, and the
/// controller's for the next thing to come. The hook also keeps the step
/// budget, so that a thread whose acquisition retries without end fails
/// inside the hook, which holds no lock, instead of hanging the test.
#[derive(Clone)]
struct Permits(Arc<Gate>);

/// The state behind [`Permits`].
struct Gate {
    held: Mutex<Held>,
    wake: Condvar,
    /// Each thread's `SweepEnded` firings.
    sweeps: [AtomicUsize; 2],
}

/// Lock a test mutex, recovering from poison: a panicking test thread must
/// not turn a second lock into a second panic.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Permits {
    /// A controller with nothing permitted.
    fn new() -> Permits {
        Permits(Arc::new(Gate {
            held: Mutex::new(Held {
                ids: [None, None],
                permits: [Some(0), Some(0)],
                log: Vec::new(),
            }),
            wake: Condvar::new(),
            sweeps: [AtomicUsize::new(0), AtomicUsize::new(0)],
        }))
    }

    /// The hook to install: a thread's firing is recorded, and that thread is
    /// held at the firing until permitted. The step budget is asserted
    /// before any lock is taken.
    fn hook(&self) -> Hook {
        let gate = Arc::clone(&self.0);
        Arc::new(move |point, key, frame| {
            let me = thread::current().id();
            let at = lock(&gate.held).ids.iter().position(|id| *id == Some(me));
            let Some(at) = at else {
                return;
            };
            let who = [Who::A, Who::B][at];
            if point == SweepEnded {
                let n = gate.sweeps[at].fetch_add(1, Ordering::SeqCst) + 1;
                assert!(
                    n <= SWEEP_BUDGET,
                    "{who:?} ended {n} sweeps, past the step budget of {SWEEP_BUDGET}"
                );
            }
            let mut held = lock(&gate.held);
            held.log.push(Got::At(Fired {
                who,
                point,
                key,
                frame,
            }));
            gate.wake.notify_all();
            while held.permits[at] == Some(0) {
                held = gate.wake.wait(held).unwrap_or_else(PoisonError::into_inner);
            }
            if let Some(left) = held.permits[at].as_mut() {
                *left -= 1;
            }
        })
    }

    /// A thread's first call, which makes its firings `who`'s.
    fn enter(&self, who: Who) {
        lock(&self.0.held).ids[who.at()] = Some(thread::current().id());
    }

    /// Let `who` pass one firing.
    fn permit(&self, who: Who) {
        let mut held = lock(&self.0.held);
        if let Some(left) = held.permits[who.at()].as_mut() {
            *left += 1;
        }
        self.0.wake.notify_all();
    }

    /// Let `who` pass every firing from here on.
    fn free(&self, who: Who) {
        lock(&self.0.held).permits[who.at()] = None;
        self.0.wake.notify_all();
    }

    /// Record that `who`'s closure ended.
    fn done(&self, who: Who) {
        lock(&self.0.held).log.push(Got::Done(who));
        self.0.wake.notify_all();
    }

    /// The `n`th thing that came, waiting until it has.
    fn came(&self, n: usize) -> Got {
        let mut held = lock(&self.0.held);
        while held.log.len() <= n {
            held = self
                .0
                .wake
                .wait(held)
                .unwrap_or_else(PoisonError::into_inner);
        }
        held.log[n]
    }
}

/// The controller's next thing from `who`; what came from the other thread
/// before it is kept in `record` too.
fn next_from(permits: &Permits, record: &mut Vec<Got>, who: Who) -> Got {
    loop {
        let got = permits.came(record.len());
        record.push(got);
        let mine = matches!(got, Got::At(f) if f.who == who) || got == Got::Done(who);
        if mine {
            return got;
        }
    }
}

/// Whether `who`'s closure ended, as `record` shows.
fn ended(record: &[Got], who: Who) -> bool {
    record.contains(&Got::Done(who))
}

/// The point a recorded thing fired, if it was a firing.
fn fired_point(got: Got) -> Option<HookPoint> {
    match got {
        Got::At(f) => Some(f.point),
        Got::Done(_) => None,
    }
}

/// Whether `who`'s last recorded thing is an `UnpinWait`: it is held at that
/// firing or just past it, and has produced nothing since.
fn at_wait(record: &[Got], who: Who) -> bool {
    matches!(
        record.iter().rev().find(|got| {
            matches!(got, Got::At(f) if f.who == who) || **got == Got::Done(who)
        }),
        Some(Got::At(f)) if f.point == UnpinWait
    )
}

/// Take `who`'s events until it waits for an unpin or its closure ends; `true`
/// if it waits.
fn settle(permits: &Permits, record: &mut Vec<Got>, who: Who) -> bool {
    let mut waits = false;
    while !waits && !ended(record, who) {
        let got = next_from(permits, record, who);
        waits = matches!(got, Got::At(f) if f.point == UnpinWait);
    }
    waits
}

/// Let `who` pass every firing from here on, and [`settle`] it. A thread held
/// at an `UnpinWait` firing sends nothing once it passes it: it is held in
/// the pool's wait until a frame is released, so it is taken to wait at once.
fn run_free(permits: &Permits, record: &mut Vec<Got>, who: Who) -> bool {
    permits.free(who);
    at_wait(record, who) || settle(permits, record, who)
}

/// What a thread's join gave, as the controller records it: its answer, and
/// the reservation it returned, if any.
fn outcome(
    joined: Result<Result<FrameReservation<'_, FaultFile>, String>, Box<dyn Any + Send>>,
    who: Who,
) -> (
    Result<FrameId, String>,
    Option<FrameReservation<'_, FaultFile>>,
) {
    let answer = match &joined {
        Ok(Ok(res)) => Ok(res.frame()),
        Ok(Err(e)) => Err(e.clone()),
        Err(_) => Err(format!("{who:?} panicked")),
    };
    (answer, joined.ok().and_then(|r| r.ok()))
}

/// Records `Done(who)` when dropped: at the end of a thread's closure, and
/// on its way out of a panic, so the controller never waits for a thread that
/// has finished.
struct DoneOnDrop {
    permits: Permits,
    who: Who,
}

impl Drop for DoneOnDrop {
    fn drop(&mut self) {
        self.permits.done(self.who);
    }
}

/// Spawn `f` on scope `s` as thread `who` of `acquire_shared_hand`.
fn spawn_held<'s, 'env, T: Send + 's>(
    s: &'s Scope<'s, 'env>,
    permits: &Permits,
    who: Who,
    f: impl FnOnce() -> T + Send + 's,
) -> ScopedJoinHandle<'s, T> {
    let permits = Permits(Arc::clone(&permits.0));
    s.spawn(move || {
        permits.enter(who);
        let _done = DoneOnDrop { permits, who };
        f()
    })
}

/// Two acquirers share the clock hand (card A-3b-0, Work 5, A6′ and Work 12;
/// gate ruling A-3b-0 R1): the hand alone does not decide. F = 2: X = L1 in
/// frame 0, loaded, clean, unpinned, its bit set; Y = L2 in frame 1, pinned
/// by the controller throughout. No frame is dirty. T_A and T_B each call
/// `reserve()`, both from `h0 = 0`. Permitted one firing at a time, T_A
/// clears X's bit at hand 0 and takes X as a candidate at hand 2, where it is
/// held at `RemoveLatched` with X's exclusive latch (no pin, no mutex), while
/// T_B passes Y at hands 1 and 3 and ends its sweep having met no busy frame.
/// So the shared hand tells T_B that every unpinned frame is dirty, which is
/// false: X is clean and T_A is about to take it. T_B's own pass (A6′) meets
/// X's latch, so T_B retries; then, T_A holding frame 0 and Y pinned, it
/// waits for an unpin, and the release of T_A's reservation serves it frame
/// 0. Mutants OWN-PASS-SKIPPED and PASS-NONE-NOT-BUSY end T_B with
/// `PoolStarved` right after its first sweep.
///
/// While a thread is held the controller makes no pool call at all. It
/// records, releases, joins, and only then asserts. The release is in this
/// order: T_A's reservation, which is what ends T_B's wait on a correct build,
/// and the controller's own pin `p` only if a waiter is still waiting then,
/// which no correct build needs. So under a mutant that makes a thread wait
/// or end where the script does not, the test still fails by an assertion; it
/// hangs only where a waiter is really waiting and never woken.
#[test]
fn acquire_shared_hand() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2] {
        put_l!(&file, &map, n);
    }
    // X = L1 in frame 0, its pin dropped, its bit set; Y = L2 in frame 1,
    // pinned by the controller throughout. No frame is dirty.
    let (x, y) = (L1, L2);
    let fx = load!(&pool, &map, x);
    let p = pinned!(pool.pin(y, &map));
    let fy = p.frame();
    assert_eq!((fx, fy), (FrameId(0), FrameId(1)));
    assert!(
        pool.frame_info(fx).unwrap().referenced,
        "X's bit is set by its load"
    );
    // The controller's pin on Y, which it drops last.
    let mut p = Some(p);

    let permits = Permits::new();
    pool.set_hook(Some(permits.hook()));
    let (record, after_first_sweep, a_answer, b_answer) = thread::scope(|s| {
        // The controller's record, in order.
        let mut record = Vec::new();
        let t_a = spawn_held(s, &permits, Who::A, || pool.reserve().map_err(text));
        // T_A's first `SweepStep`: it has passed A1 with `h0 = 0` and holds
        // nothing.
        next_from(&permits, &mut record, Who::A);
        let mut t_b = None;
        let mut after_first_sweep = None;
        if !ended(&record, Who::A) {
            t_b = Some(spawn_held(s, &permits, Who::B, || {
                pool.reserve().map_err(text)
            }));
            // T_B's first `SweepStep`: it too has passed A1 with `h0 = 0`.
            next_from(&permits, &mut record, Who::B);
            // Permits in the order A, B, A, B, B, each awaited.
            for who in [Who::A, Who::B, Who::A, Who::B, Who::B] {
                if ended(&record, Who::A) || ended(&record, Who::B) {
                    break;
                }
                permits.permit(who);
                next_from(&permits, &mut record, who);
            }
            if !ended(&record, Who::B) {
                // One more for T_B: its next event is a `SweepStep` (it
                // retried) or its end.
                permits.permit(Who::B);
                after_first_sweep = Some(next_from(&permits, &mut record, Who::B));
            }
        }
        // T_A runs freely: to its end or, under a mutant, to a wait.
        let mut a_waits = run_free(&permits, &mut record, Who::A);
        let mut t_a = Some(t_a);
        let mut a_answer = Err("T_A waits for an unpin".to_string());
        // T_A's reservation `r_A`, which the controller holds once T_A ended.
        let mut r_a = None;
        if !a_waits {
            let (answer, res) = outcome(t_a.take().unwrap().join(), Who::A);
            a_answer = answer;
            r_a = res;
        }
        let mut t_b = t_b;
        let mut b_answer = Err("T_B was not started".to_string());
        let mut b_waits = false;
        let mut r_b = None;
        if t_b.is_some() {
            b_answer = Err("T_B waits for an unpin".to_string());
            // T_B runs freely: to its wait for an unpin, or to its end.
            b_waits = run_free(&permits, &mut record, Who::B);
        }
        if !b_waits && t_b.is_some() {
            let (answer, res) = outcome(t_b.take().unwrap().join(), Who::B);
            b_answer = answer;
            r_b = res;
        }
        // Release: the reservations the controller holds. Dropping `r_A` frees
        // frame 0, which is what ends T_B's wait on a correct build.
        drop(r_a);
        drop(r_b);
        if a_waits {
            a_waits = settle(&permits, &mut record, Who::A);
        }
        if b_waits {
            b_waits = settle(&permits, &mut record, Who::B);
        }
        if a_waits || b_waits {
            // A waiter still waiting has only the controller's pin `p` left to
            // end its wait. A correct build never comes here.
            drop(p.take());
            if a_waits {
                settle(&permits, &mut record, Who::A);
            }
            if b_waits {
                settle(&permits, &mut record, Who::B);
            }
        }
        // Join what is not joined yet. A thread still waiting here is a waiter
        // that is really waiting and never woken: a lost wake-up, which on OS
        // threads only a hang can show (card A-3b-0, "For the gate").
        if let Some(t) = t_a.take() {
            a_answer = outcome(t.join(), Who::A).0;
        }
        if let Some(t) = t_b.take() {
            b_answer = outcome(t.join(), Who::B).0;
        }
        (record, after_first_sweep, a_answer, b_answer)
    });

    // The assertions, after the joins.
    let unpin_waits = record
        .iter()
        .filter(|got| matches!(got, Got::At(f) if f.who == Who::B && f.point == UnpinWait))
        .count();
    assert_eq!(
        after_first_sweep.map(fired_point),
        Some(Some(SweepStep)),
        "T_B's event after its first SweepEnded was not a SweepStep: {record:?}"
    );
    assert_eq!(unpin_waits, 1, "T_B's UnpinWait firings: {record:?}");
    for (who, answer) in [(Who::A, &a_answer), (Who::B, &b_answer)] {
        assert!(
            !matches!(answer, Err(e) if e.contains("starved")),
            "{who:?} answered PoolStarved: {answer:?}"
        );
    }
    assert_eq!(a_answer, Ok(fx), "T_A's reservation, frame 0");
    assert_eq!(b_answer, Ok(fx), "T_B's answer");
    drop(p);
}

/// `acquire_shared_hand` with its layout mirrored (card A-3b-0-F4; verdict
/// A-3b-0's follow-up F4 (i), mutant PASS-FRAME0-ONLY): the own pass must
/// visit every frame. F = 2: Y = L2 in frame 0, pinned by the controller
/// throughout; X = L1 in frame 1, loaded, clean, unpinned, its bit set. No
/// frame is dirty. T_A and T_B each call `reserve()`, both from `h0 = 0`.
/// Permitted one firing at a time, T_B passes Y at hands 0 and 2, T_A clears
/// X's bit at hand 1 and takes X as a candidate at hand 3, where it is held
/// at `RemoveLatched` with X's exclusive latch (no pin, no mutex), and T_B
/// ends its sweep having met no busy frame. The clean frame T_A holds is not
/// frame 0: T_B's own pass (A6′) must go on to frame 1, where it meets X's
/// latch, so T_B retries; then, T_A holding frame 1 and Y pinned, it waits
/// for an unpin, and the release of T_A's reservation serves it frame 1.
/// Under PASS-FRAME0-ONLY the pass visits frame 0 only, meets nothing busy
/// and answers `PoolStarved`, so T_B ends right after its first sweep.
///
/// While a thread is held the controller makes no pool call at all, as in
/// `acquire_shared_hand`: it records, releases, joins, and only then
/// asserts, on the same release rule.
#[test]
fn acquire_shared_hand_mirrored() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2] {
        put_l!(&file, &map, n);
    }
    // Y = L2 in frame 0, pinned by the controller throughout; X = L1 in
    // frame 1, its pin dropped, its bit set. No frame is dirty.
    let (x, y) = (L1, L2);
    let p = pinned!(pool.pin(y, &map));
    let fy = p.frame();
    let fx = load!(&pool, &map, x);
    assert_eq!((fx, fy), (FrameId(1), FrameId(0)));
    assert!(
        pool.frame_info(fx).unwrap().referenced,
        "X's bit is set by its load"
    );
    // The controller's pin on Y, which it drops last.
    let mut p = Some(p);

    let permits = Permits::new();
    pool.set_hook(Some(permits.hook()));
    let (record, after_first_sweep, a_answer, b_answer) = thread::scope(|s| {
        // The controller's record, in order.
        let mut record = Vec::new();
        let t_a = spawn_held(s, &permits, Who::A, || pool.reserve().map_err(text));
        // T_A's first `SweepStep`: it has passed A1 with `h0 = 0` and holds
        // nothing.
        next_from(&permits, &mut record, Who::A);
        let mut t_b = None;
        let mut after_first_sweep = None;
        if !ended(&record, Who::A) {
            t_b = Some(spawn_held(s, &permits, Who::B, || {
                pool.reserve().map_err(text)
            }));
            // T_B's first `SweepStep`: it too has passed A1 with `h0 = 0`.
            next_from(&permits, &mut record, Who::B);
            // Permits in the order B, A, B, A, B, each awaited.
            for who in [Who::B, Who::A, Who::B, Who::A, Who::B] {
                if ended(&record, Who::A) || ended(&record, Who::B) {
                    break;
                }
                permits.permit(who);
                next_from(&permits, &mut record, who);
            }
            if !ended(&record, Who::B) {
                // One more for T_B: its next event is a `SweepStep` (it
                // retried) or its end.
                permits.permit(Who::B);
                after_first_sweep = Some(next_from(&permits, &mut record, Who::B));
            }
        }
        // T_A runs freely: to its end or, under a mutant, to a wait.
        let mut a_waits = run_free(&permits, &mut record, Who::A);
        let mut t_a = Some(t_a);
        let mut a_answer = Err("T_A waits for an unpin".to_string());
        // T_A's reservation `r_A`, which the controller holds once T_A ended.
        let mut r_a = None;
        if !a_waits {
            let (answer, res) = outcome(t_a.take().unwrap().join(), Who::A);
            a_answer = answer;
            r_a = res;
        }
        let mut t_b = t_b;
        let mut b_answer = Err("T_B was not started".to_string());
        let mut b_waits = false;
        let mut r_b = None;
        if t_b.is_some() {
            b_answer = Err("T_B waits for an unpin".to_string());
            // T_B runs freely: to its wait for an unpin, or to its end.
            b_waits = run_free(&permits, &mut record, Who::B);
        }
        if !b_waits && t_b.is_some() {
            let (answer, res) = outcome(t_b.take().unwrap().join(), Who::B);
            b_answer = answer;
            r_b = res;
        }
        // Release: the reservations the controller holds. Dropping `r_A` frees
        // frame 1, which is what ends T_B's wait on a correct build.
        drop(r_a);
        drop(r_b);
        if a_waits {
            a_waits = settle(&permits, &mut record, Who::A);
        }
        if b_waits {
            b_waits = settle(&permits, &mut record, Who::B);
        }
        if a_waits || b_waits {
            // A waiter still waiting has only the controller's pin `p` left to
            // end its wait. A correct build never comes here.
            drop(p.take());
            if a_waits {
                settle(&permits, &mut record, Who::A);
            }
            if b_waits {
                settle(&permits, &mut record, Who::B);
            }
        }
        // Join what is not joined yet. A thread still waiting here is a waiter
        // that is really waiting and never woken: a lost wake-up, which on OS
        // threads only a hang can show (card A-3b-0, "For the gate").
        if let Some(t) = t_a.take() {
            a_answer = outcome(t.join(), Who::A).0;
        }
        if let Some(t) = t_b.take() {
            b_answer = outcome(t.join(), Who::B).0;
        }
        (record, after_first_sweep, a_answer, b_answer)
    });

    // The assertions, after the joins.
    let unpin_waits = record
        .iter()
        .filter(|got| matches!(got, Got::At(f) if f.who == Who::B && f.point == UnpinWait))
        .count();
    assert_eq!(
        after_first_sweep.map(fired_point),
        Some(Some(SweepStep)),
        "T_B's event after its first SweepEnded was not a SweepStep: {record:?}"
    );
    assert_eq!(unpin_waits, 1, "T_B's UnpinWait firings: {record:?}");
    for (who, answer) in [(Who::A, &a_answer), (Who::B, &b_answer)] {
        assert!(
            !matches!(answer, Err(e) if e.contains("starved")),
            "{who:?} answered PoolStarved: {answer:?}"
        );
    }
    assert_eq!(a_answer, Ok(fx), "T_A's reservation, frame 1");
    assert_eq!(b_answer, Ok(fx), "T_B's answer");
    drop(p);
}

/// The layout of `own_pass_rechecks` (card A-3b-0-F4, Work 3), that of
/// `sweep_end_rechecks_generation`: frame 0 holds `lpid(1)`, created and
/// released, so dirty and unpinned; frame 1 holds Y = L2, loaded and left
/// pinned to the controller. Returns Y, Y's frame and the controller's pin.
///
/// # Errors
/// Whatever `put_leaf`, [`Pool::reserve`], [`Pool::create`] and [`Pool::pin`]
/// return, and [`StoreError::Invalid`] when the creation or the load answers
/// another outcome.
fn own_pass_layout<'p>(
    file: &FaultFile,
    map: &TestMap,
    pool: &'p Pool<FaultFile>,
) -> Result<(PageKey, FrameId, Pin<'p, FaultFile>), StoreError> {
    put_leaf(file, map, 2, 11, 0x22)?;
    // Frame 0: dirty, unpinned. Frame 1: Y, pinned by the controller.
    let c = match pool.create(pool.reserve()?, lpid(1), Kind::Leaf, 1)? {
        CreateOutcome::Created(c) => c,
        other => {
            return Err(StoreError::Invalid(format!(
                "own_pass_layout: expected Created, got {other:?}"
            )));
        }
    };
    assert_eq!(c.frame(), FrameId(0));
    drop(c.release());
    let y = L2;
    let p = match pool.pin(y, map)? {
        PinOutcome::Pinned(p) => p,
        other => {
            return Err(StoreError::Invalid(format!(
                "own_pass_layout: expected Pinned, got {other:?}"
            )));
        }
    };
    assert_eq!(p.frame(), FrameId(1));
    Ok((y, FrameId(1), p))
}

/// What T of `own_pass_rechecks` holds at its `PassEnded`, as its controller
/// reads it while T is paused there (card A-3b-0-F4, Work 3): table-only
/// reads, and Y's latch tried through the controller's own pin, each as text
/// on error.
#[derive(Debug, PartialEq, Eq)]
struct Holds {
    /// `(free, reserved, loading, resident, orphan, unpin_waiters)`: no frame
    /// is `Reserved`, so T holds no reservation, and T is no waiter.
    stats: Result<(u32, u32, u32, u32, u32, u32), String>,
    /// The pin counts of frame 0 and of Y's frame: none, and the
    /// controller's own.
    pins: [Result<u64, String>; 2],
    /// Whether Y's latch could be taken shared: T holds no latch on Y's
    /// frame.
    y_shared: Result<bool, String>,
}

/// What the controller of `own_pass_rechecks` recorded at T's `PassEnded`:
/// whether the table mutex was free, read first, and what T holds there,
/// read only when it was.
#[derive(Debug, PartialEq, Eq)]
struct AtPassEnd {
    /// Whether the table mutex is free: the hook fired holding no mutex.
    table_free: Result<bool, String>,
    /// What T holds there; `None` if the table mutex was not free.
    holds: Option<Holds>,
}

/// Read, changing nothing, what T holds at its `PassEnded`, through `p`, the
/// controller's own pin on Y's frame `fy`. `table_free()` is read first, and
/// the rest only if the table mutex is free: a firing under it must fail the
/// test, not hang it, and `stats()` and `frame_info()` take that mutex.
/// `table_free()` answers a plain `bool`; it is kept in a `Result` beside the
/// other records.
fn at_pass_end(pool: &Pool<FaultFile>, p: &Pin<'_, FaultFile>, fy: FrameId) -> AtPassEnd {
    let table_free = Ok(pool.table_free());
    let holds = (table_free == Ok(true)).then(|| Holds {
        stats: pool
            .stats()
            .map(|s| {
                (
                    s.free,
                    s.reserved,
                    s.loading,
                    s.resident,
                    s.orphan,
                    s.unpin_waiters,
                )
            })
            .map_err(text),
        pins: [
            pool.frame_info(FrameId(0)).map(|i| i.pins).map_err(text),
            pool.frame_info(fy).map(|i| i.pins).map_err(text),
        ],
        // A `try_` form, which never blocks; its guard is dropped at once.
        y_shared: p.try_latch_shared().map(|g| g.is_some()).map_err(text),
    });
    AtPassEnd { table_free, holds }
}

/// The controller's hit in `own_pass_rechecks` (b), as it is recorded: the
/// frame it pinned, and whether that frame's latch could then be taken
/// shared through the pin (a `try_` form; its guard is dropped at once).
fn hit_record(
    hit: &Result<Option<Pin<'_, FaultFile>>, String>,
) -> Result<(FrameId, Result<bool, String>), String> {
    match hit {
        Ok(Some(pin)) => Ok((
            pin.frame(),
            pin.try_latch_shared().map(|g| g.is_some()).map_err(text),
        )),
        Ok(None) => Err("no frame holds lpid(1) Resident".to_string()),
        Err(e) => Err(e.clone()),
    }
}

/// The own pass's end and its final hold (card A-3b-0-F4; verdict A-3b-0's
/// follow-up F4 (iii), mutants PASS-NO-GEN and PASS-NO-SCAN). F = 2: frame 0
/// holds `lpid(1)`, created and released, so dirty and unpinned; frame 1
/// holds Y = L2, pinned by the controller. T's sweep takes nothing, and its
/// own pass (A6′) finds frame 0 dirty and frame 1 pinned, so it fires
/// `PassEnded` holding nothing, just before the hold that decides.
/// (a) The controller drops `p` there: the final hold sees that a frame
/// became unheld since A1, T looks again from A1, and its sweep (four
/// `SweepStep`) takes Y. No `PoolStarved`, no `UnpinWait`. Mutant PASS-NO-GEN
/// ends T with `PoolStarved` there.
/// (b) The controller pins `lpid(1)` there instead, a hit that moves no
/// generation: every frame is then held, and T goes on to wait for an unpin;
/// the controller's drop of `p` serves T frame 1. Mutant PASS-NO-SCAN ends T
/// with `PoolStarved` before any `UnpinWait`.
/// (c) The hook carries the key the acquisition is for: a pin's miss.
/// Step budget: 4 `SweepEnded` in (a) and in (b); a correct build ends one
/// in (a) and two in (b), the second before its wait.
#[test]
fn own_pass_rechecks() {
    // (a) The generation is re-checked: the controller drops `p` at T's
    //     `PassEnded`.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let (y, fy, p) = own_pass_layout(&file, &map, &pool).unwrap();
        let pz = Pauser::new();
        pool.set_hook(Some(budgeted_wait(&pz, 4, None, true)));
        pz.pause_at(PassEnded, NO_KEY);
        pz.report(Evicted, y);
        let (first, seen, got) = thread::scope(|s| {
            let t = spawn(s, &pz, "T", || pool.reserve().map_err(text));
            // T holds nothing at its PassEnded: no reservation, no pin, no
            // latch, no mutex.
            let first = pz.next_event();
            let mut seen = None;
            if first == Event::Arrived(PassEnded, NO_KEY, NO_FRAME) {
                // T is paused here, so no step is missed or added.
                pz.report(SweepStep, NO_KEY);
                let at = at_pass_end(&pool, &p, fy);
                let free = at.table_free == Ok(true);
                seen = Some(at);
                if free {
                    drop(p); // U1: the pin's own drop, no pool call.
                    pz.release(PassEnded, NO_KEY);
                } else {
                    // The table mutex is held there: nothing may wait on it.
                    pz.release_all();
                }
            } else {
                pz.release_all();
            }
            (first, seen, t.join())
        });
        assert_eq!(first, Event::Arrived(PassEnded, NO_KEY, NO_FRAME));
        let seen = seen.expect("T was held at its PassEnded");
        assert_eq!(
            seen.table_free,
            Ok(true),
            "at the pause: the hook fired holding no mutex"
        );
        let holds = seen
            .holds
            .expect("the pause's records, read only while the table mutex was free");
        assert_eq!(
            holds.stats,
            Ok((0, 0, 0, 2, 0, 0)),
            "at the pause: no frame is Reserved, so T holds no reservation, and T is no waiter"
        );
        assert_eq!(
            holds.pins,
            [Ok(0), Ok(1)],
            "at the pause: frame 0's count is none, and frame 1's is the controller's own"
        );
        assert_eq!(
            holds.y_shared,
            Ok(true),
            "at the pause: T holds no latch on Y's frame"
        );
        let got = got
            .unwrap_or_else(|e| panic!("T: {e:?}"))
            .unwrap_or_else(|e| panic!("T: {e}"));
        assert_eq!(got.frame(), fy, "T got frame 1");
        assert_eq!(
            without_done(reported(&pz)),
            vec![
                Event::Seen(SweepStep, NO_KEY, NO_FRAME),
                Event::Seen(SweepStep, NO_KEY, NO_FRAME),
                Event::Seen(SweepStep, NO_KEY, NO_FRAME),
                Event::Seen(SweepStep, NO_KEY, NO_FRAME),
                Event::Seen(Evicted, y, fy),
            ],
            "after its release T looks again from A1 and sweeps (four SweepStep) before it takes Y; the one Evicted is Y, and no UnpinWait was seen"
        );
        drop(got);
        assert_eq!(pool.frame_of(y).unwrap(), None);
        let s = pool.stats().unwrap();
        assert_eq!((s.evictions, s.unpin_waiters), (1, 0));
    }

    // (b) The scan is re-checked: the controller pins `lpid(1)` at T's
    //     `PassEnded`, T waits for an unpin, and the drop of `p` serves it.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let (y, fy, p) = own_pass_layout(&file, &map, &pool).unwrap();
        let pz = Pauser::new();
        pool.set_hook(Some(budgeted(&pz, 4, None)));
        pz.pause_at(PassEnded, NO_KEY);
        pz.report(UnpinWait, NO_KEY);
        pz.report(Evicted, y);
        let (first, seen, hit, next, waiters, got) = thread::scope(|s| {
            let t = spawn(s, &pz, "T", || pool.reserve().map_err(text));
            // T holds nothing at its PassEnded: no reservation, no pin, no
            // latch, no mutex.
            let first = pz.next_event();
            let (mut seen, mut hit, mut next, mut waiters) = (None, None, None, None);
            // The controller's pin on `lpid(1)`, kept until after the join.
            let mut q = None;
            if first == Event::Arrived(PassEnded, NO_KEY, NO_FRAME) {
                let at = at_pass_end(&pool, &p, fy);
                let free = at.table_free == Ok(true);
                seen = Some(at);
                if free {
                    // From here on `p` is kept as an `Option`: it is dropped
                    // at T's wait.
                    let mut p = Some(p);
                    // A table-only call: a hit on frame 0, which raises its
                    // count and moves no generation.
                    let made = pool.pin_resident(lpid(1)).map_err(text);
                    hit = Some(hit_record(&made));
                    q = made.ok().flatten();
                    pz.release(PassEnded, NO_KEY);
                    let e = pz.next_event();
                    next = Some(e);
                    if e == Event::Seen(UnpinWait, NO_KEY, NO_FRAME) {
                        // A table-only read.
                        waiters = Some(pool.stats().map(|s| s.unpin_waiters).map_err(text));
                        drop(p.take());
                    } else {
                        pz.release_all();
                    }
                } else {
                    // The table mutex is held there: nothing may wait on it.
                    pz.release_all();
                }
            } else {
                pz.release_all();
            }
            let got = t.join();
            drop(q);
            (first, seen, hit, next, waiters, got)
        });
        assert_eq!(first, Event::Arrived(PassEnded, NO_KEY, NO_FRAME));
        let seen = seen.expect("T was held at its PassEnded");
        assert_eq!(
            seen.table_free,
            Ok(true),
            "at the pause: the hook fired holding no mutex"
        );
        let holds = seen
            .holds
            .expect("the pause's records, read only while the table mutex was free");
        assert_eq!(
            holds.stats,
            Ok((0, 0, 0, 2, 0, 0)),
            "at the pause: no frame is Reserved, so T holds no reservation, and T is no waiter"
        );
        assert_eq!(
            holds.pins,
            [Ok(0), Ok(1)],
            "at the pause: frame 0's count is none, and frame 1's is the controller's own"
        );
        assert_eq!(
            holds.y_shared,
            Ok(true),
            "at the pause: T holds no latch on Y's frame"
        );
        assert_eq!(
            hit,
            Some(Ok((FrameId(0), Ok(true)))),
            "the controller's hit: a pin on frame 0, whose latch T no longer holds"
        );
        assert_eq!(
            next,
            Some(Event::Seen(UnpinWait, NO_KEY, NO_FRAME)),
            "T fired UnpinWait, holding nothing"
        );
        assert_eq!(waiters, Some(Ok(1)), "the count recorded at the wait");
        let got = got
            .unwrap_or_else(|e| panic!("T: {e:?}"))
            .unwrap_or_else(|e| panic!("T: {e}"));
        assert_eq!(got.frame(), fy, "T got frame 1");
        assert_eq!(
            without_done(reported(&pz)),
            vec![Event::Seen(Evicted, y, fy)]
        );
        drop(got);
        assert_eq!(pool.frame_of(y).unwrap(), None);
        let s = pool.stats().unwrap();
        assert_eq!((s.evictions, s.unpin_waiters), (1, 0));
    }

    // (c) The key: a pin's miss ends at the same hold, for its own key.
    {
        let (file, map, pool) = fixture(2).unwrap();
        let (_y, _fy, _p) = own_pass_layout(&file, &map, &pool).unwrap();
        let log = EventLog::new();
        pool.set_hook(Some(log.hook_sweep_budget(4)));
        let from = log.len();
        assert!(matches!(
            pool.pin(lpid(9), &map),
            Err(StoreError::PoolStarved)
        ));
        assert_eq!(log.since(from, PassEnded), vec![(lpid(9), NO_FRAME)]);
        assert_eq!(log.since(from, SweepEnded).len(), 1);
    }
}

// ---- Beyond the card's list: each pins a site of the acquisition or of
// ---- the reference bit that the card's own tests leave unjudged.

/// Every hook the frame acquisition fires finds the table mutex free
/// (BUF-TABLE-LEAF) and fires where card A-3b-0's hook table (Work 6)
/// places it: `SweepEnded` once per sweep that took nothing, with no page
/// and no frame, holding no reservation; `Evicted` once its victim left the
/// page table, with the victim's old key and its frame, holding that frame's
/// reservation. Single-threaded, so no other thread can hold the mutex.
#[test]
fn acquire_hooks_unlocked() {
    let (file, map, pool) = fixture(2).unwrap();
    for n in [1u64, 2, 3] {
        put_l!(&file, &map, n);
    }
    let pool = Arc::new(pool);
    let weak = Arc::downgrade(&pool);
    let fired = Arc::new(Mutex::new(Vec::<(
        HookPoint,
        PageKey,
        FrameId,
        bool,
        Option<PoolStats>,
    )>::new()));
    let sink = Arc::clone(&fired);
    let hook: Hook = Arc::new(move |point, key, frame| {
        assert_ne!(point, UnpinWait, "a single thread waited for an unpin");
        // Table-only reads, the second taken only where it cannot block: the
        // mutex is free if `table_free` says so.
        let free = weak.upgrade().map(|p| p.table_free());
        let stats = (free == Some(true))
            .then(|| weak.upgrade().and_then(|p| p.stats().ok()))
            .flatten();
        sink.lock()
            .unwrap()
            .push((point, key, frame, free.unwrap_or(false), stats));
    });
    pool.set_hook(Some(hook));
    let (f0, f1) = (load!(&pool, &map, L1), load!(&pool, &map, L2));
    assert_eq!((f0, f1), (FrameId(0), FrameId(1)));
    // A sweep that takes nothing: every frame is held.
    let p1 = pinned!(pool.pin(L1, &map));
    let p2 = pinned!(pool.pin(L2, &map));
    let from = fired.lock().unwrap().len();
    assert!(matches!(pool.try_reserve(), Ok(None)));
    // A sweep that takes a victim: frame 0's bit is cleared first.
    drop((p1, p2));
    let p3 = pinned!(pool.pin(L3, &map));
    assert_eq!(p3.frame(), f0);
    drop(p3);
    pool.set_hook(None);
    let fired = fired.lock().unwrap().clone();
    assert!(
        fired.iter().all(|&(_, _, _, free, _)| free),
        "a hook fired under the table mutex: {fired:?}"
    );
    let stats = |hand, reserved, resident, reads, evictions| PoolStats {
        frames: 2,
        free: 0,
        reserved,
        loading: 0,
        resident,
        orphan: 0,
        reads,
        buckets: 2,
        evictions,
        clock_hand: hand,
        unpin_waiters: 0,
    };
    let acquisition: Vec<_> = fired[from..]
        .iter()
        .filter(|(p, ..)| matches!(p, Evicted | SweepEnded))
        .map(|(p, k, f, _, s)| (*p, *k, *f, *s))
        .collect();
    // At `SweepEnded` nothing is held: the hand passed twice over two pinned
    // frames and rests where it began. At `Evicted` the victim is out of the
    // table and its frame is the firing thread's reservation; the hand rests
    // past it.
    assert_eq!(
        acquisition,
        vec![
            (SweepEnded, NO_KEY, NO_FRAME, Some(stats(0, 0, 2, 2, 0))),
            (Evicted, L1, f0, Some(stats(1, 1, 1, 2, 1))),
        ],
        "the acquisition's hooks, in order, each with the table's state"
    );
}

/// Sweep a full pool to `RemoveLatched` on (`key`, `frame`), which is then
/// held at R1 with its exclusive latch, and pin it from the controller with
/// `pin_frame`: R1 then answers `Kept`, and the frame stays `Resident`,
/// pinned by that pin, with the bit the hand just cleared (card A-3b-0,
/// Work 3 and A2). The paused thread holds the frame's exclusive latch; no
/// pin; no mutex. The controller makes only the table-only `pin_frame` while
/// it is paused. `key`'s frame must be the hand's next and the only frame
/// whose bit is clear at its first visit.
fn clear_and_hold<'p>(
    pool: &'p Pool<FaultFile>,
    pz: &Pauser,
    key: PageKey,
    frame: FrameId,
) -> Result<Pin<'p, FaultFile>, String> {
    let mut held: Option<Result<Option<Pin<'p, FaultFile>>, String>> = None;
    thread::scope(|s| {
        let t = spawn(s, pz, "T", || pool.try_reserve().map_err(text));
        // T holds `frame`'s exclusive latch; no pin; no mutex.
        let first = pz.next_event();
        if arrived(&first, RemoveLatched, key) {
            // A table-only call: the frame is still in the table under the
            // latch, and `pin_frame` takes no latch.
            held = Some(pool.pin_frame(frame).map_err(text));
        }
        pz.release_all();
        let got = t.join();
        // T's own `Done` is taken here, so that a later call's first event
        // is its own thread's.
        let rest = reported(pz);
        let failed = |what: &str| {
            format!("clear_and_hold {what}: first {first:?}, got {got:?}, rest {rest:?}")
        };
        if first != Event::Arrived(RemoveLatched, key, frame) {
            return Err(failed("never reached RemoveLatched"));
        }
        // T's acquisition went on past the kept frame and did not fail.
        match got {
            Ok(Ok(_)) => {}
            _ => return Err(failed("failed after the kept frame")),
        }
        if !rest.iter().all(|e| *e == Event::Done("T")) {
            return Err(failed("left events"));
        }
        Ok(())
    })?;
    let held = held.ok_or("clear_and_hold: the controller never pinned")?;
    held?.ok_or_else(|| "clear_and_hold: the frame is not Resident under its latch".to_string())
}

/// Where the clock's reference bit is set, and where it is not (card
/// A-3b-0, Work 3; S-STORE §5.2 "Clock"): a load (P10), a hit (P2 (a)),
/// `pin_resident`'s hit and `create`'s three pins each set it as they raise
/// a count; neither `Pool::pin_frame` nor `VictimGuard::pin_frame` sets it
/// (each is seen alone below; the latter by gate ruling A-3b-0 R3, mutant
/// VICTIM-PIN-SETS-BIT); the hand clears it, and so does a frame's becoming
/// `Free` or `Reserved`. (A served waiter's set is not told apart here: its
/// frame has kept the loader's bit since P10, the hand clearing no pinned
/// frame.) Beyond the card's list: each site is seen alone, on a frame whose
/// bit is clear.
#[test]
fn clock_bit_sites() {
    let bit = |pool: &Pool<FaultFile>, f: FrameId| pool.frame_info(f).unwrap().referenced;

    // Single thread: an evicting sweep leaves the other frames' bits clear.
    {
        let (file, map, pool) = fixture(3).unwrap();
        for n in [1u64, 2, 3, 5] {
            put_l!(&file, &map, n);
        }
        let log = EventLog::new();
        pool.set_hook(Some(log.hook_no_wait()));
        let (fa, fb, fc) = (
            load!(&pool, &map, lpid(1)),
            load!(&pool, &map, lpid(2)),
            load!(&pool, &map, lpid(3)),
        );
        assert_eq!((fa, fb, fc), (FrameId(0), FrameId(1), FrameId(2)));
        // A load (P10) sets it on a frame off the free list.
        assert!(bit(&pool, fa) && bit(&pool, fb) && bit(&pool, fc), "P10");

        // The hand clears every bit, then takes frame 0: a frame that
        // becomes `Reserved` has its bit clear.
        let res = pool.try_reserve().unwrap().expect("three clean frames");
        assert_eq!(res.frame(), fa);
        assert!(!bit(&pool, res.frame()), "Reserved");
        assert!(!bit(&pool, fb) && !bit(&pool, fc), "the hand cleared them");

        // `pin_frame` is not a use: frame 1 keeps its clear bit.
        let q = pool.pin_frame(fb).unwrap().expect("Resident");
        assert_eq!(q.key(), lpid(2));
        assert!(!bit(&pool, fb), "pin_frame");
        drop(q);

        // `VictimGuard::pin_frame` is not a use either (gate ruling A-3b-0
        // R3): frame 1 taken as a victim and pinned under that latch keeps
        // its clear bit.
        let v = pool.try_victim(fb).unwrap().expect("L2 is unpinned");
        let q2 = v.pin_frame().unwrap();
        assert!(!bit(&pool, fb), "VictimGuard::pin_frame");
        drop(q2);
        drop(v);

        // A hit (P2 (a)) and `pin_resident`'s hit set it.
        let p = pinned!(pool.pin(lpid(2), &map));
        assert!(bit(&pool, fb), "a hit");
        drop(p);
        let p = pool.pin_resident(lpid(3)).unwrap().expect("Resident");
        assert!(bit(&pool, fc), "pin_resident");
        drop(p);

        // `create`'s `Created`, on an absent key, sets it in the reserved
        // frame.
        let c = created!(pool.create(res, lpid(5), Kind::Leaf, 1));
        assert_eq!(c.frame(), fa);
        assert!(bit(&pool, fa), "a created page");
        drop(c.release());

        // A load into a frame the hand cleared and took: set again at P10.
        let from = log.len();
        let p = pinned!(pool.pin(lpid(1), &map));
        assert_eq!(p.frame(), fb);
        assert_eq!(evictions(&log, from), vec![(2, 1)]);
        assert!(bit(&pool, fb), "P10 on a frame whose bit was clear");
        drop(p);
    }
    // A frame off the free list is `Reserved` with its bit clear; a `Free`
    // frame's is clear.
    {
        let (_file, _map, pool) = fixture(2).unwrap();
        let res = pool.reserve().unwrap();
        assert_eq!(
            pool.frame_info(res.frame()).unwrap(),
            FrameInfo {
                state: FrameState::Reserved,
                key: None,
                pins: 0,
                freed: false,
                referenced: false
            }
        );
        drop(res);
        for n in 0..2u32 {
            let info = pool.frame_info(FrameId(n)).unwrap();
            assert_eq!((info.state, info.referenced), (FrameState::Free, false));
        }
    }

    // Two threads, twice: `create`'s `Exists` pin, then its freed branch.
    {
        let (file, map, pool) = fixture(3).unwrap();
        for n in [1u64, 2, 3] {
            put_l!(&file, &map, n);
        }
        let pz = Pauser::new();
        // The step budget: one sweep per `clear_and_hold`; and no wait for an
        // unpin, which no thread of this test may reach (gate ruling A-3b-0
        // R2: mutant TRY-WAITS ends a thread as a real waiter there).
        pool.set_hook(Some(budgeted_wait(&pz, 4, None, true)));
        let (fa, fb, fc) = (
            load!(&pool, &map, lpid(1)),
            load!(&pool, &map, lpid(2)),
            load!(&pool, &map, lpid(3)),
        );
        // The hand clears every bit and takes L1's frame 0, which stays
        // reserved; L1's reload then takes L2's frame 1, whose bit is clear,
        // in one step. The hand rests on frame 2, whose bit is clear, with
        // frame 1's set.
        let hold = pool.try_reserve().unwrap().expect("three clean frames");
        assert_eq!(hold.frame(), fa);
        let p = pinned!(pool.pin(lpid(1), &map));
        assert_eq!(p.frame(), fb);
        drop(p);
        assert!(!bit(&pool, fc), "the first sweep cleared frame 2's bit");
        assert_eq!(
            pool.stats().unwrap().clock_hand,
            2,
            "the hand is at frame 2"
        );
        let key = lpid(3);
        // L3 gets its bit back, for the hand to clear.
        let p = pinned!(pool.pin(key, &map));
        assert_eq!(p.frame(), fc);
        drop(p);
        assert!(bit(&pool, fc));

        // `create`'s `Exists` pin: L3 live with a clear bit, held by a
        // `pin_frame` pin through the creation's own `reserve`.
        pz.pause_at(RemoveLatched, key);
        let q = clear_and_hold(&pool, &pz, key, fc).unwrap();
        assert!(!bit(&pool, fc), "pin_frame set no bit");
        assert_eq!(
            pool.frame_of(lpid(1)).unwrap(),
            None,
            "T's sweep went on and took L1"
        );
        match pool.create(pool.reserve().unwrap(), key, Kind::Leaf, 1) {
            Ok(CreateOutcome::Exists(p, res)) => {
                assert_eq!(p.frame(), fc);
                assert!(bit(&pool, fc), "`create`'s `Exists` pin sets the bit");
                drop(res);
                drop(p);
            }
            other => panic!("expected Exists, got {other:?}"),
        }
        drop(q);
        assert_eq!(
            pool.stats().unwrap().clock_hand,
            2,
            "the hand is at frame 2 again"
        );

        // `create`'s freed branch: L3 freed with a clear bit, held by a
        // `pin_frame` pin through the creation's own `reserve`. Every frame
        // is kept off the free list, so that the reserve's frame is the
        // creation's own.
        let p = pinned!(pool.pin(key, &map));
        p.latch_exclusive().unwrap().mark_freed().unwrap();
        drop(p);
        assert!(pool.frame_info(fc).unwrap().freed);
        assert!(bit(&pool, fc));
        let hold_b = pool.reserve().unwrap();
        assert_eq!(hold_b.frame(), fb);
        assert_eq!(pool.stats().unwrap().free, 0);
        pz.pause_at(RemoveLatched, key);
        let q = clear_and_hold(&pool, &pz, key, fc).unwrap();
        assert!(!bit(&pool, fc), "pin_frame set no bit");
        let c = created!(pool.create(hold_b, key, Kind::Leaf, 2));
        assert_eq!(c.frame(), fc, "the freed page is remade in its own frame");
        assert!(!c.is_freed() && c.is_dirty());
        assert!(bit(&pool, fc), "`create`'s freed branch sets the bit");
        drop(c);
        drop(q);
        drop(hold);
        assert_eq!(pool.stats().unwrap().unpin_waiters, 0);
    }
}
