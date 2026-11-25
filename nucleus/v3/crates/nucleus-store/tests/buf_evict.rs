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

mod buf_support;

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use buf_support::{
    Event, EventLog, FaultFile, MARK, NO_FRAME, NO_KEY, PS, Pauser, fixture, fixture_at_minimum,
    marker, minimum_frames, put_leaf, spawn,
};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, FrameId, FrameInfo, FrameState, Hook, HookPoint, PageKey, Pin, PinOutcome, Pool,
    PoolConfig, PoolStats,
};
use nucleus_store::page::Kind;

use HookPoint::{Evicted, RemoveLatched, SweepEnded, UnpinWait};

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
    let count = Arc::new(AtomicUsize::new(0));
    let pauser = pz.hook();
    Arc::new(move |point, key, frame| {
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
/// pinned, it waits for an unpin, and the unpin serves it. Mutants
/// BUSY-AS-STARVED and BUSY-AS-HELD fail the order of events: the first
/// ends T2 at its first decision, the second makes `UnpinWait` come before
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
/// or answering starved. Mutants UNHELD-NO-BUMP-UNPIN and DECISION-NO-GEN
/// make T answer `PoolStarved`. Step budget: 2 sweeps.
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
/// a count; `pin_frame` sets it never; the hand clears it, and so does a
/// frame's becoming `Free` or `Reserved`. (A served waiter's set is not told
/// apart here: its frame has kept the loader's bit since P10, the hand
/// clearing no pinned frame.) Beyond the card's list: each site is seen
/// alone, on a frame whose bit is clear.
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
        // The step budget: one sweep per `clear_and_hold`.
        pool.set_hook(Some(budgeted(&pz, 4, None)));
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
