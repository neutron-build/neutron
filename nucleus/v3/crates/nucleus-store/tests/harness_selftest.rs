//! The harness's self-tests (card A-4c0 Work 9): planted bugs that the
//! harness must catch within its stated bounds, and checks of exhaustiveness,
//! determinism, replay, the census across every failure class, the share and
//! the step limit.
//!
//! The toys are small test-local types, not variants of production code. The
//! two latch toys implement `LatchUnderTest`, so they run the same scenarios
//! and bounds that the real latch passes:
//! - **PB-READER-PAST-WRITER**, `ToyReaderPref`: a reader-preferring latch;
//! - **PB-LOST-WAKEUP**, `lost_wakeup`: a waiter that tests a flag once,
//!   outside the mutex, and waits without re-checking;
//! - **PB-UNBOUNDED-BYPASS**, `ToyBarger`: writers that take a freed latch in
//!   whatever order they wake, with no count and no queue;
//! - **PB-COUNTER-RACE**, `racy_counter`: an increment by a load and a store.
//!
//! Every helper returns `Result`: the workspace lints deny `unwrap`, `expect`
//! and `panic!` outside `#[test]` functions.

mod harness;

use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use harness::suite::{LatchUnderTest, Stamps, TryOutcome, new_latch, run};
use harness::{
    Bounds, Census, Class, EXHAUSTIVE, EXHAUSTIVE_CAP, Evidence, Explore, FLOOR_EXECUTIONS,
    FLOOR_PCT_DEPTH, Failure, Harness, PCT, RANDOM, Shuttle, census, check, kill_evidence, replay,
    run_threads,
};
use nucleus_store::latch::{Latch, Mode};
use nucleus_store::sync::{AtomicU64, Condvar, Mutex, StdPrimitives, yield_now};

/// The second seed of `harness_same_seed_same_schedules` and
/// `harness_share_planted_bugs` (card A-4c0 Work 9).
const SECOND_SEED: u64 = 0xA4C0_0003;

/// A check that answers with its text instead of panicking.
fn require(condition: bool, what: impl Into<String>) -> Result<(), String> {
    if condition { Ok(()) } else { Err(what.into()) }
}

/// The `PCT` preset at a other seed.
fn pct_at(seed: u64) -> Bounds {
    Bounds {
        explore: Explore::Pct {
            seed,
            depth: FLOOR_PCT_DEPTH,
            executions: FLOOR_EXECUTIONS,
        },
        max_steps: RANDOM.max_steps,
    }
}

/// The `RANDOM` preset at another seed.
fn random_at(seed: u64) -> Bounds {
    Bounds {
        explore: Explore::Random {
            seed,
            executions: FLOOR_EXECUTIONS,
        },
        max_steps: RANDOM.max_steps,
    }
}

/// A failing `check`, or the text of its passing.
fn failed<T>(result: Result<T, Failure>) -> Result<Failure, String> {
    match result {
        Ok(_) => Err("the run passed, where a failure is planted".to_string()),
        Err(failure) => Ok(failure),
    }
}

// ---------------------------------------------------------------- the toys

/// Which latch policy a toy plays.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Policy {
    /// PB-READER-PAST-WRITER.
    ReaderPref,
    /// PB-UNBOUNDED-BYPASS.
    Barger,
}

/// A toy latch's state, under its mutex.
#[derive(Debug, Default)]
struct ToyState {
    readers: u32,
    writer: bool,
    /// The exclusive requests waiting (the barger counts them; the
    /// reader-preferring toy does not).
    writers_waiting: u32,
    clock: u64,
}

/// What both latch toys share: a mutex, a condition variable and a clock for
/// stamps. Neither toy has a bypass count or a queue.
struct ToyCore<H: Harness> {
    policy: Policy,
    state: Mutex<H, ToyState>,
    wake: Condvar<H>,
}

/// A toy's hold.
struct ToyGuard<'a, H: Harness> {
    core: &'a ToyCore<H>,
    mode: Mode,
    marker: PhantomData<fn() -> H>,
}

impl<H: Harness> ToyCore<H> {
    fn new(policy: Policy) -> Self {
        Self {
            policy,
            state: Mutex::new(ToyState::default()),
            wake: Condvar::new(),
        }
    }

    fn latch(&self, mode: Mode) -> Result<(ToyGuard<'_, H>, Stamps), String> {
        let mut state = self.state.lock().map_err(|e| e.to_string())?;
        state.clock = state.clock.wrapping_add(1);
        let arrived = state.clock;
        match (self.policy, mode) {
            // Shared waits only while a writer holds.
            (Policy::ReaderPref, Mode::Shared) => {
                while state.writer {
                    state = self.wake.wait(state).map_err(|e| e.to_string())?;
                }
                state.readers += 1;
            }
            // Exclusive waits while a writer holds or readers > 0.
            (Policy::ReaderPref, Mode::Exclusive) => {
                while state.writer || state.readers > 0 {
                    state = self.wake.wait(state).map_err(|e| e.to_string())?;
                }
                state.writer = true;
            }
            // Shared waits while a writer holds or any writer waits.
            (Policy::Barger, Mode::Shared) => {
                while state.writer || state.writers_waiting > 0 {
                    state = self.wake.wait(state).map_err(|e| e.to_string())?;
                }
                state.readers += 1;
            }
            // A writer takes the freed latch in whatever order the waiters
            // wake: there is no count and no queue.
            (Policy::Barger, Mode::Exclusive) => {
                state.writers_waiting += 1;
                while state.writer || state.readers > 0 {
                    state = self.wake.wait(state).map_err(|e| e.to_string())?;
                }
                state.writers_waiting -= 1;
                state.writer = true;
            }
        }
        state.clock = state.clock.wrapping_add(1);
        let granted = state.clock;
        Ok((
            ToyGuard {
                core: self,
                mode,
                marker: PhantomData,
            },
            Stamps { arrived, granted },
        ))
    }

    fn try_latch(&self, mode: Mode) -> Result<TryOutcome<ToyGuard<'_, H>>, String> {
        let mut state = self.state.lock().map_err(|e| e.to_string())?;
        state.clock = state.clock.wrapping_add(1);
        let arrived = state.clock;
        // The reader-preferring toy's try succeeds whenever it is compatible;
        // the barger's try also needs no writer waiting.
        let compatible = !state.writer
            && (self.policy == Policy::ReaderPref || state.writers_waiting == 0)
            && (mode == Mode::Shared || state.readers == 0);
        if !compatible {
            return Ok(TryOutcome::Refused(arrived));
        }
        match mode {
            Mode::Shared => state.readers += 1,
            Mode::Exclusive => state.writer = true,
        }
        state.clock = state.clock.wrapping_add(1);
        let granted = state.clock;
        Ok(TryOutcome::Granted(
            ToyGuard {
                core: self,
                mode,
                marker: PhantomData,
            },
            Stamps { arrived, granted },
        ))
    }

    fn release(&self, mode: Mode) -> Result<u64, String> {
        let mut state = self.state.lock().map_err(|e| e.to_string())?;
        match mode {
            Mode::Shared => state.readers = state.readers.saturating_sub(1),
            Mode::Exclusive => state.writer = false,
        }
        state.clock = state.clock.wrapping_add(1);
        let release = state.clock;
        self.wake.notify_all();
        Ok(release)
    }
}

/// PB-READER-PAST-WRITER: a reader-preferring latch.
struct ToyReaderPref<H: Harness>(ToyCore<H>);

/// PB-UNBOUNDED-BYPASS: §5.3's two rules, with no bound on bypass.
struct ToyBarger<H: Harness>(ToyCore<H>);

/// A latch toy the suite can build and run.
trait Toy<H: Harness>: LatchUnderTest<H> + Sized {
    /// Builds the toy. The bound and the capacity are the suite's to give;
    /// the toys ignore them.
    fn make(k: u32) -> Result<Self, String>;
}

/// The suite's constructor for a toy.
fn toy_new<H: Harness, T: Toy<H>>(k: u32, _capacity: u32) -> Result<T, String> {
    T::make(k)
}

macro_rules! latch_toy {
    ($toy:ident, $policy:expr) => {
        impl<H: Harness> Toy<H> for $toy<H> {
            fn make(_k: u32) -> Result<Self, String> {
                Ok(Self(ToyCore::new($policy)))
            }
        }

        impl<H: Harness> LatchUnderTest<H> for $toy<H> {
            type Guard<'a> = ToyGuard<'a, H>;

            fn latch(&self, mode: Mode) -> Result<(Self::Guard<'_>, Stamps), String> {
                self.0.latch(mode)
            }

            fn try_latch(&self, mode: Mode) -> Result<TryOutcome<Self::Guard<'_>>, String> {
                self.0.try_latch(mode)
            }

            fn release(guard: Self::Guard<'_>) -> Result<u64, String> {
                guard.core.release(guard.mode)
            }
        }
    };
}

latch_toy!(ToyReaderPref, Policy::ReaderPref);
latch_toy!(ToyBarger, Policy::Barger);

// ---------------------------------------------------------- the other bugs

/// PB-LOST-WAKEUP: the main thread is the waiter and the setter is the one
/// spawned thread. The waiter reads a flag outside the mutex and, if it is 0,
/// locks and waits once without re-checking. The setter stores 1 and calls
/// `notify_all` without the mutex.
fn lost_wakeup<H: Harness>() -> Result<(), String> {
    let flag = Arc::new(AtomicU64::<H>::new(0));
    let mutex = Arc::new(Mutex::<H, ()>::new(()));
    let wake = Arc::new(Condvar::<H>::new());
    let (theirs, setter_wake) = (Arc::clone(&flag), Arc::clone(&wake));
    let setter = H::spawn(move || {
        theirs.store(1);
        setter_wake.notify_all();
    });
    if flag.load() == 0 {
        let guard = mutex.lock().map_err(|e| e.to_string())?;
        let _once = wake.wait(guard).map_err(|e| e.to_string())?;
    }
    H::join(setter)
}

/// The fixed form of PB-LOST-WAKEUP: the same layout, the flag kept under the
/// mutex, and a wait in a re-checking loop.
fn fixed_handshake<H: Harness>() -> Result<(), String> {
    let state = Arc::new(Mutex::<H, u64>::new(0));
    let wake = Arc::new(Condvar::<H>::new());
    let (theirs, setter_wake) = (Arc::clone(&state), Arc::clone(&wake));
    let setter = H::spawn(move || -> Result<(), String> {
        let mut flag = theirs.lock().map_err(|e| e.to_string())?;
        *flag = 1;
        setter_wake.notify_all();
        Ok(())
    });
    let joined = H::join(setter)?;
    joined?;
    let mut flag = state.lock().map_err(|e| e.to_string())?;
    while *flag == 0 {
        flag = wake.wait(flag).map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// PB-COUNTER-RACE: each thread increments the counter `increments` times by
/// a load and then a store.
fn racy_counter<H: Harness>(threads: usize, increments: usize) -> Result<(), String> {
    let count = Arc::new(AtomicU64::<H>::new(0));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let count = Arc::clone(&count);
        handles.push(H::spawn(move || {
            for _ in 0..increments {
                let value = count.load();
                count.store(value + 1);
            }
        }));
    }
    for handle in handles {
        H::join(handle)?;
    }
    let got = count.load();
    let want = (threads * increments) as u64;
    if got == want {
        return Ok(());
    }
    Err(format!(
        "lost update: the counter holds {got}, where {want} are expected"
    ))
}

/// The fixed form of PB-COUNTER-RACE: `fetch_add`.
fn exact_counter<H: Harness>(threads: usize, increments: usize) -> Result<(), String> {
    let count = Arc::new(AtomicU64::<H>::new(0));
    let mut handles = Vec::with_capacity(threads);
    for _ in 0..threads {
        let count = Arc::clone(&count);
        handles.push(H::spawn(move || {
            for _ in 0..increments {
                count.fetch_add(1);
            }
        }));
    }
    for handle in handles {
        H::join(handle)?;
    }
    let got = count.load();
    let want = (threads * increments) as u64;
    if got == want {
        return Ok(());
    }
    Err(format!(
        "lost update: the counter holds {got}, where {want} are expected"
    ))
}

/// `harness_step_limit`'s and `mixed_outcomes`' spin: a thread waits for a
/// flag nobody sets, spinning with `yield_now` for at most 1,000,000 rounds,
/// then returns `Err("spin bound")`.
fn spin_bound<H: Harness>(rounds: u64) -> Result<(), String> {
    let flag = Arc::new(AtomicU64::<H>::new(0));
    let theirs = Arc::clone(&flag);
    let handle = H::spawn(move || -> Result<(), String> {
        for _ in 0..rounds {
            if theirs.load() == 1 {
                return Ok(());
            }
            yield_now::<H>();
        }
        Err("spin bound".to_string())
    });
    H::join(handle)?
}

/// `mixed_outcomes` (card A-4c0 Work 9, test 14): main spawns five racers,
/// each making one `compare_exchange(0, its number)` on a sync-layer atomic,
/// and joins them. By the winner, main then plants one outcome per class:
/// 1 a violation, 2 a deadlock, 3 a spin past a step limit of 1,000, 4 a
/// panic, and 5 a pass, counted outside the model.
fn mixed_outcomes<H: Harness>(passes: Arc<AtomicUsize>) -> Result<(), String> {
    let slot = Arc::new(AtomicU64::<H>::new(0));
    let mut handles = Vec::with_capacity(5);
    for racer in 1..=5u64 {
        let slot = Arc::clone(&slot);
        handles.push(H::spawn(move || {
            let _ = slot.compare_exchange(0, racer);
        }));
    }
    for handle in handles {
        H::join(handle)?;
    }
    let winner = slot.load();
    match winner {
        1 => Err("planted violation".to_string()),
        2 => {
            // A wait nothing notifies.
            let mutex = Mutex::<H, ()>::new(());
            let wake = Condvar::<H>::new();
            let guard = mutex.lock().map_err(|e| e.to_string())?;
            let _forever = wake.wait(guard).map_err(|e| e.to_string())?;
            Ok(())
        }
        3 => spin_bound::<H>(1_000_000),
        4 => {
            assert!(winner != 4, "planted panic");
            Ok(())
        }
        _ => {
            passes.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
}

/// The bounds of `mixed_outcomes` (card A-4c0 Work 9, test 14): the presets'
/// seeds, 1,000 executions, and a step limit of 1,000.
fn mixed_bounds(seed: u64, pct: bool) -> Bounds {
    Bounds {
        explore: if pct {
            Explore::Pct {
                seed,
                depth: FLOOR_PCT_DEPTH,
                executions: FLOOR_EXECUTIONS,
            }
        } else {
            Explore::Random {
                seed,
                executions: FLOOR_EXECUTIONS,
            }
        },
        max_steps: 1_000,
    }
}

/// Two threads each append three ops to a shared log; every execution's order
/// is collected outside the model (card A-4c0 Work 9, tests 7–9).
fn ordered_log<H: Harness>(orders: Arc<StdMutex<Vec<u32>>>) -> Result<(), String> {
    let log = Arc::new(Mutex::<H, Vec<u8>>::new(Vec::new()));
    let mut handles = Vec::with_capacity(2);
    for bit in [0u8, 1u8] {
        let log = Arc::clone(&log);
        handles.push(H::spawn(move || -> Result<(), String> {
            for _ in 0..3 {
                let mut guard = log.lock().map_err(|e| e.to_string())?;
                guard.push(bit);
            }
            Ok(())
        }));
    }
    for handle in handles {
        H::join(handle)??;
    }
    let guard = log.lock().map_err(|e| e.to_string())?;
    let order = guard
        .iter()
        .fold(0u32, |acc, &bit| (acc << 1) | u32::from(bit));
    if let Ok(mut orders) = orders.lock() {
        orders.push(order);
    }
    Ok(())
}

/// Runs `ordered_log` under `bounds`, and returns the order of every
/// execution.
fn orders_under(bounds: Bounds) -> Result<Vec<u32>, String> {
    let orders = Arc::new(StdMutex::new(Vec::new()));
    let theirs = Arc::clone(&orders);
    check(bounds, move || ordered_log::<Shuttle>(Arc::clone(&theirs)))
        .map_err(|f| f.to_string())?;
    let taken = orders.lock().map_err(|e| e.to_string())?;
    Ok(taken.clone())
}

// ------------------------------------------------------------------ tests 1-4

/// Test 1: PB-READER-PAST-WRITER with K = 1 on S1, under `PCT` and `RANDOM`,
/// is a `Violation` whose message starts with `M2`.
fn catches_reader_past_writer(bounds: Bounds) -> Result<(), String> {
    let failure = failed(check(bounds, move || {
        run::<Shuttle, ToyReaderPref<Shuttle>>(harness::suite::Scenario::S1, toy_new, 1)
    }))?;
    require(
        failure.class == Class::Violation,
        format!(
            "under {:?}: {}, where a violation is planted",
            bounds.explore, failure
        ),
    )?;
    require(
        failure.message.starts_with("M2"),
        format!(
            "under {:?}: {failure}, where the message starts with M2",
            bounds.explore
        ),
    )
}

#[test]
fn harness_catches_reader_past_writer_pct() {
    catches_reader_past_writer(PCT).unwrap();
}

#[test]
fn harness_catches_reader_past_writer_random() {
    catches_reader_past_writer(RANDOM).unwrap();
}

/// Tests 2 and 10: PB-LOST-WAKEUP under `bounds`, a `Deadlock`.
fn catches_lost_wakeup(bounds: Bounds) -> Result<Failure, String> {
    let failure = failed(check(bounds, lost_wakeup::<Shuttle>))?;
    require(
        failure.class == Class::Deadlock,
        format!(
            "under {:?}: {failure}, where a deadlock is planted",
            bounds.explore
        ),
    )?;
    Ok(failure)
}

/// Test 2: PB-LOST-WAKEUP under `EXHAUSTIVE` is a `Deadlock`.
#[test]
fn harness_catches_lost_wakeup_exhaustive() {
    catches_lost_wakeup(EXHAUSTIVE).unwrap();
}

/// Test 2: PB-LOST-WAKEUP under `PCT` is a `Deadlock`.
#[test]
fn harness_catches_lost_wakeup_pct() {
    catches_lost_wakeup(PCT).unwrap();
}

/// Test 2: the fixed handshake passes under `EXHAUSTIVE`, search complete.
#[test]
fn harness_fixed_handshake_exhaustive() {
    let report = check(EXHAUSTIVE, fixed_handshake::<Shuttle>).unwrap();
    println!(
        "harness_fixed_handshake_exhaustive executions: {}",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// Test 3: PB-UNBOUNDED-BYPASS with K = 1 on S4 is a `Violation` whose
/// message starts with `M4`.
fn catches_unbounded_bypass(bounds: Bounds) -> Result<(), String> {
    let failure = failed(check(bounds, move || {
        run::<Shuttle, ToyBarger<Shuttle>>(harness::suite::Scenario::S4, toy_new, 1)
    }))?;
    require(
        failure.class == Class::Violation,
        format!(
            "under {:?}: {failure}, where a violation is planted",
            bounds.explore
        ),
    )?;
    require(
        failure.message.starts_with("M4"),
        format!(
            "under {:?}: {failure}, where the message starts with M4",
            bounds.explore
        ),
    )
}

#[test]
fn harness_catches_unbounded_bypass_pct() {
    catches_unbounded_bypass(PCT).unwrap();
}

#[test]
fn harness_catches_unbounded_bypass_random() {
    catches_unbounded_bypass(RANDOM).unwrap();
}

/// Test 4: PB-COUNTER-RACE is a `Violation` whose message says "lost update".
fn catches_counter_race(bounds: Bounds, threads: usize, increments: usize) -> Result<(), String> {
    let failure = failed(check(bounds, move || {
        racy_counter::<Shuttle>(threads, increments)
    }))?;
    require(
        failure.class == Class::Violation,
        format!(
            "under {:?}: {failure}, where a violation is planted",
            bounds.explore
        ),
    )?;
    require(
        failure.message.starts_with("lost update"),
        format!(
            "under {:?}: {failure}, where the message starts with \"lost update\"",
            bounds.explore
        ),
    )
}

/// Test 4: PB-COUNTER-RACE, 2 threads × 1 increment, under `EXHAUSTIVE`.
#[test]
fn harness_catches_counter_race_exhaustive() {
    catches_counter_race(EXHAUSTIVE, 2, 1).unwrap();
}

/// Test 4: PB-COUNTER-RACE, 3 threads × 2 increments, under `PCT`.
#[test]
fn harness_catches_counter_race_pct() {
    catches_counter_race(PCT, 3, 2).unwrap();
}

/// Test 4: PB-COUNTER-RACE, 3 threads × 2 increments, under `RANDOM`.
#[test]
fn harness_catches_counter_race_random() {
    catches_counter_race(RANDOM, 3, 2).unwrap();
}

/// Test 5: the exact counter passes, 2 × 1 under `EXHAUSTIVE` (complete) and
/// 3 × 2 under `PCT`.
#[test]
fn harness_counter_exact() {
    let exhaustive = check(EXHAUSTIVE, move || exact_counter::<Shuttle>(2, 1)).unwrap();
    println!(
        "harness_counter_exact exhaustive executions: {}",
        exhaustive.executions
    );
    assert!(
        exhaustive.executions < EXHAUSTIVE_CAP,
        "the exhaustive search reached the cap"
    );
    let sampled = check(PCT, move || exact_counter::<Shuttle>(3, 2)).unwrap();
    assert_eq!(sampled.executions, FLOOR_EXECUTIONS);
}

/// Test 6: a body that counts its own executions outside the model. Under
/// `RANDOM` and `PCT`, the `Report` and the count are both exactly 1,000.
/// Under `EXHAUSTIVE`, on a 2-thread body, they are equal.
#[test]
fn harness_runs_stated_iterations() {
    for bounds in [RANDOM, PCT] {
        let runs = Arc::new(AtomicUsize::new(0));
        let theirs = Arc::clone(&runs);
        // A 2-thread body with a scheduling decision: the PCT scheduler
        // counts a step only where two tasks are runnable, and refuses a
        // closure with no such step at its second execution.
        let report = check(bounds, move || {
            theirs.fetch_add(1, Ordering::SeqCst);
            let handle = Shuttle::spawn(|| ());
            yield_now::<Shuttle>();
            Shuttle::join(handle)
        })
        .unwrap();
        assert_eq!(report.executions, 1_000, "under {:?}", bounds.explore);
        assert_eq!(
            runs.load(Ordering::SeqCst),
            1_000,
            "under {:?}",
            bounds.explore
        );
    }
    let runs = Arc::new(AtomicUsize::new(0));
    let theirs = Arc::clone(&runs);
    let report = check(EXHAUSTIVE, move || {
        theirs.fetch_add(1, Ordering::SeqCst);
        let handle = Shuttle::spawn(|| ());
        Shuttle::join(handle)
    })
    .unwrap();
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the exhaustive search reached the cap"
    );
    assert_eq!(report.executions, runs.load(Ordering::SeqCst));
}

/// Test 7: every order of two threads' three ops each is seen under
/// `EXHAUSTIVE`, and the search completes.
#[test]
fn harness_exhaustive_sees_every_order() {
    let orders = orders_under(EXHAUSTIVE).unwrap();
    let distinct: std::collections::HashSet<u32> = orders.iter().copied().collect();
    println!(
        "harness_exhaustive_sees_every_order: {} executions, {} distinct orders",
        orders.len(),
        distinct.len()
    );
    assert_eq!(distinct.len(), 20, "all C(6,3) = 20 orders are seen");
}

/// Test 8: the same body with a cap of 5 is `NotExhausted`.
#[test]
fn harness_exhaustive_reports_truncation() {
    let bounds = Bounds {
        explore: Explore::Exhaustive { max_executions: 5 },
        max_steps: EXHAUSTIVE.max_steps,
    };
    let failure = failed(check(bounds, move || {
        ordered_log::<Shuttle>(Arc::new(StdMutex::new(Vec::new())))
    }))
    .unwrap();
    assert_eq!(failure.class, Class::NotExhausted, "{failure}");
}

/// Test 9: `RANDOM` run twice gives identical sequences, and a second seed a
/// different one; the same three checks hold for `PCT`.
#[test]
fn harness_same_seed_same_schedules() {
    for (bounds, second) in [(RANDOM, random_at(SECOND_SEED)), (PCT, pct_at(SECOND_SEED))] {
        let first = orders_under(bounds).unwrap();
        let again = orders_under(bounds).unwrap();
        let other = orders_under(second).unwrap();
        assert_eq!(first.len(), 1_000, "under {:?}", bounds.explore);
        assert_eq!(
            first, again,
            "under {:?}: the same seed gives other orders",
            bounds.explore
        );
        assert_ne!(
            first, other,
            "under {:?}: a second seed gives the same orders",
            bounds.explore
        );
    }
}

/// Test 10: a failure replays. `racy_counter` under `RANDOM` fails at
/// execution `i` with schedule `S`; `check` again fails the same way, and
/// `replay(S, 10_000, …)` with the same class and message. `lost_wakeup`
/// under `PCT` replays as a `Deadlock`. A malformed schedule is a `Panic`.
#[test]
fn harness_replays_failure() {
    let first = failed(check(RANDOM, move || racy_counter::<Shuttle>(3, 2))).unwrap();
    let again = failed(check(RANDOM, move || racy_counter::<Shuttle>(3, 2))).unwrap();
    assert_eq!(first.execution, again.execution, "{first} vs {again}");
    assert_eq!(first.message, again.message, "{first} vs {again}");
    assert_eq!(first.schedule, again.schedule, "{first} vs {again}");
    let replayed = failed(replay(&first.schedule, 10_000, move || {
        racy_counter::<Shuttle>(3, 2)
    }))
    .unwrap();
    assert_eq!(replayed.class, Class::Violation, "{replayed}");
    assert_eq!(replayed.message, first.message, "{replayed} vs {first}");

    let first = failed(check(PCT, lost_wakeup::<Shuttle>)).unwrap();
    let replayed = failed(replay(&first.schedule, 10_000, move || {
        lost_wakeup::<Shuttle>()
    }))
    .unwrap();
    assert_eq!(first.class, Class::Deadlock, "{first}");
    assert_eq!(replayed.class, Class::Deadlock, "{replayed}");

    let malformed = failed(replay("not a schedule", 10_000, move || {
        lost_wakeup::<Shuttle>()
    }))
    .unwrap();
    assert_eq!(malformed.class, Class::Panic, "{malformed}");
    let empty = failed(replay("", 10_000, lost_wakeup::<Shuttle>)).unwrap();
    assert_eq!(empty.class, Class::Panic, "{empty}");
}

/// Test 11: a spin that nobody ends is a `StepLimit` under `RANDOM` with
/// `max_steps` 10,000, and a `Violation` with the step limit off.
#[test]
fn harness_step_limit() {
    let limited = failed(check(RANDOM, move || spin_bound::<Shuttle>(1_000_000))).unwrap();
    assert_eq!(limited.class, Class::StepLimit, "{limited}");
    // The step limit off: one execution, since the spin is long.
    let off = Bounds {
        explore: Explore::Random {
            seed: 0xA4C0_0001,
            executions: 1,
        },
        max_steps: usize::MAX,
    };
    let unlimited = failed(check(off, move || spin_bound::<Shuttle>(1_000_000))).unwrap();
    assert_eq!(unlimited.class, Class::Violation, "{unlimited}");
    assert_eq!(unlimited.message, "spin bound", "{unlimited}");
}

/// Test 12: the exact counter and S5 and S6 on `Latch<StdPrimitives>` pass
/// through `run_threads(20, …)`.
#[test]
fn harness_threads_mode() {
    run_threads(20, move || exact_counter::<StdPrimitives>(3, 2)).unwrap();
    for scenario in [harness::suite::Scenario::S5, harness::suite::Scenario::S6] {
        run_threads(20, move || {
            run::<StdPrimitives, Latch<StdPrimitives>>(scenario, new_latch, 1)
        })
        .unwrap();
    }
}

/// Test 13: the census counts every execution. Test 10's body and bounds.
#[test]
fn harness_census_counts() {
    let body = move || racy_counter::<Shuttle>(3, 2);
    let failure = failed(check(RANDOM, body)).unwrap();
    let counted = census(RANDOM, body).unwrap();
    assert_eq!(counted.executions, 1_000, "{counted:?}");
    assert_eq!(counted.failing, counted.violation, "{counted:?}");
    assert!((1..=1_000).contains(&counted.failing), "{counted:?}");
    let first = counted.first.as_ref().expect("a failure");
    assert_eq!(first.execution, failure.execution, "{first} vs {failure}");
    assert_eq!(first.schedule, failure.schedule, "{first} vs {failure}");
    let clean = census(RANDOM, move || exact_counter::<Shuttle>(3, 2)).unwrap();
    assert_eq!(
        (clean.failing, clean.first.is_none()),
        (0, true),
        "{clean:?}"
    );
}

/// Test 14: the census resumes past every kind of panic and counts every
/// class, under `RANDOM` and under `PCT`, with `max_steps` 1,000.
fn census_every_class(bounds: Bounds) -> Result<Evidence, String> {
    let passes = Arc::new(AtomicUsize::new(0));
    let counted_passes = Arc::clone(&passes);
    let body = move || mixed_outcomes::<Shuttle>(Arc::clone(&passes));
    let census = census(bounds, body).map_err(|f| f.to_string())?;
    assert_eq!(census.executions, 1_000, "{census:?}");
    assert!(
        census.violation >= 1
            && census.deadlock >= 1
            && census.step_limit >= 1
            && census.panic >= 1,
        "{census:?}"
    );
    assert_eq!(
        census.failing,
        census.violation + census.deadlock + census.step_limit + census.panic,
        "{census:?}"
    );
    assert_eq!(
        census.failing + counted_passes.load(Ordering::SeqCst),
        1_000,
        "{census:?}"
    );
    let failure = failed(check(bounds, move || {
        mixed_outcomes::<Shuttle>(Arc::new(AtomicUsize::new(0)))
    }))
    .map_err(|f| f.to_string())?;
    let first = census.first.as_ref().ok_or("the census saw no failure")?;
    if first.execution != failure.execution
        || first.class != failure.class
        || first.schedule != failure.schedule
    {
        return Err(format!(
            "the census's first {first} is not check's {failure}"
        ));
    }
    let passes = Arc::new(AtomicUsize::new(0));
    let evidence = kill_evidence(bounds, move || {
        mixed_outcomes::<Shuttle>(Arc::clone(&passes))
    })
    .map_err(|f| f.to_string())?;
    if !evidence.replayed || evidence.first_class != first.class {
        return Err(format!(
            "kill_evidence's {evidence} does not replay the census's first {first}"
        ));
    }
    Ok(evidence)
}

/// Test 14, under `RANDOM`.
#[test]
fn harness_census_every_class() {
    let evidence = census_every_class(mixed_bounds(0xA4C0_0001, false)).unwrap();
    println!("harness_census_every_class: {evidence}");
    let evidence = census_every_class(mixed_bounds(0xA4C0_0002, true)).unwrap();
    println!("harness_census_every_class: {evidence}");
}

/// Test 15: `kill_evidence` for each planted bug, under `RANDOM` and `PCT`
/// and under the same two at a second seed: every line replays, meets the
/// share, and has the planted bug's class and message prefix.
#[test]
fn harness_share_planted_bugs() {
    type Bug = (&'static str, Class, &'static str);
    for bounds in [RANDOM, PCT, random_at(SECOND_SEED), pct_at(SECOND_SEED)] {
        let pct = matches!(bounds.explore, Explore::Pct { .. });
        // (name, class, message prefix, evidence)
        let reader = kill_evidence(bounds, move || {
            run::<Shuttle, ToyReaderPref<Shuttle>>(harness::suite::Scenario::S1, toy_new, 1)
        })
        .unwrap();
        let lost = kill_evidence(bounds, lost_wakeup::<Shuttle>).unwrap();
        let barger = kill_evidence(bounds, move || {
            run::<Shuttle, ToyBarger<Shuttle>>(harness::suite::Scenario::S4, toy_new, 1)
        })
        .unwrap();
        let counter = kill_evidence(bounds, move || racy_counter::<Shuttle>(3, 2)).unwrap();
        let lines: [(&'static str, Evidence, Bug); 4] = [
            (
                "PB-READER-PAST-WRITER S1 K=1",
                reader,
                ("", Class::Violation, "M2"),
            ),
            ("PB-LOST-WAKEUP", lost, ("", Class::Deadlock, "deadlock!")),
            (
                "PB-UNBOUNDED-BYPASS S4 K=1",
                barger,
                ("", Class::Violation, "M4"),
            ),
            (
                "PB-COUNTER-RACE 3x2",
                counter,
                ("", Class::Violation, "lost update"),
            ),
        ];
        for (name, evidence, (_, class, prefix)) in lines {
            println!(
                "harness_share_planted_bugs {name} ({}): {evidence}",
                if pct { "pct" } else { "random" }
            );
            assert!(evidence.replayed, "{name}: {evidence}");
            assert!(evidence.meets_share, "{name}: below the share: {evidence}");
            assert_eq!(evidence.first_class, class, "{name}: {evidence}");
            assert!(
                evidence.first_message.starts_with(prefix),
                "{name}: {evidence}"
            );
        }
    }
}

/// Test 16: the share's boundary, on `Census` values built directly, and
/// `kill_evidence`'s answer when nothing fails.
#[test]
fn harness_share_boundary() {
    let census = |executions: usize, failing: usize| Census {
        executions,
        failing,
        violation: failing,
        deadlock: 0,
        step_limit: 0,
        panic: 0,
        first: None,
    };
    assert!(
        !census(1_000, 49).meets_share(),
        "49 of 1,000 does not meet the share"
    );
    assert!(
        census(1_000, 50).meets_share(),
        "50 of 1,000 meets the share"
    );
    assert!(
        !census(1_000, 0).meets_share(),
        "0 of 1,000 does not meet the share"
    );
    assert!(
        census(1_000, 1_000).meets_share(),
        "1,000 of 1,000 meets the share"
    );
    assert!(
        !census(2_000, 99).meets_share(),
        "99 of 2,000 does not meet the share"
    );
    assert!(
        census(2_000, 100).meets_share(),
        "100 of 2,000 meets the share"
    );
    let failure = kill_evidence(RANDOM, move || exact_counter::<Shuttle>(3, 2))
        .expect_err("kill_evidence on a clean body must fail");
    assert!(failure.message.contains("no execution failed"), "{failure}");
}
