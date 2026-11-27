#![allow(dead_code)]
// The two test binaries (latch and harness_selftest) each include this
// module and use a subset of it, so the rest would warn as dead code in
// each (the precedent is crates/nucleus-env/tests/common/mod.rs).

//! The interleaving harness (card A-4c0; S-STORE §15 "The interleaving
//! harness").
//!
//! S-STORE §15: "A test that runs under it runs the store's own code under a
//! model checker that chooses which thread runs at every lock, wait,
//! wake-up, atomic and yield of that code." This module is the store's use of
//! it: the model checker's backend for the store's sync layer ([`Shuttle`]),
//! one way to write a scenario that runs under the model checker and on real
//! threads ([`Harness`]), the bounds every run states, and the three ways to
//! run a scenario ([`check`], [`census`] and [`replay`]), with
//! [`kill_evidence`] for a kill that rests on a sampled exploration.
//!
//! # Determinism, the harness's invariant
//!
//! A model test's result is a function of its bounds, its code and the
//! pinned model checker. No clock, environment variable, OS randomness,
//! hash-iteration order or thread identity may reach the schedules or the
//! outcome. So a failure reproduces by running the same test again, which
//! fails at the same execution; by [`replay`] of its schedule; and by a
//! [`census`], whose resumed runs continue the same scheduler state. The
//! model checker reads `SHUTTLE_RANDOM_SEED` itself; the seed guard turns
//! that into a failure of class `Panic` whose message starts with "seed
//! overridden". This module reads no environment variable.
//!
//! # The bounds, and the numbers this card states
//!
//! S-STORE §15: each test's card states the test's exploration, "exhaustive,
//! every schedule of its fixture, which passes only if the search ends within
//! a stated cap of executions; or sampled, a stated scheduler (random, or
//! PCT at a stated depth), seed and number of executions, never below the
//! floor that the harness's card states and its self-tests justify."
//!
//! - **The floor:** a sampled exploration runs at least [`FLOOR_EXECUTIONS`]
//!   (1,000) executions, and a PCT exploration has depth at least
//!   [`FLOOR_PCT_DEPTH`] (3). The presets [`RANDOM`] and [`PCT`] are that
//!   floor. Its justification is the self-tests (`harness_selftest`): every
//!   planted bug is caught at both presets, and at the floor each fails in at
//!   least the share of the executions, at both presets and at a second seed
//!   (`harness_share_planted_bugs`). A later card that needs less states its
//!   reason, and its gate rules on it.
//! - **The share:** a kill that rests on a sampled exploration and fails in
//!   fewer than [`SHARE_PERCENT`] (5%) of that exploration's executions
//!   (fewer than 50 of the floor's 1,000) is fragile across code changes,
//!   and the card that owns its test gives the test a hook-steered form
//!   (S-STORE §15, card S-KAPPA). [`Census::meets_share`] is the one
//!   judgment of it; exactly 5% meets it. Why 5%: a kill with share `p` is
//!   missed by a fresh exploration of `n` executions with probability
//!   `(1 − p)^n`, which at the floor is 4.3 × 10⁻⁵ for 1%, 6.7 × 10⁻³ for
//!   0.5% and 0.14 for 0.2%; and a code change can also dilute a share, so
//!   the share keeps a fivefold margin over the 1% at which a fresh
//!   exploration at the floor misses with probability 4.3 × 10⁻⁵. Every
//!   planted bug meets it with room, at both presets and at a second seed.
//!   Raising the floor instead would buy the same miss probability at five
//!   times the time of every sampled test.
//! - **The cap:** [`EXHAUSTIVE_CAP`] (100,000 executions). Whether a
//!   fixture's search ends, so that the fixture is explored exhaustively, is
//!   judged against it, whatever cap a test states (S-STORE §15, card
//!   S-KAPPA). A test's own [`Explore::Exhaustive`] cap is within which its
//!   search must end for it to pass: reaching it is [`Class::NotExhausted`],
//!   never a pass. The cap holds what this card's own kills need: the
//!   smallest scenario in which two writers wait at once (S9) ends within it.
//! - **The step limit** is S-STORE §15's "step budget": "under the harness,
//!   the number of scheduling steps within which every execution of the test
//!   must end". A test states it in its [`Bounds`]; the presets' 10,000 is
//!   what a test may state "with slack over the longest run a correct build
//!   makes on its fixture". A run past it fails as a deadlock does. The
//!   exhaustive scheduler enumerates every schedule with no preemption
//!   bound, so it is for small fixtures; PCT's depth is the probabilistic
//!   analogue of a preemption bound.
//!
//! A sampled exploration is evidence, not proof: it judges a mutant only
//! together with its seed, which makes each execution replay.

use std::any::Any;
use std::cell::RefCell;
use std::fmt;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use nucleus_store::sync::{Poisoned, Primitives, StdPrimitives};
use shuttle::scheduler::{
    DfsScheduler, PctScheduler, RandomScheduler, ReplayScheduler, Schedule, Scheduler, Task, TaskId,
};
use shuttle::sync::atomic::AtomicU64 as ModelAtomicU64;
use shuttle::sync::{
    Condvar as ModelCondvar, Mutex as ModelMutex, MutexGuard as ModelMutexGuard,
    RwLock as ModelRwLock, RwLockReadGuard as ModelRwLockReadGuard,
    RwLockWriteGuard as ModelRwLockWriteGuard, TryLockError as ModelTryLockError,
};
use shuttle::{Config, FailurePersistence, MaxSteps, Runner};

pub mod monitor;
pub mod suite;

/// The model checker's backend for the store's sync layer (card A-4c0 Work
/// 5; S-STORE §15: "a test build with the checker's").
///
/// Every lock, wait, notify, atomic and yield is a scheduling point at which
/// the model checker chooses the next thread. Its mutex wraps the standard
/// library's, so a holder's panic poisons it (F5), and its condition variable
/// has no spurious wakeups and wakes, on `notify_one`, whichever waiter the
/// scheduler picks.
pub struct Shuttle;

impl Primitives for Shuttle {
    type RawMutex<T: Send> = ModelMutex<T>;
    type RawGuard<'a, T: Send + 'a> = ModelMutexGuard<'a, T>;
    type RawCondvar = ModelCondvar;
    type RawAtomicU64 = ModelAtomicU64;
    type RawRwCell<T: Send + Sync> = ModelRwLock<T>;
    type RawReadGuard<'a, T: Send + Sync + 'a> = ModelRwLockReadGuard<'a, T>;
    type RawWriteGuard<'a, T: Send + Sync + 'a> = ModelRwLockWriteGuard<'a, T>;

    fn mutex_new<T: Send>(value: T) -> Self::RawMutex<T> {
        ModelMutex::new(value)
    }

    fn mutex_lock<T: Send>(m: &Self::RawMutex<T>) -> Result<Self::RawGuard<'_, T>, Poisoned> {
        m.lock().map_err(|_| Poisoned)
    }

    fn condvar_new() -> Self::RawCondvar {
        ModelCondvar::new()
    }

    fn condvar_wait<'a, T: Send>(
        c: &Self::RawCondvar,
        g: Self::RawGuard<'a, T>,
    ) -> Result<Self::RawGuard<'a, T>, Poisoned> {
        c.wait(g).map_err(|_| Poisoned)
    }

    fn condvar_notify_one(c: &Self::RawCondvar) {
        c.notify_one();
    }

    fn condvar_notify_all(c: &Self::RawCondvar) {
        c.notify_all();
    }

    fn atomic_u64_new(v: u64) -> Self::RawAtomicU64 {
        ModelAtomicU64::new(v)
    }

    fn atomic_u64_load(a: &Self::RawAtomicU64) -> u64 {
        a.load(Ordering::SeqCst)
    }

    fn atomic_u64_store(a: &Self::RawAtomicU64, v: u64) {
        a.store(v, Ordering::SeqCst);
    }

    fn atomic_u64_fetch_add(a: &Self::RawAtomicU64, v: u64) -> u64 {
        a.fetch_add(v, Ordering::SeqCst)
    }

    fn atomic_u64_fetch_sub(a: &Self::RawAtomicU64, v: u64) -> u64 {
        a.fetch_sub(v, Ordering::SeqCst)
    }

    fn atomic_u64_compare_exchange(
        a: &Self::RawAtomicU64,
        current: u64,
        new: u64,
    ) -> Result<u64, u64> {
        a.compare_exchange(current, new, Ordering::SeqCst, Ordering::SeqCst)
    }

    fn rwcell_new<T: Send + Sync>(value: T) -> Self::RawRwCell<T> {
        ModelRwLock::new(value)
    }

    fn rwcell_try_read<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawReadGuard<'_, T>>, Poisoned> {
        match c.try_read() {
            Ok(guard) => Ok(Some(guard)),
            Err(ModelTryLockError::WouldBlock) => Ok(None),
            Err(ModelTryLockError::Poisoned(_)) => Err(Poisoned),
        }
    }

    fn rwcell_try_write<T: Send + Sync>(
        c: &Self::RawRwCell<T>,
    ) -> Result<Option<Self::RawWriteGuard<'_, T>>, Poisoned> {
        match c.try_write() {
            Ok(guard) => Ok(Some(guard)),
            Err(ModelTryLockError::WouldBlock) => Ok(None),
            Err(ModelTryLockError::Poisoned(_)) => Err(Poisoned),
        }
    }

    fn yield_now() {
        shuttle::thread::yield_now();
    }
}

/// A sync layer's backend with threads to run a scenario on (card A-4c0 Work
/// 5): the model checker's ([`Shuttle`]) or the platform's
/// ([`StdPrimitives`], on OS threads).
///
/// A scenario is a function generic over `H: Harness`. It uses only
/// [`Harness::spawn`], [`Harness::join`], the sync layer's types over `H`,
/// and `std::sync::Arc` for a reference count (S-STORE §15's one exception,
/// card S-MU: `Arc`'s atomics "never block and order nothing that store code
/// reads"). So it is written once and runs under the model checker and on
/// real threads.
pub trait Harness: Primitives {
    /// A spawned thread's handle.
    type Handle<T: Send + 'static>;
    /// Spawns a thread running `f`.
    fn spawn<F, T>(f: F) -> Self::Handle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static;
    /// Waits for the thread, and returns what it returned, or its panic's
    /// text.
    fn join<T: Send + 'static>(handle: Self::Handle<T>) -> Result<T, String>;
}

impl Harness for Shuttle {
    type Handle<T: Send + 'static> = shuttle::thread::JoinHandle<T>;

    fn spawn<F, T>(f: F) -> Self::Handle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        shuttle::thread::spawn(f)
    }

    fn join<T: Send + 'static>(handle: Self::Handle<T>) -> Result<T, String> {
        handle.join().map_err(|payload| panic_text(&*payload))
    }
}

impl Harness for StdPrimitives {
    type Handle<T: Send + 'static> = std::thread::JoinHandle<T>;

    fn spawn<F, T>(f: F) -> Self::Handle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        std::thread::spawn(f)
    }

    fn join<T: Send + 'static>(handle: Self::Handle<T>) -> Result<T, String> {
        handle.join().map_err(|payload| panic_text(&*payload))
    }
}

/// The text of a panic payload.
fn panic_text(payload: &(dyn Any + Send)) -> String {
    if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else {
        "a panic whose payload is not a string".to_string()
    }
}

/// The exploration a run states (S-STORE §15; card A-4c0 Work 5). Every call
/// states its bounds at its call site; there is no hidden default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Explore {
    /// A seeded random scheduler for `executions` executions.
    Random {
        /// The seed of the first execution.
        seed: u64,
        /// How many executions run.
        executions: usize,
    },
    /// A seeded PCT scheduler of `depth` for `executions` executions.
    Pct {
        /// The seed.
        seed: u64,
        /// The preemption depth.
        depth: usize,
        /// How many executions run.
        executions: usize,
    },
    /// An exhaustive depth-first search, which must end within
    /// `max_executions` for the run to pass.
    Exhaustive {
        /// The cap: the search reaching it is [`Class::NotExhausted`].
        max_executions: usize,
    },
}

impl Explore {
    /// The number of executions a sampled exploration states.
    fn sampled(self) -> Option<usize> {
        match self {
            Explore::Random { executions, .. } | Explore::Pct { executions, .. } => {
                Some(executions)
            }
            Explore::Exhaustive { .. } => None,
        }
    }

    /// The seed a sampled exploration states.
    fn sampled_seed(self) -> Option<u64> {
        match self {
            Explore::Random { seed, .. } | Explore::Pct { seed, .. } => Some(seed),
            Explore::Exhaustive { .. } => None,
        }
    }
}

/// What one run states (S-STORE §15): its exploration and its step budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Bounds {
    /// The exploration.
    pub explore: Explore,
    /// The step limit of one execution, S-STORE §15's "step budget": the
    /// number of scheduling steps within which every execution must end.
    pub max_steps: usize,
}

/// The floor of a sampled exploration: at least this many executions.
pub const FLOOR_EXECUTIONS: usize = 1_000;
/// The floor of a PCT exploration: at least this depth.
pub const FLOOR_PCT_DEPTH: usize = 3;
/// The cap: a fixture whose exhaustive search ends within this many
/// executions is explored exhaustively, whatever cap its test states.
pub const EXHAUSTIVE_CAP: usize = 100_000;
/// The sampled-kill share, in percent: a sampled kill that fails in fewer is
/// fragile.
pub const SHARE_PERCENT: u64 = 5;
/// The step limit of the presets, with slack over the longest run a correct
/// build makes on this card's fixtures.
const PRESET_MAX_STEPS: usize = 10_000;

/// The random preset at the floor: seed `0xA4C0_0001`, 1,000 executions.
pub const RANDOM: Bounds = Bounds {
    explore: Explore::Random {
        seed: 0xA4C0_0001,
        executions: FLOOR_EXECUTIONS,
    },
    max_steps: PRESET_MAX_STEPS,
};

/// The PCT preset at the floor: seed `0xA4C0_0002`, depth 3, 1,000
/// executions.
pub const PCT: Bounds = Bounds {
    explore: Explore::Pct {
        seed: 0xA4C0_0002,
        depth: FLOOR_PCT_DEPTH,
        executions: FLOOR_EXECUTIONS,
    },
    max_steps: PRESET_MAX_STEPS,
};

/// The exhaustive preset: a search that must end within the cap.
pub const EXHAUSTIVE: Bounds = Bounds {
    explore: Explore::Exhaustive {
        max_executions: EXHAUSTIVE_CAP,
    },
    max_steps: PRESET_MAX_STEPS,
};

/// How a run ended (card A-4c0 Work 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Class {
    /// The body returned `Err`; the message is its text.
    Violation,
    /// The model checker found a deadlock: some thread unfinished and none
    /// runnable.
    Deadlock,
    /// An execution passed its step limit.
    StepLimit,
    /// An exhaustive search reached its cap without ending.
    NotExhausted,
    /// Any other panic.
    Panic,
}

impl Class {
    /// Its name in an evidence line.
    pub fn name(self) -> &'static str {
        match self {
            Class::Violation => "violation",
            Class::Deadlock => "deadlock",
            Class::StepLimit => "step_limit",
            Class::NotExhausted => "not_exhausted",
            Class::Panic => "panic",
        }
    }
}

/// A run's failure: what ended it, where, and the schedule that replays it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Failure {
    /// How the run ended.
    pub class: Class,
    /// The violation's text, or the panic's.
    pub message: String,
    /// The bounds the run stated.
    pub bounds: Bounds,
    /// The 0-based index of the failing execution.
    pub execution: usize,
    /// That execution's schedule, in the harness's encoding: the seed in
    /// decimal, a colon, then the steps separated by commas, each a thread
    /// index in decimal or `r` for a random draw.
    pub schedule: String,
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} at execution {} under {:?} (max_steps {}): {} [schedule {}]",
            self.class.name(),
            self.execution,
            self.bounds.explore,
            self.bounds.max_steps,
            self.message,
            self.schedule
        )
    }
}

/// A passing run: how many executions ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Report {
    /// The executions the run explored: exactly the stated number for a
    /// sampled exploration, and fewer than the cap for an exhaustive one.
    pub executions: usize,
}

/// The count of every failing execution of a sampled exploration, whatever
/// its class (S-STORE §15: "how many of the executions fail"; card S-KAPPA).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Census {
    /// The executions the census ran: the stated number.
    pub executions: usize,
    /// The failing executions: the sum of the four class counts.
    pub failing: usize,
    /// Executions whose body returned `Err`.
    pub violation: usize,
    /// Executions that deadlocked.
    pub deadlock: usize,
    /// Executions that passed the step limit.
    pub step_limit: usize,
    /// Executions that ended in any other panic.
    pub panic: usize,
    /// The first failing execution, whatever its class, with its schedule.
    pub first: Option<Failure>,
}

impl Census {
    /// The one judgment of the share (card A-4c0 Work 5): `failing × 100 ≥
    /// SHARE_PERCENT × executions`, computed without overflow. Exactly the
    /// share meets it.
    pub fn meets_share(&self) -> bool {
        u128::from(self.failing as u64) * 100
            >= u128::from(SHARE_PERCENT) * u128::from(self.executions as u64)
    }
}

/// The evidence a gate is shown for a kill that rests on a sampled
/// exploration (S-STORE §15; card S-KAPPA's K12): the exploration, the first
/// failing execution and that its schedule replayed, how many executions
/// failed by class, and whether the kill meets the share.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Evidence {
    /// The scheduler's name.
    pub scheduler: &'static str,
    /// The seed.
    pub seed: u64,
    /// The PCT depth, if the scheduler is PCT.
    pub depth: Option<usize>,
    /// The executions the census ran.
    pub executions: usize,
    /// The step limit.
    pub max_steps: usize,
    /// Executions whose body returned `Err`.
    pub violation: usize,
    /// Executions that deadlocked.
    pub deadlock: usize,
    /// Executions that passed the step limit.
    pub step_limit: usize,
    /// Executions that ended in any other panic.
    pub panic: usize,
    /// The failing executions, by class.
    pub failing: usize,
    /// The first failing execution's index.
    pub first_execution: usize,
    /// Its class.
    pub first_class: Class,
    /// Its message.
    pub first_message: String,
    /// Its schedule, in the harness's encoding.
    pub first_schedule: String,
    /// That its schedule replayed to the same failure.
    pub replayed: bool,
    /// Whether the kill meets the share.
    pub meets_share: bool,
}

impl fmt::Display for Evidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let depth = self.depth.map_or("-".to_string(), |d| d.to_string());
        let tenths =
            u128::from(self.failing as u64) * 1_000 / u128::from(self.executions.max(1) as u64);
        write!(
            f,
            "{} seed {} depth {} executions {} max_steps {}: first failing execution {} ({}) replayed; \
             failing {} of {} ({}.{:01}%), V/D/S/P {}/{}/{}/{}; share {}%: {}",
            self.scheduler,
            self.seed,
            depth,
            self.executions,
            self.max_steps,
            self.first_execution,
            self.first_class.name(),
            self.failing,
            self.executions,
            tenths / 10,
            tenths % 10,
            self.violation,
            self.deadlock,
            self.step_limit,
            self.panic,
            SHARE_PERCENT,
            if self.meets_share { "met" } else { "FRAGILE" }
        )
    }
}

/// One scheduling step of a recorded schedule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// The model checker chose the thread with this index.
    Task(usize),
    /// It drew a random number.
    Random,
}

/// A recorded schedule: a seed and its steps.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Record {
    seed: u64,
    steps: Vec<Step>,
}

impl Record {
    /// The harness's encoding of the schedule (card A-4c0 Work 5): the seed
    /// in decimal, a colon, then the steps separated by commas, each a thread
    /// index in decimal or `r` for a random draw. It is this module's own,
    /// since the facade exports no schedule encoder.
    fn encode(&self) -> String {
        let mut text = format!("{}:", self.seed);
        for (n, step) in self.steps.iter().enumerate() {
            if n > 0 {
                text.push(',');
            }
            match step {
                Step::Task(index) => text.push_str(&index.to_string()),
                Step::Random => text.push('r'),
            }
        }
        text
    }

    fn decode(text: &str) -> Result<Record, String> {
        let (seed, rest) = text
            .split_once(':')
            .ok_or_else(|| "no colon between the seed and the steps".to_string())?;
        let seed = seed
            .parse::<u64>()
            .map_err(|e| format!("the seed {seed:?} is not a u64: {e}"))?;
        let mut steps = Vec::new();
        if !rest.is_empty() {
            for word in rest.split(',') {
                if word == "r" {
                    steps.push(Step::Random);
                } else {
                    let index = word.parse::<usize>().map_err(|e| {
                        format!("the step {word:?} is neither a thread index nor r: {e}")
                    })?;
                    steps.push(Step::Task(index));
                }
            }
        }
        Ok(Record { seed, steps })
    }

    /// The model checker's `Schedule` of this record, for its replay
    /// scheduler. The facade exports neither an encoder nor its schedule
    /// step type, so the record is kept here and rebuilt with `push_task`
    /// and `push_random`.
    fn schedule(&self) -> Schedule {
        let mut schedule = Schedule::new(self.seed);
        for step in &self.steps {
            match step {
                Step::Task(index) => schedule.push_task(TaskId::from(*index)),
                Step::Random => schedule.push_random(),
            }
        }
        schedule
    }
}

/// A failing execution as the recorder noted it.
struct Noted {
    index: usize,
    class: Class,
    message: String,
}

/// The state one exploration shares between its runs (card A-4c0 Work 5).
///
/// The model checker ends its run at a deadlock, a step-limit excess or a
/// panic, so a census resumes it: a fresh run over the same scheduler state,
/// through the same recorder, goes on with the executions that remain. The
/// scheduler side of this state is not `Send`, since the model's threads
/// share one OS thread; what the body's thread needs is kept in an `Arc`.
struct Exploration {
    bounds: Bounds,
    scheduler: RefCell<Box<dyn Scheduler>>,
    /// The current execution's record.
    record: RefCell<Record>,
    /// The executions started so far.
    started: Arc<AtomicUsize>,
    /// The seed the first execution must have, for the seed guard.
    want_seed: Option<u64>,
    /// The seed the model checker used instead of the stated one.
    overridden: RefCell<Option<u64>>,
    /// A violation the body reported, awaiting its snapshot.
    pending: Arc<StdMutex<Option<(usize, String)>>>,
    /// The snapshot: the violation, with the schedule as it stood when the
    /// body reported it.
    snapshot: RefCell<Option<(usize, String, Record)>>,
    /// Every failing execution, in the order the census met them.
    failures: RefCell<Vec<Noted>>,
    /// The first failing execution by index.
    first: RefCell<Option<Failure>>,
}

/// The model checker's `Scheduler` around the scheduler the bounds name
/// (card A-4c0 Work 5): it counts the executions and records each one's
/// schedule, by delegation.
struct Recorder {
    exploration: Rc<Exploration>,
}

impl Scheduler for Recorder {
    fn new_execution(&mut self) -> Option<Schedule> {
        let e = &self.exploration;
        let schedule = e.scheduler.borrow_mut().new_execution()?;
        // The seed guard: both sampled schedulers begin their seed chain
        // with the seed they were built from. Another first seed means the
        // environment replaced it, so nothing runs.
        if e.started.load(Ordering::SeqCst) == 0
            && let Some(want) = e.want_seed
            && schedule.seed != want
        {
            *e.overridden.borrow_mut() = Some(schedule.seed);
            return None;
        }
        // The previous execution has ended: its violation, if any, is final.
        e.settle();
        e.close_violation();
        *e.record.borrow_mut() = Record {
            seed: schedule.seed,
            steps: Vec::new(),
        };
        e.started.fetch_add(1, Ordering::SeqCst);
        Some(schedule)
    }

    fn next_task(
        &mut self,
        runnable: &[&Task],
        current: Option<TaskId>,
        yielding: bool,
    ) -> Option<TaskId> {
        self.exploration.settle();
        let next = self
            .exploration
            .scheduler
            .borrow_mut()
            .next_task(runnable, current, yielding);
        if let Some(id) = next {
            self.exploration
                .record
                .borrow_mut()
                .steps
                .push(Step::Task(usize::from(id)));
        }
        next
    }

    fn next_u64(&mut self) -> u64 {
        self.exploration.settle();
        self.exploration
            .record
            .borrow_mut()
            .steps
            .push(Step::Random);
        self.exploration.scheduler.borrow_mut().next_u64()
    }
}

impl Exploration {
    /// The state for an exploration under `bounds`.
    fn new(bounds: Bounds) -> Rc<Self> {
        let scheduler: Box<dyn Scheduler> = match bounds.explore {
            Explore::Random { seed, executions } => {
                Box::new(RandomScheduler::new_from_seed(seed, executions))
            }
            Explore::Pct {
                seed,
                depth,
                executions,
            } => Box::new(PctScheduler::new_from_seed(seed, depth, executions)),
            Explore::Exhaustive { max_executions } => {
                Box::new(DfsScheduler::new(Some(max_executions), false))
            }
        };
        Rc::new(Self {
            bounds,
            scheduler: RefCell::new(scheduler),
            record: RefCell::new(Record::default()),
            started: Arc::new(AtomicUsize::new(0)),
            want_seed: bounds.explore.sampled_seed(),
            overridden: RefCell::new(None),
            pending: Arc::new(StdMutex::new(None)),
            snapshot: RefCell::new(None),
            failures: RefCell::new(Vec::new()),
            first: RefCell::new(None),
        })
    }

    /// Takes the snapshot of a violation the body reported: its schedule as
    /// it stands, before any further step. In `check` the body's `Err` is
    /// raised at once, so this is the schedule `check` reports.
    fn settle(&self) {
        let pending = self.pending.lock().ok().and_then(|mut slot| slot.take());
        if let Some((index, text)) = pending {
            let record = self.record.borrow().clone();
            *self.snapshot.borrow_mut() = Some((index, text, record));
        }
    }

    /// Notes the violation the body reported, once its execution has ended.
    fn close_violation(&self) {
        if let Some((index, text, record)) = self.snapshot.borrow_mut().take() {
            self.note(index, Class::Violation, text, &record);
        }
    }

    /// Notes a failing execution: in the list, and as the first if no
    /// earlier one was noted.
    fn note(&self, index: usize, class: Class, message: String, record: &Record) {
        self.failures.borrow_mut().push(Noted {
            index,
            class,
            message: message.clone(),
        });
        let earlier = self
            .first
            .borrow()
            .as_ref()
            .is_none_or(|first| index < first.execution);
        if earlier {
            *self.first.borrow_mut() = Some(Failure {
                class,
                message,
                bounds: self.bounds,
                execution: index,
                schedule: record.encode(),
            });
        }
    }

    /// The index of the execution that is running or just ended.
    fn current(&self) -> Option<usize> {
        self.started.load(Ordering::SeqCst).checked_sub(1)
    }
}

/// The model checker's configuration for a run (card A-4c0 Work 5): the step
/// limit, a 1 MiB stack for the model's threads, failure output printed and
/// never written to a file, and no time limit.
fn config(max_steps: usize) -> Config {
    let mut config = Config::new();
    config.max_steps = MaxSteps::FailAfter(max_steps);
    config.stack_size = 1 << 20;
    config.failure_persistence = FailurePersistence::Print;
    config
}

/// How a run of the model checker ended.
enum End {
    /// The scheduler had no execution left.
    Exhausted,
    /// The run ended in a panic: the failing execution's index, its class
    /// and its message.
    Panicked(Option<usize>, Class, String),
}

/// Runs the body once under the exploration's scheduler, until the scheduler
/// has no execution left or a run panics. `strict` is `check`'s way: the
/// body's `Err` is reported through `assert!` and ends the execution.
/// Otherwise the `Err` is noted in state outside the model and the execution
/// ends normally, so the run goes on.
fn drive<F>(exploration: &Rc<Exploration>, body: Arc<F>, strict: bool) -> End
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    let started = Arc::clone(&exploration.started);
    let pending = Arc::clone(&exploration.pending);
    let outcome = catch_unwind(AssertUnwindSafe(move || {
        Runner::new(
            Recorder {
                exploration: Rc::clone(exploration),
            },
            config(exploration.bounds.max_steps),
        )
        .run(move || {
            let text = match body() {
                Ok(()) => return,
                Err(text) => text,
            };
            if strict {
                assert!(text.is_empty(), "violation: {text}");
            } else if let Ok(mut slot) = pending.lock() {
                // This execution's index: the count of those started, less
                // this one.
                *slot = Some((started.load(Ordering::SeqCst).saturating_sub(1), text));
            }
        })
    }));
    match outcome {
        Ok(_) => End::Exhausted,
        Err(payload) => {
            exploration.settle();
            let (class, message) = classify(&*payload);
            End::Panicked(exploration.current(), class, message)
        }
    }
}

/// The prefix of a violation's panic message.
const VIOLATION_PREFIX: &str = "violation: ";

/// Classes a panic payload (card A-4c0 Work 5): the body's `Err` text is a
/// [`Class::Violation`]; "deadlock! blocked tasks: [ … ]" a
/// [`Class::Deadlock`]; "exceeded max_steps bound …" a [`Class::StepLimit`];
/// anything else a [`Class::Panic`].
fn classify(payload: &(dyn Any + Send)) -> (Class, String) {
    let text = panic_text(payload);
    if let Some(violation) = text.strip_prefix(VIOLATION_PREFIX) {
        return (Class::Violation, violation.to_string());
    }
    let class = if text.starts_with("deadlock!") {
        Class::Deadlock
    } else if text.starts_with("exceeded max_steps bound") {
        Class::StepLimit
    } else {
        Class::Panic
    };
    (class, text)
}

/// A failure of the harness itself, not of an execution: a misuse of a run,
/// or a seed override.
fn harness_failure(bounds: Bounds, message: String) -> Failure {
    Failure {
        class: Class::Panic,
        message,
        bounds,
        execution: 0,
        schedule: String::new(),
    }
}

/// The failure of a run that ended in a panic.
fn panicked_failure(
    exploration: &Exploration,
    index: usize,
    class: Class,
    message: String,
) -> Failure {
    Failure {
        class,
        message,
        bounds: exploration.bounds,
        execution: index,
        schedule: exploration.record.borrow().encode(),
    }
}

/// The seed guard's failure, if the environment replaced the seed.
fn seed_override(exploration: &Exploration) -> Option<Failure> {
    let got = exploration.overridden.borrow_mut().take()?;
    let want = exploration.want_seed.unwrap_or(0);
    Some(harness_failure(
        exploration.bounds,
        format!(
            "seed overridden: the model checker's first execution has seed {got} where {want} was stated"
        ),
    ))
}

/// Runs `body` under the scheduler `bounds` names (card A-4c0 Work 5), and
/// fails at the first failing execution.
///
/// Each execution runs `body` and reports an `Err` through `assert!`. A
/// deadlock, a step-limit excess and any other panic end the run as they end
/// the model checker's, and so does a panic of its own before any execution
/// starts, which is never a pass. A pass returns how many executions ran:
/// exactly the stated number for a sampled exploration, and fewer than the cap
/// for an exhaustive one, whose search reaching the cap is
/// [`Class::NotExhausted`], never a pass.
pub fn check<F>(bounds: Bounds, body: F) -> Result<Report, Failure>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    let exploration = Exploration::new(bounds);
    let body = Arc::new(body);
    match drive(&exploration, Arc::clone(&body), true) {
        End::Exhausted => {}
        End::Panicked(Some(index), class, message) => {
            return Err(panicked_failure(&exploration, index, class, message));
        }
        End::Panicked(None, _, message) => {
            // The model checker failed before any execution started. No
            // execution ran, so that is never a pass.
            let text = format!("the model checker failed before any execution: {message}");
            return Err(harness_failure(bounds, text));
        }
    }
    if let Some(failure) = seed_override(&exploration) {
        return Err(failure);
    }
    let executions = exploration.started.load(Ordering::SeqCst);
    if let Explore::Exhaustive { max_executions } = bounds.explore
        && executions >= max_executions
    {
        let record = exploration.record.borrow();
        return Err(Failure {
            class: Class::NotExhausted,
            message: format!(
                "the exhaustive search reached its cap of {max_executions} executions without ending"
            ),
            bounds,
            execution: executions.saturating_sub(1),
            schedule: record.encode(),
        });
    }
    Ok(Report { executions })
}

/// Replays a recorded schedule once (card A-4c0 Work 5): it parses the
/// harness's encoding back into the model checker's schedule and runs `body`
/// once under its replay scheduler, with the step limit `max_steps`. The
/// result is the failure of that execution, if it fails.
///
/// A malformed encoding is a failure of class [`Class::Panic`].
pub fn replay<F>(schedule: &str, max_steps: usize, body: F) -> Result<(), Failure>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    // One fixed execution: the bounds a replay reports itself under.
    let bounds = Bounds {
        explore: Explore::Exhaustive { max_executions: 1 },
        max_steps,
    };
    let record = match Record::decode(schedule) {
        Ok(record) => record,
        Err(reason) => {
            return Err(Failure {
                class: Class::Panic,
                message: format!("malformed schedule: {reason}"),
                bounds,
                execution: 0,
                schedule: schedule.to_string(),
            });
        }
    };
    let text = schedule.to_string();
    let body = Arc::new(body);
    let outcome = catch_unwind(AssertUnwindSafe(move || {
        Runner::new(
            ReplayScheduler::new_from_schedule(record.schedule()),
            config(max_steps),
        )
        .run(move || {
            if let Err(text) = body() {
                assert!(text.is_empty(), "violation: {text}");
            }
        })
    }));
    match outcome {
        Ok(_) => Ok(()),
        Err(payload) => {
            let (class, message) = classify(&*payload);
            Err(Failure {
                class,
                message,
                bounds,
                execution: 0,
                schedule: text,
            })
        }
    }
}

/// Counts every failing execution of a sampled exploration, whatever its
/// class (card A-4c0 Work 5; S-STORE §15: "how many of the executions fail").
///
/// Only `Random` and `Pct` bounds are sampled: `Exhaustive` bounds are a
/// failure of class [`Class::Panic`], as is a seed override.
///
/// Each execution runs `body`. An `Err` is recorded, with its index and
/// text, in state outside the model, and the execution then ends normally, so
/// the run goes on. A deadlock, a step-limit excess or a panic ends the model
/// checker's run; the census catches it, counts that execution under its
/// class, and starts a fresh run over the same scheduler state, through the
/// same recorder, for the executions that remain. It stops when the scheduler
/// has no execution left.
pub fn census<F>(bounds: Bounds, body: F) -> Result<Census, Failure>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    if bounds.explore.sampled().is_none() {
        let text =
            "census: the bounds must be Random or Pct, since an exhaustive search is not sampled";
        return Err(harness_failure(bounds, text.to_string()));
    }
    let exploration = Exploration::new(bounds);
    let body = Arc::new(body);
    loop {
        let before = exploration.started.load(Ordering::SeqCst);
        let end = drive(&exploration, Arc::clone(&body), false);
        exploration.settle();
        match end {
            End::Exhausted => break,
            End::Panicked(index, class, message) => {
                let index = match index {
                    Some(index) => index,
                    None => {
                        let text =
                            format!("the model checker failed before any execution: {message}");
                        return Err(harness_failure(bounds, text));
                    }
                };
                if exploration.started.load(Ordering::SeqCst) == before {
                    // No execution started in this run, so another run would
                    // fail the same way.
                    return Err(panicked_failure(&exploration, index, class, message));
                }
                // The body's own `Err` of this execution comes first, as it
                // does in `check`.
                exploration.close_violation();
                let already = exploration
                    .failures
                    .borrow()
                    .iter()
                    .any(|noted| noted.index == index);
                if !already {
                    let record = exploration.record.borrow();
                    exploration.note(index, class, message, &record);
                }
            }
        }
    }
    exploration.close_violation();
    if let Some(failure) = seed_override(&exploration) {
        return Err(failure);
    }
    let failures = exploration.failures.borrow();
    let count = |class: Class| failures.iter().filter(|noted| noted.class == class).count();
    Ok(Census {
        executions: exploration.started.load(Ordering::SeqCst),
        failing: failures.len(),
        violation: count(Class::Violation),
        deadlock: count(Class::Deadlock),
        step_limit: count(Class::StepLimit),
        panic: count(Class::Panic),
        first: exploration.first.borrow().clone(),
    })
}

/// The evidence for one kill that rests on a sampled exploration (card
/// A-4c0 Work 5; card S-KAPPA's K12): a census, its first failing execution
/// replayed to the same failure, and the share judged.
///
/// # Errors
///
/// A failure of class [`Class::Panic`] for a misuse: "census incomplete" if
/// the census ran fewer than the stated executions, "no execution failed" if
/// none failed, "replay differs" if the replay did not fail the same way.
pub fn kill_evidence<F>(bounds: Bounds, body: F) -> Result<Evidence, Failure>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    let body = Arc::new(body);
    let census_body = Arc::clone(&body);
    let census = census(bounds, move || census_body())?;
    let (scheduler, depth) = match bounds.explore {
        Explore::Random { .. } => ("random", None),
        Explore::Pct { depth, .. } => ("pct", Some(depth)),
        Explore::Exhaustive { .. } => ("exhaustive", None),
    };
    if Some(census.executions) != bounds.explore.sampled() {
        let text = format!(
            "census incomplete: {} of {} executions ran",
            census.executions,
            bounds.explore.sampled().unwrap_or(0)
        );
        return Err(harness_failure(bounds, text));
    }
    let first = match &census.first {
        Some(first) => first,
        None => return Err(harness_failure(bounds, "no execution failed".to_string())),
    };
    let replay_body = Arc::clone(&body);
    let again = replay(&first.schedule, bounds.max_steps, move || replay_body());
    let same = match &again {
        Err(again) => {
            again.class == first.class
                && (first.class != Class::Violation || again.message == first.message)
        }
        Ok(()) => false,
    };
    if !same {
        let text = match again {
            Ok(()) => format!(
                "replay differs: the schedule of execution {} passed",
                first.execution
            ),
            Err(again) => format!(
                "replay differs: execution {} failed as {} ({}) where the census saw {} ({})",
                first.execution,
                again.class.name(),
                again.message,
                first.class.name(),
                first.message
            ),
        };
        return Err(harness_failure(bounds, text));
    }
    Ok(Evidence {
        scheduler,
        seed: bounds.explore.sampled_seed().unwrap_or(0),
        depth,
        executions: census.executions,
        max_steps: bounds.max_steps,
        violation: census.violation,
        deadlock: census.deadlock,
        step_limit: census.step_limit,
        panic: census.panic,
        failing: census.failing,
        first_execution: first.execution,
        first_class: first.class,
        first_message: first.message.clone(),
        first_schedule: first.schedule.clone(),
        replayed: true,
        meets_share: census.meets_share(),
    })
}

/// Runs `body` `repeats` times on OS threads (card A-4c0 Work 5): the body
/// spawns its threads through [`Harness`] for [`StdPrimitives`]. It never
/// judges a mutant: its schedules are the OS's.
pub fn run_threads<F>(repeats: usize, body: F) -> Result<(), String>
where
    F: Fn() -> Result<(), String> + Send + Sync + 'static,
{
    for _ in 0..repeats {
        body()?;
    }
    Ok(())
}
