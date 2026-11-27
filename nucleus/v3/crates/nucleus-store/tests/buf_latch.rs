//! `t_root_latch_fair`, the frame latch's pin, and the buffer pool's own
//! waits under the interleaving harness (card A-3b, Work 9; S-STORE §5.3
//! "Fair latches", §15 "The interleaving harness", §15.1's I-LATCH row).
//!
//! Every harness test is a scenario generic over `H: Harness`, run through
//! `check` under a stated exploration, whose bounds are the harness's
//! presets: `RANDOM` (seed `0xA4C0_0001`, 1,000 executions) for the two
//! streams, `EXHAUSTIVE` (a pass only if the search ends under the cap of
//! 100,000 executions) for the rest, each with the presets' step limit of
//! 10,000, which is every test's step budget (S-STORE §15). The pool is
//! built with `Pool::new_with_below_minimum_for_tests` on the model
//! checker's backend, so the harness runs the pool's own code: the page
//! table's mutex, both condition variables, the yields of the frame
//! acquisition, the hook's mutex and each frame's task-fair latch.
//!
//! - `t_root_latch_fair_reader_stream`, `t_root_latch_fair_writer` and
//!   `t_root_latch_fair_latch_level` are `t_root_latch_fair`'s three variants
//!   (its cold variant is A-4a's). Each is judged by the harness's monitor
//!   (`check_trace`, M1–M6) under the bypass budget `B = K + 1`, and by a
//!   direct count of its own: the later requests granted before the queued
//!   one. The two streams force their order through conditions the pool
//!   answers (`latch_info`), each read between two `yield_now`s and bounded
//!   by a count, so every reader's or writer's first request arrives while
//!   the queued request waits, in every schedule: a mutant that lets new
//!   requests over a waiting writer fails in every execution. No time enters
//!   any bound. Mutants LATCH-READER-PREF, LATCH-WRITER-PREF,
//!   TRYLATCH-BYPASS, FRAME-CAPACITY-ONE.
//! - `frame_latch_pinned` (no harness): every frame latch carries
//!   `FRAME_LATCH_BOUND` and a queue of the pool's threads, `o_frame` grows
//!   by one byte per thread, and a guard's stamps rise across arrival, grant
//!   and release. Mutants FRAME-BOUND-ZERO, FRAME-CAPACITY-ONE.
//! - `footprint_at_production_sizes`: a pool on the model checker's backend
//!   measures its footprint at the production backend's sizes, as
//!   `Pool::bytes_for` computes it (Work 6). Mutant FOOTPRINT-AT-BACKEND.
//! - `harness_cell_handoff`: a reader's and a writer's holds on one frame
//!   pass through the content cell, by a drop and by an explicit release.
//!   Mutant CELL-RELEASE-ORDER.
//! - The pool's waits, where a lost wake-up leaves every thread blocked and
//!   the harness reports a `Deadlock`, not a hang: `harness_unheld_pin_drop`
//!   (U1), `harness_unheld_reservation_drop` (U2),
//!   `harness_unheld_failed_load` (U3), `harness_load_waiter_woken` (P2 (b)
//!   and P10). Mutants UNPIN-NO-NOTIFY, WAIT-NO-REGISTER,
//!   RESERVATION-DROP-NO-NOTIFY, CLEANUP-NO-NOTIFY, LOAD-NO-NOTIFY,
//!   WAIT-NO-RECHECK, UNHELD-NO-BUMP-UNPIN.
//! - `sync_mutex_try_lock_std` and `sync_mutex_try_lock_model`: the one item
//!   this card adds to the sync layer, the try-lock `buf::Pool::table_free`
//!   needs (card A-3b, Work 5's table seam; S-STORE §9.2's poison, F5):
//!   free, held and poisoned on both backends.
//!
//! No test here reads a clock, sleeps or waits with a timeout: every bound
//! is a count, and the only spin is through `yield_now`.

mod harness;

use std::fmt;
use std::io::ErrorKind;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use harness::monitor::{Id, Outcome, Request, Violation, check_trace};
use harness::{EXHAUSTIVE, EXHAUSTIVE_CAP, Explore, Harness, PCT, RANDOM, Report, Shuttle, check};
use nucleus_env::{EnvError, File};
use nucleus_store::StoreError;
use nucleus_store::buf::{
    CreateOutcome, CurrentRef, FRAME_LATCH_BOUND, FrameId, H_MIN, Hook, HookPoint,
    OWN_PINNING_THREADS, PageKey, Pin, PinOutcome, Pool, PoolConfig, STEP2_RESERVE_FRAMES,
};
use nucleus_store::latch::{Holders, Kind, Latch, LatchCore, Mode, TryLatch};
use nucleus_store::meta::Ref;
use nucleus_store::page::{self, Kind as PageKind};
use nucleus_store::sync::{Condvar, Mutex, MutexGuard, Primitives, StdPrimitives, yield_now};

// ---------------------------------------------------------------- the bounds

/// The page size of every fixture.
const PS: u32 = 4096;

/// The byte a test reads, or writes, as a page's marker (A-3a's tests'
/// `MARK`).
const MARK: usize = 100;

/// `K`: every frame latch's bypass bound (S-STORE §5.3: "A-3b pins `K`").
const K: u32 = FRAME_LATCH_BOUND;

/// `t_root_latch_fair`'s bypass budget: `K + 1`, "a bound with slack on the
/// later requests granted before it" (S-STORE §15 I-LATCH).
const B: u32 = K + 1;

/// A stream's participants besides the queued one: `K + 2`. Each of their
/// first requests arrives while the queued one waits, so a latch that lets
/// them all over it overtakes it `K + 2` times, one more than the budget `B`
/// can excuse: the budget alone then fails it, as the monitor's M2 does.
const STREAM: u32 = K + 2;

/// Each participant's request ids start at a multiple of this, so that no
/// two requests of a trace share an id (the monitor's M6).
const IDS: Id = 8;

/// The main thread's first request.
const MAIN: Id = 0;

/// The queued participant's request: W in the reader stream, Rd in the
/// writer variant.
const QUEUED: Id = 8;

/// The stream's first participant's requests start here.
const STREAM_FIRST: Id = 16;

/// The main thread's tries in `t_root_latch_fair_latch_level`: the first
/// round's and the second's.
const TRIES: [Id; 2] = [1, 3];

/// A spin's bound, in rounds of one check and one `yield_now`: a count,
/// never a time. It is far above the harness's step limit, which ends a spin
/// that never ends as a `StepLimit`, and turns a condition that never holds
/// on the platform's backend into a failure instead of a hang.
const ROUNDS: usize = 20_000;

/// Tree X's root, the page every fixture latches.
const X: PageKey = PageKey::Lpid(1);

// ------------------------------------------------------------- the fixtures

/// A page file whose every call fails: the tests over it do no I/O, and one
/// that did would end the execution with its error.
struct NoIoFile;

/// The error every call of [`NoIoFile`] returns.
fn no_io(op: &'static str) -> EnvError {
    EnvError::Io {
        op,
        path: "no-io".into(),
        kind: ErrorKind::Other,
        detail: "this test does no I/O".into(),
    }
}

impl File for NoIoFile {
    fn read_at(&self, _off: u64, _buf: &mut [u8]) -> Result<usize, EnvError> {
        Err(no_io("read_at"))
    }

    fn write_at(&self, _off: u64, _buf: &[u8]) -> Result<(), EnvError> {
        Err(no_io("write_at"))
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        Err(no_io("sync_data"))
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        Err(no_io("sync_all"))
    }

    fn len(&self) -> Result<u64, EnvError> {
        Err(no_io("len"))
    }

    fn set_len(&self, _len: u64) -> Result<(), EnvError> {
        Err(no_io("set_len"))
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        Err(no_io("lock_exclusive"))
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        Err(no_io("lock_shared"))
    }

    fn unlock(&self) -> Result<(), EnvError> {
        Err(no_io("unlock"))
    }
}

/// What [`ModelFile`] holds, under its mutex.
#[derive(Default)]
struct ModelBytes {
    /// The file's bytes.
    bytes: Vec<u8>,
    /// The `read_at` calls so far.
    reads: u64,
}

/// A page file over a `sync::Mutex<P, _>`, so that a load runs under the
/// model checker: its read is a scheduling point of the pool's own load.
struct ModelFile<P: Primitives>(Mutex<P, ModelBytes>);

impl<P: Primitives> ModelFile<P> {
    /// A file holding exactly `bytes`.
    fn with(bytes: Vec<u8>) -> ModelFile<P> {
        ModelFile(Mutex::new(ModelBytes { bytes, reads: 0 }))
    }

    fn io(op: &'static str, detail: &str) -> EnvError {
        EnvError::Io {
            op,
            path: "model".into(),
            kind: ErrorKind::Other,
            detail: detail.into(),
        }
    }

    /// The `read_at` calls so far.
    fn reads(&self) -> Result<u64, String> {
        let held = self.0.lock().map_err(said)?;
        Ok(held.reads)
    }
}

impl<P: Primitives> File for ModelFile<P> {
    /// Copies what lies below the file's length into `buf` and returns the
    /// count (E-IO: short only at the end of the file).
    fn read_at(&self, off: u64, buf: &mut [u8]) -> Result<usize, EnvError> {
        let mut held = self
            .0
            .lock()
            .map_err(|_| Self::io("read_at", "the model file's mutex is poisoned"))?;
        held.reads = held.reads.saturating_add(1);
        let start = usize::try_from(off).unwrap_or(usize::MAX);
        let n = held.bytes.len().saturating_sub(start).min(buf.len());
        let Some(dst) = buf.get_mut(..n) else {
            return Err(Self::io("read_at", "the buffer is shorter than its length"));
        };
        let Some(src) = held.bytes.get(start..start.saturating_add(n)) else {
            return Err(Self::io("read_at", "the offset is past the file"));
        };
        dst.copy_from_slice(src);
        Ok(n)
    }

    /// Writes all of `buf` at `off`, extending the file with zeros.
    fn write_at(&self, off: u64, buf: &[u8]) -> Result<(), EnvError> {
        let mut held = self
            .0
            .lock()
            .map_err(|_| Self::io("write_at", "the model file's mutex is poisoned"))?;
        let start = usize::try_from(off).map_err(|_| Self::io("write_at", "past usize"))?;
        let end = off
            .checked_add(buf.len() as u64)
            .and_then(|end| usize::try_from(end).ok())
            .ok_or_else(|| Self::io("write_at", "past usize"))?;
        if held.bytes.len() < end {
            held.bytes.resize(end, 0);
        }
        let Some(dst) = held.bytes.get_mut(start..end) else {
            return Err(Self::io("write_at", "past the file"));
        };
        dst.copy_from_slice(buf);
        Ok(())
    }

    fn sync_data(&self) -> Result<(), EnvError> {
        Ok(())
    }

    fn sync_all(&self) -> Result<(), EnvError> {
        Ok(())
    }

    fn len(&self) -> Result<u64, EnvError> {
        let held = self
            .0
            .lock()
            .map_err(|_| Self::io("len", "the model file's mutex is poisoned"))?;
        Ok(held.bytes.len() as u64)
    }

    fn set_len(&self, len: u64) -> Result<(), EnvError> {
        let mut held = self
            .0
            .lock()
            .map_err(|_| Self::io("set_len", "the model file's mutex is poisoned"))?;
        let len = usize::try_from(len).map_err(|_| Self::io("set_len", "past usize"))?;
        held.bytes.resize(len, 0);
        Ok(())
    }

    fn lock_exclusive(&self) -> Result<(), EnvError> {
        Ok(())
    }

    fn lock_shared(&self) -> Result<(), EnvError> {
        Ok(())
    }

    fn unlock(&self) -> Result<(), EnvError> {
        Ok(())
    }
}

/// The test double of card A-3c's current page map for one key: it answers
/// a fixed reference and takes no lock, so a load adds no scheduling point
/// of the map's own.
#[derive(Debug, Clone, Copy)]
struct StaticMap {
    /// The one key it maps.
    key: PageKey,
    /// That key's reference; every other key's is null.
    r: Ref,
}

impl<H: File, P: Primitives> CurrentRef<H, P> for StaticMap {
    fn current_ref(&self, _pool: &Pool<H, P>, key: PageKey) -> Result<Ref, StoreError> {
        Ok(if key == self.key { self.r } else { Ref::NULL })
    }
}

/// A sealed `PS`-byte page: zeroed, its header written (`kind`, `id`,
/// `epoch`), `marker` at byte `MARK`. Returns the bytes and their CRC.
fn sealed_page(kind: PageKind, id: u64, epoch: u32, marker: u8) -> Result<(Vec<u8>, u32), String> {
    let mut b = vec![0u8; PS as usize];
    page::write_header(&mut b, kind, id, epoch).map_err(said)?;
    b[MARK] = marker;
    let crc = page::seal(&mut b).map_err(said)?;
    Ok((b, crc))
}

/// A file whose slot `slot` holds `page`: zero bytes up to the slot's offset.
fn file_with(slot: u64, page: &[u8]) -> Vec<u8> {
    let mut bytes = vec![0u8; slot as usize * PS as usize];
    bytes.extend_from_slice(page);
    bytes
}

/// A pool of `frames` frames at `PS` for `callers` callers over
/// [`NoIoFile`], on the backend `P`: below S-STORE §5.4's minimum, so through
/// the test-only constructor, with every frame latch's queue sized for all
/// `callers`.
fn fixture<P: Primitives>(frames: u32, callers: u32) -> Result<Pool<NoIoFile, P>, String> {
    Pool::new_with_below_minimum_for_tests(
        Arc::new(NoIoFile),
        PoolConfig {
            frames,
            page_size: PS,
            callers,
        },
    )
    .map_err(said)
}

/// Tree X's root: `Lpid(1)`, created in the pool's first free frame and
/// released, so it is `Resident`. Returns its pin and its frame.
fn root_page<H: File, P: Primitives>(
    pool: &Pool<H, P>,
) -> Result<(Pin<'_, H, P>, FrameId), String> {
    let res = pool.reserve().map_err(said)?;
    let made = pool.create(res, X, PageKind::Leaf, 1).map_err(said)?;
    match made {
        CreateOutcome::Created(page) => {
            let frame = page.frame();
            Ok((page.release(), frame))
        }
        CreateOutcome::Exists(..) | CreateOutcome::Loading(..) => {
            Err("X's root already has a frame in a fresh pool".to_string())
        }
    }
}

/// A clean, unpinned `Lpid(1)` for the frame acquisition's waits (card
/// A-3b-0's U1 fixture): created in the pool's one frame, sealed through the
/// created page's guard, and marked written to `Ref { slot: 30, epoch: 1 }`,
/// which reads no slot and writes none (`mark_written` only checks). Returns
/// the creator's pin, which keeps the frame held.
fn created_clean<H: File, P: Primitives>(pool: &Pool<H, P>) -> Result<Pin<'_, H, P>, String> {
    let res = pool.reserve().map_err(said)?;
    let made = pool.create(res, X, PageKind::Leaf, 1).map_err(said)?;
    let CreateOutcome::Created(mut page) = made else {
        return Err("Lpid(1) already has a frame in a fresh pool".to_string());
    };
    let crc = page::seal(&mut page).map_err(said)?;
    page.mark_written(Ref {
        slot: 30,
        epoch: 1,
        crc,
    })
    .map_err(said)?;
    if page.is_dirty() {
        return Err("mark_written left the created page dirty".to_string());
    }
    Ok(page.release())
}

/// A gate between the two threads of a scenario, on the sync layer's mutex
/// and condition variable (card A-3b, Work 9). It places the interleaving a
/// test judges, as S-STORE §15 asks of a hook-steered form: the schedule the
/// test is about is in every execution, so its mutants fail in every one.
/// It also keeps the exhaustive search small, which matters as much: a
/// depth-first search holds every interleaving of two threads that are
/// runnable at once, so threads runnable at once in two phases of a scenario
/// multiply its schedules past the cap, and a spin (`yield_now` until another
/// thread has done something) never ends in the search's first schedule,
/// which runs the spinning thread at every step. So a gate waits by
/// blocking, and a scenario lets its threads run at once only where it is
/// about what they do there.
///
/// The gate is worked from the pool's hook points ([`Gate::hook`]), as an
/// [`Order`] says, and by two calls outside the pool: the spawned thread's
/// [`Gate::enter`], which announces it and waits for the publication, and
/// the main thread's [`Gate::wait_ready`], which it makes before it calls
/// into the pool. A hook fires holding no pool mutex (BUF-TABLE-LEAF), so
/// the gate may block in one.
///
/// Nothing under the gate's mutex can panic, so it is never poisoned; were
/// it, a call answers its error and the hook returns, and the execution ends
/// as the harness's `Deadlock`, never as a pass.
struct Gate<H: Harness> {
    state: Mutex<H, GateState>,
    wake: Condvar<H>,
}

/// What a [`Gate`] knows, under its mutex.
#[derive(Debug, Default, Clone, Copy)]
struct GateState {
    /// The spawned thread announced itself.
    ready: bool,
    /// The main thread published.
    published: bool,
    /// The spawned thread registered.
    registered: bool,
    /// The main thread passed the last point of its call.
    done: bool,
}

/// How a scenario's gate is worked from the pool's hook points. Which
/// thread fires which point is the pool's: a load fires `LoadPublished`,
/// `LoadResolved`, `LoadRead` and `Pinned` on its loader, `LoadWaiting` and
/// `WaitWoken` on a pin that waits for it, and `UnpinWait` on an acquisition
/// that waits for an unpin.
#[derive(Debug, Clone, Copy)]
struct Order {
    /// The main thread's hold: at this point it publishes, then waits until
    /// the other thread registered.
    hold: Option<HookPoint>,
    /// Whether the main thread, its hold over, makes the scenario's probe
    /// once ([`Gate::hook`]'s `probe`): a pool call that blocks it on the
    /// page table's mutex for as long as the other thread holds that mutex,
    /// which is between the other thread's last look and its wait.
    probe: bool,
    /// The other thread's registration.
    register: HookPoint,
    /// Whether the registering thread then waits, in the hook, until the
    /// main thread's `done`: it registers between two of its own steps and
    /// goes on only once the main thread's call is over.
    register_waits: bool,
    /// The last point of the main thread's call, which sets `done`.
    done: Option<HookPoint>,
    /// Whether a woken pin holds at `WaitWoken` until `done`.
    woken_waits: bool,
}

/// The call a scenario lets its main thread make once its hold is over.
type Probe = Arc<dyn Fn() + Send + Sync>;

impl<H: Harness> Gate<H> {
    /// An empty gate.
    fn new() -> Gate<H> {
        Gate {
            state: Mutex::new(GateState::default()),
            wake: Condvar::new(),
        }
    }

    /// The spawned thread's side, before it calls into the pool: announce,
    /// then wait for the publication, re-checking after every wake.
    fn enter(&self) -> Result<(), String> {
        let mut s = self.state.lock().map_err(said)?;
        s.ready = true;
        self.wake.notify_all();
        while !s.published {
            s = self.wake.wait(s).map_err(said)?;
        }
        Ok(())
    }

    /// The main thread's side, before it calls into the pool: wait until the
    /// other thread announced itself. When this returns the other thread is
    /// asleep in [`Gate::enter`]: it released the mutex only by waiting.
    fn wait_ready(&self) -> Result<(), String> {
        let mut s = self.state.lock().map_err(said)?;
        while !s.ready {
            s = self.wake.wait(s).map_err(said)?;
        }
        Ok(())
    }

    /// Waits, holding the gate, until `what` holds.
    fn wait_for(&self, mut s: MutexGuard<'_, H, GateState>, what: impl Fn(&GateState) -> bool) {
        while !what(&s) {
            s = match self.wake.wait(s) {
                Ok(s) => s,
                Err(_) => break,
            };
        }
    }

    /// The hook that works the gate as `order` says, with the scenario's
    /// `probe` for [`Order::probe`]. Every other point costs it nothing.
    fn hook(self: &Arc<Self>, order: Order, probe: Option<Probe>) -> Hook {
        let gate = Arc::clone(self);
        Arc::new(move |point, _key, _frame| {
            if Some(point) == order.hold {
                if let Ok(mut s) = gate.state.lock() {
                    s.published = true;
                    gate.wake.notify_all();
                    gate.wait_for(s, |s| s.registered);
                }
                if order.probe
                    && let Some(probe) = &probe
                {
                    probe();
                }
            } else if point == order.register {
                // Register and wake the holder first: a registering thread
                // that then waits for `done` must not keep the holder waiting.
                if let Ok(mut s) = gate.state.lock() {
                    s.registered = true;
                }
                gate.wake.notify_all();
                if order.register_waits
                    && let Ok(s) = gate.state.lock()
                {
                    gate.wait_for(s, |s| s.done);
                }
            } else if Some(point) == order.done {
                if let Ok(mut s) = gate.state.lock() {
                    s.done = true;
                }
                gate.wake.notify_all();
            } else if order.woken_waits
                && point == HookPoint::WaitWoken
                && let Ok(s) = gate.state.lock()
            {
                gate.wait_for(s, |s| s.done);
            }
        })
    }
}

// ---------------------------------------------------------------- the helpers

/// An error as text.
fn said<E: fmt::Debug>(e: E) -> String {
    format!("{e:?}")
}

/// A granted, released request as the monitor takes it.
fn held(id: Id, mode: Mode, kind: Kind, arrived: u64, grant: u64, release: u64) -> Request {
    Request {
        id,
        mode,
        kind,
        arrived,
        outcome: Outcome::Granted {
            grant,
            release: Some(release),
        },
    }
}

/// A refused try as the monitor takes it.
fn refused(id: Id, mode: Mode, arrived: u64) -> Request {
    Request {
        id,
        mode,
        kind: Kind::Try,
        arrived,
        outcome: Outcome::Refused,
    }
}

/// Spin until `done` holds, yielding between checks (S-STORE §15: a spin is
/// made only through `yield_now`), for at most [`ROUNDS`] rounds.
fn spin_until<H: Harness>(
    what: &str,
    mut done: impl FnMut() -> Result<bool, String>,
) -> Result<(), String> {
    for _ in 0..ROUNDS {
        if done()? {
            return Ok(());
        }
        yield_now::<H>();
    }
    Err(format!("{what}: not within {ROUNDS} rounds"))
}

/// How many later requests were granted before the request `id`: those that
/// arrived after it and were granted before it was (S-STORE §15 I-LATCH:
/// "the later requests granted before it").
fn overtaken(requests: &[Request], id: Id) -> Result<usize, String> {
    let target = requests
        .iter()
        .find(|r| r.id == id)
        .ok_or_else(|| format!("no request has id {id}"))?;
    let (arrived, grant) = match target.outcome {
        Outcome::Granted { grant, .. } => (target.arrived, grant),
        Outcome::Refused => return Err(format!("the queued request {id} was refused")),
    };
    Ok(requests
        .iter()
        .filter(|r| {
            matches!(
                r.outcome,
                Outcome::Granted { grant: g, .. } if r.arrived > arrived && g < grant
            )
        })
        .count())
}

/// `t_root_latch_fair`'s two judgments on a trace: the harness's monitor
/// (`check_trace` with the budget [`B`], M1–M6) and, beside it, the direct
/// count of the later requests granted before the queued one
/// ([`QUEUED`]), which must be at most the budget (S-STORE §15 I-LATCH:
/// "which the test checks beside the budget", card S-MU). A failure names
/// both judgments, so a mutant's class and its count are read together.
fn judged(all: Vec<Vec<Request>>, what: &str) -> Result<(), String> {
    let requests: Vec<Request> = all.into_iter().flatten().collect();
    let monitor = check_trace(&requests, B).map_err(|v: Violation| v.to_string());
    let count = overtaken(&requests, QUEUED);
    let direct = match &count {
        Ok(n) if *n <= B as usize => Ok(()),
        Ok(n) => Err(format!(
            "{what}: the queued request had {n} later requests granted before it, more than the \
             budget B = {B}"
        )),
        Err(text) => Err(text.clone()),
    };
    let (monitor, direct) = (monitor.err(), direct.err());
    if monitor.is_some() || direct.is_some() {
        return Err(format!(
            "{what}: monitor {}: direct count {}",
            monitor.unwrap_or_else(|| "passed".to_string()),
            direct.unwrap_or_else(|| format!("passed, at most {B}"))
        ));
    }
    Ok(())
}

/// The main thread's two tries in `t_root_latch_fair_latch_level`: one
/// shared and one exclusive, each released at once if it is granted.
fn two_tries<P: Primitives>(
    latch: &Latch<P>,
    first: Id,
    mine: &mut Vec<Request>,
) -> Result<(), String> {
    for (step, mode) in [Mode::Shared, Mode::Exclusive].into_iter().enumerate() {
        let id = first + step as Id;
        match latch.try_latch(mode).map_err(said)? {
            TryLatch::Granted(guard) => {
                let (arrived, granted) = guard.stamps();
                let release = guard.release().map_err(said)?;
                mine.push(held(id, mode, Kind::Try, arrived, granted, release));
            }
            TryLatch::Refused(arrived) => mine.push(refused(id, mode, arrived)),
        }
    }
    Ok(())
}

/// The least frame count a pool for `callers` calling threads may have
/// (S-STORE §5.4 "Minimum size"; R-1-memory §4.2–§4.4), written from the
/// formula and not asked of the build: `n = n_c + n_s`,
/// `R_EPOCH_min = n + s + 1`, `R_MAP = n × (H_min + 2) + R_EPOCH_min` and
/// `F_min = n × 3 × (H_min + 2) + R_EPOCH_min + R_MAP`.
fn minimum_frames(callers: u32) -> u32 {
    let n = u64::from(callers) + u64::from(OWN_PINNING_THREADS);
    let per_thread = u64::from(H_MIN) + 2;
    let r_epoch = n + u64::from(STEP2_RESERVE_FRAMES) + 1;
    let r_map = n * per_thread + r_epoch;
    let f_min = n * 3 * per_thread + r_epoch + r_map;
    u32::try_from(f_min).unwrap_or(u32::MAX)
}

/// A pool at S-STORE §5.4's minimum for `callers` callers, at `PS`, through
/// `Pool::new`.
fn pool_at_minimum(callers: u32) -> Result<Pool<NoIoFile>, String> {
    Pool::new(
        Arc::new(NoIoFile),
        PoolConfig {
            frames: minimum_frames(callers),
            page_size: PS,
            callers,
        },
    )
    .map_err(said)
}

// ------------------------------------------------- t_root_latch_fair, reader stream

/// `t_root_latch_fair`'s reader stream (S-STORE §15 I-LATCH; exploration
/// `RANDOM`): a stream of overlapping readers holds X's root shared while a
/// writer queues; the writer is granted within the bypass budget, and no
/// shared request that arrives while the writer waits is granted before it.
///
/// The order is forced, so that every execution judges it: main holds X
/// shared; W pins X and asks it exclusive once; main waits until W waits;
/// then the `STREAM` readers pin X once each and take it shared twice,
/// releasing between; main waits until every reader's first request has
/// arrived while W still waits, and releases. Both waits read `latch_info`
/// between two yields, under the step budget.
fn reader_stream<H: Harness>() -> Result<(), String> {
    let pool = Arc::new(fixture::<H>(2, 2 + STREAM)?);
    let (root, f) = root_page(&pool)?;
    let held_guard = root.latch_shared().map_err(said)?;
    let (main_arrived, main_granted) = held_guard.stamps();
    let a0 = pool.latch_info(f).map_err(said)?.arrivals;

    // W pins X and takes it exclusive once.
    let theirs = Arc::clone(&pool);
    let w = H::spawn(move || {
        let pin = theirs
            .pin_resident(X)
            .map_err(said)?
            .ok_or("X's root is not resident")?;
        let guard = pin.latch_exclusive().map_err(said)?;
        let (arrived, granted) = guard.stamps();
        let release = guard.release().map_err(said)?;
        drop(pin);
        Ok::<Request, String>(held(
            QUEUED,
            Mode::Exclusive,
            Kind::Blocking,
            arrived,
            granted,
            release,
        ))
    });

    spin_until::<H>("the writer did not queue on X's root", || {
        Ok(pool.latch_info(f).map_err(said)?.waiting >= 1)
    })?;

    // The readers: each pins X once and takes it shared twice.
    let mut readers = Vec::new();
    for n in 0..STREAM {
        let theirs = Arc::clone(&pool);
        readers.push(H::spawn(move || {
            let pin = theirs
                .pin_resident(X)
                .map_err(said)?
                .ok_or("X's root is not resident")?;
            let mut mine = Vec::new();
            for step in 0..2usize {
                let guard = pin.latch_shared().map_err(said)?;
                let (arrived, granted) = guard.stamps();
                let release = guard.release().map_err(said)?;
                mine.push(held(
                    STREAM_FIRST + n as Id * IDS + step,
                    Mode::Shared,
                    Kind::Blocking,
                    arrived,
                    granted,
                    release,
                ));
            }
            drop(pin);
            Ok::<Vec<Request>, String>(mine)
        }));
    }

    // Every reader's first request has arrived while W still waits.
    let want = a0 + 1 + u64::from(STREAM);
    spin_until::<H>("not every reader's first request arrived", || {
        Ok(pool.latch_info(f).map_err(said)?.arrivals >= want)
    })?;

    let main_release = held_guard.release().map_err(said)?;
    let mut all = vec![vec![held(
        MAIN,
        Mode::Shared,
        Kind::Blocking,
        main_arrived,
        main_granted,
        main_release,
    )]];
    all.push(vec![H::join(w)??]);
    for reader in readers {
        all.push(H::join(reader)??);
    }
    judged(all, "the reader stream")
}

/// `t_root_latch_fair_reader_stream`: the reader stream under `RANDOM`.
fn reader_stream_checked() -> Result<Report, String> {
    check(RANDOM, reader_stream::<Shuttle>).map_err(|f| format!("{f}"))
}

#[test]
fn t_root_latch_fair_reader_stream() {
    let report = reader_stream_checked().unwrap();
    println!(
        "t_root_latch_fair_reader_stream: RANDOM, {} executions",
        report.executions
    );
    assert_eq!(report.executions, random_executions());
}

// ------------------------------------------------- t_root_latch_fair, writer variant

/// `t_root_latch_fair`'s writer variant (S-STORE §15 I-LATCH; exploration
/// `RANDOM`): the reader stream with the modes swapped. A stream of writers
/// asks for X's root while a reader queues; the reader is granted within the
/// same bypass budget. Main holds X exclusive; Rd takes it shared once; main
/// waits until Rd waits; `STREAM` writers each take it exclusive twice; once
/// every writer's first request has arrived while Rd waits, main releases.
fn writer_variant<H: Harness>() -> Result<(), String> {
    let pool = Arc::new(fixture::<H>(2, 2 + STREAM)?);
    let (root, f) = root_page(&pool)?;
    let held_guard = root.latch_exclusive().map_err(said)?;
    let (main_arrived, main_granted) = held_guard.stamps();
    let a0 = pool.latch_info(f).map_err(said)?.arrivals;

    // Rd pins X and takes it shared once.
    let theirs = Arc::clone(&pool);
    let rd = H::spawn(move || {
        let pin = theirs
            .pin_resident(X)
            .map_err(said)?
            .ok_or("X's root is not resident")?;
        let guard = pin.latch_shared().map_err(said)?;
        let (arrived, granted) = guard.stamps();
        let release = guard.release().map_err(said)?;
        drop(pin);
        Ok::<Request, String>(held(
            QUEUED,
            Mode::Shared,
            Kind::Blocking,
            arrived,
            granted,
            release,
        ))
    });

    spin_until::<H>("the reader did not queue on X's root", || {
        Ok(pool.latch_info(f).map_err(said)?.waiting >= 1)
    })?;

    // The writers: each pins X once and takes it exclusive twice.
    let mut writers = Vec::new();
    for n in 0..STREAM {
        let theirs = Arc::clone(&pool);
        writers.push(H::spawn(move || {
            let pin = theirs
                .pin_resident(X)
                .map_err(said)?
                .ok_or("X's root is not resident")?;
            let mut mine = Vec::new();
            for step in 0..2usize {
                let guard = pin.latch_exclusive().map_err(said)?;
                let (arrived, granted) = guard.stamps();
                let release = guard.release().map_err(said)?;
                mine.push(held(
                    STREAM_FIRST + n as Id * IDS + step,
                    Mode::Exclusive,
                    Kind::Blocking,
                    arrived,
                    granted,
                    release,
                ));
            }
            drop(pin);
            Ok::<Vec<Request>, String>(mine)
        }));
    }

    // Every writer's first request has arrived while Rd still waits.
    let want = a0 + 1 + u64::from(STREAM);
    spin_until::<H>("not every writer's first request arrived", || {
        Ok(pool.latch_info(f).map_err(said)?.arrivals >= want)
    })?;

    let main_release = held_guard.release().map_err(said)?;
    let mut all = vec![vec![held(
        MAIN,
        Mode::Exclusive,
        Kind::Blocking,
        main_arrived,
        main_granted,
        main_release,
    )]];
    all.push(vec![H::join(rd)??]);
    for writer in writers {
        all.push(H::join(writer)??);
    }
    judged(all, "the writer variant")
}

/// `t_root_latch_fair_writer`: the writer variant under `RANDOM`.
fn writer_variant_checked() -> Result<Report, String> {
    check(RANDOM, writer_variant::<Shuttle>).map_err(|f| format!("{f}"))
}

#[test]
fn t_root_latch_fair_writer() {
    let report = writer_variant_checked().unwrap();
    println!(
        "t_root_latch_fair_writer: RANDOM, {} executions",
        report.executions
    );
    assert_eq!(report.executions, random_executions());
}

/// The executions the `RANDOM` preset states.
fn random_executions() -> usize {
    match RANDOM.explore {
        Explore::Random { executions, .. } => executions,
        // The other explorations state none: `RANDOM` is a random one.
        _ => 0,
    }
}

// ---------------------------------------------- t_root_latch_fair, latch-level variant

/// `t_root_latch_fair`'s latch-level variant (S-STORE §15 I-LATCH; the latch
/// type directly; exploration `EXHAUSTIVE`): "with a writer queued on a frame
/// that a reader holds shared, `try_latch` in either mode fails, including at
/// the instant the reader releases". The card's script (Work 9): main takes
/// `Latch<H>` shared; W takes it exclusive and releases it; main makes
/// `try_latch(Shared)` and `try_latch(Exclusive)`, releasing what it is
/// granted, releases its hold, makes the same two tries again, and joins W.
///
/// W is spawned right after main takes the latch, so the search holds every
/// interleaving of W's arrival with both rounds of tries and with main's
/// release: among them the schedules in which W is queued while main still
/// holds the latch shared (the first round, where a shared try that a held
/// latch lets over the queued writer fails: mutant TRYLATCH-BYPASS-HELD) and
/// those in which W is queued at or after main's release and not yet granted
/// (the second round: mutant TRYLATCH-BYPASS). Either success over a queued
/// writer is M3 or M2 and fails the direct assertion. The search ends under
/// the cap (RESULT gives its count).
fn latch_level<H: Harness>() -> Result<(), String> {
    let latch = Arc::new(Latch::<H>::new(K, 2).map_err(said)?);

    // Main takes it shared.
    let held_guard = latch.latch(Mode::Shared).map_err(said)?;
    let (arrived, granted) = held_guard.stamps();

    // W takes it exclusive and releases it.
    let theirs = Arc::clone(&latch);
    let w = H::spawn(move || {
        let guard = theirs.latch(Mode::Exclusive).map_err(said)?;
        let (arrived, granted) = guard.stamps();
        let release = guard.release().map_err(said)?;
        Ok::<Request, String>(held(
            QUEUED,
            Mode::Exclusive,
            Kind::Blocking,
            arrived,
            granted,
            release,
        ))
    });

    // Main's two tries, over W once it has queued.
    let mut mine = Vec::new();
    two_tries(&latch, TRIES[0], &mut mine)?;

    // Main releases, then the same two tries again.
    let release = held_guard.release().map_err(said)?;
    mine.push(held(
        MAIN,
        Mode::Shared,
        Kind::Blocking,
        arrived,
        granted,
        release,
    ));
    two_tries(&latch, TRIES[1], &mut mine)?;
    let w_request = H::join(w)??;

    // The monitor (M1–M6) under the budget, and the direct assertion: every
    // try whose arrival lies between W's arrival and its grant was refused.
    let requests: Vec<Request> = mine.iter().copied().chain([w_request]).collect();
    let monitor = check_trace(&requests, B).map_err(|v: Violation| v.to_string());
    let (w_arrived, w_granted) = match w_request.outcome {
        Outcome::Granted { grant, .. } => (w_request.arrived, grant),
        Outcome::Refused => return Err("W's blocking request was refused".to_string()),
    };
    let mut granted_over_w = Vec::new();
    let mut over_w = 0;
    for r in mine.iter().filter(|r| r.kind == Kind::Try) {
        if r.arrived > w_arrived && r.arrived < w_granted {
            over_w += 1;
            if !matches!(r.outcome, Outcome::Refused) {
                granted_over_w.push(format!(
                    "the {:?} try (id {}) arrived at {}, between W's arrival {w_arrived} and \
                     grant {w_granted}, and was granted",
                    r.mode, r.id, r.arrived
                ));
            }
        }
    }
    let direct = if granted_over_w.is_empty() {
        Ok(())
    } else {
        Err(granted_over_w.join("; "))
    };
    let (monitor, direct) = (monitor.err(), direct.err());
    if monitor.is_some() || direct.is_some() {
        return Err(format!(
            "the latch-level variant: monitor {}: direct assertion {} ({over_w} of the tries \
             arrived between W's arrival and its grant)",
            monitor.unwrap_or_else(|| "passed".to_string()),
            direct.unwrap_or_else(|| "passed".to_string())
        ));
    }
    let info = latch.info().map_err(said)?;
    if info.holders != Holders::Free || info.waiting != 0 || info.arrivals != 6 {
        return Err(format!(
            "at the end the latch is {info:?}, where Free, 0 waiting and 6 arrivals are expected"
        ));
    }
    Ok(())
}

/// `t_root_latch_fair_latch_level`: the latch-level variant under
/// `EXHAUSTIVE`, its search complete.
fn latch_level_checked() -> Result<Report, String> {
    check(EXHAUSTIVE, latch_level::<Shuttle>).map_err(|f| format!("{f}"))
}

#[test]
fn t_root_latch_fair_latch_level() {
    let report = latch_level_checked().unwrap();
    println!(
        "t_root_latch_fair_latch_level: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

// --------------------------------------------------------- the frame latch's pin

/// `frame_latch_pinned` (no harness; card A-3b, Work 9): every frame latch
/// carries the pinned bound and a queue for the pool's threads, `o_frame`
/// counts one queue byte per thread, and a guard's stamps rise across
/// arrival, grant and release.
fn pinned() -> Result<(), String> {
    // The pin (S-STORE §5.3: "A-3b pins `K`"; open point OP-1): `K` is 1.
    assert_eq!(FRAME_LATCH_BOUND, 1, "A-3b pins K at 1");
    // And it is the smallest bound that lets a running request take a freed
    // latch while the queue's head wakes (§5.3: strict FIFO handoff "convoys
    // on a write-hot root"): with `K` a later exclusive request is admitted
    // over one queued exclusive, and with `K − 1` it is not. The policy
    // itself answers, with no thread.
    let mut core = LatchCore::new(FRAME_LATCH_BOUND, 4).map_err(said)?;
    if !core.admits(Mode::Shared, Kind::Blocking) {
        return Err("1:S is not admitted on a free latch".to_string());
    }
    core.grant_arrival(Mode::Shared);
    if core.admits(Mode::Exclusive, Kind::Blocking) {
        return Err("2:X is admitted behind a reader".to_string());
    }
    let seq = core.enqueue(Mode::Exclusive).map_err(said)?;
    core.release(Mode::Shared).map_err(said)?;
    if !core.may_grant(seq) {
        return Err("the queued writer is not grantable on the freed latch".to_string());
    }
    if !core.admits(Mode::Exclusive, Kind::Blocking) {
        return Err(
            "with K = FRAME_LATCH_BOUND a later exclusive request is not admitted over the queued \
             writer"
                .to_string(),
        );
    }
    core.grant_arrival(Mode::Exclusive);
    if core.waiting() != 1 || core.may_grant(seq) {
        return Err("the barge did not leave the queued writer waiting".to_string());
    }
    // The core's `Debug` shows the live queue, `queue[0..len]`, and not the
    // stale bytes past it (verdict A-4c0 5ef87b98, follow-up A-4c0-F4 (c)):
    // here the queued writer, overtaken once by the barge.
    let shown = format!("{core:?}");
    if !shown.contains("queue: [X:1]") {
        return Err(format!("the core with one queued writer prints {shown}"));
    }
    core.release(Mode::Exclusive).map_err(said)?;
    if !core.may_grant(seq) {
        return Err("the queued writer is not grantable once the barger left".to_string());
    }
    core.grant_queued(seq).map_err(said)?;
    let shown = format!("{core:?}");
    if !shown.contains("queue: []") {
        return Err(format!(
            "the core whose writer was granted prints {shown}, not an empty queue"
        ));
    }
    // With `K − 1` (0): the same arrival is not admitted.
    let mut fifo = LatchCore::new(FRAME_LATCH_BOUND - 1, 4).map_err(said)?;
    if !fifo.admits(Mode::Shared, Kind::Blocking) {
        return Err("1:S is not admitted on a free latch at K − 1".to_string());
    }
    fifo.grant_arrival(Mode::Shared);
    if fifo.admits(Mode::Exclusive, Kind::Blocking) {
        return Err("2:X is admitted behind a reader at K − 1".to_string());
    }
    fifo.enqueue(Mode::Exclusive).map_err(said)?;
    fifo.release(Mode::Shared).map_err(said)?;
    if fifo.admits(Mode::Exclusive, Kind::Blocking) {
        return Err(
            "with K − 1 a later exclusive request is admitted over the queued writer".to_string(),
        );
    }

    let callers = 3u32;
    let frames = minimum_frames(callers);
    let pool = pool_at_minimum(callers)?;
    if pool.frames() != frames {
        return Err("the pool is not at its minimum".to_string());
    }
    let threads = pool.sizing().threads;
    if threads != callers {
        return Err(format!(
            "{threads} threads are counted where {callers} are expected"
        ));
    }
    // Frames 0 and the last.
    for f in [FrameId(0), FrameId(frames - 1)] {
        let info = pool.latch_info(f).map_err(said)?;
        if (info.bound, info.capacity) != (FRAME_LATCH_BOUND, threads) {
            return Err(format!(
                "frame {f:?}'s latch has bound {} and capacity {}, where {FRAME_LATCH_BOUND} and \
                 {threads} are expected",
                info.bound, info.capacity
            ));
        }
    }
    if !matches!(
        pool.latch_info(FrameId(frames)),
        Err(StoreError::Invalid(_))
    ) {
        return Err("latch_info of a frame past the pool is not Invalid".to_string());
    }

    // `o_frame` grows by exactly one byte per thread.
    let one = pool_at_minimum(1)?.sizing().o_frame;
    if pool.sizing().o_frame.saturating_sub(one) != callers - 1 {
        return Err(format!(
            "o_frame grows by {} between pools for 1 and {callers} callers, where one byte per \
             thread, {}, is expected",
            pool.sizing().o_frame.saturating_sub(one),
            callers - 1
        ));
    }

    // Two guards' stamps strictly increase across arrival, grant and
    // release, and `release` returns a stamp above its grant stamp.
    let (pin, f) = root_page(&pool)?;
    let reader = pin.latch_shared().map_err(said)?;
    let (arrived, granted) = reader.stamps();
    let released = reader.release().map_err(said)?;
    if !(arrived < granted && granted < released) {
        return Err(format!(
            "a shared hold's stamps are {arrived}, {granted}, {released}"
        ));
    }
    let writer = pin.latch_exclusive().map_err(said)?;
    let (arrived2, granted2) = writer.stamps();
    let released2 = writer.release().map_err(said)?;
    if !(released < arrived2 && arrived2 < granted2 && granted2 < released2) {
        return Err(format!(
            "an exclusive hold's stamps are {arrived2}, {granted2}, {released2} after a release at \
             {released}"
        ));
    }
    let info = pool.latch_info(f).map_err(said)?;
    if info.holders != Holders::Free || info.waiting != 0 {
        return Err(format!("after the releases the latch is {info:?}"));
    }
    drop(pin);
    Ok(())
}

#[test]
fn frame_latch_pinned() {
    pinned().unwrap();
}

/// `footprint_at_production_sizes` (exploration `EXHAUSTIVE`, one thread;
/// card A-3b, Work 6: the footprint's `size_of` terms "are taken at
/// `StdPrimitives`, the production backend, whatever `P` the pool has"): a
/// pool on the model checker's backend measures what `Pool::bytes_for`,
/// which knows no backend, computes. A `footprint` that took the latches'
/// size at the pool's own backend measures more on the model checker's,
/// whose primitives are larger (mutant FOOTPRINT-AT-BACKEND).
fn footprint_on_backend<H: Harness>() -> Result<(), String> {
    let cfg = PoolConfig {
        frames: 2,
        page_size: PS,
        callers: 2,
    };
    let pool = fixture::<H>(cfg.frames, cfg.callers)?;
    let computed = Pool::<NoIoFile>::bytes_for(cfg).map_err(said)?;
    let measured = pool.footprint();
    if measured != computed {
        return Err(format!(
            "a pool on the harness's backend measures {measured:?}, where bytes_for computes \
             {computed:?}"
        ));
    }
    Ok(())
}

#[test]
fn footprint_at_production_sizes() {
    let report = check(EXHAUSTIVE, footprint_on_backend::<Shuttle>)
        .map_err(|f| format!("{f}"))
        .unwrap();
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

// ------------------------------------------------------------ the cell's hand-off

/// `harness_cell_handoff` (exploration `EXHAUSTIVE`): a pool of 2 frames for
/// 2 callers and X's root. Main takes X shared and reads byte `MARK`. W pins
/// X, takes it exclusive, calls `mark_dirty(1)`, writes byte `MARK`, and
/// releases. Main releases and joins W. Both must return `Ok`, and main must
/// read W's byte afterwards. The scenario runs twice, once with main
/// releasing by a drop and once by `release`: both paths must free the cell
/// before the latch, or W's grant finds the cell read-held (mutant
/// CELL-RELEASE-ORDER), in some schedule of the search.
fn cell_handoff<H: Harness>(explicit: bool) -> Result<(), String> {
    let pool = Arc::new(fixture::<H>(2, 2)?);
    let (root, f) = root_page(&pool)?;
    let held_guard = root.latch_shared().map_err(said)?;
    if held_guard[MARK] != 0 {
        return Err("a created page is not zeroed at MARK".to_string());
    }

    let theirs = Arc::clone(&pool);
    let w = H::spawn(move || -> Result<(), String> {
        let pin = theirs
            .pin_resident(X)
            .map_err(said)?
            .ok_or("X's root is not resident")?;
        let mut guard = pin.latch_exclusive().map_err(said)?;
        guard.mark_dirty(1).map_err(said)?;
        guard[MARK] = 0x5A;
        guard.release().map_err(said)?;
        drop(pin);
        Ok(())
    });

    if explicit {
        held_guard.release().map_err(said)?;
    } else {
        drop(held_guard);
    }
    H::join(w)??;

    let again = root.latch_shared().map_err(said)?;
    let read = again[MARK];
    again.release().map_err(said)?;
    drop(root);
    if read != 0x5A {
        return Err(format!(
            "main read {read:#x} at MARK after W's write, where 0x5a is expected"
        ));
    }
    let info = pool.latch_info(f).map_err(said)?;
    if info.holders != Holders::Free || info.waiting != 0 {
        return Err(format!(
            "at the end the latch is {info:?}, where Free is expected"
        ));
    }
    Ok(())
}

#[test]
fn harness_cell_handoff() {
    let report = check(EXHAUSTIVE, move || {
        cell_handoff::<Shuttle>(false)?;
        cell_handoff::<Shuttle>(true)
    })
    .map_err(|f| format!("harness_cell_handoff: {f}"))
    .unwrap();
    println!(
        "harness_cell_handoff: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

// ----------------------------------------------------------------- the pool's waits

/// `harness_unheld_pin_drop` (exploration `EXHAUSTIVE`; U1): a pool of 1
/// frame for 2 callers. Main creates `Lpid(1)`, seals it through the created
/// page's guard, calls `mark_written(Ref { slot: 30, epoch: 1, crc })`, which
/// is clean and does no I/O, releases, and keeps its pin `p`. T calls
/// `reserve()`, drops the reservation and returns `Ok`. Main drops `p` and
/// joins T. No order is placed, so the search holds every schedule: T's
/// acquisition before, during and after main's unpin. In those in which T
/// registered as a waiter for the unpin before the unpin came, it is asleep
/// when the frame becomes unheld, and a release that does not wake it
/// (UNPIN-NO-NOTIFY, WAIT-NO-REGISTER) or that moves nothing it re-tests
/// (UNHELD-NO-BUMP-UNPIN) leaves it blocked: a `Deadlock`, at the first such
/// schedule the search meets. In the others the clock takes the clean frame,
/// the latch and the cell included, under the model checker.
///
/// No order is placed on purpose. With T a waiter in every execution, the
/// search holds the interleavings of T's last steps before it sleeps with
/// the unpin's, and those of the clock's long take of the frame after its
/// wake with the unpin's last steps, and the two multiply: that search does
/// not end within the cap (measured: past 100,000 executions).
fn unheld_pin_drop<H: Harness>() -> Result<(), String> {
    let pool = Arc::new(fixture::<H>(1, 2)?);
    let p = created_clean(&pool)?;
    let theirs = Arc::clone(&pool);
    let t = H::spawn(move || -> Result<(), String> {
        let res = theirs.reserve().map_err(said)?;
        drop(res);
        Ok(())
    });
    drop(p);
    H::join(t)??;
    settled(&pool)
}

/// The pool after a wait ended well: its one frame is back on the free
/// list, and nobody waits.
fn settled<H: File, P: Primitives>(pool: &Pool<H, P>) -> Result<(), String> {
    let s = pool.stats().map_err(said)?;
    if (s.free, s.resident, s.reserved, s.unpin_waiters) != (1, 0, 0, 0) {
        return Err(format!(
            "at the end the pool is {s:?}, where one free frame and no waiter are expected"
        ));
    }
    Ok(())
}

#[test]
fn harness_unheld_pin_drop() {
    let report = check(EXHAUSTIVE, unheld_pin_drop::<Shuttle>)
        .map_err(|f| format!("harness_unheld_pin_drop: {f}"))
        .unwrap();
    println!(
        "harness_unheld_pin_drop: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// `harness_unheld_reservation_drop` (exploration `EXHAUSTIVE`; U2): a pool
/// of 1 frame for 2 callers. Main holds `reserve()`'s reservation; T calls
/// `reserve()`; main drops its reservation and joins T. No order is placed:
/// the search holds T's acquisition before main's drop, which takes the
/// frame at once, and after it, which waits for the reservation's return.
fn unheld_reservation_drop<H: Harness>() -> Result<(), String> {
    let pool = Arc::new(fixture::<H>(1, 2)?);
    let held = pool.reserve().map_err(said)?;
    let theirs = Arc::clone(&pool);
    let t = H::spawn(move || -> Result<(), String> {
        let res = theirs.reserve().map_err(said)?;
        drop(res);
        Ok(())
    });
    drop(held);
    H::join(t)??;
    let s = pool.stats().map_err(said)?;
    if (s.free, s.reserved, s.unpin_waiters) != (1, 0, 0) {
        return Err(format!(
            "at the end the pool is {s:?}, where one free frame is expected"
        ));
    }
    Ok(())
}

#[test]
fn harness_unheld_reservation_drop() {
    let report = check(EXHAUSTIVE, unheld_reservation_drop::<Shuttle>)
        .map_err(|f| format!("harness_unheld_reservation_drop: {f}"))
        .unwrap();
    println!(
        "harness_unheld_reservation_drop: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// `harness_unheld_failed_load` (exploration `EXHAUSTIVE`; U3): a pool of 1
/// frame for 2 callers over a [`ModelFile`] that holds, at slot 10,
/// `Lpid(9)`'s page with a byte flipped after sealing; a [`StaticMap`] maps
/// `Lpid(9)` to its reference. Main calls `pin(Lpid(9))`, which must be
/// `Corrupt`; T calls `reserve()` and drops the result. When T waits during
/// main's load, only the load's cleanup frees the frame.
///
/// The order is placed with a [`Gate`] and the pool's hooks: T does not call
/// `reserve()` before main's load has read its slot (`LoadRead`, after the
/// frame is published as being loaded); main goes on with the load only once
/// T's acquisition registered as a waiter for an unpin (`UnpinWait`). So T
/// waits during the failed load in every execution, and a cleanup that does
/// not wake it (CLEANUP-NO-NOTIFY) is a `Deadlock` in every one.
fn unheld_failed_load<H: Harness>() -> Result<(), String> {
    let (mut page, crc) = sealed_page(PageKind::Leaf, 9, 1, 0x22)?;
    // A byte flipped after sealing: the page fails §3.3 against its
    // reference, which still names the sealed bytes' CRC.
    page[200] ^= 0xFF;
    let file = Arc::new(ModelFile::<H>::with(file_with(10, &page)));
    let pool = Arc::new(
        Pool::<ModelFile<H>, H>::new_with_below_minimum_for_tests(
            Arc::clone(&file),
            PoolConfig {
                frames: 1,
                page_size: PS,
                callers: 2,
            },
        )
        .map_err(said)?,
    );
    let map = StaticMap {
        key: PageKey::Lpid(9),
        r: Ref {
            slot: 10,
            epoch: 1,
            crc,
        },
    };
    let gate = Arc::new(Gate::<H>::new());
    pool.set_hook(Some(gate.hook(
        Order {
            hold: Some(HookPoint::LoadRead),
            probe: false,
            register: HookPoint::UnpinWait,
            register_waits: false,
            done: None,
            woken_waits: false,
        },
        None,
    )));

    let theirs = Arc::clone(&pool);
    let theirs_gate = Arc::clone(&gate);
    let t = H::spawn(move || -> Result<(), String> {
        theirs_gate.enter()?;
        let res = theirs.reserve().map_err(said)?;
        drop(res);
        Ok(())
    });
    gate.wait_ready()?;

    let outcome = pool.pin(PageKey::Lpid(9), &map);
    if !matches!(outcome, Err(StoreError::Corrupt(_))) {
        return Err(format!(
            "a pin of the page whose byte was flipped answered {outcome:?}, where Corrupt is \
             expected"
        ));
    }
    H::join(t)??;
    pool.set_hook(None);
    settled(&pool)?;
    let reads = file.reads()?;
    if reads != 1 {
        return Err(format!(
            "the load read the slot {reads} times, where once is expected"
        ));
    }
    Ok(())
}

#[test]
fn harness_unheld_failed_load() {
    let report = check(EXHAUSTIVE, unheld_failed_load::<Shuttle>)
        .map_err(|f| format!("harness_unheld_failed_load: {f}"))
        .unwrap();
    println!(
        "harness_unheld_failed_load: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP})",
        report.executions
    );
    assert!(
        report.executions < EXHAUSTIVE_CAP,
        "the search reached the cap"
    );
}

/// `harness_load_waiter_woken`'s fixture: a pool of 2 frames for 2 callers
/// over a [`ModelFile`] that holds `Lpid(1)`'s good page at slot 10, with
/// marker `0x11`, and a [`StaticMap`]. Main and T each `pin(Lpid(1))` and
/// read the marker. Both must read `0x11`, from one frame, with one read in
/// all. The order is placed with a [`Gate`] and the pool's hooks, as
/// `parked` picks: main holds its load once the slot is read (`LoadRead`)
/// until T, which does not call `pin` before then, registered
/// (`LoadWaiting`), and main's call ends at its `Pinned`, which sets the
/// gate's `done`.
///
/// - The look order (`!parked`; exploration `EXHAUSTIVE`), for A-3a's
///   WAIT-NO-RECHECK: T, registered between its look at the frame and its
///   wait, stays in the hook until main's call is over. So main ends the
///   load after T's look and before T's wait, in every execution; a waiter
///   that waits without re-testing the frame under the mutex waits for a
///   wake-up that came before its wait: a `Deadlock` in every one.
///
/// - The parked order (`parked`; explorations `PCT` and `RANDOM`), for
///   A-3a's LOAD-NO-NOTIFY: T goes on from its registration to take the page
///   table's mutex, look, and wait; main, its hold over, takes that mutex
///   once itself (the gate's probe), which blocks it for as long as T holds
///   it; main then ends the load (P10), which wakes T; and T, woken, holds
///   at `WaitWoken` until main's call is over. T is asleep at P10 in every
///   schedule but those in which main got the mutex first and ended the load
///   before T's wait. A load that does not notify leaves T blocked: a
///   `Deadlock`. This order is sampled, not exhaustive (card A-3b, Work 9,
///   as amended: the look order exhaustive, the parked order sampled under
///   `PCT` and `RANDOM`): its exhaustive search reaches the cap without
///   ending (measured in this order: `NotExhausted` at the cap), because the
///   waiter's last steps before it sleeps interleave with the loader's end
///   of the load, and its long wake with the loader's last steps, and the
///   two multiply. The order places the kill, so the mutant fails in every
///   execution in which T sleeps before P10, most of them; the card's
///   RESULT carries the kill's `kill_evidence` lines at both presets and at
///   a second seed, each at or above the share (S-STORE §15, card S-KAPPA).
fn load_waiter<H: Harness>(parked: bool) -> Result<(), String> {
    let (page, crc) = sealed_page(PageKind::Leaf, 1, 1, 0x11)?;
    let file = Arc::new(ModelFile::<H>::with(file_with(10, &page)));
    let pool = Arc::new(
        Pool::<ModelFile<H>, H>::new_with_below_minimum_for_tests(
            Arc::clone(&file),
            PoolConfig {
                frames: 2,
                page_size: PS,
                callers: 2,
            },
        )
        .map_err(said)?,
    );
    let map = StaticMap {
        key: X,
        r: Ref {
            slot: 10,
            epoch: 1,
            crc,
        },
    };
    let order = if parked {
        Order {
            hold: Some(HookPoint::LoadRead),
            probe: true,
            register: HookPoint::LoadWaiting,
            register_waits: false,
            done: Some(HookPoint::Pinned),
            woken_waits: true,
        }
    } else {
        Order {
            hold: Some(HookPoint::LoadRead),
            probe: false,
            register: HookPoint::LoadWaiting,
            register_waits: true,
            done: Some(HookPoint::Pinned),
            woken_waits: false,
        }
    };
    // The probe: the page table's mutex, through a lookup of X.
    let theirs = Arc::clone(&pool);
    let probe: Option<Probe> = parked.then(|| {
        Arc::new(move || {
            let _ = theirs.frame_of(X);
        }) as Probe
    });
    let gate = Arc::new(Gate::<H>::new());
    pool.set_hook(Some(gate.hook(order, probe)));

    // T waits for the publication, then pins X and reads the marker.
    let theirs = Arc::clone(&pool);
    let theirs_gate = Arc::clone(&gate);
    let t = H::spawn(move || -> Result<(FrameId, u8), String> {
        theirs_gate.enter()?;
        let pin = match theirs.pin(X, &map).map_err(said)? {
            PinOutcome::Pinned(pin) => pin,
            PinOutcome::Unmapped => return Err("T found X unmapped".to_string()),
        };
        let frame = pin.frame();
        let guard = pin.latch_shared().map_err(said)?;
        let marker = guard[MARK];
        guard.release().map_err(said)?;
        drop(pin);
        Ok((frame, marker))
    });
    gate.wait_ready()?;

    // Main loads X.
    let main_pin = match pool.pin(X, &map).map_err(said)? {
        PinOutcome::Pinned(pin) => pin,
        PinOutcome::Unmapped => return Err("main found X unmapped".to_string()),
    };
    let (t_frame, t_marker) = H::join(t)??;
    let guard = main_pin.latch_shared().map_err(said)?;
    let marker = guard[MARK];
    guard.release().map_err(said)?;
    let main_frame = main_pin.frame();
    drop(main_pin);
    pool.set_hook(None);

    if marker != 0x11 || t_marker != 0x11 {
        return Err(format!(
            "main read {marker:#x} and T read {t_marker:#x}, where both must read 0x11"
        ));
    }
    if t_frame != main_frame {
        return Err(format!(
            "main holds X in {main_frame:?} and T in {t_frame:?}, where one frame is expected"
        ));
    }
    let reads = file.reads()?;
    if reads != 1 {
        return Err(format!(
            "the slot was read {reads} times, where once is expected"
        ));
    }
    let s = pool.stats().map_err(said)?;
    if (s.resident, s.loading, s.orphan, s.free) != (1, 0, 0, 1) {
        return Err(format!(
            "at the end the pool is {s:?}, where one resident page and one free frame are expected"
        ));
    }
    Ok(())
}

/// `harness_load_waiter_woken`: the look order under `EXHAUSTIVE`, its search
/// complete, and the parked order under `PCT` and under `RANDOM`.
#[test]
fn harness_load_waiter_woken() {
    let look = check(EXHAUSTIVE, move || load_waiter::<Shuttle>(false))
        .map_err(|f| format!("harness_load_waiter_woken, the look order: {f}"))
        .unwrap();
    let pct = check(PCT, move || load_waiter::<Shuttle>(true))
        .map_err(|f| format!("harness_load_waiter_woken, the parked order under PCT: {f}"))
        .unwrap();
    let random = check(RANDOM, move || load_waiter::<Shuttle>(true))
        .map_err(|f| format!("harness_load_waiter_woken, the parked order under RANDOM: {f}"))
        .unwrap();
    println!(
        "harness_load_waiter_woken: EXHAUSTIVE (the look order) {} executions (cap {}); the parked order under PCT {} and under RANDOM {} executions",
        look.executions, EXHAUSTIVE_CAP, pct.executions, random.executions
    );
    assert!(
        look.executions < EXHAUSTIVE_CAP,
        "the look order's search reached the cap"
    );
    assert_eq!(pct.executions, 1_000, "PCT's executions");
    assert_eq!(random.executions, 1_000, "RANDOM's executions");
}

// ------------------------------------------------- the sync layer's try-lock

/// `Mutex::try_lock` on both backends (card A-3b, Work 5's one addition to
/// the sync layer): free answers a guard, held answers `None`, and poisoned
/// answers `Poisoned`, never a guard (F5).
fn try_lock_ops<P: Primitives>() -> Result<(), String> {
    let mutex = Mutex::<P, u32>::new(0);
    let free = mutex.try_lock().map_err(said)?;
    if free.is_none() {
        return Err("try_lock of a free mutex answered None".to_string());
    }
    drop(free);
    let mut held = mutex.lock().map_err(said)?;
    if mutex.try_lock().map_err(said)?.is_some() {
        return Err("try_lock of a held mutex answered a guard".to_string());
    }
    *held = 7;
    drop(held);
    match mutex.try_lock().map_err(said)? {
        Some(guard) if *guard == 7 => Ok(()),
        Some(guard) => Err(format!("try_lock answered a guard over {}", *guard)),
        None => Err("try_lock after the drop answered None".to_string()),
    }
}

#[test]
fn sync_mutex_try_lock_std() {
    try_lock_ops::<StdPrimitives>().unwrap();
    // Poisoned: a thread panics while holding the mutex; a try then answers
    // `Poisoned`, twice, and never a guard.
    let mutex = Arc::new(Mutex::<StdPrimitives, u32>::new(0));
    let theirs = Arc::clone(&mutex);
    let handle = std::thread::spawn(move || {
        let _held = theirs.lock().unwrap();
        panic!("a panic under the platform's mutex, before a try");
    });
    assert!(
        handle.join().is_err(),
        "the thread's panic is returned by its join"
    );
    assert!(
        matches!(mutex.try_lock(), Err(StoreError::Poisoned(_))),
        "try_lock after the panic"
    );
    assert!(
        matches!(mutex.try_lock(), Err(StoreError::Poisoned(_))),
        "a second try_lock"
    );
}

/// `sync_mutex_try_lock_model`: a model thread holds the mutex across a
/// yield, drops it and tries again; main tries meanwhile, so that the search
/// holds both answers, held and free; and after the join main's try is
/// granted and reads the written value.
fn try_lock_model() -> Result<Report, String> {
    let seen_none = Arc::new(AtomicUsize::new(0));
    let seen_some = Arc::new(AtomicUsize::new(0));
    let (none_in, some_in) = (Arc::clone(&seen_none), Arc::clone(&seen_some));
    let report = check(EXHAUSTIVE, move || {
        let mutex = Arc::new(Mutex::<Shuttle, u32>::new(0));
        let theirs = Arc::clone(&mutex);
        let handle = Shuttle::spawn(move || -> Result<(), String> {
            let mut held = theirs.lock().map_err(said)?;
            yield_now::<Shuttle>();
            *held = 7;
            drop(held);
            match theirs.try_lock().map_err(said)? {
                Some(guard) if *guard == 7 => Ok(()),
                Some(guard) => Err(format!("a try after the drop reads {}", *guard)),
                // Main's try may hold the mutex: then the try is busy.
                None => Ok(()),
            }
        });
        match mutex.try_lock().map_err(said)? {
            Some(guard) => {
                // Before T's write the value is 0; once T is done, 7.
                if *guard != 0 && *guard != 7 {
                    return Err(format!("main's try read {}", *guard));
                }
                some_in.fetch_add(1, Ordering::SeqCst);
            }
            None => {
                none_in.fetch_add(1, Ordering::SeqCst);
            }
        }
        Shuttle::join(handle)??;
        match mutex.try_lock().map_err(said)? {
            Some(guard) if *guard == 7 => Ok(()),
            Some(guard) => Err(format!("main's try after the join reads {}", *guard)),
            None => Err("main's try after the join answered None".to_string()),
        }
    })
    .map_err(|f| format!("sync_mutex_try_lock_model: {f}"))?;
    println!(
        "sync_mutex_try_lock_model: EXHAUSTIVE, {} executions (cap {EXHAUSTIVE_CAP}); main's try \
         answered None in {} and a guard in {}",
        report.executions,
        seen_none.load(Ordering::SeqCst),
        seen_some.load(Ordering::SeqCst)
    );
    if report.executions >= EXHAUSTIVE_CAP {
        return Err("the search reached the cap".to_string());
    }
    if seen_none.load(Ordering::SeqCst) == 0 || seen_some.load(Ordering::SeqCst) == 0 {
        return Err("the search did not see both answers of a try".to_string());
    }
    Ok(report)
}

#[test]
fn sync_mutex_try_lock_model() {
    try_lock_model().unwrap();
}
