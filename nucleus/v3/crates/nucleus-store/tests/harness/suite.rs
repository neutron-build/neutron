//! One latch interface and the scenarios every latch runs (card A-4c0 Work
//! 7; S-STORE §5.3 "Fair latches").
//!
//! [`LatchUnderTest`] is implemented for [`Latch`] over any [`Harness`], by
//! the harness's planted-bug toys, and later by any latch type a card holds
//! to this suite. Each scenario is a fixed script: it builds its latch with
//! the bound `K` and with its participant count, the main thread included, as
//! the capacity (the capacity rule of S-STORE §5.3); every participant
//! records its own requests and returns them through [`Harness::join`]; the
//! main thread merges them and the monitor (M1–M6) checks the trace. A
//! scenario uses no other synchronisation than the latch and, for its
//! threads, the harness.
//!
//! S3, S5–S7 and S9 are small enough to search exhaustively.

use std::sync::Arc;

use nucleus_store::StoreError;
use nucleus_store::latch::{Holders, Kind, Latch, LatchGuard, Mode, TryLatch};
use nucleus_store::sync::{Condvar, Mutex};

use crate::harness::Harness;
use crate::harness::monitor::{Id, Outcome, Request, check_trace};

/// The stamps of one acquire (W2): the arrival's and the grant's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Stamps {
    /// The arrival's stamp.
    pub arrived: u64,
    /// The grant's stamp.
    pub granted: u64,
}

/// What a try returned (W5).
#[derive(Debug)]
pub enum TryOutcome<G> {
    /// The latch is held by `G`, arrived at and granted at the two stamps.
    Granted(G, Stamps),
    /// The latch was not taken; the value is the arrival's stamp.
    Refused(u64),
}

/// A latch held to this suite: a blocking acquire, a try acquire and a
/// release, each returning the latch's stamps.
pub trait LatchUnderTest<H: Harness>: Send + Sync + 'static {
    /// What a hold gives its holder, until the release.
    type Guard<'a>;

    /// Blocks for the latch in `mode`, and returns the hold with its stamps.
    fn latch(&self, mode: Mode) -> Result<(Self::Guard<'_>, Stamps), String>;
    /// Tries the latch in `mode` without waiting.
    fn try_latch(&self, mode: Mode) -> Result<TryOutcome<Self::Guard<'_>>, String>;
    /// Releases a hold, and returns the release's stamp.
    fn release(guard: Self::Guard<'_>) -> Result<u64, String>;
}

impl<H: Harness> LatchUnderTest<H> for Latch<H> {
    type Guard<'a> = LatchGuard<'a, H>;

    fn latch(&self, mode: Mode) -> Result<(Self::Guard<'_>, Stamps), String> {
        let guard = self.latch(mode).map_err(|e| e.to_string())?;
        let (arrived, granted) = guard.stamps();
        Ok((guard, Stamps { arrived, granted }))
    }

    fn try_latch(&self, mode: Mode) -> Result<TryOutcome<Self::Guard<'_>>, String> {
        match self.try_latch(mode).map_err(|e| e.to_string())? {
            TryLatch::Granted(guard) => {
                let (arrived, granted) = guard.stamps();
                Ok(TryOutcome::Granted(guard, Stamps { arrived, granted }))
            }
            TryLatch::Refused(arrived) => Ok(TryOutcome::Refused(arrived)),
        }
    }

    fn release(guard: Self::Guard<'_>) -> Result<u64, String> {
        guard.release().map_err(|e| e.to_string())
    }
}

/// Builds a latch for a scenario from its bound and its capacity.
pub type NewLatch<L> = fn(k: u32, capacity: u32) -> Result<L, String>;

/// The suite's constructor for [`Latch`]: `K` and the capacity as given.
pub fn new_latch<H: Harness>(k: u32, capacity: u32) -> Result<Latch<H>, String> {
    Latch::new(k, capacity).map_err(|e| e.to_string())
}

/// One step of a participant's script: a blocking acquire, released at a
/// later step, or a try, released later if it is granted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op {
    /// A blocking acquire in `mode`.
    Hold(Mode),
    /// A try in `mode`.
    TryHold(Mode),
}

/// Each participant's ids start at a multiple of this, so that no two
/// requests share an id (M6).
const IDS_PER_PARTICIPANT: Id = 8;

/// Runs one participant's script against `latch`, and returns its requests.
/// The script's ops are numbered from `first_step`.
fn actor_from<H: Harness, L: LatchUnderTest<H>>(
    latch: Arc<L>,
    participant: usize,
    ops: &'static [Op],
    first_step: usize,
) -> Result<Vec<Request>, String> {
    let mut requests = Vec::new();
    for (n, op) in ops.iter().enumerate() {
        let id = participant * IDS_PER_PARTICIPANT + first_step + n;
        match op {
            Op::Hold(mode) => {
                let (guard, stamps) = latch.latch(*mode)?;
                let release = L::release(guard)?;
                requests.push(Request {
                    id,
                    mode: *mode,
                    kind: Kind::Blocking,
                    arrived: stamps.arrived,
                    outcome: Outcome::Granted {
                        grant: stamps.granted,
                        release: Some(release),
                    },
                });
            }
            Op::TryHold(mode) => match latch.try_latch(*mode)? {
                TryOutcome::Granted(guard, stamps) => {
                    let release = L::release(guard)?;
                    requests.push(Request {
                        id,
                        mode: *mode,
                        kind: Kind::Try,
                        arrived: stamps.arrived,
                        outcome: Outcome::Granted {
                            grant: stamps.granted,
                            release: Some(release),
                        },
                    });
                }
                TryOutcome::Refused(arrived) => requests.push(Request {
                    id,
                    mode: *mode,
                    kind: Kind::Try,
                    arrived,
                    outcome: Outcome::Refused,
                }),
            },
        }
    }
    Ok(requests)
}

/// A granted blocking request, as its thread records it once it releases.
fn held(id: Id, mode: Mode, arrived: u64, granted: u64, release: u64) -> Request {
    Request {
        id,
        mode,
        kind: Kind::Blocking,
        arrived,
        outcome: Outcome::Granted {
            grant: granted,
            release: Some(release),
        },
    }
}

/// Builds the latch with `k` and its participant count as the capacity
/// (the capacity rule), spawns one thread per script in `spawned`, runs
/// `main` on the main thread, joins them, and returns the latch with every
/// participant's requests. The main thread is participant 0.
fn scene<H: Harness, L: LatchUnderTest<H>>(
    new: NewLatch<L>,
    k: u32,
    main: &'static [Op],
    spawned: &[&'static [Op]],
) -> Result<(Arc<L>, Vec<Vec<Request>>), String> {
    let latch = Arc::new(new(k, (spawned.len() + 1) as u32)?);
    let handles = spawn_all::<H, L>(Arc::clone(&latch), spawned);
    let mut all = vec![actor_from::<H, L>(Arc::clone(&latch), 0, main, 0)?];
    join_all::<H>(handles, &mut all)?;
    Ok((latch, all))
}

/// A scene whose text has main take the latch before the others act, as
/// "main takes Shared. W takes Exclusive … Main releases": main takes
/// `first`, the others are spawned, main releases, then main runs `after`.
fn scene_main_first<H: Harness, L: LatchUnderTest<H>>(
    new: NewLatch<L>,
    k: u32,
    first: Mode,
    after: &'static [Op],
    spawned: &[&'static [Op]],
) -> Result<(Arc<L>, Vec<Vec<Request>>), String> {
    let latch = Arc::new(new(k, (spawned.len() + 1) as u32)?);
    let (guard, stamps) = latch.latch(first)?;
    let handles = spawn_all::<H, L>(Arc::clone(&latch), spawned);
    let release = L::release(guard)?;
    let mut mine = vec![held(0, first, stamps.arrived, stamps.granted, release)];
    mine.extend(actor_from::<H, L>(Arc::clone(&latch), 0, after, 1)?);
    let mut all = vec![mine];
    join_all::<H>(handles, &mut all)?;
    Ok((latch, all))
}

/// Spawns one thread per script; the main thread is participant 0.
fn spawn_all<H: Harness, L: LatchUnderTest<H>>(
    latch: Arc<L>,
    spawned: &[&'static [Op]],
) -> Vec<H::Handle<Result<Vec<Request>, String>>> {
    let mut handles = Vec::with_capacity(spawned.len());
    for (n, ops) in spawned.iter().copied().enumerate() {
        let latch = Arc::clone(&latch);
        handles.push(H::spawn(move || actor_from::<H, L>(latch, n + 1, ops, 0)));
    }
    handles
}

/// Joins every spawned thread and appends its requests.
fn join_all<H: Harness>(
    handles: Vec<H::Handle<Result<Vec<Request>, String>>>,
    all: &mut Vec<Vec<Request>>,
) -> Result<(), String> {
    for handle in handles {
        all.push(H::join(handle)??);
    }
    Ok(())
}

/// The monitor's verdict on a scene's requests.
fn checked(all: Vec<Vec<Request>>, k: u32) -> Result<(), String> {
    let requests: Vec<Request> = all.into_iter().flatten().collect();
    check_trace(&requests, k).map_err(|v| v.to_string())
}

/// The scenarios, by name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Scenario {
    /// S1, reader stream: main takes Shared. W takes Exclusive. R1 and R2
    /// each take Shared. Main releases. Every participant releases what it
    /// holds. S1 has no try on purpose, so that a reader-preferring latch
    /// breaks M2 alone.
    S1,
    /// S2, writer stream: main takes Exclusive. Rd takes Shared. W1, W2 and
    /// W3 each take Exclusive. Main releases.
    S2,
    /// S3, try at the release instant: main takes Shared. W takes Exclusive.
    /// Main releases, then makes `try_latch(Shared)` and `try_latch(Exclusive)`,
    /// releasing what it is granted. Main joins W.
    S3,
    /// S4, barging writers: main takes Shared. W takes Exclusive. B takes and
    /// releases Exclusive three times. Main releases.
    S4,
    /// S5, two writers: main spawns A, which takes and releases Exclusive,
    /// then itself takes and releases Exclusive, then joins A.
    S5,
    /// S6, writer behind a reader: main takes Shared. W takes and releases
    /// Exclusive. Main releases and joins W.
    S6,
    /// S7, two readers: main spawns A. Both take and release Shared. After
    /// the join, main's `try_latch(Exclusive)` must be granted.
    S7,
    /// S8, mixed: three spawned participants with the scripts
    /// `[Shared, Exclusive]`, `[Exclusive, try Shared]` and
    /// `[try Exclusive, Shared]`, while main takes and releases Shared once.
    S8,
    /// S9, two queued writers: main takes Shared. A and B each take and
    /// release Exclusive. Main releases and joins both. The search includes
    /// the schedules in which A and B are both queued when main releases.
    S9,
}

impl Scenario {
    /// Every scenario, in order.
    pub const ALL: [Scenario; 9] = [
        Scenario::S1,
        Scenario::S2,
        Scenario::S3,
        Scenario::S4,
        Scenario::S5,
        Scenario::S6,
        Scenario::S7,
        Scenario::S8,
        Scenario::S9,
    ];

    /// Its name.
    pub fn name(self) -> &'static str {
        match self {
            Scenario::S1 => "S1",
            Scenario::S2 => "S2",
            Scenario::S3 => "S3",
            Scenario::S4 => "S4",
            Scenario::S5 => "S5",
            Scenario::S6 => "S6",
            Scenario::S7 => "S7",
            Scenario::S8 => "S8",
            Scenario::S9 => "S9",
        }
    }
}

/// Runs scenario `scenario` on a latch that `new` builds, with bound `k`, and
/// returns the monitor's verdict or a participant's error.
pub fn run<H: Harness, L: LatchUnderTest<H>>(
    scenario: Scenario,
    new: NewLatch<L>,
    k: u32,
) -> Result<(), String> {
    const HOLD_S: Op = Op::Hold(Mode::Shared);
    const HOLD_X: Op = Op::Hold(Mode::Exclusive);
    const TRY_S: Op = Op::TryHold(Mode::Shared);
    const TRY_X: Op = Op::TryHold(Mode::Exclusive);
    // Scenes whose text has main take the latch first (S1–S4, S6, S9); S5,
    // S7 and S8 spawn first, as theirs says.
    let (latch, mut all) = match scenario {
        Scenario::S1 => scene_main_first::<H, L>(
            new,
            k,
            Mode::Shared,
            &[],
            &[&[HOLD_X], &[HOLD_S], &[HOLD_S]],
        )?,
        Scenario::S2 => scene_main_first::<H, L>(
            new,
            k,
            Mode::Exclusive,
            &[],
            &[&[HOLD_S], &[HOLD_X], &[HOLD_X], &[HOLD_X]],
        )?,
        Scenario::S3 => {
            scene_main_first::<H, L>(new, k, Mode::Shared, &[TRY_S, TRY_X], &[&[HOLD_X]])?
        }
        Scenario::S4 => scene_main_first::<H, L>(
            new,
            k,
            Mode::Shared,
            &[],
            &[&[HOLD_X], &[HOLD_X, HOLD_X, HOLD_X]],
        )?,
        Scenario::S5 => scene::<H, L>(new, k, &[HOLD_X], &[&[HOLD_X]])?,
        Scenario::S6 => scene_main_first::<H, L>(new, k, Mode::Shared, &[], &[&[HOLD_X]])?,
        // S7's try comes after the join, so it is not in main's script.
        Scenario::S7 => scene::<H, L>(new, k, &[HOLD_S], &[&[HOLD_S]])?,
        Scenario::S8 => scene::<H, L>(
            new,
            k,
            &[HOLD_S],
            &[&[HOLD_S, HOLD_X], &[HOLD_X, TRY_S], &[TRY_X, HOLD_S]],
        )?,
        Scenario::S9 => {
            scene_main_first::<H, L>(new, k, Mode::Shared, &[], &[&[HOLD_X], &[HOLD_X]])?
        }
    };
    if scenario == Scenario::S7 {
        // After the join, main's `try_latch(Exclusive)` must be granted: an
        // `Err` otherwise. It is main's second request, numbered from 1.
        let more = actor_from::<H, L>(Arc::clone(&latch), 0, &[TRY_X], 1)?;
        let request = more
            .first()
            .ok_or("S7: the main thread recorded one request too few")?;
        if !matches!(request.outcome, Outcome::Granted { .. }) {
            return Err(format!(
                "S7: main's try_latch(Exclusive) after both readers released was not granted: {request:?}"
            ));
        }
        let main = all
            .first_mut()
            .ok_or("S7: the main thread recorded nothing")?;
        main.extend(more);
    }
    checked(all, k)
}

/// The scenario of `latch_model_overflow` (card A-4c0 Work 8): a latch of
/// capacity 1 that main holds exclusively while two threads ask it shared.
/// While main holds, one of the two is queued and the other overflows in
/// every schedule; the overflow is reported, never absorbed.
///
/// The error count is kept under the mutex that main waits with, not in an
/// atomic of its own beside it. That is a judgment of this card's
/// implementation: with the count in an atomic and the mutex only for the
/// wait, the exhaustive search is 467,558 executions, past the cap, where
/// this form's is 7,780 (both measured on this scenario on
/// `Latch<Shuttle>` for K in {0, 1}), and the test passes only with the
/// search complete.
pub fn overflow<H: Harness>(k: u32) -> Result<(), String> {
    let latch = Arc::new(Latch::<H>::new(k, 1).map_err(|e| e.to_string())?);
    // The error count, under the mutex with the condition variable main
    // waits on. A notify made under that mutex cannot be missed between
    // main's check and its wait.
    let reported = Arc::new((Mutex::<H, u64>::new(0), Condvar::<H>::new()));

    // Main takes the latch exclusive.
    let hold = latch.latch(Mode::Exclusive).map_err(|e| e.to_string())?;
    if hold.stamps() != (1, 2) {
        return Err(format!(
            "main's stamps are {:?}, where (1, 2) are expected",
            hold.stamps()
        ));
    }

    // A and B each make `latch(Shared)`.
    let mut handles = Vec::with_capacity(2);
    for _ in 0..2 {
        let latch = Arc::clone(&latch);
        let reported = Arc::clone(&reported);
        // `true` if the request overflowed the queue.
        handles.push(H::spawn(move || -> Result<bool, String> {
            match latch.latch(Mode::Shared) {
                Ok(guard) => {
                    guard.release().map_err(|e| e.to_string())?;
                    Ok(false)
                }
                Err(error) => {
                    // W3: an overflow is `Invalid`, names the capacity, and
                    // changed nothing. The count is raised whatever the
                    // error is, so main goes on and a wrong error reaches it
                    // at the join, not as a wait that never ends.
                    let mut count = reported.0.lock().map_err(|e| e.to_string())?;
                    *count += 1;
                    reported.1.notify_all();
                    drop(count);
                    let text = error.to_string();
                    if !matches!(error, StoreError::Invalid(_)) || !text.contains("capacity") {
                        return Err(format!(
                            "a full queue returned {text}, not `Invalid` naming the capacity"
                        ));
                    }
                    Ok(true)
                }
            }
        }));
    }

    // Main waits on the condition variable until the count is at least 1,
    // with no spin.
    {
        let mut count = reported.0.lock().map_err(|e| e.to_string())?;
        while *count == 0 {
            count = reported.1.wait(count).map_err(|e| e.to_string())?;
        }
    }

    // Both requests have arrived: the first is queued and the second
    // overflowed.
    let info = latch.info().map_err(|e| e.to_string())?;
    if info.waiting != 1 || info.arrivals != 2 {
        return Err(format!(
            "with main holding, {} wait and {} arrived, where 1 and 2 are expected",
            info.waiting, info.arrivals
        ));
    }
    // The overflow took no tick and no arrival (W2, W3): main's stamps are 1
    // and 2, the queued request's arrival is 3, and this release is 4.
    let release = hold.release().map_err(|e| e.to_string())?;
    if release != 4 {
        return Err(format!(
            "main's release is stamped {release}, where 4 is expected"
        ));
    }

    let mut overflowed = 0;
    for handle in handles {
        if H::join(handle)?? {
            overflowed += 1;
        }
    }
    if overflowed != 1 {
        return Err(format!(
            "{overflowed} requests overflowed, where exactly 1 is expected"
        ));
    }
    let info = latch.info().map_err(|e| e.to_string())?;
    if info.holders != Holders::Free || info.waiting != 0 || info.arrivals != 2 {
        return Err(format!(
            "after the joins, {info:?}, where Free, 0 waiting and 2 arrivals are expected"
        ));
    }
    match latch
        .try_latch(Mode::Exclusive)
        .map_err(|e| e.to_string())?
    {
        TryLatch::Granted(guard) => {
            guard.release().map_err(|e| e.to_string())?;
        }
        TryLatch::Refused(stamp) => {
            return Err(format!(
                "try_latch(Exclusive) on a free latch was refused at {stamp}"
            ));
        }
    }
    Ok(())
}
