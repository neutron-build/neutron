//! C-SIM: the deterministic simulator of the real txn layer (§11). The real
//! `nucleus-txn` code over `MemKv` (LSM mode behind `Fault`), driven by a
//! seeded **single-threaded** scheduler: no threads, no parkers, no wall
//! clocks (the `/sys/ts_clock` clock is the step counter). Every actor step
//! is a call into the crate's public step APIs; after every step and at
//! every `CommitProbe` point the invariants in `check.rs` are checked against
//! ghost state.
//!
//! A run is a sequence of **boot eras** (the card's lifetime note): each era
//! owns its `Core`; the RR/SER snapshot guards borrow it and live on the era
//! driver's stack, so a crash ends the era (sessions die, as in a real
//! crash) and the next era reopens the store.

pub mod check;
pub mod ghost;
pub mod kv;
pub mod min;
pub mod sched;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use nucleus_txn::boot::Core;
use nucleus_txn::commit::{
    Clock, CommitConfig, CommitPipeline, CommitProbe, CommitTicket, ProbePoint, SyncCommit,
};
use nucleus_txn::encoding::end_key;
use nucleus_txn::gc::{GcConfig, GcJob};
use nucleus_txn::locks::LockManager;
use nucleus_txn::read::{self, NoSsi};
use nucleus_txn::registry::SnapshotGuard;
use nucleus_txn::resolver::Resolver;
use nucleus_txn::ssi::Ssi;
use nucleus_txn::txn::{CancelHandle, Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::wait::{WaitBegin, WaitHandle, WaitOutcome};
use nucleus_txn::write::{
    EpqDecision, EpqRequest, KeyOpTask, RowOp, RowOpTask, RowOutcome, Step, StmtCtx, UniqueRule,
};
use nucleus_txn::{RowLockMode, Seq, Ts, TxnError, TxnId, TxnStatus};

pub use ghost::{payload_u64, u64_payload, GWrite, Ghost};
pub use kv::SimKv;
pub use sched::{Choices, Rng, Trace};

use check::Checker;

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// One run's terminal answer. (`sim.rs` reads the `Ok` payload and every
/// `Violation` field; `sim_regressions.rs` matches only the variant it
/// replays, so the unread fields are allowed dead there.)
#[allow(dead_code)]
pub enum RunResult {
    /// The run finished (or hit its step bound) with every check green.
    Ok(Box<Finished>),
    /// An invariant was violated: the run stops; the seed, config, trace
    /// and choice list are attached.
    Violation(Box<Violation>),
}

#[allow(dead_code)]
pub struct Finished {
    pub steps: u64,
    pub choices: Vec<u64>,
    pub trace: String,
    pub probe_points: u64,
}

#[derive(Clone)]
#[allow(dead_code)]
pub struct Violation {
    pub inv: &'static str,
    pub detail: String,
    pub seed: u64,
    pub config: &'static str,
    pub step: u64,
    pub choices: Vec<u64>,
    pub trace: String,
}

// ---------------------------------------------------------------------------
// Configuration and programs
// ---------------------------------------------------------------------------

/// Statement mix weights, in per-mille of statements.
#[derive(Debug, Clone)]
pub struct OpWeights {
    pub read: usize,
    pub scan: usize,
    pub update: usize,
    pub delete: usize,
    pub insert: usize,
    pub lock_update: usize,
    pub lock_key_share: usize,
    pub savepoint: usize,
    pub rollback_to: usize,
}

impl OpWeights {
    fn total(&self) -> usize {
        self.read
            + self.scan
            + self.update
            + self.delete
            + self.insert
            + self.lock_update
            + self.lock_key_share
            + self.savepoint
            + self.rollback_to
    }
}

/// One simulator configuration (work item 8).
#[derive(Debug, Clone)]
pub struct Config {
    pub name: &'static str,
    pub sessions: usize,
    /// 3..=6 logical keys.
    pub keys: usize,
    /// Per-session isolation (round-robin).
    pub isolation: Vec<Isolation>,
    pub txns_per_session: usize,
    /// 1..=4 statements per txn.
    pub max_stmts: usize,
    pub ops: OpWeights,
    /// `synchronous_commit = on` probability, per mille.
    pub sync_on_pm: usize,
    /// Probability a txn cancels itself mid-wait, per mille (1 in 8 = 125).
    pub cancel_pm: usize,
    /// Simulated `deadlock_timeout`: scheduler steps after which a wait's
    /// deadlock check becomes enabled.
    pub deadlock_after: u64,
    /// Probability the commit-thread probe yields to other actors at a
    /// probe point, per mille.
    pub yield_pm: usize,
    /// Maximum other-actor steps the probe runs at one point.
    pub yield_steps: usize,
    /// Crashes allowed in the run (0, 1 or 2).
    pub crashes: usize,
    /// Whether the GC actor runs.
    pub gc: bool,
    /// Whether I-SER is checked (every session SERIALIZABLE).
    pub check_i_ser: bool,
    /// Probability of a two-lock program in per-session-opposite orders
    /// (deadlocks).
    pub lock_pairs_pm: usize,
    pub max_steps: u64,
    /// I-PROGRESS bound R.
    pub progress_r: u32,
}

impl Config {
    /// The named suite configuration (work item 8). Both test binaries
    /// (`sim.rs`, `sim_regressions.rs`) build configs through this, so a
    /// replayed schedule always meets the configuration it was recorded on.
    pub fn named(name: &str) -> Config {
        match name {
            "rc_mix" => Config {
                name: "rc_mix",
                sessions: 3,
                keys: 4,
                isolation: vec![Isolation::ReadCommitted],
                txns_per_session: 3,
                max_stmts: 3,
                ops: OpWeights {
                    read: 200,
                    scan: 100,
                    update: 250,
                    delete: 100,
                    insert: 100,
                    lock_update: 50,
                    lock_key_share: 100,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 500,
                cancel_pm: 125,
                deadlock_after: 6,
                yield_pm: 300,
                yield_steps: 2,
                crashes: 0,
                gc: false,
                check_i_ser: false,
                lock_pairs_pm: 0,
                max_steps: 2000,
                progress_r: 8,
            },
            "rr_mix" => Config {
                name: "rr_mix",
                sessions: 3,
                keys: 4,
                isolation: vec![Isolation::RepeatableRead],
                txns_per_session: 3,
                max_stmts: 3,
                ops: OpWeights {
                    read: 150,
                    scan: 100,
                    update: 200,
                    delete: 100,
                    insert: 50,
                    lock_update: 50,
                    lock_key_share: 250,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 500,
                cancel_pm: 125,
                deadlock_after: 6,
                yield_pm: 300,
                yield_steps: 2,
                crashes: 0,
                gc: false,
                check_i_ser: false,
                lock_pairs_pm: 0,
                max_steps: 2000,
                progress_r: 8,
            },
            "ser_only" => Config {
                name: "ser_only",
                sessions: 3,
                keys: 4,
                isolation: vec![Isolation::Serializable],
                txns_per_session: 4,
                max_stmts: 3,
                ops: OpWeights {
                    read: 250,
                    scan: 150,
                    update: 250,
                    delete: 100,
                    insert: 100,
                    lock_update: 0,
                    lock_key_share: 50,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 500,
                cancel_pm: 125,
                deadlock_after: 6,
                yield_pm: 300,
                yield_steps: 2,
                crashes: 0,
                gc: false,
                check_i_ser: true,
                lock_pairs_pm: 0,
                max_steps: 2000,
                progress_r: 8,
            },
            "locks_heavy" => Config {
                name: "locks_heavy",
                sessions: 4,
                keys: 3,
                isolation: vec![
                    Isolation::ReadCommitted,
                    Isolation::RepeatableRead,
                    Isolation::ReadCommitted,
                    Isolation::RepeatableRead,
                ],
                txns_per_session: 4,
                max_stmts: 2,
                ops: OpWeights {
                    read: 50,
                    scan: 0,
                    update: 150,
                    delete: 50,
                    insert: 0,
                    lock_update: 350,
                    lock_key_share: 300,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 500,
                cancel_pm: 125,
                deadlock_after: 4,
                yield_pm: 200,
                yield_steps: 2,
                crashes: 0,
                gc: false,
                check_i_ser: false,
                lock_pairs_pm: 400,
                max_steps: 2000,
                progress_r: 8,
            },
            "crash" => Config {
                name: "crash",
                sessions: 2,
                keys: 4,
                isolation: vec![Isolation::ReadCommitted, Isolation::RepeatableRead],
                txns_per_session: 4,
                max_stmts: 3,
                ops: OpWeights {
                    read: 150,
                    scan: 50,
                    update: 250,
                    delete: 150,
                    insert: 150,
                    lock_update: 100,
                    lock_key_share: 50,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 600,
                cancel_pm: 125,
                deadlock_after: 6,
                yield_pm: 200,
                yield_steps: 2,
                crashes: 2,
                gc: false,
                check_i_ser: false,
                lock_pairs_pm: 0,
                max_steps: 2000,
                progress_r: 8,
            },
            "gc" => Config {
                name: "gc",
                sessions: 3,
                keys: 4,
                isolation: vec![
                    Isolation::RepeatableRead,
                    Isolation::ReadCommitted,
                    Isolation::RepeatableRead,
                ],
                txns_per_session: 5,
                max_stmts: 4,
                ops: OpWeights {
                    read: 200,
                    scan: 150,
                    update: 150,
                    delete: 200,
                    insert: 150,
                    lock_update: 0,
                    lock_key_share: 50,
                    savepoint: 50,
                    rollback_to: 50,
                },
                sync_on_pm: 700,
                cancel_pm: 125,
                deadlock_after: 6,
                yield_pm: 200,
                yield_steps: 2,
                crashes: 0,
                gc: true,
                check_i_ser: false,
                lock_pairs_pm: 0,
                max_steps: 2000,
                progress_r: 8,
            },
            other => panic!("unknown C-SIM configuration {other:?}"),
        }
    }
}

/// One statement of a generated program. The index is into the key space.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stmt {
    Read(usize),
    /// `[lo, hi)` over key indices.
    Scan(usize, usize),
    Update(usize),
    Delete(usize),
    Insert(usize, u64),
    LockUpdate(usize),
    LockKeyShare(usize),
    Savepoint,
    RollbackTo,
}

pub struct Program {
    pub stmts: Vec<Stmt>,
    /// The txn cancels itself while waiting (1 in 8).
    pub cancel_mid_wait: bool,
    /// `false`: the txn aborts instead of committing.
    pub commit: bool,
    pub sync: SyncCommit,
}

impl Program {
    pub fn stmt(&self, i: usize) -> Stmt {
        self.stmts[i]
    }
}

/// Program generation draws from the **aux** stream (see `sched.rs`): it is
/// not part of the recorded choice list, so minimizing a schedule does not
/// reshuffle the programs it replays.
fn gen_program(cfg: &Config, session: usize, rng: &mut Rng) -> Program {
    let nk = cfg.keys;
    // Deadlock bias: two FOR UPDATE locks in per-session-opposite orders.
    if cfg.lock_pairs_pm > 0 && rng.chance_pm(cfg.lock_pairs_pm) {
        let a = rng.below(nk);
        let b = (a + 1 + rng.below((nk - 1).max(1))) % nk;
        let (first, second) = if session.is_multiple_of(2) {
            (a, b)
        } else {
            (b, a)
        };
        return Program {
            stmts: vec![Stmt::LockUpdate(first), Stmt::LockUpdate(second)],
            cancel_mid_wait: rng.chance_pm(cfg.cancel_pm),
            commit: true,
            sync: bool_sync(rng.chance_pm(cfg.sync_on_pm)),
        };
    }
    let nstmts = 1 + rng.below(cfg.max_stmts);
    let w = &cfg.ops;
    let mut stmts = Vec::with_capacity(nstmts);
    for _ in 0..nstmts {
        let mut pick = rng.below(w.total().max(1));
        let stmt = if pick < w.read {
            Stmt::Read(rng.below(nk))
        } else {
            pick -= w.read;
            if pick < w.scan {
                let a = rng.below(nk);
                let b = rng.below(nk);
                let (lo, hi) = if a <= b { (a, b + 1) } else { (b, a + 1) };
                Stmt::Scan(lo, hi.min(nk).max(lo + 1))
            } else {
                pick -= w.scan;
                if pick < w.update {
                    Stmt::Update(rng.below(nk))
                } else {
                    pick -= w.update;
                    if pick < w.delete {
                        Stmt::Delete(rng.below(nk))
                    } else {
                        pick -= w.delete;
                        if pick < w.insert {
                            Stmt::Insert(rng.below(nk), 1 + rng.below(999) as u64)
                        } else {
                            pick -= w.insert;
                            if pick < w.lock_update {
                                Stmt::LockUpdate(rng.below(nk))
                            } else {
                                pick -= w.lock_update;
                                if pick < w.lock_key_share {
                                    Stmt::LockKeyShare(rng.below(nk))
                                } else {
                                    pick -= w.lock_key_share;
                                    if pick < w.savepoint {
                                        Stmt::Savepoint
                                    } else {
                                        Stmt::RollbackTo
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };
        stmts.push(stmt);
    }
    Program {
        stmts,
        cancel_mid_wait: rng.chance_pm(cfg.cancel_pm),
        // A txn that aborts at the end: 1 in 8.
        commit: !rng.chance_pm(125),
        sync: bool_sync(rng.chance_pm(cfg.sync_on_pm)),
    }
}

fn bool_sync(on: bool) -> SyncCommit {
    if on {
        SyncCommit::On
    } else {
        SyncCommit::Off
    }
}

// ---------------------------------------------------------------------------
// Sessions and actions
// ---------------------------------------------------------------------------

/// What `Wait` parked: the wait targets, the key and the requested mode.
pub type PendingWait = (Vec<(TxnId, u64)>, Vec<u8>, RowLockMode);

/// One read statement's outcome: its snapshot and the per-key results.
pub type ReadOut = (Ts, Vec<(usize, Option<Vec<u8>>)>);

/// The write/lock task of the current statement.
pub enum Task {
    Row(RowOpTask),
    Key(KeyOpTask),
}

/// A registered wait, with what I-LIVE needs.
pub struct WaitState {
    pub handle: WaitHandle,
    pub began: u64,
    /// The key and requested mode of the op that waits (I-LIVE a/c: who
    /// holds what).
    pub key: Vec<u8>,
    pub mode: RowLockMode,
}

/// Where a session is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    Idle,
    /// A txn is being begun to retry the current program (driver level: it
    /// creates the era's snapshot guard).
    RetryBegin,
    StartStmt,
    ReadStmt,
    Exec,
    Epq,
    WaitBegin,
    Wait,
    Savepoint,
    RollbackTo,
    CommitSubmit,
    PollTicket,
    Abort,
}

pub struct Session {
    pub id: usize,
    pub txn: Option<Txn>,
    pub cancel: Option<CancelHandle>,
    /// The txn's snapshot (RR/SER).
    pub snapshot_ts: Ts,
    pub program: Option<Program>,
    pub retries: u32,
    pub stmt_idx: usize,
    pub seq0: Seq,
    /// The current statement's snapshot S (RC: per statement).
    pub stmt_snapshot: Ts,
    /// An UPDATE statement's pre-read value (None = row absent at S).
    pub read_value: Option<u64>,
    /// The value the current UPDATE statement places (pre-read + 1, or the
    /// EPQ version + 1 after an EPQ Apply).
    pub write_value: u64,
    pub task: Option<Task>,
    pub epq: Option<EpqRequest>,
    /// The wait a step's `Wait(targets)` parked, until `wait_begin` runs.
    pub pending_wait: Option<PendingWait>,
    pub wait: Option<WaitState>,
    pub ticket: Option<CommitTicket>,
    /// The txn id the pending ticket belongs to.
    pub ticket_txn: Option<TxnId>,
    pub savepoints: Vec<Seq>,
    pub phase: Phase,
    pub cancelled: bool,
    /// I-PROGRESS: consecutive Again/Epq steps without a state change.
    pub streak: u32,
}

impl Session {
    fn new(id: usize) -> Session {
        Session {
            id,
            txn: None,
            cancel: None,
            snapshot_ts: Ts(0),
            program: None,
            retries: 0,
            stmt_idx: 0,
            seq0: 0,
            stmt_snapshot: Ts(0),
            read_value: None,
            write_value: 0,
            task: None,
            epq: None,
            pending_wait: None,
            wait: None,
            ticket: None,
            ticket_txn: None,
            savepoints: Vec::new(),
            phase: Phase::Idle,
            cancelled: false,
            streak: 0,
        }
    }

    pub fn iso(&self) -> Isolation {
        self.txn
            .as_ref()
            .map_or(Isolation::ReadCommitted, |t| t.isolation)
    }

    pub fn stmt(&self) -> Option<Stmt> {
        self.program.as_ref().map(|p| p.stmt(self.stmt_idx))
    }
}

/// One schedulable action.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Sess(usize, SAction),
    CommitThread,
    Resolver,
    Retention,
    GcPublish,
    GcTomb,
    GcFlush,
    GcCompact,
    GcCompactAll,
    Crash,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SAction {
    BeginTxn,
    RetryBeginTxn,
    StartStmt,
    ReadStmt,
    TaskStep,
    EpqDecide,
    WaitBegin,
    WaitPoll,
    WaitDeadlock,
    DoCancel,
    Savepoint,
    RollbackTo,
    CommitSubmit,
    PollTicket,
    AbortTxn,
}

impl std::fmt::Display for SAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

fn act_name(a: &Action) -> String {
    match a {
        Action::Sess(i, sa) => format!("s{i}({sa})"),
        other => format!("{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Sim: everything the scheduler and the probe share
// ---------------------------------------------------------------------------

pub struct Sim {
    pub cfg: Config,
    pub choices: Choices,
    pub trace: Trace,
    pub ghost: Ghost,
    pub kv: SimKv,
    pub core: Arc<Core<SimKv>>,
    pub ssi: Option<Arc<Ssi>>,
    pub locks: Option<Arc<LockManager>>,
    pub gc: Option<Arc<GcJob<SimKv>>>,
    pub sessions: Vec<Session>,
    pub budgets: Vec<usize>,
    pub steps: u64,
    pub era_steps: u64,
    /// Submitted, not yet acked, in submission (channel) order.
    pub inflight: Vec<TxnId>,
    /// The group `process_group` is currently running.
    pub current_group: Vec<TxnId>,
    pub resolve_pending: bool,
    pub crashes_left: usize,
    pub violation: Option<Violation>,
    pub keys: Vec<Vec<u8>>,
    pub chk: Checker,
    pub probe_points: u64,
}

impl Sim {
    pub fn violate(&mut self, inv: &'static str, detail: String) {
        if self.violation.is_none() {
            self.violation = Some(Violation {
                inv,
                detail,
                seed: 0,
                config: self.cfg.name,
                step: self.steps,
                choices: Vec::new(),
                trace: String::new(),
            });
        }
    }

    pub fn key_index(&self, key: &[u8]) -> Option<usize> {
        self.keys.iter().position(|k| k.as_slice() == key)
    }
}

/// A statement error the session handles by aborting and retrying (40001 /
/// 40P01 / 55P03 / 23505, plus 57014 from a self-cancel).
fn retryable_error(e: &TxnError) -> bool {
    matches!(
        e.sqlstate(),
        "40001" | "40P01" | "55P03" | "23505" | "57014"
    )
}

/// Whether the resolver has work: the driver's abort/commit flag, any
/// truncation candidate, or any ended current-epoch txn that still owns
/// intents (`intent_count > 0` — its §3 step 5 / §7.1 cleanup is queued;
/// lock-only placements count too).
fn resolver_needed(sim: &Sim) -> bool {
    if sim.resolve_pending || !sim.core.status.truncation_candidates().is_empty() {
        return true;
    }
    let epoch = sim.core.epoch();
    sim.ghost.txns.keys().any(|id| {
        id.epoch == epoch
            && matches!(
                sim.core.status.entry(*id),
                Some(e)
                    if e.intent_count > 0 && e.status != TxnStatus::Pending
            )
    })
}

/// Whether `a` may run while the commit thread's `process_group` is on the
/// stack (the probe): never the commit actor or a crash, and never a
/// session step that touches the era driver's snapshot guards (begin /
/// commit / abort run only in the main loop).
fn probe_runnable(a: &Action) -> bool {
    match a {
        Action::CommitThread | Action::Crash => false,
        Action::Sess(_, sa) => !matches!(
            sa,
            SAction::BeginTxn | SAction::RetryBeginTxn | SAction::CommitSubmit | SAction::AbortTxn
        ),
        _ => true,
    }
}

pub fn collect_actions(sim: &Sim, probe_mode: bool) -> Vec<Action> {
    let mut out: Vec<Action> = Vec::new();
    for (i, s) in sim.sessions.iter().enumerate() {
        let sa = match s.phase {
            Phase::Idle => {
                if sim.budgets[i] > 0 {
                    Some(SAction::BeginTxn)
                } else {
                    None
                }
            }
            Phase::RetryBegin => Some(SAction::RetryBeginTxn),
            Phase::StartStmt => Some(SAction::StartStmt),
            Phase::ReadStmt => Some(SAction::ReadStmt),
            Phase::Exec => Some(SAction::TaskStep),
            Phase::Epq => Some(SAction::EpqDecide),
            Phase::WaitBegin => Some(SAction::WaitBegin),
            Phase::Savepoint => Some(SAction::Savepoint),
            Phase::RollbackTo => Some(SAction::RollbackTo),
            Phase::CommitSubmit => Some(SAction::CommitSubmit),
            Phase::PollTicket => Some(SAction::PollTicket),
            Phase::Abort => Some(SAction::AbortTxn),
            Phase::Wait => None,
        };
        if let Some(sa) = sa {
            out.push(Action::Sess(i, sa));
        }
        if s.phase == Phase::Wait {
            out.push(Action::Sess(i, SAction::WaitPoll));
            let began = s.wait.as_ref().map_or(0, |w| w.began);
            if sim.steps.saturating_sub(began) >= sim.cfg.deadlock_after {
                out.push(Action::Sess(i, SAction::WaitDeadlock));
            }
            let planned = s.program.as_ref().is_some_and(|p| p.cancel_mid_wait);
            if planned && !s.cancelled {
                out.push(Action::Sess(i, SAction::DoCancel));
            }
        }
    }
    if !probe_mode {
        if !sim.inflight.is_empty() {
            out.push(Action::CommitThread);
        }
        if sim.crashes_left > 0 && sim.era_steps > 8 {
            out.push(Action::Crash);
        }
    }
    if resolver_needed(sim) {
        out.push(Action::Resolver);
    }
    if sim.ssi.as_ref().is_some_and(|s| s.stats().entries > 0) {
        // Rotated with the GC actor below: one maintenance action per step,
        // so background actors cannot starve the sessions.
        if sim.steps.is_multiple_of(2) {
            out.push(Action::Retention);
        }
    }
    if sim.gc.is_some() {
        // One GC action per step, rotating.
        match sim.steps % 5 {
            0 => out.push(Action::GcPublish),
            1 => out.push(Action::GcTomb),
            2 => out.push(Action::GcFlush),
            _ => {
                let has_files = sim.kv.with_live(|m| m.files().is_ok_and(|f| !f.is_empty()));
                if has_files {
                    out.push(Action::GcCompact);
                }
                out.push(Action::GcCompactAll);
            }
        }
    }
    if probe_mode {
        out.retain(probe_runnable);
    }
    out
}

// ---------------------------------------------------------------------------
// The probe and the clock
// ---------------------------------------------------------------------------

pub struct SimProbe {
    pub sim: Arc<Mutex<Sim>>,
    pub enabled: Arc<AtomicBool>,
}

impl CommitProbe for SimProbe {
    fn at(&self, point: ProbePoint) {
        if !self.enabled.load(Ordering::SeqCst) {
            return;
        }
        let mut sim = self.sim.lock().expect("sim");
        if sim.violation.is_some() {
            return;
        }
        sim.probe_points += 1;
        sim.probe_at(point);
    }
}

/// The deterministic `/sys/ts_clock` clock: "now" is the simulator's step
/// count, never the wall clock.
pub struct SimClock {
    pub sim: Arc<Mutex<Sim>>,
}

impl Clock for SimClock {
    fn now_secs(&self) -> u64 {
        self.sim.lock().expect("sim").steps
    }
}

impl Sim {
    /// One probe point: run the checks, then (with probability `yield_pm`)
    /// run other actors' steps — never the commit actor itself.
    pub fn probe_at(&mut self, point: ProbePoint) {
        check::probe_point(self, point);
        if self.violation.is_some() || self.steps >= self.cfg.max_steps {
            return;
        }
        let mut yields = 0usize;
        while yields < self.cfg.yield_steps {
            let acts = collect_actions(self, true);
            if acts.is_empty() {
                break;
            }
            // The yield decision is itself a recorded scheduling choice, so
            // a replayed choice list reproduces it.
            if self.choices.pick(1000) >= self.cfg.yield_pm {
                break;
            }
            let idx = self.choices.pick(acts.len());
            let act = acts[idx];
            self.trace.push(format!(
                "  {:>5} probe+ {} [{:?}",
                self.steps,
                act_name(&act),
                point
            ));
            exec(self, act);
            self.steps += 1;
            self.era_steps += 1;
            yields += 1;
            check::post_step(self);
            if self.violation.is_some() || self.steps >= self.cfg.max_steps {
                return;
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Step execution (the probe and the main loop share this)
// ---------------------------------------------------------------------------

pub fn exec(sim: &mut Sim, act: Action) {
    let core = Arc::clone(&sim.core);
    match act {
        Action::Sess(i, sa) => exec_sess(sim, &core, i, sa),
        Action::Resolver => {
            sim.resolve_pending = false;
            if let Err(e) = Resolver::run_once(&core) {
                sim.violate("RESOLVER", format!("resolver error: {e:?}"));
            }
        }
        Action::Retention => {
            if let Some(ssi) = &sim.ssi {
                ssi.run_retention(&core);
            }
        }
        Action::GcPublish
        | Action::GcTomb
        | Action::GcFlush
        | Action::GcCompact
        | Action::GcCompactAll => exec_gc(sim, act),
        Action::CommitThread | Action::Crash => {
            sim.violate("SCHED", format!("{act:?} is driver-level"));
        }
    }
}

fn exec_gc(sim: &mut Sim, act: Action) {
    // I-GC: around every GC step, every registered snapshot (the live
    // RR/SER txn snapshots) reads the same value for every key.
    let before = check::gc_guard_reads(sim);
    let r: Result<(), TxnError> = match act {
        Action::GcPublish => match &sim.gc {
            Some(job) => job.publish(sim.steps).map(|_| ()),
            None => Ok(()),
        },
        Action::GcTomb => match &sim.gc {
            Some(job) => job.drop_tombstones().map(|_| ()),
            None => Ok(()),
        },
        Action::GcFlush => sim
            .kv
            .with_live(|m| m.flush().map(|_| ()))
            .map_err(|e| TxnError::Kv(e.to_string())),
        Action::GcCompact => {
            let files = sim.kv.with_live(|m| m.files().unwrap_or_default());
            // Only files with a level below them can compact into it.
            let max_level = files.iter().map(|(l, _, _)| *l).max().unwrap_or(0);
            let compactable: Vec<(usize, u64)> = files
                .iter()
                .filter(|(l, _, _)| *l < max_level)
                .map(|(l, id, _)| (*l, *id))
                .collect();
            if compactable.is_empty() {
                Ok(())
            } else {
                let pick = sim.choices.pick(compactable.len());
                let (level, id) = compactable[pick];
                sim.kv
                    .with_live(move |m| m.compact(level, &[id]).map(|_| ()))
                    .map_err(|e| TxnError::Kv(e.to_string()))
            }
        }
        Action::GcCompactAll => {
            sim.kv.with_live(|m| m.compact_all());
            Ok(())
        }
        _ => Ok(()),
    };
    if let Err(e) = r {
        sim.violate("GC", format!("gc step error: {e:?}"));
        return;
    }
    check::gc_guard_check(sim, &before);
}

fn exec_sess(sim: &mut Sim, core: &Core<SimKv>, i: usize, sa: SAction) {
    // PollTicket is the only action that runs after the txn handle was
    // consumed by `commit_submit`.
    if sim.sessions[i].txn.is_none() && sa != SAction::PollTicket {
        sim.violate("SCHED", format!("session {i} action {sa:?} without a txn"));
        return;
    }
    match sa {
        SAction::BeginTxn | SAction::RetryBeginTxn | SAction::CommitSubmit | SAction::AbortTxn => {
            sim.violate("SCHED", format!("lifecycle action {sa:?} is driver-level"));
        }
        SAction::StartStmt => {
            let seq0 = match sim.sessions[i].txn.as_ref().map(|t| t.next_seq()) {
                Some(Ok(q)) => q,
                Some(Err(e)) => {
                    sim.violate("STMT", format!("next_seq: {e:?}"));
                    return;
                }
                None => return,
            };
            let stmt = match sim.sessions[i].stmt() {
                Some(s) => s,
                None => return,
            };
            let iso = sim.sessions[i].iso();
            {
                let s = &mut sim.sessions[i];
                s.seq0 = seq0;
                s.read_value = None;
                s.streak = 0;
            }
            match stmt {
                Stmt::Read(_) | Stmt::Scan(..) | Stmt::Update(_) => {
                    sim.sessions[i].phase = Phase::ReadStmt;
                }
                Stmt::Savepoint => {
                    sim.sessions[i].phase = Phase::Savepoint;
                }
                Stmt::RollbackTo => {
                    sim.sessions[i].phase = Phase::RollbackTo;
                }
                stmt => {
                    // A write/lock statement that needs no pre-read: the RC
                    // statement snapshot is taken now.
                    let snapshot = if iso == Isolation::ReadCommitted {
                        core.visible_ts()
                    } else {
                        sim.sessions[i].snapshot_ts
                    };
                    sim.sessions[i].stmt_snapshot = snapshot;
                    build_task(sim, i, stmt);
                    sim.sessions[i].phase = Phase::Exec;
                }
            }
        }
        SAction::ReadStmt => exec_read(sim, core, i),
        SAction::TaskStep => exec_task_step(sim, core, i),
        SAction::EpqDecide => exec_epq(sim, i),
        SAction::WaitBegin => {
            let Some((targets, key, mode)) = sim.sessions[i].pending_wait.take() else {
                sim.violate("SCHED", "wait_begin without a pending wait".into());
                return;
            };
            let outcome = {
                let txn = sim.sessions[i].txn.as_ref().expect("txn");
                core.wait_begin(txn, &targets)
            };
            match outcome {
                WaitBegin::Done(o) => handle_wait_outcome(sim, i, o),
                WaitBegin::Registered(h) => {
                    let began = sim.steps;
                    let s = &mut sim.sessions[i];
                    s.wait = Some(WaitState {
                        handle: h,
                        began,
                        key,
                        mode,
                    });
                    s.phase = Phase::Wait;
                }
            }
        }
        SAction::WaitPoll => {
            let poll = match sim.sessions[i].wait.as_ref() {
                Some(w) => core.wait_poll(&w.handle),
                None => None,
            };
            if let Some(o) = poll {
                let w = sim.sessions[i].wait.take().expect("wait");
                core.wait_end(w.handle);
                handle_wait_outcome(sim, i, o);
            }
        }
        SAction::WaitDeadlock => exec_deadlock_check(sim, core, i),
        SAction::DoCancel => {
            let s = &mut sim.sessions[i];
            s.cancelled = true;
            if let Some(c) = &s.cancel {
                c.cancel();
            }
        }
        SAction::Savepoint => {
            let sp = match sim.sessions[i].txn.as_ref().map(Txn::savepoint) {
                Some(Ok(q)) => q,
                Some(Err(e)) => {
                    sim.violate("STMT", format!("savepoint: {e:?}"));
                    return;
                }
                None => return,
            };
            sim.sessions[i].savepoints.push(sp);
            advance_stmt(sim, i);
        }
        SAction::RollbackTo => {
            let Some(sp) = sim.sessions[i].savepoints.last().copied() else {
                // No live savepoint: the statement is a no-op.
                advance_stmt(sim, i);
                return;
            };
            let r = {
                let txn = sim.sessions[i].txn.as_ref().expect("txn");
                core.rollback_to(txn, sp)
            };
            if let Err(e) = r {
                sim.violate("STMT", format!("rollback_to: {e:?}"));
                return;
            }
            let tid = sim.sessions[i].txn.as_ref().expect("txn").id;
            sim.ghost.rollback_to(tid, sp);
            if let Some(t) = sim.ghost.txns.get_mut(&tid) {
                t.events.push(format!("rollback_to {sp}"));
            }
            advance_stmt(sim, i);
        }
        SAction::PollTicket => {
            let ack = match sim.sessions[i].ticket.as_ref() {
                Some(t) => t.try_ack(),
                None => None,
            };
            match ack {
                None => {}
                Some(Ok(ts)) => {
                    let tid = sim.sessions[i].ticket_txn.take();
                    if let Some(tid) = tid {
                        absorb_ack(sim, i, tid, ts);
                    }
                }
                Some(Err(e)) => {
                    sim.violate("COMMIT", format!("error ack: {e:?}"));
                }
            }
        }
    }
}

/// Records an observed ack: the ghost outcome, the committed history, the
/// release observation, the I-ACK checks, and the session's next program.
fn absorb_ack(sim: &mut Sim, i: usize, tid: TxnId, ts: Ts) {
    let sync = sim.sessions[i].program.as_ref().map(|p| p.sync);
    if let Some(t) = sim.ghost.txns.get_mut(&tid) {
        t.outcome = ghost::Outcome::Acked(ts);
        t.sync = sync;
    }
    // Step 5 ran before the ack (§3); the entry may already be gone (the
    // resolver truncates a released txn), which this observation records.
    let released = sim.core.status.entry(tid).is_some_and(|e| e.released);
    if let Some(t) = sim.ghost.txns.get_mut(&tid) {
        t.released_seen |= released;
    }
    // Ts::ZERO is never a commit ts (§1): the no-write fast path commits no
    // record and joins no history.
    if ts != Ts::ZERO {
        sim.ghost.commit_at(tid, ts);
    }
    sim.inflight.retain(|t| *t != tid);
    // A committed txn with intents queued them for resolution (§3 step 5):
    // the resolver has work.
    if sim
        .ghost
        .txns
        .get(&tid)
        .is_some_and(|t| !t.writes.is_empty())
    {
        sim.resolve_pending = true;
    }
    // I-ACK: an acked ts is <= visible_ts when observed.
    if sim.core.visible_ts() < ts {
        sim.violate(
            "I-ACK",
            format!(
                "ack of {tid:?} at ts {ts:?} with visible_ts {:?}",
                sim.core.visible_ts()
            ),
        );
        return;
    }
    // I-ACK's durability half: the no-write fast path (Ts::ZERO) writes no
    // record and promises nothing.
    if ts != Ts::ZERO {
        check::on_ack(sim, &tid);
        if sim.violation.is_some() {
            return;
        }
    }
    sim.sessions[i].ticket = None;
    sim.sessions[i].ticket_txn = None;
    program_finished(sim, i);
}

/// Builds the current statement's task (the session's `stmt_snapshot`,
/// `seq0`, and for UPDATE the pre-read `read_value` must be set).
fn build_task(sim: &mut Sim, i: usize, stmt: Stmt) {
    let snapshot = sim.sessions[i].stmt_snapshot;
    let seq0 = sim.sessions[i].seq0;
    let key = |sim: &Sim, k: usize| sim.keys[k].clone();
    let ctx = StmtCtx::new(snapshot, seq0, seq0);
    let task = match stmt {
        Stmt::Update(k) => {
            let v = sim.sessions[i].read_value.expect("update with a pre-read");
            sim.sessions[i].write_value = v + 1;
            let value = u64_payload(v + 1);
            Task::Row(RowOpTask::new(
                &key(sim, k),
                None,
                RowOp::Update {
                    value,
                    key_cols_changed: false,
                },
                ctx,
            ))
        }
        Stmt::Delete(k) => Task::Row(RowOpTask::new(&key(sim, k), None, RowOp::Delete, ctx)),
        Stmt::Insert(k, v) => {
            sim.sessions[i].write_value = v;
            let value = u64_payload(v);
            Task::Key(KeyOpTask::new(
                &key(sim, k),
                None,
                value,
                ctx,
                UniqueRule::Unique { same_row: None },
            ))
        }
        Stmt::LockUpdate(k) => Task::Row(RowOpTask::new(
            &key(sim, k),
            None,
            RowOp::Lock(RowLockMode::Update),
            ctx,
        )),
        Stmt::LockKeyShare(k) => Task::Row(RowOpTask::new(
            &key(sim, k),
            None,
            RowOp::Lock(RowLockMode::KeyShare),
            ctx,
        )),
        other => {
            sim.violate("SCHED", format!("unexpected stmt {other:?} for a task"));
            return;
        }
    };
    sim.sessions[i].task = Some(task);
}

fn exec_read(sim: &mut Sim, core: &Core<SimKv>, i: usize) {
    let stmt = match sim.sessions[i].stmt() {
        Some(s) => s,
        None => return,
    };
    let iso = sim.sessions[i].iso();
    let (snapshot, results): ReadOut = match stmt {
        Stmt::Scan(lo, hi) => scan_stmt(sim, core, i, iso, lo, hi),
        Stmt::Read(k) | Stmt::Update(k) => {
            let (s, v) = read_key_stmt(sim, core, i, iso, k);
            (s, vec![(k, v)])
        }
        _ => {
            sim.violate("SCHED", format!("read phase for {stmt:?}"));
            return;
        }
    };
    let seq0 = sim.sessions[i].seq0;
    let tid = sim.sessions[i].txn.as_ref().map(|t| t.id).expect("txn");
    let mut failed = false;
    for (k, v) in results {
        let key = sim.keys[k].clone();
        let range = match stmt {
            Stmt::Scan(lo, hi) => Some((sim.keys[lo].clone(), end_key(&sim.keys[hi - 1]))),
            _ => None,
        };
        if let Some(t) = sim.ghost.txns.get_mut(&tid) {
            t.reads.push(ghost::GhostRead {
                seq0,
                snapshot,
                key: key.clone(),
                range,
                result: v.clone(),
            });
            t.events.push(format!(
                "s{seq0} read k{}@{snapshot:?} -> {:?}",
                k,
                v.as_deref().map(payload_u64)
            ));
        }
        check::read_oracle(sim, core, &tid, seq0, snapshot, k, v.as_deref());
        if sim.violation.is_some() {
            failed = true;
            break;
        }
    }
    if failed {
        return;
    }
    if let Stmt::Update(_) = stmt {
        let v = sim
            .ghost
            .txns
            .get(&tid)
            .and_then(|t| t.reads.last())
            .and_then(|r| r.result.clone())
            .map(|bytes| payload_u64(&bytes));
        sim.sessions[i].stmt_snapshot = snapshot;
        if let Some(v) = v {
            sim.sessions[i].read_value = Some(v);
            build_task(sim, i, stmt);
            sim.sessions[i].phase = Phase::Exec;
        } else {
            // Row absent at S: the UPDATE's quals fail; statement done.
            advance_stmt(sim, i);
        }
    } else {
        advance_stmt(sim, i);
    }
}

/// A point read (also the UPDATE pre-read): returns the snapshot it ran at
/// and the value.
fn read_key_stmt(
    sim: &mut Sim,
    core: &Core<SimKv>,
    i: usize,
    iso: Isolation,
    k: usize,
) -> (Ts, Option<Vec<u8>>) {
    let key = sim.keys[k].clone();
    let tid = sim.sessions[i].txn.as_ref().map(|t| t.id).expect("txn");
    let seq0 = sim.sessions[i].seq0;
    let mk = |snapshot: Ts| ReadCtx {
        txn: tid,
        snapshot,
        stmt_seq: seq0,
    };
    match iso {
        Isolation::Serializable => {
            let ssi = sim.ssi.clone().expect("ser session with Ssi installed");
            let snapshot = sim.sessions[i].snapshot_ts;
            let v = match ssi.read_key(core, tid, &key, &mk(snapshot)) {
                Ok(v) => v,
                Err(e) => {
                    sim.violate("READ", format!("ser read: {e:?}"));
                    None
                }
            };
            (snapshot, v)
        }
        Isolation::RepeatableRead => {
            let snapshot = sim.sessions[i].snapshot_ts;
            let view = core.open_view();
            let v = match read::read_key(core, &view, &key, &mk(snapshot), &mut NoSsi) {
                Ok(v) => v,
                Err(e) => {
                    sim.violate("READ", format!("rr read: {e:?}"));
                    None
                }
            };
            (snapshot, v)
        }
        Isolation::ReadCommitted => {
            // §3.1: RC takes a fresh, registered statement snapshot.
            let guard = core.registry.take_snapshot();
            let snapshot = guard.ts();
            let view = core.open_view();
            let v = match read::read_key(core, &view, &key, &mk(snapshot), &mut NoSsi) {
                Ok(v) => v,
                Err(e) => {
                    sim.violate("READ", format!("rc read: {e:?}"));
                    None
                }
            };
            (snapshot, v)
        }
    }
}

/// Maps scan rows onto the key indices in `[lo, hi)`.
fn map_scan_rows(
    sim: &Sim,
    lo: usize,
    hi: usize,
    rows: &[(nucleus_kv::Key, Vec<u8>)],
) -> Vec<(usize, Option<Vec<u8>>)> {
    (lo..hi)
        .map(|kidx| {
            let v = rows
                .iter()
                .find(|(rk, _)| rk.as_slice() == sim.keys[kidx].as_slice())
                .map(|(_, v)| v.clone());
            (kidx, v)
        })
        .collect()
}

fn scan_stmt(
    sim: &mut Sim,
    core: &Core<SimKv>,
    i: usize,
    iso: Isolation,
    lo: usize,
    hi: usize,
) -> ReadOut {
    let hi = hi.clamp(lo + 1, sim.cfg.keys);
    let tid = sim.sessions[i].txn.as_ref().map(|t| t.id).expect("txn");
    let seq0 = sim.sessions[i].seq0;
    let lo_key = sim.keys[lo].clone();
    let hi_key = sim.keys[hi - 1].clone();
    match iso {
        Isolation::Serializable => {
            let ssi = sim.ssi.clone().expect("ser session with Ssi installed");
            let snapshot = sim.sessions[i].snapshot_ts;
            let ctx = ReadCtx {
                txn: tid,
                snapshot,
                stmt_seq: seq0,
            };
            // `Ssi::scan`'s upper bound is exclusive over logical keys; the
            // end key of the last key covers exactly its entries.
            let hi_excl = end_key(&hi_key);
            match ssi.scan(core, tid, &lo_key, &hi_excl, &ctx) {
                Ok(rows) => (snapshot, map_scan_rows(sim, lo, hi, &rows)),
                Err(e) => {
                    sim.violate("READ", format!("ser scan: {e:?}"));
                    (snapshot, Vec::new())
                }
            }
        }
        _ => {
            // The RC scan's statement snapshot stays registered until the
            // scan's view is done (`_guard` drops at the end of this arm).
            let (snapshot, _guard) = if iso == Isolation::RepeatableRead {
                (sim.sessions[i].snapshot_ts, None)
            } else {
                let g = core.registry.take_snapshot();
                (g.ts(), Some(g))
            };
            let ctx = ReadCtx {
                txn: tid,
                snapshot,
                stmt_seq: seq0,
            };
            let view = core.open_view();
            let rows = read::scan(
                core,
                &view,
                (
                    std::ops::Bound::Included(lo_key.as_slice()),
                    std::ops::Bound::Included(hi_key.as_slice()),
                ),
                &ctx,
                &mut NoSsi,
            )
            .collect::<Result<Vec<_>, _>>();
            match rows {
                Ok(rows) => (snapshot, map_scan_rows(sim, lo, hi, &rows)),
                Err(e) => {
                    sim.violate("READ", format!("scan: {e:?}"));
                    (snapshot, Vec::new())
                }
            }
        }
    }
}

/// The key and requested mode of the op currently driving the session (for
/// I-LIVE's lock-ownership recomputation).
fn pending_wait_key_mode(sim: &Sim, i: usize) -> (Vec<u8>, RowLockMode) {
    match sim.sessions[i].stmt() {
        Some(Stmt::Update(k)) => (sim.keys[k].clone(), RowLockMode::NoKeyUpdate),
        Some(Stmt::Delete(k)) | Some(Stmt::LockUpdate(k)) => {
            (sim.keys[k].clone(), RowLockMode::Update)
        }
        Some(Stmt::LockKeyShare(k)) => (sim.keys[k].clone(), RowLockMode::KeyShare),
        Some(Stmt::Insert(k, _)) => (sim.keys[k].clone(), RowLockMode::NoKeyUpdate),
        _ => (Vec::new(), RowLockMode::NoKeyUpdate),
    }
}

fn exec_task_step(sim: &mut Sim, core: &Core<SimKv>, i: usize) {
    let stmt = sim.sessions[i].stmt().expect("stmt");
    let step = {
        let s = &mut sim.sessions[i];
        let Some(txn) = s.txn.as_ref() else { return };
        match s.task.as_mut() {
            Some(Task::Row(t)) => t.step(core, txn),
            Some(Task::Key(t)) => t.step(core, txn),
            None => {
                sim.violate("SCHED", "task step without a task".into());
                return;
            }
        }
    };
    match step {
        Ok(Step::Done(outcome)) => {
            let rec_after = sim.kv.rec_len();
            applied_outcome(sim, core, i, stmt, outcome, rec_after);
        }
        Ok(Step::Again) => {
            let s = &mut sim.sessions[i];
            s.streak = s.streak.saturating_add(1);
        }
        Ok(Step::Wait(targets)) => {
            let (key, mode) = pending_wait_key_mode(sim, i);
            let s = &mut sim.sessions[i];
            s.phase = Phase::WaitBegin;
            s.pending_wait = Some((targets, key, mode));
        }
        Ok(Step::Epq(req)) => {
            let s = &mut sim.sessions[i];
            s.epq = Some(req);
            s.phase = Phase::Epq;
            s.streak = s.streak.saturating_add(1);
        }
        Ok(Step::Restart) => {
            sim.violate("SCHED", "a non-arbiter task returned Restart".into());
        }
        Err(e) => stmt_error(sim, i, e),
    }
}

fn applied_outcome(
    sim: &mut Sim,
    core: &Core<SimKv>,
    i: usize,
    stmt: Stmt,
    outcome: RowOutcome,
    rec_after: usize,
) {
    let tid = sim.sessions[i].txn.as_ref().map(|t| t.id).expect("txn");
    let seq0 = sim.sessions[i].seq0;
    if outcome == RowOutcome::Applied {
        let write_value = sim.sessions[i].write_value;
        if let Some(t) = sim.ghost.txns.get_mut(&tid) {
            t.placement_bound = rec_after;
            let (key, kind) = match stmt {
                Stmt::Update(k) => (
                    sim.keys[k].clone(),
                    GWrite::Inc {
                        wrote: u64_payload(write_value),
                    },
                ),
                Stmt::Delete(k) => (sim.keys[k].clone(), GWrite::Delete),
                Stmt::Insert(k, _) => (sim.keys[k].clone(), GWrite::Set(u64_payload(write_value))),
                _ => (Vec::new(), GWrite::Set(Vec::new())),
            };
            if !key.is_empty() {
                let ki = sim.key_index(&key).unwrap_or(usize::MAX);
                if let Some(t) = sim.ghost.txns.get_mut(&tid) {
                    t.events
                        .push(format!("s{seq0} applied {stmt:?} k{ki} = {write_value}"));
                    t.writes.push(ghost::GhostWrite {
                        seq: seq0,
                        key,
                        kind,
                    });
                }
            }
        }
        check::i_ww_check(sim, core, &tid, stmt);
    }
    let s = &mut sim.sessions[i];
    s.streak = 0;
    s.task = None;
    advance_stmt(sim, i);
}

fn exec_epq(sim: &mut Sim, i: usize) {
    let Some(req) = sim.sessions[i].epq.take() else {
        sim.violate("SCHED", "epq step without a pending request".into());
        return;
    };
    use nucleus_txn::encoding::VersionValue;
    let moved = |v: &VersionValue| matches!(v, VersionValue::Tombstone { moved: true });
    let tomb = |v: &VersionValue| matches!(v, VersionValue::Tombstone { moved: false });
    let stmt = sim.sessions[i].stmt().expect("stmt");
    let mode = pending_wait_key_mode(sim, i).1;
    // §5.2 pre-rules (the crate applies them around the callback inside its
    // blocking drivers; the step-driven caller applies them around its own
    // callback): a moved tombstone is 40001, a tombstone skips, and KEY
    // SHARE examines every version above S.
    if moved(&req.v.value) {
        stmt_error(sim, i, TxnError::SerializationFailure);
        return;
    }
    if tomb(&req.v.value) {
        finish_epq_skip(sim, i);
        return;
    }
    if mode == RowLockMode::KeyShare {
        for v in &req.above_s {
            if moved(&v.value) {
                stmt_error(sim, i, TxnError::SerializationFailure);
                return;
            }
            if tomb(&v.value)
                || matches!(
                    v.value,
                    VersionValue::Live {
                        key_changed: true,
                        ..
                    }
                )
            {
                finish_epq_skip(sim, i);
                return;
            }
        }
    }
    // The callback: the quals are "the row exists"; an UPDATE recomputes its
    // increment from the newest version.
    let decision = match (&req.v.value, stmt) {
        (VersionValue::Live { payload, .. }, Stmt::Update(_)) => {
            let v = payload_u64(payload);
            sim.sessions[i].write_value = v + 1;
            EpqDecision::Apply(RowOp::Update {
                value: u64_payload(v + 1),
                key_cols_changed: false,
            })
        }
        (VersionValue::Live { .. }, Stmt::Delete(_)) => EpqDecision::Apply(RowOp::Delete),
        (VersionValue::Live { .. }, Stmt::LockUpdate(_)) => {
            EpqDecision::Apply(RowOp::Lock(RowLockMode::Update))
        }
        (VersionValue::Live { .. }, Stmt::LockKeyShare(_)) => {
            EpqDecision::Apply(RowOp::Lock(RowLockMode::KeyShare))
        }
        _ => EpqDecision::Skip,
    };
    let r = {
        let s = &mut sim.sessions[i];
        match s.task.as_mut() {
            Some(Task::Row(t)) => t.epq_result(decision.clone()),
            Some(Task::Key(_)) => Err(TxnError::Invariant("key op EPQ".into())),
            None => Err(TxnError::Invariant("epq without a task".into())),
        }
    };
    match r {
        Ok(()) => {
            if decision == EpqDecision::Skip {
                finish_epq_skip(sim, i);
            } else {
                sim.sessions[i].phase = Phase::Exec;
            }
        }
        Err(e) => sim.violate("EPQ", format!("epq_result: {e:?}")),
    }
}

fn finish_epq_skip(sim: &mut Sim, i: usize) {
    let s = &mut sim.sessions[i];
    s.task = None;
    advance_stmt(sim, i);
}

fn handle_wait_outcome(sim: &mut Sim, i: usize, o: WaitOutcome) {
    match o {
        WaitOutcome::Ended
        | WaitOutcome::Aborted
        | WaitOutcome::Committed(_)
        | WaitOutcome::GenChanged => {
            // §6: the waiter re-runs its step.
            sim.sessions[i].phase = Phase::Exec;
        }
        WaitOutcome::Cancelled => stmt_error(sim, i, TxnError::QueryCanceled),
        WaitOutcome::Deadlock => stmt_error(sim, i, TxnError::Deadlock),
        WaitOutcome::LockTimeout => stmt_error(sim, i, TxnError::LockNotAvailable),
    }
}

fn exec_deadlock_check(sim: &mut Sim, core: &Core<SimKv>, i: usize) {
    let Some(w) = sim.sessions[i].wait.as_ref() else {
        return;
    };
    let waiter = w.handle.waiter();
    let fired = core.wait_deadlock_check(&w.handle);
    // I-LIVE (c): a fired check must be on a cycle of the wait edges
    // recomputed from ghost lock ownership; (b): a parked cycle must be
    // broken (removing the victim's edges breaks every cycle through it, so
    // exactly one 40P01 fires per cycle).
    let ghost_cycle = check::waiter_on_ghost_cycle(sim, waiter);
    if fired {
        let w = sim.sessions[i].wait.take().expect("wait");
        core.wait_end(w.handle);
        if !ghost_cycle {
            sim.violate(
                "I-LIVE(c)",
                format!("40P01 for {waiter:?} without a ghost-lock cycle at that moment"),
            );
            return;
        }
        stmt_error(sim, i, TxnError::Deadlock);
    } else if ghost_cycle && check::ghost_cycle_is_parked(sim, waiter) {
        sim.violate(
            "I-LIVE(b)",
            format!("deadlock check of {waiter:?} found no cycle but a parked ghost cycle exists"),
        );
    }
}

fn stmt_error(sim: &mut Sim, i: usize, e: TxnError) {
    if !retryable_error(&e) {
        sim.violate(
            "ERROR",
            format!("statement error outside the retryable set: {e:?}"),
        );
        return;
    }
    let s = &mut sim.sessions[i];
    s.task = None;
    s.epq = None;
    s.pending_wait = None;
    s.phase = Phase::Abort;
}

fn advance_stmt(sim: &mut Sim, i: usize) {
    let s = &mut sim.sessions[i];
    s.task = None;
    s.epq = None;
    s.pending_wait = None;
    s.stmt_idx += 1;
    let n = s.program.as_ref().map_or(0, |p| p.stmts.len());
    if s.stmt_idx >= n {
        s.phase = if s.program.as_ref().is_some_and(|p| p.commit) {
            Phase::CommitSubmit
        } else {
            Phase::Abort
        };
    } else {
        s.phase = Phase::StartStmt;
    }
}

fn program_finished(sim: &mut Sim, i: usize) {
    let s = &mut sim.sessions[i];
    s.program = None;
    s.retries = 0;
    s.savepoints.clear();
    s.cancelled = false;
    s.streak = 0;
    s.phase = Phase::Idle;
}

// ---------------------------------------------------------------------------
// The run: boot eras and the scheduler loop
// ---------------------------------------------------------------------------

pub fn run(cfg: &Config, seed: u64, replay: Option<Vec<u64>>) -> RunResult {
    let kv = SimKv::new();
    let keys = make_keyspace(cfg.keys);
    // Boot the first era before building Sim (the core handle is part of
    // it; each later era replaces it).
    let mut core = match Core::open(kv.clone()) {
        Ok(c) => Arc::new(c),
        Err(e) => {
            return RunResult::Violation(Box::new(Violation {
                inv: "BOOT",
                detail: format!("initial boot: {e:?}"),
                seed,
                config: cfg.name,
                step: 0,
                choices: Vec::new(),
                trace: String::new(),
            }))
        }
    };
    let sim = Arc::new(Mutex::new(Sim {
        cfg: cfg.clone(),
        choices: Choices::new(seed, replay),
        trace: Trace::default(),
        ghost: Ghost::new(),
        kv: kv.clone(),
        core: Arc::clone(&core),
        ssi: None,
        locks: None,
        gc: None,
        sessions: (0..cfg.sessions).map(Session::new).collect(),
        budgets: vec![cfg.txns_per_session; cfg.sessions],
        steps: 0,
        era_steps: 0,
        inflight: Vec::new(),
        current_group: Vec::new(),
        resolve_pending: false,
        crashes_left: cfg.crashes,
        violation: None,
        keys,
        chk: Checker::new(),
        probe_points: 0,
    }));
    let mut first_boot = true;
    loop {
        // ---- install the era's hooks ----
        kv.set_era(core.epoch());
        let locks = LockManager::install(&core);
        let needs_ssi = cfg.isolation.contains(&Isolation::Serializable);
        let ssi = if needs_ssi {
            Some(Ssi::install(&core))
        } else {
            None
        };
        let gc = if cfg.gc {
            match GcJob::install(&core, GcConfig::default()) {
                Ok(j) => Some(Arc::new(j)),
                Err(e) => return fail(&sim, seed, "GC", format!("gc install: {e:?}")),
            }
        } else {
            None
        };
        {
            let mut s = sim.lock().expect("sim");
            s.core = Arc::clone(&core);
            s.locks = Some(Arc::clone(&locks));
            s.ssi = ssi.clone();
            s.gc = gc.clone();
            s.sessions = (0..cfg.sessions).map(Session::new).collect();
            s.era_steps = 0;
            s.current_group.clear();
            s.chk.new_era(&core);
            check::on_reboot(&mut s, &core);
        }
        if sim.lock().expect("sim").violation.is_some() {
            return fail(&sim, seed, "BOOT", String::new());
        }
        let probe_flag = Arc::new(AtomicBool::new(false));
        let probe = Arc::new(SimProbe {
            sim: Arc::clone(&sim),
            enabled: Arc::clone(&probe_flag),
        });
        let clock = Arc::new(SimClock {
            sim: Arc::clone(&sim),
        });
        let mut pipeline = match CommitPipeline::with_config(
            Arc::clone(&core),
            CommitConfig::new().with_probe(probe).with_clock(clock),
        ) {
            Ok(p) => p,
            Err(e) => return fail(&sim, seed, "BOOT", format!("pipeline: {e:?}")),
        };
        if first_boot {
            first_boot = false;
            preload(&sim, &core, &mut pipeline);
            if sim.lock().expect("sim").violation.is_some() {
                return fail(&sim, seed, "PRELOAD", String::new());
            }
        }
        // ---- drive the era ----
        let ended = drive_era(&core, &mut pipeline, &probe_flag, &sim);
        if sim.lock().expect("sim").violation.is_some() {
            return fail(&sim, seed, "RUN", String::new());
        }
        match ended {
            EraEnd::Done => {
                let mut s = sim.lock().expect("sim");
                check::final_checks(&mut s, &core);
                if s.violation.is_some() {
                    drop(s);
                    return fail(&sim, seed, "FINAL", String::new());
                }
                let steps = s.steps;
                let choices = s.choices.taken().to_vec();
                let trace = s.trace.render();
                let probe_points = s.probe_points;
                return RunResult::Ok(Box::new(Finished {
                    steps,
                    choices,
                    trace,
                    probe_points,
                }));
            }
            EraEnd::Crash => {
                let (keep, unsynced) = {
                    let mut s = sim.lock().expect("sim");
                    let unsynced = s.kv.unsynced_len();
                    (s.choices.aux.below(unsynced + 1), unsynced)
                };
                {
                    let mut s = sim.lock().expect("sim");
                    s.crashes_left -= 1;
                    // Sessions in flight die with the era (§7.2): their
                    // txns are lost.
                    let ended: Vec<Txn> =
                        s.sessions.iter_mut().filter_map(|x| x.txn.take()).collect();
                    for ssn in s.sessions.iter_mut() {
                        ssn.ticket = None;
                        ssn.ticket_txn = None;
                        ssn.wait = None;
                        ssn.pending_wait = None;
                        ssn.task = None;
                        ssn.epq = None;
                        ssn.program = None;
                        ssn.phase = Phase::Idle;
                    }
                    for t in ended {
                        if let Some(g) = s.ghost.txns.get_mut(&t.id) {
                            if g.outcome == ghost::Outcome::Active {
                                g.outcome = ghost::Outcome::LostInCrash;
                            }
                        }
                    }
                    s.inflight.clear();
                    let steps = s.steps;
                    s.trace
                        .push(format!("{steps:>5} crash keep={keep}/{unsynced}"));
                }
                drop(pipeline);
                drop(gc);
                drop(ssi);
                drop(locks);
                drop(core);
                let kept = kv.crash(keep);
                debug_assert_eq!(kept, keep);
                {
                    let mut s = sim.lock().expect("sim");
                    check::after_crash_checks(&mut s);
                }
                if sim.lock().expect("sim").violation.is_some() {
                    return fail(&sim, seed, "CRASH", String::new());
                }
                // The next era.
                core = match Core::open(kv.clone()) {
                    Ok(c) => Arc::new(c),
                    Err(e) => return fail(&sim, seed, "BOOT", format!("reboot: {e:?}")),
                };
            }
        }
    }
}

fn fail(sim: &Arc<Mutex<Sim>>, seed: u64, inv: &'static str, detail: String) -> RunResult {
    let mut s = sim.lock().expect("sim");
    if !detail.is_empty() {
        s.violate(inv, detail);
    }
    let v = s.violation.clone().expect("violation");
    RunResult::Violation(Box::new(Violation {
        seed,
        choices: s.choices.taken().to_vec(),
        trace: s.trace.render(),
        config: s.cfg.name,
        ..v
    }))
}

enum EraEnd {
    Done,
    Crash,
}

fn drive_era(
    core_arc: &Arc<Core<SimKv>>,
    pipeline: &mut CommitPipeline<SimKv>,
    probe_flag: &Arc<AtomicBool>,
    sim: &Arc<Mutex<Sim>>,
) -> EraEnd {
    // The era's snapshot guards live here, on the driver's stack, borrowing
    // the era's core — the card's boot-era lifetime pattern.
    let core: &Core<SimKv> = core_arc;
    let n_sessions = sim.lock().expect("sim").cfg.sessions;
    let mut guards: Vec<Option<SnapshotGuard<'_>>> = (0..n_sessions).map(|_| None).collect();
    loop {
        if sim.lock().expect("sim").violation.is_some() {
            return EraEnd::Done;
        }
        // The step bound: abort what is in flight, drain, end.
        {
            let s = sim.lock().expect("sim");
            if s.steps >= s.cfg.max_steps {
                drop(s);
                let mut s = sim.lock().expect("sim");
                force_abort_all(&mut s, core);
                drop(s);
                drain(sim, core, pipeline);
                return EraEnd::Done;
            }
        }
        let next = {
            let mut s = sim.lock().expect("sim");
            let acts = collect_actions(&s, false);
            if acts.is_empty() {
                None
            } else {
                let idx = s.choices.pick(acts.len());
                Some(acts[idx])
            }
        };
        let Some(act) = next else {
            let all_done = {
                let s = sim.lock().expect("sim");
                s.budgets.iter().all(|&b| b == 0) && s.sessions.iter().all(|x| x.txn.is_none())
            };
            if all_done {
                drain(sim, core, pipeline);
                return EraEnd::Done;
            }
            // Waiting sessions always have poll actions; deadlock checks
            // become enabled after the simulated timeout. Anything else
            // here is a scheduler bug.
            let mut s = sim.lock().expect("sim");
            s.violate("SCHED", "no enabled actions with drivable sessions".into());
            return EraEnd::Done;
        };
        {
            let mut s = sim.lock().expect("sim");
            let line = format!("{:>5} {}", s.steps, act_name(&act));
            s.trace.push(line);
            s.steps += 1;
            s.era_steps += 1;
        }
        match act {
            Action::Crash => {
                for g in guards.iter_mut() {
                    *g = None;
                }
                return EraEnd::Crash;
            }
            Action::CommitThread => {
                // The commit actor. The probe is enabled while the group is
                // processed; the Sim mutex is NOT held across it.
                probe_flag.store(true, Ordering::SeqCst);
                let group = pipeline.drain_available();
                {
                    let mut s = sim.lock().expect("sim");
                    let ids: Vec<TxnId> = group.iter().map(|r| r.txn).collect();
                    let mut inflight = std::mem::take(&mut s.inflight);
                    for id in &ids {
                        inflight.retain(|t| t != id);
                    }
                    s.current_group = ids;
                    s.inflight = inflight;
                }
                pipeline.process_group(group);
                probe_flag.store(false, Ordering::SeqCst);
                let mut s = sim.lock().expect("sim");
                s.current_group.clear();
                check::post_step(&mut s);
                if s.violation.is_some() {
                    return EraEnd::Done;
                }
            }
            Action::Sess(i, SAction::BeginTxn) | Action::Sess(i, SAction::RetryBeginTxn) => {
                let retry = act == Action::Sess(i, SAction::RetryBeginTxn);
                let mut s = sim.lock().expect("sim");
                let iso = s.cfg.isolation[i % s.cfg.isolation.len()];
                let txn = core.begin(iso);
                let guard = match iso {
                    Isolation::Serializable => {
                        let ssi = s.ssi.clone().expect("ssi for a ser session");
                        match ssi.begin(core, &txn, false) {
                            Ok(g) => Some(g),
                            Err(e) => {
                                s.violate("BEGIN", format!("ssi begin: {e:?}"));
                                return EraEnd::Done;
                            }
                        }
                    }
                    Isolation::RepeatableRead => Some(core.registry.take_snapshot()),
                    Isolation::ReadCommitted => None,
                };
                let id = txn.id;
                s.ghost.create(id, i, iso);
                let snap = guard.as_ref().map(|g| g.ts());
                if let Some(t) = s.ghost.txns.get_mut(&id) {
                    t.events.push(format!("begin S={snap:?}"));
                }
                if !retry {
                    s.budgets[i] -= 1;
                    let cfg = s.cfg.clone();
                    let mut aux = std::mem::replace(&mut s.choices.aux, Rng::new(0));
                    let program = gen_program(&cfg, i, &mut aux);
                    s.choices.aux = aux;
                    let ssn = &mut s.sessions[i];
                    ssn.program = Some(program);
                }
                {
                    let ssn = &mut s.sessions[i];
                    ssn.txn = Some(txn);
                    ssn.cancel = ssn.txn.as_ref().map(|t| t.cancel_handle());
                    ssn.snapshot_ts = snap.unwrap_or(Ts(0));
                    ssn.stmt_idx = 0;
                    ssn.read_value = None;
                    ssn.phase = Phase::StartStmt;
                }
                if let Some(t) = s.ghost.txns.get_mut(&id) {
                    t.snapshot = snap;
                }
                guards[i] = guard;
                drop(s);
            }
            Action::Sess(i, SAction::CommitSubmit) => {
                let (txn, sync) = {
                    let mut s = sim.lock().expect("sim");
                    let ssn = &mut s.sessions[i];
                    let txn = ssn.txn.take().expect("txn");
                    let sync = ssn.program.as_ref().map(|p| p.sync).expect("program");
                    (txn, sync)
                };
                guards[i] = None;
                let id = txn.id;
                let r = core.commit_submit(txn, sync);
                let mut s = sim.lock().expect("sim");
                match r {
                    Ok(ticket) => {
                        if let Some(t) = s.ghost.txns.get_mut(&id) {
                            t.sync = Some(sync);
                        }
                        s.sessions[i].ticket = Some(ticket);
                        s.sessions[i].ticket_txn = Some(id);
                        s.sessions[i].phase = Phase::PollTicket;
                        s.inflight.push(id);
                        // The no-write fast path is acked at submit and may
                        // be truncated by the resolver before any poll:
                        // absorb an immediate ack synchronously.
                        let ack = s.sessions[i].ticket.as_ref().and_then(|t| t.try_ack());
                        if let Some(Ok(ts)) = ack {
                            absorb_ack(&mut s, i, id, ts);
                        } else if let Some(Err(e)) = ack {
                            s.violate("COMMIT", format!("error ack: {e:?}"));
                            return EraEnd::Done;
                        }
                    }
                    Err(e) => {
                        // §8.4/§7.1: the pre-commit failed and the crate
                        // aborted the txn; the session retries.
                        if !retryable_error(&e) {
                            s.violate("COMMIT", format!("commit_submit: {e:?}"));
                            return EraEnd::Done;
                        }
                        if let Some(t) = s.ghost.txns.get_mut(&id) {
                            t.outcome = ghost::Outcome::Aborted;
                        }
                        s.resolve_pending = true;
                        retry_or_next(&mut s, i);
                    }
                }
                drop(s);
            }
            Action::Sess(i, SAction::AbortTxn) => {
                let txn = {
                    let mut s = sim.lock().expect("sim");
                    s.sessions[i].txn.take().expect("txn")
                };
                guards[i] = None;
                let id = txn.id;
                if let Err(e) = core.abort(txn) {
                    let mut s = sim.lock().expect("sim");
                    s.violate("ABORT", format!("abort: {e:?}"));
                    return EraEnd::Done;
                }
                let mut s = sim.lock().expect("sim");
                if let Some(t) = s.ghost.txns.get_mut(&id) {
                    t.outcome = ghost::Outcome::Aborted;
                }
                s.resolve_pending = true;
                retry_or_next(&mut s, i);
                drop(s);
            }
            other => {
                let mut s = sim.lock().expect("sim");
                exec(&mut s, other);
                check::post_step(&mut s);
                if s.violation.is_some() {
                    return EraEnd::Done;
                }
            }
        }
    }
}

fn force_abort_all(sim: &mut Sim, core: &Core<SimKv>) {
    for s in sim.sessions.iter_mut() {
        if let Some(t) = s.txn.take() {
            let id = t.id;
            if let Err(e) = core.abort(t) {
                sim.violate("ABORT", format!("force abort: {e:?}"));
                return;
            }
            if let Some(g) = sim.ghost.txns.get_mut(&id) {
                g.outcome = ghost::Outcome::Aborted;
            }
            s.program = None;
            s.wait = None;
            s.pending_wait = None;
            s.task = None;
            s.epq = None;
            s.phase = Phase::Idle;
        }
    }
    sim.trace
        .push(format!("{:>5} step-bound force abort", sim.steps));
}

fn retry_or_next(sim: &mut Sim, i: usize) {
    let s = &mut sim.sessions[i];
    s.task = None;
    s.epq = None;
    s.wait = None;
    s.pending_wait = None;
    s.ticket = None;
    s.ticket_txn = None;
    s.savepoints.clear();
    s.cancelled = false;
    s.streak = 0;
    if s.retries < 3 {
        s.retries += 1;
        s.phase = Phase::RetryBegin;
        s.stmt_idx = 0;
    } else {
        program_finished(sim, i);
    }
}

/// Drains the system at the end of a run: the commit thread (with ack
/// absorption), the resolver and SSI retention, until idle.
fn drain(sim: &Arc<Mutex<Sim>>, core: &Core<SimKv>, pipeline: &mut CommitPipeline<SimKv>) {
    for _ in 0..10_000 {
        let inflight = {
            let mut s = sim.lock().expect("sim");
            // Absorb acks first (the tickets of finished sessions).
            let acked: Vec<(TxnId, Ts)> = s
                .sessions
                .iter_mut()
                .filter_map(|ssn| {
                    let id = ssn.ticket_txn?;
                    let ack = ssn.ticket.as_ref()?.try_ack();
                    match ack {
                        Some(Ok(ts)) => {
                            ssn.ticket = None;
                            ssn.ticket_txn = None;
                            Some((id, ts))
                        }
                        _ => None,
                    }
                })
                .collect();
            for (id, ts) in acked {
                // process_group completed before this absorption; observe
                // the release so a later truncation passes I-VIS. Ts::ZERO
                // (the no-write fast path) is never a commit ts.
                let released = s.core.status.entry(id).is_some_and(|e| e.released);
                if ts != Ts::ZERO {
                    s.ghost.commit_at(id, ts);
                }
                if let Some(g) = s.ghost.txns.get_mut(&id) {
                    g.outcome = ghost::Outcome::Acked(ts);
                    g.released_seen |= released;
                    if !g.writes.is_empty() {
                        // §3 step 5 queued its intents for resolution.
                        s.resolve_pending = true;
                    }
                }
            }
            s.inflight.clear();
            let pending: Vec<TxnId> = s.sessions.iter().filter_map(|x| x.ticket_txn).collect();
            s.inflight.extend(pending);
            s.inflight.clone()
        };
        if !inflight.is_empty() {
            let group = pipeline.drain_available();
            pipeline.process_group(group);
        }
        let resolve = {
            let s = sim.lock().expect("sim");
            resolver_needed(&s)
        };
        if resolve {
            if let Err(e) = Resolver::run_once(core) {
                let mut s = sim.lock().expect("sim");
                s.violate("RESOLVER", format!("drain resolver: {e:?}"));
                return;
            }
            let mut s = sim.lock().expect("sim");
            s.resolve_pending = false;
        }
        {
            let ssi = sim.lock().expect("sim").ssi.clone();
            if let Some(ssi) = ssi {
                ssi.run_retention(core);
            }
        }
        let busy = {
            let s = sim.lock().expect("sim");
            !s.inflight.is_empty() || s.resolve_pending
        };
        if !busy {
            return;
        }
    }
    let mut s = sim.lock().expect("sim");
    s.violate("DRAIN", "the system did not reach quiescence".into());
}

/// Preloads one row per key (value 1) through the real API, so updates have
/// something to read. The preload txns are ghost-known commits. The Sim
/// mutex is never held across a `process_group` (the injected clock and the
/// probe take it themselves).
fn preload(sim: &Arc<Mutex<Sim>>, core: &Core<SimKv>, pipeline: &mut CommitPipeline<SimKv>) {
    let keys: Vec<Vec<u8>> = sim.lock().expect("sim").keys.clone();
    for key in keys {
        let txn = core.begin(Isolation::ReadCommitted);
        let id = txn.id;
        {
            let mut s = sim.lock().expect("sim");
            s.ghost.create(id, usize::MAX, Isolation::ReadCommitted);
        }
        let seq = match txn.next_seq() {
            Ok(s) => s,
            Err(e) => {
                let mut s = sim.lock().expect("sim");
                s.violate("PRELOAD", format!("next_seq: {e:?}"));
                return;
            }
        };
        let ctx = StmtCtx::new(core.visible_ts(), seq, seq);
        let mut task = KeyOpTask::new(
            &key,
            None,
            u64_payload(1),
            ctx,
            UniqueRule::Unique { same_row: None },
        );
        loop {
            match task.step(core, &txn) {
                Ok(Step::Done(_)) => break,
                Ok(Step::Again) => {}
                Ok(other) => {
                    let mut s = sim.lock().expect("sim");
                    s.violate("PRELOAD", format!("step {other:?}"));
                    return;
                }
                Err(e) => {
                    let mut s = sim.lock().expect("sim");
                    s.violate("PRELOAD", format!("step: {e:?}"));
                    return;
                }
            }
        }
        {
            let mut s = sim.lock().expect("sim");
            let bound = s.kv.rec_len();
            if let Some(t) = s.ghost.txns.get_mut(&id) {
                t.writes.push(ghost::GhostWrite {
                    seq,
                    key: key.clone(),
                    kind: GWrite::Set(u64_payload(1)),
                });
                t.placement_bound = bound;
            }
        }
        let ticket = match core.commit_submit(txn, SyncCommit::On) {
            Ok(t) => t,
            Err(e) => {
                let mut s = sim.lock().expect("sim");
                s.violate("PRELOAD", format!("commit: {e:?}"));
                return;
            }
        };
        let group = pipeline.drain_available();
        pipeline.process_group(group);
        match ticket.wait() {
            Ok(ts) => {
                let mut s = sim.lock().expect("sim");
                s.ghost.commit_at(id, ts);
                // Step 5 ran inside process_group before wait() returned;
                // observe the release here because the resolver below may
                // truncate the entry before any battery runs.
                let released = core.status.entry(id).is_some_and(|e| e.released);
                if let Some(t) = s.ghost.txns.get_mut(&id) {
                    t.outcome = ghost::Outcome::Acked(ts);
                    t.sync = Some(SyncCommit::On);
                    t.released_seen = released;
                }
            }
            Err(e) => {
                let mut s = sim.lock().expect("sim");
                s.violate("PRELOAD", format!("ack: {e:?}"));
                return;
            }
        }
        let _ = Resolver::run_once(core);
    }
    let _ = Resolver::run_once(core);
    let mut s = sim.lock().expect("sim");
    let steps = s.steps;
    let nkeys = s.cfg.keys;
    s.trace.push(format!("{steps:>5} preload {nkeys} keys"));
}

/// Codec-encoded logical keys: an `int8 asc` column, as the earlier tests
/// do (C-K1's golden corpus keys).
fn make_keyspace(n: usize) -> Vec<Vec<u8>> {
    use nucleus_codec::{KeyColumn, KeyType, Value as Cv};
    (0..n)
        .map(|i| {
            let mut out = Vec::new();
            nucleus_codec::encode_key(
                &[KeyColumn::asc(KeyType::Int8)],
                &[Some(Cv::Int8(i as i64))],
                &mut out,
            )
            .expect("codec key");
            out
        })
        .collect()
}
