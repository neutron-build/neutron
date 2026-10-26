//! The round harness (card X-5 Work 13; the round-engine parts of X-5's
//! `workload.rs` draft, finished under the S-BATON discipline).
//!
//! One round: a **scripted** workload (the seeded generator is X-5c's),
//! the crash, the round's media faults, recovery with nested recovery
//! crashes, the Work 2.3 extraction through the recovered store's own
//! read paths, the P-written oracle, the monitors, and the harness
//! checkpoint. Every oracle input comes from SimDisk state, parsed disk
//! bytes, or the store's read paths — never store memory (the one
//! exception the card names: the flip's reported enqueue position for
//! M-CKPT-T, Work 3.10).
//!
//! Determinism (S-DET): the schedule is a pure function of the seed; the
//! same inputs give a byte-identical digest (`tests/harness.rs`).
//!
//! Style: the step closures keep their nested conditionals un-collapsed
//! (each level names one event-bookkeeping decision).

#![allow(clippy::collapsible_if, clippy::collapsible_match)]
use std::collections::{BTreeMap, BTreeSet};

use crate::model::{CATALOG, LoadRec, Mode, Model, Op, PUBLIC, Scan, to_be64};
use crate::oracle::{
    Extracted, FaultTargets, Finding, OracleCfg, RawEntry, ReadFail, StateReader, TwinInfo, XxSite,
    check_round,
};
use crate::rng::{Rng, derive};
use crate::sim::{
    DiskEvent, FaultClass, FaultKnobs, HARNESS, MediaFault, Sched, SchedCore, SchedHost, SimDisk,
    SimFail, TaskId, WaitKey,
};
use crate::store::ckpt::{self, CkptHook};
use crate::store::tree;
use crate::store::wal::{self, K_CHUNK, K_COMMIT, K_MARKER};
use crate::store::{Cx, Fail, HasStore, Mutant, StoreCfg, StoreInner, amcheck, prims};

/// A read/serve record: `(file, offset, length)`.
type Reads = Vec<(usize, u64, u64)>;

/// Stream tags (independent splitmix64 derivations per concern).
const S_SCHED: u64 = 1;
const S_CRASH: u64 = 4;
const S_SAMPLE: u64 = 6;

/// One scripted operation of a task (card Work 13, "Workload").
#[derive(Debug, Clone)]
pub enum ScriptOp {
    /// A small logged transaction.
    Commit {
        /// Durability mode.
        sync_on: bool,
        /// The ops.
        ops: Vec<Op>,
    },
    /// A chunked transaction (§12.7): chunks first, then the marker.
    Chunked {
        /// Durability mode of the marker's ack.
        sync_on: bool,
        /// The ops.
        ops: Vec<Op>,
    },
    /// An unlogged bulk load into an unpublished tree (§12.8), with the
    /// pre-registration of §10.1 and publication after the forced
    /// checkpoint's meta is durable (I-BULK-PUBLISH).
    BulkLoad {
        /// The tree id.
        tree: u64,
        /// The entries.
        entries: Vec<(Vec<u8>, Vec<u8>)>,
    },
    /// A GC sweep over a tree (the §10.2 rule at the given watermark;
    /// `None` = the harness watermark: min(registered snapshots,
    /// visible_ts), never below the recovered gc_w).
    Gc {
        /// The tree.
        tree: u64,
        /// The watermark.
        w: Option<u64>,
    },
    /// Request a checkpoint and wait for it to complete.
    Checkpoint,
    /// Set a named park point's budget (the D5 split parking).
    Park {
        /// The park point.
        name: &'static str,
        /// Yields to burn there.
        budget: u64,
    },
    /// Burn `n` yields.
    Yields(u64),
    /// Wait until visible_ts ≥ t.
    WaitVisible(u64),
}

/// One task's script.
#[derive(Debug, Clone)]
pub struct ScriptTask {
    /// The ops, in order.
    pub ops: Vec<ScriptOp>,
}

/// The crash specification of a round (Work 13, "Crash").
#[derive(Debug, Clone)]
pub enum CrashSpec {
    /// Crash at `hook` of the checkpoint with index `ckpt_index`
    /// (0-based, in completion order).
    Hook {
        /// Which checkpoint.
        ckpt_index: u64,
        /// Which hook.
        hook: CkptHook,
    },
    /// Crash at the n-th yield point of a named task (script-level yield
    /// points: before every op and inside every park budget).
    TaskYield(TaskId, u64),
}

/// The round's script: tasks, service tasks, the crash and the nested
/// recovery crashes.
#[derive(Debug, Clone)]
pub struct Script {
    /// The scripted tasks (task ids 1..=len).
    pub tasks: Vec<ScriptTask>,
    /// Run a WAL writer task.
    pub writer: bool,
    /// Run a background evictor task.
    pub evictor: bool,
    /// Run a checkpointer task (every n commits and on force).
    pub checkpointer_every: Option<u64>,
    /// The crash point (None = no crash this round).
    pub crash: Option<CrashSpec>,
    /// Nested recovery crashes, each at a yield point inside recovery.
    pub nested: Vec<u64>,
}

impl Script {
    /// The total task count (script tasks + service tasks + harness).
    pub fn ntasks(&self) -> usize {
        1 + self.tasks.len()
            + usize::from(self.writer)
            + usize::from(self.evictor)
            + usize::from(self.checkpointer_every.is_some())
    }
}

/// The per-seed harness state (the world's harness slice): the model
/// hooks of Work 2.1 and the coordination flags the task bodies share.
#[derive(Debug, Default)]
pub struct HarnessState {
    /// The reference model.
    pub model: Model,
    /// Commit ts → (sync_on, record end lsn) — recorded at the enqueue.
    pub wal_commits: BTreeMap<u64, (bool, u64)>,
    /// The txn counter (ids).
    pub txn_counter: u64,
    /// Registered snapshots (long readers).
    pub snapshots: BTreeSet<u64>,
    /// Script tasks still running (`workload_done` = this reaching 0):
    /// the service tasks may only end once every script task has.
    pub tasks_left: usize,
    /// Published trees.
    pub published: BTreeSet<u64>,
    /// Bulk loads seen.
    pub loads: Vec<LoadRec>,
    /// The flips' reported enqueue positions (M-CKPT-T's hook, Work
    /// 3.10; copied into the outcome after the round).
    pub flip_enqueue_pos: Vec<u64>,
    /// Commits completed (the checkpoint trigger's counter).
    pub commits_done: u64,
    /// The scripted tasks are done.
    pub workload_done: bool,
    /// A forced checkpoint is pending (bulk load).
    pub force_ckpt: bool,
    /// A DEADLOCK was observed.
    pub deadlock: bool,
    /// A `SimFail::Baton` was observed.
    pub baton: bool,
    /// A task panicked.
    pub panicked: bool,
    /// A script task ended with a failure (a store or env error): the
    /// round must fail loudly — never a silent early exit that leaves
    /// the service tasks spinning.
    pub task_error: Option<String>,
}

/// The world of a harness round: the store's `Core` slices plus the
/// harness slice.
pub struct RoundWorld {
    /// Scheduling slice.
    pub sched: SchedCore,
    /// Disk slice.
    pub disk: SimDisk,
    /// Store slice.
    pub st: StoreInner,
    /// Harness slice.
    pub h: HarnessState,
}

impl crate::sim::SchedHost for RoundWorld {
    fn sched_mut(&mut self) -> &mut SchedCore {
        &mut self.sched
    }
}

impl crate::sim::HasDisk for RoundWorld {
    fn disk(&mut self) -> &mut SimDisk {
        &mut self.disk
    }
}

impl crate::store::HasStore for RoundWorld {
    fn store(&mut self) -> &mut StoreInner {
        &mut self.st
    }
}

impl RoundWorld {
    /// The harness slice.
    pub fn h(&mut self) -> &mut HarnessState {
        &mut self.h
    }
}

/// Clone a model field-by-field (`model.rs` is frozen; its fields are
/// public and its records are `Clone`).
pub fn clone_model(m: &Model) -> Model {
    let mut out = Model::new();
    out.commits = m.commits.clone();
    out.loads = m.loads.clone();
    out
}

/// Build a round world from a durable disk + store state.
pub fn round_world(seed: u64, disk: SimDisk, st: StoreInner) -> Sched<RoundWorld> {
    let host = RoundWorld {
        sched: SchedCore::new(1, seed),
        disk,
        st,
        h: HarnessState::default(),
    };
    Sched::new(host)
}

/// The store's create pre-pass (§2.3 + G6): one task, no crash points.
pub fn create_store(cfg: StoreCfg, seed: u64, uuid: u128) -> Result<(SimDisk, StoreInner), Fail> {
    let host = RoundWorld {
        sched: SchedCore::new(1, seed),
        disk: SimDisk::new(),
        st: StoreInner::empty(cfg.clone(), 0),
        h: HarnessState::default(),
    };
    let sched = Sched::new(host);
    let cx = Cx {
        w: &sched,
        t: HARNESS,
    };
    let st = ckpt::create(&cx, cfg, uuid)?;
    cx.with("h:create:install", |c| {
        *c.store() = st;
        Ok(())
    })?;
    let g = sched.lock();
    Ok((g.disk.clone(), g.st.clone()))
}

// ---- the commit path (Work 4.3) ----

/// The commit critical section (ts + enqueue, I-WAL-TS-ORDER), the
/// group-commit ack (§12.4), apply entry by entry as gated inserts, and
/// `mark_applied` (visible_ts advances in ts order). Chunked
/// transactions write their chunks first and apply after the marker's
/// group sync (§12.7). Returns the commit ts.
pub fn commit_txn(
    cx: &Cx<'_, RoundWorld>,
    ops: &[Op],
    sync_on: bool,
    chunked: bool,
) -> Result<u64, Fail> {
    let txn = cx.with("h:txn", |c| {
        let h = c.h();
        h.txn_counter += 1;
        Ok((1u32, h.txn_counter))
    })?;
    if chunked {
        let mut rest = ops.to_vec();
        while !rest.is_empty() {
            let take = 20.min(rest.len());
            let chunk: Vec<Op> = rest.drain(..take).collect();
            let mut payload = Vec::new();
            for op in &chunk {
                wal::enc_op(&to_wop(op), &mut payload);
            }
            let (lsn, _len) = wal::wal_enqueue(cx, K_CHUNK, txn, 0, &payload)?;
            cx.with("h:openchunk", |c| {
                let st = c.store();
                st.wal.open_chunks.entry(txn).or_insert(lsn);
                Ok(())
            })?;
        }
        // A named park point (Work 13): D3 holds the committer between
        // its chunks and its marker, so a flip sees the transaction
        // open and the marker lands later, before the crash.
        cx.park("chunked:premark")?;
    }
    // The critical section: take the flag, assign the ts, enqueue the
    // record at its LSN, release the flag (one critical section;
    // TS-AFTER-APPEND ends it after the ts assignment).
    let ts_after_append = cx.with("h:mut", |c| {
        Ok(matches!(c.store().cfg.mutant, Some(Mutant::TsAfterAppend)))
    })?;
    loop {
        let taken = cx.with("h:cs", |c| {
            let st = c.store();
            if st.seq.commit_cs {
                Ok(false)
            } else {
                st.seq.commit_cs = true;
                Ok(true)
            }
        })?;
        if taken {
            break;
        }
        cx.wait_for(WaitKey::Applied)?;
    }
    let ts = cx.with("h:ts", |c| {
        let st = c.store();
        let t = st.seq.next_ts;
        st.seq.next_ts += 1;
        Ok(t)
    })?;
    if ts_after_append && !chunked {
        // The mutant: the critical section ends here, a yield happens,
        // and another committer can take a higher ts at a lower LSN.
        cx.with("h:cs2", |c| {
            c.store().seq.commit_cs = false;
            Ok(())
        })?;
        cx.w.wake(WaitKey::Applied);
        cx.yield_now("h:ts-after-append")?;
        loop {
            let taken = cx.with("h:cs3", |c| {
                let st = c.store();
                if st.seq.commit_cs {
                    Ok(false)
                } else {
                    st.seq.commit_cs = true;
                    Ok(true)
                }
            })?;
            if taken {
                break;
            }
            cx.wait_for(WaitKey::Applied)?;
        }
    }
    let mut payload = Vec::new();
    if chunked {
        let nchunks = ops.len().div_ceil(20) as u32;
        payload.extend_from_slice(&nchunks.to_le_bytes());
    } else {
        for op in ops {
            wal::enc_op(&to_wop(op), &mut payload);
        }
    }
    let kind = if chunked { K_MARKER } else { K_COMMIT };
    let (lsn, len) = wal::wal_enqueue(cx, kind, txn, ts, &payload)?;
    let end_lsn = lsn + len;
    let sync_flag = sync_on;
    // Record the commit for the model (2.1) **at the enqueue** — where
    // `HarnessState.wal_commits`'s own contract places it. The draft
    // recorded it after the ack waits, so a committer that crashed
    // mid-ack (its record already durable) never entered the model: the
    // written/acked derivation then capped `hi` below the replayed
    // record and O-1 (c) failed the round on a raw entry above every
    // candidate cut (found by D7's round 2; the flags, not the
    // recording, decide visibility).
    let mode = if sync_on { Mode::SyncOn } else { Mode::SyncOff };
    cx.with("h:model", |c| {
        let h = c.h();
        h.model.commit(ts, mode, ops.to_vec());
        h.wal_commits.insert(ts, (sync_on, end_lsn));
        h.commits_done += 1;
        Ok(())
    })?;
    // (§12.6: the marker's enqueue does not close the chunk bookkeeping
    // — the entry lives until a flip whose T covers the marker, so the
    // marked case still pulls wal_start below the chunks.)
    if chunked {
        // §12.7: a chunked transaction applies after its marker's group
        // sync — wait for durability first.
        if sync_flag {
            wal::wal_wait_durable(cx, end_lsn)?;
        } else {
            wal::wal_wait_written(cx, end_lsn)?;
        }
    }
    // Release the critical section.
    cx.with("h:cs4", |c| {
        c.store().seq.commit_cs = false;
        Ok(())
    })?;
    cx.w.wake(WaitKey::Applied);
    if chunked {
        // A named park point (Work 13): D2 holds the committer after
        // its commit is recorded (the checkpoint trigger fires) but
        // before its apply, so a flip lands with the marker above T —
        // §12.6's marked case.
        cx.park("chunked:preapply")?;
    } else {
        // The same hold for a small commit (D4's c1: recorded, acked at
        // the write, apply pending at the flip).
        cx.park("commit:preapply")?;
    }
    // The ack (§12.4): SyncOn at the covering sync, SyncOff at the write.
    if sync_flag {
        wal::wal_wait_durable(cx, end_lsn)?;
    } else {
        wal::wal_wait_written(cx, end_lsn)?;
    }
    // Apply: entry by entry, as gated inserts, in ascending user-key
    // order per tree (§9.1).
    let mut ordered: Vec<&Op> = ops.iter().collect();
    ordered.sort_by_key(|op| match op {
        Op::Put { tree, key, .. } | Op::Delete { tree, key } => (*tree, key.clone()),
        _ => (0, Vec::new()),
    });
    for op in ordered {
        // A Put/Delete can race the apply of an **earlier commit's**
        // CreateTree for its tree (applies finish out of ts order,
        // §12.4): the directory entry is not installed yet, and the
        // gated insert would fail "tree missing in directory" — a
        // harness orchestration defect, not a store failure (recovery
        // replays in LSN order and never sees it). The WAL guarantees
        // that create's record precedes this one at a lower LSN, so its
        // apply is pending in this workload: wait for it. A same-commit
        // create applies first here (CreateTree sorts to the front of
        // `ordered`).
        if let Op::Put { tree, .. } | Op::Delete { tree, .. } = op {
            loop {
                let (present, left, failed) = cx.with("h:applydir", |c| {
                    Ok((
                        c.store().dir.contains_key(tree),
                        c.h().tasks_left,
                        c.h().task_error.is_some(),
                    ))
                })?;
                if present {
                    break;
                }
                if failed || left <= 1 {
                    // No other committer can still install the tree:
                    // a malformed script — fail the op loudly rather
                    // than parking forever (the service tasks would
                    // keep the baton circulating and the deadlock
                    // detector would never fire).
                    return Err(Fail::Invalid(format!("tree {tree} missing for apply")));
                }
                cx.wait_for(WaitKey::Applied)?;
            }
        }
        ckpt::apply_op(cx, &to_wop(op), ts, false)?;
        if matches!(op, Op::CreateTree { .. }) {
            // A freshly installed directory entry can release another
            // committer's apply wait above: `Applied` is the wake key
            // (a new entry is progress; Mesa re-check on wake).
            cx.w.wake(WaitKey::Applied);
        }
        // A named park point (Work 13): D6/D13 hold the committer
        // **between** apply entries, so a flip's step 2 images a
        // partially applied commit — recovery then replays the whole
        // commit and its first entries are identical re-inserts (§9.1).
        cx.park("apply:mid")?;
    }
    mark_applied(cx, ts)?;
    Ok(ts)
}

/// `mark_applied`: visible_ts advances in ts order; a committer waits
/// for its turn to pass (§12.4's visibility rule).
fn mark_applied(cx: &Cx<'_, RoundWorld>, ts: u64) -> Result<(), Fail> {
    cx.with("h:applied", |c| {
        c.store().seq.applied.insert(ts);
        Ok(())
    })?;
    loop {
        let advanced = cx.with("h:vis", |c| {
            let st = c.store();
            let mut advanced = false;
            while st.seq.applied.remove(&(st.seq.visible_ts + 1)) {
                st.seq.visible_ts += 1;
                advanced = true;
            }
            Ok(advanced)
        })?;
        if advanced {
            cx.w.wake(WaitKey::Applied);
        }
        let vis = cx.with("h:vis2", |c| Ok(c.store().seq.visible_ts))?;
        if vis >= ts {
            return Ok(());
        }
        cx.wait_for(WaitKey::Applied)?;
    }
}

/// `Op` → `WOp`.
pub fn to_wop(op: &Op) -> wal::WOp {
    match op {
        Op::CreateTree { tree } => wal::WOp::CreateTree { tree: *tree },
        Op::DropTree { tree } => wal::WOp::DropTree { tree: *tree },
        Op::Put { tree, key, value } => wal::WOp::Put {
            tree: *tree,
            key: key.clone(),
            value: value.clone(),
        },
        Op::Delete { tree, key } => wal::WOp::Delete {
            tree: *tree,
            key: key.clone(),
        },
    }
}

// ---- the task bodies ----

/// Run the workload tasks of `script` under the baton; returns when all
/// end (crash or completion). Records DEADLOCK/Baton/panic in the host.
pub fn run_script(sched: &Sched<RoundWorld>, script: &Script, seed: u64) {
    let ntasks = script.ntasks();
    {
        let mut g = sched.lock();
        g.sched = SchedCore::new(ntasks, derive(seed, S_SCHED));
        g.sched.ntasks = ntasks;
    }
    let panicked = std::sync::atomic::AtomicBool::new(false);
    let mut deadlock = false;
    let crash = std::sync::Arc::new(script.crash.clone());
    let checkpointer_every = script.checkpointer_every;
    let crash_for_join = (*crash).clone();
    let nscript = script.tasks.len();
    let writer = script.writer;
    let evictor = script.evictor;
    std::thread::scope(|scope| {
        let mut handles = Vec::new();
        {
            let mut g = sched.lock();
            g.h.tasks_left = nscript;
        }
        for t in 1..ntasks {
            let sched_ref = sched;
            let p = &panicked;
            let tasks = script.tasks.clone();
            let crash = std::sync::Arc::clone(&crash);
            handles.push(scope.spawn(move || {
                let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_task(
                        sched_ref,
                        t,
                        tasks,
                        nscript,
                        writer,
                        evictor,
                        checkpointer_every,
                        &crash,
                    )
                }));
                match r {
                    Err(_) => {
                        p.store(true, std::sync::atomic::Ordering::SeqCst);
                    }
                    Ok(Err(SimFail::Baton(_))) => {
                        // The detector fired (or a world acquisition
                        // failed): a PANIC-class per-seed failure, never
                        // a pass (card Work 11, rule 4).
                        let mut g = sched_ref.lock();
                        g.h.baton = true;
                    }
                    Ok(_) => {}
                }
                sched_ref.finish(t);
            }));
        }
        let _ = sched.start();
        for h in handles {
            let _ = h.join();
        }
        let g = sched.lock();
        if g.sched.deadlock {
            deadlock = true;
        }
        if g.sched.crashed && crash_for_join.is_some() {
            // A crash fired; expected for crashing rounds.
        }
    });
    let mut g = sched.lock();
    if deadlock {
        g.h.deadlock = true;
    }
    if panicked.load(std::sync::atomic::Ordering::SeqCst) {
        g.h.panicked = true;
    }
    // A `SimFail::Baton` seen by any task (the scheduler detector or a
    // failed world acquisition on a store path) fails the round: fold it
    // into the harness flag (the gate's minor finding: the flag existed
    // but was never set).
    if g.st.baton_fail.is_some() {
        g.h.baton = true;
    }
}

#[allow(clippy::too_many_arguments)]
fn run_task(
    sched: &Sched<RoundWorld>,
    t: TaskId,
    tasks: Vec<ScriptTask>,
    nscript: usize,
    writer: bool,
    evictor: bool,
    checkpointer_every: Option<u64>,
    crash: &Option<CrashSpec>,
) -> Result<(), SimFail> {
    let cx = Cx { w: sched, t };
    // The task's script-level yield counter (crash arming).
    let mut yields = 0u64;
    macro_rules! yld {
        () => {{
            if let Some(CrashSpec::TaskYield(want_t, want_n)) = crash
                && *want_t == t
                && *want_n == yields + 1
            {
                arm_crash(sched);
                return Err(SimFail::Crashed);
            }
            yields += 1;
            cx.yield_now("h:task").map_err(fail_of)?;
        }};
    }
    // Script tasks.
    if t <= nscript {
        if let Some(task) = tasks.get(t - 1) {
            for op in task.ops.iter() {
                yld!();
                if let Err(e) = script_op(&cx, op) {
                    // A failed script op is a round failure, recorded
                    // and reported — never a silent early exit (the
                    // service tasks would spin on a workload that can
                    // no longer finish). The recorded message carries
                    // the **original** failure: `script_op` returns the
                    // store's `Fail` unchanged (the gate's N3 — the
                    // draft's `fail_of` folded every non-sim store
                    // error into `SimFail::Crashed`, so D18's clean
                    // round recorded task_error="Crashed" and hid the
                    // real "tree 2 missing in directory" behind a
                    // phantom crash). The crash delivery itself (every
                    // task unwinds with `Crashed` once the world
                    // crashes) is the round's normal end, not a
                    // failure.
                    let world_crashed = cx
                        .with("h:taskerr", |c| Ok(c.sched_mut().crashed))
                        .unwrap_or(true);
                    if !world_crashed {
                        cx.with("h:taskerr2", |c| {
                            let h = c.h();
                            if h.task_error.is_none() {
                                h.task_error = Some(format!("{e:?}"));
                            }
                            h.workload_done = true;
                            Ok(())
                        })
                        .ok();
                        cx.w.wake(WaitKey::WalAcks);
                        cx.w.wake(WaitKey::MetaDurable);
                        cx.w.wake(WaitKey::Applied);
                    }
                    return Err(match e {
                        Fail::Sim(s) => s,
                        _ => SimFail::Internal("script op failed"),
                    });
                }
            }
        }
        // This script task is done. `workload_done` means *all* script
        // tasks are: a service task that exits while another script
        // task still holds a request (a forced checkpoint, a WAL batch)
        // would strand it (the scheduler would report DEADLOCK — loud,
        // but a harness defect all the same).
        cx.with("h:done", |c| {
            let h = c.h();
            h.tasks_left = h.tasks_left.saturating_sub(1);
            h.workload_done = h.tasks_left == 0 || h.task_error.is_some();
            Ok(())
        })
        .map_err(fail_of)?;
        cx.w.wake(WaitKey::WalAcks);
        cx.w.wake(WaitKey::MetaDurable);
        cx.w.wake(WaitKey::Applied);
        return Ok(());
    }
    let mut idx = t - nscript;
    if writer {
        if idx == 1 {
            return writer_body(&cx);
        }
        idx -= 1;
    }
    if evictor {
        if idx == 1 {
            return evictor_body(&cx);
        }
        idx -= 1;
    }
    if let Some(every) = checkpointer_every
        && idx == 1
    {
        return checkpointer_body(&cx, every, crash);
    }
    Ok(())
}

/// Run one scripted op. Returns the store's `Fail` unchanged (every
/// `Cx` call already yields `Fail`): the task runner records the
/// original error, never a masked one (the gate's N3).
fn script_op(cx: &Cx<'_, RoundWorld>, op: &ScriptOp) -> Result<(), Fail> {
    match op {
        ScriptOp::Commit { sync_on, ops } => commit_txn(cx, ops, *sync_on, false).map(|_| ()),
        ScriptOp::Chunked { sync_on, ops } => commit_txn(cx, ops, *sync_on, true).map(|_| ()),
        ScriptOp::Gc { tree, w } => gc_sweep(cx, *tree, *w),
        ScriptOp::Checkpoint => {
            cx.with("h:force", |c| {
                c.h().force_ckpt = true;
                Ok(())
            })?;
            wait_ckpt(cx)
        }
        ScriptOp::Park { name, budget } => cx.with("h:park", |c| {
            c.store().parks.insert(name, *budget);
            Ok(())
        }),
        ScriptOp::Yields(n) => {
            for _ in 0..*n {
                cx.yield_now("h:yields")?;
            }
            Ok(())
        }
        ScriptOp::WaitVisible(v) => {
            loop {
                let vis = cx.with("h:waitv", |c| Ok(c.store().seq.visible_ts))?;
                if vis >= *v {
                    break;
                }
                cx.wait_for(WaitKey::Applied)?;
            }
            Ok(())
        }
        ScriptOp::BulkLoad { tree, entries } => bulk_load(cx, *tree, entries),
    }
}

/// A full GC sweep over a tree (loop gc_step until done).
pub fn gc_sweep(cx: &Cx<'_, RoundWorld>, t: u64, w: Option<u64>) -> Result<(), Fail> {
    let w = match w {
        Some(w) => w,
        None => cx.with("h:gcw", |c| {
            let (vis, gc_w) = {
                let st = c.store();
                (st.seq.visible_ts, st.meta.gc_w)
            };
            let snap = c.h().snapshots.iter().copied().min().unwrap_or(vis);
            Ok(snap.min(vis).max(gc_w))
        })?,
    };
    let mut resume = Vec::new();
    loop {
        let root = cx.with("h:gcroot", |c| Ok(c.store().dir.get(&t).map(|e| e.root)))?;
        let Some(root) = root else {
            return Ok(());
        };
        let next = tree::gc_step(cx, t, root, w, &resume)?;
        if next.is_empty() {
            return Ok(());
        }
        resume = next;
        cx.yield_now("h:gcyield")?;
    }
}

/// The unlogged bulk load (§12.8): CreateTree (logged), a snapshot
/// registered below `S_L`, the unlogged inserts, the forced checkpoint,
/// then publication.
fn bulk_load(cx: &Cx<'_, RoundWorld>, t: u64, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<(), Fail> {
    commit_txn(cx, &[Op::CreateTree { tree: t }], true, false)?;
    // §10.1: register below S_L, then pick S_L.
    let s_l = cx.with("h:sl", |c| {
        let r = c.store().seq.visible_ts;
        c.h().snapshots.insert(r);
        let next = c.store().seq.next_ts.max(r + 1);
        let t = next;
        c.store().seq.next_ts = c.store().seq.next_ts.max(t + 1);
        // `t` is drawn from the commit sequencer but is not a commit
        // (§10.1's S_L): pre-mark it applied so `mark_applied`'s
        // contiguous walk passes through it. Without this the ts is a
        // hole — visible_ts stalls below every later commit forever and
        // the round deadlocks (the driver hang the X-5b gate's R5
        // reported; the drawn ts sat in `applied`'s way, unreachable).
        c.store().seq.applied.insert(t);
        Ok(t)
    })?;
    let root = tree::dir_root_of(cx, t)?;
    for (k, v) in entries {
        let ekey = tree::enc_ekey(k, s_l);
        let mut val = vec![0u8];
        val.extend_from_slice(v);
        tree::tree_insert(cx, t, root, &ekey, &val, false)?;
    }
    cx.with("h:load", |c| {
        c.h().model.load(t, s_l, entries.to_vec());
        c.h().loads.push(LoadRec {
            tree: t,
            s_l,
            entries: entries.to_vec(),
        });
        c.h().force_ckpt = true;
        Ok(())
    })?;
    // §12.8 step 3: publication waits for the forced checkpoint's meta
    // to become durable (I-BULK-PUBLISH). **PUBLISH-BEFORE-META's single
    // site**: the mutant skips the wait, so the publish commit can be
    // acked while the unlogged build exists nowhere durable — a crash
    // right after the ack leaves a published catalog row over an empty
    // tree, and O-1 fails.
    let publish_early = cx.with("h:pubmut", |c| {
        Ok(matches!(
            c.store().cfg.mutant,
            Some(Mutant::PublishBeforeMeta)
        ))
    })?;
    if !publish_early {
        wait_ckpt(cx)?;
    }
    // Publication (§12.8 step 3): after the forced meta is durable.
    commit_txn(
        cx,
        &[Op::Put {
            tree: CATALOG,
            key: to_be64(t),
            value: PUBLIC.to_vec(),
        }],
        true,
        false,
    )?;
    cx.with("h:pub", |c| {
        c.h().published.insert(t);
        Ok(())
    })
}

/// Wait for the pending forced checkpoint to complete (its meta
/// durable). Parks on [`WaitKey::MetaDurable`]: every checkpoint's
/// step-5 completion wakes it, and the checkpointer task never parks
/// (it spin-yields), so a checkpoint that never comes surfaces as the
/// scheduler's DEADLOCK, never a silent hang.
fn wait_ckpt(cx: &Cx<'_, RoundWorld>) -> Result<(), Fail> {
    loop {
        let (pending, open) = cx.with("h:ckptw", |c| {
            let h = c.h();
            Ok((h.force_ckpt, c.store().ckpt_open))
        })?;
        if !pending && !open {
            return Ok(());
        }
        cx.wait_for(WaitKey::MetaDurable)?;
    }
}

fn writer_body(cx: &Cx<'_, RoundWorld>) -> Result<(), SimFail> {
    loop {
        let any = cx
            .with("h:writer", |c| {
                if c.sched_mut().crashed {
                    return Ok(false);
                }
                Ok(!c.store().wal.buf.is_empty())
            })
            .map_err(fail_of)?;
        if any {
            wal::wal_writer_step(cx).map_err(fail_of)?;
        } else {
            let done = cx
                .with("h:writer2", |c| Ok(c.h().workload_done))
                .map_err(fail_of)?;
            if done {
                return Ok(());
            }
            cx.yield_now("h:writer").map_err(fail_of)?;
        }
    }
}

fn evictor_body(cx: &Cx<'_, RoundWorld>) -> Result<(), SimFail> {
    loop {
        let crashed = cx
            .with("h:ev", |c| Ok(c.sched_mut().crashed))
            .map_err(fail_of)?;
        if crashed {
            return Ok(());
        }
        if let Err(e) = prims::evict_one(cx, false) {
            return match e {
                Fail::Sim(s) => Err(s),
                _ => Ok(()),
            };
        }
        let done = cx
            .with("h:ev2", |c| Ok(c.h().workload_done))
            .map_err(fail_of)?;
        if done {
            return Ok(());
        }
        cx.yield_now("h:evictor").map_err(fail_of)?;
    }
}

fn checkpointer_body(
    cx: &Cx<'_, RoundWorld>,
    every: u64,
    crash: &Option<CrashSpec>,
) -> Result<(), SimFail> {
    let mut idx = 0u64;
    loop {
        let (trigger, final_ckpt) = cx
            .with("h:ck", |c| {
                if c.sched_mut().crashed {
                    return Ok((true, true));
                }
                let h = c.h();
                let by_commits = h.commits_done >= (idx + 1) * every || h.force_ckpt;
                Ok((
                    by_commits || h.workload_done,
                    h.workload_done && !by_commits,
                ))
            })
            .map_err(fail_of)?;
        if trigger {
            cx.with("h:ck2", |c| {
                let (f, n) = {
                    let h = c.h();
                    (h.force_ckpt, h.commits_done)
                };
                let _ = n;
                c.h().force_ckpt = false;
                let _ = f;
                Ok(())
            })
            .map_err(fail_of)?;
            let fire_at = match crash {
                Some(CrashSpec::Hook { ckpt_index, hook }) if *ckpt_index == idx => Some(*hook),
                _ => None,
            };
            let mut hooks = |hook: CkptHook| -> Result<(), Fail> {
                if Some(hook) == fire_at {
                    arm_crash(cx.w);
                    return Err(Fail::Sim(SimFail::Crashed));
                }
                Ok(())
            };
            idx += 1;
            let r = ckpt::checkpoint(cx, &mut hooks);
            match r {
                Err(Fail::Sim(SimFail::Crashed)) => return Ok(()),
                Err(Fail::Sim(s)) => return Err(s),
                Err(e) => {
                    // A checkpoint that fails without a crash (the
                    // I-GCW fail-stop, a poisoned store) is a round
                    // failure: recorded, never swallowed (the D12b
                    // shape — the draft exited silently here and the
                    // round passed with no meta written at all).
                    cx.with("h:ckfail", |c| {
                        let h = c.h();
                        if h.task_error.is_none() {
                            h.task_error = Some(format!("checkpoint failed: {e:?}"));
                        }
                        h.workload_done = true;
                        Ok(())
                    })
                    .ok();
                    return Ok(());
                }
                Ok(()) => {}
            }
            if final_ckpt {
                return Ok(());
            }
        } else {
            let done = cx
                .with("h:ck3", |c| Ok(c.h().workload_done))
                .map_err(fail_of)?;
            if done {
                return Ok(());
            }
            cx.yield_now("h:ckpt").map_err(fail_of)?;
        }
    }
}

/// Deliver the crash to the world (Work 13's hook and task-yield crash
/// points): every task unwinds with `Crashed` at its next scheduler call.
pub fn arm_crash(w: &Sched<RoundWorld>) {
    w.deliver_crash();
}

fn fail_of(f: Fail) -> SimFail {
    match f {
        Fail::Sim(s) => s,
        _ => SimFail::Crashed,
    }
}

// ---- the round outcome and engine ----

/// A directed media fault the round applies after F-CRASH (Work 6's
/// deterministic targets: D9's dropped rename uses the crash, D10b/D17
/// apply named faults).
pub type DirectedFault = MediaFault;

/// What one round produced (the Work 7 per-round record fields that exist
/// at this stage, plus the twin's). Not `Clone`: the end model is a
/// `Model` (frozen, not `Clone`) — copy fields, or rebuild via
/// [`clone_model`].
#[derive(Debug)]
pub struct RoundOutcome {
    /// The matched cut `c`, or the O-1 failure text.
    pub o1: Result<u64, String>,
    /// O-1f: the frontier is one of the satisfying cuts.
    pub o1f_ok: bool,
    /// O-2 failure.
    pub o2_fail: Option<String>,
    /// O-3 failure.
    pub o3_fail: Option<String>,
    /// O-3 (a)'s input: some life of this round served corruption (the
    /// workload pass, the recovery pass(es), the extraction's own
    /// accepts, or X5-R5's R-taint). Pinned by D17's CRC-SKIP arm — the
    /// reported `o3_fail` text can be overwritten by O-3 (b)–(d), so the
    /// directed arm needs the flag itself.
    pub served: bool,
    /// The round passes.
    pub pass: bool,
    /// `lo` (Work 2.2).
    pub lo: u64,
    /// `hi` (Work 2.2).
    pub hi: u64,
    /// The recovered frontier.
    pub frontier: u64,
    /// The opened meta's `gc_w`.
    pub gc_w: u64,
    /// The opened meta's `T`.
    pub opened_t: u64,
    /// XX001 detection sites.
    pub detections: Vec<&'static str>,
    /// Monitor hits.
    pub monitors: Vec<&'static str>,
    /// Acked commits lost above the cut (reported).
    pub acked_lost: Vec<(u64, bool)>,
    /// Amcheck (i) findings.
    pub amcheck_i: Vec<Finding>,
    /// Amcheck (ii) findings.
    pub amcheck_ii: Vec<Finding>,
    /// X5-R5: the opened meta is a rollback.
    pub meta_rollback: bool,
    /// X5-R5: the exception set R.
    pub rollback_slots: BTreeSet<u64>,
    /// The durable disk at the round's end (after recovery + the harness
    /// checkpoint).
    pub end_disk: SimDisk,
    /// The crashed disk after F-CRASH and the round's media faults,
    /// before recovery (the directed tests build their faults from its
    /// durable-write history; Work 6's deterministic targets).
    pub crashed_disk: SimDisk,
    /// The recovered store's state (for the next round).
    pub end_store: Option<StoreInner>,
    /// The truncation cut for the model (`c`).
    pub cut: u64,
    /// Whether the round was judged class C / MP / MW (for the record).
    pub class: FaultClass,
    /// A DEADLOCK occurred.
    pub deadlock: bool,
    /// A SimFail::Baton was observed (RoundOutcome).
    pub baton: bool,
    /// A task panicked.
    pub panicked: bool,
    /// A script task failed (store/env error); the round fails.
    pub task_error: Option<String>,
    /// The round's end model — every commit of this round applied and
    /// then **truncated at the cut `c`** (card Work 2.1: "rounds
    /// truncate the model at c_r"), so the next round of a multi-round
    /// seed starts from it (D7's zombie shape and X-5c's campaign).
    pub end_model: Model,
    /// The digest of the canonical record (byte-identical for the same
    /// inputs; `tests/harness.rs`).
    pub digest: u64,
}

impl Default for RoundOutcome {
    fn default() -> Self {
        Self {
            o1: Err("unset".to_owned()),
            o1f_ok: false,
            o2_fail: None,
            o3_fail: None,
            served: false,
            pass: false,
            lo: 0,
            hi: 0,
            frontier: 0,
            gc_w: 0,
            opened_t: 0,
            detections: Vec::new(),
            monitors: Vec::new(),
            acked_lost: Vec::new(),
            amcheck_i: Vec::new(),
            amcheck_ii: Vec::new(),
            meta_rollback: false,
            rollback_slots: BTreeSet::new(),
            end_disk: SimDisk::new(),
            crashed_disk: SimDisk::new(),
            end_store: None,
            cut: 0,
            class: FaultClass::C,
            deadlock: false,
            baton: false,
            panicked: false,
            task_error: None,
            end_model: Model::new(),
            digest: 0,
        }
    }
}

impl RoundOutcome {
    /// The canonical digest input.
    fn digest_input(&self) -> String {
        let mut s = String::new();
        s.push_str(&format!(
            "{:?}|{:?}|{:?}|{:?}|{}|{}|{}|{}|{}",
            self.o1,
            self.o2_fail,
            self.o3_fail,
            self.detections,
            self.lo,
            self.hi,
            self.frontier,
            self.gc_w,
            self.pass
        ));
        s.push_str(&format!(
            "|{:?}|{:?}|{}|{}|{}|{:?}",
            self.monitors,
            self.acked_lost,
            self.meta_rollback,
            self.deadlock,
            self.baton,
            self.task_error
        ));
        s
    }
}

/// The round's fault configuration.
#[derive(Debug, Clone)]
pub struct RoundFaults {
    /// The class (C rounds apply F-CRASH only).
    pub class: FaultClass,
    /// F-CRASH knobs.
    pub knobs: FaultKnobs,
    /// Directed media faults applied after F-CRASH (empty for random
    /// draws; X-5c's generator draws them itself).
    pub directed: Vec<DirectedFault>,
    /// Instrumentation-removal knob (the gate's round-2 G5, call-site
    /// level): force X5-R5's R-taint to report nothing. D10b's arm 6
    /// pins it — with the wiring in, a reused slot accepted through
    /// image k−1's stale reference fails O-3 (a); forced off, the same
    /// round passes silently. The campaign runs with it off (false).
    pub no_r_taint: bool,
}

/// Run one round: workload → crash → faults → recovery (nested crashes)
/// → extraction → oracle → monitors → the harness checkpoint. Returns
/// the outcome; the end state feeds the next round.
#[allow(clippy::too_many_arguments)]
pub fn run_round(
    seed: u64,
    round: u32,
    script: &Script,
    faults: &RoundFaults,
    cfg: StoreCfg,
    start_disk: SimDisk,
    start_store: StoreInner,
    model: &Model,
    c_prev: u64,
    mut twin: Option<&mut Vec<(usize, u64, u64)>>,
) -> RoundOutcome {
    // ---- the workload pass ----
    let ntasks = script.ntasks();
    let sched = {
        let host = RoundWorld {
            sched: SchedCore::new(ntasks, derive(seed * 1_000_003 + round as u64, S_SCHED)),
            disk: start_disk.clone(),
            st: start_store.clone(),
            h: HarnessState {
                model: clone_model(model),
                ..HarnessState::default()
            },
        };
        Sched::new(host)
    };
    run_script(&sched, script, seed);
    let (mut disk, mut model, flip_pos, flags, wal_commits, events) = {
        let g = sched.lock();
        (
            g.disk.clone(),
            clone_model(&g.h.model),
            g.st.wal.flip_enqueue_pos.clone(),
            (
                g.h.deadlock,
                g.h.baton,
                g.h.panicked,
                g.h.task_error.clone(),
            ),
            g.h.wal_commits.clone(),
            g.disk.events.clone(),
        )
    };
    // Work 2.1: written/acked from the disk events — the write carrying
    // the commit record's last byte returned, and (SyncOn) a WAL sync
    // covering it returned / (SyncOff) the write returned.
    let (written_ts, acked_ts) = derive_written_acked(&events, &wal_commits);
    for ts in written_ts {
        model.mark_written(ts);
    }
    for ts in acked_ts {
        model.mark_acked(ts);
    }

    // ---- the crash and the faults ----
    if script.crash.is_some() {
        let mut frng = Rng::new(derive(seed * 100 + round as u64, S_CRASH));
        let _ = disk.apply_crash(&mut frng, &faults.knobs);
        for f in faults.directed.iter() {
            disk.apply_one_media_fault(f);
        }
        disk.new_round();
    }
    let media_targets = if faults.directed.is_empty() {
        FaultTargets::default()
    } else {
        let pf = disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
        FaultTargets::collect(&disk.media.clone(), pf)
    };

    // ---- amcheck (i): on the crashed image, before recovery ----
    let (amcheck_i, image_slots, meta_i) = durable_image(&disk, &cfg);
    let pf = disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
    let tainted_slots: BTreeSet<u64> = image_slots
        .iter()
        .copied()
        .filter(|s| disk.range_tainted(pf, s * 4096, 4096))
        .collect();

    // ---- recovery with nested crashes ----
    let mut nested_fired = 0u32;
    let mut recovery_disk = disk.clone();
    let mut recovery_reads: Vec<(usize, u64, u64)> = Vec::new();
    // Served corruption seen by the recovery world(s) (O-3 (a): the
    // accepts of every life of this round are judged — the recovery
    // pass reads the crashed disk, so its accepts are where a served
    // corruption shows; without this the harness would judge only the
    // extraction's accepts).
    let mut recovery_served: Vec<(usize, u64, u64)> = Vec::new();
    // The interleaved history of the failed lives (the Err branch's
    // R-taint input).
    let mut recovery_events: Vec<DiskEvent> = Vec::new();
    let recovery = loop {
        let world = round_world(
            seed,
            recovery_disk.clone(),
            StoreInner::empty(cfg.clone(), 0),
        );
        let cx = Cx {
            w: &world,
            t: HARNESS,
        };
        let arm = (nested_fired as usize) < script.nested.len();
        if arm {
            let at = script
                .nested
                .get(nested_fired as usize)
                .copied()
                .unwrap_or(0);
            cx.with("h:nest", |c| {
                c.sched_mut().crash_at_yield = Some(at);
                Ok(())
            })
            .ok();
        }
        let reads_before = world.lock().disk.reads.len();
        match ckpt::recover(&cx, cfg.clone()) {
            Ok((st, frontier)) => {
                let (reads, served): (Reads, Reads) = {
                    let g = world.lock();
                    (
                        g.disk
                            .reads
                            .iter()
                            .skip(reads_before)
                            .map(|r| (r.inode, r.off, r.len))
                            .collect(),
                        g.disk.served.clone(),
                    )
                };
                if let Some(t) = twin.as_mut() {
                    t.extend(reads.iter().copied());
                }
                recovery_reads = reads;
                recovery_served.extend(served);
                let end = world.lock().disk.clone();
                if let Err(e) = cx.with("h:recinstall", |c| {
                    *c.store() = st.clone();
                    Ok(())
                }) {
                    recovery_events = world.lock().disk.events.clone();
                    break Err(e);
                }
                break Ok((st, frontier, end));
            }
            Err(Fail::Sim(SimFail::Crashed)) if arm => {
                // A nested recovery crash: F-CRASH only (never media),
                // then retry.
                nested_fired += 1;
                let mut frng = Rng::new(derive(seed * 100 + 50 + nested_fired as u64, S_CRASH));
                let mut g = world.lock();
                recovery_served.extend(g.disk.served.clone());
                recovery_events = g.disk.events.clone();
                let _ = g.disk.apply_crash(&mut frng, &faults.knobs);
                recovery_disk = g.disk.clone();
                continue;
            }
            Err(e) => {
                let g = world.lock();
                recovery_served.extend(g.disk.served.clone());
                recovery_events = g.disk.events.clone();
                break Err(e);
            }
        }
    };
    // (The opened meta and frontier are read from the end state below.)

    // ---- X5-R5: meta_rollback and R, from the durable-write history ----
    // Condition 3 needs M-NOINPLACE's per-write verdicts (Work 3.10):
    // a slot written in violation is named by the monitor, never
    // excused by the exception set.
    let mcfg = crate::monitor::MonitorCfg {
        map_leaf_cap: cfg.map_leaf_cap,
        freeset_leaf_bits: cfg.freeset_leaf_bits,
    };
    let noinplace_hits = crate::monitor::noinplace_slot_hits(&events, &mcfg);
    let (meta_rollback, rollback_slots) = match &meta_i {
        Some(opened) => {
            let rb = meta_rollback_of(&disk, pf, opened.ckpt_id);
            let r = exception_set_r(
                &disk,
                pf,
                opened.ckpt_id,
                &image_slots,
                &tainted_slots,
                &noinplace_hits,
            );
            (rb, r)
        }
        None => (false, BTreeSet::new()),
    };

    // ---- the extraction and the oracle ----
    let mut sample_rng = Rng::new(derive(seed * 7 + round as u64, S_SAMPLE));
    let mut outcome = RoundOutcome {
        class: faults.class,
        deadlock: flags.0,
        baton: flags.1,
        panicked: flags.2,
        task_error: flags.3,
        ..Default::default()
    };
    match recovery {
        Ok((st, frontier, end_disk)) => {
            let (opened_meta, _opened_xx) = read_opened_meta(&end_disk);
            let opened_t = opened_meta.as_ref().map(|m| m.t).unwrap_or(0);
            let gc_w = opened_meta.as_ref().map(|m| m.gc_w).unwrap_or(0);
            let world = round_world(seed, end_disk.clone(), st.clone());
            let cx = Cx {
                w: &world,
                t: HARNESS,
            };
            let mut reader = StoreReader { w: &world };
            let mut ex = Extracted {
                ok: true,
                frontier,
                gc_w,
                opened_t,
                amcheck_i: amcheck_i.clone(),
                amcheck_i_ran: true,
                image_slots: image_slots.clone(),
                tainted_image_slots: tainted_slots.clone(),
                meta_rollback,
                rollback_slots: rollback_slots.clone(),
                ..Default::default()
            };
            // The published set (a catalog scan at the frontier, 2.3).
            match reader.published(frontier) {
                Ok(p) => ex.published = p,
                Err(_) => ex.xx001.push(XxSite::Read23),
            }
            // The served corruption check. Every life of this round
            // contributes: the workload pass (the sched disk), the
            // recovery pass(es) and this extraction's own accepts, plus
            // X5-R5's R-taint — every slot in `R` counts as tainted
            // from open until the store next writes it, judged over the
            // interleaved write/accept history (recovery's accepts
            // included; the events walk of [`r_taint_of`]).
            {
                let g = world.lock();
                let r_taint = !faults.no_r_taint && r_taint_of(&g.disk.events, pf, &rollback_slots);
                let workload_served = {
                    let g2 = sched.lock();
                    !g2.disk.served.is_empty()
                };
                ex.served = workload_served
                    || !recovery_served.is_empty()
                    || !g.disk.served.is_empty()
                    || r_taint;
            }
            // The class MP/MW twin supply.
            let twin_info = TwinInfo {
                page_file: pf,
                recovery_reads: recovery_reads.clone(),
                image_slots: image_slots.clone(),
            };
            // Directed tests supply the twin's reads through the
            // caller's collector (Work 13's twin half; a class MP/MW
            // round without one fails closed in the oracle, as X-5a
            // round 1 ruled).
            let twin_ref = twin.as_ref().map(|_| &twin_info);
            // The oracle check on the recovered image (the card's Work
            // 13 order: extraction, oracle, harness checkpoint). The
            // (ii) fields are unset in this pass — G4's harness
            // checkpoint has not run yet.
            let first = check_round(
                &model,
                faults.class,
                c_prev,
                &ex,
                &mut reader,
                &media_targets,
                twin_ref,
                &mut sample_rng,
                &OracleCfg::default(),
            );
            let _ = first;
            // The harness checkpoint (G4: after the oracle check; it
            // supplies O-2 (ii) and the next round's base), then
            // amcheck (ii) — **fed into the verdict** (the gate's N2:
            // the draft computed them after `check_round` and never set
            // `Extracted.amcheck_ii`, so O-2 (ii) and the (ii) half of
            // O-3 (e) were skipped for every round). A checkpoint that
            // fails without a crash is a loud round failure, never a
            // silent skip.
            let ck = ckpt::checkpoint(&cx, &mut |_| Ok(()));
            if let Err(e) = &ck {
                cx.with("h:ck2fail", |c| {
                    let h = c.h();
                    if h.task_error.is_none() {
                        h.task_error = Some(format!("harness checkpoint failed: {e:?}"));
                    }
                    Ok(())
                })
                .ok();
            }
            let findings_ii = amcheck::amcheck(&cx, &cfg);
            let (end_disk2, end_store) = {
                let g = world.lock();
                (g.disk.clone(), g.st.clone())
            };
            // The (ii) image's referenced and tainted slots, from the
            // durable bytes (the same walk as the (i) point).
            let (ii_findings, image_slots_ii, _meta_ii) = durable_image(&end_disk2, &cfg);
            let _ = ii_findings;
            let tainted_slots_ii: BTreeSet<u64> = image_slots_ii
                .iter()
                .copied()
                .filter(|s| end_disk2.range_tainted(pf, s * 4096, 4096))
                .collect();
            ex.amcheck_ii = findings_ii.clone();
            ex.amcheck_ii_ran = ck.is_ok();
            ex.image_slots_ii = image_slots_ii;
            ex.tainted_image_slots_ii = tainted_slots_ii;
            // The final verdict: the same extraction plus the (ii)
            // fields, judged by the frozen oracle (a fresh sample rng,
            // seeded identically — determinism is per schedule, not per
            // call).
            let mut sample_rng2 = Rng::new(derive(seed * 7 + round as u64, S_SAMPLE));
            let verdict = check_round(
                &model,
                faults.class,
                c_prev,
                &ex,
                &mut reader,
                &media_targets,
                twin_ref,
                &mut sample_rng2,
                &OracleCfg::default(),
            );
            outcome.o1 = verdict.o1.clone();
            outcome.o1f_ok = verdict.o1f_ok;
            outcome.o2_fail = verdict.o2_fail.clone();
            outcome.o3_fail = verdict.o3_fail.clone();
            outcome.served = ex.served;
            // `pass` folds every per-seed failure flag: a DEADLOCK, a
            // PANIC or an S-BATON violation is never a pass (card "Do
            // not"; the gate's minor finding: the draft ignored
            // `deadlock` and `panicked` here).
            outcome.pass = fold_pass(
                verdict.pass,
                outcome.task_error.as_deref(),
                outcome.deadlock,
                outcome.panicked,
                outcome.baton,
            );
            outcome.lo = model.lower_bound(c_prev);
            outcome.hi = model.upper_bound(outcome.lo);
            outcome.frontier = frontier;
            outcome.gc_w = gc_w;
            outcome.opened_t = opened_t;
            outcome.detections = verdict.detections.iter().map(|d| site_name(*d)).collect();
            outcome.acked_lost = verdict.acked_lost.clone();
            outcome.amcheck_ii = findings_ii;
            outcome.cut = verdict.o1.clone().unwrap_or(outcome.lo);
            outcome.meta_rollback = meta_rollback;
            outcome.rollback_slots = rollback_slots.clone();
            outcome.amcheck_i = amcheck_i.clone();
            outcome.end_disk = end_disk2.clone();
            outcome.crashed_disk = disk.clone();
            outcome.end_store = Some(end_store.clone());
            // Work 2.1: the next round's model is this round's, truncated
            // at the matched cut.
            {
                let mut m = clone_model(&model);
                m.truncate(outcome.cut);
                outcome.end_model = m;
            }
        }
        Err(_) => {
            // Recovery failed: XX001 at Open (class C fails O-3 (b); in
            // MP/MW it is a detection).
            let mut ex = Extracted {
                ok: false,
                xx001: vec![XxSite::Open],
                served: {
                    let g2 = sched.lock();
                    !g2.disk.served.is_empty()
                        || !recovery_served.is_empty()
                        || (!faults.no_r_taint && r_taint_of(&recovery_events, pf, &rollback_slots))
                },
                amcheck_i: amcheck_i.clone(),
                amcheck_i_ran: meta_i.is_some(),
                image_slots: image_slots.clone(),
                tainted_image_slots: tainted_slots.clone(),
                meta_rollback,
                rollback_slots: rollback_slots.clone(),
                ..Default::default()
            };
            let _ = &mut ex;
            let mut nop = NopReader;
            let twin_info = TwinInfo {
                page_file: pf,
                recovery_reads: Vec::new(),
                image_slots: image_slots.clone(),
            };
            let verdict = check_round(
                &model,
                faults.class,
                c_prev,
                &ex,
                &mut nop,
                &media_targets,
                if faults.class == FaultClass::C {
                    None
                } else {
                    Some(&twin_info)
                },
                &mut sample_rng,
                &OracleCfg::default(),
            );
            outcome.o1 = verdict.o1.clone();
            outcome.o1f_ok = verdict.o1f_ok;
            outcome.o2_fail = verdict.o2_fail.clone();
            outcome.o3_fail = verdict.o3_fail.clone();
            outcome.served = ex.served;
            outcome.pass = fold_pass(
                verdict.pass,
                outcome.task_error.as_deref(),
                outcome.deadlock,
                outcome.panicked,
                outcome.baton,
            );
            outcome.lo = model.lower_bound(c_prev);
            outcome.hi = model.upper_bound(outcome.lo);
            outcome.detections = verdict.detections.iter().map(|d| site_name(*d)).collect();
            outcome.cut = outcome.lo;
            outcome.meta_rollback = meta_rollback;
            outcome.rollback_slots = rollback_slots.clone();
            outcome.amcheck_i = amcheck_i.clone();
            outcome.end_disk = disk.clone();
            outcome.crashed_disk = disk.clone();
            {
                let mut m = clone_model(&model);
                m.truncate(outcome.cut);
                outcome.end_model = m;
            }
        }
    }
    // ---- the monitors (3.10), from disk events and parsed WAL bytes ----
    {
        let g = sched.lock();
        let events_end = g.disk.events.clone();
        outcome.monitors = crate::monitor::run(&events_end, &flip_pos, &mcfg);
    }
    outcome.digest = crate::crc::crc32c(0, outcome.digest_input().as_bytes()) as u64;
    outcome
}

/// Work 2.1's durability flags from the round's disk events: for each
/// commit ts, the WAL write carrying its record's last byte returned
/// (`written`), and its ack — a covering WAL sync returned (SyncOn) or
/// the write itself (SyncOff). A WAL file's bytes are stitched from its
/// write events (issue order, latest write per offset wins), then walked
/// record by record from offset 0 (§12.5's alignment: a record lives at
/// `lsn mod WAL_SEG` in whichever segment file holds it — a recycled
/// segment's new life overwrites its old one from offset 0, so the walk
/// sees exactly the latest life). The page file's stitched bytes fail
/// the record magic immediately and contribute nothing. Record ends are
/// **file-relative** throughout (B5 fixes: split records across a
/// writer's 4-KiB batch chunks, and recycled segments' file-relative
/// positions; D9's renamed-segment shape pinned both).
pub fn derive_written_acked(
    events: &[DiskEvent],
    wal_commits: &BTreeMap<u64, (bool, u64)>,
) -> (Vec<u64>, Vec<u64>) {
    // inode → (off → latest issued bytes) and the return/sync extents.
    type FileWrites = BTreeMap<u64, Vec<u8>>;
    let mut files: BTreeMap<usize, FileWrites> = BTreeMap::new();
    let mut returned_end: BTreeMap<usize, u64> = BTreeMap::new();
    let mut durable_end: BTreeMap<usize, u64> = BTreeMap::new();
    let mut sync_after: BTreeMap<usize, Vec<u64>> = BTreeMap::new();
    for e in events {
        match e {
            DiskEvent::WriteIssued { inode, off, bytes } => {
                files.entry(*inode).or_default().insert(*off, bytes.clone());
            }
            DiskEvent::WriteReturned { inode, off, len } => {
                let w = returned_end.entry(*inode).or_default();
                *w = (*w).max(off + len);
            }
            DiskEvent::SyncReturned { inode, .. } => {
                // A sync makes durable everything the file had **through
                // the then-returned extent**; record the event order by
                // pairing it with the returned extent at this point.
                let w = returned_end.get(inode).copied().unwrap_or(0);
                sync_after.entry(*inode).or_default().push(w);
            }
            _ => {}
        }
    }
    // Per sync event (in order), the durable extent ratchets: durable =
    // max over syncs of the returned extent at sync time.
    for (inode, exts) in sync_after.iter() {
        let d = durable_end.entry(*inode).or_default();
        for w in exts {
            *d = (*d).max(*w);
        }
    }
    // Walk each stitched file's records; ts → (inode, record end in-file).
    // The header carries the record's own lsn; a record belongs at
    // `lsn mod WAL_SEG` (§12.5's position rule), which is what ties a
    // recycled file's latest life to its overwritten bytes.
    let mut ts_pos: BTreeMap<u64, (usize, u64)> = BTreeMap::new();
    let seg = crate::store::WAL_SEG;
    for (inode, writes) in files.iter() {
        // A chained round's events hold only **this round's** writes (a
        // prior round's records are below them, their flags already
        // carried by the incoming model, Work 2.1): start at the round's
        // own first write, and on a gap — a chunk boundary that begins
        // mid-record — jump to the next write offset instead of stopping
        // (records are self-delimiting through their `len`, and the
        // position rule still validates each one). Single-round disks
        // keep the from-zero walk: their first write is at 0.
        let mut off = writes.keys().copied().min().unwrap_or(0);
        loop {
            if off >= seg {
                break;
            }
            let hdr = stitched(writes, off, 24);
            if hdr.len() < 24 || !magic_ok(&hdr) {
                let Some(&next) = writes.range(off + 1..).next().map(|(k, _)| k) else {
                    break;
                };
                off = next;
                continue;
            }
            let rd = |r: std::ops::Range<usize>| -> u64 {
                let mut b = [0u8; 8];
                if let Some(s) = hdr.get(r) {
                    for (i, x) in s.iter().enumerate() {
                        b[i] = *x;
                    }
                }
                u64::from_le_bytes(b)
            };
            let len = rd(4..8) & 0xFFFF_FFFF;
            let lsn = rd(16..24);
            if len < 16 || off + len > seg || lsn % seg != off {
                let Some(&next) = writes.range(off + 1..).next().map(|(k, _)| k) else {
                    break;
                };
                off = next;
                continue;
            }
            let whole = stitched(writes, off, len as usize);
            if (whole.len() as u64) < len {
                let Some(&next) = writes.range(off + 1..).next().map(|(k, _)| k) else {
                    break;
                };
                off = next;
                continue;
            }
            match crate::store::wal::dec_record(&whole, lsn, lsn - off + seg) {
                Ok(Some(rec)) => {
                    if rec.kind == K_COMMIT || rec.kind == K_MARKER {
                        ts_pos.insert(rec.ts, (*inode, off + len));
                    }
                    off += len;
                }
                _ => {
                    let Some(&next) = writes.range(off + 1..).next().map(|(k, _)| k) else {
                        break;
                    };
                    off = next;
                }
            }
        }
    }
    let mut written = Vec::new();
    let mut acked = Vec::new();
    for (ts, (sync_on, _end)) in wal_commits.iter() {
        let Some((inode, end)) = ts_pos.get(ts).copied() else {
            continue;
        };
        let w = returned_end.get(&inode).copied().unwrap_or(0);
        let d = durable_end.get(&inode).copied().unwrap_or(0);
        if w >= end {
            written.push(*ts);
            let acked_now = if *sync_on { d >= end } else { true };
            if acked_now {
                acked.push(*ts);
            }
        }
    }
    (written, acked)
}

/// The NWAL record magic (§12.2, `enc_record`'s first word).
fn magic_ok(hdr: &[u8]) -> bool {
    let mut b = [0u8; 4];
    if let Some(s) = hdr.get(0..4) {
        b.copy_from_slice(s);
    }
    u32::from_le_bytes(b) == 0x4C41_574E
}

/// The `len` bytes at `off` of the stitched file, if fully present.
fn stitched(writes: &BTreeMap<u64, Vec<u8>>, off: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    // Walk the issued write chunks covering [off, off+len): each write is
    // a contiguous run at its own offset, and a run may **start before**
    // the byte we want (records begin mid-chunk when the writer split a
    // batch), so the covering run — the last one starting at or before
    // the cursor — is picked first.
    let mut cur = off;
    while out.len() < len {
        let cover = writes
            .range(..=cur)
            .next_back()
            .filter(|(k, v)| **k + v.len() as u64 > cur)
            .map(|(k, v)| (*k, v.clone()));
        let ahead = writes.range(cur..).next().map(|(k, v)| (*k, v.clone()));
        let Some((woff, bytes)) = cover.or(ahead) else {
            break;
        };
        if woff > cur {
            break;
        }
        let skip = (cur - woff) as usize;
        let Some(take) = bytes.get(skip..) else {
            break;
        };
        let need = len - out.len();
        let take = &take[..take.len().min(need)];
        out.extend_from_slice(take);
        cur += take.len() as u64;
        if take.is_empty() {
            break;
        }
    }
    out
}

fn site_name(d: XxSite) -> &'static str {
    match d {
        XxSite::Open => "open",
        XxSite::Replay => "replay",
        XxSite::Read23 => "read23",
        XxSite::AmcheckI => "amcheck_i",
        XxSite::AmcheckIi => "amcheck_ii",
    }
}

// ---- the durable-image walk (harness-side, from durable bytes) ----

/// Parse the newest valid durable meta and walk its referenced slots.
pub fn durable_image(
    disk: &SimDisk,
    cfg: &StoreCfg,
) -> (
    Vec<Finding>,
    BTreeSet<u64>,
    Option<crate::store::page::Meta>,
) {
    let mut out = Vec::new();
    let pf = match disk.stat(crate::sim::PAGE_FILE) {
        Some(f) => f,
        None => {
            return (
                vec![Finding {
                    slot: None,
                    what: "open failed".into(),
                }],
                BTreeSet::new(),
                None,
            );
        }
    };
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    let (meta, _slot) = match (
        crate::store::page::parse_meta(&m1),
        crate::store::page::parse_meta(&m2),
    ) {
        (Ok(a), Ok(b)) => {
            if a.ckpt_id >= b.ckpt_id {
                (a, 1)
            } else {
                (b, 2)
            }
        }
        (Ok(a), Err(())) => (a, 1),
        (Err(()), Ok(b)) => (b, 2),
        (Err(()), Err(())) => {
            return (
                vec![Finding {
                    slot: Some(1),
                    what: "no valid meta".into(),
                }],
                BTreeSet::new(),
                None,
            );
        }
    };
    // The map radix and the free set from the durable bytes.
    let mut flat: BTreeMap<u64, crate::store::page::Ref> = BTreeMap::new();
    let mut used: BTreeSet<u64> = BTreeSet::new();
    if meta.map_root.slot != 0 {
        let n = ((crate::store::page::PAGE - crate::store::page::HDR)
            / crate::store::page::MAP_ENTRY) as u64;
        let span = ckpt::radix_child_span(cfg.map_leaf_cap, n, meta.next_lpid);
        walk_map_disk(
            disk,
            pf,
            meta.map_root,
            0,
            span,
            cfg.map_leaf_cap,
            0,
            &mut flat,
            &mut used,
            &mut out,
        );
    }
    let mut free_bits: BTreeSet<u64> = BTreeSet::new();
    if meta.free_root.slot != 0 {
        let total = (meta.slot_count.saturating_sub(3)) + meta.next_lpid;
        let n = ((crate::store::page::PAGE - crate::store::page::HDR)
            / crate::store::page::MAP_ENTRY) as u64;
        let span = ckpt::radix_child_span(cfg.freeset_leaf_bits, n, total);
        walk_free_disk(
            disk,
            pf,
            meta.free_root,
            0,
            span,
            cfg.freeset_leaf_bits,
            total,
            &mut free_bits,
            &mut used,
            &mut out,
        );
    }
    // The trees, from the directory entries.
    if meta.dir_root != 0
        && let Some(r) = flat.get(&meta.dir_root).copied()
    {
        let entries = dir_entries_disk(disk, pf, meta.dir_root, r, &flat, &mut out);
        used.insert(r.slot);
        for (_id, root) in entries {
            if let Some(rr) = flat.get(&root).copied() {
                walk_tree_disk(disk, pf, root, rr, &flat, &mut used, &mut out);
            }
        }
    }
    used.insert(0);
    used.insert(1);
    used.insert(2);
    // The meta bounds of card 3.8 (the harness walk reports them so
    // O-2/(e) see them at the (i) point): `gc_w ≤ T`, `next_lpid` above
    // every mapped LPID, `slot_count` at least every referenced slot.
    // Structural findings carry no slot (the exactness half judges
    // slotted findings only).
    if meta.gc_w > meta.t {
        out.push(Finding {
            slot: None,
            what: "gc_w > T".into(),
        });
    }
    if meta.next_lpid <= flat.keys().copied().max().unwrap_or(0) {
        out.push(Finding {
            slot: None,
            what: "next_lpid not above every mapped lpid".into(),
        });
    }
    if let Some(m) = used.iter().copied().max()
        && m >= meta.slot_count
    {
        out.push(Finding {
            slot: None,
            what: "slot_count below a referenced slot".into(),
        });
    }
    // The meta slots are referenced pages of the image: a faulted one
    // whose bytes still parse (a rollback's fallout — the durable bytes
    // are the pre-write ones, tainted against the issued reference) is
    // reported, so O-3 (e)'s completeness half holds for meta-slot
    // faults too (the frozen oracle requires every tainted referenced
    // slot to be named; the loser slot of an unwritten pair never
    // differs from its issued bytes and stays silent). Gated on media
    // faults existing: a **crash-dropped** volatile meta write leaves
    // the same taint signature with no fault behind it, and reporting
    // it would fail O-2's class-C cleanliness on every ordinary crash
    // at a step-4/5 boundary (D1's After4).
    if !disk.media.is_empty() {
        for slot in [1u64, 2] {
            if disk.range_tainted(pf, slot * 4096, 4096) {
                out.push(Finding {
                    slot: Some(slot),
                    what: "stale durable meta slot (rollback fallout)".into(),
                });
            }
        }
    }
    (out, used, Some(meta))
}

#[allow(clippy::too_many_arguments)]
fn walk_map_disk(
    disk: &SimDisk,
    pf: usize,
    root: crate::store::page::Ref,
    base: u64,
    span: u64,
    cap: u64,
    depth: u8,
    flat: &mut BTreeMap<u64, crate::store::page::Ref>,
    used: &mut BTreeSet<u64>,
    out: &mut Vec<Finding>,
) {
    if depth > 8 || root.slot == 0 {
        return;
    }
    let b = disk.durable_read(pf, root.slot * 4096, 4096);
    // §3.3 with the positional expected id (the gate's R4).
    if crate::store::page::check(&b, &root, base, None).is_err() {
        out.push(Finding {
            slot: Some(root.slot),
            what: "§3.3 fail on map page".into(),
        });
    }
    used.insert(root.slot);
    let kind = crate::store::page::header_kind(&b);
    if kind == crate::store::page::KIND_MAP_LEAF {
        if let Ok(entries) = crate::store::page::parse_map_leaf(&b, base, cap, &root, None) {
            for (i, e) in entries.iter().enumerate() {
                if let Some(r) = e {
                    flat.insert(base + i as u64, *r);
                    used.insert(r.slot);
                }
            }
        }
    } else if let Ok(refs) =
        crate::store::page::parse_radix_inner(&b, base, (4096 - 24) / 16, &root, None)
    {
        let n = ((crate::store::page::PAGE - crate::store::page::HDR)
            / crate::store::page::MAP_ENTRY) as u64;
        for (i, r) in refs.into_iter().enumerate() {
            if let Some(r) = r {
                let child_span = (span / n).max(1);
                let child_base = base + i as u64 * span;
                walk_map_disk(
                    disk,
                    pf,
                    r,
                    child_base,
                    child_span,
                    cap,
                    depth + 1,
                    flat,
                    used,
                    out,
                );
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn walk_free_disk(
    disk: &SimDisk,
    pf: usize,
    root: crate::store::page::Ref,
    base: u64,
    span: u64,
    bits: u64,
    _total: u64,
    set: &mut BTreeSet<u64>,
    used: &mut BTreeSet<u64>,
    out: &mut Vec<Finding>,
) {
    if root.slot == 0 {
        return;
    }
    let b = disk.durable_read(pf, root.slot * 4096, 4096);
    // §3.3 with the positional expected id (the gate's R4).
    if crate::store::page::check(&b, &root, base, None).is_err() {
        out.push(Finding {
            slot: Some(root.slot),
            what: "§3.3 fail on free-set page".into(),
        });
    }
    used.insert(root.slot);
    let kind = crate::store::page::header_kind(&b);
    if kind == crate::store::page::KIND_FREE_LEAF {
        if let Ok(bs) = crate::store::page::parse_free_leaf(&b, base, bits, &root, None) {
            for (i, set_bit) in bs.iter().enumerate() {
                if *set_bit {
                    set.insert(base + i as u64);
                }
            }
        }
    } else if let Ok(refs) =
        crate::store::page::parse_radix_inner(&b, base, (4096 - 24) / 16, &root, None)
    {
        let n = ((crate::store::page::PAGE - crate::store::page::HDR)
            / crate::store::page::MAP_ENTRY) as u64;
        for (i, r) in refs.into_iter().enumerate() {
            if let Some(r) = r {
                let child_span = (span / n).max(1);
                let child_base = base + i as u64 * span;
                walk_free_disk(
                    disk, pf, r, child_base, child_span, bits, _total, set, used, out,
                );
            }
        }
    }
}

fn dir_entries_disk(
    disk: &SimDisk,
    pf: usize,
    dir_root: u64,
    root_ref: crate::store::page::Ref,
    flat: &BTreeMap<u64, crate::store::page::Ref>,
    out: &mut Vec<Finding>,
) -> Vec<(u64, u64)> {
    let mut out_entries = Vec::new();
    // `(lpid, ref)` — the §3.3 id check is positional: the page's header
    // id must equal its LPID (the draft passed a hardcoded 0, which
    // failed every directory page and was silently swallowed).
    let mut stack = vec![(dir_root, root_ref)];
    while let Some((l, r)) = stack.pop() {
        let b = disk.durable_read(pf, r.slot * 4096, 4096);
        if crate::store::page::check(&b, &r, l, None).is_err() {
            // Report it (the gate's N4): the draft `continue`d silently,
            // so a fault on the directory page — a leaf — left the
            // tainted referenced slot unnamed and O-3 (e)'s completeness
            // half failed the round (D17's second half found it: the
            // planted stale page was the directory's own).
            out.push(Finding {
                slot: Some(r.slot),
                what: "§3.3 fail on directory page".into(),
            });
            continue;
        }
        let kind = crate::store::page::header_kind(&b);
        if kind == crate::store::page::KIND_LEAF {
            if let Ok(cells) = tree::leaf_cells(&b) {
                for (k, v) in cells {
                    if k.len() != 8 || v.len() != 9 {
                        continue;
                    }
                    let mut rb = [0u8; 8];
                    if let Some(s) = v.get(0..8) {
                        rb.copy_from_slice(s);
                    }
                    if let Ok(id) = k.try_into() {
                        out_entries.push((u64::from_be_bytes(id), u64::from_le_bytes(rb)));
                    }
                }
            }
        } else if let Ok((seps, right)) = tree::inner_seps(&b) {
            for (c, _) in seps {
                if let Some(cr) = flat.get(&c) {
                    stack.push((c, *cr));
                }
            }
            if let Some(rr) = flat.get(&right) {
                stack.push((right, *rr));
            }
        }
    }
    out_entries
}

fn walk_tree_disk(
    disk: &SimDisk,
    pf: usize,
    lpid: u64,
    root_ref: crate::store::page::Ref,
    flat: &BTreeMap<u64, crate::store::page::Ref>,
    used: &mut BTreeSet<u64>,
    out: &mut Vec<Finding>,
) {
    let mut stack = vec![(lpid, root_ref)];
    while let Some((l, r)) = stack.pop() {
        let b = disk.durable_read(pf, r.slot * 4096, 4096);
        // §3.3 on a referenced tree page fails: report it (the gate's
        // N4 — the draft `continue`d silently, and the map walk's
        // slot-set collection pre-inserted every data slot into `used`,
        // so the dedup below skipped the check entirely; O-3 (e)'s
        // completeness half then failed rounds whose tainted tree slots
        // the walk had walked past without naming). The check runs
        // before the dedup for exactly that reason.
        if crate::store::page::check(&b, &r, l, None).is_err() {
            out.push(Finding {
                slot: Some(r.slot),
                what: "§3.3 fail on tree page".into(),
            });
            continue;
        }
        if !used.insert(r.slot) {
            continue;
        }
        if crate::store::page::header_kind(&b) == crate::store::page::KIND_INNER
            && let Ok((seps, right)) = tree::inner_seps(&b)
        {
            for (c, _) in seps {
                if let Some(cr) = flat.get(&c).copied() {
                    stack.push((c, cr));
                }
            }
            if let Some(rr) = flat.get(&right).copied() {
                stack.push((right, rr));
            }
        }
    }
}

/// The opened meta of a disk (durable bytes).
pub fn read_opened_meta(disk: &SimDisk) -> (Option<crate::store::page::Meta>, ()) {
    let pf = match disk.stat(crate::sim::PAGE_FILE) {
        Some(f) => f,
        None => return (None, ()),
    };
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    let best = match (
        crate::store::page::parse_meta(&m1),
        crate::store::page::parse_meta(&m2),
    ) {
        (Ok(a), Ok(b)) => Some(if a.ckpt_id >= b.ckpt_id { a } else { b }),
        (Ok(a), Err(())) => Some(a),
        (Err(()), Ok(b)) => Some(b),
        (Err(()), Err(())) => None,
    };
    (best, ())
}

/// X5-R5 condition 2: a meta write **covered by a completed sync_all**
/// (a durable write) carries a ckpt_id above the opened one (the opened
/// meta is a rollback). From the durable-write history only.
pub fn meta_rollback_of(disk: &SimDisk, pf: usize, opened_k: u64) -> bool {
    let mut max_k = 0u64;
    for w in disk.dwrites_of(pf) {
        if w.all
            && (w.off == 4096 || w.off == 8192)
            && w.bytes.len() == 4096
            && let Ok(m) = crate::store::page::parse_meta(&w.bytes)
        {
            max_k = max_k.max(m.ckpt_id);
        }
    }
    max_k > opened_k
}

/// X5-R5's set R: slots the opened image references that are not tainted,
/// written after the first completed `sync_all` covering a meta newer
/// than the opened one, **and never written with an M-NOINPLACE hit**
/// (condition 3, the monitor's per-write verdicts). From the
/// durable-write history.
pub fn exception_set_r(
    disk: &SimDisk,
    pf: usize,
    opened_k: u64,
    image_slots: &BTreeSet<u64>,
    tainted: &BTreeSet<u64>,
    noinplace_hits: &BTreeSet<u64>,
) -> BTreeSet<u64> {
    let mut newer_graduation: Option<usize> = None;
    let dw = disk.dwrites_of(pf);
    for (i, w) in dw.iter().enumerate() {
        if w.all
            && (w.off == 4096 || w.off == 8192)
            && w.bytes.len() == 4096
            && let Ok(m) = crate::store::page::parse_meta(&w.bytes)
            && m.ckpt_id > opened_k
        {
            newer_graduation = Some(i);
            break;
        }
    }
    let Some(g) = newer_graduation else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for (i, w) in dw.iter().enumerate() {
        if i <= g {
            continue;
        }
        let slot = w.off / 4096;
        if r_slot_qualifies(slot, image_slots, tainted, noinplace_hits) {
            out.insert(slot);
        }
    }
    out
}

/// The pure conditions of X5-R5's R for one write's slot: the slot is
/// referenced by the opened image, not tainted at amcheck (i), and —
/// condition 3 — **never written with an M-NOINPLACE hit** (a slot the
/// monitor flagged is named by the violation, never excused by the
/// exception). Extracted for its pin (the gate's N7): removing any
/// condition changes the result on the crafted input.
pub fn r_slot_qualifies(
    slot: u64,
    image_slots: &BTreeSet<u64>,
    tainted: &BTreeSet<u64>,
    noinplace_hits: &BTreeSet<u64>,
) -> bool {
    image_slots.contains(&slot) && !tainted.contains(&slot) && !noinplace_hits.contains(&slot)
}

/// The per-round pass fold: a DEADLOCK, a PANIC, an S-BATON violation
/// or a task error is never a pass, whatever the oracle said (card "Do
/// not"). Extracted for its pin (the gate's N7).
pub fn fold_pass(
    verdict_pass: bool,
    task_error: Option<&str>,
    deadlock: bool,
    panicked: bool,
    baton: bool,
) -> bool {
    verdict_pass && task_error.is_none() && !deadlock && !panicked && !baton
}

/// X5-R5's R-taint (card Work 13): a slot in `R` counts as tainted from
/// the moment open selects the rolled-back meta until the store next
/// writes it. The window covers **every post-crash life of the round**
/// — recovery's own accepts included (the gate's R3: the draft judged
/// only the extraction world's accept list, from any life) — and each
/// nested recovery crash restarts it: a later life re-opens the
/// rolled-back meta, and a slot an earlier life had written can be
/// re-read and accepted stale again. Fixed slots 0–2 are out of scope:
/// they are never page homes (§4.4), and the opened meta's own accept
/// at open is the selection itself, not a serve.
pub fn r_taint_of(events: &[DiskEvent], pf: usize, r_slots: &BTreeSet<u64>) -> bool {
    if r_slots.is_empty() {
        return false;
    }
    // Slots whose window has closed (written since the current life
    // began — i.e. since the round's crash, or the last nested one).
    let mut closed: BTreeSet<u64> = BTreeSet::new();
    for e in events {
        match e {
            DiskEvent::Crash => closed.clear(),
            DiskEvent::WriteIssued { inode, off, bytes } if *inode == pf && !bytes.is_empty() => {
                let s0 = off / 4096;
                let s1 = (off + bytes.len() as u64 - 1) / 4096;
                for s in s0..=s1 {
                    closed.insert(s);
                }
            }
            DiskEvent::Accepted { inode, off, len } if *inode == pf && *len > 0 => {
                let s0 = off / 4096;
                let s1 = (off + len - 1) / 4096;
                for s in s0..=s1 {
                    if s >= 3 && r_slots.contains(&s) && !closed.contains(&s) {
                        return true;
                    }
                }
            }
            _ => {}
        }
    }
    false
}

// ---- the 2.3 reader over the recovered store ----

/// Reads the recovered state for the oracle through the store's own read
/// paths (2.3).
pub struct StoreReader<'a> {
    /// The single-task world holding the recovered store.
    pub w: &'a Sched<RoundWorld>,
}

impl StoreReader<'_> {
    /// The published set at the frontier (a catalog scan, 2.3).
    pub fn published(&mut self, frontier: u64) -> Result<BTreeSet<u64>, ReadFail> {
        let cx = Cx {
            w: self.w,
            t: HARNESS,
        };
        let root = cx
            .with("r:cat", |c| Ok(tree::catalog_root(c.store())))
            .map_err(|_| ReadFail)?;
        if root == 0 {
            return Ok(BTreeSet::new());
        }
        let scan = tree::tree_scan(&cx, root, frontier).map_err(|_| ReadFail)?;
        let mut out = BTreeSet::new();
        for (k, v) in scan {
            if v.as_deref() == Some(PUBLIC) && k.len() == 8 {
                let mut b = [0u8; 8];
                if let Some(sl) = k.get(0..8) {
                    b.copy_from_slice(sl);
                }
                out.insert(u64::from_be_bytes(b));
            }
        }
        Ok(out)
    }
}

impl StateReader for StoreReader<'_> {
    fn scan(&mut self, tree: u64, s: u64) -> Result<Scan, ReadFail> {
        let cx = Cx {
            w: self.w,
            t: HARNESS,
        };
        let root = cx
            .with("r:root", |c| Ok(c.store().dir.get(&tree).map(|e| e.root)))
            .map_err(|_| ReadFail)?
            .ok_or(ReadFail)?;
        tree::tree_scan(&cx, root, s).map_err(|_| ReadFail)
    }

    fn raw(&mut self, tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        let cx = Cx {
            w: self.w,
            t: HARNESS,
        };
        let root = cx
            .with("r:root2", |c| Ok(c.store().dir.get(&tree).map(|e| e.root)))
            .map_err(|_| ReadFail)?
            .ok_or(ReadFail)?;
        let raw = tree::tree_raw(&cx, root).map_err(|_| ReadFail)?;
        Ok(raw
            .into_iter()
            .filter_map(|(k, tag, v)| {
                tree::dec_ekey(&k).map(|(key, ts)| RawEntry {
                    key,
                    ts,
                    tag,
                    value: v,
                })
            })
            .collect())
    }
}

/// A no-op reader for failed-recovery rounds.
pub struct NopReader;

impl StateReader for NopReader {
    fn scan(&mut self, _tree: u64, _s: u64) -> Result<Scan, ReadFail> {
        Err(ReadFail)
    }
    fn raw(&mut self, _tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        Err(ReadFail)
    }
}
