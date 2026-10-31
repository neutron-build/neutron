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

/// A WAL file's stitched issued writes: offset → latest bytes.
type FileWrites = BTreeMap<u64, Vec<u8>>;

/// Stream tags (independent splitmix64 derivations per concern).
const S_SCHED: u64 = 1;
const S_CRASH: u64 = 4;
const S_MEDIA: u64 = 5;
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
    /// A long reader (card Work 5.1): register a snapshot at the current
    /// `visible_ts`, hold it for `span` yields (the seeded span), then
    /// release exactly that snapshot. Held snapshots hold back the GC
    /// watermark (`gc_sweep`'s harness watermark is
    /// `min(registered snapshots, visible_ts)`), which is their whole
    /// effect: the model has no reader-visibility semantics to check.
    SnapshotHold {
        /// Yields to hold the snapshot for.
        span: u64,
    },
    /// Wait until every named tree exists in the store's directory (the
    /// round-setup barrier: a scripted transaction may only enqueue
    /// after the setup commit's `CreateTree` records are **applied**, so
    /// replay — which is strictly LSN-ordered — never meets a tree's ops
    /// before its creating record. W-SCRIPT's runtime half).
    WaitTrees {
        /// The trees to wait for.
        trees: Vec<u64>,
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
    /// Crash at the n-th **scheduler** yield of the round, any task (the
    /// uniform draw's true address space: every yield point, including
    /// the service tasks' and the checkpoint's internals). Armed before
    /// the workload starts.
    Yield(u64),
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
    /// X-5c (the seed-7 class): this workload pass runs the round's
    /// media faults (class MP/MW) — a script op failing with XX001 is a
    /// client-visible detection of already-recorded damage, and the
    /// task unwinds without failing the round (the ruled oracle judges
    /// the round; class C keeps the loud task error).
    pub media_round: bool,
    /// X-5c: script ops that unwound on a client-visible XX001
    /// (counted for the record; the oracle's own detections live in
    /// `RoundOutcome.detections`).
    pub client_xx001: u32,
    /// Commit ts → `(sync_on, record lsn, record len)` — recorded at the
    /// enqueue, with the extent `wal_enqueue` returned (the Work 2.1
    /// derivation's F1 fix: the extent lets it fail loudly when a
    /// record's bytes were written yet can never parse).
    pub wal_commits: BTreeMap<u64, (bool, u64, u64)>,
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
    /// A livelock was observed (X5-R13 (b)): a world of this round
    /// exceeded `YIELD_BUDGET`. Folded into `deadlock` — it counts in
    /// `DEADLOCK=` and in the rule's n, and is recorded beside it with
    /// its site.
    pub livelock: bool,
    /// The site of the yield that tripped the budget (X5-R13 (b)).
    pub livelock_site: &'static str,
    /// A `SimFail::Baton` was observed.
    pub baton: bool,
    /// A task panicked.
    pub panicked: bool,
    /// A script task ended with a failure (a store or env error): the
    /// round must fail loudly — never a silent early exit that leaves
    /// the service tasks spinning.
    pub task_error: Option<String>,
    /// Card Work 5.6's Q6 observable: the bulk-load phases live at the
    /// crash (`tree id → phase`).
    pub load_phase: BTreeMap<u64, LoadPhase>,
    /// Card Work 5.6's Q7 observable: a DropTree walk was in flight when
    /// the world crashed (the task unwinds inside the walk and never
    /// clears this).
    pub drop_walk: Option<u64>,
    /// Card Work 5.6's Q18 observable: a GC sweep ran with a watermark
    /// above the previous `gc_w` while a checkpoint window was open.
    pub gc_adv_in_window: bool,
    /// The last checkpoint hook the checkpointer passed (0 = none): a
    /// small position code for the Q4 window (1 = Before0 … 6 = After3;
    /// 7+ are later hooks). Maintained by the checkpointer's hook
    /// closure.
    pub ckpt_step: u8,
}

/// The bulk-load phases of card Work 5.6's Q6 (crash between the load and
/// the publication / after the publication with the load in the image).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoadPhase {
    /// The unlogged entries are being inserted (a crash here interrupted
    /// the load itself — not Q6's shape).
    Loading,
    /// The entries are in; the forced checkpoint (or its wait) is pending
    /// — a crash here is Q6's "between bulk load and publication".
    Loaded,
    /// The publication commit is running — also "between".
    Publishing,
    /// The publication commit completed — Q6's "after publication".
    Published,
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

/// True when the workload pass has already failed (a task error, or
/// `workload_done` set by a failed peer — a task that is still parked
/// keeps `tasks_left` above zero, so this is exactly "some task failed
/// loudly"). Every harness park loop treats this as a loud unwind,
/// never an endless park: X-5b's N3 discipline (fail loudly rather than
/// spin the service tasks into the deadlock detector) extended to the
/// `Applied`/`MetaDurable` waits. The shape it closes: a script op that
/// fails with a store error (an MP round's legitimately damaged page —
/// XX001 on a bricked tree) records its task error and wakes the park
/// keys **once** (Mesa), but the parked peers re-check their own
/// condition, still false, and park again forever — the scheduler's
/// DEADLOCK, with the round's true cause (the store error) hidden
/// behind it (seed 7 round 1 after X5-R11 (a) let round 0 pass).
fn round_failed(cx: &Cx<'_, RoundWorld>) -> Result<bool, Fail> {
    cx.with("h:failed", |c| {
        let h = c.h();
        Ok(h.task_error.is_some() || h.workload_done)
    })
}

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
        if round_failed(cx)? {
            return Err(Fail::Invalid("commit CS wait: the round failed".into()));
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
            if round_failed(cx)? {
                return Err(Fail::Invalid("commit CS wait: the round failed".into()));
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
        // X5-R10's harness guard: a commit ts the model already holds
        // can only be a sequencer reissue (a recovery that restarted
        // `next_ts` below its apply frontier). `Model::commit` would
        // silently overwrite the held record and hide the collision —
        // the seed-10 shape the ruling names; fail the round loudly
        // instead (task_error fails the pass fold).
        if h.model.commits.contains_key(&ts) {
            if h.task_error.is_none() {
                h.task_error = Some(format!("model commit at held ts {ts} (sequencer reissue)"));
            }
        } else {
            h.model.commit(ts, mode, ops.to_vec());
        }
        h.wal_commits.insert(ts, (sync_on, lsn, len));
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
    // The Q5/Q9 observable (store-side so the flip's counter is atomic
    // with `T`): this commit's apply progress, removed when it completes.
    // A crash mid-apply leaves the entry — the crash-time snapshot reads
    // exactly the partial applies.
    cx.with("h:aprog0", |c| {
        c.store()
            .apply_progress
            .insert(ts, (0, ordered.len() as u64, chunked));
        Ok(())
    })?;
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
        // Q7's observable: the DropTree walk (§6.8, bounded) runs inside
        // `apply_op`; a crash inside it leaves the flag set (the task
        // unwinds past the clear).
        let is_drop = matches!(op, Op::DropTree { .. });
        if is_drop {
            let tree = match op {
                Op::DropTree { tree } => *tree,
                _ => 0,
            };
            cx.with("h:dropwalk", |c| {
                c.h().drop_walk = Some(tree);
                Ok(())
            })?;
        }
        if let Err(e) = ckpt::apply_op(cx, &to_wop(op), ts, false) {
            client_xx001_unwind(cx, ts, e)?;
        }
        if is_drop {
            cx.with("h:dropdone", |c| {
                c.h().drop_walk = None;
                Ok(())
            })?;
        }
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
        cx.with("h:aprog1", |c| {
            if let Some(e) = c.store().apply_progress.get_mut(&ts) {
                e.0 += 1;
            }
            Ok(())
        })?;
    }
    cx.with("h:aprog2", |c| {
        c.store().apply_progress.remove(&ts);
        Ok(())
    })?;
    mark_applied(cx, ts)?;
    Ok(ts)
}

/// X-5c (the seed-7 class, R11 (g) instrumentation defect): in a
/// media round a script op's `Fail::Corrupt` is a client-visible
/// detection of already-recorded damage (the plan's clause 3: detected,
/// never served) — the harness's abort-on-error policy overrode the
/// ruled oracle and broke the 5.5 round shape (the crash never fired;
/// seed 7 round 1's drop of the damaged tree). The task unwinds; the
/// commit's record stays in the WAL (recovery is the authority for its
/// effects — §14/R10's principle), so its ts fills the visibility slot
/// (`applied`, never walked here) or every later committer would park
/// below it. The oracle then judges the round: O-1 at a cut below the
/// failed commit, or a replay-XX001 `Err` recovery — both legal. In
/// class C the error propagates unchanged (a loud task error: class C
/// has no damage, so a Corrupt there is a defect).
fn client_xx001_unwind(cx: &Cx<'_, RoundWorld>, ts: u64, e: Fail) -> Result<(), Fail> {
    let media = cx.with("h:cxx", |c| {
        Ok(c.h().media_round && matches!(e, Fail::Corrupt(_)))
    })?;
    if !media {
        return Err(e);
    }
    cx.with("h:cxx2", |c| {
        c.h().client_xx001 += 1;
        // The visibility fill: the record exists in the WAL; later
        // mark_applied walks pass through the ts.
        c.store().seq.applied.insert(ts);
        Ok(())
    })?;
    Err(e)
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
        if round_failed(cx)? {
            return Err(Fail::Invalid(format!(
                "mark_applied(ts {ts}): the round failed below the frontier"
            )));
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
        // The scheduler-level uniform crash (`CrashSpec::Yield`) is
        // armed on the fresh core: `run_script` owns the core's lifetime
        // (the reset above), so the arm belongs here and only here —
        // arming before `run_script` is silently wiped by the reset.
        if let Some(CrashSpec::Yield(at)) = script.crash.as_ref() {
            g.sched.crash_at_yield = Some(*at);
        }
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
    // X5-R13 (b): a livelocked world (a task that kept yielding and never
    // parked — the DEADLOCK detector's blind spot) is a recorded failure:
    // the flag and its site land on the round, folded into `deadlock` so
    // it counts in `DEADLOCK=` and in the rule's n, never as a pass.
    if g.sched.livelock {
        g.h.livelock = true;
        g.h.livelock_site = g.sched.livelock_site;
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
    // X5-R14 (the thread-start window): a **service** task's first
    // scheduler contact is a park — it must not act (its loop-entry
    // `Cx::with` reads, which reach `wal_writer:take` and
    // `evict:pick`'s clock-hand mutation) before the baton is ever
    // handed to it; OS thread-start order otherwise leaks into the
    // schedule and the digests stop being a function of `(seed,
    // mutant)`. Script tasks already park first: their `yld!` macro
    // yields before every op. The gate in `Sched::acquire` makes any
    // remaining pre-baton step a loud `SimFail::Baton`; this barrier is
    // what keeps the service bodies on the legal side of it.
    if t > nscript {
        let cx0 = Cx { w: sched, t };
        cx0.yield_now("h:entry").map_err(fail_of)?;
    }
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
                    let client_xx = cx
                        .with("h:taskcxx", |c| {
                            Ok(c.h().media_round && matches!(e, Fail::Corrupt(_)))
                        })
                        .unwrap_or(false);
                    if client_xx && !world_crashed {
                        // X-5c: the detection is client-visible; the task
                        // ends (its remaining ops do not run — like a
                        // crash), the ROUND continues to its own crash,
                        // faults, recovery and the ruled oracle.
                        cx.with("h:taskcxx2", |c| {
                            c.h().client_xx001 += 1;
                            Ok(())
                        })
                        .ok();
                        cx.w.wake(WaitKey::WalAcks);
                        cx.w.wake(WaitKey::MetaDurable);
                        cx.w.wake(WaitKey::Applied);
                    } else if !world_crashed {
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
/// Run one scripted op (pub for the harness tests' unit pins).
pub fn script_op(cx: &Cx<'_, RoundWorld>, op: &ScriptOp) -> Result<(), Fail> {
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
                if round_failed(cx)? {
                    return Err(Fail::Invalid(format!(
                        "WaitVisible({v}): the round failed below the frontier"
                    )));
                }
                cx.wait_for(WaitKey::Applied)?;
            }
            Ok(())
        }
        ScriptOp::WaitTrees { trees } => {
            // The round-setup barrier (W-SCRIPT's runtime half): park
            // until every named tree is installed in the directory.
            // `Applied` is the wake key (a CreateTree apply is progress;
            // the apply loop wakes it — Mesa re-check on wake). A
            // malformed script (nothing can install the tree) fails
            // loudly, exactly like the apply-path wait.
            loop {
                let (missing, left, failed) = cx.with("h:waittrees", |c| {
                    let st = c.store();
                    Ok((
                        trees.iter().filter(|t| !st.dir.contains_key(t)).count(),
                        c.h().tasks_left,
                        c.h().task_error.is_some(),
                    ))
                })?;
                if missing == 0 {
                    return Ok(());
                }
                if failed || left <= 1 {
                    return Err(Fail::Invalid(format!(
                        "trees still missing for the round setup ({missing})"
                    )));
                }
                cx.wait_for(WaitKey::Applied)?;
            }
        }
        ScriptOp::SnapshotHold { span } => {
            // A long reader (Work 5.1): register at the current
            // `visible_ts`, hold for `span` yields, release exactly that
            // snapshot. The held set only holds back the GC watermark.
            let at = cx.with("h:snapreg", |c| Ok(c.store().seq.visible_ts))?;
            cx.with("h:snapins", |c| {
                c.h().snapshots.insert(at);
                Ok(())
            })?;
            for _ in 0..*span {
                cx.yield_now("h:snapspan")?;
            }
            cx.with("h:naprel", |c| {
                c.h().snapshots.remove(&at);
                Ok(())
            })
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
    // Q18's observable: this sweep's watermark is above the previous
    // `gc_w` (the running `gc_w_used`) while a checkpoint window is open
    // — a GC advancing the watermark inside an image. Read in one step.
    cx.with("h:gcq18", |c| {
        let st = c.store();
        if w > st.gc_w_used && st.ckpt_open {
            c.h().gc_adv_in_window = true;
        }
        Ok(())
    })?;
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
    cx.with("h:loadph0", |c| {
        c.h().load_phase.insert(t, LoadPhase::Loading);
        Ok(())
    })?;
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
        c.h().load_phase.insert(t, LoadPhase::Loaded);
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
    cx.with("h:loadph1", |c| {
        c.h().load_phase.insert(t, LoadPhase::Publishing);
        Ok(())
    })?;
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
        c.h().load_phase.insert(t, LoadPhase::Published);
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
        if round_failed(cx)? {
            return Err(Fail::Invalid(
                "forced-checkpoint wait: the round failed".into(),
            ));
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
                // Q4's position code (card Work 5.6): record the hook's
                // step before any crash arm, so the crash-time snapshot
                // names the window the crash landed in (1 = Before0 …
                // 6 = After3 — the "between step 0 and step 3" window).
                // Best-effort: on an already-crashed world the recording
                // step fails and the checkpoint unwinds at its next
                // yield regardless.
                let step = match hook {
                    CkptHook::Before0 => 1,
                    CkptHook::After0 => 2,
                    CkptHook::After1 => 3,
                    CkptHook::Mid2 => 4,
                    CkptHook::After2 => 5,
                    CkptHook::After3 => 6,
                    CkptHook::After4 => 7,
                    CkptHook::After5 => 8,
                    CkptHook::Mid6 => 9,
                    CkptHook::After6 => 0,
                };
                let _ = cx.with("h:ckstep", |c| {
                    c.h().ckpt_step = step;
                    Ok(())
                });
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
    /// A livelock occurred (X5-R13 (b)): some world of this round
    /// exceeded `YIELD_BUDGET`; folded into `deadlock`.
    pub livelock: bool,
    /// The site of the yield that tripped the budget (X5-R13 (b)).
    pub livelock_site: &'static str,
    /// The park table at the trip (X5-R13 (b) diagnosis): `task:key`
    /// per task (`run` = runnable — the spinner the deadlock detector
    /// could never see). Diagnostic only; never a digest input.
    pub livelock_parked: Vec<String>,
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
    /// The workload pass's total scheduler yields (the uniform crash's
    /// address-space count; the driver's counting pass reads it with the
    /// crash disabled).
    pub sched_yields: u64,
    /// The workload pass's quota observables (card Work 5.6), snapshotted
    /// at the crash.
    pub obs: WorkloadObs,
    /// Nested recovery crashes that fired (Q2/Q3).
    pub nested_fired: u32,
    /// The nested points in force (the script's explicit list, or the
    /// seeded draw when `nested_draw` produced them) — recorded so the
    /// twin round can crash at the same points.
    pub nested_points: Vec<u64>,
    /// This round's media faults only (a chained round's disk carries the
    /// prior rounds' media list; O-3 (c)'s targets and the quota flags
    /// read exactly this slice).
    pub media_faults: Vec<MediaFault>,
    /// X-5c: script ops that unwound on a client-visible XX001 (a
    /// media round's detected damage surfacing at the client; recorded
    /// beside the rule, never inside it).
    pub client_xx001: u32,
    /// X5-R6: O-1 comparisons at the matched `c` satisfied by a
    /// detected loss (class MP only; reported beside the rule).
    pub o1_detected_loss: u32,
    /// X5-R6: this round is an R6 round (class MP/MW, recovery `Ok`,
    /// at least one XX001).
    pub r6_round: bool,
    /// Recycle renames the F-CRASH directory draw dropped (Q12): rename
    /// events whose from-name still resolves after the crash.
    pub renames_dropped: u64,
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
            livelock: false,
            livelock_site: "",
            livelock_parked: Vec::new(),
            baton: false,
            panicked: false,
            task_error: None,
            end_model: Model::new(),
            sched_yields: 0,
            obs: WorkloadObs::default(),
            nested_fired: 0,
            nested_points: Vec::new(),
            media_faults: Vec::new(),
            client_xx001: 0,
            o1_detected_loss: 0,
            r6_round: false,
            renames_dropped: 0,
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
            "|{:?}|{:?}|{}|{}|{}|{}|{:?}|{}|{}",
            self.monitors,
            self.acked_lost,
            self.meta_rollback,
            self.deadlock,
            self.livelock,
            self.livelock_site,
            self.task_error,
            self.baton,
            self.panicked
        ));
        s.push_str(&format!(
            "|{:?}|{}|{:?}|{}|{:?}",
            self.obs,
            self.nested_fired,
            self.media_faults.len(),
            self.renames_dropped,
            self.rollback_slots.len()
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
    /// The seeded media draw (card Work 5.5 step 3): after F-CRASH, the
    /// round draws its class's media faults from the crashed disk —
    /// F-MEDIA-PAGE (1–3, ≥60% on the image's referenced slots) or
    /// F-MEDIA-WAL (1–2). None for class C and the twin rounds. The
    /// draw's stream is seeded by `(seed, round)`, so the twin (the same
    /// crash resolutions, faults removed) stays schedule-identical.
    pub media: MediaDraw,
    /// A pre-supplied twin (card Work 2.5): the twin run's recovery
    /// reads and opened image slots, for this round's O-3 (c). `None`
    /// keeps X-5b's stand-in supply (this round's own reads and slots),
    /// which the directed tests use.
    pub twin_supply: Option<TwinSupply>,
    /// Nested recovery crashes to **draw** (card Work 5.5 step 4): the
    /// script's explicit `nested` points take precedence; otherwise each
    /// drawn point is uniform over this disk's recovery yield count
    /// (counted by a throwaway pass, so a point is always reachable).
    pub nested_draw: u32,
    /// **Probe pass** (the uniform crash's counting pass): run the
    /// workload only, skip the crash/faults/recovery/oracle — the
    /// outcome carries `sched_yields` and the crash-time observables,
    /// and nothing else is defined. The driver uses it to draw the
    /// scheduler-level uniform crash point.
    pub probe_pass: bool,
}

/// The round's seeded media draw kind (card Work 5.5 step 3; the class
/// MP/MW fault supply of the campaign).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MediaDraw {
    /// No draw (class C, twin rounds, directed faults only).
    #[default]
    None,
    /// F-MEDIA-PAGE: 1–3 page-file faults, ≥60% biased to the crashed
    /// image's referenced slots (meta included).
    Page,
    /// F-MEDIA-WAL: 1–2 WAL faults, uniform over every segment's
    /// candidates.
    Wal,
}

/// A pre-supplied twin for O-3 (c) (card Work 2.5): what the twin run's
/// recovery read and its opened image referenced — the same seed,
/// schedule and F-CRASH resolutions with the media faults removed.
#[derive(Debug, Clone, Default)]
pub struct TwinSupply {
    /// The twin recovery's reads `(file, offset, len)`.
    pub recovery_reads: Vec<(usize, u64, u64)>,
    /// The slots the twin's opened image referenced.
    pub image_slots: BTreeSet<u64>,
}

/// One open chunked transaction at the crash (the Q5 observable's row).
#[derive(Debug, Clone, Copy, Default)]
pub struct OpenChunk {
    /// The transaction id.
    pub txn: (u32, u64),
    /// Its first chunk's LSN.
    pub first: u64,
    /// Its marker's commit ts, when the marker was enqueued (`None` =
    /// the open, no-marker case).
    pub marker_ts: Option<u64>,
    /// Its last chunk's LSN.
    pub last: u64,
}

/// The workload pass's quota observables (card Work 5.6), snapshotted
/// from the store and harness slices the moment the tasks end — the
/// crash included: every field is the crash-time view.
#[derive(Debug, Clone, Default)]
pub struct WorkloadObs {
    /// The scripted crash fired during the workload (false: the crash
    /// point was never reached — a degenerate end-of-workload F-CRASH).
    pub crash_fired: bool,
    /// Splits inside the open checkpoint window (Q4).
    pub splits: u64,
    /// Merges inside the open checkpoint window (Q4).
    pub merges: u64,
    /// R_EPOCH exhaustion count (Q10).
    pub epoch_exhausted: u64,
    /// The free-set serialisation grew the file (Q17).
    pub freeset_grew: bool,
    /// Stale-record stops at renamed segment starts (Q11).
    pub stale_seg_reads: u64,
    /// Flips whose image held part of a commit above `T` (Q9).
    pub q9_partial_flips: u64,
    /// LPIDs dirtied in the still-open interval (Work 5.3's crash-time
    /// half; beside `dirty_max` for the completed ones).
    pub dirty_now_len: u64,
    /// The largest completed checkpoint interval's distinct dirtied LPIDs
    /// (Work 5.3).
    pub dirty_max: u64,
    /// Partial applies at the crash: `(ts, done, total, chunked)` (Q5's
    /// "marked and partially applied").
    pub partial_applies: Vec<(u64, u64, u64, bool)>,
    /// Chunked transactions open at the crash (Q5). A `None` marker ts
    /// is the open (no-marker) case; a `Some` is the marked case (the
    /// marker sits above every flip that has not covered it, or above
    /// the open flip's `T`).
    pub open_chunks: Vec<OpenChunk>,
    /// Bulk-load phases at the crash (Q6).
    pub load_phases: Vec<(u64, LoadPhase)>,
    /// A DropTree walk was in flight (Q7).
    pub drop_walk: Option<u64>,
    /// A GC sweep advanced past the previous `gc_w` inside an open
    /// checkpoint window (Q18).
    pub gc_adv_in_window: bool,
    /// The last checkpoint hook passed, as a position code (1 = Before0 …
    /// 6 = After3 — Q4's window; 0 = none).
    pub ckpt_step: u8,
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
                media_round: faults.class != FaultClass::C,
                ..HarnessState::default()
            },
        };
        Sched::new(host)
    };
    // The scheduler-level uniform crash (`CrashSpec::Yield`) is armed by
    // `run_script` itself, on the core it resets to size the task set —
    // see the arm there.
    run_script(&sched, script, seed);
    let sched_yields_total = sched.lock().sched.yields;
    let (mut disk, mut model, flip_pos, flags, wal_commits, events, obs) = {
        let g = sched.lock();
        // The workload-pass observables, snapshotted the moment the tasks
        // ended (a crash mid-task leaves the partial-apply and walk flags
        // exactly as the quota definitions read them).
        let st = &g.st;
        let partial_applies = st
            .apply_progress
            .iter()
            .map(|(ts, (done, total, chunked))| (*ts, *done, *total, *chunked))
            .collect();
        let open_chunks = st
            .wal
            .open_chunks
            .iter()
            .map(|(txn, first)| {
                let mut marker = None;
                let mut last = *first;
                for (lsn, (ts, is_marker, t)) in st.wal.buf_tags.iter() {
                    if *t != *txn {
                        continue;
                    }
                    if *is_marker {
                        marker = Some(*ts);
                    } else if *ts == 0 {
                        last = (*lsn).max(last);
                    }
                }
                OpenChunk {
                    txn: *txn,
                    first: *first,
                    marker_ts: marker,
                    last,
                }
            })
            .collect();
        let obs = WorkloadObs {
            crash_fired: g.sched.crashed,
            splits: st.ckpt_splits,
            merges: st.ckpt_merges,
            epoch_exhausted: st.epoch_exhausted,
            freeset_grew: st.freeset_grew,
            stale_seg_reads: st.stale_seg_reads,
            q9_partial_flips: st.q9_partial_flips,
            dirty_now_len: st.dirty_now.len() as u64,
            dirty_max: st.dirty_max,
            partial_applies,
            open_chunks,
            load_phases: g.h.load_phase.iter().map(|(t, p)| (*t, *p)).collect(),
            drop_walk: g.h.drop_walk,
            gc_adv_in_window: g.h.gc_adv_in_window,
            ckpt_step: g.h.ckpt_step,
        };
        (
            g.disk.clone(),
            clone_model(&g.h.model),
            st.wal.flip_enqueue_pos.clone(),
            (
                g.h.deadlock,
                g.h.baton,
                g.h.panicked,
                g.h.task_error.clone(),
                g.h.livelock,
                g.h.livelock_site,
                g.sched
                    .livelock_parked
                    .iter()
                    .enumerate()
                    .map(|(t, w)| {
                        format!(
                            "{t}:{}",
                            w.map(|k| format!("{k:?}"))
                                .unwrap_or_else(|| "run".to_owned())
                        )
                    })
                    .collect::<Vec<_>>(),
            ),
            g.h.wal_commits.clone(),
            g.disk.events.clone(),
            obs,
        )
    };
    if faults.probe_pass {
        // The counting pass (the uniform crash's address-space measure):
        // the workload's yield total and the crash-time observables are
        // defined; everything downstream of the crash is skipped.
        let mut outcome = RoundOutcome {
            class: faults.class,
            deadlock: flags.0,
            baton: flags.1,
            panicked: flags.2,
            livelock: flags.4,
            livelock_site: flags.5,
            livelock_parked: flags.6,
            ..Default::default()
        };
        outcome.sched_yields = sched_yields_total;
        outcome.client_xx001 = {
            let g = sched.lock();
            g.h.client_xx001
        };
        outcome.obs = obs;
        return outcome;
    }
    // Work 2.1: written/acked from the disk events — the write carrying
    // the commit record's last byte returned, and (SyncOn) a WAL sync
    // covering it returned / (SyncOff) the write returned.
    let (written_ts, acked_ts, unparsed_written) = derive_written_acked(&events, &wal_commits);
    for ts in written_ts {
        model.mark_written(ts);
    }
    for ts in acked_ts {
        model.mark_acked(ts);
    }
    // The F1 loud half: a commit whose record bytes were written yet can
    // never parse is a harness/store defect (only an over-cap enqueue
    // can produce it, and the cap refuses those) — the round fails,
    // never a silent model drop.
    let mut task_error = flags.3;
    if !unparsed_written.is_empty() && task_error.is_none() {
        task_error = Some(format!(
            "wal_commits written but unparsable (record len past segment): {unparsed_written:?}"
        ));
    }

    // ---- the crash and the faults ----
    // A chained round's disk carries the prior rounds' media list (taint
    // references survive lives, 1.4); this round's faults are the slice
    // appended from here on, and exactly that slice feeds O-3 (c) and the
    // quota flags.
    let media_base = disk.media.len();
    if script.crash.is_some() {
        let mut frng = Rng::new(derive(seed * 100 + round as u64, S_CRASH));
        let _ = disk.apply_crash(&mut frng, &faults.knobs);
        for f in faults.directed.iter() {
            disk.apply_one_media_fault(f);
        }
        // The seeded draw (card Work 5.5 step 3): the campaign's class
        // MP/MW faults, drawn from the crashed disk. The page draw's
        // referenced-slot bias set is the harness walk of the crashed
        // image (the meta slots included) — a harness input, never a
        // check.
        match faults.media {
            MediaDraw::None => {}
            MediaDraw::Page => {
                let referenced = durable_image(&disk, &cfg).1;
                let mut mrng = Rng::new(derive(seed * 100 + round as u64, S_MEDIA));
                disk.apply_media_page(&mut mrng, &referenced);
            }
            MediaDraw::Wal => {
                let mut mrng = Rng::new(derive(seed * 100 + round as u64, S_MEDIA));
                disk.apply_media_wal(&mut mrng);
            }
        }
        disk.new_round();
    }
    let round_media: Vec<MediaFault> = disk.media.get(media_base..).unwrap_or(&[]).to_vec();
    let media_targets = if round_media.is_empty() {
        FaultTargets::default()
    } else {
        let pf0 = disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
        FaultTargets::collect(&round_media, pf0)
    };
    // Q12 (card Work 5.6): recycle renames the F-CRASH directory draw
    // dropped — the workload's rename events (the event stream ends at
    // the crash) whose **from**-name still resolves on the crashed disk
    // (a survived rename leaves the from-name unbound).
    let renames_dropped = events
        .iter()
        .filter_map(|e| match e {
            DiskEvent::DirOpIssued { what, .. } if what.starts_with("rename /wal/") => {
                let from = what
                    .trim_start_matches("rename ")
                    .split(" ->")
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_owned();
                Some(from)
            }
            _ => None,
        })
        .filter(|from| disk.stat(from).is_some())
        .count() as u64;

    // ---- amcheck (i): on the crashed image, before recovery ----
    let (amcheck_i, image_slots, meta_i) = durable_image(&disk, &cfg);
    let pf = disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
    let tainted_slots: BTreeSet<u64> = image_slots
        .iter()
        .copied()
        .filter(|s| disk.range_tainted(pf, s * 4096, 4096))
        .collect();

    // ---- recovery with nested crashes ----
    // The points in force: the script's explicit list (the directed
    // tests), else the seeded draw — each point uniform over this disk's
    // recovery yield count, counted by a throwaway pass on a clone (a
    // blind draw past the recovery length would never fire). The points
    // are recorded on the outcome so the twin round (the same F-CRASH
    // resolutions) can crash at the same points.
    let nested_points: Vec<u64> = if !script.nested.is_empty() {
        script.nested.clone()
    } else if faults.nested_draw > 0 {
        let count_world = round_world(seed, disk.clone(), StoreInner::empty(cfg.clone(), 0));
        let ccx = Cx {
            w: &count_world,
            t: HARNESS,
        };
        let y0 = count_world.lock().sched.yields;
        let _ = ckpt::recover(&ccx, cfg.clone());
        let total = count_world.lock().sched.yields - y0;
        let mut nrng = Rng::new(derive(seed * 31 + round as u64, S_CRASH));
        (0..faults.nested_draw)
            .map(|_| 1 + nrng.below(total.max(1)))
            .collect()
    } else {
        Vec::new()
    };
    let mut nested_fired = 0u32;
    let mut recovery_disk = disk.clone();
    // X5-R13 (b): recovery itself livelocked (its single-task world
    // crossed `YIELD_BUDGET`) — recorded with the site, and the oracle is
    // skipped for this round: recovery produced no opened image to
    // compare, and an "XX001 at Open" would claim a detection that never
    // happened. The round fails through the deadlock fold.
    let mut recovery_livelock: Option<&'static str> = None;
    // Every exit of the loop below assigns this before it is read (the
    // Ok arm and the final Err arm both collect the attempt's reads);
    // the initializer only satisfies the borrow checker.
    #[allow(unused_assignments)]
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
        let arm = (nested_fired as usize) < nested_points.len();
        if arm {
            let at = nested_points
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
                if g.sched.livelock {
                    // The "crash" was the livelock failing the world
                    // (X5-R13 (b)); retrying would only re-livelock.
                    // (The counter above is undone: no nested crash
                    // fired.)
                    nested_fired -= 1;
                    recovery_livelock = Some(g.sched.livelock_site);
                    break Err(Fail::Sim(SimFail::Livelock(g.sched.livelock_site)));
                }
                recovery_served.extend(g.disk.served.clone());
                recovery_events = g.disk.events.clone();
                let _ = g.disk.apply_crash(&mut frng, &faults.knobs);
                recovery_disk = g.disk.clone();
                continue;
            }
            Err(e) => {
                let g = world.lock();
                if g.sched.livelock {
                    recovery_livelock = Some(g.sched.livelock_site);
                }
                // The failed attempt's reads are the twin's too (the
                // G7 clean shape: open **did** read both meta slots
                // before failing — without this the twin under-reported
                // them and O-3 (c) called the detection a false
                // positive, failing the round the ruling says passes).
                recovery_reads = g
                    .disk
                    .reads
                    .iter()
                    .skip(reads_before)
                    .map(|r| (r.inode, r.off, r.len))
                    .collect();
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
        livelock: flags.4,
        livelock_site: flags.5,
        livelock_parked: flags.6,
        task_error,
        ..Default::default()
    };
    match recovery {
        Err(Fail::Sim(SimFail::Livelock(site))) if recovery_livelock.is_some() => {
            // X5-R13 (b): recovery livelocked. There is no opened image
            // to judge, so the oracle's checks are skipped wholesale
            // (an "XX001 at Open" would claim a detection that never
            // happened); the round fails through the deadlock fold, with
            // the site recorded.
            outcome.deadlock = true;
            outcome.livelock = true;
            outcome.livelock_site = site;
            outcome.o1 = Err(format!("recovery livelock at {site}"));
            outcome.pass = fold_pass(
                false,
                outcome.task_error.as_deref(),
                true,
                outcome.panicked,
                outcome.baton,
            );
            outcome.lo = model.lower_bound(c_prev);
            outcome.hi = model.upper_bound(outcome.lo);
            outcome.cut = outcome.lo;
            outcome.crashed_disk = disk.clone();
            outcome.end_disk = disk.clone();
            {
                let mut m = clone_model(&model);
                m.truncate(outcome.cut);
                outcome.end_model = m;
            }
        }
        Ok((st, frontier, end_disk)) => {
            let (opened_meta, _opened_xx) = read_opened_meta(&end_disk);
            let opened_t = opened_meta.as_ref().map(|m| m.t).unwrap_or(0);
            let gc_w = opened_meta.as_ref().map(|m| m.gc_w).unwrap_or(0);
            let world = round_world(seed, end_disk.clone(), st.clone());
            let cx = Cx {
                w: &world,
                t: HARNESS,
            };
            let mut reader = StoreReader {
                w: &world,
                defects: Vec::new(),
            };
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
            // X5-R11 (d): a `Corrupt` failure marks the published-set
            // read failed (O-1 (a) is then a detected loss in MP) and
            // still records the `Read23` site; any other failure is a
            // harness defect the reader records (folded into task_error
            // after the check).
            match reader.published(frontier) {
                Ok(p) => ex.published = p,
                Err(Fail::Corrupt(_)) => {
                    ex.published_read_failed = true;
                    ex.xx001.push(XxSite::Read23);
                }
                Err(e) => {
                    reader.map(e, "published");
                    ex.xx001.push(XxSite::Read23);
                }
            }
            // X5-R11 (b): a slotted amcheck (i) finding is an XX001
            // detection site — the detection table and O-3 (b)-(d) must
            // see it (the draft pushed only `Read23` and `Open`, so an
            // amcheck detection was invisible to the verdict).
            if amcheck_i.iter().any(|f| f.slot.is_some()) && !ex.xx001.contains(&XxSite::AmcheckI) {
                ex.xx001.push(XxSite::AmcheckI);
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
            // The class MP/MW twin supply (card Work 2.5): the
            // campaign's pre-supplied twin (the faultless run's recovery
            // reads and opened image slots) when present; else this
            // round's own reads and slots — X-5b's directed-test
            // stand-in, collected through the caller's collector. A
            // class MP/MW round with neither fails closed in the oracle,
            // as X-5a round 1 ruled.
            let twin_info = match &faults.twin_supply {
                Some(t) => TwinInfo {
                    page_file: pf,
                    recovery_reads: t.recovery_reads.clone(),
                    image_slots: t.image_slots.clone(),
                },
                None => TwinInfo {
                    page_file: pf,
                    recovery_reads: recovery_reads.clone(),
                    image_slots: image_slots.clone(),
                },
            };
            let twin_ref = (twin.is_some() || faults.twin_supply.is_some()).then_some(&twin_info);
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
            // X5-R11 (b): the (ii) sites, same as the (i) push above.
            if findings_ii.iter().any(|f| f.slot.is_some())
                && !ex.xx001.contains(&XxSite::AmcheckIi)
            {
                ex.xx001.push(XxSite::AmcheckIi);
            }
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
            // X5-R11 (c): the 2.3 reads' non-Corrupt failures are
            // harness defects — a loud task error, and the seed fails.
            if !reader.defects.is_empty() {
                let d = reader.defects.join("; ");
                cx.with("h:rdefect", |c| {
                    let h = c.h();
                    if h.task_error.is_none() {
                        h.task_error = Some(format!("2.3 read defect: {d}"));
                    }
                    Ok(())
                })
                .ok();
            }
            outcome.o1 = verdict.o1.clone();
            outcome.o1f_ok = verdict.o1f_ok;
            outcome.o1_detected_loss = verdict.o1_detected_loss;
            outcome.r6_round = faults.class != FaultClass::C && ex.ok && !ex.xx001.is_empty();
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
            // X5-R11 (b): the failed-recovery path records the (i) sites
            // beside `Open` (the ruling names all three paths: real,
            // twin — the same code — and failed-recovery).
            if amcheck_i.iter().any(|f| f.slot.is_some()) && !ex.xx001.contains(&XxSite::AmcheckI) {
                ex.xx001.push(XxSite::AmcheckI);
            }
            let mut nop = NopReader;
            // The failed-recovery twin supply: the pre-supplied twin when
            // present (the G7 clean shape's reads come from the failed
            // attempt otherwise — open did read the metas before failing).
            let twin_info = match &faults.twin_supply {
                Some(t) => TwinInfo {
                    page_file: pf,
                    recovery_reads: t.recovery_reads.clone(),
                    image_slots: t.image_slots.clone(),
                },
                None => TwinInfo {
                    page_file: pf,
                    // The failed recovery's own reads (the G7 clean
                    // shape: open read both meta slots before failing —
                    // an empty set here made O-3 (c) call the detection
                    // a false positive).
                    recovery_reads: recovery_reads.clone(),
                    image_slots: image_slots.clone(),
                },
            };
            let twin_on =
                faults.class != FaultClass::C && (twin.is_some() || faults.twin_supply.is_some());
            let verdict = check_round(
                &model,
                faults.class,
                c_prev,
                &ex,
                &mut nop,
                &media_targets,
                if twin_on { Some(&twin_info) } else { None },
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
    // ---- the new per-round record fields (both branches) ----
    outcome.sched_yields = sched_yields_total;
    outcome.obs = obs;
    outcome.nested_fired = nested_fired;
    outcome.nested_points = nested_points;
    outcome.media_faults = round_media;
    outcome.renames_dropped = renames_dropped;
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
///
/// The third return is the **loud** half of the gate's round-3 F1: every
/// commit whose record header is present in some WAL file's issued
/// writes at its segment-relative position with its own lsn, but whose
/// `len` runs past the segment end — bytes written, never parseable
/// (§12.2 never places such a record; only an over-cap enqueue ever
/// could, and the cap now refuses it). The harness fails the round on
/// any; dropping the entry silently let an acked commit vanish from the
/// model while recovery lost it too (the F1 demonstration).
pub fn derive_written_acked(
    events: &[DiskEvent],
    wal_commits: &BTreeMap<u64, (bool, u64, u64)>,
) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    // inode → (off → latest issued bytes) and the return/sync extents.
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
    let mut unparsed_written = Vec::new();
    for (ts, (sync_on, lsn, _len)) in wal_commits.iter() {
        let Some((inode, end)) = ts_pos.get(ts).copied() else {
            // The walk never yielded this commit's record. If its header
            // sits issued at the segment-relative position with its own
            // lsn and a length past the segment end, the bytes were
            // written but can never parse — report it (F1); the harness
            // fails the round. A record simply never written (or torn
            // mid-body, a normal crash outcome) has no full header or a
            // length that fits, and stays silent.
            if let Some(rec_len) = issued_header_len(&files, *lsn, seg)
                && (*lsn % seg) + rec_len > seg
            {
                unparsed_written.push(*ts);
            }
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
    (written, acked, unparsed_written)
}

/// The record length an issued NWAL header carries at `lsn % seg`, if
/// some file's stitched writes hold a magic-correct header with exactly
/// this lsn embedded (the F1 report's discriminator). `None` when no
/// file's issued bytes hold such a header.
fn issued_header_len(files: &BTreeMap<usize, FileWrites>, lsn: u64, seg: u64) -> Option<u64> {
    let off = lsn % seg;
    for writes in files.values() {
        let hdr = stitched(writes, off, 24);
        if hdr.len() < 24 || !magic_ok(&hdr) {
            continue;
        }
        let embedded = {
            let s = hdr.get(16..24)?;
            let mut a = [0u8; 8];
            a.copy_from_slice(s);
            u64::from_le_bytes(a)
        };
        if embedded == lsn {
            let len = {
                let s = hdr.get(4..8)?;
                let mut a = [0u8; 4];
                a.copy_from_slice(s);
                u32::from_le_bytes(a) as u64
            };
            return Some(len);
        }
    }
    None
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
    // G7's shared selection, positional here unconditionally. A
    // misplaced newer meta is never opened — no image is opened, so the
    // walk reports the situation and offers no meta (O-3 (e) is then
    // vacuous, exactly as the ruling's D17 clean arm expects).
    let (meta, _slot) = match crate::store::page::select_meta(&m1, &m2, false) {
        crate::store::page::MetaPick::Win { meta, slot } => (meta, slot),
        crate::store::page::MetaPick::MisplacedNewer { .. } => {
            return (
                vec![Finding {
                    slot: None,
                    what: "no valid meta (G7: a misplaced valid meta outranks every placed one)"
                        .into(),
                }],
                BTreeSet::new(),
                None,
            );
        }
        crate::store::page::MetaPick::NoValid => {
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
    // The pages the opened map references (X5-R11 (a), P ruling
    // 2026-10-31): amcheck (i) applies §3.3 to **every mapped LPID's
    // `Ref`** — the directory and tree pages are LPIDs of the map, and
    // the earlier directory→tree walk never reached a leaf under an
    // inner page: `walk_map_disk` below pre-inserts every mapped slot
    // into `used`, and the walk's `used`-dedup then `continue`d past
    // the descent of inner pages on their first visit, so a tainted
    // referenced leaf (D17's slot 92, seeds 7–8's map-referenced tree
    // leaves) was never §3.3-checked and never named — hidden behind
    // O-3 (d), which failed those rounds before the (e) completeness
    // half could matter. The loop is also strictly more thorough than
    // the walk: a mapped-but-dir-unreachable page (a dropping tree's
    // pages, a directory inner's stray cell) is checked too, and the
    // expected id of every mapped page is its LPID (check (3) is
    // positional — the gate's R4).
    for (lpid, r) in flat.iter() {
        let b = disk.durable_read(pf, r.slot * 4096, 4096);
        if crate::store::page::check(b.as_slice(), r, *lpid, None).is_err() {
            out.push(Finding {
                slot: Some(r.slot),
                what: "§3.3 fail on mapped page".into(),
            });
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
/// The opened meta of a disk (durable bytes).
pub fn read_opened_meta(disk: &SimDisk) -> (Option<crate::store::page::Meta>, ()) {
    let pf = match disk.stat(crate::sim::PAGE_FILE) {
        Some(f) => f,
        None => return (None, ()),
    };
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    // G7's shared selection, positional here unconditionally (the walk
    // runs on disks whose recovery already succeeded, so `Win` is the
    // expected case; a misplaced state yields None like the (i) walk).
    let best = match crate::store::page::select_meta(&m1, &m2, false) {
        crate::store::page::MetaPick::Win { meta, .. } => Some(meta),
        crate::store::page::MetaPick::MisplacedNewer { fallback, .. } => fallback,
        crate::store::page::MetaPick::NoValid => None,
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
    /// X5-R11 (c): non-Corrupt failures seen during the 2.3 reads —
    /// each is a harness defect (the harness sets a task error from
    /// them after the check; the seed fails). Only `Fail::Corrupt`
    /// maps to `ReadFail`.
    pub defects: Vec<String>,
}

impl StoreReader<'_> {
    /// X5-R11 (c)'s mapping: `Fail::Corrupt` (XX001) is a read failure
    /// the oracle understands; anything else (`Sim`, `Poisoned`,
    /// `Invalid`) is a harness defect, recorded and never hidden.
    fn map(&mut self, e: Fail, what: &str) -> ReadFail {
        if !matches!(e, Fail::Corrupt(_)) {
            self.defects.push(format!("{what}: {e:?}"));
        }
        ReadFail
    }

    /// The published set at the frontier (a catalog scan, 2.3). The
    /// error is the store's own `Fail`: X5-R11 (d) — the caller marks
    /// `published_read_failed` on `Corrupt` (and keeps the `Read23`
    /// site); any other failure is a defect via [`Self::map`].
    pub fn published(&mut self, frontier: u64) -> Result<BTreeSet<u64>, Fail> {
        let cx = Cx {
            w: self.w,
            t: HARNESS,
        };
        let root = cx.with("r:cat", |c| Ok(tree::catalog_root(c.store())))?;
        if root == 0 {
            return Ok(BTreeSet::new());
        }
        let scan = tree::tree_scan(&cx, root, frontier)?;
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
        let root = match cx.with("r:root", |c| Ok(c.store().dir.get(&tree).map(|e| e.root))) {
            Ok(r) => r.ok_or_else(|| {
                self.defects
                    .push(format!("scan({tree}): tree missing in directory"));
                ReadFail
            }),
            Err(e) => Err(self.map(e, "scan")),
        }?;
        match tree::tree_scan(&cx, root, s) {
            Ok(x) => Ok(x),
            Err(e) => Err(self.map(e, "scan")),
        }
    }

    fn raw(&mut self, tree: u64) -> Result<Vec<RawEntry>, ReadFail> {
        let cx = Cx {
            w: self.w,
            t: HARNESS,
        };
        let root = match cx.with("r:root2", |c| Ok(c.store().dir.get(&tree).map(|e| e.root))) {
            Ok(r) => r.ok_or_else(|| {
                self.defects
                    .push(format!("raw({tree}): tree missing in directory"));
                ReadFail
            }),
            Err(e) => Err(self.map(e, "raw")),
        }?;
        let raw = match tree::tree_raw(&cx, root) {
            Ok(x) => x,
            Err(e) => return Err(self.map(e, "raw")),
        };
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
