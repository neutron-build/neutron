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
    DiskEvent, FaultClass, FaultKnobs, HARNESS, MediaFault, MediaKind, Sched, SchedCore, SchedHost,
    SimDisk, SimFail, TaskId, WaitKey,
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
    /// **X-5c-F3-F1 step 1 (e): the client-observed acks** — every
    /// commit ts whose own §12.4 ack wait (`wal_wait_durable` for
    /// SyncOn, `wal_wait_written` for SyncOff; the chunked marker's
    /// §12.7 durability wait included) returned before the crash. A
    /// **diagnostic only**, never serialized, never a digest input and
    /// never an oracle input: it exists so `E4_ACK_DIAG` can compare it
    /// against `derive_written_acked`'s derived `acked` flags (the
    /// derivation feeds `lo`, so a divergence is an instrumentation
    /// question for P, never something the worker patches in passing).
    pub client_acked: BTreeSet<u64>,
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
    /// **G8 (X5-R16's conditional fill):** the round's workload ended
    /// on a poisoned store — the crash was delivered at the poison
    /// point and the crash desc is recorded as `poison` (never a
    /// NEVER-FIRED: `crash_fired` follows the world's `crashed` flag).
    pub poison_crash: bool,
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
    /// Per checkpoint completion index, the bitmask of the hooks it
    /// reached (X5-R16 NEVER-FIRED; see [`WorkloadObs::ckpt_hooks_hit`]).
    /// Grown by the checkpointer's hook closure as checkpoints run.
    pub ckpt_hooks_hit: Vec<u16>,
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
        // S-1-F3 §12.2 (W2; X-5c-F3-F3a step 4): the txn id is
        // `(boot_id, n)`, n ≥ 1 dense per boot (the harness's
        // per-round counter is per boot — a boot is a round's life;
        // the store's session id comes from its boot checkpoint).
        let boot = c.store().boot_id;
        let h = c.h();
        h.txn_counter += 1;
        Ok((boot, h.txn_counter))
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
            // G8 (9.2, W8): the mark is in the commit critical section
            // wait's predicate (the park below holds one baton with it).
            st.alive()?;
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
        // X-5c-F3-F1 step 1 (e): the client observed this commit's
        // durability wait return (a diagnostic; see
        // `HarnessState::client_acked`).
        cx.with("h:cliack1", |c| {
            c.h().client_acked.insert(ts);
            Ok(())
        })?;
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
    // X-5c-F3-F1 step 1 (e): the client-observed ack (a diagnostic; see
    // `HarnessState::client_acked`). A task that crashes inside the
    // wait never records the ts — that is exactly the comparison's
    // point.
    cx.with("h:cliack2", |c| {
        c.h().client_acked.insert(ts);
        Ok(())
    })?;
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
            // G8 (9.2, W8): the ts-order turn's wait carries the mark.
            st.alive()?;
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
                    // **G8's harness half (X5-R16's conditional fill):**
                    // a poisoned store is the end of the round's
                    // workload. The round's crash is delivered at once
                    // (every task unwinds as `Crashed`, the world's
                    // `crashed` flag makes `crash_fired` true — never a
                    // NEVER-FIRED), the crash point is recorded as
                    // `poison`, and the round continues to its faults,
                    // recovery and the ruled oracle as usual.
                    if matches!(e, Fail::Poisoned(_)) {
                        let world_crashed0 = cx
                            .with("h:poison", |c| {
                                let crashed = c.sched_mut().crashed;
                                if !crashed {
                                    let h = c.h();
                                    h.poison_crash = true;
                                    h.workload_done = true;
                                }
                                Ok(crashed)
                            })
                            .unwrap_or(true);
                        if !world_crashed0 {
                            cx.w.wake(WaitKey::WalAcks);
                            cx.w.wake(WaitKey::MetaDurable);
                            cx.w.wake(WaitKey::Applied);
                            cx.w.deliver_crash();
                        }
                        return Err(SimFail::Crashed);
                    }
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
                        // **X-5c-F3-F3b step 3 (d):** the poisoner's
                        // exit wakes **every** wait key with a parked
                        // task (the G8 broadcast — the old three-key
                        // wake left Pin/Tree/Gate waiters parked; the
                        // mark in their predicates now returns them).
                        store_poison_broadcast(&cx);
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
        let next = match tree::gc_step(cx, t, root, w, &resume) {
            Ok(n) => n,
            Err(e) => {
                // **S-1-F3 §9.2 (W8, gate edit 3; X-5c-F3-F3b step 3
                // (a)):** a background store job's step — the GC sweep
                // — poisons the store on a non-crash failure, marked
                // before the sweep's remaining state unwinds (each
                // `gc_step` releases its own latches on exit; a sim
                // crash is never a poison), and the G8 (d) broadcast
                // wakes every parked waiter once it has.
                if !matches!(e, Fail::Sim(_)) {
                    cx.with("h:gcpois", |c| {
                        let st = c.store();
                        if st.poisoned.is_none() {
                            st.poisoned =
                                Some(format!("gc sweep of tree {t} failed: {e} (9.2, G8)"));
                        }
                        Ok(())
                    })
                    .ok();
                    store_poison_broadcast(cx);
                }
                return Err(e);
            }
        };
        if next.is_empty() {
            return Ok(());
        }
        resume = next;
        cx.yield_now("h:gcyield")?;
    }
}

/// **X-5c-F3-F3b step 3 (d): the harness-side G8 broadcast** — wake
/// every wait key that has a parked task (the fixed keys plus every
/// held tree's), the same set [`crate::store::prims::poison_broadcast`]
/// wakes, for the poison sites on harness paths (the GC sweep, the
/// script-op error exit below).
fn store_poison_broadcast(cx: &Cx<'_, RoundWorld>) {
    let trees: Vec<u64> = cx
        .with("h:bcast", |c| {
            Ok(c.store().tree_mu.keys().copied().collect::<Vec<u64>>())
        })
        .unwrap_or_default();
    for key in [
        WaitKey::WalAcks,
        WaitKey::Applied,
        WaitKey::MetaDurable,
        WaitKey::Pin,
        WaitKey::GateShared,
        WaitKey::GateFlip,
    ] {
        cx.w.wake(key);
    }
    for t in trees {
        cx.w.wake(WaitKey::Tree(t));
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
            // G8 (9.2, W8): the bulk-load publication wait's predicate
            // carries the mark.
            c.store().alive()?;
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
                // G8 (9.2, W8 (e)): nothing is written after the mark —
                // the writer checks `alive()` before each turn.
                if c.store().poisoned.is_some() {
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
            .with("h:ev", |c| {
                Ok(c.sched_mut().crashed || c.store().poisoned.is_some())
            })
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
                if c.sched_mut().crashed || c.store().poisoned.is_some() {
                    // G8 (9.2, W8 (e)): the checkpointer issues no step
                    // after the mark.
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
            let ck_idx = idx;
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
                    let h = c.h();
                    h.ckpt_step = step;
                    // X5-R16 NEVER-FIRED: this checkpoint (completion
                    // index `idx`) reached `hook` — the targeted draw's
                    // counting pass reads this mask.
                    let bit = ckpt::hook_bit(hook);
                    if c.h().ckpt_hooks_hit.len() <= ck_idx as usize {
                        c.h().ckpt_hooks_hit.resize(ck_idx as usize + 1, 0);
                    }
                    if let Some(slot) = c.h().ckpt_hooks_hit.get_mut(ck_idx as usize) {
                        *slot |= bit;
                    }
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
    /// **X5-R32 (b):** the damaged-referenced arm fired (the verdict's
    /// `r32`, a diagnostic the pass fold never reads; from F3-F1's
    /// Acceptance file on, every `r32 = true` round must be class MP).
    pub r32_round: bool,
    /// Recycle renames the F-CRASH directory draw dropped (Q12): rename
    /// events whose from-name still resolves after the crash.
    pub renames_dropped: u64,
    /// The digest of the canonical record (byte-identical for the same
    /// inputs; `tests/harness.rs`).
    pub digest: u64,
    /// X-5c-F2 step 1 (e): the anchor-0 fallback's precondition broke —
    /// this life's event stream holds a page-file
    /// `SyncReturned { all: false }` while the fallback anchor 0 was in
    /// force. A harness-side tripwire lifted to a seed-level defect by
    /// the driver (never an oracle input; never silently widening R).
    pub r5_anchor0_defect: bool,
    /// X5-R30 (a): every served range of the round lies wholly inside
    /// slots of `K` (vacuously true when nothing was served). A
    /// diagnostic in the record, never an oracle input.
    pub served_k: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `skx`** — rule 1's predicate,
    /// exact (see [`r30_predicates`]). A diagnostic in the record,
    /// never an oracle input; false unless `served` holds.
    pub skx: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `smx`** — rule 2's predicate,
    /// exact (the opened meta slot's own serve, its bytes a
    /// sync_all-covered DWrite). A diagnostic, never an oracle input.
    pub smx: bool,
    /// **X-5c-F3-F1 step 6 (X5-R30 (c)): `pass_na`** — the round's
    /// final verdict re-evaluated with `OracleCfg { no_taint: true }`,
    /// recorded only when `served ∧ (skx ∨ smx)`, else null.
    pub pass_na: Option<bool>,
    /// X5-R28 (a): the kept set `K` at the amcheck (i) point —
    /// SimDisk-history-computed (see [`kept_set_k`]); the oracle's
    /// O-3 (e) amendment reads it from `Extracted::kept_slots`.
    pub kept_slots: BTreeSet<u64>,
    /// X5-R28 (a): the kept set `K_ii` at the amcheck (ii) point
    /// (empty unless (ii) ran).
    pub kept_slots_ii: BTreeSet<u64>,
    /// **X5-R34 (f):** the round's `ack_over` — the SyncOn commit ts's
    /// whose own §12.4 ack wait returned before the crash while their
    /// record ends above the derived durable end `D` (and outside `Rc`).
    /// Empty when there are none; a non-empty list is an
    /// ACK-OVER-HOLE store failure (escalated, never relaxed). A
    /// diagnostic in the record (`run` writes it, `verify` parses it);
    /// the pass fold reads it only through the round's task error.
    pub ack_over: Vec<u64>,
    /// **S-1-F3 §14 step 3a (W6 (ii); X-5c-F3-F3b step 2):** the
    /// round's recovery `Err` is I-WAL-COVER's refusal — a typed match
    /// on `Fail::WalLoss`, never a text match. The XX001 site stays
    /// `Open` (X5-R11) and the seed ends at that round, as for any
    /// recovery `Err`. A diagnostic in the record; the pass fold does
    /// not read it.
    pub cover_err: bool,
    /// **X5-R33 (a):** the round's own life of the record stream — the
    /// driver carries it into the next round's supply. `None` on twin
    /// and probe rounds.
    pub x33_life: Option<X33Life>,
    /// **X5-R33 (c): `h`** — the opened meta's writing-life bound (the
    /// record's `x33h` diagnostic; never an oracle input beyond
    /// `Extracted::mw_h`).
    pub x33h: u64,
    /// **X5-R33 (c): `a`** — the round's largest X5-R34-acked SyncOn ts
    /// below the first damaged WAL byte (the record's `x33a`).
    pub x33a: u64,
    /// **X5-R33 (e): `wal_loss_explained`** (the record's `x33x`).
    pub x33x: bool,
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
            r32_round: false,
            skx: false,
            smx: false,
            pass_na: None,
            renames_dropped: 0,
            digest: 0,
            r5_anchor0_defect: false,
            served_k: false,
            kept_slots: BTreeSet::new(),
            kept_slots_ii: BTreeSet::new(),
            ack_over: Vec::new(),
            cover_err: false,
            x33_life: None,
            x33h: 0,
            x33a: 0,
            x33x: false,
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
    /// **G8:** the workload ended on a poisoned store; the round's crash
    /// desc is recorded as `poison` (never a NEVER-FIRED: the poison
    /// delivery sets the world's `crashed` flag, so `crash_fired`
    /// reads true).
    pub poison_crash: bool,
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
    /// **X5-R16 NEVER-FIRED (P ruling nv3-X-5c-esc-3): per checkpoint
    /// completion index, the bitmask of the hooks it reached** (bit
    /// `i` = [`crate::store::ckpt::CKPT_HOOKS[i]`], set by the
    /// checkpointer's hook closure). The driver's targeted draw reads
    /// it from a counting pass and picks an index that actually
    /// reaches the drawn hook — "make every targeted draw reach its
    /// hook, deterministically from the seed. A deterministic counting
    /// pass, like the uniform draw's, is allowed."
    pub ckpt_hooks_hit: Vec<u16>,
}

// ---- X5-R33 (binding, pre-registered): the harness supply ----

/// **X5-R33 (a)/(b):** one life of the per-seed record stream — the
/// Commit/CommitMarker records `(lsn, len, ts)` one round's workload
/// appended, beside the life's truncation point (`p0`, X5-R34's), its
/// boot id, the frontier it started from, and its append position at
/// the crash or end. Carried across rounds by the seed driver like the
/// model; never store memory (the positions come from `wal_commits` and
/// the store's append snapshot alone).
#[derive(Debug, Clone, Default)]
pub struct X33Life {
    /// The life's truncation point (the LSN it began appending at).
    pub p0: u64,
    /// This life's own `Commit`/`CommitMarker` records.
    pub records: Vec<(u64, u64, u64)>,
    /// The boot id the life's metas carry.
    pub boot: u32,
    /// The frontier the life started from.
    pub started_frontier: u64,
    /// The append position at the life's crash or end.
    pub append_end: u64,
}

/// **X5-R33 (b): the stream as it stood at a life's start** — for each
/// earlier life, its records below the lowest truncation point of every
/// later life up to it (`p0_i`); a record a later truncation superseded
/// never belongs to it.
pub fn x33_start_log(lives: &[X33Life], i: usize) -> Vec<(u64, u64, u64)> {
    let mut out = Vec::new();
    let p0 = lives.get(i).map(|l| l.p0).unwrap_or(0);
    for (j, l) in lives.iter().enumerate() {
        if j >= i {
            break;
        }
        // The lowest truncation point of every later life up to i:
        // min(p0_{j+1} ..= p0_i).
        let mut floor = p0;
        for k in (j + 1)..=i {
            if let Some(pk) = lives.get(k).map(|lk| lk.p0) {
                floor = floor.min(pk);
            }
        }

        for r in l.records.iter() {
            if r.0 < floor {
                out.push(*r);
            }
        }
    }
    out
}

/// **X5-R33 (c): `h`** — the largest ts whose Commit/CommitMarker record
/// lies wholly below `w` (`lsn + len ≤ w`), in the stream as it stood
/// when the meta carrying `w` was written: the writing life's start log
/// plus its own records. 0 when none.
pub fn x33_h_of(lives: &[X33Life], i: usize, w: u64) -> u64 {
    let mut best = 0u64;
    if let Some(l) = lives.get(i) {
        for (lsn, len, ts) in l.records.iter() {
            if lsn + len <= w {
                best = best.max(*ts);
            }
        }
    }
    for (lsn, len, ts) in x33_start_log(lives, i).iter() {
        if lsn + len <= w {
            best = best.max(*ts);
        }
    }
    best
}

/// **X5-R33 (c): the damaged WAL LSN ranges of the round** — this
/// round's media faults (and MISDIRECTED stray destinations) on WAL
/// inodes, mapped to absolute LSNs through the first LSN of the segment
/// name bound to the target inode at the crash (the current view of the
/// crashed disk). Empty when the round damaged no WAL byte.
pub fn x33_damaged_ranges(
    disk: &SimDisk,
    round_media: &[MediaFault],
    page_file: usize,
) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let wal_files = disk.list_dir(crate::sim::WAL_DIR);
    for f in round_media.iter() {
        if f.inode == page_file {
            continue;
        }
        // The segment name bound to the target inode at the crash.
        let Some(first) = wal_files.iter().find(|(_, fid)| *fid == f.inode) else {
            continue;
        };
        let Ok(seg_first) = u64::from_str_radix(first.0.trim_end_matches(".log"), 16) else {
            continue;
        };
        out.push((seg_first + f.off, seg_first + f.off + f.len));
        if let MediaKind::Misdirected { to_off } = &f.kind {
            out.push((seg_first + to_off, seg_first + to_off + f.len));
        }
    }
    out
}

/// **X5-R33 (c): `a`** — the largest X5-R34-acked SyncOn ts whose
/// record ends at or below the first damaged WAL byte; with no damaged
/// WAL byte, the round's largest acked SyncOn ts.
pub fn x33_a_of(
    acked: &[u64],
    wal_commits: &BTreeMap<u64, (bool, u64, u64)>,
    damaged: &[(u64, u64)],
) -> u64 {
    let first_damaged = damaged.iter().map(|&(a, _)| a).min();
    let mut best = 0u64;
    for ts in acked.iter() {
        let Some(&(sync_on, lsn, len)) = wal_commits.get(ts) else {
            continue;
        };
        if !sync_on {
            continue;
        }
        match first_damaged {
            Some(d) if lsn + len > d => continue,
            _ => {}
        }
        best = best.max(*ts);
    }
    best
}

/// **X5-R33 (e): `wal_loss_explained`** — some damaged WAL LSN lies in
/// `[wal_start_lsn, wal_ckpt_end)` of the opened meta.
pub fn x33_explained(damaged: &[(u64, u64)], start: u64, end: u64) -> bool {
    damaged.iter().any(|&(a, b)| a < end && b > start)
}

/// **X5-R33 (d): the harness cross-check over one meta write.** Returns
/// the mismatch text, or `None`. `expected` names the derived values in
/// the failure text. The four bounds (the gate's G4 (c) form):
/// 1. no Commit/CommitMarker record that the writing life appended
///    straddles `w`;
/// 2. every such record with ts `≤ T` ends at or below `w`;
/// 3. `w` is at or below that life's append position at its crash or
///    end;
/// 4. `t_hi` is the largest ts among that life's records ending at or
///    below `w`, or, if there is none, the frontier the writing life
///    started from.
pub fn x33_cross_check_meta(
    ckpt_id: u64,
    w: u64,
    t_hi: u64,
    t: u64,
    life: &X33Life,
    start_log: Vec<(u64, u64, u64)>,
) -> Option<String> {
    // The first violated bound is the reported one (deterministic).
    let mut why = String::new();
    let mut note = |w_: String| {
        if why.is_empty() {
            why = w_;
        }
    };
    for (lsn, len, ts) in life.records.iter().chain(start_log.iter()) {
        let end = lsn + len;
        if *lsn < w && end > w {
            note(format!("record [{lsn},{end}) straddles wal_ckpt_end"));
        }
        if *ts <= t && end > w {
            note(format!(
                "record ts {ts} <= T={t} ends at {end} above wal_ckpt_end"
            ));
        }
    }
    if w > life.append_end {
        note(format!(
            "wal_ckpt_end {w} above the life's append {}",
            life.append_end
        ));
    }
    let want_hi = life
        .records
        .iter()
        .filter(|(lsn, len, _)| lsn + len <= w)
        .map(|(_, _, ts)| *ts)
        .max();
    let expect_hi = want_hi.unwrap_or(life.started_frontier);
    if t_hi != expect_hi {
        note(format!("ts_ckpt_hi {t_hi} != expected {expect_hi}"));
    }
    if why.is_empty() {
        None
    } else {
        Some(format!(
            "X5-R33 cross-check: meta {ckpt_id} wal_ckpt_end {w} / ts_ckpt_hi {t_hi} vs {why}"
        ))
    }
}

/// Run one round: workload → crash → faults → recovery (nested crashes)
/// → extraction → oracle → monitors → the harness checkpoint. Returns
/// the outcome; the end state feeds the next round.
/// The X5-R33 supply a real round receives: the seed's earlier lives
/// (immutable; the round's own life is returned through the outcome for
/// the driver to carry), and the frontier this round's boot started
/// from (the previous round's recovery frontier; 0 in round 0). Twin
/// and probe rounds pass `None`.
#[derive(Default)]
pub struct X33In<'a> {
    /// The seed's earlier lives, in round order.
    pub lives: &'a [X33Life],
    /// The current boot's starting frontier.
    pub started_frontier: u64,
}

/// Run one round: workload → crash → faults → recovery (nested
/// crashes) → extraction → oracle → monitors → the harness checkpoint.
/// Returns the outcome; the end state feeds the next round.
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
    x33: Option<&X33In<'_>>,
) -> RoundOutcome {
    // ---- the workload pass ----
    // **X5-R35 (a):** the event count the incoming disk carries is the
    // boundary of X5-R34's `E` — everything the capture holds below it
    // is the previous round's recovery and harness boot checkpoint (the
    // slice itself is taken after the capture, through
    // [`x5r34_workload_events`]; no other reader of the capture
    // changes).
    let pre_workload = start_disk.events.len();
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
    let (
        mut disk,
        mut model,
        flip_pos,
        flags,
        wal_commits,
        events,
        obs,
        workload_served,
        client_acked,
    ) = {
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
            ckpt_hooks_hit: g.h.ckpt_hooks_hit.clone(),
            poison_crash: g.h.poison_crash,
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
            // X5-R30 (a): the workload pass's served-corruption ranges
            // (the round's served_k input; `new_round` below clears the
            // disk's list, so they are captured here).
            g.disk.served.clone(),
            // X-5c-F3-F1 step 1 (e): the client-observed acks (a
            // diagnostic for `E4_ACK_DIAG`).
            g.h.client_acked.clone(),
        )
    };
    // X5-R33 (a): the workload life's append position at its end
    // (snapshotted with the workload's other observables).
    let append_end = sched.lock().st.wal.append;
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
    // the commit record's last byte returned, and the ack per §12.4.
    // **X5-R34 (binding):** the SyncOn acked half is I-ACK-PREFIX's
    // `durable_end` `D` from SimDisk history — `p0` is the WAL append
    // position this round's workload started from (the valid end of the
    // recovery that produced the round's base), and the `/wal/` durable
    // bindings are read at the end of the workload pass through the one
    // observer accessor this card adds. Never store memory (X5-R34 (i)).
    let p0 = start_store.wal.append;
    // **X5-R35 (a):** X5-R34's `E` is the workload pass's slice of the
    // capture (`x5r34_workload_events` above); the previous round's
    // recovery and boot checkpoint that precede the workload in the
    // capture — the truncation zero-write and its sync included — are
    // not in `E`, so a synced zero-fill over an enqueued, never-written
    // record can never count as its covering write.
    let e4_events = x5r34_workload_events(&events, pre_workload);
    let wal_bindings = disk.durable_dir_bindings(crate::sim::WAL_DIR);
    // *Guard* (X5-R34 (b) 2): every record in `wal_commits` has
    // `lsn ≥ p0`. A violation is a harness defect and fails the round
    // loudly.
    let p0_guard = wal_commits
        .iter()
        .filter(|(_, (_, lsn, _))| *lsn < p0)
        .map(|(ts, (_, lsn, _))| (*ts, *lsn))
        .collect::<Vec<_>>();
    let (written_ts, acked_ts, unparsed_written) =
        derive_written_acked(e4_events, &wal_commits, p0, &wal_bindings);
    // X5-R34 (d) diagnostic (`E4_D_DIAG`, default off, stderr only,
    // never a digest input): `D`, the reason its walk stopped, `Rc`, and
    // each SyncOn commit's verdict — the (h) 1 table's inputs. `D` and
    // `Rc` are computed regardless: the (f) cross-check below reads
    // them.
    let (d_val, d_stop, d_rc) = x5r34_durable_prefix(e4_events, &wal_bindings, p0);
    if std::env::var("E4_D_DIAG").is_ok() {
        eprintln!(
            "D diag: seed={seed} round={round} class={:?} p0={p0} D={d_val} stop={d_stop} rc={:?}",
            faults.class, d_rc
        );
        for (ts, (sync_on, lsn, len)) in wal_commits.iter() {
            let name = x5r34_seg_name(*lsn - (*lsn % crate::store::WAL_SEG));
            eprintln!(
                "D diag: ts={ts} sync_on={sync_on} lsn={lsn} len={len} end={} D={d_val} in_rc={} acked={}",
                lsn + len,
                d_rc.contains(&name),
                (!sync_on && written_ts.contains(ts))
                    || (*sync_on && (lsn + len <= d_val || d_rc.contains(&name)))
            );
        }
    }
    // **X5-R34 (f): the judged cross-check (ACK-OVER-HOLE).** Every
    // SyncOn commit whose own §12.4 ack wait returned before the crash
    // (`client_acked`) must have its record at or below `D` (or live in
    // a recycled segment): a client-observed ack above the derived
    // durable end is a store failure of the round, through the same
    // path as X5-R10's guard. A commit acked by (e) but **absent** from
    // `client_acked` is not a failure (its committer crashed in its
    // wait; the record is durable and recovery must replay it).
    let ack_over = x5r34_ack_over(&client_acked, &wal_commits, d_val, &d_rc);
    // X-5c-F3-F1 step 1 (e), the cross-check (`E4_ACK_DIAG`, stderr
    // only, never a digest input): every commit's derived `acked` flag
    // against the client-observed ack (did this commit's own
    // `wal_wait_durable`/`wal_wait_written` return before the crash?).
    // Agreement validates the derivation that feeds `lo`; a divergence
    // is recorded and ESCALATEs to P — the worker never changes
    // `derive_written_acked` to make one side match the other.
    if std::env::var("E4_ACK_DIAG").is_ok() && obs.crash_fired {
        // (`obs.crash_fired`: a discarded NEVER-FIRED retry attempt —
        // whose whole workload ran to completion and whose segments may
        // have been recycled — never reaches the judge; its comparison
        // would be noise, so only the fired, judged shape prints.)
        let derived: BTreeSet<u64> = acked_ts.iter().copied().collect();
        let n = wal_commits.len();
        let both = derived.intersection(&client_acked).count();
        let derived_only: Vec<u64> = derived.difference(&client_acked).copied().collect();
        let client_only: Vec<u64> = client_acked.difference(&derived).copied().collect();
        eprintln!(
            "ACK diag: seed={seed} round={round} class={:?} commits={n} agree={both} derived_only={derived_only:?} client_only={client_only:?}",
            faults.class
        );
        for ts in derived_only
            .iter()
            .take(3)
            .chain(client_only.iter().take(3))
        {
            let (sync_on, lsn, len) = wal_commits.get(ts).copied().unwrap_or((false, 0, 0));
            // The derivation's per-cause verdict for this ts, so the
            // record names the failing half (the walk never yielding
            // the record vs the returned/durable extent tests).
            let found = ack_record_present(e4_events, lsn);
            let why = if derived.contains(ts) {
                "derived acked: the write/sync events say durable, but the\
                 \ncommitter's own ack wait never returned (crashed in or\
                 \nbefore the wait while another task completed the batch)"
            } else if found {
                "record present: an extent test failed"
            } else {
                "the walk never yielded the record"
            };
            eprintln!(
                "ACK diag: ts={ts} lsn={lsn} len={len} sync_on={sync_on} derived_acked={} client_acked={} ({why})",
                derived.contains(ts),
                client_acked.contains(ts)
            );
            // The events over the record's own segment (the escalation
            // record's evidence: issue/return/sync order, and any later
            // write into the same file at the same offsets — the
            // recycle's latest life masking the original). X5-R35 (a):
            // the same workload-pass slice `E` the derivation read.
            let seg = crate::store::WAL_SEG;
            let mut ev: Vec<String> = Vec::new();
            for e in e4_events.iter() {
                match e {
                    DiskEvent::WriteIssued { inode, off, bytes } if *off < seg => {
                        ev.push(format!("W+ f{inode}@{off}+{}", bytes.len()));
                    }
                    DiskEvent::WriteReturned { inode, off, len } if *off < seg => {
                        ev.push(format!("W= f{inode}@{off}+{len}"));
                    }
                    DiskEvent::SyncIssued { inode, all } => {
                        ev.push(format!("S+ f{inode} all={all}"));
                    }
                    DiskEvent::SyncReturned { inode, all } => {
                        ev.push(format!("S= f{inode} all={all}"));
                    }
                    _ => {}
                }
            }
            let ev = if ev.len() > 24 {
                let n = ev.len();
                Some(format!(" …{} more… ", n - 20)).map(|m| ev[..20].join(" ") + &m)
            } else {
                Some(ev.join(" "))
            };
            eprintln!("ACK diag:   events[seg0]: {}", ev.unwrap_or_default());
        }
    }
    for ts in written_ts.iter() {
        model.mark_written(*ts);
    }
    for ts in acked_ts.iter() {
        model.mark_acked(*ts);
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
    // X5-R34 (b) 2's guard: a record below `p0` is a harness defect.
    if !p0_guard.is_empty() && task_error.is_none() {
        task_error = Some(format!(
            "X5-R34 p0 guard: wal_commits record below p0={p0}: {p0_guard:?}"
        ));
    }
    // X5-R34 (f): the judged cross-check — a client-observed SyncOn ack
    // above `D` is a store failure of the round (the pass fold fails it
    // through the task error, exactly as X5-R10's guard does).
    if !ack_over.is_empty() && task_error.is_none() {
        let ts = ack_over[0];
        let (lsn, len) = match wal_commits.get(&ts) {
            Some((_, l, n)) => (*l, *n),
            None => (0, 0),
        };
        task_error = Some(x5r34_ack_over_text(ts, lsn, len, d_val));
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
    let pf0 = disk.stat(crate::sim::PAGE_FILE).unwrap_or(0);
    // X5-R32 (a) 1: the prior rounds' applied page faults fold into the
    // target set beside this round's own (`disk.media[..media_base]` —
    // the disk's own applied-fault list, which the real chain's disk
    // carries from round to round).
    let media_targets = if round_media.is_empty() && disk.media.is_empty() {
        FaultTargets::default()
    } else {
        FaultTargets::collect(&round_media, pf0)
            .with_prior(disk.media.get(..media_base).unwrap_or(&[]), pf0)
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
    // X5-R20 diagnostic (`E4_MPE_DIAG`, default off, stderr only, never
    // a digest input): the round's fault list, the amcheck (i) findings
    // and the sets O-3 (e) judges, plus the durable-write history of
    // every slot a finding names or taint covers (the meta writes named
    // by their ckpt_id, so a slot's last write can be placed against
    // the graduation anchor).
    if std::env::var("E4_MPE_DIAG").is_ok() {
        eprintln!(
            "MPE diag: === seed {seed} round {round} class {:?} crash {:?} ===",
            faults.class, script.crash
        );
        for (i, f) in round_media.iter().enumerate() {
            eprintln!(
                "MPE diag: media[{i}] {:?} inode {} off {} len {} (page slot {})",
                f.kind,
                f.inode,
                f.off,
                f.len,
                f.off / 4096
            );
        }
        eprintln!(
            "MPE diag: amcheck_i findings {:?} image_slots {:?} tainted {:?} meta_k {:?}",
            amcheck_i,
            image_slots,
            tainted_slots,
            meta_i.as_ref().map(|m| m.ckpt_id)
        );
        let mut interesting: BTreeSet<u64> = tainted_slots.clone();
        interesting.extend(amcheck_i.iter().filter_map(|f| f.slot));
        let dw = disk.dwrites_of(pf);
        for slot in interesting.iter() {
            for (i, w) in dw.iter().enumerate() {
                if w.inode == pf
                    && w.off < (slot + 1) * 4096
                    && w.off + w.bytes.len() as u64 > slot * 4096
                {
                    let k = crate::store::page::parse_meta(&w.bytes)
                        .ok()
                        .map(|m| format!(" meta k={}", m.ckpt_id))
                        .unwrap_or_default();
                    eprintln!(
                        "MPE diag: slot {slot} dw[{i}] off {} len {} all={} pre_len {}{k}",
                        w.off,
                        w.bytes.len(),
                        w.all,
                        w.pre.len()
                    );
                }
            }
            eprintln!(
                "MPE diag: slot {slot} range_tainted={}",
                disk.range_tainted(pf, slot * 4096, 4096)
            );
        }
        // The event-stream ordering R5's condition 3 keys on: every
        // meta write, every completed sync_all, and every issued write
        // to the interesting slots, by event index (the dwrites list
        // only holds sync-covered writes; "issued" lives here).
        for (i, e) in events.iter().enumerate() {
            let note = match e {
                DiskEvent::WriteIssued { inode, off, bytes }
                    if *inode == pf && (*off == 4096 || *off == 8192) =>
                {
                    match crate::store::page::parse_meta(bytes) {
                        Ok(m) => format!("meta k={} write", m.ckpt_id),
                        Err(_) => format!("non-meta write at slot {}", off / 4096),
                    }
                }
                DiskEvent::SyncReturned { inode, all: true } if *inode == pf => {
                    "sync_all returned".to_owned()
                }
                _ => continue,
            };
            eprintln!("MPE diag: ev[{i}] {note}");
        }
        for slot in interesting.iter() {
            for (i, e) in events.iter().enumerate() {
                if let DiskEvent::WriteIssued { inode, off, bytes } = e {
                    if *inode == pf
                        && *off < (slot + 1) * 4096
                        && *off + bytes.len() as u64 > slot * 4096
                    {
                        eprintln!(
                            "MPE diag: ev[{i}] issued write slot {slot} id {} epoch {}",
                            crate::store::page::header_id(bytes),
                            crate::store::page::header_epoch(bytes),
                        );
                    }
                }
            }
        }
    }
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
    // X-5c-F3-F3b step 2 (c): set by the recovery loop's final Err arm
    // (a typed match); read into the outcome below.
    #[allow(unused_assignments)]
    let mut cover_err_outcome = false;
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
    // X-5c-F3-F1 step 6 (X5-R30 (c)): every recovery life's uncovered
    // accepts — an `skx`/`smx` condition (never an oracle input).
    let mut recovery_uncovered: Vec<(usize, u64, u64)> = Vec::new();
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
                {
                    let g = world.lock();
                    recovery_uncovered.extend(g.disk.uncovered_accepts.iter().copied());
                }
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
                // **X-5c-F3-F3b step 2 (c):** the round field is a typed
                // match on I-WAL-COVER's refusal, never a text match.
                cover_err_outcome = matches!(e, Fail::WalLoss { .. });
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
                // **X-5c-F3-F1 step 4 (nv3-X-5c-F3-F2 §1): the final
                // `Err` arm extends the caller's twin collector exactly
                // as the `Ok` arm does.** The draft set `recovery_reads`
                // but never extended the collector, so a pre-supplied
                // twin whose recovery failed handed its round no reads
                // at all (3509 r0, 4699 r0, 8399 r1: the real round's
                // O-3 (c) then judged an empty set against its own
                // faults). The reads are the real round's own — this
                // life's — never a store memory or a §3.3 verdict.
                if let Some(t) = twin.as_mut() {
                    t.extend(recovery_reads.iter().copied());
                }
                recovery_served.extend(g.disk.served.clone());
                recovery_uncovered.extend(g.disk.uncovered_accepts.iter().copied());
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
                &events,
            );
            (rb, r)
        }
        None => (false, BTreeSet::new()),
    };
    // **X5-R28 (a): the kept set `K`**, computed beside R with the same
    // anchor. Non-empty only while X5-R5's conditions 1 and 2 hold (a
    // media fault of this round damaged meta slot 1 or 2 — which
    // classes C and MW never meet — and the opened meta is a rollback);
    // then a tainted, referenced, reissued-but-not-landed slot whose
    // durable bytes are the opened image's own content is kept out of
    // the completeness half's expectation (the oracle amendment, X5-R28
    // (b)). Pure SimDisk history; never store memory or §3.3 output.
    let kept_slots = match &meta_i {
        Some(opened) => kept_set_k(
            &disk,
            pf,
            opened.ckpt_id,
            &image_slots,
            &tainted_slots,
            &noinplace_hits,
            &events,
            meta_rollback && media_targets.meta_slot_fault,
        ),
        None => BTreeSet::new(),
    };
    // X5-R28 (e) evidence (`E4_MPE_DIAG`, stderr only, never a digest
    // input): each kept slot's durable bytes — which condition 2 holds
    // equal to `V_k(s)` — as a CRC, beside the round's own record.
    if std::env::var("E4_MPE_DIAG").is_ok() {
        for slot in kept_slots.iter() {
            let b = disk.durable_read(pf, slot * 4096, 4096);
            eprintln!(
                "MPE diag: K slot {slot} durable==V_k crc32c={:08x} len={}",
                crate::crc::crc32c(0, &b),
                b.len()
            );
        }
    }
    // X-5c-F2 step 1 (e): the anchor-0 fallback's precondition. When
    // the events half's anchor is the fallback 0 (the whole stream read
    // as post-graduation) and this life's stream holds a page-file
    // `sync_data` return, the round flags the tripwire — the driver
    // records the seed-level defect; R is never silently widened. (This
    // store only ever `sync_all`s the page file, so on a real campaign
    // the flag cannot rise; it exists so the unsound reading can never
    // pass silently.)
    let anchor0_broke = matches!(&meta_i, Some(opened)
        if matches!(r5_anchor(&disk, pf, opened.ckpt_id, &events), Some((_, 0)))
            && r5_anchor0_defect(&events, pf).is_some());

    // ---- X5-R33: the round's supply (SimDisk history and the model's
    // record positions only; wal_ckpt_end/ts_ckpt_hi are read from the
    // meta bytes on SimDisk, never from store memory) ----
    let x33_damaged = x33_damaged_ranges(&disk, &round_media, pf0);
    let x33_a = x33_a_of(&acked_ts, &wal_commits, &x33_damaged);
    let x33_recovered_frontier = match &recovery {
        Ok((_, f, _)) => Some(*f),
        _ => None,
    };
    let mut x33_cross: Vec<String> = Vec::new();
    if let Some(in33) = x33 {
        // The round's own life (the driver appends it after this round).
        let cur = X33Life {
            p0,
            records: wal_commits
                .iter()
                .map(|(ts, &(_, lsn, len))| (lsn, len, *ts))
                .collect(),
            boot: start_store.boot_id,
            started_frontier: in33.started_frontier,
            append_end,
        };
        let mut lives: Vec<X33Life> = in33.lives.to_vec();
        lives.push(cur);
        // The cross-check over every meta the store wrote on this
        // round's disk (every meta-slot DWrite whose bytes parse).
        let dw = disk.dwrites_of(pf);
        let mut metas: Vec<(u32, u64, u64, u64, u64)> = Vec::new();
        for w in dw.iter() {
            if w.off != 4096 && w.off != 8192 {
                continue;
            }
            if let Ok(m) = crate::store::page::parse_meta(&w.bytes) {
                metas.push((m.boot_id, m.wal_ckpt_end, m.ts_ckpt_hi, m.t, m.ckpt_id));
            }
        }
        // One check per distinct meta write (same bytes, same verdict).
        metas.dedup();
        let mut seen = std::collections::BTreeSet::new();
        for (boot, w, t_hi, t, ckpt_id) in metas.iter() {
            if !seen.insert((*boot, *w, *t_hi, *t)) {
                continue;
            }
            // The writing life by boot id: this round's workload metas
            // (the current boot), meta 0 (the create session's virtual
            // life), a past life's metas, or — the Ok arm's harness
            // checkpoint — the recovered boot, whose "life" begins at
            // this meta's own wal_ckpt_end with no records and the
            // recovered frontier (the same truncation point the next
            // round starts from).
            let writing: X33Life;
            let widx: Option<usize>;
            // Round 0's workload runs on the created store (boot 0), so
            // the current-boot match comes first; a boot-0 meta that is
            // not the current life's is the create session's own (meta
            // 0, whose w = 0 passes every bound under either life).
            if *boot == start_store.boot_id {
                widx = Some(lives.len() - 1);
                writing = lives.last().cloned().unwrap_or_default();
            } else if let Some(i) = lives.iter().position(|l| l.boot == *boot) {
                widx = Some(i);
                writing = lives[i].clone();
            } else if *boot == 0 {
                widx = None;
                writing = X33Life {
                    p0: 0,
                    records: Vec::new(),
                    boot: 0,
                    started_frontier: 0,
                    append_end: 0,
                };
            } else {
                // The recovered boot's harness checkpoint (the Ok arm's
                // only other boot; unknown boots on faulted metas are
                // skipped — their bytes were judged by open). Its life
                // begins at this meta's own wal_ckpt_end with no
                // records, and the frontier this round's recovery found.
                widx = None;
                writing = X33Life {
                    p0: *w,
                    records: Vec::new(),
                    boot: *boot,
                    started_frontier: x33_recovered_frontier.unwrap_or(*w),
                    append_end: *w,
                };
            }
            let start_log = widx.map(|i| x33_start_log(&lives, i)).unwrap_or_default();
            if let Some(text) = x33_cross_check_meta(*ckpt_id, *w, *t_hi, *t, &writing, start_log) {
                x33_cross.push(text);
            }
        }
    }

    // ---- the extraction and the oracle ----
    let mut sample_rng = Rng::new(derive(seed * 7 + round as u64, S_SAMPLE));
    // X5-R30 (a): served_k, set by the arms that judge the round (the
    // Ok and Err arms; the livelock arm fails the fold and leaves it
    // false). The initializer only satisfies the borrow checker, like
    // `recovery_reads` above.
    #[allow(unused_assignments)]
    let mut served_k_flag = false;
    // X-5c-F3-F1 step 6 (X5-R30 (c)): the round's `skx`/`smx` predicate
    // flags and the no-taint re-verdict (diagnostics, never oracle
    // inputs). Every exit of the match below assigns them before they
    // are read (both verdict arms compute them; the livelock arm
    // default-initializes); the initializers only satisfy the borrow
    // checker.
    #[allow(unused_assignments)]
    let mut skx_flag = false;
    #[allow(unused_assignments)]
    let mut smx_flag = false;
    #[allow(unused_assignments)]
    let mut pass_na_flag: Option<bool> = None;
    // X5-R33 (c): the round's mw_h / wal_loss_explained, set by both
    // verdict arms (the record's diagnostics).
    #[allow(unused_assignments)]
    let mut x33_h_of_outcome = 0u64;
    #[allow(unused_assignments)]
    let mut x33_explained_of_outcome = false;
    let mut outcome = RoundOutcome {
        class: faults.class,
        deadlock: flags.0,
        baton: flags.1,
        panicked: flags.2,
        livelock: flags.4,
        livelock_site: flags.5,
        livelock_parked: flags.6,
        task_error,
        r5_anchor0_defect: anchor0_broke,
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
            // X5-R33 (d): a cross-check mismatch is a store failure.
            if !x33_cross.is_empty() && outcome.task_error.is_none() {
                outcome.task_error = Some(x33_cross[0].clone());
            }
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
            // X-5c-F3-F1 step 7 diagnostic (`E4_CXX_DIAG`, stderr only,
            // never a digest input): the store's own amcheck right
            // after recovery (before any post-recovery write), so a
            // violation already present in the recovered tree is
            // separated from one the harness checkpoint produces.
            if std::env::var("E4_CXX_DIAG").is_ok() {
                let f = amcheck::amcheck(&cx, &cfg);
                eprintln!(
                    "CXX diag: seed={seed} round={round} post-recovery findings={:?}",
                    f
                );
            }
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
                kept_slots: kept_slots.clone(),
                // X5-R32 (a) 2: the real round's own recovery reads and
                // the page-file inode, set before the verdict's
                // `check_round` (never the twin's).
                recovery_reads: recovery_reads.clone(),
                page_file: pf,
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
            //
            // X5-R30 (a): the round's served ranges are collected in
            // full (workload pass, recovery pass(es), this world's own
            // accepts) — `ex.served` is their non-emptiness (with
            // R-taint), and `served_k` (a diagnostic in the record,
            // never an oracle input) is true iff every served range
            // lies wholly inside slots of `K` (vacuously true when
            // nothing was served).
            {
                let g = world.lock();
                let r_taint = !faults.no_r_taint && r_taint_of(&g.disk.events, pf, &rollback_slots);
                let mut served_ranges: Vec<(usize, u64, u64)> = workload_served.clone();
                served_ranges.extend(recovery_served.iter().copied());
                served_ranges.extend(g.disk.served.iter().copied());
                ex.served = !served_ranges.is_empty() || r_taint;
                served_k_flag = served_ranges.iter().all(|&(f, off, len)| {
                    if f != pf || len == 0 {
                        return false;
                    }
                    let s0 = off / 4096;
                    let s1 = (off + len - 1) / 4096;
                    (s0..=s1).all(|s| kept_slots.contains(&s))
                });
                // X-5c-F3-F1 step 6 (X5-R30 (c)): the `skx`/`smx`
                // predicates — diagnostics, never oracle inputs. The
                // byte check runs against the extraction disk (every
                // recovery life's durable-write history is cloned
                // forward into it).
                let uncovered =
                    !recovery_uncovered.is_empty() || !g.disk.uncovered_accepts.is_empty();
                let slot = meta_i
                    .as_ref()
                    .and_then(|m| opened_meta_slot_of(&disk, pf, m));
                let (a, b) = r30_predicates(
                    faults.class,
                    &served_ranges,
                    workload_served.is_empty(),
                    r_taint,
                    uncovered,
                    media_targets.meta_slot_fault,
                    meta_rollback,
                    &kept_slots,
                    slot,
                    &g.disk,
                    pf,
                );
                skx_flag = a;
                smx_flag = b;
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
            // **X5-R33 (c)/(e): the round's mw_h / wal_loss_explained**
            // — read from the opened meta's bytes on SimDisk (never
            // store memory), attributed to the writing life by boot id.
            if let Some(in33) = x33 {
                let mut lives: Vec<X33Life> = in33.lives.to_vec();
                lives.push(X33Life {
                    p0,
                    records: wal_commits
                        .iter()
                        .map(|(ts, &(_, lsn, len))| (lsn, len, *ts))
                        .collect(),
                    boot: start_store.boot_id,
                    started_frontier: in33.started_frontier,
                    append_end,
                });
                if let Some(om) = &opened_meta {
                    ex.mw_h = match lives.iter().position(|l| l.boot == om.boot_id) {
                        Some(i) => x33_h_of(&lives, i, om.wal_ckpt_end),
                        None => {
                            // The harness checkpoint's own boot: no
                            // records of its life exist yet; the stream
                            // below its wal_ckpt_end is the start log of
                            // the life that begins there (the recovered
                            // frontier's life — the next round's).
                            let start = lives.len();
                            let mut with_next = lives.clone();
                            with_next.push(X33Life {
                                p0: om.wal_ckpt_end,
                                records: Vec::new(),
                                boot: om.boot_id,
                                started_frontier: frontier,
                                append_end: om.wal_ckpt_end,
                            });
                            x33_h_of(&with_next, start, om.wal_ckpt_end)
                        }
                    };
                    ex.wal_loss_explained =
                        x33_explained(&x33_damaged, om.wal_start_lsn, om.wal_ckpt_end);
                }
                ex.mw_a = x33_a;
                x33_h_of_outcome = ex.mw_h;
                x33_explained_of_outcome = ex.wal_loss_explained;
            }
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
            // X-5c-F3-F3a step 6 (d) lead 3 (`E4_O1_DIAG`, stderr only,
            // default off, never a digest input): at the failing O-1
            // candidate, the per-key diff — the first divergent key per
            // published tree, its owning model commit, and whether that
            // commit's record was durable (record end ≤ D, or a
            // recycled segment), replayed (ts ≤ the frontier), or
            // skipped as ≤ T (ts ≤ the opened meta's T).
            if std::env::var("E4_O1_DIAG").is_ok()
                && let Err(text) = &first.o1
            {
                if let Some(c) = text
                    .split("c=")
                    .nth(1)
                    .and_then(|rest| rest.split([',', ' ']).next())
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    for t in model
                        .published_at(c)
                        .iter()
                        .filter(|t| **t != CATALOG)
                        .copied()
                        .collect::<Vec<_>>()
                    {
                        let (Ok(got), want) = (reader.scan(t, c), model.scan(t, c)) else {
                            continue;
                        };
                        let mut keys: BTreeSet<Vec<u8>> = got.keys().cloned().collect();
                        keys.extend(want.keys().cloned());
                        for k in keys.iter() {
                            let g = crate::oracle::live(&got)
                                .iter()
                                .find(|(gk, _)| *gk == k)
                                .map(|(_, v)| (*v).clone());
                            let w = crate::oracle::live(&want)
                                .iter()
                                .find(|(wk, _)| *wk == k)
                                .map(|(_, v)| (*v).clone());
                            if g == w {
                                continue;
                            }
                            let owner = model
                                .commits
                                .iter()
                                .rev()
                                .find(|(ts, rec)| {
                                    **ts <= c
                                        && rec.ops.iter().any(|op| matches!(op, Op::Put { tree, key, .. } | Op::Delete { tree, key } if *tree == t && key == k))
                                })
                                .map(|(ts, _)| *ts);
                            let flags = owner.map(|ts| {
                                let rec = wal_commits.get(&ts);
                                let mrec = model.commits.get(&ts);
                                let durable = rec
                                    .map(|&(sync_on, lsn, len)| {
                                        lsn + len <= d_val
                                            || d_rc.contains(&x5r34_seg_name(
                                                lsn - (lsn % crate::store::WAL_SEG),
                                            ))
                                            || (!sync_on
                                                && mrec.map(|m| m.written).unwrap_or(false))
                                    })
                                    .unwrap_or(false);
                                format!(
                                    "owner_ts={ts} durable={durable} replayed={} le_T={}",
                                    ts <= ex.frontier,
                                    ts <= ex.opened_t
                                )
                            });
                            eprintln!(
                                "O1 diag: seed={seed} round={round} c={c} tree={t} key={k:?} store={g:?} model={w:?} {flags:?}"
                            );
                            break;
                        }
                        // The (c) shape: which raw entries sit above the
                        // failing cut, and their owning model commit.
                        if let Ok(raw) = reader.raw(t) {
                            for e in raw.iter().filter(|e| e.ts > c) {
                                let owner = model.commits.get(&e.ts);
                                eprintln!(
                                    "O1 diag: seed={seed} round={round} c={c} tree={t} raw-above: key={:?} ts={} owner_in_model={} owner_mode={:?}",
                                    e.key,
                                    e.ts,
                                    owner.is_some(),
                                    owner.map(|o| o.mode),
                                );
                            }
                        }
                    }
                }
            }
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
            // X-5c-F3-F1 step 7 diagnostic (`E4_CXX_DIAG`, stderr only,
            // never a digest input): the store's own amcheck right
            // after the harness checkpoint (the (ii) point), so the
            // first image that violates a tree-structure invariant can
            // be named per round.
            if std::env::var("E4_CXX_DIAG").is_ok() {
                let f = amcheck::amcheck(&cx, &cfg);
                eprintln!(
                    "CXX diag: seed={seed} round={round} post-harness-ckpt findings={:?}",
                    f
                );
            }
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
            if std::env::var("E4_MPE_DIAG").is_ok() {
                eprintln!(
                    "MPE diag: amcheck(ii) findings {:?} (store amcheck on the end disk)",
                    findings_ii
                );
            }
            let (end_disk2, end_store) = {
                let g = world.lock();
                (g.disk.clone(), g.st.clone())
            };
            // X5-R29 (a): the subtree-fallout reading's evidence, on
            // this round's end disk (stderr only, never a digest
            // input).
            if std::env::var("E4_MPE_DIAG").is_ok() {
                r29_subtree_diag(&end_disk2, &cfg, &findings_ii);
            }
            // The (ii) image's referenced and tainted slots, from the
            // durable bytes (the same walk as the (i) point).
            let (ii_findings, image_slots_ii, meta_ii) = durable_image(&end_disk2, &cfg);
            let _ = ii_findings;
            let tainted_slots_ii: BTreeSet<u64> = image_slots_ii
                .iter()
                .copied()
                .filter(|s| end_disk2.range_tainted(pf, s * 4096, 4096))
                .collect();
            // **X5-R28 (a): `K_ii`** — the kept set computed the same
            // way, against the end image's own meta (`durable_image`'s
            // third value here) and the (ii) taint. The events view is
            // the round's full issue history in order (the workload
            // life's captured stream, then the end disk's own events —
            // recovery, extraction and this checkpoint); the
            // M-NOINPLACE hits are recomputed over that combined
            // stream. The end meta is normally the newest image, so no
            // newer covered meta anchors the set and K_ii stays empty;
            // the computation exists so the exotic ends (a misplaced
            // newer meta among them) follow the same rule.
            let mut events_ii = events.clone();
            events_ii.extend(end_disk2.events.iter().cloned());
            let noinplace_hits_ii = crate::monitor::noinplace_slot_hits(&events_ii, &mcfg);
            let kept_slots_ii = match &meta_ii {
                Some(endm) => kept_set_k(
                    &end_disk2,
                    pf,
                    endm.ckpt_id,
                    &image_slots_ii,
                    &tainted_slots_ii,
                    &noinplace_hits_ii,
                    &events_ii,
                    meta_rollback_of(&end_disk2, pf, endm.ckpt_id) && media_targets.meta_slot_fault,
                ),
                None => BTreeSet::new(),
            };
            ex.amcheck_ii = findings_ii.clone();
            ex.amcheck_ii_ran = ck.is_ok();
            ex.image_slots_ii = image_slots_ii;
            ex.tainted_image_slots_ii = tainted_slots_ii;
            ex.kept_slots_ii = kept_slots_ii.clone();
            if std::env::var("E4_MPE_DIAG").is_ok() {
                eprintln!(
                    "MPE diag: kept K {:?} K_ii {:?} served_k={} (X5-R28/R30; stderr only)",
                    kept_slots, kept_slots_ii, served_k_flag
                );
            }
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
            // X-5c-F3-F1 step 6 (X5-R30 (c)): `pass_na` — the final
            // verdict re-evaluated with `no_taint` (a fresh, identically
            // seeded rng; a diagnostic, recorded only when
            // `served ∧ (skx ∨ smx)`, else null).
            if ex.served && (skx_flag || smx_flag) {
                let mut na_rng = Rng::new(derive(seed * 7 + round as u64, S_SAMPLE));
                let na = check_round(
                    &model,
                    faults.class,
                    c_prev,
                    &ex,
                    &mut reader,
                    &media_targets,
                    twin_ref,
                    &mut na_rng,
                    &OracleCfg {
                        no_taint: true,
                        ..Default::default()
                    },
                );
                pass_na_flag = Some(na.pass);
            }
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
            // X5-R32 (b): the damaged-referenced arm's diagnostic.
            outcome.r32_round = verdict.r32;
            // X5-R28 (a) / X5-R30 (a): the kept sets and served_k ride
            // the outcome (the record's and the diagnostic's inputs;
            // the oracle reads them from `Extracted`, wired in the
            // X5-R28 (b) commit).
            outcome.served_k = served_k_flag;
            outcome.skx = skx_flag;
            outcome.smx = smx_flag;
            outcome.pass_na = pass_na_flag;
            outcome.kept_slots = kept_slots.clone();
            outcome.kept_slots_ii = kept_slots_ii.clone();
            // `pass` folds every per-seed failure flag: a DEADLOCK, a
            // PANIC or an S-BATON violation is never a pass (card "Do
            // not"; the gate's minor finding: the draft ignored
            // `deadlock` and `panicked` here).
            // X5-R33 (d): a cross-check mismatch is a store failure.
            if !x33_cross.is_empty() && outcome.task_error.is_none() {
                outcome.task_error = Some(x33_cross[0].clone());
            }
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
            let mut served_ranges: Vec<(usize, u64, u64)> = workload_served.clone();
            served_ranges.extend(recovery_served.iter().copied());
            let r_taint_err =
                !faults.no_r_taint && r_taint_of(&recovery_events, pf, &rollback_slots);
            // X5-R30 (a): the same served_k diagnostic over every
            // served range of the round (no extraction world exists on
            // this arm).
            served_k_flag = served_ranges.iter().all(|&(f, off, len)| {
                if f != pf || len == 0 {
                    return false;
                }
                let s0 = off / 4096;
                let s1 = (off + len - 1) / 4096;
                (s0..=s1).all(|s| kept_slots.contains(&s))
            });
            // X-5c-F3-F1 step 6 (X5-R30 (c)): the `skx`/`smx`
            // predicates on the failed-recovery arm too (both false
            // unless `served` holds). The byte check runs against the
            // last recovery life's disk (its durable-write history
            // carries every earlier life's).
            {
                let slot = meta_i
                    .as_ref()
                    .and_then(|m| opened_meta_slot_of(&disk, pf, m));
                let (a, b) = r30_predicates(
                    faults.class,
                    &served_ranges,
                    workload_served.is_empty(),
                    r_taint_err,
                    !recovery_uncovered.is_empty(),
                    media_targets.meta_slot_fault,
                    meta_rollback,
                    &kept_slots,
                    slot,
                    &recovery_disk,
                    pf,
                );
                skx_flag = a;
                smx_flag = b;
            }
            let mut ex = Extracted {
                ok: false,
                // X5-R33: the recovery Err is I-WAL-COVER's refusal
                // (a typed match — the same predicate as cover_err).
                wal_loss: cover_err_outcome,
                xx001: vec![XxSite::Open],
                served: !served_ranges.is_empty() || r_taint_err,
                amcheck_i: amcheck_i.clone(),
                amcheck_i_ran: meta_i.is_some(),
                image_slots: image_slots.clone(),
                tainted_image_slots: tainted_slots.clone(),
                meta_rollback,
                rollback_slots: rollback_slots.clone(),
                kept_slots: kept_slots.clone(),
                // X5-R32 (a) 2: the failed recovery's own reads (the
                // life whose result the round judges), set before the
                // verdict's `check_round`.
                recovery_reads: recovery_reads.clone(),
                page_file: pf,
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
            // **X5-R33 (c)/(e) on the failed-recovery arm** — the opened
            // meta read from the last recovery attempt's disk (the meta
            // open tried to select), never store memory.
            if let Some(in33) = x33 {
                let mut lives: Vec<X33Life> = in33.lives.to_vec();
                lives.push(X33Life {
                    p0,
                    records: wal_commits
                        .iter()
                        .map(|(ts, &(_, lsn, len))| (lsn, len, *ts))
                        .collect(),
                    boot: start_store.boot_id,
                    started_frontier: in33.started_frontier,
                    append_end,
                });
                let (om, _unused) = read_opened_meta(&recovery_disk);
                if std::env::var("E4_X33_DIAG").is_ok() {
                    eprintln!(
                        "X33 diag: seed={seed} round={round} ERR arm: damaged={:?} om={:?}",
                        x33_damaged,
                        om.as_ref().map(|m| (
                            m.ckpt_id,
                            m.boot_id,
                            m.wal_start_lsn,
                            m.wal_ckpt_end,
                            m.t,
                            m.ts_ckpt_hi
                        ))
                    );
                }
                if let Some(om) = &om {
                    ex.mw_h = match lives.iter().position(|l| l.boot == om.boot_id) {
                        Some(i) => x33_h_of(&lives, i, om.wal_ckpt_end),
                        None => {
                            let start = lives.len();
                            let mut with_next = lives.clone();
                            with_next.push(X33Life {
                                p0: om.wal_ckpt_end,
                                records: Vec::new(),
                                boot: om.boot_id,
                                started_frontier: 0,
                                append_end: om.wal_ckpt_end,
                            });
                            x33_h_of(&with_next, start, om.wal_ckpt_end)
                        }
                    };
                    ex.wal_loss_explained =
                        x33_explained(&x33_damaged, om.wal_start_lsn, om.wal_ckpt_end);
                }
                ex.mw_a = x33_a;
                x33_h_of_outcome = ex.mw_h;
                x33_explained_of_outcome = ex.wal_loss_explained;
            }
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
            // X-5c-F3-F1 step 6 (X5-R30 (c)): `pass_na` on the
            // failed-recovery arm (a fresh, identically seeded rng).
            if ex.served && (skx_flag || smx_flag) {
                let mut na_rng = Rng::new(derive(seed * 7 + round as u64, S_SAMPLE));
                let na = check_round(
                    &model,
                    faults.class,
                    c_prev,
                    &ex,
                    &mut nop,
                    &media_targets,
                    if twin_on { Some(&twin_info) } else { None },
                    &mut na_rng,
                    &OracleCfg {
                        no_taint: true,
                        ..Default::default()
                    },
                );
                pass_na_flag = Some(na.pass);
            }
            outcome.o1 = verdict.o1.clone();
            outcome.o1f_ok = verdict.o1f_ok;
            outcome.o2_fail = verdict.o2_fail.clone();
            outcome.o3_fail = verdict.o3_fail.clone();
            outcome.served = ex.served;
            // X5-R32 (b): the damaged-referenced arm's diagnostic.
            outcome.r32_round = verdict.r32;
            // X5-R28 (a) / X5-R30 (a): the (i) kept set and served_k
            // (amcheck (ii) never ran; K_ii stays empty).
            outcome.served_k = served_k_flag;
            outcome.skx = skx_flag;
            outcome.smx = smx_flag;
            outcome.pass_na = pass_na_flag;
            outcome.kept_slots = kept_slots.clone();
            // X5-R33 (d): a cross-check mismatch is a store failure.
            if !x33_cross.is_empty() && outcome.task_error.is_none() {
                outcome.task_error = Some(x33_cross[0].clone());
            }
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
    // X5-R34 (f): the cross-check's record field (the task error above
    // already fails any non-empty list through the pass fold).
    outcome.ack_over = ack_over;
    // X-5c-F3-F3b step 2 (c): I-WAL-COVER's refusal on the recovery Err
    // arm (a typed match; the livelock arm leaves it false).
    outcome.cover_err = cover_err_outcome;
    // X5-R33 (a): the round's own life, for the driver to carry.
    outcome.x33_life = x33.map(|in33| X33Life {
        p0,
        records: wal_commits
            .iter()
            .map(|(ts, &(_, lsn, len))| (lsn, len, *ts))
            .collect(),
        boot: start_store.boot_id,
        started_frontier: in33.started_frontier,
        append_end,
    });
    // X5-R33 (c): the record's diagnostics.
    outcome.x33h = x33_h_of_outcome;
    outcome.x33a = x33_a;
    outcome.x33x = x33_explained_of_outcome;
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
/// X-5c-F3-F1 step 1 (e) diagnostic helper (`E4_ACK_DIAG` only): does
/// the issued-write history hold a parsable record at `lsn`'s
/// segment-relative position with its own lsn (the walk's `ts_pos`
/// precondition)? Hops record-to-record exactly as
/// [`derive_written_acked`] does, so its verdict matches the walk's.
/// Never a digest input; exists only to name the failing half of a
/// derived-vs-client ack disagreement.
fn ack_record_present(events: &[DiskEvent], lsn: u64) -> bool {
    let seg = crate::store::WAL_SEG;
    let mut files: BTreeMap<usize, BTreeMap<u64, Vec<u8>>> = BTreeMap::new();
    for e in events {
        if let DiskEvent::WriteIssued { inode, off, bytes } = e {
            files.entry(*inode).or_default().insert(*off, bytes.clone());
        }
    }
    for writes in files.values() {
        let mut off = writes.keys().copied().min().unwrap_or(0);
        loop {
            if off >= seg {
                break;
            }
            let whole = stitched(writes, off, (seg - off) as usize);
            if whole.len() >= 24 && whole.get(0..4) == Some(&0x4C41574Eu32.to_le_bytes()) {
                let len = {
                    let mut b = [0u8; 4];
                    if let Some(s) = whole.get(4..8) {
                        for (i, x) in s.iter().enumerate() {
                            b[i] = *x;
                        }
                    }
                    u32::from_le_bytes(b) as u64 & 0xFFFF_FFFF
                };
                let rec_lsn = {
                    let mut b = [0u8; 8];
                    if let Some(s) = whole.get(16..24) {
                        for (i, x) in s.iter().enumerate() {
                            b[i] = *x;
                        }
                    }
                    u64::from_le_bytes(b)
                };
                if rec_lsn == lsn && rec_lsn % seg == off && len >= 16 && off + len <= seg {
                    return true;
                }
                if len >= 16 && off + len <= seg {
                    off += len;
                    continue;
                }
            }
            match writes.range(off + 1..).next() {
                Some((&next, _)) if next < seg => off = next,
                _ => break,
            }
        }
    }
    false
}

/// The third return is the **loud** half of the gate's round-3 F1: every
/// commit whose record header is present in some WAL file's issued
/// writes at its segment-relative position with its own lsn, but whose
/// `len` runs past the segment end — bytes written, never parseable
/// (§12.2 never places such a record; only an over-cap enqueue ever
/// could, and the cap now refuses it). The harness fails the round on
/// any; dropping the entry silently let an acked commit vanish from the
/// model while recovery lost it too (the F1 demonstration).
///
/// **X5-R34 (a), binding:** the **SyncOn acked half is I-ACK-PREFIX's
/// `durable_end` derived from SimDisk history** (`x5r34_durable_prefix`,
/// carrying `p0` — the round's starting append position — and the
/// durable `/wal/` directory bindings read through
/// [`SimDisk::durable_dir_bindings`]). A SyncOn commit is acked iff
/// `lsn + len ≤ D`, or its segment name is in `Rc` (the recycled
/// exemption). Unchanged: `written` (the `hi` input), SyncOff's acked
/// (= written) and the `unparsed_written` report. The acked half never
/// reads store memory (`wal.durable`/`wal.written`) — only `E`, `p0`
/// and the durable bindings (X5-R34 (b), (i)).
pub fn derive_written_acked(
    events: &[DiskEvent],
    wal_commits: &BTreeMap<u64, (bool, u64, u64)>,
    p0: u64,
    wal_bindings: &BTreeMap<String, usize>,
) -> (Vec<u64>, Vec<u64>, Vec<u64>) {
    // inode → (off → latest issued bytes) and the returned extents (the
    // `written` half's inputs). X5-R34 removed the per-inode sync-extent
    // ratchet this acked half used; `D` comes from
    // [`x5r34_durable_prefix`] below.
    let mut files: BTreeMap<usize, FileWrites> = BTreeMap::new();
    let mut returned_end: BTreeMap<usize, u64> = BTreeMap::new();
    for e in events {
        match e {
            DiskEvent::WriteIssued { inode, off, bytes } => {
                files.entry(*inode).or_default().insert(*off, bytes.clone());
            }
            DiskEvent::WriteReturned { inode, off, len } => {
                let w = returned_end.entry(*inode).or_default();
                *w = (*w).max(off + len);
            }
            _ => {}
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
    // X5-R34 (d)/(e): the durable prefix `D` and the recycled set.
    let (d, _d_stop, rc) = x5r34_durable_prefix(events, wal_bindings, p0);
    for (ts, (sync_on, lsn, _len)) in wal_commits.iter() {
        // The `written` half — unchanged (the walk + the returned
        // extents; the `hi` input).
        let w_ok = match ts_pos.get(ts).copied() {
            Some((inode, end)) => {
                let w = returned_end.get(&inode).copied().unwrap_or(0);
                if w >= end {
                    written.push(*ts);
                    true
                } else {
                    false
                }
            }
            None => {
                // The walk never yielded this commit's record. If its
                // header sits issued at the segment-relative position
                // with its own lsn and a length past the segment end,
                // the bytes were written but can never parse — report
                // it (F1); the harness fails the round. A record simply
                // never written (or torn mid-body, a normal crash
                // outcome) has no full header or a length that fits,
                // and stays silent.
                if let Some(rec_len) = issued_header_len(&files, *lsn, seg)
                    && (*lsn % seg) + rec_len > seg
                {
                    unparsed_written.push(*ts);
                }
                false
            }
        };
        // The acked half — X5-R34 (e), independent of the walk: a
        // SyncOn commit is acked iff its record ends at or below `D`,
        // or its segment was recycled away this round (`Rc`); a SyncOff
        // commit is acked iff written (unchanged).
        let acked_now = if *sync_on {
            let name = x5r34_seg_name(*lsn - (*lsn % seg));
            lsn + _len <= d || rc.contains(&name)
        } else {
            w_ok
        };
        if acked_now {
            acked.push(*ts);
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

// ---- X5-R34: I-ACK-PREFIX's `durable_end` from SimDisk history ----

/// **X5-R35 (a) (the rework gate's P ruling, binding): X5-R34's `E` is
/// the workload pass's slice of the captured event stream.**
/// `run_round` captures `disk.events` at the end of the workload pass,
/// but the capture begins with the **previous** round's recovery and
/// harness boot checkpoint: `new_round` clears the list at the end of
/// the *crashed* round (before its recovery), so round r+1's capture
/// holds round r's recovery events — the §14 G2 truncation zero-write
/// and its sync among them — ahead of round r+1's own workload events.
/// X5-R34 (b) 1's premise ("this round's events only, because
/// `new_round` cleared the list") is true only of this slice, so `E` is
/// `captured[pre_workload..]`, where `pre_workload` is the event count
/// the round's `start_disk` carried (the recovery and boot checkpoint
/// that precede the workload). Everything else in the capture keeps its
/// existing readers (the Q12 rename fold, the monitors, X5-R5's R and
/// X5-R28's `K_ii`): the ruling changes X5-R34's `E` only — "`Rc` and β
/// are read over this `E`", and (b) 1–5, (c)–(f), (i) are otherwise
/// unchanged. Without the slice, a truncation zero-write over an
/// enqueued, never-written SyncOn record counts as (W)'s "last covering
/// write" and `D` runs past commits whose bytes never reached disk (the
/// 17 false `frontier < lo` losses of the first run, 403 r1's D=65536
/// with no WAL write at all among them).
pub fn x5r34_workload_events(captured: &[DiskEvent], pre_workload: usize) -> &[DiskEvent] {
    captured.get(pre_workload..).unwrap_or(&[])
}

/// The segment file name holding LSN `x` (§12.2's `<16 hex>.log` inside
/// `"/wal/"`; X5-R34 (b) defines `name(x)`).
pub fn x5r34_seg_name(seg: u64) -> String {
    format!("{seg:016x}.log")
}

/// **X5-R34 (b): the per-round view of the workload pass's event
/// stream** `E`, built in one pass. Everything the durable-prefix
/// derivation needs and nothing else:
/// - per-inode issued writes, in event order (`writes`), their distinct
///   start offsets (`write_starts`, the SegmentEnd candidates of (c)'s
///   slack rule), and the per-inode **last-issued byte map**
///   (`last_issued`, maintained with the same split-neighbours
///   discipline as `Inode::record_issued`, but from `E` alone);
/// - the per-inode last `SyncReturned` index (`last_sync`);
/// - `β(n)` per segment name (the last `create`/rename-into-`n` dir op
///   preceding the last `sync_dir("/wal/")` return) and the recycled
///   set `Rc` (every rename **source**).
///
/// Determinism: `BTreeMap`/`BTreeSet` only; no wall clock, no store
/// memory, no taint map (X5-R34 (i)).
struct X5r34View {
    /// inode → `(event index, off, end)` of each issued write, in event
    /// order.
    writes: BTreeMap<usize, Vec<(usize, u64, u64)>>,
    /// inode → its distinct write start offsets (record candidates).
    write_starts: BTreeMap<usize, BTreeSet<u64>>,
    /// inode → its last `SyncReturned` event index (`sync_data` and
    /// `sync_all` alike — (W) does not distinguish).
    last_sync: BTreeMap<usize, usize>,
    /// inode → the last-issued bytes as an interval map (start → bytes).
    last_issued: BTreeMap<usize, BTreeMap<u64, Vec<u8>>>,
    /// `β(n)`: segment name → the last binding dir op's event index
    /// before the last `sync_dir("/wal/")` return (absent = the binding
    /// predates the round, or none ever became durable).
    beta: BTreeMap<String, usize>,
    /// `Rc`: segment names renamed away this round (the (R) exemption).
    rc: BTreeSet<String>,
}

impl X5r34View {
    /// One pass over `E` (X5-R34 (b) 1: index order is time order, and
    /// the media faults applied after `E` was captured never appear).
    fn build(events: &[DiskEvent]) -> Self {
        let mut v = X5r34View {
            writes: BTreeMap::new(),
            write_starts: BTreeMap::new(),
            last_sync: BTreeMap::new(),
            last_issued: BTreeMap::new(),
            beta: BTreeMap::new(),
            rc: BTreeSet::new(),
        };
        let mut binding_ops: Vec<(usize, String)> = Vec::new();
        let mut last_dirsync: Option<usize> = None;
        for (i, e) in events.iter().enumerate() {
            match e {
                DiskEvent::WriteIssued { inode, off, bytes } => {
                    v.writes
                        .entry(*inode)
                        .or_default()
                        .push((i, *off, *off + bytes.len() as u64));
                    v.write_starts.entry(*inode).or_default().insert(*off);
                    let m = v.last_issued.entry(*inode).or_default();
                    issued_insert(m, *off, bytes);
                }
                DiskEvent::SyncReturned { inode, .. } => {
                    v.last_sync.insert(*inode, i);
                }
                DiskEvent::SyncDirReturned { dir } if dir == crate::sim::WAL_DIR => {
                    last_dirsync = Some(i);
                }
                DiskEvent::DirOpIssued { dir, what } if dir == crate::sim::WAL_DIR => {
                    if let Some(n) = what
                        .strip_prefix("create ")
                        .and_then(|n| n.strip_prefix("/wal/"))
                    {
                        // `create /wal/<name>`
                        binding_ops.push((i, n.to_owned()));
                    } else if let Some(rest) = what.strip_prefix("rename ") {
                        // `rename /wal/<from> -> /wal/<to>`: the source is
                        // recycled away (Rc), the target newly bound.
                        if let Some((from, to)) = rest.split_once(" -> ") {
                            // Names are bare (`<16 hex>.log`), as the
                            // durable bindings key them.
                            let from = from.strip_prefix("/wal/").unwrap_or(from);
                            let to = to.strip_prefix("/wal/").unwrap_or(to);
                            v.rc.insert(from.to_owned());
                            binding_ops.push((i, to.to_owned()));
                        }
                    }
                }
                _ => {}
            }
        }
        // β(n): only ops the last `sync_dir("/wal/")` return covers can
        // have made a binding durable (X5-R34 (b) 4).
        if let Some(ds) = last_dirsync {
            for (idx, n) in binding_ops {
                if idx < ds {
                    let b = v.beta.entry(n).or_insert(idx);
                    if idx > *b {
                        *b = idx;
                    }
                }
            }
        }
        v
    }

    /// The durable byte ranges of one segment, as **file offsets**
    /// `[a, b)` within it (X5-R34 (c)): (R) the whole segment when its
    /// name was renamed away; (W) the ranges whose last issued write
    /// postdates the durable binding and a sync of the inode followed;
    /// (S) the slack after a `SegmentEnd` whose own bytes are (W)-
    /// durable and whose last-issued bytes decode as the filler at its
    /// own position.
    fn durable_ranges(
        &self,
        name: &str,
        bindings: &BTreeMap<String, usize>,
        seg: u64,
    ) -> Vec<(u64, u64)> {
        let wseg = crate::store::WAL_SEG;
        if self.rc.contains(name) {
            // (R) Recycled: an exemption, whole-segment.
            return vec![(0, wseg)];
        }
        let Some(&inode) = bindings.get(name) else {
            return Vec::new();
        };
        // (W): paint the writes in **reverse event order**, filling only
        // the not-yet-painted gaps — so the **last** write covering a
        // byte always decides it (an earlier write can never override a
        // later one).
        let mut cov: BTreeMap<u64, (u64, bool)> = BTreeMap::new();
        if let Some(ws) = self.writes.get(&inode) {
            let last_sync = self.last_sync.get(&inode).copied();
            for &(idx, off, end) in ws.iter().rev() {
                let after_binding = match self.beta.get(name) {
                    Some(&b) => idx > b,
                    None => true, // the binding predates the round
                };
                let dur = after_binding && matches!(last_sync, Some(s) if s > idx);
                for (a, b) in interval_gaps(&cov, off, end) {
                    interval_add(&mut cov, a, b, dur);
                }
            }
        }
        // (S): the slack after a SegmentEnd filler. Its own bytes must
        // be (W)-durable, and its **last-issued** bytes at its position
        // must decode as `K_SEGEND` carrying its own lsn — so a later
        // overwrite of the filler kills the slack.
        if let Some(starts) = self.write_starts.get(&inode)
            && let Some(issued) = self.last_issued.get(&inode)
        {
            for &off in starts.iter() {
                if off >= wseg {
                    continue;
                }
                let hdr = stitch_issued(issued, off, 24);
                if hdr.len() < 24 || !magic_ok(&hdr) {
                    continue;
                }
                let len = {
                    let mut b = [0u8; 4];
                    if let Some(s) = hdr.get(4..8) {
                        b.copy_from_slice(s);
                    }
                    u32::from_le_bytes(b) as u64
                };
                if len < 16 || off + len > wseg {
                    continue;
                }
                let q = seg + off;
                let whole = stitch_issued(issued, off, len as usize);
                if whole.len() < len as usize {
                    continue;
                }
                match crate::store::wal::dec_record(&whole, q, wseg) {
                    Ok(Some(r)) if r.kind == crate::store::wal::K_SEGEND && r.lsn == q => {
                        // The filler's own bytes must be (W)-durable.
                        if covered_by(&cov, off, off + len) {
                            interval_add(&mut cov, off + len, wseg, true);
                        }
                    }
                    _ => {}
                }
            }
        }
        // Normalise to the durable ranges only.
        cov.into_iter()
            .filter(|(_, (_, d))| *d)
            .map(|(a, (b, _))| (a, b))
            .collect()
    }

    /// Why the byte at file offset `off` of segment `name` is not
    /// durable — the diagnostic's stop reason (never a digest or
    /// sim-visible input).
    fn stop_reason(&self, name: &str, bindings: &BTreeMap<String, usize>, off: u64) -> String {
        if !bindings.contains_key(name) {
            return format!(
                "no durable directory binding for segment {name} \
                 (G1: created or renamed without a surviving sync_dir)"
            );
        }
        let inode = bindings[name];
        let last_covering = self.writes.get(&inode).and_then(|ws| {
            ws.iter()
                .rev()
                .find(|&&(_, a, b)| a <= off && off < b)
                .copied()
        });
        match last_covering {
            None => format!(
                "no issued write covers offset {off} on inode {inode}, and no SegmentEnd slack does"
            ),
            Some((idx, a, b)) => match self.beta.get(name) {
                Some(&bi) if idx <= bi => format!(
                    "the last covering write (ev {idx}, [{a},{b})) precedes the durable \
                     binding (ev {bi}) — stale bytes of an earlier segment name"
                ),
                _ => match self.last_sync.get(&inode) {
                    Some(&s) if s > idx => format!(
                        "the covering write (ev {idx}) is synced, but a later write \
                         re-dirtied part of its range"
                    ),
                    _ => {
                        format!("no sync of inode {inode} after the last covering write (ev {idx})")
                    }
                },
            },
        }
    }
}

/// The first LSN a segment file name encodes (`{seg:016x}.log`), or
/// `None` when the name is not a segment name.
fn seg_first_lsn_of(name: &str) -> Option<u64> {
    let hex = name.strip_suffix(".log")?;
    if hex.len() != 16 {
        return None;
    }
    u64::from_str_radix(hex, 16).ok()
}

/// Split-neighbour insert into a last-issued interval map (the same
/// discipline as `Inode::record_issued`, over `E` alone).
fn issued_insert(map: &mut BTreeMap<u64, Vec<u8>>, off: u64, bytes: &[u8]) {
    let end = off + bytes.len() as u64;
    let mut to_remove: Vec<u64> = Vec::new();
    let mut to_add: Vec<(u64, Vec<u8>)> = Vec::new();
    for (&fstart, fbytes) in map.range(..end) {
        let fend = fstart + fbytes.len() as u64;
        if fend <= off {
            continue;
        }
        if fstart < off
            && let Some(pre) = fbytes.get(..(off - fstart) as usize)
        {
            to_add.push((fstart, pre.to_vec()));
        }
        if fend > end
            && let Some(post) = fbytes.get((end - fstart) as usize..)
        {
            to_add.push((end, post.to_vec()));
        }
        to_remove.push(fstart);
    }
    for k in to_remove {
        map.remove(&k);
    }
    for (k, v) in to_add {
        map.insert(k, v);
    }
    map.insert(off, bytes.to_vec());
}

/// Stitch `len` bytes at `off` from a last-issued interval map.
fn stitch_issued(map: &BTreeMap<u64, Vec<u8>>, off: u64, len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    let mut cur = off;
    while out.len() < len {
        let Some((&start, bytes)) = map.range(..=cur).next_back() else {
            break;
        };
        if start + bytes.len() as u64 <= cur {
            break;
        }
        let skip = (cur - start) as usize;
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

/// The sub-ranges of `[a, b)` not covered by any interval in `cov`.
fn interval_gaps(cov: &BTreeMap<u64, (u64, bool)>, a: u64, b: u64) -> Vec<(u64, u64)> {
    let mut out = Vec::new();
    let mut cur = a;
    while cur < b {
        match cov.range(..=cur).next_back() {
            Some((&start, &(end, _))) if start <= cur && end > cur => {
                cur = end;
            }
            _ => {
                // Uncovered at `cur`: the gap runs to the next interval's
                // start, or to `b`.
                let next = cov
                    .range(cur..)
                    .next()
                    .map(|(&s, _)| s)
                    .filter(|s| *s > cur)
                    .unwrap_or(b)
                    .min(b);
                let stop = next.max(cur + 1);
                out.push((cur, stop));
                cur = stop;
            }
        }
    }
    out
}

/// Insert `[a, b)` with value `d` into an interval map (merging
/// adjacent equal values).
fn interval_add(cov: &mut BTreeMap<u64, (u64, bool)>, a: u64, b: u64, d: bool) {
    if a >= b {
        return;
    }
    // Merge with a same-value predecessor that touches `a`.
    let mut a = a;
    if let Some((&ps, &(pe, pd))) = cov.range(..a).next_back()
        && pd == d
        && pe >= a
    {
        a = ps;
        cov.remove(&ps);
    }
    let mut b = b;
    if let Some((&ns, &(ne, nd))) = cov.range(b..).next()
        && ns == b
        && nd == d
    {
        b = ne;
        cov.remove(&ns);
    }
    cov.insert(a, (b, d));
}

/// Is `[a, b)` wholly covered by `true` intervals?
fn covered_by(cov: &BTreeMap<u64, (u64, bool)>, a: u64, b: u64) -> bool {
    let mut cur = a;
    while cur < b {
        let Some((&start, &(end, d))) = cov.range(..=cur).next_back() else {
            return false;
        };
        if !d || start > cur || end <= cur {
            return false;
        }
        cur = end;
    }
    true
}

/// **X5-R34 (f): the judged cross-check's set.** Every SyncOn ts in
/// `client_acked` (the commits whose own §12.4 ack wait returned before
/// the crash) whose record ends above the derived durable end `D` and
/// whose segment is not in the recycled set `Rc` — the ACK-OVER-HOLE
/// store failures. A commit acked by (e) but absent from
/// `client_acked` is not here and is not a failure.
pub fn x5r34_ack_over(
    client_acked: &BTreeSet<u64>,
    wal_commits: &BTreeMap<u64, (bool, u64, u64)>,
    d: u64,
    rc: &BTreeSet<String>,
) -> Vec<u64> {
    let wseg = crate::store::WAL_SEG;
    client_acked
        .iter()
        .filter(|ts| match wal_commits.get(*ts) {
            Some((true, lsn, len)) => {
                let name = x5r34_seg_name(*lsn - (*lsn % wseg));
                lsn + len > d && !rc.contains(&name)
            }
            _ => false,
        })
        .copied()
        .collect()
}

/// The ACK-OVER-HOLE failure text (X5-R34 (f), verbatim).
pub fn x5r34_ack_over_text(ts: u64, lsn: u64, len: u64, d: u64) -> String {
    format!(
        "ACK-OVER-HOLE: client-acked SyncOn ts={ts} ends at {} above durable_end {d} (I-ACK-PREFIX, X5-R34)",
        lsn + len
    )
}

/// **X5-R34 (d): the durable prefix `D`.** The largest `L ≥ p0` such
/// that every byte in `[p0, L)` is durable (X5-R34 (c)), scanning in
/// LSN order: a segment boundary is crossed like any other position,
/// and an exact segment end or `SegmentEnd` slack continues at the next
/// segment's first LSN. Returns `(D, the stop reason, Rc)`; the reason
/// names the first non-durable byte's cause (a diagnostic, never a
/// digest or sim-visible input). Computed over per-inode interval maps
/// in one pass over `E` — never a per-byte loop over the WAL span.
pub fn x5r34_durable_prefix(
    events: &[DiskEvent],
    wal_bindings: &BTreeMap<String, usize>,
    p0: u64,
) -> (u64, String, BTreeSet<String>) {
    let v = X5r34View::build(events);
    let wseg = crate::store::WAL_SEG;
    // The scan can never pass the highest segment a name binds or a
    // recycle reached: above it no rule can hold.
    let max_seg = wal_bindings
        .keys()
        .filter_map(|n| seg_first_lsn_of(n))
        .chain(v.rc.iter().filter_map(|n| seg_first_lsn_of(n)))
        .max()
        .unwrap_or(0);
    let mut lsn = p0;
    loop {
        let seg = lsn - (lsn % wseg);
        if seg > max_seg {
            return (
                lsn,
                format!("beyond the last bound or recycled segment ({max_seg:#x})"),
                v.rc,
            );
        }
        let name = x5r34_seg_name(seg);
        let off0 = lsn - seg;
        let ranges = v.durable_ranges(&name, wal_bindings, seg);
        let mut off = off0;
        for &(a, b) in ranges.iter() {
            if a > off {
                break;
            }
            if b > off {
                off = b;
            }
            if off >= wseg {
                break;
            }
        }
        if off < wseg {
            let why = v.stop_reason(&name, wal_bindings, off);
            return (
                seg + off,
                format!("byte at lsn {} ({name}@{off}): {why}", seg + off),
                v.rc,
            );
        }
        lsn = seg + wseg;
    }
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
    // X5-R20 diagnostic (`E4_MPE_DIAG`, default off, stderr only, never
    // a digest input): the amcheck (i) inputs — each meta slot's parse
    // verdict and the pick, then every slotted §3.3/taint verdict below.
    let mpe_diag = std::env::var("E4_MPE_DIAG").is_ok();
    if mpe_diag {
        let p1 = crate::store::page::parse_meta(&m1);
        let p2 = crate::store::page::parse_meta(&m2);
        eprintln!(
            "MPE diag: durable_image meta slots: s1={:?} s2={:?} pick={:?}",
            p1.as_ref().map(|m| m.ckpt_id).map_err(|_| "parse fail"),
            p2.as_ref().map(|m| m.ckpt_id).map_err(|_| "parse fail"),
            crate::store::page::select_meta(&m1, &m2, false),
        );
    }
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
            // **X5-R20 shape A (P ruling nv3-X-5c-F1-esc-1): the
            // finding is structural — it carries no slot.** With no
            // valid meta there is no opened image, and X5-R5 says
            // O-3 (e) is then *vacuous* ("If both metas are invalid,
            // open fails with XX001 and O-3 (e) is vacuous (there is
            // no opened image)"). The draft hardcoded `slot: Some(1)`
            // here, and the frozen oracle's exactness half judges
            // every slotted finding against tainted ∪ R — with no
            // opened image both sets are empty, so 177 bc4f3bba
            // rounds failed as "amcheck reported slot 1 that is
            // neither tainted nor in R" (repro seeds 67/268 round 0:
            // a Q14 LOST fault on the only meta write ever issued —
            // the create's k=0 — leaves slot 1 unparseable and slot 2
            // never written; the damage stays detected at `Open`,
            // XX001 never served). A slot the store never opened is
            // not a slot its amcheck reported: same convention as the
            // `MisplacedNewer` arm and the bounds findings below
            // ("structural findings carry no slot").
            return (
                vec![Finding {
                    slot: None,
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
            mpe_diag,
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
            mpe_diag,
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
        let chk = crate::store::page::check(b.as_slice(), r, *lpid, None);
        if mpe_diag && (chk.is_err() || disk.range_tainted(pf, r.slot * 4096, 4096)) {
            eprintln!(
                "MPE diag: mapped lpid {lpid} slot {} §3.3={:?} tainted={}",
                r.slot,
                chk.is_ok(),
                disk.range_tainted(pf, r.slot * 4096, 4096)
            );
        }
        if chk.is_err() {
            out.push(Finding {
                slot: Some(r.slot),
                what: "§3.3 fail on mapped page".into(),
            });
        }
    }
    // **X5-R16 MP-E (completeness half, P ruling nv3-X-5c-esc-3): the
    // referenced set is the image's own slots — not the file header.**
    // Slot 0 is the §2.1 file header: §3.3's reference model has no
    // reference for it (a page-map entry or a parent ref), so amcheck
    // (i) can neither §3.3-check it nor report it, while the draft's
    // `used.insert(0)` made every fault on its range a "tainted
    // referenced slot not reported" (the 205 d2c8bba7 completeness
    // rounds, all recovery-`Err`; the ruling's repro seeds 47/118/198
    // each name slot 0). The header is still read by open (a corrupt
    // header fails open with XX001, a detection the oracle sees at
    // `Open`); it just leaves the amcheck-referenced set. The meta
    // slots (1, 2) stay referenced: a tainted one is named by the
    // rollback-fallout loop below ("no valid meta" itself is a
    // structural finding since X5-R20 shape A — no opened image, no
    // slotted amcheck report, O-3 (e) vacuous per X5-R5).
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
    // **X5-R28 (c) (P ruling nv3-X-5c-F2-amend-1): the meta-slot
    // rollback-fallout loop is deleted.** The draft pushed a finding
    // for every tainted meta slot ("stale durable meta slot (rollback
    // fallout)") — a finding derived from `range_tainted` ground truth,
    // standing in for amcheck exactly the way X5-R23 banned for the
    // completeness residual. This walk never pushes a finding derived
    // from `range_tainted`, `issued`, `dwrites` or any other ground
    // truth. Slots 1 and 2 stay in `image_slots` (O-3 (c)'s twin rule
    // and R's guard read that set); a meta-slot fault is judged where
    // it acts — open's selection (S-STORE §2.2), O-1 (the MP lower
    // bound) and O-3 (a)–(d) — and the per-slot completeness halves
    // skip slots 1 and 2 under the oracle amendment (X5-R28 (b)).
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
    diag: bool,
) {
    if depth > 8 || root.slot == 0 {
        return;
    }
    let b = disk.durable_read(pf, root.slot * 4096, 4096);
    // §3.3 with the positional expected id (the gate's R4).
    let chk = crate::store::page::check(&b, &root, base, None);
    if diag && (chk.is_err() || disk.range_tainted(pf, root.slot * 4096, 4096)) {
        eprintln!(
            "MPE diag: map page slot {} depth {depth} §3.3={:?} tainted={}",
            root.slot,
            chk.is_ok(),
            disk.range_tainted(pf, root.slot * 4096, 4096)
        );
    }
    if chk.is_err() {
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
                    diag,
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
    diag: bool,
) {
    if root.slot == 0 {
        return;
    }
    let b = disk.durable_read(pf, root.slot * 4096, 4096);
    // §3.3 with the positional expected id (the gate's R4).
    let chk = crate::store::page::check(&b, &root, base, None);
    if diag && (chk.is_err() || disk.range_tainted(pf, root.slot * 4096, 4096)) {
        eprintln!(
            "MPE diag: free-set page slot {} §3.3={:?} tainted={}",
            root.slot,
            chk.is_ok(),
            disk.range_tainted(pf, root.slot * 4096, 4096)
        );
    }
    if chk.is_err() {
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
                    disk, pf, r, child_base, child_span, bits, _total, set, used, out, diag,
                );
            }
        }
    }
}
/// The opened meta of a disk (durable bytes).
/// The slot (1 or 2) the opened meta was read from, on the disk
/// recovery opens (the crashed, fault-resolved disk): the meta slot
/// whose parsed `ckpt_id` equals the opened selection's. Ambiguous or
/// absent → `None` (the caller's predicate fails closed).
/// **X-5c-F3-F1 step 6** (X5-R30 (c)'s `smx` input).
pub fn opened_meta_slot_of(
    disk: &SimDisk,
    pf: usize,
    opened: &crate::store::page::Meta,
) -> Option<u64> {
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    let p1 = crate::store::page::parse_meta(&m1).map(|m| m.ckpt_id).ok();
    let p2 = crate::store::page::parse_meta(&m2).map(|m| m.ckpt_id).ok();
    match (p1, p2) {
        (Some(k), Some(k2)) if k == opened.ckpt_id && k2 == opened.ckpt_id => None,
        (Some(k), _) if k == opened.ckpt_id => Some(1),
        (_, Some(k)) if k == opened.ckpt_id => Some(2),
        _ => None,
    }
}

/// **X5-R30 (c)'s record fields (X-5c-F3-F1 step 6): `skx` (rule 1) and
/// `smx` (rule 2), the predicates exact** — diagnostics in the record,
/// never oracle inputs; the oracle stays frozen (apart from X5-R32).
/// Both are false unless `served` holds, on the `Ok` arm and the `Err`
/// arm alike.
///
/// `skx` holds iff all of these hold:
/// - class MP, X5-R5's condition 1 (`meta_slot_fault`) and condition 2
///   (`meta_rollback`);
/// - every served range of the round lies on the page file and wholly
///   inside slots of `K`;
/// - `workload_served` is empty (every serve was accepted in a recovery
///   life or in extraction, after open selected the rolled-back meta);
/// - there is no R-taint;
/// - no `uncovered_accepts` were recorded on any of the round's
///   recovery or extraction disks.
///
/// `smx` holds iff all of these hold:
/// - class MP, condition 1 and condition 2;
/// - every served range is exactly `(page file, s·4096, 4096)` with `s`
///   the opened meta's slot (1 or 2);
/// - the accepted bytes — that slot's durable bytes on the disk the
///   accept read — equal the bytes of some `DWrite` the store issued to
///   that offset that was covered by a completed `sync_all` (SimDisk's
///   durable-write history, the `all` mark);
/// - `workload_served` is empty, there is no R-taint, and there are no
///   uncovered accepts.
#[allow(clippy::too_many_arguments)]
pub fn r30_predicates(
    class: FaultClass,
    served_ranges: &[(usize, u64, u64)],
    workload_served_empty: bool,
    r_taint: bool,
    uncovered: bool,
    meta_slot_fault: bool,
    meta_rollback: bool,
    kept_slots: &BTreeSet<u64>,
    opened_meta_slot: Option<u64>,
    meta_disk: &SimDisk,
    pf: usize,
) -> (bool, bool) {
    let served = !served_ranges.is_empty();
    if !served || class != FaultClass::Mp || !meta_slot_fault || !meta_rollback {
        return (false, false);
    }
    if !workload_served_empty || r_taint || uncovered {
        return (false, false);
    }
    let skx = !kept_slots.is_empty()
        && served_ranges.iter().all(|&(f, off, len)| {
            if f != pf || len == 0 {
                return false;
            }
            let s0 = off / 4096;
            let s1 = (off + len - 1) / 4096;
            (s0..=s1).all(|s| kept_slots.contains(&s))
        });
    let smx = match opened_meta_slot {
        Some(s) if s == 1 || s == 2 => {
            let meta_serve = served_ranges
                .iter()
                .all(|&(f, off, len)| f == pf && off == s * 4096 && len == 4096);
            let bytes_match = {
                let durable = meta_disk.durable_read(pf, s * 4096, 4096);
                meta_disk.dwrites_of(pf).iter().any(|w| {
                    w.all && w.off == s * 4096 && w.bytes.len() == 4096 && w.bytes == durable
                })
            };
            meta_serve && bytes_match
        }
        _ => false,
    };
    (skx, smx)
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

/// **X5-R5's anchor, as the events half computes it** (X-5c-F2 step 1
/// (d) pins it at the sync's return): the pair `(g, ev_anchor)` for one
/// (disk, this life's events) view. `g` is the dwrites index of the
/// first `sync_all`-covered meta newer than `opened_k` (the durable
/// half's anchor: writes at dwrite indices above it are post-anchor);
/// `ev_anchor` is the event index the events half skips to — the
/// in-stream graduation sync's **return** when the first issued-and-
/// covered newer meta is the in-stream one, else `0`, the fallback
/// that reads this life's whole stream as post-anchor (a still earlier
/// graduation preceded it). `None` when no newer covered meta exists
/// at all: R is empty and no anchor is in force.
pub fn r5_anchor(
    disk: &SimDisk,
    pf: usize,
    opened_k: u64,
    events: &[DiskEvent],
) -> Option<(usize, usize)> {
    let dw = disk.dwrites_of(pf);
    let mut newer_graduation: Option<usize> = None;
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
    let g = newer_graduation?;
    // The anchor in the event stream: the first completed `sync_all`
    // after the first issued write of a meta newer than the opened one.
    // When that first issued-and-covered newer meta (`dw[g]`) is **not**
    // the in-stream one, a still earlier graduation happened in a prior
    // life of the seed (chained round) and the whole stream is
    // post-anchor. When no in-stream pair exists at all but `dw[g]`
    // does, same: the graduation precedes this life. Writes before the
    // anchor are not excused — the slot was the opened image's own
    // content when they were issued.
    let is_meta_issued = |e: &DiskEvent| -> bool {
        match e {
            DiskEvent::WriteIssued { inode, off, bytes }
                if *inode == pf && (*off == 4096 || *off == 8192) =>
            {
                crate::store::page::parse_meta(bytes)
                    .ok()
                    .is_some_and(|m| m.ckpt_id > opened_k)
            }
            _ => false,
        }
    };
    let in_stream = events.iter().position(is_meta_issued);
    let anchor: usize = match in_stream {
        Some(w) => {
            let synced = events.iter().skip(w + 1).position(
                |e| matches!(e, DiskEvent::SyncReturned { inode, all: true } if *inode == pf),
            );
            match synced {
                // The in-stream pair exists: if that issued write is
                // the same one `dw[g]` covered, this life holds the
                // earliest graduation and the anchor is its sync's
                // return (w + 1 + s, the event index of the
                // `SyncReturned`); otherwise a prior-life graduation
                // precedes the whole stream (the in-stream one never
                // synced, or a still earlier covered meta exists).
                Some(s) => {
                    let same = match &events[w] {
                        DiskEvent::WriteIssued { off, bytes, .. } => {
                            dw[g].off == *off && dw[g].bytes == *bytes
                        }
                        _ => false,
                    };
                    if same { w + 1 + s } else { 0 }
                }
                None => 0,
            }
        }
        None => 0,
    };
    Some((g, anchor))
}

/// **X5-R5 anchor-0 precondition (X-5c-F2 step 1 (e)): the fallback
/// anchor's tripwire.** The anchor-0 fallback reads this life's whole
/// event stream as post-graduation, which is only sound while the page
/// file was never `sync_data`-ed in this life (this store only ever
/// `sync_all`s the page file, so the tripwire cannot fire on a real
/// campaign — it exists so the unsound reading can never pass
/// silently). A page-file `SyncReturned { all: false }` in the stream
/// breaks the precondition: the round records the returned seed-level
/// defect instead of silently widening R.
pub const R5_ANCHOR0_DEFECT: &str = "page file synced by sync_data: X5-R5 anchor precondition";

/// Whether [`r5_anchor`]'s fallback anchor 0 is in force for this view
/// (the events half reads the whole stream as post-graduation).
pub fn r5_anchor0_defect(events: &[DiskEvent], pf: usize) -> Option<&'static str> {
    let sync_data = events
        .iter()
        .any(|e| matches!(e, DiskEvent::SyncReturned { inode, all: false } if *inode == pf));
    sync_data.then_some(R5_ANCHOR0_DEFECT)
}

/// X5-R5's set R: slots the opened image references that are not tainted,
/// written after the first completed `sync_all` covering a meta newer
/// than the opened one, **and never written with an M-NOINPLACE hit**
/// (condition 3, the monitor's per-write verdicts).
///
/// **X5-R20 shape B (P ruling nv3-X-5c-F1-esc-1): condition 3's write
/// history is the *issued* record, not only the sync-covered one.**
/// R5's text says "SimDisk records a page-file write to `s` **issued**
/// after [the anchor]" — and its condition-3 guard (M-NOINPLACE) is
/// itself computed over the issued-event stream. The draft consulted
/// only `dwrites_of` (whose membership is writes a completed sync
/// covered), so a slot reused after the newer graduation whose reuse
/// writes were still unsynced at the crash — the crash keeps their
/// bytes (F-CRASH's `p_keep_latest`), the bytes equal the last issued
/// so 1.4 does not taint them, and the rolled-back image references
/// the slot with its stale create-era reference CRC — was missing from
/// R, and amcheck's correct detection failed the round as "amcheck
/// reported slot … neither tainted nor in R" (the 39 bc4f3bba shape-B
/// rounds; repro seed 38 round 0: slot 4, ten issued writes after the
/// k=1 graduation's sync at ev[128], the crash kept ev[287]'s id-6
/// epoch-2 page, `check2` fails against meta k=0's map-root ref). Both
/// sources are consulted: the durable-write list (cross-life, as
/// before) and this life's event stream (the unsynced reuses), through
/// the shared anchor of [`r5_anchor`].
pub fn exception_set_r(
    disk: &SimDisk,
    pf: usize,
    opened_k: u64,
    image_slots: &BTreeSet<u64>,
    tainted: &BTreeSet<u64>,
    noinplace_hits: &BTreeSet<u64>,
    events: &[DiskEvent],
) -> BTreeSet<u64> {
    let Some((g, anchor)) = r5_anchor(disk, pf, opened_k, events) else {
        return BTreeSet::new();
    };
    let mut out = BTreeSet::new();
    for (i, w) in disk.dwrites_of(pf).iter().enumerate() {
        if i <= g {
            continue;
        }
        let slot = w.off / 4096;
        if r_slot_qualifies(slot, image_slots, tainted, noinplace_hits) {
            out.insert(slot);
        }
    }
    // The issued-write half (X5-R20 shape B), anchored at the sync's
    // return (step 1 (d)) or at the fallback 0 (step 1 (e)'s
    // precondition guards that reading).
    for (i, e) in events.iter().enumerate() {
        if i <= anchor {
            continue;
        }
        if let DiskEvent::WriteIssued { inode, off, .. } = e {
            if *inode != pf {
                continue;
            }
            let slot = off / 4096;
            if r_slot_qualifies(slot, image_slots, tainted, noinplace_hits) {
                out.insert(slot);
            }
        }
    }
    out
}

/// X5-R28 (a) condition 2's `V_k(s)`: the page's content as the
/// durable-write history had it when the given meta was written —
/// 4096 zero bytes, then every `DWrite` of the page file that overlaps
/// `[slot*4096, (slot+1)*4096)` and precedes the meta's own `DWrite`
/// (dwrite index `cutoff`, exclusive), applied in order at each
/// write's own offset. Pure history composition from SimDisk's
/// durable-write list — never store memory, never a §3.3 verdict.
pub fn v_at_meta(disk: &SimDisk, pf: usize, slot: u64, cutoff: usize) -> Vec<u8> {
    let mut page = vec![0u8; 4096];
    for (i, w) in disk.dwrites_of(pf).iter().enumerate() {
        if i >= cutoff {
            break;
        }
        let wend = w.off + w.bytes.len() as u64;
        if w.off < (slot + 1) * 4096 && wend > slot * 4096 {
            let dst0 = (w.off.max(slot * 4096) - slot * 4096) as usize;
            let src0 = (w.off.max(slot * 4096) - w.off) as usize;
            let span = ((wend.min((slot + 1) * 4096) - w.off.max(slot * 4096)) as usize)
                .min(w.bytes.len().saturating_sub(src0));
            if let (Some(dst), Some(src)) = (
                page.get_mut(dst0..dst0 + span),
                w.bytes.get(src0..src0 + span),
            ) {
                dst.copy_from_slice(src);
            }
        }
    }
    page
}

/// X5-R28 (a): the opened meta's own `DWrite` — the **last** `DWrite`
/// at offset 4096 or 8192 whose bytes parse as a meta with the opened
/// `ckpt_id`. `None` means no such write exists in the durable history
/// (the opened image's meta never became durable as a distinct write):
/// `K` fails closed to empty for the round.
pub fn meta_dwrite_cutoff(disk: &SimDisk, pf: usize, opened_k: u64) -> Option<usize> {
    let mut found: Option<usize> = None;
    for (i, w) in disk.dwrites_of(pf).iter().enumerate() {
        if (w.off == 4096 || w.off == 8192)
            && w.bytes.len() == 4096
            && let Ok(m) = crate::store::page::parse_meta(&w.bytes)
            && m.ckpt_id == opened_k
        {
            found = Some(i);
        }
    }
    found
}

/// **X5-R28 (a): the kept set `K`** (P ruling nv3-X-5c-F2-amend-1).
/// The mirror of X5-R5's `R`: after a meta rollback, a slot the newer
/// meta freed and the store reissued, where the reissue **did not
/// land** (dropped by F-CRASH, or a LOST fault reverted it), is tainted
/// by 1.4 yet its durable bytes are the opened image's own content —
/// §3.3 passes, and no correct amcheck can report it. `K` is computed
/// from SimDisk history only: never from store memory, never from
/// §3.3 or amcheck output, and never from the taint map except as
/// condition 1's membership test.
///
/// `K` is non-empty only while `r5_active` holds — X5-R5's conditions
/// 1 and 2 (a media fault of the round damaged meta slot 1 or 2, and
/// the opened meta is a rollback), the same gate `R` honours; classes
/// C and MW never meet condition 1, so `K` is empty there. Then `s ∈
/// K` iff:
///
/// 1. `s ∉ {0, 1, 2}`, `s` is referenced by the opened image, and `s`
///    is tainted at the amcheck point of this computation;
/// 2. the durable bytes of `s` equal `V_k(s)` byte for byte
///    ([`v_at_meta`]; fail-closed to empty when the opened meta's own
///    `DWrite` does not exist — [`meta_dwrite_cutoff`]);
/// 3. SimDisk records a page-file write to `s` issued after X5-R5's
///    anchor ([`r5_anchor`]: the durable half's `g`, the events half's
///    anchor — the first completed `sync_all` covering a meta newer
///    than the opened one, at its return);
/// 4. no write to `s` drew an M-NOINPLACE hit.
///
/// So `K ⊆ tainted ∩ referenced` and `K ∩ R = ∅` (R requires
/// untainted). At the (ii) point the caller computes `K_ii` the same
/// way, against the end image's own meta and the (ii) taint.
#[allow(clippy::too_many_arguments)]
pub fn kept_set_k(
    disk: &SimDisk,
    pf: usize,
    opened_k: u64,
    image_slots: &BTreeSet<u64>,
    tainted: &BTreeSet<u64>,
    noinplace_hits: &BTreeSet<u64>,
    events: &[DiskEvent],
    r5_active: bool,
) -> BTreeSet<u64> {
    let mut out = BTreeSet::new();
    if !r5_active {
        return out;
    }
    let Some((g, anchor)) = r5_anchor(disk, pf, opened_k, events) else {
        return out;
    };
    // Fail closed: no opened-meta DWrite means no composable history.
    let Some(cutoff) = meta_dwrite_cutoff(disk, pf, opened_k) else {
        return out;
    };
    for &s in tainted.iter() {
        // Condition 1.
        if s < 3 || !image_slots.contains(&s) || !tainted.contains(&s) {
            continue;
        }
        // Condition 4.
        if noinplace_hits.contains(&s) {
            continue;
        }
        // Condition 3: a write to s issued after the anchor — the
        // durable half (dwrite index above g) or the issued half
        // (event index above the events anchor).
        let dw_after = disk.dwrites_of(pf).iter().enumerate().any(|(i, w)| {
            i > g && w.off < (s + 1) * 4096 && w.off + w.bytes.len() as u64 > s * 4096
        });
        let ev_after = events.iter().enumerate().any(|(i, e)| match e {
            DiskEvent::WriteIssued { inode, off, bytes } if i > anchor && *inode == pf => {
                *off < (s + 1) * 4096 && *off + bytes.len() as u64 > s * 4096
            }
            _ => false,
        });
        if !dw_after && !ev_after {
            continue;
        }
        // Condition 2: the durable bytes are the opened image's own
        // content for this slot.
        if disk.durable_read(pf, s * 4096, 4096) == v_at_meta(disk, pf, s, cutoff) {
            out.insert(s);
        }
    }
    out
}

/// **X5-R29 (a)'s diagnostic** (`E4_MPE_DIAG`, stderr only, never a
/// digest input): on the end disk's image, the failed lpids (mapped
/// pages whose durable bytes fail §3.3 against their map `Ref` — the
/// nodes the store's `check_tree` cannot pass, so never descends), the
/// descendant set of every failed node (children parsed through the
/// store's own page decoder — from the failed node's `V_e` content,
/// composed as X5-R28 (a) composes `V_k`, because the damaged bytes
/// cannot be trusted to enumerate a subtree; intact descendants parse
/// from their own durable bytes), and the "mapped lpid N unreachable"
/// set the store's amcheck reported. Equal sets ⇒ subtree fallout (the
/// walk cannot decide reachability below a failed node); differing
/// sets ⇒ real orphan map entries (I-LPID-EXACT), F3's store
/// deviation. Pure SimDisk history; never an oracle input.
pub fn r29_subtree_diag(disk: &SimDisk, cfg: &StoreCfg, findings: &[Finding]) {
    let pf = match disk.stat(crate::sim::PAGE_FILE) {
        Some(f) => f,
        None => return,
    };
    let m1 = disk.durable_read(pf, 4096, 4096);
    let m2 = disk.durable_read(pf, 8192, 4096);
    let meta = match crate::store::page::select_meta(&m1, &m2, false) {
        crate::store::page::MetaPick::Win { meta, .. } => meta,
        _ => return,
    };
    // The flat map (lpid → Ref), by the same walk durable_image uses.
    let mut flat: BTreeMap<u64, crate::store::page::Ref> = BTreeMap::new();
    let mut used: BTreeSet<u64> = BTreeSet::new();
    let mut sink: Vec<Finding> = Vec::new();
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
            &mut sink,
            false,
        );
    }
    // The failed nodes: mapped pages failing §3.3 against their Ref.
    let failed: Vec<u64> = flat
        .iter()
        .filter(|(l, r)| {
            let b = disk.durable_read(pf, r.slot * 4096, 4096);
            crate::store::page::check(&b, r, **l, None).is_err()
        })
        .map(|(l, _)| *l)
        .collect();
    // The end meta's V_e cutoff (fail closed: without it the
    // descendant set cannot be composed and the diag says so).
    let cutoff = meta_dwrite_cutoff(disk, pf, meta.ckpt_id);
    // The children of one page's bytes, through the store's decoder.
    let children_of = |bytes: &[u8]| -> Vec<u64> {
        if crate::store::page::header_kind(bytes) == crate::store::page::KIND_LEAF {
            return Vec::new();
        }
        match crate::store::tree::inner_seps(bytes) {
            Ok((seps, right)) => seps.iter().map(|(c, _)| *c).chain([right]).collect(),
            Err(_) => Vec::new(),
        }
    };
    // Descendants of the failed nodes: expand a failed node from its
    // V_e content, an intact one from its durable bytes.
    let mut desc: BTreeSet<u64> = BTreeSet::new();
    if let Some(cut) = cutoff {
        let mut stack: Vec<u64> = failed.clone();
        while let Some(l) = stack.pop() {
            let Some(r) = flat.get(&l) else { continue };
            let is_failed = failed.contains(&l);
            let kids = if is_failed {
                let ve = v_at_meta(disk, pf, r.slot, cut);
                children_of(&ve)
            } else {
                let b = disk.durable_read(pf, r.slot * 4096, 4096);
                children_of(&b)
            };
            for c in kids {
                if desc.insert(c) {
                    stack.push(c);
                }
            }
        }
    }
    // The unreachable set, from the store's own findings.
    let unreachable: BTreeSet<u64> = findings
        .iter()
        .filter_map(|f| {
            f.what
                .strip_prefix("mapped lpid ")
                .and_then(|t| t.strip_suffix(" unreachable"))
                .and_then(|n| n.parse::<u64>().ok())
        })
        .collect();
    // Debug detail for the branch decision (stderr only): the image's
    // shape and every failed node's V_e header kind and parsed
    // children, plus the (lpid, slot) of the failed and unreachable
    // sets.
    let mut note: BTreeSet<u64> = failed.iter().copied().collect();
    note.extend(unreachable.iter().copied());
    eprintln!(
        "R29 dbg: dir_root={} next_lpid={} slots_of_note={:?}",
        meta.dir_root,
        meta.next_lpid,
        note.iter()
            .filter_map(|l| flat.get(l).map(|r| (*l, r.slot)))
            .collect::<Vec<_>>()
    );
    if let Some(cut) = cutoff {
        for l in failed.iter() {
            let Some(r) = flat.get(l) else { continue };
            let ve = v_at_meta(disk, pf, r.slot, cut);
            eprintln!(
                "R29 dbg: failed lpid {l} slot {} ve_kind={} kids={:?}",
                r.slot,
                crate::store::page::header_kind(&ve),
                children_of(&ve)
            );
        }
    }
    let equal = cutoff.is_some() && desc == unreachable;
    eprintln!(
        "R29 diag: failed_lpids {:?} cutoff_ok={} descendants {:?} unreachable {:?} equal={}",
        failed,
        cutoff.is_some(),
        desc,
        unreachable,
        equal
    );
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
