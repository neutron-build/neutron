//! C-T0 §5.1: the placement loop as **non-blocking steps**. One `step`
//! call = at most one latch section: it takes `core.latches.lock(latch_key)`
//! at its start and releases it before returning. A step never parks, never
//! calls [`Epq::recheck`](crate::write::Epq), never takes a second latch.
//! Under the latch it opens **one** registered view and reads `k@INTENT` and
//! every version of `k` from it (`[intent_key(k), end_key(k))`) — the
//! "fresh view of the latest state" — then follows §5.1 line by line, in
//! its order.
//!
//! The blocking drivers loop the steps, parking through
//! [`Core::wait_on_any`](crate::wait::Core::wait_on_any) and running the
//! §5.2 EPQ callback between steps (never under a latch).

use std::ops::Bound;

use nucleus_kv::{Batch, Durability, OrderedKv};

use crate::boot::Core;
use crate::encoding::{decode_intent, decode_version, encode_intent, end_key, intent_key, Entry};
use crate::latch::{latch_key, latch_prefix_of, LatchGuard};
use crate::registry::ViewGuard;
use crate::removal::{remove_intent_under_latch, RemovalMode};
use crate::txn::Txn;
use crate::wait::WaitOutcome;
use crate::write::ctx::{classify_foreign, is_snapshot_iso, Foreign};
use crate::write::ctx::{EpqRequest, WaitSet};
use crate::write::layer::{apply_change, Change};
use crate::write::unique::unique_verdict;
use crate::write::{
    CommittedVersion, Epq, EpqDecision, LockWait, RowOp, RowOutcome, SkipReason, StmtCtx,
    UniqueRule,
};
use crate::{LayerData, RowLockMode, Ts, TxnError, TxnId, TxnStatus};

/// What one step needs next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// The op finished with this outcome.
    Done(RowOutcome),
    /// State changed under the latch (a foreign intent was removed): retry
    /// from the top of §5.1 with a fresh view.
    Again,
    /// Conflicting holders were found; park on the wait set (§6), then
    /// retry. The generations were read under the step's latch.
    Wait(WaitSet),
    /// RC row op: run the §5.2 EvalPlanQual against `EpqRequest::v`
    /// (unlatched), then `epq_result` and retry.
    Epq(EpqRequest),
    /// Crate-internal (C-T2c, §5.3.1(3)): the ON CONFLICT arbiter lock found
    /// a version newer than its base `v_r` — restart the arbiter, never
    /// EPQ. The public drivers never return this.
    Restart,
}

/// One logical key's state read from one registered view: its intent (if
/// present) and its versions, newest first (the stored order, §2.2).
pub(crate) struct KeyState {
    pub(crate) intent: Option<crate::Intent>,
    pub(crate) versions: Vec<CommittedVersion>,
}

impl KeyState {
    /// The newest committed **data** version (§5.1: intents are not
    /// versions; lock-only intents produce none, so every version in the
    /// list is a data version).
    pub(crate) fn newest_committed(&self) -> Option<&CommittedVersion> {
        self.versions.first()
    }

    /// Whether the newest committed data version is live.
    pub(crate) fn committed_live(&self) -> bool {
        self.newest_committed()
            .is_some_and(|v| matches!(v.value, crate::encoding::VersionValue::Live { .. }))
    }

    /// `N`: data versions with `ts > base` (§5.1). Versions are newest
    /// first, so `N` is a prefix of the list.
    pub(crate) fn versions_above(&self, base: Ts) -> &[CommittedVersion] {
        let n = self
            .versions
            .iter()
            .position(|v| v.ts <= base)
            .unwrap_or(self.versions.len());
        &self.versions[..n]
    }
}

/// Reads `k@INTENT` and every version of `k` from one view: exactly the
/// entries of `[intent_key(key), end_key(key))` (§2.2).
pub(crate) fn read_key_state<S: nucleus_kv::Snapshot>(
    view: &ViewGuard<'_, S>,
    key: &[u8],
) -> Result<KeyState, TxnError> {
    let lo = intent_key(key);
    let hi = end_key(key);
    let mut state = KeyState {
        intent: None,
        versions: Vec::new(),
    };
    for entry in view.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (stored, value) = entry.map_err(crate::kv_err)?;
        match crate::encoding::parse_key(&stored) {
            Some((_, Entry::Intent)) => state.intent = Some(decode_intent(&value)?),
            Some((_, Entry::Version(ts))) => state.versions.push(CommittedVersion {
                ts,
                value: decode_version(&value)?,
            }),
            None => {}
        }
    }
    Ok(state)
}

/// §5.1 placement write: `k@INTENT` as one batch, under a guard that must
/// protect `latch_key(key, deferrable_prefix)` (seed 4: checked with a
/// debug assert and an invariant error in release builds, like
/// `remove_intent_under_latch`). A failed write calls the core's fail-stop
/// hook (§3), then returns the error.
pub(crate) fn place_intent_under_latch<K: OrderedKv>(
    core: &Core<K>,
    key: &[u8],
    deferrable_prefix: Option<&[u8]>,
    intent: &crate::Intent,
    latch: &LatchGuard<'_>,
) -> Result<(), TxnError> {
    let want = latch_key(key, deferrable_prefix);
    debug_assert!(
        latch.protects(want),
        "placement of {key:?} under the latch of {:?} (want {want:?})",
        latch.key()
    );
    if !latch.protects(want) {
        return Err(TxnError::Invariant(format!(
            "placement of {key:?} under the latch of {:?}, want {want:?} (C-T0 §5.1)",
            latch.key()
        )));
    }
    let mut batch = Batch::default();
    batch.put(intent_key(key), encode_intent(intent)?);
    if let Err(e) = core.write(batch, Durability::No) {
        core.fail_stop().on_kv_error(&e);
        return Err(e);
    }
    Ok(())
}

/// The wake generation of `id`, read while the latch that observed the
/// conflict is held (§5.1, §6): a live conflicting holder's entry cannot be
/// truncated under the latch (its release takes the same latch), so `0`
/// here is unreachable in practice.
fn gen_of<K: OrderedKv>(core: &Core<K>, id: TxnId) -> u64 {
    core.status.entry(id).map_or(0, |e| e.gen)
}

/// A shared-row-lock holder that is still live (§6: conflict checks ignore
/// ended holders — `Ended`, `Aborted`, or a visible commit).
fn holder_still_holds<K: OrderedKv>(core: &Core<K>, holder: TxnId) -> bool {
    match core.status.lookup_remembered(holder) {
        crate::status::Remembered::Ended => false,
        crate::status::Remembered::Live(TxnStatus::Aborted, _) => false,
        crate::status::Remembered::Live(TxnStatus::Committed(c), _) => c > core.visible_ts(),
        crate::status::Remembered::Live(TxnStatus::Pending, _) => true,
    }
}

/// How a wait's outcome maps into the driver loop (§6): a cancelled wait is
/// 57014, a detected deadlock 40P01, an expired `lock_timeout` 55P03;
/// everything else retries the step.
pub(crate) fn map_wait_outcome(o: WaitOutcome) -> Result<(), TxnError> {
    match o {
        WaitOutcome::Cancelled => Err(TxnError::QueryCanceled),
        WaitOutcome::Deadlock => Err(TxnError::Deadlock),
        WaitOutcome::LockTimeout => Err(TxnError::LockNotAvailable),
        WaitOutcome::Ended
        | WaitOutcome::Aborted
        | WaitOutcome::Committed(_)
        | WaitOutcome::GenChanged => Ok(()),
    }
}

/// The latch bytes of `key` under `latch_prefix` (§5.0).
fn latch_bytes(key: &[u8], latch_prefix: Option<usize>) -> &[u8] {
    latch_prefix_of(key, latch_prefix).unwrap_or(key)
}

/// Keeps one entry per txn in a wait set (rework 6: the wait registers and
/// fires `on_wait_start`/`on_wait_end` per entry, so a holder with several
/// acquisitions — KEY SHARE at seq 1, SHARE at seq 2 — must appear once).
fn dedup_targets(targets: &mut WaitSet) {
    let mut seen = std::collections::HashSet::new();
    targets.retain(|(t, _)| seen.insert(*t));
}

/// What the task places once §5.1's checks passed: a layer change, or a
/// shared lock granted in the [`RowLocks`](crate::write::RowLocks) table
/// (§2.1: shared modes are never stored in intents).
pub(crate) enum PlaceAction {
    Change(Change),
    GrantShared(RowLockMode),
}

/// One §5.1 latch section, shared by the row-op and key-existence tasks.
pub(crate) enum StepOutcome {
    Step(Step),
    Epq(EpqRequest),
}

/// §5.1, in its order, under one latch: foreign intent, shared holders,
/// unique check / §5.4 own-row rules / the newer-version rule, then the
/// placement (or the shared grant).
#[allow(clippy::too_many_arguments)]
pub(crate) fn place_step<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    key: &[u8],
    latch_prefix: Option<usize>,
    requested: RowLockMode,
    key_exist: bool,
    unique: Option<&UniqueRule>,
    own_row_rules: bool,
    action: &PlaceAction,
    ctx: &StmtCtx,
    base: Option<Ts>,
    arb_lock: bool,
) -> Result<StepOutcome, TxnError> {
    let lk = latch_bytes(key, latch_prefix);
    let latch = core.latches.lock(lk);
    // §5.1: one registered view of the latest state, under the latch.
    {
        let view = core.open_view();
        let state = read_key_state(&view, key)?;
        drop(view);

        // §5.1: foreign intent of T.
        if let Some(intent) = &state.intent {
            if intent.txn != txn.id {
                let owner = intent.txn;
                let status = core.status.lookup_for_intent(owner)?;
                match classify_foreign(status, core.visible_ts()) {
                    Foreign::RemoveResolve | Foreign::RemoveDiscard => {
                        // Remove under the latch already held, then retry
                        // with a fresh view (seed 12: never place over it).
                        let mode = if matches!(status, TxnStatus::Committed(_)) {
                            RemovalMode::Resolve
                        } else {
                            RemovalMode::Discard
                        };
                        remove_intent_under_latch(
                            core,
                            key,
                            latch_prefix_of(key, latch_prefix),
                            owner,
                            mode,
                            &latch,
                        )?;
                        drop(latch);
                        return Ok(StepOutcome::Step(Step::Again));
                    }
                    Foreign::Blocking => {
                        let top_lock = intent.top().map_or(RowLockMode::NoKeyUpdate, |t| t.lock);
                        let top_absent = intent.top().is_some_and(|t| t.data == LayerData::Absent);
                        // R3W-8: no wait on a lock-only intent over a live
                        // row: a key-existence op raises the §5.3 verdict at
                        // once. C-T2 rework 7a: only a `Unique` rule's op
                        // conflicts here, and `same_row` never does — the
                        // per-row verdict (`unique_verdict` with no own top
                        // layer: W has no intent on the key) decides.
                        if key_exist && top_absent && state.committed_live() {
                            if let Some(rule) = unique {
                                unique_verdict(
                                    core,
                                    txn,
                                    key,
                                    None,
                                    state.newest_committed(),
                                    rule,
                                    ctx.snapshot(),
                                )?;
                            }
                        }
                        if requested.conflicts_with(top_lock) {
                            let g = gen_of(core, owner);
                            drop(latch);
                            return Ok(StepOutcome::Step(wait_or_err(
                                ctx,
                                key_exist,
                                vec![(owner, g)],
                            )?));
                        }
                        // No conflict (e.g. KEY SHARE vs NO KEY UPDATE):
                        // T's intent is invisible data; pass (seed 52).
                    }
                }
            }
        }

        // §5.1: conflicting shared holders (§6): minus W, minus ended
        // holders; every conflicting holder's generation is read under this
        // latch. One entry per txn (rework 6: a holder with several
        // acquisitions appears once, so `on_wait_start`/`on_wait_end` fire
        // once per target txn).
        let holders: Vec<TxnId> = core
            .row_locks()
            .holders(key)
            .into_iter()
            .filter(|(h, m, _)| {
                *h != txn.id && holder_still_holds(core, *h) && requested.conflicts_with(*m)
            })
            .map(|(h, _, _)| h)
            .collect();
        if !holders.is_empty() {
            let mut targets: WaitSet = holders.iter().map(|h| (*h, gen_of(core, *h))).collect();
            dedup_targets(&mut targets);
            drop(latch);
            return Ok(StepOutcome::Step(wait_or_err(ctx, key_exist, targets)?));
        }

        let own = state.intent.as_ref().filter(|i| i.txn == txn.id);

        if key_exist {
            // §5.1: key-existence ops always run the unique check (§5.3),
            // own intent or not; never 40001 from N alone, never EPQ.
            if let Some(rule) = unique {
                unique_verdict(
                    core,
                    txn,
                    key,
                    own.and_then(|i| i.top()),
                    state.newest_committed(),
                    rule,
                    ctx.snapshot(),
                )?;
            }
        } else if let Some(own) = own {
            // §5.4 own-row rules, only for the statement's data-changing
            // row ops (lock requests and internal commands only build a
            // layer). No newer-version check: the own intent excluded
            // foreign writers since it was placed.
            if own_row_rules {
                let data_seq = own.top().map_or(0, |t| t.data_seq);
                if data_seq == ctx.seq0() {
                    drop(latch);
                    if ctx.wants_revisit_error() {
                        return Err(TxnError::CardinalityViolation);
                    }
                    return Ok(StepOutcome::Step(Step::Done(RowOutcome::Skipped(
                        SkipReason::SelfModified,
                    ))));
                }
                if data_seq > ctx.seq0() {
                    drop(latch);
                    return Err(TxnError::TriggeredDataChange);
                }
            }
        } else {
            // Row op with no own intent (§5.1): the newer-version rule.
            // base = S, or after an EPQ pass the ts of the version it
            // evaluated, or (ON CONFLICT lock) v_r.ts (§5.3.1).
            let base = base.unwrap_or_else(|| ctx.snapshot());
            let n = state.versions_above(base);
            if !n.is_empty() {
                if is_snapshot_iso(txn.isolation) {
                    // RR/SER: 40001. KEY SHARE: only if some version in N
                    // is a tombstone, moved-tombstone or key_changed write
                    // (examine **all** of N, seed 45).
                    let bad = requested != RowLockMode::KeyShare
                        || n.iter()
                            .any(|v| v.is_tombstone() || v.is_key_changed_write());
                    if bad {
                        drop(latch);
                        return Err(TxnError::SerializationFailure);
                    }
                    // KEY SHARE proceeds over plain newer writes.
                } else {
                    // RC (§5.2): EPQ, unlatched. The ON CONFLICT lock
                    // instead restarts its arbiter (§5.3.1(3)); with
                    // base = v_r.ts a non-empty N means the row changed
                    // again, so it never spins (I-PROGRESS).
                    if arb_lock {
                        drop(latch);
                        return Ok(StepOutcome::Step(Step::Restart));
                    }
                    let v = state.newest_committed().cloned().ok_or_else(|| {
                        TxnError::Invariant("EPQ with no committed version".into())
                    })?;
                    // §5.2: KEY SHARE examines every version above S.
                    let above_s = if requested == RowLockMode::KeyShare {
                        state.versions_above(ctx.snapshot()).to_vec()
                    } else {
                        Vec::new()
                    };
                    drop(latch);
                    return Ok(StepOutcome::Epq(EpqRequest { v, above_s }));
                }
            }
        }

        // §5.1: shared lock — record it in the lock table under this latch,
        // tagged with the writing command's seq (§5.5: `ROLLBACK TO s`
        // releases those taken at `seq >= s`); no intent is written (§2.1).
        if let PlaceAction::GrantShared(mode) = action {
            core.row_locks()
                .grant(key, txn.id, *mode, ctx.seq(), latch_prefix)?;
            drop(latch);
            return Ok(StepOutcome::Step(Step::Done(RowOutcome::Applied)));
        }

        // Placement (§5.1): log and count **both before** the write
        // (I-COUNT), build the layer (§2.1), write `k@INTENT` as one batch.
        let change = match action {
            PlaceAction::Change(c) => c,
            PlaceAction::GrantShared(_) => {
                return Err(TxnError::Invariant(
                    "a shared grant never places an intent".into(),
                ))
            }
        };
        let top_seq = own.and_then(|i| i.top()).map_or(0, |t| t.seq);
        let s = ctx.place_seq().max(top_seq);
        txn.log_write(s, key, latch_prefix);
        if own.is_none() {
            core.count_placement(txn)?;
        }
        let intent = apply_change(
            own,
            txn.id,
            ctx.place_seq(),
            ctx.data_seq(),
            change.clone(),
            state.committed_live(),
        );
        place_intent_under_latch(
            core,
            key,
            latch_prefix_of(key, latch_prefix),
            &intent,
            &latch,
        )?;
        drop(latch);
        // §8.2 writer side, after the latch is released: only data-changing
        // placements check SIREADs (lock-only and shared never do).
        if change.changes_data() {
            core.ssi_hook().on_data_placed(txn.id, txn.isolation, key)?;
        }
    }
    Ok(StepOutcome::Step(Step::Done(RowOutcome::Applied)))
}

/// §6 NOWAIT / SKIP LOCKED for a conflicting wait: `NoWait` raises 55P03;
/// `SkipLocked` skips the row (a key-existence op with SKIP LOCKED is a
/// caller bug).
fn wait_or_err(ctx: &StmtCtx, key_exist: bool, targets: WaitSet) -> Result<Step, TxnError> {
    match ctx.wait() {
        LockWait::Block => Ok(Step::Wait(targets)),
        LockWait::NoWait => Err(TxnError::LockNotAvailable),
        LockWait::SkipLocked => {
            if key_exist {
                Err(TxnError::Invariant(
                    "SKIP LOCKED on a key-existence op: the caller must handle conflicts".into(),
                ))
            } else {
                Ok(Step::Done(RowOutcome::Skipped(SkipReason::Locked)))
            }
        }
    }
}

/// A row op on one key (§5.0), driven step by step. Built through
/// [`RowOpTask::new`]; the crate-private constructors below are the C-T2c
/// seams (§5.3.1).
pub struct RowOpTask {
    key: nucleus_kv::Key,
    latch_prefix: Option<usize>,
    op: RowOp,
    ctx: StmtCtx,
    /// The base for the §5.1 newer-version rule: `None` → `S`; `Some(ts)`
    /// after an EPQ pass (the version it evaluated) or `v_r.ts` for the ON
    /// CONFLICT lock.
    base: Option<Ts>,
    /// `update_pk`'s moved delete: `Delete` changes write `moved: true`.
    moved: bool,
    /// The ON CONFLICT arbiter lock (§5.3.1(3)): never EPQ — a non-empty N
    /// is a restart signal instead.
    arb_lock: bool,
    /// The version remembered by the step that returned `Epq` (for
    /// `epq_result`'s `base = v.ts`).
    pending_epq: Option<CommittedVersion>,
}

impl RowOpTask {
    pub fn new(key: &[u8], latch_prefix: Option<usize>, op: RowOp, ctx: StmtCtx) -> RowOpTask {
        RowOpTask {
            key: key.to_vec(),
            latch_prefix,
            op,
            ctx,
            base: None,
            moved: false,
            arb_lock: false,
            pending_epq: None,
        }
    }

    /// §5.3.1(3) (C-T2c): the arbiter lock on row `r`'s `/t/` key, measured
    /// against `v_r.ts`, that never runs EPQ — a non-empty N restarts the
    /// arbiter ([`Step::Restart`]). Unit-tested in `write/tests`.
    #[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
    pub(crate) fn arbiter_lock(mut self, v_r_ts: Ts) -> RowOpTask {
        self.arb_lock = true;
        self.base = Some(v_r_ts);
        self
    }

    /// [`Core::update_pk`](crate::boot::Core::update_pk)'s old-key op: a
    /// `Delete { moved: true }` (§2.1).
    pub(crate) fn moved_delete(mut self) -> RowOpTask {
        self.moved = true;
        self
    }

    /// What this op places (§2.1): shared requests never reach
    /// `apply_change`, they are granted in the lock table.
    fn action(&self) -> PlaceAction {
        match &self.op {
            RowOp::Update {
                value,
                key_cols_changed,
            } => PlaceAction::Change(Change::Write {
                value: value.clone(),
                key_cols_changed: *key_cols_changed,
            }),
            RowOp::Delete => PlaceAction::Change(Change::Delete { moved: self.moved }),
            RowOp::Lock(mode) => {
                if mode.is_exclusive() {
                    PlaceAction::Change(Change::Lock(*mode))
                } else {
                    PlaceAction::GrantShared(*mode)
                }
            }
        }
    }

    /// One latch section of §5.1.
    pub fn step<K: OrderedKv>(&mut self, core: &Core<K>, txn: &Txn) -> Result<Step, TxnError> {
        let outcome = place_step(
            core,
            txn,
            &self.key,
            self.latch_prefix,
            self.op.requested_mode(),
            false,
            None,
            self.op.is_data_row_op() && !self.ctx.is_internal(),
            &self.action(),
            &self.ctx,
            self.base,
            self.arb_lock,
        )?;
        self.pending_epq = None;
        match outcome {
            StepOutcome::Step(s) => Ok(s),
            StepOutcome::Epq(req) => {
                self.pending_epq = Some(req.v.clone());
                Ok(Step::Epq(req))
            }
        }
    }

    /// The requested row-lock mode of the op the task currently carries
    /// (§6): an EPQ `Apply` may have replaced the op, so the §5.2 pass
    /// reads the mode here rather than caching the initial op's.
    pub(crate) fn requested_mode(&self) -> RowLockMode {
        self.op.requested_mode()
    }

    /// Feeds the caller's §5.2 EPQ verdict back: `Apply(op)` re-bases the
    /// task on the remembered version (`base = v.ts`) and adopts the new
    /// op; `Skip` is the caller's business (the driver stops with
    /// `Skipped(EpqFailed)`). On the retry, under the latch, `N` is empty
    /// while the newest committed data version is still `v`, so the intent
    /// is placed; if it is not `v` any more, the step EPQs again (seed 47).
    ///
    /// C-T2 rework 7d: a call after a step that did **not** return `Epq`
    /// is an invariant error (there is no remembered version to re-base
    /// on; silently falling back to `S` would lose the seed 47 re-check).
    pub fn epq_result(&mut self, decision: EpqDecision) -> Result<(), TxnError> {
        let Some(v) = self.pending_epq.take() else {
            return Err(TxnError::Invariant(
                "epq_result called after a step that did not return Epq".into(),
            ));
        };
        if let EpqDecision::Apply(op) = decision {
            self.base = Some(v.ts);
            // A moved delete (update_pk) stays moved; any other applied op
            // is not a move.
            if !matches!(op, RowOp::Delete) {
                self.moved = false;
            }
            self.op = op;
        }
        Ok(())
    }
}

/// A key-existence op (§5.0): INSERT of a `/t/{rel}/{pk}` key, a
/// PK-changing UPDATE's new `/t/` key, or a `/u/{idx}/{key}` unique entry.
/// Runs the full §5.1 loop with the §5.3 unique check instead of the
/// newer-version rule; never EPQ.
pub struct KeyOpTask {
    key: nucleus_kv::Key,
    latch_prefix: Option<usize>,
    value: Vec<u8>,
    ctx: StmtCtx,
    unique: UniqueRule,
}

impl KeyOpTask {
    pub fn new(
        key: &[u8],
        latch_prefix: Option<usize>,
        value: Vec<u8>,
        ctx: StmtCtx,
        unique: UniqueRule,
    ) -> KeyOpTask {
        KeyOpTask {
            key: key.to_vec(),
            latch_prefix,
            value,
            ctx,
            unique,
        }
    }

    /// One latch section of §5.1 (key-existence shape).
    pub fn step<K: OrderedKv>(&mut self, core: &Core<K>, txn: &Txn) -> Result<Step, TxnError> {
        let rule = match &self.unique {
            // §5.3: a deferrable entry's placement skips the per-row check
            // (G0-write's `is_defer_op`); the prefix is checked at end of
            // statement or commit. `None`: no per-row check either.
            UniqueRule::Deferrable | UniqueRule::None => None,
            unique @ UniqueRule::Unique { .. } => Some(unique),
        };
        let action = PlaceAction::Change(Change::Write {
            value: self.value.clone(),
            key_cols_changed: false,
        });
        let outcome = place_step(
            core,
            txn,
            &self.key,
            self.latch_prefix,
            RowLockMode::NoKeyUpdate, // §6: inserts and unique entries
            true,
            rule,
            false,
            &action,
            &self.ctx,
            None,
            false,
        )?;
        match outcome {
            // Key-existence ops never EPQ and never restart.
            StepOutcome::Epq(_) => Err(TxnError::Invariant(
                "key-existence op asked to EPQ (§5.1 forbids it)".into(),
            )),
            StepOutcome::Step(s) => match s {
                Step::Restart => Err(TxnError::Invariant(
                    "key-existence op asked to restart (§5.1 forbids it)".into(),
                )),
                s => Ok(s),
            },
        }
    }
}

impl<K: OrderedKv> Core<K> {
    /// The §5.2 rules that run before the caller's recheck, shared by
    /// every EPQ pass: a moved tombstone is 40001 ("tuple to be locked was
    /// already moved"), a plain tombstone skips, and a KEY SHARE request
    /// examines **every** version above `S`, not only the newest (seed 45).
    fn epq_pre_rules(
        requested: RowLockMode,
        req: &EpqRequest,
    ) -> Result<Option<RowOutcome>, TxnError> {
        let v = &req.v;
        // §5.2 moved rows: EPQ that reaches a moved-tombstone raises 40001;
        // a just-resolved foreign `Delete { moved: true }` intent wrote
        // exactly that version.
        if v.is_moved_tombstone() {
            return Err(TxnError::SerializationFailure);
        }
        if v.is_tombstone() {
            return Ok(Some(RowOutcome::Skipped(SkipReason::EpqFailed)));
        }
        // §5.1/§5.2: KEY SHARE examines every version above S (seed 45).
        if requested == RowLockMode::KeyShare {
            for ver in &req.above_s {
                if ver.is_moved_tombstone() {
                    return Err(TxnError::SerializationFailure);
                }
                if ver.is_tombstone() || ver.is_key_changed_write() {
                    return Ok(Some(RowOutcome::Skipped(SkipReason::EpqFailed)));
                }
            }
        }
        Ok(None)
    }

    /// The §5.2 RC EvalPlanQual pass, between steps, never under a latch:
    /// the pre-rules above, then the caller's `recheck`. `fold` applies an
    /// `Apply` verdict to the task (the public driver feeds it straight to
    /// [`RowOpTask::epq_result`]; `update_pk` also reads the new row value
    /// out of it). Returns the driver's stop outcome, or `None` to retry
    /// with the task re-based on `v`.
    fn epq_pass(
        &self,
        requested: RowLockMode,
        req: &EpqRequest,
        epq: &mut dyn Epq,
        task: &mut RowOpTask,
        fold: &mut dyn FnMut(&mut RowOpTask, EpqDecision) -> Result<(), TxnError>,
    ) -> Result<Option<RowOutcome>, TxnError> {
        if let Some(outcome) = Self::epq_pre_rules(requested, req)? {
            return Ok(Some(outcome));
        }
        match epq.recheck(&req.v) {
            EpqDecision::Skip => Ok(Some(RowOutcome::Skipped(SkipReason::EpqFailed))),
            EpqDecision::Apply(op) => {
                fold(task, EpqDecision::Apply(op))?;
                Ok(None)
            }
        }
    }

    /// The blocking row-op driver (§5.1): check the cancel flag, step, and
    /// on `Wait` park through [`Core::wait_on_any`]; on `Epq` run the §5.2
    /// pass; on `Again` retry. RR/SER serialization failures and every
    /// other §5 error propagate.
    pub fn row_op(
        &self,
        txn: &Txn,
        key: &[u8],
        latch_prefix: Option<usize>,
        op: RowOp,
        ctx: StmtCtx,
        epq: &mut dyn Epq,
    ) -> Result<RowOutcome, TxnError> {
        let deadline = ctx.lock_deadline();
        let mut task = RowOpTask::new(key, latch_prefix, op, ctx);
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            match task.step(self, txn)? {
                Step::Done(outcome) => return Ok(outcome),
                Step::Again => {}
                Step::Wait(targets) => {
                    map_wait_outcome(self.wait_on_any_deadline(txn, &targets, deadline))?;
                }
                Step::Epq(req) => {
                    let requested = task.requested_mode();
                    if let Some(outcome) =
                        self.epq_pass(requested, &req, epq, &mut task, &mut |t, d| t.epq_result(d))?
                    {
                        return Ok(outcome);
                    }
                }
                Step::Restart => {
                    return Err(TxnError::Invariant(
                        "the public row-op driver never returns Restart (C-T2c drives it)".into(),
                    ))
                }
            }
        }
    }

    /// The blocking key-existence driver (§5.1/§5.3): like
    /// [`Core::row_op`] but never EPQ (the unique check replaces the
    /// newer-version rule) and never skipping.
    pub fn insert_key(
        &self,
        txn: &Txn,
        key: &[u8],
        latch_prefix: Option<usize>,
        value: Vec<u8>,
        ctx: StmtCtx,
        unique: UniqueRule,
    ) -> Result<(), TxnError> {
        let deadline = ctx.lock_deadline();
        let mut task = KeyOpTask::new(key, latch_prefix, value, ctx, unique);
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            match task.step(self, txn)? {
                Step::Done(RowOutcome::Applied) => return Ok(()),
                Step::Done(_) => {
                    return Err(TxnError::Invariant(
                        "key-existence op skipped a row (§5.1 forbids it)".into(),
                    ))
                }
                Step::Again => {}
                Step::Wait(targets) => {
                    map_wait_outcome(self.wait_on_any_deadline(txn, &targets, deadline))?;
                }
                Step::Epq(_) | Step::Restart => {
                    return Err(TxnError::Invariant(
                        "key-existence op asked to EPQ or restart (§5.1 forbids it)".into(),
                    ))
                }
            }
        }
    }

    /// §5.0/§5.1/§5.2 primary-key change: a row op on `old_key` that writes
    /// `Delete { moved: true }` with requested mode `Update` (EPQ as usual:
    /// the caller's `epq` decides whether the row still qualifies — any
    /// `Apply` re-enters the loop as the moved delete), then a
    /// key-existence insert of `new_key` with `Unique { same_row: None }`.
    /// Resolution writes header 0x03 for the moved delete (§2.2).
    ///
    /// C-T2 rework 1: the EPQ pass runs through the full §5.2 rules
    /// (`epq_pass`: a moved tombstone is 40001, a plain tombstone skips and
    /// the new key is **not** inserted), and the inserted value comes from
    /// the `Apply(Update { value, .. })` the callback returned — computed
    /// from the newest version, never the stale snapshot value.
    pub fn update_pk(
        &self,
        txn: &Txn,
        old_key: &[u8],
        new_key: &[u8],
        value: Vec<u8>,
        ctx: StmtCtx,
        epq: &mut dyn Epq,
    ) -> Result<RowOutcome, TxnError> {
        let task = RowOpTask::new(old_key, None, RowOp::Delete, ctx.clone()).moved_delete();
        let mut new_value = value;
        let mut fold = |task: &mut RowOpTask, decision: EpqDecision| -> Result<(), TxnError> {
            match &decision {
                EpqDecision::Apply(RowOp::Update { value, .. }) => new_value = value.clone(),
                EpqDecision::Apply(other) => {
                    // Anything but an Update would leave the stale value
                    // on the new key (a lost update).
                    return Err(TxnError::Invariant(format!(
                        "update_pk: the EPQ callback must return Apply(Update), got {other:?}"
                    )));
                }
                EpqDecision::Skip => {}
            }
            // The old key keeps the moved delete whatever the callback
            // applied; only the re-base on the remembered version is
            // taken.
            task.epq_result(EpqDecision::Apply(RowOp::Delete))
        };
        match self.row_op_task(txn, task, epq, &mut fold)? {
            RowOutcome::Applied => {}
            skipped => return Ok(skipped),
        }
        self.insert_key(
            txn,
            new_key,
            None,
            new_value,
            ctx,
            UniqueRule::Unique { same_row: None },
        )?;
        Ok(RowOutcome::Applied)
    }

    /// The blocking driver for a pre-built (crate-private) task:
    /// `update_pk`'s moved delete and C-T2c's arbiter lock. The §5.2 EPQ
    /// pass runs through [`Core::row_op`]'s rules (`epq_pass`, rework 1 —
    /// never a bare `recheck`); `fold` applies an `Apply` verdict to the
    /// task, so `update_pk` can keep the moved delete and read the new row
    /// value out of the verdict.
    pub(crate) fn row_op_task(
        &self,
        txn: &Txn,
        mut task: RowOpTask,
        epq: &mut dyn Epq,
        fold: &mut dyn FnMut(&mut RowOpTask, EpqDecision) -> Result<(), TxnError>,
    ) -> Result<RowOutcome, TxnError> {
        let deadline = task.ctx.lock_deadline();
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            match task.step(self, txn)? {
                Step::Done(outcome) => return Ok(outcome),
                Step::Again => {}
                Step::Wait(targets) => {
                    map_wait_outcome(self.wait_on_any_deadline(txn, &targets, deadline))?
                }
                Step::Epq(req) => {
                    let requested = task.requested_mode();
                    if let Some(outcome) = self.epq_pass(requested, &req, epq, &mut task, fold)? {
                        return Ok(outcome);
                    }
                }
                Step::Restart => {
                    return Err(TxnError::Invariant(
                        "row_op_task never returns Restart (the arbiter lock driver is C-T2c's)"
                            .into(),
                    ))
                }
            }
        }
    }

    /// The newest committed data version of `key`, read from a registered
    /// view **without** the key's latch (§3.1: every KV read outside a
    /// latch section opens a registered view). Used by C-T2c's §5.3.1(1)
    /// pre-check to remember `v_r` (outside `r`'s latch); the value is
    /// re-verified under the latch by the lock step. Unit-tested in
    /// `write/tests`.
    #[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
    pub(crate) fn newest_committed_unlatched(
        &self,
        key: &[u8],
    ) -> Result<Option<CommittedVersion>, TxnError> {
        let view = self.open_view();
        let state = read_key_state(&view, key)?;
        Ok(state.newest_committed().cloned())
    }
}

/// The §5.3.1(1) pre-check step (crate-private, C-T2c): under
/// `latch(latch_key(a))`, read `a@INTENT` and its versions from one fresh
/// view: a foreign visible-committed or aborted intent is removed and the
/// check re-runs; a foreign Pending or committed-not-visible intent whose
/// top data is not `Absent` → wait on its owner (restart from 1 after the
/// wait); otherwise the key's current state (§5.3's rule) decides — a live
/// entry of another row is the conflict. Unit-tested in `write/tests`.
#[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
pub(crate) struct ArbiterPreCheck {
    key: nucleus_kv::Key,
    latch_prefix: Option<usize>,
    /// This row's entry payload: a live entry whose payload equals it
    /// belongs to this row, not a conflict.
    same_row: Option<Vec<u8>>,
}

/// The pre-check's outcome.
#[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ArbPreStep {
    /// No conflict: insert the proposed row (§5.3.1(2)).
    Insert,
    /// A live entry of another row `r` (§5.3.1(1) → (3)): the payload the
    /// entry names `r` by (the caller maps it to `r`'s `/t/` key).
    Conflict { entry_payload: Vec<u8> },
    /// A foreign intent was removed; re-run the pre-check.
    Again,
    /// Park on these `(txn, gen)` pairs, then re-run the pre-check.
    Wait(WaitSet),
}

impl ArbiterPreCheck {
    #[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
    pub(crate) fn new(
        key: &[u8],
        latch_prefix: Option<usize>,
        same_row: Option<Vec<u8>>,
    ) -> ArbiterPreCheck {
        ArbiterPreCheck {
            key: key.to_vec(),
            latch_prefix,
            same_row,
        }
    }

    #[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
    pub(crate) fn step<K: OrderedKv>(
        &self,
        core: &Core<K>,
        txn: &Txn,
    ) -> Result<ArbPreStep, TxnError> {
        let lk = latch_bytes(&self.key, self.latch_prefix);
        let latch = core.latches.lock(lk);
        let state = {
            let view = core.open_view();
            let state = read_key_state(&view, &self.key)?;
            drop(view);
            state
        };
        if let Some(intent) = &state.intent {
            if intent.txn != txn.id {
                let owner = intent.txn;
                let status = core.status.lookup_for_intent(owner)?;
                match classify_foreign(status, core.visible_ts()) {
                    Foreign::RemoveResolve | Foreign::RemoveDiscard => {
                        let mode = if matches!(status, TxnStatus::Committed(_)) {
                            RemovalMode::Resolve
                        } else {
                            RemovalMode::Discard
                        };
                        remove_intent_under_latch(
                            core,
                            &self.key,
                            latch_prefix_of(&self.key, self.latch_prefix),
                            owner,
                            mode,
                            &latch,
                        )?;
                        drop(latch);
                        return Ok(ArbPreStep::Again);
                    }
                    Foreign::Blocking => {
                        let top_absent = intent.top().is_some_and(|t| t.data == LayerData::Absent);
                        // §5.3.1(1): only a non-Absent foreign intent waits;
                        // a lock-only one does not block the state read.
                        if !top_absent {
                            let g = gen_of(core, owner);
                            drop(latch);
                            return Ok(ArbPreStep::Wait(vec![(owner, g)]));
                        }
                    }
                }
            }
        }
        drop(latch);
        // The key's current state (§5.3's unique-check rule): the own top
        // layer's data if it is Write/Delete, else the newest committed
        // data version. C-T2 rework 4: an own `Delete` layer means "not
        // live", exactly as in `unique_verdict` — the txn deleted the
        // entry, so a re-insert of the arbiter key is an `Insert`.
        let own = state.intent.as_ref().filter(|i| i.txn == txn.id);
        let live_payload: Option<Vec<u8>> = match own.and_then(|i| i.top()).map(|t| &t.data) {
            Some(LayerData::Write { value, .. }) => Some(value.clone()),
            // Own Delete → not live.
            Some(LayerData::Delete { .. }) => None,
            // Own Absent or no own intent → the newest committed version.
            _ => state
                .newest_committed()
                .and_then(|v| v.live_payload().map(|p| p.to_vec())),
        };
        match live_payload {
            Some(payload) if self.same_row.as_deref() != Some(payload.as_slice()) => {
                Ok(ArbPreStep::Conflict {
                    entry_payload: payload,
                })
            }
            _ => Ok(ArbPreStep::Insert),
        }
    }
}
