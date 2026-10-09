//! C-T0 §5.3.1: the `INSERT ... ON CONFLICT` arbiter protocol (PostgreSQL's
//! speculative insertion), built on C-T2's step APIs: the pre-check step
//! ([`ArbiterPreCheck`]), key-existence inserts ([`KeyOpTask`]), the arbiter
//! lock ([`RowOpTask::arbiter_lock`]), `rollback_to` and `wait_on_any`.
//!
//! Every attempt takes an internal savepoint at a fresh seq `sa`; the
//! attempt's writes place at `sa` with `data_seq = seq0`
//! ([`StmtCtx::attempt`], seed 56), and every restart rolls back to `sa`
//! (seed 54), which also discards the checks and AFTER events the SQL layer
//! queued for the attempt under the tag `sa` it was handed (seed 64).

use nucleus_kv::{Key, OrderedKv};

use crate::boot::Core;
use crate::txn::Txn;
use crate::write::ctx::{is_snapshot_iso, WaitSet};
use crate::write::step::{map_wait_outcome, read_key_state, ArbPreStep, ArbiterPreCheck};
use crate::write::{KeyOpTask, RowOp, RowOpTask, RowOutcome, Step, StmtCtx, UniqueRule};
use crate::{LayerData, RowLockMode, Seq, Ts, TxnError};

/// One unique-index entry of a proposed row (`/u/{idx}/{key}`), whose value
/// names the row (the caller's `t_key_of` maps it back to the `/t/` key).
/// `arbiter` marks an entry of the statement's arbiter set `A`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub key: Key,
    pub value: Vec<u8>,
    pub arbiter: bool,
}

/// The row `INSERT ... ON CONFLICT` proposes: its `/t/` key and value and
/// every unique entry (arbiter or not).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProposedRow {
    pub t_key: Key,
    pub value: Vec<u8>,
    pub entries: Vec<IndexEntry>,
}

/// The conflict action. `DoUpdate`'s closure gets the locked row value and
/// returns the new value and `key_cols_changed`, or `None` for a false
/// `DO UPDATE ... WHERE`. It runs outside any latch.
pub enum OnConflictAction<'a> {
    DoNothing,
    DoUpdate(&'a mut DoUpdateFn<'a>),
}

/// The `DO UPDATE` closure: locked row value → `Some((new value,
/// key_cols_changed))`, or `None` for a false `WHERE`.
pub type DoUpdateFn<'a> = dyn FnMut(&[u8]) -> Option<(Vec<u8>, bool)> + 'a;

/// What happened to the proposed row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnConflictResult {
    /// No conflict: the row and its entries were inserted.
    Inserted,
    /// The conflicting row was locked and updated.
    Updated,
    /// `DO NOTHING` on a live conflict.
    Nothing,
    /// `DO UPDATE ... WHERE` was false on the locked row (the lock stays).
    WhereFalse,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnConflictOutcome {
    pub result: OnConflictResult,
    /// Attempts restarted from §5.3.1(1). Each follows a wait or an observed
    /// change of committed state.
    pub restarts: u32,
}

/// How one attempt ended.
enum Attempt {
    Done(OnConflictResult),
    /// Abandon: roll back to `sa`, park on the wait set if any, restart.
    Restart(Option<WaitSet>),
}

impl<K: OrderedKv> Core<K> {
    /// §5.3.1 for one proposed row. `ctx` is the statement's context (`seq0`
    /// = statement start); every attempt starts with `sa = txn.next_seq()`
    /// and `on_attempt(sa)`, so the SQL layer tags the attempt's queued
    /// checks and AFTER events with `sa` (through
    /// [`Txn::queue_event`](crate::txn::Txn::queue_event)).
    pub fn insert_on_conflict(
        &self,
        txn: &Txn,
        ctx: StmtCtx,
        row: ProposedRow,
        t_key_of: &dyn Fn(&[u8]) -> Key,
        on_attempt: &mut dyn FnMut(Seq),
        mut action: OnConflictAction<'_>,
    ) -> Result<OnConflictOutcome, TxnError> {
        let mut restarts = 0u32;
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            let sa = txn.next_seq()?;
            on_attempt(sa);
            let actx = ctx.clone().attempt(sa);
            match self.arbiter_attempt(txn, &actx, &row, t_key_of, &mut action)? {
                Attempt::Done(result) => return Ok(OnConflictOutcome { result, restarts }),
                Attempt::Restart(wait) => {
                    // Abandon exactly as ROLLBACK TO SAVEPOINT sa (§5.5):
                    // only the attempt's layers go, its events are
                    // discarded, the wake generation is bumped.
                    self.rollback_to(txn, sa)?;
                    if let Some(targets) = wait {
                        map_wait_outcome(self.wait_on_any(txn, &targets))?;
                    }
                    restarts = restarts.saturating_add(1);
                }
            }
        }
    }

    fn arbiter_attempt(
        &self,
        txn: &Txn,
        actx: &StmtCtx,
        row: &ProposedRow,
        t_key_of: &dyn Fn(&[u8]) -> Key,
        action: &mut OnConflictAction<'_>,
    ) -> Result<Attempt, TxnError> {
        // (1) Pre-check, one latch section per arbiter key.
        for entry in row.entries.iter().filter(|e| e.arbiter) {
            let pre = ArbiterPreCheck::new(&entry.key, None, Some(entry.value.clone()));
            loop {
                match pre.step(self, txn)? {
                    ArbPreStep::Again => {}
                    ArbPreStep::Wait(targets) => return Ok(Attempt::Restart(Some(targets))),
                    ArbPreStep::Insert => break,
                    ArbPreStep::Conflict { entry_payload } => {
                        let r = t_key_of(&entry_payload);
                        return self.arbiter_conflict(txn, actx, &r, action);
                    }
                }
            }
        }
        // (2) Insert.
        self.arbiter_insert(txn, actx, row)
    }

    /// §5.3.1(2): the row and every entry as key-existence ops at `sa`. An
    /// arbiter key's unique conflict, or any wait, abandons the attempt.
    fn arbiter_insert(
        &self,
        txn: &Txn,
        actx: &StmtCtx,
        row: &ProposedRow,
    ) -> Result<Attempt, TxnError> {
        let mut ops: Vec<(KeyOpTask, bool)> = Vec::with_capacity(row.entries.len() + 1);
        ops.push((
            KeyOpTask::new(
                &row.t_key,
                None,
                row.value.clone(),
                actx.clone(),
                UniqueRule::Unique { same_row: None },
            ),
            false,
        ));
        for e in &row.entries {
            ops.push((
                KeyOpTask::new(
                    &e.key,
                    None,
                    e.value.clone(),
                    actx.clone(),
                    UniqueRule::Unique {
                        same_row: Some(e.value.clone()),
                    },
                ),
                e.arbiter,
            ));
        }
        for (mut task, arbiter) in ops {
            loop {
                match task.step(self, txn) {
                    Ok(Step::Done(RowOutcome::Applied)) => break,
                    Ok(Step::Again) => {}
                    Ok(Step::Wait(targets)) => return Ok(Attempt::Restart(Some(targets))),
                    Ok(other) => {
                        return Err(TxnError::Invariant(format!(
                            "ON CONFLICT insert step returned {other:?} (§5.1 forbids it)"
                        )))
                    }
                    Err(TxnError::UniqueViolation) if arbiter => return Ok(Attempt::Restart(None)),
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(Attempt::Done(OnConflictResult::Inserted))
    }

    /// §5.3.1(3): conflict on row `r`.
    fn arbiter_conflict(
        &self,
        txn: &Txn,
        actx: &StmtCtx,
        r: &[u8],
        action: &mut OnConflictAction<'_>,
    ) -> Result<Attempt, TxnError> {
        // v_r and r's own top layer from one registered view (outside r's
        // latch, §3.1).
        let (v_r, own_top) = {
            let view = self.open_view();
            let state = read_key_state(&view, r)?;
            let own_top = state
                .intent
                .as_ref()
                .filter(|i| i.txn == txn.id)
                .and_then(|i| i.top().cloned());
            (state.newest_committed().cloned(), own_top)
        };
        if is_snapshot_iso(txn.isolation) && v_r.as_ref().is_some_and(|v| v.ts > actx.snapshot()) {
            return Err(TxnError::SerializationFailure);
        }
        let update = match action {
            OnConflictAction::DoNothing => return Ok(Attempt::Done(OnConflictResult::Nothing)),
            OnConflictAction::DoUpdate(f) => f,
        };
        if own_top.is_some_and(|t| t.data_seq == actx.seq0()) {
            return Err(TxnError::CardinalityViolation);
        }

        // Lock r at base v_r.ts, never EPQ: a newer version restarts.
        let base = v_r.as_ref().map_or(Ts::ZERO, |v| v.ts);
        let mut lock = RowOpTask::new(r, None, RowOp::Lock(RowLockMode::NoKeyUpdate), actx.clone())
            .arbiter_lock(base);
        loop {
            match lock.step(self, txn)? {
                Step::Done(RowOutcome::Applied) => break,
                Step::Again => {}
                Step::Wait(targets) => map_wait_outcome(self.wait_on_any(txn, &targets))?,
                Step::Restart => return Ok(Attempt::Restart(None)),
                other => {
                    return Err(TxnError::Invariant(format!(
                        "ON CONFLICT arbiter lock returned {other:?}"
                    )))
                }
            }
        }

        // Locked: the own intent's data if any, else v_r (verified under
        // the latch: no data version newer than v_r.ts).
        let own_data = {
            let view = self.open_view();
            let state = read_key_state(&view, r)?;
            state
                .intent
                .filter(|i| i.txn == txn.id)
                .and_then(|i| i.layers.last().map(|l| l.data.clone()))
        };
        let value: Vec<u8> = match own_data {
            Some(LayerData::Write { value, .. }) => value,
            Some(LayerData::Delete { .. }) => {
                return Err(TxnError::Invariant(
                    "ON CONFLICT: a live arbiter entry names a row this txn deleted".into(),
                ))
            }
            Some(LayerData::Absent) | None => {
                match v_r.as_ref().and_then(|v| v.live_payload()) {
                    Some(p) => p.to_vec(),
                    // r died between the pre-check and v_r's read (a
                    // committed change): restart from (1).
                    None => return Ok(Attempt::Restart(None)),
                }
            }
        };

        let Some((new_value, key_cols_changed)) = update(&value) else {
            return Ok(Attempt::Done(OnConflictResult::WhereFalse));
        };
        let mut task = RowOpTask::new(
            r,
            None,
            RowOp::Update {
                value: new_value,
                key_cols_changed,
            },
            actx.clone().revisit_is_error(),
        );
        loop {
            match task.step(self, txn)? {
                Step::Done(RowOutcome::Applied) => {
                    return Ok(Attempt::Done(OnConflictResult::Updated))
                }
                Step::Again => {}
                // Shared holders (KEY SHARE vs a key-changing update): the
                // own intent excludes foreign writers, so wait and re-step.
                Step::Wait(targets) => map_wait_outcome(self.wait_on_any(txn, &targets))?,
                other => {
                    return Err(TxnError::Invariant(format!(
                        "ON CONFLICT update of the locked row returned {other:?}"
                    )))
                }
            }
        }
    }
}
