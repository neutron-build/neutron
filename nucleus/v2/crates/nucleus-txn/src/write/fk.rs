//! C-T0 §5.3: foreign-key checks, run as internal commands (the caller
//! passes a `StmtCtx` built with `.internal()` and a fresh `seq`). Every
//! read opens a registered view (§3.1, seed 44); the child side's KEY SHARE
//! on the parent goes through C-T2's row-op step, and a failed EPQ for that
//! lock raises 23503, never skips (seed 58).

use std::ops::Bound;

use nucleus_kv::{Key, OrderedKv};

use crate::boot::Core;
use crate::read::{read_key, scan, NoSsi};
use crate::txn::{Isolation, Txn};
use crate::visibility::ReadCtx;
use crate::write::ctx::{is_snapshot_iso, EpqRequest};
use crate::write::step::map_wait_outcome;
use crate::write::{EpqDecision, RowOp, RowOpTask, RowOutcome, Step, StmtCtx};
use crate::{RowLockMode, TxnError};

/// The parent-side referential action being checked (§5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FkParentMode<'a> {
    /// NO ACTION: a live parent with the old key in the same state
    /// (`[parent_lo, parent_hi)`) satisfies the check (`ri_Check_Pk_Match`).
    NoAction {
        parent_lo: &'a [u8],
        parent_hi: &'a [u8],
    },
    /// RESTRICT: any live child is 23503.
    Restrict,
}

fn require_internal(ctx: &StmtCtx) -> Result<(), TxnError> {
    if ctx.is_internal() {
        Ok(())
    } else {
        Err(TxnError::Invariant(
            "FK checks run as internal commands (§5.3): build the ctx with .internal()".into(),
        ))
    }
}

/// The child side's EPQ verdict for the KEY SHARE on the parent (§5.3): a
/// moved parent is 40001; a tombstone, a version above `S` that is a
/// tombstone or a key_changed write, or a version that no longer matches is
/// 23503 — never a skip (seed 58).
fn fk_epq(req: &EpqRequest, matches: &dyn Fn(&[u8]) -> bool) -> Result<(), TxnError> {
    if req.v.is_moved_tombstone() || req.above_s.iter().any(|v| v.is_moved_tombstone()) {
        return Err(TxnError::SerializationFailure);
    }
    if req
        .above_s
        .iter()
        .any(|v| v.is_tombstone() || v.is_key_changed_write())
    {
        return Err(TxnError::ForeignKeyViolation);
    }
    match req.v.live_payload() {
        Some(p) if matches(p) => Ok(()),
        _ => Err(TxnError::ForeignKeyViolation),
    }
}

impl<K: OrderedKv> Core<K> {
    /// §5.3 child side (insert, or FK-changing update, of a child row):
    /// read the parent at `ctx`'s snapshot plus own writes (`stmt_seq =
    /// ctx.seq`), then take KEY SHARE on `parent_key` as a row op.
    pub fn fk_check_child(
        &self,
        txn: &Txn,
        ctx: &StmtCtx,
        parent_key: &[u8],
        matches: &dyn Fn(&[u8]) -> bool,
    ) -> Result<(), TxnError> {
        require_internal(ctx)?;
        if txn.isolation == Isolation::Serializable {
            // §8.1: the SIREAD before the view opens.
            self.ssi_hook().before_point_read(txn.id, parent_key);
        }
        let found = {
            let view = self.open_view();
            read_key(
                self,
                &view,
                parent_key,
                &ReadCtx {
                    txn: txn.id,
                    snapshot: ctx.snapshot(),
                    stmt_seq: ctx.seq(),
                },
                &mut NoSsi,
            )?
        };
        match found {
            Some(v) if matches(&v) => {}
            _ => return Err(TxnError::ForeignKeyViolation),
        }
        let mut task = RowOpTask::new(
            parent_key,
            None,
            RowOp::Lock(RowLockMode::KeyShare),
            ctx.clone(),
        );
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            match task.step(self, txn)? {
                Step::Done(RowOutcome::Applied) => return Ok(()),
                Step::Again => {}
                Step::Wait(targets) => {
                    map_wait_outcome(self.wait_on_any_deadline(txn, &targets, ctx.lock_deadline()))?
                }
                Step::Epq(req) => {
                    fk_epq(&req, matches)?;
                    task.epq_result(EpqDecision::Apply(RowOp::Lock(RowLockMode::KeyShare)))?;
                }
                other => {
                    return Err(TxnError::Invariant(format!(
                        "FK KEY SHARE on the parent returned {other:?}"
                    )))
                }
            }
        }
    }

    /// §5.3 parent side (delete, or key change of a referenced key): scan
    /// the children `[child_lo, child_hi)` in the latest committed state
    /// plus own writes. No live child → ok. NO ACTION: a live parent in
    /// `[parent_lo, parent_hi)` in the same state → ok. Then, under RR/SER,
    /// a live child invisible at `ctx`'s snapshot → 40001
    /// (`detectNewRows`); otherwise 23503.
    pub fn fk_check_parent(
        &self,
        txn: &Txn,
        ctx: &StmtCtx,
        child_lo: &[u8],
        child_hi: &[u8],
        mode: FkParentMode<'_>,
    ) -> Result<(), TxnError> {
        require_internal(ctx)?;
        let children: Vec<Key> = {
            let latest = self.registry.take_snapshot();
            let rctx = ReadCtx {
                txn: txn.id,
                snapshot: latest.ts(),
                stmt_seq: ctx.seq(),
            };
            let view = self.open_view();
            let mut obs = NoSsi;
            let children = scan(
                self,
                &view,
                (Bound::Included(child_lo), Bound::Excluded(child_hi)),
                &rctx,
                &mut obs,
            )
            .map(|r| r.map(|(k, _)| k))
            .collect::<Result<Vec<Key>, TxnError>>()?;
            if children.is_empty() {
                return Ok(());
            }
            if let FkParentMode::NoAction {
                parent_lo,
                parent_hi,
            } = mode
            {
                let mut obs = NoSsi;
                let mut parents = scan(
                    self,
                    &view,
                    (Bound::Included(parent_lo), Bound::Excluded(parent_hi)),
                    &rctx,
                    &mut obs,
                );
                if let Some(p) = parents.next() {
                    p?;
                    return Ok(());
                }
            }
            children
        };
        if is_snapshot_iso(txn.isolation) {
            let view = self.open_view();
            let at_s = ReadCtx {
                txn: txn.id,
                snapshot: ctx.snapshot(),
                stmt_seq: ctx.seq(),
            };
            for child in &children {
                if read_key(self, &view, child, &at_s, &mut NoSsi)?.is_none() {
                    return Err(TxnError::SerializationFailure);
                }
            }
        }
        Err(TxnError::ForeignKeyViolation)
    }
}
