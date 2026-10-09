//! C-T0 §5: the write path's public types — the statement context, row ops
//! and their outcomes, the caller's EvalPlanQual callback, and the unique
//! rules. §-numbers refer to the protocol spec.

use crate::encoding::VersionValue;
use crate::txn::Isolation;
use crate::{RowLockMode, Seq, Ts, TxnId};

/// One statement's position, built only through [`StmtCtx::new`] and the
/// builder methods (§4, §5.4). Fields are private: C-T2b adds
/// `.lock_timeout(..)` without touching call sites.
///
/// - `snapshot` is the statement's `S` (RC: per statement; RR/SER: the
///   txn's). It is the base for the §5.1 newer-version rule until an EPQ
///   pass re-bases the task on the version it evaluated.
/// - `seq0` is the statement start: a statement never sees its own writes
///   with `seq >= seq0` (I-HALLOWEEN), and §5.4's revisit/27000 rules
///   compare against it.
/// - `seq` is the writing command's seq: `== seq0` for the statement's own
///   writes, a fresh seq for internal commands (FK checks, referential
///   actions, triggers, §5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StmtCtx {
    snapshot: Ts,
    seq0: Seq,
    seq: Seq,
    wait: LockWait,
    internal: bool,
    revisit_is_error: bool,
    /// The statement's `lock_timeout` (§6): past this deadline a blocking
    /// lock wait raises 55P03 instead of parking on. `None` (the default)
    /// waits indefinitely (C-T2b).
    lock_timeout: Option<std::time::Duration>,
    /// The seq the next layer places at (§2.1: `s = max(place_seq, top.seq)`)
    /// and the `data_seq` a data change records. Defaults: place at `seq`,
    /// `data_seq = seq0` (the statement's own write keeps `data_seq = seq0`
    /// even when a BEFORE trigger's later layer pushes `s` above it,
    /// PostgreSQL's `es_output_cid`). `.internal()` sets `data_seq = seq`
    /// (an internal command is its own writing command). The ON CONFLICT
    /// attempt override (§5.3.1) places at `sa` with `data_seq = seq0`
    /// (crate-private, C-T2c).
    place_seq: Seq,
    data_seq: Seq,
}

impl StmtCtx {
    pub fn new(snapshot: Ts, seq0: Seq, seq: Seq) -> StmtCtx {
        StmtCtx {
            snapshot,
            seq0,
            seq,
            wait: LockWait::Block,
            internal: false,
            revisit_is_error: false,
            lock_timeout: None,
            place_seq: seq,
            data_seq: seq0,
        }
    }

    /// The statement's `lock_timeout` (§6, C-T2b): a blocking lock wait
    /// past `timeout` raises 55P03 (`LockNotAvailable`) instead of parking
    /// on. The deadline starts when the wait begins.
    pub fn lock_timeout(mut self, timeout: std::time::Duration) -> StmtCtx {
        self.lock_timeout = Some(timeout);
        self
    }

    /// `NOWAIT`: a conflicting lock raises 55P03 instead of waiting (§6).
    pub fn nowait(mut self) -> StmtCtx {
        self.wait = LockWait::NoWait;
        self
    }

    /// `SKIP LOCKED`: a conflicting row is skipped instead of waiting (§6).
    pub fn skip_locked(mut self) -> StmtCtx {
        self.wait = LockWait::SkipLocked;
        self
    }

    /// Marks an internal command (FK check, referential action, trigger,
    /// §5.3): it runs at its own fresh `seq`, so §5.4's revisit/27000 rules
    /// do not apply, and its data changes record `data_seq = seq`.
    pub fn internal(mut self) -> StmtCtx {
        self.internal = true;
        self.data_seq = self.seq;
        self
    }

    /// A same-statement revisit raises 21000 instead of skipping (§5.4):
    /// `INSERT ... ON CONFLICT DO UPDATE`, MERGE matched actions.
    pub fn revisit_is_error(mut self) -> StmtCtx {
        self.revisit_is_error = true;
        self
    }

    /// The ON CONFLICT attempt override (§5.3.1, crate-private for C-T2c):
    /// the attempt's writes place at `sa` (a fresh seq), with
    /// `data_seq = seq0` (`es_output_cid`). Unit-tested in `write/tests`;
    /// C-T2c's `write/on_conflict.rs` is its first caller.
    #[allow(dead_code)] // C-T2c; exercised by the unit tests in write::tests
    pub(crate) fn attempt(mut self, sa: Seq) -> StmtCtx {
        self.place_seq = sa;
        self.data_seq = self.seq0;
        self
    }

    pub(crate) fn snapshot(&self) -> Ts {
        self.snapshot
    }
    pub(crate) fn seq0(&self) -> Seq {
        self.seq0
    }
    pub(crate) fn seq(&self) -> Seq {
        self.seq
    }
    pub(crate) fn place_seq(&self) -> Seq {
        self.place_seq
    }
    pub(crate) fn data_seq(&self) -> Seq {
        self.data_seq
    }
    pub(crate) fn is_internal(&self) -> bool {
        self.internal
    }
    pub(crate) fn wants_revisit_error(&self) -> bool {
        self.revisit_is_error
    }
    pub(crate) fn wait(&self) -> LockWait {
        self.wait
    }
    /// The `lock_timeout` deadline (§6): `Instant::now() + timeout` when the
    /// statement set one. Crate-private: the blocking drivers pass it into
    /// the wait (C-T2b's third allowed change in this module).
    pub(crate) fn lock_deadline(&self) -> Option<std::time::Instant> {
        self.lock_timeout.map(|t| std::time::Instant::now() + t)
    }
}

/// How a conflicting lock is handled (§6: NOWAIT / SKIP LOCKED / wait).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockWait {
    /// Park on the holders (the default).
    Block,
    /// 55P03 instead of waiting.
    NoWait,
    /// Skip the row.
    SkipLocked,
}

/// One operation on a row the statement found (§5.0 "row ops").
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowOp {
    /// UPDATE: the caller's new row value, computed from the version it
    /// read (or the EPQ version). `key_cols_changed` is the caller's
    /// statement that a key column (§1) differs from the newest committed
    /// version — it decides the requested mode (`Update` vs `NoKeyUpdate`)
    /// and `key_changed` (§2.1).
    Update {
        value: Vec<u8>,
        key_cols_changed: bool,
    },
    /// DELETE (a PK change's old-key delete is `moved` internally,
    /// [`Core::update_pk`](crate::boot::Core::update_pk)).
    Delete,
    /// An exclusive (`NoKeyUpdate`, `Update`) or shared (`KeyShare`,
    /// `Share`) lock-only request. Shared requests are granted in the
    /// [`RowLocks`](crate::write::RowLocks) table and never write an
    /// intent (§2.1, §6); exclusive requests build a lock-only layer.
    Lock(RowLockMode),
}

impl RowOp {
    /// The requested row-lock mode (§6): UPDATE without a key change →
    /// `NoKeyUpdate`, with → `Update`; DELETE → `Update`; inserts and
    /// unique entries → `NoKeyUpdate`; explicit locks → their mode.
    pub(crate) fn requested_mode(&self) -> RowLockMode {
        match self {
            RowOp::Update {
                key_cols_changed, ..
            } => {
                if *key_cols_changed {
                    RowLockMode::Update
                } else {
                    RowLockMode::NoKeyUpdate
                }
            }
            RowOp::Delete => RowLockMode::Update,
            RowOp::Lock(m) => *m,
        }
    }

    /// A data-changing row op of the statement (§5.4): the revisit and
    /// 27000 rules apply only to these.
    pub(crate) fn is_data_row_op(&self) -> bool {
        matches!(self, RowOp::Update { .. } | RowOp::Delete)
    }
}

/// What a row op did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowOutcome {
    /// The layer was placed (or the shared lock granted).
    Applied,
    /// The row was skipped, and why (§5.2, §5.4, §6).
    Skipped(SkipReason),
}

/// Why a row op skipped its row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// EPQ re-checked the quals against the newest version and they failed
    /// (§5.2), or the row is gone.
    EpqFailed,
    /// The statement already modified this row (§5.4 revisit).
    SelfModified,
    /// SKIP LOCKED found a conflicting holder (§6).
    Locked,
}

/// The decoded §2.2 version a step remembered for EPQ: its ts and value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedVersion {
    pub ts: Ts,
    pub value: VersionValue,
}

impl CommittedVersion {
    pub(crate) fn is_tombstone(&self) -> bool {
        matches!(self.value, crate::encoding::VersionValue::Tombstone { .. })
    }
    pub(crate) fn is_moved_tombstone(&self) -> bool {
        matches!(
            self.value,
            crate::encoding::VersionValue::Tombstone { moved: true }
        )
    }
    pub(crate) fn is_key_changed_write(&self) -> bool {
        matches!(
            self.value,
            crate::encoding::VersionValue::Live {
                key_changed: true,
                ..
            }
        )
    }
    #[allow(dead_code)] // C-T2c's pre-check reads it; unit-tested in write::tests
    pub(crate) fn live_payload(&self) -> Option<&[u8]> {
        match &self.value {
            crate::encoding::VersionValue::Live { payload, .. } => Some(payload),
            crate::encoding::VersionValue::Tombstone { .. } => None,
        }
    }
}

/// The caller's EvalPlanQual (§5.2): re-evaluate the statement's quals for
/// this row against `newest` (the newest committed data version the step
/// remembered), and compute the new op from it when they pass. Runs between
/// steps, **never under a latch** (it may run subqueries and functions).
pub trait Epq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision;
}

/// [`Epq::recheck`]'s verdict.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EpqDecision {
    /// The quals fail: skip the row.
    Skip,
    /// The quals pass: apply this op instead, computed from `newest`.
    Apply(RowOp),
}

/// The unique rule of a key-existence op (§5.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UniqueRule {
    /// No per-row unique check (the op still runs the full §5.1 loop).
    None,
    /// A unique key (`/t/` primary key, `/u/` entry): live and not
    /// `same_row` (the live value equals `same_row`) → 23505, or 40001
    /// under SERIALIZABLE with a covering SIREAD (§5.3). `same_row = None`
    /// for a primary-key insert.
    Unique { same_row: Option<Vec<u8>> },
    /// A deferrable `/i/{idx}/{key}{pk}` entry: placement skips the per-row
    /// check (G0-write's `is_defer_op`); the SQL layer checks the prefix at
    /// end of statement or commit through
    /// [`Core::check_deferrable_unique`](crate::boot::Core::check_deferrable_unique).
    Deferrable,
}

/// §3.2/§5.1: how a foreign intent's owner is treated. Only
/// `Committed(c)` with `c <= visible_ts` resolves; `Committed(c > visible_ts)`
/// and `Pending` block (a committed-but-not-visible txn is exactly like
/// `Pending` to every writer-side decision).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Foreign {
    /// The owner is a visible commit: remove the intent per §7.3 (writing
    /// its version), then retry the step.
    RemoveResolve,
    /// The owner aborted: remove the intent per §7.3 (writing nothing),
    /// then retry.
    RemoveDiscard,
    /// Pending or committed-not-visible: wait (when conflicting) or pass.
    Blocking,
}

/// The pure §5.1 classification of a foreign intent's owner.
pub(crate) fn classify_foreign(status: crate::TxnStatus, visible_ts: Ts) -> Foreign {
    use crate::TxnStatus::*;
    match status {
        Committed(c) if c <= visible_ts => Foreign::RemoveResolve,
        Aborted => Foreign::RemoveDiscard,
        Pending | Committed(_) => Foreign::Blocking,
    }
}

/// `true` when the isolation level is RR or SERIALIZABLE (the §5.1
/// newer-version rule raises 40001 instead of running EPQ).
pub(crate) fn is_snapshot_iso(iso: Isolation) -> bool {
    matches!(iso, Isolation::RepeatableRead | Isolation::Serializable)
}

/// What a step remembered for the caller's §5.2 EvalPlanQual pass: the
/// newest committed data version `v`, and for a KEY SHARE request every
/// committed data version with `ts > S` (§5.1/§5.2: KEY SHARE examines all
/// of them, seed 45).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EpqRequest {
    pub v: CommittedVersion,
    pub above_s: Vec<CommittedVersion>,
}

/// Remembers which txn and wake generation a step waits on (§5.1): the
/// foreign intent's owner, or every conflicting shared-lock holder.
pub type WaitSet = Vec<(TxnId, u64)>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classify_foreign_seed18() {
        // seed 18: a committed-but-not-visible owner blocks (I-RC-MONO).
        assert_eq!(
            classify_foreign(crate::TxnStatus::Committed(Ts(9)), Ts(7)),
            Foreign::Blocking
        );
        assert_eq!(
            classify_foreign(crate::TxnStatus::Committed(Ts(7)), Ts(7)),
            Foreign::RemoveResolve
        );
        assert_eq!(
            classify_foreign(crate::TxnStatus::Committed(Ts(6)), Ts(7)),
            Foreign::RemoveResolve
        );
        assert_eq!(
            classify_foreign(crate::TxnStatus::Pending, Ts(7)),
            Foreign::Blocking
        );
        assert_eq!(
            classify_foreign(crate::TxnStatus::Aborted, Ts(7)),
            Foreign::RemoveDiscard
        );
    }

    #[test]
    fn internal_ctx_writes_data_seq_at_its_own_seq() {
        let ctx = StmtCtx::new(Ts(5), 3, 7).internal();
        assert_eq!(ctx.place_seq(), 7);
        assert_eq!(
            ctx.data_seq(),
            7,
            "an internal command is its own writing command"
        );
        let plain = StmtCtx::new(Ts(5), 3, 3);
        assert_eq!(plain.data_seq(), 3);
        let attempt = StmtCtx::new(Ts(5), 3, 3).attempt(9);
        assert_eq!(attempt.place_seq(), 9, "the attempt places at sa");
        assert_eq!(attempt.data_seq(), 3, "the attempt's data_seq stays seq0");
    }
}
