//! C-T0 §5: the write path — the §5.1 placement loop as **non-blocking
//! steps** (one latch section per step, never parks) plus blocking drivers,
//! intent layers (§2.1), row ops and exclusive/shared lock requests, key
//! existence ops and unique checks (§5.3, deferrable included), primary-key
//! change (§5.0–§5.2), RC EvalPlanQual (§5.2), own-row rules (§5.4) and
//! savepoints with `ROLLBACK TO` (§5.5).
//!
//! Steps exist so tests and the deterministic simulator (C-SIM) can
//! interleave txns on one thread: a [`RowOpTask`]/[`KeyOpTask`] consumes at
//! most one latch section per `step`, returns what it needs next (retry,
//! wait, EPQ) and never parks. The blocking drivers
//! ([`Core::row_op`](crate::boot::Core::row_op),
//! [`Core::insert_key`](crate::boot::Core::insert_key),
//! [`Core::update_pk`](crate::boot::Core::update_pk),
//! [`Core::check_deferrable_unique`](crate::boot::Core::check_deferrable_unique))
//! loop the steps, parking through
//! [`Core::wait_on_any`](crate::wait::Core::wait_on_any).
//!
//! This card creates the seams C-T2b (locks, deadlock) and C-T3 (SSI) plug
//! into: the [`RowLocks`] and [`SsiHook`] traits installed on
//! [`Core`](crate::boot::Core), the step APIs, `commit_submit`'s ticket and
//! `map_wait_outcome`. It does **not** implement the shared-lock table (only
//! [`NoRowLocks`] and the tests' double), the wait-for graph, deadlock
//! detection, `lock_timeout`, SSI state or GC.
//!
//! Not in this module's scope (later cards): the ON CONFLICT arbiter
//! protocol §5.3.1 and FK checks §5.3 (C-T2c, built on the crate-private
//! step overrides here: `StmtCtx::attempt`, `RowOpTask::arbiter_lock`,
//! [`ArbiterPreCheck`], [`Core::newest_committed_unlatched`]), write-set
//! spilling, non-unique (non-deferrable) index entries, cursors/portals,
//! triggers and constraint timing (the SQL layer calls the checks at the
//! right time, through [`Txn::queue_event`](crate::txn::Txn::queue_event)).

mod ctx;
mod layer;
mod savepoint;
mod step;
mod unique;

pub(crate) mod rowlocks_api;
pub use rowlocks_api::{NoRowLocks, RowLocks};

pub(crate) mod ssi_api;
pub use ssi_api::{NoSsiHook, SsiHook};

pub use ctx::{
    CommittedVersion, Epq, EpqDecision, EpqRequest, LockWait, RowOp, RowOutcome, SkipReason,
    StmtCtx, UniqueRule, WaitSet,
};
pub use step::{KeyOpTask, RowOpTask, Step};
pub use unique::{DefStep, DeferrableCheckTask};
#[cfg(test)]
mod tests;
