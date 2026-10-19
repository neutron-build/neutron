//! Shared txn components used by **every** E1′ arm (card X-1a Work 3;
//! PLAN-v3 §2 N2, §3.4). Arms must use these, not their own copies, so the
//! measured difference between arms (b) and (c) is the version layout
//! alone.
//!
//! Invariants owned by this module tree:
//! - C-COMMIT-ORDER: a commit assigns its ts, applies its versions, marks
//!   the ts applied, and only then releases the txn's locks
//!   ([`commit::commit_with`]; PLAN-v3 §3.4; C-T0 §3 step 5).
//! - C-VISIBLE-PREFIX: `visible_ts` is the contiguous applied prefix of
//!   commit ts's — it never jumps past a commit that has a ts but is not
//!   yet applied ([`sequencer::Sequencer`]).
//! - C-SNAP-REG: a snapshot ts is registered in the snapshot registry in
//!   the same critical section in which it is read from the sequencer, so
//!   the GC watermark can never pass above a snapshot a reader is about to
//!   use ([`snapshots::Snapshots::register`]; C-T0 §3.1 I-SNAP-ORDER).
//! - C-GC-SAFE: [`snapshots::Snapshots::oldest`] is the only GC watermark;
//!   keeping every version above it plus the newest at or below it
//!   preserves every readable version (C-T0 §9.1, §9.2 I-GC).
//! - C-LOCK-EXCL: no two txns hold the same key, and a table-wide
//!   escalated owner excludes every row ([`lock_table::LockTable`]; C-T0
//!   §5.1 I-LOCK).
//!
//! Lock order (C-T0 §3.1): snapshots → sequencer for registration and
//! watermarking; the lock table's internal order is table-owner → per-txn
//! index → shard (see [`lock_table`]).

pub mod commit;
pub mod lock_table;
pub mod sequencer;
pub mod snapshots;
pub mod write_set;

pub use commit::commit_with;
pub use lock_table::{DEFAULT_ESCALATE_AT, LockTable, Owner, SHARDS};
pub use sequencer::Sequencer;
pub use snapshots::{SnapshotGuard, Snapshots};
pub use write_set::WriteSet;

use std::sync::{Mutex, MutexGuard, PoisonError};

/// Lock a mutex whose critical sections are panic-free by construction.
///
/// Every critical section in this module tree is infallible (no indexing,
/// no unwrapping), so the poisoned case is unreachable; `into_inner`
/// still makes this total without `unwrap` (PLAN-v3 §6.1: no panics on
/// any engine path).
pub(crate) fn lock_unpoisoned<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    match m.lock() {
        Ok(g) => g,
        Err(e) => e.into_inner(),
    }
}

/// `true` when the guard's mutex was poisoned (unreachable; used only to
/// keep error mapping honest in the lock table).
pub(crate) fn was_poisoned<T>(e: &PoisonError<T>) -> String {
    format!("mutex poisoned: {e}")
}
