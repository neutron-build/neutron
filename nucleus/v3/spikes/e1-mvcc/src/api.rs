//! The binding trait of the E1′ experiment (card X-1a Work 2; PLAN-v3 §4
//! E1′, §2 N3).
//!
//! Arms (b) (timestamp-in-key, card X-2) and (c) (newest-in-place + undo,
//! card X-3) implement exactly this surface, so the behaviour suite
//! ([`crate::suite`]) and the measurement driver can treat them alike.
//! The isolation semantics are PLAN-v3 N5: ReadCommitted with EPQ
//! (C-T0 §5.2) and RepeatableRead = SI (C-T0 §4). SERIALIZABLE/SSI is out
//! of scope for E1′ (its SIREAD tracking does not depend on the version
//! layout; card Goal).
//!
//! Invariants owned here (checked by the suite on every arm):
//! - A-API: arms add no behaviour the trait does not name; `put` is
//!   `update_with` with a constant `Some(value)` closure, so every
//!   conflict rule of [`TxnEngine::update_with`] applies to it too.
//! - A-OWN: reads inside one txn see that txn's own writes (including its
//!   own deletes) merged over the committed state at the txn's snapshot
//!   (C-T0 §4 step 1, `T == R`).
//! - A-RC: RC re-reads the world at each statement boundary
//!   ([`TxnEngine::next_statement`]); within a statement its snapshot is
//!   fixed.
//! - A-ERR: errors are the SQLSTATE-named [`TxnErr`] variants only; a txn
//!   that received an error is still open and must end with
//!   [`TxnEngine::commit`] or [`TxnEngine::abort`].

use std::time::Duration;

/// Commit timestamp. `0` is "nothing committed"; ts's are assigned
/// strictly ascending by [`crate::common::Sequencer::next_commit_ts`].
pub type Ts = u64;

/// Transaction identifier, unique per engine instance.
pub type TxnId = u64;

/// Isolation level of a txn (PLAN-v3 N5; card Goal).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Isolation {
    /// Read Committed: a fresh read snapshot per statement; writes apply
    /// to the newest committed value via EPQ (C-T0 §5.2).
    ReadCommitted,
    /// Repeatable Read = snapshot isolation: one snapshot per txn; a
    /// write to a key with a committed version newer than the snapshot
    /// fails with 40001 (C-T0 §5.1 I-WW).
    RepeatableRead,
}

/// How a lock request waits for a conflicting holder (C-T0 §6).
#[derive(Clone, Copy, Debug)]
pub enum Wait {
    /// Block for at most the given duration, then fail with
    /// [`TxnErr::LockTimeout`].
    Block(Duration),
    /// Never wait: a conflict fails immediately with
    /// [`TxnErr::LockNotAvailable`] (SQLSTATE 55P03).
    NoWait,
}

/// Error surface of every engine: SQLSTATE-named conflicts plus the
/// fail-stop engine error (PLAN-v3 §6.1: one `thiserror` enum per crate).
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum TxnErr {
    /// 40001: serialization failure (the RR newer-version rule, or a
    /// future SSI abort).
    #[error("40001 serialization failure")]
    Serialization,
    /// 23505: unique violation ([`TxnEngine::insert_unique`] found a live
    /// committed or own value).
    #[error("23505 unique violation")]
    UniqueViolation,
    /// 55P03: lock not available ([`Wait::NoWait`] met a conflict).
    #[error("55P03 lock not available")]
    LockNotAvailable,
    /// A [`Wait::Block`] expired while waiting for a lock (there is no
    /// deadlock detector in the spike; tests use timeouts, card Work 3).
    #[error("lock wait timed out")]
    LockTimeout,
    /// Fail-stop engine error, including the poisoned state after an
    /// apply failure (PLAN-v3 §3.4 "apply cannot fail logically").
    #[error("engine: {0}")]
    Engine(String),
}

/// Handle to a savepoint of one txn, allocated monotonically by the txn's
/// write set (card X-1a Work 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SavepointId(
    /// The txn-local, never-reused savepoint number.
    pub u32,
);

/// The E1′ metric surface (card Goal: "identical write-path components, so
/// that only the version layout differs"). Arms fill the fields their
/// layout owns; the measurement driver reads them after each run.
#[derive(Clone, Debug, Default)]
pub struct EngineStats {
    /// Bytes held by the version-bearing page tree (0 for the reference
    /// engine, which keeps version lists in memory).
    pub tree_bytes: u64,
    /// Key + value bytes of the newest committed live state.
    pub logical_bytes: u64,
    /// Number of (ts, value) versions currently stored.
    pub versions: u64,
    /// Bytes of undo chains (arm (c) only; 0 for the reference engine).
    pub undo_bytes: u64,
    /// Bytes of undo spilled past the memory budget (always 0 in the
    /// spike; the budget itself is E1′'s to measure, not the harness's).
    pub undo_spilled_bytes: u64,
    /// Entries currently held by the lock table (row entries plus one
    /// table-wide owner when a txn has escalated).
    pub lock_entries: u64,
    /// Bytes currently held by all open txns' write sets.
    pub write_set_bytes: u64,
    /// Cumulative key + value bytes written into the version store at
    /// commit (the apply-side counterpart of `write_set_bytes`).
    pub bytes_applied: u64,
}

/// The transactional engine surface every E1′ arm implements (card X-1a
/// Work 2). See the module docs for the invariants; the behaviour suite
/// ([`crate::suite::run_all`]) is the authority on what each method must
/// do.
pub trait TxnEngine: Send + Sync + 'static {
    /// The open-txn handle. Only one thread owns a txn at a time; the
    /// binding is `Send` so scenarios may move txns across threads.
    type Txn: Send;

    /// The arm's name, for reports.
    fn name(&self) -> &'static str;

    /// Open a txn at the given isolation level, taking its first read
    /// snapshot.
    fn begin(&self, iso: Isolation) -> Self::Txn;

    /// Statement boundary. RC: take a fresh read snapshot. RR: no-op.
    fn next_statement(&self, t: &mut Self::Txn);

    /// Point read at the txn's current snapshot, own writes merged.
    fn get(&self, t: &mut Self::Txn, key: &[u8]) -> Result<Option<Vec<u8>>, TxnErr>;

    /// Keys in `[lo, hi)`, ascending, at most `limit`, own writes merged.
    /// (The signature is card-mandated; a type alias would only blur it.)
    #[allow(clippy::type_complexity)]
    fn scan(
        &self,
        t: &mut Self::Txn,
        lo: &[u8],
        hi: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, TxnErr>;

    /// Locks `key`, then applies `f` to the current value. RC: `f` sees
    /// the newest committed value (EPQ). RR: if a version newer than the
    /// snapshot exists, return [`TxnErr::Serialization`]. `f` returning
    /// `None` deletes.
    #[allow(clippy::type_complexity)]
    fn update_with(
        &self,
        t: &mut Self::Txn,
        key: &[u8],
        wait: Wait,
        f: &mut dyn FnMut(Option<&[u8]>) -> Option<Vec<u8>>,
    ) -> Result<(), TxnErr>;

    /// Insert or overwrite: [`Self::update_with`] whose closure always
    /// returns `Some(value)`; both RR conflict rules apply to it.
    fn put(&self, t: &mut Self::Txn, key: &[u8], value: &[u8], wait: Wait) -> Result<(), TxnErr>;

    /// Locks `key`; if a committed or own live value exists, return
    /// [`TxnErr::UniqueViolation`] (C-T0 §5.3 I-UNIQUE; the RR
    /// newer-version rule deliberately does not apply to key-existence
    /// ops).
    fn insert_unique(
        &self,
        t: &mut Self::Txn,
        key: &[u8],
        value: &[u8],
        wait: Wait,
    ) -> Result<(), TxnErr>;

    /// Locks `key` and deletes it; returns whether a value existed.
    fn delete(&self, t: &mut Self::Txn, key: &[u8], wait: Wait) -> Result<bool, TxnErr>;

    /// Take a savepoint of the txn's write set.
    fn savepoint(&self, t: &mut Self::Txn) -> SavepointId;

    /// Undo every write taken at or after the savepoint (exact restore;
    /// card Work 3).
    fn rollback_to(&self, t: &mut Self::Txn, sp: SavepointId);

    /// Commit: ts → apply → mark applied → release locks, in that order
    /// (PLAN-v3 §3.4; [`crate::common::commit_with`]). Returns the commit
    /// ts.
    fn commit(&self, t: Self::Txn) -> Result<Ts, TxnErr>;

    /// Abort: leave no trace and release every lock.
    fn abort(&self, t: Self::Txn);

    /// Reclaim versions/undo no active snapshot can see. Returns bytes
    /// reclaimed.
    fn gc(&self) -> u64;

    /// The E1′ metric surface.
    fn stats(&self) -> EngineStats;
}
