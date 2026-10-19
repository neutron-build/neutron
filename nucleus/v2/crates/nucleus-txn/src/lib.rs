//! nucleus-txn: MVCC transactions over `OrderedKv`. Normative spec:
//! `docs/C-T0-txn-protocol.md`. Section numbers below refer to it.
//!
//! Card C-T1a built the shared core every later T card extends: the §2
//! on-disk encoding (`encoding`), the status table with epochs and boot
//! (`status`, `boot`), the snapshot/view registry (`registry`), latches
//! (`latch`), intent removal (`removal`) and the read path (`read`). The
//! pure §4 rule lives in `visibility`.
//!
//! Card C-T1b adds the txn handle (`txn`), the §3 commit pipeline and §7.1
//! abort (`commit`), §6 waiting with wake generations (`wait`), and the
//! resolution/truncation jobs (`resolver`).
//!
//! Card C-T2 adds the §5 write path (`write`): the §5.1 placement loop as
//! non-blocking steps plus blocking drivers, intent layers (§2.1), row ops,
//! unique checks (§5.3), savepoints and `ROLLBACK TO` (§5.5), and the
//! `RowLocks` / `SsiHook` seams that C-T2b and C-T3 plug into. The
//! wait-for graph and deadlock detection are C-T2b (`locks`); SSI state is
//! C-T3 (`ssi`).
//!
//! Card C-T4 adds the §9 GC (`gc`): the compaction filter `gc::TxnGcFilter`
//! and the job `gc::GcJob` that publishes the watermark `W` (one registry
//! critical section, then `/sys/gc_w` synced before the filter or any
//! `DeleteRange` sees it), removes newest-`<= W` tombstones, and retires
//! dropped/truncated storage prefixes (intents out through §7.3 first), so
//! I-GC and I-GC-QUIESCE hold for registered snapshots and open views.
//!
//! Card C-T3 adds §8 SSI (`ssi`): SIREADs registered before the view they
//! protect (I-SSI-ORDER), reader-, writer- and DDL-side rw-edges between
//! concurrent SERIALIZABLE txns, the commit thread's writer map, the
//! dangerous-structure check atomic with prepare and enqueue, abort cleanup,
//! retired-id SIREAD promotion and §8.6 retention. It plugs into the
//! `SsiHook`, `CommitObserver` and `ReadObserver` seams.
//! Card C-T2b adds the in-memory lock side of §6 (`locks`): the shared
//! row-lock table (KEY SHARE, SHARE) behind C-T2's `RowLocks` seam,
//! relation locks and transaction-scoped advisory locks with NOWAIT /
//! SKIP LOCKED / `lock_timeout`, the `ReleaseHook` that drops relation and
//! advisory locks at commit step 5, abort and `ROLLBACK TO`, and — in
//! `wait` — the wait-for graph with waker-side edge removal, deadlock
//! detection after `deadlock_timeout` (40P01, one victim per cycle) and
//! the split begin / poll / deadlock-check / end wait steps C-SIM drives
//! on one thread.
//!
//! Card C-T7 adds §10 "Sequences" (`seq`): non-transactional
//! `Core::seq_next` value allocation in blocks of `seq::BLOCK` — the
//! persisted high-water mark `/sys/seq/{id}` is written `Durability::Yes`
//! before the first value of a new block is handed out, so values may
//! skip but never repeat, across crashes, torn writes and concurrent
//! callers. Sequences stay off the commit path and out of every txn's
//! write set.
//!
//! Card C-T5 adds §10's catalog (`catalog`): relation and index rows as
//! versioned data under `/sys/catalog/` placed through the §5 write path
//! (DDL is thereby transactional and takes AccessExclusive first), storage
//! ids and oids from persisted block counters (never reused), and the
//! boot-time rebuild of the SSI storage map from the committed catalog.
//! The `/sys/catalog/` compaction-filter carve-out of §10 landed with
//! C-B1c: one scope predicate (the `gc_scope` rule, in the filter and
//! the tombstone pass) makes the catalog GC-able while every other
//! `/sys/` key stays untouched.
//!
//! Card C-B1a adds the §9.3 background-job framework (`jobs`): the
//! resolver, GC and SSI retention run as bounded-step jobs on std threads
//! under one runner, with a common shutdown order, fail-stop for the set,
//! per-job counters, named crash points and wake-on-enqueue for the
//! resolver (commit step 5, abort and the no-write commit all wake it, so
//! it no longer polls). The old `spawn_background`/`spawn_gc`/
//! `spawn_retention` helpers remain, now one-job `jobs::runner::JobSet`s.
//!
//! Card C-R0ba adds the blocking contract (`rt`): every entry point that
//! can block is classified KvIo / Fsync / Park / Join, and a thread marked
//! as an executor thread ([`rt::enter_executor`]) that reaches one panics
//! before the call changes any state — the checked form of §3 step 4's
//! "fsync never on a tokio worker". Unmarked threads (every existing test,
//! the commit thread, the simulator, the background jobs) are unaffected.
//!
//! Card C-B1b gives the last two §9.3 slots their bodies. The **boot
//! sweep** (`jobs::sweep`, §7.2) walks the keyspace outside `/sys/` plus
//! `/sys/catalog/` in bounded chunks through registered views and removes
//! every older-epoch intent through §7.3 (`Resolve` for a persisted
//! record, `Discard` otherwise), recording `sweep_counter` only after the
//! last removal (§7.4 condition 1) — so older-epoch `/sys/txn` records
//! finally truncate and stop piling up across reboots. The
//! **drop-cleanup** job (`jobs::drop_cleanup`, §9.2) reads the
//! transactional `/sys/catalog/retired/` rows the catalog's DDL now
//! writes, and once the durable `W` passes the snapshot that saw a row,
//! retires each of its ranges (`GcJob::retire_storage`: intents out
//! through §7.3 first, then the `DeleteRange`) and deletes the row in its
//! own txn. Both hold only raw keys, so they take `latch_key(k)` from the
//! [`LatchKeySource`](jobs::LatchKeySource) seam (§7.3 SD2b), installed
//! before either job starts and defaulting to "no deferrable keys".
//!
//! Card C-B1c finishes C-B1's plan row: per-job **I/O budgets** (§9.3) —
//! a token bucket over an injectable [`JobClock`](jobs::JobClock),
//! charged in the units the job bodies report
//! (`nucleus_job_units_total`), that only ever delays a step — plus
//! **bounded steps** so a budget can bite (the resolver takes
//! `RESOLVER_BATCH` keys per round with the unfinished work requeued at
//! the front; the GC tombstone pass walks the keyspace in
//! logical-key-boundary chunks, one registered view and one durable `W`
//! per chunk), and §10's `/sys/catalog/` GC carve-out (one scope
//! predicate in the compaction filter and the tombstone pass, so catalog
//! rows stop accumulating versions).

pub mod boot;
pub mod commit;
pub mod encoding;
pub mod gc;
#[cfg(test)]
mod internal_tests;
pub mod latch;
pub mod read;
pub mod registry;
pub mod removal;
pub mod resolver;
pub mod ssi;
pub mod status;
pub mod txn;
pub mod visibility;
pub mod wait;
pub mod write;

// C-T2b, appended at the end of the module list (a separate group, so
// rustfmt does not sort it back in).
pub mod locks;

// C-T7, appended the same way.
pub mod seq;

// C-T5, likewise appended (rustfmt does not sort it back in).
pub mod catalog;

// C-B1a, appended the same way (its own group at the end).
pub mod jobs;

// C-R0ba, likewise appended (the blocking-contract module).
pub mod rt;

/// Commit timestamp (§1). Room to widen to an HLC later without changing callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Ts(pub u64);

impl Ts {
    /// Never a commit ts.
    pub const ZERO: Ts = Ts(0);
}

/// `(boot epoch, dense counter)` (§1, §7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TxnId {
    pub epoch: u32,
    pub n: u64,
}

/// Per-txn command counter (PostgreSQL command id).
pub type Seq = u32;

/// Status as seen by a reader (§2, §7). Only `Committed` is ever persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnStatus {
    Pending,
    Committed(Ts),
    Aborted,
}

/// PostgreSQL row-lock modes (§6). Exclusive modes become intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RowLockMode {
    KeyShare,
    Share,
    NoKeyUpdate,
    Update,
}

impl RowLockMode {
    /// PostgreSQL's row-lock conflict matrix.
    pub fn conflicts_with(self, other: RowLockMode) -> bool {
        use RowLockMode::*;
        matches!(
            (self, other),
            (KeyShare, Update)
                | (Update, KeyShare)
                | (Share, NoKeyUpdate)
                | (NoKeyUpdate, Share)
                | (Share, Update)
                | (Update, Share)
                | (NoKeyUpdate, NoKeyUpdate)
                | (NoKeyUpdate, Update)
                | (Update, NoKeyUpdate)
                | (Update, Update)
        )
    }
    pub fn is_exclusive(self) -> bool {
        matches!(self, RowLockMode::NoKeyUpdate | RowLockMode::Update)
    }
}

/// A layer's own value of the key (§2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LayerData {
    /// `key_changed`: a key column differs from the previous value.
    Write { value: Vec<u8>, key_changed: bool },
    /// `moved`: left at the old `/t/` key by a primary-key change (§5.2).
    Delete { moved: bool },
    /// No own value: reads fall through to committed versions. A layer with
    /// `Absent` data only holds a lock.
    Absent,
}

/// The complete own state of the key as of `seq` (§2.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layer {
    pub seq: Seq,
    /// Seq at which `data` last changed (§5.4).
    pub data_seq: Seq,
    pub data: LayerData,
    /// Strongest exclusive mode held: `NoKeyUpdate` or `Update`.
    pub lock: RowLockMode,
}

/// The `@INTENT` slot (§2.1). At most one per logical key (I-ONE-INTENT).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Intent {
    pub txn: TxnId,
    /// Oldest first, never empty. The last layer is the current state.
    pub layers: Vec<Layer>,
}

impl Intent {
    pub fn top(&self) -> Option<&Layer> {
        self.layers.last()
    }
}

/// SQLSTATE-bearing transaction errors raised by this crate.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TxnError {
    #[error("could not serialize access")]
    SerializationFailure, // 40001
    #[error("deadlock detected")]
    Deadlock, // 40P01
    #[error("could not obtain lock")]
    LockNotAvailable, // 55P03
    #[error("duplicate key value violates unique constraint")]
    UniqueViolation, // 23505
    #[error("insert or update violates foreign key constraint")]
    ForeignKeyViolation, // 23503
    #[error("command cannot affect row a second time")]
    CardinalityViolation, // 21000
    #[error(
        "tuple to be updated was already modified by an operation triggered by the current command"
    )]
    TriggeredDataChange, // 27000
    #[error("snapshot too old")]
    SnapshotTooOld, // 72000
    #[error("canceling statement due to user request")]
    QueryCanceled, // 57014
    /// A commit whose records may have reached the WAL cannot be reported
    /// as failed: its resolution is unknown (PostgreSQL 08007). Every error
    /// ack of a group after any of its records reached the WAL uses this.
    #[error("transaction resolution unknown")]
    CommitIndeterminate, // 08007
    /// A protocol invariant was violated (C-T0 §4, §7): e.g. the owner of an
    /// intent found in a view has no current-epoch status entry. The caller
    /// makes this fatal (process abort); it is never defaulted.
    #[error("transaction invariant violated: {0}")]
    Invariant(String), // XX000
    /// Persisted bytes did not decode (§2.2/§2.3 formats). Reported, never a
    /// panic.
    #[error("corrupt persisted data: {0}")]
    Corrupt(String), // XX000
    #[error("kv: {0}")]
    Kv(String),
}

impl TxnError {
    pub fn sqlstate(&self) -> &'static str {
        match self {
            TxnError::SerializationFailure => "40001",
            TxnError::Deadlock => "40P01",
            TxnError::LockNotAvailable => "55P03",
            TxnError::UniqueViolation => "23505",
            TxnError::ForeignKeyViolation => "23503",
            TxnError::CardinalityViolation => "21000",
            TxnError::TriggeredDataChange => "27000",
            TxnError::SnapshotTooOld => "72000",
            TxnError::QueryCanceled => "57014",
            TxnError::CommitIndeterminate => "08007",
            TxnError::Invariant(_) => "XX000",
            TxnError::Corrupt(_) => "XX000",
            TxnError::Kv(_) => "XX000",
        }
    }
}

pub(crate) fn kv_err(e: nucleus_kv::KvError) -> TxnError {
    TxnError::Kv(e.to_string())
}
