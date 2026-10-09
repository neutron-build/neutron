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
//! wait-for graph, deadlock detection and SSI state are later cards; only
//! the hooks named in the card exist.
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
