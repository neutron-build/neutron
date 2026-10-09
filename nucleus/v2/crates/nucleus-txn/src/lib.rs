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
//! resolution/truncation jobs (`resolver`). The write path (§5), shared
//! locks and the wait-for graph (C-T2b), SSI (C-T3) and GC (C-T4) are later
//! cards; only the hooks named in the card exist.

pub mod boot;
pub mod commit;
pub mod encoding;
pub mod latch;
pub mod read;
pub mod registry;
pub mod removal;
pub mod resolver;
pub mod status;
pub mod txn;
pub mod visibility;
pub mod wait;

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
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
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
            TxnError::Invariant(_) => "XX000",
            TxnError::Corrupt(_) => "XX000",
            TxnError::Kv(_) => "XX000",
        }
    }
}

pub(crate) fn kv_err(e: nucleus_kv::KvError) -> TxnError {
    TxnError::Kv(e.to_string())
}
