//! nucleus-txn: MVCC transactions over `OrderedKv`. Normative spec:
//! `docs/C-T0-txn-protocol.md`. Section numbers below refer to it.
//!
//! This file fixes the core types and the pure visibility rule (§4). Everything
//! else (commit thread, write path, locks, SSI, GC) is built by the C-T cards.

pub mod visibility;

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
            TxnError::Kv(_) => "XX000",
        }
    }
}
