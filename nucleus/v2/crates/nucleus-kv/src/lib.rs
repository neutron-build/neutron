//! nucleus-kv: the ordered key-value substrate (V2 plan D1/D2, C-T0 §3, §9).
//!
//! Every backend (RocksDB, fjall, MemKv) implements `OrderedKv` and must pass the
//! shared conformance suite (C-K3), including GC. Requirements the trait cannot
//! express in types are stated on the items and checked by that suite.

use std::ops::Bound;

#[cfg(any(test, feature = "conformance"))]
pub mod conformance;
#[cfg(any(test, feature = "fault"))]
pub mod fault;
#[cfg(feature = "mem")]
pub mod mem;
#[cfg(feature = "mem")]
pub use mem::MemKv;

pub type Key = Vec<u8>;
pub type Value = Vec<u8>;

#[derive(Debug, thiserror::Error)]
pub enum KvError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("corruption: {0}")]
    Corruption(String),
    #[error("format version {found} not supported (supported {supported})")]
    Format { found: u32, supported: u32 },
    #[error("backend: {0}")]
    Backend(String),
}

pub type Result<T> = std::result::Result<T, KvError>;

/// One atomic write. Applied all-or-nothing, across every keyspace prefix.
#[derive(Debug, Default, Clone)]
pub struct Batch {
    pub ops: Vec<Op>,
}

#[derive(Debug, Clone)]
pub enum Op {
    Put(Key, Value),
    Delete(Key),
    /// Removes every key in `[start, end)`. Must be correct across all levels
    /// (no older key under the range may reappear). C-T0 §9 relies on this for
    /// tombstone GC. An empty or inverted range is a no-op.
    DeleteRange {
        start: Key,
        end: Key,
    },
}

impl Batch {
    pub fn put(&mut self, k: Key, v: Value) {
        self.ops.push(Op::Put(k, v));
    }
    pub fn delete(&mut self, k: Key) {
        self.ops.push(Op::Delete(k));
    }
    pub fn delete_range(&mut self, start: Key, end: Key) {
        self.ops.push(Op::DeleteRange { start, end });
    }
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}

/// Durability of one `write`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Appended to the WAL, not fsynced. Covered by any later `Durability::Yes` or `sync_wal`.
    No,
    /// Appended and fsynced (F_FULLFSYNC on macOS) before returning.
    Yes,
}

/// A consistent point-in-time view. Reads through it never observe later writes.
pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Value>>;
    /// Iterator over `range`, ascending, or descending when `reverse`.
    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a>;
}

/// Decides, during compaction, whether a version may be dropped (C-T0 §9).
/// Called on keys in ascending order within one compaction stream. Backends
/// that cannot provide streaming order to the filter must not register it.
pub trait GcFilter: Send + Sync {
    /// Called at the start of each compaction stream; returns per-stream state.
    fn begin(&self) -> Box<dyn GcStream>;
}

pub trait GcStream: Send {
    /// `true` = drop this key. Must never return `true` for a tombstone (§9);
    /// the kv applies the answer as given. A snapshot opened before the
    /// compaction may or may not keep returning a dropped key (C-K3 suite).
    fn drop_key(&mut self, key: &[u8], value: &[u8]) -> bool;
}

/// The substrate. Implementations: RocksDB (default), fjall, MemKv.
///
/// Requirements beyond the signatures (C-S1 disqualifiers, checked by C-K3):
/// - **Single WAL across all keys.** WAL order equals `write` call completion
///   order; a `Durability::Yes` write or `sync_wal` makes every earlier write durable.
/// - **Atomic batches** across all key prefixes, including after a crash.
/// - `write` may be called from many threads; callers that need ordering
///   (the commit thread) serialise themselves.
/// - Corruption is reported as `KvError::Corruption`, never as missing data.
pub trait OrderedKv: Send + Sync + 'static {
    type Snap: Snapshot + 'static;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()>;
    /// fsync the WAL up to everything written so far. Blocking; never call on an
    /// async executor thread (C-R0b).
    fn sync_wal(&self) -> Result<()>;
    fn snapshot(&self) -> Self::Snap;
    /// Latest-state point read (no snapshot). Used under the C-T0 §5 latch.
    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>>;
    /// Bulk-ingest pre-sorted, non-overlapping-with-live-writes data (online
    /// index build, restore). Atomic.
    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()>;
    /// Consistent on-disk checkpoint at `dir` (hard links where possible).
    /// `dir` must not exist yet. Includes every completed write.
    fn checkpoint(&self, dir: &std::path::Path) -> Result<()>;
    /// Install the compaction GC filter. Backends with native user-defined
    /// timestamps may ignore it and use `set_gc_watermark` instead.
    fn set_gc_filter(&self, filter: Box<dyn GcFilter>);
    /// Versions with ts below `watermark` may be collapsed by the engine (UDT path).
    fn set_gc_watermark(&self, watermark: u64);
}
