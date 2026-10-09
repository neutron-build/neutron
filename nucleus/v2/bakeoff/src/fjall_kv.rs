//! `FjallKv`: `OrderedKv` over one fjall 3.1 database (one keyspace, the
//! database's single journal as the one WAL across all keys).
//!
//! - **Durability:** a `Durability::Yes` write is a `WriteBatch` committed
//!   with `PersistMode::SyncAll`, which fsyncs the journal file
//!   (`journal/writer.rs:202-228`: `File::sync_all()`/`sync_data()`, i.e.
//!   std fsync — **not** F_FULLFSYNC; Rust std has no F_FULLFSYNC API).
//!   `sync_wal` is `Database::persist(PersistMode::SyncAll)`.
//! - **DeleteRange is emulated:** the store's writer mutex is held, the range
//!   is scanned, and one atomic `WriteBatch` carries the new puts, the point
//!   deletes, and a point tombstone per key currently live in the range.
//! - **GC:** fjall 3.1 exposes lsm-tree's compaction filter through
//!   `DatabaseBuilder::with_compaction_filter_factories` (an assigner called
//!   per keyspace, `recovery.rs:70-85`) and `lsm_tree::compaction::filter`
//!   (`compaction/stream.rs:142-181`): one `Factory::make_filter` per
//!   compaction run, items in ascending user-key order, tombstones never
//!   passed to the filter. The factory delegates to a slot shared with
//!   `set_gc_filter`.
//! - **Checkpoints are logical copies:** no on-disk checkpoint API exists in
//!   fjall 3.1, so `checkpoint` builds a fresh store at `dir` and ingests the
//!   latest visible state of this store (sorted, atomic) under the writer
//!   mutex.

use std::ops::Bound;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use fjall::compaction::filter::{
    CompactionFilter, CompactionFilterResult, Context, Factory, ItemAccessor, Verdict,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, PersistMode, Readable};
use nucleus_kv::conformance::Harness;
use nucleus_kv::{
    Batch, Durability, GcFilter, GcStream, Key, KvError, Op, OrderedKv, Result, Snapshot, Value,
};

use crate::{io_err, scratch_path, store_dir};

fn ok<T>(r: std::result::Result<T, KvError>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

type GcSlot = Arc<RwLock<Option<Arc<dyn GcFilter>>>>;

fn lock_slot(slot: &GcSlot) -> Option<Arc<dyn GcFilter>> {
    slot.read().unwrap_or_else(PoisonError::into_inner).clone()
}

/// lsm-tree `Factory` → one `GcStream` per compaction run.
struct FjallGcFactory {
    // `Factory: RefUnwindSafe`; `dyn GcFilter` has no such bound, so the slot
    // is wrapped (the filter itself never panics unwinds past this point).
    slot: AssertUnwindSafe<GcSlot>,
}

impl Factory for FjallGcFactory {
    fn name(&self) -> &str {
        "nucleus-fjall-gc"
    }

    fn make_filter(&self, _ctx: &Context) -> Box<dyn CompactionFilter> {
        Box::new(FjallGcFilter {
            stream: lock_slot(&self.slot).map(|f| f.begin()),
        })
    }
}

struct FjallGcFilter {
    stream: Option<Box<dyn GcStream>>,
}

impl CompactionFilter for FjallGcFilter {
    fn filter_item(&mut self, item: ItemAccessor<'_>, _ctx: &Context) -> CompactionFilterResult {
        let Some(stream) = self.stream.as_mut() else {
            return Ok(Verdict::Keep);
        };
        let value = item.value()?;
        let drop = stream.drop_key(item.key().as_ref(), value.as_ref());
        Ok(if drop { Verdict::Remove } else { Verdict::Keep })
    }
}

pub struct FjallKv {
    db: Database,
    ks: Keyspace,
    slot: GcSlot,
    watermark: Mutex<u64>,
    /// Serialises batch construction (DeleteRange emulation) and checkpoint
    /// copies against concurrent `write` calls.
    writer: Mutex<()>,
    path: PathBuf,
    owns_dir: bool,
}

impl Drop for FjallKv {
    fn drop(&mut self) {
        if self.owns_dir {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl FjallKv {
    pub fn open(path: &Path) -> Result<Self> {
        let slot: GcSlot = Arc::new(RwLock::new(None));
        let assigner_slot = Arc::clone(&slot);
        let db = Database::builder(path)
            .with_compaction_filter_factories(Arc::new(move |_name: &str| {
                let factory: Arc<dyn Factory> = Arc::new(FjallGcFactory {
                    slot: AssertUnwindSafe(Arc::clone(&assigner_slot)),
                });
                Some(factory)
            }))
            .open()
            .map_err(io_err)?;
        let ks = db
            .keyspace("default", KeyspaceCreateOptions::default)
            .map_err(io_err)?;
        Ok(Self {
            db,
            ks,
            slot,
            watermark: Mutex::new(0),
            writer: Mutex::new(()),
            path: path.to_path_buf(),
            owns_dir: false,
        })
    }

    /// A fresh store in scratch space, removed by the returned guard.
    pub fn scratch(tag: &str) -> Result<(Self, crate::ScratchGuard)> {
        let (dir, guard) = store_dir(tag);
        Ok((Self::open(&dir)?, guard))
    }

    /// A fresh scratch store that owns (and removes on drop) its directory.
    pub fn owned_scratch(tag: &str) -> Result<Self> {
        let mut kv = Self::open(&scratch_path(tag))?;
        kv.owns_dir = true;
        Ok(kv)
    }

    /// Harness `compact`: seal and flush the memtable, then one full major
    /// compaction through the registered GC filter.
    pub fn compact_all(&self) -> Result<()> {
        self.ks
            .rotate_memtable_and_wait()
            .map_err(io_err)
            .and_then(|()| self.ks.major_compact().map_err(io_err))
    }

    /// Harness `settle`: seal and flush the memtable to L0, no compaction.
    pub fn settle(&self) -> Result<()> {
        self.ks.rotate_memtable_and_wait().map_err(io_err)
    }
}

impl OrderedKv for FjallKv {
    type Snap = FjallSnap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let _w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let mut b = self
            .db
            .batch()
            .durability(if matches!(sync, Durability::Yes) {
                Some(PersistMode::SyncAll)
            } else {
                None
            });
        for op in batch.ops {
            match op {
                Op::Put(k, v) => b.insert(&self.ks, k, v),
                Op::Delete(k) => b.remove(&self.ks, k),
                // Range tombstones do not exist in the fjall 3.1 user API:
                // point tombstones for every live key in the range, applied
                // in the same atomic batch. The writer mutex is held, so no
                // concurrent write can slip into the scanned range between
                // scan and commit.
                Op::DeleteRange { start, end } => {
                    if start < end {
                        let doomed: Vec<_> = self
                            .ks
                            .range::<&[u8], _>((
                                Bound::Included(start.as_slice()),
                                Bound::Excluded(end.as_slice()),
                            ))
                            .collect();
                        for g in doomed {
                            let (k, _) = g.into_inner().map_err(io_err)?;
                            b.remove(&self.ks, k);
                        }
                    }
                }
            }
        }
        b.commit().map_err(io_err)
    }

    fn sync_wal(&self) -> Result<()> {
        self.db.persist(PersistMode::SyncAll).map_err(io_err)
    }

    fn snapshot(&self) -> FjallSnap {
        FjallSnap {
            snap: self.db.snapshot(),
            ks: self.ks.clone(),
        }
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self
            .ks
            .get(key)
            .map_err(io_err)?
            .map(|v| v.as_ref().to_vec()))
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        // lsm-tree asserts (panics) on out-of-order ingestion, so ordering is
        // validated here and a violation aborts before anything is
        // registered (`finish` is the registration point).
        let mut ingestion = self.ks.start_ingestion().map_err(io_err)?;
        let mut prev: Option<Key> = None;
        for (k, v) in entries {
            if prev.as_ref().is_some_and(|p| k <= *p) {
                return Err(KvError::Backend(
                    "ingest_sorted: keys not strictly ascending".into(),
                ));
            }
            ingestion.write(k.clone(), v).map_err(io_err)?;
            prev = Some(k);
        }
        ingestion.finish().map_err(io_err)
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        let _w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        let target = FjallKv::open(dir)?;
        let mut ingestion = target.ks.start_ingestion().map_err(io_err)?;
        for g in self.ks.iter() {
            let (k, v) = g.into_inner().map_err(io_err)?;
            ingestion
                .write(k.as_ref().to_vec(), v.as_ref().to_vec())
                .map_err(io_err)?;
        }
        ingestion.finish().map_err(io_err)?;
        target.db.persist(PersistMode::SyncAll).map_err(io_err)?;
        Ok(())
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        *self.slot.write().unwrap_or_else(PoisonError::into_inner) = Some(Arc::from(filter));
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        let mut w = self
            .watermark
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if watermark < *w {
            return Err(KvError::WatermarkRegressed {
                current: *w,
                requested: watermark,
            });
        }
        *w = watermark;
        Ok(())
    }
}

/// Frozen view over a fjall cross-keyspace snapshot (a registered GC nonce).
pub struct FjallSnap {
    snap: fjall::Snapshot,
    ks: Keyspace,
}

impl Snapshot for FjallSnap {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        Ok(self
            .snap
            .get(&self.ks, key)
            .map_err(io_err)?
            .map(|v| v.as_ref().to_vec()))
    }

    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> {
        let iter = self.snap.range::<&[u8], _>(&self.ks, range);
        let rows: Box<dyn DoubleEndedIterator<Item = fjall::Guard>> = if reverse {
            Box::new(iter.rev())
        } else {
            Box::new(iter)
        };
        Box::new(rows.map(|g| match g.into_inner() {
            Ok((k, v)) => Ok((k.as_ref().to_vec(), v.as_ref().to_vec())),
            Err(e) => Err(io_err(e)),
        }))
    }
}

/// Conformance harness.
#[derive(Clone, Default)]
pub struct FjallHarness;

impl FjallHarness {
    pub fn new() -> Self {
        Self
    }
}

impl Harness for FjallHarness {
    type Kv = FjallKv;

    fn make(&self) -> FjallKv {
        ok(FjallKv::owned_scratch("fjall-conf"), "FjallKv::open")
    }

    fn compact(&self, kv: &FjallKv) -> Result<()> {
        kv.compact_all()
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<FjallKv> {
        FjallKv::open(dir)
    }

    fn settle(&self, kv: &FjallKv) -> Result<()> {
        kv.settle()
    }
}
