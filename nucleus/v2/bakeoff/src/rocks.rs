//! `RocksKv`: `OrderedKv` over one RocksDB database (rust-rocksdb 0.25,
//! librocksdb-sys 0.19.0 + bundled C++ 11.8.1).
//!
//! - **One WAL across all keys:** a single DB, the default column family, no
//!   column-family split; every `write` is one `rocksdb::WriteBatch`, so the
//!   WAL order is the batch order and a `Durability::Yes` write (`WriteOptions
//!   .set_sync(true)` → `PosixWritableFile::Sync` → `fcntl(F_FULLFSYNC)` on
//!   macOS, `env/io_posix.cc:1826` under `HAVE_FULLFSYNC`, which
//!   `CMakeLists.txt:606-608` defines on macOS) makes every earlier write
//!   durable.
//! - **GC:** a `CompactionFilterFactory` installed at open creates one filter
//!   per compaction run (rust-rocksdb `compaction_filter_factory.rs:16-38`),
//!   which maps 1:1 onto `GcFilter::begin()` per stream. The factory delegates
//!   to a slot shared with `set_gc_filter`, so the filter can be replaced
//!   after open. Auto-compactions are disabled, so streams only start on the
//!   harness's explicit `compact_range` and every live key passes the filter
//!   exactly once.
//! - **Snapshots are copies** (see the crate docs): `snapshot()` drains one
//!   raw iterator — pinned at creation, hence consistent — into a `BTreeMap`.

use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use nucleus_kv::conformance::Harness;
use nucleus_kv::{
    Batch, Durability, GcFilter, GcStream, Key, KvError, Op, OrderedKv, Result, Snapshot, Value,
};
use rocksdb::checkpoint::Checkpoint;
use rocksdb::compaction_filter::CompactionFilter;
use rocksdb::compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory};
use rocksdb::{
    CompactionDecision, IngestExternalFileOptions, Options as RocksOptions, SstFileWriter,
    WriteBatch as RocksWriteBatch, WriteOptions, DB,
};

use crate::{io_err, store_dir};

/// `ok` for non-test code: no `unwrap`/`expect` outside tests.
fn ok<T>(r: std::result::Result<T, KvError>, what: &str) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("{what}: {e}"),
    }
}

/// Shared, replaceable GC filter holder. The compaction-filter factory reads
/// it for every compaction run; `set_gc_filter` writes it.
type GcSlot = Arc<RwLock<Option<Arc<dyn GcFilter>>>>;

fn lock_slot(slot: &GcSlot) -> Option<Arc<dyn GcFilter>> {
    slot.read().unwrap_or_else(PoisonError::into_inner).clone()
}

/// `CompactionFilterFactory` → one `GcStream` per compaction run.
struct SlotFactory {
    slot: GcSlot,
    name: CString,
}

impl CompactionFilterFactory for SlotFactory {
    type Filter = SlotFilter;

    fn create(&mut self, _context: CompactionFilterContext) -> SlotFilter {
        SlotFilter {
            stream: lock_slot(&self.slot).map(|f| f.begin()),
            name: CString::clone(&self.name),
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

struct SlotFilter {
    stream: Option<Box<dyn GcStream>>,
    name: CString,
}

impl CompactionFilter for SlotFilter {
    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        match self.stream.as_mut() {
            // No registered filter: keep everything (matches
            // `gc_without_filter_keeps_all`).
            None => CompactionDecision::Keep,
            Some(s) => {
                if s.drop_key(key, value) {
                    CompactionDecision::Remove
                } else {
                    CompactionDecision::Keep
                }
            }
        }
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

pub struct RocksKv {
    db: DB,
    slot: GcSlot,
    watermark: Mutex<u64>,
    /// Set for stores that own their directory (conformance `make`); such a
    /// store removes its directory when dropped, keeping scratch bounded.
    path: PathBuf,
    owns_dir: bool,
}

impl Drop for RocksKv {
    fn drop(&mut self) {
        if self.owns_dir {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }
}

impl RocksKv {
    /// Opens (creating if needed) a single-column-family database at `path`
    /// with the GC factory installed and auto-compactions disabled.
    pub fn open(path: &Path) -> Result<Self> {
        let slot: GcSlot = Arc::new(RwLock::new(None));
        let name = CString::new("nucleus-rocks-gc")
            .map_err(|_| KvError::Backend("compaction filter name".into()))?;
        let mut opts = RocksOptions::default();
        opts.create_if_missing(true);
        opts.set_disable_auto_compactions(true);
        opts.set_compaction_filter_factory(SlotFactory {
            slot: Arc::clone(&slot),
            name,
        });
        let db = DB::open(&opts, path).map_err(io_err)?;
        Ok(Self {
            db,
            slot,
            watermark: Mutex::new(0),
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
        let mut kv = Self::open(&crate::scratch_path(tag))?;
        kv.owns_dir = true;
        Ok(kv)
    }

    /// Full manual compaction of every SST file through the registered GC
    /// filter (harness `compact`). Two passes, because RocksDB invokes the
    /// compaction filter *before* applying range tombstones
    /// (`compaction_iterator.cc:345-347` `InvokeFilterIfNeeded` precedes the
    /// `range_del_agg_->ShouldDelete` check in `PrepareOutput`, line 1361):
    /// the first pass (filter slot emptied) settles point- and
    /// range-tombstones, so the second pass passes exactly the live keys
    /// through the filter, ascending, once each.
    pub fn compact_all(&self) -> Result<()> {
        self.db.flush().map_err(io_err)?;
        let saved = lock_slot(&self.slot);
        *self.slot.write().unwrap_or_else(PoisonError::into_inner) = None;
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        *self.slot.write().unwrap_or_else(PoisonError::into_inner) = saved;
        self.db.compact_range(None::<&[u8]>, None::<&[u8]>);
        Ok(())
    }

    /// Harness `settle`: memtable to L0 without any compaction.
    pub fn settle(&self) -> Result<()> {
        self.db.flush().map_err(io_err)
    }
}

impl OrderedKv for RocksKv {
    type Snap = RocksSnap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let mut wb = RocksWriteBatch::default();
        for op in batch.ops {
            match op {
                Op::Put(k, v) => wb.put(k, v),
                Op::Delete(k) => wb.delete(k),
                Op::DeleteRange { start, end } => {
                    // The contract makes empty and inverted ranges no-ops;
                    // RocksDB would reject them as invalid arguments.
                    if start < end {
                        wb.delete_range(start, end);
                    }
                }
            }
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(matches!(sync, Durability::Yes));
        self.db.write_opt(wb, &wo).map_err(io_err)
    }

    fn sync_wal(&self) -> Result<()> {
        self.db.flush_wal(true).map_err(io_err)
    }

    /// Copy-on-snapshot: one raw iterator pinned at creation is drained into
    /// a map, which serves `get`/`scan` afterwards. An iteration error is
    /// carried inside the snapshot and surfaces on read.
    fn snapshot(&self) -> RocksSnap {
        let mut map = BTreeMap::new();
        let mut err: Option<KvError> = None;
        let mut it = self.db.raw_iterator();
        it.seek_to_first();
        while it.valid() {
            map.insert(
                it.key().unwrap_or(&[]).to_vec(),
                it.value().unwrap_or(&[]).to_vec(),
            );
            it.next();
        }
        if let Err(e) = it.status() {
            err = Some(io_err(e));
        }
        RocksSnap { map, err }
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.db.get(key).map_err(io_err)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        let (sst_dir, _guard) = store_dir("rocks-sst");
        std::fs::create_dir_all(&sst_dir)?;
        let sst_path = sst_dir.join("ingest.sst");
        let writer_opts = RocksOptions::default();
        let mut prev: Option<Key> = None;
        let mut writer: Option<SstFileWriter> = None;
        for (k, v) in entries {
            if prev.as_ref().is_some_and(|p| k <= *p) {
                return Err(KvError::Backend(
                    "ingest_sorted: keys not strictly ascending".into(),
                ));
            }
            if writer.is_none() {
                let w = SstFileWriter::create(&writer_opts);
                w.open(&sst_path).map_err(io_err)?;
                writer = Some(w);
            }
            if let Some(w) = writer.as_mut() {
                w.put(k.as_slice(), v).map_err(io_err)?;
            }
            prev = Some(k);
        }
        match writer {
            // Empty input: nothing to ingest.
            None => Ok(()),
            Some(mut w) => {
                w.finish().map_err(io_err)?;
                self.db
                    .ingest_external_file_opts(
                        &IngestExternalFileOptions::default(),
                        vec![sst_path],
                    )
                    .map_err(io_err)
            }
        }
    }

    fn checkpoint(&self, dir: &Path) -> Result<()> {
        Checkpoint::new(&self.db)
            .map_err(io_err)?
            .create_checkpoint(dir)
            .map_err(io_err)
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

/// Frozen view: a map copied from one pinned iterator at `snapshot()` time.
pub struct RocksSnap {
    map: BTreeMap<Key, Value>,
    err: Option<KvError>,
}

impl Snapshot for RocksSnap {
    fn get(&self, key: &[u8]) -> Result<Option<Value>> {
        match &self.err {
            Some(e) => Err(KvError::Corruption(e.to_string())),
            None => Ok(self.map.get(key).cloned()),
        }
    }

    fn scan<'a>(
        &'a self,
        range: (Bound<&[u8]>, Bound<&[u8]>),
        reverse: bool,
    ) -> Box<dyn Iterator<Item = Result<(Key, Value)>> + 'a> {
        let lo: Bound<Key> = match range.0 {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let hi: Bound<Key> = match range.1 {
            Bound::Included(k) => Bound::Included(k.to_vec()),
            Bound::Excluded(k) => Bound::Excluded(k.to_vec()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let mut rows: Vec<Result<(Key, Value)>> = Vec::new();
        if let Some(e) = &self.err {
            rows.push(Err(KvError::Corruption(e.to_string())));
        } else if !range_is_empty(range.0, range.1) {
            rows.extend(
                self.map
                    .range((lo, hi))
                    .map(|(k, v)| Ok((k.clone(), v.clone()))),
            );
        }
        if reverse {
            rows.reverse();
        }
        Box::new(rows.into_iter())
    }
}

/// True for ranges that contain no key, including inverted ones (mirrors
/// `range_is_empty` in nucleus-kv's `mem.rs`; `BTreeMap::range` panics on
/// such bounds).
fn range_is_empty(lo: Bound<&[u8]>, hi: Bound<&[u8]>) -> bool {
    match (lo, hi) {
        (Bound::Included(a), Bound::Included(b)) => a > b,
        (Bound::Included(a) | Bound::Excluded(a), Bound::Excluded(b))
        | (Bound::Excluded(a), Bound::Included(b)) => a >= b,
        _ => false,
    }
}

/// Conformance harness: a fresh self-cleaning scratch store per `make`, a
/// full manual compaction through the filter for `compact`, plain flushes
/// for `settle`.
#[derive(Clone, Default)]
pub struct RocksHarness;

impl RocksHarness {
    pub fn new() -> Self {
        Self
    }
}

impl Harness for RocksHarness {
    type Kv = RocksKv;

    fn make(&self) -> RocksKv {
        ok(RocksKv::owned_scratch("rocks-conf"), "RocksKv::open")
    }

    fn compact(&self, kv: &RocksKv) -> Result<()> {
        kv.compact_all()
    }

    fn open_checkpoint(&self, dir: &Path) -> Result<RocksKv> {
        RocksKv::open(dir)
    }

    fn settle(&self, kv: &RocksKv) -> Result<()> {
        kv.settle()
    }
}
