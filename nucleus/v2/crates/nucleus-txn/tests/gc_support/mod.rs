//! Shared support for the C-T4 GC tests (`tests/gc_*.rs`): a shareable
//! `MemKv` (LSM mode) wrapper so tests can flush/compact a KV inside a
//! `Core`, a fail-on-chosen-key wrapper, intent/commit/read helpers, and
//! deterministic thread-handshake primitives for the job-thread tests.
#![allow(dead_code)]

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use nucleus_kv::mem::FileMeta;
use nucleus_kv::{
    Batch, Durability, GcFilter, Key, KvError, MemKv, Op, OrderedKv, Result, Snapshot, Value,
};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{Clock, CommitPipeline, CommitRequest, FailStop, SyncCommit};
use nucleus_txn::encoding::{
    encode_intent, encode_version, intent_key, sys_gc_w_key, sys_ts_clock_key, sys_ts_hwm_key,
    version_key,
};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Layer, LayerData, RowLockMode, Ts, TxnError, TxnId};

pub fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// `OrderedKv` over an `Arc<MemKv>` the test keeps, so a `Core` can be
/// opened on it while the test still drives `flush`/`compact`/`files`/
/// `compact_all` (the card's prescribed wrapper; `Core` owns its KV
/// privately otherwise).
#[derive(Clone)]
pub struct SharedKv {
    pub mem: Arc<MemKv>,
}

impl SharedKv {
    /// `MemKv` in LSM mode: writes go to a memtable, `flush` makes L0
    /// files, `compact`/`compact_all` run the registered filter per
    /// output stream, and a filter drop is visible to already-open
    /// snapshots (the adversarial choice G0-gc needs).
    pub fn lsm() -> SharedKv {
        SharedKv {
            mem: Arc::new(MemKv::lsm()),
        }
    }

    pub fn flush(&self) -> Option<u64> {
        ok(self.mem.flush())
    }

    pub fn compact(&self, level: usize, files: &[u64]) -> Vec<u64> {
        ok(self.mem.compact(level, files))
    }

    pub fn files(&self) -> Vec<FileMeta> {
        ok(self.mem.files())
    }

    pub fn compact_all(&self) -> usize {
        self.mem.compact_all()
    }

    /// Flush + compact all of L0 into L1 without the filter (the kv
    /// harness's state-building step).
    pub fn settle(&self) {
        ok(self.mem.settle());
    }

    /// One raw write batch, applied before `Core::open` (preloads).
    pub fn preload(&self, batch: Batch) {
        ok(self.mem.write(batch, Durability::Yes));
    }

    /// The merged raw entries of `[lo, hi)` from a fresh snapshot: what is
    /// physically visible in the store (RT-hidden entries absent, filter
    /// drops absent) — the raw-storage basis of the quiesce and retire
    /// checks.
    pub fn raw_entries(&self, lo: &[u8], hi: &[u8]) -> Vec<(Key, Value)> {
        let snap = self.mem.snapshot();
        snap.scan((Bound::Included(lo), Bound::Excluded(hi)), false)
            .map(ok)
            .collect()
    }
}

impl OrderedKv for SharedKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.mem.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.mem.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        self.mem.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.mem.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.mem.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
        self.mem.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.mem.set_gc_filter(filter);
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.mem.set_gc_watermark(watermark)
    }
}

/// An `OrderedKv` wrapper that fails the **first armed** write touching a
/// chosen key (a `Put`/`Delete` of exactly it, or a `DeleteRange` covering
/// it), then lets later writes through. Clones share the state.
#[derive(Clone)]
pub struct FailOnKey {
    inner: SharedKv,
    key: Key,
    armed: Arc<AtomicBool>,
    fired: Arc<AtomicBool>,
}

impl FailOnKey {
    pub fn new(inner: SharedKv, key: &[u8]) -> FailOnKey {
        FailOnKey {
            inner,
            key: key.to_vec(),
            armed: Arc::new(AtomicBool::new(false)),
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// From now on, the first write touching the key fails.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    fn touches(&self, ops: &[Op]) -> bool {
        ops.iter().any(|op| match op {
            Op::Put(k, _) | Op::Delete(k) => k == &self.key,
            Op::DeleteRange { start, end } => start <= &self.key && &self.key < end,
        })
    }
}

impl OrderedKv for FailOnKey {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if !batch.ops.is_empty()
            && self.armed.load(Ordering::SeqCst)
            && !self.fired.load(Ordering::SeqCst)
            && self.touches(&batch.ops)
        {
            self.fired.store(true, Ordering::SeqCst);
            return Err(KvError::Backend("injected failure on a chosen key".into()));
        }
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.inner.sync_wal()
    }

    fn snapshot(&self) -> Self::Snap {
        self.inner.snapshot()
    }

    fn get_latest(&self, key: &[u8]) -> Result<Option<Value>> {
        self.inner.get_latest(key)
    }

    fn ingest_sorted(&self, entries: &mut dyn Iterator<Item = (Key, Value)>) -> Result<()> {
        self.inner.ingest_sorted(entries)
    }

    fn checkpoint(&self, dir: &std::path::Path) -> Result<()> {
        self.inner.checkpoint(dir)
    }

    fn set_gc_filter(&self, filter: Box<dyn GcFilter>) {
        self.inner.set_gc_filter(filter);
    }

    fn set_gc_watermark(&self, watermark: u64) -> Result<()> {
        self.inner.set_gc_watermark(watermark)
    }
}

// ---- State building --------------------------------------------------------

/// A hand-written live version (§2.2), the preloaded-state primitive.
pub fn put_version(kv: &SharedKv, key: &[u8], ts: u64, value: &[u8]) {
    let mut batch = Batch::default();
    let encoded = encode_version(&LayerData::Write {
        value: value.to_vec(),
        key_changed: false,
    })
    .expect("a write layer always encodes");
    batch.put(version_key(key, Ts(ts)), encoded);
    kv.preload(batch);
}

/// A hand-written tombstone version (`Delete{moved}`, §2.2).
pub fn put_tombstone(kv: &SharedKv, key: &[u8], ts: u64, moved: bool) {
    let mut batch = Batch::default();
    let encoded =
        encode_version(&LayerData::Delete { moved }).expect("a delete layer always encodes");
    batch.put(version_key(key, Ts(ts)), encoded);
    kv.preload(batch);
}

/// Preloads `/sys/ts_hwm = ts` (boot: `visible_ts = ts_hwm`).
pub fn preload_ts_hwm(kv: &SharedKv, ts: u64) {
    let mut batch = Batch::default();
    batch.put(sys_ts_hwm_key(), ts.to_be_bytes().to_vec());
    kv.preload(batch);
}

/// Preloads `/sys/gc_w = ts` (boot: the durable W).
pub fn preload_gc_w(kv: &SharedKv, ts: u64) {
    let mut batch = Batch::default();
    batch.put(sys_gc_w_key(), ts.to_be_bytes().to_vec());
    kv.preload(batch);
}

/// Preloads a `/sys/ts_clock` sample: sampled `ts`, wall time `wall_secs`.
pub fn preload_ts_clock(kv: &SharedKv, ts: u64, wall_secs: u64) {
    let mut batch = Batch::default();
    batch.put(sys_ts_clock_key(Ts(ts)), wall_secs.to_be_bytes().to_vec());
    kv.preload(batch);
}

/// Places a one-layer write intent the §5.1 way: log, count, then one
/// batch under the key's latch.
pub fn place_write<K: OrderedKv>(core: &Core<K>, txn: &Txn, key: &[u8], value: &[u8]) {
    place_layer(
        core,
        txn,
        key,
        LayerData::Write {
            value: value.to_vec(),
            key_changed: false,
        },
        RowLockMode::NoKeyUpdate,
    );
}

/// Places a one-layer delete intent (`Delete{moved}`, §2.1).
pub fn place_delete<K: OrderedKv>(core: &Core<K>, txn: &Txn, key: &[u8], moved: bool) {
    place_layer(
        core,
        txn,
        key,
        LayerData::Delete { moved },
        RowLockMode::Update,
    );
}

fn place_layer<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    key: &[u8],
    data: LayerData,
    lock: RowLockMode,
) {
    let seq = ok(txn.next_seq());
    txn.log_write(seq, key);
    ok(core.count_placement(txn));
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![Layer {
            seq,
            data_seq: seq,
            data,
            lock,
        }],
    };
    let mut batch = Batch::default();
    batch.put(intent_key(key), ok(encode_intent(&intent)));
    // §5.0: the latch of a plain key is the key itself.
    let _latch = core.latches.lock(key);
    ok(core.write(batch, Durability::No));
}

/// Commits `txn` through a manual pipeline (§3 steps 1–5, on this thread)
/// and returns its commit ts.
pub fn commit<K: OrderedKv>(
    core: &Core<K>,
    pipeline: &mut CommitPipeline<K>,
    txn: Txn,
) -> std::result::Result<Ts, TxnError> {
    let (req, ack) = CommitRequest::new(txn.id, SyncCommit::On, None, false, txn.write_set_keys());
    core.submit(req)?;
    let group = pipeline.drain_available();
    pipeline.process_group(group);
    match ack.recv() {
        Ok(r) => r,
        Err(_) => Err(TxnError::Invariant("commit never acked".into())),
    }
}

/// Begins a txn and commits one op on `key` (write, or delete with
/// `value = None`), resolving it immediately; returns the commit ts.
pub fn commit_one<K: OrderedKv>(
    core: &Core<K>,
    pipeline: &mut CommitPipeline<K>,
    key: &[u8],
    value: Option<&[u8]>,
) -> Ts {
    let txn = core.begin(Isolation::ReadCommitted);
    match value {
        Some(v) => place_write(core, &txn, key, v),
        None => place_delete(core, &txn, key, false),
    }
    let ts = ok(commit(core, pipeline, txn));
    ok(Resolver::run_once(core));
    ts
}

/// Reads one logical key at `snapshot` through a fresh registered view
/// (§4). `reader` must not own any intent on the key.
pub fn read_at<K: OrderedKv>(
    core: &Core<K>,
    key: &[u8],
    snapshot: Ts,
    reader: TxnId,
) -> Option<Vec<u8>> {
    let view = core.open_view();
    read_through_view(core, &view, key, snapshot, reader)
}

/// Reads one logical key at `snapshot` through an existing view — the
/// open-view clause of I-GC (a view open across GC steps).
pub fn read_through_view<K: OrderedKv>(
    core: &Core<K>,
    view: &nucleus_txn::registry::ViewGuard<'_, K::Snap>,
    key: &[u8],
    snapshot: Ts,
    reader: TxnId,
) -> Option<Vec<u8>> {
    let ctx = ReadCtx {
        txn: reader,
        snapshot,
        stmt_seq: 1,
    };
    ok(read_key(core, view, key, &ctx, &mut NoSsi))
}

/// A reader id that owns nothing.
pub fn reader_id<K: OrderedKv>(core: &Core<K>) -> TxnId {
    TxnId {
        epoch: core.epoch(),
        n: u64::MAX,
    }
}

/// `/sys/gc_w`'s value through a fresh registered view (0 when absent).
pub fn sys_gc_w<K: OrderedKv>(core: &Core<K>) -> u64 {
    let view = core.open_view();
    match ok(view.get(&sys_gc_w_key())) {
        Some(v) => {
            let mut b = [0u8; 8];
            b.copy_from_slice(&v);
            u64::from_be_bytes(b)
        }
        None => 0,
    }
}

/// The id of the single live L0 file, for per-file compactions.
pub fn only_l0_file(kv: &SharedKv) -> u64 {
    let l0: Vec<u64> = kv
        .files()
        .into_iter()
        .filter(|(level, _, _)| *level == 0)
        .map(|(_, id, _)| id)
        .collect();
    assert_eq!(l0.len(), 1, "expected exactly one L0 file, got {l0:?}");
    l0[0]
}

// ---- Thread handshakes -----------------------------------------------------

/// A counted signal: `hit` counts and wakes every waiter; tests wait for a
/// count instead of sleeping (an assertion never passes on a timeout).
pub struct Flag {
    n: Mutex<u64>,
    cv: Condvar,
}

impl Flag {
    pub fn new() -> Flag {
        Flag {
            n: Mutex::new(0),
            cv: Condvar::new(),
        }
    }

    fn lock(&self) -> MutexGuard<'_, u64> {
        self.n.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn hit(&self) {
        let mut n = self.lock();
        *n += 1;
        self.cv.notify_all();
    }

    pub fn get(&self) -> u64 {
        *self.lock()
    }

    /// Waits until the count reaches `at_least`. Panics after `bound` — a
    /// timeout is a failure, never a pass.
    pub fn wait_at_least(&self, at_least: u64, bound: Duration) {
        let deadline = Instant::now() + bound;
        let mut n = self.lock();
        while *n < at_least {
            let now = Instant::now();
            if now >= deadline {
                panic!("flag reached only {} of {at_least} within {bound:?}", *n);
            }
            let (n2, timeout) = self
                .cv
                .wait_timeout(n, deadline - now)
                .unwrap_or_else(PoisonError::into_inner);
            n = n2;
            if timeout.timed_out() && *n < at_least && Instant::now() >= deadline {
                panic!("flag reached only {} of {at_least} within {bound:?}", *n);
            }
        }
    }
}

/// A [`Clock`] that counts its calls (each one signals `ticks`).
pub struct CountingClock {
    secs: AtomicU64,
    pub ticks: Flag,
}

impl CountingClock {
    pub fn new() -> Arc<CountingClock> {
        Arc::new(CountingClock {
            secs: AtomicU64::new(0),
            ticks: Flag::new(),
        })
    }
}

impl Clock for CountingClock {
    fn now_secs(&self) -> u64 {
        let n = self.secs.fetch_add(1, Ordering::SeqCst);
        self.ticks.hit();
        n
    }
}

/// A [`FailStop`] that records errors and signals `hits` once per error.
pub struct RecFailStop {
    errs: Mutex<Vec<TxnError>>,
    pub hits: Flag,
}

impl RecFailStop {
    pub fn new() -> Arc<RecFailStop> {
        Arc::new(RecFailStop {
            errs: Mutex::new(Vec::new()),
            hits: Flag::new(),
        })
    }

    pub fn errors(&self) -> Vec<TxnError> {
        self.errs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl FailStop for RecFailStop {
    fn on_kv_error(&self, err: &TxnError) {
        self.errs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(err.clone());
        self.hits.hit();
    }
}
