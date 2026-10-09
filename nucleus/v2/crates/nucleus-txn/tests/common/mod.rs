//! Shared test support for the C-T1b tests: a recording KV wrapper (WAL
//! order, §3 I-WAL-ORDER) and a failing one (fail-stop). Each test binary
//! compiles this module and uses a different subset of it.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use nucleus_kv::{Batch, Durability, GcFilter, Key, KvError, MemKv, Op, OrderedKv, Result, Value};

pub fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// One logged KV event, in call order: a write batch's ops (cloned), or a
/// `sync_wal`.
#[derive(Clone, Debug)]
pub enum Event {
    Write(Vec<Op>),
    Sync,
}

/// An `OrderedKv` wrapper that records every non-empty write batch and every
/// `sync_wal`, in call order, so tests can assert WAL order (I-WAL-ORDER).
/// Clones share the inner store and the log.
#[derive(Clone)]
pub struct RecKv {
    inner: Arc<MemKv>,
    log: Arc<Mutex<Vec<Event>>>,
}

impl RecKv {
    pub fn new() -> RecKv {
        RecKv {
            inner: Arc::new(MemKv::new()),
            log: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// The event log so far.
    pub fn log(&self) -> Vec<Event> {
        self.log.lock().expect("rec log").clone()
    }
}

impl Default for RecKv {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderedKv for RecKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if !batch.ops.is_empty() {
            self.log
                .lock()
                .expect("rec log")
                .push(Event::Write(batch.ops.clone()));
        }
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.log.lock().expect("rec log").push(Event::Sync);
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

/// An `OrderedKv` wrapper whose writes (and syncs) fail with
/// `KvError::Backend` once `fail` is set: the fail-stop trigger. Clones
/// share the inner store and the flag.
#[derive(Clone)]
pub struct FailKv {
    inner: Arc<MemKv>,
    fail: Arc<AtomicBool>,
}

impl FailKv {
    pub fn new(fail: Arc<AtomicBool>) -> FailKv {
        FailKv {
            inner: Arc::new(MemKv::new()),
            fail,
        }
    }

    pub fn shared() -> (FailKv, Arc<AtomicBool>) {
        let fail = Arc::new(AtomicBool::new(false));
        (FailKv::new(Arc::clone(&fail)), fail)
    }

    fn check(&self) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            Err(KvError::Backend("injected failure".into()))
        } else {
            Ok(())
        }
    }
}

impl OrderedKv for FailKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.check()?;
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.check()?;
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

/// A fake [`Clock`](nucleus_txn::commit::Clock) the test steps by hand.
#[derive(Default)]
pub struct FakeClock {
    secs: AtomicU64,
}

impl FakeClock {
    pub fn new(secs: u64) -> Arc<FakeClock> {
        Arc::new(FakeClock {
            secs: AtomicU64::new(secs),
        })
    }

    pub fn set(&self, secs: u64) {
        self.secs.store(secs, Ordering::SeqCst);
    }
}

impl nucleus_txn::commit::Clock for FakeClock {
    fn now_secs(&self) -> u64 {
        self.secs.load(Ordering::SeqCst)
    }
}

/// An `OrderedKv` wrapper whose writes fail once the `fail_at`th write is
/// reached (and every write after), while reads keep working. Clones share
/// the counter; `fail_at` defaults to "never".
#[derive(Clone)]
pub struct FailNthKv {
    inner: Arc<MemKv>,
    writes: Arc<AtomicU64>,
    fail_at: Arc<AtomicU64>,
}

impl FailNthKv {
    pub fn new() -> FailNthKv {
        FailNthKv {
            inner: Arc::new(MemKv::new()),
            writes: Arc::new(AtomicU64::new(0)),
            fail_at: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    /// The nth write (1-based, counting every write since construction)
    /// and everything after it fails.
    pub fn fail_from(&self, nth: u64) {
        self.fail_at.store(nth, Ordering::SeqCst);
    }

    /// Writes so far.
    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }
}

impl Default for FailNthKv {
    fn default() -> Self {
        Self::new()
    }
}

impl OrderedKv for FailNthKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if n >= self.fail_at.load(Ordering::SeqCst) {
            Err(KvError::Backend("injected nth-write failure".into()))
        } else {
            self.inner.write(batch, sync)
        }
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

/// An `OrderedKv` wrapper whose `sync_wal` fails (and counts) once `fail`
/// is set; writes keep succeeding — the fail-stop trigger for a group whose
/// records are all written.
#[derive(Clone)]
pub struct FailSyncKv {
    inner: Arc<MemKv>,
    fail: Arc<AtomicBool>,
    syncs: Arc<AtomicU64>,
}

impl FailSyncKv {
    pub fn shared() -> (FailSyncKv, Arc<AtomicBool>) {
        let fail = Arc::new(AtomicBool::new(false));
        (
            FailSyncKv {
                inner: Arc::new(MemKv::new()),
                fail: Arc::clone(&fail),
                syncs: Arc::new(AtomicU64::new(0)),
            },
            fail,
        )
    }

    /// `sync_wal` calls so far (attempted, failed or not).
    pub fn syncs(&self) -> u64 {
        self.syncs.load(Ordering::SeqCst)
    }
}

impl OrderedKv for FailSyncKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.syncs.fetch_add(1, Ordering::SeqCst);
        if self.fail.load(Ordering::SeqCst) {
            Err(KvError::Backend("injected sync failure".into()))
        } else {
            self.inner.sync_wal()
        }
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

/// An `OrderedKv` wrapper that simulates a crash at a chosen write: when
/// the `trigger`th write arrives (one-shot), it first crashes the inner
/// [`Fault`] — keeping `keep` unsynced batches — and then fails that
/// write, so a manually driven pipeline fail-stops mid-group and the store
/// reopens from the crashed state. Reads and snapshots see the post-crash
/// live store.
pub struct CrashAt<B: OrderedKv> {
    inner: Arc<nucleus_kv::fault::Fault<B>>,
    writes: Arc<AtomicU64>,
    trigger: Arc<AtomicU64>,
    keep: Arc<AtomicU64>,
    fired: Arc<AtomicBool>,
}

// Every field is an Arc, so clones share the crash state without `B:
// Clone`.
impl<B: OrderedKv> Clone for CrashAt<B> {
    fn clone(&self) -> Self {
        CrashAt {
            inner: Arc::clone(&self.inner),
            writes: Arc::clone(&self.writes),
            trigger: Arc::clone(&self.trigger),
            keep: Arc::clone(&self.keep),
            fired: Arc::clone(&self.fired),
        }
    }
}

impl<B: OrderedKv> CrashAt<B> {
    pub fn new(inner: nucleus_kv::fault::Fault<B>) -> CrashAt<B> {
        CrashAt {
            inner: Arc::new(inner),
            writes: Arc::new(AtomicU64::new(0)),
            trigger: Arc::new(AtomicU64::new(u64::MAX)),
            keep: Arc::new(AtomicU64::new(0)),
            fired: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The next write at (or after) which the crash fires.
    pub fn crash_at_write(&self, nth: u64, keep: u64) {
        self.trigger.store(nth, Ordering::SeqCst);
        self.keep.store(keep, Ordering::SeqCst);
    }

    pub fn writes(&self) -> u64 {
        self.writes.load(Ordering::SeqCst)
    }
}

impl<B: OrderedKv> OrderedKv for CrashAt<B> {
    type Snap = B::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        let n = self.writes.fetch_add(1, Ordering::SeqCst) + 1;
        if !self.fired.load(Ordering::SeqCst) && n >= self.trigger.load(Ordering::SeqCst) {
            self.fired.store(true, Ordering::SeqCst);
            self.inner
                .crash(self.keep.load(Ordering::SeqCst) as usize)?;
            return Err(KvError::Backend("crashed at a chosen write".into()));
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

/// Records that `id` was committed (the caller holds the ack). Used by
/// [`wait_released`]'s missing-entry rule: an entry that vanished may only
/// count as released for a txn known to have committed.
pub fn note_committed(id: nucleus_txn::TxnId) {
    seen_committed().lock().expect("seen").insert(id);
}

/// TxnIds this test process has observed `Committed` at least once, for
/// [`wait_released`]'s missing-entry rule.
fn seen_committed() -> &'static std::sync::Mutex<std::collections::HashSet<nucleus_txn::TxnId>> {
    static SEEN: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashSet<nucleus_txn::TxnId>>,
    > = std::sync::OnceLock::new();
    SEEN.get_or_init(|| std::sync::Mutex::new(std::collections::HashSet::new()))
}

/// Waits until commit step 5 has run for `id` (`released`). The ack (step 4)
/// legally precedes step 5, so tests that need release/resolution-queueing
/// poll for it. A missing entry counts **only** if the txn was previously
/// seen `Committed` — by this helper's polls, or by a caller's
/// [`note_committed`] after an acked commit (after which the resolver may
/// have truncated the entry before this helper first looked). An entry that
/// vanishes without the txn ever being known committed is a failure, not a
/// pass.
pub fn wait_released<K: OrderedKv>(core: &nucleus_txn::boot::Core<K>, id: nucleus_txn::TxnId) {
    for _ in 0..10_000 {
        match core.status.entry(id) {
            Some(e) => {
                if matches!(e.status, nucleus_txn::TxnStatus::Committed(_)) {
                    note_committed(id);
                }
                if e.released {
                    return;
                }
            }
            None => {
                if seen_committed().lock().expect("seen").contains(&id) {
                    return;
                }
            }
        }
        std::thread::sleep(Duration::from_micros(200));
    }
    panic!("step 5 never ran for {id:?}");
}

/// Places an intent the way §5.1 will: latch, log, count, then the write.
pub fn place_intent<K: OrderedKv>(
    core: &nucleus_txn::boot::Core<K>,
    txn: &nucleus_txn::txn::Txn,
    key: &[u8],
    value: &[u8],
) {
    let seq = ok(txn.next_seq());
    txn.log_write(seq, key);
    ok(core.count_placement(txn));
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![nucleus_txn::Layer {
            seq,
            data_seq: seq,
            data: nucleus_txn::LayerData::Write {
                value: value.to_vec(),
                key_changed: false,
            },
            lock: nucleus_txn::RowLockMode::NoKeyUpdate,
        }],
    };
    let mut batch = Batch::default();
    batch.put(
        nucleus_txn::encoding::intent_key(key),
        ok(nucleus_txn::encoding::encode_intent(&intent)),
    );
    // §5.0: the latch of a plain key is the key itself (no deferrable
    // prefix).
    let _latch = core.latches.lock(key);
    ok(core.write(batch, Durability::No));
}
