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

/// Waits until commit step 5 has run for `id` (`released`). The ack (step 4)
/// legally precedes step 5, so tests that need release/resolution-queueing
/// poll for it.
pub fn wait_released<K: OrderedKv>(core: &nucleus_txn::boot::Core<K>, id: nucleus_txn::TxnId) {
    for _ in 0..10_000 {
        if core.status.entry(id).is_some_and(|e| e.released) {
            return;
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
    let seq = txn.next_seq();
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
