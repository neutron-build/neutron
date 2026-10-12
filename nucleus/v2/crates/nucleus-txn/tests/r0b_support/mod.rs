//! Shared support for the C-R0bb tests (`tests/r0b_seq_isolation.rs`,
//! `tests/r0b_fsync_gate.rs`): the modelled slow-fsync KV, the parking
//! gate KV and the latency sample bag. Test crates cannot share code, so
//! `GateKv` copies the shape of `seq_rework.rs`'s `GatedKv`.
#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use nucleus_kv::{Batch, Durability, GcFilter, Key, MemKv, Op, OrderedKv, Result, Value};

pub fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

// ---- SlowSyncKv -------------------------------------------------------------

/// An `OrderedKv` wrapper that models a backend whose **fsync is slow**
/// but whose write path is not held across the sync (the C-B0 contract,
/// rule R4 of `rt.rs`): while `armed`, every durable write (`write(_, Yes)`
/// and `sync_wal`) first sleeps `delay` and counts one stall in `stalls`,
/// then delegates. `write(_, No)` and every read delegate immediately and
/// never wait for a stalled sync — that is exactly rule R4, and it is what
/// keeps the gate's KvIo-class probes honest: their latencies measure the
/// engine's locks, not the backend's.
///
/// It does **not** model compaction stalls (a C-B0/C-K3 concern: a
/// compaction that holds a read path is a different bug with different
/// symptoms), only the commit-path fsync.
#[derive(Clone)]
pub struct SlowSyncKv {
    inner: Arc<MemKv>,
    armed: Arc<AtomicBool>,
    delay: Duration,
    stalls: Arc<AtomicU64>,
}

impl SlowSyncKv {
    /// A store whose durable writes will stall for `delay` once armed.
    pub fn new(delay: Duration) -> SlowSyncKv {
        SlowSyncKv {
            inner: Arc::new(MemKv::new()),
            armed: Arc::new(AtomicBool::new(false)),
            delay,
            stalls: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Turns the fsync delay on. Setup work done before arming never stalls.
    pub fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// How many stalled durable writes have completed.
    pub fn stalls(&self) -> u64 {
        self.stalls.load(Ordering::SeqCst)
    }

    /// The modelled fsync delay shared by every clone.
    fn stall(&self) {
        if self.armed.load(Ordering::SeqCst) {
            std::thread::sleep(self.delay);
            self.stalls.fetch_add(1, Ordering::SeqCst);
        }
    }
}

impl OrderedKv for SlowSyncKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if sync == Durability::Yes {
            self.stall();
        }
        self.inner.write(batch, sync)
    }

    fn sync_wal(&self) -> Result<()> {
        self.stall();
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

// ---- GateKv -----------------------------------------------------------------

/// The parking lot of [`GateKv`]: the `at`-th write that puts the gated
/// key and every later one parks inside `write` (before the write applies)
/// until `release`. `parked` counts parked writers and wakes the
/// coordinator, so a test knows exactly when a thread is mid-write — no
/// sleeps.
///
/// The shape is `seq_rework.rs`'s `Gate` (test crates cannot share code).
/// One deliberate difference: `wait_until_parked` reports a timeout by
/// returning `false` instead of panicking, so this card's caller can
/// `release()` before failing — the "never hangs" rule of
/// `r0b_seq_isolation.rs`.
struct Gate {
    at: u64,
    st: Mutex<GateState>,
    parked_cv: Condvar,
    release_cv: Condvar,
}

/// How long [`Gate::wait_until_parked`] waits before reporting `false`.
const GATE_TIMEOUT: Duration = Duration::from_secs(10);

struct GateState {
    count: u64,
    parked: u64,
    released: bool,
}

impl Gate {
    fn new(at: u64) -> Gate {
        Gate {
            at,
            st: Mutex::new(GateState {
                count: 0,
                parked: 0,
                released: false,
            }),
            parked_cv: Condvar::new(),
            release_cv: Condvar::new(),
        }
    }

    /// Writer side, before the write applies. Parks while the gate holds.
    fn arrive(&self) {
        let mut st = self.st.lock().expect("gate");
        st.count += 1;
        if st.count >= self.at && !st.released {
            st.parked += 1;
            self.parked_cv.notify_all();
            while !st.released {
                st = self.release_cv.wait(st).expect("gate");
            }
        }
    }

    /// Blocks until `n` writers are parked. Returns `false` on timeout
    /// (after [`GATE_TIMEOUT`]) instead of panicking, so the caller can
    /// release the gate before failing the test.
    fn wait_until_parked(&self, n: u64) -> bool {
        let mut st = self.st.lock().expect("gate");
        while st.parked < n {
            let (guard, timed_out) = self.parked_cv.wait_timeout(st, GATE_TIMEOUT).expect("gate");
            st = guard;
            if timed_out.timed_out() {
                return false;
            }
        }
        true
    }

    /// How many writers have arrived at the gate, parked or not: every
    /// write that puts the gated key counts, before and after `release`.
    fn arrivals(&self) -> u64 {
        self.st.lock().expect("gate").count
    }

    /// Waits a bounded margin for `n` parked writers; returns whether they
    /// arrived. The margin is **extra safety only, never the
    /// synchronisation** (the C-T7r3 rule): on code where the parked
    /// writer's lock excludes the checked caller this always burns the
    /// margin, so it is kept small.
    fn wait_until_parked_margin(&self, n: u64, margin: Duration) -> bool {
        let mut st = self.st.lock().expect("gate");
        while st.parked < n {
            let (guard, timeout) = self.parked_cv.wait_timeout(st, margin).expect("gate");
            st = guard;
            if timeout.timed_out() {
                break;
            }
        }
        st.parked >= n
    }

    /// Lets every parked (and future) writer through.
    fn release(&self) {
        let mut st = self.st.lock().expect("gate");
        st.released = true;
        drop(st);
        self.release_cv.notify_all();
    }
}

/// An `OrderedKv` wrapper that parks writes putting one chosen key, from
/// the `at`-th such write onward, until released. Clones share the inner
/// store and the gate. The shape of `seq_rework.rs`'s `GatedKv` (test
/// crates cannot share code).
#[derive(Clone)]
pub struct GateKv {
    inner: Arc<MemKv>,
    key: Key,
    gate: Arc<Gate>,
}

impl GateKv {
    /// Parks the `at`-th write that puts `key` and every later one until
    /// release.
    pub fn new(key: Key, at: u64) -> GateKv {
        GateKv {
            inner: Arc::new(MemKv::new()),
            key,
            gate: Arc::new(Gate::new(at)),
        }
    }

    /// Blocks until `n` writers are parked; `false` on timeout, so the
    /// caller can [`GateKv::release`] before failing (never hang).
    pub fn wait_until_parked(&self, n: u64) -> bool {
        self.gate.wait_until_parked(n)
    }

    /// A bounded wait for `n` parked writers — extra safety only, never
    /// the synchronisation.
    pub fn wait_until_parked_margin(&self, n: u64, margin: Duration) -> bool {
        self.gate.wait_until_parked_margin(n, margin)
    }

    /// How many writes have put the gated key, parked or not.
    pub fn arrivals(&self) -> u64 {
        self.gate.arrivals()
    }

    /// Lets every parked (and future) writer through.
    pub fn release(&self) {
        self.gate.release()
    }
}

impl OrderedKv for GateKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if batch
            .ops
            .iter()
            .any(|op| matches!(op, Op::Put(k, _) if k.as_slice() == self.key.as_slice()))
        {
            self.gate.arrive();
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

// ---- Lat --------------------------------------------------------------------

/// A bag of latency samples with nearest-rank percentiles.
#[derive(Default)]
pub struct Lat {
    samples: Vec<Duration>,
}

impl Lat {
    pub fn new() -> Lat {
        Lat::default()
    }

    /// Records one sample.
    pub fn record(&mut self, d: Duration) {
        self.samples.push(d);
    }

    /// How many samples were recorded.
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether no sample was recorded.
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// The nearest-rank p99: the `ceil(0.99·n)`-th smallest sample. With
    /// fewer than 100 samples this is the maximum.
    pub fn p99(&self) -> Duration {
        self.percentile(99)
    }

    /// The nearest-rank p-th percentile (0 < p <= 100): the
    /// `ceil(p/100·n)`-th smallest sample.
    fn percentile(&self, p: usize) -> Duration {
        match self.samples.len() {
            0 => Duration::ZERO,
            n => {
                let mut sorted = self.samples.clone();
                sorted.sort_unstable();
                // ceil(p·n / 100), at least 1, at most n.
                let rank = ((p * n + 99) / 100).clamp(1, n);
                sorted[rank - 1]
            }
        }
    }

    /// The smallest sample (`Duration::ZERO` with no samples).
    pub fn min(&self) -> Duration {
        self.samples.iter().min().copied().unwrap_or(Duration::ZERO)
    }

    /// The largest sample (`Duration::ZERO` with no samples).
    pub fn max(&self) -> Duration {
        self.samples.iter().max().copied().unwrap_or(Duration::ZERO)
    }

    /// One evidence line: `name: n=… p99=… min=… max=…`.
    pub fn summary(&self, name: &str) -> String {
        format!(
            "{name}: n={} p99={:?} min={:?} max={:?}",
            self.len(),
            self.p99(),
            self.min(),
            self.max()
        )
    }
}
