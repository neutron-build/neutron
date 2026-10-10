//! C-T7r2 rework tests: the three failure modes of the C-T7 sidecar,
//! pinned against the Core-owned seq state.
//!
//! 1. **Hook swap mid-reservation** — a gated KV parks thread A inside its
//!    reservation's synced write; a `CommitPipeline` (the public
//!    `set_fail_stop` path) installs a fresh fail-stop hook; thread B calls
//!    `seq_next` on the same id; A is released. The two values must be
//!    distinct and covered by the durable `H`. The sidecar minted a second
//!    module for the same core here and handed out the same value twice.
//! 2. **Shared hook across two cores** — two cores over two separate
//!    stores configured with one `Arc<dyn FailStop>`: each core's values
//!    start at 1 and are dense within its first block, each store carries
//!    its own `H`, and a crash + reboot of each store repeats no value.
//!    The sidecar aliased both cores onto one module: store B's first
//!    value was 2 and its `/sys/seq/{id}` was never written.
//! 3. **Failed-reservation readback** — a faulting KV makes the
//!    reservation write fail; the error propagates, the state is left as
//!    before the call, and after every successful `seq_next` the returned
//!    value is `<=` the durable `H` read back from the store. This is the
//!    mutant pin for "persist before advance".

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

use nucleus_kv::fault::Fault;
use nucleus_kv::{Batch, Durability, GcFilter, Key, KvError, MemKv, Op, OrderedKv, Result, Value};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{CommitConfig, CommitPipeline, FailStop};
use nucleus_txn::seq::sys_seq_key;
use nucleus_txn::TxnError;

fn ok<T, E: std::fmt::Debug>(r: std::result::Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

/// Reads `/sys/seq/{id}` straight from the store.
fn hwm_of(kv: &impl OrderedKv, id: u64) -> Option<u64> {
    let v = ok(kv.get_latest(&sys_seq_key(id)));
    v.map(|bytes| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&bytes);
        u64::from_be_bytes(b)
    })
}

/// A fail-stop hook that does nothing: installing it is still a hook swap
/// (the core's slot takes a fresh `Arc`).
struct NopFailStop;

impl FailStop for NopFailStop {
    fn on_kv_error(&self, _err: &TxnError) {}
}

/// The parking lot of [`GatedKv`]: the `at`-th write to the gated key and
/// every later one parks inside `write` (before the write applies) until
/// `release`. `parked` counts parked writers and wakes the coordinator,
/// so a test knows exactly when a thread is mid-write — no sleeps.
struct Gate {
    at: u64,
    st: Mutex<GateState>,
    parked_cv: Condvar,
    release_cv: Condvar,
}

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

    /// Blocks until `n` writers are parked.
    fn wait_until_parked(&self, n: u64) {
        let mut st = self.st.lock().expect("gate");
        while st.parked < n {
            st = self.parked_cv.wait(st).expect("gate");
        }
    }

    /// Waits a bounded margin for `n` parked writers; returns whether they
    /// arrived. The margin is *not* the synchronisation — the gate's
    /// condvars are. It exists only because a correct core can never
    /// satisfy `n >= 2` in the hook-swap test (thread B cannot reach the
    /// KV while thread A holds the seq mutex across its synced write), so
    /// there is nothing to block on; the timeout lets the test proceed and
    /// assert on values instead.
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

/// An `OrderedKv` wrapper that parks writes touching one key in a
/// [`Gate`]. Clones share the inner store and the gate.
#[derive(Clone)]
struct GatedKv {
    inner: Arc<MemKv>,
    key: Key,
    gate: Arc<Gate>,
}

impl GatedKv {
    /// Parks the `at`-th write to `key` and every later one until release.
    fn new(key: Key, at: u64) -> GatedKv {
        GatedKv {
            inner: Arc::new(MemKv::new()),
            key,
            gate: Arc::new(Gate::new(at)),
        }
    }
}

impl OrderedKv for GatedKv {
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

/// An `OrderedKv` wrapper sharing one `Fault<MemKv>` with the test, so the
/// store can be crashed and reopened (`crash(0)` drops the unsynced
/// suffix; every seq write is synced, so `H` survives). Clones share the
/// store.
#[derive(Clone)]
struct SharedFaultKv {
    inner: Arc<Fault<MemKv>>,
}

impl SharedFaultKv {
    fn new() -> SharedFaultKv {
        SharedFaultKv {
            inner: Arc::new(Fault::new(MemKv::new(), || Ok(MemKv::new()))),
        }
    }
}

impl OrderedKv for SharedFaultKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
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

/// An `OrderedKv` wrapper whose writes fail once `fail` is set (reads keep
/// working). Clones share the inner store and the flag.
#[derive(Clone)]
struct FailKv {
    inner: Arc<MemKv>,
    fail: Arc<AtomicBool>,
}

impl FailKv {
    fn new() -> FailKv {
        FailKv {
            inner: Arc::new(MemKv::new()),
            fail: Arc::new(AtomicBool::new(false)),
        }
    }
}

impl OrderedKv for FailKv {
    type Snap = <MemKv as OrderedKv>::Snap;

    fn write(&self, batch: Batch, sync: Durability) -> Result<()> {
        if self.fail.load(Ordering::SeqCst) {
            Err(KvError::Backend("injected failure".into()))
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

// ---- 1. hook swap mid-reservation -----------------------------------------

/// A KV gate parks thread A inside its reservation's synced write (write 2
/// to `/sys/seq/0`: write 1 is the creation write). With A parked, a fresh
/// `CommitPipeline` installs a new fail-stop hook on the core — the public
/// hook-swap path — then thread B calls `seq_next` on the same id and A is
/// released.
///
/// With the seq state owned by `Core`, B blocks on the seq mutex A holds
/// across the synced write: it cannot reach the KV at all, and after A
/// finishes it draws the next value of the same block. The two values are
/// distinct and both `<=` the durable `H`.
///
/// The old sidecar minted a *second* module for the same core on the hook
/// swap: B reloaded `H = 0` (A's reservation write had not applied yet),
/// reserved the same block and both threads returned 1.
#[test]
fn hook_swap_mid_reservation_hands_out_distinct_values() {
    let kv = GatedKv::new(sys_seq_key(0), 2);
    let core = Arc::new(ok(Core::open(kv.clone())));
    core.seq_set_block_for_tests(4);

    let core_a = Arc::clone(&core);
    let handle_a = thread::spawn(move || core_a.seq_next(0));
    kv.gate.wait_until_parked(1); // A is inside the reservation's synced write

    // The hook swap: CommitPipeline::with_config -> Core::set_fail_stop.
    let _pipeline = ok(CommitPipeline::with_config(
        Arc::clone(&core),
        CommitConfig::new().with_fail_stop(Arc::new(NopFailStop)),
    ));

    // B calls seq_next on the same id while A is still parked. Under the
    // fixed code B cannot pass the seq mutex, so it can never reach the KV
    // while A is parked; under the old sidecar B's fresh module wrote to
    // /sys/seq/0 and parked at the gate too. The bounded margin is extra
    // safety only — the synchronisation is the gate itself.
    let core_b = Arc::clone(&core);
    let handle_b = thread::spawn(move || core_b.seq_next(0));
    let b_reached_the_kv = kv.gate.wait_until_parked_margin(2, Duration::from_secs(2));

    kv.gate.release();
    let a = ok(handle_a.join().expect("thread A"));
    let b = ok(handle_b.join().expect("thread B"));

    assert!(
        !b_reached_the_kv,
        "thread B reached /sys/seq/0 while A was parked mid-reservation: \
         the seq state is not owned by the core"
    );
    assert_ne!(a, b, "the hook swap handed out the same value twice");
    assert_eq!((a, b), (1, 2), "values 1 and 2 of the same first block");
    let h = hwm_of(&kv, 0).expect("the reservation is durable");
    assert!(h >= a as u64, "durable H {h} does not cover handed-out {a}");
    assert!(h >= b as u64, "durable H {h} does not cover handed-out {b}");
    assert_eq!(
        core.seq_reservations_for_tests(0),
        Some(1),
        "one reservation for the whole block, across the hook swap"
    );
}

// ---- 2. shared hook across two cores ---------------------------------------

/// `CommitConfig::with_fail_stop` is public API: two cores over two
/// separate stores may share one hook `Arc`. Their sequences must stay
/// separate. The old sidecar resolved both cores to the one module keyed
/// on the shared hook: store B's first value was 2 and its `/sys/seq/0`
/// was never written (durable `H = None`), so a crash restarted B at 1 and
/// repeated values.
#[test]
fn shared_fail_stop_hook_across_two_cores_does_not_alias_them() {
    let hook: Arc<dyn FailStop> = Arc::new(NopFailStop);
    let kv_a = SharedFaultKv::new();
    let kv_b = SharedFaultKv::new();

    let (a_vals, b_vals) = {
        let core_a = Arc::new(ok(Core::open(kv_a.clone())));
        ok(CommitPipeline::with_config(
            Arc::clone(&core_a),
            CommitConfig::new().with_fail_stop(Arc::clone(&hook)),
        ));
        let core_b = Arc::new(ok(Core::open(kv_b.clone())));
        ok(CommitPipeline::with_config(
            Arc::clone(&core_b),
            CommitConfig::new().with_fail_stop(Arc::clone(&hook)),
        ));
        core_a.seq_set_block_for_tests(4);
        core_b.seq_set_block_for_tests(4);

        // Interleave the two cores on the same sequence id: 6 values each
        // crosses into each store's second block (H: 4 -> 8).
        let mut a_vals = Vec::new();
        let mut b_vals = Vec::new();
        for _ in 0..6 {
            a_vals.push(ok(core_a.seq_next(0)));
            b_vals.push(ok(core_b.seq_next(0)));
        }
        (a_vals, b_vals)
    }; // cores and pipelines drop here, before the crashes

    assert_eq!(a_vals, vec![1, 2, 3, 4, 5, 6], "core A: dense from 1");
    assert_eq!(
        b_vals,
        vec![1, 2, 3, 4, 5, 6],
        "core B: its own sequence, dense from 1 (the old sidecar handed it 2 first)"
    );
    assert_eq!(hwm_of(&kv_a, 0), Some(8), "A's store carries A's H");
    assert_eq!(
        hwm_of(&kv_b, 0),
        Some(8),
        "B's store carries B's H (the old sidecar never wrote it)"
    );

    // Crash and reboot each store: no handed-out value repeats.
    ok(kv_a.inner.crash(0));
    ok(kv_b.inner.crash(0));
    let core_a2 = ok(Core::open(kv_a.clone()));
    core_a2.seq_set_block_for_tests(4);
    let core_b2 = ok(Core::open(kv_b.clone()));
    core_b2.seq_set_block_for_tests(4);
    for (core, tag) in [(&core_a2, "A"), (&core_b2, "B")] {
        let first = ok(core.seq_next(0));
        assert_eq!(first, 9, "{tag}: reboot boots from the durable H = 8");
        for v in a_vals.iter().chain(&b_vals) {
            assert!(
                first > *v,
                "{tag}: first value after reboot {first} repeats pre-crash value {v}"
            );
        }
        assert_eq!(
            ok(core.seq_next(0)),
            10,
            "{tag}: dense inside the new block"
        );
    }
}

// ---- 3. failed-reservation readback ----------------------------------------

/// The reservation write fails (fault KV): the error propagates and the
/// in-memory state stays exactly as before the call, so the retry hands
/// out the failed value again. After every successful `seq_next` the
/// returned value is `<=` the durable `H` read back from the store, and
/// never below a previously handed-out value. A mutant that advances
/// `hwm`/`cursor` before the synced write (still propagating the error)
/// hands out a value on the retry that the durable `H` does not cover —
/// the readback catches it.
#[test]
fn failed_reservation_readback_durable_h_covers_every_handout() {
    let kv = FailKv::new();
    let core = ok(Core::open(kv.clone()));
    core.seq_set_block_for_tests(2);
    let id = 0u64;
    let mut max_handed = 0i64;

    // Every successful handout: strictly above every earlier one, and
    // covered by the durable H read back from the store.
    let mut handout = || {
        let v = ok(core.seq_next(id));
        assert!(
            v > max_handed,
            "value {v} is below a previously handed-out {max_handed}"
        );
        max_handed = v;
        let h = hwm_of(&kv, id).expect("a successful handout has a durable H");
        assert!(h >= v as u64, "handed out {v} but durable H is only {h}");
        v
    };

    // First block: 1, 2 with H = 2 durable.
    assert_eq!(handout(), 1);
    assert_eq!(handout(), 2);
    assert_eq!(hwm_of(&kv, id), Some(2));

    // The reservation for value 3 fails: the error propagates and neither
    // the durable nor the in-memory state may move.
    kv.fail.store(true, Ordering::SeqCst);
    assert!(
        matches!(core.seq_next(id), Err(TxnError::Kv(_))),
        "the failed reservation propagates its error"
    );
    assert_eq!(
        hwm_of(&kv, id),
        Some(2),
        "the failed call left the durable H alone"
    );
    kv.fail.store(false, Ordering::SeqCst);
    assert_eq!(
        handout(),
        3,
        "the failed value is retried, not skipped into"
    );
    assert_eq!(hwm_of(&kv, id), Some(4));

    // Value 4 stays inside the reserved block: no write.
    assert_eq!(handout(), 4);
    assert_eq!(hwm_of(&kv, id), Some(4));

    // Another failed reservation at the next boundary, then a retry.
    kv.fail.store(true, Ordering::SeqCst);
    assert!(matches!(core.seq_next(id), Err(TxnError::Kv(_))));
    kv.fail.store(false, Ordering::SeqCst);
    assert_eq!(handout(), 5);
    assert_eq!(hwm_of(&kv, id), Some(6));

    // A failed CREATION write of a second id: the error propagates,
    // nothing is created, and the retry creates the sequence at 1.
    kv.fail.store(true, Ordering::SeqCst);
    assert!(matches!(core.seq_next(1), Err(TxnError::Kv(_))));
    assert_eq!(
        hwm_of(&kv, 1),
        None,
        "nothing was created by the failed call"
    );
    kv.fail.store(false, Ordering::SeqCst);
    let v = ok(core.seq_next(1));
    assert_eq!(v, 1);
    let h = hwm_of(&kv, 1).expect("created and reserved on the retry");
    assert!(h >= v as u64, "handed out {v} but durable H is only {h}");
}
