//! C-T1a tests: the snapshot/view registry (§3.1, §9.1) — `register_at`
//! bounds, `W` monotonicity, the computed min, guard unregistration, and the
//! sweep counter.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Mutex};

use nucleus_kv::{MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::{Ts, TxnError};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn core_at(visible: u64) -> Core<MemKv> {
    let core = ok(Core::open(MemKv::new()));
    core.advance_visible_ts(Ts(visible));
    core
}

#[test]
fn register_at_bounds() {
    let core = core_at(10);
    assert_eq!(core.registry.published_w(), Ts(0));
    // Publish through the one call (§9.1): the floor lands in the min.
    assert_eq!(core.registry.publish_computed_w(Some(Ts(5))), Ts(5));
    // t >= W and t <= visible_ts, else 72000 (§3.1).
    assert!(matches!(
        core.registry.register_at(Ts(4)),
        Err(TxnError::SnapshotTooOld)
    ));
    assert_eq!(ok(core.registry.register_at(Ts(5))).ts(), Ts(5));
    assert_eq!(ok(core.registry.register_at(Ts(10))).ts(), Ts(10));
    assert!(matches!(
        core.registry.register_at(Ts(11)),
        Err(TxnError::SnapshotTooOld)
    ));
}

#[test]
fn w_is_monotonic() {
    let core = core_at(10);
    assert_eq!(core.registry.publish_computed_w(Some(Ts(7))), Ts(7));
    assert_eq!(core.registry.publish_computed_w(Some(Ts(3))), Ts(7));
    assert_eq!(core.registry.publish_computed_w(Some(Ts(9))), Ts(9));
    // W is loaded from /sys/gc_w at boot and never decreases across boots.
    let mut batch = nucleus_kv::Batch::default();
    batch.put(
        nucleus_txn::encoding::sys_gc_w_key(),
        6u64.to_be_bytes().to_vec(),
    );
    ok(core.write(batch, nucleus_kv::Durability::Yes));
    let core2 = ok(Core::open(core.into_kv()));
    assert_eq!(core2.registry.published_w(), Ts(6));
    assert_eq!(core2.registry.publish_computed_w(Some(Ts(2))), Ts(6));
}

#[test]
fn computed_min_covers_snapshots_caller_ts_view_vts_and_visible_ts() {
    let core = core_at(10);
    let s1 = core.registry.take_snapshot(); // ts 10
    let caller = core
        .registry
        .register_at(Ts(6))
        .unwrap_or_else(|e| panic!("{e:?}"));
    let v1 = core.open_view(); // vts 10
    core.advance_visible_ts(Ts(20));
    let v2 = core.open_view(); // vts 20

    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(6)
    ); // caller ts
    drop(caller);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(10)
    ); // snapshot + v1
    drop(s1);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(10)
    ); // v1
    drop(v1);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(20)
    ); // v2 + visible
    drop(v2);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(20)
    ); // visible only

    // The AS OF floor (extra_floor) participates in the min.
    let s = core.registry.take_snapshot(); // 20
    assert_eq!(
        core.registry
            .with_registry(|r| r.computed_watermark(Some(Ts(12)))),
        Ts(12)
    );
    drop(s);
    assert_eq!(
        core.registry
            .with_registry(|r| r.computed_watermark(Some(Ts(30)))),
        Ts(20)
    );

    // Compute + publish in one critical section (§9.1): with the view open,
    // W is held at the view's vts, and dropping the view lets it advance.
    let v = core.open_view(); // vts 20
    core.advance_visible_ts(Ts(30));
    assert_eq!(core.registry.publish_computed_w(None), Ts(20));
    drop(v);
    assert_eq!(core.registry.publish_computed_w(None), Ts(30));
    assert_eq!(core.registry.published_w(), Ts(30));
}

#[test]
fn guards_unregister_on_drop() {
    let core = core_at(10);
    assert_eq!(core.registry.min_view_counter(), u64::MAX);
    let v1 = core.open_view();
    let c1 = v1.counter();
    let v2 = core.open_view();
    let c2 = v2.counter();
    assert!(c1 < c2);
    assert_eq!(core.registry.min_view_counter(), c1);
    drop(v1);
    assert_eq!(core.registry.min_view_counter(), c2);
    drop(v2);
    assert_eq!(core.registry.min_view_counter(), u64::MAX);

    // Snapshots are a multiset: two at the same ts hold W twice.
    let a = core.registry.take_snapshot();
    let b = core.registry.take_snapshot();
    core.advance_visible_ts(Ts(50));
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(10)
    );
    drop(a);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(10)
    );
    drop(b);
    assert_eq!(
        core.registry.with_registry(|r| r.computed_watermark(None)),
        Ts(50)
    );
}

#[test]
fn view_counters_and_sweep_counter() {
    let core = core_at(10);
    assert_eq!(core.registry.sweep_counter(), None);
    let _v = core.open_view(); // counter 1
    core.registry.record_sweep();
    assert_eq!(core.registry.sweep_counter(), Some(1));
    let _v2 = core.open_view(); // counter 2
    core.registry.record_sweep();
    assert_eq!(core.registry.sweep_counter(), Some(2));
    core.registry
        .with_registry(|r| assert_eq!(r.view_counter(), 2));
    core.registry
        .with_registry(|r| assert_eq!(r.min_snapshot_ts(), None));
    let _s = core.registry.take_snapshot();
    core.registry
        .with_registry(|r| assert_eq!(r.min_snapshot_ts(), Some(Ts(10))));
}

#[test]
fn register_at_rejects_below_published_w_after_publish() {
    // The §9.1 hazard: a caller-chosen ts registered between a computation
    // and its publish must be checked against the *published* W.
    let core = core_at(10);
    assert_eq!(core.registry.publish_computed_w(None), Ts(10));
    assert!(matches!(
        core.registry.register_at(Ts(9)),
        Err(TxnError::SnapshotTooOld)
    ));
    assert!(core.registry.register_at(Ts(10)).is_ok());
}

#[test]
fn publish_computed_w_computes_and_publishes_in_one_call() {
    // Rework item 3: the only publish path computes the §9.1 min from the
    // registry state at call time — no separately-computed value can be
    // published in a later critical section.
    let core = core_at(20);
    let s = core.registry.take_snapshot(); // registers 20
    core.advance_visible_ts(Ts(50));
    assert_eq!(core.registry.published_w(), Ts(0));
    // W lands on the min (the snapshot, not visible_ts) in the same call.
    assert_eq!(core.registry.publish_computed_w(None), Ts(20));
    assert_eq!(core.registry.published_w(), Ts(20));
    drop(s);
    assert_eq!(core.registry.publish_computed_w(None), Ts(50));
    // Monotonic: a lower floor cannot pull W back.
    assert_eq!(core.registry.publish_computed_w(Some(Ts(5))), Ts(50));
    assert_eq!(core.registry.published_w(), Ts(50));

    // The extra floor participates in the min when it is the smallest term
    // (fresh store: visible 50, nothing registered, floor 30).
    let core = core_at(50);
    assert_eq!(core.registry.publish_computed_w(Some(Ts(30))), Ts(30));
    assert_eq!(core.registry.publish_computed_w(None), Ts(50));
}

/// A KV whose `snapshot()` reports to the test and waits for it, so the test
/// can inspect the registry while a view's KV snapshot is being opened. The
/// gate is armed at construction (there is no public KV handle): it blocks
/// only once `block` is set, so boot's own snapshots pass through.
struct Gate {
    inner: MemKv,
    block: Arc<AtomicBool>,
    entered_tx: Sender<()>,
    go_rx: Mutex<Receiver<()>>,
}

impl OrderedKv for Gate {
    type Snap = <MemKv as OrderedKv>::Snap;
    fn write(&self, b: nucleus_kv::Batch, d: nucleus_kv::Durability) -> nucleus_kv::Result<()> {
        self.inner.write(b, d)
    }
    fn sync_wal(&self) -> nucleus_kv::Result<()> {
        self.inner.sync_wal()
    }
    fn snapshot(&self) -> Self::Snap {
        if self.block.load(Ordering::SeqCst) {
            let _ = self.entered_tx.send(());
            if let Ok(rx) = self.go_rx.lock() {
                let _ = rx.recv();
            }
        }
        self.inner.snapshot()
    }
    fn get_latest(&self, k: &[u8]) -> nucleus_kv::Result<Option<nucleus_kv::Value>> {
        self.inner.get_latest(k)
    }
    fn ingest_sorted(
        &self,
        e: &mut dyn Iterator<Item = (nucleus_kv::Key, nucleus_kv::Value)>,
    ) -> nucleus_kv::Result<()> {
        self.inner.ingest_sorted(e)
    }
    fn checkpoint(&self, dir: &std::path::Path) -> nucleus_kv::Result<()> {
        self.inner.checkpoint(dir)
    }
    fn set_gc_filter(&self, f: Box<dyn nucleus_kv::GcFilter>) {
        self.inner.set_gc_filter(f)
    }
    fn set_gc_watermark(&self, w: u64) -> nucleus_kv::Result<()> {
        self.inner.set_gc_watermark(w)
    }
}

#[test]
fn open_view_registers_before_opening_the_kv_snapshot() {
    // I-SNAP-ORDER (§3.1): counter and vts registered, mutex released, then
    // the KV snapshot opened.
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<()>();
    let (go_tx, go_rx) = std::sync::mpsc::channel::<()>();
    let block = Arc::new(AtomicBool::new(false));
    let gate = Gate {
        inner: MemKv::new(),
        block: Arc::clone(&block),
        entered_tx,
        go_rx: Mutex::new(go_rx),
    };
    let core = ok(Core::open(gate)); // block is off: boot snapshots pass
    core.advance_visible_ts(Ts(7));
    std::thread::scope(|s| {
        let core = &core;
        block.store(true, Ordering::SeqCst); // arm from here on
        let opener = s.spawn(move || core.open_view().counter());
        ok(entered_rx.recv());
        // Inside snapshot(): the registry is not locked and already holds the
        // view's counter and vts. Observe first, release the opener, then
        // assert, so a failure cannot leave the opener blocked.
        let min_counter = core.registry.min_view_counter();
        core.advance_visible_ts(Ts(9));
        let computed = core.registry.with_registry(|r| r.computed_watermark(None));
        ok(go_tx.send(()));
        let counter = opener.join().unwrap_or(0);
        assert_eq!(min_counter, 1, "view counter registered before snapshot");
        assert_eq!(computed, Ts(7), "view vts registered before snapshot");
        assert_eq!(counter, 1);
        block.store(false, Ordering::SeqCst);
    });
}
