//! C-T1a tests: the snapshot/view registry (§3.1, §9.1) — `register_at`
//! bounds, `W` monotonicity, the computed min, guard unregistration, and the
//! sweep counter.

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
    assert_eq!(core.registry.w(), Ts(0));
    assert_eq!(core.registry.publish_w(Ts(5)), Ts(5));
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
    assert_eq!(core.registry.publish_w(Ts(7)), Ts(7));
    assert_eq!(core.registry.publish_w(Ts(3)), Ts(7));
    assert_eq!(core.registry.publish_w(Ts(9)), Ts(9));
    // W is loaded from /sys/gc_w at boot and never decreases across boots.
    let mut batch = nucleus_kv::Batch::default();
    batch.put(
        nucleus_txn::encoding::sys_gc_w_key(),
        6u64.to_be_bytes().to_vec(),
    );
    ok(core.kv.write(batch, nucleus_kv::Durability::Yes));
    let core2 = ok(Core::open(core.kv));
    assert_eq!(core2.registry.w(), Ts(6));
    assert_eq!(core2.registry.publish_w(Ts(2)), Ts(6));
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

    assert_eq!(core.registry.computed_watermark(None), Ts(6)); // caller ts
    drop(caller);
    assert_eq!(core.registry.computed_watermark(None), Ts(10)); // snapshot + v1
    drop(s1);
    assert_eq!(core.registry.computed_watermark(None), Ts(10)); // v1
    drop(v1);
    assert_eq!(core.registry.computed_watermark(None), Ts(20)); // v2 + visible
    drop(v2);
    assert_eq!(core.registry.computed_watermark(None), Ts(20)); // visible only

    // The AS OF floor (extra_floor) participates in the min.
    let s = core.registry.take_snapshot(); // 20
    assert_eq!(core.registry.computed_watermark(Some(Ts(12))), Ts(12));
    drop(s);
    assert_eq!(core.registry.computed_watermark(Some(Ts(30))), Ts(20));

    // Compute + publish in one critical section (§9.1): with the view open,
    // W is held at the view's vts, and dropping the view lets it advance.
    let v = core.open_view(); // vts 20
    core.advance_visible_ts(Ts(30));
    assert_eq!(core.registry.publish_computed_w(None), Ts(20));
    drop(v);
    assert_eq!(core.registry.publish_computed_w(None), Ts(30));
    assert_eq!(core.registry.w(), Ts(30));
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
    assert_eq!(core.registry.computed_watermark(None), Ts(10));
    drop(a);
    assert_eq!(core.registry.computed_watermark(None), Ts(10));
    drop(b);
    assert_eq!(core.registry.computed_watermark(None), Ts(50));
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
