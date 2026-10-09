//! C-T3 seed tests (C-T0 §11 seeds 5, 6, 28, 29, 30, 32, 36, 39, 40, 43,
//! 60, 61, 63), rebuilt from G0-ssi's workloads as concrete histories driven
//! by hand on one thread. Seeds 20 and 21 need a pause point inside an SSI
//! critical section and live in the module tests (`src/ssi/tests.rs`).
//! Every test names the mutant it must kill.

mod ssi_support;

use std::sync::{Arc, Mutex};

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::CommitObserver;
use nucleus_txn::ssi::{Siread, Ssi};
use nucleus_txn::status::Remembered;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnError, TxnId, TxnStatus};
use ssi_support::Rig;

fn stamp_of(rig: &Rig, txn: TxnId, sr: &Siread) -> u64 {
    rig.ssi
        .sireads_of(txn)
        .into_iter()
        .find(|(s, _)| s == sr)
        .map(|(_, c)| c)
        .expect("SIREAD registered")
}

/// Seeds 5, 6 (G0 `Scan`): the scan's `Range` SIREAD is registered before
/// the scan's view opens (stamp < view counter), and a writer placing inside
/// the range after the view opened gets `R -> W`. Mutants: register after
/// iterating; register after opening the view.
#[test]
fn seed05_06_scan_registers_before_view() {
    let rig = Rig::new();
    rig.preload(b"i1", b"a");
    rig.preload(b"i3", b"b");
    let r = rig.ser(false);
    let rows = rig.scan(&r, b"i0", b"i9");
    assert_eq!(rows.len(), 2);
    let range = Siread::Range {
        lo: b"i0".to_vec(),
        hi: b"i9".to_vec(),
    };
    let stamp = stamp_of(&rig, r.id(), &range);
    let view = rig
        .ssi
        .last_read_view_counter(r.id())
        .expect("view counter");
    assert!(stamp < view, "SIREAD stamp {stamp} not before view {view}");
    // A phantom insert into the scanned gap, after the scan's view opened.
    let w = rig.ser(false);
    rig.insert(&w, b"i5", b"c");
    assert!(rig.has_edge(r.id(), w.id()), "scan holder must get R -> W");
}

/// Seeds 30, 36 (G0 `Scan`): the index→row fetch after a scan registers its
/// point SIREAD after the scan's view opened, then opens a **new** view
/// whose counter exceeds that stamp. Mutants: reuse the scan's view; read
/// via `latest_get` (no view, so the counter does not move past the stamp).
#[test]
fn seed30_36_fetch_uses_fresh_registered_view() {
    let rig = Rig::new();
    rig.preload(b"i1", b"h1");
    rig.preload(b"h1", b"row");
    let r = rig.ser(false);
    let rows = rig.scan(&r, b"i0", b"i9");
    assert_eq!(rows, vec![(b"i1".to_vec(), b"h1".to_vec())]);
    let scan_view = rig.ssi.last_read_view_counter(r.id()).expect("scan view");
    assert_eq!(rig.read(&r, b"h1"), Some(b"row".to_vec()));
    let fetch_view = rig.ssi.last_read_view_counter(r.id()).expect("fetch view");
    let stamp = stamp_of(
        &rig,
        r.id(),
        &Siread::Point {
            key: b"h1".to_vec(),
        },
    );
    assert!(
        stamp >= scan_view,
        "the fetch's SIREAD is registered after the scan's view opened"
    );
    assert!(
        fetch_view > stamp,
        "fetch view {fetch_view} must open after its SIREAD (stamp {stamp})"
    );
    assert!(fetch_view > scan_view);
}

/// Seed 28: inside `on_assigned` (§3 step 3) the txn's status is never yet
/// `Committed`. Mutant (C-T1b regression): step 3 moved after step 4.
#[test]
fn seed28_writer_map_before_status() {
    struct Probe {
        ssi: Arc<Ssi>,
        core: Arc<Core<MemKv>>,
        seen: Mutex<Vec<Remembered>>,
    }
    impl CommitObserver for Probe {
        fn on_assigned(&self, txn: TxnId, ts: Ts) {
            self.seen
                .lock()
                .expect("seen")
                .push(self.core.status.lookup_remembered(txn));
            self.ssi.on_assigned(txn, ts);
        }
    }
    let mut probe: Option<Arc<Probe>> = None;
    let rig = Rig::with_observer(|ssi, core| {
        let p = Arc::new(Probe {
            ssi: Arc::clone(ssi),
            core: Arc::clone(core),
            seen: Mutex::new(Vec::new()),
        });
        probe = Some(Arc::clone(&p));
        p
    });
    let probe = probe.expect("probe");
    rig.preload(b"k", b"v");
    let a = rig.ser(false);
    let b = rig.ser(false);
    rig.update(&a, b"k", b"a");
    rig.read(&b, b"z");
    let (ia, ib) = (a.id(), b.id());
    let ta = rig.submit(a).expect("submit a");
    let tb = rig.submit(b).expect("submit b");
    assert_eq!(rig.process(), 2);
    let ca = ta.wait().expect("a");
    let cb = tb.wait().expect("b");
    let seen = probe.seen.lock().expect("seen").clone();
    assert_eq!(seen.len(), 2);
    for r in seen {
        assert!(
            matches!(r, Remembered::Live(TxnStatus::Pending, _)),
            "status set before the writer map: {r:?}"
        );
    }
    assert_eq!(rig.ssi.writer_of(ca), Some(ia));
    assert_eq!(rig.ssi.writer_of(cb), Some(ib));
}

/// Seed 29: retention waits for `visible_ts >= commit_ts`. Mutant: the
/// visible condition dropped.
#[test]
fn seed29_retention_waits_for_visible() {
    let rig = Rig::new();
    let t = rig.ser(false);
    let ts = Ts(rig.core.visible_ts().0 + 5);
    rig.ssi.on_assigned(t.id(), ts);
    assert_eq!(rig.ssi.run_retention(&rig.core), 0);
    assert_eq!(rig.ssi.writer_of(ts), Some(t.id()));
}

/// Seed 32 (G0 `Trunc`): a TRUNCATE by a SER txn checks the relation's
/// SIREADs, at any granularity: a point read on its storage and a
/// relation-level SIREAD both give `R -> W`. Mutant: no DDL-side check.
#[test]
fn seed32_truncate_checks_relation_sireads() {
    let rig = Rig::new();
    rig.ssi.map_storage(b"t7", b"t8", 7);
    rig.preload(b"t7k", b"v");
    let r = rig.ser(false);
    rig.read(&r, b"t7k");
    let q = rig.ser(false);
    rig.ssi
        .lock_relation_read(q.id(), 9)
        .expect("relation siread");
    let w = rig.ser(false);
    rig.ssi
        .on_ddl_execute(w.id(), Isolation::Serializable, 7)
        .expect("ddl");
    assert!(rig.has_edge(r.id(), w.id()), "point SIREAD on relation 7");
    assert!(!rig.has_edge(q.id(), w.id()), "relation 9 is untouched");
    let w2 = rig.ser(false);
    rig.ssi
        .on_ddl_execute(w2.id(), Isolation::Serializable, 9)
        .expect("ddl");
    assert!(rig.has_edge(q.id(), w2.id()), "relation-level SIREAD on 9");
}

/// Seed 39 (G0 `Skew`): R reads k and commits at c; W begins after
/// (`S(W) >= c`) and writes k: no edge, although R's SIREAD is still held.
/// Mutant: the writer-side concurrency test dropped.
#[test]
fn seed39_no_edge_from_nonconcurrent_holder() {
    let rig = Rig::new();
    rig.preload(b"k", b"v");
    let r = rig.ser(false);
    rig.read(&r, b"k");
    let ir = r.id();
    let c = rig.commit(r).expect("r commits");
    let w = rig.ser(false);
    assert!(w.s >= c);
    rig.update(&w, b"k", b"w");
    assert!(
        !rig.ssi.sireads_of(ir).is_empty(),
        "R's SIREAD is still held (not retired)"
    );
    assert!(!rig.has_edge(ir, w.id()));
}

/// Seed 40 (G0 `Ro`): T2 writes h0 and commits; read-only T0 begins; T1
/// (read h0 before T2 committed, writes h1) commits; retention retires T2;
/// T0 reads h1 (skipping T1's version) and commits: 40001 through T1's
/// frozen `earliest_out_conflict_commit`. Mutants: retirement removes the
/// edges pointing at T2 (and the eocc derived from them); committed T1
/// tested through its edge list (T2 is gone).
#[test]
fn seed40_readonly_anomaly_survives_retirement() {
    let rig = Rig::new();
    rig.preload(b"h0", b"0");
    rig.preload(b"h1", b"0");
    let t1 = rig.ser(false);
    let t2 = rig.ser(false);
    rig.read(&t1, b"h0");
    rig.update(&t2, b"h0", b"2");
    let (i1, i2) = (t1.id(), t2.id());
    let c2 = rig.commit(t2).expect("t2 commits");
    assert_eq!(rig.ssi.earliest_out_conflict_commit(i1), Some(c2));
    let t0 = rig.ser(true);
    assert_eq!(t0.s, c2);
    rig.update(&t1, b"h1", b"1");
    let c1 = rig.commit(t1).expect("t1 commits");
    assert!(rig.ssi.run_retention(&rig.core) >= 1);
    assert_eq!(rig.ssi.writer_of(c2), None, "T2 retired");
    assert_eq!(rig.ssi.writer_of(c1), Some(i1), "T1 kept: S(T0) < c1");
    assert!(rig.has_edge(i1, i2), "retiring T2 keeps T1 -> T2");
    assert_eq!(rig.ssi.earliest_out_conflict_commit(i1), Some(c2));
    assert_eq!(rig.read(&t0, b"h1"), Some(b"0".to_vec()));
    assert_eq!(rig.commit(t0), Err(TxnError::SerializationFailure));
}

/// Seed 43: the committer in the T1 position (`T -> Y -> Z`, Z committed
/// first, Y committed with eocc set) raises 40001; and a committer in the
/// T3 position (`X -> Y -> T`, X and Y active) dooms the pivot Y. Mutant:
/// only structures with the committer as pivot are checked.
#[test]
fn seed43_all_positions_checked() {
    // T1 position.
    let rig = Rig::new();
    for k in [b"a", b"b", b"c"] {
        rig.preload(k, b"0");
    }
    let z = rig.ser(false);
    let y = rig.ser(false);
    let t = rig.ser(false);
    rig.read(&y, b"a");
    rig.update(&z, b"a", b"z");
    let cz = rig.commit(z).expect("z");
    let iy = y.id();
    rig.update(&y, b"b", b"y");
    rig.commit(y).expect("y");
    assert_eq!(rig.ssi.earliest_out_conflict_commit(iy), Some(cz));
    rig.read(&t, b"b");
    assert!(rig.has_edge(t.id(), iy));
    rig.update(&t, b"c", b"t");
    assert_eq!(rig.commit(t), Err(TxnError::SerializationFailure));

    // T3 position.
    let rig = Rig::new();
    for k in [b"a", b"b"] {
        rig.preload(k, b"0");
    }
    let x = rig.ser(false);
    let y = rig.ser(false);
    let t = rig.ser(false);
    rig.read(&x, b"a");
    rig.update(&y, b"a", b"y");
    rig.read(&y, b"b");
    rig.update(&t, b"b", b"t");
    let (ix, iy) = (x.id(), y.id());
    rig.commit(t).expect("t commits first");
    assert!(rig.ssi.is_doomed(iy), "pivot Y doomed");
    assert!(!rig.ssi.is_doomed(ix));
    assert_eq!(
        rig.ssi.check_doomed(iy),
        Err(TxnError::SerializationFailure)
    );
    assert_eq!(rig.commit(y), Err(TxnError::SerializationFailure));
    rig.commit(x).expect("x commits");
}

/// Seed 60 (G0 `Trunc`): the DDL-side edge exists right after
/// `on_ddl_execute`, before any pre-commit; and the retired-id promotion at
/// the DDL's pre-commit turns R's point SIREAD on the old storage into
/// `Relation { 7 }`, so a later write to the new storage (mapped to 7) gives
/// an edge. Mutant: the DDL check deferred to pre-commit.
#[test]
fn seed60_ddl_edge_at_execution() {
    let rig = Rig::new();
    rig.ssi.map_storage(b"o", b"p", 7);
    rig.preload(b"o1", b"v");
    let r = rig.ser(false);
    rig.read(&r, b"o1");
    let w = rig.ser(false);
    // The catalog's TRUNCATE: retire the old storage, map the new one.
    rig.ssi
        .note_retired(w.id(), 7, vec![(b"o".to_vec(), b"p".to_vec())]);
    rig.ssi.map_storage(b"n", b"o", 7);
    rig.ssi.unmap_storage(b"o", b"p");
    rig.ssi
        .on_ddl_execute(w.id(), Isolation::Serializable, 7)
        .expect("ddl");
    assert!(rig.has_edge(r.id(), w.id()), "edge at DDL execution");
    rig.commit(w).expect("w commits");
    let sireads: Vec<Siread> = rig
        .ssi
        .sireads_of(r.id())
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    assert_eq!(sireads, vec![Siread::Relation { rel_oid: 7 }]);
    let x = rig.ser(false);
    rig.insert(&x, b"n1", b"new");
    assert!(
        rig.has_edge(r.id(), x.id()),
        "promoted SIREAD covers new storage"
    );
}

/// Seed 61 (G0 `Eo`): T reads k and commits first; X writes k and commits
/// later; a non-read-only R with an edge `R -> T` then commits: no 40001,
/// and `earliest_out_conflict_commit(T)` stays unset. Mutant: set by a
/// later committer.
#[test]
fn seed61_eocc_only_from_earlier_commit() {
    let rig = Rig::new();
    for k in [b"k", b"m", b"n"] {
        rig.preload(k, b"0");
    }
    let t = rig.ser(false);
    let x = rig.ser(false);
    let r = rig.ser(false);
    rig.read(&t, b"k");
    rig.read(&r, b"m");
    rig.update(&t, b"m", b"t");
    rig.update(&x, b"k", b"x");
    let it = t.id();
    assert!(rig.has_edge(r.id(), it) && rig.has_edge(it, x.id()));
    let ct = rig.commit(t).expect("t");
    let cx = rig.commit(x).expect("x");
    assert!(cx > ct);
    assert_eq!(rig.ssi.earliest_out_conflict_commit(it), None);
    rig.update(&r, b"n", b"r");
    rig.commit(r).expect("r commits");
}

/// Seed 63: `T2 -> T3`, one group `[T3, T2]`: T3's assignment still sets
/// `earliest_out_conflict_commit(T2)` although T2 is prepared and in the
/// same group. Mutant: skip the update when T2 is prepared / in the group.
#[test]
fn seed63_eocc_same_group() {
    let rig = Rig::new();
    rig.preload(b"k", b"0");
    rig.preload(b"j", b"0");
    let t2 = rig.ser(false);
    let t3 = rig.ser(false);
    rig.read(&t2, b"k");
    rig.update(&t3, b"k", b"3");
    rig.update(&t2, b"j", b"2");
    let i2 = t2.id();
    let tk3 = rig.submit(t3).expect("t3");
    let tk2 = rig.submit(t2).expect("t2");
    assert_eq!(rig.process(), 2, "one group of two");
    let c3 = tk3.wait().expect("t3 ack");
    let c2 = tk2.wait().expect("t2 ack");
    assert!(c3 < c2);
    assert_eq!(rig.ssi.earliest_out_conflict_commit(i2), Some(c3));
}
