//! C-T3 behaviour tests: the read-only exception (§8.3), abort cleanup
//! (§8.6), the unique-rule hook `covers` (§5.3), I-SSI-PRECISION on a
//! serial schedule, and the retention bound (§8.6). Every test names the
//! mutant it must kill.

mod ssi_support;

use nucleus_txn::ssi::{Siread, SsiStats};
use nucleus_txn::write::SsiHook;
use nucleus_txn::TxnError;
use ssi_support::Rig;

/// Read-only exception (G0 `Ro` variant): T3 (= T2 of `Ro`) commits after
/// the read-only T0's snapshot, so `T0 -> T1 -> T2` is not dangerous and T0
/// commits. Mutant: the exception ignored (40001).
#[test]
fn readonly_exception_t3_after_snapshot() {
    let rig = Rig::new();
    rig.preload(b"h0", b"0");
    rig.preload(b"h1", b"0");
    let t0 = rig.ser(true);
    let t1 = rig.ser(false);
    let t2 = rig.ser(false);
    rig.read(&t1, b"h0");
    rig.update(&t2, b"h0", b"2");
    let i1 = t1.id();
    let c2 = rig.commit(t2).expect("t2");
    assert!(c2 > t0.s);
    rig.update(&t1, b"h1", b"1");
    rig.commit(t1).expect("t1");
    assert_eq!(rig.ssi.earliest_out_conflict_commit(i1), Some(c2));
    assert_eq!(rig.read(&t0, b"h0"), Some(b"0".to_vec()));
    assert_eq!(rig.read(&t0, b"h1"), Some(b"0".to_vec()));
    assert!(rig.has_edge(t0.id(), i1));
    rig.commit(t0).expect("read-only T0 commits");
}

/// Abort (§8.6): an aborted txn's SIREADs and every edge to or from it are
/// gone, so it never dooms anyone afterwards: `A -> Y -> Z` with A aborted
/// leaves Y alone when Z commits first. Mutant: abort removes only the
/// SIREADs and keeps the entry and its edges.
#[test]
fn abort_removes_edges_and_sireads() {
    let rig = Rig::new();
    rig.preload(b"a", b"0");
    rig.preload(b"b", b"0");
    let a = rig.ser(false);
    let y = rig.ser(false);
    let z = rig.ser(false);
    rig.read(&a, b"a");
    rig.update(&y, b"a", b"y");
    rig.read(&y, b"b");
    rig.update(&z, b"b", b"z");
    let (ia, iy) = (a.id(), y.id());
    assert!(rig.has_edge(ia, iy));
    rig.abort(a);
    assert!(rig.ssi.sireads_of(ia).is_empty());
    assert!(rig.ssi.edges().iter().all(|&(f, t)| f != ia && t != ia));
    rig.commit(z).expect("z commits");
    assert!(!rig.ssi.is_doomed(iy), "the aborted A doomed Y");
    rig.commit(y).expect("y commits");
}

/// `covers` (§5.3 SERIALIZABLE unique rule): true only for the SIREAD
/// holder, for point, range and relation SIREADs. Mutant: `covers` ignores
/// the txn (any holder counts).
#[test]
fn covers_only_for_the_holder() {
    let rig = Rig::new();
    rig.ssi.map_storage(b"r", b"s", 3);
    let r = rig.ser(false);
    let w = rig.ser(false);
    rig.read(&r, b"k");
    rig.scan(&r, b"m0", b"m5");
    rig.ssi
        .lock_relation_read(r.id(), 3)
        .expect("relation siread");
    let hook: &dyn SsiHook = &*rig.ssi;
    for key in [&b"k"[..], b"m3", b"r1"] {
        assert!(hook.covers(r.id(), key), "holder covers {key:?}");
        assert!(!hook.covers(w.id(), key), "non-holder covers {key:?}");
    }
    assert!(!hook.covers(r.id(), b"m5"), "range is half-open");
    assert!(!hook.covers(r.id(), b"z"));
    assert_eq!(rig.ssi.sireads_of(w.id()), Vec::<(Siread, u64)>::new());
}

/// I-SSI-PRECISION: 200 SER txns run one at a time (no overlap), each
/// reading and writing pseudo-random keys, raise no 40001. Mutant: the
/// reader side records an edge to the writer of every version it passes,
/// visible or not (committed T2s then carry an eocc and fire).
#[test]
fn precision_serial_schedule_never_aborts() {
    let rig = Rig::new();
    let keys: Vec<Vec<u8>> = (0..10).map(|i| format!("k{i}").into_bytes()).collect();
    for k in &keys {
        rig.preload(k, b"0");
    }
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    let mut next = move |n: usize| {
        seed = seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        ((seed >> 33) as usize) % n
    };
    for i in 0..200u32 {
        let t = rig.ser(false);
        for _ in 0..3 {
            rig.read(&t, &keys[next(keys.len())]);
        }
        if i % 7 == 0 {
            rig.scan(&t, b"k0", b"k9");
        }
        for _ in 0..2 {
            let v = format!("{i}").into_bytes();
            rig.update(&t, &keys[next(keys.len())], &v);
        }
        rig.commit(t).unwrap_or_else(|e| panic!("txn {i}: {e:?}"));
        if i % 10 == 0 {
            rig.ssi.run_retention(&rig.core);
        }
    }
}

/// Retention bound (§8.6): after every txn ended (committed, read-only,
/// aborted, failed with 40001) and one retention pass, no SSI entry,
/// SIREAD or writer-map entry remains. Mutant: retention counts committed
/// txns' snapshots as live (an old committed snapshot then pins everything).
#[test]
fn retention_leaves_nothing_after_all_end() {
    let rig = Rig::new();
    rig.preload(b"x", b"1");
    rig.preload(b"y", b"1");
    // Write skew: one commits, the other gets 40001.
    let a = rig.ser(false);
    let b = rig.ser(false);
    let ro = rig.ser(true);
    rig.read(&a, b"x");
    rig.read(&a, b"y");
    rig.read(&b, b"x");
    rig.read(&b, b"y");
    rig.update(&a, b"x", b"0");
    rig.update(&b, b"y", b"0");
    rig.commit(a).expect("a");
    assert_eq!(rig.commit(b), Err(TxnError::SerializationFailure));
    let later = rig.ser(false);
    rig.read(&later, b"x");
    rig.update(&later, b"y", b"5");
    let gone = rig.ser(false);
    rig.read(&gone, b"y");
    rig.abort(gone);
    rig.read(&ro, b"x");
    rig.commit(later).expect("later");
    rig.commit(ro).expect("ro");
    assert!(rig.ssi.stats().entries > 0);
    rig.ssi.run_retention(&rig.core);
    assert_eq!(
        rig.ssi.stats(),
        SsiStats {
            entries: 0,
            sireads: 0,
            writers: 0
        }
    );
}
