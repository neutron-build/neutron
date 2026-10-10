//! The §8.3 read-only exception, in all three of its parts: a declared
//! READ ONLY T1 in a dangerous structure is spared when T3 committed
//! after S(T1); the exception extends to a committing T1 that never
//! wrote (no writes is only final at commit); and it does NOT extend to
//! a write-less T1 that is not the txn committing now — that one can
//! still write, so the structure through it stays dangerous.
//!
//! Mutants (each test names the one it must kill):
//! - `is_read_only` drops the `t1 == committer` clause (the G0 model's
//!   reading: any write-less txn counts as read-only) →
//!   `no_write_non_committer_gets_no_exception`.
//! - `is_read_only` keeps only the declared flag →
//!   `committing_no_write_txn_gets_the_exception`.
//! - the ro branch ignores the `commit_ts(T3) <= S(T1)` bound →
//!   `declared_read_only_spared_when_t3_after_snapshot`.

mod ssi_support;

use nucleus_txn::TxnError;
use ssi_support::Rig;

/// T3 commits after S(T1) and T1 is declared READ ONLY: T1 -> T2 -> T3 is
/// not dangerous, T1 commits. Mutant: the exception ignored (ro treated
/// as false, or the `<= S(T1)` bound dropped).
#[test]
fn declared_read_only_spared_when_t3_after_snapshot() {
    let rig = Rig::new();
    rig.preload(b"a", b"0");
    rig.preload(b"b", b"0");
    let t2 = rig.ser(false);
    rig.read(&t2, b"a");
    let t1 = rig.ser(true);
    rig.read(&t1, b"b");
    let t3 = rig.ser(false);
    rig.update(&t3, b"a", b"z"); // t2 -> t3 (writer side, t2's SIREAD on a)
    let (i1, i2, i3) = (t1.id(), t2.id(), t3.id());
    let c3 = rig.commit(t3).expect("t3 commits first");
    assert!(c3 > t1.s);
    rig.update(&t2, b"b", b"y"); // t1 -> t2 (writer side, t1's SIREAD on b)
    assert!(rig.has_edge(i1, i2));
    assert!(rig.has_edge(i2, i3));
    // T1 -> T2 -> T3 with commit_ts(T3) > S(T1) and T1 declared read-only:
    // the exception applies, no 40001 — and the pivot T2 is not doomed.
    rig.commit(t1)
        .expect("read-only T1 is spared by the exception");
    assert!(!rig.ssi.is_doomed(i2), "the spared structure dooms nobody");
    // Unlike the no-write clause, the declared flag follows T1 forever:
    // T2's own later commit re-checks the triple with T1 still read-only
    // (commit_ts(T3) > S(T1)) and is spared too.
    rig.commit(t2)
        .expect("T2 commits after the spared structure");
}

/// The same structure, but T1 is not declared READ ONLY — it simply never
/// wrote, and it is the txn committing now: the exception still applies.
/// Mutant: `is_read_only` reduced to the declared flag alone.
#[test]
fn committing_no_write_txn_gets_the_exception() {
    let rig = Rig::new();
    rig.preload(b"a", b"0");
    rig.preload(b"b", b"0");
    let t2 = rig.ser(false);
    rig.read(&t2, b"a");
    let t1 = rig.ser(false);
    rig.read(&t1, b"b"); // T1's only operation: it never writes
    let t3 = rig.ser(false);
    rig.update(&t3, b"a", b"z");
    let (i1, i2, i3) = (t1.id(), t2.id(), t3.id());
    let c3 = rig.commit(t3).expect("t3 commits first");
    assert!(c3 > t1.s);
    rig.update(&t2, b"b", b"y");
    assert!(rig.has_edge(i1, i2));
    assert!(rig.has_edge(i2, i3));
    rig.commit(t1)
        .expect("a committing txn with no writes gets the exception");
    assert!(!rig.ssi.is_doomed(i2), "t1's own check dooms nobody");
    // At T2's later commit the same triple is re-evaluated with T1 no
    // longer the txn committing: §8.3 does not extend the exception to a
    // committed no-write txn (only the declared flag follows the txn),
    // so the pivot T2 aborts. Sound, and deliberately conservative.
    assert_eq!(
        rig.commit(t2),
        Err(TxnError::SerializationFailure),
        "the exception is T1's own commit only"
    );
}

/// A write-less T1 that is NOT the txn committing now gets no exception:
/// T3's pre-commit must doom the pivot T2. Mutant: `is_read_only` drops
/// the `t1 == committer` clause (any write-less txn counts, the G0
/// reading) — then the structure is spared, T2 is not doomed, and both
/// assertions fail.
#[test]
fn no_write_non_committer_gets_no_exception() {
    let rig = Rig::new();
    rig.preload(b"a", b"0");
    rig.preload(b"b", b"0");
    let t1 = rig.ser(false);
    rig.read(&t1, b"a"); // T1 never writes, and never commits in this test
    let t2 = rig.ser(false);
    rig.read(&t2, b"b");
    rig.update(&t2, b"a", b"y"); // t1 -> t2
    let t3 = rig.ser(false);
    rig.update(&t3, b"b", b"z"); // t2 -> t3
    assert!(rig.has_edge(t1.id(), t2.id()));
    assert!(rig.has_edge(t2.id(), t3.id()));
    rig.commit(t3).expect("t3 commits first");
    assert!(rig.ssi.is_doomed(t2.id()), "the pivot T2 is doomed");
    assert_eq!(
        rig.commit(t2),
        Err(TxnError::SerializationFailure),
        "T2 aborts: T1 can still write, so the structure stays dangerous"
    );
}
