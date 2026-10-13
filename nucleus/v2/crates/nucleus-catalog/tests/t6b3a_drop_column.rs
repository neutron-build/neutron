//! C-T6b3a item 4, DROP COLUMN half: C-T6 §3 — the column is hidden from
//! later reads while old rows still decode (dropped attnums are skipped),
//! every dependent index in **any** state is dropped in the same txn
//! (gone after commit — mutant N12; present after an abort — mutant N15,
//! the write path makes the index drop transactional), a primary-key
//! column is 0A000, an unknown or dropped attnum is 42703 (and a dropped
//! name may be reused by a later ADD COLUMN), and
//! `drop_column_vs_concurrent_index` pins the lock-first rule (mutant
//! N18: an index a concurrent txn committed while the drop waited for its
//! AccessExclusive is dropped too).

mod t6b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{
    decode_def, i32_d, i32_v, idx_def, ok, pk_schema, set_col, text_d, wait_until, Rig,
};

use nucleus_catalog::{IndexState, Stmt};
use nucleus_codec::KeyType;
use nucleus_txn::catalog::IdxStorage;
use nucleus_txn::locks::RelLockMode;

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, not a pass).
const BOUND: Duration = Duration::from_secs(30);

/// `CREATE TABLE t (id int4 PRIMARY KEY, payload text)`, committed.
fn mk_table(rig: &Rig, name: &[u8]) -> (u64, u64) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let r = ok(rig.db.create_table(&st, name, &pk_schema()));
    ok(rig.commit_rc(t));
    r
}

/// One committed row `(id, payload)` in one statement of its own txn.
fn add_row(rig: &Rig, oid: u64, id: i32, payload: Option<&str>) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig
        .db
        .insert_with(&st, &h, &[i32_d(id), payload.map(text_d).unwrap_or(None)]));
    ok(rig.commit_rc(t));
}

/// §3 DROP COLUMN: dependent indexes go in the same txn (mutants N12 and
/// N15), the def update rolls back with an abort, a later statement of
/// the dropping txn sees the index gone, and an index not using the
/// column survives.
#[test]
fn drop_column_drops_dependent_index_in_txn() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"dep");
    add_row(&rig, oid, 1, Some("a"));

    // A DeleteOnly index on attnum 2 (the state a §7 ladder leaves
    // behind — the drop must catch every state) and a Public index on
    // attnum 1 that must survive.
    let (do_oid, _do_isid) = rig.add_index(oid, &idx_def(2, false, IndexState::DeleteOnly));
    let (keep_oid, _keep_isid) = rig.add_index(oid, &idx_def(1, false, IndexState::Public));

    // Abort half (mutant N15: the index drops must travel in the txn).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 2));
        // A later statement of the same txn sees the index gone and the
        // def ahead (own writes, seq < seq0).
        let st2 = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
        assert_eq!(h.indexes.len(), 1, "only the attnum-1 index remains");
        assert_eq!(h.def_version, 2);
        rig.abort_txn(t);
        assert!(
            rig.idx_row(do_oid).is_some(),
            "the abort restores the dependent index (N15)"
        );
        assert!(rig.idx_row(keep_oid).is_some());
        let restored = decode_def(&rig.rel_row(oid).expect("rel row").def);
        assert_eq!(restored.version, 1, "the abort restores the def");
        assert!(!restored.columns[1].dropped);
    }

    // Commit half (mutant N12: the dependent index must be dropped).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 2));
        ok(rig.commit_rc(t));
        assert_eq!(
            rig.idx_row(do_oid),
            None,
            "the dependent index is dropped with the column (N12)"
        );
        assert!(
            rig.idx_row(keep_oid).is_some(),
            "an index not using the column survives"
        );
        let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
        assert_eq!(def.version, 2);
        assert!(def.columns[1].dropped, "dropped, not removed");
        assert!(!def.columns[1].not_null, "DROP COLUMN clears not_null");
    }

    // The column is hidden from later reads and old rows decode without
    // it (§3 decode rule: dropped attnums are skipped).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::AccessShare));
        assert_eq!(h.schema.live_columns().count(), 1, "only attnum 1 is live");
        let rows = ok(rig.db.scan(&st, &h));
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].1,
            vec![i32_d(1)],
            "the old row decodes, minus the dropped column"
        );
        assert_eq!(ok(rig.db.get(&st, &h, &[i32_v(1)])), Some(vec![i32_d(1)]));
        ok(rig.commit_rc(t));
    }

    // A dropped column's name may be reused (§3: attnums never are).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig
            .db
            .add_column(&st, oid, "payload", KeyType::Int4, false, None));
        ok(rig.commit_rc(t));
        let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
        assert_eq!(
            def.columns[2].attnum, 3,
            "the reused name gets a fresh attnum"
        );
        assert_eq!(def.columns[2].name, "payload");
    }
}

/// §3/§9.4: a primary-key column is refused with 0A000; an unknown or
/// already-dropped attnum is 42703 (also for SET/DROP NOT NULL); a
/// `set` that names a dropped column is 42703.
#[test]
fn drop_column_refusals() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"ref");
    add_row(&rig, oid, 1, Some("a"));

    // The primary-key column: 0A000, nothing written.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let err = rig
            .db
            .drop_column(&st, oid, 1)
            .expect_err("a primary-key column cannot be dropped");
        assert_eq!(err.sqlstate(), "0A000");
        assert!(matches!(
            err,
            nucleus_catalog::DbError::FeatureNotSupported(_)
        ));
        rig.abort_txn(t);
    }
    assert_eq!(
        decode_def(&rig.rel_row(oid).expect("rel row").def).version,
        1
    );

    // Unknown attnum, for every one of the typed ALTERs.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        for r in [
            rig.db.drop_column(&st, oid, 99),
            rig.db.set_not_null(&st, oid, 99, true),
            rig.db.set_not_null(&st, oid, 0, false),
        ] {
            let err = r.expect_err("an unknown attnum is 42703");
            assert!(
                matches!(err, nucleus_catalog::DbError::UndefinedColumn),
                "{err:?}"
            );
            assert_eq!(err.sqlstate(), "42703");
        }
        rig.abort_txn(t);
    }

    // Drop attnum 2, then every ALTER on it is 42703 — including a `set`
    // that names it (the column is hidden from DML).
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        ok(rig.db.drop_column(&st, oid, 2));
        let st2 = rig.stmt(&t, s);
        for r in [
            rig.db.drop_column(&st2, oid, 2),
            rig.db.set_not_null(&st2, oid, 2, true),
            rig.db.set_not_null(&st2, oid, 2, false),
        ] {
            let err = r.expect_err("a dropped attnum is 42703");
            assert_eq!(err.sqlstate(), "42703");
        }
        let st3 = rig.stmt(&t, s);
        let err = rig
            .db
            .update(&st3, oid, &[i32_v(1)], &[set_col(2, text_d("x"))])
            .expect_err("a set that names the dropped column is 42703");
        assert_eq!(err.sqlstate(), "42703");
        ok(rig.commit_rc(t));
    }
}

/// Mutant N18 (the index enumeration before the AccessExclusive): thread
/// B creates a DeleteOnly index on the column via
/// `catalog_create_index_with(ShareUpdateExclusive)` and commits while
/// thread A's DROP COLUMN waits for its AccessExclusive — the drop, which
/// enumerates under the lock, drops that index too. An implementation
/// that enumerates before the lock never sees B's (then uncommitted)
/// index and leaves it behind over a dropped column.
#[test]
fn drop_column_vs_concurrent_index() {
    let rig = Arc::new(Rig::new());
    let (oid, _sid) = mk_table(&rig, b"vs");

    // B: create the DeleteOnly index on attnum 2 at SUE, park, then
    // commit while A waits.
    let (held_tx, held_rx) = mpsc::channel::<u64>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        let isid = ok(rig_b.catalog().alloc_storage_id(rig_b.core()));
        let (idx_oid, _) = ok(rig_b.catalog().catalog_create_index_with(
            &rig_b.ddl(&t, s),
            oid,
            IdxStorage::Id(isid),
            &ok(idx_def(2, false, IndexState::DeleteOnly).encode()),
            RelLockMode::ShareUpdateExclusive,
        ));
        held_tx.send(idx_oid).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_b.commit_rc(t));
    });
    let idx_oid = held_rx
        .recv_timeout(BOUND)
        .expect("B holds ShareUpdateExclusive");

    // A: DROP COLUMN — the only wait this schedule can produce is A
    // parked on B's SUE.
    let (res_tx, res_rx) = mpsc::channel::<Result<(), String>>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let r = rig_a.db.drop_column(&st, oid, 2);
        let out = match &r {
            Ok(()) => Ok(()),
            Err(e) => Err(e.sqlstate().to_string()),
        };
        match r {
            Ok(()) => {
                ok(rig_a.commit_rc(t));
            }
            Err(_) => {
                rig_a.abort_txn(t);
            }
        }
        res_tx.send(out).expect("res signal");
    });

    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        res_rx.try_recv().is_err(),
        "the DROP COLUMN must not finish under B's ShareUpdateExclusive"
    );

    release_tx.send(()).expect("release");
    match res_rx.recv_timeout(BOUND).expect("A finished within bound") {
        Ok(()) => {}
        Err(state) => panic!("the drop must succeed; got {state}"),
    }
    b.join().expect("B join");
    a.join().expect("A join");

    // B's index — committed while A waited for the lock — is dropped by
    // A's drop (the enumeration ran under the AccessExclusive).
    assert!(
        rig.idx_row(idx_oid).is_none(),
        "the concurrent DeleteOnly index is dropped too (N18)"
    );
    let def = decode_def(&rig.rel_row(oid).expect("rel row").def);
    assert_eq!(def.version, 2);
    assert!(def.columns[1].dropped);
}
