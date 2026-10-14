//! C-T6b2a item 4a and the C-T6b1-fix gate's disposition: the `_with` DML
//! entry points accept a [`TableHandle`] only from the `(txn, seq0)`
//! statement whose `open_table` made it — another txn's handle
//! (`handle_rejected_in_other_txn`), a later statement's handle (mutant
//! N25: the check ignoring `seq0`), and a pre-DROP handle (the gate's
//! M-4 data-loss path) are `TxnError::Invariant` **before any catalog
//! read**; a handle older than a concurrent index-state commit is the
//! legal stale statement of C-T6 §7.3 and must be **accepted** (mutant
//! N24: comparing the handle with the current catalog would refuse it);
//! and a key-column update — `key_cols_changed` counting a non-Public
//! unique index's column (mutant N10) — takes `Update` mode, so a
//! concurrent `RowOp::Lock(KeyShare)` holder blocks it.

mod t6b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{
    i32_d, i32_v, idx_def, ok, pk_schema, row_key_of, set_col, text_d, wait_until, Rig,
};

use nucleus_catalog::{DbError, IndexState, Stmt};
use nucleus_codec::encode_key;
use nucleus_txn::catalog::table_range;
use nucleus_txn::locks::RelLockMode;
use nucleus_txn::write::{Epq, EpqDecision, RowOp, StmtCtx};
use nucleus_txn::{RowLockMode, TxnError};

/// Everything a thread may wait on is bounded by this; the joins use it
/// too (a hang is a failure, not a pass).
const BOUND: Duration = Duration::from_secs(30);

/// A handle from another txn is `Invariant` before any catalog read or
/// lock: the handle's catalog read belongs to its own statement.
#[test]
fn handle_rejected_in_other_txn() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // The opener txn stays open (its handle belongs to it).
    let opener = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&opener, s1);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));

    // Another txn's statement through that handle: Invariant.
    let other = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&other, s2);
    for name in ["insert_with", "update_with", "delete_with"] {
        let err = match name {
            "insert_with" => rig
                .db
                .insert_with(&st2, &h, &[i32_d(1), text_d("x")])
                .map(|_| ()),
            "update_with" => rig
                .db
                .update_with(&st2, &h, &[i32_v(1)], &[set_col(2, text_d("x"))])
                .map(|_| ()),
            _ => rig.db.delete_with(&st2, &h, &[i32_v(1)]).map(|_| ()),
        }
        .expect_err(name);
        assert_eq!(err.sqlstate(), "XX000", "{name}");
        assert!(
            matches!(err, DbError::Txn(TxnError::Invariant(_))),
            "{name}: {err:?}"
        );
    }
    rig.abort_txn(other);
    rig.abort_txn(opener);
}

/// Mutant N25: the handle check ignoring `seq0` — the **same txn's**
/// later statement must still be refused (the handle's catalog read was
/// made by an earlier statement, before anything that statement did).
#[test]
fn handle_rejected_in_later_statement() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st1 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    let st2 = rig.stmt(&t, s);
    assert_ne!(st1.seq0(), st2.seq0(), "two statements, two seq0s");
    let _ = &st2;

    let err = rig
        .db
        .insert_with(&st2, &h, &[i32_d(1), text_d("x")])
        .expect_err("a later statement through an old handle");
    assert!(matches!(err, DbError::Txn(TxnError::Invariant(_))));
    let err = rig
        .db
        .update_with(&st2, &h, &[i32_v(1)], &[set_col(2, text_d("x"))])
        .expect_err("a later statement through an old handle");
    assert!(matches!(err, DbError::Txn(TxnError::Invariant(_))));
    let err = rig
        .db
        .delete_with(&st2, &h, &[i32_v(1)])
        .expect_err("a later statement through an old handle");
    assert!(matches!(err, DbError::Txn(TxnError::Invariant(_))));

    // The same statement's handle is fine (the plain entry points open
    // their own).
    let st3 = rig.stmt(&t, s);
    ok(rig
        .db
        .update(&st3, oid, &[i32_v(1)], &[set_col(2, text_d("y"))]));
    rig.abort_txn(t);
}

/// Mutant O5 (M-3): `check_handle`'s lock-mode guard. A handle opened at
/// AccessShare passes the `(txn, seq0)` ownership check — it is this
/// very statement's handle — but §4: "a catalog read made under a weaker
/// lock or in another txn is never used to maintain indexes": the `_with`
/// DML entry points refuse it with `TxnError::Invariant`. The guard is
/// load-bearing for §7.3: an AccessShare opener is not collected by the
/// build's `wait_for_lockers(rel, Share)` (the modes do not conflict),
/// so a read-mode statement that maintained would write for an index the
/// builder believes every writer saw.
#[test]
fn read_mode_handle_is_refused_by_the_with_dml() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // This statement's own handle — but opened at AccessShare.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st1 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::AccessShare));

    for name in ["insert_with", "update_with", "delete_with"] {
        let err = match name {
            "insert_with" => rig
                .db
                .insert_with(&st1, &h, &[i32_d(1), text_d("x")])
                .map(|_| ()),
            "update_with" => rig
                .db
                .update_with(&st1, &h, &[i32_v(1)], &[set_col(2, text_d("x"))])
                .map(|_| ()),
            _ => rig.db.delete_with(&st1, &h, &[i32_v(1)]).map(|_| ()),
        }
        .expect_err(name);
        assert_eq!(err.sqlstate(), "XX000", "{name}");
        assert!(
            matches!(err, DbError::Txn(TxnError::Invariant(_))),
            "{name}: {err:?}"
        );
    }
    rig.abort_txn(t);
}

/// Gate rider 2 (M-4's data-loss path, dead under the new contract): a
/// handle from statement 1, an own `catalog_drop_table` in statement 2,
/// then `insert_with`/`insert` through the old handle in statement 3 —
/// `Invariant` fires before any catalog read, the legacy path still
/// refuses 42P01, and no row key lands inside the retired range.
#[test]
fn handle_rejected_after_own_drop() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    // Statement 1 opens; statement 2 (the Ddl's own seq) drops;
    // statement 3 writes through the dead handle.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st1 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.catalog().catalog_drop_table(&rig.ddl(&t, s), oid));

    let st3 = rig.stmt(&t, s);
    let err = rig
        .db
        .insert_with(&st3, &h, &[i32_d(1), text_d("ghost")])
        .expect_err("4a's check fires before any catalog read");
    assert!(matches!(err, DbError::Txn(TxnError::Invariant(_))));
    assert_eq!(err.sqlstate(), "XX000");
    // The legacy path still refuses: the relation row is gone.
    let err = rig
        .db
        .insert(&st3, &h, &[i32_d(1), text_d("ghost")])
        .expect_err("the retired storage must not be written");
    assert!(matches!(err, DbError::UndefinedTable));
    rig.abort_txn(t);

    // No row key landed inside the retired range.
    let (lo, hi) = table_range(sid);
    let mut stored = 0usize;
    {
        use nucleus_kv::{OrderedKv as _, Snapshot as _};
        let snap = rig.kv.snapshot();
        for k in snap.scan(
            (
                std::ops::Bound::Included(lo.as_slice()),
                std::ops::Bound::Excluded(hi.as_slice()),
            ),
            false,
        ) {
            k.expect("kv scan");
            stored += 1;
        }
    }
    assert_eq!(stored, 0, "no key inside the retired storage range");
}

/// Mutant N24: the handle compared with the current catalog. The legal
/// stale statement of C-T6 §7.3: a handle opened by a live statement;
/// another txn commits a DeleteOnly index (CREATE INDEX at
/// ShareUpdateExclusive, which does not conflict with the statement's
/// RowExclusive); `update_with` through the stale handle → `Ok`, and the
/// new index gets **no** maintenance from this statement.
#[test]
fn stale_handle_across_t0_accepted() {
    let rig = Arc::new(Rig::new());
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    let st2 = rig.stmt(&t0, s0);
    let h0 = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h0, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t0));

    let (opened_tx, opened_rx) = mpsc::channel::<()>();
    let (committed_tx, committed_rx) = mpsc::channel::<(u64, u64)>();

    // The writer thread: opens the handle, waits for the index's T0 to
    // commit, then updates through the stale handle. Returns the index's
    // storage id so the main thread can inspect its ranges.
    let rig_y = Arc::clone(&rig);
    let y = thread::spawn(move || -> Result<u64, String> {
        let t = rig_y.rc();
        let s = rig_y.core().visible_ts();
        let st = Stmt::new(&t, s).map_err(|e| format!("stmt: {e:?}"))?;
        let h = rig_y
            .db
            .open_table(&st, oid, RelLockMode::RowExclusive)
            .map_err(|e| format!("open: {e:?}"))?;
        assert_eq!(h.indexes.len(), 0, "the index does not exist yet");
        opened_tx.send(()).expect("opened signal");
        let (_zero, isid) = committed_rx
            .recv_timeout(BOUND)
            .expect("index committed signal");
        // The handle predates the index's T0: legally stale (§7.3), so
        // this must succeed and maintain nothing for the new index.
        rig_y
            .db
            .update_with(&st, &h, &[i32_v(1)], &[set_col(2, text_d("b"))])
            .map_err(|e| format!("update_with: {e:?}"))?;
        rig_y.commit_rc(t).map_err(|e| format!("commit: {e:?}"))?;
        Ok(isid)
    });

    // The builder thread: CREATE INDEX at ShareUpdateExclusive (the
    // online build's T0 shape), commits while the writer holds
    // RowExclusive (the modes do not conflict).
    opened_rx.recv_timeout(BOUND).expect("writer opened");
    let rig_x = Arc::clone(&rig);
    let x = thread::spawn(move || -> Result<(), String> {
        let t = rig_x.rc();
        let s = rig_x.core().visible_ts();
        let isid = rig_x
            .catalog()
            .alloc_storage_id(rig_x.core())
            .map_err(|e| format!("sid: {e:?}"))?;
        let (_idx_oid, _) = rig_x
            .catalog()
            .catalog_create_index_with(
                &rig_x.ddl(&t, s),
                oid,
                nucleus_txn::catalog::IdxStorage::Id(isid),
                &t6b_support::ok(idx_def(2, true, IndexState::DeleteOnly).encode()),
                RelLockMode::ShareUpdateExclusive,
            )
            .map_err(|e| format!("create index: {e:?}"))?;
        rig_x.commit_rc(t).map_err(|e| format!("commit: {e:?}"))?;
        committed_tx.send((0, isid)).expect("committed signal");
        Ok(())
    });

    x.join().expect("builder join").expect("builder failed");
    let isid = y.join().expect("writer join").expect("writer failed");

    // The update applied, and the new index got no maintenance from the
    // stale statement: its entry ranges are empty (an entry-aware update
    // would have tombstoned the base's ‖a entry).
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st3, &h3, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("b")]),
        "the stale-handle update applied"
    );
    ok(rig.commit_rc(t2));
    assert!(
        rig.raw_entry_versions(isid).is_empty(),
        "the new index gets no maintenance from the stale statement"
    );
}

/// Mutant N10: `key_cols_changed` ignoring non-Public unique indexes. A
/// unique **WriteOnly** index makes `payload` a key column (C-T0 §1:
/// "any non-partial, non-expression unique index", §6: "in every
/// state"), so an update changing it requests `Update` mode — which
/// conflicts with a concurrent `RowOp::Lock(KeyShare)` holder. Under the
/// mutant the request would be `NoKeyUpdate` and the update would run
/// under the KEY SHARE lock.
#[test]
fn key_column_update_blocks_on_a_keyshare_holder() {
    let rig = Arc::new(Rig::new());
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));
    let (_u, _u_sid) = rig.add_index(oid, &idx_def(2, true, IndexState::WriteOnly));

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let st1 = rig.stmt(&t1, s1);
    let h1 = ok(rig.db.open_table(&st1, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st1, &h1, &[i32_d(1), text_d("a")]));
    ok(rig.commit_rc(t1));

    // The row's /t/ key (the §3 bytes, built independently).
    let schema = pk_schema();
    let mut row_key = nucleus_catalog::table_lo(sid);
    ok(encode_key(
        &[nucleus_codec::KeyColumn::asc(nucleus_codec::KeyType::Int4)],
        &[i32_v(1)],
        &mut row_key,
    ));
    let _ = row_key_of(sid, &schema, &[i32_v(1)]);

    // Thread A holds FOR KEY SHARE on the row (a shared lock, granted in
    // the lock table, no intent written).
    struct NeverEpq;
    impl Epq for NeverEpq {
        fn recheck(&mut self, _newest: &nucleus_txn::write::CommittedVersion) -> EpqDecision {
            EpqDecision::Skip
        }
    }
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let seq = t.next_seq().expect("seq");
        let outcome = rig_a
            .core()
            .row_op(
                &t,
                &row_key,
                None,
                RowOp::Lock(RowLockMode::KeyShare),
                StmtCtx::new(s, seq, seq),
                &mut NeverEpq,
            )
            .expect("key share lock");
        assert!(matches!(outcome, nucleus_txn::write::RowOutcome::Applied));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        rig_a.abort_txn(t);
    });
    held_rx.recv_timeout(BOUND).expect("KEY SHARE held");

    // Thread B updates the key column: it must park on A's KEY SHARE
    // (Update conflicts; NoKeyUpdate would not — that is N10).
    let (done_tx, done_rx) = mpsc::channel::<Result<(), String>>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let work = (|| -> Result<(), String> {
            let t = rig_b.rc();
            let s = rig_b.core().visible_ts();
            let st = Stmt::new(&t, s).map_err(|e| format!("stmt: {e:?}"))?;
            let n = rig_b
                .db
                .update(&st, oid, &[i32_v(1)], &[set_col(2, text_d("b"))])
                .map_err(|e| format!("update: {e:?}"))?;
            if !n {
                return Err("the row was not updated".into());
            }
            rig_b.commit_rc(t).map_err(|e| format!("commit: {e:?}"))?;
            Ok(())
        })();
        done_tx.send(work).expect("done signal");
    });

    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        done_rx.try_recv().is_err(),
        "a key-column update must not run under a KEY SHARE holder"
    );

    // A releases: the update proceeds and its entry maintenance lands.
    release_tx.send(()).expect("release");
    match done_rx.recv_timeout(BOUND).expect("update finished") {
        Ok(()) => {}
        Err(e) => panic!("update thread failed: {e}"),
    }
    a.join().expect("A join");
    b.join().expect("B join");

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h2 = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(
        ok(rig.db.get(&st2, &h2, &[i32_v(1)])),
        Some(vec![i32_d(1), text_d("b")]),
        "the update applied after the holder released"
    );
    ok(rig.commit_rc(t2));
}
