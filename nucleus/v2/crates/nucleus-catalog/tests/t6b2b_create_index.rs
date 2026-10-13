//! C-T6b2b items 1 and 3: born-public CREATE INDEX (C-T6 §6.1) — the two
//! cases (the txn's own relation; an empty pre-existing one under a Share
//! lock), their abort / `ROLLBACK TO` cleanliness (no idx row, no visible
//! entry), the 0A000 refusal on a non-empty pre-existing relation (unique
//! or not), unique enforcement after a born-public creation (23505 on a
//! duplicate insert and on a key-changing update), the §7.2 own-txn route,
//! and the §6.1(b) Share-lock discipline against a concurrent inserter
//! (mutant N20).

mod t6b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use t6b_support::{
    i32_d, i32_v, idx_def, ok, pk_schema, raw_entry_trace, text_d, wait_until, Rig, ThreadedRig,
};

use nucleus_catalog::{IndexDef, IndexState, Stmt};
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
fn add_row(rig: &Rig, oid: u64, id: i32, payload: &str) {
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(id), text_d(payload)]));
    ok(rig.commit_rc(t));
}

/// An index definition whose state and built_at the born-public path must
/// override (whatever the caller passed, the row lands `Public`).
fn any_state_def(unique: bool) -> IndexDef {
    let mut def = idx_def(2, unique, IndexState::DeleteOnly);
    def.built_at = u64::MAX / 2;
    def
}

/// The storage id of a committed index row.
fn isid_of(rig: &Rig, idx_oid: u64) -> u64 {
    let row = rig.idx_row(idx_oid).expect("idx row committed");
    let IdxStorage::Id(isid) = row.storage else {
        panic!("a born-public index has an id storage")
    };
    isid
}

/// §6.1 case (a): a relation created by the txn itself — the index is
/// born `Public` with `built_at = 0`, the own rows indexed by the
/// statement, and the abort takes row, index and entries away together.
#[test]
fn born_public_on_own_table_and_abort_leaves_nothing() {
    let rig = Rig::new();
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"own", &pk_schema()));

    // Two rows in a later statement of the same txn.
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("a")]));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(2), text_d("b")]));

    // Born public on the own table: the input's state/built_at are
    // overridden.
    let st3 = rig.stmt(&t, s);
    let idx_oid = ok(rig.db.create_index(&st3, oid, &any_state_def(false)));

    // The txn's own later statement sees it: Public, built_at 0.
    let st4 = rig.stmt(&t, s);
    let h4 = ok(rig.db.open_table(&st4, oid, RelLockMode::AccessShare));
    assert_eq!(h4.indexes.len(), 1, "the creating txn sees its own index");
    assert_eq!(h4.indexes[0].def.state, IndexState::Public);
    assert_eq!(h4.indexes[0].def.built_at, 0, "case (a): built_at = 0");
    assert!(!h4.indexes[0].def.concurrently);

    // The own rows' entries exist as the txn's intents.
    assert!(
        !raw_entry_trace(&rig).is_empty(),
        "the own rows have their entries"
    );

    // Abort: no row, no idx row, no entry survives.
    rig.abort_txn(t);
    assert_eq!(rig.rel_row(oid), None, "the aborted CREATE TABLE is gone");
    assert_eq!(rig.idx_row(idx_oid), None, "no idx row");
    assert!(
        raw_entry_trace(&rig).is_empty(),
        "no entry of the aborted index survives"
    );
}

/// §6.1 case (b): an empty pre-existing relation — `ROLLBACK TO` (and an
/// abort) of the CREATE INDEX leaves no idx row and no visible entry; the
/// inserted row that predates the savepoint survives the rollback.
#[test]
fn born_public_on_empty_preexisting_rollback_and_abort_leave_nothing() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"pre");

    // Abort: the index and its single entry vanish with the txn.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), text_d("a")]));
        let st2 = rig.stmt(&t, s);
        let idx_oid = ok(rig.db.create_index(&st2, oid, &any_state_def(true)));
        assert_eq!(
            raw_entry_trace(&rig).len(),
            1,
            "the one own row got exactly one entry"
        );
        rig.abort_txn(t);
        assert_eq!(rig.idx_row(idx_oid), None, "abort leaves no idx row");
        assert!(raw_entry_trace(&rig).is_empty(), "abort leaves no entry");
    }

    // ROLLBACK TO the savepoint taken between the insert and the CREATE
    // INDEX: the index and its entry go, the row stays.
    {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(2), text_d("b")]));
        let sp = ok(t.savepoint());
        let st2 = rig.stmt(&t, s);
        let idx_oid = ok(rig.db.create_index(&st2, oid, &any_state_def(false)));
        ok(rig.core().rollback_to(&t, sp));
        assert_eq!(rig.idx_row(idx_oid), None, "the rollback leaves no idx row");
        assert!(
            raw_entry_trace(&rig).is_empty(),
            "the rollback leaves no entry"
        );
        // The txn continues and commits its row.
        ok(rig.commit_rc(t));
        let t2 = rig.rc();
        let s2 = rig.core().visible_ts();
        let st3 = rig.stmt(&t2, s2);
        let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
        assert_eq!(ok(rig.db.scan(&st3, &h3)).len(), 1, "the row survived");
        ok(rig.commit_rc(t2));
    }
}

/// §6.1's refusal: a pre-existing relation with any committed row visible
/// at `S_e` → 0A000, unique or not (§9.1: the in-block build is C-T6h's).
#[test]
fn nonempty_preexisting_refused_inside_a_txn() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"full");
    add_row(&rig, oid, 1, "a");

    for unique in [true, false] {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&t, s);
        let err = rig
            .db
            .create_index(&st, oid, &any_state_def(unique))
            .expect_err("a non-empty pre-existing relation refuses the build");
        assert_eq!(err.sqlstate(), "0A000", "unique = {unique}");
        rig.abort_txn(t);
    }
    assert!(raw_entry_trace(&rig).is_empty(), "nothing was written");
}

/// A born-public **unique** index enforces from its creation on: a later
/// duplicate insert is 23505, and a later key-changing update into an
/// existing key is 23505 (§5.3 through the /u/ entry's unique rule).
#[test]
fn unique_born_public_rejects_duplicate_insert_and_key_change() {
    let rig = Rig::new();
    let (oid, _sid) = mk_table(&rig, b"u");

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st, &h, &[i32_d(1), text_d("dup")]));
    let st2 = rig.stmt(&t, s);
    let idx_oid = ok(rig.db.create_index(&st2, oid, &any_state_def(true)));
    let ts = ok(rig.commit_rc(t));

    // The committed row: Public, born at the creating statement's S_e.
    let row = rig.idx_row(idx_oid).expect("idx row committed");
    let def = ok(IndexDef::decode(&row.def));
    assert_eq!(def.state, IndexState::Public);
    assert_eq!(def.built_at, s.0, "case (b): built_at = S_e");
    assert!(def.built_at <= ts.0);

    // A duplicate insert: 23505.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st3 = rig.stmt(&t2, s2);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    let err = rig
        .db
        .insert_with(&st3, &h3, &[i32_d(2), text_d("dup")])
        .expect_err("a duplicate key value is refused");
    assert_eq!(err.sqlstate(), "23505");
    rig.abort_txn(t2);

    // A distinct row commits; updating it into row 1's key: 23505.
    add_row(&rig, oid, 2, "other");
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st4 = rig.stmt(&t3, s3);
    let err = rig
        .db
        .update(
            &st4,
            oid,
            &[i32_v(2)],
            &[t6b_support::set_col(2, text_d("dup"))],
        )
        .expect_err("an update into an existing key is refused");
    assert_eq!(err.sqlstate(), "23505");
    rig.abort_txn(t3);

    // The good path still works.
    add_row(&rig, oid, 3, "z");
    let t4 = rig.rc();
    let s4 = rig.core().visible_ts();
    let st5 = rig.stmt(&t4, s4);
    let h5 = ok(rig.db.open_table(&st5, oid, RelLockMode::AccessShare));
    assert_eq!(ok(rig.db.scan(&st5, &h5)).len(), 3, "1, 2 and 3 are live");
    ok(rig.commit_rc(t4));
}

/// Mutant N20 (the emptiness check without the Share lock): thread B
/// inserts a row and holds its txn open; thread A's CREATE INDEX must
/// park on B's RowExclusive **before** its emptiness scan, so after B
/// commits the scan sees B's row and refuses (0A000). Without the Share
/// lock first, A would scan B's uncommitted state empty, park only at the
/// index-row creation, and return Ok over a table that has a committed
/// row with no entry — caught here by the refusal itself.
#[test]
fn emptiness_check_waits_for_the_inserter_then_refuses() {
    let rig = Arc::new(Rig::new());
    let (oid, _sid) = mk_table(&rig, b"ins");

    // B: an insert txn that stays open until the test releases it.
    let (held_tx, held_rx) = mpsc::channel::<()>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let h = ok(rig_b.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig_b.db.insert_with(&st, &h, &[i32_d(1), text_d("b")]));
        held_tx.send(()).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_b.commit_rc(t));
    });
    held_rx.recv_timeout(BOUND).expect("B holds RowExclusive");

    // A: CREATE INDEX — parks on B's RowExclusive at the Share lock.
    let (res_tx, res_rx) = mpsc::channel::<Result<u64, String>>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let r = rig_a.db.create_index(&st, oid, &any_state_def(true));
        match r {
            Ok(oid) => {
                ok(rig_a.commit_rc(t));
                res_tx.send(Ok(oid)).expect("res signal");
            }
            Err(e) => {
                rig_a.abort_txn(t);
                res_tx
                    .send(Err(e.sqlstate().to_string()))
                    .expect("res signal");
            }
        }
    });

    // A is parked on B (the only wait this schedule can produce); B's
    // commit is what unblocks it.
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    release_tx.send(()).expect("release");

    // A wakes with B's row committed at its S_e: the emptiness check
    // refuses.
    match res_rx.recv_timeout(BOUND).expect("A finished within bound") {
        Ok(oid) => panic!("the build must refuse; got an index oid {oid}"),
        Err(sqlstate) => assert_eq!(sqlstate, "0A000", "the committed row is visible at S_e"),
    }
    b.join().expect("B join");
    a.join().expect("A join");
    assert!(raw_entry_trace(&rig).is_empty(), "no index was built");
}

/// The §6.1(b) Share lock is held to the creating txn's end (§6): an
/// insert that starts under a finished born-public CREATE INDEX parks on
/// the creator's Share and completes only after it commits — and, its
/// catalog read then including the committed index row, it maintains the
/// index (§4: the writer's catalog read covers the index rows).
#[test]
fn insert_blocks_on_the_creator_until_it_commits() {
    let rig = Arc::new(Rig::new());
    let (oid, _sid) = mk_table(&rig, b"blk");

    // A: create the index born public, hold the txn open.
    let (held_tx, held_rx) = mpsc::channel::<u64>();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let rig_a = Arc::clone(&rig);
    let a = thread::spawn(move || {
        let t = rig_a.rc();
        let s = rig_a.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let idx_oid = ok(rig_a.db.create_index(&st, oid, &any_state_def(false)));
        held_tx.send(idx_oid).expect("held signal");
        release_rx.recv_timeout(BOUND).expect("release signal");
        ok(rig_a.commit_rc(t));
    });
    let idx_oid = held_rx
        .recv_timeout(BOUND)
        .expect("the creator holds Share");

    // B: the insert parks on A's Share.
    let (done_tx, done_rx) = mpsc::channel::<()>();
    let rig_b = Arc::clone(&rig);
    let b = thread::spawn(move || {
        let t = rig_b.rc();
        let s = rig_b.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let h = ok(rig_b.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig_b.db.insert_with(&st, &h, &[i32_d(9), text_d("late")]));
        ok(rig_b.commit_rc(t));
        done_tx.send(()).expect("done signal");
    });
    wait_until(BOUND, || !rig.wait_edges().is_empty());
    assert!(
        done_rx.try_recv().is_err(),
        "the insert must not complete under the creator's Share lock"
    );

    release_tx.send(()).expect("release");
    done_rx
        .recv_timeout(BOUND)
        .expect("the insert completed after the commit");
    a.join().expect("A join");
    b.join().expect("B join");

    // B's insert maintained the born-public index (one entry, its row's).
    let isid = isid_of(&rig, idx_oid);
    assert_eq!(
        rig.scan_entries(isid).len(),
        1,
        "the late insert is indexed"
    );
}

/// §7.2: the own-txn route for a plain CREATE UNIQUE INDEX outside a
/// transaction block — one RC txn that begins, creates and commits by
/// itself (the ack needs the running commit thread), so it succeeds on an
/// empty pre-existing table.
#[test]
fn create_index_own_txn_unique_on_empty_preexisting() {
    let rig = ThreadedRig::new();
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let st = ok(Stmt::new(&t, s));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"own_txn", &pk_schema()));
    ok(rig.commit_rc(t));

    let idx_oid = ok(nucleus_catalog::ddl::create_index_own_txn(
        &rig.db,
        oid,
        &any_state_def(true),
    ));
    let row = rig.idx_row(idx_oid).expect("the own txn committed the row");
    let def = ok(IndexDef::decode(&row.def));
    assert_eq!(def.state, IndexState::Public);
    assert!(def.built_at > 0, "case (b): born at its own S_e");
    assert!(!def.concurrently);

    // The born-public unique index enforces: a duplicate insert is 23505.
    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let st2 = rig.stmt(&t2, s2);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::RowExclusive));
    ok(rig.db.insert_with(&st2, &h, &[i32_d(1), text_d("k")]));
    ok(rig.commit_rc(t2));
    let t3 = rig.rc();
    let s3 = rig.core().visible_ts();
    let st3 = rig.stmt(&t3, s3);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::RowExclusive));
    let err = rig
        .db
        .insert_with(&st3, &h3, &[i32_d(2), text_d("k")])
        .expect_err("the unique index refuses the duplicate");
    assert_eq!(err.sqlstate(), "23505");
    rig.abort_txn(t3);

    rig.shutdown();
}
