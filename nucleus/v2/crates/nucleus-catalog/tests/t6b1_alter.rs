//! C-T6b1 item 9: `catalog_alter_rel_def` / `catalog_alter_idx_def` are
//! transactional — an abort (§7.1) and a `ROLLBACK TO` (§5.5) restore the
//! old row, and the altered row is visible to the DDL txn's own later
//! statements before that. Mutant N5: an alter written through `Core::write`
//! (outside the §5 path) — the abort would leave the new def behind.

mod t6b_support;

use t6b_support::{ok, pk_schema, Rig};

use nucleus_catalog::{IndexColumn, IndexDef, IndexState, Stmt};
use nucleus_codec::{Direction, Nulls};
use nucleus_txn::catalog::IdxStorage;
use nucleus_txn::locks::RelLockMode;

fn idx_def(state: IndexState) -> IndexDef {
    IndexDef {
        columns: vec![IndexColumn {
            attnum: 1,
            dir: Direction::Asc,
            nulls: Nulls::Last,
        }],
        unique: false,
        nulls_not_distinct: false,
        concurrently: false,
        state,
        built_at: 0,
    }
}

fn state_of(row: &nucleus_txn::catalog::IdxRow) -> IndexState {
    ok(IndexDef::decode(&row.def)).state
}

/// Mutant N5 (the abort half): an ALTER that writes its row through
/// `Core::write` instead of the §5 write path leaves the new def behind
/// the abort. The own-txn read in the middle pins that the alter really
/// took effect before the abort.
#[test]
fn alter_rel_def_abort_restores_the_old_row() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let schema2 = ok(pk_schema().add_column("extra", nucleus_codec::KeyType::Int8, false, None));
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t, s), oid, &ok(schema2.encode())));

    // The DDL txn's own later statement sees the new def (uncommitted,
    // visible to itself).
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(h.def_version, 2);
    assert_eq!(h.schema.columns.len(), 3);

    rig.abort_txn(t);
    let row = rig.rel_row(oid).expect("the row survives the abort");
    let restored = t6b_support::decode_def(&row.def);
    assert_eq!(restored.version, 1, "the abort restores the old def");
    assert_eq!(restored.columns.len(), 2);
}

/// Mutant N5 (the rollback half): the same, through `ROLLBACK TO
/// SAVEPOINT` — the §5.5 layer drop restores the old row, and the txn
/// continues cleanly afterwards.
#[test]
fn alter_rel_def_rollback_to_restores_the_old_row() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let sp = ok(t.savepoint());
    let schema2 = ok(pk_schema().add_column("extra", nucleus_codec::KeyType::Int8, false, None));
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t, s), oid, &ok(schema2.encode())));
    ok(rig.core().rollback_to(&t, sp));

    // The txn's own read after the rollback: the old def again.
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(h.def_version, 1, "the rollback restores the old def");

    // The txn continues: a second alter, then an abort, restores again.
    let schema3 = ok(schema2.add_column("another", nucleus_codec::KeyType::Int4, false, None));
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t, s), oid, &ok(schema3.encode())));
    rig.abort_txn(t);
    let restored = t6b_support::decode_def(&rig.rel_row(oid).expect("row").def);
    assert_eq!(restored.version, 1, "nothing the txn altered survived");
    assert_eq!(restored.columns.len(), 2);
}

/// The index twin, through the state ladder a build drives: alter to
/// WriteOnly, ROLLBACK TO restores DeleteOnly, abort after a later alter
/// still restores DeleteOnly.
#[test]
fn alter_idx_def_rollback_and_abort_restore_the_old_def() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, _sid) = ok(rig.db.create_table(&st, b"pk", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t1 = rig.rc();
    let s1 = rig.core().visible_ts();
    let isid = ok(rig.catalog().alloc_storage_id(rig.core()));
    let (idx_oid, _) = ok(rig.catalog().catalog_create_index(
        &rig.ddl(&t1, s1),
        oid,
        IdxStorage::Id(isid),
        &ok(idx_def(IndexState::DeleteOnly).encode()),
    ));
    ok(rig.commit_rc(t1));
    assert_eq!(
        state_of(&rig.idx_row(idx_oid).expect("idx row")),
        IndexState::DeleteOnly
    );

    // ROLLBACK TO restores the pre-alter def inside the txn.
    let t = rig.rc();
    let s = rig.core().visible_ts();
    let sp = ok(t.savepoint());
    ok(rig.catalog().catalog_alter_idx_def(
        &rig.ddl(&t, s),
        idx_oid,
        &ok(idx_def(IndexState::WriteOnly).encode()),
        RelLockMode::AccessExclusive,
    ));
    // The own-txn read sees WriteOnly before the rollback.
    let st2 = rig.stmt(&t, s);
    let h = ok(rig.db.open_table(&st2, oid, RelLockMode::AccessShare));
    assert_eq!(h.indexes[0].def.state, IndexState::WriteOnly);
    ok(rig.core().rollback_to(&t, sp));
    let st3 = rig.stmt(&t, s);
    let h3 = ok(rig.db.open_table(&st3, oid, RelLockMode::AccessShare));
    assert_eq!(
        h3.indexes[0].def.state,
        IndexState::DeleteOnly,
        "the rollback restores the pre-alter index def"
    );

    // A later alter + abort: still DeleteOnly.
    ok(rig.catalog().catalog_alter_idx_def(
        &rig.ddl(&t, s),
        idx_oid,
        &ok(idx_def(IndexState::Public).encode()),
        RelLockMode::AccessExclusive,
    ));
    rig.abort_txn(t);
    assert_eq!(
        state_of(&rig.idx_row(idx_oid).expect("idx row after abort")),
        IndexState::DeleteOnly
    );
}

/// The alter entry points refuse a missing relation / index (the txn
/// layer's `catalog_miss`, XX000) and the alter keeps the storage id and
/// name — only `def` changes.
#[test]
fn alter_targets_only_the_def_and_misses_cleanly() {
    let rig = Rig::new();
    let t0 = rig.rc();
    let s0 = rig.core().visible_ts();
    let st = ok(Stmt::new(&t0, s0));
    let (oid, sid) = ok(rig.db.create_table(&st, b"named", &pk_schema()));
    ok(rig.commit_rc(t0));

    let t = rig.rc();
    let s = rig.core().visible_ts();
    let schema2 = ok(pk_schema().set_not_null(2, true));
    ok(rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t, s), oid, &ok(schema2.encode())));
    ok(rig.commit_rc(t));
    let row = rig.rel_row(oid).expect("row");
    assert_eq!(row.storage_id, sid, "ALTER keeps the storage id (§3)");
    assert_eq!(row.name, b"named".to_vec(), "ALTER keeps the name");
    assert_eq!(t6b_support::decode_def(&row.def).version, 2);

    let t2 = rig.rc();
    let s2 = rig.core().visible_ts();
    let err = rig
        .catalog()
        .catalog_alter_rel_def(&rig.ddl(&t2, s2), 999, &ok(schema2.encode()))
        .expect_err("missing relation");
    assert_eq!(err.sqlstate(), "XX000");
    let err = rig
        .catalog()
        .catalog_alter_idx_def(
            &rig.ddl(&t2, s2),
            999,
            &ok(idx_def(IndexState::Public).encode()),
            RelLockMode::AccessExclusive,
        )
        .expect_err("missing index");
    assert_eq!(err.sqlstate(), "XX000");
    rig.abort_txn(t2);
}
