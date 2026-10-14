//! C-T6b3b item 2, crash half: the crash-mid-ALTER matrix. For every
//! `keep` in `0..=unsynced_len()` — every prefix of the unsynced WAL
//! tail a crash may keep — over each of ADD COLUMN, DROP COLUMN (with a
//! dependent index) and SET NOT NULL, each with one concurrent committed
//! insert (a second txn that commits while the ALTER txn is open, with
//! `synchronous_commit = off` so its record sits in the tail too):
//! crash, reopen the whole stack (a new Core/Ssi/LockManager/Catalog on
//! the surviving store), resolve (the §7.2 boot sweep to completion),
//! and then the def is old-or-new (never anything else, and DROP
//! COLUMN's dependent index travels with it), every row decodes, and
//! amcheck (§8) is clean.

mod t6b_support;

use t6b_support::{commit_off, decode_def, i32_d, idx_def, ok, pk_schema, sweep, text_d, Rig};

use nucleus_catalog::{amcheck::check_table, IndexState, Stmt};
use nucleus_codec::{Collation, KeyType, Value};
use nucleus_txn::locks::RelLockMode;

/// Which ALTER a matrix cell crashes inside.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Alter {
    AddColumn,
    DropColumn,
    SetNotNull,
}

fn alter_name(alter: Alter) -> &'static str {
    match alter {
        Alter::AddColumn => "mx_add",
        Alter::DropColumn => "mx_drop",
        Alter::SetNotNull => "mx_nn",
    }
}

/// Builds one matrix scene on a fresh rig: the durable baseline (the
/// table, and for DROP COLUMN the dependent Public index on the column,
/// committed with sync), then the ALTER txn begun first, the concurrent
/// insert committed under it (`synchronous_commit = off`), the ALTER
/// statement and its own async commit, and a resolver pass — B's and
/// A's intents, commit records and resolutions all sit in the unsynced
/// tail in commit order, which is what the crash's prefix cut sweeps.
///
/// Returns the rig, the relation oid and the dependent index's oid.
fn build_scene(alter: Alter) -> (Rig, u64, Option<u64>) {
    let rig = Rig::new();
    let (oid, _sid) = {
        let t = rig.rc();
        let s = rig.core().visible_ts();
        let st = ok(Stmt::new(&t, s));
        let r = ok(rig
            .db
            .create_table(&st, alter_name(alter).as_bytes(), &pk_schema()));
        ok(rig.commit_rc(t));
        r
    };
    let idx_oid = match alter {
        Alter::DropColumn => Some(rig.add_index(oid, &idx_def(2, false, IndexState::Public)).0),
        _ => None,
    };

    // A: the ALTER txn, begun before the insert's.
    let ddl_txn = rig.rc();

    // B: the concurrent committed insert — committed while A is open,
    // with sync off so its intents and commit record join the unsynced
    // tail the crash cuts.
    {
        let b = rig.rc();
        let s = rig.core().visible_ts();
        let st = rig.stmt(&b, s);
        let h = ok(rig.db.open_table(&st, oid, RelLockMode::RowExclusive));
        ok(rig.db.insert_with(&st, &h, &[i32_d(1), text_d("b")]));
        commit_off(&rig, b);
    }

    // A: the ALTER statement (its scans see B's committed row), then an
    // async commit: A's intents and record land in the tail after B's.
    {
        let s = rig.core().visible_ts();
        let st = rig.stmt(&ddl_txn, s);
        match alter {
            Alter::AddColumn => {
                // The fast default: no scan; old rows read `missing`.
                ok(rig.db.add_column(
                    &st,
                    oid,
                    "note",
                    KeyType::Text(Collation::C),
                    false,
                    Some(Value::Text("m".into())),
                ));
            }
            Alter::DropColumn => {
                // Drops the dependent index in the same txn (§3).
                ok(rig.db.drop_column(&st, oid, 2));
            }
            Alter::SetNotNull => {
                // The validating scan sees B's committed 'b' (§3).
                ok(rig.db.set_not_null(&st, oid, 2, true));
            }
        }
        commit_off(&rig, ddl_txn);
    }
    // The resolver's writes (intent removals, version puts, /sys/txn
    // deletes) join the tail as well: the matrix cuts between them too.
    rig.settle();
    (rig, oid, idx_oid)
}

/// One matrix cell: the scene, the crash keeping `keep` unsynced
/// batches, the reopen and the §7.2 resolve, then the assertions.
/// Returns the def version the reopened catalog settled on (1 = the DDL
/// was lost, 2 = it committed).
fn cell(alter: Alter, keep: usize) -> u32 {
    let (rig, oid, idx_oid) = build_scene(alter);
    assert!(
        rig.kv.unsynced_len() > 0,
        "the scene must have an unsynced tail to cut"
    );

    // Crash keeping `keep` of the tail; reopen the whole stack on the
    // survivor; resolve (§7.2's boot sweep).
    let rig2 = rig.crash_reopen(keep);
    sweep(&rig2);

    // The def is old-or-new, and its shape agrees with the version.
    let rel = rig2.rel_row(oid).expect("the baseline def survived");
    let def = decode_def(&rel.def);
    assert!(
        def.version == 1 || def.version == 2,
        "def neither old nor new: version {} (keep {keep})",
        def.version
    );
    match alter {
        Alter::AddColumn if def.version == 2 => {
            assert_eq!(def.columns.len(), 3, "the added column is there");
            assert_eq!(
                def.columns[2].missing,
                Some(Value::Text("m".into())),
                "the fast default is in the def"
            );
        }
        Alter::AddColumn => assert_eq!(def.columns.len(), 2),
        Alter::DropColumn => {
            let io = idx_oid.expect("the DropColumn cell created the index");
            assert_eq!(
                rig2.idx_row(io).is_some(),
                def.version == 1,
                "the dependent index travels with the column (keep {keep})"
            );
            assert_eq!(def.columns[1].dropped, def.version == 2);
        }
        Alter::SetNotNull => assert_eq!(def.columns[1].not_null, def.version == 2),
    }

    // Every row decodes, and §8 holds: amcheck at a fresh snapshot of
    // the reopened state (I-ROWFMT includes "every row decodes").
    let guard = rig2.core().registry.take_snapshot();
    let report = ok(check_table(&rig2.db, oid, &guard));
    assert!(
        report.is_clean(),
        "amcheck after crash mid-{alter:?} keep={keep}: {:?}",
        report.violations
    );
    def.version
}

/// The matrix: the scene is deterministic, so the tail length is
/// measured once per ALTER kind on a probe build, and every `keep` in
/// `0..=n` then runs its own cell. Both def outcomes must occur across
/// the sweep (`keep = 0` loses the DDL, `keep = n` keeps it).
#[test]
fn crash_mid_alter_matrix() {
    for alter in [Alter::AddColumn, Alter::DropColumn, Alter::SetNotNull] {
        let (probe, _oid, _idx) = build_scene(alter);
        let n = probe.kv.unsynced_len();
        assert!(n > 0, "{alter:?}: the scene must build a tail");
        drop(probe);

        let mut saw_old = false;
        let mut saw_new = false;
        for keep in 0..=n {
            let v = cell(alter, keep);
            saw_old |= v == 1;
            saw_new |= v == 2;
        }
        assert!(
            saw_old && saw_new,
            "{alter:?}: the matrix must reach both def outcomes (n = {n})"
        );
    }
}
