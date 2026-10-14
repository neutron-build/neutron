//! C-T6c2 item 2, the DDL crash matrix: over `Fault<MemKv>` with the
//! hand-driven pipeline (the `t6b_support` rig), the six scenarios —
//! ADD COLUMN, DROP COLUMN (with a dependent index), SET NOT NULL,
//! TRUNCATE, born-public CREATE INDEX on an empty table, and an insert
//! into the no-PK table — each with one concurrent committed DML txn
//! (one `SyncCommit::On`, one `Off`), crashed at every `keep` in
//! `0..=unsynced_len()` and at 8 `crash_seeded` seeds, then reopened
//! (Core, Ssi, LockManager, `Catalog::open`) and resolved (§7.2).
//!
//! What every cell asserts — the def is old-or-new, every acked `On`
//! commit is present (I-DURABLE), every row decodes, amcheck is clean,
//! and a model built from the surviving committed set (the reopened
//! store's `/sys/txn` records) equals a full scan — lives in
//! `diff::crash::cell`, next to the scene builders and the model.
//!
//! Mutation evidence (binding, card item 3; applied, shown failing,
//! reverted — see the card's report):
//!
//! - **E5** — `drop_column` drops dependent indexes with a
//!   `Core::write` outside the txn: a low `keep` keeps the
//!   outside-the-txn delete of the index row but cuts the DDL's commit
//!   record, so the DropColumn scene reopens with the *old* definition
//!   and *no* index row — the old-or-new assertion fails
//!   (`t6c_crash_ddl_matrix`, DropColumn / On and Off alike).
//! - **E6** — born-public index entries written outside the txn: a cut
//!   that keeps the raw entry but loses the creating txn's commit
//!   leaves a visible entry with no row and no index row naming it —
//!   the CreateIndex scene's entry-equality assertion fails
//!   (`t6c_crash_ddl_matrix`, CreateIndex / On and Off alike).

mod diff;
mod t6b_support;

use diff::crash::{build_scene, cell, CrashAt, Scenario};
use nucleus_txn::commit::SyncCommit;

/// The matrix: for every scenario × DML sync mode, the scene is
/// deterministic, so the unsynced tail length is measured once on a
/// probe build and every `keep` in `0..=n` runs its own cell, plus the
/// 8 seeded draws. Both fates of the action txn's commit must occur
/// across each sweep (`keep = 0` loses it, `keep = n` keeps it) —
/// the cells assert the store agrees with whichever survived.
#[test]
fn t6c_crash_ddl_matrix() {
    let scenarios = [
        Scenario::AddColumn,
        Scenario::DropColumn,
        Scenario::SetNotNull,
        Scenario::Truncate,
        Scenario::CreateIndex,
        Scenario::InsertU,
    ];
    for (i, scenario) in scenarios.iter().enumerate() {
        for (j, b_sync) in [SyncCommit::On, SyncCommit::Off].iter().enumerate() {
            let (probe, scene) = build_scene(*scenario, *b_sync);
            let n = probe.kv.unsynced_len();
            assert!(
                n > 0,
                "{scenario:?} / DML {b_sync:?}: the scene must build an unsynced tail"
            );
            // The scene plumbing: the DML txn's acked On commit is the
            // last of them (the Off variant does not ack one).
            if *b_sync == SyncCommit::On {
                assert_eq!(
                    scene.acked_on.last(),
                    Some(&(scene.b.0, scene.b.1)),
                    "{scenario:?} / DML On: the acked On commits end with the DML txn's"
                );
            }
            drop(probe);
            let mut saw_new = false;
            let mut saw_old = false;
            let mut cells = 0usize;
            for keep in 0..=n {
                cells += 1;
                if cell(*scenario, *b_sync, CrashAt::Keep(keep)) {
                    saw_new = true;
                } else {
                    saw_old = true;
                }
            }
            for s in 0..8u64 {
                cells += 1;
                cell(*scenario, *b_sync, CrashAt::Seeded(seed_of(i, j, s)));
            }
            // The enumeration itself is pinned (c2 FU-A): every keep in
            // 0..=n plus the 8 seeded draws, n + 1 + 8 cells per variant.
            // A trimmed sweep — a stride, {0, n} only, an off-by-one
            // bound — keeps both outcome classes (keep=1 already loses
            // the action's record, keep=n-1 already keeps it) and would
            // otherwise silently lose the interior keeps the binding
            // mutants die on.
            assert_eq!(
                cells,
                n + 1 + 8,
                "{scenario:?} / DML {b_sync:?}: the sweep must run every keep in 0..={n} \
                 plus the 8 seeded draws"
            );
            assert!(
                saw_new && saw_old,
                "{scenario:?} / DML {b_sync:?}: the sweep (n = {n}) must reach both \
                 outcomes of the action txn's commit"
            );
        }
    }
}

/// The seeded draws' fixed seeds (distinct per scenario, sync mode and
/// draw, so the 8 draws of one variant are not one another's repeats).
fn seed_of(i: usize, j: usize, s: u64) -> u64 {
    0x7cc2_5000 | ((i as u64) << 8) | ((j as u64) << 5) | s
}
