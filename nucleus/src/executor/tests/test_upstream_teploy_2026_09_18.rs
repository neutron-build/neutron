//! Regression tests for the two 2026-09-18 teploy-observe upstream reports
//! (Teploy `_internal/UPSTREAM_BUGS.md`, newest entries).
//!
//! Both defects hit the DISK stack — `BufferedDiskEngine` over `DiskEngine` —
//! which is what `main.rs` builds for every server deployment with a data
//! directory. The in-memory MVCC adapter is a different engine and never had
//! either symptom, so these tests construct the server shape directly.
//!
//! Defect 1 — "renamed table invisible to later statements in the same
//! script": a transaction that CREATEs a table and RENAMEs it failed at
//! COMMIT, because the buffered `CreateTable` op replayed against a catalog
//! the rename had already rewritten (`DiskEngine::create_table` re-asked the
//! catalog for the schema under the old name and answered `table '<old>' not
//! found in storage`). Fresh installs hit it through neutron-go's migration
//! runner, which sends a whole `.up.sql` through one pgx transaction.
//! Verified broken on the teploy-observe pinned submodule revision (5b5d0a3)
//! at migration 027, and at HEAD for the create+rename shape.
//!
//! Defect 2 — "committed replacing upsert lost in a multi-table transaction":
//! a same-key ReplacingMergeTree upsert committed as part of a multi-table tx
//! silently vanished once MergeTree part merges compacted the table
//! (delayed, so an immediate verification could pass while the row was later
//! dropped). Reproduced 8/20 on the v0.1.8 image lineage (and on main at
//! d4a30583, 2026-08-20); fixed on main by the 2026-08-25 columnar atomicity
//! work (b172b43f). This pins the observe replay-session pattern so it cannot
//! return unnoticed.

use std::sync::Arc;

use super::*;
use crate::catalog::Catalog;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;
use crate::storage::StorageEngine;

/// An executor over `BufferedDiskEngine(DiskEngine)` — the exact storage
/// shape a server builds with a data directory, where both defects lived.
fn disk_executor(dir: &std::path::Path) -> Executor {
    let catalog = Arc::new(Catalog::new());
    let disk = Arc::new(DiskEngine::open(&dir.join("t.db"), catalog.clone()).unwrap());
    let engine: Arc<dyn StorageEngine> = Arc::new(BufferedDiskEngine::new(disk));
    Executor::new(catalog, engine)
}

fn i64_of(result: &ExecResult) -> i64 {
    match result {
        ExecResult::Select { rows, .. } => match &rows[0][0] {
            Value::Int64(v) => *v,
            Value::Int32(v) => *v as i64,
            Value::Float64(v) => *v as i64,
            other => panic!("unexpected cell: {other:?}"),
        },
        other => panic!("expected Select, got {other:?}"),
    }
}

// ===========================================================================
// Defect 1 — rename visibility inside one transaction / script
// ===========================================================================

/// The minimal failing shape: one multi-statement script (the wire-level
/// shape a migration runner sends), containing BEGIN, a CREATE, an INSERT
/// into the created table, a RENAME of that table, a read of the renamed
/// table, and COMMIT. Before the fix the COMMIT failed with
/// `table '<old>' not found in storage` and left DDL debris behind.
#[tokio::test]
async fn create_rename_commit_in_one_script() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(
        &ex,
        "BEGIN; \
         CREATE TABLE d1_events (event_id TEXT, ts BIGINT); \
         INSERT INTO d1_events VALUES ('e1', 1), ('e2', 2); \
         ALTER TABLE d1_events RENAME TO d1_events_pre; \
         SELECT count(*) FROM d1_events_pre; \
         COMMIT;",
    )
    .await;
    let count = i64_of(&exec(&ex, "SELECT count(*) FROM d1_events_pre").await[0]);
    assert_eq!(count, 2, "renamed table must keep its rows after COMMIT");
}

/// The migration-027 shape: rename an already-committed table aside, recreate
/// the original name as a plain table, and copy across with INSERT..SELECT
/// FROM the renamed table — all inside the same transaction. The pinned-rev
/// failure (`table 'events_pre027' not found`) lived on this path.
#[tokio::test]
async fn migration_027_rebuild_shape_in_one_tx() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(&ex, "CREATE TABLE events (event_id TEXT, ver INT)").await;
    exec(&ex, "INSERT INTO events VALUES ('a', 1), ('b', 2), ('c', 3)").await;
    exec(
        &ex,
        "BEGIN; \
         ALTER TABLE events RENAME TO events_pre027; \
         CREATE TABLE events (event_id TEXT, ver INT, extra TEXT DEFAULT 'x'); \
         INSERT INTO events (event_id, ver) SELECT event_id, ver FROM events_pre027; \
         COMMIT;",
    )
    .await;
    let counts = exec(
        &ex,
        "SELECT (SELECT count(*) FROM events), (SELECT count(*) FROM events_pre027)",
    )
    .await;
    match &counts[0] {
        ExecResult::Select { rows, .. } => {
            let new_count = match &rows[0][0] {
                Value::Int64(v) => *v,
                other => panic!("unexpected cell: {other:?}"),
            };
            let old_count = match &rows[0][1] {
                Value::Int64(v) => *v,
                other => panic!("unexpected cell: {other:?}"),
            };
            assert_eq!(new_count, 3, "rebuilt table must carry the copied rows");
            assert_eq!(old_count, 3, "renamed-aside table must remain as written");
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

/// The rename of a per-table ENGINE override (MergeTree) inside a
/// transaction — observe's `events` tables are `WITH (engine='mergetree')`,
/// so migration 027 renamed override-engine tables, not heap tables.
#[tokio::test]
async fn mergetree_override_rename_in_tx() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(
        &ex,
        "CREATE TABLE m_events (k TEXT, ts BIGINT) WITH (engine='mergetree') ORDER BY (k)",
    )
    .await;
    exec(&ex, "INSERT INTO m_events VALUES ('a', 1), ('b', 2)").await;
    exec(
        &ex,
        "BEGIN; \
         ALTER TABLE m_events RENAME TO m_events_pre; \
         CREATE TABLE m_events (k TEXT, ts BIGINT); \
         INSERT INTO m_events SELECT k, ts FROM m_events_pre; \
         COMMIT;",
    )
    .await;
    let count = i64_of(&exec(&ex, "SELECT count(*) FROM m_events").await[0]);
    assert_eq!(count, 2, "override-engine rename must copy rows inside the tx");
    let old = i64_of(&exec(&ex, "SELECT count(*) FROM m_events_pre").await[0]);
    assert_eq!(old, 2, "renamed-aside override table must remain readable");
}

// ===========================================================================
// Defect 2 — committed replacing upsert lost in a multi-table transaction
// ===========================================================================

/// The observe replay-session pattern, reduced: a ReplacingMergeTree keyed on
/// (part, start_time, k) with a version column, an accumulated body of prior
/// rows, and per iteration (a) tx1 inserts the first version and commits;
/// (b) tx2 reads the collapsed state, inserts the merged HIGHER-version row
/// AND two child rows in a second table, and commits; (c) the collapsed read
/// must show the second row's values. The v0.1.8-era defect additionally
/// surfaced DELAYED — later part merges dropped the committed row — so after
/// the loop, extra inserts force merge pressure and EVERY key is re-verified.
#[tokio::test]
async fn replacing_upsert_multi_table_tx_survives_and_survives_merges() {
    let dir = tempfile::tempdir().unwrap();
    let ex = disk_executor(dir.path());
    exec(
        &ex,
        "CREATE TABLE replay_sessions ( \
             part TEXT NOT NULL, start_time BIGINT NOT NULL, k TEXT NOT NULL, \
             ver BIGINT NOT NULL, duration INTEGER NOT NULL DEFAULT 0, \
             page_count INTEGER NOT NULL DEFAULT 0 \
         ) WITH (engine = 'replacing_mergetree', version_column = 'ver') \
         ORDER BY (part, start_time, k)",
    )
    .await;
    exec(&ex, "CREATE TABLE replay_events (id INTEGER, site TEXT)").await;

    // Accumulated prior data — the loss was not observable on an empty table.
    for chunk in 0..3 {
        let values: Vec<String> = (0..100)
            .map(|i| format!("('p', {}, 'seed', 1, 1, 1)", 100_000 + chunk * 100 + i))
            .collect();
        exec(
            &ex,
            &format!(
                "INSERT INTO replay_sessions (part, start_time, k, ver, duration, page_count) VALUES {}",
                values.join(",")
            ),
        )
        .await;
    }

    const ITERS: i64 = 25;
    for i in 0..ITERS {
        let key = 200_000 + i;
        // (a) first version, own transaction
        exec(&ex, "BEGIN").await;
        exec(
            &ex,
            &format!(
                "INSERT INTO replay_sessions (part, start_time, k, ver, duration, page_count) \
                 VALUES ('p', {key}, 's', 1, 1, 1)"
            ),
        )
        .await;
        exec(&ex, "COMMIT").await;

        // (b) read collapsed state, upsert the merged higher version and two
        // child rows in ONE transaction — the multi-table tx from the report.
        exec(&ex, "BEGIN").await;
        let read = i64_of(
            &exec(
                &ex,
                &format!(
                    "SELECT duration FROM replay_sessions \
                     WHERE part='p' AND start_time={key} AND k='s'"
                ),
            )
            .await[0],
        );
        assert_eq!(read, 1, "tx2 must read the first version inside its tx");
        exec(
            &ex,
            &format!(
                "INSERT INTO replay_sessions (part, start_time, k, ver, duration, page_count) \
                 VALUES ('p', {key}, 's', 2, 9999, 99)"
            ),
        )
        .await;
        exec(
            &ex,
            &format!("INSERT INTO replay_events (id, site) VALUES ({i}, 'x'), ({i}, 'y')"),
        )
        .await;
        exec(&ex, "COMMIT").await;

        // (c) the collapsed read must show the second row's values.
        let after = i64_of(
            &exec(
                &ex,
                &format!(
                    "SELECT duration FROM replay_sessions \
                     WHERE part='p' AND start_time={key} AND k='s'"
                ),
            )
            .await[0],
        );
        assert_eq!(after, 9999, "iteration {i}: committed upsert vanished");
    }

    // Merge pressure: many more part-creating inserts, then re-verify EVERY
    // key. The v0.1.8-era loss was delayed — the immediate read could pass
    // while a later merge dropped the committed row.
    for chunk in 0..5 {
        let values: Vec<String> = (0..50)
            .map(|i| format!("('q', {}, 'bulk', 1, 1, 1)", 300_000 + chunk * 50 + i))
            .collect();
        exec(
            &ex,
            &format!(
                "INSERT INTO replay_sessions (part, start_time, k, ver, duration, page_count) VALUES {}",
                values.join(",")
            ),
        )
        .await;
        let lost = i64_of(
            &exec(
                &ex,
                "SELECT count(*) FILTER (WHERE duration = 1) FROM replay_sessions \
                 WHERE part='p' AND k='s'",
            )
            .await[0],
        );
        assert_eq!(lost, 0, "after merge-pressure chunk {chunk}: a committed upsert was dropped");
    }

    // The child rows of the multi-table txs must all be there too.
    let children = i64_of(&exec(&ex, "SELECT count(*) FROM replay_events").await[0]);
    assert_eq!(children, ITERS * 2, "child rows from the multi-table txs were lost");
}
