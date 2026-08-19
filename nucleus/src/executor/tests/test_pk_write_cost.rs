//! `UPDATE`/`DELETE` by primary key must not read the whole table.
//!
//! A single-row `DELETE FROM t WHERE id = K` walked every tuple in `t`, once
//! per statement, so a batch of deletes was quadratic in the table. The
//! executor's PK equality path calls `scan_where_eq_positions`, which on the
//! disk engine is a full page scan with an inline filter — and the comment at
//! the call site calls it a "fast path".
//!
//! What hid it: `rows_scanned` counts MATCHES, not tuples examined, so a
//! single-row delete reported `rows_scanned = 1` while examining 20,000 rows.
//! The first investigation used that metric to RULE OUT scanning. So this test
//! counts the other thing (`bench_hooks::tuples_examined`), and asserts a bound
//! that the pre-fix behaviour cannot meet.

use super::*;
use crate::bench_hooks;
use crate::storage::buffered_engine::BufferedDiskEngine;
use crate::storage::disk_engine::DiskEngine;

const ROWS: i32 = 2_000;
/// A point lookup may touch a handful of index and data pages. It may not
/// touch a number that grows with the table: this bound is ~2% of `ROWS`, and
/// a full scan is 100%.
const BOUND: u64 = 64;

async fn seeded(dir: &std::path::Path) -> Arc<Executor> {
    let catalog = Arc::new(crate::catalog::Catalog::new());
    let db = dir.join("cost.db");
    let engine = Arc::new(DiskEngine::open(&db, catalog.clone()).unwrap());
    let buffered = Arc::new(BufferedDiskEngine::new(engine));
    let ex = Arc::new(Executor::new(
        catalog,
        buffered as Arc<dyn crate::storage::StorageEngine>,
    ));
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(200) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
        exec(&ex, &format!("INSERT INTO t VALUES {}", values.join(", "))).await;
    }
    ex
}

#[tokio::test]
async fn a_delete_by_primary_key_does_not_read_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;

    bench_hooks::reset_tuples_examined();
    exec(&ex, "DELETE FROM t WHERE id = 1000").await;
    let examined = bench_hooks::tuples_examined();

    assert!(
        examined <= BOUND,
        "DELETE by primary key examined {examined} tuples in a {ROWS}-row table; \
         a point delete must not scan the table (bound {BOUND})"
    );
}

#[tokio::test]
async fn an_update_by_primary_key_does_not_read_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;

    bench_hooks::reset_tuples_examined();
    exec(&ex, "UPDATE t SET v = 'changed' WHERE id = 1000").await;
    let examined = bench_hooks::tuples_examined();

    assert!(
        examined <= BOUND,
        "UPDATE by primary key examined {examined} tuples in a {ROWS}-row table; \
         a point update must not scan the table (bound {BOUND})"
    );
}

/// The control that makes the bound meaningful: a predicate with no index MUST
/// still scan. Without this, "examined 0" would pass both tests above and prove
/// only that the counter stopped working.
#[tokio::test]
async fn a_non_indexed_predicate_still_scans() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;

    bench_hooks::reset_tuples_examined();
    exec(&ex, "DELETE FROM t WHERE v = 'v1500'").await;
    let examined = bench_hooks::tuples_examined();

    assert!(
        examined >= ROWS as u64,
        "a DELETE on an unindexed column examined only {examined} tuples of {ROWS}; \
         the counter is not measuring what this file assumes"
    );
}

/// And the row is actually gone — a "fast" delete that deletes nothing would
/// satisfy every bound above.
#[tokio::test]
async fn the_point_delete_deletes_exactly_one_row() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;

    exec(&ex, "DELETE FROM t WHERE id = 1000").await;
    let res = exec(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(ROWS as i64 - 1));
    let res = exec(&ex, "SELECT COUNT(*) FROM t WHERE id = 1000").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
    let res = exec(&ex, "SELECT v FROM t WHERE id = 999").await;
    assert_eq!(rows(&res[0]).len(), 1, "a neighbouring row was removed too");
}

/// A UNIQUE column takes the same path — the fast path keys off the
/// constraint, not off the primary key specifically.
#[tokio::test]
async fn a_unique_column_also_serves_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;
    exec(&ex, "CREATE TABLE u (id INT PRIMARY KEY, code TEXT UNIQUE)").await;
    for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(200) {
        let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'c{i}')")).collect();
        exec(&ex, &format!("INSERT INTO u VALUES {}", values.join(", "))).await;
    }

    bench_hooks::reset_tuples_examined();
    exec(&ex, "DELETE FROM u WHERE code = 'c1500'").await;
    let examined = bench_hooks::tuples_examined();
    assert!(
        examined <= BOUND,
        "DELETE by a UNIQUE column examined {examined} tuples of {ROWS}"
    );
    let res = exec(&ex, "SELECT COUNT(*) FROM u WHERE code = 'c1500'").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

/// The case the original attribution measured: a batch of point deletes inside
/// ONE transaction. That is where the O(table) cost was quadratic — every
/// statement materialised the whole table through the transaction overlay — and
/// where an index that declined to answer inside a transaction would have left
/// it exactly as it was.
#[tokio::test]
async fn a_batch_of_point_deletes_in_one_transaction_does_not_rescan() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;
    let sid = ex.create_session();
    let run = |sql: String| {
        let ex = &ex;
        async move { ex.execute_with_session(sid, &sql).await.expect("stmt") }
    };

    run("BEGIN".to_string()).await;
    bench_hooks::reset_tuples_examined();
    for i in 1..=20 {
        run(format!("DELETE FROM t WHERE id = {i}")).await;
    }
    let examined = bench_hooks::tuples_examined();
    run("COMMIT".to_string()).await;

    assert!(
        examined <= BOUND * 20,
        "20 point deletes in one transaction examined {examined} tuples of {ROWS}; \
         each statement is re-reading the table"
    );
    let res = exec(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(ROWS as i64 - 20));
}

/// The boundary, stated rather than left to be discovered: a NON-unique
/// secondary index does not shorten a write. The fast path keys off a
/// single-column PRIMARY KEY or UNIQUE constraint (`extract_pk_eq_value`), so
/// `DELETE ... WHERE v = 'x'` still scans even with an index on `v`.
///
/// This is a deliberate scope line, not an oversight: widening the extractor
/// changes which statements take the pre-filtered path, and a non-unique index
/// can match a large fraction of the table, where a scan is the better plan
/// anyway. The test exists so the boundary moves on purpose.
#[tokio::test]
async fn a_non_unique_secondary_index_does_not_shorten_a_write_yet() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;
    exec(&ex, "CREATE INDEX t_v_idx ON t (v)").await;

    bench_hooks::reset_tuples_examined();
    exec(&ex, "DELETE FROM t WHERE v = 'v1500'").await;
    assert!(
        bench_hooks::tuples_examined() >= ROWS as u64,
        "a non-unique secondary index now serves writes — good, but this test \
         records the old boundary and must be updated deliberately"
    );
}

/// Inside a transaction the index cannot be trusted — it does not know this
/// session's buffered writes — so the statement must still find the row, by
/// scanning. Correctness first; the cost is the fallback's.
#[tokio::test]
async fn a_write_inside_a_transaction_still_finds_its_row() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;
    let sid = ex.create_session();
    let run = |sql: &'static str| {
        let ex = &ex;
        async move { ex.execute_with_session(sid, sql).await.expect(sql) }
    };

    run("BEGIN").await;
    // A row inserted in this transaction has no index entry at all.
    run("INSERT INTO t VALUES (99999, 'fresh')").await;
    run("UPDATE t SET v = 'updated' WHERE id = 99999").await;
    run("DELETE FROM t WHERE id = 1000").await;
    let res = ex
        .execute_with_session(sid, "SELECT v FROM t WHERE id = 99999")
        .await
        .unwrap();
    assert_eq!(
        rows(&res[0])[0][0],
        Value::Text("updated".into()),
        "an UPDATE inside a transaction did not see the row this transaction inserted"
    );
    run("COMMIT").await;

    let res = exec(&ex, "SELECT v FROM t WHERE id = 99999").await;
    assert_eq!(rows(&res[0])[0][0], Value::Text("updated".into()));
    let res = exec(&ex, "SELECT COUNT(*) FROM t WHERE id = 1000").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));
}

/// The index proposes; the predicate decides. A stale or alike-encoding index
/// entry must not delete a row the predicate excludes.
#[tokio::test]
async fn a_write_rechecks_the_predicate_against_the_row() {
    let dir = tempfile::tempdir().unwrap();
    let ex = seeded(dir.path()).await;

    // Change the key of a row, then delete by the OLD key: nothing may match.
    exec(&ex, "UPDATE t SET id = 900001 WHERE id = 900").await;
    exec(&ex, "DELETE FROM t WHERE id = 900").await;
    let res = exec(&ex, "SELECT COUNT(*) FROM t WHERE id = 900001").await;
    assert_eq!(
        scalar(&res[0]),
        &Value::Int64(1),
        "deleting by the old key removed the row that had moved to a new one"
    );
    let res = exec(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(ROWS as i64));
}

/// A restart leaves the catalog naming indexes the engine no longer has, and a
/// write must neither fail nor scan forever because of it.
///
/// Measured: after reopening a data directory,
/// `index_lookup(t, "t_pkey", …)` answers `index 't_pkey' not found` — B-tree
/// indexes are built by `create_index` at DDL time and live only in the
/// engine's in-memory registry, which nothing rebuilds at startup. The first
/// write to rediscover that rebuilds the index once and proceeds; the second
/// pays nothing.
#[tokio::test]
async fn a_write_after_restart_rebuilds_the_index_it_needs() {
    use crate::storage::persistence::CatalogPersistence;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().to_path_buf();
    async fn boot(data: &std::path::Path) -> Arc<Executor> {
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let catalog_path = data.join("catalog.json");
        let _ = CatalogPersistence::new(&catalog_path)
            .load_catalog(&catalog)
            .await;
        let engine = Arc::new(DiskEngine::open(&data.join("r.db"), catalog.clone()).unwrap());
        for table in catalog.table_names().await {
            let _ = engine.create_table(&table).await;
        }
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        let ex = Arc::new(Executor::new_with_persistence(
            catalog,
            buffered as Arc<dyn crate::storage::StorageEngine>,
            Some(catalog_path),
            Some(data),
        ));
        ex.restore_table_engines().await;
        ex
    }

    {
        let ex = boot(&data).await;
        exec(&ex, "CREATE TABLE r (id INT PRIMARY KEY, v TEXT)").await;
        for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(200) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
            exec(&ex, &format!("INSERT INTO r VALUES {}", values.join(", "))).await;
        }
    }

    let ex = boot(&data).await;
    // The engine really has lost it — if this ever starts succeeding, the
    // rebuild path below is dead code and should go.
    assert!(
        ex.storage_for("r")
            .index_lookup("r", "r_pkey", &Value::Int32(1))
            .await
            .is_err(),
        "the engine kept its index across a restart; this test's premise is gone"
    );

    // First write after the restart: correct, and it repairs the index.
    exec(&ex, "DELETE FROM r WHERE id = 1").await;
    let res = exec(&ex, "SELECT COUNT(*) FROM r WHERE id = 1").await;
    assert_eq!(scalar(&res[0]), &Value::Int64(0));

    // Second write: the index is there now, so it must not scan.
    bench_hooks::reset_tuples_examined();
    exec(&ex, "DELETE FROM r WHERE id = 2").await;
    let examined = bench_hooks::tuples_examined();
    assert!(
        examined <= BOUND,
        "the write after the rebuild examined {examined} tuples of {ROWS}; the \
         index was not repaired, only worked around"
    );
}

/// The size of the prize, measured rather than asserted.
///
/// Ignored by default: it is a timing, and a timing in a correctness suite is
/// a flake waiting to happen. Run it with
/// `cargo test --lib --features server pk_write_cost_measurement -- --ignored --nocapture`.
///
/// Both arms run in one process against the same data, switched by
/// `bench_hooks::set_skip_index_dml`, so the comparison is not across builds.
/// The scan arm is what the engine did before, and is still correct — only
/// slower.
#[tokio::test]
#[ignore = "timing measurement, not a gate"]
async fn pk_write_cost_measurement() {
    use std::time::Instant;

    // What the startup index rebuild costs, since it is now paid on every boot.
    for rows in [10_000i32, 100_000] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let engine = Arc::new(DiskEngine::open(&dir.path().join("r.db"), catalog.clone()).unwrap());
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        let ex = Arc::new(Executor::new(
            catalog,
            buffered as Arc<dyn crate::storage::StorageEngine>,
        ));
        exec(&ex, "CREATE TABLE r (id INT PRIMARY KEY, v TEXT)").await;
        for chunk in (1..=rows).collect::<Vec<_>>().chunks(500) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
            exec(&ex, &format!("INSERT INTO r VALUES {}", values.join(", "))).await;
        }
        let t = Instant::now();
        ex.rebuild_persistent_indexes().await;
        println!(
            "startup index rebuild: {rows} rows, 1 index, {:.2}s",
            t.elapsed().as_secs_f64()
        );
    }

    for rows in [5_000i32, 20_000] {
        let dir = tempfile::tempdir().unwrap();
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let engine = Arc::new(DiskEngine::open(&dir.path().join("m.db"), catalog.clone()).unwrap());
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        let ex = Arc::new(Executor::new(
            catalog,
            buffered as Arc<dyn crate::storage::StorageEngine>,
        ));
        exec(&ex, "CREATE TABLE m (id INT PRIMARY KEY, v TEXT)").await;
        for chunk in (1..=rows).collect::<Vec<_>>().chunks(500) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
            exec(&ex, &format!("INSERT INTO m VALUES {}", values.join(", "))).await;
        }

        let deletes = 100i32;
        // Autocommit and in-transaction are reported separately because they
        // measure different things: autocommit pays an fsync per statement, so
        // the scan hides under the drive barrier, while a transaction pays one
        // fsync for the batch and the scan is the whole cost. The original
        // attribution measured the second.
        for (mode, in_txn) in [("autocommit", false), ("one transaction", true)] {
            let mut timing = Vec::new();
            for (label, skip) in [("scan", true), ("index", false)] {
                crate::bench_hooks::set_skip_index_dml(skip);
                let sid = ex.create_session();
                let base = match (skip, in_txn) {
                    (true, false) => 1,
                    (false, false) => rows / 4,
                    (true, true) => rows / 2,
                    (false, true) => rows / 4 * 3,
                };
                if in_txn {
                    ex.execute_with_session(sid, "BEGIN").await.unwrap();
                }
                let start = Instant::now();
                for i in 0..deletes {
                    ex.execute_with_session(sid, &format!("DELETE FROM m WHERE id = {}", base + i))
                        .await
                        .expect("delete");
                }
                let per = start.elapsed().as_secs_f64() * 1e6 / deletes as f64;
                if in_txn {
                    ex.execute_with_session(sid, "COMMIT").await.unwrap();
                }
                timing.push((label, per));
            }
            crate::bench_hooks::set_skip_index_dml(false);
            println!(
                "{rows} rows, {mode}: {} {:.0} us/delete, {} {:.0} us/delete, {:.1}x",
                timing[0].0,
                timing[0].1,
                timing[1].0,
                timing[1].1,
                timing[0].1 / timing[1].1
            );
        }
    }
}

/// A restart must leave the READ path indexed too, not only the write path.
///
/// The write path repairs an index it finds missing (see above). The read path
/// cannot: it resolves index NAMES through the executor's `btree_indexes` map,
/// which is built at DDL time and empty after a restart, and then swallows a
/// lookup error with `.ok().flatten()` and scans. So a restarted database
/// answered every indexed query with a full scan, silently and permanently.
///
/// `rebuild_persistent_indexes` at startup is what closes that, and this is the
/// test that says so: after a restart THROUGH THAT PATH, a point read examines
/// no more than a point read should.
#[tokio::test]
async fn a_read_after_restart_is_still_indexed() {
    use crate::storage::persistence::CatalogPersistence;

    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().to_path_buf();
    async fn boot(data: &std::path::Path, rebuild: bool) -> Arc<Executor> {
        let catalog = Arc::new(crate::catalog::Catalog::new());
        let catalog_path = data.join("catalog.json");
        let _ = CatalogPersistence::new(&catalog_path)
            .load_catalog(&catalog)
            .await;
        let engine = Arc::new(DiskEngine::open(&data.join("q.db"), catalog.clone()).unwrap());
        for table in catalog.table_names().await {
            let _ = engine.create_table(&table).await;
        }
        let buffered = Arc::new(BufferedDiskEngine::new(engine));
        let ex = Arc::new(Executor::new_with_persistence(
            catalog,
            buffered as Arc<dyn crate::storage::StorageEngine>,
            Some(catalog_path),
            Some(data),
        ));
        ex.restore_table_engines().await;
        if rebuild {
            ex.rebuild_persistent_indexes().await;
        }
        ex
    }

    {
        let ex = boot(&data, false).await;
        exec(&ex, "CREATE TABLE q (id INT PRIMARY KEY, v TEXT)").await;
        for chunk in (1..=ROWS).collect::<Vec<_>>().chunks(200) {
            let values: Vec<String> = chunk.iter().map(|i| format!("({i}, 'v{i}')")).collect();
            exec(&ex, &format!("INSERT INTO q VALUES {}", values.join(", "))).await;
        }
    }

    // Control: a restart WITHOUT the rebuild scans, which is what every
    // restarted database did. If this ever stops scanning, the rebuild is no
    // longer what makes the assertion below pass.
    {
        let ex = boot(&data, false).await;
        bench_hooks::reset_tuples_examined();
        let res = exec(&ex, "SELECT v FROM q WHERE id = 1000").await;
        assert_eq!(rows(&res[0]).len(), 1);
        assert!(
            bench_hooks::tuples_examined() >= ROWS as u64,
            "without the startup rebuild the read should scan the table"
        );
    }

    let ex = boot(&data, true).await;
    bench_hooks::reset_tuples_examined();
    let res = exec(&ex, "SELECT v FROM q WHERE id = 1000").await;
    assert_eq!(rows(&res[0]).len(), 1, "the row must still be found");
    let examined = bench_hooks::tuples_examined();
    assert!(
        examined <= BOUND,
        "after a restart with the index rebuild, a point read examined {examined} \
         tuples of {ROWS} — the read path is still falling back to a scan"
    );
}
