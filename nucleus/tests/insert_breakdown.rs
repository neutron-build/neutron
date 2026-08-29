// Single-INSERT latency breakdown: same statement across sync modes, and
// with catalog/stats persistence observable. Runs against a real server the
// way compete does (pgwire), one row at a time.
#![cfg(feature = "server")]

use std::time::Instant;

#[tokio::test]
async fn single_insert_latency_by_sync_mode() {
    use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb};
    use nucleus::storage::wal::SyncMode;

    for mode in [
        SyncMode::Fsync,
        SyncMode::Fdatasync,
        SyncMode::FlushOs,
        SyncMode::None,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let cfg = EngineConfig {
            sync_mode: mode,
            ..Default::default()
        };
        let db = HarnessDb::open(EngineKind::BufferedDisk, dir.path(), cfg)
            .await
            .unwrap();
        db.execute("CREATE TABLE ins (id BIGINT PRIMARY KEY, v TEXT)")
            .await
            .unwrap();

        // Warm.
        for i in 0..20 {
            db.execute(&format!("INSERT INTO ins VALUES ({i}, 'x{i}')"))
                .await
                .unwrap();
        }

        let t0 = Instant::now();
        let n = 50;
        for i in 20..20 + n {
            db.execute(&format!("INSERT INTO ins VALUES ({i}, 'x{i}')"))
                .await
                .unwrap();
        }
        let per = t0.elapsed().as_micros() as f64 / n as f64;
        println!("{mode:?}: {per:.0} us/insert");
        if mode == SyncMode::None {
            // No durability barrier is configured, so a single-row INSERT
            // must cost microseconds of real work, not milliseconds of
            // someone else's fsync. The profile (2026-08-27) had 91% of
            // every insert inside CdcWal::group_sync — the per-statement
            // CDC barrier that fire-and-forget (NU-107) never asked for.
            assert!(
                per < 500.0,
                "sync=None single insert took {per:.0} us — a durability barrier is on the hot path"
            );
        }
        let _ = db.checkpoint();
        db.close();
    }
}

/// A late-created index must not disable the unique-key probe (2026-08-27).
/// `create_index` on a non-empty table used to `mark_mutated` it —
/// permanently routing every INSERT's uniqueness check through a
/// full-table O(n) scan. Measured: 4.0-4.8ms per insert on a 250K-row
/// indexed table vs ~150us with the probe; the entire "SQLite is ~4000x
/// faster at INSERT" finding was this defect. The gate: insert rate must
/// not collapse once a secondary index exists on a loaded table.
#[tokio::test]
async fn late_index_does_not_disable_the_unique_probe() {
    use nucleus::catalog::Catalog;
    use nucleus::executor::Executor;
    use nucleus::storage::MvccStorageAdapter;
    use std::sync::Arc;

    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn nucleus::storage::StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));
    ex.execute("CREATE TABLE li (id BIGINT PRIMARY KEY, cat TEXT, amt FLOAT)")
        .await
        .unwrap();

    // Load 50K rows in chunks (the bulk path).
    let chunk = 500;
    let mut base = 1i64;
    while base <= 50_000 {
        let end = (base + chunk - 1).min(50_000);
        let vals: Vec<String> = (base..=end)
            .map(|i| format!("({i},'c{}',1.0)", i % 100))
            .collect();
        ex.execute(&format!("INSERT INTO li VALUES {}", vals.join(",")))
            .await
            .unwrap();
        base = end + 1;
    }
    // The index under test: created AFTER the load, on a non-key column.
    ex.execute("CREATE INDEX li_cat ON li(cat)").await.unwrap();

    // Warm, then timed single inserts.
    for i in 0..20 {
        ex.execute(&format!(
            "INSERT INTO li VALUES ({}00000, 'c1', 2.0)",
            90 + i
        ))
        .await
        .unwrap();
    }
    let t0 = std::time::Instant::now();
    let n = 500;
    for i in 0..n {
        ex.execute(&format!(
            "INSERT INTO li VALUES ({}00000, 'c2', 3.0)",
            200 + i
        ))
        .await
        .unwrap();
    }
    let per_us = t0.elapsed().as_micros() as f64 / n as f64;
    println!("late-index insert: {per_us:.0} us/insert");
    // Pre-fix: ~1000+ us (50K-row scan per insert). Post-fix: ~30 us.
    // 200us separates with margin both ways and tolerates CI noise.
    assert!(
        per_us < 200.0,
        "insert latency collapsed after CREATE INDEX on a loaded table: \
         {per_us:.0} us/insert — the unique probe is disabled again"
    );

    // And the index still answers.
    // The index still answers: 500 bulk-load rows carry cat='c2' (i % 100)
    // plus the n rows just inserted.
    let r = ex
        .execute("SELECT COUNT(*) FROM li WHERE cat = 'c2'")
        .await
        .unwrap();
    let nucleus::executor::ExecResult::Select { rows, .. } = &r[0] else {
        panic!("expected rows");
    };
    assert_eq!(rows[0][0], nucleus::types::Value::Int64(500 + n));
}
