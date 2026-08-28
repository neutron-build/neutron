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
