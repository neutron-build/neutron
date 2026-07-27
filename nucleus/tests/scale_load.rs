//! Large-scale load test with explicit engine selection.
//!
//! ## Which engine is measured
//!
//! This test used to construct `MvccStorageAdapter` — a `RwLock<Vec<MvccRow>>`
//! that holds every row in RAM and whose secondary indexes clone rows. The
//! server runs `BufferedDiskEngine` over the paged `DiskEngine`. Row counts and
//! timings taken from the RAM adapter describe a database nobody deploys, so the
//! default here is now the server's engine, chosen explicitly and printed beside
//! every number.
//!
//! Select an engine with `NUCLEUS_SCALE_ENGINE` (`buffered-disk` default, also
//! `disk`, `durable-mvcc`, `mvcc`, `memory`) and a row count with
//! `NUCLEUS_SCALE_ROWS`. The RAM engines stay reachable so they can be measured
//! deliberately as a comparison point.
//!
//! ```sh
//! # server engine, 1M rows
//! cargo test --release --features server --test scale_load -- --ignored --nocapture
//! # deliberate RAM-engine comparison
//! NUCLEUS_SCALE_ENGINE=mvcc NUCLEUS_SCALE_ROWS=200000 \
//!   cargo test --release --features server --test scale_load -- --ignored --nocapture
//! ```
//!
//! Reported per phase: wall time, rows/s, and p50/p95/p99 of the individual
//! statements, plus disk footprint, WAL bytes, write amplification, buffer-pool
//! hit rate, checkpoint cost, and recovery time.

#![cfg(feature = "server")]

use std::time::Instant;

use nucleus::metrics::harness::{EngineConfig, EngineKind, HarnessDb, MachineInfo};
use nucleus::metrics::latency::LatencyRecorder;
use nucleus::types::{DataType, Value};

fn i64v(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        Value::Float64(f) => *f as i64,
        other => panic!("not int-like: {other:?}"),
    }
}

fn env_engine() -> EngineKind {
    match std::env::var("NUCLEUS_SCALE_ENGINE") {
        Ok(v) => EngineKind::parse(&v).unwrap_or_else(|| {
            panic!(
                "NUCLEUS_SCALE_ENGINE='{v}' is not a known engine; expected one of {:?}",
                EngineKind::ALL.map(|k| k.name())
            )
        }),
        Err(_) => EngineKind::BufferedDisk,
    }
}

fn env_rows(default: i64) -> i64 {
    std::env::var("NUCLEUS_SCALE_ROWS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn scratch_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "nucleus_scale_{tag}_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// Load `rows` rows, verify every aggregate exactly, and report timings against
/// the named engine. Shared by the scale run and the fast smoke run so the
/// harness cannot rot between real scale runs.
async fn run_scale(engine: EngineKind, rows: i64, batch: i64, tag: &str) {
    let dir = scratch_dir(tag);
    let config = EngineConfig::default();
    let machine = MachineInfo::detect();
    let db = HarnessDb::open(engine, &dir, config)
        .await
        .expect("open harness engine");

    eprintln!("\n---- scale_load ----");
    eprintln!("machine : {}", machine.describe());
    eprintln!("{}", db.provenance());
    if !engine.is_server_default() {
        eprintln!("NOTE    : not the engine `nucleus serve` runs (that is 'buffered-disk')");
    }
    if engine.is_ram_resident() {
        eprintln!("NOTE    : rows are RAM-resident on this engine; capacity describes RAM");
    }
    eprintln!("rows    : {rows} (batch {batch})");

    db.execute("CREATE TABLE big (id BIGINT, bucket INT, amt BIGINT)")
        .await
        .unwrap();

    let before = db.snapshot();
    let insert_lat = LatencyRecorder::with_reservoir(100_000);
    let mut expected_sum: i128 = 0;
    let mut logical_bytes: u64 = 0;
    let col_types = [DataType::Int64, DataType::Int32, DataType::Int64];

    let t0 = Instant::now();
    let mut id = 0i64;
    while id < rows {
        let n = batch.min(rows - id);
        let mut sql = String::with_capacity(n as usize * 24);
        sql.push_str("INSERT INTO big (id, bucket, amt) VALUES ");
        for j in 0..n {
            let cur = id + j;
            let amt = (cur % 1000) + 1;
            expected_sum += amt as i128;
            logical_bytes += nucleus::storage::tuple::serialize_row(
                &vec![
                    Value::Int64(cur),
                    Value::Int32((cur % 10) as i32),
                    Value::Int64(amt),
                ],
                &col_types,
            )
            .len() as u64;
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({cur},{},{amt})", cur % 10));
        }
        let t = Instant::now();
        db.execute(&sql).await.unwrap();
        insert_lat.record(t.elapsed());
        id += n;
    }
    let insert_secs = t0.elapsed().as_secs_f64();
    let ins = insert_lat.summary();
    eprintln!(
        "INSERT  : {rows} rows in {insert_secs:.1}s = {:.0} rows/s; per-batch {}",
        rows as f64 / insert_secs,
        ins.fmt_ms()
    );

    // Each read phase is timed individually and asserted exactly — a fast wrong
    // answer is not a result.
    let t = Instant::now();
    let cnt = i64v(
        &db.query_one("SELECT COUNT(*) FROM big")
            .await
            .unwrap()
            .unwrap(),
    );
    eprintln!("COUNT(*): {cnt} in {:.3}s", t.elapsed().as_secs_f64());
    assert_eq!(cnt, rows, "COUNT must see all rows");

    let t = Instant::now();
    let sum = i64v(
        &db.query_one("SELECT SUM(amt) FROM big")
            .await
            .unwrap()
            .unwrap(),
    );
    eprintln!("SUM(amt): {sum} in {:.3}s", t.elapsed().as_secs_f64());
    assert_eq!(sum as i128, expected_sum, "SUM must be exact at scale");

    let t = Instant::now();
    let gc = i64v(
        &db.query_one("SELECT COUNT(*) FROM big WHERE bucket = 3")
            .await
            .unwrap()
            .unwrap(),
    );
    eprintln!(
        "FILTER  : COUNT WHERE bucket=3 = {gc} in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(gc, rows / 10, "1/10 of rows have bucket=3");

    // Point lookup on a row that exists at every tested scale.
    let probe_id = rows - 1;
    let t = Instant::now();
    let pt = i64v(
        &db.query_one(&format!("SELECT amt FROM big WHERE id = {probe_id}"))
            .await
            .unwrap()
            .unwrap(),
    );
    eprintln!(
        "POINT   : id={probe_id} -> amt={pt} in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(pt, (probe_id % 1000) + 1);

    let t = Instant::now();
    let groups = db
        .query("SELECT bucket, COUNT(*) FROM big GROUP BY bucket")
        .await
        .unwrap()
        .len();
    eprintln!(
        "GROUP BY: {groups} groups in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(groups, 10, "10 distinct buckets");

    // ---- resource accounting the M11 ledger names ----
    let checkpoint = db.checkpoint();
    let after = db.snapshot();
    let delta = after.delta(&before);
    eprintln!("\nresources");
    eprintln!(
        "  RSS                  : {} MB",
        after.rss_bytes / (1024 * 1024)
    );
    if engine.is_durable() {
        eprintln!(
            "  disk footprint       : {:.1} MB",
            after.disk_bytes as f64 / (1024.0 * 1024.0)
        );
    }
    if engine.has_buffer_pool() {
        eprintln!(
            "  WAL written / syncs  : {:.1} MB / {}",
            delta.wal_bytes as f64 / (1024.0 * 1024.0),
            delta.wal_syncs
        );
        let physical = delta.wal_bytes + delta.disk_bytes;
        if logical_bytes > 0 && physical > 0 {
            eprintln!(
                "  write amplification  : {:.2}x ({:.1} MB physical / {:.1} MB logical)",
                physical as f64 / logical_bytes as f64,
                physical as f64 / (1024.0 * 1024.0),
                logical_bytes as f64 / (1024.0 * 1024.0)
            );
        }
        match after.cache_hit_rate() {
            Some(hr) => eprintln!(
                "  buffer pool hit rate : {:.2}% ({} hits / {} misses, {} evictions)",
                hr * 100.0,
                delta.cache_hits,
                delta.cache_misses,
                delta.evictions
            ),
            None => eprintln!("  buffer pool hit rate : n/a (no page reads)"),
        }
        match &checkpoint {
            Some(Ok(cp)) => {
                eprintln!(
                    "  checkpoint cost      : {:.1} ms",
                    cp.as_secs_f64() * 1000.0
                )
            }
            Some(Err(e)) => panic!("checkpoint failed: {e}"),
            None => {}
        }
    } else {
        assert!(
            checkpoint.is_none(),
            "{engine} has no buffer pool and must not report a checkpoint"
        );
        eprintln!("  (no buffer pool / WAL page accounting on this engine)");
    }

    // ---- recovery: reopen and re-verify, so the numbers describe a database
    // that still holds its data ----
    db.close();
    if engine.is_durable() {
        let db2 = HarnessDb::open(engine, &dir, config)
            .await
            .expect("reopen after close");
        eprintln!(
            "  recovery time        : {:.1} ms (reopen of the populated directory)",
            db2.open_elapsed().as_secs_f64() * 1000.0
        );
        let cnt2 = i64v(
            &db2.query_one("SELECT COUNT(*) FROM big")
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(cnt2, rows, "every committed row must survive reopen");
        let sum2 = i64v(
            &db2.query_one("SELECT SUM(amt) FROM big")
                .await
                .unwrap()
                .unwrap(),
        );
        assert_eq!(
            sum2 as i128, expected_sum,
            "SUM must be exact after recovery"
        );
        eprintln!("  durability           : {cnt2} rows and exact SUM after reopen");
        db2.close();
    }

    let _ = std::fs::remove_dir_all(&dir);
    eprintln!(
        "SCALE PASSED — {rows} rows on '{}', all aggregates exact.\n",
        engine.name()
    );
}

/// Fast guard so the scale harness cannot silently rot between real runs. Small
/// enough for the default test suite; it proves the code path, not capacity.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scale_harness_smoke_on_server_engine() {
    run_scale(EngineKind::BufferedDisk, 2_000, 500, "smoke").await;
}

/// The real scale run. Defaults to the server engine and 1M rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale test: 1M rows by default; run explicitly"]
async fn scale_rows_on_selected_engine() {
    let engine = env_engine();
    let rows = env_rows(1_000_000);
    let batch = std::env::var("NUCLEUS_SCALE_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1_000);
    run_scale(engine, rows, batch, "rows").await;
}

/// Deliberate side-by-side: the same workload on the RAM-resident adapter the
/// old harness measured by accident. Kept so the comparison stays available and
/// clearly labelled, never as the default.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale test: RAM-engine comparison; run explicitly"]
async fn scale_ram_engine_comparison() {
    let rows = env_rows(200_000);
    run_scale(EngineKind::Mvcc, rows, 1_000, "ram").await;
}
