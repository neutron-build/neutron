//! Large-scale load test: 1M+ rows. Run explicitly:
//!   cargo test --release --features server --test scale_load -- --ignored --nocapture

use std::sync::Arc;
use std::time::Instant;

use nucleus::catalog::Catalog;
use nucleus::executor::{ExecResult, Executor};
use nucleus::storage::{MvccStorageAdapter, StorageEngine};
use nucleus::types::Value;

fn i64v(v: &Value) -> i64 {
    match v {
        Value::Int64(n) => *n,
        Value::Int32(n) => *n as i64,
        Value::Float64(f) => *f as i64,
        other => panic!("not int-like: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "scale test: 1M rows; run explicitly"]
async fn scale_one_million_rows() {
    let catalog = Arc::new(Catalog::new());
    let storage: Arc<dyn StorageEngine> = Arc::new(MvccStorageAdapter::new());
    let ex = Arc::new(Executor::new(catalog, storage));

    ex.execute("CREATE TABLE big (id BIGINT, bucket INT, amt BIGINT)")
        .await
        .unwrap();

    const N: i64 = 1_000_000;
    const BATCH: i64 = 1000;
    let mut expected_sum: i128 = 0;

    let t0 = Instant::now();
    let mut id = 0i64;
    while id < N {
        // Multi-row INSERT in batches of 1000.
        let mut sql = String::from("INSERT INTO big (id, bucket, amt) VALUES ");
        for j in 0..BATCH {
            let cur = id + j;
            let amt = (cur % 1000) + 1;
            expected_sum += amt as i128;
            if j > 0 {
                sql.push(',');
            }
            sql.push_str(&format!("({cur},{},{amt})", cur % 10));
        }
        ex.execute(&sql).await.unwrap();
        id += BATCH;
    }
    let insert_secs = t0.elapsed().as_secs_f64();
    eprintln!(
        "INSERT {N} rows in {insert_secs:.1}s = {:.0} rows/s",
        N as f64 / insert_secs
    );

    // COUNT(*) — correctness + timing
    let t = Instant::now();
    let cnt = match ex
        .execute("SELECT COUNT(*) FROM big")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => i64v(&rows[0][0]),
        o => panic!("{o:?}"),
    };
    eprintln!("COUNT(*) = {cnt} in {:.3}s", t.elapsed().as_secs_f64());
    assert_eq!(cnt, N, "COUNT must see all rows");

    // SUM(amt) — correctness vs computed expectation
    let t = Instant::now();
    let sum = match ex
        .execute("SELECT SUM(amt) FROM big")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => i64v(&rows[0][0]),
        o => panic!("{o:?}"),
    };
    eprintln!("SUM(amt) = {sum} in {:.3}s", t.elapsed().as_secs_f64());
    assert_eq!(sum as i128, expected_sum, "SUM must be exact at scale");

    // Filtered aggregate
    let t = Instant::now();
    let gc = match ex
        .execute("SELECT COUNT(*) FROM big WHERE bucket = 3")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => i64v(&rows[0][0]),
        o => panic!("{o:?}"),
    };
    eprintln!(
        "COUNT WHERE bucket=3 = {gc} in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(gc, N / 10, "1/10 of rows have bucket=3");

    // Point lookup
    let t = Instant::now();
    let pt = match ex
        .execute("SELECT amt FROM big WHERE id = 987654")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => i64v(&rows[0][0]),
        o => panic!("{o:?}"),
    };
    eprintln!(
        "point lookup id=987654 -> amt={pt} in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(pt, (987654 % 1000) + 1);

    // GROUP BY
    let t = Instant::now();
    let groups = match ex
        .execute("SELECT bucket, COUNT(*) FROM big GROUP BY bucket")
        .await
        .unwrap()
        .pop()
        .unwrap()
    {
        ExecResult::Select { rows, .. } => rows.len(),
        o => panic!("{o:?}"),
    };
    eprintln!(
        "GROUP BY bucket -> {groups} groups in {:.3}s",
        t.elapsed().as_secs_f64()
    );
    assert_eq!(groups, 10, "10 distinct buckets");

    eprintln!("SCALE TEST PASSED — 1M rows, all aggregates exact.");
}
