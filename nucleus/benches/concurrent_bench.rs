//! Concurrent access benchmarks — multi-threaded mixed read/write workloads.
//!
//! Run with: cargo bench --bench concurrent_bench
//!
//! These benchmarks measure how the embedded MVCC database handles concurrent
//! readers and writers across multiple Tokio tasks.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;

fn concurrent_4_threads_mixed(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = Arc::new(nucleus::embedded::Database::mvcc());
    rt.block_on(async {
        db.execute("CREATE TABLE conc_bench (id INT NOT NULL, val INT NOT NULL)")
            .await
            .unwrap();
        for i in 0..100 {
            db.execute(&format!("INSERT INTO conc_bench VALUES ({i}, {i})"))
                .await
                .unwrap();
        }
    });

    c.bench_function("concurrent_4_threads_mixed", |b| {
        b.iter(|| {
            rt.block_on(async {
                let mut handles = Vec::new();
                for t in 0..4u32 {
                    let db = db.clone();
                    handles.push(tokio::spawn(async move {
                        for i in 0..25u32 {
                            let id = 1000 + t * 100 + i;
                            let _ = black_box(
                                db.execute(black_box(&format!(
                                    "INSERT INTO conc_bench VALUES ({id}, {id})"
                                )))
                                .await,
                            );
                            let _ = black_box(
                                db.query(black_box("SELECT COUNT(*) FROM conc_bench"))
                                    .await,
                            );
                        }
                    }));
                }
                for h in handles {
                    let _ = h.await;
                }
            })
        })
    });
}

criterion_group!(benches, concurrent_4_threads_mixed);
criterion_main!(benches);
