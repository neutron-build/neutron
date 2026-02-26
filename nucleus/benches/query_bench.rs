//! Query execution benchmarks — measure performance of common SQL patterns.
//!
//! Run with: cargo bench
//!
//! These benchmarks help ensure Nucleus achieves its performance goals:
//! - Sub-millisecond point queries (KV workload)
//! - 3x VPS density vs traditional Postgres
//! - Linear scaling with cores

use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId};
use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::MemoryEngine;
use std::sync::Arc;
use tokio::runtime::Runtime;

/// Set up a test executor with sample data.
fn setup_executor() -> Arc<Executor> {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let catalog = Arc::new(Catalog::new());
        let storage = Arc::new(MemoryEngine::new());
        let executor = Arc::new(Executor::new(catalog, storage));

        // Create test tables
        exec(&executor, "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, email TEXT, age INT, active BOOLEAN)").await;
        exec(&executor, "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount FLOAT, created_at BIGINT)").await;
        exec(&executor, "CREATE TABLE products (id INT PRIMARY KEY, name TEXT, price FLOAT, category TEXT)").await;

        // Insert sample data
        for i in 0..1000 {
            exec(&executor, &format!(
                "INSERT INTO users VALUES ({}, 'User{}', 'user{}@example.com', {}, {})",
                i, i, i, 20 + (i % 50), i % 2 == 0
            )).await;
        }

        for i in 0..5000 {
            exec(&executor, &format!(
                "INSERT INTO orders VALUES ({}, {}, {}, {})",
                i, i % 1000, 10.0 + (i as f64 * 0.5), 1704067200 + i * 60
            )).await;
        }

        for i in 0..200 {
            exec(&executor, &format!(
                "INSERT INTO products VALUES ({}, 'Product{}', {}, '{}')",
                i, i, 9.99 + (i as f64), if i % 3 == 0 { "Electronics" } else if i % 3 == 1 { "Books" } else { "Clothing" }
            )).await;
        }

        executor
    })
}

async fn exec(executor: &Executor, sql: &str) {
    let _ = executor.execute(sql).await;
}

fn bench_point_query(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("point_query_by_pk", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box("SELECT * FROM users WHERE id = 500")).await;
            black_box(result)
        });
    });
}

fn bench_range_scan(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("range_scan_100_rows", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box("SELECT * FROM users WHERE id >= 400 AND id < 500")).await;
            black_box(result)
        });
    });
}

fn bench_aggregation(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("count_star", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box("SELECT COUNT(*) FROM orders")).await;
            black_box(result)
        });
    });

    c.bench_function("group_by_aggregate", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "SELECT user_id, COUNT(*), SUM(amount), AVG(amount) FROM orders GROUP BY user_id"
            )).await;
            black_box(result)
        });
    });
}

fn bench_join(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("hash_join_1000x5000", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 100"
            )).await;
            black_box(result)
        });
    });
}

fn bench_insert(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let mut group = c.benchmark_group("inserts");
    for batch_size in [1, 10, 100] {
        group.bench_with_input(BenchmarkId::from_parameter(batch_size), &batch_size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let executor = setup_executor();
                for i in 0..size {
                    let result = executor.execute(black_box(&format!(
                        "INSERT INTO users VALUES ({}, 'Test{}', 'test{}@example.com', 30, true)",
                        10000 + i, i, i
                    ))).await;
                    let _ = black_box(result);
                }
            });
        });
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("update_single_row", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "UPDATE users SET age = 25 WHERE id = 500"
            )).await;
            black_box(result)
        });
    });

    c.bench_function("update_batch_100_rows", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "UPDATE users SET active = false WHERE id >= 400 AND id < 500"
            )).await;
            black_box(result)
        });
    });
}

fn bench_transaction(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("transaction_with_5_writes", |b| {
        b.to_async(&rt).iter(|| async {
            let _ = executor.execute(black_box("BEGIN")).await;
            for i in 0..5 {
                let _ = executor.execute(black_box(&format!(
                    "INSERT INTO users VALUES ({}, 'TxUser{}', 'tx{}@example.com', 30, true)",
                    20000 + i, i, i
                ))).await;
            }
            let result = executor.execute(black_box("COMMIT")).await;
            black_box(result)
        });
    });
}

fn bench_delete_single_row(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("delete_single_row_by_pk", |b| {
        b.to_async(&rt).iter(|| async {
            // Fresh executor each iteration so there is always a row to delete
            let executor = setup_executor();
            let result = executor.execute(black_box(
                "DELETE FROM users WHERE id = 500"
            )).await;
            black_box(result)
        });
    });
}

fn bench_concurrent_inserts(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    c.bench_function("concurrent_inserts_100_rows", |b| {
        b.to_async(&rt).iter(|| async {
            let executor = setup_executor();
            for i in 0..100 {
                let result = executor.execute(black_box(&format!(
                    "INSERT INTO users VALUES ({}, 'Batch{}', 'batch{}@example.com', 25, true)",
                    50000 + i, i, i
                ))).await;
                let _ = black_box(result);
            }
        });
    });
}

fn bench_select_with_subquery(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("select_with_subquery", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE amount > 50)"
            )).await;
            black_box(result)
        });
    });
}

fn bench_cte_query(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("cte_query", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "WITH active_users AS (SELECT id, name, age FROM users WHERE active = true) \
                 SELECT * FROM active_users WHERE age > 30"
            )).await;
            black_box(result)
        });
    });
}

fn bench_multi_table_join(c: &mut Criterion) {
    let executor = setup_executor();
    let rt = Runtime::new().unwrap();

    c.bench_function("multi_table_join_3_tables", |b| {
        b.to_async(&rt).iter(|| async {
            let result = executor.execute(black_box(
                "SELECT u.name, o.amount, p.name \
                 FROM users u \
                 JOIN orders o ON u.id = o.user_id \
                 JOIN products p ON o.id = p.id \
                 WHERE o.amount > 100"
            )).await;
            black_box(result)
        });
    });
}

criterion_group!(
    benches,
    bench_point_query,
    bench_range_scan,
    bench_aggregation,
    bench_join,
    bench_insert,
    bench_update,
    bench_transaction,
    bench_delete_single_row,
    bench_concurrent_inserts,
    bench_select_with_subquery,
    bench_cte_query,
    bench_multi_table_join,
);
criterion_main!(benches);
