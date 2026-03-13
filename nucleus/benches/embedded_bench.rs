//! Embedded API benchmarks — KV store, embedded SQL point queries, and FTS search.
//!
//! Run with: cargo bench --bench embedded_bench
//!
//! These benchmarks exercise the high-level embedded API surface: the KvStore,
//! the embedded Database (MVCC mode), and the InvertedIndex full-text search.

use criterion::{black_box, criterion_group, criterion_main, Criterion};

use nucleus::kv::KvStore;
use nucleus::fts::InvertedIndex;
use nucleus::types::Value;

fn kv_get_1000_keys(c: &mut Criterion) {
    // Create a KvStore, insert 1000 keys, bench get throughput
    let store = KvStore::new();
    for i in 0..1000u32 {
        store.set(
            &format!("key_{i}"),
            Value::Text(format!("val_{i}")),
            None,
        );
    }
    c.bench_function("kv_get_1000_keys", |b| {
        b.iter(|| {
            for i in 0..1000u32 {
                let _ = black_box(store.get(black_box(&format!("key_{i}"))));
            }
        })
    });
}

fn embedded_point_query(c: &mut Criterion) {
    // Create embedded MVCC db, create table, insert 1000 rows, bench point query
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = nucleus::embedded::Database::mvcc();
    rt.block_on(async {
        db.execute("CREATE TABLE bench_t (id INT NOT NULL, val TEXT)")
            .await
            .unwrap();
        for i in 0..1000 {
            db.execute(&format!("INSERT INTO bench_t VALUES ({i}, 'value_{i}')"))
                .await
                .unwrap();
        }
    });
    c.bench_function("embedded_point_query", |b| {
        b.iter(|| {
            rt.block_on(async {
                let _ = black_box(
                    db.query(black_box("SELECT * FROM bench_t WHERE id = 500"))
                        .await,
                );
            })
        })
    });
}

fn fts_search_single_term(c: &mut Criterion) {
    // Create InvertedIndex, add documents, bench single-term search
    let mut idx = InvertedIndex::new();
    for i in 0..500u32 {
        idx.add_document(
            i as u64,
            &format!("the quick brown fox jumps over lazy dog document {i}"),
        );
    }
    c.bench_function("fts_search_single_term", |b| {
        b.iter(|| {
            let _ = black_box(idx.search(black_box("fox"), 10));
        })
    });
}

criterion_group!(benches, kv_get_1000_keys, embedded_point_query, fts_search_single_term);
criterion_main!(benches);
