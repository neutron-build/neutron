//! Storage layer benchmarks — buffer pool, WAL, slotted page, and B-tree index.
//!
//! Run with: cargo bench --bench storage_bench
//!
//! These benchmarks measure the raw I/O and data structure performance that
//! underpins all query execution.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tempfile::TempDir;

use nucleus::storage::buffer::BufferPool;
use nucleus::storage::btree::{BTreeIndex, RowId, value_to_key};
use nucleus::storage::disk::DiskManager;
use nucleus::storage::page;
use nucleus::storage::wal::Wal;
use nucleus::types::{DataType, Value};

/// Create a DiskManager + BufferPool backed by a temp file, then write
/// `num_pages` pages so there is data to fetch. Returns the pool and
/// temp dir (kept alive so the files are not deleted).
fn setup_pool(pool_size: usize, num_pages: u32) -> (Arc<BufferPool>, TempDir) {
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("bench.db");
    let disk = DiskManager::open(&db_path).unwrap();
    let pool = Arc::new(BufferPool::new(disk, None, pool_size, 0));

    // Allocate and write pages so there is data on disk to fetch later.
    for _ in 0..num_pages {
        let (page_id, frame_id) = pool.new_page().unwrap();
        // Write some recognisable data into the page.
        let data = pool.frame_data_mut(frame_id);
        page::init_data_page(data, 1);
        page::write_checksum(data);
        pool.mark_dirty(frame_id);
        pool.unpin(frame_id);
        // Flush to disk so subsequent fetches actually read from disk.
        pool.flush_page(page_id).unwrap();
    }

    (pool, tmp)
}

// ============================================================================
// Buffer pool benchmarks
// ============================================================================

fn buffer_pool_fetch(c: &mut Criterion) {
    // Large pool (1024 frames) with 100 pages — most fetches will be cache hits
    // after the first pass, measuring hot-path performance.
    let (pool, _tmp) = setup_pool(1024, 100);

    c.bench_function("buffer_pool_fetch_100_pages", |b| {
        b.iter(|| {
            for page_id in 0..100u32 {
                let frame_id = pool.fetch_page(black_box(page_id)).unwrap();
                black_box(pool.frame_data(frame_id));
                pool.unpin(frame_id);
            }
        });
    });
}

fn buffer_pool_eviction(c: &mut Criterion) {
    // Small pool (32 frames) with 100 pages — every fetch beyond the pool size
    // triggers eviction, measuring the LRU-K replacement path.
    let (pool, _tmp) = setup_pool(32, 100);

    c.bench_function("buffer_pool_eviction_32_frames_100_pages", |b| {
        b.iter(|| {
            for page_id in 0..100u32 {
                let frame_id = pool.fetch_page(black_box(page_id)).unwrap();
                black_box(pool.frame_data(frame_id));
                pool.unpin(frame_id);
            }
        });
    });
}

// ============================================================================
// WAL benchmarks
// ============================================================================

fn wal_write_throughput(c: &mut Criterion) {
    c.bench_function("wal_write_1000_page_records", |b| {
        b.iter_with_setup(
            || {
                let tmp = TempDir::new().unwrap();
                let wal_path = tmp.path().join("bench.wal");
                let wal = Wal::open(&wal_path).unwrap();
                (wal, tmp)
            },
            |(wal, _tmp)| {
                let page_buf: page::PageBuf = [0xABu8; page::PAGE_SIZE];
                for i in 0..1000u32 {
                    let _ = black_box(wal.log_page_write(1, i, &page_buf).unwrap());
                }
                wal.sync().unwrap();
            },
        );
    });
}

// ============================================================================
// Slotted page benchmarks
// ============================================================================

fn page_slotted_insert(c: &mut Criterion) {
    c.bench_function("page_slotted_insert_tuples", |b| {
        b.iter(|| {
            let mut pg: page::PageBuf = [0u8; page::PAGE_SIZE];
            page::init_data_page(&mut pg, 1);

            // Insert small tuples (64 bytes each) until the page is full.
            let tuple = [0x42u8; 64];
            let mut count = 0u32;
            while page::insert_tuple(&mut pg, &tuple).is_some() {
                count += 1;
            }
            black_box(count);
        });
    });
}

// ============================================================================
// B-tree benchmarks
// ============================================================================

fn btree_lookup(c: &mut Criterion) {
    // Build a B-tree with 10 000 integer keys and then benchmark point lookups.
    let tmp = TempDir::new().unwrap();
    let db_path = tmp.path().join("btree_bench.db");
    let disk = DiskManager::open(&db_path).unwrap();
    // Large pool so the tree stays in memory.
    let pool = Arc::new(BufferPool::new(disk, None, 4096, 0));

    let mut index = BTreeIndex::create(pool.clone(), DataType::Int32).unwrap();

    for i in 0..10_000i32 {
        let key = value_to_key(&Value::Int32(i));
        let row_id = RowId { page_id: (i as u32) / 100, slot_idx: (i as u16) % 100 };
        index.insert(&key, row_id).unwrap();
    }

    c.bench_function("btree_point_lookup_10k_keys", |b| {
        let mut i = 0i32;
        b.iter(|| {
            let key = value_to_key(&Value::Int32(i % 10_000));
            let result = index.lookup(black_box(&key)).unwrap();
            black_box(result);
            i += 1;
        });
    });
}


// ============================================================================
// Additional page benchmarks
// ============================================================================

fn page_slotted_read(c: &mut Criterion) {
    // Pre-fill a page with tuples, then benchmark reading them all.
    let mut pg: page::PageBuf = [0u8; page::PAGE_SIZE];
    page::init_data_page(&mut pg, 1);

    let tuple = [0x42u8; 64];
    let mut slot_count = 0u16;
    while page::insert_tuple(&mut pg, &tuple).is_some() {
        slot_count += 1;
    }

    c.bench_function("page_slotted_read_all_tuples", |b| {
        b.iter(|| {
            for slot in 0..slot_count {
                let data = page::read_tuple(black_box(&pg), slot);
                black_box(data);
            }
        });
    });
}

fn page_checksum_roundtrip(c: &mut Criterion) {
    c.bench_function("page_checksum_write_verify", |b| {
        b.iter(|| {
            let mut pg: page::PageBuf = [0xABu8; page::PAGE_SIZE];
            page::init_data_page(&mut pg, 1);
            page::write_checksum(&mut pg);
            let ok = page::verify_checksum(black_box(&pg));
            black_box(ok);
        });
    });
}

fn buffer_pool_new_page(c: &mut Criterion) {
    c.bench_function("buffer_pool_allocate_100_pages", |b| {
        b.iter_with_setup(
            || {
                // Reset: create a fresh pool for each iteration to avoid accumulation
                let tmp2 = TempDir::new().unwrap();
                let db_path = tmp2.path().join("alloc_bench.db");
                let disk = DiskManager::open(&db_path).unwrap();
                let pool = Arc::new(BufferPool::new(disk, None, 1024, 0));
                (pool, tmp2)
            },
            |(pool, _tmp2)| {
                for _ in 0..100 {
                    let (page_id, frame_id) = pool.new_page().unwrap();
                    pool.unpin(frame_id);
                    black_box(page_id);
                }
            },
        );
    });
}

criterion_group!(
    storage_benches,
    buffer_pool_fetch,
    buffer_pool_eviction,
    wal_write_throughput,
    page_slotted_insert,
    page_slotted_read,
    page_checksum_roundtrip,
    buffer_pool_new_page,
    btree_lookup,
);
criterion_main!(storage_benches);
