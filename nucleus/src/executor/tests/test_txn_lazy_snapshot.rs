//! R8 — opening a transaction must not cost the whole database.
//!
//! On a non-MVCC engine, `BEGIN` used to scan and clone EVERY table so that
//! `ROLLBACK` had something to restore from. That is O(whole database) to open
//! a transaction, paid in full by a transaction that touches one row — and
//! paid again by every `SAVEPOINT`. Before-images are now captured lazily, at
//! the transaction's first write to each table.
//!
//! The assertions here COUNT SCANS rather than time anything. A timing bound
//! would have to survive both debug and release and the parallel test suite,
//! and picking one that discriminates in all three is guesswork; the number of
//! times the engine is asked to read a table it was never going to need is
//! exact, and it is the thing that actually went wrong.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use super::super::Executor;
use super::exec;
use crate::catalog::Catalog;
use crate::storage::{MemoryEngine, StorageEngine, StorageError};
use crate::types::Row;

/// A `MemoryEngine` that records which tables get scanned.
///
/// Only the six required trait methods are delegated; everything else keeps
/// the trait default, which is fine here because the default derives from
/// `scan`/`insert` and this engine is only driven by the simple SQL below.
struct CountingEngine {
    inner: MemoryEngine,
    scans: parking_lot::Mutex<Vec<String>>,
    total: AtomicUsize,
}

impl CountingEngine {
    fn new() -> Self {
        Self {
            inner: MemoryEngine::new(),
            scans: parking_lot::Mutex::new(Vec::new()),
            total: AtomicUsize::new(0),
        }
    }
    fn record(&self, table: &str) {
        self.total.fetch_add(1, Ordering::Relaxed);
        self.scans.lock().push(table.to_string());
    }
    fn scans_of(&self, table: &str) -> usize {
        self.scans.lock().iter().filter(|t| *t == table).count()
    }
    fn reset(&self) {
        self.scans.lock().clear();
        self.total.store(0, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl StorageEngine for CountingEngine {
    async fn create_table(&self, table: &str) -> Result<(), StorageError> {
        self.inner.create_table(table).await
    }
    async fn drop_table(&self, table: &str) -> Result<(), StorageError> {
        self.inner.drop_table(table).await
    }
    async fn insert(&self, table: &str, row: Row) -> Result<(), StorageError> {
        self.inner.insert(table, row).await
    }
    async fn scan(&self, table: &str) -> Result<Vec<Row>, StorageError> {
        self.record(table);
        self.inner.scan(table).await
    }
    async fn delete(&self, table: &str, positions: &[usize]) -> Result<usize, StorageError> {
        self.inner.delete(table, positions).await
    }
    async fn update(&self, table: &str, updates: &[(usize, Row)]) -> Result<usize, StorageError> {
        self.inner.update(table, updates).await
    }
}

async fn setup() -> (Executor, Arc<CountingEngine>) {
    let catalog = Arc::new(Catalog::new());
    let engine = Arc::new(CountingEngine::new());
    let storage: Arc<dyn StorageEngine> = engine.clone();
    let ex = Executor::new(catalog, storage);
    exec(&ex, "CREATE TABLE hot (id INT, v INT)").await;
    exec(&ex, "CREATE TABLE cold (id INT, v INT)").await;
    exec(&ex, "INSERT INTO hot VALUES (1, 1)").await;
    exec(&ex, "INSERT INTO cold VALUES (1, 1)").await;
    (ex, engine)
}

#[tokio::test]
async fn begin_does_not_scan_any_table() {
    let (ex, engine) = setup().await;
    engine.reset();
    exec(&ex, "BEGIN").await;
    assert_eq!(
        engine.total.load(Ordering::Relaxed),
        0,
        "BEGIN scanned {} table(s) before the transaction touched anything: {:?}",
        engine.total.load(Ordering::Relaxed),
        engine.scans.lock()
    );
    exec(&ex, "ROLLBACK").await;
}

#[tokio::test]
async fn a_transaction_never_reads_a_table_it_does_not_touch() {
    let (ex, engine) = setup().await;
    engine.reset();

    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE hot SET v = 2 WHERE id = 1").await;
    exec(&ex, "ROLLBACK").await;

    assert_eq!(
        engine.scans_of("cold"),
        0,
        "a transaction that only touched `hot` read `cold` {} time(s) — the \
         whole-database snapshot is back",
        engine.scans_of("cold")
    );
    // And the rollback still worked, which is the point of capturing at all.
    let res = exec(&ex, "SELECT v FROM hot WHERE id = 1").await;
    match &res[0] {
        crate::executor::ExecResult::Select { rows, .. } => {
            assert_eq!(rows[0][0], crate::types::Value::Int32(1), "rollback lost the before-image")
        }
        other => panic!("expected Select, got {other:?}"),
    }
}

#[tokio::test]
async fn savepoint_does_not_scan_untouched_tables() {
    let (ex, engine) = setup().await;
    engine.reset();

    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE hot SET v = 2 WHERE id = 1").await;
    exec(&ex, "SAVEPOINT sp1").await;
    exec(&ex, "SAVEPOINT sp2").await;
    exec(&ex, "SAVEPOINT sp3").await;

    assert_eq!(
        engine.scans_of("cold"),
        0,
        "SAVEPOINT cloned the untouched table — three savepoints meant three \
         copies of the whole database"
    );
    exec(&ex, "ROLLBACK").await;
}

/// The lazy capture happens exactly once per table per transaction, not once
/// per write. Otherwise a write-heavy transaction would re-read the table on
/// every statement, which is a worse problem than the one being fixed.
#[tokio::test]
async fn the_before_image_is_captured_once_not_per_write() {
    let (ex, engine) = setup().await;
    engine.reset();

    exec(&ex, "BEGIN").await;
    for i in 2..12 {
        exec(&ex, &format!("INSERT INTO hot VALUES ({i}, {i})")).await;
    }
    let hot_scans = engine.scans_of("hot");
    exec(&ex, "ROLLBACK").await;

    // Ten writes. The before-image accounts for ONE of those reads; the rest
    // are the executor's own (constraint checks, index maintenance). What must
    // not happen is the count growing with a second before-image per write, so
    // allow the executor's reads but require far fewer than one capture each.
    assert!(
        hot_scans < 10,
        "the before-image looks like it is being retaken per write: {hot_scans} \
         scans of `hot` across 10 inserts"
    );
}

/// Rolling back to a savepoint that was never taken must be an error, not a
/// silent success that leaves the transaction's writes in place. The non-MVCC
/// path used to get this from the legacy savepoint map, which is gone.
#[tokio::test]
async fn rollback_to_an_unknown_savepoint_is_an_error() {
    let (ex, _engine) = setup().await;
    exec(&ex, "BEGIN").await;
    exec(&ex, "UPDATE hot SET v = 2 WHERE id = 1").await;
    let r = ex.execute("ROLLBACK TO SAVEPOINT nope").await;
    assert!(
        r.is_err(),
        "ROLLBACK TO SAVEPOINT on a name that was never taken must fail"
    );
    exec(&ex, "ROLLBACK").await;
}
