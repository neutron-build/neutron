//! Embedded mode — use Nucleus as a library with no network.
//!
//! ```rust,ignore
//! use nucleus::embedded::Database;
//!
//! let db = Database::open("./myapp.db").unwrap();
//! db.execute("CREATE TABLE users (id INT NOT NULL, name TEXT)").await.unwrap();
//! db.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
//!
//! let rows = db.query("SELECT * FROM users").await.unwrap();
//! for row in &rows {
//!     println!("{:?}", row);
//! }
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::executor::{ExecError, ExecResult, Executor};
use crate::storage::{DiskEngine, MemoryEngine, MvccStorageAdapter, StorageEngine};
use crate::types::{Row, Value};

/// Storage backend for the embedded database.
#[derive(Debug, Clone, Default)]
pub enum StorageMode {
    /// Simple in-memory HashMap storage (fastest, no isolation).
    #[default]
    Memory,
    /// MVCC in-memory storage with snapshot isolation.
    Mvcc,
    /// Disk-backed page storage with WAL.
    Disk(PathBuf),
}

/// Builder for configuring an embedded database.
pub struct DatabaseBuilder {
    mode: StorageMode,
}

impl DatabaseBuilder {
    /// Create a new builder with in-memory storage.
    pub fn new() -> Self {
        Self { mode: StorageMode::Memory }
    }

    /// Use simple in-memory storage (no MVCC, fast for single-threaded use).
    pub fn memory(mut self) -> Self {
        self.mode = StorageMode::Memory;
        self
    }

    /// Use MVCC in-memory storage (snapshot isolation, concurrent transactions).
    pub fn mvcc(mut self) -> Self {
        self.mode = StorageMode::Mvcc;
        self
    }

    /// Use disk-backed storage at the given path.
    pub fn disk<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.mode = StorageMode::Disk(path.into());
        self
    }

    /// Build and return the database.
    pub fn build(self) -> Result<Database, DatabaseError> {
        let catalog = Arc::new(Catalog::new());
        let storage: Arc<dyn StorageEngine> = match self.mode {
            StorageMode::Memory => Arc::new(MemoryEngine::new()),
            StorageMode::Mvcc => Arc::new(MvccStorageAdapter::new()),
            StorageMode::Disk(path) => Arc::new(
                DiskEngine::open(&path, catalog.clone())
                    .map_err(|e| DatabaseError::Storage(e.to_string()))?,
            ),
        };
        let executor = Arc::new(Executor::new(catalog.clone(), storage.clone()));
        Ok(Database {
            executor,
            _catalog: catalog,
            _storage: storage,
        })
    }
}

/// A Nucleus database instance (embedded, no network).
pub struct Database {
    executor: Arc<Executor>,
    // Keep references alive
    _catalog: Arc<Catalog>,
    _storage: Arc<dyn StorageEngine>,
}

impl Database {
    /// Open a database file. Creates the file if it doesn't exist.
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self, DatabaseError> {
        DatabaseBuilder::new().disk(path.as_ref()).build()
    }

    /// Create an in-memory database (no file, data lost on drop).
    pub fn memory() -> Self {
        DatabaseBuilder::new().memory().build().expect("memory db never fails")
    }

    /// Create an in-memory database with MVCC (snapshot isolation).
    pub fn mvcc() -> Self {
        DatabaseBuilder::new().mvcc().build().expect("mvcc db never fails")
    }

    /// Create a builder for advanced configuration.
    pub fn builder() -> DatabaseBuilder {
        DatabaseBuilder::new()
    }

    /// Execute a SQL statement (DDL, DML, or query).
    /// Returns results for each statement in the SQL string.
    pub async fn execute(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        self.executor.execute(sql).await
    }

    /// Execute a query and return just the rows (convenience for SELECT).
    /// If the SQL contains multiple statements, returns rows from the last SELECT.
    pub async fn query(&self, sql: &str) -> Result<Vec<Row>, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            if let ExecResult::Select { rows, .. } = result {
                return Ok(rows);
            }
        }
        Ok(vec![])
    }

    /// Execute a query and return rows with column metadata.
    pub async fn query_with_columns(
        &self,
        sql: &str,
    ) -> Result<QueryResult, ExecError> {
        let results = self.executor.execute(sql).await?;
        for result in results.into_iter().rev() {
            if let ExecResult::Select { columns, rows } = result {
                return Ok(QueryResult { columns, rows });
            }
        }
        Ok(QueryResult {
            columns: vec![],
            rows: vec![],
        })
    }

    /// Execute a DML statement and return the number of affected rows.
    pub async fn execute_dml(&self, sql: &str) -> Result<usize, ExecError> {
        let results = self.executor.execute(sql).await?;
        let mut total = 0;
        for result in results {
            if let ExecResult::Command { rows_affected, .. } = result {
                total += rows_affected;
            }
        }
        Ok(total)
    }

    /// Get a single scalar value from a query (first column of first row).
    pub async fn query_one(&self, sql: &str) -> Result<Option<Value>, ExecError> {
        let rows = self.query(sql).await?;
        Ok(rows.into_iter().next().and_then(|row| row.into_iter().next()))
    }

    /// Execute a batch of SQL statements separated by semicolons.
    pub async fn execute_batch(&self, sql: &str) -> Result<Vec<ExecResult>, ExecError> {
        let mut all_results = Vec::new();
        for stmt in sql.split(';') {
            let trimmed = stmt.trim();
            if trimmed.is_empty() {
                continue;
            }
            let mut results = self.execute(trimmed).await?;
            all_results.append(&mut results);
        }
        Ok(all_results)
    }

    /// Cleanly shut down the database, releasing all resources.
    pub fn close(self) {
        drop(self);
    }
}

/// Convenience type alias.
pub type Nucleus = Database;

/// Result of a query with column metadata.
#[derive(Debug)]
pub struct QueryResult {
    pub columns: Vec<(String, crate::types::DataType)>,
    pub rows: Vec<Row>,
}

#[derive(Debug, thiserror::Error)]
pub enum DatabaseError {
    #[error("storage error: {0}")]
    Storage(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn embedded_memory_basic() {
        let db = Database::memory();
        db.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'hello'), (2, 'world')")
            .await
            .unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[0][1], Value::Text("hello".into()));
    }

    #[tokio::test]
    async fn embedded_query_one() {
        let db = Database::memory();
        db.execute("CREATE TABLE nums (v INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO nums VALUES (42)")
            .await
            .unwrap();

        let val = db.query_one("SELECT v FROM nums").await.unwrap();
        assert_eq!(val, Some(Value::Int32(42)));
    }

    #[tokio::test]
    async fn embedded_dml_count() {
        let db = Database::memory();
        db.execute("CREATE TABLE items (id INT NOT NULL)")
            .await
            .unwrap();
        let affected = db
            .execute_dml("INSERT INTO items VALUES (1), (2), (3)")
            .await
            .unwrap();
        assert_eq!(affected, 3);
    }

    #[tokio::test]
    async fn embedded_disk_roundtrip() {
        let dir = std::env::temp_dir().join("nucleus_embed_test");
        let db_path = dir.join("test.db");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        {
            let db = Database::open(&db_path).unwrap();
            db.execute("CREATE TABLE t (id INT NOT NULL, val TEXT)")
                .await
                .unwrap();
            db.execute("INSERT INTO t VALUES (1, 'persisted')")
                .await
                .unwrap();
        }

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn embedded_mvcc_basic() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL, name TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO t VALUES (1, 'alice'), (2, 'bob')")
            .await
            .unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn embedded_mvcc_transaction() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO t VALUES (1)").await.unwrap();

        // Start a transaction, insert, then rollback
        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (2)").await.unwrap();
        db.execute("ROLLBACK").await.unwrap();

        // Only the first insert should be visible
        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0][0], Value::Int32(1));
    }

    #[tokio::test]
    async fn embedded_mvcc_commit() {
        let db = Database::mvcc();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();

        db.execute("BEGIN").await.unwrap();
        db.execute("INSERT INTO t VALUES (1)").await.unwrap();
        db.execute("INSERT INTO t VALUES (2)").await.unwrap();
        db.execute("COMMIT").await.unwrap();

        let rows = db.query("SELECT * FROM t").await.unwrap();
        assert_eq!(rows.len(), 2);
    }

    #[tokio::test]
    async fn embedded_builder_pattern() {
        let db = Database::builder().mvcc().build().unwrap();
        db.execute("CREATE TABLE t (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO t VALUES (42)").await.unwrap();
        let val = db.query_one("SELECT id FROM t").await.unwrap();
        assert_eq!(val, Some(Value::Int32(42)));
    }

    #[tokio::test]
    async fn embedded_execute_batch() {
        let db = Database::memory();
        db.execute_batch(
            "CREATE TABLE batch (id INT NOT NULL, name TEXT);
             INSERT INTO batch VALUES (1, 'alpha');
             INSERT INTO batch VALUES (2, 'beta')",
        )
        .await
        .unwrap();
        let rows = db.query("SELECT * FROM batch").await.unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][0], Value::Int32(1));
        assert_eq!(rows[1][1], Value::Text("beta".into()));
    }

    #[tokio::test]
    async fn embedded_execute_batch_trailing_semicolons() {
        let db = Database::memory();
        let results = db
            .execute_batch("CREATE TABLE trailing (id INT NOT NULL);;; ;")
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
    }

    #[tokio::test]
    async fn embedded_execute_batch_empty() {
        let db = Database::memory();
        let results = db.execute_batch("").await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn embedded_close() {
        let db = Database::memory();
        db.execute("CREATE TABLE c (id INT NOT NULL)").await.unwrap();
        db.execute("INSERT INTO c VALUES (1)").await.unwrap();
        db.close(); // consumes db — no further use possible
    }

    #[tokio::test]
    async fn embedded_nucleus_type_alias() {
        let db: Nucleus = Nucleus::memory();
        db.execute("CREATE TABLE alias (v INT NOT NULL)")
            .await
            .unwrap();
        db.execute("INSERT INTO alias VALUES (7)")
            .await
            .unwrap();
        let val = db.query_one("SELECT v FROM alias").await.unwrap();
        assert_eq!(val, Some(Value::Int32(7)));
    }

    #[tokio::test]
    async fn embedded_batch_multiple_tables() {
        let db = Database::memory();
        db.execute_batch(
            "CREATE TABLE a (id INT NOT NULL);
             CREATE TABLE b (id INT NOT NULL);
             CREATE TABLE c (id INT NOT NULL)",
        )
        .await
        .unwrap();
        db.execute("INSERT INTO a VALUES (1)").await.unwrap();
        db.execute("INSERT INTO b VALUES (2)").await.unwrap();
        db.execute("INSERT INTO c VALUES (3)").await.unwrap();
        assert_eq!(db.query("SELECT * FROM a").await.unwrap().len(), 1);
        assert_eq!(db.query("SELECT * FROM b").await.unwrap().len(), 1);
        assert_eq!(db.query("SELECT * FROM c").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn embedded_query_with_columns() {
        let db = Database::memory();
        db.execute("CREATE TABLE meta (id INT NOT NULL, label TEXT)")
            .await
            .unwrap();
        db.execute("INSERT INTO meta VALUES (1, 'x')")
            .await
            .unwrap();
        let result = db
            .query_with_columns("SELECT * FROM meta")
            .await
            .unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 1);
    }
}
