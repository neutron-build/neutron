//! SQL migration runner for PostgreSQL.
//!
//! Scans a directory for `*.sql` files, applies them in lexicographic order,
//! and records each completed migration in a `__pg_migrations` table.
//!
//! # File naming
//!
//! Name migration files with a zero-padded numeric prefix so they sort
//! correctly:
//! ```text
//! migrations/
//!   001_create_users.sql
//!   002_add_email_index.sql
//!   003_create_posts.sql
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! use neutron_postgres::{PgPool, PgConfig, migrate};
//!
//! let pool = PgPool::new(PgConfig::from_url("postgres://localhost/myapp"));
//! migrate(&pool, "migrations/").await?;
//! ```

use std::path::Path;

use crate::error::PgError;
use crate::pool::PgPool;

/// Apply all pending migrations from `dir` to the pool.
///
/// Creates `__pg_migrations` if it does not exist.
/// Skips files that are already recorded as applied.
/// Each migration runs in its own transaction — a failure rolls back that
/// step and returns an error, leaving previously applied migrations intact.
pub async fn migrate(pool: &PgPool, dir: impl AsRef<Path>) -> Result<(), PgError> {
    let conn   = pool.get().await?;
    let client = conn.client();

    // Ensure the migrations tracking table exists.
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS __pg_migrations (
                 name       TEXT        NOT NULL PRIMARY KEY,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             )",
            &[],
        )
        .await
        .map_err(PgError::Query)?;

    // Collect already-applied names.
    let applied: std::collections::HashSet<String> = client
        .query("SELECT name FROM __pg_migrations ORDER BY name", &[])
        .await
        .map_err(PgError::Query)?
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();

    // Read and sort migration files.
    let mut files = read_sql_files(dir.as_ref())?;
    files.sort_by(|a, b| a.0.cmp(&b.0));

    for (name, sql) in files {
        if applied.contains(&name) {
            tracing::debug!(migration = %name, "already applied — skipping");
            continue;
        }

        tracing::info!(migration = %name, "applying migration");

        client.execute("BEGIN", &[]).await.map_err(PgError::Query)?;

        let result = async {
            client.batch_execute(&sql).await.map_err(PgError::Query)?;
            client
                .execute("INSERT INTO __pg_migrations (name) VALUES ($1)", &[&name])
                .await
                .map_err(PgError::Query)?;
            client.execute("COMMIT", &[]).await.map_err(PgError::Query)?;
            Ok::<_, PgError>(())
        }
        .await;

        if let Err(e) = result {
            let _ = client.execute("ROLLBACK", &[]).await;
            return Err(PgError::Migration {
                step:   name,
                source: Box::new(e),
            });
        }
    }

    Ok(())
}

fn read_sql_files(dir: &Path) -> Result<Vec<(String, String)>, PgError> {
    let mut out = Vec::new();

    let entries = std::fs::read_dir(dir).map_err(PgError::Io)?;

    for entry in entries {
        let entry = entry.map_err(PgError::Io)?;
        let path  = entry.path();

        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }

        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        let sql = std::fs::read_to_string(&path).map_err(PgError::Io)?;
        out.push((name, sql));
    }

    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn read_sql_files_sorted_and_filtered() {
        let dir = tempdir();
        fs::write(dir.path().join("002_b.sql"), "SELECT 2").unwrap();
        fs::write(dir.path().join("001_a.sql"), "SELECT 1").unwrap();
        fs::write(dir.path().join("readme.md"), "-- ignored").unwrap();

        let mut files = read_sql_files(dir.path()).unwrap();
        files.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0, "001_a.sql");
        assert_eq!(files[0].1, "SELECT 1");
        assert_eq!(files[1].0, "002_b.sql");
        assert_eq!(files[1].1, "SELECT 2");
    }

    #[test]
    fn read_sql_files_empty_dir() {
        let dir = tempdir();
        let files = read_sql_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    #[test]
    fn read_sql_files_nonexistent_dir() {
        let result = read_sql_files(Path::new("/nonexistent/path/xyz"));
        assert!(result.is_err());
        matches!(result.unwrap_err(), PgError::Io(_));
    }

    /// Minimal tempdir helper.
    fn tempdir() -> TempDir {
        let path = std::env::temp_dir().join(format!(
            "neutron-pg-migrate-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .subsec_nanos()
        ));
        fs::create_dir_all(&path).unwrap();
        TempDir { path }
    }

    struct TempDir { path: std::path::PathBuf }
    impl TempDir { fn path(&self) -> &Path { &self.path } }
    impl Drop for TempDir { fn drop(&mut self) { let _ = fs::remove_dir_all(&self.path); } }
}
