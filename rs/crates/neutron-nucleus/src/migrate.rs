//! SQL migration runner.
//!
//! Scans a directory for `*.sql` files, applies them in lexicographic order,
//! and records each completed migration in a `__nucleus_migrations` table.
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

use std::path::Path;

use crate::error::NucleusError;
use crate::pool::NucleusPool;

/// Apply all pending migrations from `dir` to the pool.
///
/// Creates `__nucleus_migrations` if it does not exist.
/// Skips files that are already recorded as applied.
pub async fn migrate(pool: &NucleusPool, dir: impl AsRef<Path>) -> Result<(), NucleusError> {
    let conn = pool.get().await?;
    let client = conn.client();

    // Ensure the migrations tracking table exists.
    client
        .execute(
            "CREATE TABLE IF NOT EXISTS __nucleus_migrations (
                 name       TEXT        NOT NULL PRIMARY KEY,
                 applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
             )",
            &[],
        )
        .await
        .map_err(NucleusError::Query)?;

    // Collect already-applied names.
    let applied: std::collections::HashSet<String> = client
        .query("SELECT name FROM __nucleus_migrations ORDER BY name", &[])
        .await
        .map_err(NucleusError::Query)?
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

        client
            .execute("BEGIN", &[])
            .await
            .map_err(NucleusError::Query)?;

        let result = async {
            client
                .batch_execute(&sql)
                .await
                .map_err(NucleusError::Query)?;

            client
                .execute(
                    "INSERT INTO __nucleus_migrations (name) VALUES ($1)",
                    &[&name],
                )
                .await
                .map_err(NucleusError::Query)?;

            client
                .execute("COMMIT", &[])
                .await
                .map_err(NucleusError::Query)?;

            Ok::<_, NucleusError>(())
        }
        .await;

        if let Err(e) = result {
            let _ = client.execute("ROLLBACK", &[]).await;
            return Err(NucleusError::Migration {
                step:   name,
                source: Box::new(e),
            });
        }
    }

    Ok(())
}

fn read_sql_files(dir: &Path) -> Result<Vec<(String, String)>, NucleusError> {
    let mut out = Vec::new();

    let entries = std::fs::read_dir(dir).map_err(NucleusError::Io)?;

    for entry in entries {
        let entry = entry.map_err(NucleusError::Io)?;
        let path  = entry.path();

        let is_sql = path.extension().and_then(|e| e.to_str()) == Some("sql");
        if !is_sql {
            continue;
        }

        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();

        let sql = std::fs::read_to_string(&path).map_err(NucleusError::Io)?;
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
        fs::write(dir.path().join("readme.md"), "ignored").unwrap();

        let files = read_sql_files(dir.path()).unwrap();
        assert_eq!(files.len(), 2);

        let mut names: Vec<_> = files.iter().map(|(n, _)| n.as_str()).collect();
        names.sort();
        assert_eq!(names, ["001_a.sql", "002_b.sql"]);
    }

    #[test]
    fn read_sql_files_empty_dir() {
        let dir = tempdir();
        let files = read_sql_files(dir.path()).unwrap();
        assert!(files.is_empty());
    }

    /// Minimal tempdir helper — avoids pulling in the `tempfile` crate here.
    fn tempdir() -> TempDir {
        let path = std::env::temp_dir().join(format!(
            "neutron-nucleus-test-{}",
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
