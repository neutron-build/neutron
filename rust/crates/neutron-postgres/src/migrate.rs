//! Legacy, filename-only SQL migration runner (PostgreSQL only).
//!
//! Existing filename history is unverified and drift-blind: an applied filename
//! is skipped even if its SQL changes. New managed applications should use the
//! language-neutral CLI authority. This runner does not adopt CLI history.
//! SQL files are snapshotted before waiting; paired `.up.sql`/`.down.sql` files
//! are refused. Each pending file commits separately; prior commits survive a
//! later failure. An unknown commit outcome is reported without automatic retry.

use crate::{
    error::PgError,
    pool::{PgPool, PooledConn},
};
use std::{collections::HashSet, path::Path};
use tokio_postgres::Client;

const CLI_LOCK_KEY: i64 = 7043516000858342567;
const LEDGER: &str = "__pg_migrations";
const FOREIGN_LEDGER: &str = "__nucleus_migrations";

// Armed before any awaited admission/lock query. An uncertain session must
// never be recycled. PG driver shutdown may await an in-flight statement;
// cancellation does not prove immediate rollback or immediate lock release.
struct MigrationConnection {
    conn: PooledConn,
    reusable: bool,
}
impl Drop for MigrationConnection {
    fn drop(&mut self) {
        if !self.reusable {
            drop(self.conn.client.take());
        }
    }
}

struct Metadata {
    schema: String,
    schema_oid: u32,
    ledger_oid: Option<u32>,
    table: String,
}

fn refusal(message: impl Into<String>) -> PgError {
    PgError::Migration {
        step: "metadata admission".into(),
        source: Box::new(PgError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            message.into(),
        ))),
    }
}
fn quote_identifier(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

async fn capture_metadata(client: &Client) -> Result<Metadata, PgError> {
    let version: String = client
        .query_one("SELECT pg_catalog.version()", &[])
        .await
        .map_err(PgError::Query)?
        .try_get(0)
        .map_err(PgError::Query)?;
    if !version.starts_with("PostgreSQL ") || version.contains("Nucleus") {
        return Err(refusal("legacy filename migrations require PostgreSQL catalog/transaction semantics; provider unsupported"));
    }
    let row = client.query_opt("SELECT n.nspname::text,n.oid FROM pg_catalog.pg_namespace n WHERE n.nspname=pg_catalog.current_schema()", &[])
        .await.map_err(PgError::Query)?.ok_or_else(||refusal("no persistent application namespace"))?;
    let schema: String = row.try_get(0).map_err(PgError::Query)?;
    if schema == "information_schema" || schema.starts_with("pg_") {
        return Err(refusal(
            "migration namespace must be a persistent application schema",
        ));
    }
    let mut metadata = Metadata {
        table: format!("{}.{}", quote_identifier(&schema), quote_identifier(LEDGER)),
        schema,
        schema_oid: row.try_get(1).map_err(PgError::Query)?,
        ledger_oid: None,
    };
    admit_metadata(client, &mut metadata).await?;
    let visible: Option<u32> = client
        .query_one("SELECT pg_catalog.to_regclass($1)::oid", &[&LEDGER])
        .await
        .map_err(PgError::Query)?
        .try_get(0)
        .map_err(PgError::Query)?;
    if visible != metadata.ledger_oid {
        return Err(refusal(
            "filename history is shadowed by a temporary or later-search-path relation",
        ));
    }
    Ok(metadata)
}

async fn admit_metadata(client: &Client, metadata: &mut Metadata) -> Result<(), PgError> {
    let schema = client
        .query_opt(
            "SELECT oid FROM pg_catalog.pg_namespace WHERE nspname=$1",
            &[&metadata.schema],
        )
        .await
        .map_err(PgError::Query)?
        .ok_or_else(|| refusal("migration namespace disappeared"))?;
    if schema.try_get::<_, u32>(0).map_err(PgError::Query)? != metadata.schema_oid {
        return Err(refusal("migration namespace identity changed"));
    }
    let rows = client.query("SELECT c.relname::text,c.oid,c.relkind::text,c.relpersistence::text FROM pg_catalog.pg_class c WHERE c.relnamespace=$1 AND c.relname IN ($2,$3,$4)", &[&metadata.schema_oid,&LEDGER,&FOREIGN_LEDGER,&"_neutron_migrations"])
        .await.map_err(PgError::Query)?;
    let mut own = None;
    for row in rows {
        let name: String = row.try_get(0).map_err(PgError::Query)?;
        if name != LEDGER {
            return Err(refusal(format!("foreign migration authority {name} exists in the managed schema; no adoption/reset is performed")));
        }
        let kind: String = row.try_get(2).map_err(PgError::Query)?;
        let persistence: String = row.try_get(3).map_err(PgError::Query)?;
        if kind != "r" || persistence != "p" {
            return Err(refusal(
                "own filename history must be an ordinary permanent table",
            ));
        }
        own = Some(row.try_get::<_, u32>(1).map_err(PgError::Query)?);
    }
    if metadata.ledger_oid.is_some() && metadata.ledger_oid != own {
        return Err(refusal("filename history identity changed or disappeared"));
    }
    if let Some(oid) = own {
        let shape: bool = client.query_one(r#"SELECT
            (SELECT pg_catalog.count(*) FROM pg_catalog.pg_attribute a WHERE a.attrelid=$1 AND a.attnum>0 AND NOT a.attisdropped)=2
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
                WHERE a.attrelid=$1 AND a.attname='name' AND a.attnum>0 AND NOT a.attisdropped AND a.attnotnull
                AND t.oid=25 AND t.typname='text' AND t.typtype='b' AND t.typbasetype=0 AND n.nspname='pg_catalog'
                AND EXISTS (SELECT 1 FROM pg_catalog.pg_constraint p WHERE p.conrelid=$1 AND p.contype='p' AND p.conkey=ARRAY[a.attnum]))
            AND EXISTS (SELECT 1 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_type t ON t.oid=a.atttypid JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace
                WHERE a.attrelid=$1 AND a.attname='applied_at' AND a.attnum>0 AND NOT a.attisdropped AND a.attnotnull
                AND t.oid=1184 AND t.typname='timestamptz' AND t.typtype='b' AND t.typbasetype=0 AND n.nspname='pg_catalog')"#, &[&oid])
            .await.map_err(PgError::Query)?.try_get(0).map_err(PgError::Query)?;
        if !shape {
            return Err(refusal("filename history requires exactly native TEXT name primary key and native TIMESTAMPTZ applied_at; legacy contents remain unverified"));
        }
    }
    metadata.ledger_oid = own;
    Ok(())
}

/// Apply snapshotted pending files using legacy, unverified filename history.
/// PostgreSQL only; CLI/other Rust history is refused. Cancellation discards the
/// checked-out connection; it does not certify rollback of an in-flight commit.
pub async fn migrate(pool: &PgPool, dir: impl AsRef<Path>) -> Result<(), PgError> {
    let files = read_sql_files(dir.as_ref())?;
    let mut guard = MigrationConnection {
        conn: pool.get().await?,
        reusable: false,
    };
    let client = guard.conn.client();
    let mut metadata = capture_metadata(client).await?;
    client
        .query_one("SELECT pg_catalog.pg_advisory_lock($1)", &[&CLI_LOCK_KEY])
        .await
        .map_err(PgError::Query)?;
    admit_metadata(client, &mut metadata).await?;
    if metadata.ledger_oid.is_none() {
        client.batch_execute(&format!("CREATE TABLE {} (name pg_catalog.text NOT NULL PRIMARY KEY,applied_at pg_catalog.timestamptz NOT NULL DEFAULT pg_catalog.now())",metadata.table))
            .await.map_err(PgError::Query)?;
        admit_metadata(client, &mut metadata).await?;
    }
    let applied: HashSet<String> = client
        .query(
            &format!("SELECT name FROM {} ORDER BY name", metadata.table),
            &[],
        )
        .await
        .map_err(PgError::Query)?
        .into_iter()
        .map(|r| r.try_get::<_, String>(0).map_err(PgError::Query))
        .collect::<Result<_, _>>()?;
    for (name, sql) in files {
        if applied.contains(&name) {
            continue;
        }
        client
            .batch_execute("BEGIN")
            .await
            .map_err(PgError::Query)?;
        let result = async {
            client.batch_execute(&sql).await.map_err(PgError::Query)?;
            admit_metadata(client, &mut metadata).await?;
            client
                .execute(
                    &format!("INSERT INTO {} (name) VALUES ($1)", metadata.table),
                    &[&name],
                )
                .await
                .map_err(PgError::Query)?;
            Ok::<(), PgError>(())
        }
        .await;
        if let Err(error) = result {
            let _ = client.batch_execute("ROLLBACK").await;
            return Err(PgError::Migration {
                step: name,
                source: Box::new(error),
            });
        }
        if let Err(error) = client.batch_execute("COMMIT").await {
            return Err(PgError::Migration {
                step: format!("{name} (commit outcome indeterminate; no automatic retry)"),
                source: Box::new(PgError::Query(error)),
            });
        }
    }
    let unlocked: bool = client
        .query_one("SELECT pg_catalog.pg_advisory_unlock($1)", &[&CLI_LOCK_KEY])
        .await
        .map_err(PgError::Query)?
        .try_get(0)
        .map_err(PgError::Query)?;
    if !unlocked {
        return Err(refusal(
            "migration unlock unconfirmed; connection discarded",
        ));
    }
    guard.reusable = true;
    Ok(())
}

fn read_sql_files(dir: &Path) -> Result<Vec<(String, String)>, PgError> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(dir).map_err(PgError::Io)? {
        let entry = entry.map_err(PgError::Io)?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("sql") {
            continue;
        }
        if !entry.file_type().map_err(PgError::Io)?.is_file() {
            return Err(refusal(
                "migration SQL paths must be regular files, not symlinks/directories",
            ));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| refusal("migration filenames must be UTF-8"))?;
        if name.ends_with(".up.sql") || name.ends_with(".down.sql") {
            return Err(refusal("paired .up.sql/.down.sql migration naming is unsupported by the filename-only runner; use the CLI authority"));
        }
        let sql = std::fs::read_to_string(path).map_err(PgError::Io)?;
        files.push((name, sql));
    }
    files.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(files)
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

    #[test]
    fn immutable_file_capture_and_paired_refusal() {
        let dir = tempdir();
        let path = dir.path().join("001_plain.sql");
        fs::write(&path, "SELECT 1;").unwrap();
        let files = read_sql_files(dir.path()).unwrap();
        fs::write(&path, "SELECT 2;").unwrap();
        assert_eq!(files[0].1, "SELECT 1;");
        for name in ["002_step.up.sql", "002_step.down.sql"] {
            let paired = dir.path().join(name);
            fs::write(&paired, "SELECT 3;").unwrap();
            assert!(read_sql_files(dir.path()).is_err());
            fs::remove_file(paired).unwrap();
        }
    }

    #[test]
    fn sql_directory_and_invalid_utf8_refused() {
        let dir = tempdir();
        let nested = dir.path().join("nested.sql");
        fs::create_dir(&nested).unwrap();
        assert!(read_sql_files(dir.path()).is_err());
        fs::remove_dir(nested).unwrap();
        fs::write(dir.path().join("bad.sql"), [0xff]).unwrap();
        assert!(read_sql_files(dir.path()).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn sql_symlink_refused() {
        let dir = tempdir();
        let target = dir.path().join("target.txt");
        fs::write(&target, "SELECT 1;").unwrap();
        std::os::unix::fs::symlink(target, dir.path().join("link.sql")).unwrap();
        assert!(read_sql_files(dir.path()).is_err());
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

    struct TempDir {
        path: std::path::PathBuf,
    }
    impl TempDir {
        fn path(&self) -> &Path {
            &self.path
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
