//! M3: format-version rejection and full-state recovery.
//!
//! Two things the crash matrix cannot cover:
//!
//!  * **Format guards.** Opening a database written by a NEWER build must be
//!    refused, and refused *without touching the file*. A build that half-opens
//!    a future format and then writes to it corrupts data it never understood —
//!    which is worse than refusing to start.
//!  * **Full-state recovery.** The crash matrix asserts row state. A database
//!    is more than rows: catalog, sequences, views, RLS policies, and specialty
//!    indexes must all come back too, or a restart silently drops a security
//!    boundary.

use super::*;
use crate::catalog::Catalog;
use crate::storage::{DiskEngine, StorageEngine, page};

/// Content hash per file, so a test can prove a rejected open left the
/// database untouched — and report WHICH file changed rather than dumping
/// whole pages into the failure message.
fn dir_fingerprint(dir: &std::path::Path) -> Vec<(String, u64, String)> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&d) else {
            continue;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if let Ok(bytes) = std::fs::read(&p) {
                out.push((
                    p.strip_prefix(dir).unwrap().to_string_lossy().into_owned(),
                    bytes.len() as u64,
                    blake3::hash(&bytes).to_hex().to_string(),
                ));
            }
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// Human-readable description of what changed between two fingerprints.
fn describe_changes(
    before: &[(String, u64, String)],
    after: &[(String, u64, String)],
) -> String {
    let mut msgs = Vec::new();
    for (name, len, hash) in after {
        match before.iter().find(|(n, _, _)| n == name) {
            None => msgs.push(format!("created {name} ({len} bytes)")),
            Some((_, blen, bhash)) if bhash != hash => {
                msgs.push(format!("modified {name} ({blen} -> {len} bytes)"))
            }
            _ => {}
        }
    }
    for (name, _, _) in before {
        if !after.iter().any(|(n, _, _)| n == name) {
            msgs.push(format!("deleted {name}"));
        }
    }
    msgs.join("; ")
}

/// The DATA files a rejected open must never touch. Creating a fresh empty
/// sidecar (an empty WAL directory, say) is not data modification; rewriting
/// or truncating existing content is.
fn assert_data_unchanged(
    before: &[(String, u64, String)],
    after: &[(String, u64, String)],
    ctx: &str,
) {
    let mut damaged = Vec::new();
    for (name, blen, bhash) in before {
        match after.iter().find(|(n, _, _)| n == name) {
            None => damaged.push(format!("deleted {name}")),
            Some((_, alen, ahash)) if ahash != bhash => {
                damaged.push(format!("rewrote {name} ({blen} -> {alen} bytes)"))
            }
            _ => {}
        }
    }
    assert!(
        damaged.is_empty(),
        "{ctx}: a rejected open modified existing data: {}",
        damaged.join("; ")
    );
}

/// Open an executor the way the server does: catalog file + disk engine +
/// executor metadata. A bare `Executor::new` with a fresh `Catalog` has no
/// schema, so it cannot express "restart".
async fn open_executor(dir: &std::path::Path) -> Executor {
    use crate::storage::persistence::CatalogPersistence;
    let db_path = dir.join("full.db");
    let catalog_path = dir.join("catalog.json");
    let catalog = Arc::new(Catalog::new());
    CatalogPersistence::new(&catalog_path)
        .load_catalog(&catalog)
        .await
        .ok();
    let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
    let storage: Arc<dyn StorageEngine> = Arc::new(engine);
    let ex = Executor::new_with_persistence(
        catalog,
        storage,
        Some(catalog_path),
        Some(dir),
    );
    ex.load_meta().await;
    ex
}

// ============================================================================
// Format-version rejection
// ============================================================================

#[tokio::test]
async fn a_future_on_disk_format_is_rejected_without_modifying_data() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("fmt.db");

    // Build a real database with committed content.
    {
        let catalog = Arc::new(Catalog::new());
        let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        let ex = Executor::new(catalog, storage);
        exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'keep me')").await;
    }

    // Stamp a format version from the future into the meta page.
    let mut bytes = std::fs::read(&db_path).unwrap();
    let future = page::DB_FORMAT_VERSION + 9;
    bytes[page::META_DB_VERSION..page::META_DB_VERSION + 4]
        .copy_from_slice(&future.to_le_bytes());
    std::fs::write(&db_path, &bytes).unwrap();

    let before = dir_fingerprint(tmp.path());

    // Opening must fail rather than proceed against a format we cannot read.
    let catalog = Arc::new(Catalog::new());
    let opened = DiskEngine::open(&db_path, catalog);
    assert!(
        opened.is_err(),
        "a database written by a newer format version was opened anyway"
    );
    let msg = format!("{:?}", opened.err().unwrap());
    assert!(
        msg.contains("version") || msg.contains("format"),
        "rejection should name the format problem, got: {msg}"
    );

    // And the refusal must be non-destructive: no existing file rewritten or
    // removed. (A fresh empty sidecar is tolerated; damaging data is not.)
    let after = dir_fingerprint(tmp.path());
    assert_data_unchanged(&before, &after, "future format version");
    eprintln!("  (side effects: {})", describe_changes(&before, &after));
}

#[tokio::test]
async fn a_foreign_file_is_rejected_without_modifying_data() {
    let tmp = tempfile::tempdir().unwrap();
    let db_path = tmp.path().join("foreign.db");

    {
        let catalog = Arc::new(Catalog::new());
        let engine = DiskEngine::open(&db_path, catalog.clone()).unwrap();
        let storage: Arc<dyn StorageEngine> = Arc::new(engine);
        let ex = Executor::new(catalog, storage);
        exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY)").await;
        exec(&ex, "INSERT INTO t VALUES (1)").await;
    }

    // Replace the magic with another product's.
    let mut bytes = std::fs::read(&db_path).unwrap();
    bytes[page::META_MAGIC..page::META_MAGIC + 8].copy_from_slice(b"SQLite3\0");
    std::fs::write(&db_path, &bytes).unwrap();

    let before = dir_fingerprint(tmp.path());
    let catalog = Arc::new(Catalog::new());
    assert!(
        DiskEngine::open(&db_path, catalog).is_err(),
        "a file that is not a Nucleus database was opened as one"
    );
    let after = dir_fingerprint(tmp.path());
    assert_data_unchanged(&before, &after, "foreign file");
    eprintln!("  (side effects: {})", describe_changes(&before, &after));
}

#[tokio::test]
async fn the_current_format_still_opens() {
    // Control for the two rejection tests above: they would also "pass" if
    // open() simply refused everything. An untouched database must still open
    // and read its rows back.
    let tmp = tempfile::tempdir().unwrap();
    {
        let ex = open_executor(tmp.path()).await;
        exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
        exec(&ex, "INSERT INTO t VALUES (1, 'hello')").await;
    }
    let ex = open_executor(tmp.path()).await;
    let r = exec(&ex, "SELECT v FROM t WHERE id = 1").await;
    assert_eq!(rows(&r[0])[0][0], Value::Text("hello".into()));
}

// ============================================================================
// Full-state recovery: catalog, metadata, specialty index, rows
// ============================================================================

/// A restart must restore the whole database, not just its rows: constraints,
/// sequences, views, and specialty (vector) index state. RLS policy survival
/// is covered separately in `test_meta_persistence`.
#[tokio::test]
async fn catalog_metadata_and_specialty_state_all_survive_reopen() {
    let tmp = tempfile::tempdir().unwrap();

    {
        let ex = open_executor(tmp.path()).await;
        exec(
            &ex,
            "CREATE TABLE docs (id SERIAL PRIMARY KEY, owner TEXT NOT NULL, \
             score INT CHECK (score >= 0), e VECTOR(3))",
        )
        .await;
        exec(
            &ex,
            "INSERT INTO docs (owner, score, e) VALUES \
             ('alice', 10, VECTOR('[1,0,0]')), ('bob', 20, VECTOR('[0,1,0]'))",
        )
        .await;
        exec(&ex, "CREATE INDEX docs_owner_idx ON docs (owner)").await;
        exec(&ex, "CREATE VIEW docs_v AS SELECT id, owner FROM docs").await;
    }

    // Restart.
    let ex = open_executor(tmp.path()).await;

    let r = exec(&ex, "SELECT COUNT(*) FROM docs").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(2), "rows did not survive reopen");

    // Constraints must come back, or a restart silently relaxes the schema.
    assert!(
        ex.execute("INSERT INTO docs (id, owner, score) VALUES (1, 'x', 1)")
            .await
            .is_err(),
        "PRIMARY KEY was not restored — a duplicate key was accepted after reopen"
    );
    assert!(
        ex.execute("INSERT INTO docs (owner, score) VALUES ('x', -5)")
            .await
            .is_err(),
        "CHECK constraint was not restored"
    );

    // Sequence continues rather than restarting at 1.
    exec(&ex, "INSERT INTO docs (owner, score) VALUES ('carol', 1)").await;
    let r = exec(&ex, "SELECT MAX(id) FROM docs").await;
    let max_id = match scalar(&r[0]) {
        Value::Int64(n) => *n,
        Value::Int32(n) => i64::from(*n),
        other => panic!("unexpected id type: {other:?}"),
    };
    assert!(
        max_id > 2,
        "SERIAL sequence restarted after reopen (max id {max_id})"
    );

    // View survived.
    let r = exec(&ex, "SELECT COUNT(*) FROM docs_v").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(3), "view did not survive reopen");

    // Specialty index state: the vector column still answers a KNN query.
    let knn = ex
        .execute("SELECT id FROM docs ORDER BY e <-> VECTOR('[1,0,0]') LIMIT 1")
        .await;
    assert!(
        knn.is_ok(),
        "vector state did not survive reopen: {:?}",
        knn.err()
    );
}

// ============================================================================
// Online backup driven by the live instance
// ============================================================================

/// A RUNNING instance must be able to snapshot itself.
///
/// The CLI deliberately refuses to copy a live data directory, because an
/// outside process holds no lock, observes no LSN, and cannot pin WAL
/// retention — it can only produce a torn copy. That left no way at all to back
/// up a serving database, which is the milestone's actual goal. The fix routes
/// the backup through the live engine (the `pg_basebackup` shape: the server
/// snapshots itself), reachable from the executor via
/// `StorageEngine::as_backup_coordinator`.
#[tokio::test]
async fn a_live_instance_can_snapshot_itself_and_the_snapshot_restores() {
    let tmp = tempfile::tempdir().unwrap();
    let ex = open_executor(tmp.path()).await;
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").await;
    for i in 1..=50 {
        exec(&ex, &format!("INSERT INTO t VALUES ({i}, 'row{i}')")).await;
    }

    // The executor is holding the data directory open — exactly the case the
    // CLI refuses — and must still be able to snapshot itself.
    let outside = tempfile::tempdir().unwrap();
    let out = outside.path().join("snap");
    let manifest = ex
        .backup_online_to(&out, false)
        .await
        .expect("a live instance must be able to back itself up");
    assert!(manifest.online, "manifest must record an online snapshot");
    assert!(
        manifest.consistent_lsn > 0,
        "an online snapshot must name the LSN it is consistent through"
    );
    assert!(
        !manifest.database_id.is_empty(),
        "snapshot must carry a database identity"
    );

    // The instance keeps serving after the backup window closes.
    exec(&ex, "INSERT INTO t VALUES (51, 'after-backup')").await;
    let r = exec(&ex, "SELECT COUNT(*) FROM t").await;
    assert_eq!(scalar(&r[0]), &Value::Int64(51));

    // The snapshot restores to the committed point, and is USABLE — not merely
    // present. A restore that cannot take a write is not a backup.
    let restored = outside.path().join("restored");
    crate::backup::restore_data_dir(&out, &restored, false, env!("CARGO_PKG_VERSION"))
        .expect("restore the online snapshot");
    let ex2 = open_executor(&restored).await;
    let r = exec(&ex2, "SELECT COUNT(*) FROM t").await;
    let n = match scalar(&r[0]) {
        Value::Int64(v) => *v,
        Value::Int32(v) => i64::from(*v),
        other => panic!("unexpected count type: {other:?}"),
    };
    assert!(
        (50..=51).contains(&n),
        "restored snapshot must hold the rows committed by the backup point, got {n}"
    );
    exec(&ex2, "INSERT INTO t VALUES (100, 'post-restore')").await;
}

/// An engine with no physical snapshot must say so, not silently produce
/// something that looks like a backup.
#[tokio::test]
async fn an_engine_without_a_physical_snapshot_refuses_clearly() {
    let ex = test_executor(); // memory engine
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY)").await;
    let tmp = tempfile::tempdir().unwrap();
    let err = ex
        .backup_online_to(&tmp.path().join("snap"), false)
        .await
        .expect_err("a memory engine has no physical snapshot");
    let msg = err.to_string();
    assert!(
        msg.contains("physical snapshot") || msg.contains("data directory"),
        "refusal should explain why, got: {msg}"
    );
}

/// A destination inside the data directory must be refused, clearly.
///
/// Found while writing the test above: the tree copy descends into the snapshot
/// it is writing and copies it into itself until the path exceeds the OS limit,
/// which surfaced as "File name too long" — a message that tells the operator
/// nothing. `BACKUP DATABASE TO '/var/lib/nucleus/data/backup'` is an easy
/// thing to type.
#[tokio::test]
async fn a_backup_destination_inside_the_data_directory_is_refused() {
    let tmp = tempfile::tempdir().unwrap();
    let ex = open_executor(tmp.path()).await;
    exec(&ex, "CREATE TABLE t (id INT PRIMARY KEY)").await;
    exec(&ex, "INSERT INTO t VALUES (1)").await;

    let err = ex
        .backup_online_to(&tmp.path().join("inner_snap"), false)
        .await
        .expect_err("a destination inside the data directory must be refused");
    let msg = err.to_string();
    assert!(
        msg.contains("inside the data directory"),
        "refusal must explain the nesting problem, got: {msg}"
    );
}
