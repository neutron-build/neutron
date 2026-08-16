//! NU-163: a meta.json that cannot be READ must not be indistinguishable from
//! one that does not exist.
//!
//! `MetaPersistence::load` collapses three outcomes into `LoadedMeta::default()`
//! at `warn` level: the file is absent (first boot, legitimately empty), the
//! file could not be read (permissions, I/O), and the file did not parse
//! (corruption). Install then treats the result differently from every other
//! catalog: views, matviews, triggers, roles, sequences, functions and
//! extensions are each `is_empty()`-guarded and survive, but security is
//! assigned unconditionally —
//!
//! ```ignore
//! let mut security = self.security.write();
//! security.rls = loaded.rls;
//! security.masking = loaded.masking;
//! ```
//!
//! — so one unreadable byte boots the server with no RLS and no masking, while
//! the bootstrap superuser role survives (it comes from the guarded roles map).
//!
//! And it does not stop at memory. The next DDL snapshots that emptied state and
//! writes it back through the same atomic save, so a TRANSIENT read failure at
//! startup permanently destroys the policy catalog. That is the half worth
//! testing: an in-memory fail-open is bad, an on-disk one is unrecoverable.
#![cfg(feature = "server")]
use std::path::{Path, PathBuf};
use std::sync::Arc;

use nucleus::catalog::Catalog;
use nucleus::executor::Executor;
use nucleus::storage::MvccStorageAdapter;

fn boot(dir: &Path) -> Arc<Executor> {
    std::fs::create_dir_all(dir).unwrap();
    Arc::new(Executor::new_with_persistence(
        Arc::new(Catalog::new()),
        Arc::new(MvccStorageAdapter::new()),
        Some(dir.join("catalog.json")),
        Some(dir),
    ))
}

fn meta_path(dir: &Path) -> PathBuf {
    dir.join("meta.json")
}

async fn run(ex: &Executor, sql: &str) {
    ex.execute(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
}

/// Lay down a database with a real policy and masking rule, persisted.
async fn seed(dir: &Path) {
    let ex = boot(dir);
    run(
        &ex,
        "CREATE TABLE docs (id INT PRIMARY KEY, owner TEXT, ssn TEXT)",
    )
    .await;
    run(&ex, "CREATE ROLE alice LOGIN PASSWORD 'a'").await;
    run(&ex, "GRANT SELECT ON docs TO alice").await;
    run(
        &ex,
        "CREATE POLICY owner_isolation ON docs FOR ALL TO PUBLIC \
         USING (owner = CURRENT_USER) WITH CHECK (owner = CURRENT_USER)",
    )
    .await;
    run(&ex, "ALTER TABLE docs ENABLE ROW LEVEL SECURITY").await;

    let meta = std::fs::read_to_string(meta_path(dir)).expect("meta.json must have been written");
    assert!(
        meta.contains("owner_isolation"),
        "the seed must actually persist the policy, or this test proves nothing"
    );
}

/// A meta.json that exists but does not parse must not boot as "no policies".
#[tokio::test]
async fn corrupt_meta_does_not_silently_boot_with_an_empty_policy_catalog() {
    let dir = std::env::temp_dir().join(format!("nu163-parse-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    seed(&dir).await;

    // One bad byte is enough; use obvious garbage so the intent is unambiguous.
    std::fs::write(meta_path(&dir), b"{ this is not json").unwrap();

    let ex = boot(&dir);
    let loaded = ex.load_meta_checked().await;
    assert!(
        loaded.is_err(),
        "an unparseable meta.json must be reported, not folded into an empty catalog"
    );
}

/// The unrecoverable half: a failed load must never be written back over the
/// file it failed to read.
#[tokio::test]
async fn a_failed_meta_load_never_overwrites_the_policy_catalog() {
    let dir = std::env::temp_dir().join(format!("nu163-overwrite-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    seed(&dir).await;

    std::fs::write(meta_path(&dir), b"{ this is not json").unwrap();

    let corrupt = std::fs::read_to_string(meta_path(&dir)).unwrap();

    let ex = boot(&dir);
    let _ = ex.load_meta_checked().await;

    // Any DDL persists metadata. Before the fix this snapshotted the emptied
    // security state and atomically replaced meta.json with it — measured: seed
    // a policy, corrupt the file, boot, run one CREATE TABLE, and the policy is
    // gone from disk. Now the DDL is refused instead.
    let ddl = ex.execute("CREATE TABLE unrelated (id INT)").await;
    assert!(
        ddl.is_err(),
        "a DDL after a failed meta load must be refused, not allowed to persist \
         an empty policy catalog over the file that could not be read"
    );

    let after = std::fs::read_to_string(meta_path(&dir)).unwrap();
    assert_eq!(
        after, corrupt,
        "meta.json was rewritten after a failed load; the corrupt file was still \
         recoverable by hand, an empty one that overwrote it is not"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// An ABSENT meta.json is the ordinary first-boot case and must stay silent and
/// successful — otherwise the fix turns every new database into an error.
#[tokio::test]
async fn absent_meta_is_still_a_clean_first_boot() {
    let dir = std::env::temp_dir().join(format!("nu163-absent-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    let ex = boot(&dir);
    assert!(
        ex.load_meta_checked().await.is_ok(),
        "a database with no meta.json yet is not a corrupt one"
    );
    run(&ex, "CREATE TABLE t (id INT)").await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// A well-formed meta.json still round-trips: the policy is back after reboot.
#[tokio::test]
async fn a_good_meta_still_loads() {
    let dir = std::env::temp_dir().join(format!("nu163-good-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    seed(&dir).await;

    let ex = boot(&dir);
    ex.load_meta_checked()
        .await
        .expect("a valid meta.json must load");

    run(&ex, "CREATE TABLE unrelated (id INT)").await;
    let after = std::fs::read_to_string(meta_path(&dir)).unwrap();
    assert!(
        after.contains("owner_isolation"),
        "a good load followed by a DDL must preserve the policy catalog"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
