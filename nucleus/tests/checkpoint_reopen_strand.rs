//! S31-14: a checkpoint whose reopen fails strands the WAL writer on an
//! unlinked inode.
//!
//! `checkpoint` flushes, atomically replaces the log file, then reopens it.
//! When that reopen fails, the writer keeps the handle to the file the rename
//! displaced: appends to it succeed — into an inode nothing will ever read
//! again — while `group_sync`/`is_dirty` report healthy, so acknowledged
//! writes are silently non-durable. `NUCLEUS_IOFAULT` at the reopen boundary
//! arms the real failure path in a freshly spawned process, so the code under
//! test is the code a full disk or fd exhaustion would actually hit.

#![cfg(feature = "server")]

use std::process::Command;

/// Spawn this test binary's own helper in a child with the fault armed. The
/// fault point is read once per process into a `OnceLock`, so it cannot be
/// armed in-process without poisoning every other test in the binary.
fn run_child(point: &str, section: &str, dir: &std::path::Path) -> String {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe)
        .arg("--exact")
        .arg("child_harness")
        .arg("--nocapture")
        .arg("--ignored")
        .env("NUCLEUS_IOFAULT", point)
        .env("NUCLEUS_IOFAULT_KIND", "io")
        .env("STRAND_DIR", dir)
        .env("STRAND_SECTION", section)
        .output()
        .expect("spawn child");
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
#[ignore = "child process driven by the parent test"]
fn child_harness() {
    let Ok(dir) = std::env::var("STRAND_DIR") else {
        return;
    };
    let section = std::env::var("STRAND_SECTION").unwrap_or_default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = nucleus::embedded::Database::durable_mvcc(std::path::Path::new(&dir)).unwrap();
    let ex = db.executor();

    match section.as_str() {
        // One acknowledged write, a checkpoint that fails at the reopen, then
        // one more write whose acknowledgement is the whole test.
        "streams" => {
            let first = rt.block_on(db.execute("SELECT STREAM_XADD('s', 'k', 'before')"));
            println!("STREAM_XADD1 {}", first.is_ok());
            let ckpt = ex.checkpoint_streams_wal();
            println!("STREAM_CKPT {}", ckpt.is_ok());
            let second = rt.block_on(db.execute("SELECT STREAM_XADD('s', 'k', 'after')"));
            println!("STREAM_XADD2 {}", second.is_ok());
        }
        "vector" => {
            rt.block_on(db.execute("CREATE TABLE iov (id INT PRIMARY KEY, x VECTOR(4))"))
                .unwrap();
            rt.block_on(db.execute("CREATE INDEX iov_idx ON iov USING HNSW (x)"))
                .unwrap();
            let first =
                rt.block_on(db.execute("INSERT INTO iov (id, x) VALUES (1, VECTOR('[1,2,3,4]'))"));
            println!("VEC_INS1 {}", first.is_ok());
            let ckpt = ex.checkpoint_vector_wal();
            println!("VEC_CKPT {}", ckpt.is_ok());
            let second =
                rt.block_on(db.execute("INSERT INTO iov (id, x) VALUES (2, VECTOR('[5,6,7,8]'))"));
            println!("VEC_INS2 {}", second.is_ok());
        }
        _ => {}
    }
}

/// The checkpoint itself fails (that error is already loud), and the write
/// AFTER it must fail too rather than be acknowledged into the dead inode.
#[test]
fn a_stranded_streams_writer_fails_appends_instead_of_lying() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_child("streams.wal_reopen", "streams", dir.path());

    assert!(
        out.contains("STREAM_XADD1 true"),
        "the pre-fault write should succeed; got:\n{out}"
    );
    assert!(
        out.contains("STREAM_CKPT false"),
        "the injected reopen failure must fail the checkpoint; got:\n{out}"
    );
    assert!(
        out.contains("STREAM_XADD2 false"),
        "a write after the failed checkpoint was acknowledged while its log \
         record went to an unlinked inode; got:\n{out}"
    );

    // Recovery: the snapshot the checkpoint DID write holds the acknowledged
    // entry, and the rejected one is absent.
    let db = nucleus::embedded::Database::durable_mvcc(dir.path()).unwrap();
    let len = db
        .executor()
        .streams()
        .read()
        .get("s")
        .map(|s| s.xlen())
        .unwrap_or(0);
    assert_eq!(len, 1, "exactly the acknowledged entry may survive");
}

#[test]
fn a_stranded_vector_writer_fails_appends_instead_of_lying() {
    let dir = tempfile::tempdir().unwrap();
    let out = run_child("vector.wal_reopen", "vector", dir.path());

    assert!(
        out.contains("VEC_INS1 true"),
        "the pre-fault insert should succeed; got:\n{out}"
    );
    assert!(
        out.contains("VEC_CKPT false"),
        "the injected reopen failure must fail the checkpoint; got:\n{out}"
    );
    assert!(
        out.contains("VEC_INS2 false"),
        "an insert after the failed checkpoint was acknowledged while its log \
         record went to an unlinked inode; got:\n{out}"
    );

    let db = nucleus::embedded::Database::durable_mvcc(dir.path()).unwrap();
    let live = db
        .executor()
        .hnsw_index_live_ids("iov_idx")
        .expect("the HNSW index must survive via the snapshot the checkpoint wrote");
    assert_eq!(live.len(), 1, "exactly the acknowledged vector may survive");
}
