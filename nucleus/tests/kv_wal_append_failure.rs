//! A KV write whose log refuses the append must not be reported as a success.
//!
//! The mutators log the IO error and apply the change anyway — deliberate, so
//! the live view stays usable — which leaves the acknowledgement as the only
//! thing that can carry the truth. `NUCLEUS_IOFAULT=kv.wal_append` arms the
//! real failure path, so this drives the same code a full disk would.

#![cfg(feature = "server")]

use std::process::Command;

/// Run this test binary's own helper in a child with the fault armed. The
/// fault point is read once per process into a `OnceLock`, so it cannot be
/// armed in-process without poisoning every other test in the binary.
fn run_child(point: &str, body: &str, dir: &std::path::Path) -> String {
    let exe = std::env::current_exe().unwrap();
    let out = Command::new(exe)
        .arg("--exact")
        .arg("child_harness")
        .arg("--nocapture")
        .arg("--ignored")
        .env("NUCLEUS_IOFAULT", point)
        .env("NUCLEUS_IOFAULT_KIND", "full")
        .env("NUCLEUS_IOFAULT_SKIP", "1")
        .env("KVFAULT_DIR", dir)
        .env("KVFAULT_BODY", body)
        .output()
        .expect("spawn child");
    String::from_utf8_lossy(&out.stdout).to_string()
}

#[test]
#[ignore = "child process driven by the parent test"]
fn child_harness() {
    let Ok(dir) = std::env::var("KVFAULT_DIR") else {
        return;
    };
    let body = std::env::var("KVFAULT_BODY").unwrap_or_default();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let db = nucleus::embedded::Database::durable_mvcc(std::path::Path::new(&dir)).unwrap();
    for (i, stmt) in body.split(';').enumerate() {
        if stmt.trim().is_empty() {
            continue;
        }
        match rt.block_on(db.execute(stmt)) {
            Ok(_) => println!("ACKED {i}"),
            Err(e) => println!("ERRORED {i} {e:?}"),
        }
    }
}

#[test]
fn failed_kv_append_fails_the_statement() {
    let dir = tempfile::tempdir().unwrap();
    let body = "SELECT KV_SET('a', 'one');SELECT KV_SET('b', 'two');SELECT KV_SET('c', 'three')";
    let out = run_child("kv.wal_append", body, dir.path());

    assert!(
        out.contains("ACKED 0"),
        "the pre-fault write should succeed; got:\n{out}"
    );
    assert!(
        out.contains("ERRORED 1"),
        "the write whose append was refused was acknowledged as a success; got:\n{out}"
    );
}

#[test]
fn failed_collections_append_fails_the_statement() {
    let dir = tempfile::tempdir().unwrap();
    let body =
        "SELECT KV_RPUSH('l', 'one');SELECT KV_RPUSH('l', 'two');SELECT KV_RPUSH('l', 'three')";
    let out = run_child("collections.wal_append", body, dir.path());

    assert!(
        out.contains("ACKED 0"),
        "the pre-fault write should succeed; got:\n{out}"
    );
    assert!(
        out.contains("ERRORED 1"),
        "the collection write whose append was refused was acknowledged; got:\n{out}"
    );
}
