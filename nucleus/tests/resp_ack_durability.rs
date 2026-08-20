//! A RESP acknowledgement must mean the write is fsynced.
//!
//! `force_specialty_durability` drains the KV and collections logs at the SQL
//! commit boundary, and only the SQL path called it. Nothing in `src/resp/`
//! synced anything, so a write acknowledged to a Redis-protocol client survived
//! `kill -9` — kernel buffers do — and not a power cut, up to the next
//! checkpoint interval.
//!
//! Found by the S30 triage, which also made the point that matters most about
//! it: **the documented write-then-`kill -9`-then-restart evidence structurally
//! cannot catch this class**, because `kill -9` leaves the kernel's buffers
//! intact. That is why the gap looked covered.
//!
//! The assertion here is the invariant rather than a crash: when the handler
//! returns the reply bytes, the log it wrote is no longer dirty. `is_dirty`
//! means "appended past the last completed fsync", so it is exactly the
//! question "would a power cut lose this".

#![cfg(feature = "server")]

use std::sync::Arc;

use nucleus::kv::KvStore;
use nucleus::resp::handler::RespHandler;
use nucleus::resp::pubsub_registry::PubSubRegistry;

fn cmd(parts: &[&str]) -> Vec<Vec<u8>> {
    parts.iter().map(|p| p.as_bytes().to_vec()).collect()
}

fn handler(dir: &std::path::Path) -> (RespHandler, Arc<KvStore>) {
    let store = Arc::new(KvStore::open(dir).expect("open kv store"));
    let handler = RespHandler::new(Arc::clone(&store), None, Arc::new(PubSubRegistry::new()));
    (handler, store)
}

#[test]
fn a_string_write_is_fsynced_before_its_acknowledgement() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut h, store) = handler(tmp.path());

    let reply = h.handle_command(cmd(&["SET", "k", "v"]));
    assert_eq!(reply, b"+OK\r\n".to_vec());
    assert!(
        !store.wal().expect("durable store").is_dirty(),
        "the KV WAL still had unsynced bytes when the client was told OK — a \
         power cut here loses an acknowledged write"
    );
}

#[test]
fn a_collection_write_is_fsynced_before_its_acknowledgement() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut h, store) = handler(tmp.path());

    h.handle_command(cmd(&["HSET", "h", "field", "value"]));
    assert!(
        !store.collections_wal().expect("durable store").is_dirty(),
        "the collections WAL still had unsynced bytes when the client was told \
         the HSET succeeded"
    );

    h.handle_command(cmd(&["LPUSH", "l", "element"]));
    assert!(
        !store.collections_wal().expect("durable store").is_dirty(),
        "LPUSH acknowledged without an fsync"
    );

    h.handle_command(cmd(&["XADD", "s", "*", "f", "v"]));
    assert!(
        !store.collections_wal().expect("durable store").is_dirty(),
        "XADD acknowledged without an fsync"
    );
}

/// The control: a read must not pay for an fsync it does not need. If this ever
/// fails the sync is unconditional, which would make every GET a syscall.
#[test]
fn a_read_does_not_dirty_the_log() {
    let tmp = tempfile::tempdir().unwrap();
    let (mut h, store) = handler(tmp.path());
    h.handle_command(cmd(&["SET", "k", "v"]));

    let before = store.wal().expect("durable").is_dirty();
    let reply = h.handle_command(cmd(&["GET", "k"]));
    assert_eq!(reply, b"$1\r\nv\r\n".to_vec());
    assert_eq!(
        before,
        store.wal().expect("durable").is_dirty(),
        "a read changed the log's dirty state"
    );
}
