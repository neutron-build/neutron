//! KV-collection streams must survive a restart.
//!
//! `CollectionWal::log_xadd` and the `OP_XADD` replay arm both existed, with
//! unit tests — and **no caller anywhere in the crate**. So every stream entry
//! written through the KV collections (the RESP `XADD` surface) was lost on
//! restart, while every `LPUSH` beside it in the same store survived. Found by
//! the S30 triage, the first audit run over these two files.
//!
//! `XDEL` had the same shape and is covered here too: a deletion that is not
//! logged comes back on the next boot, which is the worse direction.

#![cfg(feature = "server")]

use nucleus::kv::KvStore;

#[test]
fn stream_entries_survive_a_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let ids = {
        let store = KvStore::open(&dir).expect("open");
        let a = store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "created".into())])
            .expect("xadd");
        let b = store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "updated".into())])
            .expect("xadd");
        assert_eq!(store.collections().xlen("events").expect("xlen"), 2);
        (a, b)
    };

    let reopened = KvStore::open(&dir).expect("reopen");
    assert_eq!(
        reopened.collections().xlen("events").expect("xlen"),
        2,
        "stream entries did not survive the restart — XADD is not logged"
    );

    // The entries came back with their assigned ids, not new ones: a replay
    // that re-generated ids would be a different stream with the same length.
    let range = reopened
        .collections()
        .xrange("events", "-", "+", None)
        .expect("xrange");
    let recovered: Vec<String> = range.iter().map(|e| e.id.to_string()).collect();
    assert_eq!(
        recovered,
        vec![ids.0.to_string(), ids.1.to_string()],
        "the recovered entries have different ids than the acknowledged ones"
    );
}

#[test]
fn a_deleted_stream_entry_does_not_come_back() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    let doomed = {
        let store = KvStore::open(&dir).expect("open");
        let doomed = store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "created".into())])
            .expect("xadd");
        store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "kept".into())])
            .expect("xadd");
        assert_eq!(
            store.collections().xdel("events", &[doomed]).expect("xdel"),
            1
        );
        assert_eq!(store.collections().xlen("events").expect("xlen"), 1);
        doomed
    };

    let reopened = KvStore::open(&dir).expect("reopen");
    assert_eq!(
        reopened.collections().xlen("events").expect("xlen"),
        1,
        "a deleted stream entry came back after the restart — XDEL is not logged"
    );
    let range = reopened
        .collections()
        .xrange("events", "-", "+", None)
        .expect("xrange");
    assert!(
        !range.iter().any(|e| e.id == doomed),
        "the deleted entry is the one that survived"
    );
}

/// A checkpoint absorbs the stream entries too, so they survive a restart that
/// replays only the snapshot.
#[test]
fn stream_entries_survive_a_checkpoint_and_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    {
        let store = KvStore::open(&dir).expect("open");
        store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "before".into())])
            .expect("xadd");
        store.checkpoint().expect("checkpoint");
        store
            .collections()
            .xadd("events", "*", vec![("kind".into(), "after".into())])
            .expect("xadd");
    }
    let reopened = KvStore::open(&dir).expect("reopen");
    assert_eq!(
        reopened.collections().xlen("events").expect("xlen"),
        2,
        "an entry was lost across the checkpoint boundary"
    );
}
