//! A checkpoint must not discard writes that were acknowledged before it.
//!
//! `CollectionWal::checkpoint` serialized the collections BEFORE taking the
//! writer lock and then replaced the whole log with that snapshot. Anything
//! appended between the read of a shard and the replace was in neither the
//! snapshot nor the surviving log — and the mark that follows
//! (`on_append` + `mark_synced`) told the syncer every outstanding append was
//! durable, so no later `group_sync` would ever rewrite it. The write survived
//! in memory until the process exited and was gone from disk immediately.
//!
//! Found by the S30 triage of `kv/collections.rs` + `kv/collections_wal.rs`,
//! the two files no prior audit had covered.
//!
//! This is a stress reproduction rather than a deterministic one: the window is
//! real but narrow, so the test writes hard while checkpointing repeatedly and
//! then reopens the directory. Every value it observed as acknowledged must
//! come back.

#![cfg(feature = "server")]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use nucleus::kv::KvStore;
use nucleus::types::Value;

/// Writers push a known set of members into a set; the checkpointer runs
/// concurrently. After a reopen every member must still be there.
#[test]
fn a_checkpoint_does_not_lose_writes_it_raced_with() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();

    const WRITERS: usize = 4;
    const PER_WRITER: usize = 500;

    {
        let store = Arc::new(KvStore::open(&dir).expect("open kv store"));
        let stop = Arc::new(AtomicBool::new(false));

        let checkpointer = {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                let mut rounds = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    store.checkpoint().expect("checkpoint");
                    rounds += 1;
                    std::thread::yield_now();
                }
                rounds
            })
        };

        let writers: Vec<_> = (0..WRITERS)
            .map(|w| {
                let store = Arc::clone(&store);
                std::thread::spawn(move || {
                    for i in 0..PER_WRITER {
                        store
                            .collections()
                            .sadd("members", &format!("w{w}-{i}"))
                            .expect("sadd");
                    }
                })
            })
            .collect();

        for h in writers {
            h.join().unwrap();
        }
        stop.store(true, Ordering::Relaxed);
        let rounds = checkpointer.join().unwrap();
        assert!(
            rounds > 0,
            "the checkpointer never ran, so this test proves nothing"
        );

        // Everything is acknowledged and in memory at this point.
        assert_eq!(
            store.collections().scard("members").expect("scard"),
            WRITERS * PER_WRITER,
            "the in-memory set lost writes, which is a different bug from the \
             one under test"
        );
    }

    // Reopen: the log plus its snapshot are the only record now.
    let reopened = KvStore::open(&dir).expect("reopen kv store");
    let recovered = reopened.collections().scard("members").expect("scard");
    assert_eq!(
        recovered,
        WRITERS * PER_WRITER,
        "a checkpoint discarded {} acknowledged writes — they were appended \
         after the snapshot read their shard and before the log was replaced",
        WRITERS * PER_WRITER - recovered
    );
}

/// The mechanism, deterministically: a checkpoint must WAIT while the log is
/// ahead of memory.
///
/// Every collection op logs before it takes its shard lock, so between the two
/// the log holds a record the collections do not. A checkpoint that snapshotted
/// memory in that window wrote a snapshot missing the record and replaced the
/// whole log with it — the write was acknowledged, gone from disk, and alive
/// only in the memory of the process that had not applied it yet.
///
/// The guard every `log_*` call returns marks that window, and the checkpoint
/// drains on it. Holding one open across a concurrent checkpoint is therefore
/// the whole property, in one assertion: the checkpoint may not finish first.
#[test]
fn a_checkpoint_waits_while_the_log_is_ahead_of_memory() {
    use nucleus::kv::collections::ShardedCollections;
    use nucleus::kv::collections_wal::CollectionWal;
    use std::time::{Duration, Instant};

    let tmp = tempfile::tempdir().unwrap();
    let (wal, _collections) = CollectionWal::open(tmp.path()).expect("open wal");
    let empty = ShardedCollections::new();

    // The record reaches the log; the guard says its effect has not landed.
    let guard = wal
        .log_rpush("log", &Value::Text("acknowledged".into()))
        .expect("append");

    let released = AtomicBool::new(false);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            std::thread::sleep(Duration::from_millis(200));
            released.store(true, Ordering::Release);
            drop(guard);
        });

        std::thread::sleep(Duration::from_millis(20));
        let started = Instant::now();
        wal.checkpoint(&empty).expect("checkpoint");
        let waited = started.elapsed();

        assert!(
            released.load(Ordering::Acquire),
            "the checkpoint completed while an append was still in flight — it \
             would have snapshotted memory that is missing an acknowledged \
             record and then replaced the log with that snapshot"
        );
        assert!(
            waited >= Duration::from_millis(100),
            "the checkpoint returned in {waited:?}, so it did not wait for the \
             in-flight append at all"
        );
    });
}

/// The same shape with a single writer and no concurrency, as the control: if
/// this ever fails the defect is not a race at all.
#[test]
fn a_checkpoint_keeps_writes_when_nothing_races_it() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    {
        let store = KvStore::open(&dir).expect("open");
        for i in 0..100 {
            store
                .collections()
                .sadd("members", &format!("m{i}"))
                .expect("sadd");
        }
        store.checkpoint().expect("checkpoint");
        for i in 100..200 {
            store
                .collections()
                .sadd("members", &format!("m{i}"))
                .expect("sadd");
        }
    }
    let reopened = KvStore::open(&dir).expect("reopen");
    assert_eq!(reopened.collections().scard("members").expect("scard"), 200);
}

/// Lists too — the list ops log through the same path and a lost `LPUSH` is a
/// lost element rather than a lost set member.
#[test]
fn a_checkpoint_does_not_lose_list_writes_it_raced_with() {
    let tmp = tempfile::tempdir().unwrap();
    let dir = tmp.path().to_path_buf();
    const PUSHES: usize = 1_000;

    {
        let store = Arc::new(KvStore::open(&dir).expect("open"));
        let stop = Arc::new(AtomicBool::new(false));
        let checkpointer = {
            let store = Arc::clone(&store);
            let stop = Arc::clone(&stop);
            std::thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    store.checkpoint().expect("checkpoint");
                    std::thread::yield_now();
                }
            })
        };
        for i in 0..PUSHES {
            store
                .collections()
                .rpush("log", Value::Text(format!("entry-{i}")))
                .expect("rpush");
        }
        stop.store(true, Ordering::Relaxed);
        checkpointer.join().unwrap();
        assert_eq!(store.collections().llen("log").expect("llen"), PUSHES);
    }

    let reopened = KvStore::open(&dir).expect("reopen");
    assert_eq!(
        reopened.collections().llen("log").expect("llen"),
        PUSHES,
        "a checkpoint discarded acknowledged list writes"
    );
}
