//! C-T1b tests: read-your-commits (§3 I-ACK) — after `commit` returns, a
//! new snapshot from another thread sees the write. The intents are written
//! by hand with the C-T1a encoding, as the C-T2 write path will.

mod common;

use std::sync::Arc;

use common::{ok, place_intent, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::Resolver;
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::Ts;

#[test]
fn a_new_snapshot_sees_the_commit_across_threads() {
    let core = Arc::new(ok(Core::open(RecKv::new())));
    let handle = ok(spawn_commit_thread(Arc::clone(&core)));

    // W1 commits first; W2's intent is still Pending when the reader runs.
    let w1 = core.begin(Isolation::ReadCommitted);
    place_intent(&core, &w1, b"/t/1/r", b"one");
    let w2 = core.begin(Isolation::RepeatableRead);
    let w2_id = w2.id;
    place_intent(&core, &w2, b"/t/1/s", b"two");

    let ts1 = ok(core.commit(w1, SyncCommit::On));
    let core2 = Arc::clone(&core);
    let reader = std::thread::spawn(move || -> nucleus_txn::TxnId {
        // A fresh session: its snapshot is taken now, after the ack.
        let snap = core2.registry.take_snapshot();
        let s = snap.ts();
        assert!(s >= ts1, "I-ACK: the snapshot is at or past the commit");
        let view = core2.open_view();
        let reader = core2.begin(Isolation::ReadCommitted);
        let ctx = ReadCtx {
            txn: reader.id,
            snapshot: s,
            stmt_seq: 1,
        };
        // The committed intent reads through the §4 intent rule (it is not
        // resolved yet); the Pending one is invisible.
        assert_eq!(
            ok(read_key(&core2, &view, b"/t/1/r", &ctx, &mut NoSsi)),
            Some(b"one".to_vec()),
            "read-your-commits across sessions"
        );
        assert_eq!(
            ok(read_key(&core2, &view, b"/t/1/s", &ctx, &mut NoSsi)),
            None,
            "a Pending foreign intent is invisible"
        );
        reader.id
    });
    let reader_id = reader.join().expect("reader");
    assert!(reader_id.epoch > 0);

    // A snapshot from *before* the commit must not see it (no snapshot
    // taken before ts1 exists here, so register at the boot ts: 0 values).
    // Instead: resolve and read the version at exactly ts1.
    let ts2 = ok(core.commit(w2, SyncCommit::Off));
    assert!(ts2 > ts1);
    common::wait_released(&core, w2_id);
    ok(Resolver::run_once(&core));
    let view = core.open_view();
    let ctx = ReadCtx {
        txn: core.begin(Isolation::ReadCommitted).id,
        snapshot: ts1,
        stmt_seq: 1,
    };
    assert_eq!(
        ok(read_key(&core, &view, b"/t/1/r", &ctx, &mut NoSsi)),
        Some(b"one".to_vec()),
        "the resolved version is readable at its commit ts"
    );
    drop(view);
    ok(handle.shutdown());
    let _ = Ts::ZERO;
}
