//! C-T1b tests: the txn handle (§1, §5.5) — seq monotonicity, the write-set
//! log (one entry per layer pushed or modified), `count_placement`, the
//! cancel flag, and the read-only fast path of `Core::commit` (§3: no writes
//! and not SERIALIZABLE commits without a ts).

mod common;

use common::{ok, place_intent, RecKv};
use nucleus_txn::boot::Core;
use nucleus_txn::commit::SyncCommit;
use nucleus_txn::status::Remembered;
use nucleus_txn::txn::Isolation;
use nucleus_txn::{Ts, TxnStatus};

#[test]
fn next_seq_is_strictly_increasing() {
    let core = ok(Core::open(RecKv::new()));
    let txn = core.begin(Isolation::ReadCommitted);
    assert_eq!(txn.next_seq(), Ok(1));
    assert_eq!(txn.next_seq(), Ok(2));
    // `ROLLBACK TO` never gives a seq back (§5.5); new commands keep going
    // up from the same counter.
    txn.log_write(ok(txn.next_seq()), b"/t/1/r", None);
    assert_eq!(txn.next_seq(), Ok(4));
    assert_eq!(txn.seq(), 4);
}

#[test]
fn log_write_records_once_per_seq_and_key() {
    let txn = ok(Core::open(RecKv::new())).begin(Isolation::RepeatableRead);
    txn.log_write(1, b"/t/1/a", None);
    txn.log_write(1, b"/t/1/a", None); // same layer modified in place: once per (s,k)
    txn.log_write(1, b"/t/1/b", None); // same command, other key
    txn.log_write(2, b"/t/1/a", None); // a new layer on the same key
    assert_eq!(
        txn.write_set(),
        vec![
            (1, b"/t/1/a".to_vec(), None),
            (1, b"/t/1/b".to_vec(), None),
            (2, b"/t/1/a".to_vec(), None),
        ]
    );
    assert_eq!(
        txn.write_set_keys(),
        vec![(b"/t/1/a".to_vec(), None), (b"/t/1/b".to_vec(), None)],
        "distinct keys (with their latch prefixes) in first-written order"
    );
}

#[test]
fn count_placement_counts_before_the_write() {
    let core = ok(Core::open(RecKv::new()));
    let txn = core.begin(Isolation::ReadCommitted);
    let entry = core.status.entry(txn.id);
    assert!(entry.is_some());
    assert_eq!(entry.map(|e| e.intent_count), Some(0));
    ok(core.count_placement(&txn));
    ok(core.count_placement(&txn));
    assert_eq!(core.status.entry(txn.id).map(|e| e.intent_count), Some(2));
}

#[test]
fn cancel_flag_is_settable_from_another_thread() {
    let core = ok(Core::open(RecKv::new()));
    let txn = core.begin(Isolation::ReadCommitted);
    let handle = txn.cancel_handle();
    assert!(!txn.is_cancelled());
    let t = std::thread::spawn(move || {
        handle.cancel();
    });
    t.join().expect("cancel thread");
    assert!(txn.is_cancelled());
    assert!(txn.cancel_handle().is_cancelled());
}

#[test]
fn read_only_txn_commits_without_a_ts_or_a_write() {
    let kv = RecKv::new();
    let core = ok(Core::open(kv.clone()));
    let writes_before = kv.log().len();
    let txn = core.begin(Isolation::ReadCommitted);
    let txn_id = txn.id;
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert_eq!(ts, Ts::ZERO, "no writes and not SER: no ts (§3)");
    // No record, no sync, nothing written past the boot writes.
    assert_eq!(kv.log().len(), writes_before, "wrote nothing");
    // Released immediately: truncation is possible with no views open.
    let entry = core.status.entry(txn_id);
    assert_eq!(entry.map(|e| e.released), Some(true));
    assert!(ok(core.truncate_status(txn_id)));
    assert_eq!(core.status.lookup_remembered(txn_id), Remembered::Ended);
}

#[test]
fn read_only_serializable_txn_still_takes_a_ts() {
    // SERIALIZABLE read-only txns go through the pipeline (§8: commit
    // ordering is what SSI needs), so they get a real ts.
    let core = ok(Core::open(RecKv::new()));
    let core = std::sync::Arc::new(core);
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(
        std::sync::Arc::clone(&core),
    ));
    let txn = core.begin(Isolation::Serializable);
    let txn_id = txn.id;
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO);
    assert_eq!(
        core.status.entry(txn_id).map(|e| e.status),
        Some(TxnStatus::Committed(ts))
    );
    ok(handle.shutdown());
}

#[test]
fn a_written_txn_commits_through_the_pipeline() {
    let core = ok(Core::open(RecKv::new()));
    let core = std::sync::Arc::new(core);
    let handle = ok(nucleus_txn::commit::spawn_commit_thread(
        std::sync::Arc::clone(&core),
    ));
    let txn = core.begin(Isolation::ReadCommitted);
    let txn_id = txn.id;
    place_intent(&core, &txn, b"/t/1/r", b"v");
    let ts = ok(core.commit(txn, SyncCommit::On));
    assert!(ts > Ts::ZERO);
    assert!(core.visible_ts() >= ts, "I-ACK: visible on return");
    assert_eq!(
        core.status.entry(txn_id).map(|e| e.status),
        Some(TxnStatus::Committed(ts))
    );
    // The ack (step 4) precedes step 5; poll for the release.
    common::note_committed(&core, txn_id);
    common::wait_released(&core, txn_id);
    assert_eq!(core.status.entry(txn_id).map(|e| e.released), Some(true));
    ok(handle.shutdown());
}
