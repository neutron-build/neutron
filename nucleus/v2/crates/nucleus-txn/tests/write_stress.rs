//! C-T2 stress test: 8 threads share 4 keys at RC and RR through the real
//! write path — UPDATE as an increment through EPQ, DELETE + re-insert,
//! `FOR UPDATE`, SAVEPOINT / ROLLBACK TO — committing or aborting at
//! random, with the commit thread and the resolver running. Lost-update
//! oracle (C-T2 rework 3): the **full committed history** of every key,
//! oldest first, must form a chain — each live value is the previous live
//! value + 1, or the reset value 0 right after a delete or re-insert — so
//! a lost update anywhere in the history fails, not only one hidden
//! behind a later reset. Afterwards: no intent remains, every surviving
//! status entry has count 0 (I-COUNT), no invariant error.
//!
//! Txns touch their keys in ascending order (there is no deadlock detector
//! until C-T2b, so the generator keeps a global lock order — as the §6
//! queues would otherwise hang on a real cycle).

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::encoding::{decode_version, intent_key};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::resolver::{spawn_background, Resolver};
use nucleus_txn::txn::{CancelHandle, Isolation};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{
    CommittedVersion, Epq, EpqDecision, RowOp, RowOutcome, StmtCtx, UniqueRule,
};
use nucleus_txn::{RowLockMode, Ts, TxnError};

const THREADS: usize = 8;
const KEYS: usize = 4;
const TXNS_PER_THREAD: usize = 260; // 8 × 260 = 2080 txns >= 2000
const WATCHDOG_AFTER: Duration = Duration::from_secs(15);

/// An EPQ that re-applies the op it was built for against the newest
/// version: an UPDATE increments the value it is given (or skips a
/// tombstone); a DELETE stays a delete; a lock re-locks. §5.2: the callback
/// computes *this statement's* op from `v` — the kind never changes, or the
/// stress oracle would disagree with the KV (a raced `FOR UPDATE` that EPQs
/// into an increment commits a value nobody counted).
struct IncrEpq(RowOp);
impl Epq for IncrEpq {
    fn recheck(&mut self, newest: &CommittedVersion) -> EpqDecision {
        match (&self.0, &newest.value) {
            (
                RowOp::Update {
                    key_cols_changed, ..
                },
                nucleus_txn::encoding::VersionValue::Live { payload, .. },
            ) => {
                let v = u64::from_be_bytes(payload.as_slice().try_into().expect("8-byte value"));
                EpqDecision::Apply(RowOp::Update {
                    value: (v + 1).to_be_bytes().to_vec(),
                    key_cols_changed: *key_cols_changed,
                })
            }
            (RowOp::Delete, nucleus_txn::encoding::VersionValue::Live { .. }) => {
                EpqDecision::Apply(RowOp::Delete)
            }
            (RowOp::Lock(m), nucleus_txn::encoding::VersionValue::Live { .. }) => {
                EpqDecision::Apply(RowOp::Lock(*m))
            }
            (_, nucleus_txn::encoding::VersionValue::Tombstone { .. }) => EpqDecision::Skip,
        }
    }
}

struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 33
    }
}

/// Runs one txn. `Ok(None)` = randomly aborted; `Ok(Some(()))` = committed.
fn run_txn(
    core: &Arc<Core<MemKv>>,
    iso: Isolation,
    rng: &mut Rng,
    cancels: &Arc<Mutex<Vec<CancelHandle>>>,
) -> Result<Option<()>, TxnError> {
    let txn = core.begin(iso);
    cancels.lock().expect("cancels").push(txn.cancel_handle());
    // 1-4 ops over a sorted key subset: (key, kind) with kind 0=Inc, 1=DelRes,
    // 2=FOR UPDATE, 3=Savepoint/Rollback (a dropped increment).
    let n_ops = 1 + (rng.next() as usize) % 4;
    let mut ops: Vec<(usize, u8)> = Vec::new();
    for _ in 0..n_ops {
        let k = (rng.next() as usize) % KEYS;
        let kind = (rng.next() as usize) % 8;
        let kind = if kind < 4 {
            0 // increments dominate
        } else if kind < 6 {
            1
        } else if kind < 7 {
            2
        } else {
            3
        };
        ops.push((k, kind as u8));
    }
    ops.sort();
    ops.dedup_by_key(|(k, _)| *k);

    let snap = core.registry.take_snapshot();
    let s = snap.ts();
    // On a §5 error the txn is aborted before the error surfaces: a leaked
    // pending intent would block every later txn on its keys (there is no
    // deadlock detector until C-T2b).
    let r = run_ops(core, &txn, s, &ops);
    if let Err(e) = r {
        drop(snap);
        core.abort(txn).expect("abort the failed txn");
        return Err(e);
    }
    drop(snap);
    if rng.next().is_multiple_of(8) {
        core.abort(txn)?;
        return Ok(None);
    }
    let sync = if rng.next().is_multiple_of(2) {
        SyncCommit::On
    } else {
        SyncCommit::Off
    };
    core.commit(txn, sync)?;
    Ok(Some(()))
}

fn run_ops(
    core: &Arc<Core<MemKv>>,
    txn: &nucleus_txn::txn::Txn,
    s: Ts,
    ops: &[(usize, u8)],
) -> Result<(), TxnError> {
    for &(k, kind) in ops {
        let key = format!("/t/1/k{k}").into_bytes();
        let seq = txn.next_seq()?;
        let ctx = StmtCtx::new(s, seq, seq);
        match kind {
            0 => {
                // UPDATE v = v + 1, computed from the snapshot read (RC's
                // EPQ recomputes from the newest when someone raced). A
                // missing base is a virgin key: the increment starts at 1
                // (the oracle folds `None -> Some(0) + 1`), never 0.
                let base = read_at(core, &key, s, txn.id)?;
                let v = base.map_or(1u64, |b| b + 1);
                let op = RowOp::Update {
                    value: v.to_be_bytes().to_vec(),
                    key_cols_changed: false,
                };
                core.row_op(txn, &key, None, op.clone(), ctx, &mut IncrEpq(op))?;
            }
            1 => {
                // DELETE + re-insert at 0.
                core.row_op(
                    txn,
                    &key,
                    None,
                    RowOp::Delete,
                    ctx.clone(),
                    &mut IncrEpq(RowOp::Delete),
                )?;
                core.insert_key(
                    txn,
                    &key,
                    None,
                    0u64.to_be_bytes().to_vec(),
                    ctx,
                    UniqueRule::Unique { same_row: None },
                )?;
            }
            2 => {
                // FOR UPDATE: no effect.
                core.row_op(
                    txn,
                    &key,
                    None,
                    RowOp::Lock(RowLockMode::Update),
                    ctx,
                    &mut IncrEpq(RowOp::Lock(RowLockMode::Update)),
                )?;
            }
            _ => {
                // SAVEPOINT; an increment inside it; ROLLBACK TO drops it.
                let sp = txn.savepoint()?;
                let seq2 = txn.next_seq()?;
                let ctx2 = StmtCtx::new(s, seq2, seq2);
                let base = read_at(core, &key, s, txn.id)?;
                let v = base.map_or(1u64, |b| b + 1);
                let op = RowOp::Update {
                    value: v.to_be_bytes().to_vec(),
                    key_cols_changed: false,
                };
                let out = core.row_op(txn, &key, None, op.clone(), ctx2, &mut IncrEpq(op))?;
                core.rollback_to(txn, sp)?;
                debug_assert_eq!(out, RowOutcome::Applied);
            }
        }
    }
    Ok(())
}

fn read_at(
    core: &Arc<Core<MemKv>>,
    key: &[u8],
    s: Ts,
    reader: nucleus_txn::TxnId,
) -> Result<Option<u64>, TxnError> {
    let view = core.open_view();
    let v = read_key(
        core,
        &view,
        key,
        &ReadCtx {
            txn: reader,
            snapshot: s,
            stmt_seq: 1,
        },
        &mut NoSsi,
    )?;
    drop(view);
    Ok(v.map(|b| u64::from_be_bytes(b.as_slice().try_into().expect("8-byte value"))))
}

#[test]
fn stress_write_path_lost_update_oracle() {
    let core = Arc::new(Core::open(MemKv::new()).expect("core"));
    core.set_row_locks(common::TestRowLocks::new());
    let commit_handle = spawn_commit_thread(Arc::clone(&core)).expect("commit thread");
    let bg = spawn_background(Arc::clone(&core)).expect("resolver");
    let cancels: Arc<Mutex<Vec<CancelHandle>>> = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();

    let (wd_tx, wd_rx) = std::sync::mpsc::channel::<()>();
    let watchdog = {
        let cancels = Arc::clone(&cancels);
        std::thread::spawn(move || {
            if wd_rx.recv_timeout(WATCHDOG_AFTER).is_err() {
                for c in cancels.lock().expect("cancels").iter() {
                    c.cancel();
                }
            }
        })
    };

    let mut writers = Vec::new();
    for t in 0..THREADS {
        let core = Arc::clone(&core);
        let cancels = Arc::clone(&cancels);
        let mut rng = Rng(0x9e3779b97f4a7c15 ^ (t as u64 + 101));
        writers.push(std::thread::spawn(move || {
            for _ in 0..TXNS_PER_THREAD {
                let iso = if rng.next().is_multiple_of(2) {
                    Isolation::ReadCommitted
                } else {
                    Isolation::RepeatableRead
                };
                // RR serialization failures are retried in a fresh txn.
                let mut attempt = 0;
                loop {
                    attempt += 1;
                    if attempt > 64 {
                        panic!("a txn retried 64 times without succeeding");
                    }
                    match run_txn(&core, iso, &mut rng, &cancels) {
                        Ok(_) => break,
                        Err(TxnError::SerializationFailure) => continue,
                        Err(TxnError::UniqueViolation) => continue, // a racing re-insert
                        Err(TxnError::QueryCanceled) => {
                            panic!("a writer was cancelled (stuck?): watchdog fired")
                        }
                        Err(e) => panic!("unexpected error: {e:?}"),
                    }
                }
            }
        }));
    }

    for w in writers.into_iter() {
        if let Err(e) = w.join() {
            std::panic::resume_unwind(e);
        }
    }
    let _ = wd_tx.send(());
    let _ = watchdog.join();
    ok_or(bg.stop());
    commit_handle.shutdown().expect("shutdown");
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "bounded run: {:?}",
        start.elapsed()
    );

    // Drain the jobs: no intent remains, and every surviving status entry
    // of a current-epoch writer has count 0 (I-COUNT).
    for _ in 0..500 {
        let n = Resolver::run_once(&core).expect("resolve");
        if n == 0 && count_intents(&core) == 0 {
            break;
        }
    }
    assert_eq!(
        count_intents(&core),
        0,
        "every intent resolved or discarded"
    );
    // Every current-epoch txn that still has an entry must be count 0.
    for id in core.status.truncation_candidates() {
        if let Some(e) = core.status.entry(id) {
            assert_eq!(e.intent_count, 0, "I-COUNT for {id:?}");
        }
    }

    // The lost-update oracle (rework 3): the full-history check.
    verify_full_history(&core);
}

const _: () = assert!(THREADS * TXNS_PER_THREAD >= 2000, "at least 2000 txns");

fn ok_or(r: Result<(), TxnError>) {
    if let Err(e) = r {
        panic!("resolver failed: {e:?}");
    }
}

fn count_intents(core: &Core<MemKv>) -> usize {
    let view = core.open_view();
    let rows = view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    );
    let mut n = 0;
    for row in rows {
        let (k, _) = row.expect("scan");
        if nucleus_txn::encoding::parse_key(&k)
            .is_some_and(|(_, e)| matches!(e, nucleus_txn::encoding::Entry::Intent))
        {
            n += 1;
        }
    }
    n
}

/// The full-history oracle (C-T2 rework 3): scan each key's committed
/// versions oldest first and walk the chain. Every version in this
/// workload is written by a committed txn's single net op on the key:
///
/// - an increment commits `previous live value + 1` — or `1` when the
///   statement read no row (a virgin or deleted key);
/// - a DELETE + re-insert commits the reset value `0` ("right after a
///   delete or re-insert"; over a live row or a tombstone alike);
/// - a tombstone (nothing in this generator writes a net delete, but the
///   walk handles one) resets the chain.
///
/// A lost update — RC placing the stale snapshot value without EPQ, or no
/// re-check after an EPQ pass — writes a value that is not the previous
/// live value + 1 and fails the chain, wherever in the history it lands.
fn verify_full_history(core: &Arc<Core<MemKv>>) {
    for k in 0..KEYS {
        let key = format!("/t/1/k{k}").into_bytes();
        let versions = versions_oldest_first(core, &key);
        let mut prev: Option<u64> = None;
        for (i, value) in versions.iter().enumerate() {
            match &value {
                nucleus_txn::encoding::VersionValue::Tombstone { .. } => prev = None,
                nucleus_txn::encoding::VersionValue::Live { payload, .. } => {
                    let val =
                        u64::from_be_bytes(payload.as_slice().try_into().expect("8-byte value"));
                    match prev {
                        Some(p) => assert!(
                            val == p + 1 || val == 0,
                            "key {key:?} version #{i}: {val} after live {p} — a lost update"
                        ),
                        None => assert!(
                            val == 0 || val == 1,
                            "key {key:?} version #{i}: {val} with no previous live value"
                        ),
                    }
                    prev = Some(val);
                }
            }
        }
    }
}

/// One key's committed versions, oldest first (§2.2: stored newest first).
fn versions_oldest_first(
    core: &Core<MemKv>,
    key: &[u8],
) -> Vec<nucleus_txn::encoding::VersionValue> {
    let view = core.open_view();
    let lo = intent_key(key);
    let hi = nucleus_txn::encoding::end_key(key);
    let mut versions = Vec::new();
    for row in view.scan(
        (
            std::ops::Bound::Included(lo.as_slice()),
            std::ops::Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (k, v) = row.expect("scan");
        if let Some((_, nucleus_txn::encoding::Entry::Version(_))) =
            nucleus_txn::encoding::parse_key(&k)
        {
            versions.push(decode_version(&v).expect("version"));
        }
    }
    drop(view);
    versions.reverse();
    versions
}
