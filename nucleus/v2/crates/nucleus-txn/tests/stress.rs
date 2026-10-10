//! C-T1b stress test (rework item 10): 16 writer threads share **8 keys**
//! (so `wait_on` really runs), each beginning a txn, placing an intent by
//! hand through the full §5.1 loop (latch, latest read, inline removal of
//! ended owners, wait on live ones — no give-up path), then committing or
//! aborting at random: 16 × 125 = 2000 txns. The resolver and truncation
//! run in the background and readers take snapshots throughout. Afterwards:
//! every committed write is readable at the right ts, no intent remains
//! after the jobs drain, every status is truncated, and no invariant error
//! occurred. Bounded run.

mod common;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant};

use common::ok;
use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread, SyncCommit};
use nucleus_txn::encoding::decode_intent;
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::removal::{remove_intent, RemovalMode};
use nucleus_txn::resolver::{spawn_background, Resolver};
use nucleus_txn::status::Remembered;
use nucleus_txn::txn::{CancelHandle, Isolation};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Ts, TxnError, TxnId, TxnStatus};

type Key = Vec<u8>;

const THREADS: usize = 16;
const TXNS_PER_THREAD: usize = 125; // 16 × 125 = 2000 txns
const SHARED_KEYS: usize = 8;

/// Past this, a still-stuck writer is cancelled so the test fails with a
/// message instead of hanging (a correct run never reaches it).
const WATCHDOG_AFTER: Duration = Duration::from_secs(15);

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

/// `(thread, txn, key-serial)` — unique per written value; the commit/abort
/// suffix keeps aborted and committed values disjoint.
fn tag_value(t: usize, txn: usize, k: usize, committed: bool) -> Vec<u8> {
    format!("v-{t}-{txn}-{k}-{}", if committed { "c" } else { "a" }).into_bytes()
}

/// The shared ledger: every key's committed history `Vec<(ts, value)>` (ts
/// ascending) and every aborted value (which must never be visible).
type History = Vec<(Ts, Vec<u8>)>;

#[derive(Default)]
struct Ledger {
    committed: Mutex<HashMap<Key, History>>,
    aborted: Mutex<Vec<(Key, Vec<u8>)>>,
}

/// Places an intent on `key` the way §5.1 will: the full loop — latch,
/// latest read, inline removal of an ended foreign owner, wait on a live
/// one, retry. There is no give-up path: every placement either lands or
/// the txn is cancelled and the writer errors out.
fn place_with_waits(
    core: &Arc<Core<MemKv>>,
    txn: &nucleus_txn::txn::Txn,
    seq: nucleus_txn::Seq,
    key: &[u8],
    value: &[u8],
) -> Result<(), TxnError> {
    let intent = nucleus_txn::Intent {
        txn: txn.id,
        layers: vec![nucleus_txn::Layer {
            seq,
            data_seq: seq,
            data: nucleus_txn::LayerData::Write {
                value: value.to_vec(),
                key_changed: false,
            },
            lock: nucleus_txn::RowLockMode::NoKeyUpdate,
        }],
    };
    let encoded = nucleus_txn::encoding::encode_intent(&intent)?;
    loop {
        let latch = core.latches.lock(key);
        match core.latest_get(&nucleus_txn::encoding::intent_key(key))? {
            None => {
                txn.log_write(seq, key, None);
                core.count_placement(txn)?;
                let mut batch = nucleus_kv::Batch::default();
                batch.put(nucleus_txn::encoding::intent_key(key), encoded.clone());
                core.write(batch, nucleus_kv::Durability::No)?;
                drop(latch);
                return Ok(());
            }
            Some(raw) => {
                let owner = decode_intent(&raw)?.txn;
                let (status, gen) = match core.status.lookup_remembered(owner) {
                    Remembered::Live(s, g) => (s, g),
                    Remembered::Ended => (TxnStatus::Aborted, 0),
                };
                let ended = matches!(status, TxnStatus::Aborted)
                    || matches!(status, TxnStatus::Committed(c) if c <= core.visible_ts());
                if ended {
                    // §5.1 inline removal under the held latch.
                    let mode = if matches!(status, TxnStatus::Committed(_)) {
                        RemovalMode::Resolve
                    } else {
                        RemovalMode::Discard
                    };
                    nucleus_txn::removal::remove_intent_under_latch(
                        core, key, None, owner, mode, &latch,
                    )?;
                    drop(latch);
                    continue;
                }
                // Pending or committed-not-visible: wait on the owner (§6).
                drop(latch);
                if core.wait_on(txn, owner, gen) == nucleus_txn::wait::WaitOutcome::Cancelled {
                    // Only the watchdog cancels; a wait that needed it is a
                    // bug, reported instead of hung.
                    return Err(TxnError::Invariant(format!(
                        "wait on {owner:?} was cancelled (stuck?)"
                    )));
                }
                continue;
            }
        }
    }
}

#[test]
fn stress_commit_abort_resolve_truncate_with_readers() {
    let core = Arc::new(ok(Core::open(MemKv::new())));
    let commit_handle = ok(spawn_commit_thread(Arc::clone(&core)));
    let bg = ok(spawn_background(Arc::clone(&core)));
    let ledger = Arc::new(Ledger::default());
    let all_ids: Arc<Mutex<Vec<TxnId>>> = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(AtomicBool::new(false));
    let writers_done = Arc::new(AtomicBool::new(false));
    let start = Instant::now();

    let key_of = |k: usize| -> Key { format!("/t/1/k{k}").into_bytes() };

    // The watchdog: a stuck writer is cancelled so the test fails with a
    // message instead of hanging. With correct wake paths it never fires.
    // Writers register their cancel handles; disarm on success.
    let cancels: Arc<Mutex<Vec<CancelHandle>>> = Arc::new(Mutex::new(Vec::new()));
    let (wd_tx, wd_rx) = mpsc::channel::<()>();
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
        let ledger = Arc::clone(&ledger);
        let all_ids = Arc::clone(&all_ids);
        let cancels = Arc::clone(&cancels);
        let mut rng = Rng(0x9e3779b97f4a7c15 ^ (t as u64 + 1));
        writers.push(std::thread::spawn(move || -> Result<(), TxnError> {
            for i in 0..TXNS_PER_THREAD {
                let txn = core.begin(Isolation::ReadCommitted);
                all_ids.lock().expect("ids").push(txn.id);
                cancels.lock().expect("cancels").push(txn.cancel_handle());
                let abort = rng.next().is_multiple_of(4);
                let k = (rng.next() as usize) % SHARED_KEYS;
                let key = key_of(k);
                let value = tag_value(t, i, k, !abort);
                let seq = txn.next_seq()?;
                place_with_waits(&core, &txn, seq, &key, &value)?;
                if abort {
                    core.abort(txn)?;
                    ledger.aborted.lock().expect("aborted").push((key, value));
                } else {
                    let sync = if rng.next().is_multiple_of(2) {
                        SyncCommit::On
                    } else {
                        SyncCommit::Off
                    };
                    let ts = core.commit(txn, sync)?;
                    assert!(ts > Ts::ZERO);
                    ledger
                        .committed
                        .lock()
                        .expect("committed")
                        .entry(key)
                        .or_default()
                        .push((ts, value));
                }
            }
            Ok(())
        }));
    }

    let mut readers = Vec::new();
    for r in 0..4 {
        let core = Arc::clone(&core);
        let ledger = Arc::clone(&ledger);
        let stop = Arc::clone(&stop);
        let writers_done = Arc::clone(&writers_done);
        let mut rng = Rng(0x0123_4567_89ab_cdef ^ (r as u64 + 7));
        readers.push(std::thread::spawn(move || -> Result<(), TxnError> {
            let me = core.begin(Isolation::ReadCommitted);
            let mut rounds = 0usize;
            while !stop.load(Ordering::SeqCst) || rounds < 25 {
                rounds += 1;
                let k = (rng.next() as usize) % SHARED_KEYS;
                let key = key_of(k);
                let snap = core.registry.take_snapshot();
                let s = snap.ts();
                let view = core.open_view();
                let got = read_key(
                    &core,
                    &view,
                    &key,
                    &ReadCtx {
                        txn: me.id,
                        snapshot: s,
                        stmt_seq: 1,
                    },
                    &mut NoSsi,
                )?;
                drop(view);
                drop(snap);
                if let Some(v) = got {
                    // A value read at S was committed at ts <= S, but the
                    // writer's ledger push follows its ack: wait for the
                    // push (bounded; once every writer joined, all pushes
                    // have landed) before judging.
                    let history = loop {
                        let history = ledger
                            .committed
                            .lock()
                            .expect("committed")
                            .get(&key)
                            .cloned()
                            .unwrap_or_default();
                        let found = history.iter().any(|(_, val)| *val == v);
                        if found
                            || writers_done.load(Ordering::SeqCst)
                            || start.elapsed() > Duration::from_secs(10)
                        {
                            break history;
                        }
                        std::thread::sleep(Duration::from_millis(2));
                    };
                    // The read value must be the newest committed at or
                    // below S (I-ACK + §4): no future commit is visible
                    // and nothing older can be the newest.
                    let newest = history
                        .iter()
                        .filter(|(ts, _)| *ts <= s)
                        .max_by_key(|(ts, _)| *ts)
                        .map(|(_, val)| val.clone());
                    assert_eq!(
                        newest.as_ref(),
                        Some(&v),
                        "key {key:?} at S={s:?}: newest visible history {history:?}"
                    );
                    // Values are globally unique, so an aborted value can
                    // never be the one read.
                    for (_, av) in ledger.aborted.lock().expect("aborted").iter() {
                        assert!(av != &v, "an aborted value became visible: {v:?}");
                    }
                }
                // Reader-side inline removal of ended owners, racing the
                // resolver (the G0-commit "two removers" shape).
                let raw = core.latest_get(&nucleus_txn::encoding::intent_key(&key))?;
                if let Some(raw) = raw {
                    let owner = decode_intent(&raw)?.txn;
                    match core.status.lookup_remembered(owner) {
                        Remembered::Live(TxnStatus::Committed(c), _) if c <= core.visible_ts() => {
                            remove_intent(&core, &key, None, owner, RemovalMode::Resolve)?;
                        }
                        Remembered::Live(TxnStatus::Aborted, _) => {
                            remove_intent(&core, &key, None, owner, RemovalMode::Discard)?;
                        }
                        _ => {}
                    }
                }
            }
            Ok(())
        }));
    }

    for (t, w) in writers.into_iter().enumerate() {
        if let Err(e) = w.join().expect("writer") {
            panic!("writer {t} failed: {e:?}");
        }
    }
    writers_done.store(true, Ordering::SeqCst);
    stop.store(true, Ordering::SeqCst);
    for (r, rd) in readers.into_iter().enumerate() {
        if let Err(e) = rd.join().expect("reader") {
            panic!("reader {r} failed: {e:?}");
        }
    }
    // Disarm the watchdog (all writers finished on their own).
    let _ = wd_tx.send(());
    let _ = watchdog.join();
    ok(bg.stop());
    ok(commit_handle.shutdown());
    assert!(
        start.elapsed() < Duration::from_secs(20),
        "bounded run: {:?}",
        start.elapsed()
    );

    // Drain the jobs: no intent remains.
    for _ in 0..200 {
        let n = ok(Resolver::run_once(&core));
        if n == 0 && count_intents(&core) == 0 {
            break;
        }
    }
    assert_eq!(
        count_intents(&core),
        0,
        "every intent resolved or discarded"
    );

    // Every committed write readable at the right ts.
    {
        let committed = ledger.committed.lock().expect("committed").clone();
        let reader = core.begin(Isolation::ReadCommitted);
        let view = core.open_view();
        for (key, history) in &committed {
            for (ts, value) in history {
                // At S = ts exactly the version written at ts is the newest
                // visible one (later commits are invisible, earlier ones
                // shadowed).
                let ctx = ReadCtx {
                    txn: reader.id,
                    snapshot: *ts,
                    stmt_seq: 1,
                };
                let got = ok(read_key(&core, &view, key, &ctx, &mut NoSsi));
                assert_eq!(
                    got.as_deref(),
                    Some(value.as_slice()),
                    "key {key:?} at {ts:?}: history {history:?}"
                );
            }
        }
    }

    // Truncate what the final reads' view held back, then require every
    // status entry gone: released, count 0, no views.
    for _ in 0..100 {
        let remaining = all_ids
            .lock()
            .expect("ids")
            .iter()
            .filter(|id| core.status.entry(**id).is_some())
            .count();
        if remaining == 0 {
            break;
        }
        ok(Resolver::run_once(&core));
    }
    for id in all_ids.lock().expect("ids").iter() {
        let entry = core.status.entry(*id);
        assert!(
            entry.is_none(),
            "{id:?} still has a status entry: {entry:?} (released + count 0 + no views must truncate)"
        );
    }
    assert_eq!(
        ok(Resolver::run_once(&core)),
        0,
        "queues drained: nothing left to do"
    );
    // And the volume really happened.
    assert!(
        all_ids.lock().expect("ids").len() >= 2000,
        "at least 2000 txns ran"
    );
}

/// Counts intent entries in the whole KV, excluding the engine's own
/// `/sys/` keys (ts_clock, gc and txn records also encode as intents;
/// they are not leftover user intents).
fn count_intents(core: &Core<MemKv>) -> usize {
    let view = core.open_view();
    let rows = view.scan(
        (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded),
        false,
    );
    let mut n = 0;
    for row in rows {
        let (k, _) = ok(row);
        if !k.starts_with(nucleus_txn::encoding::SYS_PREFIX)
            && nucleus_txn::encoding::parse_key(&k)
                .is_some_and(|(_, e)| matches!(e, nucleus_txn::encoding::Entry::Intent))
        {
            n += 1;
        }
    }
    n
}
