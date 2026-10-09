//! C-T1a concurrency smoke test: 8 threads taking snapshots, opening views
//! and removing intents under latches against a writer thread that commits,
//! plus truncation attempts. No deadlock, no invariant error, bounded run.

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use nucleus_kv::{Batch, Durability, MemKv, OrderedKv};
use nucleus_txn::boot::Core;
use nucleus_txn::encoding::{decode_intent, encode_intent, intent_key};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::removal::{remove_intent, RemovalMode};
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::{Intent, Layer, LayerData, RowLockMode, Ts, TxnError, TxnId, TxnStatus};

fn ok<T, E: std::fmt::Debug>(r: Result<T, E>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

fn kverr(e: nucleus_kv::KvError) -> TxnError {
    TxnError::Kv(e.to_string())
}

fn lock_done(done: &Mutex<Vec<TxnId>>) -> MutexGuard<'_, Vec<TxnId>> {
    done.lock().unwrap_or_else(PoisonError::into_inner)
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

const WORKERS: usize = 8;
const WORKER_ITERS: usize = 300;
const WRITER_TXNS: usize = 100;

fn key_of(i: usize) -> Vec<u8> {
    format!("/t/1/r{i}").into_bytes()
}

#[test]
fn concurrent_snapshots_views_removals_and_truncation() {
    let core = ok(Core::open(MemKv::new()));
    core.advance_visible_ts(Ts(100));
    let core = Arc::new(core);
    // Writer txns that finished committing (candidates for truncation).
    let done: Arc<Mutex<Vec<TxnId>>> = Arc::new(Mutex::new(Vec::new()));
    let start = Instant::now();

    let writer = {
        let core = Arc::clone(&core);
        let done = Arc::clone(&done);
        std::thread::spawn(move || -> Result<(), TxnError> {
            for i in 0..WRITER_TXNS {
                let id = core.status.begin();
                core.status.note_intent_placed(id)?;
                let value = ok(encode_intent(&Intent {
                    txn: id,
                    layers: vec![Layer {
                        seq: 1,
                        data_seq: 1,
                        data: LayerData::Write {
                            value: format!("v{i}").into_bytes(),
                            key_changed: false,
                        },
                        lock: RowLockMode::NoKeyUpdate,
                    }],
                }));
                let mut batch = Batch::default();
                batch.put(intent_key(&key_of(i)), value);
                core.kv.write(batch, Durability::No).map_err(kverr)?;
                core.status.set_committed(id, Ts(200 + i as u64))?;
                core.status.mark_released(id)?;
                lock_done(&done).push(id);
            }
            Ok(())
        })
    };

    let mut workers = Vec::new();
    for w in 0..WORKERS {
        let core = Arc::clone(&core);
        let done = Arc::clone(&done);
        workers.push(std::thread::spawn(move || -> Result<(), TxnError> {
            let mut rng = Rng(0x9e3779b97f4a7c15 ^ (w as u64 + 1));
            let me = core.status.begin();
            for _ in 0..WORKER_ITERS {
                let i = (rng.next() as usize) % WRITER_TXNS;
                let key = key_of(i);
                match rng.next() % 5 {
                    0 => {
                        let snap = core.registry.take_snapshot();
                        assert!(snap.ts() <= Ts(100));
                        drop(snap);
                    }
                    1 => {
                        let t = Ts(1 + (rng.next() % 100));
                        let reg = core.registry.register_at(t)?;
                        assert_eq!(reg.ts(), t);
                    }
                    2 | 3 => {
                        // Read through a registered view: I-TRUNC says the
                        // status of an intent visible in a view cannot have
                        // been truncated (the view holds the counter back).
                        let view = core.open_view();
                        read_key(
                            &core,
                            view.as_snap(),
                            &key,
                            &ReadCtx {
                                txn: me,
                                snapshot: Ts(100),
                                stmt_seq: 1,
                            },
                            &mut NoSsi,
                        )?;
                    }
                    _ => {
                        // The §5.1 inline-removal shape: read the intent and
                        // its owner's status, then remove under the latch.
                        let owner = core
                            .kv
                            .get_latest(&intent_key(&key))
                            .map_err(kverr)
                            .and_then(|raw| match raw {
                                Some(v) => Ok(Some(decode_intent(&v)?.txn)),
                                None => Ok(None),
                            })?;
                        if let Some(owner) = owner {
                            match core.status.lookup_for_intent(owner)? {
                                TxnStatus::Committed(_) => {
                                    remove_intent(&core, &key, None, owner, RemovalMode::Resolve)?;
                                }
                                TxnStatus::Aborted => {
                                    remove_intent(&core, &key, None, owner, RemovalMode::Discard)?;
                                }
                                TxnStatus::Pending => {}
                            }
                        }
                        // And a truncation attempt on a finished txn.
                        let finished = lock_done(&done).last().copied();
                        if let Some(id) = finished {
                            core.truncate_status(id)?;
                        }
                    }
                }
            }
            Ok(())
        }));
    }

    match writer.join() {
        Ok(Ok(())) => {}
        Ok(Err(e)) => panic!("writer failed: {e:?}"),
        Err(_) => panic!("writer panicked"),
    }
    for (w, h) in workers.into_iter().enumerate() {
        match h.join() {
            Ok(Ok(())) => {}
            Ok(Err(e)) => panic!("worker {w} failed: {e:?}"),
            Err(_) => panic!("worker {w} panicked"),
        }
    }
    let elapsed = start.elapsed();
    assert!(elapsed < Duration::from_secs(10), "took {elapsed:?}");

    // No count may go negative (a removal without a placement).
    for i in 0..WRITER_TXNS {
        let id = lock_done(&done)[i];
        if let Some(e) = core.status.entry(id) {
            assert!(e.intent_count >= 0, "count underflow for {id:?}");
        }
    }
}
