//! I-SER bank test (threads): accounts x and y with the rule `x + y >= 0`.
//! Each round resets x = 1, y = 0; 8 threads each run a txn that reads both
//! accounts and withdraws 1 from its own account (x for even threads, y for
//! odd) if the rule allows, retrying on 40001. A barrier after the first
//! reads makes every round a write-skew race. Under SERIALIZABLE the rule
//! holds after every round; the same workload under REPEATABLE READ breaks
//! it (so the test can fail). The commit thread and the retention thread
//! run in the background. Mutant: the dangerous-structure check disabled
//! (every pre-commit passes).

mod ssi_support;

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{spawn_commit_thread_with, CommitConfig, SyncCommit};
use nucleus_txn::read::{read_key, NoSsi};
use nucleus_txn::ssi::{spawn_retention, Ssi, SsiStats};
use nucleus_txn::txn::Isolation;
use nucleus_txn::visibility::ReadCtx;
use nucleus_txn::write::{RowOp, RowOutcome, StmtCtx, UniqueRule};
use nucleus_txn::{Ts, TxnError};
use ssi_support::NoEpq;

const THREADS: usize = 8;

fn parse(v: Option<Vec<u8>>) -> i64 {
    let v = v.expect("account exists");
    String::from_utf8(v).expect("utf8").parse().expect("number")
}

fn rc_set(core: &Core<MemKv>, x: i64, y: i64, insert: bool) {
    let txn = core.begin(Isolation::ReadCommitted);
    let seq = txn.next_seq().expect("seq");
    let s = core.visible_ts();
    for (k, v) in [(&b"x"[..], x), (&b"y"[..], y)] {
        let value = v.to_string().into_bytes();
        if insert {
            core.insert_key(
                &txn,
                k,
                None,
                value,
                StmtCtx::new(s, seq, seq),
                UniqueRule::Unique { same_row: None },
            )
            .expect("insert");
        } else {
            let out = core
                .row_op(
                    &txn,
                    k,
                    None,
                    RowOp::Update {
                        value,
                        key_cols_changed: false,
                    },
                    StmtCtx::new(s, seq, seq),
                    &mut NoEpq,
                )
                .expect("reset");
            assert_eq!(out, RowOutcome::Applied);
        }
    }
    core.commit(txn, SyncCommit::On).expect("rc commit");
}

fn rc_sum(core: &Core<MemKv>) -> i64 {
    let txn = core.begin(Isolation::ReadCommitted);
    let snap = core.registry.take_snapshot();
    let ctx = ReadCtx {
        txn: txn.id,
        snapshot: snap.ts(),
        stmt_seq: txn.next_seq().expect("seq"),
    };
    let view = core.open_view();
    let x = parse(read_key(core, &view, b"x", &ctx, &mut NoSsi).expect("x"));
    let y = parse(read_key(core, &view, b"y", &ctx, &mut NoSsi).expect("y"));
    drop(view);
    core.commit(txn, SyncCommit::On).expect("rc read commit");
    x + y
}

/// One withdrawal attempt. `Ok(())` when committed (with or without a
/// write), `Err(40001)` to retry.
fn attempt(
    core: &Core<MemKv>,
    ssi: &Ssi,
    iso: Isolation,
    me: usize,
    barrier: Option<&Barrier>,
) -> Result<(), TxnError> {
    let txn = core.begin(iso);
    let guard = if iso == Isolation::Serializable {
        ssi.begin(core, &txn, false)?
    } else {
        core.registry.take_snapshot()
    };
    let s = guard.ts();
    let ctx = ReadCtx {
        txn: txn.id,
        snapshot: s,
        stmt_seq: txn.next_seq()?,
    };
    let (x, y) = if iso == Isolation::Serializable {
        (
            parse(ssi.read_key(core, txn.id, b"x", &ctx)?),
            parse(ssi.read_key(core, txn.id, b"y", &ctx)?),
        )
    } else {
        let view = core.open_view();
        (
            parse(read_key(core, &view, b"x", &ctx, &mut NoSsi)?),
            parse(read_key(core, &view, b"y", &ctx, &mut NoSsi)?),
        )
    };
    if let Some(b) = barrier {
        b.wait();
    }
    if x + y > 0 {
        // Statement start: a doomed txn stops here (§8.4).
        let res = ssi.check_doomed(txn.id).and_then(|()| {
            let (key, old) = if me & 1 == 0 { (b"x", x) } else { (b"y", y) };
            let seq = txn.next_seq()?;
            core.row_op(
                &txn,
                key,
                None,
                RowOp::Update {
                    value: (old - 1).to_string().into_bytes(),
                    key_cols_changed: false,
                },
                StmtCtx::new(s, seq, seq),
                &mut NoEpq,
            )
        });
        match res {
            Ok(RowOutcome::Applied) => {}
            Ok(other) => panic!("unexpected row outcome {other:?}"),
            Err(e) => {
                core.abort(txn)?;
                return Err(e);
            }
        }
    }
    // commit_submit aborts the txn itself on a pre-commit 40001.
    let r = core.commit(txn, SyncCommit::On).map(|_: Ts| ());
    drop(guard);
    r
}

/// Runs `rounds` rounds; returns (txn attempts, rounds whose sum went
/// negative).
fn run(core: &Arc<Core<MemKv>>, ssi: &Arc<Ssi>, iso: Isolation, rounds: usize) -> (usize, usize) {
    let mut attempts = 0;
    let mut broken = 0;
    for _ in 0..rounds {
        rc_set(core, 1, 0, false);
        let barrier = Barrier::new(THREADS);
        let counts: Vec<usize> = std::thread::scope(|sc| {
            let hs: Vec<_> = (0..THREADS)
                .map(|me| {
                    let barrier = &barrier;
                    sc.spawn(move || {
                        let mut n = 0;
                        let mut first = true;
                        loop {
                            n += 1;
                            let b = if first { Some(barrier) } else { None };
                            first = false;
                            match attempt(core, ssi, iso, me, b) {
                                Ok(()) => return n,
                                Err(TxnError::SerializationFailure) => continue,
                                Err(e) => panic!("unexpected error {e:?}"),
                            }
                        }
                    })
                })
                .collect();
            hs.into_iter().map(|h| h.join().expect("thread")).collect()
        });
        attempts += counts.iter().sum::<usize>();
        let sum = rc_sum(core);
        if sum < 0 {
            broken += 1;
        }
        if iso == Isolation::Serializable {
            assert!(sum >= 0, "I-SER violated: x + y = {sum}");
        }
    }
    (attempts, broken)
}

#[test]
fn bank_rule_holds_under_serializable_and_breaks_under_rr() {
    let started = Instant::now();
    let core = Arc::new(Core::open(MemKv::new()).expect("open"));
    let ssi = Ssi::install(&core);
    let commit = spawn_commit_thread_with(
        Arc::clone(&core),
        CommitConfig::new().with_observer(ssi.commit_observer()),
    )
    .expect("commit thread");
    let retention = spawn_retention(
        Arc::clone(&core),
        Arc::clone(&ssi),
        Duration::from_millis(2),
    );
    rc_set(&core, 1, 0, true);

    let (ser_attempts, ser_broken) = run(&core, &ssi, Isolation::Serializable, 260);
    assert!(ser_attempts >= 2000, "only {ser_attempts} SER txns");
    assert_eq!(ser_broken, 0);

    let (_, rr_broken) = run(&core, &ssi, Isolation::RepeatableRead, 10);
    assert!(
        rr_broken >= 1,
        "RR never broke the rule: the test cannot fail"
    );

    retention.stop().expect("retention");
    ssi.run_retention(&core);
    assert_eq!(
        ssi.stats(),
        SsiStats {
            entries: 0,
            sireads: 0,
            writers: 0
        }
    );
    commit.shutdown().expect("shutdown");
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "took {:?}",
        started.elapsed()
    );
}
