//! C-R0bb test: per-id sequence state — a reservation (or a first-use
//! boot) parked inside its synced `/sys/seq/{id}` write blocks **only
//! callers of that same id**. Callers of every other id — cached or fresh
//! — must run to completion while the first is parked mid-fsync. This is
//! rule R3 of `rt.rs` at its sharpest edge: the per-id slot mutex is the
//! only lock held across the fsync, so it is the only thing anyone can
//! wait on.
//!
//! Deterministic: the gate parks the parked caller, the assertions stand
//! on the gate's counters and `recv_timeout`s, and every failure path
//! releases the gate first, so the test never hangs.

mod r0b_support;

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use nucleus_txn::boot::Core;
use nucleus_txn::seq::sys_seq_key;

use r0b_support::{ok, GateKv};

/// Every bounded wait in this test (the mpsc `recv_timeout`s). Generous:
/// correct code answers in microseconds; only a lost R3 can consume it.
const RECV_TIMEOUT: Duration = Duration::from_secs(5);

/// The parked-caller margin: extra safety only, never the
/// synchronisation (the C-T7r3 rule of the gate).
const MARGIN: Duration = Duration::from_millis(100);

/// Thread A parks inside id 5's creation write (the gate holds the 1st
/// write of `/sys/seq/5`). While it is parked:
///
/// - a cached `seq_next(9)` (warmed before the gate could fire) delivers
///   `2` — no lock it needs is held across A's fsync;
/// - a fresh `seq_next(11)` (its own writes are not gated) delivers `1`;
/// - thread B's `seq_next(5)` — the **same** id — must not reach the KV:
///   it waits on id 5's slot mutex, exactly as the persist-before-handout
///   rule requires (`seq_rework.rs` pins the value-distinctness side).
///
/// After the release, A and B draw 1 and 2 of id 5's first block in some
/// order, and the whole block cost one reservation.
#[test]
fn paused_reservation_blocks_only_its_own_id() {
    let kv = GateKv::new(sys_seq_key(5), 1);
    let core = Arc::new(ok(Core::open(kv.clone())));
    core.seq_set_block_for_tests(4);

    // Warm id 9 before the gate can fire: with block 4, its second value
    // is cached (no KV write), so the seq_next(9) below measures lock
    // coupling, not its own reservation.
    assert_eq!(ok(core.seq_next(9)), 1, "warm id 9 with its first value");

    // A parks inside id 5's creation write (write 1 of the gated key).
    let core_a = Arc::clone(&core);
    let handle_a = thread::spawn(move || core_a.seq_next(5));
    if !kv.wait_until_parked(1) {
        kv.release();
        panic!("thread A never parked inside id 5's creation write");
    }

    // A cached nextval of another id must complete while A is parked.
    let (tx9, rx9) = mpsc::channel();
    let core9 = Arc::clone(&core);
    let handle9 = thread::spawn(move || {
        let _ = tx9.send(core9.seq_next(9));
    });
    let v9 = match rx9.recv_timeout(RECV_TIMEOUT) {
        Ok(v) => ok(v),
        Err(_) => {
            kv.release();
            panic!(
                "cached seq_next(9) did not answer within {RECV_TIMEOUT:?} while id 5's \
                 creation write was parked: a lock it needs is held across an fsync (R3)"
            );
        }
    };
    assert_eq!(v9, 2, "id 9's second value is cached, not reserved");

    // A fresh id whose own writes are not gated must boot and answer.
    let (tx11, rx11) = mpsc::channel();
    let core11 = Arc::clone(&core);
    let handle11 = thread::spawn(move || {
        let _ = tx11.send(core11.seq_next(11));
    });
    let v11 = match rx11.recv_timeout(RECV_TIMEOUT) {
        Ok(v) => ok(v),
        Err(_) => {
            kv.release();
            panic!(
                "fresh seq_next(11) did not answer within {RECV_TIMEOUT:?} while id 5's \
                 creation write was parked: a lock it needs is held across an fsync (R3)"
            );
        }
    };
    assert_eq!(v11, 1, "id 11's first value");
    let _ = handle9.join();
    let _ = handle11.join();

    // B, calling the SAME id, must not reach the KV while A is parked: it
    // waits on id 5's slot mutex. The margin is extra safety only; the
    // synchronisation is A still being parked, so arrivals() can never
    // move past 1 on correct code, however long B waits.
    let core_b = Arc::clone(&core);
    let handle_b = thread::spawn(move || core_b.seq_next(5));
    let b_parked_early = kv.wait_until_parked_margin(2, MARGIN);
    assert!(
        !b_parked_early && kv.arrivals() == 1,
        "thread B reached /sys/seq/5 while A was parked in id 5's synced write: \
          the per-id mutex is not held across the boot/reservation (arrivals = {})",
        kv.arrivals()
    );

    kv.release();
    let a = ok(handle_a.join().expect("thread A"));
    let b = ok(handle_b.join().expect("thread B"));
    let mut got = [a, b];
    got.sort_unstable();
    assert_eq!(
        got,
        [1, 2],
        "A and B must hand out 1 and 2 of id 5's first block, got {a} and {b}"
    );
    assert_eq!(
        core.seq_reservations_for_tests(5),
        Some(1),
        "one reservation covers both values of id 5's first block"
    );
}
