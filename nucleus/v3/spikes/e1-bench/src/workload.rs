//! Shared plumbing of the W1–W6 workloads (card X-1b Work 2): the
//! batched preload, the per-workload report ids, and the seed scheme
//! that makes every arm face the identical key stream.
//!
//! Invariants owned here:
//! - WL-PRELOAD: every preload is the same sequence on every arm — RC
//!   txns of `preload_batch` [`put`](e1_mvcc::api::TxnEngine::put)s in
//!   ascending key order, committed one by one (card Read points at v2's
//!   `ArbiterCycles::preload` insert/commit sequence for arm (a)'s
//!   equivalent).
//! - WL-SEED: the seed of (workload, run, thread, purpose) is derived
//!   only from those inputs — never from the arm — so (b) and (c) draw
//!   the same keys (U-DET of [`crate::util`]).
//! - WL-ERR: an engine error inside a measured loop stops the workload
//!   as a [`BenchError`]; it is never swallowed and never retried.

use e1_mvcc::api::{Isolation, TxnEngine, TxnErr, Wait};

use crate::BenchError;
use crate::util::{Rng, key_of, value_of};

/// Map a trait error into a bench error with context (WL-ERR).
pub fn err(ctx: &str, e: TxnErr) -> BenchError {
    BenchError(format!("{ctx}: {e}"))
}

/// A blocking lock wait of `d` (the measured loops never intend to give
/// up; timeouts surface as workload errors rather than silent skips).
pub fn block(d: std::time::Duration) -> Wait {
    Wait::Block(d)
}

/// The seed of one (workload, run, thread, purpose) stream (WL-SEED).
/// The salt separates independent streams (keys vs values) inside one
/// thread.
pub fn seed(workload: u64, run: u32, thread: usize, purpose: u64) -> u64 {
    // Distinct primes per component so the streams never collide for
    // the small counts this bench uses.
    workload.wrapping_mul(0x9E37_79B9_0000_0001)
        ^ (run as u64).wrapping_mul(0xBF58_476D_0000_0002)
        ^ (thread as u64).wrapping_mul(0x94D0_49BB_0000_0003)
        ^ purpose.wrapping_mul(0x2545_F491_0000_0004)
}

/// A fresh RNG for one measured loop (WL-SEED).
pub fn rng_for(workload: u64, run: u32, thread: usize, purpose: u64) -> Rng {
    Rng::new(seed(workload, run, thread, purpose))
}

/// Preload `keys` rows in `batch`-key RC txns at version `salt`
/// (WL-PRELOAD). Values are `value_len` bytes of [`value_of`].
pub fn preload<E: TxnEngine>(
    engine: &E,
    keys: u64,
    batch: u64,
    value_len: usize,
    salt: u64,
) -> Result<(), BenchError> {
    let batch = batch.max(1);
    let mut k0 = 0u64;
    while k0 < keys {
        let end = (k0 + batch).min(keys);
        let mut t = engine.begin(Isolation::ReadCommitted);
        for i in k0..end {
            engine
                .put(
                    &mut t,
                    &key_of(i),
                    &value_of(i, salt, value_len),
                    Wait::NoWait,
                )
                .map_err(|e| err(&format!("preload put key {i}"), e))?;
        }
        engine.commit(t).map_err(|e| err("preload commit", e))?;
        k0 = end;
    }
    Ok(())
}

/// One RC single-row update txn (the W1/W2 writer mix, WL-PRELOAD's
/// measured counterpart): `put(key, value)` then commit.
pub fn single_row_update<E: TxnEngine>(
    engine: &E,
    key: &[u8],
    value: &[u8],
    wait: Wait,
) -> Result<(), BenchError> {
    let mut t = engine.begin(Isolation::ReadCommitted);
    engine
        .put(&mut t, key, value, wait)
        .map_err(|e| err("update put", e))?;
    engine.commit(t).map_err(|e| err("update commit", e))?;
    Ok(())
}

/// The workload number used in seeds (matches the W<n> in the card).
pub const W_W1: u64 = 1;
/// W2's seed number.
pub const W_W2: u64 = 2;
/// W3/W4's seed number.
pub const W_W3: u64 = 3;
/// W5's seed number.
pub const W_W5: u64 = 5;
/// W6's seed number.
pub const W_W6: u64 = 6;
