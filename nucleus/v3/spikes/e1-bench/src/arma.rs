//! Arm (a) — v2 today on `MemKv`, the reference for the single-row mix
//! (card X-1b Goal, Work 1–2; PLAN-v3 §4 E1′ "(a) v2 today on MemKv
//! (reference only)").
//!
//! This adapter drives the v2 crates exactly the way v2's own tests do
//! (card Read: `nucleus/v2/crates/nucleus-txn/tests/c_t2c_support/
//! mod.rs`, `ArbiterCycles::preload` / `Rig::update` — the
//! insert-or-update, commit and resolve sequence), with the commit
//! pipeline and the resolver running on their own threads the way
//! [`spawn_commit_thread`] and the §9.3 job runner run them in
//! production shape:
//!
//! - preload — per batch: `begin(RC)`, one `insert_key` per key at its
//!   own seq, `commit_submit` + `wait`;
//! - the W1 mix — per txn: `begin(RC)`, `next_seq`, `row_op(Update)`
//!   with an always-apply EPQ (the [`Epq`] double of the test support's
//!   `Fixed`), `commit_submit` + `wait`;
//! - the resolver job — `JobSet` + `ResolverJob`, woken by every
//!   release, drained once more at the end of a phase so the engine is
//!   quiescent before the next one.
//!
//! Invariants owned here:
//! - A-V2: nothing here edits or wraps v2 — the adapter only calls v2's
//!   public API (card Do-not: anything under `nucleus/v2/` is
//!   read-only).
//! - A-SAME: W1 runs with the same thread counts, key space, value
//!   length and seeds as arms (b)/(c) (B-SAME); bytes-per-txn is not
//!   reported because v2 exposes no applied-bytes counter — the report
//!   carries `null` rather than an estimate.

use std::sync::Arc;
use std::thread;
use std::time::Instant;

use nucleus_kv::MemKv;
use nucleus_txn::boot::Core;
use nucleus_txn::commit::{SyncCommit, spawn_commit_thread};
use nucleus_txn::jobs::runner::JobSet;
use nucleus_txn::jobs::{JobConfig, JobKind};
use nucleus_txn::resolver::{Resolver, ResolverJob};
use nucleus_txn::txn::{Isolation, Txn};
use nucleus_txn::write::{Epq, EpqDecision, RowOp, StmtCtx, UniqueRule};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{W_W1, rng_for};

/// The always-apply EPQ double of the v2 test support's `Fixed`: an
/// UPDATE whose quals cannot fail (A-V2 — the same shape
/// `ArbiterCycles::advance` uses).
struct ApplyFixed(RowOp);

impl Epq for ApplyFixed {
    fn recheck(&mut self, _newest: &nucleus_txn::write::CommittedVersion) -> EpqDecision {
        EpqDecision::Apply(self.0.clone())
    }
}

/// A v2 engine in bench shape: the core, its commit thread and its
/// resolver job (module docs). Dropping the value drops the [`JobSet`],
/// which shuts the commit thread and the resolver down in §9.3's order.
pub struct V2 {
    /// The shared core every worker drives.
    pub core: Arc<Core<MemKv>>,
    /// The job set owning the resolver and the commit thread handles.
    #[allow(dead_code)] // held for its Drop; read below is a lint nicety
    jobs: JobSet<MemKv>,
}

impl V2 {
    /// Boot the core, spawn the commit thread and start the resolver
    /// job (module docs).
    pub fn new() -> Result<V2, BenchError> {
        let core =
            Arc::new(Core::open(MemKv::new()).map_err(|e| BenchError::new("arm a Core::open", e))?);
        let handle = spawn_commit_thread(Arc::clone(&core))
            .map_err(|e| BenchError::new("arm a commit thread", e))?;
        let mut jobs = JobSet::new(Arc::clone(&core));
        jobs.start(
            Box::new(ResolverJob::new(Arc::clone(&core))),
            JobConfig::default_for(JobKind::Resolver),
        )
        .map_err(|e| BenchError::new("arm a resolver job", e))?;
        jobs.attach_commit_thread(handle);
        Ok(V2 { core, jobs })
    }

    /// One preload batch txn: `insert_key` per key, then a sync commit
    /// (module docs; A-V2).
    fn preload_batch(
        &self,
        k0: u64,
        end: u64,
        value_len: usize,
        salt: u64,
    ) -> Result<(), BenchError> {
        let t = self.core.begin(Isolation::ReadCommitted);
        for i in k0..end {
            let seq = t
                .next_seq()
                .map_err(|e| BenchError::new("arm a next_seq", e))?;
            let value = util::value_of(i, salt, value_len);
            self.core
                .insert_key(
                    &t,
                    &util::key_of(i),
                    None,
                    value,
                    StmtCtx::new(self.core.visible_ts(), seq, seq),
                    UniqueRule::Unique { same_row: None },
                )
                .map_err(|e| BenchError::new(&format!("arm a insert_key {i}"), e))?;
        }
        self.commit(t).map(|_| ())
    }

    /// Commit through the spawned pipeline and wait for the ack
    /// (`commit_submit` + `wait`, module docs).
    fn commit(&self, t: Txn) -> Result<nucleus_txn::Ts, BenchError> {
        let ticket = self
            .core
            .commit_submit(t, SyncCommit::On)
            .map_err(|e| BenchError::new("arm a commit_submit", e))?;
        ticket
            .wait()
            .map_err(|e| BenchError::new("arm a commit ack", e))
    }

    /// Drain the resolver until idle (module docs).
    fn drain(&self) -> Result<(), BenchError> {
        for _ in 0..2 {
            while Resolver::run_once(&self.core).map_err(|e| BenchError::new("arm a resolve", e))?
                > 0
            {}
        }
        Ok(())
    }

    /// Preload `keys` rows in `batch`-key txns (A-SAME mirrors
    /// [`crate::workload::preload`]).
    pub fn preload(
        &self,
        keys: u64,
        batch: u64,
        value_len: usize,
        salt: u64,
    ) -> Result<(), BenchError> {
        let batch = batch.max(1);
        let mut k0 = 0u64;
        while k0 < keys {
            let end = (k0 + batch).min(keys);
            self.preload_batch(k0, end, value_len, salt)?;
            k0 = end;
        }
        self.drain()
    }

    /// One W1 worker's loop: RC update txns on random preloaded keys
    /// (A-SAME).
    fn w1_worker(
        &self,
        keys: u64,
        txns: u64,
        value_len: usize,
        seed: u64,
    ) -> Result<(), BenchError> {
        let mut rng = util::Rng::new(seed);
        for n in 0..txns {
            let idx = rng.below(keys);
            let value = util::value_of(idx, n.wrapping_add(1), value_len);
            let t = self.core.begin(Isolation::ReadCommitted);
            let seq = t
                .next_seq()
                .map_err(|e| BenchError::new("arm a next_seq", e))?;
            let op = RowOp::Update {
                value,
                key_cols_changed: false,
            };
            self.core
                .row_op(
                    &t,
                    &util::key_of(idx),
                    None,
                    op.clone(),
                    StmtCtx::new(self.core.visible_ts(), seq, seq),
                    &mut ApplyFixed(op),
                )
                .map_err(|e| BenchError::new("arm a row_op", e))?;
            self.commit(t)?;
        }
        Ok(())
    }
}

/// Run W1 on arm (a) for one thread count and return its report object
/// (card Work 2; A-SAME — the same window and accounting as
/// [`crate::w1`], with `bytes_applied_per_txn: null`, see module docs).
pub fn w1(cfg: &Config, run: u32, threads: usize) -> Result<Json, BenchError> {
    let v2 = V2::new()?;
    v2.preload(cfg.preload_keys, cfg.preload_batch, cfg.value_len, 0)?;

    let total_txns = threads as u64 * cfg.w1_txns_per_thread;
    let tps = util::clock_ticks_per_sec();
    let cpu0 = util::cpu_micros(tps);
    let wall0 = Instant::now();

    thread::scope(|s| -> Result<(), BenchError> {
        let mut handles = Vec::new();
        for tid in 0..threads {
            let core = &v2;
            handles.push(s.spawn(move || {
                let seed = rng_for(W_W1, run, tid, 0).next_u64();
                core.w1_worker(
                    cfg.preload_keys,
                    cfg.w1_txns_per_thread,
                    cfg.value_len,
                    seed,
                )
            }));
        }
        for h in handles {
            h.join()
                .map_err(|_| BenchError("arm a W1 worker panicked".into()))??;
        }
        Ok(())
    })?;
    v2.drain()?;

    let wall = wall0.elapsed();
    let cpu1 = util::cpu_micros(tps);
    let cpu_us = cpu1.zip(cpu0).map(|(a, b)| a.saturating_sub(b));
    Ok(obj(vec![
        ("workload", "W1".into()),
        ("arm", "a (v2 MemKv)".into()),
        ("threads", threads.into()),
        ("keys", cfg.preload_keys.into()),
        ("txns", total_txns.into()),
        ("wall_s", util::secs(wall).into()),
        (
            "cpu_us_per_txn",
            cpu_us
                .map(|us| Json::Num(us as f64 / total_txns as f64))
                .unwrap_or(Json::Null),
        ),
        (
            "bytes_applied_per_txn",
            Json::Null, // A-SAME: v2 exposes no applied-bytes counter
        ),
        ("txns_per_s", (total_txns as f64 / util::secs(wall)).into()),
        (
            "cpu_us_per_txn_note",
            "process CPU (workers + commit thread + resolver job) / txns".into(),
        ),
        (
            "commit_pipeline",
            "spawned commit thread; resolver as a §9.3 job; sync commit on".into(),
        ),
    ]))
}

#[cfg(test)]
mod tests {
    use super::V2;
    use crate::util;
    use nucleus_txn::read::{NoSsi, read_key};
    use nucleus_txn::visibility::ReadCtx;

    /// A-V2/A-SAME smoke: preload through the adapter, update through
    /// the W1 shape, read back through v2's read path.
    #[test]
    fn arm_a_preload_update_read() {
        let v2 = V2::new().unwrap();
        v2.preload(16, 4, 24, 0).unwrap();
        let seed = util::Rng::new(1).next_u64();
        v2.w1_worker(16, 8, 24, seed).unwrap();
        v2.drain().unwrap();
        let snap = v2.core.registry.take_snapshot();
        let view = v2.core.open_view();
        let got = read_key(
            &v2.core,
            &view,
            &util::key_of(0),
            &ReadCtx {
                txn: nucleus_txn::TxnId { epoch: 0, n: 0 },
                snapshot: snap.ts(),
                stmt_seq: 0,
            },
            &mut NoSsi,
        )
        .unwrap();
        assert!(got.is_some(), "row 0 survived the updates");
    }
}
