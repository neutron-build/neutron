//! W5 — long reader under writers (card X-1b Work 2): hold one RR
//! snapshot while 4 writers do 2M updates (500k each — the card's "4
//! writers do 2M updates" read as a collective total, documented here).
//! Reports the peak version-store growth — `undo_bytes +
//! undo_spilled_bytes` on arm (c), `versions` on arm (b) — peak RSS
//! (`VmHWM`), the long reader's point-get latency at the end, and
//! whether it still reads its original values. Arm (c) runs with an
//! undo budget of 64 MiB (card Work 2 W5).
//!
//! Invariants owned here:
//! - W5-ORDER: the reader's RR txn begins (and registers its snapshot)
//!   after the preload and strictly before any writer's first commit —
//!   a barrier hands the workload over only then.
//! - W5-PEAK: peaks are the max over a sampler thread's `stats()` calls
//!   (every `sampler_interval`) plus the final reading; each sample is
//!   the engine's own accounting, never a guess. `VmHWM` is reset right
//!   after the preload so the peak is the measured phase's, and whether
//!   the reset worked is reported.
//! - W5-ALIVE: "alive" means the reader's end-of-run point gets return
//!   the **original preloaded values** and no error — arm (c)'s policy
//!   is that readers are never aborted (N3), so any error here fails
//!   the workload rather than recording a dead reader as a number.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Instant;

use e1_mvcc::api::{Isolation, TxnEngine};

use crate::BenchError;
use crate::Config;
use crate::json::{Json, obj};
use crate::util;
use crate::workload::{W_W5, block, err, preload, rng_for};

/// Run W5 on one fresh engine and return its report object (card Work
/// 2).
pub fn run<E: TxnEngine>(
    make: &dyn Fn(usize) -> Result<E, BenchError>,
    cfg: &Config,
    run: u32,
) -> Result<Json, BenchError> {
    let engine = Arc::new(make(cfg.pool_frames)?);
    preload(
        &*engine,
        cfg.preload_keys,
        cfg.preload_batch,
        cfg.value_len,
        0,
    )?;

    // W5-PEAK: the high-water mark restarts with the measured phase.
    let rss_reset = util::reset_peak_rss();

    // W5-ORDER: the reader registers before the barrier opens to the
    // writers.
    let mut reader = engine.begin(Isolation::RepeatableRead);

    let per_writer = cfg.w5_updates / cfg.w5_writers.max(1) as u64;
    let remainder = cfg.w5_updates - per_writer * cfg.w5_writers.max(1) as u64;

    let stop_sampler = Arc::new(AtomicBool::new(false));
    let peak_versions = Arc::new(AtomicU64::new(0));
    let peak_undo = Arc::new(AtomicU64::new(0));
    let peak_tree = Arc::new(AtomicU64::new(0));
    let peak_ws = Arc::new(AtomicU64::new(0));

    thread::scope(|s| -> Result<(), BenchError> {
        // Sampler (W5-PEAK). stats() walks the tree on both arms, so the
        // interval is 500 ms — see the README's cost note.
        {
            let e = Arc::clone(&engine);
            let stop = Arc::clone(&stop_sampler);
            let pv = Arc::clone(&peak_versions);
            let pu = Arc::clone(&peak_undo);
            let pt = Arc::clone(&peak_tree);
            let pw = Arc::clone(&peak_ws);
            s.spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    thread::sleep(cfg.sampler_interval);
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let st = e.stats();
                    track(&pv, st.versions);
                    track(&pu, st.undo_bytes + st.undo_spilled_bytes);
                    track(&pt, st.tree_bytes);
                    track(&pw, st.write_set_bytes);
                }
            });
        }
        let barrier = Arc::new(Barrier::new(cfg.w5_writers + 1));
        let mut handles = Vec::new();
        for tid in 0..cfg.w5_writers {
            let e = Arc::clone(&engine);
            let b = Arc::clone(&barrier);
            let mine = per_writer + if tid == 0 { remainder } else { 0 };
            handles.push(s.spawn(move || -> Result<u64, BenchError> {
                let mut rng = rng_for(W_W5, run, tid, 0);
                b.wait();
                let mut salt = (tid as u64) << 40;
                for _ in 0..mine {
                    let idx = rng.below(cfg.preload_keys);
                    let key = util::key_of(idx);
                    let value = util::value_of(idx, salt, cfg.value_len);
                    salt = salt.wrapping_add(1);
                    let mut t = e.begin(Isolation::ReadCommitted);
                    e.put(&mut t, &key, &value, block(cfg.write_wait))
                        .map_err(|x| err("W5 put", x))?;
                    e.commit(t).map_err(|x| err("W5 commit", x))?;
                }
                Ok(mine)
            }));
        }
        barrier.wait();
        for h in handles {
            h.join()
                .map_err(|_| BenchError("W5 writer panicked".into()))??;
        }
        stop_sampler.store(true, Ordering::Release);
        Ok(())
    })?;

    // Final sample (W5-PEAK includes it).
    let final_stats = engine.stats();
    track(&peak_versions, final_stats.versions);
    track(
        &peak_undo,
        final_stats.undo_bytes + final_stats.undo_spilled_bytes,
    );
    track(&peak_tree, final_stats.tree_bytes);
    track(&peak_ws, final_stats.write_set_bytes);

    // W5-ALIVE: the long reader's point gets at the end.
    let sample = cfg.w5_sample_keys.min(cfg.preload_keys);
    let stride = (cfg.preload_keys / sample.max(1)).max(1);
    let mut latencies = Vec::new();
    let mut original = true;
    for i in (0..cfg.preload_keys).step_by(stride as usize) {
        let key = util::key_of(i);
        let want = util::value_of(i, 0, cfg.value_len);
        let t0 = Instant::now();
        let got = engine
            .get(&mut reader, &key)
            .map_err(|e| err("W5 long-reader get", e))?;
        latencies.push(t0.elapsed().as_nanos() as u64);
        if got.as_deref() != Some(want.as_slice()) {
            original = false;
        }
    }
    engine.abort(reader);

    let (p50, p99) = util::latencies_p50_p99(&latencies);
    let peak_rss = util::peak_rss_kib();
    Ok(obj(vec![
        ("workload", "W5".into()),
        ("keys", cfg.preload_keys.into()),
        ("writers", cfg.w5_writers.into()),
        ("updates_total", cfg.w5_updates.into()),
        ("updates_per_writer", per_writer.into()),
        ("undo_budget_bytes", cfg.undo_budget_bytes.into()),
        ("peak_versions", peak_versions.load(Ordering::Relaxed).into()),
        ("peak_undo_bytes", peak_undo.load(Ordering::Relaxed).into()),
        ("peak_tree_bytes", peak_tree.load(Ordering::Relaxed).into()),
        ("peak_write_set_bytes", peak_ws.load(Ordering::Relaxed).into()),
        (
            "final_undo_bytes",
            (final_stats.undo_bytes + final_stats.undo_spilled_bytes).into(),
        ),
        ("peak_rss_kib", peak_rss.map(Json::from).unwrap_or(Json::Null)),
        ("peak_rss_reset_worked", rss_reset.into()),
        ("reader_gets", latencies.len().into()),
        (
            "reader_get_p50_ns",
            p50.map(Json::from).unwrap_or(Json::Null),
        ),
        (
            "reader_get_p99_ns",
            p99.map(Json::from).unwrap_or(Json::Null),
        ),
        ("reader_reads_original_values", original.into()),
        (
            "note",
            "peak undo = undo_bytes + undo_spilled_bytes (c); peak versions counts tree versions (b) and undo records + tree entries (c); peaks sampled every sampler_interval plus the final reading"
                .into(),
        ),
    ]))
}

/// Raise `slot` to `v` if `v` is larger (W5-PEAK).
fn track(slot: &AtomicU64, v: u64) {
    slot.fetch_max(v, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::track;
    use std::sync::atomic::AtomicU64;

    /// W5-PEAK's tracker is a max.
    #[test]
    fn track_keeps_the_max() {
        let slot = AtomicU64::new(0);
        track(&slot, 5);
        track(&slot, 3);
        track(&slot, 9);
        assert_eq!(slot.load(std::sync::atomic::Ordering::Relaxed), 9);
    }
}
