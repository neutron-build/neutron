//! A minimal C-B1: async resolution of committed txns' intents, abort
//! cleanup, and §7.4 status truncation. Both run step by step through
//! [`Resolver::run_bounded`] (bounded: at most `max_keys` keys per round,
//! the unfinished work requeued at the **front**); the unbounded
//! [`Resolver::run_once`] is a `run_bounded` over the whole queue, as
//! before C-B1c. The C-B1a job framework runs them as the
//! [`ResolverJob`] background job (§9.3), woken on enqueue by commit step
//! 5, abort and the no-write fast path instead of polling, one bounded
//! round (4096 keys) per step so a budget can bite.
//!
//! Resolution of a committed T runs only after T is a visible commit — the
//! commit thread queues it in §3 step 5, which follows visibility — so no
//! version above `visible_ts` ever exists (§3.2). Every removal goes through
//! C-T1a's [`remove_intent`](crate::removal::remove_intent): latch, re-read,
//! owner check, one batch, then the §7.3 step 4 bookkeeping. A removal that
//! finds the intent gone writes nothing and changes no count (seed 13).
//!
//! The [`JobPoint`](crate::jobs::JobPoint) probes
//! (`ResolverAfterRemoval`/`ResolverBeforeTruncation`/
//! `ResolverAfterTruncation`) fire inside the round, so hand-driven callers
//! (the simulator, tests) hit them too; they run with no latch and no
//! registry mutex held (§9.3's probe contract).

use std::sync::Arc;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::commit::ResolveEntry;
use crate::jobs::{Job, JobConfig, JobCtx, JobKind, JobOutcome, JobSet};
use crate::removal::{remove_intent, RemovalOutcome};
use crate::TxnError;

/// Keys per [`ResolverJob`] step (§9.3 bounded steps, C-B1c): a step
/// removes at most this many keys before requeueing the rest.
pub const RESOLVER_BATCH: usize = 4096;

/// What one resolver round did (the bounded `run_bounded` report; the
/// unbounded [`Resolver::run_once`] exposes the entry count).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RoundReport {
    /// Queue entries the round fully processed.
    pub entries: usize,
    /// Keys the round worked on — one §7.3 removal attempt each, whether
    /// or not an intent was still there (the attempt is the work; the
    /// I/O-budget unit is charged per key).
    pub keys: u64,
    /// Keys whose removal actually wrote a batch.
    pub removed: u64,
    /// Status entries the truncation pass removed.
    pub truncated: u64,
    /// Whether the queue was fully drained this round — the §7.4
    /// truncation pass runs only then (an entry still in flight may
    /// still hold a count).
    pub drained: bool,
}

/// The resolution and truncation job (§9.3's resolver slot).
#[derive(Debug, Clone, Copy, Default)]
pub struct Resolver;

impl Resolver {
    /// Takes every queued resolution/cleanup entry and runs
    /// `remove_intent(Resolve | Discard)` for each key of its write set
    /// (under the key's latch prefix, §5.0), then attempts §7.4 truncation
    /// of every released txn whose count is 0 (older-epoch records
    /// included; the §7.4 conditions are re-checked under the registry
    /// mutex by `truncate_status`). Returns the number of queued entries
    /// processed.
    ///
    /// On an error the work is kept: the failing entry and every
    /// unprocessed entry after it are requeued (in order) before the error
    /// is returned, and the truncation pass is skipped that round.
    pub fn run_once<K: OrderedKv>(core: &Core<K>) -> Result<usize, TxnError> {
        Resolver::run_bounded(core, usize::MAX).map(|r| r.entries)
    }

    /// One bounded resolver round (§9.3's bounded steps, C-B1c): removes
    /// intents for at most `max_keys` keys, in queue order. The first
    /// entry the round did not finish — the one the bound cut into, or the
    /// one whose removal failed — goes back at the **front** of the queue
    /// with its not-yet-attempted keys only, in order, followed by every
    /// unprocessed entry after it; nothing is dropped. The truncation pass
    /// runs only when the queue was fully drained this round (entries
    /// still in flight may hold counts; `drained` says which it was).
    pub fn run_bounded<K: OrderedKv>(
        core: &Core<K>,
        max_keys: usize,
    ) -> Result<RoundReport, TxnError> {
        let entries: Vec<ResolveEntry> = core.drain_resolve_queue();
        let mut report = RoundReport::default();
        let mut first_err: Option<TxnError> = None;
        // Where the round stopped: `(entry idx, key idx)` — the key at
        // `key idx` was not successfully processed (not attempted, or its
        // removal failed and must be retried).
        let mut stopped: Option<(usize, usize)> = None;
        'round: for (ei, e) in entries.iter().enumerate() {
            for (ki, (key, prefix)) in e.keys.iter().enumerate() {
                if report.keys >= max_keys as u64 {
                    // The bound: this key was not attempted.
                    stopped = Some((ei, ki));
                    break 'round;
                }
                match remove_intent(
                    core,
                    key,
                    crate::latch::latch_prefix_of(key, *prefix),
                    e.txn,
                    e.mode,
                ) {
                    Ok(RemovalOutcome::Removed) => {
                        report.removed += 1;
                        // §9.3 probe: latch and registry mutex both
                        // released by `remove_intent` already.
                        core.job_probe_at(crate::jobs::JobPoint::ResolverAfterRemoval);
                    }
                    Ok(_) => {}
                    Err(err) => {
                        first_err = Some(err);
                        // The failing key is re-attempted next round.
                        stopped = Some((ei, ki));
                        break 'round;
                    }
                }
                report.keys += 1;
                // §9.3 I/O budget: one unit per removal.
                core.job_units(JobKind::Resolver, 1);
            }
            report.entries += 1;
        }
        let drained = stopped.is_none();
        // Requeue the unfinished tail: the cut-into (or failing) entry
        // first with its not-yet-done keys only, in order, then every
        // entry after it — all at the FRONT, so the next round (a bounded
        // job step) retries them before anything enqueued later. An error
        // must not drop cleanup work either (the intents would linger and
        // the counts would stay up forever).
        if let Some((ei, ki)) = stopped {
            let mut q = core.lock_resolve_queue();
            let tail: Vec<ResolveEntry> = entries
                .iter()
                .enumerate()
                .skip(ei)
                .map(|(idx, e)| {
                    if idx == ei {
                        ResolveEntry {
                            txn: e.txn,
                            keys: e.keys[ki..].to_vec(),
                            mode: e.mode,
                        }
                    } else {
                        e.clone()
                    }
                })
                .filter(|e| !e.keys.is_empty())
                .collect();
            for e in tail.into_iter().rev() {
                q.push_front(e);
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        if drained {
            core.job_probe_at(crate::jobs::JobPoint::ResolverBeforeTruncation);
            for id in core.status.truncation_candidates() {
                if core.truncate_status(id)? {
                    report.truncated += 1;
                    core.job_probe_at(crate::jobs::JobPoint::ResolverAfterTruncation);
                }
            }
        }
        let work = report.removed + report.truncated;
        if work > 0 {
            core.job_items(JobKind::Resolver, work);
        }
        report.drained = drained;
        Ok(report)
    }
}

/// The resolver as a §9.3 background job: one `step` is one bounded
/// [`Resolver::run_bounded`] round of [`RESOLVER`] keys. A round that
/// drained the queue and did nothing (`entries == 0`, no removals, no
/// truncations) is `Idle` (the runner parks on the wake until the next
/// release wakes it); anything else is `Progress`.
pub struct ResolverJob<K: OrderedKv> {
    core: Arc<Core<K>>,
}

impl<K: OrderedKv> ResolverJob<K> {
    pub fn new(core: Arc<Core<K>>) -> ResolverJob<K> {
        ResolverJob { core }
    }
}

impl<K: OrderedKv> Job for ResolverJob<K> {
    fn kind(&self) -> JobKind {
        JobKind::Resolver
    }

    fn step(&mut self, _cx: &JobCtx) -> Result<JobOutcome, TxnError> {
        let r = Resolver::run_bounded(&self.core, RESOLVER_BATCH)?;
        Ok(if r.entries == 0 && r.removed == 0 && r.truncated == 0 {
            JobOutcome::Idle
        } else {
            JobOutcome::Progress
        })
    }
}

/// Runs the [`ResolverJob`] in a background thread (§9.3) until
/// [`BackgroundHandle::stop`]. The job parks on its wake between idle
/// rounds (default 50 ms) and is woken by every release: commit step 5,
/// abort and the no-write fast path call `Core::wake_job(Resolver)`. On an
/// error the thread reports it through the core's
/// [`FailStop`](crate::commit::FailStop) hook (the default aborts the
/// process; a recording hook observes it), stops every job of its set, and
/// the failed work stays queued for a later round.
pub fn spawn_background<K: OrderedKv>(core: Arc<Core<K>>) -> Result<BackgroundHandle, TxnError> {
    let mut set = JobSet::new(Arc::clone(&core));
    set.start(
        Box::new(ResolverJob::new(Arc::clone(&core))),
        JobConfig::default_for(JobKind::Resolver),
    )?;
    Ok(BackgroundHandle {
        shutdown: Some(Box::new(move || set.shutdown())),
    })
}

/// Handle to the background resolver thread: a one-job [`JobSet`] behind
/// an erasing shutdown closure, so the handle stays non-generic exactly as
/// before C-B1a. `stop` (and `Drop`) run the set's ordered shutdown and
/// return the job's first error.
pub struct BackgroundHandle {
    shutdown: Option<Box<dyn FnOnce() -> Result<(), TxnError> + Send>>,
}

impl BackgroundHandle {
    /// Signals the loop to stop, joins the thread, and returns its first
    /// error, if any.
    pub fn stop(mut self) -> Result<(), TxnError> {
        match self.shutdown.take() {
            Some(shutdown) => shutdown(),
            None => Ok(()),
        }
    }
}

impl Drop for BackgroundHandle {
    fn drop(&mut self) {
        // The ordered shutdown runs even when the handle is dropped
        // unused; a panic observed at its join is still recorded and
        // reported to the fail-stop hook inside it.
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown();
        }
    }
}

impl<K: OrderedKv> Core<K> {
    pub(crate) fn drain_resolve_queue(&self) -> Vec<ResolveEntry> {
        let mut q = self.lock_resolve_queue();
        q.drain(..).collect()
    }
}
