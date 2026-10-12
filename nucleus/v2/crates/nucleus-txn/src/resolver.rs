//! A minimal C-B1: async resolution of committed txns' intents, abort
//! cleanup, and §7.4 status truncation. Both run step by step through
//! [`Resolver::run_once`]; the C-B1a job framework runs them as the
//! [`ResolverJob`] background job (§9.3), woken on enqueue by commit step
//! 5, abort and the no-write fast path instead of polling.
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
//! `ResolverAfterTruncation`) fire inside `run_once`, so hand-driven
//! callers (the simulator, tests) hit them too; they run with no latch and
//! no registry mutex held (§9.3's probe contract).

use std::sync::Arc;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::commit::ResolveEntry;
use crate::jobs::{Job, JobConfig, JobCtx, JobKind, JobOutcome, JobSet};
use crate::removal::{remove_intent, RemovalOutcome};
use crate::TxnError;

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
        Resolver::run_once_detailed(core).map(|(entries, _work)| entries)
    }

    /// [`Resolver::run_once`] plus the work it did (successful intent
    /// removals + entries truncated), which the job maps to
    /// `Progress`/`Idle` and counts in the `items` metric.
    pub(crate) fn run_once_detailed<K: OrderedKv>(
        core: &Core<K>,
    ) -> Result<(usize, u64), TxnError> {
        let entries: Vec<ResolveEntry> = core.drain_resolve_queue();
        let n = entries.len();
        let mut keys_removed: u64 = 0;
        let mut first_err: Option<TxnError> = None;
        let mut i = 0;
        while i < entries.len() {
            let e = &entries[i];
            let mut failed: Option<TxnError> = None;
            for (key, prefix) in &e.keys {
                match remove_intent(
                    core,
                    key,
                    crate::latch::latch_prefix_of(key, *prefix),
                    e.txn,
                    e.mode,
                ) {
                    Ok(RemovalOutcome::Removed) => {
                        keys_removed += 1;
                        // §9.3 probe: latch and registry mutex both
                        // released by `remove_intent` already.
                        core.job_probe_at(crate::jobs::JobPoint::ResolverAfterRemoval);
                    }
                    Ok(_) => {}
                    Err(err) => {
                        failed = Some(err);
                        break;
                    }
                }
            }
            if let Some(err) = failed {
                first_err = Some(err);
                break;
            }
            i += 1;
        }
        // Requeue the unprocessed tail, including the failing entry: an
        // error must not drop cleanup work (the intents would linger and
        // the counts would stay up forever).
        {
            let mut q = core.lock_resolve_queue();
            for e in entries.iter().skip(i) {
                q.push_back(e.clone());
            }
        }
        if let Some(err) = first_err {
            return Err(err);
        }
        core.job_probe_at(crate::jobs::JobPoint::ResolverBeforeTruncation);
        let mut truncated: u64 = 0;
        for id in core.status.truncation_candidates() {
            if core.truncate_status(id)? {
                truncated += 1;
                core.job_probe_at(crate::jobs::JobPoint::ResolverAfterTruncation);
            }
        }
        let work = keys_removed + truncated;
        if work > 0 {
            core.job_items(JobKind::Resolver, work);
        }
        Ok((n, work))
    }
}

/// The resolver as a §9.3 background job: one `step` is one
/// [`Resolver::run_once`]. `Ok(entries == 0)` with no removals and no
/// truncations is `Idle` (the runner parks on the wake until the next
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
        let (entries, work) = Resolver::run_once_detailed(&self.core)?;
        Ok(if entries == 0 && work == 0 {
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
