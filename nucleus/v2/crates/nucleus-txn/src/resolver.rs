//! A minimal C-B1: async resolution of committed txns' intents, abort
//! cleanup, and §7.4 status truncation. Both run step by step through
//! [`Resolver::run_once`]; [`spawn_background`] runs them in a thread.
//!
//! Resolution of a committed T runs only after T is a visible commit — the
//! commit thread queues it in §3 step 5, which follows visibility — so no
//! version above `visible_ts` ever exists (§3.2). Every removal goes through
//! C-T1a's [`remove_intent`](crate::removal::remove_intent): latch, re-read,
//! owner check, one batch, then the §7.3 step 4 bookkeeping. A removal that
//! finds the intent gone writes nothing and changes no count (seed 13).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::commit::ResolveEntry;
use crate::removal::remove_intent;
use crate::TxnError;

/// How long the background thread sleeps between rounds when idle.
const IDLE_PAUSE: Duration = Duration::from_millis(1);

/// The resolution and truncation jobs (a minimal C-B1).
#[derive(Debug, Clone, Copy, Default)]
pub struct Resolver;

impl Resolver {
    /// Takes every queued resolution/cleanup entry and runs
    /// `remove_intent(Resolve | Discard)` for each key of its write set,
    /// then attempts §7.4 truncation of every released txn whose count is 0
    /// (older-epoch records included; the §7.4 conditions are re-checked
    /// under the registry mutex by `truncate_status`). Returns the number
    /// of queued entries processed.
    ///
    /// On an error the work is kept: the failing entry and every
    /// unprocessed entry after it are requeued (in order) before the error
    /// is returned, and the truncation pass is skipped that round.
    pub fn run_once<K: OrderedKv>(core: &Core<K>) -> Result<usize, TxnError> {
        let entries: Vec<ResolveEntry> = core.drain_resolve_queue();
        let n = entries.len();
        let mut first_err: Option<TxnError> = None;
        let mut i = 0;
        while i < entries.len() {
            let e = &entries[i];
            let mut failed: Option<TxnError> = None;
            for key in &e.keys {
                if let Err(err) = remove_intent(core, key, None, e.txn, e.mode) {
                    failed = Some(err);
                    break;
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
        for id in core.status.truncation_candidates() {
            core.truncate_status(id)?;
        }
        Ok(n)
    }
}

/// Runs [`Resolver::run_once`] in a background thread until
/// [`BackgroundHandle::stop`]. On an error the thread reports it through
/// the core's [`FailStop`](crate::commit::FailStop) hook (the default
/// aborts the process; a recording hook observes it) and then stops; the
/// failed work stays queued for a later round.
pub fn spawn_background<K: OrderedKv>(core: Arc<Core<K>>) -> Result<BackgroundHandle, TxnError> {
    let stop = Arc::new(AtomicBool::new(false));
    let thread = {
        let core = Arc::clone(&core);
        let stop = Arc::clone(&stop);
        std::thread::Builder::new()
            .name("nucleus-resolver".into())
            .spawn(move || -> Result<(), TxnError> {
                loop {
                    if stop.load(Ordering::SeqCst) {
                        return Ok(());
                    }
                    match Resolver::run_once(&core) {
                        Ok(0) => std::thread::sleep(IDLE_PAUSE),
                        Ok(_) => {}
                        Err(e) => {
                            core.fail_stop().on_kv_error(&e);
                            return Err(e);
                        }
                    }
                }
            })
            .map_err(|e| TxnError::Invariant(format!("failed to spawn the resolver thread: {e}")))?
    };
    Ok(BackgroundHandle {
        stop,
        thread: Some(thread),
    })
}

/// Handle to the background resolver thread.
pub struct BackgroundHandle {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<Result<(), TxnError>>>,
}

impl BackgroundHandle {
    /// Signals the loop to stop, joins the thread, and returns its last
    /// round's result (the first error it hit, if any).
    pub fn stop(mut self) -> Result<(), TxnError> {
        self.stop.store(true, Ordering::SeqCst);
        match self.thread.take() {
            Some(t) => t
                .join()
                .unwrap_or_else(|_| Err(TxnError::Invariant("resolver thread panicked".into()))),
            None => Ok(()),
        }
    }
}

impl Drop for BackgroundHandle {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(t) = self.thread.take() {
            let _ = t.join();
        }
    }
}

impl<K: OrderedKv> Core<K> {
    pub(crate) fn drain_resolve_queue(&self) -> Vec<ResolveEntry> {
        let mut q = self.lock_resolve_queue();
        q.drain(..).collect()
    }
}
