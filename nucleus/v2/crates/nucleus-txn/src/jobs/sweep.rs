//! C-T0 §7.2 (draft 7.7, card C-B1b): the boot sweep. Nothing but
//! encounter removed older-epoch intents before this job existed, so
//! `/sys/txn` records piled up across reboots; the sweep is the lazy
//! cleanup's background half, and `record_sweep` (§7.4 condition 1 for
//! older-epoch truncation) happens only here.
//!
//! The walk covers, in key order, every key outside `/sys/` plus the
//! `/sys/catalog/` subtree (the one `/sys/` family §10 makes versioned
//! table data, so DDL intents live there). No other `/sys/` key is ever
//! decoded — several of them end in bytes `parse_key` would happily read
//! as an intent suffix — and a skipped entry still costs the step its
//! budget and advances the resume key exactly like an examined one, so a
//! long contiguous `/sys/` span cannot inflate one step's walk. Each
//! step opens one registered view, examines at most `chunk` stored
//! entries (default 1024), decides each older-epoch intent's removal
//! mode **inside the view** (I-TRUNC keeps the owner's entry alive as
//! long as the view can see the intent), records the resume key just
//! past the last consumed entry, drops the view, and only then removes. `remove_intent` re-reads under the latch and no-ops on a
//! re-owned intent, so the sweep runs concurrently with writers: no
//! older-epoch intent is created after boot, so a key-ordered walk sees
//! every one that still exists, whatever a current-epoch writer does to
//! the key in the meantime.
//!
//! Never started synchronously inside `Core::open` (§7.2): startup stays
//! independent of data size. In the §9.3 shutdown order the sweep stops
//! after the commit thread and the resolver — nothing depends on it.

use std::ops::Bound;
use std::sync::Arc;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::catalog::SYS_CATALOG_PREFIX;
use crate::encoding::{decode_intent, parse_key, Entry, SYS_PREFIX};
use crate::jobs::{Job, JobCtx, JobKind, JobOutcome, JobPoint};
use crate::kv_err;
use crate::removal::{remove_intent, RemovalMode, RemovalOutcome};
use crate::{TxnError, TxnId, TxnStatus};

/// Entries per step by default.
pub const DEFAULT_CHUNK: usize = 1024;

/// One planned removal, decided inside the step's view.
struct PlannedRemoval {
    key: Vec<u8>,
    /// The deferrable prefix (§5.0), from the latch-key source.
    prefix: Option<Vec<u8>>,
    owner: TxnId,
    mode: RemovalMode,
}

/// The boot sweep (§7.2) as a §9.3 background job. Build with
/// [`BootSweep::new`], optionally `.with_chunk(n)` for tests, and either
/// start it in a [`JobSet`](crate::jobs::runner::JobSet) or drive it by
/// hand with [`BootSweep::run_once`] until
/// [`JobOutcome::Finished`](crate::jobs::JobOutcome). Hand-driving is what
/// recovery harnesses do; it hits the same probes and counters.
pub struct BootSweep<K: OrderedKv> {
    core: Arc<Core<K>>,
    chunk: usize,
    /// The stored key of the last entry the walk consumed — examined, or
    /// skipped as non-catalog `/sys/`: the next step resumes just past it
    /// (`Bound::Excluded`; stored keys are unique, so no entry is skipped
    /// or re-examined).
    resume: Option<Vec<u8>>,
}

impl<K: OrderedKv> BootSweep<K> {
    pub fn new(core: Arc<Core<K>>) -> BootSweep<K> {
        BootSweep {
            core,
            chunk: DEFAULT_CHUNK,
            resume: None,
        }
    }

    /// Consumes at most `chunk` stored entries per step — examined or
    /// skipped (tests); a 0 is clamped to 1 so the walk always advances.
    pub fn with_chunk(mut self, chunk: usize) -> BootSweep<K> {
        self.chunk = chunk.max(1);
        self
    }

    /// One step (§7.2): scan up to `chunk` stored entries from the resume
    /// key (skipped non-catalog `/sys/` keys included), plan every
    /// older-epoch intent's removal with the mode decided inside the
    /// view, record the resume key, drop the view, remove. When the walk
    /// is exhausted: `SweepBeforeRecord`, then `record_sweep()` (§7.4),
    /// then `Finished`.
    pub fn run_once(&mut self) -> Result<JobOutcome, TxnError> {
        let source = self.core.latch_key_source();
        let epoch = self.core.epoch();
        let mut plan: Vec<PlannedRemoval> = Vec::new();
        let mut examined = 0usize;
        let mut last: Option<Vec<u8>> = None;
        {
            let view = self.core.open_view();
            let lo = match &self.resume {
                Some(k) => Bound::Excluded(k.as_slice()),
                None => Bound::Unbounded,
            };
            for entry in view.scan((lo, Bound::Unbounded), false) {
                let (stored, value) = entry.map_err(kv_err)?;
                if examined == self.chunk {
                    break;
                }
                // §7.2: everything outside /sys/ plus /sys/catalog/. The
                // rest of /sys/ is skipped whole — never decoded — but a
                // skipped entry still costs the step its budget and still
                // advances the resume key (C-B1b gate L-5): one step's
                // iterator work stays bounded by `chunk` stored entries
                // however long the contiguous /sys/ span is, and the
                // next step resumes past the skipped span instead of
                // re-walking it.
                if stored.starts_with(SYS_PREFIX) && !stored.starts_with(SYS_CATALOG_PREFIX) {
                    examined += 1;
                    last = Some(stored);
                    continue;
                }
                if let Some((logical, Entry::Intent)) = parse_key(&stored) {
                    let intent = decode_intent(&value)?;
                    if intent.txn.epoch < epoch {
                        // The mode is decided inside the view (I-TRUNC):
                        // as long as the view can see the intent, the
                        // owner's entry cannot have been truncated.
                        // §7.2: a persisted record is always a visible
                        // commit at boot, and a missing older-epoch entry
                        // reads Aborted.
                        let mode = match self.core.status.lookup_for_intent(intent.txn)? {
                            TxnStatus::Committed(_) => RemovalMode::Resolve,
                            TxnStatus::Aborted => RemovalMode::Discard,
                            other => {
                                return Err(TxnError::Invariant(format!(
                                    "sweep: older-epoch owner {:?} of {logical:?} reads {other:?} \
                                     (C-T0 §7.2)",
                                    intent.txn
                                )))
                            }
                        };
                        // An over-long prefix from the source is a
                        // misbehaving source, not a shorter latch: loud,
                        // like `GcJob::retire_locked` (C-B1b gate N-5).
                        let prefix = match source.deferrable_prefix(logical) {
                            Some(len) => match logical.get(..len) {
                                Some(p) => Some(p.to_vec()),
                                None => {
                                    return Err(TxnError::Invariant(format!(
                                        "sweep: the latch key source returned prefix length {len} \
                                         for key {logical:?} of length {} (C-T0 §7.2)",
                                        logical.len()
                                    )))
                                }
                            },
                            None => None,
                        };
                        plan.push(PlannedRemoval {
                            key: logical.to_vec(),
                            prefix,
                            owner: intent.txn,
                            mode,
                        });
                    }
                }
                examined += 1;
                last = Some(stored);
            }
        }
        // The resume key is recorded just past the last examined entry
        // (§7.2), before the view drops and before any removal runs.
        self.resume = last;
        let exhausted = examined < self.chunk;
        for r in &plan {
            if let RemovalOutcome::Removed =
                remove_intent(&self.core, &r.key, r.prefix.as_deref(), r.owner, r.mode)?
            {
                self.core.job_items(JobKind::BootSweep, 1);
                // §9.3 probe: `remove_intent` released the latch and the
                // registry mutex already.
                self.core.job_probe_at(JobPoint::SweepAfterRemoval);
            }
        }
        if exhausted {
            // §7.4: the counter is recorded only now — every older-epoch
            // intent the walk could see has been removed, so an
            // older-epoch record may start truncating.
            self.core.job_probe_at(JobPoint::SweepBeforeRecord);
            self.core.registry.record_sweep();
            Ok(JobOutcome::Finished)
        } else {
            Ok(JobOutcome::Progress)
        }
    }
}

impl<K: OrderedKv> Job for BootSweep<K> {
    fn kind(&self) -> JobKind {
        JobKind::BootSweep
    }

    fn step(&mut self, _cx: &JobCtx) -> Result<JobOutcome, TxnError> {
        self.run_once()
    }
}
