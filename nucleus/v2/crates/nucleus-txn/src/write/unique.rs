//! C-T0 §5.3: unique checks. The per-row verdict for `Unique` keys runs
//! inside the §5.1 step (under the latch); the deferrable prefix check is
//! a step task of its own driven by
//! [`Core::check_deferrable_unique`](crate::boot::Core::check_deferrable_unique).

use std::ops::Bound;

use nucleus_kv::OrderedKv;

use crate::boot::Core;
use crate::removal::{remove_intent_under_latch, RemovalMode};
use crate::txn::{Isolation, Txn};
use crate::write::ctx::WaitSet;
use crate::write::ctx::{classify_foreign, Foreign};
use crate::write::step::{map_wait_outcome, KeyState};
use crate::write::{CommittedVersion, UniqueRule};
use crate::{Layer, LayerData, Ts, TxnError, TxnStatus};

/// The §5.3 per-row unique verdict, under the §5.1 latch. The key's
/// **current state** is the own top layer's data if `W` has an intent whose
/// top data is `Write` or `Delete`; otherwise (no own intent, or own top
/// `Absent`, seed 24) the newest committed data version.
///
/// - Live and not `same_row` (the live value equals `same_row`) → 23505
///   (seed 19: an own `Write` counts).
/// - Under SERIALIZABLE, a live **committed** version with `ts > S` and
///   `ssi_hook().covers(W, key)` → 40001 instead (§5.3).
/// - Not live (absent, tombstone, moved-tombstone, own `Delete`) → proceed.
pub(crate) fn unique_verdict<K: OrderedKv>(
    core: &Core<K>,
    txn: &Txn,
    key: &[u8],
    own_top: Option<&Layer>,
    newest_committed: Option<&CommittedVersion>,
    rule: &UniqueRule,
    snapshot: Ts,
) -> Result<(), TxnError> {
    let UniqueRule::Unique { same_row } = rule else {
        // `None` and `Deferrable` never reach the per-row check.
        return Ok(());
    };
    let same_row = same_row.as_deref();
    match own_top.map(|t| &t.data) {
        // The own intent's top data is the current state (§5.3).
        Some(LayerData::Write { value, .. }) => {
            if same_row == Some(value.as_slice()) {
                return Ok(());
            }
            Err(TxnError::UniqueViolation)
        }
        // Own `Delete` → proceed ("delete then re-insert of the same key").
        Some(LayerData::Delete { .. }) => Ok(()),
        // Own `Absent` (lock-only) or no own intent: the newest committed
        // data version is the current state (seed 24).
        _ => match newest_committed {
            None => Ok(()),
            Some(v) => match &v.value {
                crate::encoding::VersionValue::Tombstone { .. } => Ok(()),
                crate::encoding::VersionValue::Live { payload, .. } => {
                    if same_row == Some(payload.as_slice()) {
                        return Ok(());
                    }
                    if txn.isolation == Isolation::Serializable
                        && v.ts > snapshot
                        && core.ssi_hook().covers(txn.id, key)
                    {
                        return Err(TxnError::SerializationFailure);
                    }
                    Err(TxnError::UniqueViolation)
                }
            },
        },
    }
}

/// The §5.3 deferrable check's step outcome.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DefStep {
    /// The prefix holds at most one live entry.
    Done,
    /// A foreign intent was removed; re-scan.
    Again,
    /// A foreign Pending or committed-not-visible entry: wait on its owner.
    Wait(WaitSet),
}

/// The §5.3 deferrable prefix check as steps: under `latch(prefix)`, one
/// registered view over the prefix range, taking each entry's current state
/// as in [`UniqueRule::Unique`]: a foreign visible-committed or aborted
/// intent → remove under the held latch and re-scan; a foreign Pending or
/// committed-not-visible entry → wait on its owner; two live entries →
/// 23505.
pub struct DeferrableCheckTask {
    prefix: Vec<u8>,
}

impl DeferrableCheckTask {
    pub fn new(prefix: &[u8]) -> DeferrableCheckTask {
        DeferrableCheckTask {
            prefix: prefix.to_vec(),
        }
    }

    /// One latch section: latch(prefix), one view, the verdict or the next
    /// wait.
    pub fn step<K: OrderedKv>(&mut self, core: &Core<K>, txn: &Txn) -> Result<DefStep, TxnError> {
        let latch = core.latches.lock(&self.prefix);
        let view = core.open_view();
        let entries = scan_prefix(&view, &self.prefix)?;
        drop(view);
        for (entry_key, state) in &entries {
            if let Some(intent) = &state.intent {
                if intent.txn != txn.id {
                    let owner = intent.txn;
                    let status = core.status.lookup_for_intent(owner)?;
                    match classify_foreign(status, core.visible_ts()) {
                        Foreign::RemoveResolve | Foreign::RemoveDiscard => {
                            let mode = if matches!(status, TxnStatus::Committed(_)) {
                                RemovalMode::Resolve
                            } else {
                                RemovalMode::Discard
                            };
                            // Seed 25: the removal latches the prefix.
                            remove_intent_under_latch(
                                core,
                                entry_key,
                                Some(&self.prefix),
                                owner,
                                mode,
                                &latch,
                            )?;
                            drop(latch);
                            return Ok(DefStep::Again);
                        }
                        Foreign::Blocking => {
                            // Pending or committed-not-visible: wait on its
                            // owner (§5.3: it always conflicts), then
                            // re-check.
                            let gen = core.status.entry(owner).map_or(0, |e| e.gen);
                            drop(latch);
                            return Ok(DefStep::Wait(vec![(owner, gen)]));
                        }
                    }
                }
            }
        }
        // Verdict: each entry's current state (§5.3's rule); two live
        // entries for different rows → 23505.
        drop(latch);
        let mut live = 0usize;
        for (_, state) in &entries {
            let own = state.intent.as_ref().filter(|i| i.txn == txn.id);
            let is_live = match own.and_then(|i| i.top()).map(|t| &t.data) {
                Some(LayerData::Write { .. }) => true,
                Some(LayerData::Delete { .. }) => false,
                _ => state.committed_live(),
            };
            if is_live {
                live += 1;
            }
        }
        if live >= 2 {
            return Err(TxnError::UniqueViolation);
        }
        Ok(DefStep::Done)
    }
}

/// Scans every logical key starting with `prefix` (the `/i/{idx}/{key}`
/// entries of one key value, §5.3) from one view.
fn scan_prefix<S: nucleus_kv::Snapshot>(
    view: &crate::registry::ViewGuard<'_, S>,
    prefix: &[u8],
) -> Result<Vec<(Vec<u8>, KeyState)>, TxnError> {
    let hi = prefix_end(prefix);
    let lower = Bound::Included(prefix.to_vec());
    let upper = match hi {
        Some(end) => Bound::Excluded(end),
        None => Bound::Unbounded,
    };
    let lower_ref = match &lower {
        Bound::Included(p) => Bound::Included(p.as_slice()),
        Bound::Excluded(p) => Bound::Excluded(p.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let upper_ref = match &upper {
        Bound::Included(p) => Bound::Included(p.as_slice()),
        Bound::Excluded(p) => Bound::Excluded(p.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    };
    let mut out: Vec<(Vec<u8>, KeyState)> = Vec::new();
    for entry in view.scan((lower_ref, upper_ref), false) {
        let (stored, value) = entry.map_err(crate::kv_err)?;
        let Some((logical, kind)) = crate::encoding::parse_key(&stored) else {
            continue;
        };
        if !logical.starts_with(prefix) {
            continue; // the successor bound is exact; this is belt and braces
        }
        if out.last().is_some_and(|(l, _)| l.as_slice() == logical) {
            push_kind(&mut out, kind, value)?;
        } else {
            let mut state = KeyState {
                intent: None,
                versions: Vec::new(),
            };
            push_state(&mut state, kind, value)?;
            out.push((logical.to_vec(), state));
        }
    }
    Ok(out)
}

fn push_state(
    state: &mut KeyState,
    kind: crate::encoding::Entry,
    value: nucleus_kv::Value,
) -> Result<(), TxnError> {
    match kind {
        crate::encoding::Entry::Intent => {
            state.intent = Some(crate::encoding::decode_intent(&value)?)
        }
        crate::encoding::Entry::Version(ts) => state.versions.push(CommittedVersion {
            ts,
            value: crate::encoding::decode_version(&value)?,
        }),
    }
    Ok(())
}

fn push_kind(
    out: &mut [(Vec<u8>, KeyState)],
    kind: crate::encoding::Entry,
    value: nucleus_kv::Value,
) -> Result<(), TxnError> {
    match out.last_mut() {
        Some((_, state)) => push_state(state, kind, value),
        None => Ok(()),
    }
}

/// The exclusive upper bound of a prefix scan: the prefix with its last
/// non-`0xFF` byte incremented (truncated after it); `None` when every
/// byte is `0xFF` (scan to the end and filter, which `scan_prefix` does).
fn prefix_end(prefix: &[u8]) -> Option<Vec<u8>> {
    for i in (0..prefix.len()).rev() {
        if prefix[i] != 0xFF {
            let mut end = prefix[..=i].to_vec();
            end[i] += 1;
            return Some(end);
        }
    }
    None
}

impl<K: OrderedKv> Core<K> {
    /// The §5.3 deferrable check, called by the SQL layer at end of
    /// statement or commit (constraint timings 2/3): drives
    /// [`DeferrableCheckTask`] — wait on foreign owners, re-scan after
    /// removals — until the prefix holds at most one live entry (else
    /// 23505). `snapshot` is the txn's `S`, carried for the caller and
    /// C-T3; the check itself reads the latest state under the prefix latch
    /// (§5.3), as G0-write's `DefScan`/`DefLook` do.
    pub fn check_deferrable_unique(
        &self,
        txn: &Txn,
        prefix: &[u8],
        snapshot: Ts,
    ) -> Result<(), TxnError> {
        let _ = snapshot;
        let mut task = DeferrableCheckTask::new(prefix);
        loop {
            if txn.is_cancelled() {
                return Err(TxnError::QueryCanceled);
            }
            match task.step(self, txn)? {
                DefStep::Done => return Ok(()),
                DefStep::Again => {}
                DefStep::Wait(targets) => {
                    map_wait_outcome(self.wait_on_any(txn, &targets))?;
                }
            }
        }
    }
}
