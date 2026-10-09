//! C-T0 §7.3: intent removal, every path. `remove_intent` takes the latch,
//! re-reads the latest state, acts only if the intent is still owned by the
//! expected txn (for `DropLayersFrom`, only if the expected layers are
//! present), writes one KV batch, and then — still under the latch, under the
//! registry mutex — runs the §7.3 step 4 bookkeeping. A removal that finds
//! the intent gone or re-owned writes nothing and changes no count.

use nucleus_kv::{Batch, Durability, OrderedKv};

use crate::boot::Core;
use crate::encoding::encode_intent;
use crate::encoding::{decode_intent, encode_version, intent_key, version_key};
use crate::latch::{latch_key, LatchGuard};
use crate::{kv_err, Intent, Layer, Seq, TxnError, TxnId, TxnStatus};

/// Which removal rule to apply (§7.3 step 3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalMode {
    /// A visible-committed owner: delete the intent and write the version
    /// from the top layer (`Absent` produces no version).
    Resolve,
    /// An aborted owner: delete the intent, write nothing else.
    Discard,
    /// Savepoint rollback (§5.5): drop layers with `seq >= s`; remove the
    /// intent only if no layer remains.
    DropLayersFrom(Seq),
}

/// What a removal did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RemovalOutcome {
    /// The intent entry was deleted (§7.3 bookkeeping ran).
    Removed,
    /// Layers were dropped but the intent remains (no bookkeeping: the
    /// intent was not removed).
    LayersDropped,
    /// The intent was gone or owned by someone else, or the expected layers
    /// were not present: nothing written, nothing changed.
    Noop,
}

/// §7.3 steps 1–5: takes `latch(latch_key(key, deferrable_prefix))` and runs
/// [`remove_intent_under_latch`]. `deferrable_prefix` is `Some(prefix)` for
/// an `/i/{idx}/{key}{pk}` entry of a deferrable unique constraint (§5.0).
pub fn remove_intent<K: OrderedKv>(
    core: &Core<K>,
    key: &[u8],
    deferrable_prefix: Option<&[u8]>,
    expected: TxnId,
    mode: RemovalMode,
) -> Result<RemovalOutcome, TxnError> {
    let latch = core.latches.lock(latch_key(key, deferrable_prefix));
    remove_intent_under_latch(core, key, expected, mode, &latch)
}

/// §7.3 steps 2–5 for a caller that already holds the latch (the §5.1 loop).
/// The `_latch` witness proves a latch is held; debug builds additionally
/// assert the thread holds exactly one.
pub fn remove_intent_under_latch<K: OrderedKv>(
    core: &Core<K>,
    key: &[u8],
    expected: TxnId,
    mode: RemovalMode,
    _latch: &LatchGuard<'_>,
) -> Result<RemovalOutcome, TxnError> {
    // Step 2: re-read the latest state; act only if still owned by `expected`.
    let raw = core.kv.get_latest(&intent_key(key)).map_err(kv_err)?;
    let Some(raw) = raw else {
        return Ok(RemovalOutcome::Noop);
    };
    let intent = decode_intent(&raw)?;
    if intent.txn != expected {
        return Ok(RemovalOutcome::Noop);
    }

    let removal_ts = match mode {
        RemovalMode::Resolve => match core.status.lookup_for_intent(expected)? {
            // §7.3: resolution only runs once T is a visible commit (§3.2:
            // no version above visible_ts ever exists).
            TxnStatus::Committed(ts) if ts <= core.visible_ts() => Some(ts),
            other => {
                return Err(TxnError::Invariant(format!(
                    "Resolve removal of {expected:?} with status {other:?}"
                )))
            }
        },
        RemovalMode::Discard => match core.status.lookup_for_intent(expected)? {
            TxnStatus::Aborted => None,
            other => {
                return Err(TxnError::Invariant(format!(
                    "Discard removal of {expected:?} with status {other:?}"
                )))
            }
        },
        RemovalMode::DropLayersFrom(_) => None,
    };

    match mode {
        RemovalMode::Resolve | RemovalMode::Discard => {
            // Step 3: one KV batch — delete the intent, plus the version for
            // a committed owner whose top layer is Write/Delete.
            let mut batch = Batch::default();
            batch.delete(intent_key(key));
            if let Some(ts) = removal_ts {
                if let Some(value) = intent.top().and_then(|t| encode_version(&t.data)) {
                    batch.put(version_key(key, ts), value);
                }
            }
            core.kv.write(batch, Durability::No).map_err(kv_err)?;
            bookkeeping(core, expected)?;
            Ok(RemovalOutcome::Removed)
        }
        RemovalMode::DropLayersFrom(from) => {
            // §5.5: drop layers with seq >= from. If none is present there is
            // nothing to roll back.
            if !intent.layers.iter().any(|l| l.seq >= from) {
                return Ok(RemovalOutcome::Noop);
            }
            let kept: Vec<Layer> = intent
                .layers
                .iter()
                .filter(|l| l.seq < from)
                .cloned()
                .collect();
            let mut batch = Batch::default();
            if kept.is_empty() {
                batch.delete(intent_key(key));
                core.kv.write(batch, Durability::No).map_err(kv_err)?;
                bookkeeping(core, expected)?;
                Ok(RemovalOutcome::Removed)
            } else {
                // Restore the previous top layer exactly: the remaining
                // layers are rewritten verbatim.
                let value = encode_intent(&Intent {
                    txn: expected,
                    layers: kept,
                })?;
                batch.put(intent_key(key), value);
                core.kv.write(batch, Durability::No).map_err(kv_err)?;
                // The intent remains: no bookkeeping (§7.3 step 4 only runs
                // when an intent was removed).
                Ok(RemovalOutcome::LayersDropped)
            }
        }
    }
}

/// §7.3 step 4, under the registry mutex (the caller still holds the latch):
/// `last_removal_counter(T) = view_counter`, then — for a current-epoch T —
/// `intent_count(T) -= 1`.
fn bookkeeping<K: OrderedKv>(core: &Core<K>, id: TxnId) -> Result<(), TxnError> {
    core.registry
        .with_registry(|r| core.status.removal_bookkeeping(id, r.view_counter()))
}
