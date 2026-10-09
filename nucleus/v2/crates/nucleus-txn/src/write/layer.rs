//! C-T0 §2.1: layer building, a pure function with no I/O. The §5.1 loop
//! calls [`apply_change`] under the key's latch and writes the result as
//! one batch; every rule it implements is checked by the layer table tests
//! and by G0-write.

use crate::{Intent, Layer, LayerData, RowLockMode, Seq, TxnId};

/// One change to place on the intent (exclusive modes only for `Lock`:
/// shared requests are granted in the [`RowLocks`](crate::write::RowLocks)
/// table and never reach this function). Crate-private (C-T2 rework 7e):
/// tested by the in-crate unit tests in `write::tests`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Change {
    Write {
        value: Vec<u8>,
        /// The caller's statement that a key column differs from the newest
        /// committed version (§1).
        key_cols_changed: bool,
    },
    Delete {
        /// A PK-changing UPDATE leaves `moved: true` at the old `/t/` key
        /// (§2.1).
        moved: bool,
    },
    /// A lock-only request (§2.1: it copies the previous layer's data and
    /// `data_seq` and raises `lock`; it **never** replaces own data).
    Lock(RowLockMode),
}

impl Change {
    /// Whether this change changes data (drives the §5.1 SSI check and the
    /// layer's `data_seq`).
    pub(crate) fn changes_data(&self) -> bool {
        !matches!(self, Change::Lock(_))
    }
}

/// §2.1: `lock` is the max of the implied mode, the previous layer's lock
/// and the requested lock. Implied: `Delete`, or a `Write` with final
/// `key_changed` → `Update`; any other `Write` → `NoKeyUpdate`. A lock-only
/// change has no implied mode.
fn implied_lock(data: &LayerData) -> Option<RowLockMode> {
    match data {
        LayerData::Write { key_changed, .. } => Some(if *key_changed {
            RowLockMode::Update
        } else {
            RowLockMode::NoKeyUpdate
        }),
        LayerData::Delete { .. } => Some(RowLockMode::Update),
        LayerData::Absent => None,
    }
}

/// Builds the intent after one change (§2.1), exactly:
///
/// - `s = max(place_seq, top.seq)`. If `top.seq == s`, the top layer is
///   modified in place; otherwise a new layer with seq `s` is pushed (a
///   missing intent pushes its first layer at `place_seq`).
/// - A data change sets the layer's `data_seq` to the `data_seq` argument
///   (the writing command's seq; it differs from `place_seq` only in the ON
///   CONFLICT attempt and when a BEFORE trigger at a later seq already
///   pushed a layer). A lock-only change copies the previous layer's `data`
///   and `data_seq`; a first lock-only layer has `data: Absent, data_seq: 0`.
///   A lock-only change never replaces own data (seed 15).
/// - `lock = max(implied, previous layer's lock, requested lock)`.
/// - Final `key_changed` of a `Write` = (any earlier layer of this intent
///   has a `Write` with `key_changed`) OR (the previous layer's data is
///   `Delete`) OR (`committed_live` AND `key_cols_changed`). So an insert
///   over a non-live committed state with no own data has `false`.
///
/// `current` is the intent as read under the latch; it must be `None` or
/// owned by `owner` (the §5.1 loop removed any foreign one first).
pub(crate) fn apply_change(
    current: Option<&Intent>,
    owner: TxnId,
    place_seq: Seq,
    data_seq: Seq,
    change: Change,
    committed_live: bool,
) -> Intent {
    debug_assert!(current.is_none_or(|i| i.txn == owner));
    let mut layers: Vec<Layer> = match current {
        Some(i) => i.layers.clone(),
        None => Vec::new(),
    };
    let prev: Option<&Layer> = layers.last();
    let top_seq = prev.map_or(0, |l| l.seq);

    // The layer's data and data_seq (§2.1). A lock-only change copies the
    // previous layer's `data`/`data_seq`; a first lock-only layer is
    // `Absent` with data_seq 0.
    let (data, layer_data_seq): (LayerData, Seq) = match &change {
        Change::Lock(_) => match prev {
            Some(top) => (top.data.clone(), top.data_seq),
            None => (LayerData::Absent, 0),
        },
        Change::Write {
            value,
            key_cols_changed,
        } => {
            // Sticky key_changed (§2.1), exactly the spec's formula.
            let sticky = layers.iter().any(|l| {
                matches!(
                    l.data,
                    LayerData::Write {
                        key_changed: true,
                        ..
                    }
                )
            });
            let after_own_delete = prev.is_some_and(|l| matches!(l.data, LayerData::Delete { .. }));
            let key_changed = sticky || after_own_delete || (committed_live && *key_cols_changed);
            (
                LayerData::Write {
                    value: value.clone(),
                    key_changed,
                },
                data_seq,
            )
        }
        Change::Delete { moved } => (LayerData::Delete { moved: *moved }, data_seq),
    };

    // lock = max(implied, previous lock, requested). A lock-only request's
    // requested mode is the `Lock(m)` argument itself (exclusive only).
    let mut lock = implied_lock(&data).unwrap_or(RowLockMode::NoKeyUpdate);
    if let Some(top) = prev {
        lock = lock.max(top.lock);
    }
    if let Change::Lock(m) = &change {
        debug_assert!(
            m.is_exclusive(),
            "a shared mode is never stored in an intent (§2.1)"
        );
        lock = lock.max(*m);
    }

    let s = place_seq.max(top_seq);
    let layer = Layer {
        seq: s,
        data_seq: layer_data_seq,
        data,
        lock,
    };
    if layers.last().is_some_and(|l| l.seq == s) {
        let n = layers.len();
        layers[n - 1] = layer;
    } else {
        layers.push(layer);
    }
    Intent { txn: owner, layers }
}
