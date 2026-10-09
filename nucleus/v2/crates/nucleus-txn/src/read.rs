//! C-T0 §4: the read path over registered views. Readers never block
//! (I-NOBLOCK): a point read or scan reads intents and versions from one KV
//! view, resolves intent owners through the status table
//! (`lookup_for_intent`: a missing current-epoch entry is a fatal
//! [`TxnError::Invariant`], never defaulted), and applies the pure rule in
//! [`crate::visibility::read`].
//!
//! SSI edges (§8) surface through [`ReadObserver::on_edge`]; [`NoSsi`] is the
//! no-op. AS OF and the catalog are out of scope;
//! [`Registry::register_at`](crate::registry::Registry::register_at) is their
//! primitive.

use std::ops::Bound;

use nucleus_kv::{Key, OrderedKv, Snapshot};

use crate::boot::Core;
use crate::encoding::{decode_intent, decode_version, end_key, intent_key, parse_key, Entry};
use crate::kv_err;
use crate::visibility::{read as visibility_read, Read, ReadCtx, RwEdge, Version};
use crate::{Intent, Ts, TxnError, TxnStatus};

/// The SSI hook (§8.2): every rw-antidependency edge the read produced.
/// C-T3 implements the real observer; [`NoSsi`] ignores edges.
pub trait ReadObserver {
    fn on_edge(&mut self, edge: RwEdge);
}

/// A [`ReadObserver`] that records nothing.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoSsi;

impl ReadObserver for NoSsi {
    fn on_edge(&mut self, _edge: RwEdge) {}
}

/// Reads one logical key at the reader's snapshot (§4). Returns the visible
/// value, or `None` for not-found (including tombstones and moved
/// tombstones).
pub fn read_key<K: OrderedKv, S: Snapshot>(
    core: &Core<K>,
    view: &S,
    key: &[u8],
    ctx: &ReadCtx,
    observer: &mut dyn ReadObserver,
) -> Result<Option<Vec<u8>>, TxnError> {
    let entries = collect_entries(view, key)?;
    read_entries(core, &entries, ctx, observer)
}

/// Scans logical keys in `range` (over logical keys, not stored keys),
/// returning the visible rows in key order. Each logical key is read once;
/// intents and versions are grouped by [`parse_key`].
pub fn scan<K: OrderedKv, S: Snapshot>(
    core: &Core<K>,
    view: &S,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    ctx: &ReadCtx,
    observer: &mut dyn ReadObserver,
) -> Result<Vec<(Key, Vec<u8>)>, TxnError> {
    let mut out = Vec::new();
    let mut current: Option<(Key, KeyEntries)> = None;
    let stored_range = map_bounds(range);
    let (lo, hi) = borrow_bounds(&stored_range);
    for entry in view.scan((lo, hi), false) {
        let (stored, value) = entry.map_err(kv_err)?;
        let Some((logical, kind)) = parse_key(&stored) else {
            continue; // end keys, system keys, anything not ours
        };
        match kind {
            Entry::Intent => {
                let intent = decode_intent(&value)?;
                match &mut current {
                    Some((cur, entries)) if cur.as_slice() == logical => {
                        entries.intent = Some(intent);
                    }
                    _ => {
                        flush(core, &current, ctx, observer, &mut out)?;
                        current = Some((
                            logical.to_vec(),
                            KeyEntries {
                                intent: Some(intent),
                                versions: Vec::new(),
                            },
                        ));
                    }
                }
            }
            Entry::Version(ts) => {
                let value = decode_version(&value)?.into_option();
                match &mut current {
                    Some((cur, entries)) if cur.as_slice() == logical => {
                        entries.versions.push((ts, value));
                    }
                    _ => {
                        flush(core, &current, ctx, observer, &mut out)?;
                        current = Some((
                            logical.to_vec(),
                            KeyEntries {
                                intent: None,
                                versions: vec![(ts, value)],
                            },
                        ));
                    }
                }
            }
        }
    }
    flush(core, &current, ctx, observer, &mut out)?;
    Ok(out)
}

/// Runs the §4 rule for one logical key's entries and returns the visible
/// value, if any.
fn read_entries<K: OrderedKv>(
    core: &Core<K>,
    entries: &KeyEntries,
    ctx: &ReadCtx,
    observer: &mut dyn ReadObserver,
) -> Result<Option<Vec<u8>>, TxnError> {
    // §4: the status lookup for the owner of an intent found in a view never
    // misses (I-TRUNC). Do it eagerly: a missing current-epoch entry is the
    // fatal invariant error, regardless of the intent's data.
    let status = match &entries.intent {
        Some(i) => core.status.lookup_for_intent(i.txn)?,
        None => TxnStatus::Pending, // not consulted: no intent
    };
    let mut edges = Vec::new();
    let versions = entries.versions.iter().map(|(ts, value)| Version {
        ts: *ts,
        value: value.as_deref(),
    });
    let read = visibility_read(
        ctx,
        entries.intent.as_ref(),
        |_| status,
        versions,
        &mut edges,
    );
    for edge in edges {
        observer.on_edge(edge);
    }
    Ok(match read {
        Read::Found(v) => Some(v.to_vec()),
        Read::NotFound => None,
    })
}

/// One logical key's entries from one view: its intent (if present) and its
/// versions, newest first (the stored order).
struct KeyEntries {
    intent: Option<Intent>,
    versions: Vec<(Ts, Option<Vec<u8>>)>,
}

/// Collects exactly the entries of one logical key: the range
/// `[intent_key(key), end_key(key))` holds the intent and versions of `key`
/// and nothing else (§2.2).
fn collect_entries<S: Snapshot>(view: &S, key: &[u8]) -> Result<KeyEntries, TxnError> {
    let mut entries = KeyEntries {
        intent: None,
        versions: Vec::new(),
    };
    let lo = intent_key(key);
    let hi = end_key(key);
    for entry in view.scan(
        (
            Bound::Included(lo.as_slice()),
            Bound::Excluded(hi.as_slice()),
        ),
        false,
    ) {
        let (stored, value) = entry.map_err(kv_err)?;
        match parse_key(&stored) {
            Some((_, Entry::Intent)) => entries.intent = Some(decode_intent(&value)?),
            Some((_, Entry::Version(ts))) => {
                entries
                    .versions
                    .push((ts, decode_version(&value)?.into_option()));
            }
            None => {}
        }
    }
    Ok(entries)
}

/// Maps a logical-key range to the stored-key range that covers exactly the
/// entries of the logical keys inside it.
fn map_bounds(range: (Bound<&[u8]>, Bound<&[u8]>)) -> (Bound<Key>, Bound<Key>) {
    let lower = match range.0 {
        Bound::Included(l) => Bound::Included(intent_key(l)),
        Bound::Excluded(l) => Bound::Excluded(end_key(l)),
        Bound::Unbounded => Bound::Unbounded,
    };
    let upper = match range.1 {
        Bound::Included(l) => Bound::Excluded(end_key(l)),
        Bound::Excluded(l) => Bound::Excluded(intent_key(l)),
        Bound::Unbounded => Bound::Unbounded,
    };
    (lower, upper)
}

/// Borrows one bound of [`map_bounds`] for a `Snapshot::scan` call.
fn borrow_bound(b: &Bound<Key>) -> Bound<&[u8]> {
    match b {
        Bound::Included(k) => Bound::Included(k.as_slice()),
        Bound::Excluded(k) => Bound::Excluded(k.as_slice()),
        Bound::Unbounded => Bound::Unbounded,
    }
}

/// Borrows the bounds of [`map_bounds`] for a `Snapshot::scan` call.
fn borrow_bounds(range: &(Bound<Key>, Bound<Key>)) -> (Bound<&[u8]>, Bound<&[u8]>) {
    (borrow_bound(&range.0), borrow_bound(&range.1))
}

/// Emits the pending group, if any.
fn flush<K: OrderedKv>(
    core: &Core<K>,
    current: &Option<(Key, KeyEntries)>,
    ctx: &ReadCtx,
    observer: &mut dyn ReadObserver,
    out: &mut Vec<(Key, Vec<u8>)>,
) -> Result<(), TxnError> {
    if let Some((logical, entries)) = current {
        if let Some(value) = read_entries(core, entries, ctx, observer)? {
            out.push((logical.clone(), value));
        }
    }
    Ok(())
}
