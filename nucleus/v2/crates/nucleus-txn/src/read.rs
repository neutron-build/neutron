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

use nucleus_kv::{Key, OrderedKv, Snapshot, Value};

use crate::boot::Core;
use crate::encoding::{decode_intent, decode_version, end_key, intent_key, parse_key, Entry};
use crate::kv_err;
use crate::registry::ViewGuard;
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
pub fn read_key<K: OrderedKv>(
    core: &Core<K>,
    view: &ViewGuard<'_, K::Snap>,
    key: &[u8],
    ctx: &ReadCtx,
    observer: &mut dyn ReadObserver,
) -> Result<Option<Vec<u8>>, TxnError> {
    let entries = collect_entries(view.as_snap(), key)?;
    read_entries(core, &entries, ctx, observer)
}

/// Scans logical keys in `range` (over logical keys, not stored keys),
/// returning the visible rows in key order. Each logical key is read once;
/// intents and versions are grouped by [`parse_key`].
///
/// Streaming and lazy (§4 "a scan uses one view for its whole duration"):
/// rows are produced as the view's KV iterator is consumed, and iteration
/// stops at the range end. SSI edges fire as the rows that produce them are
/// pulled. After an error the iterator is exhausted.
pub fn scan<'a, K: OrderedKv>(
    core: &'a Core<K>,
    view: &'a ViewGuard<'_, K::Snap>,
    range: (Bound<&[u8]>, Bound<&[u8]>),
    ctx: &'a ReadCtx,
    observer: &'a mut dyn ReadObserver,
) -> ScanRows<'a, K> {
    let stored_range = map_bounds(range);
    let (lo, hi) = borrow_bounds(&stored_range);
    ScanRows {
        core,
        ctx,
        observer,
        inner: view.scan((lo, hi), false),
        current: None,
        finished: false,
    }
}

/// The iterator behind [`scan`]: pulls stored entries from the view's KV
/// iterator, groups them by logical key, and yields one visible row per
/// completed group.
pub struct ScanRows<'a, K: OrderedKv> {
    core: &'a Core<K>,
    ctx: &'a ReadCtx,
    observer: &'a mut dyn ReadObserver,
    inner: Box<dyn Iterator<Item = nucleus_kv::Result<(Key, Value)>> + 'a>,
    /// The group being collected: its logical key and entries so far.
    current: Option<(Key, KeyEntries)>,
    finished: bool,
}

impl<'a, K: OrderedKv> ScanRows<'a, K> {
    /// Adds `entry` as the start of a new group for `logical`.
    fn start_group(&mut self, logical: &[u8], kind: Entry, value: Value) -> Result<(), TxnError> {
        self.current = Some((logical.to_vec(), KeyEntries::new(kind, value)?));
        Ok(())
    }

    /// Merges `entry` into the group under `logical` (the caller checked it
    /// is the same logical key).
    fn merge_group(&mut self, kind: Entry, value: Value) -> Result<(), TxnError> {
        if let Some((_, entries)) = self.current.as_mut() {
            entries.push(kind, value)?;
        }
        Ok(())
    }

    /// Emits the completed group, if it has a visible row. Takes ownership of
    /// the group so the borrow ends before the caller mutates `self`.
    fn flush(&mut self) -> Result<Option<(Key, Vec<u8>)>, TxnError> {
        match self.current.take() {
            Some((logical, entries)) => {
                let row = read_entries(self.core, &entries, self.ctx, self.observer)?;
                Ok(row.map(|value| (logical, value)))
            }
            None => Ok(None),
        }
    }

    /// Pulls stored entries until one logical row is ready, the range ends,
    /// or an error surfaces.
    fn advance(&mut self) -> Result<Option<(Key, Vec<u8>)>, TxnError> {
        loop {
            let (stored, value) = match self.inner.next() {
                None => {
                    self.finished = true;
                    return self.flush();
                }
                Some(Err(e)) => return Err(kv_err(e)),
                Some(Ok(kv)) => kv,
            };
            let Some((logical, kind)) = parse_key(&stored) else {
                continue; // end keys, system keys, anything not ours
            };
            let same = self
                .current
                .as_ref()
                .is_some_and(|(l, _)| l.as_slice() == logical);
            if same {
                self.merge_group(kind, value)?;
            } else {
                // A new logical key: the previous group is complete.
                let row = self.flush()?;
                self.start_group(logical, kind, value)?;
                if let Some(row) = row {
                    return Ok(Some(row));
                }
            }
        }
    }
}

impl<K: OrderedKv> Iterator for ScanRows<'_, K> {
    type Item = Result<(Key, Vec<u8>), TxnError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.finished {
            return None;
        }
        let row = self.advance();
        if row.is_err() {
            self.finished = true;
        }
        row.transpose()
    }
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

impl KeyEntries {
    /// Collects one stored entry.
    fn new(kind: Entry, value: Value) -> Result<KeyEntries, TxnError> {
        let mut entries = KeyEntries {
            intent: None,
            versions: Vec::new(),
        };
        entries.push(kind, value)?;
        Ok(entries)
    }

    fn push(&mut self, kind: Entry, value: Value) -> Result<(), TxnError> {
        match kind {
            Entry::Intent => self.intent = Some(decode_intent(&value)?),
            Entry::Version(ts) => self
                .versions
                .push((ts, decode_version(&value)?.into_option())),
        }
        Ok(())
    }
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
