//! The private per-txn write set (card X-1a Work 3; PLAN-v3 §2 N2, §3.4).
//!
//! Writes never touch shared version storage: each txn buffers them here
//! as an ordered map, applies them to the version store at commit inside
//! [`crate::common::commit_with`], and drops them on abort — so an abort
//! leaves no trace and a commit's versions appear atomically.
//!
//! Invariants owned here:
//! - W-UNDO-EXACT: for every live savepoint `s`, `rollback_to(s)`
//!   restores the map byte-for-byte to its state when `s` was taken, and
//!   destroys every savepoint taken after `s` (C-T0 §5.5: "only the
//!   layers this attempt pushed or modified are dropped").
//! - W-SP-MONOTONE: savepoint ids come from a monotonic counter and are
//!   never reused, so a stale id can never alias a live savepoint after a
//!   rollback.
//! - W-BYTES: `bytes()` is the tracked memory metric of the write set
//!   (an E1′ metric of the card's Goal): the ordered map's key + value
//!   bytes plus the undo log's, at one byte of accounting per tombstone
//!   or captured-absent marker.
//! - W-MERGE: `merge_scan` overlays own writes (inserts, replaces and
//!   deletes) over a committed iterator in ascending key order without
//!   touching the base (C-T0 §4 step 1 `T == R`).

use std::collections::BTreeMap;

use crate::api::SavepointId;

/// Value bytes, where an absent value (a tombstone in the map, a
/// captured-absent marker in the undo log) costs one byte (W-BYTES).
fn val_bytes(value: Option<&Vec<u8>>) -> u64 {
    value.map_or(1, |v| v.len() as u64)
}

/// The inner value of a captured undo state, if it was present.
fn inner(v: &Option<Option<Vec<u8>>>) -> Option<&Vec<u8>> {
    v.as_ref().and_then(|x| x.as_ref())
}

/// Key bytes plus value bytes (W-BYTES).
fn entry_bytes(key: &[u8], value: &Option<Vec<u8>>) -> u64 {
    key.len() as u64 + val_bytes(value.as_ref())
}

/// One undo-log entry: the map's state for `key` before the write tagged
/// `sp` landed (outer `None` = the key was absent from the map).
struct UndoEntry {
    sp: u32,
    key: Vec<u8>,
    prev: Option<Option<Vec<u8>>>,
}

/// The write set of one open txn (see the module docs).
pub struct WriteSet {
    map: BTreeMap<Vec<u8>, Option<Vec<u8>>>,
    undo: Vec<UndoEntry>,
    bytes: u64,
    next_sp: u32,
    cur_sp: u32,
}

impl WriteSet {
    /// An empty write set with no savepoints.
    ///
    /// Writes made before any savepoint are tagged with era `0`;
    /// savepoint ids therefore start at `1` (W-SP-MONOTONE), so
    /// `rollback_to` on any real savepoint can never reach into them.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            undo: Vec::new(),
            bytes: 0,
            next_sp: 1,
            cur_sp: 0,
        }
    }

    /// Take a savepoint (W-SP-MONOTONE): allocates the next id and tags
    /// subsequent writes with it.
    pub fn savepoint(&mut self) -> SavepointId {
        let id = self.next_sp;
        self.next_sp += 1;
        self.cur_sp = id;
        SavepointId(id)
    }

    /// Roll back to a savepoint (W-UNDO-EXACT): replay the undo log from
    /// newest to oldest, restoring every entry tagged at or after `sp`,
    /// then discard those entries. Writes that predate `sp` stay.
    pub fn rollback_to(&mut self, sp: SavepointId) {
        loop {
            let undo_above = match self.undo.last() {
                Some(entry) => entry.sp >= sp.0,
                None => false,
            };
            if !undo_above {
                break;
            }
            let entry = match self.undo.pop() {
                Some(e) => e,
                None => break,
            };
            // Drop the undo record's own cost (W-BYTES).
            self.bytes = self
                .bytes
                .saturating_sub(entry.key.len() as u64 + val_bytes(inner(&entry.prev)));
            // Remove the cost of the write being undone. The map's current
            // state for this key is exactly that write, because the log is
            // replayed newest-first.
            if let Some(current) = self.map.get(&entry.key) {
                self.bytes = self.bytes.saturating_sub(entry_bytes(&entry.key, current));
            }
            match entry.prev {
                None => {
                    self.map.remove(&entry.key);
                }
                Some(prev) => {
                    self.bytes += entry_bytes(&entry.key, &prev);
                    self.map.insert(entry.key, prev);
                }
            }
        }
        // Later savepoints were destroyed by this rollback; new writes
        // belong to `sp`'s era, so a repeated rollback_to(sp) undoes them.
        self.cur_sp = sp.0;
    }

    /// Record one write: `Some(v)` upserts, `None` deletes. Tagged with
    /// the current savepoint era for [`WriteSet::rollback_to`].
    pub fn set(&mut self, key: &[u8], value: Option<Vec<u8>>) {
        let prev = self.map.get(key).cloned();
        self.undo.push(UndoEntry {
            sp: self.cur_sp,
            key: key.to_vec(),
            prev: prev.clone(),
        });
        // The undo record costs its key plus the prior state it captures.
        self.bytes += key.len() as u64 + val_bytes(inner(&prev));
        if let Some(old) = prev.as_ref() {
            self.bytes = self.bytes.saturating_sub(entry_bytes(key, old));
        }
        self.bytes += entry_bytes(key, &value);
        self.map.insert(key.to_vec(), value);
    }

    /// The own write for `key`, if any: `Some(&None)` is an own delete
    /// (A-OWN).
    pub fn get(&self, key: &[u8]) -> Option<&Option<Vec<u8>>> {
        self.map.get(key)
    }

    /// The writes in ascending key order (used by commit's apply).
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Option<Vec<u8>>)> {
        self.map.iter()
    }

    /// Overlay own writes on a committed iterator of `[lo, hi)` entries
    /// (W-MERGE): own values replace base values, own deletes suppress
    /// them, and own inserts appear in order. The result is ascending; the
    /// caller applies its `limit` **after** the merge (a pre-limited base
    /// would miss own keys below the base's cutoff). An empty interval
    /// (`lo >= hi`) yields nothing.
    pub fn merge_scan<I>(&self, base: I, lo: &[u8], hi: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)>
    where
        I: IntoIterator<Item = (Vec<u8>, Vec<u8>)>,
    {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        if lo >= hi {
            return out;
        }
        let mut base = base.into_iter().peekable();
        let mut own = self.map.range(lo.to_vec()..hi.to_vec()).peekable();
        loop {
            match (base.peek(), own.peek()) {
                (None, None) => break,
                (Some((bk, _)), Some((ok, ov))) => {
                    if bk == *ok {
                        // The own write wins, including suppression by a
                        // delete.
                        if let Some(v) = ov.as_ref() {
                            out.push(((*ok).clone(), v.clone()));
                        }
                        base.next();
                        own.next();
                    } else if bk < *ok {
                        if let Some(kv) = base.next() {
                            out.push(kv);
                        }
                    } else {
                        // An own key below the next base key: emit it (a
                        // delete emits nothing) and **always** advance the
                        // own iterator — a delete that skips its advance
                        // would spin forever here.
                        if let Some(v) = ov.as_ref() {
                            out.push(((*ok).clone(), v.clone()));
                        }
                        own.next();
                    }
                }
                (Some(_), None) => {
                    if let Some(kv) = base.next() {
                        out.push(kv);
                    }
                }
                (None, Some((ok, ov))) => {
                    if let Some(v) = ov.as_ref() {
                        out.push(((*ok).clone(), v.clone()));
                    }
                    own.next();
                }
            }
        }
        out
    }

    /// The tracked memory metric of this write set (W-BYTES).
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// Whether no write is buffered.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

impl Default for WriteSet {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::WriteSet;

    fn opt(v: &str) -> Option<Vec<u8>> {
        Some(v.as_bytes().to_vec())
    }

    /// W-MERGE: replace, suppress, insert and ordering. The base iterator
    /// is the committed data already bounded to `[lo, hi)` (the engine
    /// bounds it; the merge only overlays own writes). An own delete of a
    /// key that is not in the base must still be skipped past (the
    /// regression this pins: a delete that failed to advance its iterator
    /// spun the merge forever).
    #[test]
    fn merge_scan_overlays_own_writes() {
        let mut ws = WriteSet::new();
        ws.set(b"a", opt("own-a"));
        ws.set(b"b", None); // own delete suppresses the base entry
        ws.set(b"b1", None); // own delete of a key the base never had
        ws.set(b"c1", opt("own-c1")); // insert between base b and c
        ws.set(b"zz", opt("out")); // outside the range
        let base = vec![
            (b"a".to_vec(), b"base-a".to_vec()),
            (b"b".to_vec(), b"base-b".to_vec()),
            (b"c".to_vec(), b"base-c".to_vec()),
        ];
        let merged = ws.merge_scan(base, b"a", b"d");
        assert_eq!(
            merged,
            vec![
                (b"a".to_vec(), b"own-a".to_vec()),
                (b"c".to_vec(), b"base-c".to_vec()),
                (b"c1".to_vec(), b"own-c1".to_vec()),
            ]
        );
    }

    /// W-UNDO-EXACT, the minimal shape: rollback restores the previous
    /// value of a replaced key and removes an inserted key entirely, and
    /// never reaches into writes that predate the savepoint.
    /// (The SP-ROLLBACK-PARTIAL mutant of the card keeps the last write
    /// and fails here.)
    #[test]
    fn rollback_restores_exactly() {
        let mut ws = WriteSet::new();
        ws.set(b"pre", opt("stays"));
        ws.set(b"k", opt("v1"));
        let sp = ws.savepoint();
        ws.set(b"k", opt("v2"));
        ws.set(b"n", opt("n1"));
        ws.rollback_to(sp);
        assert_eq!(ws.get(b"k"), Some(&opt("v1")));
        assert_eq!(ws.get(b"n"), None);
        assert_eq!(ws.get(b"pre"), Some(&opt("stays")));
    }

    /// W-BYTES: the metric returns to zero when every write is undone.
    #[test]
    fn bytes_return_to_zero_after_rollback() {
        let mut ws = WriteSet::new();
        let sp = ws.savepoint();
        ws.set(b"k1", opt("v1"));
        ws.set(b"k2", None);
        ws.set(b"k1", opt("longer-v1"));
        assert!(ws.bytes() > 0);
        ws.rollback_to(sp);
        assert_eq!(ws.bytes(), 0);
    }
}
