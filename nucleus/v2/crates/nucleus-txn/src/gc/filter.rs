//! C-T0 §9.2: the compaction filter for ts-in-key backends. See the
//! [`gc`](crate::gc) module docs for how the durable W reaches it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use nucleus_kv::{GcFilter, GcStream, Key};

use crate::encoding::{parse_key, Entry, SYS_PREFIX};

/// The §9.2 compaction filter. It carries no drop state of its own: every
/// stream built by [`GcFilter::begin`] keeps its own, so filter state never
/// crosses streams or subcompactions (seed 33), and the `W` a stream uses is
/// read **once**, at `begin`, from the durable slot the GC job fills only
/// after `/sys/gc_w` is synced (seed 42) — never a `W` a GC step has not yet
/// made durable.
///
/// [`GcJob::install`](crate::gc::GcJob::install) wires one of these into the
/// KV; tests and the deterministic simulator construct it directly over a
/// shared durable-W slot.
pub struct TxnGcFilter {
    durable_w: Arc<AtomicU64>,
}

impl TxnGcFilter {
    /// A filter reading its W from `durable_w` (shared with the job).
    pub fn new(durable_w: Arc<AtomicU64>) -> TxnGcFilter {
        TxnGcFilter { durable_w }
    }
}

impl GcFilter for TxnGcFilter {
    fn begin(&self) -> Box<dyn GcStream> {
        Box::new(TxnGcStream {
            w: self.durable_w.load(Ordering::SeqCst),
            cur: Key::new(),
            seen_le_w: false,
        })
    }
}

/// One compaction stream's drop state (seed 33: per stream, never shared).
struct TxnGcStream {
    /// The W this stream runs at, read once from the durable slot.
    w: u64,
    /// The logical key whose versions the stream is currently in.
    cur: Key,
    /// Whether a version `<= w` of `cur` has already been seen in this
    /// stream. Entries arrive in ascending key order and versions of one
    /// logical key sort newest first (§2.2), so the first version `<= w` of
    /// a key is its newest `<= w`.
    seen_le_w: bool,
}

impl GcStream for TxnGcStream {
    /// §9.2, on the raw key alone; `value` is not inspected (tombstones are
    /// protected by the newest-`<= W` rule, not by their bytes).
    fn drop_key(&mut self, key: &[u8], _value: &[u8]) -> bool {
        // §10: every /sys/ key except the catalog is never dropped, even
        // when its bytes match the version-key pattern (no catalog exists
        // yet, so no /sys/ key qualifies).
        if key.starts_with(SYS_PREFIX) {
            return false;
        }
        match parse_key(key) {
            // An end key or a foreign layout: keep.
            None => false,
            Some((l, Entry::Intent)) => {
                // Never removed by GC rules (§9.2), only by §7.3. The intent
                // sorts before every version of `l`, so tracking of `l`
                // starts empty.
                self.cur = l.to_vec();
                self.seen_le_w = false;
                false
            }
            Some((l, Entry::Version(ts))) => {
                if l != self.cur.as_slice() {
                    // A new logical key resets the state.
                    self.cur = l.to_vec();
                    self.seen_le_w = false;
                }
                if ts.0 > self.w {
                    // Versions above W are kept, and do not mark `l` seen:
                    // the drop rule only ever fires below a kept newer
                    // version `<= W` (§9.2).
                    return false;
                }
                if self.seen_le_w {
                    // A newer version `<= W` of `l` passed by earlier in
                    // this stream: shadowed, may be dropped.
                    return true;
                }
                // The newest version `<= W` of `l` this stream has seen:
                // kept, tombstone or not (seed 3).
                self.seen_le_w = true;
                false
            }
        }
    }
}
