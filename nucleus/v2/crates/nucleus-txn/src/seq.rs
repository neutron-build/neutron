//! C-T0 §10 "Sequences" (card C-T7): crash-safe, non-transactional value
//! allocation — the substrate the SQL layer's `CREATE SEQUENCE` /
//! `nextval()` will sit on.
//!
//! Values are allocated in blocks of [`BLOCK`] (the card's `B = 32`). Per
//! sequence id, the state is the persisted high-water mark `H`
//! (`/sys/seq/{id}`, u64, [`Durability::Yes`] on every write, §2.3's
//! discipline) and an in-memory cursor `c ∈ (H - block, H]`:
//!
//! - first use of an id persists `H = 0` synced, before any value returns;
//! - `c < H` returns `c + 1` with no KV write;
//! - `c == H` persists `H + B` synced **first**; only once that write has
//!   returned do `H += B`, `c = H - B` and the first value of the new
//!   block come back. A KV error propagates — no value is handed out
//!   after a failed sync.
//!
//! Every handed-out value is therefore covered by a synced block, so
//! across crashes, torn writes and concurrent callers values may skip
//! (a reserved-but-unopened block is lost at boot: the cursor restarts at
//! `H`) but never repeat.
//!
//! Sequences are off the commit path (§3) and outside every txn's write
//! set: `seq_next` writes `/sys/seq/*` directly, never places intents and
//! never touches the status table, the registry, latches or the SSI state.
//! `/sys/seq/*` is never dropped by the GC filter, like every `/sys/` key
//! (§10).
//!
//! # Where the state lives
//!
//! `Core` itself lives in `boot.rs`, which this card does not touch, so
//! the per-core module state cannot be a new `Core` field. It lives in a
//! process-wide sidecar instead, one entry per core, identified by the
//! core's `fail_stop` hook `Arc`: `Core::open` allocates a fresh
//! `Arc<AbortFailStop>` per core (and a pipeline attaches its own hook
//! `Arc`), so `Arc::ptr_eq` on the current hook is an exact per-core
//! identity — two live cores never compare equal, and because each entry
//! holds a strong reference to its token, an allocation can never be
//! reused and mistaken for a live core (the hazard of keying by `&Core`
//! addresses, which the allocator hands to the next core after a drop).
//! Entries whose core is gone are swept on insert, when the entry's
//! reference is the only one left (`Arc::strong_count == 1`: a live core
//! holds another in its `fail_stop` field). Replacing the hook (a
//! pipeline attach after a sequence was used) simply starts a fresh
//! module: every cursor reloads from `H` — allowed skips, never a repeat.
//!
//! The one assumption: a `FailStop` hook `Arc` is never installed on two
//! live cores at once over different stores (every in-crate caller
//! allocates per core/pipeline; sharing one hook across cores would share
//! their cursors — values still never repeat, but a fresh store's sequence
//! would not start at 1 and its handouts would not be covered by its own
//! synced `H`).
//!
//! The sidecar list lock is held only to find a core's module, never
//! across a module lock or a KV operation.
//!
//! # Concurrency
//!
//! One mutex guards each core's module (the seq mutex). It is a leaf: no
//! latch, registry or SSI/graph mutex is taken while it is held — only KV
//! reads/writes, which take no crate locks. The block reservation's sync
//! runs **inside** the seq mutex: releasing the mutex between observing
//! `c == H` and finishing the synced write would let a second caller
//! re-observe `c == H` and start the *same* `H + B` reservation
//! concurrently (two syncs of one `H`, then overlapping values handed out
//! of one block), and ruling that out without holding the mutex means
//! parking later callers on in-flight reservations — wait machinery on a
//! path §3 keeps off the commit thread and that syncs once per [`BLOCK`]
//! values. Holding the leaf mutex across one fsync per block cannot
//! deadlock; concurrent callers serialise behind it, which the concurrency
//! test observes as exactly one reservation per block and gap-free
//! distinct values.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock, PoisonError};

use nucleus_kv::{Batch, Durability, Key, OrderedKv};

use crate::boot::Core;
use crate::commit::FailStop;
use crate::encoding::SYS_PREFIX;
use crate::TxnError;

/// The block size `B` (§10; card C-T7): one synced `/sys/seq` write per
/// `BLOCK` values. A constant in production; tests may shrink it through
/// [`Core::seq_set_block_for_tests`] (no config surface).
pub const BLOCK: u64 = 32;

/// `/sys/seq/{id}` (§2.3 discipline): `be64(H)`, the sequence's persisted
/// high-water mark. Every write carries `Durability::Yes`.
pub fn sys_seq_key(id: u64) -> Key {
    let mut k = SYS_PREFIX.to_vec();
    k.extend_from_slice(b"seq/");
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// Decodes a `/sys/seq/{id}` value: exactly `be64`, anything else is
/// [`TxnError::Corrupt`] (reported, never a panic).
fn decode_hwm(id: u64, v: &[u8]) -> Result<u64, TxnError> {
    if v.len() == 8 {
        let mut b = [0u8; 8];
        b.copy_from_slice(v);
        Ok(u64::from_be_bytes(b))
    } else {
        Err(TxnError::Corrupt(format!(
            "/sys/seq/{id} value of {} bytes, expected 8",
            v.len()
        )))
    }
}

/// Per-id state: the high-water mark `H` already persisted (synced) and
/// the in-memory cursor `c ∈ (H - block, H]`. `reservations` counts block
/// reservations (test introspection: exactly one per block).
struct SeqState {
    hwm: u64,
    cursor: u64,
    reservations: u64,
}

/// One core's seq module: the block size and the per-id states.
struct SeqModule {
    block: u64,
    seqs: HashMap<u64, SeqState>,
}

/// A sidecar entry: the owning core's identity token (a strong reference
/// to its `fail_stop` hook `Arc`, compared with `Arc::ptr_eq`) and the
/// module. See the module docs for why the state lives here.
struct ModuleEntry {
    token: Arc<dyn FailStop>,
    module: Arc<Mutex<SeqModule>>,
}

static MODULES: OnceLock<Mutex<Vec<ModuleEntry>>> = OnceLock::new();

fn modules() -> &'static Mutex<Vec<ModuleEntry>> {
    MODULES.get_or_init(|| Mutex::new(Vec::new()))
}

/// This core's seq module, created on first use. The list lock is dropped
/// before the returned module is locked.
fn module_of(core: &Core<impl OrderedKv>) -> Arc<Mutex<SeqModule>> {
    let token = Arc::clone(
        &core
            .fail_stop
            .lock()
            .unwrap_or_else(PoisonError::into_inner),
    );
    let mut entries = modules().lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(entry) = entries.iter().find(|e| Arc::ptr_eq(&e.token, &token)) {
        return Arc::clone(&entry.module);
    }
    // Sweep dead cores: their entry's reference is the token's last one
    // (a live core holds another in its fail_stop field). Sweeping under
    // the list lock is race-free; a core dying concurrently simply keeps
    // its entry until the next insert.
    entries.retain(|e| Arc::strong_count(&e.token) > 1);
    let module = Arc::new(Mutex::new(SeqModule {
        block: BLOCK,
        seqs: HashMap::new(),
    }));
    entries.push(ModuleEntry {
        token,
        module: Arc::clone(&module),
    });
    module
}

impl<K: OrderedKv> Core<K> {
    /// `nextval(id)` (§10 "Sequences"): returns the next value of `id`,
    /// creating the sequence on first use. Non-transactional: not on the
    /// commit path, not part of any txn's write set. Values may skip
    /// (across boots, and on a failed reservation retry), never repeat.
    ///
    /// Overflow never wraps: a sequence whose `H + B` would exceed `u64`,
    /// or whose next value would not fit `i64`, reports
    /// [`TxnError::Invariant`] without consuming a value. (`TxnError` has
    /// no 22003-style variant yet; the SQL layer maps it when it gains
    /// one.)
    pub fn seq_next(&self, id: u64) -> Result<i64, TxnError> {
        let module = module_of(self);
        let mut m = module.lock().unwrap_or_else(PoisonError::into_inner);
        let block = m.block;
        let st = match m.seqs.entry(id) {
            std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
            std::collections::hash_map::Entry::Vacant(e) => {
                // First use of id on this core = its boot (the card's
                // boot rule, loaded lazily per id): H present → cursor at
                // H (the reserved block's unopened remainder is lost:
                // skips allowed); H absent → create the sequence at
                // H = 0, synced, before any value returns. A latest-state
                // read is safe without a latch: /sys/seq/* never carries
                // an intent (no §7.3 removal race exists), and this
                // module is the key's only writer, under this mutex.
                let hwm = match self.latest_get(&sys_seq_key(id))? {
                    Some(v) => decode_hwm(id, &v)?,
                    None => {
                        let mut batch = Batch::default();
                        batch.put(sys_seq_key(id), 0u64.to_be_bytes().to_vec());
                        self.write(batch, Durability::Yes)?;
                        0
                    }
                };
                e.insert(SeqState {
                    hwm,
                    cursor: hwm,
                    reservations: 0,
                })
            }
        };
        if st.cursor == st.hwm {
            // Reserve the next block: the synced write happens BEFORE any
            // value of the block can exist (I-ACK-style: handout follows
            // the sync), inside the seq mutex (module docs, Concurrency).
            let new_hwm = st.hwm.checked_add(block).ok_or_else(|| {
                TxnError::Invariant(format!("sequence {id}: high-water mark exhausted u64"))
            })?;
            let mut batch = Batch::default();
            batch.put(sys_seq_key(id), new_hwm.to_be_bytes().to_vec());
            self.write(batch, Durability::Yes)?;
            st.hwm = new_hwm;
            st.cursor = new_hwm - block;
            st.reservations += 1;
        }
        // Here cursor < hwm, so cursor + 1 cannot overflow u64. Hand out
        // cursor + 1; on an i64 overflow the value is not consumed (the
        // cursor stands still and the next call reports the same error)
        // rather than wrapping or repeating.
        let value = st.cursor + 1;
        let value = i64::try_from(value).map_err(|_| {
            TxnError::Invariant(format!("sequence {id}: value {value} exceeds i64::MAX"))
        })?;
        st.cursor = value as u64;
        Ok(value)
    }

    /// Test-only (card C-T7): shrinks the block size — no config surface
    /// in production, where [`BLOCK`] is the constant. Affects reservations
    /// from this call on; callers set it before the first `seq_next(id)`.
    /// `0` clamps to `1` (a block must hold at least one value).
    #[doc(hidden)]
    pub fn seq_set_block_for_tests(&self, block: u64) {
        let module = module_of(self);
        module.lock().unwrap_or_else(PoisonError::into_inner).block = block.max(1);
    }

    /// Test-only (card C-T7): block reservations performed for `id` on
    /// this core. `None` before the first `seq_next(id)`.
    #[doc(hidden)]
    pub fn seq_reservations_for_tests(&self, id: u64) -> Option<u64> {
        let module = module_of(self);
        let m = module.lock().unwrap_or_else(PoisonError::into_inner);
        m.seqs.get(&id).map(|st| st.reservations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seq_key_is_sys_prefixed_be64() {
        let k = sys_seq_key(7);
        assert!(k.starts_with(b"/sys/seq/"));
        assert_eq!(&k[k.len() - 8..], &7u64.to_be_bytes());
        assert_eq!(
            sys_seq_key(0),
            [b"/sys/seq/".as_slice(), &[0u8; 8]].concat()
        );
    }

    #[test]
    fn seq_keys_do_not_collide_across_ids_or_sys_keys() {
        let mut keys = vec![sys_seq_key(0), sys_seq_key(1), sys_seq_key(u64::MAX)];
        keys.push(crate::encoding::sys_epoch_key());
        keys.push(crate::encoding::sys_ts_hwm_key());
        keys.push(crate::encoding::sys_txn_prefix());
        let mut sorted = keys.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(keys.len(), sorted.len(), "seq keys must be distinct");
    }

    #[test]
    fn hwm_decode_rejects_bad_lengths() {
        assert_eq!(decode_hwm(1, &500u64.to_be_bytes()).expect("decodes"), 500);
        assert_eq!(decode_hwm(1, &[0u8; 8]).expect("decodes"), 0);
        assert!(matches!(decode_hwm(1, b""), Err(TxnError::Corrupt(_))));
        assert!(matches!(
            decode_hwm(1, &[0u8; 9]),
            Err(TxnError::Corrupt(_))
        ));
    }

    #[test]
    fn block_is_32() {
        assert_eq!(BLOCK, 32);
    }
}
