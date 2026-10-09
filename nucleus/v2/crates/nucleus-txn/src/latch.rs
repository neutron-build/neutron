//! C-T0 §5.0: latches. A latch is a short striped mutex over a hash of
//! `latch_key(k)`. Held only for check-and-place, intent removal and
//! shared-lock release; never held across waits on other txns, and a thread
//! never holds two latches (§1). Debug builds assert the second rule through
//! a thread-local flag.
//!
//! `latch_key(k)` is defined once and used by every path that
//! reads-then-writes `k@INTENT` (placement, every removal in §7.3, savepoint
//! rollback): the deferrable-unique prefix `/i/{idx}/{key}` for entries of a
//! deferrable unique constraint, `k` itself otherwise.

use std::cell::Cell;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::{Mutex, MutexGuard, PoisonError};

use nucleus_kv::Key;

/// The latch key of `k` (§5.0): `deferrable_prefix` for an
/// `/i/{idx}/{key}{pk}` entry of a deferrable unique constraint (all entries
/// with that key value latch together), `k` otherwise.
pub fn latch_key<'a>(k: &'a [u8], deferrable_prefix: Option<&'a [u8]>) -> &'a [u8] {
    deferrable_prefix.unwrap_or(k)
}

/// Striped latches (§5.0). One fixed stripe set; a key always maps to the
/// same stripe.
pub struct Latches {
    strips: Vec<Mutex<()>>,
}

/// Default stripe count.
pub const DEFAULT_STRIPES: usize = 1024;

#[cfg(debug_assertions)]
thread_local! {
    /// Set while the thread holds a latch; `lock` asserts it is clear, so a
    /// thread that takes a second latch fails fast in debug builds.
    static HOLDING_LATCH: Cell<bool> = const { Cell::new(false) };
}

impl Default for Latches {
    fn default() -> Self {
        Latches::with_stripes(DEFAULT_STRIPES)
    }
}

impl Latches {
    pub fn with_stripes(stripes: usize) -> Self {
        Latches {
            strips: (0..stripes.max(1)).map(|_| Mutex::new(())).collect(),
        }
    }

    fn strip(&self, key: &[u8]) -> usize {
        let mut h = DefaultHasher::new();
        key.hash(&mut h);
        (h.finish() as usize) % self.strips.len()
    }

    /// Takes the latch for `key`. The guard releases it on drop and remembers
    /// `key`, so callers that must hold a specific key's latch (§7.3 step 1)
    /// can be checked.
    pub fn lock(&self, key: &[u8]) -> LatchGuard<'_> {
        // Checked before blocking: a second latch on the same stripe would
        // otherwise self-deadlock before the assert could fire.
        #[cfg(debug_assertions)]
        {
            let already = HOLDING_LATCH.with(Cell::get);
            debug_assert!(
                !already,
                "a thread must never hold two latches (C-T0 §1, §5.0)"
            );
        }
        let guard = self.strips[self.strip(key)]
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(debug_assertions)]
        HOLDING_LATCH.with(|h| h.set(true));
        LatchGuard {
            key: key.to_vec(),
            _guard: guard,
        }
    }
}

/// A held latch. `!Send`: a latch is never held across a wait (§1). The
/// mutex guard field is held only for its `Drop` (the unlock).
pub struct LatchGuard<'a> {
    key: Key,
    _guard: MutexGuard<'a, ()>,
}

impl LatchGuard<'_> {
    /// The latch key this guard locked.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Whether this guard latches exactly `want` (§5.0, §7.3 step 1).
    pub fn protects(&self, want: &[u8]) -> bool {
        self.key == want
    }
}

impl Drop for LatchGuard<'_> {
    fn drop(&mut self) {
        #[cfg(debug_assertions)]
        {
            let holding = HOLDING_LATCH.with(Cell::get);
            debug_assert!(holding, "latch flag unset while a latch is held");
            HOLDING_LATCH.with(|h| h.set(false));
        }
    }
}
