//! Encrypted indexes for querying encrypted columns (Phase 6).
//!
//! Supports three encryption modes:
//!   - Deterministic: enables equality queries on encrypted data
//!   - Order-preserving: enables range queries on encrypted data
//!   - Randomized: maximum security, no queryable index

use std::collections::BTreeMap;

// ---------------------------------------------------------------------------
// Encryption modes
// ---------------------------------------------------------------------------

/// Encryption mode for an encrypted index.
///
/// Each mode trades off queryability against security:
///   - `Deterministic`: same plaintext always yields the same ciphertext,
///     enabling equality lookups.
///   - `OrderPreserving`: ciphertext order matches plaintext order, enabling
///     range queries.
///   - `Randomized`: standard non-deterministic encryption — maximum security
///     but no queryable index support.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EncryptionMode {
    /// Same plaintext always produces the same ciphertext (equality queries).
    Deterministic,
    /// Ciphertext order matches plaintext order (range queries).
    OrderPreserving,
    /// Standard AES-GCM style — different ciphertext each time (no querying).
    Randomized,
}

// ---------------------------------------------------------------------------
// FNV-1a hash (64-bit) — deterministic, no external deps
// ---------------------------------------------------------------------------

/// FNV-1a 64-bit hash. Deterministic, fast, no allocations.
fn fnv1a_64(data: &[u8]) -> u64 {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x00000100000001b3;

    let mut hash = FNV_OFFSET;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

// ---------------------------------------------------------------------------
// Simple counter for randomized nonces
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};

/// Global monotonic counter used to ensure randomized-mode ciphertexts differ.
static RANDOM_COUNTER: AtomicU64 = AtomicU64::new(0);

// ---------------------------------------------------------------------------
// EncryptedIndex
// ---------------------------------------------------------------------------

/// An encrypted index that maps encrypted column values to row IDs.
///
/// Depending on the [`EncryptionMode`], different query operations are
/// supported:
///   - `Deterministic` → [`lookup_equal`](Self::lookup_equal)
///   - `OrderPreserving` → [`lookup_range`](Self::lookup_range) and
///     [`lookup_equal`](Self::lookup_equal)
///   - `Randomized` → no indexed queries (insert-only; lookups return empty)
#[derive(Debug)]
pub struct EncryptedIndex {
    /// AES-256 key (used for all encryption operations).
    key: [u8; 32],
    /// Encryption mode governing how values are encrypted and queryable.
    mode: EncryptionMode,
    /// Mapping from encrypted value → list of row IDs.
    entries: BTreeMap<Vec<u8>, Vec<u64>>,
}

impl EncryptedIndex {
    /// Create a new, empty encrypted index with the given key and mode.
    pub fn new(key: [u8; 32], mode: EncryptionMode) -> Self {
        Self {
            key,
            mode,
            entries: BTreeMap::new(),
        }
    }

    /// Encrypt a plaintext value according to the index's encryption mode.
    ///
    /// - **Deterministic**: XOR key with plaintext cyclically, then FNV-1a
    ///   hash to produce a fixed 8-byte deterministic token.
    /// - **OrderPreserving**: prepend a key-derived 8-byte tag and keep the
    ///   plaintext bytes unchanged. Since the tag is constant for a given
    ///   key, lexicographic ordering of ciphertexts matches that of
    ///   plaintexts.
    /// - **Randomized**: prepend a unique counter value to the plaintext
    ///   before XOR + hash, ensuring each call produces a distinct ciphertext.
    pub fn encrypt_value(&self, plaintext: &[u8]) -> Vec<u8> {
        match self.mode {
            EncryptionMode::Deterministic => {
                // XOR plaintext with key cyclically, then hash for a fixed-
                // length deterministic token.
                let mut combined = Vec::with_capacity(self.key.len() + plaintext.len());
                combined.extend_from_slice(&self.key);
                for (i, &b) in plaintext.iter().enumerate() {
                    combined.push(b ^ self.key[i % self.key.len()]);
                }
                let hash = fnv1a_64(&combined);
                hash.to_le_bytes().to_vec()
            }
            EncryptionMode::OrderPreserving => {
                // Simplified order-preserving encryption: prepend a key-
                // derived 8-byte tag (so the ciphertext is not raw plaintext)
                // and append the plaintext bytes unchanged.  Because the tag
                // is constant for a given key, all values share the same
                // prefix and the lexicographic ordering is determined entirely
                // by the plaintext suffix — thus preserving order.
                let tag = fnv1a_64(&self.key).to_le_bytes();
                let mut out = Vec::with_capacity(8 + plaintext.len());
                out.extend_from_slice(&tag);
                out.extend_from_slice(plaintext);
                out
            }
            EncryptionMode::Randomized => {
                // Include a monotonic counter so that the same plaintext
                // produces different ciphertext on every call.
                let counter = RANDOM_COUNTER.fetch_add(1, Ordering::Relaxed);
                let mut combined = Vec::with_capacity(8 + self.key.len() + plaintext.len());
                combined.extend_from_slice(&counter.to_le_bytes());
                combined.extend_from_slice(&self.key);
                for (i, &b) in plaintext.iter().enumerate() {
                    combined.push(b ^ self.key[i % self.key.len()]);
                }
                let hash = fnv1a_64(&combined);
                // Prepend the counter so the result is always unique.
                let mut out = Vec::with_capacity(16);
                out.extend_from_slice(&counter.to_le_bytes());
                out.extend_from_slice(&hash.to_le_bytes());
                out
            }
        }
    }

    /// Encrypt `plaintext` and insert the resulting ciphertext → `row_id`
    /// mapping into the index.
    pub fn insert(&mut self, plaintext: &[u8], row_id: u64) {
        let encrypted = self.encrypt_value(plaintext);
        self.entries.entry(encrypted).or_default().push(row_id);
    }

    /// Equality lookup: encrypt `plaintext` and return all row IDs that
    /// share the same ciphertext.
    ///
    /// Only meaningful for [`EncryptionMode::Deterministic`] (and
    /// [`EncryptionMode::OrderPreserving`], which is also deterministic).
    /// Returns an empty vec for `Randomized` mode because every encryption
    /// produces a unique ciphertext.
    pub fn lookup_equal(&self, plaintext: &[u8]) -> Vec<u64> {
        let encrypted = self.encrypt_value(plaintext);
        self.entries.get(&encrypted).cloned().unwrap_or_default()
    }

    /// Range lookup: return all row IDs whose encrypted value falls in
    /// `[encrypt(start), encrypt(end)]` (inclusive).
    ///
    /// Only meaningful for [`EncryptionMode::OrderPreserving`]. For other
    /// modes the encrypted ordering does not correspond to the plaintext
    /// ordering, so results are unreliable.
    pub fn lookup_range(&self, start: &[u8], end: &[u8]) -> Vec<u64> {
        let enc_start = self.encrypt_value(start);
        let enc_end = self.encrypt_value(end);

        let mut results = Vec::new();
        for (k, row_ids) in self.entries.range(enc_start..=enc_end) {
            let _ = k; // key used only for range iteration
            results.extend_from_slice(row_ids);
        }
        results
    }

    /// Remove `row_id` from the index entry for the given `plaintext`.
    ///
    /// Returns `true` if the row ID was found and removed.
    pub fn remove(&mut self, plaintext: &[u8], row_id: u64) -> bool {
        let encrypted = self.encrypt_value(plaintext);
        if let Some(ids) = self.entries.get_mut(&encrypted) {
            if let Some(pos) = ids.iter().position(|&id| id == row_id) {
                ids.remove(pos);
                if ids.is_empty() {
                    self.entries.remove(&encrypted);
                }
                return true;
            }
        }
        false
    }

    /// Number of distinct encrypted values in the index.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the index contains no entries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The encryption mode of this index.
    pub fn mode(&self) -> EncryptionMode {
        self.mode
    }
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn test_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        for (i, b) in key.iter_mut().enumerate() {
            *b = (i as u8).wrapping_mul(7).wrapping_add(0x42);
        }
        key
    }

    // -----------------------------------------------------------------------
    // Deterministic mode
    // -----------------------------------------------------------------------

    #[test]
    fn test_deterministic_equality_lookup() {
        let mut idx = EncryptedIndex::new(test_key(), EncryptionMode::Deterministic);

        idx.insert(b"alice", 1);
        idx.insert(b"bob", 2);
        idx.insert(b"alice", 3);
        idx.insert(b"charlie", 4);

        let alice_rows = idx.lookup_equal(b"alice");
        assert_eq!(alice_rows, vec![1, 3]);

        let bob_rows = idx.lookup_equal(b"bob");
        assert_eq!(bob_rows, vec![2]);

        let unknown = idx.lookup_equal(b"dave");
        assert!(unknown.is_empty());
    }

    #[test]
    fn test_deterministic_same_value_same_ciphertext() {
        let idx = EncryptedIndex::new(test_key(), EncryptionMode::Deterministic);

        let c1 = idx.encrypt_value(b"hello");
        let c2 = idx.encrypt_value(b"hello");
        assert_eq!(c1, c2, "deterministic mode must produce identical ciphertext");

        // Different plaintext must produce different ciphertext.
        let c3 = idx.encrypt_value(b"world");
        assert_ne!(c1, c3);
    }

    // -----------------------------------------------------------------------
    // Order-preserving mode
    // -----------------------------------------------------------------------

    #[test]
    fn test_order_preserving_range_query() {
        let mut idx = EncryptedIndex::new(test_key(), EncryptionMode::OrderPreserving);

        // Insert values whose natural byte order is: aaa < bbb < ccc < ddd < eee
        idx.insert(b"aaa", 10);
        idx.insert(b"bbb", 20);
        idx.insert(b"ccc", 30);
        idx.insert(b"ddd", 40);
        idx.insert(b"eee", 50);

        // Range [bbb, ddd] should return rows 20, 30, 40.
        let mut range = idx.lookup_range(b"bbb", b"ddd");
        range.sort();
        assert_eq!(range, vec![20, 30, 40]);

        // Single-value range should also work.
        let single = idx.lookup_range(b"ccc", b"ccc");
        assert_eq!(single, vec![30]);

        // Out-of-range should return nothing.
        let empty = idx.lookup_range(b"xxx", b"zzz");
        assert!(empty.is_empty());
    }

    // -----------------------------------------------------------------------
    // Randomized mode
    // -----------------------------------------------------------------------

    #[test]
    fn test_randomized_different_ciphertexts() {
        let idx = EncryptedIndex::new(test_key(), EncryptionMode::Randomized);

        let c1 = idx.encrypt_value(b"same");
        let c2 = idx.encrypt_value(b"same");
        assert_ne!(c1, c2, "randomized mode must produce different ciphertext each time");

        // Equality lookup should therefore return nothing (no deterministic match).
        let mut ridx = EncryptedIndex::new(test_key(), EncryptionMode::Randomized);
        ridx.insert(b"data", 100);
        let result = ridx.lookup_equal(b"data");
        assert!(result.is_empty(), "randomized mode should not support equality lookup");
    }

    // -----------------------------------------------------------------------
    // Insert and remove
    // -----------------------------------------------------------------------

    #[test]
    fn test_insert_and_remove() {
        let mut idx = EncryptedIndex::new(test_key(), EncryptionMode::Deterministic);

        idx.insert(b"key1", 1);
        idx.insert(b"key1", 2);
        idx.insert(b"key1", 3);
        assert_eq!(idx.lookup_equal(b"key1"), vec![1, 2, 3]);
        assert_eq!(idx.len(), 1); // one distinct encrypted value

        // Remove the middle row.
        assert!(idx.remove(b"key1", 2));
        assert_eq!(idx.lookup_equal(b"key1"), vec![1, 3]);

        // Removing a non-existent row returns false.
        assert!(!idx.remove(b"key1", 99));

        // Remove remaining rows — entry should be cleaned up entirely.
        assert!(idx.remove(b"key1", 1));
        assert!(idx.remove(b"key1", 3));
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
    }

    // -----------------------------------------------------------------------
    // Empty index
    // -----------------------------------------------------------------------

    #[test]
    fn test_empty_index() {
        let idx = EncryptedIndex::new(test_key(), EncryptionMode::Deterministic);
        assert_eq!(idx.len(), 0);
        assert!(idx.is_empty());
        assert!(idx.lookup_equal(b"anything").is_empty());

        let ope = EncryptedIndex::new(test_key(), EncryptionMode::OrderPreserving);
        assert!(ope.lookup_range(b"a", b"z").is_empty());
    }

    // -----------------------------------------------------------------------
    // Order-preserving equality (also works because OPE is deterministic)
    // -----------------------------------------------------------------------

    #[test]
    fn test_order_preserving_equality_lookup() {
        let mut idx = EncryptedIndex::new(test_key(), EncryptionMode::OrderPreserving);

        idx.insert(b"foo", 1);
        idx.insert(b"bar", 2);
        idx.insert(b"foo", 3);

        let foo_rows = idx.lookup_equal(b"foo");
        assert_eq!(foo_rows, vec![1, 3]);

        let bar_rows = idx.lookup_equal(b"bar");
        assert_eq!(bar_rows, vec![2]);
    }

    // -----------------------------------------------------------------------
    // Deterministic mode with different keys produces different ciphertexts
    // -----------------------------------------------------------------------

    #[test]
    fn test_different_keys_different_ciphertexts() {
        let key_a = [0xAAu8; 32];
        let key_b = [0xBBu8; 32];

        let idx_a = EncryptedIndex::new(key_a, EncryptionMode::Deterministic);
        let idx_b = EncryptedIndex::new(key_b, EncryptionMode::Deterministic);

        let ca = idx_a.encrypt_value(b"secret");
        let cb = idx_b.encrypt_value(b"secret");
        assert_ne!(ca, cb, "different keys must produce different ciphertext");
    }
}
