//! `model.rs` (card X-0 Work 6): a proptest sequence of up to 2,000 mixed
//! ops compared op-by-op against `std::collections::BTreeMap`, with a full
//! `check()` every 100 ops.
//!
//! The pool is 16 frames over `MemStore`, so eviction runs constantly
//! against a ~100-page tree: this file is the named catcher for the
//! DIRTY-LOST, SPLIT-OFF-BY-ONE, SEP-NOT-UPDATED and RANGE-SKIPS-BOUNDARY
//! mutations of the card's evidence list.

use e0_btree::{BTree, MemStore, StoreError};
use proptest::prelude::*;
use std::collections::BTreeMap;

/// One operation of the generated sequence.
#[derive(Clone, Debug)]
enum Op {
    /// Upsert.
    Insert {
        /// Key.
        k: Vec<u8>,
        /// Value.
        v: Vec<u8>,
    },
    /// Delete.
    Delete {
        /// Key.
        k: Vec<u8>,
    },
    /// Point lookup.
    Get {
        /// Key.
        k: Vec<u8>,
    },
    /// Range scan `[lo, hi)`.
    Range {
        /// Low bound (inclusive).
        lo: Vec<u8>,
        /// High bound (exclusive).
        hi: Vec<u8>,
    },
}

/// Keys live in a 0–5,000 space, big-endian so both 1- and 2-byte keys
/// occur (the 0–5k space as u16 be-bytes gives trailing-zero keys too).
fn key() -> impl Strategy<Value = Vec<u8>> {
    (0u16..5_000).prop_map(|k| k.to_be_bytes().to_vec())
}

/// Values of 0–300 random bytes (empty values included).
fn val() -> impl Strategy<Value = Vec<u8>> {
    proptest::collection::vec(any::<u8>(), 0..300)
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        4 => (key(), val()).prop_map(|(k, v)| Op::Insert { k, v }),
        2 => key().prop_map(|k| Op::Delete { k }),
        2 => key().prop_map(|k| Op::Get { k }),
        2 => (key(), key()).prop_map(|(lo, hi)| Op::Range { lo, hi }),
    ]
}

/// Panic on a store error inside a property (tests may panic; the store
/// never may).
fn unwrap<T>(r: Result<T, StoreError>) -> T {
    match r {
        Ok(v) => v,
        Err(e) => panic!("unexpected store error: {e}"),
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn sequence_matches_btree_model(ops in proptest::collection::vec(op(), 0..2_000)) {
        // capacity = 16 frames over MemStore so eviction is constant
        let store = MemStore::new(8192).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let tree = BTree::new(store, 16).map_err(|e| TestCaseError::fail(e.to_string()))?;
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (i, op) in ops.iter().enumerate() {
            match op {
                Op::Insert { k, v } => {
                    let old = unwrap(tree.insert(k, v));
                    let expected = model.insert(k.clone(), v.clone());
                    prop_assert_eq!(old, expected, "insert");
                }
                Op::Delete { k } => {
                    let old = unwrap(tree.delete(k));
                    let expected = model.remove(k);
                    prop_assert_eq!(old, expected, "delete");
                }
                Op::Get { k } => {
                    let got = unwrap(tree.get(k));
                    let expected = model.get(k).cloned();
                    prop_assert_eq!(got, expected, "get");
                }
                Op::Range { lo, hi } => {
                    let got: Vec<(Vec<u8>, Vec<u8>)> =
                        unwrap(tree.range(lo, hi)).collect::<Result<_, _>>()
                        .map_err(|e| TestCaseError::fail(e.to_string()))?;
                    if lo <= hi {
                        let want: Vec<(Vec<u8>, Vec<u8>)> = model
                            .range(lo.clone()..hi.clone())
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();
                        prop_assert_eq!(got, want, "range");
                    } else {
                        // an inverted window is empty by definition
                        prop_assert!(got.is_empty(), "inverted range");
                    }
                }
            }
            if (i + 1) % 100 == 0 {
                if let Err(msg) = tree.check() {
                    prop_assert!(false, "check failed: {}", msg);
                }
            }
        }
    }
}
