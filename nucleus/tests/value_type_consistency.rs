//! T0.1 guard — `Value` integer-width consistency (the root of the numeric
//! silent-wrong-results family and the duplicate-PK data-integrity blocker).
//!
//! Before the fix, `Value` derived variant-strict `PartialEq`/`Hash`
//! (`Int32(3) != Int64(3)`, different hash buckets) while `Ord` coerced across
//! widths — violating the std `Ord`/`Eq` agreement contract. That made every
//! ordering-keyed structure (`BTreeMap`, sort, the on-disk B-tree) disagree with
//! every equality/hash-keyed one (`HashMap`, `HashSet`, `==`), silently returning
//! wrong results once the same value was stored at two widths (`VALUES` -> `Int32`,
//! `INSERT ... SELECT`/`generate_series` -> `Int64`).
//!
//! These are Value-level asserts (no engine needed); the engine-level end-to-end
//! repros live alongside the on-disk index-key fix.

use nucleus::types::Value;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

fn hash_of(v: &Value) -> u64 {
    let mut s = DefaultHasher::new();
    v.hash(&mut s);
    s.finish()
}

#[test]
fn int32_and_int64_same_value_are_consistent() {
    let a = Value::Int32(3);
    let b = Value::Int64(3);
    assert_eq!(a, b, "Int32(3) must equal Int64(3)");
    assert_eq!(b, a, "equality must be symmetric");
    assert_eq!(a.cmp(&b), Ordering::Equal, "Ord must agree with Eq");
    assert_eq!(hash_of(&a), hash_of(&b), "equal values must hash equal");
}

#[test]
fn int_width_inequalities_preserved() {
    assert_ne!(Value::Int32(3), Value::Int64(4));
    assert_ne!(Value::Int32(3), Value::Int32(4));
    // A genuinely different magnitude at wider width stays distinct.
    assert_ne!(
        Value::Int64(i64::from(i32::MAX) + 1),
        Value::Int32(i32::MAX)
    );
}

#[test]
fn int_float_equality_stays_strict() {
    // Scope guard: Int<->Float is deliberately NOT folded in Eq/Hash (columns are
    // single-typed; folding would perturb DISTINCT/GROUP BY). `Ord` still coerces
    // them — a separate, pre-existing inconsistency left for a later change.
    assert_ne!(Value::Int32(3), Value::Float64(3.0));
    assert_ne!(Value::Int64(3), Value::Float64(3.0));
}

#[test]
fn hashset_dedups_equal_ints_across_width() {
    let mut s = HashSet::new();
    s.insert(Value::Int32(7));
    s.insert(Value::Int64(7));
    assert_eq!(
        s.len(),
        1,
        "Int32(7) and Int64(7) must collapse to one entry (DISTINCT/UNIQUE correctness)"
    );
    assert!(s.contains(&Value::Int64(7)));
    assert!(s.contains(&Value::Int32(7)));
}

#[test]
fn array_element_width_is_transparent() {
    // Element-wise equality/hashing must also be width-agnostic.
    assert_eq!(
        Value::Array(vec![Value::Int32(1), Value::Int64(2)]),
        Value::Array(vec![Value::Int64(1), Value::Int32(2)]),
    );
    assert_eq!(
        hash_of(&Value::Array(vec![Value::Int32(1)])),
        hash_of(&Value::Array(vec![Value::Int64(1)])),
    );
}

/// Root-invariant lock: for a representative matrix of values, `Eq` must imply both
/// `Ord::Equal` and hash-equality, and `Eq` must be symmetric. Any future edit that
/// desyncs `PartialEq`/`Ord`/`Hash` (the defect that started this) fails here.
///
/// Note: we assert the forward direction (`a == b` ⟹ `cmp==Equal` ∧ equal hash), not
/// the reverse — `Ord` deliberately still coerces `Int`↔`Float` (`Int32(3).cmp(Float64(3.0))
/// == Equal`) while `Eq` keeps them distinct, a scoped, pre-existing inconsistency.
#[test]
fn comparator_consistency_matrix() {
    let vals = vec![
        Value::Int32(0),
        Value::Int64(0),
        Value::Int32(3),
        Value::Int64(3),
        Value::Int32(-1),
        Value::Int64(-1),
        Value::Int32(i32::MAX),
        Value::Int64(i32::MAX as i64),
        Value::Int64(i64::MAX),
        Value::Float64(3.0),
        Value::Text("3".into()),
        Value::Bool(true),
        Value::Null,
    ];
    for a in &vals {
        for b in &vals {
            assert_eq!(a == b, b == a, "eq not symmetric: {a:?} vs {b:?}");
            if a == b {
                assert_eq!(
                    a.cmp(b),
                    Ordering::Equal,
                    "{a:?} == {b:?} but cmp() != Equal (Ord/Eq disagree)"
                );
                assert_eq!(
                    hash_of(a),
                    hash_of(b),
                    "{a:?} == {b:?} but hashes differ (Hash/Eq disagree)"
                );
            }
        }
    }
}
