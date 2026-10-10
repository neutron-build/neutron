//! Type-kernel property suite (card C-Q3, work item 3), reusing the C-K2
//! generators and the independent reference comparator in `reference.rs`.
//!
//! (a) `compare` has the sign of `memcmp` of the ASC encodings (and of the
//!     reference comparator, which never touches the encoder);
//! (b) `eq` is `compare == Equal`, and is byte equality of the encodings;
//! (c) `eq` implies equal `hash`, and `hash` is the codec's `hash_value`;
//! (d) `in_bounds` agrees with an exhaustive linear scan over a sample set;
//! (e) nothing panics on extreme or mismatched input.
//!
//! 10_000 cases per property by default; `NUCLEUS_CODEC_CASES` overrides.

mod reference;

use std::cmp::Ordering;
use std::ops::Bound;

use nucleus_codec::{
    compare, compare_key, encode_key, eq, hash, hash_value, in_bounds, try_compare,
    try_compare_key, try_eq, try_hash, try_in_bounds, Array, Collation, Decimal, Interval, Jsonb,
    KeyColumn, KeyType, Numeric, Value,
};
use proptest::collection::vec;
use proptest::prelude::*;
use reference::{
    composite_key_pair, key_cmp, key_type, key_type_with_pair, key_type_with_value, sql_cmp,
    typed_pair,
};

fn cfg() -> ProptestConfig {
    let cases = std::env::var("NUCLEUS_CODEC_CASES")
        .ok()
        .map(|s| {
            s.parse::<u32>()
                .unwrap_or_else(|e| panic!("bad NUCLEUS_CODEC_CASES {s:?}: {e}"))
        })
        .unwrap_or(10_000);
    let mut config = ProptestConfig::with_cases(cases);
    config.failure_persistence = None;
    config
}

/// The ASC NULLS LAST single-column key of a non-NULL value, from the public
/// codec API (independent of the kernels).
fn enc(ty: &KeyType, v: &Value) -> Vec<u8> {
    let mut buf = Vec::new();
    encode_key(&[KeyColumn::asc(ty.clone())], &[Some(v.clone())], &mut buf)
        .unwrap_or_else(|e| panic!("encode {ty:?} {v:?}: {e}"));
    buf
}

/// How one side of a scan range is bounded.
#[derive(Debug, Clone, Copy)]
enum Side {
    Unbounded,
    Included(usize),
    Excluded(usize),
}

fn side() -> impl Strategy<Value = Side> {
    prop_oneof![
        1 => Just(Side::Unbounded),
        2 => (0usize..64).prop_map(Side::Included),
        2 => (0usize..64).prop_map(Side::Excluded),
    ]
}

fn bound(s: Side, sample: &[Value]) -> Bound<&Value> {
    match s {
        Side::Unbounded => Bound::Unbounded,
        Side::Included(i) => Bound::Included(&sample[i % sample.len()]),
        Side::Excluded(i) => Bound::Excluded(&sample[i % sample.len()]),
    }
}

/// The scan oracle: membership by the reference comparator only.
fn oracle_in(ty: &KeyType, v: &Value, lo: Bound<&Value>, hi: Bound<&Value>) -> bool {
    let lo_ok = match lo {
        Bound::Unbounded => true,
        Bound::Included(x) => sql_cmp(ty, v, x) != Ordering::Less,
        Bound::Excluded(x) => sql_cmp(ty, v, x) == Ordering::Greater,
    };
    let hi_ok = match hi {
        Bound::Unbounded => true,
        Bound::Included(x) => sql_cmp(ty, v, x) != Ordering::Greater,
        Bound::Excluded(x) => sql_cmp(ty, v, x) == Ordering::Less,
    };
    lo_ok && hi_ok
}

/// A type with a sample set (random values plus equal / near variants of
/// them, so duplicates under `=` occur) and two sides.
fn type_sample_bounds() -> BoxedStrategy<(KeyType, Vec<Value>, Side, Side)> {
    key_type()
        .prop_flat_map(|ty| {
            let pairs = vec(typed_pair(ty.clone()), 2..=8);
            (Just(ty), pairs, side(), side())
        })
        .prop_map(|(ty, pairs, lo, hi)| {
            let sample = pairs.into_iter().flat_map(|(a, b)| [a, b]).collect();
            (ty, sample, lo, hi)
        })
        .boxed()
}

proptest! {
    #![proptest_config(cfg())]

    /// (a) P-ORDER through the kernel: `compare` is the sign of `memcmp` of
    /// the ASC encodings, and of the independent reference comparator.
    #[test]
    fn compare_is_memcmp_of_asc_encodings((ty, a, b) in key_type_with_pair()) {
        let want = enc(&ty, &a).cmp(&enc(&ty, &b));
        prop_assert_eq!(compare(&ty, &a, &b), want);
        prop_assert_eq!(try_compare(&ty, &a, &b), Ok(want));
        prop_assert_eq!(compare(&ty, &b, &a), want.reverse());
        prop_assert_eq!(compare(&ty, &a, &b), sql_cmp(&ty, &a, &b));
        // Reflexive.
        prop_assert_eq!(compare(&ty, &a, &a), Ordering::Equal);
    }

    /// (a) Transitivity of the kernel order over triples of one type.
    #[test]
    fn compare_is_transitive(
        (ty, a, b) in key_type_with_pair(),
        seed in any::<u8>(),
    ) {
        // A third value derived from the pair: one of them, so every
        // triple has repeats and chains.
        let c = if seed % 2 == 0 { a.clone() } else { b.clone() };
        let ab = compare(&ty, &a, &b);
        let bc = compare(&ty, &b, &c);
        let ac = compare(&ty, &a, &c);
        if ab != Ordering::Greater && bc != Ordering::Greater {
            prop_assert_ne!(ac, Ordering::Greater);
        }
        if ab == Ordering::Less && bc == Ordering::Less {
            prop_assert_eq!(ac, Ordering::Less);
        }
    }

    /// (b) P-EQ: `eq` is `compare == Equal` and byte equality of encodings.
    #[test]
    fn eq_is_compare_equal_is_byte_equal((ty, a, b) in key_type_with_pair()) {
        let same = compare(&ty, &a, &b) == Ordering::Equal;
        prop_assert_eq!(eq(&ty, &a, &b), same);
        prop_assert_eq!(try_eq(&ty, &a, &b), Ok(same));
        prop_assert_eq!(eq(&ty, &a, &b), enc(&ty, &a) == enc(&ty, &b));
        prop_assert_eq!(eq(&ty, &a, &b), eq(&ty, &b, &a));
        prop_assert_eq!(same, sql_cmp(&ty, &a, &b) == Ordering::Equal);
    }

    /// (c) P-HASH: `eq` implies equal hash; the hash is the codec's.
    #[test]
    fn equal_values_hash_equal((ty, a, b) in key_type_with_pair()) {
        if eq(&ty, &a, &b) {
            prop_assert_eq!(hash(&ty, &a), hash(&ty, &b));
        }
        prop_assert_eq!(try_hash(&ty, &a), hash_value(&ty, &a));
        prop_assert_eq!(hash(&ty, &a), hash_value(&ty, &a).unwrap_or(u64::MAX));
    }

    /// (d) `in_bounds` agrees with an exhaustive linear scan whose
    /// membership test is the reference comparator.
    #[test]
    fn in_bounds_matches_a_linear_scan((ty, sample, lo, hi) in type_sample_bounds()) {
        let (lo_b, hi_b) = (bound(lo, &sample), bound(hi, &sample));
        let got: Vec<bool> = sample
            .iter()
            .map(|v| in_bounds(&ty, v, lo_b, hi_b))
            .collect();
        let want: Vec<bool> = sample
            .iter()
            .map(|v| oracle_in(&ty, v, lo_b, hi_b))
            .collect();
        prop_assert_eq!(got, want);
        for v in &sample {
            prop_assert_eq!(try_in_bounds(&ty, v, lo_b, hi_b), Ok(oracle_in(&ty, v, lo_b, hi_b)));
        }
    }

    /// (d) A sorted sample: the values admitted by a range are a contiguous
    /// run of it.
    #[test]
    fn in_bounds_admits_a_contiguous_run((ty, sample, lo, hi) in type_sample_bounds()) {
        let mut sorted = sample.clone();
        sorted.sort_by(|a, b| compare(&ty, a, b));
        let (lo_b, hi_b) = (bound(lo, &sample), bound(hi, &sample));
        let flags: Vec<bool> = sorted.iter().map(|v| in_bounds(&ty, v, lo_b, hi_b)).collect();
        let first = flags.iter().position(|f| *f);
        let last = flags.iter().rposition(|f| *f);
        if let (Some(f), Some(l)) = (first, last) {
            prop_assert!(flags[f..=l].iter().all(|x| *x));
        }
    }

    /// Composed keys: `compare_key` is `memcmp` of `encode_key` and the
    /// reference key order, NULLs, direction and placement included.
    #[test]
    fn compare_key_is_memcmp_of_keys((cols, va, vb) in composite_key_pair()) {
        let mut ka = Vec::new();
        let mut kb = Vec::new();
        encode_key(&cols, &va, &mut ka).unwrap_or_else(|e| panic!("encode: {e}"));
        encode_key(&cols, &vb, &mut kb).unwrap_or_else(|e| panic!("encode: {e}"));
        prop_assert_eq!(compare_key(&cols, &va, &vb), ka.cmp(&kb));
        prop_assert_eq!(try_compare_key(&cols, &va, &vb), Ok(ka.cmp(&kb)));
        prop_assert_eq!(compare_key(&cols, &va, &vb), key_cmp(&cols, &va, &vb));
    }

    /// (e) A value of the wrong type for the declared type is an error, never
    /// a panic, in every kernel (and the shims return their fallbacks).
    #[test]
    fn mismatched_values_never_panic(
        ty in key_type(),
        (vty, v) in key_type_with_value(),
    ) {
        let r = try_compare(&ty, &v, &v);
        if vty == ty {
            prop_assert_eq!(r, Ok(Ordering::Equal));
        } else if r.is_ok() {
            // Same variant under a different payload (such as text under
            // another collation or array under another element type) is
            // acceptable only if it really encodes.
            prop_assert!(try_hash(&ty, &v).is_ok());
        }
        let _ = compare(&ty, &v, &v);
        let _ = eq(&ty, &v, &v);
        let _ = hash(&ty, &v);
        let _ = in_bounds(&ty, &v, Bound::Included(&v), Bound::Excluded(&v));
        let _ = try_in_bounds(&ty, &v, Bound::Unbounded, Bound::Included(&v));
    }

    /// (e) Random values of random types never panic any kernel, and the
    /// bounds `[v, v]` always contain `v` (every generated value is valid).
    #[test]
    fn valid_values_are_in_their_own_closed_point_range((ty, v) in key_type_with_value()) {
        prop_assert!(in_bounds(&ty, &v, Bound::Included(&v), Bound::Included(&v)));
        prop_assert!(!in_bounds(&ty, &v, Bound::Excluded(&v), Bound::Unbounded));
        prop_assert!(!in_bounds(&ty, &v, Bound::Unbounded, Bound::Excluded(&v)));
        prop_assert!(in_bounds(&ty, &v, Bound::Unbounded, Bound::Unbounded));
        // And a freshly generated value is comparable to itself.
        prop_assert_eq!(compare(&ty, &v, &v), Ordering::Equal);
    }
}

// ---------------------------------------------------------------------------
// (e) extreme values, deterministic
// ---------------------------------------------------------------------------

fn nest_arrays(depth: usize) -> Jsonb {
    let mut j = Jsonb::Null;
    for _ in 0..depth {
        j = Jsonb::Array(vec![j]);
    }
    j
}

fn nest_objects(depth: usize) -> Jsonb {
    let mut j = Jsonb::Bool(true);
    for _ in 0..depth {
        j = Jsonb::Object(vec![("k".to_string(), j)]);
    }
    j
}

fn dec(negative: bool, digits: Vec<u8>, scale: i32) -> Value {
    Value::Numeric(Numeric::Finite(Decimal {
        negative,
        digits,
        scale,
    }))
}

/// Calls every kernel on every pair; any panic fails the test. Returns the
/// number of ordered pairs where `compare` succeeded.
fn exercise(ty: &KeyType, vals: &[Value]) -> usize {
    let mut ok = 0;
    for a in vals {
        let _ = hash(ty, a);
        for b in vals {
            let o = compare(ty, a, b);
            let _ = eq(ty, a, b);
            let _ = in_bounds(ty, a, Bound::Included(b), Bound::Unbounded);
            if try_compare(ty, a, b).is_ok() {
                ok += 1;
                assert_eq!(compare(ty, b, a), o.reverse(), "{ty:?}: {a:?} {b:?}");
                assert_eq!(eq(ty, a, b), o == Ordering::Equal);
            }
        }
    }
    ok
}

#[test]
fn extreme_scalars_do_not_panic_and_order_totally() {
    let ints2 = [i16::MIN, -1, 0, 1, i16::MAX].map(Value::Int2);
    let ints4 = [i32::MIN, -1, 0, 1, i32::MAX].map(Value::Int4);
    let ints8 = [i64::MIN, -1, 0, 1, i64::MAX].map(Value::Int8);
    assert_eq!(exercise(&KeyType::Int2, &ints2), 25);
    assert_eq!(exercise(&KeyType::Int4, &ints4), 25);
    assert_eq!(exercise(&KeyType::Int8, &ints8), 25);
    assert_eq!(
        exercise(&KeyType::Date, &[i32::MIN, 0, i32::MAX].map(Value::Date)),
        9
    );
    assert_eq!(
        exercise(
            &KeyType::Timestamp,
            &[i64::MIN, 0, i64::MAX].map(Value::Timestamp)
        ),
        9
    );
    assert_eq!(
        exercise(
            &KeyType::TimestampTz,
            &[i64::MIN, 0, i64::MAX].map(Value::TimestampTz)
        ),
        9
    );
    let floats = [
        f64::NEG_INFINITY,
        f64::MIN,
        -f64::MIN_POSITIVE,
        -0.0,
        0.0,
        f64::MIN_POSITIVE,
        f64::MAX,
        f64::INFINITY,
        f64::NAN,
        -f64::NAN,
    ]
    .map(Value::Float8);
    assert_eq!(exercise(&KeyType::Float8, &floats), 100);
    let floats4 = [
        f32::NEG_INFINITY,
        f32::MIN,
        -0.0,
        f32::MIN_POSITIVE,
        f32::MAX,
        f32::INFINITY,
        f32::NAN,
    ]
    .map(Value::Float4);
    assert_eq!(exercise(&KeyType::Float4, &floats4), 49);
    // Interval at the i32 / i64 field extremes (128-bit span, no overflow).
    let intervals = [
        Interval::NEG_INFINITY,
        Interval::INFINITY,
        Interval {
            months: i32::MAX,
            days: 0,
            micros: 0,
        },
        Interval {
            months: i32::MIN,
            days: 0,
            micros: 0,
        },
        Interval {
            months: 0,
            days: i32::MAX,
            micros: i64::MAX,
        },
        Interval {
            months: 0,
            days: i32::MIN,
            micros: i64::MIN,
        },
        Interval {
            months: 0,
            days: 0,
            micros: 0,
        },
    ]
    .map(Value::Interval);
    assert_eq!(exercise(&KeyType::Interval, &intervals), 49);
    // time domain edges, and one value outside it (an error, not a panic).
    let times = [0, Interval::USECS_PER_DAY].map(Value::Time);
    assert_eq!(exercise(&KeyType::Time, &times), 4);
    let bad_times = [i64::MIN, -1, Interval::USECS_PER_DAY + 1, i64::MAX].map(Value::Time);
    assert_eq!(exercise(&KeyType::Time, &bad_times), 0);
    assert_eq!(
        exercise(
            &KeyType::Uuid,
            &[Value::Uuid([0; 16]), Value::Uuid([0xFF; 16])]
        ),
        4
    );
}

#[test]
fn huge_numerics_do_not_panic() {
    let ty = KeyType::Numeric;
    let big_digits = vec![9u8; 20_000];
    let vals = vec![
        Value::Numeric(Numeric::NaN),
        Value::Numeric(Numeric::PosInf),
        Value::Numeric(Numeric::NegInf),
        // PostgreSQL's limits: 131072 digits before the point, 16383 after.
        dec(false, vec![1; 131_072], 0),
        dec(true, vec![1; 131_072], 0),
        dec(false, vec![1; 16_383], 16_383),
        dec(false, vec![1], 16_383),
        dec(false, vec![1], -131_071),
        dec(false, big_digits.clone(), 0),
        dec(true, big_digits, 10_000),
        dec(false, vec![0; 50_000], 3),
        dec(false, vec![], i32::MAX),
    ];
    assert_eq!(exercise(&ty, &vals), vals.len() * vals.len());
    // Exponents outside i32 are errors from the codec, never panics.
    let out_of_range = dec(false, vec![1], i32::MIN);
    let _ = exercise(&ty, &[out_of_range.clone(), vals[0].clone()]);
    assert!(try_compare(&ty, &out_of_range, &vals[0]).is_err());
    // The widest representable scale is still fine.
    let wide_scale = dec(false, vec![1], i32::MAX - 1);
    assert_eq!(try_compare(&ty, &wide_scale, &vals[0]), Ok(Ordering::Less));
    // Digit values outside 0..=9 are invalid input.
    assert!(try_hash(&ty, &dec(false, vec![200], 0)).is_err());
    // Magnitude order holds at the PostgreSQL limits.
    assert_eq!(
        compare(
            &ty,
            &dec(false, vec![1; 131_072], 0),
            &dec(false, vec![1], -131_071)
        ),
        Ordering::Greater
    );
}

#[test]
fn long_strings_and_byteas_do_not_panic() {
    let long = "é".repeat(100_000);
    let longer = format!("{long}a");
    let vals = [
        Value::Text(String::new()),
        Value::Text(long.clone()),
        Value::Text(longer),
    ];
    assert_eq!(exercise(&KeyType::Text(Collation::C), &vals), 9);
    let nuls = vec![0u8; 100_000];
    let mut nuls_one = nuls.clone();
    nuls_one.push(1);
    let vals = [
        Value::Bytea(vec![]),
        Value::Bytea(nuls.clone()),
        Value::Bytea(nuls_one),
        Value::Bytea(vec![0xFF; 100_000]),
    ];
    assert_eq!(exercise(&KeyType::Bytea, &vals), 16);
    assert_eq!(compare(&KeyType::Bytea, &vals[1], &vals[2]), Ordering::Less);
}

#[test]
fn deep_jsonb_nesting_up_to_the_cap() {
    // The codec's cap is 1000 levels below the top (`MAX_JSONB_DEPTH`).
    const CAP: usize = 1000;
    let ty = KeyType::Jsonb;
    let arrays = |d: usize| Value::Jsonb(nest_arrays(d));
    let objects = |d: usize| Value::Jsonb(nest_objects(d));
    let ok_vals = [
        arrays(0),
        arrays(1),
        arrays(CAP - 1),
        arrays(CAP),
        objects(1),
        objects(CAP),
    ];
    assert_eq!(exercise(&ty, &ok_vals), ok_vals.len() * ok_vals.len());
    // Order stays sane at depth: [[..]] with one more level is greater only
    // where the first difference says so; equal shapes are equal.
    assert_eq!(compare(&ty, &arrays(CAP), &arrays(CAP)), Ordering::Equal);
    assert!(eq(&ty, &objects(CAP), &objects(CAP)));
    assert_eq!(
        hash(&ty, &arrays(CAP)),
        hash_value(&ty, &arrays(CAP)).unwrap_or(0)
    );
    // Past the cap: an error from the codec, not a stack overflow or panic.
    let too_deep = [arrays(CAP + 1), objects(CAP + 1)];
    for deep in &too_deep {
        assert!(try_hash(&ty, deep).is_err());
        assert!(try_compare(&ty, deep, deep).is_err());
        assert!(!eq(&ty, deep, deep));
        assert_eq!(hash(&ty, deep), 0);
        assert_eq!(compare(&ty, deep, &ok_vals[0]), Ordering::Greater);
        assert!(!in_bounds(&ty, deep, Bound::Unbounded, Bound::Unbounded));
    }
}

#[test]
fn deep_jsonb_inside_arrays_and_wide_arrays() {
    let aty = KeyType::Array(Box::new(KeyType::Jsonb));
    let deep = Value::Array(Array::from_elems(vec![
        Some(Value::Jsonb(nest_arrays(999))),
        None,
        Some(Value::Jsonb(Jsonb::Null)),
    ]));
    assert_eq!(exercise(&aty, &[deep.clone(), deep]), 4);
    let ity = KeyType::Array(Box::new(KeyType::Int4));
    let wide = |n: usize, last: i32| {
        let mut elems: Vec<Option<Value>> = (0..n).map(|_| Some(Value::Int4(i32::MAX))).collect();
        elems.push(Some(Value::Int4(last)));
        Value::Array(Array::from_elems(elems))
    };
    let vals = [wide(50_000, i32::MIN), wide(50_000, i32::MAX), wide(10, 0)];
    assert_eq!(exercise(&ity, &vals), 9);
    assert_eq!(compare(&ity, &vals[0], &vals[1]), Ordering::Less);
}
